/* Copyright (c) 2010 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <getopt.h>

#include <iostream>
#include <vector>

#include "Common.h"
#include "Buffer.h"
#include "CycleCounter.h"
#include "OptionParser.h"
#include "TransportManager.h"

/**
 * \file
 * A telnet client over FastTransport.
 */

/// Generate fake data if true, else read data from stdin.
bool generate;

/// Service locators of the hosts to bounce data off of.
std::vector<string> serverLocators;

/**
 * Entry point for the program.  Acts as a client which sends packets
 * to servers and expects a response.  If generate then just send garbage
 * and discard the results.  If !generate then send stdin and receive to
 * stdout.
 *
 * \param argc
 *      The number of command line args.
 * \param argv
 *      An array of length argc containing the command line args.
 */
int
main(int argc, char *argv[])
try
{
    using namespace RAMCloud; // NOLINT

    Context context(false);
    Context::Guard _(context);

    // Telnet-specific options
    OptionsDescription telnetOptions("Telnet");
    telnetOptions.add_options()
        ("generate,g",
         ProgramOptions::bool_switch(&generate),
         "Continuously send random data")
        ("server,s",
         ProgramOptions::value<vector<string> >(&serverLocators),
         "Server locator of server, can be repeated to send to all");

    OptionParser optionParser(telnetOptions, argc, argv);

    if (!serverLocators.size()) {
        optionParser.usage();
        RAMCLOUD_DIE("Error: No servers specified to telnet to.");
    }

    int serverCount = downCast<uint32_t>(serverLocators.size());
    Transport::SessionRef session[serverCount];
    for (int i = 0; i < serverCount; i++) {
        session[i] = Context::get().transportManager->getSession(
                                                    serverLocators[i].c_str());
    }

    if (!generate) {
        char sendbuf[1024];
        char recvbuf[serverCount][1024];
        Transport::ClientRpc* rpcs[serverCount];
        RAMCLOUD_LOG(DEBUG, "Sending to %d servers", serverCount);
        while (fgets(sendbuf, sizeof(sendbuf), stdin) != NULL) {
            Buffer response[serverCount];
            Buffer request[serverCount];
            for (int i = 0; i < serverCount; i++) {
                Buffer::Chunk::appendToBuffer(&request[i], sendbuf,
                    static_cast<uint32_t>(strlen(sendbuf)));
                RAMCLOUD_LOG(DEBUG, "Sending out request %d to %s",
                    i, serverLocators[i].c_str());
                rpcs[i] = session[i]->clientSend(&request[i], &response[i]);
            }

            for (int i = 0; i < serverCount; i++) {
                RAMCLOUD_LOG(DEBUG, "Getting reply %d", i);
                rpcs[i]->wait();
                uint32_t respLen = response[i].getTotalLength();
                if (respLen >= sizeof(recvbuf[i])) {
                    RAMCLOUD_LOG(WARNING, "Failed to get reply %d", i);
                    break;
                }
                recvbuf[i][respLen] = '\0';
                response[i].copy(0, respLen, recvbuf[i]);
                fputs(static_cast<char*>(recvbuf[i]), stdout);
            }
        }
    } else {
        char buf[1024];
        memset(buf, 0xcc, sizeof(buf));
        while (true) {
            Buffer request;
            Buffer response;
            uint64_t totalFrags = (generateRandom() & 0x3FF);
            for (uint32_t i = 0; i < totalFrags; i++)
                Buffer::Chunk::appendToBuffer(&request, buf, sizeof(buf));
            session[0]->clientSend(&request, &response)->wait();
        }
    }
    return 0;
} catch (RAMCloud::Exception& e) {
    using namespace RAMCloud;
    RAMCLOUD_LOG(ERROR, "%s", e.str().c_str());
    return 1;
}
