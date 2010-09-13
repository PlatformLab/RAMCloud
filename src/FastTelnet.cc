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

#include "Common.h"
#include "Buffer.h"
#include "Service.h"
#include "FastTransport.h"
#include "CycleCounter.h"
#include "UDPDriver.h"


/**
 * \file
 * A telnet client over FastTransport.
 */

/// The address to connect to.
char address[50];
/// The port to connect to.
uint16_t port;
/// Generate fake data if true, else read data from stdin.
bool generate;
/// Number of remote servers to connect to.
int multi;
/// If true changes meaning to make multi connections to a single server.
bool sameServer;

static void __attribute__((noreturn)) usage(char *arg0);
/**
 * Print help and exit.
 *
 * \param arg0
 *      The name of the executable used to launch this process.
 */
static void
usage(char *arg0)
{
    printf("Usage: %s "
            "[-p port] [-a address] [-g] [-m serverCount] [-s]\n"
           "\t-p\t--port\t\tChoose which port to connect to.\n"
           "\t-a\t--address\tChoose which address to connect to.\n"
           "\t-g\t--generate\tGenerate junk traffic.\n"
           "\t-m\t--multi\t\tConnect to addl servers on same addr on port "
                "range starting at supplied port.\n"
           "\t-s\t--same\t\tConnect to number of times by -m, but "
                "to the same address/port (multi conn to same server)\n\n",
           arg0);
    exit(EXIT_FAILURE);
}

/**
 * Process commandline args.  Sets up defaults for the globals above and
 * populates them accoring to user passed parameters.
 *
 * \param argc
 *      The number of command line args.
 * \param argv
 *      An array of length argc containing the command line args.
 */
static void
cmdline(int argc, char *argv[])
{
    port = 12242;
    strncpy(address, "127.0.0.1", sizeof(address));
    address[sizeof(address) - 1] = '\0';
    generate = false;
    multi = 1;
    sameServer = false;

    struct option long_options[] = {
        {"address", required_argument, NULL, 'a'},
        {"port", required_argument, NULL, 'p'},
        {"generate", no_argument, NULL, 'g'},
        {"multi", required_argument, NULL, 'm'},
        {"same", no_argument, NULL, 's'},
        {0, 0, 0, 0},
    };

    int c;
    int i = 0;
    while ((c = getopt_long(argc, argv, "a:p:c:gm:s",
                            long_options, &i)) >= 0)
    {
        switch (c) {
        case 'a':
            strncpy(address, optarg, sizeof(address));
            address[sizeof(address) - 1] = '\0';
            break;
        case 'p':
            port = atoi(optarg);
            break;
        case 'g':
            generate = true;
            break;
        case 'm':
            multi = atoi(optarg);
            assert(multi > 0 and multi < 100);
            break;
        case 's':
            sameServer = true;
            break;
        default:
            usage(argv[0]);
        }
    }
}

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

    cmdline(argc, argv);

    logger.setLogLevels(WARNING);

    UDPDriver d;
    FastTransport tx(&d);

    Service service[multi];
    for (int i = 0; i < multi; i++) {
        service[i].setIp(address);
        service[i].setPort(port + (sameServer ? 0 : i));
    }

    if (!generate) {
        char sendbuf[1024];
        char recvbuf[multi][1024];
        FastTransport::ClientRpc* rpcs[multi];
        LOG(DEBUG, "Sending to %d servers", multi);
        while (fgets(sendbuf, sizeof(sendbuf), stdin) != NULL) {
            Buffer response[multi];
            Buffer request[multi];
            for (int i = 0; i < multi; i++) {
                Buffer::Chunk::appendToBuffer(&request[i], sendbuf,
                    static_cast<uint32_t>(strlen(sendbuf)));
                LOG(DEBUG, "Sending out request %d to port %d",
                    i, service[i].getPort());
                rpcs[i] = tx.clientSend(&service[i], &request[i], &response[i]);
            }

            for (int i = 0; i < multi; i++) {
                LOG(DEBUG, "Getting reply %d", i);
                rpcs[i]->getReply();
                uint32_t respLen = response[i].getTotalLength();
                if (respLen >= sizeof(recvbuf[i])) {
                    LOG(WARNING, "Failed to get reply %d", i);
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
            uint64_t totalFrags = (random() & 0x3FF);
            for (uint32_t i = 0; i < totalFrags; i++)
                Buffer::Chunk::appendToBuffer(&request, buf, sizeof(buf));
            CycleCounter c;
            tx.clientSend(&service[0], &request, &response)->getReply();
        }
    }
    return 0;
} catch (RAMCloud::Exception e) {
    using namespace RAMCloud;
    LOG(ERROR, "FastTransport: %s\n", e.message.c_str());
}
