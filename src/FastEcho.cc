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
#include "FastTransport.h"

#include "Driver.h"

/**
 * \file
 * An echo server over FastTransport.
 */

char address[50];
uint16_t port;
int cpu;

void __attribute__ ((noreturn))
usage(char *arg0)
{
    printf("Usage: %s "
            "[-p port] [-a address] [-c cpu]\n"
           "\t-p\t--port\t\tChoose which port to connect to.\n"
           "\t-a\t--address\tChoose which address to connect to.\n"
           "\t-c\t--cpu\t\tRestrict the test to a specific CPU (0 indexed).\n",
           arg0);
    exit(EXIT_FAILURE);
}

void
cmdline(int argc, char *argv[])
{
    port = 12242;
    strncpy(address, "127.0.0.1", sizeof(address));
    address[sizeof(address) - 1] = '\0';
    cpu = -1;

    struct option long_options[] = {
        {"address", required_argument, NULL, 'a'},
        {"port", required_argument, NULL, 'p'},
        {"cpu", required_argument, NULL, 'a'},
        {0, 0, 0, 0},
    };

    int c;
    int i = 0;
    while ((c = getopt_long(argc, argv, "a:p:c:",
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
        case 'c':
            cpu = atoi(optarg);
            break;
        default:
            usage(argv[0]);
        }
    }
}

int
main(int argc, char *argv[])
try
{
    cmdline(argc, argv);

    using namespace RAMCloud;

    logger.setLogLevel(TRANSPORT_MODULE, DEBUG);

    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_aton(&address[0], &addr.sin_addr) == 0)
        throw Exception("inet_aton failed");

    UDPDriver d(reinterpret_cast<const sockaddr *>(&addr),
                static_cast<socklen_t>(sizeof(addr)));
    FastTransport tx(&d);

    while (true) {
        Buffer payload;
        Transport::ServerRPC* rpc = tx.serverRecv();
        Buffer::Iterator iter(rpc->recvPayload);
        while (!iter.isDone()) {
            Buffer::Chunk::appendToBuffer(&rpc->replyPayload,
                                          iter.getData(),
                                          iter.getLength());
            // TODO(ongaro): This is unsafe if the Transport discards the
            // received buffer before it is done with the response buffer.
            // I can't think of any real RPCs where this will come up.
            iter.next();
        }
        rpc->sendReply();
    }
    return 0;
} catch (RAMCloud::Exception e) {
    using namespace RAMCloud;
    LOG(ERROR, "FastEcho: %s\n", e.message.c_str());
}
