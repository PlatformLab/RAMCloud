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

#include <Common.h>
#include <Buffer.h>
#include <Service.h>
#include <FastTransport.h>
#include <CycleCounter.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <getopt.h>

/**
 * \file
 * A telnet client over FastTransport.
 */

char address[50];
uint16_t port;
int cpu;
bool generate;

void __attribute__ ((noreturn))
usage(char *arg0)
{
    printf("Usage: %s "
            "[-p port] [-a address] [-c cpu]\n"
           "\t-p\t--port\t\tChoose which port to connect to.\n"
           "\t-a\t--address\tChoose which address to connect to.\n"
           "\t-c\t--cpu\t\tRestrict the test to a specific CPU (0 indexed).\n",
           "\t-g\t--generate\t\tGenerate junk traffic.\n",
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
    generate = false;

    struct option long_options[] = {
        {"address", required_argument, NULL, 'a'},
        {"port", required_argument, NULL, 'p'},
        {"cpu", required_argument, NULL, 'a'},
        {"generate", no_argument, NULL, 'g'},
        {0, 0, 0, 0},
    };

    int c;
    int i = 0;
    while ((c = getopt_long(argc, argv, "a:p:c:g",
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
        case 'g':
            generate = true;
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
    using namespace RAMCloud; // NOLINT

    cmdline(argc, argv);

    logger.setLogLevel(TRANSPORT_MODULE, DEBUG);

    UDPDriver d;
    FastTransport tx(&d);

    Service service;
    service.setIp(address);
    service.setPort(port);

    if (!generate) {
        char buf[1024];
        while (fgets(buf, sizeof(buf), stdin) != NULL) {
            Buffer request;
            Buffer response;
            Buffer::Chunk::appendToBuffer(&request, buf,
                                          static_cast<uint32_t>(strlen(buf)));
            tx.clientSend(&service, &request, &response)->getReply();

            uint32_t respLen = response.getTotalLength();
            if (respLen >= sizeof(buf))
                return 1;
            buf[respLen] = '\0';
            response.copy(0, respLen, buf);
            fputs(static_cast<char*>(buf), stdout);
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
            tx.clientSend(&service, &request, &response)->getReply();
        }
    }
    return 0;
} catch (RAMCloud::Exception e) {
    using namespace RAMCloud;
    LOG(ERROR, "FastTransport: %s\n", e.message.c_str());
}
