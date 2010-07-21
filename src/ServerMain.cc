/* Copyright (c) 2009 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

// RAMCloud pragma [CPPLINT=0]

#include <config.h>

#include <Server.h>
#include <TCPTransport.h>

#include <stdlib.h>
#include <getopt.h>
#include <errno.h>

static int cpu = -1;

void __attribute__ ((noreturn))
usage(char *arg0)
{
    printf("Usage: %s [-r] [-p port] [-a address] [-c cpu]\n"
           "\t-r\t--restore\tRestore from backup before serving.\n"
           "\t-p\t--port\t\tChoose which port to listen on.\n"
           "\t-a\t--address\tChoose which address to listen on.\n"
           "\t-c\t--cpu\t\tRestrict the server to a specific CPU (0 indexed).\n",
           arg0);
    exit(EXIT_FAILURE);
}

void
cmdline(int argc, char *argv[], RAMCloud::ServerConfig *config)
{
    int i = 0;
    int c;
    struct option long_options[] = {
        {"restore", no_argument, NULL, 'r'},
        {"port", required_argument, NULL, 'p'},
        {"address", required_argument, NULL, 'a'},
        {"cpu", required_argument, NULL, 'a'},
        {0,0,0,0},
    };

    while((c = getopt_long(argc, argv, "rp:a:c:", long_options, &i)) >= 0) {
        switch (c) {
        case 'r':
            config->restore = true;
            break;
        case 'p':
            config->port = atoi(optarg);
            if (config->port > 65536 || config->port < 0)
                usage(argv[0]);
            break;
        case 'a':
            strncpy(config->address, optarg, sizeof(config->address));
            config->address[sizeof(config->address) - 1] = '\0';
            break;
        case 'c':
            cpu = atoi(optarg);
            break;
        default:
            usage(argv[0]);
            break;
        }
    }
}

int
main(int argc, char *argv[])
try
{
    RAMCloud::ServerConfig config;
    cmdline(argc, argv, &config);

    printf("server: Listening on interface %s\n", config.address);
    printf("server: Listening on port %d\n", config.port);

    if (cpu != -1) {
        cpu_set_t cpus;
        CPU_ZERO(&cpus);
        CPU_SET(cpu, &cpus);

        int r = sched_setaffinity(0, sizeof(cpus), &cpus);
        if (r < 0) {
            fprintf(stderr, "server: Couldn't pin to core %d: %s\n",
                    cpu, strerror(errno));
            exit(EXIT_FAILURE);
        }
        printf("server: Pinned to core %d\n", cpu);
    }

    RAMCloud::TCPTransport trans(config.address, config.port);
    RAMCloud::Server server(&config, &trans);

    server.Run();

    return 0;
} catch (RAMCloud::Exception e) {
    fprintf(stderr, "server: %s\n", e.message.c_str());
}
