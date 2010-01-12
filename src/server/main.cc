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

#include <config.h>

#include <server/server.h>
#include <shared/net.h>

#include <getopt.h>

void usage(char *arg0) {
    printf("Usage: %s [-r]\n"
           "\t-r|--restore\t\tRestore from backup before serving\n",
           arg0);
}

void
cmdline(int argc, char *argv[], RAMCloud::ServerConfig *config) {
    int i = 0;
    int c;
    struct option long_options[] = {
        {"restore", no_argument, NULL, 'r'},
        {0,0,0,0}
    };

    while((c = getopt_long(argc, argv, "r", long_options, &i)) >= 0) {
        switch (c) {
        case 'r':
            config->restore = true;
            break;
        default:
            usage(argv[0]);
            exit(EXIT_FAILURE);
            break;
        }
    }
}

int
main(int argc, char *argv[])
{
    RAMCloud::ServerConfig config;
    cmdline(argc, argv, &config);

    RAMCloud::Net *net = new RAMCloud::CNet(SVRADDR, SVRPORT,
                                            CLNTADDR, CLNTPORT);
    net->Listen();
    RAMCloud::Server *server = new RAMCloud::Server(&config, net);

    server->Run();

    delete server;
    delete net;

    return 0;
}
