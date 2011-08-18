/* Copyright (c) 2009-2011 Stanford University
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

/**
 * \file
 * Makes sure a given number of servers have registered with the coordinator.
 */

#include "Cycles.h"
#include "ShortMacros.h"
#include "OptionParser.h"
#include "RamCloud.h"

using namespace RAMCloud;

int
main(int argc, char *argv[])
try
{
    OptionsDescription clientOptions("EnsureServers");
    int number = 0;
    int timeout = 20;
    clientOptions.add_options()
        ("number,n",
         ProgramOptions::value<int>(&number),
             "The number of servers desired.")
        ("timeout,t",
         ProgramOptions::value<int>(&timeout),
             "The number of seconds for which to wait.");

    OptionParser optionParser(clientOptions, argc, argv);

    LOG(NOTICE, "client: Connecting to %s",
        optionParser.options.getCoordinatorLocator().c_str());


    uint64_t quitTime = Cycles::rdtsc() + Cycles::fromNanoseconds(
        1000000000UL * timeout);
    int actual = -1;
    do {
        ProtoBuf::ServerList serverList;
        try {
            RamCloud(optionParser.options.getCoordinatorLocator().c_str())
                .coordinator.getServerList(serverList);
        } catch (const TransportException& e) {
            usleep(10000);
            continue;
        }
        actual = serverList.server_size();
        LOG(DEBUG, "found %d servers", actual);
        if (number == actual)
            return 0;
        usleep(10000);
    } while (Cycles::rdtsc() < quitTime);
    return (actual - number);
} catch (const ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 111;
} catch (const Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 112;
}
