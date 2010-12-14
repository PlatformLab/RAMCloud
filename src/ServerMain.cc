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

#include <stdlib.h>
#include <getopt.h>
#include <errno.h>

#include "BackupClient.h"
#include "OptionParser.h"
#include "MasterServer.h"
#include "TransportManager.h"

static int cpu;
static uint32_t replicas;

int
main(int argc, char *argv[])
try
{
    using namespace RAMCloud;

    ServerConfig config;
    string masterTotalMemory, hashTableMemory;

    OptionsDescription serverOptions("Master");
    serverOptions.add_options()
        ("cpu,p",
         ProgramOptions::value<int>(&cpu)->
            default_value(-1),
         "CPU mask to pin to")
        ("HashTableMemory,h",
         ProgramOptions::value<string>(&hashTableMemory)->
            default_value("10%"),
         "Percentage or megabytes of master memory allocated to the hash table")
        ("MasterTotalMemory,m",
         ProgramOptions::value<string>(&masterTotalMemory)->
            default_value("10%"),
         "Percentage or megabytes of system memory for master log & hash table")
        ("replicas,r",
         ProgramOptions::value<uint32_t>(&replicas)->
            default_value(0),
         "Number of backups required for each segment");

    OptionParser optionParser(serverOptions, argc, argv);

    LOG(NOTICE, "Using %u backups", replicas);

    config.coordinatorLocator = optionParser.options.getCoordinatorLocator();
    config.localLocator = optionParser.options.getLocalLocator();

    LOG(NOTICE, "server: Listening on %s", config.localLocator.c_str());

    if (cpu != -1) {
        if (!pinToCpu(cpu))
            DIE("server: Couldn't pin to core %d", cpu);
        LOG(DEBUG, "server: Pinned to core %d", cpu);
    }

    MasterServer::sizeLogAndHashTable(masterTotalMemory,
                                      hashTableMemory, &config);

    transportManager.initialize(config.localLocator.c_str());

    CoordinatorClient coordinator(
        optionParser.options.getCoordinatorLocator().c_str());
    MasterServer server(config, &coordinator, replicas);
    server.run();

    return 0;
} catch (RAMCloud::Exception& e) {
    using namespace RAMCloud;
    LOG(ERROR, "server: %s", e.str().c_str());
    return 1;
}
