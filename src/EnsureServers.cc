/* Copyright (c) 2009-2015 Stanford University
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

#include "AbstractServerList.h"
#include "Cycles.h"
#include "ShortMacros.h"
#include "OptionParser.h"
#include "RamCloud.h"
#include "ServiceMask.h"

using namespace RAMCloud;

/**
 * Count the number of master and backup services enlisted. This is needed
 * since each process may have multiple different services.
 */
void
countServices(ProtoBuf::ServerList& serverList, int& masters, int &backups)
{
    masters = 0;
    backups = 0;
    for (int i = 0; i < serverList.server_size(); i++) {
        auto status = ServerStatus(serverList.server(i).status());
        if (status != ServerStatus::UP)
            continue;
        ServiceMask mask =
            ServiceMask::deserialize(serverList.server(i).services());
        if (mask.has(WireFormat::MASTER_SERVICE))
            masters++;
        if (mask.has(WireFormat::BACKUP_SERVICE))
            backups++;
    }
}

int
main(int argc, char *argv[])
try
{
    // need external context to set log levels with OptionParser
    Context context(true);

    OptionsDescription clientOptions("EnsureServers");
    int numMasters = -1;
    int numBackups = -1;
    int timeout = 20;
    clientOptions.add_options()
        ("masters,m",
         ProgramOptions::value<int>(&numMasters),
             "The desired number of enlisted master services.")
        ("backups,b",
         ProgramOptions::value<int>(&numBackups),
             "The desired number of enlisted backup services.")
        ("wait,w",
         ProgramOptions::value<int>(&timeout),
             "Give up if the servers aren't available within this many "
             "seconds.");

    OptionParser optionParser(clientOptions, argc, argv);

    if (numMasters == -1 && numBackups == -1) {
        fprintf(stderr, "Error: Specify one or both of options -m and -b\n");
        exit(1);
    }

    string coordinatorLocator =
            optionParser.options.getExternalStorageLocator();
    if (coordinatorLocator.size() == 0) {
        coordinatorLocator = optionParser.options.getCoordinatorLocator();
    }
    LOG(NOTICE, "client: Connecting to %s", coordinatorLocator.c_str());

    uint64_t quitTime = Cycles::rdtsc() + Cycles::fromNanoseconds(
        1000000000UL * timeout);
    int actualMasters = -1;
    int actualBackups = -1;
    int actualServers = -1;
    RamCloud ramcloud(&context, coordinatorLocator.c_str(),
            optionParser.options.getClusterName().c_str());
    do {
        ProtoBuf::ServerList serverList;
        CoordinatorClient::getServerList(&context, &serverList);
        actualServers = serverList.server_size();
        countServices(serverList, actualMasters, actualBackups);
        LOG(DEBUG, "found %d masters, %d backups (in %d servers)",
            actualMasters, actualBackups, actualServers);

        bool mastersReady = false;
        if (numMasters == actualMasters || numMasters == -1)
            mastersReady = true;

        bool backupsReady = false;
        if (numBackups == actualBackups || numBackups == -1)
            backupsReady = true;

        if (mastersReady && backupsReady)
            return 0;
        usleep(10000);
    } while (Cycles::rdtsc() < quitTime);
    LOG(ERROR, "want %d/%d active masters/backups, but found %d/%d "
        "(in %d servers)", numMasters, numBackups, actualMasters,
        actualBackups, actualServers);
    return 1;
} catch (const ClientException& e) {
    LOG(ERROR, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (const Exception& e) {
    LOG(ERROR, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
