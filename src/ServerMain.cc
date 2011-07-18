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
 * This file provides the main program for RAMCloud storage servers.
 */

#include <stdlib.h>
#include <getopt.h>
#include <errno.h>

#include "BackupClient.h"
#include "BackupService.h"
#include "OptionParser.h"
#include "MasterService.h"
#include "PingService.h"
#include "Segment.h"
#include "ServiceManager.h"
#include "TransportManager.h"

static int cpu;
static uint32_t replicas;

int
main(int argc, char *argv[])
try
{
    using namespace RAMCloud;

    ServerConfig masterConfig;
    string masterTotalMemory, hashTableMemory;

    BackupService::Config backupConfig;
    bool inMemory;
    uint32_t segmentCount;
    string backupFile;
    int backupStrategy;

    bool masterOnly;
    bool backupOnly;

    OptionsDescription serverOptions("Server");
    serverOptions.add_options()
        ("backupInMemory,m",
         ProgramOptions::bool_switch(&inMemory),
         "Backup will store segment replicas in memory")
        ("backupOnly,B",
         ProgramOptions::bool_switch(&backupOnly),
         "The server should run the backup service only (no master)")
        ("backupStrategy",
         ProgramOptions::value<int>(&backupStrategy)->
           default_value(RANDOM_REFINE_AVG),
         "0 random refine min, 1 random refine avg, 2 even distribution, "
         "3 uniform random")
        ("cpu,p",
         ProgramOptions::value<int>(&cpu)->
            default_value(-1),
         "CPU mask to pin to")
        ("file,f",
         ProgramOptions::value<string>(&backupFile)->
            default_value("/var/tmp/backup.log"),
         "The file path to the backup storage.")
        ("HashTableMemory,h",
         ProgramOptions::value<string>(&hashTableMemory)->
            default_value("10%"),
         "Percentage or megabytes of master memory allocated to the hash table")
        ("masterOnly,M",
         ProgramOptions::bool_switch(&masterOnly),
         "The server should run the master service only (no backup)")
        ("MasterTotalMemory,t",
         ProgramOptions::value<string>(&masterTotalMemory)->
            default_value("10%"),
         "Percentage or megabytes of system memory for master log & hash table")
        ("replicas,r",
         ProgramOptions::value<uint32_t>(&replicas)->
            default_value(0),
         "Number of backup copies to make for each segment")
        ("segments,s",
         ProgramOptions::value<uint32_t>(&segmentCount)->
            default_value(512),
         "Number of segment frames in backup storage");

    OptionParser optionParser(serverOptions, argc, argv);

    if (masterOnly && backupOnly) {
        DIE("Can't specify both -B and -M options");
    }

    const char* servicesInfo;
    if (masterOnly)
        servicesInfo = "master";
    else if (backupOnly)
        servicesInfo = "backup";
    else
        servicesInfo = "master and backup";

    if (cpu != -1) {
        if (!pinToCpu(cpu))
            DIE("server: Couldn't pin to core %d", cpu);
        LOG(DEBUG, "server: Pinned to core %d", cpu);
    }

    transportManager.initialize(
            optionParser.options.getLocalLocator().c_str());
    LOG(NOTICE, "%s: Listening on %s", servicesInfo,
            transportManager.getListeningLocatorsString().c_str());
    CoordinatorClient coordinator(
        optionParser.options.getCoordinatorLocator().c_str());

    Tub<MasterService> masterService;
    if (!backupOnly) {
        masterConfig.coordinatorLocator =
                optionParser.options.getCoordinatorLocator();
        masterConfig.localLocator = optionParser.options.getLocalLocator();
        LOG(NOTICE, "Using %u backups", replicas);
        MasterService::sizeLogAndHashTable(masterTotalMemory,
                                          hashTableMemory, &masterConfig);
        masterService.construct(masterConfig, &coordinator, replicas);
        masterService->init();
        serviceManager->addService(*masterService, MASTER_SERVICE);
    }

    std::unique_ptr<BackupStorage> storage;
    Tub<BackupService> backupService;
    if (!masterOnly) {
        backupConfig.coordinatorLocator =
                optionParser.options.getCoordinatorLocator();
        backupConfig.localLocator =
                transportManager.getListeningLocatorsString();
        backupConfig.backupStrategy = static_cast<BackupStrategy>(
                backupStrategy);
        if (inMemory)
            storage.reset(new InMemoryStorage(Segment::SEGMENT_SIZE,
                                              segmentCount));
        else
            storage.reset(new SingleFileStorage(Segment::SEGMENT_SIZE,
                                                segmentCount,
                                                backupFile.c_str(),
                                                O_DIRECT | O_SYNC));
        backupService.construct(backupConfig, *storage);
        backupService->init();
        serviceManager->addService(*backupService, BACKUP_SERVICE);
    }
    PingService pingService;
    serviceManager->addService(pingService, PING_SERVICE);
    while (true) {
        dispatch->poll();
    }

    return 0;
} catch (std::exception& e) {
    using namespace RAMCloud;
    LOG(ERROR, "server: %s", e.what());
    return 1;
}
