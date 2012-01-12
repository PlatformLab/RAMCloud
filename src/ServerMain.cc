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
#include "Context.h"
#include "CoordinatorClient.h"
#include "FailureDetector.h"
#include "InfRcTransport.h"
#include "MasterService.h"
#include "MembershipService.h"
#include "OptionParser.h"
#include "PingClient.h"
#include "PingService.h"
#include "Segment.h"
#include "ServerList.h"
#include "ServiceManager.h"
#include "ShortMacros.h"
#include "TransportManager.h"

static int cpu;
static uint32_t replicas;

int
main(int argc, char *argv[])
{
    using namespace RAMCloud;
    char locator[200] = "???";
    Context context(true);
    Context::Guard _(context);
    try
    {
        Tub<FailureDetector> failureDetector;
        ServerConfig masterConfig;
        string masterTotalMemory, hashTableMemory;

        BackupService::Config backupConfig;
        bool inMemory;
        uint32_t segmentCount;
        string backupFile;
        int backupStrategy;

        bool disableFailureDetector;
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
            ("disableLogCleaner,d",
             ProgramOptions::bool_switch(&masterConfig.disableLogCleaner),
             "Disable the log cleaner entirely. You will eventually run out "
             "of memory, but at least you can do so faster this way.")
            ("file,f",
             ProgramOptions::value<string>(&backupFile)->
                default_value("/var/tmp/backup.log"),
             "The file path to the backup storage.")
            ("hashTableMemory,h",
             ProgramOptions::value<string>(&hashTableMemory)->
                default_value("10%"),
             "Percentage or megabytes of master memory allocated to "
             "the hash table")
            ("masterOnly,M",
             ProgramOptions::bool_switch(&masterOnly),
             "The server should run the master service only (no backup)")
            ("totalMasterMemory,t",
             ProgramOptions::value<string>(&masterTotalMemory)->
                default_value("10%"),
             "Percentage or megabytes of system memory for master log & "
             "hash table")
            ("replicas,r",
             ProgramOptions::value<uint32_t>(&replicas)->
                default_value(0),
             "Number of backup copies to make for each segment")
            ("segments,s",
             ProgramOptions::value<uint32_t>(&segmentCount)->
                default_value(512),
             "Number of segment frames in backup storage")
            ("disableFailureDetector",
             ProgramOptions::bool_switch(&disableFailureDetector),
             "Disable the randomized failure detector");

        OptionParser optionParser(serverOptions, argc, argv);

        // Log all the command-line arguments.
        string args;
        for (int i = 0; i < argc; i++) {
            if (i != 0)
                args.append(" ");
            args.append(argv[i]);
        }
        LOG(NOTICE, "Command line: %s", args.c_str());

        if (masterOnly && backupOnly) {
            DIE("Can't specify both -B and -M options");
        }

        ServiceMask services;
        if (masterOnly) {
            services = {MASTER_SERVICE, MEMBERSHIP_SERVICE, PING_SERVICE};
        } else if (backupOnly) {
            services = {BACKUP_SERVICE, MEMBERSHIP_SERVICE, PING_SERVICE};
        } else {
            services = {MASTER_SERVICE, BACKUP_SERVICE,
                        MEMBERSHIP_SERVICE, PING_SERVICE};
        }

        if (cpu != -1) {
            if (!pinToCpu(cpu))
                DIE("server: Couldn't pin to core %d", cpu);
            LOG(DEBUG, "server: Pinned to core %d", cpu);
        }

        InfRcTransport<>::setName(
                optionParser.options.getLocalLocator().c_str());
        Context::get().transportManager->setTimeout(
                optionParser.options.getTransportTimeout());
        Context::get().transportManager->initialize(
                optionParser.options.getLocalLocator().c_str());
        LOG(NOTICE, "%s: Listening on %s", services.toString().c_str(),
            Context::get().
                transportManager->getListeningLocatorsString().c_str());
        snprintf(locator, sizeof(locator), "%s", Context::get().
                transportManager->getListeningLocatorsString().c_str());
        CoordinatorClient coordinator(
            optionParser.options.getCoordinatorLocator().c_str());

        Tub<MasterService> masterService;
        if (!backupOnly) {
            masterConfig.coordinatorLocator =
                    optionParser.options.getCoordinatorLocator();
            masterConfig.localLocator = Context::get().
                    transportManager->getListeningLocatorsString();
            LOG(NOTICE, "Using %u backups", replicas);
            MasterService::sizeLogAndHashTable(masterTotalMemory,
                                              hashTableMemory, &masterConfig);
            masterService.construct(masterConfig, &coordinator, replicas);
            Context::get().serviceManager->
                addService(*masterService, MASTER_SERVICE);
        }

        std::unique_ptr<BackupStorage> storage;
        Tub<BackupService> backupService;
        uint32_t backupReadSpeed = 0, backupWriteSpeed = 0;
        if (!masterOnly) {
            backupConfig.coordinatorLocator =
                    optionParser.options.getCoordinatorLocator();
            backupConfig.localLocator = Context::get().
                    transportManager->getListeningLocatorsString();
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
            backupService->benchmark(backupReadSpeed, backupWriteSpeed);
            Context::get().serviceManager
                ->addService(*backupService, BACKUP_SERVICE);
        }

        ServerId serverId;
        ServerList serverList;
        MembershipService membershipService(serverId, serverList);
        Context::get().serviceManager->addService(membershipService,
            MEMBERSHIP_SERVICE);

        PingService pingService(&serverList);
        Context::get().serviceManager->addService(pingService, PING_SERVICE);

        // Only pin down memory _after_ users of LargeBlockOfMemory have
        // obtained their allocations (since LBOM probes are much slower if
        // the memory needs to be pinned during mmap).
        pinAllMemory();

        // The following statement suppresses a "long gap" message that would
        // otherwise be generated by the next call to dispatch.poll (the
        // warning is benign, and is caused by the time to benchmark secondary
        // storage above.
        Dispatch& dispatch = *Context::get().dispatch;
        dispatch.currentTime = Cycles::rdtsc();

        // Enlist with the coordinator just before dedicating this thread
        // to RPC dispatch. This reduces the window of being unavailable to
        // service RPCs after enlisting with the coordinator (which can
        // lead to session open timeouts).
        serverId = coordinator.enlistServer(services,
            Context::get().transportManager->getListeningLocatorsString(),
            backupReadSpeed, backupWriteSpeed);

        if (masterService)
            masterService->init(serverId);
        if (backupService)
            backupService->init(serverId);
        if (!disableFailureDetector) {
            failureDetector.construct(
                optionParser.options.getCoordinatorLocator(),
                serverId,
                serverList);
            failureDetector->start();
        }

        while (true) {
            dispatch.poll();
        }

        return 0;
    } catch (std::exception& e) {
        using namespace RAMCloud;
        LOG(ERROR, "Fatal error in server at %s: %s", locator, e.what());
        return 1;
    }
}
