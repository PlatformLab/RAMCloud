/* Copyright (c) 2009-2013 Stanford University
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

#include "Context.h"
#include "CoordinatorSession.h"
#if INFINIBAND
#include "InfRcTransport.h"
#endif
#include "OptionParser.h"
#include "PortAlarm.h"
#include "Server.h"
#include "ShortMacros.h"
#include "TransportManager.h"

int
main(int argc, char *argv[])
{
    using namespace RAMCloud;
    Logger::installCrashBacktraceHandlers();
    Context context(true);
    try {
        ServerConfig config = ServerConfig::forExecution();
        string masterTotalMemory, hashTableMemory;

        bool masterOnly;
        bool backupOnly;

        OptionsDescription serverOptions("Server");
        serverOptions.add_options()
            ("backupInMemory,m",
             ProgramOptions::bool_switch(&config.backup.inMemory),
             "Backup will store segment replicas in memory")
            ("backupOnly,B",
             ProgramOptions::bool_switch(&backupOnly),
             "The server should run the backup service only (no master)")
            ("backupStrategy",
             ProgramOptions::value<int>(&config.backup.strategy)->
               default_value(RANDOM_REFINE_AVG),
             "0 random refine min, 1 random refine avg, 2 even distribution, "
             "3 uniform random")
            ("cleanerBalancer",
             ProgramOptions::value<string>(&config.master.cleanerBalancer)->
                default_value("tombstoneRatio:0.40"),
             "Which balancing algorithm to use to schedule cleaning on disk "
             "and in-memory compaction, as well as how to orchestrate multiple "
             "cleaner threads. You will almost certainly want to use the "
             "default value. Currently the only other option is \"fixed:X\", "
             "where 0 <= X <= 100 represents the percentage of CPU time the "
             "disk cleaner will be limited to (the rest is for compaction).")
            ("disableLogCleaner,d",
             ProgramOptions::bool_switch(&config.master.disableLogCleaner),
             "Disable the log cleaner entirely. You will eventually run out "
             "of memory, but at least you can do so faster this way.")
            ("disableInMemoryCleaning,D",
             ProgramOptions::bool_switch(
                &config.master.disableInMemoryCleaning),
             "Disable the in-memory cleaning portion of the log cleaner. When "
             "turned off, the cleaner will always clean both in memory and on "
             "backup disks at the same time.")
            ("diskExpansionFactor,E",
             ProgramOptions::value<double>(&config.master.diskExpansionFactor)->
                default_value(2.0),
             "Factor (>= 1.0) controlling how much backup disk space this "
             "master will use beyond its in-memory log size. For example, if "
             "the master has memory for 100 full segments and the expansion "
             "factor is 2.0, it will place up to 200 segments (each replicated "
             "R times) on backups.")
            ("file,f",
             ProgramOptions::value<string>(&config.backup.file)->
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

             // Note: we have tried changing the default value below to
             // something that would make sense in production (80%?), but
             // as of 6/2014 that causes too many problems in test and
             // development environments (e.g. hooks/pre-commit will take
             // forever, and if two people do this, rcmaster will run out
             // of memory).
             ProgramOptions::value<string>(&masterTotalMemory)->
                default_value("500"),
             "Percentage or megabytes of system memory for master log & "
             "hash table")
            ("replicas,r",
             ProgramOptions::value<uint32_t>(&config.master.numReplicas)->
                default_value(0),
             "Number of backup copies to make for each segment")
            ("useMinCopysets",
             ProgramOptions::value<bool>(&config.master.useMinCopysets)->
                default_value(false),
             "Whether to use MinCopysets or random replication")
            ("allowLocalBackup",
             ProgramOptions::bool_switch(&config.master.allowLocalBackup),
             "Allow replication to local backup")
            ("segmentFrames",
             ProgramOptions::value<uint32_t>(&config.backup.numSegmentFrames)->
                default_value(512),
             "Number of segment frames in backup storage")
            ("maxNonVolatileBuffers",
             ProgramOptions::value<uint32_t>(
               &config.backup.maxNonVolatileBuffers)->default_value(0),
             "Maximum number of segments the backup will buffer in memory. The "
             "0 value (default) is special: it tells the server to set the "
             "limit equal to the \"segmentFrames\" value, effectively making "
             "buffering unlimited.")
            ("detectFailures",
             ProgramOptions::value<bool>(&config.detectFailures)->
                default_value(true),
             "Whether to use the randomized failure detector")
            ("writeCostThreshold,w",
             ProgramOptions::value<uint32_t>(
                &config.master.cleanerWriteCostThreshold)->default_value(8),
             "If in-memory cleaning is enabled, do disk cleaning when the "
             "write cost of memory freed by the in-memory cleaner exceeds "
             "this value. Lower values cause the disk cleaner to run more "
             "frequently. Higher values do more in-memory cleaning and "
             "reduce the amount of backup disk bandwidth used during disk "
             "cleaning.")
            ("sync",
             ProgramOptions::bool_switch(&config.backup.sync),
             "Make all updates completely synchronous all the way down to "
             "stable storage.")
            ("masterServiceThreads",
             ProgramOptions::value<uint32_t>(
                &config.master.masterServiceThreadCount)->default_value(5),
             "The number of threads in MasterService determines the maximum "
             "number of client RPCs that may be processed in parallel. "
             "Increasing this value will use more cores and may improve client "
             "throuphput, especially for reads.")
            ("logCleanerThreads",
             ProgramOptions::value<uint32_t>(
                &config.master.cleanerThreadCount)->default_value(1),
             "The number of cleaner threads controls the amount of parallelism "
             "in the cleaner. More threads will use more cores, but may be "
             "able to better keep up with high write rates.")
            ("backupWriteRateLimit",
             ProgramOptions::value<size_t>(
                &config.backup.writeRateLimit)->default_value(0),
             "If non-0, specifies the maximum number of megabytes per second "
             "of bandwidth this backup should use. Useful for artificially "
             "restricting bandwidth when measuring various parts of the "
             "system.");

        OptionParser optionParser(serverOptions, argc, argv);

        // Log all the command-line arguments.
        string args;
        for (int i = 0; i < argc; i++) {
            if (i != 0)
                args.append(" ");
            args.append(argv[i]);
        }
        LOG(NOTICE, "Command line: %s", args.c_str());

        if (masterOnly && backupOnly)
            DIE("Can't specify both -B and -M options");

        if (masterOnly) {
            config.services = {WireFormat::MASTER_SERVICE,
                               WireFormat::MEMBERSHIP_SERVICE,
                               WireFormat::PING_SERVICE};
        } else if (backupOnly) {
            config.services = {WireFormat::BACKUP_SERVICE,
                               WireFormat::MEMBERSHIP_SERVICE,
                               WireFormat::PING_SERVICE};
        } else {
            config.services = {WireFormat::MASTER_SERVICE,
                               WireFormat::BACKUP_SERVICE,
                               WireFormat::MEMBERSHIP_SERVICE,
                               WireFormat::PING_SERVICE};
        }

        const string localLocator = optionParser.options.getLocalLocator();

#if INFINIBAND
        InfRcTransport::setName(localLocator.c_str());
#endif
        context.transportManager->setSessionTimeout(
                optionParser.options.getSessionTimeout());
        context.transportManager->initialize(localLocator.c_str());
        // Transports may augment the local locator somewhat.
        // Make sure the server is aware of that augmented locator.
        config.localLocator =
            context.transportManager->getListeningLocatorsString();
        LOG(NOTICE, "%s: Listening on %s",
            config.services.toString().c_str(), config.localLocator.c_str());

        config.coordinatorLocator =
            optionParser.options.getExternalStorageLocator();
        if (config.coordinatorLocator.size() == 0) {
            config.coordinatorLocator =
                optionParser.options.getCoordinatorLocator();
        }
        config.clusterName = optionParser.options.getClusterName();
        context.coordinatorSession->setLocation(
                config.coordinatorLocator.c_str(),
                config.clusterName.c_str());

        if (!backupOnly) {
            LOG(NOTICE, "Using %u backups", config.master.numReplicas);
            config.setLogAndHashTableSize(masterTotalMemory, hashTableMemory);
        }

        // Set PortTimeout and start portTimer
        LOG(NOTICE, "PortTimeOut=%d", optionParser.options.getPortTimeout());
        context.portAlarmTimer->setPortTimeout(
            optionParser.options.getPortTimeout());

        Server server(&context, &config);
        server.run(); // Never returns except for exceptions.

        return 0;
    } catch (const Exception& e) {
        LOG(ERROR, "Fatal error in server at %s: %s",
            context.transportManager->getListeningLocatorsString().c_str(),
            e.what());
        return 1;
    } catch (const std::exception& e) {
        LOG(ERROR, "Fatal error in server at %s: %s",
            context.transportManager->getListeningLocatorsString().c_str(),
            e.what());
        return 1;
    } catch (...) {
        LOG(ERROR, "Unknown fatal error in server at %s",
            context.transportManager->getListeningLocatorsString().c_str());
        return 1;
    }
}
