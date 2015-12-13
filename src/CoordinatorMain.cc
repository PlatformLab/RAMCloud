/* Copyright (c) 2010-2015 Stanford University
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

#include "Common.h"
#include "ShortMacros.h"
#include "CoordinatorService.h"
#include "ExternalStorage.h"
#include "MockExternalStorage.h"
#include "OptionParser.h"
#include "PingService.h"
#include "PortAlarm.h"
#include "ServerId.h"
#include "TableManager.h"
#include "TransportManager.h"
#include "WorkerManager.h"

/**
 * \file
 * This file provides the main program for the RAMCloud cluster coordinator.
 */

using namespace RAMCloud;

/**
 * Main program for the RAMCloud cluster coordinator.
 *
 * \param argc
 *      Count of command-line arguments
 * \param argv
 *      Values of commandline arguments
 * \return
 *      Standard return value for an application:  zero mean success,
 *      nonzero means an error occurred.
 */
int
main(int argc, char *argv[])
{
    Logger::installCrashBacktraceHandlers();
    string localLocator("???");
    uint32_t deadServerTimeout;
    uint32_t maxCores;
    bool reset;
    bool neverKill;
    Context context(true);
    CoordinatorServerList serverList(&context);
    try {
        OptionsDescription coordinatorOptions("Coordinator");
        coordinatorOptions.add_options()
            ("deadServerTimeout,d",
             ProgramOptions::value<uint32_t>(&deadServerTimeout)->
                default_value(250),
            "Number of milliseconds to wait for a potentially dead server to "
            "show signs of life before declaring it as crashed. The longer the "
            "timeout, the slower real crashes are responded to. The shorter "
            "the timeout, the greater the chance is of falsely deciding a "
            "machine is down when it's not.")
            ("maxCores",
            ProgramOptions::value<uint32_t>(
                &maxCores)->default_value(4),
             "Limit on number of cores to use for the dispatch and worker "
             "threads. This value should not exceed the number of cores "
             "available on the machine. RAMCloud will try to keep its usage "
             "under this limit, but may occasionally need to exceed it "
             "(e.g., to avoid distributed deadlocks). Th limit does not "
             "include cleaner threads and some other miscellaneous functions.")
            ("neverKill,n",
             ProgramOptions::bool_switch(&neverKill),
             "If specified, the coordinator will never attempt to kill any "
             "master or remove it from the server list.")
            ("reset",
             ProgramOptions::bool_switch(&reset),
             "If specified, the coordinator will not attempt to recover "
             "any existing cluster state; it will start a new cluster "
             "from scratch.");

        OptionParser optionParser(coordinatorOptions, argc, argv);

        // Log all the command-line arguments.
        string args;
        for (int i = 0; i < argc; i++) {
            if (i != 0)
                args.append(" ");
            args.append(argv[i]);
        }
        LOG(NOTICE, "Command line: %s", args.c_str());

        context.workerManager = new WorkerManager(&context, maxCores-1);

        pinAllMemory();
        localLocator = optionParser.options.getCoordinatorLocator();
        context.transportManager->setSessionTimeout(
                optionParser.options.getSessionTimeout());
        context.transportManager->initialize(localLocator.c_str());
        localLocator = context.transportManager->
                                getListeningLocatorsString();
        LOG(NOTICE, "coordinator: Listening on %s", localLocator.c_str());

        // Set PortTimeout and start portTimer
        LOG(NOTICE, "PortTimeOut=%d", optionParser.options.getPortTimeout());
        context.portAlarmTimer->setPortTimeout(
                optionParser.options.getPortTimeout());

        // Connect with the external storage system, and then wait until
        // we have successfully become the "coordinator in charge".
        string externalLocator =
                optionParser.options.getExternalStorageLocator();
        context.externalStorage = ExternalStorage::open(externalLocator,
                &context);
        if (context.externalStorage  == NULL) {
            // Operate without external storage (among other things, this
            // means we won't do any recovery when we start up, and we won't
            // save any information to allow recovery if we crash).
            context.externalStorage = new MockExternalStorage(false);
        }
        string workspace("/ramcloud/");

        string clusterName = optionParser.options.getClusterName();
        workspace.append(clusterName);
        if (reset) {
            LOG(WARNING, "Reset requested: deleting external storage for "
                    "workspace '%s'", workspace.c_str());
            context.externalStorage->remove(workspace.c_str());
        };
        workspace.append("/");
        LOG(NOTICE, "Cluster name is '%s', external storage workspace is '%s'",
                clusterName.c_str(), workspace.c_str());
        context.externalStorage->setWorkspace(workspace.c_str());
        context.externalStorage->becomeLeader("coordinator", localLocator);

        CoordinatorService coordinatorService(&context,
                                              deadServerTimeout,
                                              false,
                                              neverKill);
        PingService pingService(&context);
        while (true) {
            context.dispatch->poll();
        }
        return 0;
    } catch (const std::exception& e) {
        LOG(ERROR, "Fatal error in coordinator at %s: %s",
            localLocator.c_str(), e.what());
        return 1;
    } catch (...) {
        LOG(ERROR, "Unknown fatal error in coordinator at %s",
            localLocator.c_str());
        return 1;
    }
}
