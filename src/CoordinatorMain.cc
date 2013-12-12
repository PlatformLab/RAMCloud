/* Copyright (c) 2010-2013 Stanford University
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
#include "MockExternalStorage.h"
#include "OptionParser.h"
#include "PingService.h"
#include "PortAlarm.h"
#include "ServerId.h"
#include "ServiceManager.h"
#include "TableManager.h"
#include "TransportManager.h"
#include "ZooStorage.h"

/**
 * \file
 * This file provides the main program for the RAMCloud cluster coordinator.
 */

using namespace RAMCloud;

// Forward declaration.
extern void finishInitialization(Context* context, string* localLocator);

// Variables shared between main and finishInitialization.
static Tub<CoordinatorService> coordinatorService;
static uint32_t deadServerTimeout;

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
    string clusterName;
    string localLocator("???");
    bool reset;
    Context context(true);
    CoordinatorServerList serverList(&context);
    try {
        OptionsDescription coordinatorOptions("Coordinator");
        coordinatorOptions.add_options()
            ("clusterName",
             ProgramOptions::value<string>(&clusterName)->
                default_value("main"),
             "Name of the cluster. Among other things, used as a prefix for "
             "all names in external storage, so that different clusters can "
             "coexist in the same storage server.")
            ("deadServerTimeout,d",
             ProgramOptions::value<uint32_t>(&deadServerTimeout)->
                default_value(250),
            "Number of milliseconds to wait for a potentially dead server to "
            "show signs of life before declaring it as crashed. The longer the "
            "timeout, the slower real crashes are responded to. The shorter "
            "the timeout, the greater the chance is of falsely deciding a "
            "machine is down when it's not.")
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
        if (externalLocator.find("zk:") == 0) {
            string zkInfo = externalLocator.substr(3);
            context.externalStorage = new ZooStorage(zkInfo, context.dispatch);
        } else {
            // Operate without external storage (among other things, this
            // means we won't do any recovery when we start up, and we won't
            // save any information to allow recovery if we crash).
            context.externalStorage = new MockExternalStorage(false);
        }
        string workspace("/ramcloud/");
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
        PingService pingService(&context);
        context.serviceManager->addService(pingService,
                                           WireFormat::PING_SERVICE);

        std::thread(finishInitialization, &context, &localLocator).detach();

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

/**
 * This method is invoked in a separate thread to perform the final
 * pieces of initialization. It must run in his own thread, because it
 * requires facilities provided by the dispatcher, which is run by
 * main.
 *
 * \param context
 *      Overall information about this server.
 * \param localLocator
 *      Locator that other machines can use to connect to this coordinator.
 */
void
finishInitialization(Context* context, string* localLocator) {
    context->externalStorage->becomeLeader("coordinator", *localLocator);
    coordinatorService.construct(context, deadServerTimeout);
    context->serviceManager->addService(*coordinatorService,
                                        WireFormat::COORDINATOR_SERVICE);
}
