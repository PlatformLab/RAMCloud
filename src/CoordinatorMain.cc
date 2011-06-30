/* Copyright (c) 2010 Stanford University
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
#include "CoordinatorService.h"
#include "OptionParser.h"
#include "PingService.h"
#include "ServiceManager.h"
#include "TransportManager.h"

/**
 * \file
 * This file provides the main program for the RAMCloud cluster coordinator.
 */

int
main(int argc, char *argv[])
{
    using namespace RAMCloud;
    try {
        OptionParser optionParser(OptionsDescription("Coordinator"),
                                  argc, argv);
        string localLocator = optionParser.options.getCoordinatorLocator();
        transportManager.initialize(localLocator.c_str());
        localLocator = transportManager.getListeningLocatorsString();
        LOG(NOTICE, "coordinator: Listening on %s", localLocator.c_str());
        CoordinatorService coordinatorService;
        serviceManager->addService(coordinatorService, COORDINATOR_SERVICE);
        PingService pingService;
        serviceManager->addService(pingService, PING_SERVICE);
        while (true) {
            dispatch->poll();
        }
        return 0;
    } catch (RAMCloud::Exception& e) {
        LOG(ERROR, "coordinator: %s", e.str().c_str());
        return 1;
    }
}
