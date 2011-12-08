/* Copyright (c) 2011 Stanford University
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

#include <errno.h>
#include <fcntl.h>

#include "Common.h"
#include "Cycles.h"
#include "PingClient.h"
#include "CoordinatorClient.h"
#include "FailureDetector.h"
#include "IpAddress.h"
#include "ShortMacros.h"
#include "ProtoBuf.h"
#include "Rpc.h"

namespace RAMCloud {

/**
 * Create a new FailureDetector object.
 *
 * \param[in] coordinatorLocatorString
 *      The ServiceLocator string of the coordinator. 
 * \param[in] listeningLocatorsString
 *      String of ServiceLocators we're listening on. Can be obtained
 *      from TransportMananger via getListeningLocatorsString().
 */
FailureDetector::FailureDetector(const string &coordinatorLocatorString,
     const string &listeningLocatorsString)
    : localLocator(listeningLocatorsString),
      serverList(),
      thread(),
      pingClient(),
      coordinatorClient(coordinatorLocatorString.c_str()),
      haveLoggedNoServers(false)
{
}

FailureDetector::~FailureDetector()
{
    halt();
}

/**
 * Start the failure detector thread.
 *
 * This method starts the failure detector thread and returns immediately. Use
 * the halt method to terminate the failure detector thread. A valid context
 * must be initialized before calling start.
 */
void
FailureDetector::start()
{
    thread.construct(detectorThreadEntry, this, &Context::get());
}

/**
 * Halt the failure detector thread.
 *
 * This method stops the failure detector thread. Once the function returns
 * the thread will have terminated and cleaned up after itself.
 */
void
FailureDetector::halt()
{
    if (thread) {
        thread->interrupt();
        thread->join();
        thread.destroy();
    }
}

/**
 * Main thread entry point for the failure detector. Spin forever, probing 
 * hosts, checkings for responses, and alerting the coordinator of any timeouts.
 *
 * \param detector
 *          FailureDetector passed in from thread creation.
 * \param context
 *          Context object to use for all operations.
 */
void
FailureDetector::detectorThreadEntry(FailureDetector* detector,
                                     Context* context)
{
    int count = 0;
    Context::Guard _(*context);

    LOG(NOTICE, "Failure detector thread started");
    detector->requestServerList();

    while (1) {
        // Check if we have been requested to exit.
        boost::this_thread::interruption_point();

        // Ping a random server
        detector->pingRandomServer();

        // Sleep for the specified interval
        usleep(PROBE_INTERVAL_USECS);

        /*
         * TODO(mashti) this is temporary code to update the server list approximately
         * every one second. Once the creation of the ServerManager that
         * maintains a list of each server in the cluster is complete this will
         * be unneccesary.
         */
        if (count++ > QUERY_SERVER_LIST_INTERVAL) {
            detector->requestServerList();
            count = 0;
        }
    }
}

/**
 * Choose a random server from our list and ping it. Only one oustanding
 * ping is issued at a given time. If a timeout occurs we attempt to notify
 * the coordinator.
 */
void
FailureDetector::pingRandomServer()
{
    if (serverList.server_size() == 0 || (serverList.server_size() == 1 &&
      serverList.server(0).service_locator() == localLocator)) {
        // if we have no servers to ping, or we're the only one on the list,
        // then just log that fact the first time and do nothing.
        if (!haveLoggedNoServers) {
            LOG(NOTICE, "No servers besides myself to probe! "
                "List has %d entries.", serverList.server_size());
            haveLoggedNoServers = true;
        }
        return;
    }

    // Reset the haveLoggedNoServers variable
    haveLoggedNoServers = false;

    const string* locator = &localLocator;
    while (*locator == localLocator) {
        uint32_t index = downCast<uint32_t>(generateRandom() %
                                            serverList.server_size());
        locator = &serverList.server(index).service_locator();
    }

    uint64_t nonce = generateRandom();

    try {
        pingClient.ping(locator->c_str(), nonce, TIMEOUT_USECS * 1000);
        TEST_LOG("Ping succeeded to server %s", locator->c_str());
    } catch (TimeoutException &te) {
        alertCoordinator(*locator);
    } catch (TransportException &te) {
        alertCoordinator(*locator);
    }
}

/**
 * Tell the coordinator that we failed to get a timely ping response.
 *
 * \param locator
 *          Locator string of the server that is believed to have failed.
 */
void
FailureDetector::alertCoordinator(string locator)
{
    LOG(WARNING, "Ping timeout to server %s", locator.c_str());
    try {
        coordinatorClient.hintServerDown(locator);
    } catch (TransportException &te) {
        LOG(WARNING, "Hint server down failed. "
                     "Maybe the network is disconnected: %s", te.what());
    }
}

/**
 * Request the list of servers from the Coordinator.
 *
 * TODO(mashti): Will change this code to use the ClusterManager
 */
void
FailureDetector::requestServerList()
{
    LOG(DEBUG, "requesting server list");

    try {
        coordinatorClient.getServerList(serverList);
    } catch (TransportException &te) {
        LOG(WARNING, "Failed to update the server list from the coordinator: "
                     "%s", te.what());
    }
}

} // namespace
