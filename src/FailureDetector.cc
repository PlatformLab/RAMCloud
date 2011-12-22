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
#include "Rpc.h"

namespace RAMCloud {

/**
 * Create a new FailureDetector object. Note that this class depends on the
 * MembershipService running and keeping the global Context::serverList up
 * to date. Without it, we'd not know of new servers to ping.
 *
 * \param[in] coordinatorLocatorString
 *      The ServiceLocator string of the coordinator. 
 * \param[in] ourServerId
 *      The ServerId of this server, as returned by enlistment with the
 *      coordinator. Used only to avoid pinging ourself.
 */
FailureDetector::FailureDetector(const string &coordinatorLocatorString,
                                 const ServerId ourServerId)
    : ourServerId(ourServerId),
      serverTracker(),
      thread(),
      pingClient(),
      coordinatorClient(coordinatorLocatorString.c_str()),
      haveLoggedNoServers(false)
{
    assert(Context::get().serverList != NULL);
    Context::get().serverList->registerTracker(serverTracker);
}

FailureDetector::~FailureDetector()
{
    halt();
    Context::get().serverList->unregisterTracker(serverTracker);
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
    Context::Guard _(*context);

    LOG(NOTICE, "Failure detector thread started");

    while (1) {
        // Check if we have been requested to exit.
        boost::this_thread::interruption_point();

        // Drain the list of changes to update our tracker.
        ServerId dummy1;
        ServerChangeEvent dummy2;
        while (detector->serverTracker.getChange(dummy1, dummy2)) {
        }

        // Ping a random server
        detector->pingRandomServer();

        // Sleep for the specified interval
        usleep(PROBE_INTERVAL_USECS);
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
    if (serverTracker.size() == 0 || (serverTracker.size() == 1 &&
      serverTracker.getRandomServerId() == ourServerId)) {
        // If we have no servers to ping, or we're the only one on the list,
        // then just log that fact the first time and do nothing.
        if (!haveLoggedNoServers) {
            LOG(NOTICE, "No servers besides myself to probe! "
                "List has %u entries.", serverTracker.size());
            haveLoggedNoServers = true;
        }
        return;
    }

    // Reset the haveLoggedNoServers variable
    haveLoggedNoServers = false;

    ServerId pingee = ourServerId;
    while (pingee == ourServerId)
        pingee = serverTracker.getRandomServerId();

    uint64_t nonce = generateRandom();

    string locator;
    try {
        locator = Context::get().serverList->getLocator(pingee);
        pingClient.ping(locator.c_str(), nonce, TIMEOUT_USECS * 1000);
        TEST_LOG("Ping succeeded to server %s", locator.c_str());
    } catch (ServerListException &sle) {
        // This isn't an error. It's just a race between this thread and
        // the membership service. It should be quite uncommon, so just
        // bail on this round and ping again next time.
        LOG(NOTICE, "Tried to ping locator \"%s\", but id %lu was stale",
            locator.c_str(), *pingee);
    } catch (TimeoutException &te) {
        alertCoordinator(pingee, locator);
    } catch (TransportException &te) {
        alertCoordinator(pingee, locator);
    }
}

/**
 * Tell the coordinator that we failed to get a timely ping response.
 *
 * \param serverId
 *      ServerId of the server that is believed to have failed.
 *
 * \param locator
 *      Locator string of the server that is believed to have failed.
 *      Used only for logging purposes.
 */
void
FailureDetector::alertCoordinator(ServerId serverId, string locator)
{
    LOG(WARNING, "Ping timeout to server id %lu (locator \"%s\")",
        *serverId, locator.c_str());
    try {
        coordinatorClient.hintServerDown(serverId);
    } catch (TransportException &te) {
        LOG(WARNING, "Hint server down failed. "
                     "Maybe the network is disconnected: %s", te.what());
    }
}

} // namespace
