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

#ifndef RAMCLOUD_FAILUREDETECTOR_H
#define RAMCLOUD_FAILUREDETECTOR_H

#include <list>

#include "Common.h"
#include "PingClient.h"
#include "Rpc.h"
#include "ServiceLocator.h"
#include "ServerId.h"
#include "ServerList.h"
#include "ServerTracker.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * This class instantiates and manages the failure detector. Each RAMCloud
 * server should have an instantiation of this class that randomly pings
 * other servers in the cluster. If any pings time out, the coordinator
 * is warned of a possible failure via the HintServerDown RPC. It is then
 * up to the coordinator to make a diagnosis. This class simply reports
 * possible symptoms that it sees.
 *
 * Once you contruct a FailureDetector you may use start() and halt() to
 * start and stop the FailureDetector thread.
 */
class FailureDetector {
  public:
    FailureDetector(const string &coordinatorLocatorString,
                    ServerId ourServerId,
                    ServerList& serverList);
    ~FailureDetector();
    void start();
    void halt();

  PRIVATE:
    /// Number of microseconds between probes.
    static const int PROBE_INTERVAL_USECS = 100 * 1000;

    /**
     * Number of microseconds before a probe is considered to have timed out.
     * Some machines have be known to freeze for approximately 250ms, but this
     * threshold is intentionally smaller. We allow the coordinator to try again
     * with a longer timeout for those false-positives. If the coordinator ends
     * up becoming a bottleneck we may need to increase this timeout and move to
     * an asynchronous model.
     */
    static const int TIMEOUT_USECS = 50 * 1000;

    /**
     * Number of microseconds to wait before our ServerList is considered stale.
     * If we see a newer ServerList version (returned in a ping response), then
     * we will request that the coordinator re-send the list if ours does not
     * update within this timeout period. This ensures that our list does not
     * stay out of date if we happen to miss an update (and no further updates
     * are issued in the meantime that would have otherwise alerted us).
     */    
    static const int STALE_SERVER_LIST_USECS = 500 * 1000;

    static_assert(TIMEOUT_USECS <= PROBE_INTERVAL_USECS,
                  "Timeout us should be less than probe interval.");

    /// Our ServerId (used to avoid pinging oneself).
    const ServerId       ourServerId;

    /// ServerTracker used for obtaining random servers to ping. Nothing is
    /// currently stored with servers in the tracker.
    ServerTracker<bool>  serverTracker;

    /// Failure detector thread
    Tub<boost::thread>   thread;

    // Service Clients
    /// PingClient instance
    PingClient           pingClient;

    /// CoordinatorClient instance
    CoordinatorClient    coordinatorClient;

    /**
     * This variable is used to prevent excessive logging when we detect that
     * there are no other servers on the list. We only allow ourselves to log
     * once when we detect this condition, and we reset the variable once we see
     * other servers on the list.
     */
    bool                 haveLoggedNoServers;

    /// ServerList whose consistency we will check against random nodes that
    /// we ping.
    ServerList&          serverList;

    /// If true, we suspect that our ServerList is out of date and are waiting
    /// for STALE_SERVER_LIST_USECS to expire to request a new list.
    bool                 staleServerListSuspected;

    /// If staleServerListSuspected is true, this is the version of the list
    /// at the time we began suspecting that it was stale.
    uint64_t             staleServerListVersion;

    /// If staleServerListSuspected is true, this is the CPU timestamp counter
    /// at the point when we began suspecting the list was stale.
    uint64_t             staleServerListTimestamp;

    static void detectorThreadEntry(FailureDetector* detector, Context* ctx);
    void pingRandomServer();
    void alertCoordinator(ServerId serverId, string locator);
    void checkServerListVersion(uint64_t observedVersion);
    void checkForStaleServerList();

    DISALLOW_COPY_AND_ASSIGN(FailureDetector);
};

} // namespace

#endif // !RAMCLOUD_FAILUREDETECTOR_H
