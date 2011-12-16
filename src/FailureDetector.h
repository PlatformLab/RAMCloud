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
 * This class instantiates and manages the failure detector. Once you contruct a
 * FailureDetector you may use start() and halt() to start and stop the
 * FailureDetector thread.
 */
class FailureDetector {
  public:
    FailureDetector(const string &coordinatorLocatorString,
                    ServerId ourServerId);
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

    static void detectorThreadEntry(FailureDetector* detector, Context* ctx);
    void pingRandomServer();
    void alertCoordinator(string locator);

    DISALLOW_COPY_AND_ASSIGN(FailureDetector);
};

} // namespace

#endif // !RAMCLOUD_FAILUREDETECTOR_H
