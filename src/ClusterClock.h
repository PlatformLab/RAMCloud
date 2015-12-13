/* Copyright (c) 2015 Stanford University
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

#ifndef RAMCLOUD_CLUSTERCLOCK_H
#define RAMCLOUD_CLUSTERCLOCK_H

#include "Minimal.h"
#include "ClusterTime.h"

namespace RAMCloud {

/**
 * The ClusterClock is used by masters to keep track of this observed "current"
 * cluster-time.  A master's observed "current" cluster-time is defined as the
 * largest cluster-time observed by the master.  The "current" cluster-time is
 * used to logically serialize lease expiration with lease information removal.
 */
class ClusterClock {
  PUBLIC:
    /**
     * Default constructor.
     */
    ClusterClock()
        : clusterTime()
    {}

    /**
     * Return the "current" cluster-time (as observed by this module).
     */
    ClusterTime getTime() {
        return clusterTime;
    }

    /**
     * Provide an observed cluster-time to the ClusterClock to keep the
     * "current" cluster-time updated.  This method should be called every time
     * a master receives a cluster-time.
     *
     * \param observedTime
     *      A cluster-time from a client, a different master, or the coordinator
     *      providing a lower bound on the "current" cluster-time.  When this
     *      method returns, all subsequent calls to getTime will return a
     *      time at least as large this observedTime.
     */
    void updateClock(ClusterTime observedTime) {
        ClusterTime currentClusterTime = clusterTime;
        while (currentClusterTime < observedTime) {
            currentClusterTime =
                    clusterTime.compareExchange(currentClusterTime,
                                                observedTime);
        }
    }

  PRIVATE:
    /// The largest cluster-time observed by this module.
    ClusterTime clusterTime;

    DISALLOW_COPY_AND_ASSIGN(ClusterClock);
};

} // namespace RAMCloud

#endif // RAMCLOUD_CLUSTERCLOCK_H
