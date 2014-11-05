/* Copyright (c) 2014 Stanford University
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

#ifndef RAMCLOUD_COORDINATORCLUSTERCLOCK_H
#define RAMCLOUD_COORDINATORCLUSTERCLOCK_H

#include "Common.h"
#include "Context.h"
#include "Cycles.h"
#include "ExternalStorage.h"
#include "WorkerTimer.h"

namespace RAMCloud {

/**
 * The CoordinatorClusterClock controls the progression of Cluster Time and
 * maintains the follow properties:
 *  (1) The clock advances monotonically over the entire life of the cluster
 *      including across coordinator crashes.
 *  (2) The clock advances in sync with the coordinator's Cycles::rdtsc() during
 *      normal operations (see getTime() for degraded behavior).
 *
 * CoordinatorClusterClock, in conjuction with ClusterClock, maintains (1)
 * across all ClusterClocks by controlling what time values to externalize.
 * Non-coordinator server ClusterClocks can only take on values that have been
 * externalized by the CoordinatorClusterClock through a getTime() call; Cluster
 * Time as read from ClusterClocks on non-coordinator servers is always less
 * than or equal to the Cluster Time read from the CoordinatorClusterClock.
 *
 * CoordinatorClusterClock is thread-safe.
 */
class CoordinatorClusterClock {
  PUBLIC:
    explicit CoordinatorClusterClock(Context *context);
    uint64_t getTime();
    void startUpdater();

  PRIVATE:
    /**
     * The SafeTimeUpdater maintains a time on external storage that is
     * guaranteed to be greater than any clock value that this module has ever
     * externalized to anyone, so that in the event of a coordinator crash, the
     * new coordinator can ensure the monotonic property of cluster time.
     */
    class SafeTimeUpdater : public WorkerTimer {
      public:
        explicit SafeTimeUpdater(Context* context,
                                 CoordinatorClusterClock* clock);
        virtual void handleTimerEvent();

        ExternalStorage* externalStorage;
        CoordinatorClusterClock* clock;
      private:
        DISALLOW_COPY_AND_ASSIGN(SafeTimeUpdater);
    };

    /// Monitor-style lock: Protects against concurrent access to internal
    /// variables to provide thread-safety between access to getTime() and from
    /// the SafeTimeUpdater.
    SpinLock mutex;
    typedef std::lock_guard<SpinLock> Lock;

    /// Amount of time (in microseconds) to advance the safeClusterTime stored
    /// in external storage.  This value should be much larger than the time
    /// to perform the external storage write (~10ms) but much much less than
    /// the max value (2^64 - 1).
    static const uint64_t safeTimeIntervalUs = 3 * 1e6; // 3 seconds

    /// Amount of time (in seconds) between updates of the safeClusterTime to
    /// externalStorage.  This time should be less than the safeTimeIntervalMs;
    /// we recommend a value equivalent to half the safeTimeIntervalMs.
    static const double updateIntervalS = 1.5;

    /// System time of the coordinator when the clock is initialized.  Used to
    /// calculate current cluster time.
    const uint64_t startingSysTimeUs;

    /// Recovered safeClusterTime from externalStorage when the clock is
    /// initialize (may be zero if cluster is new).  Used to calculate current
    /// cluster time.
    const uint64_t startingClusterTimeUs;

    /// The last cluster time stored in externalStorage.  Represents the
    /// largest cluster time that is safe to externalize (see "getTime()").
    uint64_t safeClusterTimeUs;

    SafeTimeUpdater updater;

    uint64_t getInternal(Lock &lock);
    static uint64_t recoverClusterTime(ExternalStorage* externalStorage);

    DISALLOW_COPY_AND_ASSIGN(CoordinatorClusterClock);
};

} // namespace RAMCloud

#endif  /* RAMCLOUD_COORDINATORCLUSTERCLOCK_H */

