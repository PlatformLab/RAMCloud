/* Copyright (c) 2014-2015 Stanford University
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

#include "ClusterTime.h"
#include "Context.h"
#include "Cycles.h"
#include "ExternalStorage.h"
#include "WorkerTimer.h"

namespace RAMCloud {

/**
 * The CoordinatorClusterClock, a module that runs on the coordinator, controls
 * the progression of Cluster Time and maintains the following properties:
 *  (1) The clock advances monotonically over the entire life of the cluster
 *      including across coordinator crashes.
 *  (2) The clock advances in sync with the coordinator's Cycles::rdtsc() during
 *      normal operations (see getTime() for degraded behavior).
 *
 * CoordinatorClusterClock is thread-safe.
 */
class CoordinatorClusterClock {
  PUBLIC:
    explicit CoordinatorClusterClock(Context *context);
    ClusterTime getTime();
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

    /// System time of the coordinator when the clock is initialized.  Used to
    /// calculate current cluster time.
    const uint64_t startingSysTimeNS;

    /// Recovered safeClusterTime from externalStorage when the clock is
    /// initialized (may be zero if cluster is new).  Used to calculate current
    /// cluster time.
    const ClusterTime startingClusterTime;

    /// The last cluster time stored in externalStorage.  Represents the
    /// largest cluster time that may be returned from getTime().
    ClusterTime safeClusterTime;

    SafeTimeUpdater updater;

    ClusterTime getInternal();
    static uint64_t recoverClusterTime(ExternalStorage* externalStorage);

    DISALLOW_COPY_AND_ASSIGN(CoordinatorClusterClock);
};

/**
 * Constants used by the CoordinatorClusterClock
 */
namespace CoordinatorClusterClockConstants {
/// Duration to advance the safeClusterTime stored in external storage.
/// This value should be much larger than the time to perform the external
/// storage  write (~10ms) but much much less than the max value (2^63 - 1).
static CONSTEXPR_VAR ClusterTimeDuration safeTimeInterval =
        ClusterTimeDuration::fromNanoseconds(3 * 1e9);  // 3 seconds

/// Amount of time (in seconds) between updates of the safeClusterTime to
/// externalStorage.  This time should be less than the safeTimeIntervalMs;
/// we recommend a value equivalent to half the safeTimeIntervalMs.
static CONSTEXPR_VAR double updateIntervalS = 1.5;          // 1.5 seconds
}

} // namespace RAMCloud

#endif  /* RAMCLOUD_COORDINATORCLUSTERCLOCK_H */

