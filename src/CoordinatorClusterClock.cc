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

#include <string>

#include "CoordinatorClusterClock.h"
#include "CoordinatorClusterClock.pb.h"
#include "ShortMacros.h"
#include "Util.h"

namespace RAMCloud {

/**
 * Constructor for the CoordinatorClusterClock.
 *
 * \param context
 *      Overall information about the RAMCloud server and provides access to
 *      externalStorage (externalStorage must be non-null).
 */
CoordinatorClusterClock::CoordinatorClusterClock(Context *context)
    : startingSysTimeNS(Cycles::toNanoseconds(Cycles::rdtsc()))
    , startingClusterTime(recoverClusterTime(context->externalStorage))
    , safeClusterTime(startingClusterTime)
    , updater(context, this)
{}

/**
 * Returns the current cluster time.  Repeated calls to this method are
 * guaranteed to return monotonically non-decreasing cluster times, even across
 * Coordinator crashes.  The cluster time is expected to advance in microseconds
 * roughly at the same rate as the coordinator system clock. In rare cases (e.g.
 * when updates to externalStorage take a long time), the clock may stall
 * causing this method to return the same value for an extended period of time.
 *
 * This method is thread-safe.
 */
ClusterTime
CoordinatorClusterClock::getTime()
{
    ClusterTime time = getInternal();

    // Cache current safe time for isolation; we want to avoid a time of check
    // vs time of use problem since the safe cluster time may change while this
    // method executes.
    ClusterTime currentSafeTime = safeClusterTime;
    // In the unlikely event that the current time exceeds the safe time,
    // return the safe time so that an unsafe time can never be observed.
    if (expect_false(time > currentSafeTime)) {// Not sure predictor helps.
        RAMCLOUD_CLOG(WARNING, "Returning stale time. "
                               "SafeTimeUpdater may be running behind.");
        return currentSafeTime;
    }
    return time;
}

/**
 * Start the SafeTimeUpdater.  Must be called to ensure the clock will advance.
 * Separated out mostly for ease of testing.
 */
void
CoordinatorClusterClock::startUpdater()
{
    updater.start(0);
}

/**
 * Constructor for the SafeTimeUpdater.
 *
 * \param context
 *      Overall information about the RAMCloud server and provides access to
 *      externalStorage.
 * \param clock
 *      Provides access to the cluster clock to update the safeClusterTime.
 */
CoordinatorClusterClock::SafeTimeUpdater::SafeTimeUpdater(
        Context* context, CoordinatorClusterClock* clock)
    : WorkerTimer(context->dispatch)
    , externalStorage(context->externalStorage)
    , clock(clock)
{}

/**
 * This handler advances the safeClusterTime stored on external storage and
 * updates the CoordinatorClusterClock's safeClusterTime once successful. This
 * handler is called when the SafeTimeUpdater timer expires (i.e. once every
 * updateIntervalCycles).
 */
void
CoordinatorClusterClock::SafeTimeUpdater::handleTimerEvent()
{
    using CoordinatorClusterClockConstants::safeTimeInterval;
    using CoordinatorClusterClockConstants::updateIntervalS;

    uint64_t startTimeCycles = Cycles::rdtsc();
    ClusterTime nextSafeTime = clock->getInternal() + safeTimeInterval;
    ProtoBuf::CoordinatorClusterClock info;
    info.set_next_safe_time(nextSafeTime.getEncoded());
    std::string str;
    info.SerializeToString(&str);
    externalStorage->set(ExternalStorage::Hint::UPDATE,
                         "coordinatorClusterClock",
                         str.c_str(),
                         downCast<int>(str.length()));

    clock->safeClusterTime = nextSafeTime;
    this->start(startTimeCycles + Cycles::fromSeconds(updateIntervalS));
}

/**
 * Returns the raw, calculated, unprotected cluster time.  Should not be used
 * by external methods.
 */
ClusterTime
CoordinatorClusterClock::getInternal()
{
    uint64_t currentSysTimeNS = Cycles::toNanoseconds(Cycles::rdtsc());
    ClusterTimeDuration duration =
            ClusterTimeDuration::fromNanoseconds(currentSysTimeNS
                                                 - startingSysTimeNS);
    return startingClusterTime + duration;
}

/**
 * Recovers and returns the last stored safeClusterTime from externalStorage.
 * Used during the construction of the cluster clock to initialize the
 * startingClusterTime.
 *
 * \param externalStorage
 *      Pointer to the externalStorage module from which to recover.
 *
 * \return
 *      The last stored safeClusterTime or zero if none exists.
 */
uint64_t
CoordinatorClusterClock::recoverClusterTime(ExternalStorage* externalStorage)
{
    uint64_t startingClusterTime = ClusterTime().getEncoded();
    // Recover any previously persisted safe cluster time. Cluster time starts
    // at zero if no persisted time is found.
    ProtoBuf::CoordinatorClusterClock info;
    if (externalStorage->getProtoBuf("coordinatorClusterClock", &info)) {
        startingClusterTime = info.next_safe_time();
    } else {
        LOG(WARNING, "couldn't find \"coordinatorClusterClock\" object in "
                "external storage; starting new clock from zero; benign if "
                "starting new cluster from scratch, may cause linearizability "
                "failures otherwise");
    }

    LOG(NOTICE,
        "initializing CoordinatorClusterClock: startingClusterTime = %lu",
        startingClusterTime);

    return startingClusterTime;
}

} // namespace RAMCloud
