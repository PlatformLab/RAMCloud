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

#include <string>

#include "CoordinatorClusterClock.h"
#include "CoordinatorClusterClock.pb.h"
#include "ShortMacros.h"
#include "Util.h"

namespace RAMCloud {

/**
 * Constructor for the CoordinatorClusterClock. See the comments near the
 * declarations for important information about the relationship between
 * the set constants.
 *
 * \param context
 *      Overall information about the RAMCloud server and provides access to
 *      externalStorage (externalStorage must be non-null).
 */
CoordinatorClusterClock::CoordinatorClusterClock(Context *context)
    : mutex()
    , startingSysTimeMs(Cycles::toMicroseconds(Cycles::rdtsc()))
    , startingClusterTimeMs(recoverClusterTime(context->externalStorage))
    , safeClusterTimeMs(startingClusterTimeMs)
    , updater(context, this)
{
    updater.start(0);
}

/**
 * Returns the current cluster time.  The cluster time is expected to advance
 * in milliseconds roughly at the same rate as the coordinator system clock.
 * In rare cases (e.g. when updates to externalStorage take a long time), the
 * clock may stall and this get method will return a "stale" time.  This method
 * is thread-safe.
 */
uint64_t
CoordinatorClusterClock::getTime()
{
    Lock lock(mutex);
    uint64_t time = getInternal(lock);
    // In the unlikely event that the current time exceeds the safe time,
    // return the safe time so that an unsafe time can never be observed.
    if (expect_false(time > safeClusterTimeMs)) {// Not sure predictor helps.
        return safeClusterTimeMs;
    }
    return time;
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
    CoordinatorClusterClock::Lock lock(clock->mutex);
    uint64_t nextSafeTimeMs = clock->getInternal(lock)
                              + clock->safeTimeIntervalMs;
    ProtoBuf::CoordinatorClusterClock info;
    info.set_next_safe_time(nextSafeTimeMs);
    std::string str;
    info.SerializeToString(&str);
    externalStorage->set(ExternalStorage::Hint::UPDATE,
                         "coordinatorClusterClock",
                         str.c_str(),
                         downCast<int>(str.length()));

    clock->safeClusterTimeMs = nextSafeTimeMs;
    this->start(Cycles::rdtsc() + Cycles::fromSeconds(clock->updateIntervalS));
}

/**
 * Returns the raw, calculated, unprotected cluster time.  Should not be used
 * by external methods.
 *
 * \param lock
 *      Ensures that caller has acquired mutex; not actually used here.
 */
uint64_t
CoordinatorClusterClock::getInternal(Lock &lock)
{
    uint64_t currentSysTimeMs = Cycles::toMicroseconds(Cycles::rdtsc());
    return (currentSysTimeMs - startingSysTimeMs) + startingClusterTimeMs;
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
    uint64_t startingClusterTime = 0;
    // Recover any previously persisted safe cluster time. Cluster time starts
    // at zero if no persisted time is found.
    ProtoBuf::CoordinatorClusterClock info;
    if (externalStorage->getProtoBuf<ProtoBuf::CoordinatorClusterClock>(
            "coordinatorClusterClock", &info)) {
        startingClusterTime = info.next_safe_time();
    }

    LOG(NOTICE,
        "initializing CoordinatorClusterClock: startingClusterTime = %lu",
        startingClusterTime);

    return startingClusterTime;
}

} // namespace RAMCloud
