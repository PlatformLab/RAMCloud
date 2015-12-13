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

#include "Common.h"
#include "Dispatch.h"
#include "LogProtector.h"

namespace RAMCloud {

std::mutex LogProtector::epochProvidersMutex;
LogProtector::EpochList LogProtector::epochProviders;
uint64_t LogProtector::currentSystemEpoch = 1;

/**
 * Default constructor.
 */
LogProtector::EpochProvider::EpochProvider()
{
    LogProtector::Lock lock(LogProtector::epochProvidersMutex);
    LogProtector::epochProviders.push_back(this);
}

/**
 * Default destructor.
 */
LogProtector::EpochProvider::~EpochProvider()
{
    LogProtector::Lock lock(LogProtector::epochProvidersMutex);
    LogProtector::epochProviders.remove(this);
}

/**
 * Default constructor.
 */
LogProtector::Activity::Activity()
    : EpochProvider()
    , epoch(~0)
    , activityMask(0)
{
}

/**
 * Activity should invoke before it starts touching the log.
 *
 * \param activityMask
 *      A bit mask indicating what sorts of actions are being performed
 *      during this protection period (default: ~0, which means all activities).
 *      Users can provide a more selective value so that
 *      this LogProtector will be ignored in some cases when scanning epochs.
 *      (eg. Transport::READ_ACTIVITY)
 */
void
LogProtector::Activity::start(int activityMask)
{
    this->activityMask = activityMask;
    epoch = LogProtector::getCurrentEpoch();
}

/**
 * Activity should invoke after done with touching the log.
 */
void
LogProtector::Activity::stop()
{
    this->activityMask = 0;
    epoch = ~0;
}

// See EpochProvider for documentation.
uint64_t
LogProtector::Activity::getEarliestEpoch(int activityMask)
{
    if ((this->activityMask & activityMask) != 0) {
        return epoch;
    } else {
        return ~0;
    }
}

//////////////////////////////////////////////////////
/// Static members
//////////////////////////////////////////////////////

/**
 * Obtain the earliest (lowest value) epoch of any ongoing log-touching
 * activities in the system. If there are no eligible log-touching activities,
 * then the return value is -1 (i.e. the largest 64-bit unsigned integer).
 *
 * \param activityMask
 *      A bit mask of flags such as Transport::READ_ACTIVITY. Only
 *      matching activities will be considered.
 */
uint64_t
LogProtector::getEarliestOutstandingEpoch(int activityMask)
{
    Lock listLock(epochProvidersMutex);
    uint64_t earliest = ~0;

    EpochList::iterator it = epochProviders.begin();
    while (it != epochProviders.end()) {
        earliest = std::min((*it)->getEarliestEpoch(activityMask), earliest);
        it++;
    }

    return earliest;
}

/**
 * Return the current epoch.
 */
uint64_t
LogProtector::getCurrentEpoch()
{
    return currentSystemEpoch;
}

/**
 * After some state change, user should increment the current system epoch by
 * invoking this method.
 *
 * \return
 *      The new epoch value.
 */
uint64_t
LogProtector::incrementCurrentEpoch()
{
    // atomically increment the epoch (with a gcc builtin)
    return __sync_add_and_fetch(&LogProtector::currentSystemEpoch, 1);
}

/**
 * Waits for all conflicting activities started before now to finish.
 *
 * \param context
 *      Overall information about the RAMCloud server; used to lock the
 *      dispatcher.
 * \param activityMask
 *      A bit mask of flags such as Transport::READ_ACTIVITY. Waits only
 *      for matching activities to finish.
 */
void
LogProtector::wait(Context* context, int activityMask)
{
    // Increment the current epoch and save the last epoch any
    // currently running RPC could have been a part of
    uint64_t epoch = LogProtector::incrementCurrentEpoch() - 1;

    // Wait for the remainder of already running activities to finish.
    while (true) {
        Dispatch::Lock lock(context->dispatch);
        uint64_t earliestEpoch =
            LogProtector::getEarliestOutstandingEpoch(activityMask);
        if (earliestEpoch > epoch)
            break;
    }
}

} // namespace RAMCloud
