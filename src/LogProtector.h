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

#ifndef RAMCLOUD_LOGPROTECTOR_H
#define RAMCLOUD_LOGPROTECTOR_H

#include <list>
#include "Common.h"

namespace RAMCloud {

/**
 * A mechanism to protect accesses to RAMCloud log from system state changes
 * which may cause log inaccessible or cause data race.
 *
 * We use epoch to track starting time of log-accessing acitivities. Epochs are
 * just monotonically increasing values that represent some point in time.
 * All activities touching log are tagged with currentSystemEpoch, and those
 * activities can be queried to find the oldest activity still being processed.
 * Epochs are a coarse-grained means to determining when all activities
 * encountered after some point in time have finished.
 *
 * To prevent causing log inaccessible or data race, system state changes
 * involving log may use this epoch mechanism by waiting for all log-accessing
 * activities started before the state changes. For example, before freeing
 * memory used for an old segment (no current hash table entry points to the
 * segment), log cleaner waits for all currently ongoing log activities to
 * finish. This "waiting" allows read operations to safely de-reference
 * log references to the old segment which was obtained before log cleaning.
 *
 * This class is a wrapper for other classes related to the log protection
 * mechanism. It has static members only to track global state.
 */
class LogProtector {
  public:
    /**
     * Interface for anything that can return epoch value used for log protection.
     */
    class EpochProvider {
      public:
        EpochProvider();
        virtual ~EpochProvider() = 0;

        /**
         * Gets the earliest start time of any currently executing activity
         * associated with this object.
         *
         * \param activityMask
         *      A bit mask of activity flags such as Transport::READ_ACTIVITY.
         * \return
         *      Earliest start time of the activities that match the
         *      activityMask. If no matching activity is currently happening,
         *      we return the maximum number, ~0.
         */
        virtual uint64_t getEarliestEpoch(int activityMask) = 0;
    };

    /**
     * Create this instance to work on log. While Activity is on, no system
     * state change with conflicting modification on log is allowed.
     */
    class Activity : public EpochProvider {
      public:
        Activity();
        void start(int activityMask = ~0);
        void stop();
        virtual uint64_t getEarliestEpoch(int activityMask);

      PRIVATE:
        /**
         * Indicates start time of activity.
         */
        uint64_t epoch;

        /**
         * A bit mask of activity flags such as Transport::READ_ACTIVITY.
         * Indicates all possible log activities by the owner of this LogProtector.
         */
        int activityMask;

        DISALLOW_COPY_AND_ASSIGN(Activity);
    };

    /**
     * Lock-guard style guard for logProtector.
     * Automatically starts and stops LogProtector.
     */
    class Guard {
      public:

        Guard(Activity& p, int activities = ~0) : protector(&p) {
            protector->start(activities);
        }
        ~Guard() {
            protector->stop();
        }

      PRIVATE:
        Activity* protector;

        DISALLOW_COPY_AND_ASSIGN(Guard);
    };

    //////////////////////////////////////////////////////
    /// Static members
    //////////////////////////////////////////////////////
  public:
    static uint64_t getEarliestOutstandingEpoch(int activityMask);
    static uint64_t getCurrentEpoch();
    static uint64_t incrementCurrentEpoch();
    static void wait(Context* context, int activityMask);

  PRIVATE:
    // An unsigned integer representing the current epoch.
    static uint64_t currentSystemEpoch;

    // Keeps track of all of the EpochProviders in existence.
    typedef std::list<EpochProvider*> EpochList;
    static EpochList epochProviders;

    // Mutex for epochProviders.
    static std::mutex epochProvidersMutex;
    typedef std::lock_guard<std::mutex> Lock;
};

} // namespace RAMCloud

#endif // RAMCLOUD_LOGPROTECTOR_H
