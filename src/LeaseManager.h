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

#ifndef RAMCLOUD_LEASEMANAGER_H
#define	RAMCLOUD_LEASEMANAGER_H

#include "Common.h"

namespace RAMCloud {

class LeaseManager {
  PUBLIC:
    LeaseManager();
    explicit LeaseManager(Context *context);
  PRIVATE:
    /**
     * The SafeValueUpdater maintains clientId and cluster time values on
     * external storage that are guaranteed to be greater than value that this
     * module has ever externalized to anyone, so that in the event of a
     * coordinator crash, the new coordinator can ensure the uniquness property
     * of clientIds and monotonic property of cluster time.
     */
    class SafeValuesUpdater : public WorkerTimer {
      public:
        explicit SafeValuesUpdater(Context* context,
                                   LeaseManager* leaseManager);
        virtual ~SafeValuesUpdater() {}
        virtual void handleTimerEvent();

        ExternalStorage* externalStorage;
        LeaseManager* leaseManager;
      private:
        DISALLOW_COPY_AND_ASSIGN(SafeValuesUpdater);
    };

    /// Monitor-style lock
    SpinLock mutex;
    typedef std::lock_guard<SpinLock> Lock;

    /// Amount of time (in milliseconds) to advance the safeClusterTime stored
    /// in external storage.  This value should be much larger than the time
    /// to perform the external storage write (~10ms) but much much less than
    /// the max value (2^64 - 1).
    static const uint64_t safeTimeIntervalMs = 3000;

    /// Amount of time (in seconds) between updates of the safeClusterTime to
    /// externalStorage.  This time should be less than the safeTimeIntervalMs;
    /// we recommend a value equivalent to half the safeTimeIntervalMs.
    static const double updateIntervalS = 1.5;

    /// System time of the coordinator when the clock is initialized.  Used to
    /// calculate current cluster time.
    const uint64_t startingSysTimeMs;

    /// Recovered safeClusterTime from externalStorage when the clock is
    /// initialize (may be zero if cluster is new).  Used to calculate current
    /// cluster time.
    const uint64_t startingClusterTimeMs;

    /// The last cluster time stored in externalStorage.  Represents the
    /// largest cluster time that is safe to externalize (see "getTime()").
    uint64_t safeClusterTimeMs;

    /// The last clientId stored in externalStorage. Represents the largest
    /// clientId that is safe to issue.
    uint64_t safeClientId;

    /// The next client id should always be less that or equal to than the
    /// safeClientId.  See issueCLientId().
    uint64_t nextClientId;

    /// The amount by which the safeClientId should be ahead of nextClientId.
    /// This value should be larger than the number of client requests the
    /// coordinator should be able to handle in the time it takes to perform an
    /// external storage write (~10ms).  The value should also be much much
    /// smaller than the max value.
    static const uint64_t safeClientIdRange ;

    ///
    SafeValuesUpdater updater;

    uint64_t issueClientId(Lock &lock);
    uint64_t getTime(Lock &lock);
    uint64_t getTimeInternal(Lock &lock);
    static uint64_t recoverClusterTime(ExternalStorage* externalStorage);
    void updateSafeValues(Lock &lock);

    DISALLOW_COPY_AND_ASSIGN(LeaseManager);
};

} // namespace RAMCloud

#endif	/* RAMCLOUD_LEASEMANAGER_H */

