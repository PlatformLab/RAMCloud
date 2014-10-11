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

#include <map>
#include <unordered_set>
#include <unordered_map>

#include "Common.h"
#include "CoordinatorClusterClock.h"
#include "WireFormat.h"

namespace RAMCloud {

class LeaseManager {
  PUBLIC:
    explicit LeaseManager(Context *context);
    WireFormat::ClientLease renewLease(uint64_t leaseId);

  PRIVATE:
    class LeasePreallocator : public WorkerTimer {
      public:
        explicit LeasePreallocator(Context* context,
                                   LeaseManager* leaseManager);
        virtual ~LeasePreallocator() {}
        virtual void handleTimerEvent();

        LeaseManager* leaseManager;
      private:
        DISALLOW_COPY_AND_ASSIGN(LeasePreallocator);
    };

    class LeaseCleaner : public WorkerTimer {
      public:
        explicit LeaseCleaner(Context* context,
                              LeaseManager* leaseManager);
        virtual ~LeaseCleaner() {}
        virtual void handleTimerEvent();

        LeaseManager* leaseManager;
      private:
        DISALLOW_COPY_AND_ASSIGN(LeaseCleaner);
    };

    /// Monitor-style lock
    SpinLock mutex;
    typedef std::lock_guard<SpinLock> Lock;

    /// Shared information about the server.
    Context* context;

    /// Used to provided a guaranteed monotonically increasing number (even
    /// across crashes) that (mostly) advances in sync with real time.  This
    /// Cluster Time is used to define lease expiration times.
    CoordinatorClusterClock clock;

    /// Represents the largest leaseId that was issued from this module.  The
    /// module guarantees that no leaseId will be issued more than once.  The
    /// next leaseId issued should be ++lastIssuedLeaseId.
    uint64_t lastIssuedLeaseId;

    /// This is the largest leaseId that has been pre-allocated in external
    /// storage.  Pre-allocating leaseIds allows this module to respond to
    /// requests for new leases without waiting for an external storage
    /// operation.  This module must never issue a leaseId greater than this
    /// value.  To guarantee this value is recovered after a crash we must make
    /// sure that maxAllocatedLeaseId is never removed (i.e. has its lease
    /// freed).  In the normal case, continual pre-allocations will make sure
    /// that this value runs ahead of lastIssuedLeaseId and thus cannot have
    /// its corresponding lease freed.
    uint64_t maxAllocatedLeaseId;

    /// Maps from leaseId to its leaseTerm (the time after which the lease may
    /// expire).  This is used to quickly service requests about a lease's
    /// liveness.  This structure is updated whenever a lease is added, renewed,
    /// or removed.
    typedef std::unordered_map<uint64_t, uint64_t> LeaseMap;
    LeaseMap leaseMap;

    /// Maps from a leaseTerm to a set of leaseIds with that corresponding term.
    /// This is used to more efficiently determine what leases should be expired
    /// and cleaned.  This structure is updated whenever a lease is added,
    /// renewed, or removed.
    typedef std::map<uint64_t, std::unordered_set<uint64_t> > ReverseLeaseMap;
    ReverseLeaseMap revLeaseMap;

    void allocateNextLease(Lock &lock);
    bool cleanNextLease();
    void recover();

    DISALLOW_COPY_AND_ASSIGN(LeaseManager);
};

} // namespace RAMCloud

#endif	/* RAMCLOUD_LEASEMANAGER_H */

