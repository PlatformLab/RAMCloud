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

#ifndef RAMCLOUD_LEASEMANAGER_H
#define RAMCLOUD_LEASEMANAGER_H

#include <set>
#include <unordered_map>

#include "Common.h"
#include "CoordinatorClusterClock.h"
#include "WireFormat.h"

namespace RAMCloud {

/**
 * The LeaseManager, which resides on the coordinator, manages and acts as the
 * central authority on the liveness of a client lease.  Client leases are used
 * to track the liveness of any given client.  When a client holds a valid
 * lease, it is considered active and various pieces of state (e.g.
 * linearizability information) must be kept throughout the cluster.
 *
 * Clients must ensure they have an valid lease by periodically contacting this
 * module.  Servers in their part must contact this module to check the validity
 * of client leases. The module LeaseManager that no leaseId will be issued more
 * than once.
 */
class LeaseManager {
  PUBLIC:
    explicit LeaseManager(Context *context);
    WireFormat::ClientLease getLeaseInfo(uint64_t leaseId);
    void recover();
    WireFormat::ClientLease renewLease(uint64_t leaseId);
    void startUpdaters();

  PRIVATE:
    /**
     * The LeasePreallocator is periodically invoked to allocate a range of
     * leaseIds on external storage.  The goal is that this preallocator will
     * work ahead of the issued leases so that a client does not have to wait
     * for an external storage operation to complete a new lease request.  Every
     * invocation of the preallocator should ensure that maxAllocatedLeaseId
     * runs ahead of lastIssuedLeaseId by the PREALLOCATION_LIMIT.  This batch
     * allocation process only blocks during each individual allocation; other
     * operations like issuing leases can be safely interleaved.
     */
    class LeasePreallocator : public WorkerTimer {
      public:
        explicit LeasePreallocator(Context* context,
                                   LeaseManager* leaseManager);
        virtual void handleTimerEvent();

        LeaseManager* leaseManager;
      private:
        DISALLOW_COPY_AND_ASSIGN(LeasePreallocator);
    };

    /**
     * The LeaseCleaner periodically wakes up to expire leases.
     */
    class LeaseCleaner : public WorkerTimer {
      public:
        explicit LeaseCleaner(Context* context,
                              LeaseManager* leaseManager);
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

    /// Structure to define the entries in the ExpirationOrderSet.
    struct ExpirationOrderElem {
        uint64_t leaseTerm;     // ClusterTime after which the lease can expire.
        uint64_t leaseId;       // Id of the lease.

        /**
         * The operator < is overridden to implement the
         * correct comparison for the expirationOrder.
         */
       bool operator<(const ExpirationOrderElem& elem) const {
           return leaseTerm < elem.leaseTerm ||
               (leaseTerm == elem.leaseTerm && leaseId < elem.leaseId);
       }
    };

    /// This ordered set keeps track of the order in which leases can expire.
    /// This structure allows the module to quickly determine what leases can be
    /// expired next.  This structure should always be updated to refect changes
    /// to the LeaseMap structure when the LeaseMap is updated.
    typedef std::set<ExpirationOrderElem> ExpirationOrderSet;
    ExpirationOrderSet expirationOrder;

    LeasePreallocator preallocator;
    LeaseCleaner cleaner;

    void allocateNextLease(Lock &lock);
    bool cleanNextLease();
    WireFormat::ClientLease renewLeaseInternal(uint64_t leaseId, Lock &lock);

    DISALLOW_COPY_AND_ASSIGN(LeaseManager);
};

} // namespace RAMCloud

#endif  /* RAMCLOUD_LEASEMANAGER_H */

