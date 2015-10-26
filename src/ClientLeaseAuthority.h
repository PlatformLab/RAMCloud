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

#ifndef RAMCLOUD_CLIENTLEASEAUTHORITY_H
#define RAMCLOUD_CLIENTLEASEAUTHORITY_H

#include <set>
#include <unordered_map>

#include "Common.h"
#include "CoordinatorClusterClock.h"
#include "WireFormat.h"

namespace RAMCloud {

/**
 * The ClientLeaseAuthority, which resides on the coordinator, manages and acts
 * as the central authority on the liveness of a client lease.  Client leases
 * are used to track the liveness of any given client.  When a client holds a
 * valid lease, it is considered active and various pieces of state (e.g.
 * linearizability information) must be kept throughout the cluster.
 *
 * Clients must ensure they have an valid lease by periodically contacting this
 * module (this is managed by the ClientLeaseAgent).  Servers in their part must
 * contact this module to check the validity of client leases. This module
 * ensures that no leaseId will be issued more than once.
 */
class ClientLeaseAuthority {
  PUBLIC:
    explicit ClientLeaseAuthority(Context *context);
    WireFormat::ClientLease getLeaseInfo(uint64_t leaseId);
    void recover();
    WireFormat::ClientLease renewLease(uint64_t leaseId);
    void startUpdaters();

  PRIVATE:
    /**
     * The LeaseReservationAgent is periodically invoked to reserve a range of
     * leaseIds on external storage.  The goal is that this agent will work
     * ahead of the issued leases so that a client does not have to wait for an
     * external storage operation to service a request for a new lease.  Every
     * invocation of the agent should ensure that maxAllocatedLeaseId runs ahead
     * of lastIssuedLeaseId by the RESERVATION_LIMIT.  This batch reservation
     * process only blocks during each individual reservation; other operations
     * like issuing leases can be safely interleaved.
     *
     * The agent is structured as a WorkerTimer as a convenient way to perform
     * reservations using a separate worker thread (only start(0) is ever used).
     */
    class LeaseReservationAgent : public WorkerTimer {
      public:
        explicit LeaseReservationAgent(Context* context,
                                       ClientLeaseAuthority* leaseAuthority);
        virtual void handleTimerEvent();

        ClientLeaseAuthority* leaseAuthority;
      private:
        DISALLOW_COPY_AND_ASSIGN(LeaseReservationAgent);
    };

    /**
     * The LeaseCleaner periodically wakes up to expire leases.
     */
    class LeaseCleaner : public WorkerTimer {
      public:
        explicit LeaseCleaner(Context* context,
                              ClientLeaseAuthority* leaseAuthority);
        virtual void handleTimerEvent();

        ClientLeaseAuthority* leaseAuthority;
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

    /// This is the largest leaseId that has been reserved in external storage.
    /// To ensure this value can be recovered after a coordinator crash, this
    /// module must never issue a leaseId greater than or equal to this value.
    /// This constraint prevents the lease cleaner from ever removing the
    /// external storage record for the largest reserved leaseId.
    uint64_t maxReservedLeaseId;

    /// Maps from leaseId to its leaseExpiration.  This is used to quickly
    /// service requests about a lease's liveness.  This structure is updated
    /// whenever a lease is added, renewed, or removed.
    typedef std::unordered_map<uint64_t, ClusterTime> LeaseMap;
    LeaseMap leaseMap;

    /// Structure to define the entries in the ExpirationOrderSet.
    struct ExpirationOrderElem {
        ClusterTime leaseExpiration;// ClusterTime of possible lease expiration.
        uint64_t leaseId;           // Id of the lease.

        /**
         * The operator < is overridden to implement the
         * correct comparison for the expirationOrder.
         */
        bool operator<(const ExpirationOrderElem& elem) const {
            return leaseExpiration < elem.leaseExpiration ||
                    (leaseExpiration == elem.leaseExpiration &&
                            leaseId < elem.leaseId);
       }
    };

    /// This ordered set keeps track of the order in which leases can expire.
    /// This structure allows the module to quickly determine what leases can be
    /// expired next.  This structure should always be updated to refect changes
    /// to the LeaseMap structure when the LeaseMap is updated.
    typedef std::set<ExpirationOrderElem> ExpirationOrderSet;
    ExpirationOrderSet expirationOrder;

    LeaseReservationAgent reservationAgent;
    LeaseCleaner cleaner;

    bool cleanNextLease();
    std::string getLeaseObjName(uint64_t leaseId);
    WireFormat::ClientLease renewLeaseInternal(uint64_t leaseId, Lock &lock);
    void reserveNextLease(Lock &lock);

    DISALLOW_COPY_AND_ASSIGN(ClientLeaseAuthority);
};

} // namespace RAMCloud

#endif  /* RAMCLOUD_CLIENTLEASEAUTHORITY_H */

