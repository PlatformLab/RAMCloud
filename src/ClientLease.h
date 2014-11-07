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

#ifndef RAMCLOUD_CLIENTLEASE_H
#define RAMCLOUD_CLIENTLEASE_H

#include "Common.h"
#include "CoordinatorClient.h"
#include "WireFormat.h"

namespace RAMCloud {

class RamCloud;

/**
 * This class allows client rpcs to aquire and maintain valid leases.  These
 * leases are used to represent the lifetime of an active client so that per
 * client state stored on servers (e.g. linearizability data) can be garbage
 * collected when clients fail or become inactive.
 */
class ClientLease : Dispatch::Poller {
  public:
    explicit ClientLease(RamCloud* ramcloud);
    WireFormat::ClientLease getLease();
    virtual void poll();

  PRIVATE:
    /// Overall client state information.
    RamCloud* ramcloud;
    /// Latest ClientLease received from the coordinator.
    WireFormat::ClientLease lease;

    /// The local time (in cycles) when the last ClientLease renewal was sent to
    /// the coordinator.  Used to estimate when the lease term will elapse.
    uint64_t localTimestampCycles;

    /// Used the store the next value of localTimestampCycles so that it can
    /// represent sent time with out modifying the value before the new lease
    /// is received.
    uint64_t nextTimestampCycles;

    /// Holds a possibly outstanding RenewLeaseRpc so that this module can
    /// use asynchronous calls to the coordinator to maintain its lease.
    Tub<RenewLeaseRpc> renewLeaseRpc;

    uint64_t leaseTermRemaining();

    /// Defines the remaining lease time below which this module should starting
    /// trying to renew.  During this period, the lease has probably not expired
    /// so it is safe to perform the renewals asynchronously.  This value should
    /// be set conservatively to around half the LeaseManager::LEASE_TERM_US.
    static const uint64_t RENEW_THRESHOLD_US = 150*1e6;     // 2.5 min

    /// Defines the remaining lease time below which this module should consider
    /// the lease possibly expired.  In this case, the module must not return
    /// the current lease until it as renewed.  This value should be set to a
    /// value much larger than the time to complete a renewal.
    static const uint64_t DANGER_THRESHOLD_US = 500;

    void pollInternal();

    DISALLOW_COPY_AND_ASSIGN(ClientLease);
};

} // namespace RAMCloud

#endif  /* RAMCLOUD_CLIENTLEASE_H */

