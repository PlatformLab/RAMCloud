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

#ifndef RAMCLOUD_CLIENTLEASEAGENT_H
#define RAMCLOUD_CLIENTLEASEAGENT_H

#include "Common.h"
#include "CoordinatorClient.h"
#include "WireFormat.h"

namespace RAMCloud {

class RamCloud;

/**
 * This class allows client rpcs to acquire and maintain valid leases.  These
 * leases are used to represent the lifetime of an active client so that per
 * client state stored on servers (e.g. linearizability data) can be garbage
 * collected when clients fail or become inactive.
 *
 * This class is thread-safe (if only to allow lease renewal to happen on a
 * background worker thread).
 */
class ClientLeaseAgent {
  public:
    explicit ClientLeaseAgent(RamCloud* ramcloud);
    WireFormat::ClientLease getLease();
    void poll();

  PRIVATE:
    /// Monitor-style lock
    SpinLock mutex;
    typedef std::lock_guard<SpinLock> Lock;

    /// Overall client state information.
    RamCloud* ramcloud;

    /// Latest ClientLease received from the coordinator.
    WireFormat::ClientLease lease;

    /// The Cycles::rdtsc() value (in cycles) when the last ClientLease renewal
    /// request was issued.  Used to estimate when the lease term will elapse.
    uint64_t lastRenewalTimeCycles;

    /// The Cycles::rdtsc() value (in cycles) when the next ClientLease renewal
    /// request should be issued.
    uint64_t nextRenewalTimeCycles;

    /// If Cycles::rdtsc() returns a value larger than this value, the currently
    /// held lease may have (or will soon be) expired.  Used to determine
    /// whether getLease will need to block waiting for a new lease.
    uint64_t leaseExpirationCycles;

    /// Holds a possibly outstanding RenewLeaseRpc so that this module can
    /// use asynchronous calls to the coordinator to maintain its lease.
    Tub<RenewLeaseRpc> renewLeaseRpc;

    DISALLOW_COPY_AND_ASSIGN(ClientLeaseAgent);
};

} // namespace RAMCloud

#endif  /* RAMCLOUD_CLIENTLEASEAGENT_H */

