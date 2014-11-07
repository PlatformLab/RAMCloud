/* Copyright (c) 2014 Stanford University
 *
 * Permission to use, coly, modify, and distribute this software for any
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

#include "ClientLease.h"
#include "RamCloud.h"

namespace RAMCloud {

/**
 * Constructor for ClientLease.
 */
ClientLease::ClientLease(RamCloud* ramcloud)
    : Dispatch::Poller(ramcloud->clientContext->dispatch, "ClientLease")
    , ramcloud(ramcloud)
    , lease({0, 0, 0})
    , localTimestampCycles(Cycles::rdtsc())
    , nextTimestampCycles(localTimestampCycles)
    , renewLeaseRpc()
{}

/**
 * Return a valid client lease.  If a valid client lease has not been issued or
 * the lease is about to expire, this method will block until it is able to
 * return a valid client lease.
 */
WireFormat::ClientLease
ClientLease::getLease()
{
    do {
        pollInternal();
    } while (leaseTermRemaining() < DANGER_THRESHOLD_US);

    return lease;
}

/**
 * Make incremental progress toward ensuring a valid lease (if there are out
 * standing RPCs that require it).
 */
void
ClientLease::poll()
{
    if (ramcloud->realRpcTracker.hasUnfinishedRpc()) {
        pollInternal();
    }
}

/**
 * Internal method to calculate an estimated remaining lease term in us.  This
 * method is used to determine when the lease would be dangerous to use and when
 * a lease should be renewed.
 *
 * \return
 *      The estimated number of microseconds in the leaseTerm remaining.
 */
uint64_t
ClientLease::leaseTermRemaining()
{
    uint64_t termRemainingUs = 0;
    uint64_t leaseTermLenUs = 0;
    uint64_t elapsedTimeUs = Cycles::toMicroseconds(Cycles::rdtsc()
                                                    - localTimestampCycles);

    if (lease.leaseTerm > lease.timestamp) {
        leaseTermLenUs = lease.leaseTerm - lease.timestamp;
    }
    if (leaseTermLenUs > elapsedTimeUs) {
        termRemainingUs = leaseTermLenUs - elapsedTimeUs;
    }

    return termRemainingUs;
}

/**
 * This method implements a rules-based asynchronously algorithm to
 * incrementally make progress to ensure there is a valid lease.  This method is
 * called in getLease and should also be called repeatedly while waiting for a
 * linearizable rpc to complete (i.e. in dispatch poll).
 */
void
ClientLease::pollInternal()
{
    if (renewLeaseRpc){
        if (renewLeaseRpc->isReady()) {
            lease = renewLeaseRpc->wait();
            localTimestampCycles = nextTimestampCycles;
            renewLeaseRpc.destroy();
        }
    } else {
        if (leaseTermRemaining() < RENEW_THRESHOLD_US) {
            nextTimestampCycles = Cycles::rdtsc();
            renewLeaseRpc.construct(ramcloud->clientContext, lease.leaseId);
        }
    }
}

} // namespace RAMCloud
