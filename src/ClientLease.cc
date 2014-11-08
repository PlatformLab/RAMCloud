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
    : Dispatch::Timer(ramcloud->clientContext->dispatch)
    , ramcloud(ramcloud)
    , lease({0, 0, 0})
    , localTimestampCycles(0)
    , leaseTermElapseCycles(0)
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
    // If the lease renewal timer is not running, schedule it.
    if (!isRunning()) {
        start(0);
    }

    // Block waiting for the lease to become valid.
    while (Cycles::rdtsc() > leaseTermElapseCycles) {
        ramcloud->poll();
    }

    return lease;
}

/**
 * This method implements a rules-based asynchronously algorithm (a "task") to
 * incrementally make progress to ensure there is a valid lease (the "goal").
 * After running, this method will schedule itself to be run again in the future
 * if additional work needs to be done to reach the goal of having a valid
 * lease.  This task will sleep (i.e. not reschedule itself) if it detects
 * that there are no unfinished rpcs that require a valid lease.  This task is
 * awakened by calls to getLease.
 */
void
ClientLease::handleTimerEvent()
{
    if (!renewLeaseRpc) {
        localTimestampCycles = Cycles::rdtsc();
        renewLeaseRpc.construct(ramcloud->clientContext, lease.leaseId);
        start(0);
    } else {
        if (renewLeaseRpc->isReady()) {
            lease = renewLeaseRpc->wait();
            renewLeaseRpc.destroy();
            uint64_t leaseTermLenUs = 0;
            if (lease.leaseTerm > lease.timestamp) {
                leaseTermLenUs = lease.leaseTerm - lease.timestamp;
            }
            leaseTermElapseCycles = localTimestampCycles +
                                    Cycles::fromMicroseconds(
                                            leaseTermLenUs -
                                            DANGER_THRESHOLD_US);

            // Only reschedule for lease renewal if there are
            // unfinished rpcs.
            if (ramcloud->realRpcTracker.hasUnfinishedRpc()) {
                uint64_t renewCycleTime = 0;
                if (leaseTermLenUs > RENEW_THRESHOLD_US) {
                    renewCycleTime = Cycles::fromMicroseconds(
                            leaseTermLenUs - RENEW_THRESHOLD_US);
                }
                start(localTimestampCycles + renewCycleTime);
            }
        } else {
            // Wait for rpc to become ready.
            start(0);
        }
    }
}

} // namespace RAMCloud
