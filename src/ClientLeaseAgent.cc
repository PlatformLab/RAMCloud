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

#include "ClientLeaseAgent.h"

#include "ClusterTime.h"
#include "Cycles.h"
#include "LeaseCommon.h"
#include "RamCloud.h"
#include "TestLog.h"
#include "Unlock.h"

namespace RAMCloud {

/**
 * Constructor for ClientLease.
 */
ClientLeaseAgent::ClientLeaseAgent(RamCloud* ramcloud)
    : mutex()
    , ramcloud(ramcloud)
    , lease({0, 0, 0})
    , lastRenewalTimeCycles(0)
    , nextRenewalTimeCycles(0)
    , leaseExpirationCycles(0)
    , renewLeaseRpc()
{}

/**
 * Return a valid client lease.  If a valid client lease has not been issued or
 * the lease is about to expire, this method will block until it is able to
 * return a valid client lease.
 */
WireFormat::ClientLease
ClientLeaseAgent::getLease()
{
    Lock _(mutex);

    // Block waiting for the lease to become valid; should only occur if there
    // is a long gap between issuing RPCs that require a client lease (i.e
    // linearizable RPCs or transaction RPCs).
    while (Cycles::rdtsc() > leaseExpirationCycles) {
        RAMCLOUD_CLOG(NOTICE, "Blocked waiting for lease to renew.");

        // Release lock so poller can execute.
        Unlock<SpinLock> _(mutex);

        this->poll();
        ramcloud->poll();
    }

    return lease;
}

/**
 * Make incremental progress toward ensuring a valid lease. This method must be
 * called periodically in order to maintain a valid lease.
 */
void
ClientLeaseAgent::poll()
{
    Lock _(mutex);

    // Do nothing if it is not yet time to renew.
    if (Cycles::rdtsc() <= nextRenewalTimeCycles) {
        return;
    }

    /**
     * This method implements a rules-based asynchronous algorithm (a "task")
     * to incrementally make progress to ensure there is a valid lease (the
     * "goal").  After running, this method will schedule itself to be run again
     * in the future if additional work needs to be done to reach the goal of
     * having a valid lease.  This task will sleep (i.e. not reschedule itself)
     * if it detects that there are no unfinished rpcs that require a valid
     * lease.  This task is awakened by calls to getLease.
     */
    if (!renewLeaseRpc) {
        lastRenewalTimeCycles = Cycles::rdtsc();
        uint64_t leaseId = lease.leaseId;
        renewLeaseRpc.construct(ramcloud->clientContext, leaseId);
    } else {
        if (!renewLeaseRpc->isReady()) {
            // Wait for rpc to become ready.
        } else {
            lease = renewLeaseRpc->wait();
            renewLeaseRpc.destroy();
            // Use local rdtsc cycle time to estimate when the lease will expire
            // if the lease is not renewed.
            //
            // If any of the asserts fail, an assumption about the behavior of
            // ClientLeaseAuthority::renewLease must have been violated.
            ClusterTime timestamp(lease.timestamp);
            ClusterTime leaseExpiration(lease.leaseExpiration);
            assert(leaseExpiration >= timestamp);
            ClusterTimeDuration leaseTermLen = leaseExpiration - timestamp;

            assert(leaseTermLen >= LeaseCommon::DANGER_THRESHOLD);
            leaseExpirationCycles =
                    lastRenewalTimeCycles +
                    Cycles::fromNanoseconds(
                            (leaseTermLen -
                            LeaseCommon::DANGER_THRESHOLD).toNanoseconds());

            assert(leaseTermLen >= LeaseCommon::RENEW_THRESHOLD);
            uint64_t renewCycleTime =
                    Cycles::fromNanoseconds(
                            (leaseTermLen -
                            LeaseCommon::RENEW_THRESHOLD).toNanoseconds());
            nextRenewalTimeCycles = lastRenewalTimeCycles + renewCycleTime;
        }
    }
}

} // namespace RAMCloud
