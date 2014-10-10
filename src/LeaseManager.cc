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

#include "LeaseManager.h"

namespace RAMCloud {

/// Defines the period of time that a lease will be extended upon renewal.
const uint64_t LEASE_TERM_MS = 300000;      // 5 min = 300,000 ms

LeaseManager::LeaseManager(Context* context)
    : mutex()
    , context(context)
    , clock(context)
    , lastIssuedLeaseId(0)
    , maxAllocatedLeaseId(0)
    , leaseMap()
    , revLeaseMap()
{
}

WireFormat::ClientLease
LeaseManager::renewLease(uint64_t leaseId)
{
    Lock lock(mutex);
    WireFormat::ClientLease clientLease;

    LeaseMap::iterator leaseEntry = leaseMap.find(leaseId);
    if (leaseEntry != leaseMap.end()) {
        // Simply renew the existing lease
        clientLease.leaseId = leaseId;
        revLeaseMap[leaseEntry->second].erase(leaseId);
    } else {
        // Must issue a new lease as the old one has expired.
        // If this id has not been preallocated, allocate it now.
        // This should only ever execute once if at all.
        while (maxAllocatedLeaseId <= lastIssuedLeaseId) {
            allocateNextLease(lock);
        }
        clientLease.leaseId = ++lastIssuedLeaseId;
    }

    clientLease.leaseTerm = clock.getTime() + LEASE_TERM_MS;
    leaseMap[clientLease.leaseId] = clientLease.leaseTerm;
    revLeaseMap[clientLease.leaseTerm].insert(clientLease.leaseId);

    return clientLease;
}

/**
 * Persist the next avaliable leaseId to external storage=.
 *
 * \param lock
 *      Ensures that caller has acquired mutex; not actually used here.
 */
void
LeaseManager::allocateNextLease(Lock &lock)
{
    uint64_t nextLeaseId = maxAllocatedLeaseId + 1;
    // TODO(cstlee): Persist to External Storage
    maxAllocatedLeaseId = nextLeaseId;
}

} // namespace RAMCloud

