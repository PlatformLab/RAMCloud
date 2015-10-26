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

#include <string>

#include "ClientLeaseAuthority.h"

#include "ExternalStorage.h"
#include "LeaseCommon.h"
#include "ShortMacros.h"

namespace RAMCloud {

/// Defines the number of leases the reservation agent should try to keep ahead
/// of the lastIssuedLeaseId.  This limit should be set large enough to amortize
/// the cost of external storage writes but small enough so that we don't use up
/// the 64-bit id space (i.e. LIMIT * "num coordinator crashes" << 2^64 ).
const uint64_t RESERVATION_LIMIT = 1000;

/// Defines the number of reserved leases below which the reservation agent
/// should be started.  This should be set large enough so that the reservation
/// agent can start reserving more leases before they run out completely but
/// small enough such that the reservation agent is constantly being run.
const uint64_t RESERVATIONS_LOW = 250;

/// Defines the prefix for objects stored in external storage by this module.
const std::string STORAGE_PREFIX = "clientLeaseAuthority";

ClientLeaseAuthority::ClientLeaseAuthority(Context* context)
    : mutex()
    , context(context)
    , clock(context)
    , lastIssuedLeaseId(0)
    , maxReservedLeaseId(0)
    , leaseMap()
    , expirationOrder()
    , reservationAgent(context, this)
    , cleaner(context, this)
{}

/**
 * Return the lease info for the provided leaseId.  Used by masters to
 * implicitly check if a lease is still valid.
 *
 * \param leaseId
 *      Id of the lease whose liveness you wish to check.
 * \return
 *      If the lease exists, return lease with its current lease term.  If not,
 *      the returned the lease id will be 0 (an invalid lease id) signaling that
 *      the requested lease has already expired or possibly never existed.
 */
WireFormat::ClientLease
ClientLeaseAuthority::getLeaseInfo(uint64_t leaseId)
{
    Lock lock(mutex);
    WireFormat::ClientLease clientLease;

    // Populate the lease information if it is found.
    LeaseMap::iterator leaseEntry = leaseMap.find(leaseId);
    if (leaseEntry != leaseMap.end()) {
        clientLease.leaseId = leaseId;
        clientLease.leaseExpiration = leaseEntry->second.getEncoded();
    } else {
        clientLease.leaseId = 0;
        clientLease.leaseExpiration = 0;
    }

    clientLease.timestamp = clock.getTime().getEncoded();

    return clientLease;
}

/**
 * Recover lease information from external storage.  This should only be called
 * once after the construction of the Lease Manager.
 */
void
ClientLeaseAuthority::recover()
{
    Lock lock(mutex);

    // Fetch all lease information from external storage.
    vector<ExternalStorage::Object> objects;
    context->externalStorage->getChildren(STORAGE_PREFIX.c_str(), &objects);

    // We can safely make the approximation that every reserved lease was
    // issued with the exception of the largest reserved leaseId.
    foreach (ExternalStorage::Object& object, objects) {
        try {
            std::string name = object.name;
            uint64_t leaseId = std::stoull(name);

            // Keep track of the largest reserved leaseId and only auto "renew"
            // the other leaseIds.
            if (maxReservedLeaseId == 0) {
                maxReservedLeaseId = leaseId;
                continue;
            } else if (leaseId > maxReservedLeaseId) {
                uint64_t temp = maxReservedLeaseId;
                maxReservedLeaseId = leaseId;
                leaseId = temp;
            }

            ClusterTime leaseExpiration = clock.getTime() +
                                          LeaseCommon::LEASE_TERM;
            leaseMap[leaseId] = leaseExpiration;
            expirationOrder.insert({leaseExpiration, leaseId});
        } catch (std::invalid_argument& e) {
            LOG(ERROR, "Unable to recover lease: %s", object.name);
        }
    }

    if (maxReservedLeaseId > 0) {
        lastIssuedLeaseId = maxReservedLeaseId - 1;
    }
}

/**
 * Attempts to renew a lease with leaseId. If the requested leaseId is still
 * live the lease is renewed and its term is extended; if not a new leaseId is
 * issued and returned.
 *
 * \param leaseId
 *      The leaseId that a client wishes to renew if possible.  Note that
 *      leaseId = 0 is implicitly invalid and will always cause a new lease to
 *      be returned.
 * \return
 *      leaseId and leaseExpiration for the renewed lease or new lease.
 */
WireFormat::ClientLease
ClientLeaseAuthority::renewLease(uint64_t leaseId)
{
    Lock lock(mutex);
    WireFormat::ClientLease clientLease = renewLeaseInternal(leaseId, lock);

    // Poke reservation agent if we are getting close to running out of leases.
    if (maxReservedLeaseId - lastIssuedLeaseId < RESERVATIONS_LOW) {
        reservationAgent.start(0);
    }

    return clientLease;
}

/**
 * Start background timers to perform lease reservation and cleaning and also
 * start the cluster clock updater.
 */
void
ClientLeaseAuthority::startUpdaters()
{
    reservationAgent.start(0);
    cleaner.start(0);
    clock.startUpdater();
}

/**
 * Constructor for the LeaseReservationAgent.
 * \param context
 *      Overall information about the RAMCloud server and provides access to
 *      the dispatcher.
 * \param leaseAuthority
 *      Provides access to the leaseAuthority to perform reservation.
 */
ClientLeaseAuthority::LeaseReservationAgent::LeaseReservationAgent(
        Context* context, ClientLeaseAuthority* leaseAuthority)
    : WorkerTimer(context->dispatch)
    , leaseAuthority(leaseAuthority)
{}

/**
 * The handler performs the lease reservation and is scheduled by calls to
 * renewLease when the number of reserved leases runs low.  The handler works
 * incrementally and will continue to rescheduled itself until the number of
 * reserved leases reaches a comfortable level (RESERVATION_LIMIT).
 */
void
ClientLeaseAuthority::LeaseReservationAgent::handleTimerEvent()
{
    ClientLeaseAuthority::Lock lock(leaseAuthority->mutex);
    leaseAuthority->reserveNextLease(lock);
    uint64_t reservationCount = leaseAuthority->maxReservedLeaseId -
                                leaseAuthority->lastIssuedLeaseId;
    if (reservationCount < RESERVATION_LIMIT) {
            this->start(0);
    }
}

/**
 * Constructor for the LeaseCleaner.
 * \param context
 *      Overall information about the RAMCloud server and provides access to
 *      the dispatcher.
 * \param leaseAuthority
 *      Provides access to the leaseAuthority to perform cleaning.
 */
ClientLeaseAuthority::LeaseCleaner::LeaseCleaner(Context* context,
                                         ClientLeaseAuthority* leaseAuthority)
    : WorkerTimer(context->dispatch)
    , leaseAuthority(leaseAuthority)
{}

/**
 * This handler performs a cleaning pass on leaseAuthority incrementally; will
 * reschedule itself as necessary.
 */
void
ClientLeaseAuthority::LeaseCleaner::handleTimerEvent()
{
    if (leaseAuthority->cleanNextLease()) {
        // Cleaning pass not complete; reschedule for immediate execution.
        this->start(0);
    } else {
        // Run roughly 10 times per lease term; frequent enough to do more
        // aggressive garbage collection but waiting long enough for a bunch
        // of leases to expire.
        this->start(Cycles::rdtsc() +
                    Cycles::fromNanoseconds(
                            LeaseCommon::LEASE_TERM.toNanoseconds() / 10) );
    }
}

/**
 * Expire the next lease whose term has elapsed. If the term of the next lease
 * to be expired has not yet elapsed, this call has no effect.
 *
 * \return
 *      Return true if a lease was able to be cleaned.  Returning false implies
 *      there are no more leases that can be cleaned at this moment.
 */
bool
ClientLeaseAuthority::cleanNextLease()
{
    Lock lock(mutex);
    ExpirationOrderSet::iterator it = expirationOrder.begin();
    if (it != expirationOrder.end() && it->leaseExpiration < clock.getTime()) {
        uint64_t leaseId = it->leaseId;
        context->externalStorage->remove(getLeaseObjName(leaseId).c_str());
        expirationOrder.erase(it);
        leaseMap.erase(leaseId);
        return true;
    }
    return false;
}

/**
 * Return the external storage object name for the external storage object that
 * represents a lease with the given leaseId.
 */
std::string
ClientLeaseAuthority::getLeaseObjName(uint64_t leaseId)
{
    std::string leaseObjName = STORAGE_PREFIX + "/" + format("%lu", leaseId);
    return leaseObjName;
}

/**
 * Does most of the work for "renewLease".  Breaks up code for ease of testing.
 *
 * \param leaseId
 *      See "renewLease".
 * \param lock
 *      Ensures that caller has acquired mutex; not actually used here.
 * \return
 *      See "renewLease".
 */
WireFormat::ClientLease
ClientLeaseAuthority::renewLeaseInternal(uint64_t leaseId, Lock &lock)
{
    WireFormat::ClientLease clientLease;

    LeaseMap::iterator leaseEntry = leaseMap.find(leaseId);
    if (leaseEntry != leaseMap.end()) {
        // Simply renew the existing lease.
        clientLease.leaseId = leaseId;
        expirationOrder.erase({leaseEntry->second, leaseId});
    } else {
        // Must issue a new lease as the old one has expired.
        clientLease.leaseId = ++lastIssuedLeaseId;

        // The maxReservedLeaseId needs to always stay ahead of any issued
        // leaseId.  If it is not, more leases need to be reserved. This should
        // only ever execute once if at all.
        while (maxReservedLeaseId <= lastIssuedLeaseId) {
            // Instead of waiting for the reservation agent to run and catch up,
            // we reserve an additional leaseId manually.
            RAMCLOUD_LOG(WARNING,
                         "Lease reservations are not keeping up; "
                         "maxReservedLeaseId = %lu", maxReservedLeaseId);
            reserveNextLease(lock);
        }
    }

    ClusterTime now = clock.getTime();
    ClusterTime leaseExpiration = now + LeaseCommon::LEASE_TERM;
    clientLease.timestamp = now.getEncoded();
    clientLease.leaseExpiration = leaseExpiration.getEncoded();
    leaseMap[clientLease.leaseId] = leaseExpiration;
    expirationOrder.insert({leaseExpiration, clientLease.leaseId});

    return clientLease;
}

/**
 * Persist the next available leaseId to external storage.
 *
 * \param lock
 *      Ensures that caller has acquired mutex; not actually used here.
 */
void
ClientLeaseAuthority::reserveNextLease(Lock &lock)
{
    uint64_t nextLeaseId = maxReservedLeaseId + 1;
    context->externalStorage->set(ExternalStorage::Hint::CREATE,
                                  getLeaseObjName(nextLeaseId).c_str(),
                                  "", 0);
    maxReservedLeaseId = nextLeaseId;
}

} // namespace RAMCloud

