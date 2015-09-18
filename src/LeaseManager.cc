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

#include "LeaseManager.h"

#include "ExternalStorage.h"
#include "LeaseCommon.h"
#include "ShortMacros.h"

namespace RAMCloud {

/// Defines the number of leases the reservation agent should try to keep ahead
/// of the lastIssuedLeaseId.
const uint64_t RESERVATION_LIMIT = 1000;

/// Defines the prefix for objects stored in external storage by this module.
const std::string STORAGE_PREFIX = "leaseManager";

LeaseManager::LeaseManager(Context* context)
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
LeaseManager::getLeaseInfo(uint64_t leaseId)
{
    Lock lock(mutex);
    WireFormat::ClientLease clientLease;

    // Populate the lease information if it is found.
    LeaseMap::iterator leaseEntry = leaseMap.find(leaseId);
    if (leaseEntry != leaseMap.end()) {
        clientLease.leaseId = leaseId;
        clientLease.leaseExpiration = leaseEntry->second;
    } else {
        clientLease.leaseId = 0;
        clientLease.leaseExpiration = 0;
    }

    clientLease.timestamp = clock.getTime();

    return clientLease;
}

/**
 * Recover lease information from external storage.  This should only be called
 * once after the construction of the Lease Manager.
 */
void
LeaseManager::recover()
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

            uint64_t leaseExpiration = clock.getTime() +
                                       LeaseCommon::LEASE_TERM_US;
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
LeaseManager::renewLease(uint64_t leaseId)
{
    Lock lock(mutex);
    WireFormat::ClientLease clientLease = renewLeaseInternal(leaseId, lock);

    // Poke reservation agent if we are getting close to running out of leases.
    if (maxReservedLeaseId - lastIssuedLeaseId < RESERVATION_LIMIT / 4) {
        reservationAgent.start(0);
    }

    return clientLease;
}

/**
 * Start background timers to perform lease reservation and cleaning and also
 * start the cluster clock updater.
 */
void
LeaseManager::startUpdaters()
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
 * \param leaseManager
 *      Provides access to the leaseManager to perform reservation.
 */
LeaseManager::LeaseReservationAgent::LeaseReservationAgent(Context* context,
                                                  LeaseManager* leaseManager)
    : WorkerTimer(context->dispatch)
    , leaseManager(leaseManager)
{}

/**
 * The handler performs the lease reservation.
 */
void
LeaseManager::LeaseReservationAgent::handleTimerEvent()
{
    uint64_t reservationCount = 0;
    while (true) {
        LeaseManager::Lock lock(leaseManager->mutex);
        reservationCount = leaseManager->maxReservedLeaseId -
                             leaseManager->lastIssuedLeaseId;
        if (reservationCount >= RESERVATION_LIMIT)
            break;
        leaseManager->reserveNextLease(lock);
    }
}


/**
 * Constructor for the LeaseCleaner.
 * \param context
 *      Overall information about the RAMCloud server and provides access to
 *      the dispatcher.
 * \param leaseManager
 *      Provides access to the leaseManager to perform cleaning.
 */
LeaseManager::LeaseCleaner::LeaseCleaner(Context* context,
                                         LeaseManager* leaseManager)
    : WorkerTimer(context->dispatch)
    , leaseManager(leaseManager)
{}

/**
 * This handler performs a cleaning pass on leaseManager.
 */
void
LeaseManager::LeaseCleaner::handleTimerEvent()
{
    bool stillCleaning = true;
    while (stillCleaning) {
        stillCleaning = leaseManager->cleanNextLease();
    }
    // Run once per lease term as some will likely have expired by then.
    this->start(Cycles::rdtsc() + Cycles::fromNanoseconds(
            LeaseCommon::LEASE_TERM_US * 1000));
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
LeaseManager::cleanNextLease()
{
    Lock lock(mutex);
    ExpirationOrderSet::iterator it = expirationOrder.begin();
    if (it != expirationOrder.end() && it->leaseExpiration < clock.getTime()) {
        uint64_t leaseId = it->leaseId;
        std::string leaseObjName = STORAGE_PREFIX + "/"
                + format("%lu", leaseId);
        context->externalStorage->remove(leaseObjName.c_str());
        expirationOrder.erase(it);
        leaseMap.erase(leaseId);
        return true;
    }
    return false;
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
LeaseManager::renewLeaseInternal(uint64_t leaseId, Lock &lock)
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
            reserveNextLease(lock);
            RAMCLOUD_CLOG(WARNING, "Lease reservations are not keeping up.");
        }
    }

    clientLease.leaseExpiration = clock.getTime() + LeaseCommon::LEASE_TERM_US;
    leaseMap[clientLease.leaseId] = clientLease.leaseExpiration;
    expirationOrder.insert({clientLease.leaseExpiration, clientLease.leaseId});

    clientLease.timestamp = clock.getTime();

    return clientLease;
}

/**
 * Persist the next available leaseId to external storage.
 *
 * \param lock
 *      Ensures that caller has acquired mutex; not actually used here.
 */
void
LeaseManager::reserveNextLease(Lock &lock)
{
    uint64_t nextLeaseId = maxReservedLeaseId + 1;
    std::string leaseObjName = STORAGE_PREFIX + "/"
            + format("%lu", nextLeaseId);
    context->externalStorage->set(ExternalStorage::Hint::CREATE,
                                  leaseObjName.c_str(),
                                  "", 0);
    maxReservedLeaseId = nextLeaseId;
}

} // namespace RAMCloud

