/* Copyright (c) 2014-2015 Stanford University
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

#include <string>

#include "LeaseManager.h"

#include "ExternalStorage.h"
#include "LeaseCommon.h"
#include "ShortMacros.h"

namespace RAMCloud {

/// Defines the number of leases the preallocator should try to keep ahead of
/// the lastIssuedLeaseId.
const uint64_t PREALLOCATION_LIMIT = 1000;

/// Defines the prefix for objects stored in external storage by this module.
const std::string STORAGE_PREFIX = "leaseManager";

LeaseManager::LeaseManager(Context* context)
    : mutex()
    , context(context)
    , clock(context)
    , lastIssuedLeaseId(0)
    , maxAllocatedLeaseId(0)
    , leaseMap()
    , revLeaseMap()
    , preallocator(context, this)
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
 *      the returned the lease id will be 0.
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
        clientLease.leaseTerm = leaseEntry->second;
    } else {
        clientLease.leaseId = 0;
        clientLease.leaseTerm = 0;
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

    foreach (ExternalStorage::Object& object, objects) {
        try {
            std::string name = object.name;
            uint64_t leaseId = std::stoull(name);
            uint64_t leaseTerm = clock.getTime() + LeaseCommon::LEASE_TERM_US;
            leaseMap[leaseId] = leaseTerm;
            revLeaseMap[leaseTerm].insert(leaseId);
        } catch (std::invalid_argument& e) {
            LOG(WARNING, "Unable to recover lease: %s", object.name);
        }
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
 *      leaseId and leaseTerm for the renewed lease or new lease.
 */
WireFormat::ClientLease
LeaseManager::renewLease(uint64_t leaseId)
{
    Lock lock(mutex);
    WireFormat::ClientLease clientLease = renewLeaseInternal(leaseId, lock);

    // Poke the preallocator if we are getting close to running out of leases.
    if (maxAllocatedLeaseId - lastIssuedLeaseId < PREALLOCATION_LIMIT / 4) {
        preallocator.start(0);
    }

    return clientLease;
}

/**
 * Start background timers to perform preallocation and cleaning and also start
 * the cluster clock updater.
 */
void
LeaseManager::startUpdaters()
{
    preallocator.start(0);
    cleaner.start(0);
    clock.startUpdater();
}

/**
 * Constructor for the LeasePreallocator.
 * \param context
 *      Overall information about the RAMCloud server and provides access to
 *      the dispatcher.
 * \param leaseManager
 *      Provides access to the leaseManager to perform preallocation.
 */
LeaseManager::LeasePreallocator::LeasePreallocator(Context* context,
                                                  LeaseManager* leaseManager)
    : WorkerTimer(context->dispatch)
    , leaseManager(leaseManager)
{}

/**
 * The handler performs the lease preallocation.
 */
void
LeaseManager::LeasePreallocator::handleTimerEvent()
{
    uint64_t preallocationCount = 0;
    while (true) {
        LeaseManager::Lock lock(leaseManager->mutex);
        preallocationCount = leaseManager->maxAllocatedLeaseId -
                             leaseManager->lastIssuedLeaseId;
        if (preallocationCount >= PREALLOCATION_LIMIT)
            break;
        leaseManager->allocateNextLease(lock);
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
 * Persist the next available leaseId to external storage=.
 *
 * \param lock
 *      Ensures that caller has acquired mutex; not actually used here.
 */
void
LeaseManager::allocateNextLease(Lock &lock)
{
    uint64_t nextLeaseId = maxAllocatedLeaseId + 1;
    std::string leaseObjName = STORAGE_PREFIX + "/"
            + format("%lu", nextLeaseId);
    std::string str;    // TODO(cstlee): Is there a better way set an empty obj?
    context->externalStorage->set(ExternalStorage::Hint::CREATE,
                                  leaseObjName.c_str(),
                                  str.c_str(), 0);
    maxAllocatedLeaseId = nextLeaseId;
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
    ReverseLeaseMap::iterator it = revLeaseMap.begin();
    while (it != revLeaseMap.end()) {
        if (it->second.empty()) {
            ReverseLeaseMap::iterator next = it;
            ++next;
            revLeaseMap.erase(it);
            it = next;
            continue;
        }
        if (it->first >= clock.getTime()) {
            break;
        }

        uint64_t leaseId = *it->second.begin();
        std::string leaseObjName = STORAGE_PREFIX + "/"
                + format("%lu", leaseId);
        context->externalStorage->remove(leaseObjName.c_str());
        it->second.erase(it->second.begin());
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

    clientLease.leaseTerm = clock.getTime() + LeaseCommon::LEASE_TERM_US;
    leaseMap[clientLease.leaseId] = clientLease.leaseTerm;
    revLeaseMap[clientLease.leaseTerm].insert(clientLease.leaseId);

    clientLease.timestamp = clock.getTime();

    return clientLease;
}

} // namespace RAMCloud

