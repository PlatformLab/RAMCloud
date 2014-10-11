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

#include <string>

#include "LeaseManager.h"
#include "ExternalStorage.h"
#include "ShortMacros.h"

namespace RAMCloud {

/// Defines the period of time that a lease will be extended upon renewal.
const uint64_t LEASE_TERM_MS = 300000;      // 5 min = 300,000 ms

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
    , cleaner(context, this)
{
    recover();
    cleaner.start(0);
}

WireFormat::ClientLease
LeaseManager::renewLease(uint64_t leaseId)
{
    Lock lock(mutex);
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

    clientLease.leaseTerm = clock.getTime() + LEASE_TERM_MS;
    leaseMap[clientLease.leaseId] = clientLease.leaseTerm;
    revLeaseMap[clientLease.leaseTerm].insert(clientLease.leaseId);

    return clientLease;
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
    this->start(Cycles::rdtsc() + Cycles::fromSeconds(LEASE_TERM_MS / 1000));
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
            + std::to_string(static_cast<unsigned long long>(nextLeaseId));
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
                + std::to_string(static_cast<unsigned long long>(leaseId));
        context->externalStorage->remove(leaseObjName.c_str());
        it->second.erase(it->second.begin());
        leaseMap.erase(leaseId);
        return true;
    }
    return false;
}

/**
 * Recover lease information from external storage.  This should only be called
 * once during the construction of the Lease Manager.
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
            uint64_t leaseTerm = clock.getTime() + LEASE_TERM_MS;
            leaseMap[leaseId] = leaseTerm;
            revLeaseMap[leaseTerm].insert(leaseId);
        } catch (std::invalid_argument& e) {
            LOG(WARNING, "Unable to recover lease: %s", object.name);
        }
    }
}

} // namespace RAMCloud

