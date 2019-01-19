/* Copyright (c) 2011-2015 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <unordered_set>

#include "Cycles.h"
#include "Logger.h"
#include "SpinLock.h"

namespace RAMCloud {

/// This namespace is used to keep track of all of the SpinLocks currently
/// in existence, so that we can enumerate them to monitor lock contention.
namespace SpinLockTable {
    /**
     * Returns a structure containing the addresses of all SpinLocks
     * currently in existence.
     * 
     * There is a function wrapper around this variable to force
     * initialization before usage. This is relevant when SpinLock is
     * initialized in the constructor of a statically declared object.
     */
    std::unordered_set<SpinLock*>* allLocks() {
        static std::unordered_set<SpinLock*> map;
        return &map;
    }

    /** 
     * This mutex protects the map pointed to by "allLocks()".
     * 
     * See the comment above for why this is a function and not a variable.
     */
    std::mutex* lock() {
        static std::mutex mutex;
        return &mutex;
    }
} // namespace SpinLockTable

/**
 * Construct a new SpinLock and give it the provided name.
 */
SpinLock::SpinLock(string name)
    : mutex(0)
    , name(name)
    , acquisitions(0)
    , contendedAcquisitions(0)
    , contendedTicks(0)
    , logWaits(false)
{
    std::lock_guard<std::mutex> lock(*SpinLockTable::lock());
    SpinLockTable::allLocks()->insert(this);
}

SpinLock::~SpinLock()
{
    std::lock_guard<std::mutex> lock(*SpinLockTable::lock());
    SpinLockTable::allLocks()->erase(this);
}

/**
 * Change the name of the SpinLock. The name is intended to give some hint as
 * to the purpose of the lock, where it was declared, etc.
 *
 * \param name
 *      The string name to give this lock.
 */
void
SpinLock::setName(string name)
{
    this->name = name;
}

/**
 * Fill the given protocol buffer with statistics on every SpinLock currently
 * instantiated. This is used to remotely monitor lock contention.
 *
 * \param stats
 *      Pointer to the SpinLockStatistics protobuf that will be filled in.
 */
void
SpinLock::getStatistics(ProtoBuf::SpinLockStatistics* stats)
{
    std::lock_guard<std::mutex> lock(*SpinLockTable::lock());
    std::unordered_set<SpinLock*>* allLocks =
            SpinLockTable::allLocks();
    std::unordered_set<SpinLock*>::iterator it = allLocks->begin();
    while (it != allLocks->end()) {
        SpinLock* spin = *it;
        ProtoBuf::SpinLockStatistics_Lock* lock(stats->add_locks());
        lock->set_name(spin->name);
        lock->set_acquisitions(spin->acquisitions);
        lock->set_contended_acquisitions(spin->contendedAcquisitions);
        lock->set_contended_nsec(Cycles::toNanoseconds(spin->contendedTicks));
        it++;
    }
}

/**
 * Return the total of SpinLocks currently in existence; intended
 * primarily for testing.
 */
int
SpinLock::numLocks()
{
    std::lock_guard<std::mutex> lock(*SpinLockTable::lock());
    return downCast<int>(SpinLockTable::allLocks()->size());
}

/**
 * Log a warning if we have been stuck at acquiring the lock for too long;
 * intended primarily for debugging.
 *
 * This method is extracted from SpinLock::lock to avoid having to include
 * "Logger.h" in the header file.
 *
 * \param[out] startOfContention
 *      Time, in rdtsc ticks, when we first tried to acquire the lock.
 */
void
SpinLock::debugLongWaitAndDeadlock(uint64_t* startOfContention)
{
    if (*startOfContention == 0) {
        *startOfContention = Cycles::rdtsc();
        if (logWaits) {
            RAMCLOUD_TEST_LOG("Waiting on SpinLock");
        }
    } else {
        uint64_t now = Cycles::rdtsc();
        if (now >= *startOfContention + uint64_t(Cycles::perSecond())) {
            RAMCLOUD_LOG(WARNING,
                    "%s SpinLock locked for one second; deadlock?",
                    name.c_str());
            contendedTicks += now - *startOfContention;
            *startOfContention = now;
        }
    }
}

} // namespace RAMCloud
