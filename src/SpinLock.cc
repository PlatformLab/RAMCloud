/* Copyright (c) 2011-2014 Stanford University
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

#include <mutex>

#include "Common.h"
#include "Cycles.h"
#include "Fence.h"
#include "SpinLock.h"

namespace RAMCloud {

/// See SpinLock.h for more details.
namespace SpinLockInternal {
    /**
     * List of all SpinLocks.
     * 
     * There is a function wrapper around this variable to force
     * initialization before usage. This is relevant when SpinLock is
     * initialized in the constructor of a statically declared object.
     */
    SpinLockInternal::SpinLockList* head() {
        static SpinLockInternal::SpinLockList _;
        return &_;
    }

    /** 
     * This mutex protects the list pointed to by "head()".
     * 
     * See the comment above for why this is a function and not a variable.
     */
    std::mutex* lock() {
        static std::mutex _;
        return &_;
    }
} // namespace SpinLockList

/**
 * Construct a new, unnamed SpinLock. This method should be avoided in
 * preference of the one that takes a name argument. It exists mainly to
 * handle cases when arrays of SpinLocks are declared.
 */
SpinLock::SpinLock()
    : mutex(0)
    , name("unnamed")
    , acquisitions(0)
    , contendedAcquisitions(0)
    , contendedTicks(0)
    , listHook()
{
    std::lock_guard<std::mutex> lock(*SpinLockInternal::lock());
    SpinLockInternal::head()->push_back(*this);
}

/**
 * Construct a new SpinLock and give it the provided name.
 */
SpinLock::SpinLock(string name)
    : mutex(0)
    , name(name)
    , acquisitions(0)
    , contendedAcquisitions(0)
    , contendedTicks(0)
    , listHook()
{
    std::lock_guard<std::mutex> lock(*SpinLockInternal::lock());
    SpinLockInternal::head()->push_back(*this);
}

SpinLock::~SpinLock()
{
    std::lock_guard<std::mutex> lock(*SpinLockInternal::lock());
    SpinLockInternal::head()->erase(
            SpinLockInternal::head()->iterator_to(*this));
}

/**
 * Acquire the SpinLock; blocks the thread (by continuously polling the lock)
 * until the lock has been acquired.
 */
void
SpinLock::lock()
{
    uint64_t startOfContention = 0;

    while (mutex.exchange(1) != 0) {
        if (startOfContention == 0)
            startOfContention = Cycles::rdtsc();
    }
    Fence::enter();

    if (startOfContention != 0) {
        contendedTicks += (Cycles::rdtsc() - startOfContention);
        contendedAcquisitions++;
    }
    acquisitions++;
}

/**
 * Try to acquire the SpinLock; does not block the thread and returns
 * immediately.
 *
 * \return
 *      True if the lock was successfully acquired, false if it was already
 *      owned by some other thread.
 */
bool
SpinLock::try_lock()
{
    int old = mutex.exchange(1);
    if (old == 0) {
        Fence::enter();
        return true;
    }
    return false;
}

/**
 * Release the SpinLock.  The caller must previously have acquired the
 * lock with a call to #lock or #try_lock.
 */
void
SpinLock::unlock()
{
    Fence::leave();
    mutex.store(0);
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
    std::lock_guard<std::mutex> lock(*SpinLockInternal::lock());
    SpinLockInternal::SpinLockList::iterator it =
        SpinLockInternal::head()->begin();
    while (it != SpinLockInternal::head()->end()) {
        ProtoBuf::SpinLockStatistics_Lock* lock(stats->add_locks());
        lock->set_name(it->name);
        lock->set_acquisitions(it->acquisitions);
        lock->set_contended_acquisitions(it->contendedAcquisitions);
        lock->set_contended_nsec(Cycles::toNanoseconds(it->contendedTicks));
        it++;
    }
}

} // namespace RAMCloud
