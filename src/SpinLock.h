/* Copyright (c) 2011-2015 Stanford University
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

#ifndef RAMCLOUD_SPINLOCK_H
#define RAMCLOUD_SPINLOCK_H

#include <mutex>
#include <atomic>
#include <x86intrin.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#pragma GCC diagnostic ignored "-Weffc++"
#include "SpinLockStatistics.pb.h"
#pragma GCC diagnostic pop

namespace RAMCloud {

/**
 * This class implements locks that never block the thread: if the lock
 * isn't available during a lock operation, the thread spins until the
 * lock becomes available.  SpinLocks are intended for situations where
 * locks are not held for long periods of time, such as locks used for
 * mutual exclusion.  These locks are not recursive: if a thread attempts
 * to lock a SpinLock while holding it, the thread will deadlock.
 *
 * This class implements the Boost "Lockable" concept, so SpinLocks can be
 * used with the Boost locking facilities.
 */
class SpinLock {
  public:
    explicit SpinLock(string name);
    virtual ~SpinLock();

    /**
     * Acquire the SpinLock; blocks the thread (by continuously polling the
     * lock) until the lock has been acquired.
     */
    inline void
    lock() {
        uint64_t startOfContention = 0;
        do {
            // mutex.exchange() always invalidates the cache line mutex resides
            // in, regardless of whether it succeeded in updating the value or
            // not. To avoid bogus cache invalidation traffic, wait until we
            // observe the lock to be free. This technique is usually called the
            // Test-And-Test-And-Set (TTAS) optimization.
            while (mutex.load(std::memory_order_relaxed)) {
                debugLongWaitAndDeadlock(&startOfContention);
            }
        } while (mutex.exchange(1, std::memory_order_acquire));

        if (startOfContention != 0) {
            contendedTicks += (__rdtsc() - startOfContention);
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
    inline bool
    try_lock()
    {
        if (mutex.load(std::memory_order_relaxed)) {
            return false;
        }
        // exchange returns the previous value of the variable; if it's true,
        // someone else is owning the lock.
        return !mutex.exchange(1, std::memory_order_acquire);
    }

    /**
     * Release the SpinLock.  The caller must previously have acquired the
     * lock with a call to #lock or #try_lock.
     */
    inline void
    unlock()
    {
        mutex.store(0, std::memory_order_release);
    }

    void setName(string name);
    static void getStatistics(ProtoBuf::SpinLockStatistics* stats);
    static int numLocks();

    /*
     * This class automatically acquires a SpinLock on construction and
     * automatically releases it on destruction.
     */
    typedef std::lock_guard<SpinLock> Guard;

  PRIVATE:
    void debugLongWaitAndDeadlock(uint64_t* startOfContention);

    /// Implements the lock: False means free, True means locked.
    std::atomic_bool mutex;

    /// Descriptive name for this SpinLock. Used to identify the purpose of
    /// the lock, what it protects, where it exists in the codebase, etc.
    /// It is used when the getStatistics() method is invoked.
    string name;

    /// Total number of times this lock has been acquired.
    uint64_t acquisitions;

    /// Number of times this lock has been acquired, but not on the first try
    /// (that is, it was already locked).
    uint64_t contendedAcquisitions;

    /// Count of the number of processor ticks spent waiting to acquire this
    /// lock due to it having already been held.
    uint64_t contendedTicks;

    /// True means log when waiting for the lock; intended for unit tests only.
    bool logWaits;
};

/**
 * This class can be used to create unnamed SpinLocks; it's intended
 * for use in array constructors. Making it a subclass has the advantage
 * that programmers must explicitly request it (they can't accidentally
 * forget to provide a name to SpinLock).
 */
class UnnamedSpinLock : public SpinLock {
  public:
    UnnamedSpinLock() : SpinLock("unnamed") {}
};

} // end RAMCloud

#endif  // RAMCLOUD_SPINLOCK_H
