/* Copyright (c) 2011-2014 Stanford University
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

#include "Atomic.h"
#include "SpinLockStatistics.pb.h"

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
    SpinLock();
    explicit SpinLock(string name);
    ~SpinLock();
    void lock();
    bool try_lock();
    void unlock();
    void setName(string name);
    static void getStatistics(ProtoBuf::SpinLockStatistics* stats);
    static int numLocks();

  PRIVATE:
    /// Implements the lock: 0 means free, anything else means locked.
    Atomic<int> mutex;

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
};

} // end RAMCloud

#endif  // RAMCLOUD_SPINLOCK_H
