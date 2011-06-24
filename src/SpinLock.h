/* Copyright (c) 2011 Stanford University
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

#include "AtomicInt.h"

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
    SpinLock() : mutex(0) {}
    void lock();
    bool try_lock();
    void unlock();
  PRIVATE:
    // Implements the lock: 0 means free, anything else means locked.
    AtomicInt mutex;
};

} // end RAMCloud

#endif  // RAMCLOUD_SPINLOCK_H
