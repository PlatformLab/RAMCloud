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

#ifndef RAMCLOUD_LOCKTABLE_H
#define RAMCLOUD_LOCKTABLE_H

#include "Common.h"

#include "Atomic.h"
#include "Fence.h"

namespace RAMCloud {

/**
 * The LockTable represents a collection a locks with unique IDs.  The locks can
 * be acquired and released like any typical lock based our their lockId. The
 * only restriction on the 64-bit lockId is that the value 0 can not be used.
 *
 * This class is used, for instance, to manage per %RAMCloud object write locks
 * that protect against concurrent write transactions and write requests on the
 * same object.  In this case, the lockId can be the address of the object in
 * the %RAMCloud log.
 *
 * This class is thread-safe.
 *
 * \section impl Implementation Details
 *
 * Locks represented in the LockTable are implemented as spin-locks.  Blocking
 * on a lock will result in busying waiting.
 *
 * The LockTable is an array of #buckets.  Locks are "hashed" into buckets based
 * on the lower n-1 bits of the lockId (n is such that 2^n is the number of
 * buckets); the implementation assumes that lockIds are relatively uniformly
 * distributed.
 *
 * Each bucket consists of a one or more chained \link CacheLine
 * CacheLines\endlink, the first of which lives inline in the array of buckets.
 * Each cache line consists of several LockTable Entry objects; each Entry
 * contains the lockId of an acquired lock or 0; only acquired locks are
 * kept in the LockTable.  In exception, the first entry in a bucket's first
 * cache line is specially designated and is cast and used as the bucket's
 * \link BucketLock BucketLock\endlink; this is a monitor-style spin-lock that
 * protects against concurrent accesses to the same bucket.
 *
 * If there are two many acquired locks to fit in the bucket's first cache line,
 * additional overflow cache lines are allocated (outside the array of buckets)
 * and chained together.  Cache lines are never removed from buckets; lock
 * acquisition performance in a particular bucket degrades as more cache lines
 * are added.
 *
 * For best performance, the number of buckets should be set large enough so
 * that overflow cache lines are almost never needed but small enough that the
 * entire structure might fit in CPU cache.
 */
class LockTable {
  PUBLIC:
    explicit LockTable(uint64_t numBuckets);
    virtual ~LockTable();

    void acquireLock(uint64_t lockId);
    bool isLockAcquired(uint64_t lockId);
    void releaseLock(uint64_t lockId);
    bool tryAcquireLock(uint64_t lockId);

  PRIVATE:
    /**
     * A lock table entry.
     *
     * Lock table entries live in \link CacheLine CacheLines\endlink.  A lock
     * table entry holds the lockId of an acquired lock.
     */
    typedef uint64_t Entry;
    static_assert(sizeof(Entry) == 8, "LockTable::Entry is not 8 bytes");

    /**
     * The number of bytes per cache line in this machine.
     */
    static const uint32_t BYTES_PER_CACHE_LINE = 64;

    /**
     * The number of LockTable Entry objects in a CacheLine.
     */
    static const uint32_t ENTRIES_PER_CACHE_LINE = (BYTES_PER_CACHE_LINE /
                                                    sizeof(Entry)) - 1;
    static_assert(BYTES_PER_CACHE_LINE % sizeof(Entry) == 0,
                  "BYTES_PER_CACHE_LINE not a multiple of sizeof(Entry)");

    /**
     * A linked list of cache lines composes a bucket within the LockTable.
     *
     * Each cache line is composed of several lock table entries and a pointer
     * to the next overflow cache line (if any).
     *
     * A CacheLine is meant to fit on a single L2 cache line on the CPU.
     * Different processors may require tweaking ENTRIES_PER_CACHE_LINE to
     * achieve this.
     */
    struct CacheLine {
        /// Array of lock table entries (acquired lockIds).
        Entry entries[ENTRIES_PER_CACHE_LINE];
        /// Pointer to next cache line in bucket; NULL if last in chain.
        CacheLine* next;
    };
    static_assert(sizeof(CacheLine) == BYTES_PER_CACHE_LINE,
                  "sizeof(CacheLine) does not match BYTES_PER_CACHE_LINE");

    /**
     * The BucketLock is a spin-lock style lock used to prevent concurrent
     * access to individual LockTable buckets.
     *
     * Bucket locks live in memory of the first lock table Entry of a bucket's
     * first cache line.  As such, bucket locks must fit in a lock table entry's
     * memory footprint.
     */
    struct BucketLock {
        /**
         * Blocks until a lock can be obtained.
         */
        void lock()
        {
            while (mutex.exchange(1) != 0)
                continue;

            Fence::enter();
        }

        /**
         * Attempts to acquire the lock for without blocking.
         *
         * \return
         *      TRUE if the lock was acquired, FALSE otherwise.
         */
        bool try_lock()
        {
            uint64_t old = mutex.exchange(1);
            if (old == 0) {
                Fence::enter();
                return true;
            }
            return false;
        }

        /**
         * Releases the lock.
         */
        void unlock()
        {
            Fence::leave();
            mutex.store(0);
        }

        /// Implements the lock: 0 means free, anything else means locked.
        Atomic<uint64_t> mutex;
    };
    static_assert(sizeof(BucketLock) == 8,
                  "LockTable::BucketLock is not 8 bytes");

    /**
     * Bit masked used to determine the bucket in which a particular lockId will
     * reside.  If the LockTable contains 2^N buckets, the lower N-1 bits will
     * be used as the bucket index.
     */
    const uint64_t bucketIndexHashMask;

    /**
     * Pointer to memory used to hold the main set of buckets.
     */
    CacheLine* buckets;

    DISALLOW_COPY_AND_ASSIGN(LockTable);
};

} // namespace RAMCloud

#endif  /* RAMCLOUD_LOCKTABLE_H */
