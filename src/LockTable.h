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
#include "Log.h"

namespace RAMCloud {

/**
 * The LockTable manages a set of durable locks.  Each lock protects a unique
 * %RAMCloud Key and is durably represented by an object in the %RAMCloud Log.
 * The locks can be acquired and released like any typical lock by specifying
 * the Key and a reference to the representative object in the log.  Currently,
 * this LockTable uses PrepareOp objects (type LOG_ENTRY_TYPE_PREP).
 *
 * This class is used to manage per %RAMCloud object locks that protect against
 * concurrent transactions and object modifications to the same objects.
 *
 * This class is thread-safe.
 *
 * \section impl Implementation Details
 *
 * Locks managed in the LockTable are implemented as spin-locks.  Blocking on a
 * lock will result in busying waiting.
 *
 * The LockTable is optimized so that the following common cases are fast
 * assuming the table is sparsely populated:
 *  + Checking if lock is acquired when it is not (used for write and deletes).
 *  + Acquiring a lock when it is not already acquired (used for transactions).
 *  + Releasing a lock (used for transactions).
 *
 * The LockTable is an array of #buckets. Locks are "hashed" into buckets based
 * on the lower n bits of the Key's KeyHash (n is such that 2^n is the number of
 * buckets); the implementation assumes that KeyHashes are relatively uniformly
 * distributed.
 *
 * Each bucket consists of a one or more chained \link CacheLine
 * CacheLines\endlink, the first of which lives inline in the array of buckets.
 * Each cache line consists of several LockTable Entry objects; each Entry
 * contains the reference to the object in the log that represents an acquired
 * lock or 0; only acquired locks are kept in the LockTable.  The first entry in
 * a bucket's first cache line is specially designated and is cast and used as
 * the bucket's \link BucketLock BucketLock\endlink; this is a monitor-style
 * spin-lock that protects against concurrent accesses to the same bucket.
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
    LockTable(uint64_t numEntries, Log& log);
    virtual ~LockTable();

    void acquireLock(Key& key, Log::Reference lockObjectRef);
    bool isLockAcquired(Key& key);
    bool releaseLock(Key& key, Log::Reference lockObjectRef);
    bool tryAcquireLock(Key& key, Log::Reference lockObjectRef);

  PRIVATE:
    // Forward declaration for CacheLine.
    struct CacheLine;

    /**
     * A lock table entry.
     *
     * Lock table entries live in \link CacheLine CacheLines\endlink.  A lock
     * table entry holds the log reference to an object in the log which
     * represents an acquired lock on a specific key.
     */
    typedef uint64_t Entry;

    /**
     * The number of bytes per cache line in this machine.
     */
    static const uint32_t BYTES_PER_CACHE_LINE = 64;

    /**
     * The number of LockTable Entry objects in a CacheLine.
     */
    static const uint32_t ENTRIES_PER_CACHE_LINE =
            (BYTES_PER_CACHE_LINE - sizeof(CacheLine*))     // NOLINT
            / sizeof(Entry);
    static_assert(ENTRIES_PER_CACHE_LINE > 0,
                  "BYTES_PER_CACHE_LINE too small to fit Entry");

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
        /// Array of lock table entries.  An entry value of 0 means the entry is
        /// empty and does not hold a lock.  If the CacheLine is the first in
        /// bucket, entries[0] is commandeered to hold the BucketLock.
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
     * BucketLocks must fit in a lock table entry's memory footprint.
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
    static_assert(sizeof(BucketLock) == sizeof(Entry),
                  "LockTable::BucketLock is not same size as LockTable::Entry");

    /**
     * Bit mask used to determine the bucket in which a particular Key will
     * reside.  If the LockTable contains 2^N buckets, the lower N bits will
     * be used as the bucket index.
     */
    const uint64_t bucketIndexHashMask;

    /**
     * Pointer to memory used to hold the main set of buckets.  Memory pointed
     * to by this pointer is dynamically allocated.
     */
    CacheLine* buckets;

    /**
     * Reference to the log that contains objects that represent locks.
     */
    Log& log;

    bool keysMatch(Key& key, Entry lockObjectRef);

    DISALLOW_COPY_AND_ASSIGN(LockTable);
};

} // namespace RAMCloud

#endif  /* RAMCLOUD_LOCKTABLE_H */
