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

#include "LockTable.h"
#include "BitOps.h"
#include "Memory.h"

namespace RAMCloud {

/**
 * Constructor for LockTable.
 *
 * \param numBuckets
 *      Locks in a LockTable are stored in one or more buckets.  This parameter
 *      sets the number of buckets that should be used for this new LockTable.
 *      This value should be a power of two.
 *
 * \throw Exception
 *      An exception is thrown if the resulting number of buckets is 0.
 */
LockTable::LockTable(uint64_t numBuckets)
    : bucketIndexHashMask(BitOps::powerOfTwoLessOrEqual(numBuckets) - 1)
    , buckets()
{
    if (numBuckets != (bucketIndexHashMask + 1)) {
        RAMCLOUD_LOG(DEBUG,
                     "LockTable truncated to %lu buckets "
                     "(nearest power of two)",
                     (bucketIndexHashMask + 1));
    }

    if ((bucketIndexHashMask + 1) == 0)
        throw Exception(HERE, "LockTable numBuckets == 0?!");

    void *buf  = Memory::xmemalign(
            HERE,
            sizeof(CacheLine),
            (bucketIndexHashMask + 1) * sizeof(CacheLine));
    memset(buf, 0, (bucketIndexHashMask + 1) * sizeof(CacheLine));
    buckets = static_cast<CacheLine *>(buf);
}

/**
 * Destructor for LockTable.
 */
LockTable::~LockTable() {
    // Free any extra xmalloced CacheLines.
    for (uint64_t i = 0; i < bucketIndexHashMask + 1; i++) {
        CacheLine* cacheLine = buckets[i].next;
        while (cacheLine != NULL) {
            CacheLine* next = cacheLine->next;
            free(cacheLine);
            cacheLine = next;
        }
    }
    free(buckets);
}

/**
 * Blocks until the lock with the provided lockId can be acquired.
 *
 * \param lockId
 *      ID of lock to be acquired.  ID must not be zero.
 */
void
LockTable::acquireLock(uint64_t lockId)
{
    while (!tryAcquireLock(lockId))
        continue;
}

/**
 * Check if lock with the provided lockId is currently acquired.
 *
 * \param lockId
 *      ID of lock to be acquired.  ID must not be zero.
 * \return
 *      TRUE if the lock is currently acquired, FALSE otherwise.
 */
bool
LockTable::isLockAcquired(uint64_t lockId)
{
    // Find the right bucket.
    uint64_t bucketIndex = (lockId & bucketIndexHashMask);
    CacheLine* cacheLine = &buckets[bucketIndex];

    // Lock the bucket
    BucketLock* bucketLock =
            reinterpret_cast<BucketLock*>(&cacheLine->entries[0]);
    std::lock_guard<BucketLock> lock(*bucketLock);

    // The zeroth entry in the first CacheLine is the BucketLock so start the
    // index at 1.
    uint32_t entryIndex = 1;
    while (true) {
        for (; entryIndex < ENTRIES_PER_CACHE_LINE; entryIndex++) {
            if (cacheLine->entries[entryIndex] == lockId) {
                return true;
            }
        }
        if (cacheLine->next != NULL) {
            entryIndex = 0;
            cacheLine = cacheLine->next;
        } else {
            break;
        }
    }
    return false;
}

/**
 * Releases the lock with the provided lockId.
 *
 * \param lockId
 *      ID of lock to be unlocked.  ID must not be zero.
 */
void
LockTable::releaseLock(uint64_t lockId)
{
    // Find the right bucket.
    uint64_t bucketIndex = (lockId & bucketIndexHashMask);
    CacheLine* cacheLine = &buckets[bucketIndex];

    // Lock the bucket
    BucketLock* bucketLock =
            reinterpret_cast<BucketLock*>(&cacheLine->entries[0]);
    std::lock_guard<BucketLock> lock(*bucketLock);

    // The zeroth entry in the first CacheLine is the BucketLock so start the
    // index at 1.
    uint32_t entryIndex = 1;
    while (true) {
        for (; entryIndex < ENTRIES_PER_CACHE_LINE; entryIndex++) {
            if (cacheLine->entries[entryIndex] == lockId) {
                cacheLine->entries[entryIndex] = 0;
                return;
            }
        }
        if (cacheLine->next != NULL) {
            entryIndex = 0;
            cacheLine = cacheLine->next;
        } else {
            break;
        }
    }
}

/**
 * Attempts to acquire the lock with the provided lockId without blocking.
 *
 * \param lockId
 *      ID of lock to be acquired.  ID must not be zero.
 *
 * \return
 *      TRUE if the lock was acquired, FALSE otherwise.
 */
bool
LockTable::tryAcquireLock(uint64_t lockId)
{
    // Find the right bucket.
    uint64_t bucketIndex = (lockId & bucketIndexHashMask);
    CacheLine* cacheLine = &buckets[bucketIndex];

    // Lock the bucket
    BucketLock* bucketLock =
            reinterpret_cast<BucketLock*>(&cacheLine->entries[0]);
    std::lock_guard<BucketLock> lock(*bucketLock);

    int overflowCacheLineCnt = 0;

    // If there is an empty entry slot, store its address here.
    Entry* entryPtr = NULL;

    // The zeroth entry in the first CacheLine is the BucketLock so start
    // the index at 1.
    uint32_t entryIndex = 1;
    while (true) {
        for (; entryIndex < ENTRIES_PER_CACHE_LINE; entryIndex++) {
            if (entryPtr == NULL && cacheLine->entries[entryIndex] == 0) {
                entryPtr = &cacheLine->entries[entryIndex];
            } else if (cacheLine->entries[entryIndex] == lockId) {
                return false;
            }
        }
        if (cacheLine->next != NULL) {
            entryIndex = 0;
            cacheLine = cacheLine->next;
            overflowCacheLineCnt++;
        } else {
            break;
        }
    }

    // We need another CacheLine
    if (entryPtr == NULL) {
        overflowCacheLineCnt++;
        RAMCLOUD_CLOG(NOTICE, "Allocating overflow cache line %d for index %lu",
                    overflowCacheLineCnt, bucketIndex);

        void *buf = Memory::xmemalign(HERE, sizeof(CacheLine),
                                      sizeof(CacheLine));
        memset(buf, 0, sizeof(CacheLine));

        cacheLine->next = static_cast<CacheLine *>(buf);
        entryPtr = &cacheLine->next->entries[0];
    }

    // Assign lock
    *entryPtr = lockId;
    return true;
}

} // namespace RAMCloud
