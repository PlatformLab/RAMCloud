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
#include "PreparedOps.h"

namespace RAMCloud {

/**
 * Constructor for LockTable.
 *
 * \param numEntries
 *      The number of lock entries the table should be able to handle before
 *      overflowing.  This number should be set larger than the expected number
 *      of concurrently held locks. Making this number larger will consume more
 *      memory but will also improve common case performance.
 * \param log
 *      Contains all objects that represent locks managed by this LockTable.
 */
LockTable::LockTable(uint64_t numEntries, Log& log)
    : bucketIndexHashMask(
            BitOps::powerOfTwoGreaterOrEqual(
                    numEntries / (ENTRIES_PER_CACHE_LINE - 1)) - 1)
    , buckets()
    , log(log)
{
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
 * Blocks until the lock for the provided Key can be acquired.
 *
 * \param key
 *      The key whose lock should be acquired.
 * \param lockObjectRef
 *      Reference to the object in log that represents the lock to be acquired.
 *      The object must:
 *          (1) be of type LOG_ENTRY_TYPE_PREP
 *          (2) contain the same key as provided in key
 *          (3) remain live as long as this lock is held
 */
void
LockTable::acquireLock(Key& key, Log::Reference lockObjectRef)
{
    while (!tryAcquireLock(key, lockObjectRef))
        continue;
}

/**
 * Check if lock with the provided key is currently acquired.
 *
 * \param key
 *      The key whose "locked" status should be checked.
 *
 * \return
 *      TRUE if the lock is currently acquired, FALSE otherwise.
 */
bool
LockTable::isLockAcquired(Key& key)
{
    // Find the right bucket.
    uint64_t bucketIndex = (key.getHash() & bucketIndexHashMask);
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
            if (keysMatch(key, cacheLine->entries[entryIndex])) {
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
 * Releases the lock with the provided key and object reference.
 *
 * \param key
 *      The key whose lock should be released if found.
 * \param lockObjectRef
 *      Reference the to object in log that represents the lock to be released
 *      if found.
 *
 * \return
 *      TRUE if a lock represented by lockObjectRef for the given key was found
 *      and released; FALSE otherwise.
 */
bool
LockTable::releaseLock(Key& key, Log::Reference lockObjectRef)
{
    // Find the right bucket.
    uint64_t bucketIndex = (key.getHash() & bucketIndexHashMask);
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
            if (cacheLine->entries[entryIndex] == lockObjectRef.toInteger()) {
                cacheLine->entries[entryIndex] = 0;
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
 * Attempts to acquire the lock for the provided Key without blocking.
 *
 * \param key
 *      The key whose lock should be acquired.
 * \param lockObjectRef
 *      Reference the to object in log that represents the lock to be acquired.
 *      The object must:
 *          (1) be of type LOG_ENTRY_TYPE_PREP
 *          (2) contain the same key as provided in key
 *          (3) remain live as long as this lock is held
 *
 * \return
 *      TRUE if the lock was acquired, FALSE otherwise.
 */
bool
LockTable::tryAcquireLock(Key& key, Log::Reference lockObjectRef)
{
    // Find the right bucket.
    uint64_t bucketIndex = (key.getHash() & bucketIndexHashMask);
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
            } else if (keysMatch(key, cacheLine->entries[entryIndex])) {
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
    *entryPtr = lockObjectRef.toInteger();
    return true;
}

/**
 * Return TRUE if the given key matches the key in the referenced lock object;
 * FALSE otherwise.
 */
bool
LockTable::keysMatch(Key& key, Entry lockObjectRef)
{
    if (lockObjectRef != 0) {
        Log::Reference ref(lockObjectRef);
        Buffer buffer;
        LogEntryType type = log.getEntry(ref, buffer);
        if (type == LOG_ENTRY_TYPE_PREP) {
            PreparedOp prepOp(buffer, 0, buffer.size());
            Key refKey(prepOp.object.getTableId(),
                       prepOp.object.getKey(),
                       prepOp.object.getKeyLength());
            if (key == refKey) {
                return true;
            }
        } else {
            RAMCLOUD_DIE("LockTable contains non LOG_ENTRY_TYPE_PREP type;"
                "found %s instead", LogEntryTypeHelpers::toString(type));
        }
    }
    return false;
}

} // namespace RAMCloud
