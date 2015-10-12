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

#include "TestUtil.h"       //Has to be first, compiler complains
#include "LockTable.h"
#include "PreparedOps.h"
#include "ServerConfig.h"
#include "MasterTableMetadata.h"

namespace RAMCloud {

class DoNothingHandlers : public LogEntryHandlers {
  public:
    uint32_t getTimestamp(LogEntryType type, Buffer& buffer) { return 0; }
    void relocate(LogEntryType type,
                  Buffer& oldBuffer,
                  Log::Reference oldReference,
                  LogEntryRelocator& relocator) { }
};

class LockTableTest : public ::testing::Test {
  public:
    Context context;
    ServerId serverId;
    ServerList serverList;
    ServerConfig serverConfig;
    ReplicaManager replicaManager;
    MasterTableMetadata masterTableMetadata;
    SegletAllocator allocator;
    SegmentManager segmentManager;
    DoNothingHandlers entryHandlers;
    Log l;
    LockTable lockTable;

    LockTableTest()
        : context()
        , serverId(ServerId(57, 0))
        , serverList(&context)
        , serverConfig(ServerConfig::forTesting())
        , replicaManager(&context, &serverId, 0, false, false)
        , masterTableMetadata()
        , allocator(&serverConfig)
        , segmentManager(&context, &serverConfig, &serverId,
                         allocator, replicaManager, &masterTableMetadata)
        , entryHandlers()
        , l(&context, &serverConfig, &entryHandlers,
            &segmentManager, &replicaManager)
        , lockTable(1, l)
    {
        // Add an additional CacheLine to bucket for testing purposes.
        void *buf = Memory::xmemalign(HERE, sizeof(LockTable::CacheLine),
                                      sizeof(LockTable::CacheLine));
        memset(buf, 0, sizeof(LockTable::CacheLine));
        lockTable.buckets[0].next = static_cast<LockTable::CacheLine *>(buf);
        EXPECT_FALSE(lockTable.buckets[0].next == NULL);
    }

    ~LockTableTest()
    {}

    Log::Reference addPreparedOp(Key& key, Log& log) {
        Buffer buffer;
        Buffer logBuffer;
        Log::Reference ref;
        PreparedOp prepOp(WireFormat::TxPrepare::READ, 1, 1, 1, key, NULL,
                0, 0, 0, buffer);
        prepOp.assembleForLog(logBuffer);
        log.append(LOG_ENTRY_TYPE_PREP, logBuffer, &ref);
        return ref;
    }

    DISALLOW_COPY_AND_ASSIGN(LockTableTest);
};

TEST_F(LockTableTest, constructor) {
    LockTable lt(16 * (LockTable::ENTRIES_PER_CACHE_LINE - 1), l);
    for (uint32_t i = 0; i < 16; i++) {
        for (uint32_t j = 0; j < lt.ENTRIES_PER_CACHE_LINE; j++)
            EXPECT_EQ(0UL, lt.buckets[i].entries[j]);
        EXPECT_TRUE(lt.buckets[i].next == NULL);
    }
}

TEST_F(LockTableTest, constructor_truncate) {
    // This is effectively testing BitOps::powerOfTwoGreaterOrEqual.
    EXPECT_EQ(0x0UL,
            LockTable(1 * (LockTable::ENTRIES_PER_CACHE_LINE - 1),
                    l).bucketIndexHashMask);
    EXPECT_EQ(0x1UL,
            LockTable(2 * (LockTable::ENTRIES_PER_CACHE_LINE - 1),
                    l).bucketIndexHashMask);
    EXPECT_EQ(0x3UL,
            LockTable(3 * (LockTable::ENTRIES_PER_CACHE_LINE - 1),
                    l).bucketIndexHashMask);
    EXPECT_EQ(0x3UL,
            LockTable(4 * (LockTable::ENTRIES_PER_CACHE_LINE - 1),
                    l).bucketIndexHashMask);
    EXPECT_EQ(0x7UL,
            LockTable(5 * (LockTable::ENTRIES_PER_CACHE_LINE - 1),
                    l).bucketIndexHashMask);
    EXPECT_EQ(0x7UL,
            LockTable(6 * (LockTable::ENTRIES_PER_CACHE_LINE - 1),
                    l).bucketIndexHashMask);
    EXPECT_EQ(0x7UL,
            LockTable(7 * (LockTable::ENTRIES_PER_CACHE_LINE - 1),
                    l).bucketIndexHashMask);
    EXPECT_EQ(0x7UL,
            LockTable(8 * (LockTable::ENTRIES_PER_CACHE_LINE - 1),
                    l).bucketIndexHashMask);
}

TEST_F(LockTableTest, acquireLock) {
    Key key(12, "blah", 4);
    Log::Reference ref = addPreparedOp(key, lockTable.log);
    EXPECT_EQ(0UL, lockTable.buckets[0].entries[1]);
    lockTable.acquireLock(key, ref);
    EXPECT_EQ(ref.toInteger(), lockTable.buckets[0].entries[1]);
}

TEST_F(LockTableTest, isLockAcquired_basic) {
    Key key(12, "blah", 4);
    Log::Reference ref = addPreparedOp(key, lockTable.log);
    EXPECT_EQ(0UL, lockTable.buckets[0].next->entries[1]);
    EXPECT_FALSE(lockTable.isLockAcquired(key));
    lockTable.buckets[0].next->entries[0] = ref.toInteger();
    EXPECT_EQ(ref.toInteger(), lockTable.buckets[0].next->entries[0]);
    EXPECT_TRUE(lockTable.isLockAcquired(key));
}

TEST_F(LockTableTest, isLockAcquired_findBucket) {
    LockTable newLockTable(4 * (LockTable::ENTRIES_PER_CACHE_LINE - 1), l);
    Key key(12, "blah", 4);
    Log::Reference ref = addPreparedOp(key, lockTable.log);

    newLockTable.buckets[0].entries[2] = ref.toInteger();
    EXPECT_FALSE(newLockTable.isLockAcquired(key));
    newLockTable.buckets[0].entries[2] = 0;

    newLockTable.buckets[1].entries[2] = ref.toInteger();
    EXPECT_FALSE(newLockTable.isLockAcquired(key));
    newLockTable.buckets[1].entries[2] = 0;

    newLockTable.buckets[2].entries[2] = ref.toInteger();
    EXPECT_TRUE(newLockTable.isLockAcquired(key));
    newLockTable.buckets[2].entries[2] = 0;

    newLockTable.buckets[3].entries[2] = ref.toInteger();
    EXPECT_FALSE(newLockTable.isLockAcquired(key));
}

TEST_F(LockTableTest, releaseLock_basic) {
    Key key(12, "blah", 4);
    Log::Reference ref1 = addPreparedOp(key, lockTable.log);
    Log::Reference ref2 = addPreparedOp(key, lockTable.log);

    lockTable.buckets[0].entries[3] = ref1.toInteger();
    lockTable.buckets[0].next->entries[1] = ref2.toInteger();

    EXPECT_TRUE(lockTable.releaseLock(key, ref2));
    EXPECT_EQ(0UL, lockTable.buckets[0].next->entries[0]);

    EXPECT_FALSE(lockTable.releaseLock(key, ref2));

    EXPECT_TRUE(lockTable.releaseLock(key, ref1));
    EXPECT_EQ(0UL, lockTable.buckets[0].entries[3]);

    EXPECT_FALSE(lockTable.releaseLock(key, ref1));
}

TEST_F(LockTableTest, releaseLock_findBucket) {
    LockTable newLockTable(4 * (LockTable::ENTRIES_PER_CACHE_LINE - 1), l);
    Key key(12, "blah", 4);
    Log::Reference ref = addPreparedOp(key, lockTable.log);

    newLockTable.buckets[0].entries[2] = ref.toInteger();
    EXPECT_FALSE(newLockTable.releaseLock(key, ref));
    newLockTable.buckets[0].entries[2] = 0;

    newLockTable.buckets[1].entries[2] = ref.toInteger();
    EXPECT_FALSE(newLockTable.releaseLock(key, ref));
    newLockTable.buckets[1].entries[2] = 0;

    newLockTable.buckets[2].entries[2] = ref.toInteger();
    EXPECT_TRUE(newLockTable.releaseLock(key, ref));
    newLockTable.buckets[2].entries[2] = 0;

    newLockTable.buckets[3].entries[2] = ref.toInteger();
    EXPECT_FALSE(newLockTable.releaseLock(key, ref));
}

TEST_F(LockTableTest, tryAcquireLock_basic) {
    Key key(12, "blah", 4);
    Log::Reference ref1 = addPreparedOp(key, lockTable.log);
    Log::Reference ref2 = addPreparedOp(key, lockTable.log);
    Key key2(12, "foo", 3);
    EXPECT_TRUE(lockTable.tryAcquireLock(key2,
                                         addPreparedOp(key2, lockTable.log)));

    EXPECT_EQ(0UL, lockTable.buckets[0].entries[2]);
    EXPECT_TRUE(lockTable.tryAcquireLock(key, ref1));
    EXPECT_EQ(ref1.toInteger(), lockTable.buckets[0].entries[2]);
    EXPECT_FALSE(lockTable.tryAcquireLock(key, ref2));
}

TEST_F(LockTableTest, tryAcquireLock_addCacheLine) {
    EXPECT_TRUE(lockTable.buckets[0].next->next == NULL);
    for (uint64_t i = 1; i <= 2 * LockTable::ENTRIES_PER_CACHE_LINE; i++) {
        Key key(12, &i, sizeof(i));
        Log::Reference ref = addPreparedOp(key, lockTable.log);
        EXPECT_TRUE(lockTable.tryAcquireLock(key, ref));
    }
    EXPECT_FALSE(lockTable.buckets[0].next->next == NULL);
}

TEST_F(LockTableTest, tryAcquireLock_findBucket) {
    LockTable newLockTable(4 * (LockTable::ENTRIES_PER_CACHE_LINE - 1), l);
    Key key(12, "blah", 4);
    Log::Reference ref = addPreparedOp(key, newLockTable.log);

    newLockTable.buckets[0].entries[2] = ref.toInteger();
    EXPECT_TRUE(newLockTable.tryAcquireLock(key, ref));
    newLockTable.buckets[0].entries[2] = 0;
    EXPECT_TRUE(newLockTable.releaseLock(key, ref));

    newLockTable.buckets[1].entries[2] = ref.toInteger();
    EXPECT_TRUE(newLockTable.tryAcquireLock(key, ref));
    newLockTable.buckets[1].entries[2] = 0;
    EXPECT_TRUE(newLockTable.releaseLock(key, ref));

    newLockTable.buckets[2].entries[2] = ref.toInteger();
    EXPECT_FALSE(newLockTable.tryAcquireLock(key, ref));
    newLockTable.buckets[2].entries[2] = 0;
    EXPECT_FALSE(newLockTable.releaseLock(key, ref));

    newLockTable.buckets[3].entries[2] = ref.toInteger();
    EXPECT_TRUE(newLockTable.tryAcquireLock(key, ref));
}

TEST_F(LockTableTest, BucketLock_lock) {
    LockTable::Entry lockEntry = 0;
    LockTable::BucketLock* bl =
            reinterpret_cast<LockTable::BucketLock*>(&lockEntry);
    EXPECT_NE(1UL, lockEntry);
    EXPECT_NE(1UL, (*bl).mutex.load());
    (*bl).lock();
    EXPECT_EQ(1UL, lockEntry);
    EXPECT_EQ(1UL, (*bl).mutex.load());
}

TEST_F(LockTableTest, BucketLock_try_lock) {
    LockTable::Entry lockEntry = 0;
    LockTable::BucketLock* bl =
            reinterpret_cast<LockTable::BucketLock*>(&lockEntry);
    EXPECT_NE(1UL, lockEntry);
    EXPECT_NE(1UL, (*bl).mutex.load());
    EXPECT_TRUE((*bl).try_lock());
    EXPECT_EQ(1UL, lockEntry);
    EXPECT_EQ(1UL, (*bl).mutex.load());
    EXPECT_FALSE((*bl).try_lock());
}

TEST_F(LockTableTest, BucketLock_unlock) {
    LockTable::Entry lockEntry = 0;
    LockTable::BucketLock* bl =
            reinterpret_cast<LockTable::BucketLock*>(&lockEntry);
    (*bl).mutex.store(1);
    EXPECT_EQ(1UL, lockEntry);
    EXPECT_EQ(1UL, (*bl).mutex.load());
    (*bl).unlock();
    EXPECT_NE(1UL, lockEntry);
    EXPECT_NE(1UL, (*bl).mutex.load());
}

TEST_F(LockTableTest, keysMatch) {
    Key matchedKey(12, "match", 5);
    Key unmatchedKey(12, "unmatched", 9);
    Log::Reference matchedRef = addPreparedOp(matchedKey, lockTable.log);
    Log::Reference unmatchedRef = addPreparedOp(unmatchedKey, lockTable.log);

    EXPECT_FALSE(lockTable.keysMatch(matchedKey, 0));
    EXPECT_FALSE(lockTable.keysMatch(matchedKey, unmatchedRef.toInteger()));
    EXPECT_TRUE(lockTable.keysMatch(matchedKey, matchedRef.toInteger()));
}

} // namespace RAMCloud
