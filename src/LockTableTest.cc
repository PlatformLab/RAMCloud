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


namespace RAMCloud {

class LockTableTest : public ::testing::Test {
  public:
    LockTable lockTable;

    LockTableTest()
        : lockTable(2)
    {
        // Force an additional CacheLine to bucket 1 for testing purposes.
        for (uint64_t i = 0; i < LockTable::ENTRIES_PER_CACHE_LINE; i++) {
            lockTable.acquireLock(2 * i + 1);
        }
        for (uint64_t i = 0; i < LockTable::ENTRIES_PER_CACHE_LINE; i++) {
            lockTable.releaseLock(2 * i + 1);
        }
        EXPECT_FALSE(lockTable.buckets[1].next == NULL);
    }

    ~LockTableTest()
    {}

    DISALLOW_COPY_AND_ASSIGN(LockTableTest);
};

TEST_F(LockTableTest, constructor) {
    LockTable lt(16);
    for (uint32_t i = 0; i < 16; i++) {
        for (uint32_t j = 0; j < lt.ENTRIES_PER_CACHE_LINE; j++)
            EXPECT_EQ(0UL, lt.buckets[i].entries[j]);
        EXPECT_TRUE(lt.buckets[i].next == NULL);
    }
}

TEST_F(LockTableTest, constructor_truncate) {
    // This is effectively testing BitOps::powerOfTwoLessOrEqual.
    EXPECT_EQ(0x0UL, LockTable(1).bucketIndexHashMask);
    EXPECT_EQ(0x1UL, LockTable(2).bucketIndexHashMask);
    EXPECT_EQ(0x1UL, LockTable(3).bucketIndexHashMask);
    EXPECT_EQ(0x3UL, LockTable(4).bucketIndexHashMask);
    EXPECT_EQ(0x3UL, LockTable(5).bucketIndexHashMask);
    EXPECT_EQ(0x3UL, LockTable(6).bucketIndexHashMask);
    EXPECT_EQ(0x3UL, LockTable(7).bucketIndexHashMask);
    EXPECT_EQ(0x7UL, LockTable(8).bucketIndexHashMask);
}

TEST_F(LockTableTest, acquireLock) {
    EXPECT_EQ(0UL, lockTable.buckets[1].entries[1]);
    lockTable.acquireLock(1);
    EXPECT_EQ(1UL, lockTable.buckets[1].entries[1]);
}

TEST_F(LockTableTest, isLockAcquired) {
    EXPECT_EQ(0UL, lockTable.buckets[1].next->entries[1]);
    EXPECT_FALSE(lockTable.isLockAcquired(1));
    lockTable.buckets[1].next->entries[1] = 1;
    EXPECT_EQ(1UL, lockTable.buckets[1].next->entries[1]);
    EXPECT_TRUE(lockTable.isLockAcquired(1));
}

TEST_F(LockTableTest, releaseLock_basic) {
    lockTable.buckets[0].entries[2] = 2;
    lockTable.buckets[1].entries[1] = 1;

    EXPECT_EQ(1UL, lockTable.buckets[1].entries[1]);
    lockTable.releaseLock(1);
    EXPECT_EQ(0UL, lockTable.buckets[1].entries[1]);

    EXPECT_EQ(2UL, lockTable.buckets[0].entries[2]);
    lockTable.releaseLock(2);
    EXPECT_EQ(0UL, lockTable.buckets[0].entries[2]);
}

TEST_F(LockTableTest, releaseLock_overflow) {
    lockTable.buckets[1].next->entries[3] = 3;

    EXPECT_EQ(3UL, lockTable.buckets[1].next->entries[3]);
    lockTable.releaseLock(3);
    EXPECT_EQ(0UL, lockTable.buckets[1].next->entries[3]);
}

TEST_F(LockTableTest, releaseLock_notFound) {
    lockTable.releaseLock(42);
}

TEST_F(LockTableTest, tryAcquireLock_basic) {
    EXPECT_EQ(0UL, lockTable.buckets[1].entries[1]);
    EXPECT_TRUE(lockTable.tryAcquireLock(1));
    EXPECT_EQ(1UL, lockTable.buckets[1].entries[1]);
    EXPECT_FALSE(lockTable.tryAcquireLock(1));
}

TEST_F(LockTableTest, tryAcquireLock_addCacheLine) {
    EXPECT_TRUE(lockTable.buckets[0].next == NULL);
    for (uint64_t i = 1; i <= LockTable::ENTRIES_PER_CACHE_LINE; i++) {
        lockTable.acquireLock(2 * i);
    }
    EXPECT_FALSE(lockTable.buckets[0].next == NULL);
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

} // namespace RAMCloud
