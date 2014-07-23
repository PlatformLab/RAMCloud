/* Copyright (c) 2012-2014 Stanford University
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

#include "TestUtil.h"

#include "Seglet.h"
#include "ServerConfig.h"

namespace RAMCloud {

/**
 * Unit tests for Seglet.
 */
class SegletAllocatorTest : public ::testing::Test {
  public:
    SegletAllocatorTest()
        : serverConfig(ServerConfig::forTesting()),
          allocator(&serverConfig)
    {
    }

    ServerConfig serverConfig;
    SegletAllocator allocator;

    DISALLOW_COPY_AND_ASSIGN(SegletAllocatorTest);
};

TEST_F(SegletAllocatorTest, constructor) {
    EXPECT_EQ(serverConfig.segletSize, allocator.segletSize);
    EXPECT_EQ(0U, allocator.emergencyHeadPoolReserve);
    EXPECT_EQ(0U, allocator.emergencyHeadPool.size());
    EXPECT_EQ(0U, allocator.cleanerPoolReserve);
    EXPECT_EQ(0U, allocator.cleanerPool.size());
    EXPECT_EQ(serverConfig.master.logBytes / serverConfig.segletSize,
        allocator.defaultPool.size());
}

TEST_F(SegletAllocatorTest, destructor) {
    TestLog::Enable _;
    Tub<SegletAllocator> allocator2;
    allocator2.construct(&serverConfig);
    vector<Seglet*> seglets;
    allocator2->alloc(SegletAllocator::DEFAULT, 1, seglets);
    allocator2.destroy();
    EXPECT_EQ("~SegletAllocator: Destructor called before all seglets freed!",
        TestLog::get());
    delete seglets[0];
}

TEST_F(SegletAllocatorTest, alloc) {
    vector<Seglet*> seglets;

    allocator.initializeEmergencyHeadReserve(1);
    EXPECT_EQ(1U, allocator.emergencyHeadPool.size());
    EXPECT_TRUE(allocator.alloc(SegletAllocator::EMERGENCY_HEAD, 1, seglets));
    EXPECT_EQ(0U, allocator.emergencyHeadPool.size());
    EXPECT_FALSE(allocator.alloc(SegletAllocator::EMERGENCY_HEAD, 1, seglets));

    allocator.initializeCleanerReserve(1);
    EXPECT_EQ(1U, allocator.cleanerPool.size());
    EXPECT_TRUE(allocator.alloc(SegletAllocator::CLEANER, 1, seglets));
    EXPECT_EQ(0U, allocator.cleanerPool.size());
    EXPECT_FALSE(allocator.alloc(SegletAllocator::CLEANER, 1, seglets));

    EXPECT_EQ(318U, allocator.defaultPool.size());
    EXPECT_TRUE(allocator.alloc(SegletAllocator::DEFAULT, 254, seglets));
    EXPECT_EQ(0U, allocator.cleanerPool.size());

    foreach (Seglet* s, seglets)
        s->free();
}

TEST_F(SegletAllocatorTest, initializeEmergencyHeadReserve) {
    allocator.emergencyHeadPoolReserve = 1;
    EXPECT_FALSE(allocator.initializeEmergencyHeadReserve(1));
    EXPECT_EQ(0U, allocator.emergencyHeadPool.size());
    allocator.emergencyHeadPoolReserve = 0;

    uint32_t maxSeglets = downCast<uint32_t>(allocator.defaultPool.size());
    EXPECT_FALSE(allocator.initializeEmergencyHeadReserve(maxSeglets + 1));
    EXPECT_EQ(0U, allocator.emergencyHeadPool.size());

    EXPECT_TRUE(allocator.initializeEmergencyHeadReserve(maxSeglets));
    EXPECT_EQ(maxSeglets, allocator.emergencyHeadPool.size());
    foreach (Seglet* s, allocator.emergencyHeadPool)
        EXPECT_EQ(&allocator.emergencyHeadPool, s->getSourcePool());
}

TEST_F(SegletAllocatorTest, initializeCleanerReserve) {
    allocator.cleanerPoolReserve = 1;
    EXPECT_FALSE(allocator.initializeCleanerReserve(1));
    EXPECT_EQ(0U, allocator.cleanerPool.size());
    allocator.cleanerPoolReserve = 0;

    uint32_t maxSeglets = downCast<uint32_t>(allocator.defaultPool.size());
    EXPECT_FALSE(allocator.initializeCleanerReserve(maxSeglets + 1));
    EXPECT_EQ(0U, allocator.cleanerPool.size());

    EXPECT_TRUE(allocator.initializeCleanerReserve(maxSeglets));
    EXPECT_EQ(maxSeglets, allocator.cleanerPool.size());
    foreach (Seglet* s, allocator.cleanerPool) {
        EXPECT_EQ(static_cast<const vector<Seglet*>*>(NULL),
                  s->getSourcePool());
    }
}

TEST_F(SegletAllocatorTest, free) {
    vector<Seglet*> seglets;

    EXPECT_TRUE(allocator.alloc(SegletAllocator::DEFAULT, 2, seglets));
    seglets[0]->setSourcePool(&allocator.emergencyHeadPool);
    EXPECT_EQ(0U, allocator.emergencyHeadPool.size());
    allocator.free(seglets[0]);
    EXPECT_EQ(1U, allocator.emergencyHeadPool.size());

    allocator.emergencyHeadPool.clear();
    seglets[0]->setSourcePool(NULL);
    allocator.cleanerPoolReserve = 1;
    EXPECT_EQ(0U, allocator.cleanerPool.size());
    allocator.free(seglets[0]);
    EXPECT_EQ(1U, allocator.cleanerPool.size());

    uint32_t defaultSeglets = downCast<uint32_t>(allocator.defaultPool.size());
    allocator.free(seglets[1]);
    EXPECT_EQ(defaultSeglets + 1, allocator.defaultPool.size());
}

TEST_F(SegletAllocatorTest, getFreeCount) {
    size_t defaultSeglets = allocator.defaultPool.size();

    EXPECT_EQ(0U, allocator.getFreeCount(SegletAllocator::EMERGENCY_HEAD));
    allocator.initializeEmergencyHeadReserve(2);
    EXPECT_EQ(2U, allocator.getFreeCount(SegletAllocator::EMERGENCY_HEAD));

    EXPECT_EQ(0U, allocator.getFreeCount(SegletAllocator::CLEANER));
    allocator.initializeCleanerReserve(3);
    EXPECT_EQ(3U, allocator.getFreeCount(SegletAllocator::CLEANER));

    EXPECT_EQ(defaultSeglets - 5,
              allocator.getFreeCount(SegletAllocator::DEFAULT));
}

TEST_F(SegletAllocatorTest, getMemoryUtilization) {
    vector<Seglet*> seglets;
    uint32_t quarter = downCast<uint32_t>(allocator.getTotalCount()) / 4;

    EXPECT_EQ(0, allocator.getMemoryUtilization());
    allocator.alloc(SegletAllocator::DEFAULT, quarter, seglets);
    EXPECT_EQ(25, allocator.getMemoryUtilization());

    foreach (Seglet* s, seglets)
        s->free();
    seglets.clear();

    EXPECT_EQ(0, allocator.getMemoryUtilization());

    allocator.initializeEmergencyHeadReserve(quarter);
    EXPECT_EQ(0, allocator.getMemoryUtilization());

    allocator.initializeCleanerReserve(quarter);
    EXPECT_EQ(0, allocator.getMemoryUtilization());

    allocator.alloc(SegletAllocator::DEFAULT, quarter, seglets);
    EXPECT_EQ(50, allocator.getMemoryUtilization());

    foreach (Seglet* s, seglets)
        s->free();
    seglets.clear();

    EXPECT_EQ(0, allocator.getMemoryUtilization());
}

TEST_F(SegletAllocatorTest, allocFromPool) {
    vector<Seglet*> seglets;
    uint32_t maxSeglets = downCast<uint32_t>(allocator.defaultPool.size());

    EXPECT_FALSE(allocator.allocFromPool(allocator.defaultPool,
                                         maxSeglets + 1,
                                         seglets));

    EXPECT_EQ(maxSeglets, allocator.defaultPool.size());
    EXPECT_EQ(0U, seglets.size());
    EXPECT_TRUE(allocator.allocFromPool(allocator.defaultPool,
                                        maxSeglets,
                                        seglets));
    EXPECT_EQ(0U, allocator.defaultPool.size());
    EXPECT_EQ(maxSeglets, seglets.size());

    // return to allocator
    allocator.allocFromPool(seglets, maxSeglets, allocator.defaultPool);
}

} // namespace RAMCloud
