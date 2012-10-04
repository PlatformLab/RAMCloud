/* Copyright (c) 2010-2012 Stanford University
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

/**
 * \file
 * Unit tests for LogCleaner.
 */

#include "TestUtil.h"

#include "Segment.h"
#include "SegmentIterator.h"
#include "SegmentManager.h"
#include "ServerConfig.h"
#include "ServerId.h"
#include "ServerList.h"
#include "StringUtil.h"
#include "LogEntryTypes.h"
#include "LogCleaner.h"
#include "ReplicaManager.h"
#include "WallTime.h"

namespace RAMCloud {

class TestEntryHandlers : public LogEntryHandlers {
  public:
    TestEntryHandlers()
        : timestamp(0),
          attemptToRelocate(false)
    {
    }

    uint32_t
    getTimestamp(LogEntryType type, Buffer& buffer)
    {
        return timestamp;
    }

    void
    relocate(LogEntryType type,
             Buffer& oldBuffer,
             LogEntryRelocator& relocator)
    {
        if (attemptToRelocate)
            relocator.append(type, oldBuffer, timestamp);
    }

    uint32_t timestamp;
    bool attemptToRelocate; 
};

/**
 * Unit tests for LogCleaner.
 */
class LogCleanerTest : public ::testing::Test {
  public:
    Context context;
    ServerId serverId;
    ServerList serverList;
    ServerConfig serverConfig;
    ReplicaManager replicaManager;
    SegletAllocator allocator;
    SegmentManager segmentManager;
    TestEntryHandlers entryHandlers;
    LogCleaner cleaner;

    LogCleanerTest()
        : context(),
          serverId(ServerId(57, 0)),
          serverList(&context),
          serverConfig(ServerConfig::forTesting()),
          replicaManager(&context, serverId, 0),
          allocator(serverConfig),
          segmentManager(&context, serverConfig, serverId,
                         allocator, replicaManager),
          entryHandlers(),
          cleaner(&context, serverConfig, segmentManager,
                  replicaManager, entryHandlers)
    {
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(LogCleanerTest);
};

TEST_F(LogCleanerTest, constructor) {
    EXPECT_TRUE(cleaner.disableInMemoryCleaning);
    EXPECT_FALSE(cleaner.threadShouldExit);
    EXPECT_EQ(LogCleaner::SURVIVOR_SEGMENTS_TO_RESERVE,
        segmentManager.freeSurvivorSlots.size());

    // wct 0 => no in-memory cleaning
    SegletAllocator allocator2(serverConfig);
    SegmentManager segmentManager2(&context, serverConfig, serverId,
                                   allocator2, replicaManager);
    serverConfig.master.disableInMemoryCleaning = false;
    serverConfig.master.cleanerWriteCostThreshold = 0;
    LogCleaner cleaner2(&context, serverConfig,
                        segmentManager2, replicaManager, entryHandlers);
    EXPECT_TRUE(cleaner2.disableInMemoryCleaning);

    // ensure in-memory cleaning can be enabled
    SegletAllocator allocator3(serverConfig);
    SegmentManager segmentManager3(&context, serverConfig, serverId,
                                   allocator3, replicaManager);
    serverConfig.master.disableInMemoryCleaning = false;
    serverConfig.master.cleanerWriteCostThreshold = 1;
    LogCleaner cleaner3(&context, serverConfig,
                        segmentManager3, replicaManager, entryHandlers);
    EXPECT_FALSE(cleaner3.disableInMemoryCleaning);
}

TEST_F(LogCleanerTest, start) {
    TestLog::Enable _;
    cleaner.start();
    usleep(1000);
    EXPECT_EQ("cleanerThreadEntry: LogCleaner thread started", TestLog::get());
}

TEST_F(LogCleanerTest, stop) {
    TestLog::Enable _;
    cleaner.stop();
    EXPECT_EQ("", TestLog::get());

    cleaner.start();
    cleaner.stop();
    EXPECT_TRUE(StringUtil::endsWith(TestLog::get(),
        "cleanerThreadEntry: LogCleaner thread stopping"));
}

TEST_F(LogCleanerTest, doWork) {
    // XXXX write me
}

TEST_F(LogCleanerTest, doMemoryCleaning) {
    // XXXX write me
}

TEST_F(LogCleanerTest, doDiskCleaning) {
    // XXXX write me
}

TEST_F(LogCleanerTest, getSegmentToCompact) {
    // XXXX write me
}

TEST_F(LogCleanerTest, sortSegmentsByCostBenefit) {
    // XXXX write me
}

TEST_F(LogCleanerTest, getSegmentsToClean) {
    // XXXX write me
}

TEST_F(LogCleanerTest, sortEntriesByTimestamp) {
    LogCleaner::LiveEntryVector entries;
    entries.push_back({ NULL, 0, 5 });
    entries.push_back({ NULL, 0, 1 });
    entries.push_back({ NULL, 0, 3 });
    entries.push_back({ NULL, 0, 0 });
    cleaner.sortEntriesByTimestamp(entries);
    EXPECT_EQ(0U, entries[0].timestamp);
    EXPECT_EQ(1U, entries[1].timestamp);
    EXPECT_EQ(3U, entries[2].timestamp);
    EXPECT_EQ(5U, entries[3].timestamp);
}

TEST_F(LogCleanerTest, getSortedEntries) {
    LogSegmentVector segments;
    LogSegment* a = segmentManager.allocHead(false);
    a->append(LOG_ENTRY_TYPE_OBJ, "hi", 3);
    a->append(LOG_ENTRY_TYPE_OBJ, "bye", 4);
    LogSegment* b = segmentManager.allocHead(false);
    b->append(LOG_ENTRY_TYPE_OBJ, ":-)", 4);
    segments.push_back(a);
    segments.push_back(b);

    LogCleaner::LiveEntryVector entries;
    cleaner.getSortedEntries(segments, entries);
    string contents;
    int objectCount = 0;
    for (size_t i = 0; i < entries.size(); i++) {
        Buffer buffer;
        LogEntryType type = entries[i].segment->getEntry(entries[i].offset,
                                                         buffer);
        if (type != LOG_ENTRY_TYPE_OBJ)
            continue;

        objectCount++;
        contents += reinterpret_cast<const char*>(buffer.getRange(
            0, buffer.getTotalLength()));
    }
    EXPECT_EQ(3, objectCount);
    EXPECT_EQ("hibye:-)", contents);
}

TEST_F(LogCleanerTest, relocateLiveEntries) {
    // XXXX write me
}

TEST_F(LogCleanerTest, closeSurvivor) {
    LogSegment* a = segmentManager.allocHead(false);

    // Ensure both the Segment and ReplicatedSegment are closed.
    TestLog::Enable _;
    cleaner.closeSurvivor(a);
    EXPECT_TRUE(a->closed);
    EXPECT_TRUE(StringUtil::startsWith(TestLog::get(), "close: 57.0, 0, 0"));
}

TEST_F(LogCleanerTest, waitForAvailableSurvivors) {
    // This method should be going away shortly...
}

TEST_F(LogCleanerTest, CostBenefitComparer_constructor) {
    WallTime::mockWallTimeValue = 445566;
    Cycles::mockTscValue = 112233;

    LogCleaner::CostBenefitComparer c;
    EXPECT_EQ(445566U, c.now);
    EXPECT_EQ(112233U, c.version);

    Cycles::mockTscValue = 0;
    WallTime::mockWallTimeValue = 0;
}

TEST_F(LogCleanerTest, CostBenefitComparer_costBenefit) {
    WallTime::mockWallTimeValue = 44556677;
    LogCleaner::CostBenefitComparer c;
    LogSegment* a = segmentManager.allocHead(false);

    EXPECT_EQ(0, a->getDiskUtilization());
    EXPECT_EQ(-1UL, c.costBenefit(a));

    a->statistics.increment(827, 2836461);
    a->statistics.increment(1726, 826401);
    EXPECT_EQ(1, a->getDiskUtilization());
    EXPECT_EQ(4264836048UL, c.costBenefit(a));

    // test the timestamp warning
    TestLog::Enable _;
    c.now = 0;
    EXPECT_EQ(0U, c.costBenefit(a));
    EXPECT_EQ("costBenefit: timestamp > now", TestLog::get());

    WallTime::mockWallTimeValue = 0;
}

TEST_F(LogCleanerTest, CostBenefitComparer_operatorParen) {
    LogCleaner::CostBenefitComparer c;
    c.now = 100;
    c.version = 15;

    LogSegment* a = segmentManager.allocHead(false);
    LogSegment* b = segmentManager.allocHead(false);

    // Ensure that cached values are used if the version is the same.
    a->costBenefitVersion = c.version;
    b->costBenefitVersion = c.version;
    a->costBenefit = 10;
    b->costBenefit = 9;
    EXPECT_TRUE(c(a, b));
    a->costBenefit = 9;
    EXPECT_FALSE(c(a, b));
    b->costBenefit = 10;
    EXPECT_FALSE(c(a, b));

    // Once the version changes, cached values should, too.
    a->costBenefitVersion++;
    b->costBenefitVersion++;
    c(a, b);
    EXPECT_EQ(a->costBenefitVersion, c.version);
    EXPECT_EQ(b->costBenefitVersion, c.version);
    EXPECT_EQ(-1UL, a->costBenefit);
    EXPECT_EQ(-1UL, b->costBenefit);
}

TEST_F(LogCleanerTest, TimestampComparer) {
    LogCleaner::TimestampComparer c;
    LogCleaner::LiveEntry a(NULL, 0, 50);
    LogCleaner::LiveEntry b(NULL, 0, 51);
    EXPECT_TRUE(c(a, b));
    EXPECT_FALSE(c(a, a));
    EXPECT_FALSE(c(b, a));
}

TEST_F(LogCleanerTest, relocateEntry) {
    LogCleanerMetrics::OnDisk metrics;
    Buffer buffer;
    EXPECT_TRUE(cleaner.relocateEntry(LOG_ENTRY_TYPE_OBJ,
                                      buffer,
                                      NULL,
                                      metrics));
    entryHandlers.attemptToRelocate = true;
    EXPECT_FALSE(cleaner.relocateEntry(LOG_ENTRY_TYPE_OBJ,
                                       buffer,
                                       NULL,
                                       metrics));
}

} // namespace RAMCloud
