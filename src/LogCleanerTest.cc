/* Copyright (c) 2010-2015 Stanford University
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
#include "MasterTableMetadata.h"
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
             Log::Reference oldReference,
             LogEntryRelocator& relocator)
    {
        RAMCLOUD_TEST_LOG("type %d, size %u",
            downCast<int>(type), oldBuffer.size());
        if (attemptToRelocate)
            relocator.append(type, oldBuffer);
    }

    uint32_t timestamp;
    bool attemptToRelocate;
};

class CleanerServerConfig {
  public:
    CleanerServerConfig()
        : serverConfig(ServerConfig::forTesting())
    {
        serverConfig.segmentSize = 8 * 1024 * 1024;
        serverConfig.segletSize = 64 * 1024;
        serverConfig.master.logBytes = serverConfig.segmentSize * 50;
        serverConfig.master.numReplicas = 3;
    }

    ServerConfig*
    operator()()
    {
        return &serverConfig;
    }

    ServerConfig serverConfig;
};

/**
 * Unit tests for LogCleaner.
 */
class LogCleanerTest : public ::testing::Test {
  public:
    Context context;
    ServerId serverId;
    ServerList serverList;
    MasterTableMetadata masterTableMetadata;
    CleanerServerConfig serverConfig;
    ReplicaManager replicaManager;
    SegletAllocator allocator;
    SegmentManager segmentManager;
    TestEntryHandlers entryHandlers;
    LogCleaner cleaner;
    LogCleaner::CleanerThreadState threadState;

    LogCleanerTest()
        : context(),
          serverId(ServerId(57, 0)),
          serverList(&context),
          masterTableMetadata(),
          serverConfig(),
          replicaManager(&context, &serverId, 0, false, false),
          allocator(serverConfig()),
          segmentManager(&context, serverConfig(), &serverId,
                         allocator, replicaManager, &masterTableMetadata),
          entryHandlers(),
          cleaner(&context, serverConfig(), segmentManager,
                  replicaManager, entryHandlers),
          threadState()
    {
    }

    void clearLiveBytes(LogSegment* segment) {
        for (size_t i = 0; i < TOTAL_LOG_ENTRY_TYPES; i++) {
            segment->deadEntryLengths[i] = segment->entryLengths[i].load();
        }
    }

    void getNewCandidates() {
        LogSegmentVector newCandidates;
        segmentManager.cleanableSegments(newCandidates);
        CleanableSegmentManager::Lock guard(cleaner.cleanableSegments.lock);
        foreach (LogSegment* segment, newCandidates) {
            segment->cachedCleaningCostBenefitScore =
                    cleaner.cleanableSegments.computeCleaningCostBenefitScore(
                    segment);
            segment->cachedCompactionCostBenefitScore =
                    cleaner.cleanableSegments.computeCompactionCostBenefitScore(
                    segment);
            segment->cachedTombstoneScanScore =
                    cleaner.cleanableSegments.computeTombstoneScanScore(
                    segment);
            cleaner.cleanableSegments.insertInAll(segment, guard);
        }
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(LogCleanerTest);
};

TEST_F(LogCleanerTest, constructor) {
    EXPECT_TRUE(cleaner.disableInMemoryCleaning);
    EXPECT_FALSE(cleaner.threadsShouldExit);
    EXPECT_EQ(LogCleaner::SURVIVOR_SEGMENTS_TO_RESERVE,
        segmentManager.freeSurvivorSlots.size());

    // wct 0 => no in-memory cleaning
    SegletAllocator allocator2(serverConfig());
    SegmentManager segmentManager2(&context, serverConfig(), &serverId,
                                   allocator2, replicaManager,
                                   &masterTableMetadata);
    serverConfig()->master.disableInMemoryCleaning = false;
    serverConfig()->master.cleanerWriteCostThreshold = 0;
    LogCleaner cleaner2(&context, serverConfig(),
                        segmentManager2, replicaManager, entryHandlers);
    EXPECT_TRUE(cleaner2.disableInMemoryCleaning);

    // ensure in-memory cleaning can be enabled
    SegletAllocator allocator3(serverConfig());
    SegmentManager segmentManager3(&context, serverConfig(), &serverId,
                                   allocator3, replicaManager,
                                   &masterTableMetadata);
    serverConfig()->master.disableInMemoryCleaning = false;
    serverConfig()->master.cleanerWriteCostThreshold = 1;
    LogCleaner cleaner3(&context, serverConfig(),
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
    TestLog::Enable _;
    cleaner.disableInMemoryCleaning = false;

    // No work to do.
    SegmentManager::mockMemoryUtilization = 1;
    CleanableSegmentManager::mockLiveObjectUtilization = 1;
    cleaner.doWork(&threadState);
    EXPECT_EQ("", TestLog::get());
    TestLog::reset();

    // Low on disk segments, but lots of free memory.
    SegmentManager::mockMemoryUtilization = 1;
    SegmentManager::mockSegmentUtilization = 99;
    CleanableSegmentManager::mockLiveObjectUtilization = 99;
    cleaner.doWork(&threadState);
    EXPECT_EQ(
        "doDiskCleaning: called | "
        "getSegmentsToClean: 0 segments selected with 0 allocated segments",
        TestLog::get());
    TestLog::reset();

    // Lots of free disk segments, but low on memory.
    SegmentManager::mockMemoryUtilization = 99;
    SegmentManager::mockSegmentUtilization = 1;
    CleanableSegmentManager::mockLiveObjectUtilization = 1;
    cleaner.doWork(&threadState);
    EXPECT_EQ(
        "doMemoryCleaning: called",
        TestLog::get());

    SegmentManager::mockMemoryUtilization = 0;
    SegmentManager::mockSegmentUtilization = 0;
    CleanableSegmentManager::mockLiveObjectUtilization = 0;
}

TEST_F(LogCleanerTest, doWork_disabled) {
    TestLog::Enable _;
    cleaner.disableInMemoryCleaning = false;

    // Set up for running the disk cleaner.
    SegmentManager::mockMemoryUtilization = 1;
    SegmentManager::mockSegmentUtilization = 99;
    CleanableSegmentManager::mockLiveObjectUtilization = 99;
    {
        LogCleaner::Disabler disabler(&cleaner);
        cleaner.doWork(&threadState);
    }
    EXPECT_EQ("", TestLog::get());
    cleaner.doWork(&threadState);
    EXPECT_EQ(
        "doDiskCleaning: called | "
        "getSegmentsToClean: 0 segments selected with 0 allocated segments",
        TestLog::get());

    SegmentManager::mockMemoryUtilization = 0;
    SegmentManager::mockSegmentUtilization = 0;
    CleanableSegmentManager::mockLiveObjectUtilization = 0;
}

// Helper function that runs in a separate thread for the following test.
static void disableThread(LogCleaner* cleaner) {
    LogCleaner::Disabler disabler(cleaner);
    TEST_LOG("cleaner idle");
}

TEST_F(LogCleanerTest, doWork_notifyConditionVariable) {
    TestLog::Enable _;
    cleaner.disableInMemoryCleaning = false;

    // First try: bump activeThreads so cleaner looks busy.
    cleaner.activeThreads = 1;
    std::thread thread(disableThread, &cleaner);
    while (cleaner.disableCount == 0) {
        // Wait for the thread to acquire the lock and sleep.
    }
    cleaner.doWork(&threadState);
    Cycles::sleep(1000);
    EXPECT_EQ("", TestLog::get());

    // Second try: cleaner is really idle.
    cleaner.activeThreads = 0;
    cleaner.doWork(&threadState);
    for (int i = 0; i < 1000; i++) {
        Cycles::sleep(1000);
        if (TestLog::get().size() > 0) {
            break;
        }
    }
    EXPECT_EQ("disableThread: cleaner idle", TestLog::get());
    thread.join();
}

// There are currently no meaningful tests for doMemoryCleaning;
// please write some!

TEST_F(LogCleanerTest, doDiskCleaning) {
    // Not entirely sure what to check here. doDiskCleaning() mostly just
    // invokes a standard sequence of meatier methods and updates metrics.
    // Right now we'll just ensure that it calls all of the expected functions.

    clearLiveBytes(segmentManager.allocHeadSegment());
    segmentManager.allocHeadSegment(); // roll over
    getNewCandidates();

    TestLog::Enable _;
    cleaner.doDiskCleaning();
    EXPECT_EQ(
        "doDiskCleaning: called | "
        "getSegmentsToClean: 1 segments selected with 128 allocated segments | "
        "getSortedEntries: 4 entries extracted from 1 segments | "
        "getEntry: Contiguous entry | "
        "relocate: type 1, size 24 | "
        "getEntry: Contiguous entry | "
        "relocate: type 4, size 12 | "
        "getEntry: Contiguous entry | "
        "relocate: type 6, size 24 | "
        "getEntry: Contiguous entry | "
        "relocate: type 5, size 12 | "
        "relocateLiveEntries: Cleaner finished syncing survivor "
        "segments: 0.0 ms, 0.0 MB/sec | "
        "doDiskCleaning: used 0 seglets and 0 segments | "
        "cleaningComplete: Cleaning used 0 seglets to free 128 seglets",
        TestLog::get());
}

// The tests below were disabled a long time ago by Steve Rumble and
// never got reworked to reflect his changes, so they are currently
// broken.
#if 0
TEST_F(LogCleanerTest, getSegmentToCompact) {
    uint32_t freeableSeglets;

    EXPECT_EQ(0U, cleaner.candidates.size());
    EXPECT_EQ(static_cast<LogSegment*>(NULL),
              cleaner.getSegmentToCompact(freeableSeglets));

    LogSegment* best = segmentManager.allocHeadSegment();
    LogSegment* middle = segmentManager.allocHeadSegment();
    LogSegment* worst = segmentManager.allocHeadSegment();

    best->statistics.liveBytes = cleaner.segmentSize / 8;
    middle->statistics.liveBytes = cleaner.segmentSize / 4;
    worst->statistics.liveBytes = cleaner.segmentSize / 2;

    cleaner.candidates.push_back(middle);
    cleaner.candidates.push_back(best);
    cleaner.candidates.push_back(worst);

    EXPECT_EQ(best, cleaner.getSegmentToCompact(freeableSeglets));
    EXPECT_EQ(111U, freeableSeglets);
}

TEST_F(LogCleanerTest, getSegmentToCompact_freeSegletInvariant) {
    uint32_t freeableSeglets;

    LogSegment* s = segmentManager.allocHeadSegment();
    cleaner.candidates.push_back(s);
    s->statistics.liveBytes = cleaner.segmentSize * 98 / 100;
    EXPECT_EQ(static_cast<LogSegment*>(NULL),
              cleaner.getSegmentToCompact(freeableSeglets));

    s->statistics.liveBytes = cleaner.segmentSize * 97 / 100;
    EXPECT_NE(static_cast<LogSegment*>(NULL),
              cleaner.getSegmentToCompact(freeableSeglets));
    EXPECT_EQ(1U, freeableSeglets);
}

TEST_F(LogCleanerTest, sortSegmentsByCostBenefit) {
    WallTime::mockWallTimeValue = 10000;

    LogSegment* best = segmentManager.allocHeadSegment();
    LogSegment* middle = segmentManager.allocHeadSegment();
    LogSegment* worst = segmentManager.allocHeadSegment();

    best->statistics.liveBytes = cleaner.segmentSize / 2;
    best->statistics.spaceTimeSum = 1 * best->statistics.liveBytes;

    middle->statistics.liveBytes = cleaner.segmentSize / 2;
    middle->statistics.spaceTimeSum = 2000 * middle->statistics.liveBytes;

    worst->statistics.liveBytes = (cleaner.segmentSize * 3) / 4;
    worst->statistics.spaceTimeSum = 1000 * worst->statistics.liveBytes;

    LogSegmentVector segments;
    segments.push_back(middle);
    segments.push_back(worst);
    segments.push_back(best);
    cleaner.sortSegmentsByCostBenefit(segments);
    EXPECT_EQ(best, segments[0]);
    EXPECT_EQ(middle, segments[1]);
    EXPECT_EQ(worst, segments[2]);

    WallTime::mockWallTimeValue = 0;
}

TEST_F(LogCleanerTest, getSegmentsToClean) {
    // add segment with util > MAX_CLEANABLE_MEMORY_UTILIZATION
    LogSegment* s = segmentManager.allocHeadSegment();
    s->statistics.liveBytes = s->segmentSize;

    // add a few with lower utilizations
    LogSegment *small = segmentManager.allocHeadSegment();
    LogSegment* medium = segmentManager.allocHeadSegment();
    LogSegment* large = segmentManager.allocHeadSegment();
    small->statistics.liveBytes = s->segmentSize / 8;
    medium->statistics.liveBytes = s->segmentSize / 4;
    large->statistics.liveBytes = s->segmentSize / 2;

    // roll over head
    segmentManager.allocHeadSegment();

    // learn about the new candidates
    segmentManager.cleanableSegments(cleaner.candidates);
    EXPECT_EQ(4U, cleaner.candidates.size());

    LogSegmentVector segments;
    cleaner.getSegmentsToClean(segments);
    uint32_t totalSeglets = 0;
    foreach (LogSegment* s, segments)
        totalSeglets += s->getSegletsAllocated();

    uint32_t segletsPerSegment = cleaner.segmentSize / cleaner.segletSize;

    EXPECT_EQ(3U * segletsPerSegment, totalSeglets);
    EXPECT_EQ(small, segments[0]);
    EXPECT_EQ(medium, segments[1]);
    EXPECT_EQ(large, segments[2]);
    EXPECT_EQ(1U, cleaner.candidates.size());
    EXPECT_EQ(s, cleaner.candidates[0]);
}

TEST_F(LogCleanerTest, getSegmentsToClean_maxBytes) {
    // add a bunch of segments, ensuring we are returned no more than the
    // maximum possible amount of live data
    for (int i = 0; i < LogCleaner::MAX_LIVE_SEGMENTS_PER_DISK_PASS * 2; i++) {
        LogSegment* s = segmentManager.allocHeadSegment();
        EXPECT_TRUE(s != NULL);
        s->statistics.liveBytes = (s->segmentSize * 2) / 3;
    }

    // roll over head
    segmentManager.allocHeadSegment();

    LogSegmentVector segments;
    cleaner.getSegmentsToClean(segments);

    uint32_t totalBytes = 0;
    foreach (LogSegment *s, segments)
        totalBytes += s->statistics.liveBytes;
    EXPECT_LE(totalBytes, LogCleaner::MAX_LIVE_SEGMENTS_PER_DISK_PASS *
                          cleaner.segmentSize);
}

TEST_F(LogCleanerTest, sortEntriesByTimestamp) {
    LogCleaner::EntryVector entries;
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
    LogSegment* a = segmentManager.allocHeadSegment();
    a->append(LOG_ENTRY_TYPE_OBJ, "hi", 3);
    a->append(LOG_ENTRY_TYPE_OBJ, "bye", 4);
    LogSegment* b = segmentManager.allocHeadSegment();
    b->append(LOG_ENTRY_TYPE_OBJ, ":-)", 4);
    segments.push_back(a);
    segments.push_back(b);

    LogCleaner::EntryVector entries;
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
            0, buffer.size()));
    }
    EXPECT_EQ(3, objectCount);
    EXPECT_EQ("hibye:-)", contents);
}

TEST_F(LogCleanerTest, relocateLiveEntries) {
    entryHandlers.attemptToRelocate = true;
    LogSegment* s = segmentManager.allocHeadSegment();

    LogSegmentVector segments;
    segments.push_back(s);
    LogCleaner::EntryVector entries;
    cleaner.getSortedEntries(segments, entries);

    TestLog::Enable _;
    LogSegmentVector survivors;
    uint64_t bytes = cleaner.relocateLiveEntries(entries, survivors);
    EXPECT_EQ(
        "relocate: type 1, size 24 | "
        "alloc: purpose: 3 | "
        "allocateSegment: Allocating new replicated segment for <57.0,2> | "
        "schedule: zero replicas: nothing to schedule | "
        "allocSideSegment: id = 2 | "
        "relocate: type 1, size 24 | "
        "relocate: type 4, size 12 | "
        "relocate: type 5, size 12 | "
        "close: 57.0, 2, 0 | "
        "schedule: zero replicas: nothing to schedule | "
        "close: Segment 2 closed (length 80) | "
        "sync: syncing segment 2 to offset 80",
            TestLog::get());
    EXPECT_EQ(24U + 12 + 12 + 3 * 2, bytes);
}

TEST_F(LogCleanerTest, closeSurvivor) {
    LogSegment* a = segmentManager.allocHeadSegment();

    // Ensure both the Segment and ReplicatedSegment are closed and unused
    // seglets are freed.
    TestLog::Enable _;
    EXPECT_EQ(128U, a->getSegletsAllocated());
    cleaner.closeSurvivor(a);
    EXPECT_TRUE(a->closed);
    EXPECT_TRUE(StringUtil::startsWith(TestLog::get(), "close: 57.0, 1, 0"));
    EXPECT_EQ(1U, a->getSegletsAllocated());
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
    LogSegment* a = segmentManager.allocHeadSegment();

    EXPECT_EQ(0, a->getDiskUtilization());
    EXPECT_EQ(-1UL, c.costBenefit(a));

    a->statistics.increment(88227, 88227UL * 2836461);
    a->statistics.increment(1726, 1726UL * 826401);
    EXPECT_EQ(1, a->getDiskUtilization());
    EXPECT_EQ(4134119715UL, c.costBenefit(a));

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

    LogSegment* a = segmentManager.allocHeadSegment();
    LogSegment* b = segmentManager.allocHeadSegment();

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
    LogCleaner::Entry a(NULL, 0, 50);
    LogCleaner::Entry b(NULL, 0, 51);
    EXPECT_TRUE(c(a, b));
    EXPECT_FALSE(c(a, a));
    EXPECT_FALSE(c(b, a));
}

TEST_F(LogCleanerTest, relocateEntry) {
    LogCleanerMetrics::OnDisk metrics;
    Buffer buffer;
    EXPECT_EQ(RELOCATED, cleaner.relocateEntry(LOG_ENTRY_TYPE_OBJ,
                                               buffer,
                                               NULL,
                                               metrics));
    entryHandlers.attemptToRelocate = true;
    EXPECT_EQ(DISCARDED, cleaner.relocateEntry(LOG_ENTRY_TYPE_OBJ,
                                               buffer,
                                               NULL,
                                               metrics));
}
#endif

TEST_F(LogCleanerTest, Disabler_basics) {
    TestLog::Enable _;
    Tub<LogCleaner::Disabler> disabler1, disabler2;
    disabler1.construct(&cleaner);
    EXPECT_EQ(1, cleaner.disableCount);
    disabler2.construct(&cleaner);
    EXPECT_EQ(2, cleaner.disableCount);
    disabler1.destroy();
    EXPECT_EQ(1, cleaner.disableCount);
    disabler2.destroy();
    EXPECT_EQ(0, cleaner.disableCount);
}

// The Disabler sleep/wakeup mechanism was already tested previously.

#if 0
// Helper function that runs in a separate thread for the following test.
static void fooThread(LogCleaner* cleaner) {
    LogCleaner::Disabler disabler(cleaner);
    TEST_LOG("cleaner idle");
}

TEST_F(LogCleanerTest, doWork_notifyConditionVariable) {
    TestLog::Enable _;
    cleaner.disableInMemoryCleaning = false;

    // First try: bump activeThreads so cleaner looks busy.
    cleaner.activeThreads = 1;
    std::thread thread(disableThread, &cleaner);
    while (cleaner.disableCount == 0) {
        // Wait for the thread to acquire the lock and sleep.
    }
    cleaner.doWork(&threadState);
    Cycles::sleep(1000);
    EXPECT_EQ("", TestLog::get());

    // Second try: cleaner is really idle.
    cleaner.activeThreads = 0;
    cleaner.doWork(&threadState);
    for (int i = 0; i < 1000; i++) {
        Cycles::sleep(1000);
        if (TestLog::get().size() > 0) {
            break;
        }
    }
    EXPECT_EQ("disableThread: cleaner idle", TestLog::get());
    thread.join();
}
#endif
} // namespace RAMCloud
