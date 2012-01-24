/* Copyright (c) 2010, 2011 Stanford University
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
#include "ServerId.h"
#include "Log.h"
#include "LogTypes.h"
#include "LogCleaner.h"

namespace RAMCloud {

/**
 * Unit tests for LogCleaner.
 */
class LogCleanerTest : public ::testing::Test {
  public:
    LogCleanerTest()
        : serverId(ServerId(5, 23))
    {
    }

    Tub<ServerId> serverId;

  private:
    DISALLOW_COPY_AND_ASSIGN(LogCleanerTest);
};

TEST_F(LogCleanerTest, constructor) {
    Log log(serverId, 8192, 8192, 4298, NULL, Log::CLEANER_DISABLED);
    LogCleaner* cleaner = &log.cleaner;

    EXPECT_EQ(0U, cleaner->bytesFreedBeforeLastCleaning);
    EXPECT_EQ(0U, cleaner->cleanableSegments.size());
    EXPECT_EQ(&log, cleaner->log);
    EXPECT_EQ(log.replicaManager, cleaner->replicaManager);
}

TEST_F(LogCleanerTest, getCleanSegmentMemory) {
    Log log(serverId, 5 * 8192, 8192, 4298, NULL, Log::CLEANER_DISABLED);
    LogCleaner* cleaner = &log.cleaner;

    std::vector<void*> cleanMemory;
    EXPECT_FALSE(cleaner->getCleanSegmentMemory(5, cleanMemory));
    EXPECT_EQ(0U, cleanMemory.size());
    EXPECT_TRUE(cleaner->getCleanSegmentMemory(4, cleanMemory));
    EXPECT_EQ(4U, cleanMemory.size());

    SegmentVector empty;
    log.cleaningComplete(empty, cleanMemory);
}

TEST_F(LogCleanerTest, writeCost) {
    EXPECT_DOUBLE_EQ(1.000, LogCleaner::writeCost(10, 0));
    EXPECT_DOUBLE_EQ(1.1111111111111112, LogCleaner::writeCost(10, 1));
    EXPECT_DOUBLE_EQ(2.000, LogCleaner::writeCost(10, 5));
    EXPECT_DOUBLE_EQ(10.000, LogCleaner::writeCost(10, 9));
}

TEST_F(LogCleanerTest, isCleanable) {
    EXPECT_TRUE(LogCleaner::isCleanable(
        LogCleaner::MAXIMUM_CLEANABLE_WRITE_COST));
    EXPECT_TRUE(LogCleaner::isCleanable(
        LogCleaner::MAXIMUM_CLEANABLE_WRITE_COST - 0.00001));
    EXPECT_FALSE(LogCleaner::isCleanable(
        LogCleaner::MAXIMUM_CLEANABLE_WRITE_COST + 0.00001));
}

TEST_F(LogCleanerTest, scanNewCleanableSegments) {
    Log log(serverId, 8192, 8192, 4298, NULL, Log::CLEANER_DISABLED);
    LogCleaner* cleaner = &log.cleaner;
    char alignedBuf0[8192] __attribute__((aligned(8192)));
    char alignedBuf1[8192] __attribute__((aligned(8192)));
    char alignedBuf3[8192] __attribute__((aligned(8192)));

    Segment s0(1, 0, alignedBuf0, sizeof(alignedBuf0));
    Segment s1(1, 1, alignedBuf1, sizeof(alignedBuf1));
    Segment s3(1, 3, alignedBuf3, sizeof(alignedBuf3));
    cleaner->scanList.push_back(&s0);
    cleaner->scanList.push_back(&s1);
    cleaner->scanList.push_back(&s3);

    cleaner->scanNewCleanableSegments();
    EXPECT_EQ(1U, cleaner->scanList.size());
    EXPECT_EQ(2U, cleaner->cleanableSegments.size());
    EXPECT_EQ(0U, cleaner->cleanableSegments[0].segment->getId());
    EXPECT_EQ(1U, cleaner->cleanableSegments[1].segment->getId());
    EXPECT_EQ(3U, cleaner->scanList[0]->getId());
}

static int scanCbCalled = 0;
static void
scanCB(LogEntryHandle h, void* cookie)
{
    EXPECT_EQ(reinterpret_cast<void*>(1234), cookie);
    EXPECT_EQ(513U, h->length());
    scanCbCalled++;
}

TEST_F(LogCleanerTest, scanSegment) {
    Log log(serverId, 3 * 1024, 1024, 768, NULL, Log::CLEANER_DISABLED);
    LogCleaner* cleaner = &log.cleaner;

    log.registerType(LOG_ENTRY_TYPE_OBJ,
                     true,
                     NULL, NULL,
                     NULL, NULL,
                     NULL,
                     scanCB,
                     reinterpret_cast<void*>(1234));

    while (log.cleanableNewList.size() < 1) {
        char buf[513];
        log.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
    }

    // indirectly invokes scanSegment
    cleaner->scanNewCleanableSegments();

    EXPECT_EQ(1, scanCbCalled);
}

static int scanCb2Called = 0;
static void
scanCB2(LogEntryHandle h, void* cookie)
{
    scanCb2Called++;
}

static int liveCBCalled = 0;
static bool
liveCB(LogEntryHandle h, void* cookie)
{
    liveCBCalled++;
    return true;
}

TEST_F(LogCleanerTest, scanSegment_implicitlyFreed) {
    Log log(serverId, 3 * 1024, 1024, 768, NULL, Log::CLEANER_DISABLED);
    LogCleaner* cleaner = &log.cleaner;

    log.registerType(LOG_ENTRY_TYPE_OBJ,
                     true,
                     liveCB, NULL,
                     NULL, NULL,
                     NULL,
                     scanCB2,
                     reinterpret_cast<void*>(1234));

    log.registerType(LOG_ENTRY_TYPE_OBJTOMB,
                     false,
                     liveCB, NULL,
                     NULL, NULL,
                     NULL,
                     scanCB2,
                     reinterpret_cast<void*>(1234));

    char buf[513];
    log.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
    LogEntryHandle h = log.append(LOG_ENTRY_TYPE_OBJTOMB, buf, 27);

    uint32_t impEntries = 0;
    uint32_t impBytes = 0;
    cleaner->scanSegment(log.head, &impEntries, &impBytes);

    EXPECT_EQ(2, scanCb2Called);
    EXPECT_EQ(1U, impEntries);
    EXPECT_EQ(h->totalLength(), impBytes);
}

static int negativeLiveCBCalled = 0;
static bool
negativeLiveCB(LogEntryHandle h, void* cookie)
{
    negativeLiveCBCalled++;
    return false;
}

TEST_F(LogCleanerTest, scanForFreeSpace) {
    Log log(serverId, 5 * 8192, 8192, 4298, NULL, Log::CLEANER_DISABLED);
    log.registerType(LOG_ENTRY_TYPE_OBJ,
                     true,
                     negativeLiveCB, NULL,
                     NULL, NULL,
                     NULL,
                     NULL,
                     NULL);
    log.registerType(LOG_ENTRY_TYPE_OBJTOMB,
                     false,
                     negativeLiveCB, NULL,
                     NULL, NULL,
                     NULL,
                     NULL,
                     NULL);
    LogCleaner* cleaner = &log.cleaner;

    char sourceBuf[8192];

    // Add a seg that's certainly cleanable and ensure it is
    // not scanned.
    char cleanableBuf[8192] __attribute__((aligned(8192)));
    Segment cleanableSeg(1, 2, cleanableBuf, sizeof(cleanableBuf));
    cleanableSeg.append(LOG_ENTRY_TYPE_OBJ, "hi", 2);
    cleaner->cleanableSegments.push_back({ &cleanableSeg, 0, 0 });
    cleaner->scanForFreeSpace();
    EXPECT_EQ(0, negativeLiveCBCalled);

    // Add a seg that's not cleanable and has no implicitly freeable bytes.
    char notCleanableBuf[8192] __attribute__((aligned(8192)));
    Segment notCleanableSeg(1, 2, notCleanableBuf, sizeof(notCleanableBuf));
    notCleanableSeg.append(LOG_ENTRY_TYPE_OBJ, sourceBuf, 8100);
    cleaner->cleanableSegments.push_back({ &notCleanableSeg, 0, 0 });
    cleaner->scanForFreeSpace();
    EXPECT_EQ(0, negativeLiveCBCalled);

    // Add a seg that's not cleanable now, but could be due to implicit bytes.
    // This one should be scanned.
    char almostCleanableBuf[8192] __attribute__((aligned(8192)));
    Segment almostCleanableSeg(1, 2, almostCleanableBuf,
        sizeof(almostCleanableBuf));
    almostCleanableSeg.append(LOG_ENTRY_TYPE_OBJ, "hi", 2);
    LogEntryHandle h = almostCleanableSeg.append(
        LOG_ENTRY_TYPE_OBJTOMB, sourceBuf, 8100);
    cleaner->cleanableSegments.push_back(
        { &almostCleanableSeg, 1, h->totalLength() });
    cleaner->scanForFreeSpace();
    EXPECT_EQ(1, negativeLiveCBCalled);

    // Clean up.
    for (int i = 0; i < 3; i++)
        cleaner->cleanableSegments.pop_back();
}

static uint32_t
fiveTimestampCB(LogEntryHandle h)
{
    return 5;
}

TEST_F(LogCleanerTest, scanSegmentForFreeSpace) {
    Log log(serverId, 8192 * 1000, 8192, 4298, NULL, Log::CLEANER_DISABLED);
    log.registerType(LOG_ENTRY_TYPE_OBJ,
                     true,
                     liveCB, NULL,
                     NULL, NULL,
                     fiveTimestampCB,
                     NULL,
                     NULL);
    log.registerType(LOG_ENTRY_TYPE_OBJTOMB,
                     false,
                     negativeLiveCB, NULL,
                     NULL, NULL,
                     fiveTimestampCB,
                     NULL,
                     NULL);
    LogCleaner* cleaner = &log.cleaner;

    char segBuf[8192] __attribute__((aligned(8192)));
    Segment s(1, 2, segBuf, sizeof(segBuf));
    *const_cast<Log**>(&s.log) = &log;
    s.append(LOG_ENTRY_TYPE_OBJ, "i'm alive!", 10);
    LogEntryHandle h = s.append(LOG_ENTRY_TYPE_OBJTOMB, "dead!", 5);

    LogCleaner::CleanableSegment cs(&s, 1, h->totalLength());
    cleaner->scanSegmentForFreeSpace(cs);
    EXPECT_EQ(h->totalLength(), cs.implicitlyFreedBytes);
    EXPECT_EQ(1U, cs.implicitlyFreedEntries);
    EXPECT_EQ(h->totalLength(), s.bytesImplicitlyFreed);
    EXPECT_EQ(5 * h->totalLength(), s.implicitlyFreedSpaceTimeSum);
}

TEST_F(LogCleanerTest, getSegmentsToClean) {
    Log log(serverId, 8192 * 1000, 8192, 4298, NULL, Log::CLEANER_DISABLED);
    log.registerType(LOG_ENTRY_TYPE_OBJ, true, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL);
    LogCleaner* cleaner = &log.cleaner;
    char buf[64];

    size_t minFreeBytesForCleaning = cleaner->CLEANED_SEGMENTS_PER_PASS *
        log.getSegmentCapacity();

    SegmentVector segmentsToClean;
    while (segmentsToClean.size() == 0) {
        LogEntryHandle h = log.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        log.free(h);
        cleaner->scanNewCleanableSegments();
        cleaner->getSegmentsToClean(segmentsToClean);
    }

    size_t freeableBytes = 0;
    foreach (Segment* s, segmentsToClean)
        freeableBytes += s->getFreeBytes();
    EXPECT_GE(freeableBytes, minFreeBytesForCleaning);

    // Returned segments remain in the cleanableSegments list in case the
    // caller wants to back out. It's up to them to pop_back() if they do
    // clean.
    for (size_t i = 0; i < segmentsToClean.size(); i++) {
        EXPECT_EQ(segmentsToClean[i],
                  cleaner->cleanableSegments.back().segment);
        cleaner->cleanableSegments.pop_back();
    }
}

static bool
getSegmentsToCleanFilter(string s)
{
    return s == "getSegmentsToClean";
}

// sanity check the write cost calculation
TEST_F(LogCleanerTest, getSegmentsToClean_writeCost) {
    struct TestSetup {
        int freeEveryNth;       // after N appends, free one (0 => no frees)
        double minWriteCost;    // min acceptable writeCost (0 => no min)
        double maxWriteCost;    // max acceptable writeCost (0 => no max)
    } testSetup[] = {
        // - ~empty segments:     write cost should be very close to 1
        { 1, 1.00, 1.05 },

        // - ~50% full segments:  write cost should be very close to 2
        { 2, 2.00, 2.05 },

        // - ~80% full segments: write cost should be very close to 5
        { 5, 5.00, 5.10 },

        // terminator
        { -1, 0, 0 },
    };

    for (int i = 0; testSetup[i].freeEveryNth != -1; i++) {
        // Depending on our maximum permitted write cost, we may or may not be
        // able to run all test cases.
        if (testSetup[i].minWriteCost >
          LogCleaner::MAXIMUM_CLEANABLE_WRITE_COST) {
            continue;
        }

        Log log(serverId, 8192 * 1000, 8192, 4298, NULL, Log::CLEANER_DISABLED);
        log.registerType(LOG_ENTRY_TYPE_OBJ, true, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL);
        LogCleaner* cleaner = &log.cleaner;
        int freeEveryNth = testSetup[i].freeEveryNth;

        TestLog::Enable _(&getSegmentsToCleanFilter);
        LogCleaner::PerfCounters before = cleaner->perfCounters;

        SegmentVector segmentsToClean;
        int j = 0;
        while (segmentsToClean.size() == 0) {
            char buf[64];
            LogEntryHandle h = log.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
            if (freeEveryNth && (j++ % freeEveryNth) == 0)
                log.free(h);
            cleaner->scanNewCleanableSegments();
            cleaner->getSegmentsToClean(segmentsToClean);
        }

        double writeCost;
        sscanf(TestLog::get().c_str(),                  // NOLINT sscanf ok here
            "getSegmentsToClean: writeCost %lf", &writeCost);

        if (testSetup[i].minWriteCost)
            EXPECT_GE(writeCost, testSetup[i].minWriteCost);
        if (testSetup[i].maxWriteCost)
            EXPECT_LE(writeCost, testSetup[i].maxWriteCost);
    }
}

TEST_F(LogCleanerTest, getSegmentsToClean_costBenefitOrder) {
    Log log(serverId, 8192 * 1000, 8192, 4298, NULL, Log::CLEANER_DISABLED);
    log.registerType(LOG_ENTRY_TYPE_OBJ, true, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL);
    LogCleaner* cleaner = &log.cleaner;
    char buf[64];

    // Ages should be so close (if not all equal) that they're irrelevant.
    // Utilisation should be the only important attribute.

    // Make a very cleanable segment (~100% free space).
    while (cleaner->cleanableSegments.size() == 0) {
        LogEntryHandle h = log.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        log.free(h);
        cleaner->scanNewCleanableSegments();
    }

    // Make a bunch more that are ~90% free space.
    SegmentVector segmentsToClean;
    for (size_t i = 0; segmentsToClean.size() == 0; i++) {
        LogEntryHandle h = log.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        if ((i % 10) != 0)
            log.free(h);
        cleaner->scanNewCleanableSegments();
        cleaner->getSegmentsToClean(segmentsToClean);
    }

    int lastUtilisation = 0;
    for (size_t i = 0; i < segmentsToClean.size(); i++) {
        Segment*s = segmentsToClean[i];
        if (i == 0)
            EXPECT_LE(s->getUtilisation(), 1);
        else
            EXPECT_GE(s->getUtilisation(), 9);
        EXPECT_LE(lastUtilisation, s->getUtilisation());
        lastUtilisation = s->getUtilisation();
    }
}

static bool
livenessCB(LogEntryHandle h, void* cookie)
{
    return true;
}

static bool
relocationCB(LogEntryHandle oldH, LogEntryHandle newH, void* cookie)
{
    assert(0);
    return true;
}

static LogEntryHandle wantNewer = NULL;
static uint32_t
timestampCB(LogEntryHandle h)
{
    if (h == wantNewer)
        return 1000;
    return 1;
}

static void
scanCB3(LogEntryHandle h, void* cookie)
{
}

TEST_F(LogCleanerTest, getLiveEntries) {
    Log log(serverId, 8192 * 20, 8192, 8000, NULL, Log::CLEANER_DISABLED);
    LogCleaner* cleaner = &log.cleaner;

    log.registerType(LOG_ENTRY_TYPE_OBJ,
                     true,
                     livenessCB, NULL,
                     relocationCB, NULL,
                     timestampCB,
                     scanCB3, NULL);

    log.registerType(LOG_ENTRY_TYPE_OBJTOMB,
                     true,
                     negativeLiveCB, NULL,
                     relocationCB, NULL,
                     timestampCB,
                     scanCB3, NULL);

    LogCleaner::LiveSegmentEntryHandleVector entries;
    char buf[8192] __attribute__((aligned(8192)));
    Segment s(1, 2, buf, sizeof(buf));
    *const_cast<Log**>(&s.log) = &log;
    LogEntryHandle h1 = s.append(LOG_ENTRY_TYPE_OBJ, "live", 4);
    s.append(LOG_ENTRY_TYPE_OBJTOMB, "dead", 4);

    wantNewer = h1;
    size_t liveBytes = cleaner->getLiveEntries(&s, entries);
    EXPECT_EQ(h1->length(), liveBytes);
    EXPECT_EQ(1U, entries.size());
    EXPECT_EQ(h1, entries[0].handle);
    EXPECT_EQ(1000U, entries[0].timestamp);
    EXPECT_EQ(h1->totalLength(), entries[0].totalLength);
}

TEST_F(LogCleanerTest, getSortedLiveEntries) {
    Log log(serverId, 8192 * 20, 8192, 8000, NULL, Log::CLEANER_DISABLED);
    LogCleaner* cleaner = &log.cleaner;

    log.registerType(LOG_ENTRY_TYPE_OBJ,
                     true,
                     livenessCB, NULL,
                     relocationCB, NULL,
                     timestampCB,
                     scanCB3, NULL);

    SegmentVector segments;
    LogCleaner::LiveSegmentEntryHandleVector liveEntries;

    char buf[8192];
    LogEntryHandle older = log.append(LOG_ENTRY_TYPE_OBJ, buf, 1024);
    segments.push_back(log.head);
    LogEntryHandle newer = log.append(LOG_ENTRY_TYPE_OBJ, buf, 1024);
    wantNewer = newer;
    log.append(LOG_ENTRY_TYPE_OBJ, buf, 8192 - 2048);   // force old head closed

    size_t liveBytes = cleaner->getSortedLiveEntries(segments, liveEntries);

    EXPECT_EQ(2U, liveEntries.size());
    EXPECT_EQ(older, liveEntries[0].handle);
    EXPECT_EQ(newer, liveEntries[1].handle);
    EXPECT_EQ(older->length() + newer->length(), liveBytes);
}

static bool
relocationCBTrue(LogEntryHandle oldH, LogEntryHandle newH, void* cookie)
{
    return true;
}

static bool
relocationCBFalse(LogEntryHandle oldH, LogEntryHandle newH, void* cookie)
{
    return false;
}

TEST_F(LogCleanerTest, moveToFillSegment) {
    Log log(serverId, 8192 * 20, 8192, 8000, NULL, Log::CLEANER_DISABLED);
    LogCleaner* cleaner = &log.cleaner;

    // live
    log.registerType(LOG_ENTRY_TYPE_OBJ,
                     true,
                     livenessCB, NULL,
                     relocationCBTrue, NULL,
                     timestampCB,
                     scanCB3, NULL);

    // dead
    log.registerType(LOG_ENTRY_TYPE_OBJTOMB,
                     true,
                     negativeLiveCB, NULL,
                     relocationCBFalse, NULL,
                     timestampCB,
                     scanCB3, NULL);

    // livenessCB says live, but dies before relocationCB
    log.registerType(LOG_ENTRY_TYPE_LOGDIGEST,
                     true,
                     livenessCB, NULL,
                     relocationCBFalse, NULL,
                     timestampCB,
                     scanCB3, NULL);

    char sourceBuf[8192];
    char buf1[8192] __attribute__((aligned(8192)));
    char buf2[8192] __attribute__((aligned(8192)));
    char buf3[8192] __attribute__((aligned(8192)));
    Segment segToAppendTo(1, 1, buf1, sizeof(buf1));
    Segment sourceSegFits(1, 2, buf2, sizeof(buf2));
    Segment sourceSegDoesNotFit(1, 3, buf3, sizeof(buf3));

    LogEntryHandle h1 =
        segToAppendTo.append(LOG_ENTRY_TYPE_OBJ, sourceBuf, 5000);
    LogEntryHandle h2 =
        sourceSegFits.append(LOG_ENTRY_TYPE_OBJ, "live data fits", 14);
    sourceSegFits.append(LOG_ENTRY_TYPE_LOGDIGEST, "live, then dead", 15);
    sourceSegFits.append(LOG_ENTRY_TYPE_OBJTOMB, "dead data", 8);
    sourceSegDoesNotFit.append(LOG_ENTRY_TYPE_OBJ, sourceBuf, 5000);

    SegmentVector segsToClean;
    cleaner->cleanableSegments.push_back({ &sourceSegDoesNotFit, 0, 0 });
    uint32_t liveBefore = segToAppendTo.getLiveBytes();
    cleaner->moveToFillSegment(&segToAppendTo, segsToClean);
    EXPECT_EQ(liveBefore, segToAppendTo.getLiveBytes());
    EXPECT_EQ(0U, segsToClean.size());

    cleaner->cleanableSegments.push_back({ &sourceSegFits, 0, 0 });
    EXPECT_EQ(2U, cleaner->cleanableSegments.size());
    cleaner->moveToFillSegment(&segToAppendTo, segsToClean);
    EXPECT_EQ(1U, segsToClean.size());
    EXPECT_EQ(&sourceSegFits, segsToClean[0]);
    EXPECT_EQ(1U, cleaner->cleanableSegments.size());
    EXPECT_EQ(&sourceSegDoesNotFit, cleaner->cleanableSegments[0].segment);

    int objs = 0;
    int tombs = 0;
    for (SegmentIterator it(&segToAppendTo); !it.isDone(); it.next()) {
        if (it.getType() == LOG_ENTRY_TYPE_OBJ) {
            if (objs == 0)
                EXPECT_EQ(h1, it.getHandle());
            if (objs == 1) {
                EXPECT_EQ(h2->length(), it.getHandle()->length());
                EXPECT_EQ(0, memcmp(h2->userData(),
                                    it.getHandle()->userData(),
                                    it.getHandle()->length()));
            }
            objs++;
        }
        if (it.getType() == LOG_ENTRY_TYPE_OBJTOMB)
            tombs++;
    }
    EXPECT_EQ(2, objs);
    EXPECT_EQ(0, tombs);
}

// Ensure we fill objects into older destination Segments in order
// to get better utilisation.
TEST_F(LogCleanerTest, moveLiveData_packOldFirst) {
    // TODO(Rumble)
}

// Ensure that if we're left with significant space in the last Segment
// we try to fill it with the live data from some other Segment.
TEST_F(LogCleanerTest, moveLiveData_packLastSegment) {
    // TODO(Rumble)
}

} // namespace RAMCloud
