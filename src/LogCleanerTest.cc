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
#include "Log.h"
#include "LogTypes.h"
#include "LogCleaner.h"

namespace RAMCloud {

TEST(LogCleanerTest, constructor) {
    Tub<uint64_t> serverId(0);
    Log log(serverId, 8192, 8192, 4298, NULL, Log::CLEANER_DISABLED);
    LogCleaner* cleaner = &log.cleaner;

    EXPECT_EQ(0U, cleaner->bytesFreedBeforeLastCleaning);
    EXPECT_EQ(0U, cleaner->cleanableSegments.size());
    EXPECT_EQ(&log, cleaner->log);
    EXPECT_EQ(log.backup, cleaner->backup);
}

TEST(LogCleanerTest, scanNewCleanableSegments) {
    Tub<uint64_t> serverId(0);
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

TEST(LogCleanerTest, scanSegment) {
    Tub<uint64_t> serverId(0);
    Log log(serverId, 1024 * 2, 1024, 768, NULL, Log::CLEANER_DISABLED);
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

TEST(LogCleanerTest, getSegmentsToClean) {
    Tub<uint64_t> serverId(0);
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

    for (size_t i = 0; i < segmentsToClean.size(); i++) {
        EXPECT_TRUE(segmentsToClean[i] != NULL);

        // returned segments must no longer be in the cleanableSegments list,
        // since we're presumed to clean them now
        foreach (LogCleaner::CleanableSegment& cs, cleaner->cleanableSegments)
            EXPECT_FALSE(cs.segment == segmentsToClean[i]);
    }
}

// sanity check the write cost calculation
TEST(LogCleanerTest, getSegmentsToClean_writeCost) {
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

        Tub<uint64_t> serverId(0);
        Log log(serverId, 8192 * 1000, 8192, 4298, NULL, Log::CLEANER_DISABLED);
        log.registerType(LOG_ENTRY_TYPE_OBJ, true, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL);
        LogCleaner* cleaner = &log.cleaner;
        int freeEveryNth = testSetup[i].freeEveryNth;

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

        double writeCost = (cleaner->perfCounters - before).writeCostSum;
        if (testSetup[i].minWriteCost)
            EXPECT_GE(writeCost, testSetup[i].minWriteCost);
        if (testSetup[i].maxWriteCost)
            EXPECT_LE(writeCost, testSetup[i].maxWriteCost);
    }
}

TEST(LogCleanerTest, getSegmentsToClean_costBenefitOrder) {
    Tub<uint64_t> serverId(0);
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
scanCB2(LogEntryHandle h, void* cookie)
{
}

TEST(LogCleanerTest, getSortedLiveEntries) {
    Tub<uint64_t> serverId(0);
    Log log(serverId, 8192 * 20, 8192, 8000, NULL, Log::CLEANER_DISABLED);
    LogCleaner* cleaner = &log.cleaner;

    log.registerType(LOG_ENTRY_TYPE_OBJ,
                     true,
                     livenessCB, NULL,
                     relocationCB, NULL,
                     timestampCB,
                     scanCB2, NULL);

    SegmentVector segments;
    LogCleaner::LiveSegmentEntryHandleVector liveEntries;

    char buf[8192];
    LogEntryHandle older = log.append(LOG_ENTRY_TYPE_OBJ, buf, 1024);
    segments.push_back(log.head);
    LogEntryHandle newer = log.append(LOG_ENTRY_TYPE_OBJ, buf, 1024);
    wantNewer = newer;
    log.append(LOG_ENTRY_TYPE_OBJ, buf, 8192 - 2048);   // force old head closed

    cleaner->getSortedLiveEntries(segments, liveEntries);

    EXPECT_EQ(2U, liveEntries.size());
    EXPECT_EQ(older, liveEntries[0].handle);
    EXPECT_EQ(newer, liveEntries[1].handle);
}

// Ensure we fill objects into older destination Segments in order
// to get better utilisation.
TEST(LogCleanerTest, moveLiveData_packOldFirst) {
    // XXX
}

// Ensure that if we're left with significant space in the last Segment
// we try to fill it with the live data from some other Segment.
TEST(LogCleanerTest, moveLiveData_packLastSegment) {
    // XXX
}

} // namespace RAMCloud
