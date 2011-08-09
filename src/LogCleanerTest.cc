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
    Log log(serverId, 8192, 8192, NULL, Log::INLINED_CLEANER);
    LogCleaner* cleaner = &log.cleaner;

    EXPECT_EQ(0U, cleaner->bytesFreedBeforeLastCleaning);
    EXPECT_EQ(0U, cleaner->cleanableSegments.size());
    EXPECT_EQ(&log, cleaner->log);
    EXPECT_EQ(log.backup, cleaner->backup);
}

TEST(LogCleanerTest, getSegmentsToClean) {
    Tub<uint64_t> serverId(0);
    Log log(serverId, 8192 * 1000, 8192, NULL, Log::INLINED_CLEANER);
    LogCleaner* cleaner = &log.cleaner;
    char buf[64];

    size_t segmentsNeededForCleaning = cleaner->SEGMENTS_PER_CLEANING_PASS;

    while (log.cleanableNewList.size() < segmentsNeededForCleaning - 1) {
        LogEntryHandle h = log.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        log.free(h);
    }

    SegmentVector segmentsToClean;

    cleaner->getSegmentsToClean(segmentsToClean);
    EXPECT_EQ(0U, segmentsToClean.size());

    while (log.cleanableNewList.size() < segmentsNeededForCleaning - 1) {
        LogEntryHandle h = log.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        log.free(h);
    }

    // fill the rest up so we can test proper sorting of best candidates
    try {
        while (log.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf)) != NULL) {}
    } catch (LogException &e) {
    }

    cleaner->getSegmentsToClean(segmentsToClean);

    EXPECT_EQ(segmentsNeededForCleaning, segmentsToClean.size());

    for (size_t i = 0; i < segmentsToClean.size(); i++) {
        EXPECT_TRUE(segmentsToClean[i] != NULL);
        EXPECT_LT(segmentsToClean[i]->getUtilisation(), 5);

        // returned segments must no longer be in the cleanableSegments list,
        // since we're presumed to clean them now
        EXPECT_EQ(cleaner->cleanableSegments.end(),
                  std::find(cleaner->cleanableSegments.begin(),
                            cleaner->cleanableSegments.end(),
                            segmentsToClean[i]));
    }
}

static bool
getSegmentsToCleanFilter(string s)
{
    return s == "getSegmentsToClean";
}

// sanity check the write cost calculation
TEST(LogCleanerTest, getSegmentsToClean_writeCost) {
    size_t segmentsNeededForCleaning = LogCleaner::SEGMENTS_PER_CLEANING_PASS;

    struct TestSetup {
        int freeEveryNth;       // after N appends, free one (0 => no frees)
        double minWriteCost;    // min acceptable writeCost (0 => no min)
        double maxWriteCost;    // max acceptable writeCost (0 => no max)
    } testSetup[] = {
        // - ~empty segments:     write cost should be very close to 1
        { 1, 1.00, 1.05 },

        // - ~50% full segments:  write cost should be very close to 2
        { 2, 2.00, 2.05 },

        // - ~100% full segments: write cost should be very high (100 if u=0.99)
        { 0, 100.00, 0 },

        // terminator
        { -1, 0, 0 },
    };

    for (int i = 0; testSetup[i].freeEveryNth != -1; i++) {
        Tub<uint64_t> serverId(0);
        Log log(serverId, 8192 * 1000, 8192, NULL, Log::INLINED_CLEANER);
        LogCleaner* cleaner = &log.cleaner;
        int freeEveryNth = testSetup[i].freeEveryNth;

        int j = 0;
        while (log.cleanableNewList.size() < segmentsNeededForCleaning) {
            char buf[64];
            LogEntryHandle h = log.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
            if (freeEveryNth && (j++ % freeEveryNth) == 0)
                log.free(h);
        }

        SegmentVector dummy;
        TestLog::Enable _(&getSegmentsToCleanFilter);
        cleaner->getSegmentsToClean(dummy);
        double writeCost;
        sscanf(TestLog::get().c_str(),                  // NOLINT sscanf ok here
            "getSegmentsToClean: writeCost is %lf", &writeCost);
        if (testSetup[i].minWriteCost)
            EXPECT_TRUE(writeCost >= testSetup[i].minWriteCost);
        if (testSetup[i].maxWriteCost)
            EXPECT_TRUE(writeCost <= testSetup[i].maxWriteCost);
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

TEST(LogCleanerTest, getSortedLiveEntries) {
    Tub<uint64_t> serverId(0);
    Log log(serverId, 8192 * 20, 8192, NULL, Log::INLINED_CLEANER);
    LogCleaner* cleaner = &log.cleaner;

    SegmentVector segments;
    LogCleaner::SegmentEntryHandleVector liveEntries;

    char buf[8192];
    LogEntryHandle older = log.append(LOG_ENTRY_TYPE_OBJ, buf, 1024);
    segments.push_back(log.head);
    LogEntryHandle newer = log.append(LOG_ENTRY_TYPE_OBJ, buf, 1024);
    wantNewer = newer;
    log.append(LOG_ENTRY_TYPE_OBJ, buf, 8192 - 2048);   // force old head closed

    log.registerType(LOG_ENTRY_TYPE_OBJ, livenessCB, NULL,
                                         relocationCB, NULL,
                                         timestampCB);

    cleaner->getSortedLiveEntries(segments, liveEntries);

    EXPECT_EQ(2U, liveEntries.size());
    EXPECT_EQ(older, liveEntries[0]);
    EXPECT_EQ(newer, liveEntries[1]);
}

} // namespace RAMCloud
