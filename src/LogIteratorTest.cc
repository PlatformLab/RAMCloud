/* Copyright (c) 2011-2015 Stanford University
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

#include "Log.h"
#include "LogIterator.h"
#include "ReplicaManager.h"
#include "Segment.h"
#include "SegmentManager.h"
#include "ServerConfig.h"
#include "ServerList.h"
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

/**
 * Unit tests for LogIterator.
 */
class LogIteratorTest : public ::testing::Test {
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
    char data[1000];

    LogIteratorTest()
        : context(),
          serverId(ServerId(57, 0)),
          serverList(&context),
          serverConfig(ServerConfig::forTesting()),
          replicaManager(&context, &serverId, 0, false, false),
          masterTableMetadata(),
          allocator(&serverConfig),
          segmentManager(&context, &serverConfig, &serverId,
                         allocator, replicaManager, &masterTableMetadata),
          entryHandlers(),
          l(&context, &serverConfig, &entryHandlers,
            &segmentManager, &replicaManager),
          data()
    {
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(LogIteratorTest);
};

TEST_F(LogIteratorTest, constructor_emptyLog) {
    LogIterator i(l);
    EXPECT_TRUE(i.isDone());
    EXPECT_EQ(&l, &i.log);
    EXPECT_EQ(0U, i.segmentList.size());
    EXPECT_FALSE(i.currentIterator);
    EXPECT_EQ(-1UL, i.currentSegmentId);
}

TEST_F(LogIteratorTest, constructor_singleSegmentLog) {
    l.sync();
    l.append(LOG_ENTRY_TYPE_OBJ, data, sizeof(data));
    l.sync();

    LogIterator i(l);
    EXPECT_EQ(&l, &i.log);
    EXPECT_EQ(1U, i.segmentList.size());
    EXPECT_TRUE(i.currentIterator);
    EXPECT_EQ(1U, i.currentSegmentId);
}

TEST_F(LogIteratorTest, constructor_multiSegmentLog) {
    l.sync();
    while (l.head == NULL || l.head->id == 1)
        l.append(LOG_ENTRY_TYPE_OBJ, data, sizeof(data));
    l.sync();

    LogIterator i(l);
    EXPECT_EQ(&l, &i.log);
    EXPECT_EQ(2U, i.segmentList.size());
    EXPECT_TRUE(i.currentIterator);
    EXPECT_EQ(1U, i.currentSegmentId);
}

TEST_F(LogIteratorTest, next_basics) {
    l.sync();
    LogIterator i(l);
    EXPECT_FALSE(i.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_SEGHEADER, i.getType());
    i.next();

    EXPECT_FALSE(i.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_LOGDIGEST, i.getType());
    i.next();

    EXPECT_FALSE(i.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_TABLESTATS, i.getType());
    i.next();

    EXPECT_FALSE(i.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_SAFEVERSION, i.getType());
    i.next();

    EXPECT_TRUE(i.isDone());
}

TEST_F(LogIteratorTest, next_computeLastSegment) {
    // Create one full segment and one incomplete segment in the log,
    // then iterate until the current head is reached.
    int writeCount = 0;
    while (l.head == NULL || l.head->id < 2) {
        l.append(LOG_ENTRY_TYPE_OBJ, data, sizeof(data));
        writeCount++;
    }

    LogIterator i(l);
    int readCount = 0;
    while (!i.onHead()) {
        i.next();
        if (i.getType() == LOG_ENTRY_TYPE_OBJ) {
            readCount++;
        }
    }
    EXPECT_EQ(writeCount-1, readCount);

    // Fill up the second segment, create part of a third, iterate once,
    // then add yet more objects to the third segment. Then check how
    // far we can iterate (iteration must not include the third collection of
    // objects).
    while (l.head->id < 3) {
        l.append(LOG_ENTRY_TYPE_OBJ, data, sizeof(data));
        writeCount++;
    }
    i.next();
    if (i.getType() == LOG_ENTRY_TYPE_OBJ) {
        readCount++;
    }
    for (int j = 0; j < 5; j++) {
        l.append(LOG_ENTRY_TYPE_OBJ, data, sizeof(data));
    }
    while (1) {
        i.next();
        if (i.isDone()) {
            break;
        }
        if (i.getType() == LOG_ENTRY_TYPE_OBJ) {
            readCount++;
        }
    }
    EXPECT_EQ(writeCount, readCount);
}

TEST_F(LogIteratorTest, next_multipleSegments) {
    // Create 3 segments in the log, count the objects in each
    // segment (only 1 object in the last segment).
    int seg1Count = 0, seg2Count = 0;
    while (l.head == NULL || l.head->id < 2) {
        l.append(LOG_ENTRY_TYPE_OBJ, data, sizeof(data));
        seg1Count++;
    }
    seg1Count--;                // Most recent object is in 2nd segment
    seg2Count = 1;
    while (l.head->id < 3) {
        l.append(LOG_ENTRY_TYPE_OBJ, data, sizeof(data));
        seg2Count++;
    }
    seg2Count--;                // Most recent object is in 3rd segment

    // Iterate over the segments and count entries found in each.
    LogIterator i(l);
    int check1 = 0, check2 = 0, check3 = 0;
    while (!i.isDone()) {
        if (i.getType() == LOG_ENTRY_TYPE_OBJ) {
            if (i.currentSegmentId == 1) {
                check1++;
            } else if (i.currentSegmentId == 2) {
                check2++;
            } else if (i.currentSegmentId == 3) {
                check3++;
            } else {
                EXPECT_EQ(3u, i.currentSegmentId);
            }
        }
        i.next();
    }
    EXPECT_EQ(seg1Count, check1);
    EXPECT_EQ(seg2Count, check2);
    EXPECT_EQ(1, check3);
}

TEST_F(LogIteratorTest, next_setLimitForHeadSegment) {
    // Make the head segment overflow between when the head is first
    // reached and the next call to next.
    l.append(LOG_ENTRY_TYPE_OBJ, data, sizeof(data));
    int writeCount = 1;
    LogIterator i(l);
    EXPECT_TRUE(i.onHead());
    while (l.head->id < 2) {
        l.append(LOG_ENTRY_TYPE_OBJ, data, sizeof(data));
        writeCount++;
    }
    for (int j = 0; j < 5; j++) {
        l.append(LOG_ENTRY_TYPE_OBJ, data, sizeof(data));
        writeCount++;
    }
    int readCount = 0;
    while (1) {
        i.next();
        if (i.isDone()) {
            break;
        }
        if (i.getType() == LOG_ENTRY_TYPE_OBJ) {
            readCount++;
        }
    }
    EXPECT_EQ(writeCount, readCount);
}

TEST_F(LogIteratorTest, populateSegmentList) {
        l.sync();
        LogSegment* seg1 = segmentManager.allocHeadSegment();
        LogSegment* seg2 = segmentManager.allocHeadSegment();
        LogSegment* seg3 = segmentManager.allocHeadSegment();

        LogIterator i(l);
        EXPECT_EQ(4U, i.segmentList.size());

        i.segmentList.clear();
        i.populateSegmentList(0);
        EXPECT_EQ(4U, i.segmentList.size());

        i.segmentList.clear();
        i.populateSegmentList(1);
        EXPECT_EQ(4U, i.segmentList.size());

        i.segmentList.clear();
        i.populateSegmentList(2);
        EXPECT_EQ(3U, i.segmentList.size());

        i.segmentList.clear();
        i.populateSegmentList(3);
        EXPECT_EQ(2U, i.segmentList.size());

        i.segmentList.clear();
        i.populateSegmentList(4);
        EXPECT_EQ(1U, i.segmentList.size());

        i.segmentList.clear();
        i.populateSegmentList(5);
        EXPECT_EQ(0U, i.segmentList.size());

        // ensure segments are sorted
        i.segmentList.clear();
        *const_cast<uint64_t*>(&seg1->id) = 15;
        *const_cast<uint64_t*>(&seg2->id) = 10;
        *const_cast<uint64_t*>(&seg3->id) = 12;
        i.populateSegmentList(2);
        EXPECT_EQ(3U, i.segmentList.size());
        EXPECT_EQ(seg2, i.segmentList[2]);
        EXPECT_EQ(seg3, i.segmentList[1]);
        EXPECT_EQ(seg1, i.segmentList[0]);
}
} // namespace RAMCloud
