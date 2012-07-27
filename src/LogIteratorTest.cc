/* Copyright (c) 2011-2012 Stanford University
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

#include "LogIterator.h"
#include "Segment.h"

namespace RAMCloud {

/**
 * Unit tests for LogIterator.
 */
class LogIteratorTest : public ::testing::Test {
  public:
    Context context;
    ServerId serverId;
    Log l;

    LogIteratorTest()
        : context(),
          serverId(ServerId(57, 0)),
          l(context, serverId, 10 * 8192, 8192, 4298, NULL,
              Log::CLEANER_DISABLED)
    {
        l.registerType(LOG_ENTRY_TYPE_OBJ, true, NULL, NULL,
            NULL, NULL, NULL);
        l.registerType(LOG_ENTRY_TYPE_OBJTOMB, true, NULL, NULL,
            NULL, NULL, NULL);
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(LogIteratorTest);
};

TEST_F(LogIteratorTest, constructor_emptyLog) {
    LogIterator i(l);
    EXPECT_EQ(&l, &i.log);
    EXPECT_EQ(0U, i.segmentList.size());
    EXPECT_FALSE(i.currentIterator);
    EXPECT_EQ(static_cast<uint64_t>(-1), i.currentSegmentId);
    EXPECT_TRUE(i.headLocked);
}

TEST_F(LogIteratorTest, constructor_singleSegmentLog) {
    l.append(LOG_ENTRY_TYPE_OBJ, &serverId, sizeof(serverId));

    LogIterator i(l);
    EXPECT_EQ(&l, &i.log);
    EXPECT_EQ(0U, i.segmentList.size());
    EXPECT_TRUE(i.currentIterator);
    EXPECT_EQ(0U, i.currentSegmentId);
    EXPECT_TRUE(i.headLocked);
}

TEST_F(LogIteratorTest, constructor_multiSegmentLog) {
    l.append(LOG_ENTRY_TYPE_OBJ, &serverId, sizeof(serverId));
    Segment* oldHead = l.head;
    while (l.head == oldHead)
        l.append(LOG_ENTRY_TYPE_OBJ, &serverId, sizeof(serverId));

    LogIterator i(l);
    EXPECT_EQ(&l, &i.log);
    EXPECT_EQ(1U, i.segmentList.size());
    EXPECT_TRUE(i.currentIterator);
    EXPECT_EQ(0U, i.currentSegmentId);
    EXPECT_FALSE(i.headLocked);
    EXPECT_EQ(1, l.logIteratorCount);
}

TEST_F(LogIteratorTest, destructor) {
    // ensure the append lock is taken and released on destruction
    l.append(LOG_ENTRY_TYPE_OBJ, &serverId, sizeof(serverId));
    {
        LogIterator i(l);
        EXPECT_TRUE(i.headLocked);
        EXPECT_NE(0, l.appendLock.mutex.load());
    }
    EXPECT_EQ(0, l.appendLock.mutex.load());
    EXPECT_EQ(0, l.logIteratorCount);
}

TEST_F(LogIteratorTest, isDone_simple) {
    {
        LogIterator i(l);
        EXPECT_TRUE(i.isDone());
    }

    l.append(LOG_ENTRY_TYPE_OBJ, &serverId, sizeof(serverId));
    LogIterator i(l);
    EXPECT_FALSE(i.isDone());

    int cnt;
    for (cnt = 0; !i.isDone(); cnt++)
        i.next();
    EXPECT_EQ(3, cnt);
}

#if 0
TEST_F(LogIteratorTest, isDone_multiSegment) {
    int origObjCnt = 0;

    l.append(LOG_ENTRY_TYPE_OBJ, &serverId, sizeof(serverId));
    origObjCnt++;

    Segment* oldHead = l.head;
    while (l.head == oldHead) {
        l.append(LOG_ENTRY_TYPE_OBJ, &serverId, sizeof(serverId));
        origObjCnt++;
    }
    l.append(LOG_ENTRY_TYPE_OBJTOMB, &serverId, sizeof(serverId));

    LogEntryType lastType = LOG_ENTRY_TYPE_UNINIT;
    int objCnt = 0, tombCnt = 0, otherCnt = 0;
    for (LogIterator i(l); !i.isDone(); i.next()) {
        lastType = i.getHandle()->type();
        if (lastType == LOG_ENTRY_TYPE_OBJ)
            objCnt++;
        else if (lastType == LOG_ENTRY_TYPE_OBJTOMB)
            tombCnt++;
        else
            otherCnt++;
    }
    EXPECT_EQ(origObjCnt, objCnt);
    EXPECT_EQ(1, tombCnt);
    EXPECT_EQ(7, otherCnt);
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJTOMB, lastType);
}
#endif

TEST_F(LogIteratorTest, next) {
    {
        LogIterator i(l);
        EXPECT_TRUE(i.headLocked);
        EXPECT_FALSE(i.currentIterator);
        EXPECT_EQ(0U, i.segmentList.size());
        EXPECT_EQ(-1UL, i.currentSegmentId);
    }

    l.append(LOG_ENTRY_TYPE_OBJ, &serverId, sizeof(serverId));

    {
        LogIterator i(l);
        EXPECT_TRUE(i.headLocked);
        EXPECT_TRUE(i.currentIterator);
        EXPECT_EQ(0U, i.currentSegmentId);
        EXPECT_EQ(0U, i.segmentList.size());
        EXPECT_FALSE(i.currentIterator->isDone());
        SegmentEntryHandle prevHandle = i.getHandle();
        i.next();
        EXPECT_NE(prevHandle, i.getHandle());
        i.next();
        EXPECT_FALSE(i.isDone());
        i.next();
        EXPECT_TRUE(i.isDone());
    }

    Segment* oldHead = l.head;
    while (l.head == oldHead)
        l.append(LOG_ENTRY_TYPE_OBJ, &serverId, sizeof(serverId));

    {
        LogIterator i(l);
        EXPECT_FALSE(i.headLocked);
        EXPECT_TRUE(i.currentIterator);
        EXPECT_EQ(0U, i.currentSegmentId);
        EXPECT_EQ(1U, i.segmentList.size());

        while (!i.headLocked)
            i.next();

        EXPECT_TRUE(i.headLocked);
        EXPECT_TRUE(i.currentIterator);
        EXPECT_EQ(1U, i.currentSegmentId);
        EXPECT_EQ(0U, i.segmentList.size());

        while (!i.isDone())
            i.next();

        EXPECT_TRUE(i.headLocked);
        EXPECT_TRUE(i.currentIterator);
        EXPECT_EQ(1U, i.currentSegmentId);
        EXPECT_EQ(0U, i.segmentList.size());

        i.next();
        EXPECT_TRUE(i.isDone());
        EXPECT_TRUE(i.headLocked);
        EXPECT_FALSE(i.currentIterator);
        EXPECT_EQ(1U, i.currentSegmentId);
        EXPECT_EQ(0U, i.segmentList.size());

        // Ensure extra next()s is legal.
        for (int j = 0; j < 50; j++) {
            i.next();
            EXPECT_TRUE(i.isDone());
        }
    }

    {
        // Inject a "cleaner" segment into the log
        char alignedBuf[8192] __attribute__((aligned(8192)));
        Segment cleanerSeg(*serverId, 2, alignedBuf, sizeof(alignedBuf),
            NULL, 0);
        l.cleanableList.push_back(cleanerSeg);

        LogIterator i(l);
        while (!i.isDone())
            i.next();
        EXPECT_EQ(2U, i.currentSegmentId);

        l.cleanableList.pop_back();
    }
}

TEST_F(LogIteratorTest, populateSegmentList) {
        char alignedBuf[8192] __attribute__((aligned(8192)));
        Segment seg1(*serverId, 17243, alignedBuf, sizeof(alignedBuf));
        seg1.close(NULL);
        l.cleanableList.push_back(seg1);

        char alignedBuf2[8192] __attribute__((aligned(8192)));
        Segment seg2(*serverId, 754729, alignedBuf2, sizeof(alignedBuf2));
        seg2.close(NULL);
        l.cleanableNewList.push_back(seg2);

        char alignedBuf3[8192] __attribute__((aligned(8192)));
        Segment seg3(*serverId, 9999999, alignedBuf3, sizeof(alignedBuf3));
        seg3.close(NULL);

        l.head = &seg3;

        LogIterator i(l);
        EXPECT_EQ(17243U, i.getHandle()->userData<SegmentHeader>()->segmentId);
        EXPECT_EQ(2U, i.segmentList.size());
        EXPECT_EQ(754729U, i.segmentList[1]->getId());
        EXPECT_EQ(9999999U, i.segmentList[0]->getId());

        l.cleanableList.pop_back();
        l.cleanableNewList.pop_back();
        l.head = NULL;
}

// Ensure that the "cleaner", doesn't change the contents of the log
// until after iteration has completed (and iterators have been
// destroyed).
//
// Unclear if this belongs more in Log.cc, since it relies on some
// funny interaction between the Log and LogIterator, which breaks
// some module boundaries.
TEST_F(LogIteratorTest, cleanerInteraction) {
    l.append(LOG_ENTRY_TYPE_OBJ, &serverId, sizeof(serverId));
    Segment* oldHead = l.head;
    while (l.head == oldHead)
        l.append(LOG_ENTRY_TYPE_OBJ, &serverId, sizeof(serverId));

    Tub<LogIterator> i;
    i.construct(l);
    SegmentVector clean;
    std::vector<void*> unused;

    char alignedBuf[8192] __attribute__((aligned(8192)));
    Segment cleanerSeg(*serverId, 2, alignedBuf, sizeof(alignedBuf));
    cleanerSeg.close(NULL);

    l.getNewCleanableSegments(clean);
    l.cleaningInto(&cleanerSeg);

    // Fake having cleaned seg 0
    EXPECT_EQ(1U, clean.size());
    EXPECT_EQ(1U, l.cleanableList.size());
    clean.push_back(&l.cleanableList.back());
    l.cleaningComplete(clean, unused);

    // Sanity: seg 2 (cleaner seg) mustn't immediately become part of the log
    while (!i->isDone()) {
        foreach (Segment* s, i->segmentList)
            EXPECT_NE(2U, s->getId());
        i->next();
    }

    i.destroy();
    i.construct(l);

    // Nor must seg 2 join the log when a new head appears.
    oldHead = l.head;
    while (l.head == oldHead)
        l.append(LOG_ENTRY_TYPE_OBJ, &serverId, sizeof(serverId));
    l.append(LOG_ENTRY_TYPE_OBJ, &serverId, sizeof(serverId));
    while (!i->isDone()) {
        foreach (Segment* s, i->segmentList)
            EXPECT_NE(2U, s->getId());
        i->next();
    }

    EXPECT_EQ(1U, l.cleanablePendingDigestList.size());
    l.cleanablePendingDigestList.pop_back();
}

} // namespace RAMCloud
