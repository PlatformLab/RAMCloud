/* Copyright (c) 2011 Stanford University
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

namespace RAMCloud {

/**
 * Unit tests for LogIterator.
 */
class LogIteratorTest : public ::testing::Test {
  public:
    ServerId serverId;
    Log l;

    LogIteratorTest()
        : serverId(ServerId(57, 0)),
          l(serverId, 10 * 8192, 8192, 4298)
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

TEST_F(LogIteratorTest, next) {

}

TEST_F(LogIteratorTest, populateSegmentList) {

}

} // namespace RAMCloud
