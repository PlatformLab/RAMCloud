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

#include "TestUtil.h"

#include "Segment.h"
#include "SegmentIterator.h"
#include "LogEntryTypes.h"

namespace RAMCloud {

/**
 * Unit tests for SegmentIterator.
 */
class SegmentIteratorTest : public ::testing::Test {
  public:
    SegmentIteratorTest()
        : s()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
    }

    Segment s;

    DISALLOW_COPY_AND_ASSIGN(SegmentIteratorTest);
};

TEST_F(SegmentIteratorTest, constructor_fromSegment_empty) {
    EXPECT_NO_THROW(SegmentIterator(s));

    SegmentIterator it(s);
    EXPECT_TRUE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_SEGFOOTER, it.getType());
    EXPECT_EQ(sizeof(Segment::Footer), it.getLength());
}

TEST_F(SegmentIteratorTest, constructor_fromSegment_nonEmpty) {
    s.append(LOG_ENTRY_TYPE_OBJ, "hi", 2);

    SegmentIterator it(s);
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
    EXPECT_EQ(2U, it.getLength());

    it.next();
    EXPECT_TRUE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_SEGFOOTER, it.getType());
}

TEST_F(SegmentIteratorTest, constructor_fromBuffer) {
    char buf[8192];
    EXPECT_THROW(SegmentIterator(buf, 0), SegmentIteratorException);
    EXPECT_THROW(SegmentIterator(buf, sizeof(buf)), SegmentIteratorException);

    s.append(LOG_ENTRY_TYPE_OBJ, "hi", 2);

    Buffer buffer;
    s.appendToBuffer(buffer);
    buffer.copy(0, buffer.getTotalLength(), buf);

    EXPECT_NO_THROW(SegmentIterator(buf, buffer.getTotalLength()));
    EXPECT_NO_THROW(SegmentIterator(buf, sizeof(buf)));

    SegmentIterator it(s);
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
    EXPECT_EQ(2U, it.getLength());

    it.next();
    EXPECT_TRUE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_SEGFOOTER, it.getType());
}

TEST_F(SegmentIteratorTest, isDone) {
    EXPECT_TRUE(SegmentIterator(s).isDone());

    SegmentIterator it(s);
    s.append(LOG_ENTRY_TYPE_OBJ, "yo", 3);
    EXPECT_FALSE(it.isDone());
    it.next();
    EXPECT_TRUE(it.isDone());

    it.next();
    EXPECT_TRUE(it.isDone());
    it.next();
    EXPECT_TRUE(it.isDone());
}

TEST_F(SegmentIteratorTest, next) {
    SegmentIterator it(s);
    EXPECT_EQ(0U, it.currentOffset);
    it.next();
    EXPECT_EQ(0U, it.currentOffset);

    s.append(LOG_ENTRY_TYPE_OBJ, "blam", 5);
    // first iterator is no longer valid

    SegmentIterator it2(s);
    it2.next();
    EXPECT_EQ(7U, it2.currentOffset);

    it2.next();
    EXPECT_EQ(7U, it2.currentOffset);
    it2.next();
    EXPECT_EQ(7U, it2.currentOffset);
}

TEST_F(SegmentIteratorTest, getType) {
    SegmentIterator it(s);
    EXPECT_EQ(LOG_ENTRY_TYPE_SEGFOOTER, it.getType());

    s.append(LOG_ENTRY_TYPE_OBJ, "hi", 3);
    // first iterator is no longer valid

    SegmentIterator it2(s);
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it2.getType());
    it2.next();
    EXPECT_EQ(LOG_ENTRY_TYPE_SEGFOOTER, it2.getType());
}

TEST_F(SegmentIteratorTest, getLength) {
    SegmentIterator it(s);
    EXPECT_EQ(5U, it.getLength());

    s.append(LOG_ENTRY_TYPE_OBJ, "hi", 3);
    // first iterator is no longer valid

    SegmentIterator it2(s);
    EXPECT_EQ(3U, it2.getLength());
    it2.next();
    EXPECT_EQ(5U, it2.getLength());
}

TEST_F(SegmentIteratorTest, appendToBuffer) {
    s.append(LOG_ENTRY_TYPE_OBJ, "this is the content", 20);
    SegmentIterator it(s);
    Buffer buffer;
    it.appendToBuffer(buffer);
    EXPECT_EQ(20U, buffer.getTotalLength());
    EXPECT_EQ(0, memcmp("this is the content", buffer.getRange(0, 20), 20));
}

TEST_F(SegmentIteratorTest, setBufferTo) {
    s.append(LOG_ENTRY_TYPE_OBJ, "this is the content", 20);
    SegmentIterator it(s);
    Buffer buffer;
    buffer.append("junk first", 11);
    it.setBufferTo(buffer);
    EXPECT_EQ(20U, buffer.getTotalLength());
    EXPECT_EQ(0, memcmp("this is the content", buffer.getRange(0, 20), 20));
}

} // namespace RAMCloud
