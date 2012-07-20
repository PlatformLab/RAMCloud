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
    SegmentIteratorTest() {}

  private:
    DISALLOW_COPY_AND_ASSIGN(SegmentIteratorTest);
};

TEST_F(SegmentIteratorTest, constructor_fromSegment_empty) {
    Segment s;
    EXPECT_NO_THROW(SegmentIterator(s));
    
    SegmentIterator it(s);
    EXPECT_TRUE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_SEGFOOTER, it.getType());
    EXPECT_EQ(sizeof(Segment::Footer), it.getLength());
}

TEST_F(SegmentIteratorTest, constructor_fromSegment_nonEmpty) {
    Segment s;
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

    Segment s;
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

} // namespace RAMCloud
