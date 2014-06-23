/* Copyright (c) 2010-2014 Stanford University
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
        , certificate()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
        s.getAppendedLength(&certificate);
    }

    Segment s;
    Segment::Certificate certificate;

    DISALLOW_COPY_AND_ASSIGN(SegmentIteratorTest);
};

TEST_F(SegmentIteratorTest, constructor_fromSegment_empty) {
    SegmentIterator it(s);
    EXPECT_EQ(0u, it.certificate.segmentLength);
    EXPECT_EQ(0x48674bc7u, it.certificate.checksum);
    EXPECT_NO_THROW(it.checkMetadataIntegrity());
    EXPECT_TRUE(it.isDone());
}

TEST_F(SegmentIteratorTest, constructor_fromSegment_nonEmpty) {
    EXPECT_TRUE(s.append(LOG_ENTRY_TYPE_OBJ, "hi", 2));

    SegmentIterator it(s);
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
    EXPECT_EQ(2U, it.getLength());

    it.next();
    EXPECT_TRUE(it.isDone());
}

TEST_F(SegmentIteratorTest, constructor_fromBuffer) {
    char buf[8192];
    {
        SegmentIterator it(buf, 0, Segment::Certificate());
        EXPECT_THROW(it.checkMetadataIntegrity(),
                     SegmentIteratorException);
    }
    {
        SegmentIterator it(buf, 0, certificate);
        EXPECT_NO_THROW(it.checkMetadataIntegrity());
    }

    s.append(LOG_ENTRY_TYPE_OBJ, "hi", 2);

    Buffer buffer;
    s.appendToBuffer(buffer);
    buffer.copy(0, buffer.size(), buf);

    // The original certificate (prior to the above append) should still
    // work.
    {
        SegmentIterator it(buf, buffer.size(), certificate);
        EXPECT_NO_THROW(it.checkMetadataIntegrity());
        EXPECT_TRUE(it.isDone());
    }

    // The new certificate (including the above append) should also work.
    {
        s.getAppendedLength(&certificate);
        SegmentIterator it(buf, buffer.size(), certificate);
        EXPECT_NO_THROW(it.checkMetadataIntegrity());
        EXPECT_FALSE(it.isDone());
        EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
        EXPECT_EQ(2U, it.getLength());
        it.next();
        EXPECT_TRUE(it.isDone());
    }

    // The certificate should also work if the segment length given is larger
    // than the actual contents.
    {
        s.getAppendedLength(&certificate);
        SegmentIterator it(buf, sizeof(buf), certificate);
        EXPECT_NO_THROW(it.checkMetadataIntegrity());
        EXPECT_FALSE(it.isDone());
        EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
        EXPECT_EQ(2U, it.getLength());
        it.next();
        EXPECT_TRUE(it.isDone());
    }
}

TEST_F(SegmentIteratorTest, constructor_fromBufferNull) {
    SegmentIterator it(NULL, 0, certificate);
    EXPECT_NO_THROW(it.checkMetadataIntegrity());
    EXPECT_TRUE(it.isDone());
}

TEST_F(SegmentIteratorTest, isDone) {
    EXPECT_TRUE(SegmentIterator(s).isDone());

    s.append(LOG_ENTRY_TYPE_OBJ, "yo", 3);
    SegmentIterator it(s);
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
    EXPECT_EQ(s.getEntryHeader(0), it.currentHeader);
    it.getLength();
    EXPECT_TRUE(it.currentLength);
    it.next();
    EXPECT_TRUE(it.currentLength);  // isDone() is true, so noop
    EXPECT_EQ(0U, it.currentOffset);
    EXPECT_EQ(s.getEntryHeader(0), it.currentHeader);

    s.append(LOG_ENTRY_TYPE_OBJ, "blam", 5);
    // first iterator is no longer valid

    SegmentIterator it2(s);
    it2.getLength();
    EXPECT_TRUE(it2.currentLength);
    it2.next();
    EXPECT_FALSE(it2.currentLength);
    EXPECT_EQ(7U, it2.currentOffset);
    EXPECT_EQ(s.getEntryHeader(7), it2.currentHeader);

    it2.next();
    EXPECT_EQ(7U, it2.currentOffset);
    EXPECT_EQ(s.getEntryHeader(7), it2.currentHeader);
    it2.next();
    EXPECT_EQ(7U, it2.currentOffset);
    EXPECT_EQ(s.getEntryHeader(7), it2.currentHeader);
}

TEST_F(SegmentIteratorTest, getType) {
    s.append(LOG_ENTRY_TYPE_OBJ, "hi", 3);
    s.append(LOG_ENTRY_TYPE_OBJTOMB, "hi", 3);

    SegmentIterator it(s);
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
    it.next();
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJTOMB, it.getType());
    it.next();
}

TEST_F(SegmentIteratorTest, getLength) {
    s.append(LOG_ENTRY_TYPE_OBJ, "hi", 3);
    s.append(LOG_ENTRY_TYPE_OBJTOMB, "hihi", 5);

    SegmentIterator it(s);
    EXPECT_EQ(3U, it.getLength());
    it.next();
    EXPECT_EQ(5U, it.getLength());
}

TEST_F(SegmentIteratorTest, appendToBuffer) {
    s.append(LOG_ENTRY_TYPE_OBJ, "this is the content", 20);
    SegmentIterator it(s);
    Buffer buffer;
    it.appendToBuffer(buffer);
    EXPECT_EQ(20U, buffer.size());
    EXPECT_EQ(0, memcmp("this is the content", buffer.getRange(0, 20), 20));
}

TEST_F(SegmentIteratorTest, setBufferTo) {
    s.append(LOG_ENTRY_TYPE_OBJ, "this is the content", 20);
    SegmentIterator it(s);
    Buffer buffer;
    buffer.appendExternal("junk first", 11);
    it.setBufferTo(buffer);
    EXPECT_EQ(20U, buffer.size());
    EXPECT_EQ(0, memcmp("this is the content", buffer.getRange(0, 20), 20));
}

} // namespace RAMCloud
