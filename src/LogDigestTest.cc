/* Copyright (c) 2012-2014 Stanford University
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

#include "LogDigest.h"
#include "TestLog.h"

namespace RAMCloud {

/**
 * Unit tests for LogDigest.
 */
class LogDigestTest : public ::testing::Test {
  public:
    LogDigestTest()
    {
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(LogDigestTest);
};

TEST_F(LogDigestTest, constructor_empty)
{
    LogDigest d;
    EXPECT_EQ(0U, d.header.checksum);
    EXPECT_EQ(0U, d.segmentIds.size());
    EXPECT_EQ(0U, d.checksum.getResult());
}

TEST_F(LogDigestTest, constructor_fromSerializedDigest_empty)
{
    LogDigest d;
    Buffer buffer;
    d.appendToBuffer(buffer);

    EXPECT_NO_THROW(LogDigest(buffer.getRange(0, buffer.size()),
        buffer.size()));
}

TEST_F(LogDigestTest, constructor_fromSerializedDigest_nonempty)
{
    LogDigest d;
    d.addSegmentId(3842);
    Buffer buffer;
    d.appendToBuffer(buffer);

    EXPECT_NO_THROW(LogDigest(buffer.getRange(0, buffer.size()),
        buffer.size()));

    LogDigest d2(buffer.getRange(0, buffer.size()),
        buffer.size());
    EXPECT_EQ(1U, d2.size());
    EXPECT_EQ(3842U, d2[0]);
}

TEST_F(LogDigestTest, construct_fromSerializedDigest_noHeader)
{
    TestLog::Enable _;
    char buf[50];
    EXPECT_THROW(LogDigest(buf, 2), LogDigestException);
    EXPECT_EQ("LogDigest: buffer too small to hold header (length = 2)",
        TestLog::get());
}

TEST_F(LogDigestTest, construct_fromSerializedDigest_impossibleLength)
{
    TestLog::Enable _;
    char buf[50];
    EXPECT_THROW(LogDigest(buf, 9), LogDigestException);
    EXPECT_EQ("LogDigest: length left not even 64-bit multiple (5)",
        TestLog::get());
}

TEST_F(LogDigestTest, construct_fromSerializedDigest_badChecksum)
{
    TestLog::Enable _;
    LogDigest d;
    d.addSegmentId(58234);
    d.segmentIds[0]++;

    Buffer buffer;
    d.appendToBuffer(buffer);

    EXPECT_THROW(LogDigest(buffer.getRange(0, buffer.size()),
        buffer.size()), LogDigestException);
    EXPECT_EQ("LogDigest: invalid digest checksum (computed 0x8ca6694d, "
        "expect 0xc59a146a", TestLog::get());
}

TEST_F(LogDigestTest, addSegmentId_and_index_operator)
{
    LogDigest d;
    for (uint32_t i = 0; i < 50; i++) {
        EXPECT_EQ(i, d.size());
        d.addSegmentId(~i);
        EXPECT_EQ(i + 1, d.size());
        EXPECT_EQ(~i, d[i]);
    }
}

TEST_F(LogDigestTest, appendToBuffer)
{
    LogDigest d;
    Buffer buffer;
    d.appendToBuffer(buffer);
    EXPECT_EQ(4U, buffer.size());

    LogDigest d2;
    d2.addSegmentId(43);
    d2.addSegmentId(76);
    Buffer buffer2;
    d2.appendToBuffer(buffer2);
    EXPECT_EQ(20U, buffer2.size());
    EXPECT_EQ(static_cast<const void*>(&d2.header),
        buffer2.getStart<LogDigest::Header>());
    EXPECT_EQ(static_cast<const void*>(&d2.segmentIds[0]),
        buffer2.getRange(4, buffer2.size() - 4));

    LogDigest d3;
    for (uint32_t i = 0; i < 50; i++) {
        d3.addSegmentId(i);
        Buffer buffer3;
        d3.appendToBuffer(buffer3);
        EXPECT_EQ(4U + (8 * (i + 1)), buffer3.size());
    }
}

} // namespace RAMCloud
