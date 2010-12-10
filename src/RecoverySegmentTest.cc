/* Copyright (c) 2010 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <ext/algorithm>

#include "TestUtil.h"
#include "RecoverySegment.h"

namespace RAMCloud {

// --- RecoverySegmentIteratorTest ---

class RecoverySegmentIteratorTest : public ::testing::Test {
  public:
    RecoverySegmentIteratorTest()
        : segment(0)
    {}

    RecoverySegment segment;

  private:
    DISALLOW_COPY_AND_ASSIGN(RecoverySegmentIteratorTest);
};

TEST_F(RecoverySegmentIteratorTest, isDoneEmpty) {
    RecoverySegment::Iterator it(segment);
    EXPECT_TRUE(it.isDone());
}

TEST_F(RecoverySegmentIteratorTest, isDone) {
    segment.append(LOG_ENTRY_TYPE_OBJ, NULL, 0);

    RecoverySegment::Iterator it(segment);
    EXPECT_FALSE(it.isDone());
}

TEST_F(RecoverySegmentIteratorTest, next) {
    segment.append(LOG_ENTRY_TYPE_OBJ, NULL, 0);

    RecoverySegment::Iterator it(segment);
}

TEST_F(RecoverySegmentIteratorTest, nextWhileAtEnd) {
    RecoverySegment::Iterator it(segment);
    EXPECT_EQ(0u, it.offset);
    it.next();
    EXPECT_EQ(0u, it.offset);
}

TEST_F(RecoverySegmentIteratorTest, getEntry) {
    char junk[17];
    segment.append(LOG_ENTRY_TYPE_OBJ, junk, sizeof(junk));

    RecoverySegment::Iterator it(segment);
    EXPECT_FALSE(it.isDone());
    auto& entry = it.getEntry();
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, entry.type);
    EXPECT_EQ(sizeof(junk), entry.length);
}

TEST_F(RecoverySegmentIteratorTest, getPointer) {
    const char* msg = "why can't I own a canadian?";
    segment.append(LOG_ENTRY_TYPE_OBJ, msg, strlen(msg) + 1);

    RecoverySegment::Iterator it(segment);
    EXPECT_FALSE(it.isDone());
    EXPECT_STREQ(msg, static_cast<const char*>(it.getPointer()));
}

TEST_F(RecoverySegmentIteratorTest, getOffset) {
    char junk[17];
    segment.append(LOG_ENTRY_TYPE_OBJ, junk, sizeof(junk));

    RecoverySegment::Iterator it(segment);
    EXPECT_FALSE(it.isDone());
    auto offset = it.getOffset();
    EXPECT_EQ(sizeof(SegmentEntry), offset);
}

// --- RecoverySegmentTest ---

TEST(RecoverySegmentTest, append) {
    RecoverySegment segment(0);
    EXPECT_EQ(0u, segment.size());
    const char* msg = "foo";
    segment.append(LOG_ENTRY_TYPE_OBJ, msg, strlen(msg) + 1);

    RecoverySegment::Iterator it(segment);
    EXPECT_FALSE(it.isDone());
    EXPECT_STREQ(msg, it.get<const char>());
    it.next();
    EXPECT_TRUE(it.isDone());
}

TEST(RecoverySegmentTest, copy) {
    RecoverySegment segment(0);
    EXPECT_EQ(0u, segment.size());

    const char* msg = "foo";
    segment.append(LOG_ENTRY_TYPE_OBJ, msg, strlen(msg) + 1);
    EXPECT_EQ(sizeof(SegmentEntry) + strlen(msg) + 1, segment.size());

    char buf[segment.size()];
    segment.copy(buf);
    RecoverySegment::Iterator it(segment);
    EXPECT_STREQ(msg, it.get<const char>());
}

} // namespace RAMCloud
