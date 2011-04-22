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
#include "RecoverySegmentIterator.h"

namespace RAMCloud {

// --- RecoverySegmentIteratorTest ---

class RecoverySegmentIteratorTest : public ::testing::Test {
  public:
    RecoverySegmentIteratorTest()
        : segmentSize(1024)
        , segment()
    {}

    const uint32_t segmentSize;
    char segment[1024];

  private:
    DISALLOW_COPY_AND_ASSIGN(RecoverySegmentIteratorTest);
};

TEST_F(RecoverySegmentIteratorTest, isDoneEmpty) {
    RecoverySegmentIterator it(0, 0);
    EXPECT_TRUE(it.isDone());
}

TEST_F(RecoverySegmentIteratorTest, isDone) {
    SegmentEntry* entry = reinterpret_cast<SegmentEntry*>(segment);
    entry->type = LOG_ENTRY_TYPE_OBJ;
    entry->length = 0;

    RecoverySegmentIterator it(segment, segmentSize);
    EXPECT_FALSE(it.isDone());
}

TEST_F(RecoverySegmentIteratorTest, next) {
    SegmentEntry* entry = reinterpret_cast<SegmentEntry*>(segment);
    entry->type = LOG_ENTRY_TYPE_OBJ;
    entry->length = 0;

    RecoverySegmentIterator it(segment, segmentSize);
}

TEST_F(RecoverySegmentIteratorTest, nextWhileAtEnd) {
    RecoverySegmentIterator it(0, 0);
    EXPECT_EQ(0u, it.offset);
    it.next();
    EXPECT_EQ(0u, it.offset);
}

TEST_F(RecoverySegmentIteratorTest, getEntry) {
    SegmentEntry* entry = reinterpret_cast<SegmentEntry*>(segment);
    entry->type = LOG_ENTRY_TYPE_OBJ;
    entry->length = 17;

    RecoverySegmentIterator it(segment, segmentSize);
    EXPECT_FALSE(it.isDone());
    auto& e = it.getEntry();
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, e.type);
    EXPECT_EQ(entry->length, e.length);
}

TEST_F(RecoverySegmentIteratorTest, getPointer) {
    const char* msg = "why can't I own a canadian?";

    SegmentEntry* entry = reinterpret_cast<SegmentEntry*>(segment);
    entry->type = LOG_ENTRY_TYPE_OBJ;
    entry->length = downCast<uint32_t>(strlen(msg)) + 1;
    memcpy(segment + sizeof(SegmentEntry), msg, strlen(msg) + 1);

    RecoverySegmentIterator it(segment, segmentSize);
    EXPECT_FALSE(it.isDone());
    EXPECT_STREQ(msg, static_cast<const char*>(it.getPointer()));
}

TEST_F(RecoverySegmentIteratorTest, getOffset) {
    SegmentEntry* entry = reinterpret_cast<SegmentEntry*>(segment);
    entry->type = LOG_ENTRY_TYPE_OBJ;
    entry->length = 17;

    RecoverySegmentIterator it(segment, segmentSize);
    EXPECT_FALSE(it.isDone());
    auto offset = it.getOffset();
    EXPECT_EQ(sizeof(SegmentEntry), offset);
}

} // namespace RAMCloud
