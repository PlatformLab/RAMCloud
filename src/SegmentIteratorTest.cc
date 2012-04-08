/* Copyright (c) 2010 Stanford University
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
#include "LogTypes.h"

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

TEST_F(SegmentIteratorTest, constructor) {
    char alignedBuf[8192] __attribute__((aligned(8192)));
    memset(alignedBuf, 0, sizeof(alignedBuf));

    Segment s(1020304050, 98765, alignedBuf, sizeof(alignedBuf));

    SegmentIterator si(&s);
    EXPECT_EQ((const void *)alignedBuf, si.baseAddress);
    EXPECT_EQ(sizeof(alignedBuf), si.segmentCapacity);
    EXPECT_EQ(98765U, si.id);
    EXPECT_EQ(LOG_ENTRY_TYPE_SEGHEADER, si.type);
    EXPECT_EQ(sizeof(SegmentHeader), si.length);
    EXPECT_EQ(reinterpret_cast<const char *>(si.baseAddress) +
        sizeof(SegmentEntry), reinterpret_cast<const char *>(si.blobPtr));
    EXPECT_EQ(si.baseAddress, (const void *)si.firstEntry);
    EXPECT_EQ(si.baseAddress, (const void *)si.currentEntry);
    EXPECT_FALSE(si.sawFooter);

    SegmentIterator si2(alignedBuf, sizeof(alignedBuf));
    EXPECT_EQ((const void *)alignedBuf, si2.baseAddress);
    EXPECT_EQ(sizeof(alignedBuf), si2.segmentCapacity);
    EXPECT_EQ(98765U, si2.id);
    EXPECT_EQ(LOG_ENTRY_TYPE_SEGHEADER, si2.type);
    EXPECT_EQ(sizeof(SegmentHeader), si2.length);
    EXPECT_EQ(reinterpret_cast<const char *>(si2.baseAddress) +
        sizeof(SegmentEntry), reinterpret_cast<const char *>(si2.blobPtr));
    EXPECT_EQ(si2.baseAddress, (const void *)si2.firstEntry);
    EXPECT_EQ(si2.baseAddress, (const void *)si2.currentEntry);
    EXPECT_FALSE(si2.sawFooter);

    EXPECT_THROW(
        SegmentIterator si3(alignedBuf, sizeof(alignedBuf) - 1),
        SegmentIteratorException);

    memset(alignedBuf, 0, sizeof(SegmentEntry));
    EXPECT_THROW(
        SegmentIterator si3(alignedBuf, sizeof(alignedBuf)),
        SegmentIteratorException);
}

TEST_F(SegmentIteratorTest, isEntryValid) {
    char alignedBuf[8192] __attribute__((aligned(8192)));
    memset(alignedBuf, 0, sizeof(alignedBuf));

    Segment s(1020304050, 98765, alignedBuf, sizeof(alignedBuf));
    SegmentIterator si(alignedBuf, sizeof(alignedBuf));

    SegmentEntry *se = reinterpret_cast<SegmentEntry *>(alignedBuf);
    EXPECT_TRUE(si.isEntryValid(se));

    se->length = sizeof(alignedBuf) - sizeof(SegmentEntry);
    EXPECT_TRUE(si.isEntryValid(se));

    se->length++;
    EXPECT_FALSE(si.isEntryValid(se));
}

TEST_F(SegmentIteratorTest, isDone) {
    char alignedBuf[8192] __attribute__((aligned(8192)));
    memset(alignedBuf, 0, sizeof(alignedBuf));

    Segment s(1020304050, 98765, alignedBuf, sizeof(alignedBuf));
    SegmentIterator si(&s);

    EXPECT_FALSE(si.isDone());

    si.sawFooter = true;
    EXPECT_TRUE(si.isDone());

    si.sawFooter = false;
    SegmentEntry *se = reinterpret_cast<SegmentEntry *>(alignedBuf);
    se->length = sizeof(alignedBuf) + 1;
    EXPECT_TRUE(si.isDone());
}

TEST_F(SegmentIteratorTest, next) {
    char alignedBuf[8192] __attribute__((aligned(8192)));
    memset(alignedBuf, 0, sizeof(alignedBuf));

    {
        Segment s(1020304050, 98765, alignedBuf, sizeof(alignedBuf));
        SegmentIterator si(&s);

        si.currentEntry = NULL;
        si.next();
        EXPECT_EQ(LOG_ENTRY_TYPE_INVALID, si.type);
        EXPECT_EQ(0U, si.length);
        EXPECT_TRUE(NULL == si.blobPtr);
    }

    {
        Segment s(1020304050, 98765, alignedBuf, sizeof(alignedBuf));
        SegmentIterator si(&s);

        SegmentEntry *se = const_cast<SegmentEntry *>(si.currentEntry);
        se->type = LOG_ENTRY_TYPE_SEGFOOTER;
        si.next();
        EXPECT_EQ(LOG_ENTRY_TYPE_INVALID, si.type);
        EXPECT_EQ(0U, si.length);
        EXPECT_TRUE(NULL == si.blobPtr);
        EXPECT_TRUE(si.sawFooter);
    }

    {
        Segment s(1020304050, 98765, alignedBuf, sizeof(alignedBuf));
        SegmentIterator si(&s);

        SegmentEntry *se = const_cast<SegmentEntry *>(si.currentEntry);
        se->length = sizeof(alignedBuf) + 1;
        SegmentEntry *next = reinterpret_cast<SegmentEntry*>(
                reinterpret_cast<char*>(se) + sizeof(*se) + se->length);
        next->length = 10 * 1024 * 1024;
        si.next();
        EXPECT_TRUE(NULL == si.currentEntry);
    }

    {
        Segment s(1020304050, 98765, alignedBuf, sizeof(alignedBuf));
        SegmentIterator si(alignedBuf, sizeof(alignedBuf));

        s.close(NULL);
        si.next(); // Skip the padding entry.
        si.next(); // Now pointing at the footer entry.
        EXPECT_EQ(LOG_ENTRY_TYPE_SEGFOOTER, si.type);
        EXPECT_EQ(sizeof(SegmentFooter), si.length);
    }
}

TEST_F(SegmentIteratorTest, getters) {
    char alignedBuf[8192] __attribute__((aligned(8192)));
    memset(alignedBuf, 0, sizeof(alignedBuf));

    Segment s(1020304050, 98765, alignedBuf, sizeof(alignedBuf));

    static char buf;
    SegmentEntryHandle h = s.append(LOG_ENTRY_TYPE_OBJ, &buf, sizeof(buf));
    SegmentIterator si(&s);

    EXPECT_EQ(LOG_ENTRY_TYPE_SEGHEADER, si.getType());
    EXPECT_EQ(sizeof(SegmentHeader), si.getLength());
    EXPECT_EQ(sizeof(SegmentEntry) + sizeof(SegmentHeader),
        si.getLengthInLog());
    EXPECT_EQ(si.getLogPosition(), LogPosition(98765, 0));
    EXPECT_EQ((const void *)(alignedBuf + sizeof(SegmentEntry)),
        si.getPointer());
    EXPECT_EQ((uintptr_t)si.getPointer() -
        (uintptr_t)si.baseAddress, si.getOffset());

    si.next();
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, si.getType());
    EXPECT_EQ(sizeof(buf), si.getLength());
    uint32_t segmentOffset = h->logPosition().segmentOffset();
    EXPECT_EQ(si.getLogPosition(), LogPosition(98765, segmentOffset));
    EXPECT_TRUE(si.getLogPosition() > LogPosition(98765, 0));

    si.currentEntry = NULL;
    EXPECT_THROW(si.getType(), SegmentIteratorException);
    EXPECT_THROW(si.getLength(), SegmentIteratorException);
    EXPECT_THROW(si.getLengthInLog(), SegmentIteratorException);
    EXPECT_THROW(si.getLogPosition(), SegmentIteratorException);
    EXPECT_THROW(si.getPointer(), SegmentIteratorException);
    EXPECT_THROW(si.getType(), SegmentIteratorException);
}

TEST_F(SegmentIteratorTest, generateChecksum) {
    char alignedBuf[8192] __attribute__((aligned(8192)));
    memset(alignedBuf, 0, sizeof(alignedBuf));
    Segment s(1, 2, alignedBuf, sizeof(alignedBuf));
    SegmentIterator i(&s);
    EXPECT_EQ(0x941b1041, i.generateChecksum());
}

TEST_F(SegmentIteratorTest, isChecksumValid) {
    char alignedBuf[8192] __attribute__((aligned(8192)));
    memset(alignedBuf, 0, sizeof(alignedBuf));
    Segment s(1, 2, alignedBuf, sizeof(alignedBuf));
    SegmentIterator i(&s);
    EXPECT_TRUE(i.isChecksumValid());
    alignedBuf[sizeof(SegmentEntry)]++;
    EXPECT_FALSE(i.isChecksumValid());
}

TEST_F(SegmentIteratorTest, isCleanerSegment) {
    char alignedBuf[8192] __attribute__((aligned(8192)));
    memset(alignedBuf, 0, sizeof(alignedBuf));
    Segment s(1, 2, alignedBuf, sizeof(alignedBuf));
    EXPECT_FALSE(SegmentIterator(&s).isCleanerSegment());

    SegmentHeader* sh = reinterpret_cast<SegmentHeader*>(
        reinterpret_cast<uintptr_t>(s.baseAddress) + sizeof(SegmentEntry));
    sh->headSegmentIdDuringCleaning = 72374823421UL;
    EXPECT_TRUE(SegmentIterator(&s).isCleanerSegment());
}

TEST_F(SegmentIteratorTest, isSegmentChecksumValid) {
    char alignedBuf[8192] __attribute__((aligned(8192)));
    memset(alignedBuf, 0, sizeof(alignedBuf));

    Segment s(1, 2, alignedBuf, sizeof(alignedBuf));
    EXPECT_THROW(SegmentIterator(
        s.baseAddress, s.capacity).isSegmentChecksumValid(),
        SegmentIteratorException);
    s.close(NULL);

    EXPECT_TRUE(SegmentIterator(
        s.baseAddress, s.capacity).isSegmentChecksumValid());
    alignedBuf[sizeof(SegmentEntry)]++;
    EXPECT_FALSE(SegmentIterator(
        s.baseAddress, s.capacity).isSegmentChecksumValid());
}

} // namespace RAMCloud
