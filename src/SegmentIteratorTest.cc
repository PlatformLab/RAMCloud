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
class SegmentIteratorTest : public CppUnit::TestFixture {

    DISALLOW_COPY_AND_ASSIGN(SegmentIteratorTest); // NOLINT

    CPPUNIT_TEST_SUITE(SegmentIteratorTest);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_isEntryValid);
    CPPUNIT_TEST(test_isDone);
    CPPUNIT_TEST(test_next);
    CPPUNIT_TEST(test_getters);
    CPPUNIT_TEST(test_isChecksumValid);
    CPPUNIT_TEST(test_isSegmentChecksumValid);
    CPPUNIT_TEST(test_generateChecksum);
    CPPUNIT_TEST_SUITE_END();

  public:
    SegmentIteratorTest() {}

    void
    test_constructor()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));
        memset(alignedBuf, 0, sizeof(alignedBuf));

        Segment s(1020304050, 98765, alignedBuf, sizeof(alignedBuf));

        SegmentIterator si(&s);
        CPPUNIT_ASSERT_EQUAL((const void *)alignedBuf, si.baseAddress);
        CPPUNIT_ASSERT_EQUAL(sizeof(alignedBuf), si.segmentCapacity);
        CPPUNIT_ASSERT_EQUAL(98765, si.id);
        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_SEGHEADER, si.type);
        CPPUNIT_ASSERT_EQUAL(sizeof(SegmentHeader), si.length);
        CPPUNIT_ASSERT_EQUAL(reinterpret_cast<const char *>(si.baseAddress) +
            sizeof(SegmentEntry), reinterpret_cast<const char *>(si.blobPtr));
        CPPUNIT_ASSERT_EQUAL(si.baseAddress, (const void *)si.firstEntry);
        CPPUNIT_ASSERT_EQUAL(si.baseAddress, (const void *)si.currentEntry);
        CPPUNIT_ASSERT_EQUAL(false, si.sawFooter);

        SegmentIterator si2(alignedBuf, sizeof(alignedBuf));
        CPPUNIT_ASSERT_EQUAL((const void *)alignedBuf, si2.baseAddress);
        CPPUNIT_ASSERT_EQUAL(sizeof(alignedBuf), si2.segmentCapacity);
        CPPUNIT_ASSERT_EQUAL(98765, si2.id);
        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_SEGHEADER, si2.type);
        CPPUNIT_ASSERT_EQUAL(sizeof(SegmentHeader), si2.length);
        CPPUNIT_ASSERT_EQUAL(reinterpret_cast<const char *>(si2.baseAddress) +
            sizeof(SegmentEntry), reinterpret_cast<const char *>(si2.blobPtr));
        CPPUNIT_ASSERT_EQUAL(si2.baseAddress, (const void *)si2.firstEntry);
        CPPUNIT_ASSERT_EQUAL(si2.baseAddress, (const void *)si2.currentEntry);
        CPPUNIT_ASSERT_EQUAL(false, si2.sawFooter);

        CPPUNIT_ASSERT_THROW(
            SegmentIterator si3(alignedBuf, sizeof(alignedBuf) - 1),
            SegmentIteratorException);

        memset(alignedBuf, 0, sizeof(SegmentEntry));
        CPPUNIT_ASSERT_THROW(
            SegmentIterator si3(alignedBuf, sizeof(alignedBuf)),
            SegmentIteratorException);
    }

    void
    test_isEntryValid()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));
        memset(alignedBuf, 0, sizeof(alignedBuf));

        Segment s(1020304050, 98765, alignedBuf, sizeof(alignedBuf));
        SegmentIterator si(&s);

        SegmentEntry *se = reinterpret_cast<SegmentEntry *>(alignedBuf);
        CPPUNIT_ASSERT_EQUAL(true, si.isEntryValid(se));

        se->length = sizeof(alignedBuf) - sizeof(SegmentEntry);
        CPPUNIT_ASSERT_EQUAL(true, si.isEntryValid(se));

        se->length++;
        CPPUNIT_ASSERT_EQUAL(false, si.isEntryValid(se));
    }

    void
    test_isDone()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));
        memset(alignedBuf, 0, sizeof(alignedBuf));

        Segment s(1020304050, 98765, alignedBuf, sizeof(alignedBuf));
        SegmentIterator si(&s);

        CPPUNIT_ASSERT_EQUAL(false, si.isDone());

        si.sawFooter = true;
        CPPUNIT_ASSERT_EQUAL(true, si.isDone());

        si.sawFooter = false;
        SegmentEntry *se = reinterpret_cast<SegmentEntry *>(alignedBuf);
        se->length = sizeof(alignedBuf) + 1;
        CPPUNIT_ASSERT_EQUAL(true, si.isDone());
    }

    void
    test_next()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));
        memset(alignedBuf, 0, sizeof(alignedBuf));

        {
            Segment s(1020304050, 98765, alignedBuf, sizeof(alignedBuf));
            SegmentIterator si(&s);

            si.currentEntry = NULL;
            si.next();
            CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_INVALID, si.type);
            CPPUNIT_ASSERT_EQUAL(0, si.length);
            CPPUNIT_ASSERT_EQUAL(NULL, si.blobPtr);
        }

        {
            Segment s(1020304050, 98765, alignedBuf, sizeof(alignedBuf));
            SegmentIterator si(&s);

            SegmentEntry *se = const_cast<SegmentEntry *>(si.currentEntry);
            se->type = LOG_ENTRY_TYPE_SEGFOOTER;
            si.next();
            CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_INVALID, si.type);
            CPPUNIT_ASSERT_EQUAL(0, si.length);
            CPPUNIT_ASSERT_EQUAL(NULL, si.blobPtr);
            CPPUNIT_ASSERT_EQUAL(true, si.sawFooter);
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
            CPPUNIT_ASSERT_EQUAL(NULL, si.currentEntry);
        }

        {
            Segment s(1020304050, 98765, alignedBuf, sizeof(alignedBuf));
            SegmentIterator si(&s);

            s.close();
            si.next();
            CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_SEGFOOTER, si.type);
            CPPUNIT_ASSERT_EQUAL(sizeof(SegmentFooter), si.length);
        }
    }

    void
    test_getters()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));
        memset(alignedBuf, 0, sizeof(alignedBuf));

        Segment s(1020304050, 98765, alignedBuf, sizeof(alignedBuf));

        static char buf;
        SegmentEntryHandle h = s.append(LOG_ENTRY_TYPE_OBJ, &buf, sizeof(buf));
        SegmentIterator si(&s);

        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_SEGHEADER, si.getType());
        CPPUNIT_ASSERT_EQUAL(sizeof(SegmentHeader), si.getLength());
        CPPUNIT_ASSERT_EQUAL(sizeof(SegmentEntry) + sizeof(SegmentHeader),
            si.getLengthInLog());
        CPPUNIT_ASSERT(si.getLogTime() == LogTime(98765, 0));
        CPPUNIT_ASSERT_EQUAL((const void *)(alignedBuf + sizeof(SegmentEntry)),
            si.getPointer());
        CPPUNIT_ASSERT_EQUAL((uintptr_t)si.getPointer() -
            (uintptr_t)si.baseAddress, si.getOffset());

        si.next();
        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_OBJ, si.getType());
        CPPUNIT_ASSERT_EQUAL(sizeof(buf), si.getLength());
        uint32_t segmentOffset = h->logTime().second;
        CPPUNIT_ASSERT(si.getLogTime() == LogTime(98765, segmentOffset));
        CPPUNIT_ASSERT(si.getLogTime() > LogTime(98765, 0));

        si.currentEntry = NULL;
        CPPUNIT_ASSERT_THROW(si.getType(), SegmentIteratorException);
        CPPUNIT_ASSERT_THROW(si.getLength(), SegmentIteratorException);
        CPPUNIT_ASSERT_THROW(si.getLengthInLog(), SegmentIteratorException);
        CPPUNIT_ASSERT_THROW(si.getLogTime(), SegmentIteratorException);
        CPPUNIT_ASSERT_THROW(si.getPointer(), SegmentIteratorException);
        CPPUNIT_ASSERT_THROW(si.getType(), SegmentIteratorException);
    }

    void
    test_generateChecksum()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));
        memset(alignedBuf, 0, sizeof(alignedBuf));
        Segment s(1, 2, alignedBuf, sizeof(alignedBuf));
        SegmentIterator i(&s);
        CPPUNIT_ASSERT_EQUAL(0x0bd2d711, i.generateChecksum());
    }

    void
    test_isChecksumValid()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));
        memset(alignedBuf, 0, sizeof(alignedBuf));
        Segment s(1, 2, alignedBuf, sizeof(alignedBuf));
        SegmentIterator i(&s);
        CPPUNIT_ASSERT_EQUAL(true, i.isChecksumValid());
        alignedBuf[sizeof(SegmentEntry)]++;
        CPPUNIT_ASSERT_EQUAL(false, i.isChecksumValid());
    }

    void
    test_isSegmentChecksumValid()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));
        memset(alignedBuf, 0, sizeof(alignedBuf));

        Segment s(1, 2, alignedBuf, sizeof(alignedBuf));
        CPPUNIT_ASSERT_THROW(SegmentIterator(
            s.baseAddress, s.capacity).isSegmentChecksumValid(),
            SegmentIteratorException);
        s.close();

        CPPUNIT_ASSERT_EQUAL(true, SegmentIterator(
            s.baseAddress, s.capacity).isSegmentChecksumValid());
        alignedBuf[sizeof(SegmentEntry)]++;
        CPPUNIT_ASSERT_EQUAL(false, SegmentIterator(
            s.baseAddress, s.capacity).isSegmentChecksumValid());
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(SegmentIteratorTest);

} // namespace RAMCloud
