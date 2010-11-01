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
#include "LogTypes.h"
#include "BackupManager.h"

namespace RAMCloud {

/**
 * Unit tests for Segment.
 */
class SegmentTest : public CppUnit::TestFixture {

    DISALLOW_COPY_AND_ASSIGN(SegmentTest); // NOLINT

    CPPUNIT_TEST_SUITE(SegmentTest);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_destructor);
    CPPUNIT_TEST(test_append);
    CPPUNIT_TEST(test_free);
    CPPUNIT_TEST(test_close);
    CPPUNIT_TEST(test_appendableBytes);
    CPPUNIT_TEST(test_forEachEntry);
    CPPUNIT_TEST(test_forceAppendBlob);
    CPPUNIT_TEST(test_forceAppendWithEntry);
    CPPUNIT_TEST_SUITE_END();

  public:
    SegmentTest() {}

    static bool
    openSegmentFilter(string s)
    {
        return s == "openSegment";
    }

    static bool
    writeSegmentFilter(string s)
    {
        return s == "writeSegment";
    }

    static bool
    closeSegmentFilter(string s)
    {
        return s == "closeSegment";
    }

    static bool
    freeSegmentFilter(string s)
    {
        return s == "freeSegment";
    }

    void
    test_constructor()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));

        BackupManager backup(NULL, 0);
        TestLog::Enable _(&openSegmentFilter);
        Segment s(1020304050, 98765, alignedBuf, sizeof(alignedBuf), &backup);
        CPPUNIT_ASSERT_EQUAL("openSegment: 1020304050, 98765", TestLog::get());
        CPPUNIT_ASSERT_EQUAL(s.baseAddress,
                             reinterpret_cast<void *>(alignedBuf));
        CPPUNIT_ASSERT_EQUAL(98765, s.id);
        CPPUNIT_ASSERT_EQUAL(8192, s.capacity);
        CPPUNIT_ASSERT_EQUAL(sizeof(SegmentEntry) + sizeof(SegmentHeader),
            s.tail);
        CPPUNIT_ASSERT_EQUAL(0UL, s.bytesFreed);

        SegmentEntry *se = reinterpret_cast<SegmentEntry *>(s.baseAddress);
        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_SEGHEADER, se->type);
        CPPUNIT_ASSERT_EQUAL(sizeof(SegmentHeader), se->length);

        SegmentHeader *sh = reinterpret_cast<SegmentHeader *>(
                            reinterpret_cast<char *>(se) + sizeof(*se));
        CPPUNIT_ASSERT_EQUAL(1020304050, sh->logId);
        CPPUNIT_ASSERT_EQUAL(98765, sh->segmentId);
        CPPUNIT_ASSERT_EQUAL(sizeof(alignedBuf), sh->segmentCapacity);
    }

    void
    test_destructor()
    {
        TestLog::Enable _(&freeSegmentFilter);
        {
            char alignedBuf[8192] __attribute__((aligned(8192)));
            BackupManager backup(NULL, 0);
            Segment s(1, 2, alignedBuf, sizeof(alignedBuf), &backup);
        }
        CPPUNIT_ASSERT_EQUAL("freeSegment: 1, 2", TestLog::get());
    }

    void
    test_append()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));
        const void *p;

        BackupManager backup(NULL, 0);
        TestLog::Enable _(&writeSegmentFilter);
        Segment s(1, 2, alignedBuf, sizeof(alignedBuf), &backup);
        p = s.append(LOG_ENTRY_TYPE_SEGFOOTER, NULL, 0);
        CPPUNIT_ASSERT_EQUAL(NULL, p);

        s.closed = true;
        p = s.append(LOG_ENTRY_TYPE_OBJ, alignedBuf, 1);
        CPPUNIT_ASSERT_EQUAL(NULL, p);
        s.closed = false;

        p = s.append(LOG_ENTRY_TYPE_OBJ, NULL, s.appendableBytes() + 1);
        CPPUNIT_ASSERT_EQUAL(NULL, p);

        CPPUNIT_ASSERT_EQUAL(
            "writeSegment: 1, 2, 0, ..., 8 | "
            "writeSegment: 1, 2, 8, ..., 20",
            TestLog::get());

        int bytes = s.appendableBytes();
        char buf[bytes];
        for (int i = 0; i < bytes; i++)
            buf[i] = i;

        p = s.append(LOG_ENTRY_TYPE_OBJ, buf, bytes);
        CPPUNIT_ASSERT(p != NULL);

        SegmentEntry *se = reinterpret_cast<SegmentEntry *>(
                           reinterpret_cast<char *>(s.baseAddress) +
                           sizeof(SegmentEntry) + sizeof(SegmentHeader));

        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_OBJ, se->type);
        CPPUNIT_ASSERT_EQUAL(bytes, se->length);
        CPPUNIT_ASSERT_EQUAL(0,
            memcmp(buf, reinterpret_cast<char *>(se) + sizeof(*se), bytes));
    }

    void
    test_free()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));
        char buf[12];

        Segment s(1, 2, alignedBuf, sizeof(alignedBuf));
        const void *p = s.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        s.free(p);
        CPPUNIT_ASSERT_EQUAL(sizeof(buf) + sizeof(SegmentEntry), s.bytesFreed);
    }

    void
    test_close()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));

        BackupManager backup(NULL, 0);
        TestLog::Enable _(&closeSegmentFilter);
        Segment s(1, 2, alignedBuf, sizeof(alignedBuf), &backup);
        s.close();
        CPPUNIT_ASSERT_EQUAL("closeSegment: 1, 2", TestLog::get());

        SegmentEntry *se = reinterpret_cast<SegmentEntry *>(
                           reinterpret_cast<char *>(s.baseAddress) +
                           sizeof(SegmentEntry) + sizeof(SegmentHeader));
        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_SEGFOOTER, se->type);
        CPPUNIT_ASSERT_EQUAL(sizeof(SegmentFooter), se->length);

        SegmentFooter *sf = reinterpret_cast<SegmentFooter *>(
                            reinterpret_cast<char *>(se) + sizeof(*se));
        CPPUNIT_ASSERT_EQUAL(0x7baf8437964589e0ull, sf->checksum);

        CPPUNIT_ASSERT_EQUAL(0, s.appendableBytes());
        CPPUNIT_ASSERT_EQUAL(true, s.closed);
    }

    // The following tests are not ordered with respect to the code,
    // since the early abort line in Segment::appendableBytes() requires
    // segment closure.
    void
    test_appendableBytes()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));

        Segment s(1, 2, alignedBuf, sizeof(alignedBuf));
        CPPUNIT_ASSERT_EQUAL(sizeof(alignedBuf) - 3 * sizeof(SegmentEntry) -
            sizeof(SegmentHeader) - sizeof(SegmentFooter), s.appendableBytes());

        char buf[57];
        while (s.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf)) != NULL) {}
        CPPUNIT_ASSERT_EQUAL(15, s.appendableBytes());

        s.close();
        CPPUNIT_ASSERT_EQUAL(0, s.appendableBytes());
    }

    static void
    callback_forEachEntry(LogEntryType type, const void *p, uint64_t length,
        void *cookie)
    {
        static int i = 0;

        CPPUNIT_ASSERT(i == 0 || i == 1);

        if (i == 0) {
            CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_SEGHEADER, type);
            CPPUNIT_ASSERT_EQUAL(sizeof(SegmentHeader), length);
            CPPUNIT_ASSERT_EQUAL(NULL, cookie);

            const SegmentHeader *sh =
                reinterpret_cast<const SegmentHeader *>(p);
            CPPUNIT_ASSERT_EQUAL(112233, sh->logId);
            CPPUNIT_ASSERT_EQUAL(445566, sh->segmentId);
        } else {
            CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_SEGFOOTER, type);
            CPPUNIT_ASSERT_EQUAL(sizeof(SegmentFooter), length);
            CPPUNIT_ASSERT_EQUAL(NULL, cookie);
        }

        i++;
    }

    void
    test_forEachEntry()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));

        Segment s(112233, 445566, alignedBuf, sizeof(alignedBuf));
        s.close();
        s.forEachEntry(callback_forEachEntry, NULL);
    }

    void
    test_forceAppendBlob()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));

        char buf[64];
        for (unsigned int i = 0; i < sizeof(buf); i++)
            buf[i] = i;

        Segment s(112233, 445566, alignedBuf, sizeof(alignedBuf));
        s.forceAppendBlob(buf, sizeof(buf));
        CPPUNIT_ASSERT_EQUAL(0, memcmp(buf, reinterpret_cast<char *>(
            s.baseAddress) + sizeof(SegmentEntry) + sizeof(SegmentHeader),
            sizeof(buf)));
        CPPUNIT_ASSERT_EQUAL(sizeof(buf) + sizeof(SegmentEntry) +
            sizeof(SegmentHeader), s.tail);
    }

    void
    test_forceAppendWithEntry()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));
        const void *p;

        char buf[64];
        for (unsigned int i = 0; i < sizeof(buf); i++)
            buf[i] = i;

        Segment s(112233, 445566, alignedBuf, sizeof(alignedBuf));
        p = s.forceAppendWithEntry(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        CPPUNIT_ASSERT(p != NULL);

        const SegmentEntry *se = reinterpret_cast<const SegmentEntry *>(
                                 reinterpret_cast<const char *>(p) -
                                 sizeof(SegmentEntry));
        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_OBJ, se->type);
        CPPUNIT_ASSERT_EQUAL(sizeof(buf), se->length);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(buf, p, sizeof(buf)));

        s.tail = s.capacity - sizeof(SegmentEntry) - sizeof(buf) + 1;
        p = s.forceAppendWithEntry(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        CPPUNIT_ASSERT(p == NULL);

        s.tail--;
        p = s.forceAppendWithEntry(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        CPPUNIT_ASSERT(p != NULL);
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(SegmentTest);

} // namespace RAMCloud
