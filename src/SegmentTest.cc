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
#include "Log.h"
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
    CPPUNIT_TEST(test_forceAppendBlob);
    CPPUNIT_TEST(test_forceAppendWithEntry);
    CPPUNIT_TEST(test_syncToBackup);
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
        return s == "write";
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

        Tub<uint64_t> serverId;
        serverId.construct(1020304050);
        BackupManager backup(NULL, serverId, 0);
        TestLog::Enable _(&openSegmentFilter);
        Segment s(1020304050, 98765, alignedBuf, sizeof(alignedBuf), &backup);
        CPPUNIT_ASSERT_EQUAL("openSegment: "
                             "openSegment 1020304050, 98765, ..., 32",
                             TestLog::get());
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

        // be sure we count the header written in the LogStats
        Tub<uint64_t> serverId2;
        serverId2.construct(0);
        Log l(serverId2, 8192, 8192);
        Segment s2(&l, 0, alignedBuf, sizeof(alignedBuf), NULL,
                   LOG_ENTRY_TYPE_INVALID, NULL, 0);
        CPPUNIT_ASSERT_EQUAL(l.getBytesAppended(), s2.tail);
    }

    void
    test_destructor()
    {
        TestLog::Enable _(&freeSegmentFilter);
        {
            char alignedBuf[8192] __attribute__((aligned(8192)));
            Tub<uint64_t> serverId;
            serverId.construct(1);
            BackupManager backup(NULL, serverId, 0);
            Segment s(1, 2, alignedBuf, sizeof(alignedBuf), &backup);
        }
        CPPUNIT_ASSERT_EQUAL("freeSegment: 1, 2", TestLog::get());
    }

    void
    test_append()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));
        SegmentEntryHandle seh;

        Tub<uint64_t> serverId;
        serverId.construct(1);
        BackupManager backup(NULL, serverId, 0);
        TestLog::Enable _;
        Segment s(1, 2, alignedBuf, sizeof(alignedBuf), &backup);
        seh = s.append(LOG_ENTRY_TYPE_SEGFOOTER, NULL, 0);
        CPPUNIT_ASSERT_EQUAL(NULL, seh);

        s.closed = true;
        seh = s.append(LOG_ENTRY_TYPE_OBJ, alignedBuf, 1);
        CPPUNIT_ASSERT_EQUAL(NULL, seh);
        s.closed = false;

        seh = s.append(LOG_ENTRY_TYPE_OBJ, NULL,
                       s.appendableBytes() + 1);
        CPPUNIT_ASSERT_EQUAL(NULL, seh);

        CPPUNIT_ASSERT_EQUAL(
            "openSegment: openSegment 1, 2, ..., 32",
            TestLog::get());

        char c = '!';
        uint64_t lengthInSegment;
        uint64_t offsetInSegment;
        seh = s.append(LOG_ENTRY_TYPE_OBJ, &c, sizeof(c),
            &lengthInSegment, &offsetInSegment);
        CPPUNIT_ASSERT(seh != NULL);
        CPPUNIT_ASSERT_EQUAL(sizeof(c) + sizeof(SegmentEntry), lengthInSegment);
        CPPUNIT_ASSERT_EQUAL(sizeof(SegmentEntry) + sizeof(SegmentHeader),
            offsetInSegment);

        int bytes = s.appendableBytes();
        char buf[bytes];
        for (int i = 0; i < bytes; i++)
            buf[i] = static_cast<char>(i);

        seh = s.append(LOG_ENTRY_TYPE_OBJ, buf, bytes);
        CPPUNIT_ASSERT(seh != NULL);

        SegmentEntry *se = reinterpret_cast<SegmentEntry *>(
                           reinterpret_cast<char *>(s.baseAddress) +
                           sizeof(SegmentEntry) + sizeof(SegmentHeader) +
                           sizeof(SegmentEntry) + sizeof(c));

        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_OBJ, se->type);
        CPPUNIT_ASSERT_EQUAL(bytes, se->length);
        CPPUNIT_ASSERT_EQUAL(0,
            memcmp(buf, reinterpret_cast<char *>(se) + sizeof(*se), bytes));
    }

    void
    test_free()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));
        static char buf[12];

        Segment s(1, 2, alignedBuf, sizeof(alignedBuf));
        SegmentEntryHandle h = s.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        s.free(h);
        CPPUNIT_ASSERT_EQUAL(sizeof(buf) + sizeof(SegmentEntry), s.bytesFreed);
    }

    void
    test_close()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));

        Tub<uint64_t> serverId;
        serverId.construct(1);
        BackupManager backup(NULL, serverId, 0);
        Segment s(1, 2, alignedBuf, sizeof(alignedBuf), &backup);
        TestLog::Enable _;
        s.close();
        CPPUNIT_ASSERT_EQUAL("write: 1, 2, 48, 1 | "
                             "sync: Closed segment 1, 2",
                             TestLog::get());

        SegmentEntry *se = reinterpret_cast<SegmentEntry *>(
                           reinterpret_cast<char *>(s.baseAddress) +
                           sizeof(SegmentEntry) + sizeof(SegmentHeader));
        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_SEGFOOTER, se->type);
        CPPUNIT_ASSERT_EQUAL(sizeof(SegmentFooter), se->length);

        SegmentFooter *sf = reinterpret_cast<SegmentFooter *>(
                            reinterpret_cast<char *>(se) + sizeof(*se));
        CPPUNIT_ASSERT_EQUAL(s.checksum.getResult(), sf->checksum);

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

        static char buf[83];
        int i = 0;
        while (s.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf)))
            i++;
        CPPUNIT_ASSERT_EQUAL(85, i);
        CPPUNIT_ASSERT_EQUAL(57, s.appendableBytes());

        s.close();
        CPPUNIT_ASSERT_EQUAL(0, s.appendableBytes());
    }

    void
    test_forceAppendBlob()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));

        char buf[64];
        for (unsigned int i = 0; i < sizeof(buf); i++)
            buf[i] = static_cast<char>(i);

        Tub<uint64_t> serverId;
        serverId.construct(0);
        Log l(serverId, 8192, 8192);
        Segment s(&l, 445566, alignedBuf, sizeof(alignedBuf), NULL,
                  LOG_ENTRY_TYPE_INVALID, NULL, 0);
        uint64_t bytesBeforeAppend = l.getBytesAppended();
        s.forceAppendBlob(buf, sizeof(buf));
        CPPUNIT_ASSERT_EQUAL(0, memcmp(buf, reinterpret_cast<char *>(
            s.baseAddress) + sizeof(SegmentEntry) + sizeof(SegmentHeader),
            sizeof(buf)));
        CPPUNIT_ASSERT_EQUAL(sizeof(buf) + sizeof(SegmentEntry) +
            sizeof(SegmentHeader), s.tail);
        CPPUNIT_ASSERT_EQUAL(bytesBeforeAppend + sizeof(buf),
            l.getBytesAppended());
    }

    void
    test_forceAppendWithEntry()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));
        SegmentEntryHandle seh;

        char buf[64];
        for (unsigned int i = 0; i < sizeof(buf); i++)
            buf[i] = static_cast<char>(i);

        Segment s(112233, 445566, alignedBuf, sizeof(alignedBuf));
        seh = s.forceAppendWithEntry(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        CPPUNIT_ASSERT(seh != NULL);

        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_OBJ, seh->type());
        CPPUNIT_ASSERT_EQUAL(sizeof(buf), seh->length());
        CPPUNIT_ASSERT_EQUAL(0, memcmp(buf, seh->userData(), sizeof(buf)));

        s.tail = downCast<uint32_t>(s.capacity - sizeof(SegmentEntry) -
                                    sizeof(buf) + 1);
        seh = s.forceAppendWithEntry(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        CPPUNIT_ASSERT_EQUAL(NULL, seh);

        s.tail--;
        seh = s.forceAppendWithEntry(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        CPPUNIT_ASSERT(seh != NULL);
    }

    void
    test_syncToBackup()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));
        Tub<uint64_t> serverId;
        serverId.construct(1);
        BackupManager backup(NULL, serverId, 0);
        Segment s(1, 2, alignedBuf, sizeof(alignedBuf), &backup);
        static SegmentHeader header;
        TestLog::Enable _;
        s.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header),
            NULL, NULL, false);
        CPPUNIT_ASSERT_EQUAL("", TestLog::get());
        s.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header),
            NULL, NULL, true);
        CPPUNIT_ASSERT_EQUAL("write: 1, 2, 96, 0",
                             TestLog::get());
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(SegmentTest);

} // namespace RAMCloud
