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
    CPPUNIT_TEST(test_append_handle);
    CPPUNIT_TEST(test_rollBack);
    CPPUNIT_TEST(test_free);
    CPPUNIT_TEST(test_close);
    CPPUNIT_TEST(test_appendableBytes);
    CPPUNIT_TEST(test_forceAppendBlob);
    CPPUNIT_TEST(test_forceAppendWithEntry);
    CPPUNIT_TEST(test_syncToBackup);
    CPPUNIT_TEST(test_getSegmentBaseAddress);
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
                             "openSegment 1020304050, 98765, ..., 30",
                             TestLog::get());
        CPPUNIT_ASSERT_EQUAL(s.baseAddress,
                             reinterpret_cast<void *>(alignedBuf));
        CPPUNIT_ASSERT_EQUAL(98765, s.id);
        CPPUNIT_ASSERT_EQUAL(8192, s.capacity);
        CPPUNIT_ASSERT_EQUAL(sizeof(SegmentEntry) + sizeof(SegmentHeader),
            s.tail);
        CPPUNIT_ASSERT_EQUAL(0UL, s.bytesFreed);
        CPPUNIT_ASSERT_EQUAL(0UL, s.spaceTimeSum);

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

        // Segments must be power-of-two sized <= 2GB and be
        // aligned to their capacity.
        
        CPPUNIT_ASSERT_THROW(Segment(&l, 0, alignedBuf, sizeof(alignedBuf) + 1,
            NULL, LOG_ENTRY_TYPE_INVALID, NULL, 0), SegmentException);
        CPPUNIT_ASSERT_THROW(Segment(&l, 0, alignedBuf, 0x80000001,
            NULL, LOG_ENTRY_TYPE_INVALID, NULL, 0), SegmentException);

        CPPUNIT_ASSERT_THROW(Segment(&l, 0, &alignedBuf[1], sizeof(alignedBuf),
            NULL, LOG_ENTRY_TYPE_INVALID, NULL, 0), SegmentException);
        CPPUNIT_ASSERT_THROW(Segment(&l, 0, &alignedBuf[8], sizeof(alignedBuf),
            NULL, LOG_ENTRY_TYPE_INVALID, NULL, 0), SegmentException);
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
            "openSegment: openSegment 1, 2, ..., 30",
            TestLog::get());

        char c = '!';
        seh = s.append(LOG_ENTRY_TYPE_OBJ, &c, sizeof(c));
        CPPUNIT_ASSERT(seh != NULL);
        CPPUNIT_ASSERT_EQUAL(sizeof(c) + sizeof(SegmentEntry),
            seh->totalLength());
        CPPUNIT_ASSERT_EQUAL(sizeof(SegmentEntry) + sizeof(SegmentHeader),
            seh->logTime().second);

        // ensure the checksum argument works
        CPPUNIT_ASSERT_THROW(s.append(LOG_ENTRY_TYPE_OBJ, &c, 1, true, 5),
            SegmentException);

        int bytes = s.appendableBytes();
        char buf[bytes];
        for (int i = 0; i < bytes; i++)
            buf[i] = static_cast<char>(i);

        seh = s.append(LOG_ENTRY_TYPE_OBJ, buf, bytes, true, 0x0597577e);
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
    test_append_handle()
    {
        // Create one Segment, write an object, then write it to another
        // Segment.

        char alignedBuf[8192] __attribute__((aligned(8192)));
        Segment s(1, 2, alignedBuf, sizeof(alignedBuf), NULL);
        uint32_t object = 0xdeadbeef;
        SegmentEntryHandle seh;
        seh = s.append(LOG_ENTRY_TYPE_OBJ, &object, sizeof(object));
        CPPUNIT_ASSERT(seh != NULL);

        // Alterior motive: choose a different Segment size to test
        // the checksum code / mutableFields nonsense.
        char alignedBuf2[16384] __attribute__((aligned(16384)));
        Segment s2(2, 3, alignedBuf2, sizeof(alignedBuf2), NULL);
        SegmentEntryHandle copySeh = s2.append(seh, false);
        CPPUNIT_ASSERT(copySeh != NULL);
         
        CPPUNIT_ASSERT_EQUAL(seh->checksum(), copySeh->checksum());
        CPPUNIT_ASSERT_EQUAL(seh->generateChecksum(),
            copySeh->generateChecksum());
        CPPUNIT_ASSERT_EQUAL(0,
            memcmp(seh->userData(), copySeh->userData(), seh->length()));
    }

    void
    test_rollBack()
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));
        Segment s(1, 2, alignedBuf, sizeof(alignedBuf), NULL);
        SegmentEntryHandle seh;

        uint32_t object = 0xcafebeef;
        seh = s.append(LOG_ENTRY_TYPE_OBJ, &object, sizeof(object), true);
        CPPUNIT_ASSERT(seh != NULL);
        CPPUNIT_ASSERT_EQUAL(false, s.canRollBack);
        CPPUNIT_ASSERT_THROW(s.rollBack(seh), SegmentException);

        // Save the current Segment state. After appending and rolling back it
        // should be exactly the same as before _except for_ the saved segment
        // checksum as of the last append. Segments aren't copyable, so just
        // memcpy the contents for comparison.
        SegmentChecksum savePrevChecksum = s.prevChecksum;
        memset(&s.prevChecksum, 0, sizeof(s.prevChecksum)); 
        char saveForComparison[sizeof(Segment)];
        memcpy(saveForComparison, &s, sizeof(Segment));
        s.prevChecksum = savePrevChecksum;

        SegmentChecksum::ResultType checksumPreAppend = s.checksum.getResult();

        seh = s.append(LOG_ENTRY_TYPE_OBJ, &object, sizeof(object), false);
        CPPUNIT_ASSERT(seh != NULL);
        CPPUNIT_ASSERT_EQUAL(true, s.canRollBack);

        s.rollBack(seh);
        CPPUNIT_ASSERT_EQUAL(checksumPreAppend, s.checksum.getResult());
        memset(&s.prevChecksum, 0, sizeof(s.prevChecksum)); 
        CPPUNIT_ASSERT_EQUAL(0, memcmp(saveForComparison, &s, sizeof(Segment)));
    }

    static bool
    livenessCallback(LogEntryHandle oldHandle,
                     void* cookie)
    {
        return true;
    }

    static bool
    relocationCallback(LogEntryHandle oldHandle,
                       LogEntryHandle newHandle,
                       void* cookie)
    {
        return true;
    }

    static uint32_t
    timestampCallback(LogEntryHandle h)
    {
        return h->userData<Object>()->timestamp;
    }

    void
    test_free()
    {
        // create a fake log so we can use the timestamp callback
        // (the Log-less segment constructor won't result in the
        // spaceTimeSum value being altered otherwise.
        Log l({0}, 8192, 8192);
        l.registerType(LOG_ENTRY_TYPE_OBJ,
                       livenessCallback, NULL,
                       relocationCallback, NULL,
                       timestampCallback);

        char alignedBuf[8192] __attribute__((aligned(8192)));
        static char buf[64];

        reinterpret_cast<Object*>(buf)->timestamp = 0xf00f1234U;

        Segment s(1, 2, alignedBuf, sizeof(alignedBuf));
        *const_cast<Log**>(&s.log) = &l;
        SegmentEntryHandle h = s.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        s.free(h);
        CPPUNIT_ASSERT_EQUAL(sizeof(buf) + sizeof(SegmentEntry), s.bytesFreed);
        CPPUNIT_ASSERT_EQUAL(0UL, s.spaceTimeSum);
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
                             "write: Segment 2 closed (length 48) | "
                             "proceedNoMetrics: Closed segment 1, 2",
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
        CPPUNIT_ASSERT_EQUAL(87, i);
        CPPUNIT_ASSERT_EQUAL(47, s.appendableBytes());

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
        // create a fake log; see comment in test_free 
        Log l({0}, 8192, 8192);
        l.registerType(LOG_ENTRY_TYPE_OBJ,
                       livenessCallback, NULL,
                       relocationCallback, NULL,
                       timestampCallback);

        char alignedBuf[8192] __attribute__((aligned(8192)));
        SegmentEntryHandle seh;

        char buf[64];
        for (unsigned int i = 0; i < sizeof(buf); i++)
            buf[i] = static_cast<char>(i);

        reinterpret_cast<Object*>(buf)->timestamp = 0xf00f1234U;

        Segment s(112233, 445566, alignedBuf, sizeof(alignedBuf));
        *const_cast<Log**>(&s.log) = &l;
        seh = s.forceAppendWithEntry(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        CPPUNIT_ASSERT(seh != NULL);

        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_OBJ, seh->type());
        CPPUNIT_ASSERT_EQUAL(sizeof(buf), seh->length());
        CPPUNIT_ASSERT_EQUAL(0, memcmp(buf, seh->userData(), sizeof(buf)));
        CPPUNIT_ASSERT_EQUAL(s.spaceTimeSum, seh->totalLength() * 0xf00f1234UL);

        s.tail = downCast<uint32_t>(s.capacity - sizeof(SegmentEntry) -
                                    sizeof(buf) + 1);
        seh = s.forceAppendWithEntry(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        CPPUNIT_ASSERT_EQUAL(NULL, seh);

        s.tail--;
        seh = s.forceAppendWithEntry(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        CPPUNIT_ASSERT(seh != NULL);
        CPPUNIT_ASSERT_EQUAL(s.spaceTimeSum,
            (seh->totalLength() * 0xf00f1234UL) * 2);
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
        s.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header), false);
        CPPUNIT_ASSERT_EQUAL("", TestLog::get());
        s.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header), true);
        CPPUNIT_ASSERT_EQUAL("write: 1, 2, 90, 0",
                             TestLog::get());
    }

    void
    test_getSegmentBaseAddress()
    {
        CPPUNIT_ASSERT_EQUAL(128,
            reinterpret_cast<uintptr_t>(Segment::getSegmentBaseAddress(
            reinterpret_cast<void *>(128), 128)));
        CPPUNIT_ASSERT_EQUAL(128,
            reinterpret_cast<uintptr_t>(Segment::getSegmentBaseAddress(
            reinterpret_cast<void *>(129), 128)));
        CPPUNIT_ASSERT_EQUAL(128,
            reinterpret_cast<uintptr_t>(Segment::getSegmentBaseAddress(
            reinterpret_cast<void *>(255), 128)));
        CPPUNIT_ASSERT_EQUAL(256,
            reinterpret_cast<uintptr_t>(Segment::getSegmentBaseAddress(
            reinterpret_cast<void *>(256), 128)));
        CPPUNIT_ASSERT_EQUAL(8 * 1024 * 1024,
            reinterpret_cast<uintptr_t>(Segment::getSegmentBaseAddress(
            reinterpret_cast<void *>(8 * 1024 * 1024 + 237), 8 * 1024 * 1024)));
    }

};
CPPUNIT_TEST_SUITE_REGISTRATION(SegmentTest);

} // namespace RAMCloud
