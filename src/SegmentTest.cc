/* Copyright (c) 2010-2011 Stanford University
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
class SegmentTest : public ::testing::Test {
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

    static void
    scanCallback(LogEntryHandle h,
                 void* cookie)
    {
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(SegmentTest);
};

TEST_F(SegmentTest, constructor) {
    char alignedBuf[8192] __attribute__((aligned(8192)));

    Tub<uint64_t> serverId;
    serverId.construct(1020304050);
    BackupManager backup(NULL, serverId, 0);
    TestLog::Enable _(&openSegmentFilter);
    Segment s(1020304050, 98765, alignedBuf, sizeof(alignedBuf), &backup);
    EXPECT_EQ("openSegment: "
                            "openSegment 1020304050, 98765, ..., 30",
                            TestLog::get());
    EXPECT_EQ(s.baseAddress,
                            reinterpret_cast<void *>(alignedBuf));
    EXPECT_EQ(98765U, s.id);
    EXPECT_EQ(8192U, s.capacity);
    EXPECT_EQ(sizeof(SegmentEntry) + sizeof(SegmentHeader),
        s.tail);
    EXPECT_EQ(0UL, s.bytesExplicitlyFreed);
    EXPECT_EQ(0UL, s.bytesImplicitlyFreed);
    EXPECT_EQ(0UL, s.spaceTimeSum);
    EXPECT_EQ(0UL, s.implicitlyFreedSpaceTimeSum);
    EXPECT_FALSE(s.canRollBack);
    EXPECT_FALSE(s.closed);

    SegmentEntry *se = reinterpret_cast<SegmentEntry *>(s.baseAddress);
    EXPECT_EQ(LOG_ENTRY_TYPE_SEGHEADER, se->type);
    EXPECT_EQ(sizeof(SegmentHeader), se->length);

    SegmentHeader *sh = reinterpret_cast<SegmentHeader *>(
                        reinterpret_cast<char *>(se) + sizeof(*se));
    EXPECT_EQ(1020304050U, sh->logId);
    EXPECT_EQ(98765U, sh->segmentId);
    EXPECT_EQ(sizeof(alignedBuf), sh->segmentCapacity);

    // be sure we count the header written in the LogStats
    Tub<uint64_t> serverId2;
    serverId2.construct(0);
    Log l(serverId2, 8192, 8192, 4298);
    Segment s2(&l, 0, alignedBuf, sizeof(alignedBuf), NULL,
                LOG_ENTRY_TYPE_INVALID, NULL, 0);
    EXPECT_EQ(s2.getLiveBytes(), s2.tail);

    // Segments must be power-of-two sized <= 2GB and be
    // aligned to their capacity.

    EXPECT_THROW(Segment(&l, 0, alignedBuf, sizeof(alignedBuf) + 1,
        NULL, LOG_ENTRY_TYPE_INVALID, NULL, 0), SegmentException);
    EXPECT_THROW(Segment(&l, 0, alignedBuf, 0x80000001,
        NULL, LOG_ENTRY_TYPE_INVALID, NULL, 0), SegmentException);

    EXPECT_THROW(Segment(&l, 0, &alignedBuf[1], sizeof(alignedBuf),
        NULL, LOG_ENTRY_TYPE_INVALID, NULL, 0), SegmentException);
    EXPECT_THROW(Segment(&l, 0, &alignedBuf[8], sizeof(alignedBuf),
        NULL, LOG_ENTRY_TYPE_INVALID, NULL, 0), SegmentException);
}

TEST_F(SegmentTest, destructor) {
    TestLog::Enable _(&freeSegmentFilter);
    {
        char alignedBuf[8192] __attribute__((aligned(8192)));
        Tub<uint64_t> serverId;
        serverId.construct(1);
        BackupManager backup(NULL, serverId, 0);
        Segment s(1, 2, alignedBuf, sizeof(alignedBuf), &backup);
    }
    EXPECT_EQ("freeSegment: 1, 2", TestLog::get());
}

TEST_F(SegmentTest, append) {
    char alignedBuf[8192] __attribute__((aligned(8192)));
    SegmentEntryHandle seh;

    Tub<uint64_t> serverId;
    serverId.construct(1);
    BackupManager backup(NULL, serverId, 0);
    TestLog::Enable _;
    Segment s(1, 2, alignedBuf, sizeof(alignedBuf), &backup);
    seh = s.append(LOG_ENTRY_TYPE_SEGFOOTER, NULL, 0);
    EXPECT_TRUE(NULL == seh);

    s.closed = true;
    seh = s.append(LOG_ENTRY_TYPE_OBJ, alignedBuf, 1);
    EXPECT_TRUE(NULL == seh);
    s.closed = false;

    seh = s.append(LOG_ENTRY_TYPE_OBJ, NULL,
                    s.appendableBytes() + 1);
    EXPECT_TRUE(NULL == seh);

    EXPECT_EQ(
        "openSegment: openSegment 1, 2, ..., 30",
        TestLog::get());

    char c = '!';
    seh = s.append(LOG_ENTRY_TYPE_OBJ, &c, sizeof(c));
    EXPECT_TRUE(seh != NULL);
    EXPECT_EQ(sizeof(c) + sizeof(SegmentEntry),
        seh->totalLength());
    EXPECT_EQ(sizeof(SegmentEntry) + sizeof(SegmentHeader),
        seh->logTime().second);

    // ensure the checksum argument works
    EXPECT_THROW(s.append(LOG_ENTRY_TYPE_OBJ, &c, 1, true, 5),
        SegmentException);

    uint32_t bytes = downCast<uint32_t>(
        s.appendableBytes() - sizeof(SegmentEntry));
    char buf[bytes];
    for (uint32_t i = 0; i < bytes; i++)
        buf[i] = static_cast<char>(i);

    seh = s.append(LOG_ENTRY_TYPE_OBJ, buf, bytes, true, 0x0597577e);
    EXPECT_TRUE(seh != NULL);

    SegmentEntry *se = reinterpret_cast<SegmentEntry *>(
                        reinterpret_cast<char *>(s.baseAddress) +
                        sizeof(SegmentEntry) + sizeof(SegmentHeader) +
                        sizeof(SegmentEntry) + sizeof(c));

    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, se->type);
    EXPECT_EQ(bytes, se->length);
    EXPECT_EQ(0,
        memcmp(buf, reinterpret_cast<char *>(se) + sizeof(*se), bytes));
}

TEST_F(SegmentTest, append_handle) {
    // Create one Segment, write an object, then write it to another
    // Segment.

    char alignedBuf[8192] __attribute__((aligned(8192)));
    Segment s(1, 2, alignedBuf, sizeof(alignedBuf), NULL);
    uint32_t object = 0xdeadbeef;
    SegmentEntryHandle seh;
    seh = s.append(LOG_ENTRY_TYPE_OBJ, &object, sizeof(object));
    EXPECT_TRUE(seh != NULL);

    // Alterior motive: choose a different Segment size to test
    // the checksum code / mutableFields nonsense.
    char alignedBuf2[16384] __attribute__((aligned(16384)));
    Segment s2(2, 3, alignedBuf2, sizeof(alignedBuf2), NULL);
    SegmentEntryHandle copySeh = s2.append(seh, false);
    EXPECT_TRUE(copySeh != NULL);

    EXPECT_EQ(seh->checksum(), copySeh->checksum());
    EXPECT_EQ(seh->generateChecksum(),
        copySeh->generateChecksum());
    EXPECT_EQ(0,
        memcmp(seh->userData(), copySeh->userData(), seh->length()));
}

TEST_F(SegmentTest, rollBack) {
    char alignedBuf[8192] __attribute__((aligned(8192)));
    Segment s(1, 2, alignedBuf, sizeof(alignedBuf), NULL);
    SegmentEntryHandle seh;

    uint32_t object = 0xcafebeef;
    seh = s.append(LOG_ENTRY_TYPE_OBJ, &object, sizeof(object), true);
    EXPECT_TRUE(seh != NULL);
    EXPECT_FALSE(s.canRollBack);
    EXPECT_THROW(s.rollBack(seh), SegmentException);

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
    EXPECT_TRUE(seh != NULL);
    EXPECT_TRUE(s.canRollBack);

    EXPECT_EQ(2U, s.entryCountsByType[LOG_ENTRY_TYPE_OBJ]);
    s.rollBack(seh);
    EXPECT_EQ(1U, s.entryCountsByType[LOG_ENTRY_TYPE_OBJ]);
    EXPECT_EQ(checksumPreAppend, s.checksum.getResult());
    memset(&s.prevChecksum, 0, sizeof(s.prevChecksum));
    EXPECT_EQ(0, memcmp(saveForComparison, &s, sizeof(Segment)));
}

TEST_F(SegmentTest, free) {
    // create a fake log so we can use the timestamp callback
    // (the Log-less segment constructor won't result in the
    // spaceTimeSum value being altered otherwise.
    Log l({0}, 8192, 8192, 4298);
    l.registerType(LOG_ENTRY_TYPE_OBJ,
                   true,
                   livenessCallback, NULL,
                   relocationCallback, NULL,
                   timestampCallback,
                   scanCallback, NULL);

    char alignedBuf[8192] __attribute__((aligned(8192)));
    char buf[64];

    reinterpret_cast<Object*>(buf)->timestamp = 0xf00f1234U;

    Segment s(1, 2, alignedBuf, sizeof(alignedBuf));
    *const_cast<Log**>(&s.log) = &l;
    SegmentEntryHandle h = s.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
    s.free(h);
    EXPECT_EQ(sizeof(buf) + sizeof(SegmentEntry), s.bytesExplicitlyFreed);
    EXPECT_EQ(0UL, s.spaceTimeSum);
}

TEST_F(SegmentTest, setImplicitlyFreedCounts) {
    Log l({0}, 8192, 8192, 4298);
    l.registerType(LOG_ENTRY_TYPE_OBJ,
                   true,
                   livenessCallback, NULL,
                   relocationCallback, NULL,
                   timestampCallback,
                   scanCallback, NULL);

    char alignedBuf[8192] __attribute__((aligned(8192)));
    char buf[64];

    reinterpret_cast<Object*>(buf)->timestamp = 7;

    Segment s(1, 2, alignedBuf, sizeof(alignedBuf));
    *const_cast<Log**>(&s.log) = &l;

    uint64_t avgBefore = s.getAverageTimestamp();
    LogEntryHandle h = s.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
    s.setImplicitlyFreedCounts(h->totalLength(), h->totalLength() * 7);
    EXPECT_EQ(h->totalLength(), s.bytesImplicitlyFreed);
    EXPECT_EQ(h->totalLength() * 7, s.implicitlyFreedSpaceTimeSum);
    EXPECT_EQ(avgBefore, s.getAverageTimestamp());

    // At some point I mistakenly thought that
    //    s1 * t1 + s2 * t2 + ... + sn * tn = Sigma s x Sigma t
    // Make sure this thinko doesn't reappear.
    reinterpret_cast<Object*>(buf)->timestamp = 14;
    LogEntryHandle h2 = s.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
    s.setImplicitlyFreedCounts(h->totalLength() + h2->totalLength(),
        h->totalLength() * 7 + h2->totalLength() * 14);
    EXPECT_EQ(avgBefore, s.getAverageTimestamp());
}

TEST_F(SegmentTest, close) {
    char alignedBuf[8192] __attribute__((aligned(8192)));

    Tub<uint64_t> serverId;
    serverId.construct(1);
    BackupManager backup(NULL, serverId, 0);
    Segment s(1, 2, alignedBuf, sizeof(alignedBuf), &backup);
    TestLog::Enable _;
    s.close();
    EXPECT_EQ("write: 1, 2, 44, 1 | "
                            "write: Segment 2 closed (length 44) | "
                            "proceedNoMetrics: Closed segment 1, 2",
                            TestLog::get());

    SegmentEntry *se = reinterpret_cast<SegmentEntry *>(
                        reinterpret_cast<char *>(s.baseAddress) +
                        sizeof(SegmentEntry) + sizeof(SegmentHeader));
    EXPECT_EQ(LOG_ENTRY_TYPE_SEGFOOTER, se->type);
    EXPECT_EQ(sizeof(SegmentFooter), se->length);

    SegmentFooter *sf = reinterpret_cast<SegmentFooter *>(
                        reinterpret_cast<char *>(se) + sizeof(*se));
    EXPECT_EQ(s.checksum.getResult(), sf->checksum);

    EXPECT_EQ(0U, s.appendableBytes());
    EXPECT_TRUE(s.closed);
}

// The following tests are not ordered with respect to the code,
// since the early abort line in Segment::appendableBytes() requires
// segment closure.
TEST_F(SegmentTest, appendableBytes) {
    char alignedBuf[8192] __attribute__((aligned(8192)));

    Segment s(1, 2, alignedBuf, sizeof(alignedBuf));
    EXPECT_EQ(sizeof(alignedBuf) - 2 * sizeof(SegmentEntry) -
        sizeof(SegmentHeader) - sizeof(SegmentFooter), s.appendableBytes());

    static char buf[83];
    int i = 0;
    while (s.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf)))
        i++;
    EXPECT_EQ(87, i);
    EXPECT_EQ(57U, s.appendableBytes());

    s.close();
    EXPECT_EQ(0U, s.appendableBytes());
}

TEST_F(SegmentTest, forceAppendBlob) {
    char alignedBuf[8192] __attribute__((aligned(8192)));

    char buf[64];
    for (unsigned int i = 0; i < sizeof(buf); i++)
        buf[i] = static_cast<char>(i);

    Tub<uint64_t> serverId;
    serverId.construct(0);
    Log l(serverId, 8192, 8192, 4298);
    Segment s(&l, 445566, alignedBuf, sizeof(alignedBuf), NULL,
                LOG_ENTRY_TYPE_INVALID, NULL, 0);
    uint64_t bytesBeforeAppend = s.getLiveBytes();
    s.forceAppendBlob(buf, sizeof(buf));
    EXPECT_EQ(0, memcmp(buf, reinterpret_cast<char *>(
        s.baseAddress) + sizeof(SegmentEntry) + sizeof(SegmentHeader),
        sizeof(buf)));
    EXPECT_EQ(sizeof(buf) + sizeof(SegmentEntry) +
        sizeof(SegmentHeader), s.tail);
    EXPECT_EQ(bytesBeforeAppend + sizeof(buf),
        s.getLiveBytes());
}

TEST_F(SegmentTest, forceAppendWithEntry) {
    // create a fake log; see comment in test_free
    Log l({0}, 8192, 8192, 4298);
    l.registerType(LOG_ENTRY_TYPE_OBJ,
                   true,
                   livenessCallback, NULL,
                   relocationCallback, NULL,
                   timestampCallback,
                   scanCallback, NULL);

    char alignedBuf[8192] __attribute__((aligned(8192)));
    SegmentEntryHandle seh;

    char buf[64];
    for (unsigned int i = 0; i < sizeof(buf); i++)
        buf[i] = static_cast<char>(i);

    reinterpret_cast<Object*>(buf)->timestamp = 0xf00f1234U;

    Segment s(112233, 445566, alignedBuf, sizeof(alignedBuf));
    *const_cast<Log**>(&s.log) = &l;
    seh = s.forceAppendWithEntry(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
    EXPECT_TRUE(seh != NULL);

    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, seh->type());
    EXPECT_EQ(sizeof(buf), seh->length());
    EXPECT_EQ(0, memcmp(buf, seh->userData(), sizeof(buf)));
    EXPECT_EQ(s.spaceTimeSum, seh->totalLength() * 0xf00f1234UL);

    s.tail = downCast<uint32_t>(s.capacity - sizeof(SegmentEntry) -
                                sizeof(buf) + 1);
    seh = s.forceAppendWithEntry(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
    EXPECT_TRUE(NULL == seh);

    s.tail--;
    seh = s.forceAppendWithEntry(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
    EXPECT_TRUE(seh != NULL);
    EXPECT_EQ(s.spaceTimeSum,
        (seh->totalLength() * 0xf00f1234UL) * 2);
    EXPECT_EQ(2U, s.entryCountsByType[LOG_ENTRY_TYPE_OBJ]);
}

TEST_F(SegmentTest, syncToBackup) {
    char alignedBuf[8192] __attribute__((aligned(8192)));
    Tub<uint64_t> serverId;
    serverId.construct(1);
    BackupManager backup(NULL, serverId, 0);
    Segment s(1, 2, alignedBuf, sizeof(alignedBuf), &backup);
    static SegmentHeader header;
    TestLog::Enable _;
    s.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header), false);
    EXPECT_EQ("", TestLog::get());
    s.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header), true);
    EXPECT_EQ("write: 1, 2, 90, 0",
                            TestLog::get());
}

TEST_F(SegmentTest, getSegmentBaseAddress) {
    EXPECT_EQ(128U,
        reinterpret_cast<uintptr_t>(Segment::getSegmentBaseAddress(
        reinterpret_cast<void *>(128), 128)));
    EXPECT_EQ(128U,
        reinterpret_cast<uintptr_t>(Segment::getSegmentBaseAddress(
        reinterpret_cast<void *>(129), 128)));
    EXPECT_EQ(128U,
        reinterpret_cast<uintptr_t>(Segment::getSegmentBaseAddress(
        reinterpret_cast<void *>(255), 128)));
    EXPECT_EQ(256U,
        reinterpret_cast<uintptr_t>(Segment::getSegmentBaseAddress(
        reinterpret_cast<void *>(256), 128)));
    EXPECT_EQ(8U * 1024U * 1024U,
        reinterpret_cast<uintptr_t>(Segment::getSegmentBaseAddress(
        reinterpret_cast<void *>(8 * 1024 * 1024 + 237), 8 * 1024 * 1024)));
}

TEST_F(SegmentTest, maximumSegmentsNeededForEntries) {
    EXPECT_EQ(1,
        Segment::maximumSegmentsNeededForEntries(6,
                                                 6 * 1024 * 1024,
                                                 1 * 1024 * 1024,
                                                 8 * 1024 * 1024));
    EXPECT_EQ(2,
        Segment::maximumSegmentsNeededForEntries(7,
                                                 7 * 1024 * 1024,
                                                 1 * 1024 * 1024,
                                                 8 * 1024 * 1024));
    EXPECT_EQ(2,
        Segment::maximumSegmentsNeededForEntries(8,
                                                 8 * 1024 * 1024,
                                                 1 * 1024 * 1024,
                                                 8 * 1024 * 1024));
    EXPECT_EQ(2,
        Segment::maximumSegmentsNeededForEntries(9,
                                                 9 * 1024 * 1024,
                                                 1 * 1024 * 1024,
                                                 8 * 1024 * 1024));
    EXPECT_EQ(3,
        Segment::maximumSegmentsNeededForEntries(14,
                                                 14 * 1024 * 1024,
                                                  1 * 1024 * 1024,
                                                  8 * 1024 * 1024));
    EXPECT_EQ(101,
        Segment::maximumSegmentsNeededForEntries(700,
                                                 700 * 1024 * 1024,
                                                   1 * 1024 * 1024,
                                                   8 * 1024 * 1024));
}

} // namespace RAMCloud
