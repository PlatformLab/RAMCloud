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
#include "ReplicaManager.h"

namespace RAMCloud {

/**
 * Unit tests for Segment.
 */
class SegmentTest : public ::testing::Test {
  public:
    ServerId serverId;
    ServerList serverList;

    SegmentTest()
        : serverId(1, 0)
        , serverList()
    {
        Context::get().logger->setLogLevels(SILENT_LOG_LEVEL);
    }

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
    freeFilter(string s)
    {
        return s == "free";
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

    DISALLOW_COPY_AND_ASSIGN(SegmentTest);
};

TEST_F(SegmentTest, constructor) {
    char alignedBuf[8192] __attribute__((aligned(8192)));

    ServerId serverId(1020304050, 0);
    ReplicaManager replicaManager(serverList, serverId, 0, NULL);
    TestLog::Enable _(&openSegmentFilter);
    Segment s(1020304050, 98765, alignedBuf, sizeof(alignedBuf),
              &replicaManager);
    EXPECT_EQ("openSegment: "
                            "openSegment 1020304050, 98765, ..., 38",
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
    ServerId serverId2(42, 0);
    Log l(serverId2, 8192, 8192, 4298, NULL, Log::CLEANER_DISABLED);
    Segment s2(&l, true, 0, alignedBuf, sizeof(alignedBuf), NULL,
                LOG_ENTRY_TYPE_INVALID, NULL, 0);
    EXPECT_EQ(s2.getLiveBytes(), s2.tail);

    // Segments must be power-of-two sized <= 2GB and be
    // aligned to their capacity.

    EXPECT_THROW(Segment(&l, true, 0, alignedBuf, sizeof(alignedBuf) + 1,
        NULL, LOG_ENTRY_TYPE_INVALID, NULL, 0), SegmentException);
    EXPECT_THROW(Segment(&l, true, 0, alignedBuf, 0x80000001,
        NULL, LOG_ENTRY_TYPE_INVALID, NULL, 0), SegmentException);

    EXPECT_THROW(Segment(&l, true, 0, &alignedBuf[1], sizeof(alignedBuf),
        NULL, LOG_ENTRY_TYPE_INVALID, NULL, 0), SegmentException);
    EXPECT_THROW(Segment(&l, true, 0, &alignedBuf[8], sizeof(alignedBuf),
        NULL, LOG_ENTRY_TYPE_INVALID, NULL, 0), SegmentException);
}

TEST_F(SegmentTest, append) {
    char alignedBuf[8192] __attribute__((aligned(8192)));
    SegmentEntryHandle seh;

    ServerId serverId(1, 0);
    ReplicaManager replicaManager(serverList, serverId, 0, NULL);
    TestLog::Enable _;
    Segment s(1, 2, alignedBuf, sizeof(alignedBuf), &replicaManager);
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
        "openSegment: openSegment 1, 2, ..., 38",
        TestLog::get());

    char c = '!';
    seh = s.append(LOG_ENTRY_TYPE_OBJ, &c, sizeof(c));
    EXPECT_TRUE(seh != NULL);
    EXPECT_EQ(sizeof(c) + sizeof(SegmentEntry),
        seh->totalLength());
    EXPECT_EQ(sizeof(SegmentEntry) + sizeof(SegmentHeader),
        seh->logPosition().segmentOffset());

    // ensure the checksum argument works
    EXPECT_THROW(s.append(LOG_ENTRY_TYPE_OBJ, &c, 1, true, 5),
                 SegmentException);

    uint32_t bytes = downCast<uint32_t>(
        s.appendableBytes() - sizeof(SegmentEntry));
    char buf[bytes];
    for (uint32_t i = 0; i < bytes; i++)
        buf[i] = static_cast<char>(i);

    SegmentEntry entry(LOG_ENTRY_TYPE_OBJ, bytes);
    SegmentChecksum expectedChecksum;
    expectedChecksum.update(&entry, sizeof(entry));
    expectedChecksum.update(buf, bytes);

    seh = s.append(LOG_ENTRY_TYPE_OBJ, buf, bytes, true,
                   expectedChecksum.getResult());
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
    Log l(serverId, 8192, 8192, 4298, NULL, Log::CLEANER_DISABLED);
    l.registerType(LOG_ENTRY_TYPE_OBJ,
                   true,
                   livenessCallback, NULL,
                   relocationCallback, NULL,
                   timestampCallback);

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
    Log l(serverId, 8192, 8192, 4298, NULL, Log::CLEANER_DISABLED);
    l.registerType(LOG_ENTRY_TYPE_OBJ,
                   true,
                   livenessCallback, NULL,
                   relocationCallback, NULL,
                   timestampCallback);

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

    ServerId serverId(1, 0);
    ReplicaManager replicaManager(serverList, serverId, 0, NULL);
    Segment s(1, 2, alignedBuf, sizeof(alignedBuf), &replicaManager);
    TestLog::Enable _;
    s.close(NULL, false);
    EXPECT_EQ("write: 1, 2, 8192 | close: 1, 2, 0 | "
              "close: Segment 2 closed (length 8192)",
              TestLog::get());

    // Check the padding.
    SegmentEntry *se = reinterpret_cast<SegmentEntry *>(
                        reinterpret_cast<char *>(s.baseAddress) +
                        sizeof(SegmentEntry) + sizeof(SegmentHeader));
    EXPECT_EQ(LOG_ENTRY_TYPE_INVALID, se->type);
    EXPECT_EQ(sizeof(alignedBuf) -
              3 * sizeof(SegmentEntry) -
              sizeof(SegmentHeader) -
              sizeof(SegmentFooter), se->length);

    // Check the footer.
    se = reinterpret_cast<SegmentEntry *>(
                        reinterpret_cast<char *>(s.baseAddress) +
                        sizeof(alignedBuf) -
                        sizeof(SegmentEntry) - sizeof(SegmentFooter));
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
    EXPECT_EQ(sizeof(alignedBuf) - 3 * sizeof(SegmentEntry) -
        sizeof(SegmentHeader) - sizeof(SegmentFooter), s.appendableBytes());

    static char buf[83];
    int i = 0;
    while (s.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf)))
        i++;
    EXPECT_EQ(87, i);
    EXPECT_EQ(39U, s.appendableBytes());

    s.close(NULL);
    EXPECT_EQ(0U, s.appendableBytes());
}

TEST_F(SegmentTest, forceAppendBlob) {
    char alignedBuf[8192] __attribute__((aligned(8192)));

    char buf[64];
    for (unsigned int i = 0; i < sizeof(buf); i++)
        buf[i] = static_cast<char>(i);

    Log l(serverId, 8192, 8192, 4298, NULL, Log::CLEANER_DISABLED);
    Segment s(&l, true, 445566, alignedBuf, sizeof(alignedBuf), NULL,
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

TEST_F(SegmentTest, forceAppendRepeatedByte) {
    char alignedBuf[8192] __attribute__((aligned(8192)));

    Log l(serverId, 8192, 8192, 4298, NULL, Log::CLEANER_DISABLED);
    Segment s(&l, true, 445566, alignedBuf, sizeof(alignedBuf), NULL,
                LOG_ENTRY_TYPE_INVALID, NULL, 0);
    uint64_t bytesBeforeAppend = s.getLiveBytes();


    char expectedByte = 't';
    char expectedBytes[100];
    memset(expectedBytes, expectedByte, sizeof(expectedBytes));
    s.forceAppendRepeatedByte(expectedByte, sizeof(expectedBytes));
    EXPECT_EQ(0, memcmp(expectedBytes, reinterpret_cast<char *>(
        s.baseAddress) + sizeof(SegmentEntry) + sizeof(SegmentHeader),
        sizeof(expectedBytes)));
    EXPECT_EQ(sizeof(expectedBytes) + sizeof(SegmentEntry) +
        sizeof(SegmentHeader), s.tail);
    EXPECT_EQ(bytesBeforeAppend + sizeof(expectedBytes),
        s.getLiveBytes());
}

TEST_F(SegmentTest, forceAppendWithEntry) {
    // create a fake log; see comment in test_free
    Log l(serverId, 8192, 8192, 4298, NULL, Log::CLEANER_DISABLED);
    l.registerType(LOG_ENTRY_TYPE_OBJ,
                   true,
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

    uint32_t oldTail = s.tail;
    seh = s.forceAppendWithEntry(LOG_ENTRY_TYPE_INVALID,
                                 NULL, 10, false, false);
    char expected[10] = {};
    EXPECT_TRUE(NULL != seh);
    EXPECT_EQ(0, memcmp(expected, seh->userData(), seh->length()));
    EXPECT_EQ(s.spaceTimeSum, 0ul);
    EXPECT_EQ(1u, s.entryCountsByType[LOG_ENTRY_TYPE_INVALID]);
    s.tail = oldTail;

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
    ReplicaManager replicaManager(serverList, serverId, 0, NULL);
    Segment s(1, 2, alignedBuf, sizeof(alignedBuf), &replicaManager);
    static SegmentHeader header;
    TestLog::Enable _;
    s.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header), false);
    EXPECT_EQ("", TestLog::get());
    s.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header), true);
    EXPECT_EQ("write: 1, 2, 114 | sync: syncing",
              TestLog::get());
}

TEST_F(SegmentTest, freeReplicas) {
    TestLog::Enable _(&freeFilter);
    char alignedBuf[8192] __attribute__((aligned(8192)));
    ReplicaManager replicaManager(serverList, serverId, 0, NULL);
    Segment s(1, 2, alignedBuf, sizeof(alignedBuf), &replicaManager);
    s.close(NULL);
    s.freeReplicas();
    EXPECT_EQ("free: 1, 2", TestLog::get());
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
    EXPECT_EQ(1U,
        Segment::maximumSegmentsNeededForEntries(6,
                                                 6 * 1024 * 1024,
                                                 1 * 1024 * 1024,
                                                 8 * 1024 * 1024));
    EXPECT_EQ(2U,
        Segment::maximumSegmentsNeededForEntries(7,
                                                 7 * 1024 * 1024,
                                                 1 * 1024 * 1024,
                                                 8 * 1024 * 1024));
    EXPECT_EQ(2U,
        Segment::maximumSegmentsNeededForEntries(8,
                                                 8 * 1024 * 1024,
                                                 1 * 1024 * 1024,
                                                 8 * 1024 * 1024));
    EXPECT_EQ(2U,
        Segment::maximumSegmentsNeededForEntries(9,
                                                 9 * 1024 * 1024,
                                                 1 * 1024 * 1024,
                                                 8 * 1024 * 1024));
    EXPECT_EQ(3U,
        Segment::maximumSegmentsNeededForEntries(14,
                                                 14 * 1024 * 1024,
                                                  1 * 1024 * 1024,
                                                  8 * 1024 * 1024));
    EXPECT_EQ(101U,
        Segment::maximumSegmentsNeededForEntries(700,
                                                 700 * 1024 * 1024,
                                                   1 * 1024 * 1024,
                                                   8 * 1024 * 1024));
}
} // namespace RAMCloud
