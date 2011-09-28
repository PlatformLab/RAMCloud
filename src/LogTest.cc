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
#include "ServerRpcPool.h"
#include "Log.h"
#include "LogTypes.h"
#include "Memory.h"
#include "Transport.h"

namespace RAMCloud {

/**
 * Unit tests for Log.
 */
class LogTest : public ::testing::Test {
  public:
    LogTest() {}

  private:
    DISALLOW_COPY_AND_ASSIGN(LogTest);
};

TEST_F(LogTest, constructor) {
    Tub<uint64_t> serverId;
    serverId.construct(57);
    Log l(serverId, 2 * 8192, 8192, 4298);

    EXPECT_EQ(57U, *l.logId);
    EXPECT_EQ(2 * 8192U, l.logCapacity);
    EXPECT_EQ(8192U, l.segmentCapacity);
    EXPECT_EQ(4298U, l.maximumBytesPerAppend);
    EXPECT_EQ(2U, l.freeList.size());
    EXPECT_EQ(0U, l.cleanableNewList.size());
    EXPECT_EQ(0U, l.cleanablePendingDigestList.size());
    EXPECT_EQ(0U, l.freePendingDigestAndReferenceList.size());
    EXPECT_EQ(0U, l.freePendingReferenceList.size());
    EXPECT_EQ(0U, l.nextSegmentId);
    EXPECT_TRUE(NULL == l.head);
    EXPECT_EQ(Log::CONCURRENT_CLEANER, l.cleanerOption);

    Log l2(serverId, 2 * 8192, 8192, 4298, NULL, Log::CLEANER_DISABLED);
    EXPECT_EQ(Log::CLEANER_DISABLED, l2.cleanerOption);

    EXPECT_THROW(new Log(serverId, 8192, 8192, 8193),
        LogException);
}

TEST_F(LogTest, allocateHead_basics) {
    Tub<uint64_t> serverId;
    serverId.construct(57);
    Log l(serverId, 3 * 8192, 8192, 4298);

    {
        l.allocateHead();
        Segment* s = l.head;
        EXPECT_TRUE(s != NULL);
        const SegmentEntry *se = reinterpret_cast<const SegmentEntry*>(
            (const char *)s->getBaseAddress() + sizeof(SegmentEntry) +
            sizeof(SegmentHeader));
        const void* ldp = (const char *)s->getBaseAddress() +
            sizeof(SegmentEntry) * 2 + sizeof(SegmentHeader);
        LogDigest ld(const_cast<void*>(ldp),
            LogDigest::getBytesFromCount(1));
        EXPECT_EQ(LOG_ENTRY_TYPE_LOGDIGEST, se->type);
        EXPECT_EQ(LogDigest::getBytesFromCount(1), se->length);
        EXPECT_EQ(1, ld.getSegmentCount());
        EXPECT_EQ(s->getId(), ld.getSegmentIds()[0]);
        EXPECT_EQ(s, l.activeIdMap[s->getId()]);
        EXPECT_EQ(s,
            l.activeBaseAddressMap[s->getBaseAddress()]);
    }

    {
        Segment* oldHead = l.head;
        l.allocateHead();
        Segment* s = l.head;
        EXPECT_TRUE(s != NULL);
        const SegmentEntry *se = reinterpret_cast<const SegmentEntry*>(
            (const char *)s->getBaseAddress() + sizeof(SegmentEntry) +
            sizeof(SegmentHeader));
        const void* ldp = (const char *)s->getBaseAddress() +
            sizeof(SegmentEntry) * 2 + sizeof(SegmentHeader);
        LogDigest ld(const_cast<void*>(ldp),
            LogDigest::getBytesFromCount(2));
        EXPECT_EQ(LOG_ENTRY_TYPE_LOGDIGEST, se->type);
        EXPECT_EQ(LogDigest::getBytesFromCount(2), se->length);
        EXPECT_EQ(2, ld.getSegmentCount());
        EXPECT_EQ(s->getId(), ld.getSegmentIds()[1]);

        EXPECT_THROW(oldHead->close(), SegmentException);
        EXPECT_TRUE(s != oldHead);
    }

    EXPECT_THROW(l.allocateHead(), LogOutOfMemoryException);
}

TEST_F(LogTest, allocateHead_lists) {
    Tub<uint64_t> serverId;
    serverId.construct(57);
    Log l(serverId, 5 * 8192, 8192, 4298, NULL, Log::CLEANER_DISABLED);

    Segment* cleaned = new Segment(&l, l.allocateSegmentId(),
        l.getFromFreeList(false), 8192, NULL, LOG_ENTRY_TYPE_UNINIT,
        NULL, 0);
    l.cleanablePendingDigestList.push_back(*cleaned);

    Segment* cleanableNew = new Segment(&l, l.allocateSegmentId(),
        l.getFromFreeList(false), 8192, NULL, LOG_ENTRY_TYPE_UNINIT,
        NULL, 0);
    l.cleanableNewList.push_back(*cleanableNew);

    Segment* cleanable = new Segment(&l, l.allocateSegmentId(),
        l.getFromFreeList(false), 8192, NULL, LOG_ENTRY_TYPE_UNINIT,
        NULL, 0);
    l.cleanableList.push_back(*cleanable);

    Segment* freePending = new Segment(&l, l.allocateSegmentId(),
        l.getFromFreeList(false), 8192, NULL, LOG_ENTRY_TYPE_UNINIT,
        NULL, 0);
    l.freePendingDigestAndReferenceList.push_back(*freePending);

    l.allocateHead();

    EXPECT_EQ(0U, l.cleanablePendingDigestList.size());
    EXPECT_EQ(2U, l.cleanableNewList.size());
    EXPECT_EQ(1U, l.cleanableList.size());
    EXPECT_EQ(0U, l.freePendingDigestAndReferenceList.size());
    EXPECT_EQ(1U, l.freePendingReferenceList.size());

    const SegmentEntry *se = reinterpret_cast<const SegmentEntry*>(
        (const char *)l.head->getBaseAddress() + sizeof(SegmentEntry) +
        sizeof(SegmentHeader));
    EXPECT_EQ(LOG_ENTRY_TYPE_LOGDIGEST, se->type);
    EXPECT_EQ(LogDigest::getBytesFromCount(4), se->length);

    // Segments allocated above are deallocated in the Log destructor.
}

TEST_F(LogTest, addSegmentMemory) {
    Tub<uint64_t> serverId;
    serverId.construct(57);
    Log l(serverId, 1 * 8192, 8192, 4298);

    void *p = Memory::xmemalign(HERE, l.segmentCapacity, l.segmentCapacity);
    l.addSegmentMemory(p);
    Segment s((uint64_t)0, 0, p, 8192);

    EXPECT_EQ(2U, l.freeList.size());
    EXPECT_EQ(p, l.freeList[1]);
}

TEST_F(LogTest, getFromFreeList) {
    Tub<uint64_t> serverId;
    serverId.construct(57);
    Log l(serverId, 2 * 8192, 8192, 4298);

    // Grab a segment and reduce from 2 to 1 free segments.
    void* seg2 = l.getFromFreeList(true);
    EXPECT_TRUE(seg2 != NULL);

    // Shouldn't be able to get the last one regardless of the
    // parameter (only one seg left and cleanablePendingDigestList
    // is empty).
    EXPECT_THROW(l.getFromFreeList(false), LogOutOfMemoryException);
    EXPECT_THROW(l.getFromFreeList(true), LogOutOfMemoryException);

    // Having a Segment on cleanablePendingDigestList should
    // alter the behaviour.
    Segment* s = new Segment(5, 5, seg2, 8192, l.backup);
    l.cleanablePendingDigestList.push_back(*s);

    EXPECT_THROW(l.getFromFreeList(false), LogOutOfMemoryException);
    EXPECT_TRUE(l.getFromFreeList(true) != NULL);
    EXPECT_EQ(0U, l.freeList.size());

    // Now the free list is totally empty.
    EXPECT_THROW(l.getFromFreeList(false), LogOutOfMemoryException);
    EXPECT_THROW(l.getFromFreeList(true), LogOutOfMemoryException);
}

TEST_F(LogTest, isSegmentLive) {
    Tub<uint64_t> serverId;
    serverId.construct(57);
    Log l(serverId, 2 * 8192, 8192, 4298);
    l.registerType(LOG_ENTRY_TYPE_OBJ, true, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL);
    static char buf[64];

    uint64_t segmentId = l.nextSegmentId;
    EXPECT_FALSE(l.isSegmentLive(segmentId));
    l.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
    EXPECT_TRUE(l.isSegmentLive(segmentId));
}

TEST_F(LogTest, getSegmentId) {
    Tub<uint64_t> serverId;
    serverId.construct(57);
    Log l(serverId, 2 * 8192, 8192, 4298);
    l.registerType(LOG_ENTRY_TYPE_OBJ, true, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL);
    static char buf[64];

    const void *p = l.append(LOG_ENTRY_TYPE_OBJ,
        buf, sizeof(buf))->userData();
    EXPECT_EQ(0U, l.getSegmentId(p));
    EXPECT_THROW(l.getSegmentId(
        reinterpret_cast<const char *>(p) + 8192), LogException);
}

TEST_F(LogTest, append) {
    Tub<uint64_t> serverId;
    serverId.construct(57);
    Log l(serverId, 2 * 8192, 8192, 8138, NULL, Log::CLEANER_DISABLED);
    l.registerType(LOG_ENTRY_TYPE_OBJ, true, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL);
    static char buf[13];
    char fillbuf[l.getSegmentCapacity()];
    memset(fillbuf, 'A', sizeof(fillbuf));

    EXPECT_TRUE(l.head == NULL);
    EXPECT_EQ(2U, l.freeList.size());

    // exercise head == NULL path
    SegmentEntryHandle seh = l.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
    EXPECT_TRUE(seh != NULL);
    EXPECT_EQ(sizeof(SegmentEntry) + sizeof(buf),
        seh->totalLength());
    EXPECT_EQ(0, memcmp(buf, seh->userData(), sizeof(buf)));
    EXPECT_TRUE(LogTime(0,
        sizeof(SegmentEntry) + sizeof(SegmentHeader) + sizeof(SegmentEntry)
        + LogDigest::getBytesFromCount(1)) == seh->logTime());
    EXPECT_TRUE(l.activeIdMap.find(l.head->getId()) !=
        l.activeIdMap.end());
    EXPECT_TRUE(l.activeBaseAddressMap.find(l.head->getBaseAddress()) !=
        l.activeBaseAddressMap.end());
    EXPECT_EQ(1U, l.freeList.size());

    // assert that the LogDigest is written out correctly
    const void* ldp = (const char *)l.head->getBaseAddress() +
        sizeof(SegmentEntry) * 2 + sizeof(SegmentHeader);
    LogDigest ld(const_cast<void*>(ldp), LogDigest::getBytesFromCount(1));
    EXPECT_EQ(1, ld.getSegmentCount());
    EXPECT_EQ(l.head->getId(), ld.getSegmentIds()[0]);

    // exercise head != NULL, but too few bytes (new head) path
    Segment *oldHead = l.head;
    seh = l.append(LOG_ENTRY_TYPE_OBJ, fillbuf,
        l.head->appendableBytes() - downCast<uint32_t>(sizeof(SegmentEntry)));
    EXPECT_TRUE(seh != NULL);
    EXPECT_EQ(oldHead, l.head);
    EXPECT_EQ(0U, l.head->appendableBytes());
    seh = l.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf), NULL);
    EXPECT_TRUE(seh != NULL);
    EXPECT_TRUE(oldHead != l.head);

    // execise regular head != NULL path
    LogTime logTime = seh->logTime();
    LogTime nextTime;
    seh = l.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf), NULL);
    EXPECT_TRUE(seh != NULL);
    EXPECT_TRUE(seh->logTime() > logTime);

    EXPECT_EQ(4U, l.stats.totalAppends);

    // fill the log and get an exception. we should be on the 3rd Segment
    // now.
    EXPECT_EQ(0U, l.freeList.size());
    seh = l.append(LOG_ENTRY_TYPE_OBJ, fillbuf,
        l.head->appendableBytes() - downCast<uint32_t>(sizeof(SegmentEntry)));
    EXPECT_TRUE(seh != NULL);
    EXPECT_THROW(l.append(LOG_ENTRY_TYPE_OBJ, buf, 1),
        LogOutOfMemoryException);
}

TEST_F(LogTest, free) {
    Tub<uint64_t> serverId;
    serverId.construct(57);
    Log l(serverId, 2 * 8192, 8192, 4298);
    l.registerType(LOG_ENTRY_TYPE_OBJ, true, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL);
    static char buf[64];

    LogEntryHandle h = l.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
    l.free(h);
    Segment *s = l.head;
    EXPECT_EQ(sizeof(buf) + sizeof(SegmentEntry), s->bytesExplicitlyFreed);

    EXPECT_THROW(l.free(LogEntryHandle(NULL)), LogException);
}

static bool
livenessCallback(LogEntryHandle handle, void* cookie)
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
timestampCallback(LogEntryHandle handle)
{
    return 57;
}

static void
scanCallback(LogEntryHandle handle,
                void* cookie)
{
}

TEST_F(LogTest, registerType) {
    Tub<uint64_t> serverId;
    serverId.construct(57);
    Log l(serverId, 1 * 8192, 8192, 4298);

    l.registerType(LOG_ENTRY_TYPE_OBJ,
                   true,
                   livenessCallback, NULL,
                   relocationCallback, NULL,
                   timestampCallback,
                   scanCallback, NULL);
    EXPECT_THROW(
        l.registerType(LOG_ENTRY_TYPE_OBJ,
                       true,
                       livenessCallback, NULL,
                       relocationCallback, NULL,
                       timestampCallback,
                       scanCallback, NULL),
        LogException);
    EXPECT_THROW(
        l.registerType(LOG_ENTRY_TYPE_OBJTOMB,
                       false,
                       NULL, NULL,
                       relocationCallback, NULL,
                       timestampCallback,
                       scanCallback, NULL),
        LogException);

    LogTypeInfo *cb = l.logTypeMap[LOG_ENTRY_TYPE_OBJ];
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, cb->type);
    EXPECT_TRUE(cb->explicitlyFreed);
    EXPECT_EQ(reinterpret_cast<void *>(livenessCallback),
              reinterpret_cast<void *>(cb->livenessCB));
    EXPECT_TRUE(NULL == cb->livenessArg);
    EXPECT_EQ(reinterpret_cast<void *>(relocationCallback),
              reinterpret_cast<void *>(cb->relocationCB));
    EXPECT_TRUE(NULL == cb->relocationArg);
    EXPECT_EQ(reinterpret_cast<void *>(timestampCallback),
              reinterpret_cast<void *>(cb->timestampCB));
}

TEST_F(LogTest, getTypeInfo) {
    Tub<uint64_t> serverId;
    serverId.construct(57);
    Log l(serverId, 1 * 8192, 8192, 4298);

    l.registerType(LOG_ENTRY_TYPE_OBJ,
                   true,
                   livenessCallback, NULL,
                   relocationCallback, NULL,
                   timestampCallback,
                   scanCallback, NULL);

    const LogTypeInfo* cb = l.getTypeInfo(LOG_ENTRY_TYPE_OBJ);
    EXPECT_TRUE(cb != NULL);
    EXPECT_EQ(reinterpret_cast<void*>(livenessCallback),
              reinterpret_cast<void*>(cb->livenessCB));

    EXPECT_TRUE(NULL == l.getTypeInfo(LOG_ENTRY_TYPE_OBJTOMB));
}

TEST_F(LogTest, getNewCleanableSegments) {
    Tub<uint64_t> serverId;
    serverId.construct(57);
    Log l(serverId, 2 * 8192, 8192, 4298, NULL, Log::CLEANER_DISABLED);

    mockWallTimeValue = 1;

    SegmentVector out;
    l.getNewCleanableSegments(out);
    EXPECT_EQ(0U, out.size());

    Segment* cleanableNew = new Segment(&l, l.allocateSegmentId(),
        l.getFromFreeList(false), 8192, NULL, LOG_ENTRY_TYPE_UNINIT,
        NULL, 0);

    l.cleanableNewList.push_back(*cleanableNew);

    EXPECT_EQ(1U, l.cleanableNewList.size());
    EXPECT_EQ(0U, l.cleanableList.size());

    mockWallTimeValue = 9999;
    l.getNewCleanableSegments(out);
    EXPECT_EQ(1U, out.size());

    EXPECT_EQ(0U, l.cleanableNewList.size());
    EXPECT_EQ(1U, l.cleanableList.size());

    // cleanableNew deallocated by log destructor
}

// Need a do-nothing subclass of the abstract parent type.
class TestServerRpc : public Transport::ServerRpc {
    void sendReply() {}
};

TEST_F(LogTest, cleaningComplete) {
    Tub<uint64_t> serverId;
    serverId.construct(57);
    Log l(serverId, 3 * 8192, 8192, 4298, NULL, Log::CLEANER_DISABLED);

    ServerRpcPoolInternal::currentEpoch = 5;

    Segment* cleanSeg = new Segment(&l, l.allocateSegmentId(),
        l.getFromFreeList(false), 8192, NULL, LOG_ENTRY_TYPE_UNINIT,
        NULL, 0);

    Segment* liveSeg = new Segment(&l, l.allocateSegmentId(),
        l.getFromFreeList(false), 8192, NULL, LOG_ENTRY_TYPE_UNINIT,
        NULL, 0);

    l.cleaningInto(liveSeg);

    SegmentVector clean;
    l.cleanableList.push_back(*cleanSeg);
    clean.push_back(cleanSeg);

    std::vector<void*> empty;
    l.cleaningComplete(clean, empty);

    EXPECT_EQ(1U, l.cleanablePendingDigestList.size());
    EXPECT_EQ(1U, l.freePendingDigestAndReferenceList.size());
    EXPECT_EQ(0U, l.cleanableList.size());
    EXPECT_EQ(6U, ServerRpcPoolInternal::currentEpoch);
    EXPECT_EQ(5U, cleanSeg->cleanedEpoch);

    // ensure that segments aren't freed until possibly conflicting RPCs
    // are gone
    l.freePendingDigestAndReferenceList.erase(
        l.freePendingDigestAndReferenceList.iterator_to(*cleanSeg));
    l.freePendingReferenceList.push_back(*cleanSeg);
    ServerRpcPool<TestServerRpc> pool;
    TestServerRpc* rpc = pool.construct();
    clean.pop_back();
    cleanSeg->cleanedEpoch = 6;
    l.cleaningComplete(clean, empty);
    EXPECT_EQ(1U, l.freePendingReferenceList.size());

    pool.destroy(rpc);
    l.cleaningComplete(clean, empty);
    EXPECT_EQ(0U, l.freePendingReferenceList.size());

    // check returning unused segments memory
    clean.clear();
    void* toFreeAgain = l.freeList.back();
    l.freeList.pop_back();
    empty.push_back(toFreeAgain);
    l.cleaningComplete(clean, empty);
    EXPECT_EQ(toFreeAgain, l.freeList.back());

    // Segments above are deallocated by log destructor
}


/**
 * Unit tests for LogDigest.
 */
class LogDigestTest : public ::testing::Test {

  public:
    LogDigestTest() {}

  private:
    DISALLOW_COPY_AND_ASSIGN(LogDigestTest);
};

TEST_F(LogDigestTest, constructor) {
    // we have 2 constructors, one when creating a LogDigest to write
    // into a buffer (i.e. serialising it), and another that wraps the
    // buffer to access it later (i.e. deserialising).

    char temp[LogDigest::getBytesFromCount(3)];

    {
        LogDigest ld(3, static_cast<void*>(temp),
                        downCast<uint32_t>(sizeof(temp)));
        EXPECT_EQ(static_cast<void*>(temp),
            static_cast<void*>(ld.ldd));
        EXPECT_EQ(0U, ld.currentSegment);
        EXPECT_EQ(3U, ld.ldd->segmentCount);
        for (int i = 0; i < 3; i++) {
            EXPECT_TRUE(Segment::INVALID_SEGMENT_ID ==
                ld.ldd->segmentIds[i]);
        }
    }

    {
        LogDigest ld(static_cast<void*>(temp),
                        downCast<uint32_t>(sizeof(temp)));
        EXPECT_EQ(static_cast<void*>(temp),
            static_cast<void*>(ld.ldd));
        EXPECT_EQ(3U, ld.currentSegment);
    }
}

TEST_F(LogDigestTest, addSegment) {
    char temp[LogDigest::getBytesFromCount(3)];
    LogDigest ld(3, static_cast<void*>(temp),
                    downCast<uint32_t>(sizeof(temp)));
    EXPECT_EQ(0U, ld.currentSegment);
    ld.addSegment(54321);
    EXPECT_EQ(1U, ld.currentSegment);
    EXPECT_EQ(54321UL, ld.ldd->segmentIds[0]);
}

TEST_F(LogDigestTest, getters) {
    char temp[LogDigest::getBytesFromCount(3)];
    LogDigest ld(3, static_cast<void*>(temp),
                    downCast<uint32_t>(sizeof(temp)));

    EXPECT_EQ(3, ld.getSegmentCount());
    EXPECT_EQ(reinterpret_cast<uint64_t*>(&temp[4]),
        ld.getSegmentIds());
    EXPECT_EQ(4U, LogDigest::getBytesFromCount(0));
    EXPECT_EQ(12U, LogDigest::getBytesFromCount(1));
    EXPECT_EQ(20U, LogDigest::getBytesFromCount(2));
}

} // namespace RAMCloud
