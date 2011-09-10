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
#include "ServerRpcPool.h"
#include "Log.h"
#include "LogTypes.h"
#include "Transport.h"

namespace RAMCloud {

/**
 * Unit tests for Log.
 */
class LogTest : public CppUnit::TestFixture {

    DISALLOW_COPY_AND_ASSIGN(LogTest); // NOLINT

    CPPUNIT_TEST_SUITE(LogTest);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_allocateHead_basics);
    CPPUNIT_TEST(test_allocateHead_lists);
    CPPUNIT_TEST(test_addSegmentMemory);
    CPPUNIT_TEST(test_isSegmentLive);
    CPPUNIT_TEST(test_getSegmentId);
    CPPUNIT_TEST(test_append);
    CPPUNIT_TEST(test_free);
    CPPUNIT_TEST(test_registerType);
    CPPUNIT_TEST(test_getCallbacks);
    CPPUNIT_TEST(test_getNewCleanableSegments);
    CPPUNIT_TEST(test_cleaningComplete);
    CPPUNIT_TEST_SUITE_END();

  public:
    LogTest() {}

    void
    test_constructor()
    {
        Tub<uint64_t> serverId;
        serverId.construct(57);
        Log l(serverId, 2 * 8192, 8192);

        CPPUNIT_ASSERT_EQUAL(57, *l.logId);
        CPPUNIT_ASSERT_EQUAL(2 * 8192, l.logCapacity);
        CPPUNIT_ASSERT_EQUAL(8192, l.segmentCapacity);
        CPPUNIT_ASSERT_EQUAL(2, l.freeList.size());
        CPPUNIT_ASSERT_EQUAL(0, l.cleanableNewList.size());
        CPPUNIT_ASSERT_EQUAL(0, l.cleanablePendingDigestList.size());
        CPPUNIT_ASSERT_EQUAL(0, l.freePendingDigestAndReferenceList.size());
        CPPUNIT_ASSERT_EQUAL(0, l.freePendingReferenceList.size());
        CPPUNIT_ASSERT_EQUAL(0, l.nextSegmentId);
        CPPUNIT_ASSERT_EQUAL(8192 - 4 * sizeof(SegmentEntry) -
            sizeof(SegmentHeader) - sizeof(SegmentFooter) -
            LogDigest::getBytesFromCount(2),
            l.maximumAppendableBytes);
        CPPUNIT_ASSERT_EQUAL(NULL, l.head);
        CPPUNIT_ASSERT_EQUAL(Log::CONCURRENT_CLEANER, l.cleanerOption);

        Log l2(serverId, 2 * 8192, 8192, NULL, Log::CLEANER_DISABLED);
        CPPUNIT_ASSERT_EQUAL(Log::CLEANER_DISABLED, l2.cleanerOption);
    }

    void
    test_allocateHead_basics()
    {
        Tub<uint64_t> serverId;
        serverId.construct(57);
        Log l(serverId, 2 * 8192, 8192);

        {
            l.allocateHead();
            Segment* s = l.head;
            CPPUNIT_ASSERT(s != NULL);
            const SegmentEntry *se = reinterpret_cast<const SegmentEntry*>(
                (const char *)s->getBaseAddress() + sizeof(SegmentEntry) +
                sizeof(SegmentHeader));
            const void* ldp = (const char *)s->getBaseAddress() +
                sizeof(SegmentEntry) * 2 + sizeof(SegmentHeader);
            LogDigest ld(const_cast<void*>(ldp),
                LogDigest::getBytesFromCount(1));
            CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_LOGDIGEST, se->type);
            CPPUNIT_ASSERT_EQUAL(LogDigest::getBytesFromCount(1), se->length);
            CPPUNIT_ASSERT_EQUAL(1, ld.getSegmentCount());
            CPPUNIT_ASSERT_EQUAL(s->getId(), ld.getSegmentIds()[0]);
            CPPUNIT_ASSERT_EQUAL(s, l.activeIdMap[s->getId()]);
            CPPUNIT_ASSERT_EQUAL(s,
                l.activeBaseAddressMap[s->getBaseAddress()]);
        }

        {
            Segment* oldHead = l.head;
            l.allocateHead();
            Segment* s = l.head;
            CPPUNIT_ASSERT(s != NULL);
            const SegmentEntry *se = reinterpret_cast<const SegmentEntry*>(
                (const char *)s->getBaseAddress() + sizeof(SegmentEntry) +
                sizeof(SegmentHeader));
            const void* ldp = (const char *)s->getBaseAddress() +
                sizeof(SegmentEntry) * 2 + sizeof(SegmentHeader);
            LogDigest ld(const_cast<void*>(ldp),
                LogDigest::getBytesFromCount(2));
            CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_LOGDIGEST, se->type);
            CPPUNIT_ASSERT_EQUAL(LogDigest::getBytesFromCount(2), se->length);
            CPPUNIT_ASSERT_EQUAL(2, ld.getSegmentCount());
            CPPUNIT_ASSERT_EQUAL(s->getId(), ld.getSegmentIds()[1]);

            CPPUNIT_ASSERT_THROW(oldHead->close(), SegmentException);
            CPPUNIT_ASSERT(s != oldHead);
        }

        CPPUNIT_ASSERT_THROW(l.allocateHead(), LogException);
    }

    void
    test_allocateHead_lists()
    {
        Tub<uint64_t> serverId;
        serverId.construct(57);
        Log l(serverId, 5 * 8192, 8192, NULL, Log::CLEANER_DISABLED);

        Segment* cleaned = new Segment(&l, l.allocateSegmentId(),
            l.getFromFreeList(), 8192, NULL, LOG_ENTRY_TYPE_UNINIT,
            NULL, 0);
        l.cleanablePendingDigestList.push_back(*cleaned);

        Segment* cleanableNew = new Segment(&l, l.allocateSegmentId(),
            l.getFromFreeList(), 8192, NULL, LOG_ENTRY_TYPE_UNINIT,
            NULL, 0);
        l.cleanableNewList.push_back(*cleanableNew);

        Segment* cleanable = new Segment(&l, l.allocateSegmentId(),
            l.getFromFreeList(), 8192, NULL, LOG_ENTRY_TYPE_UNINIT,
            NULL, 0);
        l.cleanableList.push_back(*cleanable);

        Segment* freePending = new Segment(&l, l.allocateSegmentId(),
            l.getFromFreeList(), 8192, NULL, LOG_ENTRY_TYPE_UNINIT,
            NULL, 0);
        l.freePendingDigestAndReferenceList.push_back(*freePending);

        l.allocateHead();

        CPPUNIT_ASSERT_EQUAL(0, l.cleanablePendingDigestList.size());
        CPPUNIT_ASSERT_EQUAL(2, l.cleanableNewList.size());
        CPPUNIT_ASSERT_EQUAL(1, l.cleanableList.size());
        CPPUNIT_ASSERT_EQUAL(0, l.freePendingDigestAndReferenceList.size());
        CPPUNIT_ASSERT_EQUAL(1, l.freePendingReferenceList.size());

        const SegmentEntry *se = reinterpret_cast<const SegmentEntry*>(
            (const char *)l.head->getBaseAddress() + sizeof(SegmentEntry) +
            sizeof(SegmentHeader));
        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_LOGDIGEST, se->type);
        CPPUNIT_ASSERT_EQUAL(LogDigest::getBytesFromCount(4), se->length);

        // Segments allocated above are deallocated in the Log destructor.
    }

    void
    test_addSegmentMemory()
    {
        Tub<uint64_t> serverId;
        serverId.construct(57);
        Log l(serverId, 1 * 8192, 8192);

        void *p = xmemalign(l.segmentCapacity, l.segmentCapacity);
        l.addSegmentMemory(p);
        Segment s((uint64_t)0, 0, p, 8192);

        CPPUNIT_ASSERT_EQUAL(2, l.freeList.size());
        CPPUNIT_ASSERT_EQUAL(p, l.freeList[1]);
        CPPUNIT_ASSERT_EQUAL(s.appendableBytes(LogDigest::getBytesFromCount(1)),
                             l.maximumAppendableBytes);
    }

    void
    test_isSegmentLive()
    {
        Tub<uint64_t> serverId;
        serverId.construct(57);
        Log l(serverId, 2 * 8192, 8192);
        static char buf[64];

        uint64_t segmentId = l.nextSegmentId;
        CPPUNIT_ASSERT_EQUAL(false, l.isSegmentLive(segmentId));
        l.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        CPPUNIT_ASSERT_EQUAL(true, l.isSegmentLive(segmentId));
    }

    void
    test_getSegmentId()
    {
        Tub<uint64_t> serverId;
        serverId.construct(57);
        Log l(serverId, 2 * 8192, 8192);
        static char buf[64];

        const void *p = l.append(LOG_ENTRY_TYPE_OBJ,
            buf, sizeof(buf))->userData();
        CPPUNIT_ASSERT_EQUAL(0, l.getSegmentId(p));
        CPPUNIT_ASSERT_THROW(l.getSegmentId(
            reinterpret_cast<const char *>(p) + 8192), LogException);
    }

    void
    test_append()
    {
        Tub<uint64_t> serverId;
        serverId.construct(57);
        Log l(serverId, 2 * 8192, 8192, NULL, Log::CLEANER_DISABLED);
        static char buf[13];
        char fillbuf[l.getMaximumAppendableBytes()];
        memset(fillbuf, 'A', sizeof(fillbuf));

        CPPUNIT_ASSERT(l.head == NULL);
        CPPUNIT_ASSERT_EQUAL(2, l.freeList.size());

        // exercise head == NULL path
        SegmentEntryHandle seh = l.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        CPPUNIT_ASSERT(seh != NULL);
        CPPUNIT_ASSERT_EQUAL(sizeof(SegmentEntry) + sizeof(buf),
            seh->totalLength());
        CPPUNIT_ASSERT_EQUAL(0, memcmp(buf, seh->userData(), sizeof(buf)));
        CPPUNIT_ASSERT(LogTime(0,
            sizeof(SegmentEntry) + sizeof(SegmentHeader) + sizeof(SegmentEntry)
            + LogDigest::getBytesFromCount(1)) == seh->logTime());
        CPPUNIT_ASSERT(l.activeIdMap.find(l.head->getId()) !=
            l.activeIdMap.end());
        CPPUNIT_ASSERT(l.activeBaseAddressMap.find(l.head->getBaseAddress()) !=
            l.activeBaseAddressMap.end());
        CPPUNIT_ASSERT_EQUAL(1, l.freeList.size());

        // assert that the LogDigest is written out correctly
        const void* ldp = (const char *)l.head->getBaseAddress() +
            sizeof(SegmentEntry) * 2 + sizeof(SegmentHeader);
        LogDigest ld(const_cast<void*>(ldp), LogDigest::getBytesFromCount(1));
        CPPUNIT_ASSERT_EQUAL(1, ld.getSegmentCount());
        CPPUNIT_ASSERT_EQUAL(l.head->getId(), ld.getSegmentIds()[0]);

        // exercise head != NULL, but too few bytes (new head) path
        Segment *oldHead = l.head;
        seh = l.append(LOG_ENTRY_TYPE_OBJ, fillbuf, l.head->appendableBytes());
        CPPUNIT_ASSERT(seh != NULL);
        CPPUNIT_ASSERT_EQUAL(oldHead, l.head);
        CPPUNIT_ASSERT_EQUAL(0, l.head->appendableBytes());
        seh = l.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf), NULL);
        CPPUNIT_ASSERT(seh != NULL);
        CPPUNIT_ASSERT(oldHead != l.head);

        // exercise regular head != NULL path
        LogTime logTime = seh->logTime();
        LogTime nextTime;
        seh = l.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf), NULL);
        CPPUNIT_ASSERT(seh != NULL);
        CPPUNIT_ASSERT(seh->logTime() > logTime);

        CPPUNIT_ASSERT_EQUAL(4, l.stats.totalAppends);

        // fill the log and get an exception. we should be on the 3rd Segment
        // now.
        CPPUNIT_ASSERT_EQUAL(0, l.freeList.size());
        seh = l.append(LOG_ENTRY_TYPE_OBJ, fillbuf, l.head->appendableBytes());
        CPPUNIT_ASSERT(seh != NULL);
        CPPUNIT_ASSERT_THROW(l.append(LOG_ENTRY_TYPE_OBJ, buf, 1),
            LogException);
    }

    void
    test_free()
    {
        Tub<uint64_t> serverId;
        serverId.construct(57);
        Log l(serverId, 2 * 8192, 8192);
        static char buf[64];

        LogEntryHandle h = l.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        l.free(h);
        Segment *s = l.head;
        CPPUNIT_ASSERT_EQUAL(sizeof(buf) + sizeof(SegmentEntry), s->bytesFreed);

        CPPUNIT_ASSERT_THROW(l.free(LogEntryHandle(NULL)), LogException);
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

    void
    test_registerType()
    {
        Tub<uint64_t> serverId;
        serverId.construct(57);
        Log l(serverId, 1 * 8192, 8192);

        l.registerType(LOG_ENTRY_TYPE_OBJ,
                       livenessCallback, NULL,
                       relocationCallback, NULL,
                       timestampCallback,
                       scanCallback, NULL);
        CPPUNIT_ASSERT_THROW(
            l.registerType(LOG_ENTRY_TYPE_OBJ,
                           livenessCallback, NULL,
                           relocationCallback, NULL,
                           timestampCallback,
                           scanCallback, NULL),
            LogException);

        LogTypeCallback *cb = l.callbackMap[LOG_ENTRY_TYPE_OBJ];
        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_OBJ, cb->type);
        CPPUNIT_ASSERT_EQUAL(reinterpret_cast<void *>(livenessCallback),
                             reinterpret_cast<void *>(cb->livenessCB));
        CPPUNIT_ASSERT_EQUAL(NULL, cb->livenessArg);
        CPPUNIT_ASSERT_EQUAL(reinterpret_cast<void *>(relocationCallback),
                             reinterpret_cast<void *>(cb->relocationCB));
        CPPUNIT_ASSERT_EQUAL(NULL, cb->relocationArg);
        CPPUNIT_ASSERT_EQUAL(reinterpret_cast<void *>(timestampCallback),
                             reinterpret_cast<void *>(cb->timestampCB));
    }

    void
    test_getCallbacks()
    {
        Tub<uint64_t> serverId;
        serverId.construct(57);
        Log l(serverId, 1 * 8192, 8192);

        l.registerType(LOG_ENTRY_TYPE_OBJ,
                       livenessCallback, NULL,
                       relocationCallback, NULL,
                       timestampCallback,
                       scanCallback, NULL);

        const LogTypeCallback* cb = l.getCallbacks(LOG_ENTRY_TYPE_OBJ);
        CPPUNIT_ASSERT(cb != NULL);
        CPPUNIT_ASSERT_EQUAL(reinterpret_cast<void*>(livenessCallback),
                             reinterpret_cast<void*>(cb->livenessCB));

        CPPUNIT_ASSERT_EQUAL(NULL, l.getCallbacks(LOG_ENTRY_TYPE_OBJTOMB));
    }

    void
    test_getNewCleanableSegments()
    {
        Tub<uint64_t> serverId;
        serverId.construct(57);
        Log l(serverId, 2 * 8192, 8192, NULL, Log::CLEANER_DISABLED);

        mockWallTimeValue = 1;

        SegmentVector out;
        l.getNewCleanableSegments(out);
        CPPUNIT_ASSERT_EQUAL(0, out.size());

        Segment* cleanableNew = new Segment(&l, l.allocateSegmentId(),
            l.getFromFreeList(), 8192, NULL, LOG_ENTRY_TYPE_UNINIT,
            NULL, 0);

        l.cleanableNewList.push_back(*cleanableNew);

        CPPUNIT_ASSERT_EQUAL(1, l.cleanableNewList.size());
        CPPUNIT_ASSERT_EQUAL(0, l.cleanableList.size());

        mockWallTimeValue = 9999;
        l.getNewCleanableSegments(out);
        CPPUNIT_ASSERT_EQUAL(1, out.size());

        CPPUNIT_ASSERT_EQUAL(0, l.cleanableNewList.size());
        CPPUNIT_ASSERT_EQUAL(1, l.cleanableList.size());

        // cleanableNew deallocated by log destructor
    }

    // Need a do-nothing subclass of the abstract parent type.
    class TestServerRpc : public Transport::ServerRpc {
        void sendReply() {}
    };

    void
    test_cleaningComplete()
    {
        Tub<uint64_t> serverId;
        serverId.construct(57);
        Log l(serverId, 3 * 8192, 8192);

        ServerRpcPoolInternal::currentEpoch = 5;

        Segment* cleanSeg = new Segment(&l, l.allocateSegmentId(),
            l.getFromFreeList(), 8192, NULL, LOG_ENTRY_TYPE_UNINIT,
            NULL, 0);

        Segment* liveSeg = new Segment(&l, l.allocateSegmentId(),
            l.getFromFreeList(), 8192, NULL, LOG_ENTRY_TYPE_UNINIT,
            NULL, 0);

        l.cleaningInto(liveSeg);

        SegmentVector clean;
        l.cleanableList.push_back(*cleanSeg);
        clean.push_back(cleanSeg);

        l.cleaningComplete(clean);

        CPPUNIT_ASSERT_EQUAL(1, l.cleanablePendingDigestList.size());
        CPPUNIT_ASSERT_EQUAL(1, l.freePendingDigestAndReferenceList.size());
        CPPUNIT_ASSERT_EQUAL(0, l.cleanableList.size());
        CPPUNIT_ASSERT_EQUAL(6, ServerRpcPoolInternal::currentEpoch);
        CPPUNIT_ASSERT_EQUAL(5, cleanSeg->cleanedEpoch);

        // ensure that segments aren't freed until possibly conflicting RPCs
        // are gone
        l.freePendingDigestAndReferenceList.erase(
            l.freePendingDigestAndReferenceList.iterator_to(*cleanSeg));
        l.freePendingReferenceList.push_back(*cleanSeg);
        ServerRpcPool<TestServerRpc> pool;
        TestServerRpc* rpc = pool.construct();
        clean.pop_back();
        cleanSeg->cleanedEpoch = 6;
        l.cleaningComplete(clean);
        CPPUNIT_ASSERT_EQUAL(1, l.freePendingReferenceList.size());

        pool.destroy(rpc);
        l.cleaningComplete(clean);
        CPPUNIT_ASSERT_EQUAL(0, l.freePendingReferenceList.size());

        // Segments above are deallocated by log destructor
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(LogTest);


/**
 * Unit tests for LogDigest.
 */
class LogDigestTest : public CppUnit::TestFixture {

    DISALLOW_COPY_AND_ASSIGN(LogDigestTest); // NOLINT

    CPPUNIT_TEST_SUITE(LogDigestTest);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_addSegment);
    CPPUNIT_TEST(test_getters);
    CPPUNIT_TEST_SUITE_END();

  public:
    LogDigestTest() {}

    void
    test_constructor()
    {
        // we have 2 constructors, one when creating a LogDigest to write
        // into a buffer (i.e. serialising it), and another that wraps the
        // buffer to access it later (i.e. deserialising).

        char temp[LogDigest::getBytesFromCount(3)];

        {
            LogDigest ld(3, static_cast<void*>(temp),
                         downCast<uint32_t>(sizeof(temp)));
            CPPUNIT_ASSERT_EQUAL(static_cast<void*>(temp),
                static_cast<void*>(ld.ldd));
            CPPUNIT_ASSERT_EQUAL(0, ld.currentSegment);
            CPPUNIT_ASSERT_EQUAL(3, ld.ldd->segmentCount);
            for (int i = 0; i < 3; i++) {
                CPPUNIT_ASSERT_EQUAL(Segment::INVALID_SEGMENT_ID,
                    ld.ldd->segmentIds[i]);
            }
        }

        {
            LogDigest ld(static_cast<void*>(temp),
                         downCast<uint32_t>(sizeof(temp)));
            CPPUNIT_ASSERT_EQUAL(static_cast<void*>(temp),
                static_cast<void*>(ld.ldd));
            CPPUNIT_ASSERT_EQUAL(3, ld.currentSegment);
        }
    }

    void
    test_addSegment()
    {
        char temp[LogDigest::getBytesFromCount(3)];
        LogDigest ld(3, static_cast<void*>(temp),
                     downCast<uint32_t>(sizeof(temp)));
        CPPUNIT_ASSERT_EQUAL(0, ld.currentSegment);
        ld.addSegment(54321);
        CPPUNIT_ASSERT_EQUAL(1, ld.currentSegment);
        CPPUNIT_ASSERT_EQUAL(54321UL, ld.ldd->segmentIds[0]);
    }

    void
    test_getters()
    {
        char temp[LogDigest::getBytesFromCount(3)];
        LogDigest ld(3, static_cast<void*>(temp),
                     downCast<uint32_t>(sizeof(temp)));

        CPPUNIT_ASSERT_EQUAL(3, ld.getSegmentCount());
        CPPUNIT_ASSERT_EQUAL(reinterpret_cast<uint64_t*>(&temp[4]),
            ld.getSegmentIds());
        CPPUNIT_ASSERT_EQUAL(4, LogDigest::getBytesFromCount(0));
        CPPUNIT_ASSERT_EQUAL(12, LogDigest::getBytesFromCount(1));
        CPPUNIT_ASSERT_EQUAL(20, LogDigest::getBytesFromCount(2));
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(LogDigestTest);

} // namespace RAMCloud
