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

namespace RAMCloud {

/**
 * Unit tests for Log.
 */
class LogTest : public CppUnit::TestFixture {

    DISALLOW_COPY_AND_ASSIGN(LogTest); // NOLINT

    CPPUNIT_TEST_SUITE(LogTest);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_addSegmentMemory);
    CPPUNIT_TEST(test_isSegmentLive);
    CPPUNIT_TEST(test_getSegmentId);
    CPPUNIT_TEST(test_append);
    CPPUNIT_TEST(test_free);
    CPPUNIT_TEST(test_registerType);
    CPPUNIT_TEST(test_getSegmentBaseAddress);
    CPPUNIT_TEST_SUITE_END();

  public:
    LogTest() {}

    void
    test_constructor()
    {
        Log l(57, 2 * 8192, 8192);

        CPPUNIT_ASSERT_EQUAL(57, l.logId);
        CPPUNIT_ASSERT_EQUAL(2 * 8192, l.logCapacity);
        CPPUNIT_ASSERT_EQUAL(8192, l.segmentCapacity);
        CPPUNIT_ASSERT_EQUAL(2, l.segmentFreeList.size());
        CPPUNIT_ASSERT_EQUAL(0, l.nextSegmentId);
        CPPUNIT_ASSERT_EQUAL(8192 - 3 * sizeof(SegmentEntry) -
            sizeof(SegmentHeader) - sizeof(SegmentFooter),
            l.maximumAppendableBytes);
        CPPUNIT_ASSERT_EQUAL(NULL, l.head);
    }

    void
    test_addSegmentMemory()
    {
        Log l(57, 1 * 8192, 8192);

        void *p = xmemalign(l.segmentCapacity, l.segmentCapacity);
        l.addSegmentMemory(p);
        Segment s((uint64_t)0, 0, p, 8192);

        CPPUNIT_ASSERT_EQUAL(2, l.segmentFreeList.size());
        CPPUNIT_ASSERT_EQUAL(p, l.segmentFreeList[1]);
        CPPUNIT_ASSERT_EQUAL(s.appendableBytes(), l.maximumAppendableBytes);
    }

    void
    test_isSegmentLive()
    {
        Log l(57, 1 * 8192, 8192);
        char buf[64];

        uint64_t segmentId = l.nextSegmentId;
        CPPUNIT_ASSERT_EQUAL(false, l.isSegmentLive(segmentId));
        l.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        CPPUNIT_ASSERT_EQUAL(true, l.isSegmentLive(segmentId));
    }

    void
    test_getSegmentId()
    {
        Log l(57, 1 * 8192, 8192);
        char buf[64];

        const void *p = l.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        CPPUNIT_ASSERT_EQUAL(0, l.getSegmentId(p));
        CPPUNIT_ASSERT_THROW(l.getSegmentId(
            reinterpret_cast<const char *>(p) + 8192), LogException);
    }

    void
    test_append()
    {
        Log l(57, 2 * 8192, 8192);
        uint64_t lengthInLog;
        LogTime logTime;
        char buf[13];
        char fillbuf[l.getMaximumAppendableBytes()];

        // keep the cleaner from dumping our objects
        l.useCleaner = false;

        CPPUNIT_ASSERT(l.head == NULL);
        CPPUNIT_ASSERT_EQUAL(2, l.segmentFreeList.size());

        // exercise head == NULL path
        const void *p = l.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf),
            &lengthInLog, &logTime);
        CPPUNIT_ASSERT(p != NULL);
        CPPUNIT_ASSERT_EQUAL(sizeof(SegmentEntry) + sizeof(buf), lengthInLog);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(buf, p, sizeof(buf)));
        CPPUNIT_ASSERT(LogTime(0,
            sizeof(SegmentEntry) + sizeof(SegmentHeader)) == logTime);
        CPPUNIT_ASSERT(l.activeIdMap.find(l.head->getId()) !=
            l.activeIdMap.end());
        CPPUNIT_ASSERT(l.activeBaseAddressMap.find(l.head->getBaseAddress()) !=
            l.activeBaseAddressMap.end());
        CPPUNIT_ASSERT_EQUAL(1, l.segmentFreeList.size());

        // exercise head != NULL, but too few bytes (new head) path
        Segment *oldHead = l.head;
        p = l.append(LOG_ENTRY_TYPE_OBJ, fillbuf, l.head->appendableBytes());
        CPPUNIT_ASSERT(p != NULL);
        CPPUNIT_ASSERT_EQUAL(oldHead, l.head);
        CPPUNIT_ASSERT_EQUAL(0, l.head->appendableBytes());
        p = l.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf), NULL, &logTime);
        CPPUNIT_ASSERT(p != NULL);
        CPPUNIT_ASSERT(oldHead != l.head);

        // execise regular head != NULL path
        LogTime nextTime;
        p = l.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf), NULL, &nextTime);
        CPPUNIT_ASSERT(p != NULL);
        CPPUNIT_ASSERT(nextTime > logTime);

        CPPUNIT_ASSERT_EQUAL(4, l.stats.totalAppends);

        // fill the log and get an exception. we should be on the 3rd Segment
        // now.
        CPPUNIT_ASSERT_EQUAL(0, l.segmentFreeList.size());
        p = l.append(LOG_ENTRY_TYPE_OBJ, fillbuf, l.head->appendableBytes());
        CPPUNIT_ASSERT(p != NULL);
        CPPUNIT_ASSERT_THROW(l.append(LOG_ENTRY_TYPE_OBJ, buf, 1),
            LogException);
    }

    void
    test_free()
    {
        Log l(57, 1 * 8192, 8192);
        char buf[64];

        const void *p = l.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        l.free(p);
        Segment *s = l.activeIdMap[0];
        CPPUNIT_ASSERT_EQUAL(sizeof(buf) + sizeof(SegmentEntry), s->bytesFreed);

        CPPUNIT_ASSERT_THROW(l.free(NULL), LogException);
    }

    static void
    evictionCallback(LogEntryType type, const void *p,
        const uint64_t entryLength, const uint64_t lengthInLog,
        const LogTime logTime, void *cookie)
    {
    }

    void
    test_registerType()
    {
        Log l(57, 1 * 8192, 8192);

        l.registerType(LOG_ENTRY_TYPE_OBJ, evictionCallback, NULL);
        CPPUNIT_ASSERT_THROW(
            l.registerType(LOG_ENTRY_TYPE_OBJ, evictionCallback, NULL),
            LogException);

        LogTypeCallback *cb = l.callbackMap[LOG_ENTRY_TYPE_OBJ];
        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_OBJ, cb->type);
        CPPUNIT_ASSERT_EQUAL(reinterpret_cast<void *>(evictionCallback),
                             reinterpret_cast<void *>(cb->evictionCB));
        CPPUNIT_ASSERT_EQUAL(NULL, cb->evictionArg);
    }

void
test_getSegmentBaseAddress()
{
    Log l(57, 1 * 128, 128);
    CPPUNIT_ASSERT_EQUAL(128,
        reinterpret_cast<uintptr_t>(l.getSegmentBaseAddress(
        reinterpret_cast<void *>(128))));
    CPPUNIT_ASSERT_EQUAL(128,
        reinterpret_cast<uintptr_t>(l.getSegmentBaseAddress(
        reinterpret_cast<void *>(129))));
    CPPUNIT_ASSERT_EQUAL(128,
        reinterpret_cast<uintptr_t>(l.getSegmentBaseAddress(
        reinterpret_cast<void *>(255))));
    CPPUNIT_ASSERT_EQUAL(256,
        reinterpret_cast<uintptr_t>(l.getSegmentBaseAddress(
        reinterpret_cast<void *>(256))));
}

};
CPPUNIT_TEST_SUITE_REGISTRATION(LogTest);

} // namespace RAMCloud
