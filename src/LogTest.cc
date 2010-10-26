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
    CPPUNIT_TEST(test_forEachSegment);
    CPPUNIT_TEST_SUITE_END();

  public:
    LogTest() {}

    Log *
    createTestLog(uint64_t logId, uint64_t segmentSize, int numSegments)
    {
        Log *l = new Log(logId, segmentSize);
        for (int i = 0; i < numSegments; i++)
            l->addSegmentMemory(xmemalign(segmentSize, segmentSize));
        return l;
    }

    void
    destroyTestLog(Log *l)
    {
        for (unsigned int i = 0; i < l->segmentFreeList.size(); i++)
            free(l->segmentFreeList[i]);
        delete l;
    }

    void
    test_constructor()
    {
        Log l(57, 8192);

        CPPUNIT_ASSERT_EQUAL(57, l.logId);
        CPPUNIT_ASSERT_EQUAL(8192, l.segmentSize);
        CPPUNIT_ASSERT_EQUAL(0, l.segmentFreeList.size());
        CPPUNIT_ASSERT_EQUAL(0, l.nextSegmentId);
        CPPUNIT_ASSERT_EQUAL(0, l.maximumAppendableBytes);
        CPPUNIT_ASSERT_EQUAL(NULL, l.head);
    }

    void
    test_addSegmentMemory()
    {
        Log l(57, 8192);

        void *p = xmemalign(l.segmentSize, l.segmentSize);
        l.addSegmentMemory(p);
        Segment s(0, 0, p, 8192);

        CPPUNIT_ASSERT_EQUAL(1, l.segmentFreeList.size());
        CPPUNIT_ASSERT_EQUAL(p, l.segmentFreeList[0]);
        CPPUNIT_ASSERT_EQUAL(s.appendableBytes(), l.maximumAppendableBytes);

        free(p);
    }

    void
    test_isSegmentLive()
    {
        Log *l = createTestLog(57, 8192, 1);
        char buf[64];

        uint64_t segmentId = l->nextSegmentId;
        CPPUNIT_ASSERT_EQUAL(false, l->isSegmentLive(segmentId));
        l->append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        CPPUNIT_ASSERT_EQUAL(true, l->isSegmentLive(segmentId));

        destroyTestLog(l);
    }

    void
    test_getSegmentId()
    {
        Log l(57, 8192);
        char buf[64];

        void *p = xmemalign(l.segmentSize, l.segmentSize);
        l.addSegmentMemory(p);

        l.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        CPPUNIT_ASSERT_EQUAL(0, l.getSegmentId(p));
        CPPUNIT_ASSERT_EQUAL(0, l.getSegmentId((char *)p + 8191));

        free(p); 
    }

    void
    test_append()
    {
        Log *l = createTestLog(57, 8192, 1);

        //XXXXXXX- need to actually implement this.

        destroyTestLog(l);
    }

    void
    test_free()
    {
        Log l(57, 8192);
        char buf[64];

        void *p = xmemalign(l.segmentSize, l.segmentSize);
        l.addSegmentMemory(p);

        const void *p2 = l.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
        l.free(p2, sizeof(buf));
        Segment *s = l.activeIdMap[0];
        CPPUNIT_ASSERT_EQUAL(64 + sizeof(SegmentEntry), s->bytesFreed);

        free(p);
    }

    static void
    evictionCallback(LogEntryType type, const void *p, const uint64_t length,
        void *cookie)
    {
    }

    void
    test_registerType()
    {
        Log *l = createTestLog(57, 8192, 1);

        bool threwException = false;
        l->registerType(LOG_ENTRY_TYPE_OBJ, evictionCallback, NULL);
        try {
            l->registerType(LOG_ENTRY_TYPE_OBJ, evictionCallback, NULL);
        } catch (...) {
            threwException = true;
        }
        CPPUNIT_ASSERT_EQUAL(true, threwException);

        LogTypeCallback *cb = l->callbackMap[LOG_ENTRY_TYPE_OBJ];
        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_OBJ, cb->type);
        CPPUNIT_ASSERT_EQUAL((void *)evictionCallback, (void *)cb->evictionCB);
        CPPUNIT_ASSERT_EQUAL(NULL, cb->evictionArg);

        destroyTestLog(l);
    }

    void
    test_forEachSegment()
    {
        Log *l = createTestLog(57, 8192, 1);
        destroyTestLog(l);
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(LogTest);

} // namespace RAMCloud
