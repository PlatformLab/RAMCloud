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
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <Common.h>
#include <Log.h>
#include <LogTypes.h>
#include <config.h> // for SEGMENT_SIZE and SEGMENT_COUNT

#include <cppunit/extensions/HelperMacros.h>

namespace RAMCloud {

class LogTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(LogTest);
    CPPUNIT_TEST(TestInit);
    CPPUNIT_TEST(TestIsSegmentLive);
    CPPUNIT_TEST(TestGetSegmentIdOffset);
    CPPUNIT_TEST(TestAppend);
    CPPUNIT_TEST(TestFree);
    CPPUNIT_TEST(TestGetMaximumAppend);
    CPPUNIT_TEST(TestAllocateSegmentId);
    CPPUNIT_TEST(TestGetEvictionCallback);
    CPPUNIT_TEST(TestGetSegment);
    CPPUNIT_TEST(TestNewHead);
    CPPUNIT_TEST(TestChecksumHead);
    CPPUNIT_TEST(TestRetireHead);
    CPPUNIT_TEST(TestAppendAnyType);
    CPPUNIT_TEST_SUITE_END();
    RAMCloud::Log *Log;
    void *LogBase;
    MultiBackupClient *Backup;

  public:
    void
    setUp()
    {
        LogBase = xmalloc(SEGMENT_SIZE * SEGMENT_COUNT);
        Backup = new MultiBackupClient();
        Log = new RAMCloud::Log(SEGMENT_SIZE, LogBase,
            SEGMENT_SIZE * SEGMENT_COUNT, Backup);

        CPPUNIT_ASSERT_EQUAL(0, Log->numCallbacks);
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_INVALID_ID + 1,
            Log->nextSegmentId);
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_SIZE, Log->segment_size);
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_COUNT, Log->nsegments);
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_COUNT, Log->nfree_list);
        CPPUNIT_ASSERT(Log->max_append > 0);
        CPPUNIT_ASSERT(Log->max_append < Log->segment_size);
        CPPUNIT_ASSERT(Backup == Log->backup);
        CPPUNIT_ASSERT(NULL == Log->head);
        CPPUNIT_ASSERT(LogBase == Log->base);

        Log->init();
    }

    void
    tearDown()
    {
        free(LogBase);
        delete Log;
    }

    //XXX- future work
    void
    TestRestore()
    {
    }

    void
    TestInit()
    {
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_COUNT - 1, Log->nfree_list);
    }

    void
    TestIsSegmentLive()
    {
        uintptr_t b = (uintptr_t)LogBase;

        CPPUNIT_ASSERT_EQUAL(false, Log->isSegmentLive(SEGMENT_INVALID_ID));

        for (int i = 0; i < SEGMENT_COUNT; i++) {
            uint64_t id;
            uint32_t off;

            Log->getSegmentIdOffset((const void *)b, &id, &off);
            CPPUNIT_ASSERT_EQUAL((uint32_t)0, off);

            // only head is live after init
            bool ret = Log->isSegmentLive(id);
            if (id == Log->head->getId())
                CPPUNIT_ASSERT_EQUAL(true, ret);
            else
                CPPUNIT_ASSERT_EQUAL(false, ret);

            b += SEGMENT_SIZE;
        }
    }

    void
    TestGetSegmentIdOffset()
    {
        // nothing to do; Log mandates that this succeeds for now
    }

    void
    TestAppend()
    {
        char buf[1];

        // users of Log not allowed to write HEADER and CHECKSUM types
        CPPUNIT_ASSERT(Log->append(LOG_ENTRY_TYPE_SEGMENT_HEADER,
            (void *)buf, sizeof(buf)) == NULL);
        CPPUNIT_ASSERT(Log->append(LOG_ENTRY_TYPE_SEGMENT_CHECKSUM,
            (void *)buf, sizeof(buf)) == NULL);

        // all other writes handled in TestAppendAnyType()
    }

    void
    TestFree()
    {
            
    }

    void
    TestRegisterType()
    {
        // Tested in TestGetEvictionCallback
    }

    void
    TestGetMaximumAppend()
    {
        uint64_t ma = Log->getMaximumAppend();
        CPPUNIT_ASSERT(ma < SEGMENT_SIZE);
        CPPUNIT_ASSERT(ma > (SEGMENT_SIZE - 100));  // test reasonable bound
    }

    void
    TestAllocateSegmentId()
    {
        uint64_t id = Log->allocateSegmentId();
        for (uint64_t i = 1; i < 1000000; i++) {
            CPPUNIT_ASSERT_EQUAL(id + i, Log->allocateSegmentId()); 
        }
    }

    static void
    EvictionCallback(log_entry_type_t type, const void *p,
                     uint64_t len, void *cookie) 
    {
    }

    void
    TestGetEvictionCallback()
    {
        log_eviction_cb_t cb;
        int cookie = 1983;
        void *cookiep;

        CPPUNIT_ASSERT(
            Log->getEvictionCallback(LOG_ENTRY_TYPE_OBJECT, NULL) == NULL);

        Log->registerType(LOG_ENTRY_TYPE_OBJECT, EvictionCallback, &cookie);
        cb = Log->getEvictionCallback(LOG_ENTRY_TYPE_OBJECT, &cookiep);
        CPPUNIT_ASSERT((void *)cb == (void *)EvictionCallback);
        CPPUNIT_ASSERT(*(int *)cookiep == cookie);
    }

    void
    TestGetSegment()
    {
        // NB: Present code asserts success, so only sanity-check what should
        //     work. 

        CPPUNIT_ASSERT(Log->getSegment(LogBase, 0) != NULL);
        CPPUNIT_ASSERT(Log->getSegment(LogBase, 1) != NULL);
        CPPUNIT_ASSERT(Log->getSegment(LogBase, SEGMENT_SIZE) != NULL);

        uintptr_t b = (uintptr_t)LogBase;
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            CPPUNIT_ASSERT(Log->getSegment((void *)(b + (i * SEGMENT_SIZE)),
                SEGMENT_SIZE) != NULL);
        }
    }

    void
    TestClean()
    {
        // ugh.
    }

    void
    TestNewHead()
    {
    }

    void
    TestChecksumHead()
    {
    }

    void
    TestRetireHead()
    {
    }

    void
    TestAppendAnyType()
    {
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(LogTest);

} // namespace RAMCloud
