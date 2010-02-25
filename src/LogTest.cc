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
        Segment *old_head = Log->head;        

        char buf[1];
        while (Log->head == old_head) {
            uint64_t old_util = Log->head->getUtilization();
            uint64_t old_tail = Log->head->tail_bytes;
            uint64_t new_util = old_util + sizeof(log_entry) + sizeof(buf);
            uint64_t new_tail = old_tail - sizeof(log_entry) - sizeof(buf);

            const void *p = Log->append(LOG_ENTRY_TYPE_OBJECT,
                buf, sizeof(buf));
            CPPUNIT_ASSERT(p != NULL);

            // Head may have changed due to the write.
            if (Log->head == old_head) {
                CPPUNIT_ASSERT_EQUAL(new_util, Log->head->getUtilization());
                CPPUNIT_ASSERT_EQUAL(new_tail, Log->head->tail_bytes);

                Log->free(LOG_ENTRY_TYPE_OBJECT, p, sizeof(buf));

                CPPUNIT_ASSERT_EQUAL(old_util, Log->head->getUtilization());
                CPPUNIT_ASSERT_EQUAL(new_tail, Log->head->tail_bytes);
            }
        }
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
        // ugh. the present cleaner is a total hack... is it really worth
        // testing?
    }

    void
    TestNewHead()
    {
        Segment *old_head = Log->head;
        uint64_t old_nfree_list = Log->nfree_list;

        CPPUNIT_ASSERT(Log->head != NULL);
        Log->newHead();
        CPPUNIT_ASSERT(Log->head != NULL && Log->head != old_head);
        CPPUNIT_ASSERT_EQUAL(old_nfree_list - 1, Log->nfree_list);

        log_entry *le = (log_entry *)Log->head->getBase();
        CPPUNIT_ASSERT_EQUAL((uint32_t)LOG_ENTRY_TYPE_SEGMENT_HEADER, le->type);
        CPPUNIT_ASSERT_EQUAL((uint32_t)sizeof(segment_header), le->length);
        CPPUNIT_ASSERT_EQUAL(sizeof(segment_header) + sizeof(*le),
            Log->head->getUtilization());
    }

    void
    TestChecksumHead()
    {
        Log->checksumHead();

        void *segend = (char *)Log->head->base +
            Log->head->getUtilization() - sizeof(segment_checksum) -
            sizeof(segment_header);
        log_entry *le = (log_entry *)segend;
        CPPUNIT_ASSERT_EQUAL((uint32_t)LOG_ENTRY_TYPE_SEGMENT_CHECKSUM,
            le->type);
        CPPUNIT_ASSERT_EQUAL((uint32_t)sizeof(segment_checksum), le->length);

    }

    void
    TestRetireHead()
    {
        Segment *old_head = Log->head;
        CPPUNIT_ASSERT(old_head != NULL);
        CPPUNIT_ASSERT_EQUAL(true, Log->head->isMutable);
        Log->retireHead();
        CPPUNIT_ASSERT(NULL == Log->head);
        CPPUNIT_ASSERT_EQUAL(false, old_head->isMutable);
    }

    void
    TestAppendAnyType()
    {
        char buf[1];
        char maxbuf[Log->getMaximumAppend()];
        uint64_t tmp;

        // Can append up to the maximum
        CPPUNIT_ASSERT(NULL != Log->appendAnyType(LOG_ENTRY_TYPE_OBJECT,
            maxbuf, sizeof(maxbuf)));

        // Clear the Head segment so we can write another header
        Log->head->finalize();
        Log->head->reset();
        Log->head->ready(53);

        // Internal Segment Headers don't affect our bytes stored count
        tmp = Log->bytes_stored; 
        Log->appendAnyType(LOG_ENTRY_TYPE_SEGMENT_HEADER, buf, sizeof(buf)); 
        CPPUNIT_ASSERT_EQUAL(tmp, Log->bytes_stored);

        // Internal Segment Checksums don't affect our bytes stored count
        tmp = Log->bytes_stored; 
        Log->appendAnyType(LOG_ENTRY_TYPE_SEGMENT_CHECKSUM, buf, sizeof(buf)); 
        CPPUNIT_ASSERT_EQUAL(tmp, Log->bytes_stored);

        // All other objects count toward stored bytes
        tmp = Log->bytes_stored;
        Log->appendAnyType(LOG_ENTRY_TYPE_OBJECT, buf, sizeof(buf)); 
        CPPUNIT_ASSERT_EQUAL(tmp + sizeof(buf), Log->bytes_stored);
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(LogTest);

} // namespace RAMCloud
