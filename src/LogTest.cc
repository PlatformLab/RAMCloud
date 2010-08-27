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

// RAMCloud pragma [GCCWARN=5]
// RAMCloud pragma [CPPLINT=0]

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

    RAMCloud::Log *log;
    void *logBase;
    MultiBackupClient *backup;

  public:
    LogTest() : log(NULL), logBase(NULL), backup(NULL) { }
    void
    setUp()
    {
        logBase = xmalloc(SEGMENT_SIZE * SEGMENT_COUNT);
        backup = new MultiBackupClient();
        log = new RAMCloud::Log(SEGMENT_SIZE, logBase,
            SEGMENT_SIZE * SEGMENT_COUNT, backup);

        CPPUNIT_ASSERT_EQUAL(0, log->numCallbacks);
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_INVALID_ID + 1,
            log->nextSegmentId);
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_SIZE, log->segment_size);
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_COUNT, log->nsegments);
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_COUNT, log->nfree_list);
        CPPUNIT_ASSERT(log->max_append > 0);
        CPPUNIT_ASSERT(log->max_append < log->segment_size);
        CPPUNIT_ASSERT(backup == log->backup);
        CPPUNIT_ASSERT(NULL == log->head);
        CPPUNIT_ASSERT(logBase == log->base);

        log->init();
    }

    void
    tearDown()
    {
        free(logBase);
        delete log;
    }

    //XXX- future work
    void
    TestRestore()
    {
    }

    void
    TestInit()
    {
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_COUNT - 1, log->nfree_list);
    }

    void
    TestIsSegmentLive()
    {
        uintptr_t b = (uintptr_t)logBase;

        CPPUNIT_ASSERT_EQUAL(false, log->isSegmentLive(SEGMENT_INVALID_ID));

        for (int i = 0; i < SEGMENT_COUNT; i++) {
            uint64_t id;
            uint32_t off;

            log->getSegmentIdOffset((const void *)b, &id, &off);
            CPPUNIT_ASSERT_EQUAL((uint32_t)0, off);

            // only head is live after init
            bool ret = log->isSegmentLive(id);
            if (id == log->head->getId())
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

        // users of log not allowed to write HEADER and CHECKSUM types
        CPPUNIT_ASSERT(log->append(LOG_ENTRY_TYPE_SEGMENT_HEADER,
            (void *)buf, sizeof(buf)) == NULL);
        CPPUNIT_ASSERT(log->append(LOG_ENTRY_TYPE_SEGMENT_CHECKSUM,
            (void *)buf, sizeof(buf)) == NULL);

        // all other writes handled in TestAppendAnyType()
    }

    void
    TestFree()
    {
        Segment *old_head = log->head;        

        char buf[1];
        while (log->head == old_head) {
            uint64_t old_util = log->head->getUtilization();
            uint64_t old_tail = log->head->tail_bytes;
            uint64_t new_util = old_util + sizeof(log_entry) + sizeof(buf);
            uint64_t new_tail = old_tail - sizeof(log_entry) - sizeof(buf);

            const void *p = log->append(LOG_ENTRY_TYPE_OBJECT,
                buf, sizeof(buf));

            // CPPUNIT_ASSERT is too slow. Using if statements instead saves
            // ~0.6s on my machine (using 3MB segments). -Diego

            if (p == NULL)
                CPPUNIT_ASSERT(false);

            // Head may have changed due to the write.
            if (log->head == old_head) {
                if (new_util != log->head->getUtilization())
                    CPPUNIT_ASSERT(false);
                if (new_tail != log->head->tail_bytes)
                    CPPUNIT_ASSERT(false);

                log->free(LOG_ENTRY_TYPE_OBJECT, p, sizeof(buf));

                if (old_util != log->head->getUtilization())
                    CPPUNIT_ASSERT(false);
                if (new_tail != log->head->tail_bytes)
                    CPPUNIT_ASSERT(false);
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
        uint64_t ma = log->getMaximumAppend();
        CPPUNIT_ASSERT(ma < SEGMENT_SIZE);
        CPPUNIT_ASSERT(ma > (SEGMENT_SIZE - 100));  // test reasonable bound
    }

    void
    TestAllocateSegmentId()
    {
        uint64_t id = log->allocateSegmentId();
        for (uint64_t i = 1; i < 10; i++) {
            CPPUNIT_ASSERT_EQUAL(id + i, log->allocateSegmentId()); 
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
            log->getEvictionCallback(LOG_ENTRY_TYPE_OBJECT, NULL) == NULL);

        log->registerType(LOG_ENTRY_TYPE_OBJECT, EvictionCallback, &cookie);
        cb = log->getEvictionCallback(LOG_ENTRY_TYPE_OBJECT, &cookiep);
        CPPUNIT_ASSERT((void *)cb == (void *)EvictionCallback);
        CPPUNIT_ASSERT(*(int *)cookiep == cookie);
    }

    void
    TestGetSegment()
    {
        // NB: Present code asserts success, so only sanity-check what should
        //     work. 

        CPPUNIT_ASSERT(log->getSegment(logBase, 0) != NULL);
        CPPUNIT_ASSERT(log->getSegment(logBase, 1) != NULL);
        CPPUNIT_ASSERT(log->getSegment(logBase, SEGMENT_SIZE) != NULL);

        uintptr_t b = (uintptr_t)logBase;
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            CPPUNIT_ASSERT(log->getSegment((void *)(b + (i * SEGMENT_SIZE)),
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
        Segment *old_head = log->head;
        uint64_t old_nfree_list = log->nfree_list;

        CPPUNIT_ASSERT(log->head != NULL);
        log->newHead();
        CPPUNIT_ASSERT(log->head != NULL && log->head != old_head);
        CPPUNIT_ASSERT_EQUAL(old_nfree_list - 1, log->nfree_list);

        log_entry *le = (log_entry *)log->head->getBase();
        CPPUNIT_ASSERT_EQUAL((uint32_t)LOG_ENTRY_TYPE_SEGMENT_HEADER, le->type);
        CPPUNIT_ASSERT_EQUAL((uint32_t)sizeof(segment_header), le->length);
        CPPUNIT_ASSERT_EQUAL(sizeof(segment_header) + sizeof(*le),
            log->head->getUtilization());
    }

    void
    TestChecksumHead()
    {
        log->checksumHead();

        void *segend = (char *)log->head->base +
            log->head->getUtilization() - sizeof(segment_checksum) -
            sizeof(segment_header);
        log_entry *le = (log_entry *)segend;
        CPPUNIT_ASSERT_EQUAL((uint32_t)LOG_ENTRY_TYPE_SEGMENT_CHECKSUM,
            le->type);
        CPPUNIT_ASSERT_EQUAL((uint32_t)sizeof(segment_checksum), le->length);

    }

    void
    TestRetireHead()
    {
        Segment *old_head = log->head;
        CPPUNIT_ASSERT(old_head != NULL);
        CPPUNIT_ASSERT_EQUAL(true, log->head->isMutable);
        log->retireHead();
        CPPUNIT_ASSERT(NULL == log->head);
        CPPUNIT_ASSERT_EQUAL(false, old_head->isMutable);
    }

    void
    TestAppendAnyType()
    {
        char buf[1];
        char maxbuf[log->getMaximumAppend()];
        uint64_t tmp;

        // Can append up to the maximum
        CPPUNIT_ASSERT(NULL != log->appendAnyType(LOG_ENTRY_TYPE_OBJECT,
            maxbuf, sizeof(maxbuf)));

        // Clear the Head segment so we can write another header
        log->head->finalize();
        log->head->reset();
        log->head->ready(53);

        // Internal Segment Headers don't affect our bytes stored count
        tmp = log->bytes_stored; 
        log->appendAnyType(LOG_ENTRY_TYPE_SEGMENT_HEADER, buf, sizeof(buf)); 
        CPPUNIT_ASSERT_EQUAL(tmp, log->bytes_stored);

        // Internal Segment Checksums don't affect our bytes stored count
        tmp = log->bytes_stored; 
        log->appendAnyType(LOG_ENTRY_TYPE_SEGMENT_CHECKSUM, buf, sizeof(buf)); 
        CPPUNIT_ASSERT_EQUAL(tmp, log->bytes_stored);

        // All other objects count toward stored bytes
        tmp = log->bytes_stored;
        log->appendAnyType(LOG_ENTRY_TYPE_OBJECT, buf, sizeof(buf)); 
        CPPUNIT_ASSERT_EQUAL(tmp + sizeof(buf), log->bytes_stored);
    }

    DISALLOW_COPY_AND_ASSIGN(LogTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(LogTest);

} // namespace RAMCloud
