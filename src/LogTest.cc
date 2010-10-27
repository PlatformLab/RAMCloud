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

#include "TestUtil.h"

#include "Common.h"
#include "Log.h"
#include "LogTypes.h"
#include "Master.h"
#include "Segment.h"

namespace RAMCloud {

class LogTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(LogTest);
    CPPUNIT_TEST(TestInit);
    CPPUNIT_TEST(TestIsSegmentLive);
    CPPUNIT_TEST(TestGetSegmentIdOffset);
    CPPUNIT_TEST(TestAppend);
    CPPUNIT_TEST(TestFree);
    CPPUNIT_TEST(TestRegisterType);
    CPPUNIT_TEST(TestGetMaximumAppend);
    CPPUNIT_TEST(TestAllocateSegmentId);
    CPPUNIT_TEST(TestGetEvictionCallback);
    CPPUNIT_TEST(TestGetSegment);
    CPPUNIT_TEST(TestClean);
    CPPUNIT_TEST(TestNewHead);
    CPPUNIT_TEST(TestChecksumHead);
    CPPUNIT_TEST(TestRetireHead);
    CPPUNIT_TEST(TestAppendAnyType);
    CPPUNIT_TEST_SUITE_END();

    RAMCloud::Log *log;
    void *logBase;
    BackupManager* backup;

  public:
    LogTest() : log(NULL), logBase(NULL), backup(NULL) { }
    void
    setUp()
    {
        logBase = xmalloc(SEGMENT_SIZE * Master::SEGMENT_COUNT);
        backup = new BackupManager(0);
        log = new RAMCloud::Log(SEGMENT_SIZE, logBase,
            SEGMENT_SIZE * Master::SEGMENT_COUNT, backup);

        CPPUNIT_ASSERT_EQUAL(0, log->numCallbacks);
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_INVALID_ID + 1,
            log->nextSegmentId);
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_SIZE, log->segmentSize);
        CPPUNIT_ASSERT_EQUAL((uint64_t)Master::SEGMENT_COUNT, log->nsegments);
        CPPUNIT_ASSERT_EQUAL((uint64_t)Master::SEGMENT_COUNT, log->nFreeList);
        CPPUNIT_ASSERT(log->maxAppend > 0);
        CPPUNIT_ASSERT(log->maxAppend < log->segmentSize);
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

    void
    TestInit()
    {
        CPPUNIT_ASSERT_EQUAL((uint64_t)Master::SEGMENT_COUNT - 1,
                             log->nFreeList);
    }

    void
    TestIsSegmentLive()
    {
        uintptr_t b = (uintptr_t)logBase;

        CPPUNIT_ASSERT_EQUAL(false, log->isSegmentLive(SEGMENT_INVALID_ID));

        for (uint32_t i = 0; i < Master::SEGMENT_COUNT; i++) {
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
                buf, sizeof(buf)) == NULL);
        CPPUNIT_ASSERT(log->append(LOG_ENTRY_TYPE_SEGMENT_CHECKSUM,
                buf, sizeof(buf)) == NULL);

        // all other writes handled in TestAppendAnyType()
    }

    void
    TestFree()
    {
        Segment *oldHead = log->head;

        char buf[1];
        while (log->head == oldHead) {
            uint64_t oldUtil = log->head->getUtilization();
            uint64_t oldTail = log->head->tailBytes;
            uint64_t newUtil = oldUtil + sizeof(LogEntry) + sizeof(buf);
            uint64_t newTail = oldTail - sizeof(LogEntry) - sizeof(buf);

            const void *p = log->append(LOG_ENTRY_TYPE_OBJECT,
                buf, sizeof(buf));

            // CPPUNIT_ASSERT is too slow. Using if statements instead saves
            // ~0.6s on my machine (using 3MB segments). -Diego

            if (p == NULL)
                CPPUNIT_ASSERT(false);

            // Head may have changed due to the write.
            if (log->head == oldHead) {
                if (newUtil != log->head->getUtilization())
                    CPPUNIT_ASSERT(false);
                if (newTail != log->head->tailBytes)
                    CPPUNIT_ASSERT(false);

                log->free(LOG_ENTRY_TYPE_OBJECT, p, sizeof(buf));

                if (oldUtil != log->head->getUtilization())
                    CPPUNIT_ASSERT(false);
                if (newTail != log->head->tailBytes)
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
    EvictionCallback(LogEntryType type, const void *p,
                     uint64_t len, void *cookie)
    {
    }

    void
    TestGetEvictionCallback()
    {
        LogEvictionCallback cb;
        int cookie = 1983;
        void *cookiep;

        CPPUNIT_ASSERT(
            log->getEvictionCallback(LOG_ENTRY_TYPE_OBJECT, NULL) == NULL);

        log->registerType(LOG_ENTRY_TYPE_OBJECT, EvictionCallback, &cookie);
        cb = log->getEvictionCallback(LOG_ENTRY_TYPE_OBJECT, &cookiep);
        CPPUNIT_ASSERT(reinterpret_cast<void*>(cb) ==
                reinterpret_cast<void*>(EvictionCallback));
        CPPUNIT_ASSERT_EQUAL(cookie, *static_cast<int*>(cookiep));
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
        for (uint32_t i = 0; i < Master::SEGMENT_COUNT; i++) {
            CPPUNIT_ASSERT(log->getSegment(reinterpret_cast<void*>(b +
                        (i * SEGMENT_SIZE)), SEGMENT_SIZE) != NULL);
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
        Segment *oldHead = log->head;
        uint64_t old_nfree_list = log->nFreeList;

        CPPUNIT_ASSERT(log->head != NULL);
        log->newHead();
        CPPUNIT_ASSERT(log->head != NULL && log->head != oldHead);
        CPPUNIT_ASSERT_EQUAL(old_nfree_list - 1, log->nFreeList);

        const LogEntry *le = static_cast<const LogEntry*>(
                log->head->getBase());
        CPPUNIT_ASSERT_EQUAL((uint32_t)LOG_ENTRY_TYPE_SEGMENT_HEADER, le->type);
        CPPUNIT_ASSERT_EQUAL((uint32_t)sizeof(SegmentHeader), le->length);
        CPPUNIT_ASSERT_EQUAL(sizeof(SegmentHeader) + sizeof(*le),
            log->head->getUtilization());
    }

    void
    TestChecksumHead()
    {
        log->checksumHead();

        void *segend = static_cast<char*>(log->head->base) +
            log->head->getUtilization() - sizeof(SegmentChecksum) -
            sizeof(SegmentHeader);
        LogEntry *le = static_cast<LogEntry*>(segend);
        CPPUNIT_ASSERT_EQUAL((uint32_t)LOG_ENTRY_TYPE_SEGMENT_CHECKSUM,
            le->type);
        CPPUNIT_ASSERT_EQUAL((uint32_t)sizeof(SegmentChecksum), le->length);

    }

    void
    TestRetireHead()
    {
        Segment *oldHead = log->head;
        CPPUNIT_ASSERT(oldHead != NULL);
        CPPUNIT_ASSERT_EQUAL(true, log->head->isMutable);
        log->retireHead();
        CPPUNIT_ASSERT(NULL == log->head);
        CPPUNIT_ASSERT_EQUAL(false, oldHead->isMutable);
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
        tmp = log->bytesStored;
        log->appendAnyType(LOG_ENTRY_TYPE_SEGMENT_HEADER, buf, sizeof(buf));
        CPPUNIT_ASSERT_EQUAL(tmp, log->bytesStored);

        // Internal Segment Checksums don't affect our bytes stored count
        tmp = log->bytesStored;
        log->appendAnyType(LOG_ENTRY_TYPE_SEGMENT_CHECKSUM, buf, sizeof(buf));
        CPPUNIT_ASSERT_EQUAL(tmp, log->bytesStored);

        // All other objects count toward stored bytes
        tmp = log->bytesStored;
        log->appendAnyType(LOG_ENTRY_TYPE_OBJECT, buf, sizeof(buf));
        CPPUNIT_ASSERT_EQUAL(tmp + sizeof(buf), log->bytesStored);
    }

    DISALLOW_COPY_AND_ASSIGN(LogTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(LogTest);

} // namespace RAMCloud
