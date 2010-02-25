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
#include <Segment.h>
#include <BackupClient.h>
#include <config.h> // for SEGMENT_SIZE

#include <cppunit/extensions/HelperMacros.h>

namespace RAMCloud {

class SegmentTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(SegmentTest);
    CPPUNIT_TEST(TestInit);
    CPPUNIT_TEST(TestReady);
    CPPUNIT_TEST(TestReset);
    CPPUNIT_TEST(TestAppend);
    CPPUNIT_TEST(TestFullAppend);
    CPPUNIT_TEST(TestAppendTooBig);
    CPPUNIT_TEST(TestAppendUntilFull);
    CPPUNIT_TEST(TestFree);
    CPPUNIT_TEST(TestGetUtilization);
    CPPUNIT_TEST(TestCheckRange);
    CPPUNIT_TEST(TestFinalize);
    CPPUNIT_TEST(TestLink);
    CPPUNIT_TEST(TestUnlink);
    CPPUNIT_TEST_SUITE_END();
    RAMCloud::Segment *Seg;
    void *SegBase;
    BackupClient *Backup;

  public:
    void
    setUp()
    {
        SegBase = xmalloc(SEGMENT_SIZE);
        Backup = new MultiBackupClient();
        Seg = new RAMCloud::Segment(SegBase, SEGMENT_SIZE, Backup);
        TestInit();
    }

    void
    tearDown()
    {
        free(SegBase);
        delete Seg;
    }

    void
    TestInit()
    {
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_SIZE, Seg->total_bytes);
        CPPUNIT_ASSERT_EQUAL(Seg->total_bytes, Seg->free_bytes);
        CPPUNIT_ASSERT_EQUAL(Seg->total_bytes, Seg->tail_bytes);
        CPPUNIT_ASSERT_EQUAL(false, Seg->isMutable);
        CPPUNIT_ASSERT_EQUAL(SEGMENT_INVALID_ID, Seg->id);
        CPPUNIT_ASSERT_EQUAL(Backup, Seg->backup);
        CPPUNIT_ASSERT(NULL == Seg->prev);
        CPPUNIT_ASSERT(NULL == Seg->next);
    }

    void
    TestReady()
    {
        CPPUNIT_ASSERT_EQUAL(SEGMENT_INVALID_ID, Seg->id);
        CPPUNIT_ASSERT_EQUAL(false, Seg->isMutable);

        Seg->ready(0xabcdef42);

        CPPUNIT_ASSERT_EQUAL((uint64_t)0xabcdef42, Seg->id);
        CPPUNIT_ASSERT_EQUAL(true, Seg->isMutable);
    }

    void
    TestReset()
    {
        TestAppend();
        Seg->finalize();
        Seg->reset();
        TestInit();
    }

    void 
    TestAppend()
    {
        TestReady();

        const void *p = Seg->append("hi!", 4);

        CPPUNIT_ASSERT(p != NULL);
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_SIZE, Seg->total_bytes);
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_SIZE - 4, Seg->free_bytes);
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_SIZE - 4, Seg->tail_bytes);
    }

    void
    TestFullAppend()
    {
        TestReady();

        void *buf = xmalloc(SEGMENT_SIZE);
        const void *p = Seg->append(buf, SEGMENT_SIZE);

        CPPUNIT_ASSERT(p != NULL);
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_SIZE, Seg->total_bytes);
        CPPUNIT_ASSERT_EQUAL((uint64_t)0, Seg->free_bytes);
        CPPUNIT_ASSERT_EQUAL((uint64_t)0, Seg->tail_bytes);
        CPPUNIT_ASSERT(memcmp(p, buf, SEGMENT_SIZE) == 0);

        free(buf);
    }

    void
    TestAppendUntilFull()
    {
        TestReady();

        uint64_t last = Seg->tail_bytes;
        for (int i = 0; ; i++) {
            char chr = (char)i;
            const void *p = Seg->append(&chr, 1);
            if (p == NULL)
                break;
            CPPUNIT_ASSERT(memcmp(p, &chr, 1) == 0);
            CPPUNIT_ASSERT_EQUAL(Seg->tail_bytes + 1, last);
            last = Seg->tail_bytes;
        }

        CPPUNIT_ASSERT_EQUAL((uint64_t)0, Seg->free_bytes);
        CPPUNIT_ASSERT_EQUAL((uint64_t)0, Seg->tail_bytes);
    }

    void
    TestAppendTooBig()
    {
        TestReady();
        const void *p = Seg->append(&p, Seg->getLength() + 1);
        CPPUNIT_ASSERT(p == NULL);
    }

    void
    TestFree()
    {
        TestReady();
        Seg->append("obj0_version0!", 15);
        Seg->append("obj0_version1!", 15);
        CPPUNIT_ASSERT_EQUAL(Seg->total_bytes - 30, Seg->free_bytes);
        CPPUNIT_ASSERT_EQUAL(Seg->total_bytes - 30, Seg->tail_bytes);
        Seg->free(15);
        CPPUNIT_ASSERT_EQUAL(Seg->total_bytes - 15, Seg->free_bytes);
        CPPUNIT_ASSERT_EQUAL(Seg->total_bytes - 30, Seg->tail_bytes);
    }

    void
    TestGetUtilization()
    {
        TestReady();
        CPPUNIT_ASSERT_EQUAL((uint64_t)0, Seg->getUtilization());
        
        char buf[10000];
        Seg->append(buf, sizeof(buf)); 
        CPPUNIT_ASSERT_EQUAL(sizeof(buf), Seg->getUtilization());

        Seg->free(1001);
        CPPUNIT_ASSERT_EQUAL(sizeof(buf) - 1001, Seg->getUtilization());
    }

    void
    TestCheckRange()
    {
        uintptr_t base = (uintptr_t)SegBase;

        CPPUNIT_ASSERT_EQUAL(true, Seg->checkRange((const void *)base, 0));
        CPPUNIT_ASSERT_EQUAL(true,
            Seg->checkRange((const void *)(base), SEGMENT_SIZE));
        CPPUNIT_ASSERT_EQUAL(true,
            Seg->checkRange((const void *)(base + 1), SEGMENT_SIZE - 1));
        CPPUNIT_ASSERT_EQUAL(false,
            Seg->checkRange((const void *)(base + 1), SEGMENT_SIZE));
        CPPUNIT_ASSERT_EQUAL(false,
            Seg->checkRange((const void *)(base), SEGMENT_SIZE + 1));
        CPPUNIT_ASSERT_EQUAL(false,
            Seg->checkRange((const void *)(base - 1), SEGMENT_SIZE));
        CPPUNIT_ASSERT_EQUAL(true,
            Seg->checkRange((const void *)(base + SEGMENT_SIZE - 1), 1));
        CPPUNIT_ASSERT_EQUAL(false,
            Seg->checkRange((const void *)(base + SEGMENT_SIZE - 1), 2));
    }

    void
    TestFinalize()
    {
        Seg->ready(99);
        Seg->finalize();
        CPPUNIT_ASSERT_EQUAL(false, Seg->isMutable);
    }

    //XXX- future work
    void
    TestRestore()
    {
    }

    void
    TestLink()
    {
        char buf[100];
        Segment *n = new Segment(buf, sizeof(buf), Backup);
        Segment *ret = Seg->link(n);

        CPPUNIT_ASSERT(Seg == ret);
        CPPUNIT_ASSERT(NULL == Seg->prev);
        CPPUNIT_ASSERT(n == Seg->next);
        CPPUNIT_ASSERT(Seg == n->prev);
        CPPUNIT_ASSERT(NULL == n->next);
    
        delete n;
    }

    void
    TestUnlink()
    {
        Segment *ret = Seg->unlink();
        CPPUNIT_ASSERT(NULL == ret);
    
        char buf[100];
        Segment *n = new Segment(buf, sizeof(buf), Backup);

        Seg->link(n);
        ret = Seg->unlink();
        CPPUNIT_ASSERT(ret == n);
        CPPUNIT_ASSERT(NULL == Seg->next);
        CPPUNIT_ASSERT(NULL == Seg->prev);
        CPPUNIT_ASSERT(NULL == n->next);
        CPPUNIT_ASSERT(NULL == n->prev);

        Seg->link(n);
        ret = n->unlink();    
        CPPUNIT_ASSERT(NULL == ret);
        CPPUNIT_ASSERT(NULL == Seg->next);
        CPPUNIT_ASSERT(NULL == Seg->prev);
        CPPUNIT_ASSERT(NULL == n->next);
        CPPUNIT_ASSERT(NULL == n->prev);
    
        delete n;
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(SegmentTest);

} // namespace RAMCloud
