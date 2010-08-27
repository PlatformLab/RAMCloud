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

    RAMCloud::Segment *seg;
    void *segBase;
    BackupClient *backup;

  public:
    SegmentTest() : seg(NULL), segBase(NULL), backup(NULL) { }

    void
    setUp()
    {
        segBase = xmalloc(SEGMENT_SIZE);
        backup = new MultiBackupClient();
        seg = new RAMCloud::Segment(segBase, SEGMENT_SIZE, backup);
        TestInit();
    }

    void
    tearDown()
    {
        free(segBase);
        delete seg;
    }

    void
    TestInit()
    {
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_SIZE, seg->total_bytes);
        CPPUNIT_ASSERT_EQUAL(seg->total_bytes, seg->free_bytes);
        CPPUNIT_ASSERT_EQUAL(seg->total_bytes, seg->tail_bytes);
        CPPUNIT_ASSERT_EQUAL(false, seg->isMutable);
        CPPUNIT_ASSERT_EQUAL(SEGMENT_INVALID_ID, seg->id);
        CPPUNIT_ASSERT_EQUAL(backup, seg->backup);
        CPPUNIT_ASSERT(NULL == seg->prev);
        CPPUNIT_ASSERT(NULL == seg->next);
    }

    void
    TestReady()
    {
        CPPUNIT_ASSERT_EQUAL(SEGMENT_INVALID_ID, seg->id);
        CPPUNIT_ASSERT_EQUAL(false, seg->isMutable);

        seg->ready(0xabcdef42);

        CPPUNIT_ASSERT_EQUAL((uint64_t)0xabcdef42, seg->id);
        CPPUNIT_ASSERT_EQUAL(true, seg->isMutable);
    }

    void
    TestReset()
    {
        TestAppend();
        seg->finalize();
        seg->reset();
        TestInit();
    }

    void 
    TestAppend()
    {
        TestReady();

        const void *p = seg->append("hi!", 4);

        CPPUNIT_ASSERT(p != NULL);
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_SIZE, seg->total_bytes);
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_SIZE - 4, seg->free_bytes);
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_SIZE - 4, seg->tail_bytes);
    }

    void
    TestFullAppend()
    {
        TestReady();

        void *buf = xmalloc(SEGMENT_SIZE);
        const void *p = seg->append(buf, SEGMENT_SIZE);

        CPPUNIT_ASSERT(p != NULL);
        CPPUNIT_ASSERT_EQUAL((uint64_t)SEGMENT_SIZE, seg->total_bytes);
        CPPUNIT_ASSERT_EQUAL((uint64_t)0, seg->free_bytes);
        CPPUNIT_ASSERT_EQUAL((uint64_t)0, seg->tail_bytes);
        CPPUNIT_ASSERT(memcmp(p, buf, SEGMENT_SIZE) == 0);

        free(buf);
    }

    void
    TestAppendUntilFull()
    {
        TestReady();

        uint64_t last = seg->tail_bytes;
        for (int i = 0; ; i++) {
            char chr = (char)i;
            const void *p = seg->append(&chr, 1);
            if (p == NULL)
                break;

            // CPPUNIT_ASSERT is too slow. Using if statements instead saves 3s
            // on my machine (using 3MB segments). -Diego
            if (memcmp(p, &chr, 1) != 0)
                CPPUNIT_ASSERT(false);
            if (seg->tail_bytes + 1 != last)
                CPPUNIT_ASSERT(false);

            last = seg->tail_bytes;
        }

        CPPUNIT_ASSERT_EQUAL((uint64_t)0, seg->free_bytes);
        CPPUNIT_ASSERT_EQUAL((uint64_t)0, seg->tail_bytes);
    }

    void
    TestAppendTooBig()
    {
        TestReady();
        const void *p = seg->append(&p, seg->getLength() + 1);
        CPPUNIT_ASSERT(p == NULL);
    }

    void
    TestFree()
    {
        TestReady();
        seg->append("obj0_version0!", 15);
        seg->append("obj0_version1!", 15);
        CPPUNIT_ASSERT_EQUAL(seg->total_bytes - 30, seg->free_bytes);
        CPPUNIT_ASSERT_EQUAL(seg->total_bytes - 30, seg->tail_bytes);
        seg->free(15);
        CPPUNIT_ASSERT_EQUAL(seg->total_bytes - 15, seg->free_bytes);
        CPPUNIT_ASSERT_EQUAL(seg->total_bytes - 30, seg->tail_bytes);
    }

    void
    TestGetUtilization()
    {
        TestReady();
        CPPUNIT_ASSERT_EQUAL((uint64_t)0, seg->getUtilization());
        
        char buf[10000];
        seg->append(buf, sizeof(buf)); 
        CPPUNIT_ASSERT_EQUAL(sizeof(buf), seg->getUtilization());

        seg->free(1001);
        CPPUNIT_ASSERT_EQUAL(sizeof(buf) - 1001, seg->getUtilization());
    }

    void
    TestCheckRange()
    {
        uintptr_t base = (uintptr_t)segBase;

        CPPUNIT_ASSERT_EQUAL(true, seg->checkRange((const void *)base, 0));
        CPPUNIT_ASSERT_EQUAL(true,
            seg->checkRange((const void *)(base), SEGMENT_SIZE));
        CPPUNIT_ASSERT_EQUAL(true,
            seg->checkRange((const void *)(base + 1), SEGMENT_SIZE - 1));
        CPPUNIT_ASSERT_EQUAL(false,
            seg->checkRange((const void *)(base + 1), SEGMENT_SIZE));
        CPPUNIT_ASSERT_EQUAL(false,
            seg->checkRange((const void *)(base), SEGMENT_SIZE + 1));
        CPPUNIT_ASSERT_EQUAL(false,
            seg->checkRange((const void *)(base - 1), SEGMENT_SIZE));
        CPPUNIT_ASSERT_EQUAL(true,
            seg->checkRange((const void *)(base + SEGMENT_SIZE - 1), 1));
        CPPUNIT_ASSERT_EQUAL(false,
            seg->checkRange((const void *)(base + SEGMENT_SIZE - 1), 2));
    }

    void
    TestFinalize()
    {
        seg->ready(99);
        seg->finalize();
        CPPUNIT_ASSERT_EQUAL(false, seg->isMutable);
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
        Segment *n = new Segment(buf, sizeof(buf), backup);
        Segment *ret = seg->link(n);

        CPPUNIT_ASSERT(seg == ret);
        CPPUNIT_ASSERT(NULL == seg->prev);
        CPPUNIT_ASSERT(n == seg->next);
        CPPUNIT_ASSERT(seg == n->prev);
        CPPUNIT_ASSERT(NULL == n->next);
    
        delete n;
    }

    void
    TestUnlink()
    {
        Segment *ret = seg->unlink();
        CPPUNIT_ASSERT(NULL == ret);
    
        char buf[100];
        Segment *n = new Segment(buf, sizeof(buf), backup);

        seg->link(n);
        ret = seg->unlink();
        CPPUNIT_ASSERT(ret == n);
        CPPUNIT_ASSERT(NULL == seg->next);
        CPPUNIT_ASSERT(NULL == seg->prev);
        CPPUNIT_ASSERT(NULL == n->next);
        CPPUNIT_ASSERT(NULL == n->prev);

        seg->link(n);
        ret = n->unlink();    
        CPPUNIT_ASSERT(NULL == ret);
        CPPUNIT_ASSERT(NULL == seg->next);
        CPPUNIT_ASSERT(NULL == seg->prev);
        CPPUNIT_ASSERT(NULL == n->next);
        CPPUNIT_ASSERT(NULL == n->prev);
    
        delete n;
    }

    DISALLOW_COPY_AND_ASSIGN(SegmentTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(SegmentTest);

} // namespace RAMCloud
