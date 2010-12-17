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

#include "Log.h"
#include "TabletProfiler.h"

namespace RAMCloud {

/**
 * Unit tests for TabletProfiler.
 */
class TabletProfilerTest : public CppUnit::TestFixture {

    DISALLOW_COPY_AND_ASSIGN(TabletProfilerTest); // NOLINT

    CPPUNIT_TEST_SUITE(TabletProfilerTest);
    CPPUNIT_TEST(test_TabletProfiler_constructor);
    CPPUNIT_TEST(test_TabletProfiler_track);
    CPPUNIT_TEST(test_TabletProfiler_untrack);
    CPPUNIT_TEST(test_TabletProfiler_getPartitions);
    CPPUNIT_TEST(test_TabletProfiler_constructor);
    CPPUNIT_TEST(test_TabletProfiler_track);
    CPPUNIT_TEST(test_TabletProfiler_untrack);
    CPPUNIT_TEST(test_TabletProfiler_getPartitions);
    CPPUNIT_TEST(test_TabletProfiler_findBucket);
    CPPUNIT_TEST(test_PartitionCollector_constructor);
    CPPUNIT_TEST(test_PartitionCollector_addRangeLeaf);
    CPPUNIT_TEST(test_PartitionCollector_addRangeNonLeaf);
    CPPUNIT_TEST(test_PartitionCollector_done);
    CPPUNIT_TEST(test_PartitionCollector_pushCurrentTally);
    CPPUNIT_TEST(test_BucketHandle_constructor);
    CPPUNIT_TEST(test_BucketHandle_getSubrange);
    CPPUNIT_TEST(test_BucketHandle_getBucket);
    CPPUNIT_TEST(test_BucketHandle_getFirstKey);
    CPPUNIT_TEST(test_BucketHandle_getLastKey);
    CPPUNIT_TEST(test_Subrange_constructor);
    CPPUNIT_TEST(test_Subrange_findBucket);
    CPPUNIT_TEST(test_Subrange_getBucket);
    CPPUNIT_TEST(test_Subrange_getBucketFirstKey);
    CPPUNIT_TEST(test_Subrange_getBucketLastKey);
    CPPUNIT_TEST(test_Subrange_isBottom);
    CPPUNIT_TEST(test_Subrange_partitionWalk);
    CPPUNIT_TEST(test_Subrange_track);
    CPPUNIT_TEST(test_Subrange_untrack);
    CPPUNIT_TEST(test_Subrange_getters);
    CPPUNIT_TEST_SUITE_END();

  public:
    TabletProfilerTest() {}

    void
    test_TabletProfiler_constructor()
    {
        TabletProfiler tp;
        CPPUNIT_ASSERT(tp.root != NULL);
        CPPUNIT_ASSERT(tp.root->createTime == LogTime(0, 0));
        CPPUNIT_ASSERT(tp.lastTracked == LogTime(0, 0));
        CPPUNIT_ASSERT_EQUAL(0, tp.totalTracked);
        CPPUNIT_ASSERT_EQUAL(0, tp.totalTrackedBytes);
    }

    void
    test_TabletProfiler_track()
    {
        TabletProfiler tp;
        tp.track(0, 7, LogTime(1, 2));
        CPPUNIT_ASSERT(tp.lastTracked == LogTime(1, 2));
        CPPUNIT_ASSERT_EQUAL(1, tp.totalTracked);
        CPPUNIT_ASSERT_EQUAL(7, tp.totalTrackedBytes);
    }

    void
    test_TabletProfiler_untrack()
    {
        TabletProfiler tp;
        tp.track(0, 7, LogTime(1, 2));
        tp.track(0, 8, LogTime(1, 3));
        tp.untrack(0, 7, LogTime(1, 2));
        CPPUNIT_ASSERT_EQUAL(1, tp.totalTracked);
        CPPUNIT_ASSERT_EQUAL(8, tp.totalTrackedBytes);
    }

    void
    test_TabletProfiler_getPartitions()
    {
        TabletProfiler tp;
        tp.track(0, 7, LogTime(1, 2));
        tp.track(0, 8, LogTime(1, 3));
        PartitionList *l = tp.getPartitions(1000, 1000);

        CPPUNIT_ASSERT(l != NULL);
        CPPUNIT_ASSERT(l->size() == 1);
        CPPUNIT_ASSERT_EQUAL(0, l->begin()[0].firstKey);
        CPPUNIT_ASSERT_EQUAL((uint64_t)-1, l->begin()[0].lastKey);

        delete l;
    }

    void
    test_TabletProfiler_findBucket()
    {
        TabletProfiler tp;
        CPPUNIT_ASSERT_EQUAL(NULL, tp.findHint.getSubrange());

        // XXX what to test here?
    }

    void
    test_PartitionCollector_constructor()
    {
    }

    void
    test_PartitionCollector_addRangeLeaf()
    {
    }

    void
    test_PartitionCollector_addRangeNonLeaf()
    {
    }

    void
    test_PartitionCollector_done()
    {
    }

    void
    test_PartitionCollector_pushCurrentTally()
    {
    }

    void
    test_BucketHandle_constructor()
    {
    }

    void
    test_BucketHandle_getSubrange()
    {
    }

    void
    test_BucketHandle_getBucket()
    {
    }

    void
    test_BucketHandle_getFirstKey()
    {
    }

    void
    test_BucketHandle_getLastKey()
    {
    }

    void
    test_Subrange_constructor()
    {
    }

    void
    test_Subrange_findBucket()
    {
    }

    void
    test_Subrange_getBucket()
    {
    }

    void
    test_Subrange_getBucketFirstKey()
    {
    }

    void
    test_Subrange_getBucketLastKey()
    {
    }

    void
    test_Subrange_isBottom()
    {
    }

    void
    test_Subrange_partitionWalk()
    {
    }

    void
    test_Subrange_track()
    {
        TabletProfiler tp;

        // first do one insert that won't split bucket 0
        tp.track(0, TabletProfiler::BUCKET_SPLIT_BYTES - 1, LogTime(1, 1)); 
        CPPUNIT_ASSERT(!tp.root->isBottom());
        CPPUNIT_ASSERT_EQUAL(NULL, tp.root->buckets[0].child);
        CPPUNIT_ASSERT_EQUAL(TabletProfiler::BUCKET_SPLIT_BYTES - 1,
            tp.root->buckets[0].totalBytes);
        CPPUNIT_ASSERT_EQUAL(1, tp.root->buckets[0].totalReferants);
        CPPUNIT_ASSERT_EQUAL(TabletProfiler::BUCKET_SPLIT_BYTES - 1,
            tp.root->totalBytes);
        CPPUNIT_ASSERT_EQUAL(1, tp.root->totalReferants);

        // now force one that will split the bucket
        tp.track(0, 2, LogTime(1,2));

        // assert the parent hasn't changed
        CPPUNIT_ASSERT_EQUAL(TabletProfiler::BUCKET_SPLIT_BYTES - 1,
            tp.root->buckets[0].totalBytes);
        CPPUNIT_ASSERT_EQUAL(1, tp.root->buckets[0].totalReferants);
        CPPUNIT_ASSERT_EQUAL(TabletProfiler::BUCKET_SPLIT_BYTES - 1,
            tp.root->totalBytes);
        CPPUNIT_ASSERT_EQUAL(1, tp.root->totalReferants);

        // now check that the child is correct
        CPPUNIT_ASSERT_EQUAL(1, tp.root->totalChildren);
        CPPUNIT_ASSERT(tp.root->buckets[0].child != NULL);

        TabletProfiler::Subrange *child = tp.root->buckets[0].child;
        TabletProfiler::Subrange::BucketHandle rootBh =
            TabletProfiler::Subrange::BucketHandle(tp.root, 0);

        CPPUNIT_ASSERT_EQUAL(tp.root->getBucketFirstKey(rootBh), child->firstKey);
        CPPUNIT_ASSERT_EQUAL(tp.root->getBucketLastKey(rootBh), child->lastKey);
        CPPUNIT_ASSERT_EQUAL(0, child->totalChildren);
        CPPUNIT_ASSERT_EQUAL(2, child->buckets[0].totalBytes);
        CPPUNIT_ASSERT_EQUAL(1, child->buckets[0].totalReferants);
        CPPUNIT_ASSERT_EQUAL(2, child->totalBytes);
        CPPUNIT_ASSERT_EQUAL(1, child->totalReferants);
    }

    // NB: this test can be sensitive to the constants used.
    void
    test_Subrange_untrack()
    {
        TabletProfiler tp;

        // create a bucket and force it to split
        uint64_t quarter = TabletProfiler::BUCKET_SPLIT_BYTES / 4;
        tp.track(0, quarter, LogTime(1, 1));
        tp.track(0, quarter, LogTime(1, 2));
        tp.track(0, quarter, LogTime(1, 3));
        tp.track(0, quarter, LogTime(1, 4));
        tp.track(0, 100, LogTime(1, 5));
        tp.track(0, 100, LogTime(1, 6));
        tp.track(0, 100, LogTime(1, 7));

        // be sure it split
        TabletProfiler::Subrange *child = tp.root->buckets[0].child;
        CPPUNIT_ASSERT(child != NULL);
        TabletProfiler::Subrange::BucketHandle bh = tp.findBucket(0, NULL);
        CPPUNIT_ASSERT_EQUAL(1, tp.root->totalChildren);
        CPPUNIT_ASSERT_EQUAL(child, bh.getSubrange());
        CPPUNIT_ASSERT_EQUAL(300, child->totalBytes);
        CPPUNIT_ASSERT_EQUAL(3, child->totalReferants);
        CPPUNIT_ASSERT_EQUAL(300, child->buckets[0].totalBytes);
        CPPUNIT_ASSERT_EQUAL(3, child->buckets[0].totalReferants);

        // make sure it doesn't merge before we expect
        tp.untrack(0, 100, LogTime(1, 6)); 
        CPPUNIT_ASSERT_EQUAL(child, bh.getSubrange());
        CPPUNIT_ASSERT_EQUAL(200, child->totalBytes);
        CPPUNIT_ASSERT_EQUAL(2, child->totalReferants);
        CPPUNIT_ASSERT_EQUAL(200, child->buckets[0].totalBytes);
        CPPUNIT_ASSERT_EQUAL(2, child->buckets[0].totalReferants);

        // now force a merge
        tp.untrack(0, quarter, LogTime(1, 1));
        tp.untrack(0, quarter, LogTime(1, 2));
        tp.untrack(0, quarter, LogTime(1, 3));
        // our code only merges when a leaf is modified
        tp.untrack(0, 100, LogTime(1, 7));

        CPPUNIT_ASSERT_EQUAL(NULL, tp.root->buckets[0].child);
        CPPUNIT_ASSERT_EQUAL(quarter + 100, tp.root->totalBytes);
        CPPUNIT_ASSERT_EQUAL(2, tp.root->totalReferants);
        CPPUNIT_ASSERT_EQUAL(quarter + 100, tp.root->buckets[0].totalBytes);
        CPPUNIT_ASSERT_EQUAL(2, tp.root->buckets[0].totalReferants);
        CPPUNIT_ASSERT_EQUAL(0, tp.root->totalChildren);
    }

    void
    test_Subrange_getters()
    {
        TabletProfiler::Subrange::BucketHandle bh(NULL, NULL);
        TabletProfiler::Subrange s(bh, 51, 999, LogTime(134, 53)); 

        CPPUNIT_ASSERT(LogTime(134, 53) == s.getCreateTime());
        CPPUNIT_ASSERT_EQUAL(51, s.getFirstKey());
        CPPUNIT_ASSERT_EQUAL(999, s.getLastKey());
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(TabletProfilerTest);

} // namespace RAMCloud
