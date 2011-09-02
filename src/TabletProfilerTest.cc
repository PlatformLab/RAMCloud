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

    // convenience
    typedef TabletProfiler::Bucket Bucket;
    typedef TabletProfiler::Subrange Subrange;
    typedef TabletProfiler::Subrange::BucketHandle BucketHandle;
    typedef TabletProfiler::PartitionCollector PartitionCollector;

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
        tp.track(0x100000000000000L, 8, LogTime(1, 3));
        tp.track(-1, 25, LogTime(1, 4));

        PartitionList *l = tp.getPartitions(1000, 1000, 0, 0);
        CPPUNIT_ASSERT(l != NULL);
        CPPUNIT_ASSERT_EQUAL(1, l->size());
        CPPUNIT_ASSERT_EQUAL(0, l->begin()[0].firstKey);
        CPPUNIT_ASSERT_EQUAL((uint64_t)-1, l->begin()[0].lastKey);
        delete l;

        l = tp.getPartitions(1000, 1000, 984, 1);
        CPPUNIT_ASSERT(l != NULL);
        CPPUNIT_ASSERT_EQUAL(1, l->size());
        delete l;

        l = tp.getPartitions(1000, 1000, 985, 1);
        CPPUNIT_ASSERT(l != NULL);
        CPPUNIT_ASSERT_EQUAL(2, l->size());
        delete l;

        l = tp.getPartitions(1000, 1000, 20, 997);
        CPPUNIT_ASSERT(l != NULL);
        CPPUNIT_ASSERT_EQUAL(1, l->size());
        delete l;

        l = tp.getPartitions(1000, 1000, 20, 998);
        CPPUNIT_ASSERT(l != NULL);
        CPPUNIT_ASSERT_EQUAL(2, l->size());
        delete l;

        // assert if we have < the max number of bytes/referents,
        // then our count is precise. we must force a split so
        // that the walker takes into account parent bytes.
        tp.track(0, TabletProfiler::BUCKET_SPLIT_BYTES, LogTime(1, 5));
        tp.track(0, TabletProfiler::BUCKET_SPLIT_BYTES / 2, LogTime(1, 6));
        l = tp.getPartitions(640 * 1024 * 1024, 1000, 0, 0);
        CPPUNIT_ASSERT(l != NULL);
        CPPUNIT_ASSERT_EQUAL(1, l->size());
        CPPUNIT_ASSERT_EQUAL((*l)[0].minBytes, (*l)[0].maxBytes);
        CPPUNIT_ASSERT_EQUAL((*l)[0].minReferents,  (*l)[0].maxReferents);
    }

    // this is mostly just a wrapper around Subrange::findBucket.
    // the only difference is we have a little bit of subtle logic to
    // cache the last bucket for quick sequential accesses
    void
    test_TabletProfiler_findBucket()
    {
        BucketHandle bh(NULL, 0);
        TabletProfiler tp;
        TabletProfiler bizarroTp;
        CPPUNIT_ASSERT_EQUAL(NULL, tp.findHint.getSubrange());

        // tweak the create times so we can query with earler times
        tp.root->createTime = LogTime(1, 1);
        bizarroTp.root->createTime = LogTime(1, 1);

        /////
        // case1x use the hint
        /////

        // case1a: subrange != NULL && time == NULL && key in range
        bh = BucketHandle(bizarroTp.root, 1);
        tp.findHint = bh;
        CPPUNIT_ASSERT(tp.findBucket(tp.root->bucketWidth, NULL) == bh);
        CPPUNIT_ASSERT(tp.findHint == bh);

        // case1b: subrange != NULL && getCreateTime() <= *time && key in range
        bh = BucketHandle(bizarroTp.root, 1);
        tp.findHint = bh;
        CPPUNIT_ASSERT(tp.findBucket(tp.root->bucketWidth,
            &tp.root->createTime) == bh);
        CPPUNIT_ASSERT(tp.findHint == bh);

        /////
        // case2x do not use the hint
        /////

        // case2a: subrange == NULL
        bh = BucketHandle(tp.root, 1);
        tp.findHint = BucketHandle(NULL, 0);
        CPPUNIT_ASSERT(tp.findBucket(tp.root->bucketWidth, NULL) == bh);
        CPPUNIT_ASSERT(tp.findHint == bh);

        // case2b: subrange != NULL, but time mismatch
        LogTime earlier = LogTime(0, 0);
        bh = BucketHandle(tp.root, 1);
        tp.findHint = BucketHandle(bizarroTp.root, 1);
        CPPUNIT_ASSERT(tp.findBucket(tp.root->bucketWidth, &earlier) == bh);
        CPPUNIT_ASSERT(tp.findHint == bh);

        // case2c: subrange != NULL, time ok, but key range mismatch
        bizarroTp.root->bucketWidth = 1;
        bizarroTp.root->lastKey = bizarroTp.root->numBuckets - 1;
        bh = BucketHandle(tp.root, 1);
        tp.findHint = BucketHandle(bizarroTp.root, 2);
        CPPUNIT_ASSERT(tp.findBucket(tp.root->bucketWidth,
            &tp.root->createTime) == bh);
        CPPUNIT_ASSERT(tp.findHint == bh);
    }

    void
    test_PartitionCollector_constructor()
    {
        PartitionList partList;
        PartitionCollector pc(1024 * 1024, 1000, &partList, 0, 0);

        CPPUNIT_ASSERT_EQUAL(&partList, pc.partitions);
        CPPUNIT_ASSERT_EQUAL(1024 * 1024, pc.maxPartitionBytes);
        CPPUNIT_ASSERT_EQUAL(1000, pc.maxPartitionReferents);
        CPPUNIT_ASSERT_EQUAL(0, pc.nextFirstKey);
        CPPUNIT_ASSERT_EQUAL(0, pc.currentFirstKey);
        CPPUNIT_ASSERT_EQUAL(0, pc.currentKnownBytes);
        CPPUNIT_ASSERT_EQUAL(0, pc.currentKnownReferents);
        CPPUNIT_ASSERT_EQUAL(0, pc.previousPossibleBytes);
        CPPUNIT_ASSERT_EQUAL(0, pc.previousPossibleReferents);
        CPPUNIT_ASSERT_EQUAL(false, pc.isDone);
    }

    void
    test_PartitionCollector_addRangeLeaf()
    {
        PartitionList partList;
        PartitionCollector pc(1024 * 1024, 1000, &partList, 0, 0);

        bool ret = pc.addRangeLeaf(0, 10, 5, 1, 0, 0);
        CPPUNIT_ASSERT_EQUAL(true, ret);
        CPPUNIT_ASSERT_EQUAL(5, pc.currentKnownBytes);
        CPPUNIT_ASSERT_EQUAL(1, pc.currentKnownReferents);
        CPPUNIT_ASSERT_EQUAL(0, pc.previousPossibleBytes);
        CPPUNIT_ASSERT_EQUAL(0, pc.previousPossibleReferents);
        CPPUNIT_ASSERT_EQUAL(0, pc.partitions->size());
        CPPUNIT_ASSERT_EQUAL(11, pc.nextFirstKey);

        // force a new partition by size
        ret = pc.addRangeLeaf(11, 20, 1024*1024, 1, 0, 0);
        CPPUNIT_ASSERT_EQUAL(false, ret);
        CPPUNIT_ASSERT_EQUAL(21, pc.currentFirstKey);
        CPPUNIT_ASSERT_EQUAL(1, pc.partitions->size());

        // force a new partition by referents
        ret = pc.addRangeLeaf(21, 30, 1, 5000, 0, 0);
        CPPUNIT_ASSERT_EQUAL(false, ret);
        CPPUNIT_ASSERT_EQUAL(31, pc.currentFirstKey);
        CPPUNIT_ASSERT_EQUAL(2, pc.partitions->size());
    }

    void
    test_PartitionCollector_done()
    {
        // ensure the last partition is pushed to the list
        PartitionList partList1;
        PartitionCollector pc1(1024 * 1024, 1000, &partList1, 0, 0);

        pc1.addRangeLeaf(0, 10, 1024 * 1024 + 1, 1, 42, 83);
        CPPUNIT_ASSERT_EQUAL(42, pc1.previousPossibleBytes);
        CPPUNIT_ASSERT_EQUAL(83, pc1.previousPossibleReferents);
        pc1.addRangeLeaf(11, 20, 100, 1, 0, 0);
        pc1.done();
        CPPUNIT_ASSERT_EQUAL(2, pc1.partitions->size());
        CPPUNIT_ASSERT_EQUAL(0, pc1.partitions->begin()[0].firstKey);
        CPPUNIT_ASSERT_EQUAL(10, pc1.partitions->begin()[0].lastKey);
        CPPUNIT_ASSERT_EQUAL(11, pc1.partitions->begin()[1].firstKey);
        CPPUNIT_ASSERT_EQUAL((uint64_t)-1, pc1.partitions->begin()[1].lastKey);
        CPPUNIT_ASSERT_EQUAL(true, pc1.isDone);

        // if there were no objects at all, we should push the whole range
        PartitionList partList2;
        PartitionCollector pc2(1024 * 1024, 1000, &partList2, 0, 0);

        pc2.done();
        CPPUNIT_ASSERT_EQUAL(1, pc2.partitions->size());
        CPPUNIT_ASSERT_EQUAL(0, pc2.partitions->begin()[0].firstKey);
        CPPUNIT_ASSERT_EQUAL((uint64_t)-1, pc2.partitions->begin()[0].lastKey);
        CPPUNIT_ASSERT_EQUAL(true, pc2.isDone);
    }

    void
    test_PartitionCollector_pushCurrentTally()
    {
        PartitionList partList;
        PartitionCollector pc(1024 * 1024, 1000, &partList, 0, 0);

        pc.addRangeLeaf(0, 10, 1000, 1, 0, 0);
        pc.pushCurrentTally(8, 1000, 1000, 1, 1);
        CPPUNIT_ASSERT_EQUAL(1, pc.partitions->size());
        CPPUNIT_ASSERT_EQUAL(0, pc.partitions->begin()[0].firstKey);
        CPPUNIT_ASSERT_EQUAL(8, pc.partitions->begin()[0].lastKey);
        CPPUNIT_ASSERT_EQUAL(0, pc.currentKnownBytes);
        CPPUNIT_ASSERT_EQUAL(0, pc.currentKnownReferents);
        CPPUNIT_ASSERT_EQUAL(0, pc.residualMaxBytes);
        CPPUNIT_ASSERT_EQUAL(0, pc.residualMaxReferents);
    }

    void
    test_BucketHandle_constructor()
    {
        Subrange s(BucketHandle(NULL, 0), 0, 10, LogTime(0, 0));
        BucketHandle bh(&s, 1);
        CPPUNIT_ASSERT_EQUAL(&s, bh.subrange);
        CPPUNIT_ASSERT_EQUAL(1, bh.bucketIndex);
    }

    void
    test_BucketHandle_getSubrange()
    {
        Subrange s(BucketHandle(NULL, 0), 0, 10, LogTime(0, 0));
        BucketHandle bh(&s, 1);
        CPPUNIT_ASSERT_EQUAL(&s, bh.getSubrange());
    }

    void
    test_BucketHandle_getBucket()
    {
        BucketHandle bh1(NULL, 1);
        CPPUNIT_ASSERT_EQUAL(NULL, bh1.getBucket());

        Subrange s2(BucketHandle(NULL, 0), 0, 10, LogTime(0, 0));
        BucketHandle bh2(&s2, 0);
        CPPUNIT_ASSERT_EQUAL(&s2.buckets[0], bh2.getBucket());

        Subrange s3(BucketHandle(NULL, 0), 0, 10, LogTime(0, 0));
        BucketHandle bh3(&s3, s3.numBuckets - 1);
        CPPUNIT_ASSERT_EQUAL(&s3.buckets[s3.numBuckets - 1], bh3.getBucket());
    }

    void
    test_BucketHandle_getFirstKey()
    {
        Subrange s1(BucketHandle(NULL, 0), 0, 0x100000000, LogTime(0, 0));
        BucketHandle bh1(&s1, 0);
        CPPUNIT_ASSERT_EQUAL(0, bh1.getFirstKey());

        Subrange s2(BucketHandle(NULL, 0), 0, 0x100000000, LogTime(0, 0));
        BucketHandle bh2(&s2, 1);
        CPPUNIT_ASSERT_EQUAL(0 + s2.bucketWidth, bh2.getFirstKey());

        Subrange s3(BucketHandle(NULL, 0), 0, 0x100000000, LogTime(0, 0));
        BucketHandle bh3(&s3, s3.numBuckets - 1);
        CPPUNIT_ASSERT_EQUAL(0 + s3.bucketWidth * (s3.numBuckets - 1),
            bh3.getFirstKey());
    }

    void
    test_BucketHandle_getLastKey()
    {
        Subrange s1(BucketHandle(NULL, 0), 0, 0x100000000, LogTime(0, 0));
        BucketHandle bh1(&s1, 0);
        CPPUNIT_ASSERT_EQUAL(s1.bucketWidth - 1, bh1.getLastKey());

        Subrange s2(BucketHandle(NULL, 0), 0, 0x100000000, LogTime(0, 0));
        BucketHandle bh2(&s2, 1);
        CPPUNIT_ASSERT_EQUAL(0 + (s2.bucketWidth * 2) - 1, bh2.getLastKey());

        Subrange s3(BucketHandle(NULL, 0), 0, 0x100000000, LogTime(0, 0));
        BucketHandle bh3(&s3, s3.numBuckets - 1);
        CPPUNIT_ASSERT_EQUAL(0 + (s3.bucketWidth * s3.numBuckets) - 1,
            bh3.getLastKey());
    }

    void
    test_Subrange_constructor()
    {
        // case1: full key space
        Subrange s1(BucketHandle(NULL, 2), 0, (uint64_t)-1, LogTime(1, 9));
        CPPUNIT_ASSERT_EQUAL(NULL, s1.parent.subrange);
        CPPUNIT_ASSERT_EQUAL(2, s1.parent.bucketIndex);
        CPPUNIT_ASSERT(s1.bucketWidth != 0);
        CPPUNIT_ASSERT(s1.numBuckets != 0);
        CPPUNIT_ASSERT_EQUAL(0, s1.bucketWidth * s1.numBuckets);
        CPPUNIT_ASSERT_EQUAL(0, s1.firstKey);
        CPPUNIT_ASSERT_EQUAL((uint64_t)-1, s1.lastKey);
        CPPUNIT_ASSERT_EQUAL(0, s1.totalBytes);
        CPPUNIT_ASSERT_EQUAL(0, s1.totalReferents);
        CPPUNIT_ASSERT_EQUAL(0, s1.totalChildren);
        CPPUNIT_ASSERT(LogTime(1, 9) == s1.createTime);
        CPPUNIT_ASSERT(s1.buckets != NULL);

        // case2: all even levels
        Subrange s2(BucketHandle(NULL, 2),
            0x100000000, 0x1ffffffff, LogTime(1, 9));
        CPPUNIT_ASSERT_EQUAL(NULL, s2.parent.subrange);
        CPPUNIT_ASSERT_EQUAL(2, s2.parent.bucketIndex);
        CPPUNIT_ASSERT(s2.bucketWidth != 0);
        CPPUNIT_ASSERT(s2.numBuckets != 0);
        CPPUNIT_ASSERT_EQUAL(0x100000000, s2.bucketWidth * s2.numBuckets);
        CPPUNIT_ASSERT_EQUAL(0x100000000, s2.firstKey);
        CPPUNIT_ASSERT_EQUAL(0x1ffffffff, s2.lastKey);
        CPPUNIT_ASSERT_EQUAL(0, s2.totalBytes);
        CPPUNIT_ASSERT_EQUAL(0, s2.totalReferents);
        CPPUNIT_ASSERT_EQUAL(0, s2.totalChildren);
        CPPUNIT_ASSERT(LogTime(1, 9) == s2.createTime);
        CPPUNIT_ASSERT(s2.buckets != NULL);

        // case3: last level has < BITS_PER_LEVEL
        // XXX- need to make BITS_PER_LEVEL a parameter
    }

    void
    test_Subrange_findBucket()
    {
        LogTime parentTime1(1, 9);
        LogTime parentTime2(1, 11);
        LogTime childTime(1, 12);

        Subrange s(BucketHandle(NULL, 2), 0, (uint64_t)-1, parentTime1);
        Subrange *c = new Subrange(BucketHandle(&s, 1), s.bucketWidth,
            s.bucketWidth * 2 - 1, childTime);
        s.buckets[1].child = c;
        CPPUNIT_ASSERT(BucketHandle(&s, 0) == s.findBucket(0, NULL));

        CPPUNIT_ASSERT(BucketHandle(&s, 1) ==
            s.findBucket(s.bucketWidth, &parentTime1));
        CPPUNIT_ASSERT(BucketHandle(&s, 1) ==
            s.findBucket(s.bucketWidth, &parentTime2));
        CPPUNIT_ASSERT(BucketHandle(c, 0) ==
            s.findBucket(s.bucketWidth, &childTime));
        CPPUNIT_ASSERT(BucketHandle(c, 0) ==
            s.findBucket(s.bucketWidth, NULL));
    }

    void
    test_Subrange_getBucket()
    {
        Subrange s(BucketHandle(NULL, 2), 0, (uint64_t)-1, LogTime(1, 9));
        CPPUNIT_ASSERT_EQUAL(&s.buckets[0], s.getBucket(0));
        CPPUNIT_ASSERT_EQUAL(&s.buckets[s.numBuckets - 1],
            s.getBucket(s.numBuckets - 1));
    }

    void
    test_Subrange_getBucketFirstKey()
    {
        Subrange s(BucketHandle(NULL, 2), 0, (uint64_t)-1, LogTime(1, 9));
        int idx = s.numBuckets / 2;
        CPPUNIT_ASSERT_EQUAL(idx * s.bucketWidth,
            s.getBucketFirstKey(BucketHandle(&s, idx)));
    }

    void
    test_Subrange_getBucketLastKey()
    {
        Subrange s(BucketHandle(NULL, 2), 0, (uint64_t)-1, LogTime(1, 9));
        int idx = s.numBuckets / 2;
        CPPUNIT_ASSERT_EQUAL(idx * s.bucketWidth + s.bucketWidth - 1,
            s.getBucketLastKey(BucketHandle(&s, idx)));
    }

    void
    test_Subrange_isBottom()
    {
        Subrange s(BucketHandle(NULL, 2), 0, (uint64_t)-1, LogTime(1, 9));
        CPPUNIT_ASSERT_EQUAL(false, s.isBottom());
        s.bucketWidth = 1;
        CPPUNIT_ASSERT_EQUAL(true, s.isBottom());
    }

    void
    test_Subrange_partitionWalk()
    {
        Subrange s(BucketHandle(NULL, 2), 0, (uint64_t)-1, LogTime(1, 9));
        Subrange *c = new Subrange(BucketHandle(&s, 1), s.bucketWidth,
            s.bucketWidth * 2 - 1, LogTime(1, 12));

        for (uint32_t i = 0; i < s.numBuckets; i++) {
            if (i == 1) {
                s.buckets[i].child = c;
                s.buckets[i].totalBytes = 8 * 1024 * 1024;
                s.buckets[i].totalReferents = 1000;
            } else {
                s.buckets[i].totalBytes = 0;
                s.buckets[i].totalReferents = 0;
            }
        }
        for (uint32_t i = 0; i < c->numBuckets; i++) {
            c->buckets[i].totalBytes = 4 * 1024 * 1024;
            c->buckets[i].totalReferents = 1;
        }

        PartitionList partList;
        PartitionCollector pc(640 * 1024 * 1024, 10 * 1000 * 1000,
            &partList, 0, 0);
        s.partitionWalk(&pc);
        pc.done();

        CPPUNIT_ASSERT_EQUAL(2, partList.size());
        CPPUNIT_ASSERT_EQUAL(0, partList[0].firstKey);
        CPPUNIT_ASSERT_EQUAL(partList[1].firstKey - 1,
            partList[0].lastKey);
        CPPUNIT_ASSERT_EQUAL((uint64_t)-1, partList[1].lastKey);
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
        CPPUNIT_ASSERT_EQUAL(1, tp.root->buckets[0].totalReferents);
        CPPUNIT_ASSERT_EQUAL(TabletProfiler::BUCKET_SPLIT_BYTES - 1,
            tp.root->totalBytes);
        CPPUNIT_ASSERT_EQUAL(1, tp.root->totalReferents);

        // now force one that will split the bucket
        tp.track(0, 2, LogTime(1, 2));

        // assert the parent hasn't changed
        CPPUNIT_ASSERT_EQUAL(TabletProfiler::BUCKET_SPLIT_BYTES - 1,
            tp.root->buckets[0].totalBytes);
        CPPUNIT_ASSERT_EQUAL(1, tp.root->buckets[0].totalReferents);
        CPPUNIT_ASSERT_EQUAL(TabletProfiler::BUCKET_SPLIT_BYTES - 1,
            tp.root->totalBytes);
        CPPUNIT_ASSERT_EQUAL(1, tp.root->totalReferents);

        // now check that the child is correct
        CPPUNIT_ASSERT_EQUAL(1, tp.root->totalChildren);
        CPPUNIT_ASSERT(tp.root->buckets[0].child != NULL);

        Subrange *child = tp.root->buckets[0].child;
        BucketHandle rootBh = BucketHandle(tp.root, 0);

        CPPUNIT_ASSERT_EQUAL(tp.root->getBucketFirstKey(rootBh),
            child->firstKey);
        CPPUNIT_ASSERT_EQUAL(tp.root->getBucketLastKey(rootBh), child->lastKey);
        CPPUNIT_ASSERT_EQUAL(0, child->totalChildren);
        CPPUNIT_ASSERT_EQUAL(2, child->buckets[0].totalBytes);
        CPPUNIT_ASSERT_EQUAL(1, child->buckets[0].totalReferents);
        CPPUNIT_ASSERT_EQUAL(2, child->totalBytes);
        CPPUNIT_ASSERT_EQUAL(1, child->totalReferents);
    }

    // NB: this test can be sensitive to the constants used.
    void
    test_Subrange_untrack()
    {
        TabletProfiler tp;

        // create a bucket and force it to split
        uint32_t quarter = TabletProfiler::BUCKET_SPLIT_BYTES / 4;
        tp.track(0, quarter, LogTime(1, 1));
        tp.track(0, quarter, LogTime(1, 2));
        tp.track(0, quarter, LogTime(1, 3));
        tp.track(0, quarter, LogTime(1, 4));
        tp.track(0, 100, LogTime(1, 5));
        tp.track(0, 100, LogTime(1, 6));
        tp.track(0, 100, LogTime(1, 7));

        // be sure it split
        Subrange *child = tp.root->buckets[0].child;
        CPPUNIT_ASSERT(child != NULL);
        BucketHandle bh = tp.findBucket(0, NULL);
        CPPUNIT_ASSERT_EQUAL(1, tp.root->totalChildren);
        CPPUNIT_ASSERT_EQUAL(child, bh.getSubrange());
        CPPUNIT_ASSERT_EQUAL(300, child->totalBytes);
        CPPUNIT_ASSERT_EQUAL(3, child->totalReferents);
        CPPUNIT_ASSERT_EQUAL(300, child->buckets[0].totalBytes);
        CPPUNIT_ASSERT_EQUAL(3, child->buckets[0].totalReferents);

        // make sure it doesn't merge before we expect
        tp.untrack(0, 100, LogTime(1, 6));
        CPPUNIT_ASSERT_EQUAL(child, bh.getSubrange());
        CPPUNIT_ASSERT_EQUAL(200, child->totalBytes);
        CPPUNIT_ASSERT_EQUAL(2, child->totalReferents);
        CPPUNIT_ASSERT_EQUAL(200, child->buckets[0].totalBytes);
        CPPUNIT_ASSERT_EQUAL(2, child->buckets[0].totalReferents);

        // now force a merge
        tp.untrack(0, quarter, LogTime(1, 1));
        tp.untrack(0, quarter, LogTime(1, 2));
        tp.untrack(0, quarter, LogTime(1, 3));
        // our code only merges when a leaf is modified
        tp.untrack(0, 100, LogTime(1, 7));

        CPPUNIT_ASSERT_EQUAL(NULL, tp.root->buckets[0].child);
        CPPUNIT_ASSERT_EQUAL(quarter + 100, tp.root->totalBytes);
        CPPUNIT_ASSERT_EQUAL(2, tp.root->totalReferents);
        CPPUNIT_ASSERT_EQUAL(quarter + 100, tp.root->buckets[0].totalBytes);
        CPPUNIT_ASSERT_EQUAL(2, tp.root->buckets[0].totalReferents);
        CPPUNIT_ASSERT_EQUAL(0, tp.root->totalChildren);
    }

    void
    test_Subrange_getters()
    {
        BucketHandle bh(NULL, 0);
        Subrange s(bh, 51, 999, LogTime(134, 53));

        CPPUNIT_ASSERT(LogTime(134, 53) == s.getCreateTime());
        CPPUNIT_ASSERT_EQUAL(51, s.getFirstKey());
        CPPUNIT_ASSERT_EQUAL(999, s.getLastKey());
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(TabletProfilerTest);

} // namespace RAMCloud
