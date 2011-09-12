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
class TabletProfilerTest : public ::testing::Test {
  public:

    // convenience
    typedef TabletProfiler::Bucket Bucket;
    typedef TabletProfiler::Subrange Subrange;
    typedef TabletProfiler::Subrange::BucketHandle BucketHandle;
    typedef TabletProfiler::PartitionCollector PartitionCollector;

    TabletProfilerTest() {}

    DISALLOW_COPY_AND_ASSIGN(TabletProfilerTest);
};

TEST_F(TabletProfilerTest, TabletProfiler_constructor) {
    TabletProfiler tp;
    EXPECT_TRUE(tp.root != NULL);
    EXPECT_EQ(tp.root->createTime, LogTime(0, 0));
    EXPECT_EQ(tp.lastTracked, LogTime(0, 0));
    EXPECT_EQ(0U, tp.totalTracked);
    EXPECT_EQ(0U, tp.totalTrackedBytes);
}

TEST_F(TabletProfilerTest, TabletProfiler_track) {
    TabletProfiler tp;
    tp.track(0, 7, LogTime(1, 2));
    EXPECT_EQ(tp.lastTracked, LogTime(1, 2));
    EXPECT_EQ(1U, tp.totalTracked);
    EXPECT_EQ(7U, tp.totalTrackedBytes);
}

TEST_F(TabletProfilerTest, TabletProfiler_untrack) {
    TabletProfiler tp;
    tp.track(0, 7, LogTime(1, 2));
    tp.track(0, 8, LogTime(1, 3));
    tp.untrack(0, 7, LogTime(1, 2));
    EXPECT_EQ(1U, tp.totalTracked);
    EXPECT_EQ(8U, tp.totalTrackedBytes);
}

TEST_F(TabletProfilerTest, TabletProfiler_getPartitions) {
    TabletProfiler tp;
    tp.track(0, 7, LogTime(1, 2));
    tp.track(0x100000000000000L, 8, LogTime(1, 3));
    tp.track(-1, 25, LogTime(1, 4));

    PartitionList *l = tp.getPartitions(1000, 1000, 0, 0);
    EXPECT_TRUE(l != NULL);
    EXPECT_EQ(1U, l->size());
    EXPECT_EQ(0U, l->begin()[0].firstKey);
    EXPECT_EQ((uint64_t)-1, l->begin()[0].lastKey);
    delete l;

    l = tp.getPartitions(1000, 1000, 984, 1);
    EXPECT_TRUE(l != NULL);
    EXPECT_EQ(1U, l->size());
    delete l;

    l = tp.getPartitions(1000, 1000, 985, 1);
    EXPECT_TRUE(l != NULL);
    EXPECT_EQ(2U, l->size());
    delete l;

    l = tp.getPartitions(1000, 1000, 20, 997);
    EXPECT_TRUE(l != NULL);
    EXPECT_EQ(1U, l->size());
    delete l;

    l = tp.getPartitions(1000, 1000, 20, 998);
    EXPECT_TRUE(l != NULL);
    EXPECT_EQ(2U, l->size());
    delete l;

    // assert if we have < the max number of bytes/referants,
    // then our count is precise. we must force a split so
    // that the walker takes into account parent bytes.
    tp.track(0, TabletProfiler::BUCKET_SPLIT_BYTES, LogTime(1, 5));
    tp.track(0, TabletProfiler::BUCKET_SPLIT_BYTES / 2, LogTime(1, 6));
    l = tp.getPartitions(640 * 1024 * 1024, 1000, 0, 0);
    EXPECT_TRUE(l != NULL);
    EXPECT_EQ(1U, l->size());
    EXPECT_EQ((*l)[0].minBytes, (*l)[0].maxBytes);
    EXPECT_EQ((*l)[0].minReferents,  (*l)[0].maxReferents);
}

// this is mostly just a wrapper around Subrange::findBucket.
// the only difference is we have a little bit of subtle logic to
// cache the last bucket for quick sequential accesses
TEST_F(TabletProfilerTest, TabletProfiler_findBucket) {
    BucketHandle bh(NULL, 0);
    TabletProfiler tp;
    TabletProfiler bizarroTp;
    EXPECT_TRUE(NULL == tp.findHint.getSubrange());

    // tweak the create times so we can query with earler times
    tp.root->createTime = LogTime(1, 1);
    bizarroTp.root->createTime = LogTime(1, 1);

    /////
    // case1x use the hint
    /////

    // case1a: subrange != NULL && time == NULL && key in range
    bh = BucketHandle(bizarroTp.root, 1);
    tp.findHint = bh;
    EXPECT_EQ(tp.findBucket(tp.root->bucketWidth, NULL), bh);
    EXPECT_EQ(tp.findHint, bh);

    // case1b: subrange != NULL && getCreateTime() <= *time && key in range
    bh = BucketHandle(bizarroTp.root, 1);
    tp.findHint = bh;
    EXPECT_EQ(tp.findBucket(tp.root->bucketWidth, &tp.root->createTime), bh);
    EXPECT_EQ(tp.findHint, bh);

    /////
    // case2x do not use the hint
    /////

    // case2a: subrange == NULL
    bh = BucketHandle(tp.root, 1);
    tp.findHint = BucketHandle(NULL, 0);
    EXPECT_EQ(tp.findBucket(tp.root->bucketWidth, NULL), bh);
    EXPECT_EQ(tp.findHint, bh);

    // case2b: subrange != NULL, but time mismatch
    LogTime earlier = LogTime(0, 0);
    bh = BucketHandle(tp.root, 1);
    tp.findHint = BucketHandle(bizarroTp.root, 1);
    EXPECT_EQ(tp.findBucket(tp.root->bucketWidth, &earlier), bh);
    EXPECT_EQ(tp.findHint, bh);

    // case2c: subrange != NULL, time ok, but key range mismatch
    bizarroTp.root->bucketWidth = 1;
    bizarroTp.root->lastKey = bizarroTp.root->numBuckets - 1;
    bh = BucketHandle(tp.root, 1);
    tp.findHint = BucketHandle(bizarroTp.root, 2);
    EXPECT_EQ(tp.findBucket(tp.root->bucketWidth,
        &tp.root->createTime), bh);
    EXPECT_EQ(tp.findHint, bh);
}

TEST_F(TabletProfilerTest, PartitionCollector_constructor) {
    PartitionList partList;
    PartitionCollector pc(1024 * 1024, 1000, &partList, 0, 0);

    EXPECT_EQ(&partList, pc.partitions);
    EXPECT_EQ(1024U * 1024U, pc.maxPartitionBytes);
    EXPECT_EQ(1000U, pc.maxPartitionReferents);
    EXPECT_EQ(0U, pc.nextFirstKey);
    EXPECT_EQ(0U, pc.currentFirstKey);
    EXPECT_EQ(0U, pc.currentKnownBytes);
    EXPECT_EQ(0U, pc.currentKnownReferents);
    EXPECT_EQ(0U, pc.previousPossibleBytes);
    EXPECT_EQ(0U, pc.previousPossibleReferents);
    EXPECT_FALSE(pc.isDone);
}

TEST_F(TabletProfilerTest, PartitionCollector_addRangeLeaf) {
    PartitionList partList;
    PartitionCollector pc(1024 * 1024, 1000, &partList, 0, 0);

    bool ret = pc.addRangeLeaf(0, 10, 5, 1, 0, 0);
    EXPECT_TRUE(ret);
    EXPECT_EQ(5U, pc.currentKnownBytes);
    EXPECT_EQ(1U, pc.currentKnownReferents);
    EXPECT_EQ(0U, pc.previousPossibleBytes);
    EXPECT_EQ(0U, pc.previousPossibleReferents);
    EXPECT_EQ(0U, pc.partitions->size());
    EXPECT_EQ(11U, pc.nextFirstKey);

    // force a new partition by size
    ret = pc.addRangeLeaf(11, 20, 1024*1024, 1, 0, 0);
    EXPECT_FALSE(ret);
    EXPECT_EQ(21U, pc.currentFirstKey);
    EXPECT_EQ(1U, pc.partitions->size());

    // force a new partition by referants
    ret = pc.addRangeLeaf(21, 30, 1, 5000, 0, 0);
    EXPECT_FALSE(ret);
    EXPECT_EQ(31U, pc.currentFirstKey);
    EXPECT_EQ(2U, pc.partitions->size());
}

TEST_F(TabletProfilerTest, PartitionCollector_done) {
    // ensure the last partition is pushed to the list
    PartitionList partList1;
    PartitionCollector pc1(1024 * 1024, 1000, &partList1, 0, 0);

    pc1.addRangeLeaf(0, 10, 1024 * 1024 + 1, 1, 42, 83);
    EXPECT_EQ(42U, pc1.previousPossibleBytes);
    EXPECT_EQ(83U, pc1.previousPossibleReferents);
    pc1.addRangeLeaf(11, 20, 100, 1, 0, 0);
    pc1.done();
    EXPECT_EQ(2U, pc1.partitions->size());
    EXPECT_EQ(0U, pc1.partitions->begin()[0].firstKey);
    EXPECT_EQ(10U, pc1.partitions->begin()[0].lastKey);
    EXPECT_EQ(11U, pc1.partitions->begin()[1].firstKey);
    EXPECT_EQ((uint64_t)-1, pc1.partitions->begin()[1].lastKey);
    EXPECT_TRUE(pc1.isDone);

    // if there were no objects at all, we should push the whole range
    PartitionList partList2;
    PartitionCollector pc2(1024 * 1024, 1000, &partList2, 0, 0);

    pc2.done();
    EXPECT_EQ(1U, pc2.partitions->size());
    EXPECT_EQ(0U, pc2.partitions->begin()[0].firstKey);
    EXPECT_EQ((uint64_t)-1, pc2.partitions->begin()[0].lastKey);
    EXPECT_TRUE(pc2.isDone);
}

TEST_F(TabletProfilerTest, PartitionCollector_pushCurrentTally) {
    PartitionList partList;
    PartitionCollector pc(1024 * 1024, 1000, &partList, 0, 0);

    pc.addRangeLeaf(0, 10, 1000, 1, 0, 0);
    pc.pushCurrentTally(8, 1000, 1000, 1, 1);
    EXPECT_EQ(1U, pc.partitions->size());
    EXPECT_EQ(0U, pc.partitions->begin()[0].firstKey);
    EXPECT_EQ(8U, pc.partitions->begin()[0].lastKey);
    EXPECT_EQ(0U, pc.currentKnownBytes);
    EXPECT_EQ(0U, pc.currentKnownReferents);
    EXPECT_EQ(0U, pc.residualMaxBytes);
    EXPECT_EQ(0U, pc.residualMaxReferents);
}

TEST_F(TabletProfilerTest, BucketHandle_constructor) {
    Subrange s(BucketHandle(NULL, 0), 0, 10, LogTime(0, 0));
    BucketHandle bh(&s, 1);
    EXPECT_EQ(&s, bh.subrange);
    EXPECT_EQ(1, bh.bucketIndex);
}

TEST_F(TabletProfilerTest, BucketHandle_getSubrange) {
    Subrange s(BucketHandle(NULL, 0), 0, 10, LogTime(0, 0));
    BucketHandle bh(&s, 1);
    EXPECT_EQ(&s, bh.getSubrange());
}

TEST_F(TabletProfilerTest, BucketHandle_getBucket) {
    BucketHandle bh1(NULL, 1);
    EXPECT_TRUE(NULL == bh1.getBucket());

    Subrange s2(BucketHandle(NULL, 0), 0, 10, LogTime(0, 0));
    BucketHandle bh2(&s2, 0);
    EXPECT_EQ(&s2.buckets[0], bh2.getBucket());

    Subrange s3(BucketHandle(NULL, 0), 0, 10, LogTime(0, 0));
    BucketHandle bh3(&s3, s3.numBuckets - 1);
    EXPECT_EQ(&s3.buckets[s3.numBuckets - 1], bh3.getBucket());
}

TEST_F(TabletProfilerTest, BucketHandle_getFirstKey) {
    Subrange s1(BucketHandle(NULL, 0), 0, 0x100000000, LogTime(0, 0));
    BucketHandle bh1(&s1, 0);
    EXPECT_EQ(0U, bh1.getFirstKey());

    Subrange s2(BucketHandle(NULL, 0), 0, 0x100000000, LogTime(0, 0));
    BucketHandle bh2(&s2, 1);
    EXPECT_EQ(0 + s2.bucketWidth, bh2.getFirstKey());

    Subrange s3(BucketHandle(NULL, 0), 0, 0x100000000, LogTime(0, 0));
    BucketHandle bh3(&s3, s3.numBuckets - 1);
    EXPECT_EQ(0 + s3.bucketWidth * (s3.numBuckets - 1),
        bh3.getFirstKey());
}

TEST_F(TabletProfilerTest, BucketHandle_getLastKey) {
    Subrange s1(BucketHandle(NULL, 0), 0, 0x100000000, LogTime(0, 0));
    BucketHandle bh1(&s1, 0);
    EXPECT_EQ(s1.bucketWidth - 1, bh1.getLastKey());

    Subrange s2(BucketHandle(NULL, 0), 0, 0x100000000, LogTime(0, 0));
    BucketHandle bh2(&s2, 1);
    EXPECT_EQ(0 + (s2.bucketWidth * 2) - 1, bh2.getLastKey());

    Subrange s3(BucketHandle(NULL, 0), 0, 0x100000000, LogTime(0, 0));
    BucketHandle bh3(&s3, s3.numBuckets - 1);
    EXPECT_EQ(0 + (s3.bucketWidth * s3.numBuckets) - 1,
        bh3.getLastKey());
}

TEST_F(TabletProfilerTest, Subrange_constructor) {
    // case1: full key space
    Subrange s1(BucketHandle(NULL, 2), 0, (uint64_t)-1, LogTime(1, 9));
    EXPECT_TRUE(NULL == s1.parent.subrange);
    EXPECT_EQ(2, s1.parent.bucketIndex);
    EXPECT_NE(0U, s1.bucketWidth);
    EXPECT_NE(0U, s1.numBuckets);
    EXPECT_EQ(0U, s1.bucketWidth * s1.numBuckets);
    EXPECT_EQ(0U, s1.firstKey);
    EXPECT_EQ((uint64_t)-1, s1.lastKey);
    EXPECT_EQ(0U, s1.totalBytes);
    EXPECT_EQ(0U, s1.totalReferents);
    EXPECT_EQ(0U, s1.totalChildren);
    EXPECT_EQ(LogTime(1, 9), s1.createTime);
    EXPECT_TRUE(s1.buckets != NULL);

    // case2: all even levels
    Subrange s2(BucketHandle(NULL, 2),
        0x100000000, 0x1ffffffff, LogTime(1, 9));
    EXPECT_TRUE(NULL == s2.parent.subrange);
    EXPECT_EQ(2, s2.parent.bucketIndex);
    EXPECT_NE(0U, s2.bucketWidth);
    EXPECT_NE(0U, s2.numBuckets);
    EXPECT_EQ(0x100000000U, s2.bucketWidth * s2.numBuckets);
    EXPECT_EQ(0x100000000U, s2.firstKey);
    EXPECT_EQ(0x1ffffffffU, s2.lastKey);
    EXPECT_EQ(0U, s2.totalBytes);
    EXPECT_EQ(0U, s2.totalReferents);
    EXPECT_EQ(0U, s2.totalChildren);
    EXPECT_EQ(LogTime(1, 9), s2.createTime);
    EXPECT_TRUE(s2.buckets != NULL);

    // case3: last level has < BITS_PER_LEVEL
    // XXX- need to make BITS_PER_LEVEL a parameter
}

TEST_F(TabletProfilerTest, Subrange_findBucket) {
    LogTime parentTime1(1, 9);
    LogTime parentTime2(1, 11);
    LogTime childTime(1, 12);

    Subrange s(BucketHandle(NULL, 2), 0, (uint64_t)-1, parentTime1);
    Subrange *c = new Subrange(BucketHandle(&s, 1), s.bucketWidth,
        s.bucketWidth * 2 - 1, childTime);
    s.buckets[1].child = c;
    EXPECT_EQ(BucketHandle(&s, 0), s.findBucket(0, NULL));

    EXPECT_EQ(BucketHandle(&s, 1),
        s.findBucket(s.bucketWidth, &parentTime1));
    EXPECT_EQ(BucketHandle(&s, 1),
        s.findBucket(s.bucketWidth, &parentTime2));
    EXPECT_EQ(BucketHandle(c, 0),
        s.findBucket(s.bucketWidth, &childTime));
    EXPECT_EQ(BucketHandle(c, 0),
        s.findBucket(s.bucketWidth, NULL));
}

TEST_F(TabletProfilerTest, Subrange_getBucket) {
    Subrange s(BucketHandle(NULL, 2), 0, (uint64_t)-1, LogTime(1, 9));
    EXPECT_EQ(&s.buckets[0], s.getBucket(0));
    EXPECT_EQ(&s.buckets[s.numBuckets - 1],
        s.getBucket(s.numBuckets - 1));
}

TEST_F(TabletProfilerTest, Subrange_getBucketFirstKey) {
    Subrange s(BucketHandle(NULL, 2), 0, (uint64_t)-1, LogTime(1, 9));
    int idx = s.numBuckets / 2;
    EXPECT_EQ(idx * s.bucketWidth,
        s.getBucketFirstKey(BucketHandle(&s, idx)));
}

TEST_F(TabletProfilerTest, Subrange_getBucketLastKey) {
    Subrange s(BucketHandle(NULL, 2), 0, (uint64_t)-1, LogTime(1, 9));
    int idx = s.numBuckets / 2;
    EXPECT_EQ(idx * s.bucketWidth + s.bucketWidth - 1,
        s.getBucketLastKey(BucketHandle(&s, idx)));
}

TEST_F(TabletProfilerTest, Subrange_isBottom) {
    Subrange s(BucketHandle(NULL, 2), 0, (uint64_t)-1, LogTime(1, 9));
    EXPECT_FALSE(s.isBottom());
    s.bucketWidth = 1;
    EXPECT_TRUE(s.isBottom());
}

TEST_F(TabletProfilerTest, Subrange_partitionWalk) {
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

    EXPECT_EQ(2U, partList.size());
    EXPECT_EQ(0U, partList[0].firstKey);
    EXPECT_EQ(partList[1].firstKey - 1,
        partList[0].lastKey);
    EXPECT_EQ((uint64_t)-1, partList[1].lastKey);
}

TEST_F(TabletProfilerTest, Subrange_track) {
    TabletProfiler tp;

    // first do one insert that won't split bucket 0
    tp.track(0, TabletProfiler::BUCKET_SPLIT_BYTES - 1, LogTime(1, 1));
    EXPECT_FALSE(tp.root->isBottom());
    EXPECT_TRUE(NULL == tp.root->buckets[0].child);
    EXPECT_EQ(TabletProfiler::BUCKET_SPLIT_BYTES - 1,
        tp.root->buckets[0].totalBytes);
    EXPECT_EQ(1U, tp.root->buckets[0].totalReferents);
    EXPECT_EQ(TabletProfiler::BUCKET_SPLIT_BYTES - 1,
        tp.root->totalBytes);
    EXPECT_EQ(1U, tp.root->totalReferents);

    // now force one that will split the bucket
    tp.track(0, 2, LogTime(1, 2));

    // assert the parent hasn't changed
    EXPECT_EQ(TabletProfiler::BUCKET_SPLIT_BYTES - 1,
        tp.root->buckets[0].totalBytes);
    EXPECT_EQ(1U, tp.root->buckets[0].totalReferents);
    EXPECT_EQ(TabletProfiler::BUCKET_SPLIT_BYTES - 1,
        tp.root->totalBytes);
    EXPECT_EQ(1U, tp.root->totalReferents);

    // now check that the child is correct
    EXPECT_EQ(1U, tp.root->totalChildren);
    EXPECT_TRUE(tp.root->buckets[0].child != NULL);

    Subrange *child = tp.root->buckets[0].child;
    BucketHandle rootBh = BucketHandle(tp.root, 0);

    EXPECT_EQ(tp.root->getBucketFirstKey(rootBh),
        child->firstKey);
    EXPECT_EQ(tp.root->getBucketLastKey(rootBh), child->lastKey);
    EXPECT_EQ(0U, child->totalChildren);
    EXPECT_EQ(2U, child->buckets[0].totalBytes);
    EXPECT_EQ(1U, child->buckets[0].totalReferents);
    EXPECT_EQ(2U, child->totalBytes);
    EXPECT_EQ(1U, child->totalReferents);
}

// NB: this test can be sensitive to the constants used.
TEST_F(TabletProfilerTest, Subrange_untrack) {
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
    EXPECT_TRUE(child != NULL);
    BucketHandle bh = tp.findBucket(0, NULL);
    EXPECT_EQ(1U, tp.root->totalChildren);
    EXPECT_EQ(child, bh.getSubrange());
    EXPECT_EQ(300U, child->totalBytes);
    EXPECT_EQ(3U, child->totalReferents);
    EXPECT_EQ(300U, child->buckets[0].totalBytes);
    EXPECT_EQ(3U, child->buckets[0].totalReferents);

    // make sure it doesn't merge before we expect
    tp.untrack(0, 100, LogTime(1, 6));
    EXPECT_EQ(child, bh.getSubrange());
    EXPECT_EQ(200U, child->totalBytes);
    EXPECT_EQ(2U, child->totalReferents);
    EXPECT_EQ(200U, child->buckets[0].totalBytes);
    EXPECT_EQ(2U, child->buckets[0].totalReferents);

    // now force a merge
    tp.untrack(0, quarter, LogTime(1, 1));
    tp.untrack(0, quarter, LogTime(1, 2));
    tp.untrack(0, quarter, LogTime(1, 3));
    // our code only merges when a leaf is modified
    tp.untrack(0, 100, LogTime(1, 7));

    EXPECT_TRUE(NULL == tp.root->buckets[0].child);
    EXPECT_EQ(quarter + 100, tp.root->totalBytes);
    EXPECT_EQ(2U, tp.root->totalReferents);
    EXPECT_EQ(quarter + 100, tp.root->buckets[0].totalBytes);
    EXPECT_EQ(2U, tp.root->buckets[0].totalReferents);
    EXPECT_EQ(0U, tp.root->totalChildren);
}

TEST_F(TabletProfilerTest, Subrange_getters) {
    BucketHandle bh(NULL, 0);
    Subrange s(bh, 51, 999, LogTime(134, 53));

    EXPECT_EQ(LogTime(134, 53), s.getCreateTime());
    EXPECT_EQ(51U, s.getFirstKey());
    EXPECT_EQ(999U, s.getLastKey());
}

} // namespace RAMCloud
