/* Copyright (c) 2012 Stanford University
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

#include "Histogram.h"

namespace RAMCloud {

/**
 * Unit tests for Histogram.
 */
class HistogramTest : public ::testing::Test {
  public:
    HistogramTest() {}

    DISALLOW_COPY_AND_ASSIGN(HistogramTest);
};

TEST_F(HistogramTest, constructor_regular) {
    Histogram h(5000, 10);
    EXPECT_EQ(5000UL, h.numBuckets);
    EXPECT_EQ(10UL, h.bucketWidth);
    for (uint32_t i = 0; i < h.numBuckets; i++)
        EXPECT_EQ(0UL, h.buckets[i]);
    EXPECT_EQ(0UL, downCast<uint64_t>(h.sampleSum));
    EXPECT_EQ(0UL, h.outliers);
    EXPECT_EQ(~0UL, h.min);
    EXPECT_EQ(0UL, h.max);
}

TEST_F(HistogramTest, constructor_deserializer) {
    Histogram h1(100, 1);
    h1.storeSample(8);
    h1.storeSample(23482);
    h1.storeSample(27);

    ProtoBuf::Histogram protoBuf;
    h1.serialize(protoBuf);
    Histogram h2(protoBuf);

    EXPECT_EQ(h1.numBuckets, h2.numBuckets);
    EXPECT_EQ(h1.bucketWidth, h2.bucketWidth);
    EXPECT_EQ(downCast<uint64_t>(h1.sampleSum),
              downCast<uint64_t>(h2.sampleSum));
    EXPECT_EQ(h1.outliers, h2.outliers);
    EXPECT_EQ(h1.max, h2.max);
    EXPECT_EQ(h1.min, h2.min);

    EXPECT_EQ(h1.buckets.size(), h2.buckets.size());
    for (uint32_t i = 0; i < h1.buckets.size(); i++)
        EXPECT_EQ(h1.buckets[i], h2.buckets[i]);
}

TEST_F(HistogramTest, storeSample) {
    Histogram h(5000, 10);

    h.storeSample(3);
    EXPECT_EQ(3UL, h.min);
    EXPECT_EQ(3UL, h.max);
    EXPECT_EQ(0UL, h.outliers);
    EXPECT_EQ(1UL, h.buckets[0]);
    EXPECT_EQ(0UL, h.buckets[1]);
    EXPECT_EQ(0UL, h.buckets[2]);

    h.storeSample(3);
    h.storeSample(h.numBuckets * h.bucketWidth + 40);
    h.storeSample(12);
    h.storeSample(78);

    EXPECT_EQ(3UL, h.min);
    EXPECT_EQ(h.numBuckets * h.bucketWidth + 40, h.max);
    EXPECT_EQ(1UL, h.outliers);
    EXPECT_EQ(2UL, h.buckets[0]);
    EXPECT_EQ(1UL, h.buckets[1]);
    EXPECT_EQ(0UL, h.buckets[2]);

    EXPECT_EQ(3UL + 3 + 12 + 78 + h.numBuckets * h.bucketWidth + 40,
        downCast<uint64_t>(h.sampleSum));
}

TEST_F(HistogramTest, reset) {
    Histogram h(100, 1);
    h.storeSample(23);
    h.storeSample(23492834);

    h.reset();

    EXPECT_EQ(100UL, h.numBuckets);
    EXPECT_EQ(1UL, h.bucketWidth);
    for (uint32_t i = 0; i < h.numBuckets; i++)
        EXPECT_EQ(0UL, h.buckets[i]);
    EXPECT_EQ(0UL, downCast<uint64_t>(h.sampleSum));
    EXPECT_EQ(0UL, h.outliers);
    EXPECT_EQ(~0UL, h.min);
    EXPECT_EQ(0UL, h.max);
}

TEST_F(HistogramTest, toString) {
    Histogram h(100, 1);

    EXPECT_EQ("# Histogram: buckets = 100, bucket width = 1\n"
              "# 0 samples, 0 outliers, min = 18446744073709551615, max = 0\n"
              "# median = 0, average = 0\n",
        h.toString());

    h.storeSample(23);
    h.storeSample(28343);
    h.storeSample(99);
    EXPECT_EQ("# Histogram: buckets = 100, bucket width = 1\n"
              "# 3 samples, 1 outliers, min = 23, max = 28343\n"
              "# median = 99, average = 9488\n"
              "       23             1  33.333333333  33.333333333\n"
              "       99             1  33.333333333  66.666666667\n",
        h.toString());

    Histogram h2(5, 1);
    h2.storeSample(3);
    EXPECT_EQ("# Histogram: buckets = 5, bucket width = 1\n"
              "# 1 samples, 0 outliers, min = 3, max = 3\n"
              "# median = 3, average = 3\n"
              "        0             0  0.000000000  0.000000000\n"
              "        1             0  0.000000000  0.000000000\n"
              "        2             0  0.000000000  0.000000000\n"
              "        3             1  100.000000000  100.000000000\n"
              "        4             0  0.000000000  100.000000000\n",
        h2.toString(0));
}

TEST_F(HistogramTest, getOutliers) {
    Histogram h(1, 1);
    EXPECT_EQ(0UL, h.getOutliers());
    h.storeSample(0);
    h.storeSample(1);
    h.storeSample(2);
    EXPECT_EQ(2UL, h.getOutliers());

    uint64_t highestOutlier;
    h.getOutliers(&highestOutlier);
    EXPECT_EQ(2UL, highestOutlier);
}

TEST_F(HistogramTest, getTotalSamples) {
    Histogram h(1, 1);
    EXPECT_EQ(0UL, h.getTotalSamples());
    h.storeSample(0);
    h.storeSample(1);
    h.storeSample(2);
    EXPECT_EQ(3UL, h.getTotalSamples());
}

TEST_F(HistogramTest, getAverage) {
    // small sum
    Histogram h(1, 1);
    EXPECT_EQ(0UL, h.getAverage());
    h.storeSample(1);
    EXPECT_EQ(1UL, h.getAverage());
    h.storeSample(20);
    EXPECT_EQ(10UL, h.getAverage());

    // sum that doesn't fit in 64-bits
    h.storeSample(0xffffffffffffffffUL);
    h.storeSample(0x0fffffffffffffffUL);
    EXPECT_EQ(0x4400000000000004UL, h.getAverage());
}

TEST_F(HistogramTest, getMedian) {
    // totalSamples == 0
    Histogram noSamples(1, 1);
    EXPECT_EQ(0UL, noSamples.getMedian());

    // numBuckets == 0
    Histogram noBuckets(0, 1);
    noBuckets.storeSample(5);
    EXPECT_EQ(-1UL, noBuckets.getMedian());

    // median falls within outliers
    Histogram h(2, 1);
    h.storeSample(10);
    EXPECT_EQ(-1UL, h.getMedian());

    // median falls within buckets
    h.storeSample(1);
    h.storeSample(1);
    EXPECT_EQ(1UL, h.getMedian());

    // test a slightly less trivial case
    Histogram h2(11, 1);
    for (int i = 0; i <= 10; i++)
        h2.storeSample(i);
    EXPECT_EQ(5UL, h2.getMedian());
}

TEST_F(HistogramTest, serialize) {
    // Covered by 'constructor_deserializer'.
}

} // namespace RAMCloud
