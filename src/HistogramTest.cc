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

TEST_F(HistogramTest, constructor) {
    Histogram d(5000, 10);
    EXPECT_EQ(5000UL, d.numBuckets);
    EXPECT_EQ(10UL, d.bucketWidth);
    EXPECT_EQ(~0UL, d.min);
    EXPECT_EQ(0UL, d.max);
    EXPECT_EQ(0UL, d.binOverflows);
    EXPECT_EQ(0UL, d.bins[0]);
    EXPECT_EQ(0UL, d.bins[1]);
    EXPECT_EQ(0UL, d.bins[2]);
}

TEST_F(HistogramTest, storeSample) {
    Histogram d(5000, 10);

    d.storeSample(3);
    EXPECT_EQ(3UL, d.min);
    EXPECT_EQ(3UL, d.max);
    EXPECT_EQ(0UL, d.binOverflows);
    EXPECT_EQ(1UL, d.bins[0]);
    EXPECT_EQ(0UL, d.bins[1]);
    EXPECT_EQ(0UL, d.bins[2]);

    d.storeSample(3);
    d.storeSample(d.NBINS * d.BIN_WIDTH + 40);
    d.storeSample(12);
    d.storeSample(78);

    EXPECT_EQ(3UL, d.min);
    EXPECT_EQ(d.NBINS * d.BIN_WIDTH + 40, d.max);
    EXPECT_EQ(1UL, d.binOverflows);
    EXPECT_EQ(2UL, d.bins[0]);
    EXPECT_EQ(1UL, d.bins[1]);
    EXPECT_EQ(0UL, d.bins[2]);
}

} // namespace RAMCloud
