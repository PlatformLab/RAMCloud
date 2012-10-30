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

#include "Seglet.h"
#include "SegletAllocator.h"
#include "ServerConfig.h"

namespace RAMCloud {

/**
 * Unit tests for LogSegment.
 */
class LogSegmentTest : public ::testing::Test {
  public:
    LogSegmentTest()
        : serverConfig(ServerConfig::forTesting()),
          allocator(serverConfig),
          buf(new char[serverConfig.segletSize]),
          seglet(allocator, buf, sizeof(buf)),
          s()
    {
        vector<Seglet*> seglets;
        allocator.alloc(SegletAllocator::DEFAULT, 1, seglets);
        s.construct(seglets,
                    seglets[0]->getLength(),
                    seglets[0]->getLength(),
                    183,
                    38,
                    false);
    }

    ~LogSegmentTest()
    {
        delete[] buf;
    }

    ServerConfig serverConfig;
    SegletAllocator allocator;
    char* buf;
    Seglet seglet;
    Tub<LogSegment> s;

    DISALLOW_COPY_AND_ASSIGN(LogSegmentTest);
};

TEST_F(LogSegmentTest, getAverageTimestamp) {
    EXPECT_EQ(0U, s->getAverageTimestamp());
    s->statistics.liveBytes = 28345;
    s->statistics.spaceTimeSum = 8272901;
    EXPECT_EQ(291U, s->getAverageTimestamp());
}

TEST_F(LogSegmentTest, getMemoryUtilization) {
    EXPECT_EQ(0, s->getMemoryUtilization());
    s->statistics.liveBytes = s->segletSize / 2;
    EXPECT_EQ(50, s->getMemoryUtilization());
}

TEST_F(LogSegmentTest, getDiskUtilization) {
    EXPECT_EQ(0, s->getDiskUtilization());
    s->statistics.liveBytes = s->segmentSize / 2;
    EXPECT_EQ(50, s->getDiskUtilization());
}

/**
 * Unit tests for LogSegment::Statistics.
 */
class LogSegment_StatisticsTest : public ::testing::Test {
  public:
    LogSegment_StatisticsTest()
        : s()
    {
    }

    LogSegment::Statistics s;

    DISALLOW_COPY_AND_ASSIGN(LogSegment_StatisticsTest);
};

TEST_F(LogSegment_StatisticsTest, constructor) {
    EXPECT_EQ(0U, s.liveBytes);
    EXPECT_EQ(0U, s.spaceTimeSum);
}

TEST_F(LogSegment_StatisticsTest, increment) {
    s.increment(2842, 9283842);
    EXPECT_EQ(2842U, s.liveBytes);
    EXPECT_EQ(2842UL * 9283842, s.spaceTimeSum);
}

TEST_F(LogSegment_StatisticsTest, decrement) {
    s.increment(2842, 9283842);
    s.increment(828274, 27346727);
    s.increment(726, 283646292);
    s.decrement(828274, 27346727);
    EXPECT_EQ(2842U + 726, s.liveBytes);
    EXPECT_EQ(2842UL * 9283842 + 726UL * 283646292, s.spaceTimeSum);
}

TEST_F(LogSegment_StatisticsTest, get) {
    uint32_t liveBytes;
    uint64_t spaceTimeSum;

    s.get(liveBytes, spaceTimeSum);
    EXPECT_EQ(0U, liveBytes);
    EXPECT_EQ(0U, spaceTimeSum);

    s.increment(97013, 8262502);
    s.get(liveBytes, spaceTimeSum);
    EXPECT_EQ(97013U, liveBytes);
    EXPECT_EQ(801570106526UL, spaceTimeSum);
}

} // namespace RAMCloud
