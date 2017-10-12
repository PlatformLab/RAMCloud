/* Copyright (c) 2016-2017 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"
#include "QueueEstimator.h"

namespace RAMCloud {
class QueueEstimatorTest : public ::testing::Test {
  public:

    QueueEstimatorTest()
    {
        Cycles::mockCyclesPerSec = 2e09;
    }

    ~QueueEstimatorTest()
    {
        Cycles::mockCyclesPerSec = 0;
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(QueueEstimatorTest);
};

TEST_F(QueueEstimatorTest, constructor) {
    QueueEstimator estimator(10000);
    EXPECT_EQ(0.625, estimator.bandwidth);
}

TEST_F(QueueEstimatorTest, getQueueSize) {
    QueueEstimator estimator(8000);
    EXPECT_EQ(0.5, estimator.bandwidth);
    QueueEstimator::TransmitQueueState state;

    // Enqueue a 1000-byte packet at TSC 100000.
    Cycles::mockTscValue = 100000u;
    estimator.packetQueued(1000, Cycles::rdtsc(), &state);
    EXPECT_EQ(1000u, estimator.queueSize);
    EXPECT_EQ(100000u, state.idleTime);
    EXPECT_EQ(0u, state.outstandingBytes);

    // Test the queue size after 100 cycles.
    EXPECT_EQ(950u, estimator.getQueueSize(100100));

    // Enqueue another 1000-byte packet at TSC 101000.
    Cycles::mockTscValue = 101000u;
    estimator.packetQueued(1000, Cycles::rdtsc(), &state);
    EXPECT_EQ(1500u, estimator.queueSize);
    EXPECT_EQ(0u, state.idleTime);
    EXPECT_EQ(500u, state.outstandingBytes);

    // Test the queue size with a stale timestamp.
    EXPECT_EQ(1500u, estimator.getQueueSize(100000u));

    // The 1500 bytes in the queue should drain in 3000 cycles.
    EXPECT_EQ(0u, estimator.getQueueSize(105000));
    EXPECT_EQ(104000u, estimator.idleSince);
    Cycles::mockTscValue = 0;
}

}  // namespace RAMCloud
