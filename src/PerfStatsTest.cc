/* Copyright (c) 2014 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
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
#include "PerfStats.h"

namespace RAMCloud {

class PerfStatsTest : public ::testing::Test {
  public:
    PerfStats stats;

    PerfStatsTest()
        : stats()
    {
        PerfStats::registeredStats.clear();
    }

    ~PerfStatsTest()
    {
    }

    DISALLOW_COPY_AND_ASSIGN(PerfStatsTest);
};

// Helper function for the following test.
static void testThreadLocalStats() {
    PerfStats::threadStats.readCount = 10;
}

TEST_F(PerfStatsTest, threadStatsVariable) {
    // This test ensures that different threads have different PerfStats.
    PerfStats::threadStats.readCount = 99;
    std::thread thread(testThreadLocalStats);
    thread.join();
    EXPECT_EQ(99lu, PerfStats::threadStats.readCount);
}

TEST_F(PerfStatsTest, registerStats_alreadyRegistered) {
    PerfStats::registerStats(&stats);
    EXPECT_EQ(1u, PerfStats::registeredStats.size());
    PerfStats stats2;
    PerfStats::registerStats(&stats2);
    EXPECT_EQ(2u, PerfStats::registeredStats.size());
    PerfStats::registerStats(&stats);
    EXPECT_EQ(2u, PerfStats::registeredStats.size());
    PerfStats::registerStats(&stats2);
    EXPECT_EQ(2u, PerfStats::registeredStats.size());
}

TEST_F(PerfStatsTest, registerStats_initialize) {
    EXPECT_EQ(0u, PerfStats::registeredStats.size());
    stats.readCount = 99;
    stats.writeCount = 100;
    PerfStats::registerStats(&stats);
    EXPECT_EQ(1u, PerfStats::registeredStats.size());
    EXPECT_EQ(0u, stats.readCount);
    EXPECT_EQ(0u, stats.writeCount);
}

TEST_F(PerfStatsTest, collectStats) {
    PerfStats::registerStats(&stats);
    stats.readCount = 10;
    stats.writeCount = 20;
    PerfStats stats2;
    PerfStats::registerStats(&stats2);
    stats2.readCount = 100;
    stats2.writeCount = 200;
    PerfStats total;
    PerfStats::collectStats(&total);
    EXPECT_EQ(110u, total.readCount);
    EXPECT_EQ(220u, total.writeCount);
}

}  // namespace RAMCloud
