/* Copyright (c) 2011 Stanford University
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
#include "Cycles.h"

namespace RAMCloud {

class CyclesTest : public ::testing::Test {
    double savedCalibration;

  public:
    CyclesTest() : savedCalibration(Cycles::cyclesPerSec) { }
    ~CyclesTest()
    {
        // Restore the timer calibration, in case a test
        // modified it.
        Cycles::cyclesPerSec = savedCalibration;
    }

    DISALLOW_COPY_AND_ASSIGN(CyclesTest);
};

TEST_F(CyclesTest, basics) {
    uint64_t start;
    double elapsed;
    // Run some simple tests of elapsed time (but try it several
    // times in case a context switch delays any one run).
    for (int i = 0; i < 100; i++) {
        start = Cycles::rdtsc();
        usleep(1000);
        elapsed = Cycles::toSeconds(Cycles::rdtsc() - start);
        ASSERT_LE(.001, elapsed);
        ASSERT_GT(.0040, elapsed)
          << "Your system slept longer than it should have. "
             "There's probably nothing wrong -- this test is dubious.";
    }
}

TEST_F(CyclesTest, perSecond_sanity) {
    double cyclesPerSecond = Cycles::perSecond();
    // We'll never have machines slower than 500MHz, will we?
    EXPECT_GT(cyclesPerSecond, 500e06);
    // And we'll never have machines faster than 10GHz, will we?
    EXPECT_LT(cyclesPerSecond, 10e09);
}

TEST_F(CyclesTest, toSeconds) {
    Cycles::cyclesPerSec = 1000;
    double seconds = Cycles::toSeconds(500);
    EXPECT_LT(.49999, seconds);
    EXPECT_GT(.50001, seconds);
}

TEST_F(CyclesTest, fromSeconds) {
    Cycles::cyclesPerSec = 1000;
    EXPECT_EQ(20UL, Cycles::fromSeconds(.01999));
}

TEST_F(CyclesTest, toNanoseconds) {
    Cycles::cyclesPerSec = 3e09;
    EXPECT_EQ(3UL, Cycles::toNanoseconds(10));
    EXPECT_EQ(4UL, Cycles::toNanoseconds(11));
}

TEST_F(CyclesTest, fromNanoseconds) {
    Cycles::cyclesPerSec = 2e09;
    EXPECT_EQ(160UL, Cycles::fromNanoseconds(80));
}

}  // namespace RAMCloud
