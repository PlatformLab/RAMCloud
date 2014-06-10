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
    // Make sure that time advances.
    start = Cycles::rdtsc();
    usleep(1000);
    elapsed = Cycles::toSeconds(Cycles::rdtsc() - start);
    ASSERT_LE(.0009, elapsed);
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

TEST_F(CyclesTest, toSeconds_givenCyclesPerSec) {
    double seconds = Cycles::toSeconds(500, 1000);
    EXPECT_LT(.49999, seconds);
    EXPECT_GT(.50001, seconds);
}

TEST_F(CyclesTest, fromSeconds) {
    Cycles::cyclesPerSec = 1000;
    EXPECT_EQ(20UL, Cycles::fromSeconds(.01999));
}

TEST_F(CyclesTest, fromSeconds_givenCyclesPerSec) {
    EXPECT_EQ(20UL, Cycles::fromSeconds(.01999, 1000));
}

TEST_F(CyclesTest, toNanoseconds) {
    Cycles::cyclesPerSec = 3e09;
    EXPECT_EQ(3UL, Cycles::toNanoseconds(10));
    EXPECT_EQ(4UL, Cycles::toNanoseconds(11));
}

TEST_F(CyclesTest, toNanoseconds_givenCyclesPerSec) {
    EXPECT_EQ(3UL, Cycles::toNanoseconds(10, 3e09));
    EXPECT_EQ(4UL, Cycles::toNanoseconds(11, 3e09));
}

TEST_F(CyclesTest, fromNanoseconds) {
    Cycles::cyclesPerSec = 2e09;
    EXPECT_EQ(160UL, Cycles::fromNanoseconds(80));
}

TEST_F(CyclesTest, fromNanoseconds_givenCyclesPerSec) {
    EXPECT_EQ(160UL, Cycles::fromNanoseconds(80, 2e09));
}

TEST_F(CyclesTest, sleep) {
    uint64_t us = 100;
    uint64_t start = Cycles::rdtsc();
    Cycles::sleep(us);
    uint64_t end = Cycles::rdtsc();

    EXPECT_LE(us, Cycles::toMicroseconds(end-start));
}

}  // namespace RAMCloud
