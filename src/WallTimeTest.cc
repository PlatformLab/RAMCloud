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

#include "WallTime.h"

namespace RAMCloud {

/**
 * Unit tests for WallTime. Some of these tests are time-sensitive. I've never
 * seen them fail on an idle machine over hundreds of runs, but if they become
 * an issue, feel free to crank the acceptable DELTA up a little bit.
 */
class WallTimeTest : public ::testing::Test {
  public:
    // Several tests check that time readings or variables fall within a given
    // error bound. This is the bound used. The unit is seconds.
    enum { DELTA = 1 };

    WallTimeTest()
    {
        WallTime::baseTime = 0;
        WallTime::baseTsc = 0;
        WallTime::mockWallTimeValue = 0;
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(WallTimeTest);
};

TEST_F(WallTimeTest, secondsTimestamp)
{
    EXPECT_EQ(0U, WallTime::baseTime);
    EXPECT_EQ(0U, WallTime::baseTsc);
    EXPECT_EQ(0U, WallTime::mockWallTimeValue);

    WallTime::secondsTimestamp();

    EXPECT_LE(Cycles::rdtsc() - WallTime::baseTsc, Cycles::fromSeconds(DELTA));
    EXPECT_LE(time(NULL) - WallTime::baseTime, DELTA);
    EXPECT_EQ(0U, WallTime::mockWallTimeValue);
}

TEST_F(WallTimeTest, secondsTimestampToUnix)
{
    time_t unixNow = time(NULL);
    uint32_t ramcloudNow = WallTime::secondsTimestamp();
    time_t unixNowConversion = WallTime::secondsTimestampToUnix(ramcloudNow);
    time_t delta = (unixNow > unixNowConversion) ? unixNow - unixNowConversion :
                                                   unixNowConversion - unixNow;
    EXPECT_LE(delta, DELTA);
}

} // namespace RAMCloud
