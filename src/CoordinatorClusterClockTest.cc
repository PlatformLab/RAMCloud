/* Copyright (c) 2014-2015 Stanford University
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

#include "TestUtil.h"       //Has to be first, compiler complains
#include "CoordinatorClusterClock.h"
#include "MockExternalStorage.h"

namespace RAMCloud {

class CoordinatorClusterClockTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockExternalStorage storage;
    Tub<CoordinatorClusterClock> clock;
    const ClusterTimeDuration safeTimeInterval;


    CoordinatorClusterClockTest()
        : logEnabler()
        , context()
        , storage(true)
        , clock()
        , safeTimeInterval(CoordinatorClusterClockConstants::safeTimeInterval)
    {
        context.externalStorage = &storage;
        clock.construct(&context);
    }

    DISALLOW_COPY_AND_ASSIGN(CoordinatorClusterClockTest);
};

TEST_F(CoordinatorClusterClockTest, getTime) {
    // Time dependent test;
    // Set large safe time so that the test will not hit it.
    clock->safeClusterTime = ClusterTime(10000000);
    EXPECT_GT(clock->getTime(), ClusterTime(0U));
    EXPECT_LT(clock->getTime(), ClusterTime(10000000U));
}

TEST_F(CoordinatorClusterClockTest, getTime_stale) {
    usleep(1000);
    EXPECT_EQ(0U, clock->safeClusterTime.getEncoded());
    EXPECT_EQ(ClusterTime(0U), clock->getTime());
    usleep(1000);
    TestLog::reset();
    EXPECT_EQ(ClusterTime(0U), clock->getTime());
    EXPECT_EQ("getTime: "
              "Returning stale time. SafeTimeUpdater may be running behind.",
              TestLog::get());
}

TEST_F(CoordinatorClusterClockTest, startUpdater) {
    // Time dependent test.
    clock->startUpdater();
    EXPECT_TRUE(clock->updater.isRunning());
}

TEST_F(CoordinatorClusterClockTest, handleTimerEvent) {
    // Covers both the handleTimerEvent and recoverClusterTime methods.
    EXPECT_EQ(0U, clock->recoverClusterTime(context.externalStorage));
    EXPECT_EQ(0U, clock->safeClusterTime.getEncoded());
    storage.log.clear();
    clock->updater.handleTimerEvent();
    EXPECT_EQ("set(UPDATE, coordinatorClusterClock)", storage.log);
    storage.getResults.push(storage.setData);
    uint64_t storedTime = clock->recoverClusterTime(context.externalStorage);
    EXPECT_GT(static_cast<int64_t>(storedTime),
              safeTimeInterval.toNanoseconds());
    EXPECT_EQ(storedTime, clock->safeClusterTime.getEncoded());
}

TEST_F(CoordinatorClusterClockTest, getInternal) {
    ClusterTime firstTime = clock->getInternal();
    usleep(50);
    ClusterTime secondTime = clock->getInternal();
    EXPECT_GT(secondTime, firstTime);
    EXPECT_GT(firstTime, clock->startingClusterTime);
}

// recoverClusterTime covered by handleTimerEvent test.

}  // namespace RAMCloud
