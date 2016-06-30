/* Copyright (c) 2015-2016 Stanford University
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
#include "Transport.h"
#include "LogProtector.h"

namespace RAMCloud {

class LogProtectorTest : public ::testing::Test {
  public:
    LogProtectorTest()
    {
        // As of 6/2016, some tests seem to leave around junk in
        // LogProtector::epochProviders, which can cause false errors.
        // Just delete the junk.
        LogProtector::epochProviders.clear();
    }
};

TEST_F(LogProtectorTest, initialState) {
    LogProtector::Lock lock(LogProtector::epochProvidersMutex);
    EXPECT_EQ(0U, LogProtector::epochProviders.size());
    EXPECT_LE(1U, LogProtector::currentSystemEpoch);
}

class DummyEpochProvider : public LogProtector::EpochProvider {
  public:
    ~DummyEpochProvider() {}
    uint64_t getEarliestEpoch(int activityMask) {
        return ~0;
    }
};

TEST_F(LogProtectorTest, epochProvider_construct) {
    DummyEpochProvider ep;
    EXPECT_EQ(1U, LogProtector::epochProviders.size());
    EXPECT_EQ(&ep, LogProtector::epochProviders.front());
}

TEST_F(LogProtectorTest, epochProvider_destroy) {
    {
        DummyEpochProvider ep;
        EXPECT_EQ(1U, LogProtector::epochProviders.size());
        EXPECT_EQ(&ep, LogProtector::epochProviders.front());
    }
    EXPECT_EQ(0U, LogProtector::epochProviders.size());
    EXPECT_EQ(-1UL, LogProtector::getEarliestOutstandingEpoch(~0));
}

TEST_F(LogProtectorTest, activity_construct) {
    LogProtector::Activity activity;
    EXPECT_EQ(1U, LogProtector::epochProviders.size());
    EXPECT_EQ(&activity, LogProtector::epochProviders.front());
}

TEST_F(LogProtectorTest, activity_destroy) {
    {
        LogProtector::Activity activity;
        EXPECT_EQ(1U, LogProtector::epochProviders.size());
        EXPECT_EQ(&activity, LogProtector::epochProviders.front());
    }
    EXPECT_EQ(0U, LogProtector::epochProviders.size());
    EXPECT_EQ(-1UL, LogProtector::getEarliestOutstandingEpoch(~0));
}

TEST_F(LogProtectorTest, activity_start) {
    LogProtector::currentSystemEpoch = 1;
    LogProtector::Activity activity;
    EXPECT_EQ(-1UL, activity.epoch);
    EXPECT_EQ(0, activity.activityMask);
    activity.start();
    EXPECT_EQ(1U, activity.epoch);
    EXPECT_EQ(~0, activity.activityMask);
    activity.stop();
    activity.start(1);
    EXPECT_EQ(1U, activity.epoch);
    EXPECT_EQ(1, activity.activityMask);
}

TEST_F(LogProtectorTest, activity_stop) {
    LogProtector::currentSystemEpoch = 1;
    LogProtector::Activity activity;
    activity.start();
    EXPECT_EQ(1U, activity.epoch);
    activity.stop();
    EXPECT_EQ(-1UL, activity.epoch);
    EXPECT_EQ(0, activity.activityMask);
}

TEST_F(LogProtectorTest, activity_getEarliestEpoch) {
    LogProtector::currentSystemEpoch = 1;
    LogProtector::Activity activity;
    EXPECT_EQ(-1UL, activity.getEarliestEpoch(~0));
    activity.start();
    EXPECT_EQ(1U, activity.getEarliestEpoch(~0));
    EXPECT_EQ(1U, activity.getEarliestEpoch(4));
    activity.stop();
    EXPECT_EQ(-1UL, activity.getEarliestEpoch(~0));
    activity.start(1);
    EXPECT_EQ(1U, activity.getEarliestEpoch(~0));
    EXPECT_EQ(1U, activity.getEarliestEpoch(1));
    EXPECT_EQ(-1UL, activity.getEarliestEpoch(4));
}

TEST_F(LogProtectorTest, guard) {
    LogProtector::currentSystemEpoch = 1;
    LogProtector::Activity activity;
    EXPECT_EQ(-1UL, activity.epoch);
    EXPECT_EQ(0, activity.activityMask);

    {
        LogProtector::Guard g(activity);
        EXPECT_EQ(1U, activity.epoch);
        EXPECT_EQ(~0, activity.activityMask);
    }
    EXPECT_EQ(-1UL, activity.epoch);
    EXPECT_EQ(0, activity.activityMask);

    {
        LogProtector::Guard g(activity, 1);
        EXPECT_EQ(1U, activity.epoch);
        EXPECT_EQ(1, activity.activityMask);
    }
    EXPECT_EQ(-1UL, activity.epoch);
    EXPECT_EQ(0, activity.activityMask);
}

TEST_F(LogProtectorTest, getCurrentEpoch) {
    LogProtector::currentSystemEpoch = 28;
    EXPECT_EQ(28U, LogProtector::getCurrentEpoch());
}

TEST_F(LogProtectorTest, getEarliestOutstandingEpoch_basics) {
    EXPECT_EQ(-1UL, LogProtector::getEarliestOutstandingEpoch(~0));

    LogProtector::currentSystemEpoch = 57;
    Tub<LogProtector::Activity> activity;
    activity.construct();
    EXPECT_EQ(-1UL, LogProtector::getEarliestOutstandingEpoch(~0));
    activity->start(1);
    EXPECT_EQ(57UL, LogProtector::getEarliestOutstandingEpoch(~0));
    activity->stop();
    EXPECT_EQ(-1UL, LogProtector::getEarliestOutstandingEpoch(~0));
    activity->start(1);
    EXPECT_EQ(57UL, LogProtector::getEarliestOutstandingEpoch(~0));
    activity.destroy();
    EXPECT_EQ(-1UL, LogProtector::getEarliestOutstandingEpoch(~0));
}

TEST_F(LogProtectorTest, getEarliestOutstandingEpoch_activityMask) {
    LogProtector::Activity a1, a2, a3;
    LogProtector::currentSystemEpoch = 44;
    a1.start();
    LogProtector::currentSystemEpoch = 6;
    a2.start(Transport::ServerRpc::READ_ACTIVITY);
    LogProtector::currentSystemEpoch = 19;
    a3.start();

    EXPECT_EQ(6UL, LogProtector::getEarliestOutstandingEpoch(~0));
    EXPECT_EQ(19UL, LogProtector::getEarliestOutstandingEpoch(
            Transport::ServerRpc::APPEND_ACTIVITY));
}

TEST_F(LogProtectorTest, incrementCurrentEpoch) {
    LogProtector::currentSystemEpoch = 98;
    EXPECT_EQ(99U, LogProtector::incrementCurrentEpoch());
    EXPECT_EQ(99U, LogProtector::getCurrentEpoch());
}

static void waitCaller(Context* context, int activityMask, bool* done) {
    LogProtector::wait(context, activityMask);
    *done = true;
}

TEST_F(LogProtectorTest, wait_activityMask) {
    LogProtector::Activity a1, a2, a3;
    LogProtector::currentSystemEpoch = 44;
    a1.start();
    LogProtector::currentSystemEpoch = 6;
    a2.start(Transport::ServerRpc::READ_ACTIVITY);
    LogProtector::currentSystemEpoch = 19;
    a3.start();

    EXPECT_EQ(6UL, LogProtector::getEarliestOutstandingEpoch(~0));
    EXPECT_EQ(19UL, LogProtector::getEarliestOutstandingEpoch(
            Transport::ServerRpc::APPEND_ACTIVITY));

    Context context;

    // wait call should return immediately.
    LogProtector::currentSystemEpoch = 18;
    LogProtector::wait(&context, Transport::ServerRpc::APPEND_ACTIVITY);

    // wait call should return immediately.
    LogProtector::currentSystemEpoch = 5;
    LogProtector::wait(&context, ~0);

    // Now wait call is blocked by a2 (w/ epoch = 6).
    bool done;
    LogProtector::currentSystemEpoch = 18;
    std::thread thread(waitCaller, &context, ~0, &done);
    usleep(10000);
    EXPECT_FALSE(done);

    a2.stop();
    usleep(10000);
    EXPECT_TRUE(done);

    thread.join();
}

} // namespace RAMCloud
