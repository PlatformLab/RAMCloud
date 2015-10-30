/* Copyright (c) 2015 Stanford University
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
#include "ClusterTime.h"

namespace RAMCloud {

TEST(ClusterTimeDurationTest, fromNanoseconds) {
    ClusterTimeDuration duration = ClusterTimeDuration::fromNanoseconds(42);
    EXPECT_EQ(42, duration.length);
}

TEST(ClusterTimeDurationTest, toNanoseconds) {
    ClusterTimeDuration duration = ClusterTimeDuration::fromNanoseconds(42);
    EXPECT_EQ(42, duration.toNanoseconds());
}

TEST(ClusterTimeDurationTest, sub) {
    ClusterTimeDuration durationShorter(1);
    ClusterTimeDuration durationLonger(3);

    ClusterTimeDuration newDuration = durationLonger - durationShorter;
    EXPECT_EQ(2, newDuration.toNanoseconds());
}

TEST(ClusterTimeDurationTest, relational) {
    ClusterTimeDuration durationShorter(1);
    ClusterTimeDuration duration(2);
    ClusterTimeDuration durationLonger(3);

    EXPECT_FALSE(duration <  durationShorter);
    EXPECT_FALSE(duration <  duration);
    EXPECT_TRUE(duration <  durationLonger);

    EXPECT_TRUE(duration >  durationShorter);
    EXPECT_FALSE(duration >  duration);
    EXPECT_FALSE(duration >  durationLonger);

    EXPECT_FALSE(duration <= durationShorter);
    EXPECT_TRUE(duration <= duration);
    EXPECT_TRUE(duration <= durationLonger);

    EXPECT_TRUE(duration >= durationShorter);
    EXPECT_TRUE(duration >= duration);
    EXPECT_FALSE(duration >= durationLonger);

    EXPECT_FALSE(duration == durationShorter);
    EXPECT_TRUE(duration == duration);
    EXPECT_FALSE(duration == durationLonger);

    EXPECT_TRUE(duration != durationShorter);
    EXPECT_FALSE(duration != duration);
    EXPECT_TRUE(duration != durationLonger);
}

TEST(ClusterTimeDurationTest, constructor) {
    ClusterTimeDuration duration(42);
    EXPECT_EQ(42, duration.length);
}

TEST(ClusterTimeTest, constructor) {
    ClusterTime clusterTime;
    EXPECT_EQ(0, clusterTime.timestamp.load());
}

TEST(ClusterTimeTest, constructor_fromEncoded) {
    ClusterTime clusterTime(42);
    EXPECT_EQ(42, clusterTime.timestamp.load());
}

TEST(ClusterTimeTest, getEncoded) {
    ClusterTime clusterTime(42);
    EXPECT_EQ(42U, clusterTime.getEncoded());
}

TEST(ClusterTimeTest, add) {
    ClusterTime clusterTime(21);
    ClusterTimeDuration duration(21);

    ClusterTime newClusterTime = clusterTime + duration;
    EXPECT_EQ(42, newClusterTime.timestamp.load());
}

TEST(ClusterTimeTest, sub) {
    ClusterTime clusterTime(3);
    ClusterTime clusterTimeLater(45);

    ClusterTimeDuration duration = clusterTimeLater - clusterTime;
    EXPECT_EQ(42, duration.length);
}

TEST(ClusterTimeTest, relational) {
    ClusterTime timeBefore(1);
    ClusterTime time(2);
    ClusterTime timeAfter(3);

    EXPECT_FALSE(time <  timeBefore);
    EXPECT_FALSE(time <  time);
    EXPECT_TRUE(time <  timeAfter);

    EXPECT_TRUE(time >  timeBefore);
    EXPECT_FALSE(time >  time);
    EXPECT_FALSE(time >  timeAfter);

    EXPECT_FALSE(time <= timeBefore);
    EXPECT_TRUE(time <= time);
    EXPECT_TRUE(time <= timeAfter);

    EXPECT_TRUE(time >= timeBefore);
    EXPECT_TRUE(time >= time);
    EXPECT_FALSE(time >= timeAfter);

    EXPECT_FALSE(time == timeBefore);
    EXPECT_TRUE(time == time);
    EXPECT_FALSE(time == timeAfter);

    EXPECT_TRUE(time != timeBefore);
    EXPECT_FALSE(time != time);
    EXPECT_TRUE(time != timeAfter);
}

}  // namespace RAMCloud
