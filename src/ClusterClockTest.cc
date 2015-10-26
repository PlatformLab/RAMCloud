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
#include "ClusterClock.h"

namespace RAMCloud {

TEST(ClusterClock, getTime) {
    ClusterClock clock;

    clock.clusterTime = ClusterTime(42);
    EXPECT_EQ(ClusterTime(42), clock.getTime());

    clock.clusterTime = ClusterTime(64);
    EXPECT_EQ(ClusterTime(64), clock.getTime());

    clock.clusterTime = ClusterTime(8);
    EXPECT_EQ(ClusterTime(8), clock.getTime());
}

TEST(ClusterClock, updateClock) {
    ClusterClock clock;
    clock.clusterTime = ClusterTime(0);
    EXPECT_EQ(ClusterTime(0), clock.getTime());

    clock.updateClock(ClusterTime(42));
    EXPECT_EQ(ClusterTime(42), clock.getTime());

    clock.updateClock(ClusterTime(64));
    EXPECT_EQ(ClusterTime(64), clock.getTime());

    clock.updateClock(ClusterTime(8));
    EXPECT_EQ(ClusterTime(64), clock.getTime());
}

}  // namespace RAMCloud
