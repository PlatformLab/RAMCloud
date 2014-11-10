/* Copyright (c) 2014 Stanford University
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
#include "RpcTracker.h"

namespace RAMCloud {

class RpcTrackerTest : public ::testing::Test {
  public:
    RpcTracker tracker;
    LinearizableObjectRpcWrapper* w;

    RpcTrackerTest() : tracker()
                     , w(reinterpret_cast<LinearizableObjectRpcWrapper*>(1)) {
    }

    DISALLOW_COPY_AND_ASSIGN(RpcTrackerTest);
};

TEST_F(RpcTrackerTest, rpcFinished) {
    EXPECT_EQ(1UL, tracker.newRpcId(w));
    EXPECT_EQ(2UL, tracker.newRpcId(w));
    EXPECT_EQ(3UL, tracker.newRpcId(w));
    EXPECT_EQ(4UL, tracker.newRpcId(w));

    EXPECT_EQ(tracker.firstMissing, 1UL);
    tracker.rpcFinished(1);
    EXPECT_EQ(tracker.firstMissing, 2UL);
    tracker.rpcFinished(3);
    EXPECT_EQ(tracker.firstMissing, 2UL);
    tracker.rpcFinished(2);
    EXPECT_EQ(tracker.firstMissing, 4UL);

    EXPECT_EQ(5UL, tracker.newRpcId(w));
    EXPECT_EQ(6UL, tracker.newRpcId(w));
    EXPECT_EQ(7UL, tracker.newRpcId(w));

    tracker.rpcFinished(5);
    tracker.rpcFinished(6);
    EXPECT_EQ(tracker.firstMissing, 4UL);
    tracker.rpcFinished(4);
    EXPECT_EQ(tracker.firstMissing, 7UL);
    tracker.rpcFinished(7);
    EXPECT_EQ(tracker.firstMissing, 8UL);
}

TEST_F(RpcTrackerTest, newRpcId) {
    int i;
    for (i = 1; i <= tracker.windowSize; ++i) {
        EXPECT_EQ(tracker.newRpcId(w), (uint64_t)i);
    }
    tracker.rpcFinished(3);
    EXPECT_EQ(tracker.newRpcId(w), 0UL);

    for (i = 1; i <= tracker.windowSize; ++i) {
        if (i == 3)
            continue;
        EXPECT_FALSE(!tracker.rpcs[i % tracker.windowSize]);
    }
    EXPECT_TRUE(!tracker.rpcs[3]);
}

TEST_F(RpcTrackerTest, ackId) {
    EXPECT_EQ(tracker.newRpcId(w), 1UL);
    EXPECT_EQ(tracker.newRpcId(w), 2UL);
    EXPECT_EQ(tracker.newRpcId(w), 3UL);
    EXPECT_EQ(tracker.newRpcId(w), 4UL);

    EXPECT_EQ(tracker.ackId(), 0UL);
    tracker.rpcFinished(1);
    EXPECT_EQ(tracker.ackId(), 1UL);
    tracker.rpcFinished(3);
    EXPECT_EQ(tracker.ackId(), 1UL);
    tracker.rpcFinished(2);
    EXPECT_EQ(tracker.ackId(), 3UL);
    tracker.rpcFinished(4);
    EXPECT_EQ(tracker.ackId(), 4UL);
}

TEST_F(RpcTrackerTest, hasUnfinishedRpc) {
    EXPECT_FALSE(tracker.hasUnfinishedRpc());
    uint64_t id1 = tracker.newRpcId(w);
    EXPECT_TRUE(tracker.hasUnfinishedRpc());
    uint64_t id2 = tracker.newRpcId(w);
    EXPECT_TRUE(tracker.hasUnfinishedRpc());
    tracker.rpcFinished(id1);
    EXPECT_TRUE(tracker.hasUnfinishedRpc());
    tracker.rpcFinished(id2);
    EXPECT_FALSE(tracker.hasUnfinishedRpc());
}

TEST_F(RpcTrackerTest, tooManayOutstandingRpcs) {
    uint64_t i;
    for (i = 1; i <= (uint64_t)RpcTracker::windowSize; ++i) {
        EXPECT_EQ(tracker.newRpcId(w), (uint64_t)i);
    }
    EXPECT_EQ(tracker.newRpcId(w), 0UL);
    tracker.rpcFinished(1);
    EXPECT_EQ(tracker.newRpcId(w), (uint64_t)i);
    EXPECT_EQ(tracker.newRpcId(w), 0UL);
}

}
