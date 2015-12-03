/* Copyright (c) 2011-2015 Stanford University
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

/**
 * \file
 * Unit tests for ServerRpcPool.
 */

#include "TestUtil.h"
#include "LogProtector.h"
#include "ServerRpcPool.h"
#include "Transport.h"

namespace RAMCloud {

// Need a do-nothing subclass of the abstract parent type.
class TestServerRpc : public Transport::ServerRpc {
    void sendReply() {}
    string getClientServiceLocator() { return ""; }
};

TEST(ServerRpcPoolTest, constructor) {
    ServerRpcPool<> pool;
    EXPECT_EQ(0U, pool.outstandingAllocations);
}

// Old ObjectPool was evil, so leave this here in case of zombie attacks
TEST(ServerRpcPoolTest, destructor) {
    ServerRpcPool<TestServerRpc> pool;
    TestServerRpc* rpc1 = pool.construct();
    TestServerRpc* rpc2 = pool.construct();
    pool.destroy(rpc1);
    pool.destroy(rpc2);
}

TEST(ServerRpcPoolTest, construct) {
    Context context;
    ServerRpcPool<TestServerRpc> pool;

    TestServerRpc* rpc = pool.construct();
    EXPECT_EQ(true, rpc->outstandingRpcListHook.is_linked());
    EXPECT_EQ(1U, pool.outstandingAllocations);

    pool.destroy(rpc);
}

TEST(ServerRpcPoolTest, destroy) {
    Context context;
    ServerRpcPool<TestServerRpc> pool;

    TestServerRpc* rpc = pool.construct();
    pool.destroy(rpc);
    EXPECT_EQ(0U, pool.outstandingAllocations);
    EXPECT_EQ(-1UL, pool.getEarliestEpoch(~0));
}

TEST(ServerRpcPoolTest, getEarliestEpoch_basics) {
    Context context;

    LogProtector::currentSystemEpoch = 57;
    ServerRpcPool<TestServerRpc> pool;
    TestServerRpc* rpc = pool.construct();
    EXPECT_EQ(-1UL, pool.getEarliestEpoch(~0));
    rpc->epoch = 57;
    EXPECT_EQ(57UL, pool.getEarliestEpoch(~0));
    pool.destroy(rpc);

    EXPECT_EQ(-1UL, pool.getEarliestEpoch(~0));
}

TEST(ServerRpcPoolTest, getEarliestEpoch_activityMask) {
    Context context;

    ServerRpcPool<TestServerRpc> pool;
    TestServerRpc* rpc1 = pool.construct();
    rpc1->epoch = 44;
    TestServerRpc* rpc2 = pool.construct();
    rpc2->epoch = 6;
    TestServerRpc* rpc3 = pool.construct();
    rpc3->epoch = 19;
    EXPECT_EQ(6UL, pool.getEarliestEpoch(~0));
    rpc2->activities = Transport::ServerRpc::READ_ACTIVITY;
    EXPECT_EQ(19UL, pool.getEarliestEpoch(
                            Transport::ServerRpc::APPEND_ACTIVITY));
    pool.destroy(rpc1);
    pool.destroy(rpc2);
    pool.destroy(rpc3);
}

TEST(ServerRpcPoolGuardTest, generic) {
    ServerRpcPool<TestServerRpc> pool;
    {
        TestServerRpc* rpc = pool.construct();
        ServerRpcPoolGuard<TestServerRpc>(pool, rpc);
    }
    EXPECT_EQ(0U, pool.outstandingAllocations);
}

} // namespace RAMCloud
