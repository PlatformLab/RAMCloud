/* Copyright (c) 2011-2012 Stanford University
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
#include "BindTransport.h"
#include "Common.h"
#include "PingClient.h"
#include "PingService.h"
#include "RawMetrics.h"
#include "ServerMetrics.h"
#include "TransportManager.h"
#include "ServerList.h"

// Note: this file tests both PingService.cc and PingClient.cc.

namespace RAMCloud {

class PingServiceTest : public ::testing::Test {
  public:
    Context context;
    ServerList serverList;
    BindTransport transport;
    TransportManager::MockRegistrar mockRegistrar;
    PingService pingService;
    ServerId serverId;

    PingServiceTest()
        : context()
        , serverList(context)
        , transport(context)
        , mockRegistrar(context, transport)
        , pingService(context)
        , serverId(1, 3)
    {
        transport.addService(pingService, "mock:host=ping",
                             WireFormat::PING_SERVICE);
        serverList.add(serverId, "mock:host=ping",
                       {WireFormat::PING_SERVICE}, 100);
    }

    DISALLOW_COPY_AND_ASSIGN(PingServiceTest);
};

TEST_F(PingServiceTest, ping_basics) {
    TestLog::Enable _;
    EXPECT_EQ(0UL,
              PingClient::ping(context, serverId, ServerId()));
    EXPECT_EQ("ping: Received ping request from unknown endpoint "
              "(perhaps the coordinator or a client)",
              TestLog::get());
    TestLog::reset();
    EXPECT_EQ(0UL,
              PingClient::ping(context, serverId, ServerId(99)));
    EXPECT_EQ("ping: Received ping request from server 99",
              TestLog::get());
}

TEST_F(PingServiceTest, ping_wait_timeout) {
    TestLog::Enable _;
    ServerId serverId2(2, 3);
    MockTransport mockTransport(context);
    context.transportManager->registerMock(&mockTransport, "mock2");
    serverList.add(serverId2, "mock2:", {WireFormat::PING_SERVICE}, 100);
    PingRpc rpc(context, serverId2, ServerId());
    uint64_t start = Cycles::rdtsc();
    EXPECT_EQ(~0LU, rpc.wait(1000000));
    EXPECT_EQ("wait: timeout", TestLog::get());
    double elapsedMicros = 1e06* Cycles::toSeconds(Cycles::rdtsc() - start);
    EXPECT_GE(elapsedMicros, 1000.0);
    EXPECT_LE(elapsedMicros, 2000.0);
    context.transportManager->unregisterMock();
}

// Helper function that runs in a separate thread for the following test.
static void pingThread(PingRpc* rpc, uint64_t* result) {
    *result = rpc->wait(100000000);
}

TEST_F(PingServiceTest, ping_wait_serverGoesAway) {
    TestLog::Enable _;
    ServerId serverId2(2, 3);
    MockTransport mockTransport(context);
    context.transportManager->registerMock(&mockTransport, "mock2");
    serverList.add(serverId2, "mock2:", {WireFormat::PING_SERVICE}, 100);

    uint64_t result = 0;
    PingRpc rpc(context, serverId2, ServerId());
    std::thread thread(pingThread, &rpc, &result);
    usleep(100);
    EXPECT_EQ(0LU, result);

    // Delete the server, then fail the ping RPC so that there is a retry
    // that discovers that targetId is gone.
    serverList.remove(serverId2);
    mockTransport.lastNotifier->failed();

    // Give the other thread a chance to finish.
    for (int i = 0; (result == 0) && (i < 1000); i++) {
        usleep(100);
    }

    EXPECT_EQ(0xffffffffffffffffU, result);
    EXPECT_EQ("wait: server doesn't exist", TestLog::get());
    thread.join();
    context.transportManager->unregisterMock();
}

TEST_F(PingServiceTest, proxyPing_basics) {
    uint64_t ns = PingClient::proxyPing(context, serverId, serverId, 100000);
    EXPECT_NE(-1U, ns);
    EXPECT_LT(10U, ns);
}
TEST_F(PingServiceTest, proxyPing_timeout) {
    // Test the situation where the target times out.
    ServerId targetId(2, 3);
    MockTransport mockTransport(context);
    context.transportManager->registerMock(&mockTransport, "mock2");
    serverList.add(targetId, "mock2:", {WireFormat::PING_SERVICE}, 100);
    uint64_t start = Cycles::rdtsc();
    EXPECT_EQ(0xffffffffffffffffU,
              PingClient::proxyPing(context, serverId, targetId, 1000000));
    double elapsedMicros = 1e06* Cycles::toSeconds(Cycles::rdtsc() - start);
    EXPECT_GE(elapsedMicros, 1000.0);
    EXPECT_LE(elapsedMicros, 2000.0);
    context.transportManager->unregisterMock();
}

} // namespace RAMCloud
