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
    BindTransport transport;
    TransportManager::MockRegistrar mockRegistrar;
    ServerList serverList;
    PingService pingService;
    PingClient client;

    PingServiceTest()
        : context()
        , transport(context)
        , mockRegistrar(context, transport)
        , serverList(context)
        , pingService(context, &serverList)
        , client(context)
    {
        transport.addService(pingService, "mock:host=ping", PING_SERVICE);
    }

    DISALLOW_COPY_AND_ASSIGN(PingServiceTest);
};

TEST_F(PingServiceTest, getMetrics) {
    metrics->master.replicas = 99;
    metrics->temp.count3 = 33;
    ServerMetrics metrics = client.getMetrics("mock:host=ping");
    EXPECT_EQ(99U, metrics["master.replicas"]);
    EXPECT_EQ(33U, metrics["temp.count3"]);
}

TEST_F(PingServiceTest, ping_basics) {
    TestLog::Enable _;
    EXPECT_EQ(0x1234512345U,
              client.ping("mock:host=ping", ServerId(), 0x1234512345, 0));
    EXPECT_EQ("ping: Received ping request from unknown endpoint "
              "(perhaps the coordinator or a client) with nonce 1234512345 | "
              "checkStatus: status: 0", TestLog::get());
    TestLog::reset();
    EXPECT_EQ(0x12345u,
              client.ping("mock:host=ping", ServerId(99), 0x12345, 0));
    EXPECT_EQ("ping: Received ping request from server 99 with nonce 12345 | "
              "checkStatus: status: 0", TestLog::get());
}
TEST_F(PingServiceTest, ping_timeout) {
    uint64_t start = Cycles::rdtsc();
    transport.abortCounter = 1;
    EXPECT_THROW(client.ping("mock:host=ping", ServerId(), 0x12345, 100000),
                 TimeoutException);
    double elapsedMicros = 1e06* Cycles::toSeconds(Cycles::rdtsc() - start);
    EXPECT_GE(elapsedMicros, 100.0);
}

TEST_F(PingServiceTest, proxyPing_basics) {
    uint64_t ns = client.proxyPing("mock:host=ping", "mock:host=ping",
                                   100000, 100000);
    EXPECT_NE(-1U, ns);
    EXPECT_LT(10U, ns);
}
TEST_F(PingServiceTest, proxyPing_timeout2) {
    // Test the situation where the target (serviceLocator2) times out.
    uint64_t start = Cycles::rdtsc();
    transport.abortCounter = 2;
    EXPECT_EQ(0xffffffffffffffffU,
              client.proxyPing("mock:host=ping", "mock:host=ping",
                               2000000, 1000000));
    double elapsedMicros = 1e06* Cycles::toSeconds(Cycles::rdtsc() - start);
    EXPECT_GE(elapsedMicros, 1000.0);
    EXPECT_LE(elapsedMicros, 2000.0);
}
TEST_F(PingServiceTest, proxyPing_pingReturnsBadValue) {
    MockTransport mockTransport(context);
    mockTransport.setInput("0 0 55 0 1 0");
    context.transportManager->registerMock(&mockTransport, "mock2");
    transport.addService(pingService, "mock2:host=ping2", PING_SERVICE);
    EXPECT_EQ(0xffffffffffffffffU,
              client.proxyPing("mock:host=ping", "mock2:host=ping2",
                               2000000, 1000000));
    context.transportManager->unregisterMock();
}
TEST_F(PingServiceTest, proxyPing_timeout1) {
    // Test the situation where the proxy (serviceLocator1) times out.
    uint64_t start = Cycles::rdtsc();
    transport.abortCounter = 1;
    EXPECT_THROW(client.proxyPing("mock:host=ping", "mock:host=ping",
                                  2000000, 1000000),
                 TimeoutException);
    double elapsedMicros = 1e06* Cycles::toSeconds(Cycles::rdtsc() - start);
    EXPECT_GE(elapsedMicros, 2000.0);
}

} // namespace RAMCloud
