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

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "TestUtil.h"

#include "CoordinatorClient.h"
#include "PingClient.h"
#include "FailureDetector.h"
#include "Rpc.h"
#include "ServerList.pb.h"
#include "ServerList.h"
#include "StringUtil.h"

namespace RAMCloud {

/**
 * Unit tests for FailureDetector.
 */
class FailureDetectorTest : public ::testing::Test {
  public:
    Context context;
    TestLog::Enable logEnabler;
    MockTransport mockTransport;
    ServerList* serverList;
    FailureDetector *fd;

    FailureDetectorTest()
        : context(),
          logEnabler(),
          mockTransport(context),
          serverList(NULL),
          fd(NULL)
    {
        serverList = new ServerList(context);
        context.transportManager->registerMock(&mockTransport, "mock");
        fd = new FailureDetector(context, "mock:", ServerId(57, 27342),
                *serverList);
    }

    ~FailureDetectorTest()
    {
        delete fd;
        context.transportManager->unregisterMock();
        delete serverList;
    }

    static bool
    failureDetectorFilter(string s)
    {
        return s == "FailureDetector";
    }

    void
    addServer(ServerId id, string locator)
    {
        ServerDetails dummy1;
        ServerChangeEvent dummy2;
        serverList->add(id, locator, {PING_SERVICE}, 100);
        fd->serverTracker.getChange(dummy1, dummy2);
    }

    DISALLOW_COPY_AND_ASSIGN(FailureDetectorTest);
};

TEST_F(FailureDetectorTest, pingRandomServer_noServers) {
    // Ensure it doesn't spin.
    fd->pingRandomServer();
}

TEST_F(FailureDetectorTest, pingRandomServer_onlySelfServers) {
    addServer(ServerId(57, 27342), "mock:");
    // Ensure it doesn't spin.
    fd->pingRandomServer();
}

TEST_F(FailureDetectorTest, pingRandomServer_pingSuccess) {
    MockRandom _(1);
    addServer(ServerId(1, 0), "mock:");
    mockTransport.setInput("0 0 55 0 1 0");
    fd->pingRandomServer();
    EXPECT_TRUE(StringUtil::startsWith(TestLog::get(),
                "pingRandomServer: Sending ping with nonce 2 to "
                    "server 1 (mock:) | "
                "checkStatus: status: 0 | "
                "pingRandomServer: Ping with nonce 2 succeeded to "
                    "server 1 (mock:)"));
}

TEST_F(FailureDetectorTest, pingRandomServer_pingFailure) {
    MockRandom _(1);
    addServer(ServerId(1, 0), "mock:");
    mockTransport.setInput(NULL); // ping timeout
    mockTransport.setInput("0");
    fd->pingRandomServer();
    EXPECT_EQ("pingRandomServer: Sending ping with nonce 2 to "
                   "server 1 (mock:) | "
               "alertCoordinator: Ping timeout to server id 1 "
                   "(locator \"mock:\") | "
               "checkStatus: status: 0", TestLog::get());
}

TEST_F(FailureDetectorTest, pingRandomServer_pingFailureAndCoordFailure) {
    MockRandom _(1);
    addServer(ServerId(1, 0), "mock:");
    mockTransport.setInput(NULL); // ping timeout
    mockTransport.setInput(NULL); // coordinator timeout
    fd->pingRandomServer();
    EXPECT_TRUE(StringUtil::startsWith(TestLog::get(),
                "pingRandomServer: Sending ping with nonce 2 "
                    "to server 1 (mock:) | "
                "alertCoordinator: Ping timeout to server id 1 "
                    "(locator \"mock:\") | "
                "alertCoordinator: Hint server down failed. Maybe the network "
                    "is disconnected: RAMCloud::TransportException: "
                "testing thrown"));
}

TEST_F(FailureDetectorTest, checkServerListVersion) {
    serverList->version = 0;
    fd->checkServerListVersion(0);
    EXPECT_FALSE(fd->staleServerListSuspected);

    fd->checkServerListVersion(1);
    EXPECT_TRUE(fd->staleServerListSuspected);
    EXPECT_LT(Cycles::toNanoseconds(
        Cycles::rdtsc() - fd->staleServerListTimestamp), 50e6);  // lots of room
    EXPECT_EQ(0U, fd->staleServerListVersion);

    uint64_t lastTimestamp = fd->staleServerListTimestamp;
    fd->checkServerListVersion(2);
    EXPECT_TRUE(fd->staleServerListSuspected);
    EXPECT_EQ(lastTimestamp, fd->staleServerListTimestamp);
    EXPECT_EQ(0U, fd->staleServerListVersion);
}

TEST_F(FailureDetectorTest, checkForStaleServerList) {
    TestLog::Enable _;

    fd->checkForStaleServerList();
    EXPECT_EQ("checkForStaleServerList: Nothing to do.", TestLog::get());

    // ServerList has since been updated, reset.
    TestLog::reset();
    fd->staleServerListSuspected = true;
    fd->staleServerListVersion = 0;
    serverList->version = 1;
    fd->checkForStaleServerList();
    EXPECT_FALSE(fd->staleServerListSuspected);
    EXPECT_EQ("checkForStaleServerList: Version advanced. "
        "Suspicion suspended.", TestLog::get());

    // Timed out, successfully contacted coordinator
    TestLog::reset();
    fd->staleServerListSuspected = true;
    fd->staleServerListTimestamp = 0;       // way stale
    fd->staleServerListVersion = 2;
    mockTransport.setInput("0 0 0");
    fd->checkForStaleServerList();
    EXPECT_EQ(0U, TestLog::get().find("checkForStaleServerList: Stale server "
        "list detected (have 1, saw 2). Requesting new list push! "
        "Timeout after"));
    EXPECT_FALSE(fd->staleServerListSuspected);

    // Timed out, failed to contact coordinator
    TestLog::reset();
    fd->staleServerListSuspected = true;
    fd->staleServerListTimestamp = 0;       // way stale
    fd->staleServerListVersion = 2;
    mockTransport.setInput(NULL);
    fd->checkForStaleServerList();
    EXPECT_NE(string::npos, TestLog::get().find(
        "checkForStaleServerList: Request to coordinator failed:"));
    EXPECT_TRUE(fd->staleServerListSuspected);
}

} // namespace RAMCloud
