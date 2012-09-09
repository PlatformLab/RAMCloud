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
#include "CoordinatorSession.h"
#include "PingClient.h"
#include "FailureDetector.h"
#include "ServerList.pb.h"
#include "ServerList.h"
#include "ShortMacros.h"
#include "StringUtil.h"

namespace RAMCloud {

/**
 * Unit tests for FailureDetector.
 */
class FailureDetectorTest : public ::testing::Test {
  public:
    Context context;
    ServerList serverList;
    TestLog::Enable logEnabler;
    MockTransport mockTransport;
    MockTransport coordTransport;
    FailureDetector *fd;

    FailureDetectorTest()
        : context(),
          serverList(&context),
          logEnabler(),
          mockTransport(&context),
          coordTransport(&context),
          fd(NULL)
    {
        context.transportManager->registerMock(&mockTransport, "mock");
        context.transportManager->registerMock(&coordTransport, "coord");
        context.coordinatorSession->setLocation("coord:");
        fd = new FailureDetector(&context, ServerId(57, 27342));
    }

    ~FailureDetectorTest()
    {
        delete fd;
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
        serverList.add(id, locator, {WireFormat::PING_SERVICE}, 100);
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
                "pingRandomServer: Sending ping to server 1.0 (mock:) | "
                "pingRandomServer: Ping succeeded to server 1.0 (mock:)"));
}

TEST_F(FailureDetectorTest, pingRandomServer_pingFailure) {
    MockRandom _(1);
    addServer(ServerId(1, 0), "mock:");
    coordTransport.setInput("0");
    fd->pingRandomServer();
    EXPECT_EQ("pingRandomServer: Sending ping to server 1.0 (mock:) | "
              "wait: timeout | pingRandomServer: Ping timeout to "
              "server id 1.0 (locator \"mock:\")", TestLog::get());
}

TEST_F(FailureDetectorTest, checkServerListVersion) {
    serverList.version = 0;
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
    serverList.version = 1;
    fd->checkForStaleServerList();
    EXPECT_FALSE(fd->staleServerListSuspected);
    EXPECT_EQ("checkForStaleServerList: Version advanced. "
        "Suspicion suspended.", TestLog::get());

    // Timed out, successfully contacted coordinator
    TestLog::reset();
    fd->staleServerListSuspected = true;
    fd->staleServerListTimestamp = 0;       // way stale
    fd->staleServerListVersion = 2;
    coordTransport.setInput("0 0 0");
    fd->checkForStaleServerList();
    EXPECT_EQ(0U, TestLog::get().find("checkForStaleServerList: Stale server "
        "list detected (have 1, saw 2). Requesting new list push! "
        "Timeout after"));
    EXPECT_FALSE(fd->staleServerListSuspected);
}

} // namespace RAMCloud
