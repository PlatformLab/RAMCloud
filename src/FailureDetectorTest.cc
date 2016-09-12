/* Copyright (c) 2011-2016 Stanford University
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

#include "AdminClient.h"
#include "CoordinatorClient.h"
#include "CoordinatorSession.h"
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
        serverList.testingAdd({id, locator, {WireFormat::ADMIN_SERVICE},
                               100, ServerStatus::UP});
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
    mockTransport.setInput("0");
    fd->pingRandomServer();
    EXPECT_TRUE(StringUtil::startsWith(TestLog::get(),
                "pingRandomServer: Sending ping to server 1.0 (mock:) | "
                "pingRandomServer: Ping succeeded to server 1.0 (mock:)"));
}

TEST_F(FailureDetectorTest, pingRandomServer_pingFailure) {
    TestLog::Enable logSilencer("pingRandomServer", "wait", NULL);
    MockRandom _(1);
    addServer(ServerId(1, 0), "mock:");
    coordTransport.setInput("0");
    fd->pingRandomServer();
    EXPECT_EQ("pingRandomServer: Sending ping to server 1.0 (mock:) | "
              "wait: timeout | pingRandomServer: Ping timeout to "
              "server id 1.0 (locator \"mock:\")", TestLog::get());
}

TEST_F(FailureDetectorTest, pingRandomServer_notInCluster) {
    TestLog::Enable logSilencer("pingRandomServer", "Disabler",
            "VerifyMembershipRpc", NULL);
    MockRandom _(1);
    addServer(ServerId(1, 0), "mock:");
    string input = format("%d", STATUS_CALLER_NOT_IN_CLUSTER);
    mockTransport.setInput(input.c_str());
    coordTransport.setInput("0");
    fd->pingRandomServer();
    EXPECT_EQ("pingRandomServer: Sending ping to server 1.0 (mock:) | "
            "Disabler: master service disabled | "
            "VerifyMembershipRpc: verifying cluster membership for 57.27342",
            TestLog::get());
}

TEST_F(FailureDetectorTest, pingRandomServer_tooManyFailedProbes) {
    TestLog::Enable logSilencer("pingRandomServer", "wait", "Disabler",
            "VerifyMembershipRpc", NULL);
    MockRandom _(1);
    addServer(ServerId(1, 0), "mock:");
    fd->probesWithoutResponse = FailureDetector::MAX_FAILED_PROBES-2;
    coordTransport.setInput("0");
    coordTransport.setInput("0");
    coordTransport.setInput("0");
    fd->pingRandomServer();
    EXPECT_EQ("pingRandomServer: Sending ping to server 1.0 (mock:) | "
            "wait: timeout | "
            "pingRandomServer: Ping timeout to server id 1.0 "
            "(locator \"mock:\")",
            TestLog::get());
    TestLog::reset();
    fd->pingRandomServer();
    EXPECT_EQ("pingRandomServer: Sending ping to server 1.0 (mock:) | "
            "wait: timeout | "
            "pingRandomServer: Ping timeout to server id 1.0 "
            "(locator \"mock:\") | "
            "Disabler: master service disabled | "
            "VerifyMembershipRpc: verifying cluster membership for 57.27342",
            TestLog::get());
    EXPECT_EQ(0, fd->probesWithoutResponse);
}

} // namespace RAMCloud
