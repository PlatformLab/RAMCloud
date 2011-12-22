/* Copyright (c) 2011 Stanford University
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
#include "ServerList.pb.h"

namespace RAMCloud {

/**
 * Unit tests for FailureDetector.
 */
class FailureDetectorTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    MockTransport mockTransport;
    FailureDetector *fd;

    FailureDetectorTest()
        : logEnabler(),
          mockTransport(),
          fd(NULL)
    {
        Context::get().serverList = new ServerList();
        Context::get().transportManager->registerMock(&mockTransport, "mock");
        fd = new FailureDetector("mock:", ServerId(57, 27342));
    }

    ~FailureDetectorTest()
    {
        delete fd;
        Context::get().transportManager->unregisterMock();
        delete Context::get().serverList;
        Context::get().serverList = NULL;
    }

    static bool
    failureDetectorFilter(string s)
    {
        return s == "FailureDetector";
    }

    void
    addServer(ServerId id, string locator)
    {
        ServerId dummy1;
        ServerChangeEvent dummy2;
        Context::get().serverList->add(id, ServiceLocator(locator));
        fd->serverTracker.getChange(dummy1, dummy2);
    }

    DISALLOW_COPY_AND_ASSIGN(FailureDetectorTest);
};

TEST_F(FailureDetectorTest, pingRandomServer_noServers) {
    fd->pingRandomServer();
    EXPECT_EQ("pingRandomServer: No servers besides myself to probe! "
              "List has 0 entries.", TestLog::get());
}

TEST_F(FailureDetectorTest, pingRandomServer_onlySelfServers) {
    addServer(ServerId(57, 27342), "mock:");
    fd->pingRandomServer();
    EXPECT_EQ("pingRandomServer: No servers besides myself to probe! "
              "List has 1 entries.", TestLog::get());
}

TEST_F(FailureDetectorTest, pingRandomServer_pingSuccess) {
    addServer(ServerId(1, 0), "mock:");
    mockTransport.setInput("0 0 55 0");
    fd->pingRandomServer();
    EXPECT_EQ("checkStatus: status: 0 | pingRandomServer: "
              "Ping succeeded to server mock:", TestLog::get());
}

TEST_F(FailureDetectorTest, pingRandomServer_pingFailure) {
    addServer(ServerId(1, 0), "mock:");
    mockTransport.setInput(NULL); // ping timeout
    mockTransport.setInput("0");
    fd->pingRandomServer();
    EXPECT_EQ("alertCoordinator: Ping timeout to server id 1 "
        "(locator \"mock:\") | checkStatus: status: 0", TestLog::get());
}

TEST_F(FailureDetectorTest, pingRandomServer_pingFailureAndCoordFailure) {
    addServer(ServerId(1, 0), "mock:");
    mockTransport.setInput(NULL); // ping timeout
    mockTransport.setInput(NULL); // coordinator timeout
    fd->pingRandomServer();
    EXPECT_EQ(0U, TestLog::get().find("alertCoordinator: Ping timeout to "
        "server id 1 (locator \"mock:\") | alertCoordinator: Hint server "
        "down failed. Maybe the network is disconnected: "
        "RAMCloud::TransportException: testing thrown"));
}

} // namespace RAMCloud
