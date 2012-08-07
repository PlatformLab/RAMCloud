/* Copyright (c) 2012 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"
#include "CoordinatorServerList.h"
#include "CoordinatorSession.h"
#include "MockTransport.h"
#include "ServerIdRpcWrapper.h"
#include "TransportManager.h"

namespace RAMCloud {

class ServerIdRpcWrapperTest : public ::testing::Test {
  public:
    Context context;
    MockTransport transport;
    ServerId id;
    ServerId coordId;
    ServerList serverList;
    CoordinatorServerList coordinatorServerList;

    ServerIdRpcWrapperTest()
        : context()
        , transport(context)
        , id(1, 0)
        , coordId()
        , serverList(context)
        , coordinatorServerList(context)
    {
        context.transportManager->registerMock(&transport);
        context.serverList = &serverList;
        context.serverList->add(ServerId(1, 0), "mock:", {}, 100);
        context.coordinatorServerList = &coordinatorServerList;
        coordId = context.coordinatorServerList->add("mock:coord=1",
                {WireFormat::MASTER_SERVICE}, 100);
    }

    ~ServerIdRpcWrapperTest()
    {
        context.transportManager->unregisterMock();
    }

    DISALLOW_COPY_AND_ASSIGN(ServerIdRpcWrapperTest);
};

TEST_F(ServerIdRpcWrapperTest, handleTransportError_serverAlreadyDown) {
    TestLog::Enable _;
    ServerIdRpcWrapper wrapper(context, id, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    wrapper.state = RpcWrapper::RpcState::FAILED;
    wrapper.serverDown = true;
    EXPECT_TRUE(wrapper.isReady());
    EXPECT_STREQ("FAILED", wrapper.stateString());
    EXPECT_EQ("", TestLog::get());
}

TEST_F(ServerIdRpcWrapperTest, handleTransportError_serverUp) {
    TestLog::Enable _;
    ServerIdRpcWrapper wrapper(context, id, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    wrapper.state = RpcWrapper::RpcState::FAILED;
    EXPECT_FALSE(wrapper.isReady());
    EXPECT_STREQ("IN_PROGRESS", wrapper.stateString());
    EXPECT_EQ("flushSession: flushing session for mock:",
            TestLog::get());
    EXPECT_FALSE(wrapper.serverDown);
}

TEST_F(ServerIdRpcWrapperTest, handleTransportError_serverCrashed) {
    TestLog::Enable _;
    ServerIdRpcWrapper wrapper(context, id, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    wrapper.state = RpcWrapper::RpcState::FAILED;
    context.serverList->crashed(id, "mock:", {}, 100);
    EXPECT_TRUE(wrapper.isReady());
    EXPECT_STREQ("FAILED", wrapper.stateString());
    EXPECT_EQ("flushSession: flushing session for mock:",
            TestLog::get());
    EXPECT_TRUE(wrapper.serverDown);
}

TEST_F(ServerIdRpcWrapperTest, handleTransportError_coordinatorServerList) {
    TestLog::Enable _;
    context.serverList = NULL;
    ServerIdRpcWrapper wrapper(context, id, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    wrapper.state = RpcWrapper::RpcState::FAILED;
    context.coordinatorServerList->crashed(coordId);
    EXPECT_TRUE(wrapper.isReady());
    EXPECT_STREQ("FAILED", wrapper.stateString());
    EXPECT_EQ("flushSession: flushing session for mock:coord=1",
            TestLog::get());
    EXPECT_TRUE(wrapper.serverDown);
}

TEST_F(ServerIdRpcWrapperTest, handleTransportError_nonexistentServer) {
    TestLog::Enable _;
    ServerIdRpcWrapper wrapper(context, ServerId(10, 20), 4);
    wrapper.request.fillFromString("100");
    EXPECT_TRUE(wrapper.handleTransportError());
    EXPECT_STREQ("NOT_STARTED", wrapper.stateString());
    EXPECT_EQ("", TestLog::get());
    EXPECT_TRUE(wrapper.serverDown);
}

TEST_F(ServerIdRpcWrapperTest, send) {
    ServerIdRpcWrapper wrapper(context, id, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    EXPECT_STREQ("IN_PROGRESS", wrapper.stateString());
    EXPECT_EQ("sendRequest: 100", transport.outputLog);
    EXPECT_EQ("mock:", wrapper.session->getServiceLocator());
}

TEST_F(ServerIdRpcWrapperTest, send_coordinatorServerList) {
    context.serverList = NULL;
    ServerIdRpcWrapper wrapper(context, coordId, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    EXPECT_STREQ("IN_PROGRESS", wrapper.stateString());
    EXPECT_EQ("sendRequest: 100", transport.outputLog);
    EXPECT_EQ("mock:coord=1", wrapper.session->getServiceLocator());
}

TEST_F(ServerIdRpcWrapperTest, send_exception) {
    TestLog::Enable _;
    ServerIdRpcWrapper wrapper(context, ServerId(20, 3), 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    EXPECT_STREQ("FAILED", wrapper.stateString());
    EXPECT_EQ("send: ServerIdRpcWrapper couldn't get session: "
            "Invalid ServerID (12884901908)",
            TestLog::get());
}

TEST_F(ServerIdRpcWrapperTest, waitAndCheckErrors_success) {
    TestLog::Enable _;
    ServerIdRpcWrapper wrapper(context, ServerId(4, 0), 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    (new(wrapper.response, APPEND) WireFormat::ResponseCommon)->status =
            STATUS_OK;
    wrapper.state = RpcWrapper::RpcState::FINISHED;
    wrapper.waitAndCheckErrors();
}

TEST_F(ServerIdRpcWrapperTest, waitAndCheckErrors_serverDoesntExist) {
    TestLog::Enable _;
    ServerIdRpcWrapper wrapper(context, ServerId(4, 0), 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    string message = "no exception";
    try {
        wrapper.waitAndCheckErrors();
    }
    catch (ServerDoesntExistException& e) {
        message = "ServerDoesntExistException";
    }
    EXPECT_EQ("ServerDoesntExistException", message);
}

TEST_F(ServerIdRpcWrapperTest, waitAndCheckErrors_errorStatus) {
    TestLog::Enable _;
    ServerIdRpcWrapper wrapper(context, ServerId(4, 0), 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    (new(wrapper.response, APPEND) WireFormat::ResponseCommon)->status =
            STATUS_UNIMPLEMENTED_REQUEST;
    wrapper.state = RpcWrapper::RpcState::FINISHED;
    string message = "no exception";
    try {
        wrapper.waitAndCheckErrors();
    }
    catch (ClientException& e) {
        message = e.toSymbol();
    }
    EXPECT_EQ("STATUS_UNIMPLEMENTED_REQUEST", message);
}


}  // namespace RAMCloud
