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
    ServerList serverList;
    MockTransport transport;
    ServerId id;
    ServerId coordId;

    ServerIdRpcWrapperTest()
        : context()
        , serverList(context)
        , transport(context)
        , id(1, 0)
        , coordId()
    {
        context.transportManager->registerMock(&transport);
        serverList.add(ServerId(1, 0), "mock:", {}, 100);
    }

    ~ServerIdRpcWrapperTest()
    {
    }

    DISALLOW_COPY_AND_ASSIGN(ServerIdRpcWrapperTest);
};

TEST_F(ServerIdRpcWrapperTest, handleTransportError_serverAlreadyDown) {
    TestLog::Enable _;
    ServerIdRpcWrapper wrapper(&context, id, 4);
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
    ServerIdRpcWrapper wrapper(&context, id, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    wrapper.state = RpcWrapper::RpcState::FAILED;
    EXPECT_FALSE(wrapper.isReady());
    EXPECT_STREQ("IN_PROGRESS", wrapper.stateString());
    EXPECT_EQ("flushSession: flushed session for id 1.0",
            TestLog::get());
    EXPECT_FALSE(wrapper.serverDown);
}

TEST_F(ServerIdRpcWrapperTest, handleTransportError_serverCrashed) {
    TestLog::Enable _;
    ServerIdRpcWrapper wrapper(&context, id, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    wrapper.state = RpcWrapper::RpcState::FAILED;
    serverList.crashed(id, "mock:", {}, 100);
    EXPECT_TRUE(wrapper.isReady());
    EXPECT_STREQ("FAILED", wrapper.stateString());
    EXPECT_EQ("flushSession: flushed session for id 1.0",
            TestLog::get());
    EXPECT_TRUE(wrapper.serverDown);
}

TEST_F(ServerIdRpcWrapperTest, handleTransportError_nonexistentServer) {
    TestLog::Enable _;
    ServerIdRpcWrapper wrapper(&context, ServerId(10, 20), 4);
    wrapper.request.fillFromString("100");
    EXPECT_TRUE(wrapper.handleTransportError());
    EXPECT_STREQ("NOT_STARTED", wrapper.stateString());
    EXPECT_EQ("", TestLog::get());
    EXPECT_TRUE(wrapper.serverDown);
}

TEST_F(ServerIdRpcWrapperTest, send) {
    ServerIdRpcWrapper wrapper(&context, id, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    EXPECT_STREQ("IN_PROGRESS", wrapper.stateString());
    EXPECT_EQ("sendRequest: 100", transport.outputLog);
    EXPECT_EQ("mock:", wrapper.session->getServiceLocator());
}

TEST_F(ServerIdRpcWrapperTest, waitAndCheckErrors_success) {
    TestLog::Enable _;
    ServerIdRpcWrapper wrapper(&context, ServerId(4, 0), 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    (new(wrapper.response, APPEND) WireFormat::ResponseCommon)->status =
            STATUS_OK;
    wrapper.state = RpcWrapper::RpcState::FINISHED;
    wrapper.waitAndCheckErrors();
}

TEST_F(ServerIdRpcWrapperTest, waitAndCheckErrors_serverDoesntExist) {
    TestLog::Enable _;
    ServerIdRpcWrapper wrapper(&context, ServerId(4, 0), 4);
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
    ServerIdRpcWrapper wrapper(&context, ServerId(4, 0), 4);
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
