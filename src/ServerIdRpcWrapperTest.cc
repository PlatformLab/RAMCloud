/* Copyright (c) 2012-2014 Stanford University
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
#include "CoordinatorService.h"
#include "CoordinatorSession.h"
#include "MasterRecoveryManager.h"
#include "MockExternalStorage.h"
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
    TestLog::Enable logSilencer;

    ServerIdRpcWrapperTest()
        : context()
        , serverList(&context)
        , transport(&context)
        , id(1, 0)
        , coordId()
        , logSilencer()
    {
        context.transportManager->registerMock(&transport);
        serverList.testingAdd({{1, 0}, "mock:", {}, 100,
                               ServerStatus::UP});
    }

    ~ServerIdRpcWrapperTest()
    {
    }

    DISALLOW_COPY_AND_ASSIGN(ServerIdRpcWrapperTest);
};

TEST_F(ServerIdRpcWrapperTest, checkStatus_serverUp) {
    ServerIdRpcWrapper wrapper(&context, id,
                sizeof(WireFormat::BackupFree::Response));
    wrapper.allocHeader<WireFormat::BackupFree>(id);
    wrapper.send();
    WireFormat::ResponseCommon* responseCommon =
            wrapper.response->emplaceAppend<WireFormat::ResponseCommon>();
    responseCommon->status = STATUS_WRONG_SERVER;
    wrapper.completed();
    EXPECT_FALSE(wrapper.isReady());
    EXPECT_EQ("checkStatus: STATUS_WRONG_SERVER in BACKUP_FREE RPC "
            "to server 1.0 at mock: | "
            "flushSession: flushed session for id 1.0", TestLog::get());
    EXPECT_STREQ("IN_PROGRESS", wrapper.stateString());
}

TEST_F(ServerIdRpcWrapperTest, checkStatus_serverCrashed) {
    ServerIdRpcWrapper wrapper(&context, id,
                sizeof(WireFormat::BackupFree::Response));
    wrapper.allocHeader<WireFormat::BackupFree>(id);
    wrapper.send();
    WireFormat::ResponseCommon* responseCommon =
            wrapper.response->emplaceAppend<WireFormat::ResponseCommon>();
    responseCommon->status = STATUS_WRONG_SERVER;
    wrapper.completed();
    serverList.testingCrashed(id);
    EXPECT_TRUE(wrapper.isReady());
    EXPECT_EQ("checkStatus: STATUS_WRONG_SERVER in BACKUP_FREE RPC "
            "to server 1.0 at mock: | "
            "flushSession: flushed session for id 1.0", TestLog::get());
    EXPECT_STREQ("FINISHED", wrapper.stateString());
}

TEST_F(ServerIdRpcWrapperTest, checkStatus_unknownError) {
    TestLog::Enable _;
    ServerIdRpcWrapper wrapper(&context, id,
                sizeof(WireFormat::BackupFree::Response));
    wrapper.allocHeader<WireFormat::BackupFree>(id);
    wrapper.send();
    WireFormat::ResponseCommon* responseCommon =
            wrapper.response->emplaceAppend<WireFormat::ResponseCommon>();
    responseCommon->status = STATUS_UNIMPLEMENTED_REQUEST;
    wrapper.completed();
    EXPECT_TRUE(wrapper.isReady());
    EXPECT_EQ("", TestLog::get());
}

TEST_F(ServerIdRpcWrapperTest, handleTransportError_serverAlreadyDown) {
    ServerIdRpcWrapper wrapper(&context, id, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    wrapper.state = RpcWrapper::RpcState::FAILED;
    wrapper.serverCrashed = true;
    EXPECT_TRUE(wrapper.isReady());
    EXPECT_STREQ("FAILED", wrapper.stateString());
    EXPECT_EQ("", TestLog::get());
}

TEST_F(ServerIdRpcWrapperTest, handleTransportError_serverUp) {
    ServerIdRpcWrapper wrapper(&context, id, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    wrapper.state = RpcWrapper::RpcState::FAILED;
    EXPECT_FALSE(wrapper.isReady());
    EXPECT_STREQ("IN_PROGRESS", wrapper.stateString());
    EXPECT_EQ("flushSession: flushed session for id 1.0",
            TestLog::get());
    EXPECT_FALSE(wrapper.serverCrashed);
    EXPECT_EQ(1, wrapper.transportErrors);
}

TEST_F(ServerIdRpcWrapperTest, handleTransportError_serverCrashed) {
    ServerIdRpcWrapper wrapper(&context, id, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    wrapper.state = RpcWrapper::RpcState::FAILED;
    serverList.testingCrashed(id);
    EXPECT_TRUE(wrapper.isReady());
    EXPECT_STREQ("FAILED", wrapper.stateString());
    EXPECT_EQ("flushSession: flushed session for id 1.0",
            TestLog::get());
    EXPECT_TRUE(wrapper.serverCrashed);
}

TEST_F(ServerIdRpcWrapperTest, handleTransportError_nonexistentServer) {
    ServerIdRpcWrapper wrapper(&context, ServerId(10, 20), 4);
    wrapper.request.fillFromString("100");
    EXPECT_TRUE(wrapper.handleTransportError());
    EXPECT_STREQ("NOT_STARTED", wrapper.stateString());
    EXPECT_EQ("", TestLog::get());
    EXPECT_TRUE(wrapper.serverCrashed);
}

TEST_F(ServerIdRpcWrapperTest, handleTransportError_callServerCrashed) {
    // Set up a CoordinatorServerList, which requires a CoordinatorService.
    CoordinatorServerList serverList(&context);
    context.serverList = &serverList;
    MockExternalStorage storage(false);
    context.externalStorage = &storage;
    CoordinatorService coordinator(&context, 1000, false);
    coordinator.recoveryManager.doNotStartRecoveries = true;

    // Don't let the server list updater run; can cause timing-dependent
    // crashes due to order dependencies among destructors.
    serverList.haltUpdater();

    id = serverList.enlistServer({WireFormat::MASTER_SERVICE}, 100, "mock:");
    ServerIdRpcWrapper wrapper(&context, id, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    wrapper.state = RpcWrapper::RpcState::FAILED;
    wrapper.transportErrors = 2;
    TestLog::reset();
    TestLog::Enable _("startMasterRecovery");
    EXPECT_TRUE(wrapper.isReady());
    EXPECT_STREQ("FAILED", wrapper.stateString());
    EXPECT_TRUE(wrapper.serverCrashed);
    EXPECT_EQ("startMasterRecovery: Recovery requested for 1.0",
            TestLog::get());
}

TEST_F(ServerIdRpcWrapperTest, handleTransportError_slowRetry) {
    Cycles::mockTscValue = 1000;
    ServerIdRpcWrapper wrapper(&context, id, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    wrapper.state = RpcWrapper::RpcState::FAILED;
    wrapper.transportErrors = 2;
    EXPECT_FALSE(wrapper.isReady());
    Cycles::mockTscValue = 0;
    EXPECT_STREQ("RETRY", wrapper.stateString());
    double delay = Cycles::toSeconds(wrapper.retryTime - 1000);
    EXPECT_LE(0.499, delay);
    EXPECT_GE(1.501, delay);
    EXPECT_EQ(3, wrapper.transportErrors);
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
    ServerIdRpcWrapper wrapper(&context, ServerId(4, 0), 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    wrapper.response->emplaceAppend<WireFormat::ResponseCommon>()->status =
            STATUS_OK;
    wrapper.state = RpcWrapper::RpcState::FINISHED;
    wrapper.waitAndCheckErrors();
}

TEST_F(ServerIdRpcWrapperTest, waitAndCheckErrors_serverDoesntExist) {
    ServerIdRpcWrapper wrapper(&context, ServerId(4, 0), 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    string message = "no exception";
    try {
        wrapper.waitAndCheckErrors();
    }
    catch (ServerNotUpException& e) {
        message = "ServerNotUpException";
    }
    EXPECT_EQ("ServerNotUpException", message);
}

TEST_F(ServerIdRpcWrapperTest, waitAndCheckErrors_errorStatus) {
    ServerIdRpcWrapper wrapper(&context, ServerId(4, 0), 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    wrapper.response->emplaceAppend<WireFormat::ResponseCommon>()->status =
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
