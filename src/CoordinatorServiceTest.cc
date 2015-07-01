/* Copyright (c) 2010-2015 Stanford University
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
#include "ClientException.h"
#include "CoordinatorClient.h"
#include "CoordinatorService.h"
#include "MasterService.h"
#include "MembershipService.h"
#include "MockCluster.h"
#include "MockTransport.h"
#include "RamCloud.h"
#include "Recovery.h"
#include "TaskQueue.h"

namespace RAMCloud {

class CoordinatorServiceTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    ServerConfig masterConfig;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    CoordinatorService* service;
    MasterService* master;
    ServerId masterServerId;

    CoordinatorServiceTest()
        : logEnabler()
        , context()
        , masterConfig(ServerConfig::forTesting())
        , cluster(&context)
        , ramcloud()
        , service()
        , master()
        , masterServerId()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        service = cluster.coordinator.get();

        masterConfig.services = {WireFormat::MASTER_SERVICE,
                                 WireFormat::PING_SERVICE,
                                 WireFormat::MEMBERSHIP_SERVICE};
        masterConfig.localLocator = "mock:host=master";
        Server* masterServer = cluster.addServer(masterConfig);
        master = masterServer->master.get();
        master->objectManager.log.sync();
        masterServerId = masterServer->serverId;

        ramcloud.construct(&context, "mock:host=coordinator");
    }

    // Generate a string containing all of the service locators in a
    // list of servers.
    string
    getLocators(ProtoBuf::ServerList& serverList)
    {
        string result;
        foreach (const ProtoBuf::ServerList::Entry& server,
                serverList.server()) {
            if (result.size() != 0) {
                result += " ";
            }
            result += server.service_locator();
        }
        return result;
    }

    // Populate a list of ServerControlRpc based on the existing serverList.
    void
    populateServerControlRpcList(
            std::list<CoordinatorService::ServerControlRpcContainer>* rpcs,
            Service::Rpc* rpc, WireFormat::ControlOp controlOp)
    {
        ServerId nextServerId;
        bool end = false;
        while (!end) {
            nextServerId = service->serverList->nextServer(nextServerId,
                    {WireFormat::PING_SERVICE}, &end, false);
            if (!end && nextServerId.isValid()) {
                rpcs->emplace_back(service->context, nextServerId, controlOp,
                                   (const void*)NULL, (uint32_t)0);
            }
        }
    }

    DISALLOW_COPY_AND_ASSIGN(CoordinatorServiceTest);
};

TEST_F(CoordinatorServiceTest, dispatch_initNotFinished) {
    EXPECT_TRUE(service->initFinished);
    service->initFinished = false;
    Buffer request, response;
    Service::Rpc rpc(NULL, &request, &response);
    string message("no exception");
    try {
        service->dispatch(WireFormat::Opcode::ILLEGAL_RPC_TYPE, &rpc);
    } catch (RetryException& e) {
        message = e.message;
    }
    EXPECT_EQ("coordinator service not yet initialized", message);
}

TEST_F(CoordinatorServiceTest, createTable_idempotence) {
    EXPECT_EQ(1UL, ramcloud->createTable("duplicate", 1));
    EXPECT_EQ(1UL, ramcloud->createTable("duplicate", 1));
    EXPECT_EQ(2UL, ramcloud->createTable("another", 1));
}

TEST_F(CoordinatorServiceTest, getRuntimeOption) {
    Buffer value;
    ramcloud->setRuntimeOption("failRecoveryMasters", "1 2 3");
    ASSERT_EQ(3u, service->runtimeOptions.failRecoveryMasters.size());
    ramcloud->getRuntimeOption("failRecoveryMasters", &value);
    EXPECT_STREQ("1 2 3", service->getString(&value, 0,
                                            value.size()));
    EXPECT_THROW(ramcloud->getRuntimeOption("optionNotExisting",
                                            &value),
                 ObjectDoesntExistException);
}

TEST_F(CoordinatorServiceTest, getServerList) {
    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    master2Config.services = {WireFormat::MASTER_SERVICE,
                              WireFormat::BACKUP_SERVICE,
                              WireFormat::PING_SERVICE};
    cluster.addServer(master2Config);
    ServerConfig backupConfig = masterConfig;
    backupConfig.localLocator = "mock:host=backup1";
    backupConfig.services = {WireFormat::BACKUP_SERVICE,
                             WireFormat::PING_SERVICE};
    cluster.addServer(backupConfig);
    ProtoBuf::ServerList list;
    CoordinatorClient::getServerList(&context, &list);
    EXPECT_EQ("mock:host=master mock:host=master2 mock:host=backup1",
              getLocators(list));
}

TEST_F(CoordinatorServiceTest, getServerList_backups) {
    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    master2Config.services = {WireFormat::MASTER_SERVICE,
                              WireFormat::BACKUP_SERVICE,
                              WireFormat::PING_SERVICE};
    cluster.addServer(master2Config);
    ServerConfig backupConfig = masterConfig;
    backupConfig.localLocator = "mock:host=backup1";
    backupConfig.services = {WireFormat::BACKUP_SERVICE,
                             WireFormat::PING_SERVICE};
    cluster.addServer(backupConfig);
    ProtoBuf::ServerList list;
    CoordinatorClient::getBackupList(&context, &list);
    EXPECT_EQ("mock:host=master2 mock:host=backup1",
            getLocators(list));
}

TEST_F(CoordinatorServiceTest, getServerList_masters) {
    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    master2Config.services = {WireFormat::MASTER_SERVICE,
                              WireFormat::BACKUP_SERVICE,
                              WireFormat::PING_SERVICE};
    cluster.addServer(master2Config);
    ServerConfig backupConfig = masterConfig;
    backupConfig.localLocator = "mock:host=backup1";
    backupConfig.services = {WireFormat::BACKUP_SERVICE,
                             WireFormat::PING_SERVICE};
    cluster.addServer(backupConfig);
    ProtoBuf::ServerList list;
    CoordinatorClient::getMasterList(&context, &list);
    EXPECT_EQ("mock:host=master mock:host=master2",
            getLocators(list));
}

TEST_F(CoordinatorServiceTest, getTableConfig_tabletInfo) {
    ramcloud->createTable("foo");
    ProtoBuf::TableConfig tableConfigProtoBuf;
    CoordinatorClient::getTableConfig(&context, 1, &tableConfigProtoBuf);
    EXPECT_EQ("tablet { table_id: 1 start_key_hash: 0 "
              "end_key_hash: 18446744073709551615 "
              "state: NORMAL server_id: 1 "
              "service_locator: \"mock:host=master\" "
              "ctime_log_head_id: 0 ctime_log_head_offset: 0 }",
              tableConfigProtoBuf.ShortDebugString());
    // test case to make sure that a nonexistent table id
    // returns a ProtoBuf with no entries
    CoordinatorClient::getTableConfig(&context, 10, &tableConfigProtoBuf);
    EXPECT_EQ("", tableConfigProtoBuf.ShortDebugString());
}

TEST_F(CoordinatorServiceTest, getTableConfig_indexInfo) {
    ramcloud->createTable("foo");
    ramcloud->createIndex(1, 2, 1);

    ProtoBuf::TableConfig tableConfigProtoBuf;
    CoordinatorClient::getTableConfig(&context, 1, &tableConfigProtoBuf);

    foreach (const ProtoBuf::TableConfig::Index& index,
                                            tableConfigProtoBuf.index()) {
        EXPECT_EQ(2U, index.index_id());
        EXPECT_EQ(1U, index.index_type());
        foreach (const ProtoBuf::TableConfig::Index::Indexlet& indexlet,
                                                        index.indexlet()) {
            EXPECT_EQ(0, (uint8_t)*indexlet.start_key().c_str());
            EXPECT_EQ(1U, indexlet.start_key().length());
            EXPECT_EQ(127, (uint8_t)*indexlet.end_key().c_str());
            EXPECT_EQ(1U, indexlet.end_key().length());
            EXPECT_EQ(1U, indexlet.server_id());
            EXPECT_EQ("mock:host=master", indexlet.service_locator());
        }
    }
    // test case to make sure that a nonexistent table id
    // returns a ProtoBuf with no entries
    CoordinatorClient::getTableConfig(&context, 10, &tableConfigProtoBuf);
    EXPECT_EQ("", tableConfigProtoBuf.ShortDebugString());
}

TEST_F(CoordinatorServiceTest, getTableConfig_invalid) {
    ramcloud->createTable("bar");
    ProtoBuf::TableConfig tableConfig;
    CoordinatorClient::getTableConfig(&context, 123, &tableConfig);
    EXPECT_EQ("", tableConfig.ShortDebugString());
}

TEST_F(CoordinatorServiceTest, serverControlAll) {
    Buffer reqBuf;
    Buffer respBuf;
    Service::Rpc rpc(NULL, &reqBuf, &respBuf);   // Fake RPC with no worker.
    WireFormat::ServerControlAll::Request* reqHdr =
                reqBuf.emplaceAppend<WireFormat::ServerControlAll::Request>();
    WireFormat::ServerControlAll::Response* respHdr =
                respBuf.emplaceAppend<WireFormat::ServerControlAll::Response>();

    reqHdr->common.opcode = WireFormat::SERVER_CONTROL_ALL;
    reqHdr->common.service = WireFormat::COORDINATOR_SERVICE;
    reqHdr->controlOp = WireFormat::GET_TIME_TRACE;

    service->serverControlAll(reqHdr, respHdr, &rpc);
    EXPECT_EQ(1U, respHdr->serverCount);
    EXPECT_EQ(1U, respHdr->respCount);
    EXPECT_EQ(45U, respHdr->totalRespLength);
}

TEST_F(CoordinatorServiceTest, serverControlAll_rpcErrors) {
    Buffer reqBuf;
    Buffer respBuf;
    Service::Rpc rpc(NULL, &reqBuf, &respBuf);   // Fake RPC with no worker.
    WireFormat::ServerControlAll::Request* reqHdr =
                reqBuf.emplaceAppend<WireFormat::ServerControlAll::Request>();
    WireFormat::ServerControlAll::Response* respHdr =
                respBuf.emplaceAppend<WireFormat::ServerControlAll::Response>();

    reqHdr->common.opcode = WireFormat::SERVER_CONTROL_ALL;
    reqHdr->common.service = WireFormat::COORDINATOR_SERVICE;
    reqHdr->controlOp = WireFormat::ControlOp(0);

    service->serverControlAll(reqHdr, respHdr, &rpc);
    EXPECT_EQ(1U, respHdr->serverCount);
    EXPECT_EQ(1U, respHdr->respCount);
    EXPECT_EQ(16U, respHdr->totalRespLength);
}

TEST_F(CoordinatorServiceTest, setMasterRecoveryInfo) {
    ProtoBuf::MasterRecoveryInfo info;
    info.set_min_open_segment_id(10);
    info.set_min_open_segment_epoch(1);
    CoordinatorClient::setMasterRecoveryInfo(&context, masterServerId, info);
    EXPECT_EQ(10u, service->context->coordinatorServerList->operator[](
            masterServerId).masterRecoveryInfo.min_open_segment_id());
}

TEST_F(CoordinatorServiceTest, setMasterRecoveryInfo_noSuchServer) {
    string message = "no exception";
    try {
        ProtoBuf::MasterRecoveryInfo info;
        info.set_min_open_segment_id(10);
        info.set_min_open_segment_epoch(1);
        CoordinatorClient::setMasterRecoveryInfo(&context, {999, 999}, info);
    }
    catch (const ServerNotUpException& e) {
        message = e.toSymbol();
    }
    EXPECT_EQ("STATUS_SERVER_NOT_UP", message);
}

TEST_F(CoordinatorServiceTest, setRuntimeOption) {
    ramcloud->setRuntimeOption("failRecoveryMasters", "1 2 3");
    ASSERT_EQ(3u, service->runtimeOptions.failRecoveryMasters.size());
    EXPECT_EQ(1u, service->runtimeOptions.popFailRecoveryMasters());
    EXPECT_EQ(2u, service->runtimeOptions.popFailRecoveryMasters());
    EXPECT_EQ(3u, service->runtimeOptions.popFailRecoveryMasters());
    EXPECT_EQ(0u, service->runtimeOptions.popFailRecoveryMasters());
    EXPECT_THROW(ramcloud->setRuntimeOption("BAD", "1 2 3"),
                 ObjectDoesntExistException);
}

TEST_F(CoordinatorServiceTest, verifyMembership) {
    TestLog::reset();
    CoordinatorClient::verifyMembership(&context, masterServerId);
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "Membership verification succeeded for server 1.0"));
    TestLog::reset();
    ServerId bogus(3, 2);
    EXPECT_THROW(CoordinatorClient::verifyMembership(&context, bogus, false),
                 CallerNotInClusterException);
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "Membership verification failed for server 3.2"));
}

TEST_F(CoordinatorServiceTest, checkServerControlRpcs_basic) {
    std::list<CoordinatorService::ServerControlRpcContainer> rpcs;

    Buffer respBuf;
    Service::Rpc rpc(NULL, NULL, &respBuf);   // Fake RPC with no worker.
    WireFormat::ServerControlAll::Response* respHdr =
                respBuf.emplaceAppend<WireFormat::ServerControlAll::Response>();

    populateServerControlRpcList(&rpcs, &rpc, WireFormat::GET_TIME_TRACE);

    service->checkServerControlRpcs(&rpcs, respHdr, &rpc);
    EXPECT_EQ(1U, respHdr->serverCount);
    EXPECT_EQ(1U, respHdr->respCount);
    EXPECT_EQ(45U, respHdr->totalRespLength);
}

TEST_F(CoordinatorServiceTest, checkServerControlRpcs_ServerNotUpException) {
    // Use MockTransport so that RPC will not be completed automatically.
    MockTransport transport(service->context);
    service->context->transportManager->unregisterMock();
    service->context->transportManager->registerMock(&transport);

    std::list<CoordinatorService::ServerControlRpcContainer> rpcs;

    Buffer respBuf;
    Service::Rpc rpc(NULL, NULL, &respBuf);   // Fake RPC with no worker.
    WireFormat::ServerControlAll::Response* respHdr =
                respBuf.emplaceAppend<WireFormat::ServerControlAll::Response>();

    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    master2Config.services = {WireFormat::MASTER_SERVICE,
                              WireFormat::BACKUP_SERVICE,
                              WireFormat::PING_SERVICE};
    Server* crashingServer = cluster.addServer(master2Config);

    populateServerControlRpcList(&rpcs, &rpc, WireFormat::GET_TIME_TRACE);
    EXPECT_EQ(2U, rpcs.size());

    // One RPC is over BindTransport so it should complete, the other is over
    // MockTrasport, so it will not yet be ready.
    service->checkServerControlRpcs(&rpcs, respHdr, &rpc);
    EXPECT_EQ(1U, respHdr->serverCount);
    EXPECT_EQ(1U, respHdr->respCount);
    EXPECT_EQ(45U, respHdr->totalRespLength);
    EXPECT_EQ(1U, rpcs.size());

    // Crash the remaining server.
    service->context->serverList->serverCrashed(crashingServer->serverId);
    EXPECT_FALSE(service->context->serverList->isUp(crashingServer->serverId));
    rpcs.back().rpc.failed();

    service->checkServerControlRpcs(&rpcs, respHdr, &rpc);
    EXPECT_EQ(1U, respHdr->serverCount);
    EXPECT_EQ(1U, respHdr->respCount);
    EXPECT_EQ(0U, rpcs.size());
}

TEST_F(CoordinatorServiceTest, checkServerControlRpcs_skipNotReady) {
    // Use MockTransport so that RPC will not be completed automatically.
    MockTransport transport(service->context);
    service->context->transportManager->unregisterMock();
    service->context->transportManager->registerMock(&transport);

    // Add two additional servers that use the MockTransport.
    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    master2Config.services = {WireFormat::MASTER_SERVICE,
                              WireFormat::BACKUP_SERVICE,
                              WireFormat::PING_SERVICE};
    cluster.addServer(master2Config);
    ServerConfig backupConfig = masterConfig;
    backupConfig.localLocator = "mock:host=backup1";
    backupConfig.services = {WireFormat::BACKUP_SERVICE,
                             WireFormat::PING_SERVICE};
    cluster.addServer(backupConfig);

    std::list<CoordinatorService::ServerControlRpcContainer> rpcs;

    Buffer respBuf;
    Service::Rpc rpc(NULL, NULL, &respBuf);   // Fake RPC with no worker.
    WireFormat::ServerControlAll::Response* respHdr =
                respBuf.emplaceAppend<WireFormat::ServerControlAll::Response>();

    // We expect to have 4 servers in the cluster and thus 4 RPCs to send.
    populateServerControlRpcList(&rpcs, &rpc, WireFormat::GET_TIME_TRACE);
    EXPECT_EQ(3U, rpcs.size());

    // Since two of the RPCs will be sent over MockTransport, with the other one
    // sent over BindTransport, expect only one of the RPCs to complete.
    service->checkServerControlRpcs(&rpcs, respHdr, &rpc);
    EXPECT_EQ(1U, respHdr->serverCount);
    EXPECT_EQ(1U, respHdr->respCount);
    EXPECT_EQ(45U, respHdr->totalRespLength);

    // Manually complete one of remaining RPCs and expect it to be processed.
    EXPECT_EQ(2U, rpcs.size());
    WireFormat::ServerControl::Response* rpc1RespHdr =
            rpcs.front().buffer.
                    emplaceAppend<WireFormat::ServerControl::Response>();
    rpc1RespHdr->common.status = STATUS_OK;
    rpcs.front().rpc.completed();
    service->checkServerControlRpcs(&rpcs, respHdr, &rpc);
    EXPECT_EQ(2U, respHdr->serverCount);
    EXPECT_EQ(2U, respHdr->respCount);

    // Manually complete the last remaining RPC and expect it to be processed.
    EXPECT_EQ(1U, rpcs.size());
    WireFormat::ServerControl::Response* rpc2RespHdr =
            rpcs.front().buffer.
                    emplaceAppend<WireFormat::ServerControl::Response>();
    rpc2RespHdr->common.status = STATUS_OK;
    rpcs.front().rpc.completed();
    service->checkServerControlRpcs(&rpcs, respHdr, &rpc);
    EXPECT_EQ(3U, respHdr->serverCount);
    EXPECT_EQ(3U, respHdr->respCount);

    EXPECT_EQ(0U, rpcs.size());
}

TEST_F(CoordinatorServiceTest, checkServerControlRpcs_truncated) {
    std::list<CoordinatorService::ServerControlRpcContainer> rpcs;

    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    master2Config.services = {WireFormat::MASTER_SERVICE,
                              WireFormat::BACKUP_SERVICE,
                              WireFormat::PING_SERVICE};
    cluster.addServer(master2Config);

    Buffer respBuf;
    Service::Rpc rpc(NULL, NULL, &respBuf);   // Fake RPC with no worker.
    WireFormat::ServerControlAll::Response* respHdr =
                respBuf.emplaceAppend<WireFormat::ServerControlAll::Response>();

    respBuf.alloc(Transport::MAX_RPC_LEN - 45 - sizeof32(*respHdr));

    populateServerControlRpcList(&rpcs, &rpc, WireFormat::GET_TIME_TRACE);

    service->checkServerControlRpcs(&rpcs, respHdr, &rpc);
    EXPECT_EQ(2U, respHdr->serverCount);
    EXPECT_EQ(1U, respHdr->respCount);
    EXPECT_EQ(static_cast<uint32_t>(Transport::MAX_RPC_LEN), respBuf.size());
}

TEST_F(CoordinatorServiceTest, verifyServerFailure) {
    // Case 1: server up.
    EXPECT_FALSE(service->verifyServerFailure(masterServerId));

    // Case 2: server incommunicado.
    MockTransport mockTransport(&context);
    context.transportManager->registerMock(&mockTransport, "mock2");
    service->serverList->haltUpdater();
    ServerId deadId = service->serverList->enlistServer(
                {WireFormat::PING_SERVICE}, 0, 100, "mock2:");
    EXPECT_TRUE(service->verifyServerFailure(deadId));

    // Case 3: Never kill
    service->neverKill = true;
    EXPECT_FALSE(service->verifyServerFailure(deadId));
}

}  // namespace RAMCloud
