/* Copyright (c) 2010-2012 Stanford University
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
#include "CoordinatorServerManager.h"
#include "MasterService.h"
#include "MembershipService.h"
#include "MockCluster.h"
#include "RamCloud.h"
#include "Recovery.h"
#include "ServerList.h"
#include "TaskQueue.h"

namespace RAMCloud {

using LogCabin::Client::Entry;
using LogCabin::Client::EntryId;
using LogCabin::Client::NO_ID;

class CoordinatorServerManagerTest : public ::testing::Test {
  public:
    Context context;
    ServerConfig masterConfig;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    CoordinatorServerManager* serverManager;
    MasterService* master;
    ServerId masterServerId;
    CoordinatorServerList* serverList;

    CoordinatorServerManagerTest()
        : context()
        , masterConfig(ServerConfig::forTesting())
        , cluster(context)
        , ramcloud()
        , serverManager()
        , master()
        , masterServerId()
        , serverList()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        serverManager = &cluster.coordinator.get()->serverManager;

        masterConfig.services = {WireFormat::MASTER_SERVICE,
                                 WireFormat::PING_SERVICE,
                                 WireFormat::MEMBERSHIP_SERVICE};
        masterConfig.localLocator = "mock:host=master";
        Server* masterServer = cluster.addServer(masterConfig);
        master = masterServer->master.get();
        masterServerId = masterServer->serverId;

        ramcloud.construct(context, "mock:host=coordinator");
        serverList = &(serverManager->service.serverList);
    }


    ~CoordinatorServerManagerTest() {
        // Finish all pending ServerList updates before destroying cluster.
        cluster.syncCoordinatorServerList();
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

    DISALLOW_COPY_AND_ASSIGN(CoordinatorServerManagerTest);
};

TEST_F(CoordinatorServerManagerTest, assignReplicationGroup) {
    vector<ServerId> serverIds;
    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::BACKUP_SERVICE,
        WireFormat::MEMBERSHIP_SERVICE, WireFormat::PING_SERVICE};
    for (uint32_t i = 0; i < 3; i++) {
        config.localLocator = format("mock:host=backup%u", i);
        serverIds.push_back(cluster.addServer(config)->serverId);
    }
    // Check normal functionality.
    EXPECT_TRUE(serverManager->assignReplicationGroup(10U, serverIds));
    EXPECT_EQ(10U, serverList->at(serverIds[0]).replicationId);
    EXPECT_EQ(10U, serverList->at(serverIds[1]).replicationId);
    EXPECT_EQ(10U, serverList->at(serverIds[2]).replicationId);

    // Crash one of the backups. assignReplicationGroup should fail.
    // This test disabled because of RAM-429.
#if 0
    serverManager->hintServerDown(serverIds[2]);
    EXPECT_FALSE(serverManager->assignReplicationGroup(1000U, serverIds));
#endif
    serverManager->forceServerDownForTesting = false;
}

TEST_F(CoordinatorServerManagerTest, createReplicationGroup) {
    ServerId serverIds[9];
    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::BACKUP_SERVICE,
        WireFormat::MEMBERSHIP_SERVICE, WireFormat::PING_SERVICE};
    for (uint32_t i = 0; i < 8; i++) {
        config.localLocator = format("mock:host=backup%u", i);
        serverIds[i] = cluster.addServer(config)->serverId;
    }
    EXPECT_EQ(1U, serverList->at(serverIds[0]).replicationId);
    EXPECT_EQ(1U, serverList->at(serverIds[1]).replicationId);
    EXPECT_EQ(1U, serverList->at(serverIds[2]).replicationId);
    EXPECT_EQ(2U, serverList->at(serverIds[3]).replicationId);
    EXPECT_EQ(2U, serverList->at(serverIds[4]).replicationId);
    EXPECT_EQ(2U, serverList->at(serverIds[5]).replicationId);
    EXPECT_EQ(0U, serverList->at(serverIds[6]).replicationId);
    EXPECT_EQ(0U, serverList->at(serverIds[7]).replicationId);
    // Kill server 7.
    serverManager->forceServerDownForTesting = true;
    serverManager->hintServerDown(serverIds[7]);
    serverManager->forceServerDownForTesting = false;
    // Create a new server.
    config.localLocator = format("mock:host=backup%u", 9);
    serverIds[8] = cluster.addServer(config)->serverId;
    EXPECT_EQ(0U, serverList->at(serverIds[6]).replicationId);
    EXPECT_EQ(0U, serverList->at(serverIds[8]).replicationId);
}

TEST_F(CoordinatorServerManagerTest, enlistServer) {
    EXPECT_EQ(1U, master->serverId.getId());
    EXPECT_EQ(ServerId(2, 0),
        CoordinatorClient::enlistServer(context, {},
                                        {WireFormat::BACKUP_SERVICE},
                                        "mock:host=backup"));

    ProtoBuf::ServerList masterList;
    serverList->serialize(masterList, {WireFormat::MASTER_SERVICE});
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
                "server { services: 25 server_id: 1 "
                "service_locator: \"mock:host=master\" "
                "expected_read_mbytes_per_sec: [0-9]\\+ status: 0 } "
                "version_number: 2",
                masterList.ShortDebugString()));

    ProtoBuf::ServerList backupList;
    serverList->serialize(backupList, {WireFormat::BACKUP_SERVICE});
    EXPECT_EQ("server { services: 2 server_id: 2 "
              "service_locator: \"mock:host=backup\" "
              "expected_read_mbytes_per_sec: 0 status: 0 } "
              "version_number: 2",
              backupList.ShortDebugString());
}

namespace {
bool startMasterRecoveryFilter(string s) {
    return s == "startMasterRecovery";
}
}

TEST_F(CoordinatorServerManagerTest, enlistServerReplaceAMaster) {
    TaskQueue mgr;
    serverManager->service.recoveryManager.doNotStartRecoveries = true;

    ramcloud->createTable("foo");
    TestLog::Enable _(startMasterRecoveryFilter);
    EXPECT_EQ(ServerId(2, 0),
        CoordinatorClient::enlistServer(context, masterServerId,
                                        {WireFormat::BACKUP_SERVICE},
                                        "mock:host=backup"));
    EXPECT_EQ("startMasterRecovery: Scheduling recovery of master 1 | "
              "startMasterRecovery: Recovery crashedServerId: 1",
              TestLog::get());
    EXPECT_TRUE(serverList->contains(masterServerId));
    EXPECT_EQ(ServerStatus::CRASHED,
              serverList->at(masterServerId).status);
}

TEST_F(CoordinatorServerManagerTest, enlistServerReplaceANonMaster) {
    TaskQueue mgr;
    serverManager->service.recoveryManager.doNotStartRecoveries = true;

    ServerConfig config = ServerConfig::forTesting();
    config.localLocator = "mock:host=backup1";
    config.services = {WireFormat::BACKUP_SERVICE};
    ServerId replacesId = cluster.addServer(config)->serverId;

    TestLog::Enable _(startMasterRecoveryFilter);
    EXPECT_EQ(ServerId(2, 1),
        CoordinatorClient::enlistServer(context, replacesId,
                                        {WireFormat::BACKUP_SERVICE},
                                        "mock:host=backup2"));
    EXPECT_EQ("startMasterRecovery: Server 2 crashed, but it had no tablets",
              TestLog::get());
    EXPECT_FALSE(serverList->contains(replacesId));
}

// TODO(ankitak): Improve the test.
TEST_F(CoordinatorServerManagerTest, enlistServerLogCabin) {
    TaskQueue mgr;
    serverManager->service.recoveryManager.doNotStartRecoveries = true;

    ramcloud->createTable("foo");

    EXPECT_EQ(ServerId(2, 0),
        CoordinatorClient::enlistServer(context, masterServerId,
                                        {WireFormat::BACKUP_SERVICE},
                                        "mock:host=backup"));

    ProtoBuf::StateEnlistServer readState;
    serverManager->service.logCabinHelper->getProtoBufFromEntryId(
        3, readState);
    EXPECT_EQ("entry_type: \"StateEnlistServer\"\n"
              "replaces_id: 1\n"
              "new_server_id: 2\nservice_mask: 2\n"
              "read_speed: 0\nwrite_speed: 0\n"
              "service_locator: \"mock:host=backup\"\n",
              readState.DebugString());

    ProtoBuf::ServerInformation readInfo;
    serverManager->service.logCabinHelper->getProtoBufFromEntryId(
        4, readInfo);
    EXPECT_EQ("entry_type: \"ServerInformation\"\n"
              "server_id: 2\nservice_mask: 2\n"
              "read_speed: 0\nwrite_speed: 0\n"
              "service_locator: \"mock:host=backup\"\n",
              readInfo.DebugString());
}

TEST_F(CoordinatorServerManagerTest, getServerList) {
    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    master2Config.services = {WireFormat::MASTER_SERVICE,
        WireFormat::BACKUP_SERVICE, WireFormat::PING_SERVICE};
    cluster.addServer(master2Config);
    ServerConfig backupConfig = masterConfig;
    backupConfig.localLocator = "mock:host=backup1";
    backupConfig.services = {WireFormat::BACKUP_SERVICE,
        WireFormat::PING_SERVICE};
    cluster.addServer(backupConfig);
    ProtoBuf::ServerList list;
    CoordinatorClient::getServerList(context, list);
    EXPECT_EQ("mock:host=master mock:host=master2 mock:host=backup1",
            getLocators(list));
}

TEST_F(CoordinatorServerManagerTest, getServerList_backups) {
    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    master2Config.services = {WireFormat::MASTER_SERVICE,
        WireFormat::BACKUP_SERVICE, WireFormat::PING_SERVICE};
    cluster.addServer(master2Config);
    ServerConfig backupConfig = masterConfig;
    backupConfig.localLocator = "mock:host=backup1";
    backupConfig.services = {WireFormat::BACKUP_SERVICE,
        WireFormat::PING_SERVICE};
    cluster.addServer(backupConfig);
    ProtoBuf::ServerList list;
    CoordinatorClient::getBackupList(context, list);
    EXPECT_EQ("mock:host=master2 mock:host=backup1",
            getLocators(list));
}

TEST_F(CoordinatorServerManagerTest, getServerList_masters) {
    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    master2Config.services = {WireFormat::MASTER_SERVICE,
        WireFormat::BACKUP_SERVICE, WireFormat::PING_SERVICE};
    cluster.addServer(master2Config);
    ServerConfig backupConfig = masterConfig;
    backupConfig.localLocator = "mock:host=backup1";
    backupConfig.services = {WireFormat::BACKUP_SERVICE,
        WireFormat::PING_SERVICE};
    cluster.addServer(backupConfig);
    ProtoBuf::ServerList list;
    CoordinatorClient::getMasterList(context, list);
    EXPECT_EQ("mock:host=master mock:host=master2",
            getLocators(list));
}

TEST_F(CoordinatorServerManagerTest, hintServerDown_backup) {
    ServerId id =
        CoordinatorClient::enlistServer(context, {},
                                        {WireFormat::BACKUP_SERVICE},
                                        "mock:host=backup");
    EXPECT_EQ(1U, serverManager->service.serverList.backupCount());
    serverManager->forceServerDownForTesting = true;
    CoordinatorClient::hintServerDown(context, id);
    EXPECT_EQ(0U, serverList->backupCount());
    EXPECT_FALSE(serverList->contains(id));
}

TEST_F(CoordinatorServerManagerTest, hintServerDown_server) {
    TaskQueue mgr;
    serverManager->service.recoveryManager.doNotStartRecoveries = true;
    // master is already enlisted
    CoordinatorClient::enlistServer(context, {},
                                    {WireFormat::MASTER_SERVICE,
                                    WireFormat::PING_SERVICE},
                                    "mock:host=master2");
    CoordinatorClient::enlistServer(context, {}, {WireFormat::BACKUP_SERVICE},
                                    "mock:host=backup");
    ramcloud->createTable("foo");
    serverManager->forceServerDownForTesting = true;
    TestLog::Enable _(startMasterRecoveryFilter);
    CoordinatorClient::hintServerDown(context, masterServerId);
    EXPECT_EQ("startMasterRecovery: Scheduling recovery of master 1 | "
              "startMasterRecovery: Recovery crashedServerId: 1",
               TestLog::get());
    EXPECT_EQ(ServerStatus::CRASHED,
              serverList->at(master->serverId).status);
}

TEST_F(CoordinatorServerManagerTest, hintServerDownRecover) {
    TaskQueue mgr;
    serverManager->service.recoveryManager.doNotStartRecoveries = true;
    // master is already enlisted

    ramcloud->createTable("foo");
    serverManager->forceServerDownForTesting = true;
    TestLog::Enable _(startMasterRecoveryFilter);

    ProtoBuf::StateHintServerDown state;
    state.set_entry_type("StateHintServerDown");
    state.set_server_id(masterServerId.getId());

    EntryId entryId =
        serverManager->service.logCabinHelper->appendProtoBuf(state);

    serverManager->hintServerDownRecover(&state, entryId);

    EXPECT_EQ("startMasterRecovery: Scheduling recovery of master 1 | "
              "startMasterRecovery: Recovery crashedServerId: 1",
               TestLog::get());
    EXPECT_EQ(ServerStatus::CRASHED,
              serverList->at(master->serverId).status);
}

TEST_F(CoordinatorServerManagerTest, hintServerDown_execute) {
    TaskQueue mgr;
    serverManager->service.recoveryManager.doNotStartRecoveries = true;
    // master is already enlisted

    ramcloud->createTable("foo");
    serverManager->forceServerDownForTesting = true;

    TestLog::Enable _;
    serverManager->hintServerDown(masterServerId);

    string searchString = "execute: LogCabin entryId: ";
    ASSERT_NE(string::npos, TestLog::get().find(searchString));
    string entryIdString = TestLog::get().substr(
        TestLog::get().find(searchString) + searchString.length(), 1);
    EntryId entryId = strtoul(entryIdString.c_str(), NULL, 0);

    ProtoBuf::StateHintServerDown readState;
    serverManager->service.logCabinHelper->getProtoBufFromEntryId(
        entryId, readState);

    EXPECT_EQ("entry_type: \"StateHintServerDown\"\nserver_id: 1\n",
              readState.DebugString());
}

TEST_F(CoordinatorServerManagerTest, removeReplicationGroup) {
    ServerId serverIds[3];
    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::BACKUP_SERVICE,
            WireFormat::MEMBERSHIP_SERVICE, WireFormat::PING_SERVICE};
    for (uint32_t i = 0; i < 3; i++) {
        config.localLocator = format("mock:host=backup%u", i);
        serverIds[i] = cluster.addServer(config)->serverId;
    }
    serverManager->removeReplicationGroup(1);
    EXPECT_EQ(0U, serverList->at(serverIds[0]).replicationId);
    EXPECT_EQ(0U, serverList->at(serverIds[1]).replicationId);
    EXPECT_EQ(0U, serverList->at(serverIds[2]).replicationId);
}

namespace {
bool statusFilter(string s) {
    return s != "checkStatus";
}
}

TEST_F(CoordinatorServerManagerTest, sendServerList_checkLogs) {
    TestLog::Enable _(statusFilter);

    CoordinatorClient::sendServerList(context, ServerId(52, 0));
    EXPECT_EQ(0U, TestLog::get().find(
        "sendServerList: Could not send list to unknown server 52"));

    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::MASTER_SERVICE, WireFormat::PING_SERVICE};
    ServerId id = cluster.addServer(config)->serverId;

    TestLog::reset();
    CoordinatorClient::sendServerList(context, id);
    cluster.syncCoordinatorServerList();
    EXPECT_EQ(0U, TestLog::get().find(
        "sendServerList: Could not send list to server without "
        "membership service: 2"));

    config = ServerConfig::forTesting();
    config.services = {WireFormat::MEMBERSHIP_SERVICE};
    id = cluster.addServer(config)->serverId;

    TestLog::reset();
    CoordinatorClient::sendServerList(context, id);
    cluster.syncCoordinatorServerList();
    EXPECT_EQ(0U, TestLog::get().find(
        "handleRequest: Sending server list to server id"));
    EXPECT_NE(string::npos, TestLog::get().find(
        "applyFullList: Got complete list of servers"));

    serverList->crashed(id);
    cluster.syncCoordinatorServerList();    // clear crashed() messages
    TestLog::reset();
    CoordinatorClient::sendServerList(context, id);
    cluster.syncCoordinatorServerList();
    EXPECT_NE(std::string::npos, TestLog::get().find(
            "sendServerList: Could not send list to crashed server 3"));
}

TEST_F(CoordinatorServerManagerTest, sendServerList_main) {
    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::MEMBERSHIP_SERVICE};
    ServerId id = cluster.addServer(config)->serverId;

    TestLog::Enable _;
    serverManager->sendServerList(id);
    cluster.syncCoordinatorServerList();
    EXPECT_NE(string::npos, TestLog::get().find(
        "applyFullList: Got complete list of servers containing 1 "
        "entries (version number 2)"));
}

TEST_F(CoordinatorServerManagerTest, serverDown) {
    TaskQueue mgr;
    serverManager->service.recoveryManager.doNotStartRecoveries = true;
    // master is already enlisted

    ramcloud->createTable("foo");
    serverManager->forceServerDownForTesting = true;
    TestLog::Enable _(startMasterRecoveryFilter);

    serverManager->serverDown(masterServerId);

    EXPECT_EQ("startMasterRecovery: Scheduling recovery of master 1 | "
              "startMasterRecovery: Recovery crashedServerId: 1",
               TestLog::get());
    EXPECT_EQ(ServerStatus::CRASHED,
              serverList->at(master->serverId).status);
}

TEST_F(CoordinatorServerManagerTest, setMinOpenSegmentId) {
    serverManager->setMinOpenSegmentId(masterServerId, 10);
    EXPECT_EQ(10u, serverList->at(masterServerId).minOpenSegmentId);
    serverManager->setMinOpenSegmentId(masterServerId, 9);
    EXPECT_EQ(10u, serverList->at(masterServerId).minOpenSegmentId);
    serverManager->setMinOpenSegmentId(masterServerId, 11);
    EXPECT_EQ(11u, serverList->at(masterServerId).minOpenSegmentId);
}

TEST_F(CoordinatorServerManagerTest, setMinOpenSegmentIdRecover) {
    EntryId oldEntryId = serverList->getLogCabinEntryId(masterServerId);
    ProtoBuf::ServerInformation serverInfo;
    serverManager->service.logCabinHelper->getProtoBufFromEntryId(
        oldEntryId, serverInfo);

    serverInfo.set_min_open_segment_id(10);
    EntryId newEntryId =
        serverManager->service.logCabinHelper->appendProtoBuf(serverInfo);

    serverManager->setMinOpenSegmentIdRecover(&serverInfo, newEntryId);

    EXPECT_EQ(10u, serverList->at(masterServerId).minOpenSegmentId);
}

TEST_F(CoordinatorServerManagerTest, setMinOpenSegmentId_execute) {
    TestLog::Enable _;
    serverManager->setMinOpenSegmentId(masterServerId, 10);

    EntryId entryId = serverList->getLogCabinEntryId(masterServerId);
    ProtoBuf::ServerInformation readInfo;
    serverManager->service.logCabinHelper->getProtoBufFromEntryId(
        entryId, readInfo);

    EXPECT_EQ(10u, readInfo.min_open_segment_id());
}

TEST_F(CoordinatorServerManagerTest, setMinOpenSegmentId_complete_noSuchServer)
{
    EXPECT_THROW(serverManager->setMinOpenSegmentId(ServerId(2, 2), 100),
                 ServerListException);
}

TEST_F(CoordinatorServerManagerTest, verifyServerFailure) {
    // Case 1: server up.
    EXPECT_FALSE(serverManager->verifyServerFailure(masterServerId));

    // Case 2: server incommunicado.
    MockTransport mockTransport(context);
    context.transportManager->registerMock(&mockTransport, "mock2");
    ServerId deadId = serverList->add("mock2:", {WireFormat::PING_SERVICE},
                                      100);
    EXPECT_TRUE(serverManager->verifyServerFailure(deadId));
    context.transportManager->unregisterMock();
}

}  // namespace RAMCloud
