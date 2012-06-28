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
#include "Recovery.h"
#include "ServerList.h"
#include "TaskQueue.h"

namespace RAMCloud {

class CoordinatorServerManagerTest : public ::testing::Test {
  public:
    Context context;
    ServerConfig masterConfig;
    MockCluster cluster;
    CoordinatorClient* client;
    CoordinatorServerManager* serverManager;
    MasterService* master;
    ServerId masterServerId;

    CoordinatorServerManagerTest()
        : context()
        , masterConfig(ServerConfig::forTesting())
        , cluster(context)
        , client()
        , serverManager()
        , master()
        , masterServerId()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        serverManager = &cluster.coordinator.get()->serverManager;

        masterConfig.services = {MASTER_SERVICE, PING_SERVICE,
                                 MEMBERSHIP_SERVICE};
        masterConfig.localLocator = "mock:host=master";
        Server* masterServer = cluster.addServer(masterConfig);
        master = masterServer->master.get();
        masterServerId = masterServer->serverId;

        client = cluster.getCoordinatorClient();
    }

    DISALLOW_COPY_AND_ASSIGN(CoordinatorServerManagerTest);
};

TEST_F(CoordinatorServerManagerTest, assignReplicationGroup) {
    vector<ServerId> serverIds;
    ServerConfig config = ServerConfig::forTesting();
    config.services = {BACKUP_SERVICE, MEMBERSHIP_SERVICE, PING_SERVICE};
    for (uint32_t i = 0; i < 3; i++) {
        config.localLocator = format("mock:host=backup%u", i);
        serverIds.push_back(cluster.addServer(config)->serverId);
    }
    // Check normal functionality.
    EXPECT_TRUE(serverManager->assignReplicationGroup(10U, serverIds));
    EXPECT_EQ(10U,
              serverManager->service.serverList[serverIds[0]].replicationId);
    EXPECT_EQ(10U,
              serverManager->service.serverList[serverIds[1]].replicationId);
    EXPECT_EQ(10U,
              serverManager->service.serverList[serverIds[2]].replicationId);
    // Generate a single TransportException. assignReplicationGroup should
    // retry and succeed.
    cluster.transport.errorMessage = "I am Bon Jovi's pool cleaner!";
    EXPECT_TRUE(serverManager->assignReplicationGroup(100U, serverIds));
    EXPECT_EQ(100U,
              serverManager->service.serverList[serverIds[0]].replicationId);
    serverManager->forceServerDownForTesting = true;
    // Crash one of the backups. assignReplicationGroup should fail.
    serverManager->hintServerDown(serverIds[2]);
    EXPECT_FALSE(serverManager->assignReplicationGroup(1000U, serverIds));
    serverManager->forceServerDownForTesting = false;
}

TEST_F(CoordinatorServerManagerTest, createReplicationGroup) {
    ServerId serverIds[9];
    ServerConfig config = ServerConfig::forTesting();
    config.services = {BACKUP_SERVICE, MEMBERSHIP_SERVICE, PING_SERVICE};
    for (uint32_t i = 0; i < 8; i++) {
        config.localLocator = format("mock:host=backup%u", i);
        serverIds[i] = cluster.addServer(config)->serverId;
    }
    EXPECT_EQ(1U,
              serverManager->service.serverList[serverIds[0]].replicationId);
    EXPECT_EQ(1U,
              serverManager->service.serverList[serverIds[1]].replicationId);
    EXPECT_EQ(1U,
              serverManager->service.serverList[serverIds[2]].replicationId);
    EXPECT_EQ(2U,
              serverManager->service.serverList[serverIds[3]].replicationId);
    EXPECT_EQ(2U,
              serverManager->service.serverList[serverIds[4]].replicationId);
    EXPECT_EQ(2U,
              serverManager->service.serverList[serverIds[5]].replicationId);
    EXPECT_EQ(0U,
              serverManager->service.serverList[serverIds[6]].replicationId);
    EXPECT_EQ(0U,
              serverManager->service.serverList[serverIds[7]].replicationId);
    // Kill server 7.
    serverManager->forceServerDownForTesting = true;
    serverManager->hintServerDown(serverIds[7]);
    serverManager->forceServerDownForTesting = false;
    // Create a new server.
    config.localLocator = format("mock:host=backup%u", 9);
    serverIds[8] = cluster.addServer(config)->serverId;
    EXPECT_EQ(0U,
              serverManager->service.serverList[serverIds[6]].replicationId);
    EXPECT_EQ(0U,
              serverManager->service.serverList[serverIds[8]].replicationId);
}

TEST_F(CoordinatorServerManagerTest, enlistServer) {
    EXPECT_EQ(1U, master->serverId.getId());
    EXPECT_EQ(ServerId(2, 0),
        client->enlistServer({}, {BACKUP_SERVICE}, "mock:host=backup"));

    ProtoBuf::ServerList masterList;
    serverManager->service.serverList.serialize(masterList, {MASTER_SERVICE});
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
                "server { services: 25 server_id: 1 "
                "service_locator: \"mock:host=master\" "
                "expected_read_mbytes_per_sec: [0-9]\\+ status: 0 } "
                "version_number: 2",
                masterList.ShortDebugString()));

    ProtoBuf::Tablets& will = *serverManager->service.serverList[1]->will;
    EXPECT_EQ(0, will.tablet_size());

    ProtoBuf::ServerList backupList;
    serverManager->service.serverList.serialize(backupList, {BACKUP_SERVICE});
    EXPECT_EQ("server { services: 2 server_id: 2 "
              "service_locator: \"mock:host=backup\" "
              "expected_read_mbytes_per_sec: 0 status: 0 } "
              "version_number: 2",
              backupList.ShortDebugString());
}

namespace {
bool startMasterRecoveryFilter(string s) {
    return s == "startMasterRecovery" || s == "restartMasterRecovery";
}
}

TEST_F(CoordinatorServerManagerTest, enlistServerReplaceAMaster) {
    TaskQueue mgr;
    serverManager->service.recoveryManager.doNotStartRecoveries = true;

    client->createTable("foo");
    TestLog::Enable _(startMasterRecoveryFilter);
    EXPECT_EQ(ServerId(2, 0),
        client->enlistServer(masterServerId,
                             {BACKUP_SERVICE}, "mock:host=backup"));
    EXPECT_EQ("restartMasterRecovery: Scheduling recovery of master 1 | "
              "restartMasterRecovery: Recovery crashedServerId: 1 | "
              "restartMasterRecovery: Recovery will: tablet { table_id: 0 "
                  "start_key_hash: 0 end_key_hash: 18446744073709551615 "
                  "state: NORMAL user_data: 0 "
                  "ctime_log_head_id: 0 ctime_log_head_offset: 0 }",
              TestLog::get());
    EXPECT_TRUE(serverManager->service.serverList.contains(masterServerId));
    EXPECT_EQ(ServerStatus::CRASHED,
              serverManager->service.serverList[masterServerId].status);
}

TEST_F(CoordinatorServerManagerTest, enlistServerReplaceANonMaster) {
    TaskQueue mgr;
    serverManager->service.recoveryManager.doNotStartRecoveries = true;

    ServerConfig config = ServerConfig::forTesting();
    config.localLocator = "mock:host=backup1";
    config.services = {BACKUP_SERVICE};
    ServerId replacesId = cluster.addServer(config)->serverId;

    TestLog::Enable _(startMasterRecoveryFilter);
    EXPECT_EQ(ServerId(2, 1),
        client->enlistServer(replacesId,
                             {BACKUP_SERVICE}, "mock:host=backup2"));
    EXPECT_EQ("startMasterRecovery: Server 2 crashed, but it had no tablets",
              TestLog::get());
    EXPECT_FALSE(serverManager->service.serverList.contains(replacesId));
}

TEST_F(CoordinatorServerManagerTest, getMasterList) {
    // master is already enlisted
    ProtoBuf::ServerList masterList;
    client->getMasterList(masterList);
    // need to avoid non-deterministic mbytes_per_sec field.
    EXPECT_EQ(0U, masterList.ShortDebugString().find(
              "server { services: 25 server_id: 1 "
              "service_locator: \"mock:host=master\" "
              "expected_read_mbytes_per_sec: "));
}

TEST_F(CoordinatorServerManagerTest, getBackupList) {
    // master is already enlisted
    client->enlistServer({}, {BACKUP_SERVICE}, "mock:host=backup1");
    client->enlistServer({}, {BACKUP_SERVICE}, "mock:host=backup2");
    ProtoBuf::ServerList backupList;
    client->getBackupList(backupList);
    EXPECT_EQ("server { services: 2 server_id: 2 "
              "service_locator: \"mock:host=backup1\" "
              "expected_read_mbytes_per_sec: 0 "
              "status: 0 } "
              "server { services: 2 server_id: 3 "
              "service_locator: \"mock:host=backup2\" "
              "expected_read_mbytes_per_sec: 0 "
              "status: 0 } version_number: 3",
              backupList.ShortDebugString());
}

TEST_F(CoordinatorServerManagerTest, getServerList) {
    // master is already enlisted
    client->enlistServer({}, {BACKUP_SERVICE}, "mock:host=backup1");
    ProtoBuf::ServerList serverList;
    client->getServerList(serverList);
    EXPECT_EQ(2, serverList.server_size());
    auto masterMask = ServiceMask{MASTER_SERVICE, PING_SERVICE,
                                  MEMBERSHIP_SERVICE}.serialize();
    EXPECT_EQ(masterMask, serverList.server(0).services());
    auto backupMask = ServiceMask{BACKUP_SERVICE}.serialize();
    EXPECT_EQ(backupMask, serverList.server(1).services());
}

TEST_F(CoordinatorServerManagerTest, hintServerDown_backup) {
    ServerId id =
        client->enlistServer({}, {BACKUP_SERVICE}, "mock:host=backup");
    EXPECT_EQ(1U, serverManager->service.serverList.backupCount());
    serverManager->forceServerDownForTesting = true;
    client->hintServerDown(id);
    EXPECT_EQ(0U, serverManager->service.serverList.backupCount());
    EXPECT_FALSE(serverManager->service.serverList.contains(id));
}

TEST_F(CoordinatorServerManagerTest, hintServerDown_server) {
    TaskQueue mgr;
    serverManager->service.recoveryManager.doNotStartRecoveries = true;
    // master is already enlisted
    client->enlistServer({}, {MASTER_SERVICE, PING_SERVICE},
                         "mock:host=master2");
    client->enlistServer({}, {BACKUP_SERVICE}, "mock:host=backup");
    client->createTable("foo");
    serverManager->forceServerDownForTesting = true;
    TestLog::Enable _(startMasterRecoveryFilter);
    client->hintServerDown(masterServerId);
    EXPECT_EQ("restartMasterRecovery: Scheduling recovery of master 1 | "
              "restartMasterRecovery: Recovery crashedServerId: 1 | "
              "restartMasterRecovery: Recovery will: tablet { table_id: 0 "
                  "start_key_hash: 0 end_key_hash: 18446744073709551615 "
                  "state: NORMAL user_data: 0 "
                  "ctime_log_head_id: 0 ctime_log_head_offset: 0 }",
               TestLog::get());
    EXPECT_EQ(ServerStatus::CRASHED,
              serverManager->service.serverList[master->serverId].status);
}

TEST_F(CoordinatorServerManagerTest, removeReplicationGroup) {
    ServerId serverIds[3];
    ServerConfig config = ServerConfig::forTesting();
    config.services = {BACKUP_SERVICE, MEMBERSHIP_SERVICE, PING_SERVICE};
    for (uint32_t i = 0; i < 3; i++) {
        config.localLocator = format("mock:host=backup%u", i);
        serverIds[i] = cluster.addServer(config)->serverId;
    }
    serverManager->removeReplicationGroup(1);
    EXPECT_EQ(0U,
              serverManager->service.serverList[serverIds[0]].replicationId);
    EXPECT_EQ(0U,
              serverManager->service.serverList[serverIds[1]].replicationId);
    EXPECT_EQ(0U,
              serverManager->service.serverList[serverIds[2]].replicationId);
}

namespace {
bool statusFilter(string s) {
    return s != "checkStatus";
}
}

TEST_F(CoordinatorServerManagerTest, sendServerList_checkLogs) {
    TestLog::Enable _(statusFilter);

    client->sendServerList(ServerId(52, 0));
    EXPECT_EQ(0U, TestLog::get().find(
        "sendServerList: Could not send list to unknown server 52"));

    ServerConfig config = ServerConfig::forTesting();
    config.services = {MASTER_SERVICE, PING_SERVICE};
    ServerId id = cluster.addServer(config)->serverId;

    TestLog::reset();
    client->sendServerList(id);
    EXPECT_EQ(0U, TestLog::get().find(
        "sendServerList: Could not send list to server without "
        "membership service: 2"));

    config = ServerConfig::forTesting();
    config.services = {MEMBERSHIP_SERVICE};
    id = cluster.addServer(config)->serverId;

    TestLog::reset();
    client->sendServerList(id);
    EXPECT_EQ(0U, TestLog::get().find(
        "sendServerList: Sending server list to server id"));
    EXPECT_NE(string::npos, TestLog::get().find(
        "applyFullList: Got complete list of servers"));

    ProtoBuf::ServerList update;
    serverManager->service.serverList.crashed(id, update);
    TestLog::reset();
    client->sendServerList(id);
    EXPECT_EQ(
        "sendServerList: Could not send list to crashed server 3",
        TestLog::get());
}

TEST_F(CoordinatorServerManagerTest, sendServerList_main) {
    ServerConfig config = ServerConfig::forTesting();
    config.services = {MEMBERSHIP_SERVICE};
    ServerId id = cluster.addServer(config)->serverId;

    TestLog::Enable _;
    serverManager->sendServerList(id);
    EXPECT_NE(string::npos, TestLog::get().find(
        "applyFullList: Got complete list of servers containing 1 "
        "entries (version number 2)"));
}

TEST_F(CoordinatorServerManagerTest, setMinOpenSegmentId) {
    EXPECT_THROW(client->setMinOpenSegmentId(ServerId(2, 2), 100),
                 ClientException);

    client->setMinOpenSegmentId(masterServerId, 10);
    EXPECT_EQ(10u,
        serverManager->service.serverList[masterServerId].minOpenSegmentId);
    client->setMinOpenSegmentId(masterServerId, 9);
    EXPECT_EQ(10u,
        serverManager->service.serverList[masterServerId].minOpenSegmentId);
    client->setMinOpenSegmentId(masterServerId, 11);
    EXPECT_EQ(11u,
        serverManager->service.serverList[masterServerId].minOpenSegmentId);
}

static bool
setWillFilter(string s) {
    return s == "setWill";
}

TEST_F(CoordinatorServerManagerTest, setWill) {
    client->enlistServer({}, {MASTER_SERVICE}, "mock:host=master2");

    ProtoBuf::Tablets will;
    ProtoBuf::Tablets::Tablet& t(*will.add_tablet());
    t.set_table_id(0);
    t.set_start_key_hash(235);
    t.set_end_key_hash(47234);
    t.set_state(ProtoBuf::Tablets::Tablet::NORMAL);
    t.set_user_data(19);
    t.set_ctime_log_head_id(0);
    t.set_ctime_log_head_offset(0);

    TestLog::Enable _(&setWillFilter);
    client->setWill(2, will);

    EXPECT_EQ("setWill: Master 2 updated its Will (now 1 entries, was 0)",
              TestLog::get());

    // bad master id should fail
    EXPECT_THROW(client->setWill(23481234, will), InternalError);
}

TEST_F(CoordinatorServerManagerTest, verifyServerFailure) {
    EXPECT_FALSE(serverManager->verifyServerFailure(masterServerId));
    cluster.transport.errorMessage = "Server gone!";
    EXPECT_TRUE(serverManager->verifyServerFailure(masterServerId));
}

}  // namespace RAMCloud
