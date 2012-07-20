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
#include "CoordinatorClient.h"
#include "CoordinatorService.h"
#include "MasterService.h"
#include "MembershipService.h"
#include "MockCluster.h"
#include "Recovery.h"
#include "ServerList.h"
#include "TaskQueue.h"

namespace RAMCloud {

class CoordinatorServiceTest : public ::testing::Test {
  public:
    Context context;
    ServerConfig masterConfig;
    MockCluster cluster;
    CoordinatorClient* client;
    CoordinatorService* service;
    MasterService* master;
    ServerId masterServerId;

    CoordinatorServiceTest()
        : context()
        , masterConfig(ServerConfig::forTesting())
        , cluster(context)
        , client()
        , service()
        , master()
        , masterServerId()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        service = cluster.coordinator.get();

        masterConfig.services = {MASTER_SERVICE, PING_SERVICE,
                                 MEMBERSHIP_SERVICE};
        masterConfig.localLocator = "mock:host=master";
        Server* masterServer = cluster.addServer(masterConfig);
        master = masterServer->master.get();
        masterServerId = masterServer->serverId;

        client = cluster.getCoordinatorClient();
    }

    DISALLOW_COPY_AND_ASSIGN(CoordinatorServiceTest);
};

TEST_F(CoordinatorServiceTest, createTable) {
    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    MasterService& master2 = *cluster.addServer(master2Config)->master;

    // Advance the log head slightly so creation time offset is non-zero.
    Buffer empty;
    HashTable::Reference reference;
    master->log.append(LOG_ENTRY_TYPE_OBJ, empty, true, reference);

    // master is already enlisted
    EXPECT_EQ(0U, client->createTable("foo"));
    EXPECT_EQ(0U, client->createTable("foo")); // should be no-op
    EXPECT_EQ(1U, client->createTable("bar")); // should go to master2
    EXPECT_EQ(2U, client->createTable("baz")); // and back to master1

    EXPECT_EQ(0U, get(service->tables, "foo"));
    EXPECT_EQ(1U, get(service->tables, "bar"));
    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1 status: NORMAL "
              "ctime: 0, 72 } "
              "Tablet { tableId: 1 startKeyHash: 0 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 2 status: NORMAL "
              "ctime: 0, 0 } "
              "Tablet { tableId: 2 startKeyHash: 0 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1 status: NORMAL "
              "ctime: 0, 72 }",
              service->tabletMap.debugString());
    ProtoBuf::Tablets& will1 = *service->serverList[1]->will;
    EXPECT_EQ("tablet { table_id: 0 start_key_hash: 0 "
              "end_key_hash: 18446744073709551615 "
              "state: NORMAL user_data: 0 "
              "ctime_log_head_id: 0 ctime_log_head_offset: 72 } "
              "tablet { table_id: 2 start_key_hash: 0 "
              "end_key_hash: 18446744073709551615 "
              "state: NORMAL user_data: 1 "
              "ctime_log_head_id: 0 ctime_log_head_offset: 72 }",
              will1.ShortDebugString());
    ProtoBuf::Tablets& will2 = *service->serverList[2]->will;
    EXPECT_EQ("tablet { table_id: 1 start_key_hash: 0 "
              "end_key_hash: 18446744073709551615 "
              "state: NORMAL user_data: 0 "
              "ctime_log_head_id: 0 ctime_log_head_offset: 0 }",
              will2.ShortDebugString());
    EXPECT_EQ(2, master->tablets.tablet_size());
    EXPECT_EQ(1, master2.tablets.tablet_size());
}

TEST_F(CoordinatorServiceTest,
  createTableSpannedAcrossTwoMastersWithThreeServers) {
    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    MasterService& master2 = *cluster.addServer(master2Config)->master;
    ServerConfig master3Config = masterConfig;
    master3Config.localLocator = "mock:host=master3";
    MasterService& master3 = *cluster.addServer(master3Config)->master;
    // master is already enlisted
    client->createTable("foo", 2);
    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 9223372036854775807 "
              "serverId: 1 status: NORMAL "
              "ctime: 0, 0 } "
              "Tablet { tableId: 0 startKeyHash: 9223372036854775808 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 2 status: NORMAL "
              "ctime: 0, 0 }",
              service->tabletMap.debugString());
    EXPECT_EQ(1, master->tablets.tablet_size());
    EXPECT_EQ(1, master2.tablets.tablet_size());
    EXPECT_EQ(0, master3.tablets.tablet_size());
}


TEST_F(CoordinatorServiceTest,
  createTableSpannedAcrossThreeMastersWithTwoServers) {
    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    MasterService& master2 = *cluster.addServer(master2Config)->master;
    // master is already enlisted
    client->createTable("foo", 3);
    // ctime_log_head_offset is non-zero when master1 accepts the
    // second tablet since accepting the first forces the creation
    // of an initial head log segment.
    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 6148914691236517205 "
              "serverId: 1 status: NORMAL "
              "ctime: 0, 0 } "
              "Tablet { tableId: 0 startKeyHash: 6148914691236517206 "
              "endKeyHash: 12297829382473034410 "
              "serverId: 2 status: NORMAL "
              "ctime: 0, 0 } "
              "Tablet { tableId: 0 startKeyHash: 12297829382473034411 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1 status: NORMAL "
              "ctime: 0, 60 }",
              service->tabletMap.debugString());
    EXPECT_EQ(2, master->tablets.tablet_size());
    EXPECT_EQ(1, master2.tablets.tablet_size());
}

TEST_F(CoordinatorServiceTest, splitTablet) {
    // master is already enlisted
    client->createTable("foo");
    client->splitTablet("foo", 0, ~0UL, (~0UL/2));
    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 9223372036854775806 "
              "serverId: 1 status: NORMAL "
              "ctime: 0, 0 } "
              "Tablet { tableId: 0 "
              "startKeyHash: 9223372036854775807 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1 status: NORMAL "
              "ctime: 0, 0 }",
              service->tabletMap.debugString());

    client->splitTablet("foo", 0, 9223372036854775806, 4611686018427387903);
    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 4611686018427387902 "
              "serverId: 1 status: NORMAL "
              "ctime: 0, 0 } "
              "Tablet { tableId: 0 "
              "startKeyHash: 9223372036854775807 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1 status: NORMAL "
              "ctime: 0, 0 } "
              "Tablet { tableId: 0 "
              "startKeyHash: 4611686018427387903 "
              "endKeyHash: 9223372036854775806 "
              "serverId: 1 status: NORMAL "
              "ctime: 0, 0 }",
              service->tabletMap.debugString());

    EXPECT_THROW(client->splitTablet("foo", 0, 16, 8),
                 TabletDoesntExistException);

    EXPECT_THROW(client->splitTablet("foo", 0, 0, (~0UL/2)),
                 RequestFormatError);

    EXPECT_THROW(client->splitTablet("bar", 0, ~0UL, (~0UL/2)),
                 TableDoesntExistException);
}

TEST_F(CoordinatorServiceTest, dropTable) {
    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    MasterService& master2 = *cluster.addServer(master2Config)->master;

    // Add a table, so the tests won't just compare against an empty tabletMap
    client->createTable("foo");

    // Test dropping a table that is spread across one master
    client->createTable("bar");
    EXPECT_EQ(1, master2.tablets.tablet_size());
    client->dropTable("bar");
    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1 status: NORMAL "
              "ctime: 0, 0 }",
              service->tabletMap.debugString());
    EXPECT_EQ(0, master2.tablets.tablet_size());

    // Test dropping a table that is spread across two masters
    client->createTable("bar", 2);
    EXPECT_EQ(2, master->tablets.tablet_size());
    EXPECT_EQ(1, master2.tablets.tablet_size());
    client->dropTable("bar");
    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1 status: NORMAL "
              "ctime: 0, 0 }",
              service->tabletMap.debugString());
    EXPECT_EQ(1, master->tablets.tablet_size());
    EXPECT_EQ(0, master2.tablets.tablet_size());
}

TEST_F(CoordinatorServiceTest, getTableId) {
    // Get the id for an existing table
    client->createTable("foo");
    uint64_t tableId = client->getTableId("foo");
    EXPECT_EQ(0lu, tableId);

    client->createTable("foo2");
    tableId = client->getTableId("foo2");
    EXPECT_EQ(1lu, tableId);

    // Try to get the id for a non-existing table
    EXPECT_THROW(client->getTableId("bar"), TableDoesntExistException);
}

TEST_F(CoordinatorServiceTest, enlistServer) {
    EXPECT_EQ(1U, master->serverId.getId());
    EXPECT_EQ(ServerId(2, 0),
        client->enlistServer({}, {BACKUP_SERVICE}, "mock:host=backup"));

    ProtoBuf::ServerList masterList;
    service->serverList.serialize(masterList, {MASTER_SERVICE});
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
                "server { services: 25 server_id: 1 "
                "service_locator: \"mock:host=master\" "
                "expected_read_mbytes_per_sec: [0-9]\\+ status: 0 } "
                "version_number: 2",
                masterList.ShortDebugString()));

    ProtoBuf::Tablets& will = *service->serverList[1]->will;
    EXPECT_EQ(0, will.tablet_size());

    ProtoBuf::ServerList backupList;
    service->serverList.serialize(backupList, {BACKUP_SERVICE});
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

TEST_F(CoordinatorServiceTest, enlistServerReplaceAMaster) {
    TaskQueue mgr;
    service->recoveryManager.doNotStartRecoveries = true;

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
    EXPECT_TRUE(service->serverList.contains(masterServerId));
    EXPECT_EQ(ServerStatus::CRASHED,
              service->serverList[masterServerId].status);
}

TEST_F(CoordinatorServiceTest, enlistServerReplaceANonMaster) {
    TaskQueue mgr;
    service->recoveryManager.doNotStartRecoveries = true;

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
    EXPECT_FALSE(service->serverList.contains(replacesId));
}

TEST_F(CoordinatorServiceTest, getMasterList) {
    // master is already enlisted
    ProtoBuf::ServerList masterList;
    client->getMasterList(masterList);
    // need to avoid non-deterministic mbytes_per_sec field.
    EXPECT_EQ(0U, masterList.ShortDebugString().find(
              "server { services: 25 server_id: 1 "
              "service_locator: \"mock:host=master\" "
              "expected_read_mbytes_per_sec: "));
}

TEST_F(CoordinatorServiceTest, getBackupList) {
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

TEST_F(CoordinatorServiceTest, getServerList) {
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

TEST_F(CoordinatorServiceTest, getTabletMap) {
    client->createTable("foo");
    ProtoBuf::Tablets tabletMap;
    client->getTabletMap(tabletMap);
    EXPECT_EQ("tablet { table_id: 0 start_key_hash: 0 "
              "end_key_hash: 18446744073709551615 "
              "state: NORMAL server_id: 1 "
              "service_locator: \"mock:host=master\" "
              "ctime_log_head_id: 0 ctime_log_head_offset: 0 }",
              tabletMap.ShortDebugString());
}

TEST_F(CoordinatorServiceTest, hintServerDown_master) {
    TaskQueue mgr;
    service->recoveryManager.doNotStartRecoveries = true;
    // master is already enlisted
    client->enlistServer({}, {MASTER_SERVICE, PING_SERVICE},
                         "mock:host=master2");
    client->enlistServer({}, {BACKUP_SERVICE}, "mock:host=backup");
    client->createTable("foo");
    service->forceServerDownForTesting = true;
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
              service->serverList[master->serverId].status);
}

TEST_F(CoordinatorServiceTest, hintServerDown_backup) {
    ServerId id =
        client->enlistServer({}, {BACKUP_SERVICE}, "mock:host=backup");
    EXPECT_EQ(1U, service->serverList.backupCount());
    service->forceServerDownForTesting = true;
    client->hintServerDown(id);
    EXPECT_EQ(0U, service->serverList.backupCount());
    EXPECT_FALSE(service->serverList.contains(id));
}

static bool
reassignTabletOwnershipFilter(string s)
{
    return s == "reassignTabletOwnership";
}

TEST_F(CoordinatorServiceTest, reassignTabletOwnership) {
    ServerConfig master2Config = masterConfig;
    master2Config.services = { MASTER_SERVICE,
                               PING_SERVICE,
                               MEMBERSHIP_SERVICE };
    master2Config.localLocator = "mock:host=master2";
    auto* master2 = cluster.addServer(master2Config);

    // Advance the log head slightly so creation time offset is non-zero
    // on host being migrated to.
    Buffer empty;
    HashTable::Reference reference;
    master2->master->log.append(LOG_ENTRY_TYPE_OBJ, empty, true, reference); 

    // master is already enlisted
    client->createTable("foo");
    EXPECT_EQ(1, master->tablets.tablet_size());
    EXPECT_EQ(0, master2->master->tablets.tablet_size());
    Tablet tablet = service->tabletMap.getTablet(0lu, 0lu, ~(0lu));
    EXPECT_EQ(masterServerId, tablet.serverId);
    EXPECT_EQ(0U, tablet.ctime.segmentId());
    EXPECT_EQ(0U, tablet.ctime.segmentOffset());

    TestLog::Enable _(reassignTabletOwnershipFilter);

    EXPECT_THROW(client->reassignTabletOwnership(0, 0, -1, ServerId(472, 2)),
        ServerDoesntExistException);
    EXPECT_EQ("reassignTabletOwnership: Server id 8589935064 does not exist! "
        "Cannot reassign ownership of tablet 0, range "
        "[0, 18446744073709551615]!", TestLog::get());
    EXPECT_EQ(1, master->tablets.tablet_size());
    EXPECT_EQ(0, master2->master->tablets.tablet_size());

    TestLog::reset();
    EXPECT_THROW(client->reassignTabletOwnership(0, 0, 57, master2->serverId),
        TableDoesntExistException);
    EXPECT_EQ("reassignTabletOwnership: Could not reassign tablet 0, "
        "range [0, 57]: not found!", TestLog::get());
    EXPECT_EQ(1, master->tablets.tablet_size());
    EXPECT_EQ(0, master2->master->tablets.tablet_size());

    TestLog::reset();
    client->reassignTabletOwnership(0, 0, -1, master2->serverId);
    EXPECT_EQ("reassignTabletOwnership: Reassigning tablet 0, range "
        "[0, 18446744073709551615] from server id 1 to server id 2.",
        TestLog::get());
    // Calling master removes the entry itself after the RPC completes on coord.
    EXPECT_EQ(1, master->tablets.tablet_size());
    EXPECT_EQ(1, master2->master->tablets.tablet_size());
    tablet = service->tabletMap.getTablet(0lu, 0lu, ~(0lu));
    EXPECT_EQ(master2->serverId, tablet.serverId);
    EXPECT_EQ(0U, tablet.ctime.segmentId());
    EXPECT_EQ(72U, tablet.ctime.segmentOffset());
}

static bool
setWillFilter(string s) {
    return s == "setWill";
}

TEST_F(CoordinatorServiceTest, setWill) {
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

namespace {
bool statusFilter(string s) {
    return s != "checkStatus";
}
}

TEST_F(CoordinatorServiceTest, sendServerList_client) {
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
    service->serverList.crashed(id, update);
    TestLog::reset();
    client->sendServerList(id);
    EXPECT_EQ("sendServerList: Could not send list to crashed server 3",
              TestLog::get());
}

TEST_F(CoordinatorServiceTest, assignReplicationGroup) {
    vector<ServerId> serverIds;
    ServerConfig config = ServerConfig::forTesting();
    config.services = {BACKUP_SERVICE, MEMBERSHIP_SERVICE, PING_SERVICE};
    for (uint32_t i = 0; i < 3; i++) {
        config.localLocator = format("mock:host=backup%u", i);
        serverIds.push_back(cluster.addServer(config)->serverId);
    }
    // Check normal functionality.
    EXPECT_TRUE(service->assignReplicationGroup(10U, serverIds));
    EXPECT_EQ(10U, service->serverList[serverIds[0]].replicationId);
    EXPECT_EQ(10U, service->serverList[serverIds[1]].replicationId);
    EXPECT_EQ(10U, service->serverList[serverIds[2]].replicationId);
    // Generate a single TransportException. assignReplicationGroup should
    // retry and succeed.
    cluster.transport.errorMessage = "I am Bon Jovi's pool cleaner!";
    EXPECT_TRUE(service->assignReplicationGroup(100U, serverIds));
    EXPECT_EQ(100U, service->serverList[serverIds[0]].replicationId);
    service->forceServerDownForTesting = true;
    // Crash one of the backups. assignReplicationGroup should fail.
    service->hintServerDown(serverIds[2]);
    EXPECT_FALSE(service->assignReplicationGroup(1000U, serverIds));
    service->forceServerDownForTesting = false;
}

TEST_F(CoordinatorServiceTest, createReplicationGroup) {
    ServerId serverIds[9];
    ServerConfig config = ServerConfig::forTesting();
    config.services = {BACKUP_SERVICE, MEMBERSHIP_SERVICE, PING_SERVICE};
    for (uint32_t i = 0; i < 8; i++) {
        config.localLocator = format("mock:host=backup%u", i);
        serverIds[i] = cluster.addServer(config)->serverId;
    }
    EXPECT_EQ(1U, service->serverList[serverIds[0]].replicationId);
    EXPECT_EQ(1U, service->serverList[serverIds[1]].replicationId);
    EXPECT_EQ(1U, service->serverList[serverIds[2]].replicationId);
    EXPECT_EQ(2U, service->serverList[serverIds[3]].replicationId);
    EXPECT_EQ(2U, service->serverList[serverIds[4]].replicationId);
    EXPECT_EQ(2U, service->serverList[serverIds[5]].replicationId);
    EXPECT_EQ(0U, service->serverList[serverIds[6]].replicationId);
    EXPECT_EQ(0U, service->serverList[serverIds[7]].replicationId);
    // Kill server 7.
    service->forceServerDownForTesting = true;
    service->hintServerDown(serverIds[7]);
    service->forceServerDownForTesting = false;
    // Create a new server.
    config.localLocator = format("mock:host=backup%u", 9);
    serverIds[8] = cluster.addServer(config)->serverId;
    EXPECT_EQ(0U, service->serverList[serverIds[6]].replicationId);
    EXPECT_EQ(0U, service->serverList[serverIds[8]].replicationId);
}

TEST_F(CoordinatorServiceTest, removeReplicationGroup) {
    ServerId serverIds[3];
    ServerConfig config = ServerConfig::forTesting();
    config.services = {BACKUP_SERVICE, MEMBERSHIP_SERVICE, PING_SERVICE};
    for (uint32_t i = 0; i < 3; i++) {
        config.localLocator = format("mock:host=backup%u", i);
        serverIds[i] = cluster.addServer(config)->serverId;
    }
    service->removeReplicationGroup(1);
    EXPECT_EQ(0U, service->serverList[serverIds[0]].replicationId);
    EXPECT_EQ(0U, service->serverList[serverIds[1]].replicationId);
    EXPECT_EQ(0U, service->serverList[serverIds[2]].replicationId);
}

TEST_F(CoordinatorServiceTest, sendServerList_service) {
    ServerConfig config = ServerConfig::forTesting();
    config.services = {MEMBERSHIP_SERVICE};
    ServerId id = cluster.addServer(config)->serverId;

    TestLog::Enable _;
    service->sendServerList(id);
    EXPECT_NE(string::npos, TestLog::get().find(
        "applyFullList: Got complete list of servers containing 1 "
        "entries (version number 2)"));
}

TEST_F(CoordinatorServiceTest, setRuntimeOption) {
    client->setRuntimeOption("failRecoveryMasters", "1 2 3");
    ASSERT_EQ(3u, service->runtimeOptions.failRecoveryMasters.size());
    EXPECT_EQ(1u, service->runtimeOptions.popFailRecoveryMasters());
    EXPECT_EQ(2u, service->runtimeOptions.popFailRecoveryMasters());
    EXPECT_EQ(3u, service->runtimeOptions.popFailRecoveryMasters());
    EXPECT_EQ(0u, service->runtimeOptions.popFailRecoveryMasters());
    EXPECT_THROW(client->setRuntimeOption("BAD", "1 2 3"),
                 ObjectDoesntExistException);
}

TEST_F(CoordinatorServiceTest, setMinOpenSegmentId) {
    EXPECT_THROW(client->setMinOpenSegmentId(ServerId(2, 2), 100),
                 ClientException);

    client->setMinOpenSegmentId(masterServerId, 10);
    EXPECT_EQ(10u, service->serverList[masterServerId].minOpenSegmentId);
    client->setMinOpenSegmentId(masterServerId, 9);
    EXPECT_EQ(10u, service->serverList[masterServerId].minOpenSegmentId);
    client->setMinOpenSegmentId(masterServerId, 11);
    EXPECT_EQ(11u, service->serverList[masterServerId].minOpenSegmentId);
}

TEST_F(CoordinatorServiceTest, verifyServerFailure) {
    EXPECT_FALSE(service->verifyServerFailure(masterServerId));
    cluster.transport.errorMessage = "Server gone!";
    EXPECT_TRUE(service->verifyServerFailure(masterServerId));
}

}  // namespace RAMCloud
