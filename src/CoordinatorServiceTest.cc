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
#include "CoordinatorClient.h"
#include "CoordinatorService.h"
#include "MasterService.h"
#include "MembershipService.h"
#include "MockCluster.h"
#include "Recovery.h"
#include "ServerList.h"

namespace RAMCloud {

class CoordinatorServiceTest : public ::testing::Test {
  public:
    ServerConfig masterConfig;
    MockCluster cluster;
    CoordinatorClient* client;
    CoordinatorService* service;
    MasterService* master;
    ServerId masterServerId;

    CoordinatorServiceTest()
        : masterConfig(ServerConfig::forTesting())
        , cluster()
        , client()
        , service()
        , master()
        , masterServerId()
    {
        Context::get().logger->setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        service = cluster.coordinator.get();

        masterConfig.services = {MASTER_SERVICE, PING_SERVICE};
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
    // master is already enlisted
    client->createTable("foo");
    client->createTable("foo"); // should be no-op
    client->createTable("bar"); // should go to master2
    client->createTable("baz"); // and back to master1
    EXPECT_EQ(0U, get(service->tables, "foo"));
    EXPECT_EQ(1U, get(service->tables, "bar"));
    EXPECT_EQ("tablet { table_id: 0 start_key_hash: 0 "
              "end_key_hash: 18446744073709551615 "
              "state: NORMAL server_id: 1 "
              "service_locator: \"mock:host=master\" } "
              "tablet { table_id: 1 start_key_hash: 0 "
              "end_key_hash: 18446744073709551615 "
              "state: NORMAL server_id: 2 "
              "service_locator: \"mock:host=master2\" } "
              "tablet { table_id: 2 start_key_hash: 0 "
              "end_key_hash: 18446744073709551615 "
              "state: NORMAL server_id: 1 "
              "service_locator: \"mock:host=master\" }",
              service->tabletMap.ShortDebugString());
    ProtoBuf::Tablets& will1 = *service->serverList[1]->will;
    EXPECT_EQ("tablet { table_id: 0 start_key_hash: 0 "
              "end_key_hash: 18446744073709551615 "
              "state: NORMAL user_data: 0 } "
              "tablet { table_id: 2 start_key_hash: 0 "
              "end_key_hash: 18446744073709551615 "
              "state: NORMAL user_data: 1 }",
              will1.ShortDebugString());
    ProtoBuf::Tablets& will2 = *service->serverList[2]->will;
    EXPECT_EQ("tablet { table_id: 1 start_key_hash: 0 "
              "end_key_hash: 18446744073709551615 "
              "state: NORMAL user_data: 0 }",
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
    EXPECT_EQ("tablet { table_id: 0 start_key_hash: 0 "
              "end_key_hash: 9223372036854775807 "
              "state: NORMAL server_id: 1 "
              "service_locator: \"mock:host=master\" } "
              "tablet { table_id: 0 start_key_hash: 9223372036854775808 "
              "end_key_hash: 18446744073709551615 "
              "state: NORMAL server_id: 2 "
              "service_locator: \"mock:host=master2\" }",
              service->tabletMap.ShortDebugString());
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
    EXPECT_EQ("tablet { table_id: 0 start_key_hash: 0 "
              "end_key_hash: 6148914691236517205 "
              "state: NORMAL server_id: 1 "
              "service_locator: \"mock:host=master\" } "
              "tablet { table_id: 0 start_key_hash: 6148914691236517206 "
              "end_key_hash: 12297829382473034410 "
              "state: NORMAL server_id: 2 "
              "service_locator: \"mock:host=master2\" } "
              "tablet { table_id: 0 start_key_hash: 12297829382473034411 "
              "end_key_hash: 18446744073709551615 "
              "state: NORMAL server_id: 1 "
              "service_locator: \"mock:host=master\" }",
              service->tabletMap.ShortDebugString());
    EXPECT_EQ(2, master->tablets.tablet_size());
    EXPECT_EQ(1, master2.tablets.tablet_size());
}

// TODO(ongaro): Find a way to test createTable with no masters online.

// TODO(ongaro): test drop, open table

TEST_F(CoordinatorServiceTest, enlistServer) {
    EXPECT_EQ(1U, master->serverId.getId());
    EXPECT_EQ(ServerId(2, 0),
        client->enlistServer({}, {BACKUP_SERVICE}, "mock:host=backup"));

    ProtoBuf::ServerList masterList;
    service->serverList.serialize(masterList, {MASTER_SERVICE});
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
                "server { service_mask: 9 server_id: 1 "
                "service_locator: \"mock:host=master\" "
                "backup_read_mbytes_per_sec: [0-9]\\+ status: 0 } "
                "version_number: 2",
                masterList.ShortDebugString()));

    ProtoBuf::Tablets& will = *service->serverList[1]->will;
    EXPECT_EQ(0, will.tablet_size());

    ProtoBuf::ServerList backupList;
    service->serverList.serialize(backupList, {BACKUP_SERVICE});
    EXPECT_EQ("server { service_mask: 2 server_id: 2 "
              "service_locator: \"mock:host=backup\" "
              "backup_read_mbytes_per_sec: 0 status: 0 } "
              "version_number: 2",
              backupList.ShortDebugString());
}

TEST_F(CoordinatorServiceTest, enlistServerReplaceAMaster) {
    struct MockRecovery : public BaseRecovery {
        explicit MockRecovery() : called(false) {}
        void operator()(ServerId masterId,
                        const ProtoBuf::Tablets& will,
                        const CoordinatorServerList& serverList)
        {
            called = true;
            this->masterId = masterId;
        }
        void start() {}
        bool called;
    } mockRecovery;
    service->mockRecovery = &mockRecovery;

    EXPECT_EQ(ServerId(2, 0),
        client->enlistServer(masterServerId,
                             {BACKUP_SERVICE}, "mock:host=backup"));

    EXPECT_TRUE(mockRecovery.called);
    EXPECT_EQ(ServerId(1, 0), mockRecovery.masterId);
    EXPECT_TRUE(service->serverList.contains(masterServerId));
    EXPECT_EQ(ServerStatus::CRASHED,
              service->serverList[masterServerId].status);
}

TEST_F(CoordinatorServiceTest, enlistServerReplaceANonMaster) {
    struct MockRecovery : public BaseRecovery {
        explicit MockRecovery() : called(false) {}
        void operator()(ServerId masterId,
                        const ProtoBuf::Tablets& will,
                        const CoordinatorServerList& serverList)
        {
            called = true;
            this->masterId = masterId;
        }
        void start() {}
        bool called;
    } mockRecovery;
    service->mockRecovery = &mockRecovery;

    ServerConfig config = ServerConfig::forTesting();
    config.localLocator = "mock:host=backup1";
    config.services = {BACKUP_SERVICE};
    ServerId replacesId = cluster.addServer(config)->serverId;

    EXPECT_EQ(ServerId(2, 1),
        client->enlistServer(replacesId,
                             {BACKUP_SERVICE}, "mock:host=backup2"));

    EXPECT_FALSE(mockRecovery.called);
    EXPECT_FALSE(service->serverList.contains(replacesId));
}

TEST_F(CoordinatorServiceTest, getMasterList) {
    // master is already enlisted
    ProtoBuf::ServerList masterList;
    client->getMasterList(masterList);
    // need to avoid non-deterministic mbytes_per_sec field.
    EXPECT_EQ(0U, masterList.ShortDebugString().find(
              "server { service_mask: 9 server_id: 1 "
              "service_locator: \"mock:host=master\" "
              "backup_read_mbytes_per_sec: "));
}

TEST_F(CoordinatorServiceTest, getBackupList) {
    // master is already enlisted
    client->enlistServer({}, {BACKUP_SERVICE}, "mock:host=backup1");
    client->enlistServer({}, {BACKUP_SERVICE}, "mock:host=backup2");
    ProtoBuf::ServerList backupList;
    client->getBackupList(backupList);
    EXPECT_EQ("server { service_mask: 2 server_id: 2 "
              "service_locator: \"mock:host=backup1\" "
              "backup_read_mbytes_per_sec: 0 "
              "status: 0 } "
              "server { service_mask: 2 server_id: 3 "
              "service_locator: \"mock:host=backup2\" "
              "backup_read_mbytes_per_sec: 0 "
              "status: 0 } version_number: 3",
                            backupList.ShortDebugString());
}

TEST_F(CoordinatorServiceTest, getServerList) {
    // master is already enlisted
    client->enlistServer({}, {BACKUP_SERVICE}, "mock:host=backup1");
    ProtoBuf::ServerList serverList;
    client->getServerList(serverList);
    EXPECT_EQ(2, serverList.server_size());
    auto masterMask = ServiceMask{MASTER_SERVICE, PING_SERVICE}.serialize();
    EXPECT_EQ(masterMask, serverList.server(0).service_mask());
    auto backupMask = ServiceMask{BACKUP_SERVICE}.serialize();
    EXPECT_EQ(backupMask, serverList.server(1).service_mask());
}

TEST_F(CoordinatorServiceTest, getTabletMap) {
    client->createTable("foo");
    ProtoBuf::Tablets tabletMap;
    client->getTabletMap(tabletMap);
    EXPECT_EQ("tablet { table_id: 0 start_key_hash: 0 "
              "end_key_hash: 18446744073709551615 "
              "state: NORMAL server_id: 1 "
              "service_locator: \"mock:host=master\" }",
              tabletMap.ShortDebugString());
}

TEST_F(CoordinatorServiceTest, hintServerDown_master) {
    struct MockRecovery : public BaseRecovery {
        explicit MockRecovery(CoordinatorServiceTest& test)
            : test(test), called(false) {}
        void
        operator()(ServerId masterId,
                   const ProtoBuf::Tablets& will,
                   const CoordinatorServerList& serverList)
        {
            this->masterId = masterId;

            ProtoBuf::ServerList masterHosts, backupHosts;
            serverList.serialize(masterHosts, {MASTER_SERVICE});
            serverList.serialize(backupHosts, {BACKUP_SERVICE});

            EXPECT_EQ("tablet { table_id: 0 start_key_hash: 0 "
                    "end_key_hash: 18446744073709551615 "
                    "state: RECOVERING server_id: 1 "
                    "service_locator: \"mock:host=master\" }",
                    test.service->tabletMap.ShortDebugString());
            EXPECT_EQ(1LU, masterId.getId());
            EXPECT_EQ("tablet { table_id: 0 start_key_hash: 0 "
                      "end_key_hash: 18446744073709551615 "
                      "state: NORMAL user_data: 0 }",
                      will.ShortDebugString());
            EXPECT_TRUE(TestUtil::matchesPosixRegex(
                        "server { service_mask: 9 "
                        "server_id: 2 service_locator: "
                        "\"mock:host=master2\" backup_read_mbytes_per_sec: "
                        "[0-9]\\+ "
                        "status: 0 } version_number: 4",
                        masterHosts.ShortDebugString()));
            EXPECT_EQ("server { service_mask: 2 "
                      "server_id: 3 "
                      "service_locator: \"mock:host=backup\" "
                      "backup_read_mbytes_per_sec: 0 status: 0 } "
                      "version_number: 4",
                      backupHosts.ShortDebugString());
            called = true;
        }
        void start() {}
        CoordinatorServiceTest& test;
        bool called;
    } mockRecovery(*this);
    service->mockRecovery = &mockRecovery;
    // master is already enlisted
    client->enlistServer({}, {MASTER_SERVICE, PING_SERVICE},
                         "mock:host=master2");
    client->enlistServer({}, {BACKUP_SERVICE}, "mock:host=backup");
    client->createTable("foo");
    service->forceServerDownForTesting = true;
    client->hintServerDown(masterServerId);
    EXPECT_TRUE(mockRecovery.called);

    foreach (const ProtoBuf::Tablets::Tablet& tablet,
                service->tabletMap.tablet())
    {
        if (tablet.server_id() == master->serverId.getId()) {
            EXPECT_TRUE(&mockRecovery ==
                reinterpret_cast<MockRecovery*>(tablet.user_data()));
        }
    }

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
tabletsRecoveredFilter(string s) {
    return s == "tabletsRecovered";
}

TEST_F(CoordinatorServiceTest, tabletsRecovered_basics) {
    typedef ProtoBuf::Tablets::Tablet Tablet;
    typedef ProtoBuf::Tablets Tablets;

    ServerId master2Id = client->enlistServer({}, {MASTER_SERVICE},
        "mock:host=master2");
    client->enlistServer({}, {BACKUP_SERVICE}, "mock:host=backup");

    Tablets tablets;
    Tablet& tablet(*tablets.add_tablet());
    tablet.set_table_id(0);
    tablet.set_start_key_hash(0);
    tablet.set_end_key_hash(~(0ul));
    tablet.set_state(ProtoBuf::Tablets::Tablet::NORMAL);
    tablet.set_service_locator("mock:host=master2");
    tablet.set_server_id(*master2Id);
    tablet.set_user_data(0);

    Tablet& stablet(*service->tabletMap.add_tablet());
    stablet.set_table_id(0);
    stablet.set_start_key_hash(0);
    stablet.set_end_key_hash(~(0ul));
    stablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
    stablet.set_user_data(
        reinterpret_cast<uint64_t>(new BaseRecovery(master2Id)));

    EXPECT_EQ(3u, service->serverList.versionNumber);

    {
        TestLog::Enable _(&tabletsRecoveredFilter);
        client->tabletsRecovered(ServerId(1, 0), tablets);
        EXPECT_EQ(
            "tabletsRecovered: called by masterId 1 with 1 tablets | "
            "tabletsRecovered: Recovery complete on tablet "
            "0,0,18446744073709551615 | "
            "tabletsRecovered: Recovery completed for master 2 | "
            "tabletsRecovered: Coordinator tabletMap: | "
            "tabletsRecovered: table: 0 [0:18446744073709551615] "
            "state: 0 owner: 2",
                                TestLog::get());
    }

    EXPECT_EQ(1, service->tabletMap.tablet_size());
    EXPECT_EQ(ProtoBuf::Tablets::Tablet::NORMAL,
              service->tabletMap.tablet(0).state());
    EXPECT_EQ("mock:host=master2",
              service->tabletMap.tablet(0).service_locator());
    EXPECT_EQ(*master2Id, service->tabletMap.tablet(0).server_id());
    EXPECT_FALSE(service->serverList.contains(master2Id));
    EXPECT_EQ(4u, service->serverList.versionNumber);
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

TEST_F(CoordinatorServiceTest, requestServerList) {
    TestLog::Enable _(statusFilter);

    client->requestServerList(ServerId(52, 0));
    EXPECT_EQ(0U, TestLog::get().find(
        "requestServerList: Could not send list to unknown server 52"));

    TestLog::reset();
    client->requestServerList(masterServerId);
    EXPECT_EQ(0U, TestLog::get().find(
        "requestServerList: Could not send list to server without "
        "membership service: 1"));

    ServerConfig config = ServerConfig::forTesting();
    config.services = {MEMBERSHIP_SERVICE};
    ServerId id = cluster.addServer(config)->serverId;

    TestLog::reset();
    client->requestServerList(id);
    EXPECT_EQ(0U, TestLog::get().find(
        "requestServerList: Sending server list to server id"));
    EXPECT_NE(string::npos, TestLog::get().find(
        "applyFullList: Got complete list of servers"));

    ProtoBuf::ServerList update;
    service->serverList.crashed(id, update);
    TestLog::reset();
    client->requestServerList(id);
    EXPECT_EQ("requestServerList: Could not send list to crashed server 2",
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

TEST_F(CoordinatorServiceTest, sendServerList) {
    ServerConfig config = ServerConfig::forTesting();
    config.services = {MEMBERSHIP_SERVICE};
    ServerId id = cluster.addServer(config)->serverId;

    TestLog::Enable _;
    service->sendServerList(id);
    EXPECT_NE(string::npos, TestLog::get().find(
        "applyFullList: Got complete list of servers containing 1 "
        "entries (version number 2)"));
}

TEST_F(CoordinatorServiceTest, sendMembershipUpdate) {
    ServerConfig config = ServerConfig::forTesting();
    config.services = {MEMBERSHIP_SERVICE};
    ServerId id = cluster.addServer(config)->serverId;
    ProtoBuf::ServerList update;
    service->serverList.crashed(cluster.addServer(config)->serverId, update);

    update.Clear();
    update.set_version_number(4);
    TestLog::Enable _(statusFilter);
    service->sendMembershipUpdate(update, ServerId(/* invalid id */));
    EXPECT_NE(string::npos, TestLog::get().find(
        "updateServerList: Got server list update (version number 4)"));
    // Make sure updateServerList only got called once (the crashed server
    // was skipped).
    EXPECT_EQ(0u, TestLog::get().find("updateServerList"));
    EXPECT_EQ(0u, TestLog::get().rfind("updateServerList"));
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

// startMasterRecovery is covered by hintServerDown tests.

TEST_F(CoordinatorServiceTest, verifyServerFailure) {
    EXPECT_FALSE(service->verifyServerFailure(masterServerId));
    cluster.transport.errorMessage = "Server gone!";
    EXPECT_TRUE(service->verifyServerFailure(masterServerId));
}

}  // namespace RAMCloud
