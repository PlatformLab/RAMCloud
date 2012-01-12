/* Copyright (c) 2010-2011 Stanford University
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
#include "ReplicaManager.h"
#include "CoordinatorClient.h"
#include "CoordinatorService.h"
#include "MasterService.h"
#include "MembershipService.h"
#include "MockTransport.h"
#include "TransportManager.h"
#include "BindTransport.h"
#include "Recovery.h"
#include "ServerList.h"

namespace RAMCloud {

class CoordinatorServiceTest : public ::testing::Test {
  public:
    BindTransport* transport;
    CoordinatorClient* client;
    CoordinatorService* service;
    ServerConfig masterConfig;
    MasterService* master;
    ServerId masterServerId;

    CoordinatorServiceTest()
        : transport(NULL)
        , client(NULL)
        , service(NULL)
        , masterConfig()
        , master(NULL)
        , masterServerId()
    {
        masterConfig.coordinatorLocator = "mock:host=coordinator";
        masterConfig.localLocator = "mock:host=master";
        MasterService::sizeLogAndHashTable("32", "1", &masterConfig);
        transport = new BindTransport();
        Context::get().transportManager->registerMock(transport);
        service = new CoordinatorService();
        transport->addService(*service, "mock:host=coordinator",
                    COORDINATOR_SERVICE);
        client = new CoordinatorClient("mock:host=coordinator");
        // need to add the master as a transport destinaton before it is
        // created because under BindTransport it must service an rpc
        // just after its constructor is completes
        master = static_cast<MasterService*>(malloc(sizeof(MasterService)));
        transport->addService(*master, "mock:host=master", MASTER_SERVICE);
        master = new(master) MasterService(masterConfig, client, 0);
        masterServerId = client->enlistServer(
            {MASTER_SERVICE}, "mock:host=master", 0, 0);
        master->init(masterServerId);
        TestLog::enable();
    }

    ~CoordinatorServiceTest() {
        TestLog::disable();
        master->~MasterService();
        free(master);
        delete client;
        delete service;
        Context::get().transportManager->unregisterMock();
        delete transport;
    }

    DISALLOW_COPY_AND_ASSIGN(CoordinatorServiceTest);
};

TEST_F(CoordinatorServiceTest, createTable) {
    MasterService master2(masterConfig, NULL, 0);
    transport->addService(master2, "mock:host=master2", MASTER_SERVICE);
    master2.init(client->enlistServer(
        {MASTER_SERVICE}, "mock:host=master2", 0, 0));
    // master is already enlisted
    client->createTable("foo");
    client->createTable("foo"); // should be no-op
    client->createTable("bar"); // should go to master2
    client->createTable("baz"); // and back to master1
    EXPECT_EQ(0U, get(service->tables, "foo"));
    EXPECT_EQ(1U, get(service->tables, "bar"));
    EXPECT_EQ("tablet { table_id: 0 start_object_id: 0 "
              "end_object_id: 18446744073709551615 "
              "state: NORMAL server_id: 1 "
              "service_locator: \"mock:host=master\" } "
              "tablet { table_id: 1 start_object_id: 0 "
              "end_object_id: 18446744073709551615 "
              "state: NORMAL server_id: 2 "
              "service_locator: \"mock:host=master2\" } "
              "tablet { table_id: 2 start_object_id: 0 "
              "end_object_id: 18446744073709551615 "
              "state: NORMAL server_id: 1 "
              "service_locator: \"mock:host=master\" }",
              service->tabletMap.ShortDebugString());
    ProtoBuf::Tablets& will1 = *service->serverList[1]->will;
    EXPECT_EQ("tablet { table_id: 0 start_object_id: 0 "
              "end_object_id: 18446744073709551615 "
              "state: NORMAL user_data: 0 } "
              "tablet { table_id: 2 start_object_id: 0 "
              "end_object_id: 18446744073709551615 "
              "state: NORMAL user_data: 1 }",
              will1.ShortDebugString());
    ProtoBuf::Tablets& will2 = *service->serverList[2]->will;
    EXPECT_EQ("tablet { table_id: 1 start_object_id: 0 "
              "end_object_id: 18446744073709551615 "
              "state: NORMAL user_data: 0 }",
              will2.ShortDebugString());
    EXPECT_EQ(2, master->tablets.tablet_size());
    EXPECT_EQ(1, master2.tablets.tablet_size());
}

// TODO(ongaro): Find a way to test createTable with no masters online.

// TODO(ongaro): test drop, open table

TEST_F(CoordinatorServiceTest, enlistServer) {
    EXPECT_EQ(1U, master->serverId->getId());
    EXPECT_EQ(ServerId(2, 0),
        client->enlistServer({BACKUP_SERVICE}, "mock:host=backup"));

    ProtoBuf::ServerList masterList;
    service->serverList.serialise(masterList, {MASTER_SERVICE});
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
                "server { service_mask: 1 server_id: 1 "
                "service_locator: \"mock:host=master\" "
                "user_data: [0-9]\\+ is_in_cluster: true } version_number: 2",
                masterList.ShortDebugString()));

    ProtoBuf::Tablets& will = *service->serverList[1]->will;
    EXPECT_EQ(0, will.tablet_size());

    ProtoBuf::ServerList backupList;
    service->serverList.serialise(backupList, {BACKUP_SERVICE});
    EXPECT_EQ("server { service_mask: 2 server_id: 2 "
              "service_locator: \"mock:host=backup\" "
              "user_data: 0 is_in_cluster: true } version_number: 2",
              backupList.ShortDebugString());
}

TEST_F(CoordinatorServiceTest, getMasterList) {
    // master is already enlisted
    ProtoBuf::ServerList masterList;
    client->getMasterList(masterList);
    // need to avoid non-deterministic 'user_data' field.
    EXPECT_EQ(0U, masterList.ShortDebugString().find(
              "server { service_mask: 1 server_id: 1 "
              "service_locator: \"mock:host=master\" "
              "user_data: "));
}

TEST_F(CoordinatorServiceTest, getBackupList) {
    // master is already enlisted
    client->enlistServer({BACKUP_SERVICE}, "mock:host=backup1");
    client->enlistServer({BACKUP_SERVICE}, "mock:host=backup2");
    ProtoBuf::ServerList backupList;
    client->getBackupList(backupList);
    EXPECT_EQ("server { service_mask: 2 server_id: 2 "
              "service_locator: \"mock:host=backup1\" user_data: 0 "
              "is_in_cluster: true } "
              "server { service_mask: 2 server_id: 3 "
              "service_locator: \"mock:host=backup2\" user_data: 0 "
              "is_in_cluster: true } version_number: 3",
                            backupList.ShortDebugString());
}

TEST_F(CoordinatorServiceTest, getServerList) {
    // master is already enlisted
    client->enlistServer({BACKUP_SERVICE}, "mock:host=backup1");
    ProtoBuf::ServerList serverList;
    client->getServerList(serverList);
    EXPECT_EQ(2, serverList.server_size());
    auto masterMask = ServiceMask{MASTER_SERVICE}.serialize();
    EXPECT_EQ(masterMask, serverList.server(0).service_mask());
    auto backupMask = ServiceMask{BACKUP_SERVICE}.serialize();
    EXPECT_EQ(backupMask, serverList.server(1).service_mask());
}

TEST_F(CoordinatorServiceTest, getTabletMap) {
    client->createTable("foo");
    ProtoBuf::Tablets tabletMap;
    client->getTabletMap(tabletMap);
    EXPECT_EQ("tablet { table_id: 0 start_object_id: 0 "
              "end_object_id: 18446744073709551615 "
              "state: NORMAL server_id: 1 "
              "service_locator: \"mock:host=master\" }",
              tabletMap.ShortDebugString());
}

TEST_F(CoordinatorServiceTest, hintServerDown_master) {
    struct MyMockRecovery : public BaseRecovery {
        explicit MyMockRecovery(CoordinatorServiceTest& test)
            : test(test), called(false) {}
        void
        operator()(ServerId masterId,
                    const ProtoBuf::Tablets& will,
                    const CoordinatorServerList& serverList) {

            ProtoBuf::ServerList masterHosts, backupHosts;
            serverList.serialise(masterHosts, {MASTER_SERVICE});
            serverList.serialise(backupHosts, {BACKUP_SERVICE});

            EXPECT_EQ("tablet { table_id: 0 start_object_id: 0 "
                    "end_object_id: 18446744073709551615 "
                    "state: RECOVERING server_id: 1 "
                    "service_locator: \"mock:host=master\" }",
                    test.service->tabletMap.ShortDebugString());
            EXPECT_EQ(1LU, masterId.getId());
            EXPECT_EQ("tablet { table_id: 0 start_object_id: 0 "
                      "end_object_id: 18446744073709551615 "
                      "state: NORMAL user_data: 0 }",
                      will.ShortDebugString());
            EXPECT_TRUE(TestUtil::matchesPosixRegex(
                        "server { service_mask: 1 "
                        "server_id: 2 service_locator: "
                        "\"mock:host=master2\" user_data: [0-9]\\+ "
                        "is_in_cluster: true } version_number: 4",
                        masterHosts.ShortDebugString()));
            EXPECT_EQ("server { service_mask: 2 "
                      "server_id: 3 "
                      "service_locator: \"mock:host=backup\" "
                      "user_data: 0 is_in_cluster: true } version_number: 4",
                      backupHosts.ShortDebugString());
            called = true;
        }
        void start() {}
        CoordinatorServiceTest& test;
        bool called;
    } mockRecovery(*this);
    service->mockRecovery = &mockRecovery;
    // master is already enlisted
    client->enlistServer({MASTER_SERVICE}, "mock:host=master2");
    client->enlistServer({BACKUP_SERVICE}, "mock:host=backup");
    client->createTable("foo");
    service->test_forceServerReallyDown = true;
    client->hintServerDown(masterServerId);
    EXPECT_TRUE(mockRecovery.called);

    foreach (const ProtoBuf::Tablets::Tablet& tablet,
                service->tabletMap.tablet())
    {
        if (tablet.server_id() == master->serverId->getId()) {
            EXPECT_TRUE(&mockRecovery ==
                reinterpret_cast<MyMockRecovery*>(tablet.user_data()));
        }
    }
}

TEST_F(CoordinatorServiceTest, hintServerDown_backup) {
    ServerId id = client->enlistServer({BACKUP_SERVICE}, "mock:host=backup");
    EXPECT_EQ(1U, service->serverList.backupCount());
    service->test_forceServerReallyDown = true;
    client->hintServerDown(id);
    EXPECT_EQ(0U, service->serverList.backupCount());
}

static bool
tabletsRecoveredFilter(string s) {
    return s == "tabletsRecovered";
}

TEST_F(CoordinatorServiceTest, tabletsRecovered_basics) {
    typedef ProtoBuf::Tablets::Tablet Tablet;
    typedef ProtoBuf::Tablets Tablets;

    ServerId master2Id = client->enlistServer({MASTER_SERVICE},
        "mock:host=master2");
    client->enlistServer({BACKUP_SERVICE}, "mock:host=backup");

    Tablets tablets;
    Tablet& tablet(*tablets.add_tablet());
    tablet.set_table_id(0);
    tablet.set_start_object_id(0);
    tablet.set_end_object_id(~(0ul));
    tablet.set_state(ProtoBuf::Tablets::Tablet::NORMAL);
    tablet.set_service_locator("mock:host=master2");
    tablet.set_server_id(*master2Id);
    tablet.set_user_data(0);

    Tablet& stablet(*service->tabletMap.add_tablet());
    stablet.set_table_id(0);
    stablet.set_start_object_id(0);
    stablet.set_end_object_id(~(0ul));
    stablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
    stablet.set_user_data(
        reinterpret_cast<uint64_t>(new BaseRecovery()));

    Tablets will;
    Tablet& willEntry(*will.add_tablet());
    willEntry.set_table_id(0);
    willEntry.set_start_object_id(0);
    willEntry.set_end_object_id(~(0ul));
    willEntry.set_state(ProtoBuf::Tablets::Tablet::NORMAL);
    willEntry.set_user_data(0);

    {
        TestLog::Enable _(&tabletsRecoveredFilter);
        client->tabletsRecovered(1, tablets, will);
        EXPECT_EQ(
            "tabletsRecovered: called by masterId 1 with 1 tablets, "
            "1 will entries | "
            "tabletsRecovered: Recovery complete on tablet "
            "0,0,18446744073709551615 | "
            "tabletsRecovered: Recovery completed | "
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
}

static bool
setWillFilter(string s) {
    return s == "setWill";
}

TEST_F(CoordinatorServiceTest, setWill) {
    client->enlistServer({MASTER_SERVICE}, "mock:host=master2");

    ProtoBuf::Tablets will;
    ProtoBuf::Tablets::Tablet& t(*will.add_tablet());
    t.set_table_id(0);
    t.set_start_object_id(235);
    t.set_end_object_id(47234);
    t.set_state(ProtoBuf::Tablets::Tablet::NORMAL);
    t.set_user_data(19);

    TestLog::Enable _(&setWillFilter);
    client->setWill(2, will);

    EXPECT_EQ("setWill: Master 2 updated its Will (now 1 entries, was 0)",
              TestLog::get());

    // bad master id should fail
    EXPECT_THROW(client->setWill(23481234, will), InternalError);
}

TEST_F(CoordinatorServiceTest, requestServerList) {
    TestLog::Enable _;

    client->requestServerList(ServerId(52, 0));
    EXPECT_EQ(0U, TestLog::get().find(
        "requestServerList: Could not send list to unknown server 52"));

    TestLog::reset();
    client->requestServerList(masterServerId);
    EXPECT_EQ(0U, TestLog::get().find(
        "requestServerList: Could not send list to server without "
        "membership service: 1"));

    ServerId serverId;
    ServerList serverList;
    MembershipService member(serverId, serverList);
    transport->addService(member, "mock:host=member", MEMBERSHIP_SERVICE);
    ServerId id = client->enlistServer(
        {MEMBERSHIP_SERVICE}, "mock:host=member", 0, 0);
    TestLog::reset();
    client->requestServerList(id);
    EXPECT_EQ(0U, TestLog::get().find(
        "requestServerList: Sending server list to server id"));
    EXPECT_NE(string::npos, TestLog::get().find(
        "setServerList: Got complete list of servers"));
}

TEST_F(CoordinatorServiceTest, sendServerList) {
    ServerId serverId;
    ServerList serverList;
    MembershipService member(serverId, serverList);
    transport->addService(member, "mock:host=member", MEMBERSHIP_SERVICE);
    ServerId id = client->enlistServer(
        {MEMBERSHIP_SERVICE}, "mock:host=member", 0, 0);
    TestLog::Enable _();
    service->sendServerList(id);
    EXPECT_NE(string::npos, TestLog::get().find(
        "setServerList: Got complete list of servers containing 1 "
        "entries (version number 2)"));
}

TEST_F(CoordinatorServiceTest, sendMembershipUpdate) {
    ServerId serverId;
    ServerList serverList;
    MembershipService member(serverId, serverList);
    transport->addService(member, "mock:host=member", MEMBERSHIP_SERVICE);
    ServerId id = client->enlistServer(
        {MEMBERSHIP_SERVICE}, "mock:host=member", 0, 0);
    ProtoBuf::ServerList update;
    update.set_version_number(3);
    service->sendMembershipUpdate(update, ServerId(/* invalid id */));
    EXPECT_NE(string::npos, TestLog::get().find(
        "updateServerList: Got server list update (version number 3)"));
}

}  // namespace RAMCloud
