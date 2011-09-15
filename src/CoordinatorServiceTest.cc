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
#include "BackupManager.h"
#include "CoordinatorClient.h"
#include "CoordinatorService.h"
#include "MasterService.h"
#include "MockTransport.h"
#include "TransportManager.h"
#include "BindTransport.h"
#include "Recovery.h"

namespace RAMCloud {

class CoordinatorServiceTest : public ::testing::Test {
  public:
    BindTransport* transport;
    CoordinatorClient* client;
    CoordinatorService* service;
    ServerConfig masterConfig;
    MasterService* master;

    CoordinatorServiceTest()
        : transport(NULL)
        , client(NULL)
        , service(NULL)
        , masterConfig()
        , master(NULL)
    {
        masterConfig.coordinatorLocator = "mock:host=coordinator";
        masterConfig.localLocator = "mock:host=master";
        MasterService::sizeLogAndHashTable("16", "1", &masterConfig);
        transport = new BindTransport();
        Context::get().transportManager->registerMock(transport);
        service = new CoordinatorService();
        service->nextServerId = 2;
        transport->addService(*service, "mock:host=coordinator",
                    COORDINATOR_SERVICE);
        client = new CoordinatorClient("mock:host=coordinator");
        // need to add the master as a transport destinaton before it is
        // created because under BindTransport it must service an rpc
        // just after its constructor is completes
        master = static_cast<MasterService*>(malloc(sizeof(MasterService)));
        transport->addService(*master, "mock:host=master", MASTER_SERVICE);
        master = new(master) MasterService(masterConfig, client, 0);
        master->init();
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
    master2.init();
    transport->addService(master2, "mock:host=master2", MASTER_SERVICE);
    client->enlistServer(MASTER, "mock:host=master2");
    // master is already enlisted
    client->createTable("foo");
    client->createTable("foo"); // should be no-op
    client->createTable("bar"); // should go to master2
    client->createTable("baz"); // and back to master1
    EXPECT_EQ(0U, get(service->tables, "foo"));
    EXPECT_EQ(1U, get(service->tables, "bar"));
    EXPECT_EQ("tablet { table_id: 0 start_object_id: 0 "
              "end_object_id: 18446744073709551615 "
              "state: NORMAL server_id: 2 "
              "service_locator: \"mock:host=master\" } "
              "tablet { table_id: 1 start_object_id: 0 "
              "end_object_id: 18446744073709551615 "
              "state: NORMAL server_id: 3 "
              "service_locator: \"mock:host=master2\" } "
              "tablet { table_id: 2 start_object_id: 0 "
              "end_object_id: 18446744073709551615 "
              "state: NORMAL server_id: 2 "
              "service_locator: \"mock:host=master\" }",
              service->tabletMap.ShortDebugString());
    ProtoBuf::Tablets& will1(*reinterpret_cast<ProtoBuf::Tablets*>(
                                service->masterList.server(0).user_data()));
    EXPECT_EQ("tablet { table_id: 0 start_object_id: 0 "
              "end_object_id: 18446744073709551615 "
              "state: NORMAL user_data: 0 } "
              "tablet { table_id: 2 start_object_id: 0 "
              "end_object_id: 18446744073709551615 "
              "state: NORMAL user_data: 1 }",
              will1.ShortDebugString());
    ProtoBuf::Tablets& will2(*reinterpret_cast<ProtoBuf::Tablets*>(
                                service->masterList.server(1).user_data()));
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
    EXPECT_EQ(2U, *master->serverId);
    EXPECT_EQ(3LU, client->enlistServer(BACKUP, "mock:host=backup"));
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
                "server { server_type: MASTER server_id: 2 "
                "service_locator: \"mock:host=master\" "
                "user_data: [0-9]\\+ }",
                service->masterList.ShortDebugString()));
    ProtoBuf::Tablets& will(*reinterpret_cast<ProtoBuf::Tablets*>(
                                service->masterList.server(0).user_data()));
    EXPECT_EQ(0, will.tablet_size());
    EXPECT_EQ("server { server_type: BACKUP server_id: 3 "
              "service_locator: \"mock:host=backup\" "
              "user_data: 0 }",
              service->backupList.ShortDebugString());
}

TEST_F(CoordinatorServiceTest, getMasterList) {
    // master is already enlisted
    ProtoBuf::ServerList masterList;
    client->getMasterList(masterList);
    // need to avoid non-deterministic 'user_data' field.
    EXPECT_EQ(0U, masterList.ShortDebugString().find(
              "server { server_type: MASTER server_id: 2 "
              "service_locator: \"mock:host=master\" "
              "user_data: "));
}

TEST_F(CoordinatorServiceTest, getBackupList) {
    // master is already enlisted
    client->enlistServer(BACKUP, "mock:host=backup1");
    client->enlistServer(BACKUP, "mock:host=backup2");
    ProtoBuf::ServerList backupList;
    client->getBackupList(backupList);
    EXPECT_EQ("server { server_type: BACKUP server_id: 3 "
              "service_locator: \"mock:host=backup1\" user_data: 0 } "
              "server { server_type: BACKUP server_id: 4 "
              "service_locator: \"mock:host=backup2\" user_data: 0 }",
                            backupList.ShortDebugString());
}

TEST_F(CoordinatorServiceTest, getServerList) {
    // master is already enlisted
    client->enlistServer(BACKUP, "mock:host=backup1");
    ProtoBuf::ServerList serverList;
    client->getServerList(serverList);
    EXPECT_EQ(2, serverList.server_size());
    EXPECT_EQ(MASTER, downCast<int>(serverList.server(0).server_type()));
    EXPECT_EQ(BACKUP, downCast<int>(serverList.server(1).server_type()));
}

TEST_F(CoordinatorServiceTest, getTabletMap) {
    client->createTable("foo");
    ProtoBuf::Tablets tabletMap;
    client->getTabletMap(tabletMap);
    EXPECT_EQ("tablet { table_id: 0 start_object_id: 0 "
              "end_object_id: 18446744073709551615 "
              "state: NORMAL server_id: 2 "
              "service_locator: \"mock:host=master\" }",
              tabletMap.ShortDebugString());
}

TEST_F(CoordinatorServiceTest, hintServerDown_master) {
    struct MyMockRecovery : public BaseRecovery {
        explicit MyMockRecovery(CoordinatorServiceTest& test)
            : test(test), called(false) {}
        void
        operator()(uint64_t masterId,
                    const ProtoBuf::Tablets& will,
                    const ProtoBuf::ServerList& masterHosts,
                    const ProtoBuf::ServerList& backupHosts) {

            EXPECT_EQ("tablet { table_id: 0 start_object_id: 0 "
                    "end_object_id: 18446744073709551615 "
                    "state: RECOVERING server_id: 2 "
                    "service_locator: \"mock:host=master\" }",
                    test.service->tabletMap.ShortDebugString());
            EXPECT_EQ(2LU, masterId);
            EXPECT_EQ("tablet { table_id: 0 start_object_id: 0 "
                      "end_object_id: 18446744073709551615 "
                      "state: NORMAL user_data: 0 }",
                      will.ShortDebugString());
            EXPECT_TRUE(TestUtil::matchesPosixRegex(
                        "server { server_type: MASTER "
                        "server_id: 3 service_locator: "
                        "\"mock:host=master2\" "
                        "user_data: [0-9]\\+ }",
                        masterHosts.ShortDebugString()));
            EXPECT_EQ("server { server_type: BACKUP "
                      "server_id: 4 "
                      "service_locator: \"mock:host=backup\" "
                      "user_data: 0 }",
                      backupHosts.ShortDebugString());
            called = true;
        }
        void start() {}
        CoordinatorServiceTest& test;
        bool called;
    } mockRecovery(*this);
    service->mockRecovery = &mockRecovery;
    // master is already enlisted
    client->enlistServer(MASTER, "mock:host=master2");
    client->enlistServer(BACKUP, "mock:host=backup");
    client->createTable("foo");
    client->hintServerDown("mock:host=master");
    EXPECT_TRUE(mockRecovery.called);

    foreach (const ProtoBuf::Tablets::Tablet& tablet,
                service->tabletMap.tablet())
    {
        if (tablet.server_id() == *master->serverId) {
            EXPECT_TRUE(&mockRecovery ==
                reinterpret_cast<MyMockRecovery*>(tablet.user_data()));
        }
    }
}

TEST_F(CoordinatorServiceTest, hintServerDown_backup) {
    client->enlistServer(BACKUP, "mock:host=backup");
    client->hintServerDown("mock:host=backup");
    EXPECT_EQ("", service->backupList.ShortDebugString());
}

static bool
tabletsRecoveredFilter(string s) {
    return s == "tabletsRecovered";
}

TEST_F(CoordinatorServiceTest, tabletsRecovered_basics) {
    typedef ProtoBuf::Tablets::Tablet Tablet;
    typedef ProtoBuf::Tablets Tablets;

    uint64_t master2Id = client->enlistServer(MASTER, "mock:host=master2");
    client->enlistServer(BACKUP, "mock:host=backup");

    Tablets tablets;
    Tablet& tablet(*tablets.add_tablet());
    tablet.set_table_id(0);
    tablet.set_start_object_id(0);
    tablet.set_end_object_id(~(0ul));
    tablet.set_state(ProtoBuf::Tablets::Tablet::NORMAL);
    tablet.set_service_locator("mock:host=master2");
    tablet.set_server_id(master2Id);
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
        client->tabletsRecovered(2, tablets, will);
        EXPECT_EQ(
            "tabletsRecovered: called by masterId 2 with 1 tablets, "
            "1 will entries | "
            "tabletsRecovered: Recovery complete on tablet "
            "0,0,18446744073709551615 | "
            "tabletsRecovered: Recovery completed | "
            "tabletsRecovered: Coordinator tabletMap: | "
            "tabletsRecovered: table: 0 [0:18446744073709551615] "
            "state: 0 owner: 3",
                                TestLog::get());
    }

    EXPECT_EQ(1, service->tabletMap.tablet_size());
    EXPECT_EQ(ProtoBuf::Tablets::Tablet::NORMAL,
              service->tabletMap.tablet(0).state());
    EXPECT_EQ("mock:host=master2",
              service->tabletMap.tablet(0).service_locator());
    EXPECT_EQ(master2Id, service->tabletMap.tablet(0).server_id());
}

static bool
setWillFilter(string s) {
    return s == "setWill";
}

TEST_F(CoordinatorServiceTest, setWill) {
    client->enlistServer(MASTER, "mock:host=master2");

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

}  // namespace RAMCloud
