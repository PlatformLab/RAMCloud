/* Copyright (c) 2010 Stanford University
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
#include "CoordinatorServer.h"
#include "MasterServer.h"
#include "MockTransport.h"
#include "TransportManager.h"
#include "BindTransport.h"
#include "Recovery.h"

namespace RAMCloud {

class CoordinatorTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(CoordinatorTest);
    CPPUNIT_TEST(test_createTable);
    CPPUNIT_TEST(test_enlistServer);
    CPPUNIT_TEST(test_getMasterList);
    CPPUNIT_TEST(test_getBackupList);
    CPPUNIT_TEST(test_getServerList);
    CPPUNIT_TEST(test_getTabletMap);
    CPPUNIT_TEST(test_hintServerDown_master);
    CPPUNIT_TEST(test_hintServerDown_backup);
    CPPUNIT_TEST(test_tabletsRecovered_basics);
    CPPUNIT_TEST(test_setWill);
    CPPUNIT_TEST_SUITE_END();

    BindTransport* transport;
    CoordinatorClient* client;
    CoordinatorServer* server;
    ServerConfig masterConfig;
    MasterServer* master;

  public:
    CoordinatorTest()
        : transport(NULL)
        , client(NULL)
        , server(NULL)
        , masterConfig()
        , master(NULL)
    {
        masterConfig.coordinatorLocator = "mock:host=coordinator";
        masterConfig.localLocator = "mock:host=master";
        MasterServer::sizeLogAndHashTable("64", "8", &masterConfig);
    }

    void setUp() {
        transport = new BindTransport();
        transportManager.registerMock(transport);
        server = new CoordinatorServer();
        server->nextServerId = 2;
        transport->addService(*server, "mock:host=coordinator");
        client = new CoordinatorClient("mock:host=coordinator");
        // need to add the master as a transport destinaton before it is
        // created because under BindTransport it must service an rpc
        // just after its constructor is completes
        master = static_cast<MasterServer*>(malloc(sizeof(MasterServer)));
        transport->addService(*master, "mock:host=master");
        master = new(master) MasterServer(masterConfig, client, 0);
        master->serverId.construct(
            client->enlistServer(MASTER, masterConfig.localLocator));
        TestLog::enable();
    }

    void tearDown() {
        TestLog::disable();
        master->~MasterServer();
        free(master);
        delete client;
        delete server;
        transportManager.unregisterMock();
        delete transport;
    }

    void test_createTable() {
        MasterServer master2(masterConfig, NULL, 0);
        transport->addService(master2, "mock:host=master2");
        client->enlistServer(MASTER, "mock:host=master2");
        // master is already enlisted
        client->createTable("foo");
        client->createTable("foo"); // should be no-op
        client->createTable("bar"); // should go to master2
        client->createTable("baz"); // and back to master1
        CPPUNIT_ASSERT_EQUAL(0, get(server->tables, "foo"));
        CPPUNIT_ASSERT_EQUAL(1, get(server->tables, "bar"));
        CPPUNIT_ASSERT_EQUAL("tablet { table_id: 0 start_object_id: 0 "
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
                             server->tabletMap.ShortDebugString());
        ProtoBuf::Tablets& will1(*reinterpret_cast<ProtoBuf::Tablets*>(
                                    server->masterList.server(0).user_data()));
        CPPUNIT_ASSERT_EQUAL("tablet { table_id: 0 start_object_id: 0 "
                             "end_object_id: 18446744073709551615 "
                             "state: NORMAL user_data: 0 } "
                             "tablet { table_id: 2 start_object_id: 0 "
                             "end_object_id: 18446744073709551615 "
                             "state: NORMAL user_data: 1 }",
                             will1.ShortDebugString());
        ProtoBuf::Tablets& will2(*reinterpret_cast<ProtoBuf::Tablets*>(
                                    server->masterList.server(1).user_data()));
        CPPUNIT_ASSERT_EQUAL("tablet { table_id: 1 start_object_id: 0 "
                             "end_object_id: 18446744073709551615 "
                             "state: NORMAL user_data: 0 }",
                             will2.ShortDebugString());
        CPPUNIT_ASSERT_EQUAL(2, master->tablets.tablet_size());
        CPPUNIT_ASSERT_EQUAL(1, master2.tablets.tablet_size());
    }

    // TODO(ongaro): Find a way to test createTable with no masters online.

    // TODO(ongaro): test drop, open table

    void test_enlistServer() {
        CPPUNIT_ASSERT_EQUAL(2, *master->serverId);
        CPPUNIT_ASSERT_EQUAL(3,
                             client->enlistServer(BACKUP, "mock:host=backup"));
        assertMatchesPosixRegex("server { server_type: MASTER server_id: 2 "
                                "service_locator: \"mock:host=master\" "
                                "user_data: [0-9]\\+ }",
                                server->masterList.ShortDebugString());
        ProtoBuf::Tablets& will(*reinterpret_cast<ProtoBuf::Tablets*>(
                                    server->masterList.server(0).user_data()));
        CPPUNIT_ASSERT_EQUAL(0, will.tablet_size());
        CPPUNIT_ASSERT_EQUAL("server { server_type: BACKUP server_id: 3 "
                             "service_locator: \"mock:host=backup\" "
                             "user_data: 0 }",
                             server->backupList.ShortDebugString());
    }

    void test_getMasterList() {
        // master is already enlisted
        ProtoBuf::ServerList masterList;
        client->getMasterList(masterList);
        // need to avoid non-deterministic 'user_data' field.
        CPPUNIT_ASSERT_EQUAL(0, masterList.ShortDebugString().find(
                                    "server { server_type: MASTER server_id: 2 "
                                    "service_locator: \"mock:host=master\" "
                                    "user_data: "));
    }

    void test_getBackupList() {
        // master is already enlisted
        client->enlistServer(BACKUP, "mock:host=backup1");
        client->enlistServer(BACKUP, "mock:host=backup2");
        ProtoBuf::ServerList backupList;
        client->getBackupList(backupList);
        CPPUNIT_ASSERT_EQUAL(
             "server { server_type: BACKUP server_id: 3 "
             "service_locator: \"mock:host=backup1\" user_data: 0 } "
             "server { server_type: BACKUP server_id: 4 "
             "service_locator: \"mock:host=backup2\" user_data: 0 }",
                             backupList.ShortDebugString());
    }

    void test_getServerList() {
        // master is already enlisted
        client->enlistServer(BACKUP, "mock:host=backup1");
        ProtoBuf::ServerList serverList;
        client->getServerList(serverList);
        CPPUNIT_ASSERT_EQUAL(2, serverList.server_size());
        CPPUNIT_ASSERT_EQUAL(MASTER, serverList.server(0).server_type());
        CPPUNIT_ASSERT_EQUAL(BACKUP, serverList.server(1).server_type());
    }

    void test_getTabletMap() {
        client->createTable("foo");
        ProtoBuf::Tablets tabletMap;
        client->getTabletMap(tabletMap);
        CPPUNIT_ASSERT_EQUAL("tablet { table_id: 0 start_object_id: 0 "
                             "end_object_id: 18446744073709551615 "
                             "state: NORMAL server_id: 2 "
                             "service_locator: \"mock:host=master\" }",
                             tabletMap.ShortDebugString());
    }

    void test_hintServerDown_master() {
        struct MyMockRecovery : public BaseRecovery {
            explicit MyMockRecovery(CoordinatorTest& test)
                : test(test), called(false) {}
            void
            operator()(uint64_t masterId,
                       const ProtoBuf::Tablets& will,
                       const ProtoBuf::ServerList& masterHosts,
                       const ProtoBuf::ServerList& backupHosts) {

                CPPUNIT_ASSERT_EQUAL("tablet { table_id: 0 start_object_id: 0 "
                                     "end_object_id: 18446744073709551615 "
                                     "state: RECOVERING server_id: 2 "
                                     "service_locator: \"mock:host=master\" }",
                                     test.server->tabletMap.ShortDebugString());
                CPPUNIT_ASSERT_EQUAL(2, masterId);
                CPPUNIT_ASSERT_EQUAL("tablet { table_id: 0 start_object_id: 0 "
                                     "end_object_id: 18446744073709551615 "
                                     "state: NORMAL user_data: 0 }",
                                     will.ShortDebugString());
                assertMatchesPosixRegex("server { server_type: MASTER "
                                        "server_id: 3 service_locator: "
                                        "\"mock:host=master2\" "
                                        "user_data: [0-9]\\+ }",
                                        masterHosts.ShortDebugString());
                CPPUNIT_ASSERT_EQUAL("server { server_type: BACKUP "
                                     "server_id: 4 "
                                     "service_locator: \"mock:host=backup\" "
                                     "user_data: 0 }",
                                     backupHosts.ShortDebugString());
                called = true;
            }
            void start() {}
            CoordinatorTest& test;
            bool called;
        } mockRecovery(*this);
        server->mockRecovery = &mockRecovery;
        // master is already enlisted
        client->enlistServer(MASTER, "mock:host=master2");
        client->enlistServer(BACKUP, "mock:host=backup");
        client->createTable("foo");
        client->hintServerDown("mock:host=master");
        CPPUNIT_ASSERT(mockRecovery.called);

        foreach (const ProtoBuf::Tablets::Tablet& tablet,
                 server->tabletMap.tablet())
        {
            if (tablet.server_id() == *master->serverId) {
                CPPUNIT_ASSERT_EQUAL(&mockRecovery,
                    reinterpret_cast<Recovery*>(tablet.user_data()));
            }
        }
    }

    void test_hintServerDown_backup() {
        client->enlistServer(BACKUP, "mock:host=backup");
        client->hintServerDown("mock:host=backup");
        CPPUNIT_ASSERT_EQUAL("", server->backupList.ShortDebugString());
    }

    static bool
    tabletsRecoveredFilter(string s) {
        return s == "tabletsRecovered";
    }

    void test_tabletsRecovered_basics() {
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

        Tablet& stablet(*server->tabletMap.add_tablet());
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
            CPPUNIT_ASSERT_EQUAL(
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

        CPPUNIT_ASSERT_EQUAL(1, server->tabletMap.tablet_size());
        CPPUNIT_ASSERT_EQUAL(ProtoBuf::Tablets::Tablet::NORMAL,
                             server->tabletMap.tablet(0).state());
        CPPUNIT_ASSERT_EQUAL("mock:host=master2",
                             server->tabletMap.tablet(0).service_locator());
        CPPUNIT_ASSERT_EQUAL(master2Id,
                             server->tabletMap.tablet(0).server_id());
    }

    static bool
    setWillFilter(string s) {
        return s == "setWill";
    }

    void
    test_setWill()
    {
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

        CPPUNIT_ASSERT_EQUAL(
            "setWill: Master 2 updated its Will (now 1 entries, was 0)",
            TestLog::get());

        // bad master id should fail
        CPPUNIT_ASSERT_THROW(client->setWill(23481234, will),
            InternalError);
    }

    DISALLOW_COPY_AND_ASSIGN(CoordinatorTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(CoordinatorTest);

}  // namespace RAMCloud
