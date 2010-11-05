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
    CPPUNIT_TEST(test_getBackupList);
    CPPUNIT_TEST(test_getTabletMap);
    CPPUNIT_TEST(test_hintServerDown_master);
    CPPUNIT_TEST(test_hintServerDown_backup);
    CPPUNIT_TEST(test_tabletsRecovered_basics);
    CPPUNIT_TEST_SUITE_END();

    BindTransport* transport;
    BackupManager* backup;
    CoordinatorClient* client;
    CoordinatorServer* server;
    ServerConfig masterConfig;
    MasterServer* master;

  public:
    CoordinatorTest()
        : transport(NULL)
        , backup()
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
        transport->addServer(*server, "mock:host=coordinator");
        client = new CoordinatorClient("mock:host=coordinator");
        backup = new BackupManager(client, 0);
        // need to add the master as a transport destinaton before it is
        // created because under BindTransport it must service an rpc
        // just after its constructor is completes
        master = static_cast<MasterServer*>(malloc(sizeof(MasterServer)));
        transport->addServer(*master, "mock:host=master");
        master = new(master) MasterServer(masterConfig, *client, *backup);
        TestLog::enable();
    }

    void tearDown() {
        TestLog::disable();
        master->~MasterServer();
        free(master);
        delete backup;
        delete client;
        delete server;
        transportManager.unregisterMock();
        delete transport;
    }

    void test_createTable() {
        // master is already enlisted
        client->createTable("foo");
        client->createTable("foo"); // should be no-op
        client->createTable("bar");
        CPPUNIT_ASSERT_EQUAL(0, get(server->tables, "foo"));
        CPPUNIT_ASSERT_EQUAL(1, get(server->tables, "bar"));
        CPPUNIT_ASSERT_EQUAL("tablet { table_id: 0 start_object_id: 0 "
                             "end_object_id: 18446744073709551615 "
                             "state: NORMAL server_id: 2 "
                             "service_locator: \"mock:host=master\" } "
                             "tablet { table_id: 1 start_object_id: 0 "
                             "end_object_id: 18446744073709551615 "
                             "state: NORMAL server_id: 2 "
                             "service_locator: \"mock:host=master\" }",
                             server->tabletMap.ShortDebugString());
        ProtoBuf::Tablets& will(*reinterpret_cast<ProtoBuf::Tablets*>(
                                    server->masterList.server(0).user_data()));
        CPPUNIT_ASSERT_EQUAL("tablet { table_id: 0 start_object_id: 0 "
                             "end_object_id: 18446744073709551615 "
                             "state: NORMAL user_data: 0 } "
                             "tablet { table_id: 1 start_object_id: 0 "
                             "end_object_id: 18446744073709551615 "
                             "state: NORMAL user_data: 0 }",
                             will.ShortDebugString());
    }

    // TODO(ongaro): Find a way to test createTable with no masters online.

    // TODO(ongaro): test drop, open table

    void test_enlistServer() {
        CPPUNIT_ASSERT_EQUAL(2, master->serverId);
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
                             "service_locator: \"mock:host=backup\" }",
                             server->backupList.ShortDebugString());
    }

    void test_getBackupList() {
        // master is already enlisted
        client->enlistServer(BACKUP, "mock:host=backup1");
        client->enlistServer(BACKUP, "mock:host=backup2");
        ProtoBuf::ServerList serverList;
        client->getBackupList(serverList);
        CPPUNIT_ASSERT_EQUAL("server { server_type: BACKUP server_id: 3 "
                             "service_locator: \"mock:host=backup1\" } "
                             "server { server_type: BACKUP server_id: 4 "
                             "service_locator: \"mock:host=backup2\" }",
                             serverList.ShortDebugString());
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
        struct MyMockRecovery : MockRecovery {
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
                                     "service_locator: \"mock:host=backup\" }",
                                     backupHosts.ShortDebugString());
                called = true;
            }
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
        ProtoBuf::Tablets tablets;
        ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
        tablet.set_table_id(99);
        tablet.set_start_object_id(10);
        tablet.set_end_object_id(20);
        tablet.set_state(ProtoBuf::Tablets::Tablet::NORMAL);
        tablet.set_user_data(0);
        TestLog::Enable _(&tabletsRecoveredFilter);
        client->tabletsRecovered(tablets);
        CPPUNIT_ASSERT_EQUAL("tabletsRecovered: called with 1 tablets",
                             TestLog::get());
    }


    DISALLOW_COPY_AND_ASSIGN(CoordinatorTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(CoordinatorTest);

}  // namespace RAMCloud
