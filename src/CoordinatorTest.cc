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

namespace RAMCloud {

class CoordinatorTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(CoordinatorTest);
    CPPUNIT_TEST(test_enlistServer);
    CPPUNIT_TEST(test_getServerList);
    CPPUNIT_TEST(test_getTabletMap);
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
        master = new MasterServer(masterConfig, *client, *backup);
        transport->addServer(*master, "mock:host=master");
        TestLog::enable();
    }

    void tearDown() {
        TestLog::disable();
        delete master;
        delete backup;
        delete client;
        delete server;
        transportManager.unregisterMock();
        delete transport;
    }

    // TODO(ongaro): test create, drop, open table

    void test_enlistServer() {
        uint64_t serverId =
            client->enlistServer(MASTER, "mock:host=master");
        CPPUNIT_ASSERT_EQUAL(2, serverId);
        CPPUNIT_ASSERT_EQUAL("server { server_type: MASTER server_id: 2 "
                             "service_locator: \"mock:host=master\" }",
                             server->serverList.ShortDebugString());
        CPPUNIT_ASSERT_EQUAL("tablet { table_id: 0 start_object_id: 0 "
                             "end_object_id: 18446744073709551615 "
                             "state: NORMAL server_id: 2 "
                             "service_locator: \"mock:host=master\" }",
                             server->tabletMap.ShortDebugString());
        CPPUNIT_ASSERT_EQUAL(1, master->tablets.tablet().size());
    }

    void test_getServerList() {
        client->enlistServer(MASTER, "mock:host=master");
        client->enlistServer(BACKUP, "mock:host=backup");
        ProtoBuf::ServerList serverList;
        client->getServerList(serverList);
        CPPUNIT_ASSERT_EQUAL("server { server_type: MASTER server_id: 2 "
                             "service_locator: \"mock:host=master\" } "
                             "server { server_type: BACKUP server_id: 3 "
                             "service_locator: \"mock:host=backup\" }",
                             serverList.ShortDebugString());
    }

    void test_getTabletMap() {
        client->enlistServer(MASTER, "mock:host=master");
        ProtoBuf::Tablets tabletMap;
        client->getTabletMap(tabletMap);
        CPPUNIT_ASSERT_EQUAL("tablet { table_id: 0 start_object_id: 0 "
                             "end_object_id: 18446744073709551615 "
                             "state: NORMAL server_id: 2 "
                             "service_locator: \"mock:host=master\" }",
                             tabletMap.ShortDebugString());
    }

    DISALLOW_COPY_AND_ASSIGN(CoordinatorTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(CoordinatorTest);

}  // namespace RAMCloud
