/* Copyright (c) 2012 Stanford University
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

#include <thread>
#include <mutex>
#include <queue>

#include "TestUtil.h"
#include "TableManager.h"

#include "MockCluster.h"

namespace RAMCloud {

class TableManagerTest : public ::testing::Test {
  public:
    Context context;
    MockCluster cluster;
    MasterService* master; // Unit tests need to call enlistMaster()
                           // before trying to use master.
    ServerConfig masterConfig;
    ServerId masterServerId; // Unit tests need to call enlistMaster()
                             // before trying to use masterServerId.
    CoordinatorServerList* serverList;
    TableManager* tableManager;

    std::mutex mutex;
    typedef std::unique_lock<std::mutex> Lock;

    TableManagerTest()
        : context()
        , cluster(&context)
        , master()
        , masterConfig(ServerConfig::forTesting())
        , masterServerId()
        , serverList()
        , tableManager()
        , mutex()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        CoordinatorService* service = cluster.coordinator.get();
        serverList = service->context->coordinatorServerList;
        tableManager = service->context->tableManager;

        masterConfig.services = {WireFormat::MASTER_SERVICE,
                                 WireFormat::PING_SERVICE,
                                 WireFormat::MEMBERSHIP_SERVICE};
        masterConfig.localLocator = "mock:host=master";
    }

    void fillMap(const Lock& lock, uint32_t entries) {
        for (uint32_t i = 0; i < entries; ++i) {
            const uint32_t b = i * 10;
            tableManager->addTablet(lock, {b + 1, b + 2, b + 3, {b + 4, b + 5},
                                    Tablet::RECOVERING, {b + 6l, b + 7}});
        }
    }

    // Enlist a master and store details in master and masterServerId.
    void enlistMaster() {
        Server* masterServer = cluster.addServer(masterConfig);
        master = masterServer->master.get();
        master->log->sync();
        masterServerId = masterServer->serverId;
    }

    DISALLOW_COPY_AND_ASSIGN(TableManagerTest);
};

/////////////////////////////////////////////////////////////////////////////
///////////////////// Unit tests for public methods /////////////////////////
/////////////////////////////////////////////////////////////////////////////

TEST_F(TableManagerTest, createTable) {
    // Enlist master
    enlistMaster();

    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    MasterService& master2 = *cluster.addServer(master2Config)->master;

    // Advance the log head slightly so creation time offset is non-zero.
    Buffer empty;
    master->log->append(LOG_ENTRY_TYPE_INVALID, 0, empty);
    master->log->sync();

    EXPECT_EQ(0U, tableManager->createTable("foo", 1));
    EXPECT_THROW(tableManager->createTable("foo", 1),
                 TableManager::TableExists);
    EXPECT_EQ(1U, tableManager->createTable("bar", 1)); // should go to master2
    EXPECT_EQ(2U, tableManager->createTable("baz", 1)); // and back to master1

    EXPECT_EQ(0U, get(tableManager->tables, "foo"));
    EXPECT_EQ(1U, get(tableManager->tables, "bar"));
    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 2, 62 } "
              "Tablet { tableId: 1 startKeyHash: 0 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 2.0 status: NORMAL "
              "ctime: 1, 54 } "
              "Tablet { tableId: 2 startKeyHash: 0 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 3, 70 }",
              tableManager->debugString());
    EXPECT_EQ(2, master->tablets.tablet_size());
    EXPECT_EQ(1, master2.tablets.tablet_size());
}

TEST_F(TableManagerTest, createTableSpannedAcrossTwoMastersWithThreeServers) {
    // Enlist master
    enlistMaster();

    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    MasterService& master2 = *cluster.addServer(master2Config)->master;

    ServerConfig master3Config = masterConfig;
    master3Config.localLocator = "mock:host=master3";
    MasterService& master3 = *cluster.addServer(master3Config)->master;

    tableManager->createTable("foo", 2);

    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 9223372036854775807 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 2, 62 } "
              "Tablet { tableId: 0 startKeyHash: 9223372036854775808 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 2.0 status: NORMAL "
              "ctime: 1, 54 }",
              tableManager->debugString());
    EXPECT_EQ(1, master->tablets.tablet_size());
    EXPECT_EQ(1, master2.tablets.tablet_size());
    EXPECT_EQ(0, master3.tablets.tablet_size());
}


TEST_F(TableManagerTest, createTableSpannedAcrossThreeMastersWithTwoServers) {
    // Enlist master
    enlistMaster();

    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    MasterService& master2 = *cluster.addServer(master2Config)->master;

    tableManager->createTable("foo", 3);

    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 6148914691236517205 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 2, 62 } "
              "Tablet { tableId: 0 startKeyHash: 6148914691236517206 "
              "endKeyHash: 12297829382473034410 "
              "serverId: 2.0 status: NORMAL "
              "ctime: 1, 54 } "
              "Tablet { tableId: 0 startKeyHash: 12297829382473034411 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 3, 70 }",
              tableManager->debugString());
    EXPECT_EQ(2, master->tablets.tablet_size());
    EXPECT_EQ(1, master2.tablets.tablet_size());
}

TEST_F(TableManagerTest, dropTable) {
    // Enlist master
    enlistMaster();

    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    MasterService& master2 = *cluster.addServer(master2Config)->master;

    // Add a table, so the tests won't just compare against an empty tabletMap
    tableManager->createTable("foo", 1);

    // Test dropping a table that is spread across one master
    tableManager->createTable("bar", 1);
    EXPECT_EQ(1, master2.tablets.tablet_size());
    tableManager->dropTable("bar");
    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 2, 62 }",
              tableManager->debugString());
    EXPECT_EQ(0, master2.tablets.tablet_size());

    // Test dropping a table that is spread across two masters
    tableManager->createTable("bar", 2);
    EXPECT_EQ(2, master->tablets.tablet_size());
    EXPECT_EQ(1, master2.tablets.tablet_size());
    tableManager->dropTable("bar");
    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 2, 62 }",
              tableManager->debugString());
    EXPECT_EQ(1, master->tablets.tablet_size());
    EXPECT_EQ(0, master2.tablets.tablet_size());
}

TEST_F(TableManagerTest, getTableId) {
    // Enlist master
    enlistMaster();
    // Get the id for an existing table
    tableManager->createTable("foo", 1);
    uint64_t tableId = tableManager->getTableId("foo");
    EXPECT_EQ(0lu, tableId);

    tableManager->createTable("foo2", 1);
    tableId = tableManager->getTableId("foo2");
    EXPECT_EQ(1lu, tableId);

    // Try to get the id for a non-existing table
    EXPECT_THROW(tableManager->getTableId("bar"),
                 TableManager::NoSuchTable);
}

static bool
reassignTabletOwnershipFilter(string s)
{
    return s == "reassignTabletOwnership";
}

TEST_F(TableManagerTest, reassignTabletOwnership) {
    Lock lock(mutex);     // Used to trick internal calls.
    // Enlist master
    enlistMaster();

    ServerConfig master2Config = masterConfig;
    master2Config.services = { WireFormat::MASTER_SERVICE,
                               WireFormat::PING_SERVICE,
                               WireFormat::MEMBERSHIP_SERVICE };
    master2Config.localLocator = "mock:host=master2";
    auto* master2 = cluster.addServer(master2Config);
    master2->master->log->sync();

    // Advance the log head slightly so creation time offset is non-zero
    // on host being migrated to.
    Buffer empty;
    master2->master->log->append(LOG_ENTRY_TYPE_INVALID, 0, empty);
    master2->master->log->sync();

    // master is already enlisted
    tableManager->createTable("foo", 1);
    EXPECT_EQ(1, master->tablets.tablet_size());
    EXPECT_EQ(0, master2->master->tablets.tablet_size());
    Tablet tablet = tableManager->getTablet(lock, 0lu, 0lu, ~(0lu));
    EXPECT_EQ(masterServerId, tablet.serverId);
    EXPECT_EQ(2U, tablet.ctime.getSegmentId());
    EXPECT_EQ(62U, tablet.ctime.getSegmentOffset());

    TestLog::Enable _(reassignTabletOwnershipFilter);

    EXPECT_THROW(CoordinatorClient::reassignTabletOwnership(&context,
        0, 0, -1, ServerId(472, 2), 83, 835), ServerNotUpException);
    EXPECT_EQ("reassignTabletOwnership: Cannot reassign tablet "
        "[0x0,0xffffffffffffffff] in tableId 0 to 472.2: server not up",
        TestLog::get());
    EXPECT_EQ(1, master->tablets.tablet_size());
    EXPECT_EQ(0, master2->master->tablets.tablet_size());

    TestLog::reset();
    EXPECT_THROW(CoordinatorClient::reassignTabletOwnership(&context,
        0, 0, 57, master2->serverId, 83, 835), TableDoesntExistException);
    EXPECT_EQ("reassignTabletOwnership: Could not reassign tablet [0x0,0x39] "
              "in tableId 0: tablet not found", TestLog::get());
    EXPECT_EQ(1, master->tablets.tablet_size());
    EXPECT_EQ(0, master2->master->tablets.tablet_size());

    TestLog::reset();
    CoordinatorClient::reassignTabletOwnership(&context, 0, 0, -1,
        master2->serverId, 83, 835);
    EXPECT_EQ("reassignTabletOwnership: Reassigning tablet "
        "[0x0,0xffffffffffffffff] in tableId 0 from server 1.0 at "
        "mock:host=master to server 2.0 at mock:host=master2",
        TestLog::get());
    // Calling master removes the entry itself after the RPC completes on coord.
    EXPECT_EQ(1, master->tablets.tablet_size());
    EXPECT_EQ(1, master2->master->tablets.tablet_size());
    tablet = tableManager->getTablet(lock, 0lu, 0lu, ~(0lu));
    EXPECT_EQ(master2->serverId, tablet.serverId);
    EXPECT_EQ(83U, tablet.ctime.getSegmentId());
    EXPECT_EQ(835U, tablet.ctime.getSegmentOffset());
}

TEST_F(TableManagerTest, serialize) {
    Lock lock(mutex);     // Used to trick internal calls.
    ServerId id1 = serverList->generateUniqueId(lock);
    serverList->add(lock, id1, "mock:host=one",
                    {WireFormat::MASTER_SERVICE}, 1);
    ServerId id2 = serverList->generateUniqueId(lock);
    serverList->add(lock, id2, "mock:host=two",
                    {WireFormat::MASTER_SERVICE}, 2);

    tableManager->map.push_back(Tablet({0, 1, 6, id1, Tablet::NORMAL, {0, 5}}));
    tableManager->map.push_back(Tablet({1, 2, 7, id2, Tablet::NORMAL, {1, 6}}));

    ProtoBuf::Tablets tablets;
    tableManager->serialize(*serverList, tablets);
    EXPECT_EQ("tablet { table_id: 0 start_key_hash: 1 end_key_hash: 6 "
              "state: NORMAL server_id: 1 service_locator: \"mock:host=one\" "
              "ctime_log_head_id: 0 ctime_log_head_offset: 5 } "
              "tablet { table_id: 1 start_key_hash: 2 end_key_hash: 7 "
              "state: NORMAL server_id: 2 service_locator: \"mock:host=two\" "
              "ctime_log_head_id: 1 ctime_log_head_offset: 6 }",
              tablets.ShortDebugString());
}

TEST_F(TableManagerTest, setStatusForServer) {
    Lock lock(mutex);     // Used to trick internal calls.
    tableManager->addTablet(lock, {0, 1, 6, {0, 1}, Tablet::NORMAL, {0, 5}});
    tableManager->addTablet(lock, {1, 2, 7, {1, 1}, Tablet::NORMAL, {1, 6}});
    tableManager->addTablet(lock, {0, 3, 8, {0, 1}, Tablet::NORMAL, {2, 7}});

    EXPECT_EQ(0lu,
        tableManager->setStatusForServer({2, 1}, Tablet::RECOVERING).size());

    auto tablets = tableManager->setStatusForServer({0, 1}, Tablet::RECOVERING);
    EXPECT_EQ(2lu, tablets.size());
    foreach (const auto& tablet, tablets) {
        Tablet inMap = tableManager->getTablet(lock, tablet.tableId,
                                     tablet.startKeyHash,
                                     tablet.endKeyHash);
        EXPECT_EQ(ServerId(0, 1), tablet.serverId);
        EXPECT_EQ(ServerId(0, 1), inMap.serverId);
        EXPECT_EQ(Tablet::RECOVERING, tablet.status);
        EXPECT_EQ(Tablet::RECOVERING, inMap.status);
    }

    tablets = tableManager->setStatusForServer({1, 1}, Tablet::RECOVERING);
    ASSERT_EQ(1lu, tablets.size());
    auto tablet = tablets[0];
    Tablet inMap = tableManager->getTablet(lock, tablet.tableId,
                                 tablet.startKeyHash,
                                 tablet.endKeyHash);
    EXPECT_EQ(ServerId(1, 1), tablet.serverId);
    EXPECT_EQ(ServerId(1, 1), inMap.serverId);
    EXPECT_EQ(Tablet::RECOVERING, tablet.status);
    EXPECT_EQ(Tablet::RECOVERING, inMap.status);
}

TEST_F(TableManagerTest, splitTablet) {
    enlistMaster();

    tableManager->createTable("foo", 1);
    tableManager->splitTablet("foo", 0, ~0lu, ~0lu / 2);
    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 9223372036854775806 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 2, 62 } "
              "Tablet { tableId: 0 "
              "startKeyHash: 9223372036854775807 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 2, 62 }",
              tableManager->debugString());

    tableManager->splitTablet("foo", 0,
                              9223372036854775806, 4611686018427387903);
    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 4611686018427387902 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 2, 62 } "
              "Tablet { tableId: 0 "
              "startKeyHash: 9223372036854775807 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 2, 62 } "
              "Tablet { tableId: 0 "
              "startKeyHash: 4611686018427387903 "
              "endKeyHash: 9223372036854775806 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 2, 62 }",
              tableManager->debugString());

    EXPECT_THROW(tableManager->splitTablet("foo", 0, 16, 8),
                 TableManager::NoSuchTablet);

    EXPECT_THROW(tableManager->splitTablet("foo", 0, 0, ~0ul / 2),
                 TableManager::BadSplit);

    EXPECT_THROW(tableManager->splitTablet("bar", 0, ~0ul, ~0ul / 2),
                 TableManager::NoSuchTable);
}

/////////////////////////////////////////////////////////////////////////////
///////////////////// Unit tests for private methods ////////////////////////
/////////////////////////////////////////////////////////////////////////////

TEST_F(TableManagerTest, addTablet) {
    Lock lock(mutex);     // Used to trick internal calls.
    tableManager->addTablet(
        lock, {1, 2, 3, {4, 5}, Tablet::RECOVERING, {6, 7}});
    EXPECT_EQ(1lu, tableManager->size(lock));
    Tablet tablet = tableManager->getTablet(lock, 1, 2, 3);
    EXPECT_EQ(1lu, tablet.tableId);
    EXPECT_EQ(2lu, tablet.startKeyHash);
    EXPECT_EQ(3lu, tablet.endKeyHash);
    EXPECT_EQ(ServerId(4, 5), tablet.serverId);
    EXPECT_EQ(Tablet::RECOVERING, tablet.status);
    EXPECT_EQ(Log::Position(6, 7), tablet.ctime);
}

TEST_F(TableManagerTest, getTablet) {
    Lock lock(mutex);     // Used to trick internal calls.
    fillMap(lock, 3);
    for (uint32_t i = 0; i < 3; ++i) {
        const uint32_t b = i * 10;
        Tablet tablet = tableManager->getTablet(lock, b + 1, b + 2, b + 3);
        EXPECT_EQ(b + 1, tablet.tableId);
        EXPECT_EQ(b + 2, tablet.startKeyHash);
        EXPECT_EQ(b + 3, tablet.endKeyHash);
        EXPECT_EQ(ServerId(b + 4, b + 5), tablet.serverId);
        EXPECT_EQ(Tablet::RECOVERING, tablet.status);
        EXPECT_EQ(Log::Position(b + 6, b + 7), tablet.ctime);
    }
    EXPECT_THROW(tableManager->getTablet(lock, 0, 0, 0),
                 TableManager::NoSuchTablet);
}

TEST_F(TableManagerTest, getTabletsForTable) {
    Lock lock(mutex); // Used to trick internal calls.
    Tablet tablet1({0, 1, 6, {0, 1}, Tablet::NORMAL, {0, 5}});
    tableManager->addTablet(lock, tablet1);

    tableManager->addTablet(lock, {1, 2, 7, {1, 1}, Tablet::NORMAL, {1, 6}});
    tableManager->addTablet(lock, {0, 3, 8, {2, 1}, Tablet::NORMAL, {2, 7}});
    tableManager->addTablet(lock, {1, 4, 9, {3, 1}, Tablet::NORMAL, {3, 8}});
    tableManager->addTablet(lock, {2, 5, 10, {4, 1}, Tablet::NORMAL, {4, 9}});
    auto tablets = tableManager->getTabletsForTable(lock, 0);
    EXPECT_EQ(2lu, tablets.size());
    EXPECT_EQ(ServerId(0, 1), tablets[0].serverId);
    EXPECT_EQ(ServerId(2, 1), tablets[1].serverId);

    tablets = tableManager->getTabletsForTable(lock, 1);
    EXPECT_EQ(2lu, tablets.size());
    EXPECT_EQ(ServerId(1, 1), tablets[0].serverId);
    EXPECT_EQ(ServerId(3, 1), tablets[1].serverId);

    tablets = tableManager->getTabletsForTable(lock, 2);
    EXPECT_EQ(1lu, tablets.size());
    EXPECT_EQ(ServerId(4, 1), tablets[0].serverId);

    tablets = tableManager->getTabletsForTable(lock, 3);
    EXPECT_EQ(0lu, tablets.size());
}

TEST_F(TableManagerTest, modifyTablet) {
    Lock lock(mutex);     // Used to trick internal calls.
    tableManager->addTablet(lock, {0, 1, 6, {0, 1}, Tablet::NORMAL, {0, 5}});
    tableManager->modifyTablet(
        lock, 0, 1, 6, {1, 2}, Tablet::RECOVERING, {3, 9});
    Tablet tablet = tableManager->getTablet(lock, 0, 1, 6);
    EXPECT_EQ(ServerId(1, 2), tablet.serverId);
    EXPECT_EQ(Tablet::RECOVERING, tablet.status);
    EXPECT_EQ(Log::Position(3, 9), tablet.ctime);
    EXPECT_THROW(
        tableManager->modifyTablet(
                lock, 0, 0, 0, {0, 0}, Tablet::NORMAL, {0, 0}),
        TableManager::NoSuchTablet);
}

TEST_F(TableManagerTest, removeTabletsForTable) {
    Lock lock(mutex); // Used to trick internal calls.-
    tableManager->addTablet(lock, {0, 1, 6, {0, 1}, Tablet::NORMAL, {0, 5}});
    tableManager->addTablet(lock, {1, 2, 7, {1, 1}, Tablet::NORMAL, {1, 6}});
    tableManager->addTablet(lock, {0, 3, 8, {2, 1}, Tablet::NORMAL, {2, 7}});

    EXPECT_EQ(0lu, tableManager->removeTabletsForTable(lock, 2).size());
    EXPECT_EQ(3lu, tableManager->size(lock));

    auto tablets = tableManager->removeTabletsForTable(lock, 1);
    EXPECT_EQ(2lu, tableManager->size(lock));
    foreach (const auto& tablet, tablets) {
        EXPECT_THROW(tableManager->getTablet(lock, tablet.tableId,
                                   tablet.startKeyHash,
                                   tablet.endKeyHash),
                     TableManager::NoSuchTablet);
    }

    tablets = tableManager->removeTabletsForTable(lock, 0);
    EXPECT_EQ(0lu, tableManager->size(lock));
    foreach (const auto& tablet, tablets) {
        EXPECT_THROW(tableManager->getTablet(lock, tablet.tableId,
                                   tablet.startKeyHash,
                                   tablet.endKeyHash),
                     TableManager::NoSuchTablet);
    }
}

}  // namespace RAMCloud
