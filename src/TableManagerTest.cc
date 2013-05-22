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
#include "MockCluster.h"
#include "TableManager.h"

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
    CoordinatorService* service;
    TableManager* tableManager;

    LogCabinHelper* logCabinHelper;
    LogCabin::Client::Log* logCabinLog;

    std::mutex mutex;
    typedef std::unique_lock<std::mutex> Lock;

    TableManagerTest()
        : context()
        , cluster(&context)
        , master()
        , masterConfig(ServerConfig::forTesting())
        , masterServerId()
        , serverList()
        , service()
        , tableManager()
        , logCabinHelper()
        , logCabinLog()
        , mutex()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        service = cluster.coordinator.get();
        serverList = service->context->coordinatorServerList;
        tableManager = service->context->tableManager;

        logCabinHelper = service->context->logCabinHelper;
        logCabinLog = service->context->logCabinLog;

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
        master->objectManager.log.sync();
        masterServerId = masterServer->serverId;
    }

    /**
     * From the debug log messages, find the entry id specified immediately
     * next to the given search string.
     */
    EntryId
    findEntryId(string searchString) {
        auto position = TestLog::get().find(searchString);
        if (position == string::npos) {
            throw "Search string not found";
        } else {
            size_t startPoint = TestLog::get().find(searchString) +
                                searchString.length();
            size_t endPoint = TestLog::get().find("|", startPoint);
            string entryIdString = TestLog::get().substr(startPoint, endPoint);
            return strtoul(entryIdString.c_str(), NULL, 0);
        }
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
    master->objectManager.log.append(LOG_ENTRY_TYPE_INVALID, empty);
    master->objectManager.log.sync();

    EXPECT_EQ(1U, tableManager->createTable("foo", 1));
    EXPECT_THROW(tableManager->createTable("foo", 1),
                 TableManager::TableExists);
    EXPECT_EQ(2U, tableManager->createTable("bar", 1)); // should go to master2
    EXPECT_EQ(3U, tableManager->createTable("baz", 1)); // and back to master1

    EXPECT_EQ(1U, get(tableManager->tables, "foo"));
    EXPECT_EQ(2U, get(tableManager->tables, "bar"));
    EXPECT_EQ("Tablet { tableId: 1 startKeyHash: 0 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 0 } "
              "Tablet { tableId: 2 startKeyHash: 0 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 2.0 status: NORMAL "
              "ctime: 0, 0 } "
              "Tablet { tableId: 3 startKeyHash: 0 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 0 }",
              tableManager->debugString());
    EXPECT_EQ(2U, master->tabletManager.getCount());
    EXPECT_EQ(1U, master2.tabletManager.getCount());
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

    EXPECT_EQ("Tablet { tableId: 1 startKeyHash: 0 "
              "endKeyHash: 9223372036854775807 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 0 } "
              "Tablet { tableId: 1 startKeyHash: 9223372036854775808 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 2.0 status: NORMAL "
              "ctime: 0, 0 }",
              tableManager->debugString());
    EXPECT_EQ(1U, master->tabletManager.getCount());
    EXPECT_EQ(1U, master2.tabletManager.getCount());
    EXPECT_EQ(0U, master3.tabletManager.getCount());
}


TEST_F(TableManagerTest, createTableSpannedAcrossThreeMastersWithTwoServers) {
    // Enlist master
    enlistMaster();

    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    MasterService& master2 = *cluster.addServer(master2Config)->master;

    tableManager->createTable("foo", 3);

    EXPECT_EQ("Tablet { tableId: 1 startKeyHash: 0 "
              "endKeyHash: 6148914691236517205 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 0 } "
              "Tablet { tableId: 1 startKeyHash: 6148914691236517206 "
              "endKeyHash: 12297829382473034410 "
              "serverId: 2.0 status: NORMAL "
              "ctime: 0, 0 } "
              "Tablet { tableId: 1 startKeyHash: 12297829382473034411 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 0 }",
              tableManager->debugString());
    EXPECT_EQ(2U, master->tabletManager.getCount());
    EXPECT_EQ(1U, master2.tabletManager.getCount());
}

TEST_F(TableManagerTest, createTable_LogCabin) {
    // Enlist master
    enlistMaster();

    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    *cluster.addServer(master2Config)->master;

    TestLog::Enable _;
    tableManager->createTable("foo", 2);

    vector<Entry> entriesRead = logCabinLog->read(0);
    string searchString;

    ProtoBuf::TableInformation creatingTable;
    searchString = "execute: LogCabin: CreateTable entryId: ";
    ASSERT_NO_THROW(findEntryId(searchString));
    logCabinHelper->parseProtoBufFromEntry(
            entriesRead[findEntryId(searchString)], creatingTable);
    EXPECT_EQ("entry_type: \"CreateTable\"\n"
              "name: \"foo\"\ntable_id: 1\nserver_span: 2\n"
              "tablet_info {\n  "
              "start_key_hash: 0\n  end_key_hash: 9223372036854775807\n  "
              "master_id: 1\n  "
              "ctime_log_head_id: 0\n  ctime_log_head_offset: 0\n}\n"
              "tablet_info {\n  "
              "start_key_hash: 9223372036854775808\n  "
              "end_key_hash: 18446744073709551615\n  "
              "master_id: 2\n  "
              "ctime_log_head_id: 0\n  ctime_log_head_offset: 0\n}\n",
               creatingTable.DebugString());

    ProtoBuf::TableInformation stableTable;
    searchString = "complete: LogCabin: AliveTable entryId: ";
    ASSERT_NO_THROW(findEntryId(searchString));
    logCabinHelper->parseProtoBufFromEntry(
            entriesRead[findEntryId(searchString)], stableTable);
    EXPECT_EQ("entry_type: \"AliveTable\"\n"
              "name: \"foo\"\ntable_id: 1\nserver_span: 2\n"
              "tablet_info {\n  "
              "start_key_hash: 0\n  end_key_hash: 9223372036854775807\n  "
              "master_id: 1\n  "
              "ctime_log_head_id: 0\n  ctime_log_head_offset: 0\n}\n"
              "tablet_info {\n  "
              "start_key_hash: 9223372036854775808\n  "
              "end_key_hash: 18446744073709551615\n  "
              "master_id: 2\n  "
              "ctime_log_head_id: 0\n  ctime_log_head_offset: 0\n}\n",
               stableTable.DebugString());
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
    EXPECT_EQ(1U, master2.tabletManager.getCount());
    tableManager->dropTable("bar");
    EXPECT_EQ("Tablet { tableId: 1 startKeyHash: 0 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 0 }",
              tableManager->debugString());
    EXPECT_EQ(0U, master2.tabletManager.getCount());

    // Test dropping a table that is spread across two masters
    tableManager->createTable("bar", 2);
    EXPECT_EQ(2U, master->tabletManager.getCount());
    EXPECT_EQ(1U, master2.tabletManager.getCount());
    tableManager->dropTable("bar");
    EXPECT_EQ("Tablet { tableId: 1 startKeyHash: 0 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 0 }",
              tableManager->debugString());
    EXPECT_EQ(1U, master->tabletManager.getCount());
    EXPECT_EQ(0U, master2.tabletManager.getCount());
}

TEST_F(TableManagerTest, dropTable_LogCabin) {
    // Enlist master
    enlistMaster();

    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    MasterService& master2 = *cluster.addServer(master2Config)->master;

    // Add a table, so the tests won't just compare against an empty tabletMap
    tableManager->createTable("foo", 1);

    // Test dropping a table that is spread across one master
    tableManager->createTable("bar", 1);
    EXPECT_EQ(1U, master2.tabletManager.getCount());

    TestLog::Enable _;
    tableManager->dropTable("bar");

    string searchString = "execute: LogCabin: DropTable entryId: ";
    ASSERT_NO_THROW(findEntryId(searchString));
    EntryId entryId = findEntryId(searchString);
    vector<Entry> entriesRead = logCabinLog->read(entryId);

    ProtoBuf::TableDrop dropTable;
    logCabinHelper->parseProtoBufFromEntry(entriesRead[0], dropTable);
    EXPECT_EQ("entry_type: \"DropTable\"\n"
              "name: \"bar\"\n",
               dropTable.DebugString());
}

TEST_F(TableManagerTest, getTableId) {
    // Enlist master
    enlistMaster();
    // Get the id for an existing table
    tableManager->createTable("foo", 1);
    uint64_t tableId = tableManager->getTableId("foo");
    EXPECT_EQ(1lu, tableId);

    tableManager->createTable("foo2", 1);
    tableId = tableManager->getTableId("foo2");
    EXPECT_EQ(2lu, tableId);

    // Try to get the id for a non-existing table
    EXPECT_THROW(tableManager->getTableId("bar"),
                 TableManager::NoSuchTable);
}

TEST_F(TableManagerTest, markAllTabletsRecovering) {
    Lock lock(mutex);     // Used to trick internal calls.
    tableManager->addTablet(lock, {1, 1, 6, {0, 1}, Tablet::NORMAL, {0, 5}});
    tableManager->addTablet(lock, {2, 2, 7, {1, 1}, Tablet::NORMAL, {1, 6}});
    tableManager->addTablet(lock, {1, 3, 8, {0, 1}, Tablet::NORMAL, {2, 7}});

    EXPECT_EQ(0lu,
        tableManager->markAllTabletsRecovering({2, 1}).size());

    auto tablets = tableManager->markAllTabletsRecovering({0, 1});
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

    tablets = tableManager->markAllTabletsRecovering({1, 1});
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
    master2->master->objectManager.log.sync();

    // Advance the log head slightly so creation time offset is non-zero
    // on host being migrated to.
    Buffer empty;
    master2->master->objectManager.log.append(LOG_ENTRY_TYPE_INVALID, empty);
    master2->master->objectManager.log.sync();

    // master is already enlisted
    tableManager->createTable("foo", 1);
    EXPECT_EQ(1U, master->tabletManager.getCount());
    EXPECT_EQ(0U, master2->master->tabletManager.getCount());
    Tablet tablet = tableManager->getTablet(lock, 1lu, 0lu, ~(0lu));
    EXPECT_EQ(masterServerId, tablet.serverId);
    EXPECT_EQ(0U, tablet.ctime.getSegmentId());
    EXPECT_EQ(0U, tablet.ctime.getSegmentOffset());

    TestLog::Enable _(reassignTabletOwnershipFilter);

    EXPECT_THROW(CoordinatorClient::reassignTabletOwnership(&context,
        1, 0, -1, ServerId(472, 2), 83, 835), ServerNotUpException);
    EXPECT_EQ("reassignTabletOwnership: Cannot reassign tablet "
        "[0x0,0xffffffffffffffff] in tableId 1 to 472.2: server not up",
        TestLog::get());
    EXPECT_EQ(1U, master->tabletManager.getCount());
    EXPECT_EQ(0U, master2->master->tabletManager.getCount());

    TestLog::reset();
    EXPECT_THROW(CoordinatorClient::reassignTabletOwnership(&context,
        1, 0, 57, master2->serverId, 83, 835), TableDoesntExistException);
    EXPECT_EQ("reassignTabletOwnership: Could not reassign tablet [0x0,0x39] "
              "in tableId 1: tablet not found", TestLog::get());
    EXPECT_EQ(1U, master->tabletManager.getCount());
    EXPECT_EQ(0U, master2->master->tabletManager.getCount());

    TestLog::reset();
    CoordinatorClient::reassignTabletOwnership(&context, 1, 0, -1,
        master2->serverId, 83, 835);
    EXPECT_EQ("reassignTabletOwnership: Reassigning tablet "
        "[0x0,0xffffffffffffffff] in tableId 1 from server 1.0 at "
        "mock:host=master to server 2.0 at mock:host=master2",
        TestLog::get());
    // Calling master removes the entry itself after the RPC completes on coord.
    EXPECT_EQ(1U, master->tabletManager.getCount());
    EXPECT_EQ(1U, master2->master->tabletManager.getCount());
    tablet = tableManager->getTablet(lock, 1lu, 0lu, ~(0lu));
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

    tableManager->map.push_back(Tablet({1, 1, 6, id1, Tablet::NORMAL, {0, 5}}));
    tableManager->map.push_back(Tablet({2, 2, 7, id2, Tablet::NORMAL, {1, 6}}));

    ProtoBuf::Tablets tablets;
    tableManager->serialize(*serverList, tablets);
    EXPECT_EQ("tablet { table_id: 1 start_key_hash: 1 end_key_hash: 6 "
              "state: NORMAL server_id: 1 service_locator: \"mock:host=one\" "
              "ctime_log_head_id: 0 ctime_log_head_offset: 5 } "
              "tablet { table_id: 2 start_key_hash: 2 end_key_hash: 7 "
              "state: NORMAL server_id: 2 service_locator: \"mock:host=two\" "
              "ctime_log_head_id: 1 ctime_log_head_offset: 6 }",
              tablets.ShortDebugString());
}

TEST_F(TableManagerTest, splitTablet) {
    enlistMaster();

    tableManager->createTable("foo", 1);
    tableManager->splitTablet("foo", ~0lu / 2);
    EXPECT_EQ("Tablet { tableId: 1 startKeyHash: 0 "
              "endKeyHash: 9223372036854775806 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 0 } "
              "Tablet { tableId: 1 "
              "startKeyHash: 9223372036854775807 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 0 }",
              tableManager->debugString());

    tableManager->splitTablet("foo", 4611686018427387903);
    EXPECT_EQ("Tablet { tableId: 1 startKeyHash: 0 "
              "endKeyHash: 4611686018427387902 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 0 } "
              "Tablet { tableId: 1 "
              "startKeyHash: 9223372036854775807 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 0 } "
              "Tablet { tableId: 1 "
              "startKeyHash: 4611686018427387903 "
              "endKeyHash: 9223372036854775806 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 0 }",
              tableManager->debugString());

    EXPECT_THROW(tableManager->splitTablet("bar", ~0ul / 2),
                 TableManager::NoSuchTable);
}

TEST_F(TableManagerTest, splitTablet_LogCabin) {
    enlistMaster();

    tableManager->createTable("foo", 1);

    TestLog::Enable _;
    tableManager->splitTablet("foo", ~0lu / 2);

    vector<Entry> entriesRead = logCabinLog->read(0);
    string searchString;

    ProtoBuf::SplitTablet splitTablet;
    searchString = "execute: LogCabin: SplitTablet entryId: ";
    ASSERT_NO_THROW(findEntryId(searchString));
    logCabinHelper->parseProtoBufFromEntry(
            entriesRead[findEntryId(searchString)], splitTablet);
    EXPECT_EQ("entry_type: \"SplitTablet\"\n"
              "name: \"foo\"\n"
              "split_key_hash: 9223372036854775807\n",
               splitTablet.DebugString());

    ProtoBuf::TableInformation aliveTableNew;
    searchString = "complete: LogCabin: AliveTable entryId: ";
    ASSERT_NO_THROW(findEntryId(searchString));
    logCabinHelper->parseProtoBufFromEntry(
            entriesRead[findEntryId(searchString)], aliveTableNew);
    EXPECT_EQ("entry_type: \"AliveTable\"\n"
              "name: \"foo\"\ntable_id: 1\nserver_span: 2\n"
              "tablet_info {\n  start_key_hash: 0\n  "
              "end_key_hash: 9223372036854775806\n  master_id: 1\n  "
              "ctime_log_head_id: 0\n  ctime_log_head_offset: 0\n}\n"
              "tablet_info {\n  start_key_hash: 9223372036854775807\n  "
              "end_key_hash: 18446744073709551615\n  master_id: 1\n  "
              "ctime_log_head_id: 0\n  ctime_log_head_offset: 0\n}\n",
              aliveTableNew.DebugString());
}

TEST_F(TableManagerTest, tabletRecovered_LogCabin) {
    // Enlist master
    enlistMaster();

    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    MasterService& master2 = *cluster.addServer(master2Config)->master;

    tableManager->createTable("foo", 1);

    TestLog::Enable _;
    tableManager->tabletRecovered(1UL, 0UL, ~0UL, master2.serverId,
                                  Log::Position(0UL, 0U));

    vector<Entry> entriesRead = logCabinLog->read(0);
    string searchString;

    ProtoBuf::TabletRecovered tabletInfo;
    searchString = "execute: LogCabin: TabletRecovered entryId: ";
    ASSERT_NO_THROW(findEntryId(searchString));
    logCabinHelper->parseProtoBufFromEntry(
            entriesRead[findEntryId(searchString)], tabletInfo);
    EXPECT_EQ("entry_type: \"TabletRecovered\"\n"
              "table_id: 1\n"
              "start_key_hash: 0\nend_key_hash: 18446744073709551615\n"
              "server_id: 2\n"
              "ctime_log_head_id: 0\nctime_log_head_offset: 0\n",
               tabletInfo.DebugString());

    ProtoBuf::TableInformation aliveTableNew;
    searchString = "complete: LogCabin: AliveTable entryId: ";
    ASSERT_NO_THROW(findEntryId(searchString));
    logCabinHelper->parseProtoBufFromEntry(
            entriesRead[findEntryId(searchString)], aliveTableNew);
    EXPECT_EQ("entry_type: \"AliveTable\"\n"
              "name: \"foo\"\ntable_id: 1\nserver_span: 1\n"
              "tablet_info {\n  start_key_hash: 0\n  "
              "end_key_hash: 18446744073709551615\n  master_id: 2\n  "
              "ctime_log_head_id: 0\n  ctime_log_head_offset: 0\n}\n",
              aliveTableNew.DebugString());
}

/////////////////////////////////////////////////////////////////////////////
///////////////////// Unit tests for recovery methods ///////////////////////
/////////////////////////////////////////////////////////////////////////////

TEST_F(TableManagerTest, recoverAliveTable) {
    // Enlist master
    enlistMaster();

    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    MasterService& master2 = *cluster.addServer(master2Config)->master;

    ProtoBuf::TableInformation state;
    state.set_entry_type("AliveTable");
    state.set_name("foo");
    state.set_table_id(1);
    state.set_server_span(2);

    ProtoBuf::TableInformation::TabletInfo& tablet1(*state.add_tablet_info());
    tablet1.set_start_key_hash(0);
    tablet1.set_end_key_hash(9223372036854775807UL);
    tablet1.set_master_id(master->serverId.getId());
    tablet1.set_ctime_log_head_id(0);
    tablet1.set_ctime_log_head_offset(0);

    ProtoBuf::TableInformation::TabletInfo& tablet2(*state.add_tablet_info());
    tablet2.set_start_key_hash(9223372036854775808UL);
    tablet2.set_end_key_hash(~0lu);
    tablet2.set_master_id(master2.serverId.getId());
    tablet2.set_ctime_log_head_id(0);
    tablet2.set_ctime_log_head_offset(0);

    EntryId entryId = logCabinHelper->appendProtoBuf(
            *service->context->expectedEntryId, state);

    TestLog::Enable _;
    tableManager->recoverAliveTable(&state, entryId);

    EXPECT_EQ("Tablet { tableId: 1 startKeyHash: 0 "
              "endKeyHash: 9223372036854775807 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 0 } "
              "Tablet { tableId: 1 startKeyHash: 9223372036854775808 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 2.0 status: NORMAL "
              "ctime: 0, 0 }",
              tableManager->debugString());

    // The masters shouldn't actually own the tablets since the
    // recoverAliveTable() call adds the tablets only to the local tablet map
    // and doesn't assign them to the masters.
    EXPECT_EQ(0U, master->tabletManager.getCount());
    EXPECT_EQ(0U, master2.tabletManager.getCount());
}

TEST_F(TableManagerTest, recoverCreateTable) {
    // Enlist master
    enlistMaster();

    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    MasterService& master2 = *cluster.addServer(master2Config)->master;

    ProtoBuf::TableInformation state;
    state.set_entry_type("CreateTable");
    state.set_name("foo");
    state.set_table_id(0);
    state.set_server_span(2);

    ProtoBuf::TableInformation::TabletInfo& tablet1(*state.add_tablet_info());
    tablet1.set_start_key_hash(0);
    tablet1.set_end_key_hash(9223372036854775807UL);
    tablet1.set_master_id(master->serverId.getId());
    tablet1.set_ctime_log_head_id(0);
    tablet1.set_ctime_log_head_offset(0);

    ProtoBuf::TableInformation::TabletInfo& tablet2(*state.add_tablet_info());
    tablet2.set_start_key_hash(9223372036854775808UL);
    tablet2.set_end_key_hash(~0lu);
    tablet2.set_master_id(master2.serverId.getId());
    tablet2.set_ctime_log_head_id(0);
    tablet2.set_ctime_log_head_offset(0);

    EntryId entryId = logCabinHelper->appendProtoBuf(
            *service->context->expectedEntryId, state);

    TestLog::Enable _;
    tableManager->recoverCreateTable(&state, entryId);

    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 9223372036854775807 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 0 } "
              "Tablet { tableId: 0 startKeyHash: 9223372036854775808 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 2.0 status: NORMAL "
              "ctime: 0, 0 }",
              tableManager->debugString());
    EXPECT_EQ(1U, master->tabletManager.getCount());
    EXPECT_EQ(1U, master2.tabletManager.getCount());
}

TEST_F(TableManagerTest, recoverDropTable) {
    // Enlist master
    enlistMaster();

    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    MasterService& master2 = *cluster.addServer(master2Config)->master;

    // Add a table, so the tests won't just compare against an empty tabletMap
    tableManager->createTable("foo", 1);

    // Test dropping a table that is spread across one master
    tableManager->createTable("bar", 1);
    EXPECT_EQ(1U, master2.tabletManager.getCount());

    ProtoBuf::TableDrop state;
    state.set_entry_type("DropTable");
    state.set_name("bar");
    EntryId entryId = logCabinHelper->appendProtoBuf(
            *service->context->expectedEntryId, state);

    tableManager->recoverDropTable(&state, entryId);
    EXPECT_EQ("Tablet { tableId: 1 startKeyHash: 0 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 0 }",
              tableManager->debugString());
    EXPECT_EQ(0U, master2.tabletManager.getCount());
}

TEST_F(TableManagerTest, recoverSplitTablet) {
    enlistMaster();

    tableManager->createTable("foo", 1);

    ProtoBuf::SplitTablet state;
    state.set_entry_type("SplitTablet");
    state.set_name("foo");
    state.set_split_key_hash(~0lu / 2);
    EntryId entryId = logCabinHelper->appendProtoBuf(
            *service->context->expectedEntryId, state);

    tableManager->recoverSplitTablet(&state, entryId);

    EXPECT_EQ("Tablet { tableId: 1 startKeyHash: 0 "
              "endKeyHash: 9223372036854775806 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 0 } "
              "Tablet { tableId: 1 "
              "startKeyHash: 9223372036854775807 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 0 }",
              tableManager->debugString());
}

TEST_F(TableManagerTest, recoverTabletRecovered) {
    // Enlist master
    enlistMaster();

    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    MasterService& master2 = *cluster.addServer(master2Config)->master;

    tableManager->createTable("foo", 1);

    ProtoBuf::TabletRecovered state;
    state.set_entry_type("TabletRecovered");
    state.set_table_id(1);
    state.set_start_key_hash(0);
    state.set_end_key_hash(~0lu);
    state.set_server_id(master2.serverId.getId());
    state.set_ctime_log_head_id(0);
    state.set_ctime_log_head_offset(0);
    EntryId entryId = logCabinHelper->appendProtoBuf(
            *service->context->expectedEntryId, state);

    TestLog::Enable _;
    tableManager->recoverTabletRecovered(&state, entryId);

    EXPECT_EQ("Tablet { tableId: 1 "
              "startKeyHash: 0 endKeyHash: 18446744073709551615 "
              "serverId: 2.0 "
              "status: NORMAL ctime: 0, 0 }",
              tableManager->debugString());
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
    Tablet tablet1({1, 1, 6, {0, 1}, Tablet::NORMAL, {0, 5}});
    tableManager->addTablet(lock, tablet1);

    tableManager->addTablet(lock, {2, 2, 7, {1, 1}, Tablet::NORMAL, {1, 6}});
    tableManager->addTablet(lock, {1, 3, 8, {2, 1}, Tablet::NORMAL, {2, 7}});
    tableManager->addTablet(lock, {2, 4, 9, {3, 1}, Tablet::NORMAL, {3, 8}});
    tableManager->addTablet(lock, {3, 5, 10, {4, 1}, Tablet::NORMAL, {4, 9}});
    auto tablets = tableManager->getTabletsForTable(lock, 1);
    EXPECT_EQ(2lu, tablets.size());
    EXPECT_EQ(ServerId(0, 1), tablets[0].serverId);
    EXPECT_EQ(ServerId(2, 1), tablets[1].serverId);

    tablets = tableManager->getTabletsForTable(lock, 2);
    EXPECT_EQ(2lu, tablets.size());
    EXPECT_EQ(ServerId(1, 1), tablets[0].serverId);
    EXPECT_EQ(ServerId(3, 1), tablets[1].serverId);

    tablets = tableManager->getTabletsForTable(lock, 3);
    EXPECT_EQ(1lu, tablets.size());
    EXPECT_EQ(ServerId(4, 1), tablets[0].serverId);

    tablets = tableManager->getTabletsForTable(lock, 4);
    EXPECT_EQ(0lu, tablets.size());
}

TEST_F(TableManagerTest, modifyTablet) {
    Lock lock(mutex);     // Used to trick internal calls.
    tableManager->addTablet(lock, {1, 1, 6, {0, 1}, Tablet::NORMAL, {0, 5}});
    tableManager->modifyTablet(
        lock, 1, 1, 6, {1, 2}, Tablet::RECOVERING, {3, 9});
    Tablet tablet = tableManager->getTablet(lock, 1, 1, 6);
    EXPECT_EQ(ServerId(1, 2), tablet.serverId);
    EXPECT_EQ(Tablet::RECOVERING, tablet.status);
    EXPECT_EQ(Log::Position(3, 9), tablet.ctime);
    EXPECT_THROW(
        tableManager->modifyTablet(
                lock, 1, 0, 0, {0, 0}, Tablet::NORMAL, {0, 0}),
        TableManager::NoSuchTablet);
}

TEST_F(TableManagerTest, removeTabletsForTable) {
    Lock lock(mutex); // Used to trick internal calls.-
    tableManager->addTablet(lock, {1, 1, 6, {0, 1}, Tablet::NORMAL, {0, 5}});
    tableManager->addTablet(lock, {2, 2, 7, {1, 1}, Tablet::NORMAL, {1, 6}});
    tableManager->addTablet(lock, {1, 3, 8, {2, 1}, Tablet::NORMAL, {2, 7}});

    EXPECT_EQ(0lu, tableManager->removeTabletsForTable(lock, 3).size());
    EXPECT_EQ(3lu, tableManager->size(lock));

    auto tablets = tableManager->removeTabletsForTable(lock, 2);
    EXPECT_EQ(2lu, tableManager->size(lock));
    foreach (const auto& tablet, tablets) {
        EXPECT_THROW(tableManager->getTablet(lock, tablet.tableId,
                                   tablet.startKeyHash,
                                   tablet.endKeyHash),
                     TableManager::NoSuchTablet);
    }

    tablets = tableManager->removeTabletsForTable(lock, 1);
    EXPECT_EQ(0lu, tableManager->size(lock));
    foreach (const auto& tablet, tablets) {
        EXPECT_THROW(tableManager->getTablet(lock, tablet.tableId,
                                   tablet.startKeyHash,
                                   tablet.endKeyHash),
                     TableManager::NoSuchTablet);
    }
}

}  // namespace RAMCloud
