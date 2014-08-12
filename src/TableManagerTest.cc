/* Copyright (c) 2012-2014 Stanford University
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
#include "TableManager.pb.h"

namespace RAMCloud {

class TableManagerTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    CoordinatorService* service;
    CoordinatorServerList* serverList;
    CoordinatorUpdateManager* updateManager;
    TableManager* tableManager;
    ServerConfig masterConfig;

    std::mutex mutex;
    TableManager::Lock lock;

    TableManagerTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , service(cluster.coordinator.get())
        , serverList(service->context->coordinatorServerList)
        , updateManager(&service->updateManager)
        , tableManager(&service->tableManager)
        , masterConfig(ServerConfig::forTesting())
        , mutex()
        , lock(mutex)
    {
        masterConfig.services = {WireFormat::MASTER_SERVICE,
                                 WireFormat::PING_SERVICE,
                                 WireFormat::MEMBERSHIP_SERVICE};
        TestLog::reset();
        cluster.externalStorage.log.clear();
    }

    /**
     * Add a new ProtoBuf::Table::Tablet to a ProtoBuf::Table.
     */
    void
    addTablet(ProtoBuf::Table* table, uint64_t startKeyHash,
            uint64_t endKeyHash, RAMCloud::ProtoBuf::Table_Tablet_State state,
            uint64_t serverId, uint64_t logHeadId, uint32_t logHeadOffset)
    {
        ProtoBuf::Table::Tablet* tablet(table->add_tablet());
        tablet->set_start_key_hash(startKeyHash);
        tablet->set_end_key_hash(endKeyHash);
        tablet->set_state(state);
        tablet->set_server_id(serverId);
        tablet->set_ctime_log_head_id(logHeadId);
        tablet->set_ctime_log_head_offset(logHeadOffset);
    }

    struct CompareTablets {
        bool operator()(Tablet const & a,
                Tablet const & b) const
        {
            return (a.tableId < b.tableId) || ((a.tableId == b.tableId)
                    && (a.startKeyHash < b.startKeyHash));
        }
    };

    /**
     * Generate a human-readable string describing a vector of
     * Tablets.
     *
     * \param tablets
     *      Tablets to pretty-print.  This vector gets sorted by the
     *      method.
     */
    string
    tabletsToString(vector<Tablet>& tablets)
    {
        string result;
        std::sort(tablets.begin(), tablets.end(), CompareTablets());
        foreach (Tablet& tablet, tablets) {
            if (!result.empty()) {
                result.append(" ");
            }
            result.append(format("{tableId %lu, hashes 0x%lx-0x%lx, "
                    "server %s, ctime %lu.%u, %s}", tablet.tableId,
                    tablet.startKeyHash, tablet.endKeyHash,
                    tablet.serverId.toString().c_str(),
                    tablet.ctime.getSegmentId(),
                    tablet.ctime.getSegmentOffset(),
                    tablet.status == Tablet::Status::RECOVERING ? "recovering"
                    : "normal"));
        }
        return result;
    }

    DISALLOW_COPY_AND_ASSIGN(TableManagerTest);
};

TEST_F(TableManagerTest, createIndex) {
    MasterService* master1 = cluster.addServer(masterConfig)->master.get();
    MasterService* master2 = cluster.addServer(masterConfig)->master.get();
    updateManager->reset();

    EXPECT_THROW(tableManager->createIndex(1, 1, 0, 0),
                 TableManager::NoSuchTable);

    EXPECT_EQ(1U, tableManager->createTable("foo", 1));

    EXPECT_NO_THROW(tableManager->createIndex(1, 1, 0, 1));
    EXPECT_EQ(0U, master1->indexletManager.getNumIndexlets());
    EXPECT_EQ(1U, master2->indexletManager.getNumIndexlets());

    EXPECT_THROW(tableManager->createIndex(1, 0, 0, 1),
                 InvalidParameterException);
    EXPECT_EQ(0U, master1->indexletManager.getNumIndexlets());
    EXPECT_EQ(1U, master2->indexletManager.getNumIndexlets());

    EXPECT_NO_THROW(tableManager->createIndex(1, 2, 0, 1));
    EXPECT_EQ(1U, master1->indexletManager.getNumIndexlets());
    EXPECT_EQ(1U, master2->indexletManager.getNumIndexlets());

    // duplicate index already exists
    EXPECT_NO_THROW(tableManager->createIndex(1, 1, 0, 1));
};

TEST_F(TableManagerTest, dropIndex) {
    MasterService* master1 = cluster.addServer(masterConfig)->master.get();
    MasterService* master2 = cluster.addServer(masterConfig)->master.get();
    updateManager->reset();

    EXPECT_EQ(1U, tableManager->createTable("foo", 1));

    EXPECT_NO_THROW(tableManager->createIndex(1, 1, 0, 1));
    EXPECT_EQ(0U, master1->indexletManager.getNumIndexlets());
    EXPECT_EQ(1U, master2->indexletManager.getNumIndexlets());

    EXPECT_NO_THROW(tableManager->createIndex(1, 2, 0, 1));
    EXPECT_EQ(1U, master1->indexletManager.getNumIndexlets());
    EXPECT_EQ(1U, master2->indexletManager.getNumIndexlets());

    TestLog::Enable _("dropIndex");
    tableManager->dropIndex(2, 1);
    tableManager->dropIndex(1, 3);
    tableManager->dropIndex(1, 2);
    EXPECT_EQ("dropIndex: Cannot find index '1' for table '2' | "
              "dropIndex: Cannot find index '3' for table '1' | "
              "dropIndex: Dropping index '2' from table '1'",
              TestLog::get());

    EXPECT_EQ(0U, master1->indexletManager.getNumIndexlets());
    EXPECT_EQ(1U, master2->indexletManager.getNumIndexlets());
    //TODO(ashgup): Need tests for notifyCreateIndex and notifyDropIndex.
};

TEST_F(TableManagerTest, createTable_basics) {
    // Create 2 tables in a cluster with 2 masters. The first has one
    // tablet, and the second has 3 tablets.
    MasterService* master1 = cluster.addServer(masterConfig)->master.get();
    MasterService* master2 = cluster.addServer(masterConfig)->master.get();
    updateManager->reset();

    EXPECT_EQ(1U, tableManager->createTable("foo", 1));
    EXPECT_EQ(1U, tableManager->directory["foo"]->id);
    EXPECT_EQ("foo", tableManager->idMap[1]->name);
    EXPECT_EQ("Table { name: foo, id 1, Tablet { startKeyHash: 0x0, "
            "endKeyHash: 0xffffffffffffffff, serverId: 1.0, status: NORMAL, "
            "ctime: 0.0 } }",
            tableManager->debugString());
    EXPECT_EQ(1U, master1->tabletManager.getNumTablets());
    EXPECT_EQ(0U, master2->tabletManager.getNumTablets());

    EXPECT_EQ(2U, tableManager->createTable("secondTable", 3));
    EXPECT_EQ(2U, tableManager->directory["secondTable"]->id);
    EXPECT_EQ("Table { name: foo, id 1, "
            "Tablet { startKeyHash: 0x0, endKeyHash: 0xffffffffffffffff, "
            "serverId: 1.0, status: NORMAL, ctime: 0.0 } } "
            "Table { name: secondTable, id 2, "
            "Tablet { startKeyHash: 0x0, endKeyHash: 0x5555555555555555, "
            "serverId: 2.0, status: NORMAL, ctime: 0.0 } "
            "Tablet { startKeyHash: 0x5555555555555556, "
            "endKeyHash: 0xaaaaaaaaaaaaaaab, serverId: 1.0, "
            "status: NORMAL, ctime: 0.0 } "
            "Tablet { startKeyHash: 0xaaaaaaaaaaaaaaac, "
            "endKeyHash: 0xffffffffffffffff, serverId: 2.0, "
            "status: NORMAL, ctime: 0.0 } }",
            tableManager->debugString());
    EXPECT_EQ(2U, master1->tabletManager.getNumTablets());
    EXPECT_EQ(2U, master2->tabletManager.getNumTablets());

    // Make sure that information has been properly recorded on external
    // storage.
    EXPECT_EQ("name: \"secondTable\" id: 2 "
            "tablet { start_key_hash: 0 "
                "end_key_hash: 6148914691236517205 state: NORMAL "
                "server_id: 2 ctime_log_head_id: 0 ctime_log_head_offset: 0 } "
            "tablet { start_key_hash: 6148914691236517206 "
                "end_key_hash: 12297829382473034411 state: NORMAL "
                "server_id: 1 ctime_log_head_id: 0 ctime_log_head_offset: 0 } "
            "tablet { start_key_hash: 12297829382473034412 "
                "end_key_hash: 18446744073709551615 state: NORMAL "
            "server_id: 2 ctime_log_head_id: 0 ctime_log_head_offset: 0 } "
            "sequence_number: 2 created: true",
            cluster.externalStorage.getPbValue<ProtoBuf::Table>());
    EXPECT_EQ(3U, updateManager->smallestUnfinished);
}
TEST_F(TableManagerTest, createTable_tableAlreadyExists) {
    cluster.addServer(masterConfig)->master.get();
    EXPECT_EQ(1U, tableManager->createTable("foo", 1));
    EXPECT_EQ(1U, tableManager->createTable("foo", 1));
}
TEST_F(TableManagerTest, createTable_noMastersInCluster) {
    EXPECT_THROW(tableManager->createTable("foo", 1), RetryException);
}

TEST_F(TableManagerTest, dropTable_basics) {
    MasterService* master1 = cluster.addServer(masterConfig)->master.get();
    MasterService* master2 = cluster.addServer(masterConfig)->master.get();
    updateManager->reset();

    EXPECT_EQ(1U, tableManager->createTable("foo", 2));
    EXPECT_EQ(1U, master1->tabletManager.getNumTablets());
    EXPECT_EQ(1U, master2->tabletManager.getNumTablets());

    tableManager->nextTableId = 100u;
    cluster.externalStorage.log.clear();
    tableManager->dropTable("foo");
    EXPECT_EQ(0U, master1->tabletManager.getNumTablets());
    EXPECT_EQ(0U, master2->tabletManager.getNumTablets());
    EXPECT_EQ("",
            tableManager->debugString());

    // Make sure that information was properly recorded on external
    // storage.
    EXPECT_EQ("name: \"foo\" id: 1 "
            "tablet { start_key_hash: 0 end_key_hash: 9223372036854775807 "
            "state: NORMAL server_id: 1 "
            "ctime_log_head_id: 0 ctime_log_head_offset: 0 } "
            "tablet { start_key_hash: 9223372036854775808 "
            "end_key_hash: 18446744073709551615 state: NORMAL server_id: 2 "
            "ctime_log_head_id: 0 ctime_log_head_offset: 0 } "
            "sequence_number: 2 deleted: true",
            cluster.externalStorage.getPbValue<ProtoBuf::Table>());
    EXPECT_EQ(3U, updateManager->smallestUnfinished);
    EXPECT_EQ("set(UPDATE, tables/foo); remove(tables/foo)",
            cluster.externalStorage.log);
}
TEST_F(TableManagerTest, dropTable_tableDoesntExist) {
    tableManager->dropTable("foo");
    EXPECT_EQ(1U, updateManager->smallestUnfinished);
}

TEST_F(TableManagerTest, dropTable_index) {
    MasterService* master1 = cluster.addServer(masterConfig)->master.get();
    MasterService* master2 = cluster.addServer(masterConfig)->master.get();
    updateManager->reset();

    EXPECT_EQ(1U, tableManager->createTable("foo", 1));

    EXPECT_NO_THROW(tableManager->createIndex(1, 1, 0, 1));
    EXPECT_EQ(0U, master1->indexletManager.getNumIndexlets());
    EXPECT_EQ(1U, master2->indexletManager.getNumIndexlets());

    EXPECT_NO_THROW(tableManager->createIndex(1, 2, 0, 1));
    EXPECT_EQ(1U, master1->indexletManager.getNumIndexlets());
    EXPECT_EQ(1U, master2->indexletManager.getNumIndexlets());

    tableManager->dropTable("foo");
    EXPECT_EQ(0U, master1->indexletManager.getNumIndexlets());
    EXPECT_EQ(0U, master2->indexletManager.getNumIndexlets());
}


TEST_F(TableManagerTest, getTableId) {
    cluster.addServer(masterConfig)->master.get();

    tableManager->nextTableId = 100u;
    EXPECT_EQ(100U, tableManager->createTable("foo", 1));

    EXPECT_EQ(100U, tableManager->getTableId("foo"));
    EXPECT_THROW(tableManager->getTableId("bar"), TableManager::NoSuchTable);
}

TEST_F(TableManagerTest, getIndxletInfoByIndexletTableId) {
    cluster.addServer(masterConfig);
    cluster.addServer(masterConfig);
    updateManager->reset();

    EXPECT_EQ(1U, tableManager->createTable("foo", 1));
    EXPECT_NO_THROW(tableManager->createIndex(1, 1, 0, 1));
    ProtoBuf::Indexlets::Indexlet indexlet;
    EXPECT_TRUE(tableManager->getIndexletInfoByIndexletTableId(2, indexlet));
    EXPECT_EQ(1U, indexlet.table_id());
    EXPECT_EQ(1U, indexlet.index_id());
    EXPECT_EQ(2U, indexlet.indexlet_table_id());
};

TEST_F(TableManagerTest, isIndexletTable) {
    cluster.addServer(masterConfig);
    cluster.addServer(masterConfig);
    updateManager->reset();

    EXPECT_EQ(1U, tableManager->createTable("foo", 1));
    EXPECT_NO_THROW(tableManager->createIndex(1, 1, 0, 1));

    EXPECT_TRUE(tableManager->isIndexletTable(2));
    EXPECT_FALSE(tableManager->isIndexletTable(1));
    EXPECT_FALSE(tableManager->isIndexletTable(3));
};

TEST_F(TableManagerTest, markAllTabletsRecovering) {
    // Create 2 tables in a cluster with 2 masters. The first has one
    // tablet, and the second has 4 tablets.
    cluster.addServer(masterConfig)->master.get();
    cluster.addServer(masterConfig)->master.get();
    tableManager->createTable("table1", 1);
    tableManager->createTable("table2", 4);

    vector<Tablet> tablets;
    tablets = tableManager->markAllTabletsRecovering(ServerId(1));
    EXPECT_EQ("{tableId 1, hashes 0x0-0xffffffffffffffff, "
            "server 1.0, ctime 0.0, recovering} "
            "{tableId 2, hashes 0x4000000000000000-0x7fffffffffffffff, "
            "server 1.0, ctime 0.0, recovering} "
            "{tableId 2, hashes 0xc000000000000000-0xffffffffffffffff, "
            "server 1.0, ctime 0.0, recovering}", tabletsToString(tablets));
    EXPECT_EQ("Table { name: table1, id 1, "
            "Tablet { startKeyHash: 0x0, endKeyHash: 0xffffffffffffffff, "
            "serverId: 1.0, status: RECOVERING, ctime: 0.0 } } "
            "Table { name: table2, id 2, "
            "Tablet { startKeyHash: 0x0, endKeyHash: 0x3fffffffffffffff, "
            "serverId: 2.0, status: NORMAL, ctime: 0.0 } "
            "Tablet { startKeyHash: 0x4000000000000000, "
            "endKeyHash: 0x7fffffffffffffff, "
            "serverId: 1.0, status: RECOVERING, ctime: 0.0 } "
            "Tablet { startKeyHash: 0x8000000000000000, "
            "endKeyHash: 0xbfffffffffffffff, "
            "serverId: 2.0, status: NORMAL, ctime: 0.0 } "
            "Tablet { startKeyHash: 0xc000000000000000, "
            "endKeyHash: 0xffffffffffffffff, "
            "serverId: 1.0, status: RECOVERING, ctime: 0.0 } }",
            tableManager->debugString());
}

TEST_F(TableManagerTest, reassignTabletOwnership_basics) {
    cluster.addServer(masterConfig)->master.get();
    MasterService* master2 = cluster.addServer(masterConfig)->master.get();
    updateManager->reset();
    tableManager->createTable("table1", 2);
    cluster.externalStorage.log.clear();
    TestLog::reset();
    TestLog::Enable _("reassignTabletOwnership");

    tableManager->reassignTabletOwnership(ServerId(2), 1, 0, 0x7fffffffffffffff,
                99, 100);
    EXPECT_EQ("reassignTabletOwnership: Reassigning tablet "
            "[0x0,0x7fffffffffffffff] in tableId 1 from server 1.0 at "
            "mock:host=server0 to server 2.0 at mock:host=server1",
            TestLog::get());
    EXPECT_EQ("Table { name: table1, id 1, "
            "Tablet { startKeyHash: 0x0, endKeyHash: 0x7fffffffffffffff, "
            "serverId: 2.0, status: NORMAL, ctime: 99.100 } "
            "Tablet { startKeyHash: 0x8000000000000000, "
            "endKeyHash: 0xffffffffffffffff, "
            "serverId: 2.0, status: NORMAL, ctime: 0.0 } }",
            tableManager->debugString());
    EXPECT_EQ("name: \"table1\" id: 1 "
            "tablet { start_key_hash: 0 end_key_hash: 9223372036854775807 "
            "state: NORMAL server_id: 2 ctime_log_head_id: 99 "
            "ctime_log_head_offset: 100 } "
            "tablet { start_key_hash: 9223372036854775808 "
            "end_key_hash: 18446744073709551615 state: NORMAL "
            "server_id: 2 ctime_log_head_id: 0 ctime_log_head_offset: 0 } "
            "sequence_number: 2 reassign { server_id: 2 start_key_hash: 0 "
            "end_key_hash: 9223372036854775807 }",
            cluster.externalStorage.getPbValue<ProtoBuf::Table>());
    EXPECT_EQ(2U, master2->tabletManager.getNumTablets());
}

TEST_F(TableManagerTest, reassignTabletOwnership_noSuchTablet) {
    cluster.addServer(masterConfig)->master.get();
    cluster.addServer(masterConfig)->master.get();
    tableManager->createTable("table1", 2);
    cluster.externalStorage.log.clear();
    TestLog::reset();

    EXPECT_THROW(tableManager->reassignTabletOwnership(ServerId(2), 7,
            0, 0x7fffffffffffffff, 99, 100), TableManager::NoSuchTablet);
    EXPECT_THROW(tableManager->reassignTabletOwnership(ServerId(2), 1,
            0, 0x7ffffffffffffffe, 99, 100), TableManager::NoSuchTablet);
    EXPECT_THROW(tableManager->reassignTabletOwnership(ServerId(2), 1,
            1, 0x7fffffffffffffff, 99, 100), TableManager::NoSuchTablet);
}

TEST_F(TableManagerTest, recover_basics) {
    // Set up recovery information for 2 tables, one with 1 tablet and
    // the other with 2 tablets.
    ProtoBuf::Table info1, info2;
    info1.set_name("first");
    info1.set_id(12345);
    info1.set_sequence_number(88);
    addTablet(&info1, 0x100, 0x200, ProtoBuf::Table::Tablet::NORMAL,
            66, 11, 12);
    addTablet(&info1, 0x400, 0x500, ProtoBuf::Table::Tablet::NORMAL,
            77, 21, 22);
    string str;
    info1.SerializeToString(&str);
    cluster.externalStorage.getChildrenNames.push("first");
    cluster.externalStorage.getChildrenValues.push(str);
    info2.set_name("second");
    info2.set_id(444);
    info2.set_sequence_number(89);
    addTablet(&info2, 0x800, 0x900, ProtoBuf::Table::Tablet::RECOVERING,
            88, 31, 32);
    info2.SerializeToString(&str);
    cluster.externalStorage.getChildrenNames.push("second");
    cluster.externalStorage.getChildrenValues.push(str);

    tableManager->recover(87);
    EXPECT_EQ("Table { name: second, id 444, "
            "Tablet { startKeyHash: 0x800, endKeyHash: 0x900, "
            "serverId: 88.0, status: RECOVERING, ctime: 31.32 } } "
            "Table { name: first, id 12345, "
            "Tablet { startKeyHash: 0x100, endKeyHash: 0x200, "
            "serverId: 66.0, status: NORMAL, ctime: 11.12 } "
            "Tablet { startKeyHash: 0x400, endKeyHash: 0x500, "
            "serverId: 77.0, status: NORMAL, ctime: 21.22 } }",
            tableManager->debugString());
    EXPECT_EQ("get(tableManager); getChildren(tables)",
            cluster.externalStorage.log);
}
TEST_F(TableManagerTest, recover_nextTableId) {
    ProtoBuf::TableManager manager;
    manager.set_next_table_id(1234);
    string str;
    manager.SerializeToString(&str);
    cluster.externalStorage.getResults.push(str);

    tableManager->recover(100);
    EXPECT_EQ(1234u, tableManager->nextTableId);
    EXPECT_EQ("recover: initializing TableManager: nextTableId = 1234 | "
            "recover: Table recovery complete: 0 table(s)",
            TestLog::get());
}
TEST_F(TableManagerTest, recover_ignoreNullValue) {
    // First "table" isn't a table at all (might be a node?)
    cluster.externalStorage.getChildrenNames.push("first");
    cluster.externalStorage.getChildrenValues.push("");

    // Second object is really a table.
    ProtoBuf::Table info;
    info.set_name("second");
    info.set_id(444);
    info.set_sequence_number(89);
    info.set_created(true);
    addTablet(&info, 0x800, 0x900, ProtoBuf::Table::Tablet::NORMAL,
            88, 31, 32);
    string str;
    info.SerializeToString(&str);
    cluster.externalStorage.getChildrenNames.push("second");
    cluster.externalStorage.getChildrenValues.push(str);

    tableManager->recover(87);
    EXPECT_EQ("Table { name: second, id 444, "
            "Tablet { startKeyHash: 0x800, endKeyHash: 0x900, "
            "serverId: 88.0, status: NORMAL, ctime: 31.32 } }",
            tableManager->debugString());
}
TEST_F(TableManagerTest, recover_parseError) {
    // First "table" isn't a table at all (might be a node?)
    cluster.externalStorage.getChildrenNames.push("first");
    cluster.externalStorage.getChildrenValues.push("garbage");

    string message = "no exception";
    try {
        tableManager->recover(87);
    } catch (FatalError& e) {
        message = e.message;
    }
    EXPECT_EQ("couldn't parse protocol buffer in /tables/first", message);
}
TEST_F(TableManagerTest, recover_dontRecreateDeletedTable) {
    ProtoBuf::Table info;
    info.set_name("table1");
    info.set_id(444);
    info.set_sequence_number(89);
    info.set_deleted(true);
    addTablet(&info, 0x800, 0x900, ProtoBuf::Table::Tablet::NORMAL,
            88, 31, 32);
    string str;
    info.SerializeToString(&str);
    cluster.externalStorage.getChildrenNames.push("table1");
    cluster.externalStorage.getChildrenValues.push(str);

    tableManager->recover(87);
    EXPECT_EQ("", tableManager->debugString());
}
TEST_F(TableManagerTest, recover_sequenceNumberCompleted) {
    MasterService* master1 = cluster.addServer(masterConfig)->master.get();
    TestLog::reset();
    TestLog::Enable _("notifyCreate");

    ProtoBuf::Table info;
    info.set_name("table1");
    info.set_id(444);
    info.set_sequence_number(89);
    info.set_created(true);
    addTablet(&info, 0x800, 0x900, ProtoBuf::Table::Tablet::NORMAL,
            1, 31, 32);
    string str;
    info.SerializeToString(&str);
    cluster.externalStorage.getChildrenNames.push("table1");
    cluster.externalStorage.getChildrenValues.push(str);

    tableManager->recover(89);
    EXPECT_EQ("Table { name: table1, id 444, "
            "Tablet { startKeyHash: 0x800, endKeyHash: 0x900, "
            "serverId: 1.0, status: NORMAL, ctime: 31.32 } }",
            tableManager->debugString());
    EXPECT_EQ("", TestLog::get());
    EXPECT_EQ(0U, master1->tabletManager.getNumTablets());
}
TEST_F(TableManagerTest, recover_incompleteCreate) {
    MasterService* master1 = cluster.addServer(masterConfig)->master.get();
    TestLog::reset();
    TestLog::Enable _("notifyCreate");

    ProtoBuf::Table info;
    info.set_name("table1");
    info.set_id(444);
    info.set_sequence_number(89);
    info.set_created(true);
    addTablet(&info, 0x800, 0x900, ProtoBuf::Table::Tablet::NORMAL,
            1, 31, 32);
    string str;
    info.SerializeToString(&str);
    cluster.externalStorage.getChildrenNames.push("table1");
    cluster.externalStorage.getChildrenValues.push(str);

    tableManager->recover(88);
    EXPECT_EQ("Table { name: table1, id 444, "
            "Tablet { startKeyHash: 0x800, endKeyHash: 0x900, "
            "serverId: 1.0, status: NORMAL, ctime: 31.32 } }",
            tableManager->debugString());
    EXPECT_EQ("notifyCreate: Assigning table id 444, "
            "key hashes 0x800-0x900, to master 1.0",
            TestLog::get());
    EXPECT_EQ(1U, master1->tabletManager.getNumTablets());
}
TEST_F(TableManagerTest, recover_incompleteDelete) {
    ProtoBuf::Table info;
    info.set_name("table1");
    info.set_id(444);
    info.set_sequence_number(89);
    info.set_deleted(true);
    addTablet(&info, 0x800, 0x900, ProtoBuf::Table::Tablet::NORMAL,
            1, 31, 32);
    string str;
    info.SerializeToString(&str);
    cluster.externalStorage.getChildrenNames.push("table1");
    cluster.externalStorage.getChildrenValues.push(str);

    tableManager->recover(88);
    EXPECT_EQ("notifyDropTable: Requesting master 1.0 to drop table id "
            "444, key hashes 0x800-0x900 | "
            "notifyDropTable: dropTabletOwnership skipped for master 1.0 "
            "(table 444, key hashes 0x800-0x900) because server isn't "
            "running | recover: Table recovery complete: 0 table(s)",
            TestLog::get());
}
TEST_F(TableManagerTest, recover_incompleteSplitTablet) {
    ProtoBuf::Table info;
    info.set_name("table1");
    info.set_id(444);
    info.set_sequence_number(89);
    addTablet(&info, 0x800, 0x900, ProtoBuf::Table::Tablet::NORMAL,
            1, 31, 32);
    ProtoBuf::Table::Split* split = info.mutable_split();
    split->set_server_id(2);
    split->set_split_key_hash(4096);
    string str;
    info.SerializeToString(&str);
    cluster.externalStorage.getChildrenNames.push("table1");
    cluster.externalStorage.getChildrenValues.push(str);

    TestLog::Enable _("notifySplitTablet");
    tableManager->recover(88);
    EXPECT_EQ("notifySplitTablet: Requesting master 2.0 to split table "
            "id 444 at key hash 0x1000 | "
            "notifySplitTablet: splitMasterTablet skipped for master 2.0 "
            "(table 444, split key hash 0x1000) because server isn't running",
            TestLog::get());
}
TEST_F(TableManagerTest, recover_incompleteReassignTablet) {
    ProtoBuf::Table info;
    info.set_name("table1");
    info.set_id(444);
    info.set_sequence_number(89);
    addTablet(&info, 0x800, 0x900, ProtoBuf::Table::Tablet::NORMAL,
            1, 31, 32);
    ProtoBuf::Table::Reassign* reassign = info.mutable_reassign();
    reassign->set_server_id(2);
    reassign->set_start_key_hash(4096);
    reassign->set_end_key_hash(8192);
    string str;
    info.SerializeToString(&str);
    cluster.externalStorage.getChildrenNames.push("table1");
    cluster.externalStorage.getChildrenValues.push(str);

    TestLog::Enable _("notifyReassignTablet");
    tableManager->recover(88);
    EXPECT_EQ("notifyReassignTablet: Reassigning table id 444, "
            "key hashes 0x1000-0x2000 to master 2.0 | "
            "notifyReassignTablet: takeTabletOwnership failed during "
            "tablet reassignment for master 2.0 (table 444, "
            "key hashes 0x1000-0x2000) because server isn't running",
            TestLog::get());
}

TEST_F(TableManagerTest, serializeTabletConfig) {
    cluster.addServer(masterConfig)->master.get();
    cluster.addServer(masterConfig)->master.get();
    tableManager->createTable("table1", 1);
    tableManager->createTable("table2", 4);
    TestLog::reset();

    // Arrange for one of the tablet servers not to exist.
    tableManager->directory["table2"]->tablets[0]->serverId = ServerId(4);

    ProtoBuf::TableConfig tableConfig;
    tableManager->serializeTableConfig(&tableConfig, 2);
    EXPECT_EQ("tablet { table_id: 2 start_key_hash: 0 "
            "end_key_hash: 4611686018427387903 state: NORMAL "
            "server_id: 4 ctime_log_head_id: 0 ctime_log_head_offset: 0 } "
            "tablet { table_id: 2 start_key_hash: 4611686018427387904 "
            "end_key_hash: 9223372036854775807 state: NORMAL "
            "server_id: 1 service_locator: \"mock:host=server0\" "
            "ctime_log_head_id: 0 ctime_log_head_offset: 0 } "
            "tablet { table_id: 2 start_key_hash: 9223372036854775808 "
            "end_key_hash: 13835058055282163711 state: NORMAL "
            "server_id: 2 service_locator: \"mock:host=server1\" "
            "ctime_log_head_id: 0 ctime_log_head_offset: 0 } "
            "tablet { table_id: 2 start_key_hash: 13835058055282163712 "
            "end_key_hash: 18446744073709551615 state: NORMAL "
            "server_id: 1 service_locator: \"mock:host=server0\" "
            "ctime_log_head_id: 0 ctime_log_head_offset: 0 }",
            tableConfig.ShortDebugString());
    EXPECT_EQ("serializeTableConfig: Server id (4.0) in tablet map no longer "
            "in server list; omitting locator for entry",
            TestLog::get());
}

TEST_F(TableManagerTest, serializeIndexConfig) {
    cluster.addServer(masterConfig)->master.get();
    cluster.addServer(masterConfig)->master.get();

    EXPECT_EQ(1U, tableManager->createTable("foo", 1));
    EXPECT_EQ(2U, tableManager->createTable("bar", 1));

    EXPECT_NO_THROW(tableManager->createIndex(2, 1, 0, 1));

    ProtoBuf::TableConfig tableConfig;
    tableManager->serializeTableConfig(&tableConfig, 2);
    foreach (const ProtoBuf::TableConfig::Index& index, tableConfig.index()) {
        EXPECT_EQ(1U, index.index_id());
        EXPECT_EQ(0U, index.index_type());
        foreach (const ProtoBuf::TableConfig::Index::Indexlet& indexlet,
                                                        index.indexlet()) {
            EXPECT_EQ(0, (uint8_t)*indexlet.start_key().c_str());
            EXPECT_EQ(1U, indexlet.start_key().length());
            EXPECT_EQ(127, (uint8_t)*indexlet.end_key().c_str());
            EXPECT_EQ(1U, indexlet.end_key().length());
            EXPECT_EQ(1U, indexlet.server_id());
            EXPECT_EQ("mock:host=server0", indexlet.service_locator());
        }
    }
}

TEST_F(TableManagerTest, splitTablet_basics) {
    MasterService* master1 = cluster.addServer(masterConfig)->master.get();
    MasterService* master2 = cluster.addServer(masterConfig)->master.get();
    updateManager->reset();
    tableManager->createTable("foo", 2);
    cluster.externalStorage.log.clear();

    tableManager->splitTablet("foo", 0x1000);
    EXPECT_EQ("{ foo(id 1): { 0x0-0xfff on 1.0 } "
            "{ 0x8000000000000000-0xffffffffffffffff on 2.0 } "
            "{ 0x1000-0x7fffffffffffffff on 1.0 } }",
            tableManager->debugString(true));
    EXPECT_EQ(2U, master1->tabletManager.getNumTablets());
    EXPECT_EQ(1U, master2->tabletManager.getNumTablets());

    // Make sure that information was properly recorded on external
    // storage.
    ProtoBuf::Table info;
    EXPECT_TRUE(info.ParseFromString(cluster.externalStorage.setData));
    EXPECT_EQ("server_id: 1 split_key_hash: 4096",
            info.split().ShortDebugString());
    EXPECT_EQ(3U, updateManager->smallestUnfinished);
    EXPECT_EQ("set(UPDATE, tables/foo)",
            cluster.externalStorage.log);
}
TEST_F(TableManagerTest, splitTablet_splitAlreadyExists) {
    MasterService* master1 = cluster.addServer(masterConfig)->master.get();
    tableManager->createTable("foo", 2);
    cluster.externalStorage.log.clear();

    tableManager->splitTablet("foo", 0x8000000000000000);
    EXPECT_EQ("{ foo(id 1): "
            "{ 0x0-0x7fffffffffffffff on 1.0 } "
            "{ 0x8000000000000000-0xffffffffffffffff on 1.0 } }",
            tableManager->debugString(true));
    EXPECT_EQ(2U, master1->tabletManager.getNumTablets());
    EXPECT_EQ("", cluster.externalStorage.log);
}
TEST_F(TableManagerTest, splitTablet_tabletRecovering) {
    cluster.addServer(masterConfig)->master.get();
    tableManager->createTable("foo", 2);
    tableManager->directory["foo"]->tablets[1]->status = Tablet::RECOVERING;
    cluster.externalStorage.log.clear();

    EXPECT_THROW(tableManager->splitTablet("foo", 0xc000000000000000),
            RetryException);
}

TEST_F(TableManagerTest, splitRecoveringTablet_splitAlreadyExists) {
    cluster.addServer(masterConfig)->master.get();
    uint64_t tableId = tableManager->createTable("foo", 2);
    tableManager->markAllTabletsRecovering(cluster.servers[0]->serverId);

    tableManager->splitRecoveringTablet(tableId, 0x8000000000000000);
    EXPECT_EQ("{ foo(id 1): "
            "{ 0x0-0x7fffffffffffffff on 1.0 } "
            "{ 0x8000000000000000-0xffffffffffffffff on 1.0 } }",
            tableManager->debugString(true));
}
TEST_F(TableManagerTest, splitRecoveringTablet_badTableId) {
    EXPECT_THROW(tableManager->splitRecoveringTablet(99LU, 0x800),
            TableManager::NoSuchTable);
}
TEST_F(TableManagerTest, splitRecoveringTablet_success) {
    cluster.addServer(masterConfig)->master.get();
    cluster.addServer(masterConfig)->master.get();
    uint64_t tableId = tableManager->createTable("foo", 2);
    tableManager->markAllTabletsRecovering(cluster.servers[0]->serverId);

    tableManager->splitRecoveringTablet(tableId, 0x1000);
    EXPECT_EQ("{ foo(id 1): { 0x0-0xfff on 1.0 } "
            "{ 0x8000000000000000-0xffffffffffffffff on 2.0 } "
            "{ 0x1000-0x7fffffffffffffff on 1.0 } }",
            tableManager->debugString(true));
}

TEST_F(TableManagerTest, tabletRecovered_basics) {
    cluster.addServer(masterConfig)->master.get();
    tableManager->createTable("foo", 2);
    tableManager->directory["foo"]->tablets[1]->status = Tablet::RECOVERING;
    cluster.externalStorage.log.clear();

    ServerId serverId(5, 0);
    Log::Position ctime(10, 11);
    tableManager->tabletRecovered(1, 0x8000000000000000, 0xffffffffffffffff,
            serverId, ctime);
    EXPECT_EQ("name: \"foo\" id: 1 "
            "tablet { start_key_hash: 0 end_key_hash: 9223372036854775807 "
            "state: NORMAL server_id: 1 "
            "ctime_log_head_id: 0 ctime_log_head_offset: 0 } "
            "tablet { start_key_hash: 9223372036854775808 "
            "end_key_hash: 18446744073709551615 state: NORMAL server_id: 5 "
            "ctime_log_head_id: 10 ctime_log_head_offset: 11 } "
            "sequence_number: 0",
            cluster.externalStorage.getPbValue<ProtoBuf::Table>());
}
TEST_F(TableManagerTest, tabletRecovered_noSuchTablet) {
    cluster.addServer(masterConfig)->master.get();
    tableManager->createTable("foo", 2);
    tableManager->directory["foo"]->tablets[1]->status = Tablet::RECOVERING;
    cluster.externalStorage.log.clear();

    ServerId serverId(5, 0);
    Log::Position ctime(10, 11);
    EXPECT_THROW(tableManager->tabletRecovered(99, 0, 0xffffffffffffffff,
            serverId, ctime), TableManager::NoSuchTablet);
    EXPECT_THROW(tableManager->tabletRecovered(1, 0, 0x7ffffffffffffffe,
            serverId, ctime), TableManager::NoSuchTablet);
    EXPECT_THROW(tableManager->tabletRecovered(1, 1, 0x7fffffffffffffff,
            serverId, ctime), TableManager::NoSuchTablet);
    EXPECT_NO_THROW(tableManager->tabletRecovered(1, 0, 0x7fffffffffffffff,
            serverId, ctime));
}

TEST_F(TableManagerTest, findTablet) {
    TableManager::Table table("test", 111);
    table.tablets.push_back(new Tablet(111, 0, 0x100, ServerId(1, 0),
            Tablet::NORMAL, Log::Position(10, 20)));
    table.tablets.push_back(new Tablet(111, 0x101, 0x200, ServerId(2, 0),
            Tablet::RECOVERING, Log::Position(30, 40)));
    table.tablets.push_back(new Tablet(111, 0x201, 0x300, ServerId(3, 0),
            Tablet::NORMAL, Log::Position(50, 60)));
    table.tablets.push_back(new Tablet(111, 0x600, 0x700, ServerId(4, 0),
            Tablet::NORMAL, Log::Position(70, 80)));

    Tablet* tablet;
    tablet = tableManager->findTablet(lock, &table, 0x200);
    EXPECT_EQ("2.0", tablet->serverId.toString());
    tablet = tableManager->findTablet(lock, &table, 0x201);
    EXPECT_EQ("3.0", tablet->serverId.toString());
    EXPECT_THROW(tableManager->findTablet(lock, &table, 0x400), FatalError);
}

TEST_F(TableManagerTest, notifyCreate) {
    // Create a table with 4 tablets, using 2 real masters and one
    // nonexistent master.
    MasterService* master1 = cluster.addServer(masterConfig)->master.get();
    MasterService* master2 = cluster.addServer(masterConfig)->master.get();
    TableManager::Table table("test", 111);
    table.tablets.push_back(new Tablet(111, 0, 0x100, ServerId(1, 0),
            Tablet::NORMAL, Log::Position(10, 20)));
    table.tablets.push_back(new Tablet(111, 0x200, 0x300, ServerId(2, 0),
            Tablet::RECOVERING, Log::Position(30, 40)));
    table.tablets.push_back(new Tablet(111, 0x400, 0x500, ServerId(6, 2),
            Tablet::NORMAL, Log::Position(50, 60)));
    table.tablets.push_back(new Tablet(111, 0x600, 0x700, ServerId(2, 0),
            Tablet::NORMAL, Log::Position(70, 80)));

    TestLog::Enable _("notifyCreate");
    TestLog::reset();
    tableManager->notifyCreate(lock, &table);
    EXPECT_EQ(1U, master1->tabletManager.getNumTablets());
    EXPECT_EQ(2U, master2->tabletManager.getNumTablets());
    EXPECT_EQ("notifyCreate: Assigning table id 111, "
            "key hashes 0x0-0x100, to master 1.0 | "
            "notifyCreate: Assigning table id 111, "
            "key hashes 0x200-0x300, to master 2.0 | "
            "notifyCreate: Assigning table id 111, "
            "key hashes 0x400-0x500, to master 6.2 | "
            "notifyCreate: takeTabletOwnership skipped for master 6.2 "
            "(table 111, key hashes 0x400-0x500) because server "
            "isn't running | "
            "notifyCreate: Assigning table id 111, "
            "key hashes 0x600-0x700, to master 2.0",
            TestLog::get());
}

TEST_F(TableManagerTest, notifyDropTable_basics) {
    // Create a table with 2 tablets, using one real master and one
    // nonexistent master.
    cluster.addServer(masterConfig)->master.get();
    cluster.externalStorage.log.clear();
    TestLog::reset();
    ProtoBuf::Table table;
    table.set_name("table1");
    table.set_id(444);
    table.set_sequence_number(89);
    table.set_deleted(true);
    addTablet(&table, 0x200, 0x300, ProtoBuf::Table::Tablet::NORMAL,
            1, 31, 32);
    addTablet(&table, 0x800, 0x900, ProtoBuf::Table::Tablet::NORMAL,
            99, 41, 42);

    tableManager->notifyDropTable(lock, &table);
    EXPECT_EQ("notifyDropTable: Requesting master 1.0 to drop "
            "table id 444, key hashes 0x200-0x300 | "
            "deleteTablet: Could not find tablet in tableId 444 with "
            "startKeyHash 512 and endKeyHash 768 | "
            "dropTabletOwnership: Dropped ownership of (or did not own) "
            "tablet [0x200,0x300] in tableId 444 | "
            "notifyDropTable: Requesting master 99.0 to drop "
            "table id 444, key hashes 0x800-0x900 | "
            "notifyDropTable: dropTabletOwnership skipped for master 99.0 "
            "(table 444, key hashes 0x800-0x900) because server isn't running",
            TestLog::get());
    EXPECT_EQ("remove(tables/table1)",
            cluster.externalStorage.log);
}
TEST_F(TableManagerTest, notifyDropTable_syncNextTableId) {
    ProtoBuf::Table table;
    table.set_name("table1");
    table.set_id(99);
    table.set_sequence_number(89);
    table.set_deleted(true);
    addTablet(&table, 0x200, 0x300, ProtoBuf::Table::Tablet::NORMAL,
            1, 31, 32);

    tableManager->nextTableId = 100;
    tableManager->notifyDropTable(lock, &table);
    EXPECT_EQ("set(UPDATE, tableManager); remove(tables/table1)",
            cluster.externalStorage.log);
}

TEST_F(TableManagerTest, notifyReassignTablet) {
    MasterService* master1 = cluster.addServer(masterConfig)->master.get();

    ProtoBuf::Table info;
    info.set_name("foo");
    info.set_id(1);
    info.set_sequence_number(89);
    addTablet(&info, 0x800, 0x900, ProtoBuf::Table::Tablet::NORMAL,
            1, 31, 32);
    ProtoBuf::Table::Reassign* reassign = info.mutable_reassign();
    reassign->set_server_id(1);
    reassign->set_start_key_hash(0x1000);
    reassign->set_end_key_hash(0x2000);
    TestLog::Enable _("notifyReassignTablet");

    // Success.
    EXPECT_EQ(0U, master1->tabletManager.getNumTablets());
    tableManager->notifyReassignTablet(lock, &info);
    EXPECT_EQ(1U, master1->tabletManager.getNumTablets());
    EXPECT_EQ("notifyReassignTablet: Reassigning table id 1, "
            "key hashes 0x1000-0x2000 to master 1.0", TestLog::get());
    TestLog::reset();

    // No such server.
    reassign->set_server_id(5);
    tableManager->notifyReassignTablet(lock, &info);
    EXPECT_EQ("notifyReassignTablet: Reassigning table id 1, "
            "key hashes 0x1000-0x2000 to master 5.0 | "
            "notifyReassignTablet: takeTabletOwnership failed during tablet "
            "reassignment for master 5.0 (table 1, key hashes 0x1000-0x2000) "
            "because server isn't running",
            TestLog::get());
}

TEST_F(TableManagerTest, notifySplitTablet) {
    MasterService* master1 = cluster.addServer(masterConfig)->master.get();
    tableManager->createTable("foo", 1);

    ProtoBuf::Table info;
    info.set_name("foo");
    info.set_id(1);
    info.set_sequence_number(89);
    addTablet(&info, 0x800, 0x900, ProtoBuf::Table::Tablet::NORMAL,
            1, 31, 32);
    ProtoBuf::Table::Split* split = info.mutable_split();
    split->set_server_id(1);
    split->set_split_key_hash(4096);
    TestLog::Enable _("notifySplitTablet");

    // Success.
    EXPECT_EQ(1U, master1->tabletManager.getNumTablets());
    tableManager->notifySplitTablet(lock, &info);
    EXPECT_EQ(2U, master1->tabletManager.getNumTablets());
    EXPECT_EQ("notifySplitTablet: Requesting master 1.0 to split "
            "table id 1 at key hash 0x1000", TestLog::get());
    TestLog::reset();

    // No such server.
    split->set_server_id(5);
    tableManager->notifySplitTablet(lock, &info);
    EXPECT_EQ("notifySplitTablet: Requesting master 5.0 to split "
            "table id 1 at key hash 0x1000 | "
            "notifySplitTablet: splitMasterTablet skipped for master 5.0 "
            "(table 1, split key hash 0x1000) because server isn't running",
            TestLog::get());
}

TEST_F(TableManagerTest, recreateTable_basics) {
    // Create a protocol buffer that describes 3 tablets in a table.
    ProtoBuf::Table info;
    info.set_name("table1");
    info.set_id(12345);
    addTablet(&info, 0x100, 0x200, ProtoBuf::Table::Tablet::NORMAL,
            66, 11, 12);
    addTablet(&info, 0x400, 0x500, ProtoBuf::Table::Tablet::NORMAL,
            77, 21, 22);
    addTablet(&info, 0x800, 0x900, ProtoBuf::Table::Tablet::RECOVERING,
            88, 31, 32);
    tableManager->recreateTable(lock, &info);
    EXPECT_EQ("Table { name: table1, id 12345, "
            "Tablet { startKeyHash: 0x100, endKeyHash: 0x200, serverId: 66.0, "
            "status: NORMAL, ctime: 11.12 } "
            "Tablet { startKeyHash: 0x400, endKeyHash: 0x500, serverId: 77.0, "
            "status: NORMAL, ctime: 21.22 } "
            "Tablet { startKeyHash: 0x800, endKeyHash: 0x900, serverId: 88.0, "
            "status: RECOVERING, ctime: 31.32 } }",
            tableManager->debugString());
    EXPECT_EQ(12345U, tableManager->directory["table1"]->id);
    EXPECT_EQ(12346U, tableManager->nextTableId);
    EXPECT_EQ("recreateTable: Recovered tablet 0x100-0x200 for "
            "table 'table1' (id 12345) on server 66.0 | "
            "recreateTable: Recovered tablet 0x400-0x500 for "
            "table 'table1' (id 12345) on server 77.0 | "
            "recreateTable: Recovered tablet 0x800-0x900 for "
            "table 'table1' (id 12345) on server 88.0", TestLog::get());
}
TEST_F(TableManagerTest, recreateTable_directoryEntryExists) {
    ProtoBuf::Table info;
    info.set_name("table1");
    info.set_id(12345);
    addTablet(&info, 0x100, 0x200, ProtoBuf::Table::Tablet::NORMAL,
            66, 11, 12);
    tableManager->directory["table1"] = new TableManager::Table("table1", 444);
    string message = "no exception";
    try {
        tableManager->recreateTable(lock, &info);
    } catch (FatalError& e) {
        message = e.message;
    }
    EXPECT_EQ("can't recover table 'table1' (id 12345): already exists "
            "in directory", message);
}
TEST_F(TableManagerTest, recreateTable_idMapEntryExists) {
    ProtoBuf::Table info;
    info.set_name("table1");
    info.set_id(12345);
    addTablet(&info, 0x100, 0x200, ProtoBuf::Table::Tablet::NORMAL,
            66, 11, 12);
    tableManager->idMap[12345] = new TableManager::Table("table1", 444);
    string message = "no exception";
    try {
        tableManager->recreateTable(lock, &info);
    } catch (FatalError& e) {
        message = e.message;
    }
    EXPECT_EQ("can't recover table 'table1' (id 12345): already exists "
            "in idMap", message);
}
TEST_F(TableManagerTest, recreateTable_dontModifyNextTableId) {
    ProtoBuf::Table info;
    info.set_name("table1");
    info.set_id(12345);
    addTablet(&info, 0x100, 0x200, ProtoBuf::Table::Tablet::NORMAL,
            66, 11, 12);
    tableManager->nextTableId = 123457;
    tableManager->recreateTable(lock, &info);
    EXPECT_EQ(123457U, tableManager->nextTableId);
}
TEST_F(TableManagerTest, recreateTable_bogusState) {
    ProtoBuf::Table info;
    info.set_name("table1");
    addTablet(&info, 0x100, 0x200, ProtoBuf::Table::Tablet::BOGUS,
            66, 11, 12);
    EXPECT_THROW(tableManager->recreateTable(lock, &info), FatalError);
    EXPECT_EQ("recreateTable: Unknown status for tablet", TestLog::get());
}

TEST_F(TableManagerTest, serializeTable) {
    // Create a table with 4 tablets.
    TableManager::Table table("test", 111);
    table.tablets.push_back(new Tablet(111, 0, 0x100, ServerId(1, 0),
            Tablet::NORMAL, Log::Position(10, 20)));
    table.tablets.push_back(new Tablet(111, 0x200, 0x300, ServerId(2, 0),
            Tablet::RECOVERING, Log::Position(30, 40)));
    table.tablets.push_back(new Tablet(111, 0x400, 0x500, ServerId(6, 2),
            Tablet::NORMAL, Log::Position(50, 60)));
    table.tablets.push_back(new Tablet(111, 0x600, 0x700, ServerId(2, 0),
            Tablet::NORMAL, Log::Position(70, 80)));

    ProtoBuf::Table info;
    tableManager->serializeTable(lock, &table, &info);
    EXPECT_EQ("name: \"test\" id: 111 "
            "tablet { start_key_hash: 0 end_key_hash: 256 state: NORMAL "
            "server_id: 1 ctime_log_head_id: 10 ctime_log_head_offset: 20 } "
            "tablet { start_key_hash: 512 end_key_hash: 768 state: RECOVERING "
            "server_id: 2 ctime_log_head_id: 30 ctime_log_head_offset: 40 } "
            "tablet { start_key_hash: 1024 end_key_hash: 1280 state: NORMAL "
            "server_id: 8589934598 ctime_log_head_id: 50 "
            "ctime_log_head_offset: 60 } "
            "tablet { start_key_hash: 1536 end_key_hash: 1792 state: NORMAL "
            "server_id: 2 ctime_log_head_id: 70 ctime_log_head_offset: 80 }",
            info.ShortDebugString());
}

TEST_F(TableManagerTest, sync) {
    tableManager->nextTableId = 444u;
    tableManager->sync(lock);
    EXPECT_EQ("set(UPDATE, tableManager)",
            cluster.externalStorage.log);
    EXPECT_EQ("next_table_id: 444",
            cluster.externalStorage.getPbValue<ProtoBuf::TableManager>());
}

TEST_F(TableManagerTest, syncTable) {
    // Create 2 tables in a cluster with 2 masters.
    cluster.addServer(masterConfig)->master.get();
    cluster.addServer(masterConfig)->master.get();
    updateManager->reset();
    cluster.externalStorage.log.clear();

    cluster.externalStorage.log.clear();
    tableManager->createTable("foo", 1);
    EXPECT_EQ("set(UPDATE, coordinatorUpdateManager); set(UPDATE, tables/foo)",
            cluster.externalStorage.log);
    EXPECT_EQ("name: \"foo\" id: 1 tablet { "
            "start_key_hash: 0 end_key_hash: 18446744073709551615 "
            "state: NORMAL server_id: 1 ctime_log_head_id: 0 "
            "ctime_log_head_offset: 0 } "
            "sequence_number: 1 created: true",
            cluster.externalStorage.getPbValue<ProtoBuf::Table>());
    cluster.externalStorage.log.clear();
    tableManager->createTable("bar", 1);
    EXPECT_EQ("set(UPDATE, tables/bar)", cluster.externalStorage.log);
}

}  // namespace RAMCloud
