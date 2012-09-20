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
#include "RamCloud.h"
#include "Recovery.h"
#include "TaskQueue.h"

namespace RAMCloud {

class CoordinatorServiceTest : public ::testing::Test {
  public:
    Context context;
    ServerConfig masterConfig;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    CoordinatorService* service;
    MasterService* master;
    ServerId masterServerId;

    CoordinatorServiceTest()
        : context()
        , masterConfig(ServerConfig::forTesting())
        , cluster(&context)
        , ramcloud()
        , service()
        , master()
        , masterServerId()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        service = cluster.coordinator.get();

        masterConfig.services = {WireFormat::MASTER_SERVICE,
                                 WireFormat::PING_SERVICE,
                                 WireFormat::MEMBERSHIP_SERVICE};
        masterConfig.localLocator = "mock:host=master";
        Server* masterServer = cluster.addServer(masterConfig);
        master = masterServer->master.get();
        master->log->sync();
        masterServerId = masterServer->serverId;

        ramcloud.construct(&context, "mock:host=coordinator");
    }

    // Generate a string containing all of the service locators in a
    // list of servers.
    string
    getLocators(ProtoBuf::ServerList& serverList)
    {
        string result;
        foreach (const ProtoBuf::ServerList::Entry& server,
                serverList.server()) {
            if (result.size() != 0) {
                result += " ";
            }
            result += server.service_locator();
        }
        return result;
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
    master->log->append(LOG_ENTRY_TYPE_OBJ, empty, true, reference);

    // master is already enlisted
    EXPECT_EQ(0U, ramcloud->createTable("foo"));
    EXPECT_EQ(0U, ramcloud->createTable("foo")); // should be no-op
    EXPECT_EQ(1U, ramcloud->createTable("bar")); // should go to master2
    EXPECT_EQ(2U, ramcloud->createTable("baz")); // and back to master1

    EXPECT_EQ(0U, get(service->tables, "foo"));
    EXPECT_EQ(1U, get(service->tables, "bar"));
    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 64 } "
              "Tablet { tableId: 1 startKeyHash: 0 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 2.0 status: NORMAL "
              "ctime: 0, 0 } "
              "Tablet { tableId: 2 startKeyHash: 0 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 64 }",
              service->tabletMap.debugString());
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
    ramcloud->createTable("foo", 2);
    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 9223372036854775807 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 62 } "
              "Tablet { tableId: 0 startKeyHash: 9223372036854775808 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 2.0 status: NORMAL "
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
    ramcloud->createTable("foo", 3);

    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 6148914691236517205 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 62 } "
              "Tablet { tableId: 0 startKeyHash: 6148914691236517206 "
              "endKeyHash: 12297829382473034410 "
              "serverId: 2.0 status: NORMAL "
              "ctime: 0, 0 } "
              "Tablet { tableId: 0 startKeyHash: 12297829382473034411 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 62 }",
              service->tabletMap.debugString());
    EXPECT_EQ(2, master->tablets.tablet_size());
    EXPECT_EQ(1, master2.tablets.tablet_size());
}

TEST_F(CoordinatorServiceTest, splitTablet) {
    // master is already enlisted
    ramcloud->createTable("foo");
    ramcloud->splitTablet("foo", 0, ~0UL, (~0UL/2));
    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 9223372036854775806 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 62 } "
              "Tablet { tableId: 0 "
              "startKeyHash: 9223372036854775807 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 62 }",
              service->tabletMap.debugString());

    ramcloud->splitTablet("foo", 0, 9223372036854775806, 4611686018427387903);
    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 4611686018427387902 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 62 } "
              "Tablet { tableId: 0 "
              "startKeyHash: 9223372036854775807 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 62 } "
              "Tablet { tableId: 0 "
              "startKeyHash: 4611686018427387903 "
              "endKeyHash: 9223372036854775806 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 62 }",
              service->tabletMap.debugString());

    EXPECT_THROW(ramcloud->splitTablet("foo", 0, 16, 8),
                 TabletDoesntExistException);

    EXPECT_THROW(ramcloud->splitTablet("foo", 0, 0, (~0UL/2)),
                 RequestFormatError);

    EXPECT_THROW(ramcloud->splitTablet("bar", 0, ~0UL, (~0UL/2)),
                 TableDoesntExistException);
}

TEST_F(CoordinatorServiceTest, dropTable) {
    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    MasterService& master2 = *cluster.addServer(master2Config)->master;

    // Add a table, so the tests won't just compare against an empty tabletMap
    ramcloud->createTable("foo");

    // Test dropping a table that is spread across one master
    ramcloud->createTable("bar");
    EXPECT_EQ(1, master2.tablets.tablet_size());
    ramcloud->dropTable("bar");
    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 62 }",
              service->tabletMap.debugString());
    EXPECT_EQ(0, master2.tablets.tablet_size());

    // Test dropping a table that is spread across two masters
    ramcloud->createTable("bar", 2);
    EXPECT_EQ(2, master->tablets.tablet_size());
    EXPECT_EQ(1, master2.tablets.tablet_size());
    ramcloud->dropTable("bar");
    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1.0 status: NORMAL "
              "ctime: 0, 62 }",
              service->tabletMap.debugString());
    EXPECT_EQ(1, master->tablets.tablet_size());
    EXPECT_EQ(0, master2.tablets.tablet_size());
}

TEST_F(CoordinatorServiceTest, getTableId) {
    // Get the id for an existing table
    ramcloud->createTable("foo");
    uint64_t tableId = ramcloud->getTableId("foo");
    EXPECT_EQ(0lu, tableId);

    ramcloud->createTable("foo2");
    tableId = ramcloud->getTableId("foo2");
    EXPECT_EQ(1lu, tableId);

    // Try to get the id for a non-existing table
    EXPECT_THROW(ramcloud->getTableId("bar"), TableDoesntExistException);
}

TEST_F(CoordinatorServiceTest, getServerList) {
    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    master2Config.services = {WireFormat::MASTER_SERVICE,
                              WireFormat::BACKUP_SERVICE,
                              WireFormat::PING_SERVICE};
    cluster.addServer(master2Config);
    ServerConfig backupConfig = masterConfig;
    backupConfig.localLocator = "mock:host=backup1";
    backupConfig.services = {WireFormat::BACKUP_SERVICE,
                             WireFormat::PING_SERVICE};
    cluster.addServer(backupConfig);
    ProtoBuf::ServerList list;
    CoordinatorClient::getServerList(&context, &list);
    EXPECT_EQ("mock:host=master mock:host=master2 mock:host=backup1",
            getLocators(list));
}

TEST_F(CoordinatorServiceTest, getServerList_backups) {
    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    master2Config.services = {WireFormat::MASTER_SERVICE,
                              WireFormat::BACKUP_SERVICE,
                              WireFormat::PING_SERVICE};
    cluster.addServer(master2Config);
    ServerConfig backupConfig = masterConfig;
    backupConfig.localLocator = "mock:host=backup1";
    backupConfig.services = {WireFormat::BACKUP_SERVICE,
                             WireFormat::PING_SERVICE};
    cluster.addServer(backupConfig);
    ProtoBuf::ServerList list;
    CoordinatorClient::getBackupList(&context, &list);
    EXPECT_EQ("mock:host=master2 mock:host=backup1",
            getLocators(list));
}

TEST_F(CoordinatorServiceTest, getServerList_masters) {
    ServerConfig master2Config = masterConfig;
    master2Config.localLocator = "mock:host=master2";
    master2Config.services = {WireFormat::MASTER_SERVICE,
                              WireFormat::BACKUP_SERVICE,
                              WireFormat::PING_SERVICE};
    cluster.addServer(master2Config);
    ServerConfig backupConfig = masterConfig;
    backupConfig.localLocator = "mock:host=backup1";
    backupConfig.services = {WireFormat::BACKUP_SERVICE,
                             WireFormat::PING_SERVICE};
    cluster.addServer(backupConfig);
    ProtoBuf::ServerList list;
    CoordinatorClient::getMasterList(&context, &list);
    EXPECT_EQ("mock:host=master mock:host=master2",
            getLocators(list));
}

TEST_F(CoordinatorServiceTest, getTabletMap) {
    ramcloud->createTable("foo");
    ProtoBuf::Tablets tabletMap;
    CoordinatorClient::getTabletMap(&context, &tabletMap);
    EXPECT_EQ("tablet { table_id: 0 start_key_hash: 0 "
              "end_key_hash: 18446744073709551615 "
              "state: NORMAL server_id: 1 "
              "service_locator: \"mock:host=master\" "
              "ctime_log_head_id: 0 ctime_log_head_offset: 62 }",
              tabletMap.ShortDebugString());
}

static bool
reassignTabletOwnershipFilter(string s)
{
    return s == "reassignTabletOwnership";
}

TEST_F(CoordinatorServiceTest, reassignTabletOwnership) {
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
    HashTable::Reference reference;
    master2->master->log->append(LOG_ENTRY_TYPE_OBJ, empty, true, reference);

    // master is already enlisted
    ramcloud->createTable("foo");
    EXPECT_EQ(1, master->tablets.tablet_size());
    EXPECT_EQ(0, master2->master->tablets.tablet_size());
    Tablet tablet = service->tabletMap.getTablet(0lu, 0lu, ~(0lu));
    EXPECT_EQ(masterServerId, tablet.serverId);
    EXPECT_EQ(0U, tablet.ctime.getSegmentId());
    EXPECT_EQ(62U, tablet.ctime.getSegmentOffset());

    TestLog::Enable _(reassignTabletOwnershipFilter);

    EXPECT_THROW(CoordinatorClient::reassignTabletOwnership(&context,
        0, 0, -1, ServerId(472, 2)), ServerNotUpException);
    EXPECT_EQ("reassignTabletOwnership: Server id 472.2 is not up! "
        "Cannot reassign ownership of tablet 0, range "
        "[0, 18446744073709551615]!", TestLog::get());
    EXPECT_EQ(1, master->tablets.tablet_size());
    EXPECT_EQ(0, master2->master->tablets.tablet_size());

    TestLog::reset();
    EXPECT_THROW(CoordinatorClient::reassignTabletOwnership(&context,
        0, 0, 57, master2->serverId), TableDoesntExistException);
    EXPECT_EQ("reassignTabletOwnership: Could not reassign tablet 0, "
        "range [0, 57]: not found!", TestLog::get());
    EXPECT_EQ(1, master->tablets.tablet_size());
    EXPECT_EQ(0, master2->master->tablets.tablet_size());

    TestLog::reset();
    CoordinatorClient::reassignTabletOwnership(&context, 0, 0, -1,
        master2->serverId);
    EXPECT_EQ("reassignTabletOwnership: Reassigning tablet 0, range "
        "[0, 18446744073709551615] from server id 1.0 to server id 2.0.",
        TestLog::get());
    // Calling master removes the entry itself after the RPC completes on coord.
    EXPECT_EQ(1, master->tablets.tablet_size());
    EXPECT_EQ(1, master2->master->tablets.tablet_size());
    tablet = service->tabletMap.getTablet(0lu, 0lu, ~(0lu));
    EXPECT_EQ(master2->serverId, tablet.serverId);
    EXPECT_EQ(0U, tablet.ctime.getSegmentId());
    EXPECT_EQ(64U, tablet.ctime.getSegmentOffset());
}

TEST_F(CoordinatorServiceTest, setRuntimeOption) {
    ramcloud->testingSetRuntimeOption("failRecoveryMasters", "1 2 3");
    ASSERT_EQ(3u, service->runtimeOptions.failRecoveryMasters.size());
    EXPECT_EQ(1u, service->runtimeOptions.popFailRecoveryMasters());
    EXPECT_EQ(2u, service->runtimeOptions.popFailRecoveryMasters());
    EXPECT_EQ(3u, service->runtimeOptions.popFailRecoveryMasters());
    EXPECT_EQ(0u, service->runtimeOptions.popFailRecoveryMasters());
    EXPECT_THROW(ramcloud->testingSetRuntimeOption("BAD", "1 2 3"),
                 ObjectDoesntExistException);
}

TEST_F(CoordinatorServiceTest, setMasterRecoveryInfo) {
    ProtoBuf::MasterRecoveryInfo info;
    info.set_min_open_segment_id(10);
    info.set_min_open_segment_epoch(1);
    CoordinatorClient::setMasterRecoveryInfo(&context, masterServerId, info);
    EXPECT_EQ(10u, service->context->coordinatorServerList->at(
            masterServerId).masterRecoveryInfo.min_open_segment_id());
}

TEST_F(CoordinatorServiceTest, setMasterRecoveryInfo_noSuchServer) {
    string message = "no exception";
    try {
        ProtoBuf::MasterRecoveryInfo info;
        info.set_min_open_segment_id(10);
        info.set_min_open_segment_epoch(1);
        CoordinatorClient::setMasterRecoveryInfo(&context, {999, 999}, info);
    }
    catch (const ServerNotUpException& e) {
        message = e.toSymbol();
    }
    EXPECT_EQ("STATUS_SERVER_NOT_UP", message);
}

}  // namespace RAMCloud
