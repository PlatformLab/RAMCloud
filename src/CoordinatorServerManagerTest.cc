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
#include "CoordinatorServerManager.h"
#include "MasterService.h"
#include "MembershipService.h"
#include "MockCluster.h"
#include "RamCloud.h"
#include "Recovery.h"
#include "ServerList.h"
#include "TaskQueue.h"

namespace RAMCloud {

class CoordinatorServerManagerTest : public ::testing::Test {
  public:
    Context context;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    CoordinatorServerManager* serverManager;
    MasterService* master;
    ServerId masterServerId;
    CoordinatorServerList* serverList;
    LogCabinHelper* logCabinHelper;
    LogCabin::Client::Log* logCabinLog;

    CoordinatorServerManagerTest()
        : context()
        , cluster(&context)
        , ramcloud()
        , serverManager()
        , master()
        , masterServerId()
        , serverList()
        , logCabinHelper()
        , logCabinLog()
    {
        Logger::get().setLogLevels(SILENT_LOG_LEVEL);
        //Logger::get().setLogLevels(DEBUG);

        serverManager = &cluster.coordinator.get()->serverManager;

        ServerConfig masterConfig(ServerConfig::forTesting());
        masterConfig.services = {WireFormat::MASTER_SERVICE,
                                 WireFormat::PING_SERVICE,
                                 WireFormat::MEMBERSHIP_SERVICE};
        masterConfig.localLocator = "mock:host=master";
        Server* masterServer = cluster.addServer(masterConfig);
        master = masterServer->master.get();
        masterServerId = masterServer->serverId;

        ramcloud.construct(&context, "mock:host=coordinator");

        serverList = &(serverManager->service.serverList);
        logCabinHelper = serverManager->service.logCabinHelper.get();
        logCabinLog = serverManager->service.logCabinLog.get();
    }

    ~CoordinatorServerManagerTest() {
        // Finish all pending ServerList updates before destroying cluster.
        cluster.syncCoordinatorServerList();
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

    // From the debug log messages, find the entry id specified immediately
    // next to the given search string.
    EntryId
    findEntryId(string searchString)
    {
        auto position = TestLog::get().find(searchString);
        if (position == string::npos) {
            throw "Search string not found";
        } else {
            string entryIdString = TestLog::get().substr(
                TestLog::get().find(searchString) + searchString.length(), 1);
            return strtoul(entryIdString.c_str(), NULL, 0);
        }
    }

    DISALLOW_COPY_AND_ASSIGN(CoordinatorServerManagerTest);
};

TEST_F(CoordinatorServerManagerTest, assignReplicationGroup) {
    vector<ServerId> serverIds;
    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::BACKUP_SERVICE,
        WireFormat::MEMBERSHIP_SERVICE, WireFormat::PING_SERVICE};
    for (uint32_t i = 0; i < 3; i++) {
        config.localLocator = format("mock:host=backup%u", i);
        serverIds.push_back(cluster.addServer(config)->serverId);
    }
    // Check normal functionality.
    EXPECT_TRUE(serverManager->assignReplicationGroup(10U, serverIds));
    EXPECT_EQ(10U, serverList->operator[](serverIds[0]).replicationId);
    EXPECT_EQ(10U, serverList->operator[](serverIds[1]).replicationId);
    EXPECT_EQ(10U, serverList->operator[](serverIds[2]).replicationId);

    serverManager->forceServerDownForTesting = false;
}

TEST_F(CoordinatorServerManagerTest, createReplicationGroup) {
    ServerId serverIds[10];
    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::BACKUP_SERVICE,
        WireFormat::MEMBERSHIP_SERVICE, WireFormat::PING_SERVICE};
    for (uint32_t i = 0; i < 8; i++) {
        config.localLocator = format("mock:host=backup%u", i);
        serverIds[i] = cluster.addServer(config)->serverId;
    }
    EXPECT_EQ(1U, serverList->operator[](serverIds[0]).replicationId);
    EXPECT_EQ(1U, serverList->operator[](serverIds[1]).replicationId);
    EXPECT_EQ(1U, serverList->operator[](serverIds[2]).replicationId);
    EXPECT_EQ(2U, serverList->operator[](serverIds[3]).replicationId);
    EXPECT_EQ(2U, serverList->operator[](serverIds[4]).replicationId);
    EXPECT_EQ(2U, serverList->operator[](serverIds[5]).replicationId);
    EXPECT_EQ(0U, serverList->operator[](serverIds[6]).replicationId);
    EXPECT_EQ(0U, serverList->operator[](serverIds[7]).replicationId);
    // Kill server 7.
    serverManager->forceServerDownForTesting = true;
    serverManager->hintServerDown(serverIds[7]);
    serverManager->forceServerDownForTesting = false;
    // Create a new server.
    config.localLocator = format("mock:host=backup%u", 9);
    serverIds[8] = cluster.addServer(config)->serverId;
    EXPECT_EQ(0U, serverList->operator[](serverIds[6]).replicationId);
    EXPECT_EQ(0U, serverList->operator[](serverIds[8]).replicationId);
    config.localLocator = format("mock:host=backup%u", 10);
    serverIds[9] = cluster.addServer(config)->serverId;
    EXPECT_EQ(3U, serverList->operator[](serverIds[6]).replicationId);
    EXPECT_EQ(3U, serverList->operator[](serverIds[9]).replicationId);
}

TEST_F(CoordinatorServerManagerTest, enlistServer) {
    EXPECT_EQ(1U, master->serverId.getId());
    EXPECT_EQ(ServerId(2, 0),
        serverManager->enlistServer({}, {WireFormat::BACKUP_SERVICE},
                                    0, "mock:host=backup"));

    ProtoBuf::ServerList masterList;
    serverList->serialize(masterList, {WireFormat::MASTER_SERVICE});
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
                "server { services: 25 server_id: 1 "
                "service_locator: \"mock:host=master\" "
                "expected_read_mbytes_per_sec: [0-9]\\+ status: 0 "
                "replication_id: 0 } "
                "version_number: 2",
                masterList.ShortDebugString()));

    ProtoBuf::ServerList backupList;
    serverList->serialize(backupList, {WireFormat::BACKUP_SERVICE});
    EXPECT_EQ("server { services: 2 server_id: 2 "
              "service_locator: \"mock:host=backup\" "
              "expected_read_mbytes_per_sec: 0 status: 0 "
              "replication_id: 0 } "
              "version_number: 2 type: FULL_LIST",
              backupList.ShortDebugString());
}

namespace {
bool startMasterRecoveryFilter(string s) {
    return s == "startMasterRecovery";
}
}

TEST_F(CoordinatorServerManagerTest, enlistServer_ReplaceAMaster) {
    TaskQueue mgr;
    serverManager->service.recoveryManager.doNotStartRecoveries = true;
    // I can't figure out how the auto-register server list crap works.
    // It's horrible and dumb, so here's my fix.
    context.serverList = &serverManager->service.serverList;
    ServerTracker<void> tracker(&context);
    ASSERT_EQ(serverList, tracker.parent);

    ramcloud->createTable("foo");
    TestLog::Enable _(startMasterRecoveryFilter);
    EXPECT_EQ(ServerId(2, 0),
        serverManager->enlistServer(masterServerId,
                                    {WireFormat::BACKUP_SERVICE},
                                    0, "mock:host=backup"));
    EXPECT_EQ("startMasterRecovery: Scheduling recovery of master 1.0 | "
              "startMasterRecovery: Recovery crashedServerId: 1.0",
              TestLog::get());
    EXPECT_TRUE(serverList->contains(masterServerId));
    EXPECT_EQ(ServerStatus::CRASHED,
              serverList->operator[](masterServerId).status);

    ServerDetails details;
    ServerChangeEvent event;
    ASSERT_TRUE(tracker.getChange(details, event));
    EXPECT_EQ(ServerId(1, 0), details.serverId);
    EXPECT_EQ(SERVER_ADDED, event);

    ASSERT_TRUE(tracker.getChange(details, event));
    EXPECT_EQ(ServerId(1, 0), details.serverId);
    EXPECT_EQ(SERVER_CRASHED, event);

    ASSERT_TRUE(tracker.getChange(details, event));
    EXPECT_EQ(ServerId(2, 0), details.serverId);
    EXPECT_EQ(SERVER_ADDED, event);
}

TEST_F(CoordinatorServerManagerTest, enlistServer_ReplaceANonMaster) {
    TaskQueue mgr;
    serverManager->service.recoveryManager.doNotStartRecoveries = true;

    ServerConfig config = ServerConfig::forTesting();
    config.localLocator = "mock:host=backup1";
    config.services = {WireFormat::BACKUP_SERVICE};
    ServerId replacesId = cluster.addServer(config)->serverId;

    TestLog::Enable _(startMasterRecoveryFilter);
    EXPECT_EQ(ServerId(2, 1),
        serverManager->enlistServer(replacesId, {WireFormat::BACKUP_SERVICE},
                                    0, "mock:host=backup2"));
    EXPECT_EQ("startMasterRecovery: Server 2.0 crashed, but it had no tablets",
              TestLog::get());
    EXPECT_FALSE(serverList->contains(replacesId));
}

TEST_F(CoordinatorServerManagerTest, enlistServer_LogCabin) {
    TaskQueue mgr;
    serverManager->service.recoveryManager.doNotStartRecoveries = true;

    ramcloud->createTable("foo");

    TestLog::Enable _;
    EXPECT_EQ(ServerId(2, 0),
        serverManager->enlistServer(masterServerId,
                                    {WireFormat::BACKUP_SERVICE},
                                    0, "mock:host=backup"));

    vector<Entry> entriesRead = logCabinLog->read(0);
    string searchString;

    ProtoBuf::ServerInformation readState;
    searchString = "execute: LogCabin: ServerEnlisting entryId: ";
    ASSERT_NO_THROW(findEntryId(searchString));
    logCabinHelper->parseProtoBufFromEntry(
            entriesRead[findEntryId(searchString)], readState);
    EXPECT_EQ("entry_type: \"ServerEnlisting\"\n"
              "server_id: 2\nservice_mask: 2\n"
              "read_speed: 0\n"
              "service_locator: \"mock:host=backup\"\n",
              readState.DebugString());

    searchString = "complete: LogCabin: ServerEnlisted entryId: ";
    ASSERT_NO_THROW(findEntryId(searchString));
    ProtoBuf::ServerInformation readInfo;
    logCabinHelper->parseProtoBufFromEntry(
            entriesRead[findEntryId(searchString)], readInfo);
    EXPECT_EQ("entry_type: \"ServerEnlisted\"\n"
              "server_id: 2\nservice_mask: 2\n"
              "read_speed: 0\n"
              "service_locator: \"mock:host=backup\"\n",
              readInfo.DebugString());
}

namespace {
bool enlistServerFilter(string s) {
    return s == "complete";
}
}

TEST_F(CoordinatorServerManagerTest, enlistServerRecover) {
    EXPECT_EQ(1U, master->serverId.getId());

    ProtoBuf::ServerInformation state;
    state.set_entry_type("ServerEnlisting");
    state.set_server_id(ServerId(2, 0).getId());
    state.set_service_mask(
        ServiceMask({WireFormat::BACKUP_SERVICE}).serialize());
    state.set_read_speed(0);
    state.set_service_locator("mock:host=backup");

    EntryId entryId = logCabinHelper->appendProtoBuf(
                serverManager->service.expectedEntryId, state);

    TestLog::Enable _(enlistServerFilter);

    serverManager->enlistServerRecover(&state, entryId);

    string searchString = "complete: LogCabin: ServerEnlisted entryId: ";
    ASSERT_NO_THROW(findEntryId(searchString));

    EXPECT_EQ(
        format("complete: Enlisting new server at mock:host=backup "
               "(server id 2.0) supporting services: BACKUP_SERVICE | "
               "complete: Backup at id 2.0 has 0 MB/s read | "
               "complete: LogCabin: ServerEnlisted entryId: %lu",
               findEntryId(searchString)),
        TestLog::get());

    ProtoBuf::ServerList masterList;
    serverList->serialize(masterList, {WireFormat::MASTER_SERVICE});
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
                "server { services: 25 server_id: 1 "
                "service_locator: \"mock:host=master\" "
                "expected_read_mbytes_per_sec: [0-9]\\+ status: 0 "
                "replication_id: 0 } "
                "version_number: 2",
                masterList.ShortDebugString()));

    ProtoBuf::ServerList backupList;
    serverList->serialize(backupList, {WireFormat::BACKUP_SERVICE});
    EXPECT_EQ("server { services: 2 server_id: 2 "
              "service_locator: \"mock:host=backup\" "
              "expected_read_mbytes_per_sec: 0 status: 0 "
              "replication_id: 0 } "
              "version_number: 2 type: FULL_LIST",
              backupList.ShortDebugString());
}

TEST_F(CoordinatorServerManagerTest, enlistedServerRecover) {
    EXPECT_EQ(1U, master->serverId.getId());

    ProtoBuf::ServerInformation state;
    state.set_entry_type("ServerEnlisted");
    state.set_server_id(ServerId(2, 0).getId());
    state.set_service_mask(
        ServiceMask({WireFormat::BACKUP_SERVICE}).serialize());
    state.set_read_speed(0);
    state.set_service_locator("mock:host=backup");

    EntryId entryId = logCabinHelper->appendProtoBuf(
            serverManager->service.expectedEntryId, state);

    TestLog::Enable _(enlistServerFilter);

    serverManager->enlistedServerRecover(&state, entryId);

    EXPECT_EQ("", TestLog::get());

    ProtoBuf::ServerList masterList;
    serverList->serialize(masterList, {WireFormat::MASTER_SERVICE});
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
                "server { services: 25 server_id: 1 "
                "service_locator: \"mock:host=master\" "
                "expected_read_mbytes_per_sec: [0-9]\\+ status: 0 "
                "replication_id: 0 } "
                "version_number: 2",
                masterList.ShortDebugString()));

    ProtoBuf::ServerList backupList;
    serverList->serialize(backupList, {WireFormat::BACKUP_SERVICE});
    EXPECT_EQ("server { services: 2 server_id: 2 "
              "service_locator: \"mock:host=backup\" "
              "expected_read_mbytes_per_sec: 0 status: 0 "
              "replication_id: 0 } "
              "version_number: 2 type: FULL_LIST",
              backupList.ShortDebugString());
}

TEST_F(CoordinatorServerManagerTest, removeReplicationGroup) {
    ServerId serverIds[4];
    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::BACKUP_SERVICE,
            WireFormat::MEMBERSHIP_SERVICE, WireFormat::PING_SERVICE};
    for (uint32_t i = 0; i < 3; i++) {
        config.localLocator = format("mock:host=backup%u", i);
        serverIds[i] = cluster.addServer(config)->serverId;
    }
    EXPECT_EQ(1U, serverList->operator[](serverIds[1]).replicationId);
    serverManager->forceServerDownForTesting = true;
    serverManager->hintServerDown(serverIds[1]);
    serverManager->forceServerDownForTesting = false;
    EXPECT_EQ(0U, serverList->operator[](serverIds[0]).replicationId);
    EXPECT_EQ(0U, serverList->operator[](serverIds[2]).replicationId);
    config.localLocator = format("mock:host=backup%u", 3);
    serverIds[3] = cluster.addServer(config)->serverId;
    EXPECT_EQ(2U, serverList->operator[](serverIds[2]).replicationId);
    serverManager->removeReplicationGroup(2);
    EXPECT_EQ(0U, serverList->operator[](serverIds[0]).replicationId);
    EXPECT_EQ(0U, serverList->operator[](serverIds[2]).replicationId);
    EXPECT_EQ(0U, serverList->operator[](serverIds[3]).replicationId);
}

namespace {
bool statusFilter(string s) {
    return s != "checkStatus";
}
}

TEST_F(CoordinatorServerManagerTest, serverDown_backup) {
    ServerId id =
        serverManager->enlistServer({}, {WireFormat::BACKUP_SERVICE},
                                    0, "mock:host=backup");
    EXPECT_EQ(1U, serverManager->service.serverList.backupCount());
    serverManager->forceServerDownForTesting = true;
    serverManager->serverDown(id);
    EXPECT_EQ(0U, serverList->backupCount());
    EXPECT_FALSE(serverList->contains(id));
}

TEST_F(CoordinatorServerManagerTest, serverDown_server) {
    TaskQueue mgr;
    serverManager->service.recoveryManager.doNotStartRecoveries = true;
    // master is already enlisted

    ramcloud->createTable("foo");
    serverManager->forceServerDownForTesting = true;
    TestLog::Enable _(startMasterRecoveryFilter);

    serverManager->serverDown(masterServerId);

    EXPECT_EQ("startMasterRecovery: Scheduling recovery of master 1.0 | "
              "startMasterRecovery: Recovery crashedServerId: 1.0",
               TestLog::get());
    EXPECT_EQ(ServerStatus::CRASHED,
              serverList->operator[](master->serverId).status);
}

TEST_F(CoordinatorServerManagerTest, serverDown_LogCabin) {
    TaskQueue mgr;
    serverManager->service.recoveryManager.doNotStartRecoveries = true;
    // master is already enlisted

    ramcloud->createTable("foo");
    serverManager->forceServerDownForTesting = true;

    TestLog::Enable _;
    serverManager->serverDown(masterServerId);

    vector<Entry> entriesRead = logCabinLog->read(0);

    string searchString = "execute: LogCabin: StateServerDown entryId: ";
    ASSERT_NO_THROW(findEntryId(searchString));
    ProtoBuf::StateServerDown readState;
    logCabinHelper->parseProtoBufFromEntry(
            entriesRead[findEntryId(searchString)], readState);

    EXPECT_EQ("entry_type: \"StateServerDown\"\nserver_id: 1\n",
              readState.DebugString());
}

TEST_F(CoordinatorServerManagerTest, serverDownRecover) {
    TaskQueue mgr;
    serverManager->service.recoveryManager.doNotStartRecoveries = true;
    // master is already enlisted

    ramcloud->createTable("foo");
    serverManager->forceServerDownForTesting = true;
    TestLog::Enable _(startMasterRecoveryFilter);

    ProtoBuf::StateServerDown state;
    state.set_entry_type("StateServerDown");
    state.set_server_id(masterServerId.getId());

    EntryId entryId = logCabinHelper->appendProtoBuf(
            serverManager->service.expectedEntryId, state);

    serverManager->serverDownRecover(&state, entryId);

    EXPECT_EQ("startMasterRecovery: Scheduling recovery of master 1.0 | "
              "startMasterRecovery: Recovery crashedServerId: 1.0",
               TestLog::get());
    EXPECT_EQ(ServerStatus::CRASHED,
              serverList->operator[](master->serverId).status);
}

TEST_F(CoordinatorServerManagerTest, setMasterRecoveryInfo) {
    ProtoBuf::MasterRecoveryInfo info;
    info.set_min_open_segment_id(10);
    info.set_min_open_segment_epoch(1);
    serverManager->setMasterRecoveryInfo(masterServerId, info);
    auto other = serverList->operator[](masterServerId).masterRecoveryInfo;
    EXPECT_EQ(10lu, other.min_open_segment_id());
    EXPECT_EQ(1lu, other.min_open_segment_epoch());
    info.set_min_open_segment_id(9);
    info.set_min_open_segment_epoch(0);
    serverManager->setMasterRecoveryInfo(masterServerId, info);
    other = serverList->operator[](masterServerId).masterRecoveryInfo;
    EXPECT_EQ(9lu, other.min_open_segment_id());
    EXPECT_EQ(0lu, other.min_open_segment_epoch());
}

TEST_F(CoordinatorServerManagerTest, setMasterRecoveryInfoRecover) {
    ProtoBuf::ServerUpdate serverUpdate;
    serverUpdate.set_entry_type("ServerUpdate");
    serverUpdate.set_server_id(masterServerId.getId());
    serverUpdate.mutable_master_recovery_info()->set_min_open_segment_id(10);
    serverUpdate.mutable_master_recovery_info()->set_min_open_segment_epoch(1);
    EntryId entryId = logCabinHelper->appendProtoBuf(
                serverManager->service.expectedEntryId, serverUpdate);

    serverManager->setMasterRecoveryInfoRecover(&serverUpdate, entryId);

    EXPECT_EQ(10lu, serverList->operator[](masterServerId).
                        masterRecoveryInfo.min_open_segment_id());
    EXPECT_EQ(1lu, serverList->operator[](masterServerId).
                        masterRecoveryInfo.min_open_segment_epoch());
}

TEST_F(CoordinatorServerManagerTest, setMasterRecoveryInfo_execute) {
    TestLog::Enable _;
    ProtoBuf::MasterRecoveryInfo info;
    info.set_min_open_segment_id(10);
    info.set_min_open_segment_epoch(1);
    serverManager->setMasterRecoveryInfo(masterServerId, info);

    vector<Entry> entriesRead = logCabinLog->read(0);

    EntryId entryId = serverList->getServerUpdateLogId(masterServerId);
    ProtoBuf::ServerUpdate readUpdate;
    logCabinHelper->parseProtoBufFromEntry(entriesRead[entryId], readUpdate);

    EXPECT_EQ(10u, readUpdate.master_recovery_info().min_open_segment_id());
    EXPECT_EQ(1u, readUpdate.master_recovery_info().min_open_segment_epoch());
}

TEST_F(CoordinatorServerManagerTest,
       setMasterRecoveryInfo_complete_noSuchServer)
{
    ProtoBuf::MasterRecoveryInfo info;
    info.set_min_open_segment_id(10);
    info.set_min_open_segment_epoch(1);
    EXPECT_THROW(serverManager->setMasterRecoveryInfo({2, 2}, info),
                 ServerListException);
}

TEST_F(CoordinatorServerManagerTest, verifyServerFailure) {
    // Case 1: server up.
    EXPECT_FALSE(serverManager->verifyServerFailure(masterServerId));

    // Case 2: server incommunicado.
    MockTransport mockTransport(&context);
    context.transportManager->registerMock(&mockTransport, "mock2");
    ServerId deadId = serverList->generateUniqueId();
    serverList->add(deadId, "mock2:", {WireFormat::PING_SERVICE}, 100);
    EXPECT_TRUE(serverManager->verifyServerFailure(deadId));
}

}  // namespace RAMCloud
