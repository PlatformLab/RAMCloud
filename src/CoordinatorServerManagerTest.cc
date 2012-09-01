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
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

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
    EXPECT_EQ(10U, serverList->at(serverIds[0]).replicationId);
    EXPECT_EQ(10U, serverList->at(serverIds[1]).replicationId);
    EXPECT_EQ(10U, serverList->at(serverIds[2]).replicationId);

    // Crash one of the backups. assignReplicationGroup should fail.
    // This test disabled because of RAM-429.
#if 0
    serverManager->hintServerDown(serverIds[2]);
    EXPECT_FALSE(serverManager->assignReplicationGroup(1000U, serverIds));
#endif
    serverManager->forceServerDownForTesting = false;
}

TEST_F(CoordinatorServerManagerTest, createReplicationGroup) {
    ServerId serverIds[9];
    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::BACKUP_SERVICE,
        WireFormat::MEMBERSHIP_SERVICE, WireFormat::PING_SERVICE};
    for (uint32_t i = 0; i < 8; i++) {
        config.localLocator = format("mock:host=backup%u", i);
        serverIds[i] = cluster.addServer(config)->serverId;
    }
    EXPECT_EQ(1U, serverList->at(serverIds[0]).replicationId);
    EXPECT_EQ(1U, serverList->at(serverIds[1]).replicationId);
    EXPECT_EQ(1U, serverList->at(serverIds[2]).replicationId);
    EXPECT_EQ(2U, serverList->at(serverIds[3]).replicationId);
    EXPECT_EQ(2U, serverList->at(serverIds[4]).replicationId);
    EXPECT_EQ(2U, serverList->at(serverIds[5]).replicationId);
    EXPECT_EQ(0U, serverList->at(serverIds[6]).replicationId);
    EXPECT_EQ(0U, serverList->at(serverIds[7]).replicationId);
    // Kill server 7.
    serverManager->forceServerDownForTesting = true;
    serverManager->hintServerDown(serverIds[7]);
    serverManager->forceServerDownForTesting = false;
    // Create a new server.
    config.localLocator = format("mock:host=backup%u", 9);
    serverIds[8] = cluster.addServer(config)->serverId;
    EXPECT_EQ(0U, serverList->at(serverIds[6]).replicationId);
    EXPECT_EQ(0U, serverList->at(serverIds[8]).replicationId);
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
                "expected_read_mbytes_per_sec: [0-9]\\+ status: 0 } "
                "version_number: 2",
                masterList.ShortDebugString()));

    ProtoBuf::ServerList backupList;
    serverList->serialize(backupList, {WireFormat::BACKUP_SERVICE});
    EXPECT_EQ("server { services: 2 server_id: 2 "
              "service_locator: \"mock:host=backup\" "
              "expected_read_mbytes_per_sec: 0 status: 0 } "
              "version_number: 2",
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
              serverList->at(masterServerId).status);
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

    EntryId entryId = logCabinHelper->appendProtoBuf(state);

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
                "expected_read_mbytes_per_sec: [0-9]\\+ status: 0 } "
                "version_number: 2",
                masterList.ShortDebugString()));

    ProtoBuf::ServerList backupList;
    serverList->serialize(backupList, {WireFormat::BACKUP_SERVICE});
    EXPECT_EQ("server { services: 2 server_id: 2 "
              "service_locator: \"mock:host=backup\" "
              "expected_read_mbytes_per_sec: 0 status: 0 } "
              "version_number: 2",
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

    EntryId entryId = logCabinHelper->appendProtoBuf(state);

    TestLog::Enable _(enlistServerFilter);

    serverManager->enlistedServerRecover(&state, entryId);

    EXPECT_EQ("", TestLog::get());

    ProtoBuf::ServerList masterList;
    serverList->serialize(masterList, {WireFormat::MASTER_SERVICE});
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
                "server { services: 25 server_id: 1 "
                "service_locator: \"mock:host=master\" "
                "expected_read_mbytes_per_sec: [0-9]\\+ status: 0 } "
                "version_number: 2",
                masterList.ShortDebugString()));

    ProtoBuf::ServerList backupList;
    serverList->serialize(backupList, {WireFormat::BACKUP_SERVICE});
    EXPECT_EQ("server { services: 2 server_id: 2 "
              "service_locator: \"mock:host=backup\" "
              "expected_read_mbytes_per_sec: 0 status: 0 } "
              "version_number: 2",
              backupList.ShortDebugString());
}

TEST_F(CoordinatorServerManagerTest, removeReplicationGroup) {
    ServerId serverIds[3];
    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::BACKUP_SERVICE,
            WireFormat::MEMBERSHIP_SERVICE, WireFormat::PING_SERVICE};
    for (uint32_t i = 0; i < 3; i++) {
        config.localLocator = format("mock:host=backup%u", i);
        serverIds[i] = cluster.addServer(config)->serverId;
    }
    serverManager->removeReplicationGroup(1);
    EXPECT_EQ(0U, serverList->at(serverIds[0]).replicationId);
    EXPECT_EQ(0U, serverList->at(serverIds[1]).replicationId);
    EXPECT_EQ(0U, serverList->at(serverIds[2]).replicationId);
}

namespace {
bool statusFilter(string s) {
    return s != "checkStatus";
}
}

TEST_F(CoordinatorServerManagerTest, sendServerList_checkLogs) {
    TestLog::Enable _(statusFilter);

    serverManager->sendServerList(ServerId(52, 0));
    EXPECT_EQ(0U, TestLog::get().find(
        "sendServerList: Could not send list to unknown server 52"));

    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::MASTER_SERVICE, WireFormat::PING_SERVICE};
    ServerId id = cluster.addServer(config)->serverId;

    TestLog::reset();
    serverManager->sendServerList(id);
    cluster.syncCoordinatorServerList();
    EXPECT_EQ(0U, TestLog::get().find(
        "sendServerList: Could not send list to server without "
        "membership service: 2"));

    config = ServerConfig::forTesting();
    config.services = {WireFormat::MEMBERSHIP_SERVICE};
    id = cluster.addServer(config)->serverId;

    TestLog::reset();
    serverManager->sendServerList(id);
    cluster.syncCoordinatorServerList();
    EXPECT_EQ(0U, TestLog::get().find(
        "handleRequest: Sending server list to server id"));
    EXPECT_NE(string::npos, TestLog::get().find(
        "applyFullList: Got complete list of servers"));

    serverList->crashed(id);
    cluster.syncCoordinatorServerList();    // clear crashed() messages
    TestLog::reset();
    serverManager->sendServerList(id);
    cluster.syncCoordinatorServerList();
    EXPECT_NE(std::string::npos, TestLog::get().find(
            "sendServerList: Could not send list to crashed server 3"));
}

TEST_F(CoordinatorServerManagerTest, sendServerList_main) {
    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::MEMBERSHIP_SERVICE};
    ServerId id = cluster.addServer(config)->serverId;

    TestLog::Enable _;
    serverManager->sendServerList(id);
    cluster.syncCoordinatorServerList();
    EXPECT_NE(string::npos, TestLog::get().find(
        "applyFullList: Got complete list of servers containing 1 "
        "entries (version number 2)"));
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
              serverList->at(master->serverId).status);
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

    EntryId entryId = logCabinHelper->appendProtoBuf(state);

    serverManager->serverDownRecover(&state, entryId);

    EXPECT_EQ("startMasterRecovery: Scheduling recovery of master 1.0 | "
              "startMasterRecovery: Recovery crashedServerId: 1.0",
               TestLog::get());
    EXPECT_EQ(ServerStatus::CRASHED,
              serverList->at(master->serverId).status);
}

TEST_F(CoordinatorServerManagerTest, setMinOpenSegmentId) {
    serverManager->setMinOpenSegmentId(masterServerId, 10);
    EXPECT_EQ(10u, serverList->at(masterServerId).minOpenSegmentId);
    serverManager->setMinOpenSegmentId(masterServerId, 9);
    EXPECT_EQ(10u, serverList->at(masterServerId).minOpenSegmentId);
    serverManager->setMinOpenSegmentId(masterServerId, 11);
    EXPECT_EQ(11u, serverList->at(masterServerId).minOpenSegmentId);
}

TEST_F(CoordinatorServerManagerTest, setMinOpenSegmentIdRecover) {
    ProtoBuf::ServerUpdate serverUpdate;
    serverUpdate.set_entry_type("ServerUpdate");
    serverUpdate.set_server_id(masterServerId.getId());
    serverUpdate.set_min_open_segment_id(10);
    EntryId entryId = logCabinHelper->appendProtoBuf(serverUpdate);

    serverManager->setMinOpenSegmentIdRecover(&serverUpdate, entryId);

    EXPECT_EQ(10u, serverList->at(masterServerId).minOpenSegmentId);
}

TEST_F(CoordinatorServerManagerTest, setMinOpenSegmentId_execute) {
    TestLog::Enable _;
    serverManager->setMinOpenSegmentId(masterServerId, 10);

    vector<Entry> entriesRead = logCabinLog->read(0);

    EntryId entryId = serverList->getServerUpdateLogId(masterServerId);
    ProtoBuf::ServerUpdate readUpdate;
    logCabinHelper->parseProtoBufFromEntry(entriesRead[entryId], readUpdate);

    EXPECT_EQ(10u, readUpdate.min_open_segment_id());
}

TEST_F(CoordinatorServerManagerTest, setMinOpenSegmentId_complete_noSuchServer)
{
    EXPECT_THROW(serverManager->setMinOpenSegmentId(ServerId(2, 2), 100),
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
