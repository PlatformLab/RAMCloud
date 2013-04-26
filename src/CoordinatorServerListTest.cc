/* Copyright (c) 2011-2012 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <thread>
#include <mutex>
#include <queue>

#include "TestUtil.h"
#include "AbstractServerList.h"
#include "CoordinatorServerList.h"
#include "MockCluster.h"
#include "MockTransport.h"
#include "RamCloud.h"
#include "ServerTracker.h"
#include "ShortMacros.h"
#include "TransportManager.h"
#include "ServerList.pb.h"

namespace RAMCloud {

namespace {
struct MockServerTracker : public ServerTracker<int> {
    explicit MockServerTracker(Context* context)
    : ServerTracker<int>(context)
    , changes() {
    }

    void enqueueChange(const ServerDetails& server, ServerChangeEvent event) {
        changes.push({server, event});
    }

    void fireCallback() {
        TEST_LOG("called");
    }
    std::queue<ServerTracker<int>::ServerChange> changes;
};
}

class CoordinatorServerListTest : public ::testing::Test {
  public:
    Context context;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    MasterService* master; // Unit tests need to call enlistMaster()
                           // before trying to use master.
    ServerId masterServerId; // Unit tests need to call enlistMaster()
                             // before trying to use masterServerId.
    CoordinatorService* service;
    CoordinatorServerList* sl;
    Tub<MockServerTracker> tr;
    LogCabinHelper* logCabinHelper;
    LogCabin::Client::Log* logCabinLog;

    CoordinatorServerListTest()
        : context()
        , cluster(&context)
        , ramcloud()
        , master()
        , masterServerId()
        , service()
        , sl()
        , tr()
        , logCabinHelper()
        , logCabinLog()
    {
        service = cluster.coordinator.get();

        ramcloud.construct(service->context, "mock:host=coordinator");
        sl = service->context->coordinatorServerList;
        tr.construct(service->context);
        logCabinHelper = service->context->logCabinHelper;
        logCabinLog = service->context->logCabinLog;

        sl->haltUpdater();
    }

    ~CoordinatorServerListTest() {
        // Stop all pending ServerList updates before destroying cluster.
        cluster.haltCoordinatorServerListUpdater();
    }

    void add(ServerId serverId, string serviceLocator,
               ServiceMask serviceMask, uint32_t readSpeed,
               bool commit = true)
    {
        Lock lock(sl->mutex);
        sl->add(lock, serverId, serviceLocator, serviceMask, readSpeed);

        if (commit) {
            sl->version++;
            sl->pushUpdate(lock, sl->version);
        }
    }

    void crashed(ServerId serverId, bool commit = true)
    {
        Lock lock(sl->mutex);
        sl->crashed(lock, serverId);

        if (commit) {
            sl->version++;
            sl->pushUpdate(lock, sl->version);
        }
    }

    void remove(ServerId serverId, bool commit = true)
    {
        Tub<CoordinatorServerList::Entry>& entry =
                sl->serverList[serverId.indexNumber()].entry;
        entry.destroy();
    }

    ServerId generateUniqueId() {
        Lock lock(sl->mutex);
        return sl->generateUniqueId(lock);
    }

    bool isClusterUpToDate() {
        Lock lock(sl->mutex);
        return sl->isClusterUpToDate(lock);
    }

    void pushUpdate(uint64_t version) {
        Lock lock(sl->mutex);
        sl->pushUpdate(lock, version);
    }

    // Enlist a master and store details in master and masterServerId.
    void
    enlistMaster() {
        ServerConfig masterConfig(ServerConfig::forTesting());
        masterConfig.services = {WireFormat::MASTER_SERVICE,
                                 WireFormat::PING_SERVICE,
                                 WireFormat::MEMBERSHIP_SERVICE};
        masterConfig.localLocator = "mock:host=master";
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

    typedef std::unique_lock<std::mutex> Lock;
    DISALLOW_COPY_AND_ASSIGN(CoordinatorServerListTest);
};

/*
 * Return true if a CoordinatorServerList::Entry is indentical to the
 * given serialized protobuf entry.
 */
static bool
protoBufMatchesEntry(const ProtoBuf::ServerList_Entry& protoBufEntry,
        const CoordinatorServerList::Entry& serverListEntry,
        ServerStatus status) {
    if (serverListEntry.services.serialize() !=
            protoBufEntry.services())
        return false;
    if (*serverListEntry.serverId != protoBufEntry.server_id())
        return false;
    if (serverListEntry.serviceLocator != protoBufEntry.service_locator())
        return false;
    if (serverListEntry.expectedReadMBytesPerSec !=
            protoBufEntry.expected_read_mbytes_per_sec())
        return false;
    if (status != ServerStatus(protoBufEntry.status()))
        return false;

    return true;
}

namespace {
    bool statusFilter(string s) {
        return s != "checkStatus";
    }

    bool workSuccessFilter(string s) {
        return s == "workSuccess";
    }

    bool updateLoopFilter(string s) {
        return s == "updateLoop";
    }

    bool startMasterRecoveryFilter(string s) {
        return s == "startMasterRecovery";
    }

    bool enlistServerFilter(string s) {
        return s == "complete";
    }

    bool fireCallbackFilter(string s) {
        return s == "fireCallback";
    }
}

TEST_F(CoordinatorServerListTest, Entry_constructor) {
    CoordinatorServerList::Entry a(ServerId(52, 374),
        "You forgot your boarding pass", {WireFormat::MASTER_SERVICE});
    EXPECT_EQ(ServerId(52, 374), a.serverId);
    EXPECT_EQ("You forgot your boarding pass", a.serviceLocator);
    EXPECT_TRUE(a.isMaster());
    EXPECT_FALSE(a.isBackup());
    EXPECT_EQ(0U, a.expectedReadMBytesPerSec);

    CoordinatorServerList::Entry b(ServerId(27, 72),
        "I ain't got time to bleed", { WireFormat::BACKUP_SERVICE});
    EXPECT_EQ(ServerId(27, 72), b.serverId);
    EXPECT_EQ("I ain't got time to bleed", b.serviceLocator);
    EXPECT_FALSE(b.isMaster());
    EXPECT_TRUE(b.isBackup());
    EXPECT_EQ(0U, b.expectedReadMBytesPerSec);
}

TEST_F(CoordinatorServerListTest, Entry_serialize) {
    CoordinatorServerList::Entry entry(ServerId(0, 0), "",
                                       {WireFormat::BACKUP_SERVICE});
    entry.serverId = ServerId(5234, 23482);
    entry.serviceLocator = "giggity";
    entry.expectedReadMBytesPerSec = 723;

    ProtoBuf::ServerList_Entry serialEntry;
    entry.serialize(serialEntry);
    auto backupMask = ServiceMask{WireFormat::BACKUP_SERVICE}.serialize();
    EXPECT_EQ(backupMask, serialEntry.services());
    EXPECT_EQ(ServerId(5234, 23482).getId(), serialEntry.server_id());
    EXPECT_EQ("giggity", serialEntry.service_locator());
    EXPECT_EQ(723U, serialEntry.expected_read_mbytes_per_sec());
    EXPECT_EQ(ServerStatus::UP, ServerStatus(serialEntry.status()));

    entry.services = ServiceMask{WireFormat::MASTER_SERVICE};
    ProtoBuf::ServerList_Entry serialEntry2;
    entry.serialize(serialEntry2);
    auto masterMask = ServiceMask{WireFormat::MASTER_SERVICE}.serialize();
    EXPECT_EQ(masterMask, serialEntry2.services());
    EXPECT_EQ(0U, serialEntry2.expected_read_mbytes_per_sec());
    EXPECT_EQ(ServerStatus::UP, ServerStatus(serialEntry2.status()));
}

TEST_F(CoordinatorServerListTest, constructor) {
    EXPECT_EQ(0U, sl->numberOfMasters);
    EXPECT_EQ(0U, sl->numberOfBackups);
    EXPECT_EQ(0U, sl->version);
}

TEST_F(CoordinatorServerListTest, enlistServer) {
    enlistMaster();
    EXPECT_EQ(1U, master->serverId.getId());
    EXPECT_EQ(ServerId(2, 0),
              sl->enlistServer({}, {WireFormat::BACKUP_SERVICE}, 0,
                               "mock:host=backup"));

    ProtoBuf::ServerList masterList;
    sl->serialize(masterList, {WireFormat::MASTER_SERVICE});
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
                "server { services: 25 server_id: 1 "
                "service_locator: \"mock:host=master\" "
                "expected_read_mbytes_per_sec: [0-9]\\+ status: 0 "
                "replication_id: 0 } "
                "version_number: 2",
                 masterList.ShortDebugString()));

    ProtoBuf::ServerList backupList;
    sl->serialize(backupList, {WireFormat::BACKUP_SERVICE});
    EXPECT_EQ("server { services: 2 server_id: 2 "
              "service_locator: \"mock:host=backup\" "
              "expected_read_mbytes_per_sec: 0 status: 0 "
              "replication_id: 0 } "
              "version_number: 2 type: FULL_LIST",
               backupList.ShortDebugString());
}

TEST_F(CoordinatorServerListTest, enlistServer_ReplaceAMaster) {
    enlistMaster();
    service->context->recoveryManager->doNotStartRecoveries = true;

    ServerTracker<void> tracker(service->context);
    ASSERT_EQ(sl, tracker.parent);

    ramcloud->createTable("foo");
    TestLog::Enable _(startMasterRecoveryFilter);
    EXPECT_EQ(ServerId(2, 0),
              sl->enlistServer(masterServerId, {WireFormat::BACKUP_SERVICE},
                               0, "mock:host=backup"));
    EXPECT_EQ("startMasterRecovery: Scheduling recovery of master 1.0 | "
              "startMasterRecovery: Recovery crashedServerId: 1.0",
               TestLog::get());
    EXPECT_TRUE(sl->contains(masterServerId));
    EXPECT_EQ(ServerStatus::CRASHED, (*sl)[masterServerId].status);

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

TEST_F(CoordinatorServerListTest, enlistServer_ReplaceANonMaster) {
    enlistMaster();
    service->context->recoveryManager->doNotStartRecoveries = true;

    ServerConfig config = ServerConfig::forTesting();
    config.localLocator = "mock:host=backup1";
    config.services = {WireFormat::BACKUP_SERVICE};
    ServerId replacesId = cluster.addServer(config)->serverId;

    TestLog::Enable _(startMasterRecoveryFilter);
    EXPECT_EQ(ServerId(3, 0),
              sl->enlistServer(replacesId, {WireFormat::BACKUP_SERVICE},
                               0, "mock:host=backup2"));
    EXPECT_EQ("startMasterRecovery: Server 2.0 crashed, but it had no tablets",
              TestLog::get());
    // Can't test this any more since the entry for server with id replacesId
    // will actually get removed from server list only once updates to cluster
    // are acknowledged -- and that does not happen in this unit test.
    // EXPECT_FALSE(sl->contains(replacesId));
}

TEST_F(CoordinatorServerListTest, enlistServer_LogCabin) {
    enlistMaster();
    service->context->recoveryManager->doNotStartRecoveries = true;
    ramcloud->createTable("foo");

    TestLog::Enable _;
    EXPECT_EQ(ServerId(2, 0),
              sl->enlistServer(masterServerId, {WireFormat::BACKUP_SERVICE},
                               0, "mock:host=backup"));

    vector<Entry> entriesRead = logCabinLog->read(0);
    string searchString;

    ProtoBuf::EntryType readStateServerUpUpdate;
    searchString = "execute: LogCabin: ServerUpUpdate entryId: ";
    ASSERT_NO_THROW(findEntryId(searchString));
    logCabinHelper->parseProtoBufFromEntry(
            entriesRead[findEntryId(searchString)], readStateServerUpUpdate);
    EXPECT_EQ("entry_type: \"ServerUpUpdate\"\n",
               readStateServerUpUpdate.DebugString());

    ProtoBuf::ServerInformation readStateServerUp;
    searchString = "execute: LogCabin: ServerUp entryId: ";
    ASSERT_NO_THROW(findEntryId(searchString));
    logCabinHelper->parseProtoBufFromEntry(
            entriesRead[findEntryId(searchString)], readStateServerUp);
    EXPECT_EQ("entry_type: \"ServerUp\"\n"
              "server_id: 2\nservice_mask: 2\n"
              "read_speed: 0\n"
              "service_locator: \"mock:host=backup\"\n"
              "update_version: 3\n",
               readStateServerUp.DebugString());
}

TEST_F(CoordinatorServerListTest, serialize) {
    {
        ProtoBuf::ServerList serverList;
        sl->serialize(serverList, {});
        EXPECT_EQ(0, serverList.server_size());
        sl->serialize(serverList, {WireFormat::MASTER_SERVICE,
            WireFormat::BACKUP_SERVICE});
        EXPECT_EQ(0, serverList.server_size());
    }

    ServerId first = generateUniqueId();
    add(first, "", {WireFormat::MASTER_SERVICE}, 100);
    ServerId second = generateUniqueId();
    add(second, "", {WireFormat::MASTER_SERVICE}, 100);
    ServerId third = generateUniqueId();
    add(third, "", {WireFormat::MASTER_SERVICE}, 100);
    ServerId fourth = generateUniqueId();
    add(fourth, "", {WireFormat::BACKUP_SERVICE}, 100);
    ServerId last = generateUniqueId();
    add(last, "", {WireFormat::MASTER_SERVICE,
        WireFormat::BACKUP_SERVICE}, 100);
    remove(first); // ensure removed entries are skipped
    crashed(last); // ensure crashed entries are included

    auto masterMask = ServiceMask{WireFormat::MASTER_SERVICE}.serialize();
    auto backupMask = ServiceMask{WireFormat::BACKUP_SERVICE}.serialize();
    auto bothMask = ServiceMask{WireFormat::MASTER_SERVICE,
        WireFormat::BACKUP_SERVICE}.serialize();
    {
        ProtoBuf::ServerList serverList;
        sl->serialize(serverList, {});
        EXPECT_EQ(0, serverList.server_size());
        sl->serialize(serverList, {WireFormat::MASTER_SERVICE});
        EXPECT_EQ(3, serverList.server_size());
        EXPECT_EQ(masterMask, serverList.server(0).services());
        EXPECT_EQ(masterMask, serverList.server(1).services());
        EXPECT_EQ(bothMask, serverList.server(2).services());
        EXPECT_EQ(ServerStatus::CRASHED,
                  ServerStatus(serverList.server(2).status()));
    }

    {
        ProtoBuf::ServerList serverList;
        sl->serialize(serverList, {WireFormat::BACKUP_SERVICE});
        EXPECT_EQ(2, serverList.server_size());
        EXPECT_EQ(backupMask, serverList.server(0).services());
        EXPECT_EQ(bothMask, serverList.server(1).services());
        EXPECT_EQ(ServerStatus::CRASHED,
                  ServerStatus(serverList.server(1).status()));
    }

    {
        ProtoBuf::ServerList serverList;
        sl->serialize(serverList, {WireFormat::MASTER_SERVICE,
                                   WireFormat::BACKUP_SERVICE});
        EXPECT_EQ(4, serverList.server_size());
        EXPECT_EQ(masterMask, serverList.server(0).services());
        EXPECT_EQ(masterMask, serverList.server(1).services());
        EXPECT_EQ(backupMask, serverList.server(2).services());
        EXPECT_EQ(bothMask, serverList.server(3).services());
        EXPECT_EQ(ServerStatus::CRASHED,
                  ServerStatus(serverList.server(3).status()));
    }
}

TEST_F(CoordinatorServerListTest, serverCrashed_backup) {
    ServerId id = sl->enlistServer({}, {WireFormat::BACKUP_SERVICE},
                                   0, "mock:host=backup");
    EXPECT_EQ(1U, sl->backupCount());
    service->forceServerDownForTesting = true;
    sl->serverCrashed(id);
    sl->sync();
    EXPECT_EQ(0U, sl->backupCount());
    EXPECT_FALSE(sl->contains(id));
}

TEST_F(CoordinatorServerListTest, serverCrashed_server) {
    enlistMaster();
    service->context->recoveryManager->doNotStartRecoveries = true;

    ramcloud->createTable("foo");
    service->forceServerDownForTesting = true;
    TestLog::Enable _(startMasterRecoveryFilter);

    sl->serverCrashed(masterServerId);

    EXPECT_EQ("startMasterRecovery: Scheduling recovery of master 1.0 | "
              "startMasterRecovery: Recovery crashedServerId: 1.0",
               TestLog::get());
    EXPECT_EQ(ServerStatus::CRASHED, (*sl)[master->serverId].status);
}

TEST_F(CoordinatorServerListTest, serverCrashed_LogCabin) {
    enlistMaster();
    service->context->recoveryManager->doNotStartRecoveries = true;

    ramcloud->createTable("foo");
    service->forceServerDownForTesting = true;

    TestLog::Enable _;
    sl->serverCrashed(masterServerId);

    vector<Entry> entriesRead = logCabinLog->read(0);
    string searchString = "execute: LogCabin: ServerCrashed entryId: ";
    ASSERT_NO_THROW(findEntryId(searchString));
    ProtoBuf::ServerCrashInfo readState;
    logCabinHelper->parseProtoBufFromEntry(
            entriesRead[findEntryId(searchString)], readState);

    EXPECT_EQ("entry_type: \"ServerCrashed\"\nserver_id: 1\n"
              "update_version: 2\n",
              readState.DebugString());
}

TEST_F(CoordinatorServerListTest, setMasterRecoveryInfo) {
    enlistMaster();
    ProtoBuf::MasterRecoveryInfo info;
    info.set_min_open_segment_id(10);
    info.set_min_open_segment_epoch(1);
    sl->setMasterRecoveryInfo(masterServerId, info);
    auto other = (*sl)[masterServerId].masterRecoveryInfo;
    EXPECT_EQ(10lu, other.min_open_segment_id());
    EXPECT_EQ(1lu, other.min_open_segment_epoch());
    info.set_min_open_segment_id(9);
    info.set_min_open_segment_epoch(0);
    sl->setMasterRecoveryInfo(masterServerId, info);
    other = (*sl)[masterServerId].masterRecoveryInfo;
    EXPECT_EQ(9lu, other.min_open_segment_id());
    EXPECT_EQ(0lu, other.min_open_segment_epoch());
}

TEST_F(CoordinatorServerListTest, setMasterRecoveryInfo_execute) {
    enlistMaster();
    TestLog::Enable _;
    ProtoBuf::MasterRecoveryInfo info;
    info.set_min_open_segment_id(10);
    info.set_min_open_segment_epoch(1);
    sl->setMasterRecoveryInfo(masterServerId, info);

    vector<Entry> entriesRead = logCabinLog->read(0);
    string searchString = "execute: LogCabin: ServerUpdate entryId: ";
    ASSERT_NO_THROW(findEntryId(searchString));
    ProtoBuf::ServerUpdate readUpdate;
    logCabinHelper->parseProtoBufFromEntry(
                entriesRead[findEntryId(searchString)], readUpdate);

    EXPECT_EQ(10u, readUpdate.master_recovery_info().min_open_segment_id());
    EXPECT_EQ(1u, readUpdate.master_recovery_info().min_open_segment_epoch());
}

TEST_F(CoordinatorServerListTest, setMasterRecoveryInfo_complete_noSuchServer) {
    ProtoBuf::MasterRecoveryInfo info;
    info.set_min_open_segment_id(10);
    info.set_min_open_segment_epoch(1);
    EXPECT_FALSE(sl->setMasterRecoveryInfo({2, 2}, info));
}

//////////////////////////////////////////////////////////////////////
// Unit Tests for CoordinatorServerList Recovery Methods
//////////////////////////////////////////////////////////////////////

TEST_F(CoordinatorServerListTest, recoverServerCrashed) {
    enlistMaster();
    service->context->recoveryManager->doNotStartRecoveries = true;

    ramcloud->createTable("foo");
    service->forceServerDownForTesting = true;
    TestLog::Enable _(startMasterRecoveryFilter);

    ProtoBuf::ServerCrashInfo state;
    state.set_entry_type("ServerCrashed");
    state.set_server_id(masterServerId.getId());
    state.set_update_version(sl->version + 1);

    EntryId entryId = logCabinHelper->appendProtoBuf(
            *service->context->expectedEntryId, state);

    sl->recoverServerCrashed(&state, entryId);

    EXPECT_EQ("", TestLog::get());
    EXPECT_EQ(ServerStatus::CRASHED, (*sl)[master->serverId].status);
    EXPECT_EQ(2UL, sl->version);
}

TEST_F(CoordinatorServerListTest, recoverServerNeedsRecovery) {
    enlistMaster();
    service->context->recoveryManager->doNotStartRecoveries = true;

    ramcloud->createTable("foo");
    service->forceServerDownForTesting = true;
    TestLog::Enable _(startMasterRecoveryFilter);

    ProtoBuf::ServerCrashInfo state1;
    state1.set_entry_type("ServerNeedsRecovery");
    state1.set_server_id(masterServerId.getId());
    EntryId entryId1 = logCabinHelper->appendProtoBuf(
            *service->context->expectedEntryId, state1);

    ProtoBuf::ServerCrashInfo state2;
    state2.set_entry_type("ServerCrashed");
    state2.set_server_id(masterServerId.getId());
    state2.set_update_version(sl->version + 1);
    EntryId entryId2 = logCabinHelper->appendProtoBuf(
            *service->context->expectedEntryId, state2);

    sl->recoverServerNeedsRecovery(&state1, entryId1);
    sl->recoverServerCrashed(&state2, entryId2);

    EXPECT_EQ("startMasterRecovery: Scheduling recovery of master 1.0 | "
              "startMasterRecovery: Recovery crashedServerId: 1.0",
               TestLog::get());
    EXPECT_EQ(ServerStatus::CRASHED, (*sl)[master->serverId].status);
}

TEST_F(CoordinatorServerListTest, recoverServerRemoveUpdate) {
    enlistMaster();
    service->context->recoveryManager->doNotStartRecoveries = true;

    ramcloud->createTable("foo");
    service->forceServerDownForTesting = true;

    // For this unit test we're skipping the recoverServerCrashed.

    ProtoBuf::ServerCrashInfo state;
    state.set_entry_type("ServerRemoveUpdate");
    state.set_server_id(masterServerId.getId());
    state.set_update_version(sl->version + 1);

    EntryId entryId = logCabinHelper->appendProtoBuf(
            *service->context->expectedEntryId, state);

    sl->recoverServerRemoveUpdate(&state, entryId);

    EXPECT_EQ(ServerStatus::REMOVE, (*sl)[master->serverId].status);
    EXPECT_EQ(2UL, sl->version);
}

TEST_F(CoordinatorServerListTest, recoverServerUp) {
    enlistMaster();
    EXPECT_EQ(1U, master->serverId.getId());

    ProtoBuf::ServerInformation stateServerUp;
    stateServerUp.set_entry_type("ServerUp");
    stateServerUp.set_server_id(ServerId(2, 0).getId());
    stateServerUp.set_service_mask(
            ServiceMask({WireFormat::BACKUP_SERVICE}).serialize());
    stateServerUp.set_read_speed(0);
    stateServerUp.set_service_locator("mock:host=backup");
    stateServerUp.set_update_version(sl->version + 1);

    EntryId entryId = logCabinHelper->appendProtoBuf(
            *service->context->expectedEntryId, stateServerUp);

    TestLog::Enable _(enlistServerFilter);
    sl->recoverServerUp(&stateServerUp, entryId);

    ProtoBuf::ServerList masterList;
    sl->serialize(masterList, {WireFormat::MASTER_SERVICE});
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
                "server { services: 25 server_id: 1 "
                "service_locator: \"mock:host=master\" "
                "expected_read_mbytes_per_sec: [0-9]\\+ status: 0 "
                "replication_id: 0 } "
                "version_number: 1",
                 masterList.ShortDebugString()));

    ProtoBuf::ServerList backupList;
    sl->serialize(backupList, {WireFormat::BACKUP_SERVICE});
    EXPECT_EQ("server { services: 2 server_id: 2 "
              "service_locator: \"mock:host=backup\" "
              "expected_read_mbytes_per_sec: 0 status: 0 "
              "replication_id: 0 } "
              "version_number: 1 type: FULL_LIST",
               backupList.ShortDebugString());
}

TEST_F(CoordinatorServerListTest, recoverServerUpdate) {
    enlistMaster();
    ProtoBuf::ServerUpdate serverUpdate;
    serverUpdate.set_entry_type("ServerUpdate");
    serverUpdate.set_server_id(masterServerId.getId());
    serverUpdate.mutable_master_recovery_info()->set_min_open_segment_id(10);
    serverUpdate.mutable_master_recovery_info()->set_min_open_segment_epoch(1);
    EntryId entryId = logCabinHelper->appendProtoBuf(
            *service->context->expectedEntryId, serverUpdate);

    sl->recoverServerUpdate(&serverUpdate, entryId);

    EXPECT_EQ(10lu,
            (*sl)[masterServerId].masterRecoveryInfo.min_open_segment_id());
    EXPECT_EQ(1lu,
            (*sl)[masterServerId].masterRecoveryInfo.min_open_segment_epoch());
}

TEST_F(CoordinatorServerListTest, recoverServerUpdateReplicationId) {
    enlistMaster();
    ProtoBuf::ServerUpdate serverUpdate;
    serverUpdate.set_entry_type("ServerUpdateReplicationId");
    serverUpdate.set_server_id(masterServerId.getId());
    serverUpdate.set_replication_id(10lu);
    EntryId entryId = logCabinHelper->appendProtoBuf(
            *service->context->expectedEntryId, serverUpdate);
    sl->logIdServerReplicationUpdate = entryId;

    sl->recoverServerUpdate(&serverUpdate, entryId);
    CoordinatorServerList::Entry* entry = sl->getEntry(masterServerId);
    EXPECT_EQ(10lu, entry->replicationId);
}

TEST_F(CoordinatorServerListTest,
        recoverServerUpUpdateAndServerUpAndReplicationUpdate) {
    enlistMaster();
    EXPECT_EQ(1U, master->serverId.getId());

    ProtoBuf::EntryType stateServerUpUpdate;
    stateServerUpUpdate.set_entry_type("ServerUpUpdate");
    EntryId entryId = logCabinHelper->appendProtoBuf(
            *service->context->expectedEntryId, stateServerUpUpdate);

    sl->recoverServerUpUpdate(&stateServerUpUpdate, entryId);

    ProtoBuf::ServerInformation stateServerUp;
    stateServerUp.set_entry_type("ServerUp");
    stateServerUp.set_server_id(ServerId(2, 0).getId());
    stateServerUp.set_service_mask(
            ServiceMask({WireFormat::BACKUP_SERVICE}).serialize());
    stateServerUp.set_read_speed(0);
    stateServerUp.set_service_locator("mock:host=backup");
    stateServerUp.set_update_version(sl->version + 1);

    entryId = logCabinHelper->appendProtoBuf(
            *service->context->expectedEntryId, stateServerUp);

    TestLog::Enable _(enlistServerFilter);
    sl->recoverServerUp(&stateServerUp, entryId);

    ProtoBuf::ServerList masterList;
    sl->serialize(masterList, {WireFormat::MASTER_SERVICE});
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
                "server { services: 25 server_id: 1 "
                "service_locator: \"mock:host=master\" "
                "expected_read_mbytes_per_sec: [0-9]\\+ status: 0 "
                "replication_id: 0 } "
                "version_number: 2",
                 masterList.ShortDebugString()));

    ProtoBuf::ServerList backupList;
    sl->serialize(backupList, {WireFormat::BACKUP_SERVICE});
    EXPECT_EQ("server { services: 2 server_id: 2 "
              "service_locator: \"mock:host=backup\" "
              "expected_read_mbytes_per_sec: 0 status: 0 "
              "replication_id: 0 } "
              "version_number: 2 type: FULL_LIST",
               backupList.ShortDebugString());

    EXPECT_EQ(NO_ID, sl->logIdServerReplicationUpdate);

    ProtoBuf::EntryType stateServerReplication;
    stateServerReplication.set_entry_type("ServerReplicationUpdate");
    entryId = logCabinHelper->appendProtoBuf(
            *service->context->expectedEntryId, stateServerReplication);

    sl->recoverServerReplicationUpdate(&stateServerReplication, entryId);
    EXPECT_EQ(entryId, sl->logIdServerReplicationUpdate);
}

//////////////////////////////////////////////////////////////////////
// Unit Tests for CoordinatorServerList Private Methods
//////////////////////////////////////////////////////////////////////

TEST_F(CoordinatorServerListTest, getEntry) {
    sl->serverList.resize(6);

    add(ServerId(5, 2), "mock:id=5", {WireFormat::MASTER_SERVICE}, 100);
    EXPECT_TRUE(sl->getEntry({20, 0}) == NULL);
    EXPECT_TRUE(sl->getEntry({2, 0}) == NULL);
    EXPECT_TRUE(sl->getEntry({5, 1}) == NULL);
    EXPECT_TRUE(sl->getEntry({5, 2}) != NULL);
    EXPECT_EQ("mock:id=5", sl->getEntry({5, 2})->serviceLocator);
}


TEST_F(CoordinatorServerListTest, add) {
    sl->haltUpdater();
    EXPECT_EQ(0U, sl->serverList.size());
    EXPECT_EQ(0U, sl->numberOfMasters);
    EXPECT_EQ(0U, sl->numberOfBackups);

    {
        EXPECT_EQ(0U, sl->version);

        add(ServerId(1, 0), "mock:host=server1",
                {WireFormat::MASTER_SERVICE}, 100);
        EXPECT_EQ(1U, sl->version);
        EXPECT_TRUE(sl->serverList[1].entry);
        EXPECT_FALSE(sl->serverList[0].entry);
        EXPECT_EQ(1U, sl->numberOfMasters);
        EXPECT_EQ(0U, sl->numberOfBackups);
        EXPECT_EQ(ServerId(1, 0), sl->serverList[1].entry->serverId);
        EXPECT_EQ("mock:host=server1", sl->serverList[1].entry->serviceLocator);
        EXPECT_TRUE(sl->serverList[1].entry->isMaster());
        EXPECT_FALSE(sl->serverList[1].entry->isBackup());
        EXPECT_EQ(0u, sl->serverList[1].entry->expectedReadMBytesPerSec);
        EXPECT_EQ(1U, sl->serverList[1].nextGenerationNumber);
        ProtoBuf::ServerList update = sl->updates[0].incremental;
        EXPECT_EQ(1U, sl->version);
        EXPECT_EQ(1U, update.version_number());
        EXPECT_EQ(1, update.server_size());
        EXPECT_TRUE(protoBufMatchesEntry(update.server(0),
                *sl->serverList[1].entry, ServerStatus::UP));
    }

    {
        EXPECT_EQ(1U, sl->version);

        add(ServerId(2, 0), "hi again", {
            WireFormat::BACKUP_SERVICE}, 100);
        EXPECT_EQ(2U, sl->version);
        EXPECT_TRUE(sl->serverList[2].entry);
        EXPECT_EQ(ServerId(2, 0), sl->serverList[2].entry->serverId);
        EXPECT_EQ("hi again", sl->serverList[2].entry->serviceLocator);
        EXPECT_FALSE(sl->serverList[2].entry->isMaster());
        EXPECT_TRUE(sl->serverList[2].entry->isBackup());
        EXPECT_EQ(100u, sl->serverList[2].entry->expectedReadMBytesPerSec);
        EXPECT_EQ(1U, sl->serverList[2].nextGenerationNumber);
        EXPECT_EQ(1U, sl->numberOfMasters);
        EXPECT_EQ(1U, sl->numberOfBackups);
        ProtoBuf::ServerList update = sl->updates[1].incremental;
        EXPECT_EQ(2U, sl->version);
        EXPECT_EQ(2U, update.version_number());
        EXPECT_TRUE(protoBufMatchesEntry(update.server(0),
                *sl->serverList[2].entry, ServerStatus::UP));
    }
}

TEST_F(CoordinatorServerListTest, add_trackerUpdated) {
    TestLog::Enable _(fireCallbackFilter);
    ServerId serverId = generateUniqueId();
    add(serverId, "hi!", {WireFormat::MASTER_SERVICE}, 100);
    EXPECT_EQ("fireCallback: called", TestLog::get());
    ASSERT_FALSE(tr->changes.empty());
    auto& server = tr->changes.front().server;
    EXPECT_EQ(ServerId(1, 0), server.serverId);
    EXPECT_EQ("hi!", server.serviceLocator);
    EXPECT_EQ("MASTER_SERVICE", server.services.toString());
    // Not set when no BACKUP_SERVICE.
    EXPECT_EQ(0u, server.expectedReadMBytesPerSec);
    EXPECT_EQ(ServerStatus::UP, server.status);
    EXPECT_EQ(SERVER_ADDED, tr->changes.front().event);
}

TEST_F(CoordinatorServerListTest, crashed) {
    uint64_t orig_version = sl->version;
    EXPECT_THROW(crashed(ServerId(0, 0)), Exception);
    // Ensure no update was generated
    EXPECT_EQ(orig_version, sl->version);

    ServerId serverId = generateUniqueId();
    add(serverId, "hi!", {WireFormat::MASTER_SERVICE}, 100);

    CoordinatorServerList::Entry entryCopy = (*sl)[ServerId(1, 0)];
    EXPECT_NO_THROW(crashed(ServerId(1, 0)));
    ASSERT_TRUE(sl->serverList[1].entry);
    EXPECT_EQ(ServerStatus::CRASHED, sl->serverList[1].entry->status);
    EXPECT_TRUE(protoBufMatchesEntry(sl->updates[1].incremental.server(0),
                entryCopy, ServerStatus::CRASHED));

    // Already crashed; a no-op.
    crashed(ServerId(1, 0));
    EXPECT_EQ(0U, sl->numberOfMasters);
    EXPECT_EQ(0U, sl->numberOfBackups);
}

TEST_F(CoordinatorServerListTest, crashed_trackerUpdated) {
    TestLog::Enable _(fireCallbackFilter);
    ServerId serverId = generateUniqueId();
    add(serverId, "hi!", {WireFormat::MASTER_SERVICE}, 100);
    crashed(serverId);
    EXPECT_EQ("fireCallback: called | fireCallback: called", TestLog::get());
    ASSERT_FALSE(tr->changes.empty());
    tr->changes.pop();
    ASSERT_FALSE(tr->changes.empty());
    auto& server = tr->changes.front().server;
    EXPECT_EQ(serverId, server.serverId);
    EXPECT_EQ("hi!", server.serviceLocator);
    EXPECT_EQ("MASTER_SERVICE", server.services.toString());
    // Not set when no BACKUP_SERVICE.
    EXPECT_EQ(0u, server.expectedReadMBytesPerSec);
    EXPECT_EQ(ServerStatus::CRASHED, server.status);
    EXPECT_EQ(SERVER_CRASHED, tr->changes.front().event);
}

TEST_F(CoordinatorServerListTest, firstFreeIndex) {
    EXPECT_EQ(0U, sl->serverList.size());
    EXPECT_EQ(1U, sl->firstFreeIndex());
    EXPECT_EQ(2U, sl->serverList.size());
    ServerId serverId1 = generateUniqueId();
    add(serverId1, "hi", {WireFormat::MASTER_SERVICE}, 100);
    EXPECT_EQ(2U, sl->firstFreeIndex());
    ServerId serverId2 = generateUniqueId();
    add(serverId2, "hi again", {WireFormat::MASTER_SERVICE}, 100);
    EXPECT_EQ(3U, sl->firstFreeIndex());
    remove(ServerId(2, 0));
    EXPECT_EQ(2U, sl->firstFreeIndex());
    remove(ServerId(1, 0));
    EXPECT_EQ(1U, sl->firstFreeIndex());
}

TEST_F(CoordinatorServerListTest, generateUniqueId) {
    EXPECT_EQ(ServerId(1, 0), generateUniqueId());
    EXPECT_EQ(ServerId(2, 0), generateUniqueId());

    remove(ServerId(1, 0));
    EXPECT_EQ(ServerId(1, 1), generateUniqueId());
}

TEST_F(CoordinatorServerListTest, generateUniqueId_real) {
    ServerId serverId1 = generateUniqueId();
    EXPECT_EQ(ServerId(1, 0), serverId1);
    add(serverId1, "mock:host=server1", {}, 0, false);

    ServerId serverId2 = generateUniqueId();
    EXPECT_EQ(ServerId(2, 0), serverId2);
    add(serverId2, "mock:host=server2", {}, 0, false);

    sl->serverCrashed(serverId1);
    sl->sync();

    EXPECT_EQ(ServerId(1, 1), generateUniqueId());
    EXPECT_EQ(ServerId(3, 0), generateUniqueId());
}

TEST_F(CoordinatorServerListTest, serverIdIndexOperator) {
    EXPECT_THROW((*sl)[ServerId(0, 0)], Exception);
    EXPECT_THROW((*sl)[ServerId(1, 0)], Exception);
    ServerId serverId = generateUniqueId();
    add(serverId, "yo!", {WireFormat::MASTER_SERVICE}, 100);
    EXPECT_THROW((*sl)[ServerId(0, 0)], Exception);
    EXPECT_NO_THROW((*sl)[ServerId(1, 0)]);
    EXPECT_EQ(ServerId(1, 0), (*sl)[ServerId(1, 0)].serverId);
    EXPECT_EQ("yo!", (*sl)[ServerId(1, 0)].serviceLocator);
    crashed(ServerId(1, 0));
    remove(ServerId(1, 0));
    EXPECT_THROW((*sl)[ServerId(1, 0)], Exception);
    EXPECT_THROW((*sl)[ServerId(1, 1)], Exception);
    EXPECT_THROW((*sl)[ServerId(2, 0)], Exception);
}

TEST_F(CoordinatorServerListTest, recoveryCompleted) {
    uint64_t orig_version = sl->version;
    EXPECT_THROW(sl->serverCrashed(ServerId(0, 0)), Exception);
    EXPECT_EQ(orig_version, sl->version);

    ServerId serverId1 = generateUniqueId();
    ASSERT_EQ(ServerId(1, 0), serverId1);
    add(ServerId(1, 0), "hi!", {}, 100);
    CoordinatorServerList::Entry entryCopy = (*sl)[ServerId(1, 0)];
    EXPECT_EQ(1, sl->updates[0].incremental.server_size());
    sl->sync();

    EXPECT_NO_THROW(sl->serverCrashed(ServerId(1, 0)));
    // Critical that an UP server gets both crashed and down events.
    EXPECT_TRUE(protoBufMatchesEntry(sl->updates[0].incremental.server(0),
            entryCopy, ServerStatus::CRASHED));
    EXPECT_TRUE(protoBufMatchesEntry(sl->updates[1].incremental.server(0),
            entryCopy, ServerStatus::REMOVE));
    sl->sync();
    EXPECT_FALSE(sl->serverList[1].entry);

    orig_version = sl->version;
    EXPECT_THROW(sl->serverCrashed(ServerId(1, 0)), Exception);
    sl->sync();
    EXPECT_EQ(orig_version, sl->version);
    EXPECT_EQ(0U, sl->numberOfMasters);
    EXPECT_EQ(0U, sl->numberOfBackups);

    ServerId serverId2 = generateUniqueId();
    add(serverId2, "hi, again", {WireFormat::BACKUP_SERVICE}, 100);
    EXPECT_EQ(uint32_t(ServerStatus::UP),
              sl->updates[0].incremental.server(0).status());
    sl->sync();

    crashed(ServerId(1, 1));
    EXPECT_EQ(uint32_t(ServerStatus::CRASHED),
              sl->updates[0].incremental.server(0).status());
    sl->sync();
    EXPECT_TRUE(sl->serverList[1].entry);

    EXPECT_THROW(sl->serverCrashed(ServerId(1, 2)), Exception);
    EXPECT_NO_THROW(sl->serverCrashed(ServerId(1, 1)));
    EXPECT_EQ(uint32_t(ServerStatus::REMOVE),
              sl->updates[0].incremental.server(0).status());
    sl->sync();
    EXPECT_EQ(0U, sl->numberOfMasters);
    EXPECT_EQ(0U, sl->numberOfBackups);
}

TEST_F(CoordinatorServerListTest, recoveryCompleted_trackerUpdated) {
    TestLog::Enable _(fireCallbackFilter);
    ServerId serverId = generateUniqueId();
    add(serverId, "hi!", {WireFormat::BACKUP_SERVICE}, 100);
    sl->serverCrashed(serverId);
    EXPECT_EQ("fireCallback: called | fireCallback: called | "
              "fireCallback: called", TestLog::get());
    ASSERT_FALSE(tr->changes.empty());
    tr->changes.pop();
    ASSERT_FALSE(tr->changes.empty());
    tr->changes.pop();
    ASSERT_FALSE(tr->changes.empty());
    auto& server = tr->changes.front().server;
    EXPECT_EQ(serverId, server.serverId);
    EXPECT_EQ("hi!", server.serviceLocator);
    EXPECT_EQ("BACKUP_SERVICE", server.services.toString());
    EXPECT_EQ(ServerStatus::REMOVE, server.status);
    EXPECT_EQ(SERVER_REMOVED, tr->changes.front().event);
}

TEST_F(CoordinatorServerListTest, assignReplicationGroup) {
    vector<ServerId> serverIds;
    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::BACKUP_SERVICE,
        WireFormat::MEMBERSHIP_SERVICE, WireFormat::PING_SERVICE};
    for (uint32_t i = 0; i < 3; i++) {
        config.localLocator = format("mock:host=backup%u", i);
        serverIds.push_back(cluster.addServer(config)->serverId);
    }

    // Check normal functionality.
    Lock lock(sl->mutex);
    EXPECT_TRUE(sl->assignReplicationGroup(lock, 10U, serverIds));
    lock.unlock();
    EXPECT_EQ(10U, (*sl)[serverIds[0]].replicationId);
    EXPECT_EQ(10U, (*sl)[serverIds[1]].replicationId);
    EXPECT_EQ(10U, (*sl)[serverIds[2]].replicationId);

    service->forceServerDownForTesting = false;
}

TEST_F(CoordinatorServerListTest, createReplicationGroup) {
    ServerId serverIds[10];
    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::BACKUP_SERVICE,
        WireFormat::MEMBERSHIP_SERVICE, WireFormat::PING_SERVICE};
    for (uint32_t i = 0; i < 8; i++) {
        config.localLocator = format("mock:host=backup%u", i);
        serverIds[i] = cluster.addServer(config)->serverId;
    }

    EXPECT_EQ(1U, (*sl)[serverIds[0]].replicationId);
    EXPECT_EQ(1U, (*sl)[serverIds[1]].replicationId);
    EXPECT_EQ(1U, (*sl)[serverIds[2]].replicationId);
    EXPECT_EQ(2U, (*sl)[serverIds[3]].replicationId);
    EXPECT_EQ(2U, (*sl)[serverIds[4]].replicationId);
    EXPECT_EQ(2U, (*sl)[serverIds[5]].replicationId);
    EXPECT_EQ(0U, (*sl)[serverIds[6]].replicationId);
    EXPECT_EQ(0U, (*sl)[serverIds[7]].replicationId);
    // Kill server 7.
    service->forceServerDownForTesting = true;
    sl->serverCrashed(serverIds[7]);
    sl->sync();
    service->forceServerDownForTesting = false;
    // Create a new server.
    config.localLocator = format("mock:host=backup%u", 8);
    serverIds[8] = cluster.addServer(config)->serverId;
    EXPECT_EQ(0U, (*sl)[serverIds[6]].replicationId);
    EXPECT_EQ(0U, (*sl)[serverIds[8]].replicationId);
    config.localLocator = format("mock:host=backup%u", 9);
    serverIds[9] = cluster.addServer(config)->serverId;
    EXPECT_EQ(3U, (*sl)[serverIds[6]].replicationId);
    EXPECT_EQ(3U, (*sl)[serverIds[9]].replicationId);
}

TEST_F(CoordinatorServerListTest, removeReplicationGroup) {
    enlistMaster();

    ServerId serverIds[4];
    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::BACKUP_SERVICE,
        WireFormat::MEMBERSHIP_SERVICE, WireFormat::PING_SERVICE};
    for (uint32_t i = 0; i < 3; i++) {
        config.localLocator = format("mock:host=backup%u", i);
        serverIds[i] = cluster.addServer(config)->serverId;
    }
    EXPECT_EQ(1U, (*sl)[serverIds[1]].replicationId);
    service->forceServerDownForTesting = true;
    sl->serverCrashed(serverIds[1]);
    sl->sync();
    service->forceServerDownForTesting = false;
    EXPECT_EQ(0U, (*sl)[serverIds[0]].replicationId);
    EXPECT_EQ(0U, (*sl)[serverIds[2]].replicationId);
    config.localLocator = format("mock:host=backup%u", 3);
    serverIds[3] = cluster.addServer(config)->serverId;
    EXPECT_EQ(2U, (*sl)[serverIds[2]].replicationId);
    Lock lock(sl->mutex);
    sl->removeReplicationGroup(lock, 2);
    lock.unlock();
    EXPECT_EQ(0U, (*sl)[serverIds[0]].replicationId);
    EXPECT_EQ(0U, (*sl)[serverIds[2]].replicationId);
    EXPECT_EQ(0U, (*sl)[serverIds[3]].replicationId);
}

////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////
////  Tests for Updater Mechanism
////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////

TEST_F(CoordinatorServerListTest, pushUpdate) {
    ProtoBuf::ServerList& update = sl->update;
    uint64_t orig_ver = sl->version;

    // Empty update, not committed
    pushUpdate(sl->version);
    EXPECT_EQ(orig_ver, sl->version);
    EXPECT_TRUE(sl->updates.empty());

    // Update with at least something in it
    update.add_server();
    sl->version++;
    pushUpdate(sl->version);
    EXPECT_EQ(orig_ver + 1, sl->version);
    EXPECT_EQ(1UL, sl->updates.size());
    EXPECT_EQ(orig_ver + 1, sl->updates.front().version);

    // Add a second one in
    update.add_server();
    sl->version++;
    pushUpdate(sl->version);
    EXPECT_EQ(2UL, sl->updates.size());
    EXPECT_EQ(orig_ver + 1, sl->updates.front().version);
    EXPECT_EQ(orig_ver + 2, sl->updates.back().version);
}

TEST_F(CoordinatorServerListTest, haltUpdater) {
    sl->startUpdater();
    EXPECT_FALSE(sl->stopUpdater);
    EXPECT_TRUE(sl->updaterThread);
    EXPECT_TRUE(sl->updaterThread->joinable());

    sl->haltUpdater();
    EXPECT_FALSE(sl->updaterThread);
    EXPECT_TRUE(sl->stopUpdater);
}

TEST_F(CoordinatorServerListTest, startUpdater) {
    sl->haltUpdater();
    EXPECT_TRUE(sl->stopUpdater);
    EXPECT_FALSE(sl->updaterThread);

    sl->startUpdater();
    EXPECT_FALSE(sl->stopUpdater);
    EXPECT_TRUE(sl->updaterThread);
    EXPECT_TRUE(sl->updaterThread->joinable());
}

TEST_F(CoordinatorServerListTest, isClusterUpToDate) {
    CoordinatorServerList::UpdaterWorkUnit wu;
    sl->haltUpdater();

    // Empty cluster, should be up to date.
    ASSERT_EQ(0UL, sl->serverList.size());
    EXPECT_TRUE(isClusterUpToDate());

    // Add ineligible server
    ServerId id = generateUniqueId();
    add(id, "mock:host=server2", {}, 100);
    EXPECT_FALSE(sl->getWork(&wu));
    EXPECT_TRUE(isClusterUpToDate());

    // Add a server w/ commit
    id = generateUniqueId();
    add(id, "mock:host=server1", {WireFormat::MEMBERSHIP_SERVICE}, 100);
    EXPECT_FALSE(isClusterUpToDate());

    // Fake an update
    ASSERT_TRUE(sl->getWork(&wu));
    sl->workSuccess(wu.targetServer);
    ASSERT_FALSE(sl->getWork(&wu));
    ASSERT_FALSE(sl->getWork(&wu));
    EXPECT_TRUE(isClusterUpToDate());

    // Note: the second getWork() is needed because the conditions to
    // isClusterUpToDate are updated lazily and the second getWork()
    // forces a full scan of the server list and update the conditions.
}

TEST_F(CoordinatorServerListTest, pruneUpdates) {
    Lock lock(sl->mutex); // Global lock on csl.
    for (int i = 1; i <= 10; i++) {
        for (int j = 1; j <= i; j++) {
            ServerId id = sl->generateUniqueId(lock);
            sl->add(lock, id, "mock:host=server",
                    {WireFormat::MEMBERSHIP_SERVICE}, 100);
        }
        sl->version++;
        sl->pushUpdate(lock, sl->version);
    }

    EXPECT_EQ(10UL, sl->version);
    EXPECT_EQ(10UL, sl->updates.size());

    // Out of Bounds Test
    TestLog::Enable _;
    sl->minConfirmedVersion = 1000;
    sl->pruneUpdates(lock);
    EXPECT_EQ(0UL, sl->minConfirmedVersion);
    EXPECT_EQ(10UL, sl->updates.size());
    EXPECT_STREQ("pruneUpdates: Inconsistent state detected! "
            "CoordinatorServerList's minConfirmedVersion 1000 is larger "
            "than it's current version 10. This should NEVER happen!",
            TestLog::get().c_str());

    // Nothing should be pruned and without errors.
    TestLog::reset();
    sl->minConfirmedVersion = sl->UNINITIALIZED_VERSION;
    sl->pruneUpdates(lock);
    EXPECT_EQ(10UL, sl->version);
    EXPECT_EQ(10UL, sl->updates.size());

    // Normal Prune
    sl->minConfirmedVersion = 4;
    sl->pruneUpdates(lock);
    EXPECT_EQ(10UL, sl->version);
    EXPECT_EQ(6UL, sl->updates.size());

    for (int i = 5; i <= 10; i++) {
        ProtoBuf::ServerList& update = sl->updates.at(i - 5).incremental;
        EXPECT_EQ(i, static_cast<int>(update.version_number()));
        EXPECT_EQ(i, update.server_size());
    }

    // No-op
    sl->minConfirmedVersion = 2;
    sl->pruneUpdates(lock);
    EXPECT_EQ(6UL, sl->updates.size());

    // No errors should have occurred since the last reset.
    EXPECT_STREQ("", TestLog::get().c_str());
}

TEST_F(CoordinatorServerListTest, sync) {
    // Test that sync wakes up thread and flushes all updates
    MockTransport transport(service->context);
    TransportManager::MockRegistrar _(service->context, transport);
    sl->haltUpdater();

    ServerId id = generateUniqueId();
    add(id, "mock:host=server", {WireFormat::MEMBERSHIP_SERVICE}, 100);
    transport.setInput("0");

    sl->sync();
    EXPECT_EQ("sendRequest: 0x40023 1 0 11 273 0 /0 /x18/0",
            transport.outputLog);
    transport.clearOutput();

    // Test that syncs on up-to-date list don't clog
    sl->sync();
    sl->sync();
    EXPECT_EQ("", transport.outputLog);
}

TEST_F(CoordinatorServerListTest, updateLoop) {
    MockTransport transport(service->context);
    TransportManager::MockRegistrar _(service->context, transport);

    // Test unoccupied server slot. Remove must wait until after last add to
    // ensure slot isn't recycled.
    ServerId serverId1 = generateUniqueId();
    add(serverId1, "mock:host=server1", {WireFormat::MEMBERSHIP_SERVICE},
        0, false);

    // Test crashed server gets skipped as a recipient.
    ServerId serverId2 = generateUniqueId();
    add(serverId2, "mock:host=server2", {}, 0, false);
    crashed(serverId2, false);

    // Test server with no membership service.
    ServerId serverId3 = generateUniqueId();
    add(serverId3, "mock:host=server3", {}, 0, false);

    // Only Server that can receive updates
    ServerId serverId4 = generateUniqueId();
    add(serverId4, "mock:host=server4",
            {WireFormat::MEMBERSHIP_SERVICE}, 0, false);

    crashed(serverId1, false);
    sl->recoveryCompleted(serverId1);

    TestLog::Enable __(statusFilter);

    transport.setInput("0"); // Server 4 response -> ok

    // Send Full List to server 4
    sl->sync();
    EXPECT_TRUE(sl->updates.empty());
    EXPECT_EQ("workSuccess: ServerList Update Success: 4.0 update (0 => 1) | "
              "execute: LogCabin: ServerListVersion entryId: 1",
               TestLog::get());
    EXPECT_EQ("sendRequest: 0x40023 4 0 11 273 0 /0 /x18/0",
               transport.outputLog);

    TestLog::reset();
    transport.outputLog = "";

    transport.setInput("0"); // Server 5 full list received
    transport.setInput("0"); // Server 4 update received
    transport.setInput("0"); // Server 4 update received
    transport.setInput("0"); // Server 4 update received

    // Add two more servers eligible for updates and crash one
    ServerId serverId5 = generateUniqueId();
    add(serverId5, "mock:host=server5",
            {WireFormat::MEMBERSHIP_SERVICE}, 0, false);
    ServerId serverId6 = generateUniqueId();
    add(serverId6, "mock:host=server6",
            {WireFormat::MEMBERSHIP_SERVICE}, 0, false);
    crashed(serverId6, false);

    TestLog::reset();
    sl->version++;
    pushUpdate(sl->version);
    sl->sync();
    EXPECT_TRUE(sl->updates.empty());
    EXPECT_EQ("workSuccess: ServerList Update Success: 4.0 update (1 => 2) | "
              "workSuccess: ServerList Update Success: 1.1 update (0 => 2) | "
              "execute: LogCabin: ServerListVersion entryId: 7",
              TestLog::get());
}

TEST_F(CoordinatorServerListTest, updateLoop_SlowStartWithBatch) {
    MockTransport transport(service->context);
    TransportManager::MockRegistrar _(service->context, transport);
    sl->haltUpdater();

    for (int i = 1; i <= 5; i++) {
        transport.setInput("0");    // One for full list slow start (0 => 1)
        transport.setInput("0");    // One for batched updates      (1 => 4)
        ServerId id = generateUniqueId();
        add(id, "mock:host=server_xxx", {WireFormat::MEMBERSHIP_SERVICE}, 100);
    }

    TestLog::Enable __;
    sl->sync();

    EXPECT_EQ("workSuccess: ServerList Update Success: 1.0 update (0 => 1) | "
            "workSuccess: ServerList Update Success: 1.0 update (1 => 5) | "
            "workSuccess: ServerList Update Success: 2.0 update (0 => 1) | "
            "workSuccess: ServerList Update Success: 2.0 update (1 => 5) | "
            "workSuccess: ServerList Update Success: 3.0 update (0 => 1) | "
            "workSuccess: ServerList Update Success: 3.0 update (1 => 5) | "
            "workSuccess: ServerList Update Success: 4.0 update (0 => 1) | "
            "workSuccess: ServerList Update Success: 4.0 update (1 => 5) | "
            "workSuccess: ServerList Update Success: 5.0 update (0 => 1) | "
            "workSuccess: ServerList Update Success: 5.0 update (1 => 5) | "
            "execute: LogCabin: ServerListVersion entryId: 0",
               TestLog::get());
}

TEST_F(CoordinatorServerListTest, updateLoop_multiStriped) {
    MockTransport transport(service->context);
    TransportManager::MockRegistrar _(service->context, transport);
    sl->haltUpdater();

    TestLog::Enable __(workSuccessFilter);
    for (int i = 1; i <= 4; i++) {
        ServerId id = generateUniqueId();
        add(id, "mock:host=server_xxx", {WireFormat::MEMBERSHIP_SERVICE}, 100);

        // (every n-th server will cause n updates)
        for (int j = 0; j < i; j++)
            transport.setInput("0");

        sl->sync();

        // loop should prune updates
        EXPECT_GE(1LU, sl->updates.size());
    }

    EXPECT_EQ("workSuccess: ServerList Update Success: 1.0 update (0 => 1) | "
              "workSuccess: ServerList Update Success: 1.0 update (1 => 2) | "
              "workSuccess: ServerList Update Success: 2.0 update (0 => 2) | "
              "workSuccess: ServerList Update Success: 2.0 update (2 => 3) | "
              "workSuccess: ServerList Update Success: 3.0 update (0 => 3) | "
              "workSuccess: ServerList Update Success: 1.0 update (2 => 3) | "
              "workSuccess: ServerList Update Success: 1.0 update (3 => 4) | "
              "workSuccess: ServerList Update Success: 2.0 update (3 => 4) | "
              "workSuccess: ServerList Update Success: 3.0 update (3 => 4) | "
              "workSuccess: ServerList Update Success: 4.0 update (0 => 4)",
              TestLog::get());
}

// Tests disabled since the Updater in its current form
// does not use any globals to hint at any observable state changes.
// This will be fixed when ServerList is abstracted out into its own
// files and its state is exposed.
#if 0
TEST_F(CoordinatorServerListTest, updateLoop_expansion) {
    MockTransport transport(service->context);
    TransportManager::MockRegistrar _(service->context, transport);
    sl->haltUpdater();

    // Start with 0 and grow to 5; since mockTransport RPCs finish
    // instantaneously, we have to add all (5*4)/2 entries at once
    // But there will be 1 contraction in the final round,
    // causing the total to be 4
    sl->concurrentRPCs = 1;

    for (int i = 0; i < 10; i++) {
        ServerId id = generateUniqueId();
        add(id, "mock:host=server", {WireFormat::MEMBERSHIP_SERVICE}, 100);
        transport.setInput("0");
    }

    sl->sync();
    EXPECT_EQ(4UL, sl->concurrentRPCs);

    // Test that syncs w/o updates don't lower the slot count
    sl->sync();
    sl->sync();
    EXPECT_EQ(4UL, sl->concurrentRPCs);
}

TEST_F(CoordinatorServerListTest, updateLoop_contraction) {
    MockTransport transport(service->context);
    TransportManager::MockRegistrar _(service->context, transport);
    sl->haltUpdater();
    sl->concurrentRPCs = 5;

    // Single contraction since 2 < 5
    ServerId id = generateUniqueId();
    add(id, "mock:host=server", {WireFormat::MEMBERSHIP_SERVICE}, 100);
    id = generateUniqueId();
    add(id, "mock:host=server", {WireFormat::MEMBERSHIP_SERVICE}, 100);
    transport.setInput("0");
    transport.setInput("0");
    sl->sync();
    EXPECT_EQ(4UL, sl->concurrentRPCs);

    // 3 < 4 contraction
    id = generateUniqueId();
    add(id, "mock:host=server", {WireFormat::MEMBERSHIP_SERVICE}, 100);
    transport.setInput("0");
    transport.setInput("0");
    transport.setInput("0");
    sl->sync();
    EXPECT_EQ(3UL, sl->concurrentRPCs);

    // 4 updates, no change (1 expand + 1 contract)
    id = generateUniqueId();
    add(id, "mock:host=server", {WireFormat::MEMBERSHIP_SERVICE}, 100);
    transport.setInput("0");
    transport.setInput("0");
    transport.setInput("0");
    transport.setInput("0");
    sl->sync();
    EXPECT_EQ(3UL, sl->concurrentRPCs);

    // 5 + 7 = 13 updates (2 expand + 1 contract)
    id = generateUniqueId();
    add(id, "mock:host=server", {WireFormat::MEMBERSHIP_SERVICE}, 100, false);
    pushUpdate();
    id = generateUniqueId();
    add(id, "mock:host=server", {WireFormat::MEMBERSHIP_SERVICE}, 100, false);
    pushUpdate();
    for (int i = 0; i < 12; i++)
        transport.setInput("0");
    sl->sync();
    EXPECT_EQ(4UL, sl->concurrentRPCs);
}
#endif

/**
 * This test can't actually test for cases where waitForWork will stall
 * so only the 'there is work' cases are checked. If this test stalls,
 * then there's an error; check that the conditions here match the ones
 * inside waitForWork.
 */
TEST_F(CoordinatorServerListTest, waitForWork) {
    sl->stopUpdater = false;
    sl->minConfirmedVersion = 10;
    sl->waitForWork();

    sl->minConfirmedVersion = sl->version;
    sl->stopUpdater = true;
    sl->waitForWork();
}

TEST_F(CoordinatorServerListTest, getWork) {
    CoordinatorServerList::UpdaterWorkUnit wu;
    sl->haltUpdater();

    // Empty server list
    EXPECT_FALSE(sl->getWork(&wu));

    // Server ineligible for updates
    ServerId id = generateUniqueId();
    add(id, "mock:host=server1", {}, 0);
    EXPECT_FALSE(sl->getWork(&wu));
    // Should have set heuristic metrics and pruned updates
    EXPECT_EQ(sl->version, sl->minConfirmedVersion);
    EXPECT_EQ(sl->version, sl->lastScan.noWorkFoundForEpoch);
    EXPECT_EQ(0UL, sl->updates.size());
    EXPECT_EQ(0UL, sl->numUpdatingServers);

    // Updatable server
    id = generateUniqueId();
    add(id, "mock:host=server2", {WireFormat::MEMBERSHIP_SERVICE}, 0);
    EXPECT_TRUE(sl->getWork(&wu));
    EXPECT_EQ(id, wu.targetServer);
    EXPECT_TRUE(wu.sendFullList);
    EXPECT_EQ(sl->version, wu.firstUpdate->version);
    EXPECT_EQ(sl->version, wu.updateVersionTail);

    EXPECT_EQ(1UL, sl->numUpdatingServers);
    EXPECT_FALSE(sl->getWork(&wu)); // update already assigned

    // Work failed, retry
    sl->workFailed(id);
    EXPECT_EQ(0UL, sl->numUpdatingServers);
    EXPECT_TRUE(sl->getWork(&wu));
    EXPECT_EQ(id, wu.targetServer);
    EXPECT_TRUE(wu.sendFullList);
    EXPECT_EQ(2UL, wu.firstUpdate->version);
    EXPECT_FALSE(sl->getWork(&wu)); // update already assigned
    EXPECT_NE(sl->version, sl->minConfirmedVersion);
    EXPECT_EQ(1UL, sl->numUpdatingServers);

    // Work success, no more work and all updates pruned
    sl->workSuccess(id);
    EXPECT_FALSE(sl->getWork(&wu));
    EXPECT_EQ(sl->version, sl->minConfirmedVersion);
    EXPECT_EQ(0UL, sl->updates.size());
    EXPECT_EQ(0UL, sl->numUpdatingServers);
}

TEST_F(CoordinatorServerListTest, getWork_assignsFullListAfterFirstScan) {
    CoordinatorServerList::UpdaterWorkUnit wu;

    // Add server and update it, completing first scan.
    ServerId id = generateUniqueId();
    add(id, "mock:host=server1", {WireFormat::MEMBERSHIP_SERVICE}, 0);
    EXPECT_TRUE(sl->getWork(&wu));
    sl->workSuccess(id);
    EXPECT_FALSE(sl->getWork(&(wu)));

    ASSERT_LT(0UL, sl->lastScan.completeScansSinceStart);

    // Add second server and expect both to update to v2
    ServerId id2 = generateUniqueId();
    add(id2, "mock:host=server12", {WireFormat::MEMBERSHIP_SERVICE}, 0);
    EXPECT_TRUE(sl->getWork(&wu));
    EXPECT_EQ(2UL, wu.updateVersionTail);

    EXPECT_TRUE(sl->getWork(&wu));
    EXPECT_EQ(2UL, wu.updateVersionTail);

    EXPECT_FALSE(sl->getWork(&wu));
}

TEST_F(CoordinatorServerListTest, getWork_incrementalUpdate) {
    CoordinatorServerList::UpdaterWorkUnit wu, wu1, wu2;
    sl->haltUpdater();

    // Eligible for updates
    ServerId id1 = generateUniqueId();
    add(id1, "mock:host=server1", {WireFormat::MEMBERSHIP_SERVICE}, 0);

    EXPECT_TRUE(sl->getWork(&wu));
    EXPECT_FALSE(sl->getWork(&wu)); // update already assigned
    EXPECT_EQ(1UL, sl->numUpdatingServers);
    sl->workSuccess(id1);

    // Add second server; it gets full update and first server gets incremental.
    ServerId id2 = generateUniqueId();
    add(id2, "mock:host=server2", {WireFormat::MEMBERSHIP_SERVICE}, 0);

    EXPECT_TRUE(sl->getWork(&wu1));
    EXPECT_TRUE(sl->getWork(&wu2));
    EXPECT_EQ(2UL, sl->numUpdatingServers);
    EXPECT_FALSE(sl->getWork(&wu)); // work already assigned

    // Swap the two work units so that the checks are correct
    if (wu1.targetServer == id2) {
        wu = wu2;
        wu2 = wu1;
        wu1 = wu;
    }

    EXPECT_EQ(id1, wu1.targetServer);
    EXPECT_FALSE(wu1.sendFullList);
    EXPECT_EQ(sl->version, wu1.firstUpdate->version);
    EXPECT_EQ(sl->version, wu1.updateVersionTail);

    EXPECT_EQ(id2, wu2.targetServer);
    EXPECT_TRUE(wu2.sendFullList);
    EXPECT_EQ(sl->version, wu1.firstUpdate->version);
    EXPECT_EQ(sl->version, wu1.updateVersionTail);

    // Finish the requests
    EXPECT_EQ(2UL, sl->numUpdatingServers);
    sl->workSuccess(id1);
    sl->workSuccess(id2);

    // Crash & remove one and check that updates keep going
    crashed(id1);
    EXPECT_TRUE(sl->getWork(&wu2));
    EXPECT_EQ(id2, wu2.targetServer);
    EXPECT_EQ(sl->version, wu2.updateVersionTail);
    EXPECT_FALSE(wu2.sendFullList);
    EXPECT_FALSE(sl->getWork(&wu)); // work already assigned

    // Remove then finish work -> leaves work left over
    sl->recoveryCompleted(id1);
    sl->workSuccess(id2);

    EXPECT_TRUE(sl->getWork(&wu2));
    EXPECT_EQ(id2, wu2.targetServer);
    EXPECT_EQ(sl->version, wu2.updateVersionTail);
    EXPECT_FALSE(wu2.sendFullList);
    EXPECT_FALSE(sl->getWork(&wu)); // work already assigned

    // Check that all is well again
    sl->workSuccess(id2);
    EXPECT_FALSE(sl->getWork(&wu));
    EXPECT_EQ(sl->version, sl->minConfirmedVersion);
    EXPECT_EQ(0UL, sl->numUpdatingServers);
}

static const uint64_t UNINITIALIZED_VERSION =
        CoordinatorServerList::UNINITIALIZED_VERSION;

TEST_F(CoordinatorServerListTest, workSuccess) {
    CoordinatorServerList::UpdaterWorkUnit wu;
    sl->haltUpdater();

    ServerId id1 = generateUniqueId();
    add(id1, "mock:host=server1", {WireFormat::MEMBERSHIP_SERVICE}, 0);
    ServerId id2 = generateUniqueId();
    add(id2, "mock:host=server2", {WireFormat::MEMBERSHIP_SERVICE}, 0);

    TestLog::Enable _;
    ASSERT_TRUE(sl->getWork(&wu));
    ASSERT_TRUE(sl->getWork(&wu));

    TestLog::reset();
    sl->workSuccess(id1);
    EXPECT_STREQ("workSuccess: ServerList Update Success: 1.0 update (0 => 1)",
                TestLog::get().c_str());

    CoordinatorServerList::Entry* e1 = sl->getEntry(id1);
    CoordinatorServerList::Entry* e2 = sl->getEntry(id2);

    uint64_t firstVersion = sl->updates.front().version;
    EXPECT_EQ(firstVersion, e1->verifiedVersion);
    EXPECT_EQ(firstVersion, e1->updateVersion);
    EXPECT_EQ(UNINITIALIZED_VERSION, e2->verifiedVersion);
    EXPECT_EQ(firstVersion, e2->updateVersion);
}

TEST_F(CoordinatorServerListTest, workSuccess_multipleInvokes) {
    CoordinatorServerList::UpdaterWorkUnit wu;
    sl->haltUpdater();

    ServerId id1 = generateUniqueId();
    add(id1, "mock:host=server1", {WireFormat::MEMBERSHIP_SERVICE}, 0);
    ASSERT_TRUE(sl->getWork(&wu));

    TestLog::Enable _;
    sl->workSuccess(id1);
    EXPECT_STREQ("workSuccess: ServerList Update Success: 1.0 update (0 => 1)",
                 TestLog::get().c_str());

    // Second call triggers warning
    TestLog::reset();
    sl->numUpdatingServers++;
    sl->workSuccess(id1);
    EXPECT_STREQ("workSuccess: Invoked for server 1.0 even though either "
            "no update was sent out or it has already been invoked. "
            "Possible race/bookeeping issue.",
                TestLog::get().c_str());

    // Everything should be up to date from the POV of the CSL.
    EXPECT_FALSE(sl->getWork(&wu));
}

TEST_F(CoordinatorServerListTest, workSuccess_nonExistantServer) {
    sl->haltUpdater();
    TestLog::Enable _;

    // Test Non-existent ServerId
    ServerId id(100, 200);
    sl->workSuccess(id);
    EXPECT_STREQ("workSuccess: Bookeeping issue detected; server's count "
            "of numUpdatingServers just went negative. Not total failure "
            "but will cause the updater thread to spin even w/o work. "
            "Cause is mismatch # of getWork() and workSuccess/Failed() | "
            "workSuccess: Server 100.200 responded to a server list update "
            "but is no longer in the server list...",
            TestLog::get().c_str());
}

TEST_F(CoordinatorServerListTest, workFailed) {
    CoordinatorServerList::UpdaterWorkUnit wu;
    sl->haltUpdater();

    ServerId id1 = generateUniqueId();
    add(id1, "mock:host=server1", {WireFormat::MEMBERSHIP_SERVICE}, 0);
    CoordinatorServerList::Entry* e = sl->getEntry(id1);
    ASSERT_TRUE(sl->getWork(&wu));
    EXPECT_EQ(id1, wu.targetServer);
    EXPECT_EQ(UNINITIALIZED_VERSION, e->verifiedVersion);
    EXPECT_EQ(sl->version, e->updateVersion);

    TestLog::Enable _;
    sl->workFailed(id1);
    EXPECT_STREQ("workFailed: ServerList Update Failed : 1.0 update (0 => 0)",
            TestLog::get().c_str());

    EXPECT_EQ(UNINITIALIZED_VERSION, e->verifiedVersion);
    EXPECT_EQ(UNINITIALIZED_VERSION, e->updateVersion);

    // Work should be available again
    EXPECT_TRUE(sl->getWork(&wu));
    EXPECT_EQ(id1, wu.targetServer);
}

TEST_F(CoordinatorServerListTest, workFailed_multipleInvokes) {
    sl->haltUpdater();

    ServerId id1 = generateUniqueId();
    add(id1, "mock:host=server1", {WireFormat::MEMBERSHIP_SERVICE}, 0);

    TestLog::Enable _;
    sl->workFailed(id1);
    EXPECT_STREQ("workFailed: Bookeeping issue detected; server's count of "
            "numUpdatingServers just went negative. Not total failure but "
            "will cause the updater thread to spin even w/o work. Cause is "
            "mismatch # of getWork() and workSuccess/Failed() | workFailed: "
            "ServerList Update Failed : 1.0 update (0 => 0)",
            TestLog::get().c_str());
}

TEST_F(CoordinatorServerListTest, appendServerList) {
    // Generate two updates, then manually stuff them into an RPC
    ProtoBuf::ServerList list1, list2, list;
    ServerId id1 = generateUniqueId(), id2 = generateUniqueId();
    add(id1, "mock:host=server1", {WireFormat::MEMBERSHIP_SERVICE}, 0);
    add(id2, "mock:host=server2", {WireFormat::MEMBERSHIP_SERVICE}, 0);
    list1 = sl->updates.front().full;
    list2 = sl->updates.back().incremental;

    CoordinatorServerList::UpdateServerListRpc rpc(&context, {}, &list1);
    rpc.appendServerList(&list2);

    // check integrity by interpreting the lists inserted
    // List 1
    uint32_t reqOffset = sizeof32(WireFormat::UpdateServerList::Request);
    auto* part = rpc.request.getOffset<
                    WireFormat::UpdateServerList::Request::Part>(reqOffset);
    reqOffset += sizeof32(*part);
    ASSERT_TRUE(part);
    ProtoBuf::parseFromRequest(&(rpc.request), reqOffset,
                                   part->serverListLength, &list);
    reqOffset += part->serverListLength;
    EXPECT_EQ(1UL, list.version_number());

    // List 2
    part = rpc.request.getOffset<
                    WireFormat::UpdateServerList::Request::Part>(reqOffset);
    reqOffset += sizeof32(*part);
    ASSERT_TRUE(part);
    ProtoBuf::parseFromRequest(&(rpc.request), reqOffset,
                                   part->serverListLength, &list);
    reqOffset += part->serverListLength;
    EXPECT_EQ(2UL, list.version_number());

    // should be end of rpc
    EXPECT_EQ(reqOffset, rpc.request.getTotalLength());
}


} // namespace RAMCloud
