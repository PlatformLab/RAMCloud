/* Copyright (c) 2011-2014 Stanford University
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
#include "WireFormat.h"
#include "MasterRecoveryInfo.pb.h"
#include "ServerList.pb.h"
#include "ServerListEntry.pb.h"

namespace RAMCloud {

namespace {
struct MockServerTracker : public ServerTracker<int> {
    explicit MockServerTracker(Context* context)
    : ServerTracker<int>(context)
    , changes() {
    }

    void enqueueChange(const ServerDetails& server, ServerChangeEvent event) {
        changes.push({server, event});
        const char *name;
        switch (event) {
            case ServerChangeEvent::SERVER_ADDED:
                name = "SERVER_ADDED";
                break;
            case ServerChangeEvent::SERVER_CRASHED:
                name = "SERVER_CRASHED";
                break;
            case ServerChangeEvent::SERVER_REMOVED:
                name = "SERVER_REMOVED";
                break;
            default:
                name = "unknown";
        }
        TEST_LOG("pushing %s event for %s", name,
                server.serverId.toString().c_str());
    }

    void fireCallback() {
        TEST_LOG("called");
    }
    std::queue<ServerTracker<int>::ServerChange> changes;
};
}

class CoordinatorServerListTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    Tub<MockTransport> transport;  // Must be destroyed *after* cluster.
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    CoordinatorService* service;
    CoordinatorServerList* sl;
    Tub<MockServerTracker> tr;
    std::mutex mutex;
    typedef std::unique_lock<std::mutex> Lock;
    Lock lock;
    MockExternalStorage* storage;
    Tub<CoordinatorServerList::UpdateServerListRpc> rpc;

    CoordinatorServerListTest()
        : logEnabler()
        , context()
        , transport()
        , cluster(&context)
        , ramcloud()
        , service()
        , sl()
        , tr()
        , mutex()
        , lock(mutex)
        , storage()
        , rpc()
    {
        service = cluster.coordinator.get();
        cluster.coordinatorContext.recoveryManager->doNotStartRecoveries
                = true;

        ramcloud.construct(service->context, "mock:host=coordinator");
        sl = service->context->coordinatorServerList;
        sl->haltUpdater();
        tr.construct(service->context);

        transport.construct(service->context);
        service->context->transportManager->registerMock(transport.get());

        storage = &cluster.externalStorage;
    }

    ~CoordinatorServerListTest() {
        service->context->transportManager->unregisterMock();
        // Stop all pending ServerList updates before destroying cluster.
        cluster.haltCoordinatorServerListUpdater();
    }

    // Given info about a server list entry, generate the corresponding string
    // that would be stored in external storage for that entry.
    string createStorageEntry(ServerId serverId, ServiceMask services =
            ServiceMask({WireFormat::MASTER_SERVICE,
                    WireFormat::MEMBERSHIP_SERVICE}),
            ServerStatus status = ServerStatus::UP, uint64_t version = 1,
            uint64_t updateSequenceNumber = 1,
            const char* locator = "mock:host=master", uint32_t readSpeed = 100,
            uint64_t openId = 222, uint64_t openEpoch = 77,
            uint64_t replicationId = 55) {
        CoordinatorServerList::Entry entry(serverId, locator, services);
        entry.status = status;
        entry.serviceLocator = locator;
        entry.expectedReadMBytesPerSec = readSpeed;
        entry.masterRecoveryInfo.set_min_open_segment_id(openId);
        entry.masterRecoveryInfo.set_min_open_segment_epoch(openEpoch);
        entry.replicationId = replicationId;
        entry.pendingUpdates.push_back(createUpdate(status, version,
                updateSequenceNumber));
        entry.sync(storage);
        return storage->setData;
    }

    /**
     * Convenience function for creating a ProtoBuf::ServerListEntry_Update.
     *
     * \param status
     *      Desired status for the update.
     * \param version
     *      Desired version for the update.
     * \param sequenceNumber
     *      Desired sequence number for the update.
     * \return
     *      A ProtoBuf::ServerListEntry_Update corresponding to the arguments.
     */
    ProtoBuf::ServerListEntry_Update
    createUpdate(ServerStatus status, uint64_t version, uint64_t sequenceNumber)
    {
        ProtoBuf::ServerListEntry_Update update;
        update.set_status(uint32_t(status));
        update.set_version(version);
        update.set_sequence_number(sequenceNumber);
        return update;
    }

    // Fill in the response for an UpdateServerListRpc, and mark the
    // RPC as finished.
    void finishRpc(CoordinatorServerList::UpdateServerListRpc* rpc,
            const char* response) {
        rpc->response->fillFromString(response);
        rpc->completed();
    }

    // Propagate all pending updates to the rest of the cluster, faking
    // responses from other servers so that all updates appear to have
    // been fully propagated.
    void finishUpdates() {
        while (sl->activeRpcs.size() > 0) {
            finishRpc(sl->activeRpcs.front()->get(), "0 -1 -1");
        }
    }

    // This method is used to pre-initialize entries in the server list,
    // without persisting or propagating any of the information.
    CoordinatorServerList::Entry*
    initServer(ServerId serverId, string serviceLocator,
               ServiceMask serviceMask, uint32_t readSpeed,
               ServerStatus status) {
        uint32_t index = serverId.indexNumber();
        if (sl->serverList.size() <= index) {
            sl->serverList.resize(index+1);
        }
        sl->serverList[index].nextGenerationNumber =
                serverId.generationNumber() + 1;
        sl->serverList[index].entry.construct(serverId, serviceLocator,
                serviceMask);
        CoordinatorServerList::Entry* entry = sl->serverList[index].entry.get();
        entry->status = status;
        entry->replicationId = 12345;
        entry->expectedReadMBytesPerSec = readSpeed;
        entry->masterRecoveryInfo.set_min_open_segment_id(4);
        entry->masterRecoveryInfo.set_min_open_segment_epoch(17);
        return entry;
    }

    /**
     *  Given a buffer containing an UpdateServerList request, parse
     * the buffer and return a readable description of its contents.
     *
     * \param buffer
     *      Contains UpdateServerList request.
     *
     * \return
     *      String describing the request.
     */
    string
    parseUpdateRequest(Buffer* buffer) {
        string result;
        const WireFormat::UpdateServerList::Request* request =
                buffer->getStart<WireFormat::UpdateServerList::Request>();
        uint32_t totalLength = buffer->size();
        if (request == NULL) {
            result.append(format("couldn't read request header; message "
                    "contained only %u bytes, expected %u",
                    totalLength, sizeof32(*request)));
            return result;
        }
        result.append(format("opcode: %s", WireFormat::opcodeSymbol(
                request->common.opcode)));
        uint32_t offset = sizeof32(*request);
        while (offset <totalLength) {
            const WireFormat::UpdateServerList::Request::Part* part =
                    buffer->getOffset<
                    WireFormat::UpdateServerList::Request::Part>(offset);
            if (request == NULL) {
                result.append(format(", couldn't read Part; only %u bytes at"
                        "offset %u, need at least %u",
                        totalLength - offset, offset, sizeof32(*part)));
                return result;
            }
            offset += sizeof32(*part);
            if (totalLength < offset + part->serverListLength) {
                result.append(format(", incomplete ProtoBuf: only %u bytes"
                        " at offset %u, need at least %u",
                        totalLength - offset, offset, part->serverListLength));
            }
            ProtoBuf::ServerList pb;
            ProtoBuf::parseFromRequest(buffer, offset,
                    part->serverListLength, &pb);
            result.append(format(", protobuf: %s",
                    pb.ShortDebugString().c_str()));
            offset += part->serverListLength;
        }
        return result;
    }

    // Return a string with the contents of the server list entry
    // at a given index in the list.
    string
    toString(uint32_t index) {
        string result;
        if (index >= sl->serverList.size()) {
            return format("list has only %lu entries", sl->serverList.size());
        }
        CoordinatorServerList::GenerationNumberEntryPair& pair =
                sl->serverList[index];
        result += format("nextGenerationNumber: %u",
                pair.nextGenerationNumber);
        if (!pair.entry) {
            result += ", no entry";
            return result;
        }
        CoordinatorServerList::Entry* entry = pair.entry.get();
        result += format(", serverId: %s, services: %s, status: %s, "
                "locator: %s",
                entry->serverId.toString().c_str(),
                entry->services.toString().c_str(),
                AbstractServerList::toString(entry->status).c_str(),
                entry->serviceLocator.c_str());
        result += format(", readSpeed: %u, "
                "masterRecoveryInfo: {id: %lu, epoch: %lu}, "
                "replicationId: %lu",
                entry->expectedReadMBytesPerSec,
                entry->masterRecoveryInfo.min_open_segment_id(),
                entry->masterRecoveryInfo.min_open_segment_epoch(),
                entry->replicationId);
        return result;
    }

    // Return a string providing basic information about pending
    // updates.
    string
    updateInfo(CoordinatorServerList* serverList) {
        string result;
        foreach (CoordinatorServerList::ServerListUpdate& pair,
                sl->updates) {
            if (!result.empty()) {
                result.append(" | ");
            }
            result.append(format("version %lu, id %s",
                    pair.version,
                    ServerId(pair.incremental.server(0).server_id())
                    .toString().c_str()));
        }
        return result;
    }

    /**
     *  Wait a while for the updater to either go to sleep or wake up.
     * If this doesn't happen, then give up after a while.
     * \param sleeping
     *      True means wait for the updater to go to sleep; false means
     *      wait for it to wake up.
     */
    void
    waitUpdaterSleeping(bool sleeping) {
        for (int i = 0; i < 1000; i++) {
            if (sl->updaterSleeping == sleeping)
                return;
            usleep(1000);
        }
    }

    DISALLOW_COPY_AND_ASSIGN(CoordinatorServerListTest);
};

TEST_F(CoordinatorServerListTest, constructor) {
    EXPECT_EQ(0U, sl->numberOfMasters);
    EXPECT_EQ(0U, sl->numberOfBackups);
    EXPECT_EQ(0U, sl->version);
}

TEST_F(CoordinatorServerListTest, backupCount) {
    EXPECT_EQ(0U, sl->backupCount());
    sl->enlistServer({WireFormat::BACKUP_SERVICE}, 0, "mock:host=backup1");
    EXPECT_EQ(1U, sl->backupCount());
    sl->enlistServer({WireFormat::BACKUP_SERVICE}, 0, "mock:host=backup2");
    EXPECT_EQ(2U, sl->backupCount());
}

TEST_F(CoordinatorServerListTest, enlistServer_basics) {
    TestLog::Enable _("persistAndPropagate");
    ServerId id1 = sl->enlistServer({WireFormat::BACKUP_SERVICE},
            140, "mock:host=node1");
    EXPECT_EQ("1.0", id1.toString());
    EXPECT_EQ("persistAndPropagate: Persisting 1.0", TestLog::get());
    ServerId id2 = sl->enlistServer(
            {WireFormat::BACKUP_SERVICE, WireFormat::MASTER_SERVICE},
            87, "mock:host=node2");
    EXPECT_EQ("2.0", id2.toString());
    EXPECT_EQ(1U, sl->numberOfMasters);
    EXPECT_EQ(2U, sl->numberOfBackups);
    CoordinatorServerList::Entry* entry = sl->getEntry(id1);
    EXPECT_TRUE(entry != NULL);
    EXPECT_EQ(140U, entry->expectedReadMBytesPerSec);
}

TEST_F(CoordinatorServerListTest, enlistServer_reuseSlot) {
    ServerId id1 = sl->enlistServer({WireFormat::BACKUP_SERVICE},
            140, "mock:host=node1");
    ServerId id2 = sl->enlistServer(
            {WireFormat::BACKUP_SERVICE, WireFormat::MASTER_SERVICE},
            87, "mock:host=node2");
    sl->serverList[2].entry.destroy();
    ServerId id3 = sl->enlistServer(
            {WireFormat::BACKUP_SERVICE, WireFormat::MASTER_SERVICE},
            87, "mock:host=node2");
    EXPECT_EQ("2.1", id3.toString());
}

TEST_F(CoordinatorServerListTest, masterCount) {
    EXPECT_EQ(0U, sl->masterCount());
    sl->enlistServer({WireFormat::MASTER_SERVICE}, 0, "mock:host=node1");
    EXPECT_EQ(1U, sl->masterCount());
    sl->enlistServer({WireFormat::BACKUP_SERVICE, WireFormat::MASTER_SERVICE},
                0, "mock:host=node2");
    EXPECT_EQ(2U, sl->masterCount());
}

TEST_F(CoordinatorServerListTest, enlistServer_createReplicationGroup) {
    TestLog::Enable _("createReplicationGroups");
    sl->enlistServer({WireFormat::BACKUP_SERVICE}, 140, "mock:host=node1");
    EXPECT_EQ("", TestLog::get());
    sl->enlistServer({WireFormat::BACKUP_SERVICE}, 87, "mock:host=node2");
    EXPECT_EQ("", TestLog::get());
    sl->enlistServer({WireFormat::BACKUP_SERVICE}, 87, "mock:host=node3");
    EXPECT_EQ("createReplicationGroups: Server 3.0 is now in "
            "replication group 1 | "
            "createReplicationGroups: Server 2.0 is now in "
            "replication group 1 | "
            "createReplicationGroups: Server 1.0 is now in "
            "replication group 1", TestLog::get());
};

TEST_F(CoordinatorServerListTest, indexWithId) {
    ServerId id1 = sl->enlistServer({WireFormat::MASTER_SERVICE}, 0,
            "mock:host=node1");
    ServerId id2 = sl->enlistServer(
            {WireFormat::BACKUP_SERVICE, WireFormat::MASTER_SERVICE},
            0, "mock:host=node2");

    // First, try a valid id.
    EXPECT_EQ("mock:host=node2", (*sl)[id2].serviceLocator);

    // Next, try an invalid id.
    ServerId id3(2, 3);
    string message("no exception");
    try {
        (*sl)[id3];
    } catch (ServerListException& e) {
        message = e.message;
    }
    EXPECT_EQ("Invalid ServerId (2.3)", message);
}

TEST_F(CoordinatorServerListTest, indexWithSlotNum) {
    ServerId id1 = sl->enlistServer({WireFormat::MASTER_SERVICE}, 0,
            "mock:host=node1");
    ServerId id2 = sl->enlistServer(
            {WireFormat::BACKUP_SERVICE, WireFormat::MASTER_SERVICE},
            0, "mock:host=node2");

    // First, try a valid index.
    EXPECT_EQ("mock:host=node1", (*sl)[1].serviceLocator);

    // Next, try invalid indexes.
    string message("no exception");
    try {
        (*sl)[3];
    } catch (ServerListException& e) {
        message = e.message;
    }
    EXPECT_EQ("Index beyond array length (3) or entry doesn't exist", message);
    EXPECT_THROW((*sl)[0], ServerListException);
}

TEST_F(CoordinatorServerListTest, recover_formatErrorInExternalData) {
    storage->getChildrenNames.push("server1");
    storage->getChildrenValues.push("bogus");
    string message("no exception");
    try {
        sl->recover(50LU);
    } catch (FatalError& e) {
        message = e.message;
    }
    EXPECT_EQ("couldn't parse protocol buffer in servers/server1", message);
}
TEST_F(CoordinatorServerListTest, recover_entryConflict) {
    string server2 = createStorageEntry(ServerId(2, 4));
    string server2a = createStorageEntry(ServerId(2, 1),
            {WireFormat::MEMBERSHIP_SERVICE}, ServerStatus::CRASHED);
    storage->getChildrenNames.push("2");
    storage->getChildrenValues.push(server2);
    storage->getChildrenNames.push("2a");
    storage->getChildrenValues.push(server2a);
    string message("no exception");
    try {
        sl->recover(50);
    } catch (FatalError& e) {
        message = e.message;
    }
    EXPECT_EQ("couldn't process external data at servers/2a (server id 2.1): "
            "server list slot 2 already occupied", message);
    EXPECT_EQ("nextGenerationNumber: 5, serverId: 2.4, "
            "services: MASTER_SERVICE, MEMBERSHIP_SERVICE, status: UP, "
            "locator: mock:host=master, readSpeed: 100, "
            "masterRecoveryInfo: {id: 222, epoch: 77}, replicationId: 55",
            toString(2));
}
TEST_F(CoordinatorServerListTest, recover_serverRemoved) {
    string server1 = createStorageEntry(ServerId(1, 2),
            {WireFormat::MEMBERSHIP_SERVICE}, ServerStatus::REMOVE, 5, 10);
    CoordinatorServerList::Entry* entry2 = initServer(ServerId(2, 3),
            "mock:host=master", {WireFormat::MEMBERSHIP_SERVICE}, 444,
            ServerStatus::CRASHED);
    entry2->pendingUpdates.push_back(createUpdate(
            ServerStatus::CRASHED, 5, 10));
    entry2->pendingUpdates.push_back(createUpdate(
            ServerStatus::REMOVE, 6, 11));
    entry2->sync(storage);
    string server2 = storage->setData;
    // Delete the entry we just created, to revert to pristine state.
    sl->serverList[2].entry.destroy();

    storage->getChildrenNames.push("x");
    storage->getChildrenValues.push(server1);
    storage->getChildrenNames.push("y");
    storage->getChildrenValues.push(server2);
    sl->recover(10);
    EXPECT_EQ("nextGenerationNumber: 3, no entry",
            toString(1));
    EXPECT_EQ("nextGenerationNumber: 4, serverId: 2.3, "
            "services: MEMBERSHIP_SERVICE, status: CRASHED, "
            "locator: mock:host=master, readSpeed: 444, "
            "masterRecoveryInfo: {id: 4, epoch: 17}, replicationId: 12345",
            toString(2));
}
TEST_F(CoordinatorServerListTest, recover_updateMasterAndBackupCounts) {
    string server1 = createStorageEntry(ServerId(1, 0),
            {WireFormat::MEMBERSHIP_SERVICE, WireFormat::MASTER_SERVICE,
            WireFormat::BACKUP_SERVICE}, ServerStatus::UP);
    string server2 = createStorageEntry(ServerId(2, 0),
            {WireFormat::MEMBERSHIP_SERVICE, WireFormat::MASTER_SERVICE},
            ServerStatus::CRASHED);
    string server3 = createStorageEntry(ServerId(32, 0),
            {WireFormat::MEMBERSHIP_SERVICE, WireFormat::BACKUP_SERVICE},
            ServerStatus::UP);
    storage->getChildrenNames.push("a");
    storage->getChildrenValues.push(server1);
    storage->getChildrenNames.push("b");
    storage->getChildrenValues.push(server2);
    storage->getChildrenNames.push("c");
    storage->getChildrenValues.push(server3);
    TestLog::reset();
    sl->recover(50);
    EXPECT_EQ(1LU, sl->numberOfMasters);
    EXPECT_EQ(2LU, sl->numberOfBackups);
}
TEST_F(CoordinatorServerListTest, recover_notifyTrackers) {
    TestLog::Enable _("fireCallback", "enqueueChange", NULL);
    string server1 = createStorageEntry(ServerId(1, 0),
            {WireFormat::MEMBERSHIP_SERVICE}, ServerStatus::UP);
    string server2 = createStorageEntry(ServerId(2, 0),
            {WireFormat::MEMBERSHIP_SERVICE}, ServerStatus::CRASHED);
    string server3 = createStorageEntry(ServerId(3, 0),
            {WireFormat::MEMBERSHIP_SERVICE}, ServerStatus::REMOVE, 10, 100);
    storage->getChildrenNames.push("a");
    storage->getChildrenValues.push(server1);
    storage->getChildrenNames.push("b");
    storage->getChildrenValues.push(server2);
    storage->getChildrenNames.push("c");
    storage->getChildrenValues.push(server3);
    TestLog::reset();
    sl->recover(50);
    EXPECT_EQ("enqueueChange: pushing SERVER_ADDED event for 1.0 | "
            "enqueueChange: pushing SERVER_CRASHED event for 2.0 | "
            "enqueueChange: pushing SERVER_REMOVED event for 3.0 | "
            "fireCallback: called",
            TestLog::get());
}
TEST_F(CoordinatorServerListTest, recover_recomputeVersion) {
    string server1 = createStorageEntry(ServerId(1, 0),
            {WireFormat::MEMBERSHIP_SERVICE}, ServerStatus::UP, 10);
    string server2 = createStorageEntry(ServerId(2, 0),
            {WireFormat::MEMBERSHIP_SERVICE}, ServerStatus::UP, 15);
    string server3 = createStorageEntry(ServerId(32, 0),
            {WireFormat::MEMBERSHIP_SERVICE}, ServerStatus::UP, 5);
    storage->getChildrenNames.push("a");
    storage->getChildrenValues.push(server1);
    storage->getChildrenNames.push("b");
    storage->getChildrenValues.push(server2);
    storage->getChildrenNames.push("3");
    storage->getChildrenValues.push(server3);
    EXPECT_EQ(0lu, sl->version);
    sl->recover(50);
    EXPECT_EQ(15lu, sl->version);
}
TEST_F(CoordinatorServerListTest, recover_incompleteUpdates) {
    string server1 = createStorageEntry(ServerId(1, 2),
            {WireFormat::MEMBERSHIP_SERVICE}, ServerStatus::UP, 5, 10);
    CoordinatorServerList::Entry* entry2 = initServer(ServerId(2, 3),
            "mock:host=master", {WireFormat::MEMBERSHIP_SERVICE}, 444,
            ServerStatus::CRASHED);
    entry2->pendingUpdates.push_back(createUpdate(
            ServerStatus::CRASHED, 4, 9));
    entry2->pendingUpdates.push_back(createUpdate(
            ServerStatus::CRASHED, 5, 11));
    entry2->pendingUpdates.push_back(createUpdate(
            ServerStatus::REMOVE, 6, 12));
    entry2->sync(storage);
    string server2 = storage->setData;
    // Delete the entry we just created, to revert to pristine state.
    sl->serverList[2].entry.destroy();

    storage->getChildrenNames.push("x");
    storage->getChildrenValues.push(server1);
    storage->getChildrenNames.push("y");
    storage->getChildrenValues.push(server2);
    TestLog::reset();
    storage->log.clear();
    cluster.coordinatorContext.coordinatorService->updateManager.lastAssigned
            = 100;
    sl->recover(10);
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "recover: Rescheduling update for server 2.3, version 5, "
            "updateSequence 101, status CRASHED | "
            "recover: Rescheduling update for server 2.3, version 6, "
            "updateSequence 102, status REMOVE"));

    // Make sure that information was properly recorded on external
    // storage.
    ProtoBuf::ServerListEntry info;
    EXPECT_TRUE(info.ParseFromString(cluster.externalStorage.setData));
    EXPECT_EQ("services: 16 server_id: 12884901890 "
            "service_locator: \"mock:host=master\" "
            "expected_read_mbytes_per_sec: 444 status: 1 "
            "replication_id: 12345 master_recovery_info { "
            "min_open_segment_id: 4 min_open_segment_epoch: 17 } "
            "update { status: 1 version: 5 sequence_number: 101 } "
            "update { status: 2 version: 6 sequence_number: 102 }",
            info.ShortDebugString());
    EXPECT_TRUE(TestUtil::contains(storage->log, "set(UPDATE, servers/2)"));

    // Check proper recording in the update list.
    EXPECT_EQ(2lu, sl->updates.size());
    CoordinatorServerList::ServerListUpdate* update = &sl->updates[0];
    EXPECT_EQ(5lu, update->version);
    EXPECT_EQ("server { services: 16 server_id: 12884901890 "
            "service_locator: \"mock:host=master\" "
            "expected_read_mbytes_per_sec: 0 "
            "status: 1 replication_id: 12345 } version_number: 5 type: UPDATE",
            update->incremental.ShortDebugString());
    update = &sl->updates[1];
    EXPECT_EQ(6lu, update->version);
    EXPECT_EQ("server { services: 16 server_id: 12884901890 "
            "service_locator: \"mock:host=master\" "
            "expected_read_mbytes_per_sec: 0 "
            "status: 2 replication_id: 12345 } version_number: 6 type: UPDATE",
            update->incremental.ShortDebugString());

    // Check updates in the server list entries.
    EXPECT_EQ(0LU,
            sl->getEntry(ServerId(1, 2))->pendingUpdates.size());
    EXPECT_EQ(2LU,
            sl->getEntry(ServerId(2, 3))->pendingUpdates.size());
    EXPECT_EQ("status: 1 version: 5 sequence_number: 101",
            sl->getEntry(ServerId(2, 3))->pendingUpdates[0].
            ShortDebugString());
    EXPECT_EQ("status: 2 version: 6 sequence_number: 102",
            sl->getEntry(ServerId(2, 3))->pendingUpdates[1].
            ShortDebugString());
}
TEST_F(CoordinatorServerListTest, recover_repairReplicationGroups) {
    TestLog::Enable _("createReplicationGroups", "removeReplicationGroup",
            "repairReplicationGroups", NULL);
    string server2 = createStorageEntry(ServerId(2, 4),
            {WireFormat::BACKUP_SERVICE});
    string server6 = createStorageEntry(ServerId(6, 1),
            {WireFormat::BACKUP_SERVICE},
            ServerStatus::UP, 1, 1, "mock:host=master", 100,
            222, 77, 0);
    string server7 = createStorageEntry(ServerId(7, 1),
            {WireFormat::BACKUP_SERVICE},
            ServerStatus::UP, 1, 1, "mock:host=master", 100,
            222, 77, 0);
    storage->getChildrenNames.push("6");
    storage->getChildrenValues.push(server6);
    storage->getChildrenNames.push("2");
    storage->getChildrenValues.push(server2);
    storage->getChildrenNames.push("7");
    storage->getChildrenValues.push(server7);
    sl->recover(50LU);
    EXPECT_EQ("repairReplicationGroups: Removing replication group 55 "
            "(has 1 members) | "
            "removeReplicationGroup: Removed server 2.4 from "
            "replication group 55 | "
            "createReplicationGroups: Server 7.1 is now in "
            "replication group 56 | "
            "createReplicationGroups: Server 6.1 is now in "
            "replication group 56 | "
            "createReplicationGroups: Server 2.4 is now in "
            "replication group 56",
            TestLog::get());
}
TEST_F(CoordinatorServerListTest, recover_setVerifiedVersionToVersion) {
    string server1 = createStorageEntry(ServerId(1, 2),
            {WireFormat::MEMBERSHIP_SERVICE}, ServerStatus::UP, 51, 111);
    string server2 = createStorageEntry(ServerId(2, 3),
            {WireFormat::MEMBERSHIP_SERVICE}, ServerStatus::UP, 50, 109);
    storage->getChildrenNames.push("x");
    storage->getChildrenValues.push(server1);
    storage->getChildrenNames.push("y");
    storage->getChildrenValues.push(server2);
    sl->recover(120);
    EXPECT_EQ(51u, sl->serverList[1].entry->verifiedVersion);
    EXPECT_EQ(51u, sl->serverList[1].entry->updateVersion);
    EXPECT_EQ(51u, sl->serverList[2].entry->verifiedVersion);
    EXPECT_EQ(51u, sl->serverList[2].entry->updateVersion);
}
TEST_F(CoordinatorServerListTest, recover_setVerifiedVersionBeforeFirstUpdate)
{
    string server1 = createStorageEntry(ServerId(1, 2),
            {WireFormat::MEMBERSHIP_SERVICE}, ServerStatus::UP, 51, 111);
    string server2 = createStorageEntry(ServerId(2, 3),
            {WireFormat::MEMBERSHIP_SERVICE}, ServerStatus::UP, 50, 109);
    storage->getChildrenNames.push("x");
    storage->getChildrenValues.push(server1);
    storage->getChildrenNames.push("y");
    storage->getChildrenValues.push(server2);
    sl->recover(100);
    EXPECT_EQ(49u, sl->serverList[1].entry->verifiedVersion);
    EXPECT_EQ(49u, sl->serverList[1].entry->updateVersion);
    EXPECT_EQ(49u, sl->serverList[2].entry->verifiedVersion);
    EXPECT_EQ(49u, sl->serverList[2].entry->updateVersion);
}

TEST_F(CoordinatorServerListTest, recoveryCompleted_nonexistentServer) {
    uint64_t orig_version = sl->version;
    TestLog::reset();
    sl->recoveryCompleted(ServerId(3, 4));
    EXPECT_EQ(orig_version, sl->version);
    EXPECT_EQ("recoveryCompleted: Skipping removal for server 3.4: it "
            "doesn't exist", TestLog::get());
}
TEST_F(CoordinatorServerListTest, recoveryCompleted) {
    ServerId id1 = sl->enlistServer({WireFormat::MASTER_SERVICE}, 0,
            "mock:host=node1");
    sl->serverCrashed(id1);
    TestLog::reset();
    sl->recoveryCompleted(id1);
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "recoveryCompleted: Removing server 1.0 from cluster/coordinator "
            "server list"));
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "persistAndPropagate: Persisting 1.0"));
    EXPECT_EQ(ServerStatus::REMOVE, sl->getEntry(id1)->status);
}

TEST_F(CoordinatorServerListTest, serialize) {
    sl->enlistServer({WireFormat::MASTER_SERVICE}, 0, "mock:host=node1");
    sl->enlistServer(
            {WireFormat::BACKUP_SERVICE, WireFormat::MASTER_SERVICE},
            0, "mock:host=node2");
    ProtoBuf::ServerList serverList;
    sl->serialize(&serverList, {WireFormat::MASTER_SERVICE});
    EXPECT_EQ("server { services: 1 server_id: 1 "
            "service_locator: \"mock:host=node1\" "
            "expected_read_mbytes_per_sec: 0 status: 0 replication_id: 0 } "
            "server { services: 3 server_id: 2 "
            "service_locator: \"mock:host=node2\" "
            "expected_read_mbytes_per_sec: 0 status: 0 replication_id: 0 } "
            "version_number: 2 type: FULL_LIST",
            serverList.ShortDebugString());
}

TEST_F(CoordinatorServerListTest, serverCrashed_nonexistentServer) {
    TestLog::reset();
    sl->serverCrashed(ServerId(3, 4));
    EXPECT_EQ("serverCrashed: Skipping serverCrashed for server 3.4: it "
            "doesn't exist", TestLog::get());
}
TEST_F(CoordinatorServerListTest, serverCrashed_statusNotUp) {
    ServerId id1 = sl->enlistServer({WireFormat::MASTER_SERVICE}, 0,
            "mock:host=node1");
    sl->getEntry(id1)->status = ServerStatus::REMOVE;
    TestLog::reset();
    sl->serverCrashed(ServerId(3, 4));
    EXPECT_EQ("serverCrashed: Skipping serverCrashed for server 3.4: it "
            "doesn't exist", TestLog::get());
}
TEST_F(CoordinatorServerListTest, serverCrashed_success) {
    ServerId id1 = sl->enlistServer({WireFormat::MASTER_SERVICE}, 0,
            "mock:host=node1");
    ServerId id2 = sl->enlistServer(
            {WireFormat::BACKUP_SERVICE, WireFormat::MASTER_SERVICE},
            0, "mock:host=node2");
    TestLog::reset();
    sl->serverCrashed(id1);
    EXPECT_EQ(1U, sl->masterCount());
    EXPECT_EQ(1U, sl->backupCount());
    EXPECT_EQ(ServerStatus::CRASHED, sl->getEntry(id1)->status);
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "persistAndPropagate: Persisting 1.0"));
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "startMasterRecovery: Recovery requested for 1.0"));

    // Crash a second server.
    sl->serverCrashed(id2);
    EXPECT_EQ(0U, sl->masterCount());
    EXPECT_EQ(0U, sl->backupCount());
}
TEST_F(CoordinatorServerListTest, serverCrashed_fixReplicationGroups) {
    TestLog::Enable _("createReplicationGroups", "removeReplicationGroup",
            NULL);
    for (int i = 0; i < 6; i++) {
        sl->enlistServer({WireFormat::BACKUP_SERVICE}, 140, "mock:host=foo");
    }
    TestLog::reset();
    sl->serverCrashed(ServerId(2, 0));
    EXPECT_EQ("removeReplicationGroup: Removed server 1.0 from "
            "replication group 1 | "
            "removeReplicationGroup: Removed server 3.0 "
            "from replication group 1", TestLog::get());
    TestLog::reset();
    sl->serverCrashed(ServerId(6, 0));
    EXPECT_EQ("removeReplicationGroup: Removed server 4.0 from "
            "replication group 2 | "
            "removeReplicationGroup: Removed server 5.0 from "
            "replication group 2 | "
            "createReplicationGroups: Server 5.0 is now in "
            "replication group 3 | "
            "createReplicationGroups: Server 4.0 is now in "
            "replication group 3 | "
            "createReplicationGroups: Server 3.0 is now in "
            "replication group 3", TestLog::get());
};

TEST_F(CoordinatorServerListTest, setMasterRecoveryInfo_success) {
    ServerId id1 = sl->enlistServer({WireFormat::MASTER_SERVICE}, 0,
            "mock:host=node1");
    ProtoBuf::MasterRecoveryInfo recoveryInfo;
    recoveryInfo.set_min_open_segment_id(4);
    recoveryInfo.set_min_open_segment_epoch(17);
    cluster.externalStorage.log.clear();
    EXPECT_TRUE(sl->setMasterRecoveryInfo(id1, &recoveryInfo));
    EXPECT_EQ("set(UPDATE, servers/1)", cluster.externalStorage.log);
    ProtoBuf::ServerListEntry entry;
    EXPECT_TRUE(entry.ParseFromString(cluster.externalStorage.setData));
    EXPECT_EQ("min_open_segment_id: 4 min_open_segment_epoch: 17",
            entry.master_recovery_info().ShortDebugString());
}
TEST_F(CoordinatorServerListTest, setMasterRecoveryInfo_noSuchServer) {
    ProtoBuf::MasterRecoveryInfo recoveryInfo;
    recoveryInfo.set_min_open_segment_id(4);
    recoveryInfo.set_min_open_segment_epoch(17);
    cluster.externalStorage.log.clear();
    EXPECT_FALSE(sl->setMasterRecoveryInfo(ServerId{4, 5}, &recoveryInfo));
    EXPECT_EQ("", cluster.externalStorage.log);
}

//////////////////////////////////////////////////////////////////////
// Unit Tests for CoordinatorServerList Private Methods
//////////////////////////////////////////////////////////////////////

TEST_F(CoordinatorServerListTest, getEntry_withId) {
    sl->enlistServer({WireFormat::MASTER_SERVICE}, 0, "mock:host=node1");
    sl->serverList.resize(4);

    EXPECT_TRUE(sl->getEntry({1, 0}) != NULL);
    EXPECT_TRUE(sl->getEntry({1, 1}) == NULL);
    EXPECT_TRUE(sl->getEntry({2, 0}) == NULL);
    EXPECT_TRUE(sl->getEntry({10, 1}) == NULL);
    EXPECT_EQ("mock:host=node1", sl->getEntry({1, 0})->serviceLocator);
}

TEST_F(CoordinatorServerListTest, getEntry_withIndex) {
    sl->enlistServer({WireFormat::MASTER_SERVICE}, 0, "mock:host=node1");
    sl->serverList.resize(4);

    EXPECT_TRUE(sl->getEntry(1) != NULL);
    EXPECT_TRUE(sl->getEntry(2) == NULL);
    EXPECT_TRUE(sl->getEntry(10) == NULL);
    EXPECT_EQ("mock:host=node1", sl->getEntry(1)->serviceLocator);
}

TEST_F(CoordinatorServerListTest, firstFreeIndex) {
    EXPECT_EQ(0U, sl->serverList.size());
    EXPECT_EQ(1U, sl->firstFreeIndex(lock));
    EXPECT_EQ(2U, sl->serverList.size());
    sl->serverList.resize(5);
    sl->serverList[1].entry.construct(ServerId(1, 0), "server1",
            ServiceMask());
    sl->serverList[3].entry.construct(ServerId(3, 4), "server3",
            ServiceMask());
    EXPECT_EQ(2U, sl->firstFreeIndex(lock));
    sl->serverList[2].entry.construct(ServerId(2, 1), "server2",
            ServiceMask());
    EXPECT_EQ(4U, sl->firstFreeIndex(lock));
    EXPECT_EQ(5U, sl->serverList.size());
    sl->serverList[4].entry.construct(ServerId(4, 1), "server4",
            ServiceMask());
    EXPECT_EQ(5U, sl->firstFreeIndex(lock));
    EXPECT_EQ(6U, sl->serverList.size());
}

TEST_F(CoordinatorServerListTest, persistAndPropagate) {
    EXPECT_EQ(0U, sl->serverList.size());
    EXPECT_EQ(1U, sl->firstFreeIndex(lock));
    EXPECT_EQ(2U, sl->serverList.size());
    sl->serverList.resize(4);
    initServer(ServerId(2, 3), "server1",
            {WireFormat::BACKUP_SERVICE, WireFormat::MASTER_SERVICE},
            200, ServerStatus::CRASHED);
    CoordinatorServerList::Entry* entry = sl->getEntry({2, 3});
    TestLog::reset();
    cluster.externalStorage.log.clear();
    cluster.coordinatorContext.coordinatorService->updateManager.lastAssigned
            = 10;
    sl->version = 50U;
    sl->persistAndPropagate(lock, entry, ServerChangeEvent::SERVER_CRASHED);
    EXPECT_EQ(1U, entry->pendingUpdates.size());
    EXPECT_EQ("status: 1 version: 51 sequence_number: 11",
                entry->pendingUpdates.back().ShortDebugString());
    EXPECT_TRUE(TestUtil::contains(cluster.externalStorage.log,
                "set(UPDATE, servers/2"));
    ProtoBuf::ServerListEntry entryPb;
    EXPECT_TRUE(entryPb.ParseFromString(cluster.externalStorage.setData));
    EXPECT_EQ("services: 3 server_id: 12884901890 "
            "service_locator: \"server1\" expected_read_mbytes_per_sec: 200 "
            "status: 1 replication_id: 12345 "
            "master_recovery_info { min_open_segment_id: 4 "
            "min_open_segment_epoch: 17 } "
            "update { status: 1 version: 51 sequence_number: 11 }",
            entryPb.ShortDebugString());
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
                "enqueueChange: pushing SERVER_CRASHED event for 2.3"));
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
                "fireCallback: called"));
    EXPECT_EQ(51U, sl->version);
    EXPECT_EQ("version 51, id 2.3", updateInfo(sl));
}

TEST_F(CoordinatorServerListTest, serializeWithLock_serializeAll) {
    sl->serverList.resize(4);
    sl->serverList[1].entry.construct(ServerId(1, 0), "server1",
            ServiceMask({WireFormat::MASTER_SERVICE}));
    sl->serverList[3].entry.construct(ServerId(3, 4), "server3",
            ServiceMask({WireFormat::BACKUP_SERVICE,
            WireFormat::MASTER_SERVICE}));
    ProtoBuf::ServerList protoBuf;
    sl->serialize(lock, &protoBuf);
    EXPECT_EQ("server { services: 1 server_id: 1 service_locator: \"server1\" "
            "expected_read_mbytes_per_sec: 0 status: 0 replication_id: 0 } "
            "server { services: 3 server_id: 17179869187 "
            "service_locator: \"server3\" expected_read_mbytes_per_sec: 0 "
            "status: 0 replication_id: 0 } version_number: 0 type: FULL_LIST",
            protoBuf.ShortDebugString());
}
TEST_F(CoordinatorServerListTest, serializeWithLock_selectWithMask) {
    sl->serverList.resize(4);
    sl->serverList[1].entry.construct(ServerId(1, 0), "server1",
            ServiceMask({WireFormat::MASTER_SERVICE}));
    sl->serverList[3].entry.construct(ServerId(3, 4), "server3",
            ServiceMask({WireFormat::BACKUP_SERVICE,
            WireFormat::MASTER_SERVICE}));
    ProtoBuf::ServerList protoBuf;
    sl->serialize(lock, &protoBuf, {WireFormat::BACKUP_SERVICE});
    EXPECT_EQ("server { services: 3 server_id: 17179869187 "
            "service_locator: \"server3\" expected_read_mbytes_per_sec: 0 "
            "status: 0 replication_id: 0 } "
            "version_number: 0 type: FULL_LIST",
            protoBuf.ShortDebugString());
}

TEST_F(CoordinatorServerListTest, createReplicationGroups) {
    TestLog::Enable _("createReplicationGroups");
    sl->serverList.resize(20);
    // Create enough servers for two full replication groups but not
    // quite a third. Create several other servers that aren't eligible
    // for a new replication group.
    for (int i = 1; i <= 13; i++) {
        if ((i == 2) || (i == 7)) {
            continue;
        }
        sl->serverList[i].entry.construct(ServerId(i, 0), "server",
                ServiceMask({WireFormat::BACKUP_SERVICE}));
        sl->serverList[i].entry->status = ServerStatus::UP;
    }
    sl->serverList[6].entry->status = ServerStatus::CRASHED;
    sl->serverList[8].entry->services = {WireFormat::MASTER_SERVICE};
    sl->serverList[10].entry->replicationId = 111;
    TestLog::reset();
    sl->createReplicationGroups(lock);
    EXPECT_EQ("createReplicationGroups: Server 13.0 is now in "
            "replication group 1 | "
            "createReplicationGroups: Server 12.0 is now in "
            "replication group 1 | "
            "createReplicationGroups: Server 11.0 is now in "
            "replication group 1 | "
            "createReplicationGroups: Server 9.0 is now in "
            "replication group 2 | "
            "createReplicationGroups: Server 5.0 is now in "
            "replication group 2 | "
            "createReplicationGroups: Server 4.0 is now in "
            "replication group 2", TestLog::get());
    EXPECT_EQ(2LU, sl->maxReplicationId);
}

TEST_F(CoordinatorServerListTest, removeReplicationGroup) {
    TestLog::Enable _("removeReplicationGroup");
    sl->serverList.resize(20);
    for (int i = 1; i <= 10; i++) {
        if ((i == 2) || (i == 7)) {
            continue;
        }
        sl->serverList[i].entry.construct(ServerId(i, 0), "server",
                ServiceMask({WireFormat::BACKUP_SERVICE}));
        sl->serverList[i].entry->status = ServerStatus::UP;
        sl->serverList[i].entry->replicationId = 44;
    }
    sl->serverList[1].entry->replicationId = 0;
    sl->serverList[3].entry->status = ServerStatus::CRASHED;
    sl->serverList[8].entry->services = {WireFormat::MASTER_SERVICE};
    sl->serverList[9].entry->replicationId = 111;
    TestLog::reset();
    sl->removeReplicationGroup(lock, 0);
    EXPECT_EQ("", TestLog::get());
    sl->removeReplicationGroup(lock, 44);
    EXPECT_EQ("removeReplicationGroup: Removed server 4.0 from "
            "replication group 44 | "
            "removeReplicationGroup: Removed server 5.0 from "
            "replication group 44 | "
            "removeReplicationGroup: Removed server 6.0 from "
            "replication group 44 | "
            "removeReplicationGroup: Removed server 10.0 from "
            "replication group 44", TestLog::get());
}

TEST_F(CoordinatorServerListTest, repairReplicationGroups) {
    TestLog::Enable _("createReplicationGroups", "removeReplicationGroup",
            "repairReplicationGroups", NULL);
    sl->serverList.resize(20);
    // Create enough servers for two full replication groups but not
    // quite a third. Create several other servers that aren't eligible
    // for a new replication group.
    for (int i = 1; i <= 12; i++) {
        if (i == 2) {
            // Leave slot 2 empty.
            continue;
        }
        sl->serverList[i].entry.construct(ServerId(i, 0), "server",
                ServiceMask({WireFormat::BACKUP_SERVICE}));
        sl->serverList[i].entry->status = ServerStatus::UP;
    }
    // Slot 6 is crashed, and slot 7 has no backup
    sl->serverList[6].entry->status = ServerStatus::CRASHED;
    sl->serverList[7].entry->services = {WireFormat::MASTER_SERVICE};

    // Create one complete replication group (33) and one incomplete
    // replication group (44), plus leave several servers unallocated.
    sl->serverList[1].entry->replicationId = 33;
    sl->serverList[3].entry->replicationId = 0;
    sl->serverList[4].entry->replicationId = 0;
    sl->serverList[5].entry->replicationId = 99;
    sl->serverList[6].entry->replicationId = 33;
    sl->serverList[7].entry->replicationId = 33;
    sl->serverList[8].entry->replicationId = 99;
    sl->serverList[9].entry->replicationId = 33;
    sl->serverList[10].entry->replicationId = 33;
    sl->serverList[11].entry->replicationId = 0;
    sl->serverList[12].entry->replicationId = 0;
    TestLog::reset();
    sl->repairReplicationGroups(lock);
    EXPECT_EQ("repairReplicationGroups: Removing replication group 99 "
            "(has 2 members) | "
            "removeReplicationGroup: Removed server 5.0 from "
            "replication group 99 | "
            "removeReplicationGroup: Removed server 8.0 from "
            "replication group 99 | "
            "createReplicationGroups: Server 12.0 is now in "
            "replication group 100 | "
            "createReplicationGroups: Server 11.0 is now in "
            "replication group 100 | "
            "createReplicationGroups: Server 8.0 is now in "
            "replication group 100 | "
            "createReplicationGroups: Server 5.0 is now in "
            "replication group 101 | "
            "createReplicationGroups: Server 4.0 is now in "
            "replication group 101 | "
            "createReplicationGroups: Server 3.0 is now in "
            "replication group 101", TestLog::get());
    EXPECT_EQ(101LU, sl->maxReplicationId);
}

////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////
////  Tests for Updater Mechanism
////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////

TEST_F(CoordinatorServerListTest, pushUpdate) {
    CoordinatorServerList::Entry* entry = initServer({2, 0},
            "mock:host=server1",
            {WireFormat::MASTER_SERVICE, WireFormat::MEMBERSHIP_SERVICE},
            400, ServerStatus::UP);
    sl->version = 10;
    sl->pushUpdate(lock, entry);
    EXPECT_EQ(11UL, sl->version);
    EXPECT_EQ(1UL, sl->updates.size());
    CoordinatorServerList::ServerListUpdate* update = &(sl->updates.front());
    EXPECT_EQ(11UL, update->version);
    EXPECT_EQ("server { services: 17 server_id: 2 "
            "service_locator: \"mock:host=server1\" "
            "expected_read_mbytes_per_sec: 0 status: 0 "
            "replication_id: 12345 } version_number: 11 type: UPDATE",
            update->incremental.ShortDebugString());
}

TEST_F(CoordinatorServerListTest, insertUpdate_basics) {
    ServerId id = sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 100,
            "mock:host=server");
    CoordinatorServerList::Entry* entry = sl->getEntry(id);
    sl->updates.clear();
    sl->insertUpdate(lock, entry, 51);
    sl->insertUpdate(lock, entry, 49);
    sl->insertUpdate(lock, entry, 52);
    sl->insertUpdate(lock, entry, 50);
    ASSERT_EQ(4u, sl->updates.size());
    EXPECT_EQ(49u, sl->updates[0].version);
    EXPECT_EQ(50u, sl->updates[1].version);
    EXPECT_EQ(51u, sl->updates[2].version);
    EXPECT_EQ(52u, sl->updates[3].version);
}
TEST_F(CoordinatorServerListTest, insertUpdate_duplicate) {
    ServerId id1 = sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 100,
            "mock:host=server");
    ServerId id2 = sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 100,
            "mock:host=server");
    CoordinatorServerList::Entry* entry1 = sl->getEntry(id1);
    CoordinatorServerList::Entry* entry2 = sl->getEntry(id2);
    sl->updates.clear();
    sl->insertUpdate(lock, entry1, 49);
    sl->insertUpdate(lock, entry1, 51);

    string message("no exception");
    try {
        sl->insertUpdate(lock, entry2, 51);
    } catch (FatalError& e) {
        message = e.message;
    }
    EXPECT_EQ("Duplicated CSL entry version 51 for servers 2.0 and 1.0",
            message);
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
    // Initial state, should be up to date.
    ASSERT_EQ(0UL, sl->serverList.size());
    EXPECT_TRUE(sl->isClusterUpToDate(lock));

    // Add a server.
    sl->enlistServer(
            {WireFormat::MASTER_SERVICE, WireFormat::MEMBERSHIP_SERVICE}, 0,
            "mock:host=server1");
    EXPECT_FALSE(sl->isClusterUpToDate(lock));

    // Fake an update
    ASSERT_TRUE(sl->getWork(&rpc));
    sl->workSuccess(rpc->id, ~0lu);
    ASSERT_FALSE(sl->getWork(&rpc));
    // Note: the second getWork() is needed because the conditions to
    // isClusterUpToDate are updated lazily and the second getWork()
    // forces a full scan of the server list and update the conditions.
    ASSERT_FALSE(sl->getWork(&rpc));
    EXPECT_TRUE(sl->isClusterUpToDate(lock));
}

TEST_F(CoordinatorServerListTest, pruneUpdates_badMaxConfirmedVersion) {
    sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 100,
            "mock:host=server1");
    sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 100,
            "mock:host=server2");

    string message("no exception");
    try {
        sl->maxConfirmedVersion = 99;
        sl->pruneUpdates(lock);
    } catch (FatalError& e) {
        message = e.message;
    }
    EXPECT_EQ("CoordinatorServerList's  maxConfirmedVersion 99 is larger "
            "than its current version 2. This should NEVER happen!",
            message);
}
TEST_F(CoordinatorServerListTest, pruneUpdates_basics) {
    CoordinatorUpdateManager* updateManager =
            &cluster.coordinatorContext.coordinatorService->updateManager;
    updateManager->reset();

    // Create 10 items on the update list
    for (int i = 1; i <= 10; i++) {
        sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 100,
                "mock:host=server");
    }

    // Prune the oldest 4 items.
    sl->maxConfirmedVersion = 4;
    sl->pruneUpdates(lock);
    EXPECT_EQ(10UL, sl->version);
    EXPECT_EQ(6UL, sl->updates.size());
    for (int i = 5; i <= 10; i++) {
        ProtoBuf::ServerList& update = sl->updates.at(i - 5).incremental;
        EXPECT_EQ(i, static_cast<int>(update.version_number()));
    }
    EXPECT_EQ(5UL, updateManager->smallestUnfinished);
    EXPECT_EQ(0UL, sl->getEntry(ServerId(1, 0))->pendingUpdates.size());
    EXPECT_EQ(0UL, sl->getEntry(ServerId(4, 0))->pendingUpdates.size());
    EXPECT_EQ(1UL, sl->getEntry(ServerId(5, 0))->pendingUpdates.size());

    // Nothing should get pruned here.
    sl->maxConfirmedVersion = 2;
    sl->pruneUpdates(lock);
    EXPECT_EQ(6UL, sl->updates.size());
    EXPECT_EQ(5UL, updateManager->smallestUnfinished);

    // Prune everything.
    sl->maxConfirmedVersion = 10;
    sl->pruneUpdates(lock);
    EXPECT_EQ(0UL, sl->updates.size());
    EXPECT_EQ(11UL, updateManager->smallestUnfinished);
    EXPECT_EQ(0UL, sl->getEntry(ServerId(5, 0))->pendingUpdates.size());
    EXPECT_EQ(0UL, sl->getEntry(ServerId(10, 0))->pendingUpdates.size());
}
TEST_F(CoordinatorServerListTest, pruneUpdates_updateVersionLessThanEntry) {
    CoordinatorUpdateManager* updateManager =
            &cluster.coordinatorContext.coordinatorService->updateManager;
    updateManager->reset();

    ServerId id = sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 100,
            "mock:host=server");
    CoordinatorServerList::Entry* entry = sl->getEntry(id);
    entry->pendingUpdates.clear();
    entry->pendingUpdates.push_back(createUpdate(ServerStatus::UP, 15, 0));
    sl->updates.clear();
    sl->insertUpdate(lock, entry, 14);
    sl->maxConfirmedVersion = 14;
    sl->version = 20;
    TestLog::reset();
    sl->pruneUpdates(lock);
    EXPECT_EQ(1UL, entry->pendingUpdates.size());
    EXPECT_EQ(0UL, sl->updates.size());
    EXPECT_EQ("pruneUpdates: version number mismatch in update for "
            "server 1.0: completed update has version 14, "
            "but first version from entry is 15", TestLog::get());
}

TEST_F(CoordinatorServerListTest, pruneUpdates_updateVersionGreaterThanEntry) {
    CoordinatorUpdateManager* updateManager =
            &cluster.coordinatorContext.coordinatorService->updateManager;
    updateManager->reset();

    ServerId id = sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 100,
            "mock:host=server");
    CoordinatorServerList::Entry* entry = sl->getEntry(id);
    entry->pendingUpdates.clear();
    entry->pendingUpdates.push_back(createUpdate(ServerStatus::UP, 15, 0));
    entry->pendingUpdates.push_back(createUpdate(ServerStatus::UP, 16, 0));
    entry->pendingUpdates.push_back(createUpdate(ServerStatus::UP, 17, 0));
    entry->pendingUpdates.push_back(createUpdate(ServerStatus::UP, 18, 0));
    sl->updates.clear();
    sl->insertUpdate(lock, entry, 17);
    sl->maxConfirmedVersion = 17;
    sl->version = 20;
    TestLog::reset();
    sl->pruneUpdates(lock);
    EXPECT_EQ(1UL, entry->pendingUpdates.size());
    EXPECT_EQ(0UL, sl->updates.size());
    EXPECT_EQ("pruneUpdates: version number mismatch in update for "
            "server 1.0: completed update has version 17, "
            "but first version from entry is 15 | "
            "pruneUpdates: version number mismatch in update for "
            "server 1.0: completed update has version 17, "
            "but first version from entry is 16", TestLog::get());
}

TEST_F(CoordinatorServerListTest, pruneUpdates_cleanupRemovedServer) {
    ServerId id1 = sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 100,
            "mock:host=server1");
    sl->serverCrashed(id1);
    sl->recoveryCompleted(id1);
    EXPECT_EQ(3UL, sl->version);

    // Prune everything except the last update, which is the remove.
    sl->maxConfirmedVersion = 2;
    sl->pruneUpdates(lock);
    EXPECT_EQ(1UL, sl->updates.size());
    EXPECT_TRUE(sl->serverList[id1.indexNumber()].entry);

    // Now prune the last update and make sure the server list entry goes away.
    sl->maxConfirmedVersion = 3;
    sl->pruneUpdates(lock);
    EXPECT_EQ(0UL, sl->updates.size());
    EXPECT_FALSE(sl->serverList[id1.indexNumber()].entry);
}

TEST_F(CoordinatorServerListTest, sync) {
    // Test that sync wakes up thread and flushes all updates
    sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 100,
            "mock:host=server");
    transport->setInput("0 1 0");

    sl->sync();
    EXPECT_EQ("sendRequest: 0x40023 1 0 11 273 0 /0 /x18/0",
            transport->outputLog);
    transport->clearOutput();

    // Test that syncs on up-to-date list don't clog
    sl->sync();
    sl->sync();
    EXPECT_EQ("", transport->outputLog);
}

// The following test exercises both updateLoop and waitForWork (they are
// too intertwined to test separately).
TEST_F(CoordinatorServerListTest, updateLoopAndWaitForWork) {
    // Initial state: updater not running.
    EXPECT_FALSE(sl->updaterSleeping);

    // Start updater: it should go to sleep quickly.
    sl->startUpdater();
    waitUpdaterSleeping(true);
    EXPECT_TRUE(sl->updaterSleeping);

    // Enlist a server: this will wake up the updater, which will start
    // an RPC.
    sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 100,
            "mock:host=server1");
    waitUpdaterSleeping(false);
    EXPECT_FALSE(sl->updaterSleeping);
    EXPECT_EQ(1UL, sl->activeRpcs.size());

    // Generate a response for the RPC; at this point the updater should
    // go back to sleep.
    TestLog::reset();
    finishRpc(sl->activeRpcs.front()->get(), "0 -1 -1");
    waitUpdaterSleeping(true);
    EXPECT_TRUE(sl->updaterSleeping);
    EXPECT_EQ(0UL, sl->activeRpcs.size());
    EXPECT_EQ("workSuccess: ServerList Update Success: 1.0 update (0 => 1)",
            TestLog::get());

    // Stop the updater: this should wake the updater up so it can exit.
    TestLog::reset();
    sl->haltUpdater();
    waitUpdaterSleeping(false);
    EXPECT_EQ("updateLoop: Updater exited", TestLog::get());
}

TEST_F(CoordinatorServerListTest, checkUpdates) {
    // Create a cluster with 3 servers, which means we need to send out
    // updates to each of the servers.
    sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 100,
            "mock:host=server1");
    sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 100,
            "mock:host=server2");
    sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 100,
            "mock:host=server3");
    EXPECT_EQ(3UL, sl->updates.size());

    // Allow three update RPCs to start in parallel.
    sl->checkUpdates();
    EXPECT_EQ(1UL, sl->activeRpcs.size());
    EXPECT_EQ(0UL, sl->spareRpcs.size());
    sl->checkUpdates();
    EXPECT_EQ(2UL, sl->activeRpcs.size());
    sl->checkUpdates();
    EXPECT_EQ(3UL, sl->activeRpcs.size());
    sl->checkUpdates();
    EXPECT_EQ(3UL, sl->activeRpcs.size());
    EXPECT_EQ(1UL, sl->spareRpcs.size());

    // Allow two RPCs to finish in one call to checkUpdates.
    finishRpc(sl->activeRpcs.front()->get(), "0 -1 -1");
    finishRpc(sl->activeRpcs.back()->get(), "0 -1 -1");
    TestLog::reset();
    sl->checkUpdates();
    EXPECT_EQ(1UL, sl->activeRpcs.size());
    EXPECT_EQ("workSuccess: ServerList Update Success: 1.0 update (0 => 3) | "
            "workSuccess: ServerList Update Success: 3.0 update (0 => 3)",
            TestLog::get());

    // Allow the third RPC to finish.
    EXPECT_EQ(3UL, sl->updates.size());
    finishRpc(sl->activeRpcs.front()->get(), "0 -1 -1");
    sl->checkUpdates();
    EXPECT_EQ(0UL, sl->activeRpcs.size());
    EXPECT_EQ(4UL, sl->spareRpcs.size());

    // Additional calls to checkUpdates should be no-ops. But, additional
    // calls are needed to propagate version information enough to prune.
    sl->checkUpdates();
    EXPECT_EQ(0UL, sl->activeRpcs.size());
    EXPECT_EQ(4UL, sl->spareRpcs.size());
    EXPECT_EQ(0UL, sl->updates.size());
}

TEST_F(CoordinatorServerListTest, checkUpdates_ServerNotUpException) {
    // Create a cluster with 2 servers, which means we need to send out
    // updates to each of the servers.
    sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 100,
            "mock:host=server1");
    ServerId s2 = sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 100,
            "mock:host=server2");

    // Start update RPCs in parallel.
    sl->checkUpdates();
    sl->checkUpdates();
    EXPECT_EQ(2UL, sl->activeRpcs.size());

    // Finish the first RPC, and crash the second server and fail its RPC.
    // During the next checkUpdates call, a new RPC will start to notify
    // the first server of the crash.
    finishRpc(sl->activeRpcs.front()->get(), "0 -1 -1");
    sl->serverCrashed(s2);
    sl->activeRpcs.back()->get()->failed();
    TestLog::reset();
    sl->checkUpdates();
    EXPECT_EQ(1UL, sl->activeRpcs.size());
    EXPECT_EQ("workSuccess: ServerList Update Success: 1.0 update (0 => 2) | "
            "flushSession: flushed session for id 2.0 | "
            "workFailed: ServerList Update Failed : 2.0 update (0 => 0)",
            TestLog::get());

    // Finish the additional RPC.
    finishRpc(sl->activeRpcs.front()->get(), "0 -1 -1");
    TestLog::reset();
    sl->checkUpdates();
    EXPECT_EQ(0UL, sl->activeRpcs.size());
    EXPECT_EQ("workSuccess: ServerList Update Success: 1.0 update (2 => 3)",
            TestLog::get());

    // Additional calls to checkUpdates should be no-ops. But, additional
    // calls are needed to propagate version information enough to prune.
    sl->checkUpdates();
    EXPECT_EQ(0UL, sl->activeRpcs.size());
    EXPECT_EQ(2UL, sl->spareRpcs.size());
    EXPECT_EQ(0UL, sl->updates.size());
}

TEST_F(CoordinatorServerListTest, getWork_emptyServerList) {
    EXPECT_FALSE(sl->getWork(&rpc));
}

TEST_F(CoordinatorServerListTest, getWork_noWorkFoundForEpoch) {
    ServerId id1 = sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 0,
            "mock:host=server1");
    EXPECT_TRUE(sl->getWork(&rpc));
    EXPECT_EQ(1UL, sl->numUpdatingServers);

    // It may take a couple of calls to getWork before information
    // propagates to noWorkFoundForEpoch.
    EXPECT_FALSE(sl->getWork(&rpc));
    EXPECT_FALSE(sl->getWork(&rpc));
    EXPECT_EQ(1UL, sl->lastScan.noWorkFoundForEpoch);
}

TEST_F(CoordinatorServerListTest, getWork_serverCrashed) {
    ServerId id1 = sl->enlistServer(
            {WireFormat::MASTER_SERVICE, WireFormat::MEMBERSHIP_SERVICE}, 0,
            "mock:host=server1");
    CoordinatorServerList::Entry* e = sl->getEntry(id1);
    e->status = ServerStatus::CRASHED;
    EXPECT_FALSE(sl->getWork(&rpc));
}

TEST_F(CoordinatorServerListTest, getWork_noMembershipService) {
    ServerId id1 = sl->enlistServer({WireFormat::MASTER_SERVICE}, 0,
            "mock:host=server1");
    EXPECT_FALSE(sl->getWork(&rpc));
}

TEST_F(CoordinatorServerListTest, getWork_fullListUpdate) {
    ServerId id1 = sl->enlistServer(
            {WireFormat::MASTER_SERVICE, WireFormat::MEMBERSHIP_SERVICE}, 0,
            "mock:host=server1");
    EXPECT_TRUE(sl->getWork(&rpc));
    EXPECT_EQ("opcode: UPDATE_SERVER_LIST, protobuf: "
            "server { services: 17 server_id: 1 "
            "service_locator: \"mock:host=server1\" "
            "expected_read_mbytes_per_sec: 0 status: 0 replication_id: 0 } "
            "version_number: 1 type: FULL_LIST",
            parseUpdateRequest(&rpc->request));
    EXPECT_EQ(0lu, sl->lastScan.minVersion);
    CoordinatorServerList::Entry* e = sl->getEntry(id1);
    EXPECT_EQ(1lu, e->updateVersion);
    EXPECT_EQ(1lu, sl->numUpdatingServers);
}

TEST_F(CoordinatorServerListTest, getWork_incrementalUpdates) {
    // Create a bunch of servers.
    ServerId id1 = sl->enlistServer({WireFormat::MASTER_SERVICE}, 0,
            "mock:host=server1");
    ServerId id2 = sl->enlistServer({WireFormat::MASTER_SERVICE}, 0,
            "mock:host=server2");
    ServerId id3 = sl->enlistServer({WireFormat::MASTER_SERVICE}, 0,
            "mock:host=server3");
    ServerId id4 = sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 0,
            "mock:host=server4");

    // Process all pending updates
    while (sl->getWork(&rpc)) {
        sl->workSuccess(rpc->id, ~0lu);
    }

    // Generate several updates.
    sl->serverCrashed(id2);
    sl->serverCrashed(id3);
    sl->recoveryCompleted(id2);
    EXPECT_TRUE(sl->getWork(&rpc));
    EXPECT_EQ("opcode: UPDATE_SERVER_LIST, "
            "protobuf: server { services: 1 server_id: 2 "
            "service_locator: \"mock:host=server2\" "
            "expected_read_mbytes_per_sec: 0 status: 1 replication_id: 0 } "
            "version_number: 5 type: UPDATE, "
            "protobuf: server { services: 1 server_id: 3 "
            "service_locator: \"mock:host=server3\" "
            "expected_read_mbytes_per_sec: 0 status: 1 replication_id: 0 } "
            "version_number: 6 type: UPDATE, "
            "protobuf: server { services: 1 server_id: 2 "
            "service_locator: \"mock:host=server2\" "
            "expected_read_mbytes_per_sec: 0 status: 2 replication_id: 0 } "
            "version_number: 7 type: UPDATE",
            parseUpdateRequest(&rpc->request));
    EXPECT_EQ(4lu, sl->lastScan.minVersion);
    CoordinatorServerList::Entry* e = sl->getEntry(id4);
    EXPECT_EQ(7lu, e->updateVersion);
}

TEST_F(CoordinatorServerListTest, getWork_skipEntriesAlreadySeen) {
    // Create a bunch of servers.
    ServerId id1 = sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 0,
            "mock:host=server1");
    sl->enlistServer({WireFormat::MASTER_SERVICE}, 0, "mock:host=server2");
    sl->enlistServer({WireFormat::MASTER_SERVICE}, 0, "mock:host=server3");
    sl->enlistServer({WireFormat::MASTER_SERVICE}, 0, "mock:host=server4");

    // Pretend that id1 has already seen the first few updates.
    CoordinatorServerList::Entry* e = sl->getEntry(id1);
    e->updateVersion = e->verifiedVersion = 2;

    // See whether getWork skips the entries already "seen".
    EXPECT_TRUE(sl->getWork(&rpc));
    EXPECT_EQ("opcode: UPDATE_SERVER_LIST, "
            "protobuf: server { services: 1 server_id: 3 "
            "service_locator: \"mock:host=server3\" "
            "expected_read_mbytes_per_sec: 0 status: 0 replication_id: 0 } "
            "version_number: 3 type: UPDATE, "
            "protobuf: server { services: 1 server_id: 4 "
            "service_locator: \"mock:host=server4\" "
            "expected_read_mbytes_per_sec: 0 status: 0 replication_id: 0 } "
            "version_number: 4 type: UPDATE",
            parseUpdateRequest(&rpc->request));
    EXPECT_EQ(4lu, e->updateVersion);
}

TEST_F(CoordinatorServerListTest, getWork_updateStatsAndPrune) {
    // Create two servers.
    ServerId id1 = sl->enlistServer(
            {WireFormat::MASTER_SERVICE, WireFormat::MEMBERSHIP_SERVICE}, 0,
            "mock:host=server1");
    ServerId id2 = sl->enlistServer(
            {WireFormat::MASTER_SERVICE, WireFormat::MEMBERSHIP_SERVICE}, 0,
            "mock:host=server2");
    EXPECT_TRUE(sl->getWork(&rpc));
    EXPECT_TRUE(sl->getWork(&rpc));

    // Note: it can take 2 passes through getWork to propagate everything:
    // one to recompute maxConfirmedVersion, one to use it in pruneUpdates.
    EXPECT_FALSE(sl->getWork(&rpc));
    EXPECT_FALSE(sl->getWork(&rpc));

    // At this point, two update requests are outstanding.
    EXPECT_EQ(2lu, sl->numUpdatingServers);
    EXPECT_EQ(2u, sl->updates.size());

    // Finish one of them.
    sl->workSuccess(id2, ~0lu);
    EXPECT_FALSE(sl->getWork(&rpc));
    EXPECT_FALSE(sl->getWork(&rpc));
    EXPECT_EQ(1lu, sl->numUpdatingServers);
    EXPECT_EQ(0lu, sl->maxConfirmedVersion);
    EXPECT_EQ(2u, sl->updates.size());

    // Finish the second one.
    sl->workSuccess(id1, ~0lu);
    EXPECT_FALSE(sl->getWork(&rpc));
    EXPECT_FALSE(sl->getWork(&rpc));
    EXPECT_EQ(0lu, sl->numUpdatingServers);
    EXPECT_EQ(2lu, sl->maxConfirmedVersion);
    EXPECT_EQ(0u, sl->updates.size());
}

TEST_F(CoordinatorServerListTest, getWork_noUpdatableServers) {
    // Create two servers.
    sl->enlistServer({}, 0, "mock:host=server1");
    sl->enlistServer({}, 0, "mock:host=server2");
    sl->enlistServer({}, 0, "mock:host=server3");
    EXPECT_EQ(3u, sl->updates.size());
    EXPECT_FALSE(sl->getWork(&rpc));
    EXPECT_FALSE(sl->getWork(&rpc));
    EXPECT_EQ(3lu, sl->maxConfirmedVersion);
    EXPECT_EQ(0u, sl->updates.size());
}

static const uint64_t UNINITIALIZED_VERSION =
        CoordinatorServerList::UNINITIALIZED_VERSION;

TEST_F(CoordinatorServerListTest, workSuccess) {
    uint64_t initialVersion = sl->version;

    ServerId id1 = sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 0,
            "mock:host=server1");
    ASSERT_TRUE(sl->getWork(&rpc));

    ServerId id2 = sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 0,
            "mock:host=server2");
    ASSERT_TRUE(sl->getWork(&rpc));

    TestLog::Enable _;
    sl->workSuccess(id1, ~0lu);
    EXPECT_STREQ("workSuccess: ServerList Update Success: 1.0 update (0 => 1)",
                TestLog::get().c_str());

    CoordinatorServerList::Entry* e1 = sl->getEntry(id1);
    CoordinatorServerList::Entry* e2 = sl->getEntry(id2);

    EXPECT_EQ(initialVersion+1, e1->verifiedVersion);
    EXPECT_EQ(initialVersion+1, e1->updateVersion);
    EXPECT_EQ(UNINITIALIZED_VERSION, e2->verifiedVersion);
    EXPECT_EQ(initialVersion+2, e2->updateVersion);
}

TEST_F(CoordinatorServerListTest, workSuccess_nonExistentServer) {
    TestLog::Enable _;

    // Test Non-existent ServerId
    ServerId id(100, 200);
    sl->workSuccess(id, ~0lu);
    EXPECT_STREQ("workSuccess: Bookeeping issue detected; server's count "
            "of numUpdatingServers just went negative. Not total failure "
            "but will cause the updater thread to spin even w/o work. "
            "Cause is mismatch # of getWork() and workSuccess/Failed() | "
            "workSuccess: Server 100.200 responded to a server list update "
            "but is no longer in the server list...",
            TestLog::get().c_str());
}

TEST_F(CoordinatorServerListTest, workSuccess_multipleInvokes) {
    ServerId id1 = sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 0,
            "mock:host=server1");
    ASSERT_TRUE(sl->getWork(&rpc));

    TestLog::Enable _;
    sl->workSuccess(id1, ~0lu);
    EXPECT_STREQ("workSuccess: ServerList Update Success: 1.0 update (0 => 1)",
                 TestLog::get().c_str());

    // Second call triggers warning
    TestLog::reset();
    sl->numUpdatingServers++;
    sl->workSuccess(id1, ~0lu);
    EXPECT_STREQ("workSuccess: Invoked for server 1.0 even though either "
            "no update was sent out or it has already been invoked. "
            "Possible race/bookeeping issue.",
                TestLog::get().c_str());

    // Everything should be up to date from the POV of the CSL.
    EXPECT_FALSE(sl->getWork(&rpc));
}

TEST_F(CoordinatorServerListTest, workSuccess_updateVersions) {
    ServerId id1 = sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 0,
            "mock:host=server1");
    CoordinatorServerList::Entry* e1 = sl->getEntry(id1);
    e1->verifiedVersion = 20;
    e1->updateVersion = 25;
    sl->numUpdatingServers = 3;

    TestLog::Enable _;
    sl->workSuccess(id1, 22u);
    EXPECT_STREQ("workSuccess: ServerList Update Success: 1.0 update "
            "(20 => 25)", TestLog::get().c_str());
    EXPECT_EQ(22lu, e1->verifiedVersion);
    EXPECT_EQ(22lu, e1->updateVersion);
}

TEST_F(CoordinatorServerListTest, workSuccess_newVersionTooLow) {
    ServerId id1 = sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 0,
            "mock:host=server1");
    CoordinatorServerList::Entry* e = sl->getEntry(id1);
    e->verifiedVersion = 20;
    e->updateVersion = 25;
    sl->numUpdatingServers = 3;
    sl->maxConfirmedVersion = 18;
    string message("no exception");
    try {
        sl->workSuccess(id1, 17);
    }
    catch (FatalError& error) {
        message = error.message;
    }
    EXPECT_EQ("Server list for server 1.0 is so far out of date that we "
            "can't fix it (its version: 17, maxConfirmedVersion: 18)",
            message);
    EXPECT_EQ(17lu, e->verifiedVersion);
}

TEST_F(CoordinatorServerListTest, workFailed) {
    ServerId id1 = sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 0,
            "mock:host=server1");
    CoordinatorServerList::Entry* e = sl->getEntry(id1);
    ASSERT_TRUE(sl->getWork(&rpc));
    EXPECT_EQ(id1, rpc->id);
    EXPECT_EQ(UNINITIALIZED_VERSION, e->verifiedVersion);
    EXPECT_EQ(sl->version, e->updateVersion);

    TestLog::Enable _;
    sl->workFailed(id1);
    EXPECT_STREQ("workFailed: ServerList Update Failed : 1.0 update (0 => 0)",
            TestLog::get().c_str());

    EXPECT_EQ(UNINITIALIZED_VERSION, e->verifiedVersion);
    EXPECT_EQ(UNINITIALIZED_VERSION, e->updateVersion);

    // Work should be available again
    EXPECT_TRUE(sl->getWork(&rpc));
    EXPECT_EQ(id1, rpc->id);
}

TEST_F(CoordinatorServerListTest, workFailed_multipleInvokes) {

    ServerId id1 = sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 0,
            "mock:host=server1");

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
    sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 0,
            "mock:host=server1");
    sl->enlistServer({WireFormat::MEMBERSHIP_SERVICE}, 0,
            "mock:host=server2");
    list1 = sl->updates.front().incremental;
    list2 = sl->updates.back().incremental;

    CoordinatorServerList::UpdateServerListRpc rpc(&context, {}, &list1);
    rpc.appendServerList(&list2);
    EXPECT_EQ("opcode: UPDATE_SERVER_LIST, "
            "protobuf: server { services: 16 server_id: 1 "
            "service_locator: \"mock:host=server1\" "
            "expected_read_mbytes_per_sec: 0 status: 0 replication_id: 0 } "
            "version_number: 1 type: UPDATE, "
            "protobuf: server { services: 16 server_id: 2 "
            "service_locator: \"mock:host=server2\" "
            "expected_read_mbytes_per_sec: 0 status: 0 replication_id: 0 } "
            "version_number: 2 type: UPDATE",
            parseUpdateRequest(&rpc.request));
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
    entry.serialize(&serialEntry);
    auto backupMask = ServiceMask{WireFormat::BACKUP_SERVICE}.serialize();
    EXPECT_EQ(backupMask, serialEntry.services());
    EXPECT_EQ(ServerId(5234, 23482).getId(), serialEntry.server_id());
    EXPECT_EQ("giggity", serialEntry.service_locator());
    EXPECT_EQ(723U, serialEntry.expected_read_mbytes_per_sec());
    EXPECT_EQ(ServerStatus::UP, ServerStatus(serialEntry.status()));

    entry.services = ServiceMask{WireFormat::MASTER_SERVICE};
    ProtoBuf::ServerList_Entry serialEntry2;
    entry.serialize(&serialEntry2);
    auto masterMask = ServiceMask{WireFormat::MASTER_SERVICE}.serialize();
    EXPECT_EQ(masterMask, serialEntry2.services());
    EXPECT_EQ(0U, serialEntry2.expected_read_mbytes_per_sec());
    EXPECT_EQ(ServerStatus::UP, ServerStatus(serialEntry2.status()));
}

TEST_F(CoordinatorServerListTest, Entry_sync) {
    sl->serverList.resize(4);
    CoordinatorServerList::Entry* entry = initServer(ServerId(2, 3), "server1",
            {WireFormat::BACKUP_SERVICE, WireFormat::MASTER_SERVICE},
            200, ServerStatus::REMOVE);

    // Add some updates to this entry.
    entry->pendingUpdates.push_back(createUpdate(ServerStatus::UP, 12, 144));
    entry->pendingUpdates.push_back(createUpdate(ServerStatus::CRASHED,
            17, 198));

    TestLog::reset();
    cluster.externalStorage.log.clear();
    entry->sync(&cluster.externalStorage);
    EXPECT_EQ("set(UPDATE, servers/2)", cluster.externalStorage.log);
    ProtoBuf::ServerListEntry entryPb;
    EXPECT_TRUE(entryPb.ParseFromString(cluster.externalStorage.setData));
    EXPECT_EQ("services: 3 server_id: 12884901890 "
            "service_locator: \"server1\" expected_read_mbytes_per_sec: 200 "
            "status: 2 replication_id: 12345 "
            "master_recovery_info { min_open_segment_id: 4 "
            "min_open_segment_epoch: 17 } "
            "update { status: 0 version: 12 sequence_number: 144 } "
            "update { status: 1 version: 17 sequence_number: 198 }",
            entryPb.ShortDebugString());
}

} // namespace RAMCloud
