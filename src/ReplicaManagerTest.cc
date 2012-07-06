/* Copyright (c) 2009-2012 Stanford University
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
#include "MockCluster.h"
#include "Memory.h"
#include "ReplicaManager.h"
#include "Segment.h"
#include "ShortMacros.h"
#include "KeyHash.h"

namespace RAMCloud {

struct ReplicaManagerTest : public ::testing::Test {
    Context context;
    MockCluster cluster;
    const uint32_t segmentSize;
    Tub<ReplicaManager> mgr;
    ServerId serverId;
    ServerId backup1Id;
    ServerId backup2Id;
    ServerList serverList;

    ReplicaManagerTest()
        : context()
        , cluster(context)
        , segmentSize(1 << 16)
        , mgr()
        , serverId(99, 0)
        , backup1Id()
        , backup2Id()
        , serverList(context)
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        ServerConfig config = ServerConfig::forTesting();
        config.services = {BACKUP_SERVICE, MEMBERSHIP_SERVICE};
        config.segmentSize = segmentSize;
        config.backup.numSegmentFrames = 4;
        config.localLocator = "mock:host=backup1";
        backup1Id = addToServerList(cluster.addServer(config));

        config.localLocator = "mock:host=backup2";
        backup2Id = addToServerList(cluster.addServer(config));

        mgr.construct(context, serverList, serverId, 2,
                      &cluster.coordinatorLocator);
        serverId = CoordinatorClient::enlistServer(context, {},
                                                   {MASTER_SERVICE},
                                                   "", 0 , 0);
    }

    ServerId addToServerList(Server* server)
    {
        serverList.add(server->serverId,
                       server->config.localLocator,
                       server->config.services,
                       server->config.backup.mockSpeed);
        return server->serverId;
    }

    void dumpReplicatedSegments()
    {
        LOG(ERROR, "%lu open segments:", mgr->replicatedSegmentList.size());
        foreach (auto& segment, mgr->replicatedSegmentList) {
            LOG(ERROR, "Segment %lu", segment.segmentId);
            LOG(ERROR, "  data: %p", segment.data);
            LOG(ERROR, "  openLen: %u", segment.openLen);
            LOG(ERROR, "  queued.bytes: %u", segment.queued.bytes);
            LOG(ERROR, "  queued.close: %u", segment.queued.close);
            foreach (auto& replica, segment.replicas) {
                LOG(ERROR, "  Replica:%s",
                    replica.isActive ? "" : " non-existent");
                if (!replica.isActive)
                    continue;
                LOG(ERROR, "    acked.open: %u", replica.acked.open);
                LOG(ERROR, "    sent.bytes: %u", replica.sent.bytes);
                LOG(ERROR, "    sent.close: %u", replica.sent.close);
                LOG(ERROR, "    Write RPC: %s", replica.writeRpc ?
                                                    "active" : "inactive");
                LOG(ERROR, "    Free RPC: %s", replica.freeRpc ?
                                                    "active" : "inactive");
            }
            LOG(ERROR, " ");
        }
    }
};

TEST_F(ReplicaManagerTest, isReplicaNeeded) {
    ServerDetails server;
    ServerChangeEvent event;

    // Is needed if we've never heard of the calling backup.
    // It'll try later when we've found out about it from the coordinator
    // or it'll die and the next backup server that comes up will take care
    // of it.
    EXPECT_TRUE(mgr->isReplicaNeeded({2, 0}, 99));

    // Is not needed if we know about the backup (and hence the crashes of any
    // of its predecessors and we have no record of this segment.
    serverList.add({2, 0}, "mock:host=backup1", {BACKUP_SERVICE}, 100);
    while (mgr->failureMonitor.tracker.getChange(server, event));
    EXPECT_FALSE(mgr->isReplicaNeeded({2, 0}, 99));

    // Is needed if we know the calling backup has crashed; the successor
    // backup will take care of garbage collection.
    serverList.crashed({2, 0}, "mock:host=backup1", {BACKUP_SERVICE}, 100);
    while (mgr->failureMonitor.tracker.getChange(server, event));
    EXPECT_TRUE(mgr->isReplicaNeeded({2, 0}, 99));
}

TEST_F(ReplicaManagerTest, openSegment) {
    MockRandom _(1);
    const char data[] = "Hello world!";

    auto segment = mgr->openSegment(true, 88, data, arrayLength(data));

    ASSERT_FALSE(mgr->taskQueue.isIdle());
    EXPECT_EQ(segment, mgr->taskQueue.tasks.front());
    EXPECT_EQ(1u, mgr->replicatedSegmentList.size());
    EXPECT_EQ(segment, &mgr->replicatedSegmentList.front());

    segment->sync(segment->queued.bytes);

    // make sure we think data was written
    EXPECT_EQ(data, segment->data);
    EXPECT_EQ(arrayLength(data), segment->queued.bytes);
    EXPECT_FALSE(segment->queued.close);
    foreach (auto& replica, segment->replicas) {
        EXPECT_EQ(arrayLength(data), replica.sent.bytes);
        EXPECT_FALSE(replica.sent.close);
        EXPECT_FALSE(replica.writeRpc);
        EXPECT_FALSE(replica.freeRpc);
    }
    EXPECT_EQ(arrayLength(data), cluster.servers[0]->backup->bytesWritten);
    EXPECT_EQ(arrayLength(data), cluster.servers[1]->backup->bytesWritten);

    // make sure ReplicatedSegment::replicas point to ok service locators
    vector<string> backupLocators;
    foreach (auto& replica, segment->replicas) {
        backupLocators.push_back(
            replica.client->getSession()->getServiceLocator());
    }
    EXPECT_EQ((vector<string> {"mock:host=backup1", "mock:host=backup2"}),
              backupLocators);
}

// This is a test that really belongs in SegmentTest.cc, but the setup
// overhead is too high.
TEST_F(ReplicaManagerTest, writeSegment) {
    void* segMem = Memory::xmemalign(HERE, segmentSize, segmentSize);
    Segment seg(*serverId, 88, segMem, segmentSize, mgr.get());
    SegmentHeader header = { *serverId, 88, segmentSize,
        Segment::INVALID_SEGMENT_ID };
    seg.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header));

    DECLARE_OBJECT(object, 2, 0);
    object->tableId = 123;
    object->keyLength = 2;
    object->version = 0;
    memcpy(object->getKeyLocation(), "10", 2);
    seg.append(LOG_ENTRY_TYPE_OBJ, object, object->objectLength(0));
    seg.close(NULL);

    EXPECT_EQ(1U, mgr->replicatedSegmentList.size());

    ProtoBuf::Tablets will;
    ProtoBuf::Tablets::Tablet& tablet(*will.add_tablet());
    tablet.set_table_id(123);
    HashType keyHash = getKeyHash("10", 2);
    tablet.set_start_key_hash(keyHash);
    tablet.set_end_key_hash(keyHash);

    tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
    tablet.set_user_data(0); // partition id
    tablet.set_ctime_log_head_id(0);
    tablet.set_ctime_log_head_offset(0);

    foreach (auto& segment, mgr->replicatedSegmentList) {
        foreach (auto& replica, segment.replicas) {
            ASSERT_TRUE(replica.isActive);
            Buffer resp;
            replica.client->startReadingData(serverId, will);
            while (true) {
                try {
                    replica.client->getRecoveryData(serverId, 88, 0, resp);
                } catch (const RetryException& e) {
                    resp.reset();
                    continue;
                }
                break;
            }
            ASSERT_NE(0U, resp.totalLength);
            auto* entry = resp.getStart<SegmentEntry>();
            EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, entry->type);
            EXPECT_EQ(sizeof(Object) + 2, entry->length);
            resp.truncateFront(sizeof(*entry));
            auto* obj = resp.getStart<Object>();
            EXPECT_EQ("10", TestUtil::toString(obj->getKey(), obj->keyLength));
            EXPECT_EQ(123U, obj->tableId);
        }
    }
    free(segMem);
}

TEST_F(ReplicaManagerTest, proceed) {
    mgr->openSegment(true, 89, NULL, 0)->close(NULL);
    auto& segment = mgr->replicatedSegmentList.front();
    EXPECT_FALSE(segment.replicas[0].isActive);
    mgr->proceed();
    ASSERT_TRUE(segment.replicas[0].isActive);
    EXPECT_TRUE(segment.replicas[0].writeRpc);
}

TEST_F(ReplicaManagerTest, handleBackupFailure) {
    auto s1 = mgr->openSegment(true, 89, NULL, 0);
    auto s2 = mgr->openSegment(true, 90, NULL, 0);
    auto s3 = mgr->openSegment(true, 91, NULL, 0);
    mgr->proceed();
    mgr->proceed();
    mgr->proceed();
    Tub<uint64_t> segmentId = mgr->handleBackupFailure(backup1Id);
    ASSERT_TRUE(segmentId);
    EXPECT_EQ(91u, *segmentId);
    auto s4 = mgr->openSegment(true, 92, NULL, 0);
    // If we don't meet the dependencies the backup will refuse
    // to tear-down as it tries to make sure there is no data loss.
    s1->close(s2);
    s2->close(s3);
    s3->close(s4);
}

TEST_F(ReplicaManagerTest, destroyAndFreeReplicatedSegment) {
    auto* segment = mgr->openSegment(true, 89, NULL, 0);
    segment->sync(0);
    while (!mgr->taskQueue.isIdle())
        mgr->proceed(); // Make sure the close gets pushed out as well.
    EXPECT_FALSE(mgr->replicatedSegmentList.empty());
    EXPECT_EQ(segment, &mgr->replicatedSegmentList.front());
    mgr->destroyAndFreeReplicatedSegment(segment);
    EXPECT_TRUE(mgr->replicatedSegmentList.empty());
}

namespace {
bool filter(string s) {
    // Mostly to filter out non-deterministic storage stuff on the backup.
    const char* ok[] = { "main"
                       , "handleBackupFailure"
                       , "openSegment"
                       , "write"
                       , "close"
                       , "selectPrimary"
                       , "performWrite"
                       , "writeSegment"
                       , "performTask"
                       , "updateToAtLeast"
                       , "setMinOpenSegmentId"
                       };
    foreach (auto* oks, ok)
        if (s == oks)
            return true;
    return false;
}
}

/**
 * Not a test for a specific method, rather a more complete test of the
 * entire backup recovery system.  If this test gets too difficult to
 * maintain then it can be simplified to check a few simple properties
 * of the recovery, but as it is it is helpful since it lays out a
 * timeline of recovery whereas the code for recovery is non-linear.
 */
TEST_F(ReplicaManagerTest, endToEndBackupRecovery) {
    MockRandom __(1);
    const uint64_t logSegs = 4;
    Context context;
    Log log(context, serverId, logSegs * 8192, 8192, 4298,
            mgr.get(), Log::CLEANER_DISABLED);
    log.registerType(LOG_ENTRY_TYPE_OBJ, true, NULL, NULL,
                    NULL, NULL, NULL);
    // Set up the scenario:
    // Two log segments in the log, one durably closed and the other open
    // with a single pending write.
    log.allocateHead(); // First log head, will be closed during recovery.
    log.allocateHead(); // Second log head, will be open during recovery.
    static char buf[64];
    // Sync append to new head ensures that the first log segment must be
    // durably closed.
    log.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf), true);
    // Non-synced append ensures mgr isn't idle, otherwise this unit test would
    // be racy: it waits for mgr->isIdle() to ensure recovery is complete, but
    // if the wait occurs before the failure notification gets processed this
    // will be trivially true.
    log.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf), false);
    ASSERT_FALSE(mgr->isIdle());
    ASSERT_EQ(1u, log.head->getId());

    ServerConfig config = ServerConfig::forTesting();
    config.services = {BACKUP_SERVICE, MEMBERSHIP_SERVICE};
    config.segmentSize = segmentSize;
    config.backup.numSegmentFrames = 4;
    config.localLocator = "mock:host=backup3";
    ServerId backup3Id = addToServerList(cluster.addServer(config));

    EXPECT_FALSE(mgr->isIdle());

    TestLog::Enable _(filter);
    BackupFailureMonitor failureMonitor(context, serverList, mgr.get());
    failureMonitor.start(&log);
    serverList.remove(backup1Id);

    // Wait for backup recovery to finish.
    while (!mgr->isIdle());

    // Though extremely fragile this gives a great sanity check on the order
    // of things during backup recovery which is exceptionally helpful since
    // the ordering doesn't follow the code flow at all.  Comments are added
    // below to point out some of the important properties this is checking.
    // Notice the log segments involved here are Segment 0 which is closed,
    // Segment 1 which is currently the head, and Segment 2 which will be
    // the successor for Segment 1 which is created as part of the recovery
    // protocol.
    EXPECT_EQ(
        // The failure is discovered through the tracker, notice that
        // the completion of the test is also checking to make sure that
        // BackupFailureMonitor is driving the ReplicaManager proceed()
        // loop until recovery has completed.
        "main: Notifying log of failure of serverId 1 | "
        // Next few, ensure open/closed segments get tagged appropriately
        // since recovery is different for each.
        "handleBackupFailure: Handling backup failure of serverId 1 | "
        "handleBackupFailure: Segment 0 recovering from lost replica | "
        "handleBackupFailure: Segment 1 recovering from lost replica | "
        "handleBackupFailure: Lost replica(s) for segment 1 while open | "
        "handleBackupFailure: Highest affected segmentId 1 | "
        // Ensure a new log head is allocated.
        "main: Allocating a new log head | "
        // Which provides the required new log digest via open.
        "openSegment: openSegment 3, 2, ..., 76 | "
        "write: 3, 1, 8192 | "
        "close: 3, 1, 2 | "
        // And which also provides the needed close on the log segment
        // with the lost open replica.
        "close: Segment 1 closed (length 8192) | "
        // Notice the actual close to the backup is delayed because it
        // must wait on a durable open to the new log head.
        "performWrite: Cannot close segment 1 until following segment is "
            "durably open | "
        // Re-replication of Segment 1's lost replica.
        "performWrite: Starting replication on backup 4 | "
        "performWrite: Sending open to backup 4 | "
        "writeSegment: Opening <3,1> | "
        // Re-replication of Segment 0's lost replica.
        "selectPrimary: Chose server 4 with 0 primary replicas and 100 MB/s "
            "disk bandwidth (expected time to read on recovery is 80 ms) | "
        "performWrite: Starting replication on backup 4 | "
        "performWrite: Sending open to backup 4 | "
        "writeSegment: Opening <3,0> | "
        // Starting replication of new log head Segment 2.
        "selectPrimary: Chose server 2 with 1 primary replicas and 100 MB/s "
            "disk bandwidth (expected time to read on recovery is 160 ms) | "
        "performWrite: Starting replication on backup 2 | "
        "performWrite: Sending open to backup 2 | "
        "writeSegment: Opening <3,2> | "
        "performWrite: Starting replication on backup 4 | "
        "performWrite: Sending open to backup 4 | "
        "writeSegment: Opening <3,2> | "
        // Segment 1 is still waiting for the RPC for the open for Segment 2
        // to be reaped to ensure that it is durably open.
        "performWrite: Cannot close segment 1 until following segment is "
            "durably open | "
        // This part is a bit ambiguous:
        // Segment 2 is durably open now and its rpcs get reaped, so
        // That frees Segment 1 to finally send it's closing write which
        // accounts for 2 rpcs, one to each of its replicas.  The other
        // write is to finish the replication of segment 0.
        "performWrite: Sending write to backup 2 | "
        "performWrite: Sending write to backup 4 | "
        "performWrite: Sending write to backup 4 | "
        // Update minOpenSegmentId on the coordinator to complete the lost
        // open segment recovery protocol.
        // Happens a few times because the code just polls to keep updating
        // until it reads the right value has been acknowledged.  Only one
        // RPC is sent, though.
        "performTask: Updating minOpenSegmentId on coordinator to ensure lost "
            "replicas of segment 1 will not be reused | "
        "updateToAtLeast: request update to minOpenSegmentId for 3 to 2 | "
        "setMinOpenSegmentId: setMinOpenSegmentId for server 3 to 2 | "
        "performTask: Updating minOpenSegmentId on coordinator to ensure lost "
            "replicas of segment 1 will not be reused | "
        "updateToAtLeast: request update to minOpenSegmentId for 3 to 2 | "
        "performTask: coordinator minOpenSegmentId for 3 updated to 2 | "
        "performTask: minOpenSegmentId ok, lost open replica recovery "
            "complete on segment 1"
        , TestLog::get());

    // Make sure it rolled over to a new log head.
    EXPECT_EQ(2u, log.head->getId());
    // Make sure the minOpenSegmentId was updated.
    EXPECT_EQ(2u, cluster.coordinator->serverList[serverId].minOpenSegmentId);
}

} // namespace RAMCloud
