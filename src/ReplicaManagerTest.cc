/* Copyright (c) 2009-2014 Stanford University
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
#include "Key.h"

namespace RAMCloud {

struct ReplicaManagerTest : public ::testing::Test {
    TestLog::Enable logEnabler;
    Context context;
    ServerList serverList;
    MockCluster cluster;
    const uint32_t segmentSize;
    Tub<ReplicaManager> mgr;
    ServerId serverId;
    ServerId backup1Id;
    ServerId backup2Id;

    ReplicaManagerTest()
        : logEnabler()
        , context()
        , serverList(&context)
        , cluster(&context)
        , segmentSize(1 << 16)
        , mgr()
        , serverId(99, 0)
        , backup1Id()
        , backup2Id()
    {
        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::BACKUP_SERVICE,
                           WireFormat::MEMBERSHIP_SERVICE};
        config.segmentSize = segmentSize;
        config.backup.numSegmentFrames = 4;
        config.localLocator = "mock:host=backup1";
        backup1Id = addToServerList(cluster.addServer(config));

        config.localLocator = "mock:host=backup2";
        backup2Id = addToServerList(cluster.addServer(config));

        // Get the ServerId _before_ it's used. We don't pass by reference
        // anymore.
        serverId = CoordinatorClient::enlistServer(&context, {},
            {WireFormat::MASTER_SERVICE}, "", 0);
        mgr.construct(&context, &serverId, 2, false, false);
        cluster.coordinatorContext.coordinatorServerList->sync();
    }

    ServerId addToServerList(Server* server)
    {
        serverList.testingAdd({server->serverId,
                               server->config.localLocator,
                               server->config.services,
                               server->config.backup.mockSpeed,
                               ServerStatus::UP});
        return server->serverId;
    }

};

TEST_F(ReplicaManagerTest, isReplicaNeeded) {
    ServerDetails server;
    ServerChangeEvent event;

    // Is needed if we've never heard of the calling backup.
    // It'll try later when we've found out about it from the coordinator
    // or it'll die and the next backup server that comes up will take care
    // of it.
    EXPECT_TRUE(mgr->isReplicaNeeded({5, 0}, 99));

    // Is not needed if we know about the backup (and hence the crashes of any
    // of its predecessors and we have no record of this segment.
    serverList.testingAdd({{5, 0}, "mock:host=backup1",
                           {WireFormat::BACKUP_SERVICE}, 100,
                           ServerStatus::UP});
    while (mgr->failureMonitor.tracker.getChange(server, event));
    EXPECT_FALSE(mgr->isReplicaNeeded({5, 0}, 99));

    // Is needed if we know the calling backup has crashed; the successor
    // backup will take care of garbage collection.
    serverList.testingCrashed({5, 0});
    while (mgr->failureMonitor.tracker.getChange(server, event));
    EXPECT_TRUE(mgr->isReplicaNeeded({5, 0}, 99));
}

TEST_F(ReplicaManagerTest, allocateHead) {
    MockRandom _(1);
    char data[] = "Hello world!";

    Segment priorSeg(data, arrayLength(data));
    auto prior = mgr->allocateHead(87, &priorSeg, NULL);
    Segment seg(data, arrayLength(data));
    auto segment = mgr->allocateHead(88, &seg, prior);

    ASSERT_FALSE(mgr->taskQueue.isIdle());
    EXPECT_EQ(2u, mgr->replicatedSegmentList.size());
    EXPECT_EQ(segment, &mgr->replicatedSegmentList.back());
    EXPECT_EQ(segment, prior->followingSegment);
}

TEST_F(ReplicaManagerTest, allocateNonHead) {
    MockRandom _(1);
    char data[] = "Hello world!";

    Segment seg(data, arrayLength(data));
    auto segment = mgr->allocateHead(88, &seg, NULL);

    ASSERT_FALSE(mgr->taskQueue.isIdle());
    EXPECT_EQ(segment, mgr->taskQueue.tasks.front());
    EXPECT_EQ(1u, mgr->replicatedSegmentList.size());
    EXPECT_EQ(segment, &mgr->replicatedSegmentList.front());

    segment->sync(segment->queued.bytes);

    // make sure we think data was written
    EXPECT_EQ(&seg, segment->segment);
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
}

// This is a test that really belongs in SegmentTest.cc, but the setup
// overhead is too high.
TEST_F(ReplicaManagerTest, writeSegment) {
    Buffer buffer;
    Segment s;

    SegmentHeader header = { *serverId, 88, segmentSize };
    buffer.appendExternal(&header, sizeof(header));
    s.append(LOG_ENTRY_TYPE_SEGHEADER, buffer);

    Key key(123, "10", 2);
    Buffer dataBuffer;
    Object object(key, NULL, 0, 0, 0, dataBuffer);

    buffer.reset();
    object.assembleForLog(buffer);
    s.append(LOG_ENTRY_TYPE_OBJ, buffer);
    s.close();

    ReplicatedSegment* rs = mgr->allocateHead(88, &s, NULL);
    rs->close();
    rs->sync(s.head);

    EXPECT_EQ(1U, mgr->replicatedSegmentList.size());
    ProtoBuf::RecoveryPartition will;
    ProtoBuf::Tablets::Tablet& tablet(*will.add_tablet());
    tablet.set_table_id(123);
    KeyHash keyHash = Key::getHash(123, "10", 2);
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
            BackupClient::startReadingData(&context, replica.backupId, 456lu,
                                           serverId);
            BackupClient::StartPartitioningReplicas(&context, replica.backupId,
                    456lu, serverId, &will);
            Segment::Certificate certificate =
                BackupClient::getRecoveryData(&context, replica.backupId, 456lu,
                                              serverId, 88, 0, &resp);
            ASSERT_NE(0U, resp.totalLength);

            SegmentIterator it(resp.getRange(0, resp.size()),
                               resp.size(), certificate);
            EXPECT_FALSE(it.isDone());
            EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());

            Buffer buffer;
            it.appendToBuffer(buffer);
            Object object(buffer);
            EXPECT_EQ("10", TestUtil::toString(object.getKey(),
                                               object.getKeyLength()));
            EXPECT_EQ(123U, object.getTableId());
        }
    }
}

TEST_F(ReplicaManagerTest, proceed) {
    Segment seg;
    mgr->allocateHead(89, &seg, NULL)->close();
    auto& segment = mgr->replicatedSegmentList.front();
    EXPECT_FALSE(segment.replicas[0].isActive);
    mgr->proceed();
    ASSERT_TRUE(segment.replicas[0].isActive);
    EXPECT_TRUE(segment.replicas[0].writeRpc);
}

namespace {
bool handleBackupFailureFilter(string s) {
    return s == "handleBackupFailure";
}
}

TEST_F(ReplicaManagerTest, handleBackupFailure) {
    Segment seg1;
    Segment seg2;
    Segment seg3;
    auto s1 = mgr->allocateHead(89, &seg1, NULL);
    auto s2 = mgr->allocateHead(90, &seg2, s1);
    s1->close();
    auto s3 = mgr->allocateHead(91, &seg3, s2);
    s2->close();
    while (!s2->getCommitted().close || !s3->getCommitted().open) {
        mgr->proceed();
    }
    TestLog::Enable _(handleBackupFailureFilter);
    mgr->handleBackupFailure(backup1Id);
    EXPECT_EQ(
        "handleBackupFailure: Handling backup failure of serverId 1.0 | "
        "handleBackupFailure: Segment 89 recovering from lost replica which "
            "was on backup 1.0 | "
        "handleBackupFailure: Segment 90 recovering from lost replica which "
            "was on backup 1.0 | "
        "handleBackupFailure: Segment 91 recovering from lost replica which "
            "was on backup 1.0 | "
        "handleBackupFailure: Lost replica(s) for segment 91 while open due "
            "to crash of backup 1.0",
        TestLog::get());
}

TEST_F(ReplicaManagerTest, destroyAndFreeReplicatedSegment) {
    Segment seg;
    auto* segment = mgr->allocateHead(89, &seg, NULL);
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
                       , "allocateSegment"
                       , "write"
                       , "close"
                       , "selectPrimary"
                       , "performWrite"
                       , "writeSegment"
                       , "performTask"
                       , "updateToAtLeast"
                       };
    foreach (auto* oks, ok) {
        if (s == oks)
            return true;
    }
    return false;
}
}

class DoNothingHandlers : public LogEntryHandlers {
  public:
    uint32_t getTimestamp(LogEntryType type, Buffer& buffer) { return 0; }
    void relocate(LogEntryType type,
                  Buffer& oldBuffer,
                  Log::Reference oldReference,
                  LogEntryRelocator& relocator) { }
};

/**
 * Not a test for a specific method, rather a more complete test of the
 * entire backup recovery system.  If this test gets too difficult to
 * maintain then it can be simplified to check a few simple properties
 * of the recovery, but as it is it is helpful since it lays out a
 * timeline of recovery whereas the code for recovery is non-linear.
 */
TEST_F(ReplicaManagerTest, endToEndBackupRecovery) {
    MockRandom __(1);

    ServerConfig serverConfig(ServerConfig::forTesting());
    SegletAllocator allocator(&serverConfig);
    MasterTableMetadata mtm;
    SegmentManager segmentManager(&context, &serverConfig,
                                  &serverId, allocator, *mgr, &mtm);
    DoNothingHandlers entryHandlers;
    Log log(&context, &serverConfig, &entryHandlers, &segmentManager, &*mgr);
    log.sync();

    // Set up the scenario:
    // Two log segments in the log, one durably closed and the other open
    // with a single pending write.
    log.rollHeadOver();
    static char buf[64];
    log.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
    log.sync();
    // Non-synced append ensures mgr isn't idle, otherwise this unit test would
    // be racy: it waits for mgr->isIdle() to ensure recovery is complete, but
    // if the wait occurs before the failure notification gets processed this
    // will be trivially true.
    log.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf));
    log.head->replicatedSegment->schedule();
    ASSERT_FALSE(mgr->isIdle());
    ASSERT_EQ(2u, log.head->id);

    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::BACKUP_SERVICE,
                       WireFormat::MEMBERSHIP_SERVICE};
    config.segmentSize = segmentSize;
    config.backup.numSegmentFrames = 4;
    config.localLocator = "mock:host=backup3";
    ServerId backup3Id = addToServerList(cluster.addServer(config));

    EXPECT_FALSE(mgr->isIdle());

    TestLog::Enable _(filter);
    BackupFailureMonitor failureMonitor(&context, mgr.get());
    failureMonitor.start();
    serverList.testingCrashed(backup1Id);
    serverList.testingRemove(backup1Id);

    // Wait for backup recovery to finish.
    while (!mgr->isIdle());

    // Though extremely fragile this gives a great sanity check on the order
    // of things during backup recovery which is exceptionally helpful since
    // the ordering doesn't follow the code flow at all. Comments are added
    // below to point out some of the important properties this is checking.
    // Notice the log segments involved here are Segment 1 which is closed,
    // Segment 2 which is currently the head.

    EXPECT_EQ(
        // The failure is discovered through the tracker, notice that
        // the completion of the test is also checking to make sure that
        // BackupFailureMonitor is driving the ReplicaManager proceed()
        // loop until recovery has completed.
        "main: Notifying replica manager of failure of serverId 1.0 | "
        // Next few, ensure open/closed segments get tagged appropriately
        // since recovery is different for each.
        "handleBackupFailure: Handling backup failure of serverId 1.0 | "
        "handleBackupFailure: Segment 1 recovering from lost replica which "
            "was on backup 1.0 | "
        "handleBackupFailure: Segment 2 recovering from lost replica which "
            "was on backup 1.0 | "
        "handleBackupFailure: Lost replica(s) for segment 2 while open due "
            "to crash of backup 1.0 | "
        // Segment 2 goes first; since it stayed open it was always scheduled.
        // Replica slot 0 just needs an updated epoch, so that is sent out.
        "performWrite: Sending write to backup 2.0 | "
        // Replica slot 1 was the replica that was lost. Start re-replication.
        "performWrite: Starting replication of segment 2 replica slot 1 on "
            "backup 4.0 | "
        "performWrite: Sending open to backup 4.0 | "
        "writeSegment: Opening <3.0,2> | "
        // Segment 1 goes second because it was happily durable and descheduled
        // until the failure woke it up.
        "selectPrimary: Chose server 4.0 with 0 primary replicas and 100 MB/s "
            "disk bandwidth (expected time to read on recovery is 80 ms) | "
        "performWrite: Starting replication of segment 1 replica slot 0 on "
            "backup 4.0 | "
        "performWrite: Sending open to backup 4.0 | "
        "writeSegment: Opening <3.0,1> | "
        "performWrite: Write RPC finished for replica slot 0 | "
        "performWrite: Write RPC finished for replica slot 1 | "
        "performWrite: Write RPC finished for replica slot 0 | "
        // Write to re-replicate segment 2 replica slot 1.
        "performWrite: Sending write to backup 4.0 | "
        // Write to re-replicate segment 1 replica slot 0 and close it.
        "performWrite: Sending write to backup 4.0 | "
        "writeSegment: Closing <3.0,1> | "
        "performWrite: Write RPC finished for replica slot 1 | "
        // All re-replication has been taken care of; bump the epoch number
        // on the coordinator.
        "performTask: Updating replicationEpoch to 2,1 on coordinator to "
            "ensure lost replicas will not be reused | "
        "updateToAtLeast: request update to master recovery info for 3.0 "
            "to 2,1 | "
        "performWrite: Write RPC finished for replica slot 0 | "
        "performTask: Updating replicationEpoch to 2,1 on coordinator to "
            "ensure lost replicas will not be reused | "
        "updateToAtLeast: request update to master recovery info for 3.0 "
            "to 2,1 | "
        "performTask: Updating replicationEpoch to 2,1 on coordinator to "
            "ensure lost replicas will not be reused | "
        "updateToAtLeast: request update to master recovery info for 3.0 "
            "to 2,1 | "
        "performTask: coordinator replication epoch for 3.0 updated to 2,1 | "
        "performTask: replicationEpoch ok, lost open replica recovery "
            "complete on segment 2"
        , TestLog::get());

    // Make sure the replication epoch on the coordinator was updated.
    EXPECT_EQ(2u,
        cluster.coordinator->context->coordinatorServerList->operator[](
        serverId).masterRecoveryInfo.min_open_segment_id());
    EXPECT_EQ(1u,
        cluster.coordinator->context->coordinatorServerList->operator[](
        serverId).masterRecoveryInfo.min_open_segment_epoch());
}

} // namespace RAMCloud
