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
#include "Key.h"

namespace RAMCloud {

struct ReplicaManagerTest : public ::testing::Test {
    Context context;
    ServerList serverList;
    MockCluster cluster;
    const uint32_t segmentSize;
    Tub<ReplicaManager> mgr;
    ServerId serverId;
    ServerId backup1Id;
    ServerId backup2Id;

    ReplicaManagerTest()
        : context()
        , serverList(&context)
        , cluster(&context)
        , segmentSize(1 << 16)
        , mgr()
        , serverId(99, 0)
        , backup1Id()
        , backup2Id()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::BACKUP_SERVICE,
                           WireFormat::MEMBERSHIP_SERVICE};
        config.segmentSize = segmentSize;
        config.backup.numSegmentFrames = 4;
        config.localLocator = "mock:host=backup1";
        backup1Id = addToServerList(cluster.addServer(config));

        config.localLocator = "mock:host=backup2";
        backup2Id = addToServerList(cluster.addServer(config));

        mgr.construct(&context, serverId, 2);
        serverId = CoordinatorClient::enlistServer(&context, {},
            {WireFormat::MASTER_SERVICE}, "", 0);
    }

    ServerId addToServerList(Server* server)
    {
        serverList.add(server->serverId,
                       server->config.localLocator,
                       server->config.services,
                       server->config.backup.mockSpeed);
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
    EXPECT_TRUE(mgr->isReplicaNeeded({2, 0}, 99));

    // Is not needed if we know about the backup (and hence the crashes of any
    // of its predecessors and we have no record of this segment.
    serverList.add({2, 0}, "mock:host=backup1",
                   {WireFormat::BACKUP_SERVICE}, 100);
    while (mgr->failureMonitor.tracker.getChange(server, event));
    EXPECT_FALSE(mgr->isReplicaNeeded({2, 0}, 99));

    // Is needed if we know the calling backup has crashed; the successor
    // backup will take care of garbage collection.
    serverList.crashed({2, 0}, "mock:host=backup1",
                                {WireFormat::BACKUP_SERVICE}, 100);
    while (mgr->failureMonitor.tracker.getChange(server, event));
    EXPECT_TRUE(mgr->isReplicaNeeded({2, 0}, 99));
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

    SegmentHeader header = { *serverId, 88, segmentSize,
        Segment::INVALID_SEGMENT_ID };
    buffer.appendTo(&header, sizeof(header));
    s.append(LOG_ENTRY_TYPE_SEGHEADER, buffer);

    Key key(123, "10", 2);
    Object object(key, NULL, 0, 0, 0);
    buffer.reset();
    object.serializeToBuffer(buffer);
    s.append(LOG_ENTRY_TYPE_OBJ, buffer);
    s.close();

    ReplicatedSegment* rs = mgr->allocateHead(88, &s, NULL);
    rs->close();
    rs->sync(s.tail);

    EXPECT_EQ(1U, mgr->replicatedSegmentList.size());

    ProtoBuf::Tablets will;
    ProtoBuf::Tablets::Tablet& tablet(*will.add_tablet());
    tablet.set_table_id(123);
    HashType keyHash = Key::getHash(123, "10", 2);
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
            BackupClient::startReadingData(&context, replica.backupId,
                                           serverId, &will);
            Segment::Certificate certificate =
                BackupClient::getRecoveryData(&context, 0lu, replica.backupId,
                                              serverId, 88, 0, &resp);
            ASSERT_NE(0U, resp.totalLength);

            SegmentIterator it(resp.getRange(0, resp.getTotalLength()),
                               resp.getTotalLength(), certificate);
            EXPECT_FALSE(it.isDone());
            EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
            EXPECT_EQ(Object::getSerializedLength(2, 0), it.getLength());

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

TEST_F(ReplicaManagerTest, handleBackupFailure) {
    Segment seg1;
    Segment seg2;
    Segment seg3;
    auto s1 = mgr->allocateHead(89, &seg1, NULL);
    auto s2 = mgr->allocateHead(90, &seg2, s1);
    auto s3 = mgr->allocateHead(91, &seg3, s2);
    mgr->proceed();
    mgr->proceed();
    mgr->proceed();
    Tub<uint64_t> segmentId = mgr->handleBackupFailure(backup1Id);
    ASSERT_TRUE(segmentId);
    EXPECT_EQ(91u, *segmentId);
    Segment seg4;
    IGNORE_RESULT(mgr->allocateHead(92, &seg4, s3));
    // If we don't meet the dependencies the backup will refuse
    // to tear-down as it tries to make sure there is no data loss.
    s1->close();
    s2->close();
    s3->close();
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
                       , "setMinOpenSegmentId"
                       };
    foreach (auto* oks, ok) {
        if (s == oks)
            return true;
    }
    return false;
}
}

class DoNothingHandlers : public Log::EntryHandlers {
  public:
    uint32_t getTimestamp(LogEntryType type, Buffer& buffer) { return 0; }
    bool checkLiveness(LogEntryType type, Buffer& buffer) { return true; }
    bool relocate(LogEntryType type, Buffer& oldBuffer,
                  HashTable::Reference newReference) { return true; }
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

    const uint64_t logSegs = 4;
    SegmentManager::Allocator allocator(logSegs * 8192, 8192, 8192);
    SegmentManager segmentManager(&context, serverId, allocator, *mgr, 1.0);
    DoNothingHandlers entryHandlers;
    Log log(&context, entryHandlers, segmentManager, *mgr, true);
    log.sync();

    // Set up the scenario:
    // Two log segments in the log, one durably closed and the other open
    // with a single pending write. The first log head is already open and
    // will be closed during recovery. The second log head, will be open
    // during recovery.
    log.allocateHeadIfStillOn({});
    static char buf[64];
    // Sync append to new head ensures that the first log segment must be
    // durably closed.
    log.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf), true);
    // Non-synced append ensures mgr isn't idle, otherwise this unit test would
    // be racy: it waits for mgr->isIdle() to ensure recovery is complete, but
    // if the wait occurs before the failure notification gets processed this
    // will be trivially true.
    log.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf), false);
    log.head->replicatedSegment->schedule();
    ASSERT_FALSE(mgr->isIdle());
    ASSERT_EQ(1u, log.head->id);

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

    size_t curPos = 0; // Current Pos: given to getUntil() as 2nd arg, and
    // new pos is returned in 3rd argument.

    EXPECT_EQ(
        // The failure is discovered through the tracker, notice that
        // the completion of the test is also checking to make sure that
        // BackupFailureMonitor is driving the ReplicaManager proceed()
        // loop until recovery has completed.
        "main: Notifying log of failure of serverId 1.0 | "
        // Next few, ensure open/closed segments get tagged appropriately
        // since recovery is different for each.
        "handleBackupFailure: Handling backup failure of serverId 1.0 | "
        "handleBackupFailure: Segment 0 recovering from lost replica "
            "which was on backup 1.0 | "
        "handleBackupFailure: Segment 1 recovering from lost replica "
            "which was on backup 1.0 | "
        "handleBackupFailure: Lost replica(s) for segment 1 "
            "while open due to crash of backup 1.0 | "
        "handleBackupFailure: Highest affected segmentId 1 | "
        // Ensure a new log head is allocated.
        "main: Allocating a new log head | "
        , TestLog::getUntil("allocateSegment:", curPos, &curPos));

    EXPECT_EQ(
        // Which provides the required new log digest via open.
        "allocateSegment: Allocating new replicated segment for <3.0,2> | "
        "close: 3.0, 1, 2 | "
        // And which also provides the needed close on the log segment
        // with the lost open replica.
        "close: Segment 1 closed (length 194) | "
        // Notice the actual close to the backup is delayed because it
        // must wait on a durable open to the new log head.
        "performWrite: Cannot close segment 1 until following segment is "
            "durably open | "
        // Re-replication of Segment 1's lost replica.
        "performWrite: Starting replication of segment 1 replica slot 1 "
            "on backup 4.0 | "
        "performWrite: Sending open to backup 4.0 | "
        "writeSegment: Opening <3.0,1> | "
        , TestLog::getUntil("selectPrimary:", curPos, &curPos));

    EXPECT_EQ(
        // Re-replication of Segment 0's lost replica.
        "selectPrimary: Chose server 4.0 with 0 primary replicas and 100 MB/s "
            "disk bandwidth (expected time to read on recovery is 80 ms) | "
        "performWrite: Starting replication of segment 0 replica slot 0 "
            "on backup 4.0 | "
        "performWrite: Sending open to backup 4.0 | "
        "writeSegment: Opening <3.0,0> | "
        , TestLog::getUntil("selectPrimary:", curPos, &curPos));

    EXPECT_EQ(
        // Starting replication of new log head Segment 2.
        "selectPrimary: Chose server 2.0 with 1 primary replicas and 100 MB/s "
            "disk bandwidth (expected time to read on recovery is 160 ms) | "
        "performWrite: Starting replication of segment 2 replica slot 0 "
            "on backup 2.0 | "
        // But replication cannot start right away because preceding segments
        // haven't been durably opened.
        "performWrite: Cannot open segment 2 until preceding segment is "
            "durably open | "
        "performWrite: Starting replication of segment 2 replica slot 1 "
            "on backup 4.0 | "
        "performWrite: Cannot open segment 2 until preceding segment is "
            "durably open | "
        "performWrite: Cannot close segment 1 until following segment is "
            "durably open | "
        "performWrite: Sending open to backup 2.0 | "
        , TestLog::getUntil("writeSegment:", curPos, &curPos));

    EXPECT_EQ(
        // Starting replication of new log head Segment 2.
        "writeSegment: Opening <3.0,2> | "
        "performWrite: Sending open to backup 4.0 | "
        "writeSegment: Opening <3.0,2> | "
        // Segment 1 is still waiting for the RPC for the open for Segment 2
        // to be reaped to ensure that it is durably open. Repeats twice
        // because segment 2 was delayed in sending its open due to the open
        // from its preceding segment.
        "performWrite: Cannot close segment 1 until following segment is "
            "durably open | "
        "performWrite: Cannot close segment 1 until following segment is "
            "durably open | "
        // This part is a bit ambiguous:
        // Segment 2 is durably open now and its rpcs get reaped, so
        // That frees Segment 1 to finally send it's closing write which
        // accounts for 2 rpcs, one to each of its replicas.  The other
        // write is to finish the replication of segment 0.
        "performWrite: Sending write to backup 4.0 | "
        "performWrite: Sending write to backup 2.0 | "
        "performWrite: Sending write to backup 4.0 | "
        , TestLog::getUntil("performTask:", curPos, &curPos));

    EXPECT_EQ(
        // Update minOpenSegmentId on the coordinator to complete the lost
        // open segment recovery protocol.
        // Happens a few times because the code just polls to keep updating
        // until it reads the right value has been acknowledged.  Only one
        // RPC is sent, though.
        "performTask: Updating minOpenSegmentId on coordinator to ensure lost "
            "replicas of segment 1 will not be reused | "
        "updateToAtLeast: request update to minOpenSegmentId for 3.0 to 2 | "
        "setMinOpenSegmentId: setMinOpenSegmentId for server 3.0 to 2 | "
        "performTask: Updating minOpenSegmentId on coordinator to ensure lost "
            "replicas of segment 1 will not be reused | "
        "updateToAtLeast: request update to minOpenSegmentId for 3.0 to 2 | "
        "performTask: coordinator minOpenSegmentId for 3.0 updated to 2 | "
        "performTask: minOpenSegmentId ok, lost open replica recovery "
            "complete on segment 1"
        , TestLog::getUntil("", curPos, &curPos));

    // Make sure it rolled over to a new log head.
    EXPECT_EQ(2u, log.head->id);
    // Make sure the minOpenSegmentId was updated.
    EXPECT_EQ(2u,
        cluster.coordinator->context->coordinatorServerList->at(
        serverId).minOpenSegmentId);
}

} // namespace RAMCloud
