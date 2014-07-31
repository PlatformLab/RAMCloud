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

#include <queue>

#include "TestUtil.h"
#include "BackupSelector.h"
#include "Memory.h"
#include "ReplicatedSegment.h"
#include "Segment.h"
#include "ShortMacros.h"
#include "StringUtil.h"
#include "TaskQueue.h"
#include "TransportManager.h"
#include "WireFormat.h"

namespace RAMCloud {

namespace {

using namespace WireFormat; // NOLINT

// Shorten some oft used names.
typedef BackupWrite Wr;
typedef BackupWrite::Request WrReq;

/**
 * backups.push_back Backups into this selector and the select methods will
 * replay them.  No bounds checking done, use it carefully.
 */
struct MockBackupSelector : public BaseBackupSelector {
    explicit MockBackupSelector(size_t count)
        : backups()
        , primaryFreed()
        , nextIndex(0)
    {
        makeSimpleHostList(count);
    }

    ServerId selectPrimary(uint32_t numBackups, const ServerId backupIds[]) {
        return selectSecondary(numBackups, backupIds);
    }

    ServerId selectSecondary(uint32_t numBackups, const ServerId backupIds[]) {
        assert(backups.size());
        for (uint32_t i = 0; i < numBackups; ++i)
            TEST_LOG("conflicting backupId: %s",
                     backupIds[i].toString().c_str());
        ServerId backup = backups[nextIndex++];
        nextIndex %= backups.size();
        return backup;
    }

    void signalFreedPrimary(const ServerId backupId) {
        primaryFreed.push_back(backupId);
    }

    void makeSimpleHostList(size_t count) {
        for (uint32_t i = 0; i < count; ++i)
            backups.push_back(ServerId(i, 0));
    }

    std::vector<ServerId> backups;
    std::vector<ServerId> primaryFreed;
    size_t nextIndex;
};

struct CountingDeleter : public ReplicatedSegment::Deleter {
    CountingDeleter()
        : count(0) {}

    void destroyAndFreeReplicatedSegment(ReplicatedSegment*
                                            replicatedSegment) {
        ++count;
    }

    uint32_t count;
};
}

struct ReplicatedSegmentTest : public ::testing::Test {
    enum { DATA_LEN = 100 };
    enum { MAX_BYTES_PER_WRITE = 21 };

    struct CreateSegment {
        CreateSegment(ReplicatedSegmentTest* test,
                      ReplicatedSegment* precedingSegment,
                      uint64_t segmentId,
                      uint32_t numReplicas)
            : logSegment(test->data, DATA_LEN)
            , segment()
        {
            void* segMem =
                operator new(ReplicatedSegment::sizeOf(numReplicas));
            logSegment.head = test->openLen; // open queued
            segment.reset(
                new(segMem) ReplicatedSegment(&test->context,
                                              test->taskQueue,
                                              test->backupSelector,
                                              test->deleter,
                                              test->writeRpcsInFlight,
                                              test->freeRpcsInFlight,
                                              test->replicationEpoch,
                                              test->dataMutex,
                                              segmentId,
                                              &logSegment,
                                              true,
                                              test->masterId,
                                              numReplicas,
                                              NULL,
                                              MAX_BYTES_PER_WRITE));
            // Set up ordering constraints between this new segment and the
            // prior one in the log.
            if (precedingSegment) {
                precedingSegment->followingSegment = segment.get();
                segment->precedingSegmentCloseCommitted =
                    precedingSegment->getCommitted().close;
                segment->precedingSegmentOpenCommitted =
                    precedingSegment->getCommitted().open;
            }

        }
        Segment logSegment;
        std::unique_ptr<ReplicatedSegment> segment;
    };

    Context context;
    TaskQueue taskQueue;
    ServerList serverList;
    CountingDeleter deleter;
    uint32_t writeRpcsInFlight;
    uint32_t freeRpcsInFlight;
    std::mutex dataMutex;
    const ServerId masterId;
    const uint64_t segmentId;
    UpdateReplicationEpochTask replicationEpoch;
    char data[DATA_LEN];
    Segment logSegment;
    const uint32_t openLen;
    const uint32_t numReplicas;
    MockBackupSelector backupSelector;
    MockTransport transport;
    TransportManager::MockRegistrar mockRegistrar;
    CreateSegment* createSegment;
    ReplicatedSegment* segment;
    ServerId backupId1;
    ServerId backupId2;

    ReplicatedSegmentTest()
        : context()
        , taskQueue()
        , serverList(&context)
        , deleter()
        , writeRpcsInFlight(0)
        , freeRpcsInFlight(0)
        , dataMutex()
        , masterId(999, 0)
        , segmentId(888)
        , replicationEpoch(&context, &taskQueue, &masterId)
        , data()
        , logSegment()
        , openLen(10)
        , numReplicas(2)
        , backupSelector(numReplicas)
        , transport(&context)
        , mockRegistrar(&context, transport)
        , createSegment(NULL)
        , segment(NULL)
        , backupId1(0, 0)
        , backupId2(1, 0)
    {
        Logger::get().setLogLevels(SILENT_LOG_LEVEL);

        serverList.testingAdd({backupId1, "mock:host=backup1",
                               {WireFormat::BACKUP_SERVICE}, 100,
                               ServerStatus::UP});
        serverList.testingAdd({backupId2, "mock:host=backup2",
                               {WireFormat::BACKUP_SERVICE}, 100,
                               ServerStatus::UP});

        const char* msg = "abcdefghijklmnopqrstuvwxyz";
        size_t msgLen = strlen(msg);
        for (int i = 0; i < DATA_LEN; ++i)
            data[i] = msg[i % msgLen];

        // Create the segment _after_ initializing data[] so that the contents
        // don't change after construction.
        createSegment = new CreateSegment(this, NULL, segmentId, numReplicas);
        segment = createSegment->segment.get();
    }

    ~ReplicatedSegmentTest()
    {
        delete createSegment;
    }

    void reset() {
        taskQueue.tasks.pop();
        segment->scheduled = false;
    }

    DISALLOW_COPY_AND_ASSIGN(ReplicatedSegmentTest);
};

TEST_F(ReplicatedSegmentTest, varLenArrayAtEnd) {
    // replicas[0] must be the last member of ReplicatedSegment
    EXPECT_EQ(static_cast<void*>(segment + 1),
              static_cast<void*>(&segment->replicas[0].isActive));
    reset();
}

TEST_F(ReplicatedSegmentTest, constructor) {
    EXPECT_EQ(openLen, segment->queued.bytes);
    EXPECT_TRUE(segment->queued.open);
    ASSERT_FALSE(taskQueue.isIdle());
    EXPECT_EQ(segment, taskQueue.tasks.front());
    reset();
}

TEST_F(ReplicatedSegmentTest, free) {
    transport.setInput("0 0"); // write+open first replica
    transport.setInput("0 0"); // write+open second replica
    transport.setInput("0 0"); // write+close first replica
    transport.setInput("0 0"); // write+close second replica
    segment->close();
    segment->free();
    EXPECT_TRUE(segment->freeQueued);
    EXPECT_TRUE(segment->isScheduled());
    ASSERT_FALSE(taskQueue.isIdle());
    EXPECT_EQ(segment, taskQueue.tasks.front());
    reset();
}

TEST_F(ReplicatedSegmentTest, freeWriteRpcInProgress) {
    transport.setInput("0 0"); // write+open first replica
    transport.setInput("0 0"); // write+open second replica
    transport.setInput("0 0"); // write+close first replica
    transport.setInput("0 0"); // write+close second replica
    segment->close();
    taskQueue.performTask(); // writeRpc created
    EXPECT_EQ(2lu, segment->writeRpcsInFlight);
    EXPECT_TRUE(segment->replicas[0].writeRpc);
    segment->free();
    EXPECT_EQ(0lu, segment->writeRpcsInFlight);
    EXPECT_FALSE(segment->replicas[0].writeRpc);

    // make sure the backup "free" opcode was not sent
    EXPECT_TRUE(TestUtil::doesNotMatchPosixRegex("0x1001c",
                                                 transport.outputLog));
    ASSERT_TRUE(segment->replicas[0].isActive);
    EXPECT_FALSE(segment->replicas[0].writeRpc); // ensure the write completed
    EXPECT_FALSE(segment->replicas[0].freeRpc);
    EXPECT_TRUE(segment->isScheduled());

    taskQueue.performTask();
    ASSERT_TRUE(segment->replicas[0].isActive);
    EXPECT_FALSE(segment->replicas[0].writeRpc);
    EXPECT_TRUE(segment->replicas[0].freeRpc); // ensure free gets sent
    EXPECT_TRUE(segment->isScheduled());

    reset();
}

TEST_F(ReplicatedSegmentTest, isSynced) {
    EXPECT_FALSE(segment->isSynced());
    segment->replicas[0].isActive = segment->replicas[1].isActive = true;
    segment->replicas[0].sent =
        segment->replicas[0].acked =
        segment->replicas[0].committed =
        segment->replicas[1].sent =
        segment->replicas[1].acked =
        segment->replicas[1].committed = {true, 10, 0, false};
    EXPECT_TRUE(segment->isSynced());
    segment->recoveringFromLostOpenReplicas = true;
    EXPECT_FALSE(segment->isSynced());
    segment->recoveringFromLostOpenReplicas = false;
    createSegment->logSegment.head = openLen + 10; // write queued
    EXPECT_FALSE(segment->isSynced());
    reset();
}

TEST_F(ReplicatedSegmentTest, close) {
    reset();
    segment->close();
    EXPECT_TRUE(segment->isScheduled());
    ASSERT_FALSE(taskQueue.isIdle());
    EXPECT_EQ(segment, taskQueue.tasks.front());
    EXPECT_TRUE(segment->queued.close);
    reset();
}

TEST_F(ReplicatedSegmentTest, handleBackupFailureWhileOpen) {
    segment->handleBackupFailure({0, 0}, false);
    foreach (auto& replica, segment->replicas)
        EXPECT_FALSE(replica.replicateAtomically);

    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // write
    createSegment->logSegment.head = openLen; // write queued
    // Not active still, next performTask chooses backups and sends opens.
    segment->handleBackupFailure({0, 0}, false);
    foreach (auto& replica, segment->replicas)
        EXPECT_FALSE(replica.replicateAtomically);

    taskQueue.performTask();
    // Replicas are now active with an outstanding open rpc.
    segment->handleBackupFailure({88888, 0}, false);
    foreach (auto& replica, segment->replicas)
        EXPECT_FALSE(replica.replicateAtomically);

    EXPECT_EQ(2u, writeRpcsInFlight);
    segment->handleBackupFailure({0, 0}, false);
    EXPECT_EQ(1u, writeRpcsInFlight);
    // The failed replica must restart replication in atomic mode.
    EXPECT_TRUE(segment->replicas[0].replicateAtomically);
    // The other open replica is in normal (non-atomic) mode still.
    EXPECT_FALSE(segment->replicas[1].replicateAtomically);
    EXPECT_EQ(1lu, segment->queued.epoch);

    // Failure of the second replica. This one has a free request
    // flight.
    segment->replicas[1].freeRpc.construct(&context, backupId1,
                                           masterId, segmentId);
    freeRpcsInFlight = 2;
    segment->handleBackupFailure({1, 0}, false);
    EXPECT_TRUE(segment->replicas[0].replicateAtomically);
    EXPECT_TRUE(segment->replicas[1].replicateAtomically);
    EXPECT_EQ(2lu, segment->queued.epoch);
    EXPECT_EQ(1u, freeRpcsInFlight);

    reset();
}

namespace {
bool handleBackupFailureFilter(string s) {
    return s == "handleBackupFailure" ||
           s == "performTask" ||
           s == "performWrite" ||
           s == "updateToAtLeast";
}
}

TEST_F(ReplicatedSegmentTest, handleBackupFailureWhileOpenDoesntSendForClosed) {
    transport.setInput("0 0"); // open
    transport.setInput("0 0"); // open
    createSegment->logSegment.head = openLen; // write queued
    segment->close();
    taskQueue.performTask(); // send opens
    taskQueue.performTask(); // reap opens

    // Cause replica 1 to start over just to get the replication out of sync.
    segment->replicas[1].sent.open =
        segment->replicas[1].acked.open =
        segment->replicas[1].committed.open = false;
    segment->replicas[1].sent.bytes =
        segment->replicas[1].acked.bytes =
        segment->replicas[1].committed.bytes = 0;
    transport.setInput("0 0"); // close
    transport.setInput("0 0"); // open
    taskQueue.performTask(); // send close on slot 0, open on slot 1
    // Replica 0 has a close rpc for epoch 0 outstanding.
    // Replica 0 will NOT send an extra rpc to update the epoch after
    // the failure below. This is because once it reaps the close rpc its
    // epoch no longer matters, so it doesn't resend.
    // Replica 1 has resent its opening rpc.

    // Handle failure on the open replica.
    TestLog::Enable _(handleBackupFailureFilter);
    segment->handleBackupFailure({1, 0}, false);
    EXPECT_FALSE(segment->replicas[0].replicateAtomically);
    EXPECT_TRUE(segment->replicas[1].replicateAtomically);
    EXPECT_EQ(1lu, segment->queued.epoch);

    transport.setInput("0 0"); // open
    taskQueue.performTask(); // send open
    taskQueue.performTask(); // reap open
    transport.setInput("0 0"); // close
    taskQueue.performTask(); // send write
    taskQueue.performTask(); // reap write, request epoch update on coordinator
    EXPECT_EQ(
        "handleBackupFailure: Segment 888 recovering from lost replica which "
            "was on backup 1.0 | "
        "handleBackupFailure: Lost replica(s) for segment 888 while open due "
            "to crash of backup 1.0 | "
        "performWrite: Write RPC finished for replica slot 0 | "
        // Don't get weirded out by the fact that replica 0 and 1 are both
        // on the same backup. It's an artifact of the MockBackupSelector.
        // Shouldn't happen with the real BackupSelector.
        "performWrite: Starting replication of segment 888 replica slot 1 on "
            "backup 0.0 | "
        "performWrite: Sending open to backup 0.0 | "
        "performWrite: Write RPC finished for replica slot 1 | "
        "performWrite: Sending write to backup 0.0 | "
        "performWrite: Write RPC finished for replica slot 1 | "
        "performTask: Updating replicationEpoch to 888,1 on coordinator to "
            "ensure lost replicas will not be reused | "
        "updateToAtLeast: request update to master recovery info for 999.0 "
            "to 888,1", TestLog::get());
    // Importantly, there is no mention of replica 0 above; it shouldn't need
    // to do anything since it was already closed when the failure happened.

    reset();
}

TEST_F(ReplicatedSegmentTest, handleBackupFailureWhileHandlingFailure) {
    transport.setInput("0 0"); // open
    transport.setInput("0 0"); // open
    createSegment->logSegment.head = openLen; // write queued
    taskQueue.performTask(); // send opens
    taskQueue.performTask(); // reap opens

    transport.setInput("0 0"); // close
    transport.setInput("0 0"); // close
    segment->close();
    taskQueue.performTask(); // send closes
    taskQueue.performTask(); // reap closes

    // Handle failure while closed. This should technically
    // "reopen" the segment, though only in atomic replication mode.
    segment->handleBackupFailure({0, 0}, false);
    EXPECT_TRUE(segment->replicas[0].replicateAtomically);
    EXPECT_FALSE(segment->replicas[1].replicateAtomically);
    EXPECT_EQ(0lu, segment->queued.epoch);

    // Check to make sure that atomic replications aren't counted as
    // failures while open. They can't threaten the integrity of the log.
    segment->handleBackupFailure({0, 0}, false);
    EXPECT_TRUE(segment->replicas[0].replicateAtomically);
    EXPECT_FALSE(segment->replicas[1].replicateAtomically);
    EXPECT_EQ(0lu, segment->queued.epoch);

    reset();
}

TEST_F(ReplicatedSegmentTest, handleBackupFailureWithMinCopysets) {
    transport.setInput("0 0"); // write+open first replica
    transport.setInput("0 0"); // write+open second replica
    transport.setInput("0 0"); // write+close first replica
    transport.setInput("0 0"); // write+close second replica
    segment->close();
    taskQueue.performTask();

    TestLog::Enable _(handleBackupFailureFilter);
    EXPECT_FALSE(segment->replicas[0].replicateAtomically);
    EXPECT_FALSE(segment->replicas[1].replicateAtomically);
    // Should kill all replicas if any one is down.
    segment->handleBackupFailure({1, 0}, true);
    EXPECT_TRUE(segment->replicas[0].replicateAtomically);
    EXPECT_TRUE(segment->replicas[1].replicateAtomically);
}

TEST_F(ReplicatedSegmentTest, sync) {
    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // write

    createSegment->logSegment.head = openLen;
    segment->sync(segment->queued.bytes); // first sync sends the opens
    Segment::Certificate certificate;
    createSegment->logSegment.getAppendedLength(&certificate);
    EXPECT_TRUE(transport.outputMatches(0, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 0, 0, 10, true, false, true, true, certificate},
                "abcdefghij", 10));
    EXPECT_TRUE(transport.outputMatches(1, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 888, 0, 0, 10, true, false, false, true, certificate},
                "abcdefghij", 10));
    transport.clearOutput();
    EXPECT_TRUE(segment->getCommitted().open);
    EXPECT_EQ(openLen, segment->getCommitted().bytes);

    segment->sync(segment->queued.bytes); // second sync doesn't send anything
    EXPECT_EQ("", transport.outputLog);
    transport.clearOutput();
    EXPECT_EQ(openLen, segment->getCommitted().bytes);

    createSegment->logSegment.head = openLen + 10;
    segment->sync(openLen); // doesn't send anything
    EXPECT_TRUE(transport.output.empty());
    transport.clearOutput();
    segment->sync(openLen + 1); // will wait until after the next send
    createSegment->logSegment.getAppendedLength(&certificate);
    EXPECT_TRUE(transport.outputMatches(0, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 0, 10, 10, false, false, true, true, certificate},
                "klmnopqrst", 10));
    EXPECT_TRUE(transport.outputMatches(1, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 888, 0, 10, 10,
                 false, false, false, true, certificate},
                "klmnopqrst ", 10));
    EXPECT_EQ(openLen + 10, segment->getCommitted().bytes);
}

TEST_F(ReplicatedSegmentTest, syncNoArgs) {
    transport.setInput("0 0"); // opening write
    transport.setInput("0 0"); // opening write
    segment->sync(openLen);
    EXPECT_EQ(openLen, segment->getCommitted().bytes);
    EXPECT_FALSE(segment->getCommitted().close);
    EXPECT_TRUE(segment->isSynced());
    transport.setInput("0 0"); // closing write
    transport.setInput("0 0"); // closing write
    segment->close();
    segment->sync();
    EXPECT_EQ(openLen, segment->getCommitted().bytes);
    EXPECT_TRUE(segment->getCommitted().close);
    EXPECT_TRUE(segment->isSynced());
}

TEST_F(ReplicatedSegmentTest, syncDoubleCheckCrossSegmentOrderingConstraints) {
    transport.setInput("0 0"); // write - newHead open
    transport.setInput("0 0"); // write - newHead open
    transport.setInput("0 0"); // write - segment open
    transport.setInput("0 0"); // write - segment open
    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // write

    Segment::Certificate segmentOpeningCertificate;
    createSegment->logSegment.getAppendedLength(&segmentOpeningCertificate);

    CreateSegment newHeadCreateSegment(this, segment,
                                       segmentId + 1, numReplicas);
    auto newHead = newHeadCreateSegment.segment.get();

    Segment::Certificate newHeadOpeningCertificate;
    newHeadCreateSegment.
        logSegment.getAppendedLength(&newHeadOpeningCertificate);

    segment->close(); // close queued
    newHeadCreateSegment.logSegment.head = openLen + 10; // write queued
    newHead->queued.bytes = openLen + 10;
    newHeadCreateSegment.
        logSegment.getAppendedLength(&newHead->queuedCertificate);

    Segment::Certificate segmentFinalCertificate;
    createSegment->logSegment.getAppendedLength(&segmentFinalCertificate);
    Segment::Certificate newHeadFinalCertificate;
    newHeadCreateSegment.logSegment.getAppendedLength(&newHeadFinalCertificate);

    // Mess up the queue order to simulate reorder due to failure.
    // This means newHead will be going first at 'taking turns'
    // with segment.
    {
        Task* t = taskQueue.tasks.front();
        taskQueue.tasks.pop();
        taskQueue.tasks.push(t);
    }

    // Queued order of ops would be:
    // open newHead
    // open segment
    // write newHead
    // close segment

    // Required by constraints (and checked here):
    // open segment
    // open newHead (open order is enforced to prevent missing replicas)
    // close segment
    // write newHead

    EXPECT_TRUE(transport.output.empty());
    newHead->sync(newHead->queued.bytes);

    EXPECT_TRUE(transport.outputMatches(0, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 0, 0, 10, true, false, true, true,
                 segmentOpeningCertificate},
                "abcdefghij", 10));
    EXPECT_TRUE(transport.outputMatches(1, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 888, 0, 0, 10, true, false, false, true,
                 segmentOpeningCertificate},
                "abcdefghij", 10));
    EXPECT_TRUE(transport.outputMatches(2, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 889, 0, 0, 10, true, false, true, true,
                 newHeadOpeningCertificate},
                "abcdefghij", 10));
    EXPECT_TRUE(transport.outputMatches(3, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 889, 0, 0, 10, true, false, false, true,
                 newHeadOpeningCertificate},
                "abcdefghij", 10));
    EXPECT_TRUE(transport.outputMatches(4, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 0, 10, 0, false, true, true, true,
                 segmentFinalCertificate}));
    EXPECT_TRUE(transport.outputMatches(5, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 888, 0, 10, 0, false, true, false, true,
                 segmentFinalCertificate}));
    EXPECT_TRUE(transport.outputMatches(6, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 889, 0, 10, 10, false, false, true, true,
                 newHeadFinalCertificate},
                "klmnopqrst", 10));
    EXPECT_TRUE(transport.outputMatches(7, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 889, 0, 10, 10, false, false, false, true,
                 newHeadFinalCertificate},
                "klmnopqrst", 10));
}

namespace {
bool filter(string s) {
    return s != "checkStatus" && s != "schedule";
}
}

TEST_F(ReplicatedSegmentTest, syncRecoveringFromLostOpenReplicas) {
    // Generates a segment, syncs some data to it, and then simulates
    // a crash while that segment is still open. Checks to make sure
    // that the originally synced data appears unsynced while the
    // recovery is going on and that after recovery has happened the
    // data appears synced again.
    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // open
    transport.setInput("0 0"); // write/close
    transport.setInput("0 0"); // set replication epoch on coordinator

    createSegment->logSegment.head = openLen;
    segment->sync(openLen); // first sync sends the opens
    transport.clearOutput();
    segment->sync(openLen); // second sync sends nothing
    EXPECT_TRUE(transport.output.empty());
    transport.clearOutput();
    EXPECT_EQ(openLen, segment->getCommitted().bytes);
    EXPECT_EQ(backupId1, segment->replicas[0].backupId);
    EXPECT_EQ(backupId2, segment->replicas[1].backupId);
    EXPECT_FALSE(segment->getCommitted().close);

    // Now the open segment encounters a failure.
    segment->handleBackupFailure({0, 0}, false);
    EXPECT_EQ(1lu, segment->queued.epoch);
    EXPECT_TRUE(segment->recoveringFromLostOpenReplicas);
    EXPECT_FALSE(segment->replicas[0].isActive);
    EXPECT_TRUE(segment->replicas[0].replicateAtomically);
    EXPECT_EQ(0lu, segment->replicas[0].sent.epoch);
    EXPECT_EQ(0lu, segment->replicas[0].acked.epoch);
    EXPECT_EQ(0lu, segment->replicas[0].committed.epoch);
    EXPECT_TRUE(segment->replicas[1].isActive);
    EXPECT_FALSE(segment->replicas[1].replicateAtomically);
    EXPECT_EQ(0lu, segment->replicas[1].sent.epoch);
    EXPECT_EQ(0lu, segment->replicas[1].acked.epoch);
    EXPECT_EQ(0lu, segment->replicas[1].committed.epoch);
    EXPECT_EQ(backupId2, segment->replicas[1].backupId);

    EXPECT_FALSE(segment->getCommitted().open);
    EXPECT_EQ(0lu, segment->getCommitted().bytes);
    EXPECT_EQ(0lu, segment->getCommitted().epoch);
    EXPECT_FALSE(segment->getCommitted().close);

    EXPECT_EQ(0lu, replicationEpoch.current.first);
    EXPECT_EQ(0lu, replicationEpoch.current.second);
    // Notice: weird and special case that only happens during testing:
    // The replica gets recreated right back on the backup with id 0
    // because it isn't removed from the ServerList that the
    // BackupSelector is working of off.
    TestLog::Enable _(filter);
    segment->sync(openLen);
    EXPECT_TRUE(segment->getCommitted().open);
    EXPECT_EQ(openLen, segment->getCommitted().bytes);
    EXPECT_EQ(1lu, segment->getCommitted().epoch);
    EXPECT_FALSE(segment->getCommitted().close);
    // Just double check it all to make extra sure.
    EXPECT_EQ(1lu, segment->replicas[0].committed.epoch);
    EXPECT_EQ(1lu, segment->replicas[1].committed.epoch);
    // Fragile test log check, but left here because the output is pretty
    // reassuring to a human reader that the test does what one expects.
    EXPECT_EQ("sync: syncing segment 888 to offset 10 | "
              "selectSecondary: conflicting backupId: 1.0 | "
              "performWrite: Starting replication of segment 888 replica "
                  "slot 0 on backup 0.0 | "
              "performWrite: Sending open to backup 0.0 | "
              "performWrite: Sending write to backup 1.0 | "
              "performWrite: Write RPC finished for replica slot 0 | "
              "performWrite: Write RPC finished for replica slot 1 | "
              // After both replicas have received a write they should try to
              // update the replication epoch on the coordinator to invalidate
              // stale replicas.
              "performTask: Updating replicationEpoch to 888,1 on coordinator "
                  "to ensure lost replicas will not be reused | "
              "updateToAtLeast: request update to master recovery info for "
                  "999.0 to 888,1 | "
              "performTask: Updating replicationEpoch to 888,1 on coordinator "
                  "to ensure lost replicas will not be reused | "
              "updateToAtLeast: request update to master recovery info for "
                  "999.0 to 888,1 | "
              "performTask: replicationEpoch ok, lost open replica recovery "
              "complete on segment 888",
              TestLog::get());
    // Three rpcs are sent out to rereplicate the lost replica.
    // Notice the opening write doesn't include a certificate because the
    // replica should only be committed when it has been fully caught-up
    // with the master's copy. It does this by withholding the certificate
    // needed to iterate over the replica until the master is ready to
    // commit the whole thing.
    Segment::Certificate certificate;
    // Opening primary write, no certificate, new epoch.
    EXPECT_TRUE(transport.outputMatches(0, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 1, 0, 10, true, false, true, false, certificate},
                "abcdefghij", 10));
    createSegment->logSegment.getAppendedLength(&certificate);
    // Write to non-crashed replica includes certificate and new epoch.
    // No actual data is sent or change to the closed flag. Just need to update
    // the epoch is all.
    EXPECT_TRUE(transport.outputMatches(1, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 888, 1, 10, 0, false, false, false, true, certificate}));
    EXPECT_TRUE(segment->getCommitted().open);
    EXPECT_EQ(openLen, segment->getCommitted().bytes);
    EXPECT_EQ(1lu, segment->getCommitted().epoch);
    EXPECT_EQ(888lu, replicationEpoch.current.first);
    EXPECT_EQ(1lu, replicationEpoch.current.second);
    transport.clearOutput();

    segment->sync(openLen); // doesn't send anything
    EXPECT_TRUE(transport.outputLog.empty());
}

/*
 * Ensures that even if backups have acked an object, sync doesn't return for
 * that object until some subsequent certificate has been acknowledged by the
 * backup.
 */
TEST_F(ReplicatedSegmentTest, syncWaitsForCommittedEvenWhenAcked) {
    Segment::Certificate openingCertificate;
    createSegment->logSegment.getAppendedLength(&openingCertificate);

    // oversized write queued
    createSegment->logSegment.head =
        segment->maxBytesPerWriteRpc + segment->openLen + 1;
    createSegment->logSegment.checksum.result = 0xcabba9e;
    Segment::Certificate certificate;
    createSegment->logSegment.getAppendedLength(&certificate);
    segment->close();
    EXPECT_TRUE(segment->isScheduled());

    transport.setInput("0 0"); // open/write
    transport.setInput("0 0"); // open/write
    taskQueue.performTask(); // send open
    EXPECT_TRUE(segment->isScheduled());
    taskQueue.performTask(); // reap opens

    EXPECT_TRUE(transport.outputMatches(0, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 0, 0, 10, true, false, true, true,
                 openingCertificate},
                "abcdefghij", 10));
    EXPECT_TRUE(transport.outputMatches(1, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 888, 0, 0, 10, true, false, false, true,
                 openingCertificate},
                "abcdefghij", 10));

    transport.clearOutput();

    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // close
    transport.setInput("0 0"); // close

    segment->sync(openLen + 1);

    Segment::Certificate empty;
    EXPECT_TRUE(transport.outputMatches(0, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 0, 10, 21, false, false, true, false, empty},
                "klmnopqrstuvwxyzabcde", 21));
    EXPECT_TRUE(transport.outputMatches(1, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 888, 0, 10, 21, false, false, false, false, empty},
                "klmnopqrstuvwxyzabcde", 21));
    EXPECT_TRUE(transport.outputMatches(2, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 0, 31, 1, false, true, true, true, certificate},
                "f", 1));
    EXPECT_TRUE(transport.outputMatches(3, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 888, 0, 31, 1, false, true, false, true, certificate},
                "f", 1));

    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, syncOnHeaderWaitsForCloseOfPriorSegment) {
    transport.setInput("0 0"); // write - segment open
    transport.setInput("0 0"); // write - segment open
    transport.setInput("0 0"); // write - newHead open
    transport.setInput("0 0"); // write - newHead open
    transport.setInput("0 0"); // write - segment close
    transport.setInput("0 0"); // write - segment close

    Segment::Certificate segmentOpeningCertificate;
    createSegment->logSegment.getAppendedLength(&segmentOpeningCertificate);

    CreateSegment newHeadCreateSegment(this, segment,
                                       segmentId + 1, numReplicas);
    auto newHead = newHeadCreateSegment.segment.get();

    Segment::Certificate newHeadOpeningCertificate;
    newHeadCreateSegment.
        logSegment.getAppendedLength(&newHeadOpeningCertificate);

    segment->close(); // close queued

    Segment::Certificate segmentFinalCertificate;
    createSegment->logSegment.getAppendedLength(&segmentFinalCertificate);

    EXPECT_TRUE(transport.output.empty());
    newHead->sync(newHead->openLen);

    EXPECT_TRUE(transport.outputMatches(0, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 0, 0, 10, true, false, true, true,
                 segmentOpeningCertificate},
                "abcdefghij", 10));
    EXPECT_TRUE(transport.outputMatches(1, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 888, 0, 0, 10, true, false, false, true,
                 segmentOpeningCertificate},
                "abcdefghij", 10));
    EXPECT_TRUE(transport.outputMatches(2, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 889, 0, 0, 10, true, false, true, true,
                 newHeadOpeningCertificate},
                "abcdefghij", 10));
    EXPECT_TRUE(transport.outputMatches(3, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 889, 0, 0, 10, true, false, false, true,
                 newHeadOpeningCertificate},
                "abcdefghij", 10));
    EXPECT_TRUE(transport.outputMatches(4, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 0, 10, 0, false, true, true, true,
                 segmentFinalCertificate}));
    EXPECT_TRUE(transport.outputMatches(5, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 888, 0, 10, 0, false, true, false, true,
                 segmentFinalCertificate}));
}

TEST_F(ReplicatedSegmentTest, swapSegment) {
    transport.setInput("0 0"); // write+open first replica
    transport.setInput("0 0"); // write+open second replica
    transport.setInput("0 0"); // write+close first replica
    transport.setInput("0 0"); // write+close second replica
    segment->close();
    segment->sync();
    EXPECT_FALSE(segment->isScheduled());

    const Segment* oldSegment = segment->segment;
    Segment newSegment;
    newSegment.append(LOG_ENTRY_TYPE_OBJ, "hi", 3);
    EXPECT_EQ(oldSegment, segment->swapSegment(&newSegment));
    EXPECT_EQ(&newSegment, segment->segment);

    Segment::Certificate newCertificate;
    uint32_t appendedBytes = newSegment.getAppendedLength(&newCertificate);
    EXPECT_EQ(newCertificate, segment->queuedCertificate);
    EXPECT_EQ(appendedBytes, segment->queued.bytes);
    foreach (auto& replica, segment->replicas) {
        EXPECT_EQ(segment->queued, replica.committed);
        EXPECT_EQ(segment->queued, replica.acked);
        EXPECT_EQ(segment->queued, replica.sent);
    }
}

TEST_F(ReplicatedSegmentTest, swapSegmentThenBackupFailure) {
    transport.setInput("0 0"); // open
    transport.setInput("0 0"); // open
    createSegment->logSegment.head = openLen; // write queued
    taskQueue.performTask(); // send opens
    taskQueue.performTask(); // reap opens

    transport.setInput("0 0"); // close
    transport.setInput("0 0"); // close
    segment->close();
    taskQueue.performTask(); // send closes
    taskQueue.performTask(); // reap closes

    // Do the brain transplant.
    Segment newSegment("new content!", 13);
    newSegment.checksum.result = 0xdeadbeef;
    newSegment.append(LOG_ENTRY_TYPE_OBJ, "new content!", 13);
    segment->swapSegment(&newSegment);

    // Handle failure on the second replica.
    transport.clearOutput();
    TestLog::Enable _(handleBackupFailureFilter);
    segment->handleBackupFailure({1, 0}, false);

    transport.setInput("0 0"); // open
    taskQueue.performTask(); // send open
    taskQueue.performTask(); // reap open
    transport.setInput("0 0"); // close
    taskQueue.performTask(); // send write
    taskQueue.performTask(); // reap write
    EXPECT_EQ("handleBackupFailure: Segment 888 recovering from lost replica "
                "which was on backup 1.0 | "
              "performWrite: Starting replication of segment 888 replica slot "
                "1 on backup 0.0 | "
              "performWrite: Sending open to backup 0.0 | "
              "performWrite: Write RPC finished for replica slot 1 | "
              "performWrite: Sending write to backup 0.0 | "
              "performWrite: Write RPC finished for replica slot 1",
        TestLog::get());

    Segment::Certificate empty;
    Segment::Certificate certificate;
    newSegment.getAppendedLength(&certificate);

    EXPECT_TRUE(transport.outputMatches(0, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 0, 0, 10, true, false, false, false, empty},
                "new conten", 10));
    EXPECT_TRUE(transport.outputMatches(1, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 0, 10, 3, false, true, false, true, certificate},
                "t!", 3));
}

TEST_F(ReplicatedSegmentTest, scheduleWithReplicas) {
    TestLog::Enable _;
    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // write
    createSegment->logSegment.head = openLen + 1;
    segment->sync(openLen + 1);
    EXPECT_TRUE(StringUtil::contains(TestLog::get(), "schedule: scheduled"));
}

TEST_F(ReplicatedSegmentTest, scheduleWithZeroReplicas) {
    TestLog::Enable _;
    CreateSegment createSegment(this, segment, segmentId + 1, 0);
    // Task::schedule() must not have been invoked.
    EXPECT_FALSE(StringUtil::contains(TestLog::get(), "schedule: scheduled"));
    EXPECT_EQ("schedule: zero replicas: nothing to schedule", TestLog::get());
}

TEST_F(ReplicatedSegmentTest, performTaskFreeNothingToDo) {
    transport.setInput("0 0"); // write+open first replica
    transport.setInput("0 0"); // write+open second replica
    transport.setInput("0 0"); // write+close first replica
    transport.setInput("0 0"); // write+close second replica
    createSegment->logSegment.head = openLen + 10; // write queued
    segment->close();
    segment->free();
    EXPECT_TRUE(segment->isScheduled());
    EXPECT_EQ(0u, deleter.count);
    transport.setInput("0 0"); // free rpc first replica
    transport.setInput("0 0"); // free rpc second replica
    taskQueue.performTask(); // send free rpc
    taskQueue.performTask(); // reap free rpc
    EXPECT_FALSE(segment->isScheduled());
    EXPECT_EQ(1u, deleter.count);
}

TEST_F(ReplicatedSegmentTest, performTaskFreeOneReplicaToFree) {
    transport.setInput("0 0"); // write+open first replica
    transport.setInput("0 0"); // write+open second replica
    transport.setInput("0 0"); // write+close first replica
    transport.setInput("0 0"); // write+close second replica
    segment->close();
    segment->free();

    segment->replicas[0].start(backupId1);
    taskQueue.performTask();
    EXPECT_TRUE(segment->isScheduled());
    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performTaskWrite) {
    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // write

    taskQueue.performTask();
    ASSERT_TRUE(segment->replicas[0].isActive);
    EXPECT_TRUE(segment->isScheduled());
    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performTaskRecoveringFromLostOpenReplicas) {
    transport.setInput("0 0"); // open/write
    transport.setInput("0 0"); // open/write
    taskQueue.performTask(); // send open/writes
    segment->handleBackupFailure({0, 0}, false);
    EXPECT_TRUE(segment->recoveringFromLostOpenReplicas);
    EXPECT_EQ(1lu, segment->queued.epoch);
    EXPECT_EQ(0lu, segment->replicas[0].committed.epoch);
    EXPECT_EQ(0lu, segment->replicas[1].committed.epoch);
    EXPECT_FALSE(segment->replicas[0].committed.close);
    EXPECT_FALSE(segment->replicas[1].committed.close);
    transport.clearOutput();

    // Reap the remaining outstanding write, send the new atomic open on the
    // originally failed replica (original outstanding rpc should be canceled).
    // It is atomic by virtue that it doesn't include the certificate needed
    // to read the replica data back later.
    transport.setInput("0 0"); // open/write for replication
    transport.setInput("0 0"); // write for second replica to send new epoch
    taskQueue.performTask(); // restart of replica 0, reap rpc from replica 1
    // Because replica 1 already had an rpc in flight it won't be able to update
    // the epoch on the backup until the next call to performTask().
    taskQueue.performTask(); // reap replica 0, update epoch of replica 1
    TestLog::Enable _(filter);
    taskQueue.performTask(); // reap replica 1, update epoch on coordinator
    EXPECT_EQ(
        "performWrite: Write RPC finished for replica slot 1 | "
        "performTask: Updating replicationEpoch to 888,1 on coordinator to "
            "ensure lost replicas will not be reused | "
        "updateToAtLeast: request update to master recovery info for 999.0 "
            "to 888,1", TestLog::get());

    Segment::Certificate certificate;
    // Atomic re-replication open.
    EXPECT_TRUE(transport.outputMatches(0, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 1, 0, 10, true, false, true, false, certificate},
                "abcdefghij", 10));
    // Surviving replica gets epoch refreshed.
    createSegment->logSegment.getAppendedLength(&certificate);
    EXPECT_TRUE(transport.outputMatches(1, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 888, 1, 10, 0, false, false, false, true, certificate}));
    transport.clearOutput();

    transport.setInput("0 0"); // epoch update on coordinator
    taskQueue.performTask(); // send epoch update to coordinator
    taskQueue.performTask(); // reap epoch update to coordinator
    EXPECT_TRUE(transport.output.empty());

    segment->close();

    transport.setInput("0 0"); // close/write
    transport.setInput("0 0"); // close/write
    taskQueue.performTask(); // send closes
    // Atomic close.
    EXPECT_TRUE(transport.outputMatches(0, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 1, 10, 0, false, true, true, true, certificate}));
    // Normal close.
    EXPECT_TRUE(transport.outputMatches(1, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 888, 1, 10, 0, false, true, false, true, certificate}));
    transport.clearOutput();
    taskQueue.performTask(); // reap closes
    EXPECT_TRUE(transport.output.empty());
}

TEST_F(ReplicatedSegmentTest, performFreeNothingToDo) {
    segment->freeQueued = true;
    segment->performFree(segment->replicas[0]);
    EXPECT_FALSE(segment->replicas[0].isActive);
    EXPECT_TRUE(segment->isScheduled());
    reset();
}

TEST_F(ReplicatedSegmentTest, performFreeRpcIsReady) {
    reset();

    transport.setInput("0");     // freeSegment response

    segment->freeQueued = true;
    segment->replicas[0].start(backupId1);
    segment->replicas[0].freeRpc.construct(&context, backupId1,
                                           masterId, segmentId);
    freeRpcsInFlight = 2;
    EXPECT_STREQ("sendRequest: 0x1001c 0 0 999 0 888 0",
                 transport.outputLog.c_str());
    segment->performFree(segment->replicas[0]);
    EXPECT_FALSE(segment->replicas[0].isActive);
    EXPECT_EQ(1u, freeRpcsInFlight);
}

TEST_F(ReplicatedSegmentTest, performFreeRpcFailed) {
    transport.clearInput();

    reset();

    segment->freeQueued = true;
    segment->replicas[0].start({99, 99});
    segment->replicas[0].freeRpc.construct(&context, ServerId(99, 99),
                                           masterId, segmentId);
    freeRpcsInFlight = 1;
    TestLog::Enable _;
    segment->performFree(segment->replicas[0]);
    EXPECT_EQ("performFree: ServerNotUpException thrown", TestLog::get());
    EXPECT_TRUE(segment->freeQueued);
    EXPECT_FALSE(segment->isScheduled());
    ASSERT_FALSE(segment->replicas[0].isActive);
    EXPECT_FALSE(segment->replicas[0].freeRpc);
    EXPECT_EQ(0u, freeRpcsInFlight);

    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performFreeTooManyInFlight) {
    transport.clearInput();

    reset();
    freeRpcsInFlight = ReplicatedSegment::MAX_FREE_RPCS_IN_FLIGHT;

    segment->replicas[0].start({99, 99});
    segment->freeQueued = true;

    TestLog::Enable _;
    segment->performFree(segment->replicas[0]);
    EXPECT_TRUE(segment->freeQueued);
    EXPECT_TRUE(segment->isScheduled());
    ASSERT_TRUE(segment->replicas[0].isActive);
    EXPECT_FALSE(segment->replicas[0].freeRpc);
    EXPECT_EQ(ReplicatedSegment::MAX_FREE_RPCS_IN_FLIGHT, freeRpcsInFlight);
    reset();
}

TEST_F(ReplicatedSegmentTest, performFreeStartRpc) {
    transport.clearInput();

    reset();
    freeRpcsInFlight = ReplicatedSegment::MAX_FREE_RPCS_IN_FLIGHT-1;

    segment->replicas[0].start({99, 99});
    segment->freeQueued = true;

    TestLog::Enable _;
    segment->performFree(segment->replicas[0]);
    EXPECT_TRUE(segment->freeQueued);
    EXPECT_TRUE(segment->isScheduled());
    ASSERT_TRUE(segment->replicas[0].isActive);
    EXPECT_TRUE(segment->replicas[0].freeRpc);
    EXPECT_EQ(ReplicatedSegment::MAX_FREE_RPCS_IN_FLIGHT, freeRpcsInFlight);
    reset();
}

TEST_F(ReplicatedSegmentTest, performWriteTooManyInFlight) {
    Segment::Certificate openingCertificate;
    createSegment->logSegment.getAppendedLength(&openingCertificate);

    transport.setInput("0 0"); // open/write
    transport.setInput("0 0"); // open/write
    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // write

    taskQueue.performTask(); // send opens
    taskQueue.performTask(); // reap opens
    transport.clearOutput();
    ASSERT_TRUE(segment->replicas[0].isActive);
    EXPECT_FALSE(segment->replicas[0].writeRpc);
    ASSERT_TRUE(segment->replicas[1].isActive);
    EXPECT_FALSE(segment->replicas[1].writeRpc);

    writeRpcsInFlight = ReplicatedSegment::MAX_WRITE_RPCS_IN_FLIGHT;
    createSegment->logSegment.head = openLen + 10; // write queued
    segment->queued.bytes = openLen + 10;
    segment->schedule();
    taskQueue.performTask(); // try to send writes, shouldn't be able to.
    EXPECT_TRUE(transport.output.empty());

    EXPECT_TRUE(segment->replicas[0].isActive);
    EXPECT_EQ(openLen, segment->replicas[0].sent.bytes);
    EXPECT_EQ(ReplicatedSegment::MAX_WRITE_RPCS_IN_FLIGHT, writeRpcsInFlight);

    writeRpcsInFlight = ReplicatedSegment::MAX_WRITE_RPCS_IN_FLIGHT - 1;
    taskQueue.performTask(); // retry writes since a slot freed up
    EXPECT_TRUE(transport.outputMatches(0, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 0, 10, 10, false, false, true, true,
                 openingCertificate},
                "klmnopqrst", 10));
    transport.clearOutput();
    EXPECT_EQ(ReplicatedSegment::MAX_WRITE_RPCS_IN_FLIGHT, writeRpcsInFlight);
    ASSERT_TRUE(segment->replicas[0].isActive);
    EXPECT_TRUE(segment->replicas[0].writeRpc);
    EXPECT_EQ(openLen + 10, segment->replicas[0].sent.bytes);
    EXPECT_TRUE(segment->replicas[1].isActive);
    EXPECT_EQ(openLen, segment->replicas[1].sent.bytes);
    EXPECT_TRUE(segment->isScheduled());

    taskQueue.performTask(); // reap write and send the second replica's rpc
    EXPECT_TRUE(transport.outputMatches(0, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 888, 0, 10, 10, false, false, false,
                 true, openingCertificate},
                "klmnopqrst", 10));
    EXPECT_EQ(ReplicatedSegment::MAX_WRITE_RPCS_IN_FLIGHT, writeRpcsInFlight);
    ASSERT_TRUE(segment->replicas[1].isActive);
    EXPECT_TRUE(segment->replicas[1].writeRpc);
    EXPECT_EQ(openLen + 10, segment->replicas[1].sent.bytes);
    EXPECT_FALSE(segment->replicas[0].writeRpc); // make sure one was started
    EXPECT_TRUE(segment->isScheduled());

    taskQueue.performTask(); // reap write
    EXPECT_FALSE(segment->replicas[1].writeRpc);
    EXPECT_EQ(uint32_t(ReplicatedSegment::MAX_WRITE_RPCS_IN_FLIGHT - 1),
              writeRpcsInFlight);
    EXPECT_FALSE(segment->isScheduled());
    EXPECT_EQ(0u, deleter.count);
}

TEST_F(ReplicatedSegmentTest, performWriteOpen) {
    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // write

    segment->close();
    {
        TestLog::Enable _(filter);
        taskQueue.performTask();
        EXPECT_EQ("performWrite: Starting replication of segment 888 replica "
                      "slot 0 on backup 0.0 | "
                  "performWrite: Sending open to backup 0.0 | "
                  "selectSecondary: conflicting backupId: 0.0 | "
                  "performWrite: Starting replication of segment 888 replica "
                      "slot 1 on backup 1.0 | "
                  "performWrite: Sending open to backup 1.0",
                  TestLog::get());
    }

    Segment::Certificate certificate;
    createSegment->logSegment.getAppendedLength(&certificate);
    EXPECT_TRUE(transport.outputMatches(0, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 0, 0, 10, true, false, true, true, certificate},
                "abcdefghij", 10));
    EXPECT_TRUE(transport.outputMatches(1, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 888, 0, 0, 10, true, false, false, true, certificate},
                "abcdefghij", 10));

    EXPECT_TRUE(segment->replicas[0].isActive);
    EXPECT_TRUE(segment->replicas[0].writeRpc);
    EXPECT_TRUE(segment->replicas[0].sent.open);
    EXPECT_EQ(openLen, segment->replicas[0].sent.bytes);
    EXPECT_TRUE(segment->isScheduled());
    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performWriteOpenTooManyInFlight) {
    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // write

    writeRpcsInFlight = ReplicatedSegment::MAX_WRITE_RPCS_IN_FLIGHT;
    taskQueue.performTask(); // try to send writes, shouldn't be able to.

    EXPECT_TRUE(segment->replicas[0].isActive);
    EXPECT_FALSE(segment->replicas[0].sent.open);
    EXPECT_EQ(ReplicatedSegment::MAX_WRITE_RPCS_IN_FLIGHT, writeRpcsInFlight);

    writeRpcsInFlight = ReplicatedSegment::MAX_WRITE_RPCS_IN_FLIGHT - 1;
    taskQueue.performTask(); // retry writes since a slot freed up
    Segment::Certificate certificate;
    createSegment->logSegment.getAppendedLength(&certificate);
    EXPECT_TRUE(transport.outputMatches(0, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 0, 0, 10, true, false, true, true, certificate},
                "abcdefghij", 10));
    EXPECT_EQ(ReplicatedSegment::MAX_WRITE_RPCS_IN_FLIGHT, writeRpcsInFlight);
    ASSERT_TRUE(segment->replicas[0].isActive);
    EXPECT_TRUE(segment->replicas[0].writeRpc);
    EXPECT_TRUE(segment->replicas[0].sent.open);
    EXPECT_EQ(openLen, segment->replicas[0].sent.bytes);
    EXPECT_TRUE(segment->replicas[1].isActive);
    EXPECT_FALSE(segment->replicas[1].sent.open); // ensure only one was started
    EXPECT_TRUE(segment->isScheduled());

    taskQueue.performTask(); // reap write and send the second replica's rpc
    EXPECT_TRUE(transport.outputMatches(1, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 888, 0, 0, 10, true, false, false, true, certificate},
                "abcdefghij", 10));
    EXPECT_EQ(ReplicatedSegment::MAX_WRITE_RPCS_IN_FLIGHT, writeRpcsInFlight);
    ASSERT_TRUE(segment->replicas[1].isActive);
    EXPECT_TRUE(segment->replicas[1].writeRpc);
    EXPECT_TRUE(segment->replicas[1].sent.open);
    EXPECT_EQ(openLen, segment->replicas[1].sent.bytes);
    EXPECT_FALSE(segment->replicas[0].writeRpc); // make sure one was started
    EXPECT_TRUE(segment->isScheduled());

    taskQueue.performTask(); // reap write
    EXPECT_FALSE(segment->replicas[1].writeRpc);
    EXPECT_EQ(uint32_t(ReplicatedSegment::MAX_WRITE_RPCS_IN_FLIGHT - 1),
              writeRpcsInFlight);
    EXPECT_FALSE(segment->isScheduled());
    EXPECT_EQ(0u, deleter.count);
}

TEST_F(ReplicatedSegmentTest, performWriteRpcIsReady) {
    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // write

    segment->close();

    taskQueue.performTask();
    ASSERT_TRUE(segment->replicas[0].isActive);
    EXPECT_EQ(openLen, segment->replicas[0].sent.bytes);
    EXPECT_EQ(0u, segment->replicas[0].acked.bytes);
    EXPECT_EQ(0u, segment->replicas[0].committed.bytes);

    taskQueue.performTask();
    ASSERT_TRUE(segment->replicas[0].isActive);
    EXPECT_EQ(openLen, segment->replicas[0].acked.bytes);
    EXPECT_EQ(openLen, segment->replicas[0].committed.bytes);
    EXPECT_TRUE(segment->isScheduled());
    EXPECT_FALSE(segment->replicas[0].writeRpc);
    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performWriteRpcFailed) {
    ServerIdRpcWrapper::ConvertExceptionsToDoesntExist _;
    transport.clearInput();
    transport.setInput("0 0"); // ok first replica open
    transport.setInput(NULL); // error second replica open
    transport.setInput("0 0"); // ok second replica reopen
    transport.setInput(NULL); // error first replica close
    transport.setInput("0 0"); // ok second replica close
    transport.setInput("0 0"); // ok first replica reclose

    taskQueue.performTask();  // send open requests
    ASSERT_TRUE(segment->replicas[0].isActive);
    EXPECT_EQ(openLen, segment->replicas[0].sent.bytes);
    EXPECT_EQ(0u, segment->replicas[0].acked.bytes);
    EXPECT_EQ(0u, segment->replicas[0].acked.bytes);
    ASSERT_TRUE(segment->replicas[1].isActive);
    EXPECT_EQ(openLen, segment->replicas[1].sent.bytes);
    ServerId backupIdForFirstOpenAttempt = segment->replicas[1].backupId;

    Segment::Certificate certificate;
    createSegment->logSegment.getAppendedLength(&certificate);
    EXPECT_TRUE(transport.outputMatches(0, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 0, 0, 10, true, false, true, true, certificate},
                "abcdefghij", 10));
    EXPECT_TRUE(transport.outputMatches(1, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 888, 0, 0, 10, true, false, false, true, certificate},
                "abcdefghij", 10));
    transport.clearOutput();
    {
        TestLog::Enable _;
        taskQueue.performTask();  // reap rpcs, second replica got error
        EXPECT_TRUE(TestUtil::matchesPosixRegex(
            "performWrite: Couldn't write to backup 1.0; server is down",
            TestLog::get()));
    }
    EXPECT_TRUE(segment->isScheduled());
    ASSERT_TRUE(segment->replicas[0].isActive);
    EXPECT_EQ(openLen, segment->replicas[0].acked.bytes);
    EXPECT_EQ(openLen, segment->replicas[0].committed.bytes);
    EXPECT_FALSE(segment->replicas[0].writeRpc);
    ASSERT_TRUE(segment->replicas[1].isActive);
    EXPECT_EQ(0u, segment->replicas[1].acked.bytes);
    EXPECT_EQ(0u, segment->replicas[1].committed.bytes);
    // Ensure retried open rpc goes to the same backup as the first attempt.
    EXPECT_EQ(backupIdForFirstOpenAttempt, segment->replicas[1].backupId);

    taskQueue.performTask();  // resend second open request
    EXPECT_TRUE(transport.outputMatches(0, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 888, 0, 0, 10, true, false, false, true, certificate},
                "abcdefghij", 10));
    transport.clearOutput();
    taskQueue.performTask();  // reap second open request

    createSegment->logSegment.head = openLen + 10; // write queued
    segment->close();
    createSegment->logSegment.getAppendedLength(&certificate);
    taskQueue.performTask();  // send close requests
    EXPECT_TRUE(transport.outputMatches(0, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 0, 10, 10, false, true, true, true, certificate},
                "klmnopqrst", 10));
    EXPECT_TRUE(transport.outputMatches(1, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 888, 0, 10, 10, false, true, false, true, certificate},
                "klmnopqrst", 10));
    transport.clearOutput();
    {
        TestLog::Enable _;
        taskQueue.performTask();  // reap rpcs, first replica got error
        EXPECT_TRUE(TestUtil::matchesPosixRegex(
            "performWrite: Couldn't write to backup 0.0; server is down",
            TestLog::get()));
    }
    EXPECT_TRUE(segment->isScheduled());
    ASSERT_TRUE(segment->replicas[0].isActive);
    EXPECT_EQ(openLen, segment->replicas[0].committed.bytes);
    EXPECT_EQ(openLen, segment->replicas[0].acked.bytes);
    EXPECT_EQ(openLen, segment->replicas[0].sent.bytes);
    EXPECT_FALSE(segment->replicas[0].writeRpc);
    ASSERT_TRUE(segment->replicas[1].isActive);
    EXPECT_EQ(openLen + 10, segment->replicas[1].committed.bytes);
    EXPECT_EQ(openLen + 10, segment->replicas[1].acked.bytes);
    EXPECT_EQ(openLen + 10, segment->replicas[1].sent.bytes);
    EXPECT_FALSE(segment->replicas[1].writeRpc);

    taskQueue.performTask();  // resend first close request
    EXPECT_TRUE(transport.outputMatches(0, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 0, 10, 10, false, true, true, true, certificate},
                "klmnopqrst", 10));
    transport.clearOutput();
    EXPECT_TRUE(segment->isScheduled());
    ASSERT_TRUE(segment->replicas[0].isActive);
    EXPECT_EQ(openLen + 10, segment->replicas[0].sent.bytes);
    EXPECT_TRUE(segment->replicas[0].writeRpc);

    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performWriteMoreToSend) {
    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // write

    createSegment->logSegment.head = openLen + 20; // write queued
    Segment::Certificate openingCertificate;
    createSegment->logSegment.getAppendedLength(&openingCertificate);
    createSegment->logSegment.checksum.result = 0xff00ff00;
    segment->close();
    Segment::Certificate closingCertificate;
    createSegment->logSegment.getAppendedLength(&closingCertificate);
    taskQueue.performTask(); // send open
    EXPECT_TRUE(segment->isScheduled());
    taskQueue.performTask(); // reap opens
    EXPECT_TRUE(segment->isScheduled());
    transport.clearOutput();
    taskQueue.performTask(); // send second round

    EXPECT_TRUE(transport.outputMatches(0, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 0, 10, 20, false, true, true, true,
                 closingCertificate},
                "klmnopqrstuvwxyzabcd", 20));
    EXPECT_TRUE(transport.outputMatches(1, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 888, 0, 10, 20, false, true, false, true,
                 closingCertificate},
                "klmnopqrstuvwxyzabcd", 20));
    EXPECT_TRUE(segment->isScheduled());
    EXPECT_TRUE(segment->replicas[0].writeRpc);

    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performWriteClosedButLongerThanMaxTxLimit) {
    Segment::Certificate emptyCertificate;
    transport.setInput("0 0"); // open/write
    transport.setInput("0 0"); // open/write
    taskQueue.performTask(); // send open
    EXPECT_TRUE(segment->isScheduled());
    taskQueue.performTask(); // reap opens

    // oversized write queued
    createSegment->logSegment.head =
        segment->maxBytesPerWriteRpc + segment->openLen + 1;
    segment->close();
    EXPECT_TRUE(segment->isScheduled());
    Segment::Certificate certificate;
    createSegment->logSegment.getAppendedLength(&certificate);

    transport.clearOutput();
    transport.setInput("0 0"); // write
    transport.setInput("0 0"); // write
    taskQueue.performTask(); // send second round

    EXPECT_TRUE(transport.outputMatches(0, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 0, 10, 21, false, false, true, false,
                 emptyCertificate},
                "klmnopqrstuvwxyzabcde", 21));
    EXPECT_TRUE(transport.outputMatches(1, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 888, 0, 10, 21, false, false, false, false,
                 emptyCertificate},
                "klmnopqrstuvwxyzabcde", 21));
    EXPECT_TRUE(segment->isScheduled());
    EXPECT_TRUE(segment->replicas[0].writeRpc);
    transport.clearOutput();

    taskQueue.performTask(); // reap second round
    transport.setInput("0 0"); // close
    transport.setInput("0 0"); // close
    taskQueue.performTask(); // send third (closing) round

    EXPECT_TRUE(transport.outputMatches(0, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 0},
                 999, 888, 0, 31, 1, false, true, true, true, certificate},
                "f", 1));
    EXPECT_TRUE(transport.outputMatches(1, MockTransport::SEND_REQUEST,
        WrReq{{BACKUP_WRITE, BACKUP_SERVICE, 1},
                 999, 888, 0, 31, 1, false, true, false, true, certificate},
                "f", 1));
    EXPECT_TRUE(segment->isScheduled());
    EXPECT_TRUE(segment->replicas[0].writeRpc);

    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performWriteEnsureNewHeadOpenAckedBeforeClose) {
    transport.setInput("0 0"); // write - segment open
    transport.setInput("0 0"); // write - segment open
    transport.setInput("0 0"); // write - newHead open
    transport.setInput("0 0"); // write - newHead open
    transport.setInput("0 0"); // write - segment close
    transport.setInput("0 0"); // write - segment close

    taskQueue.performTask(); // send segment open
    taskQueue.performTask();
    taskQueue.performTask(); // reap segment open
    taskQueue.performTask();

    CreateSegment createSegment(this, segment, segmentId + 1, numReplicas);
    auto newHead = createSegment.segment.get();
    segment->close();

    taskQueue.performTask(); // send newHead open, try segment close but can't
    taskQueue.performTask();

    EXPECT_TRUE(newHead->isScheduled());
    ASSERT_TRUE(newHead->replicas[0].isActive);
    EXPECT_TRUE(newHead->replicas[0].writeRpc);
    EXPECT_TRUE(newHead->replicas[0].sent.open);
    EXPECT_FALSE(newHead->replicas[0].acked.open);
    EXPECT_FALSE(newHead->replicas[0].committed.open);

    EXPECT_TRUE(segment->isScheduled());
    ASSERT_TRUE(segment->replicas[0].isActive);
    EXPECT_FALSE(segment->replicas[0].writeRpc);
    EXPECT_FALSE(segment->replicas[0].sent.close);

    taskQueue.performTask(); // reap newHead open, try segment close should work
    taskQueue.performTask();

    EXPECT_FALSE(newHead->isScheduled());
    ASSERT_TRUE(newHead->replicas[0].isActive);
    EXPECT_FALSE(newHead->replicas[0].writeRpc);
    EXPECT_TRUE(newHead->replicas[0].acked.open);
    EXPECT_TRUE(newHead->replicas[0].committed.open);

    EXPECT_TRUE(segment->isScheduled());
    ASSERT_TRUE(segment->replicas[0].isActive);
    EXPECT_TRUE(segment->replicas[0].writeRpc);
    EXPECT_TRUE(segment->replicas[0].sent.close);

    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performWriteEnsureCloseBeforeNewHeadWrittenTo) {
    CreateSegment newCreateSegment(this, segment, segmentId + 1, numReplicas);
    auto newHead = newCreateSegment.segment.get();

    transport.setInput("0 0"); // write - segment open
    transport.setInput("0 0"); // write - segment open
    taskQueue.performTask(); // send segment open for segment
    taskQueue.performTask(); // cannot send open for newHead
                             // (first open not yet durable)
    taskQueue.performTask(); // reap segment open for segment
    transport.setInput("0 0"); // write - newHead open
    transport.setInput("0 0"); // write - newHead open
    taskQueue.performTask(); // send segment open for newHead
    taskQueue.performTask(); // segment - nothing to do
    taskQueue.performTask(); // reap segment open for newHead

    segment->close(); // close queued
    newCreateSegment.logSegment.head = openLen + 10; // write queued
    newHead->queued.bytes = openLen + 10;
    newHead->schedule();

    EXPECT_TRUE(newHead->isScheduled());
    EXPECT_FALSE(newHead->precedingSegmentCloseCommitted);
    ASSERT_TRUE(newHead->replicas[0].isActive);
    EXPECT_FALSE(newHead->replicas[0].writeRpc);
    EXPECT_TRUE(newHead->replicas[0].acked.open);
    EXPECT_TRUE(newHead->replicas[0].committed.open);
    EXPECT_EQ(openLen, newHead->replicas[0].sent.bytes);

    EXPECT_TRUE(segment->isScheduled());
    ASSERT_TRUE(segment->replicas[0].isActive);
    EXPECT_FALSE(segment->replicas[0].writeRpc);
    EXPECT_FALSE(segment->replicas[0].sent.close);
    EXPECT_FALSE(segment->replicas[0].acked.close);
    EXPECT_FALSE(segment->replicas[0].committed.close);

    transport.setInput("0 0"); // write - segment close replica 1
    transport.setInput("0 0"); // write - segment close replica 2
    taskQueue.performTask(); // send close rpcs for segment
    transport.setInput("0 0"); // write - newHead
    transport.setInput("0 0"); // write - newHead
    TestLog::Enable _(filter);
    taskQueue.performTask(); // try newHead write but can't
    EXPECT_EQ(
        "performWrite: Cannot write segment 889 until preceding segment "
        "is durably closed | "
        "performWrite: Cannot write segment 889 until preceding segment "
        "is durably closed",
        TestLog::get());

    EXPECT_TRUE(newHead->isScheduled());
    EXPECT_FALSE(newHead->precedingSegmentCloseCommitted);
    ASSERT_TRUE(newHead->replicas[0].isActive);
    EXPECT_FALSE(newHead->replicas[0].writeRpc);
    EXPECT_TRUE(newHead->replicas[0].acked.open);
    EXPECT_TRUE(newHead->replicas[0].committed.open);
    EXPECT_EQ(openLen, newHead->replicas[0].sent.bytes);

    EXPECT_TRUE(segment->isScheduled());
    ASSERT_TRUE(segment->replicas[0].isActive);
    EXPECT_TRUE(segment->replicas[0].writeRpc);
    EXPECT_TRUE(segment->replicas[0].sent.close);
    EXPECT_FALSE(segment->replicas[0].acked.close);
    EXPECT_FALSE(segment->replicas[0].committed.close);

    taskQueue.performTask(); // reap close rpcs on replicas

    EXPECT_TRUE(newHead->isScheduled());
    EXPECT_TRUE(newHead->precedingSegmentCloseCommitted);
    ASSERT_TRUE(newHead->replicas[0].isActive);
    EXPECT_FALSE(newHead->replicas[0].writeRpc);
    EXPECT_TRUE(newHead->replicas[0].acked.open);
    EXPECT_TRUE(newHead->replicas[0].committed.open);
    EXPECT_EQ(openLen, newHead->replicas[0].sent.bytes);

    EXPECT_FALSE(segment->isScheduled());
    ASSERT_TRUE(segment->replicas[0].isActive);
    EXPECT_FALSE(segment->replicas[0].writeRpc);
    EXPECT_TRUE(segment->replicas[0].sent.close);
    EXPECT_TRUE(segment->replicas[0].acked.close);
    EXPECT_TRUE(segment->replicas[0].committed.close);
    ASSERT_TRUE(segment->replicas[1].isActive);
    EXPECT_FALSE(segment->replicas[1].writeRpc);
    EXPECT_TRUE(segment->replicas[1].sent.close);
    EXPECT_TRUE(segment->replicas[1].acked.close);
    EXPECT_TRUE(segment->replicas[1].committed.close);

    taskQueue.performTask(); // send newHead write

    EXPECT_TRUE(newHead->isScheduled());
    EXPECT_TRUE(newHead->precedingSegmentCloseCommitted);
    ASSERT_TRUE(newHead->replicas[0].isActive);
    EXPECT_TRUE(newHead->replicas[0].writeRpc);
    EXPECT_EQ(openLen + 10, newHead->replicas[0].sent.bytes);

    EXPECT_FALSE(segment->isScheduled());
    ASSERT_TRUE(segment->replicas[0].isActive);
    EXPECT_FALSE(segment->replicas[0].writeRpc);
    EXPECT_TRUE(segment->replicas[0].acked.close);
    EXPECT_TRUE(segment->replicas[0].committed.close);

    EXPECT_EQ(0u, deleter.count);
    newHead->scheduled = false;
    reset();
}

TEST_F(ReplicatedSegmentTest, performWriteBackupRejectedOpen) {
    transport.setInput("0 0"); // write - open
    transport.setInput("13 0"); // write - open rejected
    transport.setInput("12 0"); // write - bad segment id exception

    taskQueue.performTask(); // send

    TestLog::Enable _(filter);
    taskQueue.performTask(); // reap - second replica gets rejected
    EXPECT_EQ(
        "performWrite: Write RPC finished for replica slot 0 | "
        "performWrite: Couldn't open replica on backup 1.0; server may be "
        "overloaded or may already have a replica for this segment which "
        "was found on disk after a crash; will choose another backup",
        TestLog::get());
    ASSERT_TRUE(segment->replicas[0].isActive);
    EXPECT_EQ(openLen, segment->replicas[0].sent.bytes);
    EXPECT_EQ(10u, segment->replicas[0].acked.bytes);
    EXPECT_EQ(10u, segment->replicas[0].committed.bytes);

    ASSERT_FALSE(segment->replicas[1].isActive);
    EXPECT_EQ(0u, segment->replicas[1].sent.bytes);
    EXPECT_EQ(0u, segment->replicas[1].acked.bytes);
    EXPECT_EQ(0u, segment->replicas[1].committed.bytes);
    EXPECT_FALSE(segment->replicas[1].writeRpc);
    EXPECT_EQ(ServerId(), segment->replicas[1].backupId);

    taskQueue.performTask(); // send
    // Next performTask() should blow up whole server: backup can throw
    // this if the server issues a write to a replica that wasn't created
    // by that backup but that it found on disk instead.
    EXPECT_THROW(taskQueue.performTask(),
                 BackupBadSegmentIdException);

    reset();
}

namespace {
bool performWriteFilter(string s) {
    return s == "performWrite";
}
}

TEST_F(ReplicatedSegmentTest, performWrite_CallerNotInClusterException) {
    TestLog::Enable _(performWriteFilter);
    context.coordinatorSession->setLocation("mock:host=coord");
    // Arrange for RPC responses:
    transport.setInput("0");  // accept write for first replica
    transport.setInput("26"); // reject write for 2nd replica - caller not
                              // in cluster
    transport.setInput("0");  // verifyMembership response
    transport.setInput("0");  // accept write retry for 2nd replica

    taskQueue.performTask();
    // At this point writes should have been issued for both replicas.
    EXPECT_EQ("performWrite: Starting replication of segment 888 replica "
                  "slot 0 on backup 0.0 | "
              "performWrite: Sending open to backup 0.0 | "
              "performWrite: Starting replication of segment 888 replica "
                  "slot 1 on backup 1.0 | "
              "performWrite: Sending open to backup 1.0",
              TestLog::get());
    TestLog::reset();

    taskQueue.performTask();
    // The first replica write has now succeeded. The second has completed
    // with an error, and verifyMembership has been invoked (and succeeded).
    EXPECT_EQ("performWrite: Write RPC finished for replica slot 0 | "
              "performWrite: Backup write RPC rejected by 1.0 with "
                  "STATUS_CALLER_NOT_IN_CLUSTER",
              TestLog::get());
    TestLog::reset();

    taskQueue.performTask();
    // The second replica write has now been reissued.
    EXPECT_EQ("performWrite: Sending open to backup 1.0",
              TestLog::get());
    TestLog::reset();

    taskQueue.performTask();
    // The second replica write retry has now succeeded.
    EXPECT_EQ("performWrite: Write RPC finished for replica slot 1",
              TestLog::get());
    reset();
}

TEST_F(ReplicatedSegmentTest, performWriteEnsureDurableOpensOrdered) {
    CreateSegment createSegment(this, segment, segmentId + 1, numReplicas);
    auto newHead = createSegment.segment.get();
    segment->close(); // close queued

    // Mess up the queue order to simulate reorder due to failure.
    // This means newHead will be going first at 'taking turns'
    // with segment.
    {
        Task* t = taskQueue.tasks.front();
        taskQueue.tasks.pop();
        taskQueue.tasks.push(t);
    }

    TestLog::Enable _(performWriteFilter);
    TestLog::reset();
    taskQueue.performTask(); // new head cannot open until first is acked
    EXPECT_EQ(
        "performWrite: Starting replication of segment 889 replica slot 0 "
            "on backup 0.0 | "
        "performWrite: Cannot open segment 889 until preceding segment "
            "is durably open | "
        "performWrite: Starting replication of segment 889 replica slot 1 "
            "on backup 1.0 | "
        "performWrite: Cannot open segment 889 until preceding segment "
            "is durably open",
        TestLog::get());
    EXPECT_TRUE(newHead->isScheduled());
    EXPECT_FALSE(newHead->precedingSegmentOpenCommitted);
    ASSERT_TRUE(newHead->replicas[0].isActive);
    EXPECT_FALSE(newHead->replicas[0].writeRpc);
    EXPECT_FALSE(newHead->replicas[0].committed.open);
    EXPECT_FALSE(newHead->replicas[0].acked.open);
    EXPECT_EQ(0lu, newHead->replicas[0].sent.bytes);

    transport.setInput("0 0"); // write - segment open
    transport.setInput("0 0"); // write - segment open
    taskQueue.performTask(); // send segment open for first segment
    TestLog::reset();
    taskQueue.performTask(); // new head cannot open until first is acked
    EXPECT_EQ(
        "performWrite: Cannot open segment 889 until preceding segment "
            "is durably open | "
        "performWrite: Cannot open segment 889 until preceding segment "
            "is durably open",
        TestLog::get());
    taskQueue.performTask(); // reap segment open for first segment
    TestLog::reset();
    transport.setInput("0 0"); // write - newHead open
    transport.setInput("0 0"); // write - newHead open
    taskQueue.performTask(); // send segment open for second semgent
    EXPECT_EQ(
        "performWrite: Sending open to backup 0.0 | "
        "performWrite: Sending open to backup 1.0",
        TestLog::get());
    EXPECT_TRUE(newHead->isScheduled());
    EXPECT_TRUE(newHead->precedingSegmentOpenCommitted);
    ASSERT_TRUE(newHead->replicas[0].isActive);
    EXPECT_TRUE(newHead->replicas[0].writeRpc);
    EXPECT_FALSE(newHead->replicas[0].committed.open);
    EXPECT_FALSE(newHead->replicas[0].acked.open);
    EXPECT_EQ(openLen, newHead->replicas[0].sent.bytes);

    taskQueue.performTask();
    taskQueue.performTask(); // reap open rpc on new head
    EXPECT_TRUE(newHead->replicas[0].committed.open);
    EXPECT_TRUE(newHead->replicas[0].acked.open);
}

TEST_F(ReplicatedSegmentTest, performWriteEnsureDurableOpensOrderedAlreadyOpen)
{
    transport.setInput("0 0"); // write - segment open
    transport.setInput("0 0"); // write - segment open
    taskQueue.performTask(); // send segment open for first segment
    taskQueue.performTask(); // reap segment open for first segment

    CreateSegment createSegment(this, segment, segmentId + 1, numReplicas);
    auto newHead = createSegment.segment.get();
    segment->close(); // close queued

    EXPECT_TRUE(newHead->precedingSegmentOpenCommitted);
}

} // namespace RAMCloud
