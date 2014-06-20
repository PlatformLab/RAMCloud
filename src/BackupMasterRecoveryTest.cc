/* Copyright (c) 2012-2013 Stanford University
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
#include "BackupMasterRecovery.h"
#include "InMemoryStorage.h"
#include "LogEntryTypes.h"
#include "ProtoBuf.h"
#include "ShortMacros.h"
#include "StringUtil.h"
#include "TabletsBuilder.h"
#include "TaskQueue.h"
#include "Tablets.pb.h"

namespace RAMCloud {

struct BackupMasterRecoveryTest : public ::testing::Test {
    TaskQueue taskQueue;
    ProtoBuf::RecoveryPartition partitions;
    uint32_t segmentSize;
    InMemoryStorage storage;
    std::vector<BackupStorage::FrameRef> frames;
    Buffer source;
    ServerId crashedMasterId;
    Tub<BackupMasterRecovery> recovery;

    BackupMasterRecoveryTest()
        : taskQueue()
        , partitions()
        , segmentSize()
        , storage(1024, 6, 0)
        , frames()
        , source()
        , crashedMasterId(99, 0)
        , recovery()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
        ProtoBuf::Tablets tablets;
        TabletsBuilder{tablets}
            (1, 0lu, 10lu, TabletsBuilder::NORMAL, 0lu)  // partition 0
            (1, 11lu, ~0lu, TabletsBuilder::NORMAL, 1lu)  // partition 1
            (2, 0lu, ~0lu, TabletsBuilder::NORMAL, 0lu)  // partition 0
            (3, 0lu, ~0lu, TabletsBuilder::NORMAL, 0lu); // partition 0
        source.append("test", 5);
        for (int i = 0; i < tablets.tablet_size(); i++) {
            ProtoBuf::Tablets::Tablet& tablet(*partitions.add_tablet());
            tablet = tablets.tablet(i);
        }
        recovery.construct(taskQueue, 456lu, ServerId{99, 0}, segmentSize);
    }

    void
    mockMetadata(uint64_t segmentId,
                 bool closed = true, bool primary = false,
                 bool screwItUp = false)
    {
        frames.emplace_back(storage.open(true));
        Segment::Certificate certificate;
        uint32_t epoch = downCast<uint32_t>(segmentId) + 100;
        BackupReplicaMetadata metadata(certificate, crashedMasterId.getId(),
                                       segmentId, 1024, epoch,
                                       closed, primary);
        if (screwItUp)
            metadata.checksum = 0;
        frames.back()->append(source, 0, 0, 0, &metadata, sizeof(metadata));
    }

    DISALLOW_COPY_AND_ASSIGN(BackupMasterRecoveryTest);
};

namespace {
bool mockExtractDigest(uint64_t segmentId, Buffer* digestBuffer,
                       Buffer* tableStatsBuffer) {
    if (segmentId == 92lu) {
        digestBuffer->reset();
        digestBuffer->append("digest", 7);
        tableStatsBuffer->reset();
        tableStatsBuffer->append("tableStats", 11);
        return true;
    }
    if (segmentId == 93lu) {
        digestBuffer->reset();
        digestBuffer->append("not digest", 11);
        tableStatsBuffer->reset();
        tableStatsBuffer->append("not tableStats", 15);
        return true;
    }
    return false;
}
}

TEST_F(BackupMasterRecoveryTest, start) {
    mockMetadata(88, true, true);
    mockMetadata(89);
    mockMetadata(90, true, false, true); // Screwed up metadata.
    mockMetadata(91, true, true);
    mockMetadata(93, false);
    mockMetadata(92, false);
    recovery->testingExtractDigest = &mockExtractDigest;
    Buffer buffer;
    auto response = buffer.emplaceAppend<BackupMasterRecovery::StartResponse>();
    recovery->start(frames, &buffer, response);
    recovery->setPartitionsAndSchedule(partitions);
    ASSERT_EQ(5u, response->replicaCount);
    EXPECT_EQ(2u, response->primaryReplicaCount);
    // Make sure we got the "lowest" log digest.
    EXPECT_EQ(7u, response->digestBytes);
    EXPECT_EQ(92lu, response->digestSegmentId);
    EXPECT_EQ(192u, response->digestSegmentEpoch);
    EXPECT_EQ(11u, response->tableStatsBytes);

    buffer.truncateFront(sizeof32(BackupMasterRecovery::StartResponse));
    // Verify returned segment ids and lengths.
    typedef WireFormat::BackupStartReadingData::Replica WireReplica;
    const WireReplica* replica = buffer.getStart<WireReplica>();
    EXPECT_TRUE(88lu == replica->segmentId || 91lu == replica->segmentId);
    EXPECT_TRUE(188lu == replica->segmentEpoch ||
                191lu == replica->segmentEpoch);
    buffer.truncateFront(sizeof32(WireReplica));
    replica = buffer.getStart<WireReplica>();
    EXPECT_TRUE(88lu == replica->segmentId || 91lu == replica->segmentId);
    EXPECT_TRUE(188lu == replica->segmentEpoch ||
                191lu == replica->segmentEpoch);
    buffer.truncateFront(sizeof32(WireReplica));
    replica = buffer.getStart<WireReplica>();
    EXPECT_EQ(89lu, replica->segmentId);
    EXPECT_EQ(189lu, replica->segmentEpoch);
    buffer.truncateFront(sizeof32(WireReplica));
    replica = buffer.getStart<WireReplica>();
    EXPECT_EQ(93lu, replica->segmentId);
    EXPECT_EQ(193lu, replica->segmentEpoch);
    buffer.truncateFront(sizeof32(WireReplica));
    replica = buffer.getStart<WireReplica>();
    EXPECT_EQ(92lu, replica->segmentId);
    EXPECT_EQ(192lu, replica->segmentEpoch);
    buffer.truncateFront(sizeof32(WireReplica));
    EXPECT_STREQ("digest", buffer.getStart<char>());

    // Primaries come first; random order.
    EXPECT_TRUE(recovery->replicas[0].metadata->primary);
    EXPECT_TRUE(recovery->replicas[1].metadata->primary);
    EXPECT_FALSE(recovery->replicas[2].metadata->primary);
    EXPECT_FALSE(recovery->replicas[3].metadata->primary);
    EXPECT_FALSE(recovery->replicas[4].metadata->primary);

    // All secondaries are there.
    EXPECT_EQ(89lu, recovery->replicas[2].metadata->segmentId);
    EXPECT_EQ(93lu, recovery->replicas[3].metadata->segmentId);
    EXPECT_EQ(92lu, recovery->replicas[4].metadata->segmentId);

    // Primaries have started loading; secondaries have not.
    // Any open secondaries will still have loads requested because
    // they should have been scanned for a log digest.
    typedef InMemoryStorage::Frame* p;
    EXPECT_TRUE(p(recovery->replicas[0].frame.get())->loadRequested);
    EXPECT_TRUE(p(recovery->replicas[1].frame.get())->loadRequested);
    EXPECT_FALSE(p(recovery->replicas[2].frame.get())->loadRequested);
    EXPECT_TRUE(p(recovery->replicas[3].frame.get())->loadRequested);
    EXPECT_TRUE(p(recovery->replicas[4].frame.get())->loadRequested);

    // Iterator set for build recovery segments.
    EXPECT_EQ(&recovery->replicas.front(), &*recovery->nextToBuild);
    EXPECT_TRUE(recovery->isScheduled());

    // Idempotence.
    buffer.reset();
    response = buffer.emplaceAppend<BackupMasterRecovery::StartResponse>();
    recovery->start(frames, &buffer, response);
    EXPECT_EQ(5u, response->replicaCount);
    EXPECT_EQ(2u, response->primaryReplicaCount);
    EXPECT_EQ(7u, response->digestBytes);
    EXPECT_EQ(92lu, response->digestSegmentId);
    EXPECT_EQ(192u, response->digestSegmentEpoch);
    EXPECT_EQ(11u, response->tableStatsBytes);
    EXPECT_STREQ("digest",
                 buffer.getOffset<char>(buffer.getTotalLength()
                                        - 11 - 7));
    EXPECT_STREQ("tableStats",
                 buffer.getOffset<char>(buffer.getTotalLength()
                                        - 11));
}

TEST_F(BackupMasterRecoveryTest, setPartitionsAndSchedule) {
    TestLog::Enable _;
    recovery.construct(taskQueue, 456lu, ServerId{99, 0}, segmentSize);
    recovery->startCompleted = true;
    recovery->testingSkipBuild = true;

    recovery->setPartitionsAndSchedule(partitions);
    EXPECT_EQ("setPartitionsAndSchedule: Recovery 456 building 2 recovery "
            "segments for each replica for crashed master 99.0 and filtering "
            "them according to the following partitions:\n"
            "tablet {\n"
            "  table_id: 1\n"
            "  start_key_hash: 0\n"
            "  end_key_hash: 10\n"
            "  state: NORMAL\n"
            "  server_id: 0\n"
            "  user_data: 0\n"
            "  ctime_log_head_id: 0\n"
            "  ctime_log_head_offset: 0\n"
            "}\n"
            "tablet {\n"
            "  table_id: 1\n"
            "  start_key_hash: 11\n"
            "  end_key_hash: 18446744073709551615\n"
            "  state: NORMAL\n"
            "  server_id: 0\n"
            "  user_data: 1\n"
            "  ctime_log_head_id: 0\n"
            "  ctime_log_head_offset: 0\n"
            "}\n"
            "tablet {\n"
            "  table_id: 2\n"
            "  start_key_hash: 0\n"
            "  end_key_hash: 18446744073709551615\n"
            "  state: NORMAL\n"
            "  server_id: 0\n"
            "  user_data: 0\n"
            "  ctime_log_head_id: 0\n"
            "  ctime_log_head_offset: 0\n"
            "}\n"
            "tablet {\n"
            "  table_id: 3\n"
            "  start_key_hash: 0\n"
            "  end_key_hash: 18446744073709551615\n"
            "  state: NORMAL\n  server_id: 0\n"
            "  user_data: 0\n  ctime_log_head_id: 0\n"
            "  ctime_log_head_offset: 0\n"
            "}\n"
            " | "
            "setPartitionsAndSchedule: Kicked off building recovery segments | "
            "schedule: scheduled",
              TestLog::get());

    EXPECT_EQ(2, recovery->numPartitions);
    EXPECT_EQ(&recovery->replicas.front(), &*recovery->nextToBuild);
    EXPECT_TRUE(recovery->isScheduled());
}

TEST_F(BackupMasterRecoveryTest, getRecoverySegment) {
    mockMetadata(88); // secondary
    mockMetadata(89, true, true); // primary
    recovery->testingExtractDigest = &mockExtractDigest;
    recovery->testingSkipBuild = true;
    recovery->start(frames, NULL, NULL);
    recovery->setPartitionsAndSchedule(partitions);

    EXPECT_THROW(recovery->getRecoverySegment(456, 89, 0, NULL, NULL),
                 RetryException);

    taskQueue.performTask();

    Status status = recovery->getRecoverySegment(456, 88, 0, NULL, NULL);
    EXPECT_EQ(STATUS_OK, status);
    Buffer buffer;
    buffer.append("important", 10);
    ASSERT_TRUE(recovery->replicas[1].recoverySegments[0].append(
        LOG_ENTRY_TYPE_OBJ, buffer));
    buffer.reset();
    Segment::Certificate certificate;
    memset(&certificate, 0xff, sizeof(certificate));
    status = recovery->getRecoverySegment(456, 88, 0, &buffer, &certificate);
    EXPECT_EQ(STATUS_OK, status);
    EXPECT_EQ(12lu, certificate.segmentLength);
    EXPECT_STREQ("important",
                 buffer.getOffset<char>(buffer.getTotalLength() - 10));
}

TEST_F(BackupMasterRecoveryTest, getRecoverySegment_exceptionDuringBuild) {
    mockMetadata(88);
    recovery->start(frames, NULL, NULL);
    recovery->setPartitionsAndSchedule(partitions);
    EXPECT_THROW(recovery->getRecoverySegment(456, 88, 0, NULL, NULL),
                 SegmentRecoveryFailedException);
}

TEST_F(BackupMasterRecoveryTest, getRecoverySegment_badArgs) {
    mockMetadata(88);
    recovery->testingExtractDigest = &mockExtractDigest;
    recovery->testingSkipBuild = true;
    recovery->start(frames, NULL, NULL);
    recovery->setPartitionsAndSchedule(partitions);
    EXPECT_THROW(recovery->getRecoverySegment(455, 88, 0, NULL, NULL),
                 BackupBadSegmentIdException);
    recovery->getRecoverySegment(456, 88, 0, NULL, NULL);
    EXPECT_THROW(recovery->getRecoverySegment(456, 89, 0, NULL, NULL),
                 BackupBadSegmentIdException);
    EXPECT_THROW(recovery->getRecoverySegment(456, 88, 1000, NULL, NULL),
                 BackupBadSegmentIdException);
}

TEST_F(BackupMasterRecoveryTest, free) {
    std::unique_ptr<BackupMasterRecovery> recovery(
        new BackupMasterRecovery(taskQueue, 456lu, ServerId{99, 0},
                                 segmentSize));
    TestLog::Enable _;
    recovery->free();
    taskQueue.performTask();
    recovery.release();
    EXPECT_EQ("free: Recovery 456 for crashed master 99.0 is no longer needed; "
              "will clean up as next possible chance. | "
              "schedule: scheduled | "
              "performTask: State for recovery 456 for crashed master 99.0 "
              "freed on backup", TestLog::get());
}

TEST_F(BackupMasterRecoveryTest, performTask) {
    mockMetadata(88, true, true);
    mockMetadata(89, true, false);
    recovery->testingSkipBuild = true;
    recovery->start(frames, NULL, NULL);
    recovery->setPartitionsAndSchedule(partitions);
    TestLog::Enable _;
    taskQueue.performTask();
    EXPECT_EQ(
        "schedule: scheduled | "
        "performTask: Starting to build recovery segments for (<99.0,88>) | "
        "buildRecoverySegments: <99.0,88> recovery segments took 0 ms to "
            "construct, notifying other threads | "
        "performTask: Done building recovery segments for (<99.0,88>)",
        TestLog::get());
    TestLog::reset();
    taskQueue.performTask();
    EXPECT_EQ("performTask: Took 0 ms to filter 1 segments", TestLog::get());
}

namespace {
bool buildRecoverySegmentsFilter(string s) {
    return s == "buildRecoverySegments";
}
}

TEST_F(BackupMasterRecoveryTest, buildRecoverySegments) {
    mockMetadata(88, true, true);
    recovery->testingSkipBuild = true;
    recovery->start(frames, NULL, NULL);
    TestLog::Enable _(buildRecoverySegmentsFilter);
    recovery->buildRecoverySegments(recovery->replicas.at(0));
    EXPECT_EQ("buildRecoverySegments: <99.0,88> recovery segments took 0 ms "
              "to construct, notifying other threads", TestLog::get());

    TestLog::reset();
    recovery->buildRecoverySegments(recovery->replicas.at(0));
    EXPECT_EQ("buildRecoverySegments: Recovery segments already built for "
              "<99.0,88>", TestLog::get());
    EXPECT_FALSE(recovery->replicas.at(0).recoveryException);
    EXPECT_TRUE(recovery->replicas.at(0).recoverySegments);
    EXPECT_TRUE(recovery->replicas.at(0).built);
}

TEST_F(BackupMasterRecoveryTest, buildRecoverySegments_buildThrows) {
    mockMetadata(88, true, true);
    recovery->start(frames, NULL, NULL);
    recovery->setPartitionsAndSchedule(partitions);
    TestLog::Enable _(buildRecoverySegmentsFilter);
    taskQueue.performTask();
    EXPECT_TRUE(StringUtil::startsWith(TestLog::get(),
        "buildRecoverySegments: Couldn't build recovery segments for "
        "<99.0,88>: RAMCloud::SegmentIteratorException: cannot iterate: "
        "corrupt segment thrown "));
    EXPECT_TRUE(recovery->replicas.at(0).recoveryException);
    EXPECT_FALSE(recovery->replicas.at(0).recoverySegments);
    EXPECT_TRUE(recovery->replicas.at(0).built);
}

} // namespace RAMCloud
