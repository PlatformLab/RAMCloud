/* Copyright (c) 2012-2015 Stanford University
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
#include "Object.h"
#include "ObjectManager.h"
#include "PreparedOps.h"
#include "RecoverySegmentBuilder.h"
#include "SegmentIterator.h"
#include "SegmentManager.h"
#include "ServerConfig.h"
#include "StringUtil.h"
#include "TabletsBuilder.h"
#include "MasterTableMetadata.h"

namespace RAMCloud {

struct RecoverySegmentBuilderTest : public ::testing::Test {
    Context context;
    ProtoBuf::RecoveryPartition partitions;
    ServerId serverId;
    ServerList serverList;
    ServerConfig serverConfig;
    ReplicaManager replicaManager;
    MasterTableMetadata masterTableMetadata;
    SegletAllocator allocator;
    SegmentManager segmentManager;

    RecoverySegmentBuilderTest()
        : context()
        , partitions()
        , serverId(99, 0)
        , serverList(&context)
        , serverConfig(ServerConfig::forTesting())
        , replicaManager(&context, &serverId, 0, false, false)
        , masterTableMetadata()
        , allocator(&serverConfig)
        , segmentManager(&context, &serverConfig, &serverId,
                         allocator, replicaManager, &masterTableMetadata)
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
        auto oneOneHash = Key::getHash(1, "1", 1);
        ProtoBuf::Tablets tablets;
        TabletsBuilder{tablets}
            (1, 0lu, oneOneHash - 1, TabletsBuilder::NORMAL, 0lu)    // part 0
            (1, oneOneHash, ~0lu, TabletsBuilder::NORMAL, 1lu)       // part 1
            (2, 0lu, ~0lu, TabletsBuilder::NORMAL, 0lu, {}, {2, 0})  // part 0
            (3, 0lu, 1lu, TabletsBuilder::NORMAL, 0lu, {});          // part 0
        for (int i = 0; i < tablets.tablet_size(); i++) {
            ProtoBuf::Tablets::Tablet& tablet(*partitions.add_tablet());
            tablet = tablets.tablet(i);
        }
    }

    DISALLOW_COPY_AND_ASSIGN(RecoverySegmentBuilderTest);
};

TEST_F(RecoverySegmentBuilderTest, build) {
    auto build = RecoverySegmentBuilder::build;
    LogSegment* segment = segmentManager.allocHeadSegment();

    { // Object and tombstone should go in partition 1.
        Key key(1, "1", 1);

        Buffer dataBuffer;
        Object object(key, "hello", 6, 0, 0, dataBuffer);

        Buffer buffer;
        object.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_OBJ, buffer));
        ObjectTombstone tombstone(object, 0, 0);
        buffer.reset();
        tombstone.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_OBJTOMB, buffer));

    }{ // Object and tombstone should go in partition 0.
        Key key(1, "2", 1);
        Buffer dataBuffer;
        Object object(key, "abcde", 6, 0, 0, dataBuffer);
        Buffer buffer;
        object.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_OBJ, buffer));
        ObjectTombstone tombstone(object, 0, 0);
        buffer.reset();
        tombstone.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_OBJTOMB, buffer));

    }{ // Object not in any partition.
        Key key(10, "1", 1);
        Buffer dataBuffer;
        Object object(key, "abcde", 6, 0, 0, dataBuffer);
        Buffer buffer;
        object.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_OBJ, buffer));

    }{ // Object not written before the tablet existed.
        Key key(2, "1", 1);
        Buffer dataBuffer;
        Object object(key, "abcde", 6, 0, 0, dataBuffer);
        Buffer buffer;
        object.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_OBJ, buffer));
    }{ // RpcResult should go in partition 1.
        Key key(1, "1", 1);
        Buffer dataBuffer;
        RpcResult rpcResult(1, key.getHash(), 6, 4, 2, dataBuffer);
        Buffer buffer;
        rpcResult.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_RPCRESULT, buffer));
    }{ // RpcResult should go in partition 0.
        Key key(1, "2", 1);
        Buffer dataBuffer;
        RpcResult rpcResult(1, key.getHash(), 5, 3, 1, dataBuffer);
        Buffer buffer;
        rpcResult.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_RPCRESULT, buffer));
    }{ // RpcResult not in any partition.
        Key key(10, "1", 1);
        Buffer dataBuffer;
        RpcResult rpcResult(10, key.getHash(), 10, 5, 2, dataBuffer);
        Buffer buffer;
        rpcResult.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_RPCRESULT, buffer));
    }{ // RpcResult not written before the tablet existed.
        Key key(2, "1", 1);
        Buffer dataBuffer;
        RpcResult rpcResult(2, key.getHash(), 2, 1, 0, dataBuffer);
        Buffer buffer;
        rpcResult.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_RPCRESULT, buffer));
    }{ // PreparedOp and PreparedOpTombstone should go in partition 1.
        Key key(1, "1", 1);
        Buffer dataBuffer;
        PreparedOp op(WireFormat::TxPrepare::WRITE,
                      1UL, 10UL, 10UL,
                      key, "hello", 6, 0, 0, dataBuffer);

        Buffer buffer;
        op.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_PREP, buffer));

        PreparedOpTombstone opTomb(op, 0);
        buffer.reset();
        opTomb.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_PREPTOMB, buffer));
    }{ // PreparedOp and PreparedOpTombstone should go in partition 0.
        Key key(1, "2", 1);
        Buffer dataBuffer;
        PreparedOp op(WireFormat::TxPrepare::READ,
                      1UL, 10UL, 10UL,
                      key, "hello", 6, 0, 0, dataBuffer);

        Buffer buffer;
        op.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_PREP, buffer));

        PreparedOpTombstone opTomb(op, 0);
        buffer.reset();
        opTomb.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_PREPTOMB, buffer));
    }{ // PreparedOp and PreparedOpTombstone not in any partition.
        Key key(10, "1", 1);
        Buffer dataBuffer;
        PreparedOp op(WireFormat::TxPrepare::WRITE,
                      1UL, 10UL, 10UL,
                      key, "hello", 6, 0, 0, dataBuffer);

        Buffer buffer;
        op.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_PREP, buffer));

        PreparedOpTombstone opTomb(op, 0);
        buffer.reset();
        opTomb.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_PREPTOMB, buffer));
    }{ // PreparedOp and PreparedOpTombstone not written
       // before the tablet existed.
        Key key(2, "1", 1);
        Buffer dataBuffer;
        PreparedOp op(WireFormat::TxPrepare::WRITE,
                      1UL, 10UL, 10UL,
                      key, "hello", 6, 0, 0, dataBuffer);

        Buffer buffer;
        op.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_PREP, buffer));

        PreparedOpTombstone opTomb(op, 0);
        buffer.reset();
        opTomb.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_PREPTOMB, buffer));
    }{ // TxDecisionRecord should go in partition 1.
        Key key(1, "1", 1);
        TxDecisionRecord decisionRecord(1, key.getHash(), 6, 1,
                WireFormat::TxDecision::ABORT, 100);
        Buffer buffer;
        decisionRecord.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_TXDECISION, buffer));
    }{ // TxDecisionRecord should go in partition 0.
        Key key(1, "2", 1);
        TxDecisionRecord decisionRecord(1, key.getHash(), 5, 1,
                WireFormat::TxDecision::ABORT, 100);
        Buffer buffer;
        decisionRecord.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_TXDECISION, buffer));
    }{ // TxDecisionRecord not in any partition.
        Key key(10, "1", 1);
        TxDecisionRecord decisionRecord(10, key.getHash(), 10, 1,
                WireFormat::TxDecision::ABORT, 100);
        Buffer buffer;
        decisionRecord.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_TXDECISION, buffer));
    }{ // TxDecisionRecord not written before the tablet existed.
        Key key(2, "1", 1);
        TxDecisionRecord decisionRecord(2, key.getHash(), 2, 1,
                WireFormat::TxDecision::ABORT, 100);
        Buffer buffer;
        decisionRecord.assembleForLog(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_TXDECISION, buffer));
    }

    SegmentCertificate certificate;
    uint32_t length = segment->getAppendedLength(&certificate);
    char buf[serverConfig.segmentSize];
    ASSERT_TRUE(segment->copyOut(0, buf, length));

    std::unique_ptr<Segment[]> recoverySegments(new Segment[2]);
    TestLog::Enable _;
    build(buf, length, certificate, 2, partitions, recoverySegments.get());
    EXPECT_TRUE(StringUtil::contains(TestLog::get(),
        "Couldn't place object"));
    EXPECT_TRUE(StringUtil::contains(TestLog::get(),
        "Skipping object with <tableId, keyHash> of <2"));
    EXPECT_EQ("safeVersion at offset 0, length 12 with version 1 | "
            "object at offset 14, length 34 with tableId 1, key '2' | "
            "tombstone at offset 50, length 33 with tableId 1, key '2' | "
            "rpcResult at offset 85, length 44 with tableId 1, "
                    "keyHash 0x3554F985FBED3C16, leaseId 5, rpcId 3 | "
            "preparedOp at offset 131, length 66 with tableId 1, key '2', "
                    "leaseId 1, rpcId 10 | "
            "preparedOpTombstone at offset 199, length 44 with tableId 1, "
                    "keyHash 0x3554F985FBED3C16, leaseId 1, rpcId 10 | "
            "txDecision at offset 245, length 48 with tableId 1, "
                    "keyHash 0x3554F985FBED3C16, leaseId 5",
            ObjectManager::dumpSegment(&recoverySegments[0]));
    EXPECT_EQ("safeVersion at offset 0, length 12 with version 1 | "
            "object at offset 14, length 34 with tableId 1, key '1' | "
            "tombstone at offset 50, length 33 with tableId 1, key '1' | "
            "rpcResult at offset 85, length 44 with tableId 1, "
                    "keyHash 0xDD5D9F7F60D5B056, leaseId 6, rpcId 4 | "
            "preparedOp at offset 131, length 66 with tableId 1, key '1', "
                    "leaseId 1, rpcId 10 | "
            "preparedOpTombstone at offset 199, length 44 with tableId 1, "
                    "keyHash 0xDD5D9F7F60D5B056, leaseId 1, rpcId 10 | "
            "txDecision at offset 245, length 48 with tableId 1, "
                    "keyHash 0xDD5D9F7F60D5B056, leaseId 6",
            ObjectManager::dumpSegment(&recoverySegments[1]));

    certificate.checksum = 0;
    EXPECT_THROW(
        build(buf, length, certificate, 2, partitions, recoverySegments.get()),
        SegmentIteratorException);
}

TEST_F(RecoverySegmentBuilderTest, build_safeVersionEntries) {
    auto build = RecoverySegmentBuilder::build;

    // Create one replica containing a safeVersion record.
    LogSegment* segment = segmentManager.allocHeadSegment();
    ObjectSafeVersion safeVersion(99);
    Buffer buffer;
    safeVersion.assembleForLog(buffer);

    ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_SAFEVERSION, buffer));
    SegmentCertificate certificate;
    uint32_t length = segment->getAppendedLength(&certificate);
    char buf[serverConfig.segmentSize];
    ASSERT_TRUE(segment->copyOut(0, buf, length));

    std::unique_ptr<Segment[]> recoverySegments(new Segment[3]);
    build(buf, length, certificate, 3, partitions, recoverySegments.get());

    EXPECT_EQ("safeVersion at offset 0, length 12 with version 1 | "
            "safeVersion at offset 14, length 12 with version 99",
            ObjectManager::dumpSegment(&recoverySegments[0]));
    EXPECT_EQ("safeVersion at offset 0, length 12 with version 1 | "
            "safeVersion at offset 14, length 12 with version 99",
            ObjectManager::dumpSegment(&recoverySegments[1]));
    EXPECT_EQ("safeVersion at offset 0, length 12 with version 1 | "
            "safeVersion at offset 14, length 12 with version 99",
            ObjectManager::dumpSegment(&recoverySegments[2]));
}

TEST_F(RecoverySegmentBuilderTest, build_participantList) {
    auto build = RecoverySegmentBuilder::build;

    // Create one replica containing a participantList record.
    LogSegment* segment = segmentManager.allocHeadSegment();
    WireFormat::TxParticipant participants[3];
    participants[0] = WireFormat::TxParticipant(1, 2, 10);
    participants[1] = WireFormat::TxParticipant(123, 234, 11);
    participants[2] = WireFormat::TxParticipant(111, 222, 12);
    ParticipantList record(participants, 3, 42, 9);
    record.getTransactionId();
    Buffer buffer;
    record.assembleForLog(buffer);

    ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_TXPLIST, buffer));
    SegmentCertificate certificate;
    uint32_t length = segment->getAppendedLength(&certificate);
    char buf[serverConfig.segmentSize];
    ASSERT_TRUE(segment->copyOut(0, buf, length));

    std::unique_ptr<Segment[]> recoverySegments(new Segment[3]);
    build(buf, length, certificate, 3, partitions, recoverySegments.get());

    EXPECT_EQ("safeVersion at offset 0, length 12 with version 1 | "
            "participantList at offset 14, length 96 with "
            "TxId: (leaseId 42, rpcId 9) containing 3 entries",
            ObjectManager::dumpSegment(&recoverySegments[0]));
    EXPECT_EQ("safeVersion at offset 0, length 12 with version 1 | "
            "participantList at offset 14, length 96 with "
            "TxId: (leaseId 42, rpcId 9) containing 3 entries",
            ObjectManager::dumpSegment(&recoverySegments[1]));
    EXPECT_EQ("safeVersion at offset 0, length 12 with version 1 | "
            "participantList at offset 14, length 96 with "
            "TxId: (leaseId 42, rpcId 9) containing 3 entries",
            ObjectManager::dumpSegment(&recoverySegments[2]));
}

TEST_F(RecoverySegmentBuilderTest, extractDigest) {
    auto extractDigest = RecoverySegmentBuilder::extractDigest;
    LogSegment* segment = segmentManager.allocHeadSegment();
    SegmentCertificate certificate;
    uint32_t length = segment->getAppendedLength(&certificate);
    char buffer[serverConfig.segmentSize];
    ASSERT_TRUE(segment->copyOut(0, buffer, length));
    Buffer digestBuffer;
    Buffer tableStatsBuffer;
    EXPECT_TRUE(extractDigest(buffer, sizeof32(buffer),
                              certificate, &digestBuffer, &tableStatsBuffer));
    EXPECT_NE(0u, digestBuffer.size());

    // Corrupt metadata.
    certificate.checksum = 0;
    EXPECT_FALSE(extractDigest(buffer, sizeof32(buffer),
                              certificate, &digestBuffer, &tableStatsBuffer));
    // Should have left previously found digest in the buffer.
    EXPECT_NE(0u, digestBuffer.size());

    Segment emptySegment;
    length = emptySegment.getAppendedLength(&certificate);

    // No digest.
    EXPECT_FALSE(extractDigest(buffer, sizeof32(buffer),
                              certificate, &digestBuffer, &tableStatsBuffer));
    // Should have left previously found digest in the buffer.
    EXPECT_NE(0u, digestBuffer.size());
    digestBuffer.reset();
    EXPECT_FALSE(extractDigest(buffer, sizeof32(buffer),
                              certificate, &digestBuffer, &tableStatsBuffer));
    EXPECT_EQ(0u, digestBuffer.size());
}

TEST_F(RecoverySegmentBuilderTest, isEntryAlive) {
    auto isEntryAlive = RecoverySegmentBuilder::isEntryAlive;
    // Tablet's creation time log position was (12741, 57273)
    ProtoBuf::Tablets::Tablet* tablet(partitions.mutable_tablet(2));
    tablet->set_ctime_log_head_id(12741);
    tablet->set_ctime_log_head_offset(57273);
    Tub<SegmentHeader> header;

    header.construct(123, 88, 0);
    EXPECT_TRUE(isEntryAlive({12741, 57273}, tablet));

    header.construct(123, 88, 0);
    EXPECT_TRUE(isEntryAlive({12741, 57274}, tablet));

    header.construct(123, 88, 0);
    EXPECT_TRUE(isEntryAlive({12742, 57274}, tablet));

    header.construct(123, 88, 0);
    EXPECT_FALSE(isEntryAlive({12740, 57273}, tablet));

    header.construct(123, 88, 0);
    EXPECT_FALSE(isEntryAlive({12741, 57272}, tablet));
}

TEST_F(RecoverySegmentBuilderTest, whichPartition) {
    auto whichPartition = RecoverySegmentBuilder::whichPartition;
    auto r = whichPartition(1, Key::getHash(1, "1", 1), partitions);
    EXPECT_TRUE(r);
    EXPECT_EQ(1u, r->user_data());
    r = whichPartition(1, Key::getHash(1, "2", 1), partitions);
    EXPECT_TRUE(r);
    r = whichPartition(2, Key::getHash(2, "1", 1), partitions);
    EXPECT_TRUE(r);
    EXPECT_EQ(0u, r->user_data());
    TestLog::Enable _;
    r = whichPartition(3, Key::getHash(3, "1", 1), partitions);
    EXPECT_FALSE(r);
}

} // namespace RAMCloud
