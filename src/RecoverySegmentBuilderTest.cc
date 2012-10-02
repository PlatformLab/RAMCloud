/* Copyright (c) 2012 Stanford University
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
#include "RecoverySegmentBuilder.h"
#include "SegmentIterator.h"
#include "SegmentManager.h"
#include "ServerConfig.h"
#include "StringUtil.h"
#include "TabletsBuilder.h"

namespace RAMCloud {

struct RecoverySegmentBuilderTest : public ::testing::Test {
    Context context;
    ProtoBuf::Tablets partitions;
    ServerId serverId;
    ServerList serverList;
    ServerConfig serverConfig;
    ReplicaManager replicaManager;
    SegletAllocator allocator;
    SegmentManager segmentManager;

    RecoverySegmentBuilderTest()
        : context()
        , partitions()
        , serverId(99, 0)
        , serverList(&context)
        , serverConfig(ServerConfig::forTesting())
        , replicaManager(&context, serverId, 0)
        , allocator(serverConfig)
        , segmentManager(&context, serverConfig, serverId,
                         allocator, replicaManager)
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
        auto oneOneHash = Key::getHash(1, "1", 1);
        TabletsBuilder{partitions}
            (1, 0lu, oneOneHash - 1, TabletsBuilder::NORMAL, 0lu)    // part 0
            (1, oneOneHash, ~0lu, TabletsBuilder::NORMAL, 1lu)       // part 1
            (2, 0lu, ~0lu, TabletsBuilder::NORMAL, 0lu, {}, {1, 0})  // part 0
            (3, 0lu, 1lu, TabletsBuilder::NORMAL, 0lu, {});          // part 0
    }

    DISALLOW_COPY_AND_ASSIGN(RecoverySegmentBuilderTest);
};

TEST_F(RecoverySegmentBuilderTest, build) {
    auto build = RecoverySegmentBuilder::build;
    LogSegment* segment = segmentManager.allocHead();

    uint32_t outOffset = 0;
    { // Object and tombstone should go in partition 1.
        Key key(1, "1", 1);
        Object object(key, "hello", 6, 0, 0);
        Buffer buffer;
        object.serializeToBuffer(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_OBJ,
                                    buffer, 0,
                                    buffer.getTotalLength(),
                                    outOffset));
        ObjectTombstone tombstone(object, 0, 0);
        buffer.reset();
        tombstone.serializeToBuffer(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_OBJTOMB,
                                    buffer, 0,
                                    buffer.getTotalLength(),
                                    outOffset));
    }{ // Object and tombstone should go in partition 0.
        Key key(1, "2", 1);
        Object object(key, "abcde", 6, 0, 0);
        Buffer buffer;
        object.serializeToBuffer(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_OBJ,
                                    buffer, 0,
                                    buffer.getTotalLength(),
                                    outOffset));
        ObjectTombstone tombstone(object, 0, 0);
        buffer.reset();
        tombstone.serializeToBuffer(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_OBJTOMB,
                                    buffer, 0,
                                    buffer.getTotalLength(),
                                    outOffset));
    }{ // Object not in any partition.
        Key key(10, "1", 1);
        Object object(key, "abcde", 6, 0, 0);
        Buffer buffer;
        object.serializeToBuffer(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_OBJ,
                                    buffer, 0,
                                    buffer.getTotalLength(),
                                    outOffset));
    }{ // Object not written before the tablet existed.
        Key key(2, "1", 1);
        Object object(key, "abcde", 6, 0, 0);
        Buffer buffer;
        object.serializeToBuffer(buffer);
        ASSERT_TRUE(segment->append(LOG_ENTRY_TYPE_OBJ,
                                    buffer, 0,
                                    buffer.getTotalLength(),
                                    outOffset));
    }

    Segment::Certificate certificate;
    uint32_t length = segment->getAppendedLength(certificate);
    char buf[allocator.getTotalBytes()];
    ASSERT_TRUE(segment->copyOut(0, buf, length));

    std::unique_ptr<Segment[]> recoverySegments(new Segment[2]);
    TestLog::Enable _;
    build(buf, length, certificate, partitions, recoverySegments.get());
    EXPECT_TRUE(StringUtil::contains(TestLog::get(),
        "Couldn't place object with <tableId, keyHash> of <10"));
    EXPECT_TRUE(StringUtil::contains(TestLog::get(),
        "Skipping object with <tableId, keyHash> of <2"));

    certificate.checksum = 0;
    EXPECT_THROW(
        build(buf, length, certificate, partitions, recoverySegments.get()),
        SegmentIteratorException);
}

TEST_F(RecoverySegmentBuilderTest, extractDigest) {
    auto extractDigest = RecoverySegmentBuilder::extractDigest;
    LogSegment* segment = segmentManager.allocHead();
    Segment::Certificate certificate;
    uint32_t length = segment->getAppendedLength(certificate);
    char buffer[allocator.getTotalBytes()];
    ASSERT_TRUE(segment->copyOut(0, buffer, length));
    Buffer digestBuffer;
    EXPECT_TRUE(extractDigest(buffer, sizeof32(buffer),
                              certificate, &digestBuffer));
    EXPECT_NE(0u, digestBuffer.getTotalLength());

    // Corrupt metadata.
    certificate.checksum = 0;
    EXPECT_FALSE(extractDigest(buffer, sizeof32(buffer),
                              certificate, &digestBuffer));
    // Should have left previously found digest in the buffer.
    EXPECT_NE(0u, digestBuffer.getTotalLength());

    Segment emptySegment;
    length = emptySegment.getAppendedLength(certificate);

    // No digest.
    EXPECT_FALSE(extractDigest(buffer, sizeof32(buffer),
                              certificate, &digestBuffer));
    // Should have left previously found digest in the buffer.
    EXPECT_NE(0u, digestBuffer.getTotalLength());
    digestBuffer.reset();
    EXPECT_FALSE(extractDigest(buffer, sizeof32(buffer),
                              certificate, &digestBuffer));
    EXPECT_EQ(0u, digestBuffer.getTotalLength());
}

TEST_F(RecoverySegmentBuilderTest, isEntryAlive) {
    auto isEntryAlive = RecoverySegmentBuilder::isEntryAlive;
    // Tablet's creation time log position was (12741, 57273)
    ProtoBuf::Tablets::Tablet* tablet(partitions.mutable_tablet(2));
    tablet->set_ctime_log_head_id(12741);
    tablet->set_ctime_log_head_offset(57273);
    Tub<SegmentHeader> header;

    // Is a cleaner segment...
    header.construct(123, 88, 0, 12742);
    EXPECT_TRUE(isEntryAlive({}, tablet, header.get()));

    header.construct(123, 88, 0, 12740);
    EXPECT_FALSE(isEntryAlive({}, tablet, header.get()));

    header.construct(123, 88, 0, 12741);
    EXPECT_FALSE(isEntryAlive({}, tablet, header.get()));

    // Is not a cleaner segment...
    header.construct(123, 88, 0, uint64_t(Segment::INVALID_SEGMENT_ID));
    EXPECT_TRUE(isEntryAlive({12741, 57273}, tablet, header.get()));

    header.construct(123, 88, 0, uint64_t(Segment::INVALID_SEGMENT_ID));
    EXPECT_TRUE(isEntryAlive({12741, 57274}, tablet, header.get()));

    header.construct(123, 88, 0, uint64_t(Segment::INVALID_SEGMENT_ID));
    EXPECT_TRUE(isEntryAlive({12742, 57274}, tablet, header.get()));

    header.construct(123, 88, 0, uint64_t(Segment::INVALID_SEGMENT_ID));
    EXPECT_FALSE(isEntryAlive({12740, 57273}, tablet, header.get()));

    header.construct(123, 88, 0, uint64_t(Segment::INVALID_SEGMENT_ID));
    EXPECT_FALSE(isEntryAlive({12741, 57272}, tablet, header.get()));
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
    auto hash = Key::getHash(3, "1", 1);
    r = whichPartition(3, Key::getHash(3, "1", 1), partitions);
    EXPECT_EQ(format("whichPartition: Couldn't place object with "
              "<tableId, keyHash> of <3,%lu> into any "
              "of the given tablets for recovery; hopefully it belonged to "
              "a deleted tablet or lives in another log now", hash),
              TestLog::get());
}

} // namespace RAMCloud
