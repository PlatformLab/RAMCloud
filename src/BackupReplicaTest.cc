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
#include "BackupReplica.h"
#include "InMemoryStorage.h"
#include "Object.h"
#include "SegmentIterator.h"

namespace RAMCloud {

class BackupReplicaTest : public ::testing::Test {
  public:
    typedef BackupStorage::Frame Frame;
    BackupReplicaTest()
        : segmentSize(64 * 1024)
        , storage{segmentSize, 2}
        , info{storage, ServerId(99, 0), 88, segmentSize, true}
    {
        Logger::get().setLogLevels(SILENT_LOG_LEVEL);
    }

    /**
     * Helper that simply creates and appends an object to the given segment.
     */
    void
    appendObjectNoReplication(Segment& segment, const char* data,
                              uint32_t bytes, uint64_t tableId,
                              const char* stringKey, uint16_t stringKeyLength)
    {
        Key key(tableId, stringKey, stringKeyLength);
        Object object(key, data, bytes, 0, 0);
        Buffer buffer;
        object.serializeToBuffer(buffer);
        segment.append(LOG_ENTRY_TYPE_OBJ, buffer);
    }

    /**
     * Helper that simply creates and appends a safeVersion to segment.
     */
    void
    appendSafeVerNoReplication(Segment& segment, uint64_t version)
    {
        ObjectSafeVersion objSafeVer(version);
        Buffer buffer;
        objSafeVer.serializeToBuffer(buffer);
        segment.append(LOG_ENTRY_TYPE_SAFEVERSION, buffer);
    }

    uint32_t segmentSize;
    InMemoryStorage storage;
    BackupReplica info;
};

TEST_F(BackupReplicaTest, destructorLoading) {
    {
        BackupReplica info{storage, ServerId(99, 0), 88, segmentSize, true};
        info.open(false);
        info.close();
        info.startLoading();
    }
}

void
appendTablet(ProtoBuf::Tablets& tablets,
             uint64_t partitionId,
             uint64_t tableId,
             uint64_t start, uint64_t end,
             uint64_t ctimeHeadSegmentId, uint32_t ctimeHeadSegmentOffset)
{
    ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
    tablet.set_table_id(tableId);
    tablet.set_start_key_hash(start);
    tablet.set_end_key_hash(end);
    tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
    tablet.set_user_data(partitionId);
    tablet.set_ctime_log_head_id(ctimeHeadSegmentId);
    tablet.set_ctime_log_head_offset(ctimeHeadSegmentOffset);
}

void
createTabletList(ProtoBuf::Tablets& tablets)
{
    appendTablet(tablets, 0, 123,
        Key::getHash(123, "10", 2), Key::getHash(123, "10", 2), 0, 0);
    appendTablet(tablets, 1, 123,
        Key::getHash(123, "30", 2), Key::getHash(123, "30", 2), 0, 0);

    // tablet created when log head was > (0, 0)
    appendTablet(tablets, 0, 123,
        Key::getHash(123, "XX", 2), Key::getHash(123, "XX", 2), 12741, 57273);
}

TEST_F(BackupReplicaTest, appendRecoverySegment) {
    info.open(false);
    Segment segment;

    SegmentHeader header = { 99, 88, segmentSize, Segment::INVALID_SEGMENT_ID };
    segment.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header));

    appendObjectNoReplication(segment, NULL, 0, 123, "10", 2);

    segment.close();
    Buffer src;
    Segment::Certificate certificate;
    uint32_t appendedBytes = segment.getAppendedLength(certificate);
    segment.appendToBuffer(src, 0, appendedBytes);
    info.append(src, 0, appendedBytes, 0, &certificate);
    info.close();
    info.setRecovering();
    info.startLoading();

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);
    info.buildRecoverySegments(partitions);

    Buffer buffer;
    Status status = info.appendRecoverySegment(0, &buffer, &certificate);
    ASSERT_EQ(STATUS_OK, status);
    EXPECT_EQ(30u, certificate.segmentLength);
    EXPECT_EQ(0xfdc3c812u, certificate.checksum);
    SegmentIterator it(buffer.getRange(0, buffer.getTotalLength()),
                                       buffer.getTotalLength(), certificate);
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
    EXPECT_EQ(28U, it.getLength());

    it.next();
    EXPECT_TRUE(it.isDone());
}

TEST_F(BackupReplicaTest, appendRecoverySegmentSecondarySegment) {
    BackupReplica info{storage, ServerId(99, 0), 88, segmentSize, false};
    info.open(false);
    Segment segment;

    SegmentHeader header = { 99, 88, segmentSize, Segment::INVALID_SEGMENT_ID };
    segment.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header));

    appendObjectNoReplication(segment, NULL, 0, 123, "10", 2);

    segment.close();
    Buffer src;
    Segment::Certificate certificate;
    uint32_t appendedBytes = segment.getAppendedLength(certificate);
    segment.appendToBuffer(src, 0, appendedBytes);
    info.append(src, 0, appendedBytes, 0, &certificate);
    info.close();

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);
    info.setRecovering(partitions);

    Buffer buffer;
    while (true) {
        Status status = info.appendRecoverySegment(0, &buffer, &certificate);
        if (status == STATUS_RETRY) {
            buffer.reset();
            continue;
        }
        ASSERT_EQ(status, STATUS_OK);
        break;
    }
    buffer.reset();
    while (true) {
        Status status = info.appendRecoverySegment(0, &buffer, &certificate);
        if (status == STATUS_RETRY) {
            buffer.reset();
            continue;
        }
        ASSERT_EQ(status, STATUS_OK);
        break;
    }
    EXPECT_EQ(30u, certificate.segmentLength);
    EXPECT_EQ(0xfdc3c812u, certificate.checksum);
    SegmentIterator it(buffer.getRange(0, buffer.getTotalLength()),
                        buffer.getTotalLength(),
                        certificate);
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
    EXPECT_EQ(28U, it.getLength());

    it.next();
    EXPECT_TRUE(it.isDone());
}

TEST_F(BackupReplicaTest, appendRecoverySegmentMalformedSegment) {
    info.open(false);
    Buffer src;
    src.appendTo("garbage", 7);
    info.append(src, 0, 7, 0, NULL);
    info.setRecovering();
    info.startLoading();

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);
    info.buildRecoverySegments(partitions);

    Buffer buffer;
    Segment::Certificate certificate;
    EXPECT_THROW(
        IGNORE_RESULT(info.appendRecoverySegment(0, &buffer, &certificate)),
                 SegmentRecoveryFailedException);
}

TEST_F(BackupReplicaTest, appendRecoverySegmentNotYetRecovered) {
    Buffer buffer;
    TestLog::Enable _;
    Segment::Certificate certificate;
    EXPECT_THROW(
        IGNORE_RESULT(info.appendRecoverySegment(0, &buffer, &certificate)),
                 BackupBadSegmentIdException);
    EXPECT_EQ("appendRecoverySegment: Asked for segment <99.0,88> which isn't "
              "recovering", TestLog::get());
}

TEST_F(BackupReplicaTest, appendRecoverySegmentPartitionOutOfBounds) {
    info.open(false);
    Segment segment;
    segment.close();
    Buffer src;
    Segment::Certificate certificate;
    uint32_t appendedBytes = segment.getAppendedLength(certificate);
    segment.appendToBuffer(src, 0, appendedBytes);
    info.append(src, 0, appendedBytes, 0, &certificate);
    info.close();
    info.setRecovering();
    info.startLoading();

    ProtoBuf::Tablets partitions;
    info.buildRecoverySegments(partitions);

    EXPECT_EQ(0u, info.recoverySegmentsLength);
    Buffer buffer;
    TestLog::Enable _;
    EXPECT_THROW(
        IGNORE_RESULT(info.appendRecoverySegment(0, &buffer, &certificate)),
                 BackupBadSegmentIdException);
    EXPECT_EQ("appendRecoverySegment: Asked for recovery segment 0 from "
              "segment <99.0,88> but there are only 0 partitions",
              TestLog::get());
}

TEST_F(BackupReplicaTest, isEntryAlive) {
    ProtoBuf::Tablets partitions;
    createTabletList(partitions);

    // Tablet's creation time log position was (12741, 57273)
    const ProtoBuf::Tablets::Tablet& tablet(partitions.tablet(2));
    Tub<SegmentHeader> header;

    // Is a cleaner segment...
    header.construct(123, 88, 0, 12742);
    EXPECT_TRUE(isEntryAlive({}, tablet, *header));

    header.construct(123, 88, 0, 12740);
    EXPECT_FALSE(isEntryAlive({}, tablet, *header));

    header.construct(123, 88, 0, 12741);
    EXPECT_FALSE(isEntryAlive({}, tablet, *header));

    // Is not a cleaner segment...
    header.construct(123, 88, 0, uint64_t(Segment::INVALID_SEGMENT_ID));
    EXPECT_TRUE(isEntryAlive({12741, 57273}, tablet, *header));

    header.construct(123, 88, 0, uint64_t(Segment::INVALID_SEGMENT_ID));
    EXPECT_TRUE(isEntryAlive({12741, 57274}, tablet, *header));

    header.construct(123, 88, 0, uint64_t(Segment::INVALID_SEGMENT_ID));
    EXPECT_TRUE(isEntryAlive({12742, 57274}, tablet, *header));

    header.construct(123, 88, 0, uint64_t(Segment::INVALID_SEGMENT_ID));
    EXPECT_FALSE(isEntryAlive({12740, 57273}, tablet, *header));

    header.construct(123, 88, 0, uint64_t(Segment::INVALID_SEGMENT_ID));
    EXPECT_FALSE(isEntryAlive({12741, 57272}, tablet, *header));
}

TEST_F(BackupReplicaTest, whichPartition) {
    ProtoBuf::Tablets partitions;
    createTabletList(partitions);

    info.open(false);
    Segment segment;

    auto r = whichPartition(123, Key::getHash(123, "10", 2), partitions);
    EXPECT_TRUE(r);
    EXPECT_EQ(0u, r->user_data());
    r = whichPartition(123, Key::getHash(123, "30", 2), partitions);
    EXPECT_TRUE(r);
    EXPECT_EQ(1u, r->user_data());
    TestLog::Enable _;
    auto hash = Key::getHash(123, "40", 2);
    r = whichPartition(123, Key::getHash(123, "40", 2), partitions);
    EXPECT_EQ(format("whichPartition: Couldn't place object with "
              "<tableId, keyHash> of <123,%lu> into any "
              "of the given tablets for recovery; hopefully it belonged to "
              "a deleted tablet or lives in another log now", hash),
              TestLog::get());
}

TEST_F(BackupReplicaTest, buildRecoverySegment) {
    info.open(false);
    Segment segment;

    SegmentHeader header = { 99, 88, segmentSize, Segment::INVALID_SEGMENT_ID };
    segment.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header));

    appendSafeVerNoReplication(segment, 234UL);
    appendObjectNoReplication(segment, NULL, 0, 123, "XX", 2);

    segment.close();
    Buffer src;
    Segment::Certificate certificate;
    uint32_t appendedBytes = segment.getAppendedLength(certificate);
    segment.appendToBuffer(src, 0, appendedBytes);
    info.append(src, 0, appendedBytes, 0, &certificate);
    info.close();
    info.setRecovering();
    info.startLoading();

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);

    TestLog::Enable _;
    info.buildRecoverySegments(partitions);
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "buildRecoverySegments: Copying SAFEVERSION to partition 0 (12 B) | "
        "buildRecoverySegments: Copying SAFEVERSION to partition 1 (12 B) |",
        TestLog::get()));

    // Make sure subsequent calls have no effect.
    TestLog::reset();
    info.buildRecoverySegments(partitions);
    EXPECT_EQ("buildRecoverySegments: Recovery segments already built for "
              "<99.0,88>", TestLog::get());

    EXPECT_FALSE(info.recoveryException);
    EXPECT_EQ(2u, info.recoverySegmentsLength);
    ASSERT_TRUE(info.recoverySegments);
    EXPECT_EQ(14U, info.recoverySegments[0].getAppendedLength(certificate));
    EXPECT_EQ(14u, info.recoverySegments[1].getAppendedLength(certificate));
}

TEST_F(BackupReplicaTest, buildRecoverySegmentMalformedSegment) {
    info.open(false);
    Buffer source;
    source.appendTo("garbage", 8);
    info.append(source, 0, 0, 8, NULL);
    info.setRecovering();
    info.startLoading();

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);

    info.buildRecoverySegments(partitions);
    EXPECT_TRUE(info.recoveryException);
    EXPECT_FALSE(info.recoverySegments);
    EXPECT_EQ(0u, info.recoverySegmentsLength);
}

TEST_F(BackupReplicaTest, buildRecoverySegmentNoTablets) {
    info.open(false);
    Segment segment;
    segment.close();
    Buffer src;
    Segment::Certificate certificate;
    uint32_t appendedBytes = segment.getAppendedLength(certificate);
    segment.appendToBuffer(src, 0, appendedBytes);
    info.append(src, 0, appendedBytes, 0, &certificate);
    info.setRecovering();
    info.startLoading();
    info.buildRecoverySegments(ProtoBuf::Tablets());
    EXPECT_FALSE(info.recoveryException);
    EXPECT_EQ(0u, info.recoverySegmentsLength);
    ASSERT_TRUE(info.recoverySegments);
}

TEST_F(BackupReplicaTest, close) {
    info.open(false);
    EXPECT_EQ(BackupReplica::OPEN, info.state);
    const char magic[] = "kitties!";
    uint32_t bytesToCopy = sizeof32(magic);
    Buffer src;
    Buffer::Chunk::appendToBuffer(&src, magic, bytesToCopy);
    Segment::Certificate certificate;
    info.append(src, 0, bytesToCopy, 0, &certificate);

    info.close();

    EXPECT_STREQ(magic, static_cast<const char*>(info.frame->load()));
}

TEST_F(BackupReplicaTest, closeWhileNotOpen) {
    EXPECT_THROW(info.close(), BackupBadSegmentIdException);
}

TEST_F(BackupReplicaTest, free) {
    info.open(false);
    info.close();
    info.frame->load();
    info.free();
    EXPECT_EQ(BackupReplica::FREED, info.state);
}

TEST_F(BackupReplicaTest, freeRecoveringSecondary) {
    BackupReplica info{storage, ServerId(99, 0), 88, segmentSize, false};
    info.open(false);
    info.close();
    info.setRecovering(ProtoBuf::Tablets());
    info.free();
    EXPECT_EQ(BackupReplica::FREED, info.state);
}

TEST_F(BackupReplicaTest, open) {
    info.open(false);
    EXPECT_EQ('\0', static_cast<const char*>(info.frame->load())[0]);
    EXPECT_NE(static_cast<Frame*>(NULL), info.frame);
    EXPECT_EQ(BackupReplica::OPEN, info.state);
}

TEST_F(BackupReplicaTest, openStorageAllocationFailure) {
    InMemoryStorage storage{segmentSize, 0};
    BackupReplica info{storage, ServerId(99, 0), 88, segmentSize, true};
    EXPECT_THROW(info.open(false), BackupStorageException);
    EXPECT_EQ(static_cast<Frame*>(NULL), info.frame);
    EXPECT_EQ(BackupReplica::UNINIT, info.state);
}

TEST_F(BackupReplicaTest, startLoading) {
    info.open(false);
    info.close();
    info.startLoading();
    EXPECT_EQ(BackupReplica::CLOSED, info.state);
}

TEST_F(BackupReplicaTest, append) {
    info.open(false);
    Buffer src;
    const char message[] = "this is a test";
    Buffer::Chunk::appendToBuffer(&src, message, arrayLength(message));
    Segment::Certificate certificate;
    certificate.segmentLength = 0x1234u;
    certificate.checksum = 0xabcdu;
    info.append(src, 10, 4, 1, &certificate);
    EXPECT_EQ(0, memcmp(info.frame->load(), "\0test", 5));
}

} // namespace RAMCloud
