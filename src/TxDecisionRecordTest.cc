/* Copyright (c) 2015 Stanford University
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

#include "TxDecisionRecord.h"

namespace RAMCloud {

/**
 * Unit tests for Object.
 */
class TxDecisionRecordTest : public ::testing::Test {
  public:
    TxDecisionRecordTest()
        : recordBuffer()
        , recordFromScratch()
        , recordFromBuffer()
    {
        recordFromScratch.construct(
                32, 42, 21, 2, WireFormat::TxDecision::ABORT, 100);
        recordFromScratch->addParticipant(1, 2, 3);
        recordFromScratch->addParticipant(123, 234, 345);

        recordFromScratch->assembleForLog(recordBuffer);
        recordFromBuffer.construct(recordBuffer);

        records[0] = &*recordFromScratch;
        records[1] = &*recordFromBuffer;
    }

    ~TxDecisionRecordTest()
    {
    }

    Buffer recordBuffer;
    Tub<TxDecisionRecord> recordFromScratch;
    Tub<TxDecisionRecord> recordFromBuffer;

    TxDecisionRecord* records[2];

    DISALLOW_COPY_AND_ASSIGN(TxDecisionRecordTest);
};

TEST_F(TxDecisionRecordTest, constructor_fromScratch) {
    TxDecisionRecord record(32, 42, 21, 1, WireFormat::TxDecision::ABORT, 100);
    EXPECT_EQ(32U, record.header.tableId);
    EXPECT_EQ(42U, record.header.keyHash);
    EXPECT_EQ(21U, record.header.leaseId);
    EXPECT_EQ(WireFormat::TxDecision::ABORT, record.header.decision);
    EXPECT_EQ(0U, record.header.participantCount);
    EXPECT_EQ(100U, record.header.timestamp);
    EXPECT_TRUE(record.participantBuffer != NULL);
    EXPECT_TRUE(record.defaultParticipantBuffer);
    EXPECT_EQ(0U, record.defaultParticipantBuffer->size());
    EXPECT_EQ(record.defaultParticipantBuffer.get(), record.participantBuffer);
}

TEST_F(TxDecisionRecordTest, constuctor_fromBuffer) {
    TxDecisionRecord record(recordBuffer);
    EXPECT_EQ(32U, record.header.tableId);
    EXPECT_EQ(42U, record.header.keyHash);
    EXPECT_EQ(21U, record.header.leaseId);
    EXPECT_EQ(WireFormat::TxDecision::ABORT, record.header.decision);
    EXPECT_EQ(2U, record.header.participantCount);
    EXPECT_EQ(100U, record.header.timestamp);
    EXPECT_TRUE(record.participantBuffer != NULL);
    EXPECT_FALSE(record.defaultParticipantBuffer);
    EXPECT_EQ(&recordBuffer, record.participantBuffer);
}

TEST_F(TxDecisionRecordTest, addParticipant) {
    WireFormat::TxParticipant entry;
    TxDecisionRecord record(32, 42, 21, 2, WireFormat::TxDecision::ABORT, 100);

    record.addParticipant(1, 2, 3);
    EXPECT_EQ(1U, record.header.participantCount);
    entry = record.getParticipant(0);
    EXPECT_EQ(1U, entry.tableId);
    EXPECT_EQ(2U, entry.keyHash);
    EXPECT_EQ(3U, entry.rpcId);

    record.addParticipant(123, 234, 345);
    EXPECT_EQ(2U, record.header.participantCount);
    entry = record.getParticipant(1);
    EXPECT_EQ(123U, entry.tableId);
    EXPECT_EQ(234U, entry.keyHash);
    EXPECT_EQ(345U, entry.rpcId);
}

TEST_F(TxDecisionRecordTest, assembleForLog) {
    for (uint32_t i = 0; i < arrayLength(records); i++) {
        Buffer buffer;
        TxDecisionRecord* record = records[i];

        record->assembleForLog(buffer);
        TxDecisionRecord::Header* header =
                buffer.getOffset<TxDecisionRecord::Header>(0);
        EXPECT_EQ(32U, header->tableId);
        EXPECT_EQ(42U, header->keyHash);
        EXPECT_EQ(21U, header->leaseId);
        EXPECT_EQ(2U, header->transactionId);
        EXPECT_EQ(WireFormat::TxDecision::ABORT, header->decision);
        EXPECT_EQ(2U, header->participantCount);
        EXPECT_EQ(100U, header->timestamp);

        WireFormat::TxParticipant* entry;
        entry = buffer.getOffset<WireFormat::TxParticipant>(sizeof32(*header));
        EXPECT_EQ(1U, entry->tableId);
        EXPECT_EQ(2U, entry->keyHash);
        EXPECT_EQ(3U, entry->rpcId);

        entry = buffer.getOffset<WireFormat::TxParticipant>(
                sizeof32(*header) + sizeof32(*entry));
        EXPECT_EQ(123U, entry->tableId);
        EXPECT_EQ(234U, entry->keyHash);
        EXPECT_EQ(345U, entry->rpcId);
    }
}

TEST_F(TxDecisionRecordTest, getDecision) {
    for (uint32_t i = 0; i < arrayLength(records); i++) {
        TxDecisionRecord& record = *records[i];
        EXPECT_EQ(WireFormat::TxDecision::ABORT, record.getDecision());
    }
}

TEST_F(TxDecisionRecordTest, getKeyHash) {
    for (uint32_t i = 0; i < arrayLength(records); i++) {
        TxDecisionRecord& record = *records[i];
        EXPECT_EQ(42U, record.getKeyHash());
    }
}

TEST_F(TxDecisionRecordTest, getLeaseId) {
    for (uint32_t i = 0; i < arrayLength(records); i++) {
        TxDecisionRecord& record = *records[i];
        EXPECT_EQ(21U, record.getLeaseId());
    }
}

TEST_F(TxDecisionRecordTest, getTransactionId) {
    for (uint32_t i = 0; i < arrayLength(records); i++) {
        TxDecisionRecord& record = *records[i];
        EXPECT_EQ(2U, record.getTransactionId());
    }
}

TEST_F(TxDecisionRecordTest, getParticipant) {
    for (uint32_t i = 0; i < arrayLength(records); i++) {
        WireFormat::TxParticipant entry;
        TxDecisionRecord& record = *records[i];

        entry = record.getParticipant(0);
        EXPECT_EQ(1U, entry.tableId);
        EXPECT_EQ(2U, entry.keyHash);
        EXPECT_EQ(3U, entry.rpcId);

        entry = record.getParticipant(1);
        EXPECT_EQ(123U, entry.tableId);
        EXPECT_EQ(234U, entry.keyHash);
        EXPECT_EQ(345U, entry.rpcId);
    }
}

TEST_F(TxDecisionRecordTest, getParticipantCount) {
    for (uint32_t i = 0; i < arrayLength(records); i++) {
        TxDecisionRecord& record = *records[i];
        EXPECT_EQ(2U, record.getParticipantCount());
    }
}

TEST_F(TxDecisionRecordTest, getTableId) {
    for (uint32_t i = 0; i < arrayLength(records); i++) {
        TxDecisionRecord& record = *records[i];
        EXPECT_EQ(32U, record.getTableId());
    }
}

TEST_F(TxDecisionRecordTest, getTimestamp) {
    for (uint32_t i = 0; i < arrayLength(records); i++) {
        TxDecisionRecord& record = *records[i];
        EXPECT_EQ(100U, record.getTimestamp());
    }
}

TEST_F(TxDecisionRecordTest, checkIntegrity) {
    for (uint32_t i = 0; i < arrayLength(records); i++) {
        TxDecisionRecord& record = *records[i];

        Buffer buffer;
        record.assembleForLog(buffer);
        EXPECT_TRUE(record.checkIntegrity());

        uint8_t* evil = reinterpret_cast<uint8_t*>(
            const_cast<void*>(buffer.getRange(0, 1)));
        uint8_t tmp = *evil;
        *evil = static_cast<uint8_t>(~*evil);
        EXPECT_FALSE(record.checkIntegrity());
        *evil = tmp;

        EXPECT_TRUE(record.checkIntegrity());
        evil = reinterpret_cast<uint8_t*>(
            const_cast<void*>(buffer.getRange(buffer.size() - 1, 1)));
        tmp = *evil;
        *evil = static_cast<uint8_t>(~*evil);
        EXPECT_FALSE(record.checkIntegrity());
        *evil = tmp;

        EXPECT_TRUE(record.checkIntegrity());
    }
}

} // namespace RAMCloud
