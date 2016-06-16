/* Copyright (c) 2014-2016 Stanford University
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

#include "TestUtil.h"       //Has to be first, compiler complains

#include "ParticipantList.h"

namespace RAMCloud {

/**
 * Unit tests for ParticipantList.
 */
class ParticipantListTest : public ::testing::Test {
  public:
    ParticipantListTest()
        : buffer()
        , plistFromScratch()
        , plistFromBuffer()
        , clientLeaseId(42)
        , clientTxId(9)
    {
        // construct participant list.
        participants[0] = WireFormat::TxParticipant(1, 2, 10);
        participants[1] = WireFormat::TxParticipant(123, 234, 11);
        participants[2] = WireFormat::TxParticipant(111, 222, 12);

        plistFromScratch.construct(participants, 3, clientLeaseId, clientTxId);

        // Add some junk to the front.
        Buffer temp;
        temp.appendCopy("FOOBAR", 6);
        plistFromScratch->assembleForLog(temp);
        buffer.appendCopy(temp.getRange(0, temp.size()), temp.size());
        plistFromBuffer.construct(buffer, 6);

        plists[0] = &*plistFromScratch;
        plists[1] = &*plistFromBuffer;
    }

    ~ParticipantListTest()
    {
    }

    WireFormat::TxParticipant participants[3];
    Buffer buffer;
    Tub<ParticipantList> plistFromScratch;
    Tub<ParticipantList> plistFromBuffer;
    uint64_t clientLeaseId;
    uint64_t clientTxId;

    ParticipantList* plists[2];

    DISALLOW_COPY_AND_ASSIGN(ParticipantListTest);
};

TEST_F(ParticipantListTest, constructor_fromRpc) {
    ParticipantList localPList(participants, 3, clientLeaseId, clientTxId);
    EXPECT_EQ(3U, localPList.header.participantCount);
    EXPECT_EQ(clientLeaseId, localPList.header.clientLeaseId);
    EXPECT_TRUE(localPList.participants == participants);
    EXPECT_EQ(10U, localPList.participants[0].rpcId);
    EXPECT_EQ(11U, localPList.participants[1].rpcId);
    EXPECT_EQ(12U, localPList.participants[2].rpcId);
}

TEST_F(ParticipantListTest, constructor_fromBuffer) {
    ParticipantList localPList(buffer, 6);
    EXPECT_EQ(3U, localPList.header.participantCount);
    EXPECT_EQ(clientLeaseId, localPList.header.clientLeaseId);
    EXPECT_EQ(10U, localPList.participants[0].rpcId);
    EXPECT_EQ(11U, localPList.participants[1].rpcId);
    EXPECT_EQ(12U, localPList.participants[2].rpcId);
}

TEST_F(ParticipantListTest, assembleForLog) {
    for (uint32_t i = 0; i < arrayLength(plists); i++) {
        ParticipantList& plist = *plists[i];

        Buffer buffer;
        plist.assembleForLog(buffer);

        ParticipantList::Header* header =
                buffer.getStart<ParticipantList::Header>();
        EXPECT_EQ(3U, header->participantCount);
        EXPECT_EQ(clientLeaseId, header->clientLeaseId);

        WireFormat::TxParticipant* entry;
        entry = buffer.getOffset<WireFormat::TxParticipant>(sizeof32(*header));
        EXPECT_EQ(1U, entry->tableId);
        EXPECT_EQ(2U, entry->keyHash);
        EXPECT_EQ(10U, entry->rpcId);

        entry = buffer.getOffset<WireFormat::TxParticipant>(
        sizeof32(*header) + sizeof32(*entry));
        EXPECT_EQ(123U, entry->tableId);
        EXPECT_EQ(234U, entry->keyHash);
        EXPECT_EQ(11U, entry->rpcId);

        entry = buffer.getOffset<WireFormat::TxParticipant>(
        sizeof32(*header) + sizeof32(*entry) + sizeof32(*entry));
        EXPECT_EQ(111U, entry->tableId);
        EXPECT_EQ(222U, entry->keyHash);
        EXPECT_EQ(12U, entry->rpcId);
    }
}

TEST_F(ParticipantListTest, checkIntegrity) {
    for (uint32_t i = 0; i < arrayLength(plists); i++) {
        ParticipantList& plist = *plists[i];

        Buffer buffer;
        plist.assembleForLog(buffer);
        EXPECT_TRUE(plist.checkIntegrity());

        uint8_t* evil = reinterpret_cast<uint8_t*>(
            const_cast<void*>(buffer.getRange(0, 1)));
        uint8_t tmp = *evil;
        *evil = static_cast<uint8_t>(~*evil);
        EXPECT_FALSE(plist.checkIntegrity());
        *evil = tmp;

        EXPECT_TRUE(plist.checkIntegrity());
        evil = reinterpret_cast<uint8_t*>(
            const_cast<void*>(buffer.getRange(buffer.size() - 1, 1)));
        tmp = *evil;
        *evil = static_cast<uint8_t>(~*evil);
        EXPECT_FALSE(plist.checkIntegrity());
        *evil = tmp;

        EXPECT_TRUE(plist.checkIntegrity());
    }
}

TEST_F(ParticipantListTest, getTransactionId) {
    for (uint32_t i = 0; i < arrayLength(plists); i++) {
        ParticipantList& plist = *plists[i];

        TransactionId txId = plist.getTransactionId();
        EXPECT_EQ(clientLeaseId, txId.clientLeaseId);
        EXPECT_EQ(clientTxId, txId.clientTransactionId);
    }
}

} // namespace RAMCloud
