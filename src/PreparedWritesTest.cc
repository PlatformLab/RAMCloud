/* Copyright (c) 2014 Stanford University
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

#include "PreparedWrites.h"
#include "Context.h"
#include "RamCloud.h"

namespace RAMCloud {

using WireFormat::TxParticipant;

/**
 * Unit tests for PreparedOp.
 */
class PreparedOpTest : public ::testing::Test {
  public:
    PreparedOpTest()
        : stringKey()
        , dataBlob()
        , keyHash()
        , buffer()
        , buffer2()
        , preparedOpFromRpc()
        , preparedOpFromBuffer()
    {
        snprintf(stringKey, sizeof(stringKey), "key!");
        snprintf(dataBlob, sizeof(dataBlob), "YO!");
        Key key(572, stringKey, 5);

        // write some garbage into the buffer so that the starting
        // offset of keysAndValue in keysAndValueBuffer is != 0. In this
        // case, it will be sizeof(stringKey)
        buffer.appendExternal(&stringKey, sizeof(stringKey));

        // this is the starting of keysAndValue in the keysAndValueBuffer
        buffer.emplaceAppend<KeyCount>((unsigned char)1);

        // lengths of all they keys are 3.
        // store cumulativeKeyLengths in the object
        buffer.emplaceAppend<CumulativeKeyLength>((uint16_t) 5);

        // append keys here.
        buffer.appendExternal(stringKey, sizeof(stringKey));

        // append data
        buffer.appendExternal(dataBlob, 4);

        // construct participant list.
        keyHash = key.getHash();
        participants[0] = WireFormat::TxParticipant(572U, key.getHash(), 10U);
        participants[1] = WireFormat::TxParticipant(573U, key.getHash(), 11U);
        participants[2] = WireFormat::TxParticipant(574U, key.getHash(), 12U);

        preparedOpFromRpc.construct(WireFormat::TxPrepare::WRITE,
                                    1UL,
                                    10UL,
                                    3U,
                                    participants,
                                    key.getTableId(),
                                    75,
                                    723,
                                    buffer,
                                    sizeof32(stringKey));

        preparedOpFromRpc->assembleForLog(buffer2);

        // prepend some garbage to buffer2 so that we can test the constructor
        // with a non-zero offset
        memcpy(buffer2.allocPrepend(sizeof(stringKey)), &stringKey,
                sizeof(stringKey));

        preparedOpFromBuffer.construct(buffer2, sizeof32(stringKey),
                                   buffer2.size() - sizeof32(stringKey));

        records[0] = &*preparedOpFromRpc;
        records[1] = &*preparedOpFromBuffer;
    }

    ~PreparedOpTest()
    {
    }

    // Don't use static strings, since they'll be loaded into read-only
    // memory and we can't mutate to test checksumming.
    char stringKey[5];
    char dataBlob[4];

    uint64_t keyHash;
    WireFormat::TxParticipant participants[3];

    Buffer buffer;
    Buffer buffer2;

    Tub<PreparedOp> preparedOpFromRpc;
    Tub<PreparedOp> preparedOpFromBuffer;
    //TODO(seojin): preparedRead, preparedRemove?


    PreparedOp* records[2];

    DISALLOW_COPY_AND_ASSIGN(PreparedOpTest);
};

TEST_F(PreparedOpTest, constructor_fromRpc) {
    PreparedOp& record = *records[0];

    EXPECT_EQ(WireFormat::TxPrepare::WRITE, record.header.type);
    EXPECT_EQ(1UL, record.header.clientId);
    EXPECT_EQ(10UL, record.header.rpcId);
    EXPECT_EQ(3U, record.header.participantCount);

    EXPECT_EQ(WireFormat::TxParticipant(572U, keyHash, 10U),
              record.participants[0]);
    EXPECT_EQ(WireFormat::TxParticipant(573U, keyHash, 11U),
              record.participants[1]);
    EXPECT_EQ(WireFormat::TxParticipant(574U, keyHash, 12U),
              record.participants[2]);

    EXPECT_EQ(572U, record.object.header.tableId);
    EXPECT_EQ(75U, record.object.header.version);
    EXPECT_EQ(723U, record.object.header.timestamp);
    //EXPECT_EQ(0xBB68333C, record.object.header.checksum);

    const uint8_t *keysAndValue = reinterpret_cast<const uint8_t *>(
                            record.object.getKeysAndValue());

    KeyCount numKeys = *keysAndValue;
    EXPECT_EQ(1U, numKeys);

    // skip ahead past numKeys
    keysAndValue = keysAndValue + sizeof(KeyCount);

    const CumulativeKeyLength cumulativeKeyLength = *(reinterpret_cast<
                                                const CumulativeKeyLength *>(
                                                keysAndValue));

    EXPECT_EQ(5, cumulativeKeyLength);

    // the keys are NULL terminated anyway
    EXPECT_EQ("key!", string(reinterpret_cast<const char*>(
                    keysAndValue + sizeof(CumulativeKeyLength))));

    EXPECT_EQ(12U, record.object.keysAndValueLength);
    EXPECT_FALSE(record.object.keysAndValue);
    EXPECT_TRUE(record.object.keysAndValueBuffer);

    // offset into what getKeysAndValue() returns, to point to the actual data
    // blob. Skip the cumulative key length values and the key values
    // numKeys = 3, total length of all 3 keys is 9
    EXPECT_EQ("YO!", string(reinterpret_cast<const char*>(
                    keysAndValue + sizeof(CumulativeKeyLength) + 5)));
}

TEST_F(PreparedOpTest, constructor_fromBuffer) {
    PreparedOp& record = *records[1];

    EXPECT_EQ(WireFormat::TxPrepare::WRITE, record.header.type);
    EXPECT_EQ(1UL, record.header.clientId);
    EXPECT_EQ(10UL, record.header.rpcId);
    EXPECT_EQ(3U, record.header.participantCount);
    EXPECT_EQ(WireFormat::TxParticipant(572U, keyHash, 10U),
              record.participants[0]);
    EXPECT_EQ(WireFormat::TxParticipant(573U, keyHash, 11U),
              record.participants[1]);
    EXPECT_EQ(WireFormat::TxParticipant(574U, keyHash, 12U),
              record.participants[2]);

    EXPECT_EQ(572U, record.object.header.tableId);
    EXPECT_EQ(75U, record.object.header.version);
    EXPECT_EQ(723U, record.object.header.timestamp);
    //EXPECT_EQ(0xBB68333C, record.object.header.checksum);

    const uint8_t *keysAndValue = reinterpret_cast<const uint8_t *>(
                            record.object.getKeysAndValue());

    KeyCount numKeys = *keysAndValue;
    EXPECT_EQ(1U, numKeys);

    // skip ahead past numKeys
    keysAndValue = keysAndValue + sizeof(KeyCount);

    const CumulativeKeyLength cumulativeKeyLength = *(reinterpret_cast<
                                                const CumulativeKeyLength *>(
                                                keysAndValue));

    EXPECT_EQ(5, cumulativeKeyLength);

    // the keys are NULL terminated anyway
    EXPECT_EQ("key!", string(reinterpret_cast<const char*>(
                    keysAndValue + sizeof(CumulativeKeyLength))));

    EXPECT_EQ(12U, record.object.keysAndValueLength);
    EXPECT_TRUE(record.object.keysAndValueBuffer);

    // offset into what getKeysAndValue() returns, to point to the actual data
    // blob. Skip the cumulative key length values and the key values
    // numKeys = 3, total length of all 3 keys is 9
    EXPECT_EQ("YO!", string(reinterpret_cast<const char*>(
                    keysAndValue + sizeof(CumulativeKeyLength) + 5)));
}

TEST_F(PreparedOpTest, assembleForLog) {
    for (uint32_t i = 0; i < arrayLength(records); i++) {
        Buffer buffer;
        PreparedOp& record = *(records[i]);
        record.assembleForLog(buffer);
        const PreparedOp::Header* header =
                buffer.getStart<PreparedOp::Header>();

        EXPECT_EQ(sizeof(*header) + sizeof(WireFormat::TxParticipant) * 3 +
                  sizeof(Object::Header) +
                  sizeof(KeyCount) + sizeof(CumulativeKeyLength) + 5 + 4,
                  buffer.size());

        EXPECT_EQ(WireFormat::TxPrepare::WRITE, header->type);
        EXPECT_EQ(1UL, header->clientId);
        EXPECT_EQ(10UL, header->rpcId);
        EXPECT_EQ(3UL, header->participantCount);
        //EXPECT_EQ(0xE86291D1, op->header.checksum);

        WireFormat::TxParticipant* partList =
            (WireFormat::TxParticipant*) buffer.getRange(sizeof32(*header),
            sizeof32(WireFormat::TxParticipant) * 3);
        EXPECT_EQ(WireFormat::TxParticipant(572U, keyHash, 10U),
                  partList[0]);
        EXPECT_EQ(WireFormat::TxParticipant(573U, keyHash, 11U),
                  partList[1]);
        EXPECT_EQ(WireFormat::TxParticipant(574U, keyHash, 12U),
                  partList[2]);

        uint32_t offset = sizeof32(*header) + sizeof32(TxParticipant) * 3;
        const Object::Header* objHdr =
                buffer.getOffset<Object::Header>(offset);
        EXPECT_EQ(572U, objHdr->tableId);
        EXPECT_EQ(75U, objHdr->version);
        EXPECT_EQ(723U, objHdr->timestamp);
        offset += sizeof32(Object::Header);

        const KeyCount numKeys= *buffer.getOffset<KeyCount>(offset);
        EXPECT_EQ(1U, numKeys);
        offset += sizeof32(KeyCount);

        const CumulativeKeyLength cumulativeKeyLength = *buffer.getOffset<
                                                    CumulativeKeyLength>(
                                                    offset);
        EXPECT_EQ(5U, cumulativeKeyLength);
        offset += sizeof32(CumulativeKeyLength);

        EXPECT_EQ("key!", string(reinterpret_cast<const char *>(
                        buffer.getRange(offset, 5))));
        offset += 5;

        const void* data = buffer.getRange(offset, 4);
        EXPECT_EQ("YO!", string(reinterpret_cast<const char*>(data)));
    }
}

TEST_F(PreparedOpTest, checkIntegrity) {
    //TODO(seojin): revisit this and check correctness...
    //              Not sure about testing on record[0].
    for (uint32_t i = 0; i < arrayLength(records); i++) {
        PreparedOp& record = *records[i];
        Buffer buffer;
        record.assembleForLog(buffer);
        EXPECT_TRUE(record.checkIntegrity());

        uint8_t* evil = reinterpret_cast<uint8_t*>(
            const_cast<void*>(buffer.getRange(0, 1)));
        uint8_t tmp = *evil;
        *evil = static_cast<uint8_t>(~*evil);
        EXPECT_FALSE(record.checkIntegrity());
        *evil = tmp;

        // TODO(seojin): Make sure integrity check works?
//        EXPECT_TRUE(record.checkIntegrity());
//        evil = reinterpret_cast<uint8_t*>(
//            const_cast<void*>(buffer.getRange(buffer.size() - 1, 1)));
//        tmp = *evil;
//        *evil = static_cast<uint8_t>(~*evil);
//        EXPECT_FALSE(record.checkIntegrity());
//        *evil = tmp;

        EXPECT_TRUE(record.checkIntegrity());
    }
}

/**
 * Unit tests for PreparedOpTombstone.
 */
class PreparedOpTombstoneTest : public ::testing::Test {
  public:
    PreparedOpTombstoneTest()
        : stringKey()
        , dataBlob()
        , keyHash()
        , buffer()
        , buffer2()
        , preparedOp()
        , preparedOpTombstoneFromRpc()
        , preparedOpTombstoneFromBuffer()
    {
        snprintf(stringKey, sizeof(stringKey), "key!");
        snprintf(dataBlob, sizeof(dataBlob), "YO!");
        Key key(572, stringKey, 5);

        // this is the starting of keysAndValue in the keysAndValueBuffer
        buffer.emplaceAppend<KeyCount>((unsigned char)1);
        buffer.emplaceAppend<CumulativeKeyLength>((uint16_t) 5);
        buffer.appendExternal(stringKey, sizeof(stringKey));
        buffer.appendExternal(dataBlob, 4);

        // construct participant list.
        keyHash = key.getHash();
        participants[0] = WireFormat::TxParticipant(572U, key.getHash(), 10U);
        participants[1] = WireFormat::TxParticipant(573U, key.getHash(), 11U);
        participants[2] = WireFormat::TxParticipant(574U, key.getHash(), 12U);

        preparedOp.construct(WireFormat::TxPrepare::WRITE,
                             1UL, 10UL, 3U, participants,
                             key.getTableId(), 75, 723, buffer);

        preparedOpTombstoneFromRpc.construct(*preparedOp, 999UL);

        preparedOpTombstoneFromRpc->assembleForLog(buffer2);

        // prepend some garbage to buffer2 so that we can test the constructor
        // with a non-zero offset
        memcpy(buffer2.allocPrepend(sizeof(stringKey)), &stringKey,
                sizeof(stringKey));

        preparedOpTombstoneFromBuffer.construct(buffer2, sizeof32(stringKey));

        records[0] = &*preparedOpTombstoneFromRpc;
        records[1] = &*preparedOpTombstoneFromBuffer;
    }

    ~PreparedOpTombstoneTest()
    {
    }

    // Don't use static strings, since they'll be loaded into read-only
    // memory and we can't mutate to test checksumming.
    char stringKey[5];
    char dataBlob[4];

    uint64_t keyHash;
    WireFormat::TxParticipant participants[3];

    Buffer buffer;
    Buffer buffer2;

    Tub<PreparedOp> preparedOp;
    Tub<PreparedOpTombstone> preparedOpTombstoneFromRpc;
    Tub<PreparedOpTombstone> preparedOpTombstoneFromBuffer;

    PreparedOpTombstone* records[2];

    DISALLOW_COPY_AND_ASSIGN(PreparedOpTombstoneTest);
};

TEST_F(PreparedOpTombstoneTest, constructors) {
    for (uint32_t i = 0; i < arrayLength(records); ++i) {
        PreparedOpTombstone& record = *records[i];
        EXPECT_EQ(1UL, record.header.leaseId);
        EXPECT_EQ(10UL, record.header.rpcId);
        EXPECT_EQ(999UL, record.header.segmentId);
    }
}

TEST_F(PreparedOpTombstoneTest, assembleForLog) {
    for (uint32_t i = 0; i < arrayLength(records); i++) {
        Buffer buffer;
        PreparedOpTombstone& record = *(records[i]);
        record.assembleForLog(buffer);
        const PreparedOpTombstone::Header* header =
                buffer.getStart<PreparedOpTombstone::Header>();

        EXPECT_EQ(sizeof(*header), buffer.size());

        EXPECT_EQ(1UL, header->leaseId);
        EXPECT_EQ(10UL, header->rpcId);
        EXPECT_EQ(999UL, header->segmentId);
        //EXPECT_EQ(0xE86291D1, op->header.checksum);
    }
}

TEST_F(PreparedOpTombstoneTest, checkIntegrity) {
    //TODO(seojin): revisit this and check correctness...
    //              Not sure about testing on record[0].
    for (uint32_t i = 0; i < arrayLength(records); i++) {
        PreparedOpTombstone& record = *records[i];
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

class PreparedWritesTest : public ::testing::Test {
  public:
    Context context;

    PreparedWrites writes;

    PreparedWritesTest()
        : context()
        , writes(&context)
    {
        context.dispatch = new Dispatch(false);
        writes.bufferWrite(1, 10, 1011);
    }

    ~PreparedWritesTest() {}

    DISALLOW_COPY_AND_ASSIGN(PreparedWritesTest);
};

TEST_F(PreparedWritesTest, bufferWrite) {
    writes.bufferWrite(2, 8, 1028);
    PreparedWrites::PreparedItem* item = writes.items[std::make_pair(2UL, 8UL)];
    EXPECT_EQ(1028UL, item->newOpPtr);
    EXPECT_TRUE(item->isRunning());

    // Use during recovery. Should not set timer.
    writes.bufferWrite(2, 9, 1029, true);
    item = writes.items[std::make_pair(2UL, 9UL)];
    EXPECT_EQ(1029UL, item->newOpPtr);
    EXPECT_FALSE(item->isRunning());
}

TEST_F(PreparedWritesTest, popOp) {
    EXPECT_EQ(1011UL, writes.popOp(1, 10));
    EXPECT_EQ(0UL, writes.popOp(1, 10));
    EXPECT_EQ(0UL, writes.popOp(1, 11));
    EXPECT_EQ(0UL, writes.popOp(2, 10));
}

TEST_F(PreparedWritesTest, peekOp) {
    EXPECT_EQ(1011UL, writes.peekOp(1, 10));
    EXPECT_EQ(1011UL, writes.peekOp(1, 10));
    EXPECT_EQ(1011UL, writes.popOp(1, 10));
    EXPECT_EQ(0UL, writes.peekOp(1, 10));
    EXPECT_EQ(0UL, writes.peekOp(1, 11));
    EXPECT_EQ(0UL, writes.peekOp(2, 10));
}

TEST_F(PreparedWritesTest, markDeletedAndIsDeleted) {
    EXPECT_FALSE(writes.isDeleted(1, 11));
    writes.markDeleted(1, 11);
    EXPECT_TRUE(writes.isDeleted(1, 11));

    EXPECT_FALSE(writes.isDeleted(2, 9));
    writes.bufferWrite(2, 9, 1029, true);
    EXPECT_EQ(1029UL, writes.peekOp(2, 9));
    EXPECT_FALSE(writes.isDeleted(2, 9));
}

} // namespace RAMCloud
