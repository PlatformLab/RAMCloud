/* Copyright (c) 2014-2015 Stanford University
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

#include "AbstractLog.h"
#include "Context.h"
#include "MasterService.h"
#include "MockCluster.h"
#include "PreparedOps.h"
#include "RamCloud.h"
#include "StringUtil.h"

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

        preparedOpFromRpc.construct(WireFormat::TxPrepare::WRITE,
                                    1UL,
                                    9UL,
                                    10UL,
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
    EXPECT_EQ(9UL, record.header.clientTxId);
    EXPECT_EQ(10UL, record.header.rpcId);

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
    EXPECT_EQ(9UL, record.header.clientTxId);
    EXPECT_EQ(10UL, record.header.rpcId);

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

        EXPECT_EQ(sizeof(*header) +
                  sizeof(Object::Header) +
                  sizeof(KeyCount) + sizeof(CumulativeKeyLength) + 5 + 4,
                  buffer.size());

        EXPECT_EQ(WireFormat::TxPrepare::WRITE, header->type);
        EXPECT_EQ(1UL, header->clientId);
        EXPECT_EQ(9UL, header->clientTxId);
        EXPECT_EQ(10UL, header->rpcId);
        //EXPECT_EQ(0xE86291D1, op->header.checksum);

        uint32_t offset = sizeof32(*header);
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
    for (uint32_t i = 0; i < arrayLength(records); i++) {
        PreparedOp& record = *records[i];
        Buffer buffer;
        record.assembleForLog(buffer);
        EXPECT_TRUE(record.checkIntegrity());

        // Test first bit flip.
        uint8_t* evil = reinterpret_cast<uint8_t*>(
            const_cast<void*>(buffer.getRange(0, 1)));
        uint8_t tmp = *evil;
        *evil = static_cast<uint8_t>(~*evil);
        PreparedOp recordEvil(buffer, 0, buffer.size());
        EXPECT_FALSE(recordEvil.checkIntegrity());
        *evil = tmp;
        {
            PreparedOp record2(buffer, 0, buffer.size());
            EXPECT_TRUE(record2.checkIntegrity());
        }

        // Test last bit flip.
        evil = reinterpret_cast<uint8_t*>(
            const_cast<void*>(buffer.getRange(buffer.size() - 1, 1)));
        tmp = *evil;
        *evil = static_cast<uint8_t>(~*evil);
        PreparedOp recordEvil2(buffer, 0, buffer.size());
        EXPECT_FALSE(recordEvil2.checkIntegrity());
        *evil = tmp;
        {
            PreparedOp record2(buffer, 0, buffer.size());
            EXPECT_TRUE(record2.checkIntegrity());
        }
    }
}

TEST_F(PreparedOpTest, getTransactionId) {
    for (uint32_t i = 0; i < arrayLength(records); i++) {
        PreparedOp& record = *records[i];

        record.getTransactionId();
        EXPECT_EQ(1U, record.header.clientId);
        EXPECT_EQ(9U, record.header.clientTxId);
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
                             1UL, 10UL, 10UL,
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
        EXPECT_EQ(1UL, record.header.clientLeaseId);
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

        EXPECT_EQ(1UL, header->clientLeaseId);
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

/**
 * Unit tests for PreparedOps.
 */
class PreparedOpsTest : public ::testing::Test {
  public:
    Context context;

    PreparedOps writes;

    PreparedOpsTest()
        : context()
        , writes(&context)
    {
        context.dispatch = new Dispatch(false);
        writes.bufferOp(1, 10, 1011);
    }

    ~PreparedOpsTest() {}

    DISALLOW_COPY_AND_ASSIGN(PreparedOpsTest);
};

TEST_F(PreparedOpsTest, bufferWrite) {
    writes.bufferOp(2, 8, 1028);
    PreparedOps::PreparedItem* item = writes.items[std::make_pair(2UL, 8UL)];
    EXPECT_EQ(1028UL, item->newOpPtr);
    EXPECT_TRUE(item->isRunning());

    // Use during recovery. Should not set timer.
    writes.bufferOp(2, 9, 1029, true);
    item = writes.items[std::make_pair(2UL, 9UL)];
    EXPECT_EQ(1029UL, item->newOpPtr);
    EXPECT_FALSE(item->isRunning());
}

TEST_F(PreparedOpsTest, popOp) {
    EXPECT_EQ(1011UL, writes.popOp(1, 10));
    EXPECT_EQ(0UL, writes.popOp(1, 10));
    EXPECT_EQ(0UL, writes.popOp(1, 11));
    EXPECT_EQ(0UL, writes.popOp(2, 10));
}

TEST_F(PreparedOpsTest, peekOp) {
    EXPECT_EQ(1011UL, writes.peekOp(1, 10));
    EXPECT_EQ(1011UL, writes.peekOp(1, 10));
    EXPECT_EQ(1011UL, writes.popOp(1, 10));
    EXPECT_EQ(0UL, writes.peekOp(1, 10));
    EXPECT_EQ(0UL, writes.peekOp(1, 11));
    EXPECT_EQ(0UL, writes.peekOp(2, 10));
}

TEST_F(PreparedOpsTest, markDeletedAndIsDeleted) {
    EXPECT_FALSE(writes.isDeleted(1, 11));
    writes.markDeleted(1, 11);
    EXPECT_TRUE(writes.isDeleted(1, 11));

    EXPECT_FALSE(writes.isDeleted(2, 9));
    writes.bufferOp(2, 9, 1029, true);
    EXPECT_EQ(1029UL, writes.peekOp(2, 9));
    EXPECT_FALSE(writes.isDeleted(2, 9));
}

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

/**
 * Unit tests for PreparedItem.
 */
class PreparedItemTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    Server* server1;
    Server* server2;
    Server* server3;
    MasterService* service1;
    MasterService* service2;
    MasterService* service3;
    uint64_t tableId1;
    uint64_t tableId2;
    uint64_t tableId3;

    PreparedItemTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , ramcloud()
        , server1()
        , server2()
        , server3()
        , service1()
        , service2()
        , service3()
        , tableId1(-1)
        , tableId2(-2)
        , tableId3(-3)
    {
        Cycles::mockTscValue = 100;

        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master1";
        config.maxObjectKeySize = 512;
        config.maxObjectDataSize = 1024;
        config.segmentSize = 128*1024;
        config.segletSize = 128*1024;
        server1 = cluster.addServer(config);
        service1 = server1->master.get();
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master2";
        server2 = cluster.addServer(config);
        service2 = server2->master.get();
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master3";
        server3 = cluster.addServer(config);
        service3 = server3->master.get();
        ramcloud.construct(&context, "mock:host=coordinator");

        // Make some tables.
        tableId1 = ramcloud->createTable("table1");
        tableId2 = ramcloud->createTable("table2");
        tableId3 = ramcloud->createTable("table3");
    }

    ~PreparedItemTest()
    {
        Cycles::mockTscValue = 0;
    }

    bool waitForTxRecoveryDone()
    {
        // See "Timing-Dependent Tests" in designNotes.
        for (int i = 0; i < 1000; i++) {
            service1->context->dispatch->poll();
            service2->context->dispatch->poll();
            service3->context->dispatch->poll();
            if (service1->preparedOps.items.size() == 0 &&
                    service2->preparedOps.items.size() == 0 &&
                    service3->preparedOps.items.size() == 0 &&
                    service1->txRecoveryManager.recoveries.size() == 0 &&
                    service2->txRecoveryManager.recoveries.size() == 0 &&
                    service3->txRecoveryManager.recoveries.size() == 0) {
                return true;
            }
            usleep(1000);
        }
        EXPECT_EQ(0lu, service1->preparedOps.items.size());
        EXPECT_EQ(0lu, service2->preparedOps.items.size());
        EXPECT_EQ(0lu, service3->preparedOps.items.size());
        EXPECT_EQ(0lu, service1->txRecoveryManager.recoveries.size());
        EXPECT_EQ(0lu, service2->txRecoveryManager.recoveries.size());
        EXPECT_EQ(0lu, service3->txRecoveryManager.recoveries.size());
        return false;
    }

    DISALLOW_COPY_AND_ASSIGN(PreparedItemTest);
};

TEST_F(PreparedItemTest, handleTimerEvent_basic) {
    Key key1(tableId3, "key1", 4);
    Key key2(tableId2, "key2", 4);
    Key key3(tableId1, "key3", 4);
    WireFormat::TxParticipant participants[3];
    participants[0] = {tableId3, key1.getHash(), 11};
    participants[1] = {tableId2, key2.getHash(), 12};
    participants[2] = {tableId1, key3.getHash(), 13};

    ParticipantList participantList(participants, 3, 1, 10);
    uint64_t logRef;
    service1->objectManager.logTransactionParticipantList(participantList,
                                                          &logRef);
    service1->unackedRpcResults.recoverRecord(
            participantList.header.clientLeaseId,
            participantList.header.clientTransactionId,
            0,
            reinterpret_cast<void*>(logRef));

    Buffer keyAndValBuffer;

    Object::appendKeysAndValueToBuffer(key3, "val", 3, &keyAndValBuffer);

    PreparedOp op(WireFormat::TxPrepare::OpType::READ, 1, 10, 13,
                  tableId1, 0, 0, keyAndValBuffer);
    Buffer buf;
    op.assembleForLog(buf);
    Segment::Reference opRef;
    service1->objectManager.getLog()->append(LOG_ENTRY_TYPE_PREP, buf, &opRef);

    TestLog::Enable _;

    Cycles::mockTscValue = 100;
    service1->preparedOps.bufferOp(1, 13, opRef.toInteger());
    EXPECT_EQ(opRef.toInteger(), service1->preparedOps.peekOp(1, 13));
    PreparedOps::PreparedItem* item = service1->preparedOps.items[
            std::make_pair<uint64_t, uint64_t>(1, 13)];
    EXPECT_TRUE(item != NULL && item->isRunning());
    service1->context->dispatch->poll();
    EXPECT_EQ("", TestLog::get());

    Cycles::mockTscValue += Cycles::fromMicroseconds(
            PreparedOps::PreparedItem::TX_TIMEOUT_US * 1.5);
    service1->context->dispatch->poll();
    EXPECT_TRUE(waitForTxRecoveryDone());
    EXPECT_TRUE(StringUtil::contains(TestLog::get(),
        "handleTimerEvent: TxHintFailed RPC is sent to owner of tableId 3 "
        "and keyHash 8205713012933148717."));
}

TEST_F(PreparedItemTest, handleTimerEvent_noParticipantList) {
    Key key1(tableId3, "key1", 4);
    Key key2(tableId2, "key2", 4);
    Key key3(tableId1, "key3", 4);

    Buffer keyAndValBuffer;

    Object::appendKeysAndValueToBuffer(key3, "val", 3, &keyAndValBuffer);

    PreparedOp op(WireFormat::TxPrepare::OpType::READ, 1, 11, 13,
                  tableId1, 0, 0, keyAndValBuffer);
    Buffer buf;
    op.assembleForLog(buf);
    Segment::Reference opRef;
    service1->objectManager.getLog()->append(LOG_ENTRY_TYPE_PREP, buf, &opRef);

    TestLog::Enable _("handleTimerEvent");

    Cycles::mockTscValue = 100;
    service1->preparedOps.bufferOp(1, 13, opRef.toInteger());
    EXPECT_EQ(opRef.toInteger(), service1->preparedOps.peekOp(1, 13));
    PreparedOps::PreparedItem* item = service1->preparedOps.items[
            std::make_pair<uint64_t, uint64_t>(1, 13)];

    TestLog::reset();

    item->handleTimerEvent();

    EXPECT_EQ("handleTimerEvent: "
            "Unable to find participant list record for TxId (1, 11); "
            "client transaction recovery could not be requested.",
            TestLog::get());
}

} // namespace RAMCloud
