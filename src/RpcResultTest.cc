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
#include "RpcResult.h"
#include "RamCloud.h"

namespace RAMCloud {

/**
 * Unit tests for RpcResult.
 */
class RpcResultTest : public ::testing::Test {
  public:
    RpcResultTest()
        : stringKey(),
          response({{Status::STATUS_OK}, 123UL}),
          buffer(),
          buffer2(),
          indexBuffer(),
          rpcResultFromResponse(),
          rpcResultFromBuffer(),
          indexRpcRecordFromResponse(),
          indexRpcRecordFromBuffer()
    {
        snprintf(stringKey, sizeof(stringKey), "key!");
        Key key(572, stringKey, 5);

        buffer.appendExternal(&response, sizeof(response));

        rpcResultFromResponse.construct(key.getTableId(),
                                        key.getHash(),
                                        1UL,
                                        10UL,
                                        9UL,
                                        buffer,
                                        0,
                                        sizeof32(response));

        rpcResultFromResponse->assembleForLog(buffer2);

        // prepend some garbage to buffer2 so that we can test the constructor
        // with a non-zero offset
        memcpy(buffer2.allocPrepend(sizeof(stringKey)), &stringKey,
                sizeof(stringKey));

        rpcResultFromBuffer.construct(buffer2, sizeof32(stringKey),
                                   buffer2.size() - sizeof32(stringKey));

        objectRecords[0] = &*rpcResultFromResponse;
        objectRecords[1] = &*rpcResultFromBuffer;
        records[0] = &*rpcResultFromResponse;
        records[1] = &*rpcResultFromBuffer;
        
        // Index rpc records
        indexRpcRecordFromResponse.construct(key.getTableId(),
                                             key.getStringKeyLength(),
                                             static_cast<const void*>(stringKey),
                                             1UL,
                                             10UL,
                                             9UL,
                                             buffer,
                                             0,
                                             sizeof32(response));

        indexRpcRecordFromResponse->assembleForLog(indexBuffer);

        // prepend some garbage to indexBuffer so that we can test the constructor
        // with a non-zero offset
        memcpy(indexBuffer.allocPrepend(sizeof(stringKey)), &stringKey,
                sizeof(stringKey));

        indexRpcRecordFromBuffer.construct(indexBuffer, sizeof32(stringKey),
                                           indexBuffer.size() -
                                           sizeof32(stringKey));

        indexRecords[0] = &*indexRpcRecordFromResponse;
        indexRecords[1] = &*indexRpcRecordFromBuffer;
        records[2] = &*indexRpcRecordFromResponse;
        records[3] = &*indexRpcRecordFromBuffer;
    }

    ~RpcResultTest()
    {
    }

    // Don't use static strings, since they'll be loaded into read-only
    // memory and we can't mutate to test checksumming.
    char stringKey[5];
    WireFormat::Write::Response response;

    Buffer buffer;
    Buffer buffer2;
    Buffer indexBuffer;

    Tub<RpcResult> rpcResultFromResponse;
    Tub<RpcResult> rpcResultFromBuffer;
    Tub<RpcResult> indexRpcRecordFromResponse;
    Tub<RpcResult> indexRpcRecordFromBuffer;
    //TODO(seojin): Test for constructor with response with contiguous memory.

    RpcResult* records[4];
    RpcResult* objectRecords[2];
    RpcResult* indexRecords[2];

    DISALLOW_COPY_AND_ASSIGN(RpcResultTest);
};

TEST_F(RpcResultTest, constructor_fromResponse) {
    RpcResult& record = *records[0];
    Key key(572, stringKey, 5);
    
    EXPECT_EQ(RpcResult::RecordType::OBJECT, record.header.type);

    EXPECT_EQ(572U, record.header.tableId);
    EXPECT_EQ(key.getHash(), record.header.keyHash);
    EXPECT_EQ(1UL, record.header.leaseId);
    EXPECT_EQ(10UL, record.header.rpcId);
    EXPECT_EQ(9UL, record.header.ackId);
    EXPECT_EQ(1216503646UL, record.header.checksum);

    EXPECT_TRUE(record.response);

    const WireFormat::Write::Response* resp = reinterpret_cast<
            const WireFormat::Write::Response*>(record.response);
    EXPECT_EQ(Status::STATUS_OK, resp->common.status);
    EXPECT_EQ(123UL, resp->version);
}

TEST_F(RpcResultTest, constructor_fromBuffer) {
    RpcResult& record = *records[1];
    Key key(572, stringKey, 5);
    
    EXPECT_EQ(RpcResult::RecordType::OBJECT, record.header.type);

    EXPECT_EQ(572U, record.header.tableId);
    EXPECT_EQ(key.getHash(), record.header.keyHash);
    EXPECT_EQ(1UL, record.header.leaseId);
    EXPECT_EQ(10UL, record.header.rpcId);
    EXPECT_EQ(9UL, record.header.ackId);
    EXPECT_EQ(1216503646UL, record.header.checksum);

    EXPECT_FALSE(record.response);
    EXPECT_TRUE(record.respBuffer);

    EXPECT_EQ(5 + sizeof(RpcResult::Header)
                + sizeof(WireFormat::Write::Response),
              (record.respBuffer)->size());

    WireFormat::Write::Response* resp =
        reinterpret_cast<WireFormat::Write::Response*>(
            record.respBuffer->getRange(sizeof(RpcResult::Header) + 5,
                 sizeof(WireFormat::Write::Response)));
    EXPECT_EQ(Status::STATUS_OK, resp->common.status);
    EXPECT_EQ(123UL, resp->version);
}

TEST_F(RpcResultTest, constructor_indexFromResponse) {
    RpcResult& record = *indexRecords[0];
    Key key(572, stringKey, 5);

    EXPECT_EQ(RpcResult::RecordType::INDEX, record.header.type);

    EXPECT_EQ(572U, record.header.tableId);
    EXPECT_EQ(0U, record.header.keyHash);
    EXPECT_EQ(1UL, record.header.leaseId);
    EXPECT_EQ(10UL, record.header.rpcId);
    EXPECT_EQ(9UL, record.header.ackId);
    EXPECT_EQ(2043374754UL, record.header.checksum);
    
    EXPECT_TRUE(record.indexKey);
    EXPECT_TRUE(record.response);

    EXPECT_EQ(sizeof(stringKey), record.indexKeyLength);
    EXPECT_EQ("key!", string(reinterpret_cast<const char*>(record.indexKey)));

    const WireFormat::Write::Response* resp = reinterpret_cast<
            const WireFormat::Write::Response*>(record.response);
    EXPECT_EQ(Status::STATUS_OK, resp->common.status);
    EXPECT_EQ(123UL, resp->version);
}

TEST_F(RpcResultTest, constructor_indexFromBuffer) {
    RpcResult& record = *indexRecords[1];
    Key key(572, stringKey, 5);
    
    EXPECT_EQ(RpcResult::RecordType::INDEX, record.header.type);

    EXPECT_EQ(572U, record.header.tableId);
    EXPECT_EQ(0U, record.header.keyHash);
    EXPECT_EQ(1UL, record.header.leaseId);
    EXPECT_EQ(10UL, record.header.rpcId);
    EXPECT_EQ(9UL, record.header.ackId);
    EXPECT_EQ(2043374754UL, record.header.checksum);

    EXPECT_FALSE(record.response);
    EXPECT_FALSE(record.indexKey);
    EXPECT_TRUE(record.respBuffer);
    
    EXPECT_EQ(sizeof(stringKey), record.indexKeyLength);
    EXPECT_EQ("key!", string(reinterpret_cast<const char*>(
        (record.respBuffer)->getRange(
                sizeof(RpcResult::Header) + 5 +
                sizeof(KeyLength), 5))));

    EXPECT_EQ(5 + sizeof(RpcResult::Header) +
              sizeof(WireFormat::Write::Response) +
              sizeof(KeyLength) + sizeof(stringKey),
              (record.respBuffer)->size());

    WireFormat::Write::Response* resp =
            reinterpret_cast<WireFormat::Write::Response*>(
                    record.respBuffer->getRange(
                            sizeof(RpcResult::Header) +
                            5 + sizeof(KeyLength) +
                            sizeof(stringKey),
                            sizeof(WireFormat::Write::Response)));
    EXPECT_EQ(Status::STATUS_OK, resp->common.status);
    EXPECT_EQ(123UL, resp->version);
}


TEST_F(RpcResultTest, assembleForLog) {
    Key key(572, stringKey, 5);
    for (uint32_t i = 0; i < arrayLength(objectRecords); i++) {
        RpcResult& record = *objectRecords[i];
        Buffer buffer;
        record.assembleForLog(buffer);
        const RpcResult::Header* header =
            buffer.getStart<RpcResult::Header>();

        EXPECT_EQ(sizeof(*header) + sizeof(WireFormat::Write::Response),
                  buffer.size());

        EXPECT_EQ(RpcResult::RecordType::OBJECT, header->type);

        EXPECT_EQ(572U, header->tableId);
        EXPECT_EQ(key.getHash(), header->keyHash);
        EXPECT_EQ(1UL, header->leaseId);
        EXPECT_EQ(10UL, header->rpcId);
        EXPECT_EQ(9UL, header->ackId);
        EXPECT_EQ(1216503646UL, header->checksum);

        const void* respRaw = buffer.getRange(sizeof(*header),
                    sizeof(WireFormat::Write::Response));
        const WireFormat::Write::Response* resp =
                reinterpret_cast<const WireFormat::Write::Response*>(respRaw);
        EXPECT_EQ(Status::STATUS_OK, resp->common.status);
        EXPECT_EQ(123UL, resp->version);
    }
}

TEST_F(RpcResultTest, assembleForLog_contigMemory) {
    Key key(572, stringKey, 5);
    for (uint32_t i = 0; i < arrayLength(objectRecords); i++) {
        RpcResult& record = *objectRecords[i];
        Buffer buffer;
        uint8_t* target = static_cast<uint8_t*>(buffer.alloc(
                record.getSerializedLength()));

        record.assembleForLog(target);

        RpcResult::Header* header =
            reinterpret_cast<RpcResult::Header*>(target);

        EXPECT_EQ(sizeof(*header) + sizeof(WireFormat::Write::Response),
                  record.getSerializedLength());

        EXPECT_EQ(RpcResult::RecordType::OBJECT, header->type);

        EXPECT_EQ(572U, header->tableId);
        EXPECT_EQ(key.getHash(), header->keyHash);
        EXPECT_EQ(1UL, header->leaseId);
        EXPECT_EQ(10UL, header->rpcId);
        EXPECT_EQ(9UL, header->ackId);
        EXPECT_EQ(1216503646UL, header->checksum);

        const void* respRaw = target + sizeof(*header);
        const WireFormat::Write::Response* resp =
                reinterpret_cast<const WireFormat::Write::Response*>(respRaw);
        EXPECT_EQ(Status::STATUS_OK, resp->common.status);
        EXPECT_EQ(123UL, resp->version);
    }
}

TEST_F(RpcResultTest, assembleForLog_index) {
    for (uint32_t i = 0; i < arrayLength(indexRecords); i++) {
        RpcResult& record = *indexRecords[i];
        Buffer buffer;
        record.assembleForLog(buffer);
        const RpcResult::Header* header =
            buffer.getStart<RpcResult::Header>();

        EXPECT_EQ(sizeof(*header) + sizeof(KeyLength) + sizeof(stringKey) +
                  sizeof(WireFormat::Write::Response), buffer.size());

        EXPECT_EQ(RpcResult::RecordType::INDEX, header->type);
        
        EXPECT_EQ(572U, header->tableId);
        EXPECT_EQ(0UL, header->keyHash);
        EXPECT_EQ(1UL, header->leaseId);
        EXPECT_EQ(10UL, header->rpcId);
        EXPECT_EQ(9UL, header->ackId);
        EXPECT_EQ(2043374754UL, header->checksum);

        KeyLength* keyLen = buffer.getOffset<KeyLength>(sizeof(*header));
        EXPECT_EQ(sizeof(stringKey), *keyLen);

        const void* key = buffer.getRange(sizeof(*header) + sizeof(*keyLen),
                                          *keyLen);
        EXPECT_EQ("key!", string(reinterpret_cast<const char*>(key)));

        const void* respRaw = buffer.getRange(
                sizeof32(*header) +
                sizeof32(*keyLen) +
                *keyLen,
                sizeof32(WireFormat::Write::Response));
        const WireFormat::Write::Response* resp =
                reinterpret_cast<const WireFormat::Write::Response*>(respRaw);
        EXPECT_EQ(Status::STATUS_OK, resp->common.status);
        EXPECT_EQ(123UL, resp->version);
    }
}

TEST_F(RpcResultTest, assembleForLog_indexContigMemory) {
    for (uint32_t i = 0; i < arrayLength(indexRecords); i++) {
        RpcResult& record = *indexRecords[i];
        Buffer buffer;
        uint8_t* target = static_cast<uint8_t*>(buffer.alloc(
                record.getSerializedLength()));

        record.assembleForLog(target);
        
        RpcResult::Header* header =
            reinterpret_cast<RpcResult::Header*>(target);

        EXPECT_EQ(sizeof(*header) + sizeof(KeyLength) + sizeof(stringKey) +
                  sizeof(WireFormat::Write::Response),
                  record.getSerializedLength());

        EXPECT_EQ(RpcResult::RecordType::INDEX, header->type);
        
        EXPECT_EQ(572U, header->tableId);
        EXPECT_EQ(0UL, header->keyHash);
        EXPECT_EQ(1UL, header->leaseId);
        EXPECT_EQ(10UL, header->rpcId);
        EXPECT_EQ(9UL, header->ackId);
        EXPECT_EQ(2043374754UL, header->checksum);

        KeyLength* keyLen = reinterpret_cast<KeyLength*>(target +
                                                         sizeof(*header));
        EXPECT_EQ(sizeof(stringKey), *keyLen);

        const void* key = target + sizeof(*header) + sizeof(*keyLen);
        EXPECT_EQ("key!", string(reinterpret_cast<const char*>(key)));

        const void* respRaw = target + sizeof(*header) + sizeof(*keyLen) +
                *keyLen;
        const WireFormat::Write::Response* resp =
                reinterpret_cast<const WireFormat::Write::Response*>(respRaw);
        EXPECT_EQ(Status::STATUS_OK, resp->common.status);
        EXPECT_EQ(123UL, resp->version);
    }
}


TEST_F(RpcResultTest, appendRespToBuffer) {
    for (uint32_t i = 0; i < arrayLength(records); i++) {
        RpcResult& record = *records[i];
        Buffer buffer;
        record.appendRespToBuffer(buffer);
        EXPECT_EQ(sizeof(WireFormat::Write::Response), buffer.size());
        const void* respRaw = buffer.getRange(0,
                    sizeof(WireFormat::Write::Response));
        const WireFormat::Write::Response* resp =
                reinterpret_cast<const WireFormat::Write::Response*>(respRaw);
        EXPECT_EQ(Status::STATUS_OK, resp->common.status);
        EXPECT_EQ(123UL, resp->version);
    }
}

TEST_F(RpcResultTest, appendIndexKeyToBuffer) {
    for (uint32_t i = 0; i < arrayLength(indexRecords); i++) {
        RpcResult& record = *indexRecords[i];
        Buffer buffer;
        record.appendIndexKeyToBuffer(buffer);
        EXPECT_EQ(sizeof(KeyLength) + sizeof(stringKey), buffer.size());
        
        KeyLength* keyLen = buffer.getStart<KeyLength>();
        EXPECT_EQ(sizeof(stringKey), *keyLen);
        
        const void* key = buffer.getRange(sizeof(*keyLen), *keyLen);
        EXPECT_EQ("key!", string(reinterpret_cast<const char*>(key)));
    }
}

TEST_F(RpcResultTest, getType) {
    for (uint32_t i = 0; i < arrayLength(objectRecords); i++)
        EXPECT_EQ(RpcResult::RecordType::OBJECT,
                  objectRecords[i]->getType());
    
    for (uint32_t i = 0; i < arrayLength(indexRecords); i++)
        EXPECT_EQ(RpcResult::RecordType::INDEX,
                  indexRecords[i]->getType());
}

TEST_F(RpcResultTest, getTableId) {
    for (uint32_t i = 0; i < arrayLength(records); i++)
        EXPECT_EQ(572U, records[i]->getTableId());
}

TEST_F(RpcResultTest, getKeyHash) {
    Key key(572, stringKey, 5);
    for (uint32_t i = 0; i < arrayLength(records); i++) {
        if (records[i]->header.type == RpcResult::RecordType::OBJECT) {
            EXPECT_EQ(key.getHash(), records[i]->getKeyHash());
        } else { // Index record
            EXPECT_EQ(0UL, records[i]->getKeyHash());
        }
    }
}

TEST_F(RpcResultTest, getLeaseId) {
    for (uint32_t i = 0; i < arrayLength(records); i++)
        EXPECT_EQ(1U, records[i]->getLeaseId());
}

TEST_F(RpcResultTest, getRpcId) {
    for (uint32_t i = 0; i < arrayLength(records); i++)
        EXPECT_EQ(10U, records[i]->getRpcId());
}

TEST_F(RpcResultTest, getAckId) {
    for (uint32_t i = 0; i < arrayLength(records); i++)
        EXPECT_EQ(9U, records[i]->getAckId());
}

TEST_F(RpcResultTest, getResp) {
    for (uint32_t i = 0; i < arrayLength(records); i++) {
        RpcResult& record = *records[i];

        uint32_t respLength;
        const void* respRaw = record.getResp(&respLength);
        EXPECT_EQ(sizeof(WireFormat::Write::Response), respLength);
        const WireFormat::Write::Response* resp =
                reinterpret_cast<const WireFormat::Write::Response*>(respRaw);
        EXPECT_EQ(Status::STATUS_OK, resp->common.status);
        EXPECT_EQ(123UL, resp->version);
    }
}

TEST_F(RpcResultTest, getRespLength) {
    for (uint32_t i = 0; i < arrayLength(records); i++)
        EXPECT_EQ(sizeof(WireFormat::Write::Response),
                  records[i]->getRespLength());
}

TEST_F(RpcResultTest, getIndexKey) {
    for (uint32_t i = 0; i < arrayLength(indexRecords); i++) {
        RpcResult& record = *indexRecords[i];

        KeyLength keyLen;
        const void* key = record.getIndexKey(&keyLen);
        EXPECT_EQ(sizeof(stringKey), keyLen);
        EXPECT_EQ("key!", string(reinterpret_cast<const char*>(key)));
    }
}

TEST_F(RpcResultTest, getIndexKeyLength) {
    for (uint32_t i = 0; i < arrayLength(indexRecords); i++)
        EXPECT_EQ(sizeof(stringKey), indexRecords[i]->getIndexKeyLength());
}

TEST_F(RpcResultTest, checkIntegrity) {
    for (uint32_t i = 0; i < arrayLength(records); i++) {
        RpcResult& record = *records[i];
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

TEST_F(RpcResultTest, getSerializedLength) {
    for (uint32_t i = 0; i < arrayLength(records); i++) {
        if (records[i]->header.type == RpcResult::RecordType::OBJECT) {
            EXPECT_EQ(57U, records[i]->getSerializedLength());
        } else { // Index record
            EXPECT_EQ(64U, records[i]->getSerializedLength());
        }
    }
}

} // namespace RAMCloud
