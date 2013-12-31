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

namespace RAMCloud {

/**
 * Unit tests for Object.
 */
class ObjectTest : public ::testing::Test {
  public:
    ObjectTest()
        : stringKeys(),
          dataBlob(),
          numKeys(3),
          endKeyOffsets(),
          keyList(),
          buffer(),
          buffer2(),
          buffer3(),
          buffer4(),
          objectDataFromBuffer(),
          objectFromBuffer(),
          objectFromContiguousVoidPointer(),
          objectFromVoidPointer(),
          objectForWriteRpc(),
          objects(),
          singleKeyObject(),
          writeRpcObject()
    {
        snprintf(stringKeys[0], sizeof(stringKeys[0]), "ha");
        snprintf(stringKeys[1], sizeof(stringKeys[1]), "hi");
        snprintf(stringKeys[2], sizeof(stringKeys[2]), "ho");
        snprintf(dataBlob, sizeof(dataBlob), "YO!");

        Key key(57, stringKeys[0], sizeof(stringKeys[0]));

        keyList[0].key = stringKeys[0];
        keyList[0].keyLength = 3;
        keyList[1].key = stringKeys[1];
        keyList[1].keyLength = 3;
        keyList[2].key = stringKeys[2];
        keyList[2].keyLength = 3;

        // write some garbage into the buffer so that the starting
        // offset of keysAndValue in dataBuffer is != 0. In this
        // case, it will be sizeof(stringKeys[0])
        buffer.append(&stringKeys[0], sizeof(stringKeys[0]));

        // this is the starting of keysAndValue in the dataBuffer
        buffer.append(&numKeys, sizeof(numKeys));

        // lengths of all they keys are 3.
        // store endKeyOffsets in the object

        endKeyOffsets[0] = 2;
        endKeyOffsets[1] = 5;
        endKeyOffsets[2] = 8;
        buffer.append(endKeyOffsets, 3 *sizeof(KeyLength));

        // append keys here.
        buffer.append(&stringKeys[0], sizeof(stringKeys[0]));
        buffer.append(&stringKeys[1], sizeof(stringKeys[1]));
        buffer.append(&stringKeys[2], sizeof(stringKeys[2]));

        // append data
        buffer.append(&dataBlob[0], 2);
        buffer.append(&dataBlob[2], 2);
        objectDataFromBuffer.construct(key.getTableId(), 75, 723, buffer,
                                        sizeof32(stringKeys[0]));

        objectDataFromBuffer->assembleForLog(buffer2);

        objectFromBuffer.construct(buffer2);

        objectFromContiguousVoidPointer.construct(
            buffer2.getRange(0, buffer2.getTotalLength()),
            buffer2.getTotalLength());

        // this object will only contain one key
        objectFromVoidPointer.construct(key, dataBlob, 4, 75, 723, buffer3);

        objectForWriteRpc.construct(numKeys, 57, keyList, dataBlob, 4, 75,
                                        723, buffer4);

        objects[0] = &*objectDataFromBuffer;
        objects[1] = &*objectFromBuffer;
        objects[2] = &*objectFromContiguousVoidPointer;
        singleKeyObject = &*objectFromVoidPointer;
        writeRpcObject = &*objectForWriteRpc;
    }

    ~ObjectTest()
    {
    }

    // Don't use static strings, since they'll be loaded into read-only
    // memory and we can't mutate to test checksumming.
    char stringKeys[3][3];
    char dataBlob[4];
    KeyCount numKeys;
    // the keyOffset values have to be on the heap. Can't just create temporary
    // variables in the constructor because appending to a buffer does not
    // allocate memory and these variables will go out of of scope after the
    // constructor completes execution
    KeyLength endKeyOffsets[3];
    KeyInfo keyList[3];

    Buffer buffer;
    Buffer buffer2;
    Buffer buffer3;
    Buffer buffer4;

    Tub<Object> objectDataFromBuffer;
    Tub<Object> objectFromBuffer;
    Tub<Object> objectFromContiguousVoidPointer;
    Tub<Object> objectFromVoidPointer;
    Tub<Object> objectForWriteRpc;

    Object* objects[3];
    // Handle this object separately as it only has one key.
    // This kind of object is primarily used by unit
    // tests
    Object* singleKeyObject;

    // This is to test the constructor that is used by the write RPC
    Object* writeRpcObject;

    DISALLOW_COPY_AND_ASSIGN(ObjectTest);
};

TEST_F(ObjectTest, constructor_dataFromBuffer)
{
    Object& object = *objectDataFromBuffer;

    EXPECT_EQ(57U, object.header.tableId);
    EXPECT_EQ(75U, object.header.version);
    EXPECT_EQ(723U, object.header.timestamp);
    EXPECT_EQ(0x85F0E63B, object.header.checksum);

    const uint8_t *data = reinterpret_cast<const uint8_t *>(
                            object.getKeysAndValue());

    KeyCount numKeys = *data;
    EXPECT_EQ(3U, numKeys);

    // skip ahead past numKeys
    data = data + sizeof(KeyCount);

    const KeyLength keyOffsetOne = *(reinterpret_cast<const KeyLength *>(
                                            data));
    const KeyLength keyOffsetTwo = *(reinterpret_cast<const KeyLength *>(
                                            data + 2));
    const KeyLength keyOffsetThree = *(reinterpret_cast<const KeyLength *>(
                                            data + 4));

    EXPECT_EQ(2, keyOffsetOne);
    EXPECT_EQ(5, keyOffsetTwo);
    EXPECT_EQ(8, keyOffsetThree);

    EXPECT_EQ(0, memcmp("ha", data + 3 * sizeof(KeyLength), 3));
    EXPECT_EQ(0, memcmp("hi", data + 3 * sizeof(KeyLength) + 3, 3));
    EXPECT_EQ(0, memcmp("ho", data + 3 * sizeof(KeyLength) + 6, 3));

    EXPECT_EQ(20U, object.keysAndValueLength);
    EXPECT_FALSE(object.data);
    EXPECT_TRUE(object.dataBuffer);

    // offset into what getKeysAndValue() returns, to point to the actual data
    // blob. Skip the key length values and the key values
    // numKeys = 3, total length of all 3 keys is 9
    EXPECT_EQ(0, memcmp("YO!", data + 3 * sizeof(KeyLength) + 9, 4));
}

TEST_F(ObjectTest, constructor_entirelyFromBuffer)
{
    Object& object = *objectFromBuffer;

    EXPECT_EQ(57U, object.header.tableId);
    EXPECT_EQ(75U, object.header.version);
    EXPECT_EQ(723U, object.header.timestamp);
    EXPECT_EQ(0x85F0E63B, object.header.checksum);

    const uint8_t *data = reinterpret_cast<const uint8_t *>(
                            object.getKeysAndValue());

    const KeyCount numKeys = *data;
    EXPECT_EQ(3U, numKeys);

    data = data + sizeof(KeyCount);

    const KeyLength keyOffsetOne = *(reinterpret_cast<const KeyLength *>(
                                            data));
    const KeyLength keyOffsetTwo = *(reinterpret_cast<const KeyLength *>(
                                            data + 2));
    const KeyLength keyOffsetThree = *(reinterpret_cast<const KeyLength *>(
                                            data + 4));

    EXPECT_EQ(2, keyOffsetOne);
    EXPECT_EQ(5, keyOffsetTwo);
    EXPECT_EQ(8, keyOffsetThree);

    EXPECT_EQ(0, memcmp("ha", data + 3 * sizeof(KeyLength), 3));
    EXPECT_EQ(0, memcmp("hi", data + 3 * sizeof(KeyLength) + 3, 3));
    EXPECT_EQ(0, memcmp("ho", data + 3 * sizeof(KeyLength) + 6, 3));

    EXPECT_EQ(20U, object.keysAndValueLength);
    EXPECT_FALSE(object.data);
    EXPECT_TRUE(object.dataBuffer);

    EXPECT_EQ(0, memcmp("YO!", data + 3 * sizeof(KeyLength) + 9, 4));
}

TEST_F(ObjectTest, constructor_fromContiguousVoidPointer)
{
    Object& object = *objectFromContiguousVoidPointer;

    EXPECT_EQ(57U, object.header.tableId);
    EXPECT_EQ(75U, object.header.version);
    EXPECT_EQ(723U, object.header.timestamp);
    EXPECT_EQ(0x85F0E63B, object.header.checksum);

    const uint8_t *data = reinterpret_cast<const uint8_t *>(
                            object.getKeysAndValue());

    const KeyCount numKeys = *data;
    EXPECT_EQ(3U, numKeys);

    data = data + sizeof(KeyCount);

    const KeyLength keyOffsetOne = *(reinterpret_cast<const KeyLength *>(
                                            data));
    const KeyLength keyOffsetTwo = *(reinterpret_cast<const KeyLength *>(
                                            data + 2));
    const KeyLength keyOffsetThree = *(reinterpret_cast<const KeyLength *>(
                                            data + 4));

    EXPECT_EQ(2, keyOffsetOne);
    EXPECT_EQ(5, keyOffsetTwo);
    EXPECT_EQ(8, keyOffsetThree);

    EXPECT_EQ(0, memcmp("ha", data + 3 * sizeof(KeyLength), 3));
    EXPECT_EQ(0, memcmp("hi", data + 3 * sizeof(KeyLength) + 3, 3));
    EXPECT_EQ(0, memcmp("ho", data + 3 * sizeof(KeyLength) + 6, 3));

    EXPECT_EQ(20U, object.keysAndValueLength);
    EXPECT_TRUE(object.data);
    EXPECT_FALSE(object.dataBuffer);

    EXPECT_EQ(0, memcmp("YO!", data + 3 * sizeof(KeyLength) + 9, 4));
}

TEST_F(ObjectTest, constructor_dataFromVoidPointer)
{
    Object& object = *singleKeyObject;

    EXPECT_EQ(57U, object.header.tableId);
    EXPECT_EQ(75U, object.header.version);
    EXPECT_EQ(723U, object.header.timestamp);

    // call assembleForLog to update the checksum
    Buffer buffer;
    object.assembleForLog(buffer);
    EXPECT_EQ(0x1C5C4799U, object.header.checksum);

    const uint8_t *data = reinterpret_cast<const uint8_t *>(
                            object.getKeysAndValue());

    KeyCount numKeys = *data;
    EXPECT_EQ(1U, numKeys);

    // skip ahead past numKeys
    data = data + sizeof(KeyCount);

    const KeyLength keyOffsetOne = *(reinterpret_cast<const KeyLength *>(
                                            data));

    EXPECT_EQ(2, keyOffsetOne);

    EXPECT_EQ(0, memcmp("ha", data + sizeof(KeyLength), 3));

    EXPECT_EQ(10U, object.keysAndValueLength);
    EXPECT_FALSE(object.data);
    EXPECT_TRUE(object.dataBuffer);

    // offset into what getKeysAndValue() returns, to point to the actual data
    // blob. Skip the key length value and the key value
    // numKeys = 1
    EXPECT_EQ(0, memcmp("YO!", data + sizeof(KeyLength) + 3, 4));
}

TEST_F(ObjectTest, constructor_fromWriteRpc)
{
    Object& object = *writeRpcObject;

    EXPECT_EQ(57U, object.header.tableId);
    EXPECT_EQ(75U, object.header.version);
    EXPECT_EQ(723U, object.header.timestamp);
    // call assembleForLog to update the checksum
    Buffer tempBuffer;
    object.assembleForLog(tempBuffer);
    EXPECT_EQ(0x85F0E63B, object.header.checksum);

    const uint8_t *data = reinterpret_cast<const uint8_t *>(
                            object.getKeysAndValue());

    const KeyCount numKeys = *data;
    EXPECT_EQ(3U, numKeys);

    data = data + sizeof(KeyCount);

    const KeyLength keyOffsetOne = *(reinterpret_cast<const KeyLength *>(
                                            data));
    const KeyLength keyOffsetTwo = *(reinterpret_cast<const KeyLength *>(
                                            data + 2));
    const KeyLength keyOffsetThree = *(reinterpret_cast<const KeyLength *>(
                                            data + 4));

    EXPECT_EQ(2, keyOffsetOne);
    EXPECT_EQ(5, keyOffsetTwo);
    EXPECT_EQ(8, keyOffsetThree);

    EXPECT_EQ(0, memcmp("ha", data + 3 * sizeof(KeyLength), 3));
    EXPECT_EQ(0, memcmp("hi", data + 3 * sizeof(KeyLength) + 3, 3));
    EXPECT_EQ(0, memcmp("ho", data + 3 * sizeof(KeyLength) + 6, 3));

    EXPECT_EQ(20U, object.keysAndValueLength);
    EXPECT_FALSE(object.data);
    EXPECT_TRUE(object.dataBuffer);

    EXPECT_EQ(0, memcmp("YO!", data + 3 * sizeof(KeyLength) + 9, 4));
}

TEST_F(ObjectTest, assembleForLog) {
    for (uint32_t i = 0; i < arrayLength(objects); i++) {
        Object& object = *objects[i];
        Buffer buffer;
        object.assembleForLog(buffer);
        const Object::Header* header =
            buffer.getStart<Object::Header>();

        EXPECT_EQ(sizeof(*header) + sizeof(KeyCount) + 3 * sizeof(KeyLength)
                    + 9 + 4, buffer.getTotalLength());

        EXPECT_EQ(57U, header->tableId);
        EXPECT_EQ(75U, header->version);
        EXPECT_EQ(723U, header->timestamp);
        EXPECT_EQ(0x85F0E63B, object.header.checksum);

        const KeyCount numKeys= *buffer.getOffset<KeyCount>(sizeof(*header));
        EXPECT_EQ(3U, numKeys);

        const KeyLength keyOffsetOne = *buffer.getOffset<KeyLength>(
                                        sizeof(*header) + sizeof(KeyCount));
        const KeyLength keyOffsetTwo = *buffer.getOffset<KeyLength>(
                                        sizeof(*header) + sizeof(KeyCount) +
                                        sizeof(KeyLength));
        const KeyLength keyOffsetThree = *buffer.getOffset<KeyLength>
                                         (sizeof(*header) + sizeof(KeyCount) +
                                          2 * sizeof(KeyLength));

        EXPECT_EQ(2U, keyOffsetOne);
        EXPECT_EQ(5U, keyOffsetTwo);
        EXPECT_EQ(8U, keyOffsetThree);

        EXPECT_EQ(0, memcmp("ha", buffer.getRange(sizeof(*header) + 7, 3), 3));
        EXPECT_EQ(0, memcmp("hi", buffer.getRange(sizeof(*header) + 10, 3), 3));
        EXPECT_EQ(0, memcmp("ho", buffer.getRange(sizeof(*header) + 13, 3), 3));

        const void* data = buffer.getRange(sizeof(*header) + sizeof(KeyCount)
                             + 3 * sizeof(KeyLength) + 9, 4);
        EXPECT_EQ(0, memcmp(data, "YO!", 4));
    }

    // test this function for the single key object separately
    Buffer buffer;
    Object& object = *singleKeyObject;
    object.assembleForLog(buffer);
    const Object::Header* header =
            buffer.getStart<Object::Header>();

    EXPECT_EQ(sizeof(*header) + sizeof(KeyCount) + sizeof(KeyLength)
                    + 3 + 4, buffer.getTotalLength());

    EXPECT_EQ(57U, header->tableId);
    EXPECT_EQ(75U, header->version);
    EXPECT_EQ(723U, header->timestamp);
    EXPECT_EQ(0x1C5C4799U, object.header.checksum);

    const KeyCount numKeys= *buffer.getOffset<KeyCount>(sizeof(*header));
    EXPECT_EQ(1U, numKeys);

    const KeyLength keyOffsetOne = *buffer.getOffset<KeyLength>(
                                    sizeof(*header) + sizeof(KeyCount));

    EXPECT_EQ(2U, keyOffsetOne);
    EXPECT_EQ(0, memcmp("ha", buffer.getRange(sizeof(*header) + 3, 3), 3));

    const void* data = buffer.getRange(sizeof(*header) + sizeof(KeyCount)
                             + sizeof(KeyLength) + 3, 4);
    EXPECT_EQ(0, memcmp(data, "YO!", 4));
}

TEST_F(ObjectTest, appendKeysAndValueToBuffer) {
    for (uint32_t i = 0; i < arrayLength(objects); i++) {
        Object& object = *objects[i];
        Buffer buffer;
        object.appendKeysAndValueToBuffer(buffer);
        EXPECT_EQ(20U, buffer.getTotalLength());

        const KeyCount numKeys= *buffer.getOffset<KeyCount>(0);
        EXPECT_EQ(3U, numKeys);

        const KeyLength keyOffsetOne = *buffer.getOffset<KeyLength>(
                                        sizeof(KeyCount));
        const KeyLength keyOffsetTwo = *buffer.getOffset<KeyLength>(
                                        sizeof(KeyCount) + sizeof(KeyLength));
        const KeyLength keyOffsetThree = *buffer.getOffset<KeyLength>(
                                          sizeof(KeyCount) +
                                          2 * sizeof(KeyLength));

        EXPECT_EQ(2, keyOffsetOne);
        EXPECT_EQ(5, keyOffsetTwo);
        EXPECT_EQ(8, keyOffsetThree);

        EXPECT_EQ(0, memcmp("ha", buffer.getRange(7, 3), 3));
        EXPECT_EQ(0, memcmp("hi", buffer.getRange(10, 3), 3));
        EXPECT_EQ(0, memcmp("ho", buffer.getRange(13, 3), 3));
        EXPECT_EQ(0, memcmp("YO!", buffer.getRange(16, 4), 4));
    }
}

TEST_F(ObjectTest, getTableId) {
    for (uint32_t i = 0; i < arrayLength(objects); i++)
        EXPECT_EQ(57U, objects[i]->getTableId());
}

TEST_F(ObjectTest, getKey) {
    for (uint32_t i = 0; i < arrayLength(objects); i++) {
        KeyLength keyLen;
        EXPECT_EQ(0, memcmp("ha", objects[i]->getKey(), 3));
        EXPECT_EQ(0, memcmp("ha", objects[i]->getKey(0, &keyLen), 3));
        EXPECT_EQ(3U, keyLen);
        EXPECT_EQ(0, memcmp("hi", objects[i]->getKey(1, &keyLen), 3));
        EXPECT_EQ(3U, keyLen);
        EXPECT_EQ(0, memcmp("ho", objects[i]->getKey(2, &keyLen), 3));
        EXPECT_EQ(3U, keyLen);
        // invalid key index
        EXPECT_STREQ(NULL, (const char *)objects[i]->getKey(3));
    }
}

TEST_F(ObjectTest, getKeyLength) {
    for (uint32_t i = 0; i < arrayLength(objects); i++) {
        EXPECT_EQ(3U, objects[i]->getKeyLength());
        EXPECT_EQ(3U, objects[i]->getKeyLength(0));
        EXPECT_EQ(3U, objects[i]->getKeyLength(1));
        EXPECT_EQ(3U, objects[i]->getKeyLength(2));
        // invalid key index
        EXPECT_EQ(0U, objects[i]->getKeyLength(3));
    }
}

TEST_F(ObjectTest, getEndKeyOffset) {
    for (uint32_t i = 0; i < arrayLength(objects); i++) {
        EXPECT_EQ(2U, objects[i]->getEndKeyOffset());
        EXPECT_EQ(2U, objects[i]->getEndKeyOffset(0));
        EXPECT_EQ(5U, objects[i]->getEndKeyOffset(1));
        EXPECT_EQ(8U, objects[i]->getEndKeyOffset(2));
        // invalid key index
        EXPECT_EQ(0U, objects[i]->getEndKeyOffset(3));
    }
}

TEST_F(ObjectTest, getKeysAndValue) {
    for (uint32_t i = 0; i < arrayLength(objects); i++) {
        const uint8_t *data = reinterpret_cast<const uint8_t *>(
                                    objects[i]->getKeysAndValue());
        // offset into what getKeysAndValue() returns, to point to the actual
        // data blob. Skip the key length values and the key values
        // numKeys = 3, total length of all 3 keys is 9

        const KeyCount numKeys = *(reinterpret_cast<const KeyCount *>(
                                            data));
        EXPECT_EQ(3U, numKeys);

        data = data + sizeof(KeyCount);

        const KeyLength keyOffsetOne = *(reinterpret_cast<const KeyLength *>(
                                            data));
        const KeyLength keyOffsetTwo = *(reinterpret_cast<const KeyLength *>(
                                            data + sizeof(KeyLength)));
        const KeyLength keyOffsetThree = *(reinterpret_cast<const KeyLength *>(
                                            data + 2*sizeof(KeyLength)));

        EXPECT_EQ(2, keyOffsetOne);
        EXPECT_EQ(5, keyOffsetTwo);
        EXPECT_EQ(8, keyOffsetThree);

        EXPECT_EQ(0, memcmp("ha", data + 3 * sizeof(KeyLength), 3));
        EXPECT_EQ(0, memcmp("hi", data + 3 * sizeof(KeyLength) + 3, 3));
        EXPECT_EQ(0, memcmp("ho", data + 3 * sizeof(KeyLength) + 6, 3));

        EXPECT_EQ(0, memcmp("YO!", data + 3 * sizeof(KeyLength) + 9, 4));
    }
}

TEST_F(ObjectTest, getValueContiguous) {
    uint32_t len;
    EXPECT_EQ(0, memcmp("YO!", objects[0]->getValue(&len), 4));
    EXPECT_EQ(4U, len);
    EXPECT_EQ(0, memcmp("YO!", objects[1]->getValue(&len), 4));
    EXPECT_EQ(4U, len);
    EXPECT_EQ(0, memcmp("YO!", objects[2]->getValue(&len), 4));
    EXPECT_EQ(4U, len);
}

TEST_F(ObjectTest, getValueOffset) {
    EXPECT_EQ(16U, objects[0]->getValueOffset());
    EXPECT_EQ(16U, objects[1]->getValueOffset());
    EXPECT_EQ(16U, objects[2]->getValueOffset());
}

TEST_F(ObjectTest, getKeysAndValueLength) {
    for (uint32_t i = 0; i < arrayLength(objects); i++)
        EXPECT_EQ(20U, objects[i]->getKeysAndValueLength());
}

TEST_F(ObjectTest, getVersion) {
    for (uint32_t i = 0; i < arrayLength(objects); i++)
        EXPECT_EQ(75U, objects[i]->getVersion());
}

TEST_F(ObjectTest, getTimestamp) {
    for (uint32_t i = 0; i < arrayLength(objects); i++)
        EXPECT_EQ(723U, objects[i]->getTimestamp());
}

TEST_F(ObjectTest, checkIntegrity) {
    for (uint32_t i = 0; i < arrayLength(objects); i++) {
        Object& object = *objects[i];
        Buffer buffer;
        object.assembleForLog(buffer);
        EXPECT_TRUE(object.checkIntegrity());

        // screw up the first byte (in the Header)
        uint8_t* evil = reinterpret_cast<uint8_t*>(
            const_cast<void*>(buffer.getRange(0, 1)));
        uint8_t tmp = *evil;
        *evil = static_cast<uint8_t>(~*evil);
        EXPECT_FALSE(object.checkIntegrity());
        *evil = tmp;

        // screw up the first byte - the number of keys
        EXPECT_TRUE(object.checkIntegrity());
        evil = reinterpret_cast<uint8_t*>(
            const_cast<void*>(buffer.getRange(
                sizeof(object.header), 1)));
        tmp = *evil;
        *evil = static_cast<uint8_t>(~*evil);
        EXPECT_FALSE(object.checkIntegrity());
        *evil = tmp;

        // screw up the first byte in the first key
        EXPECT_TRUE(object.checkIntegrity());
        evil = reinterpret_cast<uint8_t*>(
            const_cast<void*>(buffer.getRange(
                sizeof(object.header) + 7, 1)));
        tmp = *evil;
        *evil = static_cast<uint8_t>(~*evil);
        EXPECT_FALSE(object.checkIntegrity());
        *evil = tmp;

        // screw up the last byte (in the data blob)
        EXPECT_TRUE(object.checkIntegrity());
        evil = reinterpret_cast<uint8_t*>(
            const_cast<void*>(buffer.getRange(buffer.getTotalLength() - 1, 1)));
        tmp = *evil;
        *evil = static_cast<uint8_t>(~*evil);
        EXPECT_FALSE(object.checkIntegrity());
        *evil = tmp;

        EXPECT_TRUE(object.checkIntegrity());
    }
}

/**
 * Unit tests for ObjectTombstone.
 */
class ObjectTombstoneTest : public ::testing::Test {
  public:
    ObjectTombstoneTest()
        : stringKey(),
          dataBlob(),
          buffer(),
          numKeys(1),
          endKeyOffset(),
          object(),
          tombstoneFromObject(),
          tombstoneFromBuffer()
    {
        snprintf(stringKey, sizeof(stringKey), "key!");
        snprintf(dataBlob, sizeof(dataBlob), "data!");
        endKeyOffset = 4;

        Key key(572, stringKey, 5);

        buffer.append(&numKeys, sizeof(numKeys));
        buffer.append(&endKeyOffset, sizeof(endKeyOffset));
        buffer.append(stringKey, endKeyOffset + 1);
        buffer.append(dataBlob, 6);

        object.construct(key.getTableId(), 58, 723, buffer);
        tombstoneFromObject.construct(*object, 925, 335);

        buffer.reset();
        tombstoneFromObject->assembleForLog(buffer);
        tombstoneFromBuffer.construct(buffer);

        tombstones[0] = &*tombstoneFromObject;
        tombstones[1] = &*tombstoneFromBuffer;
    }

    ~ObjectTombstoneTest()
    {
    }

    // Don't use static strings, since they'll be loaded into read-only
    // memory and we can't mutate to test checksumming.
    char stringKey[5];
    char dataBlob[6];

    Buffer buffer;
    KeyCount numKeys;
    KeyLength endKeyOffset;

    Tub<Object> object;
    Tub<ObjectTombstone> tombstoneFromObject;
    Tub<ObjectTombstone> tombstoneFromBuffer;

    ObjectTombstone* tombstones[2];

    DISALLOW_COPY_AND_ASSIGN(ObjectTombstoneTest);
};

TEST_F(ObjectTombstoneTest, constructor_fromObject) {
    ObjectTombstone& tombstone = *tombstones[0];

    EXPECT_EQ(572U, tombstone.header.tableId);
    EXPECT_EQ(5U, tombstone.header.keyLength);
    EXPECT_EQ(925U, tombstone.header.segmentId);
    EXPECT_EQ(58U, tombstone.header.objectVersion);
    EXPECT_EQ(335U, tombstone.header.timestamp);
    EXPECT_EQ(0x837de743U, tombstone.header.checksum);

    EXPECT_TRUE(tombstone.key);
    EXPECT_FALSE(tombstone.tombstoneBuffer);
    EXPECT_EQ(0, memcmp("key!", tombstone.key, 5));
}

TEST_F(ObjectTombstoneTest, constructor_fromBuffer) {
    ObjectTombstone& tombstone = *tombstones[1];

    EXPECT_EQ(572U, tombstone.header.tableId);
    EXPECT_EQ(5U, tombstone.header.keyLength);
    EXPECT_EQ(925U, tombstone.header.segmentId);
    EXPECT_EQ(58U, tombstone.header.objectVersion);
    EXPECT_EQ(335U, tombstone.header.timestamp);
    EXPECT_EQ(0x837de743U, tombstone.header.checksum);

    EXPECT_FALSE(tombstone.key);
    EXPECT_TRUE(tombstone.tombstoneBuffer);
    EXPECT_EQ(sizeof(ObjectTombstone::Header) + 5,
        (tombstone.tombstoneBuffer)->getTotalLength());
    EXPECT_EQ(0, memcmp("key!",
        (tombstone.tombstoneBuffer)->getRange(sizeof(
            ObjectTombstone::Header), 5), 5));
}

TEST_F(ObjectTombstoneTest, assembleForLog) {
    for (uint32_t i = 0; i < arrayLength(tombstones); i++) {
        ObjectTombstone& tombstone = *tombstones[i];
        Buffer buffer;
        tombstone.assembleForLog(buffer);
        const ObjectTombstone::Header* header =
            buffer.getStart<ObjectTombstone::Header>();

        EXPECT_EQ(sizeof(*header) + 5, buffer.getTotalLength());

        EXPECT_EQ(572U, header->tableId);
        EXPECT_EQ(5U, header->keyLength);
        EXPECT_EQ(925U, header->segmentId);
        EXPECT_EQ(58U, header->objectVersion);
        EXPECT_EQ(335U, header->timestamp);
        EXPECT_EQ(0x837de743U, header->checksum);

        const void* key = buffer.getRange(sizeof(*header), 5);
        EXPECT_EQ(0, memcmp(key, "key!", 5));
    }
}

TEST_F(ObjectTombstoneTest, appendKeyToBuffer) {
    for (uint32_t i = 0; i < arrayLength(tombstones); i++) {
        ObjectTombstone& tombstone = *tombstones[i];
        Buffer buffer;
        tombstone.appendKeyToBuffer(buffer);
        EXPECT_EQ(5U, buffer.getTotalLength());
        EXPECT_EQ(0, memcmp("key!", buffer.getRange(0, 5), 5));
    }
}

TEST_F(ObjectTombstoneTest, getTableId) {
    for (uint32_t i = 0; i < arrayLength(tombstones); i++)
        EXPECT_EQ(572U, tombstones[i]->getTableId());
}

TEST_F(ObjectTombstoneTest, getKey) {
    for (uint32_t i = 0; i < arrayLength(tombstones); i++)
        EXPECT_EQ(0, memcmp("key!", tombstones[i]->getKey(), 5));
}

TEST_F(ObjectTombstoneTest, getKeyLength) {
    for (uint32_t i = 0; i < arrayLength(tombstones); i++)
        EXPECT_EQ(5U, tombstones[i]->getKeyLength());
}

TEST_F(ObjectTombstoneTest, getSegmentId) {
    for (uint32_t i = 0; i < arrayLength(tombstones); i++)
        EXPECT_EQ(925U, tombstones[i]->getSegmentId());
}

TEST_F(ObjectTombstoneTest, getObjectVersion) {
    for (uint32_t i = 0; i < arrayLength(tombstones); i++)
        EXPECT_EQ(58U, tombstones[i]->getObjectVersion());
}

TEST_F(ObjectTombstoneTest, getTimestamp) {
    for (uint32_t i = 0; i < arrayLength(tombstones); i++)
        EXPECT_EQ(335U, tombstones[i]->getTimestamp());
}

TEST_F(ObjectTombstoneTest, checkIntegrity) {
    for (uint32_t i = 0; i < arrayLength(tombstones); i++) {
        ObjectTombstone& tombstone = *tombstones[i];
        Buffer buffer;
        tombstone.assembleForLog(buffer);
        EXPECT_TRUE(tombstone.checkIntegrity());

        uint8_t* evil = reinterpret_cast<uint8_t*>(
            const_cast<void*>(buffer.getRange(0, 1)));
        uint8_t tmp = *evil;
        *evil = static_cast<uint8_t>(~*evil);
        EXPECT_FALSE(tombstone.checkIntegrity());
        *evil = tmp;

        EXPECT_TRUE(tombstone.checkIntegrity());
        evil = reinterpret_cast<uint8_t*>(
            const_cast<void*>(buffer.getRange(buffer.getTotalLength() - 1, 1)));
        tmp = *evil;
        *evil = static_cast<uint8_t>(~*evil);
        EXPECT_FALSE(tombstone.checkIntegrity());
        *evil = tmp;

        EXPECT_TRUE(tombstone.checkIntegrity());
    }
}

TEST_F(ObjectTombstoneTest, getSerializedLength) {
    EXPECT_EQ(sizeof(ObjectTombstone::Header),
              ObjectTombstone::getSerializedLength(0));
    EXPECT_EQ(sizeof(ObjectTombstone::Header) + 7,
              ObjectTombstone::getSerializedLength(7));
}

} // namespace RAMCloud
