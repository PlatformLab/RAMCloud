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
        : stringKey(),
          dataBlob(),
          _buffer(),
          _buffer2(),
          objectFromVoidPointer(),
          objectDataFromBuffer(),
          objectFromBuffer(),
          objectFromContiguousVoidPointer(),
          objects()
    {
        snprintf(stringKey, sizeof(stringKey), "hi");
        snprintf(dataBlob, sizeof(dataBlob), "YO!");

        Key key(57, stringKey, sizeof(stringKey));

        objectFromVoidPointer.construct(key, dataBlob,
                                        sizeof32(dataBlob), 75, 723);

        _buffer.append(&dataBlob[0], 2);
        _buffer.append(&dataBlob[2], 2);
        objectDataFromBuffer.construct(key, _buffer, 75, 723);

        objectFromVoidPointer->serializeToBuffer(_buffer2);
        objectFromBuffer.construct(_buffer2);

        objectFromContiguousVoidPointer.construct(
            _buffer2.getRange(0, _buffer2.getTotalLength()),
            _buffer2.getTotalLength());

        objects[0] = &*objectFromVoidPointer;
        objects[1] = &*objectDataFromBuffer;
        objects[2] = &*objectFromBuffer;
        objects[3] = &*objectFromContiguousVoidPointer;
    }

    ~ObjectTest()
    {
    }

    // Don't use static strings, since they'll be loaded into read-only
    // memory and we can't mutate to test checksumming.
    char stringKey[3];
    char dataBlob[4];

    Buffer _buffer;
    Buffer _buffer2;

    Tub<Object> objectFromVoidPointer;
    Tub<Object> objectDataFromBuffer;
    Tub<Object> objectFromBuffer;
    Tub<Object> objectFromContiguousVoidPointer;

    Object* objects[4];

    DISALLOW_COPY_AND_ASSIGN(ObjectTest);
};

TEST_F(ObjectTest, constructor_fromVoidPointer)
{
    Object& object = *objectFromVoidPointer;

    EXPECT_EQ(57U, object.serializedForm.tableId);
    EXPECT_EQ(3U, object.serializedForm.keyLength);
    EXPECT_EQ(75U, object.serializedForm.version);
    EXPECT_EQ(723U, object.serializedForm.timestamp);
    EXPECT_EQ(0x4b96e93eU, object.serializedForm.checksum);

    EXPECT_EQ(0, memcmp("hi", object.key, 3));
    EXPECT_EQ(4U, object.dataLength);
    EXPECT_TRUE(object.data);
    EXPECT_FALSE(object.dataBuffer);
    EXPECT_FALSE(object.objectBuffer);
    EXPECT_EQ(0, memcmp("YO!", *object.data, 4));
}

TEST_F(ObjectTest, constructor_dataFromBuffer)
{
    Object& object = *objectDataFromBuffer;

    EXPECT_EQ(57U, object.serializedForm.tableId);
    EXPECT_EQ(3U, object.serializedForm.keyLength);
    EXPECT_EQ(75U, object.serializedForm.version);
    EXPECT_EQ(723U, object.serializedForm.timestamp);
    EXPECT_EQ(0x4b96e93eU, object.serializedForm.checksum);

    EXPECT_EQ(0, memcmp("hi", object.key, 3));
    EXPECT_EQ(4U, object.dataLength);
    EXPECT_FALSE(object.data);
    EXPECT_TRUE(object.dataBuffer);
    EXPECT_FALSE(object.objectBuffer);
    EXPECT_EQ(0, memcmp("YO!", object.getData(), 4));
}

TEST_F(ObjectTest, constructor_entirelyFromBuffer)
{
    Object& object = *objectFromBuffer;

    EXPECT_EQ(57U, object.serializedForm.tableId);
    EXPECT_EQ(3U, object.serializedForm.keyLength);
    EXPECT_EQ(75U, object.serializedForm.version);
    EXPECT_EQ(723U, object.serializedForm.timestamp);
    EXPECT_EQ(0x4b96e93eU, object.serializedForm.checksum);

    EXPECT_EQ(static_cast<const void*>(NULL), object.key);
    EXPECT_EQ(0, memcmp("hi", object.getKey(), 3));
    EXPECT_EQ(4U, object.dataLength);
    EXPECT_FALSE(object.data);
    EXPECT_FALSE(object.dataBuffer);
    EXPECT_TRUE(object.objectBuffer);
    EXPECT_EQ(0, memcmp("YO!", object.getData(), 4));
}

TEST_F(ObjectTest, constructor_fromContiguousVoidPointer)
{
    Object& object = *objectFromContiguousVoidPointer;

    EXPECT_EQ(57U, object.serializedForm.tableId);
    EXPECT_EQ(3U, object.serializedForm.keyLength);
    EXPECT_EQ(75U, object.serializedForm.version);
    EXPECT_EQ(723U, object.serializedForm.timestamp);
    EXPECT_EQ(0x4b96e93eU, object.serializedForm.checksum);

    EXPECT_EQ(0, memcmp("hi", object.key, 3));
    EXPECT_EQ(4U, object.dataLength);
    EXPECT_TRUE(object.data);
    EXPECT_FALSE(object.dataBuffer);
    EXPECT_FALSE(object.objectBuffer);
    EXPECT_EQ(0, memcmp("YO!", object.getData(), 4));
}

TEST_F(ObjectTest, serializeToBuffer) {
    for (uint32_t i = 0; i < arrayLength(objects); i++) {
        Object& object = *objects[i];
        Buffer buffer;
        object.serializeToBuffer(buffer);
        const Object::SerializedForm* header =
            buffer.getStart<Object::SerializedForm>();

        EXPECT_EQ(sizeof(*header) + 3 + 4, buffer.getTotalLength());

        EXPECT_EQ(57U, header->tableId);
        EXPECT_EQ(3U, header->keyLength);
        EXPECT_EQ(75U, header->version);
        EXPECT_EQ(723U, header->timestamp);
        EXPECT_EQ(0x4b96e93eU, header->checksum);

        const void* key = buffer.getRange(sizeof(*header), 3);
        EXPECT_EQ(0, memcmp(key, "hi", 3));

        const void* data = buffer.getRange(sizeof(*header) + 3, 4);
        EXPECT_EQ(0, memcmp(data, "YO!", 4));
    }
}

TEST_F(ObjectTest, appendKeyToBuffer) {
    for (uint32_t i = 0; i < arrayLength(objects); i++) {
        Object& object = *objects[i];
        Buffer buffer;
        object.appendKeyToBuffer(buffer);
        EXPECT_EQ(3U, buffer.getTotalLength());
        EXPECT_EQ(0, memcmp("hi", buffer.getRange(0, 3), 3));
    }
}

TEST_F(ObjectTest, appendDataToBuffer) {
    for (uint32_t i = 0; i < arrayLength(objects); i++) {
        Object& object = *objects[i];
        Buffer buffer;
        object.appendDataToBuffer(buffer);
        EXPECT_EQ(4U, buffer.getTotalLength());
        EXPECT_EQ(0, memcmp("YO!", buffer.getRange(0, 4), 4));
    }
}

TEST_F(ObjectTest, getTableId) {
    for (uint32_t i = 0; i < arrayLength(objects); i++)
        EXPECT_EQ(57U, objects[i]->getTableId());
}

TEST_F(ObjectTest, getKey) {
    for (uint32_t i = 0; i < arrayLength(objects); i++)
        EXPECT_EQ(0, memcmp("hi", objects[i]->getKey(), 3));
}

TEST_F(ObjectTest, getKeyLength) {
    for (uint32_t i = 0; i < arrayLength(objects); i++)
        EXPECT_EQ(3U, objects[i]->getKeyLength());
}

TEST_F(ObjectTest, getData) {
    for (uint32_t i = 0; i < arrayLength(objects); i++)
        EXPECT_EQ(0, memcmp("YO!", objects[i]->getData(), 4));
}

TEST_F(ObjectTest, getDataLength) {
    for (uint32_t i = 0; i < arrayLength(objects); i++)
        EXPECT_EQ(4U, objects[i]->getDataLength());
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
        object.serializeToBuffer(buffer);
        EXPECT_TRUE(object.checkIntegrity());

        // screw up the first byte (in the SerializedForm)
        uint8_t* evil = reinterpret_cast<uint8_t*>(
            const_cast<void*>(buffer.getRange(0, 1)));
        uint8_t tmp = *evil;
        *evil = static_cast<uint8_t>(~*evil);
        EXPECT_FALSE(object.checkIntegrity());
        *evil = tmp;

        // screw up a middle byte (in the string key)
        EXPECT_TRUE(object.checkIntegrity());
        evil = reinterpret_cast<uint8_t*>(
            const_cast<void*>(buffer.getRange(
                sizeof(object.serializedForm), 1)));
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

TEST_F(ObjectTest, getSerializedLength) {
    EXPECT_EQ(sizeof(Object::SerializedForm),
              Object::getSerializedLength(0, 0));
    EXPECT_EQ(sizeof(Object::SerializedForm) + 3,
              Object::getSerializedLength(1, 2));
    EXPECT_EQ(sizeof(Object::SerializedForm) + 7,
              Object::getSerializedLength(5, 2));
}

/**
 * Unit tests for ObjectTombstone.
 */
class ObjectTombstoneTest : public ::testing::Test {
  public:
    ObjectTombstoneTest()
        : stringKey(),
          dataBlob(),
          _buffer(),
          object(),
          tombstoneFromObject(),
          tombstoneFromBuffer()
    {
        snprintf(stringKey, sizeof(stringKey), "key!");
        snprintf(dataBlob, sizeof(dataBlob), "data!");

        Key key(572, stringKey, 5);
        object.construct(key, dataBlob, 6, 58, 723);
        tombstoneFromObject.construct(*object, 925, 335);

        tombstoneFromObject->serializeToBuffer(_buffer);
        tombstoneFromBuffer.construct(_buffer);

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

    Buffer _buffer;

    Tub<Object> object;
    Tub<ObjectTombstone> tombstoneFromObject;
    Tub<ObjectTombstone> tombstoneFromBuffer;

    ObjectTombstone* tombstones[2];

    DISALLOW_COPY_AND_ASSIGN(ObjectTombstoneTest);
};

TEST_F(ObjectTombstoneTest, constructor_fromObject) {
    ObjectTombstone& tombstone = *tombstones[0];

    EXPECT_EQ(572U, tombstone.serializedForm.tableId);
    EXPECT_EQ(925U, tombstone.serializedForm.segmentId);
    EXPECT_EQ(58U, tombstone.serializedForm.objectVersion);
    EXPECT_EQ(335U, tombstone.serializedForm.timestamp);
    EXPECT_EQ(0x5d60e8efU, tombstone.serializedForm.checksum);

    EXPECT_TRUE(tombstone.key);
    EXPECT_FALSE(tombstone.tombstoneBuffer);
    EXPECT_EQ(0, memcmp("key!", *tombstone.key, 5));
}

TEST_F(ObjectTombstoneTest, constructor_fromBuffer) {
    ObjectTombstone& tombstone = *tombstones[1];

    EXPECT_EQ(572U, tombstone.serializedForm.tableId);
    EXPECT_EQ(925U, tombstone.serializedForm.segmentId);
    EXPECT_EQ(58U, tombstone.serializedForm.objectVersion);
    EXPECT_EQ(335U, tombstone.serializedForm.timestamp);
    EXPECT_EQ(0x5d60e8efU, tombstone.serializedForm.checksum);

    EXPECT_FALSE(tombstone.key);
    EXPECT_TRUE(tombstone.tombstoneBuffer);
    EXPECT_EQ(sizeof(ObjectTombstone::SerializedForm) + 5,
        (*tombstone.tombstoneBuffer)->getTotalLength());
    EXPECT_EQ(0, memcmp("key!",
        (*tombstone.tombstoneBuffer)->getRange(sizeof(
            ObjectTombstone::SerializedForm), 5), 5));
}

TEST_F(ObjectTombstoneTest, serializeToBuffer) {
    for (uint32_t i = 0; i < arrayLength(tombstones); i++) {
        ObjectTombstone& tombstone = *tombstones[i];
        Buffer buffer;
        tombstone.serializeToBuffer(buffer);
        const ObjectTombstone::SerializedForm* header =
            buffer.getStart<ObjectTombstone::SerializedForm>();

        EXPECT_EQ(sizeof(*header) + 5, buffer.getTotalLength());

        EXPECT_EQ(572U, header->tableId);
        EXPECT_EQ(925U, header->segmentId);
        EXPECT_EQ(58U, header->objectVersion);
        EXPECT_EQ(335U, header->timestamp);
        EXPECT_EQ(0x5d60e8efU, header->checksum);

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
        tombstone.serializeToBuffer(buffer);
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
    EXPECT_EQ(sizeof(ObjectTombstone::SerializedForm),
              ObjectTombstone::getSerializedLength(0));
    EXPECT_EQ(sizeof(ObjectTombstone::SerializedForm) + 7,
              ObjectTombstone::getSerializedLength(7));
}

} // namespace RAMCloud
