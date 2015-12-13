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

#include "Object.h"
#include "RamCloud.h"

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
          cumulativeKeyLengths(),
          keyList(),
          keyHash(),
          buffer(),
          buffer2(),
          buffer3(),
          buffer4(),
          contiguousBuffer(),
          objectDataFromBuffer(),
          objectFromMultiChunkBuffer(),
          objectFromContiguousBuffer(),
          objectFromContiguousVoidPointer(),
          objectFromVoidPointer(),
          objects(),
          singleKeyObject()
    {
        snprintf(stringKeys[0], sizeof(stringKeys[0]), "ha");
        snprintf(stringKeys[1], sizeof(stringKeys[1]), "hi");
        snprintf(stringKeys[2], sizeof(stringKeys[2]), "ho");
        snprintf(dataBlob, sizeof(dataBlob), "YO!");

        Key key(57, stringKeys[0], sizeof(stringKeys[0]));
        keyHash = key.getHash();

        keyList[0].key = stringKeys[0];
        keyList[0].keyLength = 3;
        keyList[1].key = stringKeys[1];
        keyList[1].keyLength = 3;
        keyList[2].key = stringKeys[2];
        keyList[2].keyLength = 3;

        // write some garbage into the buffer so that the starting
        // offset of keysAndValue in keysAndValueBuffer is != 0. In this
        // case, it will be sizeof(stringKeys[0])
        buffer.appendExternal(&stringKeys[0], sizeof(stringKeys[0]));

        // this is the starting of keysAndValue in the keysAndValueBuffer
        buffer.appendExternal(&numKeys, sizeof(numKeys));

        // lengths of all they keys are 3.
        // store cumulativeKeyLengths in the object

        cumulativeKeyLengths[0] = 3;
        cumulativeKeyLengths[1] = 6;
        cumulativeKeyLengths[2] = 9;
        buffer.appendExternal(cumulativeKeyLengths,
                              3 *sizeof(CumulativeKeyLength));

        // append keys here.
        buffer.appendExternal(&stringKeys[0], sizeof(stringKeys[0]));
        buffer.appendExternal(&stringKeys[1], sizeof(stringKeys[1]));
        buffer.appendExternal(&stringKeys[2], sizeof(stringKeys[2]));

        // append data
        buffer.appendExternal(&dataBlob[0], 2);
        buffer.appendExternal(&dataBlob[2], 2);
        objectDataFromBuffer.construct(key.getTableId(), 75, 723, buffer,
                                        sizeof32(stringKeys[0]));

        objectDataFromBuffer->assembleForLog(buffer2);

        uint32_t length = buffer2.size();
        const char* contiguous = static_cast<char*>(
                buffer2.getRange(0, length));
        objectFromContiguousVoidPointer.construct(
            contiguous, length);

        // Create a buffer holding the object in multiple chunks.
        // Prepend some garbage so that we can test the constructor
        // with a non-zero offset.
        buffer4.appendCopy(&stringKeys[0], sizeof(stringKeys[0]));
        buffer4.appendExternal(contiguous, 10);
        buffer4.appendExternal(contiguous+10, 20);
        buffer4.appendExternal(contiguous+30, length-30);
        objectFromMultiChunkBuffer.construct(buffer4, sizeof32(stringKeys[0]),
                                   length);

        contiguousBuffer.appendExternal(buffer4.getRange(0, buffer4.size()),
                                        buffer4.size());

        objectFromContiguousBuffer.construct(contiguousBuffer,
                                             sizeof32(stringKeys[0]),
                                             length);

        // this object will only contain one key
        objectFromVoidPointer.construct(key, dataBlob, 4, 75, 723, buffer3);

        objects[0] = &*objectDataFromBuffer;
        objects[1] = &*objectFromMultiChunkBuffer;
        objects[2] = &*objectFromContiguousVoidPointer;
        objects[3] = &*objectFromContiguousBuffer;
        singleKeyObject = &*objectFromVoidPointer;
    }

    ~ObjectTest()
    {
    }

    // Don't use static strings, since they'll be loaded into read-only
    // memory and we can't mutate to test checksumming.
    char stringKeys[3][3];
    char dataBlob[4];
    KeyCount numKeys;
    // the cumulativeKeyLength values have to be on the heap. Can't just
    // create temporary variables in the constructor because appending to a
    // buffer does not allocate memory and these variables will go out of of
    // scope after the constructor completes execution
    CumulativeKeyLength cumulativeKeyLengths[3];
    KeyInfo keyList[3];
    KeyHash keyHash;

    Buffer buffer;
    Buffer buffer2;
    Buffer buffer3;
    Buffer buffer4;
    Buffer contiguousBuffer;

    Tub<Object> objectDataFromBuffer;
    Tub<Object> objectFromMultiChunkBuffer;
    Tub<Object> objectFromContiguousBuffer;
    Tub<Object> objectFromContiguousVoidPointer;
    Tub<Object> objectFromVoidPointer;

    Object* objects[4];
    // Handle this object separately as it only has one key.
    // This kind of object is primarily used by unit
    // tests
    Object* singleKeyObject;

    DISALLOW_COPY_AND_ASSIGN(ObjectTest);
};

TEST_F(ObjectTest, constructor_dataFromBuffer)
{
    Object& object = *objectDataFromBuffer;

    EXPECT_EQ(57U, object.header.tableId);
    EXPECT_EQ(75U, object.header.version);
    EXPECT_EQ(723U, object.header.timestamp);
    EXPECT_EQ(0xBB68333C, object.header.checksum);

    const uint8_t *keysAndValue = reinterpret_cast<const uint8_t *>(
                            object.getKeysAndValue());

    KeyCount numKeys = *keysAndValue;
    EXPECT_EQ(3U, numKeys);

    // skip ahead past numKeys
    keysAndValue = keysAndValue + sizeof(KeyCount);

    const CumulativeKeyLength cumulativeKeyLengthOne = *(reinterpret_cast<
                                                const CumulativeKeyLength *>(
                                                keysAndValue));
    const CumulativeKeyLength cumulativeKeyLengthTwo = *(reinterpret_cast<
                                                const CumulativeKeyLength *>(
                                                keysAndValue + 2));
    const CumulativeKeyLength cumulativeKeyLengthThree = *(reinterpret_cast<
                                                const CumulativeKeyLength *>(
                                                keysAndValue + 4));

    EXPECT_EQ(3, cumulativeKeyLengthOne);
    EXPECT_EQ(6, cumulativeKeyLengthTwo);
    EXPECT_EQ(9, cumulativeKeyLengthThree);

    // the keys are NULL terminated anyway
    EXPECT_EQ("ha", string(reinterpret_cast<const char*>(
                    keysAndValue + 3 * sizeof(CumulativeKeyLength))));
    EXPECT_EQ("hi", string(reinterpret_cast<const char*>(
                    keysAndValue + 3 * sizeof(CumulativeKeyLength) + 3)));
    EXPECT_EQ("ho", string(reinterpret_cast<const char*>(
                    keysAndValue + 3 * sizeof(CumulativeKeyLength) + 6)));

    EXPECT_EQ(20U, object.keysAndValueLength);
    EXPECT_FALSE(object.keysAndValue);
    EXPECT_TRUE(object.keysAndValueBuffer);

    // offset into what getKeysAndValue() returns, to point to the actual data
    // blob. Skip the cumulative key length values and the key values
    // numKeys = 3, total length of all 3 keys is 9
    EXPECT_EQ("YO!", string(reinterpret_cast<const char*>(
                    keysAndValue + 3 * sizeof(CumulativeKeyLength) + 9)));
}

TEST_F(ObjectTest, constructor_entirelyFromBuffer)
{
    Object& object = *objectFromMultiChunkBuffer;

    EXPECT_EQ(57U, object.header.tableId);
    EXPECT_EQ(75U, object.header.version);
    EXPECT_EQ(723U, object.header.timestamp);
    EXPECT_EQ(0xBB68333C, object.header.checksum);
    EXPECT_EQ(0, object.keysAndValue);

    const uint8_t *keysAndValue = reinterpret_cast<const uint8_t *>(
                            object.getKeysAndValue());

    const KeyCount numKeys = *keysAndValue;
    EXPECT_EQ(3U, numKeys);

    keysAndValue = keysAndValue + sizeof(KeyCount);

    const CumulativeKeyLength cumulativeKeyLengthOne = *(reinterpret_cast<
                                                const CumulativeKeyLength *>(
                                                keysAndValue));
    const CumulativeKeyLength cumulativeKeyLengthTwo = *(reinterpret_cast<
                                                const CumulativeKeyLength *>(
                                                keysAndValue + 2));
    const CumulativeKeyLength cumulativeKeyLengthThree = *(reinterpret_cast<
                                                const CumulativeKeyLength *>(
                                                keysAndValue + 4));

    EXPECT_EQ(3, cumulativeKeyLengthOne);
    EXPECT_EQ(6, cumulativeKeyLengthTwo);
    EXPECT_EQ(9, cumulativeKeyLengthThree);

    EXPECT_EQ("ha", string(reinterpret_cast<const char*>(
                    keysAndValue + 3 * sizeof(CumulativeKeyLength))));
    EXPECT_EQ("hi", string(reinterpret_cast<const char*>(
                    keysAndValue + 3 * sizeof(CumulativeKeyLength) + 3)));
    EXPECT_EQ("ho", string(reinterpret_cast<const char*>(
                    keysAndValue + 3 * sizeof(CumulativeKeyLength) + 6)));

    EXPECT_EQ(20U, object.keysAndValueLength);
    EXPECT_FALSE(object.keysAndValue);
    EXPECT_TRUE(object.keysAndValueBuffer);

    EXPECT_EQ("YO!", string(reinterpret_cast<const char*>(
                    keysAndValue + 3 * sizeof(CumulativeKeyLength) + 9)));

    EXPECT_TRUE((*objectFromContiguousBuffer).keysAndValue);
}

TEST_F(ObjectTest, constructor_fromContiguousVoidPointer)
{
    Object& object = *objectFromContiguousVoidPointer;

    EXPECT_EQ(57U, object.header.tableId);
    EXPECT_EQ(75U, object.header.version);
    EXPECT_EQ(723U, object.header.timestamp);
    EXPECT_EQ(0xBB68333C, object.header.checksum);

    const uint8_t *keysAndValue = reinterpret_cast<const uint8_t *>(
                            object.getKeysAndValue());

    const KeyCount numKeys = *keysAndValue;
    EXPECT_EQ(3U, numKeys);

    keysAndValue = keysAndValue + sizeof(KeyCount);

    const CumulativeKeyLength cumulativeKeyLengthOne = *(reinterpret_cast<
                                                const CumulativeKeyLength *>(
                                                keysAndValue));
    const CumulativeKeyLength cumulativeKeyLengthTwo = *(reinterpret_cast<
                                                const CumulativeKeyLength *>(
                                                keysAndValue + 2));
    const CumulativeKeyLength cumulativeKeyLengthThree = *(reinterpret_cast<
                                                const CumulativeKeyLength *>(
                                                keysAndValue + 4));

    EXPECT_EQ(3, cumulativeKeyLengthOne);
    EXPECT_EQ(6, cumulativeKeyLengthTwo);
    EXPECT_EQ(9, cumulativeKeyLengthThree);

    EXPECT_EQ("ha", string(reinterpret_cast<const char*>(
                    keysAndValue + 3 * sizeof(CumulativeKeyLength))));
    EXPECT_EQ("hi", string(reinterpret_cast<const char*>(
                    keysAndValue + 3 * sizeof(CumulativeKeyLength) + 3)));
    EXPECT_EQ("ho", string(reinterpret_cast<const char*>(
                    keysAndValue + 3 * sizeof(CumulativeKeyLength) + 6)));

    EXPECT_EQ(20U, object.keysAndValueLength);
    EXPECT_TRUE(object.keysAndValue);
    EXPECT_FALSE(object.keysAndValueBuffer);

    EXPECT_EQ("YO!", string(reinterpret_cast<const char*>(
                    keysAndValue + 3 * sizeof(CumulativeKeyLength) + 9)));
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
    EXPECT_EQ(0xE86291D1, object.header.checksum);

    const uint8_t *keysAndValue = reinterpret_cast<const uint8_t *>(
                            object.getKeysAndValue());

    KeyCount numKeys = *keysAndValue;
    EXPECT_EQ(1U, numKeys);

    // skip ahead past numKeys
    keysAndValue = keysAndValue + sizeof(KeyCount);

    const CumulativeKeyLength cumulativeKeyLengthOne = *(reinterpret_cast<
                                             const CumulativeKeyLength *>(
                                             keysAndValue));

    EXPECT_EQ(3, cumulativeKeyLengthOne);

    EXPECT_EQ("ha", string(reinterpret_cast<const char*>(
                    keysAndValue + sizeof(CumulativeKeyLength))));

    EXPECT_EQ(10U, object.keysAndValueLength);
    EXPECT_FALSE(object.keysAndValue);
    EXPECT_TRUE(object.keysAndValueBuffer);

    // offset into what getKeysAndValue() returns, to point to the actual data
    // blob. Skip the key length value and the key value
    // numKeys = 1
    EXPECT_EQ("YO!", string(reinterpret_cast<const char*>(
                    keysAndValue + sizeof(KeyLength) + 3)));
}

TEST_F(ObjectTest, assembleForLog) {
    for (uint32_t i = 0; i < arrayLength(objects); i++) {
        Object& object = *objects[i];
        Buffer buffer;
        object.assembleForLog(buffer);
        const Object::Header* header =
            buffer.getStart<Object::Header>();

        EXPECT_EQ(sizeof(*header) + sizeof(KeyCount) +
                    3 * sizeof(CumulativeKeyLength)
                    + 9 + 4, buffer.size());

        EXPECT_EQ(57U, header->tableId);
        EXPECT_EQ(75U, header->version);
        EXPECT_EQ(723U, header->timestamp);
        EXPECT_EQ(0xBB68333C, object.header.checksum);

        const KeyCount numKeys= *buffer.getOffset<KeyCount>(sizeof(*header));
        EXPECT_EQ(3U, numKeys);

        const CumulativeKeyLength cumulativeKeyLengthOne = *buffer.getOffset<
                                                CumulativeKeyLength>(
                                                sizeof(*header) +
                                                sizeof(KeyCount));
        const CumulativeKeyLength cumulativeKeyLengthTwo = *buffer.getOffset<
                                                CumulativeKeyLength>(
                                                sizeof(*header) +
                                                sizeof(KeyCount) +
                                                sizeof(CumulativeKeyLength));
        const CumulativeKeyLength cumulativeKeyLengthThree = *buffer.getOffset<
                                                CumulativeKeyLength>
                                                (sizeof(*header) +
                                                sizeof(KeyCount) + 2 *
                                                sizeof(CumulativeKeyLength));

        EXPECT_EQ(3U, cumulativeKeyLengthOne);
        EXPECT_EQ(6U, cumulativeKeyLengthTwo);
        EXPECT_EQ(9U, cumulativeKeyLengthThree);

        EXPECT_EQ("ha", string(reinterpret_cast<const char*>(
                        buffer.getRange(sizeof(*header) + 7, 3))));
        EXPECT_EQ("hi", string(reinterpret_cast<const char*>(
                        buffer.getRange(sizeof(*header) + 10, 3))));
        EXPECT_EQ("ho", string(reinterpret_cast<const char*>(
                        buffer.getRange(sizeof(*header) + 13, 3))));

        const void* data = buffer.getRange(sizeof(*header) + sizeof(KeyCount)
                             + 3 * sizeof(CumulativeKeyLength) + 9, 4);
        EXPECT_EQ("YO!", string(reinterpret_cast<const char*>(
                         data)));
    }

    // test this function for the single key object separately
    Buffer buffer;
    Object& object = *singleKeyObject;
    object.assembleForLog(buffer);
    const Object::Header* header =
            buffer.getStart<Object::Header>();

    EXPECT_EQ(sizeof(*header) + sizeof(KeyCount) + sizeof(KeyLength)
                    + 3 + 4, buffer.size());

    EXPECT_EQ(57U, header->tableId);
    EXPECT_EQ(75U, header->version);
    EXPECT_EQ(723U, header->timestamp);
    EXPECT_EQ(0xE86291D1, object.header.checksum);

    const KeyCount numKeys= *buffer.getOffset<KeyCount>(sizeof(*header));
    EXPECT_EQ(1U, numKeys);

    const CumulativeKeyLength cumulativeKeyLengthOne = *buffer.getOffset<
                                                CumulativeKeyLength>(
                                                sizeof(*header) +
                                                sizeof(KeyCount));

    EXPECT_EQ(3U, cumulativeKeyLengthOne);
    EXPECT_EQ("ha", string(reinterpret_cast<const char *>(
                    buffer.getRange(sizeof(*header) + 3, 3))));

    const void* data = buffer.getRange(sizeof(*header) + sizeof(KeyCount)
                             + sizeof(CumulativeKeyLength) + 3, 4);
    EXPECT_EQ("YO!", string(reinterpret_cast<const char*>(
                         data)));
}

TEST_F(ObjectTest, assembleForLog_contigMemory) {
    Object& object = *singleKeyObject;
    Buffer buffer;
    uint8_t* target = static_cast<uint8_t*>(buffer.alloc(
            object.getSerializedLength()));

    object.assembleForLog(target);
    Object::Header* header = reinterpret_cast<Object::Header *>(target);

    EXPECT_EQ(sizeof(*header) + sizeof(KeyCount) + sizeof(KeyLength)
                    + 3 + 4, object.getSerializedLength());

    EXPECT_EQ(57U, header->tableId);
    EXPECT_EQ(75U, header->version);
    EXPECT_EQ(723U, header->timestamp);
    EXPECT_EQ(0xE86291D1, object.header.checksum);

    const KeyCount numKeys= *(target + sizeof32(*header));
    EXPECT_EQ(1U, numKeys);

    const CumulativeKeyLength cumulativeKeyLengthOne =
            *(target + sizeof(*header) + sizeof(KeyCount));

    EXPECT_EQ(3U, cumulativeKeyLengthOne);
    EXPECT_EQ("ha", string(reinterpret_cast<const char *>(target +
                sizeof32(*header) + 3)));

    const void* data = target +sizeof(*header) + sizeof(KeyCount) +
                       sizeof(CumulativeKeyLength) + 3;
    EXPECT_EQ("YO!", string(reinterpret_cast<const char*>(data)));

}

TEST_F(ObjectTest, appendValueToBuffer) {
    for (uint32_t i = 0; i < arrayLength(objects); i++) {
        Object& object = *objects[i];
        Buffer buffer;
        uint32_t valueOffset = 0;
        object.getValueOffset(&valueOffset);
        EXPECT_EQ(16U, valueOffset);

        object.appendValueToBuffer(&buffer);
        EXPECT_EQ(4U, buffer.size());
        EXPECT_EQ("YO!", string(reinterpret_cast<const char*>(
                        buffer.getRange(0, 4))));
    }
}

TEST_F(ObjectTest, appendKeysAndValueToBuffer) {
    for (uint32_t i = 0; i < arrayLength(objects); i++) {
        Object& object = *objects[i];
        Buffer buffer;
        object.appendKeysAndValueToBuffer(buffer);
        EXPECT_EQ(20U, buffer.size());

        const KeyCount numKeys= *buffer.getOffset<KeyCount>(0);
        EXPECT_EQ(3U, numKeys);

        const CumulativeKeyLength cumulativeKeyLengthOne =
                *buffer.getOffset<CumulativeKeyLength>(sizeof(KeyCount));
        const CumulativeKeyLength cumulativeKeyLengthTwo =
                *buffer.getOffset<CumulativeKeyLength>(sizeof(KeyCount) +
                        sizeof(CumulativeKeyLength));
        const CumulativeKeyLength cumulativeKeyLengthThree =
                *buffer.getOffset<CumulativeKeyLength>(sizeof(KeyCount) +
                        2*sizeof(CumulativeKeyLength));

        EXPECT_EQ(3, cumulativeKeyLengthOne);
        EXPECT_EQ(6, cumulativeKeyLengthTwo);
        EXPECT_EQ(9, cumulativeKeyLengthThree);

        EXPECT_EQ("ha", string(reinterpret_cast<const char*>(
                        buffer.getRange(7, 3))));
        EXPECT_EQ("hi", string(reinterpret_cast<const char*>(
                        buffer.getRange(10, 3))));
        EXPECT_EQ("ho", string(reinterpret_cast<const char*>(
                        buffer.getRange(13, 3))));
        EXPECT_EQ("YO!", string(reinterpret_cast<const char*>(
                        buffer.getRange(16, 4))));
    }
}

TEST_F(ObjectTest, appendKeysAndValueToBuffer_writeMultipleKeys) {
    Buffer buffer;
    KeyInfo keyList[3];
    keyList[0].key = "ha";
    keyList[0].keyLength = 3;
    keyList[1].key = "hi";
    keyList[1].keyLength = 3;
    keyList[2].key = "ho";
    keyList[2].keyLength = 3;

    Object::appendKeysAndValueToBuffer(57, 3, keyList, "YO!", 4, &buffer);
    EXPECT_EQ(20U, buffer.size());

    const KeyCount numKeys= *buffer.getOffset<KeyCount>(0);
    EXPECT_EQ(3U, numKeys);

    const CumulativeKeyLength cumulativeKeyLengthOne =
            *buffer.getOffset<CumulativeKeyLength>(sizeof(KeyCount));
    const CumulativeKeyLength cumulativeKeyLengthTwo =
            *buffer.getOffset<CumulativeKeyLength>(sizeof(KeyCount) +
                        sizeof(CumulativeKeyLength));
    const CumulativeKeyLength cumulativeKeyLengthThree =
            *buffer.getOffset<CumulativeKeyLength>(sizeof(KeyCount) +
                        2 * sizeof(CumulativeKeyLength));

    EXPECT_EQ(3, cumulativeKeyLengthOne);
    EXPECT_EQ(6, cumulativeKeyLengthTwo);
    EXPECT_EQ(9, cumulativeKeyLengthThree);

    EXPECT_EQ("ha", string(reinterpret_cast<const char*>(
                    buffer.getRange(7, 3))));
    EXPECT_EQ("hi", string(reinterpret_cast<const char*>(
                    buffer.getRange(10, 3))));
    EXPECT_EQ("ho", string(reinterpret_cast<const char*>(
                    buffer.getRange(13, 3))));
    EXPECT_EQ("YO!", string(reinterpret_cast<const char*>(
                    buffer.getRange(16, 4))));
}

TEST_F(ObjectTest, appendKeysAndValueToBuffer_writeSingleKey) {
    Buffer buffer;
    Key key(57, "ha", 3);

    Object::appendKeysAndValueToBuffer(key, "YO!", 4, &buffer);
    EXPECT_EQ(10U, buffer.size());

    const KeyCount numKeys= *buffer.getOffset<KeyCount>(0);
    EXPECT_EQ(1U, numKeys);

    const CumulativeKeyLength length =
                *buffer.getOffset<CumulativeKeyLength>(sizeof(KeyCount));

    EXPECT_EQ(3, length);

    EXPECT_EQ("ha", string(reinterpret_cast<const char*>(
                    buffer.getRange(3, 3))));
    EXPECT_EQ("YO!", string(reinterpret_cast<const char*>(
                    buffer.getRange(6, 4))));
}

TEST_F(ObjectTest, changeTableId)
{
    Object& object = *objectDataFromBuffer;
    object.changeTableId(899U);

    EXPECT_EQ(899U, object.header.tableId);
    EXPECT_EQ(75U, object.header.version);
    EXPECT_EQ(723U, object.header.timestamp);
    EXPECT_TRUE(object.checkIntegrity());

    const uint8_t *keysAndValue = reinterpret_cast<const uint8_t *>(
                            object.getKeysAndValue());

    KeyCount numKeys = *keysAndValue;
    EXPECT_EQ(3U, numKeys);

    // skip ahead past numKeys
    keysAndValue = keysAndValue + sizeof(KeyCount);

    const CumulativeKeyLength cumulativeKeyLengthOne = *(reinterpret_cast<
                                                const CumulativeKeyLength *>(
                                                keysAndValue));
    const CumulativeKeyLength cumulativeKeyLengthTwo = *(reinterpret_cast<
                                                const CumulativeKeyLength *>(
                                                keysAndValue + 2));
    const CumulativeKeyLength cumulativeKeyLengthThree = *(reinterpret_cast<
                                                const CumulativeKeyLength *>(
                                                keysAndValue + 4));

    EXPECT_EQ(3, cumulativeKeyLengthOne);
    EXPECT_EQ(6, cumulativeKeyLengthTwo);
    EXPECT_EQ(9, cumulativeKeyLengthThree);

    // the keys are NULL terminated anyway
    EXPECT_EQ("ha", string(reinterpret_cast<const char*>(
                    keysAndValue + 3 * sizeof(CumulativeKeyLength))));
    EXPECT_EQ("hi", string(reinterpret_cast<const char*>(
                    keysAndValue + 3 * sizeof(CumulativeKeyLength) + 3)));
    EXPECT_EQ("ho", string(reinterpret_cast<const char*>(
                    keysAndValue + 3 * sizeof(CumulativeKeyLength) + 6)));

    EXPECT_EQ(20U, object.keysAndValueLength);
    EXPECT_FALSE(object.keysAndValue);
    EXPECT_TRUE(object.keysAndValueBuffer);

    // offset into what getKeysAndValue() returns, to point to the actual data
    // blob. Skip the cumulative key length values and the key values
    // numKeys = 3, total length of all 3 keys is 9
    EXPECT_EQ("YO!", string(reinterpret_cast<const char*>(
                    keysAndValue + 3 * sizeof(CumulativeKeyLength) + 9)));
}

TEST_F(ObjectTest, fillKeyOffsets)
{
    for (uint32_t i = 0; i < arrayLength(objects); i++) {
        EXPECT_TRUE(objects[i]->fillKeyOffsets());
        KeyCount numKeys = objects[i]->keyOffsets->numKeys;
        EXPECT_EQ(3U, numKeys);
        EXPECT_EQ(3U, objects[i]->keyOffsets->cumulativeLengths[0]);
        EXPECT_EQ(6U, objects[i]->keyOffsets->cumulativeLengths[1]);
        EXPECT_EQ(9U, objects[i]->keyOffsets->cumulativeLengths[2]);
    }
}

TEST_F(ObjectTest, getTableId) {
    for (uint32_t i = 0; i < arrayLength(objects); i++)
        EXPECT_EQ(57U, objects[i]->getTableId());
}

TEST_F(ObjectTest, getKey) {
    for (uint32_t i = 0; i < arrayLength(objects); i++) {
        KeyLength keyLen;
        EXPECT_EQ("ha", string(reinterpret_cast<const char*>(
                        objects[i]->getKey())));
        EXPECT_EQ("ha", string(reinterpret_cast<const char*>(
                        objects[i]->getKey(0, &keyLen))));
        EXPECT_EQ(3U, keyLen);
        EXPECT_EQ("hi", string(reinterpret_cast<const char*>(
                        objects[i]->getKey(1, &keyLen))));
        EXPECT_EQ(3U, keyLen);
        EXPECT_EQ("ho", string(reinterpret_cast<const char*>(
                        objects[i]->getKey(2, &keyLen))));
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

TEST_F(ObjectTest, getPKHash) {
    for (uint32_t i = 0; i < arrayLength(objects); i++) {
        EXPECT_EQ(keyHash, objects[i]->getPKHash());
    }
}

TEST_F(ObjectTest, getKeysAndValue) {
    for (uint32_t i = 0; i < arrayLength(objects); i++) {
        const uint8_t *keysAndValue = reinterpret_cast<const uint8_t *>(
                                    objects[i]->getKeysAndValue());
        // offset into what getKeysAndValue() returns, to point to the actual
        // data blob. Skip the key length values and the key values
        // numKeys = 3, total length of all 3 keys is 9

        const KeyCount numKeys = *(reinterpret_cast<const KeyCount *>(
                                            keysAndValue));
        EXPECT_EQ(3U, numKeys);

        keysAndValue = keysAndValue + sizeof(KeyCount);

        const CumulativeKeyLength cumulativeKeyLengthOne =
                *(reinterpret_cast<const CumulativeKeyLength *>(keysAndValue));
        const CumulativeKeyLength cumulativeKeyLengthTwo =
                *(reinterpret_cast<const CumulativeKeyLength *>(keysAndValue +
                        sizeof(CumulativeKeyLength)));
        const CumulativeKeyLength cumulativeKeyLengthThree =
                *(reinterpret_cast<const CumulativeKeyLength *>(keysAndValue +
                        2 * sizeof(CumulativeKeyLength)));

        EXPECT_EQ(3, cumulativeKeyLengthOne);
        EXPECT_EQ(6, cumulativeKeyLengthTwo);
        EXPECT_EQ(9, cumulativeKeyLengthThree);

        EXPECT_EQ("ha", string(reinterpret_cast<const char*>(
                        keysAndValue + 3 * sizeof(CumulativeKeyLength))));
        EXPECT_EQ("hi", string(reinterpret_cast<const char*>(
                        keysAndValue + 3 * sizeof(CumulativeKeyLength) + 3)));
        EXPECT_EQ("ho", string(reinterpret_cast<const char*>(
                        keysAndValue + 3 * sizeof(CumulativeKeyLength) + 6)));

        EXPECT_EQ("YO!", string(reinterpret_cast<const char*>(
                         keysAndValue + 3 * sizeof(CumulativeKeyLength) + 9)));
    }
}

TEST_F(ObjectTest, getValueContiguous) {
    uint32_t len;
    EXPECT_EQ("YO!", string(reinterpret_cast<const char*>(
                     objects[0]->getValue(&len))));
    EXPECT_EQ(4U, len);
    EXPECT_EQ("YO!", string(reinterpret_cast<const char*>(
                     objects[1]->getValue(&len))));
    EXPECT_EQ(4U, len);
    EXPECT_EQ("YO!", string(reinterpret_cast<const char*>(
                     objects[2]->getValue(&len))));
    EXPECT_EQ(4U, len);
}

TEST_F(ObjectTest, getValueOffset) {
    uint32_t valueOffset;
    EXPECT_TRUE(objects[0]->getValueOffset(&valueOffset));
    EXPECT_EQ(16U, valueOffset);
    EXPECT_TRUE(objects[1]->getValueOffset(&valueOffset));
    EXPECT_EQ(16U, valueOffset);
    EXPECT_TRUE(objects[2]->getValueOffset(&valueOffset));
    EXPECT_EQ(16U, valueOffset);
}

TEST_F(ObjectTest, getValueLength) {
    EXPECT_EQ(4U, objects[0]->getValueLength());
    EXPECT_EQ(4U, objects[1]->getValueLength());
    EXPECT_EQ(4U, objects[2]->getValueLength());
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

TEST_F(ObjectTest, getSerializedLength) {
    EXPECT_EQ(44U, objects[0]->getSerializedLength());
    EXPECT_EQ(44U, objects[1]->getSerializedLength());
    EXPECT_EQ(44U, objects[2]->getSerializedLength());
}

TEST_F(ObjectTest, checkIntegrity) {
    for (uint32_t i = 0; i < arrayLength(objects); i++) {
        Object& object = *objects[i];
        Buffer buffer;
        object.assembleForLog(buffer);
        EXPECT_TRUE(object.checkIntegrity());

        // screw up the first byte (in the Header)
        uint8_t* evil = reinterpret_cast<uint8_t*>(&object.header);
        uint8_t tmp = *evil;
        *evil = static_cast<uint8_t>(~*evil);
        EXPECT_FALSE(object.checkIntegrity());
        *evil = tmp;

        // screw up the first byte - the number of keys
        EXPECT_TRUE(object.checkIntegrity());
        if (object.keysAndValue != NULL) {
            evil = reinterpret_cast<uint8_t*>(const_cast<void*>(
                object.keysAndValue));
        } else {
            evil = reinterpret_cast<uint8_t*>(
                const_cast<void*>(object.keysAndValueBuffer->getRange(
                object.keysAndValueOffset, 1)));
        }
        tmp = *evil;
        *evil = static_cast<uint8_t>(~*evil);
        EXPECT_FALSE(object.checkIntegrity());
        *evil = tmp;

        // screw up the first byte in the first key
        EXPECT_TRUE(object.checkIntegrity());
        if (object.keysAndValue != NULL) {
            evil = reinterpret_cast<uint8_t*>(const_cast<void*>(
                object.keysAndValue)) + 7;
        } else {
            evil = reinterpret_cast<uint8_t*>(
                const_cast<void*>(object.keysAndValueBuffer->getRange(
                object.keysAndValueOffset + 7, 1)));
        }
        tmp = *evil;
        *evil = static_cast<uint8_t>(~*evil);
        EXPECT_FALSE(object.checkIntegrity());
        *evil = tmp;

        // screw up the last byte (in the data blob)
        EXPECT_TRUE(object.checkIntegrity());
        if (object.keysAndValue != NULL) {
            evil = reinterpret_cast<uint8_t*>(const_cast<void*>(
                object.keysAndValue)) + object.keysAndValueLength - 1;
        } else {
            evil = reinterpret_cast<uint8_t*>(
                const_cast<void*>(object.keysAndValueBuffer->getRange(
                object.keysAndValueOffset + object.keysAndValueLength - 1,
                1)));
        }
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
          cumulativeKeyLength(),
          object(),
          tombstoneFromObject(),
          tombstoneFromBuffer()
    {
        snprintf(stringKey, sizeof(stringKey), "key!");
        snprintf(dataBlob, sizeof(dataBlob), "data!");
        cumulativeKeyLength = 5;

        Key key(572, stringKey, 5);

        buffer.appendExternal(&numKeys, sizeof(numKeys));
        buffer.appendExternal(&cumulativeKeyLength,
                              sizeof(cumulativeKeyLength));
        buffer.appendExternal(stringKey, cumulativeKeyLength + 1);
        buffer.appendExternal(dataBlob, 6);

        object.construct(key.getTableId(), 58, 723, buffer);
        tombstoneFromObject.construct(*object, 925, 335);

        buffer.reset();
        tombstoneFromObject->assembleForLog(buffer);

        // prepend some garbage into the buffer so that we
        // can test the constructor with a non-zero starting
        // offset
        memcpy(buffer.allocPrepend(sizeof32(stringKey)), stringKey,
                sizeof32(stringKey));
        tombstoneFromBuffer.construct(buffer, sizeof32(stringKey),
                                      buffer.size() -
                                      sizeof32(stringKey));

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
    CumulativeKeyLength cumulativeKeyLength;

    Tub<Object> object;
    Tub<ObjectTombstone> tombstoneFromObject;
    Tub<ObjectTombstone> tombstoneFromBuffer;

    ObjectTombstone* tombstones[2];

    DISALLOW_COPY_AND_ASSIGN(ObjectTombstoneTest);
};

TEST_F(ObjectTombstoneTest, constructor_fromObject) {
    ObjectTombstone& tombstone = *tombstones[0];

    EXPECT_EQ(572U, tombstone.header.tableId);
    EXPECT_EQ(925U, tombstone.header.segmentId);
    EXPECT_EQ(58U, tombstone.header.objectVersion);
    EXPECT_EQ(335U, tombstone.header.timestamp);
    EXPECT_EQ(0x5D60E8EFU, tombstone.header.checksum);

    EXPECT_TRUE(tombstone.key);
    EXPECT_FALSE(tombstone.tombstoneBuffer);
    EXPECT_EQ("key!", string(reinterpret_cast<const char*>(
                      tombstone.key)));
}

TEST_F(ObjectTombstoneTest, constructor_fromBuffer) {
    ObjectTombstone& tombstone = *tombstones[1];

    EXPECT_EQ(572U, tombstone.header.tableId);
    EXPECT_EQ(925U, tombstone.header.segmentId);
    EXPECT_EQ(58U, tombstone.header.objectVersion);
    EXPECT_EQ(335U, tombstone.header.timestamp);
    EXPECT_EQ(0x5D60E8EFU, tombstone.header.checksum);

    EXPECT_FALSE(tombstone.key);
    EXPECT_TRUE(tombstone.tombstoneBuffer);
    EXPECT_EQ(5 + sizeof(ObjectTombstone::Header) + 5,
        (tombstone.tombstoneBuffer)->size());
    EXPECT_EQ("key!", string(reinterpret_cast<const char*>(
        (tombstone.tombstoneBuffer)->getRange(sizeof(
            ObjectTombstone::Header) + 5, 5))));
}

TEST_F(ObjectTombstoneTest, assembleForLog) {
    for (uint32_t i = 0; i < arrayLength(tombstones); i++) {
        ObjectTombstone& tombstone = *tombstones[i];
        Buffer buffer;
        tombstone.assembleForLog(buffer);
        const ObjectTombstone::Header* header =
            buffer.getStart<ObjectTombstone::Header>();

        EXPECT_EQ(sizeof(*header) + 5, buffer.size());

        EXPECT_EQ(572U, header->tableId);
        EXPECT_EQ(925U, header->segmentId);
        EXPECT_EQ(58U, header->objectVersion);
        EXPECT_EQ(335U, header->timestamp);
        EXPECT_EQ(0x5d60e8efU, header->checksum);

        const void* key = buffer.getRange(sizeof(*header), 5);
        EXPECT_EQ(string(reinterpret_cast<const char*>(key)), "key!");
    }
}

TEST_F(ObjectTombstoneTest, assembleForLog_contigMemory) {
    for (uint32_t i = 0; i < arrayLength(tombstones); i++) {
        ObjectTombstone& tombstone = *tombstones[i];
        Buffer buffer;
        uint8_t* target = static_cast<uint8_t*>(buffer.alloc(
                tombstone.getSerializedLength()));

        tombstone.assembleForLog(target);
        ObjectTombstone::Header* header = reinterpret_cast<
                                          ObjectTombstone::Header *>(target);

        EXPECT_EQ(sizeof(*header) + 5, tombstone.getSerializedLength());

        EXPECT_EQ(572U, header->tableId);
        EXPECT_EQ(925U, header->segmentId);
        EXPECT_EQ(58U, header->objectVersion);
        EXPECT_EQ(335U, header->timestamp);
        EXPECT_EQ(0x5d60e8efU, header->checksum);

        const void* key = target + sizeof(*header);
        EXPECT_EQ(string(reinterpret_cast<const char*>(key)), "key!");
    }
}

TEST_F(ObjectTombstoneTest, appendKeyToBuffer) {
    for (uint32_t i = 0; i < arrayLength(tombstones); i++) {
        ObjectTombstone& tombstone = *tombstones[i];
        Buffer buffer;
        tombstone.appendKeyToBuffer(buffer);
        EXPECT_EQ(5U, buffer.size());
        EXPECT_EQ("key!", string(reinterpret_cast<const char*>(
                          buffer.getRange(0, 5))));
    }
}

TEST_F(ObjectTombstoneTest, changeTableId) {
    ObjectTombstone& tombstone = *tombstones[0];
    tombstone.changeTableId(899U);

    EXPECT_EQ(899U, tombstone.header.tableId);
    EXPECT_EQ(925U, tombstone.header.segmentId);
    EXPECT_EQ(58U, tombstone.header.objectVersion);
    EXPECT_EQ(335U, tombstone.header.timestamp);
    EXPECT_TRUE(tombstone.checkIntegrity());

    EXPECT_TRUE(tombstone.key);
    EXPECT_FALSE(tombstone.tombstoneBuffer);
    EXPECT_EQ("key!", string(reinterpret_cast<const char*>(
                      tombstone.key)));
}

TEST_F(ObjectTombstoneTest, getTableId) {
    for (uint32_t i = 0; i < arrayLength(tombstones); i++)
        EXPECT_EQ(572U, tombstones[i]->getTableId());
}

TEST_F(ObjectTombstoneTest, getKey) {
    for (uint32_t i = 0; i < arrayLength(tombstones); i++)
        EXPECT_EQ("key!", string(reinterpret_cast<const char*>(
                        tombstones[i]->getKey())));
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

        uint8_t* evil;
        if (tombstone.key != NULL) {
            evil = reinterpret_cast<uint8_t*>(const_cast<void*>(
                tombstone.key));
        } else {
            evil = reinterpret_cast<uint8_t*>(
                const_cast<void*>(tombstone.tombstoneBuffer->getRange(
                tombstone.keyOffset, 1)));
        }
        uint8_t tmp = *evil;
        *evil = static_cast<uint8_t>(~*evil);
        EXPECT_FALSE(tombstone.checkIntegrity());
        *evil = tmp;

        EXPECT_TRUE(tombstone.checkIntegrity());
        if (tombstone.key != NULL) {
            evil = reinterpret_cast<uint8_t*>(const_cast<void*>(
                tombstone.key)) + tombstone.keyLength - 1;
        } else {
            evil = reinterpret_cast<uint8_t*>(
                const_cast<void*>(tombstone.tombstoneBuffer->getRange(
                tombstone.keyOffset + tombstone.keyLength - 1, 1)));
        }
        tmp = *evil;
        *evil = static_cast<uint8_t>(~*evil);
        EXPECT_FALSE(tombstone.checkIntegrity());
        *evil = tmp;

        EXPECT_TRUE(tombstone.checkIntegrity());
    }
}

TEST_F(ObjectTombstoneTest, getProspectiveSerializedLength) {
    EXPECT_EQ(sizeof(ObjectTombstone::Header),
              ObjectTombstone::getSerializedLength(0));
    EXPECT_EQ(sizeof(ObjectTombstone::Header) + 7,
              ObjectTombstone::getSerializedLength(7));
}

TEST_F(ObjectTombstoneTest, getActualSerializedLength) {
    for (uint32_t i = 0; i < arrayLength(tombstones); i++)
        EXPECT_EQ(37U, tombstones[i]->getSerializedLength());
}

} // namespace RAMCloud
