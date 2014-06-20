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

#include "Crc32C.h"
#include "Object.h"
#include "RamCloud.h"

namespace RAMCloud {

/**
 * Construct an Object in preparation for storing it in the log.
 * This form is used when the header information is available in
 * individual pieces, while the keys and data are stored in  a Buffer
 * (typical use: during write RPCs).
 *
 * \param tableId
 *      TableId for this object.
 * \param version
 *      Version number of this object, which is used to disambiguate
 *      different incarnations of objects with the same key.
 * \param timestamp
 *      The creation time of this object, as returned by the WallTime
 *      module. Used primarily by the cleaner to order live objects and
 *      improve future cleaning performance.
 * \param keysAndValueBuffer
 *      Buffer containing all chunks that will comprise this object's
 *      keysAndValue. Refer to Object.h for a visual description
 *      of keysAndValue.
 * \param startDataOffset
 *      Byte offset in the buffer where keysAndValue start
 * \param length
 *      Length of keysAndValue. If 0, then this constructor computes it as
 *      keysAndValueBuffer.getTotalLength() - startDataOffset.
 *      This is primarily used by the multiWrite RPC in MasterService.
 */
Object::Object(uint64_t tableId,
               uint64_t version,
               uint32_t timestamp,
               Buffer& keysAndValueBuffer,
               uint32_t startDataOffset,
               uint32_t length)
    : header(tableId,
             timestamp,
             version),
      keysAndValueLength(),
      keysAndValue(),
      keysAndValueBuffer(&keysAndValueBuffer),
      keysAndValueOffset(startDataOffset),
      keyOffsets(NULL)
{
    // compute the actual default value
    if (length == 0)
        length = this->keysAndValueBuffer->getTotalLength() - startDataOffset;

    keysAndValueLength = length;
}

/**
 * Construct a new Object in preparation for writing it into the log.
 * Use this constructor when the data for an object is contiguous and
 * the header information is  available in individual pieces. This is
 * primarily used by unit tests to construct objects with just a
 * single key. The main function that will be invoked after a call
 * to this constructor is assembleForLog.
 *
 * \param key
 *      Primary key for this object
 * \param value
 *      Pointer to a single contiguous piece of memory that comprises this
 *      object's value.
 * \param valueLength
 *      Length of the value portion in bytes.
 * \param version
 *      Version number of this object, which is used to disambiguate
 *      different incarnations of objects with the same key.
 * \param timestamp
 *      The creation time of this object, as returned by the WallTime
 *      module. Used primarily by the cleaner to order live objects and
 *      improve future cleaning performance.
 * \param buffer
 *      A buffer that can be used temporarily to store the keys and value
 *      for the object. Its lifetime must cover the lifetime of this Object.
 * \param [out] length
 *      Total length of keysAndValue
 */
Object::Object(Key& key,
               const void* value,
               uint32_t valueLength,
               uint64_t version,
               uint32_t timestamp,
               Buffer& buffer,
               uint32_t *length)
    : header(key.getTableId(),
             timestamp,
             version),
      keysAndValueLength(),
      keysAndValue(),
      keysAndValueBuffer(),
      // Since we are appending to buffer, the offset where keysAndValue
      // begins wil be the current size of the buffer.
      keysAndValueOffset(buffer.getTotalLength()),
      keyOffsets(NULL)
{
    uint32_t primaryKeyInfoLength = KEY_INFO_LENGTH(1) +
                             key.getStringKeyLength();

    keysAndValueLength = primaryKeyInfoLength + valueLength;
    if (length)
        *length = keysAndValueLength;

    uint8_t* keyInfo = static_cast<uint8_t*>(
            buffer.alloc(primaryKeyInfoLength));

    KeyCount keyCount = 1;
    KeyLength keyLength = static_cast<KeyLength>
                                (key.getStringKeyLength());
    const void* keyString = key.getStringKey();
    memcpy(keyInfo, &keyCount, sizeof(KeyCount));
    memcpy(keyInfo + sizeof(KeyCount), &keyLength, sizeof(KeyLength));
    memcpy(keyInfo + KEY_INFO_LENGTH(1), keyString, keyLength);

    buffer.append(value, valueLength);
    keysAndValueBuffer = &buffer;
}

/**
 * Construct an Object using information in the log, which includes the
 * object header as well as the keys and data. This form of the constructor
 * is typically used for extracting information out of the log,
 * For example to serve read requests or to perform log cleaning.
 *
 * \param buffer
 *      Buffer referring to a complete object in the log. It is the
 *      caller's responsibility to make sure that the buffer passed in
 *      actually contains a full object. If it does not, then behavior
 *      is undefined.
 * \param offset
 *      Starting offset in the buffer where the object begins.
 * \param length
 *      Total length of the object in bytes.
 */
Object::Object(Buffer& buffer, uint32_t offset, uint32_t length)
    : header(*buffer.getOffset<Header>(offset)),
      keysAndValueLength(),
      keysAndValue(),
      keysAndValueBuffer(&buffer),
      keysAndValueOffset(offset + sizeof32(header)),
      keyOffsets(NULL)
{
    // If length is not specified, compute the length of keysAndValue
    if (length == 0)
        keysAndValueLength = buffer.getTotalLength() - offset -
                             sizeof32(header);
    else
        keysAndValueLength = length - sizeof32(header);
}

/**
 * Construct Object with information stored in contiguous memory.
 * This is used by ObjectManager and other unit tests
 *
 * \param buffer
 *      First byte of memory area containing the entire object, including
 *      header as well as keys and data. It it the caller's responsibility
 *      to make sure this actually contains a full object. If it does not,
 *      then behavior is undefined.
 * \param length
 *      Total length of the object in bytes.
 */
Object::Object(const void* buffer, uint32_t length)
    : header(*reinterpret_cast<const Header*>(buffer)),
      keysAndValueLength(length - sizeof32(header)),
      keysAndValue(reinterpret_cast<const void*>(reinterpret_cast<
                   const uint8_t*>(buffer) + sizeof32(header))),
      keysAndValueBuffer(),
      keysAndValueOffset(0),
      keyOffsets(NULL)
{
}

/**
 * Append the full object, including header, keys and value exactly as it
 * should be stored in the log, to a buffer
 *
 * \param buffer
 *      The buffer to append a serialized version of this object to.
 */
void
Object::assembleForLog(Buffer& buffer)
{
    header.checksum = computeChecksum();
    buffer.append(&header, sizeof32(header));
    appendKeysAndValueToBuffer(buffer);
}

/**
 * Copy the full object, including header, keys and value exactly as it
 * should be stored in the log. This is used when the memory for an object
 * needs to be allocated in a buffer as opposed to being logically stored
 * in a buffer.
 *
 * \param buffer
 *      Destination to copy a serialized version of this object to.
 *      The caller must ensure that sufficient memory is allocated for the
 *      complete serialized object to be stored
 */
void
Object::assembleForLog(void* buffer)
{
    uint8_t *dst = reinterpret_cast<uint8_t*>(buffer);
    header.checksum = computeChecksum();

    memcpy(dst, &header, sizeof32(header));
    memcpy(dst + sizeof32(header), getKeysAndValue(), keysAndValueLength);
}

/**
 * Append the the value associated with this object to a provided buffer.
 *
 * \param buffer
 *      The buffer to append the value to.
 * \param valueOffset
 *      Offset of the value in the keysAndValue portion of the object
 */
void
Object::appendValueToBuffer(Buffer& buffer, uint32_t valueOffset)
{
    if (keysAndValue) {
        const uint8_t *ptr = reinterpret_cast<const uint8_t *>(keysAndValue);
        buffer.append(ptr + valueOffset, getValueLength());
        return;
    }

    Buffer* sourceBuffer = keysAndValueBuffer;

    Buffer::Iterator it(sourceBuffer, keysAndValueOffset + valueOffset,
                        getValueLength());
    while (!it.isDone()) {
        buffer.append(it.getData(), it.getLength());
        it.next();
    }
}

/**
 * Append the cumulative key lengths, the keys and the value associated with
 * this object to a provided buffer.
 *
 * \param buffer
 *      The buffer to append the keys and the value to.
 */
void
Object::appendKeysAndValueToBuffer(Buffer& buffer)
{
    if (keysAndValue) {
        buffer.append(keysAndValue, keysAndValueLength);
        return;
    }

    // keysAndValueBuffer contains keyLengths, keys and value starting
    // at keysAndValueOffset
    Buffer* sourceBuffer = keysAndValueBuffer;

    Buffer::Iterator it(sourceBuffer, keysAndValueOffset, keysAndValueLength);
    while (!it.isDone()) {
        buffer.append(it.getData(), it.getLength());
        it.next();
    }
}

/**
 * The typical use case for this function is during a write RPC when we want
 * the RPC payload to mirror the format of the object in the log as much as
 * possible. This is primarily used by the RamCloud library to make sure the
 * format of the object does not leak outside this class. It is also used by
 * the MultiWrite framework for the same purpose.
 *
 * \param tableId
 *      The tableId corresponding to this object.
 * \param numKeys
 *      The number of keys in this object.
 * \param keyList
 *      List of keys and key length values as provided by the end client
 * \param value
 *      Pointer to a single contiguous piece of memory that comprises this
 *      object's value.
 * \param valueLength
 *      Length of the value portion in bytes.
 * \param [out] request
 *      A buffer that can be used temporarily to store the keys and value
 *      for the object. Its lifetime must cover the lifetime of this Object.
 * \param [out] length
 *      Total length of keysAndValue
 */
void
Object::appendKeysAndValueToBuffer(uint64_t tableId,
                                   KeyCount numKeys,
                                   KeyInfo *keyList,
                                   const void* value,
                                   uint32_t valueLength,
                                   Buffer& request,
                                   uint32_t *length)
{
    int i;
    uint32_t totalKeyLength = 0;
    uint32_t currentKeyLength = 0;
    if (keyList) {
        // allocate memory first for number of keys and all the cumulative
        // length values
        KeyOffsets *keyOffsetsHelper = reinterpret_cast<KeyOffsets *>(
                                    request.alloc(KEY_INFO_LENGTH(numKeys)));
        keyOffsetsHelper->numKeys = numKeys;
        CumulativeKeyLength *cumLengths = keyOffsetsHelper->cumulativeLengths;


        for (i = 0; i < numKeys; i++) {
            // if the length of a key is 0, we expect the corresponding key
            // is NULL terminated and hence compute the length

            if (!keyList[i].key) { // this key does not exist
                currentKeyLength = 0;
            } else if (!keyList[i].keyLength) {
                currentKeyLength = downCast<uint32_t>(
                                        strlen(static_cast<const char *>(
                                        keyList[i].key)));
            } else {
                currentKeyLength = keyList[i].keyLength;
            }
            // primary key must always exist
            if (i == 0)
                cumLengths[i] = downCast<CumulativeKeyLength>(
                                            currentKeyLength);
            else
                cumLengths[i] = downCast<CumulativeKeyLength>(
                                    cumLengths[i-1] +
                                    currentKeyLength);
            totalKeyLength += currentKeyLength;
        }
        void *keys = request.alloc(totalKeyLength);
        uint8_t *dest = reinterpret_cast<uint8_t *>(keys);
        for (i = 0; i < numKeys; i++) {
            // this key doesn't exist
            if (!keyList[i].key)
                continue;

            if (!keyList[i].keyLength) {
                currentKeyLength = downCast<uint32_t>(
                                        strlen(static_cast<const char *>(
                                        keyList[i].key)));
            } else {
                currentKeyLength = keyList[i].keyLength;
            }
            memcpy(dest, keyList[i].key, currentKeyLength);
            dest = dest + currentKeyLength;
        }
        request.append(value, valueLength);
        if (length)
            *length = KEY_INFO_LENGTH(numKeys) +
                      totalKeyLength + valueLength;
    }
}

/**
 * This is primarily used by the write RPC and the increment RPC handler in
 * MasterService when the objects have just a single key. It is also used by
 * unit tests. This is typically invoked when one does not require an instance
 * of class Object
 *
 * \param key
 *      Primary key for this object
 * \param value
 *      Pointer to a single contiguous piece of memory that comprises this
 *      object's value.
 * \param valueLength
 *      Length of the value portion in bytes.
 * \param buffer
 *      A buffer that can be used temporarily to store the keys and value
 *      for the object. Its lifetime must cover the lifetime of this Object.
 * \param [out] length
 *      Total length of keysAndValue
 */
void
Object::appendKeysAndValueToBuffer(
        Key& key,
        const void* value,
        uint32_t valueLength,
        Buffer& buffer,
        uint32_t *length)
{
    uint32_t primaryKeyInfoLength = KEY_INFO_LENGTH(1) +
                             key.getStringKeyLength();

    if (length)
        *length = primaryKeyInfoLength + valueLength;

    uint8_t* keyInfo = static_cast<uint8_t*>(
            buffer.alloc(primaryKeyInfoLength));

    KeyCount keyCount = 1;
    KeyLength keyLength = static_cast<KeyLength>
                                (key.getStringKeyLength());
    const void *keyString = key.getStringKey();
    memcpy(keyInfo, &keyCount, sizeof(KeyCount));
    memcpy(keyInfo + sizeof(KeyCount), &keyLength, sizeof(KeyLength));
    memcpy(keyInfo + KEY_INFO_LENGTH(1), keyString, keyLength);

    buffer.append(value, valueLength);
}

/**
 * Populate the keyOffsets structure so that it makes operations like
 * getKey() efficient. It is a NO-OP if is already populated.
 *
 * \return
 *      False if keysAndValueBuffer is not big enough to hold information
 *      for all the keys, True otherwise
 */
bool
Object::fillKeyOffsets()
{
    if (!keyOffsets) {
        if (keysAndValue) {
            keyOffsets = static_cast<const struct KeyOffsets *>(
                                keysAndValue);
        } else {
            KeyCount numKeys = *(keysAndValueBuffer->getOffset<KeyCount>(
                                    keysAndValueOffset));
            keyOffsets = static_cast<const struct KeyOffsets *>(
                                keysAndValueBuffer->getRange(
                                keysAndValueOffset, KEY_INFO_LENGTH(numKeys)));
            // check if the buffer is big enough
            if (!keyOffsets)
                return false;
        }
    }
    return true;
}

/**
 * Obtain the 64-bit table identifier associated with this object.
 */
uint64_t
Object::getTableId()
{
    return header.tableId;
}

/**
 * Returns a pointer to one of the object's keys. The key is guaranteed to
 * be in contiguous memory (if it wasn't already contiguous, it will be
 * copied into a contiguous region).
 *
 * \param keyIndex
 *      Index position of this key
 * \param[out] keyLength
 *      Pointer to word that will be filled in with the length of the key
 *      indicated by keyIndex; if NULL then no length is returned.
 *
 * \return
 *      Pointer to the key which will be contiguous, or NULL if there is no
 *      key corresponding to keyIndex
 */
const void*
Object::getKey(KeyIndex keyIndex, KeyLength *keyLength)
{
    if (!fillKeyOffsets())
        return NULL;

    if (keyIndex >= keyOffsets->numKeys)
        return NULL;

    uint32_t firstKeyPos = KEY_INFO_LENGTH(keyOffsets->numKeys);

    uint32_t keyOffset; // 0 corresponds to the starting of keysAndValue
    uint32_t length;
    const CumulativeKeyLength *cumLengths = keyOffsets->cumulativeLengths;
    if (keyIndex == 0) {
        keyOffset = firstKeyPos;
        length = cumLengths[0];
    } else {
        keyOffset = firstKeyPos + cumLengths[keyIndex - 1];
        length = cumLengths[keyIndex] - cumLengths[keyIndex - 1];
    }
    // key does not exist
    if (length == 0)
        return NULL;

    if (keyLength)
        *keyLength = downCast<KeyLength>(length);

    // check bounds
    if (keyOffset > keysAndValueLength)
        return NULL;

    // now that we have the key offset and key length, just return a pointer
    // to the key
    if (keysAndValue)
        return static_cast<const uint8_t *>(keysAndValue) + keyOffset;
    else
        return keysAndValueBuffer->getRange(keyOffset + keysAndValueOffset,
                                                length);
}

/**
 * Obtain the length of the key at position keyIndex.
 * If keyIndex >= numKeys, then endKeyOffset(keyIndex) will
 * return 0. This function should return 0 in such cases.
 *
 * \param keyIndex
 *      Numeric position of the index
 */
KeyLength
Object::getKeyLength(KeyIndex keyIndex)
{
    if (!fillKeyOffsets())
        return 0;

    if (keyIndex >= keyOffsets->numKeys)
        return 0;

    const CumulativeKeyLength *cumLengths = keyOffsets->cumulativeLengths;
    if (keyIndex == 0)
        return cumLengths[0];
    else
        return static_cast<KeyLength>(cumLengths[keyIndex] -
                   cumLengths[keyIndex - 1]);
}

/**
 * Obtain a pointer to a contiguous copy of this object's value.
 * If the value is not already contiguous, it will be copied.
 * This will include the number of keys, the key lengths and the keys
 * along with the value. NOTE: This might be an expensive operation
 * depending on the number of keys and the size of the value
 */
const void*
Object::getKeysAndValue()
{
    if (keysAndValue)
        return keysAndValue;

    return (keysAndValueBuffer)->getRange(keysAndValueOffset,
                                          keysAndValueLength);
}

/**
 * Get number of keys in this object.
 * \return
 *      Number of keys in this object.
 */
KeyCount
Object::getKeyCount()
{
    if (!fillKeyOffsets())
        return 0;
    return keyOffsets->numKeys;
}

/**
 * Obtain a pointer to a contiguous copy of this object's value.
 * This will not contain the number of keys, the key lengths and the keys.
 * This function is primarily used by unit tests
 *
 * \param[out] valueLength
 *      The length of the object's value in bytes.
 *
 * \return
 *      NULL if the object is malformed,
 *      a pointer to a contiguous copy of the object's value otherwise
 */
const void*
Object::getValue(uint32_t *valueLength)
{
    if (!fillKeyOffsets())
        return NULL;

    const CumulativeKeyLength *cumLengths = keyOffsets->cumulativeLengths;
    // To caclulate the starting position of the value, we have to account for
    // the number of keys, all the cumulative length values and total length
    // of all the keys. The total length of all the keys is given by the
    // cumulative length value at the last key position.
    uint32_t valueOffset = KEY_INFO_LENGTH(keyOffsets->numKeys) +
                           cumLengths[keyOffsets->numKeys - 1];
    uint32_t valueLen = keysAndValueLength - valueOffset;

    if (valueLength)
        *valueLength = valueLen;

    // checks for bogus cumulative key length values
    if (valueLen + valueOffset > keysAndValueLength)
        return NULL;

    if (keysAndValue)
        return static_cast<const uint8_t*>(keysAndValue) + valueOffset;
    else
        return keysAndValueBuffer->getRange(keysAndValueOffset + valueOffset,
                                            valueLen);
}

/**
 * Obtain the offset of the object's value in the keysAndValue portion of
 * the object.
 *
 * \param[out] offset
 *      The offset of the value within keysAndValue
 *
 * \return
 *      False if the object is malformed, True otherwise
 */
bool
Object::getValueOffset(uint16_t *offset)
{
    if (!fillKeyOffsets())
        return false;
    const CumulativeKeyLength *cumLengths = keyOffsets->cumulativeLengths;
    // To calculate the starting position of the value, we have to account for
    // the number of keys, all the cumulative length values and total length
    // of all the keys.
    uint32_t valueOffset = KEY_INFO_LENGTH(keyOffsets->numKeys) +
                           cumLengths[keyOffsets->numKeys - 1];
    // IMPORTANT:
    // here we do not add keysAndValueOffset because getValueOffset()
    // is called only after a readKeysAndValueRpc and it should be relative
    // to the starting of keysAndValue
    if (offset)
        *offset = downCast<uint16_t>(valueOffset);
    return true;
}

/**
 * Obtain the length of the object's value
 */
uint32_t
Object::getValueLength()
{
    uint16_t valueOffset;
    if (!getValueOffset(&valueOffset))
        return 0;
    return keysAndValueLength - valueOffset;
}

/**
 * Obtain the length of the keys and the value associated with this object.
 */
uint32_t
Object::getKeysAndValueLength()
{
    return keysAndValueLength;
}

/**
 * Obtain the 64-bit version number associated with this object.
 */
uint64_t
Object::getVersion()
{
    return header.version;
}

/**
 * Obtain the timestamp associated with this object. See WallTime.cc
 * for interpreting the timestamp.
 */
uint32_t
Object::getTimestamp()
{
    return header.timestamp;
}

/**
 * Obtain the total size of the object including the object header
 */
uint32_t
Object::getSerializedLength()
{
    return sizeof32(header) + keysAndValueLength;
}

/**
 * Compute a checksum on the object and determine whether or not it matches
 * what is stored in the object. Returns true if the checksum looks ok,
 * otherwise returns false.
 */
bool
Object::checkIntegrity()
{
    return computeChecksum() == header.checksum;
}

/* Set the version for this object */
void
Object::setVersion(uint64_t version)
{
    header.version = version;
}

/* Set the object creation/modification timestamp for this object */
void
Object::setTimestamp(uint32_t timestamp)
{
    header.timestamp = timestamp;
}

/**
 * Compute the object's checksum and return it.
 */
uint32_t
Object::computeChecksum()
{
    assert(OFFSET_OF(Header, checksum) == 0);

    Crc32C crc;
    // first compute the checksum on the object header excluding the
    // checksum field
    crc.update(reinterpret_cast<void *>(
               reinterpret_cast<uint8_t*>(
               &header) + sizeof(header.checksum)),
               downCast<uint32_t>(sizeof(header) -
               sizeof(header.checksum)));

    // then compute the checksum on keysAndValue.

    if (keysAndValue) {
        crc.update(keysAndValue, keysAndValueLength);
    } else {
        crc.update(*keysAndValueBuffer, keysAndValueOffset,
                   getKeysAndValueLength());
    }

    return crc.getResult();
}

/**
 * Given a pointer to a contiguous Object in memory, compute and return
 * its checksum.
 * \param object
 *      Pointer to the beginning of the object. This object contains the
 *      header and keysAndValue
 * \param totalLength
 *      Total length of the object in bytes, including the header, keys
 *      and value
 */
uint32_t
Object::computeChecksum(const Object::Header* object,
                        uint32_t totalLength)
{
    Crc32C crc;
    crc.update(reinterpret_cast<const void *>(
               reinterpret_cast<const uint8_t *>(
               object) + sizeof(header.checksum)),
               downCast<uint32_t>(sizeof(header) -
               sizeof(header.checksum)));

    uint32_t len = totalLength - sizeof32(Header);
    crc.update(&object->keysAndData[0], len);
    return crc.getResult();
}

/**
 * Construct a new tombstone for a given dead object. Use this constructor
 * when generating new tombstones to be written to the log.
 *
 * The key must not be mutated after this call, since the tombstone checksum
 * is computed during construction.
 *
 * \param object
 *      The dead object this tombstone is marking as deleted.
 * \param segmentId
 *      The 64-bit identifier of the segment in which the object this
 *      tombstone refers to exists. Once this segment is no longer in
 *      the system, this tombstone may be garbage collected.
 * \param timestamp
 *      The creation time of this tombstone, as returned by the WallTime
 *      module. Used primarily by the cleaner to order live objects and
 *      improve future cleaning performance.
 */
ObjectTombstone::ObjectTombstone(Object& object, uint64_t segmentId,
                                 uint32_t timestamp)
    : header(object.getTableId(),
             segmentId,
             object.getVersion(),
             timestamp),
      key(object.getKey()),
      keyLength(object.getKeyLength()),
      tombstoneBuffer(),
      keyOffset(0)
{
    header.checksum = computeChecksum();
}

/**
 * Construct a tombstone object by deserializing an existing tombstone. Use
 * this constructor when reading existing tombstones from the log or from
 * individual log segments.
 *
 * \param buffer
 *      Buffer pointing to a complete serialized tombstone. It is the
 *      caller's responsibility to make sure that the buffer passed in
 *      actually contains a full tombstone. If it does not, then behavior
 *      is undefined.
 * \param offset
 *      Starting offset in the buffer where the tombstone begins.
 * \param length
 *      Total length of the tombstone in bytes.
 */
ObjectTombstone::ObjectTombstone(Buffer& buffer, uint32_t offset,
                                 uint32_t length)
    : header(*buffer.getOffset<Header>(offset)),
      key(),
      keyLength(),
      tombstoneBuffer(&buffer),
      keyOffset(sizeof32(header) + offset)
{
    if (length == 0)
        keyLength = downCast<uint16_t>(buffer.getTotalLength() -
                  offset - sizeof32(Header));
    else
        keyLength = downCast<uint16_t>(length - sizeof32(Header));
}

/**
 * Append the serialized tombstone header and the primary key to the
 * provided buffer.
 *
 * \param buffer
 *      The buffer to append a serialized version of this tombstone to.
 */
void
ObjectTombstone::assembleForLog(Buffer& buffer)
{
    buffer.append(&header, sizeof32(header));
    appendKeyToBuffer(buffer);
}

/**
 * Copy the full serialized tombstone including header and the primary key
 * as it will be stored in the log. This is used when the memory for a
 * tombstone needs to be allocated in a buffer as opposed to being logically
 * stored in a buffer.
 *
 * \param buffer
 *      Destination to copy a serialized version of this tombstone to.
 *      The caller must ensure that enough memory is allocated for the
 *      complete serialized tombstone to be stored
 */
void
ObjectTombstone::assembleForLog(void* buffer)
{
    uint8_t *dst = reinterpret_cast<uint8_t*>(buffer);
    memcpy(dst, &header, sizeof32(header));
    memcpy(dst + sizeof32(header), getKey(), getKeyLength());
}

/**
 * Append the primary key portion of this tombstone to a provided
 * buffer. This is only the key blob and does not contain the table
 * identifier.
 *
 * \param buffer
 *      The buffer to append the primary key to.
 */
void
ObjectTombstone::appendKeyToBuffer(Buffer& buffer)
{
    if (key) {
        buffer.append(key, getKeyLength());
        return;
    }

    Buffer::Iterator it(tombstoneBuffer,
                        keyOffset,
                        getKeyLength());
    while (!it.isDone()) {
        buffer.append(it.getData(), it.getLength());
        it.next();
    }
}

/**
 * Obtain the 64-bit table identifier associated with this tombstone.
 */
uint64_t
ObjectTombstone::getTableId()
{
    return header.tableId;
}

/**
 * Obtain a pointer to a contiguous copy of this tombstone's primary
 * key. Note that if the key is not already contiguous, it will be copied.
 */
const void*
ObjectTombstone::getKey()
{
    if (key)
        return key;

    return (tombstoneBuffer)->getRange(keyOffset, getKeyLength());
}

/**
 * Obtain the length of this tombstone's primary key.
 */
uint16_t
ObjectTombstone::getKeyLength()
{
    return keyLength;
}

uint64_t
ObjectTombstone::getSegmentId()
{
    return header.segmentId;
}

/**
 * Obtain the 64-bit version number associated with the object this
 * tombstone is making the deletion of.
 */
uint64_t
ObjectTombstone::getObjectVersion()
{
    return header.objectVersion;
}

/**
 * Obtain the timestamp associated with this tombstone. See WallTime.cc
 * for interpreting the timestamp.
 */
uint32_t
ObjectTombstone::getTimestamp()
{
    return header.timestamp;
}

/**
 * Compute a checksum on the object and determine whether or not it matches
 * what is stored in the object. Returns true if the checksum looks ok,
 * otherwise returns false.
 */
bool
ObjectTombstone::checkIntegrity()
{
    return computeChecksum() == header.checksum;
}

/**
 * Given the length of a prospective tombstone's primary key compute
 * the exact byte byte length of such a serialized tombstone.
 */
uint32_t
ObjectTombstone::getSerializedLength(uint32_t keyLength)
{
    return sizeof32(Header) + keyLength;
}

/**
 * Compute the total length of a serialized tombstone
 */
uint32_t
ObjectTombstone::getSerializedLength()
{
    return sizeof32(Header) + getKeyLength();
}

/**
 * Compute the tombstone's checksum and return it.
 */
uint32_t
ObjectTombstone::computeChecksum()
{
    assert(OFFSET_OF(Header, checksum) ==
        (sizeof(header) - sizeof(header.checksum)));

    Crc32C crc;
    crc.update(&header,
               downCast<uint32_t>(OFFSET_OF(Header, checksum)));

    if (key) {
        crc.update(key, getKeyLength());
    } else {
        crc.update(*tombstoneBuffer,
                   keyOffset,
                   getKeyLength());
    }

    return crc.getResult();
}

/**
 * Construct a safeVersion objectg
 *
 * \param safeVer
 *      safeVersion value.
 */
ObjectSafeVersion::ObjectSafeVersion(const uint64_t safeVer)
    : header(safeVer)
{
    header.checksum = computeChecksum();
}

/**
 * Construct a safeVersion object by deserializing an existing
 * safeVersion.
 * Use this constructor when reading existing safeVersion from
 * the log or from individual log segments.
 *
 * \param buffer
 *      Buffer pointing to a complete serialized safeVersion. It is the
 *      caller's responsibility to make sure that the buffer passed in
 *      actually contains a full safeVersion. If it does not, then behavior
 *      is undefined.
 */
ObjectSafeVersion::ObjectSafeVersion(Buffer& buffer)
    : header(*buffer.getStart<Header>())
{
}

/**
 * Append the serialized safeVersion to the provided buffer.
 * Note that safeVersion is header only.
 *
 * \param buffer
 *      The buffer to append a serialized version of this safeVersion to.
 */
void
ObjectSafeVersion::assembleForLog(Buffer& buffer)
{
    buffer.append(&header, sizeof32(header));
}

/**
 * Compute a checksum on the object and determine whether or not it matches
 * what is stored in the object. Returns true if the checksum looks ok,
 * otherwise returns false.
 */
bool
ObjectSafeVersion::checkIntegrity()
{
    return computeChecksum() == header.checksum;
}

/**
 * Given the length of a prospective
 * the exact byte byte length of such a serialized safeVersion.
 */
uint32_t
ObjectSafeVersion::getSerializedLength()
{
    return sizeof32(Header);
}

/**
 * Get safeVersion value
 */
uint64_t
ObjectSafeVersion::getSafeVersion() const
{
    return header.safeVersion;
}

/**
 * Compute the safeVersion's checksum and return it.
 */
uint32_t
ObjectSafeVersion::computeChecksum()
{
    assert(OFFSET_OF(Header, checksum) ==
        (sizeof(header) - sizeof(header.checksum)));

    Crc32C crc;
    crc.update(&header,
               downCast<uint32_t>(OFFSET_OF(Header, checksum)));
    return crc.getResult();
}

} // namespace RAMCloud
