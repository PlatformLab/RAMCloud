/* Copyright (c) 2010-2012 Stanford University
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

#ifndef RAMCLOUD_OBJECT_H
#define RAMCLOUD_OBJECT_H

#include "Common.h"
#include "Buffer.h"
#include "Crc32C.h"
#include "Key.h"
#include "RamCloud.h"

namespace RAMCloud {

/**
 * This class defines the format of an object stored in the log and provides
 * methods to easily construct new ones to be appended and interpret ones that
 * have already been written. 
 *
 * In other words, this code centralizes the format and parsing of objects
 * (essentially serialization and deserialization). Different constructors
 * serve these two purposes.
 *
 * Objects basically contain keys, some additional metadata about the keys,
 * and an associated binary blob of data. When serialized in the log,
 * objects simply consist of a common header, followed immediately by the
 * number of keys in the object, the lengths of the keys, the keys and
 * finally the data. For example:
 *
 * +---------------+--------+---------------------+-----------+------------+
 * | Object Header | # keys | End Key Offsets ... | Keys .... | Data ...   |
 * +---------------+--------+---------------------+-----------+------------+
 *                                                 <--RANGE-->
 * Everything except the header and the number of keys is of variable length.
 * End key offset values are in the range given by RANGE above (0 to 64K - 1).
 * So, the constraint is that the total length of all the keys in an object has
 * to be <= 64K.
 * Offset 0 corresponds to the starting of the 0th key (primary key). The
 * length of any key can be calculated using these equations,
 *
 * Length_0 = EndKeyOffset_0 + 1
 * Length_i = max(0, EndKeyOffset_i - EndKeyOffset_i-1) for i >= 1
 *
 * If Key_i is not present, then EndKeyOffset_i = EndKeyOffset_i-1.
 * Consequently, Length_i = 0
 *
 * When creating objects, one will typically gather and compute the necessary
 * fields (tableId, number of keys, keys, version, data associated with the
 * object etc), and create an instance of this class describing that object.
 * This may then be serialized to a buffer and written to the log.
 *
 * When reading objects from the log (or from a segment of the log), one will
 * typically get a buffer referring to an object from a segment or log iterator.
 * Constructing an instance of this class with that buffer will allow the user
 * to deserialize it and access all of its fields and contents.
 *
 * NOTE: The methods/fields of this class treat 'Data' as the number of keys,
 * key lengths, keys and the value of the object together. Unless explicitly
 * noted otherwise, this is the meaning of Data in the context of this class.
 */

typedef uint8_t KeyCount; // the number of keys in an object
typedef uint16_t KeyLength; // the length of a key in an object

class Object {
  public:
    /**
     * Construct an Object. This form is used when the header information is
     * available in individual pieces, while the keys and data are stored in
     * a Buffer (typical use: during write RPCs). MasterService will call this
     * constructor to create an object with a temporary invalid version and
     * timestamp. ObjectManager.writeObject() will then update the version,
     * timestamp and the checksum
     *
     * The keys and data must not be mutated after this call, since the object
     * checksum is computed during construction.
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
     * \param dataBuffer
     *      Buffer containing all chunks that will comprise this object's
     *      number of keys, key lengths, keys and value.
     * \param dataOffset
     *      Byte offset in the buffer where keysAndValue start
     */
    Object(uint64_t tableId,
           uint64_t version,
           uint32_t timestamp,
           Buffer& dataBuffer,
           uint32_t dataOffset = 0)
        : header(tableId,
                 timestamp,
                 version),
          keysAndValueLength(),
          data(),
          dataBuffer(&dataBuffer),
          dataOffset(dataOffset)
    {
        keysAndValueLength = dataBuffer.getTotalLength() - dataOffset;
        // Don't compute the checksum now. Invocation of this constructor
        // happens during the write RPC. ObjectManager.writeObject() will
        // update the checksum after updating the version and the timestamp
    }

    /**
     * Construct a new object that we can later serialize. Use this constructor
     * when the data for an object is contiguous and the header information is
     * available in individual pieces. This is primarily used by unit tests
     * and the write RPC to construct objects with just a single key.
     * It is also used by the incremement RPC handler in MasterService.
     * The main function that will be invoked after a call to this constructor
     * is assembleForLog.
     *
     * The keys and data must not be mutated after this call, since the object
     * checksum is computed during construction.
     *
     * \param key
     *      Primary key for this object, describing its tableId and binary string
     *      key.
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
     *      The buffer to append the keys and value to. It's lifetime should
     *      be at least as much as the object's lifetime
     * \param [out] length
     *      Total length of keysAndValue
     */
    Object(Key& key,
           const void* value,
           uint32_t valueLength,
           uint64_t version,
           uint32_t timestamp,
           Buffer& buffer,
           uint32_t *length = NULL)
        : header(key.getTableId(),
                 timestamp,
                 version),
          keysAndValueLength(),
          data(),
          dataBuffer(),
          dataOffset(buffer.getTotalLength())
          // dataOffset(buffer.getTotalLength()) because we are appending to
          // buffer. The offset where this object data begins wil be the
          // current size of the buffer.
    {
        uint32_t keyInfoLength = sizeof32(KeyCount) + sizeof32(KeyLength)
                                    + key.getStringKeyLength();

        keysAndValueLength = keyInfoLength + valueLength;
        if (length)
            *length = keysAndValueLength;

        uint8_t *keyInfo = new(&buffer, APPEND) uint8_t[keyInfoLength];

        KeyCount keyCount = 1;
        KeyLength endKeyOffset = static_cast<KeyLength>
                                    (key.getStringKeyLength() - 1);
        const void *keyString = key.getStringKey();
        memcpy(keyInfo, &keyCount, sizeof(KeyCount));
        memcpy(keyInfo + sizeof(KeyCount), &endKeyOffset, sizeof(KeyLength));
        memcpy(keyInfo + sizeof(KeyCount) + sizeof(KeyLength), keyString,
                    endKeyOffset + 1);

        buffer.append(value, valueLength);
        dataBuffer = &buffer;

        // The checksum will be updated when assembleForLog() is called.
    }

    /**
     * Construct a new object that we can later serialize. Use this constructor
     * during a write RPC when we want the RPC payload to mirror the format of
     * the object in the log as much as possible. This is primarily used by the
     * RamCloud library to make sure the format of the object does not leak
     * outside this class. There is no real use for the object after a call to
     * this constructor. 
     *
     * The keys and data must not be mutated after this call, since the object
     * \param numKeys
     *      The number of keys in this object.
     * \param tableId
     *      The tableId corresponding to this object.
     * \param keyList
     *      List of keys and key length values as provided by the end client
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
     * \param [out] request
     *      The buffer to append the keys and value to. It's lifetime should
     *      be at least as much as the object's lifetime
     * \param [out] length
     *      Total length of keysAndValue
     */
    Object(KeyCount numKeys,
           uint64_t tableId,
           KeyInfo *keyList,
           const void* value,
           uint32_t valueLength,
           uint64_t version,
           uint32_t timestamp,
           Buffer& request,
           uint32_t *length = NULL)
        : header(tableId,
                 timestamp,
                 version),
          keysAndValueLength(),
          data(),
          dataBuffer(),
          dataOffset(request.getTotalLength())
    {
        int i;
        uint32_t totalLength = 0;
        uint16_t currentKeyLength = 0;
        if (keyList) {
            // allocate memory first for number of keys and all the key offset
            // values
            KeyCount *numKeysPtr = reinterpret_cast<KeyCount *>(new(&request,
                                        APPEND) uint8_t[sizeof32(KeyCount) +
                                        numKeys * sizeof32(KeyLength)]);
            *numKeysPtr = numKeys;
            KeyLength *endKeyOffsets = reinterpret_cast<KeyLength *>(
                                        numKeysPtr + sizeof32(KeyCount));
            for (i = 0; i < numKeys; i++) {
                // if the length of a key is 0, we expect the corresponding key
                // is NULL terminated and hence compute the length

                if (!keyList[i].key) { // this key does not exist
                    currentKeyLength = 0;
                } else if (!keyList[i].keyLength) {
                    currentKeyLength = static_cast<uint16_t>
                                            (strlen(static_cast<const char *>(
                                            keyList[i].key)));
                } else {
                    currentKeyLength = keyList[i].keyLength;
                }
                // primary key must always exist
                if (i == 0)
                    endKeyOffsets[i] = static_cast<uint16_t>(
                                        currentKeyLength - 1);
                else
                    endKeyOffsets[i] = static_cast<uint16_t>(
                                        endKeyOffsets[i-1] + currentKeyLength);
                totalLength += currentKeyLength;
            }
            void *keys = new(&request, APPEND) uint8_t[totalLength];
            uint8_t *dest = reinterpret_cast<uint8_t *>(keys);
            for (i = 0; i < numKeys; i++) {
                // this key doesn't exist
                if (!keyList[i].key)
                    continue;

                if (!keyList[i].keyLength) {
                    currentKeyLength = static_cast<uint16_t>
                                            (strlen(static_cast<const char *>(
                                            keyList[i].key)));
                } else {
                    currentKeyLength = keyList[i].keyLength;
                }
                memcpy(dest, keyList[i].key, currentKeyLength);
                dest = dest + currentKeyLength;
            }
            request.append(value, valueLength);
            keysAndValueLength = totalLength +
                                    numKeys * sizeof32(KeyLength) +
                                    valueLength + sizeof32(numKeys);
            dataBuffer = &request;
            if (length)
                *length = keysAndValueLength;
        }
        // The checksum will be updated when assembleForLog() is called.
    }

    /**
     * Construct an object using information in the log, which includes the
     * object header as well as the keys and data.
     *
     * \param buffer
     *      Buffer referring to a complete object in the log. It is the
     *      caller's responsibility to make sure that the buffer passed in
     *      actually contains a full object. If it does not, then behavior
     *      is undefined.
     */
    explicit Object(Buffer& buffer)
        : header(*buffer.getStart<Header>()),
          keysAndValueLength(buffer.getTotalLength() -
                     sizeof32(header)),
          data(),
          dataBuffer(&buffer),
          dataOffset(sizeof32(header))
    {
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
    Object(const void* buffer, uint32_t length)
        : header(*reinterpret_cast<const Header*>(buffer)),
          keysAndValueLength(length - sizeof32(header)),
          data(reinterpret_cast<const void*>(reinterpret_cast<const uint8_t*>(
              buffer) + sizeof32(header))),
          dataBuffer(),
          dataOffset(0)
    {
    }

    /**
     * Append the full object, including header, keys and value exactly as it
     * should be stored
     * in the log, to a buffer
     *
     * \param buffer
     *      The buffer to append a serialized version of this object to.
     */
    void
    assembleForLog(Buffer& buffer)
    {
        header.checksum = computeChecksum();
        buffer.append(&header, sizeof32(header));
        appendKeysAndValueToBuffer(buffer);
    }

    /**
     * Append the keyLengths, the keys and the value associated with this
     * object to a provided buffer.
     *
     * \param buffer
     *      The buffer to append the keys and the value to.
     */
    void
    appendKeysAndValueToBuffer(Buffer& buffer)
    {
        if (data) {
            buffer.append(data, keysAndValueLength);
            return;
        }

        // dataBuffer contains keyLengths, keys and value starting
        // at dataOffset
        Buffer* sourceBuffer = dataBuffer;

        Buffer::Iterator it(*sourceBuffer, dataOffset, keysAndValueLength);
        while (!it.isDone()) {
            buffer.append(it.getData(), it.getLength());
            it.next();
        }
    }

    /**
     * Obtain the 64-bit table identifier associated with this object.
     */
    uint64_t
    getTableId()
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
     *      Length of the corresponding key if a valid pointer is passed
     *
     * \return
     *      Pointer to the key which will be contiguous
     */
    const void*
    getKey(KeyCount keyIndex = 0, KeyLength *keyLength = NULL)
    {
        KeyCount numKeys;
        const void *objectData = getKeysAndValue();

        // skip ahead past the number of keys
        numKeys = *(reinterpret_cast<const KeyCount *>(objectData));

        if (keyIndex >= numKeys)
            return NULL;

        uint32_t totalKeyLength = numKeys * sizeof32(KeyLength);
        KeyLength keyLen = getKeyLength(keyIndex);
        if (keyLength)
            *keyLength = keyLen;

        // key does not exist
        if (keyLen == 0)
            return NULL;

        if (keyIndex == 0) {
            return reinterpret_cast<const void *>(
                         reinterpret_cast<const uint8_t *>(objectData) +
                         sizeof32(KeyCount) + totalKeyLength);
        } else {
            return reinterpret_cast<const void *>(
                         reinterpret_cast<const uint8_t *>(objectData) +
                         getEndKeyOffset(static_cast<KeyCount>(keyIndex - 1))
                         + 1 + sizeof32(KeyCount) + totalKeyLength);
        }
    }

    /**
     * Obtain the length of the key at position keyIndex
     */
    uint16_t
    getKeyLength(KeyCount keyIndex = 0)
    {
        if (keyIndex == 0) {
            return static_cast<uint16_t>(getEndKeyOffset(keyIndex) + 1);
        } else {
            // if keyIndex >= numKeys, then endKeyOffset(keyIndex) will
            // return 0. This function should return 0 in such cases.
            int keyOffset = getEndKeyOffset(keyIndex);
            int prevKeyOffset = getEndKeyOffset(static_cast<KeyCount>(
                                        keyIndex - 1));
            if (keyOffset - prevKeyOffset > 0)
                return static_cast<uint16_t>(keyOffset - prevKeyOffset);
            else
                return 0;
        }
    }

    /**
     * Obtain the ending offset of the key at position keyIndex. This
     * function is usually not invoked from outside this class.
     * Offset 0 corresponds to starting of key 0
     */
    uint16_t
    getEndKeyOffset(KeyCount keyIndex = 0)
    {
        uint32_t offset = sizeof32(KeyCount);
        if (data) {
            // to skip ahead past the number of keys
            const KeyCount numKeys = *(reinterpret_cast<const KeyCount *>(
                                            data));

            if (keyIndex >= numKeys)
                return 0;
            const KeyLength *endKeyOffset = reinterpret_cast<const KeyLength *>(
                                       reinterpret_cast<const KeyCount *>(data)
                                       + offset +
                                       keyIndex * sizeof32(KeyLength));
            return *endKeyOffset;
        }

        Buffer *buffer = dataBuffer;
        offset += dataOffset;

        // not 0 , dataOffset
        const KeyCount numKeys = *(buffer->getOffset<KeyCount>(dataOffset));

        // invalid index
        if (keyIndex >= numKeys)
            return 0;

        const KeyLength endKeyOffset = *(buffer->getOffset<KeyLength>(
                                offset + keyIndex * sizeof32(
                                KeyLength)));
        return endKeyOffset;
    }

    /**
     * Obtain a pointer to a contiguous copy of this object's data. Note
     * that if the value is not already contiguous, it will be copied.
     * This will include the number of keys, the key lengths and the keys
     * along with the value.
     */
    const void*
    getKeysAndValue()
    {
        if (data)
            return data;

        return (dataBuffer)->getRange(dataOffset, keysAndValueLength);
    }

    /**
     * Obtain a pointer to a contiguous copy of this object's value.
     * This will not contain the number of keys, the key lengths and the keys.
     * This function is primarily used by unit tests
     */
    const void*
    getValue(uint32_t *valueLength = NULL)
    {

        const void *objectData = getKeysAndValue();
        const KeyCount numKeys = *(reinterpret_cast<const KeyCount *>(
                                  objectData));
        // value begins immediately after where the last key ends.
        uint16_t valueOffset = static_cast<uint16_t>(sizeof32(numKeys) +
                                    numKeys * sizeof32(KeyLength) +
                                    getEndKeyOffset(static_cast<KeyCount>(
                                        numKeys - 1)) + 1);
        uint32_t valueLen = keysAndValueLength - valueOffset;

        if (valueLength)
            *valueLength = valueLen;

        return reinterpret_cast<const void *>(
                         reinterpret_cast<const KeyCount *>(objectData) +
                         valueOffset);
    }

    /**
     * Obtain the offset of the object's value in the keysAndValue portion of
     * the object.
     */
    uint16_t
    getValueOffset()
    {
        KeyCount numKeys;
        if (data) {
            numKeys = *(reinterpret_cast<const KeyCount *>(data));
        } else {
            numKeys = *(dataBuffer->getOffset<KeyCount>(dataOffset));
        }
        // value begins immediately after where the last key ends.
        uint16_t valueOffset = static_cast<uint16_t>(sizeof32(numKeys) +
                                    numKeys * sizeof32(KeyLength) +
                                    getEndKeyOffset(static_cast<KeyCount>(
                                        numKeys - 1)) + 1);
        return valueOffset;
    }

    /**
     * Obtain the length of the keys and the value associated with this object. 
     */
    uint32_t
    getKeysAndValueLength()
    {
        return keysAndValueLength;
    }

    /**
     * Obtain the 64-bit version number associated with this object.
     */
    uint64_t
    getVersion()
    {
        return header.version;
    }

    /**
     * Obtain the timestamp associated with this object. See WallTime.cc
     * for interpreting the timestamp.
     */
    uint32_t
    getTimestamp()
    {
        return header.timestamp;
    }

    /**
     * Compute a checksum on the object and determine whether or not it matches
     * what is stored in the object. Returns true if the checksum looks ok,
     * otherwise returns false.
     */
    bool
    checkIntegrity()
    {
        return computeChecksum() == header.checksum;
    }

    /* Set the version for this object */
    void
    setVersion(uint64_t version)
    {
        header.version = version;
    }

    /* Set the object creation/modification timestamp for this object */
    void
    setTimestamp(uint32_t timestamp)
    {
        header.timestamp = timestamp;
    }

//  PRIVATE:
    /**
     * This data structure defines the format of an object stored in a master
     * server's log. When writing an object, the fields below are written
     * first, then the binary string key, and finally the object's value are
     * written sequentially.
     */
    class Header {
      public:
        /**
         * Construct a serialized object header.
         *
         * \param tableId
         *      The 64-bit identifier for the table this object is in.
         * \param timestamp
         *      The creation time of this object, as returned by the WallTime
         *      module. Used primarily by the cleaner to order live objects and
         *      improve future cleaning performance.
         * \param version
         *      64-bit version number associated with this object.
         */
        Header(uint64_t tableId,
                       uint32_t timestamp,
                       uint64_t version)
            : checksum(0),
              timestamp(timestamp),
              version(version),
              tableId(tableId)
        {
        }

        /// CRC32C checksum covering everything but this field, including the
        /// keys and the value.
        uint32_t checksum;

        /// Object creation/modification timestamp. WallTime.cc is the clock.
        uint32_t timestamp;

        /// Version of the object. Set to some initial value upon object
        /// creation and incremented by one for each modification. See
        /// MasterService for the exact behavior.
        uint64_t version;

        /// Table to which this object belongs.
        uint64_t tableId;

        /// Following this class will be the number of keys, key lengths,
        /// the keys and finally the value. This member is only here to denote
        /// this.
        char keyAndData[0];
    } __attribute__((__packed__));
    static_assert(sizeof(Header) == 24,
        "Unexpected serialized Object size");

    /**
     * Compute the object's checksum and return it.
     */
    uint32_t
    computeChecksum()
    {
        assert(OFFSET_OF(Header, checksum) == 0);

        Crc32C crc;
        crc.update(reinterpret_cast<void *>(reinterpret_cast<uint8_t*>(
                        &header) + sizeof(header.checksum)),
                   downCast<uint32_t>(sizeof(header) -
                        sizeof(header.checksum)));

        if (data) {
            crc.update(data, keysAndValueLength);
        } else {
            crc.update(*dataBuffer, dataOffset, getKeysAndValueLength());
        }

        return crc.getResult();
    }

    /**
     * Given a pointer to a contiguous Object in memory, compute and return
     * its checksum.
     *
     * \param object
     *      Pointer to the beginning of the object.
     * \param totalLength
     *      Total length of the object in bytes, including the header, keys
     *      and value
     */
    static uint32_t
    computeChecksum(const Object::Header* object,
                        uint32_t totalLength)
    {
        Crc32C crc;
        crc.update(reinterpret_cast<const void *>(
                   reinterpret_cast<const uint8_t *>(
                   object) + sizeof(header.checksum)),
                    downCast<uint32_t>(sizeof(header) -
                    sizeof(header.checksum)));

        uint32_t dataLen = totalLength - sizeof32(Header);
        crc.update(&object->keyAndData[0], dataLen);
        return crc.getResult();
    }

    /// Copy of the object header that is in, or will be written to, the log.
    Header header;

    /// Length that includes the number of keys, the key lengths, the keys
    /// and the value. Header since it can be trivially computed
    /// as needed.
    uint32_t keysAndValueLength;

    /// If an object is created such that the value all lies in a single
    /// contiguous buffer, this will point there. This is a tub because
    /// some tests create objects with a NULL value. So, we want to
    /// distinguish between an unitialized value and a NULL value.
    const void* data;

    /// If an object is created such that the key lengths, the keys and the
    /// value portion is referred to by a Buffer, this will point to that
    /// buffer.
    Buffer* dataBuffer;

    /// The byte offset in the dataBuffer where keysAndValue start
    uint32_t dataOffset;

    DISALLOW_COPY_AND_ASSIGN(Object);
};

/**
 * This class describes the format of a tombstone stored in the log and provides
 * methods to easily construct new ones to be appended and interpret ones that
 * have already been written. 
 *
 * In other words, this code centralizes the format and parsing of tombstones
 * (essentially serialization and deserialization). Different constructors serve
 * these two purposes.
 *
 * Tombstones serve as records indicating that specific versions of objects have
 * been removed from the system (explicitly due to deletions, or implicitly due
 * to overwrites). They are necessary to avoid resurrecting previously-deleted
 * objects that are still in the log during failure recovery.
 *
 * Internally, tombstones are basically a binary string key and some additional
 * metadata. When serialized in the log, tombstones simply consist of a common
 * header, followed immediately by the binary string key. The header is of fixed
 * size, while the latter string is of variable length. For example:
 *
 *             +--------------------------+---------------------+
 *             |     Tombstone Header     |    String Key . . . |
 *             +--------------------------+---------------------+
 *               sizeof(Header)      variable length
 *
 * When creating tombstones, one will typically gather the necessary fields by
 * creating an object first (often to deserialize from what's stored in the log)
 * and then create an instance of this class describing that dead object. The
 * resulting tombstone may then be serialized to a buffer and written to the
 * log.
 *
 * When reading tombstones from the log (or from a segment of the log), one will
 * typically get a buffer referring to a tombstone from a segment or log
 * iterator. Constructing an instance of this class with that buffer will allow
 * the user to deserialize it and access all of its fields and contents.
 */
class ObjectTombstone {
  public:
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
    ObjectTombstone(Object& object, uint64_t segmentId, uint32_t timestamp)
        : header(object.getTableId(),
                         segmentId,
                         object.getVersion(),
                         timestamp),
          key(object.getKey()),
          keyLength(object.getKeyLength()),
          tombstoneBuffer()
    {
        header.checksum = computeChecksum();
    }

    /**
     * Construct a tombstone object by deserializing an existing tombstone. Use
     * this constructor when existing reading tombstones from the log or from
     * individual log segments.
     *
     * \param buffer
     *      Buffer pointing to a complete serialized tombstone. It is the
     *      caller's responsibility to make sure that the buffer passed in
     *      actually contains a full tombstone. If it does not, then behavior
     *      is undefined.
     */
    explicit ObjectTombstone(Buffer& buffer)
        : header(*buffer.getStart<Header>()),
          key(),
          keyLength(downCast<uint16_t>(buffer.getTotalLength() -
                    sizeof32(Header))),
          tombstoneBuffer(&buffer)
    {
    }

    /**
     * Append the serialized tombstone header and binary string key to the
     * provided buffer.
     *
     * \param buffer
     *      The buffer to append a serialized version of this tombstone to.
     */
    void
    assembleForLog(Buffer& buffer)
    {
        buffer.append(&header, sizeof32(header));
        appendKeyToBuffer(buffer);
    }

    /**
     * Append the binary string key portion of this tombstone to a provided
     * buffer. This is only the key blob and does not contain the table
     * identifier.
     *
     * \param buffer
     *      The buffer to append the binary string key to.
     */
    void
    appendKeyToBuffer(Buffer& buffer)
    {
        if (key) {
            buffer.append(getKey(), getKeyLength());
            return;
        }

        Buffer::Iterator it(*tombstoneBuffer,
                            sizeof32(header),
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
    getTableId()
    {
        return header.tableId;
    }

    /**
     * Obtain a pointer to a contiguous copy of this tombstone's binary string
     * key. Note that if the key is not already contiguous, it will be copied.
     */
    const void*
    getKey()
    {
        if (key)
            return key;

        return (tombstoneBuffer)->getRange(
            sizeof(header), getKeyLength());
    }

    /**
     * Obtain the length of this tombstone's binary string key.
     */
    uint16_t
    getKeyLength()
    {
        return keyLength;
    }

    uint64_t
    getSegmentId()
    {
        return header.segmentId;
    }

    /**
     * Obtain the 64-bit version number associated with the object this
     * tombstone is making the deletion of.
     */
    uint64_t
    getObjectVersion()
    {
        return header.objectVersion;
    }

    /**
     * Obtain the timestamp associated with this tombstone. See WallTime.cc
     * for interpreting the timestamp.
     */
    uint32_t
    getTimestamp()
    {
        return header.timestamp;
    }

    /**
     * Compute a checksum on the object and determine whether or not it matches
     * what is stored in the object. Returns true if the checksum looks ok,
     * otherwise returns false.
     */
    bool
    checkIntegrity()
    {
        return computeChecksum() == header.checksum;
    }

    /**
     * Given the length of a prospective tombstone's binary string key compute
     * the exact byte byte length of such a serialized tombstone.
     */
    static uint32_t
    getSerializedLength(uint32_t keyLength)
    {
        return sizeof32(Header) + keyLength;
    }

  //PRIVATE:
    /**
     * This data structure defines the format of an object's tombstone stored
     * in a master server's log. When writing a tombstone, the fields below are
     * written first, then the binary string key of the dead object.
     */
    class Header {
      public:
        /**
         * Construct a serialized object tombstone header.
         *
         * \param tableId
         *      The 64-bit identifier for the table the dead object was in.
         * \param segmentId
         *      64-bit identifier of the log segment the dead object is in.
         * \param objectVersion
         *      64-bit version number associated with the dead object.
         * \param timestamp
         *      The creation time of this tombstone, as returned by the WallTime
         *      module. Used primarily by the cleaner to order live objects and
         *      improve future cleaning performance.
         */
        Header(uint64_t tableId,
                       uint64_t segmentId,
                       uint64_t objectVersion,
                       uint32_t timestamp)
            : tableId(tableId),
              segmentId(segmentId),
              objectVersion(objectVersion),
              timestamp(timestamp),
              checksum(0)
        {
        }

        /// Table to which this object belongs. A (TableId, StringKey) tuple
        /// uniquely identifies a live object.
        uint64_t tableId;

        /// The log segment that the dead object this tombstone refers to was
        /// in. Once this segment is no longer in the system, this tombstone
        /// is no longer necessary and may be garbage collected.
        uint64_t segmentId;

        /// Version number of the dead object. The version ties this tombstone
        /// to a unique instance of an object.
        uint64_t objectVersion;

        /// Tombstone creation timestamp. WallTime.cc is the clock.
        uint32_t timestamp;

        /// CRC32C checksum covering everything but this field, including the
        /// key.
        uint32_t checksum;

        /// Following this class will be the key. This member is only here to
        /// denote this.
        char key[0];
    } __attribute__((__packed__));
    static_assert(sizeof(Header) == 32,
        "Unexpected serialized ObjectTombstone size");

    /**
     * Compute the tombstone's checksum and return it.
     */
    uint32_t
    computeChecksum()
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
                       sizeof(header),
                       getKeyLength());
        }

        return crc.getResult();
    }

    /// Copy of the tombstone header that is in, or will be written to, the log.
    Header header;

    /// Pointer to the binary string key for this object.
    const void* key;

    /// Length of the key corresponding to this tombstone. This isn't stored in
    /// Header since it can be trivially computed as needed.
    uint16_t keyLength;

    /// If a tombstone is being read from a serialized copy (for instance, from
    /// the log), this will point to the buffer that refers to the entire
    /// tombstone.
    Buffer* tombstoneBuffer;

    DISALLOW_COPY_AND_ASSIGN(ObjectTombstone);
};

/**
 *  A log entry to record safeVersion number for recovery.
 *  See \see #safeVersion in Log.h .
 *   
 *  When objects move from one server to another
 *  (for example, during recovery) the receiving server must update
 *  its safeVersion to be at least as great as the safeVersion on the
 *  server from which the objects came. This is necessary so that
 *  if an object from the old server is re-created on the new server,
 *  it won't reuse an old version number.
 *
 *  It's possible that ownership of a range of objects may
 *  move to a new server without any actual objects moving
 *  (if there were none in that range). However, the version number
 *  must still be updated to handle reincarnation of objects that 
 *  once existed within that range.
 *
 *  When a new segment is created, this log entry is inserted to 
 *  the segment and backed up.
 *  
 *  ObjectSafeVersion contains a header with uint64 number.
 *  It does not have an extension part.
 **/
class ObjectSafeVersion {
  public:
    explicit ObjectSafeVersion(const uint64_t safeVer)
       :  header(safeVer)
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
    explicit ObjectSafeVersion(Buffer& buffer)
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
    assembleForLog(Buffer& buffer)
    {
        buffer.append(&header, sizeof32(header));
    }

    /**
     * Compute a checksum on the object and determine whether or not it matches
     * what is stored in the object. Returns true if the checksum looks ok,
     * otherwise returns false.
     */
    bool
    checkIntegrity()
    {
        return computeChecksum() == header.checksum;
    }

    /**
     * Given the length of a prospective 
     * the exact byte byte length of such a serialized safeVersion.
     */
    static uint32_t
    getSerializedLength()
    {
        return sizeof32(Header);
    }

    /** 
     * Get safeVersion value
     */

    uint64_t getSafeVersion() const
    {
        return header.safeVersion;
    }

  PRIVATE:
    /**
     * This data structure defines the format of an object's safeVersion stored
     * in a master server's log. When writing a safeVersion, the fields below are
     * only written.
     */
    class Header {
      public:
        /**
         * Construct a serialized object safeVersion, which is a header
         * only object.
         *
         * \param safeVersion
         *      The 64-bit identifier for the table the dead object was in.
         */
        explicit Header(uint64_t safeVersion)
                : safeVersion(safeVersion),
                  checksum(0)
        {
        }

        // Saved safeVersion to recover the safe version number for
        // new object creation considering reincarnation.
        // See the descrition of this class.
        uint64_t safeVersion;

        /// CRC32C checksum covering everything but this field, including the
        /// key.
        uint32_t checksum;
    } __attribute__((__packed__));
    static_assert(sizeof(Header) == 12,
        "Unexpected serialized ObjectSafeVersion size");

    /**
     * Compute the safeVersion's checksum and return it.
     */
    uint32_t
    computeChecksum()
    {
        assert(OFFSET_OF(Header, checksum) ==
            (sizeof(header) - sizeof(header.checksum)));

        Crc32C crc;
        crc.update(&header,
                   downCast<uint32_t>(OFFSET_OF(Header, checksum)));
        return crc.getResult();
    }

    /// Copy of the safeVersion header that is in,
    /// or will be written to, the log.
    Header header;

    /// No pointer to safeVersion body since no body exist.
    // Tub<Buffer*> safeVersionBuffer;

    DISALLOW_COPY_AND_ASSIGN(ObjectSafeVersion);
};

} // namespace RAMCloud

#endif
