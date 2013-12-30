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

namespace RAMCloud {

/**
 * This class describes the format of an object stored in the log and provides
 * methods to easily construct new ones to be appended and interpret ones that
 * have already been written. 
 *
 * In other words, this code centralizes the format and parsing of objects
 * (essentially serialization and deserialization). Different constructors serve
 * these two purposes. 
 *
 * Objects are basically a key, some additional metadata, and an associated
 * binary blob of data. When serialized in the log, objects simply consist of
 * a common header, followed immediately by the binary string key, and then the
 * data. The header is of fixed size, while the latter two are variable in
 * length. For example:
 *
 * +-----------------------+---------------------+---------------------------+
 * |     Object Header     |    String Key . . . |          Data . . .       |
 * +-----------------------+---------------------+---------------------------+
 *  sizeof(SerializedForm)     variable length           variable length
 *
 * When creating objects, one will typically gather and compute the necessary
 * fields (tableId, binary string key, version, data associated with the object,
 * etc), and create an instance of this class describing that object. This may
 * then be serialized to a buffer and written to the log.
 *
 * When reading objects from the log (or from a segment of the log), one will
 * typically get a buffer referring to an object from a segment or log iterator.
 * Constructing an instance of this class with that buffer will allow the user
 * to deserialize it and access all of its fields and contents.
 */
class Object {
  public:
    /**
     * Construct a new object that we can later serialize. Use this constructor
     * when the data for an object is contiguous and there's no need for a
     * Buffer to encapsulate it.
     *
     * The key and data must not be mutated after this call, since the object
     * checksum is computed during construction.
     *
     * \param key
     *      Key for this object, describing its tableId and binary string key.
     * \param data
     *      Pointer to a single contiguous piece of memory that comprises this
     *      object's data.
     * \param dataLength
     *      Length of the data portion in bytes.
     * \param version
     *      Version number of this object, which is used to disambiguate
     *      different incarnations of objects with the same key.
     * \param timestamp
     *      The creation time of this object, as returned by the WallTime
     *      module. Used primarily by the cleaner to order live objects and
     *      improve future cleaning performance.
     */
    Object(Key& key,
           const void* data,
           uint32_t dataLength,
           uint64_t version,
           uint32_t timestamp)
        : serializedForm(key.getTableId(),
                         key.getStringKeyLength(),
                         version,
                         timestamp),
          key(key.getStringKey()),
          dataLength(dataLength),
          data(data),
          dataBuffer(),
          objectBuffer()
    {
        serializedForm.checksum = computeChecksum();
    }

    /**
     * Construct a new object that we can later serialize. Use this constructor
     * when the data for an object is discontiguous and described by a Buffer
     * object.
     *
     * The key and data must not be mutated after this call, since the object
     * checksum is computed during construction.
     *
     * \param key
     *      Key for this object, describing its tableId and binary string key.
     * \param dataBuffer
     *      Buffer containing all chunks that will comprise this object's data.
     * \param version
     *      Version number of this object, which is used to disambiguate
     *      different incarnations of objects with the same key.
     * \param timestamp
     *      The creation time of this object, as returned by the WallTime
     *      module. Used primarily by the cleaner to order live objects and
     *      improve future cleaning performance.
     */
    Object(Key& key,
           Buffer& dataBuffer,
           uint64_t version,
           uint32_t timestamp)
        : serializedForm(key.getTableId(),
                         key.getStringKeyLength(),
                         version,
                         timestamp),
          key(key.getStringKey()),
          dataLength(dataBuffer.getTotalLength()),
          data(),
          dataBuffer(&dataBuffer),
          objectBuffer()
    {
        serializedForm.checksum = computeChecksum();
    }

    /**
     * Construct an object by deserializing an existing object in a Buffer.
     * Use this method to read an object that was previously serialized.
     *
     * \param buffer
     *      Buffer referring to the complete serialized object. It is the
     *      caller's responsibility to make sure that the buffer passed in
     *      actually contains a full object. If it does not, then behavior
     *      is undefined.
     */
    explicit Object(Buffer& buffer)
        : serializedForm(*buffer.getStart<SerializedForm>()),
          key(NULL),
          dataLength(buffer.getTotalLength() -
                     sizeof32(serializedForm) -
                     serializedForm.keyLength),
          data(),
          dataBuffer(),
          objectBuffer(&buffer)
    {
    }

    /**
     * Construct an object by deserializing an existing object in contiguous
     * memory. Use this method to read an object that was previously serialized
     * and happens to already be contiguous.
     *
     * \param buffer
     *      Pointer to memory containing the entire serialized object. It it the
     *      caller's responsibility to make sure this actually contains a full
     *      object. If it does not, then behavior is undefined.
     * \param length
     *      Total length of the object in bytes.
     */
    Object(const void* buffer, uint32_t length)
        : serializedForm(*reinterpret_cast<const SerializedForm*>(buffer)),
          key(reinterpret_cast<const void*>(reinterpret_cast<const uint8_t*>(
              buffer) + sizeof(SerializedForm))),
          dataLength(length - sizeof32(serializedForm) -
                              serializedForm.keyLength),
          data(reinterpret_cast<const void*>(reinterpret_cast<const uint8_t*>(
              key) + serializedForm.keyLength)),
          dataBuffer(),
          objectBuffer()
    {
    }

    /**
     * Append the serialized object header, binary string key, and data blob
     * to the provided buffer.
     *
     * \param buffer
     *      The buffer to append a serialized version of this object to.
     */
    void
    serializeToBuffer(Buffer& buffer)
    {
        buffer.append(&serializedForm, sizeof32(serializedForm));
        appendKeyToBuffer(buffer);
        appendDataToBuffer(buffer);
    }

    /**
     * Append the binary string key portion of this object to a provided buffer.
     * This is only the key blob and does not contain the table identifier.
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

        Buffer::Iterator it(**objectBuffer,
                            sizeof32(serializedForm),
                            getKeyLength());
        while (!it.isDone()) {
            buffer.append(it.getData(), it.getLength());
            it.next();
        }
    }

    /**
     * Append the data blob associated with this object to a provided buffer.
     *
     * \param buffer
     *      The buffer to append the data blob to.
     */
    void
    appendDataToBuffer(Buffer& buffer)
    {
        if (data) {
            buffer.append(*data, dataLength);
            return;
        }

        uint32_t offset = 0;
        Buffer* sourceBuffer = NULL;

        if (dataBuffer) {
            sourceBuffer = *dataBuffer;
        } else {
            sourceBuffer = *objectBuffer;
            offset = sizeof32(serializedForm) + getKeyLength();
        }

        Buffer::Iterator it(*sourceBuffer, offset, dataLength);
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
        return serializedForm.tableId;
    }

    /**
     * Obtain a pointer to a contiguous copy of this object's binary string key.
     * Note that if the key is not already contiguous, it will be copied.
     */
    const void*
    getKey()
    {
        if (key)
            return key;

        return (*objectBuffer)->getRange(
            sizeof(serializedForm), getKeyLength());
    }

    /**
     * Obtain the length of this object's binary string key.
     */
    uint16_t
    getKeyLength()
    {
        return serializedForm.keyLength;
    }

    /**
     * Obtain a pointer to a contiguous copy of this object's data blob. Note
     * that if the data is not already contiguous, it will be copied.
     */
    const void*
    getData()
    {
        if (data)
            return *data;

        if (dataBuffer)
            return (*dataBuffer)->getRange(0, dataLength);

        return (*objectBuffer)->getRange(
            sizeof32(SerializedForm) + getKeyLength(), dataLength);
    }

    /**
     * Obtain the length of the data blob associated with this object. 
     */
    uint32_t
    getDataLength()
    {
        return dataLength;
    }

    /**
     * Obtain the 64-bit version number associated with this object.
     */
    uint64_t
    getVersion()
    {
        return serializedForm.version;
    }

    /**
     * Obtain the timestamp associated with this object. See WallTime.cc
     * for interpreting the timestamp.
     */
    uint32_t
    getTimestamp()
    {
        return serializedForm.timestamp;
    }

    /**
     * Compute a checksum on the object and determine whether or not it matches
     * what is stored in the object. Returns true if the checksum looks ok,
     * otherwise returns false.
     */
    bool
    checkIntegrity()
    {
        return computeChecksum() == serializedForm.checksum;
    }

    /**
     * Given the length of a prospective object's binary string key and data
     * blob compute the exact byte byte length of such a serialized object.
     */
    static uint32_t
    getSerializedLength(uint32_t keyLength, uint32_t dataLength)
    {
        return sizeof32(SerializedForm) + keyLength + dataLength;
    }

//  PRIVATE:
    /**
     * This data structure defines the format of an object stored in a master
     * server's log. When writing an object, the fields below are written
     * first, then the binary string key, and finally the object's data are
     * written sequentially.
     */
    class SerializedForm {
      public:
        /**
         * Construct a serialized object header.
         *
         * \param tableId
         *      The 64-bit identifier for the table this object is in.
         * \param keyLength
         *      Length of the object's binary string key in bytes.
         * \param version
         *      64-bit version number associated with this object.
         * \param timestamp
         *      The creation time of this object, as returned by the WallTime
         *      module. Used primarily by the cleaner to order live objects and
         *      improve future cleaning performance.
         */
        SerializedForm(uint64_t tableId,
                       uint16_t keyLength,
                       uint64_t version,
                       uint32_t timestamp)
            : tableId(tableId),
              version(version),
              timestamp(timestamp),
              keyLength(keyLength),
              checksum(0)
        {
        }

        /// Table to which this object belongs. A (TableId, StringKey) tuple
        /// uniquely identifies a live object.
        uint64_t tableId;

        /// Version of the object. Set to some initial value upon object
        /// creation and incremented by one for each modification. See
        /// MasterService for the exact behavior.
        uint64_t version;

        /// Object creation/modification timestamp. WallTime.cc is the clock.
        uint32_t timestamp;

        /// Length of the binary string key in bytes.
        uint16_t keyLength;

        /// CRC32C checksum covering everything but this field, including the
        /// key and the data.
        uint32_t checksum;

        /// Following this class will be the key and the data. This member is
        /// only here to denote this.
        char keyAndData[0];
    } __attribute__((__packed__));
    static_assert(sizeof(SerializedForm) == 26,
        "Unexpected serialized Object size");

    /**
     * Compute the object's checksum and return it.
     */
    uint32_t
    computeChecksum()
    {
        assert(OFFSET_OF(SerializedForm, checksum) ==
            (sizeof(serializedForm) - sizeof(serializedForm.checksum)));

        Crc32C crc;
        crc.update(&serializedForm,
                   downCast<uint32_t>(OFFSET_OF(SerializedForm, checksum)));

        if (key) {
            crc.update(key, getKeyLength());
        } else {
            crc.update(**objectBuffer, sizeof(serializedForm), getKeyLength());
        }

        if (data) {
            crc.update(*data, dataLength);
        } else if (dataBuffer) {
            crc.update(**dataBuffer);
        } else {
            crc.update(**objectBuffer,
                sizeof32(serializedForm) + getKeyLength(), getDataLength());
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
     *      Total length of the object in bytes, including the header, string
     *      key, and data.
     */
    static uint32_t
    computeChecksum(const Object::SerializedForm* object, uint32_t totalLength)
    {
        Crc32C crc;
        crc.update(object,
            downCast<uint32_t>(OFFSET_OF(SerializedForm, checksum)));
        uint32_t dataLen = totalLength - object->keyLength -
                           sizeof32(SerializedForm);
        crc.update(&object->keyAndData[0], object->keyLength + dataLen);
        return crc.getResult();
    }

    /// Copy of the object header that is in, or will be written to, the log.
    SerializedForm serializedForm;

    /// Pointer to the binary string key for this object.
    const void* key;

    /// Length of the user data portion of this object. This isn't stored in
    /// SerializedForm since it can be trivially computed as needed.
    uint32_t dataLength;

    /// If an object is created such that the data all lies in a single
    /// contiguous buffer, this will point there.
    Tub<const void*> data;

    /// If an object is created such that the data portion is referred to by
    /// a Buffer, this will point to that buffer.
    Tub<Buffer*> dataBuffer;

    /// If an object is being read from a serialized copy (for instance, from
    /// the log), this will point to the buffer that refers to the entire
    /// object.
    Tub<Buffer*> objectBuffer;

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
 *               sizeof(SerializedForm)      variable length
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
        : serializedForm(object.getTableId(),
                         segmentId,
                         object.getVersion(),
                         timestamp),
          key(object.getKey()),
          keyLength(object.getKeyLength()),
          tombstoneBuffer()
    {
        serializedForm.checksum = computeChecksum();
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
        : serializedForm(*buffer.getStart<SerializedForm>()),
          key(),
          keyLength(downCast<uint16_t>(buffer.getTotalLength() -
                    sizeof32(SerializedForm))),
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
    serializeToBuffer(Buffer& buffer)
    {
        buffer.append(&serializedForm, sizeof32(serializedForm));
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

        Buffer::Iterator it(**tombstoneBuffer,
                            sizeof32(serializedForm),
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
        return serializedForm.tableId;
    }

    /**
     * Obtain a pointer to a contiguous copy of this tombstone's binary string
     * key. Note that if the key is not already contiguous, it will be copied.
     */
    const void*
    getKey()
    {
        if (key)
            return *key;

        return (*tombstoneBuffer)->getRange(
            sizeof(serializedForm), getKeyLength());
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
        return serializedForm.segmentId;
    }

    /**
     * Obtain the 64-bit version number associated with the object this
     * tombstone is making the deletion of.
     */
    uint64_t
    getObjectVersion()
    {
        return serializedForm.objectVersion;
    }

    /**
     * Obtain the timestamp associated with this tombstone. See WallTime.cc
     * for interpreting the timestamp.
     */
    uint32_t
    getTimestamp()
    {
        return serializedForm.timestamp;
    }

    /**
     * Compute a checksum on the object and determine whether or not it matches
     * what is stored in the object. Returns true if the checksum looks ok,
     * otherwise returns false.
     */
    bool
    checkIntegrity()
    {
        return computeChecksum() == serializedForm.checksum;
    }

    /**
     * Given the length of a prospective tombstone's binary string key compute
     * the exact byte byte length of such a serialized tombstone.
     */
    static uint32_t
    getSerializedLength(uint32_t keyLength)
    {
        return sizeof32(SerializedForm) + keyLength;
    }

  //PRIVATE:
    /**
     * This data structure defines the format of an object's tombstone stored
     * in a master server's log. When writing a tombstone, the fields below are
     * written first, then the binary string key of the dead object.
     */
    class SerializedForm {
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
        SerializedForm(uint64_t tableId,
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
    static_assert(sizeof(SerializedForm) == 32,
        "Unexpected serialized ObjectTombstone size");

    /**
     * Compute the tombstone's checksum and return it.
     */
    uint32_t
    computeChecksum()
    {
        assert(OFFSET_OF(SerializedForm, checksum) ==
            (sizeof(serializedForm) - sizeof(serializedForm.checksum)));

        Crc32C crc;
        crc.update(&serializedForm,
                   downCast<uint32_t>(OFFSET_OF(SerializedForm, checksum)));

        if (key) {
            crc.update(*key, getKeyLength());
        } else {
            crc.update(**tombstoneBuffer,
                       sizeof(serializedForm),
                       getKeyLength());
        }

        return crc.getResult();
    }

    /// Copy of the tombstone header that is in, or will be written to, the log.
    SerializedForm serializedForm;

    /// Pointer to the binary string key for this object.
    Tub<const void*> key;

    /// Length of the key corresponding to this tombstone. This isn't stored in
    /// SerializedForm since it can be trivially computed as needed.
    uint16_t keyLength;

    /// If a tombstone is being read from a serialized copy (for instance, from
    /// the log), this will point to the buffer that refers to the entire
    /// tombstone.
    Tub<Buffer*> tombstoneBuffer;

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
       :  serializedForm(safeVer)
    {
        serializedForm.checksum = computeChecksum();
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
        : serializedForm(*buffer.getStart<SerializedForm>())
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
    serializeToBuffer(Buffer& buffer)
    {
        buffer.append(&serializedForm, sizeof32(serializedForm));
    }

    /**
     * Compute a checksum on the object and determine whether or not it matches
     * what is stored in the object. Returns true if the checksum looks ok,
     * otherwise returns false.
     */
    bool
    checkIntegrity()
    {
        return computeChecksum() == serializedForm.checksum;
    }

    /**
     * Given the length of a prospective 
     * the exact byte byte length of such a serialized safeVersion.
     */
    static uint32_t
    getSerializedLength()
    {
        return sizeof32(SerializedForm);
    }

    /** 
     * Get safeVersion value
     */

    uint64_t getSafeVersion() const
    {
        return serializedForm.safeVersion;
    }

  PRIVATE:
    /**
     * This data structure defines the format of an object's safeVersion stored
     * in a master server's log. When writing a safeVersion, the fields below are
     * only written.
     */
    class SerializedForm {
      public:
        /**
         * Construct a serialized object safeVersion, which is a header
         * only object.
         *
         * \param safeVersion
         *      The 64-bit identifier for the table the dead object was in.
         */
        explicit SerializedForm(uint64_t safeVersion)
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
    static_assert(sizeof(SerializedForm) == 12,
        "Unexpected serialized ObjectSafeVersion size");

    /**
     * Compute the safeVersion's checksum and return it.
     */
    uint32_t
    computeChecksum()
    {
        assert(OFFSET_OF(SerializedForm, checksum) ==
            (sizeof(serializedForm) - sizeof(serializedForm.checksum)));

        Crc32C crc;
        crc.update(&serializedForm,
                   downCast<uint32_t>(OFFSET_OF(SerializedForm, checksum)));
        return crc.getResult();
    }

    /// Copy of the safeVersion header that is in,
    /// or will be written to, the log.
    SerializedForm serializedForm;

    /// No pointer to safeVersion body since no body exist.
    // Tub<Buffer*> safeVersionBuffer;

    DISALLOW_COPY_AND_ASSIGN(ObjectSafeVersion);
};

} // namespace RAMCloud

#endif
