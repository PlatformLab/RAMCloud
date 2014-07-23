/* Copyright (c) 2010-2014 Stanford University
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
#include "Key.h"

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
 * Each object contains one or more variable-length keys, a variable-length
 * binary blob of data, and a few other pieces of metadata (such as table
 * id and version).  When stored in the log, an object has the following
 * layout:
 *
 * +---------------+--------+---------------------------+----------+----------+
 * | Object Header | # keys | Cumulative Key Lengths .. | Keys ... | Data ... |
 * +---------------+--------+---------------------------+----------+----------+
 *                 <--------------------- keysAndValue ----------------------->
 *
 * "Object Header" is a structure of type Header, defined below.
 * "# keys" is typedefined below as KeyCount
 * "CumulativeKeyLength" is typedefined below as CumulativeKeyLength
 *
 * Everything except the header and the number of keys is of variable length.
 * If the cumulative key length values are 16 bits each, then
 * the constraint is that the total length of all the keys in an object has
 * to be <= 64K.
 * The length of any key can be calculated using these equations,
 *
 * Length_0 = CumulativeKeyLength_0
 * Length_i = max(0, CumulativeKeyLength_i - CumulativeKeyLength_i-1) for i >= 1
 *
 * If Key_i is not present, CumulativeKeyLength_i = CumulativeKeyLength_i-1.
 * Consequently, Length_i = 0
 */

typedef uint8_t KeyCount;   // the number of keys in an object
typedef KeyCount KeyIndex;  // the position of this key: 0 to KeyCount - 1
typedef KeyLength CumulativeKeyLength;

// A convenience macro to get to the starting of the first key in an object
#define KEY_INFO_LENGTH(x) \
    (sizeof32(KeyCount) + (x) * sizeof32(CumulativeKeyLength))


// defined in RamCloud.h Forward declaring here to avoid a circular
// dependency between Object.h and RamCloud.h
struct KeyInfo;

// Represents the number of keys and the cumulative key length values
struct KeyOffsets
{
    KeyCount numKeys;                        // number of keys
    CumulativeKeyLength cumulativeLengths[]; // starting of the cumulative key
                                             // length values.
} __attribute__((packed));

class Object {
  public:
    Object(uint64_t tableId, uint64_t version, uint32_t timestamp,
           Buffer& keysAndValueBuffer, uint32_t startDataOffset = 0,
           uint32_t length = 0);
    Object(Key& key, const void* value, uint32_t valueLength, uint64_t version,
           uint32_t timestamp, Buffer& buffer, uint32_t *length = NULL);
    explicit Object(Buffer& buffer, uint32_t offset = 0, uint32_t length = 0);
    explicit Object(const void* buffer, uint32_t length);

    void assembleForLog(Buffer& buffer);
    void assembleForLog(void* buffer);
    void appendValueToBuffer(Buffer& buffer, uint32_t valueOffset = 0);
    static void appendKeysAndValueToBuffer(uint64_t tableId, KeyCount numKeys,
                                      KeyInfo *keyList, const void* value,
                                      uint32_t valueLength, Buffer& request,
                                      uint32_t *length = NULL);
    static void appendKeysAndValueToBuffer(
            Key& key, const void* value, uint32_t valueLength,
            Buffer& buffer, uint32_t *length = NULL);
    void appendKeysAndValueToBuffer(Buffer& buffer);

    bool fillKeyOffsets();

    /**
     * Obtain the 64-bit table identifier associated with this object.
     */
    uint64_t getTableId() {
        return header.tableId;
    }
    const void* getKey(KeyIndex keyIndex = 0, KeyLength *keyLength = NULL);
    KeyLength getKeyLength(KeyIndex keyIndex = 0);
    const void* getKeysAndValue();
    KeyCount getKeyCount();
    const void* getValue(uint32_t *valueLength = NULL);
    bool getValueOffset(uint16_t *offset);
    uint32_t getValueLength();

    uint32_t getKeysAndValueLength();
    uint64_t getVersion();
    uint32_t getTimestamp();
    uint32_t getSerializedLength();

    bool checkIntegrity();
    void setVersion(uint64_t version);
    void setTimestamp(uint32_t timestamp);

//  PRIVATE:
    /**
     * This data structure defines the format of an object header stored in a
     * master server's log.
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
        char keysAndData[0];
    } __attribute__((__packed__));
    static_assert(sizeof(Header) == 24,
        "Unexpected serialized Object size");


    static uint32_t computeChecksum(const Object::Header* object,
                                    uint32_t totalLength);
    uint32_t computeChecksum();


    /// Copy of the object header that is in, or will be written to, the log.
    Header header;

    /// Length that includes the number of keys, the key lengths, the keys
    /// and the value. This isn't stored in Header since it can be computed
    /// as needed.
    uint32_t keysAndValueLength;

    /// If the keys and value for the object all lie in a single
    /// contiguous region of memory, this will point there, otherwise this
    /// will point to NULL.
    const void* keysAndValue;

    /// If the keys and value for the object are stored in a Buffer,
    /// this will point to that buffer, otherwise this points to NULL.
    Buffer* keysAndValueBuffer;

    /// The byte offset in the keysAndValueBuffer where keysAndValue start
    uint32_t keysAndValueOffset;

    /// Pointer to a contiguous memory region that contains the number of
    /// keys and the cumulative key length values. This will be NULL until
    /// the first call to getKey(), getKeyLength(), getValue() or
    /// getValueOffset()
    const KeyOffsets *keyOffsets;

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
 * Internally, tombstones are basically the primary key and some additional
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
    ObjectTombstone(Object& object, uint64_t segmentId, uint32_t timestamp);
    explicit ObjectTombstone(Buffer& buffer, uint32_t offset = 0,
                             uint32_t length = 0);

    void assembleForLog(Buffer& buffer);
    void assembleForLog(void* buffer);
    void appendKeyToBuffer(Buffer& buffer);

    uint64_t getTableId();
    const void* getKey();
    uint16_t getKeyLength();
    uint64_t getSegmentId();
    uint64_t getObjectVersion();
    uint32_t getTimestamp();

    bool checkIntegrity();
    static uint32_t getSerializedLength(uint32_t keyLength);
    uint32_t getSerializedLength();
    uint32_t computeChecksum();

  //PRIVATE:
    /**
     * This data structure defines the format of an object's tombstone stored
     * in a master server's log. When writing a tombstone, the fields below are
     * written first, then the primary key of the dead object.
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

    /// Copy of the tombstone header that is in, or will be written to, the log.
    Header header;

    /// Pointer to the binary string key for this object.
    const void* key;

    /// Length of the key corresponding to this tombstone. This isn't stored in
    /// Header since it can be trivially computed as needed.
    uint16_t keyLength;

    /// If a tombstone is being read from a serialized copy (for instance, from
    /// the log), this will point to the buffer that refers to the entire
    /// tombstone. This is NULL for a new tombstone that is being constructed.
    Buffer* tombstoneBuffer;

    /// Stores the offset of the key in #tombstoneBuffer
    uint32_t keyOffset;

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
    explicit ObjectSafeVersion(const uint64_t safeVer);
    explicit ObjectSafeVersion(Buffer& buffer);

    void assembleForLog(Buffer& buffer);
    bool checkIntegrity();
    static uint32_t getSerializedLength();
    uint64_t getSafeVersion() const;

    uint32_t computeChecksum();

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

    /// Copy of the safeVersion header that is in,
    /// or will be written to, the log.
    Header header;

    /// No pointer to safeVersion body since no body exist.
    // Tub<Buffer*> safeVersionBuffer;

    DISALLOW_COPY_AND_ASSIGN(ObjectSafeVersion);
};

} // namespace RAMCloud

#endif
