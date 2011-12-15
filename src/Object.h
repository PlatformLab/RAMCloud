/* Copyright (c) 2010 Stanford University
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
#include "HashTable.h"
#include "WallTime.h"
#include "KeyHash.h"

namespace RAMCloud {

/**
 * Declare an Object, allocate the required space for it, and initialize.
 * All the memebers still have to be assigned the right values later,
 * and the key and data have to be appended.
 *
 * \param name
 *      Name for the object declared.
 * \param keyLength
 *      Space required in bytes, apart from the Object struct to store the key.
 * \param dataLength
 *      Space required in bytes, apart from the Object struct to store the data.
 */
#define DECLARE_OBJECT(name, keyLength, dataLength) \
    char name##_buf[sizeof(Object) + (keyLength + dataLength)] \
        __attribute__((aligned (8))); \
    Object *name = new(name##_buf) Object(sizeof(name##_buf)); \
    assert((reinterpret_cast<uint64_t>(name) & 0x7) == 0);

class Object {
  public:
    /*
     * This buf_size parameter is here to annoy you a little bit if you try
     * stack-allocating one of these. You'll think twice about it, maybe
     * realize sizeof(data) is bogus, and proceed to dynamically allocating
     * a buffer instead.
     */
    explicit Object(size_t buf_size)
        : tableId(-1),
          keyLength(0),
          version(-1),
          timestamp(secondsTimestamp())
    {
        assert(buf_size >= sizeof(*this));
    }

    /**
     * Return the total byte size of the Object that contains the specified
     * number of bytes in ``data''.
     * This exists because Objects do not contain a length field, since they
     * typically exist in the Log, which must record that information anyhow.
     */
    uint32_t
    objectLength(uint32_t dataBytes) const
    {
        return downCast<uint32_t>(sizeof(*this)) + keyLength + dataBytes;
    }

    /**
     * Return the number of bytes of data an Object contains, given
     * the total size of the Object.
     */
    uint32_t
    dataLength(uint32_t totalObjectBytes) const
    {
        assert(totalObjectBytes >= sizeof(*this) + keyLength);
        return totalObjectBytes -
               downCast<uint32_t>(sizeof(*this)) -
               keyLength;
    }

    /**
     * Return the key of the object.
     */
    const char*
    getKey() const
    {
        return keyAndData;
    }

    /**
     * Return a pointer to the location where the key of the object is stored.
     */
    void*
    getKeyLocation()
    {
        return keyAndData;
    }

    /**
     * Return the data stored in the object.
     */
    const char*
    getData() const
    {
        return keyAndData + keyLength;
    }

    /**
     * Return a pointer to the location where the data contained in the object
     * is stored.
     */
    void*
    getDataLocation()
    {
        return keyAndData + keyLength;
    }

    HashType
    keyHash() const
    {
        return getKeyHash(keyAndData, keyLength);
    }

    uint64_t tableId;
    uint16_t keyLength;
    uint64_t version;
    uint32_t timestamp;         // see WallTime.cc
    char keyAndData[0];         // This will first have the key (of size
                                // keyLength) and then the data.

  PRIVATE:
    Object() : tableId(-1), keyLength(0), version(-1),
               timestamp(secondsTimestamp()) { }

    // to use default constructor in arrays
    friend void hashTableBenchmark(uint64_t, uint64_t);

    DISALLOW_COPY_AND_ASSIGN(Object); // NOLINT
} __attribute__((__packed__));

/**
 * Declare a Tombstone, allocate the required space for it, and initialize.
 *
 * \param name
 *      Name for the tombstone declared.
 * \param keyLength
 *      Space required in bytes, apart from the ObjectTombstone struct to
 *      store the key.
 * \param segId
 *      Segment Id of the object which this tombstone deletes.
 * \param obj
 *      A pointer to the object which this deletes. This is used to initialize
 *      values in the tombstone.
 */
#define DECLARE_OBJECTTOMBSTONE(name, keyLength, segId, obj) \
    char name##_buf[sizeof(ObjectTombstone) + (keyLength)] \
        __attribute__((aligned (8))); \
    ObjectTombstone *name = \
        new(name##_buf) ObjectTombstone(sizeof(name##_buf), segId, obj); \
    assert((reinterpret_cast<uint64_t>(name) & 0x7) == 0);

class ObjectTombstone {
  public:
    /*
     * This buf_size parameter is here to annoy you a little bit if you try
     * stack-allocating one of these. You'll think twice about it, maybe
     * realize sizeof(data) is bogus, and proceed to dynamically allocating
     * a buffer instead.
     */
    explicit ObjectTombstone(size_t buf_size, uint64_t segId,
                             const Object *object)
        : tableId(object->tableId),
          keyLength(object->keyLength),
          segmentId(segId),
          objectVersion(object->version),
          timestamp(secondsTimestamp())
    {
        assert(buf_size == sizeof(*this) + object->keyLength);
        memcpy(key, object->getKey(), object->keyLength);
    }

    /**
     * Return the total byte size of the Tombstone.
     * This exists because Tombstones do not contain a length field, since they
     * typically exist in the Log, which must record that information anyhow.
     */
    uint32_t
    tombLength() const
    {
        return downCast<uint32_t>(sizeof(*this)) + keyLength;
    }

    /**
     * Return the key of the tombstone.
     */
    const char*
    getKey() const
    {
        return key;
    }

    /**
     * Return a pointer to the location where the key of the tombstone
     * is stored.
     */
    void*
    getKeyLocation()
    {
        return key;
    }

    HashType
    keyHash() const
    {
        return getKeyHash(key, keyLength);
    }

    uint64_t tableId;
    uint16_t keyLength;
    uint64_t segmentId;
    uint64_t objectVersion;
    uint32_t timestamp;
    char key[0];

  PRIVATE:
    ObjectTombstone(uint64_t segmentId, uint64_t tableId,
                    uint16_t keyLength, uint64_t objectVersion)
        : tableId(tableId),
          keyLength(keyLength),
          segmentId(segmentId),
          objectVersion(objectVersion),
          timestamp(secondsTimestamp())
    {
    }
} __attribute__((__packed__));

} // namespace RAMCloud

#endif
