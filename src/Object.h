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

namespace RAMCloud {

#define DECLARE_OBJECT(name, el) \
    char name##_buf[sizeof(Object) + (el)] __attribute__((aligned (8))); \
    Object *name = new(name##_buf) Object(sizeof(name##_buf)); \
    assert((reinterpret_cast<uint64_t>(name) & 0x7) == 0);

struct ObjectIdentifier {
    ObjectIdentifier(uint64_t tableId, uint64_t objectId)
        : tableId(tableId),
          objectId(objectId)
    {
        static_assert(sizeof(*this) == 16, "bad ObjectIdentifier size!");
    }

    uint64_t tableId;
    uint64_t objectId;
} __attribute__((__packed__));

class Object {
  public:
    /*
     * This buf_size parameter is here to annoy you a little bit if you try
     * stack-allocating one of these. You'll think twice about it, maybe
     * realize sizeof(data) is bogus, and proceed to dynamically allocating
     * a buffer instead.
     */
    explicit Object(size_t buf_size)
        : id(-1, -1),
          version(-1),
          timestamp(secondsTimestamp())
    {
        static_assert(sizeof(*this) == 28, "bad Object size!");
        assert(buf_size >= sizeof(*this));
    }

    /**
     * Return the total byte size of an Object that contains the specified
     * number of bytes in ``data''.  This exists because Objects do not
     * contain a length field, since they typically exist in the Log, which
     * must record that information anyhow.
     */
    uint32_t
    objectLength(uint32_t dataBytes) const
    {
        return downCast<uint32_t>(sizeof(*this)) + dataBytes;
    }

    /**
     * Return the number of bytes of data an Object contains, given
     * the total size of the Object.
     */
    uint32_t
    dataLength(uint32_t totalObjectBytes) const
    {
        assert(totalObjectBytes >= sizeof(*this));
        return totalObjectBytes - downCast<uint32_t>(sizeof(*this));
    }

    struct ObjectIdentifier id;
    uint64_t version;
    uint32_t timestamp;         // see WallTime.cc
    char data[0];

  PRIVATE:
    Object() : id(-1, -1), version(-1), timestamp(secondsTimestamp()) { }

    // to use default constructor in arrays
    friend void hashTableBenchmark(uint64_t, uint64_t);

    DISALLOW_COPY_AND_ASSIGN(Object); // NOLINT
} __attribute__((__packed__));

class ObjectTombstone {
  public:
    ObjectTombstone(uint64_t segmentId, const Object *object)
        : id(object->id),
          segmentId(segmentId),
          objectVersion(object->version)
    {
        static_assert(sizeof(*this) == 32, "bad Object size!");
    }

    struct ObjectIdentifier id;
    uint64_t segmentId;
    uint64_t objectVersion;

  PRIVATE:
    ObjectTombstone(uint64_t segmentId, uint64_t tableId,
                    uint64_t objectId, uint64_t objectVersion)
        : id(tableId, objectId),
          segmentId(segmentId),
          objectVersion(objectVersion)
    {
    }
} __attribute__((__packed__));

} // namespace RAMCloud

#endif
