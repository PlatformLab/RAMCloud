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

namespace RAMCloud {

#define DECLARE_OBJECT(name, el) \
    char name##_buf[sizeof(Object) + (el)] __attribute__((aligned (8))); \
    Object *name = new(name##_buf) Object(sizeof(name##_buf)); \
    assert((reinterpret_cast<uint64_t>(name) & 0x7) == 0);

// forward decl
class Object;
class ObjectTombstone;

class Objectable {
  public:
    Objectable(uint64_t id, uint64_t table) : id(id), table(table) {}
    virtual ~Objectable() {}

    const Object*
    asObject() const
    {
        return reinterpret_cast<const Object*>(this);
    }

    const ObjectTombstone *
    asObjectTombstone() const
    {
        return reinterpret_cast<const ObjectTombstone*>(this);
    }

    uint64_t id;
    uint64_t table;
} __attribute__((__packed__));

class Object : public Objectable {
  public:
    /*
     * This buf_size parameter is here to annoy you a little bit if you try
     * stack-allocating one of these. You'll think twice about it, maybe
     * realize sizeof(data) is bogus, and proceed to dynamically allocating
     * a buffer instead.
     */
    explicit Object(size_t buf_size) : Objectable(-1, -1), version(-1),
                                       checksum(0), data_len(0) {
        assert(buf_size >= sizeof(*this));
    }

    size_t size() const {
        return sizeof(*this) + this->data_len;
    }

    uint64_t version;
    uint64_t checksum;
    uint64_t data_len;
    char data[0];

  private:
    Object() : Objectable(-1, -1), version(-1), checksum(0), data_len(0) { }

    // to use default constructor in arrays
    friend class BackupServerTest;
    friend class HashTableTest;
    friend void hashTableBenchmark(uint64_t, uint64_t);

    DISALLOW_COPY_AND_ASSIGN(Object); // NOLINT
} __attribute__((__packed__));

class ObjectTombstone : public Objectable {
  public:
    uint64_t segmentId;
    uint64_t objectVersion;

    ObjectTombstone(uint64_t segmentId, const Object *object)
        : Objectable(object->table, object->id),
          segmentId(segmentId),
          objectVersion(object->version)
    {
    }

  private:
    ObjectTombstone(uint64_t segmentId, uint64_t tableId,
                    uint64_t objectId, uint64_t objectVersion)
        : Objectable(tableId, objectId),
          segmentId(segmentId),
          objectVersion(objectVersion)
    {
    }

    friend class BackupServerTest;
    friend class MasterTest;
} __attribute__((__packed__));

typedef HashTable<Objectable, &Objectable::table, &Objectable::id> ObjectMap;

} // namespace RAMCloud

#endif
