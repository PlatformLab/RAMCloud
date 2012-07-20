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
#include "Key.h"
#include "WallTime.h"

namespace RAMCloud {

class Object {
  public:
    Object(Key& key,
           const void* data,
           uint32_t dataLength,
           uint64_t version)
        : serializedForm(key.getTableId(),
                         key.getStringKeyLength(),
                         version,
                         secondsTimestamp()),
          key(key.getStringKey()),
          dataLength(dataLength),
          data(data),
          dataBuffer(),
          objectBuffer()
    {
    }

    Object(Key& key,
           Buffer& dataBuffer,
           uint64_t version)
        : serializedForm(key.getTableId(),
                         key.getStringKeyLength(),
                         version,
                         secondsTimestamp()),
          key(key.getStringKey()),
          dataLength(dataBuffer.getTotalLength()),
          data(),
          dataBuffer(&dataBuffer),
          objectBuffer()
    {
    }

    Object(Buffer& buffer)
        : serializedForm(*buffer.getStart<SerializedForm>()),
          key(),
          dataLength(buffer.getTotalLength() - sizeof32(serializedForm) - serializedForm.keyLength),
          data(),
          dataBuffer(),
          objectBuffer(&buffer)
    {
    }

    void
    serializeToBuffer(Buffer& buffer)
    {
        Buffer::Chunk::appendToBuffer(&buffer, &serializedForm, sizeof32(serializedForm));
        appendKeyToBuffer(buffer);
        appendDataToBuffer(buffer);
    }

    void
    appendKeyToBuffer(Buffer& buffer)
    {
        if (key) {
            Buffer::Chunk::appendToBuffer(&buffer, getKey(), getKeyLength());
            return;
        }

        Buffer::Iterator it(**objectBuffer, sizeof32(serializedForm), getKeyLength());
        while (!it.isDone()) {
            Buffer::Chunk::appendToBuffer(&buffer, it.getData(), it.getLength());
            it.next();
        }
    }

    void
    appendDataToBuffer(Buffer& buffer)
    {
        if (data) {
            Buffer::Chunk::appendToBuffer(&buffer, *data, dataLength);
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
            Buffer::Chunk::appendToBuffer(&buffer, it.getData(), it.getLength());
            it.next();
        }
    }

    uint64_t
    getTableId()
    {
        return serializedForm.tableId;
    }

    const void*
    getKey()
    {
        if (key)
            return key;

        return (*objectBuffer)->getRange(sizeof(serializedForm), getKeyLength());
    }

    uint16_t
    getKeyLength()
    {
        return serializedForm.keyLength;
    }

    const void*
    getData()
    {
        if (data)
            return *data;

        if (dataBuffer)
            return (*dataBuffer)->getRange(0, dataLength);

        return (*objectBuffer)->getRange(sizeof32(SerializedForm) + getKeyLength(), dataLength);
    }

    uint32_t
    getDataLength()
    {
        return dataLength;
    }

    uint64_t
    getVersion()
    {
        return serializedForm.version;
    }

    uint32_t
    getTimestamp()
    {
        return serializedForm.timestamp;
    }

    static uint32_t
    getSerializedLength(uint32_t keyLength, uint32_t dataLength)
    {
        return sizeof32(SerializedForm) + keyLength + dataLength;
    }

  PRIVATE:
    class SerializedForm {
      public:
        SerializedForm(uint64_t tableId,
                       uint16_t keyLength,
                       uint64_t version,
                       uint32_t timestamp)
            : tableId(tableId),
              keyLength(keyLength),
              version(version),
              timestamp(timestamp)
        {
        }

        uint64_t tableId;
        uint16_t keyLength;
        uint64_t version;
        uint32_t timestamp;         // see WallTime.cc
        char keyAndData[0];         // This will first have the key (of size
                                    // keyLength) and then the data.
    } __attribute__((__packed__));
    static_assert(sizeof(SerializedForm) == 22,
        "Unexpected serialized Object size");

    SerializedForm serializedForm;
    const void* key;
    uint32_t dataLength;
    Tub<const void*> data;
    Tub<Buffer*> dataBuffer;
    Tub<Buffer*> objectBuffer;

    DISALLOW_COPY_AND_ASSIGN(Object);
};

class ObjectTombstone {
  public:
    ObjectTombstone(Object& object, uint64_t segmentId)
        : serializedForm(object.getTableId(),
                         object.getKeyLength(),
                         segmentId,
                         object.getVersion(),
                         secondsTimestamp()),
          key(object.getKey()),
          tombstoneBuffer()
    {
    }

    ObjectTombstone(Buffer& buffer)
        : serializedForm(*buffer.getStart<SerializedForm>()),
          key(),
          tombstoneBuffer(&buffer)
    {
    }

    void
    serializeToBuffer(Buffer& buffer)
    {
        Buffer::Chunk::appendToBuffer(&buffer, &serializedForm, sizeof32(serializedForm));
        appendKeyToBuffer(buffer);
    }

    void
    appendKeyToBuffer(Buffer& buffer)
    {
        if (key) {
            Buffer::Chunk::appendToBuffer(&buffer, getKey(), getKeyLength());
            return;
        }

        Buffer::Iterator it(**tombstoneBuffer, sizeof32(serializedForm), getKeyLength());
        while (!it.isDone()) {
            Buffer::Chunk::appendToBuffer(&buffer, it.getData(), it.getLength());
            it.next();
        }
    }

    uint64_t
    getTableId()
    {
        return serializedForm.tableId;
    }

    const void*
    getKey()
    {
        if (key)
            return *key;

        return (*tombstoneBuffer)->getRange(sizeof(serializedForm), getKeyLength());
    }

    uint16_t
    getKeyLength()
    {
        return serializedForm.keyLength;
    }

    uint64_t
    getSegmentId()
    {
        return serializedForm.segmentId;
    }

    uint64_t
    getObjectVersion()
    {
        return serializedForm.objectVersion;
    }

    uint32_t
    getTimestamp()
    {
        return serializedForm.timestamp;
    }

    static uint32_t
    getSerializedLength(uint32_t keyLength)
    {
        return sizeof32(SerializedForm) + keyLength;
    }

  PRIVATE:
    class SerializedForm {
      public:
        SerializedForm(uint64_t tableId,
                       uint16_t keyLength,
                       uint64_t segmentId,
                       uint64_t objectVersion,
                       uint32_t timestamp)
            : tableId(tableId),
              keyLength(keyLength),
              segmentId(segmentId),
              objectVersion(objectVersion),
              timestamp(timestamp)
        {
        }

        uint64_t tableId;
        uint16_t keyLength;
        uint64_t segmentId;
        uint64_t objectVersion;
        uint32_t timestamp;
        char key[0];
    } __attribute__((__packed__));
    static_assert(sizeof(SerializedForm) == 30,
        "Unexpected serialized ObjectTombstone size");

    SerializedForm serializedForm;
    Tub<const void*> key;
    Tub<Buffer*> tombstoneBuffer;

    DISALLOW_COPY_AND_ASSIGN(ObjectTombstone);
};

} // namespace RAMCloud

#endif
