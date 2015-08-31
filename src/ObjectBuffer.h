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

#ifndef RAMCLOUD_OBJECTBUFFER_H
#define RAMCLOUD_OBJECTBUFFER_H

#include "Buffer.h"
#include "Object.h"

namespace RAMCloud {

/**
 * This class provides additional methods for managing buffers that contain
 * object keys and values returned typically by the readKeysAndValue RPC.
 * It makes it easy to read individual keys, plus the object value.
 * All of the methods in this class assume that the first bytes in the buffer
 * contain the keys and value for an object in the standard representation
 * used throughout RAMCloud.
 */

class ObjectBuffer : public Buffer {
  PUBLIC:
    ObjectBuffer()
    : object()
    {
    }

    ~ObjectBuffer()
    {
        reset();
    }

    KeyCount getNumKeys();
    const void *getKey(KeyIndex keyIndex = 0);
    KeyLength getKeyLength(KeyIndex keyIndex = 0);
    const void *getValue(uint32_t *dataLength = NULL);
    uint64_t getVersion();
    Object *getObject();

    /**
     * Convenience for getValue.
     * \return
     *      An appropriately casted pointer to the object's value
     */
    template<typename T> const T* get(uint32_t *dataLength = NULL) {
        return reinterpret_cast<const T*>(getValue(dataLength));
    }

    bool getValueOffset(uint32_t *offset);
    void reset();

  PRIVATE:
    Tub<Object> object;

    DISALLOW_COPY_AND_ASSIGN(ObjectBuffer);
};

} //namespace

#endif // RAMCLOUD_OBJECTBUFFER_H
