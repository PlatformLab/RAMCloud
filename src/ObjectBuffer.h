/* Copyright (c) 2013 Stanford University
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

namespace RAMCloud {

/**
 * This class provides additional methods for managing buffers that contain
 * object keys and values returned by RPCs such as read, multi-read, and
 * enumerate. It makes it easy to read individual keys, plus the object value.
 * All of the methods in this class assume that the first bytes in the buffer
 * contain the keys and value for an object in the standard representation
 * used throughout RAMCloud. This class invokes methods from the Object class
 * and hence creates dummy objects.
 */

// Cannot include Object.h here because there will be a circular dependency of
// header files otherwise - RamCloud.h, Object.h and ObjectBuffer.h. So,
// typedefine the following here again in addition to Object.h.
// IMPORTANT: this should match the definition in Object.h

typedef uint8_t KeyCount; // the number of keys in an object
typedef uint16_t KeyLength; // the length of a key in an object

class ObjectBuffer : public Buffer {
  public:

    ObjectBuffer()
    {
    }
    ~ObjectBuffer()
    {
    }

    KeyCount getNumKeys();
    const void *getKey(KeyCount keyIndex = 0);
    KeyLength getKeyLength(KeyCount keyIndex = 0);
    // callers can pass a valid reference as argument if they want the length
    // of the value
    const void *getValue(uint32_t *dataLength = NULL);
    uint32_t getValueOffset();

    DISALLOW_COPY_AND_ASSIGN(ObjectBuffer);
};

} //namespace

#endif // RAMCLOUD_OBJECTBUFFER_H
