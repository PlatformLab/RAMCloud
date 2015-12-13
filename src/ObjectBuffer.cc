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

#include "ObjectBuffer.h"
#include "Object.h"

namespace RAMCloud {

/**
 * Returns the number of keys present in this buffer

 * \return
 *      The number of keys in the buffer
 */
KeyCount
ObjectBuffer::getNumKeys()
{
    return *getOffset<KeyCount>(0);
}

/**
 * Returns a pointer to one of the keys for the current object. The key will be
 * contiguous in memory.
 *
 * \param[in] keyIndex
 *      Identifies the desired key (o means first key)
 * \return
 *      Pointer to a key or NULL if there is no key at
 *      the given index position
 */
const void*
ObjectBuffer::getKey(KeyIndex keyIndex)
{
    // create a dummy object just so that we can invoke methods from
    // the object class rather than redoing it in this class.
    if (!object)
        object.construct(1, 0, 0, *this);
    return object.get()->getKey(keyIndex);
}

/**
 * Obtain the length of a key in the object buffer.

 * \param[in] keyIndex
 *      Identifies the desired key (o means first key)
 * \return
 *      Length of a key
 */
KeyLength
ObjectBuffer::getKeyLength(KeyIndex keyIndex)
{
    if (!object)
        object.construct(1, 0, 0, *this);
    return object.get()->getKeyLength(keyIndex);
}

/**
 * Obtain a pointer to a contiguous copy of the object's value. Note
 * that if the value is not already contiguous, it will be copied.
 * NOTE: if the value of the object is large, this function will
 * be expensive as it may involve copying the data.

 * \param[out] valueLength
 *      The length of the data blob in this object buffer
 * \return
 *      The value portion of the object
 */
const void *
ObjectBuffer::getValue(uint32_t *valueLength)
{
    if (!object)
        object.construct(1, 0, 0, *this);
    return object.get()->getValue(valueLength);
}

/**
 * Obtain the offset of the 'value' portion in the ObjectBuffer.
 *
 * \return
 *      The offset of the value portion of the object
 */
bool
ObjectBuffer::getValueOffset(uint32_t *offset)
{
    if (!object)
        object.construct(1, 0, 0, *this);
    return object.get()->getValueOffset(offset);
}

/**
 * Convenience method to obtain the 64-bit version number associated
 * with the Object.
 */
uint64_t
ObjectBuffer::getVersion() {
    if (!object)
        object.construct(1, 0, 0, *this);
    return object.get()->getVersion();
}

/**
 * Obtain a pointer to the Object contained with the ObjectBuffer. The
 * pointer will remain valid until reset() or the destructor is invoked
 * on the associated ObjectBuffer.
 *
 * /return a pointer to the object.
 */
Object*
ObjectBuffer::getObject() {
    if (!object)
        object.construct(1, 0, 0, *this);
    return object.get();
}

/**
 * Overrides Buffer::reset(). Frees the object that is encapsulated
 * within this ObjectBuffer.
 */
void
ObjectBuffer::reset()
{
    object.destroy();
    Buffer::reset();
}
} // namespace
