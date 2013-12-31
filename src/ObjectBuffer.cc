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

#include "ObjectBuffer.h"
#include "Object.h"

namespace RAMCloud {

/**
 * Returns the number of keys present in this buffer where the buffer represents
 * an object as returned by a read RPC

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
 * contiguous in memory. If keyIndex is not < numkeys, then behavior is
 * undefined.

 * \param[in] keyIndex
 *      Identifies the desired key (o means first key)
 * \return
 *      Pointer to a key or NULL if there is no key at
 *      the given index position
 */
const void*
ObjectBuffer::getKey(KeyCount keyIndex)
{
    // create a dummy object just so that we can invoke methods from
    // the object class rather than redoing it in this class.
    Object object(1, 0, 0, *this);
    return object.getKey(keyIndex);
}

/**
 * Obtain the length of a key in the object buffer.
 * If keyIndex is not < numkeys, then behavior is undefined.

 * \param[in] keyIndex
 *      Identifies the desired key (o means first key)
 * \return
 *      Length of a key
 */
KeyLength
ObjectBuffer::getKeyLength(KeyCount keyIndex)
{
    Object object(1, 0, 0, *this);
    return object.getKeyLength(keyIndex);
}

/**
 * Obtain a pointer to a contiguous copy of the data blob. Note
 * that if the data is not already contiguous, it will be copied.

 * \param[out] valueLength
 *      The length of the data blob in this object buffer
 * \return
 *      The value portion of the object
 */
const void *
ObjectBuffer::getValue(uint32_t *valueLength)
{
    Object object(1, 0, 0, *this);
    return object.getValue(valueLength);
}

/**
 * Obtain the offset of the 'value' portion in the ObjectBuffer.
 *
 * \return
 *      The offset of the value portion of the object
 */
uint32_t
ObjectBuffer::getValueOffset()
{

    Object object(1, 0, 0, *this);
    return object.getValueOffset();
}

} // namespace
