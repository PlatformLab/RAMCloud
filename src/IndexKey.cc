/* Copyright (c) 2014 Stanford University
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

#include "IndexKey.h"

namespace RAMCloud {

/**
 * Compare the keys and return their comparison.
 *
 * \param key1
 *      Actual bytes of first key to compare.
 * \param keyLength1
 *      Length of key1.
 *      The value 0 corresponds to NULL key1 which indicates
 *      lowest possible key.
 * \param key2
 *      Actual bytes of second key to compare.
 * \param keyLength2
 *      Length of key2.
 *      The value 0 corresponds to NULL key2 which indicates
 *      highest possible key.
 * 
 * \return
 *      Value of 0 if the keys are equal,
 *      negative value if key1 is lexicographically < key2,
 *      positive value if key1 is lexicographically > key2.
 */
int
IndexKey::keyCompare(const void* key1, uint16_t keyLength1,
        const void* key2, uint16_t keyLength2)
{
    RAMCLOUD_LOG(DEBUG, "Comparing keys: %s vs %s",
        string(reinterpret_cast<const char*>(key1), keyLength1).c_str(),
        string(reinterpret_cast<const char*>(key2), keyLength2).c_str());

    if (keyLength2 == 0) {
        return -1;
    }

    int keyCmp = bcmp(key1, key2, std::min(keyLength1, keyLength2));
    if (keyCmp != 0) {
        return keyCmp;
    } else {
        return keyLength1 - keyLength2;
    }
}

/**
 * Compare the object's key corresponding to index id specified in keyRange
 * with the first and last keys in keyRange to determine if the key falls
 * in the keyRange, including the end points.
 *
 * \param object
 *      Object for which the key is to be compared.
 * \param keyRange
 *      IndexKeyRange specifying the parameters of comparison.
 *
 * \return
 *      Value of true if key corresponding to index id specified in
 *      keyRange, say k, is such that lexicographically it falls in
 *      the range specified by [first key, last key] in keyRange,
 *      including end points.
 */
bool
IndexKey::isKeyInRange(Object* object, IndexKeyRange* keyRange)
{
    uint16_t keyLength;
    const void* key = object->getKey(keyRange->indexId, &keyLength);

    if (keyRange->flags == IndexKeyRange::INCLUDE_BOTH &&
        keyCompare(keyRange->firstKey, keyRange->firstKeyLength,
                   key, keyLength) <= 0 &&
        keyCompare(keyRange->lastKey, keyRange->lastKeyLength,
                   key, keyLength) >= 0) {
        return true;
    } else if (keyRange->flags == IndexKeyRange::EXCLUDE_FIRST &&
        keyCompare(keyRange->firstKey, keyRange->firstKeyLength,
                   key, keyLength) < 0 &&
        keyCompare(keyRange->lastKey, keyRange->lastKeyLength,
                   key, keyLength) >= 0) {
        return true;
    } else if (keyRange->flags == IndexKeyRange::EXCLUDE_LAST &&
        keyCompare(keyRange->firstKey, keyRange->firstKeyLength,
                   key, keyLength) <= 0 &&
        keyCompare(keyRange->lastKey, keyRange->lastKeyLength,
                   key, keyLength) > 0) {
        return true;
    } else if (keyRange->flags == IndexKeyRange::EXCLUDE_BOTH &&
        keyCompare(keyRange->firstKey, keyRange->firstKeyLength,
                   key, keyLength) < 0 &&
        keyCompare(keyRange->lastKey, keyRange->lastKeyLength,
                   key, keyLength) > 0) {
        return true;
    } else {
        return false;
    }
}

} //namespace
