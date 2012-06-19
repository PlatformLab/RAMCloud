/* Copyright (c) 2009 Stanford University
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

#ifndef RAMCLOUD_KEYHASH_H
#define RAMCLOUD_KEYHASH_H

#include "MurmurHash3.h"

namespace RAMCloud {

/**
 * The type of the hash for the key of an object.
 */
typedef uint64_t HashType;

/**
 * Given a key, returns its hash value.
 *
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated like a string.
 * \param keyLength
 *      Size in bytes of the key.
 * \return
 *      Hash value of the given key.
 */
static inline HashType
getKeyHash(const char* key, uint16_t keyLength)
{
    uint64_t keyHash[2];
    MurmurHash3_x64_128(key, keyLength, 0, keyHash);
    return keyHash[0];
}
} // end RAMCloud

#endif // RAMCLOUD_KEYHASH_H
