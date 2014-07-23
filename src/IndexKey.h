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

#ifndef RAMCLOUD_INDEXKEY_H
#define RAMCLOUD_INDEXKEY_H

#include "Common.h"
#include "Object.h"

namespace RAMCloud {

class IndexKey {

  PUBLIC:

    /// Structure used to define a range of keys [first key, last key]
    /// for a particular index id, that can be used to compare a given
    /// object to determine if its corresponding key falls in this range.
    struct IndexKeyRange {
        /// Id of the index to which these index keys belong.
        const uint8_t indexId;
        /// Key blob marking the start of the key range.
        const void* firstKey;
        /// Length of firstKey.
        const uint16_t firstKeyLength;
        /// Key blob marking the end of the key range.
        const void* lastKey;
        /// Length of lastKey.
        const uint16_t lastKeyLength;
    };

    static int keyCompare(const void* key1, uint16_t keyLength1,
                          const void* key2, uint16_t keyLength2);
    static bool isKeyInRange(Object* object, IndexKeyRange* keyRange);

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(IndexKey);
};

} // namespace RAMCloud

#endif

