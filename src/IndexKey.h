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

    /// Class used to define a range of keys [first key, last key]
    /// for a particular index id, that can be used to compare a given
    /// object to determine if its corresponding key falls in this range.
    class IndexKeyRange {
      PUBLIC:
        enum BoundaryFlags : uint32_t {
            /// If INCLUDE_BOTH is set, the range is inclusive of both
            /// firstKey and lastKey.
            /// i.e., the range is [firstKey, lastKey].
            INCLUDE_BOTH = 0,
            /// If EXCLUDE_FIRST is set, the range is exclusive of firstKey,
            /// and inclusive of lastKey.
            /// i.e., the range is (firstKey, lastKey].
            EXCLUDE_FIRST = 1,
            /// If EXCLUDE_LAST is set, the range is inclusive of fistKey,
            /// and exclusive of lastKey.
            /// i.e., the range is [firstKey, lastKey).
            EXCLUDE_LAST = 2,
            /// If EXCLUDE_BOTH is set, the range is exclusive of both
            /// firstKey and lastKey.
            /// i.e., the range is (firstKey, lastKey).
            EXCLUDE_BOTH = 3,
        };

        /// Id of the index to which these index keys belong.
        const uint8_t indexId;
        /// Key blob marking the start of the key range.
        /// It does not necessarily have to be null terminated. The caller must
        /// ensure that the storage for this key is unchanged through the life
        /// of this object.
        /// NULL value indicates lowest possible key.
        const void* firstKey;
        /// Length of firstKey. 0 value corresponds to NULL firstKey.
        const uint16_t firstKeyLength;
        /// Key blob marking the end of the key range.
        /// It does not necessarily have to be null terminated. The caller must
        /// ensure that the storage for this key is unchanged through the life
        /// of this object.
        /// NULL value indicates highest possible key.
        const void* lastKey;
        /// Length of lastKey. 0 value corresponds to NULL lastKey.
        const uint16_t lastKeyLength;
        /// Flags that specify boundary conditions for this range.
        const BoundaryFlags flags;

        IndexKeyRange(const uint8_t indexId,
                const void* firstKey, const uint16_t firstKeyLength,
                const void* lastKey, const uint16_t lastKeyLength,
                const BoundaryFlags flags = INCLUDE_BOTH)
        : indexId(indexId)
        , firstKey(firstKey)
        , firstKeyLength(firstKeyLength)
        , lastKey(lastKey)
        , lastKeyLength(lastKeyLength)
        , flags(flags)
        {}
    };

    static int keyCompare(const void* key1, uint16_t keyLength1,
                          const void* key2, uint16_t keyLength2);
    static bool isKeyInRange(Object* object, IndexKeyRange* keyRange);

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(IndexKey);
};

} // namespace RAMCloud

#endif

