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

#ifndef RAMCLOUD_INDEXLETMANAGER_H
#define RAMCLOUD_INDEXLETMANAGER_H

#include <boost/unordered_map.hpp>

#include "Common.h"
#include "HashTable.h"
#include "SpinLock.h"
#include "Object.h"

namespace RAMCloud {

/**
 * This class is on every index server. It manages and stores the metadata
 * regarding indexlets (index partitions) stored on this server.
 *
 * The coordinator invokes most of these functions to manage the metadata.
 * It is responsible for storing index entries in an index server for each
 * indexlet it owns. This class interfaces with ObjectManager and index tree
 * code to manage index entries.
 *
 * Note: Every master server is also an index server for some partition
 * of some index (independent from data partition located on that master).
 */
class IndexletManager {
  PUBLIC:

    struct KeyRange {
        /// Id of the index to which these index keys to be matched belong.
        const uint8_t indexId;
        /// Key blob marking the start of the acceptable range for indexed key.
        const void* firstKey;
        /// Length of firstKey.
        const uint16_t firstKeyLength;
        /// Key blob marking the end of the acceptable range for indexed key.
        const void* lastKey;
        /// Length of lastKey.
        const uint16_t lastKeyLength;
    };

    /**
     * Each indexlet owned by a master is described by fields in this class.
     * Indexlets describe contiguous ranges of secondary key space for a
     * particular index for a given table.
     */
    class Indexlet {
      PUBLIC:
        Indexlet()
            : firstKey(NULL)
            , firstKeyLength(-1)
            , firstNotOwnedKey(NULL)
            , firstNotOwnedKeyLength(-1)
        {}

        Indexlet(const void *firstKey, uint16_t firstKeyLength,
                 const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength)
            : firstKey(firstKey)
            , firstKeyLength(firstKeyLength)
            , firstNotOwnedKey(firstNotOwnedKey)
            , firstNotOwnedKeyLength(firstNotOwnedKeyLength)
        {}

        /// Blob for the smallest key that is in this indexlet. The key is
        /// malloced during creation of index on coordinator, and is freed by
        /// the coordinator when the index is deleted.
        const void *firstKey;

        /// Length of the firstKey.
        uint16_t firstKeyLength;

        /// Blob for the first key in the key space for the owning index, which
        /// is not present in this indexlet. Storage same as firstKey.
        const void *firstNotOwnedKey;

        /// Length of the firstNotOwnedKey.
        uint16_t firstNotOwnedKeyLength;
    };


    explicit IndexletManager(Context* context);

    /////////////////////////// Meta-data related functions //////////////////

    bool addIndexlet(uint64_t tableId, uint8_t indexId,
                 const void *firstKey, uint16_t firstKeyLength,
                 const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength);
    bool deleteIndexlet(uint64_t tableId, uint8_t indexId,
                const void *firstKey, uint16_t firstKeyLength,
                const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength);
    struct Indexlet* getIndexlet(uint64_t tableId, uint8_t indexId,
                 const void *firstKey, uint16_t firstKeyLength,
                 const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength);
    size_t getCount();

    /////////////////////////// Index data related functions //////////////////

    Status insertEntry(uint64_t tableId, uint8_t indexId,
                       const void* keyStr, KeyLength keyLength,
                       uint64_t pKHash);
    Status lookupIndexKeys(uint64_t tableId, uint8_t indexId,
                           const void* firstKeyStr, KeyLength firstKeyLength,
                           const void* lastKeyStr, KeyLength lastKeyLength,
                           uint32_t* count, Buffer* outBuffer);
    Status removeEntry(uint64_t tableId, uint8_t indexId,
                       const void* keyStr, KeyLength keyLength,
                       uint64_t pKHash);

    //////////////// Static function related to indexing info /////////////////
    static bool compareKey(Object* object, KeyRange* keyRange);

  PRIVATE:
    /**
     * Shared RAMCloud information.
     */
    Context* context;

    /// This unordered_multimap is used to store and access all indexlet data.
    typedef boost::unordered_multimap< std::pair<uint64_t, uint8_t>,
                                       Indexlet> IndexletMap;

    /// Lock guard type used to hold the monitor spinlock and automatically
    /// release it.
    typedef std::lock_guard<SpinLock> Lock;

    IndexletMap::iterator lookup(uint64_t tableId, uint8_t indexId,
                              const void *key, uint16_t keyLength, Lock& lock);

    IndexletMap indexletMap;

    /// Monitor spinlock used to protect the indexletMap from concurrent access.
    SpinLock lock;

    DISALLOW_COPY_AND_ASSIGN(IndexletManager);
};

} // end RAMCloud

#endif  // RAMCLOUD_INDEXLETMANAGER_H
