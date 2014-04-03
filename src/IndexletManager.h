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
#include "Indexlet.h"
#include "btree/BtreeMultimap.h"

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

    /// Structure used as the key for key-value pairs in the indexlet tree.
    struct KeyAndHash {
        /// Actual bytes of the index key.
        void* key;
        /// Length of index key.
        uint16_t keyLength;
        /// Primary key hash of the object the index key points to.
        /// This is currently being stored as a part of the tree key to enable
        /// sorting on the key hash if there are multiple index keys with the
        /// same value of key and keyLength.
        uint64_t pKHash;

        // btree code uses  empty constructor for keys
        KeyAndHash()
        : key()
        , keyLength()
        , pKHash()
        {}

        KeyAndHash(const void *key, uint16_t keyLength, uint64_t pKHash)
        : key(NULL)
        , keyLength(keyLength)
        , pKHash(pKHash)
        {
            if (keyLength != 0){
                this->key = malloc(keyLength);
                memcpy(this->key, key, keyLength);
            }
        }

        KeyAndHash(const KeyAndHash& keyAndHash)
        : key(NULL)
        , keyLength(keyAndHash.keyLength)
        , pKHash(keyAndHash.pKHash)
        {
            if (keyLength != 0){
                this->key = malloc(keyLength);
                memcpy(this->key, keyAndHash.key, keyLength);
            }
        }

        KeyAndHash& operator =(const KeyAndHash& keyAndHash)
        {
            this->key = NULL;
            this->keyLength = keyAndHash.keyLength;
            this->pKHash = keyAndHash.pKHash;

            if (keyLength != 0){
                this->key = malloc(keyLength);
                memcpy(this->key, keyAndHash.key, keyLength);
            }
            return *this;
        }

        ~KeyAndHash()
        {
            if (keyLength != 0)
                free(key);
        }
    };

    /// Structure used to define a range of keys [first key, last key]
    /// for a particular index id, that can be used to compare a given
    /// object to determine if its corresponding key falls in this range.
    struct KeyRange {
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

    /**
     * Each indexlet owned by a master is described by fields in this class.
     * Indexlets describe contiguous ranges of secondary key space for a
     * particular index for a given table.
     */
    class Indexlet : public RAMCloud::Indexlet {
        public:
        Indexlet(const void *firstKey, uint16_t firstKeyLength,
                  const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength)
            : RAMCloud::Indexlet(firstKey, firstKeyLength, firstNotOwnedKey,
                       firstNotOwnedKeyLength)
            , bt()
        {}

        Indexlet(const Indexlet& indexlet)
            : RAMCloud::Indexlet(indexlet)
            , bt(indexlet.bt)
        {}

        // Btree key compare function
        struct KeyAndHashCompare
        {
          public:
            bool operator()(const KeyAndHash x, const KeyAndHash y) const
            {
                int keyComparison = keyCompare(x.key, x.keyLength,
                                               y.key, y.keyLength);
                if (keyComparison == 0) {
                    return (x.pKHash < y.pKHash);
                }
                return keyComparison < 0;
            }
        };

        // Attributes of the b+ tree used for holding the indexes
        template <typename KeyType>
        struct traits_nodebug : stx::btree_default_set_traits<KeyType>
        {
            static const bool selfverify = false;
            static const bool debug = false;

            static const int leafslots = 8;
            static const int innerslots = 8;
        };

        // B+ tree holding key: string, value: primary key hash
        // TODO(ashgup): Do we need to define traits?
        typedef stx::btree_multimap<KeyAndHash, uint64_t,
                KeyAndHashCompare> Btree;

        Btree bt;
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
                const void* key, KeyLength keyLength,
                uint64_t pKHash);
    Status lookupIndexKeys(uint64_t tableId, uint8_t indexId,
                const void* firstKey, KeyLength firstKeyLength,
                uint64_t firstAllowedKeyHash,
                const void* lastKey, uint16_t lastKeyLength,
                uint32_t maxNumHashes,
                Buffer* responseBuffer, uint32_t* numHashes,
                uint16_t* nextKeyLength, uint64_t* nextKeyHash);
    Status removeEntry(uint64_t tableId, uint8_t indexId,
                const void* key, KeyLength keyLength,
                uint64_t pKHash);

    //////////////// Static function related to indexing info /////////////////

    static bool isKeyInRange(Object* object, KeyRange* keyRange);

    static int keyCompare(const void* key1, uint16_t keyLength1,
                const void* key2, uint16_t keyLength2);

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

    IndexletMap::iterator lookupIndexlet(uint64_t tableId, uint8_t indexId,
                const void *key, uint16_t keyLength, Lock& lock);

    IndexletMap indexletMap;

    /// Monitor spinlock used to protect the indexletMap from concurrent access.
    SpinLock lock;

    DISALLOW_COPY_AND_ASSIGN(IndexletManager);
};

} // end RAMCloud

#endif  // RAMCLOUD_INDEXLETMANAGER_H
