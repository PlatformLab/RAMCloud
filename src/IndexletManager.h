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

#include <unordered_map>

#include "Common.h"
#include "HashTable.h"
#include "SpinLock.h"
#include "Object.h"
#include "Indexlet.h"
#include "IndexKey.h"
#include "ObjectManager.h"
#include "btreeRamCloud/BtreeMultimap.h"

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
        char key[40];
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
        : key()
        , keyLength(keyLength)
        , pKHash(pKHash)
        {
            memcpy(this->key, key, keyLength);
        }

        KeyAndHash(const KeyAndHash& keyAndHash)
        : key()
        , keyLength(keyAndHash.keyLength)
        , pKHash(keyAndHash.pKHash)
        {
            memcpy(this->key, keyAndHash.key, keyLength);
        }

        KeyAndHash& operator =(const KeyAndHash& keyAndHash)
        {
            this->keyLength = keyAndHash.keyLength;
            this->pKHash = keyAndHash.pKHash;
            memcpy(this->key, keyAndHash.key, keyLength);
            return *this;
        }
    };

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


    // B+ tree holding key: string, value: primary key hash
    // TODO(ashgup): Do we need to define traits?
    typedef str::btree_multimap<KeyAndHash, uint64_t, KeyAndHashCompare> Btree;

    /**
     * Each indexlet owned by a master is described by fields in this class.
     * Indexlets describe contiguous ranges of secondary key space for a
     * particular index for a given table.
     */
    class Indexlet : public RAMCloud::Indexlet {
        public:
        Indexlet(const void *firstKey, uint16_t firstKeyLength,
                 const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength,
                 Btree *bt)
            : RAMCloud::Indexlet(firstKey, firstKeyLength, firstNotOwnedKey,
                                 firstNotOwnedKeyLength)
            , bt(bt)
            , indexletMutex()
        {
        }

        Indexlet(const Indexlet& indexlet)
            : RAMCloud::Indexlet(indexlet)
            , bt(indexlet.bt)
            , indexletMutex()
        {}

        Indexlet& operator =(const Indexlet& indexlet)
        {
            this->firstKey = NULL;
            this->firstKeyLength = indexlet.firstKeyLength;
            this->firstNotOwnedKey = NULL;
            this->firstNotOwnedKeyLength = indexlet.firstNotOwnedKeyLength;

            if (firstKeyLength != 0){
                this->firstKey = malloc(firstKeyLength);
                memcpy(this->firstKey, indexlet.firstKey, firstKeyLength);
            }
            if (firstNotOwnedKeyLength != 0){
                this->firstNotOwnedKey = malloc(firstNotOwnedKeyLength);
                memcpy(this->firstNotOwnedKey, indexlet.firstNotOwnedKey,
                                                        firstNotOwnedKeyLength);
            }

            this->bt = indexlet.bt;
            return *this;
        }

        // Attributes of the b+ tree used for holding the indexes.
        // Note: traits are not currently used during btree initialization.
        template <typename KeyType>
        struct traits_nodebug : str::btree_default_set_traits<KeyType>
        {
            static const bool selfverify = false;
            static const bool debug = false;

            static const int leafslots = 8;
            static const int innerslots = 8;
        };

        Btree *bt;

        /// Mutex to protect the indexlet from concurrent access.
        /// A lock for this mutex MUST be held to read or modify any state in
        /// the indexlet.
        SpinLock indexletMutex;
    };

    explicit IndexletManager(Context* context, ObjectManager* objectManager);

    /////////////////////////// Meta-data related functions //////////////////

    bool addIndexlet(uint64_t tableId, uint8_t indexId,
                uint64_t indexletTableId, const void *firstKey,
                uint16_t firstKeyLength, const void *firstNotOwnedKey,
                uint16_t firstNotOwnedKeyLength);
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

    static bool isKeyInRange(Object* object, IndexKeyRange* keyRange);

    static int keyCompare(const void* key1, uint16_t keyLength1,
                const void* key2, uint16_t keyLength2);

  PROTECTED:
    // Note: I'm using unique_lock (instead of lock_guard) with mutex because
    // this allows me to explictly release the lock anywhere. I'm not sure
    // if this will perform as well as lock_guard.
    /// Lock type used to hold the mutex.
    /// This lock can be released explicitly in the code, but will be
    /// automatically released at the end of a function if not done explicitly.
    typedef std::unique_lock<SpinLock> Lock;

  PRIVATE:
    /// Shared RAMCloud information.
    Context* context;

    /// Table/key pair hash struct for unordered_multimap below
    struct tableKeyHash
    {
        size_t operator()(const std::pair<uint64_t, uint8_t> tableKey) const
        {
            return std::hash<uint64_t>()(tableKey.first)
                    ^std::hash<uint8_t>()(tableKey.second);
        }
    };

    /// This unordered_multimap is used to store and access all indexlet data.
    typedef std::unordered_multimap< std::pair<uint64_t, uint8_t>,
                                     Indexlet, tableKeyHash> IndexletMap;

    /// Indexlet map instance storing indexlet mapping for this index server.
    IndexletMap indexletMap;

    /// Mutex to protect the indexletMap from concurrent access.
    /// A lock for this mutex MUST be held to read or modify any state in
    /// the indexletMap.
    SpinLock indexletMapMutex;

    /// Object Manager to handle mapping of index as objects
    ObjectManager* objectManager;

    IndexletMap::iterator lookupIndexlet(uint64_t tableId, uint8_t indexId,
                const void *key, uint16_t keyLength, Lock& indexletMapMutex);

    DISALLOW_COPY_AND_ASSIGN(IndexletManager);
};

} // end RAMCloud

#endif  // RAMCLOUD_INDEXLETMANAGER_H
