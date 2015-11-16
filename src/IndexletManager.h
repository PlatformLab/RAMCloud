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

#ifndef RAMCLOUD_INDEXLETMANAGER_H
#define RAMCLOUD_INDEXLETMANAGER_H

#include <unordered_map>

#include "btreeRamCloud/Btree.h"
#include "Common.h"
#include "HashTable.h"
#include "SpinLock.h"
#include "Object.h"
#include "Indexlet.h"
#include "IndexKey.h"
#include "ObjectManager.h"
#include "Service.h"
#include "WireFormat.h"

namespace RAMCloud {

/**
 * This class manages and stores the metadata regarding indexlets
 * (index partitions) stored on this server.
 * The coordinator invokes metadata-related functions to manage the metadata.
 *
 * This class is also provides the interface for storing index entries on this
 * index server for each indexlet it owns, to the masters owning the
 * data objects corresponding to those index entries.
 * This class interfaces with ObjectManager and index tree code to manage
 * the index entries.
 *
 * Note: There is no seprate index service.
 * A server that has the master service can also serve as an index server
 * for some partition of some index (independent from tablet located on
 * that master).
 */
class IndexletManager {
  PUBLIC:

    /**
     * Each indexlet owned by a master is described by fields in this class.
     * Indexlets describe contiguous ranges of secondary key space for a
     * particular index for a given table.
     */
    class Indexlet : public RAMCloud::Indexlet {
      public:
        enum State : uint8_t {
            /// The indexlet is available.
            NORMAL = 0 ,
            /// The indexlet is being recovered, it is not available.
            RECOVERING = 1,
        };

        Indexlet(const void *firstKey, uint16_t firstKeyLength,
                 const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength,
                 IndexBtree *bt, IndexletManager::Indexlet::State state)
            : RAMCloud::Indexlet(firstKey, firstKeyLength, firstNotOwnedKey,
                                 firstNotOwnedKeyLength)
            , bt(bt)
            , state(state)
            , indexletMutex()
        {
        }

        Indexlet(const Indexlet& indexlet)
            : RAMCloud::Indexlet(indexlet)
            , bt(indexlet.bt)
            , state(indexlet.state)
            , indexletMutex()
        {}

        Indexlet& operator =(const Indexlet& indexlet)
        {
            this->firstKey = NULL;
            this->firstKeyLength = indexlet.firstKeyLength;
            this->firstNotOwnedKey = NULL;
            this->firstNotOwnedKeyLength = indexlet.firstNotOwnedKeyLength;

            if (firstKeyLength != 0) {
                this->firstKey = malloc(firstKeyLength);
                memcpy(this->firstKey, indexlet.firstKey, firstKeyLength);
            }
            if (firstNotOwnedKeyLength != 0) {
                this->firstNotOwnedKey = malloc(firstNotOwnedKeyLength);
                memcpy(this->firstNotOwnedKey, indexlet.firstNotOwnedKey,
                       firstNotOwnedKeyLength);
            }

            this->bt = indexlet.bt;
            this->state = indexlet.state;
            return *this;
        }

        IndexBtree *bt;

        /// The state of the tablet, see State.
        State state;

        /// Mutex to protect the indexlet from concurrent access.
        /// A lock for this mutex MUST be held to read or modify any state in
        /// the indexlet.
        SpinLock indexletMutex;
    };

    /////////////////////////// Meta-data related functions //////////////////

    bool addIndexlet(uint64_t tableId, uint8_t indexId,
            uint64_t backingTableId,
            const void *firstKey, uint16_t firstKeyLength,
            const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength,
            IndexletManager::Indexlet::State state =
                    IndexletManager::Indexlet::NORMAL,
            uint64_t nextNodeId = 0);
    bool changeState(uint64_t tableId, uint8_t indexId,
            const void *firstKey, uint16_t firstKeyLength,
            const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength,
            IndexletManager::Indexlet::State oldState,
            IndexletManager::Indexlet::State newState);
    void deleteIndexlet(uint64_t tableId, uint8_t indexId,
            const void *firstKey, uint16_t firstKeyLength,
            const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength);
    IndexletManager::Indexlet* findIndexlet(uint64_t tableId, uint8_t indexId,
            const void *key, uint16_t keyLength);
    size_t getNumIndexlets();
    bool hasIndexlet(uint64_t tableId, uint8_t indexId,
            const void *key, uint16_t keyLength);
    bool isGreaterOrEqual(Buffer* nodeObjectValue,
            const void* compareKey, uint16_t compareKeyLength);
    void truncateIndexlet(uint64_t tableId, uint8_t indexId,
            const void* truncateKey, uint16_t truncateKeyLength);
    void setNextNodeIdIfHigher(uint64_t tableId, uint8_t indexId,
            const void *key, uint16_t keyLength, uint64_t nextNodeId);

    /////////////////////////// Index data related functions //////////////////

    Status insertEntry(uint64_t tableId, uint8_t indexId,
            const void* key, KeyLength keyLength,
            uint64_t pKHash);
    void lookupIndexKeys(const WireFormat::LookupIndexKeys::Request* reqHdr,
            WireFormat::LookupIndexKeys::Response* respHdr,
            Service::Rpc* rpc);
    Status removeEntry(uint64_t tableId, uint8_t indexId,
            const void* key, KeyLength keyLength,
            uint64_t pKHash);

    explicit IndexletManager(Context* context, ObjectManager* objectManager);

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

    /// Structure used to uniquely identify the index of which this
    /// indexlet is a partition.
    struct TableAndIndexId {
        /// Id of the data table for which this indexlet stores some
        /// index information.
        uint64_t tableId;
        /// Id of the index key for which this indexlet stores some information.
        uint8_t indexId;

        /// Equality operator used by comparison functions of the struct
        /// IndexletMap that this struct is a key of.
        bool operator==(const TableAndIndexId x) const
        {
            if (this->tableId == x.tableId && this->indexId == x.indexId)
                return true;
            else
                return false;
        }
    };

    /// Hasher for TableAndIndexId which is the key in the IndexletMap.
    struct TableAndIndexIdHasher
    {
        size_t operator()(const TableAndIndexId tableKey) const
        {
            return std::hash<uint64_t>()(tableKey.tableId)
                    ^std::hash<uint8_t>()(tableKey.indexId);
        }
    };

    /// This unordered_multimap is used to store and access all indexlet data.
    typedef std::unordered_multimap<
            TableAndIndexId, IndexletManager::Indexlet, TableAndIndexIdHasher>
                    IndexletMap;

    /// Indexlet map instance storing indexlet mapping for this index server.
    IndexletMap indexletMap;

    /// Mutex to protect the indexletMap from concurrent access.
    /// A lock for this mutex MUST be held to read or modify any state in
    /// the indexletMap.
    SpinLock mutex;

    /// Object Manager to handle mapping of index as objects
    ObjectManager* objectManager;

    /////////////////////////// Meta-data related functions //////////////////

    IndexletManager::IndexletMap::iterator findIndexlet(
            uint64_t tableId, uint8_t indexId,
            const void* key, uint16_t keyLength, Lock& mutex);
    IndexletManager::IndexletMap::iterator getIndexlet(
            uint64_t tableId, uint8_t indexId,
            const void *firstKey, uint16_t firstKeyLength,
            const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength,
            Lock& mutex);

    /////////////////////////// Index data related functions //////////////////

    bool existsIndexEntry(
            uint64_t tableId, uint8_t indexId,
            const void* key, KeyLength keyLength, uint64_t pKHash);

    DISALLOW_COPY_AND_ASSIGN(IndexletManager);
};

} // end RAMCloud

#endif  // RAMCLOUD_INDEXLETMANAGER_H
