/* Copyright (c) 2010-2014 Stanford University
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
#include "ObjectManager.h"
#include "SpinLock.h"

namespace RAMCloud {

/**
 * This class is on every index server.
 * It manages and stores the metadata regarding indexlets (index partitions)
 * stored on this server.
 *
 * The coordinator invokes most of these functions to manage the metadata.
 * 
 * It is responsible for storing index entries in an index server for each
 * indexlet it owns.
 * This class interfaces with ObjectManager and index tree code to manage
 * index entries.
 * 
 * Note: Every master server is also an index server for some partition
 * of some index (independent from data partition located on that master).
 */

class IndexletManager {
  PUBLIC:

    class Indexlet {
      PUBLIC:
        Indexlet()
            : firstKeyLength(-1)
            , firstKey(NULL)
            , lastKeyLength(-1)
            , lastKey(NULL)
        {}

        Indexlet(uint16_t firstKeyLength, const void *firstKey,
                 uint16_t lastKeyLength, const void *lastKey)
            : firstKeyLength(firstKeyLength)
            , firstKey(firstKey)
            , lastKeyLength(lastKeyLength)
            , lastKey(lastKey)
        {}

        //TODO(ashgup): need to have indexlet id for rpcs

        /// The smallest value for a key that is in this indexlet.
        uint16_t firstKeyLength;
        const void *firstKey; //TODO(ashgup): change this to buffer

        /// The largest value for a key that is in this indexlet.
        uint16_t lastKeyLength;
        const void *lastKey; //TODO(ashgup): change this to buffer
    };

    IndexletManager(Context* context, ObjectManager* objectManager);
    virtual ~IndexletManager();

    /////////////////////////// Meta-data related functions //////////////////
    // Modify function signature as required. This is just an approximation.
    bool addIndexlet(uint64_t tableId, uint8_t indexId,
                     uint16_t firstKeyLength, const void *firstKey,
                     uint16_t lastKeyLength, const void *lastKey);

    bool deleteIndexlet(uint64_t tableId, uint8_t indexId,
                        uint16_t firstKeyLength, const void *firstKey,
                        uint16_t lastKeyLength, const void *lastKey);

    bool getIndexlet(uint64_t tableId, uint8_t indexId,
                     uint16_t firstKeyLength, const void *firstKey,
                     uint16_t lastKeyLength, const void *lastKey,
                     Indexlet* outIndexlet = NULL);

    /////////////////////////// Index data related functions //////////////////

    void initIndexlet(uint64_t tableId, uint8_t indexId,
                      const void* firstKeyStr, KeyLength firstKeyLength,
                      const void* lastKeyStr, KeyLength lastKeyLength);
    void deleteIndexletEntries(uint64_t tableId, uint8_t indexId);

    bool indexedRead(uint64_t tableId, uint8_t indexId, uint64_t pKHash,
                     const void* firstKeyStr, KeyLength firstKeyLength,
                     const void* lastKeyStr, KeyLength lastKeyLength,
                     uint32_t* numObjects, Buffer* outBuffer,
                     vector<uint32_t>* lengths, vector<uint64_t>* versions);
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

  PRIVATE:
    /**
     * Shared RAMCloud information.
     */
    Context* context;

    /**
     * The ObjectManager class that is responsible for object storage for this
     * master server.
     */
    ObjectManager* objectManager;

    typedef boost::unordered_multimap< std::pair<uint64_t, uint8_t>,
                                       Indexlet> IndexletMap;

    /// Lock guard type used to hold the monitor spinlock and automatically
    /// release it.
    typedef std::lock_guard<SpinLock> Lock;


    IndexletMap::iterator lookup(uint64_t tableId, uint8_t indexId,
                               uint16_t keyLength, const void *key, Lock& lock);
    /// This unordered_multimap is used to store and access all tablet data.
    IndexletMap indexletMap;

    /// Monitor spinlock used to protect the tabletMap from concurrent access.
    SpinLock lock;

    DISALLOW_COPY_AND_ASSIGN(IndexletManager);
};

} // end RAMCloud

#endif  // RAMCLOUD_INDEXLETMANAGER_H
