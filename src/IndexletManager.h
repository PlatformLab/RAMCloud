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
#include "SpinLock.h"

namespace RAMCloud {

/**
 * This class is on every index server.
 * It manages and stores the metadata regarding indexlets (index partitions)
 * stored on this server.
 *
 * The coordinator invokes most of these functions to manage the metadata.
 * 
 * This class has no information about the actual entries in the index
 * (and does not interface with ObjectManager or index tree code).
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

    IndexletManager();

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

  PRIVATE:
    /**
     * Shared RAMCloud information.
     */
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
