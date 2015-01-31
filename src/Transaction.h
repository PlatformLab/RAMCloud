/* Copyright (c) 2015 Stanford University
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

#ifndef RAMCLOUD_TRANSACTION_H
#define RAMCLOUD_TRANSACTION_H

#include <map>
#include <memory>

#include "RamCloud.h"


namespace RAMCloud {

class Transaction {
  PUBLIC:
    explicit Transaction(RamCloud* ramcloud);

    bool commit();

    void read(uint64_t tableId, const void* key, uint16_t keyLength,
            Buffer* value);

    void remove(uint64_t tableId, const void* key, uint16_t keyLength);

    void write(uint64_t tableId, const void* key, uint16_t keyLength,
            const void* buf, uint32_t length);

  PRIVATE:
    /// Overall client state information.
    RamCloud* ramcloud;

    /// Number of participant objects/operations.
    uint32_t participantCount;
    /// List of participant object identifiers.
    Buffer participantList;

    /**
     * Structure to define the key search value for the CommitCache map.
     */
    struct CacheKey {
        uint64_t tableId;       // tableId of the tablet
        KeyHash keyHash;        // start key hash value

        /**
         * The operator < is overridden to implement the
         * correct comparison for the CommitCache map.
         */
        bool operator<(const CacheKey& key) const {
            return tableId < key.tableId ||
                (tableId == key.tableId && keyHash < key.keyHash);
        }
    };

    /**
     * Structure to define the contents of the CommitCache.
     */
    struct CacheEntry {
        enum Type { READ, REMOVE, WRITE, INVALID };
        /// Type of the cached object entry.  Used to specify what kind of
        /// transaction operation needs to be performed during commit.
        Type type;
        /// Cached object value.  Used to service reads and store values for
        /// committing writes.  Ideally this would be a unique pointer to
        /// manage the memory automatically but std::multimap is missing the
        /// emplace feature.
        ObjectBuffer* objectBuf;
        /// Conditions upon which the transaction operation associated with
        /// this object should abort.
        RejectRules rejectRules;

        /// Default constructor for CacheEntry.
        CacheEntry()
            : type(CacheEntry::INVALID)
            , objectBuf(NULL)
            , rejectRules({0, 0, 0, 0, 0})
        {}

        /// Copy constructor for CacheEntry, used to get around the missing
        /// emplace feature in std::multimap.
        explicit CacheEntry(const CacheEntry& other)
            : type(other.type)
            , objectBuf(other.objectBuf)
            , rejectRules(other.rejectRules)
        {}

        /// Destructor for CacheEntry.
        ///
        /// Warning: Multiple copies of CacheEntry objects may cause the
        /// ObjectBuffer pointed in the entry to be double freed.  This
        /// is indirectly due to missing emplace feature in std::multimap.
        ~CacheEntry()
        {
            if (objectBuf)
                delete objectBuf;
        }

        /// Assignment operator for CacheEntry, used to get around the missing
        /// emplace feature in std::multimap.
        CacheEntry& operator=(const CacheEntry& other)
        {
            if (this != &other) {
                type = other.type;
                objectBuf = other.objectBuf;
                rejectRules = other.rejectRules;
            }
            return *this;
        }

    };

    /**
     * The Commit Cache is used to keep track of the  transaction operations to
     * be performed during commit and well as cache read and write values to
     * services subsequent reads.
     */
    typedef std::multimap<CacheKey, CacheEntry> CommitCacheMap;
    CommitCacheMap commitCache;

    CacheEntry* findCacheEntry(Key& key);
    CacheEntry* insertCacheEntry(uint64_t tableId, const void* key,
            uint16_t keyLength, const void* buf, uint32_t length);


    DISALLOW_COPY_AND_ASSIGN(Transaction);
};

} // end RAMCloud

#endif  /* RAMCLOUD_TRANSACTION_H */

