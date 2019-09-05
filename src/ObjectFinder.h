/* Copyright (c) 2010-2016 Stanford University
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

#ifndef RAMCLOUD_OBJECTFINDER_H
#define RAMCLOUD_OBJECTFINDER_H

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#pragma GCC diagnostic ignored "-Weffc++"
#include <boost/function.hpp>
#pragma GCC diagnostic pop

#include <map>

#include "Common.h"
#include "CoordinatorClient.h"
#include "Key.h"
#include "Transport.h"
#include "Tablet.h"
#include "Indexlet.h"

namespace RAMCloud {

/**
 * Structure to define the key search value for the ObjectFinder map.
 */
struct TabletKey {
    uint64_t tableId;       // tableId of the tablet
    KeyHash keyHash;        // start key hash value

    /**
     * The operator < is overridden to implement the
     * correct comparison for the tableMap.
     */
    bool operator<(const TabletKey& key) const {
        return tableId < key.tableId ||
            (tableId == key.tableId && keyHash < key.keyHash);
    }
};

/**
 * The following class holds information about a single indexlet of a given
 * index on a table.
 */
struct IndexletWithLocator {
    /// Details about the indexlet
    Indexlet indexlet;

    /// Used to find the server that stores the indexlet.
    string serviceLocator;

    /// Session corresponding to serviceLocator. This is a cache to avoid
    /// repeated calls to TransportManager; NULL means that we haven't
    /// yet fetched the session from TransportManager.
    Transport::SessionRef session;

    IndexletWithLocator(Indexlet indexlet, string serviceLocator)
        : indexlet(indexlet)
        , serviceLocator(serviceLocator)
        , session(NULL)
    {}

    IndexletWithLocator(const void *firstKey,
                        uint16_t firstKeyLength,
                        const void *firstNotOwnedKey,
                        uint16_t firstNotOwnedKeyLength,
                        string serviceLocator)
        : indexlet(firstKey, firstKeyLength,
                   firstNotOwnedKey, firstNotOwnedKeyLength)
        , serviceLocator(serviceLocator)
        , session(NULL)
    {}
};

/**
 * This structure holds configuration information for a single tablet.
 */
struct TabletWithLocator {
    /// Details about the tablet.
    Tablet tablet;

    /// Used to find the server that stores the tablet.
    string serviceLocator;

    /// Session corresponding to serviceLocator. This is a cache to avoid
    /// repeated calls to TransportManager; NULL means that we haven't
    /// yet fetched the session from TransportManager.
    Transport::SessionRef session;

    /// If the status of the tablet is RECOVERING, this specifies the clock
    /// time in rdtsc ticks before which we should not attempt to load the
    /// configuration information again. If the status of the tablet is
    /// NORMAL, it is simply set to 0.
    const uint64_t nextFetchTime;

    TabletWithLocator(Tablet tablet, string serviceLocator)
        : tablet(tablet)
        , serviceLocator(serviceLocator)
        , session(NULL)
        , nextFetchTime(tablet.status == Tablet::Status::RECOVERING ?
                        Cycles::rdtsc() + Cycles::fromMicroseconds(10000) : 0)
    {}
};

/**
 * This class maps from an object identifier (table and key) to a session
 * that can be used to communicate with the master that stores the object.
 * It retrieves configuration information from the coordinator and caches it.
 * This class is thread-safe.
 */
class ObjectFinder {
  public:
    class TableConfigFetcher; // forward declaration, see full declaration below

    explicit ObjectFinder(Context* context);

    /*
     * Used only for debug purposes. This function created a string
     * representation of the tablets stored in tableMap
     */
    string debugString() const;

    void flush(uint64_t tableId);
    void flushSession(uint64_t tableId, KeyHash keyHash);
    void flushSession(uint64_t tableId, uint8_t indexId,
                      const void* key, KeyLength keyLength);

    Transport::SessionRef lookup(uint64_t tableId, const void* key,
                                 KeyLength keyLength);
    Transport::SessionRef lookup(uint64_t tableId, KeyHash keyHash);

    TabletWithLocator* lookupTablet(uint64_t tableId, KeyHash keyHash);

    void reset();

    Transport::SessionRef tryLookup(uint64_t tableId, const void* key,
                                    KeyLength keyLength);
    Transport::SessionRef tryLookup(uint64_t tableId, KeyHash keyHash);
    Transport::SessionRef tryLookup(uint64_t tableId, uint8_t indexId,
                                    const void* key, KeyLength keyLength,
                                    bool* indexDoesntExist);

    void waitForTabletDown(uint64_t tableId);
    void waitForAllTabletsNormal(uint64_t tableId, uint64_t timeoutNs = ~0lu);

  PRIVATE:
    void flushImpl(const SpinLock::Guard& guard, uint64_t tableId);

    IndexletWithLocator* lookupIndexletInCache(const SpinLock::Guard& guard,
                                               uint64_t tableId,
                                               uint8_t indexId,
                                               const void* key,
                                               KeyLength keyLength);
    TabletWithLocator* lookupTabletInCache(const SpinLock::Guard& guard,
                                           const TabletKey* key);


    IndexletWithLocator* tryLookupIndexlet(uint64_t tableId, uint8_t indexId,
                                           const void* key,
                                           KeyLength keyLength,
                                           bool* indexDoesntExist);
    TabletWithLocator* tryLookupTablet(uint64_t tableId, KeyHash keyHash);

    /**
     * Shared RAMCloud information.
     */
    Context* const context;

    /**
     * Lock protecting tableMap and tableIndexMap.
     */
    mutable SpinLock mutex;

    /**
     * Update the local tablet map cache. Usually, calling
     * tableConfigFetcher.getTableConfig() is the same as calling
     * coordinator.getTableConfig(tableConfig). During unit tests, however,
     * this is swapped out with a mock implementation.
     */
    std::unique_ptr<ObjectFinder::TableConfigFetcher> tableConfigFetcher;

    typedef std::pair<uint64_t, uint8_t> TableIdIndexIdPair;

    /**
     * tableIndexMap provides a fast lookup for the current indexes being used.
     * It stores the indexlets, so they can be accessed quickly using a
     * index id and table id.
     */
    std::multimap<TableIdIndexIdPair, IndexletWithLocator> tableIndexMap;
    typedef std::multimap<TableIdIndexIdPair, IndexletWithLocator>::iterator
            IndexletIter;

    /**
     * The following variable provides a cache of configuration information
     * about tables that have been used by this client; it is loaded on-demand
     * from the coordinator on a table-by-table basis.
     */
    std::map<TabletKey, TabletWithLocator> tableMap;
    typedef std::map<TabletKey, TabletWithLocator>::iterator TabletIter;

    DISALLOW_COPY_AND_ASSIGN(ObjectFinder);
};

/**
 * The interface for ObjectFinder::tableConfigFetcher. This is usually set to
 * RealTableConfigFetcher, which is defined in ObjectFinder.cc. Implementations
 * of this interface are not necessarily thread-safe.
 */
class ObjectFinder::TableConfigFetcher {
  public:
    virtual ~TableConfigFetcher() {}

    virtual void clear() {};

    virtual bool tryGetTableConfig(
            uint64_t tableId,
            std::map<TabletKey, TabletWithLocator>* tableMap,
            std::multimap<std::pair<uint64_t, uint8_t>, IndexletWithLocator>*
                    tableIndexMap) = 0;
};

} // end RAMCloud

#endif  // RAMCLOUD_OBJECTFINDER_H
