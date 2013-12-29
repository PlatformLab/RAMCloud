/* Copyright (c) 2010-2012 Stanford University
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

#include <boost/function.hpp>

#include "Common.h"
#include "CoordinatorClient.h"
#include "Key.h"
#include "Transport.h"
#include "MasterClient.h"

namespace RAMCloud {

/**
 * This class maps from an object identifier (table and key) to a session
 * that can be used to communicate with the master that stores the object.
 * It retrieves configuration information from the coordinator and caches it.
 */
class ObjectFinder {
  public:
    class TabletMapFetcher; // forward declaration, see full declaration below

    explicit ObjectFinder(Context* context);

    Transport::SessionRef lookup(uint64_t table, const void* key,
                                 uint16_t keyLength);
    Transport::SessionRef lookup(uint64_t table, KeyHash keyHash);
    const ProtoBuf::Tablets::Tablet& lookupTablet(uint64_t table,
                                                  KeyHash keyHash);
    void flushSession(uint64_t tableId, KeyHash keyHash);

    /**
     * Jettison all tablet map entries forcing a fetch of fresh mappings
     * on subsequent lookups.
     */
    void flush() {
        RAMCLOUD_TEST_LOG("flushing object map");
        tabletMap.Clear();
    }

    void waitForTabletDown();
    void waitForAllTabletsNormal(uint64_t timeoutNs = ~0lu);

  PRIVATE:
    /**
     * Shared RAMCloud information.
     */
    Context* context;

    /**
     * A cache of the coordinator's tablet map.
     */
    ProtoBuf::Tablets tabletMap;

    /**
     * Update the local tablet map cache. Usually, calling
     * tabletMapFetcher.getTabletMap() is the same as calling
     * coordinator.getTabletMap(tabletMap). During unit tests, however,
     * this is swapped out with a mock implementation.
     */
    std::unique_ptr<ObjectFinder::TabletMapFetcher> tabletMapFetcher;

    DISALLOW_COPY_AND_ASSIGN(ObjectFinder);
};

/**
 * The interface for ObjectFinder::tabletMapFetcher. This is usually set to
 * RealTabletMapFetcher, which is defined in ObjectFinder.cc.
 */
class ObjectFinder::TabletMapFetcher {
  public:
    virtual ~TabletMapFetcher() {}
    /// See CoordinatorClient::getTabletMap.
    virtual void getTabletMap(ProtoBuf::Tablets& tabletMap) = 0;
};

} // end RAMCloud

#endif  // RAMCLOUD_OBJECTFINDER_H
