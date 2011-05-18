/* Copyright (c) 2010 Stanford University
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
#include "Transport.h"
#include "MasterClient.h"

namespace RAMCloud {

/**
 * The client uses this class to get session handles to masters.
 */
class ObjectFinder {
  public:
    explicit ObjectFinder(CoordinatorClient& coordinator);

    /**
     * A partition (or bin) corresponding to the requests to be sent
     * to one master in a multiRead / multiWrite operation.
     */
    struct MasterRequests {
        MasterRequests() : sessionref(), requests() {}
        Transport::SessionRef sessionref;
        std::vector<MasterClient::ReadObject> requests;
    };

    Transport::SessionRef lookup(uint32_t table, uint64_t objectId);
    std::vector<MasterRequests> multiLookup(MasterClient::ReadObject input[],
                                            uint32_t numRequests);


    /**
     * Lookup the master that is in charge of assigning object IDs for create
     * requests for the given table.
     */
    Transport::SessionRef lookupHead(uint32_t table) {
        return lookup(table, ~0UL);
    }

    /**
     * Jettison all tablet map entries forcing a fetch of fresh mappings
     * on subsequent lookups.
     */
    void flush() {
        tabletMap.Clear();
    }

    void waitForAllTabletsNormal();

  private:
    /**
     * A cache of the coordinator's tablet map.
     */
    ProtoBuf::Tablets tabletMap;

    /**
     * Update the local tablet map cache. Usually, calling refresher(tabletMap)
     * is the same as calling coordinator.getTabletMap(tabletMap). During unit
     * tests, however, refresher is swapped out with a mock function.
     */
    boost::function<void(ProtoBuf::Tablets&)> refresher;

    friend class ObjectFinderTest;
    DISALLOW_COPY_AND_ASSIGN(ObjectFinder);
};

} // end RAMCloud

#endif  // RAMCLOUD_OBJECTFINDER_H
