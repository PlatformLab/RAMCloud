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

#include <boost/bind.hpp>
#include <boost/ref.hpp>

#include "ObjectFinder.h"

namespace RAMCloud {

/**
 * Constructor.
 * \param coordinator
 *      This object keeps a reference to \a coordinator
 */
ObjectFinder::ObjectFinder(CoordinatorClient& coordinator)
    : tabletMap()
      // set refresher to coordinator.getTabletMap
    , refresher(boost::bind(&CoordinatorClient::getTabletMap,
                            boost::ref(coordinator), _1))
{
}

/**
 * Lookup the master for a particular object ID in a given table.
 * \throw TableDoesntExistException
 *      The coordinator has no record of the table.
 */
Transport::SessionRef
ObjectFinder::lookup(uint32_t table, uint64_t objectId) {
    /*
     * The control flow in here is a bit tricky:
     * Since tabletMap is a cache of the coordinator's tablet map, we can only
     * throw TableDoesntExistException if the table doesn't exist after
     * refreshing that cache.
     * Moreover, if the tablet turns out to be in a state of recovery, we have
     * to spin until it is recovered.
     */
    bool haveRefreshed = false;
    while (true) {
        foreach (const ProtoBuf::Tablets::Tablet& tablet, tabletMap.tablet()) {
            if (tablet.table_id() == table &&
                tablet.start_object_id() <= objectId &&
                objectId <= tablet.end_object_id()) {
                if (tablet.state() == ProtoBuf::Tablets_Tablet_State_NORMAL) {
                    // TODO(ongaro): add cache
                    return transportManager.getSession(
                                        tablet.service_locator().c_str());
                } else {
                    // tablet is recovering or something, try again
                    if (haveRefreshed)
                        usleep(10000);
                    goto refresh_and_retry;
                }
            }
        }
        // tablet not found in local tablet map cache
        if (haveRefreshed) {
            LOG(ERROR, "table doesnt exist: %u %lu %d %s",
                table, objectId, haveRefreshed,
                tabletMap.DebugString().c_str());
            throw TableDoesntExistException(HERE);
        }
  refresh_and_retry:
        refresher(tabletMap);
        haveRefreshed = true;
    }
}


} // namespace RAMCloud
