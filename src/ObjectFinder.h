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

#include "Common.h"
#include "CoordinatorClient.h"
#include "Transport.h"

namespace RAMCloud {

/**
 * The client uses this class to get session handles to masters.
 * \warning
 *      This implementation is a placeholder. For now, the only session
 *      returned is one referring to the first master to have registered with
 *      the coordinator.
 */
class ObjectFinder {
  public:
    explicit ObjectFinder(CoordinatorClient& coordinator)
        : coordinator(coordinator)
        , master() {
        ProtoBuf::Tablets tabletMap;
        while (true) {
            coordinator.getTabletMap(tabletMap);
            foreach (const ProtoBuf::Tablets::Tablet& tablet,
                     tabletMap.tablet()) {
                if (tablet.table_id() == 0) {
                    LOG(NOTICE, "Using master %s",
                        tablet.service_locator().c_str());
                    master = transportManager.getSession(
                                         tablet.service_locator().c_str());
                    return;
                }
            }
        }
    }
    Transport::SessionRef lookup(uint32_t table, uint64_t objectId) {
        return master;
    }
    Transport::SessionRef lookupHead(uint32_t table) {
        return master;
    }
  private:
    CoordinatorClient& coordinator;
    Transport::SessionRef master;
    DISALLOW_COPY_AND_ASSIGN(ObjectFinder);
};

} // end RAMCloud

#endif  // RAMCLOUD_OBJECTFINDER_H
