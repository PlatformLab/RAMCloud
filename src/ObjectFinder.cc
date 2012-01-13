/* Copyright (c) 2010-2011 Stanford University
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

#include "ObjectFinder.h"
#include "ShortMacros.h"

namespace RAMCloud {

namespace {

/**
 * The implementation of ObjectFinder::TabletMapFetcher that is used for normal
 * execution. Simply forwards getTabletMap to the coordinator client.
 */
class RealTabletMapFetcher : public ObjectFinder::TabletMapFetcher {
  public:
    explicit RealTabletMapFetcher(CoordinatorClient& coordinator)
        : coordinator(coordinator)
    {
    }
    void getTabletMap(ProtoBuf::Tablets& tabletMap) {
        coordinator.getTabletMap(tabletMap);
    }
  private:
    CoordinatorClient& coordinator;
};

} // anonymous namespace

/**
 * Constructor.
 * \param coordinator
 *      This object keeps a reference to \a coordinator
 */
ObjectFinder::ObjectFinder(CoordinatorClient& coordinator)
    : tabletMap()
    , tabletMapFetcher(new RealTabletMapFetcher(coordinator))
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
                    return Context::get().transportManager->getSession(
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
            throw TableDoesntExistException(HERE);
        }
  refresh_and_retry:
        tabletMapFetcher->getTabletMap(tabletMap);
        haveRefreshed = true;
    }
}


/**
 * Lookup the masters for a multiple object IDs in multiple tables.
 * \param requests
 *      Array listing the objects to be read/written
 * \param numRequests
 *      Length of requests array
 * \return requestBins
 *      Bins requests according to the master they correspond to.
 */

std::vector<ObjectFinder::MasterRequests>
ObjectFinder::multiLookup(MasterClient::ReadObject* requests[],
                          uint32_t numRequests) {

    std::vector<ObjectFinder::MasterRequests> requestBins;
    for (uint32_t i = 0; i < numRequests; i++){
        try {
            Transport::SessionRef currentSessionRef =
                ObjectFinder::lookup(requests[i]->tableId, requests[i]->id);

            // if this master already exists in the requestBins, add request
            // to the requestBin corresponding to that master
            bool masterFound = false;
            for (uint32_t j = 0; j < requestBins.size(); j++){
                if (currentSessionRef == requestBins[j].sessionRef){
                    requestBins[j].requests.push_back(requests[i]);
                    masterFound = true;
                    break;
                }
            }
            // else create a new requestBin corresponding to this master
            if (!masterFound) {
                requestBins.push_back(ObjectFinder::MasterRequests());
                requestBins.back().sessionRef = currentSessionRef;
                requestBins.back().requests.push_back(requests[i]);
            }
        }
        catch (TableDoesntExistException &e) {
            requests[i]->status = STATUS_TABLE_DOESNT_EXIST;
        }
    }

    return requestBins;
}

/**
 * Flush the tablet map and refresh it until we detect that at least one tablet
 * has a state set to something other than normal.
 *
 * Used only by RecoveryMain.c to detect when the failure is detected by the
 * coordinator.
 */
void
ObjectFinder::waitForTabletDown()
{
    flush();

    for (;;) {
        foreach (const ProtoBuf::Tablets::Tablet& tablet, tabletMap.tablet()) {
            if (tablet.state() != ProtoBuf::Tablets_Tablet_State_NORMAL) {
                return;
            }
        }
        usleep(200);
        tabletMapFetcher->getTabletMap(tabletMap);
    }
}


/**
 * Flush the tablet map and refresh it until it is non-empty and all of
 * the tablets have normal status.
 *
 * Used only by RecoveryMain.c to detect when the recovery is complete.
 */
void
ObjectFinder::waitForAllTabletsNormal()
{
    flush();

    for (;;) {
        bool allNormal = true;
        foreach (const ProtoBuf::Tablets::Tablet& tablet, tabletMap.tablet()) {
            if (tablet.state() != ProtoBuf::Tablets_Tablet_State_NORMAL) {
                allNormal = false;
                break;
            }
        }
        if (allNormal && tabletMap.tablet_size() > 0)
            return;
        usleep(200);
        tabletMapFetcher->getTabletMap(tabletMap);
    }
}


} // namespace RAMCloud
