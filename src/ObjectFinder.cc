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

#include "ObjectFinder.h"
#include "ShortMacros.h"
#include "KeyHash.h"

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
 * \param context
 *      Overall information about the RAMCloud server.
 * \param coordinator
 *      This object keeps a reference to \a coordinator
 */
ObjectFinder::ObjectFinder(Context& context, CoordinatorClient& coordinator)
    : context(context)
    , tabletMap()
    , tabletMapFetcher(new RealTabletMapFetcher(coordinator))
{
}

/**
 * Lookup the master for a particular key in a given table.
 *
 * \param table
 *      The table containing the desired object (return value from a
 *      previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated like a string.
 * \param keyLength
 *      Size in bytes of the key.
 *
 * \throw TableDoesntExistException
 *      The coordinator has no record of the table.
 */
Transport::SessionRef
ObjectFinder::lookup(uint64_t table, const char* key, uint16_t keyLength) {
    HashType keyHash = getKeyHash(key, keyLength);

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
                tablet.start_key_hash() <= keyHash &&
                keyHash <= tablet.end_key_hash()) {
                if (tablet.state() == ProtoBuf::Tablets_Tablet_State_NORMAL) {
                    // TODO(ongaro): add cache
                    return context.transportManager->getSession(
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
            try {
                tabletMapFetcher->getTabletMap(tabletMap);
                haveRefreshed = true;
            } catch (const TransportException& e) {
                LOG(WARNING, "Couldn't get tablet map from coordinator; "
                    "retrying: %s", e.what());
                usleep(10000);
            }
    }
}

/**
 * Lookup the masters for multiple keys across tables.
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
                ObjectFinder::lookup(requests[i]->tableId,
                                     requests[i]->key, requests[i]->keyLength);

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
        try {
            tabletMapFetcher->getTabletMap(tabletMap);
        } catch (const TransportException& e) {
            LOG(WARNING, "Couldn't get tablet map from coordinator; "
                "retrying: %s", e.what());
            usleep(10000);
        }
    }
}


/**
 * Flush the tablet map and refresh it until it is non-empty and all of
 * the tablets have normal status.
 *
 * Used for testing to detect when the recovery is complete.
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
        try {
            tabletMapFetcher->getTabletMap(tabletMap);
        } catch (const TransportException& e) {
            LOG(WARNING, "Couldn't get tablet map from coordinator; "
                "retrying: %s", e.what());
            usleep(10000);
        }
    }
}


} // namespace RAMCloud
