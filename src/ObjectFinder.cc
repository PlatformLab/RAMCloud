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

#include "Cycles.h"
#include "ObjectFinder.h"
#include "ShortMacros.h"
#include "Key.h"

namespace RAMCloud {

namespace {

/**
 * The implementation of ObjectFinder::TabletMapFetcher that is used for normal
 * execution. Simply forwards getTabletMap to the coordinator client.
 */
class RealTabletMapFetcher : public ObjectFinder::TabletMapFetcher {
  public:
    explicit RealTabletMapFetcher(Context* context)
        : context(context)
    {
    }
    void getTabletMap(ProtoBuf::Tablets& tabletMap) {
        CoordinatorClient::getTabletMap(context, &tabletMap);
    }
  private:
    Context* context;

    DISALLOW_COPY_AND_ASSIGN(RealTabletMapFetcher);
};

} // anonymous namespace

/**
 * Constructor.
 * \param context
 *      Overall information about this client.
 */
ObjectFinder::ObjectFinder(Context* context)
    : context(context)
    , tabletMap()
    , tabletMapFetcher(new RealTabletMapFetcher(context))
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
ObjectFinder::lookup(uint64_t table, const void* key, uint16_t keyLength) {
    KeyHash keyHash = Key::getHash(table, key, keyLength);

    return lookup(table, keyHash);
}

/**
 * Lookup the master for a key hash in a given table. Useful for
 * looking up a key hash range in the table when you do not have a
 * specific key.
 *
 * \param table
 *      The table containing the desired object (return value from a
 *      previous call to getTableId).
 * \param keyHash
 *      A hash value in the space of key hashes.
 *
 * \throw TableDoesntExistException
 *      The coordinator has no record of the table.
 */
Transport::SessionRef
ObjectFinder::lookup(uint64_t table, KeyHash keyHash)
{
    return context->transportManager->getSession(
                lookupTablet(table, keyHash).service_locator().c_str());
}

/**
 * Delete the session connecting to the master that owns a particular
 * object, if such a session exists. This method is typically invoked after
 * an unrecoverable error has occurred on the session, so that a new
 * session will be created the next time someone wants to communicate
 * with that master.
 *
 * \param tableId
 *      The table containing the desired object (return value from a
 *      previous call to getTableId).
 * \param keyHash
 *      Hash value corresponding to a particular object in table.
 */
void
ObjectFinder::flushSession(uint64_t tableId, KeyHash keyHash)
{
    try {
        const ProtoBuf::Tablets::Tablet& tablet =
                lookupTablet(tableId, keyHash);
        context-> transportManager->flushSession(
                tablet.service_locator().c_str());
    } catch (TableDoesntExistException& e) {
        // We don't even store this tablet anymore, so there's nothing
        // to worry about.
    }
}

/**
 * Lookup the master for a particular key in a given table.
 * Only used internally in lookup() and for some crazy testing/debugging
 * routines.
 *
 * \param table
 *      The table containing the desired object (return value from a
 *      previous call to getTableId).
 * \param keyHash
 *      A hash value in the space of key hashes.
 * \return
 *      Reference to a tablet with the details of the server that owns
 *      the specified key. This reference may be invalidated by any future
 *      calls to the ObjectFinder.
 *
 * \throw TableDoesntExistException
 *      The coordinator has no record of the table.
 */
const ProtoBuf::Tablets::Tablet&
ObjectFinder::lookupTablet(uint64_t table, KeyHash keyHash)
{
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
                    return tablet;
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
 * Used for testing to detect when the recovery is complete.
 */
void
ObjectFinder::waitForAllTabletsNormal(uint64_t timeoutNs)
{
    flush();

    uint64_t start = Cycles::rdtsc();
    while (Cycles::toNanoseconds(Cycles::rdtsc() - start) < timeoutNs) {
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
