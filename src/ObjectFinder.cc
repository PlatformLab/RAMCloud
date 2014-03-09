/* Copyright (c) 2010-2013 Stanford University
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
/**
 * The implementation of ObjectFinder::TableConfigFetcher that is used for
 * normal execution. Simply invokes CoordinatorClient::getTableConfig
 */
class RealTableConfigFetcher : public ObjectFinder::TableConfigFetcher {
  public:
    explicit RealTableConfigFetcher(Context* context)
        : context(context)
    {
    }
    void getTableConfig(
          uint64_t tableId,
          std::map<TabletKey, TabletProtoBuffer>* tableMap) {

        tableMap->clear();
        ProtoBuf::Tablets tableConfig;
        CoordinatorClient::getTableConfig(context, tableId, &tableConfig);

        foreach (const ProtoBuf::Tablets::Tablet& tablet,
                 tableConfig.tablet()) {

            uint64_t tableId = tablet.table_id();
            uint64_t startKeyHash = tablet.start_key_hash();
            uint64_t endKeyHash = tablet.end_key_hash();
            ServerId serverId(tablet.server_id());
            Tablet::Status state = Tablet::Status::RECOVERING;
            if (tablet.state() == ProtoBuf::Tablets_Tablet_State_NORMAL) {
                state = Tablet::NORMAL;
            }
            Log::Position ctime(tablet.ctime_log_head_id(),
                                            tablet.ctime_log_head_offset());
            Tablet rawTablet(tableId, startKeyHash, endKeyHash,
                                            serverId, state, ctime);
            string serviceLocator = tablet.service_locator();

            TabletKey key{tablet.table_id(),
                          tablet.start_key_hash()};
            TabletProtoBuffer tabletProtoBuffer(rawTablet, serviceLocator);

            std::map<TabletKey, TabletProtoBuffer>::iterator it
                                                        = tableMap->find(key);
            tableMap->insert(std::make_pair(key, tabletProtoBuffer));
        }
    }
  private:
    Context* context;
    DISALLOW_COPY_AND_ASSIGN(RealTableConfigFetcher);
};

/**
 * Constructor.
 * \param context
 *      Overall information about this client.
 */
ObjectFinder::ObjectFinder(Context* context)
    : context(context)
    , tableMap()
    , tableConfigFetcher(new RealTableConfigFetcher(context))
{
}

/**
 * The method flush(tableId) removes all the entries in the tableMap
 * that contains tableId.
 * \param tableId
 *      the id of the table to be flushed.
 */
// TODO(ashgup): This should also flush the index related info for this table.
// If you'd rather create a different function to do so, that's okay too,
// just update the code in IndexRpcWrapper::handleTransportError() accordingly.
void
ObjectFinder::flush(uint64_t tableId) {
    RAMCLOUD_TEST_LOG("flushing object map");
    TabletKey start {tableId, 0U};
    TabletKey end {tableId, std::numeric_limits<KeyHash>::max()};
    TabletIter lower = tableMap.lower_bound(start);
    TabletIter upper = tableMap.upper_bound(end);
    tableMap.erase(lower, upper);
}

/**
 * Return a string representation of all the table id's presented
 * at the tableMap at any given moment. Used mainly for debugging.
 *
 * \return 
 *      string
 */
string
ObjectFinder::debugString() const {
    std::map<TabletKey, TabletProtoBuffer>::const_iterator it;
    std::stringstream result;
    for (it = tableMap.begin(); it != tableMap.end(); it++) {
           if (it != tableMap.begin()) {
                result << ", ";
           }
           result << "{{tableId : " << it->first.tableId
                  << ", keyHash : " << it->first.keyHash
                  << "}, "
                  << "{start_key_hash : " << it->second.tablet.startKeyHash
                  << ", end_key_hash : " << it->second.tablet.endKeyHash
                  << ", state : " << it->second.tablet.status
                  << "}}";
    }
    return result.str();
}

/**
 * Find information about the tablet containing a key in a given table.
 *
 * \param tableId
 *      The table containing the desired object
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated like a string.
 * \param keyLength
 *      Size in bytes of the key.
 * \return 
 *      Session for communication with the server who holds the tablet
 *
 * \throw TableDoesntExistException
 *      The coordinator has no record of the table.
 */
Transport::SessionRef
ObjectFinder::lookup(uint64_t tableId, const void* key, uint16_t keyLength) {
    KeyHash keyHash = Key::getHash(tableId, key, keyLength);
    return lookup(tableId, keyHash);
}

/**
 * Lookup the master for a key hash in a given table. Useful for
 * looking up a key hash range in the table when you do not have a
 * specific key.
 *
 * \param tableId
 *      The table containing the desired object.
 * \param keyHash
 *      A hash value in the space of key hashes.
* \return 
 *      Session for communication with the server who holds the tablet
 *
 * \throw TableDoesntExistException
 *      The coordinator has no record of the table.
 */
Transport::SessionRef
ObjectFinder::lookup(uint64_t tableId, KeyHash keyHash)
{
    return context->transportManager->getSession(
                lookupTablet(tableId, keyHash)->serviceLocator.c_str());
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
        const TabletProtoBuffer* tabletProtoBuff = lookupTablet(tableId,
                                                                keyHash);
        context->transportManager->flushSession(
                                    tabletProtoBuff->serviceLocator.c_str());
    } catch (TableDoesntExistException& e) {
        // We don't even store this tablet anymore, so there's nothing
        // to worry about.
    }
}

/**
 * Lookup the master for a particular key in a given table.
 * Only used internally in lookup() and for testing/debugging routines.
 *
 * \param tableId
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
const TabletProtoBuffer*
ObjectFinder::lookupTablet(uint64_t tableId, KeyHash keyHash)
{
    bool haveRefreshed = false;
    TabletKey key{tableId, keyHash};
    TabletIter iter;
    while (true) {
        iter = tableMap.upper_bound(key);
        if (!tableMap.empty() && iter != tableMap.begin()) {
            const TabletProtoBuffer* tabletProtoBuffer = &((--iter)->second);
            if (tabletProtoBuffer->tablet.tableId == tableId &&
              tabletProtoBuffer->tablet.startKeyHash <= keyHash &&
              keyHash <= tabletProtoBuffer->tablet.endKeyHash) {

                if (tabletProtoBuffer->tablet.status != Tablet::NORMAL) {
                    if (haveRefreshed) usleep(10000);
                    haveRefreshed = false;
                } else {
                    return tabletProtoBuffer;
                }
            }
        }
        if (haveRefreshed) {
          throw TableDoesntExistException(HERE);
        }
        tableConfigFetcher->getTableConfig(tableId, &tableMap);
        haveRefreshed = true;
    }
}

/**
 * Lookup the master containing indexlet with the given key.
 * Useful for clients to get masters they need to communicate with to lookup
 * index keys.
 *
 * \param tableId
 *      The table containing the desired object.
 * \param indexId
 *      Id of the index for which keys are to be compared.
 * \param key
 *      Blob corresponding to the key.
 * \param keyLength
 *      Length of key.
 * \return
 *      Session for communication with the server that has the indexlet.
 */
Transport::SessionRef
ObjectFinder::lookup(uint64_t tableId, uint8_t indexId,
                     const void* key, uint16_t keyLength)
{
    // TODO(ashgup): Implement. Currently a stub.
    /* Notes for ashgup by ankitak:
     * 1. While implementing, use vector's reserve function to reduce space
     * allocation overheads.
     * 2. Say lookup doesn’t find any matching indexlet (and hence session).
     * Flush the cache (as in normal tablet lookup). If still not found,
     * then indelet doesn't exist. Now, don't throw exception.
     * Instead, return NULL session to the caller.
     */
    Transport::SessionRef session;
    return session;
}

// This will go away within the next few commits.
/**
 * Lookup the master for an index key in a given table.
 * Useful for a data master to lookup index master to communicate with
 * to modify an index entry corresponding to the data being modified.
 *
 * \param tableId
 *      The table containing the desired object.
 * \param indexId
 *      Id of the index for which they key is to be compared.
 * \param key
 *      Blob corresponding to the key.
 * \param keyLength
 *      Length of key.
 * 
 * \param[out] serverId
 *      Return the ServerId of the server holding the indexlet
 *      containing this index key.
 * \return
 *      Value of true indicates that this indexlet was found and the server id
 *      of the server owning it is being returned. False otherwise.
 */
bool
ObjectFinder::lookupServerId(uint64_t tableId, uint8_t indexId,
                             const void* key, uint16_t keyLength,
                             ServerId* serverId)
{
    return false;
}

/**
 * Flush the tablet map and refresh it until we detect that at least one tablet
 * has a state set to something other than normal.
 *
 * Used only by RecoveryMain.c to detect when the failure is detected by the
 * coordinator.
 */
void
ObjectFinder::waitForTabletDown(uint64_t tableId)
{
    flush(tableId);
    for (;;) {
        tableConfigFetcher->getTableConfig(tableId, &tableMap);
        TabletKey start {tableId, 0U};
        TabletKey end {tableId, std::numeric_limits<KeyHash>::max()};
        TabletIter lower = tableMap.lower_bound(start);
        TabletIter upper = tableMap.upper_bound(end);
        for (; lower != upper; ++lower) {
            const TabletProtoBuffer& tabletProtoBuffer = lower->second;
            if (tabletProtoBuffer.tablet.status != Tablet::NORMAL) {
                return;
            }
        }
        usleep(200);
    }
}

/**
 * Flush information for a given table and refresh it until it is non-empty
 * and all of its tablets have normal status.
 *
 * Used for testing to detect when the recovery is complete.
 */
void
ObjectFinder::waitForAllTabletsNormal(uint64_t tableId, uint64_t timeoutNs)
{
    uint64_t start = Cycles::rdtsc();
    flush(tableId);
    while (Cycles::toNanoseconds(Cycles::rdtsc() - start) < timeoutNs) {
        bool allNormal = true;
        tableConfigFetcher->getTableConfig(tableId, &tableMap);
        TabletKey start {tableId, 0U};
        TabletKey end {tableId, std::numeric_limits<KeyHash>::max()};
        TabletIter lower = tableMap.lower_bound(start);
        TabletIter upper = tableMap.upper_bound(end);
        for (; lower != upper; ++lower) {
            const TabletProtoBuffer& tabletProtoBuffer = lower->second;
            if (tabletProtoBuffer.tablet.status != Tablet::NORMAL) {
                allNormal = false;
                break;
            }
        }
        if (allNormal && tableMap.size() > 0)
            return;
        usleep(200);
    }
}

} // namespace RAMCloud
