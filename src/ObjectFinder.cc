/* Copyright (c) 2010-2014 Stanford University
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
#include "IndexKey.h"
#include "ObjectFinder.h"
#include "ShortMacros.h"
#include "Key.h"
#include "FailSession.h"

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
          std::map<TabletKey, TabletWithLocator>* tableMap,
          std::multimap< std::pair<uint64_t, uint8_t>,
                                   ObjectFinder::Indexlet>* tableIndexMap) {

        ProtoBuf::TableConfig tableConfig;
        CoordinatorClient::getTableConfig(context, tableId, &tableConfig);

        foreach (const ProtoBuf::TableConfig::Tablet& tablet,
                 tableConfig.tablet()) {

            uint64_t tableId = tablet.table_id();
            uint64_t startKeyHash = tablet.start_key_hash();
            uint64_t endKeyHash = tablet.end_key_hash();
            ServerId serverId(tablet.server_id());
            Tablet::Status state = Tablet::Status::RECOVERING;
            if (tablet.state() == ProtoBuf::TableConfig_Tablet_State_NORMAL) {
                state = Tablet::NORMAL;
            }
            Log::Position ctime(tablet.ctime_log_head_id(),
                                            tablet.ctime_log_head_offset());

            Tablet rawTablet(tableId, startKeyHash, endKeyHash,
                                            serverId, state, ctime);
            string serviceLocator = tablet.service_locator();

            TabletKey key{tablet.table_id(),
                          tablet.start_key_hash()};
            TabletWithLocator tabletWithLocator(rawTablet, serviceLocator);

            std::map<TabletKey, TabletWithLocator>::iterator it
                                                        = tableMap->find(key);
            tableMap->insert(std::make_pair(key, tabletWithLocator));
        }

        foreach (const ProtoBuf::TableConfig::Index& index,
                                                    tableConfig.index()) {

            uint64_t indexId = index.index_id();
            foreach (const ProtoBuf::TableConfig::Index::Indexlet& indexlet,
                                                            index.indexlet()) {
                void* firstKey;
                uint16_t firstKeyLength;
                void* firstNotOwnedKey;
                uint16_t firstNotOwnedKeyLength;

                if (indexlet.start_key().compare("") != 0) {
                    firstKey = const_cast<char *>(indexlet.start_key().c_str());
                    firstKeyLength = (uint16_t)indexlet.start_key().length();
                } else {
                    firstKey = NULL;
                    firstKeyLength = 0;
                }

                if (indexlet.end_key().compare("") != 0) {
                    firstNotOwnedKey = const_cast<char *>
                                                (indexlet.end_key().c_str());
                    firstNotOwnedKeyLength =
                                    (uint16_t)indexlet.end_key().length();
                } else {
                    firstNotOwnedKey = NULL;
                    firstNotOwnedKeyLength = 0;
                }

                ServerId serverId(indexlet.server_id());
                string serviceLocator = indexlet.service_locator();
                ObjectFinder::Indexlet rawIndexlet(firstKey, firstKeyLength,
                                    firstNotOwnedKey, firstNotOwnedKeyLength,
                                    serverId, serviceLocator);

                tableIndexMap->insert(std::make_pair(
                        std::make_pair(tableId, indexId), rawIndexlet));
            }
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
    , tableIndexMap()
    , tableConfigFetcher(new RealTableConfigFetcher(context))
{
}

/**
 * This method deletes all cached information, restoring the object
 * to its original pristine state. It's used primarily to force cached
 * Session objects to be released during RAMCloud shutdown to avoid
 * order-of-destruction problems where a transport could be deleted before
 * all of its sessions.
 */
void ObjectFinder::reset()
{
    tableMap.clear();
    tableIndexMap.clear();
}

/**
 * This method is invoked when the caller has reason to believe that
 * the configuration information for particular table is out-of-date.
 * The method deletes all information related to that table; fresh
 * information will be fetched from the coordinator the next time it
 * is needed.
 * \param tableId
 *      The id of the table to be flushed.
 */
void
ObjectFinder::flush(uint64_t tableId) {
    TabletKey start {tableId, 0U};
    TabletKey end {tableId, std::numeric_limits<KeyHash>::max()};
    TabletIter lower = tableMap.lower_bound(start);
    TabletIter upper = tableMap.upper_bound(end);
    tableMap.erase(lower, upper);

    IndexletIter indexLower = tableIndexMap.lower_bound
                                    (std::make_pair(tableId, 0));
    IndexletIter indexUpper = tableIndexMap.upper_bound(
                std::make_pair(tableId, std::numeric_limits<uint8_t>::max()));
    tableIndexMap.erase(indexLower, indexUpper);
}

/**
 * Return a string representation of all the table id's presented
 * at the tableMap at any given moment. Used mainly for testing.
 *
 * \return 
 *      A human-readable string describing the contents of tableMap.
 */
string
ObjectFinder::debugString() const {
    std::map<TabletKey, TabletWithLocator>::const_iterator it;
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
    TabletWithLocator* tablet = lookupTablet(tableId, keyHash);
    if (tablet->session == NULL) {
        tablet->session = context->transportManager->getSession(
                tablet->serviceLocator);
    }
    return tablet->session;
}

/**
 * Lookup the master holding the indexlet containing the given key.
 *
 * \param tableId
 *      The table containing the desired object.
 * \param indexId
 *      Id of a particular index in tableId.
 * \param key
 *      Blob corresponding to the key.
 * \param keyLength
 *      Length of key.
 * 
 * \return
 *      Session for communication with the server who holds the indexlet.
 *      If the indexlet doesn't exist, return a NULL session.
 */
Transport::SessionRef
ObjectFinder::lookup(uint64_t tableId, uint8_t indexId,
                     const void* key, uint16_t keyLength)
{
    Indexlet* indexlet = lookupIndexlet(tableId, indexId, key, keyLength);
    // If indexlet doesn't exist, don't throw an exception.
    if (indexlet == NULL) {
        return Transport::SessionRef();
    }
    if (indexlet->session == NULL) {
        indexlet->session = context->transportManager->getSession(
                indexlet->serviceLocator);
    }
    return indexlet->session;
}

/**
 * Lookup the indexlet containing the given key.
 *
 * \param tableId
 *      The table containing the desired object.
 * \param indexId
 *      Id of a particular index in tableId.
 * \param key
 *      Blob corresponding to the key.
 * \param keyLength
 *      Length of key.
 * 
 * \return
 *      Reference to Indexlet with the details of the server that owns
 *      the specified key. This reference may be invalidated by any future
 *      calls to the ObjectFinder.
 */
ObjectFinder::Indexlet*
ObjectFinder::lookupIndexlet(uint64_t tableId, uint8_t indexId,
                             const void* key, uint16_t keyLength)
{
    std::pair<uint64_t, uint8_t> indexKey = std::make_pair(tableId, indexId);
    IndexletIter iter;

    for (int count = 0; count < 2; count++) {

        std::pair <IndexletIter, IndexletIter> range;
        range = tableIndexMap.equal_range(indexKey);

        for (iter = range.first; iter != range.second; iter++) {

            Indexlet* indexlet = &iter->second;
            if (indexlet->firstKey != NULL &&
                    IndexKey::keyCompare(key, keyLength,
                            indexlet->firstKey, indexlet->firstKeyLength) < 0) {
                continue;
            }
            if (indexlet->firstNotOwnedKey != NULL &&
                    IndexKey::keyCompare(key, keyLength,
                            indexlet->firstNotOwnedKey,
                            indexlet->firstNotOwnedKeyLength) >= 0) {
                continue;
            }
            return indexlet;
        }

        if (count == 0) {
            flush(tableId);
            tableConfigFetcher->getTableConfig(tableId, &tableMap,
                    &tableIndexMap);
        }
    }
    return NULL;
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
TabletWithLocator*
ObjectFinder::lookupTablet(uint64_t tableId, KeyHash keyHash)
{
    bool haveRefreshed = false;
    TabletKey key{tableId, keyHash};
    TabletIter iter;
    while (true) {
        iter = tableMap.upper_bound(key);
        if (!tableMap.empty() && iter != tableMap.begin()) {
            TabletWithLocator* tabletWithLocator = &((--iter)->second);
            if (tabletWithLocator->tablet.tableId == tableId &&
              tabletWithLocator->tablet.startKeyHash <= keyHash &&
              keyHash <= tabletWithLocator->tablet.endKeyHash) {

                if (tabletWithLocator->tablet.status != Tablet::NORMAL) {
                    if (haveRefreshed) usleep(10000);
                    haveRefreshed = false;
                } else {
                    return tabletWithLocator;
                }
            }
        }
        if (haveRefreshed) {
          throw TableDoesntExistException(HERE);
        }
        flush(tableId);
        tableConfigFetcher->getTableConfig(tableId, &tableMap, &tableIndexMap);
        haveRefreshed = true;
    }
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
        TabletWithLocator* tabletWithLocator = lookupTablet(tableId,
                                                                keyHash);
        context->transportManager->flushSession(
                                    tabletWithLocator->serviceLocator);
        tabletWithLocator->session = NULL;
    } catch (TableDoesntExistException& e) {
        // We don't even store this tablet anymore, so there's nothing
        // to worry about.
    }
}

/**
 * Delete the session connecting to the master that owns a particular
 * index entry, if such a session exists. This method is typically invoked after
 * an unrecoverable error has occurred on the session, so that a new
 * session will be created the next time someone wants to communicate
 * with that master.
 *
 * \param tableId
 *      The table containing the desired object.
 * \param indexId
 *      Id of a particular index in tableId.
 * \param key
 *      Blob corresponding to the key.
 * \param keyLength
 *      Length of key.
 */
void
ObjectFinder::flushSession(uint64_t tableId, uint8_t indexId,
                           const void* key, uint16_t keyLength)
{
    Indexlet* indexlet = lookupIndexlet(tableId, indexId,
                                              key, keyLength);
    if (indexlet != NULL) {
        context->transportManager->flushSession(
                        indexlet->serviceLocator);
        indexlet->session = NULL;
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
ObjectFinder::waitForTabletDown(uint64_t tableId)
{
    RAMCLOUD_TEST_LOG("flushing object map");
    flush(tableId);
    for (;;) {
        tableConfigFetcher->getTableConfig(tableId, &tableMap, &tableIndexMap);
        TabletKey start {tableId, 0U};
        TabletKey end {tableId, std::numeric_limits<KeyHash>::max()};
        TabletIter lower = tableMap.lower_bound(start);
        TabletIter upper = tableMap.upper_bound(end);
        for (; lower != upper; ++lower) {
            const TabletWithLocator& tabletWithLocator = lower->second;
            if (tabletWithLocator.tablet.status != Tablet::NORMAL) {
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
    RAMCLOUD_TEST_LOG("flushing object map");
    while (Cycles::toNanoseconds(Cycles::rdtsc() - start) < timeoutNs) {
        bool allNormal = true;
        flush(tableId);
        tableConfigFetcher->getTableConfig(tableId, &tableMap, &tableIndexMap);
        TabletKey start {tableId, 0U};
        TabletKey end {tableId, std::numeric_limits<KeyHash>::max()};
        TabletIter lower = tableMap.lower_bound(start);
        TabletIter upper = tableMap.upper_bound(end);
        for (; lower != upper; ++lower) {
            const TabletWithLocator& tabletWithLocator = lower->second;
            if (tabletWithLocator.tablet.status != Tablet::NORMAL) {
                allNormal = false;
                break;
            }
        }
        if (allNormal && tableMap.size() > 0)
            return;
        usleep(10000);
    }
}

} // namespace RAMCloud
