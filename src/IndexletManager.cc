/* Copyright (c) 2014-2015 Stanford University
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
#include "IndexletManager.h"
#include "StringUtil.h"
#include "Util.h"
#include "TimeTrace.h"
#include "btreeRamCloud/Btree.h"

namespace RAMCloud {

IndexletManager::IndexletManager(Context* context, ObjectManager* objectManager)
    : context(context)
    , indexletMap()
    , mutex()
    , objectManager(objectManager)
{
}

///////////////////////////////////////////////////////////////////////////////
/////////////////////////// Meta-data related functions ///////////////////////
//////////////////////////////////// PUBLIC ///////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

/**
 * Add an indexlet (index partition) on this server.
 * If this indexlet already exists, change state to given state if different.
 *
 * \param tableId
 *      Id for a particular table.
 * \param indexId
 *      Id for a particular secondary index associated with tableId.
 * \param backingTableId
 *      Id of the backing table that will hold objects for this indexlet.
 * \param firstKey
 *      Key blob marking the start of the indexed key range for this indexlet.
 * \param firstKeyLength
 *      Number of bytes in firstKey.
 * \param firstNotOwnedKey
 *      Blob of the smallest key in the given index that is after firstKey
 *      in the index order but not part of this indexlet.
 * \param firstNotOwnedKeyLength
 *      Length of firstNotOwnedKey.
 * \param state
 *      State of the indexlet, whether it is normal or recovering.
 * \param nextNodeId
 *      The lowest node id that the next node allocated for this indexlet
 *      is allowed to have. This is used to ensure that we don't
 *      reuse existing node ids after crash recovery.
 * 
 * \return
 *      True if indexlet was added, false if it already existed.
 * 
 * \throw InternalError
 *      If indexlet cannot be added because the range overlaps with one or more
 *      existing indexlets.
 */
bool
IndexletManager::addIndexlet(
        uint64_t tableId, uint8_t indexId, uint64_t backingTableId,
        const void *firstKey, uint16_t firstKeyLength,
        const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength,
        IndexletManager::Indexlet::State state, uint64_t nextNodeId)
{
    Lock indexletMapLock(mutex);

    IndexletMap::iterator it = getIndexlet(tableId, indexId,
            firstKey, firstKeyLength, firstNotOwnedKey, firstNotOwnedKeyLength,
            indexletMapLock);

    if (it != indexletMap.end()) {
        // This indexlet already exists.
        LOG(NOTICE, "Adding indexlet in tableId %lu indexId %u, "
                "but already own.", tableId, indexId);
        Indexlet* indexlet = &it->second;

        // If its state is different from that provided, change the state.
        if (indexlet->state != state) {
            LOG(NOTICE, "Changing state of this indexlet from %d to %d.",
                    indexlet->state, state);
            indexlet->state = state;
        }

        LOG(NOTICE, "Returning success.");
        return false;

    } else {
        // Add a new indexlet.
        IndexBtree *bt;
        if (nextNodeId == 0)
            bt = new IndexBtree(backingTableId, objectManager);
        else
            bt = new IndexBtree(backingTableId, objectManager, nextNodeId);

        indexletMap.insert(std::make_pair(TableAndIndexId{tableId, indexId},
                Indexlet(firstKey, firstKeyLength, firstNotOwnedKey,
                        firstNotOwnedKeyLength, bt, state)));

        return true;
    }
}

/**
 * Transition the state field associated with a given indexlet from a specific
 * old state to a given new state. This is typically used when recovery has
 * completed and a indexlet is changed from the RECOVERING to NORMAL state.
 *
 * \param tableId
 *      Id for a particular table.
 * \param indexId
 *      Id for a particular secondary index associated with tableId.
 * \param firstKey
 *      Key blob marking the start of the indexed key range for this indexlet.
 * \param firstKeyLength
 *      Length of firstKeyStr.
 * \param firstNotOwnedKey
 *      Blob of the smallest key in the given index that is after firstKey
 *      in the index order but not part of this indexlet.
 * \param firstNotOwnedKeyLength
 *      Length of firstNotOwnedKey.
 * \param oldState
 *      The state the tablet is expected to be in prior to changing to the new
 *      value. This helps ensure that the proper transition is made, since
 *      another thread could modify the indexlet's state between getIndexlet()
 *      and changeState() calls.
 * \param newState
 *      The state to transition the indexlet to.
 * 
 * \return
 *      Returns true if the state was updated, otherwise false.
 * 
 * \throw InternalError
 *      If indexlet was not found because the range overlaps
 *      with one or more existing indexlets.
 */
bool
IndexletManager::changeState(uint64_t tableId, uint8_t indexId,
        const void *firstKey, uint16_t firstKeyLength,
        const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength,
        IndexletManager::Indexlet::State oldState,
        IndexletManager::Indexlet::State newState)
{
    Lock indexletMapLock(mutex);

    IndexletMap::iterator it = getIndexlet(tableId, indexId,
            firstKey, firstKeyLength, firstNotOwnedKey, firstNotOwnedKeyLength,
            indexletMapLock);
    if (it == indexletMap.end()) {
        return false;
    }

    Indexlet* indexlet = &it->second;
    if (indexlet->state != oldState) {
        return false;
    }

    indexlet->state = newState;
    return true;
}

/**
 * Delete an indexlet (stored on this server) and the entries stored in
 * that indexlet.
 *
 * \param tableId
 *      Id for a particular table.
 * \param indexId
 *      Id for a particular secondary index associated with tableId.
 * \param firstKey
 *      Key blob marking the start of the indexed key range for this indexlet.
 * \param firstKeyLength
 *      Length of firstKeyStr.
 * \param firstNotOwnedKey
 *      Blob of the smallest key in the given index that is after firstKey
 *      in the index order but not part of this indexlet.
 * \param firstNotOwnedKeyLength
 *      Length of firstNotOwnedKey.
 * 
 * \throw InternalError
 *      If indexlet was not found because the range overlaps
 *      with one or more existing indexlets.
 */
void
IndexletManager::deleteIndexlet(
        uint64_t tableId, uint8_t indexId,
        const void *firstKey, uint16_t firstKeyLength,
        const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength)
{
    Lock indexletMapLock(mutex);

    IndexletMap::iterator it = getIndexlet(tableId, indexId,
            firstKey, firstKeyLength, firstNotOwnedKey, firstNotOwnedKeyLength,
            indexletMapLock);

    if (it == indexletMap.end()) {
        RAMCLOUD_LOG(DEBUG, "Unknown indexlet in tableId %lu, indexId %u",
                tableId, indexId);
    } else {
        delete (&it->second)->bt;
        indexletMap.erase(it);
    }
}

/**
 * Given an index key, find the indexlet containing that entry.
 * This is a helper for the public methods that need to look up a indexlet.
 *
 * \param tableId
 *      Id for a particular table.
 * \param indexId
 *      Id for a particular secondary index associated with tableId.
 * \param key
 *      The secondary index key used to find a particular indexlet.
 * \param keyLength
 *      Length of key.
 * 
 * \return
 *      An Indexlet pointer to the indexlet for index indexId for table tableId
 *      that contains key, or NULL if no such indexlet could be found.
 */
IndexletManager::Indexlet*
IndexletManager::findIndexlet(uint64_t tableId, uint8_t indexId,
        const void *key, uint16_t keyLength)
{
    Lock indexletMapLock(mutex);

    IndexletMap::iterator it =
            findIndexlet(tableId, indexId, key, keyLength, indexletMapLock);
    if (it == indexletMap.end()) {
        return NULL;
    }
    return &it->second;
}

/**
 * Obtain the total number of indexlets this object is managing.
 *
 * \return
 *     Total number of indexlets this object is managing.
 */
size_t
IndexletManager::getNumIndexlets()
{
    Lock indexletMapLock(mutex);
    return indexletMap.size();
}

/**
 * Given a secondary key, check if an indexlet containing it exists.
 * This function is used to determine whether the indexlet is owned by this
 * instance of IndexletManager.
 *
 * \param tableId
 *      Id for a particular table.
 * \param indexId
 *      Id for a particular secondary index associated with tableId.
 * \param key
 *      The secondary index key used to find a particular indexlet.
 * \param keyLength
 *      Length of key blob.
 * 
 * \return
 *      True if a indexlet was found, otherwise false.
 */
bool
IndexletManager::hasIndexlet(uint64_t tableId, uint8_t indexId,
        const void *key, uint16_t keyLength)
{
    Lock indexletMapLock(mutex);

    IndexletMap::iterator it =
            findIndexlet(tableId, indexId, key, keyLength, indexletMapLock);
    if (it == indexletMap.end()) {
        return false;
    }
    return true;
}

/**
 * Given the value for the RAMCloud object encapsulating an indexlet tree node,
 * check if the node contains (or points to nodes containing) any entries whose
 * key is greater than or equal to compareKey.
 * 
 * \param nodeObjectValue
 *      Buffer holding the value of the RAMCloud object encapsulating
 *      this node. The caller must ensure the lifetime of this buffer.
 * \param compareKey
 *      The key to compare against.
 * \param compareKeyLength
 *      Length of compareKey.
 * 
 * \return
 *      True if nodeObjectValue contains (or points to nodes containing)
 *      any entries whose key is greater than or equal to compareKey;
 *      false otherwise.
 */
bool
IndexletManager::isGreaterOrEqual(Buffer* nodeObjectValue,
        const void* compareKey, uint16_t compareKeyLength)
{
    Lock indexletMapLock(mutex);

    return IndexBtree::isGreaterOrEqual(nodeObjectValue,
            BtreeEntry{compareKey, compareKeyLength, 0UL});
}

/**
 * For the indexlet containing truncateKey, modify metadata such that the
 * firstNotOwnedKey of that indexlet is truncateKey.
 *
 * \param tableId
 *      Id for a particular table.
 * \param indexId
 *      Id for a particular secondary index associated with tableId.
 * \param truncateKey
 *      The secondary index key used to find a particular indexlet and
 *      the key to which the firstNotOwnedKey of this indexlet will be set to.
 * \param truncateKeyLength
 *      Length of truncateKey blob.
 */
void
IndexletManager::truncateIndexlet(uint64_t tableId, uint8_t indexId,
        const void* truncateKey, uint16_t truncateKeyLength)
{
    Lock indexletMapLock(mutex);

    IndexletMap::iterator it = findIndexlet(tableId, indexId,
            truncateKey, truncateKeyLength, indexletMapLock);

    if (it == indexletMap.end()) {
        RAMCLOUD_LOG(ERROR, "Given indexlet in tableId %lu, indexId %u "
                "to be truncated doesn't exist anymore.", tableId, indexId);
        throw InternalError(HERE, STATUS_INTERNAL_ERROR);
    }

    Indexlet* indexlet = &it->second;
    indexlet->firstNotOwnedKeyLength = truncateKeyLength;
    free(indexlet->firstNotOwnedKey);
    indexlet->firstNotOwnedKey = malloc(truncateKeyLength);
    memcpy(indexlet->firstNotOwnedKey, truncateKey, truncateKeyLength);
}

/**
 * Given a secondary key, find the indexlet that contains it and set its
 * nextNodeId to the given nextNodeId if the given nextNodeId is higher than
 * the current nextNodeId. If there is no just indexlet, then the function
 * does nothing.
 *
 * \param tableId
 *      Id for a particular table.
 * \param indexId
 *      Id for a particular secondary index associated with tableId.
 * \param key
 *      The secondary index key used to find a particular indexlet.
 * \param keyLength
 *      Length of key blob.
 * \param nextNodeId
 *      The nextNodeId to set, if higher than current value.
 */
void
IndexletManager::setNextNodeIdIfHigher(uint64_t tableId, uint8_t indexId,
        const void *key, uint16_t keyLength, uint64_t nextNodeId)
{
    Lock indexletMapLock(mutex);

    IndexletMap::iterator it = findIndexlet(tableId, indexId,
            key, keyLength, indexletMapLock);

    if (it == indexletMap.end()) {
        return;
    }

    IndexletManager::Indexlet* indexlet = &it->second;
    if (indexlet->bt->getNextNodeId() < nextNodeId)
        (&it->second)->bt->setNextNodeId(nextNodeId);
}

///////////////////////////////////////////////////////////////////////////////
/////////////////////////// Meta-data related functions ///////////////////////
/////////////////////////////////// PRIVATE ///////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

/**
 * Given an index key, find the indexlet containing that entry.
 * This is a helper for the public methods that need to look up a indexlet.
 *
 * \param tableId
 *      Id for a particular table.
 * \param indexId
 *      Id for a particular secondary index associated with tableId.
 * \param key
 *      The secondary index key used to find a particular indexlet.
 * \param keyLength
 *      Length of key.
 * \param indexletMapLock
 *      This ensures that the caller holds this lock.
 * \return
 *      An iterator to the indexlet for index indexId for table tableId
 *      that contains key, or indexletMap.end() if no such indexlet could be
 *      found.
 *
 *      An iterator, rather than a Indexlet pointer is returned to facilitate
 *      efficient deletion.
 */
IndexletManager::IndexletMap::iterator
IndexletManager::findIndexlet(uint64_t tableId, uint8_t indexId,
        const void *key, uint16_t keyLength, Lock& indexletMapLock)
{
    // Must iterate over all of the local indexlets for the given index.
    auto range = indexletMap.equal_range(TableAndIndexId {tableId, indexId});
    IndexletMap::iterator start = range.first;
    IndexletMap::iterator end = range.second;

    // If key is NULL (and correspondingly, keyLength is 0), then return
    // the indexlet with lowest firstKey.
    if (keyLength == 0) {
        IndexletMap::iterator lowestIter = start;
        Indexlet* lowestIndexlet = &lowestIter->second;

        for (IndexletMap::iterator it = start; it != end; it++) {

            Indexlet* indexlet = &it->second;

            if (IndexKey::keyCompare(
                    indexlet->firstKey, indexlet->firstKeyLength,
                    lowestIndexlet->firstKey,
                    lowestIndexlet->firstKeyLength) > 0) {
                continue;
            } else {
                lowestIter = it;
                lowestIndexlet = indexlet;
            }
        }
        return lowestIter;
    }

    for (IndexletMap::iterator it = start; it != end; it++) {
        Indexlet* indexlet = &it->second;

        if (IndexKey::keyCompare(
                indexlet->firstKey, indexlet->firstKeyLength,
                key, keyLength) > 0) {
            continue;
        }
        if (indexlet->firstNotOwnedKey != NULL &&
            IndexKey::keyCompare(key, keyLength,
                    indexlet->firstNotOwnedKey,
                    indexlet->firstNotOwnedKeyLength) >= 0) {
            continue;
        }
        return it;
    }

    return indexletMap.end();
}

/**
 * Given the exact specification of a indexlet's range, obtain the current data
 * associated with that indexlet, if it exists. Note that the data returned is a
 * snapshot. The IndexletManager's data may be modified at any time by other
 * threads.
 *
 * \param tableId
 *      Id for a particular table.
 * \param indexId
 *      Id for a particular secondary index associated with tableId.
 * \param firstKey
 *      Key blob marking the start of the indexed key range for this indexlet.
 * \param firstKeyLength
 *      Length of firstKeyStr.
 * \param firstNotOwnedKey
 *      Blob of the smallest key in the given index that is after firstKey
 *      in the index order but not part of this indexlet.
 * \param firstNotOwnedKeyLength
 *      Length of firstNotOwnedKey.
 * \param indexletMapLock
 *      This ensures that the caller holds this lock.
 * 
 * \return
 *      An iterator to the indexlet if the specified indexlet was found,
 *      indexletMap.end() if no such indexlet could be found.
 * 
 * \throw InternalError
 *      If indexlet was not found because the range overlaps
 *      with one or more existing indexlets.
 */
IndexletManager::IndexletMap::iterator
IndexletManager::getIndexlet(uint64_t tableId, uint8_t indexId,
        const void *firstKey, uint16_t firstKeyLength,
        const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength,
        Lock& indexletMapLock)
{
    // Must iterate over all of the local indexlets for the given index.
    auto range = indexletMap.equal_range(TableAndIndexId {tableId, indexId});
    IndexletMap::iterator end = range.second;

    for (IndexletMap::iterator it = range.first; it != end; it++) {

        Indexlet* indexlet = &it->second;

        int firstKeyCompare = IndexKey::keyCompare(
                firstKey, firstKeyLength,
                indexlet->firstKey, indexlet->firstKeyLength);
        int firstNotOwnedKeyCompare = IndexKey::keyCompare(
                firstNotOwnedKey, firstNotOwnedKeyLength,
                indexlet->firstNotOwnedKey,
                indexlet->firstNotOwnedKeyLength);

        if (firstKeyCompare == 0 && firstNotOwnedKeyCompare == 0) {
            return it;
        } else if ((firstKeyCompare < 0 && firstNotOwnedKeyCompare < 0) ||
                   (firstKeyCompare > 0 && firstNotOwnedKeyCompare > 0)) {
            continue;
        } else {
            // Leaving the following in as it is useful for debugging.
//        string givenFirstKey((const char*)firstKey, firstKeyLength);
//        string givenFirstNotOwnedKey(
//                (const char*)firstNotOwnedKey, firstNotOwnedKeyLength);
//        string indexletFirstKey(
//                (const char*)indexlet->firstKey, indexlet->firstKeyLength);
//        string indexletFirstNotOwnedKey(
//                (const char*)indexlet->firstNotOwnedKey,
//                indexlet->firstNotOwnedKeyLength);
//
//        RAMCLOUD_LOG(ERROR, "Given indexlet in tableId %lu, indexId %u "
//                "with firstKey '%s' and firstNotOwnedKey '%s' "
//                "overlaps with indexlet with "
//                "firstKey '%s' and firstNotOwnedKey '%s'.",
//                tableId, indexId,
//                givenFirstKey.c_str(), givenFirstNotOwnedKey.c_str(),
//                indexletFirstKey.c_str(), indexletFirstNotOwnedKey.c_str());
            RAMCLOUD_LOG(ERROR, "Given indexlet in tableId %lu, indexId %u "
                    "overlaps with one or more other ranges.",
                    tableId, indexId);
            throw InternalError(HERE, STATUS_INTERNAL_ERROR);
        }
    }

    return indexletMap.end();
}

///////////////////////////////////////////////////////////////////////////////
////////////////////////// Index data related functions ///////////////////////
/////////////////////////////////// PUBLIC ////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

/**
 * Insert index entry for an object for a given index id.
 *
 * \param tableId
 *      Id for a particular table.
 * \param indexId
 *      Id for a particular secondary index associated with tableId.
 * \param key
 *      Key blob for for the index entry.
 * \param keyLength
 *      Length of key.
 * \param pKHash
 *      Hash of the primary key of the object.
 * \return
 *      Returns STATUS_OK if the insert succeeded.
 *      Returns STATUS_UNKNOWN_INDEXLET if the server does not own an indexlet
 *      that could contain this index entry.
 */
Status
IndexletManager::insertEntry(uint64_t tableId, uint8_t indexId,
        const void* key, KeyLength keyLength, uint64_t pKHash)
{
    Lock indexletMapLock(mutex);
    RAMCLOUD_LOG(DEBUG, "Inserting: tableId %lu, indexId %u, hash %lu,\n"
                        "key: %s", tableId, indexId, pKHash,
                        Util::hexDump(key, keyLength).c_str());

    IndexletMap::iterator it =
            findIndexlet(tableId, indexId, key, keyLength, indexletMapLock);
    if (it == indexletMap.end()) {
        RAMCLOUD_LOG(DEBUG, "Unknown indexlet: tableId %lu, indexId %u, "
                            "hash %lu,\nkey: %s", tableId, indexId, pKHash,
                            Util::hexDump(key, keyLength).c_str());
        return STATUS_UNKNOWN_INDEXLET;
    }
    Indexlet* indexlet = &it->second;

    Lock indexletLock(indexlet->indexletMutex);
    indexletMapLock.unlock();

    BtreeEntry entry = BtreeEntry(key, keyLength, pKHash);
    indexlet->bt->insert(entry);

    return STATUS_OK;
}

/**
 * Handle LOOKUP_INDEX_KEYS request.
 * 
 * \copydetails Service::ping
 */
void
IndexletManager::lookupIndexKeys(
        const WireFormat::LookupIndexKeys::Request* reqHdr,
        WireFormat::LookupIndexKeys::Response* respHdr,
        Service::Rpc* rpc)
{
    Lock indexletMapLock(mutex);

    uint32_t reqOffset = sizeof32(*reqHdr);
    uint16_t firstKeyLength = reqHdr->firstKeyLength;
    uint16_t lastKeyLength = reqHdr->lastKeyLength;
    const void* firstKey =
            rpc->requestPayload->getRange(reqOffset, firstKeyLength);
    reqOffset += firstKeyLength;
    const void* lastKey =
            rpc->requestPayload->getRange(reqOffset, lastKeyLength);

    if ((firstKey == NULL && firstKeyLength > 0) ||
            (lastKey == NULL && lastKeyLength > 0)) {
        respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
        rpc->sendReply();
        return;
    }

    RAMCLOUD_LOG(DEBUG, "Looking up: tableId %lu, indexId %u.\n"
                        "first key: %s\n last  key: %s\n",
                        reqHdr->tableId, reqHdr->indexId,
                        Util::hexDump(firstKey, firstKeyLength).c_str(),
                        Util::hexDump(lastKey, lastKeyLength).c_str());

    IndexletMap::iterator mapIter =
            findIndexlet(reqHdr->tableId, reqHdr->indexId, firstKey,
                    firstKeyLength, indexletMapLock);
    if (mapIter == indexletMap.end()) {
        respHdr->common.status = STATUS_UNKNOWN_INDEXLET;
    }
    Indexlet* indexlet = &mapIter->second;

    Lock indexletLock(indexlet->indexletMutex);
    indexletMapLock.unlock();

    // We want to use lower_bound() instead of find() because the firstKey
    // may not correspond to a key in the indexlet.
    auto iter = indexlet->bt->lower_bound(BtreeEntry {
            firstKey, firstKeyLength, reqHdr->firstAllowedKeyHash});
    auto iterEnd = indexlet->bt->end();
    bool rpcMaxedOut = false;

    respHdr->numHashes = 0;

    while (iter != iterEnd) {
        BtreeEntry currEntry = *iter;
        // If we have overshot the range to be returned (indicated by lastKey),
        // then break. Otherwise continue appending entries to response rpc.
        if (IndexKey::keyCompare(currEntry.key, currEntry.keyLength,
                lastKey, lastKeyLength) > 0)
        {
            break;
        }

        if (respHdr->numHashes < reqHdr->maxNumHashes) {
            // Can alternatively use iter.data() instead of iter.key().pKHash,
            // but we might want to make data NULL in the future, so might
            // as well use the pKHash from key right away.
            rpc->replyPayload->emplaceAppend<uint64_t>(currEntry.pKHash);
            respHdr->numHashes += 1;
            ++iter;
        } else {
            rpcMaxedOut = true;
            break;
        }
    }

    if (rpcMaxedOut) {

        respHdr->nextKeyLength = uint16_t(iter->keyLength);
        respHdr->nextKeyHash = iter->pKHash;
        rpc->replyPayload->append(iter->key, uint32_t(iter->keyLength));

    } else if (IndexKey::keyCompare(
            lastKey, lastKeyLength,
            indexlet->firstNotOwnedKey, indexlet->firstNotOwnedKeyLength) > 0) {

        respHdr->nextKeyLength = indexlet->firstNotOwnedKeyLength;
        respHdr->nextKeyHash = 0;
        rpc->replyPayload->append(indexlet->firstNotOwnedKey,
                indexlet->firstNotOwnedKeyLength);

    } else {

        respHdr->nextKeyHash = 0;
        respHdr->nextKeyLength = 0;

    }

    respHdr->common.status = STATUS_OK;
}

/**
 * Remove index entry for an object for a given index id.
 *
 * \param tableId
 *      Id for a particular table.
 * \param indexId
 *      Id for a particular secondary index associated with tableId.
 * \param key
 *      Key blob for for the index entry.
 * \param keyLength
 *      Length of key.
 * \param pKHash
 *      Hash of the primary key of the object.
 * 
 * \return
 *      Returns STATUS_OK if the remove succeeded or if the entry did not
 *      exist.
 *      Returns STATUS_UNKNOWN_INDEXLET if the server does not own an indexlet
 *      containing this index entry.
 */
Status
IndexletManager::removeEntry(uint64_t tableId, uint8_t indexId,
        const void* key, KeyLength keyLength, uint64_t pKHash)
{
    Lock indexletMapLock(mutex);

    RAMCLOUD_LOG(DEBUG, "Removing: tableId %lu, indexId %u, hash %lu,\n"
                        "key: %s", tableId, indexId, pKHash,
                        Util::hexDump(key, keyLength).c_str());

    IndexletMap::iterator it =
            findIndexlet(tableId, indexId, key, keyLength, indexletMapLock);
    if (it == indexletMap.end())
        return STATUS_UNKNOWN_INDEXLET;

    Indexlet* indexlet = &it->second;

    Lock indexletLock(indexlet->indexletMutex);
    indexletMapLock.unlock();

    // Note that we don't have to explicitly compare the key hash in value
    // since it is also a part of the key that gets compared in the tree
    // module.
    indexlet->bt->erase(BtreeEntry {key, keyLength, pKHash});

    return STATUS_OK;
}

///////////////////////////////////////////////////////////////////////////////
////////////////////////// Index data related functions ///////////////////////
/////////////////////////////////// PRIVATE ///////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

/**
 * Check whether the given index entry exists in the given index.
 * This function is currently used only for testing.
 * 
 * \param tableId
 *      Id for a particular table.
 * \param indexId
 *      Id for a particular secondary index associated with tableId.
 * \param key
 *      Key blob for for the index entry.
 * \param keyLength
 *      Length of key.
 * \param pKHash
 *      Hash of the primary key of the object.
 * \return
 *      Boolean value true if the entry exists; false if not.
 */
bool
IndexletManager::existsIndexEntry(
        uint64_t tableId, uint8_t indexId,
        const void* key, KeyLength keyLength, uint64_t pKHash)
{
    Lock indexletMapLock(mutex);

    IndexletMap::iterator mapIter =
            findIndexlet(tableId, indexId, key, keyLength, indexletMapLock);
    if (mapIter == indexletMap.end()) {
        return false;
    }
    Indexlet* indexlet = &mapIter->second;

    Lock indexletLock(indexlet->indexletMutex);
    indexletMapLock.unlock();

    return indexlet->bt->exists(BtreeEntry {key, keyLength, pKHash});
}

} //namespace
