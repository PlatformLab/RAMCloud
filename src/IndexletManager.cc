/* Copyright (c) 2014 Stanford University
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

#include "IndexletManager.h"
#include "StringUtil.h"
#include "Util.h"

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
///////////////////////////////////////////////////////////////////////////////

/**
 * Add an indexlet (index partition) on this server.
 *
 * \param tableId
 *      Id for a particular table.
 * \param indexId
 *      Id for a particular secondary index associated with tableId.
 * \param indexletTableId
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
 * \param highestUsedId
 *      The highest node id that has been used for any node in the BTree
 *      corresponding to this indexlet. This is used to ensure that we don't
 *      reuse existing node ids after crash recovery.
 *      Should be 0 for a new indexlet.
 * 
 * \throw InternalError
 *      If indexlet cannot be added because the range overlaps with one or more
 *      existing indexlets.
 */
void
IndexletManager::addIndexlet(
        uint64_t tableId, uint8_t indexId, uint64_t indexletTableId,
        const void *firstKey, uint16_t firstKeyLength,
        const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength,
        uint64_t highestUsedId)
{
    Lock indexletMapLock(mutex);

    IndexletMap::iterator it = getIndexlet(tableId, indexId,
            firstKey, firstKeyLength, firstNotOwnedKey, firstNotOwnedKeyLength,
            indexletMapLock);

    if (it != indexletMap.end()) {
        // This indexlet already exists.
        LOG(NOTICE, "Adding indexlet in tableId %lu indexId %u, "
                "but already own. Returning success.", tableId, indexId);

    } else {
        // Add a new indexlet.
        Btree *bt;
        if (highestUsedId == 0)
            bt = new Btree(indexletTableId, objectManager);
        else
            bt = new Btree(indexletTableId, objectManager, highestUsedId);

        indexletMap.insert(std::make_pair(TableAndIndexId{tableId, indexId},
                Indexlet(firstKey, firstKeyLength, firstNotOwnedKey,
                        firstNotOwnedKeyLength, bt)));
    }
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
 * Given a secondary key find the indexlet that contains it.
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
            lookupIndexlet(tableId, indexId, key, keyLength, indexletMapLock);
    if (it == indexletMap.end()) {
        return false;
    }
    return true;
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
    // TODO(ashgup): This function seems somewhat inefficient because
    // lookupIndexlet() does comparisons on first and firstNotOwned keys
    // and then we do it again here. Instead you can iterate over all the
    // indexlets and do the comparison directly here instead of calling into
    // lookupIndexlet().

    IndexletMap::iterator it = lookupIndexlet(tableId, indexId,
            firstKey, firstKeyLength, indexletMapLock);
    if (it == indexletMap.end())
        return it;

    Indexlet* indexlet = &it->second;

    if (IndexKey::keyCompare(indexlet->firstKey, indexlet->firstKeyLength,
                firstKey, firstKeyLength) != 0  ||
        IndexKey::keyCompare(indexlet->firstNotOwnedKey,
                indexlet->firstNotOwnedKeyLength,
                firstNotOwnedKey, firstNotOwnedKeyLength) != 0) {

        RAMCLOUD_LOG(ERROR, "Given indexlet in tableId %lu, indexId %u "
                "overlaps with one or more other ranges.", tableId, indexId);
        throw InternalError(HERE, STATUS_INTERNAL_ERROR);
    }

    return it;
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
IndexletManager::lookupIndexlet(uint64_t tableId, uint8_t indexId,
        const void *key, uint16_t keyLength, Lock& indexletMapLock)
{
    // Must iterate over all of the local indexlets for the given index.
    auto range = indexletMap.equal_range(TableAndIndexId {tableId, indexId});
    IndexletMap::iterator end = range.second;

    for (IndexletMap::iterator it = range.first; it != end; it++) {

        Indexlet* indexlet = &it->second;
        if (IndexKey::keyCompare(key, keyLength,
                indexlet->firstKey, indexlet->firstKeyLength) < 0) {
            continue;
        }

        if (indexlet->firstNotOwnedKey != NULL) {
            if (IndexKey::keyCompare(key, keyLength,
                    indexlet->firstNotOwnedKey,
                    indexlet->firstNotOwnedKeyLength) >= 0) {
                continue;
            }
        }
        return it;
    }
    return indexletMap.end();
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

///////////////////////////////////////////////////////////////////////////////
////////////////////////// Index data related functions ///////////////////////
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
 *      Returns STATUS_OK if the insert succeeded. Other status values
 *      indicate different failures.
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
            lookupIndexlet(tableId, indexId, key, keyLength, indexletMapLock);
    if (it == indexletMap.end()) {
        RAMCLOUD_LOG(DEBUG, "Unknown indexlet: tableId %lu, indexId %u, "
                            "hash %lu,\nkey: %s", tableId, indexId, pKHash,
                            Util::hexDump(key, keyLength).c_str());
        return STATUS_UNKNOWN_INDEXLET;
    }
    Indexlet* indexlet = &it->second;

    Lock indexletLock(indexlet->indexletMutex);
    indexletMapLock.unlock();

    KeyAndHash keyAndHash = {key, keyLength, pKHash};
    indexlet->bt->insert(keyAndHash, pKHash);

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
    const void* firstKey =
            rpc->requestPayload->getRange(reqOffset, reqHdr->firstKeyLength);
    reqOffset += reqHdr->firstKeyLength;
    const void* lastKey =
            rpc->requestPayload->getRange(reqOffset, reqHdr->lastKeyLength);

    RAMCLOUD_LOG(DEBUG, "Looking up: tableId %lu, indexId %u.\n"
                        "first key: %s\n last  key: %s\n",
                        reqHdr->tableId, reqHdr->indexId,
                        Util::hexDump(firstKey, reqHdr->firstKeyLength).c_str(),
                        Util::hexDump(lastKey, reqHdr->lastKeyLength).c_str());

    IndexletMap::iterator mapIter =
            lookupIndexlet(reqHdr->tableId, reqHdr->indexId, firstKey,
                           reqHdr->firstKeyLength, indexletMapLock);
    if (mapIter == indexletMap.end()) {
        respHdr->common.status = STATUS_UNKNOWN_INDEXLET;
    }
    Indexlet* indexlet = &mapIter->second;

    Lock indexletLock(indexlet->indexletMutex);
    indexletMapLock.unlock();

    // If there are no values in this indexlet's tree, return right away.
    // TODO(zhihao): consider remove the following check since recovered BTree
    // cannot pass the following check by only setting nextNodeId in BTree.
    if (indexlet->bt->empty()) {
        //return STATUS_OK;
    }

    // We want to use lower_bound() instead of find() because the firstKey
    // may not correspond to a key in the indexlet.
    auto iter = indexlet->bt->lower_bound(KeyAndHash {
            firstKey, reqHdr->firstKeyLength, reqHdr->firstAllowedKeyHash});

    auto iterEnd = indexlet->bt->end();
    bool rpcMaxedOut = false;

    // If the iterator is currently at the end, then it stays at the
    // same point if we try to advance (i.e., increment) the iterator.
    // This can result this loop looping forever if:
    // end of the tree is <= lastKey given by client.
    // So the second condition ensures that we break if iterator is at the end.

    // If iter == iterEnd, calling iter.key() is wrong because we will
    // then try to read an object that definitely does not exist.
    // This will cause a crash in the tree module

    respHdr->numHashes = 0;

    while (iter != iterEnd &&
           IndexKey::keyCompare(lastKey, reqHdr->lastKeyLength,
                        iter.key().key, iter.key().keyLength) >= 0) {

        if (respHdr->numHashes < reqHdr->maxNumHashes) {
            // Can alternatively use iter.data() instead of iter.key().pKHash,
            // but we might want to make data NULL in the future, so might
            // as well use the pKHash from key right away.
            rpc->replyPayload->emplaceAppend<uint64_t>(iter.key().pKHash);
            respHdr->numHashes += 1;
            ++iter;
        } else {
            rpcMaxedOut = true;
            break;
        }

    }

    if (rpcMaxedOut) {

        respHdr->nextKeyLength = uint16_t(iter.key().keyLength);
        respHdr->nextKeyHash = iter.data();
        rpc->replyPayload->appendExternal(iter.key().key,
                uint32_t(iter.key().keyLength));

    } else if (IndexKey::keyCompare(
            lastKey, reqHdr->lastKeyLength,
            indexlet->firstNotOwnedKey, indexlet->firstNotOwnedKeyLength) > 0) {

        respHdr->nextKeyLength = indexlet->firstNotOwnedKeyLength;
        respHdr->nextKeyHash = 0;
        rpc->replyPayload->appendExternal(indexlet->firstNotOwnedKey,
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
            lookupIndexlet(tableId, indexId, key, keyLength, indexletMapLock);
    if (it == indexletMap.end())
        return STATUS_UNKNOWN_INDEXLET;

    Indexlet* indexlet = &it->second;

    Lock indexletLock(indexlet->indexletMutex);
    indexletMapLock.unlock();

    // Note that we don't have to explicitly compare the key hash in value
    // since it is also a part of the key that gets compared in the tree
    // module.
    indexlet->bt->erase_one(KeyAndHash {key, keyLength, pKHash});

    return STATUS_OK;
}

} //namespace
