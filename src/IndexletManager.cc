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

#include "Common.h"
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
 * Add and initialize an index partition (indexlet) on this index server.
 *
 * \param tableId
 *      Id of the data table for which this indexlet stores some
 *      index information.
 * \param indexId
 *      Id of the index key for which this indexlet stores some information.
 * \param indexletTableId
 *      Id of the table that will hold objects for this indexlet
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
 *      The highest BTree Id that has been used in the indexletTable.
 *      If highestUsedId equals zero, it means the Btree should be an empty
 *      new tree.
 * \return
 *      Returns true if successfully added, or if the indexlet already existed
 *      (and was not added right now); false if the indexlet cannot be
 *      added because it overlaps with one or more existing indexlets.
 */
bool
IndexletManager::addIndexlet(
        uint64_t tableId, uint8_t indexId, uint64_t indexletTableId,
        const void *firstKey, uint16_t firstKeyLength,
        const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength,
        uint64_t highestUsedId)
{
    Lock indexletMapLock(mutex);

    if (lookupIndexlet(tableId, indexId, firstKey, firstKeyLength,
                       indexletMapLock) != indexletMap.end()) {
        return false;
    }

    Btree *bt;
    if (highestUsedId == 0)
        bt = new Btree(indexletTableId, objectManager);
    else
        bt = new Btree(indexletTableId, objectManager, highestUsedId);

    indexletMap.insert(std::make_pair(std::make_pair(tableId, indexId),
                       Indexlet(firstKey, firstKeyLength, firstNotOwnedKey,
                                firstNotOwnedKeyLength, bt)));
    return true;
}

/**
 * Add and initialize an index partition (indexlet) on this index server.
 *
 * \param indexlet
 *      Protocol buffer contains necessary indexlet information to construct
 *      a new indexlet.
 * \param highestUsedId
 *      The highest BTree Id that has been used in the indexletTable, which
 *      is a BTree.
 * \return
 *      Returns true if successfully added, false if the indexlet cannot be
 *      added because it overlaps with one or more existing indexlets.
 */
bool
IndexletManager::addIndexlet(ProtoBuf::Indexlets::Indexlet indexlet,
        uint64_t highestUsedId)
{
    uint64_t tableId = indexlet.table_id();
    uint8_t indexId = (uint8_t) indexlet.index_id();
    uint64_t indexletTableId = indexlet.indexlet_table_id();

    void *firstKey, *firstNotOwnedKey;
    uint16_t firstKeyLength, firstNotOwnedKeyLength;

    if (indexlet.start_key().compare("") != 0) {
        firstKey = const_cast<char *>(indexlet.start_key().c_str());
        firstKeyLength = (uint16_t)indexlet.start_key().length();
    } else {
        firstKey = NULL;
        firstKeyLength = 0;
    }

    if (indexlet.end_key().compare("") != 0) {
        firstNotOwnedKey = const_cast<char *>(indexlet.end_key().c_str());
        firstNotOwnedKeyLength = (uint16_t)indexlet.end_key().length();
    } else {
        firstNotOwnedKey = NULL;
        firstNotOwnedKeyLength = 0;
    }
    return addIndexlet(tableId, indexId, indexletTableId,
                       firstKey, firstKeyLength,
                       firstNotOwnedKey, firstNotOwnedKeyLength,
                       highestUsedId);
}

/**
 * Delete entries for an index partition (indexlet) on this index server. We can
 * have multiple indexlets for a table and an index stored on the same server.
 *
 * \param tableId
 *      Id of the data table for which this indexlet stores some
 *      index information.
 * \param indexId
 *      Id of the index key for which this indexlet stores some information.
 * \param firstKey
 *      Key blob marking the start of the indexed key range for this indexlet.
 * \param firstKeyLength
 *      Length of firstKeyStr.
 * \param firstNotOwnedKey
 *      Blob of the smallest key in the given index that is after firstKey
 *      in the index order but not part of this indexlet.
 * \param firstNotOwnedKeyLength
 *      Length of firstNotOwnedKey.
 * \return
 *      True if indexlet was deleted. Failed if indexlet did not exist.
 */
bool
IndexletManager::deleteIndexlet(
        uint64_t tableId, uint8_t indexId,
        const void *firstKey, uint16_t firstKeyLength,
        const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength)
{
    Lock indexletMapLock(mutex);

    // TODO(ashgup): This function can be simplified by using getIndexlet.
    // However, you'll still need to take the lock here and pass it on --
    // this may call for another lower level version of getIndexlet that takes
    // in a lock. The higher level getIndexlet() and this function can use that.

    IndexletMap::iterator it = lookupIndexlet(tableId, indexId,
                                              firstKey, firstKeyLength,
                                              indexletMapLock);
    if (it == indexletMap.end()) {
        return false;
    }

    Indexlet* indexlet = &it->second;

    if (IndexKey::keyCompare(indexlet->firstKey, indexlet->firstKeyLength,
                   firstKey, firstKeyLength) != 0  ||
        IndexKey::keyCompare(indexlet->firstNotOwnedKey,
                   indexlet->firstNotOwnedKeyLength,
                   firstNotOwnedKey, firstNotOwnedKeyLength) != 0) {
        return false;
    }

    delete indexlet->bt;
    indexletMap.erase(it);

    return true;
}

/**
 * Delete entries for an index partition (indexlet) on this index server. We can
 * have multiple indexlets for a table and an index stored on the same server.
 *
 * \param indexlet
 *      Protocol buffer contains information of an indexlet that is to be
 *      deleted.
 * \return
 *      True if indexlet was deleted. Failed if indexlet did not exist.
 */
bool
IndexletManager::deleteIndexlet(ProtoBuf::Indexlets::Indexlet indexlet)
{
    uint64_t tableId = indexlet.table_id();
    uint8_t indexId = (uint8_t) indexlet.index_id();
    void *firstKey, *firstNotOwnedKey;
    uint16_t firstKeyLength, firstNotOwnedKeyLength;
    if (indexlet.start_key().compare("") != 0) {
        firstKey = const_cast<char *>(indexlet.start_key().c_str());
        firstKeyLength = (uint16_t)indexlet.start_key().length();
    } else {
        firstKey = NULL;
        firstKeyLength = 0;
    }

    if (indexlet.end_key().compare("") != 0) {
        firstNotOwnedKey = const_cast<char *>(indexlet.end_key().c_str());
        firstNotOwnedKeyLength =(uint16_t)indexlet.end_key().length();
    } else {
        firstNotOwnedKey = NULL;
        firstNotOwnedKeyLength = 0;
    }
    return deleteIndexlet(tableId, indexId, firstKey, firstKeyLength,
                          firstNotOwnedKey, firstNotOwnedKeyLength);
}

/**
 * Given a secondary key in an indexlet range, find the indexlet.  This is used
 * to determine whether the indexlet is owned by this IndexletManager.
 *
 * \param tableId
 *      Id of the data table for which this indexlet stores some
 *      index information.
 * \param indexId
 *      Id of the index key for which this indexlet stores some information.
 * \param key
 *      Key blob contained to be found in the target indexlet.
 * \param keyLength
 *      Length of key blob.
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
 * Given the exact specification of a indexlet's range , obtain the current data
 * associated with that indexlet, if it exists. Note that the data returned is a
 * snapshot. The IndexletManager's data may be modified at any time by other
 * threads.
 *
 * \param tableId
 *      Id of the data table for which this indexlet stores some
 *      index information.
 * \param indexId
 *      Id of the index key for which this indexlet stores some information.
 * \param firstKey
 *      Key blob marking the start of the indexed key range for this indexlet.
 * \param firstKeyLength
 *      Length of firstKeyStr.
 * \param firstNotOwnedKey
 *      Blob of the smallest key in the given index that is after firstKey
 *      in the index order but not part of this indexlet.
 * \param firstNotOwnedKeyLength
 *      Length of firstNotOwnedKey.
 * \return
 *      True if a indexlet was found, otherwise false.
 */
IndexletManager::Indexlet*
IndexletManager::getIndexlet(uint64_t tableId, uint8_t indexId,
        const void *firstKey, uint16_t firstKeyLength,
        const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength)
{
    Lock indexletMapLock(mutex);

    // TODO(ashgup): This function seems somewhat inefficient because
    // lookupIndexlet() does comparisons on first and firstNotOwned keys
    // and then we do it again here. Instead you can iterate over all the
    // indexlets and do the comparison directly here instead of calling into
    // lookupIndexlet().

    IndexletMap::iterator it = lookupIndexlet(tableId, indexId,
                                              firstKey, firstKeyLength,
                                              indexletMapLock);
    if (it == indexletMap.end())
        return NULL;

    Indexlet* indexlet = &it->second;

    if (IndexKey::keyCompare(indexlet->firstKey, indexlet->firstKeyLength,
                   firstKey, firstKeyLength) != 0  ||
        IndexKey::keyCompare(indexlet->firstNotOwnedKey,
                   indexlet->firstNotOwnedKeyLength,
                   firstNotOwnedKey, firstNotOwnedKeyLength) != 0) {
        return NULL;
    }

    return indexlet;
}

/**
 * Helper for the public methods that need to look up a indexlet. This method
 * iterates over all candidates in the multimap.
 *
 * \param tableId
 *      Id of the data table for which this indexlet stores some
 *      index information.
 * \param indexId
 *      Id of the index key for which this indexlet stores some information.
 * \param key
 *      Key blob marking the start of the indexed key range for this indexlet.
 * \param keyLength
 *      Length of firstKeyStr.
 * \param mutex
 *      Lock from parent function to protect the indexletMap
 *      from concurrent access.
 * \return
 *      A IndexletMap::iterator is returned. If no indexlet was found, it will
 *      be equal to indexletMap.end(). Otherwise, it will refer to the desired
 *      indexlet.
 *
 *      An iterator, rather than a Indexlet pointer is returned to facilitate
 *      efficient deletion.
 */
IndexletManager::IndexletMap::iterator
IndexletManager::lookupIndexlet(uint64_t tableId, uint8_t indexId,
        const void *key, uint16_t keyLength, Lock& mutex)
{
    auto range = indexletMap.equal_range(std::make_pair(tableId, indexId));
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
IndexletManager::getCount()
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
 *      Id of the table containing the object corresponding to this index entry.
 * \param indexId
 *      Id of the index to which this index key belongs.
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
 * Lookup objects with index keys corresponding to indexId in the
 * specified range.
 *
 * \param tableId
 *      Id of the table containing the objects corresponding to these
 *      index keys.
 * \param indexId
 *      Id of the index to which these index keys belongs.
 * \param firstKey
 *      Starting key blob for the key range in which keys are to be matched.
 *      The key range includes the firstKey.
 * \param firstKeyLength
 *      Length of firstKey.
 * \param firstAllowedKeyHash
 *      Smallest primary key hash value allowed for firstKey.
 * \param lastKey
 *      Ending key for the key range in which keys are to be matched.
 *      The key range includes the lastKey.
 * \param lastKeyLength
 *      Length of lastKey.
 * \param maxNumHashes
 *      Maximum number of key hashes that can be returned as a response here.
 *      If there are more hashes to be returned than maxNumHashes, then
 *      information about the next key + keyHash to be fetched is also returned.
 *
 * \param[out] responseBuffer
 *      Return buffer containing the following:
 *      1. Actual bytes of the next key to fetch (nextKey), if any.
 *      Indicates that results for index keys starting at nextKey + nextKeyHash
 *      couldn't be returned right now, possibly because we ran out of space
 *      in the response rpc.
 *      Client can send another request to get results starting at nexKey +
 *      nextKeyHash.
 *      This part occupies the first nextKeyLength bytes of responseBuffer.
 *      2. The key hashes of the primary keys of all the objects
 *      that match the lookup query and can be returned in this response.
 *      This part occupies numHashes * sizeof(KeyHash) bytes of responseBuffer.
 * \param[out] numHashes
 *      Return the number of objects that matched the lookup, for which
 *      the primary key hashes are being returned here.
 * \param[out] nextKeyLength
 *      Length of nextKey in bytes.
 * \param[out] nextKeyHash
 *      Results starting at nextKey + nextKeyHash couldn't be returned.
 *      Client can send another request according to this.
 * \return
 *      Returns STATUS_OK if the lookup succeeded. Other status values
 *      indicate different failures.
 */
Status
IndexletManager::lookupIndexKeys(uint64_t tableId, uint8_t indexId,
        const void* firstKey, KeyLength firstKeyLength,
        uint64_t firstAllowedKeyHash,
        const void* lastKey, uint16_t lastKeyLength,
        uint32_t maxNumHashes, Buffer* responseBuffer, uint32_t* numHashes,
        uint16_t* nextKeyLength, uint64_t* nextKeyHash)
{
    Lock indexletMapLock(mutex);

    RAMCLOUD_LOG(DEBUG, "Looking up: tableId %lu, indexId %u.\n"
                        "first key: %s\n last  key: %s\n",
                        tableId, indexId,
                        Util::hexDump(firstKey, firstKeyLength).c_str(),
                        Util::hexDump(lastKey, lastKeyLength).c_str());

    IndexletMap::iterator mapIter =
            lookupIndexlet(tableId, indexId, firstKey, firstKeyLength,
                           indexletMapLock);
    if (mapIter == indexletMap.end())
        return STATUS_UNKNOWN_INDEXLET;
    Indexlet* indexlet = &mapIter->second;

    *numHashes = 0;
    *nextKeyLength = 0;

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
    auto iter = indexlet->bt->lower_bound(
                    KeyAndHash {firstKey, firstKeyLength, firstAllowedKeyHash});

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
    while (iter != iterEnd &&
           IndexKey::keyCompare(lastKey, lastKeyLength,
                        iter.key().key, iter.key().keyLength) >= 0) {

        if (*numHashes < maxNumHashes) {
            // Can alternatively use iter.data() instead of iter.key().pKHash,
            // but we might want to make data NULL in the future, so might
            // as well use the pKHash from key right away.
            responseBuffer->emplaceAppend<uint64_t>(iter.key().pKHash);
            *numHashes += 1;
            ++iter;
        } else {
            rpcMaxedOut = true;
            break;
        }
    }

    if (rpcMaxedOut) {
        *nextKeyLength = uint16_t(iter.key().keyLength);
        *nextKeyHash = iter.data();
        responseBuffer->appendExternal(iter.key().key,
                        uint32_t(iter.key().keyLength));
    } else if (IndexKey::keyCompare(
                        lastKey, lastKeyLength, indexlet->firstNotOwnedKey,
                        indexlet->firstNotOwnedKeyLength) > 0) {

        *nextKeyLength = indexlet->firstNotOwnedKeyLength;
        *nextKeyHash = 0;
        responseBuffer->appendExternal(indexlet->firstNotOwnedKey,
                                       indexlet->firstNotOwnedKeyLength);
    } else {
        *nextKeyHash = 0;
        *nextKeyLength = 0;
    }

    return STATUS_OK;
}

/**
 * Remove index entry for an object for a given index id.
 *
 * \param tableId
 *      Id of the table containing the object corresponding to this index entry.
 * \param indexId
 *      Id of the index to which this index key belongs.
 * \param key
 *      Key blob for for the index entry.
 * \param keyLength
 *      Length of key.
 * \param pKHash
 *      Hash of the primary key of the object.
 * \return
 *      Returns STATUS_OK if the remove succeeded.
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
    // module
    if (indexlet->bt->erase_one(KeyAndHash {key, keyLength, pKHash})) {
        RAMCLOUD_LOG(DEBUG, "remove succeed: tableId %lu, indexId %u, key: %s",
                    tableId, indexId, Util::hexDump(key, keyLength).c_str());

        return STATUS_OK;
    }

    // code should not reach here ideally but if it does, we ignore it because
    // we allow for garbage in the indexlet
    RAMCLOUD_LOG(DEBUG, "remove failed: tableId %lu, indexId %u, key: %s",
                    tableId, indexId, Util::hexDump(key, keyLength).c_str());

    return STATUS_OK;
}

} //namespace
