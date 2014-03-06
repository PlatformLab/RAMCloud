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

#include "Common.h"
#include "IndexletManager.h"

namespace RAMCloud {

IndexletManager::IndexletManager(Context* context, ObjectManager* objectManager)
    : context(context)
    , objectManager(objectManager)
    , indexletMap()
    , lock("IndexletManager::lock")
{
}

IndexletManager::~IndexletManager()
{
}

///////////////////////////////////////////////////////////////////////////////
/////////////////////////// Meta-data related functions ///////////////////////
///////////////////////////////////////////////////////////////////////////////

bool
IndexletManager::addIndexlet(uint64_t tableId, uint8_t indexId,
                             uint16_t firstKeyLength, const void *firstKey,
                             uint16_t lastKeyLength, const void *lastKey)
{
    Lock guard(lock);

    // If an existing tablet overlaps this range at all, fail.
    if (lookup(tableId, indexId, firstKeyLength, firstKey, guard)
            != indexletMap.end() ||
        lookup(tableId, indexId, lastKeyLength, lastKey, guard)
            != indexletMap.end()) {
        return false;
    }

    indexletMap.emplace(std::make_pair(tableId, indexId),
                    Indexlet(firstKeyLength, firstKey, lastKeyLength, lastKey));
    return true;
}

bool
IndexletManager::deleteIndexlet(uint64_t tableId, uint8_t indexId,
                                uint16_t firstKeyLength, const void *firstKey,
                                    uint16_t lastKeyLength, const void *lastKey)
{
    Lock guard(lock);

    IndexletMap::iterator it =
            lookup(tableId, indexId, firstKeyLength, firstKey, guard);
    if (it == indexletMap.end())
        return false;

    Indexlet* t = &it->second;
    //TODO(ashgup): compare the two keys
    if (t->firstKeyLength != firstKeyLength || t->lastKeyLength!= lastKeyLength)
        return false;

    indexletMap.erase(it);
    return true;
}

bool
IndexletManager::getIndexlet(uint64_t tableId, uint8_t indexId,
                    uint16_t firstKeyLength, const void *firstKey,
                    uint16_t lastKeyLength, const void *lastKey,
                    Indexlet* outIndexlet)
{
    Lock guard(lock);

    IndexletMap::iterator it =
        lookup(tableId, indexId, firstKeyLength, firstKey, guard);
    if (it == indexletMap.end())
        return false;

    Indexlet* t = &it->second;
    //TODO(ashgup): compare the two keys
    if (t->firstKeyLength != firstKeyLength || t->lastKeyLength!= lastKeyLength)
        return false;

    if (outIndexlet != NULL)
        *outIndexlet = *t;
    return true;
}

IndexletManager::IndexletMap::iterator
IndexletManager::lookup(uint64_t tableId, uint8_t indexId,
                               uint16_t keyLength, const void *key, Lock& lock)
{
    auto range = indexletMap.equal_range(std::make_pair(tableId, indexId));
    IndexletMap::iterator end = range.second;
    for (IndexletMap::iterator it = range.first; it != end; it++) {
        Indexlet* t = &it->second;
        //TODO(ashgup): compare the key
        if (keyLength >= t->firstKeyLength && keyLength <= t->lastKeyLength)
            return it;
    }
    return indexletMap.end();
}

///////////////////////////////////////////////////////////////////////////////
/////////////////////////// Index data related functions //////////////////////
///////////////////////////////////////////////////////////////////////////////

/**
 * Initialize an index partition (indexlet) on this index server.
 * 
 * \param tableId
 *      Id of the data table for which this indexlet stores some
 *      index information.
 * \param indexId
 *      Id of the index key for which this indexlet stores some information.
 * \param firstKeyStr
 *      Key blob marking the start of the indexed key range for this indexlet.
 * \param firstKeyLength
 *      Length of firstKeyStr.
 * \param lastKeyStr
 *      Key blob marking the end of the indexed key range for this indexlet.
 * \param lastKeyLength
 *      Length of lastKeyStr.
 */
void
IndexletManager::initIndexlet(uint64_t tableId, uint8_t indexId,
                           const void* firstKeyStr, KeyLength firstKeyLength,
                           const void* lastKeyStr, KeyLength lastKeyLength)
{
    // Currently a stub. TODO(ashgup).
    // Note: Will be called by takeIndexletOwnership, to the initialize tree.
    // Might not be needed. TBD.
}

/**
 * Delete entries for an index partition (indexlet) on this index server.
 * 
 * \param tableId
 *      Id of the data table for which this indexlet stores some
 *      index information.
 * \param indexId
 *      Id of the index key for which this indexlet stores some information.
 */
void
IndexletManager::deleteIndexletEntries(uint64_t tableId, uint8_t indexId)
{
    // Currently a stub. TODO(ashgup).
    // Note: Will be called by dropIndexletOwnership, to delete all entries.
}

/**
 * Read object(s) matching the given parameters.
 * We'd typically expect only one object to match the parameters, but it is
 * possible that multiple will.
 *  
 * \param tableId
 *      Id of the table containing the object(s).
 * \param indexId
 *      Id of the index to which these index keys to be matched belong.
 * \param pKHash
 *      Key hash of the primary key of the object(s).
 * \param firstKeyStr
 *      Key blob marking the start of the acceptable range for indexed key.
 * \param firstKeyLength
 *      Length of firstKey.
 * \param lastKeyStr
 *      Key blob marking the end of the acceptable range for indexed key.
 * \param lastKeyLength
 *      Length of lastKey.
 * \param[out] numObjects
 *      Num of objects being returned.
 * \param[out] outBuffer
 *      Actual object(s) matching the parameters will be returned here.
 * \param[out] lengths
 *      Length(s) of the object(s) returned in outBuffer.
 * \param[out] versions
 *      Version(s) of the object(s) returned.
 * \return
 *      Value of false if this server does not own the tablet, or if the tablet
 *      is not in NORMAL state, true otherwise.
 */
bool
IndexletManager::indexedRead(uint64_t tableId, uint8_t indexId, uint64_t pKHash,
                          const void* firstKeyStr, KeyLength firstKeyLength,
                          const void* lastKeyStr, KeyLength lastKeyLength,
                          uint32_t* numObjects, Buffer* outBuffer,
                          vector<uint32_t>* lengths, vector<uint64_t>* versions)
{
    // TODO(ankitak): Do we want IndexletManager to do key comparison?
    // If not, then maybe MasterService should directly call into ObjectManager.
    bool isOkayToContinue = objectManager->readObjectsByHash(
            tableId, indexId, pKHash,
            firstKeyStr, firstKeyLength, lastKeyStr, lastKeyLength,
            numObjects, outBuffer, lengths, versions);

    return isOkayToContinue;
}

/**
 * Insert index entry for an object for a given index id.
 * 
 * \param tableId
 *      Id of the table containing the object corresponding to this index entry.
 * \param indexId
 *      Id of the index to which this index key belongs.
 * \param keyStr
 *      Key blob for for the index entry.
 * \param keyLength
 *      Length of keyStr
 * \param pKHash
 *      Hash of the primary key of the object.
 * \return
 *      Returns STATUS_OK if the insert succeeded. Other status values
 *      indicate different failures.
 */
Status
IndexletManager::insertEntry(uint64_t tableId, uint8_t indexId,
                          const void* keyStr, KeyLength keyLength,
                          uint64_t pKHash)
{
    // Currently a stub. Return STATUS_OK. TODO(ankitak)
    return Status(0);
}

/**
 * Lookup objects with index keys corresponding to indexId in the
 * specified range or point.
 * 
 * \param tableId
 *      Id of the table containing the objects corresponding to these
 *      index keys.
 * \param indexId
 *      Id of the index to which these index keys belongs.
 * \param firstKeyStr
 *      Starting key blob for the key range in which keys are to be matched.
 *      The key range includes the firstKey.
 * \param firstKeyLength
 *      Length of firstKeyStr.
 * \param lastKeyStr
 *      Ending key for the key range in which keys are to be matched.
 *      The key range includes the lastKey.
 * \param lastKeyLength
 *      Length of lastKeyStr.
 * \param[out] count
 *      Num of key hashes being returned.
 * \param[out] outBuffer
 *      Return the key hashes of the primary keys of all the objects
 *      that match the lookup query.
 * \return
 *      Returns STATUS_OK if the lookup succeeded. Other status values
 *      indicate different failures.
 */
Status
IndexletManager::lookupIndexKeys(uint64_t tableId, uint8_t indexId,
                              const void* firstKeyStr, KeyLength firstKeyLength,
                              const void* lastKeyStr, KeyLength lastKeyLength,
                              uint32_t* count, Buffer* outBuffer)
{
    // Currently a stub. Return STATUS_OK. TODO(ankitak)
    return Status(0);
}

/**
 * Remove index entry for an object for a given index id.
 * 
 * \param tableId
 *      Id of the table containing the object corresponding to this index entry.
 * \param indexId
 *      Id of the index to which this index key belongs.
 * \param keyStr
 *      Key blob for for the index entry.
 * \param keyLength
 *      Length of keyStr
 * \param pKHash
 *      Hash of the primary key of the object.
 * \return
 *      Returns STATUS_OK if the remove succeeded. Other status values
 *      indicate different failures.
 */
Status
IndexletManager::removeEntry(uint64_t tableId, uint8_t indexId,
                          const void* keyStr, KeyLength keyLength,
                          uint64_t pKHash)
{
    // Currently a stub. Return STATUS_OK. TODO(ankitak)
    return Status(0);
    // TODO(ankitak): Later: Careful GC if multiple objs have the same pKHash.
}

} //namespace
