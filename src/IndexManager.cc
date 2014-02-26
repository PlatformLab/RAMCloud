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

#include "IndexManager.h"

namespace RAMCloud {

IndexManager::IndexManager(Context* context)
        : context(context)
{
}

IndexManager::~IndexManager()
{
}

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
 * \return
 *      Returns STATUS_OK if the initialization succeeded.
 *      Other status values indicate different failures.
 */
Status
IndexManager::initIndexlet(uint64_t tableId, uint8_t indexId,
                           const void* firstKeyStr, KeyLength firstKeyLength,
                           const void* lastKeyStr, KeyLength lastKeyLength)
{
    // Currently a stub. Return STATUS_OK. TODO(ashgup).
    // Note: Will be called by takeIndexletOwnership, to the initialize tree.
    // Might not be needed. TBD.
    return Status(0);
}

/**
 * Drop an index partition (indexlet) on this index server.
 * 
 * \param tableId
 *      Id of the data table for which this indexlet stores some
 *      index information.
 * \param indexId
 *      Id of the index key for which this indexlet stores some information.
 * \return
 *      Returns STATUS_OK if the index drop succeeded. Other status values
 *      indicate different failures.
 */
Status
IndexManager::dropIndexlet(uint64_t tableId, uint8_t indexId)
{
    // Currently a stub. Return STATUS_OK. TODO(ashgup).
    // Note: Will be called by dropIndexletOwnership, to delete all entries.
    return Status(0);
}

/**
 * Read an object matching the given parameters.
 *  
 * \param tableId
 *      Id of the table containing the object.
 * \param indexId
 *      Id of the index to which these index keys to be matched belong.
 * \param pKHash
 *      Key hash of the primary key of the object.
 * \param firstKeyStr
 *      Key blob marking the start of the acceptable range for indexed key.
 * \param firstKeyLength
 *      Length of firstKey.
 * \param lastKeyStr
 *      Key blob marking the end of the acceptable range for indexed key.
 * \param lastKeyLength
 *      Length of lastKey.
 * \param[out] outBuffer
 *      Actual object matching the parameters will be returned here.
 * \param[out] outVersion
 *      Version of the object will be returned here.
 * \return
 *      Returns STATUS_OK if the read succeeded. Other status values indicate
 *      different failures.
 */
/* We'd normally expect the parameters to match only one object,
 * but it is possible they match multiple objects. In that case multiple objects
 * have to returned.
 * Currently we're assuming that only one object will match / be returned.
 * TODO(ankitak): LATER: Implement multi return for this. Will be reflected in
 * ObjectManager code as well.
 */
Status
IndexManager::indexedRead(uint64_t tableId, uint8_t indexId, uint64_t pKHash,
                          const void* firstKeyStr, KeyLength firstKeyLength,
                          const void* lastKeyStr, KeyLength lastKeyLength,
                          Buffer* outBuffer, uint64_t* outVersion)
{
    // Currently a stub. Return STATUS_OK. TODO(ankitak)
    return Status(0);
    // Calls:
    // objectManager.readObject(tableId, pKHash, outBuffer, ourVersion)
    // Then compare key from returned value with index keys to check bounds.
    // Return value if okay.
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
IndexManager::insertEntry(uint64_t tableId, uint8_t indexId,
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
 *      If it is a point lookup instead of range search, the keys will
 *      only be matched on the firstKey.
 * \param firstKeyLength
 *      Length of firstKeyStr.
 * \param lastKeyStr
 *      Ending key for the key range in which keys are to be matched.
 *      The key range includes the lastKey.
 *      If NULL, then it is a point lookup instead of range search.
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
IndexManager::lookupIndexKeys(uint64_t tableId, uint8_t indexId,
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
IndexManager::removeEntry(uint64_t tableId, uint8_t indexId,
                          const void* keyStr, KeyLength keyLength,
                          uint64_t pKHash)
{
    // Currently a stub. Return STATUS_OK. TODO(ankitak)
    return Status(0);
    // TODO(ankitak): Later: Careful GC if multiple objs have the same pKHash.
}

} // end namespace
