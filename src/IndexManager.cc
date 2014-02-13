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
 * Read an object matching the given parameters.
 *  
 * \param tableId
 *      Table id of the table containing the object.
 * \param pKHash
 *      Key hash of the primary key of the object.
 * \param indexId
 *      Index Id for the index on which key comparison is to be done.
 * \param firstKey
 *      Starting value for the acceptable range for indexed key.
 * \param lastKey
 *      Last value for the acceptable range for indexed key.
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
IndexManager::indexedRead(uint64_t tableId, uint64_t pKHash,
                          uint8_t indexId, Key& firstKey, Key& lastKey,
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
 * Lookup objects with index keys corresponding to indexId in the
 * specified range or point.
 * 
 * \param tableId
 *      Id of the table in which lookup is to be done.
 * \param indexId
 *      Id of the index for which keys have to be compared.
 * \param firstKey
 *      Starting key for the key range in which keys are to be matched.
 *      The key range includes the firstKey.
 *      If it is a point lookup instead of range search, the keys will
 *      only be matched on the firstKey.
 * \param lastKey
 *      Ending key for the key range in which keys are to be matched.
 *      The key range includes the lastKey.
 *      If NULL, then it is a point lookup instead of range search.
 * \param[out] count
 *      Num of key hashed being returned.
 * \param[out] outBuffer
 *      Return the key hashes of the primary keys of all the objects
 *      that match the lookup query.
 * \return
 *      Returns STATUS_OK if the lookup succeeded. Other status values
 *      indicate different failures.
 */
Status
IndexManager::lookupIndexKeys(uint64_t tableId, uint8_t indexId,
                              Key& firstKey, Key& lastKey,
                              uint32_t* count, Buffer* outBuffer)
{
    // Currently a stub. Return STATUS_OK. TODO(ankitak)
    return Status(0);
}

} // end namespace
