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

namespace RAMCloud {

IndexletManager::IndexletManager(Context* context)
    : context(context)
    , indexletMap()
    , lock("IndexletManager::lock")
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
 * \param firstKey
 *      Key blob marking the start of the indexed key range for this indexlet.
 * \param firstKeyLength
 *      Length of firstKeyStr.
 * \param firstNotOwnedKey
 *      Key blob marking the first not owned key of the key space
 *      for this indexlet.
 * \param firstNotOwnedKeyLength
 *      Length of firstNotOwnedKey.
 * \return
 *      Returns true if successfully added, false if the indexlet cannot be
 *      added because it overlaps with one or more existing indexlets.
 */
bool
IndexletManager::addIndexlet(uint64_t tableId, uint8_t indexId,
                 const void *firstKey, uint16_t firstKeyLength,
                 const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength)
{
    Lock guard(lock);

    if (lookup(tableId, indexId, firstKey, firstKeyLength, guard)
            != indexletMap.end()) {
        return false;
    }

    //TODO(ashgup): allocate mem and copy keys from buffer
    indexletMap.emplace(std::make_pair(tableId, indexId), Indexlet(firstKey,
                    firstKeyLength, firstNotOwnedKey, firstNotOwnedKeyLength));
    return true;
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
 *      Key blob marking the first not owned key of the key space
 *      for this indexlet.
 * \param firstNotOwnedKeyLength
 *      Length of firstNotOwnedKey.
 * \return
 *      True if indexlet was deleted. Failed if indexlet did not exist.
 */
bool
IndexletManager::deleteIndexlet(uint64_t tableId, uint8_t indexId,
                const void *firstKey, uint16_t firstKeyLength,
                const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength)
{
    Lock guard(lock);

    IndexletMap::iterator it =
            lookup(tableId, indexId, firstKey, firstKeyLength, guard);
    if (it == indexletMap.end()){
        return false;
    }

    Indexlet* t = &it->second;
    //TODO(ashgup): convert every string operation to char* and strcmp
    string givenFirstKey = StringUtil::binaryToString(
                                firstKey, firstKeyLength);
    string tFirstKey = StringUtil::binaryToString(
                                t->firstKey, t->firstKeyLength);
    if (tFirstKey.compare(givenFirstKey) != 0){
        return false;
    }

    if (firstNotOwnedKey != NULL){
        string givenfirstNotOwnedKey = StringUtil::binaryToString(
                                    firstNotOwnedKey, firstNotOwnedKeyLength);
        string tfirstNotOwnedKey = StringUtil::binaryToString(
                                    t->firstNotOwnedKey,
                                    t->firstNotOwnedKeyLength);
        if (tfirstNotOwnedKey.compare(givenfirstNotOwnedKey) != 0)
            return false;
    } else {
        // found indexlet firstNotOwnedKey should be NULL
        if (t->firstNotOwnedKey != NULL)
            return false;
    }

    indexletMap.erase(it);
    //TODO(ashgup): free allocated memort and in destructor
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
 *      Key blob marking the first not owned key of the key space
 *      for this indexlet.
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
    Lock guard(lock);

    IndexletMap::iterator it =
        lookup(tableId, indexId, firstKey, firstKeyLength, guard);
    if (it == indexletMap.end())
        return NULL;

    Indexlet* t = &it->second;
    string tFirstKey = StringUtil::binaryToString(
                                t->firstKey, t->firstKeyLength);
    string givenFirstKey = StringUtil::binaryToString(
                                firstKey, firstKeyLength);
    if ( tFirstKey.compare(givenFirstKey) !=0 )
        return NULL;

    if (firstNotOwnedKey != NULL){
        string givenfirstNotOwnedKey = StringUtil::binaryToString(
                                    firstNotOwnedKey, firstNotOwnedKeyLength);
        string tfirstNotOwnedKey = StringUtil::binaryToString(
                                    t->firstNotOwnedKey,
                                    t->firstNotOwnedKeyLength);
        if (tfirstNotOwnedKey.compare(givenfirstNotOwnedKey) != 0)
            return NULL;
    } else {
        // found indexlet firstNotOwnedKey should be NULL
        if (t->firstNotOwnedKey != NULL)
            return NULL;
    }

    return t;
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
 * \param lock
 *      Lock from parent function to protect the indexletMap
 *      from concurrent access.
 * \return
 *      A IndexletMap::iterator is returned. If no indexlet was found, it will be
 *      equal to indexletMap.end(). Otherwise, it will refer to the desired
 *      indexlet.
 *
 *      An iterator, rather than a Indexlet pointer is returned to facilitate
 *      efficient deletion.
 */
IndexletManager::IndexletMap::iterator
IndexletManager::lookup(uint64_t tableId, uint8_t indexId,
                               const void *key, uint16_t keyLength, Lock& lock)
{
    auto range = indexletMap.equal_range(std::make_pair(tableId, indexId));
    IndexletMap::iterator end = range.second;
    string givenKey = StringUtil::binaryToString(key, keyLength);
    for (IndexletMap::iterator it = range.first; it != end; it++) {

        Indexlet* t = &it->second;
        string firstKey = StringUtil::binaryToString(
                                t->firstKey, t->firstKeyLength);
        if ( givenKey.compare(firstKey) < 0)
            continue;

        if (t->firstNotOwnedKey != NULL){
            string firstNotOwnedKey = StringUtil::binaryToString(
                                t->firstNotOwnedKey, t->firstNotOwnedKeyLength);
            if (givenKey.compare(firstNotOwnedKey) >= 0)
                continue;
        }
        return it;
    }
    return indexletMap.end();
}

 /**
  * Obtain the total number of indexlets this object is managing.
  * 
  * \return
  *     Total number of inxelets this object is managing.
  */
size_t
IndexletManager::getCount()
{
    Lock guard(lock);
    return indexletMap.size();
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
    // TODO(ankitak): Implement. Currently a stub.
    // look at BtreeTest to find the other variants. one example:
    // indexlet->bt.insert2("temp", 100);
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
IndexletManager::lookupIndexKeys(
            uint64_t tableId, uint8_t indexId,
            const void* firstKeyStr, KeyLength firstKeyLength,
            const void* lastKeyStr, KeyLength lastKeyLength,
            uint32_t* count, Buffer* outBuffer)
{
    // TODO(ankitak): Implement. Currently a stub.
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
    // TODO(ankitak): Implement. Currently a stub.

    // look at btree tests for other variants of erase
    // indexlet->bt.erase_one("temp");
    // printf("confirming erase...btree size:%d\n\n", indexlet->bt.size());

    // TODO(ankitak): Do careful GC if multiple objs have the same pKHash.

    return Status(0);
}

/**
 * Compare the object's key corresponding to index id specified in keyRange
 * with the first and last keys in keyRange to determine if the key falls
 * in the keyRange, including the end points.
 *
 * \param object
 *      Object for which the key is to be compared.
 * \param keyRange
 *      KeyRange specifying the parameters of comparison.
 *
 * \return
 *      Value of true if key corresponding to index id specified in keyRange,
 *      say k, is such that lexicographically it falls in the range
 *      specified by [first key, last key] in keyRange, including end points.
 */
bool
IndexletManager::isKeyInRange(Object* object, KeyRange* keyRange)
{
    uint16_t keyLength;
    const void* key = object->getKey(keyRange->indexId, &keyLength);

    int firstKeyCmp = bcmp(keyRange->firstKey, key,
                           std::min(keyRange->firstKeyLength, keyLength));
    int lastKeyCmp = bcmp(keyRange->lastKey, key,
                          std::min(keyRange->lastKeyLength, keyLength));

    if ((firstKeyCmp < 0 ||
            (firstKeyCmp == 0 && keyRange->firstKeyLength <= keyLength)) &&
         (lastKeyCmp > 0 ||
            (lastKeyCmp == 0 && keyRange->lastKeyLength >= keyLength))) {
        return true;
    } else {
        return false;
    }
}

/**
 * Compare the keys and return their comparison.
 *
 * \param key1
 *      Actual bytes of first key to compare.
 * \param keyLength1
 *      Length of key1.
 * \param key2
 *      Actual bytes of second key to compare.
 * \param keyLength2
 *      Length of key2.
 *
 * \return
 *      Value of 0 if the keys are equal,
 *      negative value if key1 is lexicographically < key2,
 *      positive value if key1 is lexicographically > key2.
 */
int
IndexletManager::keyCompare(const void* key1, uint16_t keyLength1,
                            const void* key2, uint16_t keyLength2)
{
    int keyCmp = bcmp(key1, key2, std::min(keyLength1, keyLength2));

    if (keyCmp != 0) {
        return keyCmp;
    } else {
        return keyLength1 - keyLength2;
    }
}

} //namespace
