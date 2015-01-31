/* Copyright (c) 2015 Stanford University
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

#include "Transaction.h"

namespace RAMCloud {

/**
 * Constructor for a transaction.
 *
 * \param ramcloud
 *      Overall information about the calling client.
 */
Transaction::Transaction(RamCloud* ramcloud)
    : ramcloud(ramcloud)
    , participantCount(0)
    , participantList()
    , commitCache()
{
}

/**
 * Commits the transaction defined by the operations performed on this
 * transaction (read, remove, write).
 *
 * \return
 *      True if the transaction was able to commit.  False otherwise.
 */
bool
Transaction::commit()
{
    return false;
}

/**
 * Read the current contents of an object as part of this transaction.
 *
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.
 * \param keyLength
 *      Size in bytes of the key.
 * \param[out] value
 *      After a successful return, this Buffer will hold the
 *      contents of the desired object - only the value portion of the object.
 */
void
Transaction::read(uint64_t tableId, const void* key, uint16_t keyLength,
        Buffer* value)
{
    Key keyObj(tableId, key, keyLength);
    CacheEntry* entry = findCacheEntry(keyObj);

    if (entry == NULL) {
        ObjectBuffer buf;
        uint64_t version;
        ramcloud->readKeysAndValue(
                tableId, key, keyLength, &buf, NULL, &version);

        uint32_t dataLength;
        const void* data = buf.getValue(&dataLength);

        entry = insertCacheEntry(tableId, buf.getKey(), buf.getKeyLength(),
                                 data, dataLength);
        entry->type = CacheEntry::READ;
        entry->rejectRules.givenVersion = version;
        entry->rejectRules.versionNeGiven = true;
    } else if (entry->type == CacheEntry::REMOVE) {
        throw ObjectDoesntExistException(HERE);
    }

    uint32_t dataLength;
    const void* data = entry->objectBuf->getValue(&dataLength);
    value->reset();
    value->appendCopy(data, dataLength);
}

/**
 * Delete an object from a table as part of this transaction. If the object does
 * not currently exist then the operation succeeds without doing anything.
 *
 * \param tableId
 *      The table containing the object to be deleted (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.
 * \param keyLength
 *      Size in bytes of the key.
 */
void
Transaction::remove(uint64_t tableId, const void* key, uint16_t keyLength)
{
    Key keyObj(tableId, key, keyLength);
    CacheEntry* entry = findCacheEntry(keyObj);

    if (entry == NULL) {
        entry = insertCacheEntry(tableId, key, keyLength, NULL, 0);
    } else {
        entry->objectBuf->reset();
        Object::appendKeysAndValueToBuffer(
                keyObj, NULL, 0, entry->objectBuf, true);
    }

    entry->type = CacheEntry::REMOVE;
}

/**
 * Replace the value of a given object, or create a new object if none
 * previously existed as part of this transaction.
 *
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.
 * \param keyLength
 *      Size in bytes of the key.
 * \param buf
 *      Address of the first byte of the new contents for the object;
 *      must contain at least length bytes.
 * \param length
 *      Size in bytes of the new contents for the object.
 */
void
Transaction::write(uint64_t tableId, const void* key, uint16_t keyLength,
        const void* buf, uint32_t length)
{
    Key keyObj(tableId, key, keyLength);
    CacheEntry* entry = findCacheEntry(keyObj);

    if (entry == NULL) {
        entry = insertCacheEntry(tableId, key, keyLength, buf, length);
    } else {
        entry->objectBuf->reset();
        Object::appendKeysAndValueToBuffer(
                keyObj, buf, length, entry->objectBuf, true);
    }

    entry->type = CacheEntry::WRITE;
}

/**
 * Find and return the cache entry identified by the given key.
 *
 * \param key
 *      Key of the object contained in the cache entry that should be returned.
 * \return
 *      Returns a pointer to the cache entry if found.  Returns NULL otherwise.
 *      Pointer is invalid once the commitCache is modified.
 */
Transaction::CacheEntry*
Transaction::findCacheEntry(Key& key)
{
    CacheKey cacheKey = {key.getTableId(), key.getHash()};
    CommitCacheMap::iterator it = commitCache.lower_bound(cacheKey);
    CacheEntry* entry = NULL;

    while (it != commitCache.end()) {
        if (cacheKey < it->first) {
            break;
        } else if (it->second.objectBuf) {
            Key otherKey(it->first.tableId,
                         it->second.objectBuf->getKey(),
                         it->second.objectBuf->getKeyLength());
            if (key == otherKey) {
                entry = &it->second;
                break;
            }
        }
        it++;
    }
    return entry;
}

/**
 * Inserts a new cache entry with the provided key and value.  Other members
 * of the cache entry are left to their default values.
 *
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.
 * \param keyLength
 *      Size in bytes of the key.
 * \param buf
 *      Address of the first byte of the new contents for the object;
 *      must contain at least length bytes.
 * \param length
 *      Size in bytes of the new contents for the object.
 * \return
 *      Returns a pointer to the inserted cache entry.  Pointer is invalid
 *      once the commitCache is modified.
 */
Transaction::CacheEntry*
Transaction::insertCacheEntry(uint64_t tableId, const void* key,
        uint16_t keyLength, const void* buf, uint32_t length)
{
    Key keyObj(tableId, key, keyLength);
    CacheKey cacheKey = {keyObj.getTableId(), keyObj.getHash()};
    CommitCacheMap::iterator it = commitCache.insert(
            std::make_pair(cacheKey, CacheEntry()));
    it->second.objectBuf = new ObjectBuffer();
    Object::appendKeysAndValueToBuffer(
            keyObj, buf, length, it->second.objectBuf, true);
    return &it->second;
}

} // namespace RAMCloud
