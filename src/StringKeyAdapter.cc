/* Copyright (c) 2011 Stanford University
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

#include <openssl/sha.h>

#include "Common.h"
#include "Buffer.h"
#include "ClientException.h"
#include "RamCloud.h"
#include "StringKeyAdapter.h"

namespace RAMCloud {

/**
 * A hash function that is at least reasonably robust against collisions.
 * We just truncate to 64-bit since that's all the RAMCloud data model
 * supports.  Note, this hash function is susceptible to collisions caused
 * by adversarial users.
 */
uint64_t
StringKeyAdapter::hash(const char* value, uint32_t length)
{
    unsigned char buffer[SHA_DIGEST_LENGTH];
    SHA1(reinterpret_cast<const uint8_t*>(value), length, buffer);
    return *reinterpret_cast<uint64_t*>(buffer);
}

/**
 * Read the current contents of an object.
 *
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to RamCloud::openTable).
 * \param key
 *      A key within tableId of the object to be read.
 * \param keyLength
 *      The length of #key.
 * \param[out] value
 *      After a successful return, this Buffer will hold the
 *      contents of the desired object.
 *
 * \exception ObjectDoesntExistException
 * \exception InternalError
 */
void
StringKeyAdapter::read(const uint32_t tableId,
                       const char* key,
                       const KeyLength keyLength,
                       Buffer& value)
{
    const auto hashedKey = hash(key, keyLength);
    client.read(tableId, hashedKey, &value);
    const auto& storedKeyLength = *value.getStart<decltype(keyLength)>();
    const char* storedKey =
        static_cast<const char*>(value.getRange(sizeof(keyLength),
                                                keyLength));
    if ((storedKeyLength != keyLength) ||
        (memcmp(key, storedKey, keyLength)))
    {
        char keyStr[keyLength + 1];
        memcpy(keyStr, key, keyLength);
        keyStr[keyLength] = '\0';
        char storedKeyStr[storedKeyLength + 1];
        memcpy(storedKeyStr, storedKey, storedKeyLength);
        storedKeyStr[storedKeyLength] = '\0';
        LOG(DEBUG, "Key mismatch, expected '%s' but '%s' was stored at %lu",
            keyStr, storedKeyStr, hashedKey);
        throw ObjectDoesntExistException(HERE);
    }
    value.truncateFront(downCast<uint32_t>(sizeof(keyLength) + keyLength));
}

/**
 * Delete an object from a table. If the object does not currently exist
 * then the operation succeeds without doing anything.
 *
 * Notice: If the hash of #key collides with the hash of another key stored
 * earlier the call will blindly remove the earlier object even though the
 * string keys do not match.  That is, this function is sloppy and may
 * delete objects other than the one specified by #key on hash collisions.
 *
 * \param tableId
 *      The table containing the object to be deleted (return value from
 *      a previous call to openTable).
 * \param key
 *      A key identifying the object to be deleted.
 * \param keyLength
 *      The length of #key.
 *
 * \exception InternalError
 */
void
StringKeyAdapter::remove(const uint32_t tableId,
                         const char* key,
                         const KeyLength keyLength)
{
    const auto hashedKey = hash(key, keyLength);
    client.remove(tableId, hashedKey);
}

/**
 * Write a specific object in a table; overwrite any existing
 * object, or create a new object if none existed.
 *
 * Notice: If the hash of key collides with a stored object then
 * this write will blindly replace that object.
 *
 * \param tableId
 *      The table containing the desired object (return value from a
 *      previous call to openTable).
 * \param key
 *      A key by which this object can be retreived.
 * \param keyLength
 *      The length of #key.
 * \param buf
 *      Address of the first byte of the new contents for the object;
 *      must contain at least length bytes.
 * \param length
 *      Size in bytes of the new contents for the object.
 *
 * \exception InternalError
 */
void
StringKeyAdapter::write(const uint32_t tableId,
                        const char* key,
                        const KeyLength keyLength,
                        const void* buf,
                        const uint32_t length)
{
    Buffer buffer;
    Buffer::Chunk::appendToBuffer(&buffer, &keyLength, sizeof(keyLength));
    Buffer::Chunk::appendToBuffer(&buffer, key, keyLength);
    Buffer::Chunk::appendToBuffer(&buffer, buf, length);
    const auto hashedKey = hash(key, keyLength);
    RamCloud::Write write(client, tableId, hashedKey, buffer);
    write();
}

}  // namespace RAMCloud
