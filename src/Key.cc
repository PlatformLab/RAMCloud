/* Copyright (c) 2012-2014 Stanford University
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
#include "Key.h"
#include "Object.h"
#include "StringUtil.h"

namespace RAMCloud {

/**
 * Construct a new key object by extracting the appropriate fields from a
 * log entry. Use this method when obtaining the key from a serialized
 * object or tombstone in the log.
 *
 * \param type
 *      The log entry type of this entry, as indicated by the log or segment
 *      code.
 * \param buffer
 *      Buffer pointing to the entire object or tombstone entry in a log or
 *      segment. The buffer must exist as long as this key object exists,
 *      since the key will simply point into the data in the buffer.
 * \throw FatalError 
 *      A FatalError exception is thrown if this class does not recognize the
 *      type argument provided.
 */
Key::Key(LogEntryType type, Buffer& buffer)
    : tableId(-1),
      key(NULL),
      keyLength(0),
      hash()
{
    if (type == LOG_ENTRY_TYPE_OBJ) {
        Object object(buffer);
        tableId = object.getTableId();
        keyLength = object.getKeyLength();
        key = object.getKey();

    } else if (type == LOG_ENTRY_TYPE_OBJTOMB) {
        ObjectTombstone tomb(buffer);
        tableId = tomb.getTableId();
        keyLength = tomb.getKeyLength();
        key = tomb.getKey();

    } else {
        throw FatalError(HERE, "unknown Log::Entry type");
    }
}

/**
 * Construct a new key object where the binary string key is contained in a
 * buffer (for example, in an RPC request).
 *
 * Note that this method will copy the binary string key into contiguous memory
 * if necessary.
 *
 * \param tableId
 *      64-bit table identifier portion of this key.
 * \param buffer
 *      Buffer containing the binary string key somewhere inside.
 * \param keyOffset
 *      Byte offset of the binary string key in the buffer.
 * \param keyLength
 *      Length of the binary string key in bytes.
 */
Key::Key(uint64_t tableId, Buffer& buffer,
         uint32_t keyOffset, KeyLength keyLength)
    : tableId(tableId),
      key(buffer.getRange(keyOffset, keyLength)),
      keyLength(keyLength),
      hash()
{
    // TODO(anyone): We could instead delay calling getRange() until absolutely
    // necessary. This might make things faster since getRange() could allocate
    // and copy memory if the data is not all contiguous. It is unclear how
    // frequently this might happen and whether or not it can adversely affect
    // performance, so the current code errs on the side of simplicity.
}

/**
 * Construct a new key object where the binary string key is contained in some
 * contiguous memory buffer.
 *
 * \param tableId
 *      64-bit table identifier portion of this key.
 * \param key
 *      Pointer to the binary string key.
 * \param keyLength
 *      Length of the binary string key in bytes.
 */
Key::Key(uint64_t tableId, const void* key, KeyLength keyLength)
    : tableId(tableId),
      key(key),
      keyLength(keyLength),
      hash()
{
}

/**
 * Return the 64-bit hash of this key, which covers the table identifier and the
 * binary string key. The first invocation will compute the key and cache it for
 * all subsequent invocations.
 */
uint64_t
Key::getHash()
{
    if (expect_false(!hash))
        hash.construct(getHash(tableId, key, keyLength));

    return *hash;
}

/**
 * Given a key, returns its hash value.
 *
 * \param tableId
 *      64-bit identifier of the table this key is in.
 * \param key
 *      Variable length binary string that uniquely identifies the object
 *      within tableId. It does not necessarily have to be nul-terminated
 *      like a C-style string.
 * \param keyLength
 *      Size key in bytes.
 * \return
 *      Hash value of the Key that these arguments represent.
 */
KeyHash
Key::getHash(uint64_t tableId, const void* key, KeyLength keyLength)
{
    // It would be nice if MurmurHash3 took a 64-bit seed so we could cheaply
    // include the full tableId. For now just cast down to 32-bits and hope for
    // the best. We used to invoke the hash function multiple times, but that
    // added too much additional latency to hash table lookups.
    uint32_t tableIdSeed = static_cast<uint32_t>(tableId);
    uint64_t out[2];
    MurmurHash3_x64_128(key, keyLength, tableIdSeed, &out);
    return out[0];
}

/**
 * Return a pointer to the binary string key. It is guaranteed to be contiguous
 * in memory.
 */
const void*
Key::getStringKey() const
{
    return key;
}

/**
 * Return the binary string key's length in bytes.
 */
KeyLength
Key::getStringKeyLength() const
{
    return keyLength;
}

/**
 * Return the 64-bit table identifier.
 */
uint64_t
Key::getTableId() const
{
    return tableId;
}

/**
 * Compare two keys for equality. This method tries to avoid full binary string
 * key comparison by first checking hashes (if available), table identifiers,
 * and binary string key lengths.
 *
 * \return
 *      True if the keys are equal, otherwise false.
 */
bool
Key::operator==(const Key& other) const
{
    if (hash && other.hash && *hash != *other.hash)
        return false;
    if (tableId != other.tableId)
        return false;
    if (keyLength != other.keyLength)
        return false;

    // bcmp is used here because it's about 20-25ns faster than memcmp with
    // 8-byte strings. The problem with memcmp appears to be that GCC emits
    // an "optimization" (repz cmpsb) that is much slower than glibc's memcmp.
    return (bcmp(key, other.key, keyLength) == 0);
}

/**
 * Compare two keys for inequality. See operator==(), which this simply
 * negates.
 */
bool
Key::operator!=(const Key& other) const
{
    return !operator==(other);
}

/**
 * Return a string representation of this key, which describes the entire
 * 3-tuple of table identifier, binary string key, and the string key's
 * length.
 *
 * Since string keys need not be printable ASCII strings, this function
 * will escape all non-printable characters, as well as all non-space
 * whitespace characters in C-style form (for example, "\x0a" for the
 * newline character).
 */
string
Key::toString() const
{
    string printable = StringUtil::binaryToString(key, keyLength);
    string s = format("<tableId: %lu, key: \"%s\", keyLength: %hu, "
        "hash: 0x%lx>", tableId, printable.c_str(), keyLength,
        getHash(tableId, key, keyLength));
    return s;
}

} // end RAMCloud
