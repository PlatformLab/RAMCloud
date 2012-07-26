/* Copyright (c) 2012 Stanford University
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
 *      Buffer pointing to the entire object or object tombstone in a log or
 *      segment.
 * \throw KeyException
 *      An exception is thrown if this class does not recognize the type
 *      argument provided.
 */
Key::Key(LogEntryType type, Buffer& buffer)
    : tableId(-1),
      stringKey(NULL),
      stringKeyLength(0),
      hash()
{
    if (type == LOG_ENTRY_TYPE_OBJ) {
        Object object(buffer);
        tableId = object.getTableId();
        stringKeyLength = object.getKeyLength();
        stringKey = object.getKey();

    } else if (type == LOG_ENTRY_TYPE_OBJTOMB) {
        ObjectTombstone tomb(buffer);
        tableId = tomb.getTableId();
        stringKeyLength = tomb.getKeyLength();
        stringKey = tomb.getKey();

    } else {
        throw KeyException(HERE, "unknown Log::Entry type");
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
 * \param stringKeyOffset
 *      Byte offset of the binary string key in the buffer.
 * \param stringKeyLength
 *      Length of the binary string key in bytes.
 */
Key::Key(uint64_t tableId, Buffer& buffer, uint32_t stringKeyOffset, uint16_t stringKeyLength)
    : tableId(tableId),
      stringKey(buffer.getRange(stringKeyOffset, stringKeyLength)),
      stringKeyLength(stringKeyLength),
      hash()
{
}

/**
 * Construct a new key object where the binary string key is contained in some
 * contiguous memory buffer.
 *
 * \param tableId
 *      64-bit table identifier portion of this key.
 * \param stringKey
 *      Pointer to the binary string key.
 * \param stringKeyLength
 *      Length of the binary string key in bytes.
 */
Key::Key(uint64_t tableId, const void* stringKey, uint16_t stringKeyLength)
    : tableId(tableId),
      stringKey(stringKey),
      stringKeyLength(stringKeyLength),
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
    if (!hash)
        hash.construct(getHash(tableId, stringKey, stringKeyLength));

    return *hash;
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
    if (stringKeyLength != other.stringKeyLength)
        return false;
    return (memcmp(stringKey, other.stringKey, stringKeyLength) == 0);
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
 * Return the 64-bit table identifier.
 */
uint64_t
Key::getTableId()
{
    return tableId;
}

/**
 * Return a pointer to the binary string key. 
 */
const void*
Key::getStringKey()
{
    return stringKey;
}

/**
 * Return the binary string key's length in bytes.
 */
uint16_t
Key::getStringKeyLength()
{
    return stringKeyLength;
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
Key::toString()
{
    string printable = printableBinaryString(stringKey, stringKeyLength);
    string s = format("<tableId: %lu, stringKey: \"%s\", stringKeyLength: %hu, "
        "hash: 0x%lx>", tableId, printable.c_str(), stringKeyLength,
        getHash());
    return s;
}

} // end RAMCloud
