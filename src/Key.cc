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

Key::Key(uint64_t tableId, Buffer& buffer, uint32_t stringKeyOffset, uint16_t stringKeyLength)
    : tableId(tableId),
      stringKey(buffer.getRange(stringKeyOffset, stringKeyLength)),
      stringKeyLength(stringKeyLength),
      hash()
{
}

Key::Key(uint64_t tableId, const void* stringKey, uint16_t stringKeyLength)
    : tableId(tableId),
      stringKey(stringKey),
      stringKeyLength(stringKeyLength),
      hash()
{
}

uint64_t
Key::getHash()
{
    if (!hash)
        hash.construct(getHash(tableId, stringKey, stringKeyLength));

    return *hash;
}

bool
Key::operator==(const Key& other)
{
    if (hash && other.hash && *hash != *other.hash)
        return false;
    if (tableId != other.tableId)
        return false;
    if (stringKeyLength != other.stringKeyLength)
        return false;
    return (memcmp(stringKey, other.stringKey, stringKeyLength) == 0);
}

uint64_t
Key::getTableId()
{
    return tableId;
}

const void*
Key::getStringKey()
{
    return stringKey;
}

uint16_t
Key::getStringKeyLength()
{
    return stringKeyLength;
}

string
Key::toString()
{
    string s = format("<tableId: %lu, stringKey: %s, stringKeyLength: %hu, "
        "hash: %lu>", tableId, printableStringKey().c_str(), stringKeyLength,
        getHash());
    return s;
}

/******************************************************************************
 * PRIVATE METHODS
 ******************************************************************************/

/**
 * Take the binary string key and convert it into a printable string.
 * Any printable ASCII characters (including space, but not other
 * whitespace), will be unchanged. Any non-printable characters will
 * be represented in escaped hexadecimal form, for example '\xf8\x07'.
 */
string
Key::printableStringKey()
{
    string s = "";
    const unsigned char* c = reinterpret_cast<const unsigned char*>(stringKey);

    for (uint16_t i = 0; i < stringKeyLength; i++) {
        if (isprint(c[i]))
            s += c[i];
        else
            s += format("\\x%02x", static_cast<uint32_t>(c[i]));
    }

    return s;
}

} // end RAMCloud
