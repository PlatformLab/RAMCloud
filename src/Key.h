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

#ifndef RAMCLOUD_KEY_H
#define RAMCLOUD_KEY_H

#include "Common.h"
#include "LogEntryTypes.h"
#include "MurmurHash3.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * The type of the hash for the key of an object.
 */
typedef uint64_t HashType;

/**
 * An exception that is thrown when a Key cannot be constructed,
 * perhaps because the type of log entry is unknown.
 */
struct KeyException : public Exception {
    KeyException(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
};

class Key {
  public:
    Key(LogEntryType type, Buffer& buffer);
    Key(uint64_t tableId, Buffer& buffer, uint32_t stringKeyOffset, uint16_t stringKeyLength);
    Key(uint64_t tableId, const void* stringKey, uint16_t stringKeyLength);
    HashType getHash();
    bool operator==(const Key& other);
    uint64_t getTableId();
    const void* getStringKey();
    uint16_t getStringKeyLength();
    string toString();

    /**
     * Given a key, returns its hash value.
     *
     * \param tableId
     *      64-bit identifier of the table this key is in.
     * \param stringKey
     *      Variable length binary string that uniquely identifies the object
     *      within tableId. It does not necessarily have to be nul-terminated
     *      like a C-style string.
     * \param stringKeyLength
     *      Size stringKey in bytes.
     * \return
     *      Hash value of the Key that these arguments represent.
     */
    static HashType
    getHash(uint64_t tableId, const void* stringKey, uint16_t stringKeyLength)
    {
        // Indexes 0 and 1 get the tableId's hash, 2 and 3 the string key's.
        uint64_t tmp[4];
        MurmurHash3_x64_128(&tableId, sizeof(tableId), 0, &tmp[0]);
        MurmurHash3_x64_128(stringKey, stringKeyLength, 0, &tmp[2]);

        uint64_t out[2];
        MurmurHash3_x64_128(tmp, sizeof(tmp), 0, out);
        return out[0];
    }

  PRIVATE:
    string printableStringKey();

    uint64_t tableId;
    const void* stringKey;
    uint16_t stringKeyLength;
    Tub<HashType> hash;

    DISALLOW_COPY_AND_ASSIGN(Key);
};

} // end RAMCloud

#endif // RAMCLOUD_KEY_H
