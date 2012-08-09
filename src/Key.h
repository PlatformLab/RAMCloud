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

/**
 * RAMCloud objects are named using a 2-tuple consisting of a 64-bit table
 * identifier and a binary string key. This class represents that tuple,
 * and allows us to pass Key references around, rather than a 64-bit tableId,
 * void pointer, and a length.
 *
 * This class also caches the hash value of the key, avoiding recomputation
 * when the hash is needed in various layers of the system.
 *
 * Multiple constructors provide convenient ways to construct a key given an
 * entry in the log, a Buffer containing a key (perhaps an RPC request), etc.
 *
 * Note that this class does not manage any memory. It simply creates a wrapper
 * around memory that is assumed to live at least as long as the key object
 * does itself. This is typically not a concern, since keys are usually built
 * in an RPC handler and refer to data either in the request itself, or in the
 * log, both of which remain allocated for the duration of the RPC.
 */
class Key {
  public:
    Key(LogEntryType type, Buffer& buffer);
    Key(uint64_t tableId, Buffer& buffer,
        uint32_t stringKeyOffset, uint16_t stringKeyLength);
    Key(uint64_t tableId, const void* stringKey, uint16_t stringKeyLength);
    HashType getHash();
    bool operator==(const Key& other) const;
    bool operator!=(const Key& other) const;
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
    /**
     * Take the binary string key and convert it into a printable string.
     * Any printable ASCII characters (including space, but not other
     * whitespace), will be unchanged. Any non-printable characters will
     * be represented in escaped hexadecimal form, for example "\xf8\x07".
     *
     * \param input
     *      Pointer to some memory that may or may not contain ascii
     *      characters.
     * \param length
     *      Length of the input in bytes.
     */
    static string
    printableBinaryString(const void* input, uint32_t length)
    {
        string s = "";
        const unsigned char* c = reinterpret_cast<const unsigned char*>(input);

        for (uint16_t i = 0; i < length; i++) {
            if (isprint(c[i]))
                s += c[i];
            else
                s += format("\\x%02x", static_cast<uint32_t>(c[i]));
        }

        return s;
    }

    /// The 64-bit table identifier.
    uint64_t tableId;

    /// Pointer to the binary string key.
    const void* stringKey;

    /// Length of the binary string key in bytes.
    uint16_t stringKeyLength;

    /// Cache for this key's hash. Initially empty and filled on demand when
    /// getHash() is called. Used to avoid recalculation in subsequent calls.
    Tub<HashType> hash;

    DISALLOW_COPY_AND_ASSIGN(Key);
};

} // end RAMCloud

#endif // RAMCLOUD_KEY_H
