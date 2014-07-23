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

#ifndef RAMCLOUD_KEY_H
#define RAMCLOUD_KEY_H

#include "Common.h"
#include "Buffer.h"
#include "LogEntryTypes.h"
#include "MurmurHash3.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * The type of the hash for the key of an object.
 */
typedef uint64_t KeyHash;
/*
 * The length of a key in an object.
 */
typedef uint16_t KeyLength;

/**
 * RAMCloud objects can contain multiple keys in addition to the primary key.
 * However, this class is used only for the primary key which is a 2-tuple
 * consisting of a 64-bit table identifier and a binary string key. This
 * class represents that tuple, and allows us to pass Key references around,
 * rather than a 64-bit tableId, void pointer, and a length.
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
        uint32_t keyOffset, KeyLength keyLength);
    Key(uint64_t tableId, const void* key, KeyLength keyLength);
    KeyHash getHash();
    bool operator==(const Key& other) const;
    bool operator!=(const Key& other) const;
    uint64_t getTableId() const;
    const void* getStringKey() const;
    KeyLength getStringKeyLength() const;
    string toString() const;
    static KeyHash getHash(uint64_t tableId,
                           const void* key,
                           KeyLength keyLength);

  PRIVATE:
    /// The 64-bit table identifier.
    uint64_t tableId;

    /// Pointer to the binary string key.
    const void* key;

    /// Length of the binary string key in bytes.
    KeyLength keyLength;

    /// Cache for this key's hash. Initially empty and filled on demand when
    /// getHash() is called. Used to avoid recalculation in subsequent calls.
    Tub<KeyHash> hash;

    DISALLOW_COPY_AND_ASSIGN(Key);
};

} // end RAMCloud

#endif // RAMCLOUD_KEY_H
