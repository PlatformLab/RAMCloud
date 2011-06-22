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

#ifndef RAMCLOUD_STRINGKEYADAPTER_H
#define RAMCLOUD_STRINGKEYADAPTER_H

#include "Common.h"
#include "Buffer.h"
#include "RamCloud.h"

namespace RAMCloud {

/**
 * Provides an alternate data model on top of RAMCloud with
 * string keys to address read/write/remove values.
 *
 * Warning: Read the documentation carefully for each of the calls.  This
 * implementation takes shortcuts (on hash collisions) in order to be efficient
 * and simple.
 *
 * The current implementation doesn't provide the sophisticated features of
 * the normal RAMCloud data model because some (RejectRules) would be
 * inefficient in a client-side only implementation.
 */
class StringKeyAdapter {
  public:
    /// A one-stop place to tune the maximum string key length.
    typedef uint32_t KeyLength;
    // Object format: KeyLength keyLength, char key[keyLength], data...

    /**
     * Construct a wrapper for a normal RamCloud client which provides
     * additional methods for string key addressed storage.
     *
     * \param client
     *      A connected RamCloud where data will be stored.
     */
    explicit StringKeyAdapter(RamCloud& client)
        : client(client)
    {
    }

    static uint64_t hash(const char* value,
                         uint32_t length);
    void read(uint32_t tableId,
              const char* key,
              KeyLength keyLength,
              Buffer& value);
    void remove(uint32_t tableId,
                const char* key,
                KeyLength keyLength);
    void write(uint32_t tableId,
               const char* key,
               KeyLength keyLength,
               const void* buf,
               uint32_t length);

  private:
    /// A RAMCloud client which is connected over which this adapter works.
    RamCloud& client;
};

} // namespace RAMCloud

#endif // RAMCLOUD_STRINGKEYADAPTER_H
