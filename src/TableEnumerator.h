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

#ifndef RAMCLOUD_TABLEENUMERATOR_H
#define RAMCLOUD_TABLEENUMERATOR_H

#include "RamCloud.h"
#include "Object.h"

namespace RAMCloud {

/**
 * This class provides the client-side interface for table enumeration;
 * each instance of this class can be used to enumerate the objects in
 * a single table.
 */
class TableEnumerator {
  public:
    TableEnumerator(RamCloud& ramCloud, uint64_t tableId, bool keysOnly);
    bool hasNext();
    void next(uint32_t* size, const void** object);
    void nextObjectBlob(Buffer** buffer);
    void nextKeyAndData(uint32_t* keyLength, const void** key,
                        uint32_t* dataLength, const void** data);
  private:
    void requestMoreObjects();

    /// The RamCloud master object.
    RamCloud& ramcloud;

    /// The table containing the tablet being enumerated.
    uint64_t tableId;

    /// False means that full objects are returned, containing both keys
    /// and data. True means that the returned objects have
    /// been truncated so that the object data (normally the last
    /// field of the object) is omitted.
    bool keysOnly;

    /// The start hash of the tablet being enumerated.
    uint64_t tabletStartHash;

    /// Set to true when the entire enumeration has completed.
    bool done;

    /// Opaque storage keeps track of the state of enumeration;
    /// contents are managed by the server.
    Buffer state;

    /// A buffer to hold the payload of object last received from
    /// the server, and currently being read out by the client.
    Buffer objects;

    /// The next offset to read within the objects buffer.
    uint32_t nextOffset;

    DISALLOW_COPY_AND_ASSIGN(TableEnumerator);
};

} // end RAMCloud

#endif  // RAMCLOUD_TABLEENUMERATOR_H
