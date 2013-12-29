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

#ifndef RAMCLOUD_ENUMERATION_H
#define RAMCLOUD_ENUMERATION_H

#include "Buffer.h"
#include "EnumerationIterator.h"
#include "HashTable.h"
#include "Log.h"

namespace RAMCloud {

/**
 * The Enumeration class encapsulates the server-side logic for
 * servicing an EnumerationRPC. This class is intended to be
 * instantiated from MasterService::enumeration().
 *
 * Each Enumeration iterates through the master's hash table in bucket
 * order, collecting objects from the requested tablet into a buffer,
 * until the buffer fills up. The Enumeration also updates the
 * provided EnumerationIterator with the state necessary to resume on
 * the next EnumerationRPC.
 */
class Enumeration {
  public:
    Enumeration(uint64_t tableId,
                bool keysOnly,
                uint64_t requestedTabletStartHash,
                uint64_t actualTabletStartHash,
                uint64_t actualTabletEndHash,
                uint64_t* nextTabletStartHash,
                EnumerationIterator& iter,
                Log& log,
                HashTable& objectMap,
                Buffer& payload, uint32_t maxPayloadBytes);
    void complete();

  PRIVATE:
    /// The table containing the tablet being enumerated.
    uint64_t tableId;

    /// False means that full objects are returned, containing both keys
    /// and data. True means that the returned objects have
    /// been truncated so that the object data (normally the last
    /// field of the object) is omitted.
    bool keysOnly;

    /// The start hash of the tablet as requested by the client.
    uint64_t requestedTabletStartHash;

    /// The start hash of the tablet that actually lives on this server.
    uint64_t actualTabletStartHash;

    /// The end hash of the tablet that actually lives on this server.
    uint64_t actualTabletEndHash;

    /// A place to store the next tabletStartHash to return to client.
    uint64_t* nextTabletStartHash;

    /// The iterator provided by the client.
    EnumerationIterator& iter;

    /// The log we're enumerating over. Needed to look up hash table references.
    Log& log;

    /// The hash table of objects living on this server.
    HashTable& objectMap;

    /// A Buffer to hold the resulting objects.
    Buffer& payload;

    /// The maximum number of bytes of objects to be returned.
    uint32_t maxPayloadBytes;
};

}

#endif
