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

#ifndef RAMCLOUD_ENUMERATIONITERATOR_H
#define RAMCLOUD_ENUMERATIONITERATOR_H

#include "EnumerationIterator.pb.h"

#include "Common.h"

namespace RAMCloud {

/**
 * The iterator used in servicing the MasterService::enumeration RPC
 * is a stack containing all information needed to resume enumeration
 * on subsequent RPCs.
 *
 * The contents of the iterator are intentionally opaque to
 * clients. You should never need to include this header from client
 * (or client library) code.
 *
 * Each frame on the iterator stack refers to the state of enumeration
 * after the return of a specific RPC. If a server crashes, or the
 * tablet configuration changes, etc., whatever server receives the
 * iterator can use the information left on the stack to filter out
 * entries that have already been returned to the user. Otherwise, if
 * enumeration proceeds normally, stack frames are popped from the
 * stack when enumeration of a tablet is complete.
 *
 * Iterators are serialized to the network via
 * ProtoBuf::EnumerationIterator. Any changes made here must also be
 * made in ProtoBuf::EnumerationIterator.
 */
class EnumerationIterator {
  public:
    /**
     * A stack frame in the iterator.
     *
     * Any changes made here must also be made in
     * ProtoBuf::EnumerationIterator_Frame.
     */
    struct Frame {
        Frame(uint64_t tabletStartHash, uint64_t tabletEndHash,
              uint64_t numBuckets, uint64_t bucketIndex,
              uint64_t bucketNextHash);

        /// The smallest key hash value for the tablet being enumerated.
        uint64_t tabletStartHash;

        /// The largest key hash value for the tablet being enumerated.
        uint64_t tabletEndHash;

        /// The number of buckets in the hash table of the server
        /// currently owning the tablet.
        uint64_t numBuckets;

        /// The next bucket to iterate when resuming iteration.
        uint64_t bucketIndex;

        /// The next key hash to iterate when resuming inside a
        /// bucket that was too large to fit inside a single RPC.
        uint64_t bucketNextHash;
    };

    EnumerationIterator(Buffer& buffer, uint32_t offset, uint32_t length);
    Frame& top();
    const Frame& get(uint32_t index);
    void pop();
    void push(const Frame& frame);
    void clear();
    uint32_t size();
    uint32_t serialize(Buffer& buffer);

  PRIVATE:
    /// The stack of iterator frames.
    std::vector<Frame> frames;
};

}

#endif
