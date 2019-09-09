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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#pragma GCC diagnostic ignored "-Weffc++"
#include "EnumerationIterator.pb.h"
#pragma GCC diagnostic pop

#include "Common.h"
#include "Buffer.h"

namespace RAMCloud {

/**
 * This class describes the current state of an enumeration; it is
 * created by a server, returned in serialized form to the client with
 * each batch of enumeration data, and the client then returns the
 * iterator with its next request so that the server can ensure that
 * each object is returned exactly once.
 *
 * The contents of the iterator are intentionally opaque to
 * clients. You should never need to include this header from client
 * (or client library) code.
 *
 * The iterator consists of a stack of entries, each of which describes
 * an incomplete enumeration. Together, they guarantee that each object
 * is returned exactly once, even in the face of server crashes and
 * tablet reconfigurations.
 *
 * Iterators are serialized to the network via
 * ProtoBuf::EnumerationIterator. Any changes made here must also be
 * made in ProtoBuf::EnumerationIterator.
 */
class EnumerationIterator {
  public:
    /**
     * A stack frame in the iterator. Each frame is a filter that excludes
     * objects that have already been enumerated. Normally there is only
     * a single frame on the stack, which represents the current state of
     * enumerating a particular tablet. However, it is possible for an
     * enumeration to be interrupted when a tablet has been partially
     * enumerated (e.g. server crashes). When the enumeration resumes, the
     * key hash range being enumerated could be on a different server, with
     * a different tablet structure and even a different hash table structure
     * (e.g., different number of buckets in the table).  When this happens
     * an additional frame is added to the stack for the current server, but
     * the old frame is retained in order to exclude objects that were already
     * enumerated by the old server. If a tablet moves multiple times during
     * iterations then there can be multiple frames on the stack, one for each
     * reconfiguration that occurred.
     *
     * When a server receives an iterator during enumeration, it can continue
     * working with the same iterator if the \c tabletStartHash,
     * \c tabletEndHash, and \c numBuckets in the top stack entry match its own
     * configuration; if not, it must add a new frame to the top of its stack.
     * Only the top entry on the stack is updated during enumeration; the
     * others are read-only. Stack frames can be deleted once all objects with
     * key hashes less than or equal to the frame's \c tabletEndHash have been
     * enumerated.
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

        /// The next bucket to iterate when resuming iteration: if an
        /// object is in the range given by \c tabletStartHash and
        /// \c tabletEndHash, and if it would hash to a bucket index
        /// less than this in a table of size \c numBuckets, then it has
        /// already been enumerated.
        uint64_t bucketIndex;

        /// The next key hash to iterate when resuming inside a bucket that
        /// contained too many objects to return in a single RPC. If an
        /// object is in the range given by \c tabletStartHash and
        /// \c tabletEndHash, and if it would hash to \c bucketIndex
        /// in a table of size \c numBuckets, and if its key hash is less
        /// than this, then it has already been enumerated.
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
