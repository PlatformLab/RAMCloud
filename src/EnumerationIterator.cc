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

#include "EnumerationIterator.h"
#include "ProtoBuf.h"

namespace RAMCloud {

/**
 * Construct a Frame.
 *
 * \param tabletStartHash
 *      The smallest key hash value for the tablet being enumerated.
 * \param tabletEndHash
 *      The largest key hash value for the tablet being enumerated.
 * \param numBuckets
 *      The number of buckets in the hash table of the server
 *      currently owning the tablet.
 * \param bucketIndex
 *      The next bucket to iterate when resuming iteration.
 * \param bucketNextHash
 *      The next key hash to iterate when resuming inside a
 *      bucket that was too large to fit inside a single RPC.
 */
EnumerationIterator::Frame::Frame(uint64_t tabletStartHash,
                                  uint64_t tabletEndHash,
                                  uint64_t numBuckets, uint64_t bucketIndex,
                                  uint64_t bucketNextHash)
    : tabletStartHash(tabletStartHash)
    , tabletEndHash(tabletEndHash)
    , numBuckets(numBuckets)
    , bucketIndex(bucketIndex)
    , bucketNextHash(bucketNextHash)
{
}

/**
 * Parse an EnumerationIterator from a request Buffer.
 *
 * \param buffer
 *      A request Buffer.
 * \param offset
 *      The offset into the buffer at which to read the iterator.
 * \param length
 *      The length of the iterator in bytes.
 */
EnumerationIterator::EnumerationIterator(Buffer& buffer,
                                         uint32_t offset, uint32_t length)
    : frames()
{
    ProtoBuf::EnumerationIterator message;
    ProtoBuf::parseFromRequest(&buffer, offset, length, &message);
    foreach(const auto& frame, message.frames()) {
        frames.push_back(Frame(
            frame.tablet_start_hash(), frame.tablet_end_hash(),
            frame.num_buckets(), frame.bucket_index(),
            frame.bucket_next_hash()));
    }
}

/**
 * Returns the top Frame on the iterator stack.
 *
 * \return
 *      The top Frame on the stack.
 */
EnumerationIterator::Frame&
EnumerationIterator::top()
{
    return frames.back();
}

/**
 * Returns the requested Frame from the iterator stack.
 *
 * \param index
 *      An index into the stack.
 * \return
 *      The requested Frame.
 */
const EnumerationIterator::Frame&
EnumerationIterator::get(uint32_t index)
{
    return frames[index];
}

/**
 * Pops the top Frame from the iterator stack.
 */
void
EnumerationIterator::pop()
{
    frames.pop_back();
}

/**
 * Pushes a Frame onto the iterator stack.
 *
 * \param frame
 *      A Frame.
 */
void
EnumerationIterator::push(const Frame& frame)
{
    frames.push_back(frame);
}

/**
 * Clears the iterator stack.
 */
void
EnumerationIterator::clear()
{
    frames.clear();
}

/**
 * Returns the number of Frame's on the iterator stack.
 */
uint32_t
EnumerationIterator::size()
{
    return downCast<uint32_t>(frames.size());
}

/**
 * Serializes the iterator to a response Buffer.
 *
 * \param buffer
 *      A response Buffer.
 * \return
 *      The number of bytes added to the buffer.
 */
uint32_t
EnumerationIterator::serialize(Buffer& buffer)
{
    uint32_t offsetAtStart = buffer.size();

    ProtoBuf::EnumerationIterator message;
    foreach(const auto& frame, frames) {
        ProtoBuf::EnumerationIterator_Frame& part(*message.add_frames());

        part.set_tablet_start_hash(frame.tabletStartHash);
        part.set_tablet_end_hash(frame.tabletEndHash);
        part.set_num_buckets(frame.numBuckets);
        part.set_bucket_index(frame.bucketIndex);
        part.set_bucket_next_hash(frame.bucketNextHash);
    }
    ProtoBuf::serializeToResponse(&buffer, &message);

    return buffer.size() - offsetAtStart;
}

}
