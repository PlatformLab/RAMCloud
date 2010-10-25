/* Copyright (c) 2009, 2010 Stanford University
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

// RAMCloud pragma [GCCWARN=5]
// RAMCloud pragma [CPPLINT=0]

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <Segment.h>
#include <SegmentIterator.h>
#include <LogTypes.h>

namespace RAMCloud {

/**
 * Constructor for Segment.
 * \param[in] logId
 *      The unique identifier for the Log to which this Segment belongs.
 * \param[in] segmentId
 *      The unique identifier for this Segment.
 * \param[in] baseAddress
 *      A pointer to memory that will back this Segment. This memory must be
 *      aligned to the size of the Segment in bytes, i.e. the capacity. Doing
 *      so permits quick calculation of the baseAddress from a random pointer
 *      into the Segment.
 * \param[in] capacity
 *      The size of the backing memory pointed to by baseAddress in bytes.
 * \return
 *      The newly constructed Segment object.
 */
Segment::Segment(uint64_t logId, uint64_t segmentId, void *baseAddress,
    uint64_t capacity)
    : baseAddress(baseAddress),
      id(segmentId),
      capacity(capacity),
      tail(0),
      bytesFreed(0)
{
    // segments must be `capacity'-aligned for fast baseAddress computation
    // we could add a power-of-2 segment size restriction to make it even faster
    if ((uintptr_t)baseAddress % capacity)
        throw 0;

    SegmentHeader header = { logId, id, capacity };
    const void *p = append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header));
    assert(p != NULL);
}

/**
 * Clean up after the Segment. Since Segments currently do not allocate
 * any memory, this is a no-op.
 */
Segment::~Segment()
{
    static_assert(sizeof(SegmentEntry) == 8);
    static_assert(sizeof(SegmentHeader) == 24);
    static_assert(sizeof(SegmentFooter) == 8);
}

/**
 * Append an entry to this Segment. Entries consist of a typed header, followed
 * by the user-specified contents. Note that this operation makes no guarantees
 * about data alignment.
 * \param[in] type
 *      The type of entry to append. All types except LOG_ENTRY_TYPE_SEGFOOTER
 *      are permitted.
 * \param[in] buffer
 *      Data to be appended to this Segment.
 * \param[in] length
 *      Length of the data to be appended in bytes.
 * \return
 *      On success, a const pointer into the Segment's backing memory with
 *      the same contents as `buffer'. On failure, NULL. 
 */
const void *
Segment::append(LogEntryType type, const void *buffer, uint64_t length)
{
    if (type == LOG_ENTRY_TYPE_SEGFOOTER || appendableBytes() < length)
        return NULL;

    return forceAppendWithEntry(type, buffer, length);
}

/**
 * Mark bytes in this Segment as freed. This simply maintains a tally that
 * can be used to compute utilisation of the Segment.
 * \param[in] length
 *      The number of bytes to mark as freed.
 */
void
Segment::free(const uint64_t length)
{
    bytesFreed += length;
    assert(bytesFreed <= capacity);
}

/**
 * Close the Segment. Once a Segment has been closed, it is considered
 * immutable, i.e. it cannot be appended to. Calling #free on a closed
 * Segment to maintain utilisation counts is still permitted. 
 */
void
Segment::close()
{
    SegmentFooter footer = { -1 };
    const void *p = forceAppendWithEntry(LOG_ENTRY_TYPE_SEGFOOTER,
                                 &footer, sizeof(footer));
    assert(p != NULL);

    // chew up remaining space to ensure that any future append() will fail
    tail += appendableBytes();
}

/**
 * Obtain a const pointer to the first byte of backing memory for this Segment.
 */
const void *
Segment::getBaseAddress() const
{
    return baseAddress;
}

/**
 * Obtain the Segment's Id, which was originally specified in the constructor.
 */
uint64_t
Segment::getId() const
{
    return id;
}

/**
 * Obtain the number of bytes of backing memory that this Segment represents.
 */
uint64_t
Segment::getCapacity() const
{
    return capacity;
}

/**
 * Obtain the maximum number of bytes that can be appended to this Segment
 * using the #append method. Buffers equal to this size or smaller are
 * guaranteed to succeed, whereas buffers larger will fail to be appended.
 */
uint64_t
Segment::appendableBytes() const
{
    uint64_t freeBytes = capacity - tail;
    uint64_t headRoom  = sizeof(SegmentEntry) + sizeof(SegmentFooter);

    // possible if called after segment has been closed
    if (freeBytes < headRoom)
        return 0;

    if ((freeBytes - headRoom) < sizeof(SegmentEntry))
        return 0;

    return freeBytes - headRoom - sizeof(SegmentEntry);
}

/**
 * Iterate over all entries in this Segment and pass them to the callback
 * provided. This is simply a convenience wrapper around #SegmentIterator.
 * \param[in] cb
 *      The callback to use on each entry.
 * \param[in] cookie
 *      A void* argument to be passed with the specified callback.
 */
void
Segment::forEachEntry(SegmentEntryCallback cb, void *cookie) const
{
    for (SegmentIterator i(this); !i.isDone(); i.next())
        cb(i.getType(), i.getPointer(), i.getLength(), cookie);
}

////////////////////////////////////////
/// Private Methods
////////////////////////////////////////

/**
 * Append exactly the provided raw bytes to the memory backing this Segment.
 * Note that no SegmentEntry is written and the only sanity check is to ensure
 * that the backing memory is not overrun.
 * \param[in] buffer
 *      Pointer to the data to be appended to the Segment's backing memory.
 * \param[in] length
 *      Length of the buffer to be appended in bytes.
 */
const void *
Segment::forceAppendBlob(const void *buffer, uint64_t length)
{
    assert((tail + length) <= capacity);
    void *p = (uint8_t *)baseAddress + tail;
    memcpy(p, buffer, length);
    tail += length;
    return p;
}

/**
 * Append an entry of any type to the Segment. This function will always
 * succeed so long as there is sufficient room left in the tail of the Segment.
 * \param[in] type
 *      The type of entry to append.
 * \param[in] buffer
 *      Data to be appended to this Segment.
 * \param[in] length
 *      Length of the data to be appended in bytes.
 */
const void *
Segment::forceAppendWithEntry(LogEntryType type, const void *buffer,
    uint64_t length)
{
    uint64_t freeBytes = capacity - tail;
    uint64_t needBytes = sizeof(SegmentEntry) + length;
    if (freeBytes < needBytes)
        return NULL;

    SegmentEntry entry = { type, length };
    forceAppendBlob(&entry, sizeof(entry));
    return forceAppendBlob(buffer, length);
}

} // namespace
