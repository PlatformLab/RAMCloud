/* Copyright (c) 2009-2012 Stanford University
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

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "SegmentIterator.h"
#include "LogEntryTypes.h"

namespace RAMCloud {

/**
 * This default constructor creates an unusable iterator. This only exists
 * so that mock subclasses can be written.
 */
SegmentIterator::SegmentIterator()
    : segment(),
      segmentBuffer(),
      segmentLength(),
      currentOffset(0)
{
}

/**
 * Construct a new SegmentIterator for the given Segment object. This
 * is typically used to iterate a segment still in master server memory.
 *
 * \param segment
 *      The Segment object to be iterated over.
 *
 * \return
 *      The newly constructed SegmentIterator object.
 */
SegmentIterator::SegmentIterator(Segment& segment)
    : segment(&segment),
      segmentBuffer(),
      segmentLength(),
      currentOffset(0)
{
    checkIntegrity();
}

/**
 * Construct a new SegmentIterator given a contiguous piece of memory that
 * contains the serialized contents of a segment. This is typically used
 * to iterate a segment after it was written to a backup.
 * 
 * \param buffer
 *      A pointer to the first byte of the segment.
 *
 * \param length
 *      The total length of the buffer.
 */
SegmentIterator::SegmentIterator(const void *buffer, uint32_t length)
    : segment(),
      segmentBuffer(buffer),
      segmentLength(length),
      currentOffset(0)
{
    checkIntegrity();
}

/**
 * This destructor only exists for testing (that is, for mock subclasses).
 */
SegmentIterator::~SegmentIterator()
{
}

/**
 * Test if the SegmentIterator has exhausted all entries.
 * \return
 *      true if there are no more entries left to iterate, else false.
 */
bool
SegmentIterator::isDone()
{
    return getEntryHeader(currentOffset)->getType() == LOG_ENTRY_TYPE_SEGFOOTER;
}

/**
 * Progress the iterator to the next entry in the Segment, if there is one.
 * Future calls to #getType, #getLength, #getPointer, and #getOffset will
 * reflect the next SegmentEntry's parameters.
 */
void
SegmentIterator::next()
{
    const Segment::EntryHeader* header = getEntryHeader(currentOffset);
    if (header->getType() == LOG_ENTRY_TYPE_SEGFOOTER)
        return;

    uint32_t length = 0;
    copyOut(currentOffset + sizeof32(*header), &length, header->getLengthBytes());
    currentOffset += (sizeof32(*header) + header->getLengthBytes() + length);
}

LogEntryType
SegmentIterator::getType()
{
    const Segment::EntryHeader* header = getEntryHeader(currentOffset);
    return header->getType();
}

uint32_t
SegmentIterator::getLength()
{
    const Segment::EntryHeader* header = getEntryHeader(currentOffset);
    uint32_t length = 0;
    copyOut(currentOffset + sizeof32(*header), &length, header->getLengthBytes());
    return length;
}

Buffer&
SegmentIterator::appendToBuffer(Buffer& buffer)
{
    const Segment::EntryHeader* header = getEntryHeader(currentOffset);
    uint32_t offset = currentOffset + sizeof32(*header) + header->getLengthBytes();
    uint32_t length = getLength();

    // TODO(Steve): This is nearly identical to Segment::appendToBuffer.
    while (length > 0) {
        uint32_t contigBytes = std::min(length, getContiguousBytesAt(offset));
        if (contigBytes == 0)
            break;

        Buffer::Chunk::appendToBuffer(&buffer,
                                      getAddressAt(offset),
                                      contigBytes);

        offset += contigBytes;
        length -= contigBytes;
    }

    return buffer;
}

Buffer&
SegmentIterator::setBufferTo(Buffer& buffer)
{
    buffer.reset();
    return appendToBuffer(buffer);
}

/******************************************************************************
 * PRIVATE METHODS
 ******************************************************************************/

const void*
SegmentIterator::getAddressAt(uint32_t offset)
{
    if (segment)
        return (*segment)->getAddressAt(offset);

    if (offset >= *segmentLength)
        return NULL;

    return reinterpret_cast<const void*>(
        reinterpret_cast<const uint8_t*>(*segmentBuffer) + offset);
}

uint32_t
SegmentIterator::getContiguousBytesAt(uint32_t offset)
{
    if (segment)
        return (*segment)->getContiguousBytesAt(offset);

    if (offset <= *segmentLength)
        return *segmentLength - offset;

    return 0;
}

void
SegmentIterator::copyOut(uint32_t offset, void* buffer, uint32_t length)
{
    if (segment) {
        (*segment)->copyOut(offset, buffer, length);
        return;
    }
    
    if (offset >= *segmentLength || (offset + length) > *segmentLength)
        throw SegmentIteratorException(HERE, "offset and/or length overflow");

    memcpy(buffer,
           reinterpret_cast<const uint8_t*>(*segmentBuffer) + offset,
           length);
}

const Segment::EntryHeader*
SegmentIterator::getEntryHeader(uint32_t offset)
{
    assert(sizeof(Segment::EntryHeader) == 1);
    return reinterpret_cast<const Segment::EntryHeader*>(getAddressAt(offset));
}

/**
 * Check the integrity of a segment by iterating over all entries and
 * ensuring that:
 *
 *  1) All entry lengths are within bounds.
 *  2) A footer is present.
 *  3) The metadata checksum matches what's in the footer.
 *
 *  Any violation results in an exception.
 */
void
SegmentIterator::checkIntegrity()
{
    uint32_t offset = 0;
    Crc32C checksum;

    const Segment::EntryHeader* header = NULL;
    while (getContiguousBytesAt(offset) > 0) {
        header = getEntryHeader(offset); 
        checksum.update(header, sizeof(*header));

        uint32_t length = 0;
        copyOut(offset + sizeof32(*header), &length, header->getLengthBytes());
        checksum.update(&length, header->getLengthBytes());

        if (header->getType() == LOG_ENTRY_TYPE_SEGFOOTER)
            break;

        offset += (sizeof32(*header) + header->getLengthBytes() + length);
    }

    if (header == NULL || header->getType() != LOG_ENTRY_TYPE_SEGFOOTER)
        throw SegmentIteratorException(HERE, "segment corrupt: no footer");

    Segment::Footer footerInSegment(false, checksum);
    copyOut(offset + sizeof32(*header) + header->getLengthBytes(),
            &footerInSegment, sizeof32(footerInSegment));
    
    Segment::Footer expectedFooter(footerInSegment.closed,
                                   checksum);

    if (footerInSegment.checksum != expectedFooter.checksum)
        throw SegmentIteratorException(HERE, "segment corrupt: bad checksum");
}

} // namespace
