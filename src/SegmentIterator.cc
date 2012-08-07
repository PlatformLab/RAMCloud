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
    : wrapperSegment(),
      segment(NULL),
      currentOffset(0),
      currentType(),
      currentLength()
{
}

/**
 * Construct a new SegmentIterator for the given Segment object. This
 * is typically used to iterate a segment still in master server memory.
 *
 * Note that behaviour is undefined if the segment is modified after the
 * iterator has been constructed.
 *
 * \param segment
 *      The Segment object to be iterated over.
 *
 * \return
 *      The newly constructed SegmentIterator object.
 */
SegmentIterator::SegmentIterator(Segment& segment)
    : wrapperSegment(),
      segment(&segment),
      currentOffset(0),
      currentType(),
      currentLength()
{
    if (!segment.checkMetadataIntegrity())
        throw SegmentIteratorException(HERE, "cannot iterate: corrupt segment");
}

/**
 * Construct a new SegmentIterator given a contiguous piece of memory that
 * contains the serialized contents of a segment. This is typically used
 * to iterate a segment after it was written to a backup.
 *
 * Note that behaviour is undefined if the segment is modified after the
 * iterator has been constructed.
 * 
 * \param buffer
 *      A pointer to the first byte of the segment.
 *
 * \param length
 *      The total length of the buffer.
 */
SegmentIterator::SegmentIterator(const void *buffer, uint32_t length)
    : wrapperSegment(),
      segment(NULL),
      currentOffset(0),
      currentType(),
      currentLength()
{
    wrapperSegment.construct(buffer, length);
    segment = &*wrapperSegment;

    if (!segment->checkMetadataIntegrity())
        throw SegmentIteratorException(HERE, "cannot iterate: corrupt segment");
}

/**
 * This destructor only exists for testing (that is, for mock subclasses).
 */
SegmentIterator::~SegmentIterator()
{
}

/**
 * Test if the SegmentIterator has exhausted all entries. More concretely, if
 * the current entry is valid, this will return false. After next() has been
 * called on the last valid entry, this will return true.
 *
 * \return
 *      true if there are no more entries left to iterate, else false.
 */
bool
SegmentIterator::isDone()
{
    return getType() == LOG_ENTRY_TYPE_SEGFOOTER;
}

/**
 * Progress the iterator to the next entry in the segment, if there is one.
 * If there is another entry, then after this method returns any future calls
 * to getType, getLength, appendToBuffer, etc. will use the next in the log.
 * If there are no more entries, the iterator will stay at the last one (the
 * footer).
 */
void
SegmentIterator::next()
{
    if (getType() == LOG_ENTRY_TYPE_SEGFOOTER)
        return;

    const Segment::EntryHeader* header = segment->getEntryHeader(currentOffset);
    currentOffset += sizeof32(*header) + header->getLengthBytes() + getLength();

    currentType.destroy();
    currentLength.destroy();
}

/**
 * Return the type of the entry currently pointed to by the iterator.
 */
LogEntryType
SegmentIterator::getType()
{
    if (!currentType) {
        currentType.construct(
            segment->getEntryHeader(currentOffset)->getType());
    }
    return *currentType;
}

/**
 * Return the length of the entry currently pointed to by the iterator.
 */
uint32_t
SegmentIterator::getLength()
{
    // We may be iterating a segment that has been appended to again,
    // so evict the cache if we last saw a footer.
    if (!currentLength) {
        const Segment::EntryHeader* header = segment->getEntryHeader(currentOffset);
        uint32_t length = 0;
        segment->copyOut(currentOffset + sizeof32(*header),
                         &length,
                         header->getLengthBytes());
        currentLength.construct(length);
    }
    return *currentLength;
}

/**
 * Return the byte offset of the entry currently pointed to by the iterator
 * within the segment. This is primarily useful in building Log::Position
 * objects.
 */
uint32_t
SegmentIterator::getOffset()
{
    return currentOffset;
}

/**
 * Append the current entry to the provided buffer.
 */
uint32_t
SegmentIterator::appendToBuffer(Buffer& buffer)
{
    segment->getEntry(currentOffset, buffer);
    return buffer.getTotalLength();
}

/**
 * Append the current entry to the provided buffer after first resetting the
 * buffer, removing any previous contents.
 */
uint32_t
SegmentIterator::setBufferTo(Buffer& buffer)
{
    buffer.reset();
    return appendToBuffer(buffer);
}

/******************************************************************************
 * PRIVATE METHODS
 ******************************************************************************/

} // namespace
