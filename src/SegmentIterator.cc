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

#include "SegmentIterator.h"
#include "LogEntryTypes.h"

namespace RAMCloud {

/**
 * This default constructor creates an unusable iterator. This only exists
 * so that mock subclasses can be written.
 */
SegmentIterator::SegmentIterator()
    : wrapperSegment(),
      buffer(NULL),
      length(0),
      segment(NULL),
      certificate(),
      currentOffset(0),
      currentHeader(NULL),
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
 * Operations are only safe and valid if the segment it iterates over
 * is well-formed. If unsure, use checkMetadataIntegrity() ensure the
 * segment data is safe for iteration before use.
 *
 * \param segment
 *      The Segment object to be iterated over.
 *
 * \return
 *      The newly constructed SegmentIterator object.
 */
SegmentIterator::SegmentIterator(Segment& segment)
    : wrapperSegment(),
      buffer(NULL),
      length(0),
      segment(&segment),
      certificate(),
      currentOffset(0),
      currentHeader(segment.getEntryHeader(0)),
      currentLength()
{
    segment.getAppendedLength(certificate);
}

/**
 * Construct a new SegmentIterator given a contiguous piece of memory that
 * contains the serialized contents of a segment. This is typically used
 * to iterate a segment after it was written to a backup.
 *
 * Note that behaviour is undefined if the segment is modified after the
 * iterator has been constructed.
 *
 * Operations are only safe and valid if the segment it iterates over
 * is well-formed. If unsure, use checkMetadataIntegrity() ensure the
 * segment data is safe for iteration before use.
 * 
 * \param buffer
 *      A pointer to the first byte of the segment.
 *
 * \param length
 *      The total length of the buffer.
 *
 * \param certificate
 *      A Certificate which is used to find the length of this segment and
 *      to check the integrity of its metadata. Certificates are generated
 *      by getAppendedLength().
 */
SegmentIterator::SegmentIterator(const void *buffer, uint32_t length,
                                 const Segment::Certificate& certificate)
    : wrapperSegment(),
      buffer(buffer),
      length(length),
      segment(NULL),
      certificate(certificate),
      currentOffset(0),
      currentHeader(NULL),
      currentLength()
{
    wrapperSegment.construct(buffer, length);
    segment = &*wrapperSegment;
    currentHeader = segment->getEntryHeader(0);
}

SegmentIterator::SegmentIterator(const SegmentIterator& other)
    : wrapperSegment(),
      buffer(other.buffer),
      length(other.length),
      segment(other.segment),
      certificate(other.certificate),
      currentOffset(other.currentOffset),
      currentHeader(other.currentHeader),
      currentLength(other.currentLength)
{
    if (other.wrapperSegment) {
        wrapperSegment.construct(buffer, length);
        segment = wrapperSegment.get();
    }
}

SegmentIterator&
SegmentIterator::operator=(const SegmentIterator& other)
{
    if (this == &other)
        return *this;
    buffer = other.buffer;
    length = other.length;
    segment = other.segment;
    certificate = other.certificate;
    currentOffset = other.currentOffset;
    currentHeader = other.currentHeader;
    currentLength = other.currentLength;
    if (other.wrapperSegment) {
        wrapperSegment.construct(buffer, length);
        segment = wrapperSegment.get();
    }
    return *this;
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
    return getOffset() >= certificate.segmentLength;
}

/**
 * Progress the iterator to the next entry in the segment, if there is one.
 * If there is another entry, then after this method returns any future calls
 * to getType, getLength, appendToBuffer, etc. will use the next in the log.
 * Calling next() after the iterator isDone() has no effect.
 */
void
SegmentIterator::next()
{
    if (isDone())
        return;

    currentOffset += sizeof32(*currentHeader) +
                     currentHeader->getLengthBytes() +
                     getLength();
    currentHeader = segment->getEntryHeader(currentOffset);
    currentLength.destroy();
}

/**
 * Return the type of the entry currently pointed to by the iterator.
 * If no entry is currently pointed to, returns LOG_ENTRY_TYPE_INVALID.
 */
LogEntryType
SegmentIterator::getType()
{
    if (currentHeader == NULL)
        return LOG_ENTRY_TYPE_INVALID;
    return currentHeader->getType();
}

/**
 * Return the length of the entry currently pointed to by the iterator.
 * If no entry is currently pointed to, returns 0.
 */
uint32_t
SegmentIterator::getLength()
{
    if (currentHeader == NULL)
        return 0;

    if (!currentLength) {
        uint32_t length = 0;
        segment->copyOut(currentOffset + sizeof32(*currentHeader),
                         &length,
                         currentHeader->getLengthBytes());
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
    uint32_t entryOffset = currentOffset +
                           sizeof32(*currentHeader) +
                           currentHeader->getLengthBytes();
    return segment->appendToBuffer(buffer, entryOffset, getLength());
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

/**
 * Check the integrity of the segment's metadata by iterating over all entries
 * and ensuring that:
 *
 *  1) All entry lengths are within bounds.
 *  2) The computed length and checksum match those stored in the provided
 *     certificate.
 *
 * If the check passes, this segment may be safely iterated over in the most
 * trivial way. Further, with high probability the metadata is correct and the
 * appropriate data will be observed.
 *
 * Segments are not responsible for the integrity of the data they store, so
 * appended data that anyone cares about should include their own checksums.
 *
 * \throw SegmentIteratorException
 *      If the segment metadata is corrupt. Advancing the iterator after
 *      this exception is unsafe.
 */
void
SegmentIterator::checkMetadataIntegrity()
{
    if (!segment->checkMetadataIntegrity(certificate))
        throw SegmentIteratorException(HERE, "cannot iterate: corrupt segment");
}

/******************************************************************************
 * PRIVATE METHODS
 ******************************************************************************/

} // namespace
