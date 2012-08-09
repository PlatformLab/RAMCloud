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

#include "Common.h"
#include "Crc32C.h"
#include "Segment.h"
#include "ShortMacros.h"
#include "LogEntryTypes.h"

namespace RAMCloud {

namespace SegmentInternal {
/// Default heap allocator for the zero-argument constructor.
Segment::DefaultHeapAllocator heapAllocator;
}

/**
 * Construct a segment using Segment::DEFAULT_SEGMENT_SIZE bytes dynamically
 * allocated on the heap. This constructor is useful, for instance, when a
 * temporary segment is needed to move data between servers.
 */
Segment::Segment()
    : fakeAllocator(),
      allocator(SegmentInternal::heapAllocator),
      seglets(),
      closed(false),
      tail(0),
      bytesFreed(0),
      checksum(),
      currentFooter()
{
    for (uint32_t i = 0; i < allocator.getSegletsPerSegment(); i++)
        seglets.push_back(allocator.alloc());

    appendFooter();
}

/**
 * Construct a segment using the specified allocator. Supplying a custom
 * allocator allows the creator to control the size of the segment, as well
 * as how many discontiguous pieces comprise the segment. This constructor
 * is primarily used by the log's SegmentManager, which keeps a static pool
 * of free memory for the log.
 *
 * \param allocator
 *      Allocator from which this segment will obtain its seglets.
 */
Segment::Segment(Allocator& allocator)
    : fakeAllocator(),
      allocator(allocator),
      seglets(),
      closed(false),
      tail(0),
      bytesFreed(0),
      checksum(),
      currentFooter()
{
    // If only we had C++11 support this duplication wouldn't be necessary...
    for (uint32_t i = 0; i < allocator.getSegletsPerSegment(); i++)
        seglets.push_back(allocator.alloc());

    appendFooter();
}

/**
 * Construct a segment object that wraps a previously serialized segment.
 * This constructor is primarily used when iterating over segments that
 * were written to disk or transmitted over the network.
 *
 * Note that segments created using this constructor are immutable. They
 * may not be appended to.
 *
 * \param buffer
 *      Contiguous buffer containing the entire serialized segment.
 * \param length
 *      Length of the buffer in bytes.
 */
Segment::Segment(const void* buffer, uint32_t length)
    : fakeAllocator(length),
      allocator(*fakeAllocator),
      seglets(),
      closed(true),
      tail(length),
      bytesFreed(0),
      checksum(),
      currentFooter()
{
    // We promise not to scribble on it, honest!
    seglets.push_back(const_cast<void*>(buffer));
}

/**
 * Destroy the segment, returning any memory allocated to its allocator.
 */
Segment::~Segment()
{
    for (SegletVector::iterator it = seglets.begin(); it != seglets.end(); it++)
        allocator.free(*it);
}

/**
 * Append a typed entry to this segment. Entries are binary blobs described by
 * a simple <type, length> tuple.
 *
 * \param type
 *      Type of the entry. See LogEntryTypes.h.
 * \param buffer
 *      Buffer object describing the entry to be appended.
 * \param offset
 *      Byte offset within the buffer object to begin appending from.
 * \param length
 *      Number of bytes to append starting from the given offset in the buffer.
 * \param[out] outOffset
 *      If appending was successful, the segment offset of the new entry is
 *      returned here. This is used to address the entry.
 * \return
 *      True if the append succeeded, false if there was insufficient space to
 *      complete the operation.
 */
bool
Segment::append(LogEntryType type,
                Buffer& buffer,
                uint32_t offset,
                uint32_t length,
                uint32_t& outOffset)
{
    EntryHeader entryHeader(type, length);

    // Check if sufficient space to store this and have room for a footer.
    if (bytesLeft() < (bytesNeeded(sizeof32(Footer)) + bytesNeeded(length)))
        return false;

    uint32_t startOffset = tail;

    copyIn(tail, &entryHeader, sizeof(entryHeader));
    checksum.update(&entryHeader, sizeof(entryHeader));
    tail += sizeof32(entryHeader);

    copyIn(tail, &length, entryHeader.getLengthBytes());
    checksum.update(&length, entryHeader.getLengthBytes());
    tail += entryHeader.getLengthBytes();

    copyInFromBuffer(tail, buffer, offset, length);
    tail += length;

    appendFooter();

    outOffset = startOffset;

    return true;
}

/**
 * Abbreviated append method provided for convenience. Please see the first
 * append method's documentation.
 */
bool
Segment::append(LogEntryType type, Buffer& buffer, uint32_t& outOffset)
{
    return append(type, buffer, 0, buffer.getTotalLength(), outOffset);
}

/**
 * Abbreviated append method provided for convenience. Please see the first
 * append method's documentation.
 */
bool
Segment::append(LogEntryType type, Buffer& buffer)
{
    uint32_t outOffset;
    return append(type, buffer, outOffset);
}

/**
 * Append from a void pointer, rather than a buffer. Provided for convenience.
 * Please see the first append method for documentation.
 */
bool
Segment::append(LogEntryType type,
                const void* data,
                uint32_t length,
                uint32_t& outOffset)
{
    Buffer buffer;
    buffer.appendTo(data, length);
    return append(type, buffer, 0, length, outOffset);
}

/**
 * Abbreviated append method provided for convenience. Please see the previous
 * append method's documentation.
 */
bool
Segment::append(LogEntryType type, const void* data, uint32_t length)
{
    uint32_t dummy;
    return append(type, data, length, dummy);
}

/**
 * Mark a previously appended entry as free. This simply updates usage
 * statistics.
 */
void
Segment::free(uint32_t offset)
{
    const EntryHeader* header = getEntryHeader(offset);

    uint32_t length = 0;
    copyOut(offset + sizeof32(*header), &length, header->getLengthBytes());

    bytesFreed += (sizeof32(*header) + header->getLengthBytes() + length);
    assert(bytesFreed <= tail);
}

/**
 * Make the segment as immutable. Closing it will cause all future append
 * operations to fail.
 */
void
Segment::close()
{
    if (!closed) {
        closed = true;
        appendFooter();
    }
}

/**
 * Append contents of the segment to a provided buffer.
 *
 * \param buffer
 *      Buffer to append segment contents to.
 * \param offset
 *      Offset in the segment to begin appending from.
 * \param length
 *      Number of bytes in the segmet to append, starting from the offset.
 * \return
 *      The number of actual bytes appended to the buffer. If this was less
 *      than expected, the offset or length parameter was invalid.
 */
uint32_t
Segment::appendToBuffer(Buffer& buffer, uint32_t offset, uint32_t length) const
{
    uint32_t initialLength = length;

    while (length > 0) {
        const void* contigPointer = NULL;
        uint32_t contigBytes = std::min(length, peek(offset, &contigPointer));
        if (contigBytes == 0)
            break;

        buffer.appendTo(contigPointer, contigBytes);

        offset += contigBytes;
        length -= contigBytes;
    }

    return initialLength - length;
}

/**
 * Append the entire contents of the segment to the provided buffer. This is
 * typically used when transferring a segment over the network.
 */
uint32_t
Segment::appendToBuffer(Buffer& buffer)
{
    // Tail does not include the footer.
    uint32_t length = tail + bytesNeeded(sizeof32(Footer));
    return appendToBuffer(buffer, 0, length);
}

/**
 * Get access to an entry stored in this segment after it has been appended.
 *
 * \param offset
 *      Offset of the entry in the segment. This value is typically the result
 *      of an append call on this segment.
 * \param buffer
 *      Buffer to append the entry to.
 * \return
 *      The entry's type as specified when it was appended (LogEntryType).
 */
LogEntryType
Segment::getEntry(uint32_t offset, Buffer& buffer)
{
    LogEntryType type;
    uint32_t entryDataOffset, entryDataLength;
    getEntryInfo(offset, type, entryDataOffset, entryDataLength);
    appendToBuffer(buffer, entryDataOffset, entryDataLength);
    return type;
}

/**
 * Return the total number of bytes appended to the segment, but not including
 * the footer. The complete footer entry, including the appropriate segment
 * metadata, is passed back by value in the 'footerEntry' parameter. A separate
 * copy must be returned since we will overwrite the footer on the next append
 * operation.
 *
 * \param[out] footerEntry
 *      The footer entry will be copied out here.
 * \return
 *      The total number of bytes appended to the segment, not including the
 *      footer.
 */
uint32_t
Segment::getAppendedLength(OpaqueFooterEntry& footerEntry) const
{
    footerEntry = currentFooter;
    return tail;
}

/**
 * Return the number of seglets allocated to this segment.
 */
uint32_t
Segment::getSegletsAllocated()
{
    return downCast<uint32_t>(seglets.size());
}

/**
 * Return the number of seglets needed by this segment to store the live data
 * it contains. 
 */
uint32_t
Segment::getSegletsNeeded()
{
    uint32_t liveBytes = tail - bytesFreed + bytesNeeded(sizeof(Footer));
    return (liveBytes + allocator.getSegletSize() - 1) /
        allocator.getSegletSize();
}

/**
 * Check the integrity of the segment's metadata by iterating over all entries
 * and ensuring that:
 *
 *  1) All entry lengths are within bounds.
 *  2) A footer is present.
 *  3) The metadata observed matches the checksum in the footer.
 *
 * If the check passes, this segment may be safely iterated over in the most
 * trivial way. Further, with high probability the metadata is correct and the
 * appropriate data will be observed.
 *
 * Segments are not responsible for the integrity of the data they store, so
 * appended data that anyone cares about should include their own checksums.
 *
 * \return
 *      True if the integrity check passes, otherwise false.
 */
bool
Segment::checkMetadataIntegrity()
{
    uint32_t offset = 0;
    Crc32C currentChecksum;

    const EntryHeader* header = NULL;
    const void* unused = NULL;
    while (peek(offset, &unused) > 0) {
        header = getEntryHeader(offset);
        currentChecksum.update(header, sizeof(*header));

        uint32_t length = 0;
        copyOut(offset + sizeof32(*header), &length, header->getLengthBytes());
        currentChecksum.update(&length, header->getLengthBytes());

        if (header->getType() == LOG_ENTRY_TYPE_SEGFOOTER)
            break;

        offset += (sizeof32(*header) + header->getLengthBytes() + length);
    }

    if (header == NULL || header->getType() != LOG_ENTRY_TYPE_SEGFOOTER) {
        LOG(WARNING, "segment corrupt: no footer by offset %u", offset);
        return false;
    }

    Footer footerInSegment(false, Crc32C());
    copyOut(offset + sizeof32(*header) + header->getLengthBytes(),
            &footerInSegment, sizeof32(footerInSegment));

    Footer expectedFooter(footerInSegment.closed, currentChecksum);

    if (footerInSegment.checksum != expectedFooter.checksum) {
        LOG(WARNING, "segment corrupt: bad checksum (expected 0x%08x, "
            "was 0x%08x)", footerInSegment.checksum, expectedFooter.checksum);
        return false;
    }

    return true;
}

/******************************************************************************
 * PRIVATE METHODS
 ******************************************************************************/

/**
 * Write an update-to-date footer to the end of this segment. The footer
 * serves to denote the end of the segment, indicate whether or not it is
 * closed, and contains a checksum to verify integrity.
 */
void
Segment::appendFooter()
{
    // Appending a footer doesn't alter the checksum or the tail pointer
    // cached in this object, since we normally overwrite footers with
    // each append.
    Crc32C tempChecksum = checksum;
    uint32_t tempTail = tail;

    uint32_t length = sizeof32(Footer);
    EntryHeader entryHeader(LOG_ENTRY_TYPE_SEGFOOTER, length);
    tempChecksum.update(&entryHeader, sizeof(entryHeader));
    tempChecksum.update(&length, entryHeader.getLengthBytes());
    copyIn(tempTail, &entryHeader, sizeof(entryHeader));
    tempTail += sizeof32(entryHeader);
    copyIn(tempTail, &length, entryHeader.getLengthBytes());
    tempTail += entryHeader.getLengthBytes();

    Footer footer(closed, tempChecksum);
    copyIn(tempTail, &footer, sizeof(footer));

    // Temporary hack. Look away, please.
    copyOut(tail, &currentFooter, sizeof(currentFooter));
}

/**
 * Return a pointer to an EntryHeader structure within the segment at the given
 * offset. Since that structure is only one byte long, we need not worry about
 * it being spread across discontiguous seglets.
 *
 * \param offset
 *      Offset of the desired entry header. This is typically a value that was
 *      returned via an append call.
 * \return
 *      Pointer to the desired entry header, or NULL if the offset was invalid.
 */
const Segment::EntryHeader*
Segment::getEntryHeader(uint32_t offset)
{
    static_assert(sizeof(EntryHeader) == 1,
                  "Contiguity in segments not guaranteed!");
    const EntryHeader* header;
    peek(offset, reinterpret_cast<const void**>(&header));
    return header;
}

/**
 * Given the offset of an entry in the segment, return the length of that
 * entry's data blob.
 *
 * \param offset
 *      Offset of the entry in the segment. This should point to the entry
 *      header structure. Normally this value is obtained as the result of
 *      an append call.
 * \param outType
 *      The type of the queried entry is returned in this out parameter.
 * \param outDataOffset
 *      The segment byte offset at which the queried entry's data begins is
 *      returned in this out parameter.
 * \param outDataLength
 *      The length of the queried entry (not including metadata), is returned
 *      in this out parameter.
 */
void
Segment::getEntryInfo(uint32_t offset,
                      LogEntryType& outType,
                      uint32_t& outDataOffset,
                      uint32_t& outDataLength)
{
    const EntryHeader* header = getEntryHeader(offset);
    outType = header->getType();
    outDataOffset = offset + sizeof32(*header) + header->getLengthBytes();

    outDataLength = 0;
    copyOut(offset + sizeof32(*header), &outDataLength,
        header->getLengthBytes());
}

/**
 * 'Peek' into the segment by specifying a logical byte offset and getting
 * back a pointer to some contiguous space underlying the start and the number
 * of contiguous bytes at that location. In other words, resolve the offset
 * to a pointer and learn how far from the end of the seglet that offset is.
 *
 * \param offset
 *      Logical segment offset to being peeking into.
 * \param[out] outAddress
 *      Pointer to contiguous memory corresponding to the given offset.
 * \return
 *      The number of contiguous bytes accessible from the returned pointer
 *      (outAddress). 
 */
uint32_t
Segment::peek(uint32_t offset, const void** outAddress) const
{
    uint32_t segletSize = allocator.getSegletSize();

    if (offset >= (segletSize * seglets.size()))
        return 0;

    uint32_t segletOffset = offset % segletSize;
    uint32_t contiguousBytes = segletSize - segletOffset;

    uint32_t segletIndex = offset / segletSize;
    uint8_t* segletPtr = reinterpret_cast<uint8_t*>(seglets[segletIndex]);
    assert(segletPtr != NULL);
    *outAddress = static_cast<void*>(segletPtr + segletOffset);

    return contiguousBytes;
}

/**
 * Return the number of bytes left in the segment for appends. This method
 * returns the total raw number of bytes left and does not subtract any
 * space that should be reserved for footers or other metadata.
 */
uint32_t
Segment::bytesLeft()
{
    if (closed)
        return 0;

    // TODO(Steve): Remove the footer entry reservation after we start passing
    // the footer always on the side (and rename it something better, like
    // "certificate", perhaps).
    uint32_t capacity = getSegletsAllocated() * allocator.getSegletSize();
    return capacity - tail - sizeof32(OpaqueFooterEntry);
}

/**
 * Return the number of segment bytes needed to append an entry with a blob of
 * the given length. This method takes into account the metadata needed to
 * store the entry.
 *
 * \param length
 *      Length of the proposed entry's data blob.
 * \return
 *      The actual number of bytes needed to store an entry of the specified
 *      length. 
 */
uint32_t
Segment::bytesNeeded(uint32_t length)
{
    EntryHeader header(LOG_ENTRY_TYPE_INVALID, length);
    return sizeof32(EntryHeader) + header.getLengthBytes() + length;
}

/**
 * Copy data out of the segment and into a contiguous output buffer.
 *
 * \param offset
 *      Offset within the segment to begin copying from.
 * \param buffer
 *      Pointer to the buffer to copy data to.
 * \param length
 *      Number of bytes to copy out of the segment.
 * \return
 *      The actual number of bytes copied. May be less than requested if the end
 *      of the segment is reached.
 */
uint32_t
Segment::copyOut(uint32_t offset, void* buffer, uint32_t length) const
{
    uint32_t initialLength = length;
    uint8_t* bufferBytes = static_cast<uint8_t*>(buffer);

    while (length > 0) {
        const void* contigPointer = NULL;
        uint32_t contigBytes = std::min(length, peek(offset, &contigPointer));
        if (contigBytes == 0)
            break;

        // Yes, this ugliness actually provides a small improvement...
        switch (contigBytes) {
        case sizeof(uint8_t):
            *bufferBytes = *reinterpret_cast<const uint8_t*>(contigPointer);
            break;
        case sizeof(uint16_t):
            *reinterpret_cast<uint16_t*>(bufferBytes) =
                *reinterpret_cast<const uint16_t*>(contigPointer);
            break;
        case sizeof(uint32_t):
            *reinterpret_cast<uint32_t*>(bufferBytes) =
                 *reinterpret_cast<const uint32_t*>(contigPointer);
            break;
        case sizeof(uint64_t):
            *reinterpret_cast<uint64_t*>(bufferBytes) =
                 *reinterpret_cast<const uint64_t*>(contigPointer);
            break;
        default:
            memcpy(bufferBytes, contigPointer, contigBytes);
        }

        bufferBytes += contigBytes;
        offset += contigBytes;
        length -= contigBytes;
    }

    return initialLength - length;
}

/**
 * Copy a contiguous buffer into the segment at the specified offset.
 *
 * \param offset
 *      Offset in the segment to begin writing the buffer to.
 * \param buffer
 *      Pointer to a buffer that will be written to the segment.
 * \param length
 *      Number of bytes in the buffer to write into the segment.
 * \return
 *     The actual number of bytes copied. May be less than requested if the end
 *     of the segment is reached.
 */
uint32_t
Segment::copyIn(uint32_t offset, const void* buffer, uint32_t length)
{
    uint32_t initialLength = length;
    const uint8_t* bufferBytes = static_cast<const uint8_t*>(buffer);

    while (length > 0) {
        const void* contigPointer = NULL;
        uint32_t contigBytes = std::min(length, peek(offset, &contigPointer));
        if (contigBytes == 0)
            break;

        memcpy(const_cast<void*>(contigPointer), bufferBytes, contigBytes);
        bufferBytes += contigBytes;
        offset += contigBytes;
        length -= contigBytes;
    }

    return initialLength - length;
}

/**
 * Copy contents into the segment from a given buffer.
 *
 * \param segmentOffset
 *      Offset within the segment to begin copying to.
 * \param buffer
 *      Buffer to copy from.
 * \param bufferOffset
 *      Offset in the buffer to begin copying from.
 * \param length
 *      Number of bytes to copy from the buffer.
 * \return
 *      The actual number of bytes copied. May be less than requested if the end
 *      of the segment is reached.
 */
uint32_t
Segment::copyInFromBuffer(uint32_t segmentOffset,
                          Buffer& buffer,
                          uint32_t bufferOffset,
                          uint32_t length)
{
    uint32_t bytesCopied = 0;
    Buffer::Iterator it(buffer, bufferOffset, length);
    while (!it.isDone()) {
        uint32_t bytes = copyIn(segmentOffset, it.getData(), it.getLength());

        bytesCopied += bytes;
        if (bytes != it.getLength())
            break;

        segmentOffset += it.getLength();
        it.next();
    }

    return bytesCopied;
}

} // namespace
