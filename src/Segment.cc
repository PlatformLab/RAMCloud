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
      checksum()
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
      checksum()
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
      checksum()
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
Segment::append(LogEntryType type, const void* data, uint32_t length, uint32_t& outOffset)
{
    Buffer buffer;
    Buffer::Chunk::appendToBuffer(&buffer, data, length);
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
    copyOut(offset, &length, header->getLengthBytes());

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
 * Append a specific entry in the segment to the provided buffer. This is the
 * primary means of accessing an entry after it has been appended.
 *
 * \param offset
 *      Offset of the entry in the segment. This value is typically the result
 *      of an append call on this segment.
 * \param buffer
 *      Buffer to append the entry to.
 * \return
 *      The number of bytes appended to the buffer is returned. In other words,
 *      the length of the entry.
 */
uint32_t
Segment::appendEntryToBuffer(uint32_t offset, Buffer& buffer)
{
    uint32_t entryDataOffset = getEntryDataOffset(offset);
    uint32_t entryDataLength = getEntryDataLength(offset);
    return appendToBuffer(buffer, entryDataOffset, entryDataLength);
}

/**
 * Return the type of entry present at a given offset in the segment.
 *
 * \param offset
 *      Offset of the entry in the segment. This value is typically the result
 *      of an append call on this segment.
 * \return
 *      The LogEntryType enum corresponding to the requested entry.
 */
LogEntryType
Segment::getEntryTypeAt(uint32_t offset)
{
    const EntryHeader* header = getEntryHeader(offset);
    return header->getType();
}

/**
 * Return the length of the entry present at a given offset in the segment.
 *
 * \param offset
 *      Offset of the entry in the segment. This value is typically the result
 *      of an append call on this segment.
 * \return
 *      The length of the entry in bytes.
 */
uint32_t
Segment::getEntryLengthAt(uint32_t offset)
{
    return getEntryDataLength(offset);
}

/**
 * Return the offset of the tail of the segment. This the offset at which the
 * next entry will be written (assuming there's enough space).
 */
uint32_t
Segment::getTailOffset()
{
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
    return (liveBytes + allocator.getSegletSize() - 1) / allocator.getSegletSize();
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
    while (getContiguousBytesAt(offset) > 0) {
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
 * Append contents of the segment to a provided buffer.
 *
 * \param buffer
 *      Buffer to append segment contents to.
 * \param offset
 *      Offset in the segment to begin appending from.
 * \param
 *      Number of bytes in the segmet to append, starting from the offset.
 * \return
 *      The number of actual bytes appended to the buffer. If this was less
 *      than expected, the offset or length parameter was invalid.
 */
uint32_t
Segment::appendToBuffer(Buffer& buffer, uint32_t offset, uint32_t length)
{
    uint32_t initialLength = length;

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

    return initialLength - length;
}

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
    static_assert(sizeof(Segment::EntryHeader) == 1,
                  "Contiguity in segments not guaranteed!");
    return reinterpret_cast<const Segment::EntryHeader*>(getAddressAt(offset));
}

/**
 * Given the offset of an entry in the segment, return the offset of that
 * entry's data blob.
 *
 * \param offset
 *      Offset of the entry in the segment. This should point to the entry
 *      header structure. Normally this value is obtained as the result of
 *      an append call.
 * \return
 *      Offset of the specified entry's data blob in the segment.
 */
uint32_t
Segment::getEntryDataOffset(uint32_t offset)
{
    const EntryHeader* header = getEntryHeader(offset);
    return offset + sizeof32(*header) + header->getLengthBytes();
}

/**
 * Given the offset of an entry in the segment, return the length of that
 * entry's data blob.
 *
 * \param offset
 *      Offset of the entry in the segment. This should point to the entry
 *      header structure. Normally this value is obtained as the result of
 *      an append call.
 * \return
 *      Length of the specified entry's data blob in the segment.
 */
uint32_t
Segment::getEntryDataLength(uint32_t offset)
{
    const EntryHeader* header = getEntryHeader(offset);
    uint32_t dataLength = 0;
    copyOut(offset + sizeof32(*header), &dataLength, header->getLengthBytes());
    return dataLength;
}

/**
 * Given a logical offset into the segment, obtain a pointer to the
 * corresponding memory. To obtain the number of contiguous bytes at that
 * location, call getContiguousBytes on the same offset.
 *
 * \param offset
 *      Logical offset in the segment.
 * \return
 *      NULL if offset is invalid, else a pointer to the memory at that offset.
 */
void*
Segment::getAddressAt(uint32_t offset)
{
    if (getContiguousBytesAt(offset) == 0)
        return NULL;

    uint32_t segletOffset = offset % allocator.getSegletSize();
    uint8_t* segletPtr = reinterpret_cast<uint8_t*>(offsetToSeglet(offset));
    assert(segletPtr != NULL);
    return static_cast<void*>(segletPtr + segletOffset);
}

/**
 * Given a logical offset into the segment, obtain the number of contiguous
 * bytes that back the segment starting from that position. This is typically
 * used in conjunction with getAddressAt.
 *
 * \param offset
 *      Logical offset in the segment.
 * \return
 *      0 if offset is invalid, else the number of contiguous bytes mapped at
 *      that logical offset.
 */
uint32_t
Segment::getContiguousBytesAt(uint32_t offset)
{
    if (offset >= (allocator.getSegletSize() * seglets.size()))
        return 0;

    uint32_t segletOffset = offset % allocator.getSegletSize();
    return allocator.getSegletSize() - segletOffset;
}

/**
 * Given a logical offset in a segment, return the first byte of the seglet
 * that is mapped to that location.
 *
 * \param offset
 *      Logical offset in the segment.
 * \return
 *      NULL if offset is invalid, else the a pointer to the beginning of the
 *      requested seglet.
 */
void*
Segment::offsetToSeglet(uint32_t offset)
{
    uint32_t index = offset / allocator.getSegletSize();
    if (index >= seglets.size())
        return NULL;
    return seglets[index];
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

    uint32_t capacity = getSegletsAllocated() * allocator.getSegletSize();
    return capacity - tail;
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
Segment::copyOut(uint32_t offset, void* buffer, uint32_t length)
{
    uint32_t initialLength = length;
    uint8_t* bufferBytes = static_cast<uint8_t*>(buffer);

    while (length > 0) {
        uint32_t contigBytes = std::min(length, getContiguousBytesAt(offset));
        if (contigBytes == 0)
            break;

        memcpy(bufferBytes, getAddressAt(offset), contigBytes);
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
        uint32_t contigBytes = std::min(length, getContiguousBytesAt(offset));
        if (contigBytes == 0)
            break;

        memcpy(getAddressAt(offset), bufferBytes, contigBytes);
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
