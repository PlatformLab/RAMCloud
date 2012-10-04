/* Copyright (c) 2009-2012 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "Common.h"
#include "BitOps.h"
#include "Crc32C.h"
#include "Segment.h"
#include "ShortMacros.h"
#include "LogEntryTypes.h"

namespace RAMCloud {

/**
 * Construct a segment using Segment::DEFAULT_SEGMENT_SIZE bytes dynamically
 * allocated on the heap. This constructor is useful, for instance, when a
 * temporary segment is needed to move data between servers.
 */
Segment::Segment()
    : segletSize(DEFAULT_SEGMENT_SIZE),
      segletSizeShift(0),
      seglets(),
      segletBlocks(),
      immutable(false),
      closed(false),
      mustFreeBlocks(true),
      head(0),
      checksum()
{
    segletBlocks.push_back(new uint8_t[segletSize]);
}

/**
 * Construct a segment using the provided seglets of the specified size.
 */
Segment::Segment(vector<Seglet*>& seglets, uint32_t segletSize)
    : segletSize(segletSize),
      segletSizeShift(BitOps::findFirstSet(segletSize)),
      seglets(seglets),
      segletBlocks(),
      immutable(false),
      closed(false),
      mustFreeBlocks(false),
      head(0),
      checksum()
{
    assert(BitOps::isPowerOfTwo(segletSize));
    foreach (Seglet* seglet, seglets) {
        segletBlocks.push_back(seglet->get());
        assert(seglet->getLength() == segletSize);
    }
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
    : segletSize(length),
      segletSizeShift(0),
      seglets(),
      segletBlocks(),
      immutable(false),
      closed(true),
      mustFreeBlocks(false),
      head(length),
      checksum()
{
    // We promise not to scribble on it, honest!
    segletBlocks.push_back(const_cast<void*>(buffer));
}

/**
 * Destroy the segment, freeing any Seglets that were allocated.
 */
Segment::~Segment()
{
    // Check if the 0-argument constructor dynamically allocated space we need
    // to free.
    if (mustFreeBlocks) {
        foreach(void* block, segletBlocks)
            delete[] reinterpret_cast<uint8_t*>(block);
    }

    foreach (Seglet* seglet, seglets)
        seglet->free();
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

    // Check if sufficient space to store this.
    if (bytesLeft() < bytesNeeded(length))
        return false;

    uint32_t startOffset = head;

    copyIn(head, &entryHeader, sizeof(entryHeader));
    checksum.update(&entryHeader, sizeof(entryHeader));
    head += sizeof32(entryHeader);

    copyIn(head, &length, entryHeader.getLengthBytes());
    checksum.update(&length, entryHeader.getLengthBytes());
    head += entryHeader.getLengthBytes();

    copyInFromBuffer(head, buffer, offset, length);
    head += length;

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
    buffer.append(data, length);
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
 * Close the segment, making permanently immutable. Closing it will cause all
 * future append operations to fail.
 */
void
Segment::close()
{
    if (!closed) {
        immutable = true;
        closed = true;
    }
}

/**
 * Mark the segment as immutable, disabling any future appends until it is made
 * mutable again by calling enableAppends(). This is used to ensure that open
 * emergency head segments are not appended to by the log. See SegmentManager
 * for more details.
 *
 * Note that segments are mutable by default after construction. 
 */
void
Segment::disableAppends()
{
    immutable = true;
}

/**
 * Mark the segment as mutable again after a call to disableAppends(). If the
 * segment is closed, it can never be made mutable again and this call will
 * fail and return false. Otherwise returns true if the segment is mutable.
 *
 * Segments are already mutable after construction.
 */
bool
Segment::enableAppends()
{
    if (closed)
        return false;
    immutable = false;
    return true;
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

        buffer.append(contigPointer, contigBytes);

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
    return appendToBuffer(buffer, 0, head);
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
 * the footer. Calling this method before and after an append will indicate
 * exactly how many bytes were consumed in storing the appended entry, including
 * metadata.
 */
uint32_t
Segment::getAppendedLength() const
{
    return head;
}

/**
 * Return the total number of bytes appended to the segment.
 * A Certificate which can be used to validate the integrity of the segment's
 * metadata is passed back by value in the 'certificate' parameter. A separate
 * copy must be returned since the certificate will change on the next append
 * operation.
 *
 * \param[out] certificate
 *      The certificate entry will be copied out here.
 * \return
 *      The total number of bytes appended to the segment.
 */
uint32_t
Segment::getAppendedLength(Certificate& certificate) const
{
    certificate.segmentLength = head;
    Crc32C certificateChecksum = checksum;
    certificateChecksum.update(
        &certificate, sizeof(certificate) - sizeof(certificate.checksum));
    certificate.checksum = certificateChecksum.getResult();
    return head;
}

/**
 * Return the number of seglets allocated to this segment.
 */
uint32_t
Segment::getSegletsAllocated()
{
    // We use 'segletBlocks', rather than 'seglets', because not all segments
    // are constructed using Seglet objects. Some just wrap unmanaged buffers.
    return downCast<uint32_t>(segletBlocks.size());
}

/**
 * Return the number of seglets this segment is currently using due to prior
 * append operations.
 */
uint32_t
Segment::getSegletsInUse()
{
    return (head + segletSize - 1) / segletSize;
}

/**
 * Free the given number of unused seglets from the end of a closed segment.
 *
 * \return
 *      True if the operation succeeded. False if no action was taken because
 *      the segment is not closed or the given count exceeds the number of
 *      unused seglets.
 */
bool
Segment::freeUnusedSeglets(uint32_t count)
{
    // If we're closed or don't have any seglets allocated (either because
    // they've all been freed or we started with a static or heap allocation
    // not backed by Seglet classes), there's nothing to be done.
    if (!closed || seglets.size() == 0)
        return false;

    size_t unusedSeglets = seglets.size() - getSegletsInUse();
    if (count > unusedSeglets)
        return false;

    for (uint32_t i = 0; i < count; i++) {
        assert(seglets.back()->get() == segletBlocks.back());
        seglets.back()->free();
        seglets.pop_back();
        segletBlocks.pop_back();
    }

    return true;
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
 * \param certificate
 *      A Certificate which is used to check the integrity of the metadata
 *      of this segment. Certificates are generated by getAppendedLength().
 * \return
 *      True if the integrity check passes, otherwise false.
 */
bool
Segment::checkMetadataIntegrity(const Certificate& certificate)
{
    uint32_t offset = 0;
    Crc32C currentChecksum;

    const EntryHeader* header = NULL;
    const void* unused = NULL;
    while (offset < certificate.segmentLength && peek(offset, &unused) > 0) {
        header = getEntryHeader(offset);
        currentChecksum.update(header, sizeof(*header));

        uint32_t length = 0;
        copyOut(offset + sizeof32(*header), &length, header->getLengthBytes());
        currentChecksum.update(&length, header->getLengthBytes());

        offset += (sizeof32(*header) + header->getLengthBytes() + length);
        size_t segmentSize = segletBlocks.size() * segletSize;
        if (offset > segmentSize) {
            LOG(WARNING, "segment corrupt: entries run off past "
                "allocated segment size (segment size %lu, next entry would "
                "have started at %u)",
                segmentSize, offset);
            return false;
        }
    }
    if (offset > certificate.segmentLength) {
        LOG(WARNING, "segment corrupt: entries run off past expected "
            "length (expected %u, next entry would have started at %u)",
            certificate.segmentLength, offset);
        return false;
    }

    currentChecksum.update(&certificate,
                           sizeof(certificate) - sizeof(certificate.checksum));

    if (certificate.checksum != currentChecksum.getResult()) {
        LOG(WARNING, "segment corrupt: bad checksum (expected 0x%08x, "
            "was 0x%08x)", certificate.checksum, currentChecksum.getResult());
        return false;
    }

    return true;
}

/******************************************************************************
 * PRIVATE METHODS
 ******************************************************************************/

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
    if (__builtin_expect(offset >= (segletSize * segletBlocks.size()), false))
        return 0;

    uint32_t segletOffset = offset;
    uint32_t segletIndex = 0;

    // If we have more than one seglet, then they must all be the same size and
    // a power of two, so use bit ops rather than division and modulo to save
    // time. This method can be hot enough that this makes a big difference.
    if (__builtin_expect(segletSizeShift != 0, true)) {
        segletOffset = offset & (segletSize - 1);
        segletIndex = offset >> segletSizeShift;
    }

    uint8_t* segletPtr = reinterpret_cast<uint8_t*>(segletBlocks[segletIndex]);
    assert(segletPtr != NULL);
    *outAddress = static_cast<void*>(segletPtr + segletOffset);

    return segletSize - segletOffset;
}

/**
 * Return the number of bytes left in the segment for appends. This method
 * returns the total raw number of bytes left and does not subtract any
 * space that should be reserved for other metadata.
 */
uint32_t
Segment::bytesLeft()
{
    if (immutable || closed)
        return 0;

    uint32_t capacity = getSegletsAllocated() * segletSize;
    return capacity - head;
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
