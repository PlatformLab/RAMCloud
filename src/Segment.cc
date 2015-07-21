/* Copyright (c) 2009-2015 Stanford University
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
#include "CycleCounter.h"
#include "Segment.h"
#include "LogSegment.h"
#include "ShortMacros.h"
#include "LogEntryTypes.h"
#include "TestLog.h"

namespace RAMCloud {

static bool
segletLessThan(const Seglet* a, const Seglet* b)
{
    return a->get() < b->get();
}

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
Segment::Segment(const vector<Seglet*>& allocatedSeglets, uint32_t segletSize)
    : segletSize(segletSize),
      segletSizeShift(BitOps::findFirstSet(segletSize) - 1),
      seglets(allocatedSeglets),
      segletBlocks(),
      closed(false),
      mustFreeBlocks(false),
      head(0),
      checksum()
{
    assert(BitOps::isPowerOfTwo(segletSize));

    // Sort our seglets by increasing address. This allows us to binary
    // search for the seglet that a pointer into this segment begins in.
    std::sort(seglets.begin(), seglets.end(), segletLessThan);

    foreach (Seglet* seglet, seglets) {
        assert(seglet->getLength() == segletSize);
        segletBlocks.push_back(seglet->get());
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
 * Check whether or not the segment has sufficient space to append one or more
 * entries.
 *
 * \param entryLengths
 *      An array containing lengths of entries.
 * \param numEntries
 *      The number of lengths in the entryLengths array.
 * \return
 *      True if the segment has enough space to fit all of the entries,
 *      otherwise false.
 */
bool
Segment::hasSpaceFor(uint32_t* entryLengths, uint32_t numEntries)
{
    uint32_t totalBytesNeeded = 0;

    for (uint32_t i = 0; i < numEntries; i++) {
        EntryHeader header(LOG_ENTRY_TYPE_INVALID, entryLengths[i]);
        totalBytesNeeded += sizeof32(EntryHeader) +
                            header.getLengthBytes() +
                            entryLengths[i];
    }

    uint32_t bytesLeft = 0;
    if (!closed) {
        uint32_t capacity = getSegletsAllocated() * segletSize;
        bytesLeft = capacity - head;
    }

    return totalBytesNeeded <= bytesLeft;
}

/**
 * Check whether or not the segment has sufficient space to append a
 * given number of bytes. This method is used when the caller already
 * has all the bytes required for one or many log entries.
 *
 * \param length
 *      Number of bytes that need to be appended to this segment
 * \return
 *      True if the segment has enough space to fit 'length' number of bytes,,
 *      false otherwise.
 */
bool
Segment::hasSpaceFor(uint32_t length)
{
    uint32_t bytesLeft = 0;
    if (!closed) {
        uint32_t capacity = getSegletsAllocated() * segletSize;
        bytesLeft = capacity - head;
    }

    return length <= bytesLeft;
}

/**
 * Append a typed entry to this segment. Entries are binary blobs. The segment
 * records metadata identifying their type and length.
 *
 * \param type
 *      Type of the entry. See LogEntryTypes.h.
 * \param buffer
 *      Pointer to the buffer containing the entry to be appended.
 * \param length
 *      Number of bytes to append from the provided buffer.
 * \param[out] outReference
 *      If the append was successful, a Segment::Reference pointing to the new
 *      entry is returned here. This is used to later access the entry within
 *      segment (see getEntry).
 * \return
 *      True if the append succeeded, false if there was insufficient space to
 *      complete the operation.
 */
bool
Segment::append(LogEntryType type,
                const void* buffer,
                uint32_t length,
                Reference* outReference)
{
    EntryHeader entryHeader(type, length);

    if (!hasSpaceFor(&length, 1))
        return false;

    uint32_t startOffset = head;

    copyIn(head, &entryHeader, sizeof(entryHeader));
    checksum.update(&entryHeader, sizeof(entryHeader));
    head += sizeof32(entryHeader);

    // Note that this assumes a little-endian byte order. I think this is
    // justified considering how widely we have assume byte order (if not
    // x86 in particular).
    copyIn(head, &length, entryHeader.getLengthBytes());
    checksum.update(&length, entryHeader.getLengthBytes());
    head += entryHeader.getLengthBytes();

    copyIn(head, buffer, length);
    head += length;

    if (outReference != NULL)
        *outReference = Reference(this, startOffset);

    return true;
}

/**
 * Append a typed entry to this segment. Entries are binary blobs. The segment
 * records metadata identifying their type and length.
 *
 * \param type
 *      Type of the entry. See LogEntryTypes.h.
 * \param buffer
 *      Buffer object describing the entry to be appended.
 * \param[out] outReference
 *      If the append was successful, a Segment::Reference pointing to the new
 *      entry is returned here. This is used to later access the entry within
 *      segment (see getEntry).
 * \return
 *      True if the append succeeded, false if there was insufficient space to
 *      complete the operation.
 */
bool
Segment::append(LogEntryType type,
                Buffer& buffer,
                Reference* outReference)
{
    uint32_t length = buffer.size();
    return append(type, buffer.getRange(0, length), length, outReference);
}

/**
 * Append a complete log entry to this segment. This function is called
 * when the log entry header information is already part of the
 * function input. Entries are binary blobs. The segment
 * records metadata identifying their type and length.
 *
 * \param buffer
 *      Pointer to the buffer containing the log entry to be appended.
 *      This points to the start of the entry header
 * \param[out] entryDataLength
 *      Number of bytes that belong to the current entry's contents.
 *      This does not include the log entry header information
 * \param[out] type
 *      Type of the entry. See LogEntryTypes.h.
 * \param[out] outReference
 *      If the append was successful, a Segment::Reference pointing to the new
 *      entry is returned here. This is used to later access the entry within
 *      segment (see getEntry).
 * \return
 *      True if the append succeeded, false if there was insufficient space to
 *      complete the operation.
 */
bool
Segment::append(const void* buffer,
                uint32_t* entryDataLength,
                LogEntryType *type,
                Reference* outReference)
{
    const EntryHeader* entryHeader = reinterpret_cast<
                                     const EntryHeader*>(buffer);

    uint32_t lengthWithoutMetadata = 0, lengthWithMetadata = 0;
    LogEntryType entryType = getEntry(buffer, &lengthWithoutMetadata,
                                      &lengthWithMetadata);

    if (!hasSpaceFor(lengthWithMetadata))
        return false;

    uint32_t startOffset = head;

    copyIn(head, entryHeader, sizeof(*entryHeader));
    checksum.update(entryHeader, sizeof(*entryHeader));
    head += sizeof32(*entryHeader);

    // Note that this assumes a little-endian byte order. I think this is
    // justified considering how widely we have assume byte order (if not
    // x86 in particular).
    copyIn(head, &lengthWithoutMetadata, entryHeader->getLengthBytes());
    checksum.update(&lengthWithoutMetadata, entryHeader->getLengthBytes());
    head += entryHeader->getLengthBytes();

    const uint8_t* contigPointer = reinterpret_cast<const uint8_t*>(buffer);
    const uint8_t* entryContents = contigPointer + head - startOffset;
    copyIn(head, entryContents, lengthWithoutMetadata);
    head += lengthWithoutMetadata;

    if (entryDataLength)
        *entryDataLength = lengthWithoutMetadata;

    if (type)
        *type = entryType;

    if (outReference != NULL)
        *outReference = Reference(this, startOffset);

    return true;
}

/**
 * Adds a log entry header to a buffer. The size of the header is
 * determined by the object size for which this header is to be
 * constructed.
 *
 * \param type
 *      Type of the entry. See LogEntryTypes.h.
 * \param objectSize
 *      Size of the object for which the entry header is to be
 *      constructed
 * \param[out] logBuffer
 *      The buffer to which the log entry header will be appended
 */
void
Segment::appendLogHeader(LogEntryType type,
                         uint32_t objectSize,
                         Buffer *logBuffer)
{
    EntryHeader entryHeader(type, objectSize);

    // Allocate memory in the buffer for the entry header and the number
    // of length bytes required
    EntryHeader *header = reinterpret_cast<EntryHeader *>(
                            logBuffer->alloc(sizeof(EntryHeader) +
                            entryHeader.getLengthBytes()));
    *header = entryHeader;
    memcpy(header + 1, &objectSize, entryHeader.getLengthBytes());
}

/**
 * Close the segment, making it permanently immutable. Closing it will cause all
 * future append operations to fail.
 *
 * Note that this is only soft state. Neither the contents of the segment, nor
 * the certificate indicate closure. Backups have their own notion of closed
 * segments, which is propagated by the ReplicatedSegment class.
 */
void
Segment::close()
{
    closed = true;
}

uint64_t bufferAppendTicks;
uint64_t bufferAppendSizes;
uint64_t bufferAppendCount;

/**
 * Append contents of the segment to a provided buffer.
 *
 * \param buffer
 *      Buffer to append segment contents to.
 * \param offset
 *      Offset in the segment to begin appending from.
 * \param length
 *      Number of bytes in the segment to append, starting from the offset.
 *      This value must not exceed the current end of the segment.
 * \throw FatalError
 *      A FatalError is thrown if #length bytes cannot be appended from #offset,
 *      due to either or both of the parameters being invalid.
 */
void
Segment::appendToBuffer(Buffer& buffer, uint32_t offset, uint32_t length) const
{
    while (length > 0) {
        const void* contigPointer = NULL;
        uint32_t contigBytes = std::min(length, peek(offset, &contigPointer));
        if (contigBytes == 0)
            break;

        buffer.append(contigPointer, contigBytes);

        offset += contigBytes;
        length -= contigBytes;
    }

    if (length != 0) {
        throw FatalError(HERE, format("invalid length (%u) and/or offset (%u) "
            "parameter(s)", length, offset));
    }
}

/**
 * Append the entire contents of the segment to the provided buffer. This is
 * typically used when transferring a segment over the network.
 *
 * \param buffer
 *      The buffer to append the entire segment's contents to.
 * \return
 *      The number of bytes appended to the buffer are returned (in other words,
 *      the total length of the segment).
 */
uint32_t
Segment::appendToBuffer(Buffer& buffer)
{
    appendToBuffer(buffer, 0, head);
    return head;
}

/**
 * Get access to an entry stored in this segment after it has been appended by
 * specifying the logical offset of the entry in the Segment. This method is
 * primarily used as a helper function when looking up entries by their
 * Segment::Reference.
 *
 * \param offset
 *      Offset of the entry in the segment. This value must be the result of a
 *      previous append call on this segment. Behaviour is undefined when using
 *      any other values.
 * \param buffer
 *      Buffer to append the entry to.
 * \param lengthWithMetadata
 *      If non-NULL, return the total number of bytes this entry uses in the
 *      segment here, including any internal segment metadata. This is used by
 *      LogSegment to keep track of the exact amount of live data within a
 *      segment.
 * \return
 *      The entry's type as specified when it was appended (LogEntryType).
 */
LogEntryType
Segment::getEntry(uint32_t offset, Buffer* buffer, uint32_t* lengthWithMetadata)
{
    EntryHeader header = getEntryHeader(offset);
    uint32_t entryDataOffset = offset +
                               sizeof32(header) +
                               header.getLengthBytes();

    uint32_t entryDataLength = 0;
    copyOut(offset + sizeof32(header), &entryDataLength,
        header.getLengthBytes());

    if (buffer != NULL)
        appendToBuffer(*buffer, entryDataOffset, entryDataLength);

    if (lengthWithMetadata != NULL) {
        *lengthWithMetadata = entryDataLength +
                              sizeof32(header) +
                              header.getLengthBytes();
    }

    return header.getType();
}

/**
 * Get access to an entry stored in this segment using a Segment::Reference
 * pointing to the entry. This the main method used to access entries that have
 * been appended to a segment.
 *
 * \param reference 
 *      The Segment::Reference object pointing to the desired entry. This should
 *      always have been a result of a previous call to Segment::getReference().
 * \param buffer
 *      Buffer to append the entry to.
 * \param lengthWithMetadata
 *      If non-NULL, return the total number of bytes this entry uses in the
 *      segment here, including any internal segment metadata. This is used by
 *      LogSegment to keep track of the exact amount of live data within a
 *      segment.
 * \return
 *      The entry's type as specified when it was appended (LogEntryType).
 */
LogEntryType
Segment::getEntry(Reference reference,
                  Buffer* buffer,
                  uint32_t* lengthWithMetadata)
{
    // Binary search our seglets to find out which one this entry starts in and
    // compute the offset in the segment.
    void* p = reinterpret_cast<void*>(reference.toInteger());
    vector<void*>::iterator it = std::lower_bound(segletBlocks.begin(),
                                                  segletBlocks.end(), p);

    // If 'p' points to the first byte of a seglet, 'it' will point to the
    // seglet base address that we want. Otherwise, it will point one higher,
    // so we need to adjust.
    if (it > segletBlocks.begin() && p != *(it - 1))
        it--;

    assert(it != segletBlocks.end());
    uint64_t segletOffset = reinterpret_cast<uintptr_t>(p) -
                            reinterpret_cast<uintptr_t>(*it);
    assert(segletOffset < segletSize);
    uint64_t offset = (it - segletBlocks.begin()) << segletSizeShift;
    offset += segletOffset;
    return getEntry(downCast<uint32_t>(offset), buffer, lengthWithMetadata);
}

/**
 * Get access to an entry present at a given memory location.
 * This function is primarily used when flushing a series of
 * well-formed log entries into the log atomically
 *
 * \param buffer
 *      Points to the starting of the entry.
 * \param entryDataLength
 *      If non-NULL, return the total number of bytes this entry uses
 *      excluding any internal segment metadata.
 * \param lengthWithMetadata
 *      If non-NULL, return the total number of bytes this entry uses in the
 *      segment here, including any internal segment metadata.
 * \return
 *      The entry's type as specified when it was appended (LogEntryType).
 */
LogEntryType
Segment::getEntry(const void* buffer, uint32_t* entryDataLength,
                  uint32_t* lengthWithMetadata)
{
    const EntryHeader *entryHeader =
                            reinterpret_cast<const EntryHeader*>(buffer);

    if (entryDataLength) {
        const uint8_t* contigPointer = reinterpret_cast<const uint8_t*>(buffer);
        uint8_t *contentsLengthPtr = reinterpret_cast<uint8_t *>(
                                                        entryDataLength);

        // Yes, this ugliness actually provides a small improvement when
        // pulling out the header length field.
        switch (entryHeader->getLengthBytes()) {
            case sizeof(uint8_t):
                *contentsLengthPtr =
                    *reinterpret_cast<const uint8_t*>(contigPointer +
                    sizeof(*entryHeader));
                break;
            case sizeof(uint16_t):
                *reinterpret_cast<uint16_t*>(contentsLengthPtr) =
                    *reinterpret_cast<const uint16_t*>(contigPointer +
                    sizeof(*entryHeader));
                break;
            default:
                memcpy(contentsLengthPtr,
                       contigPointer + sizeof(*entryHeader),
                       entryHeader->getLengthBytes());
        }

        if (lengthWithMetadata != NULL) {
            *lengthWithMetadata = sizeof32(*entryHeader) +
                                  entryHeader->getLengthBytes() +
                                  *entryDataLength;
        }
    }

    return entryHeader->getType();
}

/**
 * Get access to an entry stored in a buffer at a given offset.
 * This function is primarily when updating the hash table for
 * a series of log entries that were flushed atomically.
 *
 * \param buffer
 *      Buffer containing the entry.
 * \param offset
 *      Starting offset in the buffer for the entry.
 * \param entryDataLength
 *      If non-NULL, return the total number of bytes this entry uses
 *      excluding any internal segment metadata.
 * \param lengthWithMetadata
 *      If non-NULL, return the total number of bytes this entry uses in the
 *      segment here, including any internal segment metadata.
 * \return
 *      The entry's type as specified when it was appended (LogEntryType).
 */
LogEntryType
Segment::getEntry(Buffer* buffer, uint32_t offset,
                  uint32_t* entryDataLength, uint32_t* lengthWithMetadata)
{
    const EntryHeader *entryHeader = buffer->getOffset<EntryHeader>(offset);
    assert(entryHeader != NULL);

    if (entryDataLength) {
        // clear the value before we go about copying specific bytes.
        *entryDataLength = 0;
        uint8_t *contentsLengthPtr = reinterpret_cast<uint8_t *>(
                                                    entryDataLength);

        // Yes, this ugliness actually provides a small improvement when
        // pulling out the header length field.
        switch (entryHeader->getLengthBytes()) {
            case sizeof(uint8_t):
                *contentsLengthPtr =
                    *buffer->getOffset<uint8_t>(offset +
                                                sizeof32(*entryHeader));
                break;
            case sizeof(uint16_t):
                *reinterpret_cast<uint16_t*>(contentsLengthPtr) =
                    *buffer->getOffset<uint16_t>(offset +
                                                 sizeof32(*entryHeader));
                break;
            default:
                memcpy(contentsLengthPtr,
                       buffer->getRange(offset + sizeof32(*entryHeader),
                                        entryHeader->getLengthBytes()),
                       entryHeader->getLengthBytes());
        }

        if (lengthWithMetadata != NULL) {
            *lengthWithMetadata = sizeof32(*entryHeader) +
                                  entryHeader->getLengthBytes() +
                                  *entryDataLength;
        }
    }

    return entryHeader->getType();
}

/**
 * Return the total number of bytes appended to the segment. Calling this method
 * before and after an append will indicate exactly how many bytes were consumed
 * in storing the appended entry, including metadata.
 * 
 * A SegmentCertificate which can be used to validate the integrity of the
 * segment's metadata is optionally passed back by value in the 'certificate'
 * parameter.  A copy must be done since the certificate will change on the
 * next append operation.
 *
 * This method is mostly used by ReplicatedSegment to find our how much data
 * needs to be replicated and to provide backups with a means of verifying the
 * metadata integrity of segments and step their replicated version from one
 * consistent snapshot of the segment to another as more entries are appended.
 *
 * \param[out] certificate
 *      The certificate entry will be copied out here.
 * \return
 *      The total number of bytes appended to the segment.
 */
uint32_t
Segment::getAppendedLength(SegmentCertificate* certificate) const
{
    if (certificate != NULL) {
        certificate->segmentLength = head;
        Crc32C certificateChecksum = checksum;
        certificateChecksum.update(
            certificate, static_cast<unsigned>
            (sizeof(*certificate) - sizeof(certificate->checksum)));
        certificate->checksum = certificateChecksum.getResult();
    }
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
 * append operations. Only full seglets at the end of the segment that have
 * never been appended to can be considered not in use.
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
 * proper entrys will be observed.
 *
 * Segments are not responsible for the integrity of the contents of the entries
 * they store. Entries should include their own internal checksums if this is a
 * concern.
 *
 * \param certificate
 *      A SegmentCertificate which is used to check the integrity of the
 *      metadata of this segment. SegmentCertificates are generated by
 *      getAppendedLength().
 * \return
 *      True if the integrity check passes, otherwise false.
 */
bool
Segment::checkMetadataIntegrity(const SegmentCertificate& certificate)
{
    uint32_t offset = 0;
    Crc32C currentChecksum;

    const void* unused = NULL;
    while (offset < certificate.segmentLength && peek(offset, &unused) > 0) {
        EntryHeader header = getEntryHeader(offset);
        currentChecksum.update(&header, sizeof(header));

        uint32_t length = 0;
        copyOut(offset + sizeof32(header), &length, header.getLengthBytes());
        currentChecksum.update(&length, header.getLengthBytes());

        offset += (sizeof32(header) + header.getLengthBytes() + length);
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

    currentChecksum.update(&certificate, static_cast<unsigned>
                           (sizeof(certificate)-sizeof(certificate.checksum)));

    if (certificate.checksum != currentChecksum.getResult()) {
        LOG(WARNING, "segment corrupt: bad checksum (expected 0x%08x, "
            "was 0x%08x)", certificate.checksum, currentChecksum.getResult());
        return false;
    }

    return true;
}

/**
 * Copy data out of the segment and into a contiguous output buffer.
 *
 * \param offset
 *      Offset within the segment to begin copying from.
 * \param buffer
 *      Pointer to the buffer to copy the data to.
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

        // Yes, this ugliness actually provides a small improvement when
        // pulling out the header length field.
        switch (contigBytes) {
        case sizeof(uint8_t):
            *reinterpret_cast<uint8_t*>(bufferBytes) =
                *reinterpret_cast<const uint8_t*>(contigPointer);
            break;
        case sizeof(uint16_t):
            *reinterpret_cast<uint16_t*>(bufferBytes) =
                *reinterpret_cast<const uint16_t*>(contigPointer);
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

/******************************************************************************
 * PRIVATE METHODS
 ******************************************************************************/

/**
 * Return a copy of the EntryHeader structure within the segment at the given
 * offset. Since that structure is only one byte long, we need not worry about
 * it being spread across discontiguous seglets.
 *
 * \param offset
 *      Offset of the desired entry header. This must be a value that was
 *      returned via an append call. Behaviour is undefined when invalid offsets
 *      are provided.
 * \return
 *      Copy of the desired entry header.
 */
Segment::EntryHeader
Segment::getEntryHeader(uint32_t offset)
{
    static_assert(sizeof(EntryHeader) == 1,
                  "Contiguity in segments not guaranteed!");
    const EntryHeader* header = NULL;
    peek(offset, reinterpret_cast<const void**>(&header));
    return *header;
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
    Buffer::Iterator it(&buffer, bufferOffset, length);
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

Segment::Reference
Segment::getReference(uint32_t offset)
{
    const void* p = NULL;
    peek(offset, &p);
    return Reference(reinterpret_cast<uint64_t>(p));
}

LogEntryType
Segment::Reference::getEntry(SegletAllocator* allocator,
                             Buffer* buffer,
                             uint32_t* lengthWithMetadata)
{
    uint32_t segletSize = allocator->getSegletSize();

    // See if we can take the fast path for contiguous entries.
    EntryHeader* header = reinterpret_cast<EntryHeader*>(reference);
    uint32_t offset = downCast<uint32_t>(reference & (segletSize - 1));
    uint32_t fullHeaderLength = sizeof32(*header) +
                                header->getLengthBytes();
    if (expect_true(offset + fullHeaderLength <= segletSize)) {
        // Looks like the header fits. Now grab the length and see if
        // the whole entry fits.
        TEST_LOG("Contiguous entry");
        uint32_t dataLength = 0;
        const uint64_t offsetOfLength = reference + sizeof(*header);
        switch (header->getLengthBytes()) {
        case 1:
            dataLength = *reinterpret_cast<uint8_t*>(offsetOfLength);
            break;
        case 2:
            dataLength = *reinterpret_cast<uint16_t*>(offsetOfLength);
            break;
        default:
            // Note that this assumes little endian byte order.
            memcpy(&dataLength,
                   reinterpret_cast<uint16_t*>(offsetOfLength),
                   header->getLengthBytes());
        }

        uint32_t fullLength = fullHeaderLength + dataLength;
        if (expect_true(offset + fullLength <= segletSize)) {
            // The entry is contiguous.
            if (buffer != NULL) {
                if (fullLength < 300)
                    prefetch(
                        reinterpret_cast<void*>(reference + fullHeaderLength),
                            dataLength);
                buffer->append(
                    reinterpret_cast<void*>(reference + fullHeaderLength),
                    dataLength);
            }
            if (lengthWithMetadata != NULL)
                *lengthWithMetadata = fullLength;
            return header->getType();
        }
    }

    // Slow path for a discontiguous entry. Need to figure out which
    // Segment this belongs to so we can look up subsequent seglets.
    // This is likely to involve at least 3 additional cache misses.
    TEST_LOG("Discontiguous entry");
    LogSegment* segment = allocator->getOwnerSegment(
        reinterpret_cast<void*>(reference));
    return segment->getEntry(*this, buffer, lengthWithMetadata);
}

} // namespace
