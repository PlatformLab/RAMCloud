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

Segment::DefaultHeapAllocator heapAllocator;

Segment::Segment(Allocator& allocator)
    : allocator(allocator),
      seglets(),
      closed(false),
      tail(0),
      bytesFreed(0),
      spaceTimeSum(0),
      checksum(),
      entryCountsByType()
{
    memset(entryCountsByType, 0, sizeof(entryCountsByType));

    for (uint32_t i = 0; i < allocator.getSegletsPerSegment(); i++)
        seglets.push_back(allocator.alloc());

    appendFooter();
}

// TODO(anyone): Once we rely on a c++11 compiler, use constructor delegation
//               rather than duplicating the above constructor.
Segment::Segment()
    : allocator(heapAllocator),
      seglets(),
      closed(false),
      tail(0),
      bytesFreed(0),
      spaceTimeSum(0),
      checksum(),
      entryCountsByType()
{
    memset(entryCountsByType, 0, sizeof(entryCountsByType));

    for (uint32_t i = 0; i < allocator.getSegletsPerSegment(); i++)
        seglets.push_back(allocator.alloc());

    appendFooter();
}

Segment::~Segment()
{
    for (SegletVector::iterator it = seglets.begin(); it != seglets.end(); it++)
        allocator.free(*it);
}

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

    entryCountsByType[downCast<uint8_t>(type)]++;

    appendFooter();

    outOffset = startOffset;

    return true;
}

bool
Segment::append(LogEntryType type, Buffer& buffer, uint32_t& outOffset)
{
    return append(type, buffer, 0, buffer.getTotalLength(), outOffset);
}

bool
Segment::append(LogEntryType type, Buffer& buffer)
{
    uint32_t outOffset;
    return append(type, buffer, outOffset);
}

bool
Segment::append(LogEntryType type, const void* data, uint32_t length, uint32_t& outOffset)
{
    Buffer buffer;
    Buffer::Chunk::appendToBuffer(&buffer, data, length);
    return append(type, buffer, 0, length, outOffset);
}

bool
Segment::append(LogEntryType type, const void* data, uint32_t length)
{
    uint32_t dummy;
    return append(type, data, length, dummy);
}

void
Segment::free(uint32_t offset)
{
    EntryHeader header;
    copyOut(offset, &header, sizeof(header));

    uint32_t length = 0;
    copyOut(offset, &length, header.getLengthBytes());

    bytesFreed += (sizeof32(header) + header.getLengthBytes() + length);
    assert(bytesFreed <= tail);
}

void
Segment::close()
{
    closed = true;
}

uint32_t
Segment::appendToBuffer(Buffer& buffer, uint32_t offset, uint32_t length)
{
    uint32_t bytesAppended = 0;

    while (length > 0) {
        uint32_t contigBytes = std::min(length, getContiguousBytesAt(offset));
        if (contigBytes == 0)
            break;

        Buffer::Chunk::appendToBuffer(&buffer,
                                      getAddressAt(offset),
                                      contigBytes);
        offset += contigBytes;
        length -= contigBytes;
        bytesAppended += contigBytes;
    }

    return bytesAppended;
}

uint32_t
Segment::appendToBuffer(Buffer& buffer)
{
    // Tail does not include the footer.
    uint32_t length = tail + bytesNeeded(sizeof32(Footer));
    return appendToBuffer(buffer, 0, length);
}

uint32_t
Segment::appendEntryToBuffer(uint32_t offset, Buffer& buffer)
{
    uint32_t entryDataOffset = getEntryDataOffset(offset);
    uint32_t entryDataLength = getEntryDataLength(offset);
    return appendToBuffer(buffer, entryDataOffset, entryDataLength);
}

LogEntryType
Segment::getEntryTypeAt(uint32_t offset)
{
    EntryHeader header;
    copyOut(offset, &header, sizeof(header));
    return header.getType();
}

uint32_t
Segment::getTailOffset()
{
    return tail;
}

uint32_t
Segment::getSegletsAllocated()
{
    return downCast<uint32_t>(seglets.size());
}

uint32_t
Segment::getSegletsNeeded()
{
    uint32_t liveBytes = tail - bytesFreed;
    return (liveBytes + allocator.getSegletSize() - 1) / allocator.getSegletSize();
}

/******************************************************************************
 * PRIVATE METHODS
 ******************************************************************************/

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

uint32_t
Segment::getEntryDataOffset(uint32_t offset)
{
    EntryHeader header;
    copyOut(offset, &header, sizeof(header));
    return offset + sizeof32(header) + header.getLengthBytes();
}

uint32_t
Segment::getEntryDataLength(uint32_t offset)
{
    EntryHeader header;
    copyOut(offset, &header, sizeof(header));

    uint32_t dataLength = 0;
    copyOut(offset + sizeof32(header), &dataLength, header.getLengthBytes());
    return dataLength;
}

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

uint32_t
Segment::getContiguousBytesAt(uint32_t offset)
{
    // XXX- fix check when in-memory cleaner can reduce # of seglets per segment
    if (offset >= allocator.getSegmentSize())
        return 0;

    uint32_t segletOffset = offset % allocator.getSegletSize();
    return allocator.getSegletSize() - segletOffset;
}

void*
Segment::offsetToSeglet(uint32_t offset)
{
    uint32_t index = offset / allocator.getSegletSize();
    if (index >= seglets.size())
        return NULL;
    return seglets[index];
}

uint32_t
Segment::bytesLeft()
{
    if (closed)
        return 0;

    static EntryHeader footerHeader(LOG_ENTRY_TYPE_SEGFOOTER, sizeof(Footer));
    uint32_t capacity = getSegletsAllocated() * allocator.getSegletSize();
    return capacity - tail;
}

uint32_t
Segment::bytesNeeded(uint32_t length)
{
    EntryHeader header(LOG_ENTRY_TYPE_INVALID, length);
    return sizeof32(EntryHeader) + header.getLengthBytes() + length;
}

void
Segment::copyOut(uint32_t offset, void* buffer, uint32_t length)
{
    uint8_t* bufferBytes = static_cast<uint8_t*>(buffer);
    while (length > 0) {
        uint32_t contigBytes = std::min(length, getContiguousBytesAt(offset));
        memcpy(bufferBytes, getAddressAt(offset), contigBytes);
        bufferBytes += contigBytes;
        offset += contigBytes;
        length -= contigBytes;
    }
}

void
Segment::copyIn(uint32_t offset, const void* buffer, uint32_t length)
{
    const uint8_t* bufferBytes = static_cast<const uint8_t*>(buffer);
    while (length > 0) {
        uint32_t contigBytes = std::min(length, getContiguousBytesAt(offset));
        memcpy(getAddressAt(offset), bufferBytes, contigBytes);
        bufferBytes += contigBytes;
        offset += contigBytes;
        length -= contigBytes;
    }
}

void
Segment::copyInFromBuffer(uint32_t segmentOffset,
                          Buffer& buffer,
                          uint32_t bufferOffset,
                          uint32_t length)
{
    Buffer::Iterator it(buffer, bufferOffset, length);
    while (!it.isDone()) {
        copyIn(segmentOffset, it.getData(), it.getLength());
        segmentOffset += it.getLength();
        it.next();
    }
}

} // namespace
