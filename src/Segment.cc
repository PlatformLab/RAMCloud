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

Segment::~Segment()
{
    static_assert(sizeof(SegmentEntry) == 8);
    static_assert(sizeof(SegmentHeader) == 24);
    static_assert(sizeof(SegmentFooter) == 8);
}

const void *
Segment::append(LogEntryType type, const void *buffer, uint64_t length)
{
    if (type == LOG_ENTRY_TYPE_SEGFOOTER || appendableBytes() < length)
        return NULL;

    return forceAppendWithEntry(type, buffer, length);
}

void
Segment::free(const uint64_t length)
{
    bytesFreed += length;
    assert(bytesFreed <= capacity);
}

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

const void *
Segment::getBaseAddress() const
{
    return baseAddress;
}

uint64_t
Segment::getId() const
{
    return id;
}

uint64_t
Segment::getLength() const
{
    return capacity;
}

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

void
Segment::forEachEntry(SegmentEntryCallback cb, void *cookie) const
{
    for (SegmentIterator i(this); !i.isDone(); i.next())
        cb(i.getType(), i.getPointer(), i.getLength(), cookie);
}

////////////////////////////////////////
/// Private Methods
////////////////////////////////////////

const void *
Segment::forceAppendBlob(const void *buffer, uint64_t length)
{
    void *p = (uint8_t *)baseAddress + tail;
    memcpy(p, buffer, length);
    tail += length;
    assert(tail <= capacity);
    return p;
}

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
