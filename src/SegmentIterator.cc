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

SegmentIterator::SegmentIterator(const Segment *segment)
    : baseAddress(segment->getBaseAddress()),
      segmentLength(segment->getLength()),
      id(segment->getId()),
      type(LOG_ENTRY_TYPE_INVALID),
      length(0),
      blobPtr(0),
      firstEntry(0),
      currentEntry(0),
      sawFooter(false)
{
    CommonConstructor();
}

SegmentIterator::SegmentIterator(const void *buffer, uint64_t length)
    : baseAddress(buffer),
      segmentLength(length),
      id(-1),
      type(LOG_ENTRY_TYPE_INVALID),
      length(0),
      blobPtr(0),
      firstEntry(0),
      currentEntry(0),
      sawFooter(false)
{
    CommonConstructor();
}

void
SegmentIterator::CommonConstructor()
{
    if (segmentLength < (sizeof(SegmentEntry) + sizeof(SegmentHeader)))
        throw 0;

    const SegmentEntry *entry = (const SegmentEntry *)baseAddress;
    if (entry->type   != LOG_ENTRY_TYPE_SEGHEADER ||
        entry->length != sizeof(SegmentHeader) ||
        !isEntryValid(entry)) {
        throw 0;
    }

    const SegmentHeader *header = (const SegmentHeader *)((char *)baseAddress +
        sizeof(SegmentEntry));
    if (header->segmentLength != segmentLength)
        throw 0;

    type    = entry->type;
    length  = entry->length;
    blobPtr = (char *)baseAddress + sizeof(*entry);

    currentEntry = firstEntry = entry;
}

bool
SegmentIterator::isEntryValid(const SegmentEntry *entry) const
{
    uintptr_t lastByte      = (uintptr_t)baseAddress + segmentLength - 1;
    uintptr_t entryStart    = (uintptr_t)entry;
    uintptr_t entryLastByte = entryStart + entry->length + sizeof(*entry) - 1;

    // this is an internal error
    if (entryStart < (uintptr_t)baseAddress)
        throw 0;

    if (entryLastByte > lastByte)
        return false;

    return true;
}

bool
SegmentIterator::isDone() const
{
    return (sawFooter || !isEntryValid(currentEntry));
}

void
SegmentIterator::next()
{
    type = LOG_ENTRY_TYPE_INVALID;
    length = 0;
    blobPtr = NULL;

    if (currentEntry == NULL)
        return;

    if (currentEntry->type == LOG_ENTRY_TYPE_SEGFOOTER) {
        sawFooter = true;
        return;
    }

    uintptr_t nextEntry = (uintptr_t)currentEntry + sizeof(*currentEntry) +
        currentEntry->length;
    const SegmentEntry *entry = (const SegmentEntry *)nextEntry;

    if (!isEntryValid(entry)) {
        currentEntry = NULL;
        return;
    }

    type    = entry->type;
    length  = entry->length;
    blobPtr = (const void *)((uintptr_t)entry + sizeof(*entry));
    currentEntry = entry;
}

LogEntryType
SegmentIterator::getType() const
{
    if (currentEntry == NULL)
        throw 0;
    return type;
}

uint64_t
SegmentIterator::getLength() const
{
    if (currentEntry == NULL)
        throw 0;
    return length;
}

const void *
SegmentIterator::getPointer() const
{
    if (currentEntry == NULL)
        throw 0;
    return blobPtr;
}

uint64_t
SegmentIterator::getOffset() const
{
    if (currentEntry == NULL)
        throw 0;
    return (uintptr_t)blobPtr - (uintptr_t)baseAddress;
}

} // namespace
