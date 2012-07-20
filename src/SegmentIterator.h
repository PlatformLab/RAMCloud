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

#ifndef RAMCLOUD_SEGMENTITERATOR_H
#define RAMCLOUD_SEGMENTITERATOR_H

#include <stdint.h>
#include <vector>
#include "Crc32C.h"
#include "Log.h"
#include "Segment.h"

using std::vector;

namespace RAMCloud {

/**
 * An exception that is thrown when the SegmentIterator class cannot iterate
 * over a segment due to corruption.
 */
struct SegmentIteratorException : public Exception {
    SegmentIteratorException(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
};

class SegmentIterator {
  public:
    SegmentIterator();
    explicit SegmentIterator(Segment& segment);
    SegmentIterator(const void *buffer, uint32_t length);
    VIRTUAL_FOR_TESTING ~SegmentIterator();
    VIRTUAL_FOR_TESTING bool isDone();
    VIRTUAL_FOR_TESTING void next();
    VIRTUAL_FOR_TESTING LogEntryType getType();
    VIRTUAL_FOR_TESTING uint32_t getLength();
    VIRTUAL_FOR_TESTING Buffer& appendToBuffer(Buffer& buffer);
    VIRTUAL_FOR_TESTING Buffer& setBufferTo(Buffer& buffer);

  PRIVATE:
    Tub<Segment*> segment;
    Tub<const void*> segmentBuffer;
    Tub<uint32_t> segmentLength;
    uint32_t currentOffset;
 
    const void* getAddressAt(uint32_t offset);
    uint32_t getContiguousBytesAt(uint32_t offset);
    void copyOut(uint32_t offset, void* destination, uint32_t length);
    const Segment::EntryHeader* getEntryHeader(uint32_t offset);
    void checkIntegrity();

    DISALLOW_COPY_AND_ASSIGN(SegmentIterator);
};

} // namespace

#endif // !RAMCLOUD_SEGMENTITERATOR_H
