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
    SegmentIterator(const void* buffer, uint32_t length);
    VIRTUAL_FOR_TESTING ~SegmentIterator();
    VIRTUAL_FOR_TESTING bool isDone();
    VIRTUAL_FOR_TESTING void next();
    VIRTUAL_FOR_TESTING LogEntryType getType();
    VIRTUAL_FOR_TESTING uint32_t getLength();
    VIRTUAL_FOR_TESTING uint32_t appendToBuffer(Buffer& buffer);
    VIRTUAL_FOR_TESTING uint32_t setBufferTo(Buffer& buffer);

  PRIVATE:
    /// If the constructor was called on a void pointer, we'll create a wrapper
    /// segment to access the data in a common way using segment object calls.
    Tub<Segment> wrapperSegment;

    /// Pointer to the segment we're iterating on. This points either to the
    /// segment object passed in to the constructor, or wrapperSegment above if
    /// we're iterating over a void buffer.
    Segment* segment;

    /// Current offset into the segment. This points to the entry we're on and
    /// will use in the getType, getLength, appendToBuffer, etc. calls.
    uint32_t currentOffset;
 
    DISALLOW_COPY_AND_ASSIGN(SegmentIterator);
};

} // namespace

#endif // !RAMCLOUD_SEGMENTITERATOR_H
