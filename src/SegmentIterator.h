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

// RAMCloud pragma [CPPLINT=0]

#ifndef RAMCLOUD_SEGMENTITERATOR_H
#define RAMCLOUD_SEGMENTITERATOR_H

#include <stdint.h>
#include <Segment.h>
#include <vector>
using std::vector;

namespace RAMCloud {

class SegmentIterator {
  public:
    SegmentIterator(const Segment *segment);
    SegmentIterator(const void *buffer, uint64_t length);

    bool         isDone() const;
    void         next();
    LogEntryType getType() const;
    uint64_t     getLength() const;
    const void  *getPointer() const;
    uint64_t     getOffset() const;

  private:
    void            CommonConstructor(void);
    bool            isEntryValid(const SegmentEntry *entry) const;

    const void     *baseAddress;     // base address for the Segment
    uint64_t        segmentCapacity; // maximum length of the segment in bytes
    uint64_t        id;              // segment identification number

    // current iteration state
    LogEntryType     type;
    uint64_t         length; 
    const void      *blobPtr;
    bool             sawFooter;

    const SegmentEntry    *firstEntry;
    const SegmentEntry    *currentEntry;

    DISALLOW_COPY_AND_ASSIGN(SegmentIterator);

    friend class SegmentIteratorTest;
};

} // namespace

#endif // !RAMCLOUD_SEGMENTITERATOR_H
