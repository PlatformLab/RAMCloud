/* Copyright (c) 2011 Stanford University
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

#ifndef RAMCLOUD_LOGITERATOR_H
#define RAMCLOUD_LOGITERATOR_H

#include <stdint.h>
#include <vector>
#include "Log.h"
#include "SegmentIterator.h"

namespace RAMCloud {

class LogIterator {
  PUBLIC:
    explicit LogIterator(Log& log);
    ~LogIterator();

    bool isDone();
    void next();
    bool onHead();
    LogEntryType getType();
    uint32_t getLength();
    Buffer& appendToBuffer(Buffer& buffer);
    Buffer& setBufferTo(Buffer& buffer);

  PRIVATE:
    /**
     * Comparison functor used to sort Segments by ID in descending order.
     */
    struct SegmentIdLessThan {
      public:
        bool
        operator()(const LogSegment* a, const LogSegment* b)
        {
            return a->id > b->id;
        }
    };

    void populateSegmentList(uint64_t nextSegmentId);

    /// Reference to the Log we're iterating.
    Log& log;

    /// Current list of segments to iterate over. Once exhausted, the list may
    /// need to be updated (since the log could have advanced forward).
    LogSegmentVector segmentList;

    /// Pointer to the current LogSegment we're interating over.
    LogSegment* currentSegment;

    /// SegmentIterator for the Segment we're currently iterating over.
    Tub<SegmentIterator> currentIterator;

    /// Indication that the head is locked and must be unlocked on destruction.
    bool headLocked;

    DISALLOW_COPY_AND_ASSIGN(LogIterator);
};

} // namespace

#endif // !RAMCLOUD_LOGITERATOR_H
