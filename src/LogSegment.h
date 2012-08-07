/* Copyright (c) 2012 Stanford University
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

#ifndef RAMCLOUD_LOGSEGMENT_H
#define RAMCLOUD_LOGSEGMENT_H

#include "BoostIntrusive.h"
#include "ReplicatedSegment.h"
#include "Segment.h"

namespace RAMCloud {

/**
 * LogSegment is a simple subclass of Segment. It exists to associate data the
 * Log and LogCleaner care about with a particular Segment (which shouldn't
 * have to know about these things).
 */
class LogSegment : public Segment {
  public:
    LogSegment(Allocator& allocator,
               uint64_t id,
               uint32_t slot)
        : Segment(allocator),
          id(id),
          slot(slot),
          cleanedEpoch(0),
          replicatedSegment(NULL),
          listEntries(),
          allListEntries()
    {
    }

    /// Log-unique 64-bit identifier for this segment.
    const uint64_t id;

    /// SegmentManager slot associated with this segment.
    const uint32_t slot;

    /// Epoch during which this segment was cleaned.
    uint64_t cleanedEpoch;

    /// The ReplicatedSegment instance that is handling backups of this segment.
    ReplicatedSegment* replicatedSegment;

    /// Hook used for linking this LogSegment into an intrusive list according
    /// to this object's state in SegmentManager.
    IntrusiveListHook listEntries;

    /// Hook used for linking this LogSegment into a global instrusive list
    /// of all LogSegments in SegmentManager.
    IntrusiveListHook allListEntries;

    DISALLOW_COPY_AND_ASSIGN(LogSegment);
};

typedef std::vector<LogSegment*> LogSegmentVector;

} // namespace

#endif // !RAMCLOUD_LOGSEGMENT_H
