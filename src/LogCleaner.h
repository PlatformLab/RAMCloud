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

#ifndef RAMCLOUD_LOGCLEANER_H
#define RAMCLOUD_LOGCLEANER_H

#include <thread>
#include <vector>

//#include "LogStatistics.pb.h"

#include "Common.h"
#include "Segment.h"
#include "LogSegment.h"
#include "SegmentManager.h"
#include "ReplicaManager.h"

namespace RAMCloud {

/**
 * The LogCleaner defragments a Log's closed Segments, writing out any live
 * data to new "survivor" Segments and passing the survivors, as well as the
 * cleaned Segments, to the Log that owns them. The cleaner is designed to
 * run asynchronously in a separate thread, though it can be run inline with
 * the Log code as well.
 *
 * The cleaner employs some heuristics to aid efficiency. For instance, it
 * tries to minimise the cost of cleaning by choosing Segments that have a
 * good 'cost-benefit' ratio. That is, it looks for Segments that have lots
 * of free space, but also for Segments that have less free space but a lot
 * of old data (the assumption being that old data is unlikely to die and
 * cleaning old data will reduce fragmentation and not soon require another
 * cleaning).
 *
 * In addition, the LogCleaner attempts to segregate entries by age in the
 * hopes of packing old data and new data into different Segments. This has
 * two main benefits. First, old data is less likely to fragment (be freed)
 * so those Segments will maintain high utilization and therefore require
 * less cleaning. Second, new data is more likely to fragment, so Segments
 * containing newer data will hopefully be cheaper to clean in the future.
 */
class LogCleaner {
  public:
    LogCleaner(Context* context,
               SegmentManager& segmentManager,
               ReplicaManager& replicaManager,
               uint32_t writeCostThreshold);
    ~LogCleaner();
    void statistics(/*ProtoBuf::LogStatistics& logStats*/) const
    {
    }
};

} // namespace

#endif // !RAMCLOUD_LOGCLEANER_H
