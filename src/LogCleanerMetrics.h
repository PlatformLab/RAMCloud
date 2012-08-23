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

#ifndef RAMCLOUD_LOGCLEANERMETRICS_H
#define RAMCLOUD_LOGCLEANERMETRICS_H

//#include "LogStatistics.pb.h"

#include "Common.h"

namespace RAMCloud {

namespace LogCleanerMetrics {

/**
 * Metrics for in-memory cleaning.
 */
class InMemory {
  public:

};

/**
 * Metrics for on-disk cleaning.
 */
class OnDisk {
  public:
    OnDisk()
        : totalBytesAppendedToSurvivors(0),
          totalMemoryBytesFreed(0),
          totalDiskBytesFreed(0),
          totalBytesAllocatedInCleanedSegments(0),
          totalTicks(0),
          getSegmentsToCleanTicks(0),
          getSortedLiveEntriesTicks(0),
          relocateLiveEntriesTicks(0),
          cleaningCompleteTicks(0)
    {
    }

    double
    getAverageMemoryWriteCost()
    {
        return 1 + static_cast<double>(totalBytesAppendedToSurvivors) /
                   static_cast<double>(totalMemoryBytesFreed);
    }

    double
    getAverageDiskWriteCost()
    {
        return 1 + static_cast<double>(totalBytesAppendedToSurvivors) /
                   static_cast<double>(totalDiskBytesFreed);
    }

    double
    getAverageCleanedSegmentUtilization()
    {
        return 100 * static_cast<double>(totalBytesAppendedToSurvivors) /
                     static_cast<double>(totalBytesAllocatedInCleanedSegments);
    }

    /// Total number of bytes appended to survivor segments.
    uint64_t totalBytesAppendedToSurvivors;

    /// Total number of bytes freed in RAM by cleaning (net gain).
    uint64_t totalMemoryBytesFreed;

    /// Total number of bytes freed on disk by cleaning (net gain).
    uint64_t totalDiskBytesFreed;

    /// Total number of bytes allocated to segments that were cleaned.
    uint64_t totalBytesAllocatedInCleanedSegments;

    /// Total number of cpu cycles spent in doDiskCleaning().
    uint64_t totalTicks;

    /// Total number of cpu cycles spent in getSegmentsToClean().
    uint64_t getSegmentsToCleanTicks;

    /// Total number of cpu cycles spent in getSortedLiveEntries().
    uint64_t getSortedLiveEntriesTicks;

    /// Total number of cpu cycles spent in relocateLiveEntries().
    uint64_t relocateLiveEntriesTicks;

    /// Total number of cpu cycles spent in SegmentManager::cleaningComplete().
    uint64_t cleaningCompleteTicks;
};

} // namespace LogCleanerMetrics

} // namespace RAMCloud

#endif // !RAMCLOUD_LOGCLEANERMETRICS_H
