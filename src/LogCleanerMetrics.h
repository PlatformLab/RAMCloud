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

#ifndef RAMCLOUD_LOGCLEANERMETRICS_H
#define RAMCLOUD_LOGCLEANERMETRICS_H

#include "Common.h"

#include "LogMetrics.pb.h"

namespace RAMCloud {

namespace LogCleanerMetrics {

/**
 * Metrics for in-memory cleaning.
 */
class InMemory {
  public:
    /**
     * Construct a new InMemory metrics object with all counters zeroed.
     */
    InMemory()
        : totalRelocationCallbacks(0),
          totalRelocationAppends(0),
          relocationCallbackTicks(0),
          relocationAppendTicks(0)
    {
    }

    /**
     * Serialize the metrics in this class to the given protocol buffer so we
     * can ship it to another machine.
     *
     * \param[out] m
     *      The protocol buffer to fill in.
     */
    void
    serialize(ProtoBuf::LogMetrics_CleanerMetrics_InMemoryMetrics& m) const
    {
        m.set_total_relocation_callbacks(totalRelocationCallbacks);
        m.set_total_relocation_appends(totalRelocationAppends);
        m.set_relocation_callback_ticks(relocationCallbackTicks);
        m.set_relocation_append_ticks(relocationAppendTicks);
    }

    /// Total number of times the entry relocation handler was called.
    uint64_t totalRelocationCallbacks;

    /// Total number of successful appends done in relocating a live object.
    /// Appends that weren't successful due to insufficient space would have
    /// bailed quickly and been retried after allocating a new survivor segment.
    uint64_t totalRelocationAppends;

    /// Total number of cpu cycles spent in the relocation callback. Note that
    /// this will include time spent appending to the survivor segment if the
    /// entry needed to be relocated.
    uint64_t relocationCallbackTicks;

    /// Total number of cpu cycles spent appending relocated entries.
    uint64_t relocationAppendTicks;
};

/**
 * Metrics for on-disk cleaning.
 */
class OnDisk {
  public:
    /**
     * Construct a new OnDisk metrics object with all counters zeroed.
     */
    OnDisk()
        : totalBytesAppendedToSurvivors(0),
          totalMemoryBytesFreed(0),
          totalDiskBytesFreed(0),
          totalMemoryBytesInCleanedSegments(0),
          totalDiskBytesInCleanedSegments(0),
          totalRelocationCallbacks(0),
          totalRelocationAppends(0),
          totalTicks(0),
          getSegmentsToCleanTicks(0),
          costBenefitSortTicks(0),
          getSortedEntriesTicks(0),
          timestampSortTicks(0),
          relocateLiveEntriesTicks(0),
          cleaningCompleteTicks(0),
          relocationCallbackTicks(0),
          relocationAppendTicks(0)
    {
    }

    /**
     * Serialize the metrics in this class to the given protocol buffer so we
     * can ship it to another machine.
     *
     * \param[out] m
     *      The protocol buffer to fill in.
     */
    void
    serialize(ProtoBuf::LogMetrics_CleanerMetrics_OnDiskMetrics& m) const
    {
        m.set_total_bytes_appended_to_survivors(totalBytesAppendedToSurvivors);
        m.set_total_memory_bytes_freed(totalMemoryBytesFreed);
        m.set_total_disk_bytes_freed(totalDiskBytesFreed);
        m.set_total_memory_bytes_in_cleaned_segments(
            totalMemoryBytesInCleanedSegments);
        m.set_total_disk_bytes_in_cleaned_segments(
            totalDiskBytesInCleanedSegments);
        m.set_total_relocation_callbacks(totalRelocationCallbacks);
        m.set_total_relocation_appends(totalRelocationAppends);
        m.set_total_ticks(totalTicks);
        m.set_get_segments_to_clean_ticks(getSegmentsToCleanTicks); 
        m.set_cost_benefit_sort_ticks(costBenefitSortTicks);
        m.set_get_sorted_entries_ticks(getSortedEntriesTicks);
        m.set_timestamp_sort_ticks(timestampSortTicks);
        m.set_relocate_live_entries_ticks(relocateLiveEntriesTicks);
        m.set_cleaning_complete_ticks(cleaningCompleteTicks);
        m.set_relocation_callback_ticks(relocationCallbackTicks);
        m.set_relocation_append_ticks(relocationAppendTicks);
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
    getAverageCleanedSegmentMemoryUtilization()
    {
        return 100 * static_cast<double>(totalBytesAppendedToSurvivors) /
                     static_cast<double>(totalMemoryBytesInCleanedSegments);
    }

    double
    getAverageCleanedSegmentDiskUtilization()
    {
        return 100 * static_cast<double>(totalBytesAppendedToSurvivors) /
                     static_cast<double>(totalDiskBytesInCleanedSegments);
    }

    /// Total number of bytes appended to survivor segments.
    uint64_t totalBytesAppendedToSurvivors;

    /// Total number of bytes freed in RAM by cleaning (net gain).
    uint64_t totalMemoryBytesFreed;

    /// Total number of bytes freed on disk by cleaning (net gain).
    uint64_t totalDiskBytesFreed;

    /// Total number of bytes allocated to segments that were cleaned. This is
    /// the amount of space in memory only.
    uint64_t totalMemoryBytesInCleanedSegments;

    /// Total number of bytes on disk for segments that were cleaned. This is
    /// equal to the number of segments cleaned times the segment size.
    uint64_t totalDiskBytesInCleanedSegments;

    /// Total number of times the entry relocation handler was called.
    uint64_t totalRelocationCallbacks;

    /// Total number of successful appends done in relocating a live object.
    /// Appends that weren't successful due to insufficient space would have
    /// bailed quickly and been retried after allocating a new survivor segment.
    uint64_t totalRelocationAppends;

    /// Total number of cpu cycles spent in doDiskCleaning().
    uint64_t totalTicks;

    /// Total number of cpu cycles spent in getSegmentsToClean().
    uint64_t getSegmentsToCleanTicks;

    /// Total number of cpu cycles spent sorting candidate segments by best
    /// cost-benefit.
    uint64_t costBenefitSortTicks;

    /// Total number of cpu cycles spent in getSortedEntries().
    uint64_t getSortedEntriesTicks;

    /// Total number of cpu cycles spent sorting entries from segments being
    /// cleaned according to their timestamp.
    uint64_t timestampSortTicks;

    /// Total number of cpu cycles spent in relocateLiveEntries().
    uint64_t relocateLiveEntriesTicks;

    /// Total number of cpu cycles spent in SegmentManager::cleaningComplete().
    uint64_t cleaningCompleteTicks;

    /// Total number of cpu cycles spent in the relocation callback. Note that
    /// this will include time spent appending to the survivor segment if the
    /// entry needed to be relocated.
    uint64_t relocationCallbackTicks;

    /// Total number of cpu cycles spent appending relocated entries.
    uint64_t relocationAppendTicks;
};

} // namespace LogCleanerMetrics

} // namespace RAMCloud

#endif // !RAMCLOUD_LOGCLEANERMETRICS_H
