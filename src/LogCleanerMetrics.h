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

#if __GNUC__ >= 4 && __GNUC_MINOR__ >= 5
#include <atomic>
#else
#include <cstdatomic>
#endif

#include "Common.h"
#include "Histogram.h"
#include "SpinLock.h"

#include "LogMetrics.pb.h"

namespace RAMCloud {

namespace LogCleanerMetrics {

/// Just in case using atomics for metrics becomes a performance problem, we
/// will use this typedef to allow us to run in fast-and-loose mode with
/// regular uint64_ts, if needed.
typedef std::atomic_uint_fast64_t Metric64BitType;

/// Convenience typedef for CycleCounters of type Metric64BitType.
typedef CycleCounter<Metric64BitType> MetricCycleCounter;

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
          totalBytesFreed(0),
          totalBytesInCompactedSegments(0),
          totalBytesAppendedToSurvivors(0),
          totalSegmentsCompacted(0),
          totalEntriesScanned(),
          totalLiveEntriesScanned(),
          totalTicks(0),
          getSegmentToCompactTicks(0),
          waitForFreeSurvivorTicks(0),
          relocationCallbackTicks(0),
          relocationAppendTicks(0),
          compactionCompleteTicks(0)
    {
        memset(totalEntriesScanned, 0, sizeof(totalEntriesScanned));
        memset(totalLiveEntriesScanned, 0, sizeof(totalLiveEntriesScanned));
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
        m.set_total_bytes_freed(totalBytesFreed);
        m.set_total_bytes_in_compacted_segments(totalBytesInCompactedSegments);
        m.set_total_bytes_appended_to_survivors(totalBytesAppendedToSurvivors);
        m.set_total_segments_compacted(totalSegmentsCompacted);

        foreach (uint64_t count, totalEntriesScanned)
            m.add_total_entries_scanned(count);
        foreach (uint64_t count, totalLiveEntriesScanned)
            m.add_total_live_entries_scanned(count);

        m.set_total_ticks(totalTicks);
        m.set_get_segment_to_compact_ticks(getSegmentToCompactTicks);
        m.set_wait_for_free_survivor_ticks(waitForFreeSurvivorTicks);
        m.set_relocation_callback_ticks(relocationCallbackTicks);
        m.set_relocation_append_ticks(relocationAppendTicks);
        m.set_compaction_complete_ticks(compactionCompleteTicks);
    }

    /// Total number of times the entry relocation handler was called.
    Metric64BitType totalRelocationCallbacks;

    /// Total number of successful appends done in relocating a live object.
    /// Appends that weren't successful due to insufficient space would have
    /// bailed quickly and been retried after allocating a new survivor segment.
    Metric64BitType totalRelocationAppends;

    /// Total number of bytes freed by compacting segments in memory. This will
    /// be a multiple of the seglets size.
    Metric64BitType totalBytesFreed;

    /// Total number of bytes originally allocated to segments before they were
    /// compacted in memory. This will be a multiple of the seglet size.
    Metric64BitType totalBytesInCompactedSegments;

    /// Total number of bytes appended to survivor segments during compaction.
    /// In other words, the amount of live data.
    Metric64BitType totalBytesAppendedToSurvivors;

    /// Total number of times a segment has been compacted. Since the
    /// doMemoryCleaning() method processes one segment per call, this also
    /// implies the number of times it was called and did work.
    Metric64BitType totalSegmentsCompacted;

    /// Total number of each log entry the disk cleaner has encountered while
    /// cleaning segments. These counts include both dead and alive entries.
    Metric64BitType totalEntriesScanned[TOTAL_LOG_ENTRY_TYPES];

    /// Total number of each log entry the disk cleaner has encountered while
    /// cleaning segments. These counts only include live entries that were
    /// relocated. Take the difference from totalEntriesScanned to get the
    /// dead entry count.
    Metric64BitType totalLiveEntriesScanned[TOTAL_LOG_ENTRY_TYPES];

    /// Total number of cpu cycles spent in doMemoryCleaning().
    Metric64BitType totalTicks;

    /// Total number of cpu cycles spent choosing a segment to compact.
    Metric64BitType getSegmentToCompactTicks;

    /// Total number of cpu cycles spent waiting for a free survivor segment.
    Metric64BitType waitForFreeSurvivorTicks;

    /// Total number of cpu cycles spent in the relocation callback. Note that
    /// this will include time spent appending to the survivor segment if the
    /// entry needed to be relocated.
    Metric64BitType relocationCallbackTicks;

    /// Total number of cpu cycles spent appending relocated entries.
    Metric64BitType relocationAppendTicks;

    /// Total number of cpu cycles spent in SegmentManager::compactionComplete.
    Metric64BitType compactionCompleteTicks;
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
          totalSegmentsCleaned(0),
          totalSurvivorsCreated(0),
          totalRuns(0),
          totalEntriesScanned(),
          totalLiveEntriesScanned(),
          totalTicks(0),
          getSegmentsToCleanTicks(0),
          costBenefitSortTicks(0),
          getSortedEntriesTicks(0),
          timestampSortTicks(0),
          relocateLiveEntriesTicks(0),
          waitForFreeSurvivorsTicks(0),
          cleaningCompleteTicks(0),
          relocationCallbackTicks(0),
          relocationAppendTicks(0),
          cleanedSegmentMemoryHistogram(101, 1),
          cleanedSegmentDiskHistogram(101, 1)
    {
        memset(totalEntriesScanned, 0, sizeof(totalEntriesScanned));
        memset(totalLiveEntriesScanned, 0, sizeof(totalLiveEntriesScanned));
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
        m.set_total_segments_cleaned(totalSegmentsCleaned);
        m.set_total_survivors_created(totalSurvivorsCreated);
        m.set_total_runs(totalRuns);

        foreach (uint64_t count, totalEntriesScanned)
            m.add_total_entries_scanned(count);
        foreach (uint64_t count, totalLiveEntriesScanned)
            m.add_total_live_entries_scanned(count);

        m.set_total_ticks(totalTicks);
        m.set_get_segments_to_clean_ticks(getSegmentsToCleanTicks);
        m.set_cost_benefit_sort_ticks(costBenefitSortTicks);
        m.set_get_sorted_entries_ticks(getSortedEntriesTicks);
        m.set_timestamp_sort_ticks(timestampSortTicks);
        m.set_relocate_live_entries_ticks(relocateLiveEntriesTicks);
        m.set_wait_for_free_survivors_ticks(waitForFreeSurvivorsTicks);
        m.set_cleaning_complete_ticks(cleaningCompleteTicks);
        m.set_relocation_callback_ticks(relocationCallbackTicks);
        m.set_relocation_append_ticks(relocationAppendTicks);
        cleanedSegmentMemoryHistogram.serialize(
            *m.mutable_cleaned_segment_memory_histogram());
        cleanedSegmentDiskHistogram.serialize(
            *m.mutable_cleaned_segment_disk_histogram());
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
    Metric64BitType totalBytesAppendedToSurvivors;

    /// Total number of bytes freed in RAM by cleaning (net gain).
    Metric64BitType totalMemoryBytesFreed;

    /// Total number of bytes freed on disk by cleaning (net gain).
    Metric64BitType totalDiskBytesFreed;

    /// Total number of bytes allocated to segments that were cleaned. This is
    /// the amount of space in memory only.
    Metric64BitType totalMemoryBytesInCleanedSegments;

    /// Total number of bytes on disk for segments that were cleaned. This is
    /// equal to the number of segments cleaned times the segment size.
    Metric64BitType totalDiskBytesInCleanedSegments;

    /// Total number of times the entry relocation handler was called.
    Metric64BitType totalRelocationCallbacks;

    /// Total number of successful appends done in relocating a live object.
    /// Appends that weren't successful due to insufficient space would have
    /// bailed quickly and been retried after allocating a new survivor segment.
    Metric64BitType totalRelocationAppends;

    /// Total number of segments the disk cleaner has cleaned.
    Metric64BitType totalSegmentsCleaned;

    /// Total number of survivor segments created to relocate live data into.
    Metric64BitType totalSurvivorsCreated;

    /// Total number of disk cleaner runs. That is, the number of times the
    /// disk cleaner did some work (chose some segments and relocated their
    /// live data to survivors).
    Metric64BitType totalRuns;

    /// Total number of each log entry the disk cleaner has encountered while
    /// cleaning segments. These counts include both dead and alive entries.
    Metric64BitType totalEntriesScanned[TOTAL_LOG_ENTRY_TYPES];

    /// Total number of each log entry the disk cleaner has encountered while
    /// cleaning segments. These counts only include live entries that were
    /// relocated. Take the difference from totalEntriesScanned to get the
    /// dead entry count.
    Metric64BitType totalLiveEntriesScanned[TOTAL_LOG_ENTRY_TYPES];

    /// Total number of cpu cycles spent in doDiskCleaning().
    Metric64BitType totalTicks;

    /// Total number of cpu cycles spent in getSegmentsToClean().
    Metric64BitType getSegmentsToCleanTicks;

    /// Total number of cpu cycles spent sorting candidate segments by best
    /// cost-benefit.
    Metric64BitType costBenefitSortTicks;

    /// Total number of cpu cycles spent in getSortedEntries().
    Metric64BitType getSortedEntriesTicks;

    /// Total number of cpu cycles spent sorting entries from segments being
    /// cleaned according to their timestamp.
    Metric64BitType timestampSortTicks;

    /// Total number of cpu cycles spent in relocateLiveEntries().
    Metric64BitType relocateLiveEntriesTicks;

    /// Total number of cpu cycles waiting for sufficient survivor segments.
    Metric64BitType waitForFreeSurvivorsTicks;

    /// Total number of cpu cycles spent in SegmentManager::cleaningComplete().
    Metric64BitType cleaningCompleteTicks;

    /// Total number of cpu cycles spent in the relocation callback. Note that
    /// this will include time spent appending to the survivor segment if the
    /// entry needed to be relocated.
    Metric64BitType relocationCallbackTicks;

    /// Total number of cpu cycles spent appending relocated entries.
    Metric64BitType relocationAppendTicks;

    /// Histogram of memory utilizations for segments cleaned on disk.
    /// This lets us see how frequency we clean segments with varying amounts
    /// of live data.
    Histogram cleanedSegmentMemoryHistogram;

    /// Histogram of disk space utilizations for segments cleaned on disk.
    Histogram cleanedSegmentDiskHistogram;
};

/**
 * Metrics that keep track of the cleaner's threads.
 */
class Threads {
  public:
    /**
     * Construct a new Thread object for tracking cleaner threads.
     *
     * \param maxThreads
     *      The maximum number of threads the cleaner will ever have running
     *      simultaneously.
     */
    explicit Threads(size_t maxThreads)
        : lock("LogCleanerMetrics::Threads")
        , activeTicks()
        , activeThreads(0)
        , cycleCounter()
    {
        activeTicks.resize(maxThreads + 1, 0);
        cycleCounter.construct();
    }

    /**
     * Note that another thread has awoken in the cleaner and started doing
     * some work. This is used to maintain a distribution of the amount of
     * time the cleaner spends with various numbers of threads running.
     */
    void
    noteThreadStart()
    {
            lock.lock();
            activeTicks[activeThreads] += cycleCounter->stop();
            cycleCounter.construct();
            activeThreads++;
            lock.unlock();
    }

    /**
     * Note that a thread in the cleaner has finished whatever work it was doing
     * for the moment. This is used to maintain a distribution of the amount of
     * time the cleaner spends with various numbers of threads running.
     */
    void
    noteThreadStop()
    {
        lock.lock();
        activeTicks[activeThreads] += cycleCounter->stop();
        cycleCounter.construct();
        activeThreads--;
        lock.unlock();
    }

    /**
     * Serialize the metrics in this class to the given protocol buffer so we
     * can ship it to another machine.
     *
     * \param[out] m
     *      The protocol buffer to fill in.
     */
    void
    serialize(ProtoBuf::LogMetrics_CleanerMetrics_ThreadMetrics& m)
    {
        lock.lock();
        foreach (uint64_t ticks, activeTicks)
            m.add_active_ticks(ticks);
        lock.unlock();
    }

  private:
    /// Monitor lock protecting all members on this class.
    SpinLock lock;

    /// Number of ticks the cleaner has spent with varying numbers of threads
    /// active simultaneously. [0] doubles as a count of the time all threads
    /// are sleeping without work to do.
    vector<uint64_t> activeTicks;

    /// Count of the number of threads currently active in the cleaner.
    uint32_t activeThreads;

    /// Number of cycles expended since the last change to 'activeThreads'.
    Tub<MetricCycleCounter> cycleCounter;
};

} // namespace LogCleanerMetrics

} // namespace RAMCloud

#endif // !RAMCLOUD_LOGCLEANERMETRICS_H
