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

#if (__GNUC__ == 4 && __GNUC_MINOR__ >= 5) || (__GNUC__ > 4)
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
/// regular uint32_ts and uint64_ts, if needed.
typedef std::atomic_uint_fast64_t Atomic64BitType;

/// Convenience typedef for declaring atomic CycleCounters.
typedef CycleCounter<Atomic64BitType> AtomicCycleCounter;

/**
 * Metrics for in-memory cleaning.
 */
template<typename CounterType = Atomic64BitType>
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
          totalEmptySegmentsCompacted(0),
          totalEntriesScanned(),
          totalLiveEntriesScanned(),
          totalScannedEntryLengths(),
          totalLiveScannedEntryLengths(),
          totalTicks(0),
          getSegmentToCompactTicks(0),
          waitForFreeSurvivorTicks(0),
          relocationCallbackTicks(0),
          relocationAppendTicks(0),
          compactionCompleteTicks(0)
    {
        memset(totalEntriesScanned, 0, sizeof(totalEntriesScanned));
        memset(totalLiveEntriesScanned, 0, sizeof(totalLiveEntriesScanned));
        memset(totalScannedEntryLengths, 0, sizeof(totalScannedEntryLengths));
        memset(totalLiveScannedEntryLengths, 0,
            sizeof(totalLiveScannedEntryLengths));
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
        m.set_total_empty_segments_compacted(totalEmptySegmentsCompacted);

        foreach (uint64_t count, totalEntriesScanned)
            m.add_total_entries_scanned(count);
        foreach (uint64_t count, totalLiveEntriesScanned)
            m.add_total_live_entries_scanned(count);
        foreach (uint64_t count, totalScannedEntryLengths)
            m.add_total_scanned_entry_lengths(count);
        foreach (uint64_t count, totalLiveScannedEntryLengths)
            m.add_total_live_scanned_entry_lengths(count);

        m.set_total_ticks(totalTicks);
        m.set_get_segment_to_compact_ticks(getSegmentToCompactTicks);
        m.set_wait_for_free_survivor_ticks(waitForFreeSurvivorTicks);
        m.set_relocation_callback_ticks(relocationCallbackTicks);
        m.set_relocation_append_ticks(relocationAppendTicks);
        m.set_compaction_complete_ticks(compactionCompleteTicks);
    }

    /**
     * Merge counters from ``other'' into this InMemory instance. This is
     * typically used to merge thread-local counts into global metrics,
     * where the thread-local counters are backed by simple uint64_ts,
     * rather than atomic integers. This allows us to maintain fine-grained
     * metrics without horribly impacting performance by aggregating them
     * less frequently.
     */
    template<typename T>
    void
    merge(InMemory<T>& other)
    {
#define MERGE_FIELD(_x) _x += other._x;
        MERGE_FIELD(totalRelocationCallbacks);
        MERGE_FIELD(totalRelocationAppends);
        MERGE_FIELD(totalBytesFreed);
        MERGE_FIELD(totalBytesInCompactedSegments);
        MERGE_FIELD(totalBytesAppendedToSurvivors);
        MERGE_FIELD(totalSegmentsCompacted);
        MERGE_FIELD(totalEmptySegmentsCompacted);

        for (int i = 0; i < TOTAL_LOG_ENTRY_TYPES; i++) {
            MERGE_FIELD(totalEntriesScanned[i]);
            MERGE_FIELD(totalLiveEntriesScanned[i]);
            MERGE_FIELD(totalScannedEntryLengths[i]);
            MERGE_FIELD(totalLiveScannedEntryLengths[i]);
        }

        MERGE_FIELD(totalTicks);
        MERGE_FIELD(getSegmentToCompactTicks);
        MERGE_FIELD(waitForFreeSurvivorTicks);
        MERGE_FIELD(relocationCallbackTicks);
        MERGE_FIELD(relocationAppendTicks);
        MERGE_FIELD(compactionCompleteTicks);
#undef MERGE_FIELD
    }

    /// Total number of times the entry relocation handler was called.
    CounterType totalRelocationCallbacks;

    /// Total number of successful appends done in relocating a live object.
    /// Appends that weren't successful due to insufficient space would have
    /// bailed quickly and been retried after allocating a new survivor segment.
    CounterType totalRelocationAppends;

    /// Total number of bytes freed by compacting segments in memory. This will
    /// be a multiple of the seglets size.
    CounterType totalBytesFreed;

    /// Total number of bytes originally allocated to segments before they were
    /// compacted in memory. This will be a multiple of the seglet size.
    CounterType totalBytesInCompactedSegments;

    /// Total number of bytes appended to survivor segments during compaction.
    /// In other words, the amount of live data.
    CounterType totalBytesAppendedToSurvivors;

    /// Total number of times a segment has been compacted. Since the
    /// doMemoryCleaning() method processes one segment per call, this also
    /// implies the number of times it was called and did work.
    CounterType totalSegmentsCompacted;

    /// Number of segments compacted that were empty (no live data whatsoever).
    CounterType totalEmptySegmentsCompacted;

    /// Total number of each log entry the in-memory cleaner has encountered
    /// while compacting segments. These counts include both dead and alive
    /// entries.
    CounterType totalEntriesScanned[TOTAL_LOG_ENTRY_TYPES];

    /// Total number of each log entry the in-memory cleaner has encountered
    /// while compacting segments. These counts only include live entries that
    /// were relocated. Take the difference from totalEntriesScanned to get the
    /// dead entry count.
    CounterType totalLiveEntriesScanned[TOTAL_LOG_ENTRY_TYPES];

    /// Total number of bytes in each log entry the in-memory cleaner has
    /// encountered while compacting segments. This includes both dead and
    /// alive entries. Each time a count in totalEntriesScanned is incremented,
    /// the entry's corresponding length is added here. This way we can compute
    /// both the percentage of total entries of each type we see, as well as the
    /// ratio of total bytes they correspond to.
    CounterType totalScannedEntryLengths[TOTAL_LOG_ENTRY_TYPES];

    /// The analogue of totalScannedEntryLengths, but only for live objects.
    /// This corresponds to the totalLiveEntriesScanned. Each live entry
    /// recorded there will have its length reflected here.
    CounterType totalLiveScannedEntryLengths[TOTAL_LOG_ENTRY_TYPES];

    /// Total number of cpu cycles spent in doMemoryCleaning().
    CounterType totalTicks;

    /// Total number of cpu cycles spent choosing a segment to compact.
    CounterType getSegmentToCompactTicks;

    /// Total number of cpu cycles spent waiting for a free survivor segment.
    CounterType waitForFreeSurvivorTicks;

    /// Total number of cpu cycles spent in the relocation callback. Note that
    /// this will include time spent appending to the survivor segment if the
    /// entry needed to be relocated.
    CounterType relocationCallbackTicks;

    /// Total number of cpu cycles spent appending relocated entries.
    CounterType relocationAppendTicks;

    /// Total number of cpu cycles spent in SegmentManager::compactionComplete.
    CounterType compactionCompleteTicks;
};

/**
 * Metrics for on-disk cleaning.
 */
template<typename CounterType = Atomic64BitType>
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
          totalEmptySegmentsCleaned(0),
          totalSurvivorsCreated(0),
          totalRuns(0),
          totalLowDiskSpaceRuns(0),
          memoryUtilizationAtStartSum(0),
          totalEntriesScanned(),
          totalLiveEntriesScanned(),
          totalScannedEntryLengths(),
          totalLiveScannedEntryLengths(),
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
          closeSurvivorTicks(0),
          survivorSyncTicks(0),
          lastRunTimestamp(0),
          cleanedSegmentMemoryHistogram(101, 1),
          cleanedSegmentDiskHistogram(101, 1),
          allSegmentsDiskHistogram(101, 1)
    {
        memset(totalEntriesScanned, 0, sizeof(totalEntriesScanned));
        memset(totalLiveEntriesScanned, 0, sizeof(totalLiveEntriesScanned));
        memset(totalScannedEntryLengths, 0, sizeof(totalScannedEntryLengths));
        memset(totalLiveScannedEntryLengths, 0,
            sizeof(totalLiveScannedEntryLengths));
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
        m.set_total_empty_segments_cleaned(totalEmptySegmentsCleaned);
        m.set_total_survivors_created(totalSurvivorsCreated);
        m.set_total_runs(totalRuns);
        m.set_total_low_disk_space_runs(totalLowDiskSpaceRuns);
        m.set_memory_utilization_at_start_sum(memoryUtilizationAtStartSum);

        foreach (uint64_t count, totalEntriesScanned)
            m.add_total_entries_scanned(count);
        foreach (uint64_t count, totalLiveEntriesScanned)
            m.add_total_live_entries_scanned(count);
        foreach (uint64_t count, totalScannedEntryLengths)
            m.add_total_scanned_entry_lengths(count);
        foreach (uint64_t count, totalLiveScannedEntryLengths)
            m.add_total_live_scanned_entry_lengths(count);

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
        m.set_close_survivor_ticks(closeSurvivorTicks);
        m.set_survivor_sync_ticks(survivorSyncTicks);
        cleanedSegmentMemoryHistogram.serialize(
            *m.mutable_cleaned_segment_memory_histogram());
        cleanedSegmentDiskHistogram.serialize(
            *m.mutable_cleaned_segment_disk_histogram());
        allSegmentsDiskHistogram.serialize(
            *m.mutable_all_segments_disk_histogram());
    }

    /**
     * Merge counters from ``other'' into this OnDisk instance. This is
     * typically used to merge thread-local counts into global metrics,
     * where the thread-local counters are backed by simple uint64_ts,
     * rather than atomic integers. This allows us to maintain fine-grained
     * metrics without horribly impacting performance by aggregating them
     * less frequently.
     */
    template<typename T>
    void
    merge(OnDisk<T>& other)
    {
#define MERGE_FIELD(_x) _x += other._x;
        MERGE_FIELD(totalBytesAppendedToSurvivors);
        MERGE_FIELD(totalMemoryBytesFreed);
        MERGE_FIELD(totalDiskBytesFreed);
        MERGE_FIELD(totalMemoryBytesInCleanedSegments);
        MERGE_FIELD(totalDiskBytesInCleanedSegments);
        MERGE_FIELD(totalRelocationCallbacks);
        MERGE_FIELD(totalRelocationAppends);
        MERGE_FIELD(totalSegmentsCleaned);
        MERGE_FIELD(totalEmptySegmentsCleaned);
        MERGE_FIELD(totalSurvivorsCreated);
        MERGE_FIELD(totalRuns);
        MERGE_FIELD(totalLowDiskSpaceRuns);
        MERGE_FIELD(memoryUtilizationAtStartSum);

        for (int i = 0; i < TOTAL_LOG_ENTRY_TYPES; i++) {
            MERGE_FIELD(totalEntriesScanned[i]);
            MERGE_FIELD(totalLiveEntriesScanned[i]);
            MERGE_FIELD(totalScannedEntryLengths[i]);
            MERGE_FIELD(totalLiveScannedEntryLengths[i]);
        }

        MERGE_FIELD(totalTicks);
        MERGE_FIELD(getSegmentsToCleanTicks);
        MERGE_FIELD(costBenefitSortTicks);
        MERGE_FIELD(getSortedEntriesTicks);
        MERGE_FIELD(timestampSortTicks);
        MERGE_FIELD(relocateLiveEntriesTicks);
        MERGE_FIELD(waitForFreeSurvivorsTicks);
        MERGE_FIELD(cleaningCompleteTicks);
        MERGE_FIELD(relocationCallbackTicks);
        MERGE_FIELD(relocationAppendTicks);
        MERGE_FIELD(closeSurvivorTicks);
        MERGE_FIELD(survivorSyncTicks);
#undef MERGE_FIELD
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
    CounterType totalBytesAppendedToSurvivors;

    /// Total number of bytes freed in RAM by cleaning (net gain).
    CounterType totalMemoryBytesFreed;

    /// Total number of bytes freed on disk by cleaning (net gain).
    CounterType totalDiskBytesFreed;

    /// Total number of bytes allocated to segments that were cleaned. This is
    /// the amount of space in memory only.
    CounterType totalMemoryBytesInCleanedSegments;

    /// Total number of bytes on disk for segments that were cleaned. This is
    /// equal to the number of segments cleaned times the segment size.
    CounterType totalDiskBytesInCleanedSegments;

    /// Total number of times the entry relocation handler was called.
    CounterType totalRelocationCallbacks;

    /// Total number of successful appends done in relocating a live object.
    /// Appends that weren't successful due to insufficient space would have
    /// bailed quickly and been retried after allocating a new survivor segment.
    CounterType totalRelocationAppends;

    /// Total number of segments the disk cleaner has cleaned.
    CounterType totalSegmentsCleaned;

    /// Total number of segments the disk cleaner has cleaned that contained
    /// no live data at all.
    CounterType totalEmptySegmentsCleaned;

    /// Total number of survivor segments created to relocate live data into.
    CounterType totalSurvivorsCreated;

    /// Total number of disk cleaner runs. That is, the number of times the
    /// disk cleaner did some work (chose some segments and relocated their
    /// live data to survivors).
    CounterType totalRuns;

    /// Total number of disk cleaner runs that were initiated because we ran
    /// out of disk space (rather than ran out of memory).
    CounterType totalLowDiskSpaceRuns;

    /// Sum of the integer % of memory (0 to 100) utilized at the start of
    /// each disk cleaning pass. Divide by totalRuns to get the average. This
    /// helps disambiguate cases where the cleaner is keeping up at cleaning
    /// just at the threshold and cases where it is not keeping up and cleaning
    /// at near 100% (the latter is more efficient, but stalls clients more).
    CounterType memoryUtilizationAtStartSum;

    /// Total number of each log entry the disk cleaner has encountered while
    /// cleaning segments. These counts include both dead and alive entries.
    CounterType totalEntriesScanned[TOTAL_LOG_ENTRY_TYPES];

    /// Total number of each log entry the disk cleaner has encountered while
    /// cleaning segments. These counts only include live entries that were
    /// relocated. Take the difference from totalEntriesScanned to get the
    /// dead entry count.
    CounterType totalLiveEntriesScanned[TOTAL_LOG_ENTRY_TYPES];

    /// Total number of bytes in each log entry the disk cleaner has encountered
    /// while cleaning segments. This includes both dead and alive entries.
    /// Each time a count in totalEntriesScanned is incremented, the entry's
    /// corresponding length is added here. This way we can compute both the
    /// percentage of total entries of each type we see, as well as the ratio of
    /// total bytes they correspond to.
    CounterType totalScannedEntryLengths[TOTAL_LOG_ENTRY_TYPES];

    /// The analogue of totalScannedEntryLengths, but only for live objects.
    /// This corresponds to the totalLiveEntriesScanned. Each live entry
    /// recorded there will have its length reflected here.
    CounterType totalLiveScannedEntryLengths[TOTAL_LOG_ENTRY_TYPES];

    /// Total number of cpu cycles spent in doDiskCleaning().
    CounterType totalTicks;

    /// Total number of cpu cycles spent in getSegmentsToClean().
    CounterType getSegmentsToCleanTicks;

    /// Total number of cpu cycles spent sorting candidate segments by best
    /// cost-benefit.
    CounterType costBenefitSortTicks;

    /// Total number of cpu cycles spent in getSortedEntries().
    CounterType getSortedEntriesTicks;

    /// Total number of cpu cycles spent sorting entries from segments being
    /// cleaned according to their timestamp.
    CounterType timestampSortTicks;

    /// Total number of cpu cycles spent in relocateLiveEntries().
    CounterType relocateLiveEntriesTicks;

    /// Total number of cpu cycles waiting for sufficient survivor segments.
    CounterType waitForFreeSurvivorsTicks;

    /// Total number of cpu cycles spent in SegmentManager::cleaningComplete().
    CounterType cleaningCompleteTicks;

    /// Total number of cpu cycles spent in the relocation callback. Note that
    /// this will include time spent appending to the survivor segment if the
    /// entry needed to be relocated.
    CounterType relocationCallbackTicks;

    /// Total number of cpu cycles spent appending relocated entries.
    CounterType relocationAppendTicks;

    /// Total number of cpu cycles spent in the closeSurvivor() method, which
    /// closes both the Segment and ReplicatedSegment (and initiates transfer
    /// to backups).
    CounterType closeSurvivorTicks;

    /// Total number of cpu cycles spent syncing survivor segments to backups.
    CounterType survivorSyncTicks;

    /// WallTime timestamp when the disk cleaner last completed a run.
    CounterType lastRunTimestamp;

    /// Histogram of memory utilizations for segments cleaned on disk.
    /// This lets us see how frequency we clean segments with varying amounts
    /// of live data.
    Histogram cleanedSegmentMemoryHistogram;

    /// Histogram of disk space utilizations for segments cleaned on disk.
    Histogram cleanedSegmentDiskHistogram;

    /// Histogram of disk space utilizations for all segments prior to running
    /// a disk cleaner pass. Includes segments chosen to clean in that pass.
    Histogram allSegmentsDiskHistogram;
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
    Tub<CycleCounter<uint64_t>> cycleCounter;
};

} // namespace LogCleanerMetrics

} // namespace RAMCloud

#endif // !RAMCLOUD_LOGCLEANERMETRICS_H
