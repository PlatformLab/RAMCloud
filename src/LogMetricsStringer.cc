/* Copyright (c) 2013-2015 Stanford University
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

/**
 * \file
 * This module groks the LogMetrics protocol buffer returned from a server and
 * returns various multi-line, human-readable strings that can be dumped to the
 * console to study how it is behaving.
 */

#include "Common.h"
#include "CycleCounter.h"
#include "Histogram.h"
#include "LogMetricsStringer.h"

namespace RAMCloud {

///////////////////
/// Public Methods
///////////////////

LogMetricsStringer::LogMetricsStringer(ProtoBuf::LogMetrics* logMetrics,
                                       ProtoBuf::ServerConfig* serverConfig,
                                       double elapsedTime,
                                       string lineStarter)
    : logMetrics(logMetrics)
    , logMetricsTub()
    , serverConfig(serverConfig)
    , serverConfigTub()
    , elapsedTime(elapsedTime)
    , ls(lineStarter)
{
}

LogMetricsStringer::LogMetricsStringer(RamCloud* ramcloud,
                                       string masterLocator,
                                       double elapsedTime,
                                       string lineStarter)
    : logMetrics(NULL)
    , logMetricsTub()
    , serverConfig(NULL)
    , serverConfigTub()
    , elapsedTime(elapsedTime)
    , ls(lineStarter)
{
    logMetricsTub.construct();
    logMetrics = logMetricsTub.get();
    ramcloud->getLogMetrics(masterLocator.c_str(), *logMetrics);

    serverConfigTub.construct();
    serverConfig = serverConfigTub.get();
    ramcloud->getServerConfig(masterLocator.c_str(), *serverConfig);
}

/**
 * XXX
 */
string
LogMetricsStringer::getServerParameters()
{
    string s;

    s += ls + format("===> SERVER PARAMETERS\n");

    s += ls + format("  Locator:                       %s\n",
        serverConfig->local_locator().c_str());

    uint64_t logSize = logMetrics->seglet_metrics().total_usable_seglets() *
                       serverConfig->seglet_size();
    s += ls + format("  Usable Log Size:               %lu MB\n",
        logSize / 1024 / 1024);

    s += ls + format("    Total Allocated:             %lu MB\n",
        serverConfig->master().log_bytes() / 1024 / 1024);

    s += ls + format("  Hash Table Size:               %lu MB\n",
        serverConfig->master().hash_table_bytes() / 1024 / 1024);

    s += ls + format("  Segment Size:                  %d\n",
        serverConfig->segment_size());

    s += ls + format("  Seglet Size:                   %d\n",
        serverConfig->seglet_size());

    s += ls + format("  WC Threshold:                  %d\n",
        serverConfig->master().cleaner_write_cost_threshold());

    s += ls + format("  Replication Factor:            %d\n",
        serverConfig->master().num_replicas());

    s += ls + format("  Disk Expansion Factor:         %.3f\n",
        serverConfig->master().backup_disk_expansion_factor());

    s += ls + format("  Log Cleaner:                   %s\n",
        (serverConfig->master().disable_log_cleaner()) ? "disabled" :
                                                         "enabled");

    s += ls + format("  In-memory Cleaner:             %s\n",
        (serverConfig->master().disable_in_memory_cleaning()) ?
            "disabled" : "enabled");

    s += ls + format("  Cleaner Threads:               %u\n",
        serverConfig->master().cleaner_thread_count());

    s += ls + format("  Cleaner Balancer:              %s\n",
        serverConfig->master().cleaner_balancer().c_str());

    s += ls + format("===> LOG CONSTANTS:\n");

    s += ls + format("  Poll Interval:                 %d us\n",
        logMetrics->cleaner_metrics().poll_usec());

    s += ls + format("  Max Utilization:               %d\n",
        logMetrics->cleaner_metrics().max_cleanable_memory_utilization());
    s += ls + format("  Live Segments per Pass:        %d\n",
        logMetrics->cleaner_metrics().live_segments_per_disk_pass());

    s += ls + format("  Reserved Survivor Segs:        %d\n",
        logMetrics->cleaner_metrics().survivor_segments_to_reserve());

    s += ls + format("  Min Memory Utilization:        %d\n",
        logMetrics->cleaner_metrics().min_memory_utilization());

    s += ls + format("  Min Disk Utilization:          %d\n",
        logMetrics->cleaner_metrics().min_disk_utilization());

    return s;
}

/**
 * XXX
 */
string
LogMetricsStringer::getGenericCleanerMetrics()
{
    string s = ls + format("===> GENERIC CLEANER METRICS\n");

    const ProtoBuf::LogMetrics_CleanerMetrics& cleanerMetrics =
        logMetrics->cleaner_metrics();

    double serverHz = logMetrics->ticks_per_second();

    s += ls + format("  Total Cleaner Time:            %.3f sec\n",
        Cycles::toSeconds(cleanerMetrics.do_work_ticks(), serverHz));
    s += ls + format("    Time Sleeping:               %.3f sec\n",
        Cycles::toSeconds(cleanerMetrics.do_work_sleep_ticks(), serverHz));

    const ProtoBuf::LogMetrics_CleanerMetrics_ThreadMetrics& threadMetrics =
        cleanerMetrics.thread_metrics();

    uint64_t totalTicks = 0;
    foreach (uint64_t ticks, threadMetrics.active_ticks())
        totalTicks += ticks;
    s += ls + format("  Active Thread Distribution:\n");
    int i = 0;
    foreach (uint64_t ticks, threadMetrics.active_ticks()) {
        s += ls + format("    %3d simultaneous:            %.3f%% of time\n",
            i++, d(ticks) / d(totalTicks) * 100);
    }

    return s;
}

/**
 * XXX
 */
string
LogMetricsStringer::getDiskCleanerMetrics()
{
    string s = ls + format("===> DISK METRICS\n");

    const ProtoBuf::LogMetrics_CleanerMetrics_OnDiskMetrics& onDiskMetrics =
        logMetrics->cleaner_metrics().on_disk_metrics();

    double serverHz = logMetrics->ticks_per_second();
    double cleanerTime = Cycles::toSeconds(onDiskMetrics.total_ticks(),
                                           serverHz);

    uint64_t diskFreed = onDiskMetrics.total_disk_bytes_freed();
    uint64_t memFreed = onDiskMetrics.total_memory_bytes_freed();
    uint64_t wrote = onDiskMetrics.total_bytes_appended_to_survivors();

    s += ls + format("  Duty Cycle:                    %.2f%% (%.2f sec)\n",
        100.0 * cleanerTime / elapsedTime, cleanerTime);

    s += ls + format("  Disk Write Cost:               %.3f\n",
        d(diskFreed + wrote) / d(diskFreed));

    s += ls + format("  Memory Write Cost:             %.3f\n",
        d(memFreed + wrote) / d(memFreed));

    uint64_t totalRuns = onDiskMetrics.total_runs();
    uint64_t totalLowDiskRuns = onDiskMetrics.total_low_disk_space_runs();
    s += ls + format("  Total Runs:                    %lu  (%lu / %.3f%% due "
        "to low disk space)\n",
        totalRuns,
        totalLowDiskRuns,
        100.0 * d(totalLowDiskRuns) / d(totalRuns));


    uint64_t diskBytesInCleanedSegments =
        onDiskMetrics.total_disk_bytes_in_cleaned_segments();
    s += ls + format("  Avg Cleaned Seg Disk Util:     %.2f%%\n",
        100.0 * d(wrote) / d(diskBytesInCleanedSegments));

    uint64_t memoryBytesInCleanedSegments =
        onDiskMetrics.total_memory_bytes_in_cleaned_segments();
    s += ls + format("  Avg Cleaned Seg Memory Util:   %.2f%%\n",
        100.0 * d(wrote) / d(memoryBytesInCleanedSegments));

    uint64_t memoryUtilizationSum =
        onDiskMetrics.memory_utilization_at_start_sum();
    s += ls + format("  Avg Memory Util At Run Start:  %.2f%%\n",
        d(memoryUtilizationSum) / d(totalRuns));

    uint64_t totalCleaned = onDiskMetrics.total_segments_cleaned();
    s += ls + format("  Total Segments Cleaned:        %lu (%.2f/s, "
        "%.2f/s active; %lu empty)\n",
        totalCleaned,
        d(totalCleaned) / elapsedTime,
        d(totalCleaned) / cleanerTime,
        onDiskMetrics.total_empty_segments_cleaned());

    uint64_t survivorsCreated = onDiskMetrics.total_survivors_created();
    s += ls + format("  Total Survivors Created:       %lu (%.2f/s, "
        "%.2f/s active)\n",
        survivorsCreated,
        d(survivorsCreated) / elapsedTime,
        d(survivorsCreated) / cleanerTime);

    s += ls + format("  Avg Time to Clean Segment:     %.2f ms\n",
        cleanerTime / d(totalCleaned) * 1000);

    s += ls + format("  Avg Time per Disk Run:         %.2f ms\n",
        cleanerTime / d(totalRuns) * 1000);

    s += ls + format("  Avg Segs Cleaned per Disk Run: %.2f\n",
        d(totalCleaned) / d(totalRuns));

    s += ls + format("  Avg Survivors per Disk Run:    %.2f\n",
        d(survivorsCreated) / d(totalRuns));

    s += ls + format("  Disk Space Freeing Rate:       %.3f MB/s "
        "(%.3f MB/s active)\n",
        d(diskFreed) / elapsedTime / 1024 / 1024,
        d(diskFreed) / cleanerTime / 1024 / 1024);

    s += ls + format("  Memory Space Freeing Rate:     %.3f MB/s "
        "(%.3f MB/s active)\n",
        d(memFreed) / elapsedTime / 1024 / 1024,
        d(memFreed) / cleanerTime / 1024 / 1024);

    s += ls + format("  Survivor Bytes Written:        %lu (%.3f MB/s, "
        "%.3f MB/s active)\n",
        wrote,
        d(wrote) / elapsedTime / 1024 / 1024,
        d(wrote) / cleanerTime / 1024 / 1024);

    s += ls + getSegmentEntriesScanned(&onDiskMetrics, cleanerTime);

    s += ls + format("  Total Time:                    %.3f sec "
        "(%.2f%% active)\n", cleanerTime, 100.0 * cleanerTime / elapsedTime);

    double chooseTime = Cycles::toSeconds(
        onDiskMetrics.get_segments_to_clean_ticks(), serverHz);
    s += ls + format("    Choose Segments:             %.3f sec "
        "(%.2f%%, %.2f%% active)\n",
        chooseTime,
        100.0 * chooseTime / elapsedTime,
        100.0 * chooseTime / cleanerTime);

    double sortSegmentTime = Cycles::toSeconds(
        onDiskMetrics.cost_benefit_sort_ticks(), serverHz);
    s += ls + format("      Sort Segments:             %.3f sec "
        "(%.2f%%, %.2f%% active)\n",
        sortSegmentTime,
        100.0 * sortSegmentTime / elapsedTime,
        100.0 * sortSegmentTime / cleanerTime);

    double extractEntriesTime = Cycles::toSeconds(
        onDiskMetrics.get_sorted_entries_ticks(), serverHz);
    s += ls + format("    Extract Entries:             %.3f sec "
        "(%.2f%%, %.2f%% active)\n",
        extractEntriesTime,
        100.0 * extractEntriesTime / elapsedTime,
        100.0 * extractEntriesTime / cleanerTime);

    double timestampSortTime = Cycles::toSeconds(
        onDiskMetrics.timestamp_sort_ticks(), serverHz);
    s += ls + format("      Sort Entries:              %.3f sec "
        "(%.2f%%, %.2f%% active)\n",
        timestampSortTime,
        100.0 * timestampSortTime / elapsedTime,
        100.0 * timestampSortTime / cleanerTime);

    double relocateTime = Cycles::toSeconds(
        onDiskMetrics.relocate_live_entries_ticks(), serverHz);
    s += ls + format("    Relocate Entries:            %.3f sec "
        "(%.2f%%, %.2f%% active)\n",
        relocateTime,
        100.0 * relocateTime / elapsedTime,
        100.0 * relocateTime / cleanerTime);

    double waitTime = Cycles::toSeconds(
        onDiskMetrics.wait_for_free_survivors_ticks(), serverHz);
    s += ls + format("      Wait for Free Survivors:   %.3f sec "
        "(%.2f%%, %.2f%% active)\n",
        waitTime,
        100.0 * waitTime / elapsedTime,
        100.0 * waitTime / cleanerTime);

    double callbackTime = Cycles::toSeconds(
        onDiskMetrics.relocation_callback_ticks(), serverHz);
    s += ls + format("      Callbacks:                 %.3f sec "
        "(%.2f%%, %.2f%% active, %.2f us avg)\n",
        callbackTime,
        100.0 * callbackTime / elapsedTime,
        100.0 * callbackTime / cleanerTime,
        1.0e6 * callbackTime / d(onDiskMetrics.total_relocation_callbacks()));

    double appendTime = Cycles::toSeconds(
        onDiskMetrics.relocation_append_ticks(), serverHz);
    s += ls + format("        Segment Appends:         %.3f sec "
        "(%.2f%%, %.2f%% active, %.2f us avg)\n",
        appendTime,
        100.0 * appendTime / elapsedTime,
        100.0 * appendTime / cleanerTime,
        1.0e6 * appendTime / d(onDiskMetrics.total_relocation_appends()));

    double closeTime = Cycles::toSeconds(
        onDiskMetrics.close_survivor_ticks(), serverHz);
    s += ls + format("    Close Survivors:             %.3f sec "
        "(%.2f%%, %.2f%% active)\n",
        closeTime,
        100.0 * closeTime / elapsedTime,
        100.0 * closeTime / cleanerTime);

    double syncTime = Cycles::toSeconds(
        onDiskMetrics.survivor_sync_ticks(), serverHz);
    s += ls + format("    Sync Survivors:              %.3f sec "
        "(%.2f%%, %.2f%% active)\n",
        syncTime,
        100.0 * syncTime / elapsedTime,
        100.0 * syncTime / cleanerTime);

    double completeTime = Cycles::toSeconds(
        onDiskMetrics.cleaning_complete_ticks(), serverHz);
    s += ls + format("    Cleaning Complete:           %.3f sec "
        "(%.2f%%, %.2f%% active)\n",
        completeTime,
        100.0 * completeTime / elapsedTime,
        100.0 * completeTime / cleanerTime);

    return s;
}

/**
 * XXX
 */
string
LogMetricsStringer::getMemoryCompactorMetrics()
{
    string s = ls + format("===> MEMORY METRICS\n");

    const ProtoBuf::LogMetrics_CleanerMetrics_InMemoryMetrics& inMemoryMetrics =
        logMetrics->cleaner_metrics().in_memory_metrics();

    double serverHz = logMetrics->ticks_per_second();
    double cleanerTime = Cycles::toSeconds(inMemoryMetrics.total_ticks(),
                                           serverHz);

    uint64_t freed = inMemoryMetrics.total_bytes_freed();
    uint64_t wrote = inMemoryMetrics.total_bytes_appended_to_survivors();

    s += ls + format("  Duty Cycle:                    %.2f%% (%.2f sec)\n",
        100.0 * cleanerTime / elapsedTime, cleanerTime);

    s += ls + format("  Memory Write Cost:             %.3f\n",
        d(freed + wrote) / d(freed));

    uint64_t segmentsCompacted = inMemoryMetrics.total_segments_compacted();
    s += ls + format("  Total Segments Compacted:      %lu (%.2f/s, "
        "%.2f/s active; %lu empty)\n",
        segmentsCompacted,
        d(segmentsCompacted) / elapsedTime,
        d(segmentsCompacted) / cleanerTime,
        inMemoryMetrics.total_empty_segments_compacted());

    uint64_t bytesInCompactedSegments =
        inMemoryMetrics.total_bytes_in_compacted_segments();
    s += ls + format("  Avg Seg Util Pre-Compaction:   %.2f%%\n",
        100.0 * d(wrote) / d(bytesInCompactedSegments));

    s += ls + format("  Avg Seglets Freed/Compaction:  %.2f\n",
        d(freed) / d(segmentsCompacted) / d(serverConfig->seglet_size()));

    s += ls + format("  Avg Time to Compact Segment:   %.2f ms\n",
        cleanerTime * 1000 / d(segmentsCompacted));

    s += ls + format("  Memory Space Freeing Rate:     %.3f MB/s "
        "(%.3f MB/s active)\n",
        d(freed) / elapsedTime / 1024 / 1024,
        d(freed) / cleanerTime / 1024 / 1024);

    s += ls + format("  Survivor Bytes Written:        %lu "
        "(%.3f MB/s active)\n",
        wrote,
        d(wrote) / cleanerTime / 1024 / 1024);

    s += ls + getSegmentEntriesScanned(&inMemoryMetrics, cleanerTime);

    s += ls + format("  Total Time:                    %.3f sec "
        "(%.2f%% active)\n",
        cleanerTime,
        100.0 * cleanerTime / elapsedTime);

    double chooseTime = Cycles::toSeconds(
        inMemoryMetrics.get_segment_to_compact_ticks(), serverHz);
    s += ls + format("    Choose Segments:             %.3f sec "
        "(%.2f%%, %.2f%% active)\n",
        chooseTime,
        100.0 * chooseTime / elapsedTime,
        100.0 * chooseTime / cleanerTime);

    double waitTime = Cycles::toSeconds(
        inMemoryMetrics.wait_for_free_survivor_ticks(), serverHz);
    s += ls + format("    Wait for Free Survivor:      %.3f sec "
        "(%.2f%%, %.2f%% active)\n",
        waitTime,
        100.0 * waitTime / elapsedTime,
        100.0 * waitTime / cleanerTime);

    double callbackTime = Cycles::toSeconds(
        inMemoryMetrics.relocation_callback_ticks(), serverHz);
    s += ls + format("    Callbacks:                   %.3f sec "
        "(%.2f%%, %.2f%% active, %.2f us avg)\n",
        callbackTime,
        100.0 * callbackTime / elapsedTime,
        100.0 * callbackTime / cleanerTime,
        1.0e6 * callbackTime / d(inMemoryMetrics.total_relocation_callbacks()));

    double appendTime = Cycles::toSeconds(
        inMemoryMetrics.relocation_append_ticks(), serverHz);
    s += ls + format("      Segment Appends:           %.3f sec "
        "(%.2f%%, %.2f%% active, %.2f us avg)\n",
        appendTime,
        100.0 * appendTime / elapsedTime,
        100.0 * appendTime / cleanerTime,
        1.0e6 * appendTime / d(inMemoryMetrics.total_relocation_appends()));

    double compactionCompleteTime = Cycles::toSeconds(
        inMemoryMetrics.compaction_complete_ticks(), serverHz);
    s += ls + format("    Compaction Complete:         %.3f sec "
        "(%.2f%%, %.2f%% active)\n",
        chooseTime,
        100.0 * compactionCompleteTime / elapsedTime,
        100.0 * compactionCompleteTime / cleanerTime);

    return s;
}

/**
 * XXX
 */
string
LogMetricsStringer::getLogMetrics()
{
    string s = ls + format("===> LOG METRICS\n");

    double serverHz = logMetrics->ticks_per_second();

    s += ls + format("  Total Non-metadata Appends:    %.2f MB\n",
        d(logMetrics->total_bytes_appended()) / 1024 / 1024);

    s += ls + format("  Total Metadata Appends:        %.2f MB\n",
        d(logMetrics->total_metadata_bytes_appended()) / 1024 / 1024);

    double appendTime = Cycles::toSeconds(logMetrics->total_append_ticks(),
                                          serverHz);
    s += ls + format("  Total Time Appending:          %.3f sec (%.2f%%)\n",
        appendTime,
        100.0 * appendTime / elapsedTime);

    double syncTime = Cycles::toSeconds(logMetrics->total_sync_ticks(),
                                        serverHz);
    s += ls + format("  Total Time Syncing:            %.3f sec (%.2f%%)\n",
        syncTime, 100.0 * syncTime / elapsedTime);
    s += ls + format("    Avg Per Operation (RPC):     %.2f us\n",
        syncTime * 1.0e6 / d(logMetrics->total_sync_calls()));

    double noMemTime = Cycles::toSeconds(logMetrics->total_no_space_ticks(),
                                         serverHz);
    s += ls + format("  Time Out of Memory:            %.3f sec (%.2f%%)\n",
        noMemTime, 100.0 * noMemTime / elapsedTime);

    s += ls + format("  Total (Dead or Alive) Entry Counts in All Segments:\n");

    const ProtoBuf::LogMetrics_SegmentMetrics& sm =
        logMetrics->segment_metrics();
    uint64_t totalCount = 0;
    uint64_t totalLength = 0;
    foreach (uint64_t count, sm.total_entry_counts())
        totalCount += count;
    foreach (uint64_t length, sm.total_entry_lengths())
        totalLength += length;

    for (int i = 0; i < sm.total_entry_counts_size(); i++) {
        s += ls + format("    %-26.26s   %5.2f%%  (%5.2f%% space)\n",
            LogEntryTypeHelpers::toString(static_cast<LogEntryType>(i)),
                d(sm.total_entry_counts(i)) / d(totalCount) * 100.0,
                d(sm.total_entry_lengths(i)) / d(totalLength) * 100.0);
    }

    Histogram segmentsOnDisk(
        logMetrics->segment_metrics().segments_on_disk_histogram());
    uint64_t logSegments = logMetrics->seglet_metrics().total_usable_seglets() *
        serverConfig->seglet_size() / serverConfig->segment_size();
    s += ls + format("  Avg Number of Segs on Disk:    %lu "
        "(%.2f%% of total in memory)\n",
        segmentsOnDisk.getAverage(),
        100 * static_cast<double>(segmentsOnDisk.getAverage()) /
          static_cast<double>(logSegments));

    uint64_t onDisk = logMetrics->segment_metrics().current_segments_on_disk();
    s += ls + format("    Last Count:                  %lu "
        "(%.2f%%)\n",
        onDisk,
        100 * static_cast<double>(onDisk) / static_cast<double>(logSegments));

    return s;
}

} // namespace RAMCloud
