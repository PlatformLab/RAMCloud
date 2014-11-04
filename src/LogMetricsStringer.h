/* Copyright (c) 2013 Stanford University
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

#ifndef RAMCLOUD_LOGMETRICSSTRINGER_H
#define RAMCLOUD_LOGMETRICSSTRINGER_H

#include <thread>
#include <vector>

#include "Common.h"
#include "RamCloud.h"
#include "LogEntryTypes.h"
#include "LogMetrics.pb.h"
#include "ServerConfig.pb.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * XXX
 */
class LogMetricsStringer {
  public:
    LogMetricsStringer(ProtoBuf::LogMetrics* logMetrics,
                       ProtoBuf::ServerConfig* serverConfig,
                       double elapsedTime,
                       string lineStarter = "");
    LogMetricsStringer(RamCloud* ramcloud,
                       string masterLocator,
                       double elapsedTime,
                       string lineStarter = "");
    string getServerParameters();
    string getGenericCleanerMetrics();
    string getDiskCleanerMetrics();
    string getMemoryCompactorMetrics();
    string getLogMetrics();

  PRIVATE:
    /**
     * Shorthand for static_cast<double>ing.
     */
    template<typename T>
    static double
    d(T value)
    {
        return static_cast<double>(value);
    }

    /**
     * XXX
     */
    template<typename T>
    string
    getSegmentEntriesScanned(T* metrics, double cleanerTime)
    {
        string s;

        uint64_t totalEntriesScanned = 0;
        foreach (uint64_t count, metrics->total_entries_scanned())
            totalEntriesScanned += count;

        uint64_t totalLiveEntriesScanned = 0;
        foreach (uint64_t count, metrics->total_entries_scanned())
            totalLiveEntriesScanned += count;

        uint64_t totalScannedEntryLengths = 0;
        foreach (uint64_t length, metrics->total_scanned_entry_lengths())
            totalScannedEntryLengths += length;

        uint64_t totalLiveScannedEntryLengths = 0;
        foreach (uint64_t length, metrics->total_live_scanned_entry_lengths())
            totalLiveScannedEntryLengths += length;

        s += format("  Segment Entries Scanned:       %lu (%.2f/sec, "
            "%.2f/sec active)\n",
            totalEntriesScanned,
            d(totalEntriesScanned) / elapsedTime,
            d(totalEntriesScanned) / cleanerTime);
        s += format("    Summary:\n");
        s += format("      Type                       %% Total  (Space)  "
            "%% Alive  (Space)   %% Dead  (Space)\n");

        for (int i = 0; i < metrics->total_entries_scanned_size(); i++) {
            uint64_t totalCount = metrics->total_entries_scanned(i);
            uint64_t totalLengths = metrics->total_scanned_entry_lengths(i);
            uint64_t liveCount = metrics->total_live_entries_scanned(i);
            uint64_t liveLengths = metrics->total_live_scanned_entry_lengths(i);
            uint64_t deadCount = totalCount - liveCount;
            uint64_t deadLengths = totalLengths - liveLengths;

            if (totalCount == 0)
                continue;

            s += format("      %-26.26s %6.2f%% (%6.2f%%) %6.2f%% (%6.2f%%) "
                "%6.2f%% (%6.2f%%)\n",
                LogEntryTypeHelpers::toString(static_cast<LogEntryType>(i)),
                d(totalCount) / d(totalEntriesScanned) * 100,
                d(totalLengths) / d(totalScannedEntryLengths) * 100,
                d(liveCount) / d(totalCount) * 100,
                d(liveLengths) / d(totalScannedEntryLengths) * 100,
                d(deadCount) / d(totalCount) * 100,
                d(deadLengths) / d(totalScannedEntryLengths) * 100);
        }

        return s;
    }

    /// Pointer to the LogMetrics instance we'll pull various metrics from.
    ProtoBuf::LogMetrics* logMetrics;

    /// Used when we fetch the metrics in the alternate constructor.
    /// When constructed, logMetrics points to this.
    Tub<ProtoBuf::LogMetrics> logMetricsTub;

    /// Pointer to the ServerConfig instance we'll pull a few important server
    /// constants from (like seglet size, for example).
    ProtoBuf::ServerConfig* serverConfig;

    /// Used when we fetch the configuration in the alternate constructor.
    /// When constructed, serverConfig points to this.
    Tub<ProtoBuf::ServerConfig> serverConfigTub;

    /// User-specified elapsed time that we'll divide various values from to get
    /// average rates, etc. We could use the server's uptime value, but this is
    /// not always what we want, as some measurements may start later on (like
    /// once cleaning has first kicked in).
    double elapsedTime;

    /// The 'lineStarter' parameter given to the constructor (shortened to avoid
    /// superfluous linewrapping). This is prepended to the beginning of every
    /// line of output. It can be set to "#" to make all output look like
    /// comments, for example, so they can be injected directly into benchmark
    /// output, rather than having to go in a separate file.
    string ls;

    DISALLOW_COPY_AND_ASSIGN(LogMetricsStringer);
};

} // namespace

#endif // !RAMCLOUD_LOGMETRICSSTRINGER_H
