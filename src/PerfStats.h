/* Copyright (c) 2014-2015 Stanford University
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

#ifndef RAMCLOUD_PERFSTATS_H
#define RAMCLOUD_PERFSTATS_H

#include <unordered_map>
#include <vector>
#include "Buffer.h"
#include "SpinLock.h"

namespace RAMCloud {

/**
 * An object of this class records various performance-related information.
 * Each server thread has a private instance of this object, which eliminates
 * cash conflicts when updating statistics and makes the class thread-safe.
 * In addition, an object of this class is returned by collectStats, which
 * aggregates the statistics from all of the individual threads (this is
 * used, for example, as a result of the GET_PERF_STATS server control).
 * In order for aggregation to work, each thread must invoke the
 * registerStats method.
 *
 * If you add a new metric, be sure to update all of the relevant methods
 * in PerfStats.cc. For example, search for all of the places where
 * "collectionTime" appears and add appropriate lines for the new metric.
 *
 * This class should eventually replace RawMetrics because it is more
 * efficient (due to its use of thread-local structures).
 */
struct PerfStats {
    /// Unique identifier for this thread (threads are numbered starting
    /// at 1); Only used in thread-local instances (0 means this object
    /// contains aggregate statistics). This field isn't typically
    /// needed for gathering performance stats, but this is a convenient
    /// place to put it, since it's readily available in all threads.
    int threadId;

    /// Time (in cycles) when the statistics were gathered (only
    /// present in aggregate statistics, not in thread-local instances).
    uint64_t collectionTime;

    /// Conversion factor from collectionTime to seconds (only present
    /// in aggregate statistics, not in thread-local instances).
    double cyclesPerSecond;

    /// Total number of RAMCloud objects read (each object in a multi-read
    /// operation counts as one).
    uint64_t readCount;

    /// Total number of bytes in objects read (includes just actual object
    /// bytes; no keys or other metadata).
    uint64_t readObjectBytes;

    /// Total number of bytes in all keys for objects read (includes key
    /// metadata).
    uint64_t readKeyBytes;

    /// Total number of RAMCloud objects written (each object in a multi-write
    /// operation counts as one).
    uint64_t writeCount;

    /// Total number of bytes in objects written (includes just actual object
    /// bytes; no keys or other metadata).
    uint64_t writeObjectBytes;

    /// Total number of bytes in all keys for objects written (includes key
    /// metadata).
    uint64_t writeKeyBytes;

    /// Total time (in Cycles::rdtsc ticks) spent in calls to Dispatch::poll
    /// that did useful work (if a call to Dispatch::poll found no useful
    /// work, then it's execution time is excluded).
    uint64_t dispatchActiveCycles;

    /// Total time (in Cycles::rdtsc ticks) spent by executing RPC requests
    /// as a worker.
    uint64_t workerActiveCycles;

    //--------------------------------------------------------------------
    // Statistics for log replication follow below. These metrics are
    // related to new information appended to the head segment (i.e., not
    // including cleaning).
    //--------------------------------------------------------------------
    /// Total bytes appended to the log head.
    uint64_t logBytesAppended;

    /// Number of replication RPCs made to the primary replica of a head
    /// segment.
    uint64_t replicationRpcs;

    /// Total time (in cycles) spent by worker threads waiting for log
    /// syncs (i.e. if 2 threads are waiting at once, this counter advances
    /// at twice real time).
    uint64_t logSyncCycles;

    /// Total time (in cycles) spend my segments in a state where they have
    /// at least one replica that has not yet been successfully opened. If
    /// this value is significant, it probably means that backups don't have
    /// enough I/O bandwidth to keep up with replication traffic, so they are
    /// rejecting open requests.
    uint64_t segmentUnopenedCycles;

    //--------------------------------------------------------------------
    // Statistics for the log cleaner follow below.
    //--------------------------------------------------------------------
    /// Total number of bytes read by the log compactor.
    uint64_t compactorInputBytes;

    /// Total bytes of live data copied to new segments by the log compactor.
    uint64_t compactorSurvivorBytes;

    /// Total time (in Cycles::rdtsc ticks) spent executing the compactor.
    uint64_t compactorActiveCycles;

    /// Total number of memory bytes that were cleaned by the combined
    /// cleaner (i.e., the combined sizes of all the in-memory segments that
    /// were freed after their contents were cleaned and live data
    /// written to new segments).
    uint64_t cleanerInputMemoryBytes;

    /// Total number of disk bytes that were cleaned by the combined cleaner
    /// (these bytes were not actually read, since the cleaner operates on
    /// in-memory information).
    uint64_t cleanerInputDiskBytes;

    /// Total bytes of live data copied to new segments by the combined
    /// cleaner.
    uint64_t cleanerSurvivorBytes;

    /// Total time (in Cycles::rdtsc ticks) spent executing the combined
    /// cleaner.
    uint64_t cleanerActiveCycles;

    //--------------------------------------------------------------------
    // Statistics for backup I/O follow below.
    //--------------------------------------------------------------------

    /// Total number of read operations from secondary storage.
    uint64_t backupReadOps;

    /// Total bytes of data read from secondary storage.
    uint64_t backupReadBytes;

    /// Total time (in Cycles::rdtsc ticks) during which secondary
    /// storage device(s) were actively performing backup reads.
    uint64_t backupReadActiveCycles;

    /// Total bytes of data appended to backup segments.
    uint64_t backupBytesReceived;

    /// Total number of write operations from secondary storage.
    uint64_t backupWriteOps;

    /// Total bytes of data written to secondary storage. This will be
    /// more than backupBytesReceived because I/O is rounded up to even
    /// numbers of blocks (see SingleFileStorage::BLOCK_SIZE).
    uint64_t backupWriteBytes;

    /// Total time (in Cycles::rdtsc ticks) during which secondary
    /// storage device(s) were actively performing backup writes.
    uint64_t backupWriteActiveCycles;

    //--------------------------------------------------------------------
    // Statistics for the network follow below.
    //--------------------------------------------------------------------

    /// Total bytes received from the network via all transports.
    uint64_t networkInputBytes;

    /// Total bytes transmitted on the network by all transports.
    uint64_t networkOutputBytes;

    //--------------------------------------------------------------------
    // Temporary counters. The values below have no pre-defined use;
    // they are intended for temporary use during debugging or performance
    // analysis. Committed code in the repo should not set these counters.
    //--------------------------------------------------------------------
    uint64_t temp1;
    uint64_t temp2;
    uint64_t temp3;
    uint64_t temp4;
    uint64_t temp5;

    //--------------------------------------------------------------------
    // Miscellaneous information
    //--------------------------------------------------------------------

    /// Objects of this type hold the differences between two PerfStats
    /// readings for a collection of servers. Each entry in the
    /// unordered_map holds all of the data for one PerfStats value,
    /// such as "networkOutputBytes", and the key is the name from
    /// the structure above. Each of the values in the unordered_map holds
    /// one number for each server, sorted by increasing server id. The
    /// value is the difference between "before" and "after" readings of
    /// that metric on they are given server. Two metrics are handled
    /// specially:
    /// * No difference is computed for cyclesPerSecond; instead, the "before"
    ///   value is used verbatim.
    /// * The unordered_map will contain an additional vector named
    ///   "serverId", whose values are the indexNumbers of the ServerId
    ///   for each of the servers.
    typedef std::unordered_map<string, std::vector<double>> Diff;

    static string formatMetric(Diff* diff, const char* metric,
            const char* formatString, double scale = 1.0);
    static string formatMetricRate(Diff* diff, const char* metric,
            const char* formatString, double scale = 1.0);
    static string formatMetricRatio(Diff* diff, const char* metric1,
            const char* metric2, const char* formatString, double scale = 1.0);
    static void clusterDiff(Buffer* before, Buffer* after,
            PerfStats::Diff* diff);
    static void collectStats(PerfStats* total);
    static string printClusterStats(Buffer* first, Buffer* second);
    static void registerStats(PerfStats* stats);

    /// The following thread-local variable is used to access the statistics
    /// for the current thread.
    static __thread PerfStats threadStats;

  PRIVATE:
    static void parseStats(Buffer* rawData, std::vector<PerfStats>* results);

    /// Used in a monitor-style fashion for mutual exclusion.
    static SpinLock mutex;

    /// Keeps track of all the PerfStat structures that have been passed
    /// to registerStats (e.g. the different thread-local structures for
    /// each thread). This allows us to find all of the structures to
    /// aggregate their statistics in collectStats.
    static std::vector<PerfStats*> registeredStats;

    /// Next value to assign for the threadId member variable.  Used only
    /// by RegisterStats.
    static int nextThreadId;
};

} // end RAMCloud

#endif  // RAMCLOUD_PERFSTATS_H
