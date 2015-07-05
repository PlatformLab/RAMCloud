/* Copyright (c) 2014 Stanford University
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

#include <vector>
#include "Common.h"
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

    /// Total number of RAMCloud objects written (each object in a multi-write
    /// operation counts as one).
    uint64_t writeCount;

    /// Total time (in Cycles::rdtsc ticks) spent in calls to Dispatch::poll
    /// that did useful work (if a call to Dispatch::poll found no useful
    /// work, then it's execution time is excluded).
    uint64_t dispatchActiveCycles;

    /// Total time (in Cycles::rdtsc ticks) spent by executing RPC requests
    /// as a worker.
    uint64_t workerActiveCycles;

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
    // Statistics for the dispatch thread follow below.
    //--------------------------------------------------------------------

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

    static void collectStats(PerfStats* total);
    static void registerStats(PerfStats* stats);

    /// The following thread-local variable is used to access the statistics
    /// for the current thread.
    static __thread PerfStats threadStats;

  PRIVATE:

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
