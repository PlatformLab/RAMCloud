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
 * An object of this class records various performance-related information
 * for a single server thread; each thread has a private instance, so that
 * there are no cache conflicts when updating the statistics. This class is
 * thread-safe.
 */
struct PerfStats {
    /// The following thread-local variable is used to access the statistics
    /// for the current thread.
    static __thread PerfStats threadStats;

    /// Total number of RAMCloud objects read by this thread (e.g., each object
    /// in a multi-read operation counts as one).
    uint64_t readCount;

    /// Total number of RAMCloud objects written by this thread (e.g., each
    /// object in a multi-write operation counts as one).
    uint64_t writeCount;

    /// Total time (in Cycles::rdtsc ticks) spent by this thread executing
    /// RPC requests.
    uint64_t activeCycles;

    /// Set in aggregate statistics by collectStats to indicate the server
    /// time (in cycles) when the statistics were gathered.
    uint64_t collectionTime;

    /// Conversion factor from collectionTime to seconds: recorded by
    /// collectStats in its result.
    double cyclesPerSecond;

    static void collectStats(PerfStats* total);
    static void registerStats(PerfStats* stats);

  PRIVATE:

    /// Used in a monitor-style fashion for mutual exclusion.
    static SpinLock mutex;

    /// Keeps track of all the PerfStat structures that have been passed
    /// to registerStats (e.g. the different thread-local structures for
    /// each thread). This allows us to find all of the structures to
    /// aggregate their statistics in collectStats.
    static std::vector<PerfStats*> registeredStats;
};

} // end RAMCloud

#endif  // RAMCLOUD_PERFSTATS_H
