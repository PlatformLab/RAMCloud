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

#include <boost/thread.hpp>
#include <vector>

#include "Common.h"
#include "BackupManager.h"
#include "LogTypes.h"
#include "Log.h"
#include "Segment.h"

namespace RAMCloud {

// forward decl around the circular Log/LogCleaner dependency
class Log;

class LogCleaner {
  public:
    explicit LogCleaner(Log* log, BackupManager* backup, bool startThread);
    ~LogCleaner();
    bool clean();
    void halt();

  PRIVATE:
    // Sorting a vector of handles is expensive since we have to take a few
    // cache misses to access the ages for each comparison. Instead, include
    // the age directly in the elements being sorted. We access the object
    // anyhow as we scan (to check liveness), so this is essentially free.
    // The sorted array is 50% larger, but comparing elements is much faster.
    typedef std::pair<SegmentEntryHandle, uint32_t> SegmentEntryHandleAndAge;
    typedef std::vector<SegmentEntryHandleAndAge> SegmentEntryHandleVector;

    /**
     * CostBenefit comparison functor for a vector of Segment pointers.
     * This is used to sort our array of cleanable Segments based on each
     * one's associated cost-benefit calculation. Higher values (the better
     * candidates) come last. This lets us easily remove them by popping
     * the back, rather than pulling from the front and shifting all elements
     * down.
     */
    struct CostBenefitLessThan {
      public:
        CostBenefitLessThan()
            : now(secondsTimestamp())
        {
        }

        uint64_t
        costBenefit(Segment *s)
        {
            uint64_t costBenefit = ~(0UL);       // empty Segments are priceless

            int utilisation = s->getUtilisation();
            if (utilisation != 0) {
                uint64_t timestamp = s->getAverageTimestamp();

                // Mathematically this should be assured, however, a few issues
                // can potentially pop up:
                //  1) improper synchronisation in Segment.cc
                //  2) unsynchronised clocks and "newer" recovered data in the
                //     Log
                //  3) unsynchronised TSCs (WallTime uses rdtsc)
                assert(timestamp <= now);

                uint64_t age = now - timestamp;
                costBenefit = ((100 - utilisation) * age) / utilisation;
            }

            return costBenefit;
        }

        bool
        operator()(Segment* a, Segment* b)
        {
            return costBenefit(a) < costBenefit(b);
        }

      private:
        uint64_t now;
    };

    /**
     * Comparison functor that is used to sort Segment entries based on their
     * age (timestamp). Older entries come first (i.e. those with lower
     * timestamps).
     */
    struct SegmentEntryAgeLessThan {
      public:
        explicit SegmentEntryAgeLessThan(Log* log)
            : log(log)
        {
        }

        bool
        operator()(const LogCleaner::SegmentEntryHandleAndAge a,
                   const LogCleaner::SegmentEntryHandleAndAge b)
        {
            return a.second < b.second;
        }

      private:
        Log* log;
    };

    /**
     * Comparison functor used to sort Segments by their IDs. Higher (newer)
     * IDs come first.
     */
    struct SegmentIdLessThan {
      public:
        bool
        operator()(const Segment* a, const Segment* b)
        {
            return a->getId() > b->getId();
        }
    };

    /**
     * Various counters to measure performance of the cleaner. This mostly
     * consists of ticks spent in various bits of code.
     *
     * XXX- This should be part of RawMetrics someday, but the difference
     * (operator-) functionality is very useful and we don't yet snapshot
     * RawMetrics.
     */
    class PerfCounters {
      public:
        PerfCounters()
            : scanTicks(0), getSegmentsTicks(0), collectLiveEntriesTicks(0),
              sortLiveEntriesTicks(0), moveLiveDataTicks(0), packPriorTicks(0),
              packLastTicks(0), closeAndSyncTicks(0), cleaningCompleteTicks(0),
              cleanTicks(0), segmentsGenerated(0), segmentsCleaned(0)
        {
        }

        /**
         * Get the difference between two PerfCounter objects. Simply
         * takes the difference of all internal counters. Useful when
         * you want just the counters for a more recent period of time,
         * rather than since the beginning.
         */
        PerfCounters
        operator-(const PerfCounters& other)
        {
            PerfCounters ret;

            #define _sub(_x) ret._x = _x - other._x
            _sub(scanTicks);
            _sub(getSegmentsTicks);
            _sub(collectLiveEntriesTicks);
            _sub(sortLiveEntriesTicks);
            _sub(moveLiveDataTicks);
            _sub(packPriorTicks);
            _sub(packLastTicks);
            _sub(closeAndSyncTicks);
            _sub(cleaningCompleteTicks);
            _sub(cleanTicks);
            _sub(segmentsGenerated);
            _sub(segmentsCleaned);
            #undef _sub

            return ret;
        }

        uint64_t scanTicks;                 /// Time in scanSegment().
        uint64_t getSegmentsTicks;          /// Time in getSegmentsToClean().
        uint64_t collectLiveEntriesTicks;   /// Part of getSortedLiveEntries().
        uint64_t sortLiveEntriesTicks;      /// Rest of getSortedLiveEntries().
        uint64_t moveLiveDataTicks;         /// Total time in moveLiveData().
        uint64_t packPriorTicks;            /// Optimisation in moveLiveData().
        uint64_t packLastTicks;             /// Time in moveToFillSegment().
        uint64_t closeAndSyncTicks;         /// MoveLiveData() close/sync time.
        uint64_t cleaningCompleteTicks;     /// Time in Log::cleaningComplete().
        uint64_t cleanTicks;                /// Time in clean().
        uint64_t segmentsGenerated;         /// New segments with live data.
        uint64_t segmentsCleaned;           /// Clean segments produced.
    };

    // cleaner thread entry point
    static void cleanerThreadEntry(LogCleaner* logCleaner, Context* context);

    void dumpCleaningPassStats(PerfCounters& before);
    void scanNewCleanableSegments();
    void scanSegment(Segment* segment);
    void getSegmentsToClean(SegmentVector&);
    void getSortedLiveEntries(SegmentVector& segments,
                              SegmentEntryHandleVector& liveEntries);
    uint32_t moveToFillSegment(Segment* lastNewSegment,
                               SegmentVector& segmentsToClean);
    void moveLiveData(SegmentEntryHandleVector& data,
                      SegmentVector& segmentsToClean);

    /// After cleaning, wake the cleaner again after this many microseconds.
    static const size_t CLEANER_POLL_USEC = 50000;

    /// Number of clean Segments to produce in each cleaning pass.
    static const size_t CLEANED_SEGMENTS_PER_PASS = 10;

    /// Maximum write cost we'll permit. Anything higher and we won't clean.
    static const double MAXIMUM_CLEANABLE_WRITE_COST = 3.0;

    /// Enable optimisation that tries to pack objects into prior new Segments
    /// before trying the latest one.
    static const bool packPriorOptimisation = true;

    /// Enable optimisation that packs the last Segment the cleaner relocates
    /// to.
    static const bool packLastOptimisation = true;

    /// The number of bytes that have been freed in the Log since the last
    /// cleaning operation completed. This is used to avoid invoking the
    /// cleaner if there isn't likely any work to be done.
    uint64_t        bytesFreedBeforeLastCleaning;

    /// Segments that the Log considers cleanable, but which haven't been
    /// scanned yet (i.e. the scan callback has not been called on each
    /// entry. This list originally existed for asynchronous updates to
    /// TabletProfiler structures, but the general callback may serve
    /// arbitrary purposes for whoever registered a log type.
    SegmentVector   scanList;

    /// Segments are scanned in precise order of SegmentId. This integer
    /// tracks the next SegmentId to be scanned. The assumption is that
    /// the Log begins at ID 0.
    uint64_t        nextScannedSegmentId;

    /// Closed segments that are part of the Log - these may be cleaned
    /// at any time. Only Segments that have been scanned (i.e. previously
    /// were on the #scanList can be added here.
    SegmentVector   cleanableSegments;

    /// The Log we're cleaning.
    Log*            log;

    /// Our own private BackupManager (not the Log's). BackupManager isn't
    /// reentrant, and there's little reason for it to be, so use this one
    // to manage the Segments we create while cleaning.
    BackupManager*  backup;

    // Tub containing our cleaning thread, if we're told to instantiate one
    // by whoever constructs this object.
    Tub<boost::thread> thread;

    // Current performance counters.
    PerfCounters perfCounters;

    friend class LogCleanerTest;

    DISALLOW_COPY_AND_ASSIGN(LogCleaner);
};

} // namespace

#endif // !RAMCLOUD_LOGCLEANER_H
