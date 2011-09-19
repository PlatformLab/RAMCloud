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
    // The sorted array is larger, but comparing elements is much faster.
    // While we're here, also cache the total length for future prefetching.
    class LiveSegmentEntry {
      public:
        LiveSegmentEntry(SegmentEntryHandle handle, uint32_t age)
            : handle(handle), age(age), totalLength(handle->totalLength())
        {
            static_assert(sizeof(LiveSegmentEntry) == 16,
                "LiveSegmentEntry isn't the expected size!");
        }
        SegmentEntryHandle handle;
        uint32_t age;
        uint32_t totalLength;
    };
    typedef std::vector<LiveSegmentEntry> SegmentEntryHandleVector;

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
        operator()(const LogCleaner::LiveSegmentEntry a,
                   const LogCleaner::LiveSegmentEntry b)
        {
            return a.age < b.age;
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
     * XXX- This should be part of Metrics someday, but the difference
     * (operator-) functionality is very useful and we don't yet snapshot
     * Metrics (wiping the old data when a recovery starts is a bummer).
     */
    class PerfCounters {
      public:
        PerfCounters()
            : scanTicks(0),
              getSegmentsTicks(0),
              collectLiveEntriesTicks(0),
              livenessCallbackTicks(0),
              sortLiveEntriesTicks(0),
              moveLiveDataTicks(0),
              packLastTicks(0),
              segmentAppendTicks(0),
              closeAndSyncTicks(0),
              relocationCallbackTicks(0),
              cleaningCompleteTicks(0),
              cleanTicks(0),
              cleaningPassTicks(0),
              cleaningPasses(0),
              writeCostSum(0),
              entriesLivenessChecked(0),
              liveEntryBytes(0),
              liveEntriesRelocated(0),
              entriesRolledBack(0),
              segmentsGenerated(0),
              segmentsCleaned(0),
              packLastDidWork(0),
              packLastImprovementSum(0),
              generatedUtilisationSum(0),
              entriesInCleanedSegments(0),
              entryTypeCounts(),
              relocEntryTypeCounts()
        {
            memset(entryTypeCounts, 0, sizeof(entryTypeCounts));
            memset(relocEntryTypeCounts, 0, sizeof(relocEntryTypeCounts));
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
            _sub(livenessCallbackTicks);
            _sub(sortLiveEntriesTicks);
            _sub(moveLiveDataTicks);
            _sub(packLastTicks);
            _sub(segmentAppendTicks);
            _sub(closeAndSyncTicks);
            _sub(relocationCallbackTicks);
            _sub(cleaningCompleteTicks);
            _sub(cleanTicks);
            _sub(cleaningPassTicks);
            _sub(cleaningPasses);
            _sub(writeCostSum);
            _sub(entriesLivenessChecked);
            _sub(liveEntryBytes);
            _sub(liveEntriesRelocated);
            _sub(entriesRolledBack);
            _sub(segmentsGenerated);
            _sub(segmentsCleaned);
            _sub(packLastDidWork);
            _sub(packLastImprovementSum);
            _sub(generatedUtilisationSum);
            _sub(entriesInCleanedSegments);
            for (size_t i = 0; i < arrayLength(entryTypeCounts); i++)
                _sub(entryTypeCounts[i]);
            for (size_t i = 0; i < arrayLength(relocEntryTypeCounts); i++)
                _sub(relocEntryTypeCounts[i]);
            #undef _sub

            return ret;
        }

        uint64_t scanTicks;                 /// Time in scanSegment().
        uint64_t getSegmentsTicks;          /// Time in getSegmentsToClean().
        uint64_t collectLiveEntriesTicks;   /// Part of getSortedLiveEntries().
        uint64_t livenessCallbackTicks;     /// Time checking entry liveness.
        uint64_t sortLiveEntriesTicks;      /// Rest of getSortedLiveEntries().
        uint64_t moveLiveDataTicks;         /// Total time in moveLiveData().
        uint64_t packLastTicks;             /// Time in moveToFillSegment().
        uint64_t segmentAppendTicks;        /// MoveLiveData() seg append time.
        uint64_t closeAndSyncTicks;         /// MoveLiveData() close/sync time.
        uint64_t relocationCallbackTicks;   /// Time in the relocation callback.
        uint64_t cleaningCompleteTicks;     /// Time in Log::cleaningComplete().
        uint64_t cleanTicks;                /// Total time in clean().
        uint64_t cleaningPassTicks;         /// Time in clean() when we clean.
        uint64_t cleaningPasses;            /// Total cleaning passes done.
        double   writeCostSum;              /// Sum of wr costs for all passes.
        uint64_t entriesLivenessChecked;    /// Entries liveness checked for.
        uint64_t liveEntryBytes;            /// Total bytes in live entries.
        uint64_t liveEntriesRelocated;      /// Entries successfully relocated.
        uint64_t entriesRolledBack;         /// Entries rolled back (not live).
        uint64_t segmentsGenerated;         /// New segments with live data.
        uint64_t segmentsCleaned;           /// Clean segments produced.
        uint64_t packLastDidWork;           /// # of segments pack last helped.
        uint64_t packLastImprovementSum;    /// % improvement packing last segs.
        uint64_t generatedUtilisationSum;   /// Sum of all generated segs' util.
        uint64_t entriesInCleanedSegments;  /// Total entries in cleaned segs.
        uint64_t entryTypeCounts[256];      /// Counters for each type seen.
        uint64_t relocEntryTypeCounts[256]; /// Counters for relocated types.
    };

    /**
     * To make the prior packing optimisation very efficient, we need a way to
     * quickly find any Segments that have enough space to take a given object.
     *
     * This class stores lists of Segments based on the amount of appendable
     * space they have. There are N lists, each of which contains Segments that
     * have space between 2^N and 2^(N+1)-1 bytes. If we want to store an entry
     * of size X bytes, we can immediately jump to the smallest bin that can
     * accommodate >= X bytes. If that bin is empty, we can use any larger bin
     * instead. All operations are fast and constant time.
     *
     * Note that this code appears to be too fast to meaningfully measure
     * using CycleCounters internally. However, since it's called into for
     * every single entry we process it is absolutely performance critical.
     * Measuring it just doesn't appear accurate enough to represent this fact.
     */
    class PowerOfTwoSegmentBins {
      public:
        explicit PowerOfTwoSegmentBins(PerfCounters& counters)
            : bins(), binOccupancyMask(0), lastIndex(-1), perfCounters(counters)
        {
            static_assert(NUM_BINS > 0 && NUM_BINS <= 64, "invalid NUM_BINS");

            // It's significantly more efficient to allocate up front.
            for (int i = 0; i < NUM_BINS; i++)
                bins[i].reserve(RESERVE_SEGMENTS_PER_BIN);
        }

        void
        addSegment(Segment* newSegment)
        {
            uint32_t appendableBytes = newSegment->appendableBytes();
            if (appendableBytes < 1)
                return;

            int index = getBinIndex(appendableBytes);
            bins[index].push_back(newSegment);
            binOccupancyMask |= (1UL << index);
        }

        Segment*
        getSegment(uint32_t needBytes)
        {
            int index = getBinIndex(needBytes);

            // See which bins, if any, can satisfy this request.
            uint64_t mask = ~((1UL << index) - 1);
            if ((binOccupancyMask & mask) == 0)
                return NULL;

            // Get the first non-empty bin.
            index = BitOps::findFirstSet(binOccupancyMask & mask) - 1;
            assert(index != 0);
            assert(!bins[index].empty());
            lastIndex = index;
            return bins[index].back();
        }

        void
        updateSegment(Segment* lastSegment)
        {
            assert(lastIndex != -1);
            assert(bins[lastIndex].back() == lastSegment);

            // Move to another bin, if necessary.
            uint32_t appendableBytes = lastSegment->appendableBytes();
            if (appendableBytes < 1) {
                bins[lastIndex].pop_back();
            } else {
                int newIndex = getBinIndex(appendableBytes);
                if (newIndex != lastIndex) {
                    bins[lastIndex].pop_back();
                    bins[newIndex].push_back(lastSegment);
                    binOccupancyMask |= (1UL << newIndex);
                }
            }
            if (bins[lastIndex].size() == 0)
                binOccupancyMask &= ~(1UL << lastIndex);

            lastIndex = -1;
        }

      PRIVATE:
        /**
         * Given a desired number of bytes, return the index of the lowest
         * bin that could satisfy the allocation (assuming it's not empty).
         */
        int
        getBinIndex(uint64_t bytes)
        {
            assert(bytes > 0);
            uint64_t bucketStartSize = BitOps::powerOfTwoLessOrEqual(bytes);
            return BitOps::findFirstSet(bucketStartSize) - 1;
        }

        /// Maximum number of bins we'll divide Segments into. There's only
        /// inconsequential overhead in having more than log2(SegmentSize), so
        /// just fix this at the maximum supported.
        static const int NUM_BINS = 64;

        /// It's significantly more efficient to avoid the memory allocator
        /// having to resize each bucket as they fill up. This is the number
        /// of entries to reserve up front (in the constructor) for each bin.
        static const int RESERVE_SEGMENTS_PER_BIN = 100;

        /// Bins of candidate Segments. Each bin is a list of Segments that have
        /// at least 2^N bytes and less than 2^(N+1) bytes available for append
        /// operations.
        std::vector<Segment*> bins[NUM_BINS];

        /// Bit field representing the availability of Segments in each bin.
        /// For example, if bins[5].empty() is true, then the 5th bit will be
        /// a 0. This allows us to quickly find out if we can find a Segment
        /// with the desired free space and, if so, quickly get the smallest
        /// one.
        uint64_t binOccupancyMask;

        /// Index of the last bin from which we returned a Segment (in
        /// getSegment()). This state is kept so that we can relocate the
        /// Segment after it has been appended to. This may be necessary if
        /// the new amount of appendable space would place it in another bucket.
        int lastIndex;

        /// Parent LogCleaner's performance counters so we can update with our
        /// added overheads.
        PerfCounters& perfCounters;
    };

    // cleaner thread entry point
    static void cleanerThreadEntry(LogCleaner* logCleaner, Context* context);

    void dumpCleaningPassStats(PerfCounters& before);
    void scanNewCleanableSegments();
    void scanSegment(Segment* segment);
    void getSegmentsToClean(SegmentVector&);
    void getSortedLiveEntries(SegmentVector& segments,
                              SegmentEntryHandleVector& liveEntries);
    void moveToFillSegment(Segment* lastNewSegment,
                           SegmentVector& segmentsToClean);
    void moveLiveData(SegmentEntryHandleVector& data,
                      SegmentVector& segmentsToClean);

    /// After cleaning, wake the cleaner again after this many microseconds.
    static const size_t CLEANER_POLL_USEC = 50000;

    /// Number of clean Segments to produce in each cleaning pass.
    static const size_t CLEANED_SEGMENTS_PER_PASS = 10;

    /// Maximum write cost we'll permit. Anything higher and we won't clean.
    static const double MAXIMUM_CLEANABLE_WRITE_COST = 3.0;

    /// When walking the age-sorted list of entries to relocate, prefetch the
    /// entry this far ahead of current position. Each operation takes long
    /// enough that prefetching need not be far ahead.
    static const int PREFETCH_OFFSET = 2;

    /// A limit on how many bytes we'll prefetch. We know the exact entry
    /// size, but we may want to cap it to save cache if we prefetch
    /// farther ahead.
    static const uint32_t MAX_PREFETCH_BYTES = 2048;

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
