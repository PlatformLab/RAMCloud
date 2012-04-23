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

#include <thread>
#include <vector>

#include "Common.h"
#include "LogTypes.h"
#include "Log.h"
#include "Segment.h"
#include "ReplicaManager.h"

namespace RAMCloud {

// forward decl around the circular Log/LogCleaner dependency
class Log;

/**
 * The LogCleaner defragments a Log's closed Segments, writing out any live
 * data to new "survivor" Segments and passing the survivors, as well as the
 * cleaned Segments, to the Log that owns them. The cleaner is designed to
 * run asynchronously in a separate thread, though it can be run inline with
 * the Log code as well.
 *
 * The cleaner employs some heuristics to aid efficiency. For instance, it
 * tries to minimise the cost of cleaning by choosing Segments that have a
 * good 'cost-benefit' ratio. That is, it looks for Segments that have lots
 * of free space, but also for Segments that have less free space but a lot
 * of old data (the assumption being that old data is unlikely to die and
 * cleaning old data will reduce fragmentation and not soon require another
 * cleaning).
 *
 * In addition, the LogCleaner attempts to segregate entries by age in the
 * hopes of packing old data and new data into different Segments. This has
 * two main benefits. First, old data is less likely to fragment (be freed)
 * so those Segments will maintain high utilisation and therefore require
 * less cleaning. Second, new data is more likely to fragment, so Segments
 * containing newer data will hopefully be cheaper to clean in the future.
 */
class LogCleaner {
  public:
    explicit LogCleaner(Log* log, ReplicaManager* replicaManager,
                        bool startThread);
    ~LogCleaner();
    bool clean();
    void halt();

  PRIVATE:
    // Sorting a vector of handles is expensive since we have to take a few
    // cache misses to access the timestamps for each comparison. Instead,
    // include the timestamp directly in the elements being sorted. We access
    // the object anyhow as we scan (to check liveness), so this is essentially
    // free. The sorted array is larger, but comparing elements is much faster.
    // While we're here, also cache the total length for future prefetching.
    class LiveSegmentEntry {
      public:
        LiveSegmentEntry(SegmentEntryHandle handle, uint32_t timestamp)
            : handle(handle),
              timestamp(timestamp),
              totalLength(handle->totalLength())
        {
            static_assert(sizeof(LiveSegmentEntry) == 16,
                "LiveSegmentEntry isn't the expected size!");
        }
        SegmentEntryHandle handle;
        uint32_t timestamp;
        uint32_t totalLength;
    };
    typedef std::vector<LiveSegmentEntry> LiveSegmentEntryHandleVector;

    /// Structure containing a Segment that can be cleaned and some
    /// miscellanous state we want to keep associated with it.
    class CleanableSegment {
      public:
        CleanableSegment(Segment* segment,
                         uint32_t implicitlyFreeableEntries,
                         uint32_t implicitlyFreeableBytes)
            : segment(segment),
              implicitlyFreeableEntries(implicitlyFreeableEntries),
              implicitlyFreedEntries(0),
              implicitlyFreeableBytes(implicitlyFreeableBytes),
              implicitlyFreedBytes(0),
              timesRandomlyScanned(0)
        {
        }

        /// The Segment that could be cleaned.
        Segment* segment;

        /// Number of entries in this Segment that won't be explicitly
        /// freed by the user of the Log. We need to query the liveness
        /// of any such objects ourselves and update counts.
        uint32_t implicitlyFreeableEntries;

        /// Number of entries the cleaner has discovered to be free.
        uint32_t implicitlyFreedEntries;

        /// Number of bytes worth of entries that won't be explicitly
        /// freed.
        uint32_t implicitlyFreeableBytes;

        /// Number of bytes the claner has discovered to be free.
        uint32_t implicitlyFreedBytes;

        /// Times this segment has been randomly scanned. Used to Mitzenmacher
        /// between a few random choices to get appoximately even scanning.
        uint64_t timesRandomlyScanned;
    };

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
        operator()(CleanableSegment a, CleanableSegment b)
        {
            return costBenefit(a.segment) < costBenefit(b.segment);
        }

      private:
        uint64_t now;
    };

    /**
     * Comparison functor that sorts a vector of Segments by utilisation.
     * This is used during low memory conditions to choose the least-
     * utilised segments in an attempt to make quick forward progress.
     * Lower values (the better candidates) come last so that we can easily
     * pop from the back of the list.
     */
    struct UtilisationLessThan {
        bool
        operator()(CleanableSegment a, CleanableSegment b)
        {
            return a.segment->getUtilisation() > b.segment->getUtilisation();
        }
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
            return a.timestamp < b.timestamp;
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
     * TODO(Rumble): This should be part of RawMetrics someday, but the difference
     * (operator-) functionality is very useful and we don't yet snapshot
     * RawMetrics.
     */
    class PerfCounters {
      public:
        PerfCounters()
            : newScanTicks(0),
              scanForFreeSpaceTicks(0),
              scanForFreeSpaceProgress(0),
              scanForFreeSpaceSegments(0),
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
              emergencyCleaningPasses(0),
              failedNormalPasses(0),
              failedNormalPassTicks(0),
              failedEmergencyPasses(0),
              failedEmergencyPassTicks(0),
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
            _sub(newScanTicks);
            _sub(scanForFreeSpaceTicks);
            _sub(scanForFreeSpaceProgress);
            _sub(scanForFreeSpaceSegments);
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
            _sub(emergencyCleaningPasses);
            _sub(failedNormalPasses),
            _sub(failedNormalPassTicks),
            _sub(failedEmergencyPasses),
            _sub(failedEmergencyPassTicks),
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

        uint64_t newScanTicks;              /// Time in scanSegment().
        uint64_t scanForFreeSpaceTicks;     /// Time in scanForFreeSpace().
        uint64_t scanForFreeSpaceProgress;  /// Invocations that got new stats.
        uint64_t scanForFreeSpaceSegments;  /// Total segs scanned for space.
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
        uint64_t emergencyCleaningPasses;   /// Total emergency cleaning passes.
        uint64_t failedNormalPasses;        /// Total normal passes that failed.
        uint64_t failedNormalPassTicks;     /// Time in failed normal passes.
        uint64_t failedEmergencyPasses;     /// Total emerg. passes that failed.
        uint64_t failedEmergencyPassTicks;  /// Time in failed emerg. passes.
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
            // See which bins, if any, can satisfy this request.
            // It's possible that the index - 1 bin could, but
            // there's no guarantee.
            int index = getBinIndex(needBytes) + 1;
            uint64_t mask = ~((1UL << index) - 1);
            if ((binOccupancyMask & mask) == 0)
                return NULL;

            // Get the first non-empty bin.
            index = BitOps::findFirstSet(binOccupancyMask & mask) - 1;
            assert(index != 0);
            assert(!bins[index].empty());
            assert(bins[index].back()->appendableBytes() >= needBytes);
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

        /**
         * Given a desired number of bytes, return the index of the bin
         * into which a Segment with this many appendable bytes would be
         * placed. Note that this will not return the index of a bucket
         * guaranteed to contain Segments that have at least so many
         * appendable bytes (but you can simply add 1 to the returned
         * index to do so).
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
    bool getCleanSegmentMemory(size_t segmentsNeeded,
                               std::vector<void*>& cleanSegmentMemory);
    static double writeCost(uint64_t totalCapacity, uint64_t liveBytes);
    static bool isCleanable(double _writeCost);
    void scanNewCleanableSegments();
    void scanSegment(Segment*  segment,
                     uint32_t* implicitlyFreeableEntries,
                     uint32_t* implicitlyFreeableBytes);
    void scanForFreeSpace();
    void scanSegmentForFreeSpace(CleanableSegment& cleanableSegment);
    void getSegmentsToClean(SegmentVector&);
    size_t getLiveEntries(Segment* segment,
                          LiveSegmentEntryHandleVector& liveEntries);
    size_t getSortedLiveEntries(SegmentVector& segments,
                                LiveSegmentEntryHandleVector& liveEntries);
    void moveToFillSegment(Segment* lastNewSegment,
                           SegmentVector& segmentsToClean);
    void moveLiveData(LiveSegmentEntryHandleVector& data,
                      std::vector<void*>& cleanSegmentMemory,
                      SegmentVector& segmentsToClean);
    bool setUpNormalCleaningPass(LiveSegmentEntryHandleVector& data,
                                 std::vector<void*>& cleanSegmentMemory,
                                 SegmentVector& segmentsToClean);
    bool setUpEmergencyCleaningPass(LiveSegmentEntryHandleVector& data,
                                    std::vector<void*>& cleanSegmentMemory,
                                    SegmentVector& segmentsToClean);

    /// After cleaning, wake the cleaner again after this many microseconds.
    static const size_t POLL_USEC = 50000;

    /// Number of clean Segments to produce in each cleaning pass.
    static const size_t CLEANED_SEGMENTS_PER_PASS = 10;

    // Initializing a double statically from a constant is (still) not legal
    // C++, though g++ 4.4 allowed it.  g++ 4.6 now disallows it but C++11 now
    // supports this via constexpr, but, of course, g++ 4.4 doesn't support
    // constexpr.  Awesome.  Another option would be to remove 'static' and
    // have it be a member variable that is initialized at runtime.
#if __GNUC__ == 4 && __GNUC_MINOR__ >= 6
    /// Maximum write cost we'll permit. Anything higher and we won't clean.
    static constexpr double MAXIMUM_CLEANABLE_WRITE_COST = 6.0;
#else
    /// Maximum write cost we'll permit. Anything higher and we won't clean.
    static const double MAXIMUM_CLEANABLE_WRITE_COST = 6.0;
#endif

    /// When walking the age-sorted list of entries to relocate, prefetch the
    /// entry this far ahead of current position. Each operation takes long
    /// enough that prefetching need not be far ahead.
    static const int PREFETCH_OFFSET = 2;

    /// A limit on how many bytes we'll prefetch. We know the exact entry
    /// size, but we may want to cap it to save cache if we prefetch
    /// farther ahead.
    static const uint32_t MAX_PREFETCH_BYTES = 2048;

    /// Approximation of the smallest entries we'll expect to see. Used to
    /// reserve space ahead of time, e.g. in SegmentEntryVectors.
    static const uint32_t MIN_ENTRY_BYTES = 50;


    /// If an emergency cleaning pass fails (utilisation is too high), we're
    /// either out of luck or our implicit utilisation counts are off. On
    /// each failure, randomly select this many segments to scan and update
    /// counts for.
    static const size_t EMERGENCY_CLEANING_RANDOM_FREE_SPACE_SCANS = 20;

    /// The number of bytes that have been freed in the Log since the last
    /// cleaning operation completed. This is used to avoid invoking the
    /// cleaner if there isn't likely any work to be done.
    uint64_t bytesFreedBeforeLastCleaning;

    /// Segments that the Log considers cleanable, but which haven't been
    /// scanned yet to analyse the contents (to aid in future cleaning
    /// decisions.)
    SegmentVector scanList;

    /// Closed segments that are part of the Log - these may be cleaned
    /// at any time. Only Segments that have been scanned (i.e. previously
    /// were on the #scanList can be added here.
    std::vector<CleanableSegment> cleanableSegments;

    /// The Log we're cleaning.
    Log* log;

    /// Our own private ReplicaManager (not the Log's). ReplicaManager isn't
    /// reentrant, and there's little reason for it to be, so use this one
    /// to manage the Segments we create while cleaning.
    ReplicaManager* replicaManager;

    /// Tub containing our cleaning thread, if we're told to instantiate one
    /// by whoever constructs this object.
    Tub<std::thread> thread;

    /// Set by halt() to ask the cleaning thread to exit.
    bool threadShouldExit;

    // Current performance counters.
    PerfCounters perfCounters;

    DISALLOW_COPY_AND_ASSIGN(LogCleaner);
};

} // namespace

#endif // !RAMCLOUD_LOGCLEANER_H
