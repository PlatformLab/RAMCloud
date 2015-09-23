/* Copyright (c) 2009-2015 Stanford University
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
#include "CleanableSegmentManager.h"
#include "Segment.h"
#include "LogCleanerMetrics.h"
#include "LogEntryHandlers.h"
#include "LogEntryRelocator.h"
#include "LogSegment.h"
#include "SegmentManager.h"
#include "ReplicaManager.h"

#include "LogMetrics.pb.h"

namespace RAMCloud {

class ServerConfig;

/**
 * The LogCleaner defragments a Log's closed segments, writing out any live
 * data to new "survivor" segments and reclaiming space used by dead log
 * entries. The cleaner runs in parallel with regular log operations in its
 * own thread.
 *
 * The cleaner employs some heuristics to aid efficiency. For instance, it
 * tries to minimise the cost of cleaning by choosing segments that have a
 * good 'cost-benefit' ratio. That is, it looks for segments that have lots
 * of free space, but also for segments that have less free space but a lot
 * of old data (the assumption being that old data is unlikely to die and
 * cleaning old data will reduce fragmentation and not soon require another
 * cleaning).
 *
 * In addition, the LogCleaner attempts to segregate entries by age in the
 * hopes of packing old data and new data into different segments. This has
 * two main benefits. First, old data is less likely to fragment (be freed)
 * so those segments will maintain high utilization and therefore require
 * less cleaning. Second, new data is more likely to fragment, so segments
 * containing newer data will hopefully be cheaper to clean in the future.
 */
class LogCleaner {
  public:
    LogCleaner(Context* context,
               const ServerConfig* config,
               SegmentManager& segmentManager,
               ReplicaManager& replicaManager,
               LogEntryHandlers& entryHandlers);
    ~LogCleaner();
    void start();
    void stop();
    void getMetrics(ProtoBuf::LogMetrics_CleanerMetrics& m);

    /// The maximum amount of live data we'll process in any single disk
    /// cleaning pass. The units are full segments. The cleaner will multiply
    /// this value by the number of bytes in a full segment and extract live
    /// entries from candidate segments until it exceeds that product.
    enum { MAX_LIVE_SEGMENTS_PER_DISK_PASS = 10 };

    /// The maximum in-memory segment utilization we will clean at. This upper
    /// limit, in conjunction with the number of seglets per segment, ensures
    /// that we can never consume more seglets in cleaning than we free.
    enum { MAX_CLEANABLE_MEMORY_UTILIZATION = 98 };

    /// The following class is used to keep the cleaner from running
    /// during certain other operations (e.g., at the tail end of log
    /// iteration we need to make sure that there is no data lurking in
    /// side segments). No cleaning activities will be underway (and no
    /// data will be present inside segments) at any time when there is
    /// at least one of these objects whose constructor has returned
    /// and whose destructor has not been called.
    class Disabler {
      public:
        explicit Disabler(LogCleaner* cleaner);
        ~Disabler();
      PRIVATE:
        /// Copy of construct argument.
        LogCleaner* cleaner;
        DISALLOW_COPY_AND_ASSIGN(Disabler);
    };

  PRIVATE:
    typedef LogCleanerMetrics::AtomicCycleCounter AtomicCycleCounter;
    typedef std::unique_lock<std::mutex> Lock;

    /// If no cleaning work had to be done the last time we checked, sleep for
    /// this many microseconds before checking again.
    enum { POLL_USEC = 10000 };

    /// The number of full survivor segments to reserve with the SegmentManager.
    /// Must be large enough to ensure that if we get the worst possible
    /// fragmentation during cleaning, we'll still have enough space to fit in
    /// MAX_LIVE_SEGMENTS_PER_DISK_PASS of live data before freeing unused
    /// seglets at the ends of survivor segments.
    enum { SURVIVOR_SEGMENTS_TO_RESERVE = 15 };

    /// The minimum amount of memory utilization we will begin cleaning at using
    /// the in-memory cleaner.
    enum { MIN_MEMORY_UTILIZATION = 90 };

    /// The minimum amount of backup disk utilization we will begin cleaning at
    /// using the disk cleaner. Note that the disk cleaner may also run if the
    /// in-memory cleaner is not working efficiently enough to keep up with the
    /// log writes (accumulation of tombstones will eventually create such
    /// inefficiency and requires disk cleaning to free them).
    enum { MIN_DISK_UTILIZATION = 95 };

    /**
     * Tuple containing a reference to an entry being cleaned, as well as a
     * cache of its timestamp. The purpose of this is to make sorting entries
     * by age much faster by caching the timestamp when we first examine the
     * entry in getSortedEntries(), rather than extracting it on each sort
     * comparison.
     */
    class Entry {
      public:
        Entry(Segment::Reference reference, uint32_t timestamp)
            : reference(reference),
              timestamp(timestamp)
        {
            static_assert(sizeof(Entry) == 16, "Entry isn't the expected size");
        }

        /// Reference to the entry. If the entry is a live object, this
        /// reference will also be in the hash table.
        Segment::Reference reference;

        /// Timestamp of the entry (see WallTime). Used to sort the entries.
        uint32_t timestamp;
    };
    typedef std::vector<Entry> EntryVector;

    /**
     * Comparison functor for sorting entries extracted from segments by their
     * timestamp. In sorting objects by age we can hopefully segregate objects
     * that will quickly decay and those that will last long into different
     * segments, which in turn makes cleaning more efficient.
     */
    class TimestampComparer {
      public:
        bool
        operator()(const Entry& a, const Entry& b)
        {
            return a.timestamp < b.timestamp;
        }
    };

    class CleanerThreadState {
      public:
        CleanerThreadState()
            : threadNumber(0)
            , diskCleaningTicks(0)
            , memoryCompactionTicks(0)
        {
        }
        uint32_t threadNumber;
        uint64_t diskCleaningTicks;
        uint64_t memoryCompactionTicks;
    };

    class Balancer {
      public:
        enum CleaningTask { COMPACT_MEMORY, CLEAN_DISK, SLEEP };

        explicit Balancer(LogCleaner* cleaner)
            : cleaner(cleaner)
            , compactionFailures(0)
            , compactionFailuresHandled(0)
        {
        }
        virtual ~Balancer() { }
        CleaningTask requestTask(CleanerThreadState* thread);
        void compactionFailed();

      PROTECTED:
        bool isMemoryLow(CleanerThreadState* thread);
        virtual bool isDiskCleaningNeeded(CleanerThreadState* thread) = 0;
        LogCleaner* cleaner;
        std::atomic<uint64_t> compactionFailures;
        std::atomic<uint64_t> compactionFailuresHandled;

        DISALLOW_COPY_AND_ASSIGN(Balancer);
    };

    class TombstoneRatioBalancer : public Balancer {
      public:
        TombstoneRatioBalancer(LogCleaner* cleaner, double ratio);
        ~TombstoneRatioBalancer();

      PRIVATE:
        bool isDiskCleaningNeeded(CleanerThreadState* thread);
        double ratio;
    };

    class FixedBalancer: public Balancer {
      public:
        FixedBalancer(LogCleaner* cleaner, uint32_t cleaningPercentage);
        ~FixedBalancer();

      PRIVATE:
        bool isDiskCleaningNeeded(CleanerThreadState* thread);

        const uint32_t cleaningPercentage;
    };

    static void cleanerThreadEntry(LogCleaner* logCleaner, Context* context);
    int getLiveObjectUtilization();
    int getUndeadTombstoneUtilization();
    bool checkIfCleaningNeeded(CleanerThreadState* thread);
    bool checkIfDiskCleaningNeeded(CleanerThreadState* thread);
    void doWork(CleanerThreadState* state);
    void doMemoryCleaning();
    void doDiskCleaning();
    LogSegment* getSegmentToCompact();
    void sortSegmentsByCostBenefit(LogSegmentVector& segments);
    void debugDumpSegments(LogSegmentVector& segments);
    void getSegmentsToClean(LogSegmentVector& outSegmentsToClean);
    uint32_t computeFreeableSeglets(LogSegment* survivor);
    void sortEntriesByTimestamp(EntryVector& entries);
    void getSortedEntries(LogSegmentVector& segmentsToClean,
                          EntryVector& outEntries,
                          LogCleanerMetrics::OnDisk<uint64_t>* localMetrics);
    uint64_t relocateLiveEntries(EntryVector& entries,
                            LogSegmentVector& outSurvivors,
                            LogCleanerMetrics::OnDisk<uint64_t>* localMetrics);
    void closeSurvivor(LogSegment* survivor);
    void waitForAvailableSurvivors(size_t count, uint64_t& outTicks);

    /**
     * Return values for the relocateEntry() method. There are three possible
     * results: an entry was dead and needed no relocation, or it was alive
     * and either was successfully relocated, or some failure occurred and it
     * could not be relocated.
     */
    enum RelocStatus {
        /// The entry was live but could not be relocated (usually because we
        /// need to allocate another survivor segment, but it could be due to
        /// an internal error that caused us to be out of memory entirely).
        RELOCATION_FAILED = 0,

        /// The entry was live and successfully relocated.
        RELOCATED = 1,

        /// The entry was dead and was discarded (not relocated).
        DISCARDED = 2
    };

    /**
     * Helper method that relocates a log entry and updates various metrics
     * to track relocation performance. It is templated so that it may be
     * used to track both relocations due to disk cleaning and in-memory
     * cleaning (compaction). 
     *
     * During cleaning, this is invoked on every entry in the segments being
     * cleaned. It is up to the LogEntryHandlers callee to decide whether or not
     * they want the entry anymore and to use the LogEntryRelocator this method
     * provides them to perpetuate the entry if they need it.
     *
     * \param type
     *      The type of entry that may need relocation.
     * \param buffer
     *      Buffer containing the segment entry that may need relocation.
     * \param reference
     *      Log reference to the entry that may need relocation.
     * \param survivor
     *      Survivor segment into which this entry may be relocated. This may
     *      be NULL, in which case the method will return false if relocation
     *      is attempted, or true if the entry was no longer needed and no
     *      relocation was tried.
     * \param metrics
     *      The appropriate metrics to update with relocation performance
     *      statistics. This should be a thread-local instance of
     *      LogCleanerMetrics::InMemory or LogCleanerMetrics::OnDisk templated
     *      on uint64_t, rather than the atomic uint64_t used for the global
     *      set of metrics. The point is to keep counters thread-local and as
     *      cheap as possible so as not to affect performance. Occasionally
     *      these counters will be merged into the global set of metrics to
     *      maintain aggregated statistics for all threads.
     * \param outBytesAppended
     *      The total number of bytes appended during relocation (including any
     *      metadata) is returned in this counter. Must not be NULL.
     * \return
     *      Returns true if the operation succeeded (the entry was successfully
     *      relocated or was not needed and no relocation was performed).
     *      Returns false if relocation was needed, but failed because the
     *      survivor segment was either NULL or had insufficient space. In this
     *      case, the caller will simply allocate a new survivor and retry.
     */
    template<typename T>
    RelocStatus
    relocateEntry(LogEntryType type,
                  Buffer& buffer,
                  Log::Reference reference,
                  LogSegment* survivor,
                  T* metrics,
                  uint32_t* outBytesAppended)
    {
        LogEntryRelocator relocator(survivor, buffer.size());
        *outBytesAppended = 0;

        {
            metrics->totalRelocationCallbacks++;
            CycleCounter<uint64_t> _(&metrics->relocationCallbackTicks);
            entryHandlers.relocate(type, buffer, reference, relocator);
        }

        if (relocator.failed())
            return RELOCATION_FAILED;

        if (relocator.relocated()) {
            *outBytesAppended = relocator.getTotalBytesAppended();
            metrics->totalRelocationAppends++;
            metrics->relocationAppendTicks += relocator.getAppendTicks();
            return RELOCATED;
        }

        return DISCARDED;
    }

    /// Shared RAMCloud information.
    Context* context;

    /// The SegmentManager instance that we use to allocate survivor segments,
    /// report cleaned segments to, etc. This class owns all of the segments
    /// and seglets in the system.
    SegmentManager& segmentManager;

    /// The ReplicaManager instance that we use to store copies of log segments
    /// on remote backups. The cleaner needs this in order to replicate survivor
    /// segments generated during cleaning.
    ReplicaManager& replicaManager;

    /// EntryHandlers used to query information about entries we are cleaning
    /// (such as liveness), and to notify when an entry has been relocated.
    LogEntryHandlers& entryHandlers;

    /// Instance of the class that keeps track of all of the segments available
    /// for cleaning.
    CleanableSegmentManager cleanableSegments;

    /// Threshold defining how much work the in-memory cleaner should do before
    /// forcing a disk cleaning pass. Necessary because in-memory cleaning does
    /// not free up tombstones and can become very expensive before we run out
    /// of disk space and fire up the disk cleaner.
    double writeCostThreshold;

    /// If true, the in-memory cleaner will never be run. Instead, the disk
    /// cleaner will run in its place.
    bool disableInMemoryCleaning;

    /// The number of cleaner threads to run concurrently. More threads will
    /// allow the system to perform more cleaning and compaction in parallel to
    /// keep up with higher write rates and memory utilizations.
    const int numThreads;

    /// Size of each seglet in bytes. Used to calculate the best segment for in-
    /// memory cleaning.
    uint32_t segletSize;

    /// Size of each full segment in bytes. Used to calculate the amount of
    /// space freed on backup disks.
    uint32_t segmentSize;

    /// Total number of threads actively working (not just sleeping);
    /// used to implement Disablers.
    int activeThreads;

    /// Number of Disabler objects currently in existence; if non-zero,
    /// no new cleaning activities will commence (but existing activities
    /// may complete).
    int disableCount;

    /// This variable is signaled if activeThreads becomes zero at a time
    /// when disableCount is nonzero.
    std::condition_variable cleanerIdle;

    /// Protects access to activeThreads, disableCount, and cleanerIdle
    /// (used for synchronization between cleaner threads and Disabler
    /// objects).
    std::mutex mutex;

    /// Number of cpu cycles spent in the doWork() routine.
    LogCleanerMetrics::Atomic64BitType doWorkTicks;

    /// Number of cpu cycles spent sleeping in the doWork() routine because
    /// memory was not low.
    LogCleanerMetrics::Atomic64BitType doWorkSleepTicks;

    /// Metrics kept for measuring in-memory cleaning (compaction) performance.
    /// Note that this instance uses the default template that uses atomic
    /// operations. Be careful incrementing these counters in hot paths. It's
    /// often better to aggregate in local variables and then apply updates to
    /// this in batch. See LogCleanerMetrics::InMemory<>::merge().
    LogCleanerMetrics::InMemory<> inMemoryMetrics;

    /// Metrics kept for measuring on-disk cleaning performance. Be careful
    /// incrementing these counters in hot paths. It's often better to aggregate
    /// in local variables and then apply updates to this in batch. See
    /// LogCleanerMetrics::OnDisk<>::merge().
    LogCleanerMetrics::OnDisk<> onDiskMetrics;

    /// Metrics kept for measuring how many threads the cleaner is using.
    LogCleanerMetrics::Threads threadMetrics;

    /// Set by halt() to indicate that the cleaning thread(s) should exit.
    bool threadsShouldExit;

    /// The cleaner spins one or more threads to perform its work (#numThreads).
    /// This vector contains pointers to these threads. When the cleaner is
    /// started, these threads are created. When stopped, they are deleted and
    /// the pointers in this vector are set to NULL.
    vector<std::thread*> threads;

    /// Instance of the balancer module that decides when to clean on disk,
    /// when to compact in memory, and how many of our threads to employ.
    Balancer* balancer;

    friend class CleanerCompactionBenchmark;

    DISALLOW_COPY_AND_ASSIGN(LogCleaner);
};

} // namespace

#endif // !RAMCLOUD_LOGCLEANER_H
