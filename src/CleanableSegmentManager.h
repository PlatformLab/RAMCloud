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

#ifndef RAMCLOUD_CLEANABLESEGMENTMANAGER_H
#define RAMCLOUD_CLEANABLESEGMENTMANAGER_H

#include <thread>
#include <vector>

#include "Common.h"
#include "BoostIntrusive.h"
#include "LogSegment.h"
#include "LogCleanerMetrics.h"
#include "SegmentManager.h"
#include "ServerConfig.h"
#include "SpinLock.h"

namespace RAMCloud {

/**
 * This class is responsible for tracking LogSegments that the cleaner has
 * available for compaction or disk cleaning. The purpose of this class is
 * to provide a simple and very fast interface for obtaining segments to
 * clean or compact, as well as for obtaining aggregate statistics on these
 * segments in order to decide if cleaning is necessary and if so, what sort
 * of cleaning to perform.
 *
 * The most important requirement is to provide this functionality at very
 * high performance as the number of segments scales, since the more time we
 * waste here the less time we have to do actual cleaning. This was designed
 * with back-of-the-envelope calculations that assumed 1TB of log space (131072
 * segments at 8MB a piece). Hopefully it will last much longer.
 *
 * This class provides essentially three services to the LogCleaner class:
 *
 *   1) It aggregates statistics from the segments so that the cleaner may make
 *      policy decisions (when to run, what mode to clean in, etc).
 *   2) It chooses the best candidate segment for memory compaction.
 *   3) It chooses the best candidate segments for disk cleaning.
 *
 * In order to make these operations fast, this class maintains several
 * different data structures that it updates periodically to avoid the overhead
 * needed to return perfect statistics or choose optimal candidate segments at
 * every invocation.
 *
 * For example, it maintains aggregate statistics that are up to N seconds old
 * to avoid querying the counters in each segment. It also maintains a view of
 * optimal cleaning candidates that are up to N seconds old. It turns out that
 * N = 1 is both plenty fast and also very accurate since the percentage change
 * possible within 1 second isn't very much at high capacities.
 *
 * Since tombstone liveness is difficult to track (a tombstone is not dead until
 * the segment it refers to is cleaned on disk), this class also occasionally
 * scans segments to compute the amount of dead tombstone space they contain.
 * This happens automatically and internally to the class and allows it to give
 * LogCleaner more accurate information.
 */
class CleanableSegmentManager {
  public:
    CleanableSegmentManager(SegmentManager& segmentManager,
                            const ServerConfig* config,
                            Context* context,
                            LogCleanerMetrics::OnDisk<>& onDiskMetrics);
    ~CleanableSegmentManager();
    int getLiveObjectUtilization();
    int getUndeadTombstoneUtilization();
    LogSegment* getSegmentToCompact();
    void getSegmentsToClean(LogSegmentVector& outSegsToClean);

  PRIVATE:
    typedef std::lock_guard<SpinLock> Lock;

    void update(Lock& guard);
    void scanSegmentTombstones(Lock& guard);
    uint64_t computeCleaningCostBenefitScore(LogSegment* s);
    uint64_t computeCompactionCostBenefitScore(LogSegment* s);
    uint64_t computeTombstoneScanScore(LogSegment* s);
    uint32_t computeFreeableSeglets(LogSegment* s);
    void insertInAll(LogSegment* s, Lock& guard);
    void eraseFromAll(LogSegment* s, Lock& guard);
    string toString();

    /// Update our data structures at most every this many microseconds. This
    /// parameter is a trade-off between overhead of managing many segments and
    /// providing optimal results. Note that this does not apply if there are
    /// very few segments -- see MIN_SEGMENTS_BEFORE_UPDATE_DELAY.
    enum { UPDATE_USEC = 1000000 };

    /// If we are tracking this many segments or fewer, don't wait UPDATE_USEC
    /// between updating data structures; just do it without delay since it's
    /// so cheap anyhow. This helps ensure reasonable performance when running
    /// a low capacity server.
    enum { MIN_SEGMENTS_BEFORE_UPDATE_DELAY = 100 };

    /// A single segment will be scanned for dead tombstones after this many
    /// segments have been returned via the getSegmentToCompact() and
    /// getSegmentsToClean() methods. This acts as a throttle to trade off
    /// better accounting accuracy for the overhead of scanning.
    ///
    /// A value of 1 means that a segment is scanned for each segment the
    /// cleaner cleans or compacts. Larger values reduce scanning accordingly.
    /// 0 may be used to disable tombstone scanning entirely (not recommended).
    ///
    /// The current value 5 was experimentally found to be reasonable (good
    /// performance in a number of benchmarks, and relatively low overhead).
    enum { SCAN_TOMBSTONES_EVERY_N_SEGMENTS = 5 };

    /// This context is used for reaching into the hash table to query the
    /// liveness of tombstones, because tombstones that are still referenced by
    /// the hash table cannot be safely cleaned.
    Context* context;

    /// The SegmentManager responsible for allocating log segments. This is
    /// needed to query for newly cleanable segments.
    SegmentManager& segmentManager;

    /// Timestamp at which our structures were last updated (see UPDATE_USEC).
    uint64_t lastUpdateTimestamp;

    /// Cached count of live object bytes in all segments we're tracking. Used
    /// to generate the value returned by getLiveObjectUtilization().
    uint64_t liveObjectBytes;

    /// Cached count of possibly alive tombstone bytes in all segments we're
    /// tracking. Used to generate the value returned by
    /// getUndeadTombstoneUtilization().
    ///
    /// Note that any tombstones we have scanned and found to be dead in
    /// scanSegmentTombstones() are not included in this count, since they
    /// can be compacted away and don't require disk cleaning to be made
    /// freeable first.
    uint64_t undeadTombstoneBytes;

    /// Count of the number of segments returned via getSegmentToCompact() and
    /// getSegmentsToClean().
    uint64_t segmentsToCleaner;

    /// Count of the number of segments scanned for dead tombstones.
    uint64_t tombstoneScans;

    /**
     * Compare two segment's cached cost-benefit ratios for cleaning.
     */
    class CleanerCostBenefitComparer {
      public:
        bool
        operator()(const LogSegment& a, const LogSegment& b) const
        {
            return a.cachedCleaningCostBenefitScore >
                   b.cachedCleaningCostBenefitScore;
        }
    };

    INTRUSIVE_MULTISET_TYPEDEF(LogSegment,
                               cleanableCostBenefitEntries,
                               CleanerCostBenefitComparer) CostBenefitTree;

    /// Tree of cleaner candidate segments we're tracking based on their cost-
    /// benefit score (LogSegment::cachedCleaningCostBenefitScore). This allows
    /// us to efficiently find the segments with the best scores.
    ///
    /// Note that segment S exists in this tree iff S exists in
    /// 'compactionCandidates'.
    CostBenefitTree costBenefitCandidates;

    /**
     * Compare two segment's cost-benefit ratios for compaction.
     */
    class CompactionCostBenefitComparer {
      public:
        bool
        operator()(const LogSegment& a, const LogSegment& b) const
        {
            return a.cachedCompactionCostBenefitScore >
                   b.cachedCompactionCostBenefitScore;
        }
    };

    INTRUSIVE_MULTISET_TYPEDEF(LogSegment,
                               cleanableCompactionEntries,
                               CompactionCostBenefitComparer) CompactionTree;

    /// Tree of cleaner candidate segments we're tracking based on the number
    /// of freeable seglets (LogSegment::cachedCompactionCostBenefitScore).
    /// This allows us to efficiently find the segments with the best scores.
    ///
    /// Note that segment S exists in this tree iff S exists in
    /// 'costBenefitCandidates'.
    CompactionTree compactionCandidates;

    /**
     * Compare two segment's scores indicating how important they are for
     * scanning for dead tombstones.
     */
    class TombstoneScanScoreComparer {
      public:
        bool
        operator()(const LogSegment& a, const LogSegment& b) const
        {
            return a.cachedTombstoneScanScore >
                   b.cachedTombstoneScanScore;
        }
    };

    INTRUSIVE_MULTISET_TYPEDEF(LogSegment,
                               tombstoneScanEntries,
                               TombstoneScanScoreComparer) TombstoneScanTree;

    /// Tree of cleaner candidate segments we're tracking based on how important
    /// we think they are to scan for dead tombstones.
    ///
    /// Note that while scanning a segment, it is removed from this tree in
    /// addition to the costBenefitCandidates and compactionCandidates trees.
    /// The segment is re-added to all of them once scanning has completed.
    TombstoneScanTree tombstoneScanCandidates;

    /// Monitor lock guarding all members of this class. The whole point of
    /// this class is to hold this lock as briefly as possible.
    SpinLock lock;

    /// Size of each seglet in bytes.
    const uint32_t segletSize;

    /// Size of each full seglet in bytes.
    const uint32_t segmentSize;

    /// Maximum number of seglets in each segment.
    const uint32_t segletsPerSegment;

    /// Metrics shared with the LogCleaner module.
    LogCleanerMetrics::OnDisk<>& onDiskMetrics;

#ifdef TESTING
    /// If nonzero, then getLiveObjectUtilization will always return this
    /// value. Used in unit tests.
    static int mockLiveObjectUtilization;
#endif

    DISALLOW_COPY_AND_ASSIGN(CleanableSegmentManager);
};

} // namespace

#endif // !RAMCLOUD_CLEANABLESEGMENTMANAGER_H
