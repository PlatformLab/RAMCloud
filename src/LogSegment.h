/* Copyright (c) 2012-2013 Stanford University
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

#ifndef RAMCLOUD_LOGSEGMENT_H
#define RAMCLOUD_LOGSEGMENT_H

#if __GNUC__ >= 4 && __GNUC_MINOR__ >= 5
#include <atomic>
#else
#include <cstdatomic>
#endif

#include "AbstractLog.h"
#include "BoostIntrusive.h"
#include "ReplicatedSegment.h"
#include "Segment.h"
#include "WallTime.h"

namespace RAMCloud {

/// Redeclare the typedef defined in SegmentManager.h to avoid a cyclical
/// dependency. See SegmentManager.h's typedef comments for documentation on
/// this type.
typedef uint32_t SegmentSlot;

/**
 * LogSegment is a simple subclass of Segment. It exists to associate data the
 * Log and LogCleaner care about with a particular Segment (which shouldn't
 * have to know about these things).
 *
 * It's important to note that the same logical log segment may correspond to
 * multiple instances of the LogSegment class over time. This is because the
 * cleaner will occasionally compact a segment in memory. When it does, it moves
 * the live log entries to a new LogSegment instance that has the same segment
 * identifier as before compaction.
 */
class LogSegment : public Segment {
  public:
    /**
     * Construct a new LogSegment.
     *
     * \param seglets
     *      The seglets backing this segment in memory.
     * \param segletSize
     *      Size of each seglet in bytes.
     * \param segmentSize
     *      Size of the full segment in bytes.
     * \param id
     *      64-bit identifier of the segment in the log.
     * \param slot
     *      Slot from which this segment was allocated in the SegmentManager.
     * \param creationTimestamp
     *      WallTime seconds timestamp when this segment was created. This is a
     *      parameter because the cleaner will allocate a new Segment object
     *      when it compacts an existing segment and the timestamp needs to be
     *      preserved.
     * \param isEmergencyHead
     *      If true, this is a special segment that is being used to roll over
     *      to a new head and write a new digest when otherwise out of memory.
     */
    LogSegment(vector<Seglet*>& seglets,
               uint32_t segletSize,
               uint32_t segmentSize,
               uint64_t id,
               SegmentSlot slot,
               uint32_t creationTimestamp,
               bool isEmergencyHead)
        : Segment(seglets, segletSize),
          id(id),
          slot(slot),
          segletSize(segletSize),
          segmentSize(segmentSize),
          creationTimestamp(creationTimestamp),
          isEmergencyHead(isEmergencyHead),
          cleanedEpoch(0),
          cachedCleaningCostBenefitScore(0),
          cachedCompactionCostBenefitScore(0),
          cachedTombstoneScanScore(0),
          replicatedSegment(NULL),
          listEntries(),
          allListEntries(),
          cleanableCostBenefitEntries(),
          cleanableCompactionEntries(),
          tombstoneScanEntries(),
          syncedLength(0),
          lastCompactionTimestamp(WallTime::secondsTimestamp()),
          lastTombstoneScanTimestamp(WallTime::secondsTimestamp()),
          entryCounts(),
          deadEntryCounts(),
          entryLengths(),
          deadEntryLengths()
    {
        memset(entryCounts, 0, sizeof(entryCounts));
        memset(deadEntryCounts, 0, sizeof(deadEntryCounts));
        memset(entryLengths, 0, sizeof(entryLengths));
        memset(deadEntryLengths, 0, sizeof(deadEntryLengths));
    }

    /**
     * Return the number of entries of the given type that have been appended to
     * this segment. There is no notion of dead or alive entries. Any that were
     * ever appended are reflected in the result.
     */
    uint32_t
    getEntryCount(LogEntryType type)
    {
        return entryCounts[type];
    }

    /*
     * Return the number of bytes taken up by entries of the given type that have
     * been appended to this segment. There is no notion of dead or alive entries.
     * Any that were ever appended are reflected in the result.
     *
     * Note that this value includes the header overheads (type and length field)
     * within the Segment.
     */
    uint32_t
    getEntryLengths(LogEntryType type)
    {
        return entryLengths[type];
    }

    /*
     * Return the number of bytes taken up by dead entries of a given type.
     */
    uint32_t
    getDeadEntryLengths(LogEntryType type) {
        return entryLengths[type];
    }

    uint32_t
    getLiveBytes()
    {
        uint32_t sum = 0;
        for (size_t i = 0; i < TOTAL_LOG_ENTRY_TYPES; i++) {
            assert(entryLengths[i] >= deadEntryLengths[i]);
            sum += entryLengths[i] - deadEntryLengths[i];
        }
        return sum;
    }

    /**
     * Get the in-memory utilization of the segment. This is the percentage of
     * allocated memory bytes that belong to live data. The value returned is
     * in the range [0, 100].
     */
    int
    getMemoryUtilization()
    {
        uint32_t bytesAllocated = getSegletsAllocated() * segletSize;
        if (bytesAllocated == 0) {
            assert(getLiveBytes() == 0);
            return 0;
        }
        return static_cast<int>(
            (static_cast<uint64_t>(getLiveBytes()) * 100) / bytesAllocated);
    }

    /**
     * Get the on-disk utilization of the segment. This is the percentage of
     * the full segment that is being used by live data. The full segment on
     * disk may be larger than the one in memory due to memory compaction (the
     * in-memory cleaner). The value returned is in the range [0, 100].
     */
    int
    getDiskUtilization()
    {
        assert(segmentSize != 0);
        return static_cast<int>(
            (static_cast<uint64_t>(getLiveBytes()) * 100) / segmentSize);
    }

    /**
     * Given an offset into this segment, return a corresponding Log::Reference.
     * This is primarily used by the cleaner to provide ObjectManager with a
     * simple way to checking if an object is alive (this reference is in the
     * hash table iff it's alive).
     */
    AbstractLog::Reference
    getReference(uint32_t offset)
    {
        return AbstractLog::Reference(this, offset);
    }

    void
    trackNewEntries(LogEntryType type,
                    uint32_t numEntries,
                    uint32_t lengthWithMetadata)
    {
        entryCounts[type] += numEntries;
        entryLengths[type] += lengthWithMetadata;
    }

    void
    trackNewEntry(LogEntryType type, uint32_t lengthWithMetadata)
    {
        trackNewEntries(type, 1, lengthWithMetadata);
    }

    void
    trackDeadEntry(LogEntryType type, uint32_t lengthWithMetadata)
    {
        deadEntryCounts[type]++;
        deadEntryLengths[type] += lengthWithMetadata;
    }

    /// Log-unique 64-bit identifier for this segment.
    const uint64_t id;

    /// Index of the entry for this segment in SegmentManager's "segments"
    /// table.
    const SegmentSlot slot;

    /// Size of seglets used in this segment.
    const uint32_t segletSize;

    /// Number of bytes each full segment consumes. All segments on backups use
    /// this much space. This may be greater than the actual size of any given
    /// segment in memory when in-memory cleaning is enabled.
    const uint32_t segmentSize;

    /// Timestamp of this segment's creation in seconds (via WallTime::). Used
    /// by the cleaner when selecting segments to clean (as part of the cost-
    /// benefit formula).
    const uint32_t creationTimestamp;

    /// If true, this segment is one of two special emergency heads the system
    /// reserves so that it can always open a new log head even if out of
    /// memory. This is needed so that the cleaner can advance the head and
    /// finish a cleaning pass regardless of free space, and so that the
    /// replica manager can close the current head and open a new one if there
    /// had been a failure on any of its replicas.
    ///
    /// Note that emergency segments must never contain data that must outlive
    /// the head segment. That is, it may contain digests and other entries that
    /// will be superceded by the next head, but must not contain other data
    /// that is expected to live longer.
    const bool isEmergencyHead;

    /// The epoch value when cleaning was completed on this segment. Once no
    /// more RPCs in the system exist with epochs less than or equal to this,
    /// there can be no more outstanding references into the segment and its
    /// memory may be safely freed and reused.
    uint64_t cleanedEpoch;

    /// Cached value of this segment's cost-benefit score for cleaning as
    /// computed by the CleanableSegmentManager. This value is only really
    /// of interest to that module. It is cached to ensure the value does not
    /// change and violate the weak strict ordering requirement.
    uint64_t cachedCleaningCostBenefitScore;

    /// Cached value of this segment's cost-benefit score for compaction as
    /// computed by CleanableSegmentManager. This value is only really of
    /// interest to that module. It is cached to ensure the value does not
    /// change and violate the weak strict ordering requirement.
    uint64_t cachedCompactionCostBenefitScore;

    /// Cached value of this segment's cost-benefit score for scanning of dead
    /// tombstones. This value is only really of interest to that module. It is
    /// cached to ensure the value does not change and violate the weak strict
    /// ordering requirement.
    uint64_t cachedTombstoneScanScore;

    /// The ReplicatedSegment instance that is responsible for replicating and
    /// this segment to backups.
    ReplicatedSegment* replicatedSegment;

    /// Hook used for linking this LogSegment into an intrusive list according
    /// to this object's state in SegmentManager.
    IntrusiveListHook listEntries;

    /// Hook used for linking this LogSegment into the single "allSegments"
    /// instrusive list in SegmentManager.
    IntrusiveListHook allListEntries;

    /// Hook used for linking this LogSegment into an rbtree maintained by
    /// CleanableSegmentManager that tracks cleanable segments by cost-benefit
    /// score.
    IntrusiveSetHook cleanableCostBenefitEntries;

    /// Hook used for linking this LogSegment into a list maintained by
    /// CleanableSegmentManager that tracks the best candidate segments for
    /// compaction based on the number of freeable seglets they have.
    IntrusiveSetHook cleanableCompactionEntries;

    /// XXX
    IntrusiveSetHook tombstoneScanEntries;

    /// Number of bytes in this segment that have been synced in Log::sync. This
    /// is used in Log::sync to avoid issuing a sync() call to ReplicatedSegment
    /// when the desired data has already been synced (perhaps by another thread
    /// that bundled our replication traffic with theirs). The point is to allow
    /// batching of objects during backup writes when there are multiple threads
    /// appending to the log.
    uint32_t syncedLength;

    /// Timestamp when this segment was last compacted or created. Used by the
    /// cleaner to decide when to scan for dead tombstones. Sometimes segments
    /// will accumulate tombstones and appear cold even though many of the
    /// tombstones may be dead. This ensures that the cleaner occasionally scans
    /// such segments to reclaim any tombstones it can.
    const uint32_t lastCompactionTimestamp;

    /// XXX
    uint32_t lastTombstoneScanTimestamp;

    /// Counts of the number of each log entry type appended to this segment.
    /// These values monotonically increase and therefore reflect the total
    /// number of entries in the segment, not just the ones that are still
    /// considered alive.
    ///
    /// Note that for compacted segments these counts only include entries
    /// still in memory. They do not include ones on disk.
    std::atomic<uint32_t> entryCounts[TOTAL_LOG_ENTRY_TYPES];

    /// Counts of the number of each log entry previously recorded in
    /// entryCounts that are now dead (that is, the cleaner can reclaim the
    /// space they used). These values monotonically increase, and the
    /// difference between entryCounts[x] and deadEntryCounts[x] is the number
    /// of live entries of a particular type x.
    ///
    /// Note that for compacted segments these counts only include entries
    /// still in memory. They do not include ones on disk.
    std::atomic<uint32_t> deadEntryCounts[TOTAL_LOG_ENTRY_TYPES];

    /// Counts of the number of bytes appended to this segment corresponding
    /// to each log entry type. These values monotonically increase and
    /// therefore reflect the total number of bytes in the segment, not just
    /// the ones that are still considered alive.
    ///
    /// Note that for compacted segments these counts only include entries
    /// still in memory. They do not include ones on disk.
    std::atomic<uint32_t> entryLengths[TOTAL_LOG_ENTRY_TYPES];

    /// Counts of the number of bytes from entries previously recorded in
    /// entryLengths that are now dead (that is, the cleaner can reclaim the
    /// space they used). These values monotonically increase, and the
    /// difference between entryLengths[x] and deadEntryLengths[x] is the number
    /// of bytes used by live entries of a particular type x.
    ///
    /// Note that for compacted segments these counts only include entries
    /// still in memory. They do not include ones on disk.
    std::atomic<uint32_t> deadEntryLengths[TOTAL_LOG_ENTRY_TYPES];

    DISALLOW_COPY_AND_ASSIGN(LogSegment);
};

typedef std::vector<LogSegment*> LogSegmentVector;

} // namespace

#endif // !RAMCLOUD_LOGSEGMENT_H
