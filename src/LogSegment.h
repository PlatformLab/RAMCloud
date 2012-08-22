/* Copyright (c) 2012 Stanford University
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

#include "BoostIntrusive.h"
#include "ReplicatedSegment.h"
#include "Segment.h"

namespace RAMCloud {

/**
 * LogSegment is a simple subclass of Segment. It exists to associate data the
 * Log and LogCleaner care about with a particular Segment (which shouldn't
 * have to know about these things).
 */
class LogSegment : public Segment {
  PRIVATE:
    /**
     * Usage statistics for this segment. These are used to make cleaning
     * decisions. More specifically, as part of the cleaner's cost-benefit
     * analysis when it ranks potential segments to clean.
     *
     * The counters are protected by a spinlock because the liveBytes and
     * spaceTimeSum fields are closely related and the cleaner should not get
     * inconsistent values (old liveBytes, new spaceTimeSum for instance). We
     * could use the cmpxchg16b instruction instead, but a SpinLock acquire/
     * release is only about 20ns (cached, uncontended) vs. 10ns for the cas.
     * I don't expect lock contention to be great enough to make a difference,
     * but we'll see what happens.
     *
     * Note that these counters may sometimes underflow temporarily during
     * cleaning. For example, the cleaner could be in the middle of relocating
     * objects to a survivor segment and a delete RPC could come in and
     * decrement the survivor segment's counts in-between the cleaner relocating
     * the object and updating the statistics.
     */
    class Statistics {
      public:
        Statistics()
            : liveBytes(0),
              spaceTimeSum(0),
              lock()
        {
        }

        void
        increment(uint32_t newLiveBytes, uint32_t timestamp)
        {
            std::lock_guard<SpinLock> guard(lock);
            liveBytes += newLiveBytes;
            spaceTimeSum += static_cast<uint64_t>(newLiveBytes) * timestamp;
        }

        void
        decrement(uint32_t freedBytes, uint32_t timestamp)
        {
            std::lock_guard<SpinLock> guard(lock);
            liveBytes -= freedBytes;
            spaceTimeSum -= static_cast<uint64_t>(freedBytes) * timestamp;
        }

        void
        get(uint32_t& outLiveBytes, uint64_t& outSpaceTimeSum)
        {
            std::lock_guard<SpinLock> guard(lock);
            outLiveBytes = liveBytes;
            outSpaceTimeSum = spaceTimeSum;
        }

      private:
        /// The current number of live bytes in a segment.
        uint32_t liveBytes;

        /// Sum of the products of each entry's size in bytes and timestamp (as
        /// provided by WallTime) in a segment. Used in conjunction with the
        /// liveBytes value to compute an average timestamp for each byte in
        /// the segment. That, in turn, is used to make cleaning decisions.
        uint64_t spaceTimeSum;

        /// Lock ensuring that the liveBytes and spaceTimeSum values are read
        /// and written consistently.
        SpinLock lock;
    };

  public:
    LogSegment(vector<Seglet*>& seglets,
               uint32_t segletSize,
               uint32_t segmentSize,
               uint64_t id,
               uint32_t slot,
               bool isEmergencyHead)
        : Segment(seglets, segletSize),
          id(id),
          slot(slot),
          segletSize(segletSize),
          segmentSizeOnBackups(segmentSize),
          isEmergencyHead(isEmergencyHead),
          statistics(),
          cleanedEpoch(0),
          costBenefit(0),
          costBenefitVersion(0),
          replicatedSegment(NULL),
          headSegmentIdDuringCleaning(Segment::INVALID_SEGMENT_ID),
          listEntries(),
          allListEntries()
    {
    }

    uint32_t
    getAverageTimestamp()
    {
        uint32_t liveBytes;
        uint64_t spaceTimeSum;
        statistics.get(liveBytes, spaceTimeSum);
        return downCast<uint32_t>(spaceTimeSum / liveBytes);
    }

    int
    getMemoryUtilization()
    {
        uint32_t liveBytes;
        uint64_t unused;
        statistics.get(liveBytes, unused);
        uint32_t bytesAllocated = getSegletsAllocated() * segletSize;
        return static_cast<int>(
            (static_cast<uint64_t>(liveBytes) * 100) / bytesAllocated);
    }

    int
    getDiskUtilization()
    {
        uint32_t liveBytes;
        uint64_t unused;
        statistics.get(liveBytes, unused);
        return static_cast<int>(
            (static_cast<uint64_t>(liveBytes) * 100) / segmentSizeOnBackups);
    }

    uint32_t
    getLiveBytes()
    {
        uint32_t liveBytes;
        uint64_t unused;
        statistics.get(liveBytes, unused);
        return liveBytes;
    }

    /// Log-unique 64-bit identifier for this segment.
    const uint64_t id;

    /// SegmentManager slot associated with this segment.
    const uint32_t slot;

    /// Size of seglets used in this segment.
    const uint32_t segletSize;

    /// Number of bytes each segment on a backup consumes. This may differ from
    /// the size of a segment in memory when in-memory cleaning is enabled.
    const uint32_t segmentSizeOnBackups;

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

    /// Statistics that track the usage of this segment. Used by the cleaner in
    /// deciding which segments to clean.
    Statistics statistics;

    /// Epoch during which this segment was cleaned.
    uint64_t cleanedEpoch;

    /// Cached value of this segment's cost-benefit analysis as computed by the
    /// cleaner. This value is really only of interest to the cleaner.
    uint64_t costBenefit;

    /// Version of our cached costBenefit value. The cleaner uses this to check
    /// when it must recompute and when it must use the cached value instead.
    uint64_t costBenefitVersion;

    /// The ReplicatedSegment instance that is handling backups of this segment.
    ReplicatedSegment* replicatedSegment;

    /// For survivor segments generated by the disk cleaner, this is set to the
    /// head segment id of the log at the start of the cleaning pass. This makes
    /// ordering cleaner-generated and regular segments possible.
    uint64_t headSegmentIdDuringCleaning;

    /// Hook used for linking this LogSegment into an intrusive list according
    /// to this object's state in SegmentManager.
    IntrusiveListHook listEntries;

    /// Hook used for linking this LogSegment into a global instrusive list
    /// of all LogSegments in SegmentManager.
    IntrusiveListHook allListEntries;

    DISALLOW_COPY_AND_ASSIGN(LogSegment);
};

typedef std::vector<LogSegment*> LogSegmentVector;

} // namespace

#endif // !RAMCLOUD_LOGSEGMENT_H
