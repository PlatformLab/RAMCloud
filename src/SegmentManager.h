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

#ifndef RAMCLOUD_SEGMENTMANAGER_H
#define RAMCLOUD_SEGMENTMANAGER_H

#include <stdint.h>
#include <unordered_map>
#include <vector>

#include "BoostIntrusive.h"
#include "LargeBlockOfMemory.h"
#include "LogSegment.h"
#include "ReplicaManager.h"
#include "ServerId.h"
#include "SpinLock.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * Special exception for when the log runs out of memory entirely.
 */
struct SegmentManagerException : public Exception {
    SegmentManagerException(const CodeLocation& where, std::string msg)
            : Exception(where, msg) {}
};

/**
 */
class SegmentManager {
  public:
    /// Invalid slot number. #alloc returns this if there is not enough free
    /// memory.
    enum { INVALID_SLOT = -1 };

    // Defined after this class.
    class Allocator;

    SegmentManager(Context& context,
                   ServerId& logId,
                   Allocator& allocator,
                   ReplicaManager& replicaManager,
                   double diskExpansionFactor);
    ~SegmentManager();
    LogSegment* allocHead();
    LogSegment* allocSurvivor();
    void cleaningComplete(LogSegmentVector& clean);
    void cleanableSegments(LogSegmentVector& out);
    void logIteratorCreated();
    void logIteratorDestroyed();
    void getActiveSegments(uint64_t nextSegmentId, LogSegmentVector& list);
    void setSurvivorSegmentReserve(uint32_t numSegments);
    LogSegment& operator[](uint32_t slot);
    bool doesIdExist(uint64_t id);
    uint32_t getAllocatedSegmentCount();
    uint32_t getFreeSegmentCount();
    uint32_t getMaximumSegmentCount();
    uint32_t getSegletSize();
    uint32_t getSegmentSize();

  private:
    /**
     * The various possible states that an in-memory segment can be in.
     * For every state there is only one successor state.
     *
     * For segments created by appending to the head of the log, the sequence
     * is:
     *
     *     HEAD --> NEWLY_CLEANABLE --> CLEANABLE -->
     *        FREEABLE_PENDING_DIGEST_AND_REFERENCES -->
     *        FREEABLE_PENDING_REFERENCES --> FREE*
     *
     * For segments created during cleaning to hold survivor data, the sequence
     * is: 
     *
     *     CLEANING_INTO --> CLEANABLE_PENDING_DIGEST -->
     *        FREEABLE_PENDING_DIGEST_AND_REFERENCES -->
     *        FREEABLE_PENDING_REFERENCES --> FREE*
     *
     * [*] There is no explicit FREE state. Rather, the segment is destroyed
     *     at this point.
     */
    enum State {
        /// The segment is the head of the log. At most two segments may be in
        /// this state at any point in time (only during transition between
        /// segments). Typically there is just one.
        HEAD = 0,

        /// The segment is closed and may be cleaned, but the cleaner has not
        /// yet learned of it. It will not be cleaned until discovered.
        NEWLY_CLEANABLE,

        /// The segment is closed and may be cleaned. The cleaner knows of its
        /// existence and may clean it at any time.
        CLEANABLE,

        /// The segment is currently being used to store survivor data from
        /// segments during cleaning. It will eventually be injected into the
        /// log after cleaning completes.
        CLEANING_INTO,

        /// The segment holds survivor data from a previous cleaning pass. Once
        /// it is added to the log (with the next LogDigest), it can be cleaned.
        CLEANABLE_PENDING_DIGEST,

        /// The segment was cleaned, but it cannot be freed until it is removed
        /// from the log and all outstanding references to its data in memory
        /// have completed.
        FREEABLE_PENDING_DIGEST_AND_REFERENCES,

        /// The segmen was cleaned and has been removed from the log, but it
        /// cannot be freed until all outstanding references to its data in
        /// memory have completed.
        FREEABLE_PENDING_REFERENCES,

        /// Not a state, but rather a count of the number of states.
        TOTAL_STATES
    };

    INTRUSIVE_LIST_TYPEDEF(LogSegment, listEntries) SegmentList;
    INTRUSIVE_LIST_TYPEDEF(LogSegment, allListEntries) AllSegmentList;
    typedef std::lock_guard<SpinLock> Lock;

    void writeHeader(LogSegment* segment, uint64_t headSegmentIdDuringCleaning);
    void writeDigest(LogSegment* head);
    LogSegment* getHeadSegment();
    void changeState(LogSegment& s, State newState);
    bool mayAlloc(bool forCleaner);
    LogSegment* alloc(bool forCleaner);
    void addToLists(LogSegment& s);
    void removeFromLists(LogSegment& s);
    void free(LogSegment* s);
    void freeUnreferencedSegments();

    Context& context;

    const ServerId& logId;

    Allocator& allocator;

    ReplicaManager& replicaManager;

    const uint32_t maxSegments;

    size_t numSurvivorSegments;

    size_t numSurvivorSegmentsAlloced;

    Tub<LogSegment> *segments;

    Tub<State> *states;

    std::vector<uint32_t> freeSlots;

    uint64_t nextSegmentId;

    std::unordered_map<uint64_t, uint32_t> idToSlotMap;

    AllSegmentList allSegments;

    SegmentList segmentsByState[TOTAL_STATES];

    SpinLock lock;

    /// Count of the number of LogIterators currently in existence that
    /// refer to the log we're managing. So long as this count is non-zero,
    /// no changes made by the cleaner may be applied. That means that
    /// survivor segments must not be added to the log and cleaned segments
    /// must not be freed.
    int logIteratorCount;

    DISALLOW_COPY_AND_ASSIGN(SegmentManager);
};

class SegmentManager::Allocator : public Segment::Allocator {
  public:
    Allocator(size_t totalBytes, uint32_t segmentSize, uint32_t segletSize)
        : segmentSize(segmentSize),
          segletSize(segletSize),
          freeList(),
          block(totalBytes)
    {
        construct();
    }

    Allocator(size_t totalBytes)
        : segmentSize(Segment::DEFAULT_SEGMENT_SIZE),
          segletSize(Segment::DEFAULT_SEGLET_SIZE),
          freeList(),
          block(totalBytes)
    {
        construct();
    }

    uint32_t
    getSegletsPerSegment()
    {
        return segmentSize / segletSize;
    }

    const void*
    getBaseAddress()
    {
        return block.get();
    }

    uint32_t
    getTotalBytes()
    {
        return downCast<uint32_t>(block.length);
    }

    uint32_t
    getFreeSegmentCount()
    {
        return downCast<uint32_t>((freeList.size() * segletSize) / segmentSize);
    }

    uint32_t
    getSegletSize()
    {
        return segletSize;
    }

    void* 
    alloc()
    {
        if (getFreeSegmentCount() == 0)
            throw SegmentManagerException(HERE, "out of seglets");

        void* p = freeList.back();
        freeList.pop_back();
        return p;
    }

    void
    free(void* seglet)
    {
        freeList.push_back(seglet);
    }

  PRIVATE:
    void
    construct()
    {
        if (block.length < segmentSize) {
            throw SegmentManagerException(HERE,
                "not enough space for even one segment");
        }

        if (segmentSize % segletSize != 0) {
            throw SegmentManagerException(HERE,
                "segments must be an integer multiple of seglet size");
        }

        uint8_t* seglet = block.get();
        for (size_t i = 0; i < (block.length / segletSize); i++) {
            freeList.push_back(seglet);
            seglet += segletSize;
        }
    }

    const uint32_t segmentSize;
    const uint32_t segletSize;
    vector<void*> freeList;
    LargeBlockOfMemory<uint8_t> block;
};

} // namespace

#endif // !RAMCLOUD_SEGMENTMANAGER_H
