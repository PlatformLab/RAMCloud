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
 * The SegmentManager is essentially the core of the master server's in-memory
 * log. It handles allocation of segments and manages them throughout their
 * lifetime in the system. It also has a part in ensuring log integrity by
 * transitioning from one head segment to another and stamping them with a list
 * of segments currently in the log (the LogDigest).
 *
 * Although this functionality could (and used to) be a part of the Log module,
 * pulling it into its own class has a number of advantages. First, it hides
 * much of the log's complexity, making that module simpler to reason about and
 * modify. Second, it provides a common point of interaction between the user-
 * visible log code (primarily Log::append) and the log cleaner. Frankly, it
 * avoids providing a large, mucky interface that is used by both regular
 * consumers of the log (like MasterService) and the cleaner, which no normal
 * users should need to understand or have access to.
 *
 * The SegmentManager owns all memory allocated to log and doles it out to
 * segments as needed. It also tracks the state of each segment and determines
 * when it is safe to reuse memory from cleaned segments (since RPCs could
 * still reference a segment after the cleaner has finished with it).
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
    LogSegment* allocSurvivor(uint64_t headSegmentIdDuringCleaning);
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

  PRIVATE:
    /**
     * The various possible states that an in-memory segment can be in. Every
     * segment is in one and only one state at any point in time. These states
     * exist primarily due to complexities involved in cleaning. For instance,
     * how do we identify new segments to tell the cleaner about? How do we
     * keep track of cleaned segments that cannot yet be freed because they're
     * still part of the log, or because RPCs may still reference their
     * contents?
     *
     * Fortunately, despite the non-trivial number of states, only this module
     * really needs to know that they exist. Furthermore, for every state there
     * is only one successor state, so transitions are really easy.
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

    /// The pervasive RAMCloud context.
    Context& context;

    /// ServerId this log will be tagged with (for example, in SegmentHeader
    /// structures).
    const ServerId& logId;

    /// Allocator to allocate segment memory from. See the class documentation
    /// for SegmentManager::Allocator.
    Allocator& allocator;

    /// The ReplicaManager class instance that handles replication of our
    /// segments.
    ReplicaManager& replicaManager;

    /// Maximum number of segments we will allocate. This dictates the maximum
    /// amount of disk space that may be used on backups, which may differ from
    /// the amount of RAM in the master server if disk expansion factors larger
    /// than 1 are used.
    const uint32_t maxSegments;

    /// The number of survivor segments that are being reserved for the log
    /// cleaner. This is used to maintain a cache of segments that will never
    /// be allocated for the head of the log to ensure that we don't deadlock
    /// because cleaning can't take place.
    size_t numSurvivorSegments;

    /// The number of reserved survivor segments that have been allocated to the
    /// cleaner.
    size_t numSurvivorSegmentsAlloced;

    /// Array of all segments in the system. This is array is allocated in the
    /// constructor based on the 'maxSegments' field.
    Tub<LogSegment>* segments;

    /// Array of the states of all segments. For each constructed object in
    /// 'segments', there is a corresponding state in this array at the same
    /// index. The state is not kept within the LogSegment class because its
    /// private to this class. A pair<> would also suffice, but seems uglier.
    Tub<State>* states;

    /// List of indices in 'segments' that are free. This exists simply to allow
    /// for fast allocation of LogSegments.
    std::vector<uint32_t> freeSlots;

    /// Monotonically increasing identifier to be given to the next segment
    /// allocated by this module.
    uint64_t nextSegmentId;

    /// Lookup table from segment identifier to the corresponding index in
    /// 'segments' and 'states'. Currently used only to check for existence
    /// of a particular segment.
    std::unordered_map<uint64_t, uint32_t> idToSlotMap;

    /// Linked list of all LogSegments managed by this module.
    AllSegmentList allSegments;

    /// Linked lists of LogSegments based on their current state. The contents
    /// of all lists together is equivalent to the 'allSegments' list.
    SegmentList segmentsByState[TOTAL_STATES];

    /// Monitor lock protecting the SegmentManager from multiple calling
    /// threads. At a minimum, the log and log cleaner modules may operate
    /// simultaneously.
    SpinLock lock;

    /// Count of the number of LogIterators currently in existence that
    /// refer to the log we're managing. So long as this count is non-zero,
    /// no changes made by the cleaner may be applied. That means that
    /// survivor segments must not be added to the log and cleaned segments
    /// must not be freed.
    int logIteratorCount;

    DISALLOW_COPY_AND_ASSIGN(SegmentManager);
};

/**
 * This class manages the log's memory. It it used to keep track of, allocate,
 * and free the individual seglets that comprise segments. It also specifies
 * how many seglets are in each full segment and how large each seglet is.
 *
 * To enable the simple zero-copy transport hack, we assume (and ensure) that
 * all memory is allocated from once large contiguous block.
 */
class SegmentManager::Allocator : public Segment::Allocator {
  public:
    /**
     * Construct a new allocator from which the SegmentManager may get backing
     * memory for log segments.
     *
     * \param totalBytes
     *      Total number of bytes to allocate for the log.
     * \param segmentSize
     *      The size of each segment in bytes.
     * \param segletSize
     *      The size of each seglet in bytes. This should evenly divide into the
     *      value of segmentSize.
     */
    Allocator(size_t totalBytes, uint32_t segmentSize, uint32_t segletSize)
        : segmentSize(segmentSize),
          segletSize(segletSize),
          freeList(),
          block(totalBytes)
    {
        construct();
    }

    /**
     * Construct a new allocator from which the SegmentManager may get backing
     * memory for log segments. Use the statically-defined defaults for the
     * sizes of segments and seglets.
     *
     * \param totalBytes
     *      Total number of bytes to allocate for the log.
     */
    explicit Allocator(size_t totalBytes)
        : segmentSize(Segment::DEFAULT_SEGMENT_SIZE),
          segletSize(Segment::DEFAULT_SEGLET_SIZE),
          freeList(),
          block(totalBytes)
    {
        construct();
    }

    /**
     * Return the number of seglets in each segment.
     */
    uint32_t
    getSegletsPerSegment()
    {
        return segmentSize / segletSize;
    }

    /**
     * Return a pointer to the first byte of the contiguous buffer that all log
     * memory is allocated from. This is used by the transports to register
     * memory areas for zero-copy transmits.
     */
    const void*
    getBaseAddress()
    {
        return block.get();
    }

    /**
     * Return the total number of bytes this allocator has for the log.
     */
    uint64_t
    getTotalBytes()
    {
        return block.length;
    }

    /**
     * Return the number of full segment's worth of memory the allocator has
     * free. 
     */
    uint32_t
    getFreeSegmentCount()
    {
        return downCast<uint32_t>((freeList.size() * segletSize) / segmentSize);
    }

    /**
     * Return the size of each seglet.
     */
    uint32_t
    getSegletSize()
    {
        return segletSize;
    }

    /**
     * Allocate a seglet's worth of memory.
     */
    void*
    alloc()
    {
        if (getFreeSegmentCount() == 0)
            throw SegmentManagerException(HERE, "out of seglets");

        void* p = freeList.back();
        freeList.pop_back();
        return p;
    }

    /**
     * Free a previously-allocated seglet.
     */
    void
    free(void* seglet)
    {
        freeList.push_back(seglet);
    }

  PRIVATE:
    /**
     * Common constructor. Sanity checks the given parameters and sets up the
     * free list of seglets.
     */
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

    /// Size of each segment in bytes.
    const uint32_t segmentSize;

    /// Size of each seglet in bytes.
    const uint32_t segletSize;

    /// List of free seglets.
    vector<void*> freeList;

    /// Contiguous block of memory backing our allocations.
    LargeBlockOfMemory<uint8_t> block;

    DISALLOW_COPY_AND_ASSIGN(Allocator);
};

} // namespace

#endif // !RAMCLOUD_SEGMENTMANAGER_H
