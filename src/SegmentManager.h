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
#include "Histogram.h"
#include "ReplicaManager.h"
#include "ServerId.h"
#include "SpinLock.h"
#include "Tub.h"

#include "LogMetrics.pb.h"

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

    SegmentManager(Context* context,
                   const ServerConfig& config,
                   ServerId& logId,
                   SegletAllocator& allocator,
                   ReplicaManager& replicaManager);
    ~SegmentManager();
    void getMetrics(ProtoBuf::LogMetrics_SegmentMetrics& m);
    SegletAllocator& getAllocator() const;
    LogSegment* allocHead(bool mustNotFail = false);
    LogSegment* allocSurvivor(uint64_t headSegmentIdDuringCleaning);
    LogSegment* allocSurvivor(LogSegment* replacing);
    void cleaningComplete(LogSegmentVector& clean);
    void memoryCleaningComplete(LogSegment* cleaned);
    void cleanableSegments(LogSegmentVector& out);
    void logIteratorCreated();
    void logIteratorDestroyed();
    void getActiveSegments(uint64_t nextSegmentId, LogSegmentVector& list);
    bool initializeSurvivorReserve(uint32_t numSegments);
    LogSegment& operator[](uint32_t slot);
    bool doesIdExist(uint64_t id);
    size_t getFreeSurvivorCount();
    int getSegmentUtilization();
    uint64_t allocateVersion();
    bool raiseSafeVersion(uint64_t minimum);

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
    void writeDigest(LogSegment* newHead, LogSegment* prevHead);
    void writeSafeVersion(LogSegment* head);
    LogSegment* getHeadSegment();
    void changeState(LogSegment& s, State newState);
    LogSegment* alloc(SegletAllocator::AllocationType type, uint64_t segmentId);
    void addToLists(LogSegment& s);
    void removeFromLists(LogSegment& s);
    uint32_t allocSlot(SegletAllocator::AllocationType type);
    void freeSlot(uint32_t slot, bool wasEmergencyHead);
    void free(LogSegment* s);
    void freeUnreferencedSegments();

    /// The pervasive RAMCloud context->
    Context* context;

    /// Size of each full segment in bytes.
    const uint32_t segmentSize;

    /// ServerId this log will be tagged with (for example, in SegmentHeader
    /// structures).
    const ServerId& logId;

    /// Allocator to allocate segment memory (seglets) from.
    SegletAllocator& allocator;

    /// The ReplicaManager class instance that handles replication of our
    /// segments.
    ReplicaManager& replicaManager;

    /// The number of seglets in a full segment.
    const uint32_t segletsPerSegment;

    /// Maximum number of segments we will allocate. This dictates the maximum
    /// amount of disk space that may be used on backups, which may differ from
    /// the amount of RAM in the master server if disk expansion factors larger
    /// than 1 are used.
    const uint32_t maxSegments;

    /// Array of all segments in the system. This is array is allocated in the
    /// constructor based on the 'maxSegments' field.
    Tub<LogSegment>* segments;

    /// Array of the states of all segments. For each constructed object in
    /// 'segments', there is a corresponding state in this array at the same
    /// index. The state is not kept within the LogSegment class because its
    /// private to this class. A pair<> would also suffice, but seems uglier.
    Tub<State>* states;

    /// List of indices in 'segments' that are reserved for new emergency head
    /// segments.
    vector<uint32_t> freeEmergencyHeadSlots;

    /// List of indices in 'segments' that are reserved for new survivor
    /// segments allocated by the cleaner.
    vector<uint32_t> freeSurvivorSlots;

    /// List of indices in 'segments' that are free. This exists simply to allow
    /// for fast allocation of LogSegments.
    vector<uint32_t> freeSlots;

    /// Number of segments we need to reserve for emergency heads (to allow us
    /// to roll over to a new log digest when otherwise out of memory).
    const uint32_t emergencyHeadSlotsReserved;

    /// Number of segments the cleaner requested to keep reserved for it.
    uint32_t survivorSlotsReserved;

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

    /// Number of segments currently on backup disks. This is exactly the number
    /// of ReplicatedSegments that exist.
    //
    // TODO(rumble): It would probably be cleaner to have a way to query
    // ReplicaManager for this value. It already maintains a list of existing
    // ReplicatedSegments.
    uint32_t segmentsOnDisk;

    /// Histogram used to track the number of segments present on disk so that
    /// disk utilization of masters can be monitored. This is updated every
    /// time a segment is allocated or freed.
    Histogram segmentsOnDiskHistogram;

    /**
     * Safe version number for a new object in the log.
     * Single safeVersion is maintained in each master through recovery.
     *
     * \li We guarantee that every value assigned to a particular key
     * will have a distinct version number, even if the object is removed
     * and recreated.
     *
     * \li We guarantee that version numbers for a particular key
     * monotonically increases over time, so that comparing two version numbers
     * tells which one is more recent.
     *
     * \li We guarantee that the version number of an object increases by
     * exactly one when it is updated, so that clients can accurately predict
     * the version numbers that they will write before the write completes.
     *
     * These guarantees are implemented as follows:
     *
     * \li #safeVersion, the safe version number, contains the next available
     * version number for any new object on the master. It is initialized to
     * a small integer when the log is created and is recoverable after crashes.
     *
     * \li When an object is created, its new version number is set to 
     * the safeVersion, and the safeVersion
     * is incremented. See #AllocateVersion.
     *
     * \li When an object is updated, its version number is incremented.
     * Note that its incremented version number does not affect the safeVersion. 
     *
     * \li SafeVersion might be behind the version number 
     * of any particular object.
     * As far as the object is not removed, its 
     * version number has no influence of the safeVersion, because the safeVersion
     * is used to keep the monotonicitiy of the version number of any recreated
     * object.
     *
     * \li When an object is removed, set the safeVersion
     * to the higher than any version number of the removed
     * object's version number. See #RaiseSafeVersion.
     *
     **/
    uint64_t safeVersion;

    /// Used for testing only. If true, allocSurvivor() will not block until
    /// memory is free, but instead returns immediately with a NULL pointer if
    /// nothing could be allocated.
    bool testing_allocSurvivorMustNotBlock;

    DISALLOW_COPY_AND_ASSIGN(SegmentManager);
};

} // namespace

#endif // !RAMCLOUD_SEGMENTMANAGER_H
