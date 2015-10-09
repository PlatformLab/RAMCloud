/* Copyright (c) 2012-2015 Stanford University
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

#if __GNUC__ >= 4 && __GNUC_MINOR__ >= 5
#include <atomic>
#else
#include <cstdatomic>
#endif

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
// Forward declaration.
class MasterTableMetadata;

/**
 * Exception thrown when invalid arguments are passed to the constructor.
 */
struct SegmentManagerException : public Exception {
    SegmentManagerException(const CodeLocation& where, std::string msg)
            : Exception(where, msg) {}
};

/// Slots are indexes in the "segments" table of SegmentManager. That table
/// contains all LogSegment objects corresponding to segments being managed
/// by this class. The SegmentSlot serves as a short reference to a particular
/// segment. Users of the log are returned references to their entries
/// containing the slot and an offset within the segment. This makes it easy
/// to both address the entry within the segment, and figure out which
/// LogSegment structure corresponds to it.
typedef uint32_t SegmentSlot;

/// Invalid SegmentSlot number. May be used as a return value to indicate error.
enum : SegmentSlot { INVALID_SEGMENT_SLOT = -1U };

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
 *
 * This class is necessarily thread-safe, since the log and the log cleaner
 * run in parallel.
 */
class SegmentManager {
  public:
    // Defined after this class.
    class Allocator;

    /// Flags that may be passed to the allocHeadSegment and allocSideSegment
    /// methods below. Each flag is assigned a power of two value and may
    /// be ORed together to specify multiple parameters.
    enum AllocSegmentFlags : uint32_t {
        /// The null flag. This means that no flags are specified.
        EMPTY = 0,

        /// The allocation must not be allowed to fail. The method may block
        /// until more memory is available, or may return a closed emergency
        /// head segment, as appropriate.
        MUST_NOT_FAIL = 1,

        /// The segment being allocated will be used for cleaning. This simply
        /// tells SegmentManager which pool of memory to allocate from. This
        /// flag only makes sense in the allocSideSegment method.
        FOR_CLEANING = 2
    };

    SegmentManager(Context* context,
                   const ServerConfig* config,
                   ServerId* logId,
                   SegletAllocator& allocator,
                   ReplicaManager& replicaManager,
                   MasterTableMetadata* masterTableMetadata);
    ~SegmentManager();
    void getMetrics(ProtoBuf::LogMetrics_SegmentMetrics& m);
    SegletAllocator& getAllocator() const;
    LogSegment* allocHeadSegment(uint32_t flags = EMPTY);
    LogSegment* allocSideSegment(uint32_t flags = EMPTY,
                                 LogSegment* replacing = NULL);
    void cleaningComplete(LogSegmentVector& clean, LogSegmentVector& survivors);
    void compactionComplete(LogSegment* oldSegment, LogSegment* newSegment);
    void injectSideSegments(LogSegmentVector& segments);
    void freeUnusedSideSegments(LogSegmentVector& segments);
    void cleanableSegments(LogSegmentVector& out);
    void getActiveSegments(uint64_t nextSegmentId, LogSegmentVector& list);
    bool initializeSurvivorReserve(uint32_t numSegments);
    LogSegment& operator[](SegmentSlot slot);
    bool doesIdExist(uint64_t id);
    int getSegmentUtilization();
    uint64_t allocateVersion();
    bool raiseSafeVersion(uint64_t minimum);
    int getMemoryUtilization();

#ifdef TESTING
    /// Used to mock the return value of getSegmentUtilization() when set to
    /// anything other than 0.
    static int mockSegmentUtilization;

    /// If nonzero, then getMemoryUtilization will always return this
    /// value. Used in unit tests.
    static int mockMemoryUtilization;
#endif

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
     * For segments written by the SideLog class (for example, those created
     * during cleaning to hold survivor data or populated during crash
     * recovery), the sequence is identical aside from the first two states:
     *
     *     SIDELOG --> CLEANABLE_PENDING_DIGEST --> NEWLY_CLEANABLE -->
     *        CLEANABLE --> FREEABLE_PENDING_DIGEST_AND_REFERENCES -->
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

        /// The segment is currently being used by an instance of the SideLog
        /// class (typically for log cleaning or memory compaction, but also
        /// recovery or tablet migration). It will eventually be either injected
        /// into the log or freed immediately, depending on whether the SideLog
        /// is committed or aborted.
        SIDELOG,

        /// The segment holds entries from a committed SideLog. When the head is
        /// rolled over and a new LogDigest is written, the segment will be part
        /// of the durable log and eligible for cleaning.
        CLEANABLE_PENDING_DIGEST,

        /// The segment was cleaned, but it cannot be freed until it is removed
        /// from the log and all outstanding references to its data in memory
        /// have completed.
        FREEABLE_PENDING_DIGEST_AND_REFERENCES,

        /// The segment was cleaned and has been removed from the log, but it
        /// cannot be freed until all outstanding references to its data in
        /// memory have completed. Aborted SideLog segments also transition to
        /// this state when being freed.
        FREEABLE_PENDING_REFERENCES,

        /// Not a state, but rather a count of the number of states.
        TOTAL_STATES,

        /// Also not a state. Indicates that a State variable is uninitialized.
        INVALID_STATE = -1
    };

    INTRUSIVE_LIST_TYPEDEF(LogSegment, listEntries) SegmentList;
    INTRUSIVE_LIST_TYPEDEF(LogSegment, allListEntries) AllSegmentList;
    typedef std::lock_guard<SpinLock> Lock;

    /// The private alloc() routine allocates segments for four different
    /// purposes: heads, emergency heads, regular SideLog segments, and
    /// cleaner SideLog segments. These enums specify which. They affect the
    /// pools from which segments (and seglets) are allocated, as well as the
    /// initial states of the segments returned.
    enum AllocPurpose {
        /// Allocate a head segment that entries may be appended to. The log
        /// will be rolled over to this new segment in memory and on backups.
        /// These segments are allocated from the default pool.
        ALLOC_HEAD = 0,

        /// Allocate an emergency head segment. The log will be rolled over to
        /// this new segment in memory and on backups, but it cannot be appended
        /// to. This simply serves to move the log forward and write out a new
        /// digest when memory is low. These segments are allocated from the
        /// emergency head pool.
        ALLOC_EMERGENCY_HEAD,

        /// Allocate a segment for use by a SideLog instance that is not being
        /// used for cleaning. These segments are allocated from the default
        /// pool.
        ALLOC_REGULAR_SIDELOG,

        /// Allocate a segment for use by a SideLog instance that is involved
        /// in log cleaning. These segments are allocated from the cleaner pool.
        /// This separate pool ensures that the system does not deadlock itself
        /// by allocating all segments and having nothing left to clean with.
        ALLOC_CLEANER_SIDELOG
    };

    LogSegment* alloc(AllocPurpose purpose,
                      uint64_t segmentId,
                      uint32_t creationTimestamp);
    void injectSideSegment(LogSegment* segment, State nextState, Lock& lock);
    void freeSegment(LogSegment* segment, bool waitForDigest, Lock& lock);
    void writeHeader(LogSegment* segment);
    void writeDigest(LogSegment* newHead, LogSegment* prevHead);
    void writeSafeVersion(LogSegment* head);
    void writeTableStatsDigest(LogSegment* head);
    LogSegment* getHeadSegment();
    void changeState(LogSegment& s, State newState);
    void addToLists(LogSegment& s);
    void removeFromLists(LogSegment& s);
    SegmentSlot allocSlot(AllocPurpose purpose);
    void freeSlot(SegmentSlot slot, bool wasEmergencyHead);
    void free(LogSegment* s);
    void freeUnreferencedSegments();

    /// The pervasive RAMCloud context.
    Context* context;

    /// Size of each full segment in bytes.
    const uint32_t segmentSize;

    /// ServerId this log will be tagged with (for example, in SegmentHeader
    /// structures).
    const ServerId* logId;

    /// Allocator to allocate segment memory (seglets) from.
    SegletAllocator& allocator;

    /// The ReplicaManager class instance that handles replication of our
    /// segments.
    ReplicaManager& replicaManager;

    /// MasterTableMetadata container that holds this master's per table
    /// metadata.  This is used to extract table stats information to be
    /// serialized during log head rollover.
    MasterTableMetadata* masterTableMetadata;

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
    /// For any index in segments that not containing a constructed LogSegment
    /// the corresponding index in this array will have the value INVALID_STATE.
    State* states;

    /// List of indices in 'segments' that are reserved for new emergency head
    /// segments.
    vector<SegmentSlot> freeEmergencyHeadSlots;

    /// List of indices in 'segments' that are reserved for new survivor
    /// segments allocated by the cleaner.
    vector<SegmentSlot> freeSurvivorSlots;

    /// List of indices in 'segments' that are free. This exists simply to allow
    /// for fast allocation of LogSegments.
    vector<SegmentSlot> freeSlots;

    /// Number of segments we need to reserve for emergency heads (to allow us
    /// to roll over to a new log digest when otherwise out of memory).
    const uint32_t emergencyHeadSlotsReserved;

    /// Number of segments the cleaner requested to keep reserved for it.
    uint32_t survivorSlotsReserved;

    /// Monotonically increasing identifier to be given to the next segment
    /// allocated by this module. The first segment allocated is given id 1.
    /// 0 and -1 are reserved values.
    uint64_t nextSegmentId;

    /// Lookup table from segment identifier to the corresponding index in
    /// 'segments' and 'states'. Currently used only to check for existence
    /// of a particular segment.
    std::unordered_map<uint64_t, SegmentSlot> idToSlotMap;

    /// Unordered linked list of all LogSegments managed by this module.
    AllSegmentList allSegments;

    /// Unordered linked lists of LogSegments based on their current state. Each
    /// segment is present in exactly one of these lists at any given time.
    SegmentList segmentsByState[TOTAL_STATES];

    /// Monitor lock protecting the SegmentManager from multiple calling
    /// threads. At a minimum, the log and log cleaner modules may operate
    /// simultaneously.
    SpinLock lock;

    /// Number of segments currently on backup disks. This is exactly the number
    /// of ReplicatedSegments that exist.
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
     * version number has no influence of the safeVersion, because the
     * safeVersion is used to keep the monotonicity of the version number of
     * any recreated object.
     *
     * \li When an object is removed, set the safeVersion
     * to the higher than any version number of the removed
     * object's version number. See #RaiseSafeVersion.
     *
     **/
    std::atomic_uint_fast64_t safeVersion;


    /// The following variables allow us to log messages if the epoch
    /// mechanism gets "stuck", where some old RPC is never completing and
    /// that prevents us from freeing cleaned segments. OldestRpcEpoch is
    /// the epoch of the oldest incomplete RPC.
    uint64_t oldestRpcEpoch;

    /// Time (in Cycles::rdtsc() units) of the beginning of a time interval
    /// during which oldestRpcEpoch is stuck at a particular value that is
    /// preventing segments from being freed.
    uint64_t stuckStartTime;

    /// Time (in seconds measured from stuckStartTime) at which we will
    /// generate the next log message indicating that segment freeing is
    /// stuck. 0 means we aren't stuck.
    double nextMessageSeconds;

    DISALLOW_COPY_AND_ASSIGN(SegmentManager);
};

} // namespace

#endif // !RAMCLOUD_SEGMENTMANAGER_H
