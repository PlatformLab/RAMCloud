/* Copyright (c) 2011-2015 Stanford University
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

#ifndef RAMCLOUD_REPLICATEDSEGMENT_H
#define RAMCLOUD_REPLICATEDSEGMENT_H

#include <thread>

#include "Common.h"
#include "BackupClient.h"
#include "BackupSelector.h"
#include "BoostIntrusive.h"
#include "CycleCounter.h"
#include "UpdateReplicationEpochTask.h"
#include "RawMetrics.h"
#include "Transport.h"
#include "TaskQueue.h"
#include "VarLenArray.h"

namespace RAMCloud {

/**
 * Toggles human and machine readable log messages on the start and finish of
 * every replication write rpc, the receipt of recovery segments, and the start
 * and end of replay for each recovery segment. Log messages are stamped with
 * the number of microseconds elapsed since the start of recovery. This can be
 * used with scripts/extract-recovery-rpc-timeline.sh and
 * recovery-rpc-timeline.gnuplot to generate a human readable log and a
 * visualization of replication progress during recovery.
 * NOTE: You must use DEBUG log level as well for this to work.
 */
enum { LOG_RECOVERY_REPLICATION_RPC_TIMING = false };

/**
 * Acts as a handle for the log module to enqueue changes to its segments for
 * eventual replication and freeing; logically part of the BackupMananger.
 * The log module calls ReplicaManager::openSegment() to allocate and acquire
 * a ReplicatedSegment.  Users of this class should carefully read the
 * documentation for free(), close(), and write() to understand what
 * requirements they must meet and what guarantees are provided.
 *
 * ReplicatedSegment is also used internally by ReplicaManager, unless you are
 * interested in the guts of ReplicaManager and ReplicatedSegment you should
 * stop reading now.
 *
 * The ReplicaManager uses ReplicatedSegment to track the state of replication
 * of a log segment.  Because this class must internally handle and mask a wide
 * variety of failures from higher-level code it has a specialized style.  In
 * particular, each segment always tries to make progress (either trying to
 * replicate or trying free its replicas), whenever a failure occurs replica
 * state is reset to some well-known starting point.  For example, if a replica
 * has failed its state is reset to the same state as if the replica had never
 * been created to begin with.  Because of this the code order itself is
 * generally *not* used to sequence operations which means the code is
 * ambivelent if some replica's state reverts (due to a failure); whether its
 * the first time or the 30th the segment simply queues up the next step needed
 * to get the replicas in the state the log module has requested.  Continuing
 * the example, the segment would inspect its replicas' states and find it was
 * missing one replica.  In that case, it would proceed as normal (choosing a
 * new backup, sending out open rpcs, etc.).  ReplicatedSegments subclass Task
 * and are scheduled as part of the ReplicaManager's TaskQueue whenever their
 * state changed in a way that may cause them to need to perform work (write(),
 * close(), free(), or host failure).  ReplicatedSegments keep themselves
 * scheduled until they are in the state the log module has requested.
 */
class ReplicatedSegment : public Task {
  PUBLIC:
    /**
     * Internal to ReplicaManager; describes what to do whenever we don't want
     * to go on living.  ReplicaManager needs to do some special cleanup after
     * ReplicatedSegments, but ReplicatedSegments know best when to destroy
     * themselves.  The default logic does nothing which is useful for
     * ReplicatedSegment during unit testing.  Must be public because otherwise
     * it is impossible to subclass this; the friend declaration for
     * ReplicaManager doesn't work; it seems C++ doesn't consider the subclass
     * list to be part of the superclass's scope.
     */
    struct Deleter {
        virtual void destroyAndFreeReplicatedSegment(ReplicatedSegment*
                                                        replicatedSegment) {}
        virtual ~Deleter() {}
    };

  PRIVATE:
    /**
     * For internal use; represents "how much" of a segment or replica.
     * ReplicatedSegment must track "how much" of a segment or replica has
     * reached some status.  Concretely, separate instances are used to track
     * how much data the log module expects us to replicate, how much has been
     * sent to each backup, and how much has been acknowledged as at least
     * buffered by each backup.  This isn't simply a count of bytes because its
     * important to track the status of the open and closed flags as part of
     * the "how much".  Has value semantics.
     */
    struct Progress {
        /// Whether an open has been queued/sent/acknowledged.
        bool open;

        /// Bytes that have been queued/sent/acknowledged.
        uint32_t bytes;

        /**
         * A logical clock that ticks on backup failures. Replicas are modified
         * during some epoch. If a replica becomes lost replicas can be
         * recreated with in a new epoch and then the coordinator can be told
         * that any replica with an older epoch should not be used in recovery.
         * When a backup for some replica of an open segment fails this is
         * incremented in 'queued' which causes all replicas to transmit this
         * new epoch number to backups before being considered committed again.
         */
        uint64_t epoch;

        /// Whether a close has been queued/sent/acknowledged.
        bool close;

        /// Create an instance representing no progress.
        Progress()
            : open(false), bytes(0), epoch(0), close(false) {}

        /**
         * Create an instance representing a specific amount of progress.
         *
         * \param open
         *      Whether the open operation has been queued/sent/acknowledged.
         * \param bytes
         *      Bytes that have been queued/sent/acknowledged.
         * \param epoch
         *      Which epoch the data described by \a bytes is part of. See
         *      #epoch.
         * \param close
         *      Whether the close operation has been queued/sent/acknowledged.
         */
        Progress(bool open, uint32_t bytes, uint64_t epoch, bool close)
            : open(open), bytes(bytes), epoch(epoch), close(close) {}

        /**
         * Update in place with the minimum progress on each of field
         * between this Progress and another.
         *
         * \param other
         *      Another Progress which will "shorten" this Progress if
         *      any of its fields have made less progress.
         */
        void min(const Progress& other) {
            open &= other.open;
            if (bytes > other.bytes)
                bytes = other.bytes;
            // A bit subtle: if the other is closed then its epoch doesn't
            // matter. A closed replica can always be used during recovery.
            // Having "close" set is like having an infinite epoch.
            if (!other.close)
                epoch = std::min(epoch, other.epoch);
            close &= other.close;
        }

        /**
         * Return true if this Progress is exactly as much as another.
         *
         * \param other
         *      Another Progress to compare against.
         */
        bool operator==(const Progress& other) const {
            // A bit subtle: if two Progresses have replicated the same
            // bytes and are both closed then their epoch is irrelevant.
            // Having "close" set is like having an infinite epoch.
            return (open == other.open &&
                    bytes == other.bytes &&
                    ((close && other.close) ||
                     (!close && !other.close && epoch == other.epoch)));
        }

        /**
         * Return true if this Progress is not exactly as much as another.
         *
         * \param other
         *      Another Progress to compare against.
         */
        bool operator!=(const Progress& other) const {
            return !(*this == other);
        }

        /**
         * Return true if this Progress is less than another.
         *
         * \param other
         *      Another Progress to compare against.
         */
        bool operator<(const Progress& other) const {
            // Extremely subtle: Order matters here. epoch must be tested
            // after close. If the close bits are the same (be it set or
            // cleared) ONLY THEN should the epochs be compared. This is
            // because a closed replica is like having a replica with an
            // infinite epoch. It can never be invalidated.
            if (open < other.open)
                return true;
            else if (bytes < other.bytes)
                return true;
            else if (close < other.close)
                return true;
            else if (epoch < other.epoch)
                return true;
            else
                return false;
        }
    };

    /**
     * For internal use; stores all state for a single (potentially incomplete)
     * replica of a ReplicatedSegment.
     */
    class Replica {
      PUBLIC:
        Replica()
            : isActive(false)
            , backupId()
            , committed()
            , acked()
            , sent()
            , freeRpc()
            , writeRpc()
            , replicateAtomically(false)
        {}

        ~Replica() {
            if (writeRpc)
                writeRpc->cancel();
            if (freeRpc)
                freeRpc->cancel();
        }

        /**
         * Record which backup this replica in being replicated to and set up
         * the state so that the ReplicaManager loop will begin replication.
         *
         * \param backupId
         *      The ServerId of the process which is the target of this
         *      replica.
         */
        void start(ServerId backupId) {
            isActive = true;
            this->backupId = backupId;
        }

        /**
         * Mark this replica as having failed.  This has two effects. First,
         * the replica state will be reset so that future iterations of
         * the ReplicaManager will start rereplication. Second, it sets a
         * flag indicating that all future replication of this segment
         * should be done atomically.  That is, the segment is either
         * replicated completely to close or the backup disavows all knowledge
         * of it.
         */
        void failed() {
            this->~Replica();
            new(this) Replica;
            replicateAtomically = true;
        }

        /**
         * Reset all state associated with this replica.  Note that it is the
         * boolean freeQueued in ReplicatedSegment that prevents this replica
         * from being replicated again by the main ReplicaManager loop if the
         * replica has been freed.
         */
        void reset() {
            this->~Replica();
            new(this) Replica;
        }

        /**
         * If true the rest of the fields in this structure are valid and
         * represent the known state of some replica.  Otherwise, this
         * instance is not associated with a replica (which indicates to
         * the ReplicaManager loop that a new replica should be created to
         * fill this missing replica).
         */
        bool isActive;

        /// Id of remote backup server where this replica is (to be) stored.
        ServerId backupId;

        /**
         * Tracks how much of a replica is durably stored on a backup and will
         * be available during master recovery if the backup is available at
         * the time of recovery. Not all data acked as received by a backup is
         * committed. Some backup rpcs may be sent without a certificate (if
         * the data is too large to send in a single rpc or if the replica is
         * being created atomically). In such a case the data in that rpc will
         * not be part of a recovery even if it is acknowledged by the backup.
         * It only becomes committed when some subsequent provides a new
         * certificate for the segment.
         */
        Progress committed;

        /**
         * Tracks how much of a replica has been acknowledged as received
         * by a backup. (Note: acked data will *not* necessarily be
         * recovered by a backup; see #committed).
         */
        Progress acked;

        /**
         * Tracks how much of a replica has been sent to be stored on a
         * backup. (Note: but not necessarily acknowledged or committed,
         * see #acked and #committed).
         */
        Progress sent;

        /// The outstanding free operation to this backup, if any.
        Tub<FreeSegmentRpc> freeRpc;

        /// The outstanding write operation to this backup, if any.
        Tub<WriteSegmentRpc> writeRpc;

        // Fields below survive across failed()/start() calls.

        /**
         * This is set whenever replication is happening in response to a
         * failure.  It indicates that future replication should be done
         * atomically.  That is the if the backup receives the entire replica
         * including its close then it can be used during recovery, otherwise
         * if the close has not been received by the backup it will disavow
         * all knowledge of the replica.  This is used to prevent replicas
         * which are being recreated for closed segments in response to
         * failures from appearing open during a recovery and silently
         * truncating the log.
         */
        bool replicateAtomically;

        DISALLOW_COPY_AND_ASSIGN(Replica);
    };

// --- ReplicatedSegment ---
  PUBLIC:
    void free();
    bool isSynced() const;
    void close();
    void handleBackupFailure(ServerId failedId, bool useMinCopysets);
    void sync(uint32_t offset = ~0u, SegmentCertificate* certificate = NULL);
    const Segment* swapSegment(const Segment* newSegment);

    /**
     * Unsafe, but handy global used to generate time-since-start-of-recovery
     * stamps for recovery replication debugging log messages. Simply holds
     * the TSC at the start of recovery on this recovery master.
     * Set in MasterService::recover(). See LOG_RECOVERY_REPLICATION_RPC_TIMING
     * above.
     */
    static uint64_t recoveryStart;

  PRIVATE:
    friend class ReplicaManager;

    /**
     * Maximum number of simultaneous outstanding write rpcs to backups
     * to allow across all ReplicatedSegments.
     */
    enum { MAX_WRITE_RPCS_IN_FLIGHT = 8 };

    /**
     * Maximum number of simultaneous outstanding free rpcs to backups
     * to allow across all ReplicatedSegments.
     */
    enum { MAX_FREE_RPCS_IN_FLIGHT = 3 };

    ReplicatedSegment(Context* context,
                      TaskQueue& taskQueue,
                      BaseBackupSelector& backupSelector,
                      Deleter& deleter,
                      uint32_t& writeRpcsInFlight,
                      uint32_t& freeRpcsInFlight,
                      UpdateReplicationEpochTask& replicationEpoch,
                      std::mutex& dataMutex,
                      uint64_t segmentId,
                      const Segment* segment,
                      bool normalLogSegment,
                      ServerId masterId,
                      uint32_t numReplicas,
                      Tub<CycleCounter<RawMetric>>* replicationCounter = NULL,
                      uint32_t maxBytesPerWriteRpc = 1024 * 1024);
    ~ReplicatedSegment();

    void schedule();
    void performTask();
    void performFree(Replica& replica);
    void performWrite(Replica& replica);

    void dumpProgress();

    /**
     * Returns the minimum progress any Replica has made in durably
     * committing data to its chosen backup.
     */
    Progress getCommitted() const {
        Progress p = queued;
        foreach (auto& replica, replicas) {
            if (replica.isActive)
                p.min(replica.committed);
            else
                return Progress();
        }
        return p;
    }

    /// Return true if this replica should be considered the primary replica.
    bool replicaIsPrimary(Replica& replica) const {
        return &replica == &replicas[0];
    }

    /// Bytes needed to hold a ReplicatedSegment instance due to VarLenArray.
    static size_t sizeOf(uint32_t numReplicas) {
        return sizeof(ReplicatedSegment) + sizeof(replicas[0]) * numReplicas;
    }

// - member variables -
    /// Shared RAMCloud information.
    Context* context;

    /// Used to choose where to store replicas. Shared among ReplicatedSegments.
    BaseBackupSelector& backupSelector;

    /// Deletes this when this determines it is no longer needed.  See #Deleter.
    Deleter& deleter;

    /**
     * Number of outstanding write rpcs to backups across all
     * ReplicatedSegments.  Used to throttle write rpcs.
     */
    uint32_t& writeRpcsInFlight;

    /**
     * Number of outstanding free rpcs to backups across all
     * ReplicatedSegments.  Needed because the cleaner can potentially
     * generate a lot of free segments at once, which could use up
     * all outgoing RPC resources, causing other activity to come to
     * a halt.
     */
    uint32_t& freeRpcsInFlight;

    /**
     * Provides access to the latest replicationEpoch acknowledged by the
     * coordinator for this server and allows easy, asynchronous updates
     * to the value stored on the coordinator.
     * Used when a replica is lost while this segment was not durably closed.
     */
    UpdateReplicationEpochTask& replicationEpoch;

    /**
     * Reference to ReplicaManager::dataMutex. See documentation for
     * ReplicaManager::dataMutex; there are many subtleties to the locking
     * in this module.
     */
    std::mutex& dataMutex;
    typedef std::lock_guard<std::mutex> Lock;

    /**
     * Used to select a "winner" thread that is allowed to sync. Once some
     * thread acquires this lock it holds it until its sync() call returns.
     * Threads take turns driving replication in sync(), though work is batched
     * (that is, some thread may sync data that another thread is waiting on).
     * A lock on #dataMutex is still needed to access or modify fields in this
     * or any other segment.
     * See ReplicaManager::dataMutex; there are many subtleties to the locking
     * in this module.
     */
    std::mutex syncMutex;

    /**
     * Segment to be replicated. It is expected that the segment already
     * contains a header when this ReplicatedSegment is constructed;
     * see #openLen.
     */
    const Segment* segment;

    /**
     * False if this segment was/is being created by the log cleaner,
     * true if this segment was opened as a head of the log (that is,
     * it has a log digest and was/is actively written to by worker
     * as a log head).
     */
    const bool normalLogSegment;

    /// The server id of the Master whose log this segment belongs to.
    const ServerId masterId;

    /// Id for the segment, must match the segmentId given by the log module.
    const uint64_t segmentId;

    /**
     * Maximum number of bytes to send in any single write rpc
     * to backups. The idea is to avoid starving other rpcs to the
     * backup by not inundating it with segment-sized writes on
     * recovery.
     */
    const uint32_t maxBytesPerWriteRpc;

    /**
     * Tracks how much of a segment the log module has made available for
     * replication.
     */
    Progress queued;

    /**
     * Provided by the #segment which is being replicated that must be stored
     * along with the replica in storage. During recovery this certificate is
     * used to determine how much of the replica is valid and to detect
     * corruption. Therefore, it must be in place for updates to replicas to be
     * considered durable. If replica manager cannot send all the data ready to
     * be replicated in the next rpc (that is, when #queued.bytes -
     * #acked.bytes > maxBytesPerWriteRpc) it will not send a certificate with
     * the write. When this happens the backup ensures the most recently
     * transmitted certificate is used if the replica is closed or recovered in
     * a way such that all writes not covered by the certificate (since the
     * last write containing a certificate) will be atomically undone.
     */
    SegmentCertificate queuedCertificate;

    /**
     * Bytes to send atomically to backups with the opening backup write rpc.
     * Queried from #segment when this ReplicatedSegment is constructed.
     */
    uint32_t openLen;

    /**
     * Similar to #queuedCertificate, except it is the certificate just for the
     * data sent to the backup with the opening write. Must be kept separately
     * because new data with new certificate can be queued for replication
     * before the opening write has been sent but the opening write must be
     * sent without any object data (due to the no-write-before-preceding-close
     * rule).
     */
    SegmentCertificate openingWriteCertificate;

    /// True if all known replicas of this segment should be freed on backups.
    bool freeQueued;

    /**
     * The segment that logically follows this one in the log; set by close().
     * Needed to make two guarantees.
     * 1) A new head segment is durably open before closes can be sent for
     *    this segment (by checking followingSegment.getCommitted().open).
     * 2) followingSegment receives no writes (beyond its opening write) before
     *    this segment is durably closed (by setting
     *    followingSegment.precedingSegmentCloseCommitted) when
     *    this->getCommitted().close is set).
     * See close() for more details about these guarantees.
     */
    ReplicatedSegment* followingSegment;

    /**
     * No write rpcs (beyond the opening write rpc) for this segment are
     * allowed until precedingSegmentCloseCommitted becomes true.  This
     * constraint is only enforced for segments which have a followingSegment
     * (see close(), other segments created as part of the log cleaner and unit
     * tests skip this check).  This segment must wait to send write rpcs until
     * the preceding segment in the log sets it to true (the preceding segment
     * is the one which has this segment as its followingSegment).  This
     * happens immedately after the preceding segment is durably closed on
     * backups.  The goal is to prevent data written in this segment from being
     * undetectably lost in the case that all replicas of it are lost.
     */
    bool precedingSegmentCloseCommitted;

    /**
     * An open rpc for a replica cannot be issued until the open for the
     * prior segment in the log has been acknowledged. This constraint is
     * only enfoced for segments which have a followingSegment (see close(),
     * other segments created as part of the log cleaner and unit tests skip
     * this check. This segment waits until the preceding segment sets this
     * to true which happens immediately after the preceding segment receives
     * acknowledgment back from its replicas for its opening write. The goal
     * is to prevent log digests which occur later in the log from being
     * written before the replicas which it mentions have been made durable.
     * Otherwise, data loss could be detected on recovery.
     */
    bool precedingSegmentOpenCommitted;

    /**
     * Set to true if this segment lost a replica while it was open and hasn't
     * finished recovering from it yet, once recovery has completed
     * (that is, this segment has been durably closed, a successor with the
     * digest is durably open and, this segment's lost open replicas are
     * invalidated using #replicationEpoch) the flag is cleared.
     */
    bool recoveringFromLostOpenReplicas;

    /// Intrusive list entries for #ReplicaManager::replicatedSegmentList.
    IntrusiveListHook listEntries;

    /**
     * Used to measure time when backup write rpcs are active.
     * Shared among ReplicatedSegments.
     */
    Tub<CycleCounter<RawMetric>>* replicationCounter;

    /**
     * Used to figure out how long it takes the segment to get to a
     * properly opened state.  If non-zero, it means the segment has
     * not yet reached a state where all of its replicas are open;
     * the value indicates the time (in rdtsc cycles) when the segment
     * was constructed.
     */
    uint64_t unopenedStartCycles;

    /**
     * An array of #ReplicaManager::replica backups on which the segment is
     * (being) replicated.
     */
    VarLenArray<Replica> replicas; // must be last member of class

    DISALLOW_COPY_AND_ASSIGN(ReplicatedSegment);
};

} // namespace RAMCloud

#endif
