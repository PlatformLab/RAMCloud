/* Copyright (c) 2011 Stanford University
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

#include "Common.h"
#include "BackupClient.h"
#include "BackupSelector.h"
#include "BoostIntrusive.h"
#include "RawMetrics.h"
#include "Transport.h"
#include "TaskManager.h"
#include "VarLenArray.h"

namespace RAMCloud {

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
 * and are scheduled as part of the ReplicaManager's TaskManager whenever their
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

        /// Whether a close has been queued/sent/acknowledged.
        bool close;

        /// Create an instance representing no progress.
        Progress()
            : open(false), bytes(0), close(false) {}

        /**
         * Create an instance representing a specific amount of progress.
         *
         * \param open
         *      Whether the open operation has been queued/sent/acknowledged.
         * \param bytes
         *      Bytes that have been queued/sent/acknowledged.
         * \param close
         *      Whether the close operation has been queued/sent/acknowledged.
         */
        Progress(bool open, uint32_t bytes, bool close)
            : open(open), bytes(bytes), close(close) {}

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
            close &= other.close;
        }

        /**
         * Return true if this Progress is exactly as much as another.
         *
         * \param other
         *      Another Progress to compare against.
         */
        bool operator==(const Progress& other) const {
            return (open == other.open &&
                    bytes == other.bytes &&
                    close == other.close);
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
            if (open < other.open)
                return true;
            else if (bytes < other.bytes)
                return true;
            else if (close < other.close)
                return true;
            else
                return false;
        }

        /**
         * Return true if this Progress is greater or equal to another.
         *
         * \param other
         *      Another Progress to compare against.
         */
        bool operator>=(const Progress& other) const
        {
            return !(*this < other);
        }
    };

    /**
     * For internal use; stores all state for a single (potentially incomplete)
     * replica of a ReplicatedSegment.
     */
    struct Replica {
        explicit Replica(ServerId backupId, Transport::SessionRef session)
            : backupId(backupId)
            , client(session)
            , acked()
            , sent()
            , freeRpc()
            , writeRpc()
        {}

        /// Id of remote backup server where this replica is (to be) stored.
        const ServerId backupId;

        /// Client to remote backup server where this replica is (to be) stored.
        BackupClient client;

        /**
         * Tracks how much of a segment has been acknowledged as buffered
         * durably on a backup.
         */
        Progress acked;

        /**
         * Tracks how much of a segment has been sent to be buffered
         * durably on a backup.
         * (Note: but not necessarily acknowledged, see #acked).
         */
        Progress sent;

        /// The outstanding free operation to this backup, if any.
        Tub<BackupClient::FreeSegment> freeRpc;

        /// The outstanding write operation to this backup, if any.
        Tub<BackupClient::WriteSegment> writeRpc;

        DISALLOW_COPY_AND_ASSIGN(Replica);
    };

// --- ReplicatedSegment ---
  PUBLIC:
    void free();
    void close(ReplicatedSegment* followingSegment);
    void write(uint32_t offset);

  PRIVATE:
    friend class ReplicaManager;

    /**
     * Maximum number of simultaenously outstanding write rpcs to backups
     * to allow across all ReplicatedSegments.
     */
    enum { MAX_WRITE_RPCS_IN_FLIGHT = 4 };

    ReplicatedSegment(TaskManager& taskManager,
                      BaseBackupSelector& backupSelector,
                      Deleter& deleter,
                      uint32_t& writeRpcsInFlight,
                      ServerId masterId, uint64_t segmentId,
                      const void* data, uint32_t openLen,
                      uint32_t numReplicas,
                      uint32_t maxBytesPerWriteRpc = 1024 * 1024);
    ~ReplicatedSegment();

    void performTask();
    void performFree(Tub<Replica>& replica);
    void performWrite(Tub<Replica>& replica);

    /**
     * Return the minimum Progress made in syncing this replica to Backups
     * for any of the replicas.
     */
    Progress getAcked() const {
        Progress p = queued;
        foreach (auto& replica, replicas) {
            if (replica)
                p.min(replica->acked);
            else
                return Progress();
        }
        return p;
    }

    /**
     * Returns true when all data queued for writing by the log module for this
     * segment is durably synced to #numReplicas including any outstanding
     * flags.
     */
    bool isSynced() const { return getAcked() == queued; }

    /// Return true if this replica should be considered the primary replica.
    bool replicaIsPrimary(Tub<Replica>& replica) const {
        return &replica == &replicas[0];
    }

    /// Bytes needed to hold a ReplicatedSegment instance due to VarLenArray.
    static size_t sizeOf(uint32_t numReplicas) {
        return sizeof(ReplicatedSegment) + sizeof(replicas[0]) * numReplicas;
    }

// - member variables -
    /// Used to choose where to store replicas. Shared among ReplicatedSegments.
    BaseBackupSelector& backupSelector;

    /// Deletes this when this determines it is no longer needed.  See #Deleter.
    Deleter& deleter;

    /**
     * Number of outstanding write rpcs to backups across all
     * ReplicatedSegments.  Used to throttle write rpcs.
     */
    uint32_t& writeRpcsInFlight;

    /// The server id of the Master whose log this segment belongs to.
    const ServerId masterId;

    /// Id for the segment, must match the segmentId given by the log module.
    const uint64_t segmentId;

    /// The start of raw bytes of the in-memory log segment to be replicated.
    const void* data;

    /// Bytes to send atomically to backups with the open segment rpc.
    const uint32_t openLen;

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

    /// True if all known replicas of this segment should be freed on backups.
    bool freeQueued;

    /**
     * The segment that logically follows this one in the log; set by close().
     * Needed to make two guarantees.
     * 1) A new head segment is durably open before closes can be sent for
     *    this segment (by checking followingSegment.getAcked().open).
     * 2) followingSegment receives no writes (beyond its opening write) before
     *    this segment is durably closed (by setting
     *    followingSegment.precedingSegmentCloseAcked) when
     *    this->getAcked().close is set).
     * See close() for more details about these guarantees.
     */
    ReplicatedSegment* followingSegment;

    /**
     * No write rpcs (beyond the opening write rpc) for this segment are
     * allowed until precedingSegmentCloseAcked becomes true.  This constraint
     * is only enforced for segments which have a followingSegment (see
     * close(), other segments created as part of the log cleaner and unit
     * tests skip this check).  This segment must wait to send write rpcs until
     * the preceding segment in the log sets it to true (the preceding segment
     * is the one which has this segment as its followingSegment).  This
     * happens immedately after the preceding segment is durably closed on
     * backups.  The goal is to prevent data written in this segment from being
     * undetectably lost in the case that all replicas of it are lost.
     */
    bool precedingSegmentCloseAcked;

    /// Intrusive list entries for #ReplicaManager::replicatedSegmentList.
    IntrusiveListHook listEntries;

    /**
     * An array of #ReplicaManager::replica backups on which the segment is
     * (being) replicated.
     */
    VarLenArray<Tub<Replica>> replicas; // must be last member of class

    DISALLOW_COPY_AND_ASSIGN(ReplicatedSegment);
};

} // namespace RAMCloud

#endif
