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

// forward declarations
class TaskManager;
class Task;
class BackupManager;
class ReplicatedSegment;

/**
 * Tracks state of replication of a log segment and reacts to cluster changes to
 * restore replication invariants; logically part of BackupManager.
 *
 * There are two users of ReplicatedSegment:
 * 1) The log module calls BackupManager::openSegment() which returns a pointer
 *    to a ReplicatedSegment.  From the log's perspective only the public
 *    methods are interesting.  They allow the log to queue portions of the
 *    log segment data for replication and to eventually free replicas of the
 *    log segment once the segment has been cleaned.
 * 2) The BackupManager uses ReplicatedSegment to track the progress made in
 *    replicating the log segment's data.  The basic idea is that when cluster
 *    failures occur the ReplicatedSegment is made aware of the failure.  The
 *    ReplicatedSegment then schedules itself as part of the work that the
 *    BackupManager does and attempts to restore the replication invariants
 *    for its assocaited log segment.  Anytime the ReplicatedSegment meets
 *    all replication invariants for its log segment data it deschedules itself
 *    and only "wakes up" on future cluster membership changes that might
 *    affect replication invariants for its log segment.
 */
class ReplicatedSegment : public Task {
  PUBLIC:
    /**
     * Describes what to do whenever we don't want to go on living.
     * BackupManager needs to do some specicalize cleanup after
     * ReplicatedSegments, but ReplicatedSegment best know when to destroy
     * themselves.
     * The default logic does nothing which is useful for ReplicatedSegment
     * during unit testing.
     * Must be public because otherwise it is impossible to subclass this;
     * the friend declaration for BackupManager doesn't work; it seems C++ doesn't
     * consider the subclass list to be part of the superclass's scope.
     */
    struct Deleter {
        virtual void destroyAndFreeReplicatedSegment(ReplicatedSegment*
                                                        replicatedSegment) {}
        virtual ~Deleter() {}
    };

  PRIVATE:
    /**
     * Represents "how much" of a segment or replica. Has value semantics.
     * ReplicatedSegment must track "how much" of a segment or replica has
     * reached some status.  Concretely, separate instances are used to track
     * how much data the log module expects us to replicate, how much has been
     * sent to each backup, and how much has been acknowledged as at least
     * buffered by each backup.  This isn't simply a count of bytes because its
     * important to track the status of the open and closed flags as part of
     * the "how much".
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
     * Stores all state for a single (potentially incomplete) replica of
     * a ReplicatedSegment.
     */
    struct Replica {
        explicit Replica(uint64_t backupId, Transport::SessionRef session)
            : backupId(backupId)
            , client(session)
            , sent()
            , acked()
            , writeRpc()
            , freeRpc() {}

        /// Id of remote backup server where this replica is (to be) stored.
        const uint64_t backupId;

        /// Client to remote backup server where this replica is (to be) stored.
        BackupClient client;

        /**
         * Tracks how much of a segment has been sent to be buffered
         * durably on a backup.
         * (Note: but not necessarily acknowledged, see #acked).
         */
        Progress sent;

        /**
         * Tracks how much of a segment has been acknowledged as buffered
         * durably on a backup.
         */
        Progress acked;

        /// The outstanding write operation to this backup, if any.
        Tub<BackupClient::WriteSegment> writeRpc;

        /// The outstanding free operation to this backup, if any.
        Tub<BackupClient::FreeSegment> freeRpc;

        DISALLOW_COPY_AND_ASSIGN(Replica);
    };

// --- ReplicatedSegment ---
  PUBLIC:
    void free();
    void close() { write(queued.bytes, true); }
    void write(uint32_t offset, bool closeSegment);

  PRIVATE:
    friend class BackupManager;

    ReplicatedSegment(TaskManager& taskManager,
                      BaseBackupSelector& backupSelector,
                      Deleter& deleter,
                      uint64_t masterId, uint64_t segmentId,
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
    Progress getAcked() {
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
     * segment is durably synced to #numReplicas including any outstanding flags.
     */
    bool isSynced() { return getAcked() == queued; }

    /// Return true if this replica should be considered the primary replica.
    bool replicaIsPrimary(Tub<Replica>& replica) {
        return &replica == &replicas[0];
    }

    /// Return the bytes required to construct an ReplicatedSegment instance.
    static size_t sizeOf(uint32_t numReplicas) {
        return sizeof(ReplicatedSegment) + sizeof(replicas[0]) * numReplicas;
    }

// - member variables -
    /// Used to choose where to store replicas. Shared among ReplicatedSegments.
    BaseBackupSelector& backupSelector;

    /// The server id of the Master whose log this segment belongs to.
    const uint64_t masterId;

    /// Id for the segment, must match the segmentId given by the log module.
    const uint64_t segmentId;

    /// The start of raw bytes of the in-memory log segment to be replicated.
    const void* data;

    /// Bytes to send atomically to backups with the open segment RPC.
    const uint32_t openLen;

    /**
     * Maximum number of bytes we'll send in any single write RPC
     * to backups. The idea is to avoid starving other RPCs to the
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

    /// Intrusive list entries for #BackupManager::replicatedSegmentList.
    IntrusiveListHook listEntries;

    /// Deletes this when this determines it is no longer needed.  See #Deleter.
    Deleter& deleter;

    /**
     * An array of #BackupManager::replica backups on which the segment is
     * (being) replicated.
     */
    VarLenArray<Tub<Replica>> replicas; // must be last member of class

    DISALLOW_COPY_AND_ASSIGN(ReplicatedSegment);
};

// TODO: Remove this once OpenSegment is removed from other classes.
typedef ReplicatedSegment OpenSegment;

} // namespace RAMCloud

#endif
