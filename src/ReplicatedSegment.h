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
 * TODO: Violates JO no more info than the name rule.
 * TODO: Who calls, what does it do (from external perspective), what is involved?
 * A segment that is being replicated to backups or is durable on backups.
 * Most of this class is used to store state internal to the BackupManager.
 */
class ReplicatedSegment : public Task {
  PUBLIC:
    void free();

    /// See OpenSegment::close().
    void close() {
        write(queued.bytes, true);
    }

    void write(uint32_t offset, bool closeSegment);

  PRIVATE:
    friend class BackupManager;

    ReplicatedSegment(BackupManager& backupManager, TaskManager& taskManager,
                      uint64_t segmentId, const void* data, uint32_t len,
                      uint32_t numReplicas);
    ~ReplicatedSegment();

    void performTask();

    /**
     * Return the number of bytes of space required on which to construct
     * an ReplicatedSegment instance.
     */
    static size_t sizeOf(uint32_t numReplicas) {
        return sizeof(ReplicatedSegment) + sizeof(replicas[0]) * numReplicas;
    }

    /// Intrusive list entries for #BackupManager::replicatedSegmentList.
    IntrusiveListHook listEntries;

    /**
     * Returns true when any data queued for this segment is durably
     * synced to #numReplicas including any outstanding flags.
     */
    bool isSynced() {
        return getDone() == queued;
    }

    /**
     * TODO Tracks something.
     * Different instances can track progress of different statuses.
     * For example, BackupManager tracks progress queued, sent, and
     * done replicating.
     */
    struct Progress {
        /// Whether an open has "happened" for this replica.
        bool open;

        /// Bytes that have reached a certain status.
        uint32_t bytes;

        /// Whether a close has "happened" for this replica.
        bool close;

        /**
         * Create an instance representing no progress.
         */
        Progress()
            : open(false), bytes(0), close(false) {}

        /**
         * Create an instance representing a specific amount of progress.
         *
         * \param open
         *      Whether the open operation on this replica has reached
         *      a certain status.
         * \param bytes
         *      Bytes that have reached a certain status.
         * \param close
         *      Whether the close operation on this replica has reached
         *      a certain status.
         */
        Progress(bool open, uint32_t bytes, bool close)
            : open(open), bytes(bytes), close(close) {}

        /**
         * Update in place with the minimum progress on each of field
         * between this instance and another.
         *
         * \param other
         *      Another Progress which will "shorten" this Progress if
         *      any of its fields have only reached a lesser progress.
         */
        void min(const Progress& other)
        {
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
        bool operator==(const Progress& other) const
        {
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
        bool operator!=(const Progress& other) const
        {
            return !(*this == other);
        }

        /**
         * Return true if this Progress is less than another.
         *
         * \param other
         *      Another Progress to compare against.
         */
        bool operator<(const Progress& other) const
        {
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

    /// The state needed for a single (partial) replica of this segment.
    struct Replica {
        explicit Replica(Transport::SessionRef session)
            : client(session)
            , sent()
            , done()
            , closeTicks()
            , writeRpc()
            , freeRpc()
        {
        }

        /// A client to the remote backup server.
        BackupClient client;

        /**
         * Tracks whether open and closes have been sent for this segment
         * and how many bytes of log data have been sent to this Backup.
         * (Note: but not necessarily acknowledged, see #done).
         */
        Progress sent;

        /**
         * Tracks whether open and closes have been synced for this segment
         * and how many bytes of log data have been synced to this Backup.
         */
        Progress done;

        /// Measures the amount of time the close RPC is active.
        Tub<CycleCounter<RawMetric>> closeTicks;

        /// The outstanding write operation to this backup, if any.
        Tub<BackupClient::WriteSegment> writeRpc;

        /// The outstanding free operation to this backup, if any.
        Tub<BackupClient::FreeSegment> freeRpc;

        DISALLOW_COPY_AND_ASSIGN(Replica);
    };

    void performFree(Tub<Replica>& replica);
    void performWrite(Tub<Replica>& replica);

    /**
     * Return the minimum Progress made in syncing this replica to Backups
     * for any of the replicas.
     */
    Progress getDone() {
        Progress p = queued;
        foreach (auto& replica, replicas) {
            if (replica)
                p.min(replica->done);
            else
                return Progress();
        }
        return p;
    }

    bool replicaIsPrimary(Tub<Replica>& replica) {
        return &replica == &replicas[0];
    }

    /**
     * The BackupManager instance which owns this ReplicatedSegment.
     */
    BackupManager& backupManager;

    /**
     * A unique ID for the segment.
     */
    const uint64_t segmentId;

    /**
     * The start of an array of bytes to be replicated.
     */
    const void* data;

    /**
     * The number of bytes to send atomically to backups with the open
     * segment RPC.
     */
    uint32_t openLen;

    /**
     * Tracks whether open and closes have been queued for this segment
     * and how many bytes of log data have been queued for replication
     * to Backups.
     */
    Progress queued;

    /// True if all known replicas of this segment should be eliminated.
    bool freeQueued;

    /**
     * An array of #BackupManager::replica backups on which to replicate
     * the segment.
     */
    VarLenArray<Tub<Replica>> replicas; // must be last member of class

    DISALLOW_COPY_AND_ASSIGN(ReplicatedSegment);
};

// TODO: Remove this once OpenSegment is removed from other classes.
typedef ReplicatedSegment OpenSegment;

} // namespace RAMCloud

#endif
