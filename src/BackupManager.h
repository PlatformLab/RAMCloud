/* Copyright (c) 2009-2010 Stanford University
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


#ifndef RAMCLOUD_BACKUPMANAGER_H
#define RAMCLOUD_BACKUPMANAGER_H

#include <unordered_map>
#include <boost/pool/pool.hpp>

#include "BoostIntrusive.h"
#include "BackupClient.h"
#include "Common.h"
#include "RawMetrics.h"
#include "Tub.h"
#include "VarLenArray.h"

namespace RAMCloud {

/**
 * Replicates segments to backup servers. This class is used on masters to
 * replicate segments of the log to backups. It handles selecting backup
 * servers on a segment-by-segment basis and replicates local segments to
 * remote backups.
 */
class BackupManager {
  PUBLIC:
    /**
     * A segment that is being replicated to backups.
     * Most of this class is used to store state internal to the BackupManager.
     */
    class OpenSegment {
      PUBLIC:
        /**
         * Convenience method for calling #write() with the last offset and
         * closeSegment set.
         */
        void close() {
            write(queued.bytes, true);
        }
        void write(uint32_t offset, bool closeSegment);

      PRIVATE:
        // The following private members are for BackupManager's use:
        friend class BackupManager;

        /**
         * Tracks progress in replicating a segment replica.
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
        };

        /**
         * Return the minimum Progress made in syncing this segment to Backups
         * for any of the Backups.
         */
        Progress getDone() {
            Progress p = queued;
            foreach (auto& backup, backups) {
                if (backup)
                    p.min(backup->done);
                else
                    return Progress();
            }
            return p;
        }

        /**
         * Return the number of bytes of space required on which to construct
         * an OpenSegment instance.
         */
        static size_t sizeOf(uint32_t numReplicas) {
            return sizeof(OpenSegment) + sizeof(backups[0]) * numReplicas;
        }

        OpenSegment(BackupManager& backupManager, uint64_t segmentId,
                    const void* data, uint32_t len);

        /**
         * The BackupManager instance which owns this OpenSegment.
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

        /**
         * Intrusive list entries for #BackupManager::openSegmentList.
         */
        IntrusiveListHook listEntries;

        /**
         * The state needed for a single (partial) replica of this segment.
         */
        struct Backup {
            explicit Backup(Transport::SessionRef session)
                : client(session)
                , sent()
                , done()
                , closeTicks()
                , rpc()
            {
            }

            /**
             * A handle to the remote backup server.
             */
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

            /**
             * Space for an asynchronous RPC call.
             */
            Tub<BackupClient::WriteSegment> rpc;
        };

        /**
         * An array of #BackupManager::replica backups on which to replicate
         * the segment.
         */
        VarLenArray<Tub<Backup>> backups; // must be last member of class

        DISALLOW_COPY_AND_ASSIGN(OpenSegment);
    };

    BackupManager(CoordinatorClient* coordinator,
                  const Tub<uint64_t>& masterId,
                  uint32_t numReplicas);
    explicit BackupManager(BackupManager* prototype);
    ~BackupManager();

    void freeSegment(uint64_t segmentId);
    OpenSegment* openSegment(uint64_t segmentId,
                             const void* data, uint32_t len);
        __attribute__((warn_unused_result));
    void sync();
    void proceed();
    void dumpOpenSegments(); // defined for testing only

  PRIVATE:
    /// Maximum number of bytes we'll send in any single write RPC
    /// to backups. The idea is to avoid starving other RPCs to the
    /// backup by not inundating it with segment-sized writes on
    /// recovery.
    static const uint32_t MAX_WRITE_RPC_BYTES = 1024 * 1024;

    void proceedNoMetrics();
    bool isSynced();
    void unopenSegment(OpenSegment* openSegment);

    /// Cluster coordinator. May be NULL for testing purposes.
    CoordinatorClient* const coordinator;

    /**
     * The coordinator-assigned server ID for this master or, equivalently, its
     * globally unique #Log ID.
     */
    const Tub<uint64_t>& masterId;

    /**
     * Selects backups on which to store replicas.
     */
    class BackupSelector {
      PUBLIC:
        typedef ProtoBuf::ServerList::Entry Backup;
        explicit BackupSelector(CoordinatorClient* coordinator);
        void select(uint32_t numBackups, Backup* backups[]);
        Backup* selectAdditional(uint32_t numBackups,
                                 const Backup* const backups[]);
      PRIVATE:
        Backup* getRandomHost();
        bool conflict(const Backup* a, const Backup* b) const;
        bool conflictWithAny(const Backup* a, uint32_t numBackups,
                                 const Backup* const backups[]) const;
        void updateHostListFromCoordinator();

        /// A hook for testing purposes.
        DelayedThrower<> updateHostListThrower;

        /// Cluster coordinator. May be NULL for testing purposes.
        CoordinatorClient* const coordinator;

        /// The list of backups from which to select.
        ProtoBuf::ServerList hosts;

        /**
         * Used in #getRandomHost(). This is some permutation of the integers
         * between 0 and hosts.size() - 1, inclusive.
         */
        vector<uint32_t> hostsOrder;

        /**
         * Used in #getRandomHost(). This is the number of backups that have
         * been returned by #getRandomHost() since its last pass over the
         * #hosts list.
         */
        uint32_t numUsedHosts;

        DISALLOW_COPY_AND_ASSIGN(BackupSelector);
    };
    BackupSelector backupSelector; ///< See #BackupSelector.

  PUBLIC:
    /// The number of backups to replicate each segment on.
    const uint32_t numReplicas;

  PRIVATE:
    /**
     * The mapped_type in ReplicaLocations.
     *
     * Tracks where one replica is stored in the cluster.
     */
    struct ReplicaLocation {
        ReplicaLocation(uint64_t backupId, Transport::SessionRef session)
            : backupId(backupId)
            , session(session)
        {
        }

        /// The serverId where this replica is stored.
        uint64_t backupId;

        /// A SessionRef to the Backup where this replica is stored.
        Transport::SessionRef session;
    };
    typedef std::unordered_multimap<uint64_t, ReplicaLocation>
            ReplicaLocations;
    /// Tells which backup each segment is stored on.
    ReplicaLocations replicaLocations;

    /// A pool from which all OpenSegment objects are allocated.
    boost::pool<> openSegmentPool;

    INTRUSIVE_LIST_TYPEDEF(OpenSegment, listEntries) OpenSegmentList;
    /**
     * A FIFO queue of all existing OpenSegment objects.
     * Newly opened segments are pushed to the back of this list.
     */
    OpenSegmentList openSegmentList;

    /// The number of RPCs that have been issued to backups but have not yet
    /// completed.
    int outstandingRpcs;

    /// Used to count the amount of time that outstandingRpcs > 0.
    Tub<CycleCounter<RawMetric>> activeTime;

    DISALLOW_COPY_AND_ASSIGN(BackupManager);
};

} // namespace RAMCloud

#endif
