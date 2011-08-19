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
#include "Metrics.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * Orchestrates operations on backup servers.
 * This class handles selecting backup servers on a segment-by-segment basis
 * and relays backup operations to those servers.
 */
class BackupManager {
  PUBLIC:
    /// Represents a segment that is being replicated to backups.
    class OpenSegment {

      PUBLIC:
        /**
         * Convenience method for calling #write() with the last offset and
         * closeSegment set.
         */
        void close() {
            write(offsetQueued, true);
        }
        void write(uint32_t offset, bool closeSegment);

      PRIVATE:
        // The following private members are for BackupManager's use:
        friend class BackupManager;

        /**
         * Return the number of bytes of space required on which to construct
         * an OpenSegment instance.
         */
        static size_t sizeOf(uint32_t replicas) {
            return sizeof(OpenSegment) + sizeof(backups[0]) * replicas;
        }
        OpenSegment(BackupManager& backupManager, uint64_t segmentId,
                    const void* data, uint32_t len);
        ~OpenSegment();
        void sync();

        // The following are really private:

        struct Backup {
            explicit Backup(Transport::SessionRef session)
                : client(session)
                , openIsDone(false)
                , offsetSent(0)
                , closeSent(false)
                , closeTicks()
                , rpc()
            {}
            BackupClient client;

            bool openIsDone;
            /**
             * The number of bytes of #OpenSegment::data that have been
             * transmitted to the backup (but not necessarily acknowledged).
             */
            uint32_t offsetSent;
            /**
             * Whether the backup has been told that the segment is closed (but
             * not necessarily acknowledged).
             */
            bool closeSent;

            /// Measures the amount of time the close RPC is active.
            Tub<CycleCounter<Metric>> closeTicks;

            /**
             * Space for an asynchronous RPC call.
             */
            Tub<BackupClient::WriteSegment> rpc;
        };

        /**
         * Return a range of iterators across #backups
         * for use in foreach loops.
         */
        std::pair<Tub<Backup>*, Tub<Backup>*>
        backupIter() {
            return {&backups[0], &backups[backupManager.replicas]};
        }

        void sendWriteRequests();
        void waitForWriteRequests();

        BackupManager& backupManager;
        /**
         * A unique ID for the segment.
         */
        const uint64_t segmentId;
        /**
         * The start of an array of bytes to be replicated.
         */
        const void* data;

        uint32_t openLen;

        /**
         * The number of bytes of #data written by the user of this class (not
         * necessarily yet replicated).
         */
        uint32_t offsetQueued;
        /**
         * Whether the user of this class has closed this segment (not
         * necessarily yet replicated).
         */
        bool closeQueued;
        /**
         * Intrusive list entries for #BackupManager::openSegmentList.
         */
        IntrusiveListHook listEntries;
        /**
         * An array of #BackupManager::replica backups on which to replicate
         * the segment.
         */
        Tub<Backup> backups[0]; // must be last member of class
        DISALLOW_COPY_AND_ASSIGN(OpenSegment);
    };

    BackupManager(CoordinatorClient* coordinator,
                  const Tub<uint64_t>& masterId,
                  uint32_t replicas);
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
    void proceedNoMetrics();
    void ensureSufficientHosts();
    bool isSynced();
    void unopenSegment(OpenSegment* openSegment);
    void updateHostListFromCoordinator();

    /// Cluster coordinator. May be NULL for testing purposes.
    CoordinatorClient* const coordinator;

    /**
     * The coordinator-assigned server ID for this master or, equivalently, its
     * globally unique #Log ID.
     */
    const Tub<uint64_t>& masterId;

    /// The host pool to schedule backups from.
    ProtoBuf::ServerList hosts;

  PUBLIC:
    /// The number of backups to replicate each segment on.
    const uint32_t replicas;

  PRIVATE:
    typedef std::unordered_multimap<uint64_t, Transport::SessionRef>
            SegmentMap;
    /// Tells which backup each segment is stored on.
    SegmentMap segments;

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
    Tub<CycleCounter<Metric>> activeTime;

    DISALLOW_COPY_AND_ASSIGN(BackupManager);
};

} // namespace RAMCloud

#endif
