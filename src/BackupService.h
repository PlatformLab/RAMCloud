/* Copyright (c) 2009-2012 Stanford University
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

#ifndef RAMCLOUD_BACKUPSERVICE_H
#define RAMCLOUD_BACKUPSERVICE_H

#include <thread>
#include <map>

#include "Common.h"
#include "BackupClient.h"
#include "BackupMasterRecovery.h"
#include "BackupStorage.h"
#include "CoordinatorClient.h"
#include "MasterClient.h"
#include "Service.h"
#include "ServerConfig.h"
#include "TaskQueue.h"

namespace RAMCloud {

/**
 * Handles rpc requests from Masters and the Coordinator to persistently store
 * replicas of segments and to facilitate the recovery of object data when
 * masters crash.
 */
class BackupService : public Service
                    , ServerTracker<void>::Callback {
  PUBLIC:
    BackupService(Context* context, const ServerConfig* config);
    virtual ~BackupService();
    void benchmark();
    void dispatch(WireFormat::Opcode opcode, Rpc* rpc);
    ServerId getFormerServerId() const;
    ServerId getServerId() const;
    uint32_t getReadSpeed() { return readSpeed; }

  PRIVATE:
    void freeSegment(const WireFormat::BackupFree::Request* reqHdr,
                     WireFormat::BackupFree::Response* respHdr,
                     Rpc* rpc);
    void getRecoveryData(
        const WireFormat::BackupGetRecoveryData::Request* reqHdr,
        WireFormat::BackupGetRecoveryData::Response* respHdr,
        Rpc* rpc);
    void killAllStorage();
    void recoveryComplete(
        const WireFormat::BackupRecoveryComplete::Request* reqHdr,
        WireFormat::BackupRecoveryComplete::Response* respHdr,
        Rpc* rpc);
    void restartFromStorage();
    void startReadingData(
        const WireFormat::BackupStartReadingData::Request* reqHdr,
        WireFormat::BackupStartReadingData::Response* respHdr,
        Rpc* rpc);
    void startPartitioningReplicas(
        const WireFormat::BackupStartPartitioningReplicas::Request* reqHdr,
        WireFormat::BackupStartPartitioningReplicas::Response* respHdr,
        Rpc* rpc);
    void writeSegment(const WireFormat::BackupWrite::Request* req,
                      WireFormat::BackupWrite::Response* resp,
                      Rpc* rpc);
    void gcMain();
    void initOnceEnlisted();
    void trackerChangesEnqueued();

    /**
     * Shared RAMCloud information.
     */
    Context* context;

    /**
     * Provides mutual exclusion between handling RPCs and garbage collector.
     * Locked once for all RPCs in dispatch().
     */
    std::mutex mutex;
    typedef std::mutex Mutex;
    typedef std::unique_lock<Mutex> Lock;

    /// Settings passed to the constructor
    const ServerConfig* config;

    /**
     * If the backup was formerly part of a cluster this was its server id.
     * This is extracted from a superblock that is part of BackupStorage.
     * "Rejoining" means this backup service may have segment replicas stored
     * that were created by masters in the cluster.
     * In this case, the coordinator must be made told of the former server
     * id under which these replicas were created in order to ensure that
     * all masters are made aware of the former server's crash before learning
     * of its re-enlistment.
     */
    ServerId formerServerId;

    /**
     * The storage backend where closed segments are to be placed.
     * Must come before #frames so that if the reference count of some frames
     * drops to zero when the map is destroyed won't have been destroyed yet
     * in the storage instance.
     */
  PUBLIC:
    std::unique_ptr<BackupStorage> storage;
  PRIVATE:

    /// Type of the key for the frame map.
    struct MasterSegmentIdPair {
        MasterSegmentIdPair(ServerId masterId, uint64_t segmentId)
            : masterId(masterId)
            , segmentId(segmentId)
        {
        }

        /// Comparison is needed for the type to be a key in a map.
        bool
        operator<(const MasterSegmentIdPair& right) const
        {
            return std::make_pair(*masterId, segmentId) <
                   std::make_pair(*right.masterId, right.segmentId);
        }

        ServerId masterId;
        uint64_t segmentId;
    };
    /// Type of the frame map.
    typedef std::map<MasterSegmentIdPair, BackupStorage::FrameRef> FrameMap;
    /**
     * Mapping from (MasterId, SegmentId) to a BackupStorage::FrameRef for
     * replicas that are currently open or in storage.
     */
    FrameMap frames;

    /**
     * Master recoveries this backup is participating in; maps a crashed master
     * id to the most recent recovery that was started for it. Entries
     * added in startReadingData and removed by garbage collection tasks when
     * the crashed master is marked as down in the server list.
     */
    std::map<ServerId, BackupMasterRecovery*> recoveries;

    /// The uniform size of each segment this backup deals with.
    const uint32_t segmentSize;

    /// The results of storage.benchmark() in MB/s.
    uint32_t readSpeed;

    /// For unit testing.
    uint64_t bytesWritten;

    /// Used to ensure that init() is invoked before the dispatcher runs.
    bool initCalled;

    /// Used to determine server status of masters for garbage collection.
    ServerTracker<void> gcTracker;

    /// Runs garbage collection tasks.
    Tub<std::thread> gcThread;

    /// For testing; don't start gcThread when tracker changes are enqueued.
    bool testingDoNotStartGcThread;

    /// Set during unit tests to skip the check that ensures the caller is
    /// actually in the cluster.
    bool testingSkipCallerIdCheck;

    /**
     * Executes enqueued tasks for replica garbage collection and master
     * recovery.
     */
    TaskQueue taskQueue;

    /**
     * Counts old replicas (those that existed at startup) that have
     * not yet been freed by the garbage collector. This value should
     * become zero pretty soon after startup.
     */
    int oldReplicas;

    /**
     * Try to garbage collect replicas from a particular master found on disk
     * until it is finally removed. Usually replicas are freed explicitly by
     * masters, but this doesn't work for cases where the replica was found on
     * disk as part of an old master.
     *
     * This task may generate RPCs to the master to determine the status of the
     * replica which survived on-storage across backup failures.
     */
    class GarbageCollectReplicasFoundOnStorageTask : public Task {
      PUBLIC:
        GarbageCollectReplicasFoundOnStorageTask(BackupService& service,
                                                 ServerId masterId);
        void addSegmentId(uint64_t segmentId);
        void performTask();

      PRIVATE:
        bool tryToFreeReplica(uint64_t segmentId);
        void deleteReplica(uint64_t segmentId);

        /// Backup which is trying to garbage collect the replica.
        BackupService& service;

        /// Id of the master which originally created the replica.
        ServerId masterId;

        /// Segment ids of the replicas which are candidates for removal.
        std::deque<uint64_t> segmentIds;

        /**
         * Space for a rpc to the master to ask it explicitly if it would
         * like this replica to be retain as it makes more replica elsewhere.
         */
        Tub<IsReplicaNeededRpc> rpc;

        DISALLOW_COPY_AND_ASSIGN(GarbageCollectReplicasFoundOnStorageTask);
    };
    friend class GarbageCollectReplicaFoundOnStorageTask;

    /**
     * Garbage collect all state for a now down master. This includes any
     * replicas created by it as well as any outstanding recovery state for it.
     * Downed servers are known to be fully recovered and out of the cluster, so
     * this is safe. Usually replicas are freed explicitly by masters, but this
     * doesn't work for cases where the replica was created by a master which
     * has crashed.
     */
    class GarbageCollectDownServerTask : public Task {
      PUBLIC:
        GarbageCollectDownServerTask(BackupService& service, ServerId masterId);
        void performTask();

      PRIVATE:
        /// Backup trying to garbage collect replicas from some removed master.
        BackupService& service;

        /// Id of the master now known to have been removed from the cluster.
        ServerId masterId;

        DISALLOW_COPY_AND_ASSIGN(GarbageCollectDownServerTask);
    };
    friend class GarbageCollectDownServerTask;

    DISALLOW_COPY_AND_ASSIGN(BackupService);
};

} // namespace RAMCloud

#endif
