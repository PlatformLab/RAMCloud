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

/**
 * \file
 * Declarations for the backup service, currently all backup RPC
 * requests are handled by this module including all the heavy lifting
 * to complete the work requested by the RPCs.
 */

#ifndef RAMCLOUD_BACKUPSERVICE_H
#define RAMCLOUD_BACKUPSERVICE_H

#if __GNUC__ >= 4 && __GNUC_MINOR__ >= 5
#include <atomic>
#else
#include <cstdatomic>
#endif
#include <thread>
#include <memory>
#include <map>
#include <queue>

#include "Common.h"
#include "Atomic.h"
#include "BackupClient.h"
#include "BackupStorage.h"
#include "CoordinatorClient.h"
#include "CycleCounter.h"
#include "Fence.h"
#include "LogEntryTypes.h"
#include "MasterClient.h"
#include "RawMetrics.h"
#include "Service.h"
#include "ServerConfig.h"
#include "SegmentIterator.h"
#include "TaskQueue.h"

namespace RAMCloud {

class BackupReplica;

/**
 * Handles Rpc requests from Masters and the Coordinator to persistently store
 * Segments and to facilitate the recovery of object data when Masters crash.
 */
class BackupService : public Service
                    , ServerTracker<void>::Callback {
    /**
     * Decrement the value referred to on the constructor on
     * destruction.
     *
     * \tparam T
     *      Type of the value that will be decremented when an
     *      instance of a class that is a an instance of this
     *      template gets destructed.
     */
    template <typename T>
    class ReferenceDecrementer {
      public:
        /**
         * \param value
         *      Reference to a value that should be decremented when
         *      #this is destroyed.
         */
        explicit ReferenceDecrementer(T& value)
            : value(value)
        {
        }

        /// Decrement the value referred to by #value.
        ~ReferenceDecrementer()
        {
            // The following statement is really only needed when value
            // is an Atomic.
            Fence::leave();
            --value;
        }

      private:
        /// Reference to the value to be decremented on destruction of this.
        T& value;
    };

  public:
    /**
     * Asynchronously loads and splits stored segments into recovery segments,
     * trying to acheive efficiency by overlapping work where possible using
     * threading.  See #replicas for important details on sharing constraints
     * for BackupReplica structures while these threads are processing.
     */
    class RecoverySegmentBuilder
    {
      public:
        RecoverySegmentBuilder(Context* context,
                               const vector<BackupReplica*>& replicas,
                               const ProtoBuf::Tablets& partitions,
                               Atomic<int>& recoveryThreadCount,
                               uint32_t segmentSize);
        RecoverySegmentBuilder(const RecoverySegmentBuilder& src)
            : context(src.context)
            , replicas(src.replicas)
            , partitions(src.partitions)
            , recoveryThreadCount(src.recoveryThreadCount)
            , segmentSize(src.segmentSize)
        {}
        RecoverySegmentBuilder& operator=(const RecoverySegmentBuilder& src)
        {
            if (this != &src) {
                context = src.context;
                replicas = src.replicas;
                partitions = src.partitions;
                recoveryThreadCount = src.recoveryThreadCount;
                segmentSize = src.segmentSize;
            }
            return *this;
        }
        void operator()();

      private:
        /// The context in which the thread will execute.
        Context* context;

        /**
         * The BackupReplicas that will be loaded from disk (asynchronously)
         * and (asynchronously) split into recovery segments.  These
         * BackupReplicas are owned by this RecoverySegmentBuilder instance
         * which will run in a separate thread by locking #mutex.
         * All public methods of BackupReplica obey this mutex ensuring
         * thread safety.
         */
        vector<BackupReplica*> replicas;

        /// Copy of the partitions used to split out the recovery segments.
        ProtoBuf::Tablets partitions;

        Atomic<int>& recoveryThreadCount;

        /// The uniform size of each segment this backup deals with.
        uint32_t segmentSize;
    };

    BackupService(Context* context, const ServerConfig& config);
    virtual ~BackupService();
    void benchmark();
    void dispatch(WireFormat::Opcode opcode, Rpc& rpc);
    ServerId getFormerServerId() const;
    ServerId getServerId() const;
    void init(ServerId id);
    uint32_t getReadSpeed() { return readSpeed; }

  PRIVATE:
    void assignGroup(const WireFormat::BackupAssignGroup::Request& reqHdr,
                     WireFormat::BackupAssignGroup::Response& respHdr,
                     Rpc& rpc);
    void freeSegment(const WireFormat::BackupFree::Request& reqHdr,
                     WireFormat::BackupFree::Response& respHdr,
                     Rpc& rpc);
    BackupReplica* findBackupReplica(ServerId masterId, uint64_t segmentId);
    void getRecoveryData(
        const WireFormat::BackupGetRecoveryData::Request& reqHdr,
        WireFormat::BackupGetRecoveryData::Response& respHdr,
        Rpc& rpc);
    void killAllStorage();
    void quiesce(const WireFormat::BackupQuiesce::Request& reqHdr,
                 WireFormat::BackupQuiesce::Response& respHdr,
                 Rpc& rpc);
    void recoveryComplete(
        const WireFormat::BackupRecoveryComplete::Request& reqHdr,
        WireFormat::BackupRecoveryComplete::Response& respHdr,
        Rpc& rpc);
    void restartFromStorage();
    void startReadingData(
        const WireFormat::BackupStartReadingData::Request& reqHdr,
        WireFormat::BackupStartReadingData::Response& respHdr,
        Rpc& rpc);
    void writeSegment(const WireFormat::BackupWrite::Request& req,
                      WireFormat::BackupWrite::Response& resp,
                      Rpc& rpc);
    void gcMain();
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
    const ServerConfig& config;

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

    /// Coordinator-assigned ID for this backup service
    ServerId serverId;

    /**
     * Times each recovery. This is an Tub that is reset on every
     * recovery, since CycleCounters can't presently be restarted.
     */
    Tub<CycleCounter<RawMetric>> recoveryTicks;

    /// Count of threads performing recoveries.
    Atomic<int> recoveryThreadCount;

    /// Type of the key for the segments map.
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
    /// Type of the segments map.
    typedef std::map<MasterSegmentIdPair, BackupReplica*> SegmentsMap;
    /**
     * Mapping from (MasterId, SegmentId) to a BackupReplica for segments
     * that are currently open or in storage.
     */
    SegmentsMap segments;

    /// The uniform size of each segment this backup deals with.
    const uint32_t segmentSize;

    /// The storage backend where closed segments are to be placed.
    std::unique_ptr<BackupStorage> storage;

    /// The results of storage.benchmark() in MB/s.
    uint32_t readSpeed;

    /// For unit testing.
    uint64_t bytesWritten;

    /// Used to ensure that init() is invoked before the dispatcher runs.
    bool initCalled;

    /// Used to identify the replication group that the backup belongs to. Each
    /// segment is replicated to a specific replication group. The default
    /// replicationId is 0, which means that the backup has not been assigned
    /// a replication group.
    uint64_t replicationId;

    /// The ServerId's all the members of the replication group. The backup
    /// needs to notify the masters who the other members in its group are.
    vector<ServerId> replicationGroup;

    /// Used to determine server status of masters for garbage collection.
    ServerTracker<void> gcTracker;

    /// Runs garbage collection tasks.
    Tub<std::thread> gcThread;

    /// For testing; don't start gcThread when tracker changes are enqueued.
    bool testingDoNotStartGcThread;

    /**
     * Enqueues requests to garbage collect replicas and tries to make
     * progress on each of them.
     */
    TaskQueue gcTaskQueue;

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
     * Try to garbage collect all replicas stored by a master which is now
     * known to have been completely recovered and removed from the cluster.
     * Usually replicas are freed explicitly by masters, but this
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
