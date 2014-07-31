/* Copyright (c) 2009-2014 Stanford University
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

#ifndef RAMCLOUD_REPLICAMANAGER_H
#define RAMCLOUD_REPLICAMANAGER_H

#include <unordered_map>
#include <boost/pool/pool.hpp>

#include "Common.h"
#include "BackupFailureMonitor.h"
#include "BoostIntrusive.h"
#include "BackupSelector.h"
#include "CoordinatorClient.h"
#include "UpdateReplicationEpochTask.h"
#include "ReplicatedSegment.h"
#include "ServerTracker.h"
#include "TaskQueue.h"
#include "Tub.h"

namespace RAMCloud {

class Segment;

/**
 * Creates and tracks replicas of local in-memory segments on remote backups.
 *
 * A master's log module issues requests to a ReplicaManager which replicates
 * log data and can be used to free replicas of segments from backups in the
 * cluster.  ReplicaManager also responds to changes in cluster configuration;
 * it restores durability of segments and transparently masks backup failures
 * (both for closed segments and for segments which are actively being written
 * when a backup fails). ReplicaManager tries to mask all failures that can
 * occur in replication (for example, naming, network, or host failures).
 *
 * All operations issued to the ReplicaManager are only queued.  To force all
 * queued operations to complete sync() must be called, otherwise the
 * ReplicaManager casually tries to perform some replication whenever proceed()
 * is called.
 *
 * The log module uses openSegment() to make the ReplicaManager aware of
 * an in-memory segment which must be replicated.  See ReplicatedSegment for
 * details on how the log module informs the ReplicaManager of changes to the
 * in-memory segment image and what guarantees the ReplicaManager provides.
 *
 * There must be exactly one ReplicaManager per log otherwise behavior is
 * undefined.
 */
class ReplicaManager
    : public ReplicatedSegment::Deleter
{
  PUBLIC:
    typedef std::lock_guard<std::mutex> Lock;

    ReplicaManager(Context* context,
                   const ServerId* masterId,
                   uint32_t numReplicas,
                   bool useMinCopysets,
                   bool allowLocalBackup);
    ~ReplicaManager();

    bool isIdle();
    bool isReplicaNeeded(ServerId backupServerId, uint64_t segmentId);
    ReplicatedSegment* allocateHead(uint64_t segmentId, const Segment* segment,
                                    ReplicatedSegment* precedingSegment);
        __attribute__((warn_unused_result));
    ReplicatedSegment* allocateNonHead(uint64_t segmentId,
                                       const Segment* segment);
        __attribute__((warn_unused_result));
    void startFailureMonitor();
    void haltFailureMonitor();
    void proceed();

  PRIVATE:
    ReplicatedSegment* allocateSegment(const Lock& lock, uint64_t segmentId,
                                       const Segment* segment, bool isLogHead);
        __attribute__((warn_unused_result));

    /// Shared RAMCloud information.
    Context* context;

  PUBLIC:
    /// Number replicas to keep of each segment.
    const uint32_t numReplicas;

  PRIVATE:
    /// Selects backups to store replicas while obeying placement constraints.
    std::unique_ptr<BackupSelector> backupSelector;

    /**
     * Protects all internal data structures during concurrent calls to the
     * ReplicaManager and any of its ReplicatedSegments.
     * This includes all data being tracked for each individual segment and
     * its replicas as well as helper structures like the #taskQueue and
     * #replicatedSegmentList.  A lock for this mutex must be held to read
     * or modify any state in the ReplicaManager.
     *
     * Locking in ReplicaManager and ReplicatedSegment maintains several
     * important properties:
     * 1) Only one thread is in proceed() at a time. When any thread is
     *    "in" the ReplicaManager performing replication all other threads
     *    are locked out.
     * 2) When multiple threads call sync() simultaneously only one is
     *    admitted at a time and that thread doesn't release locks until
     *    its objects are committed on backups, but sync() still coalesces
     *    operations so that when threads acquire the appropriate locks in
     *    sync they may find their work has already been done by another thread.
     *    There is a subtle starvation problem that can occur which
     *    ReplicatedSegment::syncMutex prevents. See
     *    ReplicatedSegment::sync() for the details.
     * 3) Calls to sync() cannot lock out calls to handleBackupFailure()
     *    indefinitely. Calls to sync() may block forever if they are trying to
     *    communicate with failed servers. Threads sync()ing objects must
     *    release #dataMutex periodically to ensure the BackupFailureMonitor can
     *    modify ReplicatedSegments which have inoperable backups.
     * 4) Part of a handling backup failure may include allocating a new log
     *    head. #dataMutex MUST NOT be held when allocating a head since
     *    log code calls into the ReplicaManager and ReplicatedSegments which
     *    would cause deadlock. Because of this allocating a new log head
     *    in response to backup failures only happens in the
     *    BackupFailureMonitor where it is safe to do so.
     */

    std::mutex dataMutex;

    /// Id of master that this will be managing replicas for.
    const ServerId* masterId;

    /// Allows fast reuse of ReplicatedSegment allocations.
    boost::pool<> replicatedSegmentPool;

    INTRUSIVE_LIST_TYPEDEF(ReplicatedSegment, listEntries)
        ReplicatedSegmentList;

    /**
     * A list all ReplicatedSegments (one for each segment in the log
     * which hasn't been freed). Newly opened segments are pushed to the back.
     */
    ReplicatedSegmentList replicatedSegmentList;

    /**
     * Enqueues segments that need replication/freeing and makes progress
     * on enqueued operations whenever taskQueue.performTask() is called.
     */
    TaskQueue taskQueue;

    /**
     * Number of collective outstanding write rpcs to all backups.
     * Used by ReplicatedSegment to throttle rpc creation.
     */
    uint32_t writeRpcsInFlight;

    /**
     * Number of collective outstanding free rpcs to all backups.
     * Used by ReplicatedSegment to throttle rpc creation.
     */
    uint32_t freeRpcsInFlight;

    /**
     * Provides access to the latest replicationEpoch acknowledged by the
     * coordinator for this server and allows easy, asynchronous updates
     * to the value stored on the coordinator.
     */
    Tub<UpdateReplicationEpochTask> replicationEpoch;

    /**
     * Waits for backup failure notifications from the Server's main ServerList
     * and informs the ReplicaManager which takes corrective actions.  Runs in
     * a separate thread in order to provide immediate response to failures and
     * to provide a context for potentially long-running corrective actions even
     * while the master is otherwise idle.
     */
    BackupFailureMonitor failureMonitor;

    /**
     * Used to measure time when backup write rpcs are active. Passed in to and
     * shared among ReplicatedSegments.
     */
    Tub<CycleCounter<RawMetric>> replicationCounter;

    /**
     * Specifies whether to use the MinCopysets replication scheme.
     */
    bool useMinCopysets;

    /**
     * Specifies whether to allow replication to local backup.
     */
    bool allowLocalBackup;

  PUBLIC:
    // Only used by BackupFailureMonitor.
    void handleBackupFailure(ServerId failedId);

    // Only used by ReplicatedSegment.
    void destroyAndFreeReplicatedSegment(ReplicatedSegment* replicatedSegment);

    DISALLOW_COPY_AND_ASSIGN(ReplicaManager);
};

} // namespace RAMCloud

#endif
