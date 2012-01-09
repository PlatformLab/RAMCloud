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

#include "BackupClient.h"
#include "CycleCounter.h"
#include "Logger.h"
#include "ShortMacros.h"
#include "RawMetrics.h"
#include "ReplicaManager.h"

namespace RAMCloud {

// --- ReplicaManager ---

/**
 * Create a ReplicaManager.  Creating more than one ReplicaManager for a
 * single log results in undefined behavior.
 * \param serverList
 *      Used to construct a tracker to find backups and track replica
 *      distribution stats.
 * \param masterId
 *      Server id of master that this will be managing replicas for (also
 *      serves as the log id).
 * \param numReplicas
 *      Number replicas to keep of each segment.
 */
ReplicaManager::ReplicaManager(ServerList& serverList,
                               const ServerId& masterId,
                               uint32_t numReplicas)
    : numReplicas(numReplicas)
    , tracker(serverList)
    , backupSelector(tracker)
    , dataMutex()
    , masterId(masterId)
    , replicatedSegmentPool(ReplicatedSegment::sizeOf(numReplicas))
    , replicatedSegmentList()
    , taskManager()
    , writeRpcsInFlight(0)
{
}

/**
 * Sync replicas with all queued operations, wait for any outstanding frees
 * to complete, then cleanup and release an local resources (all durably
 * stored but unfreed replicas will remain on backups).
 */
ReplicaManager::~ReplicaManager()
{
    Lock lock(dataMutex);

    foreach (auto& segment, replicatedSegmentList)
        while (!segment.isSynced())
            taskManager.proceed();
    // sync() is insufficient, may have outstanding frees, etc. Done below.
    while (!taskManager.isIdle())
        taskManager.proceed();
    while (!replicatedSegmentList.empty())
        destroyAndFreeReplicatedSegment(&replicatedSegmentList.front());
}

/**
 * Enqueue a segment for replication on backups, return a handle to schedule
 * future operations on the segment.  Selection of backup locations and
 * replication are performed at a later time.  The segment data isn't
 * guaranteed to be durably open on backups until sync() is called.  The
 * returned handle allows future operations like enqueueing more data for
 * replication, waiting for data to be replicated, or freeing replicas.  Read
 * the documentation for ReplicatedSegment::write, ReplicatedSegment::close,
 * ReplicatedSegment::free carefully; some of the requirements and guarantees
 * in order to ensure data is recovered correctly after a crash are subtle.
 *
 * The caller must not enqueue writes before ReplicatedSegment::close is
 * called on the ReplicatedSegment that logically precedes this one in the
 * log; see ReplicatedSegment::close for details on how this works.
 *
 * The caller must not reuse the memory starting at #data up through the bytes
 * enqueued via ReplicatedSegment::write until after ReplicatedSegment::free
 * is called and returns (until that time outstanding backup write rpcs may
 * still refer to the segment data).
 *
 * \param segmentId
 *      The unique identifier for this segment given to it by the log module.
 *      The caller must ensure a segment with this segmentId has never been
 *      opened before as part of the log this ReplicaManager is managing.
 * \param data
 *      Starting location of the raw segment data to be replicated.
 * \param openLen
 *      Number of bytes to send atomically to backups with open segment rpc;
 *      used to send the segment header and log digest (when applicable) along
 *      with the open rpc to a backup.
 * \return
 *      Pointer to a ReplicatedSegment that is valid until
 *      ReplicatedSegment::free() is called on it or until the ReplicaManager
 *      is destroyed.
 */

ReplicatedSegment*
ReplicaManager::openSegment(uint64_t segmentId, const void* data,
                            uint32_t openLen)
{
    CycleCounter<RawMetric> _(&metrics->master.replicaManagerTicks);
    Lock __(dataMutex);

    LOG(DEBUG, "openSegment %lu, %lu, ..., %u",
        masterId.getId(), segmentId, openLen);
    auto* p = replicatedSegmentPool.malloc();
    if (p == NULL)
        DIE("Out of memory");
    auto* replicatedSegment =
        new(p) ReplicatedSegment(taskManager, tracker, backupSelector, *this,
                                 writeRpcsInFlight,
                                 dataMutex,
                                 masterId, segmentId,
                                 data, openLen, numReplicas);
    replicatedSegmentList.push_back(*replicatedSegment);
    replicatedSegment->schedule();
    return replicatedSegment;
}

/**
 * Make progress on replicating the log to backups and freeing unneeded
 * replicas, but don't block.  This method checks for completion of outstanding
 * replication or replica freeing operations and starts new ones when possible.
 */
void
ReplicaManager::proceed()
{
    CycleCounter<RawMetric> _(&metrics->master.replicaManagerTicks);
    Lock __(dataMutex);
    taskManager.proceed();
}

// - private -

/**
 * Respond to a change in cluster configuration by scheduling any work that is
 * needed to restore durablity guarantees.  One call is sufficient since tasks
 * reschedule themselves until all guarantees are restored.  This method will
 * be superceded by its pending integration with the ServerTracker.
 */
void
ReplicaManager::clusterConfigurationChanged()
{
    Lock _(dataMutex);
    foreach (auto& segment, replicatedSegmentList)
        segment.schedule();
}

/**
 * Only used by ReplicatedSegment and ~ReplicaManager.
 * Invoked by ReplicatedSegment to indicate that the ReplicaManager no longer
 * needs to keep an information about this segment (for example, when all
 * replicas are freed on backups or during shutdown).
 */
void
ReplicaManager::destroyAndFreeReplicatedSegment(ReplicatedSegment*
                                                    replicatedSegment)
{
    // Only called from destructor and ReplicatedSegment::performTask
    // so lock on dataMutex should always be held.
    assert(!replicatedSegment->isScheduled());
    erase(replicatedSegmentList, *replicatedSegment);
    replicatedSegment->~ReplicatedSegment();
    replicatedSegmentPool.free(replicatedSegment);
}

} // namespace RAMCloud
