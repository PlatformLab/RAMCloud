/* Copyright (c) 2009-2011 Stanford University
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
#include "BackupManager.h"
#include "CycleCounter.h"
#include "ShortMacros.h"
#include "RawMetrics.h"

namespace RAMCloud {

/**
 * Create a BackupManager.
 * \param coordinator
 *      Cluster coordinator; used to get a list of backup servers.
 *      May be NULL for testing purposes.
 * \param masterId
 *      Server id of master that this will be managing replicas for.
 * \param numReplicas
 *      Number replicas to keep of each segment.
 */
BackupManager::BackupManager(CoordinatorClient* coordinator,
                             const Tub<uint64_t>& masterId,
                             uint32_t numReplicas)
    : numReplicas(numReplicas)
    , backupSelector(coordinator)
    , coordinator(coordinator)
    , masterId(masterId)
    , replicatedSegmentPool(ReplicatedSegment::sizeOf(numReplicas))
    , replicatedSegmentList()
    , taskManager()
    , writeRpcsInFlight(0)
{
}

/**
 * Create a BackupManager; extremely broken, do not use this.
 * This manager is constructed the same way as a previous manager.
 * This is used, for instance, by the LogCleaner to obtain a private
 * BackupManager that is configured equivalently to the Log's own
 * manager (without having to share the two).
 *
 * TODO: This is completely broken and needs to be done away with.
 * TODO: Eliminate #coordinator when this is fixed.
 * 
 * \param prototype
 *      The BackupManager that serves as a prototype for this newly
 *      created one. The same masterId, number of replicas, and
 *      coordinator are used.
 */
BackupManager::BackupManager(BackupManager* prototype)
    : numReplicas(prototype->numReplicas)
    , backupSelector(prototype->coordinator)
    , coordinator(prototype->coordinator)
    , masterId(prototype->masterId)
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
BackupManager::~BackupManager()
{
    sync();
    // sync() is insufficient, may have outstanding frees, etc. Done below.
    while (!taskManager.isIdle())
        proceed();
    while (!replicatedSegmentList.empty())
        destroyAndFreeReplicatedSegment(&replicatedSegmentList.front());
}

/**
 * Queue a segment for replication on backups.  Allocates and returns a
 * ReplicatedSegment which acts as a handle for the log module to perform
 * future operations related to this segment (like queueing more data for
 * replication, waiting for data to be replicated, or freeing replicas).
 * Note, the segment isn't guaranteed to be durably open on backups until
 * #sync() is called.
 *
 * \param segmentId
 *      A unique identifier for this segment. The caller must ensure a
 *      segment with this segmentId is not already open.
 * \param data
 *      Starting location of the raw segment data to be replicated.
 * \param len
 *      Number of bytes to send atomically to backups with open segment RPC;
 *      used to send the segment header and log digest (when applicable) along
 *      with the open RPC to a backup.
 * \return
 *      Pointer to a ReplicatedSegment that is valid until
 *      ReplicatedSegment::free() is called on it.
 */
ReplicatedSegment*
BackupManager::openSegment(uint64_t segmentId, const void* data, uint32_t len)
{
    CycleCounter<RawMetric> _(&metrics->master.backupManagerTicks);
    LOG(DEBUG, "openSegment %lu, %lu, ..., %u", *masterId, segmentId, len);
    auto* p = replicatedSegmentPool.malloc();
    if (p == NULL)
        DIE("Out of memory");
    auto* replicatedSegment =
        new(p) ReplicatedSegment(taskManager, backupSelector, *this,
                                 writeRpcsInFlight, *masterId, segmentId,
                                 data, len, numReplicas);
    replicatedSegmentList.push_back(*replicatedSegment);
    replicatedSegment->schedule();
    return replicatedSegment;
}

/**
 * Make progress on replicating the log to backups and freeing unneeded
 * replicas, but don't block.  This method checks for completion of outstanding
 * backup operations and starts new ones when possible.
 */
void
BackupManager::proceed()
{
    CycleCounter<RawMetric> _(&metrics->master.backupManagerTicks);
    taskManager.proceed();
}

/**
 * Wait until all data enqueued for replication has been acknowledged by the
 * backups for all segments (where acknowledged means the data is durably
 * buffered by each backup).  This must be called after any openSegment()
 * or ReplicatedSegment::write() calls where the operation must be durable.
 */
void
BackupManager::sync()
{
    {
        CycleCounter<RawMetric> _(&metrics->master.backupManagerTicks);
        while (!isSynced()) {
            taskManager.proceed();
        }
    } // block ensures that _ is destroyed and counter stops
}

// - private -

/**
 * Respond to a change in cluster configuration by scheduling any work that
 * is needed to restore durablity guarantees.  The work is queued into
 * #taskManager which is then executed during calls to #proceed().  One call
 * is sufficient since tasks reschedule themselves until all guarantees are
 * restored.
 */
void
BackupManager::clusterConfigurationChanged()
{
    foreach (auto& segment, replicatedSegmentList)
        segment.schedule();
}

/**
 * Internal helper for #sync().
 * Returns true when all data queued for replication by the log module is
 * durably replicated.
 */
bool
BackupManager::isSynced()
{
    foreach (auto& segment, replicatedSegmentList) {
        if (!segment.isSynced())
            return false;
    }
    return true;
}

/**
 * Invoked by ReplicatedSegment to indicate that the BackupManager no longer
 * needs to keep an information about this segment (for example, when all
 * replicas are freed on backups or during shutdown).
 * Only used by ReplicatedSegment and ~BackupManager.
 */
void
BackupManager::destroyAndFreeReplicatedSegment(ReplicatedSegment*
                                                    replicatedSegment)
{
    assert(!replicatedSegment->isScheduled());
    erase(replicatedSegmentList, *replicatedSegment);
    replicatedSegment->~ReplicatedSegment();
    replicatedSegmentPool.free(replicatedSegment);
}

} // namespace RAMCloud
