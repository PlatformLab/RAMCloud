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
#include "Segment.h"

namespace RAMCloud {

/**
 * Create a BackupManager, initially with no backup hosts to communicate
 * with.
 * \param coordinator
 *      \copydoc coordinator
 * \param masterId
 *      \copydoc masterId
 * \param numReplicas
 *      \copydoc numReplicas
 */
BackupManager::BackupManager(CoordinatorClient* coordinator,
                             const Tub<uint64_t>& masterId,
                             uint32_t numReplicas)
    : numReplicas(numReplicas)
    , masterId(masterId)
    , backupSelector(coordinator)
    , coordinator(coordinator)
    , replicatedSegmentPool(ReplicatedSegment::sizeOf(numReplicas))
    , replicatedSegmentList()
    , taskManager()
    , outstandingRpcs(0)
    , activeTime()
{
}

/**
 * Create a BackupManager, initially with no backup hosts to communicate
 * with. This manager is constructed the same way as a previous manager.
 * This is used, for instance, by the LogCleaner to obtain a private
 * BackupManager that is configured equivalently to the Log's own
 * manager (without having to share the two).
 * 
 * \param prototype
 *      The BackupManager that serves as a prototype for this newly
 *      created one. The same masterId, number of replicas, and
 *      coordinator are used.
 */
BackupManager::BackupManager(BackupManager* prototype)
    : numReplicas(prototype->numReplicas)
    , masterId(prototype->masterId)
    , backupSelector(prototype->coordinator)
    , coordinator(prototype->coordinator)
    , replicatedSegmentPool(ReplicatedSegment::sizeOf(numReplicas))
    , replicatedSegmentList()
    , taskManager()
    , outstandingRpcs(0)
    , activeTime()
{
}

BackupManager::~BackupManager()
{
    sync();
    // Waiting for sync() is insufficient, may have outstanding frees, etc.
    while (!taskManager.isIdle())
        proceed();
    while (!replicatedSegmentList.empty())
        forgetReplicatedSegment(&replicatedSegmentList.front());
}

/**
 * Ask backups to discard a segment.
 */
void
BackupManager::freeSegment(uint64_t segmentId)
{
    CycleCounter<RawMetric> _(&metrics->master.backupManagerTicks);

    // TODO: Don't allow free on an open segment.

    // Note: cannot use foreach since forgetReplicatedSegment
    // modifies the replicatedSegmentList.
    auto it = replicatedSegmentList.begin();
    while (it != replicatedSegmentList.end()) {
        it->free();
        ++it;
        while (!taskManager.isIdle())
            proceed();
    }
}

/**
 * Eventually begin replicating a segment on backups.
 *
 * \param segmentId
 *      A unique identifier for this segment. The caller must ensure this
 *      segment is not already open.
 * \param data
 *      Location at which data to be replicated for this segment begins.
 * \param len
 *      The number of bytes to send atomically to backups with the open segment
 *      RPC.
 * \return
 *      A pointer to an OpenSegment object that is valid only until that
 *      segment is closed.
 */
OpenSegment*
BackupManager::openSegment(uint64_t segmentId, const void* data, uint32_t len)
{
    CycleCounter<RawMetric> _(&metrics->master.backupManagerTicks);
    LOG(DEBUG, "openSegment %lu, %lu, ..., %u", *masterId, segmentId, len);
    auto* p = replicatedSegmentPool.malloc();
    if (p == NULL)
        DIE("Out of memory");
    auto* replicatedSegment = new(p) ReplicatedSegment(*this, taskManager,
                                                       segmentId, data, len,
                                                       numReplicas);
    replicatedSegmentList.push_back(*replicatedSegment);
    replicatedSegment->schedule();
    return replicatedSegment;
}

/**
 * Make progress on replicating the log to backups, but don't block.
 * This method checks for completion of outstanding backup operations and
 * starts new ones when possible.
 */
void
BackupManager::proceed()
{
    CycleCounter<RawMetric> _(&metrics->master.backupManagerTicks);
    proceedNoMetrics();
}

/**
 * Wait until all written data has been acknowledged by the backups for all
 * segments.
 */
void
BackupManager::sync()
{
    {
        CycleCounter<RawMetric> _(&metrics->master.backupManagerTicks);
        while (!isSynced()) {
            proceedNoMetrics();
        }
    } // block ensures that _ is destroyed and counter stops
    // TODO: may need to rename this (outstandingWriteRpcs?)
    assert(outstandingRpcs == 0);
}

// - private -

/**
 * TODO: this is a lot of how
 * Walk through all segments the BackupManager is responsible for and make
 * sure that all their durability invariants hold.
 * If some invariant is not met then this method schedules the appropriate
 * tasks so that #proceed() will restore the invariant.
 */
void
BackupManager::scheduleWorkIfNeeded()
{
    foreach (auto& segment, replicatedSegmentList)
        segment.performTask();
}

/// Internal helper for #sync().
bool
BackupManager::isSynced()
{
    foreach (auto& segment, replicatedSegmentList) {
        if (!segment.isSynced())
            return false;
    }
    return true;
}

/// \copydoc proceed()
void
BackupManager::proceedNoMetrics()
{
    taskManager.proceed();
}

/**
 * Invoked by ReplicatedSegment to indicate that the BackupManager no longer
 * needs to keep an information about this segment (e.g. when all
 * replicas are freed on backups or during shutdown).
 */
void
BackupManager::forgetReplicatedSegment(ReplicatedSegment* replicatedSegment)
{
    erase(replicatedSegmentList, *replicatedSegment);
    replicatedSegment->~ReplicatedSegment();
    replicatedSegmentPool.free(replicatedSegment);
}

} // namespace RAMCloud
