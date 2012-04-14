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
 * \param coordinatorLocator
 *      Locator of the coordinator where the minOpenSegmentId will be
 *      updated for this server in the case of some failures.  May be
 *      NULL for testing in which case updates will not be sent to the
 *      coordinator.
 */
ReplicaManager::ReplicaManager(ServerList& serverList,
                               const ServerId& masterId,
                               uint32_t numReplicas,
                               const string* coordinatorLocator)
    : numReplicas(numReplicas)
    , tracker(serverList)
    , backupSelector(tracker)
    , coordinator()
    , dataMutex()
    , masterId(masterId)
    , replicatedSegmentPool(ReplicatedSegment::sizeOf(numReplicas))
    , replicatedSegmentList()
    , taskManager()
    , writeRpcsInFlight(0)
    , minOpenSegmentId()
{
    if (coordinatorLocator)
        coordinator.construct(coordinatorLocator->c_str());
    minOpenSegmentId.construct(&taskManager,
                               coordinator ? coordinator.get() : NULL,
                               &masterId);
}

/**
 * Sync replicas with all queued operations, wait for any outstanding frees
 * to complete, then cleanup and release an local resources (all durably
 * stored but unfreed replicas will remain on backups).
 */
ReplicaManager::~ReplicaManager()
{
    Lock lock(dataMutex);

    if (!taskManager.isIdle())
        LOG(WARNING, "Master exiting while outstanding replication "
            "operations were still pending");
    while (!replicatedSegmentList.empty())
        destroyAndFreeReplicatedSegment(&replicatedSegmentList.front());
}

/**
 * Returns true when all data that has been queued (via write()) is properly
 * replicated for all segments in the log. Can occasionally return false
 * even when the above property is true (e.g. while pending free requests are
 * being performed).
 */
bool
ReplicaManager::isIdle()
{
    Lock lock(dataMutex);
    return taskManager.isIdle();
}

/**
 * Returns the durability status of a particular segment.
 *
 * \param segmentId
 *      The id of the segment whose durability is being queried.
 * \return
 *      Returns empty when no segment with \a segmentId is known about (i.e.
 *      the segment was never created or it was freed).  Otherwise, returns
 *      true when all data that has been queued (via write()) is properly
 *      replicated for the specified segment.  Returns false, if not.
 */
Tub<bool>
ReplicaManager::isSegmentSynced(uint64_t segmentId)
{
    Lock lock(dataMutex);
    // TODO(stutsman): Potentially slow, probably want to add an incrementally
    // maintained index to ReplicaManager.
    foreach (auto& segment, replicatedSegmentList) {
        if (segmentId == segment.segmentId)
            return {segment.isSynced()};
    }

    return {};
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
 * \param isLogHead
 *      Whether this segment is part of the normal log and will be considered
 *      the log head when opened.  This is typically true, but should be false
 *      for segments generated by the LogCleaner.  This affects how
 *      ReplicaManager recovers from replica loss of this segment.
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
ReplicaManager::openSegment(bool isLogHead, uint64_t segmentId,
                            const void* data, uint32_t openLen)
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
                                 writeRpcsInFlight, *minOpenSegmentId,
                                 dataMutex,
                                 isLogHead, masterId, segmentId,
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
    metrics->master.replicationTasks = taskManager.outstandingTasks();
}

// - private -

/**
 * Respond to a change in cluster configuration by scheduling any work that is
 * needed to restore durability guarantees; please read the documentation
 * for the return value carefully as it requires action on the part of the
 * caller to ensure correctness of the log.
 *
 * \param failedId
 *      ServerId of the backup which has failed.
 * \return
 *      Empty if the failure didn't affect any replicas, otherwise contains
 *      the id of the highest numbered non-cleaner-generated segment that
 *      was affected by the failure.
 *      True indicates that one or more segments lost replicas and will take
 *      corrective actions on future iterations of the ReplicaManager loop.
 *      These corrective actions rely on closing the segment if it was
 *      still open.  Therefore, the caller must take appropriate action
 *      to ensure that if the only current and recoverable copy of the digest
 *      was stored in the segment whose id is returned that it creates a new,
 *      current, and recoverable digest.  Checking to see if this segment id
 *      is the current head from the Log's perspective, and if so, rolling
 *      over to a new log head solves both constraints; it guarantees the
 *      segment with the reported id (and all segments with lower ids
 *      that were not generated by the cleaner) are closed from the log's
 *      perspective and ensures a current log digest exists in another
 *      segment.  Internally, the rereplicating segments take care not
 *      to close themselves until their successor is at least durably
 *      open as usual.
 */
Tub<uint64_t>
ReplicaManager::handleBackupFailure(ServerId failedId)
{
    Lock _(dataMutex);
    LOG(NOTICE, "Handling backup failure of serverId %lu", failedId.getId());

    Tub<uint64_t> failedOpenSegmentId;
    foreach (auto& segment, replicatedSegmentList) {
        if (segment.handleBackupFailure(failedId)) {
            // Find the highest open segmentId that was affected by failure.
            // Use this to see if the head segment must be rolled over to
            // ensure that there will still be a valid log digest even
            // after the all the affected segments are closed (either the
            // usual way or through the brute force setMinOpenSegmentId
            // that any non-(durably-closed) segments will do once
            // rereplication has finished there is a durably open log
            // segment ahead of them in the log.
            if (!failedOpenSegmentId ||
                *failedOpenSegmentId < segment.segmentId) {
                failedOpenSegmentId.construct(segment.segmentId);
            }
        }
    }
    if (failedOpenSegmentId)
        LOG(DEBUG, "Highest affected segmentId %lu", *failedOpenSegmentId);

    return failedOpenSegmentId;
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
    if (replicatedSegment->isScheduled())
        LOG(WARNING, "Master exiting while segment %lu had operations "
            "still pending", replicatedSegment->segmentId);
    erase(replicatedSegmentList, *replicatedSegment);
    replicatedSegment->~ReplicatedSegment();
    replicatedSegmentPool.free(replicatedSegment);
}

} // namespace RAMCloud
