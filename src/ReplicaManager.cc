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

#include "BackupClient.h"
#include "CycleCounter.h"
#include "Logger.h"
#include "MinCopysetsBackupSelector.h"
#include "ShortMacros.h"
#include "RawMetrics.h"
#include "ReplicaManager.h"
#include "Segment.h"

namespace RAMCloud {

// --- ReplicaManager ---

/**
 * Create a ReplicaManager.  Creating more than one ReplicaManager for a
 * single log results in undefined behavior.
 * \param context
 *      Overall information about the RAMCloud server.
 * \param masterId
 *      Server id of master that this will be managing replicas for (also
 *      serves as the log id).
 * \param numReplicas
 *      Number replicas to keep of each segment.
 * \param useMinCopysets
 *      Specifies whether to use the MinCopysets replication scheme or random
 *      replication.
 * \param allowLocalBackup
 *      Specifies whether to allow replication to the local backup.
 */
ReplicaManager::ReplicaManager(Context* context,
                               const ServerId* masterId,
                               uint32_t numReplicas,
                               bool useMinCopysets,
                               bool allowLocalBackup)
    : context(context)
    , numReplicas(numReplicas)
    , backupSelector()
    , dataMutex()
    , masterId(masterId)
    , replicatedSegmentPool(ReplicatedSegment::sizeOf(numReplicas))
    , replicatedSegmentList()
    , taskQueue()
    , writeRpcsInFlight(0)
    , freeRpcsInFlight(0)
    , replicationEpoch()
    , failureMonitor(context, this)
    , replicationCounter()
    , useMinCopysets(useMinCopysets)
    , allowLocalBackup(allowLocalBackup)
{
    if (useMinCopysets) {
        backupSelector.reset(new MinCopysetsBackupSelector(context, masterId,
                                                           numReplicas,
                                                           allowLocalBackup));
    } else {
        backupSelector.reset(new BackupSelector(context, masterId,
                                                numReplicas, allowLocalBackup));
    }
    replicationEpoch.construct(context, &taskQueue, masterId);
}

/**
 * Sync replicas with all queued operations, wait for any outstanding frees
 * to complete, then cleanup and release an local resources (all durably
 * stored but unfreed replicas will remain on backups).
 */
ReplicaManager::~ReplicaManager()
{
    // Make sure failureMonitor isn't in proceed().
    failureMonitor.halt();

    Lock lock(dataMutex);

    if (!taskQueue.isIdle())
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
    return taskQueue.isIdle();
}

/**
 * Indicates whether a replica for a particular segment that this master
 * generated is needed for durability, or whether it can be safely discarded.
 *
 * This method ensures (together with server list update ordering constraints
 * and backup server id replacement on enlistment) that the ReplicaManager has
 * been made aware (via #failureMonitor) of all the failures necessary
 * to ensure the master can correctly respond to the garbage collection query.
 *
 * \param backupServerId
 *      The id of the server which has the replica in storage. This is used
 *      to ensure that this master only retuns false if it has been made aware
 *      of any crashes of (now dead) backup servers which used the same
 *      storage as the calling backup.
 * \param segmentId
 *      The id of the segment of the replica whose status is in question.
 * \return
 *      True if the replica for \a segmentId may be needed to recover from
 *      failures.  False if the replica is no longer needed because the
 *      segment is fully replicated.
 */
bool
ReplicaManager::isReplicaNeeded(ServerId backupServerId, uint64_t segmentId)
{
    // If backupServerId does not appear "up" then the master can be in one
    // of two situtations:
    // 1) It hasn't heard about backupServerId yet, in which case it also
    //    may not know that the failure of the server which backupServerId
    //    is replacing (i.e. the backup that formerly operated the storage
    //    that backupServerId has restarted from and is attempting to
    //    garbage collect).
    // 2) backupServerId has come and gone in the cluster and is actually
    //    now dead.
    // In either of these cases the only safe thing to do is to tell
    // the backup to hold on to the replica.
    // serverIsUp() can return false even when backupServerId is up according
    // to this server if it would have to wait on a lock to determine for
    // sure. This code does 'the right thing' in that case and just tells
    // the backup to hold on to the replica and come back later when the
    // lock isn't contended.
    if (!failureMonitor.serverIsUp(backupServerId))
        return true;

    Lock lock(dataMutex);
    // Now that the master knows it has at least heard and processed
    // the failure notification for the backup server that is being
    // replaced by backupServerId it is safe to indicate the replica
    // is no longer needed if the segment seems to be fully replicated.

    // TODO(stutsman): Slow, probably want to add an incrementally
    // maintained index to ReplicaManager.
    foreach (auto& segment, replicatedSegmentList) {
        if (segmentId == segment.segmentId)
            return !segment.isSynced();
    }

    return false;
}

/**
 * Enqueue a segment for replication on backups, return a handle to schedule
 * future operations on the segment; this method is for "normal" log segments
 * which are allocated as part of the main-line log, see allocateNonHead() to
 * create other types of segments. Selection of backup locations and
 * replication are performed at a later time. The segment data isn't guaranteed
 * to be durably open on backups until sync() is called. The returned handle
 * allows future operations like enqueueing more data for replication, waiting
 * for data to be replicated, or freeing replicas. Read the documentation for
 * ReplicatedSegment::write, ReplicatedSegment::close, ReplicatedSegment::free
 * carefully; some of the requirements and guarantees in order to ensure data
 * is recovered correctly after a crash are subtle.
 *
 * Details of the ordering constraints (most readers will want to skip down
 * to params now):
 *
 * The timing of when opens/closes are replicated for a segment relative to
 * open and write requests for the following segment affects the integrity
 * of the log during recovery. During log cleaning and unit testing this
 * ordering isn't important (see Log cleaning and unit testing below).
 *
 * Normal operation:
 *
 * ReplicatedSegments are chained together in the order they are to appear in
 * the log via calls to allocateHead(). This is used to enforce a safe
 * ordering of operations issued to backups; therefore, its correct use is
 * critical to ensure:
 *  1) That log is not mistakenly detected as incomplete during recovery, and
 *  2) That all data loss is detected during recovery.
 *
 * For a log transitioning from a full head segment s1 to a new, empty
 * head segment s2 the caller must guarantee:
 *  s2 = allocateHead(s1, ...);
 *
 * Explanation of the problems which can occur:
 *
 * Problem 1.
 * If the coordinator cannot find an open log segment during recovery it
 * has no way of knowing if it has all of the log (any number of segments
 * from the head may have been lost). Because of this it is critical that
 * there always be at least one segment durably marked as open on backups.
 * Call this the open-before-close rule. followingSegment allows an easy
 * check to make sure that the new head segment in the log is durably open
 * before issuing any close rpcs for old head segment.
 * Not obeying open-before-close threatens the integrity of the entire log
 * during recovery.
 *
 * Problem 2.
 * If log data is (even durably) stored in an open segment while other
 * segments which precede it in the log are still open the data may not be
 * detected as lost if it is lost. This is because if all the replicas for
 * the segment with the data in it are lost the coordinator will still
 * conclude it has recovered the entire log since it was able to find an
 * open segment (and thus the head of the log).
 * Call this the no-write-before-preceding-close rule; not obeying this rule
 * can result in loss of data acknowledged to applications as durable after
 * a recovery.
 *
 * Problem 3.
 * Asynchronous writes (particularly during recovery) can cause many segments
 * to be allocated without the data in them being flushed out. If the segments
 * are opened on backups out-of-order then the log digests might indicate
 * segments which have not yet made it to backups. This would cause the
 * detection of data loss in an otherwise fine log. Forcing open requests
 * to the backups to proceed in the order the segments were allocated solves
 * this.
 *
 * Together these rules transitively create the following flow during
 * normal operation for any two segments s1 and s2 which follows s1:
 * s1 is durably opened -> s2 is durably opened -> s1 is durably closed ->
 * writes are issued for s2.
 * This cycle repeats as segments are added to the log.
 *
 * Log cleaning and unit testing:
 *
 * During log cleaning many segments at a time are allocated and written
 * (writes for different cleaned segments can be interleaved) and sync() is
 * called explicitly at the end to ensure all writes on them have been
 * completed before they are added to the log.  Since they are spliced into
 * the log atomically as part of another open segment they do not (and cannot
 * obey these extra ordering constraints). To bypass these constraints the
 * log cleaner can simply pass NULL in for \a precedingSegment. Similarly,
 * since unit tests should almost always pass NULL to avoid these extra
 * ordering checks.
 *
 * Cleaned log segment replicas can appear as open during recovery without
 * issue (neither this class or the caller are expected to wait for those
 * segments to be durably closed). This is because the system will not
 * consider segments without a digest to be the head of the log and a cleaned
 * replica can only be considered part of the log if it was named in a log
 * digest. Cleaned segment replicas are simply sync()'ed before being
 * spliced into the log to ensure all the data is durable.
 *
 * \param segmentId
 *      Log-unique 64-bit identifier for the segment being replicated.
 * \param segment
 *      Segment to be replicated. It is expected that the segment already
 *      contains a header. The segment must live at least until free() is
 *      called on the returned handle, and, up until then, it must be able
 *      to accept Segment::appendRangeToBuffer() calls for any region of the
 *      segment which is covered by any calls to sync(). Caller must ensure
 *      no segment with the same segmentId has ever been opened before as
 *      part of the log this ReplicaManager is managing.
 * \param precedingSegment
 *      The current log head. Used to set up ordering constraints on the
 *      operations issued to backups to ensure safety during crashes. Pass
 *      NULL if this is the first segment in the log.
 * \return
 *      Pointer to a ReplicatedSegment that is valid until
 *      ReplicatedSegment::free() is called on it or until the ReplicaManager
 *      is destroyed.
 */
ReplicatedSegment*
ReplicaManager::allocateHead(uint64_t segmentId,
                             const Segment* segment,
                             ReplicatedSegment* precedingSegment)
{
    CycleCounter<RawMetric> _(&metrics->master.replicaManagerTicks);
    Lock lock(dataMutex);
    auto* replicatedSegment = allocateSegment(lock, segmentId,
                                              segment, true);

    // Set up ordering constraints between this new segment and the prior
    // one in the log.
    if (precedingSegment) {
        precedingSegment->followingSegment = replicatedSegment;
        replicatedSegment->precedingSegmentCloseCommitted =
            precedingSegment->getCommitted().close;
        replicatedSegment->precedingSegmentOpenCommitted =
            precedingSegment->getCommitted().open;
    }

    return replicatedSegment;
}

/**
 * Enqueue a segment for replication on backups, return a handle to schedule
 * future operations on the segment; this method is for log segments
 * which are NOT allocated as part of the main-line log (segments generated
 * by the log cleaner, for example). The replica manager makes no guarantees
 * about ordering of replication operations relative to other segments when
 * this method is used. The caller is responsible for using methods (sync(),
 * for example) to ensure data is durable.
 *
 * The caller must not reuse the memory starting at #data up through the bytes
 * enqueued via ReplicatedSegment::write until after ReplicatedSegment::free
 * is called and returns (until that time outstanding backup write rpcs may
 * still refer to the segment data).
 *
 * \param segmentId
 *      Log-unique 64-bit identifier for the segment being replicated.
 * \param segment
 *      Segment to be replicated. It is expected that the segment already
 *      contains a header. The segment must live at least until free() is
 *      called on the returned handle, and, up until then, it must be able to
 *      accept Segment::appendRangeToBuffer() calls for any region of the
 *      segment which is covered by any calls to sync(). Caller must ensure
 *      no segment with the same segmentId has ever been opened before as
 *      part of the log this ReplicaManager is managing.
 * \return
 *      Pointer to a ReplicatedSegment that is valid until
 *      ReplicatedSegment::free() is called on it or until the ReplicaManager
 *      is destroyed.
 */
ReplicatedSegment*
ReplicaManager::allocateNonHead(uint64_t segmentId, const Segment* segment)
{
    CycleCounter<RawMetric> _(&metrics->master.replicaManagerTicks);
    Lock lock(dataMutex);
    return allocateSegment(lock, segmentId, segment, false);
}

/**
 * Start monitoring for failures. Subsequent calls to startFailureMonitor()
 * have no effect.
 */
void
ReplicaManager::startFailureMonitor()
{
    failureMonitor.start();
}

/**
 * Stop monitoring for failures.
 * After this call returns the ReplicaManager holds no references to the
 * Log (passed in on startFailureMonitor()). Failing to call this before
 * the destruction of the Log will result in undefined behavior.
 */
void
ReplicaManager::haltFailureMonitor()
{
    failureMonitor.halt();
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
    taskQueue.performTask();
    metrics->master.replicationTasks =
        std::max(metrics->master.replicationTasks.load(),
                 taskQueue.outstandingTasks());
}

// - private -

/**
 * Provides code common to allocateHead() and allocateNonHead(). See those
 * methods for more details.
 *
 * \param lock
 *      Caller must acquire a lock on #dataMutex before calling. Variable
 *      is unused, but guarantees the caller at least thought about safety.
 * \param segmentId
 *      Log-unique 64-bit identifier for the segment being replicated.
 * \param segment
 *      Segment to be replicated. It is expected that the segment already
 *      contains a header. The segment must live at least until free() is
 *      called on the returned handle, and, up until then, it must be able
 *      to accept Segment::appendRangeToBuffer() calls for any region of
 *      the segment which is covered by any calls to sync(). Caller must
 *      ensure no segment with the same segmentId has ever been opened
 *      before as part of the log this ReplicaManager is managing.
 * \param isLogHead
 *      True if the segment being allocated is part of the main-line log
 *      and should have ordering constraints placed on it to ensure the
 *      log remains recoverable. False indicates the segment is for
 *      other purposes and that the caller is responsible for using sync()
 *      to ensure data is durable before use.
 * \return
 *      Pointer to a ReplicatedSegment that is valid until
 *      ReplicatedSegment::free() is called on it or until the ReplicaManager
 *      is destroyed.
 */
ReplicatedSegment*
ReplicaManager::allocateSegment(const Lock& lock,
                                uint64_t segmentId,
                                const Segment* segment,
                                bool isLogHead)
{
    LOG(DEBUG, "Allocating new replicated segment for <%s,%lu>",
        masterId->toString().c_str(), segmentId);
    auto* p = replicatedSegmentPool.malloc();
    if (p == NULL)
        DIE("Out of memory");
    ReplicatedSegment* replicatedSegment =
        new(p) ReplicatedSegment(context, taskQueue, *backupSelector, *this,
                                 writeRpcsInFlight, freeRpcsInFlight,
                                 *replicationEpoch,
                                 dataMutex, segmentId, segment,
                                 isLogHead, *masterId, numReplicas,
                                 &replicationCounter);
    replicatedSegmentList.push_back(*replicatedSegment);

    // ReplicatedSegment's constructor has scheduled the open.

    return replicatedSegment;
}

/**
 * Respond to a change in cluster configuration by scheduling any work that is
 * needed to restore durability guarantees. Keep in mind a context needs to be
 * provided to actually drive the re-replication (for example,
 * BackupFailureMonitor).
 *
 * \param failedId
 *      ServerId of the backup which has failed.
 */
void
ReplicaManager::handleBackupFailure(ServerId failedId)
{
    Lock _(dataMutex);
    LOG(NOTICE, "Handling backup failure of serverId %s",
        failedId.toString().c_str());

    foreach (auto& segment, replicatedSegmentList)
        segment.handleBackupFailure(failedId, useMinCopysets);
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
