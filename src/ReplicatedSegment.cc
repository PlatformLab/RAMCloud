/* Copyright (c) 2011-2012 Stanford University
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

#include "BitOps.h"
#include "ReplicatedSegment.h"
#include "Segment.h"
#include "ShortMacros.h"

namespace RAMCloud {

uint64_t ReplicatedSegment::recoveryStart = 0;

/**
 * Toggles ordering constraints on log replication operations. This must be
 * enabled to ensure that replicas are be consistent in future recoveries and
 * to ensure that recovery won't report data loss if none has occurred.
 * Removing these contraints can be helpful for (replication performance)
 * testing.
 */
enum { OBEY_SAFETY_CONSTRAINTS = true };

// --- ReplicatedSegment ---

/**
 * Create a ReplicatedSegment.  Only called by ReplicaManager.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param taskQueue
 *      The ReplicaManager's work queue, this is added to it when schedule()
 *      is called.
 * \param backupSelector
 *      Used to choose where to store replicas. Shared among ReplicatedSegments.
 * \param writeRpcsInFlight
 *      Number of outstanding write rpcs to backups across all
 *      ReplicatedSegments.  Used to throttle write rpcs.
 * \param freeRpcsInFlight
 *      Number of outstanding free rpcs to backups across all
 *      ReplicatedSegments.  Used to throttle free rpcs.
 * \param replicationEpoch
 *      The ReplicaManager's UpdateReplicationEpochTask which is shared among
 *      ReplicatedSegments to track and update the replicationEpoch value
 *      stored on the coordinator.
 * \param dataMutex
 *      Mutex which protects all ReplicaManager state; shared with the
 *      ReplicaManager and all other ReplicatedSegments.
 * \param deleter
 *      Deletes this when this determines it is no longer needed.
 * \param segmentId
 *      Log-unique, 64-bit identifier for the segment being replicated.
 * \param segment
 *      Segment to be replicated. It is expected that the segment already
 *      contains a header; see \a openLen.
 * \param normalLogSegment
 *      False if this segment is being created by the log cleaner,
 *      true if this segment was opened as a head of the log (that is,
 *      it has a log digest and is to be actively written to by worker
 *      as a log head).
 * \param masterId
 *      The server id of the master whose log this segment belongs to.
 * \param numReplicas
 *      Number of replicas of this segment that must be maintained.
 * \param replicationCounter
 *      Used to measure time when backup write rpcs are active.
 *      Shared among ReplicatedSegments.
 * \param maxBytesPerWriteRpc
 *      Maximum bytes to send in a single write rpc; can help latency of
 *      GetRecoveryDataRequests by unclogging backups a bit.
 */
ReplicatedSegment::ReplicatedSegment(Context* context,
                                     TaskQueue& taskQueue,
                                     BaseBackupSelector& backupSelector,
                                     Deleter& deleter,
                                     uint32_t& writeRpcsInFlight,
                                     uint32_t& freeRpcsInFlight,
                                     UpdateReplicationEpochTask&
                                                            replicationEpoch,
                                     std::mutex& dataMutex,
                                     uint64_t segmentId,
                                     const Segment* segment,
                                     bool normalLogSegment,
                                     ServerId masterId,
                                     uint32_t numReplicas,
                                     Tub<CycleCounter<RawMetric>>*
                                                             replicationCounter,
                                     uint32_t maxBytesPerWriteRpc)
    : Task(taskQueue)
    , context(context)
    , backupSelector(backupSelector)
    , deleter(deleter)
    , writeRpcsInFlight(writeRpcsInFlight)
    , freeRpcsInFlight(freeRpcsInFlight)
    , replicationEpoch(replicationEpoch)
    , dataMutex(dataMutex)
    , syncMutex()
    , segment(segment)
    , normalLogSegment(normalLogSegment)
    , masterId(masterId)
    , segmentId(segmentId)
    , maxBytesPerWriteRpc(maxBytesPerWriteRpc)
    , queued(true, 0, 0, false)
    , queuedCertificate()
    , openLen(0)
    , openingWriteCertificate()
    , freeQueued(false)
    , followingSegment(NULL)
    , precedingSegmentCloseCommitted(true)
    , precedingSegmentOpenCommitted(true)
    , recoveringFromLostOpenReplicas(false)
    , listEntries()
    , replicationCounter(replicationCounter)
    , replicas(numReplicas)
{
    openLen = segment->getAppendedLength(&openingWriteCertificate);
    if (LOG_RECOVERY_REPLICATION_RPC_TIMING && recoveryStart) {
        LOG(DEBUG, "@%7lu: Segment <%s,%lu> open queued",
            Cycles::toMicroseconds(Cycles::rdtsc() - recoveryStart),
            masterId.toString().c_str(), segmentId);
    }
    queued.bytes = openLen;
    queuedCertificate = openingWriteCertificate;
    schedule(); // schedule to replicate the opening data
}

ReplicatedSegment::~ReplicatedSegment()
{
}

/**
 * Request the eventual freeing all known replicas of a segment from its
 * backups; segment must have already has close() called on it.
 * The caller's ReplicatedSegment pointer is invalidated upon the
 * return of this function. After the return of this call all outstanding
 * write rpcs for this segment are guaranteed to have completed so the log
 * memory associated with this segment is free for reuse. This implies that
 * this call can spin waiting for write rpcs, though, it tries to be
 * friendly to concurrent operations by releasing and reacquiring the
 * internal ReplicaManager lock each time it checks rpcs for completion.
 *
 * Currently, there is no public interface to ensure enqueued free
 * operations have completed.
 */
void
ReplicatedSegment::free()
{
    TEST_LOG("%s, %lu", masterId.toString().c_str(), segmentId);
    assert(queued.close); // Unlocked access, but... whatever.

    // Finish any outstanding work for this segment. This makes sure that if
    // other segments are waiting on things to happen with this segment it is
    // all taken care before we get rid of the replica. Because locking is
    // tricky for syncing a segment while tolerating failure notifications it
    // is best to leave the heavy lifting to sync().
    sync();

    Lock _(dataMutex);
    assert(!followingSegment);
    assert(getCommitted().close);

    // Since there is a gap in the lock above write rpcs could have started in
    // response to failures for this replica. It's safe just to cancel them
    // and queue the free. Marking the segment as free will prevent the start
    // of any more write rpcs. Canceling rpcs is NOT safe on all transports
    // for log data since on some (infrc) it is zero-copied. It's possible
    // the nic is in the middle of transmitting a write to the backup using
    // this portion of the log. However, even if this piece of memory is reused
    // the checksum stored in the replica metadata keeps this safe; if garbage
    // is sent it will not be used during recovery.
    foreach (auto& replica, replicas) {
        if (!replica.isActive || !replica.writeRpc)
            continue;
        replica.writeRpc->cancel();
        replica.writeRpc.destroy();
        --writeRpcsInFlight;
    }

    // Segment should free itself ASAP. It must not start new write rpcs after
    // this.
    freeQueued = true;

    schedule();
}

/**
 * Return true if no further actions are needed to durably replicate this
 * segment.  This can change as this master learns about failures in the
 * cluster.
 */
bool
ReplicatedSegment::isSynced() const
{
    if (recoveringFromLostOpenReplicas)
        return false;
    if (normalLogSegment && !precedingSegmentCloseCommitted)
        return false;
    uint32_t appendedBytes = segment->getAppendedLength();
    if (queued.bytes != appendedBytes)
        return false;
    if (getCommitted() != queued)
        return false;
    return true;
}

/**
 * Request the eventual close of the replicas of a segment on its backups.
 *
 * Once close() is called the only valid operation on the segment is free();
 * no further write() calls are permitted.  The caller cannot ensure that the
 * closed status of the segment is reflected durably in its replicas without
 * getting creative; this class takes care of that detail for callers.
 */
void
ReplicatedSegment::close()
{
    Lock _(dataMutex);
    TEST_LOG("%s, %lu, %lu", masterId.toString().c_str(), segmentId,
             followingSegment ? followingSegment->segmentId : 0);
    if (LOG_RECOVERY_REPLICATION_RPC_TIMING && recoveryStart) {
        LOG(DEBUG, "@%7lu: Segment <%s,%lu> close queued",
            Cycles::toMicroseconds(Cycles::rdtsc() - recoveryStart),
            masterId.toString().c_str(), segmentId);
    }

    // immutable after close
    assert(!queued.close);
    queued.close = true;
    // It is necessary to update queued.bytes here because the segment believes
    // it has fully replicated all data when queued.close and
    // getCommitted().bytes == queued.bytes.
    Segment::Certificate certificate;
    uint32_t appendedBytes = segment->getAppendedLength(&certificate);
    queued.bytes = appendedBytes;
    queuedCertificate = certificate;
    // Dealing with followingSegment here helps unit tests when
    // numReplicas == 0. Otherwise, performWrite never gets called so there is
    // no chance to clear it, which breaks calls to free().
    if (getCommitted().open && followingSegment)
        followingSegment->precedingSegmentOpenCommitted = true;
    if (getCommitted().close && followingSegment) {
        followingSegment->precedingSegmentCloseCommitted = true;
        // Don't poke at potentially non-existent segments later.
        followingSegment = NULL;
    }
    schedule();

    LOG(DEBUG, "Segment %lu closed (length %d)", segmentId, queued.bytes);
    ++metrics->master.segmentCloseCount;
}

/**
 * Respond to a change in cluster configuration by scheduling any work that is
 * needed to restore durability guarantees. Keep in mind a context needs to be
 * provided to actually drive the re-replication (for example,
 * BackupFailureMonitor).
 * If the server is using MinCopysets, we kill all the replicas that belong
 * to the same replication group if any of the group members fails. Otherwise,
 * we just kill the replica that belongs to the failed server.
 *
 * \param failedId
 *      ServerId of the backup which has failed.
 * \param useMinCopysets
 *      Specifies whether to use the MinCopysets replication scheme.
 */
void
ReplicatedSegment::handleBackupFailure(ServerId failedId, bool useMinCopysets)
{
    // If we are using MinCopysets and any of the backups on which the
    // segment fails, we re-replicate all the copies of that segment.
    bool replicationGroupFailed = false;
    if (useMinCopysets) {
        foreach (auto& replica, replicas) {
            if (!replica.isActive)
                continue;
            if (replica.backupId == failedId)
                replicationGroupFailed = true;
        }
    }

    bool someOpenReplicaLost = false;
    foreach (auto& replica, replicas) {
        if (!replica.isActive)
            continue;
        if (!replicationGroupFailed && replica.backupId != failedId)
            continue;
        LOG(DEBUG, "Segment %lu recovering from lost replica which was on "
            "backup %s", segmentId, failedId.toString().c_str());

        if (!replica.committed.close && !replica.replicateAtomically) {
            someOpenReplicaLost = true;
            LOG(DEBUG, "Lost replica(s) for segment %lu while open due to "
                "crash of backup %s", segmentId, failedId.toString().c_str());
            ++metrics->master.openReplicaRecoveries;
        }

        if (replica.writeRpc)
            --writeRpcsInFlight;
        if (replica.freeRpc)
            --freeRpcsInFlight;
        replica.failed();
        schedule();
        ++metrics->master.replicaRecoveries;
    }
    if (someOpenReplicaLost) {
        ++queued.epoch;
        recoveringFromLostOpenReplicas = true;
    }
}

/**
 * Wait for the durable replication (meaning at least durably buffered on
 * backups) of data starting at the beginning of the segment up through \a
 * offset bytes (non-inclusive). If \a offset is not provided wait for all
 * enqueued data AND closing flag to made durable on backups.
 * Using the no-arg form is the only way to safely wait for a closed segment
 * to be fully replicated.
 * After return the data will be recovered in the case that the master crashes
 * (provided warnings on ReplicatedSegment::close are obeyed). Note,
 * this method can wait forever if \a offset bytes are never enqueued
 * for replication (or if no \a offset is provided and close() is never
 * called).
 *
 * This must be called after any openSegment() calls where the operation must
 * be immediately durable (though, keep in mind, host failures could have
 * eliminated some replicas even as sync returns).
 *
 * ReplicaManager::dataMutex documents some of issues with locking that
 * affect this method. Two important subtleties are handled with the
 * locking in this method:
 * 1) #dataMutex cannot be held indefinitely while threads wait for their
 *    objects to be synced. This is because sync() may block forever in
 *    the case of failures until handleBackupFailure() can be called by
 *    the BackupFailureMonitor which also requires #dataMutex.
 * 2) #syncMutex chooses just one thread at a time to attempt to sync all
 *    the data found in the segment immediately after it acquires the locks.
 *    This is important because it prevents repeated calls to
 *    segment->getAppendedLength(). Repeated calls could "stretch out" the
 *    offset of the next certificate to be sent to the backups. Then, since
 *    there is a limit the size of write rpcs, it is possible that several
 *    back-to-back rpcs wouldn't include a certificate. Some sync()s could
 *    have to wait for several round trips while they wait for the next
 *    certificate to be sent out before they are considered committed and safe
 *    for acknowledgement to clients.
 *
 * \param offset
 *      The number of bytes of the segment that must be replicated before the
 *      call will return. If offset is not provided then the call will only
 *      return when all enqueued data has been synced including the
 *      closed flag.
 *
 * \param certificate
 *      If non-NULL, this points to the certificate associated with the provided
 *      offset and will be sent to backups if the segment has not been synced
 *      to this point yet.
 *
 *      Specifying this parameter avoids querying the segment for the latest
 *      length and certificate. This is sometimes necessary when one thread is
 *      syncing a segment, but does not want to block other threads from
 *      appending to it.
 */
void
ReplicatedSegment::sync(uint32_t offset, Segment::Certificate* certificate)
{
    CycleCounter<RawMetric> _(&metrics->master.replicaManagerTicks);
    TEST_LOG("syncing segment %lu to offset %u", segmentId, offset);

    Lock syncLock(syncMutex);
    Tub<Lock> lock;
    lock.construct(dataMutex);

    // Definition of synced changes if this segment isn't durably closed
    // and is recovering from a lost replica.  In that case the data
    // the data isn't durable until it has been replicated *along with*
    // a durable close on the replicas as well *and* any lost, open
    // replicas have been shot down by setting the replicationEpoch.
    // Once this flag is cleared those conditions have been met and
    // it is safe to use the usual definition.
    if (!recoveringFromLostOpenReplicas) {
        if (!normalLogSegment || precedingSegmentCloseCommitted) {
            if (offset == ~0u) {
                if (getCommitted().close)
                    return;
            } else {
                if (getCommitted().bytes >= offset)
                    return;
            }
        }
    }

    // If the caller did not provide the desired certificate, obtain the
    // latest one and use that.
    uint32_t appendedBytes = offset;
    Segment::Certificate localCertificate;
    if (certificate == NULL) {
        appendedBytes = segment->getAppendedLength(&localCertificate);
        certificate = &localCertificate;
    }

    if (appendedBytes > queued.bytes) {
        queued.bytes = appendedBytes;
        queuedCertificate = *certificate;
        schedule();
    }

    uint64_t syncStartTicks = Cycles::rdtsc();
    while (true) {
        taskQueue.performTask();
        if (!recoveringFromLostOpenReplicas) {
            if (!normalLogSegment || precedingSegmentCloseCommitted) {
                if (offset == ~0u) {
                    if (getCommitted().close)
                        return;
                } else {
                    if (getCommitted().bytes >= offset)
                        return;
                }
            }
        }
        double waited = Cycles::toSeconds(Cycles::rdtsc() - syncStartTicks);
        if (waited > 1) {
            LOG(WARNING, "Log write sync has taken over 1s; seems to be stuck");
            dumpProgress();
            syncStartTicks = Cycles::rdtsc();
        }
        lock.construct(dataMutex);
    }
}

/**
 * Replace the current in-memory segment this object is providing durability for
 * with a different, but logically identical one, and return the old segment.
 *
 * This is used during memory compaction when segment A has dead entries dropped
 * and an equivalent segment A' is produced. In order to free the memory used by
 * A, the replicated segment for A must be told to use the new version (A') and
 * drop any references to the previous one.
 *
 * An alternative would be to create an entirely new replicated segment for A',
 * but that would cause A' to be written to backups unnecessarily (A' only ever
 * needs to be written to backups if a replica for A fails).
 *
 * After this method returns, the previous segment that was replaced will never
 * again be accessed by the replication system.
 *
 * This method may block until the current segment has been fully replicated
 * before proceeding.
 *
 * This method may only be called on segments that have already been closed.
 *
 * \param newSegment
 *      A new segment this replicated segment will provide replication for. It
 *      must be logically identical to the current segment. That is, it must
 *      have the same segment id and same live data contents.
 * \return
 *      The previous segment that was just replaced is returned.
 */
const Segment*
ReplicatedSegment::swapSegment(const Segment* newSegment)
{
    // Wait until we're at a quiescent point for this segment.
    // There should be no outstanding RPCs.
    Tub<Lock> lock;
    while (true) {
        lock.construct(dataMutex);
        if (isSynced())
            break;
        lock.destroy();
        sync();
    }
    assert(getCommitted() == queued);

    // Only closed segments are permitted.
    assert(getCommitted().close);

    const Segment* oldSegment = segment;
    segment = newSegment;

    queued.bytes = segment->getAppendedLength(&queuedCertificate);
    foreach (auto& replica, replicas)
        replica.committed = replica.acked = replica.sent = queued;

    // TODO(stutsman): Does openingWriteCertificate need to be updated?
    //                 Will, for example, replicateAtomically ever affect this?
    //
    //                 Should anything be done about openLen?
    //
    //                 Also, why do we send openLen bytes in the open RPC when
    //                 rereplicating closed segments in performWrite? If no
    //                 certificate is sent, shouldn't it not matter? Why not
    //                 send maxBytesPerWriteRpc?

    return oldSegment;
}

// - private -

/**
 * Schedule this task if the number of replicas is greater than zero.
 */
void
ReplicatedSegment::schedule()
{
    if (replicas.empty())
        TEST_LOG("zero replicas: nothing to schedule");
    else
        Task::schedule();
}

/**
 * Check replication state and make progress in restoring invariants;
 * generally don't invoke this directly, instead use schedule().
 *
 * This method must be called (indirectly via schedule()) when the
 * state of this ReplicatedSegment changes in a non-trivial way in order to
 * ensure that replication invariants hold and to start work in response,
 * if needed.
 *
 * schedule() is called in three cases:
 * 1) A cluster membership change may have affected replication invariants
 *    for this segment.
 * 2) An action by the log module requires some work to be done (e.g. more
 *    replication, freeing replicas).
 * 3) An action done during performTask() itself requires future work (e.g.
 *    work couldn't be completed yet or work generated some new work
 *    which won't be done until a future time).
 */
void
ReplicatedSegment::performTask()
{
    if (freeQueued && !recoveringFromLostOpenReplicas) {
        foreach (auto& replica, replicas)
            performFree(replica);
        if (!isScheduled()) // Everything is freed, destroy ourself.
            deleter.destroyAndFreeReplicatedSegment(this);
    } else if (!freeQueued) {
        foreach (auto& replica, replicas)
            performWrite(replica);
    }
    // Have to be a bit careful: these steps must be completed even if a
    // free has been enqueued, otherwise lost open replicas could still
    // be detected as the head of the log during a recovery. Hence the
    // extra condition above.
    if (recoveringFromLostOpenReplicas) {
        if (getCommitted() == queued) {
            // Take care to update to the queued.epoch, not the committed epoch.
            // If enough replicas are closed on backups regardless of the
            // committed epoch we ok to shoot down stale replicas. In that case,
            // though, committed epoch might still include stale replicas.
            if (replicationEpoch.isAtLeast(segmentId, queued.epoch)) {
                // Ok, this segment is now recovered.
                LOG(DEBUG,
                    "replicationEpoch ok, lost open replica recovery "
                    "complete on segment %lu", segmentId);
                recoveringFromLostOpenReplicas = false;
                // All done, don't reschedule.
            } else  {
                // Now that the old head segment has been re-replicated and all
                // replicas have the new epoch its time to make sure replicas
                // with old epochs can never appear as an open segment in the
                // log again (even if some lost replica comes back from the
                // grave).
                LOG(DEBUG, "Updating replicationEpoch to %lu,%lu on "
                    "coordinator to ensure lost replicas will not be reused",
                    segmentId, queued.epoch);
                replicationEpoch.updateToAtLeast(segmentId, queued.epoch);
                schedule();
            }
        } else {
            // This shouldn't be needed, but if the code handling the roll
            // over to the new log head doesn't close this segment before
            // the next call to proceed it's possible no work will be
            // possible or performWrite() won't schedule it, but it also
            // isn't synced since it is recovering.
            schedule();
        }
    }
    if (replicationCounter) {
        if (writeRpcsInFlight > 0) {
            if (!*replicationCounter)
                replicationCounter->
                    construct(&metrics->master.replicationTicks);
        } else {
                replicationCounter->destroy();
        }
    }
}

/**
 * Make progress, if possible, in freeing a known replica of a segment
 * regardless of what state the replica is in (both locally and remotely).
 * If future work is required this method automatically re-schedules this
 * segment for future attention from the ReplicaManager.
 * \pre freeQueued must be true, otherwise behavior is undefined.
 */
void
ReplicatedSegment::performFree(Replica& replica)
{
    /*
     * Internally this method is written as a set of nested
     * if-with-unqualified-else clauses (sometimes the else is implicit) with
     * explicit returns at the end of each block.  This repeatedly splits the
     * segment states between two cases until exactly one of the cases is
     * executed.  This makes it easy ensure all cases are covered and which
     * case a particular state will fall into.  performWrite() is written
     * is a similar style for the same reason.
     */
    if (!replica.isActive) {
        // Do nothing if there was no replica, no need to reschedule.
        return;
    }

    if (replica.freeRpc) {
        // A free rpc is outstanding to the backup storing this replica.
        if (replica.freeRpc->isReady()) {
            // Request is finished, clean up the state.
            try {
                replica.freeRpc->wait();
            } catch (const ServerNotUpException& e) {
                // If the backup is already out of the cluster the master's
                // job is done. If the replica is found on storage when the
                // process restarts on that server it will be the job of the
                // backup's replica garbage collector to free it.
                TEST_LOG("ServerNotUpException thrown");
            }
            // Let the backupSelector know if a primary replica is freed.
            if (replicaIsPrimary(replica)) {
                backupSelector.signalFreedPrimary(replica.backupId);
            }
            replica.reset();
            --freeRpcsInFlight;
            // Free completed, no need to reschedule.
            return;
        } else {
            // Request is not yet finished, stay scheduled to wait on it.
            schedule();
            return;
        }
    } else {
        // No free rpc is outstanding.
        if (freeRpcsInFlight == MAX_FREE_RPCS_IN_FLIGHT) {
            schedule();
            return;
        }
        if (replica.writeRpc) {
            // Impossible by construction. See free().
            assert(false);
        } else {
            // Issue a free rpc for this replica, reschedule to wait on it.
            replica.freeRpc.construct(context, replica.backupId,
                                      masterId, segmentId);
            ++freeRpcsInFlight;
            schedule();
            return;
        }
    }
    assert(false); // Unreachable by construction.
}

/**
 * Make progress, if possible, in durably writing segment data to a particular
 * replica.  If future work is required this method automatically re-schedules
 * this segment for future attention from the ReplicaManager.
 * \pre freeQueued must be false, otherwise behavior is undefined.
 */
void
ReplicatedSegment::performWrite(Replica& replica)
{
    assert(!replica.freeRpc);

    if (replica.isActive && replica.committed == queued) {
        // If this replica is synced no further work is needed for now.
        return;
    }

    if (!replica.isActive) {
        // This replica does not exist yet. Choose a backup.
        // Selection of a backup is separated from the send of the open rpc
        // because failures of the open rpc require retrying on the same
        // backup unless it is discovered that that backup failed.
        // Not doing so risks the existence a lost open replica which
        // isn't recovered from properly.
        ServerId constraints[replicas.numElements];
        uint32_t numConstraints = 0;
        foreach (auto& constrainingReplica, replicas) {
            if (constrainingReplica.isActive)
                constraints[numConstraints++] = constrainingReplica.backupId;
            assert(numConstraints <= replicas.numElements);
        }
        ServerId backupId;
        if (replicaIsPrimary(replica)) {
            backupId = backupSelector.selectPrimary(numConstraints,
                                                    constraints);
        } else {
            backupId = backupSelector.selectSecondary(numConstraints,
                                                      constraints);
        }

        if (!backupId.isValid()) {
            schedule();
            return;
        }

        LOG(DEBUG, "Starting replication of segment %lu replica slot %ld "
            "on backup %s", segmentId, &replica - &replicas[0],
            backupId.toString().c_str());
        replica.start(backupId);
        // Fall-through: this should drop down into the case that no
        // writeRpc is outstanding and the open hasn't been acknowledged
        // yet to send out the open rpc.  That block is also responsible
        // for scheduling the task.
    }

    if (replica.writeRpc) {
        // This replica has a write request outstanding to a backup.
        if (replica.writeRpc->isReady()) {
            // Wait for it to complete if it is ready.
            try {
                replica.writeRpc->wait();
                TEST_LOG("Write RPC finished for replica slot %ld",
                         &replica - &replicas[0]);
                replica.acked = replica.sent;
                if (replica.acked == queued || replica.acked.bytes == openLen) {
                    // #committed advances whenever a certificate was sent.
                    // Which happens in two cases:
                    // a) all the queued data was acked or
                    // b) the opening write was acked
                    replica.committed = replica.acked;
                }
                if (getCommitted().open && followingSegment)
                    followingSegment->precedingSegmentOpenCommitted = true;
                if (getCommitted().close && followingSegment) {
                    followingSegment->precedingSegmentCloseCommitted = true;
                    // Don't poke at potentially non-existent segments later.
                    followingSegment = NULL;
                }
            } catch (const ServerNotUpException& e) {
                // Retry; wait for BackupFailureMonitor to call
                // handleBackupFailure to reset the replica and break this
                // loop.
                replica.sent = replica.acked;
                LOG(WARNING, "Couldn't write to backup %s; server is down",
                    replica.backupId.toString().c_str());
            } catch (const BackupOpenRejectedException& e) {
                // TODO(Stutsman): This message occurs far too frequently to
                // print when backups are being heavily loaded. Perhaps we want
                // to differentiate the loaded and existing replica cases, as
                // the former might be rarer and of greater interest.
                LOG(DEBUG,
                    "Couldn't open replica on backup %s; server may be "
                    "overloaded or may already have a replica for this segment "
                    "which was found on disk after a crash; will choose "
                    "another backup", replica.backupId.toString().c_str());
                replica.reset();
            } catch (const CallerNotInClusterException& e) {
                // The backup seems to think we have crashed (or never existed).
                // Check with the coordinator to be sure, then retry.  See
                // "Zombies" in designNotes for more information.
                LOG(WARNING, "Backup write RPC rejected by %s with "
                    "STATUS_CALLER_NOT_IN_CLUSTER",
                    replica.backupId.toString().c_str());
                replica.sent = replica.acked;
                CoordinatorClient::verifyMembership(context, masterId);
            }
            replica.writeRpc.destroy();
            --writeRpcsInFlight;
            if (LOG_RECOVERY_REPLICATION_RPC_TIMING && recoveryStart) {
                LOG(DEBUG, "@%7lu: Replica <%s,%lu,%lu> write <- %7u "
                    "%u rpcs out %s",
                    Cycles::toMicroseconds(Cycles::rdtsc() - recoveryStart),
                    masterId.toString().c_str(),
                    segmentId, &replica - &replicas[0], replica.acked.bytes,
                    writeRpcsInFlight, replica.committed.close ? " CLOSE" : "");
            }
            if (replica.committed != queued || recoveringFromLostOpenReplicas)
                schedule();
            return;
        } else {
            // Request is not yet finished, stay scheduled to wait on it.
            schedule();
            return;
        }
    } else {
        if (!replica.committed.open) {
            if (OBEY_SAFETY_CONSTRAINTS && !precedingSegmentOpenCommitted) {
                TEST_LOG("Cannot open segment %lu until preceding segment "
                         "is durably open", segmentId);
                schedule();
                return;
            }
            // No outstanding write, but not yet durably open.
            if (writeRpcsInFlight == MAX_WRITE_RPCS_IN_FLIGHT) {
                schedule();
                return;
            }

            // If segment is being re-replicated don't send the certificate
            // for the opening write; the replica should atomically commit when
            // it has been fully caught up.
            Segment::Certificate* certificateToSend = &openingWriteCertificate;
            if (replica.replicateAtomically)
                certificateToSend = NULL;

            TEST_LOG("Sending open to backup %s",
                     replica.backupId.toString().c_str());
            replica.writeRpc.construct(context, replica.backupId,
                                       masterId, segmentId, queued.epoch,
                                       segment, 0, openLen, certificateToSend,
                                       true, false, replicaIsPrimary(replica));
            ++writeRpcsInFlight;
            if (LOG_RECOVERY_REPLICATION_RPC_TIMING && recoveryStart) {
                LOG(DEBUG, "@%7lu: Replica <%s,%lu,%lu> write -> %7u+%7u "
                    "%u rpcs out OPEN",
                    Cycles::toMicroseconds(Cycles::rdtsc() - recoveryStart),
                    masterId.toString().c_str(), segmentId,
                    &replica - &replicas[0],
                    0, openLen, writeRpcsInFlight);
            }
            replica.sent.open = true;
            replica.sent.bytes = openLen;
            replica.sent.epoch = queued.epoch;
            schedule();
            return;
        }

        // No outstanding write but not yet synced.
        if (replica.sent < queued) {
            // Some part of the data hasn't been sent yet.  Send it.
            if (OBEY_SAFETY_CONSTRAINTS && !precedingSegmentCloseCommitted) {
                TEST_LOG("Cannot write segment %lu until preceding segment "
                         "is durably closed", segmentId);
                // This segment must wait to send write rpcs until the
                // preceding segment in the log sets
                // precedingSegmentCloseCommitted to true. The goal is to
                // prevent data written in this segment from being undetectably
                // lost in the case that all replicas of it are lost. See
                // #precedingSegmentCloseCommitted.

                schedule();
                return;
            }

            uint32_t offset = replica.sent.bytes;
            uint32_t length = queued.bytes - offset;
            Segment::Certificate* certificateToSend = &queuedCertificate;

            // Breaks atomicity of log entries, but it could happen anyway
            // if a segment gets partially written to disk.
            if (length > maxBytesPerWriteRpc) {
                length = maxBytesPerWriteRpc;
                certificateToSend = NULL;
            }

            bool sendClose = queued.close && (offset + length) == queued.bytes;
            if (OBEY_SAFETY_CONSTRAINTS &&
                sendClose &&
                followingSegment &&
                !followingSegment->getCommitted().open) {
                TEST_LOG("Cannot close segment %lu until following segment "
                         "is durably open", segmentId);
                // Do not send a closing write rpc for this replica until
                // some other segment later in the log has been durably
                // opened.  This ensures that the coordinator will find
                // an open segment during recovery which lets it know
                // the entire log has been found (that is, log isn't missing
                // some head segments).
                schedule();
                return;
            }

            if (writeRpcsInFlight == MAX_WRITE_RPCS_IN_FLIGHT) {
                TEST_LOG("Cannot write segment %lu, too many writes "
                         "in flight", segmentId);
                schedule();
                return;
            }

            TEST_LOG("Sending write to backup %s",
                     replica.backupId.toString().c_str());
            replica.writeRpc.construct(context, replica.backupId, masterId,
                                       segmentId, queued.epoch,
                                       segment, offset, length,
                                       certificateToSend,
                                       false, sendClose,
                                       replicaIsPrimary(replica));
            ++writeRpcsInFlight;
            if (LOG_RECOVERY_REPLICATION_RPC_TIMING && recoveryStart) {
                LOG(DEBUG, "@%7lu: Replica <%s,%lu,%lu> write -> %7u+%7u "
                    "%u rpcs out %s",
                    Cycles::toMicroseconds(Cycles::rdtsc() - recoveryStart),
                    masterId.toString().c_str(), segmentId,
                    &replica - &replicas[0], offset, length,
                    writeRpcsInFlight, sendClose ? " CLOSE" : "");
            }
            replica.sent.bytes += length;
            replica.sent.epoch = queued.epoch;
            replica.sent.close = sendClose;
            schedule();
            return;
        } else {
            // Replica not synced, no rpc outstanding, but all data was sent.
            // Impossible in the one in-flight rpc per replica case.
            assert(false);
            return;
        }
    }
    assert(false); // Unreachable by construction
}

/**
 * Prints a ton of internal state of the replica. Useful for diagnosing why
 * a particular segment's replication is stuck.
 */
void
ReplicatedSegment::dumpProgress()
{
    string info = format(
        "ReplicatedSegment <%s,%lu>\n"
        "    queued: open %u, bytes %u, close %u\n"
        "    committed: open %u, bytes, %u, close %u\n",
        masterId.toString().c_str(), segmentId,
        queued.open, queued.bytes, queued.close,
        getCommitted().open, getCommitted().bytes, getCommitted().close);
    uint32_t i = 0;
    foreach (const auto& replica, replicas) {
        string backupLocator = "<unknown>";
        try {
            backupLocator = context->serverList->getLocator(replica.backupId);
        } catch (ServerListException& e) {}
        info.append(format(
            "  Replica %u on Backup %s (%s)\n"
            "    sent: open %u, bytes %u, close %u\n"
            "    acked: open %u, bytes %u, close %u\n"
            "    committed: open %u, bytes, %u, close %u\n"
            "    write rpc outstanding: %u\n",
            i++,
            replica.backupId.toString().c_str(), backupLocator.c_str(),
            replica.sent.open, replica.sent.bytes, replica.sent.close,
            replica.acked.open, replica.acked.bytes, replica.acked.close,
            replica.committed.open, replica.committed.bytes,
            replica.committed.close,
            bool(replica.writeRpc)));
    }
    LOG(NOTICE, "\n%s", info.c_str());
}

} // namespace RAMCloud
