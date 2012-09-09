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
#include "CycleCounter.h"
#include "ReplicatedSegment.h"
#include "Segment.h"
#include "ShortMacros.h"

namespace RAMCloud {

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
 * \param minOpenSegmentId
 *      The ReplicaManager's MinOpenSegmentId Task which is shared among
 *      ReplicatedSegments to track and update the minOpenSegmentId value
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
 * \param maxBytesPerWriteRpc
 *      Maximum bytes to send in a single write rpc; can help latency of
 *      GetRecoveryDataRequests by unclogging backups a bit.
 */
ReplicatedSegment::ReplicatedSegment(Context* context,
                                     TaskQueue& taskQueue,
                                     BaseBackupSelector& backupSelector,
                                     Deleter& deleter,
                                     uint32_t& writeRpcsInFlight,
                                     MinOpenSegmentId& minOpenSegmentId,
                                     std::mutex& dataMutex,
                                     uint64_t segmentId,
                                     const Segment* segment,
                                     bool normalLogSegment,
                                     ServerId masterId,
                                     uint32_t numReplicas,
                                     uint32_t maxBytesPerWriteRpc)
    : Task(taskQueue)
    , context(context)
    , backupSelector(backupSelector)
    , deleter(deleter)
    , writeRpcsInFlight(writeRpcsInFlight)
    , minOpenSegmentId(minOpenSegmentId)
    , dataMutex(dataMutex)
    , syncMutex()
    , segment(segment)
    , normalLogSegment(normalLogSegment)
    , masterId(masterId)
    , segmentId(segmentId)
    , maxBytesPerWriteRpc(maxBytesPerWriteRpc)
    , queued(true, 0, false)
    , queuedFooterEntry()
    , openLen(0)
    , openingWriteFooterEntry()
    , freeQueued(false)
    , followingSegment(NULL)
    , precedingSegmentCloseCommitted(true)
    , precedingSegmentOpenCommitted(true)
    , recoveringFromLostOpenReplicas(false)
    , listEntries()
    , replicas(numReplicas)
{
    openLen = segment->getAppendedLength(openingWriteFooterEntry);
    queued.bytes = openLen;
    queuedFooterEntry = openingWriteFooterEntry;
    schedule(); // schedule to replicate the opening data
}

ReplicatedSegment::~ReplicatedSegment()
{
}

/**
 * Request the eventual freeing all known replicas of a segment from its
 * backups. The caller's ReplicatedSegment pointer is invalidated upon the
 * return of this function. After the return of this call all outstanding
 * write rpcs for this segment are guaranteed to have completed so the log
 * memory associated with this segment is free for reuse. This implies that
 * this call can spin waiting for write rpcs, though, it tries to be
 * friendly to concurrent operations by releasing and reacquiring the
 * internal ReplicaManager lock each time it checks rpcs for completion.
 * Eventually canceling any outstanding write rpc would be better, but
 * doing so is unsafe in this case (RAM-359).
 *
 * Currently, there is no public interface to ensure enqueued free
 * operations have completed.
 */
void
ReplicatedSegment::free()
{
    TEST_LOG("%s, %lu", masterId.toString().c_str(), segmentId);

    // The order is important and rather subtle here:
    // First, ensure that any outstanding work scheduled on this segment is
    // completed.
    // Next, mark the segment as queued for freeing.
    // Then make sure not to return to the caller before any outstanding
    // write request has finished.
    // If the segment isn't marked free first then new write requests for
    // other replicas may get started as we wait to reap the outstanding
    // writeRpc.  This can cause the length of time the lock is held to
    // stretch out.

    Tub<Lock> lock;
    lock.construct(dataMutex);
    assert(queued.close);

    while (isScheduled())
        taskQueue.performTask();

    freeQueued = true;

    checkAgain:
    foreach (auto& replica, replicas) {
        if (!replica.isActive || !replica.writeRpc)
            continue;
        taskQueue.performTask();
        // Release and reacquire the lock; this gives other operations
        // a chance to slip in while this thread waits for all write
        // rpcs to finish up.
        lock.construct(dataMutex);
        goto checkAgain;
    }

    assert(!followingSegment);
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
    uint32_t appendedBytes = segment->getAppendedLength();
    if (queued.bytes != appendedBytes)
        return false;
    return !recoveringFromLostOpenReplicas && (getCommitted() == queued);
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

    // immutable after close
    assert(!queued.close);
    queued.close = true;
    // It is necessary to update queued.bytes here because the segment believes
    // it has fully replicated all data when queued.close and
    // getCommitted().bytes == queued.bytes.
    Segment::OpaqueFooterEntry footerEntry;
    uint32_t appendedBytes = segment->getAppendedLength(footerEntry);
    queued.bytes = appendedBytes;
    queuedFooterEntry = footerEntry;
    schedule();

    LOG(DEBUG, "Segment %lu closed (length %d)", segmentId, queued.bytes);
    ++metrics->master.segmentCloseCount;
}

/**
 * Respond to a change in cluster configuration by scheduling any work that is
 * needed to restore durability guarantees; please read the documentation
 * for the return value carefully as it requires action on the part of the
 * caller to ensure correctness of the log.
 *
 * \param failedId
 *      ServerId of the backup which has failed.
 * \return
 *      True indicates that this segment was not a cleaner generated segment,
 *      that it lost replicas, and that it will take corrective actions on
 *      future iterations of the ReplicaManager loop.  These corrective actions
 *      rely on closing the segment if it was still open.  Therefore, the
 *      caller must take appropriate action to ensure that if the only current
 *      and recoverable copy of the digest was stored in this segment that it
 *      creates a new, current, and recoverable digest.  Checking to see if
 *      this segment is the current head from the Log's perspective, and, if
 *      so, rolling over to a new log head solves both constraints; it closes
 *      this segment and ensures a current log digest exists in another
 *      segment.  Not rolling over to a new log head may result in an infinite
 *      loop not just during normal operation, but even during the destruction
 *      of the MasterService.  This is because the server makes every effort to
 *      ensure that all writes are durable/recoverable but it cannot make that
 *      guarantee for pending writes unless failures are handled.
 *      False indicates this segment cannot be the home of the current log
 *      digest, in which case no action needs to be taken by the caller aside
 *      from driving the ReplicaManager proceed loop to drive re-replication.
 */
bool
ReplicatedSegment::handleBackupFailure(ServerId failedId)
{
    auto alreadyRecoveringFromLostOpenReplicas = recoveringFromLostOpenReplicas;
    foreach (auto& replica, replicas) {
        if (!replica.isActive || replica.backupId != failedId)
            continue;
        LOG(DEBUG, "Segment %lu recovering from lost replica which was on "
            "backup %s", segmentId, failedId.toString().c_str());
        ++metrics->master.replicaRecoveries;

        // If the segment contains a digest, isn't durably acked, and
        // could appear open during recovery (that is, it isn't being
        // replicated with the atomic replication protocol) then we have
        // a problem we have to patch up.
        if (normalLogSegment &&
            !getCommitted().close &&
            !replica.replicateAtomically &&
            !recoveringFromLostOpenReplicas) {
            recoveringFromLostOpenReplicas = true;
            LOG(DEBUG, "Lost replica(s) for segment %lu while open due to "
                "crash of backup %s", segmentId, failedId.toString().c_str());
            ++metrics->master.openReplicaRecoveries;
        }

        replica.failed();
        schedule();
    }

    // Only need higher-level code to roll over to a new log head once.
    // If more replicas fail for the same segment while recovering
    // from lost open replicas don't rollover again.
    return !alreadyRecoveringFromLostOpenReplicas &&
           recoveringFromLostOpenReplicas;
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
 *    segment->getAppendedLength(). Repeated calls could "push out" the
 *    offset of the next footer to be sent to the backups. Then, since there is
 *    a limit the size of write rpcs, it is possible that several back-to-back
 *    rpcs wouldn't include a footer. Some sync()s could have to wait for
 *    several round trips while they wait for the next footer to be sent out
 *    before they are considered committed and safe for acknowledgement to
 *    clients.
 *
 * \param offset
 *      The number of bytes of the segment that must be replicated before the
 *      call will return. If offset is not provided then the call will only
 *      return when all enqueued data has been synced including the
 *      closed flag.
 */
void
ReplicatedSegment::sync(uint32_t offset)
{
    CycleCounter<RawMetric> _(&metrics->master.replicaManagerTicks);
    TEST_LOG("syncing");

    Lock syncLock(syncMutex);
    Tub<Lock> lock;
    lock.construct(dataMutex);

    // Definition of synced changes if this segment isn't durably closed
    // and is recovering from a lost replica.  In that case the data
    // the data isn't durable until it has been replicated *along with*
    // a durable close on the replicas as well *and* any lost, open
    // replicas have been shot down by setting the minOpenSegmentId.
    // Once this flag is cleared those conditions have been met and
    // it is safe to use the usual definition.
    if (!recoveringFromLostOpenReplicas) {
        if (offset == ~0u) {
            if (getCommitted().close)
                return;
        } else {
            if (getCommitted().bytes >= offset)
                return;
        }
    }

    Segment::OpaqueFooterEntry footerEntry;
    uint32_t appendedBytes = segment->getAppendedLength(footerEntry);
    if (appendedBytes > queued.bytes) {
        queued.bytes = appendedBytes;
        queuedFooterEntry = footerEntry;
        schedule();
    }

    uint64_t syncStartTicks = Cycles::rdtsc();
    while (true) {
        taskQueue.performTask();
        if (!recoveringFromLostOpenReplicas) {
            if (offset == ~0u) {
                if (getCommitted().close)
                    return;
            } else {
                if (getCommitted().bytes >= offset)
                    return;
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
    } else {
        foreach (auto& replica, replicas)
            performWrite(replica);
        // Have to be a bit careful: these steps must be completed even if a
        // free has been enqueued, otherwise lost open replicas could still
        // be detected as the head of the log during a recovery.  Hence the
        // extra condition above.
        if (recoveringFromLostOpenReplicas) {
            if (getCommitted().close) {
                if (minOpenSegmentId.isGreaterThan(segmentId)) {
                    // Ok, this segment is now recovered.
                    LOG(DEBUG,
                        "minOpenSegmentId ok, lost open replica recovery "
                        "complete on segment %lu", segmentId);
                    recoveringFromLostOpenReplicas = false;
                    // All done, don't reschedule.
                } else  {
                    // Now that the old head segment has been re-replicated
                    // and durably closed its time to make sure it can never
                    // appear as an open segment in the log again (even if
                    // some lost replica comes back from the grave).
                    LOG(DEBUG, "Updating minOpenSegmentId on coordinator to "
                             "ensure lost replicas of segment %lu will not be "
                             "reused", segmentId);
                    minOpenSegmentId.updateToAtLeast(segmentId + 1);
                    schedule();
                }
            } else {
                // This shouldn't be needed, but if the code handling the roll
                // over to the new log head doesn't close this segment before
                // the next call to proceed it's possible no work will be
                // possible or performWrite() won't schedule it, but it also
                // isn't isSycned() since it is recovering which will trip the
                // useful assertion below.
                schedule();
            }
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
            replica.reset();
            // Free completed, no need to reschedule.
            return;
        } else {
            // Request is not yet finished, stay scheduled to wait on it.
            schedule();
            return;
        }
    } else {
        // No free rpc is outstanding.
        if (replica.writeRpc) {
            // Cannot issue free, a write is outstanding. Make progress on it.
            performWrite(replica);
            // Stay scheduled even if synced since we have to do free still.
            schedule();
            return;
        } else {
            // Issue a free rpc for this replica, reschedule to wait on it.
            replica.freeRpc.construct(context, replica.backupId,
                                      masterId, segmentId);
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
        ServerId conflicts[replicas.numElements];
        uint32_t numConflicts = 0;
        // A master conflicts with its local backup.
        conflicts[numConflicts++] = masterId;
        foreach (auto& conflictingReplica, replicas) {
            if (conflictingReplica.isActive)
                conflicts[numConflicts++] = conflictingReplica.backupId;
            assert(numConflicts <= replicas.numElements);
        }
        ServerId backupId;
        if (replicaIsPrimary(replica)) {
            backupId = backupSelector.selectPrimary(numConflicts, conflicts);
        } else {
            backupId = backupSelector.selectSecondary(numConflicts, conflicts);
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
                replica.acked = replica.sent;
                if (replica.acked == queued || replica.acked.bytes == openLen) {
                    // committed advances whenever a footer was sent.
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
                LOG(NOTICE,
                    "Couldn't open replica on backup %s; server may be "
                    "overloaded or may already have a replica for this segment "
                    "which was found on disk after a crash; will choose "
                    "another backup", replica.backupId.toString().c_str());
                replica.reset();
            }
            replica.writeRpc.destroy();
            --writeRpcsInFlight;
            if (replica.committed != queued)
                schedule();
            return;
        } else {
            // Request is not yet finished, stay scheduled to wait on it.
            schedule();
            return;
        }
    } else {
        if (!replica.committed.open) {
            if (!precedingSegmentOpenCommitted) {
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

            WireFormat::BackupWrite::Flags flags =
                WireFormat::BackupWrite::OPEN;
            if (replicaIsPrimary(replica))
                flags = WireFormat::BackupWrite::OPENPRIMARY;

            TEST_LOG("Sending open to backup %s",
                     replica.backupId.toString().c_str());
            replica.writeRpc.construct(context, replica.backupId,
                                       masterId, segmentId, segment, 0,
                                       openLen, &openingWriteFooterEntry,
                                       flags, replica.replicateAtomically);
            ++writeRpcsInFlight;
            replica.sent.open = true;
            replica.sent.bytes = openLen;
            schedule();
            return;
        }

        // No outstanding write but not yet synced.
        if (replica.sent < queued) {
            // Some part of the data hasn't been sent yet.  Send it.
            if (!precedingSegmentCloseCommitted) {
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
            Segment::OpaqueFooterEntry* footerEntryToSend = &queuedFooterEntry;

            bool sendClose = queued.close && (offset + length) == queued.bytes;
            WireFormat::BackupWrite::Flags flags =
                sendClose ?  WireFormat::BackupWrite::CLOSE :
                             WireFormat::BackupWrite::NONE;

            // Breaks atomicity of log entries, but it could happen anyway
            // if a segment gets partially written to disk.
            if (length > maxBytesPerWriteRpc) {
                length = maxBytesPerWriteRpc;
                flags = WireFormat::BackupWrite::NONE;
                footerEntryToSend = NULL;
            }

            if (flags == WireFormat::BackupWrite::CLOSE &&
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
                                       segmentId, segment, offset, length,
                                       footerEntryToSend,
                                       flags, replica.replicateAtomically);
            ++writeRpcsInFlight;
            replica.sent.bytes += length;
            replica.sent.close = (flags == WireFormat::BackupWrite::CLOSE);
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
        info.append(format(
            "  Replica %u on Backup %s\n"
            "    sent: open %u, bytes %u, close %u\n"
            "    acked: open %u, bytes %u, close %u\n"
            "    committed: open %u, bytes, %u, close %u\n"
            "    write rpc outstanding: %u\n",
            i++, replica.backupId.toString().c_str(),
            replica.sent.open, replica.sent.bytes, replica.sent.close,
            replica.acked.open, replica.acked.bytes, replica.acked.close,
            replica.committed.open, replica.committed.bytes,
            replica.committed.close,
            bool(replica.writeRpc)));
    }
    LOG(DEBUG, "\n%s", info.c_str());
}

} // namespace RAMCloud
