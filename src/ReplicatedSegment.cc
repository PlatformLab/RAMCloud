/* Copyright (c) 2011 Stanford University
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

#include "ReplicatedSegment.h"
#include "BackupManager.h"
#include "ShortMacros.h"
#include "TaskManager.h"

namespace RAMCloud {

// --- ReplicatedSegment ---

/**
 * Create a ReplicatedSegment.  Only called by BackupManager.
 *
 * \param taskManager
 *      The BackupManager's work queue, this is added to it when schedule()
 *      is called.
 * \param backupSelector
 *      Used to choose where to store replicas. Shared among ReplicatedSegments.
 * \param deleter
 *      Deletes this when this determines it is no longer needed.
 * \param masterId
 *      The server id of the Master whose log this segment belongs to.
 * \param segmentId
 *      ID for the segment, must match the segmentId given by the log module.
 * \param data
 *      The start of raw bytes of the in-memory log segment to be replicated.
 * \param openLen
 *      Bytes to send atomically to backups with the open segment RPC.
 * \param numReplicas
 *      Number of replicas of this segment that must be maintained.
 * \param maxBytesPerWriteRpc
 *      Maximum bytes to send in a single write RPC; can help latency of
 *      GetRecoveryDataRequests by unclogging backups a bit.
 */
ReplicatedSegment::ReplicatedSegment(TaskManager& taskManager,
                                     BaseBackupSelector& backupSelector,
                                     Deleter& deleter,
                                     uint64_t masterId,
                                     uint64_t segmentId,
                                     const void* data,
                                     uint32_t openLen,
                                     uint32_t numReplicas,
                                     uint32_t maxBytesPerWriteRpc)
    : Task(taskManager)
    , backupSelector(backupSelector)
    , masterId(masterId)
    , segmentId(segmentId)
    , data(data)
    , openLen(openLen)
    , maxBytesPerWriteRpc(maxBytesPerWriteRpc)
    , queued(true, openLen, false)
    , freeQueued(false)
    , listEntries()
    , deleter(deleter)
    , replicas(numReplicas)
{
    schedule(); // schedule to replicate the opening data
}

ReplicatedSegment::~ReplicatedSegment()
{
}

/**
 * Eventually free all known replicas of a segment from its backups.
 * Requires that the segment has been closed (it does not require that
 * the close has been sent and acknowledged by the backups, though).
 * The caller's ReplicatedSegment pointer is invalidated upon the return
 * of this function.
 */
void
ReplicatedSegment::free()
{
    assert(queued.close);
    freeQueued = true;
    schedule();
}

/**
 * Eventually replicate bytes of data ending at \a offset non-inclusive on
 * a set backups for durability.  Guarantees that no replica will see this
 * write until it has seen all previous writes on this segment.
 *
 * \pre
 *      All previous segments have been closed (at least locally).
 * \param offset
 *      The number of bytes into the segment to replicate.
 * \param closeSegment
 *      Whether to close the segment after writing this data.  If this is
 *      true no subsequent writes to this segment are permitted (free is
 *      the only valid operation at that point).
 */
void
ReplicatedSegment::write(uint32_t offset,
                         bool closeSegment)
{
    // offset monotonically increases
    assert(offset >= queued.bytes);
    queued.bytes = offset;

    // immutable after close
    assert(!queued.close);
    queued.close = closeSegment;
    if (queued.close) {
        LOG(DEBUG, "Segment %lu closed (length %d)", segmentId, queued.bytes);
        ++metrics->master.segmentCloseCount;
    }

    schedule();
}

// - private -

/**
 * Check replication state and make progress in restoring invariants; never
 * invoke this directly, instead use schedule().
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
    if (freeQueued) {
        foreach (auto& replica, replicas)
            performFree(replica);
        if (!isScheduled()) // Everything is freed, destroy ourself.
            deleter.destroyAndFreeReplicatedSegment(this);
    } else {
        foreach (auto& replica, replicas)
            performWrite(replica);
        assert(isSynced() || isScheduled());
    }
}

/**
 * Make progress, if possible, in freeing a known replica of a segment
 * regardless of what state the replica is in (both locally and remotely).
 * If this future work is required this method automatically re-schedules this
 * segment for future attention from the BackupManager.
 * \pre freeQueued must be true, otherwise behavior is undefined.
 */
void
ReplicatedSegment::performFree(Tub<Replica>& replica)
{
    if (!replica) {
        // Do nothing is there was no replica.
        return;
    } else if (replica->freeRpc && replica->freeRpc->isReady()) {
        // Wait for the replica's outstanding free to complete.
        try {
            (*replica->freeRpc)();
            replica.destroy();
        } catch (ClientException& e) {
            // TODO: When do I get an Exception?  Only when the backup
            // is permanently out of the cluster?  If so, just keep
            // retrying until the coordinator kills one of the two of us.
            // TODO: Need a better log message once
            // correct host details are in the Replica.
            LOG(WARNING,
                "Failure while freeing replica on backup: %s",
                e.what());
            DIE("TODO: Haven't decided what to do when a free to "
                "a backup fails");
        }
        return;
    } else if (!replica->writeRpc) {
        // Issue a free RPC for this replica.
        replica->freeRpc.construct(replica->client, masterId, segmentId);
        schedule();
        return;
    } else {
        // Couldn't issue a free because a write is outstanding. Wait on it.
        performWrite(replica);
        // Stay scheduled even if synced since we have to do free still.
        schedule();
        return;
    }
}

/**
 * Make progress, if possible, in durably writing segment data to a particular
 * replica.
 * If this future work is required this method automatically re-schedules this
 * segment for future attention from the BackupManager.
 * \pre freeQueued must be false, otherwise behavior is undefined.
 */
void
ReplicatedSegment::performWrite(Tub<Replica>& replica)
{
    // TODO: make ::close take pointer to the next log segment
    if (!replica /* TODO && nextSegment->getAcked().open */) {
        // This replica does not exist yet. Choose a backup and send the open.
        // Happens for a new segment or if a replica was known to be lost.
        BackupSelector::Backup* backup;
        // TODO: add the conflict arrays
        // maybe push this stuff down into the constructOpen
        BackupWriteRpc::Flags flags = BackupWriteRpc::OPEN;
        if (replicaIsPrimary(replica)) {
            backup = backupSelector.selectPrimary(0, NULL);
            flags = BackupWriteRpc::OPENPRIMARY;
        } else {
            backup = backupSelector.selectSecondary(0, NULL);
        }
        const string& serviceLocator = backup->service_locator();
        Transport::SessionRef session =
            Context::get().transportManager->getSession(serviceLocator.c_str());
        replica.construct(session);
        //LOG(ERROR, "Sending backup opening write: seg %lu off %u len %u flags %u",
        //    segmentId, 0, openLen, flags);
        replica->writeRpc.construct(replica->client, masterId, segmentId,
                                    0, data, openLen, flags);
        replica->sent.open = true;
        replica->sent.bytes = openLen;
    } else if (replica->writeRpc && replica->writeRpc->isReady()) {
        // This backup has a write request outstanding to a backup.
        // Wait for it to complete if it is ready.
        try {
            (*replica->writeRpc)();
            replica->acked = replica->sent;
            //LOG(ERROR, "Backup write completed");
            replica->writeRpc.destroy();
        } catch (ClientException& e) {
            // TODO: What do we want to do here?
            LOG(WARNING,
                "Failure while writing replica on backup: %s",
                e.what());
            DIE("TODO: Haven't decided what to do when a write to "
                "a backup fails");
        }
        //LOG(ERROR, "Backup write completed");
    } else if (replica->sent < queued) {
        // More data needs to be sent to the backup for this replica.
        // Send out a write RPC if one isn't already outstanding.
        schedule();
        if (replica->writeRpc)
            goto done;

        assert(!replica->freeRpc);
        assert(!replica->sent.close);

        uint32_t offset = replica->sent.bytes;
        uint32_t length = queued.bytes - replica->sent.bytes;
        BackupWriteRpc::Flags flags = queued.close ?
                                        BackupWriteRpc::CLOSE :
                                        BackupWriteRpc::NONE;

        if (length > maxBytesPerWriteRpc) {
            length = maxBytesPerWriteRpc;
            flags = BackupWriteRpc::NONE;
        }

        //LOG(ERROR, "Sending backup write: seg %lu off %u len %u flags %u",
        //    segmentId, offset, length, flags);
        replica->writeRpc.construct(replica->client, masterId, segmentId,
                                    offset, data, length, flags);
        replica->sent.bytes += length;
        replica->sent.close = (flags == BackupWriteRpc::CLOSE);
        return;
    }

  done:
    if (!isSynced())
        schedule();
}

} // namespace RAMCloud
