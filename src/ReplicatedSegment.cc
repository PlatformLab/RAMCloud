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

ReplicatedSegment::ReplicatedSegment(BackupManager& backupManager,
                                     TaskManager& taskManager,
                                     uint64_t segmentId,
                                     const void* data,
                                     uint32_t len,
                                     uint32_t numReplicas)
    : Task(taskManager)
    , listEntries()
    , backupManager(backupManager)
    , segmentId(segmentId)
    , data(data)
    , openLen(len)
    , queued(true, len, false)
    , freeQueued(false)
    , replicas(numReplicas)
{
}

ReplicatedSegment::~ReplicatedSegment()
{
}

// Return whether this replica has been freed.
void
ReplicatedSegment::performFree(Tub<Replica>& replica)
{
    if (!replica) {
        return;
    } else if (replica->freeRpc && replica->freeRpc->isReady()) {
        try {
            (*replica->freeRpc)();
            //LOG(ERROR, "Backup free completed");
            replica.destroy();
        } catch (ClientException& e) {
            // TODO: What do we want to do here?
            //  a) move forward, lose storage
            //  b) wedge?
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
        replica->freeRpc.construct(replica->client,
                                   *backupManager.masterId, segmentId);
        schedule();
        return;
    }
}

// Return whether all outstanding work for this replica is completed.
void
ReplicatedSegment::performWrite(Tub<Replica>& replica)
{
    if (!replica) {
        // TODO: document the problem each if case is solving
        // Replica state can be reset if it was lost due to a failure.
        // Select a new backup for that replica and start over.
        BackupSelector::Backup* backup;
        // TODO: add the conflict arrays
        // maybe push this stuff down into the constructOpen
        if (replicaIsPrimary(replica))
            backup = backupManager.backupSelector.selectPrimary(0, NULL);
        else
            backup = backupManager.backupSelector.selectSecondary(0, NULL);
        const string& serviceLocator = backup->service_locator();
        Transport::SessionRef session =
            Context::get().transportManager->getSession(serviceLocator.c_str());
        replica.construct(session);
        BackupWriteRpc::Flags flags = BackupWriteRpc::OPEN;
        if (replicaIsPrimary(replica))
            flags = BackupWriteRpc::OPENPRIMARY;
        //LOG(ERROR, "Sending backup opening write");
        replica->writeRpc.construct(replica->client,
                                    *backupManager.masterId, segmentId,
                                    0, data, openLen, flags);
        replica->sent.open = true;
        replica->sent.bytes = openLen;
        schedule();
        return;
    } else if (replica->writeRpc && replica->writeRpc->isReady()) {
        // TODO prefer early exits
        try {
            (*replica->writeRpc)();
            replica->done = replica->sent;
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
        return;
    } else if (!replica->writeRpc && replica->sent < queued) {
        // This backup doesn't have an outstanding task and it
        // has some data which hasn't been sent to the backup.
        assert(!replica->freeRpc);
        assert(!replica->sent.close);

        uint32_t offset = replica->sent.bytes;
        uint32_t length = queued.bytes - replica->sent.bytes;
        BackupWriteRpc::Flags flags = queued.close ?
                                        BackupWriteRpc::CLOSE :
                                        BackupWriteRpc::NONE;

        if (length > BackupManager::MAX_BYTES_PER_WRITE_RPC) {
            length = BackupManager::MAX_BYTES_PER_WRITE_RPC;
            flags = BackupWriteRpc::NONE;
        }

        //LOG(ERROR, "Sending backup write");
        replica->writeRpc.construct(replica->client,
                                    *backupManager.masterId, segmentId,
                                    offset, data, length, flags);
        replica->sent.bytes += length;
        replica->sent.close = (flags == BackupWriteRpc::CLOSE);

        schedule();
        return;
    }
}

void
ReplicatedSegment::performTask()
{
    if (freeQueued) {
        foreach (auto& replica, replicas)
            performFree(replica);
        if (!isScheduled()) // Everything is freed, destroy ourself.
            backupManager.forgetReplicatedSegment(this);
    } else {
        foreach (auto& replica, replicas)
            performWrite(replica);
    }
}

void
ReplicatedSegment::free()
{
    freeQueued = true;
    schedule();
}

/// See OpenSegment::write.
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

} // namespace RAMCloud
