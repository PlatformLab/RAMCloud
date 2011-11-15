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

// --- OpenSegment ---

/**
 * Convenience method for calling #write() with the most recently queued
 * offset and queueing the close flag.
 */
void
OpenSegment::close()
{
    segment.close();
}

/**
 * Eventually replicate the \a len bytes of data starting at \a offset into the
 * segment.
 * Guarantees that no replica will see this write until it has seen all
 * previous writes on this segment.
 * \pre
 *      All previous segments have been closed (at least locally).
 * \param offset
 *      The number of bytes into the segment through which to replicate.
 * \param closeSegment
 *      Whether to close the segment after writing this data. If this is true,
 *      the caller's ReplicatedSegment pointer is invalidated upon the
 *      return of this function.
 */
void
OpenSegment::write(uint32_t offset, bool closeSegment)
{
    segment.write(offset, closeSegment);
}

// --- ReplicatedSegment ---

ReplicatedSegment::ReplicatedSegment(BackupManager& backupManager,
                                     uint64_t segmentId,
                                     const void* data,
                                     uint32_t len,
                                     uint32_t numReplicas)
    : listEntries()
    , openSegment(*this)
    , backupManager(backupManager)
    , segmentId(segmentId)
    , data(data)
    , openLen(len)
    , queued(true, len, false)
    , replicas(numReplicas)
{
}

ReplicatedSegment::~ReplicatedSegment()
{
}

void
ReplicatedSegment::performTask(TaskManager& taskManager)
{
    /*
    if (freeing) {
        if (outstanding rpcs) {
            taskManager.add;
            return;
        }
    }

    if (no replicas created yet) {
        select replicas;
        scheduleOpen();
        taskManager.add;
        return;
    }
    */

    // TODO(stutsman): if still in send mode
    foreach (auto& replica, replicas) {
        if (!replica) {
            // Replica state can be reset if it was lost due to a failure.
            // Select a new backup for that replica and start over.
            // TODO(stutsman): choose a random backup
            //replica.construct(...session here...);
            scheduleOpen(taskManager, *replica);
            continue;
        }
        if (replica->writeRpc || replica->freeRpc)
            continue;
        if (replica->sent < queued) {
            // This backup doesn't have an outstanding task and it
            // has some data which hasn't been sent to the backup.
            scheduleWrite(taskManager, *replica);
        }
    }
    // TODO(stutsman): else if in free mode then schedule a free
    taskManager.add(this);
}

void
ReplicatedSegment::scheduleOpen(TaskManager& taskManager,
                                Replica& replica)
{
    BackupWriteRpc::Flags flags = BackupWriteRpc::OPEN;
    if (replicaIsPrimary(replica))
        flags = BackupWriteRpc::OPENPRIMARY;
    replica.writeRpc.construct(replica.client,
                               *backupManager.masterId, segmentId,
                               0, data, openLen, flags);
}

void
ReplicatedSegment::scheduleWrite(TaskManager& taskManager,
                              Replica& replica)
{
    uint32_t offset = replica.sent.bytes;
    uint32_t length = queued.bytes - replica.sent.bytes;
    BackupWriteRpc::Flags flags = queued.close ?
                                    BackupWriteRpc::CLOSE :
                                    BackupWriteRpc::NONE;

    if (length > BackupManager::MAX_BYTES_PER_WRITE_RPC) {
        length = BackupManager::MAX_BYTES_PER_WRITE_RPC;
        flags = BackupWriteRpc::NONE;
    }

    replica.writeRpc.construct(replica.client,
                               *backupManager.masterId, segmentId,
                               offset, data, length, flags);
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
}

} // namespace RAMCloud
