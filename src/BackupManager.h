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

#ifndef RAMCLOUD_BACKUPMANAGER_H
#define RAMCLOUD_BACKUPMANAGER_H

#include <unordered_map>
#include <boost/pool/pool.hpp>

#include "Common.h"
#include "BoostIntrusive.h"
#include "BackupSelector.h"
#include "ReplicatedSegment.h"
#include "TaskManager.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * Manages durable replicas of local in-memory log segments on remote backups.
 *
 * Executive summary: A master's log module issues requests to its
 * BackupManager which replicates data and can be used to free replicas of a
 * segment from backups in the cluster.  BackupManager responds to changes in
 * cluster configuration; it restores durability of segments and transparently
 * masks backup failures (both for closed segments and for segments which are
 * actively being written when a backup fails).
 *
 * Details: Initially, a master constructs a single BackupManager to replicate
 * segments from its in-memory log.  There must be exactly one BackupManager
 * per log otherwise behavior is undefined.  The log module can then use
 * #openSegment to make the BackupManager aware of an in-memory segment which
 * must be replicated.  All operations issued to the BackupManager are only
 * queued.  To force all queued operations to complete #sync() must be called,
 * otherwise the BackupManager casually tries to perform some replication
 * whenever #proceed() is called.  After a call to #sync() all queued
 * operations have been successfully "applied" to the required number of
 * replicas (though, keep in mind, host failures could have eliminated some
 * replicas even as sync returns).  "Applied" means all state for each replica
 * is at least buffered on some backups.
 *
 * See ReplicatedSegment::write() and ReplicatedSegment::free() for details on
 * how the log module informs the BackupManager of changes to the in-memory
 * segment image and what guarantees the BackupManager provides.
 *
 * BackupManager is intended to mask any failures that can occur in replication
 * (for example, naming, network, or host failures).  Eventually, BackupManager
 * will be made aware of cluster configuration changes using ServerTracker and
 * will automatically restore durablity of ReplicatedSegment which have lost
 * some of their replicas.
 */
class BackupManager : public ReplicatedSegment::Deleter {
   PUBLIC:
    BackupManager(CoordinatorClient* coordinator,
                  const Tub<uint64_t>& masterId, uint32_t numReplicas);
    explicit BackupManager(BackupManager* prototype);
    ~BackupManager();

    ReplicatedSegment* openSegment(uint64_t segmentId,
                                   const void* data, uint32_t len);
        __attribute__((warn_unused_result));
    void proceed();
    void sync();

    /// Number replicas to keep of each segment.
    const uint32_t numReplicas;

  PRIVATE:
    void clusterConfigurationChanged();
    bool isSynced();

    /// Selects backups to store replicas while obeying placement constraints.
    BackupSelector backupSelector;

    // TODO: Remove this once the alt constructor has been eliminated.
    /// Cluster coordinator. May be NULL for testing purposes.
    CoordinatorClient* const coordinator;

    /// Id of master that this will be managing replicas for.
    const Tub<uint64_t>& masterId;

    /// Allows fast reuse of ReplicatedSegment allocations.
    boost::pool<> replicatedSegmentPool;

    INTRUSIVE_LIST_TYPEDEF(ReplicatedSegment, listEntries)
        ReplicatedSegmentList;

    /**
     * A list all ReplicatedSegments (one for each segment in the log
     * which hasn't been freed). Newly opened segments are pushed to the back.
     */
    ReplicatedSegmentList replicatedSegmentList;

    /// Tracks segments that need replication/freeing and dispatches work.
    TaskManager taskManager;

    /**
     * Number of collective outstanding write RPCs to all backups.
     * Used by ReplicatedSegment to throttle RPC creation.
     */
    uint32_t writeRpcsInFlight;

  PUBLIC:
    // Only used by ReplicatedSegment.
    void destroyAndFreeReplicatedSegment(ReplicatedSegment* replicatedSegment);

    DISALLOW_COPY_AND_ASSIGN(BackupManager);
};

} // namespace RAMCloud

#endif
