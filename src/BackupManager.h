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
#include "RawMetrics.h"
#include "ReplicatedSegment.h"
#include "TaskManager.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * Manages durable replicas of local in-memory log segments on remote backups.
 * The master's log module issues requests to the BackupManager to have log
 * data replicated and to free all known replicas of a segment in the cluster.
 * BackupManager responds to changes in cluster configuration; it restores
 * durablity of segments and transparently masks backup failures(both for
 * closed segments and for segments which are actively being written when a
 * backup fails).
 */
class BackupManager : public ReplicatedSegment::Deleter {
  PUBLIC:
    BackupManager(CoordinatorClient* coordinator,
                  const Tub<uint64_t>& masterId,
                  uint32_t numReplicas);
    explicit BackupManager(BackupManager* prototype);
    ~BackupManager();

    void freeSegment(uint64_t segmentId);
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

    /// Id of master that this will be managing replicas for.
    const Tub<uint64_t>& masterId;

    // TODO: Remove this once the alt constructor has been eliminated.
    /// Cluster coordinator. May be NULL for testing purposes.
    CoordinatorClient* const coordinator;

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
