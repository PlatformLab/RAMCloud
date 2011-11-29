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
 * Replicates segments to backup servers. This class is used on masters to
 * replicate segments of the log to backups. It handles selecting backup
 * servers on a segment-by-segment basis and replicates local segments to
 * remote backups.
 */
class BackupManager : public ReplicatedSegment::Deleter {
  PUBLIC:
    BackupManager(CoordinatorClient* coordinator,
                  const Tub<uint64_t>& masterId,
                  uint32_t numReplicas);
    explicit BackupManager(BackupManager* prototype);
    ~BackupManager();

    void freeSegment(uint64_t segmentId);
    OpenSegment* openSegment(uint64_t segmentId,
                             const void* data, uint32_t len);
        __attribute__((warn_unused_result));
    void proceed();
    void sync();
    void dumpReplicatedSegments(); // defined for testing only

    // TODO: The public stuff below is for ReplicatedSegment
    // I'd really like to hide this from the higher level interface.

    void destroyAndFreeReplicatedSegment(ReplicatedSegment* replicatedSegment);

    /// Number replicas to keep of each segment.
    const uint32_t numReplicas;

  PRIVATE:
    void scheduleWorkIfNeeded(); // TODO configurationChanged?
    bool isSynced();
    void proceedNoMetrics();

    BackupSelector backupSelector; ///< See #BackupSelector.

    /**
     * The coordinator-assigned server ID for this master or, equivalently, its
     * globally unique #Log ID.
     */
    const Tub<uint64_t>& masterId;

    /// Cluster coordinator. May be NULL for testing purposes.
    CoordinatorClient* const coordinator;

    /// A pool from which all ReplicatedSegment objects are allocated.
    boost::pool<> replicatedSegmentPool;

    INTRUSIVE_LIST_TYPEDEF(ReplicatedSegment, listEntries)
        ReplicatedSegmentList;

    /**
     * A FIFO queue of all existing ReplicatedSegment objects.
     * Newly opened segments are pushed to the back of this list.
     */
    ReplicatedSegmentList replicatedSegmentList;

    TaskManager taskManager;

    /// The number of RPCs that have been issued to backups but have not yet
    /// completed.
    int outstandingRpcs;

    /// Used to count the amount of time that outstandingRpcs > 0.
    Tub<CycleCounter<RawMetric>> activeTime;

    DISALLOW_COPY_AND_ASSIGN(BackupManager);
};

} // namespace RAMCloud

#endif
