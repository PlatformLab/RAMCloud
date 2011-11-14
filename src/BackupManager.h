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
#include "DurableSegment.h"
#include "RawMetrics.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * Replicates segments to backup servers. This class is used on masters to
 * replicate segments of the log to backups. It handles selecting backup
 * servers on a segment-by-segment basis and replicates local segments to
 * remote backups.
 */
class BackupManager {
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
    void sync();
    void proceed();
    void dumpDurableSegments(); // defined for testing only

    /// The number of backups to replicate each segment on.
    const uint32_t numReplicas;

  PRIVATE:
    void proceedNoMetrics();
    bool isSynced();
    void forgetDurableSegment(DurableSegment* durableSegment);

    /// Maximum number of bytes we'll send in any single write RPC
    /// to backups. The idea is to avoid starving other RPCs to the
    /// backup by not inundating it with segment-sized writes on
    /// recovery.
    enum { MAX_WRITE_RPC_BYTES = 1024 * 1024 };

    /// Cluster coordinator. May be NULL for testing purposes.
    CoordinatorClient* const coordinator;

    /**
     * The coordinator-assigned server ID for this master or, equivalently, its
     * globally unique #Log ID.
     */
    const Tub<uint64_t>& masterId;

    BackupSelector backupSelector; ///< See #BackupSelector.

  PRIVATE:
    typedef std::unordered_map<uint64_t, DurableSegment*>
            SegmentMap;
    /// Tells which backup each segment is stored on.
    SegmentMap segments;

    /// A pool from which all DurableSegment objects are allocated.
    boost::pool<> durableSegmentPool;

    INTRUSIVE_LIST_TYPEDEF(DurableSegment, listEntries) DurableSegmentList;

    /**
     * A FIFO queue of all existing DurableSegment objects.
     * Newly opened segments are pushed to the back of this list.
     */
    DurableSegmentList durableSegmentList;

    /// The number of RPCs that have been issued to backups but have not yet
    /// completed.
    int outstandingRpcs;

    /// Used to count the amount of time that outstandingRpcs > 0.
    Tub<CycleCounter<RawMetric>> activeTime;

    DISALLOW_COPY_AND_ASSIGN(BackupManager);
};

} // namespace RAMCloud

#endif
