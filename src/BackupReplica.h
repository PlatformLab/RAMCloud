/* Copyright (c) 2009-2012 Stanford University
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

#ifndef RAMCLOUD_BACKUPREPLICA_H
#define RAMCLOUD_BACKUPREPLICA_H

// TODO(stutsman): Drop the dependency on BackupService once the details
// of the IoScheduler have been moved down into BackupStorage.
#include "BackupService.h"

#include "BackupStorage.h"
#include "Log.h"

namespace RAMCloud {

class SegmentIterator;

#if TESTING
bool isEntryAlive(Log::Position& position,
                  const SegmentIterator& it,
                  Buffer& buffer,
                  const ProtoBuf::Tablets::Tablet& tablet,
                  SegmentHeader& header);
Tub<uint64_t> whichPartition(Log::Position& position,
                             const SegmentIterator& it,
                             Buffer& buffer,
                             const ProtoBuf::Tablets& partitions,
                             SegmentHeader& header);
#endif

/**
 * Only used internally by BackupService; tracks the details of a replica for
 * a segment. Manages all resources and storage related to the replica.
 * Public calls are protected by #mutex to provide thread safety.
 */
class BackupReplica {
  typedef BackupService::ThreadSafePool ThreadSafePool;
  typedef BackupService::IoScheduler IoScheduler;
  public:
    /// The type of locks used to lock #mutex.
    typedef std::unique_lock<std::mutex> Lock;

    /**
     * Tracks current state of the segment which is sufficient to
     * determine which operations are legal.
     */
    enum State {
        UNINIT,     ///< open() and delete are the only valid ops.
        /**
         * Storage is reserved but segment is mutable.
         * This segment will be closed if this is deleted.
         */
        OPEN,
        CLOSED,     ///< Immutable and has moved to stable store.
        RECOVERING, ///< Rec segs building, all other ops must wait.
        FREED,      ///< delete is the only valid op.
    };

    BackupReplica(BackupStorage& storage, ThreadSafePool& pool,
                IoScheduler& ioScheduler,
                ServerId masterId, uint64_t segmentId,
                uint32_t segmentSize, bool primary);
    BackupReplica(BackupStorage& storage, ThreadSafePool& pool,
                IoScheduler& ioScheduler,
                ServerId masterId, uint64_t segmentId,
                uint32_t segmentSize, uint32_t segmentFrame, bool isClosed);
    ~BackupReplica();
    Status appendRecoverySegment(uint64_t partitionId, Buffer& buffer)
        __attribute__((warn_unused_result));
    void buildRecoverySegments(const ProtoBuf::Tablets& partitions);
    void close();
    void free();

    /// See #rightmostWrittenOffset.
    uint32_t
    getRightmostWrittenOffset()
    {
        Lock lock(mutex);
        return rightmostWrittenOffset;
    }

    /**
     * Return true if this replica can be used for recovery.  Some
     * replicas are open for atomic replication.  Those segments
     * may be open and have data written to them, but they cannot
     * be used for recovery because they aren't considered to be
     * valid replicas until the closing write is processed.
     */
    bool
    satisfiesAtomicReplicationGuarantees()
    {
        Lock lock(mutex);
        return !replicateAtomically || state == CLOSED;
    }

    /**
     * Return true if this replica is open. Notice, this isn't the same
     * as state == OPEN. A replica may be considered open even if its
     * state is RECOVERING, for example.  This nastiness is a good
     * indicator that BackupReplica needs a complete rewrite (as well
     * as a rename, and relocation to a new file).
     */
    bool
    isOpen()
    {
        Lock lock(mutex);
        return rightmostWrittenOffset != BYTES_WRITTEN_CLOSED;
    }

    void open();
    bool setRecovering();
    bool setRecovering(const ProtoBuf::Tablets& partitions);
    void startLoading();
    void write(Buffer& src, uint32_t srcOffset,
               uint32_t length, uint32_t destOffset,
               const Segment::OpaqueFooterEntry* footerEntry,
               bool atomic);
    bool getLogDigest(Buffer* digestBuffer);

    /// The id of the master from which this segment came.
    const ServerId masterId;

    /**
     * True if this is the primary copy of this segment for the master
     * who stored it.  Determines whether recovery segments are built
     * at recovery start or on demand.
     */
    const bool primary;

    /// The segment id given to this segment by the master who sent it.
    const uint64_t segmentId;

#if TESTING
    bool createdByCurrentProcess;
#else
    const bool createdByCurrentProcess;
#endif

  PRIVATE:
    void applyFooterEntry();

    /// Return true if this segment is fully in memory.
    bool inMemory() { return segment; }

    /// Return true if this segment has storage allocated.
    bool inStorage() const { return storageHandle; }

    /// Return true if this segment's recovery segments have been built.
    bool isRecovered() const { return recoverySegments; }

    /**
     * Wait for any LoadOps or StoreOps to complete.
     * The caller must be holding a lock on #mutex.
     */
    void
    waitForOngoingOps(Lock& lock)
    {
#ifndef SINGLE_THREADED_BACKUP
        int lastThreadCount = 0;
        while (storageOpCount > 0) {
            if (storageOpCount != lastThreadCount) {
                RAMCLOUD_LOG(DEBUG,
                             "Waiting for storage threads to terminate "
                             "for a segment, %d threads still running",
                             static_cast<int>(storageOpCount));
                lastThreadCount = storageOpCount;
            }
            condition.wait(lock);
        }
#endif
    }

    /**
     * Provides mutal exclusion between all public method calls and
     * storage operations that can be performed on BackupReplicas.
     */
    std::mutex mutex;

    /**
     * Notified when a store or load for this segment completes, or
     * when this segment's recovery segments are constructed and valid.
     * Used in conjunction with #mutex.
     */
    std::condition_variable condition;

    /// Gatekeeper through which async IOs are scheduled.
    IoScheduler& ioScheduler;

    /// An array of recovery segments when non-null.
    /// The exception if one occurred while recovering a segment.
    std::unique_ptr<SegmentRecoveryFailedException> recoveryException;

    /**
     * Only used if this segment is recovering but the filtering is
     * deferred (i.e. this isn't the primary segment backup copy).
     */
    Tub<ProtoBuf::Tablets> recoveryPartitions;

    /// An array of recovery segments when non-null.
    Segment* recoverySegments;

    /// The number of Segments in #recoverySegments.
    uint32_t recoverySegmentsLength;

    /**
     * Where in #segment the most-recently-queued footer should be
     * plunked down when the segment is closed or recovered (see
     * #footerEntry).
     */
    uint32_t footerOffset;

    /**
     * Footer log entry provided on write by the master which is creating
     * the replica. The footer must be stamped on the end of the replicas
     * before the replica is sent to storage or used for recovery.  During
     * recovery this footer is used to detect corruption and so must be in
     * place for updates to replicas to be considered durable. If replica
     * manager cannot send all the data ready to be replicated in one rpc
     * it will not send a footer with the write. When this happens the
     * backup ensures the most recently transmitted footer is used if the
     * replica is closed or recovered in a way such that all non-footered
     * writes sent since the last footered write will be atomically undone.
     * This footer is stored in two locations in each replica: once at the
     * end of the data (which is the authoritative footer, and again
     * aligned at the end of the replica. The second copy is an
     * optimization which allows the backup to find the footer quickly from
     * disk in a single IO. applyFooterEntry() is a helper function that
     * will plunk the most-recently-queued footers into place.
     */
    Segment::OpaqueFooterEntry footerEntry;

    /**
     * Indicate to callers of startReadingData() that particular
     * segment's #rightmostWrittenOffset is not needed because it was
     * successfully closed.
     */
    enum { BYTES_WRITTEN_CLOSED = ~(0u) };

    /**
     * An approximation for written segment "length" for startReadingData
     * if this segment is still open, otherwise BYTES_WRITTEN_CLOSED.
     */
    uint32_t rightmostWrittenOffset;

    /**
     * The staging location for this segment in memory.
     *
     * Only valid when inMemory() (happens while OPEN or CLOSED).
     */
    char* segment;

    /// The size in bytes that make up this segment.
    const uint32_t segmentSize;

    /// The state of this segment.  See State.
    State state;

    /**
     * If this is true the data buffered for this segment is invalid until
     * a closing write has been processed.  It will not be written to disk
     * in any way and it will not be used or reported during recovery.
     */
    bool replicateAtomically;

    /**
     * Handle to provide to the storage layer to access this segment.
     *
     * Allocated while OPEN, CLOSED.
     */
    BackupStorage::Handle* storageHandle;

    /// To allocate memory for segments to be staged/recovered in.
    ThreadSafePool& pool;

    /// For allocating, loading, storing segments to.
    BackupStorage& storage;

    /// Count of loads/stores pending for this segment.
    int storageOpCount;

    friend class BackupService::IoScheduler;
    friend class BackupService::RecoverySegmentBuilder;
    DISALLOW_COPY_AND_ASSIGN(BackupReplica);
};

} // namespace RAMCloud

#endif
