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

    BackupReplica(BackupStorage& storage,
                ServerId masterId, uint64_t segmentId,
                uint32_t segmentSize, bool primary);
// XXX
#if 0
    BackupReplica(BackupStorage& storage,
                ServerId masterId, uint64_t segmentId,
                uint32_t segmentSize, uint32_t segmentFrame, bool isClosed);
#endif
    ~BackupReplica();
    Status appendRecoverySegment(uint64_t partitionId, Buffer* buffer,
                                 Segment::Certificate* certificate);
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

    void open(bool sync);
    bool setRecovering();
    bool setRecovering(const ProtoBuf::Tablets& partitions);
    void startLoading();
    void append(Buffer& source,
                size_t sourceOffset,
                size_t length,
                size_t destinationOffset,
                const void* metadata,
                size_t metadataLength);
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
    /// Return true if this segment's recovery segments have been built.
    bool isRecovered() const { return recoverySegments; }

    /**
     * Provides mutal exclusion between all public method calls and
     * storage operations that can be performed on BackupReplicas.
     */
    std::mutex mutex;

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

    /// The size in bytes that make up this segment.
    const uint32_t segmentSize;

    /// The state of this segment.  See State.
    State state;

    /**
     * Handle to provide to the storage layer to access this segment.
     *
     * Allocated while OPEN, CLOSED.
     */
    BackupStorage::Frame* frame;

    /// For allocating, loading, storing segments to.
    BackupStorage& storage;

    DISALLOW_COPY_AND_ASSIGN(BackupReplica);
};

} // namespace RAMCloud

#endif
