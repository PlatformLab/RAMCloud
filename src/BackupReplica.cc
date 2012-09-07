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

#include <unordered_set>

#include "BackupReplica.h"
#include "Object.h"
#include "SegmentIterator.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Construct a BackupReplica to manage a segment.
 *
 * \param storage
 *      The storage which this segment will be backed by when closed.
 * \param masterId
 *      The master id of the segment being managed.
 * \param segmentId
 *      The segment id of the segment being managed.
 * \param segmentSize
 *      The number of bytes contained in this segment.
 * \param primary
 *      True if this is the primary copy of this segment for the master
 *      who stored it.  Determines whether recovery segments are built
 *      at recovery start or on demand.
 */
BackupReplica::BackupReplica(BackupStorage& storage,
                             ServerId masterId,
                             uint64_t segmentId,
                             uint32_t segmentSize,
                             bool primary)
    : masterId(masterId)
    , primary(primary)
    , segmentId(segmentId)
    , createdByCurrentProcess(true)
    , mutex()
    , recoveryException()
    , recoveryPartitions()
    , recoverySegments(NULL)
    , recoverySegmentsLength()
    , rightmostWrittenOffset(0)
    , segmentSize(segmentSize)
    , state(UNINIT)
    , frame()
    , storage(storage)
{
}

/**
 * Construct a BackupReplica to manage a replica which already has an
 * existing representation on storage.
 *
 * \param storage
 *      The storage which this segment will be backed by when closed.
 * \param masterId
 *      The master id of the segment being managed.
 * \param segmentId
 *      The segment id of the segment being managed.
 * \param segmentSize
 *      The number of bytes contained in this segment.
 * \param frame
 *      BackupStorage::Frame this replica's representation is in. This
 *      reassociates it with that representation so operations to this
 *      instance affect that segment frame.
 * \param isClosed
 *      Whether the on-storage replica metadata indicates a closed or
 *      open status. This instance will reflect that status. If this is
 *      true the segment is retreived from storage to simplify some
 *      code that assumes open replicas reside in memory.
 */
BackupReplica::BackupReplica(BackupStorage& storage,
                             ServerId masterId,
                             uint64_t segmentId,
                             uint32_t segmentSize,
                             BackupStorage::Frame* frame,
                             bool isClosed)
    : masterId(masterId)
    , primary(false)
    , segmentId(segmentId)
    , createdByCurrentProcess(false)
    , mutex()
    , recoveryException()
    , recoveryPartitions()
    , recoverySegments(NULL)
    , recoverySegmentsLength()
    , rightmostWrittenOffset(0)
    , segmentSize(segmentSize)
    , state(isClosed ? CLOSED : OPEN)
    , frame(frame)
    , storage(storage)
{
    if (state == OPEN)
        frame->load();

    // TODO(stutsman): Claim this replica is the longest if it was closed
    // and the shortest if it was open. This field should go away soon, but
    // this should make it so that replicas from backups that haven't crashed
    // are preferred over this one.
    if (state == CLOSED)
        rightmostWrittenOffset = BYTES_WRITTEN_CLOSED;
}

/**
 * Store any open segments to storage and then release all resources
 * associate with them except permanent storage.
 */
BackupReplica::~BackupReplica()
{
    Lock lock(mutex);

    if (state == OPEN) {
        LOG(WARNING, "Backup shutting down with open segment <%s,%lu>",
            masterId.toString().c_str(),
            segmentId);
        state = CLOSED;
        // May want to add a sync() of some kind here for safe shutdowns.
    }
    if (isRecovered())
        delete[] recoverySegments;
    // recoveryException cleaned up by unique_ptr
}

/**
 * Append a recovery segment to a Buffer.  This segment must have its
 * recovery segments constructed first; see buildRecoverySegments().
 *
 * \param partitionId
 *      The partition id corresponding to the tablet ranges of the recovery
 *      segment to append to \a buffer.
 * \param[out] buffer
 *      A buffer which onto which the requested recovery segment will be
 *      appended.
 * \param[out] certificate
 *      Certificate for the recovery segment returned in \a buffer. Used by
 *      recovery masters to check the integrity of the metadata of the
 *      returned recovery segment and to iterate over it.
 * \return
 *      Status code: STATUS_OK if the recovery segment was appended,
 *      STATUS_RETRY if the caller should try again later.
 * \throw BadSegmentIdException
 *      If the segment to which this recovery segment belongs is not yet
 *      recovered or there is no such recovery segment for that
 *      #partitionId.
 * \throw SegmentRecoveryFailedException
 *      If the segment to which this recovery segment belongs failed to
 *      recover.
 */
Status
BackupReplica::appendRecoverySegment(uint64_t partitionId,
                                     Buffer* buffer,
                                     Segment::Certificate* certificate)
{
    Lock lock(mutex, std::try_to_lock_t());
    if (!lock.owns_lock()) {
        LOG(DEBUG, "Deferring because couldn't acquire lock immediately");
        return STATUS_RETRY;
    }

    if (state != RECOVERING) {
        LOG(WARNING, "Asked for segment <%s,%lu> which isn't recovering",
           masterId.toString().c_str(), segmentId);
        throw BackupBadSegmentIdException(HERE);
    }

    if (!primary) {
        if (!isRecovered() && !recoveryException) {
            LOG(DEBUG, "Requested segment <%s,%lu> is secondary, "
                "starting build of recovery segments now",
                masterId.toString().c_str(), segmentId);
            frame->load();
            lock.unlock();
            buildRecoverySegments(*recoveryPartitions);
            lock.lock();
        }
    }

    if (!isRecovered() && !recoveryException) {
        LOG(DEBUG, "Deferring because <%s,%lu> not yet filtered",
            masterId.toString().c_str(), segmentId);
        return STATUS_RETRY;
    }
    assert(state == RECOVERING);

    if (primary)
        ++metrics->backup.primaryLoadCount;
    else
        ++metrics->backup.secondaryLoadCount;

    if (recoveryException) {
        auto e = SegmentRecoveryFailedException(*recoveryException);
        recoveryException.reset();
        throw e;
    }

    if (partitionId >= recoverySegmentsLength) {
        LOG(WARNING, "Asked for recovery segment %lu from segment <%s,%lu> "
                     "but there are only %u partitions",
            partitionId, masterId.toString().c_str(), segmentId,
            recoverySegmentsLength);
        throw BackupBadSegmentIdException(HERE);
    }

    recoverySegments[partitionId].appendToBuffer(*buffer);
    recoverySegments[partitionId].getAppendedLength(*certificate);

    LOG(DEBUG, "appendRecoverySegment <%s,%lu>", masterId.toString().c_str(),
        segmentId);
    return STATUS_OK;
}

/**
 * Returns true if the entry is alive and should be recovered, otherwise
 * false if it should be ignored.
 *
 * This decision is necessary in order to avoid zombies. For example, when
 * migrating a tablet away and then back again, its possible for old objects
 * to be in the log from the first instance of the tablet that are no longer
 * alive. If the server fails, we need to filter them out (lest they come
 * back to feast on our delicious brains).
 *
 * To combat this, the coordinator keeps the minimum log offset at which
 * valid objects in each tablet may exist. Any objects that come before this
 * point could not have been part of the current tablet.
 *
 * The situation is slightly complicated by the cleaner, since it may create
 * segments with identifiers ahead of the log head despite containing data
 * from only segments before the head at the time of cleaning. To address
 * this, each segment generated by the cleaner includes the head's segment
 * id at the time it was generated. This allows us to logically order cleaner
 * segments and segments written by the log (i.e. that were previously heads).
 *
 * When a tablet is created, the current log head is recorded. Any cleaner-
 * generated segments marked with that head id or lower cannot contain live
 * data for the new tablet. Only cleaner-generated segments with higher IDs
 * could contain valid data. Segments written by the log (i.e. the head or
 * previous heads) will contain valid data for the new tablet only at positions
 * greater than or equal to the recorded log head at tablet instantation time.
 * Importantly, this requires that the hash table be purged of all objects that
 * were previously in a tablet before a tablet is reassigned, since the cleaner
 * uses the presence of a tablet and hash table entries to gauge liveness.
 *
 * \param position
 *      Log::Position indicating where this entry occurred in the log. Used to
 *      determine if it was written before a tablet it could belong to was
 *      created.
 * \param tablet
 *      Tablet to which this entry would belong if were current. It's up to
 *      the caller to ensure that the given entry falls within this tablet
 *      range.
 * \param header
 *      The SegmentHeader of the segment that contains this entry. Used to
 *      determine if the segment was generated by the cleaner or not, and
 *      if so, when it was created. This information precludes objects that
 *      pre-existed the current tablet, but were copied ahead in the log by
 *      the cleaner.
 * \return
 *      true if this object belongs to the given tablet, false if it is from
 *      a previous instance of the tablet and should be dropped.
 */
bool
isEntryAlive(const Log::Position& position,
             const ProtoBuf::Tablets::Tablet& tablet,
             const SegmentHeader& header)
{
    if (header.generatedByCleaner()) {
        uint64_t headId = header.headSegmentIdDuringCleaning;
        if (headId <= tablet.ctime_log_head_id())
            return false;
    } else {
        if (position.getSegmentId() < tablet.ctime_log_head_id())
            return false;
        if (position.getSegmentId() == tablet.ctime_log_head_id() &&
          position.getSegmentOffset() < tablet.ctime_log_head_offset()) {
            return false;
        }
    }
    return true;
}

/**
 * Find which of a set of partitions this object or tombstone is in.
 *
 * \param tableId
 *      Id of the table which this object was created in.
 * \param keyHash
 *      Hash of the string key contained inside this object.
 * \param partitions
 *      The set of object ranges into which the object should be placed.
 *      the cleaner.
 * \return
 *      Pointer to an entry in \a partitions which this object or tombstone
 *      belongs in. Note this object may still not be safe to include in
 *      a recovered segment depending on its position in the log relative to
 *      the time the partition was assigned to the crashed master (see
 *      isEntryLive()). If the object doesn't fit into any partition given
 *      by \a partitions NULL is returned.
 */
const ProtoBuf::Tablets::Tablet*
whichPartition(uint64_t tableId,
               HashType keyHash,
               const ProtoBuf::Tablets& partitions)
{
    for (int i = 0; i < partitions.tablet_size(); i++) {
        const ProtoBuf::Tablets::Tablet& tablet(partitions.tablet(i));
        if (tablet.table_id() == tableId &&
            (tablet.start_key_hash() <= keyHash &&
            tablet.end_key_hash() >= keyHash)) {
            return &tablet;
        }
    }
    LOG(WARNING, "Couldn't place object with <tableId, keyHash> of <%lu,%lu> "
                 "into any of the given "
                 "tablets for recovery; hopefully it belonged to a deleted "
                 "tablet or lives in another log now", tableId, keyHash);
    return NULL;
}

/**
 * Construct recovery segments for this segment data splitting data among
 * them according to #partitions.  After completion and setting
 * #recoverySegments #condition is notified to wake up threads waiting
 * for recovery segments for this segment.
 *
 * \param partitions
 *      A set of tablets grouped into partitions which are used to divide
 *      the stored segment data into recovery segments.
 */
void
BackupReplica::buildRecoverySegments(const ProtoBuf::Tablets& partitions)
{
    Lock lock(mutex);
    assert(state == RECOVERING);

    if (recoverySegments) {
        LOG(NOTICE, "Recovery segments already built for <%s,%lu>",
            masterId.toString().c_str(), segmentId);
        // Skip if the recovery segments were generated earlier.
        return;
    }

    // No need to check integrity or which segment it is for. That is
    // checked on startup and then taken care of by construction.
    const BackupReplicaMetadata* metadata =
        reinterpret_cast<const BackupReplicaMetadata*>(frame->getMetadata());
    void* segment = frame->load();

    uint64_t start = Cycles::rdtsc();

    recoveryException.reset();

    uint32_t partitionCount = 0;
    // a store for non-duplicated list of partitions for the segment.
    std::unordered_set<uint64_t> partitionSet;

    for (int i = 0; i < partitions.tablet_size(); ++i) {
        partitionCount = std::max(partitionCount,
                  downCast<uint32_t>(partitions.tablet(i).user_data() + 1));

        // Insert the partition ID to partitionSet
        partitionSet.insert(partitions.tablet(i).user_data());
    }
    LOG(NOTICE, "Building %u recovery segments for segment %lu",
        partitionCount, segmentId);
    CycleCounter<RawMetric> _(&metrics->backup.filterTicks);

    recoverySegments = new Segment[partitionCount];
    recoverySegmentsLength = partitionCount;

    try {
        const SegmentHeader* header = NULL;
        SegmentIterator it(segment, segmentSize, metadata->getCertificate());
        it.checkMetadataIntegrity();
        for (; !it.isDone(); it.next()) {
            LogEntryType type = it.getType();

            if (type == LOG_ENTRY_TYPE_SEGHEADER) {
                Buffer buffer;
                it.appendToBuffer(buffer);
                header = buffer.getStart<SegmentHeader>();
                continue;
            }

            if (type != LOG_ENTRY_TYPE_OBJ &&
                type != LOG_ENTRY_TYPE_OBJTOMB &&
                type != LOG_ENTRY_TYPE_SAFEVERSION)
                continue;

            if (header == NULL) {
                DIE("Found object,rtombstone, or safeVersion"
                    "before header while "
                    "building recovery segments for <%s,%lu>",
                    masterId.toString().c_str(), segmentId);
            }

            Buffer buffer;
            it.appendToBuffer(buffer);

            Log::Position position(segmentId, it.getOffset());

            if (type == LOG_ENTRY_TYPE_SAFEVERSION) {
                // duplicate the entry to all partitions 
                // for safeVersion recovery
                foreach (uint64_t partition, partitionSet) {
                    bool success =
                            recoverySegments[partition].append(type, buffer);
                    if (!success) {
                        LOG(WARNING, "Failed to append safeVersion"
                            "to masterId,segment,partition=<%s,%lu,%lu>",
                            masterId.toString().c_str(),
                            segmentId, partition);
                        throw Exception(HERE, "catch this and bail");
                    }
                }
                continue;
            }

            uint64_t tableId = -1;
            HashType keyHash = -1;
            if (type == LOG_ENTRY_TYPE_OBJ) {
                Object object(buffer);
                tableId = object.getTableId();
                keyHash = Key::getHash(tableId,
                                       object.getKey(), object.getKeyLength());
            } else { // LOG_ENTRY_TYPE_OBJTOMB:
                ObjectTombstone tomb(buffer);
                tableId = tomb.getTableId();
                keyHash = Key::getHash(tableId,
                                       tomb.getKey(), tomb.getKeyLength());
            }

            // find out which partition this entry belongs in
            const auto* partition = whichPartition(tableId, keyHash,
                                                   partitions);
            if (!partition)
                continue;
            uint64_t partitionId = partition->user_data();

            if (!isEntryAlive(position, *partition, *header)) {
                LOG(NOTICE, "Skipping object with <tableId, keyHash> of "
                    "<%lu,%lu> because it appears to have existed prior "
                    "to this tablet's creation.", tableId, keyHash);
                continue;
            }

            bool success = recoverySegments[partitionId].append(type, buffer);
            if (!success) {
                LOG(WARNING, "Failed to append data to recovery segment");
                throw Exception(HERE, "catch this and bail");
            }
        }
#if TESTING
        for (uint64_t i = 0; i < recoverySegmentsLength; ++i) {
            Segment::Certificate unused;
            LOG(DEBUG, "Recovery segment for <%s,%lu> partition %lu is %u B",
                masterId.toString().c_str(), segmentId, i,
                 recoverySegments[i].getAppendedLength(unused));
        }
#endif
    } catch (const SegmentIteratorException& e) {
        LOG(WARNING, "Exception occurred building recovery segments: %s",
            e.what());
        delete[] recoverySegments;
        recoverySegments = NULL;
        recoverySegmentsLength = 0;
        recoveryException.reset(new SegmentRecoveryFailedException(e.where));
        // leave state as RECOVERING, we'll want to garbage collect this
        // segment at the end of the recovery even if we couldn't parse
        // this segment
    } catch (...) {
        LOG(WARNING, "Unknown exception occurred building recovery segments");
        delete[] recoverySegments;
        recoverySegments = NULL;
        recoverySegmentsLength = 0;
        recoveryException.reset(new SegmentRecoveryFailedException(HERE));
        // leave state as RECOVERING, see note in above block
    }
    LOG(DEBUG, "<%s,%lu> recovery segments took %lu ms to construct, "
               "notifying other threads",
        masterId.toString().c_str(), segmentId,
        Cycles::toNanoseconds(Cycles::rdtsc() - start) / 1000 / 1000);
}

/**
 * Close this segment to permanent storage and free up in memory
 * resources.
 *
 * After this writeSegment() cannot be called for this segment id any
 * more.  The segment will be restored on recovery unless the client later
 * calls freeSegment() on it and will appear to be closed to the recoverer.
 *
 * \throw BackupBadSegmentIdException
 *      If this segment is not open.
 */
void
BackupReplica::close()
{
    Lock lock(mutex);

    if (state == CLOSED)
        return;
    else if (state != OPEN)
        throw BackupBadSegmentIdException(HERE);

    state = CLOSED;
    rightmostWrittenOffset = BYTES_WRITTEN_CLOSED;

    assert(frame);
    frame->close();
}

/**
 * Release all resources related to this segment including storage.
 * This will block for all outstanding storage operations before
 * freeing the storage resources.
 */
void
BackupReplica::free()
{
    Lock lock(mutex);
    // Don't wait for secondary segment recovery segments
    // they aren't running in a separate thread.
    while (state == RECOVERING &&
           primary &&
           !isRecovered() &&
           !recoveryException);

    frame->free();
    frame = NULL;
    state = FREED;
    TEST_LOG("called");
}

/**
 * Allocate a frame on storage, resetting its state to accept appends for a new
 * replica. Open is not synchronous itself. Even after return from open() if
 * this backup crashes it may find the replica which was formerly stored in
 * this frame or metadata for the former replica and data for the newly open
 * replica. Recovery is expected to address these consistency issues with the
 * checksums.
 *
 * This call is NOT idempotent since it allocates storage space.
 * The caller must take care not to repeat calls to open() for a single
 * replica.
 *
 * \param sync
 *      Only return from write() calls when all enqueued data and the  most
 *      recently enqueued metadata are durable on storage.
 */
void
BackupReplica::open(bool sync)
{
    Lock lock(mutex);
    assert(state == UNINIT);
    frame = storage.open(sync);
    state = OPEN;
}

/**
 * Set the state to #RECOVERING from #OPEN or #CLOSED.  This can only be called
 * on a primary segment.  Returns true if the segment was already #RECOVERING.
 */
bool
BackupReplica::setRecovering()
{
    Lock lock(mutex);
    assert(primary);
    bool wasRecovering = state == RECOVERING;
    state = RECOVERING;
    return wasRecovering;
}

/**
 * Set the state to #RECOVERING from #OPEN or #CLOSED and store a copy of the
 * supplied tablet information in case construction of recovery segments is
 * needed later for this secondary segment. Returns true if the segment was
 * already #RECOVERING.
 */
bool
BackupReplica::setRecovering(const ProtoBuf::Tablets& partitions)
{
    Lock lock(mutex);
    assert(!primary);
    bool wasRecovering = state == RECOVERING;
    state = RECOVERING;
    // Make a copy of the partition list for deferred filtering.
    recoveryPartitions.construct(partitions);
    return wasRecovering;
}


/**
 * Begin reading of segment from disk to memory. Marks the segment
 * CLOSED (immutable) as well.  Once the segment is in memory
 * #segment will be set and waiting threads will be notified via
 * #condition.
 */
void
BackupReplica::startLoading()
{
    Lock lock(mutex);
    frame->startLoading();
}

/**
 * Store data from a buffer into the replica.
 * Tracks, for this replica, which is the rightmost written byte which
 * is used as a proxy for "replica length" for the return of
 * startReadingData calls.
 *
 * \param source
 *      Buffer contained the data to be copied into the replica.
 * \param sourceOffset
 *      Offset into \a source where data should be copied from.
 * \param length
 *      Bytes to copy to the frame starting at \a sourceOffset in \a source.
 * \param destinationOffset
 *      Offset into the replica where the source data should be copied.
 * \param certificate
 *      Written as part of the metadata for this replica. Used
 *      during recovery to determine how much of the replica contains valid
 *      data and to verify the integrity of the segment metadata. This may
 *      be NULL which has two ramifications. First, the data included in
 *      this write will not be recovered (or, is not durable) until the
 *      after the next write which includes a certificate. Second, the
 *      most recently appended certificate will be used during recovery,
 *      which means only data covered by that certificate will be recovered
 *      regardless of how much has been transmitted to the backup.
 */
void
BackupReplica::append(Buffer& source,
                      size_t sourceOffset,
                      size_t length,
                      size_t destinationOffset,
                      const Segment::Certificate* certificate)
{
    Lock lock(mutex);
    assert(state == OPEN);
    if (certificate) {
        BackupReplicaMetadata metadata(*certificate,
                                       masterId.getId(), segmentId,
                                       segmentSize, false);
        frame->append(source, sourceOffset, length, destinationOffset,
                      &metadata, sizeof(metadata));
    } else {
        frame->append(source, sourceOffset, length, destinationOffset, NULL, 0);
    }
    rightmostWrittenOffset =
        std::max(rightmostWrittenOffset,
                 downCast<uint32_t>(destinationOffset + length));
}

/**
 * Scan the current Segment for a LogDigest. If it exists, append it to the
 * given buffer.
 *
 * \param[out] digestBuffer
 *      Buffer to append the digest to, if found. May be NULL if the data is
 *      not desired and only the presence is needed.
 * \return
 *      True if the digest was found and appended to the given buffer, otherwise
 *      false.
 */
bool
BackupReplica::getLogDigest(Buffer* digestBuffer)
{
    if (!isOpen())
        return false;

    // No need to check integrity or which segment it is for. That is
    // checked on startup and then taken care of by construction.
    const BackupReplicaMetadata* metadata =
        reinterpret_cast<const BackupReplicaMetadata*>(frame->getMetadata());
    // This should never block since getLogDigest is only called for open
    // segments which we always keep in memory.
    void* replicaData = frame->load();

    // If the Segment is malformed somehow, just ignore it. The
    // coordinator will have to deal.
    try {
        SegmentIterator it(replicaData, segmentSize,
                           metadata->getCertificate());
        it.checkMetadataIntegrity();
        while (!it.isDone()) {
            if (it.getType() == LOG_ENTRY_TYPE_LOGDIGEST) {
                if (digestBuffer != NULL)
                    it.appendToBuffer(*digestBuffer);
                return true;
            }
            it.next();
        }
    } catch (SegmentIteratorException& e) {
        LOG(WARNING, "SegmentIterator constructor failed: %s", e.str().c_str());
    }
    return false;
}

} // namespace RAMCloud
