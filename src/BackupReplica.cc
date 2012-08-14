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
 * \param pool
 *      The pool from which this segment should draw and return memory
 *      when it is moved in and out of memory.
 * \param ioScheduler
 *      The IoScheduler through which IO requests are made.
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
                             ThreadSafePool& pool,
                             IoScheduler& ioScheduler,
                             ServerId masterId,
                             uint64_t segmentId,
                             uint32_t segmentSize,
                             bool primary)
    : masterId(masterId)
    , primary(primary)
    , segmentId(segmentId)
    , createdByCurrentProcess(true)
    , mutex()
    , condition()
    , ioScheduler(ioScheduler)
    , recoveryException()
    , recoveryPartitions()
    , recoverySegments(NULL)
    , recoverySegmentsLength()
    , footerOffset(0)
    , footerEntry()
    , rightmostWrittenOffset(0)
    , segment()
    , segmentSize(segmentSize)
    , state(UNINIT)
    , replicateAtomically(false)
    , storageHandle()
    , pool(pool)
    , storage(storage)
    , storageOpCount(0)
{
}

/**
 * Construct a BackupReplica to manage a replica which already has an
 * existing representation on storage.
 *
 * \param storage
 *      The storage which this segment will be backed by when closed.
 * \param pool
 *      The pool from which this segment should draw and return memory
 *      when it is moved in and out of memory.
 * \param ioScheduler
 *      The IoScheduler through which IO requests are made.
 * \param masterId
 *      The master id of the segment being managed.
 * \param segmentId
 *      The segment id of the segment being managed.
 * \param segmentSize
 *      The number of bytes contained in this segment.
 * \param segmentFrame
 *      The segment frame number this replica's representation is in.  This
 *      reassociates it with that representation so operations to this
 *      instance affect that segment frame.
 * \param isClosed
 *      Whether the on-storage replica indicates a closed or open status.
 *      This instance will reflect that status.  If this is true the
 *      segment is retreived from storage to simplify some legacy code
 *      that assumes open replicas reside in memory.
 */
BackupReplica::BackupReplica(BackupStorage& storage,
                             ThreadSafePool& pool,
                             IoScheduler& ioScheduler,
                             ServerId masterId,
                             uint64_t segmentId,
                             uint32_t segmentSize,
                             uint32_t segmentFrame,
                             bool isClosed)
    : masterId(masterId)
    , primary(false)
    , segmentId(segmentId)
    , createdByCurrentProcess(false)
    , mutex()
    , condition()
    , ioScheduler(ioScheduler)
    , recoveryException()
    , recoveryPartitions()
    , recoverySegments(NULL)
    , recoverySegmentsLength()
    , footerOffset(0)
    , footerEntry()
    , rightmostWrittenOffset(0)
    , segment()
    , segmentSize(segmentSize)
    , state(isClosed ? CLOSED : OPEN)
    , replicateAtomically(false)
    , storageHandle()
    , pool(pool)
    , storage(storage)
    , storageOpCount(0)
{
    storageHandle = storage.associate(segmentFrame);
    if (state == OPEN) {
        // There is some disagreement between the masters and backups about
        // the meaning of "open".  In a few places the backup code assumes
        // open replicas must be in memory.  For now its easiest just to
        // make sure that is the case, so on cold start we have to reload
        // open replicas into ram.
        try {
            // Get memory for staging the segment writes
            segment = static_cast<char*>(pool.malloc());
            storage.getSegment(storageHandle, segment);
        } catch (...) {
            pool.free(segment);
            delete storageHandle;
            throw;
        }
    }

    // TODO(stutsman): Claim this replica is the longest if it was closed
    // and the shortest if it was open.  This field should go away soon, but
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
    waitForOngoingOps(lock);

    if (state == OPEN) {
        if (replicateAtomically && state != CLOSED) {
            LOG(NOTICE, "Backup shutting down with open segment <%lu,%lu>, "
                "which was open for atomic replication; discarding since the "
                "replica was incomplete", *masterId, segmentId);
        } else {
            LOG(NOTICE, "Backup shutting down with open segment <%lu,%lu>, "
                "closing out to storage", *masterId, segmentId);
            state = CLOSED;
            CycleCounter<RawMetric> _(&metrics->backup.storageWriteTicks);
            ++metrics->backup.storageWriteCount;
            metrics->backup.storageWriteBytes += segmentSize;
            storage.putSegment(storageHandle, segment);
        }
    }
    if (isRecovered()) {
        delete[] recoverySegments;
        recoverySegments = NULL;
        recoverySegmentsLength = 0;
    }
    if (inStorage()) {
        delete storageHandle;
        storageHandle = NULL;
    }
    if (inMemory()) {
        pool.free(segment);
        segment = NULL;
    }
    // recoveryException cleaned up by unique_ptr
}

/**
 * Append a recovery segment to a Buffer.  This segment must have its
 * recovery segments constructed first; see buildRecoverySegments().
 *
 * \param partitionId
 *      The partition id corresponding to the tablet ranges of the recovery
 *      segment to append to #buffer.
 * \param[out] buffer
 *      A buffer which onto which the requested recovery segment will be
 *      appended.
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
BackupReplica::appendRecoverySegment(uint64_t partitionId, Buffer& buffer)
{
    Lock lock(mutex, std::try_to_lock_t());
    if (!lock.owns_lock()) {
        LOG(DEBUG, "Deferring because couldn't acquire lock immediately");
        return STATUS_RETRY;
    }

    if (state != RECOVERING) {
        LOG(WARNING, "Asked for segment <%lu,%lu> which isn't recovering",
            *masterId, segmentId);
        throw BackupBadSegmentIdException(HERE);
    }

    if (!primary) {
        if (!isRecovered() && !recoveryException) {
            LOG(DEBUG, "Requested segment <%lu,%lu> is secondary, "
                "starting build of recovery segments now",
                *masterId, segmentId);
            waitForOngoingOps(lock);
            ioScheduler.load(*this);
            ++storageOpCount;
            lock.unlock();
            buildRecoverySegments(*recoveryPartitions);
            lock.lock();
        }
    }

    if (!isRecovered() && !recoveryException) {
        LOG(DEBUG, "Deferring because <%lu,%lu> not yet filtered",
            *masterId, segmentId);
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
        LOG(WARNING, "Asked for recovery segment %lu from segment <%lu,%lu> "
                     "but there are only %u partitions",
            partitionId, *masterId, segmentId, recoverySegmentsLength);
        throw BackupBadSegmentIdException(HERE);
    }

    recoverySegments[partitionId].appendToBuffer(buffer);

    LOG(DEBUG, "appendRecoverySegment <%lu,%lu>", *masterId, segmentId);
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
 * \param type
 *      Type of the log entry.
 * \param buffer
 *      Buffer containing the log entry.
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
isEntryAlive(Log::Position& position,
             LogEntryType type,
             Buffer& buffer,
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
 * \param position
 *      Log::Position indicating where this entry occurred in the log. Used to
 *      determine if it was written before a tablet it could belong to was
 *      created.
 * \param type
 *      Type of the log entry.
 * \param buffer
 *      Buffer containing the log entry.
 * \param partitions
 *      The set of object ranges into which the object should be placed.
 * \param header
 *      The SegmentHeader of the segment that contains this entry. Used to
 *      determine if the segment was generated by the cleaner or not, and
 *      if so, when it was created. This information precludes objects that
 *      pre-existed the current tablet, but were copied ahead in the log by
 *      the cleaner.
 * \return
 *      The id of the partition which this object or tombstone belongs in.
 * \throw BackupMalformedSegmentException
 *      If the object or tombstone doesn't belong to any of the partitions.
 */
Tub<uint64_t>
whichPartition(Log::Position& position,
               LogEntryType type,
               Buffer& buffer,
               const ProtoBuf::Tablets& partitions,
               const SegmentHeader& header)
{
    uint64_t tableId = -1;
    HashType keyHash = -1;

    if (type == LOG_ENTRY_TYPE_OBJ) {
        Object object(buffer);
        tableId = object.getTableId();
        keyHash = Key::getHash(tableId, object.getKey(), object.getKeyLength());
    } else { // LOG_ENTRY_TYPE_OBJTOMB:
        ObjectTombstone tomb(buffer);
        tableId = tomb.getTableId();
        keyHash = Key::getHash(tableId, tomb.getKey(), tomb.getKeyLength());
    }

    Tub<uint64_t> ret;
    // TODO(stutsman): need to check how slow this is, can do better with a tree
    for (int i = 0; i < partitions.tablet_size(); i++) {
        const ProtoBuf::Tablets::Tablet& tablet(partitions.tablet(i));
        if (tablet.table_id() == tableId &&
            (tablet.start_key_hash() <= keyHash &&
            tablet.end_key_hash() >= keyHash)) {

            if (!isEntryAlive(position, type, buffer, tablet, header)) {
                LOG(NOTICE, "Skipping object with <tableId, keyHash> of "
                    "<%lu,%lu> because it appears to have existed prior "
                    "to this tablet's creation.", tableId, keyHash);
                return ret;
            }

            ret.construct(tablet.user_data());
            return ret;
        }
    }
    LOG(WARNING, "Couldn't place object with <tableId, keyHash> of <%lu,%lu> "
                 "into any of the given "
                 "tablets for recovery; hopefully it belonged to a deleted "
                 "tablet or lives in another log now", tableId, keyHash);
    return ret;
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
        LOG(NOTICE, "Recovery segments already built for <%lu,%lu>",
            masterId.getId(), segmentId);
        // Skip if the recovery segments were generated earlier.
        condition.notify_all();
        return;
    }

    waitForOngoingOps(lock);

    assert(inMemory());

    uint64_t start = Cycles::rdtsc();

    recoveryException.reset();

    uint32_t partitionCount = 0;
    for (int i = 0; i < partitions.tablet_size(); ++i) {
        partitionCount = std::max(partitionCount,
                  downCast<uint32_t>(partitions.tablet(i).user_data() + 1));
    }
    LOG(NOTICE, "Building %u recovery segments for %lu",
        partitionCount, segmentId);
    CycleCounter<RawMetric> _(&metrics->backup.filterTicks);

    recoverySegments = new Segment[partitionCount];
    recoverySegmentsLength = partitionCount;

    try {
        const SegmentHeader* header = NULL;
        for (SegmentIterator it(segment, segmentSize);
             !it.isDone();
             it.next())
        {
            LogEntryType type = it.getType();

            if (type == LOG_ENTRY_TYPE_SEGHEADER) {
                Buffer buffer;
                it.appendToBuffer(buffer);
                header = buffer.getStart<SegmentHeader>();
                continue;
            }

            if (type != LOG_ENTRY_TYPE_OBJ && type != LOG_ENTRY_TYPE_OBJTOMB)
                continue;

            if (header == NULL) {
                LOG(WARNING, "Object or tombstone came before header");
                throw Exception(HERE, "catch this and bail");
            }

            Buffer buffer;
            it.appendToBuffer(buffer);

            Log::Position position(segmentId, it.getOffset());

            // find out which partition this entry belongs in
            Tub<uint64_t> partitionId = whichPartition(position,
                                                       type,
                                                       buffer,
                                                       partitions,
                                                       *header);
            if (!partitionId)
                continue;

            bool success = recoverySegments[*partitionId].append(type, buffer);
            if (!success) {
                LOG(WARNING, "Failed to append data to recovery segment");
                throw Exception(HERE, "catch this and bail");
            }
        }
#if TESTING
        for (uint64_t i = 0; i < recoverySegmentsLength; ++i) {
            Segment::OpaqueFooterEntry unused;
            LOG(DEBUG, "Recovery segment for <%lu,%lu> partition %lu is %u B",
                *masterId, segmentId, i,
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
    LOG(DEBUG, "<%lu,%lu> recovery segments took %lu ms to construct, "
               "notifying other threads",
        *masterId, segmentId,
        Cycles::toNanoseconds(Cycles::rdtsc() - start) / 1000 / 1000);
    condition.notify_all();
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

    applyFooterEntry();

    state = CLOSED;
    rightmostWrittenOffset = BYTES_WRITTEN_CLOSED;

    assert(storageHandle);
    ioScheduler.store(*this);
    ++storageOpCount;
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
    waitForOngoingOps(lock);

    // Don't wait for secondary segment recovery segments
    // they aren't running in a separate thread.
    while (state == RECOVERING &&
           primary &&
           !isRecovered() &&
           !recoveryException)
        condition.wait(lock);

    if (inMemory())
        pool.free(segment);
    segment = NULL;
    storage.free(storageHandle);
    storageHandle = NULL;
    state = FREED;
}

/**
 * Open the segment.  After this the segment is in memory and mutable
 * with reserved storage.  Any controlled shutdown of the server will
 * ensure the segment reaches stable storage unless the segment is
 * freed in the meantime.
 *
 * The backing memory is zeroed out, though the storage may still have
 * its old contents.
 */
void
BackupReplica::open()
{
    Lock lock(mutex);
    assert(state == UNINIT);
    // Get memory for staging the segment writes
    char* segment = static_cast<char*>(pool.malloc());
    BackupStorage::Handle* handle;
    try {
        // Reserve the space for this on disk
        handle = storage.allocate();
    } catch (...) {
        // Release the staging memory if storage.allocate throws
        pool.free(segment);
        throw;
    }

    this->segment = segment;
    this->storageHandle = handle;
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
    applyFooterEntry();
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
    applyFooterEntry();
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
    waitForOngoingOps(lock);

    ioScheduler.load(*this);
    ++storageOpCount;
}

/**
 * Store data from a buffer into the backup segment.
 * Tracks, for this segment, which is the rightmost written byte which
 * is used as a proxy for "segment length" for the return of
 * startReadingData calls.
 *
 * \param src
 *      The Buffer from which data is to be stored.
 * \param srcOffset
 *      Offset in bytes at which to start copying.
 * \param length
 *      The number of bytes to be copied.
 * \param destOffset
 *      Offset into the backup segment in bytes of where the data is to
 *      be copied.
 * \param footerEntry
 *      New footer which should be written at #destOffset + #length and
 *      at the end of the segment frame if the replica is closed or used
 *      for recovery. NULL if the master provided no footer for this write.
 *      Note this means this write isn't committed on the backup until the
 *      next footered write. See #footerEntry.
 * \param atomic
 *      If true then this replica is considered invalid until a closing
 *      write (or subsequent call to write with \a atomic set to false).
 *      This means that the data will never be written to disk and will
 *      not be reported to or used in recoveries unless the replica is
 *      closed.  This allows masters to create replicas of segments
 *      without the threat that they'll be detected as the head of the
 *      log.  Each value of \a atomic for each write call overrides the
 *      last, so in order to atomically write an entire segment all
 *      writes must have \a atomic set to true (though, it is
 *      irrelvant for the last, closing write).  A call with atomic
 *      set to false will make that replica available for normal
 *      treatment as an open segment.
 */
void
BackupReplica::write(Buffer& src,
                     uint32_t srcOffset,
                     uint32_t length,
                     uint32_t destOffset,
                     const Segment::OpaqueFooterEntry* footerEntry,
                     bool atomic)
{
    Lock lock(mutex);
    assert(state == OPEN && inMemory());
    src.copy(srcOffset, length, &segment[destOffset]);
    replicateAtomically = atomic;
    rightmostWrittenOffset = std::max(rightmostWrittenOffset,
                                      destOffset + length);
    if (footerEntry) {
        const uint32_t targetFooterOffset = destOffset + length;
        if (targetFooterOffset < footerOffset) {
            LOG(ERROR, "Write to <%lu,%lu> included a footer which was "
                "requested to be written at offset %u but a prior write "
                "placed a footer later in the segment at %u",
                masterId.getId(), segmentId, targetFooterOffset, footerOffset);
            throw BackupSegmentOverflowException(HERE);
        }
        if (targetFooterOffset + 2 * sizeof(*footerEntry) > segmentSize) {
            // Need room for two copies of the footer. One at the end of the
            // data and the other aligned to the end of the segment which
            // can be found during restart without reading the whole segment.
            LOG(ERROR, "Write to <%lu,%lu> included a footer which was "
                "requested to be written at offset %u but there isn't "
                "enough room in the segment for the footer", masterId.getId(),
                segmentId, targetFooterOffset);
            throw BackupSegmentOverflowException(HERE);
        }
        this->footerEntry = *footerEntry;
        footerOffset = targetFooterOffset;
    }
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
    Lock lock(mutex);
    // If the Segment is malformed somehow, just ignore it. The
    // coordinator will have to deal.
    try {
        SegmentIterator it(segment, segmentSize);
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

// - private -

/**
 * Flatten #footerEntry into two locations in the in-memory buffer (#segment).
 * The first goes where the master asked (#footerOffset), the second goes at
 * the end of the footer, which makes it efficient to find on restart.
 * This has the effect of truncating any non-footered writes since the last
 * footered-write. See #footerEntry for details.
 */
void
BackupReplica::applyFooterEntry()
{
    if (state != OPEN)
        return;
    memcpy(segment + footerOffset, &footerEntry, sizeof(footerEntry));
    memcpy(segment + segmentSize - sizeof(footerEntry),
           &footerEntry, sizeof(footerEntry));
}

} // namespace RAMCloud
