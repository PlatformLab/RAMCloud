/* Copyright (c) 2009-2010 Stanford University
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

#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>

#include "BackupServer.h"
#include "BackupStorage.h"
#include "BenchUtil.h"
#include "Buffer.h"
#include "ClientException.h"
#include "Log.h"
#include "LogTypes.h"
#include "RecoverySegmentIterator.h"
#include "Rpc.h"
#include "Segment.h"
#include "SegmentIterator.h"
#include "Status.h"
#include "TransportManager.h"

namespace RAMCloud {

// --- BackupServer::SegmentInfo ---

/**
 * Construct a a SegmentInfo to manage a segment.
 *
 * \param storage
 *      The storage which this segment will be backed by when closed.
 * \param pool
 *      The pool from which this segment should draw and return memory
 *      when it is moved in and out of memory.
 * \param masterId
 *      The master id of the segment being managed.
 * \param segmentId
 *      The segment id of the segment being managed.
 */
BackupServer::SegmentInfo::SegmentInfo(BackupStorage& storage, Pool& pool,
                                       uint64_t masterId, uint64_t segmentId)
    : lockedForRecovery()
    , masterId(masterId)
    , recoveryException()
    , recoverySegments(NULL)
    , recoverySegmentsLength()
    , segmentId(segmentId)
    , segment()
    , segmentSync()
    , state(UNINIT)
    , storageHandle()
    , pool(pool)
    , storage(storage)
{
}

/**
 * Store any open segments to storage and then release all resources
 * associate with them except permanent storage.
 */
BackupServer::SegmentInfo::~SegmentInfo()
{
    waitForRecoveryCompletion();
    if (isOpen()) {
        LOG(NOTICE, "Backup shutting down with open segment <%lu,%lu>, "
            "closing out to storage", masterId, segmentId);
        close();
    }
    if (isLoading())
        getSegment();
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
    // recoveryException cleaned up by scoped_ptr
    // segmentSync cleaned up by scoped_ptr
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
 * \throw BadSegmentIdException
 *      If the segment to which this recovery segment belongs is not yet
 *      recovered or there is no such recovery segment for that
 *      #partitionId.
 * \throw SegmentRecoveryFailedException
 *      If the segment to which this recovery segment belongs failed to
 *      recover.
 */
void
BackupServer::SegmentInfo::appendRecoverySegment(uint64_t partitionId,
                                                 Buffer& buffer)
{
    uint64_t start = rdtsc();
    waitForRecoveryCompletion();
#if !TESTING
    LOG(DEBUG, "Done spinning for segment <%lu,%lu>, waited %lu ms", masterId,
        segmentId, cyclesToNanoseconds(rdtsc() - start) / 1000 / 1000);
#endif
    if (recoveryException) {
        auto e = SegmentRecoveryFailedException(*recoveryException);
        recoveryException.reset();
        throw e;
    }
    if (!isRecovered()) {
        LOG(WARNING, "Asked for segment <%lu,%lu> which wasn't recovered yet",
            masterId, segmentId);
        throw BackupBadSegmentIdException(HERE);
    }
    if (partitionId >= recoverySegmentsLength) {
        LOG(WARNING, "Asked for recovery segment %lu from segment <%lu,%lu> "
                     "but there are only %u partitions",
            partitionId, masterId, segmentId, recoverySegmentsLength);
        throw BackupBadSegmentIdException(HERE);
    }

    for (Buffer::Iterator it(recoverySegments[partitionId]);
         !it.isDone(); it.next())
    {
        Buffer::Chunk::appendToBuffer(&buffer, it.getData(), it.getLength());
    }
    LOG(DEBUG, "appendRecoverySegment <%lu,%lu>, took %lu ms",
        masterId, segmentId,
        cyclesToNanoseconds(rdtsc() - start) / 1000 / 1000);
}

/**
 * Find which of a set of partitions this object or tombstone is in.
 *
 * \param type
 *      Either LOG_ENTRY_TYPE_OBJ or LOG_ENTRY_TYPE_OBJTOMB.  The result
 *      of this function for a SegmentEntry of any other type is undefined.
 * \param data
 *      The start of an object or tombstone in memory.
 * \param partitions
 *      The set of object ranges into which the object should be placed.
 * \return
 *      The id of the partition which this object or tombstone belongs in.
 * \throw BackupMalformedSegmentException
 *      If the object or tombstone doesn't belong to any of the partitions.
 */
ObjectTub<uint64_t>
whichPartition(const LogEntryType type,
               const void* data,
               const ProtoBuf::Tablets& partitions)
{
    const Object* object =
        reinterpret_cast<const Object*>(data);
    const ObjectTombstone* tombstone =
        reinterpret_cast<const ObjectTombstone*>(data);

    uint64_t tableId;
    uint64_t objectId;
    if (type == LOG_ENTRY_TYPE_OBJ) {
        tableId = object->table;
        objectId = object->id;
    } else { // LOG_ENTRY_TYPE_OBJTOMB:
        tableId = tombstone->table;
        objectId = tombstone->id;
    }

    ObjectTub<uint64_t> ret;
    // TODO(stutsman): need to check how slow this is, can do better with a tree
    for (int i = 0; i < partitions.tablet_size(); i++) {
        const ProtoBuf::Tablets::Tablet& tablet(partitions.tablet(i));
        if (tablet.table_id() == tableId &&
            (tablet.start_object_id() <= objectId &&
            tablet.end_object_id() >= objectId)) {
            ret.construct(tablet.user_data());
            return ret;
        }
    }

    LOG(WARNING, "Couldn't place object <%lu,%lu> into any of the given "
                 "tablets for recovery; hopefully it belonged to a deleted "
                 "tablet or lives in another log now", tableId, objectId);
    return ret;
}

/**
 * Construct recovery segments for this segment data splitting data among
 * them according to #partitions.  NOTE: This method is run in another
 * thread and only occurs when that thread has ownership of this SegmentInfo
 * (see #lockedForRecovery).
 *
 * \param partitions
 *      A set of tablets grouped into partitions which are used to divide
 *      the stored segment data into recovery segments.
 * \param segmentSize
 *      The size in bytes of the segments stored by this backup server.
 */
void
BackupServer::SegmentInfo::buildRecoverySegments(
    const ProtoBuf::Tablets& partitions, uint32_t segmentSize)
{
    uint64_t start = rdtsc();
    assert(state == OPEN || state == CLOSED);

    recoveryException.reset();

    uint64_t partitionCount = 0;
    for (int i = 0; i < partitions.tablet_size(); ++i) {
        partitionCount = std::max(partitionCount,
                                  partitions.tablet(i).user_data() + 1);
    }
    LOG(NOTICE, "Building %lu recovery segments for %lu",
        partitionCount, segmentId);

    recoverySegments = new Buffer[partitionCount];
    recoverySegmentsLength = partitionCount;
    try {
        for (SegmentIterator it(getSegment(), segmentSize);
             !it.isDone();
             it.next())
        {
            if (it.getType() == LOG_ENTRY_TYPE_UNINIT)
                break;
            if (it.getType() != LOG_ENTRY_TYPE_OBJ &&
                it.getType() != LOG_ENTRY_TYPE_OBJTOMB)
                continue;

            // find out which partition this entry belongs in
            ObjectTub<uint64_t> partitionId = whichPartition(it.getType(),
                                                             it.getPointer(),
                                                             partitions);
            if (!partitionId)
                continue;
            const SegmentEntry* entry = reinterpret_cast<const SegmentEntry*>(
                    reinterpret_cast<const char*>(it.getPointer()) -
                    sizeof(*entry));
            const uint32_t len = sizeof(*entry) + it.getLength();
            void *out = new(&recoverySegments[*partitionId], APPEND)
                                char[len];
            memcpy(out, entry, len);
        }
#if TESTING
        for (uint64_t i = 0; i < recoverySegmentsLength; ++i)
            LOG(DEBUG, "Recovery segment for <%lu,%lu> partition %lu is %u B",
                masterId, segmentId, i, recoverySegments[i].getTotalLength());
#endif
    } catch (const SegmentIteratorException& e) {
        LOG(WARNING, "Exception occurred building recovery segments: %s",
            e.what());
        delete[] recoverySegments;
        recoverySegments = NULL;
        recoverySegmentsLength = 0;
        recoveryException.reset(new SegmentRecoveryFailedException(e.where));
    } catch (...) {
        LOG(WARNING, "Unknown exception occurred building recovery segments");
        delete[] recoverySegments;
        recoverySegments = NULL;
        recoverySegmentsLength = 0;
    }
    LOG(DEBUG, "<%lu,%lu> recovery segments took %lu ms to construct, "
               "returning ownership to the main thread",
        masterId, segmentId,
        cyclesToNanoseconds(rdtsc() - start) / 1000 / 1000);
    setUnlockedAfterRecovery();
}

/**
 * Make this segment immutable and flush this segment to disk.
 */
void
BackupServer::SegmentInfo::close()
{
    if (!isOpen())
        throw BackupBadSegmentIdException(HERE);
    storage.putSegment(storageHandle, segment);
    pool.free(segment);
    segment = NULL;
    state = CLOSED;
}

/**
 * Release all resources related to this segment including storage.
 */
void
BackupServer::SegmentInfo::free()
{
    waitForRecoveryCompletion();
    if (isLoading())
        getSegment();
    if (inMemory())
        pool.free(segment);
    segment = NULL;
    storage.free(storageHandle);
    storageHandle = NULL;
    state = FREED;
}

/**
 * Return a pointer to the valid segment data in memory.  If the segment is
 * being loaded from storage as part of recovery this call will block until
 * the data is ready.
 *
 * The segment must be in memory or being loaded from storage (i.e. should
 * only be called while the segment is OPEN.
 */
char*
BackupServer::SegmentInfo::getSegment()
{
    assert(state == OPEN || state == CLOSED);
    if (inMemory())
        return segment;
    if (!isLoading())
        startLoading();
    try {
        (*segmentSync)();
    } catch (...) {
        segmentSync.reset();
        if (segment) {
            pool.free(segment);
            segment = NULL;
        }
        state = CLOSED;
        throw;
    }
    segmentSync.reset();
    assert(inMemory());
    return segment;
}

/**
 * Open the segment.  After this the segment is in memory and mutable
 * with reserved storage.  Any controlled shutdown of the server will
 * ensure the segment reaches stable storage unless the segment if
 * freed in the meantime.
 *
 * The backing memory is zeroed out, though the storage may still have
 * its old contents.
 */
void
BackupServer::SegmentInfo::open()
{
    assert(state == UNINIT);
    // Get memory for staging the segment writes
    char* segment = static_cast<char*>(pool.malloc());
    memset(segment, 0, pool.get_requested_size());
    BackupStorage::Handle* handle;
    try {
        // Reserve the space for this on disk
        handle = storage.allocate(masterId, segmentId);
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
 * Begin reading of segment from disk to memory. Marks the segment
 * CLOSED (immutable) as well.
 * Call getSegment() to block until the load completes.
 */
void
BackupServer::SegmentInfo::startLoading()
{
    assert(state == OPEN || state == CLOSED);
    state = CLOSED;
    if (inMemory() || isLoading())
        return;
    char* segment = static_cast<char*>(pool.malloc());
    try {
        segmentSync.reset(
            storage.getSegment(storageHandle, segment));
    } catch (...) {
        pool.free(segment);
        segment = NULL;
        throw;
    }
    this->segment = segment;
}

// --- BackupServer::RecoverySegmentBuilder ---

/**
 * Constructs a RecoverySegmentBuilder which handles recovery
 * segment construction for a single recovery.
 * Makes a copy of each of the arguments as a thread local copy
 * for use as the thread runs.
 *
 * \param infos
 *      Pointers to SegmentInfo which will be loaded from storage
 *      and split into recovery segments.  Notice, the copy here is
 *      only of the pointer vector and not of the SegmentInfo objects.
 *      Sharing of the SegmentInfo objects is controlled as described
 *      in RecoverySegmentBuilder::infos.
 * \param partitions
 *      The partitions among which the segment should be split for recovery.
 * \param segmentSize
 *      The size of the segments stored for each of the #infos.
 */
BackupServer::RecoverySegmentBuilder::RecoverySegmentBuilder(
        const vector<SegmentInfo*>& infos,
        const ProtoBuf::Tablets& partitions,
        const uint32_t segmentSize)
    : infos(infos)
    , partitions(partitions)
    , segmentSize(segmentSize)
{
}

/**
 * In order, load each of the segments into memory, meanwhile constructing
 * the recovery segments for the previously loaded segment.
 *
 * Notice this runs in a separate thread and maintains exclusive access to the
 * SegmentInfo objects because the main thread has set #lockedForRecovery.  As
 * the recovery segments for each SegmentInfo are constructed ownership of the
 * SegmentInfo is returned to the main thread by resetting #lockedForRecovery.
 */
void
BackupServer::RecoverySegmentBuilder::operator()()
{
    LOG(DEBUG, "Building recovery segments on new thread");
    if (infos.empty())
        return;

    SegmentInfo* loadingInfo = infos[0];
    SegmentInfo* buildingInfo = NULL;

    // Bring the first segment into memory
    loadingInfo->getSegment();

    for (uint32_t i = 1;; i++) {
        buildingInfo = loadingInfo;
        if (i < infos.size()) {
            loadingInfo = infos[i];
            LOG(DEBUG, "Starting load of %uth segment", i);
            loadingInfo->startLoading();
        }
        buildingInfo->buildRecoverySegments(partitions, segmentSize);
        LOG(DEBUG, "Done building recovery segments for %u", i - 1);
        if (i == infos.size())
            return;
        loadingInfo->getSegment();
        LOG(DEBUG, "%uth segment loaded", i);
    }
    LOG(DEBUG, "Done building recovery segments, thread exiting");
}


// --- BackupServer ---

/**
 * Create a BackupServer which is ready to run().
 *
 * \param config
 *      Settings for this instance. The caller guarantees that config will
 *      exist for the duration of this BackupServer's lifetime.
 * \param storage
 *      The storage backend used to persist segments.
 */
BackupServer::BackupServer(const Config& config,
                           BackupStorage& storage)
    : config(config)
    , coordinator(config.coordinatorLocator.c_str())
    , serverId(0)
    , pool(storage.getSegmentSize())
    , segments()
    , segmentSize(storage.getSegmentSize())
    , storage(storage)
    , bytesWritten(0)
{
}

/**
 * Flush open segments to disk and shutdown.
 */
BackupServer::~BackupServer()
{
    // Clean up any extra SegmentInfos laying around
    // but don't free them; perhaps we want to load them up
    // on a cold start.  The infos destructor will ensure the
    // segment is properly stored before termination.
    foreach (const SegmentsMap::value_type& value, segments) {
        SegmentInfo* info = value.second;
        delete info;
    }
    segments.clear();
}

/// Returns the serverId granted to this backup by the coordinator.
uint64_t
BackupServer::getServerId() const
{
    return serverId;
}

/**
 * Accept and dispatch RPCs until the end of time.
 */
void __attribute__ ((noreturn))
BackupServer::run()
{
    serverId = coordinator.enlistServer(BACKUP, config.localLocator);
    LOG(NOTICE, "My server ID is %lu", serverId);
    while (true)
        handleRpc<BackupServer>();
}

// - private -

/**
 * Close the specified segment to permanent storage and free up in memory
 * resources.
 *
 * After this writeSegment() cannot be called for this segment id any
 * more.  The segment will be restored on recovery unless the client later
 * calls freeSegment() on it and will appear to be closed to the recoverer.
 *
 * \param masterId
 *      The serverId of the master whose segment is being closed.
 * \param segmentId
 *      The segmentId of the segment which is being closed.
 *
 * \throw BackupBadSegmentIdException
 *      If this segmentId is unknown for this master or this segment
 *      is not open.
 */
void
BackupServer::closeSegment(uint64_t masterId, uint64_t segmentId)
{
    SegmentInfo* info = findSegmentInfo(masterId, segmentId);
    if (!info)
        throw BackupBadSegmentIdException(HERE);

    info->close();
}

// See Server::dispatch.
void
BackupServer::dispatch(RpcType type, Transport::ServerRpc& rpc,
                       Responder& responder)
{
    switch (type) {
        case BackupFreeRpc::type:
            callHandler<BackupFreeRpc, BackupServer,
                        &BackupServer::freeSegment>(rpc);
            break;
        case BackupGetRecoveryDataRpc::type:
            callHandler<BackupGetRecoveryDataRpc, BackupServer,
                        &BackupServer::getRecoveryData>(rpc);
            break;
        case PingRpc::type:
            callHandler<PingRpc, Server,
                        &Server::ping>(rpc);
            break;
        case BackupStartReadingDataRpc::type:
            callHandler<BackupStartReadingDataRpc, BackupServer,
                        &BackupServer::startReadingData>(rpc, responder);
            break;
        case BackupWriteRpc::type:
            callHandler<BackupWriteRpc, BackupServer,
                        &BackupServer::writeSegment>(rpc);
            break;
        default:
            throw UnimplementedRequestError(HERE);
    }
}

/**
 * Removes the specified segment from permanent storage and releases
 * any in memory copy if it exists.
 *
 * After this call completes the segment number segmentId will no longer
 * be in permanent storage and will not be recovered during recovery.
 *
 * This is a no-op if the backup is unaware of this segment.
 *
 * \param reqHdr
 *      Header of the Rpc request containing the segment number to free.
 * \param respHdr
 *      Header for the Rpc response.
 * \param rpc
 *      The Rpc being serviced.
 */
void
BackupServer::freeSegment(const BackupFreeRpc::Request& reqHdr,
                          BackupFreeRpc::Response& respHdr,
                          Transport::ServerRpc& rpc)
{
    LOG(DEBUG, "Handling: %s %lu %lu",
        __func__, reqHdr.masterId, reqHdr.segmentId);

    SegmentsMap::iterator it =
        segments.find(MasterSegmentIdPair(reqHdr.masterId, reqHdr.segmentId));
    if (it == segments.end()) {
        LOG(WARNING, "Master tried to free non-existent segment: <%lu,%lu>",
            reqHdr.masterId, reqHdr.segmentId);
        return;
    }

    SegmentInfo *info = it->second;
    info->free();
    segments.erase(it);
    delete info;
}

/**
 * Find SegmentInfo for a segment or NULL if we don't know about it.
 *
 * \param masterId
 *      The master id of the master of the segment whose info is being sought.
 * \param segmentId
 *      The segment id of the segment whose info is being sought.
 * \return
 *      A pointer to the SegmentInfo for the specified segment or NULL if
 *      none exists.
 */
BackupServer::SegmentInfo*
BackupServer::findSegmentInfo(uint64_t masterId, uint64_t segmentId)
{
    SegmentsMap::iterator it =
        segments.find(MasterSegmentIdPair(masterId, segmentId));
    if (it == segments.end())
        return NULL;
    return it->second;
}


/**
 * Return the data for a particular tablet that was recovered by a call
 * to startReadingData().
 *
 * \param reqHdr
 *      Header of the Rpc request which contains the Rpc arguments.
 * \param respHdr
 *      Header for the Rpc response containing a count of RecoveredObjects being
 *      returned.
 * \param rpc
 *      The Rpc being serviced.  A back-to-back list of RecoveredObjects
 *      follows the respHdr in this buffer of length
 *      respHdr->recoveredObjectCount.
 *
 * \throw BackupBadSegmentIdException
 *      If the segment has not had recovery started for it (startReadingData()
 *      must have been called for its corresponding master id).
 */
void
BackupServer::getRecoveryData(const BackupGetRecoveryDataRpc::Request& reqHdr,
                              BackupGetRecoveryDataRpc::Response& respHdr,
                              Transport::ServerRpc& rpc)
{
    LOG(DEBUG, "getRecoveryData masterId %lu, segmentId %lu, partitionId %lu",
        reqHdr.masterId, reqHdr.segmentId, reqHdr.partitionId);

    SegmentInfo* info = findSegmentInfo(reqHdr.masterId, reqHdr.segmentId);
    if (!info) {
        LOG(WARNING, "Asked for bad segment <%lu,%lu>",
            reqHdr.masterId, reqHdr.segmentId);
        throw BackupBadSegmentIdException(HERE);
    }

    info->appendRecoverySegment(reqHdr.partitionId, rpc.replyPayload);

    LOG(DEBUG, "getRecoveryData complete");
}

/**
 * Predicate to test if an entry should be sent back to a recovering master
 * on a getRecoveryData() request.
 *
 * \param type
 *      The LogEntryType of the SegmentEntry being considered.
 * \param data
 *      A pointer to the data follow the SegmentEntry.  Used to extract
 *      table and object ids from interesting entries for comparison
 *      to the tablet ranges.
 * \param tablets
 *      A set of table id, object start id, object end id tuples that
 *      are applied to this log entry as a select filter.
 */
bool
BackupServer::keepEntry(const LogEntryType type,
                        const void* data,
                        const ProtoBuf::Tablets& tablets) const
{
    uint64_t tableId = 0;
    uint64_t objectId = 0;

    const Object* object =
        reinterpret_cast<const Object*>(data);
    const ObjectTombstone* tombstone =
        reinterpret_cast<const ObjectTombstone*>(data);
    switch (type) {
      case LOG_ENTRY_TYPE_SEGHEADER:
      case LOG_ENTRY_TYPE_SEGFOOTER:
        return false;
      case LOG_ENTRY_TYPE_OBJ:
        tableId = object->table;
        objectId = object->id;
        break;
      case LOG_ENTRY_TYPE_OBJTOMB:
        tableId = tombstone->table;
        objectId = tombstone->id;
        break;
      case LOG_ENTRY_TYPE_LOGDIGEST:
      case LOG_ENTRY_TYPE_INVALID:
      case LOG_ENTRY_TYPE_UNINIT: // causes break, see getRecoveryData().
        return true;
    }

    for (int i = 0; i < tablets.tablet_size(); i++) {
        const ProtoBuf::Tablets::Tablet& tablet(tablets.tablet(i));
        if (tablet.table_id() == tableId &&
            (tablet.start_object_id() <= objectId &&
             tablet.end_object_id() >= objectId)) {
            return true;
        }
    }

    return false;
}

/**
 * Allocate space to receive backup writes for a segment.  If this call
 * succeeds the Backup must not reject subsequest writes to this segment for
 * lack of space.  This segment is guaranteed to be returned to a master
 * recovering the the master this segment belongs to and will appear to be
 * open.
 *
 * The caller must ensure that this masterId,segmentId pair is unique and
 * hasn't been used before on this backup or any other.
 *
 * The storage space remains in use until the master calls freeSegment() or
 * until the backup crashes.
 *
 * \param masterId
 *      The server id of the master from which the segment data will come.
 * \param segmentId
 *      The segment id of the segment data to be written to this backup.
 * \throw BackupSegmentAlreadyOpenException
 *      If this segment is already open on this backup.
 */
void
BackupServer::openSegment(uint64_t masterId, uint64_t segmentId)
{
    LOG(DEBUG, "Handling: %s %lu %lu",
        __func__, masterId, segmentId);

    SegmentInfo* info = findSegmentInfo(masterId, segmentId);
    if (info)
        throw BackupSegmentAlreadyOpenException(HERE);

    try {
        std::unique_ptr<SegmentInfo> info(
            new SegmentInfo(storage, pool, masterId, segmentId));
        segments[MasterSegmentIdPair(masterId, segmentId)] = info.get();
        info->open();
        info.release();
    } catch (...) {
        segments.erase(MasterSegmentIdPair(masterId, segmentId));
        throw;
    }
}

/**
 * Returns true if left points to a SegmentInfo with a lesser
 * segmentId than that pointed to by right.
 *
 * \param left
 *      A pointer to a SegmentInfo to be compared to #right.
 * \param right
 *      A pointer to a SegmentInfo to be compared to #left.
 */
bool
BackupServer::segmentInfoLessThan(const SegmentInfo* left,
                                  const SegmentInfo* right)
{
    return *left < *right;
}

/**
 * Begin reading disk data for a Master and bucketing the objects in the
 * Segments according to a requested TabletMap, returning a list of backed
 * up Segments this backup has for the Master.
 *
 * \param reqHdr
 *      Header of the Rpc request which contains the Rpc arguments.
 * \param respHdr
 *      Header for the Rpc response containing a count of segmentIds being
 *      returned.
 * \param rpc
 *      The Rpc being serviced.  A back-to-back list of uint64_t segmentIds
 *      follows the respHdr in this buffer of length respHdr->segmentIdCount.
 * \param responder
 *      Functor to respond to the RPC before returning from this method.
 */
void
BackupServer::startReadingData(const BackupStartReadingDataRpc::Request& reqHdr,
                               BackupStartReadingDataRpc::Response& respHdr,
                               Transport::ServerRpc& rpc,
                               Responder& responder)
{
    LOG(DEBUG, "Handling: %s %lu", __func__, reqHdr.masterId);

    ProtoBuf::Tablets partitions;
    ProtoBuf::parseFromResponse(rpc.recvPayload, sizeof(reqHdr),
                                reqHdr.partitionsLength, partitions);

    vector<SegmentInfo*> segmentsToFilter;
    uint32_t segmentIdCount = 0;
    for (SegmentsMap::iterator it = segments.begin();
         it != segments.end(); it++)
    {
        uint64_t masterId = it->first.masterId;
        if (masterId == reqHdr.masterId) {
            uint64_t* segmentId = new(&rpc.replyPayload, APPEND) uint64_t;
            *segmentId = it->first.segmentId;
            segmentsToFilter.push_back(it->second);
            it->second->setLockedForRecovery();
            LOG(DEBUG, "Crashed master %lu had segment %lu",
                masterId, *segmentId);
            segmentIdCount++;
        }
    }
    respHdr.segmentIdCount = segmentIdCount;
    LOG(DEBUG, "Sending %u segment ids for this master", segmentIdCount);

    responder();

    std::sort(segmentsToFilter.begin(),
              segmentsToFilter.end(),
              &segmentInfoLessThan);

    RecoverySegmentBuilder builder(segmentsToFilter, partitions, segmentSize);
    boost::thread builderThread(builder);
    LOG(DEBUG, "Kicked off building recovery segments; "
               "main thread going back to dispatching requests");
}

/**
 * Store an opaque string of bytes in a currently open segment on
 * this backup server.  This data is guaranteed to be considered on recovery
 * of the master to which this segment belongs to the best of the ability of
 * this BackupServer and will appear open unless a closeSegment() is called
 * on it beforehand.
 *
 * \param reqHdr
 *      Header of the Rpc request which contains the Rpc arguments except
 *      the data to be written.
 * \param respHdr
 *      Header for the Rpc response.
 * \param rpc
 *      The Rpc being serviced, used for access to the opaque bytes to
 *      be written which follow reqHdr.
 *
 * \throw BackupSegmentOverflowException
 *      If the write request is beyond the end of the segment.
 * \throw BackupBadSegmentIdException
 *      If the segment is not open (see openSegment()).
 */
void
BackupServer::writeSegment(const BackupWriteRpc::Request& reqHdr,
                           BackupWriteRpc::Response& respHdr,
                           Transport::ServerRpc& rpc)
{
    if (reqHdr.flags == BackupWriteRpc::OPEN ||
        reqHdr.flags == BackupWriteRpc::OPENCLOSE)
        openSegment(reqHdr.masterId, reqHdr.segmentId);

    SegmentInfo* info = findSegmentInfo(reqHdr.masterId, reqHdr.segmentId);
    if (!info || !info->isOpen())
        throw BackupBadSegmentIdException(HERE);

    // Need to check all three conditions because overflow is possible
    // on the addition
    if (reqHdr.length > segmentSize ||
        reqHdr.offset > segmentSize ||
        reqHdr.length + reqHdr.offset > segmentSize)
        throw BackupSegmentOverflowException(HERE);

    rpc.recvPayload.copy(sizeof(reqHdr), reqHdr.length,
                         &info->getSegment()[reqHdr.offset]);
    bytesWritten += reqHdr.length;

    if (reqHdr.flags == BackupWriteRpc::CLOSE ||
        reqHdr.flags == BackupWriteRpc::OPENCLOSE)
        closeSegment(reqHdr.masterId, reqHdr.segmentId);
}

} // namespace RAMCloud

