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
#include <boost/lambda/lambda.hpp>
#include <boost/lambda/bind.hpp>

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
 * Construct a SegmentInfo to manage a segment.
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
 * \param segmentSize
 *      The number of bytes contained in this segment.
 * \param primary
 *      True if this is the primary copy of this segment for the master
 *      who stored it.  Determines whether recovery segments are built
 *      at recovery start or on demand.
 */
BackupServer::SegmentInfo::SegmentInfo(BackupStorage& storage,
                                       ThreadSafePool& pool,
                                       uint64_t masterId,
                                       uint64_t segmentId,
                                       uint32_t segmentSize,
                                       bool primary)
    : masterId(masterId)
    , primary(primary)
    , segmentId(segmentId)
    , mutex()
    , condition()
    , recoveryException()
    , recoveryPartitions()
    , recoverySegments(NULL)
    , recoverySegmentsLength()
    , rightmostWrittenOffset(0)
    , segment()
    , segmentSize(segmentSize)
    , state(UNINIT)
    , storageHandle()
    , pool(pool)
    , storage(storage)
    , storageThreadCount(0)
{
}

/**
 * Store any open segments to storage and then release all resources
 * associate with them except permanent storage.
 */
BackupServer::SegmentInfo::~SegmentInfo()
{
    Lock lock(mutex);
    waitForOngoingOps(lock);

    if (state == OPEN) {
        LOG(NOTICE, "Backup shutting down with open segment <%lu,%lu>, "
            "closing out to storage", masterId, segmentId);
        state = CLOSED;
        storage.putSegment(storageHandle, segment);
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
    // recoveryException cleaned up by scoped_ptr
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
    Lock lock(mutex);

    if (state != RECOVERING) {
        LOG(WARNING, "Asked for segment <%lu,%lu> which isn't recovering",
            masterId, segmentId);
        throw BackupBadSegmentIdException(HERE);
    }

    if (!primary) {
        if (!isRecovered() && !recoveryException) {
            LOG(DEBUG, "Requested segment <%lu,%lu> is secondary, "
                "starting build of recovery segments now", masterId, segmentId);
            waitForOngoingOps(lock);
            LoadOp io(*this);
            ++storageThreadCount;
            boost::thread _(io);
            lock.unlock();
            buildRecoverySegments(*recoveryPartitions);
            lock.lock();
        }
    }

    uint64_t start = rdtsc();
    while (!isRecovered() && !recoveryException) {
        LOG(DEBUG, "<%lu,%lu> not yet recovered; waiting", masterId, segmentId);
        condition.wait(lock);
    }
    assert(state == RECOVERING);
#if !TESTING
    LOG(DEBUG, "Done spinning for segment <%lu,%lu>, waited %lu ms", masterId,
        segmentId, cyclesToNanoseconds(rdtsc() - start) / 1000 / 1000);
#endif

    if (recoveryException) {
        auto e = SegmentRecoveryFailedException(*recoveryException);
        recoveryException.reset();
        throw e;
    }

    if (partitionId >= recoverySegmentsLength) {
        LOG(WARNING, "Asked for recovery segment %lu from segment <%lu,%lu> "
                     "but there are only %u partitions",
            partitionId, masterId, segmentId, recoverySegmentsLength);
        throw BackupBadSegmentIdException(HERE);
    }

#ifdef PERF_DEBUG_RECOVERY_CONTIGUOUS_RECOVERY_SEGMENTS
    auto& bufLen = recoverySegments[partitionId];
    Buffer::Chunk::appendToBuffer(&buffer, bufLen.first, bufLen.second);
#else
    for (Buffer::Iterator it(recoverySegments[partitionId]);
         !it.isDone(); it.next())
    {
        Buffer::Chunk::appendToBuffer(&buffer, it.getData(), it.getLength());
    }
#endif
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
 * them according to #partitions.  After completion and setting
 * #recoverySegments #condition is notified to wake up threads waiting
 * for recovery segments for this segment.
 *
 * \param partitions
 *      A set of tablets grouped into partitions which are used to divide
 *      the stored segment data into recovery segments.
 */
void
BackupServer::SegmentInfo::buildRecoverySegments(
    const ProtoBuf::Tablets& partitions)
{
    Lock lock(mutex);
    assert(state == RECOVERING);

    waitForOngoingOps(lock);

    assert(inMemory());

    uint64_t start = rdtsc();

    recoveryException.reset();

    uint64_t partitionCount = 0;
    for (int i = 0; i < partitions.tablet_size(); ++i) {
        partitionCount = std::max(partitionCount,
                                  partitions.tablet(i).user_data() + 1);
    }
    LOG(NOTICE, "Building %lu recovery segments for %lu",
        partitionCount, segmentId);

#ifdef PERF_DEBUG_RECOVERY_CONTIGUOUS_RECOVERY_SEGMENTS
    recoverySegments = new pair<char[Segment::SEGMENT_SIZE], uint32_t>
                                                [partitionCount];
    for (uint32_t i = 0; i < partitionCount; ++i)
        recoverySegments[i].second = 0;
#else
    recoverySegments = new Buffer[partitionCount];
#endif
    recoverySegmentsLength = partitionCount;

    try {
        for (SegmentIterator it(segment, segmentSize);
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
#ifdef PERF_DEBUG_RECOVERY_CONTIGUOUS_RECOVERY_SEGMENTS
            auto& bufLen = recoverySegments[*partitionId];
            assert(bufLen.second + len <= Segment::SEGMENT_SIZE);
            void *out = bufLen.first + bufLen.second;
            bufLen.second += len;
#else
            void *out = new(&recoverySegments[*partitionId], APPEND) char[len];
#endif
            memcpy(out, entry, len);
        }
#if TESTING
        for (uint64_t i = 0; i < recoverySegmentsLength; ++i) {
            LOG(DEBUG, "Recovery segment for <%lu,%lu> partition %lu is %u B",
#ifdef PERF_DEBUG_RECOVERY_CONTIGUOUS_RECOVERY_SEGMENTS
                masterId, segmentId, i, recoverySegments[i].second);
#else
                masterId, segmentId, i, recoverySegments[i].getTotalLength());
#endif
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
        masterId, segmentId,
        cyclesToNanoseconds(rdtsc() - start) / 1000 / 1000);
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
BackupServer::SegmentInfo::close()
{
    Lock lock(mutex);

    if (state != OPEN)
        throw BackupBadSegmentIdException(HERE);

    state = CLOSED;
    rightmostWrittenOffset = BYTES_WRITTEN_CLOSED;

    assert(storageHandle);
    StoreOp io(*this);
    ++storageThreadCount;
    boost::thread _(io);
}

/**
 * Release all resources related to this segment including storage.
 * This will block for all outstanding storage operations before
 * freeing the storage resources.
 */
void
BackupServer::SegmentInfo::free()
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
BackupServer::SegmentInfo::open()
{
    Lock lock(mutex);
    assert(state == UNINIT);
    // Get memory for staging the segment writes
    char* segment = static_cast<char*>(pool.malloc());
    memset(segment, 0, segmentSize);
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
 * CLOSED (immutable) as well.  Once the segment is in memory
 * #segment will be set and waiting threads will be notified via
 * #condition.
 */
void
BackupServer::SegmentInfo::startLoading()
{
    Lock lock(mutex);
    waitForOngoingOps(lock);

    LoadOp io(*this);
    ++storageThreadCount;
    boost::thread _(io);
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
 */
void
BackupServer::SegmentInfo::write(Buffer& src,
                                 uint32_t srcOffset,
                                 uint32_t length,
                                 uint32_t destOffset)
{
    Lock lock(mutex);
    assert(state == OPEN && inMemory());
    src.copy(srcOffset, length, &segment[destOffset]);
    rightmostWrittenOffset = std::max(rightmostWrittenOffset,
                                      destOffset + length);
}

/**
 * Scan the current Segment for a LogDigest. If it exists, return a pointer
 * to it. The out parameter can be used to supply the number of bytes it
 * occupies.
 *
 * \param[out] byteLength
 *      Optional out parameter for the number of bytes the LogDigest
 *      occupies.
 * \return
 *      NULL if no LogDigest exists in the Segment, else a valid pointer.
 */
const void*
BackupServer::SegmentInfo::getLogDigest(uint32_t* byteLength)
{
    // If the Segment is malformed somehow, just ignore it. The
    // coordinator will have to deal.
    try {
        SegmentIterator si(segment, segmentSize, true);
        while (!si.isDone()) {
            if (si.getType() == LOG_ENTRY_TYPE_LOGDIGEST) {
                if (byteLength != NULL)
                    *byteLength = si.getLength();
                return si.getPointer();
            }
            si.next();
        }
    } catch (SegmentIteratorException& e) {
        LOG(WARNING, "SegmentIterator constructor failed: %s", e.str().c_str());
    }
    return NULL;
}

// --- BackupServer::LoadOp ---

/**
 * \param info
 *      The SegmentInfo which should be loaded from storage and placed
 *      into newly a pool allocated buffer.
 */
BackupServer::LoadOp::LoadOp(SegmentInfo& info)
    : info(info)
{
}

// See BackupServer::LoadOp.
void
BackupServer::LoadOp::operator()()
{
    SegmentInfo::Lock lock(info.mutex);
    ReferenceDecrementer<int> _(info.storageThreadCount);

    LOG(DEBUG, "Loading segment <%lu,%lu>", info.masterId, info.segmentId);
    if (info.inMemory()) {
        LOG(DEBUG, "Already in memory, skipping load on <%lu,%lu>",
            info.masterId, info.segmentId);
        info.condition.notify_all();
        return;
    }

    char* segment = static_cast<char*>(info.pool.malloc());
    try {
        info.storage.getSegment(info.storageHandle, segment);
    } catch (...) {
        info.pool.free(segment);
        segment = NULL;
        info.condition.notify_all();
        throw;
    }
    info.segment = segment;
    info.condition.notify_all();
}

// --- BackupServer::StoreOp ---

/**
 * \param info
 *      The SegmentInfo which should be stored to storage and released
 *      from memory.
 */
BackupServer::StoreOp::StoreOp(SegmentInfo& info)
    : info(info)
{
}

// See BackupServer::StoreOp.
void
BackupServer::StoreOp::operator()()
{
    SegmentInfo::Lock lock(info.mutex);
    ReferenceDecrementer<int> _(info.storageThreadCount);
    LOG(DEBUG, "Storing segment <%lu,%lu>", info.masterId, info.segmentId);
    try {
        info.storage.putSegment(info.storageHandle, info.segment);
    } catch (...) {
        LOG(WARNING, "Problem storing segment <%lu,%lu>",
            info.masterId, info.segmentId);
        info.condition.notify_all();
        throw;
    }
    info.pool.free(info.segment);
    info.segment = NULL;
    LOG(DEBUG, "Done storing segment <%lu,%lu>", info.masterId, info.segmentId);
    info.condition.notify_all();
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
 * \param recoveryThreadCount
 *      Reference to an atomic count for tracking number of running recoveries.
 */
BackupServer::RecoverySegmentBuilder::RecoverySegmentBuilder(
        const vector<SegmentInfo*>& infos,
        const ProtoBuf::Tablets& partitions,
        AtomicInt& recoveryThreadCount)
    : infos(infos)
    , partitions(partitions)
    , recoveryThreadCount(recoveryThreadCount)
{
}

/**
 * In order, load each of the segments into memory, meanwhile constructing
 * the recovery segments for the previously loaded segment.
 *
 * Notice this runs in a separate thread and maintains exclusive access to the
 * SegmentInfo objects using #SegmentInfo::mutex.  As
 * the recovery segments for each SegmentInfo are constructed ownership of the
 * SegmentInfo is released and #SegmentInfo::condition is notified to wake
 * up any threads waiting for the recovery segments.
 */
void
BackupServer::RecoverySegmentBuilder::operator()()
{
    ReferenceDecrementer<AtomicInt> _(recoveryThreadCount);
    LOG(DEBUG, "Building recovery segments on new thread");

    if (infos.empty())
        return;

    SegmentInfo* loadingInfo = infos[0];
    SegmentInfo* buildingInfo = NULL;

    // Bring the first segment into memory
    loadingInfo->startLoading();

    for (uint32_t i = 1;; i++) {
        assert(buildingInfo != loadingInfo);
        buildingInfo = loadingInfo;
        if (i < infos.size()) {
            loadingInfo = infos[i];
            LOG(DEBUG, "Starting load of %uth segment", i);
            loadingInfo->startLoading();
        }
        buildingInfo->buildRecoverySegments(partitions);
        LOG(DEBUG, "Done building recovery segments for %u", i - 1);
        if (i == infos.size())
            break;
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
    , recoveryThreadCount{0}
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
    int lastThreadCount = 0;
    while (recoveryThreadCount > 0) {
        if (lastThreadCount != recoveryThreadCount) {
            LOG(WARNING, "Waiting for recovery threads to terminate before "
                "deleting BackupServer, %d threads "
                "still running", static_cast<int>(recoveryThreadCount));
            lastThreadCount = recoveryThreadCount;
        }
    }

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
 * Returns true if left points to a SegmentInfo with a lesser
 * segmentId than that pointed to by right.
 *
 * \param left
 *      A pointer to a SegmentInfo to be compared to #right.
 * \param right
 *      A pointer to a SegmentInfo to be compared to #left.
 */
bool
BackupServer::segmentInfoLessThan(SegmentInfo* left,
                                  SegmentInfo* right)
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

    uint32_t logDigestLastId = 0;
    uint32_t logDigestBytes = 0;
    const void* logDigestPtr = NULL;

    vector<SegmentInfo*> primarySegments;
    vector<SegmentInfo*> secondarySegments;

    for (SegmentsMap::iterator it = segments.begin();
         it != segments.end(); it++)
    {
        uint64_t masterId = it->first.masterId;
        SegmentInfo* info = it->second;
        if (masterId == reqHdr.masterId) {
            (info->primary ?
                primarySegments :
                secondarySegments).push_back(info);

            // Obtain the LogDigest from the highest Segment Id of any
            // #OPEN Segment.
            uint64_t segmentId = info->segmentId;
            if (info->isOpen() && segmentId >= logDigestLastId) {
                const void* newDigest = NULL;
                uint32_t newDigestBytes;
                newDigest = info->getLogDigest(&newDigestBytes);
                if (newDigest != NULL) {
                    logDigestLastId = segmentId;
                    logDigestBytes = newDigestBytes;
                    logDigestPtr = newDigest;
                    LOG(DEBUG, "Segment %lu's LogDigest queued for response",
                        segmentId);
                } else {
                    LOG(WARNING, "Segment %lu missing LogDigest", segmentId);
                }
            }
        }
    }

    // sort the primary entries
    std::sort(primarySegments.begin(),
              primarySegments.end(),
              &segmentInfoLessThan);
    std::reverse(primarySegments.begin(), primarySegments.end());
    // sort the secondary entries
    std::sort(secondarySegments.begin(),
              secondarySegments.end(),
              &segmentInfoLessThan);
    std::reverse(secondarySegments.begin(), secondarySegments.end());

    foreach (auto info, primarySegments) {
        new(&rpc.replyPayload, APPEND) pair<uint64_t, uint32_t>
            (info->segmentId, info->getRightmostWrittenOffset());
        LOG(DEBUG, "Crashed master %lu had segment %lu (primary)",
            info->masterId, info->segmentId);
        info->setRecovering();
    }
    foreach (auto info, secondarySegments) {
        new(&rpc.replyPayload, APPEND) pair<uint64_t, uint32_t>
            (info->segmentId, info->getRightmostWrittenOffset());
        LOG(DEBUG, "Crashed master %lu had segment %lu (secondary), "
            "stored partitions for deferred recovery segment construction",
            info->masterId, info->segmentId);
        info->setRecovering(partitions);
    }
    respHdr.segmentIdCount = primarySegments.size() + secondarySegments.size();
    LOG(DEBUG, "Sending %u segment ids for this master",
        respHdr.segmentIdCount);

    respHdr.logDigestBytes = logDigestBytes;
    if (respHdr.logDigestBytes) {
        void* out = new(&rpc.replyPayload, APPEND) char[respHdr.logDigestBytes];
        memcpy(out, logDigestPtr, respHdr.logDigestBytes);
        LOG(DEBUG, "Sent %u bytes of LogDigest to master", logDigestBytes);
    }

    responder();

    RecoverySegmentBuilder builder(primarySegments,
                                   partitions,
                                   recoveryThreadCount);
    ++recoveryThreadCount;
    boost::thread builderThread(builder);
    LOG(DEBUG, "Kicked off building recovery segments; "
               "main thread going back to dispatching requests");
}

/**
 * Store an opaque string of bytes in a currently open segment on
 * this backup server.  This data is guaranteed to be considered on recovery
 * of the master to which this segment belongs to the best of the ability of
 * this BackupServer and will appear open unless it is closed
 * beforehand.
 *
 * If BackupWriteRpc::OPEN flag is set then space is allocated to receive
 * backup writes for a segment.  If this call succeeds the Backup must not
 * reject subsequent writes to this segment for lack of space.  The caller must
 * ensure that this masterId,segmentId pair is unique and hasn't been used
 * before on this backup or any other.  The storage space remains in use until
 * the master calls freeSegment(), the segment is involved in a recovery that
 * completes successfully, or until the backup crashes.
 *
 * If BackupWriteRpc::CLOSE flag is set then once this call succeeds the
 * segment will be considered closed and immutable and the Backup will
 * use this as a hint to move the segment to appropriate storage.
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
 *      If the segment is not open.
 */
void
BackupServer::writeSegment(const BackupWriteRpc::Request& reqHdr,
                           BackupWriteRpc::Response& respHdr,
                           Transport::ServerRpc& rpc)
{
    uint64_t masterId = reqHdr.masterId;
    uint64_t segmentId = reqHdr.segmentId;

    SegmentInfo* info = findSegmentInfo(masterId, segmentId);

    if (reqHdr.flags & BackupWriteRpc::OPEN) {
        if (info)
            throw BackupSegmentAlreadyOpenException(HERE);

        try {
            bool primary = reqHdr.flags & BackupWriteRpc::PRIMARY;
            info = new SegmentInfo(storage, pool,
                                   masterId, segmentId, segmentSize, primary);
            segments[MasterSegmentIdPair(masterId, segmentId)] = info;
            info->open();
        } catch (...) {
            segments.erase(MasterSegmentIdPair(masterId, segmentId));
            if (info)
                delete info;
            throw;
        }
    }

    if (!info || !info->isOpen())
        throw BackupBadSegmentIdException(HERE);

    // Need to check all three conditions because overflow is possible
    // on the addition
    if (reqHdr.length > segmentSize ||
        reqHdr.offset > segmentSize ||
        reqHdr.length + reqHdr.offset > segmentSize)
        throw BackupSegmentOverflowException(HERE);

    info->write(rpc.recvPayload, sizeof(reqHdr),
                reqHdr.length, reqHdr.offset);
    bytesWritten += reqHdr.length;

    if (reqHdr.flags & BackupWriteRpc::CLOSE)
        info->close();
}

} // namespace RAMCloud

