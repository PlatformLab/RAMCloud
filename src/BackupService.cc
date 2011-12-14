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

#include <boost/thread.hpp>

#include "BackupService.h"
#include "BackupStorage.h"
#include "Buffer.h"
#include "ClientException.h"
#include "Cycles.h"
#include "Log.h"
#include "ShortMacros.h"
#include "LogTypes.h"
#include "RecoverySegmentIterator.h"
#include "Rpc.h"
#include "Segment.h"
#include "SegmentIterator.h"
#include "Status.h"
#include "TransportManager.h"

namespace RAMCloud {

namespace {
/**
 * The TSC reading at the start of the last recovery.
 * Used to update #RawMetrics.backup.readingDataTicks.
 */
uint64_t recoveryStart;
} // anonymous namespace

// --- BackupService::SegmentInfo ---

/**
 * Construct a SegmentInfo to manage a segment.
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
BackupService::SegmentInfo::SegmentInfo(BackupStorage& storage,
                                        ThreadSafePool& pool,
                                        IoScheduler& ioScheduler,
                                        ServerId masterId,
                                        uint64_t segmentId,
                                        uint32_t segmentSize,
                                        bool primary)
    : masterId(masterId)
    , primary(primary)
    , segmentId(segmentId)
    , mutex()
    , condition()
    , ioScheduler(ioScheduler)
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
    , storageOpCount(0)
{
}

/**
 * Store any open segments to storage and then release all resources
 * associate with them except permanent storage.
 */
BackupService::SegmentInfo::~SegmentInfo()
{
    Lock lock(mutex);
    waitForOngoingOps(lock);

    if (state == OPEN) {
        LOG(NOTICE, "Backup shutting down with open segment <%lu,%lu>, "
            "closing out to storage", *masterId, segmentId);
        state = CLOSED;
        CycleCounter<RawMetric> _(&metrics->backup.storageWriteTicks);
        ++metrics->backup.storageWriteCount;
        metrics->backup.storageWriteBytes += segmentSize;
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
BackupService::SegmentInfo::appendRecoverySegment(uint64_t partitionId,
                                                  Buffer& buffer)
{
    Lock lock(mutex, boost::try_to_lock_t());
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

    for (Buffer::Iterator it(recoverySegments[partitionId]);
         !it.isDone(); it.next())
    {
        Buffer::Chunk::appendToBuffer(&buffer, it.getData(), it.getLength());
    }

    LOG(DEBUG, "appendRecoverySegment <%lu,%lu>", *masterId, segmentId);
    return STATUS_OK;
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
Tub<uint64_t>
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
        tableId = object->id.tableId;
        objectId = object->id.objectId;
    } else { // LOG_ENTRY_TYPE_OBJTOMB:
        tableId = tombstone->id.tableId;
        objectId = tombstone->id.objectId;
    }

    Tub<uint64_t> ret;
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
BackupService::SegmentInfo::buildRecoverySegments(
    const ProtoBuf::Tablets& partitions)
{
    Lock lock(mutex);
    assert(state == RECOVERING);

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

    recoverySegments = new Buffer[partitionCount];
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
            Tub<uint64_t> partitionId = whichPartition(it.getType(),
                                                             it.getPointer(),
                                                             partitions);
            if (!partitionId)
                continue;
            const SegmentEntry* entry = reinterpret_cast<const SegmentEntry*>(
                    reinterpret_cast<const char*>(it.getPointer()) -
                    sizeof(*entry));
            const uint32_t len = (downCast<uint32_t>(sizeof(*entry)) +
                                  it.getLength());
            void *out = new(&recoverySegments[*partitionId], APPEND) char[len];
            memcpy(out, entry, len);
        }
#if TESTING
        for (uint64_t i = 0; i < recoverySegmentsLength; ++i) {
            LOG(DEBUG, "Recovery segment for <%lu,%lu> partition %lu is %u B",
                *masterId, segmentId, i, recoverySegments[i].getTotalLength());
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
BackupService::SegmentInfo::close()
{
    Lock lock(mutex);

    if (state == CLOSED)
        return;
    else if (state != OPEN)
        throw BackupBadSegmentIdException(HERE);

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
BackupService::SegmentInfo::free()
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
BackupService::SegmentInfo::open()
{
    Lock lock(mutex);
    assert(state == UNINIT);
    // Get memory for staging the segment writes
    char* segment = static_cast<char*>(pool.malloc());
    {
        CycleCounter<RawMetric> q(&metrics->backup.writeClearTicks);
        memset(segment, 0, segmentSize < sizeof(SegmentEntry) ?
                            segmentSize : sizeof(SegmentEntry));
    }
    BackupStorage::Handle* handle;
    try {
        // Reserve the space for this on disk
        handle = storage.allocate(*masterId, segmentId);
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
BackupService::SegmentInfo::startLoading()
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
 */
void
BackupService::SegmentInfo::write(Buffer& src,
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
BackupService::SegmentInfo::getLogDigest(uint32_t* byteLength)
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

// --- BackupService::IoScheduler ---

/**
 * Construct an IoScheduler.  There is just one instance of this
 * with its operator() running in a new thread which is instantated
 * by the BackupService.
 */
BackupService::IoScheduler::IoScheduler()
    : queueMutex()
    , queueCond()
    , loadQueue()
    , storeQueue()
    , running(true)
    , outstandingStores(0)
{
}

/**
 * Dequeue IO requests and process them on a thread separate from the
 * main thread.  Processes just one IO at a time.  Prioritizes loads
 * over stores.
 */
void
BackupService::IoScheduler::operator()()
{
    while (true) {
        SegmentInfo* info = NULL;
        bool isLoad = false;
        {
            Lock lock(queueMutex);
            while (loadQueue.empty() && storeQueue.empty()) {
                if (!running)
                    return;
                queueCond.wait(lock);
            }
            isLoad = !loadQueue.empty();
            if (isLoad) {
                info = loadQueue.front();
                loadQueue.pop();
            } else {
                info = storeQueue.front();
                storeQueue.pop();
            }
        }

        if (isLoad) {
            LOG(DEBUG, "Dispatching load of <%lu,%lu>",
                *info->masterId, info->segmentId);
            doLoad(*info);
        } else {
            LOG(DEBUG, "Dispatching store of <%lu,%lu>",
                *info->masterId, info->segmentId);
            doStore(*info);
        }
    }
}

/**
 * Queue a segment load operation to be done on a separate thread.
 *
 * \param info
 *      The SegmentInfo whose data will be loaded from storage.
 */
void
BackupService::IoScheduler::load(SegmentInfo& info)
{
#ifdef SINGLE_THREADED_BACKUP
    doLoad(info);
#else
    Lock lock(queueMutex);
    loadQueue.push(&info);
    uint32_t count = downCast<uint32_t>(loadQueue.size() + storeQueue.size());
    LOG(DEBUG, "Queued load of <%lu,%lu> (%u segments waiting for IO)",
        *info.masterId, info.segmentId, count);
    queueCond.notify_all();
#endif
}

/**
 * Flush all data to storage.
 * Returns once all dirty buffers have been written to storage.
 */
void
BackupService::IoScheduler::quiesce()
{
    while (outstandingStores > 0) {
        /* pass */;
    }
}

/**
 * Queue a segment store operation to be done on a separate thread.
 *
 * \param info
 *      The SegmentInfo whose data will be stored.
 */
void
BackupService::IoScheduler::store(SegmentInfo& info)
{
#ifdef SINGLE_THREADED_BACKUP
    doStore(info);
#else
    ++outstandingStores;
    Lock lock(queueMutex);
    storeQueue.push(&info);
    uint32_t count = downCast<uint32_t>(loadQueue.size() + storeQueue.size());
    LOG(DEBUG, "Queued store of <%lu,%lu> (%u segments waiting for IO)",
        *info.masterId, info.segmentId, count);
    queueCond.notify_all();
#endif
}

/**
 * Wait for the IO scheduler to finish currently queued requests and
 * return once the scheduler has terminated and its thread is disposed
 * of.
 *
 * \param ioThread
 *      The thread this IoScheduler is running on.  It will be joined
 *      to ensure the scheduler has fully exited before this call
 *      returns.
 */
void
BackupService::IoScheduler::shutdown(boost::thread& ioThread)
{
    {
        Lock lock(queueMutex);
        LOG(DEBUG, "IoScheduler thread exiting");
        uint32_t count = downCast<uint32_t>(loadQueue.size() +
                                            storeQueue.size());
        if (count)
            LOG(DEBUG, "IoScheduler must service %u pending IOs before exit",
                count);
        running = false;
        queueCond.notify_one();
    }
    ioThread.join();
}

// - private -

/**
 * Load a segment from disk into a valid buffer in memory.  Locks
 * the #SegmentInfo::mutex to ensure other operations aren't performed
 * on the segment in the meantime.
 *
 * \param info
 *      The SegmentInfo whose data will be loaded from storage.
 */
void
BackupService::IoScheduler::doLoad(SegmentInfo& info) const
{
#ifndef SINGLE_THREADED_BACKUP
    SegmentInfo::Lock lock(info.mutex);
#endif
    ReferenceDecrementer<int> _(info.storageOpCount);

    LOG(DEBUG, "Loading segment <%lu,%lu>", *info.masterId, info.segmentId);
    if (info.inMemory()) {
        LOG(DEBUG, "Already in memory, skipping load on <%lu,%lu>",
            *info.masterId, info.segmentId);
        info.condition.notify_all();
        return;
    }

    char* segment = static_cast<char*>(info.pool.malloc());
    try {
        CycleCounter<RawMetric> _(&metrics->backup.storageReadTicks);
        ++metrics->backup.storageReadCount;
        metrics->backup.storageReadBytes += info.segmentSize;
        uint64_t startTime = Cycles::rdtsc();
        info.storage.getSegment(info.storageHandle, segment);
        uint64_t transferTime = Cycles::toNanoseconds(Cycles::rdtsc() -
            startTime);
        LOG(DEBUG, "Load of <%lu,%lu> took %lu us (%f MB/s)",
            *info.masterId, info.segmentId,
            transferTime / 1000,
            (info.segmentSize / (1 << 20)) /
            (static_cast<double>(transferTime) / 1000000000lu));
    } catch (...) {
        info.pool.free(segment);
        segment = NULL;
        info.condition.notify_all();
        throw;
    }
    info.segment = segment;
    info.condition.notify_all();
    metrics->backup.readingDataTicks = Cycles::rdtsc() - recoveryStart;
}

/**
 * Store a segment to disk from a valid buffer in memory.  Locks
 * the #SegmentInfo::mutex to ensure other operations aren't performed
 * on the segment in the meantime.
 *
 * \param info
 *      The SegmentInfo whose data will be stored.
 */
void
BackupService::IoScheduler::doStore(SegmentInfo& info) const
{
#ifndef SINGLE_THREADED_BACKUP
    SegmentInfo::Lock lock(info.mutex);
#endif
    ReferenceDecrementer<int> _(info.storageOpCount);

    LOG(DEBUG, "Storing segment <%lu,%lu>", *info.masterId, info.segmentId);
    try {
        CycleCounter<RawMetric> _(&metrics->backup.storageWriteTicks);
        ++metrics->backup.storageWriteCount;
        metrics->backup.storageWriteBytes += info.segmentSize;
        uint64_t startTime = Cycles::rdtsc();
        info.storage.putSegment(info.storageHandle, info.segment);
        uint64_t transferTime = Cycles::toNanoseconds(Cycles::rdtsc() -
            startTime);
        LOG(DEBUG, "Store of <%lu,%lu> took %lu us (%f MB/s)",
            *info.masterId, info.segmentId,
            transferTime / 1000,
            (info.segmentSize / (1 << 20)) /
            (static_cast<double>(transferTime) / 1000000000lu));
    } catch (...) {
        LOG(WARNING, "Problem storing segment <%lu,%lu>",
            *info.masterId, info.segmentId);
        info.condition.notify_all();
        throw;
    }
    info.pool.free(info.segment);
    info.segment = NULL;
    --outstandingStores;
    LOG(DEBUG, "Done storing segment <%lu,%lu>",
        *info.masterId, info.segmentId);
    info.condition.notify_all();
}


// --- BackupService::RecoverySegmentBuilder ---

/**
 * Constructs a RecoverySegmentBuilder which handles recovery
 * segment construction for a single recovery.
 * Makes a copy of each of the arguments as a thread local copy
 * for use as the thread runs.
 *
 * \param context
 *      The context in which the new thread will run (same as that of the
 *      parent thread).
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
BackupService::RecoverySegmentBuilder::RecoverySegmentBuilder(
        Context& context,
        const vector<SegmentInfo*>& infos,
        const ProtoBuf::Tablets& partitions,
        AtomicInt& recoveryThreadCount)
    : context(context)
    , infos(infos)
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
BackupService::RecoverySegmentBuilder::operator()()
{
    Context::Guard scopedContext(context);

    uint64_t startTime = Cycles::rdtsc();
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
    uint64_t totalTime = Cycles::toNanoseconds(Cycles::rdtsc() - startTime);
    LOG(DEBUG, "RecoverySegmentBuilder took %lu ms to filter %lu segments "
               "(%f MB/s)",
        totalTime / 1000 / 1000,
        infos.size(),
        static_cast<double>(Segment::SEGMENT_SIZE * infos.size() / (1 << 20)) /
        static_cast<double>(totalTime) / 1e9);
}


// --- BackupService ---

/**
 * Create a BackupService.
 *
 * \param config
 *      Settings for this instance. The caller guarantees that config will
 *      exist for the duration of this BackupService's lifetime.
 * \param storage
 *      The storage backend used to persist segments.
 */
BackupService::BackupService(const Config& config,
                             BackupStorage& storage)
    : config(config)
    , coordinator(config.coordinatorLocator.c_str())
    , serverId(0)
    , recoveryTicks()
    , pool(storage.getSegmentSize())
    , recoveryThreadCount{0}
    , segments()
    , segmentSize(storage.getSegmentSize())
    , storage(storage)
    , storageBenchmarkResults()
    , bytesWritten(0)
    , ioScheduler()
#ifdef SINGLE_THREADED_BACKUP
    , ioThread()
#else
    , ioThread(boost::ref(ioScheduler))
#endif
    , initCalled(false)
{
    try {
        recoveryTicks.construct(); // make unit tests happy

        // Prime the segment pool. This removes an overhead that would
        // otherwise be seen during the first recovery.
        void* mem[100];
        foreach(auto& m, mem)
            m = pool.malloc();
        foreach(auto& m, mem)
            pool.free(m);
    } catch (...) {
        ioScheduler.shutdown(ioThread);
        throw;
    }
}

/**
 * Flush open segments to disk and shutdown.
 */
BackupService::~BackupService()
{
    int lastThreadCount = 0;
    while (recoveryThreadCount > 0) {
        if (lastThreadCount != recoveryThreadCount) {
            LOG(DEBUG, "Waiting for recovery threads to terminate before "
                "deleting BackupService, %d threads "
                "still running", static_cast<int>(recoveryThreadCount));
            lastThreadCount = recoveryThreadCount;
        }
    }
    Fence::enter();

    ioScheduler.shutdown(ioThread);

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
ServerId
BackupService::getServerId() const
{
    return serverId;
}

/**
 * Perform an initial benchmark of the storage system. This is later passed to
 * the coordinator during init() so that masters can intelligently select
 * backups.
 *
 * This is not part of the constructor because we don't want the benchmark
 * running during unit tests. It's not part of init() because that method must
 * be fast (right now, as soon as init runs for some service, clients can start
 * accessing the service; however, the dispatch loop won't run until init
 * completes for all services).
 */
void
BackupService::benchmark(uint32_t& readSpeed, uint32_t& writeSpeed) {
    storageBenchmarkResults = storage.benchmark(config.backupStrategy);
    readSpeed = storageBenchmarkResults.first;
    writeSpeed = storageBenchmarkResults.second;
}

// See Server::dispatch.
void
BackupService::dispatch(RpcOpcode opcode, Rpc& rpc)
{
    assert(initCalled);
    CycleCounter<RawMetric> serviceTicks(&metrics->backup.serviceTicks);

    switch (opcode) {
        case BackupFreeRpc::opcode:
            callHandler<BackupFreeRpc, BackupService,
                        &BackupService::freeSegment>(rpc);
            break;
        case BackupGetRecoveryDataRpc::opcode:
            callHandler<BackupGetRecoveryDataRpc, BackupService,
                        &BackupService::getRecoveryData>(rpc);
            break;
        case BackupQuiesceRpc::opcode:
            callHandler<BackupQuiesceRpc, BackupService,
                        &BackupService::quiesce>(rpc);
            break;
        case BackupRecoveryCompleteRpc::opcode:
            callHandler<BackupRecoveryCompleteRpc, BackupService,
                        &BackupService::recoveryComplete>(rpc);
            break;
        case BackupStartReadingDataRpc::opcode:
            callHandler<BackupStartReadingDataRpc, BackupService,
                        &BackupService::startReadingData>(rpc);
            break;
        case BackupWriteRpc::opcode:
            callHandler<BackupWriteRpc, BackupService,
                        &BackupService::writeSegment>(rpc);
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
BackupService::freeSegment(const BackupFreeRpc::Request& reqHdr,
                           BackupFreeRpc::Response& respHdr,
                           Rpc& rpc)
{
    LOG(DEBUG, "Handling: %lu %lu", reqHdr.masterId, reqHdr.segmentId);

    SegmentsMap::iterator it =
        segments.find(MasterSegmentIdPair(ServerId(reqHdr.masterId),
                                                   reqHdr.segmentId));
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
BackupService::SegmentInfo*
BackupService::findSegmentInfo(ServerId masterId, uint64_t segmentId)
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
BackupService::getRecoveryData(const BackupGetRecoveryDataRpc::Request& reqHdr,
                               BackupGetRecoveryDataRpc::Response& respHdr,
                               Rpc& rpc)
{
    LOG(DEBUG, "getRecoveryData masterId %lu, segmentId %lu, partitionId %lu",
        reqHdr.masterId, reqHdr.segmentId, reqHdr.partitionId);

    SegmentInfo* info = findSegmentInfo(ServerId(reqHdr.masterId),
                                                 reqHdr.segmentId);
    if (!info) {
        LOG(WARNING, "Asked for bad segment <%lu,%lu>",
            reqHdr.masterId, reqHdr.segmentId);
        throw BackupBadSegmentIdException(HERE);
    }

    Status status = info->appendRecoverySegment(reqHdr.partitionId,
                                                rpc.replyPayload);
    if (status != STATUS_OK) {
        respHdr.common.status = status;
        return;
    }

    ++metrics->backup.readCompletionCount;
    LOG(DEBUG, "getRecoveryData complete");
}

/**
 * Perform once-only initialization for the backup service after having
 * enlisted the process with the coordinator
 */
void
BackupService::init(ServerId id)
{
#if 0
    coordinator.enlistServer(BACKUP, config.localLocator,
                                        storageBenchmarkResults.first,
                                        storageBenchmarkResults.second);
#endif

    assert(!initCalled);

    serverId = id;
    LOG(NOTICE, "My server ID is %lu", *id);
    if (metrics->serverId == 0) {
        metrics->serverId = *serverId;
    }

    initCalled = true;
}

/**
 * Flush all data to storage.
 * Returns once all dirty buffers have been written to storage.
 * \param reqHdr
 *      Header of the Rpc request.
 * \param respHdr
 *      Header for the Rpc response.
 * \param rpc
 *      The Rpc being serviced.
 */
void
BackupService::quiesce(const BackupQuiesceRpc::Request& reqHdr,
                       BackupQuiesceRpc::Response& respHdr,
                       Rpc& rpc)
{
    ioScheduler.quiesce();
}

/**
 * Clean up state associated with the given master after recovery.
 * \param reqHdr
 *      Header of the Rpc request containing the segment number to free.
 * \param respHdr
 *      Header for the Rpc response.
 * \param rpc
 *      The Rpc being serviced.
 */
void
BackupService::recoveryComplete(
        const BackupRecoveryCompleteRpc::Request& reqHdr,
        BackupRecoveryCompleteRpc::Response& respHdr,
        Rpc& rpc)
{
    LOG(DEBUG, "masterID: %lu", reqHdr.masterId);
    rpc.sendReply();
    recoveryTicks.destroy();
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
BackupService::segmentInfoLessThan(SegmentInfo* left,
                                   SegmentInfo* right)
{
    return *left < *right;
}

namespace {
/**
 * Make generateRandom model RandomNumberGenerator.
 * This makes it easy to mock the calls for randomness during testing.
 *
 * \param n
 *      Limits the result to the range [0, n).
 * \return
 *      Returns a pseudo-random number in the range [0, n).
 */
uint32_t
randomNumberGenerator(uint32_t n)
{
    return static_cast<uint32_t>(generateRandom()) % n;
}
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
 */
void
BackupService::startReadingData(
        const BackupStartReadingDataRpc::Request& reqHdr,
        BackupStartReadingDataRpc::Response& respHdr,
        Rpc& rpc)
{
    LOG(DEBUG, "Handling: %lu", reqHdr.masterId);
    recoveryTicks.construct(&metrics->backup.recoveryTicks);
    recoveryStart = Cycles::rdtsc();
    metrics->backup.recoveryCount++;
    metrics->backup.storageType = static_cast<uint64_t>(storage.storageType);

    ProtoBuf::Tablets partitions;
    ProtoBuf::parseFromResponse(rpc.requestPayload, sizeof(reqHdr),
                                reqHdr.partitionsLength, partitions);

    uint64_t logDigestLastId = ~0UL;
    uint32_t logDigestLastLen = 0;
    uint32_t logDigestBytes = 0;
    const void* logDigestPtr = NULL;

    vector<SegmentInfo*> primarySegments;
    vector<SegmentInfo*> secondarySegments;

    for (SegmentsMap::iterator it = segments.begin();
         it != segments.end(); it++)
    {
        ServerId masterId = it->first.masterId;
        SegmentInfo* info = it->second;
        if (*masterId == reqHdr.masterId) {
            (info->primary ?
                primarySegments :
                secondarySegments).push_back(info);

            // Obtain the LogDigest from the lowest Segment Id of any
            // #OPEN Segment.
            uint64_t segmentId = info->segmentId;
            if (info->isOpen() && segmentId <= logDigestLastId) {
                const void* newDigest = NULL;
                uint32_t newDigestBytes;
                newDigest = info->getLogDigest(&newDigestBytes);
                if (newDigest != NULL) {
                    logDigestLastId = segmentId;
                    logDigestLastLen = info->getRightmostWrittenOffset();
                    logDigestBytes = newDigestBytes;
                    logDigestPtr = newDigest;
                    LOG(DEBUG, "Segment %lu's LogDigest queued for response",
                        segmentId);
                }
            }
        }
    }

    // Shuffle the primary entries, this helps all recovery
    // masters to stay busy even if the log contains long sequences of
    // back-to-back segments that only have objects for a particular
    // partition.  Secondary order is irrelevant.
    std::random_shuffle(primarySegments.begin(),
                        primarySegments.end(),
                        randomNumberGenerator);

    foreach (auto info, primarySegments) {
        new(&rpc.replyPayload, APPEND) pair<uint64_t, uint32_t>
            (info->segmentId, info->getRightmostWrittenOffset());
        LOG(DEBUG, "Crashed master %lu had segment %lu (primary)",
            *info->masterId, info->segmentId);
        info->setRecovering();
    }
    foreach (auto info, secondarySegments) {
        new(&rpc.replyPayload, APPEND) pair<uint64_t, uint32_t>
            (info->segmentId, info->getRightmostWrittenOffset());
        LOG(DEBUG, "Crashed master %lu had segment %lu (secondary), "
            "stored partitions for deferred recovery segment construction",
            *info->masterId, info->segmentId);
        info->setRecovering(partitions);
    }
    respHdr.segmentIdCount = downCast<uint32_t>(primarySegments.size() +
                                                secondarySegments.size());
    respHdr.primarySegmentCount = downCast<uint32_t>(primarySegments.size());
    LOG(DEBUG, "Sending %u segment ids for this master (%u primary)",
        respHdr.segmentIdCount, respHdr.primarySegmentCount);

    respHdr.digestSegmentId  = logDigestLastId;
    respHdr.digestSegmentLen = logDigestLastLen;
    respHdr.digestBytes = logDigestBytes;
    if (respHdr.digestBytes > 0) {
        void* out = new(&rpc.replyPayload, APPEND) char[respHdr.digestBytes];
        memcpy(out, logDigestPtr, respHdr.digestBytes);
        LOG(DEBUG, "Sent %u bytes of LogDigest to coord", respHdr.digestBytes);
    }

    rpc.sendReply();

#ifndef SINGLE_THREADED_BACKUP
    RecoverySegmentBuilder builder(Context::get(),
                                   primarySegments,
                                   partitions,
                                   recoveryThreadCount);
    ++recoveryThreadCount;
    boost::thread builderThread(builder);
    LOG(DEBUG, "Kicked off building recovery segments; "
               "main thread going back to dispatching requests");
#endif
}

/**
 * Store an opaque string of bytes in a currently open segment on
 * this backup server.  This data is guaranteed to be considered on recovery
 * of the master to which this segment belongs to the best of the ability of
 * this BackupService and will appear open unless it is closed
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
BackupService::writeSegment(const BackupWriteRpc::Request& reqHdr,
                            BackupWriteRpc::Response& respHdr,
                            Rpc& rpc)
{
    ServerId masterId(reqHdr.masterId);
    uint64_t segmentId = reqHdr.segmentId;

    SegmentInfo* info = findSegmentInfo(masterId, segmentId);

    // peform open, if any
    if ((reqHdr.flags & BackupWriteRpc::OPEN) && !info) {
        LOG(DEBUG, "Opening <%lu,%lu>", *masterId, segmentId);
        try {
#ifdef SINGLE_THREADED_BACKUP
            bool primary = false;
#else
            bool primary = reqHdr.flags & BackupWriteRpc::PRIMARY;
#endif
            info = new SegmentInfo(storage, pool, ioScheduler,
                                   masterId, segmentId, segmentSize, primary);
            segments[MasterSegmentIdPair(masterId, segmentId)] = info;
            info->open();
        } catch (...) {
            segments.erase(MasterSegmentIdPair(masterId, segmentId));
            delete info;
            throw;
        }
    }

    // peform write
    if (!info) {
        throw BackupBadSegmentIdException(HERE);
    } else if (info->isOpen()) {
        // Need to check all three conditions because overflow is possible
        // on the addition
        if (reqHdr.length > segmentSize ||
            reqHdr.offset > segmentSize ||
            reqHdr.length + reqHdr.offset > segmentSize)
            throw BackupSegmentOverflowException(HERE);

        {
            CycleCounter<RawMetric> __(&metrics->backup.writeCopyTicks);
            info->write(rpc.requestPayload, sizeof(reqHdr),
                        reqHdr.length, reqHdr.offset);
        }
        metrics->backup.writeCopyBytes += reqHdr.length;
        bytesWritten += reqHdr.length;
    } else {
        if (!(reqHdr.flags & BackupWriteRpc::CLOSE))
            throw BackupBadSegmentIdException(HERE);
        LOG(WARNING, "Closing segment write after close, may have been "
            "a redundant closing write, ignoring");
    }

    // peform close, if any
    if (reqHdr.flags & BackupWriteRpc::CLOSE)
        info->close();
}

} // namespace RAMCloud
