/* Copyright (c) 2010-2015 Stanford University
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

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#include "SingleFileStorage.h"
#include "Buffer.h"
#include "Crc32C.h"
#include "ClientException.h"
#include "CycleCounter.h"
#include "Cycles.h"
#include "Memory.h"
#include "RawMetrics.h"
#include "ShortMacros.h"
#include "PerfStats.h"

namespace RAMCloud {

/**
 * Linux (and its specific filesystems) mandate a certain alignment for files
 * using O_DIRECT. In Linux 2.6 that alignment is 512 bytes.
 */
enum { BUFFER_ALIGNMENT = 512 };

/**
 * Maximum number of replica buffers to pool. These are used whenever a
 * replica is opened on a backup or when a replica is loaded from disk.
 * Backups will pool any buffers they allocate up to this number. Any buffers
 * beyond this amount that are no longer needed will be returned to the OS.
 */
enum { MAX_POOLED_BUFFERS = 128 };

/**
 * Number of replicas buffers to allocate on startup. Used to warm backup
 * replica buffer pools. See MAX_POOLED_BUFFERS for more details.
 */
enum { INIT_POOLED_BUFFERS = MAX_POOLED_BUFFERS };

// --- SingleFileStorage::Frame ---

bool SingleFileStorage::Frame::testingSkipRealIo = false;

/**
 * Create a Frame associated with a particular region of the file on storage.
 * Only called when SingleFileStorage is constructed. After construction Frames
 * can be used in one of two ways:
 * 1) loadMetadata() can be called to reload the metadata from the associated
 *    frame. Used when backups restart to determine which replicas they have.
 * 2) (1) can be skipped, and instead open() can be called. Data/metadata in the
 *    frame will (eventually) be lost/replaced. Used when backups restart and
 *    wish to ignore all replicas on storage or if they intend to wipe the
 *    storage.
 */
SingleFileStorage::Frame::Frame(SingleFileStorage* storage, size_t frameIndex)
    : PriorityTask(storage->ioQueue)
    , storage(storage)
    , frameIndex(frameIndex)
    , buffer(NULL, storage->bufferDeleter)
    , isOpen(false)
    , isClosed(false)
    , sync(false)
    , isWriteBuffer(false)
    , appendedToByCurrentProcess(false)
    , appendedLength(0)
    , committedLength(0)
    , appendedMetadata(Memory::xmemalign(HERE,
                                         BUFFER_ALIGNMENT,
                                         METADATA_SIZE),
                       std::free)
    , appendedMetadataLength(0)
    , appendedMetadataVersion(0)
    , committedMetadataVersion(0)
    , loadRequested(false)
    , performingIo(false)
    , epoch(1)
    , scheduledInEpoch(0)
    , testingHadToWaitForBufferOnLoad(false)
    , testingHadToWaitForSyncOnLoad(false)
{
    memset(appendedMetadata.get(), '\0', METADATA_SIZE);
}

SingleFileStorage::Frame::~Frame()
{
    deschedule();
}

/**
 * Do NOT call this method. It is here to prevent calls to the base class
 * implementation. Use the method below.
 */
void
SingleFileStorage::Frame::schedule(Priority priority)
{
    DIE("Unsafe use of base class schedule, use schedule(Lock&, Priority)");
}

/**
 * Schedule this task to perform io. Internally marks down the frame
 * epoch this io was scheduled in. If the frame is freed before the
 * task is invoked the invocation will return immediately without
 * doing anything.
 *
 * \param lock
 *      Lock on the storage mutex which must be held before calling.
 *      Not actually used; just here to sanity check locking.
 * \param priority
 *      Priority of this task versus others. Reads are performed with
 *      NORMAL priority; writes are performed with LOW priority.
 */
void
SingleFileStorage::Frame::schedule(Lock& lock, Priority priority)
{
    scheduledInEpoch = epoch;
    PriorityTask::schedule(priority);
}

/**
 * Returns true if append has been called on this frame during the life of this
 * process; returns false otherwise. This includes across free()/open() cycles.
 * This is used by the backup replica garbage collector to determine whether it
 * might have missed a BackupFree rpc from a master, in which case it has to
 * query the master for the replica status. This is "appended to" rather than
 * "opened by" because of benchmark(); benchmark "opens" replicas and loads
 * them but doesn't do any appends. Masters open and append data in a single
 * rpc, so this is a fine proxy.
 */
bool
SingleFileStorage::Frame::wasAppendedToByCurrentProcess()
{
    return appendedToByCurrentProcess;
}

/**
 * Reloads metadata from storage into memory; only used when restarting a
 * backup from storage. After this call returns getMetadata() will return
 * the metadata as found on storage up until the first call to open() on
 * this frame. This can only be safely called immediately after frame is
 * constructed (NOT opened), before any methods are called on it.
 */
void
SingleFileStorage::Frame::loadMetadata()
{
    const size_t metadataStart = storage->offsetOfMetadataFrame(frameIndex);
    ssize_t r = pread(storage->fd, appendedMetadata.get(), METADATA_SIZE,
                      metadataStart);
    if (r == -1) {
        DIE("Failed to read metadata stored in frame %lu: %s, "
            "starting offset %lu, length %d",
            frameIndex, strerror(errno), metadataStart, METADATA_SIZE);
    } else if (r != METADATA_SIZE) {
        DIE("Failed to read metadata stored in frame %lu: reached end of "
            "file, starting offset %lu, length %d",
            frameIndex, metadataStart, METADATA_SIZE);
    }
}

/**
 * Return a pointer to the most recently appended metadata for this frame.
 * The first getMetadataSize() bytes of the returned pointers are valid, though
 * it may or may not contain valid or meaningful (or even consistent with the
 * replica) metadata.
 * Warning: Concurrent calls to append modify the metadata that the return
 * of this method points to. In practice it should only be called when the
 * frame isn't accepting appends (either it was just constructed or one of 
 * close, load, or free has already been called on it). Used only during
 * backup restart and master recovery to extract details about the replica
 * in this frame without loading the frame.
 */
const void*
SingleFileStorage::Frame::getMetadata()
{
    return appendedMetadata.get();
}

/**
 * Start loading the replica in this frame from storage without blocking.
 * Use load() to wait for the load to complete and to retreive a pointer
 * to the loaded replica data. isLoaded() can be used to poll for the
 * completion of the load without blocking.
 *
 * After this call the start of new appends to this frame are rejected until
 * this frame is recycled for use with another replica (via open()).
 */
void
SingleFileStorage::Frame::startLoading()
{
    Lock lock(storage->mutex);
    if (loadRequested)
        return;
    loadRequested = true;
    if (buffer)
        return;
    schedule(lock, NORMAL);
}

/**
 * Returns true if calling load() would not block. Always returns false if
 * startLoading() or load() hasn't been called.
 */
bool
SingleFileStorage::Frame::isLoaded()
{
    Lock _(storage->mutex);
    return loadRequested && buffer;
}

/**
 * Return a pointer to the replica data for recovery. If needed, the replica is
 * loaded from storage into memory. If the replica is already in memory a load
 * from disk is avoided. If the replica buffer is dirty this call blocks until
 * all data has been flushed to disk to ensure that recoveries only use durable
 * data.
 *
 * After this call the start of new appends to this frame are rejected until
 * this frame is recycled for use with another replica (via open()).
 */
void*
SingleFileStorage::Frame::load()
{
    startLoading();
    while (true) {
        Lock lock(storage->mutex);
        if (!buffer) {
            testingHadToWaitForBufferOnLoad = true;
            continue;
        }
        if (!isSynced()) {
            testingHadToWaitForSyncOnLoad = true;
            continue;
        }
        return buffer.get();
    }
}

/**
 * Release a replica from in-memory buffers so they can be reused for other
 * replicas. Called after filtering completes on a replica during recovery.
 * Must only be called after a replica has been fully loaded.
 */
void
SingleFileStorage::Frame::unload()
{
    Lock lock(storage->mutex);
    assert(loadRequested);
    buffer.reset();
    loadRequested = false;
}

/**
 * Append data to frame and update metadata.
 * Data will be written to storage using the policy selected on open():
 * Writes data and metadata to storage asynchronously if #sync is false.
 * Writes data and metadata to storage synchronously if #sync is true.
 *
 * Idempotence: the caller must guarantee duplicated calls provide identical
 * arguments.
 *
 * append() after a load() or a close() throws an exception to
 * the master performing the append since it is either an error by the master
 * or the master has crashed.
 *
 * \param source
 *      Buffer containing the data to be copied into the frame.
 * \param sourceOffset
 *      Offset into \a source where data should be copied from.
 * \param length
 *      Bytes to copy to the frame starting at \a sourceOffset in \a source.
 * \param destinationOffset
 *      Offset into the frame where the source data should be copied.
 * \param metadata
 *      Metadata which should be written to storage immediately after the data
 *      appended is written. May be NULL if there is no updated metadata to
 *      commit to storage along with this data.
 * \param metadataLength
 *      Bytes of metadata pointed to by \a metadata. Ignored if \a metadata
 *      is NULL.
 */
void
SingleFileStorage::Frame::append(Buffer& source,
                                 size_t sourceOffset,
                                 size_t length,
                                 size_t destinationOffset,
                                 const void* metadata,
                                 size_t metadataLength)
{
    Lock lock(storage->mutex);
    if (!isOpen) {
        LOG(WARNING, "Tried to append to a frame but it wasn't "
            "open on this backup; this can happen legitimately if a master's "
            "rpc system retried a closing write rpc.");
        throw BackupBadSegmentIdException(HERE);
    }
    if (loadRequested) {
        LOG(NOTICE, "Tried to append to a frame but it was already enqueued "
            "for load for recovery; calling master is probably already dead");
        throw BackupBadSegmentIdException(HERE);
    }
    // Three conditions because overflow is possible on addition.
    if (length > storage->segmentSize ||
        destinationOffset > storage->segmentSize ||
        length + destinationOffset > storage->segmentSize)
    {
        LOG(ERROR, "Out-of-bounds appended attempted on storage frame: "
            "offset %lu, length %lu, segmentSize %lu ",
            destinationOffset, length, storage->segmentSize);
        throw BackupSegmentOverflowException(HERE);
    }
    if (metadataLength > METADATA_SIZE) {
        LOG(ERROR, "Tried to append to a frame with metadata of length %lu "
            "but storage only allows max length of %d",
            metadataLength, METADATA_SIZE);
        throw BackupSegmentOverflowException(HERE);
    }

    appendedToByCurrentProcess = true;
    source.copy(downCast<uint32_t>(sourceOffset),
                downCast<uint32_t>(length),
                static_cast<char*>(buffer.get()) + destinationOffset);

    // Update appendedLength, but only if it would get larger (there are
    // situations where older data could get rewritten, such as a delayed
    // RPC or RAM-573).
    if ((destinationOffset + length) > appendedLength) {
        appendedLength = destinationOffset + length;
    }

    if (metadata) {
        appendedMetadataLength = metadataLength;
        memcpy(appendedMetadata.get(), metadata, appendedMetadataLength);
        ++appendedMetadataVersion;
    }

    if (!isSynced()) {
        if (sync) {
            performWrite(lock);
        } else {
            schedule(lock, LOW);
        }
    }
}

/**
 * Mark this frame as closed. Once all data has been flushed to storage
 * in-memory buffers for this frame will be released. For synchronous mode the
 * caller must guarantee that there are no ongoing calls to append() for this
 * frame. Close is idempotent. Calls to close after a call to load() throw
 * BackupBadSegmentIdException which should kill the calling master; in this
 * case recovery has already started for them so they are likely already dead.
 */
void
SingleFileStorage::Frame::close()
{
    Lock lock(storage->mutex);
    if (isClosed)
        return;
    if (loadRequested) {
        LOG(NOTICE, "Tried to close a frame but it was already enqueued "
            "for load for recovery; calling master is probably already dead");
        throw BackupBadSegmentIdException(HERE);
    }
    isOpen = false;
    isClosed = true;

    if (isSynced()) {
        if (buffer) {
            buffer.reset();
            if (isWriteBuffer) {
                --storage->writeBuffersInUse;
                isWriteBuffer = false;
            }
        }
    }
}

/**
 * Perform outstanding IO for this frame. Frames prioritize writes over loads
 * since loads require writes to finish first.
 */
void
SingleFileStorage::Frame::performTask()
{
    Lock lock(storage->mutex);
    if (epoch != scheduledInEpoch)
        return;
    performingIo = true;
    if (!isSynced()) {
        performWrite(lock);
    } else if (loadRequested && !buffer) {
        performRead(lock);
    }
    performingIo = false;
}

// - protected -

/**
 * Do not call; see BackupStorage::freeFrame().
 * Make this frame available for reuse; data previously stored in this frame
 * may or may not be part of future recoveries. It does not modify storage,
 * only in-memory bookkeeping structures, so a previously freed frame will not
 * be free on restart until higher-level backup code explicitly free them after
 * it determines it is not needed. May block until any currently ongoing IO
 * operation for the completes.
 */
void
SingleFileStorage::Frame::free()
{
    Lock lock(storage->mutex);
    while (performingIo) {
        lock.unlock();
        lock.lock();
    }
    ++epoch;
    deschedule();
    if (!isSynced())
        CycleCounter<RawMetric> _(&metrics->backup.uncommittedFramesFreed);
    isOpen = false;
    isClosed = false;
    // Must reset this before open(), because on startup after benchmark, the
    // frame may be loaded without open.
    loadRequested = false;

    // Reset these to ensure we don't have a case where a freed frame still
    // appears to be !isSynced().
    appendedLength = 0;
    committedLength = 0;
    memset(appendedMetadata.get(), '\0', METADATA_SIZE);
    appendedMetadataLength = 1;
    appendedMetadataVersion = 0;
    committedMetadataVersion = 0;

    if (buffer) {
        buffer.reset();
        if (isWriteBuffer) {
            --storage->writeBuffersInUse;
            isWriteBuffer = false;
        }
    }

    storage->freeMap[frameIndex] = 1;
}

// See BackupStorage.h for documentation.
void
SingleFileStorage::Frame::reopen(size_t length)
{
    assert(!isOpen && !isClosed);
    load();

    Lock _(storage->mutex);
    appendedLength = length;
    committedLength = length;
    isOpen = true;
    loadRequested = false;
}

// - private -

/**
 * Open the frame, resetting its state to accept appends for a new replica.
 * Open is not synchronous itself. Even after return from open() if this
 * backup crashes it may find the replica which was formerly stored in this
 * frame or metadata for the former replica and data for the newly open replica.
 * Recovery is expected to address these consistency issues with the checksums.
 *
 * Idempotence: caller must guarantee all calls to open() for a single replica
 * provide the same arguments. Duplicate calls to open() are ignored until
 * the frame is freed. Calling open() after in is freed will reset this
 * frame for reuse with an new replica.
 *
 * \param sync
 *      Only return from append() calls when all enqueued data and the most
 *      recently enqueued metadata are durable on storage.
 */
void
SingleFileStorage::Frame::open(bool sync)
{
    Lock _(storage->mutex);
    if (isOpen || isClosed)
        return;
    buffer = storage->allocateBuffer();

    // Be careful, if this method throws an exception the storage layer
    // above will leak the count of a non-volatile buffer.
    storage->writeBuffersInUse++;
    isOpen = true;
    isClosed = false;
    this->sync = sync;
    isWriteBuffer = true;
    appendedLength = 0;
    committedLength = 0;
    memset(appendedMetadata.get(), '\0', METADATA_SIZE);
    appendedMetadataLength = 1;
    appendedMetadataVersion = 0;
    committedMetadataVersion = 0;
    loadRequested = false;
}

/**
 * Wrapper for pread that releases \a lock during IO and DIEs on any problem.
 * If useDevNull is true, this method does not consider short reads to be a
 * "problem".
 */
void
SingleFileStorage::unlockedRead(Frame::Lock& lock, void* buf, size_t count,
                                off_t offset, bool usingDevNull) const
{
    lock.unlock();
    ssize_t r;
    {
        CycleCounter<RawMetric> _(&metrics->backup.storageReadTicks);
        r = pread(fd, buf, count, offset);
        PerfStats::threadStats.backupReadActiveCycles += _.stop();
    }
    if (r == -1) {
        DIE("Failed to read replica: %s, "
            "starting offset in file %lu, length %lu",
            strerror(errno), offset, count);
    } else if (r != downCast<ssize_t>(count)) {
        if (!usingDevNull)
            DIE("Failure performing asynchronous IO (short read: "
                "wanted %lu, got %lu at offset %lu; errno %d: %s)",
                count, r, offset, errno, strerror(errno));
        else
            assert(errno == 0);
    }
    lock.lock();
}

/**
 * Wrapper for pwrite that releases \a lock during IO and DIEs on any problem.
 * Performs two pwrites back-to-back: one for new data being appended and
 * another to write out the most recently appended metadata block.
 */
void
SingleFileStorage::unlockedWrite(Frame::Lock& lock, void* buf, size_t count,
                                 off_t offset, void* metadataBuf,
                                 size_t metadataCount,
                                 off_t metadataOffset) const
{
    CycleCounter<RawMetric> writeTicks(&metrics->backup.storageWriteTicks);
    lock.unlock();
    ssize_t r = pwrite(fd, buf, count, offset);
    if (r == -1) {
        DIE("Failed to write to replica: %s, "
            "starting offset in file %lu, length %lu",
            strerror(errno), offset, count);
    } else if (r != downCast<ssize_t>(count)) {
        DIE("Unexpectedly short write to replica, starting offset in "
            "file %lu, length %lu",
             offset, count);
    }
    r = pwrite(fd, metadataBuf, metadataCount, metadataOffset);
    PerfStats::threadStats.backupWriteActiveCycles += writeTicks.stop();
    if (r == -1) {
        DIE("Failed to write metadata for replica: %s, "
            "starting offset in file %lu, length %lu",
            strerror(errno), metadataOffset, metadataCount);
    } else if (r != downCast<ssize_t>(metadataCount)) {
        DIE("Unexpectedly short write metadata for replica, starting offset "
            "in file %lu, expected length %lu, actual write length %ld",
            metadataOffset, metadataCount, r);
    }
    // Reduce our bandwidth (if so configured) by delaying this operation.
    CycleCounter<RawMetric> _(&metrics->backup.storageWriteTicks);
    sleepToThrottleWrites(count + metadataCount, writeTicks.stop());
    lock.lock();
}

namespace {
/**
 * Round \a offset down to a block boundary.
 * Required due to IO alignment constraints for files/devices opened O_DIRECT.
 */
size_t
roundDown(size_t offset)
{
    return offset & ~(SingleFileStorage::BLOCK_SIZE - 1);
}

/**
 * Round \a length up to a block boundary.
 * Required due to IO alignment constraints for files/devices opened O_DIRECT.
 */
size_t
roundUp(size_t length)
{
    return (length + (SingleFileStorage::BLOCK_SIZE - 1)) /
           SingleFileStorage::BLOCK_SIZE *
           SingleFileStorage::BLOCK_SIZE;
}
}

/**
 * Loads replica data from disk (excluding metadata) and then atomically sets
 * the #buffer member to a buffer pointing to the replica data.
 * Note: the lock on #mutex is released while actual IO is happening so
 * invariants need to be rechecked after the call to unlockedRead.
 */
void
SingleFileStorage::Frame::performRead(Lock& lock)
{
    assert(loadRequested);
    BufferPtr buffer = storage->allocateBuffer();
    const size_t frameStart = storage->offsetOfFrame(frameIndex);

    if (testingSkipRealIo) {
        TEST_LOG("count %lu offset %lu", storage->segmentSize, frameStart);
    } else {
        ++metrics->backup.storageReadCount;
        metrics->backup.storageReadBytes += storage->segmentSize;
        ++PerfStats::threadStats.backupReadOps;
        PerfStats::threadStats.backupReadBytes += storage->segmentSize;
        // Lock released during this call; assume any field could have changed.
        storage->unlockedRead(lock, buffer.get(),
                     storage->segmentSize, frameStart, storage->usingDevNull);
    }

    assert(!this->buffer);
    this->buffer = std::move(buffer);
}

/**
 * Flush any appended data and the latest appended metadata to disk.
 * Requires #buffer to remain set for the duration of the operation, though
 * data can be appended to it concurrently. Releases the buffer if it won't
 * be needed in the immediate future and reschedules if any additional IO
 * has been requested by the time the method completes.
 * Note: the lock on #mutex is released while actual IO is happening so
 * invariants need to be rechecked after the call to unlockedWrite.
 */
void
SingleFileStorage::Frame::performWrite(Lock& lock)
{
    assert(buffer);

    const size_t startOfFirstDirtyBlock = roundDown(committedLength);
    const size_t startOfNextCleanBlock = roundUp(appendedLength);
    const size_t dirtyLength = startOfNextCleanBlock - startOfFirstDirtyBlock;

    char* firstDirtyBlock =
        static_cast<char*>(buffer.get()) + startOfFirstDirtyBlock;
    char* metadataBlock =
        static_cast<char*>(buffer.get()) + storage->segmentSize;

    // Snapshot values which will be needed after the write and the
    // metadata block. Appends to the main buffer that are concurrent
    // with the write are ok.
    const size_t appendedLength = this->appendedLength;
    memcpy(metadataBlock, appendedMetadata.get(), appendedMetadataLength);
    const size_t appendedMetadataVersion = this->appendedMetadataVersion;

    const size_t frameStart = storage->offsetOfFrame(frameIndex);
    const size_t metadataStart = storage->offsetOfMetadataFrame(frameIndex);
    if (testingSkipRealIo) {
        TEST_LOG("sourceBufferOffset %lu count %lu offset %lu "
                 "metadataOffset %lu",
                 startOfFirstDirtyBlock, dirtyLength,
                 frameStart + startOfFirstDirtyBlock, metadataStart);
    } else {
        ++metrics->backup.storageWriteCount;
        metrics->backup.storageWriteBytes += dirtyLength;
        ++PerfStats::threadStats.backupWriteOps;
        PerfStats::threadStats.backupWriteBytes += dirtyLength;
        // Lock released during this call; assume any field could have changed.
        storage->unlockedWrite(lock, firstDirtyBlock, dirtyLength,
                      frameStart + startOfFirstDirtyBlock,
                      metadataBlock, METADATA_SIZE, metadataStart);
    }

    assert(buffer);

    // Update committed based on the above snapshots of fields taken
    // just before the write.
    committedLength = appendedLength;
    committedMetadataVersion = appendedMetadataVersion;

    // Release the in-memory copy if it won't be used again.
    if (isClosed && isSynced() && !loadRequested && buffer) {
        buffer.reset();
        assert(isWriteBuffer);
        --storage->writeBuffersInUse;
        isWriteBuffer = false;
    }

    if (loadRequested) {
        schedule(lock, NORMAL);
    } else if (!isSynced()) {
        schedule(lock, LOW);
    }
}

/// Return true if all appended data and metadata have been flushed to storage.
bool
SingleFileStorage::Frame::isSynced() const
{
    return (appendedLength == committedLength) &&
           (appendedMetadataVersion == committedMetadataVersion);
}

// --- SingleFileStorage::BufferDeleter ---

/**
 * Create a functor that returns chunks of memory to #storage.buffers or
 * to the OS.
 *
 * \param storage
 *      SingleFileStorage to which returned buffers are pushed.
 */
SingleFileStorage::BufferDeleter::BufferDeleter(SingleFileStorage* storage)
    : storage(storage)
{
}

/**
 * Return a buffer allocated with SingleFileStorage::allocateBuffer().
 * Returns the buffer to a pool, or if there are already plenty of buffers
 * it returns it to the OS (which will unmap it).
 * Should only be called by SingleFileStorage::BufferPtr objects.
 *
 * \param buffer
 *      Pointer to buffer which is being released by a
 *      SingleFileStorage::BufferPtr and should be returned to the pool or
 *      the OS.
 */
void
SingleFileStorage::BufferDeleter::operator()(void* buffer)
{
    if (buffer) {
        if (storage->buffers.size() >= MAX_POOLED_BUFFERS) {
            std::free(buffer);
        } else {
            storage->buffers.push(buffer);
        }
    }
}

// --- SingleFileStorage ---

/**
 * Create a SingleFileStorage.
 *
 * \param segmentSize
 *      The size in bytes of the segments this storage will deal with.
 * \param frameCount
 *      The number of segments this storage can store simultaneously.
 * \param writeRateLimit
 *      When specified, writes to this storage instance should be limited
 *      to at most the given rate (in megabytes per second). The special
 *      value 0 turns off throttling.
 * \param maxWriteBuffers
 *      Limit on the number of segment replicas representing new data from
 *      masters that can be stored in memory at any given time.
 * \param filePath
 *      A filesystem path to the device or file where segments will be stored.
 *      If NULL then a temporary file in the system temp directory is created
 *      and it is deleted when this storage instance is destroyed. /dev/null
 *      may also be used if you never want to see your data again, even while
 *      the system is still running (that is, don't expect recoveries to work).
 * \param openFlags
 *      Extra flags for use while opening filePath (default to 0, O_DIRECT may
 *      be used to disable the OS buffer cache.
 */
SingleFileStorage::SingleFileStorage(size_t segmentSize,
                                     size_t frameCount,
                                     size_t writeRateLimit,
                                     size_t maxWriteBuffers,
                                     const char* filePath,
                                     int openFlags)
    : BackupStorage(segmentSize, Type::DISK, writeRateLimit)
    , mutex()
    , ioQueue()
    , superblock()
    , lastSuperblockFrame(1)
    , frames()
    , frameCount(frameCount)
    , freeMap(frameCount)
    , lastAllocatedFrame(FreeMap::npos)
    , openFlags(openFlags)
    , fd(-1)
    , usingDevNull(filePath != NULL && string(filePath) == "/dev/null")
    , tempFilePath()
    , writeBuffersInUse(0)
    , maxWriteBuffers(maxWriteBuffers)
    , bufferDeleter(this)
    , buffers()
{
    freeMap.set();

    if (filePath == NULL || filePath[0] == '\0') {
        tempFilePath =
            strdup("/tmp/ramcloud-backup-storage-test-delete-this-XXXXXX");
        fd = ::mkostemp(tempFilePath,
                        O_CREAT | O_RDWR | openFlags);
        filePath = tempFilePath;
    } else {
        // If we were given /dev/null (to take disk bandwidth out of the
        // equation during testing/benchmarking), don't supply the O_DIRECT
        // flag since it's not supported. Print an I-told-you-so while here.
        if (usingDevNull) {
            openFlags &= ~O_DIRECT;
            LOG(WARNING, "Using /dev/null to \"store\" your data. I hope you "
                "know what you're doing!");
        }
        fd = ::open(filePath,
                    O_CREAT | O_RDWR | openFlags,
                    0666);
    }
    if (fd == -1) {
        int e = errno;
        LOG(ERROR, "Failed to open backup storage file %s: %s",
            filePath, strerror(e));
        throw BackupStorageException(HERE,
              format("Failed to open backup storage file %s", filePath), e);
    }

    // If its a regular file reserve space, otherwise
    // assume its a device and we don't need to bother.
    struct stat st;
    int r = stat(filePath, &st);
    if (r == -1)
        return;
    if (st.st_mode & S_IFREG)
        reserveSpace();

    // The following line is completely black magic. I (stutsman) have no
    // idea why it improves things. As of 10/29/2012 removing the following
    // allocation and immediate free seems to cause nearly all memory related
    // operations during recovery to slow down. Hopefully in the future we'll
    // find that it is no longer needed or we come to understand what effect
    // it has. Upon removal check to ensure that loading replicas from disk
    // proceeds as quickly as it did before, that replay and replication
    // times during recovery look good, and that end-to-end recovery times
    // are good in a variety of configurations. As of writing this removing
    // this line takes a recovery of 10 600 MB partitions across 20 hosts
    // with 10 recovery masters with flash from a time of 1.45 s to
    // 1.75 ms.
    std::free(Memory::xmemalign(HERE, BUFFER_ALIGNMENT, segmentSize));

    { // Pre-fill the buffer pool.
        std::vector<BufferPtr> buffers;
        for (int i = 0; i < INIT_POOLED_BUFFERS; ++i)
            buffers.emplace_back(allocateBuffer());
    }

    for (size_t frame = 0; frame < frameCount; ++frame)
        frames.emplace_back(this, frame);

    ioQueue.start();
}

/// Close the file.
SingleFileStorage::~SingleFileStorage()
{
    ioQueue.halt();

    int r = close(fd);
    if (r == -1)
        LOG(ERROR, "Couldn't close backup log");

    if (tempFilePath) {
        unlink(tempFilePath);
        ::free(tempFilePath);
        tempFilePath = NULL;
    }

    while (!buffers.empty()) {
        std::free(buffers.top());
        buffers.pop();
    }
}

/**
 * Return a buffer large enough to hold the replica and the metadata block
 * that meets the alignment constraints required for O_DIRECT.
 * Caller must hold a lock on #mutex and must hold a lock on #mutex when
 * the returned pointer is destroyed or reset().
 */
SingleFileStorage::BufferPtr
SingleFileStorage::allocateBuffer()
{
    if (buffers.empty()) {
        void* block = Memory::xmemalign(HERE, BUFFER_ALIGNMENT,
                                        segmentSize + METADATA_SIZE);
        buffers.push(block);
    }
    void* buffer = buffers.top();
    buffers.pop();
    return BufferPtr{buffer, bufferDeleter};
}

/**
 * Allocate a frame on storage, resetting its state to accept appends for a new
 * replica. Open is not synchronous itself. Even after return from open() if
 * this backup crashes it may find the replica which was formerly stored in
 * this frame or metadata for the former replica and data for the newly open
 * replica. Recovery is expected to address these consistency issues with the
 * checksums.
 *
 * This call is NOT idempotent since it allocates and return resources to
 * the caller. The caller must take care not to lose frames. Any returned
 * frame which is not freed may be leaked until the backup (or the creating
 * master crashes). For example, the BackupService will need to guarantee that
 * any returned frame is associated with a particular replica and that future
 * RPCs requesting the creation of that replica reuse the returned frame.
 *
 * \param sync
 *      Only return from append() calls when all enqueued data and the  most
 *      recently enqueued metadata are durable on storage.
 * \return
 *      Reference to a frame through which handles all IO for a single
 *      replica. Maintains a reference count; when destroyed if the
 *      reference count drops to zero the frame will be freed for reuse with
 *      another replica.
 */
SingleFileStorage::FrameRef
SingleFileStorage::open(bool sync)
{
    Lock lock(mutex);
    if (writeBuffersInUse >= maxWriteBuffers) {
        // Force the master to find some place else and/or backoff.
        LOG(DEBUG, "Master tried to open a storage frame but too many "
            "frames already buffered to accept it; rejecting");
        throw BackupOpenRejectedException(HERE);
    }
    FreeMap::size_type next = freeMap.find_next(lastAllocatedFrame);
    if (next == FreeMap::npos) {
        next = freeMap.find_first();
        if (next == FreeMap::npos) {
            RAMCLOUD_CLOG(NOTICE, "Master tried to open a storage frame "
                "but there are no frames free; rejecting");
            throw BackupOpenRejectedException(HERE);
        }
    }
    lastAllocatedFrame = next;
    size_t frameIndex = next;
    assert(freeMap[frameIndex] == 1);
    freeMap[frameIndex] = 0;
    Frame* frame = &frames[frameIndex];
    lock.unlock();
    frame->open(sync);
    return {frame, BackupStorage::freeFrame};
}

/**
 * Same as BackupStorage::benchmark() except it resets the storage to reuse
 * the segment frames that may have been used during benchmarking.
 * This allows benchmark to be called without
 * wasting early segment frames on the disk which may be faster.
 */
uint32_t
SingleFileStorage::benchmark(BackupStrategy backupStrategy)
{
    uint32_t r = BackupStorage::benchmark(backupStrategy);
    lastAllocatedFrame = FreeMap::npos;
    return r;
}

/**
 * Returns the maximum number of bytes of metadata that can be stored
 * which each append(). Also, how many bytes of getMetadata() are safe
 * for access after getMetadata() calls, though returned data may or may
 * not contain valid or meaningful (or even consistent with the
 * replica) metadata.
 */
size_t
SingleFileStorage::getMetadataSize()
{
    return METADATA_SIZE;
}

/**
 * Marks ALL storage frames as allocated and blows away any in-memory copies
 * of metadata. This should only be performed at backup startup. The caller is
 * reponsible for freeing the frames if the metadata indicates the replica
 * data stored there isn't useful.
 *
 * \return
 *      Pointer to every frame which has various uses depending on the
 *      metadata that is found in that frame. BackupService code is expected
 *      to examine the metadata and either free the frame or take note of the
 *      metadata in the frame for potential use in future recoveries.
 */
std::vector<BackupStorage::FrameRef>
SingleFileStorage::loadAllMetadata()
{
    std::vector<FrameRef> ret;
    ret.reserve(frames.size());
    foreach (Frame& frame, frames) {
        frame.loadMetadata();
        assert(freeMap[frame.frameIndex] == 1);
        freeMap[frame.frameIndex] = 0;
        ret.push_back({&frame, BackupStorage::freeFrame});
    }
    return ret;
}

/**
 * Overwrite the on-storage superblock with new information that future
 * backups reusing this storage will need (in the case of this backup's
 * demise).
 * This is done safely so that a failure in the middle of the update
 * will leave either the old superblock or the new.
 *
 * \param serverId
 *      The server id of the process as assigned by the coordinator.
 *      It is persisted for the benefit of future processes reusing this
 *      storage.
 * \param clusterName
 *      Controls the reuse of replicas stored on this backup.  'Tags'
 *      replicas created on this backup with this cluster name.  This has
 *      two effects.  First, any replicas found in storage are discarded
 *      unless they are tagged with an identical cluster name. Second, any
 *      replicas created by the backup process will only be reused by future
 *      backup processes if the cluster name on the stored replica matches
 *      the cluster name of future process. The name '__unnamed__' is
 *      special and never matches any cluster name (even itself), so it
 *      guarantees all stored replicas are discarded on start and that all
 *      replicas created by this process are discarded by future backups.
 *      This is convenient for testing.
 * \param frameSkipMask
 *      Used for testing. This storage keeps two superblock images and
 *      overwrites the older first then the newer in the case a failure
 *      occurs in the middle of writing. Setting frameSkipMask to 0x1
 *      skips writing the first superblock image, 0x2 skips the second,
 *      and 0x3 skips both.
 */
void
SingleFileStorage::resetSuperblock(ServerId serverId,
                                   const string& clusterName,
                                   const uint32_t frameSkipMask)
{
    Superblock newSuperblock =
        Superblock(superblock.version + 1, serverId, clusterName.c_str());

    Memory::unique_ptr_free block(
        Memory::xmemalign(HERE, BUFFER_ALIGNMENT, BLOCK_SIZE), std::free);
    struct FileContents {
        explicit FileContents(const Superblock& newSuperblock)
            : superblock(newSuperblock)
            , checksum()
        {
            Crc32C crc;
            crc.update(&superblock, sizeof(superblock));
            checksum = crc.getResult();
        }
        Superblock superblock;
        Crc32C::ResultType checksum;
    } __attribute__((packed));
    new(block.get()) FileContents(newSuperblock);

    // Overwrite the two superblock images starting with the older one.
    for (uint32_t i = 0; i < 2; ++i) {
        const uint32_t nextFrame = (lastSuperblockFrame + 1) % 2;
        if (!((frameSkipMask >> nextFrame) & 0x01)) {
            const uint64_t offset = offsetOfSuperblockFrame(nextFrame);
            ssize_t r = pwrite(fd, block.get(), BLOCK_SIZE, offset);
            // An accurate superblock is required to determine if replicas
            // should be preserved at startup.  Without it any replicas
            // written by this backup would be in jeopardy.
            if (r == -1) {
                DIE("Failed to write the backup superblock; "
                    "cannot continue safely: %s", strerror(errno));
            } else if (r < BLOCK_SIZE) {
                DIE("Short write while writing the backup superblock; "
                    "cannot continue safely");
            }
            int s = fdatasync(fd);
            if (s == -1 && !usingDevNull) {
                DIE("Failed to flush the backup superblock; "
                    "cannot continue safely: %s", strerror(errno));
            }
            LOG(DEBUG, "Superblock frame %u written", nextFrame);
        }
        lastSuperblockFrame = nextFrame;
    }

    superblock = newSuperblock;
}

/**
 * Read both on-storage superblock locations and return the most up-to-date
 * and complete superblock since the last resetSuperblock().
 *
 * \return
 *      The most up-to-date complete superblock found on storage.  If no
 *      superblock can be found a default superblock is returned which
 *      indicates no prior backup instance left behind intelligible
 *      traces of life on storage.
 */
BackupStorage::Superblock
SingleFileStorage::loadSuperblock()
{
    Tub<Superblock> left;
    Tub<Superblock> right;

    try {
        left = tryLoadSuperblock(0);
    } catch (Exception& e) {}
    try {
        right = tryLoadSuperblock(1);
    } catch (Exception& e) {}

    bool chooseLeft = false;
    if (left && right) {
        chooseLeft = left->version >= right->version;
    } else if (!left && !right) {
        LOG(WARNING,
            "Backup couldn't find existing superblock; "
            "starting as fresh backup.");
        right.construct();
        chooseLeft = false;
    } else {
        chooseLeft = left;
    }

    if (chooseLeft) {
        superblock = *left;
        lastSuperblockFrame = 0;
    } else {
        superblock = *right;
        lastSuperblockFrame = 1;
    }

    LOG(DEBUG,
        "Reloading backup superblock (version %lu, superblockFrame %u) "
        "from previous run", superblock.version, lastSuperblockFrame);
    LOG(DEBUG, "Prior backup had ServerId %s",
        ServerId(superblock.serverId).toString().c_str());
    LOG(DEBUG, "Prior backup had cluster name '%s'", superblock.clusterName);

    return superblock;
}

/**
 * Return only after all data appended to all Frames prior to this call
 * has been flushed to storage. This simple implementation may be starved
 * if appends continue concurrent with this call.
 */
void
SingleFileStorage::quiesce()
{
    foreach (const Frame& frame, frames) {
        uint64_t start = Cycles::rdtsc();
        while (true) {
            Lock lock(mutex);
            if (frame.isSynced())
                break;
            if (Cycles::toSeconds(Cycles::rdtsc() - start) > 1.0) {
                LOG(WARNING, "Quiesce waiting for frame %lu to sync "
                    "(isScheduled: %u)", frame.frameIndex, frame.isScheduled());
                start = Cycles::rdtsc();
            }
        }
    }
}

/**
 * Scribble on all the metadata blocks of all the storage frames to prevent
 * what is already on disk from being reused in future runs.
 * Only safe immedately after this class is instantiated, before it is used
 * to allocate or perform operations on frames.
 * Called whenever the cluster name changes from what is stored in
 * the superblock to prevent replicas already on storage from getting
 * confused for ones written by the starting up backup process.
 */
void
SingleFileStorage::fry()
{
    Buffer empty;
    uint8_t zeroes[getMetadataSize()];
    memset(zeroes, 0, sizeof(zeroes));
    foreach (Frame& frame, frames) {
        frame.open(true);
        frame.append(empty, 0, 0, 0, zeroes, sizeof(zeroes));
        frame.free();
    }
}

// - private -

/**
 * Returns the offset into the file a particular frame starts at.
 *
 * \param frameIndex 
 *      Frame to find start of storage for in the file.
 */
off_t
SingleFileStorage::offsetOfFrame(size_t frameIndex) const
{
    const size_t firstFrameStart = offsetOfSuperblockFrame(2);
    return firstFrameStart + frameIndex * (segmentSize + METADATA_SIZE);
}

/**
 * Returns the offset into the file the metadata for a particular frame
 * starts at.
 *
 * \param frameIndex 
 *      Frame to find start of storage for in the file.
 */
off_t
SingleFileStorage::offsetOfMetadataFrame(size_t frameIndex) const
{
    const size_t frameStart = offsetOfFrame(frameIndex);
    return frameStart + segmentSize;
}

/**
 * Returns the offset into the file where a copy of the superblock may
 * be located.
 *
 * \param superblockIndex
 *      The superblock can be stored at two locations in the file.
 *      Passing 0 returns the start of the first location,
 *      passing 1 returns the start of the second location,
 *      passing 2 returns the start of the first segment frame.
 */
off_t
SingleFileStorage::offsetOfSuperblockFrame(size_t superblockIndex) const
{
    return superblockIndex *
           ((sizeof(Superblock) + BLOCK_SIZE - 1) / BLOCK_SIZE) *
           BLOCK_SIZE;
}

/**
 * Fix the size of the logfile to ensure that the OS doesn't tell us
 * the filesystem is out of space later.
 *
 * \throw BackupStorageException
 *      If space for frameCount segments of segmentSize cannot be reserved.
 */
void
SingleFileStorage::reserveSpace()
{
    uint64_t logSpace = offsetOfFrame(frameCount);

    LOG(DEBUG, "Reserving %lu bytes of log space", logSpace);
    int r = ftruncate(fd, logSpace);
    if (r == -1)
        throw BackupStorageException(HERE,
                "Couldn't reserve storage space for backup", errno);
}

/**
 * Try to read one of the multiple storage locations which may contain
 * a superblock.
 *
 * \param superblockFrame
 *      Which of the multiple superblock storage locations to read.
 *      Currently, there are two in order to avoid corrupting an existing
 *      superblock if a failure occurs in the middle of an update.
 * \return
 *      The superblock stored at \a superblockFrame is returned if
 *      it was loaded the stored checksum was correct.  Otherwise,
 *      if there was a problem reading the file or the contents appears
 *      to be damaged or incomplete the returned value is empty.
 */
Tub<BackupStorage::Superblock>
SingleFileStorage::tryLoadSuperblock(uint32_t superblockFrame)
{
    Memory::unique_ptr_free block(
        Memory::xmemalign(HERE, BUFFER_ALIGNMENT, BLOCK_SIZE), std::free);
    struct FileContents {
        Superblock superblock;
        Crc32C::ResultType checksum;
    } __attribute__((packed));
    uint64_t offset = offsetOfSuperblockFrame(superblockFrame);
    ssize_t r = pread(fd, block.get(), BLOCK_SIZE, offset);
    if (r == -1) {
        LOG(NOTICE, "Couldn't read superblock from superblock frame %u: %s",
            superblockFrame, strerror(errno));
        return {};
    } else if (r < BLOCK_SIZE) {
        LOG(NOTICE, "Couldn't read superblock from superblock frame %u: "
            "read was short (read %ld bytes of %u)",
            superblockFrame, r, BLOCK_SIZE);
        return {};
    }
    FileContents* fileContents = reinterpret_cast<FileContents*>(block.get());
    Superblock& superblock = fileContents->superblock;

    Crc32C crc;
    crc.update(&superblock, sizeof(superblock));
    uint32_t checksum = crc.getResult();

    // Check stored checksum against the computed checksum for stored data.
    if (fileContents->checksum != checksum) {
        LOG(NOTICE, "Stored superblock had a bad checksum: "
            "stored checksum was %x, but stored data had checksum %x",
            fileContents->checksum, checksum);
        return {};
    }
    char& endOfName =
        superblock.clusterName[sizeof(superblock.clusterName) - 1];
    if (endOfName != '\0')
        DIE("Stored superblock's cluster name should end in \\0; "
            "this should never happen unless there is a software bug");

    return { superblock };
}

} // namespace RAMCloud
