/* Copyright (c) 2010-2013 Stanford University
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

#include "InMemoryStorage.h"
#include "ClientException.h"
#include "Buffer.h"
#include "CycleCounter.h"
#include "ShortMacros.h"

namespace RAMCloud {

// --- InMemoryStorage::Frame ---

/**
 * Create a Frame associated with a chunk of memory that may hold a replica
 * in storage.
 */
InMemoryStorage::Frame::Frame(InMemoryStorage* storage, size_t frameIndex)
    : storage(storage)
    , frameIndex(frameIndex)
    , buffer()
    , isOpen()
    , isClosed()
    , appendedToByCurrentProcess()
    , loadRequested()
    , metadata(new char[METADATA_SIZE])
{
    memset(metadata.get(), '\0', METADATA_SIZE);
}

/**
 * Returns true if append has been called on this frame during the life of this
 * process; returns false otherwise. This includes across free()/open() cycles.
 * This is used by the backup replica garbage collector to determine whether it
 * might have missed a BackupFree rpc from a master, in which case it has to
 * query the master for the replica status. This is "appended to" rather than
 * "opened by" because of benchmark(); benchmark "opens" replicas and loads them
 * but doesn't do any. Masters open and append data in a single rpc, so this
 * is a fine proxy.
 */
bool
InMemoryStorage::Frame::wasAppendedToByCurrentProcess()
{
    return appendedToByCurrentProcess;
}

/**
 * No-op for InMemoryStorage.
 */
void
InMemoryStorage::Frame::loadMetadata()
{
}

/**
 * Return a pointer to the most recently appended metadata for this frame.
 * Warning: Concurrent calls to append modify the metadata that the return
 * of this method points to. In practice it should only be called when the
 * frame isn't accepting appends (either it was just constructed or one of 
 * close, load, or free has already been called on it). Used only during
 * backup restart and master recovery to extract details about the replica
 * in this frame without loading the frame.
 */
const void*
InMemoryStorage::Frame::getMetadata()
{
    return metadata.get();
}

/**
 * Doesn't do much for InMemoryStorage; prevents any further append()
 * calls from being accepted.
 */
void
InMemoryStorage::Frame::startLoading()
{
    Lock lock(storage->mutex);
    loadRequested = true;
}

/**
 * Returns true if calling load() would not block. Always returns false if
 * startLoading() or load() hasn't been called.
 */
bool
InMemoryStorage::Frame::isLoaded()
{
    return true;
}

/**
 * Return a pointer to the replica data for recovery.
 * load() never blocks for InMemoryStorage; prevents any further append()
 * calls from being accepted.
 */
void*
InMemoryStorage::Frame::load()
{
    startLoading();
    return buffer.get();
}

/**
 * Has no effect for InMemoryStorage.
 */
void
InMemoryStorage::Frame::unload()
{
}

/**
 * Append data to frame and update metadata.
 *
 * Idempotence: the caller must guarantee duplicated calls provide identical
 * arguments.
 *
 * append() after a load() or a close() throws an exception to
 * the master performing the append since it is either an error by the master
 * or the master has crashed.
 *
 * \param source
 *      Buffer contained the data to be copied into the frame.
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
InMemoryStorage::Frame::append(Buffer& source,
                               size_t sourceOffset,
                               size_t length,
                               size_t destinationOffset,
                               const void* metadata,
                               size_t metadataLength)
{
    Lock lock(storage->mutex);
    CycleCounter<uint64_t> ticks;
    if (!isOpen) {
        LOG(ERROR, "Tried to append to a frame but it wasn't"
            "open on this backup");
        throw BackupBadSegmentIdException(HERE);
    }
    if (loadRequested) {
        LOG(NOTICE, "Tried to append to a frame but it was already enqueued "
            "for load for recovery; calling master is probabaly already dead");
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

    if (metadata)
        memcpy(this->metadata.get(), metadata, metadataLength);

    storage->sleepToThrottleWrites(length + metadataLength, ticks.stop());
}

/**
 * Mark this frame as closed. Calls to close after a call to load() throw
 * BackupBadSegmentIdException which should kill the calling master; in this
 * case recovery has already started for them so they are likely already dead.
 */
void
InMemoryStorage::Frame::close()
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
}

// See BackupStorage.h for documentation.
void
InMemoryStorage::Frame::reopen(size_t length)
{
    // This method should never be invoked, since this subclass provides
    // no persistent storage.
}

/**
 * Do not call; see BackupStorage::freeFrame().
 * Make this frame available for reuse; data previously stored in this frame
 * may or may not be part of future recoveries.
 */
void
InMemoryStorage::Frame::free()
{
    Lock lock(storage->mutex);
    isOpen = false;
    isClosed = false;
    storage->freeMap[frameIndex] = 1;
}

// - private -

/**
 * Open the frame, resetting its state to accept appends for a new replica.
 * Open is not synchronous itself. Even after return from open() if this
 * backup crashes it may find the replica which was formerly stored in this
 * frame or metadata for the former replica and data for the newly open replica.
 * Recovery is expected to address these consistency issues with the checksums.
 *
 * Idempotence: Duplicate calls to open() are ignored until the frame is freed.
 * Calling open() after the frame is freed will reset this frame for reuse
 * with an new replica.
 */
void
InMemoryStorage::Frame::open()
{
    Lock _(storage->mutex);
    if (isOpen || isClosed)
        return;
    buffer.reset(new char[storage->segmentSize]);
    memset(buffer.get(), '\0', storage->segmentSize); // Quiet valgrind.
    isOpen = true;
    isClosed = false;
    memset(metadata.get(), '\0', METADATA_SIZE);
    loadRequested = false;
}

// --- InMemoryStorage ---

/**
 * Create an InMemoryStorage.
 *
 * \param segmentSize
 *      The size in bytes of the segments this storage will deal with.
 * \param frameCount
 *      The number of segments this storage can store simultaneously.
 * \param writeRateLimit
 *      When specified, writes to this storage instance should be
 *      limited to at most the given rate (in megabytes per second).
 *      The special value 0 turns off throttling.
 */
InMemoryStorage::InMemoryStorage(size_t segmentSize,
                                 size_t frameCount,
                                 size_t writeRateLimit)
    : BackupStorage(segmentSize, Type::MEMORY, writeRateLimit)
    , mutex()
    , frames()
    , frameCount(frameCount)
    , freeMap(frameCount)
    , lastAllocatedFrame(FreeMap::npos)
{
    for (size_t frame = 0; frame < frameCount; ++frame)
        frames.emplace_back(this, frame);
    freeMap.set();
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
 *      Ignored for InMemoryStorage. All append() calls store data
 *      synchronously.
 * \param masterId
 *      The server that owns the segment associated with this replica.
 * \param segmentId
 *      Unique identifier (in the log of masterId) of the segment
 *      associated with this replica.
 * \return
 *      Reference to a frame through which handles all IO for a single
 *      replica. Maintains a reference count; when destroyed if the
 *      reference count drops to zero the frame will be freed for reuse with
 *      another replica.
 */
BackupStorage::FrameRef
InMemoryStorage::open(bool sync, ServerId masterId, uint64_t segmentId)
{
    Lock lock(mutex);
    FreeMap::size_type next = freeMap.find_next(lastAllocatedFrame);
    if (next == FreeMap::npos) {
        next = freeMap.find_first();
        if (next == FreeMap::npos) {
            RAMCLOUD_CLOG(NOTICE, "Rejecting open: no free storage frames");
            throw BackupOpenRejectedException(HERE);
        }
    }
    lastAllocatedFrame = next;
    size_t frameIndex = next;
    assert(freeMap[frameIndex] == 1);
    freeMap[frameIndex] = 0;
    Frame* frame = &frames[frameIndex];
    lock.unlock();
    frame->open();
    return {frame, BackupStorage::freeFrame};
}

/**
 * Returns the maximum number of bytes of metadata that can be stored
 * which each append(). Also, how many bytes of getMetadata() are safe
 * for access after getMetadata() calls, though returned data may or may
 * not contain valid or meaningful (or even consistent with the
 * replica) metadata.
 */
size_t
InMemoryStorage::getMetadataSize()
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
InMemoryStorage::loadAllMetadata()
{
    std::vector<FrameRef> ret;
    ret.reserve(frames.size());
    foreach (auto& frame, frames) {
        frame.loadMetadata();
        assert(freeMap[frame.frameIndex] == 1);
        freeMap[frame.frameIndex] = 0;
        ret.push_back({&frame, BackupStorage::freeFrame});
    }
    return ret;
}

/**
 * No-op for InMemoryStorage.
 */
void
InMemoryStorage::resetSuperblock(ServerId serverId,
                                 const string& clusterName,
                                 const uint32_t frameSkipMask)
{
}

/**
 * Returns an empty Superblock for InMemoryStorage.
 */
BackupStorage::Superblock
InMemoryStorage::loadSuperblock()
{
    return {};
}

/**
 * No-op for InMemoryStorage.
 */
void
InMemoryStorage::quiesce()
{
}

/**
 * No-op for InMemoryStorage.
 */
void
InMemoryStorage::fry()
{
}

} // namespace RAMCloud
