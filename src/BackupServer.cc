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

#include "BackupServer.h"
#include "BackupStorage.h"
#include "Buffer.h"
#include "Log.h"
#include "Rpc.h"
#include "Segment.h"
#include "Status.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Create a BackupServer which is ready to run().
 *
 * \param storage
 *      The storage backend used to persist segments.
 */
BackupServer::BackupServer(BackupStorage& storage)
    : pool(storage.getSegmentSize())
    , segments()
    , segmentSize(storage.getSegmentSize())
    , storage(storage)
{
}

/**
 * Flush open segments to disk and shutdown.
 */
BackupServer::~BackupServer()
{
    pool.purge_memory();
}

/**
 * Accept and dispatch RPCs until the end of time.
 */
void __attribute__ ((noreturn))
BackupServer::run()
{
    while (true)
        handleRpc<BackupServer>();
}

// - private -

/**
 * Commit the specified segment to permanent storage.
 *
 * After this writeSegment() cannot be called for this segment id any
 * more.  The segment will be restored on recovery unless the client later
 * calls freeSegment() on it.
 *
 * \param reqHdr
 *      Header of the Rpc request containing the segment number to commit.
 * \param respHdr
 *      Header for the Rpc response.
 * \param rpc
 *      The Rpc being serviced.
 */
void
BackupServer::commitSegment(const BackupCommitRpc::Request& reqHdr,
                            BackupCommitRpc::Response& respHdr,
                            Transport::ServerRpc& rpc)
{
    LOG(DEBUG, "Handling: %s", __func__);
    SegmentInfo* info = findSegmentInfo(reqHdr.masterId, reqHdr.segmentId);
    if (!info)
        throw ClientException(STATUS_BACKUP_BAD_SEGMENT_ID);

    storage.putSegment(info->storageHandle, info->segment);
    pool.free(info->segment);
    info->segment = NULL;
}

/**
 * Direct incoming RPCs to the correct handler method.  See Server.
 *
 * \param type
 *      The type of the RPC request.
 * \param rpc
 *      The ServerRpc being serviced.
 */
void
BackupServer::dispatch(RpcType type, Transport::ServerRpc& rpc)
{
    switch (type) {
        case BackupCommitRpc::type:
            callHandler<BackupCommitRpc, BackupServer,
                        &BackupServer::commitSegment>(rpc);
            break;
        case BackupFreeRpc::type:
            callHandler<BackupFreeRpc, BackupServer,
                        &BackupServer::freeSegment>(rpc);
            break;
        case BackupGetRecoveryDataRpc::type:
            callHandler<BackupGetRecoveryDataRpc, BackupServer,
                        &BackupServer::getRecoveryData>(rpc);
            break;
        case BackupOpenRpc::type:
            callHandler<BackupOpenRpc, BackupServer,
                        &BackupServer::openSegment>(rpc);
            break;
        case BackupStartReadingDataRpc::type:
            callHandler<BackupStartReadingDataRpc, BackupServer,
                        &BackupServer::startReadingData>(rpc);
            break;
        case BackupWriteRpc::type:
            callHandler<BackupWriteRpc, BackupServer,
                        &BackupServer::writeSegment>(rpc);
            break;
        default:
            throw UnimplementedRequestError();
    }
}

/**
 * Removes the specified segment from permanent storage.
 *
 * After this call completes the segment number segmentId will no longer
 * be in permanent storage and will not be recovered during recovery.
 *
 * Failure with STATUS_BAD_SEGMENT_ID means the segment never existed
 * on this backup or it has already been freed.  Failure with
 * STATUS_BACKUP_SEGMENT_ALREADY_OPEN indicates that the segment is still
 * open on this backup and must be committed before being freed.
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
    LOG(DEBUG, "Handle: %s", __func__);

    SegmentInfo* info = findSegmentInfo(reqHdr.masterId, reqHdr.segmentId);
    if (!info)
        throw ClientException(STATUS_BACKUP_BAD_SEGMENT_ID);
    if (info->segment)
        throw ClientException(STATUS_BACKUP_SEGMENT_ALREADY_OPEN);

    segments.erase(MasterSegmentIdPair(reqHdr.masterId, reqHdr.segmentId));
    storage.free(info->storageHandle);
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
    return &it->second;
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
 */
void
BackupServer::getRecoveryData(const BackupGetRecoveryDataRpc::Request& reqHdr,
                              BackupGetRecoveryDataRpc::Response& respHdr,
                              Transport::ServerRpc& rpc)
{
    LOG(DEBUG, "Handling: %s", __func__);

    // TODO(stutsman) RAII
    char* segment = new char[segmentSize];
    //uint32_t partitions = 2;

    for (SegmentsMap::iterator it = segments.begin();
         it != segments.end(); it++)
    {
        uint64_t masterId = it->first.first;
        if (masterId != reqHdr.masterId)
            continue;

        //uint64_t segmentId = it->first.second;
        SegmentInfo &info = it->second;

        storage.getSegment(info.storageHandle, segment);

        // Iterate and bucket
        // TODO(stutsman) shouldn't have to box this in a Segment
        Segment s(segment, segmentSize, NULL);
        LogEntryIterator it(&s);
        const struct LogEntry *entry;
        const void *p;

        while (it.getNext(&entry, &p)) {
            if (entry->type == LOG_ENTRY_TYPE_SEGMENT_HEADER ||
                entry->type == LOG_ENTRY_TYPE_SEGMENT_CHECKSUM)
                continue;
            LOG(ERROR, "Found object or tombstone: %u %u",
                entry->type, entry->length);
        }
    }

}

/**
 * Allocate space to receive backup writes for a segment.  If this call
 * succeeds the Backup must not reject subsequest writes to this segment for
 * lack of space.
 *
 * The caller must ensure that this masterId,segmentId pair is unique and
 * hasn't been used before on this backup or any other.  Returns a status
 * of STATUS_BACKUP_SEGMENT_ALREADY_OPEN if this segment is already open on
 * this backup.
 *
 * The storage space remains in use until the master calls freeSegment() or
 * until the backup * crashes.
 *
 * \param reqHdr
 *      Header of the Rpc request containing the masterId and segmentId for
 *      the segment to open.
 * \param respHdr
 *      Header for the Rpc response.
 * \param rpc
 *      The Rpc being serviced.
 */
void
BackupServer::openSegment(const BackupOpenRpc::Request& reqHdr,
                          BackupOpenRpc::Response& respHdr,
                          Transport::ServerRpc& rpc)
{
    LOG(DEBUG, "Handling: %s", __func__);

    SegmentInfo* info = findSegmentInfo(reqHdr.masterId, reqHdr.segmentId);
    if (info)
        throw ClientException(STATUS_BACKUP_SEGMENT_ALREADY_OPEN);

    // Get memory for staging the segment writes
    char* segment = static_cast<char*>(pool.malloc());
    BackupStorage::Handle* handle;
    try {
        // Reserve the space for this on disk
        handle = storage.allocate(reqHdr.masterId, reqHdr.segmentId);
    } catch (...) {
        // Release the staging memory if storage.allocate throws
        pool.free(segment);
        throw;
    }

    segments[MasterSegmentIdPair(reqHdr.masterId, reqHdr.segmentId)] =
        SegmentInfo(segment, handle);
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
BackupServer::startReadingData(const BackupStartReadingDataRpc::Request& reqHdr,
                               BackupStartReadingDataRpc::Response& respHdr,
                               Transport::ServerRpc& rpc)
{
    LOG(DEBUG, "Handling: %s", __func__);

    // TODO(stutsman) use aio to get data into RAM before getRecoveryData
    uint32_t segmentIdCount = 0;
    for (SegmentsMap::iterator it = segments.begin();
         it != segments.end(); it++)
    {
        if (it->first.first == reqHdr.masterId) {
            Buffer::Chunk::appendToBuffer(&rpc.replyPayload,
                                          &it->first.second,
                                          sizeof(it->first.second));
            segmentIdCount++;
        }
    }
    respHdr.segmentIdCount = segmentIdCount;
}

/**
 * Store an opaque string of bytes in a currently open segment on
 * this backup server.
 *
 * Status is STATUS_BACKUP_SEGMENT_OVERFLOW if the write request is beyond
 * the end of the segment.  Status is STATUS_BACKUP_BAD_SEGMENT_ID if the
 * segment is not open (see openSegment()).
 *
 * \param reqHdr
 *      Header of the Rpc request which contains the Rpc arguments except
 *      the data to be written.
 * \param respHdr
 *      Header for the Rpc response.
 * \param rpc
 *      The Rpc being serviced, used for access to the opaque bytes to
 *      be written which follow reqHdr.
 */
void
BackupServer::writeSegment(const BackupWriteRpc::Request& reqHdr,
                           BackupWriteRpc::Response& respHdr,
                           Transport::ServerRpc& rpc)
{
    LOG(DEBUG, "%s: segment <%lu,%lu>",
        __func__, reqHdr.masterId, reqHdr.segmentId);

    SegmentInfo* info = findSegmentInfo(reqHdr.masterId, reqHdr.segmentId);
    if (!info)
        throw ClientException(STATUS_BACKUP_BAD_SEGMENT_ID);

    // Need to check all three conditions because overflow is possible
    // on the addition
    if (reqHdr.length > segmentSize ||
        reqHdr.offset > segmentSize ||
        reqHdr.length + reqHdr.offset > segmentSize)
        throw ClientException(STATUS_BACKUP_SEGMENT_OVERFLOW);

    rpc.recvPayload.copy(sizeof(reqHdr), reqHdr.length,
                         &info->segment[reqHdr.offset]);
}

} // namespace RAMCloud

