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
#include "ClientException.h"
#include "Log.h"
#include "Rpc.h"
#include "Segment.h"
#include "SegmentIterator.h"
#include "Status.h"
#include "TransportManager.h"

namespace RAMCloud {

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
{
}

/**
 * Flush open segments to disk and shutdown.
 */
BackupServer::~BackupServer()
{
    pool.purge_memory();
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
 * \param reqHdr
 *      Header of the Rpc request containing the segment number to close.
 * \param respHdr
 *      Header for the Rpc response.
 * \param rpc
 *      The Rpc being serviced.
 *
 * \throw BackupBadSegmentIdException
 *      If this segmentId is unknown for this master or this segment
 *      is not open.
 */
void
BackupServer::closeSegment(const BackupCloseRpc::Request& reqHdr,
                           BackupCloseRpc::Response& respHdr,
                           Transport::ServerRpc& rpc)
{
    LOG(DEBUG, "Handling: %s", __func__);
    closeSegment(reqHdr.masterId, reqHdr.segmentId);
}

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
    if (!info || info->state != SegmentInfo::OPEN)
        throw BackupBadSegmentIdException();

    storage.putSegment(info->storageHandle, info->segment);
    pool.free(info->segment);
    info->segment = NULL;
    info->state = SegmentInfo::CLOSED;
}

// See Server::dispatch.
void
BackupServer::dispatch(RpcType type, Transport::ServerRpc& rpc,
                       Responder& responder)
{
    switch (type) {
        case BackupCloseRpc::type:
            callHandler<BackupCloseRpc, BackupServer,
                        &BackupServer::closeSegment>(rpc);
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
 * Removes the specified segment from permanent storage and releases
 * any in memory copy if it exists.
 *
 * After this call completes the segment number segmentId will no longer
 * be in permanent storage and will not be recovered during recovery.
 *
 * \param reqHdr
 *      Header of the Rpc request containing the segment number to free.
 * \param respHdr
 *      Header for the Rpc response.
 * \param rpc
 *      The Rpc being serviced.
 *
 * \throw BackupBackupSegmentIdException
 *      If the segment does exist on this backup (never opened or was freed).
 */
void
BackupServer::freeSegment(const BackupFreeRpc::Request& reqHdr,
                          BackupFreeRpc::Response& respHdr,
                          Transport::ServerRpc& rpc)
{
    LOG(DEBUG, "Handle: %s", __func__);

    SegmentInfo* info = findSegmentInfo(reqHdr.masterId, reqHdr.segmentId);
    if (!info)
        throw BackupBadSegmentIdException();

    if (info->segment)
        pool.free(info->segment);
    info->segment = NULL;

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
 *
 * \throw BackupBadSegmentIdException
 *      If the segment is not in RECOVERING state (startReadingData() must have
 *      been called for its corresponding master id).
 */
void
BackupServer::getRecoveryData(const BackupGetRecoveryDataRpc::Request& reqHdr,
                              BackupGetRecoveryDataRpc::Response& respHdr,
                              Transport::ServerRpc& rpc)
try
{
    TEST_LOG("%lu, %lu", reqHdr.masterId, reqHdr.segmentId);

    ProtoBuf::Tablets tablets;
    ProtoBuf::parseFromResponse(rpc.recvPayload, sizeof(reqHdr),
                                reqHdr.tabletsLength, tablets);

    SegmentInfo* info = findSegmentInfo(reqHdr.masterId, reqHdr.segmentId);
    if (!info || info->state != SegmentInfo::RECOVERING)
        throw BackupBadSegmentIdException();

    // If not already in memory then reload it.
    if (!info->segment) {
        char* segment = static_cast<char*>(pool.malloc());
        try {
            storage.getSegment(info->storageHandle, segment);
        } catch (...) {
            pool.free(segment);
            throw;
        }
        info->segment = segment;
    }

    for (SegmentIterator it(info->segment, segmentSize);
         !it.isDone(); it.next())
    {
        if (!keepEntry(it.getType(), it.getPointer(), tablets))
            continue;

        uint32_t totalEntryLength = it.getLength() + sizeof(SegmentEntry);
        char* packedEntry =
            new(&rpc.replyPayload, APPEND) char[totalEntryLength];
        memcpy(packedEntry,
               static_cast<const char*>(it.getPointer()) - sizeof(SegmentEntry),
               totalEntryLength);
    }

    SegmentEntry* footerEntry = new(&rpc.replyPayload, APPEND) SegmentEntry;
    footerEntry->type = LOG_ENTRY_TYPE_SEGFOOTER;
    footerEntry->length = sizeof(SegmentFooter);
    SegmentFooter* footer = new(&rpc.replyPayload, APPEND) SegmentFooter;
    footer->checksum =
        SegmentIterator::generateChecksum(info->segment, segmentSize);
} catch (const SegmentIteratorException& e) {
    LOG(WARNING, "getRecoveryData failed due to malformed segment data: "
        "masterId %lu, segmentId %lu", reqHdr.masterId, reqHdr.segmentId);
    throw BackupMalformedSegmentException();
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
      case LOG_ENTRY_TYPE_SEGFOOTER:
        return false;
      case LOG_ENTRY_TYPE_OBJ:
        tableId = object->table;
        objectId = object->id;
        break;
      case LOG_ENTRY_TYPE_OBJTOMB:
        tableId = tombstone->tableId;
        objectId = tombstone->objectId;
        break;
      case LOG_ENTRY_TYPE_SEGHEADER:
      case LOG_ENTRY_TYPE_LOGDIGEST:
      case LOG_ENTRY_TYPE_INVALID:
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
 * \param reqHdr
 *      Header of the Rpc request containing the masterId and segmentId for
 *      the segment to open.
 * \param respHdr
 *      Header for the Rpc response.
 * \param rpc
 *      The Rpc being serviced.
 *
 * \throw BackupSegmentAlreadyOpenException
 *      If this segment is already open on this backup.
 */
void
BackupServer::openSegment(const BackupOpenRpc::Request& reqHdr,
                          BackupOpenRpc::Response& respHdr,
                          Transport::ServerRpc& rpc)
{
    LOG(DEBUG, "Handling: %s", __func__);

    SegmentInfo* info = findSegmentInfo(reqHdr.masterId, reqHdr.segmentId);
    if (info)
        throw BackupSegmentAlreadyOpenException();

    // Get memory for staging the segment writes
    char* segment = static_cast<char*>(pool.malloc());
    memset(segment, 0, segmentSize);
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
        uint64_t masterId = it->first.masterId;
        if (masterId == reqHdr.masterId) {
            uint64_t* segmentId = new(&rpc.replyPayload, APPEND) uint64_t;
            *segmentId = it->first.segmentId;
            it->second.state = SegmentInfo::RECOVERING;
            segmentIdCount++;
        }
    }
    respHdr.segmentIdCount = segmentIdCount;
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
    LOG(DEBUG, "%s: segment <%lu,%lu>",
        __func__, reqHdr.masterId, reqHdr.segmentId);

    SegmentInfo* info = findSegmentInfo(reqHdr.masterId, reqHdr.segmentId);
    if (!info || info->state != SegmentInfo::OPEN)
        throw BackupBadSegmentIdException();

    // Need to check all three conditions because overflow is possible
    // on the addition
    if (reqHdr.length > segmentSize ||
        reqHdr.offset > segmentSize ||
        reqHdr.length + reqHdr.offset > segmentSize)
        throw BackupSegmentOverflowException();

    rpc.recvPayload.copy(sizeof(reqHdr), reqHdr.length,
                         &info->segment[reqHdr.offset]);
}

} // namespace RAMCloud

