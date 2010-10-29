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

#include "BackupClient.h"
#include "Buffer.h"
#include "ClientException.h"
#include "Mark.h"
#include "PerfCounterType.h"
#include "Rpc.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Create a BackupClient.
 *
 * \param session
 *      The Session by which to communicate with the backup server.
 */
BackupClient::BackupClient(Transport::SessionRef session)
        : counterValue(0)
        , session(session)
        , status(STATUS_OK)
{
}

BackupClient::~BackupClient()
{
}

/**
 * Close an open segment on this backup.
 *
 * On success the backup server promises, to the best of its ability, to
 * ensure this segment reflects its closed status during recovery of masterId
 * until such time the segment is freed via freeSegment().  Futhermore, the
 * backup promises to diallow all modifications to the segment, henceforth
 * (aside from freeing it).
 *
 * \param masterId
 *      The id of the master of the segment to be opened.
 * \param segmentId
 *      The id of the segment to be opened.
 * \throw BackupBadSegmentIdException
 *      If the segment is not open or is unknown to the backup server.
 */
void
BackupClient::closeSegment(uint64_t masterId,
                           uint64_t segmentId)
{
    Buffer req, resp;
    BackupCloseRpc::Request& reqHdr(allocHeader<BackupCloseRpc>(req));
    reqHdr.masterId = masterId;
    reqHdr.segmentId = segmentId;
    sendRecv<BackupCloseRpc>(session, req, resp);
    checkStatus();
}

/**
 * Frees a closed segment from storage on this backup.
 *
 * On success the backup server promises, to the best of its ability, to
 * ensure the data contained in this segment is not provided during recovery
 * of masterId.  This allows the backup server to reuse its storage.
 *
 * \param masterId
 *      The id of the master of the segment to be freed.
 * \param segmentId
 *      The id of the segment to be freed.
 * \throw BackupBadSegmentIdException
 *      If the segment is not open or is unknown to the backup server.
 */
void
BackupClient::freeSegment(uint64_t masterId,
                          uint64_t segmentId)
{
    Buffer req, resp;
    BackupFreeRpc::Request& reqHdr(allocHeader<BackupFreeRpc>(req));
    reqHdr.masterId = masterId;
    reqHdr.segmentId = segmentId;
    sendRecv<BackupFreeRpc>(session, req, resp);
    checkStatus();
}

/**
 * Get the objects stored for the given tablets of the given server.
 *
 * \param masterId
 *      The id of the crashed master which is being recovered.
 * \param segmentId
 *      The id of the segment to recover which the crashed master had stored
 *      on this backup.
 * \param tablets
 *      A set of table is and object id ranges which is used to select
 *      which objects are send back as part of the recovery segment.
 * \param[out] resp
 *      An empty Buffer which will contain the filtered recovery segment
 *      upon return.
 * \throw BackupBadSegmentIdException
 *      If the segment unknown to the backup server or is not is recovery.
 */
void
BackupClient::getRecoveryData(uint64_t masterId,
                              uint64_t segmentId,
                              const TabletMap& tablets,
                              Buffer& resp)
{
    Buffer req;
    BackupGetRecoveryDataRpc::Request&
        reqHdr(allocHeader<BackupGetRecoveryDataRpc>(req));
    reqHdr.masterId = masterId;
    reqHdr.segmentId = segmentId;
    // TODO(stutsman) pass tablets argument!
    const BackupGetRecoveryDataRpc::Response&
        respHdr(sendRecv<BackupGetRecoveryDataRpc>(session, req, resp));
    checkStatus();
    resp.truncateFront(sizeof(respHdr));
}

Transport::SessionRef
BackupClient::getSession()
{
    return session;
}

/**
 * Allocate space to receive backup writes for a segment.
 *
 * On success the backup server promises, to the best of its ability, to
 * provide this empty segment during recovery of masterId
 * until such time the segment is freed via freeSegment().
 *
 * \param masterId
 *      Id of this server.
 * \param segmentId
 *      Id of the segment to be backed up.
 */
void
BackupClient::openSegment(uint64_t masterId,
                        uint64_t segmentId)
{
    Buffer req, resp;
    BackupOpenRpc::Request& reqHdr(allocHeader<BackupOpenRpc>(req));
    reqHdr.masterId = masterId;
    reqHdr.segmentId = segmentId;
    sendRecv<BackupOpenRpc>(session, req, resp);
    checkStatus();
}

/**
 * Begin reading the objects stored for the given server from disk.
 *
 * \param masterId
 *      The id of the master whose data is to be recovered.
 * \return
 *      A set of segment IDs for that server which will be read from disk.
 */
vector<uint64_t>
BackupClient::startReadingData(uint64_t masterId)
{
    Buffer req, resp;
    BackupStartReadingDataRpc::Request&
        reqHdr(allocHeader<BackupStartReadingDataRpc>(req));
    reqHdr.masterId = masterId;
    const BackupStartReadingDataRpc::Response&
        respHdr(sendRecv<BackupStartReadingDataRpc>(session, req, resp));
    checkStatus();

    uint64_t segmentIdCount = respHdr.segmentIdCount;
    resp.truncateFront(sizeof(respHdr));
    uint64_t const * segmentIdsRaw = resp.getStart<uint64_t>();
    return vector<uint64_t>(segmentIdsRaw, segmentIdsRaw + segmentIdCount);
}

/**
 * Write the byte range specified in an open segment on the backup.
 *
 * On success the backup server promises, to the best of its ability, to
 * provide the data contained in this segment during recovery of masterId
 * until such time the segment is freed via freeSegment().
 *
 * \param masterId
 *      The id of the master to which the data belongs.
 * \param segmentId
 *      The id of the segment on which the data is to be stored.
 * \param offset
 *      The position in the segment where this data will be placed.
 * \param buf
 *      The start of the data to be written into this segment.
 * \param length
 *      The length in bytes of the data to write.
 */
void
BackupClient::writeSegment(uint64_t masterId,
                         uint64_t segmentId,
                         uint32_t offset,
                         const void *buf,
                         uint32_t length)
{
    Buffer req, resp;
    BackupWriteRpc::Request& reqHdr(allocHeader<BackupWriteRpc>(req));
    reqHdr.masterId = masterId;
    reqHdr.segmentId = segmentId;
    reqHdr.offset = offset;
    reqHdr.length = length;
    Buffer::Chunk::appendToBuffer(&req, buf, length);
    sendRecv<BackupWriteRpc>(session, req, resp);
    checkStatus();
}

} // namespace RAMCloud
