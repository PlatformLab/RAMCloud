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
        : session(session)
{
}

BackupClient::~BackupClient()
{
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
    checkStatus(HERE);
}

/**
 * Get the objects stored for the given tablets of the given server.  This
 * object is a continuation that blocks until #responseBuffer is populated
 * when invoked.
 *
 * \param client
 *      The BackupClient whose Session should be used for the call.
 * \param masterId
 *      The id of the crashed master which is being recovered.
 * \param segmentId
 *      The id of the segment to recover which the crashed master had stored
 *      on this backup.
 * \param partitionId
 *      Which partition of those send are part of the will on
 *      startReadingData() to fetch the recovery segment of.
 * \param[out] responseBuffer
 *      An empty Buffer which will contain the filtered recovery segment
 *      upon return.
 */
BackupClient::GetRecoveryData::GetRecoveryData(BackupClient& client,
                                               uint64_t masterId,
                                               uint64_t segmentId,
                                               uint64_t partitionId,
                                               Buffer& responseBuffer)
    : client(client)
    , requestBuffer()
    , responseBuffer(responseBuffer)
    , state()
{
    BackupGetRecoveryDataRpc::Request&
        reqHdr(client.allocHeader<BackupGetRecoveryDataRpc>(requestBuffer));
    reqHdr.masterId = masterId;
    reqHdr.segmentId = segmentId;
    reqHdr.partitionId = partitionId;
    Transport::SessionRef session(client.getSession());
    state = client.send<BackupGetRecoveryDataRpc>(session,
                                                  requestBuffer,
                                                  responseBuffer);
}

/**
 * Block until the getRecoveryData call has completed and the response
 * Buffer passed to it has been populated with a filtered Segment.
 *
 * \throw BackupBadSegmentIdException
 *      If the segment unknown to the backup server or is not is
 *      recovery.
 */
void
BackupClient::GetRecoveryData::operator()()
{
    client.recv<BackupGetRecoveryDataRpc>(state);
    client.checkStatus(HERE);
    responseBuffer.truncateFront(sizeof(BackupGetRecoveryDataRpc::Response));
}

Transport::SessionRef
BackupClient::getSession()
{
    return session;
}

/**
 * Test that a server exists and is responsive.
 *
 * This operation issues a no-op RPC request, which causes
 * communication with the given server but doesn't actually do
 * anything on the server.
 *
 * \exception InternalError
 */
void
BackupClient::ping()
{
    Buffer req, resp;
    allocHeader<PingRpc>(req);
    sendRecv<PingRpc>(session, req, resp);
    checkStatus(HERE);
}

/**
 * Begin reading the objects stored for the given server from disk and
 * split them into recovery segments.
 *
 * \param masterId
 *      The id of the master whose data is to be recovered.
 * \param partitions
 *      The will of the crashed master which is used to determine how to
 *      build recovery segments from the backup's stored segments.
 * \return
 *      A set of segment IDs for that server which will be read from disk.
 */
vector<pair<uint64_t, uint32_t>>
BackupClient::startReadingData(uint64_t masterId,
                               const ProtoBuf::Tablets& partitions)
{
    Buffer req, resp;
    BackupStartReadingDataRpc::Request&
        reqHdr(allocHeader<BackupStartReadingDataRpc>(req));
    reqHdr.masterId = masterId;
    reqHdr.partitionsLength = ProtoBuf::serializeToResponse(req,
                                                            partitions);
    const BackupStartReadingDataRpc::Response&
        respHdr(sendRecv<BackupStartReadingDataRpc>(session, req, resp));
    checkStatus(HERE);

    uint64_t segmentIdCount = respHdr.segmentIdCount;
    resp.truncateFront(sizeof(respHdr));
    auto const* segmentIdsRaw = resp.getStart<pair<uint64_t, uint32_t>>();
    return vector<pair<uint64_t, uint32_t>>(segmentIdsRaw,
                                            segmentIdsRaw + segmentIdCount);
}

/**
 * Write the byte range specified in an open segment on the backup.
 * This object is a continuation that blocks until the backup responds.
 *
 * On success the backup server promises, to the best of its ability, to
 * provide the data contained in this segment during recovery of masterId
 * until such time the segment is freed via freeSegment().
 *
 * \param client
 *      The BackupClient whose Session should be used for the call.
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
 * \param flags
 *      Whether the write should open or close the segment or both or
 *      neither.  Defaults to neither.
 */
BackupClient::WriteSegment::WriteSegment(BackupClient& client,
                                         uint64_t masterId,
                                         uint64_t segmentId,
                                         uint32_t offset,
                                         const void *buf,
                                         uint32_t length,
                                         BackupWriteRpc::Flags flags)
    : client(client)
    , requestBuffer()
    , responseBuffer()
    , state()
{
    BackupWriteRpc::Request& reqHdr(
        client.allocHeader<BackupWriteRpc>(requestBuffer));
    reqHdr.masterId = masterId;
    reqHdr.segmentId = segmentId;
    reqHdr.offset = offset;
    reqHdr.length = length;
    reqHdr.flags = flags;
    Buffer::Chunk::appendToBuffer(&requestBuffer, buf, length);
    state = client.send<BackupWriteRpc>(client.session,
                                        requestBuffer,
                                        responseBuffer);
}

/**
 * Block until the writeSegment call has completed.
 */
void
BackupClient::WriteSegment::operator()()
{
    client.recv<BackupWriteRpc>(state);
    client.checkStatus(HERE);
}

} // namespace RAMCloud
