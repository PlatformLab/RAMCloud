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
 * \param client
 *      The BackupClient whose Session should be used for the call.
 * \param masterId
 *      The id of the master of the segment to be freed.
 * \param segmentId
 *      The id of the segment to be freed.
 * \throw BackupBadSegmentIdException
 *      If the segment is not open or is unknown to the backup server.
 */
BackupClient::FreeSegment::FreeSegment(BackupClient& client,
                                       ServerId masterId,
                                       uint64_t segmentId)
    : client(client)
    , requestBuffer()
    , responseBuffer()
    , state()
{
    BackupFreeRpc::Request& reqHdr(
        client.allocHeader<BackupFreeRpc>(requestBuffer));
    reqHdr.masterId = *masterId;
    reqHdr.segmentId = segmentId;
    state = client.send<BackupFreeRpc>(client.session,
                                       requestBuffer,
                                       responseBuffer);
}

/**
 * Block until the freeSegment call has completed.
 */
void
BackupClient::FreeSegment::operator()()
{
    client.recv<BackupFreeRpc>(state);
    client.checkStatus(HERE);
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
                                               ServerId masterId,
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
    reqHdr.masterId = *masterId;
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
 * Flush all data to storage.
 * Returns once all dirty buffers have been written to storage.
 * This is useful for measuring recovery performance accurately.
 *
 * \exception InternalError
 */
void
BackupClient::quiesce()
{
    Buffer req, resp;
    allocHeader<BackupQuiesceRpc>(req);
    sendRecv<BackupQuiesceRpc>(session, req, resp);
    checkStatus(HERE);
}

/**
 * Signal to the backup server that recovery has completed. The backup server
 * will then free any resources it has for the recovered master.
 */
BackupClient::RecoveryComplete::RecoveryComplete(BackupClient& client,
                                                 ServerId masterId)
    : client(client)
    , requestBuffer()
    , responseBuffer()
    , state()
{
    auto& reqHdr =
        client.allocHeader<BackupRecoveryCompleteRpc>(requestBuffer);
    reqHdr.masterId = masterId.getId();
    state = client.send<BackupRecoveryCompleteRpc>(client.session,
                                                   requestBuffer,
                                                   responseBuffer);
}
void
BackupClient::RecoveryComplete::operator()()
{
    client.recv<BackupRecoveryCompleteRpc>(state);
    client.checkStatus(HERE);
}

/**
 * Begin reading the objects stored for the given server from disk and
 * split them into recovery segments.
 *
 * \param client
 *      The BackupClient whose Session should be used for the call.
 * \param masterId
 *      The id of the master whose data is to be recovered.
 * \param partitions
 *      The will of the crashed master which is used to determine how to
 *      build recovery segments from the backup's stored segments.
 */
BackupClient::StartReadingData::StartReadingData(
    BackupClient& client,
    ServerId masterId,
    const ProtoBuf::Tablets& partitions)
    : client(client)
    , requestBuffer()
    , responseBuffer()
    , state()
{
    BackupStartReadingDataRpc::Request&
        reqHdr(client.allocHeader<BackupStartReadingDataRpc>(requestBuffer));
    reqHdr.masterId = masterId.getId();
    reqHdr.partitionsLength = ProtoBuf::serializeToResponse(requestBuffer,
                                                            partitions);
    Transport::SessionRef session(client.getSession());
    state = client.send<BackupStartReadingDataRpc>(session,
                                                   requestBuffer,
                                                   responseBuffer);
}

/**
 * \param[out] result
 *      Return a set of segment IDs for masterId which will be read from disk
 *      along with their written lengths, as well as a buffer containing
 *      the LogDigest of the newest open Segment from this masterId, if one
 *      exists.
 */
void
BackupClient::StartReadingData::operator()(
    BackupClient::StartReadingData::Result* result)
{
    const BackupStartReadingDataRpc::Response& respHdr(
        client.recv<BackupStartReadingDataRpc>(state));
    client.checkStatus(HERE);

    uint64_t segmentIdCount = respHdr.segmentIdCount;
    uint64_t primarySegmentCount = respHdr.primarySegmentCount;
    uint32_t digestBytes = respHdr.digestBytes;
    uint64_t digestSegmentId = respHdr.digestSegmentId;
    uint64_t digestSegmentLen = respHdr.digestSegmentLen;

    responseBuffer.truncateFront(sizeof(respHdr));
    auto const* segmentIdsRaw =
        responseBuffer.getStart<pair<uint64_t, uint32_t>>();
    // TODO(ongaro): It's not safe to get a pointer to something in a buffer
    // and then modify that buffer.

    const void* digestPtr = NULL;
    if (digestBytes > 0) {
        responseBuffer.truncateFront(downCast<uint32_t>(segmentIdCount *
            sizeof(pair<uint64_t, uint32_t>)));
        digestPtr = responseBuffer.getRange(0, digestBytes);
    }

    result->set(segmentIdsRaw, segmentIdCount,
                downCast<uint32_t>(primarySegmentCount),
                digestPtr, digestBytes, digestSegmentId,
                downCast<uint32_t>(digestSegmentLen));
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
                                         ServerId masterId,
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
    reqHdr.masterId = *masterId;
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
