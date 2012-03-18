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
#include "ShortMacros.h"
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
 * Assign the backup a replicationId and notify it of its replication group
 * members.
 */
BackupClient::AssignGroup::AssignGroup(BackupClient& client,
                                       ServerId masterId,
                                       uint64_t replicationId,
                                       uint32_t numReplicas,
                                       uint64_t* replicationGroupIds)
                                       : client(client)
                                       , requestBuffer()
                                       , responseBuffer()
                                       , state()
{
    auto& reqHdr = client.allocHeader<BackupAssignGroupRpc>(requestBuffer);
    reqHdr.masterId = masterId.getId();
    reqHdr.replicationId = replicationId;
    reqHdr.numReplicas = numReplicas;
    uint64_t* dest = new(&requestBuffer, APPEND) uint64_t[numReplicas];
    for (uint32_t i = 0; i < numReplicas; i++) {
        dest[i] = replicationGroupIds[i];
    }
    state = client.send<BackupAssignGroupRpc>(client.session,
                                              requestBuffer,
                                              responseBuffer);
}

void
BackupClient::AssignGroup::operator()()
{
    client.recv<BackupAssignGroupRpc>(state);
    client.checkStatus(HERE);
}

// class BackupClient::StartReadingData::Result

BackupClient::StartReadingData::Result::Result()
    : segmentIdAndLength()
    , primarySegmentCount(0)
    , logDigestBuffer()
    , logDigestBytes(0)
    , logDigestSegmentId(-1)
    , logDigestSegmentLen(-1)
{
}

BackupClient::StartReadingData::Result::Result(Result&& other)
    : segmentIdAndLength(std::move(other.segmentIdAndLength))
    , primarySegmentCount(other.primarySegmentCount)
    , logDigestBuffer(std::move(other.logDigestBuffer))
    , logDigestBytes(other.logDigestBytes)
    , logDigestSegmentId(other.logDigestSegmentId)
    , logDigestSegmentLen(other.logDigestSegmentLen)
{
}

BackupClient::StartReadingData::Result&
BackupClient::StartReadingData::Result::operator=(Result&& other)
{
    segmentIdAndLength = std::move(other.segmentIdAndLength);
    primarySegmentCount = other.primarySegmentCount;
    logDigestBuffer = std::move(other.logDigestBuffer);
    logDigestBytes = other.logDigestBytes;
    logDigestSegmentId = other.logDigestSegmentId;
    logDigestSegmentLen = other.logDigestSegmentLen;
    return *this;
}

// class BackupClient::StartReadingData

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

BackupClient::StartReadingData::Result
BackupClient::StartReadingData::operator()()
{
    const BackupStartReadingDataRpc::Response& respHdr(
        client.recv<BackupStartReadingDataRpc>(state));
    client.checkStatus(HERE);

    Result result;

    uint32_t segmentIdCount = respHdr.segmentIdCount;
    uint32_t primarySegmentCount = respHdr.primarySegmentCount;
    uint32_t digestBytes = respHdr.digestBytes;
    uint64_t digestSegmentId = respHdr.digestSegmentId;
    uint32_t digestSegmentLen = respHdr.digestSegmentLen;
    responseBuffer.truncateFront(sizeof(respHdr));

    // segmentIdAndLength
    typedef BackupStartReadingDataRpc::Replica Replica;
    const Replica* replicaArray = responseBuffer.getStart<Replica>();
    for (uint64_t i = 0; i < segmentIdCount; ++i) {
        const Replica& replica = replicaArray[i];
        result.segmentIdAndLength.push_back({replica.segmentId,
                                             replica.segmentLength});
    }
    responseBuffer.truncateFront(
        downCast<uint32_t>(segmentIdCount * sizeof(Replica)));

    // primarySegmentCount
    result.primarySegmentCount = primarySegmentCount;

    // logDigest fields
    if (digestBytes > 0) {
        result.logDigestBuffer.reset(new char[digestBytes]);
        responseBuffer.copy(0, digestBytes,
                            result.logDigestBuffer.get());
        result.logDigestBytes = digestBytes;
        result.logDigestSegmentId = digestSegmentId;
        result.logDigestSegmentLen = digestSegmentLen;
    }
    return result;
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
 * \param atomic
 *      If true then this replica is considered invalid until a closing
 *      write (or subsequent call to write with \a atomic set to false).
 *      This means that the data will never be written to disk and will
 *      not be reported to or used in recoveries unless the replica is
 *      closed.  This allows masters to create replicas of segments
 *      without the threat that they'll be detected as the head of the
 *      log.  Each value of \a atomic for each write call overrides the
 *      last, so in order to atomically write an entire segment all
 *      writes must have \a atomic set to true (though, it is
 *      irrelvant for the last, closing write).  A call with atomic
 *      set to false will make that replica available for normal
 *      treatment as an open segment.
 */
BackupClient::WriteSegment::WriteSegment(BackupClient& client,
                                         ServerId masterId,
                                         uint64_t segmentId,
                                         uint32_t offset,
                                         const void *buf,
                                         uint32_t length,
                                         BackupWriteRpc::Flags flags,
                                         bool atomic)
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
    reqHdr.atomic = atomic;
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
