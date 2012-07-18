/* Copyright (c) 2009-2012 Stanford University
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
 * Assign a backup a replicationId and notify it of its replication group
 * members.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param backupId
 *      The id of a backup server.
 * \param replicationId
 *      A unique Id that specifies the replication group that the backup
 *      belongs to. All of the replicas of a given segment are replicated to
 *      the same replication group.
 * \param numReplicas
 *      The number of replicas for each segment. This is also the number of
 *      replicas in a given replication group.
 * \param replicationGroupIds
 *      The ServerId's of all the backups in the replication group.
 */
void
BackupClient::assignGroup(Context& context, ServerId backupId,
        uint64_t replicationId, uint32_t numReplicas,
        const ServerId* replicationGroupIds)
{
    AssignGroupRpc2 rpc(context, backupId, replicationId, numReplicas,
            replicationGroupIds);
    rpc.wait();
}

/**
 * Constructor for AssignGroupRpc2: initiates an RPC in the same way as
 * #BackupClient::assignGroup, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param backupId
 *      The id of a backup server.
 * \param replicationId
 *      A unique Id that specifies the replication group that the backup
 *      belongs to. All of the replicas of a given segment are replicated to
 *      the same replication group.
 * \param numReplicas
 *      The number of replicas for each segment. This is also the number of
 *      replicas in a given replication group.
 * \param replicationGroupIds
 *      The ServerId's of all the backups in the replication group.
 */
AssignGroupRpc2::AssignGroupRpc2(Context& context, ServerId backupId,
        uint64_t replicationId, uint32_t numReplicas,
        const ServerId* replicationGroupIds)
    : ServerIdRpcWrapper(context, backupId,
            sizeof(WireFormat::BackupAssignGroup::Response))
{
    WireFormat::BackupAssignGroup::Request& reqHdr(
            allocHeader<WireFormat::BackupAssignGroup>());
    reqHdr.replicationId = replicationId;
    reqHdr.numReplicas = numReplicas;
    uint64_t* dest = new(&request, APPEND) uint64_t[numReplicas];
    for (uint32_t i = 0; i < numReplicas; i++) {
        dest[i] = replicationGroupIds[i].getId();
    }
    send();
}

/**
 * Notify a backup that it can reclaim the storage for a given segment.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param backupId
 *      The id of a backup server.
 * \param masterId
 *      The id of the master that created the segment to be freed.
 * \param segmentId
 *      The id of the segment to be freed.
 */
void
BackupClient::freeSegment(Context& context, ServerId backupId,
        ServerId masterId, uint64_t segmentId)
{
    FreeSegmentRpc2 rpc(context, backupId, masterId, segmentId);
    rpc.wait();
}

/**
 * Constructor for FreeSegmentRpc2: initiates an RPC in the same way as
 * #BackupClient::freeSegment, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param backupId
 *      The id of a backup server.
 * \param masterId
 *      The id of the master that created the segment to be freed.
 * \param segmentId
 *      The id of the segment to be freed.
 */
FreeSegmentRpc2::FreeSegmentRpc2(Context& context, ServerId backupId,
        ServerId masterId, uint64_t segmentId)
    : ServerIdRpcWrapper(context, backupId,
            sizeof(WireFormat::BackupFree::Response))
{
    WireFormat::BackupFree::Request& reqHdr(
            allocHeader<WireFormat::BackupFree>());
    reqHdr.masterId = masterId.getId();
    reqHdr.segmentId = segmentId;
    send();
}

/**
 * This method is invoked by recovery masters during crash recovery: it
 * retrieves from a backup all the objects from a particular segment that
 * belong to a particular recovery partition.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param backupId
 *      Identifies a particular backup, which is believed to hold a
 *      replica of the desired segment.
 * \param masterId
 *      The id of the master that created the desired segment.
 * \param segmentId
 *      The id of the desired segment.
 * \param partitionId
 *      Identifies a collection of tablets: only objects in this partition
 *      will be returned by the backup.
 * \param[out] response
 *      The objects matching the above parameters will be returned in this
 *      buffer, organized as a Segment.
 */
void
BackupClient::getRecoveryData(Context& context, ServerId backupId,
        ServerId masterId, uint64_t segmentId, uint64_t partitionId,
        Buffer& response)
{
    GetRecoveryDataRpc2 rpc(context, backupId, masterId, segmentId,
            partitionId, response);
    rpc.wait();
}

/**
 * Constructor for GetRecoveryDataRpc2: initiates an RPC in the same way as
 * #BackupClient::getRecoveryData, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param backupId
 *      Identifies a particular backup, which is believed to hold a
 *      replica of the desired segment.
 * \param masterId
 *      The id of the master that created the desired segment.
 * \param segmentId
 *      The id of the desired segment.
 * \param partitionId
 *      Identifies a collection of tablets: only objects in this partition
 *      will be returned by the backup.
 * \param[out] response
 *      The objects matching the above parameters will be returned in this
 *      buffer, organized as a Segment.
 */
GetRecoveryDataRpc2::GetRecoveryDataRpc2(Context& context, ServerId backupId,
        ServerId masterId, uint64_t segmentId, uint64_t partitionId,
        Buffer& response)
    : ServerIdRpcWrapper(context, backupId,
            sizeof(WireFormat::BackupGetRecoveryData::Response), &response)
{
    WireFormat::BackupGetRecoveryData::Request& reqHdr(
            allocHeader<WireFormat::BackupGetRecoveryData>());
    reqHdr.masterId = masterId.getId();
    reqHdr.segmentId = segmentId;
    reqHdr.partitionId = partitionId;
    send();
}

/**
 * Wait for a getRecoveryData RPC to complete, and throw exceptions for
 * any errors.
 *
 * \throw ServerDoesntExistException
 *      The intended server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */
void
GetRecoveryDataRpc2::wait()
{
    waitAndCheckErrors();
    response->truncateFront(sizeof(
            WireFormat::BackupGetRecoveryData::Response));
}

/**
 * Ask a backup to flush all of its data to durable storage.
 * Returns once all dirty buffers have been written to storage.
 * This is useful for measuring recovery performance accurately.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param backupId
 *      Backup whose data should be flushed.
 */
void
BackupClient::quiesce(Context& context, ServerId backupId)
{
    BackupQuiesceRpc2 rpc(context, backupId);
    rpc.wait();
}

/**
 * Constructor for QuiesceRpc2: initiates an RPC in the same way as
 * #BackupClient::quiesce, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param backupId
 *      Backup whose data should be flushed.
 */
BackupQuiesceRpc2::BackupQuiesceRpc2(Context& context, ServerId backupId)
    : ServerIdRpcWrapper(context, backupId,
            sizeof(WireFormat::BackupQuiesce::Response))
{
    allocHeader<WireFormat::BackupQuiesce>();
    send();
}

/**
 * This RPC signals to particular backup that recovery of a particular master
 * has completed. The backup server will then free any resources it has for the
 * recovered master.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param backupId
 *      Backup that will receive this RPC.
 * \param masterId
 *      The id of a crashed master whose recovery is now complete.
 */
void
BackupClient::recoveryComplete(Context& context, ServerId backupId,
        ServerId masterId)
{
    RecoveryCompleteRpc2 rpc(context, backupId, masterId);
    rpc.wait();
}

/**
 * Constructor for RecoveryCompleteRpc2: initiates an RPC in the same way as
 * #BackupClient::recoveryComplete, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param backupId
 *      Backup that will receive this RPC.
 * \param masterId
 *      The id of a crashed master whose recovery is now complete.
 */
RecoveryCompleteRpc2::RecoveryCompleteRpc2(Context& context, ServerId backupId,
        ServerId masterId)
    : ServerIdRpcWrapper(context, backupId,
            sizeof(WireFormat::BackupRecoveryComplete::Response))
{
    WireFormat::BackupRecoveryComplete::Request& reqHdr(
            allocHeader<WireFormat::BackupRecoveryComplete>());
    reqHdr.masterId = masterId.getId();
    send();
}


/**
 * This RPC is invoked at the beginning of recovering from a crashed master;
 * it asks a particular backup to begin reading from disk the segment replicas
 * from the crashed master.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param backupId
 *      Backup that will receive this RPC.
 * \param masterId
 *      The id of the master whose data is to be recovered.
 * \param partitions
 *      Describes how the objects belonging to \a masterId are to be divided
 *      into groups for recovery.
 *
 * \return
 *      The return value is an object that describes all of the segment
 *      replicas stored on \a backupId for \a masterId.
 */
StartReadingDataRpc2::Result
BackupClient::startReadingData(Context& context, ServerId backupId,
        ServerId masterId, const ProtoBuf::Tablets& partitions)
{
    StartReadingDataRpc2 rpc(context, backupId, masterId, partitions);
    return rpc.wait();
}

/**
 * Constructor for StartReadingDataRpc2: initiates an RPC in the same way as
 * #BackupClient::startReadingData, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param backupId
 *      Backup that will receive this RPC.
 * \param masterId
 *      The id of the master whose data is to be recovered.
 * \param partitions
 *      Describes how the objects belonging to \a masterId are to be divided
 *      into groups for recovery.
 */
StartReadingDataRpc2::StartReadingDataRpc2(Context& context, ServerId backupId,
        ServerId masterId, const ProtoBuf::Tablets& partitions)
    : ServerIdRpcWrapper(context, backupId,
            sizeof(WireFormat::BackupStartReadingData::Response))
{
    WireFormat::BackupStartReadingData::Request& reqHdr(
            allocHeader<WireFormat::BackupStartReadingData>());
    reqHdr.masterId = masterId.getId();
    reqHdr.partitionsLength = ProtoBuf::serializeToResponse(request,
            partitions);
    send();
}

/**
 * Wait for a startReadingData RPC to complete.
 *
 * \return
 *      The return value is an object that describes all of the segment
 *      replicas stored on \a backupId for \a masterId.
 *
 * \throw ServerDoesntExistException
 *      The intended server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */
StartReadingDataRpc2::Result
StartReadingDataRpc2::wait()
{
    waitAndCheckErrors();
    const WireFormat::BackupStartReadingData::Response& respHdr(
            getResponseHeader<WireFormat::BackupStartReadingData>());

    Result result;

    uint32_t segmentIdCount = respHdr.segmentIdCount;
    uint32_t primarySegmentCount = respHdr.primarySegmentCount;
    uint32_t digestBytes = respHdr.digestBytes;
    uint64_t digestSegmentId = respHdr.digestSegmentId;
    uint32_t digestSegmentLen = respHdr.digestSegmentLen;
    response->truncateFront(sizeof(respHdr));

    // segmentIdAndLength
    typedef BackupStartReadingDataRpc::Replica Replica;
    const Replica* replicaArray = response->getStart<Replica>();
    for (uint64_t i = 0; i < segmentIdCount; ++i) {
        const Replica& replica = replicaArray[i];
        result.segmentIdAndLength.push_back({replica.segmentId,
                                             replica.segmentLength});
    }
    response->truncateFront(
        downCast<uint32_t>(segmentIdCount * sizeof(Replica)));

    // primarySegmentCount
    result.primarySegmentCount = primarySegmentCount;

    // logDigest fields
    if (digestBytes > 0) {
        result.logDigestBuffer.reset(new char[digestBytes]);
        response->copy(0, digestBytes,
                            result.logDigestBuffer.get());
        result.logDigestBytes = digestBytes;
        result.logDigestSegmentId = digestSegmentId;
        result.logDigestSegmentLen = digestSegmentLen;
    }
    return result;
}

StartReadingDataRpc2::Result::Result()
    : segmentIdAndLength()
    , primarySegmentCount(0)
    , logDigestBuffer()
    , logDigestBytes(0)
    , logDigestSegmentId(-1)
    , logDigestSegmentLen(-1)
{
}

StartReadingDataRpc2::Result::Result(Result&& other)
    : segmentIdAndLength(std::move(other.segmentIdAndLength))
    , primarySegmentCount(other.primarySegmentCount)
    , logDigestBuffer(std::move(other.logDigestBuffer))
    , logDigestBytes(other.logDigestBytes)
    , logDigestSegmentId(other.logDigestSegmentId)
    , logDigestSegmentLen(other.logDigestSegmentLen)
{
}

StartReadingDataRpc2::Result&
StartReadingDataRpc2::Result::operator=(Result&& other)
{
    segmentIdAndLength = std::move(other.segmentIdAndLength);
    primarySegmentCount = other.primarySegmentCount;
    logDigestBuffer = std::move(other.logDigestBuffer);
    logDigestBytes = other.logDigestBytes;
    logDigestSegmentId = other.logDigestSegmentId;
    logDigestSegmentLen = other.logDigestSegmentLen;
    return *this;
}

/**
 * Write data to open segment replica on a given backup. On success
 * the backup server promises, to the best of its ability, to provide
 * the data contained in this segment during recovery of masterId
 * until such time the segment is freed via freeSegment().
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param backupId
 *      Backup that will store the segment data.
 * \param masterId
 *      The id of the master to which the data belongs.
 * \param segmentId
 *      The id of the segment on which the data is to be stored.
 * \param offset
 *      The position in the segment where this data will be placed.
 * \param buf
 *      The actual data to be written into this segment.
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
 *
 * \return
 *      A vector describing the replication group for the backup
 *      that handled the RPC (secondary replicas will then be
 *      replicated to this group of backups).  The list includes
 *      the backup that handled this RPC.
 */
vector<ServerId>
BackupClient::writeSegment(Context& context, ServerId backupId,
        ServerId masterId, uint64_t segmentId, uint32_t offset,
        const void* buf, uint32_t length, BackupWriteRpc::Flags flags,
        bool atomic)
{
    WriteSegmentRpc2 rpc(context, backupId, masterId, segmentId, offset,
        buf, length, flags, atomic);
    return rpc.wait();
}

/**
 * Constructor for WriteSegmentRpc2: initiates an RPC in the same way as
 * #BackupClient::writeSegment, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param backupId
 *      Backup that will store the segment data.
 * \param masterId
 *      The id of the master to which the data belongs.
 * \param segmentId
 *      The id of the segment on which the data is to be stored.
 * \param offset
 *      The position in the segment where this data will be placed.
 * \param buf
 *      The actual data to be written into this segment.
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
WriteSegmentRpc2::WriteSegmentRpc2(Context& context, ServerId backupId,
        ServerId masterId, uint64_t segmentId, uint32_t offset,
        const void* buf, uint32_t length, BackupWriteRpc::Flags flags,
        bool atomic)
    : ServerIdRpcWrapper(context, backupId,
            sizeof(WireFormat::BackupWrite::Response))
{
    WireFormat::BackupWrite::Request& reqHdr(
            allocHeader<WireFormat::BackupWrite>());
    reqHdr.masterId = masterId.getId();
    reqHdr.segmentId = segmentId;
    reqHdr.offset = offset;
    reqHdr.length = length;
    reqHdr.flags = flags;
    reqHdr.atomic = atomic;
    Buffer::Chunk::appendToBuffer(&request, buf, length);
    send();
}

/**
 * Wait for a writeSegment RPC to complete.
 *
 * \return
 *      A vector describing the replication group for the backup
 *      that handled the RPC (secondary replicas will then be
 *      replicated to this group of backups).  The list includes
 *      the backup that handled this RPC.
 *
 * \throw ServerDoesntExistException
 *      The intended server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */
vector<ServerId>
WriteSegmentRpc2::wait()
{
    waitAndCheckErrors();
    const WireFormat::BackupWrite::Response& respHdr(
            getResponseHeader<WireFormat::BackupWrite>());
    vector<ServerId> group;
    uint32_t respOffset = downCast<uint32_t>(sizeof(respHdr));
    for (uint32_t i = 0; i < respHdr.numReplicas; i++) {
        const uint64_t *backupId =
            response->getOffset<uint64_t>(respOffset);
        group.push_back(ServerId(*backupId));
        respOffset += downCast<uint32_t>(sizeof(*backupId));
    }
    return group;
}

} // namespace RAMCloud
