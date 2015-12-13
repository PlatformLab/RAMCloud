/* Copyright (c) 2009-2015 Stanford University
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
#include "CycleCounter.h"
#include "RawMetrics.h"
#include "Segment.h"
#include "ShortMacros.h"
#include "TransportManager.h"

namespace RAMCloud {

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
BackupClient::freeSegment(Context* context, ServerId backupId,
        ServerId masterId, uint64_t segmentId)
{
    FreeSegmentRpc rpc(context, backupId, masterId, segmentId);
    rpc.wait();
}

/**
 * Constructor for FreeSegmentRpc: initiates an RPC in the same way as
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
FreeSegmentRpc::FreeSegmentRpc(Context* context, ServerId backupId,
        ServerId masterId, uint64_t segmentId)
    : ServerIdRpcWrapper(context, backupId,
            sizeof(WireFormat::BackupFree::Response))
{
    WireFormat::BackupFree::Request* reqHdr(
            allocHeader<WireFormat::BackupFree>(backupId));
    reqHdr->masterId = masterId.getId();
    reqHdr->segmentId = segmentId;
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
 * \param recoveryId
 *      Which recovery this master is requesting a recovery segment for.
 *      Recovery segments may be partitioned differently for different
 *      recoveries even for the same master. This prevents accidental
 *      use of mispartitioned segments.
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
SegmentCertificate
BackupClient::getRecoveryData(Context* context,
                              ServerId backupId,
                              uint64_t recoveryId,
                              ServerId masterId,
                              uint64_t segmentId,
                              uint64_t partitionId,
                              Buffer* response)
{
    GetRecoveryDataRpc rpc(context, backupId, recoveryId,
                           masterId, segmentId,
                           partitionId, response);
    return rpc.wait();
}

/**
 * Constructor for GetRecoveryDataRpc: initiates an RPC in the same way as
 * #BackupClient::getRecoveryData, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param backupId
 *      Identifies a particular backup, which is believed to hold a
 *      replica of the desired segment.
 * \param recoveryId
 *      Which recovery this master is requesting a recovery segment for.
 *      Recovery segments may be partitioned differently for different
 *      recoveries even for the same master. This prevents accidental
 *      use of mispartitioned segments.
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
GetRecoveryDataRpc::GetRecoveryDataRpc(Context* context,
                                       ServerId backupId,
                                       uint64_t recoveryId,
                                       ServerId masterId,
                                       uint64_t segmentId,
                                       uint64_t partitionId,
                                       Buffer* response)
    : ServerIdRpcWrapper(context, backupId,
            sizeof(WireFormat::BackupGetRecoveryData::Response), response)
{
    WireFormat::BackupGetRecoveryData::Request* reqHdr(
            allocHeader<WireFormat::BackupGetRecoveryData>(backupId));
    reqHdr->recoveryId = recoveryId;
    reqHdr->masterId = masterId.getId();
    reqHdr->segmentId = segmentId;
    reqHdr->partitionId = partitionId;
    send();
}

/**
 * Wait for a getRecoveryData RPC to complete, and throw exceptions for
 * any errors.
 *
 * \return
 *      Certificate for the recovery segment which was populated
 *      into the response Buffer given at the start of this rpc call.
 *      Passed to SegmentIterator to verify the metadata integrity of the
 *      recovery segment and iterate its contents.
 * \throw ServerNotUpException
 *      The intended server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */
SegmentCertificate
GetRecoveryDataRpc::wait()
{
    waitAndCheckErrors();
    const WireFormat::BackupGetRecoveryData::Response* respHdr(
            getResponseHeader<WireFormat::BackupGetRecoveryData>());
    SegmentCertificate certificate = respHdr->certificate;

    // respHdr off limits.
    response->truncateFront(sizeof(
            WireFormat::BackupGetRecoveryData::Response));

    return certificate;
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
BackupClient::recoveryComplete(Context* context, ServerId backupId,
        ServerId masterId)
{
    RecoveryCompleteRpc rpc(context, backupId, masterId);
    rpc.wait();
}

/**
 * Constructor for RecoveryCompleteRpc: initiates an RPC in the same way as
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
RecoveryCompleteRpc::RecoveryCompleteRpc(Context* context, ServerId backupId,
        ServerId masterId)
    : ServerIdRpcWrapper(context, backupId,
            sizeof(WireFormat::BackupRecoveryComplete::Response))
{
    WireFormat::BackupRecoveryComplete::Request* reqHdr(
            allocHeader<WireFormat::BackupRecoveryComplete>(backupId));
    reqHdr->masterId = masterId.getId();
    send();
}

/**
 * This RPC is invoked at the beginning of recovering from a crashed master;
 * it asks a particular backup to begin reading from disk the segment replicas
 * from the crashed master. This RPC should be followed by a
 * #BackupClient::StartPartitioningReplicas call to finish rebuilding and
 * partitioning the replicas.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param backupId
 *      Backup that will receive this RPC.
 * \param recoveryId
 *      which recovery the coordinator is starting.
 *      recovery segments may be partitioned differently for different
 *      recoveries even for the same master. this prevents accidental
 *      use of mispartitioned segments.
 * \param masterId
 *      The id of the master whose data is to be recovered.
 *
 * \return
 *      The return value is an object that describes all of the segment
 *      replicas stored on \a backupId for \a masterId.
 */
StartReadingDataRpc::Result
BackupClient::startReadingData(Context* context,
                               ServerId backupId,
                               uint64_t recoveryId,
                               ServerId masterId)
{
    StartReadingDataRpc rpc(context, backupId, recoveryId, masterId);
    return rpc.wait();
}

/**
 * Constructor for StartReadingDataRpc: initiates an RPC in the same way as
 * #BackupClient::startReadingData, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param backupId
 *      Backup that will receive this RPC.
 * \param recoveryId
 *      which recovery the coordinator is starting.
 *      recovery segments may be partitioned differently for different
 *      recoveries even for the same master. this prevents accidental
 *      use of mispartitioned segments.
 * \param masterId
 *      The id of the master whose data is to be recovered.
 */
StartReadingDataRpc::StartReadingDataRpc(Context* context,
                                         ServerId backupId,
                                         uint64_t recoveryId,
                                         ServerId masterId)
    : ServerIdRpcWrapper(context, backupId,
            sizeof(WireFormat::BackupStartReadingData::Response))
{
    WireFormat::BackupStartReadingData::Request* reqHdr(
            allocHeader<WireFormat::BackupStartReadingData>(backupId));
    reqHdr->recoveryId = recoveryId;
    reqHdr->masterId = masterId.getId();
    send();
}

/**
 * Wait for a startReadingData RPC to complete.
 *
 * \return
 *      The return value is an object that describes all of the segment
 *      replicas stored on \a backupId for \a masterId.
 *
 * \throw ServerNotUpException
 *      The intended server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */
StartReadingDataRpc::Result
StartReadingDataRpc::wait()
{
    waitAndCheckErrors();
    Result result;

    uint32_t replicaCount = 0;
    {
        const auto* respHdr(
            getResponseHeader<WireFormat::BackupStartReadingData>());
        replicaCount = respHdr->replicaCount;
        result.primaryReplicaCount = respHdr->primaryReplicaCount;
        result.logDigestBytes = respHdr->digestBytes;
        result.logDigestSegmentId = respHdr->digestSegmentId;
        result.logDigestSegmentEpoch = respHdr->digestSegmentEpoch;
        result.tableStatsBytes = respHdr->tableStatsBytes;
        response->truncateFront(sizeof(*respHdr));
        // Remove header. Pointer now invalid.
    }

    // Build #replicas.
    uint32_t offset = 0;
    for (uint64_t i = 0; i < replicaCount; ++i) {
        result.replicas.push_back(*(response->getOffset<Replica>(offset)));
        offset += sizeof32(Replica);
    }
    response->truncateFront(offset);

    // Copy out log digest.
    if (result.logDigestBytes > 0) {
        result.logDigestBuffer.reset(new char[result.logDigestBytes]);
        response->copy(0, result.logDigestBytes, result.logDigestBuffer.get());
    }
    response->truncateFront(result.logDigestBytes);

    // Copy out tabletMetrics fields
    if (result.tableStatsBytes > 0) {
        result.tableStatsBuffer.reset(new char[result.tableStatsBytes]);
        response->copy(0, result.tableStatsBytes,
                        result.tableStatsBuffer.get());
    }

    return result;
}

/**
 * This RPC should be invoked after StartReadingDataRpc; it asks a
 * particular backup to begin rebuilding and partitioning the replicas
 * read from StartReadingDataRpc.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param backupId
 *      Backup that will receive this RPC.
 * \param recoveryId
 *      which recovery the coordinator is starting.
 *      recovery segments may be partitioned differently for different
 *      recoveries even for the same master. this prevents accidental
 *      use of mispartitioned segments.
 * \param masterId
 *      The id of the master whose data is to be recovered.
 * \param partitions
 *      Describes how the coordinator would like the backup to split up the
 *      contents of the replicas for delivery to different recovery masters.
 *      The partition ids inside each entry act as an index describing which
 *      recovery segment for a particular replica each object should be placed
 *      in.
 */
void
BackupClient::StartPartitioningReplicas(Context* context,
                               ServerId backupId,
                               uint64_t recoveryId,
                               ServerId masterId,
                               const ProtoBuf::RecoveryPartition* partitions)
{
    StartPartitioningRpc rpc(context, backupId, recoveryId,
                            masterId, partitions);
    rpc.wait();
}

/**
 * Constructor for StartPartitioningRpc: initiates an RPC in the same way as
 * #BackupClient::StartPartioningReplicas, but returns once the RPC has been
 * initiated, without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param backupId
 *      Backup that will receive this RPC.
 * \param recoveryId
 *      which recovery the coordinator is starting.
 *      recovery segments may be partitioned differently for different
 *      recoveries even for the same master. this prevents accidental
 *      use of mispartitioned segments.
 * \param masterId
 *      The id of the master whose data is to be recovered.
 * \param partitions
 *      Describes how the coordinator would like the backup to split up the
 *      contents of the replicas for delivery to different recovery masters.
 *      The partition ids inside each entry act as an index describing which
 *      recovery segment for a particular replica each object should be placed
 *      in.
 */
StartPartitioningRpc::StartPartitioningRpc(
    Context* context,
    ServerId backupId,
    uint64_t recoveryId,
    ServerId masterId,
    const ProtoBuf::RecoveryPartition* partitions)
    : ServerIdRpcWrapper(context, backupId,
            sizeof(WireFormat::BackupStartPartitioningReplicas::Response))
{
    WireFormat::BackupStartPartitioningReplicas::Request* reqHdr(
            allocHeader<WireFormat::BackupStartPartitioningReplicas>(backupId));
    reqHdr->recoveryId = recoveryId;
    reqHdr->masterId = masterId.getId();
    reqHdr->partitionsLength = ProtoBuf::serializeToRequest(&request,
            partitions);
    send();
}

StartReadingDataRpc::Result::Result()
    : replicas()
    , primaryReplicaCount(0)
    , logDigestBuffer()
    , tableStatsBuffer()
    , logDigestBytes(0)
    , logDigestSegmentId(-1)
    , logDigestSegmentEpoch(-1)
    , tableStatsBytes(-1)
{
}

StartReadingDataRpc::Result::Result(Result&& other)
    : replicas(std::move(other.replicas))
    , primaryReplicaCount(other.primaryReplicaCount)
    , logDigestBuffer(std::move(other.logDigestBuffer))
    , tableStatsBuffer(std::move(other.tableStatsBuffer))
    , logDigestBytes(other.logDigestBytes)
    , logDigestSegmentId(other.logDigestSegmentId)
    , logDigestSegmentEpoch(other.logDigestSegmentEpoch)
    , tableStatsBytes(other.tableStatsBytes)
{
}

StartReadingDataRpc::Result&
StartReadingDataRpc::Result::operator=(Result&& other)
{
    replicas = std::move(other.replicas);
    primaryReplicaCount = other.primaryReplicaCount;
    logDigestBuffer = std::move(other.logDigestBuffer);
    tableStatsBuffer = std::move(other.tableStatsBuffer);
    logDigestBytes = other.logDigestBytes;
    logDigestSegmentId = other.logDigestSegmentId;
    tableStatsBytes = other.tableStatsBytes;
    logDigestSegmentEpoch = other.logDigestSegmentEpoch;
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
 *       Log-unique, 64-bit identifier for the segment being replicated.
 * \param segmentEpoch
 *      The epoch of the segment being replicated. Used during recovery by the
 *      coordinator to filter out replicas which may have become inconsistent.
 *      When a master loses contact with a backup to which it was replicating
 *      an open segment the master increments this epoch, ensures that it has a
 *      sufficient number of replicas tagged with the new epoch number, and
 *      then tells the coordinator of the new epoch so the coordinator can
 *      filter out the stale replicas from earlier epochs.
 *      Backups blindly and durably store this as part of the replica metadata
 *      on each update.
 * \param segment
 *      Segment whose data is to be replicated.
 * \param offset
 *      The position in the segment where this data will be placed.
 * \param length
 *      The length in bytes of the data to write.
 * \param certificate
 *      Backups write this as part of their metadata for the segment.  Used
 *      during recovery to determine how much of the segment contains valid
 *      data and to verify the integrity of the segment metadata. This may
 *      be NULL which has two ramifications. First, the data included in
 *      this write will not be recovered (or, is not durable) until the
 *      after the next write which includes a certificate.  Second, the
 *      most recently transmitted certificate will be used during recovery,
 *      which means only data covered by that certificate will be recovered
 *      regardless of how much has been transmitted to the backup.
 * \param open
 *      Whether this write is an opening write to the replica. If so, causes
 *      the backup to allocate storage space if it hasn't for this replica yet.
 *      Backups may reject write requests with this flag set.
 * \param close
 *      Whether this write is a closing write to the replica. Must be to an
 *      already opened replica (or one that was opened along with this write).
 *      Writes to already closed replicas result in a client exception. If this
 *      client exception is due to a retry request then masters won't ever
 *      response with the client exception (if the first request was received
 *      by the backup the master will receive the non-exceptional response and
 *      drop the exceptional one; if the first request was not received by the
 *      backup then it will never generate an exception to begin with).
 * \param primary
 *      Whether this particular replica should be loaded and filtered at the
 *      start of master recovery (as opposed to having it loaded and filtered
 *      on demand. May be reset on each subsequent write.
 * \return
 *      A vector describing the replication group for the backup
 *      that handled the RPC (secondary replicas will then be
 *      replicated to this group of backups).  The list includes
 *      the backup that handled this RPC.
 */
void
BackupClient::writeSegment(Context* context,
                           ServerId backupId,
                           ServerId masterId,
                           uint64_t segmentId,
                           uint64_t segmentEpoch,
                           const Segment* segment,
                           uint32_t offset,
                           uint32_t length,
                           const SegmentCertificate* certificate,
                           bool open,
                           bool close,
                           bool primary)
{
    WriteSegmentRpc rpc(context, backupId, masterId, segmentId, segmentEpoch,
                        segment, offset, length, certificate,
                        open, close, primary);
    rpc.wait();
}

/**
 * Constructor for WriteSegmentRpc: initiates an RPC in the same way as
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
 *      Log-unique, 64-bit identifier for the segment being replicated.
 * \param segmentEpoch
 *      The epoch of the segment being replicated. Used during recovery by the
 *      coordinator to filter out replicas which may have become inconsistent.
 *      When a master loses contact with a backup to which it was replicating
 *      an open segment the master increments this epoch, ensures that it has a
 *      sufficient number of replicas tagged with the new epoch number, and
 *      then tells the coordinator of the new epoch so the coordinator can
 *      filter out the stale replicas from earlier epochs.
 *      Backups blindly and durably store this as part of the replica metadata
 *      on each update.
 * \param segment
 *      Segment whose data is to be replicated.
 * \param offset
 *      Both the position in the replica where this data will be placed
 *      and the starting offset in the segment where the data to be
 *      replicated starts.
 * \param length
 *      The length in bytes of the data to write.
 * \param certificate
 *      Backups write this as part of their metadata for the segment.  Used
 *      during recovery to determine how much of the segment contains valid
 *      data and to verify the integrity of the segment metadata. This may
 *      be NULL which has two ramifications. First, the data included in
 *      this write will not be recovered (or, is not durable) until the
 *      after the next write which includes a certificate.  Second, the
 *      most recently transmitted certificate will be used during recovery,
 *      which means only data covered by that certificate will be recovered
 *      regardless of how much has been transmitted to the backup.
 * \param open
 *      Whether this write is an opening write to the replica. If so, causes
 *      the backup to allocate storage space if it hasn't for this replica yet.
 *      Backups may reject write requests with this flag set.
 * \param close
 *      Whether this write is a closing write to the replica. Must be to an
 *      already opened replica (or one that was opened along with this write).
 *      Writes to already closed replicas result in a client exception. If this
 *      client exception is due to a retry request then masters won't ever
 *      response with the client exception (if the first request was received
 *      by the backup the master will receive the non-exceptional response and
 *      drop the exceptional one; if the first request was not received by the
 *      backup then it will never generate an exception to begin with).
 * \param primary
 *      Whether this particular replica should be loaded and filtered at the
 *      start of master recovery (as opposed to having it loaded and filtered
 *      on demand. May be reset on each subsequent write.
 */
WriteSegmentRpc::WriteSegmentRpc(Context* context,
                                 ServerId backupId,
                                 ServerId masterId,
                                 uint64_t segmentId,
                                 uint64_t segmentEpoch,
                                 const Segment* segment,
                                 uint32_t offset,
                                 uint32_t length,
                                 const SegmentCertificate* certificate,
                                 bool open,
                                 bool close,
                                 bool primary)
    : ServerIdRpcWrapper(context, backupId,
                         sizeof(WireFormat::BackupWrite::Response))
{
    WireFormat::BackupWrite::Request* reqHdr(
            allocHeader<WireFormat::BackupWrite>(backupId));
    reqHdr->masterId = masterId.getId();
    reqHdr->segmentId = segmentId;
    reqHdr->segmentEpoch = segmentEpoch;
    reqHdr->offset = offset;
    reqHdr->length = length;
    reqHdr->certificateIncluded = (certificate != NULL);
    if (reqHdr->certificateIncluded)
        reqHdr->certificate = *certificate;
    else
        reqHdr->certificate = SegmentCertificate();
    reqHdr->open = open;
    reqHdr->close = close;
    reqHdr->primary = primary;
    if (segment)
        segment->appendToBuffer(request, offset, length);
    CycleCounter<RawMetric> _(&metrics->master.replicationPostingWriteRpcTicks);
    send();
}

/**
 * Wait for a writeSegment RPC to complete.
 *
 * \throw ServerNotUpException
 *      The intended server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */
void
WriteSegmentRpc::wait()
{
    waitAndCheckErrors();
}

} // namespace RAMCloud
