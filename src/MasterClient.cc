/* Copyright (c) 2010 Stanford University
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

#include "MasterClient.h"
#include "TransportManager.h"
#include "ProtoBuf.h"
#include "Log.h"
#include "Segment.h"
#include "Object.h"
#include "Status.h"
#include "WireFormat.h"

namespace RAMCloud {

// Default RejectRules to use if none are provided by the caller.
RejectRules defaultRejectRules;

/**
 * Instruct the master that it must no longer serve requests for the tablet
 * specified. The server may reclaim all memory previously allocated to that
 * tablet.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for the target server.
 * \param tableId
 *      Identifier for the table containing the tablet.
 * \param firstKeyHash
 *      Smallest value in the 64-bit key hash space for this table that belongs
 *      to the tablet.
 * \param lastKeyHash
 *      Largest value in the 64-bit key hash space for this table that belongs
 *      to the tablet.
 */
void
MasterClient::dropTabletOwnership(Context& context, ServerId serverId,
        uint64_t tableId, uint64_t firstKeyHash, uint64_t lastKeyHash)
{
    DropTabletOwnershipRpc rpc(context, serverId, tableId, firstKeyHash,
            lastKeyHash);
    rpc.wait();
}

/**
 * Constructor for DropTabletOwnershipRpc: initiates an RPC in the same way as
 * #MasterClient::dropTabletOwnership, but returns once the RPC has been
 * initiated, without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for the target server.
 * \param tableId
 *      Identifier for the table containing the tablet.
 * \param firstKeyHash
 *      Smallest value in the 64-bit key hash space for this table that belongs
 *      to the tablet.
 * \param lastKeyHash
 *      Largest value in the 64-bit key hash space for this table that belongs
 *      to the tablet.
 */
DropTabletOwnershipRpc::DropTabletOwnershipRpc(Context& context,
        ServerId serverId, uint64_t tableId, uint64_t firstKeyHash,
        uint64_t lastKeyHash)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::DropTabletOwnership::Response))
{
    WireFormat::DropTabletOwnership::Request& reqHdr(
            allocHeader<WireFormat::DropTabletOwnership>());
    reqHdr.tableId = tableId;
    reqHdr.firstKeyHash = firstKeyHash;
    reqHdr.lastKeyHash = lastKeyHash;
    send();
}

/**
 * Obtain a master's log head position.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for the target server.
 *
 * \return
 *      The return value is the first (lowest) position in \a serverId's log
 *      that does not yet contain data (i.e., any future data accepted by
 *      \a serverId will have a log position at least this high).
 *
 * \throw ServerDoesntExistException
 *      The intended server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */
LogPosition
MasterClient::getHeadOfLog(Context& context, ServerId serverId)
{
    GetHeadOfLogRpc rpc(context, serverId);
    return rpc.wait();
}

/**
 * Constructor for GetHeadOfLogRpc: initiates an RPC in the same way as
 * #MasterClient::getHeadOfLog, but returns once the RPC has been
 * initiated, without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for the target server.
 */
GetHeadOfLogRpc::GetHeadOfLogRpc(Context& context, ServerId serverId)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::GetHeadOfLog::Response))
{
    allocHeader<WireFormat::GetHeadOfLog>();
    send();
}

/**
 * Wait for a getHeadOfLog RPC to complete.
 *
 * \return
 *      The return value is the first (lowest) position in the respondent's
 *      log that does not yet contain data (i.e., any future data accepted by
 *      the respondent will have a log position at least this high).
 *
 * \throw ServerDoesntExistException
 *      The intended server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */
LogPosition
GetHeadOfLogRpc::wait()
{
    waitAndCheckErrors();
    const WireFormat::GetHeadOfLog::Response& respHdr(
            getResponseHeader<WireFormat::GetHeadOfLog>());
    return { respHdr.headSegmentId, respHdr.headSegmentOffset };
}

/**
 * Return whether a replica for a segment created by a given master may still
 * be needed for recovery. Backups use this when restarting after a failure
 * to determine if replicas found in persistent storage must be retained.
 *
 * The cluster membership protocol must guarantee that if the master "knows
 * about" the calling backup server then it must already know about the crash
 * of the backup which created the on-storage replicas the calling backup
 * has rediscovered.  This guarantees that when the master responds to this
 * call that it must have already recovered from crash mentioned above if
 * it returns false.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for the target server.
 * \param backupServerId
 *      The server id which is requesting information about a replica.
 *      This is used to ensure the master is aware of the backup via
 *      the cluster membership protocol, which ensures that it is
 *      aware of any crash of the backup that created the replica
 *      being inquired about.
 * \param segmentId
 *      The segmentId of the replica which a backup server is considering
 *      freeing.
 *
 * \return
 *      True means that the calling backup must continue to retain the given
 *      replica (it could be needed for crash recovery in the future). False means
 *      the replica is no longer needed, so the backup can reclaim its space.
 */
bool
MasterClient::isReplicaNeeded(Context& context, ServerId serverId,
        ServerId backupServerId, uint64_t segmentId)
{
    IsReplicaNeededRpc rpc(context, serverId, backupServerId, segmentId);
    return rpc.wait();
}

/**
 * Constructor for IsReplicaNeededRpc: initiates an RPC in the same way as
 * #MasterClient::isReplicaNeeded, but returns once the RPC has been
 * initiated, without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for the target server.
 * \param backupServerId
 *      The server id which is requesting information about a replica.
 *      This is used to ensure the master is aware of the backup via
 *      the cluster membership protocol, which ensures that it is
 *      aware of any crash of the backup that created the replica
 *      being inquired about.
 * \param segmentId
 *      The segmentId of the replica which a backup server is considering
 *      freeing.
 */
IsReplicaNeededRpc::IsReplicaNeededRpc(Context& context, ServerId serverId,
        ServerId backupServerId, uint64_t segmentId)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::IsReplicaNeeded::Response))
{
    WireFormat::IsReplicaNeeded::Request& reqHdr(
            allocHeader<WireFormat::IsReplicaNeeded>());
    reqHdr.backupServerId = backupServerId.getId();
    reqHdr.segmentId = segmentId;
    send();
}

/**
 * Wait for an isReplicaNeeded RPC to complete.
 *
 * \return
 *      True means that the calling backup must continue to retain the given
 *      replica (it could be needed for crash recovery in the future). False means
 *      the replica is no longer needed, so the backup can reclaim its space.
 *
 * \throw ServerDoesntExistException
 *      The target server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */
bool
IsReplicaNeededRpc::wait()
{
    waitAndCheckErrors();
    const WireFormat::IsReplicaNeeded::Response& respHdr(
            getResponseHeader<WireFormat::IsReplicaNeeded>());
    return respHdr.needed;
}

/**
 * Request that a master decide whether it will accept a migrated tablet
 * and set up any necessary state to begin receiving tablet data from the
 * original master.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for a master that will (hopefully) accept a
 *      migrated tablet.
 * \param tableId
 *      Identifier for the table.
 * \param firstKeyHash
 *      Lowest key hash in the tablet range to be migrated.
 * \param lastKeyHash
 *      Highest key hash in the tablet range to be migrated.
 * \param expectedObjects
 *      Estimate of the total number of objects that will be migrated.
 * \param expectedBytes
 *      Estimate of the total number of bytes that will be migrated.
 */
void
MasterClient::prepForMigration(Context& context, ServerId serverId,
        uint64_t tableId, uint64_t firstKeyHash, uint64_t lastKeyHash,
        uint64_t expectedObjects, uint64_t expectedBytes)
{
    PrepForMigrationRpc rpc(context, serverId, tableId, firstKeyHash,
            lastKeyHash, expectedObjects, expectedBytes);
    rpc.wait();
}

/**
 * Constructor for PrepForMigrationRpc: initiates an RPC in the same way as
 * #MasterClient::prepForMigration, but returns once the RPC has been
 * initiated, without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for a master that will (hopefully) accept a
 *      migrated tablet.
 * \param tableId
 *      Identifier for the table.
 * \param firstKeyHash
 *      Lowest key hash in the tablet range to be migrated.
 * \param lastKeyHash
 *      Highest key hash in the tablet range to be migrated.
 * \param expectedObjects
 *      Estimate of the total number of objects that will be migrated.
 * \param expectedBytes
 *      Estimate of the total number of bytes that will be migrated.
 */
PrepForMigrationRpc::PrepForMigrationRpc(Context& context, ServerId serverId,
        uint64_t tableId, uint64_t firstKeyHash, uint64_t lastKeyHash,
        uint64_t expectedObjects, uint64_t expectedBytes)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::PrepForMigration::Response))
{
    WireFormat::PrepForMigration::Request& reqHdr(
            allocHeader<WireFormat::PrepForMigration>());
    reqHdr.tableId = tableId;
    reqHdr.firstKeyHash = firstKeyHash;
    reqHdr.lastKeyHash = lastKeyHash;
    reqHdr.expectedObjects = expectedObjects;
    reqHdr.expectedBytes = expectedBytes;
    send();
}

/**
 * Request that a master add some migrated data to its storage.
 * The receiving master will not service requests on the data,
 * but will add it to its log and hash table.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for a master that has previously agreed to accept
 *      migrated data for this tablet.
 * \param tableId
 *      Identifier for the table.
 * \param firstKeyHash
 *      Lowest key hash in the tablet range to be migrated.
 * \param segment
 *      A block of data, formatted like a segment, containing objects
 *      and tombstones to be migrated.
 * \param segmentBytes
 *      Number of bytes in \a segment.
 */
void
MasterClient::receiveMigrationData(Context& context, ServerId serverId,
        uint64_t tableId, uint64_t firstKeyHash, const void* segment,
        uint32_t segmentBytes)
{
    ReceiveMigrationDataRpc rpc(context, serverId, tableId, firstKeyHash,
            segment, segmentBytes);
    rpc.wait();
}

/**
 * Constructor for ReceiveMigrationDataRpc: initiates an RPC in the same way as
 * #MasterClient::receiveMigrationData, but returns once the RPC has been
 * initiated, without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for a master that has previously agreed to accept
 *      migrated data for this tablet.
 * \param tableId
 *      Identifier for the table.
 * \param firstKeyHash
 *      Lowest key hash in the tablet range to be migrated.
 * \param segment
 *      A block of data, formatted like a segment, containing objects
 *      and tombstones to be migrated.
 * \param segmentBytes
 *      Number of bytes in \a segment.
 */
ReceiveMigrationDataRpc::ReceiveMigrationDataRpc(Context& context,
        ServerId serverId, uint64_t tableId, uint64_t firstKeyHash,
        const void* segment, uint32_t segmentBytes)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::ReceiveMigrationData::Response))
{
    WireFormat::ReceiveMigrationData::Request& reqHdr(
            allocHeader<WireFormat::ReceiveMigrationData>());
    reqHdr.tableId = tableId;
    reqHdr.firstKeyHash = firstKeyHash;
    reqHdr.segmentBytes = segmentBytes;
    Buffer::Chunk::appendToBuffer(&request, segment, segmentBytes);
    send();
}

/**
 * This RPC is sent to a recovery master to request that it begin recovering
 * a collection of tablets previously stored on a master that has crashed.
 * The RPC completes once the recipient has begun recovery; it does not wait
 * for recovery to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for the recovery master.
 * \param recoveryId
 *      Identifies the recovery that \a serverId is part of.  Must be returned to
 *      the coordinator in a future recoveryMasterFinished() call.
 * \param crashedServerId
 *      The ServerId of the crashed master whose data is to be recovered.
 * \param partitionId
 *      The partition id of #tablets inside the crashed master's will.
 * \param tablets
 *      A set of tables with key ranges describing which poritions of which
 *      tables the recovery Master should take over for.
 * \param replicas
 *      An array describing where to find replicas of each segment.
 * \param numReplicas
 *      The number of replicas in the 'replicas' list.
 */
void
MasterClient::recover(Context& context, ServerId serverId, uint64_t recoveryId,
        ServerId crashedServerId, uint64_t partitionId,
        const ProtoBuf::Tablets& tablets,
        const WireFormat::Recover::Replica* replicas, uint32_t numReplicas)
{
    RecoverRpc rpc(context, serverId, recoveryId, crashedServerId,
            partitionId, tablets, replicas, numReplicas);
    rpc.wait();
}

/**
 * Constructor for RecoverRpc: initiates an RPC in the same way as
 * #MasterClient::recover, but returns once the RPC has been
 * initiated, without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for the recovery master.
 * \param recoveryId
 *      Identifies the recovery that \a serverId is part of.  Must be returned to
 *      the coordinator in a future recoveryMasterFinished() call.
 * \param crashedServerId
 *      The ServerId of the crashed master whose data is to be recovered.
 * \param partitionId
 *      The partition id of #tablets inside the crashed master's will.
 * \param tablets
 *      A set of tables with key ranges describing which poritions of which
 *      tables the recovery Master should take over for.
 * \param replicas
 *      An array describing where to find replicas of each segment.
 * \param numReplicas
 *      The number of replicas in the 'replicas' list.
 */
RecoverRpc::RecoverRpc(Context& context, ServerId serverId,
        uint64_t recoveryId, ServerId crashedServerId, uint64_t partitionId,
        const ProtoBuf::Tablets& tablets,
        const WireFormat::Recover::Replica* replicas,
        uint32_t numReplicas)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::Recover::Response))
{
    WireFormat::Recover::Request& reqHdr(
            allocHeader<WireFormat::Recover>());
    reqHdr.recoveryId = recoveryId;
    reqHdr.crashedServerId = crashedServerId.getId();
    reqHdr.partitionId = partitionId;
    reqHdr.tabletsLength = serializeToRequest(request, tablets);
    reqHdr.numReplicas = numReplicas;
    Buffer::Chunk::appendToBuffer(&request, replicas,
            downCast<uint32_t>(sizeof(replicas[0])) * numReplicas);
    send();
}

/**
 * This method is invoked by the coordinator to split a tablet inside a
 * master into two separate tablets.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for the master containing the tablet.
 * \param tableId
 *      Id of the table that contains the tablet to be split.
 * \param firstKeyHash
 *      Lowest key hash in the range of the tablet to be split.
 * \param lastKeyHash
 *      Highest key hash in the range of the tablet to be split.
 * \param splitKeyHash
 *      The key hash where the split occurs. This will become the
 *      lowest key hash of the second tablet after the split.
 */
void
MasterClient::splitMasterTablet(Context& context, ServerId serverId,
        uint64_t tableId, uint64_t firstKeyHash,
        uint64_t lastKeyHash, uint64_t splitKeyHash)
{
    SplitMasterTabletRpc rpc(context, serverId, tableId, firstKeyHash,
        lastKeyHash, splitKeyHash);
    rpc.wait();
}

/**
 * Constructor for SplitMasterTabletRpc: initiates an RPC in the same way as
 * #MasterClient::splitMasterTablet, but returns once the RPC has been
 * initiated, without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for the master containing the tablet.
 * \param tableId
 *      Id of the table that contains the tablet to be split.
 * \param firstKeyHash
 *      Lowest key hash in the range of the tablet to be split.
 * \param lastKeyHash
 *      Highest key hash in the range of the tablet to be split.
 * \param splitKeyHash
 *      The key hash where the split occurs. This will become the
 *      lowest key hash of the second tablet after the split.
 */
SplitMasterTabletRpc::SplitMasterTabletRpc(Context& context,
        ServerId serverId, uint64_t tableId, uint64_t firstKeyHash,
        uint64_t lastKeyHash, uint64_t splitKeyHash)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::SplitMasterTablet::Response))
{
    WireFormat::SplitMasterTablet::Request& reqHdr(
            allocHeader<WireFormat::SplitMasterTablet>());
    reqHdr.tableId = tableId;
    reqHdr.firstKeyHash = firstKeyHash;
    reqHdr.lastKeyHash = lastKeyHash;
    reqHdr.splitKeyHash = splitKeyHash;
    send();
}

/**
 * Instruct a master that it should begin serving requests for a particular
 * tablet. If the master does not already store this tablet, then it will
 * create a new tablet. If the master already has information for the tablet,
 * but the tablet was frozen (e.g. because data migration was underway),
 * then the tablet will be unfrozen.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for the target server.
 * \param tableId
 *      Identifier for the table containing the tablet.
 * \param firstKeyHash
 *      Smallest value in the 64-bit key hash space for this table that belongs
 *      to the tablet.
 * \param lastKeyHash
 *      Largest value in the 64-bit key hash space for this table that belongs
 *      to the tablet.
 */
void
MasterClient::takeTabletOwnership(Context& context, ServerId serverId,
        uint64_t tableId, uint64_t firstKeyHash, uint64_t lastKeyHash)
{
    TakeTabletOwnershipRpc rpc(context, serverId, tableId, firstKeyHash,
            lastKeyHash);
    rpc.wait();
}

/**
 * Constructor for TakeTabletOwnershipRpc: initiates an RPC in the same way as
 * #MasterClient::takeTabletOwnership, but returns once the RPC has been
 * initiated, without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for the target server.
 * \param tableId
 *      Identifier for the table containing the tablet.
 * \param firstKeyHash
 *      Smallest value in the 64-bit key hash space for this table that belongs
 *      to the tablet.
 * \param lastKeyHash
 *      Largest value in the 64-bit key hash space for this table that belongs
 *      to the tablet.
 */
TakeTabletOwnershipRpc::TakeTabletOwnershipRpc(
        Context& context, ServerId serverId, uint64_t tableId,
        uint64_t firstKeyHash, uint64_t lastKeyHash)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::TakeTabletOwnership::Response))
{
    WireFormat::TakeTabletOwnership::Request& reqHdr(
            allocHeader<WireFormat::TakeTabletOwnership>());
    reqHdr.tableId = tableId;
    reqHdr.firstKeyHash = firstKeyHash;
    reqHdr.lastKeyHash = lastKeyHash;
    send();
}

}  // namespace RAMCloud
