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
    DropTabletOwnershipRpc2 rpc(context, serverId, tableId, firstKeyHash,
            lastKeyHash);
    rpc.wait();
}

/**
 * Constructor for DropTabletOwnershipRpc2: initiates an RPC in the same way as
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
DropTabletOwnershipRpc2::DropTabletOwnershipRpc2(Context& context,
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
    GetHeadOfLogRpc2 rpc(context, serverId);
    return rpc.wait();
}

/**
 * Constructor for GetHeadOfLogRpc2: initiates an RPC in the same way as
 * #MasterClient::getHeadOfLog, but returns once the RPC has been
 * initiated, without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for the target server.
 */
GetHeadOfLogRpc2::GetHeadOfLogRpc2(Context& context, ServerId serverId)
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
GetHeadOfLogRpc2::wait()
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
    IsReplicaNeededRpc2 rpc(context, serverId, backupServerId, segmentId);
    return rpc.wait();
}

/**
 * Constructor for IsReplicaNeededRpc2: initiates an RPC in the same way as
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
IsReplicaNeededRpc2::IsReplicaNeededRpc2(Context& context, ServerId serverId,
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
IsReplicaNeededRpc2::wait()
{
    waitAndCheckErrors();
    const WireFormat::IsReplicaNeeded::Response& respHdr(
            getResponseHeader<WireFormat::IsReplicaNeeded>());
    return respHdr.needed;
}

/**
 * Request that the master owning a particular tablet migrate it
 * to another master.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for the master to receive this RPC; it must
 *      currently own the tablet described by the arguments below.
 * \param tableId
 *      Identifier for the table containing the tablet to be migrated.
 * \param firstKeyHash
 *      Lowest key hash in the tablet range to be migrated.
 * \param lastKeyHash
 *      Highest key hash in the tablet range to be migrated.
 * \param newOwnerId
 *      Identifies the server to which the tablet should be migrated.
 */
void
MasterClient::migrateTablet(Context& context, ServerId serverId,
        uint64_t tableId, uint64_t firstKeyHash, uint64_t lastKeyHash,
        ServerId newOwnerId)
{
    MigrateTabletRpc2 rpc(context, serverId, tableId, firstKeyHash,
            lastKeyHash, newOwnerId);
    rpc.wait();
}

/**
 * Constructor for MigrateTabletRpc2: initiates an RPC in the same way as
 * #MasterClient::migrateTablet, but returns once the RPC has been
 * initiated, without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for the master to receive this RPC; it must
 *      currently own the tablet described by the arguments below.
 * \param tableId
 *      Identifier for the table containing the tablet to be migrated.
 * \param firstKeyHash
 *      Lowest key hash in the tablet range to be migrated.
 * \param lastKeyHash
 *      Highest key hash in the tablet range to be migrated.
 * \param newOwnerId
 *      Identifies the server to which the tablet should be migrated.
 */
MigrateTabletRpc2::MigrateTabletRpc2(Context& context, ServerId serverId,
        uint64_t tableId, uint64_t firstKeyHash, uint64_t lastKeyHash,
        ServerId newOwnerId)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::MigrateTablet::Response))
{
    WireFormat::MigrateTablet::Request& reqHdr(
            allocHeader<WireFormat::MigrateTablet>());
    reqHdr.tableId = tableId;
    reqHdr.firstKeyHash = firstKeyHash;
    reqHdr.lastKeyHash = lastKeyHash;
    reqHdr.newOwnerMasterId = newOwnerId.getId();
    send();
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
    PrepForMigrationRpc2 rpc(context, serverId, tableId, firstKeyHash,
            lastKeyHash, expectedObjects, expectedBytes);
    rpc.wait();
}

/**
 * Constructor for PrepForMigrationRpc2: initiates an RPC in the same way as
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
PrepForMigrationRpc2::PrepForMigrationRpc2(Context& context, ServerId serverId,
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
    ReceiveMigrationDataRpc2 rpc(context, serverId, tableId, firstKeyHash,
            segment, segmentBytes);
    rpc.wait();
}

/**
 * Constructor for ReceiveMigrationDataRpc2: initiates an RPC in the same way as
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
ReceiveMigrationDataRpc2::ReceiveMigrationDataRpc2(Context& context,
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
        const ProtoBuf::Tablets& tablets, const RecoverRpc::Replica* replicas,
        uint32_t numReplicas)
{
    RecoverRpc2 rpc(context, serverId, recoveryId, crashedServerId,
            partitionId, tablets, replicas, numReplicas);
    rpc.wait();
}

/**
 * Constructor for RecoverRpc2: initiates an RPC in the same way as
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
RecoverRpc2::RecoverRpc2(Context& context, ServerId serverId,
        uint64_t recoveryId, ServerId crashedServerId, uint64_t partitionId,
        const ProtoBuf::Tablets& tablets, const RecoverRpc::Replica* replicas,
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
    SplitMasterTabletRpc2 rpc(context, serverId, tableId, firstKeyHash,
        lastKeyHash, splitKeyHash);
    rpc.wait();
}

/**
 * Constructor for SplitMasterTabletRpc2: initiates an RPC in the same way as
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
SplitMasterTabletRpc2::SplitMasterTabletRpc2(Context& context,
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
    TakeTabletOwnershipRpc2 rpc(context, serverId, tableId, firstKeyHash,
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
TakeTabletOwnershipRpc2::TakeTabletOwnershipRpc2(
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

//-------------------------------------------------------
// OLD: everything below here should eventually go away.
//-------------------------------------------------------

/**
 * Returns the ServerStatistics protobuf to a client. This protobuf
 * contains all statistical information about a master, see
 * ServerStatistics.proto for all contained fields.
 *
 * \param[out] serverStats
 *      A ServerStatsistics protobuf containing the current statistical
 *      information about the master.
 *
 */
void
MasterClient::getServerStatistics(ProtoBuf::ServerStatistics& serverStats)
{
    Buffer req;
    Buffer resp;
    allocHeader<GetServerStatisticsRpc>(req);
    const GetServerStatisticsRpc::Response& respHdr(
        sendRecv<GetServerStatisticsRpc>(session, req, resp));
    checkStatus(HERE);
    ProtoBuf::parseFromResponse(resp, sizeof(respHdr),
        respHdr.serverStatsLength, serverStats);
}

/// Start an enumeration RPC for an object. See MasterClient::enumeration.
MasterClient::Enumeration::Enumeration(MasterClient& client, uint64_t tableId,
                                       uint64_t tabletStartHash,
                                       uint64_t* nextTabletStartHashOut,
                                       Buffer* iter, Buffer* nextIterOut,
                                       Buffer* objectsOut)
    : client(client)
    , requestBuffer()
    , responseBuffer()
    , nextTabletStartHash(nextTabletStartHashOut)
    , nextIter(*nextIterOut)
    , objects(*objectsOut)
    , state()
{
    nextIter.reset();
    objects.reset();
    EnumerationRpc::Request& reqHdr(
        client.allocHeader<EnumerationRpc>(requestBuffer));
    reqHdr.tableId = tableId;
    reqHdr.tabletStartHash = tabletStartHash;
    reqHdr.iteratorBytes = iter->getTotalLength();
    for (Buffer::Iterator it(*iter); !it.isDone(); it.next())
        Buffer::Chunk::appendToBuffer(&requestBuffer,
                                      it.getData(), it.getLength());

    state = client.send<EnumerationRpc>(client.session,
                                        requestBuffer,
                                        responseBuffer);
}

/// Wait for the enumeration RPC to complete.
void
MasterClient::Enumeration::operator()()
{
    const EnumerationRpc::Response& respHdr(client.recv<EnumerationRpc>(state));

    uint32_t payloadBytes = respHdr.payloadBytes;
    uint32_t iteratorBytes = respHdr.iteratorBytes;
    uint32_t respOffset = downCast<uint32_t>(sizeof(respHdr));
    assert(responseBuffer.getTotalLength() ==
           respOffset + payloadBytes + iteratorBytes);

    // Copy tablet start hash into out pointer.
    *nextTabletStartHash = respHdr.tabletStartHash;

    // Copy objects from response into objects buffer.
    responseBuffer.copy(respOffset, payloadBytes,
                        new(&objects, APPEND) char[payloadBytes]);
    respOffset += payloadBytes;

    // Copy iterator from response into nextIter buffer.
    responseBuffer.copy(respOffset, iteratorBytes,
                        new(&nextIter, APPEND) char[iteratorBytes]);
    respOffset += iteratorBytes;

    client.checkStatus(HERE);
}

/**
 * Enumerate the contents of a tablet.
 *
 * \param tableId
 *      The table containing the desired tablet (return value from
 *      a previous call to getTableId).
 * \param tabletStartHash
 *      The tablet to iterate. The caller should provide zero to the
 *      initial call. On subsequent calls, the caller should pass the
 *      value returned through nextTabletStartHash from the previous
 *      call.
 * \param[out] nextTabletStartHash
 *      The next tablet to iterate. The caller should pass the value
 *      returned as the tabletStartHash parameter to the next call to
 *      enumeration(). When iteration over the entire table is
 *      complete, zero will be returned through this parameter (and no
 *      objects will be returned).
 * \param iter
 *      The opaque iterator to pass to the server. The caller should
 *      provide an empty Buffer to the initial call. On subsequent
 *      calls, the caller should pass the contents returned through
 *      nextIter from the previous call.
 * \param[out] nextIter
 *      The next iterator state. The caller should pass the value
 *      returned as the iter parameter to the next call to
 *      enumeration().
 * \param[out] objects
 *      After a successful return, this buffer will contain zero or
 *      more objects from the requested tablet. If zero objects are
 *      returned, then there are no more objects remaining in the
 *      tablet. When this happens, nextTabletStartHash will be set to
 *      point to the next tablet, or will be set to zero if this is
 *      the end of the entire table.
 *
 * \exception InternalError
 */
void
MasterClient::enumeration(uint64_t tableId,
                          uint64_t tabletStartHash,
                          uint64_t* nextTabletStartHash,
                          Buffer* iter, Buffer* nextIter,
                          Buffer* objects)
{
    Enumeration(*this, tableId,
                tabletStartHash, nextTabletStartHash,
                iter, nextIter, objects)();
}

/**
 * Request that the master owning a particular tablet migrate it
 * to another designated master.
 *
 * \param tableId
 *      Identifier for the table.
 *
 * \param firstKey
 *      First key of the tablet range to be migrated.
 *
 * \param lastKey
 *      Last key of the tablet range to be migrated.
 *
 * \param newOwnerMasterId
 *      ServerId of the node to which the tablet should be migrated.
 */
void
MasterClient::migrateTablet(uint64_t tableId,
                            uint64_t firstKey,
                            uint64_t lastKey,
                            ServerId newOwnerMasterId)
{
    Buffer req, resp;

    MigrateTabletRpc::Request& reqHdr(allocHeader<MigrateTabletRpc>(req));
    reqHdr.tableId = tableId;
    reqHdr.firstKey = firstKey;
    reqHdr.lastKey = lastKey;
    reqHdr.newOwnerMasterId = *newOwnerMasterId;
    sendRecv<MigrateTabletRpc>(session, req, resp);
    checkStatus(HERE);
}

}  // namespace RAMCloud
