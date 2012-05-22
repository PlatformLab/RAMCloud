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

namespace RAMCloud {

// Default RejectRules to use if none are provided by the caller.
RejectRules defaultRejectRules;

/**
 * Fill a master server with the given number of objects, each of the
 * same given size. Objects are added to all tables in the master in
 * a round-robin fashion. This method exists simply to quickly fill a
 * master for experiments.
 *
 * See MasterClient::fillWithTestData() for more information.
 *
 * \bug Will return an error if the master only owns part of a table
 * (because the hash of the fabricated keys may land in a region it
 * doesn't own).
 *
 * \param numObjects
 *      Total number of objects to add to the server.
 * \param objectSize
 *      Bytes of garbage data to place in each object not including the
 *      key (the keys are ASCII strings starting with "0" and increasing
 *      numerically in each table).
 */
void
MasterClient::fillWithTestData(uint32_t numObjects, uint32_t objectSize)
{
    Buffer req, resp;
    FillWithTestDataRpc::Request& reqHdr(allocHeader<FillWithTestDataRpc>(req));
    reqHdr.numObjects = numObjects;
    reqHdr.objectSize = objectSize;
    sendRecv<FillWithTestDataRpc>(session, req, resp);
    checkStatus(HERE);
}

/**
 * Obtain a master's log head position. This is currently used to determine
 * the minimum position at which data belonging to newly assigned tablet
 * may exist at. Any prior data can be ignored.
 */
LogPosition
MasterClient::getHeadOfLog()
{
    Buffer req, resp;
    allocHeader<GetHeadOfLogRpc>(req);
    const GetHeadOfLogRpc::Response& respHdr(
        sendRecv<GetHeadOfLogRpc>(session, req, resp));
    checkStatus(HERE);
    return { respHdr.headSegmentId, respHdr.headSegmentOffset };
}

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

/**
 * Recover a set of tablets on behalf of a crashed master.
 *
 * \param client
 *      The MasterClient instance over which the RPC should be issued.
 * \param recoveryId
 *      Identifies the recovery this master is a part of.
 *      Should be returned to this coordinator in future recoveryMasterFinished()
 *      calls.
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
MasterClient::Recover::Recover(MasterClient& client,
                               uint64_t recoveryId,
                               ServerId crashedServerId,
                               uint64_t partitionId,
                               const ProtoBuf::Tablets& tablets,
                               const RecoverRpc::Replica* replicas,
                               uint32_t numReplicas)
    : client(client)
    , requestBuffer()
    , responseBuffer()
    , state()
{
    RecoverRpc::Request& reqHdr(client.allocHeader<RecoverRpc>(requestBuffer));
    reqHdr.recoveryId = recoveryId;
    reqHdr.crashedServerId = crashedServerId.getId();
    reqHdr.partitionId = partitionId;
    reqHdr.tabletsLength = serializeToResponse(requestBuffer, tablets);
    reqHdr.numReplicas = numReplicas;
    Buffer::Chunk::appendToBuffer(&requestBuffer, replicas,
                  downCast<uint32_t>(sizeof(replicas[0])) * numReplicas);
    state = client.send<RecoverRpc>(client.session,
                                    requestBuffer,
                                    responseBuffer);
}

void
MasterClient::Recover::operator()()
{
    client.recv<RecoverRpc>(state);
    client.checkStatus(HERE);
}

void
MasterClient::recover(uint64_t recoveryId,
                      ServerId crashedServerId,
                      uint64_t partitionId,
                      const ProtoBuf::Tablets& tablets,
                      const RecoverRpc::Replica* replicas,
                      uint32_t numReplicas)
{
    Recover(*this, recoveryId, crashedServerId,
            partitionId, tablets, replicas, numReplicas)();
}

/// Start a read RPC for an object. See MasterClient::read.
MasterClient::Read::Read(MasterClient& client,
                         uint64_t tableId, const char* key,
                         uint16_t keyLength, Buffer* value,
                         const RejectRules* rejectRules,
                         uint64_t* version)
    : client(client)
    , version(version)
    , requestBuffer()
    , responseBuffer(*value)
    , state()
{
    responseBuffer.reset();
    ReadRpc::Request& reqHdr(client.allocHeader<ReadRpc>(requestBuffer));
    reqHdr.tableId = tableId;
    reqHdr.keyLength = keyLength;
    reqHdr.rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    Buffer::Chunk::appendToBuffer(&requestBuffer, key, keyLength);

    state = client.send<ReadRpc>(client.session,
                                 requestBuffer,
                                 responseBuffer);
}

/// Wait for the read RPC to complete.
void
MasterClient::Read::operator()()
{
    const ReadRpc::Response& respHdr(client.recv<ReadRpc>(state));
    if (version != NULL)
        *version = respHdr.version;

    // Truncate the response Buffer so that it consists of nothing
    // but the object data.
    responseBuffer.truncateFront(sizeof(respHdr));
    assert(respHdr.length == responseBuffer.getTotalLength());
    client.checkStatus(HERE);
}

/**
 * Read the current contents of an object.
 *
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated like a string.
 *      The caller is responsible for ensuring that this key remains valid
 *      until the call is reaped/canceled.
 * \param keyLength
 *      Size in bytes of the key.
 * \param[out] value
 *      After a successful return, this Buffer will hold the
 *      contents of the desired object.
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the read
 *      should be aborted with an error.
 * \param[out] version
 *      If non-NULL, the version number of the object is returned
 *      here.
 *
 * \exception RejectRulesException
 * \exception InternalError
 */
void
MasterClient::read(uint64_t tableId, const char* key, uint16_t keyLength,
                   Buffer* value, const RejectRules* rejectRules,
                   uint64_t* version)
{
    Read(*this, tableId, key, keyLength, value, rejectRules, version)();
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
 * Request that a master decide whether it will accept a migrated tablet
 * and set up any necessary state to begin receiving its data from the
 * original master.
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
 * \param expectedObjects
 *      Estimate of the total number of objects that will be migrated.
 *
 * \param expectedBytes
 *      Estimate of the total number of bytes that will be migrated.
 */
void
MasterClient::prepForMigration(uint64_t tableId,
                               uint64_t firstKey,
                               uint64_t lastKey,
                               uint64_t expectedObjects,
                               uint64_t expectedBytes)
{
    Buffer req, resp;

    PrepForMigrationRpc::Request& reqHdr(allocHeader<PrepForMigrationRpc>(req));
    reqHdr.tableId = tableId;
    reqHdr.firstKey = firstKey;
    reqHdr.lastKey = lastKey;
    reqHdr.expectedObjects = expectedObjects;
    reqHdr.expectedBytes = expectedBytes;

    sendRecv<PrepForMigrationRpc>(session, req, resp);
    checkStatus(HERE);
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

/**
 * Request that a master add some migrated data to its storage.
 * The receiving master will not service requests on the data,
 * but will add it to its log and hash table.
 *
 * \param tableId
 *      Identifier for the table.
 *
 * \param firstKey
 *      First key of the tablet range to be migrated.
 *
 * \param segment
 *      Segment containing the data to be migrated.
 *
 * \param segmentBytes
 *      Number of bytes in the segment to be migrated.
 */
void
MasterClient::receiveMigrationData(uint64_t tableId,
                                   uint64_t firstKey,
                                   const void* segment,
                                   uint32_t segmentBytes)
{
    Buffer req, resp;

    ReceiveMigrationDataRpc::Request& reqHdr(
        allocHeader<ReceiveMigrationDataRpc>(req));
    reqHdr.tableId = tableId;
    reqHdr.firstKey = firstKey;
    reqHdr.segmentBytes = segmentBytes;
    memcpy(new(&req, APPEND) char[segmentBytes],
           segment,
           segmentBytes);

    sendRecv<ReceiveMigrationDataRpc>(session, req, resp);
    checkStatus(HERE);
}

/// Start a multiRead RPC. See MasterClient::multiRead.
MasterClient::MultiRead::MultiRead(MasterClient& client,
                                   std::vector<ReadObject*>& requests)
    : client(client)
    , requestBuffer()
    , responseBuffer()
    , state()
    , requests(requests)
{
    MultiReadRpc::Request&
        reqHdr(client.allocHeader<MultiReadRpc>(requestBuffer));
    reqHdr.count = downCast<uint32_t>(requests.size());

    foreach (ReadObject *request, requests) {
        new(&requestBuffer, APPEND)
            MultiReadRpc::Request::Part(request->tableId, request->keyLength);
        Buffer::Chunk::appendToBuffer(&requestBuffer, request->key,
                                      request->keyLength);
    }

    state = client.send<MultiReadRpc>(client.session, requestBuffer,
                                      responseBuffer);
}

/// Wait for multiRead RPC to complete.
void
MasterClient::MultiRead::complete()
{
    const MultiReadRpc::Response& respHdr(client.recv<MultiReadRpc>(state));
    client.checkStatus(HERE);

    uint32_t respOffset = downCast<uint32_t>(sizeof(respHdr));

    // Each iteration extracts one object from the response
    foreach (ReadObject *request, requests) {
        const Status *status = responseBuffer.getOffset<Status>(respOffset);
        respOffset += downCast<uint32_t>(sizeof(Status));
        request->status = *status;

        if (*status == STATUS_OK) {

            const SegmentEntry* entry = responseBuffer.getOffset<SegmentEntry>(
                                                                    respOffset);
            respOffset += downCast<uint32_t>(sizeof(SegmentEntry));

            /*
            * Need to check checksum
            * If computed checksum does not match stored checksum for a segment:
            * Retry getting the data from server.
            * If it is still bad, ensure (in some way) that data on the server
            * is corrupted. Then crash that server.
            * Wait for recovery and then return the data
            */

            const Object* obj = responseBuffer.getOffset<Object>(respOffset);
            respOffset += downCast<uint32_t>(sizeof(Object));
            respOffset += obj->keyLength;

            request->version = obj->version;

            uint32_t dataLength = obj->dataLength(entry->length);
            request->value->construct();
            responseBuffer.copy(respOffset, dataLength,
                                new(request->value->get(), APPEND)
                                char[dataLength]);
            respOffset += dataLength;
        }
    }
}

/**
 * Read the current contents of multiple objects.
 *
 * \param requests
 *      Vector (of ReadObject's) listing the objects to be read
 *      and where to place their values
 *
 * \exception RejectRulesException
 * \exception InternalError
 */
void
MasterClient::multiRead(std::vector<ReadObject*> requests)
{
    MultiRead(*this, requests).complete();
}

/**
 * Increments a numeric object by a specifiable value. The
 * object should be an 8-byte, two's complement, little-endian integer.
 * If the object has a different size than 8bytes,
 * the method throws a STATUS_INVALID_OBJECT exception.
 * The increment value can be negative in order to decrease the value of the
 * object.
 *
 * \param tableId
 *      The table containing the to be incremented object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated like a string.
 *      The caller is responsible for ensuring that this key remains valid
 *      until the call is reaped/canceled.
 * \param keyLength
 *      Size in bytes of the key.
 * \param incrementValue
 *      The value that the object should be incremented by (can be negative).
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the read
 *      should be aborted with an error.
 * \param[out] version
 *      May not be NULL. The version number of the object is returned here.
 * \param[out] newValue
 *      May not be NULL. The new value of the object after incrementing.
 */
void
MasterClient::increment(uint64_t tableId, const char* key, uint16_t keyLength,
               int64_t incrementValue, const RejectRules* rejectRules,
               uint64_t* version, int64_t* newValue)
{
    Buffer req, resp;
    IncrementRpc::Request& reqHdr(allocHeader<IncrementRpc>(req));
    reqHdr.tableId = tableId;
    reqHdr.keyLength = keyLength;
    reqHdr.incrementValue = incrementValue;
    reqHdr.rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    Buffer::Chunk::appendToBuffer(&req, key, keyLength);

    const IncrementRpc::Response& respHdr(sendRecv<IncrementRpc>(
                                                        session, req, resp));
    if (version != NULL)
        *version = respHdr.version;
    if (newValue != NULL)
        *newValue = respHdr.newValue;

    checkStatus(HERE);
}

/**
 * Return whether a replica for a segment created by this master may still
 * be needed for recovery. Backups use this after restarting after a failure
 * to determine if replicas found in persistent storage must be retained.
 *
 * The cluster membership protocol must guarantee that if the master "knows
 * about" the calling backup server that it must already know about the crash
 * of the backup which created the on-storage replicas the calling backup
 * has rediscovered.  This guarantees that when the master responds to this
 * call that it must have already recovered from crash mentioned above if
 * it returns false.
 *
 * \param backupServerId
 *      The server id which is requesting information about a replica.
 *      This is used to ensure the master is aware of the backup via
 *      the cluster membership protocol, which ensures that it is
 *      aware of any crash of the backup that created the replica
 *      being inquired about.
 * \param segmentId
 *      The segmentId of the replica which a backup server is considering
 *      freeing.
 * \return
 *      Master returns true if the segment is not currently known to be
 *      adequately replicated. This means if the master knows of any
 *      replicas which haven't been fully synced or that haven't been
 *      fully recreated in response to a crash it returns true.  Otherwise,
 *      if the master believes the segment is adequately replicated then
 *      it returns false.
 */
bool
MasterClient::isReplicaNeeded(ServerId backupServerId, uint64_t segmentId)
{
    Buffer req, resp;
    IsReplicaNeededRpc::Request& reqHdr(allocHeader<IsReplicaNeededRpc>(req));
    reqHdr.backupServerId = backupServerId.getId();
    reqHdr.segmentId = segmentId;
    const IsReplicaNeededRpc::Response& respHdr(sendRecv<IsReplicaNeededRpc>(
                                                        session, req, resp));
    checkStatus(HERE);
    return respHdr.needed;
}

/**
 * Delete an object from a table. If the object does not currently exist and
 * no rejectRules match, then the operation succeeds without doing anything.
 *
 * \param tableId
 *      The table containing the object to be deleted (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated like a string.
 *      The caller is responsible for ensuring that this key remains valid
 *      until the call is reaped/canceled.
 * \param keyLength
 *      Size in bytes of the key.
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the delete
 *      should be aborted with an error. If NULL, the object is
 *      deleted unconditionally.
 * \param[out] version
 *      If non-NULL, the version number of the object (prior to
 *      deletion) is returned here. If the object didn't exist
 *      then 0 will be returned.
 *
 * \exception RejectRulesException
 * \exception InternalError
 */
void
MasterClient::remove(uint64_t tableId, const char* key, uint16_t keyLength,
                     const RejectRules* rejectRules, uint64_t* version)
{
    Buffer req, resp;
    RemoveRpc::Request& reqHdr(allocHeader<RemoveRpc>(req));
    reqHdr.tableId = tableId;
    reqHdr.keyLength = keyLength;
    reqHdr.rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    Buffer::Chunk::appendToBuffer(&req, key, keyLength);

    const RemoveRpc::Response& respHdr(sendRecv<RemoveRpc>(session, req, resp));
    if (version != NULL)
        *version = respHdr.version;
    checkStatus(HERE);
}

/**
 * Instruct the master that it must no longer serve requests for the tablet
 * specified. The server may reclaim all memory previously allocated to that
 * tablet.
 * \warning
 *      Adding a tablet, removing it, and then adding it back is not currently
 *      supported.
 */
void
MasterClient::dropTabletOwnership(uint64_t tableId,
                                  uint64_t firstKey,
                                  uint64_t lastKey)
{
    Buffer req, resp;
    DropTabletOwnershipRpc::Request& reqHdr(
        allocHeader<DropTabletOwnershipRpc>(req));
    reqHdr.tableId = tableId;
    reqHdr.firstKey = firstKey;
    reqHdr.lastKey = lastKey;
    sendRecv<DropTabletOwnershipRpc>(session, req, resp);
    checkStatus(HERE);
}

/**
 * Split a tablet in a master.
 *
 * This function splits a tablet inside a master and is issued by the
 * coordinator. The tablet is identified by the table it belongs to and its
 * start- and endKeyHash. Additionally, a splitKeyHash has
 * to be provided that indicates where the tablet should be split. This key
 * will be the first key of the second part after the split.
 *
 * \param tableId
 *      Id of the table that contains the to be split tablet
 * \param startKeyHash
 *      First key of the key range of the to be split tablet.
 * \param endKeyHash
 *      Last key of the key range of the to be split tablet.
 * \param splitKeyHash
 *      The key where the split occurs.
 *
 */
void
MasterClient::splitMasterTablet(uint64_t tableId,
                                uint64_t startKeyHash,
                                uint64_t endKeyHash,
                                uint64_t splitKeyHash)
{
    Buffer req, resp;
    SplitMasterTabletRpc::Request& reqHdr(
        allocHeader<SplitMasterTabletRpc>(req));
    reqHdr.tableId = tableId;
    reqHdr.startKeyHash = startKeyHash;
    reqHdr.endKeyHash = endKeyHash;
    reqHdr.splitKeyHash = splitKeyHash;
    sendRecv<SplitMasterTabletRpc>(session, req, resp);
    checkStatus(HERE);
}

/**
 * Instruct a master that has been receiving migrated tablet data that it
 * should take ownership of the tablet. The coordinator will issue this to
 * complete the migration. Before this RPC is received, the coordinator will
 * have already considered this host the owner of the data. This call simply
 * tells the host to begin servicing requests for the tablet.
 * \warning
 *      Adding a tablet, removing it, and then adding it back is not currently
 *      supported.
 */
void
MasterClient::takeTabletOwnership(uint64_t tableId,
                                  uint64_t firstKey,
                                  uint64_t lastKey)
{
    Buffer req, resp;
    TakeTabletOwnershipRpc::Request& reqHdr(
        allocHeader<TakeTabletOwnershipRpc>(req));
    reqHdr.tableId = tableId;
    reqHdr.firstKey = firstKey;
    reqHdr.lastKey = lastKey;
    sendRecv<TakeTabletOwnershipRpc>(session, req, resp);
    checkStatus(HERE);
}

/// Start a write RPC for an object. See MasterClient::write.
MasterClient::Write::Write(MasterClient& client,
                           uint64_t tableId,
                           const char* key, uint16_t keyLength,
                           const void* buf, uint32_t length,
                           const RejectRules* rejectRules,
                           uint64_t* version, bool async)
    : client(client)
    , version(version)
    , requestBuffer()
    , responseBuffer()
    , state()
{
    WriteRpc::Request& reqHdr(client.allocHeader<WriteRpc>(requestBuffer));
    reqHdr.tableId = tableId;
    reqHdr.keyLength = keyLength;
    reqHdr.length = length;
    reqHdr.rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    reqHdr.async = async;
    Buffer::Chunk::appendToBuffer(&requestBuffer, key, keyLength);
    Buffer::Chunk::appendToBuffer(&requestBuffer, buf, length);
    state = client.send<WriteRpc>(client.session,
                                  requestBuffer,
                                  responseBuffer);
}

/// Start a write RPC. See MasterClient::write.
MasterClient::Write::Write(MasterClient& client,
                           uint64_t tableId,
                           const char* key, uint16_t keyLength,
                           Buffer& buffer,
                           const RejectRules* rejectRules,
                           uint64_t* version, bool async)
    : client(client)
    , version(version)
    , requestBuffer()
    , responseBuffer()
    , state()
{
    WriteRpc::Request& reqHdr(client.allocHeader<WriteRpc>(requestBuffer));
    reqHdr.tableId = tableId;
    reqHdr.keyLength = keyLength;
    reqHdr.length = buffer.getTotalLength();
    reqHdr.rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    reqHdr.async = async;
    Buffer::Chunk::appendToBuffer(&requestBuffer, key, keyLength);
    for (Buffer::Iterator it(buffer); !it.isDone(); it.next())
        Buffer::Chunk::appendToBuffer(&requestBuffer,
                                      it.getData(), it.getLength());
    state = client.send<WriteRpc>(client.session,
                                  requestBuffer,
                                  responseBuffer);
}

/// Wait for the write RPC to complete
void
MasterClient::Write::operator()()
{
    const WriteRpc::Response& respHdr(client.recv<WriteRpc>(state));
    if (version != NULL)
        *version = respHdr.version;
    client.checkStatus(HERE);
}

/**
 * Write a specific object in a table; overwrite any existing object,
 * or create a new object if none existed.
 *
 * \param tableId
 *      The table containing the desired object (return value from a
 *      previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated like a string.
 *      The caller is responsible for ensuring that this key remains valid
 *      until the call is reaped/canceled.
 * \param keyLength
 *      Size in bytes of the key.
 * \param buf
 *      Address of the first byte of the new contents for the object;
 *      must contain at least length bytes.
 * \param length
 *      Size in bytes of the new contents for the object.
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the write
 *      should be aborted with an error. NULL means the object should
 *      be written unconditionally.
 * \param[out] version
 *      If non-NULL, the version number of the object is returned
 *      here. If the operation was successful this will be the new
 *      version for the object; if this object has ever existed
 *      previously the new version is guaranteed to be greater than
 *      any previous version of the object. If the operation failed
 *      then the version number returned is the current version of
 *      the object, or 0 if the object does not exist.
 * \param async
 *      If true, the new object will not be immediately replicated to backups.
 *      Data loss may occur!
 *
 * \exception RejectRulesException
 * \exception InternalError
 */
void
MasterClient::write(uint64_t tableId, const char* key, uint16_t keyLength,
                    const void* buf, uint32_t length,
                    const RejectRules* rejectRules, uint64_t* version,
                    bool async)
{
    Write(*this, tableId, key, keyLength, buf, length, rejectRules,
          version, async)();
}

}  // namespace RAMCloud
