
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
#include "Log.h"
#include "Status.h"

namespace RAMCloud {

// Default RejectRules to use if none are provided by the caller.
RejectRules defaultRejectRules;

/// Start a create RPC. See MasterClient::create.
MasterClient::Create::Create(MasterClient& client,
                             uint32_t tableId,
                             const void* buf, uint32_t length,
                             uint64_t* version, bool async)
    : client(client)
    , version(version)
    , requestBuffer()
    , responseBuffer()
    , state()
{
    CreateRpc::Request& reqHdr(client.allocHeader<CreateRpc>(requestBuffer));
    reqHdr.tableId = tableId;
    reqHdr.length = length;
    reqHdr.async = async;
    Buffer::Chunk::appendToBuffer(&requestBuffer, buf, length);
    state = client.send<CreateRpc>(client.session,
                                   requestBuffer,
                                   responseBuffer);
}

/// Wait for the create RPC to complete.
uint64_t
MasterClient::Create::operator()()
{
    const CreateRpc::Response& respHdr(client.recv<CreateRpc>(state));
    if (version != NULL)
        *version = respHdr.version;
    client.checkStatus(HERE);
    return respHdr.id;
}

/// Start a write RPC. See MasterClient::write.
MasterClient::Write::Write(MasterClient& client,
                           uint32_t tableId, uint64_t id,
                           Buffer& buffer,
                           const RejectRules* rejectRules, uint64_t* version,
                           bool async)
    : client(client)
    , version(version)
    , requestBuffer()
    , responseBuffer()
    , state()
{
    WriteRpc::Request& reqHdr(client.allocHeader<WriteRpc>(requestBuffer));
    reqHdr.id = id;
    reqHdr.tableId = tableId;
    reqHdr.length = buffer.getTotalLength();
    reqHdr.rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    reqHdr.async = async;
    for (Buffer::Iterator it(buffer); !it.isDone(); it.next())
        Buffer::Chunk::appendToBuffer(&requestBuffer,
                                      it.getData(), it.getLength());
    state = client.send<WriteRpc>(client.session,
                                  requestBuffer,
                                  responseBuffer);
}

/// Start a write RPC. See MasterClient::write.
MasterClient::Write::Write(MasterClient& client,
                           uint32_t tableId, uint64_t id,
                           const void* buf, uint32_t length,
                           const RejectRules* rejectRules, uint64_t* version,
                           bool async)
    : client(client)
    , version(version)
    , requestBuffer()
    , responseBuffer()
    , state()
{
    WriteRpc::Request& reqHdr(client.allocHeader<WriteRpc>(requestBuffer));
    reqHdr.id = id;
    reqHdr.tableId = tableId;
    reqHdr.length = length;
    reqHdr.rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    reqHdr.async = async;
    Buffer::Chunk::appendToBuffer(&requestBuffer, buf, length);
    state = client.send<WriteRpc>(client.session,
                                  requestBuffer,
                                  responseBuffer);
}

/// Wait for the write RPC to complete.
void
MasterClient::Write::operator()()
{
    const WriteRpc::Response& respHdr(client.recv<WriteRpc>(state));
    if (version != NULL)
        *version = respHdr.version;
    client.checkStatus(HERE);
}

/**
 * Report to a master that a particular backup has failed so that it
 * can rereplicate any segments that might have been stored there.
 *
 * \param client
 *      The MasterClient instance over which the RPC should be issued.
 * \param backupId
 *      The server id of a backup which has failed.
 */
MasterClient::RereplicateSegments::RereplicateSegments(MasterClient& client,
                                                       uint64_t backupId)
    : client(client)
    , backupId(backupId)
    , requestBuffer()
    , responseBuffer()
    , state()
{
    RereplicateSegmentsRpc::Request& reqHdr(
        client.allocHeader<RereplicateSegmentsRpc>(requestBuffer));
    reqHdr.backupId = backupId;
    state = client.send<RereplicateSegmentsRpc>(client.session,
                                                requestBuffer,
                                                responseBuffer);
}

/// Wait for the rereplicateSegments RPC to complete.
void
MasterClient::RereplicateSegments::operator()()
{
    client.recv<RereplicateSegmentsRpc>(state);
    client.checkStatus(HERE);
}

/**
 * Fill a master server with the given number of objects, each of the
 * same given size. Objects are added to all tables in the master in
 * a round-robin fashion.
 *
 * This method exists simply to quickly fill a master for experiments.
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
 * Create a new object in a table, with an id assigned by the server.
 *
 * \param tableId
 *      The table in which the new object is to be created (return
 *      value from a previous call to openTable).
 * \param buf
 *      Address of the first byte of the contents for the new object;
 *      must contain at least length bytes.
 * \param length
 *      Size in bytes of the new object.
 * \param[out] version
 *      If non-NULL, the version number of the new object is returned
 *      here; guaranteed to be greater than that of any previous
 *      object that used the same id in the same table.
 * \param async
 *      If true, the new object will not be immediately replicated to backups.
 *      Data loss may occur!
 * \return
 *      The identifier for the new object: unique within the table
 *      and guaranteed not to be in use already. Generally, servers
 *      choose ids sequentially starting at 1 (but they may need
 *      to skip over ids previously created using \c write).
 * \exception InternalError
 */
uint64_t
MasterClient::create(uint32_t tableId, const void* buf, uint32_t length,
                     uint64_t* version, bool async)
{
    return Create(*this, tableId, buf, length, version, async)();
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
MasterClient::ping()
{
    Buffer req, resp;
    allocHeader<PingRpc>(req);
    sendRecv<PingRpc>(session, req, resp);
    checkStatus(HERE);
}

/**
 * Recover a set of tablets on behalf of a crashed master.
 *
 * \param masterId
 *      The id of the crashed master whose data is to be recovered.
 * \param partitionId
 *      The partition id of #tablets inside the crashed master's will.
 * \param tablets
 *      A set of tables with key ranges describing which poritions of which
 *      tables the recovery Master should take over for.
 * \param backups
 *      A list of backup locators along with a segmentId specifying for each
 *      segmentId a backup who can provide a filtered recovery data segment.
 *      A particular segment may be listed more than once if it has multiple
 *      viable backups, hence a particular backup locator can also be listed
 *      many times.
 */
void
MasterClient::recover(uint64_t masterId, uint64_t partitionId,
                      const ProtoBuf::Tablets& tablets,
                      const ProtoBuf::ServerList& backups)
{
    Buffer req, resp;
    RecoverRpc::Request& reqHdr(allocHeader<RecoverRpc>(req));
    reqHdr.masterId = masterId;
    reqHdr.partitionId = partitionId;
    reqHdr.tabletsLength = serializeToResponse(req, tablets);
    reqHdr.serverListLength = serializeToResponse(req, backups);
    sendRecv<RecoverRpc>(session, req, resp);
    checkStatus(HERE);
}

MasterClient::Recover::Recover(MasterClient& client,
                               uint64_t masterId, uint64_t partitionId,
                               const ProtoBuf::Tablets& tablets,
                               const char* backups, uint32_t backupsLen)
    : client(client)
    , requestBuffer()
    , responseBuffer()
    , state()
{
    RecoverRpc::Request& reqHdr(client.allocHeader<RecoverRpc>(requestBuffer));
    reqHdr.masterId = masterId;
    reqHdr.partitionId = partitionId;
    reqHdr.tabletsLength = serializeToResponse(requestBuffer, tablets);
    reqHdr.serverListLength = backupsLen;
    Buffer::Chunk::appendToBuffer(&requestBuffer, backups, backupsLen);
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
MasterClient::recover(uint64_t masterId, uint64_t partitionId,
                      const ProtoBuf::Tablets& tablets,
                      const char* backups, uint32_t backupsLen)
{
    Recover(*this, masterId, partitionId, tablets, backups, backupsLen)();
}

MasterClient::Read::Read(MasterClient& client,
                         uint32_t tableId, uint64_t id, Buffer* value,
                         const RejectRules* rejectRules, uint64_t* version)
    : client(client)
    , version(version)
    , requestBuffer()
    , responseBuffer(*value)
    , state()
{
    responseBuffer.reset();
    ReadRpc::Request& reqHdr(client.allocHeader<ReadRpc>(requestBuffer));
    reqHdr.tableId = tableId;
    reqHdr.id = id;
    reqHdr.rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    state = client.send<ReadRpc>(client.session,
                                 requestBuffer,
                                 responseBuffer);
}

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
 *      a previous call to openTable).
 * \param id
 *      Identifier within tableId of the object to be read.
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
MasterClient::read(uint32_t tableId, uint64_t id, Buffer* value,
        const RejectRules* rejectRules, uint64_t* version)
{
    Read(*this, tableId, id, value, rejectRules, version)();
}

/**
 * Read the current contents of multiple objects.
 *
 * \param requests
 *      Vector (of ReadObject's) listing the objects to be read
 *      and where to place their values
 */
void
MasterClient::multiRead(std::vector<ReadObject*> requests)
{
    // Create Request
    Buffer req;
    MultiReadRpc::Request& reqHdr(allocHeader<MultiReadRpc>(req));
    reqHdr.count = downCast<uint32_t>(requests.size());

    foreach (ReadObject *request, requests) {
        new(&req, APPEND) MultiReadRpc::Request::Part(request->tableId,
                                                      request->id);
    }

    // Send and Receive Rpc
    Buffer respBuffer;
    const MultiReadRpc::Response& respHdr(sendRecv<MultiReadRpc>(
                                          session, req, respBuffer));
    checkStatus(HERE);

    // Extract Response
    uint32_t respOffset = downCast<uint32_t>(sizeof(respHdr));

    // Each iteration extracts one object from the response
    foreach (ReadObject *request, requests) {
        const Status *status = respBuffer.getOffset<Status>(respOffset);
        respOffset += downCast<uint32_t>(sizeof(Status));
        request->status = *status;

        if (*status == STATUS_OK) {
            const SegmentEntry* entry = respBuffer.getOffset<SegmentEntry>(
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

            const Object* obj = respBuffer.getOffset<Object>(respOffset);
            respOffset += downCast<uint32_t>(sizeof(Object));

            request->version = obj->version;

            uint32_t dataLength = obj->dataLength(entry->length);
            request->value->construct();
            respBuffer.copy(respOffset, dataLength, new(
                            request->value->get(), APPEND) char[dataLength]);
            respOffset += dataLength;
        }
    }
}

MasterClient::MultiRead::MultiRead(MasterClient& client,
                                   std::vector<ReadObject*>& requests)
    : client(client)
    , requestBuffer()
    , responseBuffer()
    , state()
    , requests(requests)
{
    MultiReadRpc::Request& reqHdr(client.allocHeader<MultiReadRpc>(
                                                    requestBuffer));
    reqHdr.count = downCast<uint32_t>(requests.size());

    foreach (ReadObject *request, requests) {
        new(&requestBuffer, APPEND) MultiReadRpc::Request::Part(
                                  request->tableId, request->id);
    }

    state = client.send<MultiReadRpc>(client.session, requestBuffer,
                                      responseBuffer);
}

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

            request->version = obj->version;

            uint32_t dataLength = obj->dataLength(entry->length);
            request->value->construct();
            responseBuffer.copy(respOffset, dataLength, new(
                            request->value->get(), APPEND) char[dataLength]);
            respOffset += dataLength;
        }
    }
}

/**
 * Delete an object from a table. If the object does not currently exist
 * and no rejectRules match, then the operation succeeds without doing
 * anything.
 *
 * \param tableId
 *      The table containing the object to be deleted (return value from
 *      a previous call to openTable).
 * \param id
 *      Identifier within tableId of the object to be deleted.
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the delete
 *      should be aborted with an error.  If NULL, the object is
 *      deleted unconditionally.
 * \param[out] version
 *      If non-NULL, the version number of the object (prior to
 *      deletion) is returned here.  If the object didn't exist
 *      then 0 will be returned.
 *
 * \exception RejectRulesException
 * \exception InternalError
 */
void
MasterClient::remove(uint32_t tableId, uint64_t id,
        const RejectRules* rejectRules, uint64_t* version)
{
    Buffer req, resp;
    RemoveRpc::Request& reqHdr(allocHeader<RemoveRpc>(req));
    reqHdr.id = id;
    reqHdr.tableId = tableId;
    reqHdr.rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    const RemoveRpc::Response& respHdr(sendRecv<RemoveRpc>(session, req, resp));
    if (version != NULL)
        *version = respHdr.version;
    checkStatus(HERE);
}

/**
 * Set the set of tablets the master owns.
 * Any new tablets appearing in this set will have no objects. Any tablets that
 * no longer appear in this set will be deleted from the master.
 * \warning
 *      Adding a tablet, removing it, and then adding it back is not currently
 *      supported.
 */
void
MasterClient::setTablets(const ProtoBuf::Tablets& tablets)
{
    Buffer req, resp;
    SetTabletsRpc::Request& reqHdr(allocHeader<SetTabletsRpc>(req));
    reqHdr.tabletsLength = ProtoBuf::serializeToResponse(req, tablets);
    sendRecv<SetTabletsRpc>(session, req, resp);
    checkStatus(HERE);
}

/**
 * Write a specific object in a table; overwrite any existing
 * object, or create a new object if none existed.
 *
 * \param tableId
 *      The table containing the desired object (return value from a
 *      previous call to openTable).
 * \param id
 *      Identifier within tableId of the object to be written; may or
 *      may not refer to an existing object.
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
MasterClient::write(uint32_t tableId, uint64_t id,
                    const void* buf, uint32_t length,
                    const RejectRules* rejectRules, uint64_t* version,
                    bool async)
{
    Write(*this, tableId, id, buf, length, rejectRules, version, async)();
}

}  // namespace RAMCloud
