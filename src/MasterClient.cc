
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
    value->reset();
    Buffer req;
    ReadRpc::Request& reqHdr(allocHeader<ReadRpc>(req));
    reqHdr.id = id;
    reqHdr.tableId = tableId;
    reqHdr.rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    const ReadRpc::Response& respHdr(sendRecv<ReadRpc>(session, req, *value));
    if (version != NULL)
        *version = respHdr.version;

    // Truncate the response Buffer so that it consists of nothing
    // but the object data.
    value->truncateFront(sizeof(respHdr));
    assert(respHdr.length == value->getTotalLength());
    checkStatus(HERE);
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
