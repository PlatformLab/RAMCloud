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

#include "CoordinatorClient.h"
#include "CoordinatorSession.h"
#include "ShortMacros.h"
#include "ProtoBuf.h"

namespace RAMCloud {

/**
 * Servers call this when they come online. This request tells the coordinator
 * that the server is available and can be assigned work.
 * 
 * \param context
 *      Overall information about the RAMCloud server or client.
 * \param replacesId
 *      Server id the calling server used to operate at; the coordinator must
 *      make sure this server is removed from the cluster before enlisting
 *      the calling server.  If !isValid() then this step is skipped; the
 *      enlisting server is simply added.
 * \param serviceMask
 *      Which services are available on the enlisting server. MASTER_SERVICE,
 *      BACKUP_SERVICE, etc.
 * \param localServiceLocator
 *      Describes how other hosts can contact this server.
 * \param readSpeed
 *      Read speed of the backup in MB/s if serviceMask includes BACKUP,
 *      otherwise ignored.
 * \param writeSpeed
 *      Write speed of the backup in MB/s if serviceMask includes BACKUP,
 *      otherwise ignored.
 * \return
 *      A ServerId guaranteed never to have been used before.
 */
ServerId
CoordinatorClient::enlistServer(Context& context, ServerId replacesId,
        ServiceMask serviceMask, string localServiceLocator,
        uint32_t readSpeed, uint32_t writeSpeed)
{
    EnlistServerRpc2 rpc(context, replacesId, serviceMask, localServiceLocator,
            readSpeed, writeSpeed);
    return rpc.wait();
}

/**
 * Constructor for EnlistServerRpc: initiates an RPC in the same way as
 * #CoordinatorClient::enlistServer, but returns once the RPC has been
 * initiated, without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param replacesId
 *      Server id the calling server used to operate at; the coordinator must
 *      make sure this server is removed from the cluster before enlisting
 *      the calling server.  If !isValid() then this step is skipped; the
 *      enlisting server is simply added.
 * \param serviceMask
 *      Which services are available on the enlisting server. MASTER_SERVICE,
 *      BACKUP_SERVICE, etc.
 * \param localServiceLocator
 *      Describes how other hosts can contact this server.
 * \param readSpeed
 *      Read speed of the backup in MB/s if serviceMask includes BACKUP,
 *      otherwise ignored.
 * \param writeSpeed
 *      Write speed of the backup in MB/s if serviceMask includes BACKUP,
 *      otherwise ignored.
 * \return
 *      A ServerId guaranteed never to have been used before.
 */
CoordinatorClient::EnlistServerRpc2::EnlistServerRpc2(Context & context,
        ServerId replacesId, ServiceMask serviceMask,
        string localServiceLocator, uint32_t readSpeed, uint32_t writeSpeed)
    : CoordinatorRpcWrapper(context,
            sizeof(WireFormat::EnlistServer::Response))
{
    WireFormat::EnlistServer::Request& reqHdr(
            allocHeader<WireFormat::EnlistServer>());
    reqHdr.replacesId = replacesId.getId();
    reqHdr.serviceMask = serviceMask.serialize();
    reqHdr.readSpeed = readSpeed;
    reqHdr.writeSpeed = writeSpeed;
    reqHdr.serviceLocatorLength =
        downCast<uint32_t>(localServiceLocator.length() + 1);
    strncpy(new(&request, APPEND) char[reqHdr.serviceLocatorLength],
            localServiceLocator.c_str(),
            reqHdr.serviceLocatorLength);
    send();
}

/**
 * Wait for the RPC to complete, and return the same results as
 * #CoordinatorClient::enlistServer.
 */
ServerId
CoordinatorClient::EnlistServerRpc2::wait()
{
    waitInternal(*context.dispatch);
    const WireFormat::EnlistServer::Response& respHdr(
            getResponseHeader<WireFormat::EnlistServer>());
    if (respHdr.common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr.common.status);
    return ServerId(respHdr.serverId);
}

//-------------------------------------------------------
// OLD: everything below here should eventually go away.
//-------------------------------------------------------


/**
 * Create a new table.
 *
 * \param name
 *      Name for the new table (NULL-terminated string).
 *
 * \param serverSpan
 *      The number of servers across which this table will be divided
 *      (defaults to 1). Keys within the table will be evenly distributed
 *      to this number of servers according to their hash. This is a temporary
 *      work-around until tablet migration is complete; until then, we must
 *      place tablets on servers statically.
 *
 * \exception InternalError
 */
void
CoordinatorClient::createTable(const char* name, uint32_t serverSpan)
{
    Buffer req;
    uint32_t length = downCast<uint32_t>(strlen(name) + 1);
    CreateTableRpc::Request& reqHdr(allocHeader<CreateTableRpc>(req));
    reqHdr.nameLength = length;
    reqHdr.serverSpan = serverSpan;
    memcpy(new(&req, APPEND) char[length], name, length);
    while (true) {
        Buffer resp;
        Transport::SessionRef session =
                context.coordinatorSession->getSession();
        sendRecv<CreateTableRpc>(session, req, resp);
        try {
            checkStatus(HERE);
            return;
        } catch (const RetryException& e) {
            LOG(DEBUG, "RETRY trying to create table");
            usleep(500);
        }
    }
}

/**
 * Delete a table.
 *
 * All objects in the table are implicitly deleted, along with any
 * other information associated with the table (such as, someday,
 * indexes).  If the table does not currently exist than the operation
 * returns successfully without actually doing anything.
 *
 * \param name
 *      Name of the table to delete (NULL-terminated string).
 *
 * \exception InternalError
 */
void
CoordinatorClient::dropTable(const char* name)
{
    Buffer req, resp;
    uint32_t length = downCast<uint32_t>(strlen(name) + 1);
    DropTableRpc::Request& reqHdr(allocHeader<DropTableRpc>(req));
    reqHdr.nameLength = length;
    memcpy(new(&req, APPEND) char[length], name, length);
    Transport::SessionRef session = context.coordinatorSession->getSession();
    sendRecv<DropTableRpc>(session, req, resp);
    checkStatus(HERE);
}

/**
 * Split a tablet.
 *
 * The tablet is identified by the table it belongs to and its start- and
 * endKeyHash. Additionally, a splitKeyHash has to be provided that
 * indicates where the tablet should be split. This key will be the
 * first key of the second part after the split. Internally, this
 * function updates the entries in the tabletMap in the cooridnator first and
 * then tells the corresponding master to execute the split as well.
 *
 * \param name
 *      Name of the table that contains the to be split tablet
 *     (NULL-terminated string).
 * \param startKeyHash
 *      First key of the key range of the to be split tablet.
 * \param endKeyHash
 *      Last key of the key range of the to be split tablet.
 * \param splitKeyHash
 *      The key where the split occurs.
 *
 * \exception StatusRequestFormatError
 * \exception StatusTableDoesntExist
 * \exception StatusTabletDoesntExist
 */
void
CoordinatorClient::splitTablet(const char* name, uint64_t startKeyHash,
                               uint64_t endKeyHash, uint64_t splitKeyHash)
{
    Buffer req, resp;
    uint32_t length = downCast<uint32_t>(strlen(name) + 1);
    SplitTabletRpc::Request& reqHdr(allocHeader<SplitTabletRpc>(req));
    reqHdr.nameLength = length;
    reqHdr.startKeyHash = startKeyHash;
    reqHdr.endKeyHash = endKeyHash;
    reqHdr.splitKeyHash = splitKeyHash;
    memcpy(new(&req, APPEND) char[length], name, length);
    Transport::SessionRef session = context.coordinatorSession->getSession();
    sendRecv<SplitTabletRpc>(session, req, resp);
    checkStatus(HERE);
}

/**
 * Look up a table by name and return a small integer handle that
 * can be used to access the table.
 *
 * \param name
 *      Name of the desired table (NULL-terminated string).
 *
 * \return
 *      The return value is an identifier for the table; this is used
 *      instead of the table's name for most RAMCloud operations
 *      involving the table.
 *
 * \exception TableDoesntExistException
 * \exception InternalError
 */
uint64_t
CoordinatorClient::getTableId(const char* name)
{
    Buffer req, resp;
    uint32_t length = downCast<uint32_t>(strlen(name) + 1);
    GetTableIdRpc::Request& reqHdr(allocHeader<GetTableIdRpc>(req));
    reqHdr.nameLength = length;
    memcpy(new(&req, APPEND) char[length], name, length);
    Transport::SessionRef session = context.coordinatorSession->getSession();
    const GetTableIdRpc::Response& respHdr(
        sendRecv<GetTableIdRpc>(session, req, resp));
    checkStatus(HERE);
    return respHdr.tableId;
}

/**
 * List all live servers providing services of the given types.
 * \param[in] services
 *      Used to restrict the server list returned to containing only servers
 *      that support the specified services.  A server is returned if it
 *      matches *any* service described in \a services (as opposed to all the
 *      services).
 * \param[out] serverList
 *      An empty ServerList that will be filled with current servers supporting
 *      the desired services.
 */
void
CoordinatorClient::getServerList(ServiceMask services,
                                 ProtoBuf::ServerList& serverList)
{
    Buffer req;
    Buffer resp;
    GetServerListRpc::Request& reqHdr(
        allocHeader<GetServerListRpc>(req));
    reqHdr.serviceMask = services.serialize();
    Transport::SessionRef session = context.coordinatorSession->getSession();
    const GetServerListRpc::Response& respHdr(
        sendRecv<GetServerListRpc>(session, req, resp));
    checkStatus(HERE);
    ProtoBuf::parseFromResponse(resp, sizeof(respHdr),
                                respHdr.serverListLength, serverList);
}

/**
 * List all live servers.
 * Used in ensureServers.
 * \param[out] serverList
 *      An empty ServerList that will be filled with current servers.
 */
void
CoordinatorClient::getServerList(ProtoBuf::ServerList& serverList)
{
    getServerList({MASTER_SERVICE, BACKUP_SERVICE}, serverList);
}

/**
 * List all live master servers.
 * The failure detector uses this to periodically probe for failed masters.
 * \param[out] serverList
 *      An empty ServerList that will be filled with current master servers.
 */
void
CoordinatorClient::getMasterList(ProtoBuf::ServerList& serverList)
{
    getServerList({MASTER_SERVICE}, serverList);
}

/**
 * List all live backup servers.
 * Masters call and cache this periodically to find backups. The failure
 * detector also uses this to periodically probe for failed backups.
 * \param[out] serverList
 *      An empty ServerList that will be filled with current backup servers.
 */
void
CoordinatorClient::getBackupList(ProtoBuf::ServerList& serverList)
{
    getServerList({BACKUP_SERVICE}, serverList);
}

/**
 * Return the entire tablet map.
 * Clients use this to find objects.
 * If the returned data becomes too big, we should add parameters to
 * specify a subrange.
 * \param[out] tabletMap
 *      An empty Tablets that will be filled with current tablets.
 *      Each tablet has a service locator string describing where to find
 *      its master.
 */
void
CoordinatorClient::getTabletMap(ProtoBuf::Tablets& tabletMap)
{
    Buffer req;
    Buffer resp;
    Transport::SessionRef session = context.coordinatorSession->getSession();
    allocHeader<GetTabletMapRpc>(req);
    const GetTabletMapRpc::Response& respHdr(
        sendRecv<GetTabletMapRpc>(session, req, resp));
    checkStatus(HERE);
    ProtoBuf::parseFromResponse(resp, sizeof(respHdr),
                                respHdr.tabletMapLength, tabletMap);
}

/**
 * Report a slow or dead server.
 */
void
CoordinatorClient::hintServerDown(ServerId serverId)
{
    Buffer req;
    Buffer resp;
    Transport::SessionRef session = context.coordinatorSession->getSession();
    HintServerDownRpc::Request& reqHdr(allocHeader<HintServerDownRpc>(req));
    reqHdr.serverId = *serverId;
    sendRecv<HintServerDownRpc>(session, req, resp);
    checkStatus(HERE);
}

/**
 * Have all backups flush their dirty segments to storage.
 * This is useful for measuring recovery performance accurately.
 */
void
CoordinatorClient::quiesce()
{
    Buffer req;
    Buffer resp;
    Transport::SessionRef session = context.coordinatorSession->getSession();
    BackupQuiesceRpc::Request& reqHdr(
        allocHeader<BackupQuiesceRpc>(req));
    // By default this RPC is since the backup service; retarget it
    // for the coordinator service (which will forward it on to all
    // backups).
    reqHdr.common.service = COORDINATOR_SERVICE;
    sendRecv<BackupQuiesceRpc>(session, req, resp);
    checkStatus(HERE);
}

/**
 * After migrating all data for a tablet to another master, instruct the
 * coordinator to transfer ownership of that data and alert the new
 * master so that they take ownership (i.e. process requests on the
 * tablet).
 *
 * \param[in] tableId
 *      TableId of the tablet that was migrated.
 *
 * \param[in] firstKey
 *      First key in the range of the tablet that was migrated.
 *
 * \param[in] lastKey
 *      Last key in the range of the tablet that was migrated.
 *
 * \param[in] newOwnerMasterId
 *      ServerId of the master that we want ownership of the tablet
 *      to be transferred to.
 */
void
CoordinatorClient::reassignTabletOwnership(uint64_t tableId,
                                           uint64_t firstKey,
                                           uint64_t lastKey,
                                           ServerId newOwnerMasterId)
{
    Buffer req, resp;
    Transport::SessionRef session = context.coordinatorSession->getSession();

    ReassignTabletOwnershipRpc::Request& reqHdr(
        allocHeader<ReassignTabletOwnershipRpc>(req));
    reqHdr.tableId = tableId;
    reqHdr.firstKey = firstKey;
    reqHdr.lastKey = lastKey;
    reqHdr.newOwnerMasterId = *newOwnerMasterId;
    sendRecv<ReassignTabletOwnershipRpc>(session, req, resp);
    checkStatus(HERE);
}

/**
 * Tell the coordinator that the partition of a crashed master that it
 * asked this master to recover has finished (either successfully, or
 * unsuccessfully).
 *
 * \param recoveryId
 *      Identifies the recovery this master has completed a portion of.
 *      This id is received as part of the recover rpc and should simply
 *      be returned as given by the coordinator.
 * \param recoveryMasterId
 *      ServerId of the server invoking this method.
 * \param tablets
 *      The tablets which form a partition of a will which are
 *      now done recovering.
 * \param successful
 *      Indicates to the coordinator whether this recovery master succeeded
 *      in recovering its partition of the crashed master. If false the
 *      coordinator will not assign ownership to this master and this master
 *      can clean up any state resulting attempting recovery.
 */
void
CoordinatorClient::recoveryMasterFinished(uint64_t recoveryId,
                                          ServerId recoveryMasterId,
                                          const ProtoBuf::Tablets& tablets,
                                          bool successful)
{
    Buffer req, resp;
    Transport::SessionRef session = context.coordinatorSession->getSession();
    RecoveryMasterFinishedRpc::Request&
        reqHdr(allocHeader<RecoveryMasterFinishedRpc>(req));
    reqHdr.recoveryId = recoveryId;
    reqHdr.recoveryMasterId = recoveryMasterId.getId();
    reqHdr.tabletsLength = serializeToRequest(req, tablets);
    reqHdr.successful = successful;
    sendRecv<RecoveryMasterFinishedRpc>(session, req, resp);
    checkStatus(HERE);
}

/**
 * Update a masterId's Will with the Coordinator.
 *
 * \param[in] masterId
 *      The masterId whose Will we're updating.
 * \param[in] will
 *      The ProtoBuf serialised representation of the Will.
 */
void
CoordinatorClient::setWill(uint64_t masterId, const ProtoBuf::Tablets& will)
{
    Buffer req, resp;
    Transport::SessionRef session = context.coordinatorSession->getSession();
    SetWillRpc::Request& reqHdr(allocHeader<SetWillRpc>(req));
    reqHdr.masterId = masterId;
    reqHdr.willLength = serializeToRequest(req, will);
    sendRecv<SetWillRpc>(session, req, resp);
    checkStatus(HERE);
}

/**
 * Request that the coordinator send a complete server list to the
 * given server.
 *
 * \param destination
 *      ServerId of the server the coordinator should send the list
 *      to.
 */
void
CoordinatorClient::sendServerList(ServerId destination)
{
    Buffer req, resp;
    Transport::SessionRef session = context.coordinatorSession->getSession();
    SendServerListRpc::Request& reqHdr(
        allocHeader<SendServerListRpc>(req));
    reqHdr.serverId = *destination;
    sendRecv<SendServerListRpc>(session, req, resp);
    checkStatus(HERE);
}

CoordinatorClient::SetMinOpenSegmentId::SetMinOpenSegmentId(
        CoordinatorClient& client,
        ServerId serverId,
        uint64_t segmentId)
    : client(client)
    , requestBuffer()
    , responseBuffer()
    , state()
{
    Transport::SessionRef session =
            client.context.coordinatorSession->getSession();
    auto& reqHdr =
        client.allocHeader<SetMinOpenSegmentIdRpc>(requestBuffer);
    reqHdr.serverId = serverId.getId();
    reqHdr.segmentId = segmentId;
    state = client.send<SetMinOpenSegmentIdRpc>(session,
                                                requestBuffer,
                                                responseBuffer);
}

void
CoordinatorClient::SetMinOpenSegmentId::operator()()
{
    client.recv<SetMinOpenSegmentIdRpc>(state);
    client.checkStatus(HERE);
}

} // namespace RAMCloud
