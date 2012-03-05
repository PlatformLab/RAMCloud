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
#include "ShortMacros.h"
#include "ProtoBuf.h"

namespace RAMCloud {

/**
 * Create a new table.
 *
 * \param name
 *      Name for the new table (NULL-terminated string).
 *
 * \exception InternalError
 */
void
CoordinatorClient::createTable(const char* name)
{
    Buffer req;
    uint32_t length = downCast<uint32_t>(strlen(name) + 1);
    CreateTableRpc::Request& reqHdr(allocHeader<CreateTableRpc>(req));
    reqHdr.nameLength = length;
    memcpy(new(&req, APPEND) char[length], name, length);
    while (true) {
        Buffer resp;
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
    sendRecv<DropTableRpc>(session, req, resp);
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
uint32_t
CoordinatorClient::openTable(const char* name)
{
    Buffer req, resp;
    uint32_t length = downCast<uint32_t>(strlen(name) + 1);
    OpenTableRpc::Request& reqHdr(allocHeader<OpenTableRpc>(req));
    reqHdr.nameLength = length;
    memcpy(new(&req, APPEND) char[length], name, length);
    const OpenTableRpc::Response& respHdr(
        sendRecv<OpenTableRpc>(session, req, resp));
    checkStatus(HERE);
    return respHdr.tableId;
}

/**
 * Servers call this when they come online to beg for work.
 * \param replacesId
 *      Server id the calling server used to operate at; the coordinator must
 *      make sure this server is removed from the cluster before enlisting
 *      the calling server.  If !isValid() then this step is skipped; the
 *      enlisting server is simply added.
 * \param serviceMask
 *      Which services are available on the enlisting server. MASTER_SERVICE,
 *      BACKUP_SERVICE, etc.
 * \param localServiceLocator
 *      The service locator describing how other hosts can contact the server.
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
CoordinatorClient::enlistServer(ServerId replacesId,
                                ServiceMask serviceMask,
                                string localServiceLocator,
                                uint32_t readSpeed,
                                uint32_t writeSpeed)
{
    while (true) {
        try {
            Buffer req;
            Buffer resp;
            EnlistServerRpc::Request& reqHdr(
                allocHeader<EnlistServerRpc>(req));
            reqHdr.replacesId = replacesId.getId();
            reqHdr.serviceMask = serviceMask.serialize();
            reqHdr.readSpeed = readSpeed;
            reqHdr.writeSpeed = writeSpeed;
            reqHdr.serviceLocatorLength =
                downCast<uint32_t>(localServiceLocator.length() + 1);
            strncpy(new(&req, APPEND) char[reqHdr.serviceLocatorLength],
                    localServiceLocator.c_str(),
                    reqHdr.serviceLocatorLength);
            const EnlistServerRpc::Response& respHdr(
                sendRecv<EnlistServerRpc>(session, req, resp));
            checkStatus(HERE);
            return ServerId(respHdr.serverId);
        } catch (TransportException& e) {
            LOG(NOTICE,
                "TransportException trying to talk to coordinator: %s",
                e.str().c_str());
            LOG(NOTICE, "retrying");
            usleep(500);
        }
    }
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
 * Tell the coordinator that recovery of a particular tablets have
 * been recovered on the master who is calling.
 *
 * \param[in] masterId
 *      The masterId of the server invoking this method.
 * \param[in] tablets
 *      The tablets which form a partition of a will which are
 *      now done recovering.
 */
void
CoordinatorClient::tabletsRecovered(ServerId masterId,
                                    const ProtoBuf::Tablets& tablets)
{
    Buffer req, resp;
    TabletsRecoveredRpc::Request& reqHdr(allocHeader<TabletsRecoveredRpc>(req));
    reqHdr.masterId = *masterId;
    reqHdr.tabletsLength = serializeToRequest(req, tablets);
    sendRecv<TabletsRecoveredRpc>(session, req, resp);
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
CoordinatorClient::requestServerList(ServerId destination)
{
    Buffer req, resp;
    RequestServerListRpc::Request& reqHdr(
        allocHeader<RequestServerListRpc>(req));
    reqHdr.serverId = *destination;
    sendRecv<RequestServerListRpc>(session, req, resp);
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
    auto& reqHdr =
        client.allocHeader<SetMinOpenSegmentIdRpc>(requestBuffer);
    reqHdr.serverId = serverId.getId();
    reqHdr.segmentId = segmentId;
    state = client.send<SetMinOpenSegmentIdRpc>(client.session,
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
