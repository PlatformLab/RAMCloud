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

#include "CoordinatorServer.h"
#include "MasterClient.h"
#include "ProtoBuf.h"

namespace RAMCloud {

CoordinatorServer::CoordinatorServer()
    : nextServerId(generateRandom())
    , backupList()
    , masterList()
    , firstMaster(NULL)
    , tabletMap()
    , tables()
    , nextTableId(0)
{
}

CoordinatorServer::~CoordinatorServer()
{
    // delete wills
    foreach (const ProtoBuf::ServerList::Entry& master, masterList.server())
        delete reinterpret_cast<ProtoBuf::Tablets*>(master.user_data());
}

void
CoordinatorServer::run()
{
    while (true)
        handleRpc<CoordinatorServer>();
}

void
CoordinatorServer::dispatch(RpcType type,
                            Transport::ServerRpc& rpc,
                            Responder& responder)
{
    switch (type) {
        case CreateTableRpc::type:
            callHandler<CreateTableRpc, CoordinatorServer,
                        &CoordinatorServer::createTable>(rpc);
            break;
        case DropTableRpc::type:
            callHandler<DropTableRpc, CoordinatorServer,
                        &CoordinatorServer::dropTable>(rpc);
            break;
        case OpenTableRpc::type:
            callHandler<OpenTableRpc, CoordinatorServer,
                        &CoordinatorServer::openTable>(rpc);
            break;
        case EnlistServerRpc::type:
            callHandler<EnlistServerRpc, CoordinatorServer,
                        &CoordinatorServer::enlistServer>(rpc, responder);
            break;
        case GetBackupListRpc::type:
            callHandler<GetBackupListRpc, CoordinatorServer,
                        &CoordinatorServer::getBackupList>(rpc);
            break;
        case GetTabletMapRpc::type:
            callHandler<GetTabletMapRpc, CoordinatorServer,
                        &CoordinatorServer::getTabletMap>(rpc);
            break;
        case PingRpc::type:
            callHandler<PingRpc, Server, &Server::ping>(rpc);
            break;
        default:
            throw UnimplementedRequestError();
    }
}

/**
 * Top-level server method to handle the CREATE_TABLE request.
 * \copydetails Server::ping
 */
void
CoordinatorServer::createTable(const CreateTableRpc::Request& reqHdr,
                               CreateTableRpc::Response& respHdr,
                               Transport::ServerRpc& rpc)
{
    const char* name = getString(rpc.recvPayload, sizeof(reqHdr),
                                 reqHdr.nameLength);
    if (tables.find(name) != tables.end())
        return;
    uint32_t tableId = nextTableId++;
    tables[name] = tableId;
    if (tableId != 0) // check is temporary, see RAM-139
        createTable(tableId, *firstMaster); // race, see RAM-138
}

/**
 * Common code used by #createTable() above and temporarily by #enlistServer().
 */
void
CoordinatorServer::createTable(uint32_t tableId,
                               ProtoBuf::ServerList_Entry& master) {
    // Create tablet map entry.
    ProtoBuf::Tablets_Tablet& tablet(*tabletMap.add_tablet());
    tablet.set_table_id(tableId);
    tablet.set_start_object_id(0);
    tablet.set_end_object_id(~0UL);
    tablet.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
    tablet.set_server_id(master.server_id());
    tablet.set_service_locator(master.service_locator());

    // Create will entry. The tablet is empty, so it doesn't matter where it
    // goes or in how many partitions, initially. It just has to go somewhere.
    ProtoBuf::Tablets& will(
        *reinterpret_cast<ProtoBuf::Tablets*>(master.user_data()));
    ProtoBuf::Tablets_Tablet& willEntry(*will.add_tablet());
    willEntry.set_table_id(tableId);
    willEntry.set_start_object_id(0);
    willEntry.set_end_object_id(~0UL);
    willEntry.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
    uint64_t maxPartitionId;
    if (will.tablet_size() > 1)
        maxPartitionId = will.tablet(will.tablet_size() - 2).user_data();
    else
        maxPartitionId = 0;
    willEntry.set_user_data(maxPartitionId);

    // Inform the master.
    MasterClient masterClient(
        transportManager.getSession(master.service_locator().c_str()));
    // TODO(ongaro): filter tabletMap for those tablets belonging to master
    // before sending
    masterClient.setTablets(tabletMap);
}

/**
 * Top-level server method to handle the DROP_TABLE request.
 * \copydetails Server::ping
 */
void
CoordinatorServer::dropTable(const DropTableRpc::Request& reqHdr,
                             DropTableRpc::Response& respHdr,
                             Transport::ServerRpc& rpc)
{
    const char* name = getString(rpc.recvPayload, sizeof(reqHdr),
                                 reqHdr.nameLength);
    Tables::iterator it = tables.find(name);
    if (it == tables.end())
        return;
    uint32_t tableId = it->second;
    tables.erase(it);
    int32_t i = 0;
    while (i < tabletMap.tablet_size()) {
        if (tabletMap.tablet(i).table_id() == tableId) {
            tabletMap.mutable_tablet()->SwapElements(
                                            tabletMap.tablet_size() - 1, i);
            tabletMap.mutable_tablet()->RemoveLast();
        } else {
            ++i;
        }
    }
    // TODO(ongaro): update only affected masters, filter tabletMap for those
    // tablets belonging to each master
    MasterClient master(
        transportManager.getSession(firstMaster->service_locator().c_str()));
    master.setTablets(tabletMap);
}

/**
 * Top-level server method to handle the OPEN_TABLE request.
 * \copydetails Server::ping
 */
void
CoordinatorServer::openTable(const OpenTableRpc::Request& reqHdr,
                             OpenTableRpc::Response& respHdr,
                             Transport::ServerRpc& rpc)
{
    const char* name = getString(rpc.recvPayload, sizeof(reqHdr),
                                 reqHdr.nameLength);
    Tables::iterator it(tables.find(name));
    if (it == tables.end())
        throw TableDoesntExistException();
    respHdr.tableId = it->second;
}

/**
 * Handle the ENLIST_SERVER RPC.
 * \copydetails Server::ping
 * \param responder
 *      Functor to respond to the RPC before returning from this method. Used
 *      to avoid deadlock between first master and coordinator.
 */
void
CoordinatorServer::enlistServer(const EnlistServerRpc::Request& reqHdr,
                                EnlistServerRpc::Response& respHdr,
                                Transport::ServerRpc& rpc,
                                Responder& responder)
{
    uint64_t serverId = nextServerId++;
    ProtoBuf::ServerType serverType =
        static_cast<ProtoBuf::ServerType>(reqHdr.serverType);
    ProtoBuf::ServerList& serverList(serverType == ProtoBuf::MASTER
                                     ? masterList
                                     : backupList);
    ProtoBuf::ServerList_Entry& server(*serverList.add_server());
    server.set_server_type(serverType);
    server.set_server_id(serverId);
    server.set_service_locator(getString(rpc.recvPayload, sizeof(reqHdr),
                                         reqHdr.serviceLocatorLength));
    if (server.server_type() == ProtoBuf::MASTER) {
        // create empty will
        server.set_user_data(
            reinterpret_cast<uint64_t>(new ProtoBuf::Tablets));
    }
    LOG(DEBUG, "Server enlisted with id %lu", serverId);
    respHdr.serverId = serverId;
    responder();

    // reqHdr, respHdr, and rpc are off-limits now

    if (firstMaster == NULL && server.server_type() == ProtoBuf::MASTER) {
        firstMaster = &server;
        // first master gets table 0
        // for backwards compatibility, the first table to be explicitly
        // created should get id 0 as well
        createTable(nextTableId, server);
    }
}

/**
 * Handle the GET_BACKUP_LIST RPC.
 * \copydetails Server::ping
 */
void
CoordinatorServer::getBackupList(const GetBackupListRpc::Request& reqHdr,
                                 GetBackupListRpc::Response& respHdr,
                                 Transport::ServerRpc& rpc)
{
    respHdr.serverListLength = serializeToResponse(rpc.replyPayload,
                                                   backupList);
}

/**
 * Handle the GET_TABLET_MAP RPC.
 * \copydetails Server::ping
 */
void
CoordinatorServer::getTabletMap(const GetTabletMapRpc::Request& reqHdr,
                                GetTabletMapRpc::Response& respHdr,
                                Transport::ServerRpc& rpc)
{
    respHdr.tabletMapLength = serializeToResponse(rpc.replyPayload,
                                                  tabletMap);
}

} // namespace RAMCloud
