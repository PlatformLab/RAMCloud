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

#include <boost/scoped_ptr.hpp>

#include "BackupClient.h"
#include "CoordinatorServer.h"
#include "MasterClient.h"
#include "ProtoBuf.h"
#include "Recovery.h"

namespace RAMCloud {

CoordinatorServer::CoordinatorServer()
    : nextServerId(1)
    , backupList()
    , masterList()
    , tabletMap()
    , tables()
    , nextTableId(0)
    , nextTableMasterIdx(0)
    , mockRecovery(NULL)
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
                        &CoordinatorServer::enlistServer>(rpc);
            break;
        case GetServerListRpc::type:
            callHandler<GetServerListRpc, CoordinatorServer,
                        &CoordinatorServer::getServerList>(rpc);
            break;
        case GetTabletMapRpc::type:
            callHandler<GetTabletMapRpc, CoordinatorServer,
                        &CoordinatorServer::getTabletMap>(rpc);
            break;
        case HintServerDownRpc::type:
            callHandler<HintServerDownRpc, CoordinatorServer,
                        &CoordinatorServer::hintServerDown>(rpc, responder);
            break;
        case TabletsRecoveredRpc::type:
            callHandler<TabletsRecoveredRpc, CoordinatorServer,
                        &CoordinatorServer::tabletsRecovered>(rpc);
            break;
        case PingRpc::type:
            callHandler<PingRpc, CoordinatorServer,
                        &CoordinatorServer::ping>(rpc);
            break;
        case SetWillRpc::type:
            callHandler<SetWillRpc, CoordinatorServer,
                        &CoordinatorServer::setWill>(rpc);
            break;
        default:
            throw UnimplementedRequestError(HERE);
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
    if (masterList.server_size() == 0)
        throw RetryException(HERE);

    const char* name = getString(rpc.recvPayload, sizeof(reqHdr),
                                 reqHdr.nameLength);
    if (tables.find(name) != tables.end())
        return;
    uint32_t tableId = nextTableId++;
    tables[name] = tableId;

    uint32_t masterIdx = nextTableMasterIdx++ % masterList.server_size();
    ProtoBuf::ServerList_Entry& master(*masterList.mutable_server(masterIdx));

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
        maxPartitionId = -1;
    willEntry.set_user_data(maxPartitionId + 1);

    // Inform the master.
    MasterClient masterClient(
        transportManager.getSession(master.service_locator().c_str()));
    ProtoBuf::Tablets masterTabletMap;
    foreach (const ProtoBuf::Tablets::Tablet& tablet, tabletMap.tablet()) {
        if (tablet.server_id() == master.server_id())
            *masterTabletMap.add_tablet() = tablet;
    }
    masterClient.setTablets(masterTabletMap);

    LOG(NOTICE, "Created table '%s' with id %u on master %lu",
                name, tableId, master.server_id());
    LOG(DEBUG, "There are now %d tablets in the map", tabletMap.tablet_size());
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
    const string& locator(masterList.server(0).service_locator());
    MasterClient master(transportManager.getSession(locator.c_str()));
    master.setTablets(tabletMap);

    LOG(NOTICE, "Dropped table '%s' with id %u", name, tableId);
    LOG(DEBUG, "There are now %d tablets in the map", tabletMap.tablet_size());
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
        throw TableDoesntExistException(HERE);
    respHdr.tableId = it->second;
}

/**
 * Handle the ENLIST_SERVER RPC.
 * \copydetails Server::ping
 */
void
CoordinatorServer::enlistServer(const EnlistServerRpc::Request& reqHdr,
                                EnlistServerRpc::Response& respHdr,
                                Transport::ServerRpc& rpc)
{
    uint64_t serverId = nextServerId++;
    ProtoBuf::ServerType serverType =
        static_cast<ProtoBuf::ServerType>(reqHdr.serverType);
    const char *serviceLocator = getString(rpc.recvPayload, sizeof(reqHdr),
                                           reqHdr.serviceLocatorLength);

    ProtoBuf::ServerList& serverList(serverType == ProtoBuf::MASTER
                                     ? masterList
                                     : backupList);
    ProtoBuf::ServerList_Entry& server(*serverList.add_server());
    server.set_server_type(serverType);
    server.set_server_id(serverId);
    server.set_service_locator(serviceLocator);

    if (server.server_type() == ProtoBuf::MASTER) {
        // create empty will
        server.set_user_data(
            reinterpret_cast<uint64_t>(new ProtoBuf::Tablets));
        LOG(DEBUG, "Master enlisted with id %lu, sl [%s]", serverId,
            serviceLocator);
    } else {
        LOG(DEBUG, "Backup enlisted with id %lu, sl [%s]", serverId,
            serviceLocator);
    }
    respHdr.serverId = serverId;
}

/**
 * Handle the GET_SERVER_LIST RPC.
 * \copydetails Server::ping
 */
void
CoordinatorServer::getServerList(const GetServerListRpc::Request& reqHdr,
                                 GetServerListRpc::Response& respHdr,
                                 Transport::ServerRpc& rpc)
{
    switch (reqHdr.serverType) {
    case MASTER:
        respHdr.serverListLength = serializeToResponse(rpc.replyPayload,
                                                       masterList);
        break;

    case BACKUP:
        respHdr.serverListLength = serializeToResponse(rpc.replyPayload,
                                                       backupList);
        break;

    default:
        throw RequestFormatError(HERE);
    }
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

/**
 * Handle the ENLIST_SERVER RPC.
 * \copydetails Server::ping
 * \param responder
 *      Functor to respond to the RPC before returning from this method. Used
 *      to avoid deadlock between first master and coordinator.
 */
void
CoordinatorServer::hintServerDown(const HintServerDownRpc::Request& reqHdr,
                                  HintServerDownRpc::Response& respHdr,
                                  Transport::ServerRpc& rpc,
                                  Responder& responder)
{
    string serviceLocator(getString(rpc.recvPayload, sizeof(reqHdr),
                                    reqHdr.serviceLocatorLength));
    responder();

    // reqHdr, respHdr, and rpc are off-limits now

    LOG(DEBUG, "Hint server down: %s", serviceLocator.c_str());

    // is it a master?
    for (int32_t i = 0; i < masterList.server_size(); i++) {
        const ProtoBuf::ServerList::Entry& master(masterList.server(i));
        if (master.service_locator() == serviceLocator) {
            uint64_t serverId = master.server_id();
            boost::scoped_ptr<ProtoBuf::Tablets> will(
                reinterpret_cast<ProtoBuf::Tablets*>(master.user_data()));

            masterList.mutable_server()->SwapElements(
                                            masterList.server_size() - 1, i);
            masterList.mutable_server()->RemoveLast();

            // master is off-limits now

            foreach (ProtoBuf::Tablets::Tablet& tablet,
                     *tabletMap.mutable_tablet()) {
                if (tablet.server_id() == serverId)
                    tablet.set_state(ProtoBuf::Tablets_Tablet::RECOVERING);
            }

            LOG(DEBUG, "Trying partition recovery on %lu with %u masters "
                "and %u backups", serverId, masterList.server_size(),
                backupList.server_size());

            BaseRecovery* recovery = NULL;
            if (mockRecovery != NULL) {
                (*mockRecovery)(serverId, *will, masterList, backupList);
                recovery = mockRecovery;
            } else {
                recovery = new Recovery(serverId, *will,
                                        masterList, backupList);
            }

            // Keep track of recovery for each of the tablets its working on
            foreach (ProtoBuf::Tablets::Tablet& tablet,
                     *tabletMap.mutable_tablet()) {
                if (tablet.server_id() == serverId)
                    tablet.set_user_data(reinterpret_cast<uint64_t>(recovery));
            }

            recovery->start();

            return;
        }
    }

    // is it a backup?
    for (int32_t i = 0; i < backupList.server_size(); i++) {
        const ProtoBuf::ServerList::Entry& backup(backupList.server(i));
        if (backup.service_locator() == serviceLocator) {
            backupList.mutable_server()->SwapElements(
                                            backupList.server_size() - 1, i);
            backupList.mutable_server()->RemoveLast();

            // backup is off-limits now

            // TODO(ongaro): inform masters they need to replicate more
            return;
        }
    }
}

/**
 * Handle the TABLETS_RECOVERED RPC.
 * \copydetails Server::ping
 */
void
CoordinatorServer::tabletsRecovered(const TabletsRecoveredRpc::Request& reqHdr,
                                    TabletsRecoveredRpc::Response& respHdr,
                                    Transport::ServerRpc& rpc)
{
    if (reqHdr.status != STATUS_OK) {
        // we'll need to restart a recovery of that partition elsewhere
        // right now this just leaks the recovery object in the tabletMap
        LOG(ERROR, "A recovery master failed to recover its partition");
    }

    ProtoBuf::Tablets recoveredTablets;
    ProtoBuf::parseFromResponse(rpc.recvPayload, sizeof(reqHdr),
                                reqHdr.tabletsLength, recoveredTablets);
    ProtoBuf::Tablets* newWill = new ProtoBuf::Tablets;
    ProtoBuf::parseFromResponse(rpc.recvPayload,
                                sizeof(reqHdr) + reqHdr.tabletsLength,
                                reqHdr.willLength, *newWill);

    LOG(NOTICE, "called by masterId %lu with %u tablets, %u will entries",
        reqHdr.masterId, recoveredTablets.tablet_size(),
        newWill->tablet_size());

    // update the will
    setWill(reqHdr.masterId, rpc.recvPayload,
        sizeof(reqHdr) + reqHdr.tabletsLength, reqHdr.willLength);

    // update tablet map to point to new owner and mark as available
    foreach (const ProtoBuf::Tablets::Tablet& recoveredTablet,
             recoveredTablets.tablet())
    {
        foreach (ProtoBuf::Tablets::Tablet& tablet,
                 *tabletMap.mutable_tablet())
        {
            if (recoveredTablet.table_id() == tablet.table_id() &&
                recoveredTablet.start_object_id() == tablet.start_object_id() &&
                recoveredTablet.end_object_id() == tablet.end_object_id())
            {
                LOG(NOTICE, "Recovery complete on tablet %lu,%lu,%lu",
                    tablet.table_id(), tablet.start_object_id(),
                    tablet.end_object_id());
                BaseRecovery* recovery =
                    reinterpret_cast<Recovery*>(tablet.user_data());
                tablet.set_state(ProtoBuf::Tablets_Tablet::NORMAL);
                tablet.set_user_data(0);
                // The caller has filled in recoveredTablets with new service
                // locator and server id of the recovery master,
                // so just copy it over.
                tablet.set_service_locator(recoveredTablet.service_locator());
                tablet.set_server_id(recoveredTablet.server_id());
                bool recoveryComplete =
                    recovery->tabletsRecovered(recoveredTablets);
                if (recoveryComplete) {
                    LOG(NOTICE, "Recovery completed");
                    delete recovery;
                    // dump the tabletMap out for easy debugging
                    LOG(NOTICE, "Coordinator tabletMap:");
#ifndef __INTEL_COMPILER
                    foreach (const ProtoBuf::Tablets::Tablet& tablet,
                             tabletMap.tablet()) {
                        LOG(NOTICE, "table: %lu [%lu:%lu] state: %u owner: %lu",
                            tablet.table_id(), tablet.start_object_id(),
                            tablet.end_object_id(), tablet.state(),
                            tablet.server_id());
                    }
#endif
                    return;
                }
            }
        }
    }
}

/**
 * Top-level server method to handle the PING request.
 *
 * For debugging it print out statistics on the RPCs that it has
 * handled and instructs all the machines in the RAMCloud to do
 * so also (by pinging them all).
 *
 * \copydetails Server::ping
 */
void
CoordinatorServer::ping(const PingRpc::Request& reqHdr,
                        PingRpc::Response& respHdr,
                        Transport::ServerRpc& rpc)
{
    // dump out all the RPC stats for all the hosts so far
    foreach (const ProtoBuf::ServerList::Entry& server,
             backupList.server())
        BackupClient(transportManager.getSession(
            server.service_locator().c_str())).ping();
    foreach (const ProtoBuf::ServerList::Entry& server,
             masterList.server())
        MasterClient(transportManager.getSession(
            server.service_locator().c_str())).ping();

    Server::ping(reqHdr, respHdr, rpc);
}

/**
 * Update the Will associated with a specific Master. This is used
 * by Masters to keep their partitions balanced for efficient
 * recovery.
 *
 * \copydetails Server::ping
 */
void
CoordinatorServer::setWill(const SetWillRpc::Request& reqHdr,
                           SetWillRpc::Response& respHdr,
                           Transport::ServerRpc& rpc)
{
    if (!setWill(reqHdr.masterId, rpc.recvPayload, sizeof(reqHdr),
        reqHdr.willLength)) {
        respHdr.common.status = Status(-1);
    }
}

bool
CoordinatorServer::setWill(uint64_t masterId, Buffer& buffer,
    uint32_t offset, uint32_t length)
{
    foreach (auto& master, *masterList.mutable_server()) {
        if (master.server_id() == masterId) {
            ProtoBuf::Tablets* oldWill =
                reinterpret_cast<ProtoBuf::Tablets*>(master.user_data());

            ProtoBuf::Tablets* newWill = new ProtoBuf::Tablets();
            ProtoBuf::parseFromResponse(buffer, offset, length, *newWill);
            master.set_user_data(reinterpret_cast<uint64_t>(newWill));

            LOG(NOTICE, "Master %lu updated its Will (now %d entries, was %d)",
                masterId, newWill->tablet_size(), oldWill->tablet_size());

            delete oldWill;
            return true;
        }
    }

    LOG(WARNING, "Master %lu could not be found!!", masterId);
    return false;
}

} // namespace RAMCloud
