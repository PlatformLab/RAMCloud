/* Copyright (c) 2009-2011 Stanford University
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

#include <memory>

#include "BackupClient.h"
#include "CoordinatorService.h"
#include "MasterClient.h"
#include "MembershipClient.h"
#include "PingClient.h"
#include "ProtoBuf.h"
#include "Recovery.h"
#include "ShortMacros.h"
#include "ServiceMask.h"

namespace RAMCloud {

CoordinatorService::CoordinatorService()
    : serverList()
    , tabletMap()
    , tables()
    , nextTableId(0)
    , nextTableMasterIdx(0)
    , mockRecovery(NULL)
    , forceServerDownForTesting(false)
{
}

CoordinatorService::~CoordinatorService()
{
    // Delete wills. They aren't automatically deleted in the
    // CoordinatorServerList::Entry destructor because we may
    // want to stash a copy of the entry and then remove from
    // the list before using it (e.g. when we start recovery).
    for (size_t i = 0; i < serverList.size(); i++) {
        CoordinatorServerList::Entry* entry = serverList[i];
        if (entry != NULL && entry->isMaster() && entry->will != NULL)
            delete entry->will;
    }
}

void
CoordinatorService::dispatch(RpcOpcode opcode,
                             Rpc& rpc)
{
    switch (opcode) {
        case CreateTableRpc::opcode:
            callHandler<CreateTableRpc, CoordinatorService,
                        &CoordinatorService::createTable>(rpc);
            break;
        case DropTableRpc::opcode:
            callHandler<DropTableRpc, CoordinatorService,
                        &CoordinatorService::dropTable>(rpc);
            break;
        case OpenTableRpc::opcode:
            callHandler<OpenTableRpc, CoordinatorService,
                        &CoordinatorService::openTable>(rpc);
            break;
        case EnlistServerRpc::opcode:
            callHandler<EnlistServerRpc, CoordinatorService,
                        &CoordinatorService::enlistServer>(rpc);
            break;
        case GetServerListRpc::opcode:
            callHandler<GetServerListRpc, CoordinatorService,
                        &CoordinatorService::getServerList>(rpc);
            break;
        case GetTabletMapRpc::opcode:
            callHandler<GetTabletMapRpc, CoordinatorService,
                        &CoordinatorService::getTabletMap>(rpc);
            break;
        case HintServerDownRpc::opcode:
            callHandler<HintServerDownRpc, CoordinatorService,
                        &CoordinatorService::hintServerDown>(rpc);
            break;
        case TabletsRecoveredRpc::opcode:
            callHandler<TabletsRecoveredRpc, CoordinatorService,
                        &CoordinatorService::tabletsRecovered>(rpc);
            break;
        case BackupQuiesceRpc::opcode:
            callHandler<BackupQuiesceRpc, CoordinatorService,
                        &CoordinatorService::quiesce>(rpc);
            break;
        case SetWillRpc::opcode:
            callHandler<SetWillRpc, CoordinatorService,
                        &CoordinatorService::setWill>(rpc);
            break;
        case RequestServerListRpc::opcode:
            callHandler<RequestServerListRpc, CoordinatorService,
                        &CoordinatorService::requestServerList>(rpc);
            break;
        case SetMinOpenSegmentIdRpc::opcode:
            callHandler<SetMinOpenSegmentIdRpc, CoordinatorService,
                        &CoordinatorService::setMinOpenSegmentId>(rpc);
            break;
        default:
            throw UnimplementedRequestError(HERE);
    }
}

/**
 * Top-level server method to handle the CREATE_TABLE request.
 * \copydetails Service::ping
 */
void
CoordinatorService::createTable(const CreateTableRpc::Request& reqHdr,
                                CreateTableRpc::Response& respHdr,
                                Rpc& rpc)
{
    if (serverList.masterCount() == 0) {
        respHdr.common.status = STATUS_RETRY;
        return;
    }

    const char* name = getString(rpc.requestPayload, sizeof(reqHdr),
                                 reqHdr.nameLength);
    if (tables.find(name) != tables.end())
        return;
    uint32_t tableId = nextTableId++;
    tables[name] = tableId;

    // Find the next master in the list.
    CoordinatorServerList::Entry* master = NULL;
    while (true) {
        size_t masterIdx = nextTableMasterIdx++ % serverList.size();
        if (serverList[masterIdx] != NULL &&
            serverList[masterIdx]->isMaster()) {
            master = serverList[masterIdx];
            break;
        }
    }

    // Create tablet map entry.
    ProtoBuf::Tablets_Tablet& tablet(*tabletMap.add_tablet());
    tablet.set_table_id(tableId);
    tablet.set_start_object_id(0);
    tablet.set_end_object_id(~0UL);
    tablet.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
    tablet.set_server_id(master->serverId.getId());
    tablet.set_service_locator(master->serviceLocator);

    // Create will entry. The tablet is empty, so it doesn't matter where it
    // goes or in how many partitions, initially. It just has to go somewhere.
    ProtoBuf::Tablets& will = *master->will;
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
    const char* locator = master->serviceLocator.c_str();
    MasterClient masterClient(
        Context::get().transportManager->getSession(locator));
    ProtoBuf::Tablets masterTabletMap;
    foreach (const ProtoBuf::Tablets::Tablet& tablet, tabletMap.tablet()) {
        if (tablet.server_id() == master->serverId.getId())
            *masterTabletMap.add_tablet() = tablet;
    }
    masterClient.setTablets(masterTabletMap);

    LOG(NOTICE, "Created table '%s' with id %u on master %lu",
                name, tableId, master->serverId.getId());
    LOG(DEBUG, "There are now %d tablets in the map", tabletMap.tablet_size());
}

/**
 * Top-level server method to handle the DROP_TABLE request.
 * \copydetails Service::ping
 */
void
CoordinatorService::dropTable(const DropTableRpc::Request& reqHdr,
                              DropTableRpc::Response& respHdr,
                              Rpc& rpc)
{
    const char* name = getString(rpc.requestPayload, sizeof(reqHdr),
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

    for (size_t i = 0; i < serverList.size(); i++) {
        if (serverList[i] == NULL || !serverList[i]->isMaster())
            continue;
        const char* locator = serverList[i]->serviceLocator.c_str();
        MasterClient master(
            Context::get().transportManager->getSession(locator));
        master.setTablets(tabletMap);
    }

    LOG(NOTICE, "Dropped table '%s' with id %u", name, tableId);
    LOG(DEBUG, "There are now %d tablets in the map", tabletMap.tablet_size());
}

/**
 * Top-level server method to handle the OPEN_TABLE request.
 * \copydetails Service::ping
 */
void
CoordinatorService::openTable(const OpenTableRpc::Request& reqHdr,
                              OpenTableRpc::Response& respHdr,
                              Rpc& rpc)
{
    const char* name = getString(rpc.requestPayload, sizeof(reqHdr),
                                 reqHdr.nameLength);
    Tables::iterator it(tables.find(name));
    if (it == tables.end()) {
        respHdr.common.status = STATUS_TABLE_DOESNT_EXIST;
        return;
    }
    respHdr.tableId = it->second;
}

/**
 * Handle the ENLIST_SERVER RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::enlistServer(const EnlistServerRpc::Request& reqHdr,
                                 EnlistServerRpc::Response& respHdr,
                                 Rpc& rpc)
{
    ServerId replacesId = ServerId(reqHdr.replacesId);
    ServiceMask serviceMask = ServiceMask::deserialize(reqHdr.serviceMask);
    const uint32_t readSpeed = reqHdr.readSpeed;
    const uint32_t writeSpeed = reqHdr.writeSpeed;
    const char* serviceLocator = getString(rpc.requestPayload, sizeof(reqHdr),
                                           reqHdr.serviceLocatorLength);

    // Keep track of the details of the server id that is being forced out
    // of the cluster by the enlister so we can start recovery.
    Tub<CoordinatorServerList::Entry> replacedEntry;

    // The order of the updates in serverListUpdate is important: the remove
    // must be ordered before the add to ensure that as members apply the
    // update they will see the removal of the old server id before the
    // addition of the new, replacing server id.
    ProtoBuf::ServerList serverListUpdate;
    if (serverList.contains(replacesId)) {
        LOG(NOTICE, "%s is enlisting claiming to replace server id "
            "%lu, which is still in the server list, taking its word "
            "for it and assuming the old server has failed",
            serviceLocator, replacesId.getId());
        replacedEntry.construct(serverList[replacesId]);
        // Don't increment server list yet; done after the add below.
        // Note, if the server being replaced is already crashed this may
        // not append an update at all.
        serverList.crashed(replacesId, serverListUpdate);
        // If the server being replaced did not have a master then there
        // will be no recovery.  That means it needs to transition to
        // removed status now (usually recoveries remove servers from the
        // list when they complete).
        if (!replacedEntry->isMaster())
            serverList.remove(replacesId, serverListUpdate);
    }

    ServerId newServerId = serverList.add(serviceLocator,
                                          serviceMask,
                                          readSpeed,
                                          serverListUpdate);
    serverList.incrementVersion(serverListUpdate);
    CoordinatorServerList::Entry& entry = serverList[newServerId];

    LOG(NOTICE, "Enlisting new server at %s (server id %lu) supporting "
        "services: %s",
        serviceLocator, newServerId.getId(),
        entry.serviceMask.toString().c_str());

    if (entry.isMaster()) {
        // create empty will
        entry.will = new ProtoBuf::Tablets;
    }

    if (entry.isBackup()) {
        LOG(DEBUG, "Backup at id %lu has %u MB/s read %u MB/s write ",
            newServerId.getId(), readSpeed, writeSpeed);
    }

    respHdr.serverId = newServerId.getId();
    rpc.sendReply();

    if (entry.serviceMask.has(MEMBERSHIP_SERVICE))
        sendServerList(newServerId);
    sendMembershipUpdate(serverListUpdate, newServerId);

    // Recovery on the replaced host is deferred until the replacing host
    // has been enlisted.
    if (replacedEntry)
        startMasterRecovery(*replacedEntry);
}

/**
 * Handle the GET_SERVER_LIST RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::getServerList(const GetServerListRpc::Request& reqHdr,
                                  GetServerListRpc::Response& respHdr,
                                  Rpc& rpc)
{
    ServiceMask serviceMask = ServiceMask::deserialize(reqHdr.serviceMask);

    ProtoBuf::ServerList serialServerList;
    serverList.serialize(serialServerList, serviceMask);

    respHdr.serverListLength =
        serializeToResponse(rpc.replyPayload, serialServerList);
}

/**
 * Handle the GET_TABLET_MAP RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::getTabletMap(const GetTabletMapRpc::Request& reqHdr,
                                 GetTabletMapRpc::Response& respHdr,
                                 Rpc& rpc)
{
    respHdr.tabletMapLength = serializeToResponse(rpc.replyPayload,
                                                  tabletMap);
}

/**
 * Handle the ENLIST_SERVER RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::hintServerDown(const HintServerDownRpc::Request& reqHdr,
                                   HintServerDownRpc::Response& respHdr,
                                   Rpc& rpc)
{
    ServerId serverId(reqHdr.serverId);
    rpc.sendReply();

    // reqHdr, respHdr, and rpc are off-limits now

    if (!serverList.contains(serverId) ||
        serverList[serverId.indexNumber()]->status != ServerStatus::UP) {
        LOG(NOTICE, "Spurious crash report on unknown server id %lu",
            serverId.getId());
        return;
    }

    if (!verifyServerFailure(serverId))
        return;

    LOG(NOTICE, "Server id %lu has crashed, notifying the cluster and "
        "starting recovery", serverId.getId());

    // If this machine has a backup and master on the same server it is best
    // to remove the dead backup before initiating recovery. Otherwise, other
    // servers may try to backup onto a dead machine which will cause delays.
    CoordinatorServerList::Entry entry = serverList[serverId];
    ProtoBuf::ServerList update;
    serverList.crashed(serverId, update);
    // If the server being replaced did not have a master then there
    // will be no recovery.  That means it needs to transition to
    // removed status now (usually recoveries remove servers from the
    // list when they complete).
    if (!entry.isMaster())
        serverList.remove(serverId, update);
    serverList.incrementVersion(update);

    // Update cluster membership information.
    // Backup recovery is kicked off via this update.
    // Deciding whether to place this before or after the start of master
    // recovery is difficult.  With small cluster sizes kicking off backup
    // recoveries this early didn't interfere with master recovery
    // performance, but we'll want to keep an eye on this.
    sendMembershipUpdate(update, ServerId(/* invalid id */));

    startMasterRecovery(entry);
}

/**
 * Handle the TABLETS_RECOVERED RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::tabletsRecovered(const TabletsRecoveredRpc::Request& reqHdr,
                                     TabletsRecoveredRpc::Response& respHdr,
                                     Rpc& rpc)
{
    if (reqHdr.status != STATUS_OK) {
        // we'll need to restart a recovery of that partition elsewhere
        // right now this just leaks the recovery object in the tabletMap
        LOG(ERROR, "A recovery master failed to recover its partition");
    }

    ProtoBuf::Tablets recoveredTablets;
    ProtoBuf::parseFromResponse(rpc.requestPayload,
                                downCast<uint32_t>(sizeof(reqHdr)),
                                reqHdr.tabletsLength, recoveredTablets);
    ProtoBuf::Tablets* newWill = new ProtoBuf::Tablets;
    ProtoBuf::parseFromResponse(rpc.requestPayload,
                                downCast<uint32_t>(sizeof(reqHdr)) +
                                reqHdr.tabletsLength,
                                reqHdr.willLength, *newWill);

    LOG(NOTICE, "called by masterId %lu with %u tablets, %u will entries",
        reqHdr.masterId, recoveredTablets.tablet_size(),
        newWill->tablet_size());

    // update the will
    setWill(ServerId(reqHdr.masterId), rpc.requestPayload,
        downCast<uint32_t>(sizeof(reqHdr)) + reqHdr.tabletsLength,
        reqHdr.willLength);

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
                    LOG(NOTICE, "Recovery completed for master %lu",
                        recovery->masterId.getId());
                    delete recovery;
                    // dump the tabletMap out for easy debugging
                    LOG(DEBUG, "Coordinator tabletMap:");
                    foreach (const ProtoBuf::Tablets::Tablet& tablet,
                             tabletMap.tablet()) {
                        LOG(DEBUG, "table: %lu [%lu:%lu] state: %u owner: %lu",
                            tablet.table_id(), tablet.start_object_id(),
                            tablet.end_object_id(), tablet.state(),
                            tablet.server_id());
                    }
                    ProtoBuf::ServerList update;
                    serverList.remove(recovery->masterId, update);
                    serverList.incrementVersion(update);
                    sendMembershipUpdate(update, ServerId(/* invalid id */));
                    return;
                }
            }
        }
    }
}

/**
 * Have all backups flush their dirty segments to storage.
 * \copydetails Service::ping
 */
void
CoordinatorService::quiesce(const BackupQuiesceRpc::Request& reqHdr,
                            BackupQuiesceRpc::Response& respHdr,
                            Rpc& rpc)
{
    for (size_t i = 0; i < serverList.size(); i++) {
        if (serverList[i] != NULL && serverList[i]->isBackup()) {
            BackupClient(Context::get().transportManager->getSession(
                serverList[i]->serviceLocator.c_str())).quiesce();
        }
    }
}

/**
 * Update the Will associated with a specific Master. This is used
 * by Masters to keep their partitions balanced for efficient
 * recovery.
 *
 * \copydetails Service::ping
 */
void
CoordinatorService::setWill(const SetWillRpc::Request& reqHdr,
                            SetWillRpc::Response& respHdr,
                            Rpc& rpc)
{
    if (!setWill(ServerId(reqHdr.masterId), rpc.requestPayload, sizeof(reqHdr),
        reqHdr.willLength)) {
        // TODO(ongaro): should be some other error or silent
        throw RequestFormatError(HERE);
    }
}

/**
 * Handle the REQUEST_SERVER_LIST RPC.
 *
 * The Coordinator always pushes server lists and their updates. If a server's
 * FailureDetector determines that the list is out of date, it issues an RPC
 * here to request that we re-send the list.
 *
 * \copydetails Service::ping
 */
void
CoordinatorService::requestServerList(
    const RequestServerListRpc::Request& reqHdr,
    RequestServerListRpc::Response& respHdr,
    Rpc& rpc)
{
    ServerId id(reqHdr.serverId);
    rpc.sendReply();

    if (!serverList.contains(id)) {
        LOG(WARNING, "Could not send list to unknown server %lu", *id);
        return;
    }

    if (serverList[id.indexNumber()]->status != ServerStatus::UP) {
        LOG(WARNING, "Could not send list to crashed server %lu", *id);
        return;
    }

    if (!serverList[id].serviceMask.has(MEMBERSHIP_SERVICE)) {
        LOG(WARNING, "Could not send list to server without membership "
            "service: %lu", *id);
        return;
    }

    LOG(DEBUG, "Sending server list to server id %lu as requested", *id);
    sendServerList(id);
}

/**
 * Issue a cluster membership update to all enlisted servers in the system
 * that are running the MembershipService.
 *
 * \param update
 *      Protocol Buffer containing the update to be sent.
 *
 * \param excludeServerId
 *      ServerId of a server that is not to receive this update. This is
 *      used to avoid sending an update message to a server immediately
 *      following its enlistment (since we'll be sending the entire list
 *      instead).
 */
void
CoordinatorService::sendMembershipUpdate(ProtoBuf::ServerList& update,
                                         ServerId excludeServerId)
{
    MembershipClient client;
    for (size_t i = 0; i < serverList.size(); i++) {
        if (serverList[i] == NULL ||
            serverList[i]->status != ServerStatus::UP ||
            !serverList[i]->serviceMask.has(MEMBERSHIP_SERVICE))
            continue;
        if (serverList[i]->serverId == excludeServerId)
            continue;

        bool succeeded = false;
        try {
            succeeded = client.updateServerList(
                serverList[i]->serviceLocator.c_str(), update);
        } catch (TransportException& e) {
            // It's suspicious that pushing the update failed, but
            // perhaps it's best to wait to try the full list push
            // before jumping to conclusions.
        }

        // If this server had missed a previous update it will return
        // failure and expect us to push the whole list again.
        if (!succeeded) {
            LOG(NOTICE, "Server %lu had lost an update. Sending whole list.",
                *serverList[i]->serverId);
            try {
                sendServerList(serverList[i]->serverId);
            } catch (TransportException& e) {
                // TODO(stutsman): Things aren't looking good for this
                // server.  The coordinator will probably want to investigate
                // the server and evict it.
            }
        }
    }
}

/**
 * Push the entire server list to the specified server. This is used to both
 * push the initial list when a server enlists, as well as to push the list
 * again if a server misses any updates and has gone out of sync.
 *
 * \param destination
 *      ServerId of the server to send the list to.
 */
void
CoordinatorService::sendServerList(ServerId destination)
{
    ProtoBuf::ServerList serializedServerList;
    serverList.serialize(serializedServerList);

    MembershipClient client;
    client.setServerList(serverList[destination].serviceLocator.c_str(),
                         serializedServerList);
}

/**
 * Handle the SET_MIN_OPEN_SEGMENT_ID.
 *
 * Updates the minimum open segment id for a particular server.  If the
 * requested update is less than the current value maintained by the
 * coordinator then the old value is retained (that is, the coordinator
 * ignores updates that decrease the value).
 * Any open replicas found during recovery are considered invalid
 * if they have a segmentId less than the minimum open segment id maintained
 * by the coordinator.  This is used by masters to invalidate replicas they
 * have lost contact with while actively writing to them.
 *
 * \copydetails Service::ping
 */
void
CoordinatorService::setMinOpenSegmentId(
    const SetMinOpenSegmentIdRpc::Request& reqHdr,
    SetMinOpenSegmentIdRpc::Response& respHdr,
    Rpc& rpc)
{
    ServerId serverId(reqHdr.serverId);
    uint64_t segmentId = reqHdr.segmentId;

    LOG(DEBUG, "setMinOpenSegmentId for server %lu to %lu",
        serverId.getId(), segmentId);

    if (!serverList.contains(serverId)) {
        LOG(WARNING, "setMinOpenSegmentId server doesn't exist: %lu",
            serverId.getId());
        respHdr.common.status = STATUS_SERVER_DOESNT_EXIST;
        return;
    }

    CoordinatorServerList::Entry& entry = serverList[serverId];
    if (entry.minOpenSegmentId < segmentId)
        entry.minOpenSegmentId = segmentId;
}

/**
 * Update the will associated with a specific master ServerId.
 *
 * \param masterId
 *      ServerId of the master whose will is to be set.
 *
 * \param buffer
 *      Buffer containing the will.
 *
 * \param offset
 *      Byte offset of the will in the buffer.
 *
 * \param length
 *      Length of the will in bytes.
 */
bool
CoordinatorService::setWill(ServerId masterId, Buffer& buffer,
                            uint32_t offset, uint32_t length)
{
    if (serverList.contains(masterId)) {
        CoordinatorServerList::Entry& master = serverList[masterId];
        if (!master.isMaster()) {
            LOG(WARNING, "Server %lu is not a master!!", masterId.getId());
            return false;
        }

        ProtoBuf::Tablets* oldWill = master.will;
        ProtoBuf::Tablets* newWill = new ProtoBuf::Tablets();
        ProtoBuf::parseFromResponse(buffer, offset, length, *newWill);
        master.will = newWill;

        LOG(NOTICE, "Master %lu updated its Will (now %d entries, was %d)",
            masterId.getId(), newWill->tablet_size(), oldWill->tablet_size());

        delete oldWill;
        return true;
    }

    LOG(WARNING, "Master %lu could not be found!!", masterId.getId());
    return false;
}

/**
 * Initiate a recovery of a crashed master.
 *
 * \param serverEntry
 *      The crashed server which is to be recovered.  If the server was
 *      not running a master service then nothing is done.
 * \throw Exception
 *      If \a serverId is not in #serverList.
 */
void
CoordinatorService::startMasterRecovery(
                                const CoordinatorServerList::Entry& serverEntry)
{
    if (!serverEntry.isMaster())
        return;

    ServerId serverId = serverEntry.serverId;
    std::unique_ptr<ProtoBuf::Tablets> will(serverEntry.will);

    foreach (ProtoBuf::Tablets::Tablet& tablet,
             *tabletMap.mutable_tablet()) {
        if (tablet.server_id() == serverId.getId())
            tablet.set_state(ProtoBuf::Tablets_Tablet::RECOVERING);
    }

    LOG(NOTICE, "Recovering master %lu (\"%s\") on %u recovery masters "
        "using %u backups", serverId.getId(),
        serverEntry.serviceLocator.c_str(),
        serverList.masterCount(),
        serverList.backupCount());

    BaseRecovery* recovery = NULL;
    if (mockRecovery != NULL) {
        (*mockRecovery)(serverId, *will, serverList);
        recovery = mockRecovery;
    } else {
        recovery = new Recovery(serverId, *will, serverList);
    }

    // Keep track of recovery for each of the tablets it's working on.
    foreach (ProtoBuf::Tablets::Tablet& tablet,
             *tabletMap.mutable_tablet()) {
        if (tablet.server_id() == serverId.getId())
            tablet.set_user_data(reinterpret_cast<uint64_t>(recovery));
    }

    recovery->start();
}

/**
 * Investigate \a serverId and make a verdict about its whether it is alive.
 *
 * \param serverId
 *      Server to investigate.
 * \return
 *      True if the server is dead, false if it is alive.
 * \throw Exception
 *      If \a serverId is not in #serverList.
 */
bool
CoordinatorService::verifyServerFailure(ServerId serverId) {
    // Skip the real ping if this is from a unit test
    if (forceServerDownForTesting)
        return true;

    const string& serviceLocator = serverList[serverId].serviceLocator;
    try {
        uint64_t nonce = generateRandom();
        PingClient pingClient;
        pingClient.ping(serviceLocator.c_str(),
                        nonce, TIMEOUT_USECS * 1000);
        LOG(NOTICE, "False positive for server id %lu (\"%s\")",
                    *serverId, serviceLocator.c_str());
        return false;
    } catch (TransportException& e) {
    } catch (TimeoutException& e) {
    }
    LOG(NOTICE, "Verified host failure: id %lu (\"%s\")",
        *serverId, serviceLocator.c_str());

    return true;
}

} // namespace RAMCloud
