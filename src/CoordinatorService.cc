/* Copyright (c) 2009-2012 Stanford University
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

CoordinatorService::CoordinatorService(Context& context)
    : context(context)
    , serverList(context)
    , tabletMap()
    , tables()
    , nextTableId(0)
    , nextTableMasterIdx(0)
    , nextReplicationId(1)
    , runtimeOptions()
    , recoveryManager(context, serverList, tabletMap, &runtimeOptions)
    , forceServerDownForTesting(false)
{
    recoveryManager.start();
}

CoordinatorService::~CoordinatorService()
{
    recoveryManager.halt();
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
        case GetTableIdRpc::opcode:
            callHandler<GetTableIdRpc, CoordinatorService,
                        &CoordinatorService::getTableId>(rpc);
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
        case RecoveryMasterFinishedRpc::opcode:
            callHandler<RecoveryMasterFinishedRpc, CoordinatorService,
                        &CoordinatorService::recoveryMasterFinished>(rpc);
            break;
        case BackupQuiesceRpc::opcode:
            callHandler<BackupQuiesceRpc, CoordinatorService,
                        &CoordinatorService::quiesce>(rpc);
            break;
        case SetWillRpc::opcode:
            callHandler<SetWillRpc, CoordinatorService,
                        &CoordinatorService::setWill>(rpc);
            break;
        case SendServerListRpc::opcode:
            callHandler<SendServerListRpc, CoordinatorService,
                        &CoordinatorService::sendServerList>(rpc);
            break;
        case SetRuntimeOptionRpc::opcode:
            callHandler<SetRuntimeOptionRpc, CoordinatorService,
                        &CoordinatorService::setRuntimeOption>(rpc);
            break;
        case ReassignTabletOwnershipRpc::opcode:
            callHandler<ReassignTabletOwnershipRpc, CoordinatorService,
                        &CoordinatorService::reassignTabletOwnership>(rpc);
            break;
        case SetMinOpenSegmentIdRpc::opcode:
            callHandler<SetMinOpenSegmentIdRpc, CoordinatorService,
                        &CoordinatorService::setMinOpenSegmentId>(rpc);
            break;
        case SplitTabletRpc::opcode:
            callHandler<SplitTabletRpc, CoordinatorService,
                        &CoordinatorService::splitTablet>(rpc);
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
    uint64_t tableId = nextTableId++;
    tables[name] = tableId;
    respHdr.tableId = tableId;

    uint32_t serverSpan = reqHdr.serverSpan;
    if (serverSpan == 0)
        serverSpan = 1;

    for (uint32_t i = 0; i < serverSpan; i++) {
        uint64_t startKeyHash = i * (~0UL / serverSpan);
        if (i != 0)
            startKeyHash++;
        uint64_t endKeyHash = (i + 1) * (~0UL / serverSpan);
        if (i == serverSpan - 1)
            endKeyHash = ~0UL;

        // Find the next master in the list.
        CoordinatorServerList::Entry master;
        while (true) {
            size_t masterIdx = nextTableMasterIdx++ % serverList.size();
            auto entry = serverList[masterIdx];
            if (entry && entry->isMaster()) {
                master = *entry;
                break;
            }
        }
        const char* locator = master.serviceLocator.c_str();
        MasterClient masterClient(
            context.transportManager->getSession(locator));
        // Get current log head. Only entries >= this can be part of the tablet.
        Log::Position headOfLog = masterClient.getHeadOfLog();

        // Create tablet map entry.
        tabletMap.addTablet({tableId, startKeyHash, endKeyHash,
                             master.serverId, Tablet::NORMAL, headOfLog});

        // Create will entry. The tablet is empty, so it doesn't matter where it
        // goes or in how many partitions, initially.
        // It just has to go somewhere.
        ProtoBuf::Tablets will;
        ProtoBuf::Tablets_Tablet& willEntry(*will.add_tablet());
        willEntry.set_table_id(tableId);
        willEntry.set_start_key_hash(startKeyHash);
        willEntry.set_end_key_hash(endKeyHash);
        willEntry.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
        // Hack which hints to the server list that it should assign
        // partition ids.
        willEntry.set_user_data(~(0lu));
        willEntry.set_ctime_log_head_id(headOfLog.getSegmentId());
        willEntry.set_ctime_log_head_offset(headOfLog.getSegmentOffset());
        serverList.addToWill(master.serverId, will);

        // Inform the master.
        masterClient.takeTabletOwnership(tableId, startKeyHash, endKeyHash);

        LOG(DEBUG, "Created table '%s' with id %lu and a span %u on master %lu",
                    name, tableId, serverSpan, master.serverId.getId());
    }

    LOG(NOTICE, "Created table '%s' with id %lu",
                    name, tableId);
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
    uint64_t tableId = it->second;
    tables.erase(it);
    vector<Tablet> removed = tabletMap.removeTabletsForTable(tableId);
    foreach (const auto& tablet, removed) {
            MasterClient master(serverList.getSession(tablet.serverId));
            master.dropTabletOwnership(tableId,
                                       tablet.startKeyHash,
                                       tablet.endKeyHash);
    }

    LOG(NOTICE, "Dropped table '%s' with id %lu", name, tableId);
    LOG(DEBUG, "There are now %lu tablets in the map", tabletMap.size());
}

/**
 * Top-level server method to handle the SPLIT_TABLET request.
 * \copydetails Service::ping
 */
void
CoordinatorService::splitTablet(const SplitTabletRpc::Request& reqHdr,
                              SplitTabletRpc::Response& respHdr,
                              Rpc& rpc)
{
    // Check that the tablet with the described key ranges exists.
    // If the tablet exists, adjust its endKeyHash so it becomes the tablet
    // for the first part after the split and also copy the tablet and use
    // the copy for the second part after the split.
    const char* name = getString(rpc.requestPayload, sizeof(reqHdr),
                                 reqHdr.nameLength);
    Tables::iterator it(tables.find(name));
    if (it == tables.end()) {
        respHdr.common.status = STATUS_TABLE_DOESNT_EXIST;
        return;
    }
    uint64_t tableId = it->second;

    ServerId serverId;
    try {
        serverId = tabletMap.splitTablet(tableId,
                                         reqHdr.startKeyHash,
                                         reqHdr.endKeyHash,
                                         reqHdr.splitKeyHash).first.serverId;
    } catch (const TabletMap::NoSuchTablet& e) {
        respHdr.common.status = STATUS_TABLET_DOESNT_EXIST;
        return;
    } catch (const TabletMap::BadSplit& e) {
        respHdr.common.status = STATUS_REQUEST_FORMAT_ERROR;
        return;
    }

    // Tell the master to split the tablet
    MasterClient master(serverList.getSession(serverId));
    master.splitMasterTablet(tableId, reqHdr.startKeyHash, reqHdr.endKeyHash,
                             reqHdr.splitKeyHash);

    LOG(NOTICE, "In table '%s' I split the tablet that started at key %lu and "
                "ended at key %lu", name, reqHdr.startKeyHash,
                reqHdr.endKeyHash);
}

/**
 * Top-level server method to handle the GET_TABLE_ID request.
 * \copydetails Service::ping
 */
void
CoordinatorService::getTableId(const GetTableIdRpc::Request& reqHdr,
                              GetTableIdRpc::Response& respHdr,
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
    CoordinatorServerList::Entry entry = serverList[newServerId];

    LOG(NOTICE, "Enlisting new server at %s (server id %lu) supporting "
        "services: %s",
        serviceLocator, newServerId.getId(),
        entry.services.toString().c_str());
    if (replacesId.isValid()) {
        LOG(NOTICE, "Newly enlisted server %lu replaces server %lu",
            newServerId.getId(), replacesId.getId());
    }

    if (entry.isBackup()) {
        LOG(DEBUG, "Backup at id %lu has %u MB/s read %u MB/s write",
            newServerId.getId(), readSpeed, writeSpeed);
        createReplicationGroup();
    }

    respHdr.serverId = newServerId.getId();
    rpc.sendReply();

    if (entry.services.has(MEMBERSHIP_SERVICE))
        sendServerList(newServerId);
    serverList.sendMembershipUpdate(serverListUpdate, newServerId);

    // Recovery on the replaced host is deferred until the replacing host
    // has been enlisted.
    if (replacedEntry)
        recoveryManager.startMasterRecovery(replacedEntry->serverId);
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
    ProtoBuf::Tablets tablets;
    tabletMap.serialize(serverList, tablets);
    respHdr.tabletMapLength = serializeToResponse(rpc.replyPayload,
                                                  tablets);
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
    hintServerDown(serverId);
}

/**
 * Returns true if server is down, false otherwise.
 *
 * \param serverId
 *     ServerId of the server that is suspected to be down.
 */
bool
CoordinatorService::hintServerDown(ServerId serverId)
{
    if (!serverList.contains(serverId) ||
        serverList[serverId].status != ServerStatus::UP) {
        LOG(NOTICE, "Spurious crash report on unknown server id %lu",
            serverId.getId());
        return true;
    }

    if (!verifyServerFailure(serverId))
        return false;
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
    // recovery is difficult.
    serverList.sendMembershipUpdate(update, ServerId(/* invalid id */));

    recoveryManager.startMasterRecovery(entry.serverId);

    removeReplicationGroup(entry.replicationId);
    createReplicationGroup();

    return true;
}

/**
 * Handle the TABLETS_RECOVERED RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::recoveryMasterFinished(
    const RecoveryMasterFinishedRpc::Request& reqHdr,
    RecoveryMasterFinishedRpc::Response& respHdr,
    Rpc& rpc)
{
    ProtoBuf::Tablets recoveredTablets;
    ProtoBuf::parseFromResponse(rpc.requestPayload,
                                sizeof32(reqHdr),
                                reqHdr.tabletsLength, recoveredTablets);

    ServerId serverId = ServerId(reqHdr.recoveryMasterId);
    recoveryManager.recoveryMasterFinished(reqHdr.recoveryId,
                                           serverId,
                                           recoveredTablets,
                                           reqHdr.successful);
    // Dump the tabletMap out for easy debugging.
    LOG(DEBUG, "Coordinator tabletMap: %s",
        tabletMap.debugString().c_str());

    // TODO(stutsman): Eventually we'll want to be able to 'reject' recovery
    // master completions, so we'll need to get a return value from
    // recoveryMasterFinished.
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
        if (serverList[i] && serverList[i]->isBackup()) {
            BackupClient(context.transportManager->getSession(
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
 * Handle the REASSIGN_TABLET_OWNER RPC. This involves switching
 * ownership of the tablet and alerting the new owner that it may
 * begin servicing requests on that tablet.
 *
 * \copydetails Service::ping
 */
void
CoordinatorService::reassignTabletOwnership(
    const ReassignTabletOwnershipRpc::Request& reqHdr,
    ReassignTabletOwnershipRpc::Response& respHdr,
    Rpc& rpc)
{
    ServerId newOwner(reqHdr.newOwnerMasterId);
    if (!serverList.contains(newOwner)) {
        LOG(WARNING, "Server id %lu does not exist! Cannot reassign "
            "ownership of tablet %lu, range [%lu, %lu]!", *newOwner,
            reqHdr.tableId, reqHdr.firstKey, reqHdr.lastKey);
        respHdr.common.status = STATUS_SERVER_DOESNT_EXIST;
        return;
    }

    try {
        // Note currently this log message may not be exactly correct due to
        // concurrent operations on the tablet map which may slip in between
        // the message and the actual operation.
        Tablet tablet = tabletMap.getTablet(reqHdr.tableId,
                                           reqHdr.firstKey, reqHdr.lastKey);
        LOG(NOTICE, "Reassigning tablet %lu, range [%lu, %lu] from server "
            "id %lu to server id %lu.", reqHdr.tableId, reqHdr.firstKey,
            reqHdr.lastKey, tablet.serverId.getId(), newOwner.getId());
    } catch (const TabletMap::NoSuchTablet& e) {
        LOG(WARNING, "Could not reassign tablet %lu, range [%lu, %lu]: "
            "not found!", reqHdr.tableId, reqHdr.firstKey, reqHdr.lastKey);
        respHdr.common.status = STATUS_TABLE_DOESNT_EXIST;
        return;
    }

    Transport::SessionRef session = serverList.getSession(newOwner);
    MasterClient masterClient(session);
    // Get current head of log to preclude all previous data in the log
    // from being considered part of this tablet.
    Log::Position headOfLog = masterClient.getHeadOfLog();

    try {
        tabletMap.modifyTablet(reqHdr.tableId, reqHdr.firstKey, reqHdr.lastKey,
                               newOwner, Tablet::NORMAL, headOfLog);
    } catch (const TabletMap::NoSuchTablet& e) {
        LOG(WARNING, "Could not reassign tablet %lu, range [%lu, %lu]: "
            "not found!", reqHdr.tableId, reqHdr.firstKey, reqHdr.lastKey);
        respHdr.common.status = STATUS_TABLE_DOESNT_EXIST;
        return;
    }

    // No need to keep the master waiting. If it sent us this request,
    // then it has guaranteed that all data has been migrated. So long
    // as we think the new owner is up (i.e. haven't started a recovery
    // on it), then it's safe for the old owner to drop the data and for
    // us to switch over.
    rpc.sendReply();

    // TODO(rumble/slaughter) If we fail to alert the new owner we could
    //      get stuck in limbo. What should we do? Retry? Fail the
    //      server and recover it? Can't return to the old master if we
    //      reply early...
    masterClient.takeTabletOwnership(reqHdr.tableId,
                                     reqHdr.firstKey,
                                     reqHdr.lastKey);
}

/**
 * Handle the SEND_SERVER_LIST RPC.
 *
 * The Coordinator always pushes server lists and their updates. If a server's
 * FailureDetector determines that the list is out of date, it issues an RPC
 * here to request that we re-send the list.
 *
 * \copydetails Service::ping
 */
void
CoordinatorService::sendServerList(
    const SendServerListRpc::Request& reqHdr,
    SendServerListRpc::Response& respHdr,
    Rpc& rpc)
{
    ServerId id(reqHdr.serverId);
    rpc.sendReply();

    if (!serverList.contains(id)) {
        LOG(WARNING, "Could not send list to unknown server %lu", *id);
        return;
    }

    if (serverList[id].status != ServerStatus::UP) {
        LOG(WARNING, "Could not send list to crashed server %lu", *id);
        return;
    }

    if (!serverList[id].services.has(MEMBERSHIP_SERVICE)) {
        LOG(WARNING, "Could not send list to server without membership "
            "service: %lu", *id);
        return;
    }

    LOG(DEBUG, "Sending server list to server id %lu as requested", *id);
    sendServerList(id);
}

/**
 * Sets a runtime option field on the coordinator to the indicated value.
 * See CoordinatorClient::setRuntimeOption() for details.
 *
 * \copydetails Service::ping
 */
void
CoordinatorService::setRuntimeOption(const SetRuntimeOptionRpc::Request& reqHdr,
                                     SetRuntimeOptionRpc::Response& respHdr,
                                     Rpc& rpc)
{
    const char* option = getString(rpc.requestPayload, sizeof(reqHdr),
                                   reqHdr.optionLength);
    const char* value = getString(rpc.requestPayload,
                                  sizeof32(reqHdr) + reqHdr.optionLength,
                                  reqHdr.valueLength);
    try {
        runtimeOptions.set(option, value);
    } catch (const std::out_of_range& e) {
        respHdr.common.status = STATUS_OBJECT_DOESNT_EXIST;
    }
}

/**
 * Assign a new replicationId to a backup, and inform the backup which nodes
 * are in its replication group.
 *
 * \param replicationId
 *      New replication group Id that is assigned to backup.
 *
 * \param replicationGroupIds
 *      Includes the ServerId's of all the members of the replication group.
 *
 * \return
 *      False if one of the servers is dead, true if all of them are alive.
 */
bool
CoordinatorService::assignReplicationGroup(
    uint64_t replicationId, const vector<ServerId>& replicationGroupIds)
{
    foreach (ServerId backupId, replicationGroupIds) {
        if (!serverList.contains(backupId)) {
            return false;
        }
        serverList.setReplicationId(backupId, replicationId);
        // Try to send an assignReplicationId Rpc to a backup. If the Rpc
        // fails, hintServerDown. If hintServerDown is true, the function
        // aborts. If it fails, keep trying to send the Rpc to the backup.
        // Note that this is an optimization. Even if we didn't abort in case
        // of a failed Rpc, masters would still not use the failed replication
        // group, since it would not accept their Rpcs.
        while (true) {
            try {
                const char* locator =
                    serverList[backupId].serviceLocator.c_str();
                BackupClient backupClient(
                    context.transportManager->getSession(locator));
                backupClient.assignGroup(
                    replicationId,
                    static_cast<uint32_t>(replicationGroupIds.size()),
                    &replicationGroupIds[0]);
            } catch (TransportException& e) {
                if (hintServerDown(backupId)) {
                    return false;
                } else {
                    continue;
                }
            } catch (TimeoutException& e) {
                if (hintServerDown(backupId)) {
                    return false;
                } else {
                    continue;
                }
            }
            break;
        }
    }
    return true;
}

/**
 * Try to create a new replication group. Look for groups of backups that
 * are not assigned a replication group and are up.
 * If there are not enough available candidates for a new group, the function
 * returns without sending out any Rpcs. If there are enough group members
 * to form a new group, but one of the servers is down, hintServerDown will
 * reset the replication group of that server.
 */
void
CoordinatorService::createReplicationGroup()
{
    // Create a list of all servers who do not belong to a replication group
    // and are up. Note that this is a performance optimization and is not
    // required for correctness.
    vector<ServerId> freeBackups;
    for (size_t i = 0; i < serverList.size(); i++) {
        if (serverList[i] &&
            serverList[i]->isBackup() &&
            serverList[i]->replicationId == 0) {
            freeBackups.push_back(serverList[i]->serverId);
        }
    }

    // TODO(cidon): The coordinator currently has no knowledge of the
    // replication factor, so we manually set the replication group size to 3.
    // We should make this parameter configurable.
    const uint32_t numReplicas = 3;
    vector<ServerId> group;
    while (freeBackups.size() >= numReplicas) {
        group.clear();
        // Update the replicationId on serverList.
        for (uint32_t i = 0; i < numReplicas; i++) {
            const ServerId& backupId = freeBackups.back();
            group.push_back(backupId);
            serverList.setReplicationId(backupId, nextReplicationId);
            freeBackups.pop_back();
        }
        // Assign a new replication group. AssignReplicationGroup handles
        // Rpc failures.
        assignReplicationGroup(nextReplicationId,
                               group);
        nextReplicationId++;
    }
}

/**
 * Reset the replicationId for all backups with groupId.
 *
 * \param groupId
 *      Replication group that needs to be reset.
 */
void
CoordinatorService::removeReplicationGroup(uint64_t groupId)
{
    // Cannot remove groupId 0, since it is the default groupId.
    if (groupId == 0) {
        return;
    }
    for (size_t i = 0; i < serverList.size(); i++) {
        if (serverList[i] &&
            serverList[i]->replicationId == groupId) {
            vector<ServerId> group;
            group.push_back(serverList[i]->serverId);
            serverList.setReplicationId(serverList[i]->serverId, 0);
            // We check whether the server is up, in order to prevent
            // recursively calling removeReplicationGroup by hintServerDown.
            // If the backup is still up, we tell it to reset its
            // replicationId, so it will stop accepting Rpcs. This is an
            // optimization; even if we didn't reset its replicationId, Rpcs
            // sent to the server's group members would fail, because at least
            // one of the servers in the group is down.
            if (serverList[i]->isBackup()) {
                assignReplicationGroup(0, group);
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

    MembershipClient client(context);
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

    try {
        serverList.setMinOpenSegmentId(serverId, segmentId);
    } catch (const Exception& e) {
        LOG(WARNING, "setMinOpenSegmentId server doesn't exist: %lu",
            serverId.getId());
        respHdr.common.status = STATUS_SERVER_DOESNT_EXIST;
        return;
    }
}

/**
 * Update the will associated with a specific master ServerId.
 *
 * \param masterId
 *      ServerId of the master whose will is to be set.
 * \param buffer
 *      Buffer containing the will.
 * \param offset
 *      Byte offset of the will in the buffer.
 * \param length
 *      Length of the will in bytes.
 * \return
 *      False if masterId is not in the server list, true otherwise.
 */
bool
CoordinatorService::setWill(ServerId masterId, Buffer& buffer,
                            uint32_t offset, uint32_t length)
{
    ProtoBuf::Tablets newWill;
    ProtoBuf::parseFromResponse(buffer, offset, length, newWill);
    try {
        serverList.setWill(masterId, newWill);
    } catch (const Exception& e) {
        LOG(WARNING, "Master %lu could not be found!!", masterId.getId());
        return false;
    }
    return true;
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
        PingClient pingClient(context);
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
