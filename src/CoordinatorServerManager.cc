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

#include "CoordinatorServerManager.h"
#include "CoordinatorService.h"
#include "MembershipClient.h"
#include "PingClient.h"
#include "ShortMacros.h"

namespace RAMCloud {

CoordinatorServerManager::CoordinatorServerManager(
        CoordinatorService& coordinatorService)
    : service(coordinatorService)
    , nextReplicationId(1)
    , forceServerDownForTesting(false)
    , mutex()
{
}

CoordinatorServerManager::~CoordinatorServerManager()
{
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
CoordinatorServerManager::assignReplicationGroup(
    uint64_t replicationId, const vector<ServerId>& replicationGroupIds)
{
    foreach (ServerId backupId, replicationGroupIds) {
        if (!service.serverList.contains(backupId)) {
            return false;
        }
        service.serverList.setReplicationId(backupId, replicationId);
        // Try to send an assignReplicationId Rpc to a backup. If the RPC
        // fails because the server is no longer in the cluster, the function
        // aborts. Even if we didn't abort in case of a failed RPC, masters
        // would still not use the failed replication group, since it would
        // not accept their RPCs.
        try {
            // This code is not currently safe; see RAM-429.  Fortunately,
            // replication groups aren't actually used right now, so we can
            // just do nothing.
#if 0
            BackupClient::assignGroup(service.context, backupId,
                replicationId,
                static_cast<uint32_t>(replicationGroupIds.size()),
                &replicationGroupIds[0]);
#endif
        } catch (const ServerDoesntExistException& e) {
            return false;
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
CoordinatorServerManager::createReplicationGroup()
{
    // Create a list of all servers who do not belong to a replication group
    // and are up. Note that this is a performance optimization and is not
    // required for correctness.
    vector<ServerId> freeBackups;
    for (size_t i = 0; i < service.serverList.size(); i++) {
        if (service.serverList[i] &&
            service.serverList[i]->isBackup() &&
            service.serverList[i]->replicationId == 0) {
            freeBackups.push_back(service.serverList[i]->serverId);
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
            service.serverList.setReplicationId(backupId, nextReplicationId);
            freeBackups.pop_back();
        }
        // Assign a new replication group. AssignReplicationGroup handles
        // Rpc failures.
        assignReplicationGroup(nextReplicationId, group);
        nextReplicationId++;
    }
}

 /**
 * Implements enlisting a server onto the CoordinatorServerList and
 * propagating updates to the cluster.
 *
 * TODO(ankitak): Re-work after RAM-431 is resolved.
 *
 * \param replacesId
 *      Server id of the server that the enlisting server is replacing.
 * \param serviceMask
 *      Services supported by the enlisting server.
 * \param readSpeed
 *      Read speed of the enlisting server.
 * \param writeSpeed
 *      Write speed of the enlisting server.
 * \param serviceLocator
 *      Service Locator of the enlisting server.
 *
 * \return
 *      Server id assigned to the enlisting server.
 */
ServerId
CoordinatorServerManager::enlistServer(
    ServerId replacesId, ServiceMask serviceMask, const uint32_t readSpeed,
    const uint32_t writeSpeed,  const char* serviceLocator)
{
    Tub<CoordinatorServerList::Entry> replacedEntry;
    Tub<LogCabin::Client::EntryId> replacesEntryId;

    // The order of the updates in serverListUpdate is important: the remove
    // must be ordered before the add to ensure that as members apply the
    // update they will see the removal of the old server id before the
    // addition of the new, replacing server id.
    if (service.serverList.contains(replacesId)) {
        replacesEntryId =
            service.serverList.getLogCabinEntryId(replacesId);
        LOG(NOTICE, "%s is enlisting claiming to replace server id "
            "%lu, which is still in the server list, taking its word "
            "for it and assuming the old server has failed",
            serviceLocator, replacesId.getId());
        replacedEntry.construct(service.serverList[replacesId]);
        service.serverList.crashed(replacesId);
        // If the server being replaced did not have a master then there
        // will be no recovery.  That means it needs to transition to
        // removed status now (usually recoveries remove servers from the
        // list when they complete).
        if (!replacedEntry.get()->services.has(WireFormat::MASTER_SERVICE))
            service.serverList.remove(replacesId);
    }

    ServerId newServerId = service.serverList.add(serviceLocator,
                                                   serviceMask,
                                                   readSpeed);

    service.serverList.sendMembershipUpdate(newServerId);

    // Log Cabin Entry TODO(ankitak) look this over.
    ProtoBuf::StateEnlistServer state;
    state.set_entry_type("StateEnlistServer");
    state.set_replaces_id(replacesId.getId());
    state.set_new_server_id(newServerId.getId());
    state.set_service_mask(serviceMask.serialize());
    state.set_read_speed(readSpeed);
    state.set_write_speed(writeSpeed);
    state.set_service_locator(string(serviceLocator));
    auto stateEntryId = service.logCabinHelper->appendProtoBuf(state);

    CoordinatorServerList::Entry entry = service.serverList[newServerId];
    LOG(NOTICE, "Enlisting new server at %s (server id %lu) supporting "
        "services: %s", serviceLocator, newServerId.getId(),
        entry.services.toString().c_str());

    if (entry.services.has(WireFormat::MEMBERSHIP_SERVICE))
        sendServerList(newServerId);

    // Recovery on the replaced host is deferred until the replacing host
    // has been enlisted.
    if (replacedEntry)
        service.recoveryManager.startMasterRecovery(
            replacedEntry.get()->serverId);

    if (replacesId.isValid()) {
        LOG(NOTICE, "Newly enlisted server %lu replaces server %lu",
            newServerId.getId(), replacesId.getId());
    }

    if (entry.isBackup()) {
        LOG(DEBUG, "Backup at id %lu has %u MB/s read %u MB/s write",
            newServerId.getId(), readSpeed, writeSpeed);
        createReplicationGroup();
    }

     ProtoBuf::ServerInformation info;
    info.set_entry_type("ServerInformation");
    info.set_server_id(newServerId.getId());
    info.set_service_mask(serviceMask.serialize());
    info.set_read_speed(readSpeed);
    info.set_write_speed(writeSpeed);
    info.set_service_locator(string(serviceLocator));

    vector<EntryId> invalidates;
    invalidates.push_back(stateEntryId);
    if (replacesEntryId)
        invalidates.push_back(replacesEntryId);

    EntryId infoEntryId =
        service.logCabinHelper->appendProtoBuf(info, invalidates);
    service.serverList.addLogCabinEntryId(newServerId, infoEntryId);

    return newServerId;
}

/**
 * Return the serialized server list filtered by the serviceMask.
 *
 * \param serviceMask
 *      Return servers that support this service.
 * \return
 *      Serialized server list filtered by the serviceMask.
 */
ProtoBuf::ServerList
CoordinatorServerManager::getServerList(ServiceMask serviceMask)
{
    ProtoBuf::ServerList serialServerList;
    service.serverList.serialize(serialServerList, serviceMask);
    return serialServerList;
}

bool
CoordinatorServerManager::HintServerDown::execute()
{
     if (!manager.service.serverList.contains(serverId) ||
         manager.service.serverList[serverId].status != ServerStatus::UP) {
         LOG(NOTICE, "Spurious crash report on unknown server id %lu",
             serverId.getId());
         return true;
     }

     LOG(NOTICE, "Checking server id %lu (%s)",
         serverId.getId(), manager.service.serverList.getLocator(serverId));
     if (!manager.verifyServerFailure(serverId))
         return false;

     LOG(NOTICE, "Server id %lu has crashed, notifying the cluster and "
         "starting recovery", serverId.getId());

     ProtoBuf::StateHintServerDown state;
     state.set_entry_type("StateHintServerDown");
     state.set_server_id(this->serverId.getId());

     EntryId entryId = manager.service.logCabinHelper->appendProtoBuf(state);
     LOG(DEBUG, "LogCabin entryId: %lu", entryId);

     return complete(entryId);
}

bool
CoordinatorServerManager::HintServerDown::complete(EntryId entryId)
{
     // Get the entry id for the LogCabin entry corresponding to this
     // server before the server information is removed from serverList,
     // so that the LogCabin entry can be invalidated later.
     EntryId serverInfoEntryId =
         manager.service.serverList.getLogCabinEntryId(serverId);

     // If this machine has a backup and master on the same server it is best
     // to remove the dead backup before initiating recovery. Otherwise, other
     // servers may try to backup onto a dead machine which will cause delays.
     CoordinatorServerList::Entry entry = manager.service.serverList[serverId];
     manager.service.serverList.crashed(serverId);
     // If the server being replaced did not have a master then there
     // will be no recovery.  That means it needs to transition to
     // removed status now (usually recoveries remove servers from the
     // list when they complete).
     if (!entry.services.has(WireFormat::MASTER_SERVICE))
         manager.service.serverList.remove(serverId);

     // Update cluster membership information.
     // Backup recovery is kicked off via this update.
     // Deciding whether to place this before or after the start of master
     // recovery is difficult.
     manager.service.serverList.sendMembershipUpdate(
                                    ServerId(/* invalid id */));

     manager.service.recoveryManager.startMasterRecovery(entry.serverId);

     manager.removeReplicationGroup(entry.replicationId);
     manager.createReplicationGroup();

     manager.service.logCabinLog->invalidate(
         vector<EntryId>(serverInfoEntryId, entryId));

     return true;
}

/**
 * Returns true if server is down, false otherwise.
 *
 * \param serverId
 *      ServerId of the server that is suspected to be down.
 * \return
 *      True if server is down, false otherwise.
 */
bool
CoordinatorServerManager::hintServerDown(ServerId serverId)
{
    Lock _(mutex);
    return HintServerDown(*this, serverId).execute();
}

/**
 * Complete a hintServerDown during coordinator recovery.
 *
 * \param state
 *      The ProtoBuf that encapsulates the state of the hintServerDown
 *      operation to be recovered.
 * \param entryId
 *      The entry id of the LogCabin entry corresponding to the state.
 */
void
CoordinatorServerManager::hintServerDownRecover(
    ProtoBuf::StateHintServerDown* state, EntryId entryId)
{
    Lock _(mutex);
    ServerId serverId = ServerId(state->server_id());
    HintServerDown(*this, serverId).complete(entryId);
}

/**
 * Reset the replicationId for all backups with groupId.
 *
 * \param groupId
 *      Replication group that needs to be reset.
 */
void
CoordinatorServerManager::removeReplicationGroup(uint64_t groupId)
{
    // Cannot remove groupId 0, since it is the default groupId.
    if (groupId == 0) {
        return;
    }
    for (size_t i = 0; i < service.serverList.size(); i++) {
        if (service.serverList[i] &&
            service.serverList[i]->replicationId == groupId) {
            vector<ServerId> group;
            group.push_back(service.serverList[i]->serverId);
            service.serverList.setReplicationId(
                    service.serverList[i]->serverId, 0);
            // We check whether the server is up, in order to prevent
            // recursively calling removeReplicationGroup by hintServerDown.
            // If the backup is still up, we tell it to reset its
            // replicationId, so it will stop accepting Rpcs. This is an
            // optimization; even if we didn't reset its replicationId, Rpcs
            // sent to the server's group members would fail, because at least
            // one of the servers in the group is down.
            if (service.serverList[i]->isBackup()) {
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
 * \param serverId
 *      ServerId of the server to send the list to.
 */
void
CoordinatorServerManager::sendServerList(ServerId serverId)
{
    service.serverList.sendServerList(serverId);
}

void
CoordinatorServerManager::SetMinOpenSegmentId::execute()
{
    EntryId oldEntryId =
        manager.service.serverList.getLogCabinEntryId(serverId);
    ProtoBuf::ServerInformation serverInfo;
    manager.service.logCabinHelper->getProtoBufFromEntryId(
        oldEntryId, serverInfo);

    serverInfo.set_min_open_segment_id(segmentId);

    EntryId newEntryId =
        manager.service.logCabinHelper->appendProtoBuf(
            serverInfo, vector<EntryId>(oldEntryId));

    complete(newEntryId);
}

void
CoordinatorServerManager::SetMinOpenSegmentId::complete(EntryId entryId)
{
    try {
        // Update local state.
        manager.service.serverList.addLogCabinEntryId(serverId, entryId);
        manager.service.serverList.setMinOpenSegmentId(serverId, segmentId);
    } catch (const ServerListException& e) {
        LOG(WARNING, "setMinOpenSegmentId server doesn't exist: %lu",
            serverId.getId());
        manager.service.logCabinLog->invalidate(vector<EntryId>(entryId));
        throw ServerListException(e);
    }
}

/**
 * Set the minOpenSegmentId of the specified server to the specified segmentId.
 *
 * \param serverId
 *      ServerId of the server whose minOpenSegmentId will be set.
 * \param segmentId
 *      The minOpenSegmentId to be set.
 */
void
CoordinatorServerManager::setMinOpenSegmentId(
    ServerId serverId, uint64_t segmentId)
{
    Lock _(mutex);
    SetMinOpenSegmentId(*this, serverId, segmentId).execute();
}

/**
 * Set the minOpenSegmentId of the server specified in the serverInfo Protobuf
 * to the segmentId specified in the Protobuf.
 *
 * \param serverInfo
 *      The ProtoBuf that has the information about the server whose
 *      minOpenSegmentId is to be set.
 * \param entryId
 *      The entry id of the LogCabin entry corresponding to the serverInfo.
 */
void
CoordinatorServerManager::setMinOpenSegmentIdRecover(
    ProtoBuf::ServerInformation* serverInfo, EntryId entryId)
{
    Lock _(mutex);
    ServerId serverId = ServerId(serverInfo->server_id());
    uint64_t segmentId = serverInfo->min_open_segment_id();
    SetMinOpenSegmentId(*this, serverId, segmentId).complete(entryId);
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
CoordinatorServerManager::verifyServerFailure(ServerId serverId) {
    // Skip the real ping if this is from a unit test
    if (forceServerDownForTesting)
        return true;

    const string& serviceLocator = service.serverList[serverId].serviceLocator;
    PingRpc pingRpc(service.context, serverId, ServerId());
    if (pingRpc.wait(TIMEOUT_USECS * 1000) != ~0UL) {
        LOG(NOTICE, "False positive for server id %lu (\"%s\")",
                    *serverId, serviceLocator.c_str());
        return false;
    }
    LOG(NOTICE, "Verified host failure: id %lu (\"%s\")",
        *serverId, serviceLocator.c_str());
    return true;
}

} // namespace RAMCloud
