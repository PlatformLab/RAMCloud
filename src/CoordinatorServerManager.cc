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
        } catch (const ServerNotUpException& e) {
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
 * Do everything needed to execute the EnlistServer operation.
 * Do any processing required before logging the state
 * in LogCabin, log the state in LogCabin, then call #complete().
 */
ServerId
CoordinatorServerManager::EnlistServer::execute()
{
    newServerId = manager.service.serverList.generateUniqueId();

    ProtoBuf::ServerInformation state;
    state.set_entry_type("ServerEnlisting");
    state.set_server_id(newServerId.getId());
    state.set_service_mask(serviceMask.serialize());
    state.set_read_speed(readSpeed);
    state.set_service_locator(string(serviceLocator));

    EntryId entryId =
        manager.service.logCabinHelper->appendProtoBuf(state);
    manager.service.serverList.addServerInfoLogId(newServerId, entryId);
    LOG(DEBUG, "LogCabin: ServerEnlisting entryId: %lu", entryId);

    return complete(entryId);
}

/**
 * Complete the EnlistServer operation after its state has been
 * logged in LogCabin.
 * This is called internally by #execute() in case of normal operation
 * (which is in turn called by #enlistServer()), and
 * directly for coordinator recovery (by #enlistServerRecover()).
 *
 * \param entryId
 *      The entry id of the LogCabin entry corresponding to the state
 *      of the operation to be completed.
 */
ServerId
CoordinatorServerManager::EnlistServer::complete(EntryId entryId)
{
    manager.service.serverList.add(
            newServerId, serviceLocator, serviceMask, readSpeed);

    CoordinatorServerList::Entry entry =
            manager.service.serverList[newServerId];

    LOG(NOTICE, "Enlisting new server at %s (server id %s) supporting "
        "services: %s", serviceLocator, newServerId.toString().c_str(),
        entry.services.toString().c_str());

    if (entry.isBackup()) {
        LOG(DEBUG, "Backup at id %s has %u MB/s read",
            newServerId.toString().c_str(), readSpeed);
        manager.createReplicationGroup();
    }

    ProtoBuf::ServerInformation state;
    state.set_entry_type("ServerEnlisted");
    state.set_server_id(newServerId.getId());
    state.set_service_mask(serviceMask.serialize());
    state.set_read_speed(readSpeed);
    state.set_service_locator(string(serviceLocator));

    EntryId newEntryId = manager.service.logCabinHelper->appendProtoBuf(
        state, vector<EntryId>({entryId}));
    manager.service.serverList.addServerInfoLogId(newServerId, newEntryId);
    LOG(DEBUG, "LogCabin: ServerEnlisted entryId: %lu", newEntryId);

    return newServerId;
}

/**
 * Implements enlisting a server onto the CoordinatorServerList and
 * propagating updates to the cluster.
 *
 * \param replacesId
 *      Server id of the server that the enlisting server is replacing.
 * \param serviceMask
 *      Services supported by the enlisting server.
 * \param readSpeed
 *      Read speed of the enlisting server.
 * \param serviceLocator
 *      Service Locator of the enlisting server.
 *
 * \return
 *      Server id assigned to the enlisting server.
 */
ServerId
CoordinatorServerManager::enlistServer(
    ServerId replacesId, ServiceMask serviceMask, const uint32_t readSpeed,
    const char* serviceLocator)
{
    Lock _(mutex);

    // The order of the updates in serverListUpdate is important: the remove
    // must be ordered before the add to ensure that as members apply the
    // update they will see the removal of the old server id before the
    // addition of the new, replacing server id.

    if (service.serverList.contains(replacesId)) {
        LOG(NOTICE, "%s is enlisting claiming to replace server id "
            "%s, which is still in the server list, taking its word "
            "for it and assuming the old server has failed",
            serviceLocator, replacesId.toString().c_str());

        serverDown(replacesId);
    }

    ServerId newServerId =
        EnlistServer(*this, ServerId(), serviceMask,
                     readSpeed, serviceLocator).execute();

    if (replacesId.isValid()) {
        LOG(NOTICE, "Newly enlisted server %s replaces server %s",
                    newServerId.toString().c_str(),
                    replacesId.toString().c_str());
    }

    return newServerId;
}

/**
 * Complete an enlistServer during coordinator recovery.
 *
 * \param state
 *      The ProtoBuf that encapsulates the state of the enlistServer
 *      operation to be recovered.
 * \param entryId
 *      The entry id of the LogCabin entry corresponding to the state.
 */
void
CoordinatorServerManager::enlistServerRecover(
    ProtoBuf::ServerInformation* state, EntryId entryId)
{
    Lock _(mutex);
    LOG(DEBUG, "CoordinatorServerManager::enlistServerRecover()");
    EnlistServer(*this,
                 ServerId(state->server_id()),
                 ServiceMask::deserialize(state->service_mask()),
                 state->read_speed(),
                 state->service_locator().c_str()).complete(entryId);
}

/**
 * During coordinator recovery, add a server that had already been
 * enlisted to local server list.
 *
 * \param state
 *      The ProtoBuf that encapsulates the information about server
 *      to be added.
 * \param entryId
 *      The entry id of the LogCabin entry corresponding to the state.
 */
void
CoordinatorServerManager::enlistedServerRecover(
    ProtoBuf::ServerInformation* state, EntryId entryId)
{
    Lock _(mutex);

    LOG(DEBUG, "CoordinatorServerManager::enlistedServerRecover()");
    // TODO(ankitak): This will automatically queue a serverlist update
    // to be sent to the cluster. We don't want to do that for efficiency.
    service.serverList.add(
            ServerId(state->server_id()),
            state->service_locator().c_str(),
            ServiceMask::deserialize(state->service_mask()),
            state->read_speed());
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
    if (!service.serverList.contains(serverId) ||
         service.serverList[serverId].status != ServerStatus::UP) {
         LOG(NOTICE, "Spurious crash report on unknown server id %s",
             serverId.toString().c_str());
         return true;
     }

     LOG(NOTICE, "Checking server id %s (%s)",
         serverId.toString().c_str(), service.serverList.getLocator(serverId));
     if (!verifyServerFailure(serverId))
         return false;

     LOG(NOTICE, "Server id %s has crashed, notifying the cluster and "
         "starting recovery", serverId.toString().c_str());

     serverDown(serverId);

     return true;
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
 * Do everything needed to execute the ServerDown operation.
 * Do any processing required before logging the state
 * in LogCabin, log the state in LogCabin, then call #complete().
 */
void
CoordinatorServerManager::ServerDown::execute()
{
    ProtoBuf::StateServerDown state;
    state.set_entry_type("StateServerDown");
    state.set_server_id(this->serverId.getId());

    EntryId entryId = manager.service.logCabinHelper->appendProtoBuf(state);
    LOG(DEBUG, "LogCabin: StateServerDown entryId: %lu", entryId);

    complete(entryId);
}

/**
 * Complete the ServerDown operation after its state has been
 * logged in LogCabin.
 * This is called internally by #execute() in case of normal operation, and
 * directly for coordinator recovery (by #serverDownRecover()).
 *
 * \param entryId
 *      The entry id of the LogCabin entry corresponding to the state
 *      of the operation to be completed.
 */
void
CoordinatorServerManager::ServerDown::complete(EntryId entryId)
{
    // Get the entry ids for the LogCabin entries corresponding to this
    // server before the server information is removed from serverList,
    // so that the LogCabin entry can be invalidated later.
    EntryId serverInfoLogId =
        manager.service.serverList.getServerInfoLogId(serverId);
    EntryId serverUpdateLogId =
        manager.service.serverList.getServerUpdateLogId(serverId);

    // If this machine has a backup and master on the same server it is best
    // to remove the dead backup before initiating recovery. Otherwise, other
    // servers may try to backup onto a dead machine which will cause delays.
    CoordinatorServerList::Entry entry(manager.service.serverList[serverId]);

    manager.service.serverList.crashed(serverId);
    // If the server being replaced did not have a master then there
    // will be no recovery.  That means it needs to transition to
    // removed status now (usually recoveries remove servers from the
    // list when they complete).
    if (!entry.services.has(WireFormat::MASTER_SERVICE))
        manager.service.serverList.remove(serverId);

    manager.service.recoveryManager.startMasterRecovery(entry.serverId);

    manager.removeReplicationGroup(entry.replicationId);
    manager.createReplicationGroup();

    vector<EntryId> invalidates {serverInfoLogId, entryId};
    if (serverUpdateLogId)
        invalidates.push_back(serverUpdateLogId);

    manager.service.logCabinLog->invalidate(invalidates);
}

/**
 * Force out a server from the cluster.
 * \warning The calling function has to hold a lock for thread safety.
 *
 * \param serverId
 *      Server id of the server to be forced out.
 */
void
CoordinatorServerManager::serverDown(ServerId serverId)
{
    ServerDown(*this, serverId).execute();
}

/**
 * Complete a ServerDown during coordinator recovery.
 *
 * \param state
 *      The ProtoBuf that encapsulates the state of the ServerDown
 *      operation to be recovered.
 * \param entryId
 *      The entry id of the LogCabin entry corresponding to the state.
 */
void
CoordinatorServerManager::serverDownRecover(
    ProtoBuf::StateServerDown* state, EntryId entryId)
{
    Lock _(mutex);
    LOG(DEBUG, "CoordinatorServerManager::serverDownRecover()");
    ServerDown(*this, ServerId(state->server_id())).complete(entryId);
}

/**
 * Do everything needed to execute the SetMasterRecoveryInfo operation.
 * Do any processing required before logging the state
 * in LogCabin, log the state in LogCabin, then call #complete().
 */
void
CoordinatorServerManager::SetMasterRecoveryInfo::execute()
{
    EntryId oldEntryId =
        manager.service.serverList.getServerUpdateLogId(serverId);

    ProtoBuf::ServerUpdate serverUpdate;
    vector<EntryId> invalidates;

    if (oldEntryId) {
        // TODO(ankitak): After ongaro has added curser API to LogCabin,
        // use that to read in only one entry here.
        vector<Entry> entriesRead =
                manager.service.logCabinLog->read(oldEntryId);
        manager.service.logCabinHelper->parseProtoBufFromEntry(
                entriesRead[0], serverUpdate);
        invalidates.push_back(oldEntryId);
    } else {
        serverUpdate.set_entry_type("ServerUpdate");
        serverUpdate.set_server_id(serverId.getId());
    }

    (*serverUpdate.mutable_master_recovery_info()) = recoveryInfo;

    EntryId newEntryId =
        manager.service.logCabinHelper->appendProtoBuf(
            serverUpdate, invalidates);

    complete(newEntryId);
}

/**
 * Complete the SetMasterRecoveryInfo operation after its state has been
 * logged in LogCabin.
 * This is called internally by #execute() in case of normal operation
 * (which is in turn called by #setMasterRecoveryInfo()), and
 * directly for coordinator recovery (by #setMasterRecoveryInfoRecover()).
 *
 * \param entryId
 *      The entry id of the LogCabin entry corresponding to the state
 *      of the operation to be completed.
 */
void
CoordinatorServerManager::SetMasterRecoveryInfo::complete(EntryId entryId)
{
    try {
        // Update local state.
        manager.service.serverList.addServerUpdateLogId(serverId, entryId);
        manager.service.serverList.setMasterRecoveryInfo(serverId,
                                                         recoveryInfo);
    } catch (const ServerListException& e) {
        LOG(WARNING, "setMasterRecoveryInfo server doesn't exist: %s",
            serverId.toString().c_str());
        manager.service.logCabinLog->invalidate(vector<EntryId>(entryId));
        throw ServerListException(e);
    }
}

/**
 * Reset extra metadata for \a serverId that will be needed to safely recover
 * the master's log.
 *
 * \param serverId
 *      ServerId of the server whose master recovery info will be set.
 * \param recoveryInfo
 *      Information the coordinator will need to safely recover the master
 *      at \a serverId. The information is opaque to the coordinator other
 *      than its master recovery routines, but, basically, this is used to
 *      prevent inconsistent open replicas from being used during recovery.
 */
void
CoordinatorServerManager::setMasterRecoveryInfo(
    ServerId serverId, const ProtoBuf::MasterRecoveryInfo& recoveryInfo)
{
    Lock _(mutex);
    SetMasterRecoveryInfo(*this, serverId, recoveryInfo).execute();
}

/**
 * Reset the extra metadata for master recovery of the server specified in
 * the serverInfo Protobuf.
 *
 * \param serverUpdate
 *      The ProtoBuf that has the update about the server whose
 *      masterRecoveryInfo is to be set.
 * \param entryId
 *      The entry id of the LogCabin entry corresponding to serverUpdate.
 */
void
CoordinatorServerManager::setMasterRecoveryInfoRecover(
    ProtoBuf::ServerUpdate* serverUpdate, EntryId entryId)
{
    Lock _(mutex);
    LOG(DEBUG, "CoordinatorServerManager::setMasterRecoveryInfoRecover()");
    SetMasterRecoveryInfo(*this,
                          ServerId(serverUpdate->server_id()),
                          serverUpdate->master_recovery_info()).
                                                        complete(entryId);
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
        LOG(NOTICE, "False positive for server id %s (\"%s\")",
                    serverId.toString().c_str(), serviceLocator.c_str());
        return false;
    }
    LOG(NOTICE, "Verified host failure: id %s (\"%s\")",
        serverId.toString().c_str(), serviceLocator.c_str());
    return true;
}

} // namespace RAMCloud
