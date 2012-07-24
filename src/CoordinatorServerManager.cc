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

ServerId
CoordinatorServerManager::EnlistServer::beforeReply()
{
    // The order of the updates in serverListUpdate is important: the remove
    // must be ordered before the add to ensure that as members apply the
    // update they will see the removal of the old server id before the
    // addition of the new, replacing server id.
    if (manager.service.serverList.contains(replacesId)) {
        replacesEntryId =
            manager.service.serverList.getLogCabinEntryId(replacesId);
        LOG(NOTICE, "%s is enlisting claiming to replace server id "
            "%lu, which is still in the server list, taking its word "
            "for it and assuming the old server has failed",
            serviceLocator, replacesId.getId());
        replacedEntry.construct(manager.service.serverList[replacesId]);
        // Don't increment server list yet; done after the add below.
        // Note, if the server being replaced is already crashed this may
        // not append an update at all.
        manager.service.serverList.crashed(replacesId, serverListUpdate);
        // If the server being replaced did not have a master then there
        // will be no recovery.  That means it needs to transition to
        // removed status now (usually recoveries remove servers from the
        // list when they complete).
        if (!replacedEntry->services.has(MASTER_SERVICE))
            manager.service.serverList.remove(replacesId, serverListUpdate);
    }

    newServerId = manager.service.serverList.add(
        serviceLocator, serviceMask, readSpeed, serverListUpdate);
    manager.service.serverList.incrementVersion(serverListUpdate);

    ProtoBuf::StateEnlistServer state;
    state.set_entry_type("StateEnlistServer");
    state.set_replaces_id(replacesId.getId());
    state.set_new_server_id(newServerId.getId());
    state.set_service_mask(serviceMask.serialize());
    state.set_read_speed(readSpeed);
    state.set_write_speed(writeSpeed);
    state.set_service_locator(string(serviceLocator));

    stateEntryId = manager.service.logCabinHelper->appendProtoBuf(state);

    CoordinatorServerList::Entry entry =
        manager.service.serverList[newServerId];

    LOG(NOTICE,
        "Enlisting new server at %s (server id %lu) supporting services: %s",
        serviceLocator, newServerId.getId(), entry.services.toString().c_str());
    if (replacesId.isValid()) {
        LOG(NOTICE, "Newly enlisted server %lu replaces server %lu",
            newServerId.getId(), replacesId.getId());
    }
    if (entry.isBackup()) {
        LOG(DEBUG, "Backup at id %lu has %u MB/s read %u MB/s write",
            newServerId.getId(), readSpeed, writeSpeed);
        manager.createReplicationGroup();
    }

    return newServerId;
}

void
CoordinatorServerManager::EnlistServer::afterReply()
{
    CoordinatorServerList::Entry entry =
        manager.service.serverList[newServerId];

    if (entry.services.has(MEMBERSHIP_SERVICE))
        manager.sendServerList(newServerId);
    manager.service.serverList.sendMembershipUpdate(
        serverListUpdate, newServerId);

    // Recovery on the replaced host is deferred until the replacing host
    // has been enlisted.
    if (replacedEntry)
        manager.service.recoveryManager.startMasterRecovery(
            replacedEntry.get()->serverId);

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
        manager.service.logCabinHelper->appendProtoBuf(info, invalidates);
    manager.service.serverList.addLogCabinEntryId(newServerId, infoEntryId);
}

/**
 * Implements the part to handle enlistServer before responding to the
 * enlisting server with the serverId assigned to it.
 *
 * TODO(ankitak): Re-work after RAM-431 is resolved.
 *
 * \param ref
 *      Reference to the EnlistServer object that stores all the data
 *      required to complete this operation.
 *
 * \return
 *      Server id assigned to the enlisting server.
 */
ServerId
CoordinatorServerManager::enlistServerBeforeReply(EnlistServer& ref)
{
    return ref.beforeReply();
}

/**
 * Implements the part to handle enlistServer after responding to the
 * enlisting server with the serverId assigned to it.
 *
 * TODO(ankitak): Re-work after RAM-431 is resolved.
 *
 * \param ref
 *      Reference to the EnlistServer object that stores all the data
 *      required to complete this operation.
 */
void
CoordinatorServerManager::enlistServerAfterReply(EnlistServer& ref)
{
    ref.afterReply();
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
     state.set_opcode("HintServerDown");
     state.set_done(false);
     state.set_server_id(this->serverId.getId());

     EntryId entryId = manager.service.logCabinHelper->appendProtoBuf(state);
     LOG(DEBUG, "LogCabin entryId: %lu", entryId);

     return complete(entryId);
}

bool
CoordinatorServerManager::HintServerDown::complete(EntryId entryId)
{
     manager.service.serverList.addLogCabinEntryId(serverId, entryId);

     // If this machine has a backup and master on the same server it is best
     // to remove the dead backup before initiating recovery. Otherwise, other
     // servers may try to backup onto a dead machine which will cause delays.
     CoordinatorServerList::Entry entry = manager.service.serverList[serverId];
     ProtoBuf::ServerList update;
     manager.service.serverList.crashed(serverId, update);
     // If the server being replaced did not have a master then there
     // will be no recovery.  That means it needs to transition to
     // removed status now (usually recoveries remove servers from the
     // list when they complete).
     if (!entry.services.has(MASTER_SERVICE))
         manager.service.serverList.remove(serverId, update);
     manager.service.serverList.incrementVersion(update);

     // Update cluster membership information.
     // Backup recovery is kicked off via this update.
     // Deciding whether to place this before or after the start of master
     // recovery is difficult.
     manager.service.serverList.sendMembershipUpdate(
             update, ServerId(/* invalid id */));

     manager.service.recoveryManager.startMasterRecovery(entry.serverId);

     manager.removeReplicationGroup(entry.replicationId);
     manager.createReplicationGroup();

     // TODO(ankitak): After enlistServer starts saving state to LogCabin,
     // there will be an entry corresponding to the server information.
     // At this point, that entry will be invalidated from LogCabin,
     // along with invalidating the StateHintServerDown entry appended in the
     // execute() function call. The following state append will not be needed.
     ProtoBuf::StateHintServerDown state;
     state.set_opcode("HintServerDown");
     state.set_done(true);
     state.set_server_id(this->serverId.getId());

     entryId = manager.service.logCabinHelper->appendProtoBuf(
                    state, vector<EntryId>(entryId));
     LOG(DEBUG, "LogCabin entryId: %lu", entryId);

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
    if (!service.serverList.contains(serverId)) {
        LOG(WARNING, "Could not send list to unknown server %lu", *serverId);
        return;
    }

    if (service.serverList[serverId].status != ServerStatus::UP) {
        LOG(WARNING, "Could not send list to crashed server %lu", *serverId);
        return;
    }

    if (!service.serverList[serverId].services.has(MEMBERSHIP_SERVICE)) {
        LOG(WARNING, "Could not send list to server without membership "
            "service: %lu", *serverId);
        return;
    }

    LOG(DEBUG, "Sending server list to server id %lu as requested", *serverId);

    ProtoBuf::ServerList serializedServerList;
    service.serverList.serialize(serializedServerList);

    MembershipClient::setServerList(service.context, serverId,
        serializedServerList);
}

void
CoordinatorServerManager::SetMinOpenSegmentId::execute()
{
    // TODO(ankitak): After enlistServer starts saving state to LogCabin,
    // there will be an entry corresponding to the server information.
    // At this point, that entry will be read from LogCabin, the information
    // updated with this setMinOpenSegmentId, and then appended to the log.
    // We will no longer need a state entry for SetMinOpenSegmentId.

    ProtoBuf::StateSetMinOpenSegmentId state;
    state.set_opcode("SetMinOpenSegmentId");
    state.set_done(true);
    state.set_server_id(serverId.getId());
    state.set_segment_id(segmentId);

    EntryId entryId = manager.service.logCabinHelper->appendProtoBuf(state);
    LOG(DEBUG, "LogCabin entryId: %lu", entryId);

    complete(entryId);
}

void
CoordinatorServerManager::SetMinOpenSegmentId::complete(EntryId entryId)
{
    try {
        // Update local state.
        manager.service.serverList.addLogCabinEntryId(serverId, entryId);
        manager.service.serverList.setMinOpenSegmentId(serverId, segmentId);
    } catch (const CoordinatorServerList::NoSuchServer& e) {
        LOG(WARNING, "setMinOpenSegmentId server doesn't exist: %lu",
            serverId.getId());
        manager.service.logCabinLog->invalidate(vector<EntryId>(entryId));
        throw CoordinatorServerList::NoSuchServer(e);
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
 * Set the minOpenSegmentId of the server specified in the state Protobuf
 * to the segmentId specified in the state Protobuf.
 *
 * \param state
 *      The ProtoBuf that encapsulates the state of the setMinOpenSegmentId
 *      operation to be recovered.
 * \param entryId
 *      The entry id of the LogCabin entry corresponding to the state.
 */
void
CoordinatorServerManager::setMinOpenSegmentIdRecover(
    ProtoBuf::StateSetMinOpenSegmentId* state, EntryId entryId)
{
    Lock _(mutex);
    ServerId serverId = ServerId(state->server_id());
    uint64_t segmentId = state->segment_id();
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
    PingRpc2 pingRpc(service.context, serverId, ServerId());
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
