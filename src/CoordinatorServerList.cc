/* Copyright (c) 2011-2012 Stanford University
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

#include <list>

#include "Common.h"
#include "ClientException.h"
#include "CoordinatorServerList.h"
#include "Cycles.h"
#include "LogCabinHelper.h"
#include "MasterRecoveryManager.h"
#include "ServerTracker.h"
#include "ShortMacros.h"
#include "TransportManager.h"
#include "Context.h"

namespace RAMCloud {

/**
 * Constructor for CoordinatorServerList.
 *
 * \param context
 *      Overall information about the RAMCloud server.  The constructor
 *      will modify \c context so that its \c serverList and
 *      \c coordinatorServerList members refer to this object.
 */
CoordinatorServerList::CoordinatorServerList(Context* context)
    : AbstractServerList(context)
    , serverList()
    , numberOfMasters(0)
    , numberOfBackups(0)
    , stopUpdater(true)
    , lastScan()
    , update()
    , updates()
    , hasUpdatesOrStop()
    , listUpToDate()
    , updaterThread()
    , minConfirmedVersion(0)
    , numUpdatingServers(0)
    , nextReplicationId(1)
    , logIdAppendServerAlive(NO_ID)
    , logIdServerListVersion(NO_ID)
    , logIdServerUpUpdate(NO_ID)
    , logIdServerReplicationUpUpdate(NO_ID)
{
    context->coordinatorServerList = this;
    startUpdater();
}

/**
 * Destructor for CoordinatorServerList.
 */
CoordinatorServerList::~CoordinatorServerList()
{
    haltUpdater();
}

//////////////////////////////////////////////////////////////////////
// CoordinatorServerList Public Methods
//////////////////////////////////////////////////////////////////////

/**
 * Get the number of backups in the list; does not include servers in
 * crashed status.
 */
uint32_t
CoordinatorServerList::backupCount() const
{
    Lock _(mutex);
    return numberOfBackups;
}

/**
 * Implements enlisting a server onto the CoordinatorServerList and
 * propagating updates to the cluster.
 *
 * \param replacesId
 *      Server id of the server that the enlisting server is replacing.
 *      A null value means that the enlisting server is not replacing another
 *      server.
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
CoordinatorServerList::enlistServer(
    ServerId replacesId, ServiceMask serviceMask, const uint32_t readSpeed,
    const char* serviceLocator)
{
    Lock lock(mutex);

    // The order of the updates in serverListUpdate is important: the remove
    // must be ordered before the add to ensure that as members apply the
    // update they will see the removal of the old server id before the
    // addition of the new, replacing server id.

    if (iget(replacesId)) {
        LOG(NOTICE, "%s is enlisting claiming to replace server id "
            "%s, which is still in the server list, taking its word "
            "for it and assuming the old server has failed",
            serviceLocator, replacesId.toString().c_str());

        ServerNeedsRecovery(*this, lock, replacesId).execute();
        ServerCrashed(*this, lock, replacesId, version + 1).execute();
    }

    // Indicate that the next server enlistment would have to send out "UP"
    // updates to the cluster.
    // However:
    // I can skip this step if such a log entry is pointed to by the csl's
    // logIdServerUpUpdate.
    // Because:
    // This log entry is is not specific to a particular server that is
    // enlisting, rather just meant for the "next" server enlisting.
    // If it were meant for a particular server, it would be pointed to
    // by the server entry's logIdServerUpUpdate, not csl's logIdServerUpUpdate.
    // This situation an arise if the coordinator was in the middle of
    // an enlistServer() operation (it had completed ServerUpUpdate::execute(),
    // but not EnlistServer::complete()) when it last crashed.
    // Reusing such an entry also helps prevent dangling ServerUpUpdate
    // log entries.
    if (logIdServerUpUpdate == NO_ID) {
        ServerUpUpdate(*this, lock).execute();
    }

    // Enlist the server.
    ServerId newServerId = EnlistServer(*this, lock, ServerId(),
                                        serviceMask, readSpeed,
                                        serviceLocator, version + 1).execute();

    if (replacesId.isValid()) {
        LOG(NOTICE, "Newly enlisted server %s replaces server %s",
                    newServerId.toString().c_str(),
                    replacesId.toString().c_str());
    }

    return newServerId;
}

/**
 * Get the number of masters in the list; does not include servers in
 * crashed status.
 */
uint32_t
CoordinatorServerList::masterCount() const
{
    Lock _(mutex);
    return numberOfMasters;
}

/**
 * Returns a copy of the details associated with the given ServerId.
 *
 * Note: This function explictly acquires a lock, and is hence to be used
 * only by functions external to CoordinatorServerList to prevent deadlocks.
 * If a function in CoordinatorServerList class (that has already acquired
 * a lock) wants to use this functionality, it should directly call
 * #getEntry function.
 *
 * \param serverId
 *      ServerId to look up in the list.
 * \throw
 *      Exception is thrown if the given ServerId is not in this list.
 */
CoordinatorServerList::Entry
CoordinatorServerList::operator[](ServerId serverId) const
{
    Lock _(mutex);
    Entry* entry = getEntry(serverId);

    if (!entry) {
        throw ServerListException(HERE,
                format("Invalid ServerId (%s)", serverId.toString().c_str()));
    }

    return *entry;
}

/**
 * Returns a copy of the details associated with the given position
 * in the server list.
 *
 * Note: This function explictly acquires a lock, and is hence to be used
 * only by functions external to CoordinatorServerList to prevent deadlocks.
 * If a function in CoordinatorServerList class (that has already acquired
 * a lock) wants to use this functionality, it should directly call
 * #getEntry function.
 *
 * \param index
 *      Position of entry in the server list to return a copy of.
 * \throw
 *      Exception is thrown if the position in the list is unoccupied.
 */
CoordinatorServerList::Entry
CoordinatorServerList::operator[](size_t index) const
{
    Lock _(mutex);
    Entry* entry = getEntry(index);

    if (!entry) {
        throw ServerListException(HERE,
            format("Index beyond array length (%zd) or entry"
                   "doesn't exist", index));
    }

    return *entry;
}

/**
 * Mark a server as REMOVE, typically when it is no longer part of
 * the system and we don't care about it anymore (it crashed and has
 * been properly recovered).
 * 
 * When the update sent to and acknowledged by the rest of the cluster
 * is being pruned, the server list entry for this server will be removed.
 *
 * This method may actually append two entries to \a update (see below).
 *
 * The result of this operation will be added in the class's update Protobuffer
 * intended for the cluster. To send out the update, call pushUpdate()
 * which will also increment the version number. Calls to remove()
 * and crashed() must proceed call to add() to ensure ordering guarantees
 * about notifications related to servers which re-enlist.
 *
 * The addition will be pushed to all registered trackers and those with
 * callbacks will be notified.
 *
 * \param serverId
 *      The ServerId of the server to remove from the CoordinatorServerList.
 *      It must be in the list (either UP or CRASHED).
 */
void
CoordinatorServerList::recoveryCompleted(ServerId serverId)
{
    Lock lock(mutex);
    ServerRemoveUpdate(*this, lock, serverId, version + 1).execute();
}

/**
 * Serialize this list (or part of it, depending on which services the
 * caller wants) to a protocol buffer. Not all state is included, but
 * enough to be useful for disseminating cluster membership information
 * to other servers.
 *
 * \param[out] protoBuf
 *      The ProtoBuf to fill.
 * \param services
 *      If a server has *any* service included in \a services it will be
 *      included in the serialization; otherwise, it is skipped.
 */
void
CoordinatorServerList::serialize(ProtoBuf::ServerList* protoBuf,
                                 ServiceMask services) const
{
    Lock lock(mutex);
    serialize(lock, protoBuf, services);
}

/**
 * This method is invoked when a server is determined to have crashed.
 * It marks the server as crashed, propagates that information
 * (through server trackers and the cluster updater) and invokes recovery.
 * It returns before the recovery has completed.
 * Once recovery has finished, the server will be removed from the server list.
 *
 * \param serverId
 *      ServerId of the server that is suspected to be down.
 */
void
CoordinatorServerList::serverCrashed(ServerId serverId)
{
    Lock lock(mutex);

    // Indicate that the crashed server needs to be recovered.
    // However:
    // I can skip this step if such a log entry already exists.
    // This situation can arise if the coordinator was in the middle of
    // a crashedServer() operation (it had completed
    // ServerNeedsRecovery::execute(), but not ServerCrashed::complete())
    // or had completed serverCrashed() not completed the recovery for
    // the crashed server when it last crashed.
    // Reusing this log entry also helps prevent having multiple
    // ServerNeedsRecovery log entries for crashed servers.
    Entry* entry = getEntry(serverId);
    if (entry && entry->logIdServerNeedsRecovery == NO_ID) {
        ServerNeedsRecovery(*this, lock, serverId).execute();
    }

    // Remove the crashed server from the cluster.
    ServerCrashed(*this, lock, serverId, version + 1).execute();
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
 *      A copy is made of the contents of this structure
 * \return
 *      Whether the operation succeeded or not (i.e. if the serverId exists).
 */
bool
CoordinatorServerList::setMasterRecoveryInfo(
    ServerId serverId, const ProtoBuf::MasterRecoveryInfo* recoveryInfo)
{
    Lock lock(mutex);
    Entry* entry = getEntry(serverId);

    if (entry) {
        entry->masterRecoveryInfo = *recoveryInfo;
        ServerUpdate(*this, lock,
                     serverId, recoveryInfo,
                     entry->logIdServerUpdate).execute();
        return true;
    } else {
        return false;
    }
}

//////////////////////////////////////////////////////////////////////
// CoordinatorServerList Recovery Methods
//////////////////////////////////////////////////////////////////////

/**
 * Complete a ServerCrashed during coordinator recovery.
 *
 * \param state
 *      The ProtoBuf that encapsulates the state of the ServerCrashed
 *      operation to be recovered.
 * \param logIdServerCrashed
 *      The entry id of the LogCabin entry corresponding to the state.
 */
void
CoordinatorServerList::recoverServerCrashed(
    ProtoBuf::ServerCrashInfo* state, EntryId logIdServerCrashed)
{
    Lock lock(mutex);
    LOG(DEBUG, "CoordinatorServerList::recoverServerCrashed()");
    ServerCrashed(*this, lock,
                  ServerId(state->server_id()),
                  state->update_version()).complete(logIdServerCrashed);
}

void
CoordinatorServerList::recoverServerListVersion(
    ProtoBuf::ServerListVersion* state, EntryId logIdServerListVersion)
{
    Lock lock(mutex);

    uint64_t version = state->version();
    PersistServerListVersion(*this, lock, version).complete(
            logIdServerListVersion);

    // When coordinator recovers, all the server list entries created
    // corresponding to all the servers, will have a verified version and
    // update version of 0. (Since it It will be too expensive to log the
    // most up-to-date value of verified version for each entry during
    // normal operation and then recover that later.)
    // This would result in the entire server list being sent out to all
    // the servers. This is not expected behavior (and the servers will
    // shoot themselves in the head).
    // Hence:
    // Set these versions for every server entry in server list to be the
    // ServerListVersion number that was last logged to LogCabin
    // (since which was the minimum verified update number).
    // This way, some servers might still get some updates they had already
    // received, but the number will hopefully be low, and they will at least
    // never receive the entire server list again.
    for (size_t index = 0; index < isize(); index++) {
        Entry* entry = getEntry(index);
        if (entry != NULL) {
            entry->verifiedVersion = version;
            entry->updateVersion = version;
        }
    }
}

/**
 * During coordinator recovery, record in server list that for this server
 * (that had crashed), we need to start the master crash recovery. 
 *
 * \param state
 *      The ProtoBuf that encapsulates the state of the ServerNeedsRecovery
 *      operation to be recovered.
 * \param logIdServerNeedsRecovery
 *      The entry id of the LogCabin entry corresponding to the state.
 */
void
CoordinatorServerList::recoverServerNeedsRecovery(
    ProtoBuf::ServerCrashInfo* state, EntryId logIdServerNeedsRecovery)
{
    Lock lock(mutex);
    LOG(DEBUG, "CoordinatorServerList::recoverServerNeedsRecovery()");
    ServerNeedsRecovery(*this, lock,
                        ServerId(state->server_id())).complete(
                                        logIdServerNeedsRecovery);
}

/**
 * During coordinator recovery, propagate REMOVE updates for a crashed server
 * whose recovery had already completed.
 *
 * \param state
 *      The ProtoBuf that encapsulates the state of the ServerRemoveUpdate
 *      operation to be recovered.
 * \param logIdServerRemoveUpdate
 *      The entry id of the LogCabin entry corresponding to the state.
 */
void
CoordinatorServerList::recoverServerRemoveUpdate(
    ProtoBuf::ServerCrashInfo* state, EntryId logIdServerRemoveUpdate)
{
    Lock lock(mutex);
    LOG(DEBUG, "CoordinatorServerList::recoverServerRemoveUpdate()");
    ServerRemoveUpdate(*this, lock,
                       ServerId(state->server_id()),
                       state->update_version()).complete(
                                        logIdServerRemoveUpdate);
}

/**
 * During Coordinator recovery, enlist a server that was either being enlisted
 * at the time of crash, or had already successfully enlisted.
 *
 * \param state
 *      The ProtoBuf that encapsulates the information about the server to be
 *      enlisted.
 * \param logIdServerUp
 *      The entry id of the LogCabin entry corresponding to the state.
 */
void
CoordinatorServerList::recoverServerUp(
    ProtoBuf::ServerInformation* state, EntryId logIdServerUp)
{
    Lock lock(mutex);
    LOG(DEBUG, "CoordinatorServerList::recoverServerUp()");
    EnlistServer(*this, lock,
                 ServerId(state->server_id()),
                 ServiceMask::deserialize(state->service_mask()),
                 state->read_speed(),
                 state->service_locator().c_str(),
                 state->update_version()).complete(logIdServerUp);
}

/**
 * During Coordinator recovery, recover the entry that indicates that the
 * next server enlistment will have to send out "UP" updates.
 *
 * \param state
 *      The ProtoBuf that indicates that the server enlistment will have to
 *      send out "UP" updates.
 * \param logIdServerUpUpdate
 *      The entry id of the LogCabin entry corresponding to the state.
 */
void
CoordinatorServerList::recoverServerUpUpdate(
    ProtoBuf::EntryType* state, EntryId logIdServerUpUpdate)
{
    Lock lock(mutex);
    LOG(DEBUG, "CoordinatorServerList::recoverServerUpUpdate()");
    ServerUpUpdate(*this, lock).complete(logIdServerUpUpdate);
}

/**
 * During Coordinator recovery, recover the entry that indicates that the
 * replication id update will have to send out updates to the entire cluster.
 *
 * \param state
 *      The ProtoBuf that indicates that the coordinator will need to send
 *      out replication id updates.
 * \param logIdServerReplicationUpUpdate
 *      The entry id of the LogCabin entry corresponding to the state.
 */
void
CoordinatorServerList::recoverServerReplicationUpUpdate(
    ProtoBuf::EntryType* state, EntryId logIdServerReplicationUpUpdate)
{
    Lock lock(mutex);
    LOG(DEBUG, "CoordinatorServerList::recoverServerReplicationUpUpdate()");
    ServerReplicationUpUpdate(*this, lock).complete(
        logIdServerReplicationUpUpdate);
}

/**
 * During Coordinator recovery, set update-able fields for the server.
 *
 * \param state
 *      The ProtoBuf that has the updates for the server.
 * \param logIdServerUpdate
 *      The entry id of the LogCabin entry corresponding to serverUpdate.
 */
void
CoordinatorServerList::recoverServerUpdate(
    ProtoBuf::ServerUpdate* state, EntryId logIdServerUpdate)
{
    Lock lock(mutex);
    LOG(DEBUG, "CoordinatorServerList::recoverServerUpdate()");
    // If there are other update-able fields in the future, read them in from
    // ServerUpdate and update them all.
    ServerUpdate(*this, lock, ServerId(state->server_id()),
                 &state->master_recovery_info()).complete(logIdServerUpdate);
}

/**
 * During Coordinator recovery, set update-able fields for the server.
 *
 * \param state
 *      The ProtoBuf that has the updates for the server.
 * \param logIdServerReplicationUpdate
 *      The entry id of the LogCabin entry corresponding to
 *      serverReplicationUpdate.
 */
void
CoordinatorServerList::recoverServerReplicationUpdate(
    ProtoBuf::ServerReplicationUpdate* state,
    EntryId logIdServerReplicationUpdate)
{
    Lock lock(mutex);
    LOG(DEBUG, "CoordinatorServerList::recoverServerReplicationUpdate()");
    // If there are other update-able fields in the future, read them in from
    // ServerReplicationUpdate and update them all.
    ServerReplicationUpdate(*this, lock, ServerId(state->server_id()),
                           &state->master_recovery_info(),
                           state->replication_id(),
                           version + 1).complete(logIdServerReplicationUpdate);
}

//////////////////////////////////////////////////////////////////////
// CoordinatorServerList Private Methods
//////////////////////////////////////////////////////////////////////

/**
 * Do everything needed to execute the EnlistServer operation.
 * Do any processing required before logging the state
 * in LogCabin, log the state in LogCabin, then call #complete().
 */
ServerId
CoordinatorServerList::EnlistServer::execute()
{
    newServerId = csl.generateUniqueId(lock);

    ProtoBuf::ServerInformation stateServerUp;
    stateServerUp.set_entry_type("ServerUp");
    stateServerUp.set_server_id(newServerId.getId());
    stateServerUp.set_service_mask(serviceMask.serialize());
    stateServerUp.set_read_speed(readSpeed);
    stateServerUp.set_service_locator(string(serviceLocator));
    stateServerUp.set_update_version(updateVersion);

    EntryId logIdServerUp =
        csl.context->logCabinHelper->appendProtoBuf(
                *csl.context->expectedEntryId, stateServerUp);
    LOG(DEBUG, "LogCabin: ServerUp entryId: %lu", logIdServerUp);

    return complete(logIdServerUp);
}

/**
 * Complete the EnlistServer operation after its state has been
 * logged in LogCabin.
 * This is called internally by #execute() in case of normal operation
 * (which is in turn called by #enlistServer()), and
 * directly for coordinator recovery (by #recoverEnlistServer()).
 *
 * \param logIdServerUp
 *      The entry id of the LogCabin entry that has initial information
 *      for this server.
 */
ServerId
CoordinatorServerList::EnlistServer::complete(
        EntryId logIdServerUp)
{
    if (csl.logIdServerUpUpdate == NO_ID) {
        // Server had already been successfully enlisted (presumably before a
        // coordinator crash) -- hence its "UP" updates had been sent out to the
        // cluster and acknowledged. Just add the entry to the coordinator
        // server list, and do not send updates to the cluster.
        csl.add(lock, newServerId, serviceLocator, serviceMask, readSpeed,
                false);
    } else {
        csl.add(lock, newServerId, serviceLocator, serviceMask, readSpeed);
        csl.version = updateVersion;
        csl.pushUpdate(lock, updateVersion);
    }

    Entry* entry = csl.getEntry(newServerId);
    entry->logIdServerUp = logIdServerUp;

    // No-op if csl.logIdServerUpUpdate is already NO_ID.
    entry->logIdServerUpUpdate = csl.logIdServerUpUpdate;
    csl.logIdServerUpUpdate = NO_ID;

    LOG(NOTICE, "Enlisting server at %s (server id %s) supporting "
        "services: %s", serviceLocator, newServerId.toString().c_str(),
        entry->services.toString().c_str());

    if (entry->isBackup()) {
        LOG(DEBUG, "Backup at id %s has %u MB/s read",
            newServerId.toString().c_str(), readSpeed);
        csl.createReplicationGroup(lock);
    }

    return newServerId;
}

void
CoordinatorServerList::PersistServerListVersion::execute()
{
    ProtoBuf::ServerListVersion state;
    state.set_entry_type("ServerListVersion");
    state.set_version(version);

    vector<EntryId> invalidates;
    if (csl.logIdServerListVersion != NO_ID)
        invalidates.push_back(csl.logIdServerListVersion);
    EntryId entryId =
        csl.context->logCabinHelper->appendProtoBuf(
            *csl.context->expectedEntryId, state, invalidates);
    LOG(DEBUG, "LogCabin: ServerListVersion entryId: %lu", entryId);

    complete(entryId);
}

void
CoordinatorServerList::PersistServerListVersion::complete(
        EntryId logIdServerListVersion)
{
    csl.version = version;
    csl.logIdServerListVersion = logIdServerListVersion;
}

/**
 * Do everything needed to remove a server from the cluster.
 * Do any processing required before logging the state
 * in LogCabin, log the state in LogCabin, then call #complete().
 */
void
CoordinatorServerList::ServerCrashed::execute()
{
    if (!csl.getEntry(serverId)) {
        throw ServerListException(HERE,
            format("Invalid ServerId (%s)", serverId.toString().c_str()));
    }

    ProtoBuf::ServerCrashInfo state;
    state.set_entry_type("ServerCrashed");
    state.set_server_id(serverId.getId());
    state.set_update_version(updateVersion);

    EntryId entryId =
        csl.context->logCabinHelper->appendProtoBuf(
            *csl.context->expectedEntryId, state);
    LOG(DEBUG, "LogCabin: ServerCrashed entryId: %lu", entryId);

    complete(entryId);
}

/**
 * Complete the operation to remove a server from the cluster
 * after its state has been logged in LogCabin.
 * This is called internally by #execute() in case of normal operation, and
 * directly for coordinator recovery (by #recoverServerCrashed()).
 *
 * \param entryId
 *      The entry id of the LogCabin entry corresponding to the state
 *      of the operation to be completed.
 */
void
CoordinatorServerList::ServerCrashed::complete(EntryId entryId)
{
    csl.crashed(lock, serverId);
    csl.version = updateVersion;
    csl.pushUpdate(lock, updateVersion);

    // If this machine has a backup and master on the same server it is best
    // to remove the dead backup before initiating recovery. Otherwise, other
    // servers may try to backup onto a dead machine which will cause delays.
    Entry* entry = csl.getEntry(serverId);
    entry->logIdServerCrashed = entryId;

    if (entry->needsRecovery) {
        if (!entry->services.has(WireFormat::MASTER_SERVICE)) {
            // If the server being replaced did not have a master then there
            // will be no recovery.  That means it needs to transition to
            // removed status now (usually recoveries remove servers from the
            // list when they complete).
            CoordinatorServerList::ServerRemoveUpdate(
                            csl, lock, serverId, ++csl.version).execute();
        }

        csl.context->recoveryManager->startMasterRecovery(*entry);
    } else {
        // Don't start recovery when the coordinator is replaying a serverDown
        // log entry for a server that had already been recovered.
    }

    csl.removeReplicationGroup(lock, entry->replicationId);
    csl.createReplicationGroup(lock);
}

/**
 * Indicate that a crashed server needs to be recovered.
 * Log the state in LogCabin, then call #complete().
 */
void
CoordinatorServerList::ServerNeedsRecovery::execute()
{
    if (!csl.getEntry(serverId)) {
        throw ServerListException(HERE,
            format("Invalid ServerId (%s)", serverId.toString().c_str()));
    }

    ProtoBuf::ServerCrashInfo state;
    state.set_entry_type("ServerNeedsRecovery");
    state.set_server_id(serverId.getId());

    EntryId entryId =
        csl.context->logCabinHelper->appendProtoBuf(
            *csl.context->expectedEntryId, state);
    LOG(DEBUG, "LogCabin: ServerNeedsRecovery entryId: %lu", entryId);

    complete(entryId);
}

/**
 * Complete the operation to indicate that a crashed server needs to be
 * recovered after its state has been logged in LogCabin.
 * This is called internally by #execute() in case of normal operation, and
 * directly for coordinator recovery (by #recoverServerNeedsRecovery()).
 *
 * \param entryId
 *      The entry id of the LogCabin entry corresponding to the state
 *      of the operation to be completed.
 */
void
CoordinatorServerList::ServerNeedsRecovery::complete(EntryId entryId)
{
    Entry* entry = csl.getEntry(serverId);

    if (!entry) {
        LOG(WARNING, "Server being updated doesn't exist: %s",
            serverId.toString().c_str());
        csl.context->logCabinHelper->invalidate(
            *csl.context->expectedEntryId, vector<EntryId>(entryId));
        return;
    }

    entry->needsRecovery = true;
    entry->logIdServerNeedsRecovery = entryId;
}

/**
 * Indicate that a crashed server needs to be recovered.
 * Log the state in LogCabin, then call #complete().
 */
void
CoordinatorServerList::ServerRemoveUpdate::execute()
{
    Entry* entry = csl.getEntry(serverId);
    if (!entry) {
        throw ServerListException(HERE,
            format("Invalid ServerId (%s)", serverId.toString().c_str()));
    }

    ProtoBuf::ServerCrashInfo state;
    state.set_entry_type("ServerRemoveUpdate");
    state.set_server_id(serverId.getId());
    state.set_update_version(updateVersion);

    vector<EntryId> invalidates;
    if (entry->logIdServerNeedsRecovery)
        invalidates.push_back(entry->logIdServerNeedsRecovery);

    EntryId entryId =
            csl.context->logCabinHelper->appendProtoBuf(
                *csl.context->expectedEntryId, state, invalidates);
    LOG(DEBUG, "LogCabin: ServerRemoveUpdate entryId: %lu", entryId);

    complete(entryId);
}

/**
 * Complete the operation to indicate that a crashed server needs to be
 * recovered after its state has been logged in LogCabin.
 * This is called internally by #execute() in case of normal operation, and
 * directly for coordinator recovery (by #recoverServerRemoveUpdate()).
 *
 * \param entryId
 *      The entry id of the LogCabin entry corresponding to the state
 *      of the operation to be completed.
 */
void
CoordinatorServerList::ServerRemoveUpdate::complete(EntryId entryId)
{
    Entry* entry = csl.getEntry(serverId);

    if (!entry) {
        LOG(WARNING, "Server being updated doesn't exist: %s",
            serverId.toString().c_str());
        csl.context->logCabinHelper->invalidate(
            *csl.context->expectedEntryId, vector<EntryId>(entryId));
        return;
    }

    entry->logIdServerRemoveUpdate = entryId;

    // This is a no-op if server was already marked as CRASHED.
    csl.crashed(lock, serverId);
    // Setting state gets the serialized update message's state field correct.
    entry->status = ServerStatus::REMOVE;
    LOG(NOTICE, "Removing %s from cluster/coordinator server list",
        serverId.toString().c_str());

    ProtoBuf::ServerList_Entry& protoBufEntry(*(csl.update).add_server());
    entry->serialize(&protoBufEntry);

    foreach (ServerTrackerInterface* tracker, csl.trackers)
        tracker->enqueueChange(*entry, ServerChangeEvent::SERVER_REMOVED);
    foreach (ServerTrackerInterface* tracker, csl.trackers)
        tracker->fireCallback();

    csl.version = updateVersion;
    csl.pushUpdate(lock, updateVersion);
}

/**
 * Do everything needed to set update-able fields corresponding to a server.
 * Do any processing required before logging the state to LogCabin,
 * log the state in LogCabin, then call #complete().
 */
void
CoordinatorServerList::ServerUpdate::execute()
{
    ProtoBuf::ServerUpdate serverUpdate;
    serverUpdate.set_entry_type("ServerUpdate");
    serverUpdate.set_server_id(serverId.getId());
    (*serverUpdate.mutable_master_recovery_info()) = recoveryInfo;

    vector<EntryId> invalidates;
    if (oldServerUpdateEntryId != NO_ID)
        invalidates.push_back(oldServerUpdateEntryId);

    EntryId newEntryId =
        csl.context->logCabinHelper->appendProtoBuf(
            *csl.context->expectedEntryId, serverUpdate, invalidates);
    LOG(DEBUG, "LogCabin: ServerUpdate entryId: %lu", newEntryId);

    complete(newEntryId);
}

/**
 * Complete the operation to set update-able fields corresponding to a server
 * after its state has been logged to LogCabin.
 *
 * \param entryId
 *      The entry id of the LogCabin entry corresponding to the state
 *      of the operation to be completed.
 */
void
CoordinatorServerList::ServerUpdate::complete(EntryId entryId)
{
    Entry* entry = csl.getEntry(serverId);

    if (entry) {
        entry->masterRecoveryInfo = recoveryInfo;
        entry->logIdServerUpdate = entryId;
    } else {
        LOG(WARNING, "Server being updated doesn't exist: %s",
            serverId.toString().c_str());
        csl.context->logCabinHelper->invalidate(
            *csl.context->expectedEntryId, vector<EntryId>(entryId));
    }
}

/**
 * Do everything needed to set update-able fields corresponding to a server.
 * Do any processing required before logging the state to LogCabin,
 * log the state in LogCabin, then call #complete().
 */
void
CoordinatorServerList::ServerReplicationUpdate::execute()
{
    ProtoBuf::ServerReplicationUpdate serverReplicationUpdate;
    serverReplicationUpdate.set_entry_type("ServerReplicationUpdate");
    serverReplicationUpdate.set_replication_id(replicationId);
    serverReplicationUpdate.set_server_id(serverId.getId());
    (*serverReplicationUpdate.mutable_master_recovery_info()) = recoveryInfo;

    vector<EntryId> invalidates;
    if (oldServerReplicationUpdateEntryId != NO_ID)
        invalidates.push_back(oldServerReplicationUpdateEntryId);

    EntryId newEntryId =
        csl.context->logCabinHelper->appendProtoBuf(
            *csl.context->expectedEntryId, serverReplicationUpdate,
            invalidates);
    LOG(DEBUG, "LogCabin: ServerReplicationUpdate entryId: %lu", newEntryId);

    complete(newEntryId);
}

/**
 * Complete the operation to set update-able fields corresponding to a server
 * after its state has been logged to LogCabin.
 *
 * \param entryId
 *      The entry id of the LogCabin entry corresponding to the state
 *      of the operation to be completed.
 */
void
CoordinatorServerList::ServerReplicationUpdate::complete(EntryId entryId)
{
    Entry* entry = csl.getEntry(serverId);

    if (entry) {
        entry->masterRecoveryInfo = recoveryInfo;
        entry->logIdServerReplicationUpdate = entryId;
        entry->replicationId = replicationId;
        entry->logIdServerReplicationUpUpdate =
            csl.logIdServerReplicationUpUpdate;
        // We check to see if the replication update field is set in
        // Log Cabin. If it is, we can be sure that the replication id
        // update hasn't been sent out to the masters.
        if (csl.logIdServerReplicationUpUpdate != NO_ID) {
            ProtoBuf::ServerList_Entry& protoBufEntry(
                *(csl.update).add_server());
            entry->serialize(&protoBufEntry);
            csl.version = updateVersion;
            csl.pushUpdate(lock, updateVersion);
            csl.logIdServerReplicationUpUpdate = NO_ID;
        }
    } else {
        LOG(WARNING, "Server being updated doesn't exist: %s",
            serverId.toString().c_str());
        csl.context->logCabinHelper->invalidate(
            *csl.context->expectedEntryId, vector<EntryId>(entryId));
    }
}

/**
 * Indicate that a the next server to be enlisted has to send out "UP" updates
 * to the cluster.
 * Log the state in LogCabin, then call #complete().
 */
void
CoordinatorServerList::ServerUpUpdate::execute()
{
    ProtoBuf::EntryType state;
    state.set_entry_type("ServerUpUpdate");

    EntryId entryId =
            csl.context->logCabinHelper->appendProtoBuf(
                *csl.context->expectedEntryId, state);
    LOG(DEBUG, "LogCabin: ServerUpUpdate entryId: %lu", entryId);

    complete(entryId);
}

/**
 * Complete the operation to indicate that the next server to be enlisted
 * has to send out "UP" updates to the cluster.
 * This is called internally by #execute() in case of normal operation, and
 * directly for coordinator recovery (by #recoverServerUpUpdate()).
 *
 * \param entryId
 *      The entry id of the LogCabin entry corresponding to the state
 *      of the operation to be completed.
 */
void
CoordinatorServerList::ServerUpUpdate::complete(EntryId entryId)
{
    csl.logIdServerUpUpdate = entryId;
}

ServerDetails*
CoordinatorServerList::iget(ServerId id)
{
    return getEntry(id);
}

ServerDetails*
CoordinatorServerList::iget(uint32_t index)
{
    return (serverList[index].entry) ? serverList[index].entry.get() : NULL;
}

/**
 * Indicate that all servers have already received a replication Id update.
 * Log the state in LogCabin, then call #complete().
 */
void
CoordinatorServerList::ServerReplicationUpUpdate::execute()
{
    ProtoBuf::EntryType state;
    state.set_entry_type("ServerReplicationUpUpdate");

    EntryId entryId =
            csl.context->logCabinHelper->appendProtoBuf(
                *csl.context->expectedEntryId, state);
    LOG(DEBUG, "LogCabin: ServerReplicationUpUpdate entryId: %lu", entryId);

    complete(entryId);
}

/**
 * Complete the operation to indicate that all the servers have already
 * received a replication Id update.
 * This is called internally by #execute().
 *
 * \param entryId
 *      The entry id of the LogCabin entry corresponding to the state
 *      of the operation to be completed.
 */
void
CoordinatorServerList::ServerReplicationUpUpdate::complete(EntryId entryId)
{
    csl.logIdServerReplicationUpUpdate = entryId;
}

/**
 * Return the number of valid indexes in this list w/o lock. Valid does not mean
 * that they're occupied, only that they are within the bounds of the array.
 */
size_t
CoordinatorServerList::isize() const
{
    return serverList.size();
}

/**
 * Returns the entry corresponding to a ServerId with bounds checks.
 * Assumes caller already has CoordinatorServerList lock.
 *
 * \param id
 *      ServerId corresponding to the entry you want to get.
 * \return
 *      The Entry, null if id is invalid.
 */
CoordinatorServerList::Entry*
CoordinatorServerList::getEntry(ServerId id) const {
    uint32_t index = id.indexNumber();
    if ((index < serverList.size()) && serverList[index].entry) {
        Entry* e = const_cast<Entry*>(serverList[index].entry.get());
        if (e->serverId == id)
            return e;
    }

    return NULL;
}

/**
 * Obtain a pointer to the entry associated with the given position
 * in the server list with bounds check. Assumes caller already has
 * CoordinatorServerList lock.
 *
 * \param index
 *      Position of entry in the server list to return a copy of.
 * \return
 *      The Entry, null if index doesn't have an entry or is out of bounds
 */
CoordinatorServerList::Entry*
CoordinatorServerList::getEntry(size_t index) const {
    if ((index < serverList.size()) && serverList[index].entry) {
        Entry* e = const_cast<Entry*>(serverList[index].entry.get());
        return e;
    }

    return NULL;
}

/**
 * Add a new server to the CoordinatorServerList with a given ServerId.
 *
 * The result of this operation will be added in the class's update Protobuffer
 * intended for the cluster. To send out the update, call pushUpdate()
 * which will also increment the version number. Calls to remove()
 * and crashed() must precede call to add() to ensure ordering guarantees
 * about notifications related to servers which re-enlist.
 *
 * The addition will be pushed to all registered trackers and those with
 * callbacks will be notified.
 *
 * It doesn't acquire locks and does not send out updates
 * since it is used internally.
 *
 * \param lock
 *      Explicity needs CoordinatorServerList lock.
 * \param serverId
 *      The serverId to be assigned to the new server.
 * \param serviceLocator
 *      The ServiceLocator string of the server to add.
 * \param serviceMask
 *      Which services this server supports.
 * \param readSpeed
 *      Speed of the storage on the enlisting server if it includes a backup
 *      service. Argument is ignored otherwise.
 * \param enqueueUpdate
 *      Whether the update (to be sent to the cluster) about the enlisting
 *      server should be enqueued. This is false during coordinator recovery
 *      while replaying an AliveServer entry since it only needs local
 *      (coordinator) change, and the rest of the cluster had already
 *      acknowledged the update.
 */
void
CoordinatorServerList::add(Lock& lock,
                           ServerId serverId,
                           string serviceLocator,
                           ServiceMask serviceMask,
                           uint32_t readSpeed,
                           bool enqueueUpdate)
{
    uint32_t index = serverId.indexNumber();

    // When add is not preceded by generateUniqueId(),
    // for example, during coordinator recovery while adding a server that
    // had already enlisted before the previous coordinator leader crashed,
    // the serverList might not have space allocated for this index number.
    // So we need to resize it explicitly.
    if (index >= serverList.size())
        serverList.resize(index + 1);

    auto& pair = serverList[index];
    pair.nextGenerationNumber = serverId.generationNumber();
    pair.nextGenerationNumber++;
    pair.entry.construct(serverId, serviceLocator, serviceMask);

    if (serviceMask.has(WireFormat::MASTER_SERVICE)) {
        numberOfMasters++;
    }

    if (serviceMask.has(WireFormat::BACKUP_SERVICE)) {
        numberOfBackups++;
        pair.entry->expectedReadMBytesPerSec = readSpeed;
    }

    if (enqueueUpdate) {
        ProtoBuf::ServerList_Entry& protoBufEntry(*update.add_server());
        pair.entry->serialize(&protoBufEntry);

        foreach (ServerTrackerInterface* tracker, trackers)
            tracker->enqueueChange(*pair.entry,
                                   ServerChangeEvent::SERVER_ADDED);
        foreach (ServerTrackerInterface* tracker, trackers)
            tracker->fireCallback();
    }
}

/**
 * Mark a server as crashed in the list (when it has crashed and is
 * being recovered and resources [replicas] for its recovery must be
 * retained).
 *
 * This is a no-op if the server is already marked as crashed;
 * the effect is undefined if the server's status is REMOVE.
 *
 * The result of this operation will be added in the class's update Protobuffer
 * intended for the cluster. To send out the update, call pushUpdate()
 * which will also increment the version number. Calls to remove()
 * and crashed() must proceed call to add() to ensure ordering guarantees
 * about notifications related to servers which re-enlist.
 *
 * It doesn't acquire locks and does not send out updates
 * since it is used internally.
 *
 * The addition will be pushed to all registered trackers and those with
 * callbacks will be notified.
 *
 * \param lock
 *      Explicity needs CoordinatorServerList lock.
 * \param serverId
 *      The ServerId of the server to remove from the CoordinatorServerList.
 *      It must not have been removed already (see remove()).
 */

void
CoordinatorServerList::crashed(const Lock& lock,
                               ServerId serverId)
{
    Entry* entry = getEntry(serverId);

    if (!entry) {
        throw ServerListException(HERE,
            format("Invalid ServerId (%s)", serverId.toString().c_str()));
    }

    if (entry->status == ServerStatus::CRASHED)
        return;
    assert(entry->status != ServerStatus::REMOVE);

    if (entry->isMaster())
        numberOfMasters--;
    if (entry->isBackup())
        numberOfBackups--;

    entry->status = ServerStatus::CRASHED;

    ProtoBuf::ServerList_Entry& protoBufEntry(*update.add_server());
    entry->serialize(&protoBufEntry);

    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->enqueueChange(*entry, ServerChangeEvent::SERVER_CRASHED);
    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->fireCallback();
}

/**
 * Return the first free index in the server list. If the list is
 * completely full, resize it and return the next free one.
 *
 * Note that index 0 is reserved. This method must never return it.
 */
uint32_t
CoordinatorServerList::firstFreeIndex()
{
    // Naive, but probably fast enough for a good long while.
    size_t index;
    for (index = 1; index < serverList.size(); index++) {
        if (!serverList[index].entry)
            break;
    }

    if (index >= serverList.size())
        serverList.resize(index + 1);

    assert(index != 0);
    return downCast<uint32_t>(index);
}

/**
 * Generate a new, unique ServerId that may later be assigned to a server
 * using add().
 *
 * \param lock
 *      Explicity needs CoordinatorServerList lock.
 * \return
 *      The unique ServerId generated.
 */
ServerId
CoordinatorServerList::generateUniqueId(Lock& lock)
{
    uint32_t index = firstFreeIndex();

    auto& pair = serverList[index];
    ServerId id(index, pair.nextGenerationNumber);
    pair.nextGenerationNumber++;
    pair.entry.construct(id, "", ServiceMask());

    return id;
}

/**
 * Serialize the entire list to a Protocol Buffer form. Only used internally in
 * CoordinatorServerList; requires a lock on #mutex is held for duration of call.
 *
 * \param lock
 *      Unused, but required to statically check that the caller is aware that
 *      a lock must be held on #mutex for this call to be safe.
 * \param[out] protoBuf
 *      The ProtoBuf to fill.
 */
void
CoordinatorServerList::serialize(const Lock& lock,
                                 ProtoBuf::ServerList* protoBuf) const
{
    serialize(lock, protoBuf, {WireFormat::MASTER_SERVICE,
        WireFormat::BACKUP_SERVICE});
}

/**
 * Serialize this list (or part of it, depending on which services the
 * caller wants) to a protocol buffer. Not all state is included, but
 * enough to be useful for disseminating cluster membership information
 * to other servers. Only used internally in CoordinatorServerList; requires
 * a lock on #mutex is held for duration of call.
 *
 * All entries are serialize to the protocol buffer in the order they appear
 * in the server list. The order has some important implications. See
 * ServerList::applyServerList() for details.
 *
 * \param lock
 *      Unused, but required to statically check that the caller is aware that
 *      a lock must be held on #mutex for this call to be safe.
 * \param[out] protoBuf
 *      The ProtoBuf to fill.
 * \param services
 *      If a server has *any* service included in \a services it will be
 *      included in the serialization; otherwise, it is skipped.
 */
void
CoordinatorServerList::serialize(const Lock& lock,
                                 ProtoBuf::ServerList* protoBuf,
                                 ServiceMask services) const
{
    for (size_t i = 0; i < serverList.size(); i++) {
        if (!serverList[i].entry)
            continue;

        const Entry& entry = *serverList[i].entry;

        if ((entry.services.has(WireFormat::MASTER_SERVICE) &&
             services.has(WireFormat::MASTER_SERVICE)) ||
            (entry.services.has(WireFormat::BACKUP_SERVICE) &&
             services.has(WireFormat::BACKUP_SERVICE)))
        {
            ProtoBuf::ServerList_Entry& protoBufEntry(*protoBuf->add_server());
            entry.serialize(&protoBufEntry);
        }
    }

    protoBuf->set_version_number(version);
    protoBuf->set_type(ProtoBuf::ServerList_Type_FULL_LIST);
}

/**
 * Assign a new replicationId to a backup, and inform the backup which nodes
 * are in its replication group.
 *
 * \param lock
 *      Explicity needs CoordinatorServerList lock.
 * \param replicationId
 *      New replication group Id that is assigned to backup.
 * \param replicationGroupIds
 *      Includes the ServerId's of all the members of the replication group.
 *
 * \return
 *      False if one of the servers is dead, true if all of them are alive.
 */
bool
CoordinatorServerList::assignReplicationGroup(
    Lock& lock, uint64_t replicationId,
    const vector<ServerId>* replicationGroupIds)
{
    foreach (ServerId backupId, *replicationGroupIds) {
        Entry* e = getEntry(backupId);
        if (!e) {
            return false;
        }

        if (e->status == ServerStatus::UP) {
            ServerReplicationUpUpdate(*this, lock).execute();
            ServerReplicationUpdate(*this, lock, e->serverId,
                &e->masterRecoveryInfo, replicationId, version + 1).execute();
        }
    }
    return true;
}

/**
 * Try to create a new replication group. Look for groups of backups that
 * are not assigned a replication group and are up.
 * If there are not enough available candidates for a new group, the function
 * returns without sending out any Rpcs. If there are enough group members
 * to form a new group, but one of the servers is down, hintServerCrashed will
 * reset the replication group of that server.
 *
 * \param lock
 *      Explicity needs CoordinatorServerList lock.
 */
void
CoordinatorServerList::createReplicationGroup(Lock& lock)
{
    // Create a list of all servers who do not belong to a replication group
    // and are up. Note that this is a performance optimization and is not
    // required for correctness.
    vector<ServerId> freeBackups;
    for (size_t i = 0; i < isize(); i++) {
        if (serverList[i].entry &&
            serverList[i].entry->isBackup() &&
            serverList[i].entry->replicationId == 0) {
            freeBackups.push_back(serverList[i].entry->serverId);
        }
    }

    // TODO(cidon): The coordinator currently has no knowledge of the
    // replication factor, so we manually set the replication group size to 3.
    // We should make this parameter configurable.
    const uint32_t numReplicas = 3;
    vector<ServerId> group;
    while (freeBackups.size() >= numReplicas) {
        group.clear();
        for (uint32_t i = 0; i < numReplicas; i++) {
            const ServerId& backupId = freeBackups.back();
            group.push_back(backupId);
            freeBackups.pop_back();
        }
        assignReplicationGroup(lock, nextReplicationId, &group);
        nextReplicationId++;
    }
}

/**
 * Reset the replicationId for all backups with groupId.
 *
 * \param lock
 *      Explicity needs CoordinatorServerList lock.
 * \param groupId
 *      Replication group that needs to be reset.
 */
void
CoordinatorServerList::removeReplicationGroup(Lock& lock, uint64_t groupId)
{
    // Cannot remove groupId 0, since it is the default groupId.
    if (groupId == 0) {
        return;
    }
    vector<ServerId> group;
    for (size_t i = 0; i < isize(); i++) {
        if (serverList[i].entry &&
            serverList[i].entry->isBackup() &&
            serverList[i].entry->replicationId == groupId) {
            group.push_back(serverList[i].entry->serverId);
        }
        if (group.size() != 0) {
            assignReplicationGroup(lock, 0, &group);
        }
    }
}

/**
 * Increments the server list version and notifies the async updater to
 * propagate the buffered Protobuf::ServerList update. The buffered update
 * will be Clear()ed and empty updates are silently ignored.
 *
 * \param lock
 *      Explicity needs CoordinatorServerList lock.
 * \param updateVersion
 *      Server list version number to be assigned to the update being pushed.
 */
void
CoordinatorServerList::pushUpdate(const Lock& lock, uint64_t updateVersion)
{
    ProtoBuf::ServerList full;

    // If there are no updates, don't generate a send.
    if (update.server_size() == 0)
        return;

    // prepare incremental server list
    update.set_version_number(updateVersion);
    update.set_type(ProtoBuf::ServerList_Type_UPDATE);

    // prepare full server list
    serialize(lock, &full);

    updates.emplace_back(&update, &full);

    // Link the previous tail with the new tail in the deque.
    if (updates.size() > 1)
      (updates.end()-2)->next = &(updates.back());

    hasUpdatesOrStop.notify_one();
    update.Clear();
}

/**
 * Stops the background updater. It cancel()'s all pending update rpcs
 * and leaves the cluster potentially out-of-date. To force a
 * synchronization point before halting, call sync() first.
 *
 * This will block until the updater thread stops.
 */
void
CoordinatorServerList::haltUpdater()
{
    // Signal stop
    Lock lock(mutex);
    stopUpdater = true;
    hasUpdatesOrStop.notify_one();
    lock.unlock();

    // Wait for Thread stop
    if (updaterThread && updaterThread->joinable()) {
        updaterThread->join();
        updaterThread.destroy();
    }
}

/**
 * Starts the background updater that keeps the cluster's
 * server lists up-to-date.
 */
void
CoordinatorServerList::startUpdater()
{
    Lock _(mutex);

    // Start thread if not started
    if (!updaterThread) {
        lastScan.reset();
        stopUpdater = false;
        updaterThread.construct(&CoordinatorServerList::updateLoop, this);
    }

    // Tell it to start work regardless
    hasUpdatesOrStop.notify_one();
}

/**
 * Checks if the cluster is up-to-date.
 *
 * \param lock
 *      explicity needs CoordinatorServerList lock
 * \return
 *      true if entire list is up-to-date
 */
bool
CoordinatorServerList::isClusterUpToDate(const Lock& lock) {
    return (serverList.size() == 0) ||
                (numUpdatingServers == 0 &&
                minConfirmedVersion == version);
}

/**
 * Causes a deletion of server list updates that are no longer needed
 * by the coordinator serverlist. This will delete all updates older than
 * the CoordinatorServerList's current minConfirmedVersion.
 *
 * This is safe to invoke whenever, but is typically done so after a
 * new minConfirmedVersion is set.
 *
  * \param lock
 *      explicity needs CoordinatorServerList lock
 */
void
CoordinatorServerList::pruneUpdates(const Lock& lock)
{
    if (minConfirmedVersion == UNINITIALIZED_VERSION)
        return;

    if (minConfirmedVersion > version) {
        LOG(ERROR, "Inconsistent state detected! CoordinatorServerList's "
                   "minConfirmedVersion %lu is larger than it's current "
                   "version %lu. This should NEVER happen!",
                   minConfirmedVersion, version);

        // Reset minVersion in the hopes of it being a transient bug.
        minConfirmedVersion = 0;
        return;
    }

    if (minConfirmedVersion == version) {
        PersistServerListVersion(*this, lock, version).execute();
    }

    while (!updates.empty() && updates.front().version <= minConfirmedVersion) {

        ProtoBuf::ServerList currentUpdate = updates.front().incremental;
        assert(currentUpdate.type() ==
                ProtoBuf::ServerList::Type::ServerList_Type_UPDATE);

        for (int i = 0; i < currentUpdate.server().size(); i++) {
            ProtoBuf::ServerList::Entry currentEntry = currentUpdate.server(i);
            ServerStatus updateStatus =
                    ServerStatus(currentEntry.status());

            if (updateStatus == ServerStatus::UP) {
                // An enlistServer operation has successfully completed.

                if (context->logCabinHelper) {
                    ServerId serverId =
                            ServerId(currentEntry.server_id());
                    Entry* entry = getEntry(serverId);
                    assert(entry != NULL);
                    // We are relying on the fact that enlistServer has to be
                    // sent out before the replication id update is sent out by
                    // the coordinator, and therefore the order of
                    // acknowledgement has to be in the same order.
                    // Otherwise, the coordinator will invalidate the wrong
                    // updates.
                    bool isUpUpdate = true;
                    if (entry->logIdServerUpUpdate != NO_ID) {
                        vector<EntryId> invalidates
                            {entry->logIdServerUpUpdate};
                        context->logCabinHelper->invalidate(
                            *context->expectedEntryId, invalidates);
                    } else {
                        isUpUpdate = false;
                        vector<EntryId> invalidates
                            {entry->logIdServerReplicationUpUpdate};
                        context->logCabinHelper->invalidate(
                            *context->expectedEntryId, invalidates);
                   }
                    if (isUpUpdate) {
                        entry->logIdServerUpUpdate = NO_ID;
                    } else {
                        entry->logIdServerReplicationUpUpdate = NO_ID;
                    }
                }

            } else if (updateStatus == ServerStatus::CRASHED) {
                // A serverCrashed operation has completed. The server that
                // crashed may be still recovering.
                // So we're not invalidating its LogCabin entries just yet.

            } else if (updateStatus == ServerStatus::REMOVE) {
                // This marks the final completion of serverCrashed() operation.
                // i.e., If the crashed server was a master, then its recovery
                // (sparked by a serverCrashed operation) has completed.
                // If it was not a master, then the serverCrashed operation
                // (which will not spark any recoveries) has completed.

                ServerId serverId = ServerId(currentEntry.server_id());
                Tub<Entry>& entry = serverList[serverId.indexNumber()].entry;

                if (context->logCabinHelper) {
                    assert(entry);
                    vector<EntryId> invalidates {
                                entry->logIdServerUp,
                                entry->logIdServerCrashed,
                                entry->logIdServerRemoveUpdate};
                    if (entry->logIdServerUpdate != NO_ID)
                        invalidates.push_back(entry->logIdServerUpdate);

                    context->logCabinHelper->invalidate(
                                *context->expectedEntryId, invalidates);
                }

                entry.destroy();
            }
        }

        updates.pop_front();
    }

    if (updates.empty())    // Empty list = no updates to send
        listUpToDate.notify_all();
}

/**
 * Blocks until all of the cluster is up-to-date.
 */
void
CoordinatorServerList::sync()
{
    startUpdater();
    Lock lock(mutex);
    while (!isClusterUpToDate(lock)) {
        listUpToDate.wait(lock);
    }
}


/**
 * Main loop that checks for outdated servers and sends out update rpcs.
 * This is the top-level method of a dedicated thread separate from the
 * main coordinator's.
 *
 * Once invoked, this loop can be exited by calling haltUpdater() in another
 * thread.
 *
 * The Updater Loop manages starting, stopping, and following up on ServerList
 * Update RPCs asynchronous to the main thread with minimal locking of
 * Coordinator Server List, CSL. The intention of this mechanism is to
 * ensure that the critical sections of the CSL are not delayed while
 * waiting for RPCs to finish.
 *
 * Since Coordinator Server List houses all the information about updatable
 * servers, this mechanism requires at least two entry points (conceptual)
 * into the server list, a) a way to get info about outdated servers and b)
 * a way to signal update success/failure. The former is achieved via
 * getWork() and the latter is achieved by workSucceeded()/workFailed().
 * For polling efficiency, there is an additional call, waitForWork(), that
 * will sleep until more servers get out of date. These are the only calls
 * that require locks on the Coordinator Server List that UpdateLoop uses.
 * Other than that, the updateLoop operates asynchronously from the CSL.
 *
 */
void
CoordinatorServerList::updateLoop()
{
    UpdaterWorkUnit wu;
    uint64_t max_rpcs = 8;
    std::deque<Tub<UpdateServerListRpc>> rpcs_backing;
    std::list<Tub<UpdateServerListRpc>*> rpcs;
    std::list<Tub<UpdateServerListRpc>*>::iterator it;


    /**
     * The Updater Loop manages a number of outgoing RPCs. The maximum number
     * of concurrent RPCs is determined dynamically in the hopes of finding a
     * sweet spot where the time to iterate through the list of outgoing RPCs
     * is roughly equivalent to the time of one RPC finishing. The heuristic
     * for doing this is simply only allowing one new RPC to be started with
     * each pass through the internal list of outgoing RPCs and allowing an
     * unlimited number to finish. The intuition for doing this is based on
     * the observation that checking whether an RPC is finished and finishing
     * it takes ~10-20ns whereas starting a new RPC takes much longer. Thus,
     * by limiting the number of RPCs started per iteration, we would allow
     * for rapid polling of unfinished RPCs and a rapid ramp up to the steady
     * state where roughly one RPC finishes per iteration. This is preferable
     * over trying start multiple new RPCs per iteration.
     *
     * The Updater keeps track of the outgoing RPCs internally in a
     * List<Tub<UpdateServerListRPC*>> that is organized as such:
     *
     * **************************************************....
     * * Active RPCS   *   Inactive RPCs  *  Unused RPCs ....
     * **************************************************
     *                                    /\ max_rpcs
     *
     * Active RPCs are ones that have started but not finished. They are
     * compacted to the left so that scans through the list would look at
     * the active RPCs first. Inactive RPCs are ones that have not been
     * started and are allocated, unoccupied Tubs. Unused RPCs conceptually
     * represent RPCs that are unallocated and are outside the max_rpcs range.
     *
     * The division between Active RPCs and Inactive RPCs is defined as the
     * index where the first, leftmost unoccupied Tub resides. The division
     * between inactive and unused RPCs is the physical size of the list
     * which monotonically increases as the updater determines that more
     * RPCs need to be started.
     *
     * In normal operation, the Updater will start scanning through the list
     * left to right. Conceptually, the actions it takes would depend on
     * which region of the list it's in. In the active RPCs range, it will
     * check for finished RPCs. If it encounters one, it will clean up the
     * RPC and place its Tub at the back of the list to compact the active
     * range. Once in the inactive range, it will start up one new RPC and
     * continue on.
     *
     * At this point, the iteration will either still be in the inactive
     * range or it would have reached the max_rpcs marker (unused range).
     * If it's in the unused range, it will allocate more Tubs. Otherwise,
     * it will determine if the thread should sleep or not. The thread will
     * sleep when the active range is empty.
     */
    try {
        while (!stopUpdater) {
            // Phase 0: Alloc more RPCs to fill max_rpcs.
            for (size_t i = rpcs.size(); i < max_rpcs && !stopUpdater; i++) {
                rpcs_backing.emplace_back();
                rpcs.push_back(&rpcs_backing.back());
            }

            // Phase 1: Scan through and compact active rpcs to the front.
            it = rpcs.begin();
            while ( it != rpcs.end() && !stopUpdater ) {
                UpdateServerListRpc* rpc = (*it)->get();

                // Reached end of active rpcs, enter phase 2.
                if (!rpc)
                    break;

                // Skip not-finished rpcs
                if (!rpc->isReady()) {
                    it++;
                    continue;
                }

                // Finished rpc found
                bool success = false;
                try {
                    rpc->wait();
                    success = true;
                } catch (const ServerNotUpException& e) {}

                if (success)
                    workSuccess(rpc->id);
                else
                    workFailed(rpc->id);
                (*it)->destroy();

                // Compaction swap
                rpcs.push_back(*it);
                it = rpcs.erase(it);
            }

            // Phase 2: Start up to 1 new rpc
            if ( it != rpcs.end() && !stopUpdater ) {
                if (getWork(&wu)) {
                    auto* initial_list = (wu.sendFullList) ?
                       &(wu.firstUpdate->full) : &(wu.firstUpdate->incremental);
                    (*it)->construct(context, wu.targetServer, initial_list);

                    // Batch up multiple requests
                    const ServerListUpdatePair* updatePtr = wu.firstUpdate;
                    while (updatePtr->version < wu.updateVersionTail) {
                        updatePtr = updatePtr->next;
                        (**it)->appendServerList(&(updatePtr->incremental));
                    }

                    (**it)->send();
                    it++;
                }
            }

            // Phase 3: Expand and/or stop
            if (it == rpcs.end()) {
                max_rpcs += 8;
            } else if (it == rpcs.begin()) {
                waitForWork();
            }
        }

        // wend; stopUpdater = true
        for (size_t i = 0; i < rpcs_backing.size(); i++) {
            if (rpcs_backing[i]) {
                workFailed(rpcs_backing[i]->id);
                rpcs_backing[i]->cancel();
                rpcs_backing[i].destroy();

            }
        }

    } catch (const std::exception& e) {
        LOG(ERROR, "Fatal error in CoordinatorServerList: %s", e.what());
        throw;
    } catch (...) {
        LOG(ERROR, "Unknown fatal error in CoordinatorServerList.");
        throw;
    }
}

/**
 * Invoked by the updater thread to wait (sleep) until there are
 * more updates. This will block until there is more updating work
 * to be done and will notify those waiting for the server list to
 * be up to date (if any).
 */
void
CoordinatorServerList::waitForWork()
{
    Lock lock(mutex);

    while (minConfirmedVersion == version && !stopUpdater) {
        listUpToDate.notify_all();
        hasUpdatesOrStop.wait(lock);
    }
}

/**
 * Attempts to find servers that require updates that don't already
 * have outstanding update rpcs.
 *
 * This call MUST be followed by a workSuccuss or workFailed call
 * with the serverId contained within the WorkUnit at some point in
 * the future to ensure that internal metadata is reset for the work
 * unit so that the server can receive future updates.
 *
 * There is a contract that comes with call. All updates that come after-
 * and the update that starts on- the iterator in the WorkUnit is
 * GUARANTEED to be NOT deleted as long as a workSuccess or workFailed
 * is not invoked with the corresponding serverId in the WorkUnit. There
 * are no guarantees for updates that come before the iterator's starting
 * position, so don't iterate backwards. See documentation on UpdaterWorkUnit
 * for more info.
 *
 * \param wu
 *      Work Unit that can be filled out by this method
 *
 * \return
 *      true if an updatable server (work) was found
 *
 */
bool
CoordinatorServerList::getWork(UpdaterWorkUnit* wu) {
    Lock lock(mutex);

    // Heuristic to prevent duplicate scans when no new work has shown up.
    if (serverList.size() == 0 ||
          (numUpdatingServers > 0 && lastScan.noWorkFoundForEpoch == version))
        return false;

    /**
     * Searches through the server list for servers that are eligible
     * for updates and are currently not updating. The former is defined as
     * the server having MEMBERSHIP_SERVICE, is UP, and has a verfiedVersion
     * not equal to the current version. The latter is defined as a server
     * having verfiedVersion == updateVersion.
     *
     * The search starts at lastScan.searchIndex, which is a marker for
     * where the last invocation of getWork() stopped before returning.
     * The search ends when either the entire server list has been scanned
     * (when we reach lastScan.searchIndex again) or when an outdated server
     * eligible for updates has been found. In the latter case, the function
     * will package the update details into a WorkUnit and mark the index it
     * left off on in lastScan.searchIndex.
     *
     * Updates get packed into WorkUnits as one would expect; servers that
     * haven't heard from the Coordinator get one full list and those that
     * have get a batch of incremental updates. One peculiarity is the version
     * of the full list. If the work unit was created during the first scan
     * through the server list, the lowest version is used. Otherwise, the
     * latest server list is used.
     *
     * The reason for this is because when the updater thread stops or the
     * coordinator crashes, update rpcs may be prematurely canceled. Some
     * servers may have actually gotten a full list and applied it but could
     * not respond in time. Thus, to prevent sending servers a newer full
     * server list they can't process, we send the oldest server list on the
     * first scan through the server list and then later patch it up to date
     * with a batch of incremental updates. This is called 'slow start.'
     *
     * While scanning, the loop will also keep track of the minimum
     * verifiedVersion in the server list and will propagate this
     * value to minConfirmedVersion. This is done to track whether
     * the server list is up to date and which old updates, which
     * are guaranteed to never be used again, can be pruned. The
     * propagation occurs only when the minVersion in the scan is
     * interesting, i.e. not MAX64 and not UNINITIALIZED_VERSION.
     * The former means that there are no updatable servers and the
     * latter means there are updatable servers with uninitialized
     * server lists. Neither contribute to the knowledge of what
     * the minimum server list version in the cluster is and what
     * can be pruned.
     */
    size_t i = lastScan.searchIndex;
    uint64_t numUpdatableServers = 0;
    do {
        Entry* server = serverList[i].entry.get();
        // Does server exist and is it updatable?
        if (server && server->status == ServerStatus::UP &&
                server->services.has(WireFormat::MEMBERSHIP_SERVICE)) {

            // Record Stats
            numUpdatableServers++;
            if (server->verifiedVersion < lastScan.minVersion) {
                lastScan.minVersion = server->verifiedVersion;
            }

            // update required
            if (server->updateVersion != version &&
                    server->updateVersion == server->verifiedVersion) {
                // New server, send full server list
                if (server->verifiedVersion == UNINITIALIZED_VERSION) {
                    wu->sendFullList = true;

                    // 'slow start'
                    if (lastScan.completeScansSinceStart == 0) {
                        wu->firstUpdate = &(updates.front());
                    } else {
                        wu->firstUpdate = &(updates.back());
                    }
                } else {
                    // Incremental update(s).
                    wu->sendFullList = false;

                    uint64_t firstVersion = server->verifiedVersion + 1;
                    size_t offset =  firstVersion - updates.front().version;
                    wu->firstUpdate = &*(updates.begin()+=offset);
                }

                wu->targetServer = server->serverId;
                wu->updateVersionTail = std::min(version,
                              wu->firstUpdate->version + MAX_UPDATES_PER_RPC-1);

                numUpdatingServers++;
                lastScan.searchIndex = i;
                server->updateVersion = wu->updateVersionTail;
                return true;
            }
        }

        i = (i+1)%serverList.size();

        // update statistics
        if (i == 0) {
            // Record min version only if it's interesting
            if ( lastScan.minVersion != UNINITIALIZED_VERSION &&
                    lastScan.minVersion != MAX64)
                minConfirmedVersion = lastScan.minVersion;

            lastScan.completeScansSinceStart++;
            lastScan.minVersion = MAX64;
            pruneUpdates(lock);
        }
    // While we haven't reached a full scan through the list yet.
    } while (i != lastScan.searchIndex);

    // If no one is updating, then it's safe to prune ALL updates.
    if (numUpdatableServers == 0) {
        minConfirmedVersion = version;
        pruneUpdates(lock);
    }

    lastScan.noWorkFoundForEpoch = version;
    return false;
}

/**
 * Signals the success of updater to complete a work unit. This
 * will update internal metadata to allow the server described by
 * the work unit to be updatable again.
 *
 * Note that this should only be invoked AT MOST ONCE for each WorkUnit.
 *
 * \param id
 *      The serverId originally contained in the work unit
 *      that just succeeded.
 */
void
CoordinatorServerList::workSuccess(ServerId id) {
    Lock lock(mutex);

    // Error checking for next 3 blocks
    if (numUpdatingServers > 0) {
        numUpdatingServers--;
    } else {
        LOG(ERROR, "Bookeeping issue detected; server's count of "
                "numUpdatingServers just went negative. Not total failure "
                "but will cause the updater thread to spin even w/o work. "
                "Cause is mismatch # of getWork() and workSuccess/Failed()");
    }

    Entry* server = getEntry(id);
    if (server == NULL) {
        // Typically not an error, but this is UNUSUAL in normal cases.
        LOG(DEBUG, "Server %s responded to a server list update but "
                "is no longer in the server list...",
                id.toString().c_str());
        return;
    }

    if (server->verifiedVersion == server->updateVersion) {
        LOG(ERROR, "Invoked for server %s even though either no update "
                "was sent out or it has already been invoked. Possible "
                "race/bookeeping issue.",
                server->serverId.toString().c_str());
    } else {
        // Meat of actual functionality
        LOG(DEBUG, "ServerList Update Success: %s update (%ld => %ld)",
                server->serverId.toString().c_str(),
                server->verifiedVersion,
                server->updateVersion);
        server->verifiedVersion = server->updateVersion;
    }

    // If update didn't update all the way or it's the last server updating,
    // hint for a full scan to update minConfirmedVersion.
    if (server->verifiedVersion < version)
        lastScan.noWorkFoundForEpoch = 0;
}

/**
 * Signals the failure of the updater to execute a work unit. Causes
 * an internal rollback on metadata so that the server involved will
 * be retried later.
 *
 * \param id
 *      The serverId contained within the work unit that the updater
 *      had failed to send out.
 */
void
CoordinatorServerList::workFailed(ServerId id) {
    Lock lock(mutex);

    if (numUpdatingServers > 0) {
        numUpdatingServers--;
    } else {
        LOG(ERROR, "Bookeeping issue detected; server's count of "
                "numUpdatingServers just went negative. Not total failure "
                "but will cause the updater thread to spin even w/o work. "
                "Cause is mismatch # of getWork() and workSuccess/Failed()");
    }

    Entry* server = getEntry(id);
    if (server) {
        server->updateVersion = server->verifiedVersion;

        LOG(DEBUG, "ServerList Update Failed : %s update (%ld => %ld)",
               server->serverId.toString().c_str(),
               server->verifiedVersion,
               server->updateVersion);
    }

    lastScan.noWorkFoundForEpoch = 0;
}

//////////////////////////////////////////////////////////////////////
// CoordinatorServerList::UpdateServerListRpc Methods
//////////////////////////////////////////////////////////////////////
/**
 * Constructor for UpdateServerListRpc that creates but does not send()
 * a server list update rpc. It initially accepts one protobuf to ensure
 * that the rpc would never be sent empty and provides a convenient way to
 * send only a single full list.
 *
 * After invoking the constructor, it is up to the caller to invoke
 * as many appendServerList() calls as necessary to batch up multiple
 * updates and follow it up with a send().
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param serverId
 *      Identifies the server to which this update should be sent.
 * \param list
 *      The complete server list representing all cluster membership.
 */
CoordinatorServerList::UpdateServerListRpc::UpdateServerListRpc(
            Context* context,
            ServerId serverId,
            const ProtoBuf::ServerList* list)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::UpdateServerList::Response))
{
    allocHeader<WireFormat::UpdateServerList>(serverId);

    auto* part = new(&request, APPEND)
            WireFormat::UpdateServerList::Request::Part();

    part->serverListLength = serializeToRequest(&request, list);
}

/**
 * Appends a server list update ProtoBuf to the request rpc. This is used
 * to batch up multiple server list updates into one rpc for the server and
 * can be invoked multiple times on the same rpc.
 *
 * It is the caller's responsibility invoke send() on the rpc when enough
 * updates have been appended and to ensure that this append is invoked only
 * when the RPC has not been started yet.
 *
 * \param list
 *      ProtoBuf::ServerList to append to the new RPC
 * \return
 *      true if append succeeded, false if it didn't fit in the current rpc
 *      and has been removed. In the later case, it's time to call send().
 */
bool
CoordinatorServerList::UpdateServerListRpc::appendServerList(
                                        const ProtoBuf::ServerList* list)
{
    assert(this->getState() == NOT_STARTED);
    uint32_t sizeBefore = request.getTotalLength();

    auto* part = new(&request, APPEND)
            WireFormat::UpdateServerList::Request::Part();

    part->serverListLength = serializeToRequest(&request, list);

    uint32_t sizeAfter = request.getTotalLength();
    if (sizeAfter > Transport::MAX_RPC_LEN) {
        request.truncateEnd(sizeAfter - sizeBefore);
        return false;
    }

    return true;
}


//////////////////////////////////////////////////////////////////////
// CoordinatorServerList::Entry Methods
//////////////////////////////////////////////////////////////////////

/**
 * Construct a new Entry, which contains no valid information.
 */
CoordinatorServerList::Entry::Entry()
    : ServerDetails()
    , masterRecoveryInfo()
    , needsRecovery(false)
    , verifiedVersion(UNINITIALIZED_VERSION)
    , updateVersion(UNINITIALIZED_VERSION)
    , logIdServerCrashed(NO_ID)
    , logIdServerNeedsRecovery(NO_ID)
    , logIdServerRemoveUpdate(NO_ID)
    , logIdServerUp(NO_ID)
    , logIdServerUpdate(NO_ID)
    , logIdServerUpUpdate(NO_ID)
    , logIdServerReplicationUpdate(NO_ID)
    , logIdServerReplicationUpUpdate(NO_ID)
{
}

/**
 * Construct a new Entry, which contains the data a coordinator
 * needs to maintain about an enlisted server.
 *
 * \param serverId
 *      The ServerId of the server this entry describes.
 *
 * \param serviceLocator
 *      The ServiceLocator string that can be used to address this
 *      entry's server.
 *
 * \param services
 *      Which services this server supports.
 */
CoordinatorServerList::Entry::Entry(ServerId serverId,
                                    const string& serviceLocator,
                                    ServiceMask services)

    : ServerDetails(serverId,
                    serviceLocator,
                    services,
                    0,
                    ServerStatus::UP)
    , masterRecoveryInfo()
    , needsRecovery(false)
    , verifiedVersion(UNINITIALIZED_VERSION)
    , updateVersion(UNINITIALIZED_VERSION)
    , logIdServerCrashed(NO_ID)
    , logIdServerNeedsRecovery(NO_ID)
    , logIdServerRemoveUpdate(NO_ID)
    , logIdServerUp(NO_ID)
    , logIdServerUpdate(NO_ID)
    , logIdServerUpUpdate(NO_ID)
    , logIdServerReplicationUpdate(NO_ID)
    , logIdServerReplicationUpUpdate(NO_ID)
{
}

/**
 * Serialize this entry into the given ProtoBuf.
 */
void
CoordinatorServerList::Entry::serialize(ProtoBuf::ServerList_Entry* dest) const
{
    dest->set_services(services.serialize());
    dest->set_server_id(serverId.getId());
    dest->set_service_locator(serviceLocator);
    dest->set_status(uint32_t(status));
    if (isBackup())
        dest->set_expected_read_mbytes_per_sec(expectedReadMBytesPerSec);
    else
        dest->set_expected_read_mbytes_per_sec(0); // Tests expect the field.
    dest->set_replication_id(replicationId);
}
} // namespace RAMCloud
