/* Copyright (c) 2011-2014 Stanford University
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
#include <unordered_map>

#include "ServerListEntry.pb.h"

#include "Common.h"
#include "ClientException.h"
#include "CoordinatorServerList.h"
#include "CoordinatorService.h"
#include "Cycles.h"
#include "MasterRecoveryManager.h"
#include "ServerTracker.h"
#include "ShortMacros.h"
#include "TransportManager.h"
#include "Context.h"

namespace RAMCloud {

/**
 * Constructor for CoordinatorServerList. Note: the updater is initially
 * disabled, so updates will be queued but not sent out to the rest of the
 * cluster. To enable updates, call the \c startUpdater method.
 *
 * \param context
 *      Overall information about the RAMCloud server.  The constructor
 *      will modify \c context so that its \c serverList and
 *      \c coordinatorServerList members refer to this object.
 */
CoordinatorServerList::CoordinatorServerList(Context* context)
    : AbstractServerList(context)
    , context(context)
    , serverList()
    , numberOfMasters(0)
    , numberOfBackups(0)
    , stopUpdater(true)
    , updaterSleeping(false)
    , lastScan()
    , updates()
    , hasUpdatesOrStop()
    , listUpToDate()
    , updaterThread()
    , activeRpcs()
    , spareRpcs()
    , maxConfirmedVersion(0)
    , numUpdatingServers(0)
    , replicationGroupSize(3)
    , maxReplicationId(0)
{
    context->coordinatorServerList = this;
}

/**
 * Destructor for CoordinatorServerList.
 */
CoordinatorServerList::~CoordinatorServerList()
{
    haltUpdater();
    foreach (Tub<UpdateServerListRpc>* rpcTub, activeRpcs) {
        delete rpcTub;
    }
    foreach (Tub<UpdateServerListRpc>* rpcTub, spareRpcs) {
        delete rpcTub;
    }
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
 * Add a newly booted server to the cluster, and arrange for information
 * about that server to be propagated to the rest of the cluster. The
 * actual propagation occurs in the background, and may not have
 * completed before this method returns.
 *
 * \param serviceMask
 *      Services supported by the enlisting server.
 * \param readSpeed
 *      Read speed of the enlisting server.
 * \param serviceLocator
 *      Indicates how to communicate with the enlisting server.
 *
 * \return
 *      Server id assigned to the enlisting server.
 */
ServerId
CoordinatorServerList::enlistServer(ServiceMask serviceMask,
                                    const uint32_t readSpeed,
                                    const char* serviceLocator)
{
    Lock lock(mutex);

    uint32_t index = firstFreeIndex(lock);
    GenerationNumberEntryPair* pair = &serverList[index];
    ServerId id(index, pair->nextGenerationNumber);
    pair->nextGenerationNumber++;
    pair->entry.construct(id, serviceLocator, serviceMask);
    if (serviceMask.has(WireFormat::MASTER_SERVICE)) {
        numberOfMasters++;
    }
    if (serviceMask.has(WireFormat::BACKUP_SERVICE)) {
        numberOfBackups++;
        pair->entry->expectedReadMBytesPerSec = readSpeed;
    }
    LOG(NOTICE, "Enlisting server at %s (server id %s) supporting "
        "services: %s", serviceLocator, id.toString().c_str(),
        serviceMask.toString().c_str());
    persistAndPropagate(lock, pair->entry.get(),
            ServerChangeEvent::SERVER_ADDED);
    if (serviceMask.has(WireFormat::BACKUP_SERVICE)) {
        LOG(DEBUG, "Backup at id %s has %u MB/s read",
            id.toString().c_str(), readSpeed);
        createReplicationGroups(lock);
    }
    return id;
}

/**
 * Get the number of masters in the list; does not include crashed servers.
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
 * a lock) wants to use this functionality, it should directly call the
 * #getEntry method.
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
 *      The first valid index is 0.
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
            format("Index beyond array length (%zd) or entry "
                   "doesn't exist", index));
    }

    return *entry;
}

/**
 * This method is called shortly after the coordinator assumes leadership
 * of the cluster; it recovers all of the server list information from
 * external storage and re-initiates incomplete operations, such as
 * crash recoveries and update notifications to other servers. The caller
 * must make sure that the updater is not running concurrently with this
 * method. This method must be called when the CoordinatorServerList is in
 * its initialized state (i.e. before any updates).
 *
 * \param lastCompletedUpdate
 *      Sequence number of the last update from the previous coordinator
 *      that is known to have finished. Any updates after this may or may
 *      not have finished, so we must do whatever is needed to complete
 *      them.
 */
void
CoordinatorServerList::recover(uint64_t lastCompletedUpdate)
{
    Lock lock(mutex);

    // Fetch all of the server list information from external storage.
    vector<ExternalStorage::Object> objects;
    context->externalStorage->getChildren("servers", &objects);

    // Each iteration through the following loop processes information
    // for one entry in the server list.
    foreach (ExternalStorage::Object& object, objects) {
        // First, parse the protocol buffer.
        if (object.value == NULL)
            continue;
        ProtoBuf::ServerListEntry info;
        string str(object.value, object.length);
        if (!info.ParseFromString(str)) {
            throw FatalError(HERE, format(
                    "couldn't parse protocol buffer in servers/%s",
                    object.name));
        }
        ServerId id(info.server_id());

        uint32_t index = id.indexNumber();
        if (index >= serverList.size())
            serverList.resize(index + 1);
        GenerationNumberEntryPair* pair = &serverList[index];
        if (pair->entry) {
            throw FatalError(HERE, format(
                    "couldn't process external data at servers/%s (server "
                    "id %s): server list slot %u already occupied",
                    object.name, id.toString().c_str(), index));
        }
        pair->nextGenerationNumber = id.generationNumber() + 1;

        // Special case: leave the entry uninitialized if the server had
        // been removed from the cluster and all notifications were completed.
        if ((ServerStatus(info.status()) == ServerStatus::REMOVE)
                && ((info.update_size() == 0) ||
                (info.update(info.update_size()-1).sequence_number()
                <= lastCompletedUpdate))) {
            continue;
        }

        // Re-create the entry from the protocol buffer.
        pair->entry.construct(id, info.service_locator(),
                ServiceMask::deserialize(info.services()));
        CoordinatorServerList::Entry* entry = pair->entry.get();
        entry->expectedReadMBytesPerSec = info.expected_read_mbytes_per_sec();
        entry->status = ServerStatus(info.status());
        entry->replicationId = info.replication_id();
        entry->masterRecoveryInfo = info.master_recovery_info();
        LOG(NOTICE, "Recreated server %s at %s with services %s, status %s",
                entry->serverId.toString().c_str(),
                entry->serviceLocator.c_str(),
                entry->services.toString().c_str(),
                toString(entry->status).c_str());

        if (entry->status == ServerStatus::UP) {
            if (entry->services.has(WireFormat::MASTER_SERVICE)) {
                numberOfMasters++;
            }
            if (entry->services.has(WireFormat::BACKUP_SERVICE)) {
                numberOfBackups++;
            }
        }

        // Notify local ServerTrackers about this entry.
        ServerChangeEvent event = ServerChangeEvent::SERVER_ADDED;
        if (entry->status == ServerStatus::CRASHED) {
            event = ServerChangeEvent::SERVER_CRASHED;
        } else if (entry->status == ServerStatus::REMOVE) {
            event = ServerChangeEvent::SERVER_REMOVED;
        }
        foreach (ServerTrackerInterface* tracker, trackers) {
            tracker->enqueueChange(*entry, event);
        }

        // Scan all the update information from external storage. For each
        // update that was not fully propagated, start a new update.
        bool incompleteUpdates = false;
        for (int i = 0; i < info.update_size(); i++) {
            const ProtoBuf::ServerListEntry_Update* updateInfo =
                    &(info.update(i));

            // Keep track of the highest version seen so far.
            if (updateInfo->version() > version) {
                version = updateInfo->version();
            }

            if (updateInfo->sequence_number() > lastCompletedUpdate) {
                // This entry was not fully propagated to the cluster before
                // the coordinator crashed. Create a new update to finish the
                // propagation (with a fresh sequence number that we can track).
                incompleteUpdates = true;
                uint64_t sequenceNumber = context->coordinatorService->
                        updateManager.nextSequenceNumber();
                uint64_t version = updateInfo->version();
                ServerStatus status = ServerStatus(updateInfo->status());
                LOG(NOTICE, "Rescheduling update for server %s, version %lu, "
                        "updateSequence %lu, status %s",
                        entry->serverId.toString().c_str(),
                        version, sequenceNumber,
                        toString(status).c_str());

                // First, record the update in the entry itself.
                entry->pendingUpdates.push_back(*updateInfo);
                entry->pendingUpdates.back().set_sequence_number(
                        sequenceNumber);

                // Next, create an update to notify the rest of the cluster.
                ServerStatus savedStatus = entry->status;
                entry->status = status;
                insertUpdate(lock, entry, version);
                entry->status = savedStatus;
            }
        }
        if (incompleteUpdates) {
            // Must save the to ExternalStorage to guarantee durability
            // of the new sequence numbers for updates (otherwise, another
            // coordinator crash before the updates are completed could
            // cause the updates never to be finished).
            entry->sync(context->externalStorage);
        }
    }

    // Repair inconsistencies in the replication groups.
    repairReplicationGroups(lock);

    // There used to be a consistency check here that scanned the
    // update list to ensure that the version numbers formed a contiguous
    // range. However we can't guarantee this property, since old versions
    // that have been fully propagated get removed from external storage
    // in a number to order.  If there are any holes in the update list,
    // the updates preceding the holes are obsolete and have been
    // fully propagated.

    // Mark all of the servers so that they will receive all updates
    // currently waiting to be propagated.
    uint64_t lastReceived = version;
    if (!updates.empty()) {
        lastReceived = updates.front().incremental.version_number() - 1;
    }
    foreach (GenerationNumberEntryPair& pair, serverList) {
        if (!pair.entry)
            continue;
        pair.entry->verifiedVersion = pair.entry->updateVersion =
                lastReceived;
    }

    // Perform the second stage of tracker notification (firing callbacks).
    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->fireCallback();

    LOG(NOTICE, "CoordinatorServerList recovery completed: %u master(s), "
            "%u backup(s), %lu update(s) to disseminate, server list version "
            "is %lu",
            numberOfMasters, numberOfBackups, updates.size(), version);
}

/**
 * This method is invoked by crash recovery code, after it has finished
 * recovering a crashed master. This method marks the server as no longer
 * in the cluster and propagates that information both to the servers in
 * the cluster and to local trackers. The state change is also recorded on
 * external storage to ensure that it survives coordinator crashes.
 * Updating happens in the background, so this method returns before the
 * information has been fully propagated.
 *
 * \param serverId
 *      The ServerId of the server to remove from the CoordinatorServerList.
 *      If there is no such server in the list, then this method returns
 *      without doing anything.
 */
void
CoordinatorServerList::recoveryCompleted(ServerId serverId)
{
    Lock lock(mutex);
    Entry* entry = getEntry(serverId);
    if (entry == NULL) {
        LOG(NOTICE, "Skipping removal for server %s: it doesn't exist",
            serverId.toString().c_str());
        return;
    }
    assert(entry->status == ServerStatus::CRASHED);

    LOG(NOTICE, "Removing server %s from cluster/coordinator server list",
        serverId.toString().c_str());
    entry->status = ServerStatus::REMOVE;
    persistAndPropagate(lock, entry, ServerChangeEvent::SERVER_REMOVED);
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
 *
 * \param serverId
 *      ServerId of the server that is suspected to be down. If this
 *      server no longer exists or has already been marked crashed,
 *      then the method returns without doing anything.
 */
void
CoordinatorServerList::serverCrashed(ServerId serverId)
{
    Lock lock(mutex);

    Entry* entry = getEntry(serverId);
    if (entry == NULL) {
        LOG(NOTICE, "Skipping serverCrashed for server %s: it doesn't exist",
            serverId.toString().c_str());
        return;
    }
    if (entry->status != ServerStatus::UP) {
        LOG(NOTICE, "Skipping serverCrashed for server %s: state is %s",
            serverId.toString().c_str(), toString(entry->status).c_str());
        return;
    }

    if (entry->isMaster())
        numberOfMasters--;
    if (entry->isBackup())
        numberOfBackups--;
    entry->status = ServerStatus::CRASHED;

    // Be sure to update replication groups before marking server crashed;
    // otherwise, the replication group updates could get lost if we crash
    // after marking the server crashed but before updating replication
    // groups.
    removeReplicationGroup(lock, entry->replicationId);
    createReplicationGroups(lock);
    entry->replicationId = 0;

    persistAndPropagate(lock, entry, ServerChangeEvent::SERVER_CRASHED);

    context->recoveryManager->startMasterRecovery(*entry);
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
        // Note: we DON'T assign a new updateSequenceNumber here, since
        // this information does not need to be propagated to the cluster
        // and no special crash recovery actions are needed: if the
        // coordinator crashes before updating external storage, this
        // operation will be retried by the initiating master.
        entry->sync(context->externalStorage);
        return true;
    } else {
        return false;
    }
}

//////////////////////////////////////////////////////////////////////
// CoordinatorServerList Private Methods
//////////////////////////////////////////////////////////////////////

ServerDetails*
CoordinatorServerList::iget(ServerId id)
{
    return getEntry(id);
}

ServerDetails*
CoordinatorServerList::iget(uint32_t index)
{
    return getEntry(index);
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
 * Returns the entry corresponding to a ServerId.  Assumes caller already
 * has CoordinatorServerList lock. This method is distinct from iget
 * because it returns CoordinatorServerList::Entry* rather than ServerDetails*.
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
 * in the server list. Assumes caller already has CoordinatorServerList
 * lock.  This method is distinct from iget because it returns
 * CoordinatorServerList::Entry* rather than ServerDetails*.
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
 * Return the first free index in the server list. If the list is
 * completely full, enlarge it and return the next free one.  Index 0
 * is reserved and will never be returned.
 *
 * \param lock
 *      Make sure caller has acquired CoordinatorServerList lock.
 *      Not explicitly used.
 */
uint32_t
CoordinatorServerList::firstFreeIndex(const Lock& lock)
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
 * This method is invoked whenever a server list entry is modified (e.g.
 * to enlist a server, start crash recovery, etc.). It stores a copy
 * of the entry on external storage so it will survive coordinator crashes,
 * then propagates information about the modification to other interested
 * parties. This means notifying local ServerTrackers, and also notifying
 * all of the other servers in the cluster. At the time this method returns
 * the entry will be persistent and notification will have begun, but
 * notifications will not have completed yet (this happens in a separate
 * thread, running in the background).
 *
 * \param lock
 *      Make sure caller has acquired CoordinatorServerList lock.
 *      Not explicitly used.
 * \param entry
 *      The entry that was just created or modified.
 * \param event
 *      Indicates the nature of this change; used when notifying
 *      ServerTrackers.
 */
void
CoordinatorServerList::persistAndPropagate(const Lock& lock, Entry* entry,
        ServerChangeEvent event)
{
    TEST_LOG("Persisting %s", entry->serverId.toString().c_str());

    // Add a new update to the list of those in progress for this entry,
    // then write the entire entry to external storage to ensure that cluster
    // notification completes eventually, even in the presence of
    // coordinator crashes.
    entry->pendingUpdates.emplace_back();
    ProtoBuf::ServerListEntry_Update* update = &entry->pendingUpdates.back();
    update->set_status(uint32_t(entry->status));
    update->set_version(version + 1);
    update->set_sequence_number(
            context->coordinatorService->updateManager.nextSequenceNumber());
    entry->sync(context->externalStorage);

    // Notify local ServerTrackers about the change.
    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->enqueueChange(*entry, event);
    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->fireCallback();

    // Begin the process of notifying all the servers in the cluster.
    pushUpdate(lock, entry);
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

        if (entry.services.hasAny(services)) {
            ProtoBuf::ServerList_Entry& protoBufEntry(*protoBuf->add_server());
            entry.serialize(&protoBufEntry);
        }
    }

    protoBuf->set_version_number(version);
    protoBuf->set_type(ProtoBuf::ServerList_Type_FULL_LIST);
}

/**
 * Try to create one or more new replication groups, if there are backups
 * that are not currently part of any replication group
 *
 * \param lock
 *      Explicity needs CoordinatorServerList lock.
 */
void
CoordinatorServerList::createReplicationGroups(const Lock& lock)
{
    // Create a list of all servers that do not belong to a replication group
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

    // Use the list of available backups to create new replication groups.
    while (freeBackups.size() >= replicationGroupSize) {
        maxReplicationId++;
        for (uint32_t i = 0; i < replicationGroupSize; i++) {
            Entry* e = getEntry(freeBackups.back());
            freeBackups.pop_back();
            e->replicationId = maxReplicationId;
            persistAndPropagate(lock, e, ServerChangeEvent::SERVER_ADDED);
            LOG(NOTICE, "Server %s is now in replication group %lu",
                    e->serverId.toString().c_str(), e->replicationId);
        }
    }
}

/**
 * Delete a replication group, making its backups available for use in
 * other groups.
 *
 * \param lock
 *      Explicity needs CoordinatorServerList lock.
 * \param groupId
 *      Replication group to delete.
 */
void
CoordinatorServerList::removeReplicationGroup(const Lock& lock,
        uint64_t groupId)
{
    // Cannot remove groupId 0, since it is the default groupId.
    if (groupId == 0) {
        return;
    }
    for (size_t i = 0; i < isize(); i++) {
        if (serverList[i].entry &&
                serverList[i].entry->isBackup() &&
                serverList[i].entry->replicationId == groupId) {
            Entry* e = serverList[i].entry.get();
            LOG(NOTICE, "Removed server %s from replication group %lu",
                    e->serverId.toString().c_str(), e->replicationId);
            e->replicationId = 0;
            persistAndPropagate(lock, e, ServerChangeEvent::SERVER_ADDED);
        }
    }
}

/**
 * This method is invoked during coordinator crash recovery to repair
 * any replication groups that are incomplete (they might have been
 * left in inconsistent state if the coordinator crashed in the wrong
 * place).
 *
 * \param lock
 *      Ensures that the caller has required the monitor lock. Not
 *      actually used in this method.
 */
void
CoordinatorServerList::repairReplicationGroups(const Lock& lock)
{
    // The following hash is indexed by replication group number; the value
    // holds the number of servers in that group.
    std::unordered_map<uint64_t, uint32_t> counts;

    // Scan through all of the servers to compute the number of servers
    // in each group.
    for (size_t i = 0; i < isize(); i++) {
        if (serverList[i].entry) {
            Entry* entry = serverList[i].entry.get();
            if (entry->isBackup() && entry->replicationId != 0) {
                counts[entry->replicationId]++;
                if (entry->replicationId > maxReplicationId) {
                    maxReplicationId = entry->replicationId;
                }
            }
        }
    }

    // Now scan through all of the counts. Delete any replication groups
    // that are incomplete.
    std::unordered_map<uint64_t, uint32_t>::iterator it;
    for (it = counts.begin(); it != counts.end(); it++) {
        if (it->second != replicationGroupSize) {
            LOG(NOTICE, "Removing replication group %lu (has %d members)",
                    it->first, it->second);
            removeReplicationGroup(lock, it->first);
        }
    }

    // Finally, make new groups if possible.
    createReplicationGroups(lock);
}

/**
 * Given a server list entry whose contents have just been changed, arrange
 * for the changes to be propagated to all the other servers in the cluster.
 * This method queues an update for a separate updater thread, but returns
 * before the update has been fully propagated.
 *
 * \param lock
 *      Explicitly needs CoordinatorServerList lock.
 * \param entry
 *      Server list entry to propagate to the cluster.
 */
void
CoordinatorServerList::pushUpdate(const Lock& lock, Entry* entry)
{
    ++version;
    updates.emplace_back(version);
    ServerListUpdate* update = &(updates.back());
    update->incremental.set_version_number(version);
    update->incremental.set_type(ProtoBuf::ServerList_Type_UPDATE);
    entry->serialize(update->incremental.add_server());

    // Wake up the updater, if it was sleeping.
    hasUpdatesOrStop.notify_one();
}

/**
 * This method is called during recovery to add a new update to the
 * list. The twist here is that we may need to insert the update in the
 * middle of the list, since the order in which updates are discovered
 * is unpredictable. This method does not actually trigger the updater.
 *
 * \param lock
 *      Explicitly needs CoordinatorServerList lock.
 * \param entry
 *      Server list entry to propagate to the cluster.
 * \param version
 *      Server list version number for this update.
 */
void
CoordinatorServerList::insertUpdate(const Lock& lock, Entry* entry,
        uint64_t version)
{
    // Find the right place to insert this entry.
    std::deque<ServerListUpdate>::iterator it;
    for (it = updates.begin(); it != updates.end(); it++) {
        if (it->version == version) {
            DIE("Duplicated CSL entry version %lu for servers %s and %s",
                    version, entry->serverId.toString().c_str(),
                    ServerId(it->incremental.server(0).server_id())
                    .toString().c_str());
        }
        if (it->version > version) {
            break;
        }
    }

    // Note: "insert" is used instead of "emplace" below, because
    // emplace doesn't appear to work in gcc 4.4.7 (as of 10/2013).
    it = updates.insert(it, ServerListUpdate(version));
    it->incremental.set_version_number(version);
    it->incremental.set_type(ProtoBuf::ServerList_Type_UPDATE);
    entry->serialize(it->incremental.add_server());
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
        lastScan.noWorkFoundForEpoch = 0;
        lastScan.searchIndex = 0;
        lastScan.minVersion = version;
        stopUpdater = false;
        updaterThread.construct(&CoordinatorServerList::updateLoop, this);
    }

    // Tell it to start work regardless
    hasUpdatesOrStop.notify_one();
}

/**
 * Returns true if the current state of the server list has been
 * fully propagated to all of the other servers in the cluster, and
 * false if updates are still pending.
 *
 * \param lock
 *      explicity needs CoordinatorServerList lock
 */
bool
CoordinatorServerList::isClusterUpToDate(const Lock& lock) {
    return (maxConfirmedVersion == version);
}

/**
 * Causes a deletion of server list updates that are no longer needed
 * by the coordinator serverlist. This will delete all updates as old
 * or older than the CoordinatorServerList's current maxConfirmedVersion.
 *
 * This is safe to invoke at any time, but is typically called after a
 * new maxConfirmedVersion is set.
 *
  * \param lock
 *      explicity needs CoordinatorServerList lock
 */
void
CoordinatorServerList::pruneUpdates(const Lock& lock)
{
    if (maxConfirmedVersion > version) {
        DIE("CoordinatorServerList's  maxConfirmedVersion %lu is larger "
                "than its current version %lu. This should NEVER happen!",
                maxConfirmedVersion, version);
    }

    while (!updates.empty() && updates.front().version <= maxConfirmedVersion) {
        // The oldest update has now been fully propagated, so we can
        // remove it (after performing appropriate cleanups).
        ProtoBuf::ServerList* currentUpdate = &updates.front().incremental;
        assert(currentUpdate->type() ==
                ProtoBuf::ServerList::Type::ServerList_Type_UPDATE);
        assert(currentUpdate->server_size() == 1);
        const ProtoBuf::ServerList_Entry* currentEntry =
                &currentUpdate->server(0);

        // The oldest pending update in the ServerList entry should have the
        // same version as the update that just completed, in which case we
        // can delete the pending update.
        ServerId serverId = ServerId(currentEntry->server_id());
        Entry* entry = getEntry(serverId);
        if (entry != NULL) {
            while (1) {
                ProtoBuf::ServerListEntry_Update* updateFromEntry =
                        &entry->pendingUpdates.front();
                uint64_t completed = currentUpdate->version_number();
                uint64_t entryFirst = updateFromEntry->version();
                if (completed >= entryFirst) {
                    uint64_t sequenceNumber =
                            updateFromEntry->sequence_number();
                    // The following check is a convenience for tests; the
                    // value should never be 0 in production.
                    if (sequenceNumber != 0) {
                        context->coordinatorService->updateManager.
                                updateFinished(sequenceNumber);
                    }
                    entry->pendingUpdates.pop_front();
                }
                if (completed == entryFirst) {
                    break;
                }

                // We should never get here...
                LOG(ERROR, "version number mismatch in update for server "
                        "%s: completed update has version %lu, but first "
                        "version from entry is %lu",
                        serverId.toString().c_str(), completed, entryFirst);
                if (entryFirst > completed) {
                    break;
                }
            }
        }

        ServerStatus updateStatus =
                ServerStatus(currentEntry->status());
        if (updateStatus == ServerStatus::UP) {
            // An enlistServer operation has successfully completed.

        } else if (updateStatus == ServerStatus::CRASHED) {
            // A serverCrashed operation has completed. The server that
            // crashed may be still recovering.

        } else if (updateStatus == ServerStatus::REMOVE) {
            // This marks the final completion of serverCrashed() operation.
            // i.e., If the crashed server was a master, then its recovery
            // (sparked by a serverCrashed operation) has completed.
            // If it was not a master, then the serverCrashed operation
            // (which will not spark any recoveries) has completed.

            serverList[serverId.indexNumber()].entry.destroy();
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
 * This is the top-level method of a dedicated thread.
 *
 * Once invoked, this loop can be exited by calling haltUpdater() in another
 * thread.
 *
 */
void
CoordinatorServerList::updateLoop()
{
    try {
        while (!stopUpdater) {
            checkUpdates();
            if (activeRpcs.empty()) {
                waitForWork();
            }
        }
    } catch (const std::exception& e) {
        LOG(ERROR, "Fatal error in CoordinatorServerList: %s", e.what());
        throw;
    } catch (...) {
        LOG(ERROR, "Unknown fatal error in CoordinatorServerList.");
        throw;
    }
    TEST_LOG("Updater exited");
}

/**
 * Invoked by the updater thread to wait (sleep) until there are
 * more updates.
 */
void
CoordinatorServerList::waitForWork()
{
    Lock lock(mutex);

    while (maxConfirmedVersion == version && !stopUpdater) {
        updaterSleeping = true;
        hasUpdatesOrStop.wait(lock);
        updaterSleeping = false;
    }
}

/**
 * This method does most of the work of updateLoop; it is placed in a
 * separate method for ease of testing. Each call to this method checks
 * for RPCs that have been completed, then starts new RPCs if possible.
 * This method does not block.
 */
void
CoordinatorServerList::checkUpdates()
{
    // This method uses concurrent asynchronous RPCs and dynamically adjusts
    // the number of update RPCs that are outstanding. The goal is to use
    // just enough concurrency to keep this thread totally busy (by the
    // time we clean up one RPC and start another, some other RPC has
    // finished). We don't want to immediately start an RPC for every
    // available update: this could create a very large number of RPCs,
    // most of which would already have finished before we get all of them
    // started.

    // Phase 1: Scan active RPCs to see if any have completed.
    std::list<Tub<UpdateServerListRpc>*>::iterator it = activeRpcs.begin();
    while (it != activeRpcs.end()) {
        UpdateServerListRpc* rpc = (*it)->get();

        // Skip not-finished rpcs
        if (!rpc->isReady()) {
            it++;
            continue;
        }

        // Finished rpc found
        try {
            rpc->wait();
            workSuccess(rpc->id, rpc->getResponseHeader<
                    WireFormat::UpdateServerList>()->currentVersion);
        } catch (const ServerNotUpException& e) {
            workFailed(rpc->id);
        }
        (*it)->destroy();
        spareRpcs.push_back(*it);
        it = activeRpcs.erase(it);
    }

    // Phase 2: Start up to 1 new rpc
    if (spareRpcs.empty()) {
        Tub<UpdateServerListRpc>* rpcTub = new Tub<UpdateServerListRpc>;
        spareRpcs.push_back(rpcTub);
    }
    Tub<UpdateServerListRpc>* rpcTub = spareRpcs.back();
    if (getWork(rpcTub)) {
        (*rpcTub)->send();
        activeRpcs.push_back(rpcTub);
        spareRpcs.pop_back();
    }

    // Phase 3: delete any updates that have been fully reflected across
    // the entire cluster.
    if (activeRpcs.empty()) {
        Lock lock(mutex);
        pruneUpdates(lock);
    }
}

/**
 * Attempts to find servers that require updates and don't already
 * have outstanding update rpcs.
 *
 * This call MUST eventually be followed by a workSuccuss or workFailed call
 * with the serverId contained within the RPC at some point in
 * the future to ensure that internal metadata is reset for the server
 * to whom the RPC is intended.
 *
 * \param rpc
 *      If there is work to do, an outgoing UpdateServerList RPC will
 *      be generated here, including all of the data that needs to be sent
 *      to that server.  The send method for the RPC is not invoked here.
 *
 * \return
 *      true if an updatable server (work) was found
 *
 */
bool
CoordinatorServerList::getWork(Tub<UpdateServerListRpc>* rpc) {
    Lock lock(mutex);

    // Heuristic to prevent duplicate scans when no new work has shown up.
    if (serverList.size() == 0 ||
          (numUpdatingServers > 0 && lastScan.noWorkFoundForEpoch == version))
        return false;

    /**
     * Searches through the server list for servers that are eligible
     * for updates and are currently not updating.
     *
     * The search starts at lastScan.searchIndex, which is a marker for
     * where the last invocation of getWork() stopped before returning.
     * The search ends when either the entire server list has been scanned
     * (when we reach lastScan.searchIndex again) or when an outdated server
     * eligible for updates has been found.
     *
     * While scanning, the loop will also keep track of the minimum
     * verifiedVersion in the server list and will propagate this
     * value to maxConfirmedVersion. This is done to track whether
     * the server list is up to date and which old updates, which
     * are guaranteed to never be used again, can be pruned.
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
            if (server->verifiedVersion < version &&
                    server->updateVersion == server->verifiedVersion) {
                if (server->verifiedVersion == UNINITIALIZED_VERSION) {
                    // New server, send full server list
                    ProtoBuf::ServerList fullList;
                    serialize(lock, &fullList, {WireFormat::MASTER_SERVICE,
                            WireFormat::BACKUP_SERVICE});
                    rpc->construct(context, server->serverId, &fullList);
                    server->updateVersion = version;
                } else {
                    // Incremental update(s). Create an RPC containing all
                    // the updates that this server hasn't yet seen.
                    int updatesInRpc = 0;
                    for (size_t i = 0; i < updates.size(); i++) {
                        ServerListUpdate* update = &updates[i];
                        if (update->version <= server->verifiedVersion) {
                            continue;
                        }
                        if (updatesInRpc == 0) {
                            rpc->construct(context, server->serverId,
                                    &update->incremental);
                        } else {
                            (*rpc)->appendServerList(&update->incremental);
                        }
                        server->updateVersion = update->version;
                        updatesInRpc++;
                        if (updatesInRpc >= MAX_UPDATES_PER_RPC) {
                            break;
                        }
                    }
                }

                numUpdatingServers++;
                lastScan.searchIndex = i;
                return true;
            }
        }

        i = (i+1)%serverList.size();

        // Update statistics and remove updates that are no longer needed.
        if (i == 0) {
            maxConfirmedVersion = lastScan.minVersion;

            // Reinitialize minVersion for the next scan.  This choice of
            // initial value serves several purposes:
            // * If the server list contains no updatable servers, then the
            //   value won't change, and after the next scan we'll be able
            //   to discard all of the current updates.
            // * It prevents any new updates (which will have version numbers
            //   larger than this) from being garbage collected after the
            //   next scan. This is important, since those updates might have
            //   been added after we already scanned some entries.
            lastScan.minVersion = version;
            lastScan.completeScansSinceStart++;
            pruneUpdates(lock);
        }
    // While we haven't reached a full scan through the list yet.
    } while (i != lastScan.searchIndex);

    // If we get here, it means we have scanned every entry in the
    // server list without finding any new work to do.

    lastScan.noWorkFoundForEpoch = version;
    return false;
}

/**
 * Signals the success of updater to complete an update RPC. This
 * will update internal metadata to allow the target server to be
 * updatable again.
 *
 * Note that this should only be invoked AT MOST ONCE for each WorkUnit.
 *
 * \param id
 *      The serverId originally that succesfully responded to an update
 *      RPC.
 * \param currentVersion
 *      The server list version returned by the server that we just
 *      updated. Normally this will be the same as server->updateVersion,
 *      but it will differ if we are out of sync with the server being
 *      updated (which can happen after coordinator crashes), in which
 *      case the server may not have been able to apply the update(s) we
 *      sent. In any case, this parameter gives the truth about the
 *      server's current version.
 */
void
CoordinatorServerList::workSuccess(ServerId id, uint64_t currentVersion) {
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

        if (currentVersion == ~0lu) {
            // This situation is just a convenience for unit testing
            // (it makes it easier to set up pre-canned results from
            // RPCs). We should never get here in actual use.
            server->verifiedVersion = server->updateVersion;
        } else {
            server->verifiedVersion = server->updateVersion = currentVersion;
            if (currentVersion < maxConfirmedVersion) {
                DIE("Server list for server %s is so far out of date that we "
                        "can't fix it (its version: %lu, maxConfirmedVersion: "
                        "%lu)", server->serverId.toString().c_str(),
                        currentVersion, maxConfirmedVersion);
            }
        }
    }

    // If update didn't update all the way or it's the last server updating,
    // hint for a full scan to update maxConfirmedVersion.
    if (server->verifiedVersion < version)
        lastScan.noWorkFoundForEpoch = 0;
}

/**
 * Signals the failure of the updater to execute an update RPC. Causes
 * an internal rollback on metadata so that the server involved will
 * be retried later.
 *
 * \param id
 *      The server whose update failed.
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

    auto* part = request.emplaceAppend<
            WireFormat::UpdateServerList::Request::Part>();

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
    uint32_t sizeBefore = request.size();

    auto* part = request.emplaceAppend<
            WireFormat::UpdateServerList::Request::Part>();

    part->serverListLength = serializeToRequest(&request, list);

    uint32_t sizeAfter = request.size();
    if (sizeAfter > Transport::MAX_RPC_LEN) {
        request.truncate(sizeBefore);
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
    , verifiedVersion(UNINITIALIZED_VERSION)
    , updateVersion(UNINITIALIZED_VERSION)
    , pendingUpdates()
{
}

/**
 * Construct a new Entry, which contains the data a coordinator
 * needs to maintain about an enlisted server.
 *
 * \param serverId
 *      The ServerId of the server this entry describes.
 * \param serviceLocator
 *      The ServiceLocator string that can be used to address this
 *      entry's server.
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
    , verifiedVersion(UNINITIALIZED_VERSION)
    , updateVersion(UNINITIALIZED_VERSION)
    , pendingUpdates()
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

/**
 * Write a persistent copy of a server list entry to external storage,
 * so that it will survive coordinator crashes. If a new sequence number
 * is to be associated with this update, it is up to the caller to
 * allocate that and stored in the entry.
 * 
 * \param externalStorage
 *      Storage system in which to persist this entry.
 */
void
CoordinatorServerList::Entry::sync(ExternalStorage* externalStorage)
{
    ProtoBuf::ServerListEntry externalInfo;
    externalInfo.set_services(services.serialize());
    externalInfo.set_server_id(serverId.getId());
    externalInfo.set_service_locator(serviceLocator);
    externalInfo.set_expected_read_mbytes_per_sec(expectedReadMBytesPerSec);
    externalInfo.set_status(uint32_t(status));
    externalInfo.set_replication_id(replicationId);
    *externalInfo.mutable_master_recovery_info() = masterRecoveryInfo;
    foreach (ProtoBuf::ServerListEntry_Update& update, pendingUpdates) {
        *(externalInfo.add_update()) = update;
    }

    // Compute the name of the external storage object for this entry:
    // it is based on the index of the entry in the server list. This
    // approach means that we don't have to worry about garbage collecting
    // the external objects when servers die: the external object will
    // record the death, then be overwritten when a new server is allocated
    // the same slot in the server list.
    char objectName[30];
    snprintf(objectName, sizeof(objectName), "servers/%d",
            serverId.indexNumber());

    string str;
    externalInfo.SerializeToString(&str);
    externalStorage->set(ExternalStorage::UPDATE, objectName, str.c_str(),
            downCast<int>(str.length()));
}
} // namespace RAMCloud
