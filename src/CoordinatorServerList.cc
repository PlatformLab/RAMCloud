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
    , concurrentRPCs(5)
    , rpcTimeoutNs(0)  // 0 = infinite timeout
    , stopUpdater(true)
    , lastScan()
    , update()
    , updates()
    , hasUpdatesOrStop()
    , listUpToDate()
    , thread()
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
// CoordinatorServerList Protected Methods From AbstractServerList
//////////////////////////////////////////////////////////////////////
ServerDetails*
CoordinatorServerList::iget(ServerId id)
{
    uint32_t index = id.indexNumber();
    if ((index < serverList.size()) && serverList[index].entry) {
        ServerDetails* details = serverList[index].entry.get();
        if (details->serverId == id)
            return details;
    }
    return NULL;
}

ServerDetails*
CoordinatorServerList::iget(uint32_t index)
{
    return (serverList[index].entry) ? serverList[index].entry.get() : NULL;
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

//////////////////////////////////////////////////////////////////////
// CoordinatorServerList Public Methods
//////////////////////////////////////////////////////////////////////
/**
 * Add a new server to the CoordinatorServerList with a given ServerId.
 *
 * The result of this operation will be added in the class's update Protobuffer
 * intended for the cluster. To send out the update, call commitUpdate()
 * which will also increment the version number. Calls to remove()
 * and crashed() must proceed call to add() to ensure ordering guarantees
 * about notifications related to servers which re-enlist.
 *
 * The addition will be pushed to all registered trackers and those with
 * callbacks will be notified.
 *
 * \param serverId
 *      The serverId to be assigned to the new server.
 * \param serviceLocator
 *      The ServiceLocator string of the server to add.
 * \param serviceMask
 *      Which services this server supports.
 * \param readSpeed
 *      Speed of the storage on the enlisting server if it includes a backup
 *      service. Argument is ignored otherwise.
 */
void
CoordinatorServerList::add(ServerId serverId,
                           string serviceLocator,
                           ServiceMask serviceMask,
                           uint32_t readSpeed)
{
    Lock lock(mutex);
    add(lock, serverId, serviceLocator, serviceMask, readSpeed);
    commitUpdate(lock);
}

/**
 * Mark a server as crashed in the list (when it has crashed and is
 * being recovered and resources [replicas] for its recovery must be
 * retained).
 *
 * This is a no-op of the server is already marked as crashed;
 * the effect is undefined if the server's status is DOWN.
 *
 * The result of this operation will be added in the class's update Protobuffer
 * intended for the cluster. To send out the update, call commitUpdate()
 * which will also increment the version number. Calls to remove()
 * and crashed() must proceed call to add() to ensure ordering guarantees
 * about notifications related to servers which re-enlist.
 *
 * The addition will be pushed to all registered trackers and those with
 * callbacks will be notified.
 *
 * \param serverId
 *      The ServerId of the server to remove from the CoordinatorServerList.
 *      It must not have been removed already (see remove()).
 */
void
CoordinatorServerList::crashed(ServerId serverId)
{
    Lock lock(mutex);
    crashed(lock, serverId);
    commitUpdate(lock);
}

/**
 * Remove a server from the list, typically when it is no longer part of
 * the system and we don't care about it anymore (it crashed and has
 * been properly recovered).
 *
 * This method may actually append two entries to \a update (see below).
 *
 * The result of this operation will be added in the class's update Protobuffer
 * intended for the cluster. To send out the update, call commitUpdate()
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
CoordinatorServerList::remove(ServerId serverId)
{
    Lock lock(mutex);
    remove(lock, serverId);
    commitUpdate(lock);
}

/**
 * Generate a new, unique ServerId that may later be assigned to a server
 * using add().
 *
 * \return
 *      The unique ServerId generated.
 */
ServerId
CoordinatorServerList::generateUniqueId()
{
    uint32_t index = firstFreeIndex();

    auto& pair = serverList[index];
    ServerId id(index, pair.nextGenerationNumber);
    pair.nextGenerationNumber++;
    pair.entry.construct(id, "", ServiceMask());

    return id;
}


/**
 * Modify the min open segment id associated with a specific server.
 *
 * \param serverId
 *      Server whose min open segment id is being changed.
 * \param recoveryInfo
 *      Information the coordinator will need to safely recover the master
 *      at \a serverId. The information is opaque to the coordinator other
 *      than its master recovery routines, but, basically, this is used to
 *      prevent inconsistent open replicas from being used during recovery.
 * \throw
 *      Exception is thrown if the given ServerId is not in this list.
 */
void
CoordinatorServerList::setMasterRecoveryInfo(
        ServerId serverId, const ProtoBuf::MasterRecoveryInfo& recoveryInfo)
{
    Lock lock(mutex);
    Entry& entry = const_cast<Entry&>(getReferenceFromServerId(lock, serverId));
    entry.masterRecoveryInfo = recoveryInfo;
}

/**
 * Modify the replication group id associated with a specific server.
 *
 * \param serverId
 *      Server whose replication group id is being changed.
 * \param replicationId
 *      New replication group id for the server \a serverId.
 * \throw
 *      Exception is thrown if the given ServerId is not in this list.
 */
void
CoordinatorServerList::setReplicationId(ServerId serverId,
                                        uint64_t replicationId)
{
    Lock lock(mutex);
    Entry& entry = const_cast<Entry&>(getReferenceFromServerId(lock, serverId));
    if (entry.status != ServerStatus::UP) {
        return;
    }
    entry.replicationId = replicationId;
    ProtoBuf::ServerList_Entry& protoBufEntry(*update.add_server());
    entry.serialize(protoBufEntry);
    commitUpdate(lock);
}

/**
 * Returns a copy of the details associated with the given ServerId.
 * 
 * Note: This function explictly acquires a lock, and is hence to be used
 * only by functions external to CoordinatorServerList to prevent deadlocks.
 * If a function in CoordinatorServerList class (that has already acquired
 * a lock) wants to use this functionality, it should directly call
 * #getReferenceFromServerId function.
 *
 * \param serverId
 *      ServerId to look up in the list.
 * \throw
 *      Exception is thrown if the given ServerId is not in this list.
 */
CoordinatorServerList::Entry
CoordinatorServerList::operator[](const ServerId& serverId) const
{
    Lock lock(mutex);
    return getReferenceFromServerId(lock, serverId);
}

/**
 * Returns a copy of the details associated with the given position
 * in the server list or empty if the position in the list is
 * unoccupied.
 * 
 * Note: This function explictly acquires a lock, and is hence to be used
 * only by functions external to CoordinatorServerList to prevent deadlocks.
 * If a function in CoordinatorServerList class (that has already acquired
 * a lock) wants to use this functionality, it should directly call
 * #getReferenceFromServerId function.
 *
 * \param index
 *      Position of entry in the server list to return a copy of.
 * \throw
 *      Exception is thrown if the given ServerId is not in this list.
 */
Tub<CoordinatorServerList::Entry>
CoordinatorServerList::operator[](size_t index) const
{
    Lock lock(mutex);
    return getReferenceFromIndex(lock, index);
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
 * Finds a master in the list starting at some position in the list.
 *
 * \param startIndex
 *      Position in the list to start searching for a master.
 * \return
 *      If no backup is found in the remainder of the list then -1,
 *      otherwise the position of the first master in the list
 *      starting at or after \a startIndex. Also, -1 if
 *      \a startIndex is greater than or equal to the list size.
 */
uint32_t
CoordinatorServerList::nextMasterIndex(uint32_t startIndex) const
{
    Lock _(mutex);
    for (; startIndex < serverList.size(); startIndex++) {
        uint32_t i = startIndex;
        if (serverList[i].entry && serverList[i].entry->isMaster())
            break;
    }
    return (startIndex >= serverList.size()) ? -1 : startIndex;
}

/**
 * Finds a backup in the list starting at some position in the list.
 *
 * \param startIndex
 *      Position in the list to start searching for a backup.
 * \return
 *      If no backup is found in the remainder of the list then -1,
 *      otherwise the position of the first backup in the list
 *      starting at or after \a startIndex. Also, -1 if
 *      \a startIndex is greater than or equal to the list size.
 */
uint32_t
CoordinatorServerList::nextBackupIndex(uint32_t startIndex) const
{
    Lock _(mutex);
    for (; startIndex < serverList.size(); startIndex++) {
        uint32_t i = startIndex;
        if (serverList[i].entry && serverList[i].entry->isBackup())
            break;
    }
    return (startIndex >= serverList.size()) ? -1 : startIndex;
}

/**
 * Serialize the entire list to a Protocol Buffer form.
 *
 * \param[out] protoBuf
 *      Reference to the ProtoBuf to fill.
 */
void
CoordinatorServerList::serialize(ProtoBuf::ServerList& protoBuf) const
{
    serialize(protoBuf, {WireFormat::MASTER_SERVICE,
        WireFormat::BACKUP_SERVICE});
}

/**
 * Serialize this list (or part of it, depending on which services the
 * caller wants) to a protocol buffer. Not all state is included, but
 * enough to be useful for disseminating cluster membership information
 * to other servers.
 *
 * \param[out] protoBuf
 *      Reference to the ProtoBuf to fill.
 * \param services
 *      If a server has *any* service included in \a services it will be
 *      included in the serialization; otherwise, it is skipped.
 */
void
CoordinatorServerList::serialize(ProtoBuf::ServerList& protoBuf,
                                 ServiceMask services) const
{
    Lock lock(mutex);
    serialize(lock, protoBuf, services);
}

/**
 * Add a LogCabin entry id corresponding to the intial information for a server.
 *
 * \param serverId
 *      ServerId of the server for which the LogCabin entry id is being stored.
 *
 * \param entryId
 *      LogCabin entry id corresponding to the intial information for server.
 */
void
CoordinatorServerList::addServerInfoLogId(ServerId serverId,
                                          LogCabin::Client::EntryId entryId)
{
    Lock lock(mutex);
    Entry& entry = const_cast<Entry&>(getReferenceFromServerId(lock, serverId));
    entry.serverInfoLogId = entryId;
}

/**
 * Return the entry id corresponding to entry in LogCabin log
 * that has the intial information for the server.
 *
 * \param serverId
 *      ServerId of the server for whose initial information the
 *      LogCabin entry id is being requested.
 *
 * \return
 *      LogCabin entry id corresponding to the intial information for server.
 */
LogCabin::Client::EntryId
CoordinatorServerList::getServerInfoLogId(ServerId serverId)
{
    Lock lock(mutex);
    Entry& entry = const_cast<Entry&>(getReferenceFromServerId(lock, serverId));
    return entry.serverInfoLogId;
}

/**
 * Add a LogCabin entry id corresponding to the updates for a server.
 *
 * \param serverId
 *      ServerId of the server for which the LogCabin entry id is being stored.
 *
 * \param entryId
 *      LogCabin entry id corresponding to the updates for server.
 */
void
CoordinatorServerList::addServerUpdateLogId(ServerId serverId,
                                            LogCabin::Client::EntryId entryId)
{
    Lock lock(mutex);
    Entry& entry = const_cast<Entry&>(getReferenceFromServerId(lock, serverId));
    entry.serverUpdateLogId = entryId;
}

/**
 * Return the entry id corresponding to entry in LogCabin log
 * that has the updates for the server.
 *
 * \param serverId
 *      ServerId of the server for whose updates the
 *      LogCabin entry id is being requested.
 *
 * \return
 *      LogCabin entry id corresponding to the updates for server.
 */
LogCabin::Client::EntryId
CoordinatorServerList::getServerUpdateLogId(ServerId serverId)
{
    Lock lock(mutex);
    Entry& entry = const_cast<Entry&>(getReferenceFromServerId(lock, serverId));
    return entry.serverUpdateLogId;
}

// TODO(ankitak): I'm not catching the errors that can be thrown
// in the above 4 operations.

// See docs on public version.
// This version doesn't acquire locks and does not send out updates
// since it is used internally.
void
CoordinatorServerList::add(Lock& lock,
                           ServerId serverId,
                           string serviceLocator,
                           ServiceMask serviceMask,
                           uint32_t readSpeed)
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

    ProtoBuf::ServerList_Entry& protoBufEntry(*update.add_server());
    pair.entry->serialize(protoBufEntry);

    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->enqueueChange(*pair.entry, ServerChangeEvent::SERVER_ADDED);
    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->fireCallback();
}

// See docs on public version.
// This version doesn't acquire locks and does not send out updates
// since it is used internally.
void
CoordinatorServerList::crashed(const Lock& lock,
                               ServerId serverId)
{
    uint32_t index = serverId.indexNumber();
    if (index >= serverList.size() || !serverList[index].entry ||
        serverList[index].entry->serverId != serverId) {
        throw ServerListException(HERE,
            format("Invalid ServerId (%s)", serverId.toString().c_str()));
    }

    auto& entry = serverList[index].entry;
    if (entry->status == ServerStatus::CRASHED)
        return;
    assert(entry->status != ServerStatus::DOWN);

    if (entry->isMaster())
        numberOfMasters--;
    if (entry->isBackup())
        numberOfBackups--;

    entry->status = ServerStatus::CRASHED;

    ProtoBuf::ServerList_Entry& protoBufEntry(*update.add_server());
    entry->serialize(protoBufEntry);

    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->enqueueChange(*entry, ServerChangeEvent::SERVER_CRASHED);
    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->fireCallback();
}

// See docs on public version.
// This version doesn't acquire locks and does not send out updates
// since it is used internally.
void
CoordinatorServerList::remove(Lock& lock,
                              ServerId serverId)
{
    uint32_t index = serverId.indexNumber();
    if (index >= serverList.size() || !serverList[index].entry ||
        serverList[index].entry->serverId != serverId) {
        throw ServerListException(HERE,
            format("Invalid ServerId (%s)", serverId.toString().c_str()));
    }

    crashed(lock, serverId);

    auto& entry = serverList[index].entry;
    // Even though we destroy this entry almost immediately setting the state
    // gets the serialized update message's state field correct.
    entry->status = ServerStatus::DOWN;

    ProtoBuf::ServerList_Entry& protoBufEntry(*update.add_server());
    entry->serialize(protoBufEntry);

    Entry removedEntry = *entry;
    entry.destroy();

    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->enqueueChange(removedEntry, ServerChangeEvent::SERVER_REMOVED);
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
 * Obtain a reference to the entry associated with the given ServerId.
 *
 * \param lock
 *      Unused, but required to statically check that the caller is aware that
 *      a lock must be held on #mutex for this call to be safe.
 * \param serverId
 *      The ServerId to look up in the list.
 *
 * \throw
 *      An exception is thrown if the given ServerId is not in this list.
 */
const CoordinatorServerList::Entry&
CoordinatorServerList::getReferenceFromServerId(const Lock& lock,
                                                const ServerId& serverId) const
{
    uint32_t index = serverId.indexNumber();
    if (index < serverList.size() && serverList[index].entry
            && serverList[index].entry->serverId == serverId)
        return *serverList[index].entry;

    throw ServerListException(HERE,
        format("Invalid ServerId (%s)", serverId.toString().c_str()));
}

/**
 * Returns a copy of the details associated with the given position
 * in the server list or empty if the position in the list is
 * unoccupied.
 *
 * \param lock
 *      Unused, but required to statically check that the caller is aware that
 *      a lock must be held on #mutex for this call to be safe.
 * \param index
 *      Position of entry in the server list to return a copy of.
 * 
 * \throw
 *      Exception is thrown if the given ServerId is not in this list.
 */
Tub<CoordinatorServerList::Entry>
CoordinatorServerList::getReferenceFromIndex(const Lock& lock,
                                             size_t index) const
{
    if (index < serverList.size())
        return serverList[index].entry;

    throw Exception(HERE, format("Index beyond array length (%zd)", index));
}

/**
 * Serialize the entire list to a Protocol Buffer form. Only used internally in
 * CoordinatorServerList; requires a lock on #mutex is held for duration of call.
 *
 * \param lock
 *      Unused, but required to statically check that the caller is aware that
 *      a lock must be held on #mutex for this call to be safe.
 * \param[out] protoBuf
 *      Reference to the ProtoBuf to fill.
 */
void
CoordinatorServerList::serialize(const Lock& lock,
                                 ProtoBuf::ServerList& protoBuf) const
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
 *      Reference to the ProtoBuf to fill.
 * \param services
 *      If a server has *any* service included in \a services it will be
 *      included in the serialization; otherwise, it is skipped.
 */
void
CoordinatorServerList::serialize(const Lock& lock,
                                 ProtoBuf::ServerList& protoBuf,
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
            ProtoBuf::ServerList_Entry& protoBufEntry(*protoBuf.add_server());
            entry.serialize(protoBufEntry);
        }
    }

    protoBuf.set_version_number(version);
    protoBuf.set_type(ProtoBuf::ServerList_Type_FULL_LIST);
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


//////////////////////////////////////////////////////////////////////
// CoordinatorServerList Private Methods
//////////////////////////////////////////////////////////////////////

/**
 * Scans the server list to see if all entries eligible for server
 * list updates are up-to-date.
 *
 * \param lock
 *      explicity needs CoordinatorServerList lock
 *
 * \return
 *      true if entire list is up-to-date
 */
bool
CoordinatorServerList::isClusterUpToDate(const Lock& lock) {
    for (size_t i = 0; i < serverList.size(); i++) {
        if (!serverList[i].entry)
            continue;

        Entry& entry = *serverList[i].entry;
        if (entry.services.has(WireFormat::MEMBERSHIP_SERVICE) &&
                entry.status == ServerStatus::UP &&
                (entry.serverListVersion != version ||
                entry.isBeingUpdated > 0) ) {
            return false;
        }
    }

    return true;
}

/**
 * Increments the server list version and notifies the async updater to
 * propagate the buffered Protobuf::ServerList update. The buffered update
 * will be Clear()ed and empty updates are silently ignored.
 *
 * \param lock
 *      explicity needs CoordinatorServerList lock
 */
void
CoordinatorServerList::commitUpdate(const Lock& lock)
{
    // If there are no updates, don't generate a send.
    if (update.server_size() == 0)
        return;

    version++;
    update.set_version_number(version);
    update.set_type(ProtoBuf::ServerList_Type_UPDATE);
    updates.push_back(update);
    lastScan.noUpdatesFound = false;
    hasUpdatesOrStop.notify_one();
    update.Clear();
}

/**
 * Deletes past updates up to and including the param version. This
 * help maintain the updates list so that it does not get too large.
 *
  * \param lock
 *      explicity needs CoordinatorServerList lock
 *
 *  \param version
 *      the version to delete up to and including
 */
void
CoordinatorServerList::pruneUpdates(const Lock& lock, uint64_t version)
{
    assert(version <= this->version);

    while (!updates.empty() && updates.front().version_number() <= version)
        updates.pop_front();

    if (updates.empty())    // Empty list = no updates to send
        listUpToDate.notify_all();

    //TODO(syang0) ankitak -> This is where you detect oldest version.
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
    if (!thread) {
        stopUpdater = false;
        thread.construct(&CoordinatorServerList::updateLoop, this);
    }

    // Tell it to start work regardless
    hasUpdatesOrStop.notify_one();
}

/**
 * Stops the background updater. It cancel()'s all pending update rpcs
 * and leaves the cluster out-of-date. To force a synchronization point
 * before halting, call sync() first.
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
    if (thread && thread->joinable()) {
        thread->join();
        thread.destroy();
    }
}

/**
 * Main loop that checks for outdated servers and sends out rpcs. This is
 * intended to run thread separate from the master.
 *
 * Once called, this loop can be exited by calling haltUpdater().
 */
void
CoordinatorServerList::updateLoop()
{
    /**
     * updateSlots stores all the slots we've ever allocated. It can grow
     * as necessary. inUse stores the indeces of slots in updateSlotes
     * that are eligible for update rpcs. The free list are the left over
     * rpcs that are allocated but ineligible for updates.
     *
     * The motivation behind this is that we want just enough slots such
     * that by the time we loop all the way through the list, the rpcs we
     * sent out in the previous iteration would be done. We don't want too many
     * slots such that done rpcs wait for a long period of time before we
     * get back to them and constantly allocating/deallocating update slots
     * is expensive. Thus, the solution was to keep track of how many slots
     * are "inUse" and we'd have to iterate over. This list grows and shrinks
     * as necessary to achieve our goal outlined in the first sentence.
     */
    try {
        std::deque<UpdateSlot> updateSlots;
        std::list<uint64_t>::iterator it;
        std::list<uint64_t> inUse;
        std::list<uint64_t> free;

        // Prefill RPC slots
        for (uint64_t i = 0; i < concurrentRPCs; i++) {
            updateSlots.emplace_back();
            inUse.push_back(i);
        }

        while (!stopUpdater) {
            std::list<uint64_t>::iterator lastFree = inUse.end();
            uint32_t liveRpcs = 0;

            // Handle Rpc logic
            for (it = inUse.begin(); it != inUse.end(); it++) {
                if (dispatchRpc(updateSlots.at(*it)))
                    liveRpcs++;
                else
                    lastFree = it;
            }

            // Expand/Contract slots as necessary
            if (inUse.size() == liveRpcs && lastFree == inUse.end()) {
                // All slots are in use and there are no new free slots, Expand
                if (free.empty()) {
                    updateSlots.emplace_back();
                    free.push_back(updateSlots.size() - 1);
                }

                concurrentRPCs++;
                inUse.push_back(free.front());
                free.pop_front();
            } else if (inUse.size() < liveRpcs - 1) {
                // Contract
                concurrentRPCs--;
                free.push_back(*lastFree);
                inUse.erase(lastFree);
            }

            // If there are no live rpcs, wait for more updates
            if ( liveRpcs == 0 ) {
                Lock lock(mutex);

                while (!hasUpdates(lock) && !stopUpdater) {
                    assert(isClusterUpToDate(lock)); //O(n) check can be deleted
                    listUpToDate.notify_all();
                    hasUpdatesOrStop.wait(lock);
                }
            }
        }

        // if (stopUpdater) Stop all Rpcs
        for (it = inUse.begin(); it != inUse.end(); it++) {
            UpdateSlot& update = updateSlots[*it];
            if (update.rpc) {
                update.rpc->cancel();
                updateEntryVersion(update.serverId, update.originalVersion);
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
 * Core logic that handles starting rpcs, timeouts, and following up on them.
 *
 * \param update
 *      The UpdateSlot that as an rpc to manage
 *
 * \return
 *      true if the UpdateSlot contains an active rpc
 */
bool
CoordinatorServerList::dispatchRpc(UpdateSlot& update) {
    if (update.rpc) {
        // Check completion/ error
        if (update.rpc->isReady()) {
            uint64_t newVersion;
            try {
                update.rpc->wait();
                newVersion = update.protobuf.version_number();
            } catch (const ServerNotUpException& e) {
                newVersion = update.originalVersion;

                LOG(NOTICE, "Async update to %s occurred during/after it was "
                "crashed/downed in the CoordinatorServerList.",
                update.serverId.toString().c_str());
            }

            update.rpc.destroy();
            updateEntryVersion(update.serverId, newVersion);

            // Check timeout event
        } else {
            uint64_t ns = Cycles::toNanoseconds(Cycles::rdtsc() -
                                                update.startCycle);
            if (rpcTimeoutNs != 0 && ns > rpcTimeoutNs) {
                LOG(NOTICE, "ServerList update to %s timed out after %lu ms; "
                    "trying again later",
                    update.serverId.toString().c_str(),
                    ns / 1000 / 1000);
                update.rpc.destroy();
                updateEntryVersion(update.serverId, update.originalVersion);
            }
        }
    }

    // Valid update still in progress
    if (update.rpc)
        return true;

    // Else load new rpc and start if applicable
    if (!loadNextUpdate(update))
        return false;

    update.rpc.construct(context, update.serverId, &(update.protobuf));
    update.startCycle = Cycles::rdtsc();

    return true;
}

/**
 * Searches through the server list looking for servers that need
 * to be sent updates/full lists. This search omits entries that are
 * currently being updated which means false can be returned even if
 * !clusterIsUpToDate().
 *
 * This is internally used to search for entries that need to be sent
 * update rpcs that do not currently have an rpc attached to them.
 *
 * \param lock
 *      Needs exclusive CoordinatorServerlist lock
 *
 * \return
 *      true if there are server list entries that are out of date
 *      AND do not have rpcs sent to them yet.
 */
bool
CoordinatorServerList::hasUpdates(const Lock& lock)
{
    if (lastScan.noUpdatesFound || serverList.size() == 0)
        return false;

    size_t i = lastScan.searchIndex;
    do {
        if (i == 0) {
            pruneUpdates(lock, lastScan.minVersion);
            lastScan.minVersion = 0;
        }

        if (serverList[i].entry) {
            Entry& entry = *serverList[i].entry;
            if (entry.services.has(WireFormat::MEMBERSHIP_SERVICE) &&
                    entry.status == ServerStatus::UP )
            {
                 // Check for new minVersion
                uint64_t entryMinVersion = (entry.serverListVersion) ?
                    entry.serverListVersion : entry.isBeingUpdated;

                if (lastScan.minVersion == 0 || (entryMinVersion > 0 &&
                        entryMinVersion < lastScan.minVersion)) {
                    lastScan.minVersion = entryMinVersion;
                }

                // Check for Update eligibility
                if (entry.serverListVersion != version &&
                        !entry.isBeingUpdated) {
                    lastScan.searchIndex = i;
                    lastScan.noUpdatesFound = false;
                    return true;
                }
            }
        }

        i = (i+1)%serverList.size();
    } while (i != lastScan.searchIndex);

    lastScan.noUpdatesFound = true;
    return false;
}

/**
 * Loads the information needed to start an async update rpc to a server into
 * an UpdateSlot. The entity managing the UpdateSlot is MUST call back with
 * updateEntryVersion() regardless of rpc success or fail. Failure to do so
 * will result internal mismanaged state.
 *
 * This will return false if there are no entries that need an update.
 *
 * \param updateSlot
 *      the update slot to load the update info into
 *
 * \return bool
 *      true if update has been loaded; false if no servers require an update.
 *
 */
bool
CoordinatorServerList::loadNextUpdate(UpdateSlot& updateSlot)
{
    Lock lock(mutex);

    // Check for Updates
    if (!hasUpdates(lock))
        return false;

    // Grab entry that needs update
    // note: lastScan.searchIndex was set by hasUpdates(lock)
    Entry& entry = *serverList[lastScan.searchIndex].entry;
    lastScan.searchIndex = (lastScan.searchIndex + 1)%serverList.size();

    // Package info and return
    updateSlot.originalVersion = entry.serverListVersion;
    updateSlot.serverId = entry.serverId;

    if (entry.serverListVersion == 0) {
        updateSlot.protobuf.Clear();
        serialize(lock, updateSlot.protobuf,
                {WireFormat::MASTER_SERVICE, WireFormat::BACKUP_SERVICE});
        entry.isBeingUpdated = version;
    } else {
        assert(!updates.empty());
        assert(updates.front().version_number() <= version);
        assert(updates.back().version_number()  >= version);

        uint64_t head = updates.front().version_number();
        uint64_t targetVersion = entry.serverListVersion + 1;
        updateSlot.protobuf = updates.at(targetVersion - head);
        entry.isBeingUpdated = targetVersion;
    }

    return true;
}

/**
 * Updates the server list version of an entry. Updates to non-existent
 * serverIds will be ignored silently.
 *
 * \param serverId
 *      ServerId of the entry that has just been updated.
 * \param version
 *      The version to update that entry to.
 */
void
CoordinatorServerList::updateEntryVersion(ServerId serverId, uint64_t version)
{
    Lock lock(mutex);

    try {
        Entry& entry =
                const_cast<Entry&>(getReferenceFromServerId(lock, serverId));
        LOG(DEBUG, "server %s updated (%lu->%lu)", serverId.toString().c_str(),
                entry.serverListVersion, version);

        entry.serverListVersion = version;
        entry.isBeingUpdated = 0;

        if (version < this->version)
            lastScan.noUpdatesFound = false;

    } catch(ServerListException& e) {
        // Don't care if entry no longer exists.
    }
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
    , serverListVersion(0)
    , isBeingUpdated(0)
    , serverInfoLogId(0)
    , serverUpdateLogId(0)
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
    , serverListVersion(0)
    , isBeingUpdated(0)
    , serverInfoLogId(LogCabin::Client::EntryId(0))
    , serverUpdateLogId(LogCabin::Client::EntryId(0))
{
}

/**
 * Serialize this entry into the given ProtoBuf.
 */
void
CoordinatorServerList::Entry::serialize(ProtoBuf::ServerList_Entry& dest) const
{
    dest.set_services(services.serialize());
    dest.set_server_id(serverId.getId());
    dest.set_service_locator(serviceLocator);
    dest.set_status(uint32_t(status));
    if (isBackup())
        dest.set_expected_read_mbytes_per_sec(expectedReadMBytesPerSec);
    else
        dest.set_expected_read_mbytes_per_sec(0); // Tests expect the field.
    dest.set_replication_id(replicationId);
}
} // namespace RAMCloud
