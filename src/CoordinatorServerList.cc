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

/**
 * \file
 * This file implements the CoordinatorServerList class.
 */

#include "Common.h"
#include "CoordinatorServerList.h"
#include "MembershipClient.h"
#include "ShortMacros.h"
#include "ServerTracker.h"
#include "TransportManager.h"

namespace RAMCloud {

//////////////////////////////////////////////////////////////////////
// CoordinatorServerList Public Methods
//////////////////////////////////////////////////////////////////////

/**
 * Constructor for CoordinatorServerList.
 */
CoordinatorServerList::CoordinatorServerList(Context& context)
    : context(context)
    , mutex()
    , serverList()
    , numberOfMasters(0)
    , numberOfBackups(0)
    , versionNumber(0)
    , trackers()
{
}

/**
 * Destructor for CoordinatorServerList.
 */
CoordinatorServerList::~CoordinatorServerList()
{
}

/**
 * Add a new server to the CoordinatorServerList and generate a new, unique
 * ServerId for it.
 *
 * After an add() but before sending \a update to the cluster
 * incrementVersion() must be called.  Also, \a update can contain remove,
 * crash, and add notifications, but removals/crashes must precede additions
 * in the update to ensure ordering guarantees about notifications related to
 * servers which re-enlist.  For now, this means calls to remove()/crashed()
 * must proceed call to add() if they have a common \a update.
 *
 * The addition will be pushed to all registered trackers and those with
 * callbacks will be notified.
 *
 * \param serviceLocator
 *      The ServiceLocator string of the server to add.
 * \param serviceMask
 *      Which services this server supports.
 *  \param readSpeed
 *      Speed of the storage on the enlisting server if it includes a backup
 *      service.  Argument is ignored otherwise.
 * \param update
 *      Cluster membership update message to append a serialized add
 *      notification to.
 * \return
 *      The unique ServerId assigned to this server.
 */
ServerId
CoordinatorServerList::add(string serviceLocator,
                           ServiceMask serviceMask,
                           uint32_t readSpeed,
                           ProtoBuf::ServerList& update)
{
    Lock _(mutex);
    uint32_t index = firstFreeIndex();

    auto& pair = serverList[index];
    ServerId id(index, pair.nextGenerationNumber);
    pair.nextGenerationNumber++;
    pair.entry.construct(id, serviceLocator, serviceMask);

    if (serviceMask.has(MASTER_SERVICE)) {
        numberOfMasters++;
    }

    if (serviceMask.has(BACKUP_SERVICE)) {
        numberOfBackups++;
        pair.entry->expectedReadMBytesPerSec = readSpeed;
    }

    ProtoBuf::ServerList_Entry& protoBufEntry(*update.add_server());
    pair.entry->serialize(protoBufEntry);

    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->enqueueChange(*pair.entry, ServerChangeEvent::SERVER_ADDED);
    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->fireCallback();

    return id;
}

/**
 * Mark a server as crashed in the list (when it has crashed and is
 * being recovered and resources [replicas] for its recovery must be
 * retained).
 *
 * This is a no-op of the server is already marked as crashed;
 * the effect is undefined if the server's status is DOWN.
 *
 * After a crashed() but before sending \a update to the cluster
 * incrementVersion() must be called.  Also, \a update can contain remove,
 * crash, and add notifications, but removals/crashes must precede additions
 * in the update to ensure ordering guarantees about notifications related to
 * servers which re-enlist.  For now, this means calls to remove()/crashed()
 * must proceed call to add() if they have a common \a update.
 *
 * The addition will be pushed to all registered trackers and those with
 * callbacks will be notified.
 *
 * \param serverId
 *      The ServerId of the server to remove from the CoordinatorServerList.
 *      It must not have been removed already (see remove()).
 * \param update
 *      Cluster membership update message to append a serialized crash
 *      notification to.
 */
void
CoordinatorServerList::crashed(ServerId serverId,
                               ProtoBuf::ServerList& update)
{
    Lock lock(mutex);
    crashed(lock, serverId, update);
}

/**
 * Remove a server from the list, typically when it is no longer part of
 * the system and we don't care about it anymore (it crashed and has
 * been properly recovered).
 *
 * This method may actually append two entries to \a update (see below).
 *
 * After a remove() but before sending \a update to the cluster
 * incrementVersion() must be called.  Also, \a update can contain remove,
 * crash, and add notifications, but removals/crashes must precede additions
 * in the update to ensure ordering guarantees about notifications related to
 * servers which re-enlist.  For now, this means calls to remove()/crashed()
 * must proceed call to add() if they have a common \a update.
 *
 * The addition will be pushed to all registered trackers and those with
 * callbacks will be notified.
 *
 * \param serverId
 *      The ServerId of the server to remove from the CoordinatorServerList.
 *      It must be in the list (either UP or CRASHED).
 * \param update
 *      Cluster membership update message to append a serialized removal
 *      notification to.  A crash notification will be appended before the
 *      removal notification if this server was removed while in UP status.
 */
void
CoordinatorServerList::remove(ServerId serverId,
                              ProtoBuf::ServerList& update)
{
    Lock lock(mutex);
    uint32_t index = serverId.indexNumber();
    if (index >= serverList.size() || !serverList[index].entry ||
        serverList[index].entry->serverId != serverId) {
        throw NoSuchServer(HERE,
            format("Invalid ServerId (%lu)", serverId.getId()));
    }

    crashed(lock, serverId, update);

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
 * Increments the list's version number and sets the version number on
 * \a update to match, this must be called after remove()/add() calls have
 * changed the list but before the update message has been sent to the
 * cluster members.
 */
void
CoordinatorServerList::incrementVersion(ProtoBuf::ServerList& update)
{
    Lock _(mutex);
    versionNumber++;
    update.set_version_number(versionNumber);
}

/**
 * Modify the min open segment id associated with a specific server.
 *
 * \param serverId
 *      Server whose min open segment id is being changed.
 * \param segmentId
 *      New min open segment id for the server \a serverId.
 *      If the current min open segment id is at least as high as
 *      \a segmentId then the current value is not changed.
 * \throw
 *      Exception is thrown if the given ServerId is not in this list.
 */
void
CoordinatorServerList::setMinOpenSegmentId(ServerId serverId,
                                           uint64_t segmentId)
{
    Lock _(mutex);
    Entry& entry = const_cast<Entry&>(getReferenceFromServerId(serverId));
    if (entry.minOpenSegmentId < segmentId)
        entry.minOpenSegmentId = segmentId;
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
    Lock _(mutex);
    Entry& entry = const_cast<Entry&>(getReferenceFromServerId(serverId));
    entry.replicationId = replicationId;
}

/**
 * Obtain the locator associated with the given ServerId.
 *
 * \param id
 *      The ServerId to look up the locator for.
 * \return
 *      The ServiceLocator string assocated with the given ServerId.
 * \throw Exception
 *      Exception is thrown if the given ServerId is not in this list.
 */
const char*
CoordinatorServerList::getLocator(ServerId id) const
{
    Lock _(mutex);
    return getReferenceFromServerId(id).serviceLocator.c_str();
}

/**
 * Open a session to the given ServerId. This method simply calls through to
 * TransportManager::getSession. See the documentation there for exceptions
 * that may be thrown.
 *
 * \throw Exception
 *      Exception is thrown if the given ServerId is not in this list.
 */
Transport::SessionRef
CoordinatorServerList::getSession(ServerId id) const
{
    return context.transportManager->getSession(getLocator(id), id);
}

/**
 * Indicate whether a particular server is still believed to be
 * actively participating in the cluster.
 *
 * \param id
 *      Identifier for a particular server.
 * \return
 *      Returns true if the server given by #id exists in the server
 *      list and its state is "up"; returns false otherwise.
 */
bool
CoordinatorServerList::isUp(ServerId id) const
{
    Lock _(mutex);
    uint32_t index = id.indexNumber();
    return index < serverList.size() && serverList[index].entry
            && serverList[index].entry->serverId == id
            && serverList[index].entry->status == ServerStatus::UP;
}

/**
 * Returns a copy of the details associated with the given ServerId.
 *
 * \param serverId
 *      ServerId to look up in the list.
 * \throw
 *      Exception is thrown if the given ServerId is not in this list.
 */
CoordinatorServerList::Entry
CoordinatorServerList::operator[](const ServerId& serverId) const
{
    Lock _(mutex);
    return getReferenceFromServerId(serverId);
}

/**
 * Returns a copy of the details associated with the given position
 * in the server list or empty if the position in the list is
 * unoccupied.
 *
 * \param index
 *      Position of entry in the server list to return a copy of.
 * \throw
 *      Exception is thrown if the given ServerId is not in this list.
 */
Tub<CoordinatorServerList::Entry>
CoordinatorServerList::operator[](size_t index) const
{
    Lock _(mutex);
    if (index >= serverList.size())
        throw Exception(HERE, format("Index beyond array length (%zd)", index));
    return serverList[index].entry;
}

/**
 * Return true if the given serverId is in this list regardless of
 * whether it is crashed or not.  This can be used to check membership,
 * rather than having to try and catch around the index operator.
 */
bool
CoordinatorServerList::contains(ServerId serverId) const
{
    Lock _(mutex);
    if (!serverId.isValid())
        return false;

    uint32_t index = serverId.indexNumber();

    if (index >= serverList.size())
        return false;

    if (!serverList[index].entry)
        return false;

    return serverList[index].entry->serverId == serverId;
}

/**
 * Return the number of valid indexes in this list. Valid does not mean that
 * they're occupied, only that they are within the bounds of the array.
 */
size_t
CoordinatorServerList::size() const
{
    Lock _(mutex);
    return serverList.size();
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
    serialize(protoBuf, {MASTER_SERVICE, BACKUP_SERVICE});
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
 * Issue a cluster membership update to all enlisted servers in the system
 * that are running the MembershipService.
 *
 * Currently this call happens synchronously on the calling thread, but
 * eventually the CoordinatorServerList should provide a context to
 * send these updates automatically and asynchronously.
 * TODO(stutsman): Implement this feature.
 *
 * \param update
 *      Protocol Buffer containing the update to be sent.
 * \param excludeServerId
 *      ServerId of a server that is not to receive this update. This is
 *      used to avoid sending an update message to a server immediately
 *      following its enlistment (since we'll be sending the entire list
 *      instead).
 */
void
CoordinatorServerList::sendMembershipUpdate(ProtoBuf::ServerList& update,
                                            ServerId excludeServerId)
{
    Lock lock(mutex);

    Tub<ProtoBuf::ServerList> serializedServerList;

    MembershipClient client(context);
    for (size_t i = 0; i < serverList.size(); i++) {
        Tub<Entry>& entry = serverList[i].entry;
        if (!entry ||
            entry->status != ServerStatus::UP ||
            !entry->services.has(MEMBERSHIP_SERVICE))
            continue;
        if (entry->serverId == excludeServerId)
            continue;

        bool succeeded = false;
        try {
            succeeded =
                client.updateServerList(entry->serviceLocator.c_str(), update);
        } catch (const TransportException& e) {
            // It's suspicious that pushing the update failed, but
            // perhaps it's best to wait to try the full list push
            // before jumping to conclusions.
            LOG(NOTICE, "Failed to send cluster membership update to %lu: %s",
                entry->serverId.getId(), e.what());
        }

        // If this server had missed a previous update it will return
        // failure and expect us to push the whole list again.
        if (!succeeded) {
            LOG(NOTICE, "Server %lu had lost an update. Sending whole list.",
                entry->serverId.getId());
            if (!serializedServerList) {
                serializedServerList.construct();
                serialize(lock, *serializedServerList);
            }
            try {
                client.setServerList(entry->serviceLocator.c_str(),
                                     *serializedServerList);
            } catch (const TransportException& e) {
                // TODO(stutsman): Things aren't looking good for this
                // server.  The coordinator will probably want to investigate
                // the server and evict it.
                LOG(NOTICE, "Failed to send full cluster server list to %lu "
                    "after it failed to accept the update: %s",
                    entry->serverId.getId(), e.what());
            }
        }

        LOG(DEBUG, "Server list update sent to server %lu",
            entry->serverId.getId());
    }
}

/**
 * Register a ServerTracker with this list. Any updates to this
 * list (additions, removals, or crashes) will be propagated to the tracker.
 * The current list of hosts will be pushed to the tracker immediately so
 * that its state is synchronised with this list.
 *
 * \throw ServerListException
 *      An exception is thrown if the same tracker is registered more
 *      than once.
 */
void
CoordinatorServerList::registerTracker(ServerTrackerInterface& tracker)
{
    Lock _(mutex);

    auto it = std::find(trackers.begin(), trackers.end(), &tracker);
    if (it != trackers.end()) {
        throw ServerListException(HERE,
            "Cannot register the same tracker twice!");
    }

    trackers.push_back(&tracker);

    // Push all known servers which are crashed first.
    // Order is important to guarantee that if one server replaced another
    // during enlistment that the registering tracker queue will have the
    // crash event for the replaced server before the add event of the
    // server which replaced it.
    foreach (const auto& server, serverList) {
        if (!server.entry || server.entry->status != ServerStatus::CRASHED)
            continue;
        const Entry& entry = *server.entry;
        ServerDetails details = entry;
        details.status = ServerStatus::UP;
        tracker.enqueueChange(details, ServerChangeEvent::SERVER_ADDED);
        tracker.enqueueChange(entry, ServerChangeEvent::SERVER_CRASHED);
    }
    // Push all known server which are up.
    foreach (const auto& server, serverList) {
        if (!server.entry || server.entry->status != ServerStatus::UP)
            continue;
        tracker.enqueueChange(*server.entry, ServerChangeEvent::SERVER_ADDED);
    }
    tracker.fireCallback();
}

/**
 * Unregister a ServerTracker that was previously registered with this
 * list. Doing so will cease all update propagation.
 */
void
CoordinatorServerList::unregisterTracker(ServerTrackerInterface& tracker)
{
    Lock _(mutex);
    for (auto it = trackers.begin(); it != trackers.end(); ++it) {
        if (*it == &tracker) {
            trackers.erase(it);
            break;
        }
    }
}

//////////////////////////////////////////////////////////////////////
// CoordinatorServerList Private Methods
//////////////////////////////////////////////////////////////////////

// See docs on public version.
// This version doesn't acquire locks since it is used internally.
void
CoordinatorServerList::crashed(const Lock& lock,
                               ServerId serverId,
                               ProtoBuf::ServerList& update)
{
    uint32_t index = serverId.indexNumber();
    if (index >= serverList.size() || !serverList[index].entry ||
        serverList[index].entry->serverId != serverId) {
        throw NoSuchServer(HERE,
            format("Invalid ServerId (%lu)", serverId.getId()));
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
 * \param serverId
 *      The ServerId to look up in the list.
 *
 * \throw
 *      An exception is thrown if the given ServerId is not in this list.
 */
const CoordinatorServerList::Entry&
CoordinatorServerList::getReferenceFromServerId(const ServerId& serverId) const
{
    uint32_t index = serverId.indexNumber();
    if (index < serverList.size() && serverList[index].entry
            && serverList[index].entry->serverId == serverId)
        return *serverList[index].entry;

    throw NoSuchServer(HERE,
        format("Invalid ServerId (%lu)", serverId.getId()));
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
    serialize(lock, protoBuf, {MASTER_SERVICE, BACKUP_SERVICE});
}

/**
 * Serialize this list (or part of it, depending on which services the
 * caller wants) to a protocol buffer. Not all state is included, but
 * enough to be useful for disseminating cluster membership information
 * to other servers. Only used internally in CoordinatorServerList; requires
 * a lock on #mutex is held for duration of call.
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

        if ((entry.isMaster() && services.has(MASTER_SERVICE)) ||
            (entry.isBackup() && services.has(BACKUP_SERVICE))) {
            ProtoBuf::ServerList_Entry& protoBufEntry(*protoBuf.add_server());
            entry.serialize(protoBufEntry);
        }
    }

    protoBuf.set_version_number(versionNumber);
}

//////////////////////////////////////////////////////////////////////
// CoordinatorServerList::Entry Methods
//////////////////////////////////////////////////////////////////////

/**
 * Construct a new Entry, which contains no valid information.
 */
CoordinatorServerList::Entry::Entry()
    : ServerDetails()
    , minOpenSegmentId()
    , replicationId()
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
    , minOpenSegmentId(0)
    , replicationId(0)
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
}

} // namespace RAMCloud
