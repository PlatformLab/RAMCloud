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

#include <unordered_set>

#include "Common.h"
#include "ServerList.h"
#include "ServerTracker.h"
#include "ShortMacros.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Constructor for ServerList.

 * \param context
 *      Overall information about the RAMCloud server
 */
ServerList::ServerList(Context& context)
    : context(context),
      serverList(),
      version(0),
      trackers(),
      mutex()
{
}

/**
 * Destructor for ServerList.
 */
ServerList::~ServerList()
{
}

/**
 * Obtain the locator associated with the given ServerId.
 *
 * \param id
 *      The ServerId to look up the locator for.
 *
 * \return
 *      The ServiceLocator string assocated with the given ServerId.
 *
 * \throw ServerListException
 *      An exception is thrown if this ServerId is not in the list.
 *      This could happen due to a stale id that refers to a server
 *      that has since left the system. It may be possible in the
 *      future for other RPCs to refer to ServerIds that this machine
 *      does not yet know of.
 */
const char*
ServerList::getLocator(ServerId id)
{
    Lock lock(mutex);
    uint32_t index = id.indexNumber();
    if (index >= serverList.size() || !serverList[index] ||
      serverList[index]->serverId != id) {
        throw ServerListException(HERE, format(
            "ServerId %lu is not in the ServerList", *id));
    }

    return serverList[index]->serviceLocator.c_str();
}

/**
 * Indicate whether a particular server is still believed to be
 * actively participating in the cluster.
 *
 * \param id
 *      Identifier for a particular server.
 *
 * \return
 *      Returns true if the server given by #id exists in the server
 *      list and its state is "up"; returns false otherwise.
 */
bool
ServerList::isUp(ServerId id)
{
    Lock lock(mutex);
    uint32_t index = id.indexNumber();
    return index < serverList.size() && serverList[index]
            && serverList[index]->serverId == id
            && serverList[index]->status == ServerStatus::UP;
}

/**
 * Return a human-readable string representation for a server.
 *
 * \param id
 *      The ServerId for the server.
 *
 * \return
 *      The most informative human-readable string associated with the given
 *      ServerId.
 */
string
ServerList::toString(ServerId id)
{
    string locator;
    try {
        locator = getLocator(id);
    } catch (const ServerListException& e) {
        locator = "(locator unavailable)";
    }
    return format("server %lu at %s",
                  id.getId(),
                  locator.c_str());
}

/**
 * Return a human-readable string representation of a status.
 *
 * \return
 *      The string representing the status.
 */
string
ServerList::toString(ServerStatus status)
{
    switch (status) {
        case ServerStatus::UP:
            return "UP";
        case ServerStatus::CRASHED:
            return "CRASHED";
        case ServerStatus::DOWN:
            return "DOWN";
        default:
            return "UNKOWN";
    }
}

/**
 * Return a human-readable string representation of the contents of
 * the list.
 *
 * \return
 *      The string representing the contents of the list.
 */
string
ServerList::toString()
{
    Lock lock(mutex);

    string result;
    foreach (const auto& server, serverList) {
        if (!server)
            continue;
        result.append(
            format("server %lu at %s with %s is %s\n",
                   server->serverId.getId(),
                   server->serviceLocator.c_str(),
                   server->services.toString().c_str(),
                   toString(server->status).c_str()));
    }

    return result;
}

/**
 * Open a session to the given ServerId. This method simply calls through to
 * TransportManager::getSession. See the documentation there for exceptions
 * that may be thrown.
 *
 * \throw ServerListException
 *      A ServerListException is thrown if the given ServerId is not in this
 *      list.
 */
Transport::SessionRef
ServerList::getSession(ServerId id)
{
    return context.transportManager->getSession(getLocator(id), id);
}

/**
 * Return the current size of this list.
 */
uint32_t
ServerList::size()
{
    Lock lock(mutex);
    return downCast<uint32_t>(serverList.size());
}

/**
 * Return the ServerId associated with a given index. If there is none,
 * an invalid ServerId is returned (i.e. the isValid() method will return
 * false.
 */
ServerId
ServerList::operator[](uint32_t index)
{
    Lock lock(mutex);
    if (index >= serverList.size() || !serverList[index])
        return ServerId(/* invalid id */);
    return serverList[index]->serverId;
}

/**
 * Return true if the given ServerId is in the list, otherwise return false.
 * Notice, that even when true is returned the server may be in CRASHED
 * state rather than UP state.
 */
bool
ServerList::contains(ServerId serverId)
{
    Lock lock(mutex);
    return contains(lock, serverId);
}

/**
 * Get the version of this list, as set by #setVersion. Used to tell whether or
 * not the list of out of date with the coordinator (and other hosts).
 */
uint64_t
ServerList::getVersion()
{
    Lock lock(mutex);
    return version;
}

/**
 * Register a ServerTracker with this ServerList. Any updates to this
 * list (additions or removals) will be propagated to the tracker. The
 * current list of hosts will be pushed to the tracker immediately so
 * that its state is synchronised with this ServerList.
 *
 * \throw ServerListException
 *      An exception is thrown if the same tracker is registered more
 *      than once.
 */
void
ServerList::registerTracker(ServerTrackerInterface& tracker)
{
    Lock lock(mutex);

    bool alreadyRegistered =
        std::find(trackers.begin(), trackers.end(), &tracker) != trackers.end();
    if (alreadyRegistered) {
        throw ServerListException(HERE,
            "Cannot register the same tracker twice!");
    }

    trackers.push_back(&tracker);

    // Push all known servers which are crashed first.
    // Order is important to guarantee that if one server replaced another
    // during enlistment that the registering tracker queue will have the
    // crash event for the replaced server before the add event of the
    // server which replaced it.
    foreach (const Tub<ServerDetails>& server, serverList) {
        if (!server || server->status != ServerStatus::CRASHED)
            continue;
        ServerDetails details = *server;
        details.status = ServerStatus::UP;
        tracker.enqueueChange(details, ServerChangeEvent::SERVER_ADDED);
        tracker.enqueueChange(*server, ServerChangeEvent::SERVER_CRASHED);
    }
    // Push all known server which are up.
    foreach (const Tub<ServerDetails>& server, serverList) {
        if (!server || server->status != ServerStatus::UP)
            continue;
        tracker.enqueueChange(*server, ServerChangeEvent::SERVER_ADDED);
    }
    tracker.fireCallback();
}

/**
 * Unregister a ServerTracker that was previously registered with this
 * ServerList. Doing so will cease all update propagation.
 */
void
ServerList::unregisterTracker(ServerTrackerInterface& tracker)
{
    Lock lock(mutex);
    for (size_t i = 0; i < trackers.size(); i++) {
        if (trackers[i] == &tracker) {
            trackers.erase(trackers.begin() + i);
            break;
        }
    }
}

/**
 * Given a complete server list from the coordinator reconcile the list with
 * the local server list so they are consistent.  In addition, all registered
 * trackers will receive notification of all events related to servers they
 * are aware of along with notifications for any new servers in the updated
 * list.
 *
 * \param list
 *      A complete snapshot of the coordinator's server list.
 */
void
ServerList::applyFullList(const ProtoBuf::ServerList& list)
{
    Lock lock(mutex);

    LOG(NOTICE, "Got complete list of servers containing %d entries (version "
        "number %lu)", list.server_size(), list.version_number());

    // Build a temporary map of servers currently in the server list
    // so that we can efficiently evict down servers from the list.
    std::unordered_set<uint64_t> listIds;
    foreach (const auto& server, list.server()) {
        ServerStatus status = ServerStatus(server.status());
        if (status == ServerStatus::DOWN) {
            LOG(WARNING, "Coordinator provided server list contains servers "
                "which are down. Ignoring, but this is likely due to a "
                "serious bug and is likely to cause worse bugs: offending "
                "server id %lu", server.server_id());
        } else {
            assert(!RAMCloud::contains(listIds, server.server_id()));
            listIds.insert(server.server_id());
        }
    }

    // Order matters here.  First all downs are done for all servers, then all
    // crashes, then all adds.  This is important because when enlisting some
    // servers may "replace" others and a guarantee is given to them that
    // whenever tracker clients become aware of the enlisting server
    // through a tracker event queue that same tracker has already been made
    // aware of the crash event of the server id being replaced.

    // DOWNs are done first.
    foreach (const Tub<ServerDetails>& server, serverList) {
        if (!server)
            continue;
        assert(server->serverId.isValid());
        if (!RAMCloud::contains(listIds, server->serverId.getId())) {
            remove(server->serverId);
        }
    }

    // CRASHED is done next.
    foreach (const auto& server, list.server()) {
        if (ServerStatus(server.status()) != ServerStatus::CRASHED)
            continue;
        crashed(ServerId(server.server_id()), server.service_locator(),
                ServiceMask::deserialize(server.services()),
                server.expected_read_mbytes_per_sec());
    }

    // Finally UPs are done.
    foreach (const auto& server, list.server()) {
        if (ServerStatus(server.status()) != ServerStatus::UP)
            continue;
        add(ServerId(server.server_id()), server.service_locator(),
            ServiceMask::deserialize(server.services()),
            server.expected_read_mbytes_per_sec());
    }

    version = list.version_number();

    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->fireCallback();
}

/**
 * Apply a server list update from the coordinator to  the local server list
 * so they are consistent.  In addition, all registered  trackers will receive
 * notification of all events related to servers they  are aware of along with
 * notifications for any new servers in the updated list.
 *
 * \param update
 *      A complete snapshot of the coordinator's server list.
 * \return
 *      True if updates were lost and a full server list should be requested
 *      from the coordinator, or false if \a update was applied successfully.
 */
bool
ServerList::applyUpdate(const ProtoBuf::ServerList& update)
{
    Lock lock(mutex);

    // If this isn't the next expected update, request that the entire list
    // be pushed again.
    if (update.version_number() != (version + 1)) {
        LOG(NOTICE, "Update generation number is %lu, but last seen was %lu. "
            "Something was lost! Grabbing complete list again!",
            update.version_number(), version);
        return true;
    }

    LOG(NOTICE, "Got server list update (version number %lu)",
        update.version_number());

    foreach (const auto& server, update.server()) {
        ServerId id(server.server_id());
        assert(id.isValid());
        ServerStatus status = ServerStatus(server.status());
        const string& locator = server.service_locator();
        ServiceMask services =
            ServiceMask::deserialize(server.services());
        uint32_t readMBytesPerSec = server.expected_read_mbytes_per_sec();
        if (status == ServerStatus::UP) {
            LOG(NOTICE, "  Adding server id %lu (locator \"%s\") "
                         "with services %s and %u MB/s storage",
                *id, locator.c_str(), services.toString().c_str(),
                readMBytesPerSec);
            add(id, locator, services, readMBytesPerSec);
        } else if (status == ServerStatus::CRASHED) {
            if (!contains(lock, id)) {
                LOG(ERROR, "  Cannot mark server id %lu as crashed: The server "
                    "is not in our list, despite list version numbers matching "
                    "(%lu). Something is screwed up! Requesting the entire "
                    "list again.", *id, update.version_number());
                return true;
            }

            LOG(NOTICE, "  Marking server id %lu as crashed", id.getId());
            crashed(id, locator, services, readMBytesPerSec);
        } else if (status == ServerStatus::DOWN) {
            if (!contains(lock, id)) {
                LOG(ERROR, "  Cannot remove server id %lu: The server is "
                    "not in our list, despite list version numbers matching "
                    "(%lu). Something is screwed up! Requesting the entire "
                    "list again.", *id, update.version_number());
                return true;
            }

            LOG(NOTICE, "  Removing server id %lu", id.getId());
            remove(id);
        }
    }

    version = update.version_number();

    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->fireCallback();

    return false;
}

// - private -

/**
 * Return true if the given ServerId is in the list, otherwise return false.
 * Notice, that even when true is returned the server may be in CRASHED
 * state rather than UP state.
 */
bool
ServerList::contains(const Lock& lock, const ServerId serverId)
{
    uint32_t index = serverId.indexNumber();
    if (index >= serverList.size() || !serverList[index])
        return false;
    return serverList[index]->serverId == serverId;
}

/**
 * Add a new server to the ServerList along with some details.
 * All registered ServerTrackers will have the changes enqueued to them.
 * The caller is responsible for firing tracker callbacks if the
 * server list changed in response to this call.
 *
 * Upon successful return the slot in the server list which corresponds to
 * the indexNumber of \a id will reflect the passed in details and with
 * the server having an UP status.
 *
 * \param id
 *      The ServerId of the server to add.
 * \param locator
 *      The service locator of the server to add.
 * \param services
 *      Which services this server provides.
 * \param expectedReadMBytesPerSec
 *      If services.has(BACKUP_SERVICE) then this should describe the storage
 *      performance the server reported when enlisting with the coordiantor,
 *      otherwise the value is ignored.  In MB/s.
 */
bool
ServerList::add(ServerId id, const string& locator,
                ServiceMask services, uint32_t expectedReadMBytesPerSec)
{
    /*
     * Breakdown of the actions this method takes based on the id of the
     * existing server in the same slot id will reside in.
     *           | ids equal  | id is newer than entry
     * ----------+----------------------------------------
     *  Up       | No-op      | Crash, Down current; Up id
     *  Crashed  | Log/Ignore | Down current; Up id
     *  Down     | Columns indistinguishable; Up id
     */
    uint32_t index = id.indexNumber();

    if (index >= serverList.size())
        serverList.resize(index + 1);

    Tub<ServerDetails>& entry = serverList[index];
    if (entry) {
        if (id.generationNumber() < entry->serverId.generationNumber()) {
            // Add of older ServerId; drop it.
            LOG(WARNING, "Dropping addition of ServerId older than the current "
                "entry (%lu < %lu)!", id.getId(),
                entry->serverId.getId());
            return false;
        } else if (id.generationNumber() > entry->serverId.generationNumber()) {
            // Add of newer ServerId; need to play notifications to remove
            // current entry.
            LOG(WARNING, "Addition of %lu seen before removal of %lu! Issuing "
                "removal before addition.",
                id.getId(), entry->serverId.getId());
            remove(entry->serverId);
            // Fall through to do addition.
        } else { // Generations are equal
            if (entry->status == ServerStatus::UP) {
                // Nothing to do; already in the right status.
                LOG(WARNING, "Duplicate add of ServerId %lu!", id.getId());
            } else {
                // Something's not right; shouldn't see an add for a crashed
                // server.
                LOG(WARNING, "Add of ServerId %lu after it had already been "
                    "marked crashed; ignoring", id.getId());
            }
            return false;
        }
    }
    assert(!entry);

    entry.construct(id, locator, services,
                    expectedReadMBytesPerSec, ServerStatus::UP);
    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->enqueueChange(*entry, ServerChangeEvent::SERVER_ADDED);
    return true;
}

/**
 * Mark a server as crashed in the ServerList.
 * All registered ServerTrackers will have the changes enqueued to them.
 * The caller is responsible for firing tracker callbacks if the
 * server list changed in response to this call.
 *
 * The additional arguments besides \a id are used in the case that the
 * server must be marked CRASHED when the server was never marked as UP
 * (in this case the details of the server aren't in the list).
 *
 * Upon successful return the slot in the server list which corresponds to
 * the indexNumber of \a id will reflect the passed in details and with
 * the server having an CRASHED status.
 *
 * \param id
 *      The ServerId of the server to mark as crashed.
 * \param locator
 *      The service locator of the server to add (in the case that the details
 *      of the server were never added).
 * \param services
 *      Which services this server provides (in the case that the details of
 *      the server were never added).
 * \param expectedReadMBytesPerSec
 *      If services.has(BACKUP_SERVICE) then this should describe the storage
 *      performance the server reported when enlisting with the coordiantor,
 *      otherwise the value is ignored.  In MB/s.  Only used in the case that
 *      the details of the server were never added.
 */
bool
ServerList::crashed(ServerId id, const string& locator,
                    ServiceMask services, uint32_t expectedReadMBytesPerSec)
{
    /*
     * Breakdown of the actions this method takes based on the id of the
     * existing server in the same slot id will reside in.
     *           | ids equal  | id is newer than entry
     * ----------+-----------------------------------------------
     *  Up       | Crash      | Crash, Down current; Up, Crash id
     *  Crashed  | No-op      | Down current; Up id, Crash id
     *  Down     | Columns indistinguishable; Up, Crash id
     */
    uint32_t index = id.indexNumber();

    if (index >= serverList.size() || !serverList[index]) {
        // No existing entry; need to add first.
        add(id, locator, services, expectedReadMBytesPerSec);
    }

    Tub<ServerDetails>& entry = serverList[index];
    if (entry) {
        if (id.generationNumber() < entry->serverId.generationNumber()) {
            // Crash of older ServerId; drop it.
            LOG(WARNING, "Dropping crash of ServerId older than the current "
                "entry (%lu < %lu)!", id.getId(),
                entry->serverId.getId());
            return false;
        } else if (id.generationNumber() > entry->serverId.generationNumber()) {
            // Crash of newer ServerId; need to play notifications to remove
            // current entry and add id before marking it as crashed.
            LOG(WARNING, "Crash of %lu seen before crash of %lu! Issuing "
                "crash/removal before addition.",
                id.getId(), entry->serverId.getId());
            remove(entry->serverId);
            // We have a crash event for a server that was never added just
            // make up some unusable details about the server.  No one should
            // ever contact it and if they do the locator won't work.
            add(id, locator, services, expectedReadMBytesPerSec);
            // Fall through to do crash of id.
        } else { // Generations are equal.
            if (entry->status == ServerStatus::CRASHED) {
                // Nothing to do; already in the right status.
                LOG(WARNING, "Duplicate crash of ServerId %lu!", id.getId());
                return false;
            }
            // Fall through to do crash of id.
        }
    }

    // At this point the entry should exist, be for id, and should be up.
    assert(entry);
    assert(entry->serverId == id);
    assert(entry->status == ServerStatus::UP);

    entry->status = ServerStatus::CRASHED;
    foreach (ServerTrackerInterface* tracker, trackers) {
        tracker->enqueueChange(
            ServerDetails(entry->serverId, ServerStatus::CRASHED),
            ServerChangeEvent::SERVER_CRASHED);
    }

    return true;
}

/**
 * Remove a server from the ServerList.
 * All registered ServerTrackers will have the changes enqueued to them.
 * The caller is responsible for firing tracker callbacks if the
 * server list changed in response to this call.
 *
 * Upon successful return the slot in the server list which corresponds to
 * the indexNumber of \a id will be empty (which implies DOWN).
 *
 * \param id
 *      The ServerId of the server to remove from the ServerList.
 */
bool
ServerList::remove(ServerId id)
{
    /*
     * Breakdown of the actions this method takes based on the id of the
     * existing server in the same slot id will reside in.
     *           | ids equal or id is newer than entry
     * ----------+-----------------------------------------------
     *  Up       | Crash, Down current; ignore id
     *  Crashed  | Down current; ignore id
     *  Down     | Ignore id
     */
    uint32_t index = id.indexNumber();

    // If we're told to remove a server we're never heard of, just log
    // and ignore it. This shouldn't happen normally, but could in
    // theory if we never learn of a server and then hear about its
    // demise, or if a short-lived server's addition notification is
    // reordered and arrives after the removal notification, or if a
    // new server that occupies the same index has an addition
    // notification arrive before the previous one's removal.
    if (index >= serverList.size() ||
        !serverList[index] ||
        (id.generationNumber() <
             serverList[index]->serverId.generationNumber())) {
        LOG(WARNING, "Ignoring removal of unknown ServerId %lu", id.getId());
        return false;
    }

    ServerDetails& entry = *serverList[index];

    // In theory it's possible we could have missed both a prior removal and
    // the next addition, and then see the removal for something newer than
    // what's stored. Unlikely, but let's log it just in case.
    if (id.generationNumber() > entry.serverId.generationNumber()) {
        LOG(WARNING, "Removing ServerId %lu because removal for a newer "
            "generation number was received (%lu)",
            entry.serverId.getId(), id.getId());
    }

    // Be sure to use the stored id, not the advertised one, just in case
    // we're removing an older entry (see previous comment above).
    if (entry.status == ServerStatus::UP) {
        crashed(entry.serverId, entry.serviceLocator,
                entry.services, entry.expectedReadMBytesPerSec);
    }
    foreach (ServerTrackerInterface* tracker, trackers) {
        tracker->enqueueChange(
            ServerDetails(entry.serverId, ServerStatus::DOWN),
            ServerChangeEvent::SERVER_REMOVED);
    }

    serverList[index].destroy();
    return true;
}

} // namespace RAMCloud
