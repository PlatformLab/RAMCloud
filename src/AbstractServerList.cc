/* Copyright (c) 2011-2015 Stanford University
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

#include "TransportManager.h"

#include "AbstractServerList.h"
#include "Cycles.h"
#include "PingClient.h"
#include "FailSession.h"
#include "ServerTracker.h"

namespace RAMCloud {
bool AbstractServerList::skipServerIdCheck = false;

/**
 * Constructor for AbstractServerList.

 * \param context
 *      Overall information about the RAMCloud server
 */
AbstractServerList::AbstractServerList(Context* context)
    : context(context)
    , isBeingDestroyed(false)
    , version(0)
    , trackers()
    , mutex()
{
    context->serverList = this;
}

/**
 * Destructor for AbstractServerList.
 */
AbstractServerList::~AbstractServerList()
{
    // Unregister All Trackers (clear their pointers)
    Lock _(mutex);
    isBeingDestroyed = true;

    for (size_t i = 0; i < trackers.size(); i++) {
        if (trackers[i])
            trackers[i]->setParent(NULL);
    }
}

/**
 * Obtain the locator associated with the given ServerId.
 *
 * \param id
 *      The ServerId to look up the locator for.
 *
 * \return
 *      The ServiceLocator string associated with the given ServerId.
 *
 * \throw ServerListException
 *      An exception is thrown if this ServerId is not in the list.
 */
string
AbstractServerList::getLocator(ServerId id)
{
    Lock _(mutex);
    ServerDetails* details = iget(id);
    if (details != NULL)
        return details->serviceLocator;

    throw ServerListException(HERE,
            format("Invalid ServerId (%s)", id.toString().c_str()));
}

/**
 * Returns the current status of a server in the cluster.
 *
 * \param id
 *      Identifier for a particular server.
 * \return
 *      Returns ServerStatus::UP if the server given by id is currently
 *      part of the cluster and believed to be operating normally.
 *      ServerStatus::CRASHED is returned if the server is part of the
 *      cluster but has crashed, and crash recovery has not completed yet.
 *      ServerStatus::REMOVE is returned if there is no such server in
 *      the cluster (or if there once was such a server, but it has crashed
 *      and been fully recovered).
 */
ServerStatus
AbstractServerList::getStatus(ServerId id) {
    Lock _(mutex);
    ServerDetails* details = iget(id);
    if (details == NULL) {
        return ServerStatus::REMOVE;
    }
    return details->status;
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
AbstractServerList::isUp(ServerId id) {
    Lock _(mutex);
    ServerDetails* details = iget(id);
    return (details != NULL) && (details->status == ServerStatus::UP);
}

/**
 * Return a session to the given ServerId; a FailSession is returned if the
 * given server doesn't exist or a session cannot be opened.
 *
 * \param id
 *      Identifier for a particular server.
 */
Transport::SessionRef
AbstractServerList::getSession(ServerId id)
{
    // This method is a bit tricky because we don't want to hold the
    // lock while opening a new session. This means it's possible that
    // two different threads might each discover that there is no cached
    // session and open new sessions in parallel.  In addition, it's
    // possible that a server could be deleted while a session is being
    // opened for it.
    string locator;
    {
        Lock _(mutex);
        ServerDetails* details = iget(id);
        if (details == NULL)
            return FailSession::get();
        if (details->session != NULL)
            return details->session;
        locator = details->serviceLocator;
    }

    // No cached session. Open a new session.
    Transport::SessionRef session =
            context->transportManager->openSession(locator);

    // Verify that the server at the given locator is actually the
    // server we want (it's possible that a different incarnation of
    // the server uses the same locator, but has a different server id).
    // See RAM-571 for more on this.
    if (!skipServerIdCheck) {
        while (1) {
            ServerId actualId = PingClient::getServerId(context, session);
            if (actualId == id) {
                break;
            }
            if (actualId.isValid()) {
                RAMCLOUD_LOG(NOTICE, "server for locator %s has incorrect id "
                        "(expected %s, got %s); discarding session",
                        locator.c_str(), id.toString().c_str(),
                        actualId.toString().c_str());
                return FailSession::get();
            }
            // If we get here, it means that the server doesn't yet know its
            // id (it must not have completed the enlistment process yet).
            // Keep trying.
            RAMCLOUD_CLOG(NOTICE, "retrying server id check for %s: "
                    "server not yet enlisted", id.toString().c_str());
            Cycles::sleep(1000);
        }
    }

    // We've successfully opened a session. Add it back back into the server
    // list, assuming this ServerId is still valid and no one else has put a
    // session there first.
    {
        Lock _(mutex);
        ServerDetails* details = iget(id);
        if (details == NULL)
            return FailSession::get();
        if (details->session == NULL)
            details->session = session;
        return details->session;
    }
}

/**
 * If there is a session cached for the given server id, flush it so that
 * future attempts to communicate with the server will create a new session.
 *
 * \param id
 *      Identifier for a particular server.
 */
void
AbstractServerList::flushSession(ServerId id)
{
    Lock _(mutex);
    ServerDetails* details = iget(id);
    if (details != NULL) {
        details->session = NULL;
        RAMCLOUD_TEST_LOG("flushed session for id %s", id.toString().c_str());
    }
}

/**
 * Return true if the given serverId is in this list regardless of
 * whether it is crashed or not.  This can be used to check membership,
 * rather than having to try and catch around the index operator.
 */
bool
AbstractServerList::contains(ServerId id) {
    Lock _(mutex);

    return iget(id) != NULL;
}

/**
 * This method is used for iterating through the servers in the
 * cluster. Given the id for a particular server, it returns the
 * id for the next server that supports a given set of services.
 * 
 * \param prev
 *      Return value from a previous indication of this method;
 *      the current invocation will return the next server after
 *      this one. If this is an invalid ServerId (e.g. constructed
 *      with the default constructor), then this call will return
 *      the id of the first matching server.
 * \param services
 *      Restricts the set of servers that will be returned: for a
 *      server to be returned, it must support all of these services.
 * \param end
 *      If this pointer is non-NULL, then the value it references will
 *      be set to true if we reached the end of the server list (in
 *      which case we wrapped back to the beginning again); otherwise
 *      it will be set to false.
 * \param includeCrashed
 *      Normally, only servers that are up will be returned; if this
 *      parameter is true then crashed servers will also be returned
 *      (as long as they have not been fully recovered, at which point
 *      they cease to exist).
 * 
 * \return
 *      The return value is the ServerId for the next server after
 *      prev in the server list that supports the given set of
 *      services. If there are no servers with a given set of services,
 *      then an invalid ServerId is returned.
 */
ServerId
AbstractServerList::nextServer(ServerId prev, ServiceMask services,
        bool* end, bool includeCrashed)
{
    Lock _(mutex);
    uint32_t startIndex = prev.isValid() ? prev.indexNumber() : -1U;
    uint32_t index = startIndex;
    size_t size = isize();
    if (end != NULL)
        *end = false;

    // Loop circularly through all of the servers currently known.
    for (size_t count = size; count > 0; count--) {
        ++index;
        if (index >= size) {
            if (end != NULL)
                *end = true;
            index = 0;
            if (size == 0)
                break;
        }
        ServerDetails* details = iget(index);
        if ((details != NULL)
                && ((details->status == ServerStatus::UP) || includeCrashed)
                && details->services.hasAll(services)) {
            return details->serverId;
        }
    }

    // Either the server list is empty, or it contains no server with
    // the desired services.
    if (end != NULL)
        *end = true;
    return ServerId();
}

/**
 * Register a ServerTracker with this ServerList. Any updates to this
 * list (additions or removals) will be propagated to the tracker. The
 * current list of hosts will be pushed to the tracker immediately so
 * that its state is synchronised with this ServerList.
 *
 * \throw ServerListException
 *      An exception is thrown if the same tracker is registered more
 *      than once or if ServerList is in the process of being destroyed.
 */
void
AbstractServerList::registerTracker(ServerTrackerInterface& tracker)
{
    Lock _(mutex);

    if (isBeingDestroyed)
        throw ServerListException(HERE, "ServerList has entered its destruction"
                " phase and will not accept new trackers.");

    bool alreadyRegistered =
        std::find(trackers.begin(), trackers.end(), &tracker) != trackers.end();

    if (alreadyRegistered) {
        throw ServerListException(HERE,
            "Cannot register the same tracker twice!");
    }

    trackers.push_back(&tracker);
    tracker.setParent(this);

    for (uint32_t n = 0; n < isize(); n++)  {
        const ServerDetails* server = iget(n);
        if (!server)
            continue;
        tracker.enqueueChange(*server, server->status == ServerStatus::UP ?
                                            ServerChangeEvent::SERVER_ADDED :
                                            ServerChangeEvent::SERVER_CRASHED);
    }

    tracker.fireCallback();
}

/**
 * Unregister a ServerTracker that was previously registered with this
 * ServerList. Doing so will cease all update propagation.
 */
void
AbstractServerList::unregisterTracker(ServerTrackerInterface& tracker)
{
    Lock _(mutex);
    if (isBeingDestroyed) {
        tracker.setParent(NULL);
        return;
    }

    for (size_t i = 0; i < trackers.size(); i++) {
        if (trackers[i] == &tracker) {
            trackers.erase(trackers.begin() + i);
            tracker.setParent(NULL);
            break;
        }
    }
}

/**
 * Get the version of this list, as set by #setVersion. Used to tell whether or
 * not the list of out of date with the coordinator (and other hosts).
 */
uint64_t
AbstractServerList::getVersion() const
{
    Lock _(mutex);
    return version;
}

/**
 * Return the number of valid indexes in this list. Valid does not mean that
 * they're occupied, only that they are within the bounds of the array.
 */
size_t
AbstractServerList::size() const
{
    Lock _(mutex);
    return isize();
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
AbstractServerList::toString(ServerId id)
{
    string locator;
    try {
        locator = getLocator(id);
    } catch (const ServerListException& e) {
        locator = "(locator unavailable)";
    }
    return format("server %s at %s",
                  id.toString().c_str(),
                  locator.c_str());
}

/**
 * Return a human-readable string representation of a status.
 *
 * \return
 *      The string representing the status.
 */
string
AbstractServerList::toString(ServerStatus status)
{
    switch (status) {
        case ServerStatus::UP:
            return "UP";
        case ServerStatus::CRASHED:
            return "CRASHED";
        case ServerStatus::REMOVE:
            return "REMOVE";
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
AbstractServerList::toString()
{
    Lock lock(mutex);

    string result;
    for (uint32_t n = 0; n < isize(); n++) {
        ServerDetails* server = iget(n);
        if (!server)
            continue;

        result.append(
            format("server %s at %s with %s is %s\n",
                   server->serverId.toString().c_str(),
                   server->serviceLocator.c_str(),
                   server->services.toString().c_str(),
                   toString(server->status).c_str()));
    }

    return result;
}

} /// namespace RAMCloud
