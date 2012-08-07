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

#include "TransportManager.h"

#include "AbstractServerList.h"
#include "ServerTracker.h"

namespace RAMCloud {

/**
 * Constructor for AbstractServerList.

 * \param context
 *      Overall information about the RAMCloud server
 */
AbstractServerList::AbstractServerList(Context& context)
    : context(context)
    , isBeingDestroyed(false)
    , version(0)
    , trackers()
    , mutex()
{
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
const char*
AbstractServerList::getLocator(ServerId id)
{
    Lock _(mutex);

    if (icontains(id))
        return iget(id.indexNumber())->serviceLocator.c_str();

    throw ServerListException(HERE,
            format("Invalid ServerID (%lu)", id.getId()));


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

    return icontains(id) && iget(id.indexNumber())->status == ServerStatus::UP;
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
AbstractServerList::getSession(ServerId id)
{
    // getLocator(id) locks list for access
    return context.transportManager->getSession(getLocator(id), id);
}

/**
 * Return true if the given serverId is in this list regardless of
 * whether it is crashed or not.  This can be used to check membership,
 * rather than having to try and catch around the index operator.
 */
bool
AbstractServerList::contains(ServerId id) const {
    Lock _(mutex);

    return icontains(id);
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

    // Push all known servers which are crashed first.
    // Order is important to guarantee that if one server replaced another
    // during enlistment that the registering tracker queue will have the
    // crash event for the replaced server before the add event of the
    // server which replaced it.
    for (size_t n = 0; n < isize(); n++)  {
        const ServerDetails* server = iget(n);
        if (!server || server->status != ServerStatus::CRASHED)
            continue;

        ServerDetails details = *server;
        details.status = ServerStatus::UP;
        tracker.enqueueChange(details, ServerChangeEvent::SERVER_ADDED);
        tracker.enqueueChange(*server, ServerChangeEvent::SERVER_CRASHED);
    }

    // Push all known servers that are up
    for (size_t n = 0; n < isize(); n++)  {
        ServerDetails* server = iget(n);
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
AbstractServerList::toString(ServerStatus status)
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
AbstractServerList::toString()
{
    Lock lock(mutex);

    string result;
    for (size_t n = 0; n < isize(); n++) {
        ServerDetails* server = iget(n);
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

} /// namespace RAMCloud
