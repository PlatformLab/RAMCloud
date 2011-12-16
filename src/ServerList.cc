/* Copyright (c) 2011 Stanford University
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
 * This file implements the ServerList class.
 */

#include <boost/thread/locks.hpp>

#include "Common.h"
#include "ServerList.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Constructor for ServerList.
 */
ServerList::ServerList()
    : serverList(1),        // Avoid having to return -1 in getHighestIndex().
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
 * Add a new server to the ServerList and associate its ServerId with the
 * ServiceLocator used to address it.
 *
 * \param id
 *      The ServerId of the server to add.
 *
 * \param locator
 *      The ServiceLocator of the server to add.
 */
void
ServerList::add(ServerId id, ServiceLocator locator)
{
    boost::lock_guard<SpinLock> lock(mutex);

    if (!id.isValid()) {
        LOG(WARNING, "Ignoring addition of invalid ServerId.");
        return;
    }

    uint32_t index = id.indexNumber();

    if (index >= serverList.size()) {
        // XXX- Sanity check this first?
        serverList.resize(index + 1);
    }

    // If we happen to get an ADD for a server that overwrites a slot
    // belonging to a server we haven't yet received a REMOVE for, we
    // will fake up a REMOVE first, then handle the ADD. If the REMOVE
    // was reordered (not lost) and eventually shows up, we'll just have
    // to suppress it. If we see an ADD for a ServerId less than the one
    // currently stored at the same index, drop it. Finally, if we get
    // duplicate ADDs, just suppress them as well.
    if (serverList[index]) {
        // Duplicate ADD.
        if (serverList[index]->serverId == id) {
            LOG(WARNING, "Duplicate add of ServerId %lu!", id.getId());
            return;
        }

        // ADD of older ServerId.
        uint32_t newGen = id.generationNumber();
        if (newGen < serverList[index]->serverId.generationNumber()) {
            LOG(WARNING, "Dropping addition of ServerId older than the current "
                "entry (%lu < %lu)!", id.getId(),
                serverList[index]->serverId.getId());
            return;
        }

        // ADD before previous REMOVE.
        ServerId oldId = serverList[index]->serverId;
        LOG(WARNING, "Addition of %lu seen before removal of %lu! Issuing "
            "removal before addition.", id.getId(), oldId.getId());
        foreach (ServerTrackerInterface* tracker, trackers)
            tracker->enqueueChange(oldId, ServerChangeEvent::SERVER_REMOVED);

        serverList[index].destroy();
    }

    serverList[index].construct(id, locator);

    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->enqueueChange(id, ServerChangeEvent::SERVER_ADDED);
}

/**
 * Remove a server from the list, typically when it is no longer part of
 * the system (e.g. it has crashed).
 *
 * \param id
 *      The ServerId of the server to remove from the ServerList.
 */
void
ServerList::remove(ServerId id)
{
    boost::lock_guard<SpinLock> lock(mutex);

    if (!id.isValid()) {
        LOG(WARNING, "Ignoring removal of invalid ServerId.");
        return;
    }

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
      id.generationNumber() < serverList[index]->serverId.generationNumber()) {
        LOG(WARNING, "Ignoring removal of unknown ServerId %lu", id.getId());
        return;
    }

    // In theory it's possible we could have missed both a prior removal and
    // the next addition, and then see the removal for something newer than
    // what's stored. Unlikely, but let's log it just in case.
    if (id.generationNumber() >
      serverList[index]->serverId.generationNumber()) {
        LOG(WARNING, "Removing ServerId %lu because removal for a newer "
            "generation number was received (%lu)",
            serverList[index]->serverId.getId(), id.getId());
    }

    // Be sure to use the stored id, not the advertised one, just in case we're
    // removing an older entry (see previous comment above).
    foreach (ServerTrackerInterface* tracker, trackers) {
        tracker->enqueueChange(serverList[index]->serverId,
            ServerChangeEvent::SERVER_REMOVED);
    }

    serverList[index].destroy();
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
 *
 * XXX- Perhaps references to the locator should be passed down into
 *      ServerTracker instead and looked up there by clients? But
 *      then again, we probably don't usually want locators returned.
 *      Instead, we want sessions that will ensure we're talking to
 *      the right server.
 */
string
ServerList::getLocator(ServerId id)
{
    boost::lock_guard<SpinLock> lock(mutex);

    uint32_t index = id.indexNumber();
    if (index >= serverList.size() || !serverList[index] ||
      serverList[index]->serverId != id) {
        throw ServerListException(HERE, format(
            "ServerId %lu is not in the ServerList", *id));
    }

    return serverList[index]->serviceLocator.getOriginalString();
}

/**
 * Return the highest index that has ever been used in this ServerList.
 *
 * XXX- Would we prefer the highest currently active index? The two
 *      should be the same or close, but perhaps there's a good argument
 *      for the alternative.
 */
uint32_t
ServerList::size()
{
    boost::lock_guard<SpinLock> lock(mutex);

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
    boost::lock_guard<SpinLock> lock(mutex);

    if (index >= serverList.size() || !serverList[index])
        return ServerId(/* invalid id */);
    return serverList[index]->serverId;
}

/**
 * Return true if the given ServerId is in the list, otherwise return false.
 */
bool
ServerList::contains(ServerId serverId)
{
    boost::lock_guard<SpinLock> lock(mutex);

    uint32_t index = serverId.indexNumber();
    if (index >= serverList.size() || !serverList[index])
        return false;
    return serverList[index]->serverId == serverId;
}

/**
 * Get the version of this list, as set by #setVersion. Used to tell whether or
 * not the list of out of date with the coordinator (and other hosts).
 */
uint64_t
ServerList::getVersion()
{
    boost::lock_guard<SpinLock> lock(mutex);

    return version;
}

/**
 * Set the version of this list. See #getVersion and notes on the #version member
 * for more details.
 */
void
ServerList::setVersion(uint64_t newVersion)
{
    boost::lock_guard<SpinLock> lock(mutex);

    version = newVersion;
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
    boost::lock_guard<SpinLock> lock(mutex);

    bool alreadyRegistered =
        std::find(trackers.begin(), trackers.end(), &tracker) != trackers.end();
    if (alreadyRegistered) {
        throw ServerListException(HERE,
            "Cannot register the same tracker twice!");
    }

    trackers.push_back(&tracker);

    // Push ADDs for all known servers to this tracker.
    for (size_t i = 0; i < serverList.size(); i++) {
        if (serverList[i]) {
            tracker.enqueueChange(serverList[i]->serverId,
                                 ServerChangeEvent::SERVER_ADDED);
        }
    }
}

/**
 * Unregister a ServerTracker that was previously registered with this
 * ServerList. Doing so will cease all update propagation.
 */
void
ServerList::unregisterTracker(ServerTrackerInterface& tracker)
{
    boost::lock_guard<SpinLock> lock(mutex);

    for (size_t i = 0; i < trackers.size(); i++) {
        if (trackers[i] == &tracker) {
            trackers.erase(trackers.begin() + i);
            break;
        }
    }
}

} // namespace RAMCloud
