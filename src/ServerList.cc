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
    : serverList(),
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

    if (id == ServerId::INVALID_SERVERID) {
        LOG(WARNING, "Ignoring addition of INVALID_SERVERID.");
        return;
    }

    uint32_t index = id.indexNumber();

    if (serverList.size() >= index) {
        // XXX- Sanity check this first?
        serverList.reserve(index + 1);
    }

    // If we happen to get an ADD for a server that overwrites a slot
    // belonging to a server we haven't yet received a REMOVE for, we
    // will fake up a REMOVE first, then handle the ADD. If the REMOVE
    // was reordered (not lost) and eventually shows up, we'll just have
    // to suppress it. If we see an ADD for a ServerId less than the one
    // currently stored at the same index, drop it. Finally, if we get
    // duplicate ADDs, just suppress them as well.
    if (serverList[index].first != ServerId::INVALID_SERVERID) {
        // Duplicate ADD.
        if (serverList[index].first == id) {
            LOG(WARNING, "Duplicate add of ServerId %lu!", id.getId());
            return;
        }

        // ADD of older ServerId.
        uint32_t newGen = id.generationNumber();
        if (newGen < serverList[index].first.generationNumber()) {
            LOG(WARNING, "Dropping addition of ServerId older than the current "
                "entry (%lu < %lu)!", id.getId(),
                serverList[index].first.getId());
            return;
        }

        // ADD before previous REMOVE.
        ServerId oldId = serverList[index].first;
        LOG(WARNING, "Addition of %lu seen before removal of %lu! Issuing "
            "removal before addition.", id.getId(), oldId.getId());
        foreach (ServerTrackerInterface* tracker, trackers)
            tracker->handleChange(id, ServerChangeEvent::SERVER_REMOVED);
    }

    serverList[index] = std::pair<ServerId, ServiceLocator>(id, locator);

    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->handleChange(id, ServerChangeEvent::SERVER_ADDED);
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

    if (id == ServerId::INVALID_SERVERID) {
        LOG(WARNING, "Ignoring removal of INVALID_SERVERID.");
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
    if (index >= serverList.size() || serverList[index].first != id) {
        LOG(WARNING, "Ignoring removal of unknown ServerId %lu", id.getId());
        return;
    }

    serverList[index].first = ServerId::INVALID_SERVERID;

    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->handleChange(id, ServerChangeEvent::SERVER_REMOVED);
}

/**
 * Register a ServerTracker with this ServerList. Any updates to this
 * list (additions or removals) will be propagated to the tracker. The
 * current list of hosts will be pushed to the tracker immediately so
 * that its state is synchronised with this ServerList.
 *
 * \throw Exception
 *      An exception is thrown if the same tracker is registered more
 *      than once.
 */
void
ServerList::registerTracker(ServerTrackerInterface& tracker)
{
    boost::lock_guard<SpinLock> lock(mutex);

    if (std::find(trackers.begin(), trackers.end(), &tracker) != trackers.end())
        throw Exception(HERE, "Cannot register the same tracker twice!");

    trackers.push_back(&tracker);

    // Push ADDs for all known servers to this tracker.
    for (size_t i = 0; i < serverList.size(); i++) {
        if (serverList[i].first != ServerId::INVALID_SERVERID) {
            tracker.handleChange(serverList[i].first,
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
