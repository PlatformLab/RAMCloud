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
 * This file defines the ServerList class.
 */

#ifndef RAMCLOUD_SERVERLIST_H
#define RAMCLOUD_SERVERLIST_H

#include "ServerId.h"
#include "ServerTracker.h"
#include "ServiceLocator.h"
#include "SpinLock.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * A ServerList maintains a mapping of coordinator-allocated ServerIds to
 * the ServiceLocators that address particular servers. Here a "server"
 * is not a physical machine, but rather a specific instance of a RAMCloud
 * server process.
 *
 * The intent is to have a single ServerList per process. If a module wishes
 * to keep track of changes to the ServerList (i.e. be notified when machines
 * are added or removed), then it may register its own private ServerTracker
 * with the master ServerList. The tracker will be fed updates whenever servers
 * come or go. The tracker also provides a convenient way to associate their
 * own per-server state with ServerIds that they're using or keeping track of.
 *
 * The ServerList is re-entrant. It is expected that one or more threads will
 * feed it updates from the coordinator while others register or unregister
 * ServerTrackers.
 */
class ServerList {
  PUBLIC:
    ServerList();
    ~ServerList();
    void add(ServerId id, ServiceLocator locator);
    void remove(ServerId id);
    uint32_t getHighestIndex();
    ServerId getServerId(uint32_t indexNumber);
    void registerTracker(ServerTrackerInterface& tracker);
    void unregisterTracker(ServerTrackerInterface& tracker);

  PRIVATE:
    /**
     * This class is only used to group ServerIds and ServiceLocators in the
     * serverList vector.
     */
    class ServerIdServiceLocatorPair {
      PUBLIC:
        ServerIdServiceLocatorPair(ServerId id, ServiceLocator& sl)
            : serverId(id),
              serviceLocator(sl)
        {
        }

        /// ServerId associated with this index in the serverList.
        ServerId serverId;

        /// ServiceLocator associated with this serverId in the serverList.
        ServiceLocator serviceLocator;
    };

    /// Slots in the server list.
    std::vector<Tub<ServerIdServiceLocatorPair>> serverList;

    /// ServerTrackers that have registered with us and will receive updates
    /// regarding additions or removals from this list.
    std::vector<ServerTrackerInterface*> trackers;

    /// Mutex to protect the class. Simultaneous access can occur due to
    /// ServerTrackers registering/unregistering while adds and removes
    /// are being handled.
    SpinLock mutex;

    DISALLOW_COPY_AND_ASSIGN(ServerList);
};

} // namespace RAMCloud

#endif // !RAMCLOUD_SERVERLIST_H
