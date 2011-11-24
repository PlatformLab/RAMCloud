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
 * This file defines the ServerTracker object.
 */

#ifndef RAMCLOUD_SERVERTRACKER_H
#define RAMCLOUD_SERVERTRACKER_H

#include <boost/thread/locks.hpp>

#include "ServerId.h"
#include "SpinLock.h"
#include "Tub.h"

namespace RAMCloud {

/*
 * Possible issues:
 *  - What if users don't want to see changes? Could this ever be the case?
 *  - What if users don't service their queues efficiently? Should there be
 *    any cap? If so, what would the behaviour be? Block? Drop on the floor?
 */

/**
 * The two possible events that could be associated with a
 * ServerTracker<T>::ServerChange event object.
 */
enum ServerChangeEvent {
    SERVER_ADDED,
    SERVER_REMOVED
};

/**
 * Interface used to ensure that every template typed ServerTracker object
 * has an enqueueChange() method that can be called by the ServerList to
 * update interested trackers. Also serves as the supertype used to store
 * different subtypes in ServerList.
 */
class ServerTrackerInterface {
  PUBLIC:
    virtual ~ServerTrackerInterface() { }
    virtual void handleChange(ServerId serverId, ServerChangeEvent event) = 0;
};

/**
 * A ServerTracker is an object that serves essentially two purposes:
 *
 *  1) Managing notifications that indicate when servers are added to and
 *     removed from the system. Users of this class pull such events from
 *     a queue and can be poked into action with a synchronous callback.
 *
 *  2) Associating state with servers known to the system. Users of this
 *     class may find it convenient to store some typed, copyable state
 *     with some or all of the servers known to it. For example, the
 *     BackupManager could use this to keep track of segments replicated
 *     on various machines.
 *
 * ServerTrackers are fed server notifications (ServerChanges) from a 
 * ServerList with which it has been registered. The canonical hierarchy
 * is a single ServerList root feeding updates to one or more ServerTrackers:
 *
 *                           [  ServerList  ]
 *                          /     |     |    \
 *                         /      |     |     \
 *             [ ServerTracker ]   . . .   [ ServerTracker ]
 *
 * ServerTrackers' lists of servers are synchronised with the ServerList, but
 * updates only occur when explicitly invoked by their users. That is, users of
 * ServerTrackers decide when updates are applied by pulling events off of the
 * queue. In between doing so, the state in their ServerTracker is not muted.
 * This way, data can be associated with a particular ServerId and will not go
 * away (even if that machine goes down) until the user of the class explicitly
 * pulls the event that indicates that the machine went away. This helps avoid
 * concurrency issues while providing a convenient way to associate information
 * with a ServerId that is (or was recently) active.
 */
template<typename T>
class ServerTracker : public ServerTrackerInterface {
  PUBLIC:
    /**
     * A ServerChange represents a single addition or removal of a server
     * in the system. In the case of removals, it contains a copy of the
     * data once stored with that particular ServerId. Once the event has
     * been delivered, that original data will have been discarded and the
     * location may soon be overwritten if a new server is added into the
     * same slot as the previous one.
     */
    class ServerChange {
      PUBLIC:
        ServerChange(ServerId serverId, ServerChangeEvent event)
            : serverId(serverId),
              event(event),
              removedData()
        {
        }

        /// ServerId of the server that has been added to or removed from the
        /// system.
        ServerId serverId;

        /// Event type: either addition or removal.
        ServerChangeEvent event;

        /// If a removal occurred, this will contain a copy of the data
        /// previously associated with that ServerId.
        Tub<T> removedData;
    };

    /**
     * Constructor for ServerTracker.
     *
     * \param callback
     *      A callback functor to be invoked whenever there is an
     *      upcoming change to the list. This functor will execute
     *      in the context of the ServerList that has fed us the
     *      event and should be extremely efficient so as to not
     *      hold up delivery of the same or future events to other
     *      ServerTrackers.
     */ 
    ServerTracker(/*callback*/)
        : ServerTrackerInterface(),
          serverList(),
          changes()
          //callback(callback)
    {
    }

    /**
     * Destructor for ServerTracker.
     */ 
    ~ServerTracker()
    {
    }

    /**
     * Method used by the parent ServerList to inject ordered server
     * updates. This enqueues the updates and does not process them
     * until the client invokes ServerTracker::getChange() to get the
     * oldest change in the list state. If the constructor was provided
     * a callback, then this method will fire the callback to alert the
     * client that a new change is waiting to be handled.
     *
     * Note that the ServerList takes care to ensure that many "weird"
     * events don't happen here. For example, an ADD event for a ServerId
     * whose index is already occupied will never be seen (this takes care
     * of duplicate ADDs, ADDs of ServerIds that are older, and ADDs of
     * newer ServerIds before the REMOVAL of the currently stored one).
     */
    void
    handleChange(ServerId serverId, ServerChangeEvent event)
    {
        uint32_t index = serverId.indexNumber();

        if (index >= serverList.size())
            serverList.reserve(index + 1);

        // Ensure that the ServerList guarantees hold.
        assert((event == SERVER_ADDED &&
                serverList[index].first == ServerId::INVALID_SERVERID) ||
               (event == SERVER_REMOVED &&
                serverList[index].first == serverId));
        
        changes.addChange(serverId, event);

        // Fire the callback to notify that the queue has a new entry.
        //if (callback)
        //  callback();
    }

    /**
     * Returns true if there are any outstanding list changes that the client
     * of this tracker may want to be aware of. Otherwise returns false.
     */
    bool
    areChanges()
    {
        return changes.areChanges();
    }

    /**
     * Returns the next enqueued change to the ServerList, if there
     * is one. For a removal, any data associated with the removed
     * ServerId will be passed back as part of the ServerChange
     * object and cleared from the ServerTracker.
     */
    ServerChange
    getChange()
    {
        ServerChange change = changes.getChange();

        uint32_t index = change.serverId.indexNumber();
        if (change.event == SERVER_ADDED) {
            serverList[index].first = change.serverId;
            assert(!serverList[index].second);
        } else if (change.event == SERVER_REMOVED) {
            serverList[index].first = ServerId::INVALID_SERVERID;
            if (serverList[index].second) {
                change.removedData = serverList[index].second;
                serverList[index].second.destroy();
            }
        } else {
            assert(0);
        }

        return change;
    }

    /**
     * Obtain a random ServerId stored in this tracker. If these are none,
     * return ServerId::INVALID_SERVERID instead.
     */
    ServerId
    getRandomServerId()
    {
        // This could get a little slow if the list isn't dense, but the
        // coordinator should aggressively reuse slots to maintain density
        // and we're unlikely to have clusters that grow really big only to
        // then shrink down drastically in such a way that there are large
        // contiguous regions of unused indexes in the serverList.
        if (serverList.size() > 0) {
            size_t start = generateRandom() % serverList.size();
            size_t i = start;
            do {
                if (serverList[i].first != ServerId::INVALID_SERVERID)
                    return serverList[i].first;
                i = (i + 1) % serverList.size();
            } while (i != start);
        }

        return ServerId::INVALID_SERVERID;
    }

    /**
     * Return a reference to the Tub<T> we're associating with an active
     * ServerId. If the exact serverId given does not exist an exception
     * is thrown.
     *
     * Since the data stored may be invalidated when #getChange() is called
     * (e.g. a server is removed or re-added), callers must not hold a
     * reference across invocations to #getChange(). To avoid this, it might
     * be a good idea to always use this index operator and indirect via
     * a ServerTracker, rather than storing a reference and risking it later
     * being invalidated.
     */
    Tub<T>&
    operator[](const ServerId& serverId)
    {
        uint32_t index = serverId.indexNumber();
        if (index >= serverList.size() || serverList[index].first != serverId)
            throw Exception(HERE, "ServerId given is not in this tracker.");

        return serverList[index].second;
    }

  PRIVATE:
    /**
     * A ChangeQueue is used to communicate additions and removals from a
     * ServerList to some consumer. It is used by ServerTrackers to have
     * changes propagated from the master ServerList.
     */
    class ChangeQueue {
      PUBLIC:
        ChangeQueue() : changes(), vectorLock() {}

        /**
         * Add the given change event for the given ServerId to the queue.
         */
        void
        addChange(ServerId serverId, ServerChangeEvent event)
        {
            boost::lock_guard<SpinLock> lock(vectorLock);
            changes.push_back(ServerChange(serverId, event));
        }

        /**
         * Obtain the next change event from the queue.
         *
         * \throw Exception
         *      An Exception is thrown if there are no more elements
         *      on the queue.
         */
        ServerChange
        getChange()
        {
            boost::lock_guard<SpinLock> lock(vectorLock);
            if (changes.empty())
                throw Exception(HERE, "ChangeQueue is empty - cannot dequeue!");
            ServerChange ret = *changes->back();
            changes.pop_back();
            return ret;
        }

        /**
         * Returns true if there are changes to be obtained via #getChange(),
         * otherwise returns false is there are none.
         */
        bool
        areChanges()
        {
            boost::lock_guard<SpinLock> lock(vectorLock);
            return !changes.empty();
        }

      PRIVATE:
        /// Fifo queue of changes to cluster membership.
        std::vector<ServerChange> changes;

        /// Lock to protect the vector from concurrent access.
        SpinLock vectorLock;
    };

    /// Slots in the server list.
    std::vector<std::pair<ServerId, Tub<T>>> serverList;

    /// Queue of list membership changes.
    ChangeQueue changes;

    /// Optional callback to fire each time an entry is added to the queue.
    //Callback callback;

    DISALLOW_COPY_AND_ASSIGN(ServerTracker);
};

} // namespace RAMCloud

#endif // !RAMCLOUD_SERVERTRACKER_H
