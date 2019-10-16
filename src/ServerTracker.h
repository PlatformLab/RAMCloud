/* Copyright (c) 2012-2019 Stanford University
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

#include <queue>

#include "Common.h"
#include "AbstractServerList.h"
#include "ServerId.h"
#include "SpinLock.h"
#include "Tub.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * The possible events that could be associated with a
 * ServerTracker<T>::ServerChange event object.
 */
enum ServerChangeEvent {
    SERVER_ADDED,
    SERVER_CRASHED,
    SERVER_REMOVED,
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
    virtual void enqueueChange(const ServerDetails& server,
                               ServerChangeEvent event) = 0;
    virtual void fireCallback() = 0;

  PRIVATE:
    friend class AbstractServerList;
    virtual void setParent(AbstractServerList* parent) = 0;
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
 *     ReplicaManager could use this to keep track of segments replicated
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
 * queue. In between doing so, the state in their ServerTracker is not mutated.
 * This way, data can be associated with a particular ServerId and will not go
 * away (even if that machine goes down) until the user of the class explicitly
 * pulls the event that indicates that the machine went away. This helps avoid
 * concurrency issues while providing a convenient way to associate information
 * with a ServerId that is (or was recently) active.
 *
 * This class provides limited thread-safety. It is safe to invoke
 * enqueueChanges in any number of threads and all the other methods in
 * a single other thread. However, calls to methods such as getLocator are
 * not safe in the presence of concurrent calls to getChanges.
 */
template<typename T>
class ServerTracker : public ServerTrackerInterface {
  PUBLIC:
    /**
     * To use a ServerTracker, a client creates a subclass of this
     * class, which is used to notify the client of changes to the
     * server list.
     */
    class Callback {
      public:
        /**
         * This method is defined in a subclass corresponding to a
         * ServerTracker client; it is invoked to indicate that there are
         * one or more pending changes to the tracker (i.e., getChange will
         * return true). This method executes in the context of the ServerList
         * feeding us the event, so it must be extremely efficient so as not
         * to hold up delivery of events to other Server Trackers (e.g., it
         * must not invoke an RPC).
         */
        virtual void trackerChangesEnqueued() = 0;
        virtual ~Callback() {};
    };

    /**
     * Constructor for a ServerTracker.
     *
     * \param context
     *      Overall information about the RAMCloud server or client.
     *      Updates will be obtained from this context's serverList.
     *      If the context has no serverList (for testing only) then
     *      no updating will occur.
     */
    explicit ServerTracker(Context* context)
        : ServerTrackerInterface()
        , mutex("ServerTracker")
        , context(context)
        , parent(NULL)
        , serverList()
        , changes()
        , lastRemovedIndex(-1)
        , eventCallback()
        , numberOfServers(0)
        , testing_avoidGetChangeAssertion(false)
    {
        if (context->serverList != NULL)
            context->serverList->registerTracker(*this);
    }

    /**
     * Constructor for ServerTracker.
     *
     * \param context
     *      Overall information about the RAMCloud server or client.
     *      Updates will be obtained from this context's serverList.
     * \param eventCallback
     *      A callback functor to be invoked whenever there is an
     *      upcoming change to the list. This functor will execute
     *      in the context of the ServerList that has fed us the
     *      event and should be extremely efficient so as to not
     *      hold up delivery of the same or future events to other
     *      ServerTrackers.
     */
    explicit ServerTracker(Context* context,
                           Callback* eventCallback)
        : ServerTrackerInterface()
        , mutex("ServerTracker")
        , context(context)
        , parent(NULL)
        , serverList()
        , changes()
        , lastRemovedIndex(-1)
        , eventCallback(eventCallback)
        , numberOfServers(0)
        , testing_avoidGetChangeAssertion(false)
    {
        context->serverList->registerTracker(*this);
    }

    /**
     * Destructor for ServerTracker.
     */
    ~ServerTracker()
    {
        if (parent != NULL)
            parent->unregisterTracker(*this);
    }

    /**
     * Method used by the parent ServerList to inject ordered server updates.
     * This enqueues the updates and does not process them until the client
     * invokes getChange() to get the oldest change in the list state. See
     * getChange() for some details about the sequences of events users
     * of trackers can expect to see.
     */
    void
    enqueueChange(const ServerDetails& server, ServerChangeEvent event)
    {
        SpinLock::Guard lock(mutex);

        // Make sure the server status is consistent with the event being
        // enqueued.
        assert((event == SERVER_ADDED &&
                server.status == ServerStatus::UP) ||
               (event == SERVER_CRASHED &&
                server.status == ServerStatus::CRASHED) ||
               (event == SERVER_REMOVED &&
                server.status == ServerStatus::REMOVE));
        changes.push(ServerChange(server, event));
    }

    /**
     * If the constructor was provided a callback, then this method will fire
     * the callback to alert the client that a new change is waiting to be
     * handled.
     * ServerList enqueues changes to all registered trackers before invoking
     * callbacks on any of them.
     */
    void
    fireCallback()
    {
        // Fire the callback to notify that the queue has one or more
        // new entries.
        if (eventCallback)
            eventCallback->trackerChangesEnqueued();
    }

    /**
     * Returns true if there are any outstanding list changes that the client
     * of this tracker may want to be aware of. Otherwise returns false.
     */
    bool
    hasChanges()
    {
        SpinLock::Guard lock(mutex);
        return !changes.empty();
    }

    /**
     * Returns the next enqueued change to the ServerList, if there is one. If
     * the event indicates removal, then the caller must NULL out any pointer
     * they were storing with the associated ServerId before the next
     * invocation of getChange(). This helps to ensure that the caller is
     * properly handling objects that were referenced by this tracker. After
     * the subsequent call to getChange(), the old ServerId cannot be used to
     * index into the tracker anymore.
     *
     * To be clear, this means that something like the following idiom
     * must always be used:
     *   ServerDetails server;
     *   ServerChangeEvent event;
     *   if (!getChange(server, event)) return;
     *   if (event == SERVER_ADDED) {
     *       if (!tracker[server.serverId]) // This check is very necessary.
     *           tracker[server.serverId] = new T(...);
     *   } else if (event == SERVER_REMOVED) {
     *       auto* p = tracker[server.serverId];
     *       // 'delete p;' or stash the pointer elsewhere, perhaps
     *
     *       // THE FOLLOWING MUST BE DONE BEFORE getChange() IS CALLED
     *       // AGAIN (tracker[server.serverId] will return a valid ref
     *       // until then):
     *       tracker[server.serverId] = NULL;
     *   }
     *
     * To use this method safely here are a few properties and warnings:
     * 1) This method always reports events for a server in the order
     *    SERVER_ADDED, SERVER_CRASHED, SERVER_REMOVED; each event
     *    will be reported for server over its lifetime. SERVER_CRASHED and
     *    SERVER_REMOVED will each be reported exactly once.
     * 2) SERVER_ADDED can happen more than once for a single server; it
     *    is used to notify callers that some properties of the server has
     *    changed (for example, the replicationId has changed). This means
     *    that it is *important* that the pointer stored in the tracker
     *    is checked before allocating space and storing the pointer in it.
     *    Otherwise, if a server's details are modified, space leaks or
     *    worse may occur.
     * 3) There is one extremely subtle scenario which only impacts new
     *    server enlistment and server which "replace" others in the cluster.
     *    The are some complicated ordering constraints that must be enforced
     *    on events to ensure that backups don't accidentally discard replicas
     *    prematurely. See ServerList::applyServerList() for that sadness.
     *    This property is enforced by other code in RAMCloud, not this
     *    class.
     *
     * \param[out] server
     *      Details about the server that was added, changed, crashed, removed
     *      from in the cluster. All fields of \a server are valid if after
     *      return event == SERVER_ADDED; otherwise, if event == SERVER_REMOVED
     *      only the serverId field is valid.
     * \param[out] event
     *      Type of event (SERVER_ADDED or SERVER_REMOVED).
     * \return
     *      True if there was a change returned, else false.
     */
    bool
    getChange(ServerDetails& server, ServerChangeEvent& event)
    {
        SpinLock::Guard lock(mutex);
        if (lastRemovedIndex != static_cast<uint32_t>(-1)) {
            if ((serverList[lastRemovedIndex].pointer != NULL)
                    && !testing_avoidGetChangeAssertion) {
                // If this trips, then you're not clearing the pointer you
                // stored with the last ServerId that was removed. This
                // exists solely to ensure that you aren't leaking anything.
                RAMCLOUD_DIE("User of this ServerTracker did not NULL out "
                    "previous pointer for server %s",
                    serverList[lastRemovedIndex].server.serverId.
                    toString().c_str());
            }

            // Blank out details about the server and it extra state.
            serverList[lastRemovedIndex].server = ServerDetails();
            serverList[lastRemovedIndex].pointer = NULL;
            lastRemovedIndex = -1;
        }

        // This loop processes one event in each iteration, and normally
        // executes only one iteration; additional iterations are
        // required only if there are remove events that we discard without
        // returning.
        while (1) {
            if (changes.empty())
                return false;

            ServerChange* change = &changes.front();
            uint32_t index = change->server.serverId.indexNumber();

            // Resizing may cause the vector to allocate new internal space,
            // meaning that any references into it may be invalidated after
            // this statement!
            if (index >= serverList.size())
                serverList.resize(index + 1);
            ServerDetailsWithTPtr* slot = &serverList[index];
            event = change->event;

            // The code below must handle cases where a crashed or removed
            // event arrives without the preceding event(s) (this can happen
            // during cold starts, for example). The solution is to synthesize
            // missing events.
            if (event == SERVER_ADDED) {
                if (slot->server.status == ServerStatus::CRASHED) {
                    goto error;
                }
                slot->server = change->server;
                changes.pop();
                numberOfServers++;
            } else if (event == SERVER_CRASHED) {
                if (slot->server.status == ServerStatus::UP) {
                    // This is the expected case.
                    slot->server = change->server;
                    changes.pop();
                } else if (slot->server.status == ServerStatus::REMOVE) {
                    // Synthesize a SERVER_ADDED event and retain the CRASHED
                    // event for the next call.
                    slot->server = change->server;
                    slot->server.status = ServerStatus::UP;
                    event = SERVER_ADDED;
                    numberOfServers++;
                } else {
                    // Duplicate crash events shouldn't happen.
                    goto error;
                }
            } else if (event == SERVER_REMOVED) {
                if (slot->server.status == ServerStatus::UP) {
                    // Synthesize a SERVER_CRASHED event and retain the REMOVED
                    // event for the next call.
                    slot->server = change->server;
                    slot->server.status = ServerStatus::CRASHED;
                    event = SERVER_CRASHED;
                } else if (slot->server.status == ServerStatus::CRASHED) {
                    // This is the normal case.
                    slot->server = change->server;
                    changes.pop();
                    lastRemovedIndex = index;
                    assert(numberOfServers > 0);
                    numberOfServers--;
                } else {
                    // We have never seen anything about the server before, so
                    // we can just ignore the remove event (no need to create
                    // the server and then immediately delete it again).
                    changes.pop();
                    continue;
                }
            } else {
                RAMCLOUD_DIE("Unknown event type %d", static_cast<int>(event));
            }
            server = slot->server;
            return true;

          error:
            RAMCLOUD_DIE("Consistency error between event (server %s, "
                    "status %s) and slot (server %s, status %s)",
                    change->server.serverId.toString().c_str(),
                    eventString(change->event),
                    slot->server.serverId.toString().c_str(),
                    AbstractServerList::toString(slot->server.status).c_str());
        }
    }

    /**
     * Deterministically obtain the ServerId at serverList[index], but only
     * if index is valid, the serverId is valid, the server is up, and it
     * has the specified service. We return invalid id otherwise.
     *
     * \param index
     *      The index of serverList[] we want returned.
     * \param service
     *      Restricts returned ServerId to a server that was known by this tracker
     *      to be running an instance of a specific service type.
     * \return
     *      The ServerId of a server that was known by this tracker to be
     *      running an instance of the requested service type, provided the
     *      criteria passes.
     */
    ServerId
    getServerIdAtIndexWithService(uint32_t index,
                                  WireFormat::ServiceType service)
    {
        if (serverList.size() > 0 &&
            index < serverList.size() &&
            index != lastRemovedIndex &&
            serverList[index].server.serverId.isValid() &&
            serverList[index].server.status == ServerStatus::UP &&
            serverList[index].server.services.has(service))
        {
            return serverList[index].server.serverId;
        }
        return ServerId(/* invalid id */);
    }

    /**
     * Obtain a random ServerId stored in this tracker which is running a
     * particular service.
     * The caller should check ServerId::isValid() since this method can
     * return an invalid ServerId if no host matches the criteria or if the
     * caller is unlucky.
     *
     * Internally, this method chooses an entry at random and checks to see
     * if it matches the criteria, if not it tries again.  Eventually it
     * gives up if it cannot find a matching server.  When the list is
     * empty or possibly just sparse this method will return an invalid id.
     * We should switch to something more efficient if this probabilistic
     * approach doesn't work well.
     *
     * \param service
     *      Limits returned ServerId to a server that was known by this tracker
     *      to be running an instance of a specific service type.
     * \return
     *      The ServerId of a server that was known by this tracker to be
     *      running an instance of the requested service type.
     */
    ServerId
    getRandomServerIdWithService(WireFormat::ServiceType service)
    {
        // This could get a little slow if the list isn't dense, but the
        // coordinator should aggressively reuse slots to maintain density
        // and we're unlikely to have clusters that grow really big only to
        // then shrink down drastically in such a way that there are large
        // contiguous regions of unused indexes in the serverList.
        if (serverList.size() > 0) {
            for (size_t j = 0; j < serverList.size() * 10; ++j) {
                size_t i = generateRandom() % serverList.size();

                if (i != lastRemovedIndex &&
                    serverList[i].server.serverId.isValid() &&
                    serverList[i].server.status == ServerStatus::UP &&
                    serverList[i].server.services.has(service))
                    return serverList[i].server.serverId;
            }
            return ServerId(/* invalid id */);
        }

        return ServerId(/* invalid id */);
    }

    /**
     * Obtain the locator associated with the given ServerId.
     *
     * \param id
     *      The ServerId to look up the locator for.
     * \return
     *      The ServiceLocator string assocated with the given ServerId.
     * \throw ServerListException
     *      An exception is thrown if this ServerId is not in the tracker.
     *      This can happen if servers fail, if knowledge of a server is
     *      communicated out-of-band from the ServerList and the ServerList
     *      hasn't received the addition yet, or simply because this tracker
     *      hasn't applied some outstanding changes from its ServerList.
     */
    string
    getLocator(ServerId id)
    {
        return getServerDetails(id)->serviceLocator;
    }

    /**
     * Access the fields the tracker has associated with the given ServerId
     * that are part of all ServiceList entries (includes ServerId, locator
     * ServiceMask, and backup storage performance).  Be careful with this
     * call: every call to getChange() invalidates all pointers previously
     * returned from this method.
     *
     * \param id
     *      The ServerId to look up the locator for.
     * \return
     *      Pointer to the ServerDetails assocated with the given ServerId.
     *      Warning, the pointer is only guaranteed to be valid until the
     *      next call to getChange().
     * \throw ServerListException
     *      An exception is thrown if this ServerId is not in the tracker.
     *      This can happen if servers fail, if knowledge of a server is
     *      communicated out-of-band from the ServerList and the ServerList
     *      hasn't received the addition yet, or simply because this tracker
     *      hasn't applied some outstanding changes from its ServerList.
     */
    ServerDetails*
    getServerDetails(ServerId id)
    {
        uint32_t index = id.indexNumber();
        if (index >= serverList.size() ||
            serverList[index].server.serverId != id) {
            throw Exception(HERE,
                            format("ServerId %s is not in this tracker.",
                                   id.toString().c_str()));
        }

        return &serverList[index].server;
    }

    /**
     * Return a reference to the T* we're associating with an active ServerId.
     * If the exact serverId given does not exist an exception is thrown.
     *
     * Since the data stored may be invalidated when #getChange() is called
     * (e.g. a server is removed or re-added), callers must not hold a
     * reference across invocations to #getChange(). To avoid this, it might
     * be a good idea to always use this index operator and indirect via
     * a ServerTracker, rather than storing a reference and risking it later
     * being invalidated.
     */
    T*&
    operator[](const ServerId& serverId)
    {
        uint32_t index = serverId.indexNumber();
        if (index >= serverList.size() ||
          serverList[index].server.serverId != serverId) {
            throw Exception(HERE, "ServerId given is not in this tracker.");
        }

        return serverList[index].pointer;
    }

    /**
     * Return the number of servers currently in this tracker. Note that this
     * does not include any that will be added due to enqueue change events,
     * only the number presently being tracked.
     */
    uint32_t
    size()
    {
        return numberOfServers;
    }

    /**
     * Return a human-readable string representation of the contents of
     * the tracker. Useful for unit tests, among other things.
     *
     * \return
     *      The string representing the contents of the tracker.
     */
    string
    toString()
    {
        string result;
        foreach (const auto& server, serverList) {
            if (!server.server.serverId.isValid())
                continue;
            result.append(
                format("server %s at %s with %s is %s\n",
                       server.server.serverId.toString().c_str(),
                       server.server.serviceLocator.c_str(),
                       server.server.services.toString().c_str(),
                       ServerList::toString(server.server.status).c_str()));
        }

        return result;
    }

    /**
     * Find all servers having a particular service.
     *
     * \param service
     *      Only return servers which have this particular service type
     *      running.
     * \return
     *      List of ServerIds of servers this tracker contains that have
     *      #service.
     */
    std::vector<ServerId>
    getServersWithService(WireFormat::ServiceType service)
    {
        std::vector<ServerId> result;
        result.reserve(serverList.size());
        foreach (const auto& entry, serverList) {
            const auto& server = entry.server;
            if (server.status == ServerStatus::UP &&
                server.services.has(service))
                result.push_back(server.serverId);
        }
        return result;
    }

    /**
     * Returns a human-readable string describing the argument;
     * intended for testing and logging.
     * \param event
     *     Event of interest.
     */
    static const char*
    eventString(ServerChangeEvent event) {
        switch (event) {
            case SERVER_ADDED: return "SERVER_ADDED";
            case SERVER_CRASHED: return "SERVER_CRASHED";
            case SERVER_REMOVED: return "SERVER_REMOVED";
        }
        return "??";
    }

  PRIVATE:
    /**
     * Method used by a parent AbstractServerList to  set the parent pointer
     * upon construction/destruction and register/unregister
     *
     * \param parent
     *          A pointer to the new parent
     */
    virtual void
    setParent(AbstractServerList* parent) {
        this->parent = parent;
    }
    /**
     * A ServerChange represents a single event.
     */
    class ServerChange {
      PUBLIC:
        ServerChange(const ServerDetails& server,
                     ServerChangeEvent event)
            : server(server),
              event(event)
        {
        }

        /// Details of the server which was added to or removed from the system.
        ServerDetails server;

        /// Event type.
        ServerChangeEvent event;
    };

    /**
     * This class is only used to group server details and T*s in the serverList
     * vector.
     */
    class ServerDetailsWithTPtr {
      PUBLIC:
        ServerDetailsWithTPtr()
            : server()
            , pointer(NULL)
        {
        }

        ServerDetailsWithTPtr(const ServerDetailsWithTPtr&) = default;
        ServerDetailsWithTPtr&
        operator=(const ServerDetailsWithTPtr&) = default;

        /// Details of the server associated with this index in the serverList.
        ServerDetails server;

        /// Pointer type T associated with this ServerId in the serverList.
        T* pointer;
    };

    /// Used to synchronize calls to enqueueChange with calls to getChange.
    /// Other methods of the class are unprotected and not thread-safe.
    SpinLock mutex;

    /// Shared RAMCloud information.
    Context* context;

    /// CoordinatorServerList/ServerList from which this tracker is registered
    /// gets all the updates, NULL if unregistered.  We use this variable
    /// internally instead of context->serverList, because this variable will be
    /// NULLified if the parent server list goes away, and that's important for
    /// us to know.
    AbstractServerList* parent;

    /// Servers that we're tracking and the templated state we're associating
    /// with them. Note that this list is not synchronously updated when the
    /// parent ServerList changes, rather it is updated when the #getChange
    /// method is invoked. See the #ServerTracker class documentation for more
    /// details.
    std::vector<ServerDetailsWithTPtr> serverList;

    /// Queue of list membership changes.
    std::queue<ServerChange> changes;

    /// Previous change index. This is set when a SERVER_REMOVE event is pulled
    /// via #getChange(). On the next #getChange() invocation, we check to see
    /// if the user has NULLed out the pointer for that index and remove the
    /// entry. This sanity check helps to ensure that the user of this class is
    /// playing by the rules. -1 implies the previous event was not a removal.
    uint32_t lastRemovedIndex;

    /// Optional callback to fire each time an entry (i.e. a server add or
    /// remove notice) is added to queue
    Callback* eventCallback;

    /// Number of servers in the tracker.
    uint32_t numberOfServers;

    /// Normally false. Only set for testing purposes.
    bool testing_avoidGetChangeAssertion;

    DISALLOW_COPY_AND_ASSIGN(ServerTracker);
};

} // namespace RAMCloud

#endif // !RAMCLOUD_SERVERTRACKER_H
