/* Copyright (c) 2011-2013 Stanford University
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

#ifndef RAMCLOUD_ABSTRACTSERVERLIST_H
#define RAMCLOUD_ABSTRACTSERVERLIST_H

#include <mutex>

#include "Common.h"
#include "ServiceMask.h"
#include "ServerId.h"
#include "ServerList.pb.h"
#include "Transport.h"
#include "Tub.h"

namespace RAMCloud {
/**
 * Describes for a server whether that server is up, crashed, or down.
 * See each value for details about the individuals states.
 * Values are 32-bit because of a protocol buffer limitation; these values
 * are sent over the wire as is.
 */
enum class ServerStatus : uint32_t {
    /// The server believed to be available.
    UP = 0,
    /// The server has failed, but the cluster has not fully recovered
    /// from its loss, so any resources (replicas) related to it must
    /// be held.
    CRASHED = 1,
    /// The server is no longer part of the cluster and will never be
    /// referred to again.
    REMOVE = 2,
};

/**
 * Each change provided to a ServerTracker by its ServerList includes a
 * ServerDetails which describes details about the server which was
 * added, crashed, or was removed from the cluster.
 */
class ServerDetails {
  PUBLIC:
    /**
     * Create an instance that doesn't represent any server.
     */
    ServerDetails()
        : serverId()
        , serviceLocator()
        , session()
        , services()
        , expectedReadMBytesPerSec()
        , status(ServerStatus::REMOVE)
        , replicationId(0)
    {}

    /**
     * Create an instance where only #serverId is valid. Used to represent
     * the details of a SERVER_CRASHED or SERVER_REMOVED event.
     */
    ServerDetails(ServerId id, ServerStatus status, uint64_t replicationId = 0)
        : serverId(id)
        , serviceLocator()
        , session()
        , services()
        , expectedReadMBytesPerSec()
        , status(status)
        , replicationId(replicationId)
    {}

    /**
     * Create an instance where all fields are valid.
     */
    ServerDetails(ServerId id,
                  const string& locator,
                  ServiceMask services,
                  uint32_t expectedReadMBytesPerSec,
                  ServerStatus status,
                  uint64_t replicationId = 0)
        : serverId(id)
        , serviceLocator(locator)
        , session()
        , services(services)
        , expectedReadMBytesPerSec(expectedReadMBytesPerSec)
        , status(status)
        , replicationId(replicationId)
    {}

    /**
     * Create an instance populated from a ProtoBuf::ServerList::Entry which
     * is used for shipping these structures around.
     */
    explicit ServerDetails(const ProtoBuf::ServerList::Entry& entry)
        : serverId(entry.server_id())
        , serviceLocator(entry.service_locator())
        , session()
        , services(ServiceMask::deserialize(entry.services()))
        , expectedReadMBytesPerSec(entry.expected_read_mbytes_per_sec())
        , status(ServerStatus(entry.status()))
        , replicationId(entry.replication_id())
    {}

    /*
     * Explicit string copies for the service locators in the following copy/move
     * constructors/assignments get rid of some kind of internal race in the
     * g++ 4.4.6's libstdc++ ref counting string implementation.
     */

    ServerDetails(const ServerDetails& other)
        : serverId(other.serverId)
        , serviceLocator(other.serviceLocator.c_str())
        , session(other.session)
        , services(other.services)
        , expectedReadMBytesPerSec(other.expectedReadMBytesPerSec)
        , status(other.status)
        , replicationId(other.replicationId)
    {
    }

    ServerDetails& operator=(const ServerDetails& other) {
        if (this == &other)
            return *this;
        serverId = other.serverId;
        serviceLocator = other.serviceLocator.c_str();
        session = other.session;
        services = other.services;
        expectedReadMBytesPerSec = other.expectedReadMBytesPerSec;
        status = other.status;
        replicationId = other.replicationId;
        return *this;
    }

    ServerDetails(ServerDetails&& other)
        : serverId(other.serverId)
        , serviceLocator(other.serviceLocator.c_str())
        , session(other.session)
        , services(other.services)
        , expectedReadMBytesPerSec(other.expectedReadMBytesPerSec)
        , status(other.status)
        , replicationId(other.replicationId)
    {
    }

    ServerDetails& operator=(ServerDetails&& other) {
        if (this == &other)
            return *this;
        serverId = other.serverId;
        serviceLocator = other.serviceLocator.c_str();
        session = other.session;
        services = other.services;
        expectedReadMBytesPerSec = other.expectedReadMBytesPerSec;
        status = other.status;
        replicationId = other.replicationId;
        return *this;
    }

    virtual ~ServerDetails() {}

    /// ServerId associated with this index in the serverList (never reused).
    ServerId serverId;

    /// Service locator that can be used to contact this server.
    string serviceLocator;

    /// Cached session for communication with the server (may be NULL).
    Transport::SessionRef session;

    /// Which services are supported by the process at #serverId.
    ServiceMask services;

    /**
     * Disk bandwidth of the backup server in MB/s, if
     * services.has(BACKUP_SERVICE), invalid otherwise.
     */
    uint32_t expectedReadMBytesPerSec;

    /**
     * Whether this server is believed to be up, crashed, or down.
     * The crashed state initiates some operations throughout the cluster
     * (backup recovery) and also indicates that resources needed to recover
     * this server (replicas) need to be retained until this server is
     * completely removed from the cluster.
     */
    ServerStatus status;

    /**
     * The replication Id identifies a replication group for this server.
     * Each server belongs to a single replication group. With MinCopysets,
     * the segment's replicas are deterministically replicated on the
     * backups that are part of the server's replication group.
     * Each group has a unique Id. The default Id is 0.
     */
    uint64_t replicationId;
};

/**
 * Exception type thrown by the ServerList class.
 */
struct ServerListException : public Exception {
    ServerListException(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
};

/// Forward declaration.
class ServerTrackerInterface;

/**
 * AbstractServerList provides a common interface to READ from a map of
 * ServerIds to ServerDetails. This abstract class relies on its subclasses to
 * provide an underlying storage mechanism to map ServerIds to ServerDetails.
 *
 * This class provides support for ServerTrackers. If a module wishes
 * to keep track of changes to the ServerList, then it may register its own
 * private ServerTracker with the AbstractServerList. The tracker will be fed
 * updates (dependent on subclass). The tracker also provides a convenient
 * way to associate their own per-server state with ServerIds that they're
 * using or keeping track of.
 *
 * All the functions in this class are thread-safe (monitor-style).
 */
class AbstractServerList {
  PUBLIC:
    explicit AbstractServerList(Context* context);
    virtual ~AbstractServerList();

    virtual bool contains(ServerId id);
    string getLocator(ServerId id);
    ServerStatus getStatus(ServerId id);
    void flushSession(ServerId id);
    Transport::SessionRef getSession(ServerId id);
    uint64_t getVersion() const;
    bool isUp(ServerId id);
    size_t size() const;
    ServerId nextServer(ServerId prev, ServiceMask services,
            bool* end = NULL, bool includeCrashed = false);

    void registerTracker(ServerTrackerInterface& tracker);
    void unregisterTracker(ServerTrackerInterface& tracker);

    // The following method declaration is a hack that saves us from
    // linking CoordinatorServerList (and its dependents) in applications
    // other than the coordinator. The problem is that ServerIdRpcWrapper
    // is linked everywhere and invokes the method (but only if it knows
    // it's running on the coordinator). This method should never actually be
    // invoked except on CoordinatorServerList objects (which have overrided
    // this definition).
    virtual void serverCrashed(ServerId serverId) { }

    string toString(ServerId serverId);
    static string toString(ServerStatus status);
    string toString();

  PROTECTED:
    /// Internal Use Only - Does not grab locks

    /**
     * Retrieve the ServerDetails for the given server, or NULL if there
     * is no server with that id.
     *
     * \param id
     *      Identifies the desired server.
     */
    virtual ServerDetails* iget(ServerId id) = 0;

    /**
     * Retrieve the ServerDetails stored in the underlying subclass storage
     * at \a index, or NULL if there is no active server in that slot.
     *
     * \param index
     *      Index into table of server entries.
     */
    virtual ServerDetails* iget(uint32_t index) = 0;

    /**
     * Return the number of valid indexes in the list
     *
     * \return size_t - number of valid indexes.
     */
    virtual size_t isize() const = 0;

    /// Shared RAMCloud information.
    Context* context;

    /// Used to detect when the AbstractServerList has entered its destruction
    /// phase and will no longer accept new trackers.
    bool isBeingDestroyed;

    /// This variable is used to maintain consistency between the server
    /// list on the coordinator and those on masters. On the coordinator,
    /// this value is incremented for each update that it sends out to
    /// the rest of the cluster, and it includes that version number in the
    /// update RPCs.  On servers other than the coordinator, this variable
    /// contains the version of the most recent update received from the
    /// coordinator. It is used to ignore stale updates.
    uint64_t version;

    /// ServerTrackers that have registered with us and will receive updates
    /// regarding additions or removals from this list.
    std::vector<ServerTrackerInterface*> trackers;

    /// Mutex to protect the class. Simultaneous access can occur due to
    /// ServerTrackers registering/unregistering while adds and removes
    /// are being handled. Provides monitor-style protection for all operations
    /// on the ServerId map. A Lock for this mutex MUST be held to read or
    /// modify any state in the server list.
    mutable std::mutex mutex;

    typedef std::unique_lock<std::mutex> Lock;

    /**
     * The following variable is set to true during unit tests to skip
     * the server id check in getSession.
     */
    static bool skipServerIdCheck;

    DISALLOW_COPY_AND_ASSIGN(AbstractServerList);
};

} //namespace RAMCloud

#endif  ///!RAMCLOUD_ABSTRACTSERVERLIST_H
