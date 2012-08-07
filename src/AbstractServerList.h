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

#ifndef RAMCLOUD_ABSTRACTSERVERLIST_H
#define RAMCLOUD_ABSTRACTSERVERLIST_H

#include <mutex>

#include "Common.h"
#include "ServiceMask.h"
#include "ServerId.h"
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
    DOWN = 2,
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
        , services()
        , expectedReadMBytesPerSec()
        , status(ServerStatus::DOWN)
    {}

    /**
     * Create an instance where only #serverId is valid. Used to represent
     * the details of a SERVER_CRASHED or SERVER_REMOVED event.
     */
    ServerDetails(ServerId id, ServerStatus status)
        : serverId(id)
        , serviceLocator()
        , services()
        , expectedReadMBytesPerSec()
        , status(status)
    {}

    /**
     * Create an instance where all fields are valid.
     */
    ServerDetails(ServerId id,
                  const string& locator,
                  ServiceMask services,
                  uint32_t expectedReadMBytesPerSec,
                  ServerStatus status)
        : serverId(id)
        , serviceLocator(locator)
        , services(services)
        , expectedReadMBytesPerSec(expectedReadMBytesPerSec)
        , status(status)
    {}

    /*
     * Explicit string copies for the service locators in the following copy/move
     * constructors/assignments get rid of some kind of internal race in the
     * g++ 4.4.6's libstdc++ ref counting string implementation.
     */

    ServerDetails(const ServerDetails& other)
        : serverId(other.serverId)
        , serviceLocator(other.serviceLocator.c_str())
        , services(other.services)
        , expectedReadMBytesPerSec(other.expectedReadMBytesPerSec)
        , status(other.status)
    {
    }

    ServerDetails& operator=(const ServerDetails& other) {
        if (this == &other)
            return *this;
        serverId = other.serverId;
        serviceLocator = other.serviceLocator.c_str();
        services = other.services;
        expectedReadMBytesPerSec = other.expectedReadMBytesPerSec;
        status = other.status;
        return *this;
    }

    ServerDetails(ServerDetails&& other)
        : serverId(other.serverId)
        , serviceLocator(other.serviceLocator.c_str())
        , services(other.services)
        , expectedReadMBytesPerSec(other.expectedReadMBytesPerSec)
        , status(other.status)
    {
    }

    ServerDetails& operator=(ServerDetails&& other) {
        if (this == &other)
            return *this;
        serverId = other.serverId;
        serviceLocator = other.serviceLocator.c_str();
        services = other.services;
        expectedReadMBytesPerSec = other.expectedReadMBytesPerSec;
        status = other.status;
        return *this;
    }

    virtual ~ServerDetails() {}

    /// ServerId associated with this index in the serverList (never reused).
    ServerId serverId;

    /// Service locator which can be used to contact this server.
    string serviceLocator;

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
    explicit AbstractServerList(Context& context);
    virtual ~AbstractServerList();

    Transport::SessionRef getSession(ServerId id);
    virtual bool contains(ServerId id) const;
    const char* getLocator(ServerId id);
    uint64_t getVersion() const;
    bool isUp(ServerId id);
    size_t size() const;

    void registerTracker(ServerTrackerInterface& tracker);
    void unregisterTracker(ServerTrackerInterface& tracker);

    string toString(ServerId serverId);
    static string toString(ServerStatus status);
    string toString();

  PROTECTED:
    /// Internal Use Only - Does not grab locks

    /**
     * Retrieve the ServerDetails stored in the underlying subclass storage
     * at index index;
     *
     * \param index - index of underlying storage
     * \return ServerDetails contained at index
     */
    virtual ServerDetails* iget(size_t index) = 0;

    /**
     * Check of this ServerId is contained within the list.
     *
     * \param id - ServerId that to look up
     * \return bool - true if id is within list.
     */
    virtual bool icontains(ServerId id) const = 0;

    /**
     * Return the number of valid indexes in the list
     *
     * \return size_t - number of valid indexes.
     */
    virtual size_t isize() const = 0;

    /// Shared RAMCloud information.
    Context& context;

    /// Used to detect when the AbstractServerList has entered its destruction
    /// phase and will no longer accept new trackers.
    bool isBeingDestroyed;

    /// Incremented each time the server list is modified (i.e. when add or
    /// remove is called). Since we usually send delta updates to clients,
    /// they can use this to determine if any previous RPC was missed and
    /// then re-fetch the latest list in its entirety to get back on track.
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

    typedef std::lock_guard<std::mutex> Lock;
};

} //namespace RAMCloud

#endif  ///!RAMCLOUD_ABSTRACTSERVERLIST_H
