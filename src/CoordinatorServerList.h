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

/**
 * \file
 * This file defines the CoordinatorServerList class.
 */

#ifndef RAMCLOUD_COORDINATORSERVERLIST_H
#define RAMCLOUD_COORDINATORSERVERLIST_H

#include "ServerList.pb.h"
#include "Tablets.pb.h"

#include "Rpc.h"
#include "ServiceMask.h"
#include "ServerId.h"
#include "ServerList.h"
#include "Tub.h"

namespace RAMCloud {

/// Forward declartion.
class ServerTrackerInterface;

/**
 * A CoordinatorServerList allocates ServerIds and holds Coordinator
 * state associated with live servers. It is closely related to the
 * ServerList and ServerTracker classes in that it essentially consists
 * of a map of ServerIds to some data. However, unlike ServerLists it
 * does not propagate events to trackers, and unlike ServerTrackers it
 * is not an asynchronous model of queued updates.
 *
 * If a module wishes to keep track of changes to the CoordinatorServerList
 * (when machines are added, crash, or are removed), then it may register
 * its own private ServerTracker with the CoordinatorServerList.
 * The tracker will be fed updates whenever servers come or go. The tracker
 * also provides a convenient way to associate their own per-server state
 * with ServerIds that they're using or keeping track of.
 *
 * Operations on CoordinatorServerList are thread-safe.
 */
class CoordinatorServerList {
  PUBLIC:
    /**
     * This class represents one entry in the CoordinatorServerList. Each
     * entry describes a specific server in the system and contains the
     * state that the Coordinator is maintain on its behalf.
     *
     * Note that pointer members are not allocated or freed by this class.
     * It's up to the user to ensure proper memory management, and they're
     * free to copy entries all they want.
     */
    class Entry : public ServerDetails {
      public:
        Entry();
        Entry(ServerId serverId,
              const string& serviceLocator,
              ServiceMask services);
        Entry(const Entry& other) = default;
        Entry& operator=(const Entry& other) = default;
        void serialize(ProtoBuf::ServerList_Entry& dest) const;

        bool isMaster() const {
            return (status == ServerStatus::UP) &&
                   services.has(MASTER_SERVICE);
        }
        bool isBackup() const {
            return (status == ServerStatus::UP) &&
                   services.has(BACKUP_SERVICE);
        }

        // Fields below this point are maintained on the coordinator only
        // and are not transmitted to members' ServerLists.

        /**
         * Any open replicas found during recovery are considered invalid
         * if they have a segmentId less than this.  This is used by masters
         * to invalidate replicas they have lost contact with while actively
         * writing to them.
         */
        uint64_t minOpenSegmentId;

        /**
         * Each segment's replicas are replicated on a set of backups, called
         * a replication group. Each group has a unique Id.
         */
        uint64_t replicationId;
    };

    explicit CoordinatorServerList(Context& context);
    ~CoordinatorServerList();
    ServerId add(string serviceLocator, ServiceMask serviceMask,
                 uint32_t readSpeed,
                 ProtoBuf::ServerList& update);
    void crashed(ServerId serverId,
                 ProtoBuf::ServerList& update);
    void remove(ServerId serverId,
                ProtoBuf::ServerList& update);
    void incrementVersion(ProtoBuf::ServerList& update);

    void setMinOpenSegmentId(ServerId serverId, uint64_t segmentId);
    void setReplicationId(ServerId serverId, uint64_t segmentId);

    const char* getLocator(ServerId id) const;
    Transport::SessionRef getSession(ServerId id) const;
    bool isUp(ServerId id) const;
    Entry operator[](const ServerId& serverId) const;
    Tub<Entry> operator[](size_t index) const;
    bool contains(ServerId serverId) const;
    size_t size() const;
    uint32_t masterCount() const;
    uint32_t backupCount() const;
    uint32_t nextMasterIndex(uint32_t startIndex) const;
    uint32_t nextBackupIndex(uint32_t startIndex) const;
    void serialize(ProtoBuf::ServerList& protoBuf) const;
    void serialize(ProtoBuf::ServerList& protobuf,
                   ServiceMask services) const;
    void sendMembershipUpdate(ProtoBuf::ServerList& update,
                              ServerId excludeServerId);

    void registerTracker(ServerTrackerInterface& tracker);
    void unregisterTracker(ServerTrackerInterface& tracker);

  PRIVATE:
    /**
     * The list of servers is just a vector of the following structure,
     * containing a permanent generation number that increments each
     * time an index is reused, and a Tubbed Entry, which describes the
     * server currently allocated to that slot, if there is one.
     */
    class GenerationNumberEntryPair {
      public:
        GenerationNumberEntryPair()
            : nextGenerationNumber(0),
              entry()
        {
        }

        /// The next generation number to be assigned in this slot.
        uint32_t nextGenerationNumber;

        /// If allocated, the entry associated with the ServerId in this slot.
        Tub<Entry> entry;
    };

    typedef std::lock_guard<std::mutex> Lock;

    void crashed(const Lock& lock,
                 ServerId serverId,
                 ProtoBuf::ServerList& update);
    uint32_t firstFreeIndex();
    const Entry& getReferenceFromServerId(const ServerId& serverId) const;
    void serialize(const Lock& lock, ProtoBuf::ServerList& protoBuf) const;
    void serialize(const Lock& lock, ProtoBuf::ServerList& protobuf,
                   ServiceMask services) const;

    /// Shared RAMCloud information.
    Context& context;

    /// Provides monitor-style protection for all operations on the tablet map.
    /// A Lock for this mutex must be held to read or modify any state in
    /// the server list.
    mutable std::mutex mutex;

    /// Slots in the server list.
    std::vector<GenerationNumberEntryPair> serverList;

    /// Number of masters in the server list.
    uint32_t numberOfMasters;

    /// Number of backups in the server list.
    uint32_t numberOfBackups;

    /// Incremented each time the server list is modified (i.e. when add or
    /// remove is called). Since we usually send delta updates to clients,
    /// they can use this to determine if any previous RPC was missed and
    /// then re-fetch the latest list in its entirety to get back on track.
    uint64_t versionNumber;

    /**
     * ServerTrackers that have registered with us and will receive updates
     * regarding additions or removals from this list.
     */
    std::vector<ServerTrackerInterface*> trackers;

    DISALLOW_COPY_AND_ASSIGN(CoordinatorServerList);
};

} // namespace RAMCloud

#endif // !RAMCLOUD_COORDINATORSERVERLIST_H
