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

#ifndef RAMCLOUD_COORDINATORSERVERLIST_H
#define RAMCLOUD_COORDINATORSERVERLIST_H

#include "ServerList.pb.h"

#include "AbstractServerList.h"
#include "ServiceMask.h"
#include "ServerId.h"
#include "ServerListUpdater.h"
#include "Tub.h"

namespace RAMCloud {

/// Forward Declaration
class ServerListUpdater;

/**
 * A CoordinatorServerList allocates ServerIds and holds Coordinator
 * state associated with live servers. It is closely related to the
 * ServerList and ServerTracker classes in that it essentially consists
 * of a map of ServerIds to some data and supports ServerTrackers. The
 * tracker will be fed updates whenever servers come or go (add,
 * crashed, removed).
 *
 * Additionally, this class contains the logic to propagate membership updates
 * (add/crashed/remove) and send the full list to ServerIds on the list.
 * Add/Crashed/Removes statuses are buffered into an internally managed
 * Protobuf until sendMembershipUpdate() is called, which will flush the buffer.
 * The updates are done asynchronously from the CoordinatorServerList call
 * thread. sync() can be called to force a synchronization point.
 *
 * CoordinatorServerList is thread-safe and supports ServerTrackers.
 *
 * This class publicly extends AbstractServerList to provide a common
 * interface to READ from map of ServerIds and (un)register trackers.
 */
class CoordinatorServerList : public AbstractServerList{
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
                   services.has(WireFormat::MASTER_SERVICE);
        }
        bool isBackup() const {
            return (status == ServerStatus::UP) &&
                   services.has(WireFormat::BACKUP_SERVICE);
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

        /**
         * Entry id corresponding to entry in LogCabin log that has
         * intial information for this server.
         */
         LogCabin::Client::EntryId serverInfoLogId;

        /**
         * Entry id corresponding to entry in LogCabin log that has
         * updates for this server.
         */
         LogCabin::Client::EntryId serverUpdateLogId;
    };

    explicit CoordinatorServerList(Context* context);
    ~CoordinatorServerList();
    void add(ServerId serverId, string serviceLocator,
             ServiceMask serviceMask, uint32_t readSpeed);
    void crashed(ServerId serverId);
    ServerId generateUniqueId();
    void remove(ServerId serverId);

    void setMinOpenSegmentId(ServerId serverId, uint64_t segmentId);
    void setReplicationId(ServerId serverId, uint64_t segmentId);

    Entry operator[](const ServerId& serverId) const;
    Tub<Entry> operator[](size_t index) const;
    Entry at(const ServerId& serverId) const;
    Tub<Entry> at(size_t index) const;
    uint32_t masterCount() const;
    uint32_t backupCount() const;
    uint32_t nextMasterIndex(uint32_t startIndex) const;
    uint32_t nextBackupIndex(uint32_t startIndex) const;
    void serialize(ProtoBuf::ServerList& protoBuf) const;
    void serialize(ProtoBuf::ServerList& protobuf,
                   ServiceMask services) const;

    void sendServerList(ServerId& serverId);
    void sync();

    void addServerInfoLogId(ServerId serverId,
                            LogCabin::Client::EntryId entryId);
    void addServerUpdateLogId(ServerId serverId,
                              LogCabin::Client::EntryId entryId);
    LogCabin::Client::EntryId getServerInfoLogId(ServerId serverId);
    LogCabin::Client::EntryId getServerUpdateLogId(ServerId serverId);

  PROTECTED:
    /// Internal Use Only - Does not grab locks
    ServerDetails* iget(ServerId id);
    ServerDetails* iget(uint32_t index);
    size_t isize() const;

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

    void add(Lock& lock, ServerId serverId, string serviceLocator,
             ServiceMask serviceMask, uint32_t readSpeed);
    void crashed(const Lock& lock, ServerId serverId);
    void remove(Lock& lock, ServerId serverId);
    void sendMembershipUpdate(ServerId excludeServerId);
    uint32_t firstFreeIndex();
    const Entry& getReferenceFromServerId(const ServerId& serverId) const;
    void serialize(const Lock& lock, ProtoBuf::ServerList& protoBuf) const;
    void serialize(const Lock& lock, ProtoBuf::ServerList& protobuf,
                   ServiceMask services) const;

     /// Used to propagate ServerList changes in the background.
    ServerListUpdater updater;

    /// Slots in the server list.
    std::vector<GenerationNumberEntryPair> serverList;

    /// Number of masters in the server list.
    uint32_t numberOfMasters;

    /// Number of backups in the server list.
    uint32_t numberOfBackups;

    /**
     * Stores add/remove/crashed updates to server list until a
     * sendMembershipUpdate call which will update the version number, enqueue
     * a copy to the BackgroundUpdater work queue and clear() this entry.
     * \a update can contain remove, crash, and add notifications,
     * but removals/crashes must precede additions in the update to ensure
     * ordering guarantees about notifications related to servers which
     * re-enlist.  For now, this means calls to remove() and crashed() must
     * proceed call to add() if they have a common \a update.
     */
    ProtoBuf::ServerList updates;

    DISALLOW_COPY_AND_ASSIGN(CoordinatorServerList);
};

} // namespace RAMCloud

#endif // !RAMCLOUD_COORDINATORSERVERLIST_H
