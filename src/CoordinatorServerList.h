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


#include <Client/Client.h>
#include <deque>

#include "MasterRecoveryInfo.pb.h"
#include "ServerList.pb.h"

#include "ServerInformation.pb.h"
#include "ServerUpdate.pb.h"
#include "ServerDown.pb.h"

#include "AbstractServerList.h"
#include "MembershipClient.h"
#include "ServiceMask.h"
#include "ServerId.h"
#include "Tub.h"

namespace RAMCloud {

// Not using LogCabin::Client::Entry since the CoordinatorServerList also
// defines an Entry class.
using LogCabin::Client::EntryId;
using LogCabin::Client::NO_ID;

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
  * Protobuf until commitUpdate() is called, which will finalize the update.
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
         * Stores information about the server for use during recovery.
         * This information is completely opaque to the coordinator during
         * normal operation and is only used in master recovery. Basically,
         * it is used in cases when masters need to ensure that replicas
         * from backups which they lost contact with cannot be used during
         * recovery.
         */
        ProtoBuf::MasterRecoveryInfo masterRecoveryInfo;

        /**
         * The last version of the ServerList that was successfully received
         * by this server. A value of 0 indicates that a server list was
         * never sent.
         */
        uint64_t serverListVersion;

        /**
         * Marks whether the entry is being sent an update rpc or not.
         * 0 means it's not updating and any other value is what it's being
         * updated to.
         */
        uint64_t isBeingUpdated;

        /**
         * Entry id corresponding to entry in LogCabin log that has
         * initial information for this server.
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

    void removeAfterRecovery(ServerId serverId);

    void setMasterRecoveryInfo(ServerId serverId,
        const ProtoBuf::MasterRecoveryInfo& recoveryInfo);
    void recoverMasterRecoveryInfo(ProtoBuf::ServerUpdate* state,
                                      EntryId entryId);

    Entry operator[](ServerId serverId) const;
    Entry operator[](size_t index) const;

    uint32_t masterCount() const;
    uint32_t backupCount() const;

    void serialize(ProtoBuf::ServerList& protobuf, ServiceMask services) const;

    void serverDown(ServerId serverId);
    ServerId enlistServer(ServerId replacesId, ServiceMask serviceMask,
                          const uint32_t readSpeed, const char* serviceLocator);
    void recoverEnlistServer(ProtoBuf::ServerInformation* state,
                             EntryId entryId);
    void recoverEnlistedServer(ProtoBuf::ServerInformation* state,
                               EntryId entryId);
    void recoverServerDown(ProtoBuf::ServerDown* state,
                           EntryId entryId);

  PRIVATE:
    /**
     * Defines methods and stores data to enlist a server.
     */
    class EnlistServer {
      public:
          EnlistServer(CoordinatorServerList &csl,
                       Lock& lock,
                       ServerId newServerId,
                       ServiceMask serviceMask,
                       uint32_t readSpeed,
                       const char* serviceLocator)
              : csl(csl), lock(lock),
                newServerId(newServerId),
                serviceMask(serviceMask),
                readSpeed(readSpeed),
                serviceLocator(serviceLocator) {}
          ServerId execute();
          ServerId complete(EntryId entryId);

      private:
          /**
           * Reference to the instance of CoordinatorServerList
           * initializing this class.
           */
          CoordinatorServerList &csl;
          /**
           * Explicity needs CoordinatorServerList lock.
           */
          Lock& lock;
          /**
           * The id assigned to the enlisting server.
           */
          ServerId newServerId;
    	  /**
    	   * Services supported by the enlisting server.
    	   */
          ServiceMask serviceMask;
          /**
           * Read speed of the enlisting server in MB/s.
    	   */
          const uint32_t readSpeed;
    	  /**
    	   * Service Locator of the enlisting server.
    	   */
          const char* serviceLocator;
          DISALLOW_COPY_AND_ASSIGN(EnlistServer);
    };

    /**
     * Defines methods and stores data to remove a server from the cluster.
     */
    class ServerDown {
        public:
            ServerDown(CoordinatorServerList &csl,
                            Lock& lock,
                            ServerId serverId)
                : csl(csl), lock(lock), serverId(serverId) {}
            void execute();
            void complete(EntryId entryId);
        private:
            /**
             * Reference to the instance of CoordinatorServerList
             * initializing this class.
             */
            CoordinatorServerList &csl;
            /**
             * Explicity needs CoordinatorServerList lock.
             */
            Lock& lock;
            /**
             * ServerId of the server that is suspected to be down.
             */
            ServerId serverId;
            DISALLOW_COPY_AND_ASSIGN(ServerDown);
    };

    /**
     * Defines methods and stores data to set recovery info of server
     * with id serverId to segmentId.
     */
    class SetMasterRecoveryInfo {
        public:
            SetMasterRecoveryInfo(
                    CoordinatorServerList &csl,
                    Lock& lock,
                    ServerId serverId,
                    const ProtoBuf::MasterRecoveryInfo& recoveryInfo)
                : csl(csl), lock(lock), serverId(serverId)
                , recoveryInfo(recoveryInfo) {}
            void execute();
            void complete(EntryId entryId);
        private:
            /**
             * Reference to the instance of CoordinatorServerList
             * initializing this class.
             */
            CoordinatorServerList &csl;
            /**
             * Explicity needs CoordinatorServerList lock.
             */
            Lock& lock;
            /**
             * ServerId of the server whose recovery info will be set.
             */
            ServerId serverId;
            /**
             * The new master recovery info to be set.
             */
            ProtoBuf::MasterRecoveryInfo recoveryInfo;
            DISALLOW_COPY_AND_ASSIGN(SetMasterRecoveryInfo);
    };

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

    /**
     * Information needed to handle an async server list RPC and call back.
     */
    struct UpdateSlot {
        UpdateSlot()
            : startCycle()
            , serverId()
            , originalVersion()
            , protobuf()
            , rpc()
        {
        }

        /// Cycle at which the update Rpc was started
        uint64_t startCycle;

        /// Intended recipient of the update
        ServerId serverId;

        /// Server List version of the recipient before update
        uint64_t originalVersion;

        /// Update to send out (can be full list or partial membership update)
        ProtoBuf::ServerList protobuf;

        /// Actual Rpc
        Tub<UpdateServerListRpc> rpc;

        DISALLOW_COPY_AND_ASSIGN(UpdateSlot);
    };

    /**
     * State of partial scans through the server list to find updates.
     * Modifying these fields can break search heuristics and cause
     * \a deadlock.
     */
    struct ScanMetadata {
        /**
         * Indicates that no updates were found in the last scan.
         * Only hasUpdates() should toggle this field to true, but
         * it is safe to toggle false by other entities. It is used
         * to skip scans through the server list.
         */
        bool noUpdatesFound;

        /**
         * The index that the server list left off on during
         * its last scan for entries to update.
         */
        size_t searchIndex;

        /**
         * Minimum version of all the entry server list versions that have
         * been encountered thus far in the scan.
         */
        uint64_t minVersion;
    };

    /// Functions related to modifying the server list
    void add(Lock& lock, ServerId serverId, string serviceLocator,
             ServiceMask serviceMask, uint32_t readSpeed);
    void crashed(const Lock& lock, ServerId serverId);
    void remove(Lock& lock, ServerId serverId);
    ServerId generateUniqueId(Lock& lock);
    uint32_t firstFreeIndex();
    const Entry& getReferenceFromServerId(const Lock& lock,
                                          ServerId serverId) const;
    const Entry& getReferenceFromIndex(const Lock& lock, size_t index) const;
    void serialize(const Lock& lock, ProtoBuf::ServerList& protoBuf) const;
    void serialize(const Lock& lock, ProtoBuf::ServerList& protobuf,
                   ServiceMask services) const;

    /// Functions related to keeping the cluster up-to-date
    bool isClusterUpToDate(const Lock& lock);
    void commitUpdate(const Lock& lock);
    void pruneUpdates(const Lock& lock, uint64_t version);
    void startUpdater();
    void haltUpdater();
    void updateLoop();
    void sync();
    bool dispatchRpc(UpdateSlot& update);
    bool hasUpdates(const Lock& lock);
    bool loadNextUpdate(UpdateSlot& updateRequest);
    void updateEntryVersion(ServerId serverId, uint64_t version);

    void setReplicationId(Lock& lock, ServerId serverId, uint64_t segmentId);
    void addServerInfoLogId(Lock& lock, ServerId serverId,
                            LogCabin::Client::EntryId entryId);
    void addServerUpdateLogId(Lock& lock, ServerId serverId,
                              LogCabin::Client::EntryId entryId);
    LogCabin::Client::EntryId getServerInfoLogId(Lock& lock, ServerId serverId);
    LogCabin::Client::EntryId getServerUpdateLogId(Lock& lock,
                                                   ServerId serverId);

    bool assignReplicationGroup(Lock& lock, uint64_t replicationId,
                                const vector<ServerId>& replicationGroupIds);
    void createReplicationGroup(Lock& lock);
    void removeReplicationGroup(Lock& lock, uint64_t groupId);
    void serverDown(Lock& lock, ServerId serverId);

    /// Internal Use Only - Does not grab locks
    ServerDetails* iget(ServerId id);
    ServerDetails* iget(uint32_t index);
    size_t isize() const;

    /// Slots in the server list.
    std::vector<GenerationNumberEntryPair> serverList;

    /// Number of masters in the server list.
    uint32_t numberOfMasters;

    /// Number of backups in the server list.
    uint32_t numberOfBackups;

    /// max number of concurrent update Rpcs that can be issued (soft limit)
    uint32_t concurrentRPCs;

    /// Timeout for the rpcs in nanoseconds before they are cancel()ed
    /// Value of 0 = infinite time
    uint64_t rpcTimeoutNs;

    /**
     * Indicates that the updateLoop() method should return and
     * therefore exit the updater thread. Do NOT set this manually,
     * use haltUpdater() and startUpdater().
     */
    bool stopUpdater;

    /// Metadata from previous partial scan through server list to find updates
    ScanMetadata lastScan;

    /**
     * Stores add/remove/crashed updates to server list until a
     * commitUpdate call which will update the version number, enqueue
     * a copy to the updates list and clear() this entry.
     *
     * \a update can contain remove, crash, and add notifications,
     * but removals/crashes must precede additions in the update to ensure
     * ordering guarantees about notifications related to servers which
     * re-enlist.  For now, this means calls to remove() and crashed() must
     * proceed call to add() if they have a common \a update.
     */
    ProtoBuf::ServerList update;

    /**
     * Past updates that lead up to the \a version. This does not contain
     * all the updates created, only the ones needed by the servers
     * currently in the server list. Older updates are pruned.
     */
    std::deque<ProtoBuf::ServerList> updates;

    /**
     * Triggered when the server list is detected to be out of date or
     * when the stop is toggled (to start/stop the updater thread).
     */
    std::condition_variable hasUpdatesOrStop;

    /**
     * Triggered when all the servers (that can accept updates) in the
     * server list have the most recent version of the server list. This
     * used to notify entities that want to know when all the server list
     * updates have been pushed to the entire cluster.
     */
    std::condition_variable listUpToDate;

    /// Thread that runs in the background to send out updates.
    Tub<std::thread> thread;

    /**
     * The id of the next replication group to be created. The replication
     * group is a set of backups that store all of the replicas of a segment.
     * NextReplicationId starts at 1 and is never reused.
     * Id 0 is reserved for nodes that do not belong to a replication group.
     */
    uint64_t nextReplicationId;

    DISALLOW_COPY_AND_ASSIGN(CoordinatorServerList);
};
} // namespace RAMCloud

#endif // !RAMCLOUD_COORDINATORSERVERLIST_H
