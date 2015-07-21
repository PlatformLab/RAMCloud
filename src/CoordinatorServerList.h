/* Copyright (c) 2011-2015 Stanford University
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

#include <condition_variable>
#include <thread>
#include <list>
#include <deque>

#include "MasterRecoveryInfo.pb.h"
#include "ServerListEntry.pb.h"
#include "ServerList.pb.h"

#include "AbstractServerList.h"
#include "CoordinatorUpdateManager.h"
#include "ServiceMask.h"
#include "ServerId.h"
#include "ServerTracker.h"
#include "Tub.h"

#include "ServerIdRpcWrapper.h"

namespace RAMCloud {

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
 * Protobuf until pushUpdate() is called, which will finalize the update.
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
    static const uint64_t MAX64 = ((uint64_t)-1);

    /// Used to describe new servers' versions
    static const uint64_t UNINITIALIZED_VERSION = ((uint64_t)0);

    /// Maximum number of server list incremental updates to batch
    /// in one UPDATE_SERVER_LIST RPC (large enough to get effective
    /// batching, small enough that we never overflow the RPC size limit).
    static const int MAX_UPDATES_PER_RPC = 100;

    /**
     * This class represents one entry in the CoordinatorServerList. Each
     * entry describes a specific server in the system and contains the
     * state that the Coordinator maintains on its behalf.
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
        void serialize(ProtoBuf::ServerList_Entry* dest) const;
        void sync(ExternalStorage* externalStorage);

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
         * The two fields below, verifiedVersion and updateVersion,
         * provide a mechanism to do a 2-phase commit when updating
         * servers.
         *
         * \b updateVersion stores the last version of the server list
         * sent to the server in an update rpc that has either not
         * been responded to yet or has succeeded already. In a sense,
         * this stores the speculative version of the server's server
         * list.
         *
         * \b verifiedVersion stores the latest version of the server
         * list that the server has acknowledged receiving.
         *
         * == Semantic meaning ==
         * Together, these variables help determine the update state
         * of the server. If they are equal to each other, then that
         * means there are currently no update rpcs being sent to
         * the server. Otherwise, there is an update rpc being sent
         * to the server and that rpc is trying to update the server
         * list up to updateVersion.
         *
         * One special state of the server is when both the variables
         * are equal to UNINITIALIZED_VERSION. This means that the
         * server has just been added to the server list and has not
         * yet have any updates sent to it yet.
         */

        /**
         * The latest version of the ServerList that server has acknowledged
         * receiving.
         */
        uint64_t verifiedVersion;

        /*
         * The version of the ServerList that was last sent out in an
         * RPC, which may be in progress or has completed successfully.
         * See comment block above verfiedVersion for more info.
         */
        uint64_t updateVersion;

        /**
         * Keeps track of updates to this entry that haven't yet been
         * sent to all of the servers in the cluster. This information also
         * gets recorded in external storage to ensure that the updates get
         * completed if we crash partway through.
         */
        std::deque<ProtoBuf::ServerListEntry_Update> pendingUpdates;
    };

    explicit CoordinatorServerList(Context* context);
    ~CoordinatorServerList();

    uint32_t backupCount() const;
    ServerId enlistServer(ServiceMask serviceMask, uint32_t preferredIndex,
                          uint32_t readSpeed, const char* serviceLocator);
    void haltUpdater();
    uint32_t masterCount() const;
    Entry operator[](ServerId serverId) const;
    Entry operator[](size_t index) const;
    void recover(uint64_t lastCompletedUpdate);
    void recoveryCompleted(ServerId serverId);
    void serialize(ProtoBuf::ServerList* protobuf, ServiceMask services) const;
    virtual void serverCrashed(ServerId serverId);
    bool setMasterRecoveryInfo(ServerId serverId,
                const ProtoBuf::MasterRecoveryInfo* recoveryInfo);
    void startUpdater();

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

   /**
    * This class implements the client-side Rpc to the membership service,
    * which runs on each RAMCloud server. The coordinator uses this Rpc to
    * push cluster membership updates so that servers have an up-to-date view of
    * all other servers in the cluster and receive failure notifications that
    * may require some action.
    *
    * See #MembershipService for more information.
    */
    class UpdateServerListRpc : public ServerIdRpcWrapper {
      friend class CoordinatorServerList;
      public:
        UpdateServerListRpc(Context* context, ServerId serverId,
                const ProtoBuf::ServerList* list);
        ~UpdateServerListRpc() {}
        /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
        void wait() {waitAndCheckErrors();}
        ServerId getTargetServerId();

      PRIVATE:
        bool appendServerList(const ProtoBuf::ServerList* list);
        DISALLOW_COPY_AND_ASSIGN(UpdateServerListRpc);
    };

    /**
     * This class is used by getWork to record information while scanning all
     * of the entries in the server list.
     */
    struct ScanMetadata {
        /**
         * Encodes the last version in which getWork() could not find
         * a server that needed a server list update that wasn't
         * already being updated. A value of 0 indicates that either
         * work was found during the last scan or there's a suspicion
         * there's additional work in the current epoch/version.
         *
         * The design decision of this being an epoch is so that when
         * the heuristic fails, it's only transient; it goes away
         * when a new server comes up or another dies (i.e. when the
         * server list updates to a newer version).
         */
        uint64_t noWorkFoundForEpoch;

        /**
         * Marks where a scan through the server list to find updates
         * would restart. This is set when the search loop exits and
         * during the scan, it serves as both a start and stop.
         */
        size_t searchIndex;

        /**
         * Minimum version of all the entry server list versions that have
         * been encountered since searchIndex was last reset to 0.
         */
        uint64_t minVersion;

        /**
         * The number of complete scans through the server list by getWork().
         * Since the server list expands with time, each scan through the
         * server list may represent different amounts of work and thus it's
         * not necessarily an interesting performance metric. It's used
         * primarily for debugging.
         */
        uint64_t completeScansSinceStart;

        ScanMetadata() : noWorkFoundForEpoch(0), searchIndex(0),
                     minVersion(0), completeScansSinceStart(0) {}
    };

    /**
     * Stores a ServerList protobuf for the changes that resulted in a
     * particular version of the server list. This is used by the
     * server list to keep track of past server list updates.
     */
    struct ServerListUpdate {
        /// Server list version # that corresponds with this update
        uint64_t version;

        /// Describes changes in the server list from previous version to
        /// this version.
        ProtoBuf::ServerList incremental;

        explicit ServerListUpdate(uint64_t version)
                : version(version)
                , incremental()
        {}

        ServerListUpdate(const ServerListUpdate& source)
                : version(source.version)
                , incremental(source.incremental)
        {}

        ServerListUpdate& operator=(const ServerListUpdate& source)
        {
            version = source.version;
            incremental = source.incremental;
            return *this;
        }
    };

    /// Internal Use Only - Does not grab locks
    ServerDetails* iget(ServerId id);
    ServerDetails* iget(uint32_t index);
    size_t isize() const;

    /// Functions related to modifying the server list
    uint32_t firstFreeIndex(const Lock& lock);
    CoordinatorServerList::Entry* getEntry(ServerId id) const;
    CoordinatorServerList::Entry* getEntry(size_t index) const;
    void persistAndPropagate(const Lock& lock, Entry* entry,
                             ServerChangeEvent event);
    void recoveryCompleted(const Lock& lock, ServerId serverId);
    void serialize(const Lock& lock, ProtoBuf::ServerList* protoBuf) const;
    void serialize(const Lock& lock, ProtoBuf::ServerList* protoBuf,
                   ServiceMask services) const;

    /// Functions related to replication groups.
    bool assignReplicationGroup(const Lock& lock, uint64_t replicationId,
                                const vector<ServerId>* replicationGroupIds);
    void createReplicationGroups(const Lock& lock);
    void removeReplicationGroup(const Lock& lock, uint64_t groupId);
    void repairReplicationGroups(const Lock& lock);

    /// Functions related to keeping the cluster up-to-date
    void pushUpdate(const Lock& lock, Entry* entry);
    void insertUpdate(const Lock& lock, Entry* entry, uint64_t version);
    void updateLoop();
    void checkUpdates();
    void sync();

    bool isClusterUpToDate(const Lock& lock);
    void pruneUpdates(const Lock& lock);

    bool getWork(Tub<UpdateServerListRpc>* rpc);
    void workSuccess(ServerId id, uint64_t currentVersion);
    void workFailed(ServerId id);
    void waitForWork();

    /// Shared information about the server.
    Context* context;

    /// Slots in the server list.
    std::vector<GenerationNumberEntryPair> serverList;

    /// Number of masters in the server list.
    uint32_t numberOfMasters;

    /// Number of backups in the server list.
    uint32_t numberOfBackups;

    /**
     * Indicates that the updateLoop() method should return and
     * therefore exit the updater thread. Do NOT set this manually,
     * use haltUpdater() and startUpdater().
     */
    bool stopUpdater;

    /**
     * True means the updater is sleeping because there is no work for
     * it to do; false means the updater is actively sending RPCs and
     * polling for completion. This variable is intended only for testing.
     */
    bool updaterSleeping;

    /// Metadata from previous partial scan through server list to find updates
    ScanMetadata lastScan;

    /**
     * Past updates that lead up to the \a version. This does not contain
     * all the updates created, only the ones that haven't yet been propagated
     * to the rest of the cluster. Older updates are pruned.
     */
    std::deque<ServerListUpdate> updates;

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

    /// Runs the asynchronous server list updater (updateLoop())
    Tub<std::thread> updaterThread;

    /**
     * RPC objects for updates that are currently outstanding. These are
     * dynamically allocated and must be freed manually.
     */
    std::list<Tub<UpdateServerListRpc>*> activeRpcs;

    /**
     * RPC objects not currently in use (and not constructed); we save them
     * here to avoid having to reallocate them later.  These are
     * dynamically allocated and must be freed manually.
     */
    std::list<Tub<UpdateServerListRpc>*> spareRpcs;

    /**
     * The highest ServerLister version that is known to have been
     * received by all servers in the cluster (i.e. updates for this
     * and earlier versions can now be discarded).
     */
    uint64_t maxConfirmedVersion;

    /**
     * Number of servers currently being sent updates. This is used
     * as part of a fast check to see if servers are being updated.
     */
    uint32_t numUpdatingServers;

    /**
     * The number of backups in a replication group. Currently there is
     * no way to set this value based on cluster configuration information
     * (RAM-555).
     */
    uint32_t replicationGroupSize;

    /**
     * The highest id of a replication group that has been assigned so far.
     * Note: id 0 is never used.
     */
    uint64_t maxReplicationId;
    DISALLOW_COPY_AND_ASSIGN(CoordinatorServerList);
};
} // namespace RAMCloud

#endif // !RAMCLOUD_COORDINATORSERVERLIST_H
