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

#include <condition_variable>
#include <deque>

#include <Client/Client.h> // NOLINT

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
    static const uint64_t UNINITIALIZED_VERSION = ((uint64_t)-1);

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
         * list that the server received, applied, and responded to.
         * In a sense, this stores the version of the server list that
         * has been "committed" on the server.
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
         *
         * == Legal modifications ==
         * With these definitions in place, the mapping of a 2-phase
         * commit to these variables is quite natural:
         *
         * Semantic Action        -> Literal Action
         * Start a new update RPC -> Set updateVersion = version of update RPC
         * RPC failed (rollback)  -> Set updateVersion = verifiedVersion
         * RPC Success (commit)   -> Set verfiedVersion = updateVersion
         *
         */

         /**
          * The latest version of the ServerList that server received,
          * applied, and ACKed to. See comment block above for more info.
          */
        uint64_t verifiedVersion;

         /*
          * The version of the ServerList that was last sent out in an
          * RPC, which may be in progress or has completed successfully.
          * See comment block above verfiedVersion for more info.
          */
        uint64_t updateVersion;

        /**
         * Entry id corresponding to entry in LogCabin log that has
         * initial information for this server.
         */
         LogCabin::Client::EntryId serverInfoLogId;

        /**
         * Entry id corresponding to entry in LogCabin log that has
         * the most recent update for this server.
         */
         LogCabin::Client::EntryId serverUpdateLogId;
    };

    explicit CoordinatorServerList(Context* context);
    ~CoordinatorServerList();

    uint32_t backupCount() const;
    ServerId enlistServer(ServerId replacesId, ServiceMask serviceMask,
                          const uint32_t readSpeed, const char* serviceLocator);
    uint32_t masterCount() const;
    Entry operator[](ServerId serverId) const;
    Entry operator[](size_t index) const;
    void removeAfterRecovery(ServerId serverId);
    void serialize(ProtoBuf::ServerList& protobuf, ServiceMask services) const;
    void serverDown(ServerId serverId);
    bool setMasterRecoveryInfo(ServerId serverId,
                const ProtoBuf::MasterRecoveryInfo& recoveryInfo);

    /// Functions for CoordinatorServerList Recovery.
    void recoverEnlistedServer(ProtoBuf::ServerInformation* state,
                               EntryId entryId);
    void recoverEnlistServer(ProtoBuf::ServerInformation* state,
                             EntryId entryId);
    void recoverServerDown(ProtoBuf::ServerDown* state,
                           EntryId entryId);
    void recoverServerUpdate(ProtoBuf::ServerUpdate* state,
                             EntryId entryId);

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
     * Defines methods for enlisting a server, for persisting required
     * information in LogCabin, and using it to recover if the
     * Coordinator crashes.
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
     * Defines methods and stores data to remove a server from the cluster,
     * for persisting required information in LogCabin, and using it to
     * recover if the Coordinator crashes.
     *
     * Removing the server includes marking the server as crashed,
     * propagating that information (through server trackers and the
     * cluster updater) and invoking recovery.
     * Once recovery has finished, the server will be removed from server list.
     */
    class ServerDown {
        public:
            ServerDown(CoordinatorServerList &csl,
                       Lock& lock,
                       ServerId serverId)
                : csl(csl), lock(lock),
                  serverId(serverId) {}
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
     * Defines methods and stores data to set update-able fields corresponding
     * to a server, for persisting required information in LogCabin,
     * and using it to recover if the Coordinator crashes.
     */
    class ServerUpdate {
        public:
            ServerUpdate(CoordinatorServerList &csl,
                         Lock& lock,
                         ServerId serverId,
                         const ProtoBuf::MasterRecoveryInfo& recoveryInfo,
                         EntryId oldServerUpdateEntryId = NO_ID)
                : csl(csl), lock(lock),
                  serverId(serverId),
                  recoveryInfo(recoveryInfo),
                  oldServerUpdateEntryId(oldServerUpdateEntryId) {}
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
            /**
             * LogCabin entry id for the previous ServerUpdate entry appended
             * to LogCabin corresponding to this server (if any).
             */
            EntryId oldServerUpdateEntryId;
            DISALLOW_COPY_AND_ASSIGN(ServerUpdate);
    };

    /**
     * State of partial scans through the server list to find servers
     * that require updates.
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
         * during the scan, it serves as both a start and stop
         */
        size_t searchIndex;

        /**
         * Minimum version of all the entry server list versions that have
         * been encountered thus far in the scan.
         */
        uint64_t minVersion;

        ScanMetadata() : noWorkFoundForEpoch(0), searchIndex(0),
                     minVersion(UNINITIALIZED_VERSION) {}
    };

    /**
     * Stores the incremental and full Server List protobufs for a
     * particular version of the server list. This is used by the server list
     * to keep track of past server list updates. Note that the full
     * list is a Tub because it may not be needed such as in the case
     * of a crash/remove only update (i.e. no new server needs a full
     * list) and in the case where servers are added faster than the
     * updater thread can poll. In the latter case, many full versions
     * may be skipped in favor for the latest full server list.
     */
    struct ServerListUpdatePair {
        /// Version of the ServerLists contained
        uint64_t version;

        /// Incremental ServerList for this version
        ProtoBuf::ServerList incremental;

        /// Full ServerList for this version (may not be occupied, see above)
        Tub<ProtoBuf::ServerList> full;

        explicit ServerListUpdatePair(ProtoBuf::ServerList& incremental)
                : version(incremental.version_number())
                , incremental(incremental)
                , full()
        {}
    };

    /**
     * Describes the basic work unit that can be assigned to the
     * updater thread. It provides the serverId and the RANGE of
     * updates that should be batched up and sent to the server
     * in one shot.
     *
     * There is an implicit contract that comes with every work unit
     * handed out by the coordinator. Once a work unit is handed out,
     * it is expected that a call back to workSuccess or workFailed
     * with the target serverId will eventually occur and until it
     * does, these conditions hold:
     *      a) The ServerList will not hand out more UpdateUnit's for
     *         the server addressed by targerServer.
     *      b) The range of updates described by firstUpdate to
     *         updateVersionTail are GUARANTEED to remain valid
     *         until a call back to workSuccess/Failed occurs.
     *      c) There are no guarantees about the integrity of updates
     *         outside this range so don't decrement the iterator and
     *         don't iterate past the updateVersionTail.
     *
     * The implications of a dropped WorkUnit would mean that part of
     * the cluster will indefinitely remain out of date and the false
     * report of a workSuccess would result in server/backup suicide.
     * The latter case happens because if a server/backup misses an
     * update, there is no guarantee that the required update protobuf
     * version would still be around on the coordinator when the server
     * realizes that it had missed an update.
     *
     * A false report of a workFailed however, would result in a transient
     * bug whereby duplicate updates are sent to the server. This will not
     * result in suicide so it is safe to invoke workFailed in error cases.
     */
    struct UpdaterWorkUnit {
        /// To whom to send the update
        ServerId targetServer;

        /// Whether to send full or partial update
        bool sendFullList;

        /// An iterator to the update deque starting at the first
        /// update that should be sent to the server.
        std::deque<ServerListUpdatePair>::const_iterator firstUpdate;

        /// Signifies the end range to be sent to the server.
        /// Practically, it is used to stop iterating.
        uint64_t updateVersionTail;

        UpdaterWorkUnit()
          : targetServer(), sendFullList(), firstUpdate(), updateVersionTail()
        {}
    };

    /// Internal Use Only - Does not grab locks
    ServerDetails* iget(ServerId id);
    ServerDetails* iget(uint32_t index);
    size_t isize() const;

    /// Functions related to modifying the server list
    void add(Lock& lock, ServerId serverId, string serviceLocator,
             ServiceMask serviceMask, uint32_t readSpeed);
    void crashed(const Lock& lock, ServerId serverId);
    uint32_t firstFreeIndex();
    ServerId generateUniqueId(Lock& lock);
    CoordinatorServerList::Entry* getEntry(ServerId id) const;
    CoordinatorServerList::Entry* getEntry(size_t index) const;
    void remove(Lock& lock, ServerId serverId);
    void serialize(const Lock& lock, ProtoBuf::ServerList& protoBuf) const;
    void serialize(const Lock& lock, ProtoBuf::ServerList& protoBuf,
                   ServiceMask services) const;

    /// Functions related to replication groups.
    bool assignReplicationGroup(Lock& lock, uint64_t replicationId,
                                const vector<ServerId>& replicationGroupIds);
    void createReplicationGroup(Lock& lock);
    void removeReplicationGroup(Lock& lock, uint64_t groupId);

    /// Functions related to keeping the cluster up-to-date
    void pushUpdate(const Lock& lock);
    void haltUpdater();
    void startUpdater();
    void updateLoop();
    void sync();

    bool isClusterUpToDate(const Lock& lock);
    void pruneUpdates(const Lock& lock);

    bool getWork(UpdaterWorkUnit* wu);
    void workSuccess(ServerId id) ;
    void workFailed(ServerId id);
    void waitForWork();

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

    /// Metadata from previous partial scan through server list to find updates
    ScanMetadata lastScan;

    /**
     * Stores add/remove/crashed updates to server list until a
     * pushUpdate call which will update the version number, enqueue
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
    std::deque<ServerListUpdatePair> updates;

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
     * Indicates the the oldest ServerList version amongst servers
     * that have received updates from us.
     */
    uint64_t minConfirmedVersion;

    /**
     * Number of servers currently being sent updates. This is used
     * as part of a fast check to see if servers are being updated.
     */
    uint32_t numUpdatingServers;

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
