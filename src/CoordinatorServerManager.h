/* Copyright (c) 2009-2012 Stanford University
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

#ifndef RAMCLOUD_COORDINATORSERVERMANAGER_H
#define RAMCLOUD_COORDINATORSERVERMANAGER_H

#include <Client/Client.h>

#include "Common.h"
#include "CoordinatorServerList.h"
#include "StateHintServerDown.pb.h"
#include "StateSetMinOpenSegmentId.pb.h"

namespace RAMCloud {

class CoordinatorService;

using LogCabin::Client::Entry;
using LogCabin::Client::EntryId;
using LogCabin::Client::NO_ID;

/**
 * Handles all server configuration details on behalf of the coordinator.
 * Provides an interface to the coordinator to service rpcs that modify
 * server configuration.
 */
class CoordinatorServerManager {
  PUBLIC:
    explicit CoordinatorServerManager(CoordinatorService& coordinatorService);
    ~CoordinatorServerManager();

    /**
     * Defines methods and stores data to enlist a server.
     */
    // TODO(ankitak): This code will become much simpler after
    // RAM-431 is resolved.
    class EnlistServer {
        public:
            EnlistServer(CoordinatorServerManager &manager,
                         ServerId replacesId,
                         ServiceMask serviceMask,
                         const uint32_t readSpeed,
                         const uint32_t writeSpeed,
                         const char* serviceLocator)
                : manager(manager),
                  replacesId(replacesId), replacedEntry(),
                  newServerId(), serviceMask(serviceMask),
                  readSpeed(readSpeed), writeSpeed(writeSpeed),
                  serviceLocator(serviceLocator),
                  serverListUpdate() {}
            ServerId beforeReply();
            void afterReply();
        private:
            /**
             * Reference to the instance of coordinator server manager
             * initializing this class.
             * Used to get access to CoordinatorService& service.
             */
            CoordinatorServerManager &manager;
			/**
			 * Server id of the server that the enlisting server is replacing.
			 */
            ServerId replacesId;
			/**
			 * Keeps track of the details of the server that is being forced
			 * out of the cluster by the enlister so we can start recovery.
			 */
            Tub<CoordinatorServerList::Entry> replacedEntry;
			/**
			 * The id assigned to the enlisting server.
			 */
            ServerId newServerId;
			/**
			 * Services supported by the enlisting server.
			 */
            ServiceMask serviceMask;
			/**
			 * Read speed of the enlisting server.
			 */
            const uint32_t readSpeed;
			/**
			 * Write speed of the enlisting server.
			 */
            const uint32_t writeSpeed;
			/**
			 * Service Locator of the enlisting server.
			 */
            const char* serviceLocator;
			/**
			 * Keeps track of the server list updates to be sent
			 * to the cluster.
			 */
            ProtoBuf::ServerList serverListUpdate;
            DISALLOW_COPY_AND_ASSIGN(EnlistServer);
    };

    /**
     * The ping timeout used when the Coordinator verifies an incoming
     * hint server down message. Until we resolve the scheduler issues that we
     * have been seeing this timeout should be at least 250ms.
     */
    static const int TIMEOUT_USECS = 250 * 1000;

    bool assignReplicationGroup(uint64_t replicationId,
                                const vector<ServerId>& replicationGroupIds);
    void createReplicationGroup();
    ServerId enlistServerBeforeReply(EnlistServer& ref);
    void enlistServerAfterReply(EnlistServer& ref);
    ProtoBuf::ServerList getServerList(ServiceMask serviceMask);
    bool hintServerDown(ServerId serverId);
    void hintServerDownRecover(ProtoBuf::StateHintServerDown* state,
                               EntryId entryId);
    void removeReplicationGroup(uint64_t groupId);
    void sendServerList(ServerId serverId);
    void setMinOpenSegmentId(ServerId serverId, uint64_t segmentId);
    void setMinOpenSegmentIdRecover(ProtoBuf::StateSetMinOpenSegmentId* state,
                                    EntryId entryId);
    bool verifyServerFailure(ServerId serverId);

  PRIVATE:

    /**
     * Defines methods and stores data to hintServerDown.
     */
    class HintServerDown {
        public:
            HintServerDown(CoordinatorServerManager &manager,
                           ServerId serverId)
                : manager(manager), serverId(serverId) {}
            bool execute();
            bool complete(EntryId entryId);
        private:
            /**
             * Reference to the instance of coordinator server manager
             * initializing this class.
             * Used to get access to CoordinatorService& service.
             */
            CoordinatorServerManager &manager;
            /**
             * ServerId of the server that is suspected to be down.
             */
            ServerId serverId;
            DISALLOW_COPY_AND_ASSIGN(HintServerDown);
    };

    /**
     * Defines methods and stores data to set minOpenSegmentId of server
     * with id serverId to segmentId.
     */
    class SetMinOpenSegmentId {
        public:
            SetMinOpenSegmentId(CoordinatorServerManager &manager,
                                ServerId serverId,
                                uint64_t segmentId)
                : manager(manager), serverId(serverId), segmentId(segmentId) {}
            void execute();
            void complete(EntryId entryId);
        private:
            /**
             * Reference to the instance of coordinator server manager
             * initializing this class.
             * Used to get access to CoordinatorService& service.
             */
            CoordinatorServerManager &manager;
            /**
             * ServerId of the server whose minOpenSegmentId will be set.
             */
            ServerId serverId;
            /**
             * The minOpenSegmentId to be set.
             */
            uint64_t segmentId;
            DISALLOW_COPY_AND_ASSIGN(SetMinOpenSegmentId);
    };

    /**
     * Reference to the coordinator service initializing this class.
     * Used to get access to the context, serverList and recoveryManager
     * in coordinator service.
     */
    CoordinatorService& service;

    /**
     * The id of the next replication group to be created. The replication
     * group is a set of backups that store all of the replicas of a segment.
     * NextReplicationId starts at 1 and is never reused.
     * Id 0 is reserved for nodes that do not belong to a replication group.
     */
    uint64_t nextReplicationId;

    /**
     * Used for testing only. If true, the HINT_SERVER_DOWN handler will
     * assume that the server has failed (rather than checking for itself).
     */
    bool forceServerDownForTesting;

    /**
     * Provides monitor-style protection for all operations in the
     * CoordinatorServerManger.
     * A Lock for this mutex must be held to read or modify any server
     * configuration.
     */
    mutable std::mutex mutex;
    typedef std::lock_guard<std::mutex> Lock;

    DISALLOW_COPY_AND_ASSIGN(CoordinatorServerManager);
};

} // namespace RAMCloud

#endif // RAMCLOUD_COORDINATORSERVERMANAGER_H
