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

#include "Common.h"
#include "CoordinatorServerList.h"
#include "ServerId.h"

namespace RAMCloud {

class CoordinatorService;

/**
 * Handles all server configuration details on behalf of the coordinator.
 * Provides an interface to the coordinator to service rpcs that modify
 * server configuration.
 */
class CoordinatorServerManager {
  public:
    explicit CoordinatorServerManager(CoordinatorService& coordinatorService);
    ~CoordinatorServerManager();

    /**
     * The ping timeout used when the Coordinator verifies an incoming
     * hint server down message. Until we resolve the scheduler issues that we
     * have been seeing this timeout should be at least 250ms.
     */
    static const int TIMEOUT_USECS = 250 * 1000;

    bool assignReplicationGroup(uint64_t replicationId,
                                const vector<ServerId>& replicationGroupIds);
    void createReplicationGroup();
    ServerId enlistServerStart(ServerId replacesId,
                               Tub<CoordinatorServerList::Entry>* replacedEntry,
                               ServiceMask serviceMask,
                               const uint32_t readSpeed,
                               const uint32_t writeSpeed,
                               const char* serviceLocator,
                               ProtoBuf::ServerList* serverListUpdate);
    void enlistServerComplete(Tub<CoordinatorServerList::Entry>* replacedEntry,
                              ServerId newServerId,
                              ProtoBuf::ServerList* serverListUpdate);
    ProtoBuf::ServerList getServerList(ServiceMask serviceMask);
    bool hintServerDown(ServerId serverId);
    void removeReplicationGroup(uint64_t groupId);
    void sendServerList(ServerId serverId);
    void setMinOpenSegmentId(ServerId serverId, uint64_t segmentId);
    bool setWill(ServerId masterId, Buffer& buffer,
                 uint32_t offset, uint32_t length);
    bool verifyServerFailure(ServerId serverId);

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

    DISALLOW_COPY_AND_ASSIGN(CoordinatorServerManager);
};

} // namespace RAMCloud

#endif // RAMCLOUD_COORDINATORSERVERMANAGER_H
