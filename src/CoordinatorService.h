/* Copyright (c) 2009-2011 Stanford University
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

#ifndef RAMCLOUD_COORDINATORSERVICE_H
#define RAMCLOUD_COORDINATORSERVICE_H

#include "ServerList.pb.h"
#include "Tablets.pb.h"

#include "Common.h"
#include "ClientException.h"
#include "RawMetrics.h"
#include "Recovery.h"
#include "Rpc.h"
#include "ServerId.h"
#include "Service.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Serves RPCs for the cluster coordinator.
 */
class CoordinatorService : public Service {
  public:
    CoordinatorService();
    ~CoordinatorService();
    void dispatch(RpcOpcode opcode,
                  Rpc& rpc);

  PRIVATE:
    /**
     * The ping timeout used when the Coordinator verifies an incoming
     * hint server down message. Until we resolve the scheduler issues that we
     * have been seeing this timeout should be at least 250ms.
     */
    static const int TIMEOUT_USECS = 250 * 1000;

    // - rpc handlers -
    void createTable(const CreateTableRpc::Request& reqHdr,
                     CreateTableRpc::Response& respHdr,
                     Rpc& rpc);
    void dropTable(const DropTableRpc::Request& reqHdr,
                   DropTableRpc::Response& respHdr,
                   Rpc& rpc);
    void openTable(const OpenTableRpc::Request& reqHdr,
                   OpenTableRpc::Response& respHdr,
                   Rpc& rpc);
    void enlistServer(const EnlistServerRpc::Request& reqHdr,
                      EnlistServerRpc::Response& respHdr,
                      Rpc& rpc);
    void getServerList(const GetServerListRpc::Request& reqHdr,
                       GetServerListRpc::Response& respHdr,
                       Rpc& rpc);
    void getTabletMap(const GetTabletMapRpc::Request& reqHdr,
                      GetTabletMapRpc::Response& respHdr,
                      Rpc& rpc);
    void hintServerDown(const HintServerDownRpc::Request& reqHdr,
                        HintServerDownRpc::Response& respHdr,
                        Rpc& rpc);
    void tabletsRecovered(const TabletsRecoveredRpc::Request& reqHdr,
                          TabletsRecoveredRpc::Response& respHdr,
                          Rpc& rpc);
    void quiesce(const BackupQuiesceRpc::Request& reqHdr,
                 BackupQuiesceRpc::Response& respHdr,
                 Rpc& rpc);
    void setWill(const SetWillRpc::Request& reqHdr,
                 SetWillRpc::Response& respHdr,
                 Rpc& rpc);
    void reassignTabletOwnership(
                const ReassignTabletOwnershipRpc::Request& reqHdr,
                ReassignTabletOwnershipRpc::Response& respHdr,
                Rpc& rpc);
    void requestServerList(const RequestServerListRpc::Request& reqHdr,
                           RequestServerListRpc::Response& respHdr,
                           Rpc& rpc);
    // - helper methods -
    bool assignReplicationGroup(uint64_t replicationId,
                                const vector<ServerId>& replicationGroupIds);
    void createReplicationGroup();
    bool hintServerDown(ServerId serverId);
    void removeReplicationGroup(uint64_t replicationId);
    void sendMembershipUpdate(ProtoBuf::ServerList& update,
                              ServerId excludeServerId);
    void sendServerList(ServerId destination);
    void setMinOpenSegmentId(const SetMinOpenSegmentIdRpc::Request& reqHdr,
                             SetMinOpenSegmentIdRpc::Response& respHdr,
                             Rpc& rpc);
    bool setWill(ServerId masterId, Buffer& buffer,
                 uint32_t offset, uint32_t length);
    void startMasterRecovery(const CoordinatorServerList::Entry& serverEntry);
    bool verifyServerFailure(ServerId serverId);

    /**
     * List of all servers in the system. This structure is used to allocate
     * ServerIds as well as to keep track of any information we need to keep
     * for individual servers (e.g. ServiceLocator strings, Wills, etc).
     */
    CoordinatorServerList serverList;

    /**
     * What are the tablets, and who is the master for each.
     */
    ProtoBuf::Tablets tabletMap;

    typedef std::map<string, uint64_t> Tables;
    /**
     * Map from table name to table id.
     */
    Tables tables;

    /**
     * The id of the next table to be created.
     * These start at 0 and are never reused.
     */
    uint64_t nextTableId;

    /**
     * Used in #createTable() to assign new tables to masters.
     * If you take this modulo the number of entries in #masterList, you get
     * the index into #masterList of the master that should be assigned the
     * next table.
     */
    uint32_t nextTableMasterIdx;

    /**
     * The id of the next replication group to be created. The replication
     * group is a set of backups that store all of the replicas of a segment.
     * NextReplicationId starts at 1 and is never reused.
     * Id 0 is reserved for nodes that do not belong to a replication group.
     */
    uint64_t nextReplicationId;

    /// Used in unit testing.
    BaseRecovery* mockRecovery;

    /**
     * Used for testing only. If true, the HINT_SERVER_DOWN handler will
     * assume that the server has failed (rather than checking for itself).
     */
    bool forceServerDownForTesting;

    DISALLOW_COPY_AND_ASSIGN(CoordinatorService);
};

} // namespace RAMCloud

#endif // RAMCLOUD_COORDINATORSERVICE_H
