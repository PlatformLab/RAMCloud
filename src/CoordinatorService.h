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

#ifndef RAMCLOUD_COORDINATORSERVICE_H
#define RAMCLOUD_COORDINATORSERVICE_H

#include "ServerList.pb.h"
#include "Tablets.pb.h"

#include "Common.h"
#include "ClientException.h"
#include "MasterRecoveryManager.h"
#include "RawMetrics.h"
#include "Recovery.h"
#include "Rpc.h"
#include "RuntimeOptions.h"
#include "Service.h"
#include "TabletMap.h"
#include "TransportManager.h"
#include "CoordinatorServerManager.h"

namespace RAMCloud {

/**
 * Serves RPCs for the cluster coordinator.
 */
class CoordinatorService : public Service {
  public:
    explicit CoordinatorService(Context& context);
    ~CoordinatorService();
    void dispatch(RpcOpcode opcode,
                  Rpc& rpc);

  PRIVATE:
    // - rpc handlers -
    void createTable(const CreateTableRpc::Request& reqHdr,
                     CreateTableRpc::Response& respHdr,
                     Rpc& rpc);
    void dropTable(const DropTableRpc::Request& reqHdr,
                   DropTableRpc::Response& respHdr,
                   Rpc& rpc);
    void splitTablet(const SplitTabletRpc::Request& reqHdr,
                   SplitTabletRpc::Response& respHdr,
                   Rpc& rpc);
    void getTableId(const GetTableIdRpc::Request& reqHdr,
                    GetTableIdRpc::Response& respHdr,
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
    void recoveryMasterFinished(
            const RecoveryMasterFinishedRpc::Request& reqHdr,
            RecoveryMasterFinishedRpc::Response& respHdr,
            Rpc& rpc);
    void quiesce(const BackupQuiesceRpc::Request& reqHdr,
                 BackupQuiesceRpc::Response& respHdr,
                 Rpc& rpc);
    void reassignTabletOwnership(
            const ReassignTabletOwnershipRpc::Request& reqHdr,
            ReassignTabletOwnershipRpc::Response& respHdr,
            Rpc& rpc);
    void sendServerList(const SendServerListRpc::Request& reqHdr,
                        SendServerListRpc::Response& respHdr,
                        Rpc& rpc);
    void setRuntimeOption(const SetRuntimeOptionRpc::Request& reqHdr,
                          SetRuntimeOptionRpc::Response& respHdr,
                          Rpc& rpc);
    void setMinOpenSegmentId(const SetMinOpenSegmentIdRpc::Request& reqHdr,
                             SetMinOpenSegmentIdRpc::Response& respHdr,
                             Rpc& rpc);

    /**
     * Shared RAMCloud information.
     */
    Context& context;

  public:
    /**
     * List of all servers in the system. This structure is used to allocate
     * ServerIds as well as to keep track of any information we need to keep
     * for individual servers (e.g. ServiceLocator strings, Wills, etc).
     */
    CoordinatorServerList& serverList;

  PRIVATE:
    /**
     * What are the tablets, and who is the master for each.
     */
    TabletMap tabletMap;

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
     * Contains coordinator configuration options which can be modified while
     * the cluster is running. Currently mostly used for setting debugging
     * or testing parameters.
     */
    RuntimeOptions runtimeOptions;

    /**
     * Handles all master recovery details on behalf of the coordinator.
     */
    MasterRecoveryManager recoveryManager;

    /**
     * Handles all server configuration details on behalf of the coordinator.
     */
    CoordinatorServerManager serverManager;

    friend class CoordinatorServerManager;
    DISALLOW_COPY_AND_ASSIGN(CoordinatorService);
};

} // namespace RAMCloud

#endif // RAMCLOUD_COORDINATORSERVICE_H
