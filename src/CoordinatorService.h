/* Copyright (c) 2009-2015 Stanford University
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
#include "TableConfig.pb.h"

#include "Common.h"
#include "ClientException.h"
#include "ClientLeaseAuthority.h"
#include "CoordinatorServerList.h"
#include "CoordinatorUpdateManager.h"
#include "MasterRecoveryManager.h"
#include "PingClient.h"
#include "RawMetrics.h"
#include "Recovery.h"
#include "RuntimeOptions.h"
#include "Service.h"
#include "TableManager.h"
#include "TransportManager.h"
#include "ServerConfig.h"

namespace RAMCloud {

/**
 * Serves RPCs for the cluster coordinator.
 */
class CoordinatorService : public Service {
  public:
    explicit CoordinatorService(Context* context,
                                uint32_t deadServerTimeout,
                                bool unitTesting = false,
                                bool neverKill = false);
    ~CoordinatorService();
    void dispatch(WireFormat::Opcode opcode,
            Rpc* rpc);
    RuntimeOptions *getRuntimeOptionsFromCoordinator();

  PRIVATE:
    // - rpc handlers -
    void coordSplitAndMigrateIndexlet(
            const WireFormat::CoordSplitAndMigrateIndexlet::Request* reqHdr,
            WireFormat::CoordSplitAndMigrateIndexlet::Response* respHdr,
            Rpc* rpc);
    void createIndex(const WireFormat::CreateIndex::Request* reqHdr,
            WireFormat::CreateIndex::Response* respHdr,
            Rpc* rpc);
    void createTable(const WireFormat::CreateTable::Request* reqHdr,
            WireFormat::CreateTable::Response* respHdr,
            Rpc* rpc);
    void dropIndex(const WireFormat::DropIndex::Request* reqHdr,
            WireFormat::DropIndex::Response* respHdr,
            Rpc* rpc);
    void dropTable(const WireFormat::DropTable::Request* reqHdr,
            WireFormat::DropTable::Response* respHdr,
            Rpc* rpc);
    void enlistServer(const WireFormat::EnlistServer::Request* reqHdr,
            WireFormat::EnlistServer::Response* respHdr,
            Rpc* rpc);
    void getBackupConfig(const WireFormat::GetBackupConfig::Request* reqHdr,
            WireFormat::GetBackupConfig::Response* respHdr,
            Rpc* rpc);
    void getLeaseInfo(const WireFormat::GetLeaseInfo::Request* reqHdr,
                      WireFormat::GetLeaseInfo::Response* respHdr,
                      Rpc* rpc);
    void getMasterConfig(const WireFormat::GetMasterConfig::Request* reqHdr,
            WireFormat::GetMasterConfig::Response* respHdr,
            Rpc* rpc);
    void getRuntimeOption(const WireFormat::GetRuntimeOption::Request* reqHdr,
            WireFormat::GetRuntimeOption::Response* respHdr,
            Rpc* rpc);
    void getServerList(const WireFormat::GetServerList::Request* reqHdr,
            WireFormat::GetServerList::Response* respHdr,
            Rpc* rpc);
    void getTableConfig(const WireFormat::GetTableConfig::Request* reqHdr,
            WireFormat::GetTableConfig::Response* respHdr,
            Rpc* rpc);
    void getTableId(const WireFormat::GetTableId::Request* reqHdr,
            WireFormat::GetTableId::Response* respHdr,
            Rpc* rpc);
    void hintServerCrashed(const WireFormat::HintServerCrashed::Request* reqHdr,
            WireFormat::HintServerCrashed::Response* respHdr,
            Rpc* rpc);
    void reassignTabletOwnership(
            const WireFormat::ReassignTabletOwnership::Request* reqHdr,
            WireFormat::ReassignTabletOwnership::Response* respHdr,
            Rpc* rpc);
    void recoveryMasterFinished(
            const WireFormat::RecoveryMasterFinished::Request* reqHdr,
            WireFormat::RecoveryMasterFinished::Response* respHdr,
            Rpc* rpc);
    void renewLease(const WireFormat::RenewLease::Request* reqHdr,
                    WireFormat::RenewLease::Response* respHdr,
                    Rpc* rpc);
    void serverControlAll(const WireFormat::ServerControlAll::Request* reqHdr,
            WireFormat::ServerControlAll::Response* respHdr,
            Rpc* rpc);
    void setMasterRecoveryInfo(
            const WireFormat::SetMasterRecoveryInfo::Request* reqHdr,
            WireFormat::SetMasterRecoveryInfo::Response* respHdr,
            Rpc* rpc);
    void setRuntimeOption(const WireFormat::SetRuntimeOption::Request* reqHdr,
            WireFormat::SetRuntimeOption::Response* respHdr,
            Rpc* rpc);
    void splitTablet(const WireFormat::SplitTablet::Request* reqHdr,
            WireFormat::SplitTablet::Response* respHdr,
            Rpc* rpc);
    void verifyMembership(
            const WireFormat::VerifyMembership::Request* reqHdr,
            WireFormat::VerifyMembership::Response* respHdr,
            Rpc* rpc);

    // - helper methods -

    /// Associates an allocated buffer to a ServerControlRpc.  Used internally
    /// in serverControlAll and checkServerControlRpcs.
    struct ServerControlRpcContainer {
        Buffer buffer;
        ServerControlRpc rpc;

        ServerControlRpcContainer(Context* context, ServerId serverId,
                WireFormat::ControlOp controlOp, const void* inputData = NULL,
                uint32_t inputLength = 0)
            : buffer()
            , rpc(context, serverId, controlOp, inputData, inputLength, &buffer)
        {}
    };

    static void init(CoordinatorService* service, bool unitTesting);
    void checkServerControlRpcs(std::list<ServerControlRpcContainer>* rpcs,
            WireFormat::ServerControlAll::Response* respHdr,
            Rpc* rpc);
    bool verifyServerFailure(ServerId serverId);

    /**
     * Shared RAMCloud information.
     */
    Context* context;

  public:
    /**
     * List of all servers in the system. This structure is used to allocate
     * ServerIds as well as to keep track of any information we need to keep
     * for individual servers (e.g. ServiceLocator strings, Wills, etc).
     */
    CoordinatorServerList* serverList;

    /**
     * The ping timeout, in milliseconds, used when the Coordinator verifies an
     * incoming hint server crashed message.
     * Until we resolve the scheduler issues that we have been seeing,
     * this timeout should be at least 250ms.
     */
    uint32_t deadServerTimeout;

    /**
     * Keeps track of incomplete operations, for use in recovery by
     * our successor if we crash.
     */
    CoordinatorUpdateManager updateManager;

    /**
     * Manages the tables and constituting tablets information on Coordinator.
     */
    TableManager tableManager;

    /**
     * Manages all client lease and serves requests for new leases and checks
     * for lease validity.
     */
    ClientLeaseAuthority leaseAuthority;

  PRIVATE:
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
     * Used for testing only. If true, the HINT_SERVER_CRASHED handler will
     * assume that the server has failed (rather than checking for itself).
     */
    bool forceServerDownForTesting;

    /**
     * Used for debugging. If true, this coordinator will never kill a master.
     */
    bool neverKill;

    /**
     * True means that the init method has completed its initialization.
     */
    bool initFinished;

    /**
     * Used by unit tests to force synchronous completion of initialization.
     */
    static bool forceSynchronousInit;

    /**
     * Default backup configuration that new backups entering the cluster should
     * adopt.  Used by getBackupConfig.
     */
    ServerConfig::Backup backupConfig;

    /**
     * Default master configuration that new masters entering the cluster should
     * adopt.  Used by getMasterConfig.
     */
    ServerConfig::Master masterConfig;

    friend class CoordinatorServiceRecovery;
    friend class CoordinatorServerList;
    friend class MockCluster;

    DISALLOW_COPY_AND_ASSIGN(CoordinatorService);
};

} // namespace RAMCloud

#endif // RAMCLOUD_COORDINATORSERVICE_H
