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

#include <Client/Client.h>

#include "ServerList.pb.h"
#include "Tablets.pb.h"

#include "Common.h"
#include "ClientException.h"
#include "CoordinatorServerManager.h"
#include "CoordinatorServiceRecovery.h"
#include "LogCabinHelper.h"
#include "MasterRecoveryManager.h"
#include "RawMetrics.h"
#include "Recovery.h"
#include "RuntimeOptions.h"
#include "Service.h"
#include "TabletMap.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Serves RPCs for the cluster coordinator.
 */
class CoordinatorService : public Service {
  public:
    explicit CoordinatorService(Context* context,
                                uint32_t deadServerTimeout,
                                string LogCabinLocator = "testing");
    ~CoordinatorService();
    void dispatch(WireFormat::Opcode opcode,
                  Rpc& rpc);

  PRIVATE:
    // - rpc handlers -
    void createTable(const WireFormat::CreateTable::Request& reqHdr,
                     WireFormat::CreateTable::Response& respHdr,
                     Rpc& rpc);
    void dropTable(const WireFormat::DropTable::Request& reqHdr,
                   WireFormat::DropTable::Response& respHdr,
                   Rpc& rpc);
    void splitTablet(const WireFormat::SplitTablet::Request& reqHdr,
                   WireFormat::SplitTablet::Response& respHdr,
                   Rpc& rpc);
    void getTableId(const WireFormat::GetTableId::Request& reqHdr,
                    WireFormat::GetTableId::Response& respHdr,
                    Rpc& rpc);
    void enlistServer(const WireFormat::EnlistServer::Request& reqHdr,
                      WireFormat::EnlistServer::Response& respHdr,
                      Rpc& rpc);
    void getServerList(const WireFormat::GetServerList::Request& reqHdr,
                       WireFormat::GetServerList::Response& respHdr,
                       Rpc& rpc);
    void getTabletMap(const WireFormat::GetTabletMap::Request& reqHdr,
                      WireFormat::GetTabletMap::Response& respHdr,
                      Rpc& rpc);
    void hintServerDown(const WireFormat::HintServerDown::Request& reqHdr,
                        WireFormat::HintServerDown::Response& respHdr,
                        Rpc& rpc);
    void recoveryMasterFinished(
            const WireFormat::RecoveryMasterFinished::Request& reqHdr,
            WireFormat::RecoveryMasterFinished::Response& respHdr,
            Rpc& rpc);
    void quiesce(const WireFormat::BackupQuiesce::Request& reqHdr,
                 WireFormat::BackupQuiesce::Response& respHdr,
                 Rpc& rpc);
    void reassignTabletOwnership(
            const WireFormat::ReassignTabletOwnership::Request& reqHdr,
            WireFormat::ReassignTabletOwnership::Response& respHdr,
            Rpc& rpc);
    void setRuntimeOption(const WireFormat::SetRuntimeOption::Request& reqHdr,
                          WireFormat::SetRuntimeOption::Response& respHdr,
                          Rpc& rpc);
    void setMasterRecoveryInfo(
        const WireFormat::SetMasterRecoveryInfo::Request& reqHdr,
        WireFormat::SetMasterRecoveryInfo::Response& respHdr,
        Rpc& rpc);

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
    CoordinatorServerList& serverList;

    /**
     * The ping timeout, in milliseconds, used when the Coordinator verifies an
     * incoming hint server down message. Until we resolve the scheduler issues
     * that we have been seeing this timeout should be at least 250ms.
     */
    const uint32_t deadServerTimeout;

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

    /**
     * Handles recovery of a coordinator.
     */
    CoordinatorServiceRecovery coordinatorRecovery;

    /**
     * Handle to the cluster of LogCabin which provides reliable, consistent
     * storage.
     */
    Tub<LogCabin::Client::Cluster> logCabinCluster;

    /**
     * Handle to the log interface provided by LogCabin.
     */
    Tub<LogCabin::Client::Log> logCabinLog;

    /**
     * Handle to a helper class that provides higher level abstractions
     * to interact with LogCabin.
     */
    Tub<LogCabinHelper> logCabinHelper;

    friend class CoordinatorServerManager;
    friend class CoordinatorServiceRecovery;

    DISALLOW_COPY_AND_ASSIGN(CoordinatorService);
};

} // namespace RAMCloud

#endif // RAMCLOUD_COORDINATORSERVICE_H
