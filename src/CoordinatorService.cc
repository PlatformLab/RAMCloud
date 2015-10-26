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

#include <memory>
#include <string>
#include <list>

#include "BackupClient.h"
#include "CoordinatorService.h"
#include "MasterClient.h"
#include "ProtoBuf.h"
#include "Recovery.h"
#include "ShortMacros.h"
#include "ServerId.h"
#include "ServiceMask.h"

namespace RAMCloud {
bool CoordinatorService::forceSynchronousInit = false;

/*
 * Construct a CoordinatorService.
 *
 * \param context
 *      Overall information about the RAMCloud server.  The new service
 *      will be registered in this context.
 * \param deadServerTimeout
 *      Servers are presumed dead if they cannot respond to a ping request
 *      in this many milliseconds.
 * \param unitTesting
 *      False (typical usage) means we should start the various manager threads.
 *      True is used only during testing to not run various background tasks.
 */
CoordinatorService::CoordinatorService(Context* context,
                                       uint32_t deadServerTimeout,
                                       bool unitTesting,
                                       bool neverKill)
    : context(context)
    , serverList(context->coordinatorServerList)
    , deadServerTimeout(deadServerTimeout)
    , updateManager(context->externalStorage)
    , tableManager(context, &updateManager)
    , leaseAuthority(context)
    , runtimeOptions()
    , recoveryManager(context, tableManager, &runtimeOptions)
    , forceServerDownForTesting(false)
    , neverKill(neverKill)
    , initFinished(false)
    , backupConfig()
    , masterConfig()
{
    context->services[WireFormat::COORDINATOR_SERVICE] = this;
    context->recoveryManager = &recoveryManager;

    // Invoke the rest of initialization in a separate thread (except during
    // unit tests). This is needed because some of the recovery operations
    // carried out during initialization require the dispatch thread running
    // to be operating, and in normal operation the thread that calls this
    // constructor also acts as dispatch thread once the constructor returns.
    // Thus we can't perform recovery synchronously here.

    if (forceSynchronousInit) {
        init(this, unitTesting);
    } else {
        std::thread(init, this, unitTesting).detach();
    }
}

CoordinatorService::~CoordinatorService()
{
    context->services[WireFormat::COORDINATOR_SERVICE] = NULL;
    recoveryManager.halt();
}

/**
 * This method does most of the work of initializing a CoordinatorService.
 * It is invoked by the constructor and runs in a separate thread (see
 * explanation in the constructor).
 * \param service
 *      Coordinator service that is being constructed.
 * \param unitTesting
 *      False means don't start the various background tasks (this should always
 *      be true, except during unit tests)
 */
void
CoordinatorService::init(CoordinatorService* service,
        bool unitTesting)
{
    // This is the top-level method in a thread, so it must catch all
    // exceptions.
    try {
        // Recover state (and incomplete operations) from external storage.
        service->leaseAuthority.recover();
        uint64_t lastCompletedUpdate = service->updateManager.init();
        service->serverList->recover(lastCompletedUpdate);
        service->tableManager.recover(lastCompletedUpdate);
        service->updateManager.recoveryFinished();
        LOG(NOTICE, "Coordinator state has been recovered from external "
                "storage; starting service");

        // Don't enable server list updates until recovery is finished (before
        // this point the server list may not contain all information needed
        // for updating).
        service->serverList->startUpdater();

        if (!unitTesting) {
            service->leaseAuthority.startUpdaters();
            // When the recovery manager starts up below, it will resume
            // recovery for crashed nodes; it isn't safe to do that until
            // after the server list and table manager have recovered (e.g.
            // it will need accurate information about which tables are stored
            // on a crashed server).
            service->recoveryManager.start();
        }


        service->initFinished = true;
    } catch (std::exception& e) {
        LOG(ERROR, "%s", e.what());
        throw; // will likely call std::terminate()
    } catch (...) {
        LOG(ERROR, "unknown exception");
        throw; // will likely call std::terminate()
    }
}

// See Server::dispatch.
void
CoordinatorService::dispatch(WireFormat::Opcode opcode,
                             Rpc* rpc)
{
    if (!initFinished) {
        throw RetryException(HERE, 1000000, 2000000,
                "coordinator service not yet initialized");
    }
    switch (opcode) {
        case WireFormat::CoordSplitAndMigrateIndexlet::opcode:
            callHandler<WireFormat::CoordSplitAndMigrateIndexlet,
                        CoordinatorService,
                        &CoordinatorService::coordSplitAndMigrateIndexlet>(rpc);
            break;
        case WireFormat::CreateIndex::opcode:
            callHandler<WireFormat::CreateIndex, CoordinatorService,
                        &CoordinatorService::createIndex>(rpc);
            break;
        case WireFormat::CreateTable::opcode:
            callHandler<WireFormat::CreateTable, CoordinatorService,
                        &CoordinatorService::createTable>(rpc);
            break;
        case WireFormat::DropIndex::opcode:
            callHandler<WireFormat::DropIndex, CoordinatorService,
                        &CoordinatorService::dropIndex>(rpc);
            break;
        case WireFormat::DropTable::opcode:
            callHandler<WireFormat::DropTable, CoordinatorService,
                        &CoordinatorService::dropTable>(rpc);
            break;
        case WireFormat::EnlistServer::opcode:
            callHandler<WireFormat::EnlistServer, CoordinatorService,
                        &CoordinatorService::enlistServer>(rpc);
            break;
        case WireFormat::GetRuntimeOption::opcode:
            callHandler<WireFormat::GetRuntimeOption, CoordinatorService,
                        &CoordinatorService::getRuntimeOption>(rpc);
            break;
        case WireFormat::GetTableId::opcode:
            callHandler<WireFormat::GetTableId, CoordinatorService,
                        &CoordinatorService::getTableId>(rpc);
            break;
        case WireFormat::GetBackupConfig::opcode:
            callHandler<WireFormat::GetBackupConfig, CoordinatorService,
                        &CoordinatorService::getBackupConfig>(rpc);
            break;
        case WireFormat::GetLeaseInfo::opcode:
            callHandler<WireFormat::GetLeaseInfo, CoordinatorService,
                        &CoordinatorService::getLeaseInfo>(rpc);
            break;
        case WireFormat::GetMasterConfig::opcode:
            callHandler<WireFormat::GetMasterConfig, CoordinatorService,
                        &CoordinatorService::getMasterConfig>(rpc);
            break;
        case WireFormat::GetServerList::opcode:
            callHandler<WireFormat::GetServerList, CoordinatorService,
                        &CoordinatorService::getServerList>(rpc);
            break;
        case WireFormat::GetTableConfig::opcode:
            callHandler<WireFormat::GetTableConfig, CoordinatorService,
                        &CoordinatorService::getTableConfig>(rpc);
            break;
        case WireFormat::HintServerCrashed::opcode:
            callHandler<WireFormat::HintServerCrashed, CoordinatorService,
                        &CoordinatorService::hintServerCrashed>(rpc);
            break;
        case WireFormat::ReassignTabletOwnership::opcode:
            callHandler<WireFormat::ReassignTabletOwnership, CoordinatorService,
                        &CoordinatorService::reassignTabletOwnership>(rpc);
            break;
        case WireFormat::RecoveryMasterFinished::opcode:
            callHandler<WireFormat::RecoveryMasterFinished, CoordinatorService,
                        &CoordinatorService::recoveryMasterFinished>(rpc);
            break;
        case WireFormat::RenewLease::opcode:
            callHandler<WireFormat::RenewLease, CoordinatorService,
                        &CoordinatorService::renewLease>(rpc);
            break;
        case WireFormat::ServerControlAll::opcode:
            callHandler<WireFormat::ServerControlAll, CoordinatorService,
                        &CoordinatorService::serverControlAll>(rpc);
            break;
        case WireFormat::SetMasterRecoveryInfo::opcode:
            callHandler<WireFormat::SetMasterRecoveryInfo, CoordinatorService,
                        &CoordinatorService::setMasterRecoveryInfo>(rpc);
            break;
        case WireFormat::SetRuntimeOption::opcode:
            callHandler<WireFormat::SetRuntimeOption, CoordinatorService,
                        &CoordinatorService::setRuntimeOption>(rpc);
            break;
        case WireFormat::SplitTablet::opcode:
            callHandler<WireFormat::SplitTablet, CoordinatorService,
                        &CoordinatorService::splitTablet>(rpc);
            break;
        case WireFormat::VerifyMembership::opcode:
            callHandler<WireFormat::VerifyMembership, CoordinatorService,
                        &CoordinatorService::verifyMembership>(rpc);
            break;
        default:
            throw UnimplementedRequestError(HERE);
    }
}

/**
 * Get a reference to the runtimeOptions in the Coordinator
 */
RuntimeOptions *
CoordinatorService::getRuntimeOptionsFromCoordinator()
{
    return &runtimeOptions;
}

/*
 * Top-level server method to handle the COORD_SPLIT_AND_MIGRATE_INDEXLET
 * request.
 * \copydetails Service::ping
 */
void
CoordinatorService::coordSplitAndMigrateIndexlet(
        const WireFormat::CoordSplitAndMigrateIndexlet::Request* reqHdr,
        WireFormat::CoordSplitAndMigrateIndexlet::Response* respHdr,
        Rpc* rpc)
{
    const void* splitKey = rpc->requestPayload->getRange(
            sizeof(*reqHdr), reqHdr->splitKeyLength);

    if (splitKey == NULL) {
        respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
        rpc->sendReply();
        return;
    }

    tableManager.coordSplitAndMigrateIndexlet(
            ServerId(reqHdr->newOwnerId), reqHdr->tableId, reqHdr->indexId,
            splitKey, reqHdr->splitKeyLength);
}

/**
 * Top-level server method to handle the CREATE_INDEX request.
 * \copydetails Service::ping
 */
void
CoordinatorService::createIndex(
        const WireFormat::CreateIndex::Request* reqHdr,
        WireFormat::CreateIndex::Response* respHdr,
        Rpc* rpc)
{
    tableManager.createIndex(reqHdr->tableId, reqHdr->indexId,
            reqHdr->indexType, reqHdr->numIndexlets);
}

/**
 * Top-level server method to handle the CREATE_TABLE request.
 * \copydetails Service::ping
 */
void
CoordinatorService::createTable(
        const WireFormat::CreateTable::Request* reqHdr,
        WireFormat::CreateTable::Response* respHdr,
        Rpc* rpc)
{
    if (serverList->masterCount() == 0) {
        respHdr->common.status = STATUS_RETRY;
        return;
    }

    const char* name = getString(rpc->requestPayload, sizeof(*reqHdr),
                                 reqHdr->nameLength);
    uint32_t serverSpan = reqHdr->serverSpan;

    respHdr->tableId = tableManager.createTable(name, serverSpan);
}

/**
 * Top-level server method to handle the DROP_INDEX request.
 * \copydetails Service::ping
 */
void
CoordinatorService::dropIndex(
        const WireFormat::DropIndex::Request* reqHdr,
        WireFormat::DropIndex::Response* respHdr,
        Rpc* rpc)
{
    tableManager.dropIndex(reqHdr->tableId, reqHdr->indexId);
}

/**
 * Top-level server method to handle the DROP_TABLE request.
 * \copydetails Service::ping
 */
void
CoordinatorService::dropTable(
        const WireFormat::DropTable::Request* reqHdr,
        WireFormat::DropTable::Response* respHdr,
        Rpc* rpc)
{
    const char* name = getString(rpc->requestPayload, sizeof(*reqHdr),
                                 reqHdr->nameLength);
    tableManager.dropTable(name);
}

/**
 * Handle the ENLIST_SERVER RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::enlistServer(
        const WireFormat::EnlistServer::Request* reqHdr,
        WireFormat::EnlistServer::Response* respHdr,
        Rpc* rpc)
{
    ServerId replacesId = ServerId(reqHdr->replacesId);
    ServiceMask serviceMask = ServiceMask::deserialize(reqHdr->serviceMask);
    const uint32_t readSpeed = reqHdr->readSpeed;
    const char* serviceLocator = getString(rpc->requestPayload, sizeof(*reqHdr),
                                           reqHdr->serviceLocatorLength);

    // If the new server is replacing an existing one, we must start
    // crash recovery on the old server. Furthermore, we must initiate that
    // right away, so that existing servers will be notified of the crash
    // before finding out about the new server.
    if (serverList->isUp(replacesId)) {
        LOG(NOTICE, "enlisting server %s claims to replace server id "
            "%s, which is still in the server list; taking its word "
            "for it and assuming the old server has failed",
            serviceLocator, replacesId.toString().c_str());
        serverList->serverCrashed(replacesId);
    }

    ServerId newServerId = serverList->enlistServer(serviceMask,
                                                    reqHdr->preferredIndex,
                                                    readSpeed,
                                                    serviceLocator);
    respHdr->serverId = newServerId.getId();
}

/**
 * Handle the GET_BACKUP_CONFIG RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::getBackupConfig(
        const WireFormat::GetBackupConfig::Request* reqHdr,
        WireFormat::GetBackupConfig::Response* respHdr,
        Rpc* rpc)
{
    ProtoBuf::ServerConfig_Backup backupConfigBuf;
    backupConfig.serialize(backupConfigBuf);
    respHdr->backupConfigLength = ProtoBuf::serializeToResponse(
            rpc->replyPayload, &backupConfigBuf);
}

/**
 * Handle the GET_LEASE_INFO RPC.
 *
 * \copydetails Service::ping
 */
void
CoordinatorService::getLeaseInfo(
    const WireFormat::GetLeaseInfo::Request* reqHdr,
    WireFormat::GetLeaseInfo::Response* respHdr,
    Rpc* rpc)
{
    respHdr->lease = leaseAuthority.getLeaseInfo(reqHdr->leaseId);
}

/**
 * Handle the GET_MASTER_CONFIG RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::getMasterConfig(
        const WireFormat::GetMasterConfig::Request* reqHdr,
        WireFormat::GetMasterConfig::Response* respHdr,
        Rpc* rpc)
{
    ProtoBuf::ServerConfig_Master masterConfigBuf;
    masterConfig.serialize(masterConfigBuf);
    respHdr->masterConfigLength = ProtoBuf::serializeToResponse(
            rpc->replyPayload, &masterConfigBuf);
}

/**
 * Gets the value of a runtime option field that's been previously set
 * using CoordinatorClient::setRuntimeOption().
 * \copydetails Service::ping
 */
void
CoordinatorService::getRuntimeOption(
        const WireFormat::GetRuntimeOption::Request* reqHdr,
        WireFormat::GetRuntimeOption::Response* respHdr,
        Rpc* rpc)
{
    const char* option = getString(rpc->requestPayload, sizeof(*reqHdr),
                                   reqHdr->optionLength);
    try {
        std::string value = runtimeOptions.get(option);
        respHdr->valueLength = downCast<uint32_t>(value.size() + 1);
        rpc->replyPayload->append(value.c_str(), respHdr->valueLength);
    } catch (const std::out_of_range& e) {
        respHdr->common.status = STATUS_OBJECT_DOESNT_EXIST;
        return;
    }
}

/**
 * Handle the GET_SERVER_LIST RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::getServerList(
        const WireFormat::GetServerList::Request* reqHdr,
        WireFormat::GetServerList::Response* respHdr,
        Rpc* rpc)
{
    ServiceMask serviceMask = ServiceMask::deserialize(reqHdr->serviceMask);

    ProtoBuf::ServerList serialServerList;
    serverList->serialize(&serialServerList, serviceMask);

    respHdr->serverListLength =
        serializeToResponse(rpc->replyPayload, &serialServerList);
}

/**
 * Handle the GET_TABLE_CONFIG RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::getTableConfig(
        const WireFormat::GetTableConfig::Request* reqHdr,
        WireFormat::GetTableConfig::Response* respHdr,
        Rpc* rpc)
{
    ProtoBuf::TableConfig tableConfig;
    tableManager.serializeTableConfig(&tableConfig, reqHdr->tableId);
    respHdr->tableConfigLength = serializeToResponse(rpc->replyPayload,
                                                     &tableConfig);
}

/**
 * Top-level server method to handle the GET_TABLE_ID request.
 * \copydetails Service::ping
 */
void
CoordinatorService::getTableId(
        const WireFormat::GetTableId::Request* reqHdr,
        WireFormat::GetTableId::Response* respHdr,
        Rpc* rpc)
{
    const char* name = getString(rpc->requestPayload, sizeof(*reqHdr),
                                 reqHdr->nameLength);

    try {
        uint64_t tableId = tableManager.getTableId(name);
        respHdr->tableId = tableId;
    } catch (TableManager::NoSuchTable& e) {
        respHdr->common.status = STATUS_TABLE_DOESNT_EXIST;
        return;
    }
}

/**
 * Handle the ENLIST_SERVER RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::hintServerCrashed(
        const WireFormat::HintServerCrashed::Request* reqHdr,
        WireFormat::HintServerCrashed::Response* respHdr,
        Rpc* rpc)
{
    ServerId serverId(reqHdr->serverId);
    rpc->sendReply();
    // reqHdr, respHdr, and rpc are off-limits now

    if (!serverList->contains(serverId) ||
            (*serverList)[serverId].status != ServerStatus::UP) {
        LOG(NOTICE, "Spurious crash report on unknown server id %s",
                     serverId.toString().c_str());
        return;
     }

     LOG(NOTICE, "Checking server id %s (%s)",
         serverId.toString().c_str(), serverList->getLocator(serverId).c_str());
     if (!verifyServerFailure(serverId))
         return;

     LOG(NOTICE, "Server id %s has crashed, notifying the cluster and "
         "starting recovery", serverId.toString().c_str());

     serverList->serverCrashed(serverId);
}

/**
 * Handle the REASSIGN_TABLET_OWNER RPC.
 *
 * \copydetails Service::ping
 */
void
CoordinatorService::reassignTabletOwnership(
        const WireFormat::ReassignTabletOwnership::Request* reqHdr,
        WireFormat::ReassignTabletOwnership::Response* respHdr,
        Rpc* rpc)
{
    ServerId newOwner(reqHdr->newOwnerId);
    uint64_t tableId = reqHdr->tableId;
    uint64_t startKeyHash = reqHdr->firstKeyHash;
    uint64_t endKeyHash = reqHdr->lastKeyHash;

    if (!serverList->isUp(newOwner)) {
        LOG(WARNING, "Cannot reassign tablet [0x%lx,0x%lx] in tableId %lu "
                     "to %s: server not up",
                      startKeyHash, endKeyHash, tableId,
                      newOwner.toString().c_str());
        respHdr->common.status = STATUS_SERVER_NOT_UP;
        return;
    }

    try {
        tableManager.reassignTabletOwnership(
                newOwner, tableId, startKeyHash, endKeyHash,
                reqHdr->ctimeSegmentId, reqHdr->ctimeSegmentOffset);
    } catch (const TableManager::NoSuchTablet& e) {
        LOG(WARNING, "Could not reassign tablet [0x%lx,0x%lx] in tableId %lu: "
            "tablet not found",
            startKeyHash, endKeyHash, tableId);
        respHdr->common.status = STATUS_TABLE_DOESNT_EXIST;
    }
}

/**
 * Handle the TABLETS_RECOVERED RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::recoveryMasterFinished(
        const WireFormat::RecoveryMasterFinished::Request* reqHdr,
        WireFormat::RecoveryMasterFinished::Response* respHdr,
        Rpc* rpc)
{
    ProtoBuf::RecoveryPartition recoveryPartition;
    ProtoBuf::parseFromRequest(rpc->requestPayload,
                               sizeof32(*reqHdr),
                               reqHdr->tabletsLength, &recoveryPartition);

    ServerId serverId = ServerId(reqHdr->recoveryMasterId);
    respHdr->cancelRecovery =
        recoveryManager.recoveryMasterFinished(reqHdr->recoveryId,
                                               serverId,
                                               recoveryPartition,
                                               reqHdr->successful);
}

/**
 * Handle the RENEW_LEASE RPC.
 *
 * \copydetails Service::ping
 */
void
CoordinatorService::renewLease(
    const WireFormat::RenewLease::Request* reqHdr,
    WireFormat::RenewLease::Response* respHdr,
    Rpc* rpc)
{
    respHdr->lease = leaseAuthority.renewLease(reqHdr->leaseId);
}

/**
 * Send ServerControl RPCs to all servers in the ServerList.
 *
 * \copydetails Service::ping
 */
void
CoordinatorService::serverControlAll(
        const WireFormat::ServerControlAll::Request* reqHdr,
        WireFormat::ServerControlAll::Response* respHdr,
        Rpc* rpc)
{
    respHdr->serverCount = 0;
    respHdr->respCount = 0;
    respHdr->totalRespLength = 0;
    uint32_t reqOffset = sizeof32(*reqHdr);
    const void* inputData = rpc->requestPayload->getRange(reqOffset,
            reqHdr->inputLength);   // inputData may be NULL.

    std::list<ServerControlRpcContainer> rpcs;
    ServerId nextServerId;
    bool end = false;

    // If all RPCs complete and ServerId list has reached the end, we are done.
    while (rpcs.size() > 0 || !end) {
        // Check all outstanding RPCs.
        checkServerControlRpcs(&rpcs, respHdr, rpc);

        // If there are still servers left, send out 1 RPC
        if (!end) {
            nextServerId = serverList->nextServer(nextServerId,
                                                  {WireFormat::PING_SERVICE},
                                                  &end,
                                                  false);
            if (!end && nextServerId.isValid()) {
                rpcs.emplace_back(context, nextServerId, reqHdr->controlOp,
                                  inputData, reqHdr->inputLength);
            }
        }
    }
    respHdr->common.status = STATUS_OK;
}

/**
 * Updates the master recovery info for a particular server. This metadata
 * is stored in the server list until a master crashes at which point it is
 * used in the recovery of the master. Currently, masters store log metadata
 * there needed to invalidate replicas which might have become stale due to
 * backup crashes. Only the master recovery portion of the coordinator needs
 * to understand the contents.
 *
 * \copydetails Service::ping
 */
void
CoordinatorService::setMasterRecoveryInfo(
        const WireFormat::SetMasterRecoveryInfo::Request* reqHdr,
        WireFormat::SetMasterRecoveryInfo::Response* respHdr,
        Rpc* rpc)
{
    ServerId serverId(reqHdr->serverId);
    ProtoBuf::MasterRecoveryInfo recoveryInfo;
    ProtoBuf::parseFromRequest(rpc->requestPayload,
                               sizeof32(*reqHdr),
                               reqHdr->infoLength, &recoveryInfo);

    LOG(DEBUG, "setMasterRecoveryInfo for server %s to %s",
        serverId.toString().c_str(), recoveryInfo.ShortDebugString().c_str());

    if (!serverList->setMasterRecoveryInfo(serverId, &recoveryInfo)) {
        LOG(WARNING, "setMasterRecoveryInfo server doesn't exist: %s",
            serverId.toString().c_str());
        respHdr->common.status = STATUS_SERVER_NOT_UP;
        return;
    }
}

/**
 * Sets a runtime option field on the coordinator to the indicated value.
 * See CoordinatorClient::setRuntimeOption() for details.
 *
 * \copydetails Service::ping
 */
void
CoordinatorService::setRuntimeOption(
        const WireFormat::SetRuntimeOption::Request* reqHdr,
        WireFormat::SetRuntimeOption::Response* respHdr,
        Rpc* rpc)
{
    const char* option = getString(rpc->requestPayload, sizeof(*reqHdr),
                                   reqHdr->optionLength);
    const char* value = getString(rpc->requestPayload,
                                  sizeof32(*reqHdr) + reqHdr->optionLength,
                                  reqHdr->valueLength);
    try {
        runtimeOptions.set(option, value);
    } catch (const std::out_of_range& e) {
        respHdr->common.status = STATUS_OBJECT_DOESNT_EXIST;
    }
}

/**
 * Top-level server method to handle the SPLIT_TABLET request.
 * \copydetails Service::ping
 */
void
CoordinatorService::splitTablet(
        const WireFormat::SplitTablet::Request* reqHdr,
        WireFormat::SplitTablet::Response* respHdr,
        Rpc* rpc)
{
    // Check that the tablet with the described key ranges exists.
    // If the tablet exists, adjust its lastKeyHash so it becomes the tablet
    // for the first part after the split and also copy the tablet and use
    // the copy for the second part after the split.
    const char* name = getString(rpc->requestPayload, sizeof(*reqHdr),
                                 reqHdr->nameLength);
    try {
        tableManager.splitTablet(
                name, reqHdr->splitKeyHash);
        LOG(NOTICE,
            "In table '%s' I split the tablet at key %lu",
            name, reqHdr->splitKeyHash);
    } catch (const TableManager::NoSuchTablet& e) {
        respHdr->common.status = STATUS_TABLET_DOESNT_EXIST;
        return;
    } catch (const TableManager::BadSplit& e) {
        respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
        return;
    } catch (TableManager::NoSuchTable& e) {
        respHdr->common.status = STATUS_TABLE_DOESNT_EXIST;
        return;
    }
}

/**
 * Check to see whether a given server is still considered an active member
 * of the cluster; if not, return STATUS_CALLER_NOT_IN_CLUSTER status.
 *
 * \copydetails Service::ping
 */
void
CoordinatorService::verifyMembership(
        const WireFormat::VerifyMembership::Request* reqHdr,
        WireFormat::VerifyMembership::Response* respHdr,
        Rpc* rpc)
{
    ServerId serverId(reqHdr->serverId);
    if (!serverList->isUp(serverId)) {
        respHdr->common.status = STATUS_CALLER_NOT_IN_CLUSTER;
        LOG(WARNING, "Membership verification failed for %s",
            serverList->toString(serverId).c_str());
    } else {
        LOG(NOTICE, "Membership verification succeeded for %s",
            serverList->toString(serverId).c_str());
    }
}

/**
 * Checks all outstanding ServerControlRpcs in rpcs and appends the results to
 * the replyPayload of rpc if isReady.  Used in serverControlAll.
 *
 * \param rpcs
 *      List of ServerControlRpcContainers to check and process.
 * \param respHdr
 *      Various fields in this header may be updated after the call.
 * \param rpc
 *      New response data may be appended to the replyPayload after the call.
 */
void
CoordinatorService::checkServerControlRpcs(
        std::list<ServerControlRpcContainer>* rpcs,
        WireFormat::ServerControlAll::Response* respHdr,
        Rpc* rpc)
{
    // Check all outstanding RPCs.
    std::list<ServerControlRpcContainer>::iterator it = rpcs->begin();
    while (it != rpcs->end()) {
        ServerControlRpcContainer* controlRpc = &(*it);

        // Rule 1: Skip to next rpc if not ready.
        if (!controlRpc->rpc.isReady()) {
            it++;
            continue;
        }

        // Rule 2: Try to wait on the rpc. If the server responded, we can
        // process the rpc.
        if (controlRpc->rpc.waitRaw()) {
            // if the response RPC fits in the response, append the response.
            uint32_t bufferSize = controlRpc->buffer.size();
            if (Transport::MAX_RPC_LEN - rpc->replyPayload->size() >=
                    bufferSize) {
                respHdr->respCount++;
                respHdr->totalRespLength += bufferSize;
                rpc->replyPayload->appendCopy(
                        controlRpc->buffer.getRange(0, bufferSize), bufferSize);
            }
            respHdr->serverCount++;
        }

        // Destroy object.
        it = rpcs->erase(it);
    }
}

/**
 * Investigate \a serverId and make a verdict about its whether it is alive.
 *
 * \param serverId
 *      Server to investigate.
 * \return
 *      True if the server is dead, false if it is alive.
 * \throw Exception
 *      If \a serverId is not in #serverList.
 */
bool
CoordinatorService::verifyServerFailure(ServerId serverId) {
    // Skip the real ping if this is from a unit test
    if (forceServerDownForTesting)
        return true;
    if (neverKill)
        return false;

    const string& serviceLocator = serverList->getLocator(serverId);
    PingRpc pingRpc(context, serverId, ServerId());

    if (pingRpc.wait(deadServerTimeout * 1000 * 1000)) {
        LOG(NOTICE, "False positive for server id %s (\"%s\")",
                    serverId.toString().c_str(), serviceLocator.c_str());
        return false;
    }
    LOG(NOTICE, "Verified host failure: id %s (\"%s\")",
        serverId.toString().c_str(), serviceLocator.c_str());
    return true;
}

} // namespace RAMCloud
