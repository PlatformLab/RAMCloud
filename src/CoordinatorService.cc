/* Copyright (c) 2009-2013 Stanford University
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

#include "BackupClient.h"
#include "CoordinatorService.h"
#include "MasterClient.h"
#include "ProtoBuf.h"
#include "Recovery.h"
#include "ShortMacros.h"
#include "ServerId.h"
#include "ServiceMask.h"
#include "PingClient.h"

namespace RAMCloud {
bool CoordinatorService::forceSynchronousInit = false;

/*
 * Construct a CoordinatorService.
 *
 * \param context
 *      Overall information about the RAMCloud server. A pointer to this
 *      object will be stored at context->coordinatorService.
 * \param deadServerTimeout
 *      Servers are presumed dead if they cannot respond to a ping request
 *      in this many milliseconds.
 * \param startRecoveryManager
 *      True means we should start the thread that manages crash recovery.
 *      False is typically used only during testing.
 */
CoordinatorService::CoordinatorService(Context* context,
                                       uint32_t deadServerTimeout,
                                       bool startRecoveryManager,
                                       uint32_t maxThreads)
    : context(context)
    , serverList(context->coordinatorServerList)
    , deadServerTimeout(deadServerTimeout)
    , updateManager(context->externalStorage)
    , tableManager(context, &updateManager)
    , runtimeOptions()
    , recoveryManager(context, tableManager, &runtimeOptions)
    , threadLimit(maxThreads)
    , forceServerDownForTesting(false)
    , initFinished(false)
{
    context->recoveryManager = &recoveryManager;
    context->coordinatorService = this;

    // Invoke the rest of initialization in a separate thread (except during
    // unit tests). This is needed because some of the recovery operations
    // carried out during initialization require the dispatch thread running
    // to be operating, and in normal operation the thread that calls this
    // constructor also acts as dispatch thread once the constructor returns.
    // Thus we can't perform recovery synchronously here.

    if (forceSynchronousInit) {
        init(this, startRecoveryManager);
    } else {
        std::thread(init, this, startRecoveryManager).detach();
    }
}

CoordinatorService::~CoordinatorService()
{
    recoveryManager.halt();
}

/**
 * This method does most of the work of initializing a CoordinatorService.
 * It is invoked by the constructor and runs in a separate thread (see
 * explanation in the constructor).
 * \param service
 *      Coordinator service that is being constructed.
 * \param startRecoveryManager
 *      True means start the thread that handles master recoveries (this
 *      should always be true, except during unit tests)
 */
void
CoordinatorService::init(CoordinatorService* service,
        bool startRecoveryManager)
{
    // This is the top-level method in a thread, so it must catch all
    // exceptions.
    try {
        // Recover state (and incomplete operations) from external storage.
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

        // When the recovery manager starts up below, it will resume
        // recovery for crashed nodes; it isn't safe to do that until
        // after the server list and table manager have recovered (e.g.
        // it will need accurate information about which tables are stored on
        // a crashed server).
        if (startRecoveryManager)
            service->recoveryManager.start();

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
        case WireFormat::CreateTable::opcode:
            callHandler<WireFormat::CreateTable, CoordinatorService,
                        &CoordinatorService::createTable>(rpc);
            break;
        case WireFormat::DropTable::opcode:
            callHandler<WireFormat::DropTable, CoordinatorService,
                        &CoordinatorService::dropTable>(rpc);
            break;
        case WireFormat::GetRuntimeOption::opcode:
            callHandler<WireFormat::GetRuntimeOption, CoordinatorService,
                        &CoordinatorService::getRuntimeOption>(rpc);
            break;
        case WireFormat::GetTableId::opcode:
            callHandler<WireFormat::GetTableId, CoordinatorService,
                        &CoordinatorService::getTableId>(rpc);
            break;
        case WireFormat::EnlistServer::opcode:
            callHandler<WireFormat::EnlistServer, CoordinatorService,
                        &CoordinatorService::enlistServer>(rpc);
            break;
        case WireFormat::GetServerList::opcode:
            callHandler<WireFormat::GetServerList, CoordinatorService,
                        &CoordinatorService::getServerList>(rpc);
            break;
        case WireFormat::GetTabletMap::opcode:
            callHandler<WireFormat::GetTabletMap, CoordinatorService,
                        &CoordinatorService::getTabletMap>(rpc);
            break;
        case WireFormat::HintServerCrashed::opcode:
            callHandler<WireFormat::HintServerCrashed, CoordinatorService,
                        &CoordinatorService::hintServerCrashed>(rpc);
            break;
        case WireFormat::RecoveryMasterFinished::opcode:
            callHandler<WireFormat::RecoveryMasterFinished, CoordinatorService,
                        &CoordinatorService::recoveryMasterFinished>(rpc);
            break;
        case WireFormat::BackupQuiesce::opcode:
            callHandler<WireFormat::BackupQuiesce, CoordinatorService,
                        &CoordinatorService::quiesce>(rpc);
            break;
        case WireFormat::SetRuntimeOption::opcode:
            callHandler<WireFormat::SetRuntimeOption, CoordinatorService,
                        &CoordinatorService::setRuntimeOption>(rpc);
            break;
        case WireFormat::ReassignTabletOwnership::opcode:
            callHandler<WireFormat::ReassignTabletOwnership, CoordinatorService,
                        &CoordinatorService::reassignTabletOwnership>(rpc);
            break;
        case WireFormat::SetMasterRecoveryInfo::opcode:
            callHandler<WireFormat::SetMasterRecoveryInfo, CoordinatorService,
                        &CoordinatorService::setMasterRecoveryInfo>(rpc);
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

/**
 * Top-level server method to handle the CREATE_TABLE request.
 * \copydetails Service::ping
 */
void
CoordinatorService::createTable(const WireFormat::CreateTable::Request* reqHdr,
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
 * Top-level server method to handle the DROP_TABLE request.
 * \copydetails Service::ping
 */
void
CoordinatorService::dropTable(const WireFormat::DropTable::Request* reqHdr,
                              WireFormat::DropTable::Response* respHdr,
                              Rpc* rpc)
{
    const char* name = getString(rpc->requestPayload, sizeof(*reqHdr),
                                 reqHdr->nameLength);
    tableManager.dropTable(name);
}

/**
 * Top-level server method to handle the SPLIT_TABLET request.
 * \copydetails Service::ping
 */
void
CoordinatorService::splitTablet(const WireFormat::SplitTablet::Request* reqHdr,
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

    ServerId newServerId = serverList->enlistServer(serviceMask, readSpeed,
                                                    serviceLocator);

    respHdr->serverId = newServerId.getId();
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
 * Handle the GET_TABLET_MAP RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::getTabletMap(
    const WireFormat::GetTabletMap::Request* reqHdr,
    WireFormat::GetTabletMap::Response* respHdr,
    Rpc* rpc)
{
    ProtoBuf::Tablets tablets;
    tableManager.serialize(&tablets);
    respHdr->tabletMapLength = serializeToResponse(rpc->replyPayload,
                                                   &tablets);
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
 * Handle the TABLETS_RECOVERED RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::recoveryMasterFinished(
    const WireFormat::RecoveryMasterFinished::Request* reqHdr,
    WireFormat::RecoveryMasterFinished::Response* respHdr,
    Rpc* rpc)
{
    ProtoBuf::Tablets recoveredTablets;
    ProtoBuf::parseFromRequest(rpc->requestPayload,
                               sizeof32(*reqHdr),
                               reqHdr->tabletsLength, &recoveredTablets);

    ServerId serverId = ServerId(reqHdr->recoveryMasterId);
    respHdr->cancelRecovery =
        recoveryManager.recoveryMasterFinished(reqHdr->recoveryId,
                                               serverId,
                                               recoveredTablets,
                                               reqHdr->successful);
}

/**
 * Have all backups flush their dirty segments to storage.
 * \copydetails Service::ping
 */
void
CoordinatorService::quiesce(const WireFormat::BackupQuiesce::Request* reqHdr,
                            WireFormat::BackupQuiesce::Response* respHdr,
                            Rpc* rpc)
{
    for (size_t i = 0; i < serverList->size(); i++) {
        try {
            if ((*serverList)[i].isBackup()) {
                BackupClient::quiesce(context, (*serverList)[i].serverId);
            }
        } catch (ServerListException& e) {
            // Do nothing for the server that doesn't exist. Continue.
        }
    }
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
