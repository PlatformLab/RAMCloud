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

#include <memory>

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

CoordinatorService::CoordinatorService(Context* context,
                                       uint32_t deadServerTimeout,
                                       string LogCabinLocator,
                                       bool startRecoveryManager)
    : context(context)
    , serverList(context->coordinatorServerList)
    , deadServerTimeout(deadServerTimeout)
    , tableManager(context->tableManager)
    , tables()
    , nextTableId(0)
    , nextTableMasterIdx(0)
    , runtimeOptions()
    , recoveryManager(context, *tableManager, &runtimeOptions)
    , coordinatorRecovery(*this)
    , logCabinCluster()
    , logCabinLog()
    , logCabinHelper()
    , expectedEntryId(LogCabin::Client::NO_ID)
    , forceServerDownForTesting(false)
{
    if (strcmp(LogCabinLocator.c_str(), "testing") == 0) {
        LOG(NOTICE, "Connecting to mock LogCabin cluster for testing.");
        logCabinCluster.construct(LogCabin::Client::Cluster::FOR_TESTING);
    } else {
        LOG(NOTICE, "Connecting to LogCabin cluster at %s",
                    LogCabinLocator.c_str());
        logCabinCluster.construct(LogCabinLocator);
    }
    logCabinLog.construct(logCabinCluster->openLog("coordinator"));
    logCabinHelper.construct(*logCabinLog);
    LOG(NOTICE, "Connected to LogCabin cluster.");

    if (logCabinLog.get()->getLastId() == LogCabin::Client::NO_ID)
        expectedEntryId = 0;
    else
        expectedEntryId = logCabinLog.get()->getLastId() + 1;

    context->recoveryManager = &recoveryManager;
    context->logCabinLog = logCabinLog.get();
    context->logCabinHelper = logCabinHelper.get();
    context->expectedEntryId = &expectedEntryId;

    if (startRecoveryManager)
        recoveryManager.start();

    // Replay the entire log (if any) before we start servicing the RPCs.
    coordinatorRecovery.replay();
}

CoordinatorService::~CoordinatorService()
{
    recoveryManager.halt();
}

void
CoordinatorService::dispatch(WireFormat::Opcode opcode,
                             Rpc* rpc)
{
    switch (opcode) {
        case WireFormat::CreateTable::opcode:
            callHandler<WireFormat::CreateTable, CoordinatorService,
                        &CoordinatorService::createTable>(rpc);
            break;
        case WireFormat::DropTable::opcode:
            callHandler<WireFormat::DropTable, CoordinatorService,
                        &CoordinatorService::dropTable>(rpc);
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
        case WireFormat::HintServerDown::opcode:
            callHandler<WireFormat::HintServerDown, CoordinatorService,
                        &CoordinatorService::hintServerDown>(rpc);
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
    if (tables.find(name) != tables.end())
        return;
    uint64_t tableId = nextTableId++;
    tables[name] = tableId;
    respHdr->tableId = tableId;

    LOG(NOTICE, "Created table '%s' with id %lu",
                    name, tableId);

    uint32_t serverSpan = reqHdr->serverSpan;
    if (serverSpan == 0)
        serverSpan = 1;

    for (uint32_t i = 0; i < serverSpan; i++) {
        uint64_t firstKeyHash = i * (~0UL / serverSpan);
        if (i != 0)
            firstKeyHash++;
        uint64_t lastKeyHash = (i + 1) * (~0UL / serverSpan);
        if (i == serverSpan - 1)
            lastKeyHash = ~0UL;

        // Find the next master in the list.
        CoordinatorServerList::Entry master;
        while (true) {
            size_t masterIdx = nextTableMasterIdx++ % serverList->size();
            CoordinatorServerList::Entry entry;
            try {
                entry = (*serverList)[masterIdx];
                if (entry.isMaster()) {
                    master = entry;
                    break;
                }
            } catch (ServerListException& e) {
                continue;
            }
        }
        // Get current log head. Only entries >= this can be part of the tablet.
        Log::Position headOfLog = MasterClient::getHeadOfLog(context,
                                                             master.serverId);

        // Create tablet map entry.
        tableManager->addTablet({tableId, firstKeyHash, lastKeyHash,
                             master.serverId, Tablet::NORMAL, headOfLog});

        // Inform the master.
        MasterClient::takeTabletOwnership(context, master.serverId, tableId,
                                          firstKeyHash, lastKeyHash);

        LOG(NOTICE, "Assigned tablet [0x%lx,0x%lx] in table '%s' (id %lu) "
                    "to %s",
                    firstKeyHash, lastKeyHash, name, tableId,
                    serverList->toString(master.serverId).c_str());
    }
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
    Tables::iterator it = tables.find(name);
    if (it == tables.end())
        return;
    uint64_t tableId = it->second;
    tables.erase(it);
    vector<Tablet> removed = tableManager->removeTabletsForTable(tableId);
    foreach (const auto& tablet, removed) {
            MasterClient::dropTabletOwnership(context,
                                              tablet.serverId,
                                              tableId,
                                              tablet.startKeyHash,
                                              tablet.endKeyHash);
    }

    LOG(NOTICE, "Dropped table '%s' (id %lu), %lu tablets left in map",
        name, tableId, tableManager->size());
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
    Tables::iterator it(tables.find(name));
    if (it == tables.end()) {
        respHdr->common.status = STATUS_TABLE_DOESNT_EXIST;
        return;
    }
    uint64_t tableId = it->second;

    ServerId serverId;
    try {
        serverId = tableManager->splitTablet(tableId,
                                         reqHdr->firstKeyHash,
                                         reqHdr->lastKeyHash,
                                         reqHdr->splitKeyHash).first.serverId;
    } catch (const TableManager::NoSuchTablet& e) {
        respHdr->common.status = STATUS_TABLET_DOESNT_EXIST;
        return;
    } catch (const TableManager::BadSplit& e) {
        respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
        return;
    }

    // Tell the master to split the tablet
    MasterClient::splitMasterTablet(context, serverId, tableId,
                                    reqHdr->firstKeyHash, reqHdr->lastKeyHash,
                                    reqHdr->splitKeyHash);

    LOG(NOTICE, "In table '%s' I split the tablet that started at key %lu and "
                "ended at key %lu", name, reqHdr->firstKeyHash,
                reqHdr->lastKeyHash);
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
    Tables::iterator it(tables.find(name));
    if (it == tables.end()) {
        respHdr->common.status = STATUS_TABLE_DOESNT_EXIST;
        return;
    }
    respHdr->tableId = it->second;
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

    ServerId newServerId = serverList->enlistServer(
        replacesId, serviceMask, readSpeed, serviceLocator);

    respHdr->serverId = newServerId.getId();
    rpc->sendReply();
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
    serverList->serialize(serialServerList, serviceMask);

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
    tableManager->serialize(*serverList, tablets);
    respHdr->tabletMapLength = serializeToResponse(rpc->replyPayload,
                                                   &tablets);
}

/**
 * Handle the ENLIST_SERVER RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::hintServerDown(
        const WireFormat::HintServerDown::Request* reqHdr,
        WireFormat::HintServerDown::Response* respHdr,
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
         serverId.toString().c_str(), serverList->getLocator(serverId));
     if (!verifyServerFailure(serverId))
         return;

     LOG(NOTICE, "Server id %s has crashed, notifying the cluster and "
         "starting recovery", serverId.toString().c_str());

     serverList->serverDown(serverId);
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
    recoveryManager.recoveryMasterFinished(reqHdr->recoveryId,
                                           serverId,
                                           recoveredTablets,
                                           reqHdr->successful);

    // TODO(stutsman): Eventually we'll want to be able to 'reject' recovery
    // master completions, so we'll need to get a return value from
    // recoveryMasterFinished.
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
 * Handle the REASSIGN_TABLET_OWNER RPC. This involves switching
 * ownership of the tablet and alerting the new owner that it may
 * begin servicing requests on that tablet.
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
    if (!serverList->isUp(newOwner)) {
        LOG(WARNING, "Cannot reassign tablet [0x%lx,0x%lx] in tableId %lu "
            "to %s: server not up", reqHdr->firstKeyHash,
            reqHdr->lastKeyHash, reqHdr->tableId,
            newOwner.toString().c_str());
        respHdr->common.status = STATUS_SERVER_NOT_UP;
        return;
    }

    try {
        // Note currently this log message may not be exactly correct due to
        // concurrent operations on the tablet map which may slip in between
        // the message and the actual operation.
        Tablet tablet = tableManager->getTablet(reqHdr->tableId,
                                           reqHdr->firstKeyHash,
                                           reqHdr->lastKeyHash);
        LOG(NOTICE, "Reassigning tablet [0x%lx,0x%lx] in tableId %lu "
            "from %s to %s", reqHdr->firstKeyHash,
            reqHdr->lastKeyHash, reqHdr->tableId,
            serverList->toString(tablet.serverId).c_str(),
            serverList->toString(newOwner).c_str());
    } catch (const TableManager::NoSuchTablet& e) {
        LOG(WARNING, "Could not reassign tablet [0x%lx,0x%lx] in tableId %lu: "
            "tablet not found", reqHdr->firstKeyHash,
            reqHdr->lastKeyHash, reqHdr->tableId);
        respHdr->common.status = STATUS_TABLE_DOESNT_EXIST;
        return;
    }

    // Get current head of log to preclude all previous data in the log
    // from being considered part of this tablet.
    Log::Position headOfLogAtCreation(reqHdr->ctimeSegmentId,
                                      reqHdr->ctimeSegmentOffset);

    try {
        tableManager->modifyTablet(reqHdr->tableId, reqHdr->firstKeyHash,
                               reqHdr->lastKeyHash, newOwner, Tablet::NORMAL,
                               headOfLogAtCreation);
    } catch (const TableManager::NoSuchTablet& e) {
        LOG(WARNING, "Could not reassign tablet [0x%lx,0x%lx] in tableId %lu: "
            "tablet not found", reqHdr->firstKeyHash,
            reqHdr->lastKeyHash, reqHdr->tableId);
        respHdr->common.status = STATUS_TABLE_DOESNT_EXIST;
        return;
    }

    // No need to keep the master waiting. If it sent us this request,
    // then it has guaranteed that all data has been migrated. So long
    // as we think the new owner is up (i.e. haven't started a recovery
    // on it), then it's safe for the old owner to drop the data and for
    // us to switch over.
    rpc->sendReply();

    // TODO(rumble/slaughter) If we fail to alert the new owner we could
    //      get stuck in limbo. What should we do? Retry? Fail the
    //      server and recover it? Can't return to the old master if we
    //      reply early...
    MasterClient::takeTabletOwnership(context, newOwner, reqHdr->tableId,
                                     reqHdr->firstKeyHash, reqHdr->lastKeyHash);
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

    try {
        serverList->setMasterRecoveryInfo(serverId, recoveryInfo);
    } catch (const ServerListException& e) {
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
