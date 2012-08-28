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

namespace RAMCloud {

CoordinatorService::CoordinatorService(Context& context, string LogCabinLocator)
    : context(context)
    , serverList(*context.coordinatorServerList)
    , tabletMap()
    , tables()
    , nextTableId(0)
    , nextTableMasterIdx(0)
    , runtimeOptions()
    , recoveryManager(context, tabletMap, &runtimeOptions)
    , serverManager(*this)
    , coordinatorRecovery(*this)
    , logCabinCluster()
    , logCabinLog()
    , logCabinHelper()
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

    recoveryManager.start();
}

CoordinatorService::~CoordinatorService()
{
    recoveryManager.halt();
}

void
CoordinatorService::dispatch(WireFormat::Opcode opcode,
                             Rpc& rpc)
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
        case WireFormat::SendServerList::opcode:
            callHandler<WireFormat::SendServerList, CoordinatorService,
                        &CoordinatorService::sendServerList>(rpc);
            break;
        case WireFormat::SetRuntimeOption::opcode:
            callHandler<WireFormat::SetRuntimeOption, CoordinatorService,
                        &CoordinatorService::setRuntimeOption>(rpc);
            break;
        case WireFormat::ReassignTabletOwnership::opcode:
            callHandler<WireFormat::ReassignTabletOwnership, CoordinatorService,
                        &CoordinatorService::reassignTabletOwnership>(rpc);
            break;
        case WireFormat::SetMinOpenSegmentId::opcode:
            callHandler<WireFormat::SetMinOpenSegmentId, CoordinatorService,
                        &CoordinatorService::setMinOpenSegmentId>(rpc);
            break;
        case WireFormat::SplitTablet::opcode:
            callHandler<WireFormat::SplitTablet, CoordinatorService,
                        &CoordinatorService::splitTablet>(rpc);
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
CoordinatorService::createTable(const WireFormat::CreateTable::Request& reqHdr,
                                WireFormat::CreateTable::Response& respHdr,
                                Rpc& rpc)
{
    if (serverList.masterCount() == 0) {
        respHdr.common.status = STATUS_RETRY;
        return;
    }

    const char* name = getString(rpc.requestPayload, sizeof(reqHdr),
                                 reqHdr.nameLength);
    if (tables.find(name) != tables.end())
        return;
    uint64_t tableId = nextTableId++;
    tables[name] = tableId;
    respHdr.tableId = tableId;

    uint32_t serverSpan = reqHdr.serverSpan;
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
            size_t masterIdx = nextTableMasterIdx++ % serverList.size();
            auto entry = serverList[masterIdx];
            if (entry && entry->isMaster()) {
                master = *entry;
                break;
            }
        }
        // Get current log head. Only entries >= this can be part of the tablet.
        Log::Position headOfLog = MasterClient::getHeadOfLog(&context,
                                                             master.serverId);

        // Create tablet map entry.
        tabletMap.addTablet({tableId, firstKeyHash, lastKeyHash,
                             master.serverId, Tablet::NORMAL, headOfLog});

        // Inform the master.
        MasterClient::takeTabletOwnership(&context, master.serverId, tableId,
                                          firstKeyHash, lastKeyHash);

        LOG(DEBUG, "Created table '%s' with id %lu and a span %u on master %s",
                    name, tableId, serverSpan,
                    master.serverId.toString().c_str());
    }

    LOG(NOTICE, "Created table '%s' with id %lu",
                    name, tableId);
}

/**
 * Top-level server method to handle the DROP_TABLE request.
 * \copydetails Service::ping
 */
void
CoordinatorService::dropTable(const WireFormat::DropTable::Request& reqHdr,
                              WireFormat::DropTable::Response& respHdr,
                              Rpc& rpc)
{
    const char* name = getString(rpc.requestPayload, sizeof(reqHdr),
                                 reqHdr.nameLength);
    Tables::iterator it = tables.find(name);
    if (it == tables.end())
        return;
    uint64_t tableId = it->second;
    tables.erase(it);
    vector<Tablet> removed = tabletMap.removeTabletsForTable(tableId);
    foreach (const auto& tablet, removed) {
            MasterClient::dropTabletOwnership(&context,
                                              tablet.serverId,
                                              tableId,
                                              tablet.startKeyHash,
                                              tablet.endKeyHash);
    }

    LOG(NOTICE, "Dropped table '%s' with id %lu", name, tableId);
    LOG(DEBUG, "There are now %lu tablets in the map", tabletMap.size());
}

/**
 * Top-level server method to handle the SPLIT_TABLET request.
 * \copydetails Service::ping
 */
void
CoordinatorService::splitTablet(const WireFormat::SplitTablet::Request& reqHdr,
                              WireFormat::SplitTablet::Response& respHdr,
                              Rpc& rpc)
{
    // Check that the tablet with the described key ranges exists.
    // If the tablet exists, adjust its lastKeyHash so it becomes the tablet
    // for the first part after the split and also copy the tablet and use
    // the copy for the second part after the split.
    const char* name = getString(rpc.requestPayload, sizeof(reqHdr),
                                 reqHdr.nameLength);
    Tables::iterator it(tables.find(name));
    if (it == tables.end()) {
        respHdr.common.status = STATUS_TABLE_DOESNT_EXIST;
        return;
    }
    uint64_t tableId = it->second;

    ServerId serverId;
    try {
        serverId = tabletMap.splitTablet(tableId,
                                         reqHdr.firstKeyHash,
                                         reqHdr.lastKeyHash,
                                         reqHdr.splitKeyHash).first.serverId;
    } catch (const TabletMap::NoSuchTablet& e) {
        respHdr.common.status = STATUS_TABLET_DOESNT_EXIST;
        return;
    } catch (const TabletMap::BadSplit& e) {
        respHdr.common.status = STATUS_REQUEST_FORMAT_ERROR;
        return;
    }

    // Tell the master to split the tablet
    MasterClient::splitMasterTablet(&context, serverId, tableId,
                                    reqHdr.firstKeyHash, reqHdr.lastKeyHash,
                                    reqHdr.splitKeyHash);

    LOG(NOTICE, "In table '%s' I split the tablet that started at key %lu and "
                "ended at key %lu", name, reqHdr.firstKeyHash,
                reqHdr.lastKeyHash);
}

/**
 * Top-level server method to handle the GET_TABLE_ID request.
 * \copydetails Service::ping
 */
void
CoordinatorService::getTableId(
    const WireFormat::GetTableId::Request& reqHdr,
    WireFormat::GetTableId::Response& respHdr,
    Rpc& rpc)
{
    const char* name = getString(rpc.requestPayload, sizeof(reqHdr),
                                 reqHdr.nameLength);
    Tables::iterator it(tables.find(name));
    if (it == tables.end()) {
        respHdr.common.status = STATUS_TABLE_DOESNT_EXIST;
        return;
    }
    respHdr.tableId = it->second;
}

/**
 * Handle the ENLIST_SERVER RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::enlistServer(
    const WireFormat::EnlistServer::Request& reqHdr,
    WireFormat::EnlistServer::Response& respHdr,
    Rpc& rpc)
{
    ServerId replacesId = ServerId(reqHdr.replacesId);
    ServiceMask serviceMask = ServiceMask::deserialize(reqHdr.serviceMask);
    const uint32_t readSpeed = reqHdr.readSpeed;
    const uint32_t writeSpeed = reqHdr.writeSpeed;
    const char* serviceLocator = getString(rpc.requestPayload, sizeof(reqHdr),
                                           reqHdr.serviceLocatorLength);

    ServerId newServerId = serverManager.enlistServer(
        replacesId, serviceMask, readSpeed, writeSpeed,
        serviceLocator);

    respHdr.serverId = newServerId.getId();
    rpc.sendReply();
}

/**
 * Handle the GET_SERVER_LIST RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::getServerList(
    const WireFormat::GetServerList::Request& reqHdr,
    WireFormat::GetServerList::Response& respHdr,
    Rpc& rpc)
{
    ServiceMask serviceMask = ServiceMask::deserialize(reqHdr.serviceMask);

    ProtoBuf::ServerList serialServerList =
        serverManager.getServerList(serviceMask);

    respHdr.serverListLength =
        serializeToResponse(&rpc.replyPayload, &serialServerList);
}

/**
 * Handle the GET_TABLET_MAP RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::getTabletMap(
    const WireFormat::GetTabletMap::Request& reqHdr,
    WireFormat::GetTabletMap::Response& respHdr,
    Rpc& rpc)
{
    ProtoBuf::Tablets tablets;
    tabletMap.serialize(serverList, tablets);
    respHdr.tabletMapLength = serializeToResponse(&rpc.replyPayload,
                                                  &tablets);
}

/**
 * Handle the ENLIST_SERVER RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::hintServerDown(
        const WireFormat::HintServerDown::Request& reqHdr,
        WireFormat::HintServerDown::Response& respHdr,
        Rpc& rpc)
{
    ServerId serverId(reqHdr.serverId);
    rpc.sendReply();

    // reqHdr, respHdr, and rpc are off-limits now
    serverManager.hintServerDown(serverId);
}

/**
 * Handle the TABLETS_RECOVERED RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::recoveryMasterFinished(
    const WireFormat::RecoveryMasterFinished::Request& reqHdr,
    WireFormat::RecoveryMasterFinished::Response& respHdr,
    Rpc& rpc)
{
    ProtoBuf::Tablets recoveredTablets;
    ProtoBuf::parseFromRequest(&rpc.requestPayload,
                               sizeof32(reqHdr),
                               reqHdr.tabletsLength, &recoveredTablets);

    ServerId serverId = ServerId(reqHdr.recoveryMasterId);
    recoveryManager.recoveryMasterFinished(reqHdr.recoveryId,
                                           serverId,
                                           recoveredTablets,
                                           reqHdr.successful);

    // TODO(stutsman): Eventually we'll want to be able to 'reject' recovery
    // master completions, so we'll need to get a return value from
    // recoveryMasterFinished.
}

/**
 * Have all backups flush their dirty segments to storage.
 * \copydetails Service::ping
 */
void
CoordinatorService::quiesce(const WireFormat::BackupQuiesce::Request& reqHdr,
                            WireFormat::BackupQuiesce::Response& respHdr,
                            Rpc& rpc)
{
    for (size_t i = 0; i < serverList.size(); i++) {
        if (serverList[i] && serverList[i]->isBackup()) {
            BackupClient::quiesce(&context, serverList[i]->serverId);
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
    const WireFormat::ReassignTabletOwnership::Request& reqHdr,
    WireFormat::ReassignTabletOwnership::Response& respHdr,
    Rpc& rpc)
{
    ServerId newOwner(reqHdr.newOwnerId);
    if (!serverList.contains(newOwner)) {
        LOG(WARNING, "Server id %s does not exist! Cannot reassign "
            "ownership of tablet %lu, range [%lu, %lu]!",
            newOwner.toString().c_str(), reqHdr.tableId,
            reqHdr.firstKeyHash, reqHdr.lastKeyHash);
        respHdr.common.status = STATUS_SERVER_DOESNT_EXIST;
        return;
    }

    try {
        // Note currently this log message may not be exactly correct due to
        // concurrent operations on the tablet map which may slip in between
        // the message and the actual operation.
        Tablet tablet = tabletMap.getTablet(reqHdr.tableId,
                                           reqHdr.firstKeyHash,
                                           reqHdr.lastKeyHash);
        LOG(NOTICE, "Reassigning tablet %lu, range [%lu, %lu] from server "
            "id %s to server id %s.", reqHdr.tableId, reqHdr.firstKeyHash,
            reqHdr.lastKeyHash, tablet.serverId.toString().c_str(),
            newOwner.toString().c_str());
    } catch (const TabletMap::NoSuchTablet& e) {
        LOG(WARNING, "Could not reassign tablet %lu, range [%lu, %lu]: "
            "not found!", reqHdr.tableId, reqHdr.firstKeyHash,
            reqHdr.lastKeyHash);
        respHdr.common.status = STATUS_TABLE_DOESNT_EXIST;
        return;
    }

    // Get current head of log to preclude all previous data in the log
    // from being considered part of this tablet.
    Log::Position headOfLog = MasterClient::getHeadOfLog(&context, newOwner);

    try {
        tabletMap.modifyTablet(reqHdr.tableId, reqHdr.firstKeyHash,
                               reqHdr.lastKeyHash, newOwner, Tablet::NORMAL,
                               headOfLog);
    } catch (const TabletMap::NoSuchTablet& e) {
        LOG(WARNING, "Could not reassign tablet %lu, range [%lu, %lu]: "
            "not found!", reqHdr.tableId, reqHdr.firstKeyHash,
            reqHdr.lastKeyHash);
        respHdr.common.status = STATUS_TABLE_DOESNT_EXIST;
        return;
    }

    // No need to keep the master waiting. If it sent us this request,
    // then it has guaranteed that all data has been migrated. So long
    // as we think the new owner is up (i.e. haven't started a recovery
    // on it), then it's safe for the old owner to drop the data and for
    // us to switch over.
    rpc.sendReply();

    // TODO(rumble/slaughter) If we fail to alert the new owner we could
    //      get stuck in limbo. What should we do? Retry? Fail the
    //      server and recover it? Can't return to the old master if we
    //      reply early...
    MasterClient::takeTabletOwnership(&context, newOwner, reqHdr.tableId,
                                     reqHdr.firstKeyHash, reqHdr.lastKeyHash);
}

/**
 * Handle the SEND_SERVER_LIST RPC.
 *
 * The Coordinator always pushes server lists and their updates. If a server's
 * FailureDetector determines that the list is out of date, it issues an RPC
 * here to request that we re-send the list.
 *
 * \copydetails Service::ping
 */
void
CoordinatorService::sendServerList(
    const WireFormat::SendServerList::Request& reqHdr,
    WireFormat::SendServerList::Response& respHdr,
    Rpc& rpc)
{
    ServerId id(reqHdr.serverId);
    rpc.sendReply();

    serverManager.sendServerList(id);
}

/**
 * Sets a runtime option field on the coordinator to the indicated value.
 * See CoordinatorClient::setRuntimeOption() for details.
 *
 * \copydetails Service::ping
 */
void
CoordinatorService::setRuntimeOption(
    const WireFormat::SetRuntimeOption::Request& reqHdr,
    WireFormat::SetRuntimeOption::Response& respHdr,
    Rpc& rpc)
{
    const char* option = getString(rpc.requestPayload, sizeof(reqHdr),
                                   reqHdr.optionLength);
    const char* value = getString(rpc.requestPayload,
                                  sizeof32(reqHdr) + reqHdr.optionLength,
                                  reqHdr.valueLength);
    try {
        runtimeOptions.set(option, value);
    } catch (const std::out_of_range& e) {
        respHdr.common.status = STATUS_OBJECT_DOESNT_EXIST;
    }
}

/**
 * Handle the SET_MIN_OPEN_SEGMENT_ID.
 *
 * Updates the minimum open segment id for a particular server.  If the
 * requested update is less than the current value maintained by the
 * coordinator then the old value is retained (that is, the coordinator
 * ignores updates that decrease the value).
 * Any open replicas found during recovery are considered invalid
 * if they have a segmentId less than the minimum open segment id maintained
 * by the coordinator.  This is used by masters to invalidate replicas they
 * have lost contact with while actively writing to them.
 *
 * \copydetails Service::ping
 */
void
CoordinatorService::setMinOpenSegmentId(
    const WireFormat::SetMinOpenSegmentId::Request& reqHdr,
    WireFormat::SetMinOpenSegmentId::Response& respHdr,
    Rpc& rpc)
{
    ServerId serverId(reqHdr.serverId);
    uint64_t segmentId = reqHdr.segmentId;

    LOG(DEBUG, "setMinOpenSegmentId for server %s to %lu",
        serverId.toString().c_str(), segmentId);

    try {
        serverManager.setMinOpenSegmentId(serverId, segmentId);
    } catch (const ServerListException& e) {
        LOG(WARNING, "setMinOpenSegmentId server doesn't exist: %s",
            serverId.toString().c_str());
        respHdr.common.status = STATUS_SERVER_DOESNT_EXIST;
        return;
    }
}

} // namespace RAMCloud
