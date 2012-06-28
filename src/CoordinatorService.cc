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

CoordinatorService::CoordinatorService(Context& context)
    : context(context)
    , serverList(context)
    , tabletMap()
    , tables()
    , nextTableId(0)
    , nextTableMasterIdx(0)
    , runtimeOptions()
    , recoveryManager(context, serverList, tabletMap, &runtimeOptions)
    , serverManager(*this)
{
    recoveryManager.start();
}

CoordinatorService::~CoordinatorService()
{
    recoveryManager.halt();
}

void
CoordinatorService::dispatch(RpcOpcode opcode,
                             Rpc& rpc)
{
    switch (opcode) {
        case CreateTableRpc::opcode:
            callHandler<CreateTableRpc, CoordinatorService,
                        &CoordinatorService::createTable>(rpc);
            break;
        case DropTableRpc::opcode:
            callHandler<DropTableRpc, CoordinatorService,
                        &CoordinatorService::dropTable>(rpc);
            break;
        case GetTableIdRpc::opcode:
            callHandler<GetTableIdRpc, CoordinatorService,
                        &CoordinatorService::getTableId>(rpc);
            break;
        case EnlistServerRpc::opcode:
            callHandler<EnlistServerRpc, CoordinatorService,
                        &CoordinatorService::enlistServer>(rpc);
            break;
        case GetServerListRpc::opcode:
            callHandler<GetServerListRpc, CoordinatorService,
                        &CoordinatorService::getServerList>(rpc);
            break;
        case GetTabletMapRpc::opcode:
            callHandler<GetTabletMapRpc, CoordinatorService,
                        &CoordinatorService::getTabletMap>(rpc);
            break;
        case HintServerDownRpc::opcode:
            callHandler<HintServerDownRpc, CoordinatorService,
                        &CoordinatorService::hintServerDown>(rpc);
            break;
        case RecoveryMasterFinishedRpc::opcode:
            callHandler<RecoveryMasterFinishedRpc, CoordinatorService,
                        &CoordinatorService::recoveryMasterFinished>(rpc);
            break;
        case BackupQuiesceRpc::opcode:
            callHandler<BackupQuiesceRpc, CoordinatorService,
                        &CoordinatorService::quiesce>(rpc);
            break;
        case SetWillRpc::opcode:
            callHandler<SetWillRpc, CoordinatorService,
                        &CoordinatorService::setWill>(rpc);
            break;
        case SendServerListRpc::opcode:
            callHandler<SendServerListRpc, CoordinatorService,
                        &CoordinatorService::sendServerList>(rpc);
            break;
        case SetRuntimeOptionRpc::opcode:
            callHandler<SetRuntimeOptionRpc, CoordinatorService,
                        &CoordinatorService::setRuntimeOption>(rpc);
            break;
        case ReassignTabletOwnershipRpc::opcode:
            callHandler<ReassignTabletOwnershipRpc, CoordinatorService,
                        &CoordinatorService::reassignTabletOwnership>(rpc);
            break;
        case SetMinOpenSegmentIdRpc::opcode:
            callHandler<SetMinOpenSegmentIdRpc, CoordinatorService,
                        &CoordinatorService::setMinOpenSegmentId>(rpc);
            break;
        case SplitTabletRpc::opcode:
            callHandler<SplitTabletRpc, CoordinatorService,
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
CoordinatorService::createTable(const CreateTableRpc::Request& reqHdr,
                                CreateTableRpc::Response& respHdr,
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
        uint64_t startKeyHash = i * (~0UL / serverSpan);
        if (i != 0)
            startKeyHash++;
        uint64_t endKeyHash = (i + 1) * (~0UL / serverSpan);
        if (i == serverSpan - 1)
            endKeyHash = ~0UL;

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
        const char* locator = master.serviceLocator.c_str();
        MasterClient masterClient(
            context.transportManager->getSession(locator));
        // Get current log head. Only entries >= this can be part of the tablet.
        LogPosition headOfLog = masterClient.getHeadOfLog();

        // Create tablet map entry.
        tabletMap.addTablet({tableId, startKeyHash, endKeyHash,
                             master.serverId, Tablet::NORMAL, headOfLog});

        // Create will entry. The tablet is empty, so it doesn't matter where it
        // goes or in how many partitions, initially.
        // It just has to go somewhere.
        ProtoBuf::Tablets will;
        ProtoBuf::Tablets_Tablet& willEntry(*will.add_tablet());
        willEntry.set_table_id(tableId);
        willEntry.set_start_key_hash(startKeyHash);
        willEntry.set_end_key_hash(endKeyHash);
        willEntry.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
        // Hack which hints to the server list that it should assign
        // partition ids.
        willEntry.set_user_data(~(0lu));
        willEntry.set_ctime_log_head_id(headOfLog.segmentId());
        willEntry.set_ctime_log_head_offset(headOfLog.segmentOffset());
        serverList.addToWill(master.serverId, will);

        // Inform the master.
        masterClient.takeTabletOwnership(tableId, startKeyHash, endKeyHash);

        LOG(DEBUG, "Created table '%s' with id %lu and a span %u on master %lu",
                    name, tableId, serverSpan, master.serverId.getId());
    }

    LOG(NOTICE, "Created table '%s' with id %lu",
                    name, tableId);
}

/**
 * Top-level server method to handle the DROP_TABLE request.
 * \copydetails Service::ping
 */
void
CoordinatorService::dropTable(const DropTableRpc::Request& reqHdr,
                              DropTableRpc::Response& respHdr,
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
            MasterClient master(serverList.getSession(tablet.serverId));
            master.dropTabletOwnership(tableId,
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
CoordinatorService::splitTablet(const SplitTabletRpc::Request& reqHdr,
                              SplitTabletRpc::Response& respHdr,
                              Rpc& rpc)
{
    // Check that the tablet with the described key ranges exists.
    // If the tablet exists, adjust its endKeyHash so it becomes the tablet
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
                                         reqHdr.startKeyHash,
                                         reqHdr.endKeyHash,
                                         reqHdr.splitKeyHash).first.serverId;
    } catch (const TabletMap::NoSuchTablet& e) {
        respHdr.common.status = STATUS_TABLET_DOESNT_EXIST;
        return;
    } catch (const TabletMap::BadSplit& e) {
        respHdr.common.status = STATUS_REQUEST_FORMAT_ERROR;
        return;
    }

    // Tell the master to split the tablet
    MasterClient master(serverList.getSession(serverId));
    master.splitMasterTablet(tableId, reqHdr.startKeyHash, reqHdr.endKeyHash,
                             reqHdr.splitKeyHash);

    LOG(NOTICE, "In table '%s' I split the tablet that started at key %lu and "
                "ended at key %lu", name, reqHdr.startKeyHash,
                reqHdr.endKeyHash);
}

/**
 * Top-level server method to handle the GET_TABLE_ID request.
 * \copydetails Service::ping
 */
void
CoordinatorService::getTableId(const GetTableIdRpc::Request& reqHdr,
                              GetTableIdRpc::Response& respHdr,
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
CoordinatorService::enlistServer(const EnlistServerRpc::Request& reqHdr,
                                 EnlistServerRpc::Response& respHdr,
                                 Rpc& rpc)
{
    ServerId replacesId = ServerId(reqHdr.replacesId);
    Tub<CoordinatorServerList::Entry> replacedEntry;
    ServiceMask serviceMask = ServiceMask::deserialize(reqHdr.serviceMask);
    const uint32_t readSpeed = reqHdr.readSpeed;
    const uint32_t writeSpeed = reqHdr.writeSpeed;
    const char* serviceLocator = getString(rpc.requestPayload, sizeof(reqHdr),
                                           reqHdr.serviceLocatorLength);
    ProtoBuf::ServerList serverListUpdate;

    ServerId newServerId = serverManager.enlistServerStart(
        replacesId, &replacedEntry, serviceMask, readSpeed, writeSpeed,
        serviceLocator, &serverListUpdate);

    respHdr.serverId = newServerId.getId();
    rpc.sendReply();

    serverManager.enlistServerComplete(&replacedEntry,
                                       newServerId,
                                       &serverListUpdate);
}

/**
 * Handle the GET_SERVER_LIST RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::getServerList(const GetServerListRpc::Request& reqHdr,
                                  GetServerListRpc::Response& respHdr,
                                  Rpc& rpc)
{
    ServiceMask serviceMask = ServiceMask::deserialize(reqHdr.serviceMask);

    ProtoBuf::ServerList serialServerList =
        serverManager.getServerList(serviceMask);

    respHdr.serverListLength =
        serializeToResponse(rpc.replyPayload, serialServerList);
}

/**
 * Handle the GET_TABLET_MAP RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::getTabletMap(const GetTabletMapRpc::Request& reqHdr,
                                 GetTabletMapRpc::Response& respHdr,
                                 Rpc& rpc)
{
    ProtoBuf::Tablets tablets;
    tabletMap.serialize(serverList, tablets);
    respHdr.tabletMapLength = serializeToResponse(rpc.replyPayload,
                                                  tablets);
}

/**
 * Handle the ENLIST_SERVER RPC.
 * \copydetails Service::ping
 */
void
CoordinatorService::hintServerDown(const HintServerDownRpc::Request& reqHdr,
                                   HintServerDownRpc::Response& respHdr,
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
    const RecoveryMasterFinishedRpc::Request& reqHdr,
    RecoveryMasterFinishedRpc::Response& respHdr,
    Rpc& rpc)
{
    ProtoBuf::Tablets recoveredTablets;
    ProtoBuf::parseFromResponse(rpc.requestPayload,
                                downCast<uint32_t>(sizeof(reqHdr)),
                                reqHdr.tabletsLength, recoveredTablets);

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
CoordinatorService::quiesce(const BackupQuiesceRpc::Request& reqHdr,
                            BackupQuiesceRpc::Response& respHdr,
                            Rpc& rpc)
{
    for (size_t i = 0; i < serverList.size(); i++) {
        if (serverList[i] && serverList[i]->isBackup()) {
            BackupClient(context.transportManager->getSession(
                serverList[i]->serviceLocator.c_str())).quiesce();
        }
    }
}

/**
 * Update the Will associated with a specific Master. This is used
 * by Masters to keep their partitions balanced for efficient
 * recovery.
 *
 * \copydetails Service::ping
 */
void
CoordinatorService::setWill(const SetWillRpc::Request& reqHdr,
                            SetWillRpc::Response& respHdr,
                            Rpc& rpc)
{
    if (!serverManager.setWill(
            ServerId(reqHdr.masterId), rpc.requestPayload, sizeof(reqHdr),
            reqHdr.willLength)) {
        // TODO(ongaro): should be some other error or silent
        throw RequestFormatError(HERE);
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
    const ReassignTabletOwnershipRpc::Request& reqHdr,
    ReassignTabletOwnershipRpc::Response& respHdr,
    Rpc& rpc)
{
    ServerId newOwner(reqHdr.newOwnerMasterId);
    if (!serverList.contains(newOwner)) {
        LOG(WARNING, "Server id %lu does not exist! Cannot reassign "
            "ownership of tablet %lu, range [%lu, %lu]!", *newOwner,
            reqHdr.tableId, reqHdr.firstKey, reqHdr.lastKey);
        respHdr.common.status = STATUS_SERVER_DOESNT_EXIST;
        return;
    }

    try {
        // Note currently this log message may not be exactly correct due to
        // concurrent operations on the tablet map which may slip in between
        // the message and the actual operation.
        Tablet tablet = tabletMap.getTablet(reqHdr.tableId,
                                           reqHdr.firstKey, reqHdr.lastKey);
        LOG(NOTICE, "Reassigning tablet %lu, range [%lu, %lu] from server "
            "id %lu to server id %lu.", reqHdr.tableId, reqHdr.firstKey,
            reqHdr.lastKey, tablet.serverId.getId(), newOwner.getId());
    } catch (const TabletMap::NoSuchTablet& e) {
        LOG(WARNING, "Could not reassign tablet %lu, range [%lu, %lu]: "
            "not found!", reqHdr.tableId, reqHdr.firstKey, reqHdr.lastKey);
        respHdr.common.status = STATUS_TABLE_DOESNT_EXIST;
        return;
    }

    Transport::SessionRef session = serverList.getSession(newOwner);
    MasterClient masterClient(session);
    // Get current head of log to preclude all previous data in the log
    // from being considered part of this tablet.
    LogPosition headOfLog = masterClient.getHeadOfLog();

    try {
        tabletMap.modifyTablet(reqHdr.tableId, reqHdr.firstKey, reqHdr.lastKey,
                               newOwner, Tablet::NORMAL, headOfLog);
    } catch (const TabletMap::NoSuchTablet& e) {
        LOG(WARNING, "Could not reassign tablet %lu, range [%lu, %lu]: "
            "not found!", reqHdr.tableId, reqHdr.firstKey, reqHdr.lastKey);
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
    masterClient.takeTabletOwnership(reqHdr.tableId,
                                     reqHdr.firstKey,
                                     reqHdr.lastKey);
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
    const SendServerListRpc::Request& reqHdr,
    SendServerListRpc::Response& respHdr,
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
CoordinatorService::setRuntimeOption(const SetRuntimeOptionRpc::Request& reqHdr,
                                     SetRuntimeOptionRpc::Response& respHdr,
                                     Rpc& rpc)
{
    const char* option = getString(rpc.requestPayload, sizeof(reqHdr),
                                   reqHdr.optionLength);
    const char* value = getString(rpc.requestPayload,
                                  downCast<uint32_t>(sizeof(reqHdr) +
                                                     reqHdr.optionLength),
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
    const SetMinOpenSegmentIdRpc::Request& reqHdr,
    SetMinOpenSegmentIdRpc::Response& respHdr,
    Rpc& rpc)
{
    ServerId serverId(reqHdr.serverId);
    uint64_t segmentId = reqHdr.segmentId;

    LOG(DEBUG, "setMinOpenSegmentId for server %lu to %lu",
        serverId.getId(), segmentId);

    try {
        serverManager.setMinOpenSegmentId(serverId, segmentId);
    } catch (const Exception& e) {
        LOG(WARNING, "setMinOpenSegmentId server doesn't exist: %lu",
            serverId.getId());
        respHdr.common.status = STATUS_SERVER_DOESNT_EXIST;
        return;
    }
}

} // namespace RAMCloud
