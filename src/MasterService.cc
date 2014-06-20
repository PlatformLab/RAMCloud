/* Copyright (c) 2009-2014 Stanford University
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

#include <unordered_map>
#include <unordered_set>

#include "Buffer.h"
#include "ClientException.h"
#include "Cycles.h"
#include "Dispatch.h"
#include "Enumeration.h"
#include "EnumerationIterator.h"
#include "IndexKey.h"
#include "LogIterator.h"
#include "MasterClient.h"
#include "MasterService.h"
#include "ObjectBuffer.h"
#include "PerfCounter.h"
#include "ProtoBuf.h"
#include "RawMetrics.h"
#include "Segment.h"
#include "ServiceManager.h"
#include "ShortMacros.h"
#include "Transport.h"
#include "Tub.h"
#include "WallTime.h"

namespace RAMCloud {

// struct MasterService::Replica

/**
 * Constructor.
 * \param backupId
 *      See #backupId member.
 * \param segmentId
 *      See #segmentId member.
 * \param state
 *      See #state member. The default (NOT_STARTED) is usually what you want
 *      here, but other values are allowed for testing.
 */
MasterService::Replica::Replica(uint64_t backupId, uint64_t segmentId,
                                State state)
    : backupId(backupId)
    , segmentId(segmentId)
    , state(state)
{
}

// --- MasterService ---

/**
 * Construct a MasterService.
 *
 * \param context
 *      Overall information about the RAMCloud server or client.
 * \param config
 *      Contains various parameters that configure the operation of
 *      this server.
 */
MasterService::MasterService(Context* context,
                             const ServerConfig* config)
    : context(context)
    , config(config)
    , objectFinder(context)
    , disableCount(0)
    , initCalled(false)
    , logEverSynced(false)
    , masterTableMetadata()
    , maxResponseRpcLen(Transport::MAX_RPC_LEN)
    , objectManager(context,
                    &serverId,
                    config,
                    &tabletManager,
                    &masterTableMetadata)
    , tabletManager()
    , indexletManager(context, &objectManager)
{
}

MasterService::~MasterService()
{
}

// See Server::dispatch.
void
MasterService::dispatch(WireFormat::Opcode opcode, Rpc* rpc)
{
    if (!initCalled) {
        LOG(WARNING, "%s invoked before initialization complete; "
                "returning STATUS_RETRY", WireFormat::opcodeSymbol(opcode));
        throw RetryException(HERE, 100, 100,
                "master service not yet initialized");
    }

    if (disableCount > 0) {
        LOG(NOTICE, "requesting retry of %s request (master disable count %d)",
                WireFormat::opcodeSymbol(opcode),
                disableCount.load());
        prepareErrorResponse(rpc->replyPayload, STATUS_RETRY);
        return;
    }

    switch (opcode) {
        case WireFormat::DropTabletOwnership::opcode:
            callHandler<WireFormat::DropTabletOwnership, MasterService,
                        &MasterService::dropTabletOwnership>(rpc);
            break;
        case WireFormat::DropIndexletOwnership::opcode:
            callHandler<WireFormat::DropIndexletOwnership, MasterService,
                        &MasterService::dropIndexletOwnership>(rpc);
            break;
        case WireFormat::Enumerate::opcode:
            callHandler<WireFormat::Enumerate, MasterService,
                        &MasterService::enumerate>(rpc);
            break;
        case WireFormat::GetHeadOfLog::opcode:
            callHandler<WireFormat::GetHeadOfLog, MasterService,
                        &MasterService::getHeadOfLog>(rpc);
            break;
        case WireFormat::GetLogMetrics::opcode:
            callHandler<WireFormat::GetLogMetrics, MasterService,
                        &MasterService::getLogMetrics>(rpc);
            break;
        case WireFormat::GetServerStatistics::opcode:
            callHandler<WireFormat::GetServerStatistics, MasterService,
                        &MasterService::getServerStatistics>(rpc);
            break;
        case WireFormat::FillWithTestData::opcode:
            callHandler<WireFormat::FillWithTestData, MasterService,
                        &MasterService::fillWithTestData>(rpc);
            break;
        case WireFormat::Increment::opcode:
            callHandler<WireFormat::Increment, MasterService,
                        &MasterService::increment>(rpc);
            break;
        case WireFormat::InsertIndexEntry::opcode:
            callHandler<WireFormat::InsertIndexEntry, MasterService,
                        &MasterService::insertIndexEntry>(rpc);
            break;
        case WireFormat::IsReplicaNeeded::opcode:
            callHandler<WireFormat::IsReplicaNeeded, MasterService,
                        &MasterService::isReplicaNeeded>(rpc);
            break;
        case WireFormat::LookupIndexKeys::opcode:
            callHandler<WireFormat::LookupIndexKeys, MasterService,
                        &MasterService::lookupIndexKeys>(rpc);
            break;
        case WireFormat::MigrateTablet::opcode:
            callHandler<WireFormat::MigrateTablet, MasterService,
                        &MasterService::migrateTablet>(rpc);
            break;
        case WireFormat::IndexedRead::opcode:
            callHandler<WireFormat::IndexedRead, MasterService,
                        &MasterService::indexedRead>(rpc);
            break;
        case WireFormat::MultiOp::opcode:
            callHandler<WireFormat::MultiOp, MasterService,
                        &MasterService::multiOp>(rpc);
            break;
        case WireFormat::PrepForMigration::opcode:
            callHandler<WireFormat::PrepForMigration, MasterService,
                        &MasterService::prepForMigration>(rpc);
            break;
        case WireFormat::Read::opcode:
            callHandler<WireFormat::Read, MasterService,
                        &MasterService::read>(rpc);
            break;
        case WireFormat::ReadKeysAndValue::opcode:
            callHandler<WireFormat::ReadKeysAndValue, MasterService,
                        &MasterService::readKeysAndValue>(rpc);
            break;
        case WireFormat::ReceiveMigrationData::opcode:
            callHandler<WireFormat::ReceiveMigrationData, MasterService,
                        &MasterService::receiveMigrationData>(rpc);
            break;
        case WireFormat::Remove::opcode:
            callHandler<WireFormat::Remove, MasterService,
                        &MasterService::remove>(rpc);
            break;
        case WireFormat::RemoveIndexEntry::opcode:
            callHandler<WireFormat::RemoveIndexEntry, MasterService,
                        &MasterService::removeIndexEntry>(rpc);
            break;
        case WireFormat::SplitMasterTablet::opcode:
            callHandler<WireFormat::SplitMasterTablet, MasterService,
                        &MasterService::splitMasterTablet>(rpc);
            break;
        case WireFormat::TakeTabletOwnership::opcode:
            callHandler<WireFormat::TakeTabletOwnership, MasterService,
                        &MasterService::takeTabletOwnership>(rpc);
            break;
        case WireFormat::TakeIndexletOwnership::opcode:
            callHandler<WireFormat::TakeIndexletOwnership, MasterService,
                        &MasterService::takeIndexletOwnership>(rpc);
            break;
        case WireFormat::Write::opcode:
            callHandler<WireFormat::Write, MasterService,
                        &MasterService::write>(rpc);
            break;
        // Recovery. Should eventually move away with other recovery code.
        case WireFormat::Recover::opcode:
            callHandler<WireFormat::Recover, MasterService,
                        &MasterService::recover>(rpc);
            break;
        default:
            prepareErrorResponse(rpc->replyPayload,
                                 STATUS_UNIMPLEMENTED_REQUEST);
    }
}

/**
 * Construct a Disabler object (disable the associated master).
 *
 * \param service
 *      The MasterService that should be disabled.  If NULL, then no
 *      service is disabled.
 */
MasterService::Disabler::Disabler(MasterService* service)
    : service(service)
{
    if (service != NULL) {
        service->disableCount++;
    }
    TEST_LOG("master service disabled");
}

/**
 * Destroy a Disabler object (reenable the associated master).
 */
MasterService::Disabler::~Disabler()
{
    reenable();
}

/**
 * Reenable request servicing on the associated MasterService.
 */
void
MasterService::Disabler::reenable()
{
    if (service != NULL) {
        service->disableCount--;
        service = NULL;
    }
}

/**
 * Top-level server method to handle the DROP_TABLET_OWNERSHIP request.
 *
 * This RPC is issued by the coordinator when a table is dropped and all
 * tablets are being destroyed. This is not currently used in migration,
 * since the source master knows that it no longer owns the tablet when
 * the coordinator has responded to its REASSIGN_TABLET_OWNERSHIP rpc.
 *
 * \copydetails Service::ping
 */
void
MasterService::dropTabletOwnership(
        const WireFormat::DropTabletOwnership::Request* reqHdr,
        WireFormat::DropTabletOwnership::Response* respHdr,
        Rpc* rpc)
{
    bool deleted = tabletManager.deleteTablet(reqHdr->tableId,
                                              reqHdr->firstKeyHash,
                                              reqHdr->lastKeyHash);
    if (deleted) {
        // Ensure that the ObjectManager never returns objects from this deleted
        // tablet again.
        objectManager.removeOrphanedObjects();
        LOG(NOTICE, "Dropped ownership of tablet [0x%lx,0x%lx] in tableId %lu",
            reqHdr->firstKeyHash, reqHdr->lastKeyHash, reqHdr->tableId);
    } else {
        // to make this operation idempotent, don't return bad status.

        LOG(WARNING, "Could not drop ownership on unknown tablet [0x%lx,0x%lx]"
            " in tableId %lu!", reqHdr->firstKeyHash, reqHdr->lastKeyHash,
            reqHdr->tableId);
    }
}

/**
 * Top-level server method to handle the DROP_INDEXLET_OWNERSHIP request.
 *
 * This RPC is issued by the coordinator when an index is dropped and all
 * indexlets are being destroyed.
 *
 * \copydetails Service::ping
 */
void
MasterService::dropIndexletOwnership(
        const WireFormat::DropIndexletOwnership::Request* reqHdr,
        WireFormat::DropIndexletOwnership::Response* respHdr,
        Rpc* rpc)
{
    uint32_t reqOffset = sizeof32(*reqHdr);
    const void* firstKey =
        rpc->requestPayload->getRange(reqOffset, reqHdr->firstKeyLength);
    reqOffset+=reqHdr->firstKeyLength;
    const void* firstNotOwnedKey =
      rpc->requestPayload->getRange(reqOffset, reqHdr->firstNotOwnedKeyLength);

    bool deleted = indexletManager.deleteIndexlet(
                            reqHdr->tableId, reqHdr->indexId,
                            firstKey, reqHdr->firstKeyLength,
                            firstNotOwnedKey, reqHdr->firstNotOwnedKeyLength);

    if (deleted) {
        LOG(NOTICE, "Dropped ownership of indexlet in tableId %lu"
            " indexId %u", reqHdr->tableId, reqHdr->indexId);
    } else {
        // to make this operation idempotent, don't return bad status.
        LOG(WARNING, "Could not drop ownership on unknown indexlet "
            "for tableId %lu indexId %u!", reqHdr->tableId, reqHdr->indexId);
    }
}

/**
 * Top-level server method to handle the ENUMERATE request.
 *
 * \copydetails Service::ping
 */
void
MasterService::enumerate(const WireFormat::Enumerate::Request* reqHdr,
                         WireFormat::Enumerate::Response* respHdr,
                         Rpc* rpc)
{
    TabletManager::Tablet tablet;
    bool found = tabletManager.getTablet(reqHdr->tableId,
                                         reqHdr->tabletFirstHash,
                                         &tablet);
    if (!found) {
        // TODO(Ouster): The code has never handled non-NORMAL table states.
        // Does this matter at all?
        respHdr->common.status = STATUS_UNKNOWN_TABLET;
        return;
    }

    // In some cases, actualTabletStartHash may differ from
    // reqHdr->tabletFirstHash, e.g. when a tablet is merged in between
    // RPCs made to enumerate that tablet. If that happens, we must
    // filter by reqHdr->tabletFirstHash, NOT the actualTabletStartHash
    // for the tablet we own.
    uint64_t actualTabletStartHash = tablet.startKeyHash;
    uint64_t actualTabletEndHash = tablet.endKeyHash;

    EnumerationIterator iter(
        *rpc->requestPayload,
        downCast<uint32_t>(sizeof(*reqHdr)), reqHdr->iteratorBytes);

    Buffer payload;
    // A rough upper bound on how much space will be available in the response.
    uint32_t maxPayloadBytes =
            downCast<uint32_t>(Transport::MAX_RPC_LEN - sizeof(*respHdr)
                               - reqHdr->iteratorBytes);
    Enumeration enumeration(reqHdr->tableId, reqHdr->keysOnly,
                            reqHdr->tabletFirstHash,
                            actualTabletStartHash, actualTabletEndHash,
                            &respHdr->tabletFirstHash, iter,
                            *objectManager.getLog(),
                            *objectManager.getObjectMap(),
                            *rpc->replyPayload, maxPayloadBytes);
    enumeration.complete();
    respHdr->payloadBytes = rpc->replyPayload->getTotalLength()
            - downCast<uint32_t>(sizeof(*respHdr));

    // Add new iterator to the end of the response.
    uint32_t iteratorBytes = iter.serialize(*rpc->replyPayload);
    respHdr->iteratorBytes = iteratorBytes;
}

/**
 * Top-level server method to handle the GET_HEAD_OF_LOG request.
 */
void
MasterService::getHeadOfLog(const WireFormat::GetHeadOfLog::Request* reqHdr,
                            WireFormat::GetHeadOfLog::Response* respHdr,
                            Rpc* rpc)
{
    Log::Position head = objectManager.getLog()->rollHeadOver();
    respHdr->headSegmentId = head.getSegmentId();
    respHdr->headSegmentOffset = head.getSegmentOffset();
}

/**
 * Obtain various metrics from the log and return to the caller. Used to
 * remotely monitor the log's utilization and performance.
 *
 * \copydetails Service::ping
 */
void
MasterService::getLogMetrics(
        const WireFormat::GetLogMetrics::Request* reqHdr,
        WireFormat::GetLogMetrics::Response* respHdr,
        Rpc* rpc)
{
    ProtoBuf::LogMetrics logMetrics;
    objectManager.getLog()->getMetrics(logMetrics);
    respHdr->logMetricsLength = ProtoBuf::serializeToResponse(rpc->replyPayload,
                                                              &logMetrics);
}

/**
 * Top-level server method to handle the GET_SERVER_STATISTICS request.
 */
void
MasterService::getServerStatistics(
        const WireFormat::GetServerStatistics::Request* reqHdr,
        WireFormat::GetServerStatistics::Response* respHdr,
        Rpc* rpc)
{
    ProtoBuf::ServerStatistics serverStats;
    tabletManager.getStatistics(&serverStats);
    SpinLock::getStatistics(serverStats.mutable_spin_lock_stats());
    respHdr->serverStatsLength = serializeToResponse(rpc->replyPayload,
                                                     &serverStats);
}

/**
 * Fill a master server with the given number of objects, each of the
 * same given size. Objects are added to all tables in the master in
 * a round-robin fashion. This method exists simply to quickly fill a
 * master for experiments.
 *
 * See MasterClient::fillWithTestData() for more information.
 *
 * \bug Will return an error if the master only owns part of a table
 * (because the hash of the fabricated keys may land in a region it
 * doesn't own).
 *
 * \copydetails Service::ping
 */
void
MasterService::fillWithTestData(
        const WireFormat::FillWithTestData::Request* reqHdr,
        WireFormat::FillWithTestData::Response* respHdr,
        Rpc* rpc)
{
    vector<TabletManager::Tablet> tablets;
    tabletManager.getTablets(&tablets);

    for (size_t i = 0; i < tablets.size(); i++) {
        // Only use tablets that span the entire table here.
        // The key calculation is not safe otherwise.
        TabletManager::Tablet* tablet = &tablets[i];
        if (tablet->startKeyHash != 0 || tablet->endKeyHash != ~0UL) {
            tablets[i] = tablets[tablets.size() - 1];
            tablets.pop_back();
            i--;
        }
    }
    if (tablets.size() == 0)
        throw ObjectDoesntExistException(HERE);

    LOG(NOTICE, "Filling with %u objects of %u bytes each in %Zd tablets",
        reqHdr->numObjects, reqHdr->objectSize, tablets.size());

    RejectRules rejectRules;
    memset(&rejectRules, 0, sizeof(RejectRules));
    rejectRules.exists = 1;

    for (uint32_t objects = 0; objects < reqHdr->numObjects; objects++) {
        Buffer buffer;

        int t = downCast<int>(objects % tablets.size());

        // safe? doubtful. simple? you bet.
        uint8_t data[reqHdr->objectSize];
        memset(data, 0xcc, reqHdr->objectSize);

        string keyString = format("%lu", objects / tablets.size());
        Key key(tablets[t].tableId,
                keyString.c_str(),
                downCast<uint16_t>(keyString.length()));

        Object::appendKeysAndValueToBuffer(key, data, reqHdr->objectSize,
                                           buffer);

        uint64_t newVersion;
        Object object(tablets[t].tableId, 0, 0, buffer);
        Status status = objectManager.writeObject(object,
                                                   &rejectRules,
                                                   &newVersion);
        if (status == STATUS_RETRY) {
            LOG(ERROR, "Server ran out of space while filling with test data; "
                "run your experiment again with a larger master; "
                "stored %u of %u objects before running out of space",
                objects, reqHdr->numObjects);
            status = STATUS_NO_TABLE_SPACE;
        }

        if (status != STATUS_OK) {
            respHdr->common.status = status;
            return;
        }

        if ((objects % 50) == 0) {
            objectManager.getReplicaManager()->proceed();
        }
    }

    objectManager.syncChanges();

    LOG(NOTICE, "Done writing objects.");
}

/**
 * Top-level server method to handle the INCREMENT request.
 *
 * \copydetails MasterService::read
 */
void
MasterService::increment(const WireFormat::Increment::Request* reqHdr,
                     WireFormat::Increment::Response* respHdr,
                     Rpc* rpc)
{
    // Read the current value of the object and add the increment value
    Key key(reqHdr->tableId, *rpc->requestPayload, sizeof32(*reqHdr),
            reqHdr->keyLength);

    Status *status = &respHdr->common.status;

    ObjectBuffer value;
    RejectRules rejectRules = reqHdr->rejectRules;
    *status = objectManager.readObject(key, &value, &rejectRules, NULL);
    if (*status != STATUS_OK)
        return;

    uint32_t dataLen;
    const int64_t oldValue = *value.get<int64_t>(&dataLen);

    if (dataLen != sizeof(int64_t)) {
        *status = STATUS_INVALID_OBJECT;
        return;
    }

    int64_t newValue = oldValue + reqHdr->incrementValue;

    // Write the new value back
    Buffer newValueBuffer;

    // create object to populate newValueBuffer.
    Object::appendKeysAndValueToBuffer(key, &newValue, sizeof(int64_t),
                                       newValueBuffer);

    Object newObject(reqHdr->tableId, 0, 0, newValueBuffer);
    *status = objectManager.writeObject(newObject, &rejectRules,
                                         &respHdr->version);
    if (*status != STATUS_OK)
        return;
    objectManager.syncChanges();

    // Return new value
    respHdr->newValue = newValue;
}

/**
 * Top-level server method to handle the INDEXED_READ request.
 *
 * \param reqHdr
 *      Header from the incoming RPC request; contains all the
 *      parameters for this operation except the keyhashes for objects
 *      and the lookup key or key range.
 * \param[out] respHdr
 *      Header for the response that will be returned to the client.
 *      The caller has pre-allocated the right amount of space in the
 *      response buffer for this type of request, and has zeroed out
 *      its contents (so, for example, status is already zero).
 * \param[out] rpc
 *      Complete information about the remote procedure call.
 *      It contains the keyhashes for objects and the lookup key range.
 */
void
MasterService::indexedRead(
        const WireFormat::IndexedRead::Request* reqHdr,
        WireFormat::IndexedRead::Response* respHdr,
        Rpc* rpc)
{
    uint32_t reqOffset = sizeof32(*reqHdr);

    const void* firstKeyStr =
            rpc->requestPayload->getRange(reqOffset, reqHdr->firstKeyLength);
    reqOffset+=reqHdr->firstKeyLength;

    const void* lastKeyStr =
            rpc->requestPayload->getRange(reqOffset, reqHdr->lastKeyLength);
    reqOffset+=reqHdr->lastKeyLength;

    IndexKeyRange keyRange = {reqHdr->indexId,
                              firstKeyStr, reqHdr->firstKeyLength,
                              lastKeyStr, reqHdr->lastKeyLength};

    objectManager.indexedRead(reqHdr->tableId, reqHdr->numHashes,
                              rpc->requestPayload, reqOffset, &keyRange,
                              maxResponseRpcLen - sizeof32(*reqHdr),
                              rpc->replyPayload, &respHdr->numHashes,
                              &respHdr->numObjects);
}

/**
 * Perform once-only initialization for the master service after having
 * enlisted the process with the coordinator.
 *
 * Any actions performed here must not block the process or dispatch thread,
 * otherwise the server may be timed out and declared failed by the coordinator.
 */
void
MasterService::initOnceEnlisted()
{
    assert(!initCalled);

    LOG(NOTICE, "My server ID is %s", serverId.toString().c_str());
    metrics->serverId = serverId.getId();
    objectManager.initOnceEnlisted();

    initCalled = true;
}

/**
 * Top-level server method to handle the INSERT_INDEX_ENTRY request;
 * As an index server, this function inserts an entry to an index.
 * The RPC requesting this is typically initiated by a data master
 * that was writing the object that this index entry corresponds to.
 */
void
MasterService::insertIndexEntry(
        const WireFormat::InsertIndexEntry::Request* reqHdr,
        WireFormat::InsertIndexEntry::Response* respHdr,
        Rpc* rpc)
{
    uint32_t reqOffset = sizeof32(*reqHdr);
    const void* indexKeyStr =
            rpc->requestPayload->getRange(reqOffset, reqHdr->indexKeyLength);
    respHdr->common.status = indexletManager.insertEntry(
                                reqHdr->tableId, reqHdr->indexId,
                                indexKeyStr, reqHdr->indexKeyLength,
                                reqHdr->primaryKeyHash);
}

/**
 * RPC handler for IS_REPLICA_NEEDED; indicates to backup servers whether
 * a replica for a particular segment that this master generated is needed
 * for durability or that it can be safely discarded.
 */
void
MasterService::isReplicaNeeded(
        const WireFormat::IsReplicaNeeded::Request* reqHdr,
        WireFormat::IsReplicaNeeded::Response* respHdr,
        Rpc* rpc)
{
    ServerId backupServerId = ServerId(reqHdr->backupServerId);
    respHdr->needed = objectManager.getReplicaManager()->isReplicaNeeded(
        backupServerId, reqHdr->segmentId);
}

/**
 * Top-level server method to handle the LOOKUP_INDEX_KEYS request.
 *
 * \param reqHdr
 *      Header from the incoming RPC request; contains all the
 *      parameters for this operation except the lookup key or key range.
 * \param[out] respHdr
 *      Header for the response that will be returned to the client.
 *      The caller has pre-allocated the right amount of space in the
 *      response buffer for this type of request, and has zeroed out
 *      its contents (so, for example, status is already zero).
 * \param[out] rpc
 *      Complete information about the remote procedure call.
 *      It contains the lookup key or key range.
 */
void
MasterService::lookupIndexKeys(
    const WireFormat::LookupIndexKeys::Request* reqHdr,
    WireFormat::LookupIndexKeys::Response* respHdr,
    Rpc* rpc)
{
    uint32_t reqOffset = sizeof32(*reqHdr);

    const void* firstKeyStr =
            rpc->requestPayload->getRange(reqOffset, reqHdr->firstKeyLength);
    reqOffset+=reqHdr->firstKeyLength;

    const void* lastKeyStr =
            rpc->requestPayload->getRange(reqOffset, reqHdr->lastKeyLength);

    // We're hard-coding a number that we think is this is enough bulk to
    // amortize all the fixed costs per RPC.
    uint32_t maxNumHashes = 1000;

    respHdr->common.status = indexletManager.lookupIndexKeys(
                    reqHdr->tableId, reqHdr->indexId,
                    firstKeyStr, reqHdr->firstKeyLength,
                    reqHdr->firstAllowedKeyHash,
                    lastKeyStr, reqHdr->lastKeyLength,
                    maxNumHashes,
                    rpc->replyPayload, &respHdr->numHashes,
                    &respHdr->nextKeyLength, &respHdr->nextKeyHash);
}

/**
 * Top-level server method to handle the MIGRATE_TABLET request.
 *
 * This is used to manually initiate the migration of a tablet (or piece of a
 * tablet) that this master owns to another master.
 *
 * \copydetails Service::ping
 */
void
MasterService::migrateTablet(const WireFormat::MigrateTablet::Request* reqHdr,
                             WireFormat::MigrateTablet::Response* respHdr,
                             Rpc* rpc)
{
    uint64_t tableId = reqHdr->tableId;
    uint64_t firstKeyHash = reqHdr->firstKeyHash;
    uint64_t lastKeyHash = reqHdr->lastKeyHash;
    ServerId newOwnerMasterId(reqHdr->newOwnerMasterId);

    // Find the tablet we're trying to move. We only support migration
    // when the tablet to be migrated consists of a range within a single,
    // contiguous tablet of ours.
    bool found = tabletManager.getTablet(tableId, firstKeyHash, lastKeyHash, 0);
    if (!found) {
        LOG(WARNING, "Migration request for tablet this master does not own: "
            "tablet [0x%lx,0x%lx] in tableId %lu", firstKeyHash, lastKeyHash,
            tableId);
        respHdr->common.status = STATUS_UNKNOWN_TABLET;
        return;
    }

    if (newOwnerMasterId == serverId) {
        LOG(WARNING, "Migrating to myself doesn't make much sense");
        respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
        return;
    }

    // TODO(rumble/slaughter) what if we end up splitting?!?

    // TODO(rumble/slaughter) add method to query for # objs, # bytes in a
    // range in order for this to really work, we'll need to split on a bucket
    // boundary. Otherwise we can't tell where bytes are in the chosen range.
    MasterClient::prepForMigration(context, newOwnerMasterId, tableId,
                                   firstKeyHash, lastKeyHash, 0, 0);
    Log::Position newOwnerLogHead = MasterClient::getHeadOfLog(context,
                                                              newOwnerMasterId);

    LOG(NOTICE, "Migrating tablet [0x%lx,0x%lx] in tableId %lu to %s",
        firstKeyHash, lastKeyHash, tableId,
        context->serverList->toString(newOwnerMasterId).c_str());

    // We'll send over objects in Segment containers for better network
    // efficiency and convenience.
    Tub<Segment> transferSeg;

    // TODO(rumble/slaughter): These should probably be metrics.
    uint64_t totalObjects = 0;
    uint64_t totalTombstones = 0;
    uint64_t totalBytes = 0;

    // Hold on to the iterator since it locks the head Segment, avoiding any
    // additional appends once we've finished iterating.
    LogIterator it(*objectManager.getLog());
    for (; !it.isDone(); it.next()) {
        LogEntryType type = it.getType();
        if (type != LOG_ENTRY_TYPE_OBJ && type != LOG_ENTRY_TYPE_OBJTOMB) {
            // We aren't interested in any other types.
            continue;
        }

        Buffer buffer;
        it.appendToBuffer(buffer);
        Key key(type, buffer);

        // Skip if not applicable.
        if (key.getTableId() != tableId)
            continue;

        if (key.getHash() < firstKeyHash || key.getHash() > lastKeyHash)
            continue;

        if (type == LOG_ENTRY_TYPE_OBJ) {
            // Only send objects when they're currently in the hash table.
            // Otherwise they're dead.
            Buffer currentBuffer;
            uint64_t currentVersion;
            if (objectManager.readObject(
              key, &currentBuffer, 0, &currentVersion) != STATUS_OK) {
                continue;
            }

            // Skip objects older than what is currently in the hash table.
            Object iteratorObject(buffer);
            if (iteratorObject.getVersion() < currentVersion)
                continue;

            totalObjects++;
        } else {
            // We must always send tombstones, since an object we may have sent
            // could have been deleted more recently. We could be smarter and
            // more selective here, but that'd require keeping extra state to
            // know what we've already sent.
            //
            // TODO(rumble/slaughter) Actually, we can do better. The stupid way
            //      is to track each object or tombstone we've sent. The smarter
            //      way is to just record the Log::Position when we started
            //      iterating and only send newer tombstones.

            totalTombstones++;
        }

        totalBytes += buffer.getTotalLength();

        if (!transferSeg)
            transferSeg.construct();

        // If we can't fit it, send the current buffer and retry.
        if (!transferSeg->append(type, buffer)) {
            transferSeg->close();
            LOG(DEBUG, "Sending migration segment");
            MasterClient::receiveMigrationData(context, newOwnerMasterId,
                tableId, firstKeyHash, transferSeg.get());

            transferSeg.destroy();
            transferSeg.construct();

            // If it doesn't fit this time, we're in trouble.
            if (!transferSeg->append(type, buffer)) {
                LOG(ERROR, "Tablet migration failed: could not fit object "
                    "into empty segment (obj bytes %u)",
                    buffer.getTotalLength());
                respHdr->common.status = STATUS_INTERNAL_ERROR;
                return;
            }
        }
    }

    if (transferSeg) {
        transferSeg->close();
        LOG(DEBUG, "Sending last migration segment");
        MasterClient::receiveMigrationData(context, newOwnerMasterId,
            tableId, firstKeyHash, transferSeg.get());
        transferSeg.destroy();
    }

    // Now that all data has been transferred, we can reassign ownership of
    // the tablet. If this succeeds, we are free to drop the tablet. The
    // data is all on the other machine and the coordinator knows to use it
    // for any recoveries.
    CoordinatorClient::reassignTabletOwnership(context,
        tableId, firstKeyHash, lastKeyHash, newOwnerMasterId,
        newOwnerLogHead.getSegmentId(), newOwnerLogHead.getSegmentOffset());

    LOG(NOTICE, "Migration succeeded for tablet [0x%lx,0x%lx] in "
        "tableId %lu; sent %lu objects and %lu tombstones to %s, "
        "%lu bytes in total",
        firstKeyHash, lastKeyHash, tableId, totalObjects, totalTombstones,
        context->serverList->toString(newOwnerMasterId).c_str(), totalBytes);

    tabletManager.deleteTablet(tableId, firstKeyHash, lastKeyHash);

    // Ensure that the ObjectManager never returns objects from this deleted
    // tablet again.
    objectManager.removeOrphanedObjects();
}

/**
 * Multiplexor for the MultiOp opcode.
 */
void
MasterService::multiOp(const WireFormat::MultiOp::Request* reqHdr,
                       WireFormat::MultiOp::Response* respHdr,
                       Rpc* rpc)
{
    switch (reqHdr->type) {
        case WireFormat::MultiOp::OpType::READ:
            multiRead(reqHdr, respHdr, rpc);
            break;
        case WireFormat::MultiOp::OpType::REMOVE:
            multiRemove(reqHdr, respHdr, rpc);
            break;
        case WireFormat::MultiOp::OpType::WRITE:
            multiWrite(reqHdr, respHdr, rpc);
            break;
        default:
            LOG(ERROR, "Unimplemented multiOp (type = %u) received!",
                    (uint32_t) reqHdr->type);
            prepareErrorResponse(rpc->replyPayload,
                         STATUS_UNIMPLEMENTED_REQUEST);
            break;
    }
}

/**
 * Top-level server method to handle the MULTI_READ request.
 *
 * \param reqHdr
 *      Header from the incoming RPC request; contains the parameters
 *      for this operation except the tableId, key, keyLength for each
 *      of the objects to be read.
 * \param[out] respHdr
 *      Header for the response that will be returned to the client.
 *      The caller has pre-allocated the right amount of space in the
 *      response buffer for this type of request, and has zeroed out
 *      its contents (so, for example, status is already zero).
 * \param[out] rpc
 *      Complete information about the remote procedure call.
 *      It contains the tableId, key and keyLength for each of the
 *      objects to be read. It can also be used to read additional
 *      information beyond the request header and/or append additional
 *      information to the response buffer.
 */
void
MasterService::multiRead(const WireFormat::MultiOp::Request* reqHdr,
                         WireFormat::MultiOp::Response* respHdr,
                         Rpc* rpc)
{
    uint32_t numRequests = reqHdr->count;
    uint32_t reqOffset = sizeof32(*reqHdr);

    respHdr->count = numRequests;
    uint32_t oldResponseLength = rpc->replyPayload->getTotalLength();

    // Each iteration extracts one request from request rpc, finds the
    // corresponding object, and appends the response to the response rpc.
    for (uint32_t i = 0; ; i++) {
        // If the RPC response has exceeded the legal limit, truncate it
        // to the last object that fits below the limit (the client will
        // retry the objects we don't return).
        uint32_t newLength = rpc->replyPayload->getTotalLength();
        if (newLength > maxResponseRpcLen) {
            rpc->replyPayload->truncate(oldResponseLength);
            respHdr->count = i-1;
            break;
        } else {
            oldResponseLength = newLength;
        }
        if (i >= numRequests) {
            // The loop-termination check is done here rather than in the
            // "for" statement above so that we have a chance to do the
            // size check above even for every object inserted, including
            // the last object and those with STATUS_OBJECT_DOESNT_EXIST.
            break;
        }

        const WireFormat::MultiOp::Request::ReadPart *currentReq =
            rpc->requestPayload->getOffset<
                WireFormat::MultiOp::Request::ReadPart>(reqOffset);
        reqOffset += sizeof32(WireFormat::MultiOp::Request::ReadPart);
        const void* stringKey = rpc->requestPayload->getRange(
            reqOffset, currentReq->keyLength);
        reqOffset += currentReq->keyLength;
        Key key(currentReq->tableId, stringKey, currentReq->keyLength);

        WireFormat::MultiOp::Response::ReadPart* currentResp =
           rpc->replyPayload->emplaceAppend<
               WireFormat::MultiOp::Response::ReadPart>();

        uint32_t initialLength = rpc->replyPayload->getTotalLength();
        currentResp->status = objectManager.readObject(key,
                                                        rpc->replyPayload,
                                                        NULL,
                                                        &currentResp->version);

        if (currentResp->status != STATUS_OK)
            continue;

        currentResp->length = rpc->replyPayload->getTotalLength() -
                                initialLength;
    }
}

/**
 * Top-level server method to handle the MULTI_REMOVE request.
 *
 * \param reqHdr
 *      Header from the incoming RPC request; contains the parameters
 *      for this operation except the tableId, key, keyLength for each
 *      of the objects to be read.
 * \param[out] respHdr
 *      Header for the response that will be returned to the client.
 *      The caller has pre-allocated the right amount of space in the
 *      response buffer for this type of request, and has zeroed out
 *      its contents (so, for example, status is already zero).
 * \param[out] rpc
 *      Complete information about the remote procedure call.
 *      It contains the the key and value for each object, as well as
 *      RejectRules to support conditional removes.
 */
void
MasterService::multiRemove(const WireFormat::MultiOp::Request* reqHdr,
                           WireFormat::MultiOp::Response* respHdr,
                           Rpc* rpc)
{
    uint32_t numRequests = reqHdr->count;
    uint32_t reqOffset = sizeof32(*reqHdr);

    // Store info about objects being removed so that we can later
    // remove index entries corresponding to them.
    Buffer objectBuffers[numRequests];

    respHdr->count = numRequests;

    // Each iteration extracts one request from request rpc, deletes the
    // corresponding object if possible, and appends the response to the
    // response rpc.
    for (uint32_t i = 0; ; i++) {
        const WireFormat::MultiOp::Request::RemovePart *currentReq =
            rpc->requestPayload->getOffset<
                WireFormat::MultiOp::Request::RemovePart>(reqOffset);

        if (currentReq == NULL) {
            respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
            break;
        }

        reqOffset += sizeof32(WireFormat::MultiOp::Request::RemovePart);
        const void* stringKey = rpc->requestPayload->getRange(
            reqOffset, currentReq->keyLength);
        reqOffset += currentReq->keyLength;

        if (stringKey == NULL) {
            respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
            break;
        }

        Key key(currentReq->tableId, stringKey, currentReq->keyLength);

        WireFormat::MultiOp::Response::RemovePart* currentResp =
            rpc->replyPayload->emplaceAppend<
                WireFormat::MultiOp::Response::RemovePart>();

        RejectRules rejectRules = currentReq->rejectRules;
        currentResp->status = objectManager.removeObject(key, &rejectRules,
                                                         &currentResp->version,
                                                         &objectBuffers[i]);
    }

    // All of the individual removes were done asynchronously. We must sync
    // them to backups before returning to the caller.
    objectManager.syncChanges();

    // Respond to the client RPC now. Removing old index entries can be
    // done asynchronously while maintaining strong consistency.
    rpc->sendReply();
    // reqHdr, respHdr, and rpc are off-limits now!

    // If any of the write parts overwrites, delete old index entries if any.
    for (uint32_t i = 0; i < numRequests; i++) {
        if (objectBuffers[i].getTotalLength() > 0) {
            requestRemoveIndexEntries(objectBuffers[i]);
        }
    }
}

/**
 * Top-level server method to handle the MULTI_WRITE request.
 *
 * \param reqHdr
 *      Header from the incoming RPC request. Lists the number of writes
 *      contained in this request.
 * \param[out] respHdr
 *      Header for the response that will be returned to the client.
 *      The caller has pre-allocated the right amount of space in the
 *      response buffer for this type of request, and has zeroed out
 *      its contents (so, for example, status is already zero).
 * \param[out] rpc
 *      Complete information about the remote procedure call.
 *      It contains the the key and value for each object, as well as
 *      RejectRules to support conditional writes.
 */
void
MasterService::multiWrite(const WireFormat::MultiOp::Request* reqHdr,
                          WireFormat::MultiOp::Response* respHdr,
                          Rpc* rpc)
{
    uint32_t numRequests = reqHdr->count;
    uint32_t reqOffset = sizeof32(*reqHdr);
    respHdr->count = numRequests;

    // Store info about objects being removed (overwritten)
    // so that we can later remove index entries corresponding to them.
    Buffer oldObjectBuffers[numRequests];

    // Each iteration extracts one request from the rpc, writes the object
    // if possible, and appends a status and version to the response buffer.
    for (uint32_t i = 0; i < numRequests; i++) {
        const WireFormat::MultiOp::Request::WritePart *currentReq =
            rpc->requestPayload->getOffset<
                WireFormat::MultiOp::Request::WritePart>(reqOffset);

        if (currentReq == NULL) {
            respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
            break;
        }

        reqOffset += sizeof32(WireFormat::MultiOp::Request::WritePart);

        if (rpc->requestPayload->getTotalLength() <
                reqOffset + currentReq->length) {
            respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
            break;
        }
        WireFormat::MultiOp::Response::WritePart* currentResp =
            rpc->replyPayload->emplaceAppend<
                WireFormat::MultiOp::Response::WritePart>();

        Object object(currentReq->tableId, 0, 0, *(rpc->requestPayload),
                      reqOffset, currentReq->length);

        // Insert new index entries, if any, before writing object (for strong
        // consistency).
        requestInsertIndexEntries(object);

        // Write the object.
        RejectRules rejectRules = currentReq->rejectRules;
        currentResp->status = objectManager.writeObject(object, &rejectRules,
                                                        &currentResp->version,
                                                        &oldObjectBuffers[i]);
        reqOffset += currentReq->length;
    }

    // By design, our response will be shorter than the request. This ensures
    // that the response can go back in a single RPC.
    assert(rpc->replyPayload->getTotalLength() <= Transport::MAX_RPC_LEN);

    // All of the individual writes were done asynchronously. Sync the objects
    // now to propagate them in bulk to backups.
    objectManager.syncChanges();

    // Respond to the client RPC now. Removing old index entries can be
    // done asynchronously while maintaining strong consistency.
    rpc->sendReply();
    // reqHdr, respHdr, and rpc are off-limits now!

    // If any of the write parts overwrites, delete old index entries if any.
    for (uint32_t i = 0; i < numRequests; i++) {
        if (oldObjectBuffers[i].getTotalLength() > 0) {
            requestRemoveIndexEntries(oldObjectBuffers[i]);
        }
    }
}

/**
 * Top-level server method to handle the PREP_FOR_MIGRATION request.
 *
 * This is used during tablet migration to request that a destination
 * master take on a tablet from the current owner. The receiver may
 * accept or refuse.
 *
 * \copydetails Service::ping
 */
void
MasterService::prepForMigration(
        const WireFormat::PrepForMigration::Request* reqHdr,
        WireFormat::PrepForMigration::Response* respHdr,
        Rpc* rpc)
{
    // TODO(anyone): Decide if we want to decline this request.

    // Try to add the tablet. If it fails, there's some overlapping tablet.
    bool added = tabletManager.addTablet(reqHdr->tableId,
                                         reqHdr->firstKeyHash,
                                         reqHdr->lastKeyHash,
                                         TabletManager::RECOVERING);
    if (added) {
        // TODO(rumble) would be nice to have a method to get a SL from an Rpc
        // object.
        LOG(NOTICE, "Ready to receive tablet [0x%lx,0x%lx] in tableId %lu from "
            "\"??\"", reqHdr->firstKeyHash, reqHdr->lastKeyHash,
            reqHdr->tableId);
    } else {
        TabletManager::Tablet tablet;
        if (!tabletManager.getTablet(reqHdr->tableId,
                                     reqHdr->firstKeyHash,
                                     &tablet)) {
            if (!tabletManager.getTablet(reqHdr->tableId,
                                         reqHdr->lastKeyHash,
                                         &tablet)) {
                LOG(NOTICE, "Failed to add tablet [0x%lx,0x%lx] in tableId %lu "
                    ", but no overlap found. Assuming innocuous race and "
                    "sending STATUS_RETRY.", reqHdr->firstKeyHash,
                    reqHdr->lastKeyHash, reqHdr->tableId);
                respHdr->common.status = STATUS_RETRY;
                return;
            }
        }
        LOG(WARNING, "Already have tablet [0x%lx,0x%lx] in tableId %lu, "
            "cannot add [0x%lx,0x%lx]",
            tablet.startKeyHash, tablet.endKeyHash, tablet.tableId,
            reqHdr->firstKeyHash, reqHdr->lastKeyHash);
        respHdr->common.status = STATUS_OBJECT_EXISTS;
        return;
    }
}

/**
 * Top-level server method to handle the READ request.
 *
 * \param reqHdr
 *      Header from the incoming RPC request; contains all the
 *      parameters for this operation except the key of the object.
 * \param[out] respHdr
 *      Header for the response that will be returned to the client.
 *      The caller has pre-allocated the right amount of space in the
 *      response buffer for this type of request, and has zeroed out
 *      its contents (so, for example, status is already zero).
 * \param[out] rpc
 *      Complete information about the remote procedure call.
 *      It contains the key for the object. It can also be used to
 *      read additional information beyond the request header and/or
 *      append additional information to the response buffer.
 */
void
MasterService::read(const WireFormat::Read::Request* reqHdr,
                    WireFormat::Read::Response* respHdr,
                    Rpc* rpc)
{
    using RAMCloud::Perf::ReadRPC_MetricSet;
    ReadRPC_MetricSet::Interval _(&ReadRPC_MetricSet::readRpcTime);

    uint32_t reqOffset = sizeof32(*reqHdr);
    const void* stringKey = rpc->requestPayload->getRange(reqOffset,
                                                        reqHdr->keyLength);
    Key key(reqHdr->tableId, stringKey, reqHdr->keyLength);

    RejectRules rejectRules = reqHdr->rejectRules;
    bool valueOnly = true;
    uint32_t initialLength = rpc->replyPayload->getTotalLength();
    respHdr->common.status = objectManager.readObject(key,
                                                       rpc->replyPayload,
                                                       &rejectRules,
                                                       &respHdr->version,
                                                       valueOnly);
    if (respHdr->common.status != STATUS_OK)
        return;

    respHdr->length = rpc->replyPayload->getTotalLength() - initialLength;
}

/**
 * Top-level server method to handle the READ_KEYS_AND_VALUE request.
 *
 * \param reqHdr
 *      Header from the incoming RPC request; contains all the
 *      parameters for this operation except the key of the object.
 * \param[out] respHdr
 *      Header for the response that will be returned to the client.
 *      The caller has pre-allocated the right amount of space in the
 *      response buffer for this type of request, and has zeroed out
 *      its contents (so, for example, status is already zero).
 * \param[out] rpc
 *      Complete information about the remote procedure call.
 *      It contains the key for the object. It can also be used to
 *      read additional information beyond the request header and/or
 *      append additional information to the response buffer.
 */
void
MasterService::readKeysAndValue(
                const WireFormat::ReadKeysAndValue::Request* reqHdr,
                WireFormat::ReadKeysAndValue::Response* respHdr,
                Rpc* rpc)
{
    uint32_t reqOffset = sizeof32(*reqHdr);
    const void* stringKey = rpc->requestPayload->getRange(reqOffset,
                                                        reqHdr->keyLength);
    Key key(reqHdr->tableId, stringKey, reqHdr->keyLength);

    RejectRules rejectRules = reqHdr->rejectRules;
    uint32_t initialLength = rpc->replyPayload->getTotalLength();
    respHdr->common.status = objectManager.readObject(key,
                                                       rpc->replyPayload,
                                                       &rejectRules,
                                                       &respHdr->version);
    if (respHdr->common.status != STATUS_OK)
        return;

    respHdr->length = rpc->replyPayload->getTotalLength() - initialLength;
}

/**
 * Top-level server method to handle the RECEIVE_MIGRATION_DATA request.
 *
 * This RPC delivers tablet data to be added to a master during migration.
 * It must have been preceeded by an appropriate PREP_FOR_MIGRATION rpc.
 *
 * \copydetails Service::ping
 */
void
MasterService::receiveMigrationData(
        const WireFormat::ReceiveMigrationData::Request* reqHdr,
        WireFormat::ReceiveMigrationData::Response* respHdr,
        Rpc* rpc)
{
    uint64_t tableId = reqHdr->tableId;
    uint64_t firstKeyHash = reqHdr->firstKeyHash;
    uint32_t segmentBytes = reqHdr->segmentBytes;

    LOG(NOTICE, "Receiving %u bytes of migration data for tablet [0x%lx,??] "
        "in tableId %lu", segmentBytes, firstKeyHash, tableId);

    // TODO(rumble/slaughter) need to make sure we already have a table
    // created that was previously prepped for migration.
    TabletManager::Tablet tablet;
    bool found = tabletManager.getTablet(tableId,
                                         firstKeyHash,
                                         &tablet);
    if (!found) {
        LOG(WARNING, "migration data received for unknown tablet [0x%lx,??] "
            "in tableId %lu", firstKeyHash, tableId);
        respHdr->common.status = STATUS_UNKNOWN_TABLET;
        return;
    }

    if (tablet.state != TabletManager::RECOVERING) {
        LOG(WARNING, "migration data received for tablet not in the "
            "RECOVERING state (state = %d)!",
            static_cast<int>(tablet.state));
        // TODO(rumble/slaughter): better error code here?
        respHdr->common.status = STATUS_INTERNAL_ERROR;
        return;
    }

    Segment::Certificate certificate = reqHdr->certificate;
    rpc->requestPayload->truncateFront(sizeof(*reqHdr));
    if (rpc->requestPayload->getTotalLength() != segmentBytes) {
        LOG(ERROR, "RPC size (%u) does not match advertised length (%u)",
            rpc->requestPayload->getTotalLength(),
            segmentBytes);
        respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
        return;
    }
    const void* segmentMemory = rpc->requestPayload->getStart<const void*>();
    SegmentIterator it(segmentMemory, segmentBytes, certificate);
    it.checkMetadataIntegrity();
    SideLog sideLog(objectManager.getLog());
    objectManager.replaySegment(&sideLog, it);
    sideLog.commit();

    // TODO(rumble/slaughter) what about tablet version numbers?
    //          - need to be made per-server now, no? then take max of two?
    //            but this needs to happen at the end (after head on orig.
    //            master is locked)
    //    - what about autoincremented keys?
    //    - what if we didn't send a whole tablet, but rather split one?!
    //      how does this affect autoincr. keys and the version number(s),
    //      if at all?
}

/**
 * Top-level server method to handle the REMOVE request.
 *
 * \copydetails MasterService::read
 */
void
MasterService::remove(const WireFormat::Remove::Request* reqHdr,
                      WireFormat::Remove::Response* respHdr,
                      Rpc* rpc)
{
    const void* stringKey = rpc->requestPayload->getRange(sizeof32(*reqHdr),
                                                          reqHdr->keyLength);
    Key key(reqHdr->tableId, stringKey, reqHdr->keyLength);

    // Buffer for object being removed, so we can remove corresponding
    // index entries later.
    Buffer oldBuffer;

    // Remove the object.
    RejectRules rejectRules = reqHdr->rejectRules;
    respHdr->common.status = objectManager.removeObject(key, &rejectRules,
                                                        &respHdr->version,
                                                        &oldBuffer);
    if (respHdr->common.status == STATUS_OK)
        objectManager.syncChanges();

    // Respond to the client RPC now. Removing old index entries can be
    // done asynchronously while maintaining strong consistency.
    rpc->sendReply();
    // reqHdr, respHdr, and rpc are off-limits now!

    // Remove index entries corresponding to old object, if any.
    if (oldBuffer.getTotalLength() > 0) {
        requestRemoveIndexEntries(oldBuffer);
    }
}

/**
 * RPC handler for REMOVE_INDEX_ENTRY;
 * As an index server, this process removes an index entry. The RPC is
 * typically initiated by a data master that was removing data that this index
 * entry corresponds to.
 */
void
MasterService::removeIndexEntry(
        const WireFormat::RemoveIndexEntry::Request* reqHdr,
        WireFormat::RemoveIndexEntry::Response* respHdr,
        Rpc* rpc)
{
    uint32_t reqOffset = sizeof32(*reqHdr);
    const void* indexKeyStr =
            rpc->requestPayload->getRange(reqOffset, reqHdr->indexKeyLength);
    respHdr->common.status = indexletManager.removeEntry(
                                reqHdr->tableId, reqHdr->indexId,
                                indexKeyStr, reqHdr->indexKeyLength,
                                reqHdr->primaryKeyHash);
}

/**
 * Top-level server method to handle the SPLIT_MASTER_TABLET_OWNERSHIP request.
 *
 * This RPC is issued by the coordinator when a tablet should be splitted. The
 * coordinator specifies the to be splitted tablet and at which point the split
 * should occur (splitKeyHash).
 *
 * \copydetails Service::ping
 */
void
MasterService::splitMasterTablet(
        const WireFormat::SplitMasterTablet::Request* reqHdr,
        WireFormat::SplitMasterTablet::Response* respHdr,
        Rpc* rpc)
{
    bool split = tabletManager.splitTablet(reqHdr->tableId,
                                           reqHdr->splitKeyHash);
    if (split) {
        LOG(NOTICE, "In table '%lu' I split the tablet at key %lu ",
            reqHdr->tableId, reqHdr->splitKeyHash);
    } else {
        LOG(WARNING, "Could not split table %lu at key hash %lu:"
                      "no such tablet on this master",
             reqHdr->tableId, reqHdr->splitKeyHash);
        respHdr->common.status = STATUS_UNKNOWN_TABLET;
    }
}

/**
 * Top-level server method to handle the TAKE_TABLET_OWNERSHIP request.
 *
 * This RPC is issued by the coordinator when assigning ownership of a
 * tablet. This can occur due to both tablet creation and to complete
 * migration. As far as the coordinator is concerned, the master
 * receiving this rpc owns the tablet specified and all requests for it
 * will be directed here from now on.
 *
 * \copydetails Service::ping
 */
void
MasterService::takeTabletOwnership(
        const WireFormat::TakeTabletOwnership::Request* reqHdr,
        WireFormat::TakeTabletOwnership::Response* respHdr,
        Rpc* rpc)
{
    // The code immediately below is tricky, for two reasons:
    // * Before any tablets can be assigned to this master it must have at
    //   least one segment on backups, otherwise it is impossible to
    //   distinguish between the loss of its entire log and the case where
    //   no data was ever written to it. The log's constructor does not
    //   create a head segment because doing so can lead to deadlock: the
    //   first master blocks, waiting to hear about enough backup servers,
    //   meanwhile the coordinator is trying to issue an RPC to the master,
    //   but it isn't even servicing transports yet!
    // * Unfortunately, calling syncChanges can lead to deadlock during
    //   coordinator restarts if the cluster doesn't have enough backups
    //   to sync the log (see RAM-572). The code below is a partial solution:
    //   only call syncChanges for the very first tablet accepted.  This
    //   doesn't completely eliminate the deadlock, but makes it much less
    //   likely.
    if (!logEverSynced) {
        objectManager.syncChanges();
        logEverSynced = true;
    }

    bool added = tabletManager.addTablet(reqHdr->tableId,
                                         reqHdr->firstKeyHash,
                                         reqHdr->lastKeyHash,
                                         TabletManager::NORMAL);
    if (added) {
        LOG(NOTICE, "Took ownership of new tablet [0x%lx,0x%lx] in tableId %lu",
            reqHdr->firstKeyHash, reqHdr->lastKeyHash, reqHdr->tableId);
    } else {
        TabletManager::Tablet tablet;
        if (tabletManager.getTablet(reqHdr->tableId,
                                    reqHdr->firstKeyHash,
                                    reqHdr->lastKeyHash,
                                    &tablet)) {
            if (tablet.state == TabletManager::NORMAL) {
                LOG(NOTICE, "Told to take ownership of tablet [0x%lx,0x%lx] in "
                    "tableId %lu, but already own [0x%lx,0x%lx]. Returning "
                    "success.", reqHdr->firstKeyHash, reqHdr->lastKeyHash,
                    reqHdr->tableId, tablet.startKeyHash, tablet.endKeyHash);
                return;
            }
        }

        // It's possible we already have the tablet in the RECOVERING state.
        // Try to update it to the NORMAL state to take ownership.
        bool changed = tabletManager.changeState(reqHdr->tableId,
                                                 reqHdr->firstKeyHash,
                                                 reqHdr->lastKeyHash,
                                                 TabletManager::RECOVERING,
                                                 TabletManager::NORMAL);
        if (changed) {
            LOG(NOTICE, "Took ownership of existing tablet [0x%lx,0x%lx] in "
                "tableId %lu in RECOVERING state", reqHdr->firstKeyHash,
                reqHdr->lastKeyHash, reqHdr->tableId);
        } else {
            LOG(WARNING, "Could not take ownership of tablet [0x%lx,0x%lx] in "
                "tableId %lu: overlaps with one or more different ranges.",
                reqHdr->firstKeyHash, reqHdr->lastKeyHash, reqHdr->tableId);

            // TODO(anybody): Do we want a more meaningful error code?
            respHdr->common.status = STATUS_INTERNAL_ERROR;
        }
    }
}

/**
 * Top-level server method to handle the TAKE_INDEXLET_OWNERSHIP request.
 *
 * This RPC is issued by the coordinator when assigning ownership of a
 * indexlet. As far as the coordinator is concerned, the master
 * receiving this rpc owns the indexlet specified and all requests for it
 * will be directed here from now on.
 *
 * \copydetails Service::ping
 */
void
MasterService::takeIndexletOwnership(
        const WireFormat::TakeIndexletOwnership::Request* reqHdr,
        WireFormat::TakeIndexletOwnership::Response* respHdr,
        Rpc* rpc)
{
    uint32_t reqOffset = sizeof32(*reqHdr);
    const void* firstKey =
        rpc->requestPayload->getRange(reqOffset, reqHdr->firstKeyLength);
    reqOffset+=reqHdr->firstKeyLength;
    const void* firstNotOwnedKey = rpc->requestPayload->getRange(
                reqOffset, reqHdr->firstNotOwnedKeyLength);

    bool added = indexletManager.addIndexlet(
                             reqHdr->tableId, reqHdr->indexId,
                             reqHdr->indexletTableId,
                             firstKey, reqHdr->firstKeyLength,
                             firstNotOwnedKey, reqHdr->firstNotOwnedKeyLength);
    if (added) {
        LOG(NOTICE, "Took ownership of indexlet in tableId "
            "%lu indexId %u", reqHdr->tableId, reqHdr->indexId);
    } else if (indexletManager.getIndexlet(
                reqHdr->tableId, reqHdr->indexId,
                firstKey, reqHdr->firstKeyLength,
                firstNotOwnedKey, reqHdr->firstNotOwnedKeyLength) != NULL) {
            LOG(NOTICE, "Told to take ownership of indexlet "
                        "in tableId %lu indexId %u, but already own. "
                        "Returning success.", reqHdr->tableId, reqHdr->indexId);
            return;
    } else {
        LOG(WARNING, "Could not take ownership of indexlet in "
            "tableId %lu indexId %u overlaps with one or more different "
            "ranges.", reqHdr->tableId, reqHdr->indexId);
        // TODO(ouster): Do we want a more meaningful error code?
        respHdr->common.status = STATUS_INTERNAL_ERROR;
    }
}

/**
 * Top-level server method to handle the WRITE request.
 *
 * \copydetails MasterService::read
 */
void
MasterService::write(const WireFormat::Write::Request* reqHdr,
                     WireFormat::Write::Response* respHdr,
                     Rpc* rpc)
{
    // This is a temporary object that has an invalid version and timestamp.
    // An object is created to make here sure the object format does not leak
    // outside the object class. ObjectManager will update the version,
    // timestamp and the checksum
    // This is also used to get key information to update indexes as needed.
    Object object(reqHdr->tableId, 0, 0, *(rpc->requestPayload),
                  sizeof32(*reqHdr));

    // Insert new index entries, if any, before writing object.
    requestInsertIndexEntries(object);

    // Buffer for object being overwritten, so we can remove corresponding
    // index entries later.
    Buffer oldObjectBuffer;

    // Write the object.
    RejectRules rejectRules = reqHdr->rejectRules;
    respHdr->common.status = objectManager.writeObject(object, &rejectRules,
                                                       &respHdr->version,
                                                       &oldObjectBuffer);

    if (respHdr->common.status == STATUS_OK)
        objectManager.syncChanges();

    // Respond to the client RPC now. Removing old index entries can be
    // done asynchronously while maintaining strong consistency.
    rpc->sendReply();
    // reqHdr, respHdr, and rpc are off-limits now!

    // If this is a overwrite, delete old index entries if any.
    if (oldObjectBuffer.getTotalLength() > 0) {
        requestRemoveIndexEntries(oldObjectBuffer);
    }
}

/**
 * Helper function used by write methods in this class to send request
 * for inserting index entries (corresponding to the object being written)
 * to the index servers.
 * \param object
 *      Object for which index entries are to be inserted.
 */
void
MasterService::requestInsertIndexEntries(Object& object)
{
    KeyCount keyCount = object.getKeyCount();
    if (keyCount <= 1)
        return;

    uint64_t tableId = object.getTableId();
    KeyLength primaryKeyLength;
    const void* primaryKey = object.getKey(0, &primaryKeyLength);
    KeyHash primaryKeyHash =
            Key(tableId, primaryKey, primaryKeyLength).getHash();

    Tub<InsertIndexEntryRpc> rpcs[keyCount-1];

    // Send rpcs to all index servers involved.
    for (KeyCount keyIndex = 1; keyIndex <= keyCount - 1; keyIndex++) {
        KeyLength keyLength;
        const void* key = object.getKey(keyIndex, &keyLength);

        if (key != NULL && keyLength > 0) {
            RAMCLOUD_LOG(DEBUG, "Inserting index entry for tableId %lu, "
                                "keyIndex %u, key %s, primaryKeyHash %lu",
                                tableId, keyIndex,
                                string(reinterpret_cast<const char*>(key),
                                       keyLength).c_str(),
                                primaryKeyHash);

            rpcs[keyIndex-1].construct(this, tableId, keyIndex,
                                       key, keyLength, primaryKeyHash);
        }
    }

    // Wait to receive response to all rpcs.
    for (KeyCount keyIndex = 1; keyIndex <= keyCount - 1; keyIndex++) {
        if (rpcs[keyIndex-1]) {
            rpcs[keyIndex-1]->wait();
        }
    }
}

/**
 * Helper function used by remove methods in this class to send request
 * for removing index entries (corresponding to the object being removed)
 * to the index servers.
 * \param objectBuffer
 *      Pointer to the buffer in log for the object for which
 *      index entries are to be deleted.
 */
void
MasterService::requestRemoveIndexEntries(Buffer& objectBuffer)
{
    Object object(objectBuffer);

    KeyCount keyCount = object.getKeyCount();
    if (keyCount <= 1)
        return;

    uint64_t tableId = object.getTableId();
    KeyLength primaryKeyLength;
    const void* primaryKey = object.getKey(0, &primaryKeyLength);
    KeyHash primaryKeyHash =
            Key(tableId, primaryKey, primaryKeyLength).getHash();

    Tub<RemoveIndexEntryRpc> rpcs[keyCount-1];

    // Send rpcs to all index servers involved.
    for (KeyCount keyIndex = 1; keyIndex <= keyCount - 1; keyIndex++) {
        KeyLength keyLength;
        const void* key = object.getKey(keyIndex, &keyLength);

        if (key != NULL && keyLength > 0) {
            RAMCLOUD_LOG(DEBUG, "Removing index entry for tableId %lu, "
                                "keyIndex %u, key %s, primaryKeyHash %lu",
                                tableId, keyIndex,
                                string(reinterpret_cast<const char*>(key),
                                       keyLength).c_str(),
                                primaryKeyHash);

            rpcs[keyIndex-1].construct(this, tableId, keyIndex,
                                       key, keyLength, primaryKeyHash);
        }
    }

    // Wait to receive response to all rpcs.
    for (KeyCount keyIndex = 1; keyIndex <= keyCount - 1; keyIndex++) {
        if (rpcs[keyIndex-1]) {
            rpcs[keyIndex-1]->wait();
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
/////Recovery related code. This should eventually move into its own file./////
///////////////////////////////////////////////////////////////////////////////

namespace MasterServiceInternal {
/**
 * Each object of this class is responsible for fetching recovery data
 * for a single segment from a single backup.
 */
class RecoveryTask {
  PUBLIC:
    RecoveryTask(Context* context,
                 uint64_t recoveryId,
                 ServerId masterId,
                 uint64_t partitionId,
                 MasterService::Replica& replica)
        : context(context)
        , recoveryId(recoveryId)
        , masterId(masterId)
        , partitionId(partitionId)
        , replica(replica)
        , response()
        , startTime(Cycles::rdtsc())
        , rpc()
    {
        rpc.construct(context, replica.backupId,
                      recoveryId, masterId, replica.segmentId,
                      partitionId, &response);
    }
    ~RecoveryTask()
    {
        if (rpc && !rpc->isReady()) {
            LOG(WARNING, "Task destroyed while RPC active: segment %lu, "
                    "server %s", replica.segmentId,
                    context->serverList->toString(replica.backupId).c_str());
        }
    }
    void resend() {
        LOG(DEBUG, "Resend %lu", replica.segmentId);
        response.reset();
        rpc.construct(context, replica.backupId,
                      recoveryId, masterId, replica.segmentId,
                      partitionId, &response);
    }
    Context* context;
    uint64_t recoveryId;
    ServerId masterId;
    uint64_t partitionId;
    MasterService::Replica& replica;
    Buffer response;
    const uint64_t startTime;
    Tub<GetRecoveryDataRpc> rpc;
    DISALLOW_COPY_AND_ASSIGN(RecoveryTask);
};
} // namespace MasterServiceInternal

using namespace MasterServiceInternal; // NOLINT

/**
 * Look through \a backups and ensure that for each segment id that appears
 * in the list that at least one copy of that segment was replayed.
 *
 * \param masterId
 *      The id of the crashed master this recovery master is recovering for.
 *      Only used for logging detailed log information on failure.
 * \param partitionId
 *      The id of the partition of the crashed master this recovery master is
 *      recovering. Only used for logging detailed log information on failure.
 * \param replicas
 *      The list of replicas and their statuses to be checked to ensure
 *      recovery of this partition was successful.
 * \throw SegmentRecoveryFailedException
 *      If some segment was not recovered and the recovery master is not
 *      a valid replacement for the crashed master.
 */
void
MasterService::detectSegmentRecoveryFailure(
            const ServerId masterId,
            const uint64_t partitionId,
            const vector<MasterService::Replica>& replicas)
{
    std::unordered_set<uint64_t> failures;
    foreach (const auto& replica, replicas) {
        switch (replica.state) {
        case MasterService::Replica::State::OK:
            failures.erase(replica.segmentId);
            break;
        case MasterService::Replica::State::FAILED:
            failures.insert(replica.segmentId);
            break;
        case MasterService::Replica::State::WAITING:
        case MasterService::Replica::State::NOT_STARTED:
        default:
            assert(false);
            break;
        }
    }
    if (!failures.empty()) {
        LOG(ERROR, "Recovery master failed to recover master %lu "
            "partition %lu", *masterId, partitionId);
        foreach (auto segmentId, failures)
            LOG(ERROR, "Unable to recover segment %lu", segmentId);
        throw SegmentRecoveryFailedException(HERE);
    }
}

/**
 * Helper for public recover() method.
 * Collect all the filtered log segments from backups for a set of tablets
 * formerly belonging to a crashed master which is being recovered and pass
 * them to the recovery master to have them replayed.
 *
 * \param recoveryId
 *      Id of the recovery this recovery master was performing.
 * \param masterId
 *      The id of the crashed master for which recoveryMaster will be taking
 *      over ownership of tablets.
 * \param partitionId
 *      The partition id of tablets of the crashed master that this master
 *      is recovering.
 * \param replicas
 *      A list specifying for each segmentId a backup who can provide a
 *      filtered recovery data segment. A particular segment may be listed more
 *      than once if it has multiple viable backups.
 * \param highestBTreeIdMap
 *      A unordered map that keeps track of the highest used BTree ID in
 *      each indexlet table.
 * \throw SegmentRecoveryFailedException
 *      If some segment was not recovered and the recovery master is not
 *      a valid replacement for the crashed master.
 */
void
MasterService::recover(uint64_t recoveryId,
                       ServerId masterId,
                       uint64_t partitionId,
                       vector<Replica>& replicas,
                       std::unordered_map<uint64_t, uint64_t>&
                           highestBTreeIdMap)
{
    /* Overview of the internals of this method and its structures.
     *
     * The main data structure is "replicas".  It works like a
     * scoreboard, tracking which segments have requests to backup
     * servers in-flight for data, which have been replayed, and
     * which have failed and must be replayed by another entry in
     * the table.
     *
     * replicasEnd is an iterator to the end of the segment replica list
     * which aids in tracking when the function is out of work.
     *
     * notStarted tracks the furtherest entry into the list which
     * has not been requested from a backup yet (State::NOT_STARTED).
     *
     * Here is a sample of what the structure might look like
     * during execution:
     *
     * backupId     segmentId  state
     * --------     ---------  -----
     *   8            99       OK
     *   3            88       FAILED
     *   1            77       OK
     *   2            77       OK
     *   6            88       WAITING
     *   2            66       NOT_STARTED  <- notStarted
     *   3            55       WAITING
     *   1            66       NOT_STARTED
     *   7            66       NOT_STARTED
     *   3            99       OK
     *
     * The basic idea is, the code kicks off up to some fixed
     * number worth of RPCs marking them WAITING starting from the
     * top of the list working down.  When a response comes it
     * marks the entry as FAILED if there was an error fetching or
     * replaying it. If it succeeded in replaying, though then ALL
     * entries for that segment_id are marked OK. (This is done
     * by marking the entry itself and then iterating starting
     * at "notStarted" and checking each row for a match).
     *
     * One other structure "runningSet" tracks which segment_ids
     * have RPCs in-flight.  When starting new RPCs rows that
     * have a segment_id that is in the set are skipped over.
     * However, since the row is still NOT_STARTED, notStarted
     * must point to it or to an earlier entry, so the entry
     * will be revisited in the case the other in-flight request
     * fails.  If the other request succeeds then the previously
     * skipped entry is marked OK and notStarted is advanced (if
     * possible).
     */
    uint64_t usefulTime = 0;
    uint64_t start = Cycles::rdtsc();
    LOG(NOTICE, "Recovering master %s, partition %lu, %lu replicas available",
        masterId.toString().c_str(), partitionId, replicas.size());

    std::unordered_set<uint64_t> runningSet;
    Tub<RecoveryTask> tasks[4];
    uint32_t activeRequests = 0;

    auto notStarted = replicas.begin();
    auto replicasEnd = replicas.end();

    // The SideLog we'll append recovered entries to. It will be committed after
    // replay completes on all segments, making all of the recovered data
    // durable.
    SideLog sideLog(objectManager.getLog());

    // Start RPCs
    auto replicaIt = notStarted;
    foreach (auto& task, tasks) {
        while (!task) {
            if (replicaIt == replicasEnd)
                goto doneStartingInitialTasks;
            auto& replica = *replicaIt;
            LOG(DEBUG, "Starting getRecoveryData from %s for segment %lu "
                "on channel %ld (initial round of RPCs)",
                context->serverList->toString(replica.backupId).c_str(),
                replica.segmentId,
                &task - &tasks[0]);
            task.construct(context, recoveryId, masterId, partitionId,
                           replica);
            replica.state = Replica::State::WAITING;
            runningSet.insert(replica.segmentId);
            ++metrics->master.segmentReadCount;
            ++activeRequests;
            ++replicaIt;
            while (replicaIt != replicasEnd &&
                   contains(runningSet, replicaIt->segmentId)) {
                ++replicaIt;
            }
        }
    }
  doneStartingInitialTasks:

    // As RPCs complete, process them and start more
    Tub<CycleCounter<RawMetric>> readStallTicks;

    bool gotFirstGRD = false;

    std::unordered_multimap<uint64_t, Replica*> segmentIdToBackups;
    foreach (Replica& replica, replicas)
        segmentIdToBackups.insert({replica.segmentId, &replica});

    while (activeRequests) {
        if (!readStallTicks)
            readStallTicks.construct(&metrics->master.segmentReadStallTicks);
        objectManager.getReplicaManager()->proceed();
        foreach (auto& task, tasks) {
            if (!task)
                continue;
            if (!task->rpc->isReady())
                continue;
            readStallTicks.destroy();
            LOG(DEBUG, "Waiting on recovery data for segment %lu from %s",
                task->replica.segmentId,
                context->serverList->toString(task->replica.backupId).c_str());
            try {
                Segment::Certificate certificate = task->rpc->wait();
                task->rpc.destroy();
                uint64_t grdTime = Cycles::rdtsc() - task->startTime;
                metrics->master.segmentReadTicks += grdTime;

                if (!gotFirstGRD) {
                    metrics->master.replicationBytes =
                        0 - metrics->transport.transmit.byteCount;
                    metrics->master.replicationTransmitCopyTicks =
                        0 - metrics->transport.transmit.copyTicks;
                    metrics->master.replicationTransmitActiveTicks =
                        0 - metrics->transport.infiniband.transmitActiveTicks;
                    metrics->master.replicationPostingWriteRpcTicks = 0;
                    metrics->master.replayMemoryReadBytes = 0 -
                        // tx
                        (metrics->master.replicationBytes +
                        // tx copy
                         metrics->master.replicationBytes +
                        // backup write copy
                         metrics->backup.writeCopyBytes +
                        // read from filtering objects
                         metrics->backup.storageReadBytes +
                        // log append copy
                         metrics->master.liveObjectBytes);
                    metrics->master.replayMemoryWrittenBytes = 0 -
                        // tx copy
                        (metrics->master.replicationBytes +
                        // backup write copy
                         metrics->backup.writeCopyBytes +
                        // disk read into memory
                         metrics->backup.storageReadBytes +
                        // copy from filtering objects
                         metrics->backup.storageReadBytes +
                        // rx into memory
                         metrics->transport.receive.byteCount +
                        // log append copy
                         metrics->master.liveObjectBytes);
                    gotFirstGRD = true;
                }
                if (LOG_RECOVERY_REPLICATION_RPC_TIMING) {
                    LOG(DEBUG, "@%7lu: Got getRecoveryData response from %s, "
                        "took %.1f us on channel %ld",
                        Cycles::toMicroseconds(Cycles::rdtsc() -
                            ReplicatedSegment::recoveryStart),
                        context->serverList->toString(
                            task->replica.backupId).c_str(),
                        Cycles::toSeconds(grdTime)*1e06,
                        &task - &tasks[0]);
                }

                uint32_t responseLen = task->response.getTotalLength();
                metrics->master.segmentReadByteCount += responseLen;
                uint64_t startUseful = Cycles::rdtsc();
                SegmentIterator it(task->response.getRange(0, responseLen),
                                   responseLen, certificate);
                it.checkMetadataIntegrity();
                if (LOG_RECOVERY_REPLICATION_RPC_TIMING) {
                    LOG(DEBUG, "@%7lu: Replaying segment %lu with length %u",
                        Cycles::toMicroseconds(Cycles::rdtsc() -
                            ReplicatedSegment::recoveryStart),
                        task->replica.segmentId, responseLen);
                }
                objectManager.replaySegment(&sideLog, it, highestBTreeIdMap);
                usefulTime += Cycles::rdtsc() - startUseful;
                TEST_LOG("Segment %lu replay complete",
                         task->replica.segmentId);
                if (LOG_RECOVERY_REPLICATION_RPC_TIMING) {
                    LOG(DEBUG, "@%7lu: Replaying segment %lu done",
                        Cycles::toMicroseconds(Cycles::rdtsc() -
                            ReplicatedSegment::recoveryStart),
                        task->replica.segmentId);
                }

                runningSet.erase(task->replica.segmentId);
                // Mark this and any other entries for this segment as OK.
                LOG(DEBUG, "Checking %s off the list for %lu",
                    context->serverList->toString(
                        task->replica.backupId).c_str(),
                    task->replica.segmentId);
                task->replica.state = Replica::State::OK;
                foreach (auto it, segmentIdToBackups.equal_range(
                                        task->replica.segmentId)) {
                    Replica& otherReplica = *it.second;
                    LOG(DEBUG, "Checking %s off the list for %lu",
                        context->serverList->toString(
                            otherReplica.backupId).c_str(),
                        otherReplica.segmentId);
                    otherReplica.state = Replica::State::OK;
                }
            } catch (const SegmentIteratorException& e) {
                LOG(WARNING, "Recovery segment for segment %lu corrupted; "
                    "trying next backup: %s", task->replica.segmentId,
                    e.what());
                task->replica.state = Replica::State::FAILED;
                runningSet.erase(task->replica.segmentId);
            } catch (const ServerNotUpException& e) {
                LOG(WARNING, "No record of backup %s, trying next backup",
                    task->replica.backupId.toString().c_str());
                task->replica.state = Replica::State::FAILED;
                runningSet.erase(task->replica.segmentId);
            } catch (const ClientException& e) {
                LOG(WARNING, "getRecoveryData failed on %s, "
                    "trying next backup; failure was: %s",
                    context->serverList->toString(
                        task->replica.backupId).c_str(),
                    e.str().c_str());
                task->replica.state = Replica::State::FAILED;
                runningSet.erase(task->replica.segmentId);
            }

            task.destroy();

            // move notStarted up as far as possible
            while (notStarted != replicasEnd &&
                   notStarted->state != Replica::State::NOT_STARTED) {
                ++notStarted;
            }

            // Find the next NOT_STARTED entry that isn't in-flight
            // from another entry.
            auto replicaIt = notStarted;
            while (!task && replicaIt != replicasEnd) {
                while (replicaIt->state != Replica::State::NOT_STARTED ||
                       contains(runningSet, replicaIt->segmentId)) {
                    ++replicaIt;
                    if (replicaIt == replicasEnd)
                        goto outOfHosts;
                }
                Replica& replica = *replicaIt;
                LOG(DEBUG, "Starting getRecoveryData from %s for segment %lu "
                    "on channel %ld (after RPC completion)",
                    context->serverList->toString(replica.backupId).c_str(),
                    replica.segmentId,
                    &task - &tasks[0]);
                task.construct(context, recoveryId,
                               masterId, partitionId, replica);
                replica.state = Replica::State::WAITING;
                runningSet.insert(replica.segmentId);
                ++metrics->master.segmentReadCount;
            }
          outOfHosts:
            if (!task)
                --activeRequests;
        }
    }
    readStallTicks.destroy();

    detectSegmentRecoveryFailure(masterId, partitionId, replicas);

    {
        CycleCounter<RawMetric> logSyncTicks(&metrics->master.logSyncTicks);
        LOG(NOTICE, "Committing the SideLog...");
        metrics->master.logSyncBytes =
            0 - metrics->transport.transmit.byteCount;
        metrics->master.logSyncTransmitCopyTicks =
            0 - metrics->transport.transmit.copyTicks;
        metrics->master.logSyncTransmitActiveTicks =
            0 - metrics->transport.infiniband.transmitActiveTicks;
        metrics->master.logSyncPostingWriteRpcTicks =
            0 - metrics->master.replicationPostingWriteRpcTicks;
        sideLog.commit();
        metrics->master.logSyncBytes += metrics->transport.transmit.byteCount;
        metrics->master.logSyncTransmitCopyTicks +=
            metrics->transport.transmit.copyTicks;
        metrics->master.logSyncTransmitActiveTicks +=
            metrics->transport.infiniband.transmitActiveTicks;
        metrics->master.logSyncPostingWriteRpcTicks +=
            metrics->master.replicationPostingWriteRpcTicks;
        LOG(NOTICE, "SideLog finished committing (data is durable).");
    }

    metrics->master.replicationBytes += metrics->transport.transmit.byteCount;
    metrics->master.replicationTransmitCopyTicks +=
        metrics->transport.transmit.copyTicks;
    // See the lines with "0 -" above to get the purpose of each of these
    // fields in this metric.
    metrics->master.replayMemoryReadBytes +=
        (metrics->master.replicationBytes +
         metrics->master.replicationBytes +
         metrics->backup.writeCopyBytes +
         metrics->backup.storageReadBytes +
         metrics->master.liveObjectBytes);
    metrics->master.replayMemoryWrittenBytes +=
        (metrics->master.replicationBytes +
         metrics->backup.writeCopyBytes +
         metrics->backup.storageReadBytes +
         metrics->transport.receive.byteCount +
         metrics->backup.storageReadBytes +
         metrics->master.liveObjectBytes);
    metrics->master.replicationTransmitActiveTicks +=
        metrics->transport.infiniband.transmitActiveTicks;

    double totalSecs = Cycles::toSeconds(Cycles::rdtsc() - start);
    double usefulSecs = Cycles::toSeconds(usefulTime);
    LOG(NOTICE, "Recovery complete, took %.1f ms, useful replaying "
        "time %.1f ms (%.1f%% effective)",
        totalSecs * 1e03,
        usefulSecs * 1e03,
        100 * usefulSecs / totalSecs);
}

/**
 * Thrown during recovery in recoverSegment when a log append fails. Caught
 * by recover() which aborts the recovery cleanly and notifies the coordinator
 * that this master could not recover the partition.
 */
struct OutOfSpaceException : public Exception {
    explicit OutOfSpaceException(const CodeLocation& where)
        : Exception(where) {}
};

/**
 * Top-level server method to handle the RECOVER request.
 * \copydetails Service::ping
 */
void
MasterService::recover(const WireFormat::Recover::Request* reqHdr,
                       WireFormat::Recover::Response* respHdr,
                       Rpc* rpc)
{
    ReplicatedSegment::recoveryStart = Cycles::rdtsc();
    CycleCounter<RawMetric> recoveryTicks(&metrics->master.recoveryTicks);
    metrics->master.recoveryCount++;
    metrics->master.replicas = objectManager.getReplicaManager()->numReplicas;

    uint64_t recoveryId = reqHdr->recoveryId;
    ServerId crashedServerId(reqHdr->crashedServerId);
    uint64_t partitionId = reqHdr->partitionId;
    if (partitionId == ~0u)
        DIE("Recovery master %s got super secret partition id; killing self.",
            serverId.toString().c_str());
    ProtoBuf::RecoveryPartition recoveryPartition;
    ProtoBuf::parseFromResponse(rpc->requestPayload, sizeof(*reqHdr),
                                reqHdr->tabletsLength, &recoveryPartition);

    uint32_t offset = sizeof32(*reqHdr) + reqHdr->tabletsLength;
    vector<Replica> replicas;
    replicas.reserve(reqHdr->numReplicas);
    for (uint32_t i = 0; i < reqHdr->numReplicas; ++i) {
        const WireFormat::Recover::Replica* replicaLocation =
            rpc->requestPayload->getOffset<WireFormat::Recover::Replica>(
            offset);
        offset += sizeof32(WireFormat::Recover::Replica);
        Replica replica(replicaLocation->backupId,
                        replicaLocation->segmentId);
        replicas.push_back(replica);
    }
    LOG(DEBUG, "Starting recovery %lu for crashed master %s; "
        "recovering partition %lu (see user_data) of the following "
        "partitions:\n%s",
        recoveryId, crashedServerId.toString().c_str(), partitionId,
        recoveryPartition.DebugString().c_str());
    rpc->sendReply();

    // reqHdr, respHdr, and rpc are off-limits now

    // Install tablets we are recovering and mark them as such (we don't
    // own them yet).
    foreach (const ProtoBuf::Tablets::Tablet& newTablet,
             recoveryPartition.tablet()) {
        bool added = tabletManager.addTablet(newTablet.table_id(),
                                             newTablet.start_key_hash(),
                                             newTablet.end_key_hash(),
                                             TabletManager::RECOVERING);
        if (!added) {
            throw Exception(HERE, format("Cannot recover tablet that overlaps "
                "an already existing one (tablet to recover: %lu "
                "range [0x%lx,0x%lx], current tablet map: %s)",
                newTablet.table_id(),
                newTablet.start_key_hash(),
                newTablet.end_key_hash(),
                tabletManager.toString().c_str()));
        }
    }

    // Record the log position before recovery started.
    Log::Position headOfLog = objectManager.getLog()->rollHeadOver();

    // Recover Segments, firing ObjectManager::replaySegment for each one.
    bool successful = false;
    try {
        // This unordered_map is used to keep track of the highest BTree id
        // in every indexlet table
        std::unordered_map<uint64_t, uint64_t> highestBTreeIdMap;
        foreach (const ProtoBuf::Indexlets::Indexlet& indexlet,
                 recoveryPartition.indexlet()) {
            highestBTreeIdMap[indexlet.indexlettable_id()] = 0;
        }
        recover(recoveryId, crashedServerId, partitionId, replicas,
                highestBTreeIdMap);
        // Install indexlets we are recovering
        foreach (const ProtoBuf::Indexlets::Indexlet& newIndexlet,
                 recoveryPartition.indexlet()) {
            LOG(NOTICE, "Starting recovery %lu for crashed indexlet %d",
                recoveryId, newIndexlet.index_id());
            bool added = indexletManager.addIndexlet(newIndexlet,
                         highestBTreeIdMap[newIndexlet.indexlettable_id()]);
            if (!added) {
                throw Exception(HERE, format("Indexlet already exists."));
            }
        }
        successful = true;
    } catch (const SegmentRecoveryFailedException& e) {
        // Recovery wasn't successful.
    } catch (const OutOfSpaceException& e) {
        // Recovery wasn't successful.
    }

    // Once the coordinator and the recovery master agree that the
    // master has taken over for the tablets it can update its tables
    // and begin serving requests.

    // Update the recoveryTablets to reflect the fact that this master is
    // going to try to become the owner. The coordinator will assign final
    // ownership in response to the RECOVERY_MASTER_FINISHED rpc (i.e.
    // we'll only be owners if the call succeeds. It could fail if the
    // coordinator decided to recover these tablets elsewhere instead).
    foreach (ProtoBuf::Tablets::Tablet& tablet,
             *recoveryPartition.mutable_tablet()) {
        LOG(NOTICE, "set tablet %lu %lu %lu to locator %s, id %s",
                 tablet.table_id(), tablet.start_key_hash(),
                 tablet.end_key_hash(), config->localLocator.c_str(),
                 serverId.toString().c_str());
        tablet.set_service_locator(config->localLocator);
        tablet.set_server_id(serverId.getId());
        tablet.set_ctime_log_head_id(headOfLog.getSegmentId());
        tablet.set_ctime_log_head_offset(headOfLog.getSegmentOffset());
    }
    foreach (ProtoBuf::Indexlets::Indexlet& indexlet,
             *recoveryPartition.mutable_indexlet()) {
        LOG(NOTICE, "set indexlet %lu to locator %s, id %s",
                 indexlet.table_id(), config->localLocator.c_str(),
                 serverId.toString().c_str());
        indexlet.set_service_locator(config->localLocator);
        indexlet.set_server_id(serverId.getId());
    }

    LOG(NOTICE, "Reporting completion of recovery %lu", reqHdr->recoveryId);
    bool cancelRecovery =
        CoordinatorClient::recoveryMasterFinished(context, recoveryId,
                                                  serverId, &recoveryPartition,
                                                  successful);
    if (!cancelRecovery) {
        // Ok - we're expected to be serving now. Mark recovered tablets
        // as normal so we can handle clients.
        foreach (const ProtoBuf::Tablets::Tablet& tablet,
                     recoveryPartition.tablet()) {
            bool changed = tabletManager.changeState(tablet.table_id(),
                                                     tablet.start_key_hash(),
                                                     tablet.end_key_hash(),
                                                     TabletManager::RECOVERING,
                                                     TabletManager::NORMAL);
            if (!changed) {
                throw FatalError(HERE, format("Could not change recovering "
                    "tablet's state to NORMAL (%lu range [%lu,%lu])",
                        tablet.table_id(),
                        tablet.start_key_hash(),
                        tablet.end_key_hash()));
            }
        }
    } else {
        LOG(WARNING, "Failed to recover partition for recovery %lu; "
            "aborting recovery on this recovery master", recoveryId);
        // If recovery failed then clean up all objects written by
        // recovery before starting to serve requests again.
        foreach (const ProtoBuf::Tablets::Tablet& tablet,
                 recoveryPartition.tablet()) {
            bool deleted = tabletManager.deleteTablet(tablet.table_id(),
                                                      tablet.start_key_hash(),
                                                      tablet.end_key_hash());
            if (!deleted) {
                throw FatalError(HERE, format("Could not delete recovery "
                    "tablet (%lu range [%lu,%lu]). It disappeared!?",
                        tablet.table_id(),
                        tablet.start_key_hash(),
                        tablet.end_key_hash()));
            }
        }
        foreach (const ProtoBuf::Indexlets::Indexlet& indexlet,
                 recoveryPartition.indexlet()) {
            bool deleted = indexletManager.deleteIndexlet(indexlet);
            if (!deleted) {
                throw FatalError(HERE, format("Could not delete recovery "
                    "indexlet (%lu, %u, %s, %s). It disappeared!?",
                        indexlet.table_id(),
                        indexlet.index_id(),
                        indexlet.start_key().c_str(),
                        indexlet.end_key().c_str()));
            }
        }
        objectManager.removeOrphanedObjects();
    }
}

} // namespace RAMCloud
