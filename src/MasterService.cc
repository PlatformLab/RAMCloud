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

#include <boost/unordered_set.hpp>

#include "Buffer.h"
#include "ClientException.h"
#include "Cycles.h"
#include "Dispatch.h"
#include "ShortMacros.h"
#include "MasterService.h"
#include "Metrics.h"
#include "Tub.h"
#include "ProtoBuf.h"
#include "Rpc.h"
#include "Segment.h"
#include "ServiceManager.h"
#include "Transport.h"
#include "Will.h"

namespace RAMCloud {

// --- MasterService ---

void objectEvictionCallback(LogEntryHandle handle,
                            const LogTime,
                            void* cookie);
void tombstoneEvictionCallback(LogEntryHandle handle,
                               const LogTime,
                               void* cookie);

/**
 * Construct a MasterService.
 *
 * \param config
 *      Contains various parameters that configure the operation of
 *      this server.
 * \param coordinator
 *      A client to the coordinator for the RAMCloud this Master is in.
 * \param replicas
 *      The number of backups required before writes are considered safe.
 */
MasterService::MasterService(const ServerConfig config,
                             CoordinatorClient* coordinator,
                             uint32_t replicas)
    : config(config)
    , coordinator(coordinator)
    , serverId()
    , backup(coordinator, serverId, replicas)
    , bytesWritten(0)
    , log(serverId, config.logBytes, Segment::SEGMENT_SIZE, &backup)
    , objectMap(config.hashTableBytes /
        HashTable<LogEntryHandle>::bytesPerCacheLine())
    , tablets()
{
    log.registerType(LOG_ENTRY_TYPE_OBJ, objectEvictionCallback, this);
    log.registerType(LOG_ENTRY_TYPE_OBJTOMB, tombstoneEvictionCallback, this);
}

MasterService::~MasterService()
{
    std::set<Table*> tables;
    foreach (const ProtoBuf::Tablets::Tablet& tablet, tablets.tablet())
        tables.insert(reinterpret_cast<Table*>(tablet.user_data()));
    foreach (Table* table, tables)
        delete table;
}

void
MasterService::dispatch(RpcOpcode opcode, Rpc& rpc)
{
    switch (opcode) {
        case CreateRpc::opcode:
            callHandler<CreateRpc, MasterService,
                        &MasterService::create>(rpc);
            break;
        case FillWithTestDataRpc::opcode:
            callHandler<FillWithTestDataRpc, MasterService,
                        &MasterService::fillWithTestData>(rpc);
            break;
        case MultiReadRpc::opcode:
            callHandler<MultiReadRpc, MasterService,
                        &MasterService::multiRead>(rpc);
            break;
        case ReadRpc::opcode:
            callHandler<ReadRpc, MasterService,
                        &MasterService::read>(rpc);
            break;
        case RecoverRpc::opcode:
            callHandler<RecoverRpc, MasterService,
                        &MasterService::recover>(rpc);
            break;
        case RemoveRpc::opcode:
            callHandler<RemoveRpc, MasterService,
                        &MasterService::remove>(rpc);
            break;
        case RereplicateSegmentsRpc::opcode:
            callHandler<RereplicateSegmentsRpc, MasterService,
                        &MasterService::rereplicateSegments>(rpc);
            break;
        case SetTabletsRpc::opcode:
            callHandler<SetTabletsRpc, MasterService,
                        &MasterService::setTablets>(rpc);
            break;
        case WriteRpc::opcode:
            callHandler<WriteRpc, MasterService,
                        &MasterService::write>(rpc);
            break;
        default:
            throw UnimplementedRequestError(HERE);
    }
}


/**
 * Make connections with the coordinator and backups, so that the service
 * is ready to begin handling requests.
 */
void
MasterService::init()
{
    // Permit a NULL coordinator for testing/benchmark purposes.
    if (coordinator) {
        // Open a session with each of the backups so that this doesn't slow
        // down replication later.
        ProtoBuf::ServerList backups;
        coordinator->getBackupList(backups);
        foreach(auto& backup, backups.server())
            transportManager.getSession(backup.service_locator().c_str());

        // Enlist with the coordinator.
        serverId.construct(coordinator->enlistServer(MASTER,
                                                     config.localLocator));
        LOG(NOTICE, "My server ID is %lu", *serverId);
    }
}

/**
 * Top-level server method to handle the CREATE request.
 * See the documentation for the corresponding method in RamCloudClient for
 * complete information about what this request does.
 * \copydetails Service::ping
 */
void
MasterService::create(const CreateRpc::Request& reqHdr,
                      CreateRpc::Response& respHdr,
                      Rpc& rpc)
{
    Table& t(getTable(reqHdr.tableId, ~0UL));
    uint64_t id = t.AllocateKey(&objectMap);

    RejectRules rejectRules;
    memset(&rejectRules, 0, sizeof(RejectRules));
    rejectRules.exists = 1;

    storeData(reqHdr.tableId, id, &rejectRules,
              &rpc.requestPayload, sizeof(reqHdr), reqHdr.length,
              &respHdr.version,
              reqHdr.async);
    respHdr.id = id;
}

/**
 * Fill this server with test data. Objects are added to all
 * existing tables in a round-robin fashion.
 * \copydetails Service::ping
 */
void
MasterService::fillWithTestData(const FillWithTestDataRpc::Request& reqHdr,
                                FillWithTestDataRpc::Response& respHdr,
                                Rpc& rpc)
{
    LOG(NOTICE, "Filling with %u objects of %u bytes each in %u tablets",
        reqHdr.numObjects, reqHdr.objectSize, tablets.tablet_size());

    Table* tables[tablets.tablet_size()];
    uint32_t i = 0;
    foreach (const ProtoBuf::Tablets::Tablet& tablet, tablets.tablet())
        tables[i++] = reinterpret_cast<Table*>(tablet.user_data());

    // safe? doubtful. simple? you bet.
    char data[reqHdr.objectSize];
    memset(data, 0xcc, reqHdr.objectSize);
    Buffer buffer;
    Buffer::Chunk::appendToBuffer(&buffer, data, reqHdr.objectSize);

    RejectRules rejectRules;
    memset(&rejectRules, 0, sizeof(RejectRules));
    rejectRules.exists = 1;

    for (uint32_t objects = 0; objects < reqHdr.numObjects; objects++) {
        int t = objects % tablets.tablet_size();
        uint64_t newVersion;
        storeData(tables[t]->getId(), tables[t]->AllocateKey(&objectMap),
                  &rejectRules, &buffer, 0, reqHdr.objectSize,
                  &newVersion, true);
        if ((objects % 50) == 0) {
            backup.proceed();
        }
    }

    log.sync();

    LOG(NOTICE, "Done writing objects.");
}

/**
 * Top-level server method to handle the MULTIREAD request.
 *
 * \copydetails Service::ping
 */
void
MasterService::multiRead(const MultiReadRpc::Request& reqHdr,
                         MultiReadRpc::Response& respHdr,
                         Rpc& rpc)
{
    uint32_t numRequests = reqHdr.count;
    uint32_t reqOffset = downCast<uint32_t>(sizeof(reqHdr));

    respHdr.count = numRequests;

    // Each iteration extracts one request from request rpc, finds the
    // corresponding object, and appends the response to the response rpc.
    for (uint32_t i = 0; i < numRequests; i++) {
        const MultiReadRpc::Request::Part *currentReq =
              rpc.requestPayload.getOffset<MultiReadRpc::Request::Part>(
              reqOffset);
        reqOffset += downCast<uint32_t>(sizeof(MultiReadRpc::Request::Part));

        Status* status = new(&rpc.replyPayload, APPEND) Status(STATUS_OK);
        // We must note the status if the table does not exist. Also, we might
        // have an entry in the hash table that's invalid because its tablet no
        // longer lives here.
        try{
            getTable(currentReq->tableId, currentReq->id);
        }
        catch(TableDoesntExistException &e) {
            *status = STATUS_TABLE_DOESNT_EXIST;
            continue;
        }
        LogEntryHandle handle = objectMap.lookup(currentReq->tableId,
                                                 currentReq->id);
        if (handle == NULL || handle->type() != LOG_ENTRY_TYPE_OBJ) {
             *status = STATUS_OBJECT_DOESNT_EXIST;
             continue;
        }

        const SegmentEntry* entry = reinterpret_cast<
                                    const SegmentEntry*>(handle);
        Buffer::Chunk::appendToBuffer(&rpc.replyPayload, entry,
                                      downCast<uint32_t>(sizeof(SegmentEntry))
                                      + handle->length());
    }
}

/**
 * Top-level server method to handle the READ request.
 * \copydetails create
 */
void
MasterService::read(const ReadRpc::Request& reqHdr,
                   ReadRpc::Response& respHdr,
                   Rpc& rpc)
{
    if (reqHdr.id == TOTAL_READ_REQUESTS_OBJID) {
        new(&rpc.replyPayload, APPEND) ServerStats(serverStats);
        respHdr.length = sizeof(serverStats);
        memset(&serverStats, 0, sizeof(serverStats));
        return; // TODO(nandu) - if an actual object uses this objid
                // then we do not return its real value back. Should
                // change this to use an RPC other than read. Write to
                // this object has undesirable behavior too.
    }
    CycleCounter<uint64_t> timeThisRead;

    // We must throw an exception if the table does not exist. Also, we might
    // have an entry in the hash table that's invalid because its tablet no
    // longer lives here.
    getTable(reqHdr.tableId, reqHdr.id);

    LogEntryHandle handle = objectMap.lookup(reqHdr.tableId, reqHdr.id);
    if (handle == NULL || handle->type() != LOG_ENTRY_TYPE_OBJ) {
        throw ObjectDoesntExistException(HERE);
    }

    const Object* obj = handle->userData<Object>();
    respHdr.version = obj->version;
    rejectOperation(&reqHdr.rejectRules, obj->version);
    Buffer::Chunk::appendToBuffer(&rpc.replyPayload,
        obj->data, obj->dataLength(handle->length()));
    // TODO(ongaro): We'll need a new type of Chunk to block the cleaner
    // from scribbling over obj->data.
    respHdr.length = obj->dataLength(handle->length());
    serverStats.totalReadRequests++;
    serverStats.totalReadNanos += Cycles::toNanoseconds(timeThisRead.stop());
}

/**
 * This method allocates an ObjectTombstone on the heap, initialises it
 * to the given ``srcTomb'', and prepends a SegmentEntry structure to make
 * it look as though it's a valid Log entry. The purpose is so that we can
 * avoid writing ObjectTombstones to the Log on recovery while still being
 * able to put them in the regular HashTable.
 *
 * This is an ugly hack, but I don't see a better way right now. In the
 * future, we may want to use a temporary backup-less Log and write into
 * that, but we'd need that Log, as well as our main Log, to use a common
 * pool of Segments, since we don't know how many tombstones we might
 * encounter.
 *
 * \param[in] srcTomb
 *      A source ObjectTombstone to copy into our allocated tombstone.
 * \return
 *      A valid LogEntryHandle to the ObjectTombstone allocated.
 */
LogEntryHandle
MasterService::allocRecoveryTombstone(const ObjectTombstone* srcTomb)
{
    uint8_t* buf = new uint8_t[sizeof(SegmentEntry) + sizeof(ObjectTombstone)];
    SegmentEntry* se = reinterpret_cast<SegmentEntry*>(buf);
    se->type = LOG_ENTRY_TYPE_OBJTOMB;
    se->length = sizeof(ObjectTombstone);
    memcpy(buf + sizeof(SegmentEntry), srcTomb, sizeof(ObjectTombstone));
    return reinterpret_cast<LogEntryHandle>(buf);
}

/**
 * Free the tombstone allocated in #allocRecoveryTombstone().
 */
void
MasterService::freeRecoveryTombstone(LogEntryHandle handle)
{
    const uint8_t* p = reinterpret_cast<const uint8_t*>(handle);
    delete[] p;
}

/**
 * Callback used to purge the tombstones from the hash table. Invoked by
 * HashTable::forEach.
 */
void
recoveryCleanup(LogEntryHandle maybeTomb, uint8_t type, void *cookie)
{
    if (maybeTomb->type() == LOG_ENTRY_TYPE_OBJTOMB) {
        const ObjectTombstone *tomb = maybeTomb->userData<ObjectTombstone>();
        MasterService *server = reinterpret_cast<MasterService*>(cookie);
        bool r = server->objectMap.remove(tomb->id.tableId, tomb->id.objectId);
        assert(r);
        server->freeRecoveryTombstone(maybeTomb);
    }
}

/**
 * A Dispatch::Poller which lazily removes tombstones from the main HashTable.
 */
class RemoveTombstonePoller : public Dispatch::Poller {
  public:
    /**
     * Clean tombstones from #objectMap lazily and in the background.
     *
     * Instances of this class must be allocated with new since they
     * delete themselves when the #objectMap scan is completed which
     * automatically deregisters it from Dispatch.
     *
     * \param masterService
     *      The instance of MasterService which owns the #objectMap.
     * \param objectMap
     *      The HashTable which will be purged of tombstones.
     */
    RemoveTombstonePoller(MasterService& masterService,
                          HashTable<LogEntryHandle>& objectMap)
        : Dispatch::Poller()
        , currentBucket(0)
        , masterService(masterService)
        , objectMap(objectMap)
    {
        LOG(NOTICE, "Starting cleanup of tombstones in background");
    }

    /**
     * Remove tombstones from a single bucket and yield to other work
     * in the system.
     */
    virtual void
    poll()
    {
        // This method runs in the dispatch thread, so it isn't safe to
        // manipulate any of the objectMap state if any RPCs are currently
        // executing.
        if (!serviceManager->idle())
            return;
        objectMap.forEachInBucket(
            recoveryCleanup, &masterService, currentBucket);
        ++currentBucket;
        if (currentBucket == objectMap.getNumBuckets()) {
            LOG(NOTICE, "Cleanup of tombstones complete");
            delete this;
        }
    }

  private:
    /// Which bucket of #objectMap should be cleaned out next.
    uint64_t currentBucket;

    /// The MasterService used by the #recoveryCleanup callback.
    MasterService& masterService;

    /// The hash table to be purged of tombstones.
    HashTable<LogEntryHandle>& objectMap;

    DISALLOW_COPY_AND_ASSIGN(RemoveTombstonePoller);
};

/**
 * Remove leftover tombstones in the hash table added during recovery.
 * This method exists independently for testing purposes.
 */
void
MasterService::removeTombstones()
{
    CycleCounter<Metric> _(&metrics->master.removeTombstoneTicks);
#if TESTING
    // Asynchronous tombstone removal raises hell in unit tests.
    objectMap.forEach(recoveryCleanup, this);
#else
    Dispatch::Lock lock;
    new RemoveTombstonePoller(*this, objectMap);
#endif
}

namespace {
/**
 * Each object of this class is responsible for fetching recovery data
 * for a single segment from a single backup.  This class is defined
 * in the anonymous namespace so it doesn't need to appear in the header
 * file.
 */
struct Task {
    Task(uint64_t masterId,
         uint64_t partitionId,
         ProtoBuf::ServerList::Entry& backupHost)
        : masterId(masterId)
        , partitionId(partitionId)
        , backupHost(backupHost)
        , response()
        , client(transportManager.getSession(
                    backupHost.service_locator().c_str()))
        , startTime(Cycles::rdtsc())
        , rpc()
        , resendTime(0)
    {
          rpc.construct(client, masterId, backupHost.segment_id(),
                        partitionId, response);
    }
    void resend() {
        LOG(DEBUG, "Resend %lu", backupHost.segment_id());
        response.reset();
        rpc.construct(client, masterId, backupHost.segment_id(),
                      partitionId, response);
    }
    uint64_t masterId;
    uint64_t partitionId;
    ProtoBuf::ServerList::Entry& backupHost;
    Buffer response;
    BackupClient client;
    const uint64_t startTime;
    Tub<BackupClient::GetRecoveryData> rpc;

    /// If we have to retry a request, this variable indicates the rdtsc time at
    /// which we should retry.  0 means we're not waiting for a retry.
    uint64_t resendTime;
    DISALLOW_COPY_AND_ASSIGN(Task);
};
}

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
 * \param backups
 *      The list of backups which have statuses set in their user_data field
 *      to be checked to ensure recovery of this partition was successful.
 * \throw SegmentRecoveryFailedException
 *      If some segment was not recovered and the recovery master is not
 *      a valid replacement for the crashed master.
 */
void
detectSegmentRecoveryFailure(const uint64_t masterId,
                             const uint64_t partitionId,
                             const ProtoBuf::ServerList& backups)
{
    boost::unordered_set<uint64_t> failures;
    foreach (const auto& backup, backups.server()) {
        switch (backup.user_data()) {
        case MasterService::REC_REQ_OK:
            failures.erase(backup.segment_id());
            break;
        case MasterService::REC_REQ_FAILED:
            failures.insert(backup.segment_id());
            break;
        case MasterService::REC_REQ_WAITING:
        case MasterService::REC_REQ_NOT_STARTED:
        default:
            assert(false);
            break;
        }
    }
    if (!failures.empty()) {
        LOG(ERROR, "Recovery master failed to recover master %lu "
            "partition %lu", masterId, partitionId);
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
 * \param masterId
 *      The id of the crashed master for which recoveryMaster will be taking
 *      over ownership of tablets.
 * \param partitionId
 *      The partition id of tablets inside the crashed master's will that
 *      this master is recovering.
 * \param backups
 *      A list of backup locators along with a segmentId specifying for each
 *      segmentId a backup who can provide a filtered recovery data segment.
 *      A particular segment may be listed more than once if it has multiple
 *      viable backups, hence a particular backup locator can also be listed
 *      many times.
 * \throw SegmentRecoveryFailedException
 *      If some segment was not recovered and the recovery master is not
 *      a valid replacement for the crashed master.
 */
void
MasterService::recover(uint64_t masterId,
                      uint64_t partitionId,
                      ProtoBuf::ServerList& backups)
{
    /* Overview of the internals of this method and its structures.
     *
     * The main data structure is "backups".  It works like a
     * scoreboard, tracking which segments have requests to backup
     * servers in-flight for data, which have been replayed, and
     * which have failed and must be replayed by another entry in
     * the table.
     *
     * backupsEnd is an iterator to the end of the segment list
     * which aids in tracking when the function is out of work.
     *
     * notStarted tracks the furtherest entry into the list which
     * has not been requested from a backup yet (REC_REQ_NOT_STARTED).
     *
     * These statuses are all tracked in the "user_data" field of
     * "backups".  Here is a sample of what the structure might
     * look like during execution:
     *
     * service_locator     segment_id  user_data
     * ---------------     ----------  ---------
     * 10.0.0.8,123        99          OK
     * 10.0.0.3,123        88          FAILED
     * 10.0.0.1,123        77          OK
     * 10.0.0.2,123        77          OK
     * 10.0.0.6,123        88          WAITING
     * 10.0.0.2,123        66          NOT_STARTED  <- notStarted
     * 10.0.0.3,123        55          WAITING
     * 10.0.0.1,123        66          NOT_STARTED
     * 10.0.0.7,123        66          NOT_STARTED
     * 10.0.0.3,123        99          OK
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
    LOG(NOTICE, "Recovering master %lu, partition %lu, %u hosts",
        masterId, partitionId, backups.server_size());

    boost::unordered_set<uint64_t> runningSet;
    foreach (auto& backup, *backups.mutable_server())
        backup.set_user_data(REC_REQ_NOT_STARTED);

#ifdef PERF_DEBUG_RECOVERY_SERIAL
    Tub<Task> tasks[1];
#else
    Tub<Task> tasks[4];
#endif
    uint32_t activeRequests = 0;

    auto notStarted = backups.mutable_server()->begin();
    auto backupsEnd = backups.mutable_server()->end();

    // Start RPCs
    auto backup = notStarted;
    foreach (auto& task, tasks) {
        while (!task) {
            if (backup == backupsEnd)
                goto doneStartingInitialTasks;
            LOG(DEBUG, "Starting getRecoveryData from %s for segment %lu "
                "on channel %ld (initial round of RPCs)",
                backup->service_locator().c_str(),
                backup->segment_id(),
                &task - &tasks[0]);
            try {
                task.construct(masterId, partitionId, *backup);
                backup->set_user_data(REC_REQ_WAITING);
                runningSet.insert(backup->segment_id());
                ++metrics->master.segmentReadCount;
                ++activeRequests;
            } catch (const TransportException& e) {
                LOG(DEBUG, "Couldn't contact %s, trying next backup; "
                    "failure was: %s",
                    backup->service_locator().c_str(),
                    e.str().c_str());
                backup->set_user_data(REC_REQ_FAILED);
            }
            ++backup;
            while (backup != backupsEnd &&
                   contains(runningSet, backup->segment_id()))
                ++backup;
        }
    }
  doneStartingInitialTasks:

    // As RPCs complete, process them and start more
    Tub<CycleCounter<Metric>> readStallTicks;
    readStallTicks.construct(&metrics->master.segmentReadStallTicks);

    bool gotFirstGRD = false;

    boost::unordered_multimap<uint64_t, ProtoBuf::ServerList::Entry*>
        segmentIdToBackups;
    foreach (auto& backup, *backups.mutable_server())
        segmentIdToBackups.insert({backup.segment_id(), &backup});

    while (activeRequests) {
        this->backup.proceed();
        uint64_t currentTime = Cycles::rdtsc();
        foreach (auto& task, tasks) {
            if (!task)
                continue;
            if (task->resendTime != 0) {
                if (currentTime > task->resendTime) {
                    task->resendTime = 0;
                    task->resend();
                }
                continue;
            }
            if (!task->rpc->isReady())
                continue;
            readStallTicks.destroy();
            LOG(DEBUG, "Waiting on recovery data for segment %lu from %s",
                task->backupHost.segment_id(),
                task->backupHost.service_locator().c_str());
            try {
                (*task->rpc)();
                uint64_t grdTime = Cycles::rdtsc() - task->startTime;

                if (!gotFirstGRD) {
                    metrics->master.replicationTicks =
                        0 - Cycles::rdtsc();
                    metrics->master.replicationBytes =
                        0 - metrics->transport.transmit.byteCount;
                    gotFirstGRD = true;
                }
                LOG(DEBUG, "Got getRecoveryData response, took %.1f us "
                    "on channel %ld",
                    Cycles::toSeconds(grdTime)*1e06,
                    &task - &tasks[0]);

                uint32_t responseLen = task->response.getTotalLength();
                metrics->master.segmentReadByteCount += responseLen;
                LOG(DEBUG, "Recovering segment %lu with size %u",
                    task->backupHost.segment_id(), responseLen);
                uint64_t startUseful = Cycles::rdtsc();
                recoverSegment(task->backupHost.segment_id(),
                               task->response.getRange(0, responseLen),
                               responseLen);
                usefulTime += Cycles::rdtsc() - startUseful;

                runningSet.erase(task->backupHost.segment_id());
                // Mark this and any other entries for this segment as OK.
                LOG(DEBUG, "Checking %s off the list for %lu",
                    task->backupHost.service_locator().c_str(),
                    task->backupHost.segment_id());
                task->backupHost.set_user_data(REC_REQ_OK);
                auto its = segmentIdToBackups.equal_range(
                    task->backupHost.segment_id());
                for (auto it = its.first; it != its.second; ++it) {
                    LOG(DEBUG, "Checking %s off the list for %lu",
                        it->second->service_locator().c_str(),
                        it->second->segment_id());
                    it->second->set_user_data(REC_REQ_OK);
                }
            } catch (const RetryException& e) {
                // The backup isn't ready yet, try back in 1 ms.
                task->resendTime = currentTime +
                    static_cast<int>(Cycles::perSecond()/1000.0);
                readStallTicks.construct(
                                    &metrics->master.segmentReadStallTicks);
                continue;
            } catch (const TransportException& e) {
                LOG(DEBUG, "Couldn't contact %s, trying next backup; "
                    "failure was: %s",
                    task->backupHost.service_locator().c_str(),
                    e.str().c_str());
                task->backupHost.set_user_data(REC_REQ_FAILED);
                runningSet.erase(task->backupHost.segment_id());
            } catch (const ClientException& e) {
                LOG(DEBUG, "getRecoveryData failed on %s, trying next backup; "
                    "failure was: %s",
                    task->backupHost.service_locator().c_str(),
                    e.str().c_str());
                task->backupHost.set_user_data(REC_REQ_FAILED);
                runningSet.erase(task->backupHost.segment_id());
            }

            task.destroy();

            // move notStarted up as far as possible
            while (notStarted != backupsEnd &&
                   (notStarted->user_data() != REC_REQ_NOT_STARTED))
                ++notStarted;

            // Find the next NOT_STARTED entry that isn't in-flight
            // from another entry.
            auto backup = notStarted;
            while (!task && backup != backupsEnd) {
                while (backup->user_data() != REC_REQ_NOT_STARTED ||
                       contains(runningSet, backup->segment_id())) {
                    ++backup;
                    if (backup == backupsEnd)
                        goto outOfHosts;
                }
                LOG(DEBUG, "Starting getRecoveryData from %s for segment %lu "
                    "on channel %ld (after RPC completion)",
                    backup->service_locator().c_str(),
                    backup->segment_id(),
                    &task - &tasks[0]);
                try {
                    task.construct(masterId, partitionId, *backup);
                    backup->set_user_data(REC_REQ_WAITING);
                    runningSet.insert(backup->segment_id());
                    ++metrics->master.segmentReadCount;
                } catch (const TransportException& e) {
                    LOG(DEBUG, "Couldn't contact %s, trying next backup; "
                        "failure was: %s",
                        backup->service_locator().c_str(),
                        e.str().c_str());
                    backup->set_user_data(REC_REQ_FAILED);
                }
            }
          outOfHosts:
            if (!task)
                --activeRequests;
            readStallTicks.construct(&metrics->master.segmentReadStallTicks);
        }
    }
    if (readStallTicks)
        readStallTicks.destroy();

    detectSegmentRecoveryFailure(masterId, partitionId, backups);

    {
        CycleCounter<Metric> logSyncTicks(&metrics->master.logSyncTicks);
        LOG(NOTICE, "Syncing the log");
        metrics->master.logSyncBytes =
            0 - metrics->transport.transmit.byteCount;
        log.sync();
        metrics->master.logSyncBytes += metrics->transport.transmit.byteCount;
    }

    metrics->master.replicationTicks += Cycles::rdtsc();
    metrics->master.replicationBytes += metrics->transport.transmit.byteCount;

    double totalSecs = Cycles::toSeconds(Cycles::rdtsc() - start);
    double usefulSecs = Cycles::toSeconds(usefulTime);
    LOG(NOTICE, "Recovery complete, took %.1f ms, useful replaying "
        "time %.1f ms (%.1f%% effective)",
        totalSecs * 1e03,
        usefulSecs * 1e03,
        100 * usefulSecs / totalSecs);
}

/**
 * Top-level server method to handle the RECOVER request.
 * \copydetails Service::ping
 */
void
MasterService::recover(const RecoverRpc::Request& reqHdr,
                       RecoverRpc::Response& respHdr,
                       Rpc& rpc)
{
    {
        CycleCounter<Metric> recoveryTicks(&metrics->master.recoveryTicks);
        // Don't reset metrics here: the backup service has already done it,
        // and doing it again here may cause information to be lost.
        // reset(metrics, *serverId);
        metrics->hasMaster = 1;
        metrics->master.replicas = backup.replicas;

        const auto& masterId = reqHdr.masterId;
        const auto& partitionId = reqHdr.partitionId;
        ProtoBuf::Tablets recoveryTablets;
        ProtoBuf::parseFromResponse(rpc.requestPayload, sizeof(reqHdr),
                                    reqHdr.tabletsLength, recoveryTablets);
        ProtoBuf::ServerList backups;
        ProtoBuf::parseFromResponse(rpc.requestPayload,
                                    downCast<uint32_t>(sizeof(reqHdr)) +
                                    reqHdr.tabletsLength,
                                    reqHdr.serverListLength,
                                    backups);
        LOG(DEBUG, "Starting recovery of %u tablets on masterId %lu",
            recoveryTablets.tablet_size(), *serverId);
        rpc.sendReply();

        // reqHdr, respHdr, and rpc are off-limits now

        // Union the new tablets into an updated tablet map
        ProtoBuf::Tablets newTablets(tablets);
        newTablets.mutable_tablet()->MergeFrom(recoveryTablets.tablet());
        // and set ourself as open for business.
        setTablets(newTablets);

        // Recover Segments, firing MasterService::recoverSegment for each one.
        recover(masterId, partitionId, backups);

        // Free recovery tombstones left in the hash table.
        removeTombstones();

        // Once the coordinator and the recovery master agree that the
        // master has taken over for the tablets it can update its tables
        // and begin serving requests.

        // Update the recoveryTablets to reflect the fact that this master is
        // going to try to become the owner.
        foreach (ProtoBuf::Tablets::Tablet& tablet,
                 *recoveryTablets.mutable_tablet()) {
            LOG(NOTICE, "set tablet %lu %lu %lu to locator %s, id %lu",
                     tablet.table_id(), tablet.start_object_id(),
                     tablet.end_object_id(), config.localLocator.c_str(),
                     *serverId);
            tablet.set_service_locator(config.localLocator);
            tablet.set_server_id(*serverId);
        }

        // TODO(ongaro): don't need to calculate a new will here
        ProtoBuf::Tablets recoveryWill;
        {
            CycleCounter<Metric> _(&metrics->master.recoveryWillTicks);
            Will will(tablets, maxBytesPerPartition, maxReferantsPerPartition);
            will.serialize(recoveryWill);
        }

        {
            CycleCounter<Metric> _(&metrics->master.tabletsRecoveredTicks);
            coordinator->tabletsRecovered(*serverId, recoveryTablets,
                                          recoveryWill);
        }
        // Ok - we're free to start serving now.

        // TODO(stutsman) update local copy of the will
    }
    // No need to dump metrics now: it will be done in the backup code.
    // dump(metrics);
}

/**
 * Given a RecoverySegmentIterator for the Segment we're currently
 * recovering, advance it and issue prefetches on the hash tables.
 * This is used exclusively by recoverSegment().
 *
 * \param[in] i
 *      A RecoverySegmentIterator to use for prefetching. Note that this
 *      method modifies the iterator, so the caller should not use
 *      it for its own iteration.
 */
void
MasterService::recoverSegmentPrefetcher(RecoverySegmentIterator& i)
{
    i.next();

    if (i.isDone())
        return;

    LogEntryType type = i.getType();
    uint64_t objId = ~0UL, tblId = ~0UL;

    if (type == LOG_ENTRY_TYPE_OBJ) {
        const Object *recoverObj = reinterpret_cast<const Object *>(
                     i.getPointer());
        objId = recoverObj->id.objectId;
        tblId = recoverObj->id.tableId;
    } else if (type == LOG_ENTRY_TYPE_OBJTOMB) {
        const ObjectTombstone *recoverTomb =
            reinterpret_cast<const ObjectTombstone *>(i.getPointer());
        objId = recoverTomb->id.objectId;
        tblId = recoverTomb->id.tableId;
    } else {
        return;
    }

    objectMap.prefetchBucket(tblId, objId);
}

/**
 * Replay a filtered segment from a crashed Master that this Master is taking
 * over for.
 *
 * \param segmentId
 *      The segmentId of the segment as it was in the log of the crashed Master.
 * \param buffer 
 *      A pointer to a valid segment which has been pre-filtered of all
 *      objects except those that pertain to the tablet ranges this Master
 *      will be responsible for after the recovery completes.
 * \param bufferLength
 *      Length of the buffer in bytes.
 */
void
MasterService::recoverSegment(uint64_t segmentId, const void *buffer,
                              uint32_t bufferLength)
{
    uint64_t start = Cycles::rdtsc();
    LOG(DEBUG, "recoverSegment %lu, ...", segmentId);
    CycleCounter<Metric> _(&metrics->master.recoverSegmentTicks);

    RecoverySegmentIterator i(buffer, bufferLength);
#ifndef PERF_DEBUG_RECOVERY_REC_SEG_NO_PREFETCH
    RecoverySegmentIterator prefetch(buffer, bufferLength);
#endif

#ifdef PERF_DEBUG_RECOVERY_REC_SEG_JUST_ITER
    for (; !i.isDone(); i.next());
    return;
#endif
    uint64_t lastOffsetBackupProgress = 0;
    while (!i.isDone()) {
        LogEntryType type = i.getType();

        if (i.getOffset() > lastOffsetBackupProgress + 50000) {
            lastOffsetBackupProgress = i.getOffset();
            this->backup.proceed();
        }

#ifndef PERF_DEBUG_RECOVERY_REC_SEG_NO_PREFETCH
        recoverSegmentPrefetcher(prefetch);
#endif

        if (type == LOG_ENTRY_TYPE_OBJ) {
            const Object *recoverObj = reinterpret_cast<const Object *>(
                i.getPointer());
            uint64_t objId = recoverObj->id.objectId;
            uint64_t tblId = recoverObj->id.tableId;

#ifdef PERF_DEBUG_RECOVERY_REC_SEG_NO_HT
            const Object *localObj = 0;
            const ObjectTombstone *tomb = 0;
#else
            const Object *localObj = NULL;
            const ObjectTombstone *tomb = NULL;
            LogEntryHandle handle = objectMap.lookup(tblId, objId);
            if (handle != NULL) {
                if (handle->type() == LOG_ENTRY_TYPE_OBJTOMB)
                    tomb = handle->userData<ObjectTombstone>();
                else
                    localObj = handle->userData<Object>();
            }
#endif

            // can't have both a tombstone and an object in the hash tables
            assert(tomb == NULL || localObj == NULL);

            uint64_t minSuccessor = 0;
            if (localObj != NULL)
                minSuccessor = localObj->version + 1;
            else if (tomb != NULL)
                minSuccessor = tomb->objectVersion + 1;

            if (recoverObj->version >= minSuccessor) {
#ifdef PERF_DEBUG_RECOVERY_REC_SEG_NO_LOG
                const Object* newObj = localObj;
#else
                // write to log (with lazy backup flush) & update hash table
                uint64_t lengthInLog;
                LogTime logTime;
                LogEntryHandle newObjHandle = log.append(
                    LOG_ENTRY_TYPE_OBJ, recoverObj, i.getLength(), &lengthInLog,
                    &logTime, false, i.checksum());
                ++metrics->master.objectAppendCount;
                metrics->master.liveObjectBytes +=
                    localObj->dataLength(i.getLength());

                // update the TabletProfiler
                Table& t(getTable(downCast<uint32_t>(recoverObj->id.tableId),
                                  recoverObj->id.objectId));
                t.profiler.track(recoverObj->id.objectId,
                                 downCast<uint32_t>(lengthInLog), logTime);
#endif

#ifndef PERF_DEBUG_RECOVERY_REC_SEG_NO_HT
                objectMap.replace(newObjHandle);
#endif

                // nuke the tombstone, if it existed
                if (tomb != NULL)
                    freeRecoveryTombstone(handle);

                // nuke the old object, if it existed
                if (localObj != NULL) {
                    metrics->master.liveObjectBytes -=
                        localObj->dataLength(handle->length());
                    log.free(handle);
                } else {
                    ++metrics->master.liveObjectCount;
                }
            } else {
                ++metrics->master.objectDiscardCount;
            }
        } else if (type == LOG_ENTRY_TYPE_OBJTOMB) {
            const ObjectTombstone *recoverTomb =
                reinterpret_cast<const ObjectTombstone *>(i.getPointer());
            uint64_t objId = recoverTomb->id.objectId;
            uint64_t tblId = recoverTomb->id.tableId;

            bool checksumIsValid = ({
                CycleCounter<Metric> c(&metrics->master.verifyChecksumTicks);
                i.isChecksumValid();
            });
            if (!checksumIsValid) {
                LOG(WARNING, "invalid tombstone checksum! tbl: %lu, obj: %lu, "
                    "ver: %lu", tblId, objId, recoverTomb->objectVersion);
            }

            const Object *localObj = NULL;
            const ObjectTombstone *tomb = NULL;
            LogEntryHandle handle = objectMap.lookup(tblId, objId);
            if (handle != NULL) {
                if (handle->type() == LOG_ENTRY_TYPE_OBJTOMB)
                    tomb = handle->userData<ObjectTombstone>();
                else
                    localObj = handle->userData<Object>();
            }

            // can't have both a tombstone and an object in the hash tables
            assert(tomb == NULL || localObj == NULL);

            uint64_t minSuccessor = 0;
            if (localObj != NULL)
                minSuccessor = localObj->version;
            else if (tomb != NULL)
                minSuccessor = tomb->objectVersion + 1;

            if (recoverTomb->objectVersion >= minSuccessor) {
                // allocate memory for the tombstone & update hash table
                // TODO(ongaro): Change to new with copy constructor?
                LogEntryHandle newTomb = allocRecoveryTombstone(recoverTomb);
                ++metrics->master.tombstoneAppendCount;
                objectMap.replace(newTomb);

                // nuke the old tombstone, if it existed
                if (tomb != NULL)
                    freeRecoveryTombstone(handle);

                // nuke the object, if it existed
                if (localObj != NULL) {
                    --metrics->master.liveObjectCount;
                    metrics->master.liveObjectBytes -=
                        localObj->dataLength(handle->length());
                    log.free(handle);
                }
            } else {
                ++metrics->master.tombstoneDiscardCount;
            }
        }

        i.next();
    }
    double replayTime = Cycles::toSeconds(Cycles::rdtsc() - start);
    LOG(DEBUG, "Segment %lu replay complete, took %.1f ms",
        segmentId, replayTime*1e03);
}

/**
 * Top-level server method to handle the REMOVE request.
 * \copydetails create
 */
void
MasterService::remove(const RemoveRpc::Request& reqHdr,
                      RemoveRpc::Response& respHdr,
                      Rpc& rpc)
{
    Table& t(getTable(reqHdr.tableId, reqHdr.id));
    LogEntryHandle handle = objectMap.lookup(reqHdr.tableId, reqHdr.id);
    if (handle == NULL || handle->type() != LOG_ENTRY_TYPE_OBJ) {
        rejectOperation(&reqHdr.rejectRules, VERSION_NONEXISTENT);
        return;
    }

    const Object *obj = handle->userData<Object>();
    respHdr.version = obj->version;

    // Abort if we're trying to delete the wrong version.
    rejectOperation(&reqHdr.rejectRules, respHdr.version);

    t.RaiseVersion(obj->version + 1);

    ObjectTombstone tomb(log.getSegmentId(obj), obj);

    // Mark the deleted object as free first, since the append could
    // invalidate it
    log.free(LogEntryHandle(obj));

    // Write the tombstone into the Log, update our tablet
    // counters, and remove from the hash table.
    uint64_t lengthInLog;
    LogTime logTime;

    log.append(LOG_ENTRY_TYPE_OBJTOMB, &tomb, sizeof(tomb),
        &lengthInLog, &logTime);
    t.profiler.track(obj->id.objectId,
                     downCast<uint32_t>(lengthInLog), logTime);
    objectMap.remove(reqHdr.tableId, reqHdr.id);
}


/**
 * Top-level server method to handle the REREPLICATE_SEGMENTS request.
 * Using the server id of a crashed backup from #reqHdr this MasterService
 * rereplicates any live segments it had stored on that backup to new backups
 * in order to maintain any replication requirements after a backup failure.
 *
 * \copydetails Service::ping
 */
void
MasterService::rereplicateSegments(
    const RereplicateSegmentsRpc::Request& reqHdr,
    RereplicateSegmentsRpc::Response& respHdr,
    Rpc& rpc)
{
    const uint64_t failedBackupId = reqHdr.backupId;
    LOG(NOTICE, "Backup %lu failed, rereplicating segments elsewhere",
        failedBackupId);
}

/**
 * Set the list of tablets that this master serves.
 *
 * Notice that this method does nothing about the objects and data
 * for a particular tablet.  That is, the log and hashtable must already
 * contain a consistent view of the tablet before being set as an active
 * tablet with this method.
 *
 * \param newTablets
 *      The new set of tablets this master is serving.
 */
void
MasterService::setTablets(const ProtoBuf::Tablets& newTablets)
{
    typedef std::map<uint32_t, Table*> Tables;
    Tables tables;

    // create map from table ID to Table of pre-existing tables
    foreach (const ProtoBuf::Tablets::Tablet& oldTablet, tablets.tablet()) {
        tables[downCast<uint32_t>(oldTablet.table_id())] =
            reinterpret_cast<Table*>(oldTablet.user_data());
    }

    // overwrite tablets with new tablets
    tablets = newTablets;

    // delete pre-existing tables that no longer live here
    foreach (Tables::value_type oldTable, tables) {
        foreach (const ProtoBuf::Tablets::Tablet& newTablet,
                 tablets.tablet()) {
            if (oldTable.first == newTablet.table_id())
                goto next;
        }
        delete oldTable.second;
        oldTable.second = NULL;
      next:
        { /* pass */ }
    }

    // create new Tables and assign all new tablets tables
    LOG(NOTICE, "Now serving tablets:");
    foreach (ProtoBuf::Tablets::Tablet& newTablet, *tablets.mutable_tablet()) {
        LOG(NOTICE, "table: %20lu, start: %20lu, end  : %20lu",
            newTablet.table_id(), newTablet.start_object_id(),
            newTablet.end_object_id());
        Table* table = tables[downCast<uint32_t>(newTablet.table_id())];
        if (table == NULL) {
            table = new Table(newTablet.table_id());
            tables[downCast<uint32_t>(newTablet.table_id())] = table;
        }
        newTablet.set_user_data(reinterpret_cast<uint64_t>(table));
    }
}

/**
 * Top-level server method to handle the SET_TABLETS request.
 * \copydetails create
 */
void
MasterService::setTablets(const SetTabletsRpc::Request& reqHdr,
                          SetTabletsRpc::Response& respHdr,
                          Rpc& rpc)
{
    ProtoBuf::Tablets newTablets;
    ProtoBuf::parseFromRequest(rpc.requestPayload, sizeof(reqHdr),
                               reqHdr.tabletsLength, newTablets);
    setTablets(newTablets);
}

/**
 * Top-level server method to handle the WRITE request.
 * \copydetails create
 */
void
MasterService::write(const WriteRpc::Request& reqHdr,
                    WriteRpc::Response& respHdr,
                    Rpc& rpc)
{
    CycleCounter<uint64_t> timeThis;
    storeData(reqHdr.tableId, reqHdr.id,
              &reqHdr.rejectRules, &rpc.requestPayload, sizeof(reqHdr),
              static_cast<uint32_t>(reqHdr.length), &respHdr.version,
              reqHdr.async);
    serverStats.totalWriteRequests++;
    serverStats.totalWriteNanos += Cycles::toNanoseconds(timeThis.stop());
}

/**
 * Ensures that this master owns the tablet for the given object
 * and returns the corresponding Table.
 *
 * \param tableId
 *      Identifier for a desired table.
 * \param objectId
 *      Identifier for a desired object.
 *
 * \return
 *      The Table of which the tablet containing this object is a part.
 *
 * \exception TableDoesntExist
 *      Thrown if that tablet isn't owned by this server.
 */
// TODO(ongaro): Masters don't know whether tables exist.
// This be something like ObjectNotHereException.
Table&
MasterService::getTable(uint32_t tableId, uint64_t objectId) {

    foreach (const ProtoBuf::Tablets::Tablet& tablet, tablets.tablet()) {
        if (tablet.table_id() == tableId &&
            tablet.start_object_id() <= objectId &&
            objectId <= tablet.end_object_id()) {
            return *reinterpret_cast<Table*>(tablet.user_data());
        }
    }
    throw TableDoesntExistException(HERE);
}

/**
 * Check a set of RejectRules against the current state of an object
 * to decide whether an operation is allowed.
 *
 * \param rejectRules
 *      Specifies conditions under which the operation should fail.
 * \param version
 *      The current version of an object, or VERSION_NONEXISTENT
 *      if the object does not currently exist (used to test rejectRules)
 *
 * \return
 *      The return value is STATUS_OK if none of the reject rules
 *      indicate that the operation should be rejected. Otherwise
 *      the return value indicates the reason for the rejection.
 */
void
MasterService::rejectOperation(const RejectRules* rejectRules, uint64_t version)
{
    if (version == VERSION_NONEXISTENT) {
        if (rejectRules->doesntExist)
            throw ObjectDoesntExistException(HERE);
        return;
    }
    if (rejectRules->exists)
        throw ObjectExistsException(HERE);
    if (rejectRules->versionLeGiven && version <= rejectRules->givenVersion)
        throw WrongVersionException(HERE);
    if (rejectRules->versionNeGiven && version != rejectRules->givenVersion)
        throw WrongVersionException(HERE);
}

//-----------------------------------------------------------------------
// Everything below here is "old" code, meaning it probably needs to
// get refactored at some point, it doesn't follow the coding conventions,
// and there are no unit tests for it.
//-----------------------------------------------------------------------

/**
 * Callback used by the LogCleaner when it's cleaning a Segment and evicts
 * an Object (i.e. an entry of type LOG_ENTRY_TYPE_OBJ).
 *
 * Upon return, the object will be discarded. Objects must therefore be
 * perpetuated when the object being evicted is exactly the object referenced
 * by the hash table. Otherwise, it's an old object and a tombstone for it
 * exists.
 *
 * \param[in]  handle
 *      LogEntryHandle to the entry being evicted.
 * \param[in]  logTime
 *      The LogTime corresponding to the append operation that wrote this
 *      entry.
 * \param[in]  cookie
 *      The opaque state pointer registered with the callback.
 */
void
objectEvictionCallback(LogEntryHandle handle,
                       const LogTime logTime,
                       void* cookie)
{
    assert(handle->type() == LOG_ENTRY_TYPE_OBJ);

    MasterService *svr = static_cast<MasterService *>(cookie);
    assert(svr != NULL);

    Log& log = svr->log;

    const Object *evictObj = handle->userData<Object>();
    assert(evictObj != NULL);

    Table *t = NULL;
    try {
        t = &svr->getTable(downCast<uint32_t>(evictObj->id.tableId),
                           evictObj->id.objectId);
    } catch (TableDoesntExistException& e) {
        // That tablet doesn't exist on this server anymore.
        // Just remove the hash table entry, if it exists.
        svr->objectMap.remove(evictObj->id.tableId, evictObj->id.objectId);
        return;
    }

    LogEntryHandle hashTblHandle =
        svr->objectMap.lookup(evictObj->id.tableId, evictObj->id.objectId);
    assert(hashTblHandle->type() == LOG_ENTRY_TYPE_OBJ);
    const Object *hashTblObj = hashTblHandle->userData<Object>();

    // simple pointer comparison suffices
    if (hashTblObj == evictObj) {
        uint64_t newLengthInLog;
        LogTime newLogTime;
        LogEntryHandle newObjHandle = log.append(LOG_ENTRY_TYPE_OBJ,
            evictObj, handle->length(), &newLengthInLog, &newLogTime);
        t->profiler.track(evictObj->id.objectId,
                          downCast<uint32_t>(newLengthInLog), newLogTime);
        svr->objectMap.replace(newObjHandle);
    }

    // remove the evicted entry whether it is discarded or not
    t->profiler.untrack(evictObj->id.objectId, handle->totalLength(), logTime);
}

/**
 * Callback used by the LogCleaner when it's cleaning a Segment and evicts
 * an ObjectTombstone (i.e. an entry of type LOG_ENTRY_TYPE_OBJTOMB).
 *
 * Tombstones are perpetuated when the Segment they reference is still
 * valid in the system.
 *
 * \param[in]  handle
 *      LogEntryHandle to the entry being evicted.
 * \param[in]  logTime
 *      The LogTime corresponding to the append operation that wrote this
 *      entry.
 * \param[in]  cookie
 *      The opaque state pointer registered with the callback.
 */
void
tombstoneEvictionCallback(LogEntryHandle handle,
                          const LogTime logTime,
                          void* cookie)
{
    assert(handle->type() == LOG_ENTRY_TYPE_OBJTOMB);

    MasterService *svr = static_cast<MasterService *>(cookie);
    assert(svr != NULL);

    Log& log = svr->log;

    const ObjectTombstone *tomb = handle->userData<ObjectTombstone>();
    assert(tomb != NULL);

    Table *t = NULL;
    try {
        t = &svr->getTable(downCast<uint32_t>(tomb->id.tableId),
                           tomb->id.objectId);
    } catch (TableDoesntExistException& e) {
        // That tablet doesn't exist on this server anymore.
        return;
    }

    // see if the referant is still there
    if (log.isSegmentLive(tomb->segmentId)) {
        uint64_t newLengthInLog;
        LogTime newLogTime;
        log.append(LOG_ENTRY_TYPE_OBJTOMB, tomb, sizeof(*tomb),
            &newLengthInLog, &newLogTime);
        t->profiler.track(tomb->id.objectId,
                          downCast<uint32_t>(newLengthInLog),
                          newLogTime);
    }

    // remove the evicted entry whether it is discarded or not
    t->profiler.untrack(tomb->id.objectId, handle->totalLength(), logTime);
}

void
MasterService::storeData(uint64_t tableId, uint64_t id,
                         const RejectRules* rejectRules, Buffer* data,
                         uint32_t dataOffset, uint32_t dataLength,
                         uint64_t* newVersion, bool async)
{
    Table& t(getTable(downCast<uint32_t>(tableId), id));

    const Object *obj = NULL;
    LogEntryHandle handle = objectMap.lookup(tableId, id);
    if (handle != NULL) {
        if (handle->type() == LOG_ENTRY_TYPE_OBJTOMB) {
            recoveryCleanup(handle,
                            static_cast<uint8_t>(LOG_ENTRY_TYPE_OBJTOMB),
                            this);
            handle = NULL;
        } else {
            assert(handle->type() == LOG_ENTRY_TYPE_OBJ);
            obj = handle->userData<Object>();
        }
    }

    uint64_t version = (obj != NULL) ? obj->version : VERSION_NONEXISTENT;
    uint64_t lengthInLog;
    LogTime logTime;

    try {
        rejectOperation(rejectRules, version);
    } catch (...) {
        *newVersion = version;
        throw;
    }

    DECLARE_OBJECT(newObject, dataLength);

    newObject->id.objectId = id;
    newObject->id.tableId = tableId;
    if (obj != NULL)
        newObject->version = obj->version + 1;
    else
        newObject->version = t.AllocateVersion();
    assert(obj == NULL || newObject->version > obj->version);
    data->copy(dataOffset, dataLength, newObject->data);

    // If the Object is being overwritten, we need to mark the previous space
    // used as free and add a tombstone that references it.
    if (obj != NULL) {
        // Mark the old object as freed _before_ writing the new object to the
        // log. If we do it afterwards, the LogCleaner could be triggered and
        // `o' could be reclaimed before log->append() returns. The subsequent
        // free then breaks, as that Segment may have been cleaned.
        log.free(handle);

        uint64_t segmentId = log.getSegmentId(obj);
        ObjectTombstone tomb(segmentId, obj);
        // Request an async append explicitly so that the tombstone
        // and the object are sent out together to the backups.
        bool sync = false;
        log.append(LOG_ENTRY_TYPE_OBJTOMB, &tomb, sizeof(tomb), &lengthInLog,
            &logTime, sync);
        t.profiler.track(id, downCast<uint32_t>(lengthInLog), logTime);
    }

    LogEntryHandle objHandle = log.append(LOG_ENTRY_TYPE_OBJ, newObject,
        newObject->objectLength(dataLength), &lengthInLog, &logTime, !async);
    t.profiler.track(id, downCast<uint32_t>(lengthInLog), logTime);
    objectMap.replace(objHandle);

    *newVersion = newObject->version;
    bytesWritten += dataLength;
}

} // namespace RAMCloud
