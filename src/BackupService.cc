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

#include "BackupService.h"
#include "Buffer.h"
#include "ClientException.h"
#include "Cycles.h"
#include "InMemoryStorage.h"
#include "PerfStats.h"
#include "ServerConfig.h"
#include "ShortMacros.h"
#include "SingleFileStorage.h"
#include "Status.h"

namespace RAMCloud {

// --- BackupService ---

/**
 * Create a BackupService.
 *
 * \param context
 *      Overall information about the RAMCloud server. The new service
 *      will be registered in this context.
 * \param config
 *      Settings for this instance. The caller guarantees that config will
 *      exist for the duration of this BackupService's lifetime.
 */
BackupService::BackupService(Context* context,
                             const ServerConfig* config)
    : context(context)
    , mutex()
    , config(config)
    , formerServerId()
    , storage()
    , frames()
    , recoveries()
    , segmentSize(config->segmentSize)
    , readSpeed()
    , bytesWritten(0)
    , initCalled(false)
    , gcTracker(context, this)
    , gcThread()
    , testingDoNotStartGcThread(false)
    , testingSkipCallerIdCheck(false)
    , taskQueue()
    , oldReplicas(0)
{
    context->services[WireFormat::BACKUP_SERVICE] = this;
    if (config->backup.inMemory) {
        storage.reset(new InMemoryStorage(config->segmentSize,
                                          config->backup.numSegmentFrames,
                                          config->backup.writeRateLimit));
    } else {
        size_t maxWriteBuffers = config->backup.maxNonVolatileBuffers;
        if (maxWriteBuffers == 0) {
            // Allow unlimited write buffers; this is risky, because an
            // overloaded backup process may buffer enough to exhaust
            // the server's free memory, in which case the Linux OOM killer
            // will kill the process or, even worse, there will be paging.
            maxWriteBuffers = config->backup.numSegmentFrames;
        }

        storage.reset(new SingleFileStorage(config->segmentSize,
                                            config->backup.numSegmentFrames,
                                            config->backup.writeRateLimit,
                                            maxWriteBuffers,
                                            config->backup.file.c_str(),
                                            O_DIRECT | O_SYNC));
    }
    if (storage->getMetadataSize() < sizeof(BackupReplicaMetadata))
        DIE("Storage metadata block too small to hold BackupReplicaMetadata");

    benchmark();

    BackupStorage::Superblock superblock = storage->loadSuperblock();
    if (config->clusterName == "__unnamed__") {
        LOG(NOTICE, "Cluster '__unnamed__'; ignoring existing backup storage. "
            "Any replicas stored will not be reusable by future backups. "
            "Specify clusterName for persistence across backup restarts.");
    } else {
        LOG(NOTICE, "Backup storing replicas with clusterName '%s'. Future "
            "backups must be restarted with the same clusterName for replicas "
            "stored on this backup to be reused.", config->clusterName.c_str());
        if (config->clusterName == superblock.getClusterName()) {
            LOG(NOTICE, "Replicas stored on disk have matching clusterName "
                "('%s'). Scanning storage to find all replicas and to make "
                "them available to recoveries.",
                superblock.getClusterName());
            formerServerId = superblock.getServerId();
            LOG(NOTICE, "Will enlist as a replacement for formerly crashed "
                "server %s which left replicas behind on disk",
                formerServerId.toString().c_str());
            restartFromStorage();
        } else {
            LOG(NOTICE, "Replicas stored on disk have a different clusterName "
                "('%s'). Scribbling storage to ensure any stale replicas left "
                "behind by old backups aren't used by future backups",
                superblock.getClusterName());
            storage->fry();
        }
    }
}

BackupService::~BackupService()
{
    context->services[WireFormat::BACKUP_SERVICE] = NULL;
    // Stop the garbage collector.
    taskQueue.halt();
    if (gcThread)
        gcThread->join();
    gcThread.destroy();

    // All frames will get free() called on them when there ref count drops
    // to zero as #frames is destroyed, but since free doesn't modify storage
    // and rpcs won't be serviced the storage won't be reused, so it is safe.
}

/**
 * If this server is rejoining a cluster the previous server id it
 * operated under is returned, otherwise an invalid server is returned.
 * "Rejoining" means this backup service may have segment replicas stored
 * that were created by masters in the cluster.
 * In this case, the coordinator must be made told of the former server
 * id under which these replicas were created in order to ensure that
 * all masters are made aware of the former server's crash before learning
 * of its re-enlistment.
 */
ServerId
BackupService::getFormerServerId() const
{
    return formerServerId;
}

/// Returns the serverId granted to this backup by the coordinator.
ServerId
BackupService::getServerId() const
{
    return serverId;
}

/**
 * Perform an initial benchmark of the storage system. This is later passed to
 * the coordinator during enlistment so that masters can intelligently select
 * backups.
 *
 * Benchmark is not run during unit if tests config.backup.mockSpeed is not 0.
 * It is not run as part of init() because that method must be fast (right now,
 * as soon as enlistment completes runs for some service, clients can start
 * accessing the service).
 */
void
BackupService::benchmark()
{
    if (config->backup.mockSpeed == 0) {
        auto strategy = static_cast<BackupStrategy>(config->backup.strategy);
        readSpeed = storage->benchmark(strategy);
    } else {
        readSpeed = config->backup.mockSpeed;
    }
}

// See Server::dispatch.
void
BackupService::dispatch(WireFormat::Opcode opcode, Rpc* rpc)
{
    Lock _(mutex); // Lock out GC while any RPC is being processed.

    // This is a hack. We allow the AssignGroup Rpc to be processed before
    // initCalled is set to true, since it is sent during initialization.
    if (!initCalled) {
        LOG(WARNING, "%s invoked before initialization complete; "
                "returning STATUS_RETRY", WireFormat::opcodeSymbol(opcode));
        throw RetryException(HERE, 100, 100,
                "backup service not yet initialized");
    }
    CycleCounter<RawMetric> serviceTicks(&metrics->backup.serviceTicks);

    switch (opcode) {
        case WireFormat::BackupFree::opcode:
            callHandler<WireFormat::BackupFree, BackupService,
                        &BackupService::freeSegment>(rpc);
            break;
        case WireFormat::BackupGetRecoveryData::opcode:
            callHandler<WireFormat::BackupGetRecoveryData, BackupService,
                        &BackupService::getRecoveryData>(rpc);
            break;
        case WireFormat::BackupRecoveryComplete::opcode:
            callHandler<WireFormat::BackupRecoveryComplete, BackupService,
                        &BackupService::recoveryComplete>(rpc);
            break;
        case WireFormat::BackupStartReadingData::opcode:
            callHandler<WireFormat::BackupStartReadingData, BackupService,
                        &BackupService::startReadingData>(rpc);
            break;
        case WireFormat::BackupStartPartitioningReplicas::opcode:
            callHandler<WireFormat::BackupStartPartitioningReplicas,
                        BackupService,
                        &BackupService::startPartitioningReplicas>(rpc);
            break;
        case WireFormat::BackupWrite::opcode:
            callHandler<WireFormat::BackupWrite, BackupService,
                        &BackupService::writeSegment>(rpc);
            break;
        default:
            throw UnimplementedRequestError(HERE);
    }
}

/**
 * Enqueues the release of the replica for the specified segment from permanent
 * storage which will allow the storage to be reused. After this call completes
 * the replica may still be found during subsequent recoveries for the master
 * which created the replica.
 *
 * This is a no-op if the backup is unaware of this segment.
 *
 * \param reqHdr
 *      Header of the Rpc request containing the segment number to free.
 * \param respHdr
 *      Header for the Rpc response.
 * \param rpc
 *      The Rpc being serviced.
 */
void
BackupService::freeSegment(const WireFormat::BackupFree::Request* reqHdr,
                           WireFormat::BackupFree::Response* respHdr,
                           Rpc* rpc)
{
    ServerId masterId(reqHdr->masterId);
    LOG(DEBUG, "Freeing replica for master %s segment %lu",
        masterId.toString().c_str(), reqHdr->segmentId);

    auto it =
        frames.find(MasterSegmentIdPair(masterId, reqHdr->segmentId));
    if (it == frames.end()) {
        LOG(WARNING, "Master tried to free non-existent segment: <%s,%lu>",
            masterId.toString().c_str(), reqHdr->segmentId);
        return;
    }

    frames.erase(it);
}

/**
 * Return the data for a particular tablet that was recovered by a call
 * to startReadingData().
 *
 * \param reqHdr
 *      Header of the Rpc request which contains the Rpc arguments.
 * \param respHdr
 *      Header for the Rpc response containing a count of RecoveredObjects being
 *      returned.
 * \param rpc
 *      The Rpc being serviced.  A back-to-back list of RecoveredObjects
 *      follows the respHdr in this buffer of length
 *      respHdr->recoveredObjectCount.
 *
 * \throw BackupBadSegmentIdException
 *      If the segment has not had recovery started for it (startReadingData()
 *      must have been called for its corresponding master id).
 */
void
BackupService::getRecoveryData(
    const WireFormat::BackupGetRecoveryData::Request* reqHdr,
    WireFormat::BackupGetRecoveryData::Response* respHdr,
    Rpc* rpc)
{
    ServerId crashedMasterId(reqHdr->masterId);
    LOG(DEBUG, "getRecoveryData masterId %s, segmentId %lu, partitionId %lu",
        crashedMasterId.toString().c_str(),
        reqHdr->segmentId, reqHdr->partitionId);

    auto recoveryIt = recoveries.find(crashedMasterId);
    if (recoveryIt == recoveries.end()) {
        LOG(WARNING, "Asked for recovery segment for <%s,%lu> but the master "
            "wasn't under recovery on the backup",
            crashedMasterId.toString().c_str(), reqHdr->segmentId);
        throw BackupBadSegmentIdException(HERE);
    }

    Status status =
        recoveryIt->second->getRecoverySegment(reqHdr->recoveryId,
                                               reqHdr->segmentId,
                                               downCast<int>(
                                                   reqHdr->partitionId),
                                               rpc->replyPayload,
                                               &respHdr->certificate);
    if (status != STATUS_OK) {
        respHdr->common.status = status;
        return;
    }

    ++metrics->backup.readCompletionCount;
    LOG(DEBUG, "getRecoveryData complete");
}

/**
 * Perform once-only initialization for the backup service after having
 * enlisted the process with the coordinator.
 *
 * Any actions performed here must not block the process or dispatch thread,
 * otherwise the server may be timed out and declared failed by the coordinator.
 */
void
BackupService::initOnceEnlisted()
{
    assert(!initCalled);

    LOG(NOTICE, "My server ID is %s", serverId.toString().c_str());
    if (metrics->serverId == 0) {
        metrics->serverId = *serverId;
    }

    storage->resetSuperblock(serverId, config->clusterName);
    LOG(NOTICE, "Backup %s will store replicas under cluster name '%s'",
        serverId.toString().c_str(), config->clusterName.c_str());
    initCalled = true;
}

/**
 * Clean up state associated with the given master after recovery.
 * \param reqHdr
 *      Header of the Rpc request containing the segment number to free.
 * \param respHdr
 *      Header for the Rpc response.
 * \param rpc
 *      The Rpc being serviced.
 */
void
BackupService::recoveryComplete(
        const WireFormat::BackupRecoveryComplete::Request* reqHdr,
        WireFormat::BackupRecoveryComplete::Response* respHdr,
        Rpc* rpc)
{
    LOG(DEBUG, "masterID: %s",
        ServerId(reqHdr->masterId).toString().c_str());
    rpc->sendReply();
}

/**
 * Scans storage to find replicas left behind by former backups and
 * incorporates them into this backup. This should only be called before
 * the backup has serviced any requests.
 *
 * Only replica metadata is scanned. It is possible that the actual replica
 * data found on storage during recovery is inconsistent with the metadata
 * scanned here at startup. Recovery must correctly deal with such
 * inconsistencies.
 */
void
BackupService::restartFromStorage()
{
    CycleCounter<> restartTime;

    std::unordered_map<ServerId, GarbageCollectReplicasFoundOnStorageTask*>
        gcTasks;

    std::vector<BackupStorage::FrameRef> allFrames = storage->loadAllMetadata();
    foreach (BackupStorage::FrameRef& frame, allFrames) {
        const BackupReplicaMetadata* metadata =
            static_cast<const BackupReplicaMetadata*>(frame->getMetadata());
        if (!metadata->checkIntegrity())
            continue;
        if (metadata->segmentCapacity != segmentSize) {
            LOG(ERROR, "On-storage replica metadata indicates a different "
                "segment size than the backup service was told to use; "
                "ignoring the problem; note ANY call to open a segment "
                "on this backup could cause the overwrite of your strangely "
                "sized replicas.");
            continue;
        }
        const ServerId masterId(metadata->logId);
        LOG(NOTICE, "Found stored replica <%s,%lu> on backup storage in "
                   "frame which was %s",
            masterId.toString().c_str(),
            metadata->segmentId, metadata->closed ? "closed" : "open");

        if (!metadata->closed) {
            // We could potentially skip loading the frame, but right
            // now there's an assumption that open segments are always
            // present in memory if needed for recovery.  Changing
            // this would probably not damage anything significantly...
            frame->load();
        }
        frames[MasterSegmentIdPair(masterId, metadata->segmentId)] =
            frame;
        if (gcTasks.find(masterId) == gcTasks.end()) {
            gcTasks[masterId] =
                new GarbageCollectReplicasFoundOnStorageTask(*this, masterId);
        }
        gcTasks[masterId]->addSegmentId(metadata->segmentId);
    }

    // Kick off the garbage collection for replicas stored on disk.
    foreach (const auto& value, gcTasks)
        value.second->schedule();

    LOG(NOTICE, "Replica information retrieved from storage: %f ms",
        1000. * Cycles::toSeconds(restartTime.stop()));
}

/**
 * Begin reading disk data for a Master, returning a list of backed
 * up Segments this backup has for the Master.
 *
 * A startPartitioningReplicas call should occur after this to properly
 * partition the segments before starting a recovery master.
 *
 * \param reqHdr
 *      Header of the Rpc request which contains the Rpc arguments.
 * \param respHdr
 *      Header for the Rpc response containing a count of segmentIds being
 *      returned.
 * \param rpc
 *      The Rpc being serviced.  A back-to-back list of uint64_t segmentIds
 *      follows the respHdr in this buffer of length respHdr->segmentIdCount.
 */
void
BackupService::startReadingData(
        const WireFormat::BackupStartReadingData::Request* reqHdr,
        WireFormat::BackupStartReadingData::Response* respHdr,
        Rpc* rpc)
{
    ServerId crashedMasterId(reqHdr->masterId);

    bool mustCreateRecovery = false;
    auto recoveryIt = recoveries.find(crashedMasterId);
    if (recoveryIt == recoveries.end()) {
        mustCreateRecovery = true;
    } else if (recoveryIt->second->getRecoveryId() != reqHdr->recoveryId) {
        // The old recovery may be in the middle of processing so we can just
        // delete it outright. Let it know it should schedule itself on the task
        // queue and should delete itself on the next iteration.
        LOG(NOTICE, "Got startReadingData for recovery %lu for crashed master "
            "%s; abandoning existing recovery %lu for that master and starting "
            "anew.", reqHdr->recoveryId, crashedMasterId.toString().c_str(),
            recoveryIt->second->getRecoveryId());
        BackupMasterRecovery* oldRecovery = recoveryIt->second;
        recoveries.erase(recoveryIt);
        oldRecovery->free();
        mustCreateRecovery = true;
    }
    BackupMasterRecovery* recovery;
    if (mustCreateRecovery) {
        recovery = new BackupMasterRecovery(taskQueue,
                                            reqHdr->recoveryId,
                                            crashedMasterId,
                                            segmentSize);
        recoveries[crashedMasterId] = recovery;
    }
    recovery = recoveries[crashedMasterId];

    std::vector<BackupStorage::FrameRef> framesForRecovery;
    for (auto it = frames.lower_bound({crashedMasterId, 0});
         it != frames.end(); ++it)
    {
        if (it->first.masterId != crashedMasterId)
            break;
        framesForRecovery.emplace_back(it->second);
    }
    recovery->start(framesForRecovery, rpc->replyPayload, respHdr);
    metrics->backup.storageType = uint64_t(storage->storageType);
}

/**
 * Begin bucketing the objects in the Segments according to a requested
 * TabletMap for a specific recovery. This follows after startReadingData.
 *
 * \param reqHdr
 *      Header of the Rpc request which contains the partitioning metadata.
 * \param respHdr
 *      Header for the Rpc response.
 * \param rpc
 *      The Rpc being serviced; used to access the actual Tablet ProtoBuf.
 */
void
BackupService::startPartitioningReplicas(
        const WireFormat::BackupStartPartitioningReplicas::Request* reqHdr,
        WireFormat::BackupStartPartitioningReplicas::Response* respHdr,
        Rpc* rpc)
{
    ServerId crashedMasterId(reqHdr->masterId);
    BackupMasterRecovery* recovery = recoveries[crashedMasterId];

    if (recovery == NULL || recovery->getRecoveryId() != reqHdr->recoveryId) {
        LOG(ERROR, "Cannot partition segments from master %s since they have "
                "not been read yet (no preceeding startReadingDataRpc call)",
                crashedMasterId.toString().c_str());

        throw PartitionBeforeReadException(HERE);
    }

    ProtoBuf::RecoveryPartition partitions;
    ProtoBuf::parseFromResponse(rpc->requestPayload, sizeof(*reqHdr),
                                reqHdr->partitionsLength, &partitions);
    recovery->setPartitionsAndSchedule(partitions);
}

/**
 * Store an opaque string of bytes in a currently open segment on
 * this backup server.  This data is guaranteed to be considered on recovery
 * of the master to which this segment belongs to the best of the ability of
 * this BackupService and will appear open unless it is closed
 * beforehand.
 *
 * If BackupWriteRpc::OPEN flag is set then space is allocated to receive
 * backup writes for a segment.  If this call succeeds the Backup must not
 * reject subsequent writes to this segment for lack of space.  The caller must
 * ensure that this masterId,segmentId pair is unique and hasn't been used
 * before on this backup or any other.  The storage space remains in use until
 * the master calls freeSegment(), the segment is involved in a recovery that
 * completes successfully, or until the backup crashes.
 *
 * If BackupWriteRpc::CLOSE flag is set then once this call succeeds the
 * segment will be considered closed and immutable and the Backup will
 * use this as a hint to move the segment to appropriate storage.
 *
 * \param reqHdr
 *      Header of the Rpc request which contains the Rpc arguments except
 *      the data to be written.
 * \param respHdr
 *      Header for the Rpc response.
 * \param rpc
 *      The Rpc being serviced, used for access to the opaque bytes to
 *      be written which follow reqHdr.
 *
 * \throw BackupSegmentOverflowException
 *      If the write request is beyond the end of the segment.
 * \throw BackupBadSegmentIdException
 *      If the segment is not open.
 */
void
BackupService::writeSegment(const WireFormat::BackupWrite::Request* reqHdr,
                            WireFormat::BackupWrite::Response* respHdr,
                            Rpc* rpc)
{
    ServerId masterId(reqHdr->masterId);
    uint64_t segmentId = reqHdr->segmentId;

    if  (!context->serverList->isUp(masterId) && !testingSkipCallerIdCheck) {
        // See "Zombies" in designNotes.
        LOG(WARNING, "Received backup write request from server %s which is "
            "not in server list version %lu:\n%s",
            masterId.toString().c_str(),
            context->serverList->getVersion(),
            context->serverList->toString().c_str());
        throw CallerNotInClusterException(HERE);
    }
    auto frameIt = frames.find({masterId, segmentId});
    BackupStorage::FrameRef frame;
    if (frameIt != frames.end())
        frame = frameIt->second;

    if (frame && !frame->wasAppendedToByCurrentProcess()) {
        if (reqHdr->open) {
            // We get here if a backup crashes, restarts, reloads a
            // replica from disk, and then the master detects the crash and
            // tries to re-replicate the segment that lost a replica on
            // the restarted backup. Reopen the segment (see RAM-573).
            const BackupReplicaMetadata* metadata =
                static_cast<const BackupReplicaMetadata*>(frame->getMetadata());
            frame->reopen(metadata->certificate.segmentLength);
        } else {
            // This should never happen.
            LOG(ERROR, "Master tried to write replica for <%s,%lu> but "
                "another replica was recovered from storage with the same id; "
                "rejecting write request", masterId.toString().c_str(),
                segmentId);
            // This exception will crash the calling master.
            throw BackupBadSegmentIdException(HERE);
        }
    }

    // Perform open, if any.
    if (reqHdr->open && !frame) {
        LOG(DEBUG, "Opening <%s,%lu>", masterId.toString().c_str(),
            segmentId);
        try {
            frame = storage->open(config->backup.sync);
        } catch (const BackupStorageException& e) {
            LOG(NOTICE, "Master tried to open replica for <%s,%lu> but "
                "there was a problem allocating storage space; "
                "rejecting open request: %s", masterId.toString().c_str(),
                segmentId, e.what());
            throw BackupOpenRejectedException(HERE);
        }
        frames[MasterSegmentIdPair(masterId, segmentId)] = frame;
    }

    // Perform write.
    if (!frame) {
        LOG(WARNING, "Tried write to a replica of segment <%s,%lu> but "
            "no such replica was open on this backup (server id %s)",
            masterId.toString().c_str(), segmentId,
            serverId.toString().c_str());
        throw BackupBadSegmentIdException(HERE);
    } else {
        CycleCounter<RawMetric> __(&metrics->backup.writeCopyTicks);
        Tub<BackupReplicaMetadata> metadata;
        if (reqHdr->certificateIncluded) {
            metadata.construct(reqHdr->certificate,
                               masterId.getId(), segmentId,
                               segmentSize,
                               reqHdr->segmentEpoch,
                               reqHdr->close, reqHdr->primary);
        }
        frame->append(*rpc->requestPayload, sizeof(*reqHdr),
                      reqHdr->length, reqHdr->offset,
                      metadata.get(), sizeof(*metadata));
        metrics->backup.writeCopyBytes += reqHdr->length;
        PerfStats::threadStats.backupBytesReceived += reqHdr->length;
        bytesWritten += reqHdr->length;
    }

    // Perform close, if any.
    if (reqHdr->close) {
        LOG(DEBUG, "Closing <%s,%lu>", masterId.toString().c_str(), segmentId);
        frame->close();
    }
}

/**
 * Runs garbage collection tasks.
 */
void
BackupService::gcMain()
try {
    taskQueue.performTasksUntilHalt();
} catch (const std::exception& e) {
    LOG(ERROR, "Fatal error in BackupService::gcThread: %s", e.what());
    throw;
} catch (...) {
    LOG(ERROR, "Unknown fatal error in BackupService::gcThread.");
    throw;
}

/**
 * Start garbage collection of replicas for any server that gets removed
 * from the cluster. Called when changes to the server wide server list
 * are enqueued.
 */
void
BackupService::trackerChangesEnqueued()
{
    // Start the replica garbage collector if it hasn't been started yet.
    // Starting late ensures that garbage collection tasks don't get
    // processed before the first push to the server list from the coordinator.
    if (!initCalled)
        return;
    if (!gcThread && !testingDoNotStartGcThread) {
        LOG(NOTICE, "Starting backup replica garbage collector thread");
        gcThread.construct(&BackupService::gcMain, this);
    }
    ServerDetails server;
    ServerChangeEvent event;
    while (gcTracker.getChange(server, event)) {
        if (event == SERVER_REMOVED) {
            (new GarbageCollectDownServerTask(*this,
                                              server.serverId))->schedule();
        }
    }
}

// --- BackupService::GarbageCollectReplicasFoundOnStorageTask ---

/**
 * Try to garbage collect a replica found on disk until it is finally
 * removed. Usually replicas are freed explicitly by masters, but this
 * doesn't work for cases where the replica was found on disk as part
 * of an old master.
 *
 * This task may generate RPCs to the master to determine the
 * status of the replica which survived on-storage across backup
 * failures.
 *
 * \param service
 *      Backup which is trying to garbage collect the replica.
 * \param masterId
 *      Id of the master which originally created the replica.
 */
BackupService::
    GarbageCollectReplicasFoundOnStorageTask::
    GarbageCollectReplicasFoundOnStorageTask(BackupService& service,
                                            ServerId masterId)
    : Task(service.taskQueue)
    , service(service)
    , masterId(masterId)
    , segmentIds()
    , rpc()
{
}

/**
 * Add a segmentId for a replica which should be considered for garbage
 * collection. Garbage collection won't actually start until schedule() is
 * called. All calls to addSegment must precede the call to schedule() the
 * task.
 *
 * \param segmentId
 *      Id of the segment a particular replica is associated with that was
 *      found on disk storage. Once this task is scheduled it will be
 *      considered for garbage collection until freed.
 */
void
BackupService::GarbageCollectReplicasFoundOnStorageTask::
    addSegmentId(uint64_t segmentId)
{
    segmentIds.push_back(segmentId);
    service.oldReplicas++;
}

/**
 * Try to make progress in garbage collecting replicas without blocking.
 * Attempts one replica at a time in order to prevent flooding the
 * master this task is associated with.
 */
void
BackupService::GarbageCollectReplicasFoundOnStorageTask::performTask()
{
    if (!service.config->backup.gc || segmentIds.empty()) {
        delete this;
        return;
    }
    uint64_t segmentId = segmentIds.front();
    bool done = tryToFreeReplica(segmentId);
    if (done) {
        segmentIds.pop_front();
        service.oldReplicas--;
    }
    schedule();
}

/**
 * Only used internally; try to make progress in garbage collecting one replica
 * without blocking.
 *
 * \param segmentId
 *      Id of the segment of the replica which is on the chopping block. Once
 *      a value is passed as \a segmentId that value must be passed on all
 *      subsequent calls until true is returned.
 * \return
 *      True if no additional work is need to free the segment from storage,
 *      false otherwise. See important notes on \a segmentId.
 */
bool
BackupService::GarbageCollectReplicasFoundOnStorageTask::
    tryToFreeReplica(uint64_t segmentId)
{
    // First, see if this replica still needs to be freed.
    {
        BackupService::Lock _(service.mutex);
        auto frameIt = service.frames.find({masterId, segmentId});
        if (frameIt == service.frames.end()) {
            // Frame no longer exists; no need to free.
            return true;
        }
    }

    // Due to RAM-447 it is a bit tricky to decipher when a server is in
    // crashed status. It is done here outside the context of the
    // ServerNotUpException because of that bug.
    if (service.context->serverList->contains(masterId) &&
        !service.context->serverList->isUp(masterId))
    {
        // In the server list but not up implies crashed.
        // Since server has crashed just let
        // GarbageCollectDownServerTask free it. It will get
        // scheduled when master recovery finishes for masterId.
        LOG(DEBUG, "Server %s marked crashed; waiting for cluster "
            "to recover from its failure before freeing <%s,%lu>",
            masterId.toString().c_str(), masterId.toString().c_str(),
            segmentId);
        return true;
    }

    if (rpc) {
        if (rpc->isReady()) {
            bool needed = true;
            try {
                needed = rpc->wait();
            } catch (const ServerNotUpException& e) {
                needed = false;
                LOG(DEBUG, "Server %s marked down; cluster has recovered "
                    "from its failure", masterId.toString().c_str());
            }
            rpc.destroy();
            if (!needed) {
                LOG(NOTICE, "Server has recovered from lost replica; "
                    "freeing replica for <%s,%lu> (%d more old replicas left)",
                    masterId.toString().c_str(), segmentId,
                    service.oldReplicas-1);
                deleteReplica(segmentId);
                return true;
            } else {
                LOG(DEBUG, "Server has not recovered from lost "
                    "replica; retaining replica for <%s,%lu>; will "
                    "probe replica status again later",
                    masterId.toString().c_str(), segmentId);
            }
        }
    } else {
        rpc.construct(service.context, masterId, service.serverId, segmentId);
    }
    return false;
}

/**
 * Only used internally; safely frees a replica and removes metadata about
 * it from the backup's frame map.
 */
void
BackupService::GarbageCollectReplicasFoundOnStorageTask::
    deleteReplica(uint64_t segmentId)
{
    BackupService::Lock _(service.mutex);
    auto frameIt = service.frames.find({masterId, segmentId});
    if (frameIt == service.frames.end())
        return;
    if (frameIt->second->wasAppendedToByCurrentProcess()) {
        // This frame has been called back into active service (e.g.
        // its master decided to rereplicate that frame on this
        // machine; see RAM-573). This means we need to retain
        // the frame indefinitely.
        LOG(NOTICE, "Old replica for <%s,%lu> has been called back "
                "into service, so won't garbage-collect it (%d more "
                "old replicas left)",
                masterId.toString().c_str(), segmentId,
                service.oldReplicas-1);
        return;
    }
    service.frames.erase(frameIt);
}

// --- BackupService::GarbageCollectDownServerTask ---

/**
 * Try to garbage collect all replicas stored by a master which is now
 * known to have been completely recovered and removed from the cluster.
 * Usually replicas are freed explicitly by masters, but this
 * doesn't work for cases where the replica was created by a master which
 * has crashed.
 *
 * \param service
 *      Backup trying to garbage collect replicas from some removed master.
 * \param masterId
 *      Id of the master now known to have been removed from the cluster.
 */
BackupService::
    GarbageCollectDownServerTask::
    GarbageCollectDownServerTask(BackupService& service,
                                 ServerId masterId)
    : Task(service.taskQueue)
    , service(service)
    , masterId(masterId)
{}

/**
 * Try to free up state that was allocated for the downed master. Includes any
 * replicas stored by it and any recovery state used to recover it. Doesn't
 * block.
 */
void
BackupService::GarbageCollectDownServerTask::performTask()
{
    BackupService::Lock _(service.mutex);
    // First, tell any ongoing recoveries for that master to clean up.
    auto recoveryIt = service.recoveries.find(masterId);
    if (recoveryIt != service.recoveries.end()) {
        BackupMasterRecovery* recovery = recoveryIt->second;
        service.recoveries.erase(recoveryIt);
        recovery->free();
    }

    // Then, if replica garbage collection is enabled, clean up replicas stored
    // for that now irrelevant master.
    if (!service.config->backup.gc) {
        delete this;
        return;
    }
    auto key = MasterSegmentIdPair(masterId, 0lu);
    auto it = service.frames.upper_bound(key);
    if (it != service.frames.end() && it->first.masterId == masterId) {
        LOG(DEBUG, "Server %s marked down; cluster has recovered "
                "from its failure; freeing replica <%s,%lu>",
            masterId.toString().c_str(), masterId.toString().c_str(),
            it->first.segmentId);
        service.frames.erase(it->first);
        schedule();
    } else {
        delete this;
    }
}

} // namespace RAMCloud
