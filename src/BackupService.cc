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

#include <thread>

#include "BackupReplica.h"
#include "BackupService.h"
#include "BackupStorage.h"
#include "Buffer.h"
#include "ClientException.h"
#include "Cycles.h"
#include "Log.h"
#include "LogEntryTypes.h"
#include "MasterClient.h"
#include "Object.h"
#include "ServerConfig.h"
#include "SegmentIterator.h"
#include "ShortMacros.h"
#include "Status.h"
#include "TransportManager.h"

namespace RAMCloud {

namespace {

/**
 * The TSC reading at the start of the last recovery.
 * Used to update #RawMetrics.backup.readingDataTicks.
 */
uint64_t recoveryStart;
} // anonymous namespace

// --- BackupService::IoScheduler ---

/**
 * Construct an IoScheduler.  There is just one instance of this
 * with its operator() running in a new thread which is instantated
 * by the BackupService.
 */
BackupService::IoScheduler::IoScheduler()
    : queueMutex()
    , queueCond()
    , loadQueue()
    , storeQueue()
    , running(true)
    , outstandingStores(0)
{
}

/**
 * Dequeue IO requests and process them on a thread separate from the
 * main thread.  Processes just one IO at a time.  Prioritizes loads
 * over stores.
 */
void
BackupService::IoScheduler::operator()()
try {
    while (true) {
        BackupReplica* replica = NULL;
        bool isLoad = false;
        {
            Lock lock(queueMutex);
            while (loadQueue.empty() && storeQueue.empty()) {
                if (!running)
                    return;
                queueCond.wait(lock);
            }
            isLoad = !loadQueue.empty();
            if (isLoad) {
                replica = loadQueue.front();
                loadQueue.pop();
            } else {
                replica = storeQueue.front();
                storeQueue.pop();
            }
        }

        if (isLoad) {
            LOG(DEBUG, "Dispatching load of <%s,%lu>",
                replica->masterId.toString().c_str(), replica->segmentId);
            doLoad(*replica);
        } else {
            LOG(DEBUG, "Dispatching store of <%s,%lu>",
                replica->masterId.toString().c_str(), replica->segmentId);
            doStore(*replica);
        }
    }
} catch (const std::exception& e) {
    LOG(ERROR, "Fatal error in BackupService::IoScheduler: %s", e.what());
    throw;
} catch (...) {
    LOG(ERROR, "Unknown fatal error in BackupService::IoScheduler.");
    throw;
}

/**
 * Queue a segment load operation to be done on a separate thread.
 *
 * \param replica
 *      The BackupReplica whose data will be loaded from storage.
 */
void
BackupService::IoScheduler::load(BackupReplica& replica)
{
#ifdef SINGLE_THREADED_BACKUP
    doLoad(replica);
#else
    Lock lock(queueMutex);
    loadQueue.push(&replica);
    uint32_t count = downCast<uint32_t>(loadQueue.size() + storeQueue.size());
    LOG(DEBUG, "Queued load of <%s,%lu> (%u segments waiting for IO)",
        replica.masterId.toString().c_str(), replica.segmentId, count);
    queueCond.notify_all();
#endif
}

/**
 * Flush all data to storage.
 * Returns once all dirty buffers have been written to storage.
 */
void
BackupService::IoScheduler::quiesce()
{
    while (outstandingStores > 0) {
        /* pass */;
    }
}

/**
 * Queue a segment store operation to be done on a separate thread.
 *
 * \param replica
 *      The BackupReplica whose data will be stored.
 */
void
BackupService::IoScheduler::store(BackupReplica& replica)
{
#ifdef SINGLE_THREADED_BACKUP
    doStore(replica);
#else
    ++outstandingStores;
    Lock lock(queueMutex);
    storeQueue.push(&replica);
    uint32_t count = downCast<uint32_t>(loadQueue.size() + storeQueue.size());
    LOG(DEBUG, "Queued store of <%s,%lu> (%u segments waiting for IO)",
        replica.masterId.toString().c_str(), replica.segmentId, count);
    queueCond.notify_all();
#endif
}

/**
 * Wait for the IO scheduler to finish currently queued requests and
 * return once the scheduler has terminated and its thread is disposed
 * of.
 *
 * \param ioThread
 *      The thread this IoScheduler is running on.  It will be joined
 *      to ensure the scheduler has fully exited before this call
 *      returns.
 */
void
BackupService::IoScheduler::shutdown(std::thread& ioThread)
{
    {
        Lock lock(queueMutex);
        LOG(DEBUG, "IoScheduler thread exiting");
        uint32_t count = downCast<uint32_t>(loadQueue.size() +
                                            storeQueue.size());
        if (count)
            LOG(DEBUG, "IoScheduler must service %u pending IOs before exit",
                count);
        running = false;
        queueCond.notify_one();
    }
    ioThread.join();
}

// - private -

/**
 * Load a segment from disk into a valid buffer in memory.  Locks
 * the #BackupReplica::mutex to ensure other operations aren't performed
 * on the segment in the meantime.
 *
 * \param replica
 *      The BackupReplica whose data will be loaded from storage.
 */
void
BackupService::IoScheduler::doLoad(BackupReplica& replica) const
{
#ifndef SINGLE_THREADED_BACKUP
    BackupReplica::Lock lock(replica.mutex);
#endif
    ReferenceDecrementer<int> _(replica.storageOpCount);

    LOG(DEBUG, "Loading segment <%s,%lu>",
        replica.masterId.toString().c_str(), replica.segmentId);
    if (replica.inMemory()) {
        LOG(DEBUG, "Already in memory, skipping load on <%s,%lu>",
            replica.masterId.toString().c_str(), replica.segmentId);
        replica.condition.notify_all();
        return;
    }

    char* segment = static_cast<char*>(replica.pool.malloc());
    try {
        CycleCounter<RawMetric> _(&metrics->backup.storageReadTicks);
        ++metrics->backup.storageReadCount;
        metrics->backup.storageReadBytes += replica.segmentSize;
        uint64_t startTime = Cycles::rdtsc();
        replica.storage.getSegment(replica.storageHandle, segment);
        uint64_t transferTime = Cycles::toNanoseconds(Cycles::rdtsc() -
            startTime);
        LOG(DEBUG, "Load of <%s,%lu> took %lu us (%f MB/s)",
            replica.masterId.toString().c_str(), replica.segmentId,
            transferTime / 1000,
            (replica.segmentSize / (1 << 20)) /
            (static_cast<double>(transferTime) / 1000000000lu));
    } catch (...) {
        replica.pool.free(segment);
        segment = NULL;
        replica.condition.notify_all();
        throw;
    }
    replica.segment = segment;
    replica.condition.notify_all();
    metrics->backup.readingDataTicks = Cycles::rdtsc() - recoveryStart;
}

/**
 * Store a segment to disk from a valid buffer in memory.  Locks
 * the #BackupReplica::mutex to ensure other operations aren't performed
 * on the segment in the meantime.
 *
 * \param replica
 *      The BackupReplica whose data will be stored.
 */
void
BackupService::IoScheduler::doStore(BackupReplica& replica) const
{
#ifndef SINGLE_THREADED_BACKUP
    BackupReplica::Lock lock(replica.mutex);
#endif
    ReferenceDecrementer<int> _(replica.storageOpCount);

    LOG(DEBUG, "Storing segment <%s,%lu>",
        replica.masterId.toString().c_str(), replica.segmentId);
    try {
        CycleCounter<RawMetric> _(&metrics->backup.storageWriteTicks);
        ++metrics->backup.storageWriteCount;
        metrics->backup.storageWriteBytes += replica.segmentSize;
        uint64_t startTime = Cycles::rdtsc();
        replica.storage.putSegment(replica.storageHandle, replica.segment);
        uint64_t transferTime = Cycles::toNanoseconds(Cycles::rdtsc() -
            startTime);
        LOG(DEBUG, "Store of <%s,%lu> took %lu us (%f MB/s)",
            replica.masterId.toString().c_str(), replica.segmentId,
            transferTime / 1000,
            (replica.segmentSize / (1 << 20)) /
            (static_cast<double>(transferTime) / 1000000000lu));
    } catch (...) {
        LOG(WARNING, "Problem storing segment <%s,%lu>",
            replica.masterId.toString().c_str(), replica.segmentId);
        replica.condition.notify_all();
        throw;
    }
    replica.pool.free(replica.segment);
    replica.segment = NULL;
    --outstandingStores;
    LOG(DEBUG, "Done storing segment <%s,%lu>",
        replica.masterId.toString().c_str(), replica.segmentId);
    replica.condition.notify_all();
}


// --- BackupService::RecoverySegmentBuilder ---

/**
 * Constructs a RecoverySegmentBuilder which handles recovery
 * segment construction for a single recovery.
 * Makes a copy of each of the arguments as a thread local copy
 * for use as the thread runs.
 *
 * \param context
 *      The context in which the new thread will run (same as that of the
 *      parent thread).
 * \param replicas
 *      Pointers to BackupReplica which will be loaded from storage
 *      and split into recovery segments.  Notice, the copy here is
 *      only of the pointer vector and not of the BackupReplica objects.
 *      Sharing of the BackupReplica objects is controlled as described
 *      in RecoverySegmentBuilder::replicas.
 * \param partitions
 *      The partitions among which the segment should be split for recovery.
 * \param recoveryThreadCount
 *      Reference to an atomic count for tracking number of running recoveries.
 * \param segmentSize
 *      The size of segments stored on masters and in the backup's storage.
 */
BackupService::RecoverySegmentBuilder::RecoverySegmentBuilder(
        Context& context,
        const vector<BackupReplica*>& replicas,
        const ProtoBuf::Tablets& partitions,
        Atomic<int>& recoveryThreadCount,
        uint32_t segmentSize)
    : context(context)
    , replicas(replicas)
    , partitions(partitions)
    , recoveryThreadCount(recoveryThreadCount)
    , segmentSize(segmentSize)
{
}

/**
 * In order, load each of the segments into memory, meanwhile constructing
 * the recovery segments for the previously loaded segment.
 *
 * Notice this runs in a separate thread and maintains exclusive access to the
 * BackupReplica objects using #BackupReplica::mutex.  As
 * the recovery segments for each BackupReplica are constructed ownership of the
 * BackupReplica is released and #BackupReplica::condition is notified to wake
 * up any threads waiting for the recovery segments.
 */
void
BackupService::RecoverySegmentBuilder::operator()()
try {
    uint64_t startTime = Cycles::rdtsc();
    ReferenceDecrementer<Atomic<int>> _(recoveryThreadCount);
    LOG(DEBUG, "Building recovery segments on new thread");

    if (replicas.empty())
        return;

    BackupReplica* loadingInfo = replicas[0];
    BackupReplica* buildingInfo = NULL;

    // Bring the first segment into memory
    loadingInfo->startLoading();

    for (uint32_t i = 1;; i++) {
        assert(buildingInfo != loadingInfo);
        buildingInfo = loadingInfo;
        if (i < replicas.size()) {
            loadingInfo = replicas[i];
            LOG(DEBUG, "Starting load of %uth segment (<%s,%lu>)", i,
                loadingInfo->masterId.toString().c_str(),
                loadingInfo->segmentId);
            loadingInfo->startLoading();
        }
        buildingInfo->buildRecoverySegments(partitions);
        LOG(DEBUG, "Done building recovery segments for %uth segment "
            "(<%s,%lu>)", i - 1,
            buildingInfo->masterId.toString().c_str(), buildingInfo->segmentId);
        if (i == replicas.size())
            break;
    }
    LOG(DEBUG, "Done building recovery segments, thread exiting");
    uint64_t totalTime = Cycles::toNanoseconds(Cycles::rdtsc() - startTime);
    LOG(DEBUG, "RecoverySegmentBuilder took %lu ms to filter %lu segments "
               "(%f MB/s)",
        totalTime / 1000 / 1000,
        replicas.size(),
        static_cast<double>(segmentSize * replicas.size() / (1 << 20)) /
        static_cast<double>(totalTime) / 1e9);
} catch (const std::exception& e) {
    LOG(ERROR, "Fatal error in BackupService::RecoverySegmentBuilder: %s",
        e.what());
    throw;
} catch (...) {
    LOG(ERROR, "Unknown fatal error in BackupService::RecoverySegmentBuilder.");
    throw;
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
    : Task(service.gcTaskQueue)
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
}

/**
 * Try to make progress in garbage collecting replicas without blocking.
 * Attempts one replica at a time in order to prevent flooding the
 * master this task is associated with.
 */
void
BackupService::GarbageCollectReplicasFoundOnStorageTask::performTask()
{
    if (!service.config.backup.gc || segmentIds.empty()) {
        delete this;
        return;
    }
    uint64_t segmentId = segmentIds.front();
    bool done = tryToFreeReplica(segmentId);
    if (done)
        segmentIds.pop_front();
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
    {
        BackupService::Lock _(service.mutex);
        auto* replica = service.findBackupReplica(masterId, segmentId);
        if (!replica)
            return true;
    }

    // Due to RAM-447 it is a bit tricky to decipher when a server is in
    // crashed status. It is done here outside the context of the
    // ServerDoesntExistException because of that bug.
    if (service.context.serverList->contains(masterId) &&
        !service.context.serverList->isUp(masterId))
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
            } catch (const ServerDoesntExistException& e) {
                needed = false;
                LOG(DEBUG, "Server %s marked down; cluster has recovered "
                    "from its failure", masterId.toString().c_str());
            }
            rpc.destroy();
            if (!needed) {
                LOG(DEBUG, "Server has recovered from lost replica; "
                    "freeing replica for <%s,%lu>",
                    masterId.toString().c_str(), segmentId);
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
 * it from the backup's segments map.
 */
void
BackupService::GarbageCollectReplicasFoundOnStorageTask::
    deleteReplica(uint64_t segmentId)
{
    BackupReplica* replica = NULL;
    {
        BackupService::Lock _(service.mutex);
        replica = service.findBackupReplica(masterId, segmentId);
        if (!replica)
            return;
        service.segments.erase({masterId, segmentId});
    }
    replica->free();
    delete replica;
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
    : Task(service.gcTaskQueue)
    , service(service)
    , masterId(masterId)
{}

/**
 * Try to make progress in garbage collecting replicas without blocking.
 */
void
BackupService::GarbageCollectDownServerTask::performTask()
{
    if (!service.config.backup.gc) {
        delete this;
        return;
    }
    BackupService::Lock _(service.mutex);
    auto key = MasterSegmentIdPair(masterId, 0lu);
    auto it = service.segments.upper_bound(key);
    if (it != service.segments.end() && it->second->masterId == masterId) {
        LOG(DEBUG, "Server %s marked down; cluster has recovered "
                "from its failure; freeing replica <%s,%lu>",
            masterId.toString().c_str(), masterId.toString().c_str(),
            it->first.segmentId);
        it->second->free();
        service.segments.erase(it->first);
        delete it->second;
        schedule();
    } else {
        delete this;
    }
}

// --- BackupService ---

/**
 * Create a BackupService.
 *
 * \param context
 *      Overall information about the RAMCloud server.
 * \param config
 *      Settings for this instance. The caller guarantees that config will
 *      exist for the duration of this BackupService's lifetime.
 */
BackupService::BackupService(Context& context,
                             const ServerConfig& config)
    : context(context)
    , mutex()
    , config(config)
    , formerServerId()
    , serverId(0)
    , recoveryTicks()
    , pool(config.segmentSize)
    , recoveryThreadCount{0}
    , segments()
    , segmentSize(config.segmentSize)
    , storage()
    , storageBenchmarkResults()
    , bytesWritten(0)
    , ioScheduler()
#ifdef SINGLE_THREADED_BACKUP
    , ioThread()
#else
    , ioThread(std::ref(ioScheduler))
#endif
    , initCalled(false)
    , replicationId(0)
    , replicationGroup()
    , gcTracker(context, this)
    , gcThread()
    , testingDoNotStartGcThread(false)
    , gcTaskQueue()
{
    if (config.backup.inMemory)
        storage.reset(new InMemoryStorage(config.segmentSize,
                                          config.backup.numSegmentFrames));
    else
        storage.reset(new SingleFileStorage(config.segmentSize,
                                            config.backup.numSegmentFrames,
                                            config.backup.file.c_str(),
                                            O_DIRECT | O_SYNC));

    try {
        recoveryTicks.construct(); // make unit tests happy

        // Prime the segment pool. This removes an overhead that would
        // otherwise be seen during the first recovery.
        void* mem[100];
        foreach(auto& m, mem)
            m = pool.malloc();
        foreach(auto& m, mem)
            pool.free(m);
    } catch (...) {
        ioScheduler.shutdown(ioThread);
        throw;
    }

    BackupStorage::Superblock superblock = storage->loadSuperblock();
    if (config.clusterName == "__unnamed__") {
        LOG(NOTICE, "Cluster '__unnamed__'; ignoring existing backup storage. "
            "Any replicas stored will not be reusable by future backups. "
            "Specify clusterName for persistence across backup restarts.");
    } else {
        LOG(NOTICE, "Backup storing replicas with clusterName '%s'. Future "
            "backups must be restarted with the same clusterName for replicas "
            "stored on this backup to be reused.", config.clusterName.c_str());
        if (config.clusterName == superblock.getClusterName()) {
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
            killAllStorage();
        }
    }
}

/**
 * Flush open segments to disk and shutdown.
 */
BackupService::~BackupService()
{
    // Stop the garbage collector.
    gcTaskQueue.halt();
    if (gcThread)
        gcThread->join();
    gcThread.destroy();

    int lastThreadCount = 0;
    while (recoveryThreadCount > 0) {
        if (lastThreadCount != recoveryThreadCount) {
            LOG(DEBUG, "Waiting for recovery threads to terminate before "
                "deleting BackupService, %d threads "
                "still running", static_cast<int>(recoveryThreadCount));
            lastThreadCount = recoveryThreadCount;
        }
    }
    Fence::enter();

    ioScheduler.shutdown(ioThread);

    // Clean up any extra BackupReplicas laying around
    // but don't free them; perhaps we want to load them up
    // on a cold start.  The replicas destructor will ensure the
    // segment is properly stored before termination.
    foreach (const SegmentsMap::value_type& value, segments) {
        BackupReplica* replica = value.second;
        delete replica;
    }
    segments.clear();
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
 * the coordinator during init() so that masters can intelligently select
 * backups.
 *
 * This is not part of the constructor because we don't want the benchmark
 * running during unit tests. It's not part of init() because that method must
 * be fast (right now, as soon as init runs for some service, clients can start
 * accessing the service; however, the dispatch loop won't run until init
 * completes for all services).
 */
void
BackupService::benchmark(uint32_t& readSpeed, uint32_t& writeSpeed) {
    auto strategy = static_cast<BackupStrategy>(config.backup.strategy);
    storageBenchmarkResults = storage->benchmark(strategy);
    readSpeed = storageBenchmarkResults.first;
    writeSpeed = storageBenchmarkResults.second;
}

// See Server::dispatch.
void
BackupService::dispatch(WireFormat::Opcode opcode, Rpc& rpc)
{
    Lock _(mutex); // Lock out GC while any RPC is being processed.

    // This is a hack. We allow the AssignGroup Rpc to be processed before
    // initCalled is set to true, since it is sent during initialization.
    assert(initCalled || opcode == WireFormat::BackupAssignGroup::opcode);
    CycleCounter<RawMetric> serviceTicks(&metrics->backup.serviceTicks);

    switch (opcode) {
        case WireFormat::BackupAssignGroup::opcode:
            callHandler<WireFormat::BackupAssignGroup, BackupService,
                        &BackupService::assignGroup>(rpc);
            break;
        case WireFormat::BackupFree::opcode:
            callHandler<WireFormat::BackupFree, BackupService,
                        &BackupService::freeSegment>(rpc);
            break;
        case WireFormat::BackupGetRecoveryData::opcode:
            callHandler<WireFormat::BackupGetRecoveryData, BackupService,
                        &BackupService::getRecoveryData>(rpc);
            break;
        case WireFormat::BackupQuiesce::opcode:
            callHandler<WireFormat::BackupQuiesce, BackupService,
                        &BackupService::quiesce>(rpc);
            break;
        case WireFormat::BackupRecoveryComplete::opcode:
            callHandler<WireFormat::BackupRecoveryComplete, BackupService,
                        &BackupService::recoveryComplete>(rpc);
            break;
        case WireFormat::BackupStartReadingData::opcode:
            callHandler<WireFormat::BackupStartReadingData, BackupService,
                        &BackupService::startReadingData>(rpc);
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
 * Assign a replication group to a backup, and notifies it of its peer group
 * members. The replication group serves as a set of backups that store all
 * the replicas of a segment.
 *
 * \param reqHdr
 *      Header of the Rpc request containing the replication group Ids.
 *
 * \param respHdr
 *      Header for the Rpc response.
 *
 * \param rpc
 *      The Rpc being serviced.
 */
void
BackupService::assignGroup(
        const WireFormat::BackupAssignGroup::Request& reqHdr,
        WireFormat::BackupAssignGroup::Response& respHdr,
        Rpc& rpc)
{
    replicationId = reqHdr.replicationId;
    uint32_t reqOffset = sizeof32(reqHdr);
    replicationGroup.clear();
    for (uint32_t i = 0; i < reqHdr.numReplicas; i++) {
        const uint64_t *backupId =
            rpc.requestPayload.getOffset<uint64_t>(reqOffset);
        replicationGroup.push_back(ServerId(*backupId));
        reqOffset += sizeof32(*backupId);
    }
}

/**
 * Removes the specified segment from permanent storage and releases
 * any in memory copy if it exists.
 *
 * After this call completes the segment number segmentId will no longer
 * be in permanent storage and will not be recovered during recovery.
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
BackupService::freeSegment(const WireFormat::BackupFree::Request& reqHdr,
                           WireFormat::BackupFree::Response& respHdr,
                           Rpc& rpc)
{
    ServerId masterId(reqHdr.masterId);
    LOG(DEBUG, "Freeing replica for master %s segment %lu",
        masterId.toString().c_str(), reqHdr.segmentId);

    SegmentsMap::iterator it =
        segments.find(MasterSegmentIdPair(masterId, reqHdr.segmentId));
    if (it == segments.end()) {
        LOG(WARNING, "Master tried to free non-existent segment: <%s,%lu>",
            masterId.toString().c_str(), reqHdr.segmentId);
        return;
    }

    it->second->free();
    segments.erase(it);
    delete it->second;
}

/**
 * Find BackupReplica for a segment or NULL if we don't know about it.
 *
 * \param masterId
 *      The master id of the master of the segment whose info is being sought.
 * \param segmentId
 *      The segment id of the segment whose info is being sought.
 * \return
 *      A pointer to the BackupReplica for the specified segment or NULL if
 *      none exists.
 */
BackupReplica*
BackupService::findBackupReplica(ServerId masterId, uint64_t segmentId)
{
    SegmentsMap::iterator it =
        segments.find(MasterSegmentIdPair(masterId, segmentId));
    if (it == segments.end())
        return NULL;
    return it->second;
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
    const WireFormat::BackupGetRecoveryData::Request& reqHdr,
    WireFormat::BackupGetRecoveryData::Response& respHdr,
    Rpc& rpc)
{
    ServerId masterId(reqHdr.masterId);
    LOG(DEBUG, "getRecoveryData masterId %s, segmentId %lu, partitionId %lu",
        masterId.toString().c_str(),
        reqHdr.segmentId, reqHdr.partitionId);

    BackupReplica* replica = findBackupReplica(masterId, reqHdr.segmentId);
    if (!replica) {
        LOG(WARNING, "Asked for bad segment <%s,%lu>",
            masterId.toString().c_str(), reqHdr.segmentId);
        throw BackupBadSegmentIdException(HERE);
    }

    Status status = replica->appendRecoverySegment(reqHdr.partitionId,
                                                rpc.replyPayload);
    if (status != STATUS_OK) {
        respHdr.common.status = status;
        return;
    }

    ++metrics->backup.readCompletionCount;
    LOG(DEBUG, "getRecoveryData complete");
}

/**
 * Perform once-only initialization for the backup service after having
 * enlisted the process with the coordinator
 */
void
BackupService::init(ServerId id)
{
    assert(!initCalled);

    serverId = id;
    LOG(NOTICE, "My server ID is %s", id.toString().c_str());
    if (metrics->serverId == 0) {
        metrics->serverId = *serverId;
    }

    storage->resetSuperblock(serverId, config.clusterName);
    LOG(NOTICE, "Backup %s will store replicas under cluster name '%s'",
        serverId.toString().c_str(), config.clusterName.c_str());
    initCalled = true;
}

/**
 * Scribble a bit over all the headers of all the segment frames to prevent
 * what is already on disk from being reused in future runs.
 * This is called whenever the user requests to not use the stored segment
 * frames.  Not doing this could yield interesting surprises to the user.
 * For example, one might find replicas for the same segment id with
 * different contents during future runs.
 */
void
BackupService::killAllStorage()
{
    CycleCounter<> killTime;
    for (uint32_t frame = 0; frame < config.backup.numSegmentFrames; ++frame) {
        std::unique_ptr<BackupStorage::Handle>
            handle(storage->associate(frame));
        if (handle)
            storage->free(handle.release());
    }

    LOG(NOTICE, "On-storage replica destroyed: %f ms",
        1000. * Cycles::toSeconds(killTime.stop()));
}

/**
 * Flush all data to storage.
 * Returns once all dirty buffers have been written to storage.
 * \param reqHdr
 *      Header of the Rpc request.
 * \param respHdr
 *      Header for the Rpc response.
 * \param rpc
 *      The Rpc being serviced.
 */
void
BackupService::quiesce(const WireFormat::BackupQuiesce::Request& reqHdr,
                       WireFormat::BackupQuiesce::Response& respHdr,
                       Rpc& rpc)
{
    TEST_LOG("Backup at %s quiescing", config.localLocator.c_str());
    ioScheduler.quiesce();
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
        const WireFormat::BackupRecoveryComplete::Request& reqHdr,
        WireFormat::BackupRecoveryComplete::Response& respHdr,
        Rpc& rpc)
{
    LOG(DEBUG, "masterID: %s",
        ServerId(reqHdr.masterId).toString().c_str());
    rpc.sendReply();
    recoveryTicks.destroy();
}

/**
 * Scans storage to find replicas left behind by former backups and
 * incorporates them into this backup.  This should only be called before
 * the backup has serviced any requests.
 *
 * Only replica headers and footers are scanned.  Any replica with a valid
 * header but no discernable footer is included and considered open.  If a
 * header is found and a reasonable footer then the segment is considered
 * closed.  At some point backups will want to write a checksum even for open
 * segments so we'll need some changes to the logic of this method to deal
 * with that.  Also, at that point we should augment the checksum to cover
 * the log entry type of the footer as well.
 */
void
BackupService::restartFromStorage()
{
    CycleCounter<> restartTime;

// This is all broken. BackupService should not dig so deeply into the internals
// of segments.
    return;
#if 0
    struct HeaderAndFooter {
        SegmentEntry headerEntry;
        SegmentHeader header;
        SegmentEntry footerEntry;
        SegmentFooter footer;
    } __attribute__ ((packed));
    assert(sizeof(HeaderAndFooter) ==
           2 * sizeof(SegmentEntry) +
           sizeof(SegmentHeader) +
           sizeof(SegmentFooter));

    std::unique_ptr<char[]> allHeadersAndFooters =
        storage->getAllHeadersAndFooters(
            sizeof(SegmentEntry) + sizeof(SegmentHeader),
            sizeof(SegmentEntry) + sizeof(SegmentFooter));

    if (!allHeadersAndFooters) {
        LOG(NOTICE, "Selected backup storage does not support restart of "
            "backups, continuing as an empty backup");
        return;
    }

    const HeaderAndFooter* entries =
        reinterpret_cast<const HeaderAndFooter*>(allHeadersAndFooters.get());

    // Keeps track of which task is garbage collecting replicas for each
    // masterId.
    std::unordered_map<ServerId, GarbageCollectReplicasFoundOnStorageTask*>
        gcTasks;

    for (uint32_t frame = 0; frame < config.backup.numSegmentFrames; ++frame) {
        auto& entry = entries[frame];
        // Check header.
        if (entry.headerEntry.type != LOG_ENTRY_TYPE_SEGHEADER) {
            TEST_LOG("Log entry type for header does not match in frame %u",
                     frame);
            continue;
        }
        if (entry.headerEntry.length != sizeof32(SegmentHeader)) {
            LOG(WARNING, "Unexpected log entry length while reading segment "
                "replica header from backup storage, discarding replica, "
                "(expected length %lu, stored length %u)",
                sizeof(SegmentHeader), entry.headerEntry.length);
            continue;
        }
        if (entry.header.segmentCapacity != segmentSize) {
            LOG(ERROR, "An on-storage segment header indicates a different "
                "segment size than the backup service was told to use; "
                "ignoring the problem; note ANY call to open a segment "
                "on this backup could cause the overwrite of your strangely "
                "sized replicas.");
        }

        const uint64_t logId = entry.header.logId;
        const uint64_t segmentId = entry.header.segmentId;
        // Check footer.
        bool wasClosedOnStorage = false;
        if (entry.footerEntry.type == LOG_ENTRY_TYPE_SEGFOOTER &&
            entry.footerEntry.length == sizeof(SegmentFooter)) {
            wasClosedOnStorage = true;
        }
        // TODO(stutsman): Eventually will need open segment checksums.
        LOG(DEBUG, "Found stored replica <%s,%lu> on backup storage in "
                   "frame %u which was %s",
            ServerId(entry.header.logId).toString().c_str(),
            entry.header.segmentId, frame,
            wasClosedOnStorage ? "closed" : "open");

        // Add this replica to metadata.
        const ServerId masterId(logId);
        auto* replica = new BackupReplica(*storage, pool, ioScheduler,
                               masterId, segmentId, segmentSize,
                               frame, wasClosedOnStorage);
        segments[MasterSegmentIdPair(masterId, segmentId)] = replica;
        if (gcTasks.find(masterId) == gcTasks.end()) {
            gcTasks[masterId] =
                new GarbageCollectReplicasFoundOnStorageTask(*this, masterId);
        }
        gcTasks[masterId]->addSegmentId(segmentId);
    }

    // Kick off the garbage collection for replicas stored on disk.
    foreach (const auto& value, gcTasks)
        value.second->schedule();

    LOG(NOTICE, "Replica information retreived from storage: %f ms",
        1000. * Cycles::toSeconds(restartTime.stop()));
#endif
}

/**
 * Begin reading disk data for a Master and bucketing the objects in the
 * Segments according to a requested TabletMap, returning a list of backed
 * up Segments this backup has for the Master.
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
        const WireFormat::BackupStartReadingData::Request& reqHdr,
        WireFormat::BackupStartReadingData::Response& respHdr,
        Rpc& rpc)
{
    recoveryTicks.construct(&metrics->backup.recoveryTicks);
    recoveryStart = Cycles::rdtsc();
    metrics->backup.recoveryCount++;
    metrics->backup.storageType = static_cast<uint64_t>(storage->storageType);

    ProtoBuf::Tablets partitions;
    ProtoBuf::parseFromResponse(rpc.requestPayload, sizeof(reqHdr),
                                reqHdr.partitionsLength, partitions);
    LOG(DEBUG, "Backup preparing for recovery of crashed server %s; "
        "loading replicas and filtering them according to the following "
        "partitions:\n%s",
        ServerId(reqHdr.masterId).toString().c_str(),
        partitions.DebugString().c_str());

    uint64_t logDigestLastId = ~0UL;
    uint32_t logDigestLastLen = 0;
    Buffer logDigest;

    vector<BackupReplica*> primarySegments;
    vector<BackupReplica*> secondarySegments;

    for (SegmentsMap::iterator it = segments.begin();
         it != segments.end(); it++)
    {
        ServerId masterId = it->first.masterId;
        BackupReplica* replica = it->second;

        if (*masterId == reqHdr.masterId) {
            if (!replica->satisfiesAtomicReplicationGuarantees()) {
                LOG(WARNING, "Asked for replica <%s,%lu> which was being "
                    "replicated atomically but which has yet to be closed; "
                    "ignoring the replica",
                    masterId.toString().c_str(), replica->segmentId);
                continue;
            }
            (replica->primary ?
                primarySegments :
                secondarySegments).push_back(replica);
        }
    }

    // Shuffle the primary entries, this helps all recovery
    // masters to stay busy even if the log contains long sequences of
    // back-to-back segments that only have objects for a particular
    // partition.  Secondary order is irrelevant.
    std::random_shuffle(primarySegments.begin(),
                        primarySegments.end(),
                        randomNumberGenerator);

    typedef WireFormat::BackupStartReadingData::Replica Replica;

    bool allRecovered = true;
    foreach (auto replica, primarySegments) {
        new(&rpc.replyPayload, APPEND) Replica
            {replica->segmentId, replica->getRightmostWrittenOffset()};
        LOG(DEBUG, "Crashed master %s had segment %lu (primary) with len %u",
            replica->masterId.toString().c_str(), replica->segmentId,
            replica->getRightmostWrittenOffset());
        bool wasRecovering = replica->setRecovering();
        allRecovered &= wasRecovering;
    }
    foreach (auto replica, secondarySegments) {
        new(&rpc.replyPayload, APPEND) Replica
            {replica->segmentId, replica->getRightmostWrittenOffset()};
        LOG(DEBUG, "Crashed master %s had segment %lu (secondary) with len %u"
            ", stored partitions for deferred recovery segment construction",
            replica->masterId.toString().c_str(), replica->segmentId,
            replica->getRightmostWrittenOffset());
        bool wasRecovering = replica->setRecovering(partitions);
        allRecovered &= wasRecovering;
    }
    respHdr.segmentIdCount = downCast<uint32_t>(primarySegments.size() +
                                                secondarySegments.size());
    respHdr.primarySegmentCount = downCast<uint32_t>(primarySegments.size());
    LOG(DEBUG, "Sending %u segment ids for this master (%u primary)",
        respHdr.segmentIdCount, respHdr.primarySegmentCount);

    vector<BackupReplica*> allSegments[2] = {primarySegments,
                                             secondarySegments};
    foreach (auto segments, allSegments) {
        foreach (auto replica, segments) {
            // Obtain the LogDigest from the lowest Segment Id of any
            // open replica.
            if (replica->isOpen() && replica->segmentId <= logDigestLastId) {
                if (replica->getLogDigest(NULL)) {
                    logDigest.reset();
                    replica->getLogDigest(&logDigest);
                    logDigestLastId = replica->segmentId;
                    logDigestLastLen = replica->getRightmostWrittenOffset();
                    LOG(DEBUG, "Segment %lu's LogDigest queued for response",
                        replica->segmentId);
                }
            }
        }
    }

    respHdr.digestSegmentId  = logDigestLastId;
    respHdr.digestSegmentLen = logDigestLastLen;
    respHdr.digestBytes = logDigest.getTotalLength();
    if (respHdr.digestBytes > 0) {
        void* out = new(&rpc.replyPayload, APPEND) char[respHdr.digestBytes];
        memcpy(out,
               logDigest.getRange(0, respHdr.digestBytes),
               respHdr.digestBytes);
        LOG(DEBUG, "Sent %u bytes of LogDigest to coord", respHdr.digestBytes);
    }

    rpc.sendReply();

    // Don't spin up a thread if there isn't anything that needs to be built.
    if (allRecovered)
        return;

#ifndef SINGLE_THREADED_BACKUP
    RecoverySegmentBuilder builder(context,
                                   primarySegments,
                                   partitions,
                                   recoveryThreadCount,
                                   segmentSize);
    ++recoveryThreadCount;
    std::thread builderThread(builder);
    builderThread.detach();
    LOG(DEBUG, "Kicked off building recovery segments; "
               "main thread going back to dispatching requests");
#endif
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
BackupService::writeSegment(const WireFormat::BackupWrite::Request& reqHdr,
                            WireFormat::BackupWrite::Response& respHdr,
                            Rpc& rpc)
{
    ServerId masterId(reqHdr.masterId);
    uint64_t segmentId = reqHdr.segmentId;

    // The default value for numReplicas is 0.
    respHdr.numReplicas = 0;

    BackupReplica* replica = findBackupReplica(masterId, segmentId);
    if (replica && !replica->createdByCurrentProcess) {
        if ((reqHdr.flags & WireFormat::BackupWrite::OPEN)) {
            // This can happen if a backup crashes, restarts, reloads a
            // replica from disk, and then the master detects the crash and
            // tries to re-replicate the segment which lost a replica on
            // the restarted backup.
            LOG(NOTICE, "Master tried to open replica for <%s,%lu> but "
                "another replica was recovered from storage with the same id; "
                "rejecting open request", masterId.toString().c_str(),
                segmentId);
            throw BackupOpenRejectedException(HERE);
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
    if ((reqHdr.flags & WireFormat::BackupWrite::OPEN)) {
#ifdef SINGLE_THREADED_BACKUP
        bool primary = false;
#else
        bool primary = reqHdr.flags & WireFormat::BackupWrite::PRIMARY;
#endif
        if (primary) {
            uint32_t numReplicas = downCast<uint32_t>(
                replicationGroup.size());
            // Respond with the replication group members.
            respHdr.numReplicas = numReplicas;
            if (numReplicas > 0) {
                uint64_t* dest =
                    new(&rpc.replyPayload, APPEND) uint64_t[numReplicas];
                int i = 0;
                foreach(ServerId id, replicationGroup) {
                    dest[i] = id.getId();
                    i++;
                }
            }
        }
        if (!replica) {
            LOG(DEBUG, "Opening <%s,%lu>", masterId.toString().c_str(),
                segmentId);
            try {
                replica = new BackupReplica(*storage,
                                       pool,
                                       ioScheduler,
                                       masterId,
                                       segmentId,
                                       segmentSize,
                                       primary);
                segments[MasterSegmentIdPair(masterId, segmentId)] = replica;
                replica->open();
            } catch (const BackupStorageException& e) {
                segments.erase(MasterSegmentIdPair(masterId, segmentId));
                delete replica;
                LOG(NOTICE, "Master tried to open replica for <%s,%lu> but "
                    "there was a problem allocating storage space; "
                    "rejecting open request: %s", masterId.toString().c_str(),
                    segmentId, e.what());
                throw BackupOpenRejectedException(HERE);
            } catch (...) {
                segments.erase(MasterSegmentIdPair(masterId, segmentId));
                delete replica;
                throw;
            }
        }
    }

    // Perform write.
    if (!replica) {
        LOG(WARNING, "Tried write to a replica of segment <%s,%lu> but "
            "no such replica was open on this backup (server id %s)",
            masterId.toString().c_str(), segmentId,
            serverId.toString().c_str());
        throw BackupBadSegmentIdException(HERE);
    } else if (replica->isOpen()) {
        // Need to check all three conditions because overflow is possible
        // on the addition
        if (reqHdr.length > segmentSize ||
            reqHdr.offset > segmentSize ||
            reqHdr.length + reqHdr.offset > segmentSize)
            throw BackupSegmentOverflowException(HERE);

        {
            CycleCounter<RawMetric> __(&metrics->backup.writeCopyTicks);
            auto* footerEntry = reqHdr.footerIncluded ?
                                                &reqHdr.footerEntry : NULL;
            replica->write(rpc.requestPayload, sizeof(reqHdr),
                        reqHdr.length, reqHdr.offset,
                        footerEntry, reqHdr.atomic);
        }
        metrics->backup.writeCopyBytes += reqHdr.length;
        bytesWritten += reqHdr.length;
    } else {
        if (!(reqHdr.flags & WireFormat::BackupWrite::CLOSE)) {
            LOG(WARNING, "Tried write to a replica of segment <%s,%lu> but "
                "the replica was already closed on this backup (server id %s)",
                masterId.toString().c_str(), segmentId,
                serverId.toString().c_str());
            throw BackupBadSegmentIdException(HERE);
        }
        LOG(WARNING, "Closing segment write after close, may have been "
            "a redundant closing write, ignoring");
    }

    // Perform close, if any.
    if (reqHdr.flags & WireFormat::BackupWrite::CLOSE)
        replica->close();
}

/**
 * Runs garbage collection tasks.
 */
void
BackupService::gcMain()
try {
    gcTaskQueue.performTasksUntilHalt();
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

} // namespace RAMCloud
