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

#include "BackupMasterRecovery.h"
#include "BackupService.h"
#include "Object.h"
#include "RecoverySegmentBuilder.h"
#include "ShortMacros.h"

namespace RAMCloud {

SpinLock BackupMasterRecovery::deletionMutex
    ("BackupMasterRecovery::deletionMutex");

// -- BackupMasterRecovery --

/**
 * Create an instance to manage all aspects of a single master recovery.
 * Master recovery happens in a few phases on the backup; construction
 * doesn't start any of them. See start() for details on the first phase of
 * master recovery which is initiated by the coordinator.
 *
 * \param taskQueue
 *      Task queue which will provide the context to load and filter replicas in
 *      the background. Not used until just before start() completes. This
 *      instance takes care of all the details of scheduling itself. The user
 *      needn't worry about calling schedule().
 * \param recoveryId
 *      Which master recovery this is for. The coordinator may schedule
 *      multiple recoveries for a single master (though, it only schedules one
 *      at a time). The backup only keeps one recovery around for a particular
 *      master at a time, calling free() any BackupMasterRecovery for before
 *      creating a new one. This id is how the backup distiguishes between
 *      retries for the start of a recovery from the start of a new recovery.
 *      See BackupService::startReadingData().
 * \param crashedMasterId
 *      Server id of the master which has crashed whose replicas should be
 *      processed and made available to recovery masters.
 * \param segmentSize
 *      Size of the replicas on storage. Needed for bounds-checking on the
 *      SegmentIterators which walk the stored replicas.
 * \param readSpeed
 *      The read speed of this recovery's backup disk, in MB/s.
 * \param maxReplicasInMemory
 *     The maximum number of replicas and frames to have in memory at any given
 *     point during recovery. This number will determine the size of this
 *     recovery's CyclicReplicaBuffer.
 */
BackupMasterRecovery::BackupMasterRecovery(TaskQueue& taskQueue,
                                           uint64_t recoveryId,
                                           ServerId crashedMasterId,
                                           uint32_t segmentSize,
                                           uint32_t readSpeed,
                                           uint32_t maxReplicasInMemory)
    : Task(taskQueue)
    , recoveryId(recoveryId)
    , crashedMasterId(crashedMasterId)
    , partitions()
    , segmentSize(segmentSize)
    , numPartitions()
    , replicas()
    , segmentIdToReplica()
    , replicaBuffer(maxReplicasInMemory, segmentSize, readSpeed, this)
    , logDigest()
    , logDigestSegmentId(~0lu)
    , logDigestSegmentEpoch()
    , tableStatsDigest()
    , startCompleted()
    , recoveryTicks()
    , readingDataTicks()
    , buildingStartTicks()
    , testingExtractDigest()
    , testingSkipBuild()
    , destroyer(taskQueue, this)
    , pendingDeletion(false)
{
}

/**
 * Perform logging of cleanup inside a destructor, since we are using a
 * distinct task to clean up the BackupMasterRecovery instance.
 */
BackupMasterRecovery::~BackupMasterRecovery() {
    LOG(NOTICE, "Freeing recovery state on backup for crashed master %s "
            "(recovery %lu), including %lu filtered replicas",
            crashedMasterId.toString().c_str(), recoveryId,
            replicaBuffer.size());
}

/**
 * Extract the details of all the replicas stored for the crashed master,
 * returning them for the coordinator to perform an inventory of the log,
 * and queue them to be asynchronously loaded by the CyclicReplicaBuffer in
 * preparation of replica data requests from recovery masters. Idempotent
 * but not thread-safe. It is expected that only a single BackupService
 * Worker thread calls this at a time.
 *
 * \param frames
 *      List of frames which likely contain a replica created by the crashed
 *      master. Provided by BackupService using its mapping from master/segment
 *      id pairs to frames. While this recovery won't load any replicas not
 *      listed in \a frames it may not load all replicas listed in \a frames.
 *      This can occur because of replica corruption or because the metadata
 *      stored in the replica is for the wrong master. In other words, this
 *      is seen as a hint and is not relied on for correctness; all information
 *      is taken directly from the replica and its metadata in frame.
 * \param[out] buffer
 *      Buffer to which the replica information and (possible) log digest are
 *      appended. See WireFormat::BackupStartReadingData::Response for details.
 * \param[out] response
 *      Metadata for the data appended to \a buffer. See
 *      WireFormat::BackupStartReadingData::Response for details.
 */
void
BackupMasterRecovery::start(const std::vector<BackupStorage::FrameRef>& frames,
                            Buffer* buffer,
                            StartResponse* response)
{
    if (startCompleted) {
        populateStartResponse(buffer, response);
        return;
    }

    recoveryTicks.construct(&metrics->backup.recoveryTicks);
    metrics->backup.recoveryCount++;

    vector<BackupStorage::FrameRef> primaries;
    vector<BackupStorage::FrameRef> secondaries;
    foreach (auto frame, frames) {
        const BackupReplicaMetadata* metadata =
           reinterpret_cast<const BackupReplicaMetadata*>(frame->getMetadata());
        if (!metadata->checkIntegrity()) {
            LOG(NOTICE, "Replica of <%s,%lu> metadata failed integrity check; "
                "will not be used for recovery (note segment id in this log "
                "message may be corrupted as a result)",
                crashedMasterId.toString().c_str(), metadata->segmentId);
            continue;
        }
        if (metadata->logId != crashedMasterId.getId())
            continue;
        (metadata->primary ? primaries : secondaries).push_back(frame);
    }

    LOG(NOTICE, "Backup preparing for recovery %lu of crashed server %s; "
                "loading %lu primary replicas", recoveryId,
                ServerId(crashedMasterId).toString().c_str(),
                primaries.size());

    // Build the deque and the mapping from segment ids to replicas.
    readingDataTicks.construct(&metrics->backup.readingDataTicks);

    // Arrange for replicas to be processed in reverse chronological order (most
    // recent replicas first) by enqueuing them in #replicaBuffer in reverse
    // order. This improves recovery performance by ensuring that the buffer
    // loads replicas in the same order they will be requested by the recovery
    // masters.
    vector<BackupStorage::FrameRef>::reverse_iterator rit;
    for (rit = primaries.rbegin(); rit != primaries.rend(); ++rit) {
        replicas.emplace_back(*rit);
        auto& replica = replicas.back();
        replicaBuffer.enqueue(&replica, CyclicReplicaBuffer::NORMAL);
        segmentIdToReplica[replica.metadata->segmentId] = &replica;
    }
    foreach (auto& frame, secondaries) {
        replicas.emplace_back(frame);
        auto& replica = replicas.back();
        segmentIdToReplica[replica.metadata->segmentId] = &replica;
    }

    // Obtain the LogDigest from the lowest segment id of any open replica
    // that has the highest epoch number. The epoch part shouldn't matter
    // since backups don't accept multiple replicas for the same segment, but
    // better to put this in in case things change in the future.
    foreach (auto& replica, replicas) {
        if (replica.metadata->closed)
            continue;
        if (logDigestSegmentId < replica.metadata->segmentId)
            continue;
        if (logDigestSegmentId == replica.metadata->segmentId &&
            logDigestSegmentEpoch > replica.metadata->segmentEpoch)
        {
            continue;
        }
        // This shouldn't block since the backup keeps all open segments
        // in memory (it even reloads them into memory upon restarts).
        void* replicaData = replica.frame->load();
        bool foundDigest = false;
        if (testingExtractDigest) {
            foundDigest =
                (*testingExtractDigest)(replica.metadata->segmentId,
                                        &logDigest, &tableStatsDigest);
        } else {
            foundDigest =
                RecoverySegmentBuilder::extractDigest(replicaData, segmentSize,
                    replica.metadata->certificate, &logDigest,
                    &tableStatsDigest);
        }
        if (foundDigest) {
            logDigestSegmentId = replica.metadata->segmentId;
            logDigestSegmentEpoch = replica.metadata->segmentEpoch;
        }
    }
    if (logDigestSegmentId != ~0lu) {
        LOG(NOTICE, "Found log digest in replica for segment %lu",
            logDigestSegmentId);
    }

    startCompleted = true;
    populateStartResponse(buffer, response);
}

/**
 * To be invoked after start() via a startPartitioningReplicasRpc. This
 * starts the partitioning of the replica segments read during start.
 * This call is idempotent.
 *
 * \param partitions
 *       The partitioning scheme by which the replicas should be split
 */
void
BackupMasterRecovery::setPartitionsAndSchedule(
                          ProtoBuf::RecoveryPartition partitions)
{
    assert(startCompleted);

    // idempotency
    if (this->partitions) {
        schedule();
        return;
    }

    this->partitions.construct(partitions);

    for (int i = 0; i < partitions.tablet_size(); ++i) {
        numPartitions = std::max(numPartitions,
                                 downCast<int>(
                                 partitions.tablet(i).user_data() + 1));
    }

    LOG(NOTICE, "Recovery %lu building %d recovery segments for each "
            "replica for crashed master %s and filtering them according to "
            "the following " "partitions:\n%s",
            recoveryId, numPartitions, crashedMasterId.toString().c_str(),
            partitions.DebugString().c_str());

    LOG(DEBUG, "Kicked off building recovery segments");
    buildingStartTicks = Cycles::rdtsc();
    schedule();
}

/**
 * Append the specified recovery segment to \a buffer along with the
 * certificate needed verify its integrity and iterate it. Used to fulfill
 * GetRecoveryData rpcs from recovery masters. If the recovery segment hasn't
 * been constructed yet the return status indicates the recovery master should
 * try to collect other recovery segments and come back for this one later. This
 * occurs regardless of whether the requested replica is a primary or secondary
 * replica.
 *
 * \param recoveryId
 *      Which master recovery this is for. The coordinator may schedule
 *      multiple recoveries for a single master (though, it only schedules one
 *      at a time). Used to prevent a recovery master for one recovery from
 *      accidentally using recovery segments for another recovery.
 * \param segmentId
 *      Id of the segment which the recovery master is seeking a recovery
 *      segment for.
 * \param partitionId
 *      The partition id corresponding to the tablet ranges of the recovery
 *      segment to append to \a buffer.
 * \param[out] buffer
 *      A buffer which onto which the requested recovery segment will be
 *      appended. May be null for testing.
 * \param[out] certificate
 *      Certificate for the recovery segment returned in \a buffer. Used by
 *      recovery masters to check the integrity of the metadata of the
 *      returned recovery segment and to iterate over it. May be null for
 *      testing.
 * \return
 *      Status code: STATUS_OK if the recovery segment was appended,
 *      STATUS_RETRY if the caller should try again later.
 * \throw BadSegmentIdException
 *      If the segment to which this recovery segment belongs is not yet
 *      recovered, there is no such recovery segment for that
 *      #partitionId, or a different recovery id was requested.
 * \throw SegmentRecoveryFailedException
 *      If the segment to which this recovery segment belongs failed to
 *      recover.
 */
Status
BackupMasterRecovery::getRecoverySegment(uint64_t recoveryId,
                                         uint64_t segmentId,
                                         int partitionId,
                                         Buffer* buffer,
                                         SegmentCertificate* certificate)
{
    if (this->recoveryId != recoveryId) {
        LOG(ERROR, "Requested recovery segment from recovery %lu, but current "
            "recovery for that master is %lu", recoveryId, this->recoveryId);
        throw BackupBadSegmentIdException(HERE);
    }
    auto replicaIt = segmentIdToReplica.find(segmentId);
    if (replicaIt == segmentIdToReplica.end()) {
        LOG(NOTICE, "Asked for a recovery segment for segment <%s,%lu> "
            "which isn't part of this recovery",
           crashedMasterId.toString().c_str(), segmentId);
        throw BackupBadSegmentIdException(HERE);
    }
    Replica* replica = replicaIt->second;
    CyclicReplicaBuffer::ActiveReplica activeRecord(replica, &replicaBuffer);

    if (!replicaBuffer.contains(replica)) {
        // This replica isn't loaded into memory, so try to schedule it to be
        // added to the buffer.
        replicaBuffer.enqueue(replica, CyclicReplicaBuffer::HIGH);
        throw RetryException(HERE, 5000, 10000,
            "desired segment has not yet been loaded into buffer");
    }

    Fence::lfence();
    if (!replica->built) {
        if (replica->frame->isLoaded()) {
            LOG(DEBUG, "Deferring because <%s,%lu> not yet filtered: %p",
                crashedMasterId.toString().c_str(), segmentId, replica);
        } else {
            LOG(DEBUG, "Deferring because <%s,%lu> not yet loaded: %p",
                crashedMasterId.toString().c_str(), segmentId, replica);
        }

        throw RetryException(HERE, 5000, 10000,
                "desired segment not yet filtered");
    }

    if (replica->metadata->primary)
        ++metrics->backup.primaryLoadCount;
    else
        ++metrics->backup.secondaryLoadCount;

    if (replica->recoveryException) {
        auto e = SegmentRecoveryFailedException(*replica->recoveryException);
        replica->recoveryException.reset();
        throw e;
    }

    if (partitionId >= numPartitions) {
        LOG(WARNING, "Asked for partition %d from segment <%s,%lu> "
                     "but there are only %d partitions",
            partitionId, crashedMasterId.toString().c_str(), segmentId,
            numPartitions);
        throw BackupBadSegmentIdException(HERE);
    }

    if (buffer)
        replica->recoverySegments[partitionId].appendToBuffer(*buffer);
    if (certificate)
        replica->recoverySegments[partitionId].getAppendedLength(certificate);

    replica->fetchCount++;
    return STATUS_OK;
}

/**
 * Inform this recovery that it should cleanup and release all resources as
 * soon as possible (including any references to frames, which may allow them
 * to be freed/reused). Destruction is non-trivial because the instance may
 * become redundant while it is filtering a replica.
 */
void
BackupMasterRecovery::free()
{
    LOG(DEBUG, "Recovery %lu for crashed master %s is no longer needed; will "
        "clean up as next possible chance.",
        recoveryId, crashedMasterId.toString().c_str());
    SpinLock::Guard lock(deletionMutex);
    pendingDeletion = true;
    destroyer.schedule();
}

/**
 * Returns which master recovery this is for. The coordinator may schedule
 * multiple recoveries for a single master (though, it only schedules one
 * at a time). The backup only keeps one recovery around for a particular
 * master at a time, calling free() any BackupMasterRecovery for before
 * creating a new one. This id is how the backup distiguishes between
 * retries for the start of a recovery from the start of a new recovery.
 * See BackupService::startReadingData().
 */
uint64_t
BackupMasterRecovery::getRecoveryId()
{
    return recoveryId;
}

/**
 * Alternates between attempting to add the next replica to the buffer (see
 * bufferNext()) and building the next recovery segment from a previously loaded
 * replica (see buildNext()). Invoked by a task queue in a separate thread from
 * the backup worker thread so building recovery segments is done in the
 * background.
 */
void
BackupMasterRecovery::performTask()
{
    {
        SpinLock::Guard lock(deletionMutex);
        if (!pendingDeletion)
            schedule();
    }

    replicaBuffer.bufferNext();
    replicaBuffer.buildNext();
}

// - private -

/**
 * Append replica information and the log digest (if any) to \a responseBuffer
 * and populate \a response with the corresponding details about the
 * data placed in the buffer. Used only by start() to send information about
 * replicas back to the coordinator as a helper to provide idempotence across
 * StartReadingData rpcs. If \a responseBuffer is NULL (for testing) this
 * function has no effect.
 */
void
BackupMasterRecovery::populateStartResponse(Buffer* responseBuffer,
                                            StartResponse* response)
{
    if (responseBuffer == NULL)
        return;

    response->replicaCount = 0;
    response->primaryReplicaCount = 0;
    foreach (const auto& replica, replicas) {
        responseBuffer->emplaceAppend<
                WireFormat::BackupStartReadingData::Replica>(
                replica.metadata->segmentId, replica.metadata->segmentEpoch,
                replica.metadata->closed);
        ++response->replicaCount;
        if (replica.metadata->primary)
            ++response->primaryReplicaCount;
        LOG(DEBUG, "Crashed master %s had %s %s replica for segment %lu",
            crashedMasterId.toString().c_str(),
            replica.metadata->closed ? "closed" : "open",
            replica.metadata->primary ? "primary" : "secondary",
            replica.metadata->segmentId);
    }

    LOG(DEBUG, "Sending %u segment ids for this master (%u primary)",
        response->replicaCount, response->primaryReplicaCount);

    response->digestSegmentId = logDigestSegmentId;
    response->digestSegmentEpoch = logDigestSegmentEpoch;
    response->digestBytes = logDigest.size();
    if (response->digestBytes > 0) {
        responseBuffer->appendCopy(logDigest.getRange(0, response->digestBytes),
               response->digestBytes);
        LOG(DEBUG, "Sent %u bytes of LogDigest to coordinator",
            response->digestBytes);
    }

    response->tableStatsBytes = tableStatsDigest.size();
    if (response->tableStatsBytes > 0) {
        responseBuffer->appendCopy(tableStatsDigest.getRange(0,
                response->tableStatsBytes), response->tableStatsBytes);
        LOG(DEBUG, "Sent %u bytes of table statistics to coordinator",
            response->tableStatsBytes);
    }
}

// -- BackupMasterRecovery --

BackupMasterRecovery::Replica::Replica(const BackupStorage::FrameRef& frame)
    : frame(frame)
    , metadata(static_cast<const BackupReplicaMetadata*>(frame->getMetadata()))
    , recoverySegments()
    , recoveryException()
    , built()
    , lastAccessTime(0)
    , refCount(0)
    , fetchCount(0)
{
}

// --- TaskKiller ---

TaskKiller::TaskKiller(TaskQueue& taskQueue, Task* taskToKill)
    : Task(taskQueue)
    , taskToKill(taskToKill)
{
}

void
TaskKiller::performTask()
{
    delete taskToKill;
}

// -- CyclicReplicaBuffer --

/**
 * Constructs a cyclic buffer used to manage memory during recovery.
 *
 * \param maxReplicasInMemory
 *      The maximum number of replicas to be kept in memory by this buffer.
 * \param segmentSize
 *      Size of the replicas on storage (in MB). Needed for eviction
 *      calculations.
 * \param readSpeed
 *      The read speed of this recovery's backup disk, in MB/s. This is used
 *      to calculate bufferReadTime, which determines the eviction timeout
 *      (see bufferNext()).
 * \param recovery
 *     A reference to the BackupMasterRecovery that owns this buffer.
 */
BackupMasterRecovery::CyclicReplicaBuffer::CyclicReplicaBuffer(
        uint32_t maxReplicasInMemory, uint32_t segmentSize, uint32_t readSpeed,
        BackupMasterRecovery* recovery)
    : mutex("cyclicReplicaBuffeMutex")
    , maxReplicasInMemory(maxReplicasInMemory)
    , inMemoryReplicas()
    , oldestReplicaIdx(0)
    , normalPriorityQueuedReplicas()
    , highPriorityQueuedReplicas()
    , bufferReadTime(static_cast<double>(segmentSize)
                     * maxReplicasInMemory / readSpeed
                     / (1 << 20)) // MB to bytes
    , recovery(recovery)
{
    inMemoryReplicas.reserve(maxReplicasInMemory);
    LOG(NOTICE, "Constructed cyclic recovery buffer with %u replicas and a "
                "read time of %f s", maxReplicasInMemory, bufferReadTime);
}

BackupMasterRecovery::CyclicReplicaBuffer::~CyclicReplicaBuffer()
{
}

/**
 * Returns the number of replicas currently in memory in the buffer.
 */
size_t
BackupMasterRecovery::CyclicReplicaBuffer::size() {
    SpinLock::Guard lock(mutex);
    return inMemoryReplicas.size();
}

/**
 * Returns true if the given replica is currently in the buffer and false
 * otherwise. A replica is in the buffer when it is either in memory or has
 * a pending disk read.
 */
bool
BackupMasterRecovery::CyclicReplicaBuffer::contains(Replica* replica)
{
    SpinLock::Guard lock(mutex);
    return std::find(inMemoryReplicas.begin(), inMemoryReplicas.end(), replica)
        != inMemoryReplicas.end();
}

/**
 * Schedules the given replica to be added to the buffer at a later point by
 * bufferNext(). This method should be called in the order in which replicas
 * will be requested by recovery masters. See bufferNext() for details on how
 * priorities impact ordering. If a replica is already enqueued but not yet in
 * the buffer, it will not be enqueued again, regardless of the priority given
 * (this is to ensure that we stick to the pre-defined recovery order).
 * Similarly, a replica curretly in the buffer cannot be enqueued again until it
 * has been evicted.
 *
 * \param replica
 *      The replica to enqueue.
 * \param priority
 *      The priority to give to this replica. HIGH priority should only be given
 *      to replicas that need to be buffered outside of the standard ordering
 *      (when a replica was previously evicted by bufferNext() or when it is
 *      a secondary replica).
 */
void
BackupMasterRecovery::CyclicReplicaBuffer::enqueue(Replica* replica,
                                                   Priority priority) {
    SpinLock::Guard lock(mutex);

    // Don't enqueue a replica already in the buffer
    if (std::find(inMemoryReplicas.begin(), inMemoryReplicas.end(), replica)
            != inMemoryReplicas.end()) {
        return;
    }

    if (std::find(normalPriorityQueuedReplicas.begin(),
                  normalPriorityQueuedReplicas.end(), replica)
            != normalPriorityQueuedReplicas.end()) {
        // This replica was scheduled normally and we just haven't gotten to
        // it yet.
        LOG(DEBUG, "A master is ahead, requesting segment %lu",
            replica->metadata->segmentId);
        return;
    } else if (priority == NORMAL) {
        LOG(DEBUG, "Adding replica for segment %lu to normal priority "
            "recovery queue", replica->metadata->segmentId);
        normalPriorityQueuedReplicas.push_back(replica);
    } else {
        // Don't schedule a replica as high priority more than once
        if (std::find(highPriorityQueuedReplicas.begin(),
                      highPriorityQueuedReplicas.end(), replica)
                == highPriorityQueuedReplicas.end()) {
            LOG(WARNING, "Adding replica for segment %lu to high priority "
                "recovery queue", replica->metadata->segmentId);
            highPriorityQueuedReplicas.push_back(replica);
        }
    }
}

/**
 * This method attempts to read the next queued replica into the buffer. It
 * fails if the buffer is full and there are no replicas eligible for eviction
 * (see eviction criteria below). This method should be called periodically
 * during recovery to ensure that the buffer remains up-to-date with requests
 * from masters.
 *
 * \return
 *     True if a new replica was added to the buffer and false otherwise.
 */
bool
BackupMasterRecovery::CyclicReplicaBuffer::bufferNext()
{
    SpinLock::Guard lock(mutex);

    // Get the next replica to read into memory from the high priority list. If
    // there are no high priority replicas queued, then get one from the normal
    // priority list.
    Replica* nextReplica;
    std::deque<Replica*>* replicaDeque;
    if (!highPriorityQueuedReplicas.empty()) {
        nextReplica = highPriorityQueuedReplicas.front();
        replicaDeque = &highPriorityQueuedReplicas;
    } else if (!normalPriorityQueuedReplicas.empty()) {
        nextReplica = normalPriorityQueuedReplicas.front();
        replicaDeque = &normalPriorityQueuedReplicas;
    } else {
        return false;
    }

    if (inMemoryReplicas.size() < maxReplicasInMemory) {
        // We haven't filled up the buffer yet.
        inMemoryReplicas.push_back(nextReplica);
    } else {
        /**
         * The buffer is full, so try to evict something.
         *
         * Eviction happens in a cyclic fashion starting with the oldest replica
         * in the buffer. A replica is eligible for eviction only if all of the
         * following are true:
         *     1. It is the oldest replica in the buffer.
         *     2. It has been partitioned into recovery segments.
         *     3. It has been fetched by at least one recovery master.
         *     4. It is not currently being fetched by a recovery master.
         *     5. It has not been requested for half the time it takes to read
         *        the whole buffer from disk into memory.
         *
         * The final eviction criteria handles cases where some recovery masters
         * are slower than others. We'd rather not evict a replica until it has
         * been read by all of the recovery masters, but it's also possible that
         * a recovery master has crashed, or that no master has been assigned
         * for a particular partition. Thus, eventually, we must evict a replica
         * even if it hasn't been read by every master, so the recovery doesn't
         * stall. If the master is still alive and eventually asks for the
         * replica, we will have to reread it. The time constant balances the
         * desire to keep replicas around for slow masters against the desire to
         * free space for the next replicas that will be needed.
         */
        Replica* replicaToRemove = inMemoryReplicas[oldestReplicaIdx];

        if (!replicaToRemove->built || replicaToRemove->fetchCount == 0 ||
            replicaToRemove->refCount > 0) {
            // This replica isn't eligible for eviction yet.
            return false;
        }

        // Only evict a replica if it's been inactive for half the time it takes
        // to read the entire bufer from disk.
        double entryInactiveTime = Cycles::toSeconds(
            Cycles::rdtsc() - replicaToRemove->lastAccessTime);
        if (entryInactiveTime <= bufferReadTime / 2) {
            return false;
        }


        // Evict
        replicaToRemove->recoverySegments.release();
        replicaToRemove->built = false;
        inMemoryReplicas[oldestReplicaIdx] = nextReplica;
        LOG(DEBUG, "Evicted replica <%s,%lu> from the recovery buffer",
            recovery->crashedMasterId.toString().c_str(),
            replicaToRemove->metadata->segmentId);

        oldestReplicaIdx = (oldestReplicaIdx + 1) % maxReplicasInMemory;
    }

    // Read the next replica from disk.
    nextReplica->frame->startLoading();
    replicaDeque->pop_front();

    LOG(DEBUG, "Added replica <%s,%lu> to the recovery buffer",
        recovery->crashedMasterId.toString().c_str(),
        nextReplica->metadata->segmentId);
    return true;
}

/**
 * This method constructs recovery segments for the oldest replica in the buffer
 * that has been read from disk but not already partitioned. It should be called
 * periodically during recovery to partition the replicas being read into the
 * buffer by bufferNext().
 *
 * After this method completes exactly one of replica.recoverySegments or
 * replica.recoveryException should be set. Notice: both of these are
 * std::unique_ptr so setting/testing them is not atomic. Use replica.built to
 * test to see if the construction of the recovery segments has completed (along
 * with a proper lfence first). This method doesn't throw exceptions; any
 * exceptions are boxed into replica.recoveryException so the exception can be
 * returned to recovery masters which request recovery segments from it (which
 * indicates to them that they need to find the recovery segment on another
 * backup).
 *
 * \return
 *     True if a replica was partitioned (or an exception occurred while trying
 *     to partition a replica), and false if there were no replicas eligible to
 *     partition.
 */
bool
BackupMasterRecovery::CyclicReplicaBuffer::buildNext()
{
    Replica* replicaToBuild = NULL;
    {
        // Find the next loaded, unbuilt replica.
        SpinLock::Guard lock(mutex);
        for (size_t i = 0; i < inMemoryReplicas.size(); i++) {
            size_t idx = (i + oldestReplicaIdx) % inMemoryReplicas.size();
            Replica* candidate = inMemoryReplicas[idx];
            if (candidate->frame->isLoaded() && !candidate->built) {
                replicaToBuild = candidate;
                break;
            }
        }
    }

    if (!replicaToBuild) {
        return false;
    }

    replicaToBuild->recoveryException.reset();
    replicaToBuild->recoverySegments.reset();

    void* replicaData = replicaToBuild->frame->load();
    CycleCounter<RawMetric> _(&metrics->backup.filterTicks);

    // Recovery segments for this replica data are constructed by splitting data
    // among them according to #partitions. If we move to multiple worker
    // threads then replicas will need to be locked for this filtering.
    std::unique_ptr<Segment[]> recoverySegments(
        new Segment[recovery->numPartitions]);
    uint64_t start = Cycles::rdtsc();
    try {
        if (!recovery->testingSkipBuild) {
            assert(recovery->partitions);
            RecoverySegmentBuilder::build(replicaData, recovery->segmentSize,
                                          replicaToBuild->metadata->certificate,
                                          recovery->numPartitions,
                                          *(recovery->partitions),
                                          recoverySegments.get());
        }
    } catch (const Exception& e) {
        // Can throw SegmentIteratorException or SegmentRecoveryFailedException.
        // Exception is a little broad, but it catches them both; hopefully we
        // don't try to recover from anything else too serious.
        LOG(NOTICE, "Couldn't build recovery segments for <%s,%lu>: %s",
            recovery->crashedMasterId.toString().c_str(),
            replicaToBuild->metadata->segmentId, e.what());
        replicaToBuild->recoveryException.reset(
            new SegmentRecoveryFailedException(HERE));
        Fence::sfence();
        replicaToBuild->built = true;
        return true;
    }

    LOG(DEBUG, "<%s,%lu> recovery segments took %lu ms to construct, "
               "notifying other threads",
        recovery->crashedMasterId.toString().c_str(),
        replicaToBuild->metadata->segmentId,
        Cycles::toNanoseconds(Cycles::rdtsc() - start) / 1000 / 1000);
    replicaToBuild->recoverySegments = std::move(recoverySegments);
    Fence::sfence();
    replicaToBuild->built = true;
    replicaToBuild->lastAccessTime = Cycles::rdtsc();
    replicaToBuild->frame->unload();
    return true;
}

/**
 * Useful for debugging. Prints out the replicas currently in the buffer along
 * with information about whether it is loaded, built, and/or fetched. An arrow
 * is printed next to the next replica to be evicted.
 */
void
BackupMasterRecovery::CyclicReplicaBuffer::logState() {
    SpinLock::Guard lock(mutex);
    for (size_t i = 0; i < inMemoryReplicas.size(); i++) {
        Replica* replica = inMemoryReplicas[i];
        std::string state;
        if (replica->built) {
            state = "is built";
        } else if (replica->frame->isLoaded()) {
            state = "is loaded but not built";
        } else {
            state = "is not loaded";
        }

        LOG(NOTICE, "%sbuffer entry %lu: segment %lu, %s, fetched %d times",
            oldestReplicaIdx == i ? "-->" : "",
            i,
            replica->metadata->segmentId,
            state.c_str(),
            replica->fetchCount.load());
    }
}


} // namespace RAMCloud
