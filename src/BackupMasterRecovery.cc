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

#include "BackupMasterRecovery.h"
#include "BackupService.h"
#include "Object.h"
#include "RecoverySegmentBuilder.h"
#include "ShortMacros.h"

namespace RAMCloud {

enum { DISABLE_BACKGROUND_BUILDING = false };

// -- BackupMasterRecovery --

/**
 * Create an instance to manage all aspects of a single master recovery.
 * Master recovery happens in a few phases on the backup; construction
 * doesn't start any of them. See start() for details on the first phase of
 * master recovery which is initiated by the coordinator.
 *
 * \param taskQueue
 *      Task queue which will provide the context to filter loaded replicas in
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
 */
BackupMasterRecovery::BackupMasterRecovery(TaskQueue& taskQueue,
                                           uint64_t recoveryId,
                                           ServerId crashedMasterId,
                                           uint32_t segmentSize)
    : Task(taskQueue)
    , recoveryId(recoveryId)
    , crashedMasterId(crashedMasterId)
    , partitions()
    , segmentSize(segmentSize)
    , numPartitions()
    , replicas()
    , nextToBuild()
    , firstSecondaryReplica()
    , segmentIdToReplica()
    , logDigest()
    , logDigestSegmentId(~0lu)
    , logDigestSegmentEpoch()
    , tableStatsDigest()
    , startCompleted()
    , freeQueued()
    , recoveryTicks()
    , readingDataTicks()
    , buildingStartTicks()
    , testingExtractDigest()
    , testingSkipBuild()
{
}

/**
 * Extract the details of all the replicas stored for the crashed master,
 * returning them for the coordinator to perform an inventory of the log,
 * and asynchronously start loading primary replicas in the background in
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

    LOG(NOTICE, "Backup preparing for recovery %lu of crashed server %s; "
               "loading replicas", recoveryId,
               ServerId(crashedMasterId).toString().c_str());

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

    // Shuffle the primary entries, this helps all recovery
    // masters to stay busy even if the log contains long sequences of
    // back-to-back replicas that only have objects for a particular
    // partition. Secondary order is irrelevant.
    std::random_shuffle(primaries.begin(),
                        primaries.end(),
                        randomNumberGenerator);

    // Build the deque and the mapping from segment ids to replicas.
    readingDataTicks.construct(&metrics->backup.readingDataTicks);
    foreach (auto& frame, primaries) {
        replicas.emplace_back(frame);
        frame->startLoading();
        auto& replica = replicas.back();
        segmentIdToReplica[replica.metadata->segmentId] = &replica;
    }
    firstSecondaryReplica = replicas.end();
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
    nextToBuild = replicas.begin();
    buildingStartTicks = Cycles::rdtsc();
    schedule();
}

/**
 * Append the specified recovery segment to \a buffer along with the
 * certificate needed verify its integrity and iterate it. Used to fulfill
 * GetRecoveryData rpcs from recovery masters. If the requested recovery
 * segment comes from a secondary replica then block while loading and
 * filtering the replica if it hasn't been done yet. If the requested recovery
 * segment comes from a primary replica then do not block; if the recovery
 * segment hasn't been constructed yet the return status indicates the recovery
 * master should try to collect other recovery segments and come back for this
 * one later.
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
                                         Segment::Certificate* certificate)
{
    if (this->recoveryId != recoveryId) {
        LOG(ERROR, "Requested recovery segment from recovery %lu, but current "
            "recovery for that master is %lu", recoveryId, this->recoveryId);
        throw BackupBadSegmentIdException(HERE);
    }
    auto replicaIt = segmentIdToReplica.find(segmentId);
    if (replicaIt == segmentIdToReplica.end()) {
        LOG(WARNING, "Asked for a recovery segments for segment <%s,%lu> "
            "which isn't part of this recovery",
           crashedMasterId.toString().c_str(), segmentId);
        throw BackupBadSegmentIdException(HERE);
    }
    Replica* replica = replicaIt->second;

    if (!replica->metadata->primary || DISABLE_BACKGROUND_BUILDING) {
        LOG(DEBUG, "Requested segment <%s,%lu> is secondary, "
            "starting build of recovery segments now",
            crashedMasterId.toString().c_str(), segmentId);
        replica->frame->load();
        buildRecoverySegments(*replica);
    }

    Fence::lfence();
    if (!replica->built) {
        LOG(DEBUG, "Deferring because <%s,%lu> not yet filtered",
            crashedMasterId.toString().c_str(), segmentId);
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
    freeQueued = true;
    schedule();
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
 * Check to see if a primary replica is finished loading from disk and, if so,
 * build the recovery segments. Invoked by a task queue in a separate thread
 * from the backup worker thread so building recovery segments for primary
 * replicas is done in the background. Works down #replicas in order starting
 * at the beginning (which #nextToBuild is initially set to in start()) until
 * the end of #replicas or a secondary replica is encountered.
 */
void
BackupMasterRecovery::performTask()
{
    if (freeQueued) {
        LOG(DEBUG, "State for recovery %lu for crashed master %s freed on "
            "backup", recoveryId, crashedMasterId.toString().c_str());
        // Destructor will take care of everything including dropping
        // references to the storage frames.
        delete this;
        return;
    }
    if (DISABLE_BACKGROUND_BUILDING)
        return;

    if (nextToBuild == firstSecondaryReplica) {
        readingDataTicks.destroy();
        uint64_t ns =
            Cycles::toNanoseconds(Cycles::rdtsc() - buildingStartTicks);
        LOG(NOTICE, "Took %lu ms to filter %lu segments",
            ns / 1000 / 1000, firstSecondaryReplica - replicas.begin());
        return;
    }

    schedule();

    if (!nextToBuild->frame->isLoaded()) {
        // Can't afford to log here at any level; generates tons of logging.
        return;
    }
    LOG(DEBUG, "Starting to build recovery segments for (<%s,%lu>)",
        crashedMasterId.toString().c_str(), nextToBuild->metadata->segmentId);
    buildRecoverySegments(*nextToBuild);
    LOG(DEBUG, "Done building recovery segments for (<%s,%lu>)",
        crashedMasterId.toString().c_str(), nextToBuild->metadata->segmentId);
    nextToBuild->frame->unload();
    ++nextToBuild;
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
    response->digestBytes = logDigest.getTotalLength();
    if (response->digestBytes > 0) {
        responseBuffer->appendCopy(logDigest.getRange(0, response->digestBytes),
               response->digestBytes);
        LOG(DEBUG, "Sent %u bytes of LogDigest to coordinator",
            response->digestBytes);
    }

    response->tableStatsBytes = tableStatsDigest.getTotalLength();
    if (response->tableStatsBytes > 0) {
        responseBuffer->appendCopy(tableStatsDigest.getRange(0,
                response->tableStatsBytes), response->tableStatsBytes);
    }
}

/**
 * Construct recovery segments for this replica data splitting data among
 * them according to #partitions. Idempotent; if replicas have already been
 * built the function returns immediately. After recovery segments are
 * constructed replica.built is set after an sfence. getRecoverySegment()
 * performs an lfence and checks replica.built before using the generated
 * recovery segments.
 *
 * After this method completes exactly one of replica.recoverySegments or
 * replica.recoveryException should be set. Notice: both of these are
 * std::unique_ptr so setting/testing them is not atomic. Use replica.built to
 * test to see if the construction of the recovery segments has completed
 * (along with a proper lfence first). This method doesn't throw exceptions;
 * any exceptions are boxed into replica.recoveryException so the exception
 * can be returned to recovery masters which request recovery segments from it
 * (which indicates to them that they need to find the recovery segment on
 * another backup).
 *
 * This method is NOT thread-safe for multiple simulatenous calls for the SAME
 * replica. Multiple invocations for different replicas is OK and expected.
 * BackupMasterRecovery serializes the processing of primary replicas via a
 * TaskQueue; primary replicas are ONLY processed by that task (performTask()).
 * Secondaries are ONLY processed by the backup worker thread. Since the worker
 * thread serializes all rpcs secondary processing is serialized. Since the two
 * sets are disjoint it all works out.
 *
 * If we move to multiple worker threads then replicas will need to be locked
 * for this filtering.
 *
 * \param replica
 *      Replica whose data should be walked and bucketed into recovery segments
 *      according to #partitions.
 */
void
BackupMasterRecovery::buildRecoverySegments(Replica& replica)
{
    if (replica.built) {
        LOG(NOTICE, "Recovery segments already built for <%s,%lu>",
            crashedMasterId.toString().c_str(), replica.metadata->segmentId);
        return;
    }

    replica.recoveryException.reset();
    replica.recoverySegments.reset();

    void* replicaData = replica.frame->load();
    CycleCounter<RawMetric> _(&metrics->backup.filterTicks);

    std::unique_ptr<Segment[]> recoverySegments(new Segment[numPartitions]);
    uint64_t start = Cycles::rdtsc();
    try {
        if (!testingSkipBuild) {
            assert(partitions);
            RecoverySegmentBuilder::build(replicaData, segmentSize,
                                          replica.metadata->certificate,
                                          numPartitions,
                                          *partitions,
                                          recoverySegments.get());
        }
    } catch (const Exception& e) {
        // Can throw SegmentIteratorException or SegmentRecoveryFailedException.
        // Exception is a little broad, but it catches them both; hopefully we
        // don't try to recover from anything else too serious.
        LOG(NOTICE, "Couldn't build recovery segments for <%s,%lu>: %s",
            crashedMasterId.toString().c_str(),
            replica.metadata->segmentId, e.what());
        replica.recoveryException.reset(
            new SegmentRecoveryFailedException(HERE));
        Fence::sfence();
        replica.built = true;
        return;
    }

    LOG(DEBUG, "<%s,%lu> recovery segments took %lu ms to construct, "
               "notifying other threads",
        crashedMasterId.toString().c_str(), replica.metadata->segmentId,
        Cycles::toNanoseconds(Cycles::rdtsc() - start) / 1000 / 1000);
    replica.recoverySegments = std::move(recoverySegments);
    Fence::sfence();
    replica.built = true;
}

// -- BackupMasterRecovery --

BackupMasterRecovery::Replica::Replica(const BackupStorage::FrameRef& frame)
    : frame(frame)
    , metadata(static_cast<const BackupReplicaMetadata*>(frame->getMetadata()))
    , recoverySegments()
    , recoveryException()
    , built()
{
}

} // namespace RAMCloud
