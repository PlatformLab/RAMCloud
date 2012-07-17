/* Copyright (c) 2010-2012 Stanford University
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

#include <unordered_set>

#include "Recovery.h"
#include "BackupClient.h"
#include "Buffer.h"
#include "MasterClient.h"
#include "ShortMacros.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * Create a Recovery to manage the recovery of a crashed master.
 * No recovery operations are performed until performTask() is called
 * (presumably by the MasterRecoveryManager's TaskQueue).
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param taskQueue
 *      MasterRecoveryManager TaskQueue which drives this recovery
 *      (by calling performTask() whenever this recovery is scheduled).
 * \param tabletMap
 *      Coordinator's authoritative information about tablets and their
 *      mapping to servers. Used during to find out which tablets need
 *      to be recovered for the crashed master.
 * \param tracker
 *      The MasterRecoveryManager's tracker which maintains a list of all
 *      servers in RAMCloud along with a pointer to any Recovery the server is
 *      particpating in (as a recovery master). This is used to select recovery
 *      masters and to find all backup data for the crashed master.
 * \param owner
 *      The owner is called back when recovery finishes and when it is ready
 *      to be deallocated. This is usually MasterRecoveryManager, but may be
 *      NULL for testing if the test takes care of deallocation of the Recovery.
 * \param crashedServerId
 *      The crashed master this Recovery will rebuild.
 * \param minOpenSegmentId
 *      Used to filter out replicas of segments which may have become
 *      inconsistent. A replica with a segment id less than this is
 *      not eligible to be used for recovery (both for log digest and
 *      object data purposes).
 */
Recovery::Recovery(Context& context,
                   TaskQueue& taskQueue,
                   TabletMap* tabletMap,
                   RecoveryTracker* tracker,
                   Owner* owner,
                   ServerId crashedServerId,
                   uint64_t minOpenSegmentId)
    : Task(taskQueue)
    , context(context)
    , crashedServerId(crashedServerId)
    , minOpenSegmentId(minOpenSegmentId)
    , tabletsToRecover()
    , tabletMap(tabletMap)
    , tracker(tracker)
    , owner(owner)
    , recoveryId(generateRandom())
    , status(START_RECOVERY_ON_BACKUPS)
    , recoveryTicks()
    , replicaMap()
    , numPartitions()
    , successfulRecoveryMasters()
    , unsuccessfulRecoveryMasters()
    , testingBackupStartTaskSendCallback()
    , testingMasterStartTaskSendCallback()
    , testingBackupEndTaskSendCallback()
    , testingFailRecoveryMasters()
{
}

Recovery::~Recovery()
{
}

/**
 * Finds out which tablets belonged to the crashed master and parititions
 * them into groups. Each of the groups is recovered later, one to each
 * recovery master. The result is left in #tabletsToRecover.
 *
 * Right now this just naively puts each tablet from the crashed master
 * in its own group (and thus on its own recovery master). At some
 * point we'll need smart logic to group tablets based on their
 * expected recovery time.
 */
void
Recovery::partitionTablets()
{
    auto tablets = tabletMap->setStatusForServer(crashedServerId,
                                                 Tablet::RECOVERING);
    // Figure out the number of partitions to be recovered and bucket tablets
    // together for recovery on a single recovery master. Right now the
    // bucketing is each tablet from the crashed master is recovered on a
    // single recovery master.
    foreach (auto& tablet, tablets) {
        ProtoBuf::Tablets::Tablet& entry = *tabletsToRecover.add_tablet();
        tablet.serialize(entry);
        entry.set_user_data(numPartitions++);
    }
}

/**
 * Perform or schedule (without blocking (much)) whatever work is needed in
 * order to recover the crashed master. Called by MasterRecoveryManager
 * in response to schedule() requests by this recovery.
 * Notice, not all recovery related work is done via this method; some
 * work is done in response to external calls (for example,
 * recoveryMasterFinished()).
 */
void
Recovery::performTask()
{
    metrics->coordinator.recoveryCount++;
    switch (status) {
    case START_RECOVERY_ON_BACKUPS:
        partitionTablets();
        startBackups();
        break;
    case START_RECOVERY_MASTERS:
        startRecoveryMasters();
        break;
    case WAIT_FOR_RECOVERY_MASTERS:
        // Calls to recoveryMasterFinished drive
        // recovery from WAIT_FOR_RECOVERY_MASTERS to
        // BROADCAST_RECOVERY_COMPLETE.
        assert(false);
        break;
    case BROADCAST_RECOVERY_COMPLETE:
// Waiting to broadcast end-of-recovery until after the driving
// tabletsRecovered RPC completes makes sense, but it breaks recovery
// metrics since they use this broadcast as a signal to stop their recovery
// timers resulting in many divide by zeroes (since the client app sees
// the tablets as being up it gathers metrics before the backups are
// informed of the end of recovery).
// An idea for a solution could be to have the backups stop their timers
// when they receive the get metrics request.
#define BCAST_INLINE 0
#if !BCAST_INLINE
        broadcastRecoveryComplete();
#endif
        status = DONE;
        if (owner)
            owner->destroyAndFreeRecovery(this);
        break;
    case DONE:
    default:
        assert(false);
    }
}

/**
 * Returns true if all partitions of the will were recovered successfully.
 * False if some recovery master failed to recover its partition or if
 * recovery never got off the ground for some reason (for example, a
 * complete log could not be found among available backups).
 */
bool
Recovery::wasCompletelySuccessful() const
{
    return status > WAIT_FOR_RECOVERY_MASTERS &&
           unsuccessfulRecoveryMasters == 0;
}

/**
 * Returns a unique identifier associated with this recovery.
 * Used to reassociate recovery related rpcs from recovery masters to the
 * recovery that they are part of.
 */
uint64_t
Recovery::getRecoveryId() const
{
    return recoveryId;
}

// - private -

namespace RecoveryInternal {
BackupStartTask::BackupStartTask(
            Recovery* recovery,
            ServerId backupId,
            ServerId crashedMasterId,
            const ProtoBuf::Tablets& partitions,
            uint64_t minOpenSegmentId)
    : backupId(backupId)
    , result()
    , recovery(recovery)
    , crashedMasterId(crashedMasterId)
    , partitions(partitions)
    , minOpenSegmentId(minOpenSegmentId)
    , rpc()
    , done()
    , testingCallback(recovery ?
                      recovery->testingBackupStartTaskSendCallback :
                      NULL)
{
}

/// Asynchronously send the startReadingData RPC to #backupHost.
void
BackupStartTask::send()
{
    LOG(DEBUG, "Starting startReadingData on backup %lu", backupId.getId());
    if (!testingCallback) {
        try {
            rpc.construct(recovery->context, backupId, crashedMasterId,
                          partitions);
            return;
        } catch (const TransportException& e) {
            LOG(WARNING, "Couldn't contact backup %lu to start recovery: %s",
                backupId.getId(), e.what());
        } catch (const ClientException& e) {
            LOG(WARNING, "Couldn't contact backup %lu to start recovery: %s",
                backupId.getId(), e.what());
        }
        done = true;
    } else {
        testingCallback->backupStartTaskSend(result);
    }
}

/**
 * Removes replicas and log digests from results that may be
 * inconsistent with the most recent state of the log being recovered.
 *
 * This involves checking "open" replicas and log digests against the
 * recorded minOpenSegmentId for the master being recovered and discarding
 * any entries or digests that have come from replicas for segments with an
 * id less than it.
 */
void
BackupStartTask::filterOutInvalidReplicas()
{
    // Remove any replicas from the results that are invalid because they
    // were found open and their segmentId is less than the minOpenSegmentId
    // for this master.
    enum { BYTES_WRITTEN_CLOSED = ~(0u) };
    vector<pair<uint64_t, uint32_t>> newIdAndLength;
    newIdAndLength.reserve(result.segmentIdAndLength.size());
    uint32_t newPrimarySegmentCount = 0;
    for (size_t i = 0; i < result.segmentIdAndLength.size(); ++i) {
        auto& idAndLength = result.segmentIdAndLength[i];
        if (idAndLength.second != BYTES_WRITTEN_CLOSED &&
            idAndLength.first < minOpenSegmentId) {
            LOG(DEBUG, "Removing replica for segmentId %lu from replica list "
                "for backup %lu because it was open and had an id less than "
                "the minOpenSegmentId (%lu) for the recovering master",
               idAndLength.first, backupId.getId(),
               minOpenSegmentId);
            continue;
        }
        if (i < result.primarySegmentCount)
            ++newPrimarySegmentCount;
        newIdAndLength.emplace_back(idAndLength);
    }
    std::swap(result.segmentIdAndLength, newIdAndLength);
    std::swap(result.primarySegmentCount, newPrimarySegmentCount);
    // We cannot use a log digest if it comes from a segment with a segmentId
    // less than minOpenSegmentId (the fact that minOpenSegmentId is higher
    // than this replicas segmentId demonstrates that a more recent log digest
    // was durably written to a later segment).
    if (result.logDigestSegmentId < minOpenSegmentId) {
        LOG(DEBUG, "Backup %lu returned a log digest for segmentId %lu but "
            "minOpenSegmentId for this master is %lu so discarding it",
            backupId.getId(), result.logDigestSegmentId, minOpenSegmentId);
        result.logDigestBytes = 0;
        result.logDigestBuffer.reset();
        result.logDigestSegmentId = -1;
        result.logDigestSegmentLen = -1;
    }
}

void
BackupStartTask::wait()
{
    try {
        if (!testingCallback)
            result = rpc->wait();
    } catch (const ServerDoesntExistException& e) {
        // Ryan, please add appropriate code here.
    } catch (const TransportException& e) {
        LOG(WARNING, "Couldn't contact %lu, failure was: %s",
            backupId.getId(), e.str().c_str());
        // Leave empty result as if the backup has no replicas.
    } catch (const ClientException& e) {
        LOG(WARNING, "startReadingData failed on %lu, failure was: %s",
            backupId.getId(), e.str().c_str());
        // Leave empty result as if the backup has no replicas.
    }
    rpc.destroy();

    filterOutInvalidReplicas();

    done = true;
    LOG(DEBUG, "Backup %lu has %lu segment replicas",
        backupId.getId(), result.segmentIdAndLength.size());
}

/**
 * Given lists of replicas provided by backups determine whether all
 * the segments in a log digest are claimed to be available on at
 * least one backup.
 *
 * \param tasks
 *      Already run tasks holding the results of startReadingData calls
 *      to all of the available backups.
 * \param taskCount
 *      Number of elements in #tasks.
 * \param digest
 *      Log digest which lists all the segments which must be replayed
 *      for a recovery of the associated crashed server's log to be
 *      successful and complete.
 *
 * \return
 *      True if at least one replica is available on some backup for
 *      every segment mentioned in the log digest.
 */
bool
verifyLogComplete(Tub<BackupStartTask> tasks[],
                 size_t taskCount,
                 const LogDigest& digest)
{
    std::unordered_set<uint64_t> replicaSet;
    for (size_t i = 0; i < taskCount; ++i) {
        foreach (auto replica, tasks[i]->result.segmentIdAndLength)
            replicaSet.insert(replica.first);
    }

    uint32_t missing = 0;
    for (int i = 0; i < digest.getSegmentCount(); i++) {
        uint64_t id = digest.getSegmentIds()[i];
        if (!contains(replicaSet, id)) {
            LOG(NOTICE, "Segment %lu listed in the log digest but not found "
                "among available backups", id);
            missing++;
        }
    }

    if (missing) {
        LOG(NOTICE,
            "%u segments in the digest but not available from backups",
            missing);
    }

    return !missing;
}

/**
 * Extract log digest from all the startReadingData results.
 * If multiple log digests are found the one from the replica with the
 * lowest segment id is used. If multiple digests are found taken from
 * replicas with the same segment id the shortest of the replicas is
 * returned. The rationale is that the objects in it are most
 * widely replicated. Keep in mind inconsistent open replicas (ones which
 * are missing writes that were acknowledged to applications) won't
 * be considered due to minOpenSegmentId, see
 * BackupStartTask::filterOutInvalidReplicas()).
 *
 * \param tasks
 *      Already run tasks holding the results of startReadingData calls
 *      to all of the available backups.
 * \param taskCount
 *      Number of elements in #tasks.
 * \return
 *      Tuple of segment id of the replica from which the log digest was
 *      taken, the length of the replica from which the log digest was
 *      taken, and the log digest itself. Empty if no log digest is found.
 */
Tub<std::tuple<uint64_t, uint32_t, LogDigest>>
findLogDigest(Tub<BackupStartTask> tasks[], size_t taskCount)
{
    uint64_t headId = ~0ul;
    uint32_t headLength = 0;
    void* headBuffer = NULL;
    uint32_t headBufferLength = 0;

    for (size_t i = 0; i < taskCount; ++i) {
        const auto& result = tasks[i]->result;
        if (!result.logDigestBuffer)
            continue;
        const uint64_t id = result.logDigestSegmentId;
        const uint32_t length = result.logDigestSegmentLen;
        if (length == ~0u) {
            LOG(ERROR, "Backup returned a log digest for segment %lu but "
                "listed the replica length as %u (which indicates it was "
                "closed); this is a bug and should never happen; "
                "ignoring this digest", id, length);
            continue;
        }
        if (id < headId || (id == headId && length < headLength)) {
            headBuffer = result.logDigestBuffer.get();
            headBufferLength = result.logDigestBytes;
            headId = id;
            headLength = length;
        }
    }

    if (headId == ~(0lu))
        return {};
    return {std::make_tuple(headId, headLength,
                            LogDigest(headBuffer, headBufferLength))};
}

/// Used in buildReplicaMap().
struct ReplicaAndLoadTime {
    RecoverRpc::Replica replica;
    uint64_t expectedLoadTimeMs;
    bool operator<(const ReplicaAndLoadTime& r) const {
        return expectedLoadTimeMs < r.expectedLoadTimeMs;
    }
};

/**
 * Create the script that recovery masters will replay.
 * First add all primaries to the list, then all secondaries.
 * Order primaries (and even secondaries among themselves anyway) based
 * on when they are expected to be loaded in from disk.
 *
 * \param tasks
 *      Already run tasks holding the results of startReadingData calls
 *      to all of the available backups.
 * \param taskCount
 *      Number of elements in #tasks.
 *  \param tracker
 *      Provides the estimated backup read speed to order the script entries.
 *  \param headId
 *      Only replicas from segments less or equal than this are included in the
 *      script.
 *      Determined by findLogDigest().
 *  \param headLength
 *      Replicas of the head segment are only included in the script if their
 *      length matches this. Determined by findLogDigest().
 *  \return
 *      Script which indicates to recovery masters which replicas are on which
 *      backups and (approximately) what order segments should be replayed in.
 *      Sent to all recovery masters verbatim.
 */
vector<RecoverRpc::Replica>
buildReplicaMap(Tub<BackupStartTask> tasks[],
                size_t taskCount,
                RecoveryTracker* tracker,
                uint64_t headId,
                uint32_t headLength)
{
    vector<ReplicaAndLoadTime> replicasToSort;
    for (uint32_t taskIndex = 0; taskIndex < taskCount; taskIndex++) {
        const auto& task = tasks[taskIndex];
        const auto backupId = task->backupId;
        const uint64_t speed = (*tracker).getServerDetails(backupId)->
                                                    expectedReadMBytesPerSec;

        LOG(DEBUG, "Adding %lu segment replicas from %lu "
                   "with bench speed of %lu",
            task->result.segmentIdAndLength.size(), backupId.getId(), speed);

        for (size_t i = 0; i < task->result.segmentIdAndLength.size(); ++i) {
            uint64_t expectedLoadTimeMs;
            if (i < task->result.primarySegmentCount) {
                // for primaries just estimate when they'll load
                expectedLoadTimeMs = (i + 1) * 8 * 1000 / (speed ?: 1);
            } else {
                // for secondaries estimate when they'll load
                // but add a huge bias so secondaries don't overlap
                // with primaries but are still interleaved
                expectedLoadTimeMs =
                    ((i + 1) - task->result.primarySegmentCount) *
                                     8 * 1000/ (speed ?: 1);
                expectedLoadTimeMs += 1000000;
            }
            uint64_t segmentId = task->result.segmentIdAndLength[i].first;
            uint32_t segmentLen = task->result.segmentIdAndLength[i].second;
            if (segmentId < headId ||
                (segmentId == headId && segmentLen == headLength)) {
                ReplicaAndLoadTime r {{ backupId.getId(), segmentId },
                                      expectedLoadTimeMs};
                replicasToSort.push_back(r);
            } else {
                const char* why;
                if (segmentId == headId && segmentLen < headLength) {
                    why = "shorter than";
                } else if (segmentId == headId && segmentLen > headLength) {
                    why = "longer than";
                } else {
                    why = "past";
                }
                LOG(DEBUG, "Ignoring replica for "
                    "segment ID %lu, len %u from backup %lu "
                    "because it's %s the head segment (%lu, %u)",
                    segmentId, segmentLen, backupId.getId(),
                    why, headId, headLength);
            }
        }
    }
    std::sort(replicasToSort.begin(), replicasToSort.end());
    vector<RecoverRpc::Replica> replicaMap;
    foreach(const auto& sortedReplica, replicasToSort) {
        LOG(DEBUG, "Load segment %lu replica from backup %lu "
            "with expected load time of %lu ms",
            sortedReplica.replica.segmentId,
            sortedReplica.replica.backupId,
            sortedReplica.expectedLoadTimeMs);
        replicaMap.push_back(sortedReplica.replica);
    }
    return replicaMap;
}
} // end namespace
using namespace RecoveryInternal; // NOLINT

/**
 * Builds a map describing where replicas for each segment that is part of
 * the crashed master's log can be found. Collects replica information by
 * contacting all backups and ensures that the collected information makes
 * up a complete and recoverable log.
 */
void
Recovery::startBackups()
{
    recoveryTicks.construct(&metrics->coordinator.recoveryTicks);
    CycleCounter<RawMetric>
        _(&metrics->coordinator.recoveryBuildReplicaMapTicks);

    if (numPartitions == 0) {
        LOG(NOTICE, "Server %lu crashed, but it had no tablets",
            crashedServerId.getId());
        status = DONE;
        if (owner) {
            owner->recoveryFinished(this);
            owner->destroyAndFreeRecovery(this);
        }
        return;
    }

    LOG(DEBUG, "Getting segment lists from backups and preparing "
               "them for recovery");

    const uint32_t maxActiveBackupHosts = 10;
    std::vector<ServerId> backups =
        tracker->getServersWithService(BACKUP_SERVICE);
    /// List of asynchronous startReadingData tasks and their replies
    auto backupStartTasks = std::unique_ptr<Tub<BackupStartTask>[]>(
            new Tub<BackupStartTask>[backups.size()]);
    uint32_t i = 0;
    foreach (ServerId backup, backups) {
        backupStartTasks[i++].construct(this,
                                        backup,
                                        crashedServerId, tabletsToRecover,
                                        minOpenSegmentId);
    }
    parallelRun(backupStartTasks.get(), backups.size(), maxActiveBackupHosts);

    auto digestInfo = findLogDigest(backupStartTasks.get(), backups.size());
    if (!digestInfo) {
        LOG(NOTICE, "No log digest among replicas on available backups. "
            "Will retry recovery later.");
        if (owner) {
            owner->recoveryFinished(this);
            owner->destroyAndFreeRecovery(this);
        }
        return;
    }
    uint64_t headId = std::get<0>(*digestInfo);
    uint32_t headLength = std::get<1>(*digestInfo);
    LogDigest digest = std::get<2>(*digestInfo);

    LOG(NOTICE, "Segment %lu of length %u bytes is the head of the log",
        headId, headLength);

    bool logIsComplete = verifyLogComplete(backupStartTasks.get(),
                                           backups.size(), digest);
    if (!logIsComplete) {
        LOG(NOTICE, "Some replicas from log digest not on available backups. "
            "Will retry recovery later.");
        if (owner) {
            owner->recoveryFinished(this);
            owner->destroyAndFreeRecovery(this);
        }
        return;
    }

    replicaMap = buildReplicaMap(backupStartTasks.get(), backups.size(),
                                 tracker, headId, headLength);

    status = START_RECOVERY_MASTERS;
    schedule();
}

namespace RecoveryInternal {
/// Used in Recovery::startRecoveryMasters().
struct MasterStartTask {
    MasterStartTask(Recovery& recovery,
                    ServerId serverId,
                    uint32_t partitionId,
                    const vector<RecoverRpc::Replica>& replicaMap)
        : recovery(recovery)
        , serverId(serverId)
        , replicaMap(replicaMap)
        , partitionId(partitionId)
        , tabletsToRecover()
        , masterClient()
        , rpc()
        , done(false)
        , testingCallback(recovery.testingMasterStartTaskSendCallback)
    {}
    bool isReady() { return testingCallback || (rpc && rpc->isReady()); }
    bool isDone() { return done; }
    void send() {
        LOG(NOTICE, "Starting recovery %lu on recovery master %lu, "
            "partition %d", recovery.recoveryId, serverId.getId(), partitionId);
        (*recovery.tracker)[serverId] = &recovery;
        try {
            if (!testingCallback) {
                masterClient.construct(recovery.tracker->getSession(serverId));
                rpc.construct(*masterClient,
                              recovery.recoveryId,
                              recovery.crashedServerId,
                              recovery.testingFailRecoveryMasters > 0
                                  ? ~0u : partitionId,
                              tabletsToRecover,
                              replicaMap.data(),
                              downCast<uint32_t>(replicaMap.size()));
                if (recovery.testingFailRecoveryMasters > 0) {
                    LOG(NOTICE, "Told recovery master %lu to kill itself",
                        serverId.getId());
                    --recovery.testingFailRecoveryMasters;
                }
            } else {
                testingCallback->masterStartTaskSend(recovery.recoveryId,
                                                     recovery.crashedServerId,
                                                     partitionId,
                                                     tabletsToRecover,
                                                     replicaMap.data(),
                                                     replicaMap.size());
            }
            return;
        } catch (const TransportException& e) {
            LOG(WARNING, "Couldn't contact server %lu to start recovery: %s",
                serverId.getId(), e.what());
        } catch (const ClientException& e) {
            LOG(WARNING, "Couldn't contact server %lu to start recovery: %s",
                serverId.getId(), e.what());
        }
        recovery.recoveryMasterFinished(serverId, false);
        done = true;
    }
    void wait() {
        try {
            if (!testingCallback)
                (*rpc)();
            done = true;
            return;
        } catch (const TransportException& e) {
            LOG(WARNING, "Couldn't contact server %lu to start recovery: %s",
                serverId.getId(), e.what());
        } catch (const ClientException& e) {
            LOG(WARNING, "Couldn't contact server %lu to start recovery: %s",
                serverId.getId(), e.what());
        }
        recovery.recoveryMasterFinished(serverId, false);
        done = true;
    }

    /// The parent recovery object.
    Recovery& recovery;

    /// Id of master server to kick off this partition's recovery on.
    ServerId serverId;

    const vector<RecoverRpc::Replica>& replicaMap;
    const uint32_t partitionId;
    ProtoBuf::Tablets tabletsToRecover;
    Tub<MasterClient> masterClient;
    Tub<MasterClient::Recover> rpc;
    bool done;
    MasterStartTaskTestingCallback* testingCallback;
    DISALLOW_COPY_AND_ASSIGN(MasterStartTask);
};
}
using namespace RecoveryInternal; // NOLINT

/**
 * Start recovery of each of the partitions of the will on a recovery master.
 * Each master will only be assigned one partition of one will at a time. If
 * there are too few masters to perform the full recovery then only a subset
 * of the partitions will be recovered. When this recovery completes if there
 * are partitions of the will the still need recovery a follow up recovery
 * will be scheduled.
 */
void
Recovery::startRecoveryMasters()
{
    CycleCounter<RawMetric> _(&metrics->coordinator.recoveryStartTicks);
    LOG(NOTICE, "Starting recovery %lu for crashed server %lu with %u "
        "partitions", recoveryId, crashedServerId.getId(), numPartitions);

    // Set up the tasks to execute the RPCs.
    std::vector<ServerId> masters =
        tracker->getServersWithService(MASTER_SERVICE);
    uint32_t started = 0;
    Tub<MasterStartTask> recoverTasks[numPartitions];
    foreach (ServerId master, masters) {
        if (started == numPartitions)
            break;
        Recovery* preexistingRecovery = (*tracker)[master];
        if (!preexistingRecovery) {
            auto& task = recoverTasks[started];
            task.construct(*this, master, started, replicaMap);
            ++started;
        }
    }

    // If we couldn't find enough masters that weren't already busy with
    // another recovery, then count the remaining partitions as having
    // been on unsuccessful recovery masters so we know when to quit
    // waiting for recovery masters.
    const uint32_t partitionsWithoutARecoveryMaster = (numPartitions - started);
    if (partitionsWithoutARecoveryMaster > 0) {
        LOG(NOTICE, "Couldn't find enough masters not already performing a "
            "recovery to recover all partitions: %u partitions will be "
            "recovered later", partitionsWithoutARecoveryMaster);
        for (uint32_t i = 0; i < partitionsWithoutARecoveryMaster; ++i)
            recoveryMasterFinished(ServerId(), false);
    }

    // Hand out each tablet from the will to one of the recovery masters
    // depending on which partition it was in.
    foreach (auto& tablet, tabletsToRecover.tablet()) {
        auto& task = recoverTasks[tablet.user_data()];
        if (task)
            *task->tabletsToRecover.add_tablet() = tablet;
    }

    // Tell the recovery masters to begin recovery.
    parallelRun(recoverTasks, numPartitions, 10);

    // If all of the recovery masters failed to get off to a start then
    // skip waiting for them.
    if (status > WAIT_FOR_RECOVERY_MASTERS)
        return;
    status = WAIT_FOR_RECOVERY_MASTERS;
    LOG(DEBUG, "Waiting for recovery to complete on recovery masters");
}

/**
 * Record the completion of a recovery on a single recovery master.
 * If this call causes all the recovery masters that are part of
 * the recovery to be accounted for then the recovery is marked
 * as done and moves to the next phase (cleanup phases).
 * Idempotent for each recovery master; duplicated calls by a
 * recovery master will be ignored.
 *
 * \param recoveryMasterId
 *      ServerId of the master which has successfully or
 *      unsuccessfully finished recovering the partition of the
 *      will which was assigned to it. If invalid (ServerId())
 *      then the check to ensure this recovery master is
 *      stil part of the recovery is skipped (used above to
 *      recycle this code for the case when there was no
 *      recovery master to give a partition to.
 * \param successful
 *      True if the recovery master successfully recovered its
 *      partition of the crashed master and is ready to take
 *      ownership of those tablets. Othwerwise, false (if the
 *      recovery master couldn't recover its partition or the
 *      recovery master failed before completing recovery.
 */
void
Recovery::recoveryMasterFinished(ServerId recoveryMasterId,
                                 bool successful)
{
    if (recoveryMasterId.isValid()) {
        if (!(*tracker)[recoveryMasterId])
            return;
        (*tracker)[recoveryMasterId] = NULL;
    }

    if (successful) {
        ++successfulRecoveryMasters;
    } else {
        ++unsuccessfulRecoveryMasters;
        if (recoveryMasterId.isValid())
            LOG(NOTICE, "Recovery master %lu failed to recover its partition "
                "of the will for crashed server %lu", recoveryMasterId.getId(),
                crashedServerId.getId());
    }

    const uint32_t completedRecoveryMasters =
        successfulRecoveryMasters + unsuccessfulRecoveryMasters;
    if (completedRecoveryMasters == numPartitions) {
        recoveryTicks.destroy();
        status = BROADCAST_RECOVERY_COMPLETE;
        if (wasCompletelySuccessful()) {
            schedule();
            if (owner)
                owner->recoveryFinished(this);
#if BCAST_INLINE
            broadcastRecoveryComplete();
#endif
        } else {
            LOG(DEBUG, "Recovery wasn't completely successful; will not "
                "broadcast the end of recovery %lu for server %lu to backups",
                recoveryId, crashedServerId.getId());
            status = DONE;
            if (owner) {
                owner->recoveryFinished(this);
                owner->destroyAndFreeRecovery(this);
            }
        }
    }
}

namespace RecoveryInternal {
/**
 * AsynchronousTaskConcept which contacts a backup and informs it
 * that recovery has completed.
 * Used in Recovery::broadcastRecoveryComplete().
 */
struct BackupEndTask {
    BackupEndTask(Recovery& recovery,
                  ServerId serverId,
                  ServerId crashedServerId)
        : recovery(recovery)
        , serverId(serverId)
        , crashedServerId(crashedServerId)
        , rpc()
        , done(false)
        , testingCallback(recovery.testingBackupEndTaskSendCallback)
    {}
    bool isReady() { return rpc && rpc->isReady(); }
    bool isDone() { return done; }
    void send() {
        if (testingCallback) {
            testingCallback->backupEndTaskSend(serverId, crashedServerId);
            done = true;
            return;
        }
        try {
            rpc.construct(recovery.context, serverId, crashedServerId);
            return;
        } catch (const TransportException& e) {
            LOG(DEBUG, "recoveryComplete failed on %lu, ignoring; "
                "failure was: %s", serverId.getId(), e.what());
        } catch (const ClientException& e) {
            LOG(DEBUG, "recoveryComplete failed on %lu, ignoring; "
                "failure was: %s", serverId.getId(), e.what());
        }
        rpc.destroy();
        done = true;
    }
    void wait() {
        if (!rpc)
            return;
        try {
            rpc->wait();
        } catch (const ServerDoesntExistException& e) {
            // Ryan, please add appropriate code here.
        } catch (const TransportException& e) {
            LOG(DEBUG, "recoveryComplete failed on %lu, ignoring; "
                "failure was: %s", serverId.getId(), e.what());
        } catch (const ClientException& e) {
            LOG(DEBUG, "recoveryComplete failed on %lu, ignoring; "
                "failure was: %s", serverId.getId(), e.what());
        }
        done = true;
    }
    Recovery& recovery;
    const ServerId serverId;
    const ServerId crashedServerId;
    Tub<RecoveryCompleteRpc2> rpc;
    bool done;
    BackupEndTaskTestingCallback* testingCallback;
    DISALLOW_COPY_AND_ASSIGN(BackupEndTask);
};
} // end namespace

/**
 * Notify backups that the crashed master has been recovered and all state
 * associated with it can be discarded.
 */
void
Recovery::broadcastRecoveryComplete()
{
    LOG(DEBUG, "Broadcasting the end of recovery %lu for server %lu to backups",
        recoveryId, crashedServerId.getId());
    CycleCounter<RawMetric> ticks(&metrics->coordinator.recoveryCompleteTicks);
    std::vector<ServerId> backups =
        tracker->getServersWithService(BACKUP_SERVICE);
    Tub<BackupEndTask> tasks[backups.size()];
    size_t taskNum = 0;
    foreach (ServerId backup, backups)
        tasks[taskNum++].construct(*this, backup, crashedServerId);
    parallelRun(tasks, backups.size(), 10);
}

} // namespace RAMCloud
