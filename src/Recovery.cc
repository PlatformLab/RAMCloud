/* Copyright (c) 2010-2011 Stanford University
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
 * \param taskQueue
 *      MasterRecoveryManager TaskQueue which drives this recovery
 *      (by calling performTask() whenever this recovery is scheduled).
 * \param tracker
 *      The MasterRecoveryManager's tracker which maintains a list of all
 *      servers in RAMCloud along with a pointer to any Recovery the server is
 *      particpating in (as a recovery master). This is used to select recovery
 *      masters and to find all backup data for the crashed master.
 * \param deleter
 *      Used to delete this (and inform the MasterRecoveryManager) when
 *      this determines it is no longer needed. May be NULL for testing
 *      if the test takes care of deallocation of the Recovery.
 * \param crashedServerId
 *      The crashed master this Recovery will rebuild.
 * \param will
 *      A partitioned set of tablets or "will" of the crashed Master.
 *      It is represented as a tablet map with a partition id in the
 *      user_data field.  Partition ids must start at 0 and be
 *      consecutive.  No partition id can have 0 entries before
 *      any other partition that has more than 0 entries.  This
 *      is because the recovery recovers partitions up but excluding the
 *      first with no entries.
 * \param minOpenSegmentId
 *      Used to filter out replicas of segments which may have become
 *      inconsistent. A replica with a segment id less than this is
 *      not eligible to be used for recovery (both for log digest and
 *      object data purposes).
 */
Recovery::Recovery(TaskQueue& taskQueue,
                   RecoveryTracker* tracker,
                   Deleter* deleter,
                   ServerId crashedServerId,
                   const ProtoBuf::Tablets& will,
                   uint64_t minOpenSegmentId)
    : Task(taskQueue)
    , crashedServerId(crashedServerId)
    , will(will)
    , minOpenSegmentId(minOpenSegmentId)
    , tracker(tracker)
    , deleter(deleter)
    , recoveryId(generateRandom())
    , status(BUILD_REPLICA_MAP)
    , recoveryTicks()
    , replicaMap()
    , numPartitions()
    , successfulRecoveryMasters()
    , unsuccessfulRecoveryMasters()
    , testingBackupStartTaskSendCallback()
    , testingMasterStartTaskSendCallback()
{
    metrics->coordinator.recoveryCount++;

    // Figure out the number of partitions specified in the will.
    foreach (auto& tablet, will.tablet()) {
        if (tablet.user_data() + 1 > numPartitions)
            numPartitions = downCast<uint32_t>(tablet.user_data()) + 1;
    }
}

Recovery::~Recovery()
{
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
    switch (status) {
    case BUILD_REPLICA_MAP:
        recoveryTicks.construct(&metrics->coordinator.recoveryTicks);
        LOG(DEBUG, "Building replica map");
        buildReplicaMap();
        status = START_RECOVERY_MASTERS;
        schedule();
        break;
    case START_RECOVERY_MASTERS:
        LOG(DEBUG, "Starting recovery on recovery masters");
        startRecoveryMasters();
        status = WAIT_FOR_RECOVERY_MASTERS;
        LOG(DEBUG, "Waiting for recovery to complete on recovery masters");
        // Don't schedule(); calls to recoveryMasterFinished drive
        // recovery from WAIT_FOR_RECOVERY_MASTERS to
        // BROADCAST_RECOVERY_COMPLETE.
        break;
    case WAIT_FOR_RECOVERY_MASTERS:
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
        if (deleter)
            deleter->destroyAndFreeRecovery(this);
        break;
    case DONE:
    default:
        assert(false);
    }
}

/**
 * Returns true if recovery was started on recovery masters and all have
 * completed (either successfully or unsuccessfully). When this returns
 * true the crashed master has been recovered, though the recovery may
 * continue doing additional cleanup operations before deleting itself.
 */
bool
Recovery::isDone() const
{
    return status > WAIT_FOR_RECOVERY_MASTERS;
}

/**
 * If isDone() then return whether all the recovery masters were successful
 * in recovering their partitions of the crashed master. Useful for
 * determining if a further recovery needs to be scheduled for this
 * crashed master. If !isDone() the return value has no meaning.
 */
bool
Recovery::wasCompletelySuccessful() const
{
    assert(isDone());
    return unsuccessfulRecoveryMasters == 0;
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
/**
 * AsynchronousTaskConcept which contacts a backup, informs it
 * that it should load/partition replicas for segments belonging to the
 * crashed master, and gathers any log digest and list of replicas the
 * backup had for the crashed master.
 * Only used in Recovery::buildReplicaMap().
 */
class BackupStartTask {
  PUBLIC:
    BackupStartTask(Recovery& recovery,
                    ServerId backupId,
                    ServerId crashedMasterId,
                    const ProtoBuf::Tablets& partitions,
                    uint64_t minOpenSegmentId);
    bool isDone() const { return done; }
    bool isReady() { return testingCallback || (rpc && rpc->isReady()); }
    void send();
    void filterOutInvalidReplicas();
    void wait();
    const ServerId backupId;
    BackupClient::StartReadingData::Result result;
  PRIVATE:
    const ServerId crashedMasterId;
    const ProtoBuf::Tablets& partitions;
    const uint64_t minOpenSegmentId;
    Tub<BackupClient> client;
    Tub<BackupClient::StartReadingData> rpc;
    bool done;
    BackupStartTaskTestingCallback* testingCallback;
    DISALLOW_COPY_AND_ASSIGN(BackupStartTask);
};

BackupStartTask::BackupStartTask(
            Recovery& recovery,
            ServerId backupId,
            ServerId crashedMasterId,
            const ProtoBuf::Tablets& partitions,
            uint64_t minOpenSegmentId)
    : backupId(backupId)
    , result()
    , crashedMasterId(crashedMasterId)
    , partitions(partitions)
    , minOpenSegmentId(minOpenSegmentId)
    , client()
    , rpc()
    , done()
    , testingCallback(recovery.testingBackupStartTaskSendCallback)
{
    if (!testingCallback)
        client.construct(recovery.tracker->getSession(backupId));
}

/// Asynchronously send the startReadingData RPC to #backupHost.
void
BackupStartTask::send()
{
    if (!testingCallback) {
        rpc.construct(*client, crashedMasterId, partitions);
    } else {
        testingCallback->backupStartTaskSend(result);
    }
    LOG(DEBUG, "Starting startReadingData on backup %lu", backupId.getId());
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
        newIdAndLength.push_back(idAndLength);
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
            backupId.getId(), result.logDigestSegmentId,
            minOpenSegmentId);
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
            result = (*rpc)();
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
    client.destroy();

    filterOutInvalidReplicas();

    done = true;
    LOG(DEBUG, "Backup %lu has %lu segment replicas",
        backupId.getId(), result.segmentIdAndLength.size());
}

/**
 * Used in buildReplicaMap() to sort replicas by the expected time when
 * they will be ready.
 */
struct ReplicaAndLoadTime {
    RecoverRpc::Replica replica;
    uint64_t expectedLoadTimeMs;
};

/**
 * Used in buildReplicaMap() to sort replicas by the expected time when
 * they will be ready.
 */
bool
expectedLoadCmp(const ReplicaAndLoadTime& l, const ReplicaAndLoadTime& r)
{
    return l.expectedLoadTimeMs < r.expectedLoadTimeMs;
}

struct SegmentAndDigestTuple {
    SegmentAndDigestTuple(ServerId backupId,
                          uint64_t segmentId, uint32_t segmentLength,
                          const void* logDigestPtr, uint32_t logDigestBytes)
        : backupId(backupId)
        , segmentId(segmentId)
        , segmentLength(segmentLength)
        , logDigest(logDigestPtr, logDigestBytes)
    {}

    ServerId backupId;
    uint64_t  segmentId;
    uint32_t  segmentLength;
    LogDigest logDigest;
};
} // end namespace
using namespace RecoveryInternal; // NOLINT

/**
 * Builds a map describing where replicas for each segment that is part of
 * the crashed master's log can be found. Collects replica information by
 * contacting all backups and ensures that the collected information makes
 * up a complete and recoverable log.
 */
void
Recovery::buildReplicaMap()
{
    CycleCounter<RawMetric>
        _(&metrics->coordinator.recoveryBuildReplicaMapTicks);
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
        backupStartTasks[i++].construct(*this,
                                        backup,
                                        crashedServerId, will,
                                        minOpenSegmentId);
    }
    parallelRun(backupStartTasks.get(), backups.size(), maxActiveBackupHosts);

    // Map of Segment Ids -> counts of backup copies that were found.
    // This is used to cross-check with the log digest.
    std::unordered_map<uint64_t, uint32_t> segmentMap;
    for (size_t i = 0; i < backups.size(); ++i) {
        const auto& task = backupStartTasks[i];
        foreach (auto replica, task->result.segmentIdAndLength) {
            uint64_t segmentId = replica.first;
            ++segmentMap[segmentId];
        }
    }

    // List of serialised LogDigests from possible log heads, including
    // the corresponding Segment IDs and lengths.
    vector<SegmentAndDigestTuple> digestList;
    for (size_t i = 0; i < backups.size(); ++i) {
        const auto& task = backupStartTasks[i];

        // If this backup returned a potential head of the log and it
        // includes a LogDigest, set it aside for verifyCompleteLog().
        if (task->result.logDigestBuffer) {
            digestList.push_back({ task->backupId,
                                   task->result.logDigestSegmentId,
                                   task->result.logDigestSegmentLen,
                                   task->result.logDigestBuffer.get(),
                                   task->result.logDigestBytes });

            if (task->result.logDigestSegmentId == (uint32_t)-1) {
                LOG(ERROR, "Segment %lu has a LogDigest, but its length "
                    "is -1 from server %lu", task->result.logDigestSegmentId,
                    task->backupId.getId());
            }
        }
    }

    /*
     * Check to see if the Log being recovered can actually be recovered.
     * This requires us to use the Log head's LogDigest, which was returned
     * in response to startReadingData. That gives us a complete list of
     * the Segment IDs we need to do a full recovery.
     */
    uint64_t headId = ~0UL;
    uint32_t headLen = 0;
    {
        // find the newest head
        SegmentAndDigestTuple* headReplica = NULL;

        foreach (auto& digestTuple, digestList) {
            uint64_t id = digestTuple.segmentId;
            uint32_t len = digestTuple.segmentLength;

            if (id < headId || (id == headId && len >= headLen)) {
                headReplica = &digestTuple;
                headId = id;
                headLen = len;
            }
        }

        if (headReplica == NULL) {
            // we're seriously boned.
            LOG(ERROR, "No log head & digest found!! Kiss your data good-bye!");
            throw Exception(HERE, "ouch! data lost!");
        }

        LOG(NOTICE, "Segment %lu of length %u bytes from backup %lu "
            "is the head of the log",
            headId, headLen, headReplica->backupId.getId());

        // scan the backup map to determine if all needed segments are available
        uint32_t missing = 0;
        for (int i = 0; i < headReplica->logDigest.getSegmentCount(); i++) {
            uint64_t id = headReplica->logDigest.getSegmentIds()[i];
            if (!contains(segmentMap, id)) {
                LOG(ERROR, "Segment %lu is missing!", id);
                missing++;
            }
        }

        if (missing) {
            LOG(ERROR,
                "%u segments in the digest, but not obtained from backups!",
                missing);
            // TODO(ongaro): Should we abort here?
        }
    }

    // Create the script that recovery masters will replay.
    // First add all primaries to the list, then all secondaries.
    // Order primaries (and even secondaries among themselves anyway) based
    // on when they are expected to be loaded in from disk.
    vector<ReplicaAndLoadTime> replicasToSort;
    for (uint32_t taskIndex = 0; taskIndex < backups.size(); taskIndex++) {
        const auto& task = backupStartTasks[taskIndex];
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
                expectedLoadTimeMs = i * 8 * 1000 / (speed ?: 1);
            } else {
                // for secondaries estimate when they'll load
                // but add a huge bias so secondaries don't overlap
                // with primaries but are still interleaved
                expectedLoadTimeMs = (i - task->result.primarySegmentCount) *
                                     8 * 1000/ (speed ?: 1);
                expectedLoadTimeMs += 1000000;
            }
            uint64_t segmentId = task->result.segmentIdAndLength[i].first;
            uint32_t segmentLen = task->result.segmentIdAndLength[i].second;
            if (segmentId < headId ||
                (segmentId == headId && segmentLen == headLen)) {
                ReplicaAndLoadTime r {{ backupId.getId(), segmentId },
                                      expectedLoadTimeMs};
                replicasToSort.push_back(r);
            } else {
                const char* why;
                if (segmentId == headId && segmentLen < headLen) {
                    why = "shorter than";
                } else if (segmentId == headId && segmentLen > headLen) {
                    why = "longer than";
                } else {
                    why = "past";
                }
                LOG(DEBUG, "Ignoring replica for "
                    "segment ID %lu, len %u from backup %lu "
                    "because it's %s the head segment (%lu, %u)",
                    segmentId, segmentLen, backupId.getId(),
                    why, headId, headLen);
            }
        }
    }
    std::sort(replicasToSort.begin(), replicasToSort.end(), expectedLoadCmp);
    foreach(const auto& sortedReplica, replicasToSort) {
        LOG(DEBUG, "Load segment %lu replica from backup %lu "
            "with expected load time of %lu ms",
            sortedReplica.replica.segmentId,
            sortedReplica.replica.backupId,
            sortedReplica.expectedLoadTimeMs);
        replicaMap.push_back(sortedReplica.replica);
    }
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
        , tablets()
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
                              recovery.crashedServerId, partitionId,
                              tablets,
                              replicaMap.data(),
                              downCast<uint32_t>(replicaMap.size()));
            } else {
                testingCallback->masterStartTaskSend(recovery.recoveryId,
                                                     recovery.crashedServerId,
                                                     partitionId,
                                                     tablets,
                                                     replicaMap.data(),
                                                     replicaMap.size());
            }
            return;
        } catch (const TransportException& e) {
            LOG(WARNING, "Couldn't contact server %lu; to start recovery: %s",
                serverId.getId(), e.what());
        } catch (const ClientException& e) {
            LOG(WARNING, "Couldn't contact server %lu; to start recovery: %s",
                serverId.getId(), e.what());
        }
        ++recovery.unsuccessfulRecoveryMasters;
        (*recovery.tracker)[serverId] = NULL;
    }
    void wait() {
        try {
            if (!testingCallback)
                (*rpc)();
            done = true;
            return;
        } catch (const TransportException& e) {
            LOG(WARNING, "Couldn't contact server %lu; to start recovery: %s",
                serverId.getId(), e.what());
        } catch (const ClientException& e) {
            LOG(WARNING, "Couldn't contact server %lu; to start recovery: %s",
                serverId.getId(), e.what());
        }
        ++recovery.unsuccessfulRecoveryMasters;
        (*recovery.tracker)[serverId] = NULL;
    }

    /// The parent recovery object.
    Recovery& recovery;

    /// Id of master server to kick off this partition's recovery on.
    ServerId serverId;

    const vector<RecoverRpc::Replica>& replicaMap;
    const uint32_t partitionId;
    ProtoBuf::Tablets tablets;
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

    // Set up the tasks to execute the RPCs.
    std::vector<ServerId> masters =
        tracker->getServersWithService(MASTER_SERVICE);
    uint32_t started = 0;
    Tub<MasterStartTask> recoverTasks[numPartitions];
    foreach (ServerId master, masters) {
        Recovery* preexistingRecovery = (*tracker)[master];
        if (!preexistingRecovery) {
            auto& task = recoverTasks[started];
            task.construct(*this, master, started, replicaMap);
            ++started;
        }
        if (started == numPartitions)
            break;
    }

    // If we couldn't find enough masters that weren't already busy with
    // another recovery, then count the remaining partitions as having
    // been on unsuccessful recovery masters so we know when to quit
    // waiting for recovery masters.
    unsuccessfulRecoveryMasters += (numPartitions - started);
    if (unsuccessfulRecoveryMasters > 0)
        LOG(NOTICE, "Couldn't find enough masters not already performing a "
            "recovery to recover all partitions: %u partitions will be "
            "recovered later", unsuccessfulRecoveryMasters);

    // Hand out each tablet from the will to on of the recovery masters
    // depending on which partition it was in.
    foreach (auto& tablet, will.tablet()) {
        auto& task = recoverTasks[tablet.user_data()];
        if (task)
            *task->tablets.add_tablet() = tablet;
    }

    // Tell the recovery masters to begin recovery.
    LOG(NOTICE, "Starting recovery for %u partitions", numPartitions);
    parallelRun(recoverTasks, numPartitions, 10);
}

/**
 * Record the completion of a recovery on a single recovery master.
 * If this call causes all the recovery masters that are part of
 * the recovery to be accounted for then the recovery is marked
 * as isDone() and moves to the next phase (cleanup phases).
 *
 * \param recoveryMasterId
 *      ServerId of the master which has successfully or
 *      unsuccessfully finished recovering the partition of the
 *      will which was assigned to it.
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
    if (!(*tracker)[recoveryMasterId])
        return;
    (*tracker)[recoveryMasterId] = NULL;

    if (successful)
        ++successfulRecoveryMasters;
    else
        ++unsuccessfulRecoveryMasters;

    const uint32_t completedRecoveryMasters =
        successfulRecoveryMasters + unsuccessfulRecoveryMasters;
    if (completedRecoveryMasters == numPartitions) {
        recoveryTicks.destroy();
        status = BROADCAST_RECOVERY_COMPLETE;
        schedule();
#if BCAST_INLINE
        broadcastRecoveryComplete();
#endif
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
        , client()
        , rpc()
        , done(false)
    {}
    bool isReady() { return rpc && rpc->isReady(); }
    bool isDone() { return done; }
    void send() {
        try {
            client.construct(recovery.tracker->getSession(serverId));
            rpc.construct(*client, crashedServerId);
            return;
        } catch (const TransportException& e) {
            LOG(DEBUG, "recoveryComplete failed on %lu, ignoring; "
                "failure was: %s", serverId.getId(), e.what());
        } catch (const ClientException& e) {
            LOG(DEBUG, "recoveryComplete failed on %lu, ignoring; "
                "failure was: %s", serverId.getId(), e.what());
        }
        client.destroy();
        rpc.destroy();
        done = true;
    }
    void wait() {
        if (!rpc)
            return;
        try {
            (*rpc)();
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
    Tub<BackupClient> client;
    Tub<BackupClient::RecoveryComplete> rpc;
    bool done;
    DISALLOW_COPY_AND_ASSIGN(BackupEndTask);
};
} // end namespace

/**
 * Notify backups that the crashed master has been recovered and all state
 * associated with it can be discarded. Also stops the backup local
 * recovery-length timer for recovery metrics.
 */
void
Recovery::broadcastRecoveryComplete()
{
    CycleCounter<RawMetric> ticks(&metrics->coordinator.recoveryCompleteTicks);
    // broadcast to backups that recovery is done
    std::vector<ServerId> backups =
        tracker->getServersWithService(BACKUP_SERVICE);
    Tub<BackupEndTask> tasks[backups.size()];
    size_t taskNum = 0;
    foreach (ServerId backup, backups)
        tasks[taskNum++].construct(*this, backup, crashedServerId);
    parallelRun(tasks, backups.size(), 10);
}

} // namespace RAMCloud
