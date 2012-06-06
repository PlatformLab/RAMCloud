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
 * \param deleter
 *      Used to delete this (and inform the MasterRecoveryManager) when
 *      this determines it is no longer needed.
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
 * \param serverList
 *      Reference to the Coordinator's list of all servers in RAMCloud. This
 *      is used to select recovery masters and to find all backup data for
 *      the crashed master.
 */
Recovery::Recovery(TaskQueue& taskQueue,
                   const CoordinatorServerList& serverList,
                   Deleter& deleter,
                   ServerId crashedServerId,
                   const ProtoBuf::Tablets& will)
    : Task(taskQueue)
    , crashedServerId(crashedServerId)
    , will(will)
    , serverList(serverList)
    , deleter(deleter)
    , recoveryId(generateRandom())
    , status(BUILD_REPLICA_MAP)
    , recoveryTicks()
    , replicaMap()
    , startedRecoveryMasters()
    , successfulRecoveryMasters()
    , unsuccessfulRecoveryMasters()
{
    metrics->coordinator.recoveryCount++;
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
        deleter.destroyAndFreeRecovery(this);
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
Recovery::isDone()
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
Recovery::wasCompletelySuccessful()
{
    assert(isDone());
    return unsuccessfulRecoveryMasters == 0;
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
    BackupStartTask(const CoordinatorServerList::Entry& backupHost,
                    ServerId crashedMasterId,
                    const ProtoBuf::Tablets& partitions,
                    uint64_t minOpenSegmentId);
    bool isDone() const { return done; }
    bool isReady() { return rpc && rpc->isReady(); }
    void send();
    void filterOutInvalidReplicas();
    void wait();
    const CoordinatorServerList::Entry backupHost;
  PRIVATE:
    const ServerId crashedMasterId;
    const ProtoBuf::Tablets& partitions;
    const uint64_t minOpenSegmentId;
    Tub<BackupClient> client;
    Tub<BackupClient::StartReadingData> rpc;
  PUBLIC:
    BackupClient::StartReadingData::Result result;
  PRIVATE:
    bool done;
    DISALLOW_COPY_AND_ASSIGN(BackupStartTask);
};

BackupStartTask::BackupStartTask(
            const CoordinatorServerList::Entry& backupHost,
            ServerId crashedMasterId,
            const ProtoBuf::Tablets& partitions,
            uint64_t minOpenSegmentId)
    : backupHost(backupHost)
    , crashedMasterId(crashedMasterId)
    , partitions(partitions)
    , minOpenSegmentId(minOpenSegmentId)
    , client()
    , rpc()
    , result()
    , done()
{
}

/// Asynchronously send the startReadingData RPC to #backupHost.
void
BackupStartTask::send()
{
    client.construct(
        Context::get().transportManager->getSession(
            backupHost.serviceLocator.c_str()));
    rpc.construct(*client, crashedMasterId, partitions);
    RAMCLOUD_LOG(DEBUG, "Starting startReadingData on %s",
                 backupHost.serviceLocator.c_str());
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
               idAndLength.first, backupHost.serverId.getId(),
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
            backupHost.serverId.getId(), result.logDigestSegmentId,
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
        result = (*rpc)();
    } catch (const TransportException& e) {
        LOG(WARNING, "Couldn't contact %s, failure was: %s",
            backupHost.serviceLocator.c_str(), e.str().c_str());
        // Leave empty result as if the backup has no replicas.
    } catch (const ClientException& e) {
        LOG(WARNING, "startReadingData failed on %s, failure was: %s",
            backupHost.serviceLocator.c_str(), e.str().c_str());
        // Leave empty result as if the backup has no replicas.
    }
    rpc.destroy();
    client.destroy();

    filterOutInvalidReplicas();

    done = true;
    LOG(DEBUG, "Backup %lu has %lu segment replicas",
        backupHost.serverId.getId(),
        result.segmentIdAndLength.size());
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

class SegmentAndDigestTuple {
  public:
    SegmentAndDigestTuple(const char* backupServiceLocator,
                          uint64_t segmentId, uint32_t segmentLength,
                          const void* logDigestPtr, uint32_t logDigestBytes)
        : backupServiceLocator(backupServiceLocator),
          segmentId(segmentId),
          segmentLength(segmentLength),
          logDigest(logDigestPtr, logDigestBytes)
    {
    }

    const char* backupServiceLocator;
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

    const uint32_t numBackups = serverList.backupCount();
    const uint32_t maxActiveBackupHosts = 10;

    /// List of asynchronous startReadingData tasks and their replies
    auto backupStartTasks = std::unique_ptr<Tub<BackupStartTask>[]>(
            new Tub<BackupStartTask>[numBackups]);
    uint32_t nextIssueIndex = 0;
    uint64_t minOpenSegmentId = 0;
    try {
        minOpenSegmentId = serverList[crashedServerId].minOpenSegmentId;
    } catch (const Exception& e) {
        LOG(ERROR, "Couldn't determine minOpenSegmentId while recovering "
            "master %lu; recovery could use invalid replicas",
            crashedServerId.getId());
        // This is handled essentially so unit tests succeed even when
        // the server being recovered is not in the CoordinatorServerList.
        // It is a risk to continue in this case.
    }
    for (uint32_t i = 0; i < numBackups; ++i) {
        nextIssueIndex = serverList.nextBackupIndex(nextIssueIndex);
        backupStartTasks[i].construct(*serverList[nextIssueIndex],
                                      crashedServerId, will, minOpenSegmentId);
        ++nextIssueIndex;
    }
    parallelRun(backupStartTasks.get(), numBackups, maxActiveBackupHosts);

    // Map of Segment Ids -> counts of backup copies that were found.
    // This is used to cross-check with the log digest.
    std::unordered_map<uint64_t, uint32_t> segmentMap;
    for (uint32_t i = 0; i < numBackups; ++i) {
        const auto& task = backupStartTasks[i];
        foreach (auto replica, task->result.segmentIdAndLength) {
            uint64_t segmentId = replica.first;
            ++segmentMap[segmentId];
        }
    }

    // List of serialised LogDigests from possible log heads, including
    // the corresponding Segment IDs and lengths.
    vector<SegmentAndDigestTuple> digestList;
    for (uint32_t i = 0; i < numBackups; ++i) {
        const auto& task = backupStartTasks[i];

        // If this backup returned a potential head of the log and it
        // includes a LogDigest, set it aside for verifyCompleteLog().
        if (task->result.logDigestBuffer) {
            digestList.push_back({ task->backupHost.serviceLocator.c_str(),
                                   task->result.logDigestSegmentId,
                                   task->result.logDigestSegmentLen,
                                   task->result.logDigestBuffer.get(),
                                   task->result.logDigestBytes });

            if (task->result.logDigestSegmentId == (uint32_t)-1) {
                LOG(ERROR, "segment %lu has a LogDigest, but len "
                    "== -1!! from %s\n", task->result.logDigestSegmentId,
                    task->backupHost.serviceLocator.c_str());
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

        LOG(NOTICE, "Segment %lu of length %u bytes from backup %s "
            "is the head of the log",
            headId, headLen, headReplica->backupServiceLocator);

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
    for (uint32_t nextBackupIndex = 0, taskIndex = 0;
         taskIndex < numBackups;
         nextBackupIndex++, taskIndex++) {
        nextBackupIndex = serverList.nextBackupIndex(nextBackupIndex);
        assert(nextBackupIndex != (uint32_t)-1);

        const auto& task = backupStartTasks[taskIndex];
        auto backup = *serverList[nextBackupIndex];
        const uint64_t speed = backup.expectedReadMBytesPerSec;

        LOG(DEBUG, "Adding %lu segment replicas from %s "
                   "with bench speed of %lu",
            task->result.segmentIdAndLength.size(),
            backup.serviceLocator.c_str(), speed);

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
                ReplicaAndLoadTime r {{ backup.serverId.getId(), segmentId },
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
                    "segment ID %lu, len %u from backup %s "
                    "because it's %s the head segment (%lu, %u)",
                    segmentId, segmentLen,
                    backup.serviceLocator.c_str(),
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
                    const CoordinatorServerList::Entry& masterEntry,
                    uint32_t partitionId,
                    const vector<RecoverRpc::Replica>& replicaMap)
        : recovery(recovery)
        , masterEntry(masterEntry)
        , replicaMap(replicaMap)
        , partitionId(partitionId)
        , tablets()
        , masterClient()
        , rpc()
        , done(false)
    {}
    bool isReady() { return rpc && rpc->isReady(); }
    bool isDone() { return done; }
    void send() {
        auto locator = masterEntry.serviceLocator.c_str();
        LOG(NOTICE, "Initiating recovery with recovery master %s, "
            "partition %d", locator, partitionId);
        try {
            masterClient.construct(
                Context::get().transportManager->getSession(locator));
            rpc.construct(*masterClient,
                          recovery.recoveryId,
                          recovery.crashedServerId, partitionId,
                          tablets,
                          replicaMap.data(),
                          downCast<uint32_t>(replicaMap.size()));
        } catch (const TransportException& e) {
            LOG(WARNING, "Couldn't contact %s, trying next master; "
                "failure was: %s", locator, e.message.c_str());
            send();
        } catch (const ClientException& e) {
            LOG(WARNING, "recover failed on %s, trying next master; "
                "failure was: %s", locator, e.toString());
            send();
        }
    }
    void wait() {
        try {
            (*rpc)();
        } catch (const TransportException& e) {
            auto locator = masterEntry.serviceLocator.c_str();
            LOG(WARNING, "Couldn't contact %s, trying next master; "
                "failure was: %s", locator, e.message.c_str());
            send();
            return;
        } catch (const ClientException& e) {
            auto locator = masterEntry.serviceLocator.c_str();
            LOG(WARNING, "recover failed on %s, trying next master; "
                "failure was: %s", locator, e.toString());
            send();
            return;
        }
        ++recovery.startedRecoveryMasters;
        done = true;
    }

    /// The parent recovery object.
    Recovery& recovery;

    /// The master server to kick off this partition's recovery on.
    const CoordinatorServerList::Entry masterEntry;

    const vector<RecoverRpc::Replica>& replicaMap;
    const uint32_t partitionId;
    ProtoBuf::Tablets tablets;
    Tub<MasterClient> masterClient;
    Tub<MasterClient::Recover> rpc;
    bool done;
    DISALLOW_COPY_AND_ASSIGN(MasterStartTask);
};
}
using namespace RecoveryInternal; // NOLINT

/**
 * Begin recovery, recovering one partition on a master.
 *
 * \bug Only tries each master once regardless of failure.
 * \bug Only tries each master once regardless of whether recovery
 *      failure could be avoided by doing multiple parition recoveries
 *      on a single master.
 */
void
Recovery::startRecoveryMasters()
{
    CycleCounter<RawMetric> _(&metrics->coordinator.recoveryStartTicks);

    // Figure out the number of partitions specified in the will.
    uint32_t numPartitions = 0;
    foreach (auto& tablet, will.tablet()) {
        if (tablet.user_data() + 1 > numPartitions)
            numPartitions = downCast<uint32_t>(tablet.user_data()) + 1;
    }

    if (serverList.masterCount() < numPartitions) {
        // TODO(ongaro): this is not ok in a real system
        DIE("not enough recovery masters to complete recovery "
            "(only %d available)", serverList.masterCount());
    }

    // Set up the tasks to execute the RPCs.
    uint32_t nextMasterIndex = 0;
    Tub<MasterStartTask> recoverTasks[numPartitions];
    for (uint32_t i = 0; i < numPartitions; i++) {
        auto& task = recoverTasks[i];
        nextMasterIndex = serverList.nextMasterIndex(nextMasterIndex);
        task.construct(*this, *serverList[nextMasterIndex],
                       i, replicaMap);
        nextMasterIndex++;
    }
    foreach (auto& tablet, will.tablet()) {
        auto& task = recoverTasks[tablet.user_data()];
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
 * \param successful
 *      True if the recovery master successfully recovered its
 *      partition of the crashed master and is ready to take
 *      ownership of those tablets. Othwerwise, false (if the
 *      recovery master couldn't recover its partition or the
 *      recovery master failed before completing recovery.
 */
void
Recovery::recoveryMasterFinished(bool successful)
{
    if (successful)
        ++successfulRecoveryMasters;
    else
        ++unsuccessfulRecoveryMasters;

    const uint32_t completedRecoveryMasters =
        successfulRecoveryMasters + unsuccessfulRecoveryMasters;
    if (completedRecoveryMasters == startedRecoveryMasters) {
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
    BackupEndTask(const string& serviceLocator, ServerId crashedServerId)
        : crashedServerId(crashedServerId)
        , serviceLocator(serviceLocator)
        , client()
        , rpc()
        , done(false)
    {}
    bool isReady() { return rpc && rpc->isReady(); }
    bool isDone() { return done; }
    void send() {
        auto locator = serviceLocator.c_str();
        try {
            client.construct(
                Context::get().transportManager->getSession(locator));
            rpc.construct(*client, crashedServerId);
            return;
        } catch (const TransportException& e) {
            LOG(WARNING, "Couldn't contact %s, ignoring; "
                "failure was: %s", locator, e.message.c_str());
        } catch (const ClientException& e) {
            LOG(WARNING, "recoveryComplete failed on %s, ignoring; "
                "failure was: %s", locator, e.toString());
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
            auto locator = serviceLocator.c_str();
            LOG(WARNING, "Couldn't contact %s, ignoring; "
                "failure was: %s", locator, e.message.c_str());
        } catch (const ClientException& e) {
            auto locator = serviceLocator.c_str();
            LOG(WARNING, "recoveryComplete failed on %s, ignoring; "
                "failure was: %s", locator, e.toString());
        }
        done = true;
    }
    const ServerId crashedServerId;
    const string& serviceLocator; // NOLINT
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
    uint32_t numBackups = serverList.backupCount();
    Tub<BackupEndTask> tasks[numBackups];
    for (size_t i = 0, taskNum = 0; i < serverList.size(); i++) {
        if (!serverList[i] || !serverList[i]->isBackup())
            continue;
        auto& backup = *serverList[i];
        tasks[taskNum++].construct(backup.serviceLocator, crashedServerId);
    }
    parallelRun(tasks, numBackups, 10);
}

} // namespace RAMCloud
