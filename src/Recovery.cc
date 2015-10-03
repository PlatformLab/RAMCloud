/* Copyright (c) 2010-2015 Stanford University
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
#include <algorithm>
#include <cmath>

#include "Recovery.h"
#include "BackupClient.h"
#include "Buffer.h"
#include "MasterClient.h"
#include "ParallelRun.h"
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
 * \param tableManager
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
 * \param recoveryInfo
 *      Used to filter out replicas of segments which may have become
 *      inconsistent. A replica with a segment id less than this or
 *      an equal segmentId with a lesser epoch is not eligible to be used
 *      for recovery (both for log digest and object data purposes).
 *      Stored in and provided by the coordinator server list.
 */
Recovery::Recovery(Context* context,
                   TaskQueue& taskQueue,
                   TableManager* tableManager,
                   RecoveryTracker* tracker,
                   Owner* owner,
                   ServerId crashedServerId,
                   const ProtoBuf::MasterRecoveryInfo& recoveryInfo)
    : Task(taskQueue)
    , context(context)
    , crashedServerId(crashedServerId)
    , masterRecoveryInfo(recoveryInfo)
    , dataToRecover()
    , tableManager(tableManager)
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
    // if the crashed master had no tablets, recovery is effectively done.
    // When this recovery gets scheduled, it will call back into
    // CoordinatorServerList to remove this server from the coordinator
    // server list.
    auto tablets = tableManager->markAllTabletsRecovering(crashedServerId);
    if (tablets.size() == 0) {
        status = DONE;
    }

}

Recovery::~Recovery()
{
}

/**
 * Splits tablets to ensure all tablets are less than the byte and record count
 * limits for a partition.  This will be called on the tablets to be recovered
 * before they are partitioned.  If a tablet is split, the resulting tablets
 * will be (best effort) the same "size" in both byte and record count.
 *
 * \param tablets
 *      Pointer to vector of tablets to be split.  Modified to reflect any
 *      tablet splits that occur.
 * \param estimator
 *      Pointer to tablet information estimator.  If estimator is NULL, the
 *      "split" operation will not be performed.  This may occur if for some
 *      reason the table stats digest information was not found in the head
 *      segment.
 */
void
Recovery::splitTablets(vector<Tablet> *tablets,
                       TableStats::Estimator* estimator)
{
    if (estimator == NULL || !estimator->valid) {
        return;
    }

    size_t size = tablets->size();
    for (size_t i = 0; i < size; ++i) {
        Tablet* tablet = &tablets->at(i);

        // The backing table for an indexlet should not be split.
        // This is because we want to recover an indexlet on a single server
        // rather than splitting it up into smaller partitions sprayed across
        // the cluster for better scalability of index range queries.
        if (tableManager->isIndexletTable(tablet->tableId))
            continue;

        TableStats::Estimator::Estimate stats = estimator->estimate(tablet);

        uint64_t startKeyHash = tablet->startKeyHash;
        uint64_t endKeyHash = tablet->endKeyHash;

        // Decide number of resulting tablets.
        // Both byteTCount and recordTCount should be << 2^64 - 1.  As such,
        // byteTCount, recordTCount, tabletCount will all also be << 2^64 - 1.
        // For this reason, we do not code for the case in which these values
        // will overflow.
        uint64_t byteTCount =
                (stats.byteCount + Recovery::PARTITION_MAX_BYTES - 1)
                / Recovery::PARTITION_MAX_BYTES;
        uint64_t recordTCount =
                (stats.recordCount + Recovery::PARTITION_MAX_RECORDS - 1)
                / Recovery::PARTITION_MAX_RECORDS;
        uint64_t tabletCount = std::max(byteTCount, recordTCount);

        // The number of splits should be one less than the number of resulting
        // tablets but must also be less than then number of keys in the
        // since the smallest tablet size has exactly one key. The "if" is
        // needed so that tabletCount - 1 does not underflow.
        uint64_t splits = 0;
        if (tabletCount > 0) {
             splits = std::min(endKeyHash - startKeyHash, tabletCount - 1);
        }
        // Recompute tabletCount in case it changed.
        tabletCount = splits + 1;

        if (tabletCount > 1) {
            // Since the full key range is not always a multiple of the number
            // of desired tablets, some tablets may be 1 key range larger than
            // others.  We determine the number of "big" tablets needed by
            // using a simple mod.

            // Note that (endKeyHash - startKeyHash) is not inclusive when it
            // should be.  This will be accounted for in the calculation of
            // bigCount.  Also note that keyRange can only be zero when
            // (endKeyHash - startKeyHash)  = tabletCount - 1.  In that case,
            // the calulation of bigCount will be equal to tabletCount and thus
            // keyRangeBig will be used exclusively.
            uint64_t keyRange = (endKeyHash - startKeyHash) / tabletCount;
            uint64_t keyRangeBig = keyRange + 1;

            // This is the number of "big" tablets we will need.  We add 1 after
            // the mod to account for the fact that the key range should be
            // inclusive but incrementing (endKeyHash - startKeyHash) before
            // the operation would risk overflow.  Note that if the inclusive
            // key range (endKeyHash - startKeyHash + 1) is actually a multiple
            // of tabletCount, bigCount would equal tabletCount thus accounting
            // for the off-by-one cacluation performed for keyRange.
            uint64_t bigCount = ((endKeyHash - startKeyHash) % tabletCount) + 1;

            // Update first tablet in place.  There is always at least one "big"
            // tablet so we can use keyRangeBig here.
            tablet->endKeyHash = startKeyHash + (keyRangeBig - 1);
            startKeyHash += keyRangeBig;
            uint64_t tabletsComplete = 1;

            // Create all "big" tablets.
            for (; tabletsComplete < bigCount; tabletsComplete++) {
                Tablet temp = tablets->at(i);
                temp.startKeyHash = startKeyHash;
                temp.endKeyHash = startKeyHash + (keyRangeBig - 1);
                tableManager->splitRecoveringTablet(temp.tableId, startKeyHash);
                tablets->push_back(temp);
                startKeyHash += keyRangeBig;
            }

            // Create all remaining tablets.
            for (; tabletsComplete < tabletCount; tabletsComplete++) {
                Tablet temp = tablets->at(i);
                temp.startKeyHash = startKeyHash;
                temp.endKeyHash = startKeyHash + (keyRange - 1);
                tableManager->splitRecoveringTablet(temp.tableId, startKeyHash);
                tablets->push_back(temp);
                startKeyHash += keyRange;
            }
        }
    }
}

/// Anonymous namespace hiding structures for use in partitionTablets.
namespace {
/**
 * Repesents a collection of tablets (a single recovery partition). This
 * structure is used internal to the partitionTablets method and is not meant
 * for use outside the method. This representation of a partition only keeps
 * track of the cumulative byteCount and recordCount of tablets assigned to it.
 */
struct Partition {
    uint64_t partitionId;    //< Id used to differentiate tablet partitions
    uint64_t byteCount;      //< Number of bytes assigned to this partition.
    uint64_t recordCount;    //< Number of records assigned to this partition.

    /**
     * Constructs a new partition with partitionId.
     */
    explicit Partition(uint64_t partitionId)
        : partitionId(partitionId)
        , byteCount(0)
        , recordCount(0)
    {}

    /**
     * Returns a number between scaled between 0.0 and 1.0 where 1.0 means the
     * partition is completely full. The fullness is based on both the number of
     * assigned bytes as well as the number of assigned records.
     */
    double usage() {
        double byte2 = (double(byteCount) * double(byteCount))
                       / (double(Recovery::PARTITION_MAX_BYTES)
                          * double(Recovery::PARTITION_MAX_BYTES));
        double record2 = (double(recordCount) * double(recordCount))
                         / (double(Recovery::PARTITION_MAX_RECORDS)
                            * double(Recovery::PARTITION_MAX_RECORDS));
        return sqrt(byte2 + record2) / sqrt(2);
    }

    /**
     * Given a tablet's estimator entry, returns true if the tablet would fit
     * in the partition and false otherwise.
     *
     * \param estimate
     *      Contains the tablet's estatmated stats information that is used to
     *      determine if said tablet would fit in the partition.
     */
    bool fits(TableStats::Estimator::Estimate estimate) {
        if ((byteCount + estimate.byteCount) > Recovery::PARTITION_MAX_BYTES)
            return false;
        if ((recordCount + estimate.recordCount) >
                Recovery::PARTITION_MAX_RECORDS)
            return false;
        return true;
    }

    /**
     * Assigns the tablet whose estimator entry is provided to the partition.
     *
     * \param estimate
     *      Contains the estatmated stats information of the tablet that is to
     *      be assigned.
     */
    void add(TableStats::Estimator::Estimate estimate) {
        byteCount += estimate.byteCount;
        recordCount += estimate.recordCount;
    }
};
}

/**
 * Divides the tablets belonging to a master into partitions, where the number
 * of bytes and number of records in each partition is limited (to ensure fast
 * crash recovery) and there are as few partitions as possible.
 *
 * Partitions are set by serializing the tablet entry into dataToRecover and
 * setting partitionId in the entry's "user_data".
 *
 * \param tablets
 *      Vector of tablets to be partitioned for recovery.
 * \param estimator
 *      Pointer to tablet information estimator.  If estimator is not valid,
 *      we will naively place one tablet per partition.
 */
void
Recovery::partitionTablets(vector<Tablet> tablets,
                           TableStats::Estimator* estimator)
{
    numPartitions = 0;

    // If no usable estimator is available, this method will perform a naive
    // partition where each tablet will be placed in its own partition.  This
    // may occur if for some reason the table stats digest information is not
    // found in the head segment and thus no estimator could be generated.
    // While this should never happend during normal operations, we would like
    // recovery to still continue in this error case (albeit in a degrade mode).
    if (estimator == NULL || !estimator->valid) {
        LOG(ERROR,
            "No valid TableStats Estimator; using naive partitioning.");
        foreach (auto& tablet, tablets) {
            ProtoBuf::Tablets::Tablet& entry = *dataToRecover.add_tablet();
            tablet.serialize(entry);
            entry.set_user_data(numPartitions++);
        }
        return;
    }

    splitTablets(&tablets, estimator);

    // An "open" partition has been partially assigned to but is not full.
    std::vector<Partition> openPartitions;
    foreach (Tablet& tablet, tablets) {
        bool done = false;
        size_t numOpenPartitions = openPartitions.size();
        // From a few ramdomly selected open partitions, assign the tablet to
        // the first open partition that fits.  If none of these partitions fit,
        // create a new partition.
        for (size_t i = 0; i < std::min(numOpenPartitions, 5lu); i++) {
            size_t j = generateRandom() % numOpenPartitions;
            Partition& partition = openPartitions[j];
            if (partition.fits(estimator->estimate(&tablet))) {
                partition.add(estimator->estimate(&tablet));
                ProtoBuf::Tablets::Tablet& entry =
                                            *dataToRecover.add_tablet();
                tablet.serialize(entry);
                entry.set_user_data(partition.partitionId);
                // If the partition is mostly full, remove the partition
                if (partition.usage() > 0.9) {
                    partition = openPartitions.back();
                    openPartitions.pop_back();
                }
                done = true;
                break;
            }
        }
        if (!done) {
            // This tablet did not fit in any of the open partitions we tried,
            // so make a new partition.
            Partition partition(numPartitions++);
            partition.add(estimator->estimate(&tablet));
            ProtoBuf::Tablets::Tablet& entry = *dataToRecover.add_tablet();
            tablet.serialize(entry);
            entry.set_user_data(partition.partitionId);
            // If the partition still has room for more tablets, add it to the
            // available set of partitions.
            if (partition.usage() <= 0.9) {
                openPartitions.push_back(partition);
            }
        }
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
    // set to DONE if there were no tablets in the crashed master when the
    // constructor for this Recovery object was invoked
    if (status == DONE) {
        LOG(NOTICE, "No tablets in crashed server %s, removing it from"
                    " coordinator server list",
                     crashedServerId.toString().c_str());
        if (owner) {
            owner->recoveryFinished(this);
            owner->destroyAndFreeRecovery(this);
        }
        return;
    }

    metrics->coordinator.recoveryCount++;
    switch (status) {
    case START_RECOVERY_ON_BACKUPS:
        LOG(NOTICE, "Starting recovery %lu for crashed server %s",
            recoveryId, crashedServerId.toString().c_str());
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
BackupStartTask::BackupStartTask(Recovery* recovery,
                                 ServerId backupId)
    : backupId(backupId)
    , result()
    , recovery(recovery)
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
    LOG(DEBUG, "Starting startReadingData on backup %s",
        backupId.toString().c_str());
    if (!testingCallback) {
        rpc.construct(recovery->context, backupId, recovery->recoveryId,
                      recovery->crashedServerId);
    } else {
        testingCallback->backupStartTaskSend(result);
    }
}

/**
 * Removes replicas and log digests from results that may be inconsistent with
 * the most recent state of the log being recovered.
 *
 * When masters lose contact with backups they were replicating an open segment
 * to that replica may become inconsistent. To remedy this masters store a bit
 * of recovery metadata in the server list on the coordinator called
 * "MasterRecoveryInfo".
 *
 * This method uses that metadata which provides a segment id and an "epoch"
 * for that segment. Any *open* replica found on backups with either a)
 * a segment id that is less that that stored in the recovery info or b)
 * a segment id that is equal to that stored in the recovery info AND an
 * epoch less than that stored in the recovery info will be filtered out of
 * the results coming back from the backup since the replica could have become
 * inconsistent. This includes filtering out any log digests that could have
 * come from inconsistent replicas as well.
 */
void
BackupStartTask::filterOutInvalidReplicas()
{
    uint64_t minOpenSegmentId =
            recovery->masterRecoveryInfo.min_open_segment_id();
    uint64_t minOpenSegmentEpoch =
            recovery->masterRecoveryInfo.min_open_segment_epoch();

    // Remove any replicas from the results that are invalid because they
    // were found open and they are from a segment that was later closed or
    // was re-replicated with the later epoch number.
    vector<StartReadingDataRpc::Replica> newReplicas;
    newReplicas.reserve(result.replicas.size());
    uint32_t newPrimaryReplicaCount = 0;
    for (size_t i = 0; i < result.replicas.size(); ++i) {
        auto& replica = result.replicas[i];
        if (!replica.closed) {
            if (replica.segmentId < minOpenSegmentId ||
                (replica.segmentId == minOpenSegmentId &&
                 replica.segmentEpoch < minOpenSegmentEpoch))
            {
                LOG(DEBUG, "Removing replica for segmentId %lu from replica "
                    "list for backup %s because it was open and had "
                    "<id, epoch> <%lu ,%lu> which is less than the required "
                    "<id, epoch> <%lu ,%lu> for the recovering master",
                    replica.segmentId, backupId.toString().c_str(),
                    replica.segmentId, replica.segmentEpoch,
                    minOpenSegmentId, minOpenSegmentEpoch);
                continue;
            }
        }
        if (i < result.primaryReplicaCount)
            ++newPrimaryReplicaCount;
        newReplicas.emplace_back(replica);
    }
    std::swap(result.replicas, newReplicas);
    std::swap(result.primaryReplicaCount, newPrimaryReplicaCount);
    // We cannot use a log digest if it comes from a segment which was filtered
    // out as invalid.
    if (result.logDigestSegmentId < minOpenSegmentId ||
        (result.logDigestSegmentId == minOpenSegmentId &&
         result.logDigestSegmentEpoch < minOpenSegmentEpoch))
    {
        LOG(DEBUG, "Backup %s returned a log digest for segment id/epoch "
            "<%lu, %lu> but the minimum <id, epoch> for this master is "
            "<%lu, %lu> so discarding it", backupId.toString().c_str(),
            result.logDigestSegmentId, result.logDigestSegmentEpoch,
            minOpenSegmentId, minOpenSegmentEpoch);
        result.logDigestBytes = 0;
        result.logDigestBuffer.reset();
        result.logDigestSegmentId = -1;
        result.logDigestSegmentEpoch = -1;
    }
}

void
BackupStartTask::wait()
{
    try {
        if (!testingCallback)
            result = rpc->wait();
    } catch (const ServerNotUpException& e) {
        LOG(WARNING, "Couldn't contact %s; server no longer in server list",
            backupId.toString().c_str());
        // Leave empty result as if the backup has no replicas.
    } catch (const ClientException& e) {
        LOG(WARNING, "startReadingData failed on %s, failure was: %s",
            backupId.toString().c_str(), e.str().c_str());
        // Leave empty result as if the backup has no replicas.
    }
    rpc.destroy();

    filterOutInvalidReplicas();

    done = true;
    LOG(DEBUG, "Backup %s has %lu segment replicas",
        backupId.toString().c_str(), result.replicas.size());
}

BackupStartPartitionTask::BackupStartPartitionTask(Recovery* recovery,
                                                   ServerId backupServerId)
        : done()
        , rpc()
        , backupServerId(backupServerId)
        , recovery(recovery)
{
}

/// Asynchronously send the startPartitioningReplicas RPC to #backupHost.
void
BackupStartPartitionTask::send()
{
    LOG(DEBUG, "Sending StartPartitioning: %s",
        backupServerId.toString().c_str());
    rpc.construct(recovery->context, backupServerId, recovery->recoveryId,
                recovery->crashedServerId, &(recovery->dataToRecover));
}

void
BackupStartPartitionTask::wait()
{
    try {
        rpc->wait();
        LOG(DEBUG, "Backup %s started partitioning replicas",
                        backupServerId.toString().c_str());
    } catch (const ServerNotUpException& e) {
        LOG(WARNING, "Couldn't contact %s; server no longer in server list",
            backupServerId.toString().c_str());
    } catch (const ClientException& e) {
        LOG(WARNING, "startPartition failed on %s, failure was: %s",
            backupServerId.toString().c_str(), e.str().c_str());
    }
    rpc.destroy();

    done = true;
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
        foreach (auto replica, tasks[i]->result.replicas)
            replicaSet.insert(replica.segmentId);
    }

    uint32_t missing = 0;
    for (uint32_t i = 0; i < digest.size(); i++) {
        uint64_t id = digest[i];
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
 * Extract log digest and table stats from all the startReadingData results.
 * If multiple log digests are found the one from the replica with the
 * lowest segment id is used. When there are multiple replicas for an open
 * segment the first one that is encountered is returned; it make no
 * difference whatsoever: all of the replicas must have identical log
 * digests by construction. Keep in mind inconsistent open replicas (ones which
 * are missing writes that were acknowledged to applications) won't
 * be considered due to min open segment id/epoch filtering, see
 * BackupStartTask::filterOutInvalidReplicas()).
 *
 * \param tasks
 *      Already run tasks holding the results of startReadingData calls
 *      to all of the available backups.
 * \param taskCount
 *      Number of elements in #tasks.
 * \return
 *      Triple of segment id of the replica from which the log digest was
 *      taken, the log digest itself, and a pointer to the table stats buffer.
 *      Empty if no log digest is found.
 */
Tub<std::tuple<uint64_t, LogDigest, TableStats::Digest*>>
findLogDigest(Tub<BackupStartTask> tasks[], size_t taskCount)
{
    uint64_t headId = ~0ul;
    void* headBuffer = NULL;
    uint32_t headBufferLength = 0;
    TableStats::Digest* tableStatsBuffer = NULL;

    for (size_t i = 0; i < taskCount; ++i) {
        const auto& result = tasks[i]->result;
        if (!result.logDigestBuffer)
            continue;
        if (result.logDigestSegmentId < headId) {
            headBuffer = result.logDigestBuffer.get();
            headBufferLength = result.logDigestBytes;
            headId = result.logDigestSegmentId;
            if (result.tableStatsBytes >= sizeof(TableStats::Digest)) {
                // The buffer is returned as a pointer into an object inside
                // "tasks".  This task object should live throughout the scope
                // of this method's caller.
                tableStatsBuffer = reinterpret_cast<TableStats::Digest*>
                                            (result.tableStatsBuffer.get());
            }
        }
    }

    if (headId == ~(0lu))
        return {};
    return {std::make_tuple(headId,
                            LogDigest(headBuffer, headBufferLength),
                            tableStatsBuffer)};
}

/// Used in buildReplicaMap().
struct ReplicaAndLoadTime {
    WireFormat::Recover::Replica replica;
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
 *  \return
 *      Script which indicates to recovery masters which replicas are on which
 *      backups and (approximately) what order segments should be replayed in.
 *      Sent to all recovery masters verbatim.
 */
vector<WireFormat::Recover::Replica>
buildReplicaMap(Tub<BackupStartTask> tasks[],
                size_t taskCount,
                RecoveryTracker* tracker,
                uint64_t headId)
{
    vector<ReplicaAndLoadTime> replicasToSort;
    for (uint32_t taskIndex = 0; taskIndex < taskCount; taskIndex++) {
        const auto& task = tasks[taskIndex];
        const auto backupId = task->backupId;
        const uint64_t speed = (*tracker).getServerDetails(backupId)->
                                                    expectedReadMBytesPerSec;

        LOG(DEBUG, "Adding %lu segment replicas from %s "
                   "with bench speed of %lu",
            task->result.replicas.size(),
            backupId.toString().c_str(), speed);

        for (size_t i = 0; i < task->result.replicas.size(); ++i) {
            uint64_t expectedLoadTimeMs;
            if (i < task->result.primaryReplicaCount) {
                // for primaries just estimate when they'll load
                expectedLoadTimeMs = (i + 1) * 8 * 1000 / (speed ?: 1);
            } else {
                // for secondaries estimate when they'll load
                // but add a huge bias so secondaries don't overlap
                // with primaries but are still interleaved
                expectedLoadTimeMs =
                    ((i + 1) - task->result.primaryReplicaCount) *
                                     8 * 1000/ (speed ?: 1);
                expectedLoadTimeMs += 1000000;
            }
            const auto& replica = task->result.replicas[i];
            if (replica.segmentId <= headId) {
                ReplicaAndLoadTime r{{ backupId.getId(), replica.segmentId },
                                      expectedLoadTimeMs};
                replicasToSort.push_back(r);
            } else {
                // Getting here is not necessarily a sign of a problem.
                // Any replicas with higher ids will either
                // be empty or only contain data written async which is ok
                // to lose.
                LOG(DEBUG, "Ignoring replica for segment id %lu from backup %s "
                    "because it's past the head segment (%lu)",
                    replica.segmentId, backupId.toString().c_str(), headId);
            }
        }
    }
    std::sort(replicasToSort.begin(), replicasToSort.end());
    vector<WireFormat::Recover::Replica> replicaMap;
    foreach(const auto& sortedReplica, replicasToSort) {
        LOG(DEBUG, "Load segment %lu replica from backup %s "
            "with expected load time of %lu ms",
            sortedReplica.replica.segmentId,
            ServerId(sortedReplica.replica.backupId).toString().c_str(),
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

    auto tablets = tableManager->markAllTabletsRecovering(crashedServerId);

    if (tablets.size() == 0) {
        LOG(NOTICE, "Server %s crashed, but it had no tablets",
            crashedServerId.toString().c_str());
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
        tracker->getServersWithService(WireFormat::BACKUP_SERVICE);
    /// List of asynchronous startReadingData tasks and their replies
    auto backupStartTasks = std::unique_ptr<Tub<BackupStartTask>[]>(
            new Tub<BackupStartTask>[backups.size()]);
    auto backupPartitionTasks =
        std::unique_ptr<Tub<BackupStartPartitionTask>[]>(
            new Tub<BackupStartPartitionTask>[backups.size()]);
    uint32_t i = 0;
    foreach (ServerId backup, backups) {
        backupStartTasks[i].construct(this, backup);
        backupPartitionTasks[i].construct(this, backup);
        i++;
    }

    /* Broadcast 1: start reading replicas from disk and verify log integrity */
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
    uint64_t headId = std::get<0>(*digestInfo.get());
    LogDigest digest = std::get<1>(*digestInfo.get());
    // This tableStats digest is a pointer into an object inside
    // backupStartTasks.  Thus it is only valid as long as backupStartTasks
    // is live.  backupStartTasks is on this methods stack and is live for
    // the scope for this method.
    TableStats::Digest* tableStats = std::get<2>(*digestInfo.get());

    LOG(NOTICE, "Segment %lu is the head of the log", headId);

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

    /* Broadcast 2: partition replicas into tablets for recovery masters */
    TableStats::Estimator estimator(tableStats);
    partitionTablets(tablets, &estimator);
    LOG(NOTICE, "Partition Scheme for Recovery:\n%s",
                dataToRecover.DebugString().c_str());

    parallelRun(backupPartitionTasks.get(), backups.size(),
            maxActiveBackupHosts);

    replicaMap = buildReplicaMap(backupStartTasks.get(), backups.size(),
                                 tracker, headId);

    status = START_RECOVERY_MASTERS;
    schedule();
}

namespace RecoveryInternal {
/// Used in Recovery::startRecoveryMasters().
struct MasterStartTask {
    MasterStartTask(Recovery& recovery,
                    ServerId serverId,
                    uint32_t partitionId,
                    const vector<WireFormat::Recover::Replica>& replicaMap)
        : recovery(recovery)
        , serverId(serverId)
        , replicaMap(replicaMap)
        , partitionId(partitionId)
        , dataToRecover()
        , rpc()
        , done(false)
        , testingCallback(recovery.testingMasterStartTaskSendCallback)
    {}
    bool isReady() { return testingCallback || (rpc && rpc->isReady()); }
    bool isDone() { return done; }
    void send() {
        LOG(NOTICE, "Starting recovery %lu on recovery master %s, "
            "partition %d", recovery.recoveryId, serverId.toString().c_str(),
            partitionId);
        (*recovery.tracker)[serverId] = &recovery;
        if (!testingCallback) {
            rpc.construct(recovery.context,
                          serverId,
                          recovery.recoveryId,
                          recovery.crashedServerId,
                          recovery.testingFailRecoveryMasters > 0
                              ? ~0u : partitionId,
                          &dataToRecover,
                          replicaMap.data(),
                          downCast<uint32_t>(replicaMap.size()));
            if (recovery.testingFailRecoveryMasters > 0) {
                LOG(NOTICE, "Told recovery master %s to kill itself",
                    serverId.toString().c_str());
                --recovery.testingFailRecoveryMasters;
            }
        } else {
            testingCallback->masterStartTaskSend(recovery.recoveryId,
                                                 recovery.crashedServerId,
                                                 partitionId,
                                                 dataToRecover,
                                                 replicaMap.data(),
                                                 replicaMap.size());
        }
    }
    void wait() {
        try {
            if (!testingCallback)
                rpc->wait();
            done = true;
            return;
        } catch (const ServerNotUpException& e) {
            LOG(WARNING, "Couldn't contact server %s to start recovery: %s",
                serverId.toString().c_str(), e.what());
        } catch (const ClientException& e) {
            LOG(WARNING, "Couldn't contact server %s to start recovery: %s",
                serverId.toString().c_str(), e.what());
        }
        recovery.recoveryMasterFinished(serverId, false);
        done = true;
    }

    /// The parent recovery object.
    Recovery& recovery;

    /// Id of master server to kick off this partition's recovery on.
    ServerId serverId;

    const vector<WireFormat::Recover::Replica>& replicaMap;
    const uint32_t partitionId;
    ProtoBuf::RecoveryPartition dataToRecover;
    Tub<RecoverRpc> rpc;
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
    LOG(NOTICE, "Starting recovery %lu for crashed server %s with %u "
        "partitions", recoveryId, crashedServerId.toString().c_str(),
        numPartitions);

    // Set up the tasks to execute the RPCs.
    std::vector<ServerId> masters =
        tracker->getServersWithService(WireFormat::MASTER_SERVICE);
    std::random_shuffle(masters.begin(), masters.end(), randomNumberGenerator);
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
    foreach (auto& tablet, dataToRecover.tablet()) {
        auto& task = recoverTasks[tablet.user_data()];
        if (task) {
            *task->dataToRecover.add_tablet() = tablet;
            if (tableManager->isIndexletTable(tablet.table_id())) {
                ProtoBuf::Indexlet& entry =
                    *task->dataToRecover.add_indexlet();
                tableManager->getIndexletInfoByBackingTableId(
                                  tablet.table_id(), entry);
                LOG(NOTICE, "Starting recovery %lu for crashed server %s with "
                    "index %d", recoveryId, crashedServerId.toString().c_str(),
                    entry.index_id());
            }
        }
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
            LOG(NOTICE, "Recovery master %s failed to recover its partition "
                "of the will for crashed server %s",
                recoveryMasterId.toString().c_str(),
                crashedServerId.toString().c_str());
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
                "broadcast the end of recovery %lu for server %s to backups",
                recoveryId, crashedServerId.toString().c_str());
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
        rpc.construct(recovery.context, serverId, crashedServerId);
    }
    void wait() {
        if (!rpc)
            return;
        try {
            rpc->wait();
        } catch (const ServerNotUpException& e) {
            LOG(DEBUG, "recoveryComplete failed on %s, ignoring; "
                "server no longer in the servers list",
                serverId.toString().c_str());
        } catch (const ClientException& e) {
            LOG(DEBUG, "recoveryComplete failed on %s, ignoring; "
                "failure was: %s", serverId.toString().c_str(), e.what());
        }
        done = true;
    }
    Recovery& recovery;
    const ServerId serverId;
    const ServerId crashedServerId;
    Tub<RecoveryCompleteRpc> rpc;
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
    LOG(DEBUG, "Broadcasting the end of recovery %lu for server %s to backups",
        recoveryId, crashedServerId.toString().c_str());
    CycleCounter<RawMetric> ticks(&metrics->coordinator.recoveryCompleteTicks);
    std::vector<ServerId> backups =
        tracker->getServersWithService(WireFormat::BACKUP_SERVICE);
    Tub<BackupEndTask> tasks[backups.size()];
    size_t taskNum = 0;
    foreach (ServerId backup, backups)
        tasks[taskNum++].construct(*this, backup, crashedServerId);
    parallelRun(tasks, backups.size(), 10);
}

} // namespace RAMCloud
