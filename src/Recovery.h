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


#ifndef RAMCLOUD_RECOVERY_H
#define RAMCLOUD_RECOVERY_H

#include "Common.h"
#include "BackupClient.h"
#include "CoordinatorServerList.h"
#include "CycleCounter.h"
#include "LogDigest.h"
#include "ParallelRun.h"
#include "RawMetrics.h"
#include "ServerTracker.h"
#include "TableManager.h"
#include "RecoveryPartition.pb.h"
#include "TaskQueue.h"
#include "TableStats.h"

namespace RAMCloud {

class Recovery;
typedef ServerTracker<Recovery> RecoveryTracker;
class Tablet;

namespace RecoveryInternal {
/**
 * AsynchronousTaskConcept which contacts a backup, informs it
 * that it should load replicas for segments belonging to the
 * crashed master, and gathers any log digest and list of replicas the
 * backup had for the crashed master. This should be followed by a
 * BackupStartPartitionTask to finish a recovery.
 * Only used in Recovery::buildReplicaMap().
 */
class BackupStartTask {
  PUBLIC:
    BackupStartTask(Recovery* recovery,
                    ServerId backupId);
    bool isDone() const { return done; }
    bool isReady() { return testingCallback || (rpc && rpc->isReady()); }
    void send();
    void filterOutInvalidReplicas();
    void wait();
    const ServerId backupId;
    StartReadingDataRpc::Result result;

  PRIVATE:
    Recovery* recovery;
    Tub<StartReadingDataRpc> rpc;
    bool done;

  PUBLIC:
    struct TestingCallback {
        virtual void backupStartTaskSend(
                        StartReadingDataRpc::Result& result) {}
        virtual ~TestingCallback() {}
    };
    TestingCallback* testingCallback;
    DISALLOW_COPY_AND_ASSIGN(BackupStartTask);
};

/**
 * AsynchronousTaskConcept which contacts a backup, informs it
 * that it should partition replicas for segments belonging to the
 * crashed master that have been read since from BackupStartTask.
 * Only used in Recovery::buildReplicaMap().
 */
class BackupStartPartitionTask {
  PUBLIC:
    BackupStartPartitionTask(Recovery* recovery, ServerId backupServerId);
    bool isReady() { return rpc && rpc->isReady(); }
    bool isDone() const { return done; }
    void send();
    void wait();


  PRIVATE:
    bool done;
    Tub<StartPartitioningRpc> rpc;
    const ServerId backupServerId;
    const Recovery* recovery;

    DISALLOW_COPY_AND_ASSIGN(BackupStartPartitionTask);
};

bool verifyLogComplete(Tub<BackupStartTask> tasks[],
                       size_t taskCount,
                       const LogDigest& digest);
Tub<std::tuple<uint64_t, LogDigest, TableStats::Digest*>>
findLogDigest(Tub<BackupStartTask> tasks[], size_t taskCount);
vector<WireFormat::Recover::Replica> buildReplicaMap(
    Tub<BackupStartTask> tasks[], size_t taskCount,
    RecoveryTracker* tracker, uint64_t headId);

struct MasterStartTask;
struct MasterStartTaskTestingCallback {
    virtual void masterStartTaskSend(uint64_t recoveryId,
        ServerId crashedServerId, uint32_t partitionId,
        const ProtoBuf::RecoveryPartition& dataToRecover,
        const WireFormat::Recover::Replica replicaMap[],
        size_t replicaMapSize) {}
    virtual ~MasterStartTaskTestingCallback() {}
};

struct BackupEndTask;
struct BackupEndTaskTestingCallback {
    virtual void backupEndTaskSend(ServerId backup, ServerId crashedServerId) {}
    virtual ~BackupEndTaskTestingCallback() {}
};
}

/**
 * Runs on the coordinator and attempts the recovery of a single crashed master.
 * Created and managed by the MasterRecoveryManager on the coordinator.
 */
class Recovery : public Task {
  public:
    /**
     * Internal to MasterRecoveryManager; describes what to do when the
     * recovery has completed. MasterRecoveryManager needs to do some
     * special cleanup after Recoveries, but Recoveries know best when
     * to destroy themselves. The default logic does nothing which is
     * useful for Recovery during unit testing.
     */
    struct Owner {
        virtual void recoveryFinished(Recovery* recovery) {}
        virtual ~Owner() {}
    };
    Recovery(Context* context,
             TaskQueue& taskQueue,
             TableManager* tableManager,
             RecoveryTracker* tracker,
             Owner* owner,
             ServerId crashedServerId,
             const ProtoBuf::MasterRecoveryInfo& recoveryInfo);
    ~Recovery();

    virtual void performTask();
    void recoveryMasterFinished(ServerId recoveryMasterId, bool successful);

    bool isDone() const;
    bool wasCompletelySuccessful() const;
    uint64_t getRecoveryId() const;

    /// Shared RAMCloud information.
    Context* context;

    /// The id of the crashed master which is being recovered.
    const ServerId crashedServerId;

    /**
     * Used to filter out replicas of segments which may have become
     * inconsistent. A replica with a segment id less than this or
     * an equal segment id but a lower epoch is not eligible to be used
     * for recovery (both for log digest and object data purposes).
     * This information comes from the coordinator server list at the
     * start of recovery and is originally provided by masters when
     * they lose contact with masters to which they are replicating
     * an open log segment.
     */
    const ProtoBuf::MasterRecoveryInfo masterRecoveryInfo;

    /// Defines max number of bytes a tablet partition should accommodate.
    static const uint64_t PARTITION_MAX_BYTES = 500*1024*1024;
    /// Defines the max number of records a tablet partition should accommodate.
    static const uint64_t PARTITION_MAX_RECORDS = 2000000;

  PRIVATE:
    void splitTablets(vector<Tablet> *tablets,
                      TableStats::Estimator* estimator);
    void partitionTablets(vector<Tablet> tablets,
                          TableStats::Estimator* estimator);
    void startBackups();
    void startRecoveryMasters();
    void broadcastRecoveryComplete();

    /**
     * Partitioning of tablets for the crashed master which describes how its
     * contents should be divided up among recovery masters in order to balance
     * recovery time across recovery masters.  It is represented as a
     * serialized tablet map with a partition id in the user_data field.
     * Partition ids must start at 0 and be consecutive. No partition id can
     * have 0 entries before any other partition that has more than 0 entries.
     * This is because the recovery recovers partitions up but excluding the
     * first with no entries.
     */
    ProtoBuf::RecoveryPartition dataToRecover;

    /**
     * Coordinator's authoritative information about tablets and their mapping
     * to servers. Used during to find out which tablets need to be recovered
     * for the crashed master.
     */
    TableManager* tableManager;

     /**
      * The MasterRecoveryManager's tracker which maintains a list of all
      * servers in RAMCloud along with a pointer to any Recovery the server is
      * particpating in (as a recovery master). This is used to select recovery
      * masters and to find all backup data for the crashed master.
      */
    RecoveryTracker* tracker;

    /**
     * Owning object, if any, which gets called back on certain events.
     * See #Owner.
     */
    Owner* owner;

    /**
     * A unique identifier associated with this recovery generated on
     * construction. Used to reassociate recovery related rpcs from recovery
     * masters to the recovery that they are part of.
     */
    uint64_t recoveryId;

    enum Status {
        START_RECOVERY_ON_BACKUPS,     ///< Contact all backups and find
                                       ///  replicas.
        START_RECOVERY_MASTERS,        ///< Choose and start recovery masters.
        WAIT_FOR_RECOVERY_MASTERS,     ///< Wait on recovery master completion.
        ALL_RECOVERY_MASTERS_FINISHED, ///< All recovery managers have either
                                       ///  succeeded or failed.
        DONE,
    };
    /**
     * What stage of master recovery this recovery is at. See #Status for
     * details about each status.
     */
    Status status;

    /**
     * Measures time between start and end of recovery on the coordinator.
     */
    Tub<CycleCounter<RawMetric>> recoveryTicks;

    /**
     * A mapping of segmentIds to backup host service locators.
     * Populated by buildReplicaMap().
     */
    vector<WireFormat::Recover::Replica> replicaMap;

    /**
     * Number of partitions in recovery (i.e. number of recovery
     * masters to use for recovery).
     * Recovery isDone() and moves to BROADCAST_RECOVERY_COMPLETE phase
     * when this is equal to the sum of successful and unsuccessful recovery
     * masters.
     */
    uint32_t numPartitions;

    /**
     * Number of recovery masters which have completed (as part of the
     * WAIT_FOR_RECOVERY_MASTERS phase) and successfully recovered
     * their partition of the tablets of the crashed master.
     */
    uint32_t successfulRecoveryMasters;

    /**
     * Number of recovery masters which either encountered some error
     * recovering their partition of the tablets under recovery or
     * which crashed without completing recovery.
     */
    uint32_t unsuccessfulRecoveryMasters;

  PUBLIC:
    /**
     * If non-NULL then this callback is invoked instead of
     * sending startReadingData RPCs to backups giving a chance
     * to mock out the call and results. Exact interface is provided
     * above.
     */
    RecoveryInternal::BackupStartTask::TestingCallback*
        testingBackupStartTaskSendCallback;

    /**
     * If non-NULL then this callback is invoked instead of
     * sending recover RPCs to recovery masters giving a chance
     * to mock out the call and results. Exact interface is provided
     * above.
     */
    RecoveryInternal::MasterStartTaskTestingCallback*
        testingMasterStartTaskSendCallback;

    /**
     * If non-NULL then this callback is invoked instead of
     * sending recoveryComplete RPCs to backup giving a chance
     * to mock out the call. Exact interface is provided
     * above.
     */
    RecoveryInternal::BackupEndTaskTestingCallback*
        testingBackupEndTaskSendCallback;

    /**
     * For testing; send a magical partition id to this many of
     * the recovery masters which will cause them to explode.
     */
    uint32_t testingFailRecoveryMasters;

    friend class RecoveryInternal::BackupStartTask;
    friend class RecoveryInternal::BackupStartPartitionTask;
    friend class RecoveryInternal::MasterStartTask;
    friend class RecoveryInternal::BackupEndTask;
    DISALLOW_COPY_AND_ASSIGN(Recovery);
};

} // namespace RAMCloud

#endif
