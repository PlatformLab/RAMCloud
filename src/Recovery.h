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


#ifndef RAMCLOUD_RECOVERY_H
#define RAMCLOUD_RECOVERY_H

#include "Common.h"
#include "CoordinatorServerList.h"
#include "CycleCounter.h"
#include "Log.h"
#include "RawMetrics.h"
#include "ProtoBuf.h"
#include "ServerList.pb.h"
#include "Tablets.pb.h"
#include "TaskQueue.h"

namespace RAMCloud {

namespace RecoveryInternal {
struct MasterStartTask;
}

/**
 * Manages the recovery of a crashed master.
 */
class Recovery : public Task {
  public:
    /**
     * Internal to MasterRecoveryManager; describes what to do whenever
     * we don't want to go on living. MasterRecoveryManager needs to do some
     * special cleanup after Recoveries, but Recoveries know best when to
     * destroy themselves. The default logic does nothing which is useful for
     * Recovery during unit testing.
     */
    struct Deleter {
        virtual void destroyAndFreeRecovery(Recovery* recovery) {}
        virtual ~Deleter() {}
    };

    Recovery(TaskQueue& taskQueue,
             const CoordinatorServerList& serverList,
             Deleter& deleter,
             ServerId masterId,
             const ProtoBuf::Tablets& will);
    ~Recovery();

    virtual void performTask();
    void recoveryMasterCompleted(bool success);

    bool isDone();
    bool wasCompletelySuccessful();

    /// The id of the crashed master which is being recovered.
    const ServerId masterId;

    /**
     * A partitioning of tablets ('will') for the crashed master which
     * describes how its contents should be divided up among recovery
     * masters in order to balance recovery time across recovery masters.
     */
    const ProtoBuf::Tablets will;

  PRIVATE:
    void buildReplicaMap();
    void startRecoveryMasters();
    void broadcastRecoveryComplete();

    /// The list of all servers in the system.
    const CoordinatorServerList& serverList;

    /// Deletes this when this determines it is no longer needed. See #Deleter.
    Deleter& deleter;

    enum Status {
        BUILD_REPLICA_MAP,           ///< Contact all backups and find replicas.
        START_RECOVERY_MASTERS,      ///< Choose and start recovery masters.
        WAIT_FOR_RECOVERY_MASTERS,   ///< Wait on completion of recovery masters.
        BROADCAST_RECOVERY_COMPLETE, ///< Inform backups of end of recovery.
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
    vector<RecoverRpc::Replica> replicaLocations;

    /**
     * Number of recovery masters started during START_RECOVERY_MASTERS phase.
     * Recovery isDone() and moves to BROADCAST_RECOVERY_COMPLETE phase
     * when this is equal to the sum of successful and unsuccessful recovery
     * masters.
     */
    uint32_t startedRecoveryMasters;

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

    friend class RecoveryInternal::MasterStartTask;
    DISALLOW_COPY_AND_ASSIGN(Recovery);
};

} // namespace RAMCloud

#endif
