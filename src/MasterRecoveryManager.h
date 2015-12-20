/* Copyright (c) 2012-2015 Stanford University
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

#ifndef RAMCLOUD_MASTERRECOVERYMANAGER_H
#define RAMCLOUD_MASTERRECOVERYMANAGER_H

#include <thread>

#include "CoordinatorServerList.h"
#include "ProtoBuf.h"
#include "Recovery.h"
#include "RuntimeOptions.h"
#include "ServerTracker.h"
#include "TableManager.h"
#include "Tub.h"

namespace RAMCloud {

namespace MasterRecoveryManagerInternal {
class ApplyTrackerChangesTask;
class MaybeStartRecoveryTask;
class EnqueueMasterRecoveryTask;
class RecoveryMasterFinishedTask;
}

/**
 * Runs on the coordinator and manages the recovery of one or more crashed
 * masters. The coordinator enqueues servers for recovery and the manager
 * takes care of the rest.
 *
 * This manager ensures the recovery eventually completes successfully (or
 * continues retrying indefinitely). These recoveries proceed independently of
 * the main coordinator worker threads via a thread provided internally by the
 * manager. The manager must make pervasive use of the coordinator's serverList
 * and tabletMap which are synchronized to make operations safe. The
 * coordinator delegates the handling of recovery related RPCs to this manager.
 */
class MasterRecoveryManager : public Recovery::Owner
                            , public ServerTracker<Recovery>::Callback
{
  PUBLIC:
    MasterRecoveryManager(Context* context,
                          TableManager& tableManager,
                          RuntimeOptions* runtimeOptions);
    ~MasterRecoveryManager();

    void start();
    void halt();

    void startMasterRecovery(CoordinatorServerList::Entry crashedServer);
    bool recoveryMasterFinished(uint64_t recoveryId,
                                ServerId recoveryMasterId,
                                const ProtoBuf::RecoveryPartition&
                                      recoveryPartition,
                                bool successful);

    virtual void trackerChangesEnqueued();

    virtual void recoveryFinished(Recovery* recovery);

  PRIVATE:
    void main();

    /// Shared RAMCloud information.
    Context* context;

    /// Authoritative information about tablets and their mapping to servers.
    TableManager& tableManager;

    /**
     * Contains coordinator configuration options which can be modified while
     * the cluster is running. Currently mostly used for setting debugging
     * or testing parameters.
     */
    RuntimeOptions* runtimeOptions;

    /**
     * Drives recoveries; wakes up whenever new recoveries are waiting
     * or active recoveries have new work to complete.
     */
    Tub<std::thread> thread;

    /**
     * Recoveries the coordinator must complete in order to restore the
     * cluster to full working condition, but which haven't been started
     * yet.
     */
    std::queue<Recovery*> waitingRecoveries;

    typedef std::unordered_map<uint64_t, Recovery*> RecoveryMap;
    /**
     * Recoveries which are actively in progress in the cluster.  Maps recovery
     * ids to an ongoing recovery. Used to reassociate recovery masters which
     * finished recovery to the recovery that was recovering them.  The size of
     * this map is less than or equal to #maxActiveRecoveries at all times.
     */
    RecoveryMap activeRecoveries;

    /**
     * Maximum number of concurrent recoveries to attempt. Right now anything
     * higher than 1 will fail (likely with deadlock).
     */
    uint32_t maxActiveRecoveries;

    /**
     * Enqueues recoveries that are ready to take steps toward completion
     * and makes progress on enqueued recoveries whenever
     * taskQueue.performTask() is called.
     */
    TaskQueue taskQueue;

    /**
     * Maintains a list of all servers in RAMCloud along with a pointer to any
     * Recovery the server is particpating in (as a recovery master). This is
     * used by recoveries to select recovery masters and to find all backup
     * data for the crashed master. Changes to #serverList are pushed to
     * #tracker and applied asynchronously at an opportune time. See
     * trackerChangesEnqueued().
     */
    RecoveryTracker tracker;

    /**
     * Prevents startMasterRecovery() from actually starting recovery and,
     * instead, logs arguments to the call. Used for unit testing.
     */
    bool doNotStartRecoveries;

    /**
     * Used during unit testing: disables the initial check in
     * startMasterRecovery.
     */
    bool startRecoveriesEvenIfNoThread;

    /**
     * Used during unit testing: disables the delay when rescheduling
     * a failed recovery.
     */
    bool skipRescheduleDelay;

    friend class MasterRecoveryManagerInternal::ApplyTrackerChangesTask;
    friend class MasterRecoveryManagerInternal::MaybeStartRecoveryTask;
    friend class MasterRecoveryManagerInternal::EnqueueMasterRecoveryTask;
    friend class MasterRecoveryManagerInternal::RecoveryMasterFinishedTask;
    DISALLOW_COPY_AND_ASSIGN(MasterRecoveryManager);
};

} // namespace RAMCloud

#endif
