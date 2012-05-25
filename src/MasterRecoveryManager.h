/* Copyright (c) 2012 Stanford University
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
#include "TabletMap.h"
#include "Tub.h"

namespace RAMCloud {

namespace MasterRecoveryManagerInternal {
class MaybeStartRecoveryTask;
class EnqueueMasterRecoveryTask;
class RecoveryMasterFinishedTask;
}

/**
 * Handles all master recovery details on behalf of the coordinator.
 * Provides an interface to the coordinator to start recoveries. This manager
 * ensures the recovery eventually completes successfully (or continues
 * retrying indefinitely). These recoveries proceed independently of the
 * main coordinator worker threads via a thread provided internally by
 * the manager. The manager must make pervasive use of the coordinator's
 * serverList and tabletMap which are synchronized to make operations
 * safe.  The coordinator delegates the handling of recovery related RPCs
 * to this manager.
 */
class MasterRecoveryManager : public Recovery::Deleter
{
  PUBLIC:
    MasterRecoveryManager(CoordinatorServerList& serverList,
                          TabletMap& tabletMap);
    ~MasterRecoveryManager();

    void start();
    void halt();

    void startMasterRecovery(ServerId crashedServerId,
                             const ProtoBuf::Tablets& will);
    void recoveryMasterFinished(uint64_t recoveryId,
                                ServerId recoveryMasterId,
                                const ProtoBuf::Tablets& recoveredTablets,
                                bool successful);

    void handleServerFailure(ServerId serverId);

    virtual void destroyAndFreeRecovery(Recovery* recovery);

  PRIVATE:
    void main(Context& context);
    void restartMasterRecovery(ServerId crashedServerId,
                               const ProtoBuf::Tablets& will);

    /// Authoritative list of all servers in the system and their details.
    CoordinatorServerList& serverList;

    /// Authoritative information about tablets and their mapping to servers.
    TabletMap& tabletMap;

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
     * Prevents startMasterRecovery() from actually starting recovery and,
     * instead, logs arguments to the call. Used for unit testing.
     */
    bool doNotStartRecoveries;

    friend class MasterRecoveryManagerInternal::MaybeStartRecoveryTask;
    friend class MasterRecoveryManagerInternal::EnqueueMasterRecoveryTask;
    friend class MasterRecoveryManagerInternal::RecoveryMasterFinishedTask;
    DISALLOW_COPY_AND_ASSIGN(MasterRecoveryManager);
};

} // namespace RAMCloud

#endif
