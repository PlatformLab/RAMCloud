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

#include "MasterRecoveryManager.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Create a new instance; usually just one instance is created as part
 * of the CoordinatorService.
 *
 * \param serverList
 *      Authoritative list of all servers in the system and their details.
 * \param  tabletMap
 *      Authoritative information about tablets and their mapping to servers.
 */
MasterRecoveryManager::MasterRecoveryManager(CoordinatorServerList& serverList,
                                             TabletMap& tabletMap)
    : serverList(serverList)
    , tabletMap(tabletMap)
    , thread()
    , waitingRecoveries()
    , activeRecoveries()
    , maxActiveRecoveries(1u)
    , taskQueue()
    , doNotStartRecoveries()
{
}

/**
 * Halt the thread, if running, and destroy this.
 */
MasterRecoveryManager::~MasterRecoveryManager()
{
    halt();
}

/**
 * Start thread for performing recoveries; this must be called before other
 * operations to ensure recoveries actually happen.
 * Calling start() on an instance that is already started has no effect.
 * start() and halt() are not thread-safe.
 */
void
MasterRecoveryManager::start()
{
    if (!thread)
        thread.construct(&MasterRecoveryManager::main,
                         this, std::ref(Context::get()));
}

/**
 * Stop progress on recoveries.  Calling halt() on an instance that is
 * already halted or has never been started has no effect.
 * start() and halt() are not thread-safe.
 */
void
MasterRecoveryManager::halt()
{
    taskQueue.halt();
    if (thread)
        thread->join();
    thread.destroy();
}

/**
 * Mark the tablets belonging to a now crashed server as RECOVERING and enqueue
 * the recovery of the tablets; actual recovery happens asynchronously.
 *
 * \param crashedServerId
 *      The crashed server which is to be recovered. If the server did
 *      not own any tablets when it crashed then no recovery is started.
 * \param will
 *      A partitioned set of tablets or "will" of the crashed Master.
 *      It is represented as a tablet map with a partition id in the
 *      user_data field.  Partition ids must start at 0 and be
 *      consecutive.  No partition id can have 0 entries before
 *      any other partition that has more than 0 entries.  This
 *      is because the recovery recovers partitions up but excluding the
 *      first with no entries.
 */
void
MasterRecoveryManager::startMasterRecovery(ServerId crashedServerId,
                                           const ProtoBuf::Tablets& will)
{
    auto tablets =
        tabletMap.setStatusForServer(crashedServerId, Tablet::RECOVERING);
    if (tablets.empty()) {
        LOG(NOTICE, "Server %lu crashed, but it had no tablets",
            crashedServerId.getId());
        return;
    }
    restartMasterRecovery(crashedServerId, will);
}

void
MasterRecoveryManager::handleServerFailure(ServerId serverId)
{
    assert(false);
    /*
     * TODO(stutsman): Need some kind of tracker-like mechanism on
     * the coordinator server list.
     * Work through all active recoveries and subtract off the count of
     * tablets they are waiting on.
     */
}

/**
 * Deletes a Recovery and cleans up all resources associated with
 * it in the MasterRecoveryManager. Invoked by Recovery instances
 * when they've outlived their usefulness.
 */
void
MasterRecoveryManager::destroyAndFreeRecovery(Recovery* recovery)
{
    activeRecoveries.erase(recovery->getRecoveryId());
    LOG(NOTICE,
        "Recovery of server %lu done (now %lu active recoveries)",
        recovery->crashedServerId.getId(), activeRecoveries.size());
    delete recovery;
}

// - Recovery sub-tasks -

namespace MasterRecoveryManagerInternal {
class MaybeStartRecoveryTask : public Task {
  PUBLIC:
    /**
     * Schedule an attempt to start enqueued recoveries.
     * This is a one-shot task that when fired starts one or more recoveries
     * that were delayed waiting for other recoveries to finish.
     * If there are no recoveries waiting to start or if there are too many
     * recoveries already in progress the task is a no-op.
     *
     * \param recoveryManager
     *      MasterRecoveryManager which should dequeue a waiting recovery
     *      and start it, if possible.
     */
    explicit MaybeStartRecoveryTask(MasterRecoveryManager& recoveryManager)
        : Task(recoveryManager.taskQueue)
        , mgr(recoveryManager) {}

    /**
     * Called by #taskQueue which serializes it with other tasks; this makes
     * access to all #recoveryManager state and the state of recoveries
     * safe.
     */
    void performTask()
    {
        std::vector<Recovery*> alreadyActive;
        while (!mgr.waitingRecoveries.empty() &&
               mgr.activeRecoveries.size() < mgr.maxActiveRecoveries)
        {
            Recovery* recovery = mgr.waitingRecoveries.front();
            // Do not allow two recoveries for the same crashed master
            // at the same time. This can happen if one recovery fails
            // and schedules another. The second may get started before
            // the first finishes without this check.
            bool serverAlreadyRecovering = false;
            foreach (const auto& other, mgr.activeRecoveries) {
                if (other.second->crashedServerId == recovery->crashedServerId)
                {
                    serverAlreadyRecovering = true;
                    break;
                }
            }
            if (serverAlreadyRecovering) {
                alreadyActive.push_back(recovery);
                mgr.waitingRecoveries.pop();
                LOG(NOTICE,
                    "Delaying start of recovery of server %lu; "
                    "another recovery is active for the same ServerId",
                    recovery->crashedServerId.getId());
            } else {
                recovery->schedule();
                mgr.activeRecoveries[recovery->getRecoveryId()] = recovery;
                mgr.waitingRecoveries.pop();
                LOG(NOTICE, "Starting recovery of server %lu (now %lu active "
                    "recoveries)", recovery->crashedServerId.getId(),
                    mgr.activeRecoveries.size());
            }
        }
        foreach (auto* recovery, alreadyActive)
            mgr.waitingRecoveries.push(recovery);
        if (mgr.waitingRecoveries.size() > 0)
            LOG(NOTICE,
                "%lu recoveries blocked waiting for other recoveries",
                mgr.waitingRecoveries.size());
        delete this;
    }

  PRIVATE:
    MasterRecoveryManager& mgr;
};

class EnqueueMasterRecoveryTask : public Task {
  PUBLIC:
    /**
     * Construct a Recovery and schedule it to be placed on the
     * #waitingRecoveries queue when it is safe to do so. When this
     * task completes it will schedule a MaybeStartRecoveryTask so
     * the recovery will be started, if possible.
     * This is a one-shot task that deletes itself when complete.
     *
     * \param recoveryManager
     *      MasterRecoveryManager which should enqueue a new recovery.
     * \param crashedServerId
     *      The crashed server which is to be recovered.
     * \param will
     *      A partitioned set of tablets or "will" of the crashed Master.
     *      It is represented as a tablet map with a partition id in the
     *      user_data field.  Partition ids must start at 0 and be
     *      consecutive.  No partition id can have 0 entries before
     *      any other partition that has more than 0 entries.  This
     *      is because the recovery recovers partitions up but excluding the
     *      first with no entries.
     */
    EnqueueMasterRecoveryTask(MasterRecoveryManager& recoveryManager,
                              ServerId crashedServerId,
                              const ProtoBuf::Tablets& will)
        : Task(recoveryManager.taskQueue)
        , mgr(recoveryManager)
        , recovery()
    {
        recovery = new Recovery(mgr.taskQueue, mgr.serverList, mgr,
                                crashedServerId, will);
    }

    /**
     * Called by #taskQueue which serializes it with other tasks; this makes
     * access to all #recoveryManager state and the state of recoveries
     * safe.
     */
    void performTask()
    {
        mgr.waitingRecoveries.push(recovery);
        (new MaybeStartRecoveryTask(mgr))->schedule();
        delete this;
    }

  PRIVATE:
    MasterRecoveryManager& mgr;
    Recovery* recovery;
    DISALLOW_COPY_AND_ASSIGN(EnqueueMasterRecoveryTask);
};

class RecoveryMasterFinishedTask : public Task {
  PUBLIC:
    /**
     * Schedule the notification of an ongoing Recovery that a recovery
     * master has finished recovering its partition
     * (successfully or unsuccessfully). If, when invoked, this task
     * causes a recovery to be finished, then additional, end-of-recovery
     * tasks are performed and/or scheduled.
     * This is a one-shot task that deletes itself when complete.
     *
     * \param recoveryManager
     *      MasterRecoveryManager this recovery is part of.
     * \param recoveryId
     *      Id of the recovery this recovery master was performing.
     * \param recoveryMasterId
     *      ServerId of the recovery master which has finished recovering
     *      its portion of the will.
     * \param recoveredTablets
     *      Tablets describing the portion of the will that the recovery
     *      master recovered. Only used if \a successful is true.
     *      Recovery masters fill in each of the entries with their own
     *      ServerId which is used to set the new owner of tablets.
     * \param successful
     *      If true indicates the recovery master was successful in
     *      recovering its partition of the will and that it is
     *      ready to start serving requests for the data. If false
     *      then \a recoveredTablets is ignored and the tablets of
     *      the partition the recovery master was supposed to recover
     *      are left marked RECOVERING.
     */
    RecoveryMasterFinishedTask(MasterRecoveryManager& recoveryManager,
                               uint64_t recoveryId,
                               ServerId recoveryMasterId,
                               const ProtoBuf::Tablets& recoveredTablets,
                               bool successful)
        : Task(recoveryManager.taskQueue)
        , mgr(recoveryManager)
        , recoveryId(recoveryId)
        , recoveryMasterId(recoveryMasterId)
        , recoveredTablets(recoveredTablets)
        , successful(successful)
    {}

    /**
     * Called by #taskQueue which serializes it with other tasks; this makes
     * access to all #recoveryManager state and the state of recoveries
     * safe.
     */
    void performTask()
    {
        auto it = mgr.activeRecoveries.find(recoveryId);
        if (it == mgr.activeRecoveries.end()) {
            LOG(ERROR, "Recovery master reported completing recovery "
                "%lu but that there is no ongoing recovery with that id; "
                "this should never happen in RAMCloud", recoveryId);
            delete this;
            return;
        }

        if (successful) {
            // Update tablet map to point to new owner and mark as available.
            foreach (const auto& tablet, recoveredTablets.tablet()) {
                // TODO(stutsman): Currently won't work with concurrent access
                // on the tablet map but recovery will soon be revised to
                // accept only one call into recovery per master instead of
                // tablet which will fix this.

                // The caller has filled in recoveredTablets with new service
                // locator and server id of the recovery master, so just copy
                // it over.  Record the log position of the recovery master at
                // creation of this new tablet assignment. The value is the
                // position of the head at the very start of recovery.
                try {
                    mgr.tabletMap.modifyTablet(tablet.table_id(),
                                           tablet.start_key_hash(),
                                           tablet.end_key_hash(),
                                           ServerId(tablet.server_id()),
                                           Tablet::NORMAL,
                                           {tablet.ctime_log_head_id(),
                                            tablet.ctime_log_head_offset()});
                } catch (const Exception& e) {
                    // TODO(stutsman): What should we do here?
                    DIE("Entry wasn't in the list anymore; "
                        "we need to handle this sensibly");
                }
            }
        } else {
            LOG(WARNING, "A recovery master failed to recover its partition");
        }

        Recovery* recovery = it->second;
        recovery->recoveryMasterFinished(successful);
        if (recovery->isDone()) {
            LOG(NOTICE, "Recovery completed for master %lu",
                recovery->crashedServerId.getId());
            if (recovery->wasCompletelySuccessful()) {
                // Remove recovered server from the server list and broadcast
                // the change to the cluster.
                // TODO(stutsman): Eventually we'll want CoordinatorServerList
                // to take care of this for us automatically. So we can just
                // do the remove.
                ProtoBuf::ServerList update;
                mgr.serverList.remove(recovery->crashedServerId, update);
                mgr.serverList.incrementVersion(update);
                mgr.serverList.sendMembershipUpdate(update, {});
                (new MaybeStartRecoveryTask(mgr))->schedule();
            } else {
                LOG(NOTICE,
                    "Recovery of server %lu failed to recover some "
                    "tablets, rescheduling another recovery",
                    recovery->crashedServerId.getId());
                // Enqueue will schedule a MaybeStartRecoveryTask.
                (new EnqueueMasterRecoveryTask(mgr,
                                               recovery->crashedServerId,
                                               recovery->will))->schedule();
            }
        }
        delete this;
    }

  PRIVATE:
    MasterRecoveryManager& mgr;
    uint64_t recoveryId;
    ServerId recoveryMasterId;
    ProtoBuf::Tablets recoveredTablets;
    bool successful;
};
}
using namespace MasterRecoveryManagerInternal; // NOLINT

/**
 * Schedule the notification of an ongoing Recovery that a recovery
 * master has finished recovering its partition
 * (successfully or unsuccessfully). The actual notification happens
 * asynchronously.
 *
 * \param recoveryId
 *      Id of the recovery this recovery master was performing.
 * \param recoveryMasterId
 *      ServerId of the recovery master which has finished recovering
 *      its portion of the will.
 * \param recoveredTablets
 *      Tablets describing the portion of the will that the recovery
 *      master recovered. Only used if \a successful is true.
 *      Recovery masters fill in each of the entries with their own
 *      ServerId which is used to set the new owner of tablets.
 * \param successful
 *      If true indicates the recovery master was successful in
 *      recovering its partition of the will and that it is
 *      ready to start serving requests for the data. If false
 *      then \a recoveredTablets is ignored and the tablets of
 *      the partition the recovery master was supposed to recover
 *      are left marked RECOVERING.
 */
void
MasterRecoveryManager::recoveryMasterFinished(
    uint64_t recoveryId,
    ServerId recoveryMasterId,
    const ProtoBuf::Tablets& recoveredTablets,
    bool successful)
{
    LOG(NOTICE, "called by masterId %lu with %u tablets",
        recoveryMasterId.getId(), recoveredTablets.tablet_size());

    TEST_LOG("Recovered tablets");
    TEST_LOG("%s", recoveredTablets.ShortDebugString().c_str());

    (new RecoveryMasterFinishedTask(*this, recoveryId, recoveryMasterId,
                                   recoveredTablets, successful))->schedule();
}

// - private -

/**
 * Drive the next step in any ongoing recoveries; start new
 * recoveries if they were blocked on other recoveries. Exits
 * when taskQueue.halt() is called.
 *
 * \param context
 *      The Context this thread should start in.
 */
void
MasterRecoveryManager::main(Context& context)
{
    Context::Guard _(context);
    taskQueue.performTasksUntilHalt();
}

/**
 * Enqueue the recovery of the tablets indicated by \a will; actual recovery
 * happens asynchronously. This method does NOT mark the tablets of \a will
 * as RECOVERING; see startMasterRecovery() for that.
 *
 * \param crashedServerId
 *      The crashed server which is to be recovered. If the server did
 *      not own any tablets when it crashed then no recovery is started.
 * \param will
 *      A partitioned set of tablets or "will" of the crashed Master.
 *      It is represented as a tablet map with a partition id in the
 *      user_data field.  Partition ids must start at 0 and be
 *      consecutive.  No partition id can have 0 entries before
 *      any other partition that has more than 0 entries.  This
 *      is because the recovery recovers partitions up but excluding the
 *      first with no entries.
 */
void
MasterRecoveryManager::restartMasterRecovery(ServerId crashedServerId,
                                             const ProtoBuf::Tablets& will)
{
    LOG(NOTICE, "Scheduling recovery of master %lu", crashedServerId.getId());

    if (doNotStartRecoveries) {
        TEST_LOG("Recovery crashedServerId: %lu", crashedServerId.getId());
        TEST_LOG("Recovery will: %s", will.ShortDebugString().c_str());
        return;
    }

    (new EnqueueMasterRecoveryTask(*this, crashedServerId, will))->schedule();
}

} // namespace RAMCloud
