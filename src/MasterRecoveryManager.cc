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
        , mgr(recoveryManager)
    {}

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
                    "Delaying start of recovery of server %s; "
                    "another recovery is active for the same ServerId",
                    recovery->crashedServerId.toString().c_str());
            } else {
                if (mgr.runtimeOptions)
                    recovery->testingFailRecoveryMasters =
                        mgr.runtimeOptions->popFailRecoveryMasters();
                recovery->schedule();
                mgr.activeRecoveries[recovery->getRecoveryId()] = recovery;
                mgr.waitingRecoveries.pop();
                LOG(NOTICE, "Starting recovery of server %s (now %lu active "
                    "recoveries)",
                    recovery->crashedServerId.toString().c_str(),
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
     * \param minOpenSegmentId
     *      Used to filter out replicas of segments which may have become
     *      inconsistent. A replica with a segment id less than this is
     *      not eligible to be used for recovery (both for log digest and
     *      object data purposes). Stored in and provided by the coordinator
     *      server list.
     */
    EnqueueMasterRecoveryTask(MasterRecoveryManager& recoveryManager,
                              ServerId crashedServerId,
                              uint64_t minOpenSegmentId)
        : Task(recoveryManager.taskQueue)
        , mgr(recoveryManager)
        , recovery()
        , minOpenSegmentId(minOpenSegmentId)
    {
        recovery = new Recovery(recoveryManager.context, mgr.taskQueue,
                                &mgr.tabletMap, &mgr.tracker,
                                &mgr, crashedServerId, minOpenSegmentId);
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
    uint64_t minOpenSegmentId;
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
                "%lu but there is no ongoing recovery with that id; "
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
                    LOG(ERROR, "Modifying tablet map to set recovery master %s "
                        "as master for %lu, %lu, %lu",
                        ServerId(tablet.server_id()).toString().c_str(),
                        tablet.table_id(), tablet.start_key_hash(),
                        tablet.end_key_hash());
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
            LOG(DEBUG, "Coordinator tabletMap after recovery master %s "
                "finished: %s",
                recoveryMasterId.toString().c_str(),
                mgr.tabletMap.debugString().c_str());
        } else {
            LOG(WARNING, "A recovery master failed to recover its partition");
        }

        Recovery* recovery = it->second;
        recovery->recoveryMasterFinished(recoveryMasterId, successful);
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
 * Create a new instance; usually just one instance is created as part
 * of the CoordinatorService.
 *
 * \param context
 *      Overall information about the RAMCloud server or client..
 * \param  tabletMap
 *      Authoritative information about tablets and their mapping to servers.
 * \param runtimeOptions
 *      Configuration options which are stored by the coordinator.
 *      May be NULL for testing.
 */
MasterRecoveryManager::MasterRecoveryManager(Context* context,
                                             TabletMap& tabletMap,
                                             RuntimeOptions* runtimeOptions)
    : context(context)
    , tabletMap(tabletMap)
    , runtimeOptions(runtimeOptions)
    , thread()
    , waitingRecoveries()
    , activeRecoveries()
    , maxActiveRecoveries(1u)
    , taskQueue()
    , tracker(context, this)
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
        thread.construct(&MasterRecoveryManager::main, this);
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
 * the recovery of the crashed master's tablets; actual recovery happens
 * asynchronously.
 *
 * \param crashedServerId
 *      The crashed server which is to be recovered. If the server did
 *      not own any tablets when it crashed then no recovery is started.
 */
void
MasterRecoveryManager::startMasterRecovery(ServerId crashedServerId)
{
    auto tablets =
        tabletMap.setStatusForServer(crashedServerId, Tablet::RECOVERING);
    if (tablets.empty()) {
        LOG(NOTICE, "Server %s crashed, but it had no tablets",
            crashedServerId.toString().c_str());
        return;
    }

    try {
        CoordinatorServerList::Entry server =
            context->coordinatorServerList->at(crashedServerId);
        LOG(NOTICE, "Scheduling recovery of master %s",
            crashedServerId.toString().c_str());

        if (doNotStartRecoveries) {
            TEST_LOG("Recovery crashedServerId: %s",
                     crashedServerId.toString().c_str());
            return;
        }

        (new EnqueueMasterRecoveryTask(*this, crashedServerId,
                                       server.minOpenSegmentId))->schedule();
    } catch (const Exception& e) {
        // Check one last time just to sanity check the correctness of the
        // recovery mananger: make sure that if the server isn't in the
        // server list anymore (presumably because a recovery completed on
        // it since the time of the start of the call) that there really aren't
        // any tablets left on it.
        tablets =
            tabletMap.setStatusForServer(crashedServerId, Tablet::RECOVERING);
        if (!tablets.empty()) {
            LOG(ERROR, "Tried to start recovery for crashed server %s which "
                "has tablets in the tablet map but is no longer in the "
                "coordinator server list. Cannot recover. Kiss your data "
                "goodbye.", crashedServerId.toString().c_str());
        }
    }
}

namespace MasterRecoveryManagerInternal {
class ApplyTrackerChangesTask : public Task {
  PUBLIC:
    /**
     * Create a task which when run will apply all enqueued changes to
     * #tracker and notifies any recoveries which have lost recovery masters.
     * This brings #mgr.tracker into sync with #mgr.serverList. Because this
     * task is run by #mgr.taskQueue is it serialized with other tasks.
     */
    explicit ApplyTrackerChangesTask(MasterRecoveryManager& mgr)
        : Task(mgr.taskQueue)
        , mgr(mgr)
    {
    }

    void performTask()
    {
        ServerDetails server;
        ServerChangeEvent event;
        while (mgr.tracker.getChange(server, event)) {
            if (event == SERVER_CRASHED || event == SERVER_REMOVED) {
                Recovery* recovery = mgr.tracker[server.serverId];
                if (!recovery)
                    break;
                LOG(NOTICE, "Recovery master %s crashed while recovering "
                    "a partition of server %s",
                    server.serverId.toString().c_str(),
                    recovery->crashedServerId.toString().c_str());
                // Like it or not, recovery is done on this recovery master
                // but unsuccessfully.
                recovery->recoveryMasterFinished(server.serverId, false);
            }
        }
        delete this;
    }

    MasterRecoveryManager& mgr;
};
}
using namespace MasterRecoveryManagerInternal; // NOLINT

/**
 * Schedule the handling of recovery master failures and the application
 * of changes to #tracker.  Invoked by #serverList whenever #tracker has
 * pending changes are pushed to it due to modifications to #serverList.
 */
void
MasterRecoveryManager::trackerChangesEnqueued()
{
    (new ApplyTrackerChangesTask(*this))->schedule();
}

/**
 * Deletes a Recovery and cleans up all resources associated with
 * it in the MasterRecoveryManager. Invoked by Recovery instances
 * when they've outlived their usefulness.
 * Note, this method performs no synchronization itself and is unsafe to call
 * in general. Basically it should only be called in the context of a performTask
 * method on a Recovery which is serialized by #taskQueue.
 *
 * \param recovery
 *      Recovery that is prepared to meet its maker.
 */
void
MasterRecoveryManager::destroyAndFreeRecovery(Recovery* recovery)
{
    // Waiting until destruction to remove the activeRecoveries
    // means another recovery won't start until after the end of
    // recovery broadcast. To change that just move the erase
    // to recoveryFinished().
    activeRecoveries.erase(recovery->getRecoveryId());
    LOG(NOTICE,
        "Recovery of server %s done (now %lu active recoveries)",
        recovery->crashedServerId.toString().c_str(),
        activeRecoveries.size());
    delete recovery;
}

/**
 * Note \a recovery as finished and either send out the updated server list
 * marked the crashed master as down or, if the recovery wasn't completely
 * successful, schedule a follow up recovery.
 * Called by a recovery once it has done as much as it can.
 * This means either recovery couldn't find a complete log and bailed
 * out almost immediately, or that all the recovery masters either finished
 * recovering their partition of the crashed master's will or failed.
 * This recovery may still be performing some cleanup tasks and will
 * call destroyAndFreeRecovery() when it is safe to delete it.
 * Note, this method performs no synchronization itself and is unsafe to call
 * in general. Basically it should only be called in the context of a performTask
 * method on a Recovery which is serialized by #taskQueue.
 *
 * \param recovery
 *      Recovery which is done.
 */
void
MasterRecoveryManager::recoveryFinished(Recovery* recovery)
{
    // Waiting until destruction to remove the activeRecoveries
    // means another recovery won't start until after the end of
    // recovery broadcast. To change that just move the erase
    // from destroyAndFreeRecovery() to recoveryFinished().
    LOG(NOTICE, "Recovery %lu completed for master %s",
        recovery->getRecoveryId(),
        recovery->crashedServerId.toString().c_str());
    if (recovery->wasCompletelySuccessful()) {
        // Remove recovered server from the server list and broadcast
        // the change to the cluster.
        // TODO(stutsman): Eventually we'll want CoordinatorServerList
        // to take care of this for us automatically. So we can just
        // do the remove.
        try {
            context->coordinatorServerList->remove(recovery->crashedServerId);
        } catch (const Exception& e) {
            // Server may have already been removed from the list
            // because of an earlier recovery.
        }
        (new MaybeStartRecoveryTask(*this))->schedule();
    } else {
        LOG(NOTICE,
            "Recovery of server %s failed to recover some "
            "tablets, rescheduling another recovery",
            recovery->crashedServerId.toString().c_str());
        // Enqueue will schedule a MaybeStartRecoveryTask.
        (new EnqueueMasterRecoveryTask(*this,
                                       recovery->crashedServerId,
                                       recovery->minOpenSegmentId))->
                                                            schedule();
    }
}

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
    LOG(NOTICE, "called by masterId %s with %u tablets",
        recoveryMasterId.toString().c_str(), recoveredTablets.tablet_size());

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
 */
void
MasterRecoveryManager::main()
try {
    taskQueue.performTasksUntilHalt();
} catch (const std::exception& e) {
    LOG(ERROR, "Fatal error in MasterRecoveryManager: %s", e.what());
    throw;
} catch (...) {
    LOG(ERROR, "Unknown fatal error in MasterRecoveryManager.");
    throw;
}

} // namespace RAMCloud
