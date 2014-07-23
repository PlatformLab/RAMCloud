/* Copyright (c) 2012-2013 Stanford University
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
     * \param recoveryInfo
     *      Used to filter out replicas of segments which may have become
     *      inconsistent. A replica with a segment id less than this or
     *      an equal segmentId with a lesser epoch is not eligible to be used
     *      for recovery (both for log digest and object data purposes).
     *      Stored in and provided by the coordinator server list.
     */
    EnqueueMasterRecoveryTask(MasterRecoveryManager& recoveryManager,
                              ServerId crashedServerId,
                              const ProtoBuf::MasterRecoveryInfo& recoveryInfo)
        : Task(recoveryManager.taskQueue)
        , mgr(recoveryManager)
        , crashedServerId(crashedServerId)
        , masterRecoveryInfo(recoveryInfo)
    { }

    /**
     * Called by #taskQueue which serializes it with other tasks; this makes
     * access to all #recoveryManager state and the state of recoveries
     * safe.
     */
    void performTask()
    {
        mgr.waitingRecoveries.push(new Recovery(mgr.context, mgr.taskQueue,
                                &mgr.tableManager, &mgr.tracker,
                                &mgr, crashedServerId, masterRecoveryInfo));
        (new MaybeStartRecoveryTask(mgr))->schedule();
        delete this;
    }

  PRIVATE:
    MasterRecoveryManager& mgr;
    ServerId crashedServerId;
    ProtoBuf::MasterRecoveryInfo masterRecoveryInfo;
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
     * Notice: these tasks don't delete themselves so that the creator
     * can extract the result of the task via wait(). The creator is
     * responsible for reclaiming space when the task is no longer
     * needed.
     *
     * \param recoveryManager
     *      MasterRecoveryManager this recovery is part of.
     * \param recoveryId
     *      Id of the recovery this recovery master was performing.
     * \param recoveryMasterId
     *      ServerId of the recovery master which has finished recovering
     *      its portion of the will.
     * \param recoveryPartition
     *      Tablets describing the portion of the will that the recovery
     *      master recovered. Only used if \a successful is true.
     *      Recovery masters fill in each of the entries with their own
     *      ServerId which is used to set the new owner of tablets.
     * \param successful
     *      If true indicates the recovery master was successful in
     *      recovering its partition of the will and that it is
     *      ready to start serving requests for the data. If false
     *      then \a recoveryPartition is ignored and the tablets of
     *      the partition the recovery master was supposed to recover
     *      are left marked RECOVERING.
     */
    RecoveryMasterFinishedTask(MasterRecoveryManager& recoveryManager,
                               uint64_t recoveryId,
                               ServerId recoveryMasterId,
                               const ProtoBuf::RecoveryPartition&
                                     recoveryPartition,
                               bool successful)
        : Task(recoveryManager.taskQueue)
        , mgr(recoveryManager)
        , recoveryId(recoveryId)
        , recoveryMasterId(recoveryMasterId)
        , recoveryPartition(recoveryPartition)
        , successful(successful)
        , mutex()
        , taskPerformed(false)
        , performed()
        , cancelRecoveryOnRecoveryMaster(false)
    {}

    /**
     * Called by #taskQueue which serializes it with other tasks; this makes
     * access to all #recoveryManager state and the state of recoveries
     * safe.
     */
    void performTask()
    {
        Lock _(mutex);
        auto it = mgr.activeRecoveries.find(recoveryId);
        if (it == mgr.activeRecoveries.end()) {
            LOG(ERROR, "Recovery master reported completing recovery "
                "%lu but there is no ongoing recovery with that id; "
                "this should only happen after coordinator rollover; "
                "asking recovery master to abort this recovery", recoveryId);
            taskPerformed = true;
            cancelRecoveryOnRecoveryMaster = true;
            performed.notify_all();
            return;
        }

        if (successful) {
            // Update tablet map to point to new owner and mark as available.
            foreach (const auto& tablet, recoveryPartition.tablet()) {
                // TODO(stutsman): Currently won't work with concurrent access
                // on the tablet map but recovery will soon be revised to
                // accept only one call into recovery per master instead of
                // tablet which will fix this.

                // The caller has filled in recoveryPartition with new service
                // locator and server id of the recovery master, so just copy
                // it over.  Record the log position of the recovery master at
                // creation of this new tablet assignment. The value is the
                // position of the head at the very start of recovery.
                try {
                    LOG(DEBUG, "Modifying tablet map to set recovery master %s "
                        "as master for %lu, %lu, %lu",
                        ServerId(tablet.server_id()).toString().c_str(),
                        tablet.table_id(), tablet.start_key_hash(),
                        tablet.end_key_hash());
                    mgr.tableManager.tabletRecovered(
                        tablet.table_id(),
                        tablet.start_key_hash(), tablet.end_key_hash(),
                        ServerId(tablet.server_id()),
                        {tablet.ctime_log_head_id(),
                                tablet.ctime_log_head_offset()});
                } catch (const Exception& e) {
                    // TODO(stutsman): What should we do here?
                    DIE("Entry wasn't in the list anymore; "
                        "we need to handle this sensibly");
                }
            }

            foreach (const auto& indexlet, recoveryPartition.indexlet()) {
                try{
                    LOG(NOTICE, "Modifying indexlet map to set recovery master"
                        "%s as master for %lu, %u, %lu",
                        ServerId(indexlet.server_id()).toString().c_str(),
                        indexlet.table_id(), indexlet.index_id(),
                        indexlet.indexlet_table_id());
                    void* firstKey;
                    uint16_t firstKeyLength;
                    void* firstNotOwnedKey;
                    uint16_t firstNotOwnedKeyLength;
                    if (indexlet.start_key().compare("") != 0) {
                        firstKey =
                            const_cast<char *>(indexlet.start_key().c_str());
                        firstKeyLength =
                            (uint16_t)indexlet.start_key().length();
                    } else {
                        firstKey = NULL;
                        firstKeyLength = 0;
                    }

                    if (indexlet.end_key().compare("") != 0) {
                        firstNotOwnedKey =
                            const_cast<char *>(indexlet.end_key().c_str());
                        firstNotOwnedKeyLength =
                            (uint16_t)indexlet.end_key().length();
                    } else {
                        firstNotOwnedKey = NULL;
                        firstNotOwnedKeyLength = 0;
                    }
                    mgr.tableManager.indexletRecovered(
                        indexlet.table_id(), (uint8_t) indexlet.index_id(),
                        firstKey, firstKeyLength,
                        firstNotOwnedKey, firstNotOwnedKeyLength,
                        ServerId(indexlet.server_id()),
                        indexlet.indexlet_table_id());
                } catch (const Exception& e) {
                    // TODO(zhihao): What should we do here?
                    DIE("Entry wasn't in the list anymore; "
                        "we need to handle this sensibly.");
                }
            }
            LOG(DEBUG, "Coordinator tableManager after recovery master %s "
                "finished: %s",
                recoveryMasterId.toString().c_str(),
                mgr.tableManager.debugString().c_str());
        } else {
            LOG(WARNING, "A recovery master failed to recover its partition");
            cancelRecoveryOnRecoveryMaster = true;
        }

        Recovery* recovery = it->second;
        recovery->recoveryMasterFinished(recoveryMasterId, successful);
        taskPerformed = true;
        performed.notify_all();
    }

    /**
     * Block until this task has been executed.
     *
     * \return
     *      True if the recovery master should abort the recovery without
     *      taking ownership of the recovered tablets; false if the
     *      recovery master now owns the recovered tablets and can safely
     *      begin servicing requests for the data. Always return true if #successful is false
     *      true is always returned (if the recovery wasn't successful it
     *      should trivially be aborted already).
     */
    bool wait()
    {
        Lock lock(mutex);
        while (!taskPerformed)
            performed.wait(lock);
        bool shouldAbort = this->cancelRecoveryOnRecoveryMaster;
        return shouldAbort;
    }

  PRIVATE:
    MasterRecoveryManager& mgr;
    uint64_t recoveryId;
    ServerId recoveryMasterId;
    ProtoBuf::RecoveryPartition recoveryPartition;
    bool successful;

    /**
     * Mutex to synchronize access to the #taskPerformed and
     * #cancelRecoveryOnRecoveryMaster fields.
     */
    std::mutex mutex;
    typedef std::unique_lock<std::mutex> Lock;

    /**
     * False initially, set to true once the task has been performed. Used by
     * the caller of recoveryMasterFinished to determine when the recovery has
     * been notified of the success/failure of the recovery master.
     * #cancelRecoveryOnRecoveryMaster is not valid until this field is true.
     */
    bool taskPerformed;

    /**
     * Notified when taskPerformed is set to true so the caller can wake up and
     * extract the result from #cancelRecoveryOnRecoveryMaster.
     */
    std::condition_variable performed;

    /**
     * Valid only after taskPerformed is true; if true the calling recovery
     * master should abort the recovery and should NOT serve requests for the
     * data which they were originally assigned to recover. If false then the
     * calling recovery master now owns the recovered tablets and can begin
     * servicing requests for the data.
     */
    bool cancelRecoveryOnRecoveryMaster;
};
}
using namespace MasterRecoveryManagerInternal; // NOLINT

/**
 * Create a new instance; usually just one instance is created as part
 * of the CoordinatorService.
 *
 * \param context
 *      Overall information about the RAMCloud server or client..
 * \param  tableManager
 *      Authoritative information about tablets and their mapping to servers.
 * \param runtimeOptions
 *      Configuration options which are stored by the coordinator.
 *      May be NULL for testing.
 */
MasterRecoveryManager::MasterRecoveryManager(Context* context,
                                             TableManager& tableManager,
                                             RuntimeOptions* runtimeOptions)
    : context(context)
    , tableManager(tableManager)
    , runtimeOptions(runtimeOptions)
    , thread()
    , waitingRecoveries()
    , activeRecoveries()
    , maxActiveRecoveries(1u)
    , taskQueue()
    , tracker(context, this)
    , doNotStartRecoveries(false)
    , startRecoveriesEvenIfNoThread(false)
    , skipRescheduleDelay(false)
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
 * This method is invoked during server startup to enable the mechanism
 * for recovering crashed masters. In addition to starting the thread that
 * performs recoveries, this method scans the server list for servers already
 * in the crashed state, and (re)starts recovery for them. This can occur
 * when the coordinator is restarting after a crash, and there were recoveries
 * in progress at the time of the crash. 
 * start() and halt() are not thread-safe.
 */
void
MasterRecoveryManager::start()
{
    if (!thread)
        thread.construct(&MasterRecoveryManager::main, this);

    ServerId id;
    while (1) {
        bool end;
        id = context->coordinatorServerList->nextServer(id,
                ServiceMask({WireFormat::MASTER_SERVICE}), &end, true);
        if (end) {
            break;
        }
        if (context->coordinatorServerList->getStatus(id)
                == ServerStatus::CRASHED) {
            startMasterRecovery((*context->coordinatorServerList)[id]);
        }
    }
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
 * \param crashedServer
 *      The crashed server which is to be recovered.
 */
void
MasterRecoveryManager::startMasterRecovery(
        CoordinatorServerList::Entry crashedServer)
{
    ServerId crashedServerId = crashedServer.serverId;
    if (!thread && !startRecoveriesEvenIfNoThread) {
        // Recovery has not yet been officially enabled, so don't do
        // anything (when the start method is invoked, it will
        // automatically start recovery of all servers in the crashed state).
        TEST_LOG("Recovery requested for %s",
                crashedServerId.toString().c_str());
        return;
    }
    LOG(NOTICE, "Scheduling recovery of master %s",
        crashedServerId.toString().c_str());
    if (doNotStartRecoveries) {
        TEST_LOG("Recovery crashedServerId: %s",
                 crashedServerId.toString().c_str());
        return;
    }
    (new EnqueueMasterRecoveryTask(*this, crashedServerId,
                    crashedServer.masterRecoveryInfo))->schedule();
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
                mgr.tracker[server.serverId] = NULL;
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
        try {
            context->coordinatorServerList->recoveryCompleted(
                        recovery->crashedServerId);
        } catch (const ServerListException& e) {
            // Server may have already been removed from the list
            // because of an earlier recovery.
        }
        (new MaybeStartRecoveryTask(*this))->schedule();
    } else {
        LOG(NOTICE,
            "Recovery of server %s failed to recover some "
            "tablets, rescheduling another recovery",
            recovery->crashedServerId.toString().c_str());

        // Delay a while before rescheduling; otherwise the coordinator
        // will flood its log with recovery messages in situations
        // were there aren't enough resources to recover.
        if (!skipRescheduleDelay) {
            usleep(2000000);
        }

        // Enqueue will schedule a MaybeStartRecoveryTask.
        (new EnqueueMasterRecoveryTask(*this,
                                       recovery->crashedServerId,
                                       recovery->masterRecoveryInfo))->
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
 * \param recoveryPartition
 *      Tablets describing the portion of the will that the recovery
 *      master recovered. Only used if \a successful is true.
 *      Recovery masters fill in each of the entries with their own
 *      ServerId which is used to set the new owner of tablets.
 * \param successful
 *      If true indicates the recovery master was successful in
 *      recovering its partition of the will and that it is
 *      ready to start serving requests for the data. If false
 *      then \a recoveryPartition is ignored and the tablets of
 *      the partition the recovery master was supposed to recover
 *      are left marked RECOVERING.
 * \return
 *      True if the recovery master should abort the recovery without
 *      taking ownership of the recovered tablets; false if the
 *      recovery master now owns the recovered tablets and can safely
 *      begin servicing requests for the data. If \a successful is false
 *      then this always returns true (the recovery master should abort if
 *      the recovery wasn't successful).
 */
bool
MasterRecoveryManager::recoveryMasterFinished(
    uint64_t recoveryId,
    ServerId recoveryMasterId,
    const ProtoBuf::RecoveryPartition& recoveryPartition,
    bool successful)
{
    LOG(NOTICE, "Called by masterId %s with %u tablets and %u indexlets",
        recoveryMasterId.toString().c_str(), recoveryPartition.tablet_size(),
        recoveryPartition.indexlet_size());

    TEST_LOG("Recovered tablets");
    TEST_LOG("%s", recoveryPartition.ShortDebugString().c_str());

    // RecoveryMasterFinishedTasks don't delete themselves so we can get the
    // result back via wait().
    RecoveryMasterFinishedTask task(*this, recoveryId, recoveryMasterId,
                                    recoveryPartition, successful);
    task.schedule();
    bool shouldAbort = task.wait();
    if (shouldAbort)
        LOG(NOTICE, "Asking recovery master to abort its recovery");
    else
        LOG(NOTICE, "Notifying recovery master ok to serve tablets");
    return shouldAbort;
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
