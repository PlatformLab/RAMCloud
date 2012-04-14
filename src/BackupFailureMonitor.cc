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

#include "BackupFailureMonitor.h"
#include "Log.h"
#include "ReplicaManager.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Create an instance that will listen for changes to #serverList and
 * inform #log of backup failures.  After construction failures
 * won't be dispatched until after start() is called, which starts a
 * thread to monitor for failures.  The thread is cleaned up on destruction.
 *
 * \param serverList
 *      A ServerList maintained by the MembershipService which will be
 *      monitored for changes.
 * \param replicaManager
 *      Which ReplicaManager should be informed of backup failures (via
 *      ReplicaManager::handleBackupFailures()). Can be NULL for testing,
 *      in which case no action will be taken on backup failures.
 * \param log
 *      Which Log is associated with \a replicaManager.  Used to roll over
 *      the log head in the case that a replica of the head is lost.  Can
 *      be NULL for testing, but take care because operations on
 *      \a replicaManager may fail to sync (instead spinning forever) since
 *      rolling over to a new log head is required for queued writes to
 *      make progress.
 */
BackupFailureMonitor::BackupFailureMonitor(ServerList& serverList,
                                           ReplicaManager* replicaManager,
                                           Log* log)
    : replicaManager(replicaManager)
    , log(log)
    , running(false)
    , changesOrExit()
    , mutex()
    , thread()
    , tracker()
{
    assert((!replicaManager && !log) || (replicaManager && log));
    // Important that this get constructed AFTER "this" is constructed
    // because the construction of tracker will cause an invocation of
    // trackerChangesEnqueued() and its important that the instance
    // is constructed by that time.  Hence the Tub.
    tracker.construct(serverList, this);
}

/**
 * Halt the thread, if running, and destroy this.
 */
BackupFailureMonitor::~BackupFailureMonitor()
{
    halt();
}

/**
 * Main loop of the BackupFailureMonitor; waits for notifications from the
 * Server's main ServerList and kicks-off actions in response to backup
 * failures.  This method shouldn't be called directly; use start() to start a
 * handler and halt() to terminate one cleanly.
 *
 * \param context
 *      The Context this thread should start in.
 */
void
BackupFailureMonitor::main(Context& context)
{
    Context::Guard _(context);
    Lock lock(mutex);
    while (true) {
        // If the replicaManager isn't working and there aren't any
        // cluster membership notifications, then go to sleep.
        while ((!replicaManager || replicaManager->isIdle()) &&
               !tracker->hasChanges()) {
            if (!running)
                return;
            changesOrExit.wait(lock);
        }
        // Careful: on remove events, for some less than clear reason only
        // the serverId field is valid.  The on SERVER_CRASHED other fields
        // remain in the tracker until the next call to getChange, so they
        // can be accessed that way.
        ServerDetails server;
        ServerChangeEvent event;
        while (tracker->getChange(server, event)) {
            ServerId id = server.serverId;
            if (event != SERVER_CRASHED)
                continue;
            LOG(DEBUG,
                "Notifying log of failure of serverId %lu",
                id.getId());
            if (replicaManager) {
                Tub<uint64_t> failedOpenSegment =
                    replicaManager->handleBackupFailure(id);
                if (log && failedOpenSegment) {
                    LOG(DEBUG, "Allocating a new log head");
                    log->allocateHeadIfStillOn(*failedOpenSegment);
                }
            }
        }
        if (replicaManager)
            replicaManager->proceed();
    }
}

/**
 * Start monitoring for failures.  Calling start() on an instance that is
 * already started has no effect.
 */
void
BackupFailureMonitor::start()
{
    Lock lock(mutex);
    if (running)
        return;
    running = true;
    thread.construct(&BackupFailureMonitor::main,
                     this, std::ref(Context::get()));
}

/**
 * Stop monitoring for failures.  Calling halt() on an instance that is
 * already halted or has never been started has no effect.
 */
void
BackupFailureMonitor::halt()
{
    Lock lock(mutex);
    if (!running)
        return;
    running = false;
    changesOrExit.notify_one();
    lock.unlock();
    thread->join();
    thread.destroy();
}

/**
 * Accepts notifications from the ServerList (via #tracker) and wakes up
 * the main loop to process changes if it sleeping.
 */
void
BackupFailureMonitor::trackerChangesEnqueued()
{
    Lock lock(mutex);
    changesOrExit.notify_one();
}

/**
 * Indicates a replica for a particular segment that this master generated is
 * needed for durability or that it can be safely discarded.
 *
 * This method really belongs on ReplicaManager, but due to some circular
 * dependencies it is hard to provide there. It is here so that the method
 * so the calling backup can ensure the ReplicaManager has been made aware
 * (via #tracker) of all the failures necessary to ensure the master can
 * correctly respond to the garbage collection query.
 *
 * \param backupServerId
 *      The id of the server which has the replica in storage. This is used
 *      to ensure that this master only retuns false if it has been made aware
 *      of any crashes of (now dead) backup servers which used the same
 *      storage as the calling backup.
 * \param segmentId
 *      The id of the segment of the replica whose status is in question.
 * \return
 *      True if the replica for \a segmentId may be needed to recover from
 *      failures.  False if the replica is no longer needed because the
 *      segment is fully replicated.
 */
bool
BackupFailureMonitor::isReplicaNeeded(ServerId backupServerId,
                                      uint64_t segmentId)
{
    // If backupServerId does not appear "up" then the master can be in one
    // of two situtations:
    // 1) It hasn't heard about backupServerId yet, in which case it also
    //    may not know that the failure of the server which backupServerId
    //    is replacing (i.e. the backup that formerly operated the storage
    //    that backupServerId has restarted from and is attempting to
    //    garbage collect).
    // 2) backupServerId has come and gone in the cluster and is actually
    //    now dead.
    // In either of these cases the only safe thing to do is to tell
    // the backup to hold on to the replica.
    ServerDetails* backup = NULL;
    try {
        backup = tracker->getServerDetails(backupServerId);
    } catch (const Exception& e) {}
    if (!backup || backup->status != ServerStatus::UP)
        return true;

    // This is just for testing.
    if (!replicaManager)
        return false;

    // Now that the master knows it has at least heard and processed
    // the failure notification for the backup server that is being
    // replaced by backupServerId it is safe to indicate the replica
    // is no longer needed if the segment seems to be fully replicated.
    Tub<bool> synced = replicaManager->isSegmentSynced(segmentId);
    return !synced || !*synced;
}

} // namespace RAMCloud
