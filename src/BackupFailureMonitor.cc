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
 */
BackupFailureMonitor::BackupFailureMonitor(ServerList& serverList,
                                           ReplicaManager* replicaManager)
    : replicaManager(replicaManager)
    , log(NULL)
    , running(false)
    , changesOrExit()
    , mutex()
    , thread()
    , tracker(serverList, this)
{
    // #tracker may call trackerChangesEnqueued() but all notifications will
    // be ignored until start() is called.
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
               !tracker.hasChanges()) {
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
        while (tracker.getChange(server, event)) {
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
 * already started has no effect, unless \a log is different between the
 * calls, in which case the behavior is undefined.
 *
 * \param log
 *      Which Log is associated with #replicaManager.  Used to roll over
 *      the log head in the case that a replica of the head is lost.  Can
 *      be NULL for testing, but take care because operations on
 *      #replicaManager may fail to sync (instead spinning forever) since
 *      rolling over to a new log head is required for queued writes to
 *      make progress.
 */
void
BackupFailureMonitor::start(Log* log)
{
    Lock lock(mutex);
    assert(!this->log || this->log == log);
    if (running)
        return;
    this->log = log;
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
    log = NULL;
    running = false;
    changesOrExit.notify_one();
    lock.unlock();
    thread->join();
    thread.destroy();
}

/**
 * Return whether the server \a serverId is up as far as the
 * local ReplicaManager is aware.
 *
 * \param serverId
 *      A coordinator-assigned server id whose status is to be checked.
 * \return
 *      True if \a serverId is up according to the server list updates
 *      that the local ReplicaManager has been informed of, false otherwise.
 */
bool
BackupFailureMonitor::serverIsUp(ServerId serverId)
{
    Lock lock(mutex);
    ServerDetails* backup = NULL;
    try {
        backup = tracker.getServerDetails(serverId);
    } catch (const Exception& e) {}
    if (!backup || backup->status != ServerStatus::UP)
        return false;
    return true;
}

/**
 * Accepts notifications from the ServerList (via #tracker) and wakes up
 * the main loop to process changes if it sleeping.
 */
void
BackupFailureMonitor::trackerChangesEnqueued()
{
    Lock lock(mutex);
    if (!running)
        return;
    changesOrExit.notify_one();
}

} // namespace RAMCloud
