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
#include "ReplicaManager.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Create an instance that will listen for changes to #serverList and
 * inform #replicaManager of backup failures.  After construction failures
 * won't be dispatched until after start() is called, which starts a
 * thread to monitor for failures.  The thread is cleaned up on destruction.
 *
 * \param context
 *      Overall information about the RAMCloud server.
 * \param replicaManager
 *      Which ReplicaManager should be informed of backup failures (via
 *      ReplicaManager::handleBackupFailures()). Can be NULL for testing,
 *      in which case no action will be taken on backup failures.
 */
BackupFailureMonitor::BackupFailureMonitor(Context* context,
                                           ReplicaManager* replicaManager)
    : replicaManager(replicaManager)
    , running(false)
    , changesOrExit()
    , mutex()
    , thread()
    , tracker(context, this)
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
 * failures. This method shouldn't be called directly; use start() to start a
 * handler and halt() to terminate one cleanly.  There are several
 * synchronization issues with this method that are subtle. See
 * ReplicaManager::dataMutex for a synopsis; handling backup failures requires
 * ReplicaManager::dataMutex which ReplicatedSegment::sync() must take care not
 * to hold indefinitely.
 */
void
BackupFailureMonitor::main()
try {
    ServerDetails server;
    ServerChangeEvent event;
    while (running) {
        {
            Lock lock(mutex);
            // If the replicaManager isn't working and there aren't any
            // cluster membership notifications, then go to sleep.
            while ((!replicaManager || replicaManager->isIdle()) &&
                   !tracker.hasChanges()) {
                if (!running)
                    return;
                changesOrExit.wait(lock);
            }
        }

        while (true) {
            bool change;
            {
                Lock lock(mutex);
                change = tracker.getChange(server, event);
            }
            if (!change)
                break;
            // Careful: on remove events, for some less than clear reason
            // only the serverId field is valid.
            ServerId id = server.serverId;
            if (event != SERVER_CRASHED)
                continue;
            LOG(DEBUG,
                "Notifying replica manager of failure of serverId %s",
                id.toString().c_str());
            if (replicaManager)
                replicaManager->handleBackupFailure(id);
        }
        if (replicaManager)
            replicaManager->proceed();
    }
} catch (const std::exception& e) {
    LOG(ERROR, "Fatal error in BackupFailureMonitor: %s", e.what());
    throw;
} catch (...) {
    LOG(ERROR, "Unknown fatal error in BackupFailureMonitor.");
    throw;
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
    thread.construct(&BackupFailureMonitor::main, this);
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
 * Return whether the server \a serverId is up as far as the
 * local ReplicaManager is aware. May spuriously return false in cases
 * where it would otherwise have to block to return the accurate answer.
 *
 * \param serverId
 *      A coordinator-assigned server id whose status is to be checked.
 * \return
 *      True if \a serverId is up according to the server list updates
 *      that the local ReplicaManager has been informed of, false otherwise.
 *      May spuriously return false in cases where it would otherwise have
 *      to block to return the accurate answer.
 */
bool
BackupFailureMonitor::serverIsUp(ServerId serverId)
{
    Lock lock(mutex, std::try_to_lock_t());
    if (!lock.owns_lock()) {
        return false;
    }
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
    changesOrExit.notify_one();
}

} // namespace RAMCloud
