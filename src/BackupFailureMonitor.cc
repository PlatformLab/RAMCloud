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
    while (running) {
        // Careful: on remove events, for some less than clear reason only the
        // serverId field is valid.  The on SERVER_REMOVED other fields remain
        // in the tracker until the next call to getChange, so they can be
        // accessed that way.
        // TODO(stutsman): Why does getChange() sometimes copy out the fields
        // and other time not?  It should be consistent.  Probably should just
        // return the id and event type and let the user get the details from
        // the tracker entry.
        ServerDetails server;
        ServerChangeEvent event;
        do {
            while (tracker->getChange(server, event)) {
                ServerId id = server.serverId;
                if (event == SERVER_REMOVED) {
                    // Only able to access details still if the event was a
                    // remove and getChange() hasn't been called again yet.
                    ServerDetails* details = tracker->getServerDetails(id);
                    if (details->services.has(BACKUP_SERVICE)) {
                        LOG(DEBUG,
                            "Notifying log of failure of serverId %lu",
                            id.getId());
                        if (replicaManager) {
                            Tub<uint64_t> failedOpenSegment =
                                replicaManager->handleBackupFailure(id);
                            if (log && failedOpenSegment)
                                log->allocateHeadIfStillOn(*failedOpenSegment);
                        }
                    }
                }
            }
            if (replicaManager)
                replicaManager->proceed();
        } while (replicaManager && !replicaManager->isIdle());
        changesOrExit.wait(lock);
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

} // namespace RAMCloud
