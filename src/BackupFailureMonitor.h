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

#ifndef RAMCLOUD_BACKUPFAILUREMONITOR_H
#define RAMCLOUD_BACKUPFAILUREMONITOR_H

#include <condition_variable>
#include <thread>

#include "ServerTracker.h"
#include "Tub.h"

namespace RAMCloud {

class ReplicaManager;

/**
 * Waits for backup failure notifications from the Server's main ServerList
 * and informs the ReplicaManager which takes corrective actions. Runs in
 * a separate thread in order to provide immediate response to failures and
 * to provide a context for potentially long-running corrective actions even
 * while the master is otherwise idle.
 *
 * Logically part of the ReplicaManager.
 */
class BackupFailureMonitor
    : public ServerTracker<void>::Callback
{
  PUBLIC:
    BackupFailureMonitor(Context* context,
                         ReplicaManager* replicaManager);
    ~BackupFailureMonitor();

    void start();
    void halt();

    bool serverIsUp(ServerId serverId);

    void trackerChangesEnqueued();

  PRIVATE:
    void main();

    /**
     * The ReplicaManager to take corrective actions on when a backup failure
     * is discovered.
     */
    ReplicaManager* const replicaManager;

    /**
     * Used by start()/halt() to inform the main() loop of when it should
     * exit.  Protected by #mutex and changes are notified through
     * #changesOrExit.
     */
    bool running;

    /**
     * Used to inform the main() loop of when it should wake up which happens
     * in two cases: 1) running has been changed, or 2) changes have been
     * enqueued in the change list of #tracker.
     */
    std::condition_variable changesOrExit;

    /**
     * Protects all fields in this class so methods can safely communicate
     * with the loop running in main().
     */
    std::mutex mutex;

    /**
     * unique_lock is used to lock #mutex since the lock needs to be
     * relinquished when waiting on #changesOrExit.
     */
    typedef std::unique_lock<std::mutex> Lock;

    /**
     * Waits for notifications of changes to #tracker which indicates backup
     * failure and dispatches those changes to #replicaManager for it to take
     * corrective actions. #thread will ensure the corrective actions take
     * place in a timely manner (by driving the re-replication process, if
     * needed, and ensuring it completes).
     */
    Tub<std::thread> thread;

    typedef ServerTracker<void> FailureTracker;
    /**
     * A tracker which is only used to receive change notifications from
     * the master's main ServerList.  No extra/optional data is stored in
     * this tracker.
     */
    FailureTracker tracker;

    DISALLOW_COPY_AND_ASSIGN(BackupFailureMonitor);
};

} // namespace RAMCloud

#endif
