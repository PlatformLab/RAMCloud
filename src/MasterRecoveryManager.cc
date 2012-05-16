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
    , running(false)
    , changesOrExit()
    , mutex()
    , thread()
    , waitingRecoveries()
    , activeRecoveries()
    , maxActiveRecoveries(1u)
    , taskManager()
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
 * opearations to ensure recoveries actually happen.
 * Calling start() on an instance that is already started has no effect.
 */
void
MasterRecoveryManager::start()
{
    Lock lock(mutex);
    if (running)
        return;
    running = true;
    thread.construct(&MasterRecoveryManager::main,
                     this, std::ref(Context::get()));
}

/**
 * Stop progress on recoveries.  Calling halt() on an instance that is
 * already halted or has never been started has no effect.
 */
void
MasterRecoveryManager::halt()
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
 * Initiate a recovery of a crashed master.
 * There's got to be A LOT more to say here.
 * Must say that the server must be crashed in order to perform recovery.
 *
 * \param serverId
 *      The crashed server which is to be recovered.  If the server did
 *      not own any tablets when it crashed then no recovery is started.
 *      The serverId must be in #serverList and must be have a CRASHED status.
 * \throw Exception
 *      If \a serverId is not in #serverList.
 */
void
MasterRecoveryManager::startMasterRecovery(ServerId serverId,
                                           const ProtoBuf::Tablets& will)
{
    Lock lock(mutex);
    auto tablets = tabletMap.setStatusForServer(serverId, Tablet::RECOVERING);
    if (tablets.empty()) {
        LOG(NOTICE, "Server %lu crashed, but it had no tablets",
            serverId.getId());
        return;
    }
    startMasterRecovery(lock, serverId, will);
}

// See above; internal version used when lock is already held.
void
MasterRecoveryManager::startMasterRecovery(Lock& lock,
                                           ServerId serverId,
                                           const ProtoBuf::Tablets& will)
{
    LOG(NOTICE, "Scheduling recovery of master %lu", serverId.getId());

    if (doNotStartRecoveries) {
        TEST_LOG("Recovery serverId: %lu", serverId.getId());
        TEST_LOG("Recovery will: %s", will.ShortDebugString().c_str());
        return;
    }

    Recovery* recovery =
        new Recovery(taskManager, serverList, *this, serverId, will);
    waitingRecoveries.push(recovery);
    changesOrExit.notify_one();
}

void
MasterRecoveryManager::handleServerFailure(ServerId serverId)
{
    Lock lock(mutex);
    assert(false);
    /*
     * TODO(stutsman): Need some kind of tracker-like mechanism on
     * the coordinator server list.
     * Work through all active recoveries and subtract off the count of
     * tablets they are waiting on.
     */
}

void
MasterRecoveryManager::destroyAndFreeRecovery(Recovery* recovery)
{
    activeRecoveries.erase(recovery->masterId.getId());
    LOG(NOTICE,
        "Recovery of server %lu done (now %lu active recoveries)",
        recovery->masterId.getId(), activeRecoveries.size());
    delete recovery;
    changesOrExit.notify_one();
}

/**
 * Drive the next step in any ongoing recoveries; start new
 * recoveries if they were blocked on other recoveries.
 *
 * \param context
 *      The Context this thread should start in.
 */
void
MasterRecoveryManager::main(Context& context)
{
    Context::Guard _(context);
    Lock lock(mutex);
    while (true) {
        while (taskManager.isIdle() &&
               (waitingRecoveries.empty() ||
                activeRecoveries.size() >= maxActiveRecoveries))
                // TODO(stutsman): Need another condition here - wait even if
                // we could otherwise start a recovery, but one is already
                // going on for the same crashed master.
        {
            if (!running)
                return;
            changesOrExit.wait(lock);
        }

        // Start a new recovery if we are under the current limit
        if (!waitingRecoveries.empty() &&
            activeRecoveries.size() < maxActiveRecoveries)
        {
            Recovery* recovery = waitingRecoveries.front();
            if (activeRecoveries.find(recovery->masterId.getId()) !=
                activeRecoveries.end())
            {
                // Do not allow two recoveries for the same crashed master
                // at the same time. This can happen if one recovery fails
                // and schedules another. The second may get started before
                // the first finishes without this check.
                waitingRecoveries.pop();
                waitingRecoveries.push(recovery);
            }
            recovery->schedule();
            activeRecoveries[recovery->masterId.getId()] = recovery;
            waitingRecoveries.pop();
            LOG(NOTICE,
                "Starting recovery of server %lu (now %lu active recoveries)",
                recovery->masterId.getId(), activeRecoveries.size());
        }

        taskManager.proceed();
    }
}

bool
MasterRecoveryManager::tabletsRecovered(
    ServerId serverId,
    ServerId crashedMasterId,
    const ProtoBuf::Tablets& recoveredTablets,
    const ProtoBuf::Tablets& will,
    Status status)
{
    Lock lock(mutex);
    LOG(NOTICE, "called by masterId %lu with %u tablets",
        serverId.getId(), recoveredTablets.tablet_size());

    TEST_LOG("Recovered tablets");
    TEST_LOG("%s", recoveredTablets.ShortDebugString().c_str());

    auto it = activeRecoveries.find(crashedMasterId.getId());
    if (it == activeRecoveries.end()) {
        LOG(ERROR, "Recovery master reported completing recovery of "
            "server %lu but that server doesn't seem to be "
            "under recovery; this should never happen in RAMCloud",
            crashedMasterId.getId());
        return false;
    }
    Recovery* recovery = it->second;

    bool successful;
    if (status == STATUS_OK) {
        // Update tablet map to point to new owner and mark as available.
        foreach (const auto& tablet, recoveredTablets.tablet()) {
            // TODO(stutsman): Currently won't work with concurrent access on the
            // tablet map but recovery will soon be revised to accept only one call
            // into recovery per master instead of tablet which will fix this.

            // The caller has filled in recoveredTablets with new service
            // locator and server id of the recovery master, so just copy
            // it over.
            // Record the log position of the recovery master at creation of
            // this new tablet assignment. The value is the position of the
            // head at the very start of recovery.
            try {
                tabletMap.modifyTablet(tablet.table_id(),
                                       tablet.start_key_hash(),
                                       tablet.end_key_hash(),
                                       ServerId(tablet.server_id()),
                                       Tablet::NORMAL,
                                       {tablet.ctime_log_head_id(),
                                        tablet.ctime_log_head_offset()});
            } catch (const Exception& e) {
                // TODO(stutsman): What should we do here?
                DIE("Entry wasn't in the list anymore; we need to handle this "
                    "sensibly");
            }
        }
        successful = true;
    } else {
        LOG(ERROR, "A recovery master failed to recover its partition");
        successful = false;
    }

    recovery->recoveryMasterCompleted(successful);
    if (recovery->isDone()) {
        // Recoveries add themselves on to the task manager queue in
        // order to broadcast the end of recovery message. Need to
        // wait the thread in main() to complete the work.
        changesOrExit.notify_one();
        LOG(NOTICE, "Recovery completed for master %lu",
            recovery->masterId.getId());
        if (!recovery->wasCompletelySuccessful()) {
            LOG(NOTICE,
                "Recovery of server %lu failed to recover some "
                "tablets, rescheduling another recovery",
                recovery->masterId.getId());
            startMasterRecovery(lock, recovery->masterId, recovery->will);
        }
        return true;
    }
    return false;
}

} // namespace RAMCloud
