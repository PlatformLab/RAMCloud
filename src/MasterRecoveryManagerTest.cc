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

#include "TestUtil.h"
#include "CoordinatorServerList.h"
#include "MasterRecoveryManager.h"
#include "MockCluster.h"
#include "ShortMacros.h"
#include "TabletsBuilder.h"

namespace RAMCloud {

using namespace MasterRecoveryManagerInternal; // NOLINT

struct MasterRecoveryManagerTest : public ::testing::Test {
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    CoordinatorService* service;
    CoordinatorServerList* serverList;
    TableManager* tableManager;
    MasterRecoveryManager* mgr;
    std::mutex mutex;

    typedef std::unique_lock<std::mutex> Lock;

    MasterRecoveryManagerTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , service()
        , serverList()
        , tableManager()
        , mgr()
        , mutex()
    {
        service = cluster.coordinator.get();
        serverList = service->context->coordinatorServerList;
        tableManager = service->context->tableManager;
        mgr = service->context->recoveryManager;

        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
    }

    /**
     * Add an entry to #serverList and process all tasks in #taskQueue
     * (because adding an entry generates a Task which must be processed
     * to bring #tracker into sync with #serverList).
     * Careful not to call this when there are existing tasks on
     * #taskQueue.
     *
     * \param lock
     *      Explicity needs lock to be held by calling function.
     * \return
     *      ServerId of the entry added to #serverList.
     */
    ServerId addMaster(Lock& lock) {
        ServerId serverId = serverList->generateUniqueId(lock);
        serverList->add(lock, serverId, "fake-locator",
            {WireFormat::MASTER_SERVICE}, 0);
        serverList->version++;
        serverList->pushUpdate(lock, serverList->version);
        serverList->sync();
        serverList->update.Clear(); // prevents cross contamination
        while (!mgr->taskQueue.isIdle())
            mgr->taskQueue.performTask();
        return serverId;
    }

    /**
     * Change an entry in #serverList to crashed state and process all
     * tasks in #taskQueue to force application of the server list change
     * to #tracker.
     * Careful not to call this when there are existing tasks on
     * #taskQueue.
     *
     * \param lock
     *      Explicity needs lock to be held by calling function.
     * \param crashedServerId
     *      Server to mark as crashed.
     */
    void crashServer(Lock& lock, ServerId crashedServerId) {
        serverList->crashed(lock, crashedServerId);
        serverList->version++;
        serverList->pushUpdate(lock, serverList->version);
        serverList->sync();
        serverList->update.Clear(); // prevents cross contamination
        while (!mgr->taskQueue.isIdle())
            mgr->taskQueue.performTask();
    }

    DISALLOW_COPY_AND_ASSIGN(MasterRecoveryManagerTest);
};

TEST_F(MasterRecoveryManagerTest, startAndHalt) {
    mgr->start(); // check start
    EXPECT_TRUE(mgr->thread);
    mgr->start(); // check dup start call
    EXPECT_TRUE(mgr->thread);
    mgr->halt(); // check halt
    EXPECT_FALSE(mgr->thread);
    mgr->halt(); // check dup halt call
    EXPECT_FALSE(mgr->thread);
    mgr->start(); // check restart after halt
    EXPECT_TRUE(mgr->thread);
}

TEST_F(MasterRecoveryManagerTest, startMasterRecoveryNoTablets) {
    Lock lock(mutex); // For calls to internal functions without real lock.
    auto crashedServerId = addMaster(lock);
    TestLog::Enable _;
    mgr->startMasterRecovery((*serverList)[crashedServerId]);
    EXPECT_EQ("startMasterRecovery: Scheduling recovery of master 1.0 | "
              "schedule: scheduled", TestLog::get());
}

TEST_F(MasterRecoveryManagerTest, startMasterRecovery) {
    Lock lock(mutex); // For calls to internal functions without real lock.
    auto crashedServerId = addMaster(lock);
    crashServer(lock, crashedServerId);
    tableManager->addTablet(
        lock, {0, 0, ~0lu, crashedServerId, Tablet::NORMAL, {2, 3}});
    TestLog::Enable _;
    mgr->startMasterRecovery((*serverList)[crashedServerId]);
    EXPECT_EQ("startMasterRecovery: Scheduling recovery of master 1.0 | "
              "schedule: scheduled",
              TestLog::get());
    auto tablet = tableManager->getTablet(lock, 0, 0, ~0lu);
    EXPECT_EQ(tablet.status, Tablet::RECOVERING);

    EXPECT_EQ(1lu, mgr->taskQueue.outstandingTasks());
    EXPECT_EQ(0lu, mgr->waitingRecoveries.size());
    mgr->taskQueue.performTask();
    EXPECT_EQ(1lu, mgr->taskQueue.outstandingTasks());
    EXPECT_EQ(1lu, mgr->waitingRecoveries.size());
    mgr->taskQueue.performTask();
    EXPECT_EQ(1lu, mgr->taskQueue.outstandingTasks());
    EXPECT_EQ(0lu, mgr->waitingRecoveries.size());
}

TEST_F(MasterRecoveryManagerTest, destroyAndFreeRecovery) {
    std::unique_ptr<Recovery> recovery{
        new Recovery(&context, mgr->taskQueue, tableManager, &mgr->tracker, mgr,
                     {1, 0}, {})};
    mgr->activeRecoveries[recovery->recoveryId] = recovery.get();
    mgr->destroyAndFreeRecovery(recovery.get());
    recovery.release();
    EXPECT_EQ(0lu, mgr->activeRecoveries.size());
}

TEST_F(MasterRecoveryManagerTest, trackerChangesEnqueued) {
    Lock lock(mutex); // For calls to internal functions without real lock.
    // Changes to serverList implicitly call trackerChangesEnqueued.
    auto serverId = addMaster(lock);

    // Create a recovery which has serverId as a recovery master, make
    // sure it gets informed if serverId crashes.
    std::unique_ptr<Recovery> recovery{
        new Recovery(&context, mgr->taskQueue, tableManager, &mgr->tracker, mgr,
                     {1, 0},  {})};
    recovery->numPartitions = 2;
    mgr->tracker[ServerId(1, 0)] = recovery.get();

    TestLog::Enable _;
    EXPECT_EQ(0lu, recovery->unsuccessfulRecoveryMasters);
    crashServer(lock, serverId);
    EXPECT_EQ(1lu, recovery->unsuccessfulRecoveryMasters);
}

TEST_F(MasterRecoveryManagerTest, recoveryFinished) {
    Lock lock(mutex); // For calls to internal functions without real lock.
    EXPECT_EQ(0lu, serverList->version);
    addMaster(lock);
    EXPECT_EQ(1lu, serverList->version);
    Recovery recovery(&context, mgr->taskQueue, tableManager, &mgr->tracker,
                      NULL, {1, 0}, {});
    recovery.status = Recovery::BROADCAST_RECOVERY_COMPLETE;
    ASSERT_EQ(0lu, mgr->taskQueue.outstandingTasks());
    EXPECT_EQ(1lu, serverList->version);
    mgr->recoveryFinished(&recovery);

    // ApplyTrackerChangesTask for crashed, one for remove, and the
    // MaybeStartRecoveryTask.
    EXPECT_EQ(3lu, mgr->taskQueue.outstandingTasks());
    EXPECT_EQ(2lu, serverList->version);
}

TEST_F(MasterRecoveryManagerTest, recoveryFinishedUnsuccessful) {
    Lock lock(mutex); // For calls to internal functions without real lock.
    EXPECT_EQ(0lu, serverList->version);
    addMaster(lock);
    EXPECT_EQ(1lu, serverList->version);
    Recovery recovery(&context, mgr->taskQueue, tableManager, &mgr->tracker,
                      NULL, {1, 0},  {});
    ASSERT_EQ(0lu, mgr->taskQueue.outstandingTasks());
    EXPECT_EQ(1lu, serverList->version);
    mgr->recoveryFinished(&recovery);

    // EnqueueRecoveryTask.
    EXPECT_EQ(3lu, mgr->taskQueue.outstandingTasks());
    EXPECT_EQ(2lu, serverList->version);
}

TEST_F(MasterRecoveryManagerTest, recoveryMasterFinishedNoSuchRecovery) {
    Lock lock(mutex); // For calls to internal functions without real lock.
    addMaster(lock);
    const ProtoBuf::Tablets recoveredTablets;
    TestLog::Enable _;
    std::thread thread(&MasterRecoveryManager::recoveryMasterFinished,
                       std::ref(*mgr),
                       0lu, ServerId{1, 0}, recoveredTablets, false);
    while (!mgr->taskQueue.performTask()); // Do RecoveryMasterFinishedTask.
    thread.join();
    EXPECT_EQ(
        "recoveryMasterFinished: Called by masterId 1.0 with 0 tablets | "
        "recoveryMasterFinished: Recovered tablets | "
        "recoveryMasterFinished:  | "
        "schedule: scheduled | "
        "performTask: Recovery master reported completing recovery 0 "
        "but there is no ongoing recovery with that id; "
        "this should only happen after coordinator rollover; "
        "asking recovery master to abort this recovery | "
        "recoveryMasterFinished: Asking recovery master to abort its recovery",
        TestLog::get());
}

TEST_F(MasterRecoveryManagerTest, recoveryMasterFinished) {
    Lock lock(mutex); // For calls to internal functions without real lock.
    MockRandom __(1);
    tableManager->tables["foo"] = 0;
    tableManager->addTablet(lock, {0, 0, ~0lu, {1, 0}, Tablet::NORMAL, {2, 3}});

    EXPECT_EQ(0lu, serverList->version);
    auto crashedServerId = addMaster(lock);
    crashServer(lock, crashedServerId);
    addMaster(lock); // Recovery master.
    EXPECT_EQ(3lu, serverList->version);

    std::unique_ptr<Recovery> recovery{
        new Recovery(&context, mgr->taskQueue, tableManager, &mgr->tracker, mgr,
                     crashedServerId, {})};
    recovery->numPartitions = 1;
    mgr->activeRecoveries[recovery->recoveryId] = recovery.get();
    // Register {2, 0} as a recovery master for this recovery.
    mgr->tracker[ServerId(2, 0)] = recovery.get();

    ProtoBuf::Tablets recoveredTablets;
    TabletsBuilder{recoveredTablets}
        (0, 0, ~0lu, TabletsBuilder::RECOVERING, 0, {2, 0});
    tableManager->addTablet(
        lock, {0, 0, ~0lu, {1, 0}, Tablet::RECOVERING, {2, 3}});

    // Add information about table to LogCabin, so t
    ProtoBuf::TableInformation state;
    state.set_entry_type("AliveTable");
    state.set_name("foo");
    state.set_table_id(0);
    state.set_server_span(1);
    ProtoBuf::TableInformation::TabletInfo& tablet(*state.add_tablet_info());
    tablet.set_start_key_hash(0);
    tablet.set_end_key_hash(~0lu);
    tablet.set_master_id(ServerId(1, 0).getId());
    tablet.set_ctime_log_head_id(2);
    tablet.set_ctime_log_head_offset(3);
    EntryId entryId = service->context->logCabinHelper->appendProtoBuf(
            *service->context->expectedEntryId, state);
    tableManager->setTableInfoLogId(lock, 0, entryId);

    EXPECT_EQ(3lu, serverList->version);

    TestLog::Enable _;
    std::thread thread(&MasterRecoveryManager::recoveryMasterFinished,
                       std::ref(*mgr),
                       recovery->recoveryId,
                       ServerId{2, 0}, recoveredTablets, true);
    while (!mgr->taskQueue.performTask()); // Do RecoveryMasterFinishedTask.
    thread.join();
    serverList->sync();
    EXPECT_EQ(
        "recoveryMasterFinished: Called by masterId 2.0 with 1 tablets | "
        "recoveryMasterFinished: Recovered tablets | "
        "recoveryMasterFinished: tablet { "
            "table_id: 0 start_key_hash: 0 end_key_hash: 18446744073709551615 "
            "state: RECOVERING server_id: 2 user_data: 0 ctime_log_head_id: 0 "
            "ctime_log_head_offset: 0 } | "
        "schedule: scheduled | "
        "performTask: Modifying tablet map to set recovery master 2.0 as "
            "master for 0, 0, 18446744073709551615 | "
        "execute: LogCabin: TabletRecovered entryId: 6 | "
        "complete: LogCabin: AliveTable entryId: 7 | "
        "performTask: Coordinator tableManager after recovery master 2.0 "
            "finished: "
        "Tablet { tableId: 0 startKeyHash: 0 endKeyHash: 18446744073709551615 "
            "serverId: 2.0 status: NORMAL ctime: 0, 0 } "
        "Tablet { tableId: 0 startKeyHash: 0 endKeyHash: 18446744073709551615 "
            "serverId: 1.0 status: RECOVERING ctime: 2, 3 } | "
        "schedule: scheduled | "
        "recoveryFinished: Recovery 1 completed for master 1.0 | "
        "execute: LogCabin: ServerRemoveUpdate entryId: 8 | "
        "complete: Removing 1.0 from cluster/coordinator server list | "
        "schedule: scheduled | schedule: scheduled | "
        "execute: LogCabin: ServerListVersion entryId: 9 | "
        "recoveryMasterFinished: Notifying recovery master ok to serve tablets",
              TestLog::get());

    // Recovery task which is finishing up, ApplyTrackerChangesTask (due to
    // change in server list to remove crashed master), and MaybeStart task.
    EXPECT_EQ(3lu, mgr->taskQueue.outstandingTasks());

    // Ensure server list broadcast happened.
    EXPECT_EQ(4lu, serverList->version);
}

TEST_F(MasterRecoveryManagerTest,
       recoveryMasterFinishedNotCompletelySuccessful)
{
    Lock lock(mutex); // For calls to internal functions without real lock.
    MockRandom __(1);
    tableManager->addTablet(lock, {0, 0, ~0lu, {1, 0}, Tablet::NORMAL, {2, 3}});

    auto crashedServerId = addMaster(lock);
    crashServer(lock, crashedServerId);
    addMaster(lock); // Recovery master.

    std::unique_ptr<Recovery> recovery{
        new Recovery(&context, mgr->taskQueue, tableManager, &mgr->tracker, mgr,
                     crashedServerId, {})};
    recovery->numPartitions = 1;
    mgr->activeRecoveries[recovery->recoveryId] = recovery.get();
    // Register {2, 0} as a recovery master for this recovery.
    mgr->tracker[ServerId(2, 0)] = recovery.get();

    ProtoBuf::Tablets recoveredTablets;
    TabletsBuilder{recoveredTablets}
        (0, 0, ~0lu, TabletsBuilder::RECOVERING, 0, {2, 0});
    tableManager->addTablet(
        lock, {0, 0, ~0lu, {1, 0}, Tablet::RECOVERING, {2, 3}});

    TestLog::Enable _;
    std::thread thread(&MasterRecoveryManager::recoveryMasterFinished,
                       std::ref(*mgr),
                       recovery->recoveryId,
                       ServerId{2, 0}, recoveredTablets, false);
    while (!mgr->taskQueue.performTask());
    thread.join();
    EXPECT_EQ(
        "recoveryMasterFinished: Called by masterId 2.0 with 1 tablets | "
        "recoveryMasterFinished: Recovered tablets | "
        "recoveryMasterFinished: tablet { table_id: 0 start_key_hash: 0 "
        "end_key_hash: 18446744073709551615 state: RECOVERING server_id: 2 "
        "user_data: 0 ctime_log_head_id: 0 ctime_log_head_offset: 0 } | "
        "schedule: scheduled | "
        "performTask: A recovery master failed to recover its partition | "
        "recoveryMasterFinished: Recovery master 2.0 failed to recover its "
            "partition of the will for crashed server 1.0 | "
        "recoveryMasterFinished: Recovery wasn't completely successful; will "
            "not broadcast the end of recovery 1 for server 1.0 to backups | "
        "recoveryFinished: Recovery 1 completed for master 1.0 | "
        "recoveryFinished: Recovery of server 1.0 failed to recover some "
            "tablets, rescheduling another recovery | "
        "schedule: scheduled | "
        "destroyAndFreeRecovery: Recovery of server 1.0 done (now 0 active "
            "recoveries) | "
        "recoveryMasterFinished: Asking recovery master to abort its recovery"
        , TestLog::get());
    recovery.release();

    TestLog::reset();
    mgr->taskQueue.performTask();  // EnqueueMasterRecoveryTask.
    EXPECT_EQ("schedule: scheduled", TestLog::get());

    TestLog::reset();
    mgr->taskQueue.performTask();  // MaybeStartMasterRecoveryTask.
    EXPECT_EQ("schedule: scheduled | performTask: Starting recovery of server "
                  "1.0 (now 1 active recoveries)", TestLog::get());
}

TEST_F(MasterRecoveryManagerTest,
       MaybeStartRecoveryTaskTwoRecoveriesAtTheSameTime)
{
    // Damn straight. I always wanted to do that, man.

    Lock lock(mutex); // For calls to internal functions without real lock.
    tableManager->addTablet(lock, {0, 0, ~0lu, {1, 0}, Tablet::NORMAL, {2, 3}});
    tableManager->addTablet(lock, {1, 0, ~0lu, {2, 0}, Tablet::NORMAL, {2, 3}});
    tableManager->addTablet(lock, {2, 0, ~0lu, {3, 0}, Tablet::NORMAL, {2, 3}});

    crashServer(lock, addMaster(lock));
    crashServer(lock, addMaster(lock));
    crashServer(lock, addMaster(lock));

    mgr->startMasterRecovery((*serverList)[ServerId(1, 0)]);
    mgr->startMasterRecovery((*serverList)[ServerId(2, 0)]);
    mgr->startMasterRecovery((*serverList)[ServerId(3, 0)]);
    // Process each of the Enqueue tasks.
    mgr->taskQueue.performTask();
    mgr->taskQueue.performTask();
    mgr->taskQueue.performTask();
    // Three MaybeStartRecoveryTasks now on taskQueue.

    mgr->maxActiveRecoveries = 2;

    EXPECT_EQ(3lu, mgr->waitingRecoveries.size());
    EXPECT_EQ(0lu, mgr->activeRecoveries.size());
    TestLog::Enable _;
    mgr->taskQueue.performTask();
    EXPECT_EQ("schedule: scheduled | "
              "performTask: Starting recovery of server 1.0 "
                  "(now 1 active recoveries) | "
              "schedule: scheduled | "
              "performTask: Starting recovery of server 2.0 "
                  "(now 2 active recoveries) | "
              "performTask: 1 recoveries blocked waiting for other recoveries",
              TestLog::get());
    EXPECT_EQ(1lu, mgr->waitingRecoveries.size());
    EXPECT_EQ(2lu, mgr->activeRecoveries.size());
}

TEST_F(MasterRecoveryManagerTest,
       MaybeStartRecoveryTaskServerAlreadyRecovering)
{
    Lock lock(mutex); // For calls to internal functions without real lock.
    tableManager->addTablet(lock, {0, 0, ~0lu, {1, 0}, Tablet::NORMAL, {2, 3}});
    tableManager->addTablet(lock, {1, 0, ~0lu, {2, 0}, Tablet::NORMAL, {2, 3}});
    tableManager->addTablet(lock, {2, 0, ~0lu, {3, 0}, Tablet::NORMAL, {2, 3}});

    auto crashedServerId = addMaster(lock);
    crashServer(lock, crashedServerId);
    EXPECT_EQ(ServerId(1, 0), crashedServerId);

    mgr->startMasterRecovery((*serverList)[ServerId(1, 0)]);
    mgr->startMasterRecovery((*serverList)[ServerId(1, 0)]);
    // Process each of the Enqueue tasks.
    mgr->taskQueue.performTask();
    mgr->taskQueue.performTask();
    // Two MaybeStartRecoveryTasks now on taskQueue.

    mgr->maxActiveRecoveries = 2;

    EXPECT_EQ(2lu, mgr->waitingRecoveries.size());
    EXPECT_EQ(0lu, mgr->activeRecoveries.size());
    TestLog::Enable _;
    mgr->taskQueue.performTask();
    EXPECT_EQ("schedule: scheduled | "
              "performTask: Starting recovery of server 1.0 "
                  "(now 1 active recoveries) | "
              "performTask: Delaying start of recovery of server 1.0; "
                  "another recovery is active for the same ServerId | "
              "performTask: 1 recoveries blocked waiting for other recoveries",
              TestLog::get());
    EXPECT_EQ(1lu, mgr->waitingRecoveries.size());
    EXPECT_EQ(1lu, mgr->activeRecoveries.size());
}

} // namespace RAMCloud
