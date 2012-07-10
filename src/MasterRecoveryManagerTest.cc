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
#include "ShortMacros.h"
#include "TabletMap.h"
#include "TabletsBuilder.h"

namespace RAMCloud {

using namespace MasterRecoveryManagerInternal; // NOLINT

struct MasterRecoveryManagerTest : public ::testing::Test {
    Context context;
    CoordinatorServerList serverList;
    TabletMap tabletMap;
    MasterRecoveryManager mgr;

    MasterRecoveryManagerTest()
        : context()
        , serverList(context)
        , tabletMap()
        , mgr(context, serverList, tabletMap, NULL)
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
    }

    /**
     * Add an entry to #serverList and process all tasks in #taskQueue
     * (because adding an entry generates a Task which must be processed
     * to bring #tracker into sync with #serverList).
     * Careful not to call this when there are existing tasks on
     * #taskQueue.
     *
     * \return
     *      ServerId of the entry added to #serverList.
     */
    ServerId addMaster() {
        ProtoBuf::ServerList update;
        ServerId serverId =
            serverList.add("fake-locator", {MASTER_SERVICE}, 0, update);
        while (!mgr.taskQueue.isIdle())
            mgr.taskQueue.performTask();
        return serverId;
    }

    /**
     * Change an entry in #serverList to crashed state and process all
     * tasks in #taskQueue to force application of the server list change
     * to #tracker.
     * Careful not to call this when there are existing tasks on
     * #taskQueue.
     *
     * \param crashedServerId
     *      Server to mark as crashed.
     */
    void crashServer(ServerId crashedServerId) {
        ProtoBuf::ServerList update;
        serverList.crashed(crashedServerId, update);
        while (!mgr.taskQueue.isIdle())
            mgr.taskQueue.performTask();
    }
};

TEST_F(MasterRecoveryManagerTest, startAndHalt) {
    mgr.start(); // check start
    EXPECT_TRUE(mgr.thread);
    mgr.start(); // check dup start call
    EXPECT_TRUE(mgr.thread);
    mgr.halt(); // check halt
    EXPECT_FALSE(mgr.thread);
    mgr.halt(); // check dup halt call
    EXPECT_FALSE(mgr.thread);
    mgr.start(); // check restart after halt
    EXPECT_TRUE(mgr.thread);
}

TEST_F(MasterRecoveryManagerTest, startMasterRecoveryNoTablets) {
    auto crashedServerId = addMaster();
    TestLog::Enable _;
    mgr.startMasterRecovery(crashedServerId);
    EXPECT_EQ("startMasterRecovery: Server 1 crashed, "
              "but it had no tablets", TestLog::get());
}

TEST_F(MasterRecoveryManagerTest, startMasterRecovery) {
    auto crashedServerId = addMaster();
    crashServer(crashedServerId);
    tabletMap.addTablet({0, 0, ~0lu, crashedServerId, Tablet::NORMAL, {2, 3}});
    TestLog::Enable _;
    mgr.startMasterRecovery(crashedServerId);
    EXPECT_EQ("startMasterRecovery: Scheduling recovery of master 1",
              TestLog::get());
    auto tablet = tabletMap.getTablet(0, 0, ~0lu);
    EXPECT_EQ(tablet.status, Tablet::RECOVERING);

    EXPECT_EQ(1lu, mgr.taskQueue.outstandingTasks());
    EXPECT_EQ(0lu, mgr.waitingRecoveries.size());
    mgr.taskQueue.performTask();
    EXPECT_EQ(1lu, mgr.taskQueue.outstandingTasks());
    EXPECT_EQ(1lu, mgr.waitingRecoveries.size());
    mgr.taskQueue.performTask();
    EXPECT_EQ(1lu, mgr.taskQueue.outstandingTasks());
    EXPECT_EQ(0lu, mgr.waitingRecoveries.size());
}

TEST_F(MasterRecoveryManagerTest, destroyAndFreeRecovery) {
    std::unique_ptr<Recovery> recovery{
        new Recovery(mgr.taskQueue, &tabletMap, &mgr.tracker, &mgr,
                     {1, 0}, 0lu)};
    mgr.activeRecoveries[recovery->recoveryId] = recovery.get();
    mgr.destroyAndFreeRecovery(recovery.get());
    recovery.release();
    EXPECT_EQ(0lu, mgr.activeRecoveries.size());
}

TEST_F(MasterRecoveryManagerTest, trackerChangesEnqueued) {
    // Changes to serverList implicitly call trackerChangesEnqueued.
    auto serverId = addMaster();

    // Create a recovery which has serverId as a recovery master, make
    // sure it gets informed if serverId crashes.
    std::unique_ptr<Recovery> recovery{
        new Recovery(mgr.taskQueue, &tabletMap, &mgr.tracker, &mgr,
                     {1, 0},  0lu)};
    recovery->numPartitions = 2;
    mgr.tracker[ServerId(1, 0)] = recovery.get();

    TestLog::Enable _;
    EXPECT_EQ(0lu, recovery->unsuccessfulRecoveryMasters);
    crashServer(serverId);
    EXPECT_EQ(1lu, recovery->unsuccessfulRecoveryMasters);
}

TEST_F(MasterRecoveryManagerTest, recoveryFinished) {
    addMaster();
    Recovery recovery(mgr.taskQueue, &tabletMap, &mgr.tracker,
                      NULL, {1, 0}, 0lu);
    recovery.status = Recovery::BROADCAST_RECOVERY_COMPLETE;
    ASSERT_EQ(0lu, mgr.taskQueue.outstandingTasks());
    EXPECT_EQ(0lu, serverList.versionNumber);
    mgr.recoveryFinished(&recovery);

    // ApplyTrackerChangesTask for crashed, one for remove, and the
    // MaybeStartRecoveryTask.
    EXPECT_EQ(3lu, mgr.taskQueue.outstandingTasks());
    EXPECT_EQ(1lu, serverList.versionNumber);
}

TEST_F(MasterRecoveryManagerTest, recoveryFinishedUnsuccessful) {
    addMaster();
    Recovery recovery(mgr.taskQueue, &tabletMap, &mgr.tracker,
                      NULL, {1, 0},  0lu);
    ASSERT_EQ(0lu, mgr.taskQueue.outstandingTasks());
    EXPECT_EQ(0lu, serverList.versionNumber);
    mgr.recoveryFinished(&recovery);

    // EnqueueRecoveryTask.
    EXPECT_EQ(1lu, mgr.taskQueue.outstandingTasks());
    EXPECT_EQ(0lu, serverList.versionNumber);
}

TEST_F(MasterRecoveryManagerTest, recoveryMasterFinishedNoSuchRecovery) {
    addMaster();
    const ProtoBuf::Tablets recoveredTablets;
    mgr.recoveryMasterFinished(0lu, {1, 0}, recoveredTablets, false);
    TestLog::Enable _;
    mgr.taskQueue.performTask(); // Do RecoveryMasterFinishedTask.
    EXPECT_EQ("performTask: Recovery master reported completing recovery 0 "
              "but there is no ongoing recovery with that id; this "
              "should never happen in RAMCloud", TestLog::get());
}

TEST_F(MasterRecoveryManagerTest, recoveryMasterFinished) {
    MockRandom __(1);
    tabletMap.addTablet({0, 0, ~0lu, {1, 0}, Tablet::NORMAL, {2, 3}});

    auto crashedServerId = addMaster();
    crashServer(crashedServerId);
    addMaster(); // Recovery master.

    std::unique_ptr<Recovery> recovery{
        new Recovery(mgr.taskQueue, &tabletMap, &mgr.tracker, &mgr,
                     crashedServerId,  0lu)};
    recovery->numPartitions = 1;
    mgr.activeRecoveries[recovery->recoveryId] = recovery.get();
    // Register {2, 0} as a recovery master for this recovery.
    mgr.tracker[ServerId(2, 0)] = recovery.get();

    ProtoBuf::Tablets recoveredTablets;
    TabletsBuilder{recoveredTablets}
        (0, 0, ~0lu, TabletsBuilder::RECOVERING, 0, {2, 0});
    tabletMap.addTablet({0, 0, ~0lu, {1, 0}, Tablet::RECOVERING, {2, 3}});

    mgr.recoveryMasterFinished(recovery->recoveryId,
                               {2, 0}, recoveredTablets, true);
    EXPECT_EQ(0lu, serverList.versionNumber);
    EXPECT_EQ(1lu, mgr.taskQueue.outstandingTasks());
    TestLog::Enable _;
    mgr.taskQueue.performTask(); // Do RecoveryMasterFinishedTask.
    EXPECT_EQ(
        "performTask: Coordinator tabletMap after recovery master 2 finished: "
        "Tablet { tableId: 0 startKeyHash: 0 endKeyHash: 18446744073709551615 "
            "serverId: 2 status: NORMAL ctime: 0, 0 } "
        "Tablet { tableId: 0 startKeyHash: 0 endKeyHash: 18446744073709551615 "
            "serverId: 1 status: RECOVERING ctime: 2, 3 } | "
        "recoveryFinished: Recovery 1 completed for master 1",
              TestLog::get());

    // Recovery task which is finishing up, ApplyTrackerChangesTask (due to
    // change in server list to remove crashed master), and MaybeStart task.
    EXPECT_EQ(3lu, mgr.taskQueue.outstandingTasks());

    // Ensure server list broadcast happened.
    EXPECT_EQ(1lu, serverList.versionNumber);
}

TEST_F(MasterRecoveryManagerTest,
       recoveryMasterFinishedNotCompletelySuccessful)
{
    MockRandom __(1);
    tabletMap.addTablet({0, 0, ~0lu, {1, 0}, Tablet::NORMAL, {2, 3}});

    auto crashedServerId = addMaster();
    crashServer(crashedServerId);
    addMaster(); // Recovery master.

    std::unique_ptr<Recovery> recovery{
        new Recovery(mgr.taskQueue, &tabletMap, &mgr.tracker, &mgr,
                     crashedServerId, 0lu)};
    recovery->numPartitions = 1;
    mgr.activeRecoveries[recovery->recoveryId] = recovery.get();
    // Register {2, 0} as a recovery master for this recovery.
    mgr.tracker[ServerId(2, 0)] = recovery.get();

    ProtoBuf::Tablets recoveredTablets;
    TabletsBuilder{recoveredTablets}
        (0, 0, ~0lu, TabletsBuilder::RECOVERING, 0, {2, 0});
    tabletMap.addTablet({0, 0, ~0lu, {1, 0}, Tablet::RECOVERING, {2, 3}});

    mgr.recoveryMasterFinished(recovery->recoveryId, {2, 0},
                               recoveredTablets, false);
    TestLog::Enable _;
    mgr.taskQueue.performTask();
    EXPECT_EQ(
        "performTask: A recovery master failed to recover its partition | "
        "recoveryMasterFinished: Recovery master 2 failed to recover its "
            "partition of the will for crashed server 1 | "
        "recoveryMasterFinished: Recovery wasn't completely successful; will "
            "not broadcast the end of recovery 1 for server 1 to backups | "
        "recoveryFinished: Recovery 1 completed for master 1 | "
        "recoveryFinished: Recovery of server 1 failed to recover some "
            "tablets, rescheduling another recovery | "
        "destroyAndFreeRecovery: Recovery of server 1 done (now 0 active "
            "recoveries)"
        , TestLog::get());
    recovery.release();

    TestLog::reset();
    mgr.taskQueue.performTask();
    EXPECT_EQ("", TestLog::get()); // EnqueueMasterRecoveryTask.

    TestLog::reset();
    mgr.taskQueue.performTask();  // MaybeStartMasterRecoveryTask.
    EXPECT_EQ("performTask: Starting recovery of server 1 "
                  "(now 1 active recoveries)", TestLog::get());
}

TEST_F(MasterRecoveryManagerTest,
       MaybeStartRecoveryTaskTwoRecoveriesAtTheSameTime)
{
    // Damn straight. I always wanted to do that, man.

    tabletMap.addTablet({0, 0, ~0lu, {1, 0}, Tablet::NORMAL, {2, 3}});
    tabletMap.addTablet({1, 0, ~0lu, {2, 0}, Tablet::NORMAL, {2, 3}});
    tabletMap.addTablet({2, 0, ~0lu, {3, 0}, Tablet::NORMAL, {2, 3}});

    crashServer(addMaster());
    crashServer(addMaster());
    crashServer(addMaster());

    mgr.startMasterRecovery({1, 0});
    mgr.startMasterRecovery({2, 0});
    mgr.startMasterRecovery({3, 0});
    // Process each of the Enqueue tasks.
    mgr.taskQueue.performTask();
    mgr.taskQueue.performTask();
    mgr.taskQueue.performTask();
    // Three MaybeStartRecoveryTasks now on taskQueue.

    mgr.maxActiveRecoveries = 2;

    EXPECT_EQ(3lu, mgr.waitingRecoveries.size());
    EXPECT_EQ(0lu, mgr.activeRecoveries.size());
    TestLog::Enable _;
    mgr.taskQueue.performTask();
    EXPECT_EQ("performTask: Starting recovery of server 1 "
                  "(now 1 active recoveries) | "
              "performTask: Starting recovery of server 2 "
                  "(now 2 active recoveries) | "
              "performTask: 1 recoveries blocked waiting for other recoveries",
              TestLog::get());
    EXPECT_EQ(1lu, mgr.waitingRecoveries.size());
    EXPECT_EQ(2lu, mgr.activeRecoveries.size());
}

TEST_F(MasterRecoveryManagerTest,
       MaybeStartRecoveryTaskServerAlreadyRecovering)
{
    tabletMap.addTablet({0, 0, ~0lu, {1, 0}, Tablet::NORMAL, {2, 3}});
    tabletMap.addTablet({1, 0, ~0lu, {2, 0}, Tablet::NORMAL, {2, 3}});
    tabletMap.addTablet({2, 0, ~0lu, {3, 0}, Tablet::NORMAL, {2, 3}});

    auto crashedServerId = addMaster();
    crashServer(crashedServerId);
    EXPECT_EQ(ServerId(1, 0), crashedServerId);

    mgr.startMasterRecovery({1, 0});
    mgr.startMasterRecovery({1, 0});
    // Process each of the Enqueue tasks.
    mgr.taskQueue.performTask();
    mgr.taskQueue.performTask();
    // Two MaybeStartRecoveryTasks now on taskQueue.

    mgr.maxActiveRecoveries = 2;

    EXPECT_EQ(2lu, mgr.waitingRecoveries.size());
    EXPECT_EQ(0lu, mgr.activeRecoveries.size());
    TestLog::Enable _;
    mgr.taskQueue.performTask();
    EXPECT_EQ("performTask: Starting recovery of server 1 "
                  "(now 1 active recoveries) | "
              "performTask: Delaying start of recovery of server 1; "
                  "another recovery is active for the same ServerId | "
              "performTask: 1 recoveries blocked waiting for other recoveries",
              TestLog::get());
    EXPECT_EQ(1lu, mgr.waitingRecoveries.size());
    EXPECT_EQ(1lu, mgr.activeRecoveries.size());
}

} // namespace RAMCloud
