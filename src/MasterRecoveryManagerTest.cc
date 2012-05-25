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

namespace RAMCloud {

using namespace MasterRecoveryManagerInternal; // NOLINT

struct MasterRecoveryManagerTest : public ::testing::Test {
    CoordinatorServerList serverList;
    TabletMap tabletMap;
    MasterRecoveryManager mgr;

    MasterRecoveryManagerTest()
        : serverList()
        , tabletMap()
        , mgr(serverList, tabletMap)
    {
        Context::get().logger->setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
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
    ProtoBuf::ServerList update;
    const ServerId crashedServerId =
        serverList.add("cerulean", {MASTER_SERVICE}, 0, update);
    ProtoBuf::Tablets will;
    TestLog::Enable _;
    mgr.startMasterRecovery(crashedServerId, will);
    EXPECT_EQ("startMasterRecovery: Server 1 crashed, "
              "but it had no tablets", TestLog::get());
}

TEST_F(MasterRecoveryManagerTest, startMasterRecovery) {
    ProtoBuf::ServerList update;
    const ServerId crashedServerId =
        serverList.add("cerulean", {MASTER_SERVICE}, 0, update);
    serverList.crashed(crashedServerId, update);
    tabletMap.addTablet({0, 0, ~0lu, crashedServerId, Tablet::NORMAL, {2, 3}});
    ProtoBuf::Tablets will;
    TestLog::Enable _;
    mgr.startMasterRecovery(crashedServerId, will);
    EXPECT_EQ("restartMasterRecovery: Scheduling recovery of master 1",
              TestLog::get());
    auto tablet = tabletMap.getTablet(0, 0, ~0lu);
    EXPECT_EQ(tablet.status, Tablet::RECOVERING);
}

TEST_F(MasterRecoveryManagerTest, destroyAndFreeRecovery) {
    ProtoBuf::Tablets will;
    std::unique_ptr<Recovery> recovery{
        new Recovery(mgr.taskQueue, serverList, mgr, {1, 0}, will)};
    mgr.activeRecoveries[recovery->recoveryId] = recovery.get();
    mgr.destroyAndFreeRecovery(recovery.get());
    recovery.release();
    EXPECT_EQ(0lu, mgr.activeRecoveries.size());
}

TEST_F(MasterRecoveryManagerTest, recoveryMasterFinishedNoSuchRecovery) {
    const ProtoBuf::Tablets recoveredTablets;
    mgr.recoveryMasterFinished(0lu, {1, 0}, recoveredTablets, false);
    TestLog::Enable _;
    mgr.taskQueue.performTask();
    EXPECT_EQ("performTask: Recovery master reported completing recovery 0 "
              "but that there is no ongoing recovery with that id; this "
              "should never happen in RAMCloud", TestLog::get());
}

TEST_F(MasterRecoveryManagerTest, recoveryMasterFinished) {
    tabletMap.addTablet({0, 0, ~0lu, {1, 0}, Tablet::NORMAL, {2, 3}});

    ProtoBuf::ServerList update;
    const ServerId crashedServerId =
        serverList.add("cerulean", {MASTER_SERVICE}, 0, update);
    serverList.crashed(crashedServerId, update);

    const ProtoBuf::Tablets will;
    std::unique_ptr<Recovery> recovery{
        new Recovery(mgr.taskQueue, serverList, mgr, crashedServerId, will)};
    recovery->startedRecoveryMasters = 1;
    mgr.activeRecoveries[recovery->recoveryId] = recovery.get();

    ProtoBuf::Tablets recoveredTablets;
    auto& tablet = *recoveredTablets.add_tablet();
    tablet.set_table_id(0);
    tablet.set_start_key_hash(0);
    tablet.set_end_key_hash(~0lu);
    tablet.set_server_id(ServerId(2, 0).getId());
    tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
    tabletMap.addTablet({0, 0, ~0lu, {1, 0}, Tablet::RECOVERING, {2, 3}});

    mgr.recoveryMasterFinished(recovery->recoveryId,
                               {1, 0}, recoveredTablets, true);
    EXPECT_EQ(0lu, serverList.versionNumber);
    TestLog::Enable _;
    mgr.taskQueue.performTask();
    EXPECT_EQ("performTask: Recovery completed for master 1", TestLog::get());

    // Recovery task which is finishing up and MaybeStart task.
    EXPECT_EQ(2lu, mgr.taskQueue.outstandingTasks());

    // Ensure server list broadcast happened.
    EXPECT_EQ(1lu, serverList.versionNumber);
}

TEST_F(MasterRecoveryManagerTest,
       recoveryMasterFinishedNotCompletelySuccessful)
{
    tabletMap.addTablet({0, 0, ~0lu, {1, 0}, Tablet::NORMAL, {2, 3}});

    ProtoBuf::ServerList update;
    const ServerId crashedServerId =
        serverList.add("cerulean", {MASTER_SERVICE}, 0, update);
    serverList.crashed(crashedServerId, update);

    const ProtoBuf::Tablets will;
    std::unique_ptr<Recovery> recovery{
        new Recovery(mgr.taskQueue, serverList, mgr, crashedServerId, will)};
    recovery->startedRecoveryMasters = 1;
    mgr.activeRecoveries[recovery->recoveryId] = recovery.get();

    ProtoBuf::Tablets recoveredTablets;
    auto& tablet = *recoveredTablets.add_tablet();
    tablet.set_table_id(0);
    tablet.set_start_key_hash(0);
    tablet.set_end_key_hash(~0lu);
    tablet.set_server_id(ServerId(2, 0).getId());
    tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
    tabletMap.addTablet({0, 0, ~0lu, {1, 0}, Tablet::RECOVERING, {2, 3}});

    mgr.recoveryMasterFinished(recovery->recoveryId, {1, 0},
                               recoveredTablets, false);
    TestLog::Enable _;
    mgr.taskQueue.performTask();
    EXPECT_EQ(
        "performTask: A recovery master failed to recover its partition | "
        "performTask: Recovery completed for master 1 | "
        "performTask: Recovery of server 1 failed to recover some tablets, "
            "rescheduling another recovery", TestLog::get());

    TestLog::reset();
    mgr.taskQueue.performTask();
    EXPECT_EQ("destroyAndFreeRecovery: Recovery of server 1 done "
                  "(now 0 active recoveries)", TestLog::get());
    recovery.release();

    TestLog::reset();
    mgr.taskQueue.performTask();
    EXPECT_EQ("", TestLog::get()); // EnqueueMasterRecoveryTask.

    TestLog::reset();
    mgr.taskQueue.performTask();  // MaybeStartMasterRecoveryTask.
    EXPECT_EQ("performTask: Starting recovery of server 1 "
                  "(now 1 active recoveries)", TestLog::get());
}

TEST_F(MasterRecoveryManagerTest, restartMasterRecovery) {
    const ProtoBuf::Tablets will;
    mgr.restartMasterRecovery({1, 0}, will);
    EXPECT_EQ(1lu, mgr.taskQueue.outstandingTasks());
    EXPECT_EQ(0lu, mgr.waitingRecoveries.size());
    TestLog::Enable _;
    mgr.taskQueue.performTask();
    EXPECT_EQ(1lu, mgr.taskQueue.outstandingTasks());
    EXPECT_EQ(1lu, mgr.waitingRecoveries.size());
    mgr.taskQueue.performTask();
    EXPECT_EQ(1lu, mgr.taskQueue.outstandingTasks());
    EXPECT_EQ(0lu, mgr.waitingRecoveries.size());
}

TEST_F(MasterRecoveryManagerTest,
       MaybeStartRecoveryTaskTwoRecoveriesAtTheSameTime)
{
    // Damn straight. I always wanted to do that, man.
    ProtoBuf::Tablets will;
    mgr.restartMasterRecovery({1, 0}, will);
    mgr.restartMasterRecovery({2, 0}, will);
    mgr.restartMasterRecovery({3, 0}, will);
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
    ProtoBuf::Tablets will;
    mgr.restartMasterRecovery({1, 0}, will);
    mgr.restartMasterRecovery({1, 0}, will);
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
