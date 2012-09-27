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
#include "BackupFailureMonitor.h"
#include "ServerList.h"
#include "ShortMacros.h"
#include "StringUtil.h"

namespace RAMCloud {

/**
 * Note that some additional tests which test backup recovery in a
 * more end-to-end way can be found in ReplicaManagerTest.cc.
 */
struct BackupFailureMonitorTest : public ::testing::Test {
    Context context;
    ServerList serverList;
    BackupFailureMonitor monitor;

    BackupFailureMonitorTest()
        : context()
        , serverList(&context)
        , monitor(&context, NULL)
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
    }
};

static bool mainFilter(string s) { return s == "main"; }

TEST_F(BackupFailureMonitorTest, main) {
    TestLog::Enable _(&mainFilter);
    monitor.start();
    serverList.testingAdd({{2, 0}, "mock:host=backup1",
                          {WireFormat::BACKUP_SERVICE}, 100,
                          ServerStatus::UP});
    serverList.testingCrashed({2, 0});
    serverList.testingAdd({{3, 0}, "mock:host=master",
                           {WireFormat::MASTER_SERVICE}, 100,
                           ServerStatus::UP});
    serverList.testingRemove({3, 0});
    monitor.trackerChangesEnqueued();
    while (true) {
        // Until getChanges has drained the queue.
        BackupFailureMonitor::Lock lock(monitor.mutex);
        if (!monitor.tracker.hasChanges())
            break;
    }
    while (TestLog::get() == "");
    EXPECT_TRUE(StringUtil::contains(TestLog::get(),
        "main: Notifying replica manager of failure of serverId 2"));
}

TEST_F(BackupFailureMonitorTest, startAndHalt) {
    monitor.start(); // check start
    {
        BackupFailureMonitor::Lock lock(monitor.mutex);
        EXPECT_TRUE(monitor.running);
        EXPECT_TRUE(monitor.thread);
    }
    monitor.start(); // check dup start call
    {
        BackupFailureMonitor::Lock lock(monitor.mutex);
        EXPECT_TRUE(monitor.running);
        EXPECT_TRUE(monitor.thread);
    }
    monitor.halt(); // check halt
    {
        BackupFailureMonitor::Lock lock(monitor.mutex);
        EXPECT_FALSE(monitor.running);
        EXPECT_FALSE(monitor.thread);
    }
    monitor.halt(); // check dup halt call
    {
        BackupFailureMonitor::Lock lock(monitor.mutex);
        EXPECT_FALSE(monitor.running);
        EXPECT_FALSE(monitor.thread);
    }
    monitor.start(); // check restart after halt
    {
        BackupFailureMonitor::Lock lock(monitor.mutex);
        EXPECT_TRUE(monitor.running);
        EXPECT_TRUE(monitor.thread);
    }
}

TEST_F(BackupFailureMonitorTest, serverIsUp) {
    ServerDetails server;
    ServerChangeEvent event;

    EXPECT_FALSE(monitor.serverIsUp({2, 0}));

    serverList.testingAdd({{2, 0}, "mock:host=backup1",
                           {WireFormat::BACKUP_SERVICE}, 100,
                           ServerStatus::UP});
    while (monitor.tracker.getChange(server, event));
    EXPECT_TRUE(monitor.serverIsUp({2, 0}));

    // Ensure it is non-blocking.
    {
        BackupFailureMonitor::Lock lock(monitor.mutex);
        EXPECT_FALSE(monitor.serverIsUp({2, 0}));
    }

    serverList.testingCrashed({2, 0});
    while (monitor.tracker.getChange(server, event));
    EXPECT_FALSE(monitor.serverIsUp({2, 0}));
}

} // namespace RAMCloud
