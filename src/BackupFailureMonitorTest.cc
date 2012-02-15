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

namespace RAMCloud {

struct BackupFailureMonitorTest : public ::testing::Test {
    ServerList serverList;
    BackupFailureMonitor monitor;

    BackupFailureMonitorTest()
        : serverList()
        , monitor(serverList, NULL, NULL)
    {
        Context::get().logger->setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
    }
};

static bool mainFilter(string s) { return s == "main"; }

TEST_F(BackupFailureMonitorTest, main) {
    TestLog::Enable _(&mainFilter);
    monitor.start();
    serverList.add(ServerId(2, 0), "mock:host=backup1",
                   {BACKUP_SERVICE}, 100);
    serverList.remove(ServerId(2, 0));
    serverList.add(ServerId(3, 0), "mock:host=master",
                   {MASTER_SERVICE}, 100);
    serverList.remove(ServerId(3, 0));
    while (monitor.tracker->areChanges()); // getChanges has drained the queue.
    BackupFailureMonitor::Lock lock(monitor.mutex); // processing is done.
    EXPECT_EQ("main: Notifying log of failure of serverId 2",
              TestLog::get());
}

TEST_F(BackupFailureMonitorTest, startAndHalt) {
    monitor.start(); // check start
    EXPECT_TRUE(monitor.running);
    EXPECT_TRUE(monitor.thread);
    monitor.start(); // check dup start call
    EXPECT_TRUE(monitor.running);
    EXPECT_TRUE(monitor.thread);
    monitor.halt(); // check halt
    EXPECT_FALSE(monitor.running);
    EXPECT_FALSE(monitor.thread);
    monitor.halt(); // check dup halt call
    EXPECT_FALSE(monitor.running);
    EXPECT_FALSE(monitor.thread);
    monitor.start(); // check restart after halt
    EXPECT_TRUE(monitor.running);
    EXPECT_TRUE(monitor.thread);
}

TEST_F(BackupFailureMonitorTest, trackerChangesEnqueued) {
    // First two entries are racy: either the first iteration
    // during the start up of main() will process them or the
    // callback from the serverList.  There is no good way to
    // tell which caused the processing, so run through these
    // entries and set up the real test once this race is over.
    monitor.start();
    serverList.add(ServerId(2, 0), "mock:host=backup1",
                   {BACKUP_SERVICE}, 100);
    serverList.remove(ServerId(2, 0));
    while (monitor.tracker->areChanges()); // getChanges has drained the queue.
    BackupFailureMonitor::Lock lock(monitor.mutex); // processing is done.
    lock.unlock();

    // Ok - now set up the real test: make sure changes are processed in
    // response to trackerChangesEnqueued().

    // Prevents tracker from calling trackerChangesEnqueued on add/remove.
    monitor.tracker->eventCallback = NULL;

    serverList.add(ServerId(3, 0), "mock:host=backup2",
                   {BACKUP_SERVICE}, 100);
    serverList.remove(ServerId(3, 0));

    TestLog::Enable _(&mainFilter);
    monitor.trackerChangesEnqueued();     // Notify the monitor thread.
    while (monitor.tracker->areChanges()); // getChanges has drained the queue.
    lock.lock(); // processing changes is done.
    lock.unlock();
    // Make sure it processed the new event.
    EXPECT_EQ("main: Notifying log of failure of serverId 3",
              TestLog::get());
}

} // namespace RAMCloud
