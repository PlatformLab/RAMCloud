/* Copyright (c) 2013 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.xx
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"
#include "CoordinatorUpdateInfo.pb.h"
#include "CoordinatorUpdateManager.h"
#include "MockExternalStorage.h"

namespace RAMCloud {
class CoordinatorUpdateManagerTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    MockExternalStorage storage;
    CoordinatorUpdateManager manager;

    CoordinatorUpdateManagerTest()
        : logEnabler()
        , storage(true)
        , manager(&storage)
    {
        // For most tests it's simplest to pretend recovery has already
        // finished.
        manager.recoveryFinished();

        // Load a configuration record into the external storage.
        ProtoBuf::CoordinatorUpdateInfo info;
        info.set_lastfinished(20);
        info.set_firstavailable(1000);

        string str;
        info.SerializeToString(&str);
        storage.getResults.push(str);
    }

    ~CoordinatorUpdateManagerTest()
    {}

    // Pretty-print the contents of activeUpdates.
    string
    toString()
    {
        string result = format("%ld -- ", manager.smallestUnfinished);
        for (size_t i = 0; i < manager.activeUpdates.size(); i++) {
            result.append(manager.activeUpdates[i] ? "T" : "F");
        }
        return result;
    }

    // Pretty-print the configuration record from external storage.
    string
    configString()
    {
        ProtoBuf::CoordinatorUpdateInfo info;
        if (!info.ParseFromString(storage.setData)) {
            return "??";
        }
        return format("lastFinished: %lu, firstAvailable: %lu",
                info.lastfinished(), info.firstavailable());
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(CoordinatorUpdateManagerTest);
};

TEST_F(CoordinatorUpdateManagerTest, basics) {
    manager.init();
    EXPECT_EQ(1000ul, manager.nextSequenceNumber());
    EXPECT_EQ(1001ul, manager.nextSequenceNumber());
    EXPECT_EQ(1002ul, manager.nextSequenceNumber());
    EXPECT_EQ(1003ul, manager.nextSequenceNumber());
    manager.updateFinished(1001);
    EXPECT_EQ("1000 -- FTFF", toString());
    manager.updateFinished(1000);
    EXPECT_EQ("1002 -- FF", toString());
}

TEST_F(CoordinatorUpdateManagerTest, init_noInfoAvailable) {
    storage.getResults.pop();
    EXPECT_EQ(0ul, manager.init());
    EXPECT_EQ(0ul, manager.externalLastFinished);
    EXPECT_EQ(1ul, manager.externalFirstAvailable);
    EXPECT_EQ(1ul, manager.smallestUnfinished);
    EXPECT_EQ(0ul, manager.lastAssigned);
    EXPECT_EQ("init: couldn't find \"coordinatorUpdateManager\" object in "
            "external storage; starting new cluster from scratch",
            TestLog::get());
}
TEST_F(CoordinatorUpdateManagerTest, init_formatError) {
    storage.getResults.pop();
    storage.getResults.push("xyzzy");
    string message("no exception");
    try {
        manager.init();
    } catch (FatalError& e) {
        message = e.message;
    }
    EXPECT_EQ("format error in \"coordinatorUpdateManager\" object in "
            "external storage", message);
}
TEST_F(CoordinatorUpdateManagerTest, init_success) {
    manager.init();
    EXPECT_EQ(20ul, manager.externalLastFinished);
    EXPECT_EQ(1000ul, manager.externalFirstAvailable);
    EXPECT_EQ(1000ul, manager.smallestUnfinished);
    EXPECT_EQ(999ul, manager.lastAssigned);
    EXPECT_EQ("init: initializing CoordinatorUpdateManager: lastFinished = 20, "
            "firstAvailable = 1000",
            TestLog::get());
}

TEST_F(CoordinatorUpdateManagerTest, recoveryComplete) {
    CoordinatorUpdateManager manager2(&storage);
    EXPECT_FALSE(manager2.recoveryComplete);
    manager2.init();
    EXPECT_FALSE(manager2.recoveryComplete);
    manager2.recoveryFinished();
    EXPECT_TRUE(manager2.recoveryComplete);
}

TEST_F(CoordinatorUpdateManagerTest, nextSequenceNumber) {
    manager.init();
    TestLog::reset();
    storage.log.clear();
    EXPECT_EQ(1000ul, manager.nextSequenceNumber());
    EXPECT_EQ("set(UPDATE, coordinatorUpdateManager)",
            storage.log);
    EXPECT_EQ("1000 -- F", toString());

    storage.log.clear();
    EXPECT_EQ(1001ul, manager.nextSequenceNumber());
    EXPECT_EQ(1002ul, manager.nextSequenceNumber());
    EXPECT_EQ("", storage.log);
    EXPECT_EQ("1000 -- FFF", toString());
}

TEST_F(CoordinatorUpdateManagerTest, updateFinished_basics) {
    manager.init();
    for (int i = 0; i < 10; i++) {
        manager.nextSequenceNumber();
    }
    TestLog::reset();
    storage.log.clear();

    manager.updateFinished(1004);
    manager.updateFinished(1005);
    manager.updateFinished(1003);
    manager.updateFinished(1001);
    EXPECT_EQ("", storage.log);
    EXPECT_EQ("1000 -- FTFTTTFFFF", toString());

    manager.updateFinished(1000);
    EXPECT_EQ("", storage.log);
    EXPECT_EQ("1002 -- FTTTFFFF", toString());
}
TEST_F(CoordinatorUpdateManagerTest, updateFinished_sync) {
    manager.init();
    for (int i = 0; i < 10; i++) {
        manager.nextSequenceNumber();
    }
    TestLog::reset();
    storage.log.clear();

    manager.externalLastFinished = 902;
    manager.updateFinished(1000);
    EXPECT_EQ("", storage.log);
    manager.updateFinished(1001);
    EXPECT_EQ("", storage.log);
    manager.updateFinished(1002);
    EXPECT_EQ("set(UPDATE, coordinatorUpdateManager)", storage.log);
    storage.log.clear();
    manager.updateFinished(1003);
    EXPECT_EQ("", storage.log);
}

TEST_F(CoordinatorUpdateManagerTest, sync_normal) {
    manager.init();
    manager.nextSequenceNumber();
    EXPECT_EQ("lastFinished: 999, firstAvailable: 2000", configString());

    CoordinatorUpdateManager::Lock lock(manager.mutex);
    manager.smallestUnfinished = 2000;
    manager.lastAssigned = 2500;
    manager.sync(lock);
    EXPECT_EQ("lastFinished: 1999, firstAvailable: 3500", configString());
}
TEST_F(CoordinatorUpdateManagerTest, sync_recoveryNotFinished) {
    manager.externalLastFinished = 100;
    manager.externalFirstAvailable = 1000;
    manager.smallestUnfinished = 800;
    manager.lastAssigned = 900;
    manager.recoveryComplete = false;
    {
        CoordinatorUpdateManager::Lock lock(manager.mutex);
        manager.sync(lock);
    }
    EXPECT_EQ("lastFinished: 100, firstAvailable: 1900", configString());
    manager.recoveryFinished();
    EXPECT_EQ("lastFinished: 799, firstAvailable: 1900", configString());
}

}  // namespace RAMCloud
