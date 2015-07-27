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

#if ENABLE_ZOOKEEPER

#include "TestUtil.h"
#include "ZooStorage.h"

namespace RAMCloud {
class ZooStorageTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Dispatch dispatch;
    Tub<ZooStorage> zoo;

    // Send ZeroKeeper log output to /dev/null so it doesn't clutter the
    // console output.
    FILE* devNull;

    ZooStorageTest()
        : logEnabler()
        , dispatch(false)
        , zoo()
        , devNull(NULL)
    {
        devNull = fopen("/dev/null", "w");
        zoo_set_log_stream(devNull);

        string info("localhost:2181");
        zoo.construct(info, &dispatch);

        // For most tests it's easier if the lease renewer never runs.
        zoo->renewLeaseIntervalCycles = 0;

        zoo->remove("/test");
        TestLog::reset();
    }

    ~ZooStorageTest()
    {
        if (zoo) {
            ZooStorage::Lock lock(zoo->mutex);
            zoo->close(lock);
        }
        zoo_set_log_stream(NULL);
        if (devNull) {
            fclose(devNull);
        }
    }

    struct CompareObjects {
        bool operator()(ZooStorage::Object const & a,
                ZooStorage::Object const & b) const
        {
            return strcmp(a.name, b.name) < 0;
        }
    };

    // Sort the results from a getChildren call by name (to eliminate
    // dependencies on the order in which results are returned).
    void sort(vector<ZooStorage::Object>* children)
    {
        // gcc 4.4 doesn't seem to be able to sort vectors of movable but not
        // copyable objects
#if 0
        std::sort(children->begin(), children->end(), CompareObjects());
#else
        // stupid insertion sort
        vector<ZooStorage::Object> sorted;
        while (!children->empty()) {
            auto it = std::min_element(children->begin(), children->end(),
                                       CompareObjects());
            sorted.emplace_back(std::move(*it));
            children->erase(it);
        }
        std::swap(sorted, *children);
#endif
    }

    // Pretty-print the results from a getChildren call (collect all names
    // and values into a single string).
    string toString(vector<ZooStorage::Object>* children)
    {
        string result;
        for (size_t i = 0; i < children->size(); i++) {
            if (result.length() != 0) {
                result += ", ";
            }
            ZooStorage::Object* o = &children->at(i);
            result.append(o->name);
            result.append(": ");
            if (o->value == NULL) {
                result.append("-");
            } else {
                result.append(o->value, static_cast<size_t>(o->length));
            }
        }
        return result;
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(ZooStorageTest);
};

TEST_F(ZooStorageTest, sanityCheck) {
    Buffer value;
    zoo->set(ExternalStorage::Hint::CREATE, "/test/var1", "value1");
    EXPECT_TRUE(zoo->get("/test/var1", &value));
    EXPECT_EQ("value1", TestUtil::toString(&value));
}

TEST_F(ZooStorageTest, becomeLeader) {
    Buffer value;
    zoo->set(ExternalStorage::Hint::CREATE, "/test/leader", "locator:old");
    uint64_t before = Cycles::rdtsc();
    zoo->becomeLeader("/test/leader", "locator:new");
    double elapsed = Cycles::toSeconds(Cycles::rdtsc() - before);
    EXPECT_GT(elapsed, .200);
    EXPECT_EQ("checkLeader: Became leader with version 1 (old leader info "
            "was \"locator:old\")", TestLog::get());
    zoo->get("/test/leader", &value);
    EXPECT_EQ("locator:new", TestUtil::toString(&value));
}

TEST_F(ZooStorageTest, get_lostLeadership) {
    zoo->lostLeadership = true;
    Buffer value;
    EXPECT_THROW(zoo->get("/test/var1", &value),
                ExternalStorage::LostLeadershipException);
}
TEST_F(ZooStorageTest, get_noSuchObject) {
    Buffer value;
    value.fillFromString("abc");
    EXPECT_FALSE(zoo->get("/test/var1", &value));
    EXPECT_EQ("", TestUtil::toString(&value));
}
TEST_F(ZooStorageTest, get_initialBufferTooSmall) {
    Buffer value1, value2;
    TestUtil::fillLargeBuffer(&value1, 5000);
    zoo->set(ExternalStorage::Hint::CREATE, "/test/var1",
                static_cast<const char*>(value1.getRange(0, 5000)), 5000);
    EXPECT_TRUE(zoo->get("/test/var1", &value2));
    EXPECT_EQ("ok", TestUtil::checkLargeBuffer(&value2, 5000));
}
TEST_F(ZooStorageTest, get_valueFound) {
    Buffer value;
    zoo->set(ExternalStorage::Hint::CREATE, "/test/var1", "value1");
    EXPECT_TRUE(zoo->get("/test/var1", &value));
    EXPECT_EQ("value1", TestUtil::toString(&value));
}

TEST_F(ZooStorageTest, getChildren_basics) {
    vector<ZooStorage::Object> children;
    zoo->set(ExternalStorage::Hint::CREATE, "/test/var1", "value1");
    zoo->set(ExternalStorage::Hint::CREATE, "/test/var2", "value2");
    zoo->set(ExternalStorage::Hint::CREATE, "/test/var3", "value3");
    zoo->getChildren("/test", &children);
    EXPECT_EQ(3u, children.size());
    sort(&children);
    EXPECT_EQ("/test/var1: value1, /test/var2: value2, /test/var3: value3",
            toString(&children));
}
TEST_F(ZooStorageTest, getChildren_lostLeadership) {
    zoo->lostLeadership = true;
    vector<ZooStorage::Object> children;
    EXPECT_THROW(zoo->getChildren("/test", &children),
                ExternalStorage::LostLeadershipException);
}
TEST_F(ZooStorageTest, getChildren_noSuchObject) {
    vector<ZooStorage::Object> children;
    children.emplace_back("a", "b", 1);
    zoo->getChildren("/test/bogus", &children);
    EXPECT_EQ(0u, children.size());
}
TEST_F(ZooStorageTest, getChildren_recoverableErrorReadingNames) {
    vector<ZooStorage::Object> children;
    zoo->set(ExternalStorage::Hint::CREATE, "/test/var1", "value1");
    zoo->set(ExternalStorage::Hint::CREATE, "/test/var2", "value2");
    zoo->testStatus1 = ZINVALIDSTATE;
    zoo->getChildren("/test", &children);
    EXPECT_EQ(2u, children.size());
    sort(&children);
    EXPECT_EQ("/test/var1: value1, /test/var2: value2",
            toString(&children));
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "getChildren: Retrying after invalid zhandle state error "
            "getting children of /test"));
}
TEST_F(ZooStorageTest, getChildren_unrecoverableErrorReadingNames) {
    vector<ZooStorage::Object> children;
    zoo->testStatus1 = ZBADARGUMENTS;
    EXPECT_THROW(zoo->getChildren("ignored", &children), FatalError);
    EXPECT_EQ("handleError: ZooKeeper API error: bad arguments",
            TestLog::get());
}
TEST_F(ZooStorageTest, getChildren_recoverableErrorReadingValues) {
    vector<ZooStorage::Object> children;
    zoo->set(ExternalStorage::Hint::CREATE, "/test/var1", "value1");
    zoo->set(ExternalStorage::Hint::CREATE, "/test/var2", "value2");
    zoo->testStatus2 = ZOPERATIONTIMEOUT;
    zoo->getChildren("/test", &children);
    EXPECT_EQ(2u, children.size());
    sort(&children);
    EXPECT_EQ("/test/var1: value1, /test/var2: value2",
            toString(&children));
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "getChildren: Retrying after operation timeout error "
            "reading child /test/var1"));
}
TEST_F(ZooStorageTest, getChildren_mustGrowBuffer) {
    vector<ZooStorage::Object> children;
    zoo->set(ExternalStorage::Hint::CREATE, "/test/v1", "value1");
    Buffer big;
    TestUtil::fillLargeBuffer(&big, 4000);
    zoo->set(ExternalStorage::Hint::CREATE, "/test/v2",
                static_cast<const char*>(big.getRange(0, 4000)), 4000);
    zoo->getChildren("/test", &children);
    sort(&children);
    Buffer result;
    result.appendCopy(children[1].value, children[1].length);
    EXPECT_EQ("ok", TestUtil::checkLargeBuffer(&result, 4000));
}
TEST_F(ZooStorageTest, getChildren_weirdNodes) {
    // This test tries a node with no data, and one with an empty string.
    vector<ZooStorage::Object> children;
    zoo->set(ExternalStorage::Hint::CREATE, "/test/v1", "normal");
    zoo->set(ExternalStorage::Hint::CREATE, "/test/child/v2", "value2");
    zoo->set(ExternalStorage::Hint::CREATE, "/test/empty", "");
    zoo->getChildren("/test", &children);
    sort(&children);
    EXPECT_EQ("/test/child: -, /test/empty: -, /test/v1: normal",
            toString(&children));
}

TEST_F(ZooStorageTest, remove_lostLeadership) {
    zoo->lostLeadership = true;
    EXPECT_THROW(zoo->remove("/test/var1"),
                ExternalStorage::LostLeadershipException);
}
TEST_F(ZooStorageTest, removeInternal_singleObject) {
    Buffer value;
    zoo->set(ExternalStorage::Hint::CREATE, "/test/var1", "value1");
    zoo->remove("/test/var1");
    EXPECT_FALSE(zoo->get("/test/var1", &value));
}
TEST_F(ZooStorageTest, removeInternal_nonexistentObject) {
    Buffer value;
    zoo->remove("/test/var1");
    EXPECT_FALSE(zoo->get("/test/var1", &value));
}
TEST_F(ZooStorageTest, removeInternal_recoverableErrorInDelete) {
    Buffer value;
    zoo->set(ExternalStorage::Hint::CREATE, "/test/var1", "value1");
    zoo->testStatus1 = ZUNIMPLEMENTED;
    zoo->remove("/test/var1");
    EXPECT_FALSE(zoo->get("/test/var1", &value));
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "removeInternal: Retrying after unimplemented error "
            "deleting /test/var1"));
}
TEST_F(ZooStorageTest, removeInternal_recoverableErrorReadingChildrenNames) {
    Buffer value;
    zoo->set(ExternalStorage::Hint::CREATE, "/test/var1", "value1");
    zoo->testStatus2 = ZUNIMPLEMENTED;
    zoo->remove("/test");
    EXPECT_FALSE(zoo->get("/test", &value));
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "Retrying after unimplemented error reading children "
            "of /test for removal"));
}
TEST_F(ZooStorageTest, removeInternal_withChildren) {
    Buffer value;
    zoo->set(ExternalStorage::Hint::CREATE, "/test/var1", "value1");
    zoo->set(ExternalStorage::Hint::CREATE, "/test/var2", "value2");
    zoo->set(ExternalStorage::Hint::CREATE, "/test/var3", "value3");
    zoo->remove("/test");
    EXPECT_FALSE(zoo->get("/test", &value));
}
TEST_F(ZooStorageTest, removeInternal_deeplyNested) {
    Buffer value;
    zoo->set(ExternalStorage::Hint::CREATE, "/test/a/b/c/v1", "value1");
    zoo->set(ExternalStorage::Hint::CREATE, "/test/a/b/c/v2", "value2");
    zoo->set(ExternalStorage::Hint::CREATE, "/test/a/b/v3", "value3");
    zoo->remove("/test");
    EXPECT_FALSE(zoo->get("/test", &value));
}

TEST_F(ZooStorageTest, set_lostLeadership) {
    zoo->lostLeadership = true;
    EXPECT_THROW(zoo->set(ExternalStorage::Hint::CREATE, "/test", "value1"),
                ExternalStorage::LostLeadershipException);
}
TEST_F(ZooStorageTest, setInternal_createSucess) {
    Buffer value;
    zoo->set(ExternalStorage::Hint::CREATE, "/test", "value1");
    EXPECT_TRUE(zoo->get("/test", &value));
    EXPECT_EQ("value1", TestUtil::toString(&value));
    EXPECT_EQ("", TestLog::get());
}
TEST_F(ZooStorageTest, setInternal_createParent) {
    Buffer value;
    zoo->set(ExternalStorage::Hint::CREATE, "/test/var1", "value1");
    EXPECT_TRUE(zoo->get("/test/var1", &value));
    EXPECT_EQ("value1", TestUtil::toString(&value));
}
TEST_F(ZooStorageTest, setInternal_createHintIncorrect) {
    Buffer value;
    zoo->set(ExternalStorage::Hint::CREATE, "/test/var1", "value1");
    zoo->set(ExternalStorage::Hint::CREATE, "/test/var1", "value2");
    EXPECT_TRUE(zoo->get("/test/var1", &value));
    EXPECT_EQ("value2", TestUtil::toString(&value));
    EXPECT_EQ("setInternal: Incorrect CREATE hint for \"/test/var1\": "
            "object already exists",
            TestLog::get());
}
TEST_F(ZooStorageTest, setInternal_recoverableCreateError) {
    Buffer value;
    zoo->testStatus1 = ZOPERATIONTIMEOUT;
    zoo->set(ExternalStorage::Hint::CREATE, "/test", "value1");
    EXPECT_TRUE(zoo->get("/test", &value));
    EXPECT_EQ("value1", TestUtil::toString(&value));
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "setInternal: Retrying after operation timeout error "
            "writing /test"));
}
TEST_F(ZooStorageTest, setInternal_updateSucess) {
    Buffer value;
    zoo->set(ExternalStorage::Hint::CREATE, "/test", "value1");
    zoo->set(ExternalStorage::Hint::UPDATE, "/test", "value2");
    EXPECT_TRUE(zoo->get("/test", &value));
    EXPECT_EQ("value2", TestUtil::toString(&value));
    EXPECT_EQ("", TestLog::get());
}
TEST_F(ZooStorageTest, setInternal_updateHintIncorrect) {
    Buffer value;
    zoo->set(ExternalStorage::Hint::UPDATE, "/test/var1", "value1");
    EXPECT_TRUE(zoo->get("/test/var1", &value));
    EXPECT_EQ("value1", TestUtil::toString(&value));
    EXPECT_EQ("setInternal: Incorrect UPDATE hint for \"/test/var1\": "
            "object doesn't exist",
            TestLog::get());
}
TEST_F(ZooStorageTest, setInternal_recoverableUpdateError) {
    Buffer value;
    zoo->set(ExternalStorage::Hint::CREATE, "/test", "value1");
    zoo->testStatus2 = ZOPERATIONTIMEOUT;
    zoo->set(ExternalStorage::Hint::UPDATE, "/test", "value2");
    EXPECT_TRUE(zoo->get("/test", &value));
    EXPECT_EQ("value2", TestUtil::toString(&value));
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "setInternal: Retrying after operation timeout error "
            "writing /test"));
}

TEST_F(ZooStorageTest, checkLeader_objectDoesntExist) {
    Buffer value;
    zoo->leaderObject = "/test";
    zoo->leaderInfo = "Leader Info";
    zoo->renewLeaseIntervalCycles = Cycles::fromSeconds(1000.0);
    {
        ZooStorage::Lock lock(zoo->mutex);
        EXPECT_TRUE(zoo->checkLeader(lock));
    }
    EXPECT_EQ("checkLeader: Became leader (no existing leader)",
                TestLog::get());
    zoo->get("/test", &value);
    EXPECT_EQ("Leader Info", TestUtil::toString(&value));
    EXPECT_TRUE(zoo->leaseRenewer->isRunning());
}
TEST_F(ZooStorageTest, checkLeader_createParentNode) {
    Buffer value;
    zoo->leaderObject = "/test/leader";
    zoo->leaderInfo = "Leader Info";
    EXPECT_FALSE(zoo->get("/test", &value));
    {
        ZooStorage::Lock lock(zoo->mutex);
        EXPECT_FALSE(zoo->checkLeader(lock));
    }
    EXPECT_TRUE(zoo->get("/test", &value));

    // Second attempt should succeed.
    {
        ZooStorage::Lock lock(zoo->mutex);
        EXPECT_TRUE(zoo->checkLeader(lock));
    }
    EXPECT_EQ("checkLeader: Became leader (no existing leader)",
                TestLog::get());
    zoo->get("/test/leader", &value);
    EXPECT_EQ("Leader Info", TestUtil::toString(&value));
}
TEST_F(ZooStorageTest, checkLeader_unrecoverableError) {
    ZooStorage::Lock lock(zoo->mutex);
    zoo->leaderObject = "bogusName";
    zoo->leaderInfo = "Leader Info";
    EXPECT_THROW(zoo->checkLeader(lock), FatalError);
    EXPECT_EQ("handleError: ZooKeeper API error: bad arguments",
            TestLog::get());
}
TEST_F(ZooStorageTest, checkLeader_leaderAlive) {
    Buffer value;
    zoo->leaderObject = "/test/leader";
    zoo->leaderInfo = "Leader Info";
    zoo->set(ExternalStorage::Hint::CREATE, "/test/leader", "old leader v1");
    {
        ZooStorage::Lock lock(zoo->mutex);
        EXPECT_FALSE(zoo->checkLeader(lock));
    }
    zoo->get("/test/leader", &value);
    EXPECT_EQ("old leader v1", TestUtil::toString(&value));
    int32_t oldVersion = zoo->leaderVersion;

    // Try again, after updating the leader object.
    zoo->set(ExternalStorage::Hint::CREATE, "/test/leader", "old leader v2");
    {
        ZooStorage::Lock lock(zoo->mutex);
        EXPECT_FALSE(zoo->checkLeader(lock));
    }
    EXPECT_NE(oldVersion, zoo->leaderVersion);
    zoo->get("/test/leader", &value);
    EXPECT_EQ("old leader v2", TestUtil::toString(&value));
}
TEST_F(ZooStorageTest, checkLeader_oldLeaderInactive) {
    Buffer value;
    zoo->leaderObject = "/test/leader";
    zoo->leaderInfo = "Leader Info";
    zoo->renewLeaseIntervalCycles = Cycles::fromSeconds(1000.0);
    zoo->set(ExternalStorage::Hint::CREATE, "/test/leader", "locator:old");
    {
        ZooStorage::Lock lock(zoo->mutex);
        EXPECT_FALSE(zoo->checkLeader(lock));
        EXPECT_TRUE(zoo->checkLeader(lock));
    }
    EXPECT_EQ("checkLeader: Became leader with version 1 (old leader info "
            "was \"locator:old\")", TestLog::get());
    zoo->get("/test/leader", &value);
    EXPECT_EQ("Leader Info", TestUtil::toString(&value));
    EXPECT_TRUE(zoo->leaseRenewer->isRunning());
}

TEST_F(ZooStorageTest, close) {
    {
        ZooStorage::Lock lock(zoo->mutex);
        zoo->close(lock);
        EXPECT_TRUE(zoo->zoo == NULL);
        EXPECT_EQ("close: ZooKeeper connection closed",
                TestLog::get());
        TestLog::reset();
        zoo->close(lock);
        EXPECT_EQ("", TestLog::get());
    }
    zoo.destroy();
}

TEST_F(ZooStorageTest, createParent_severalLevels) {
    Buffer value;
    {
        ZooStorage::Lock lock(zoo->mutex);
        zoo->createParent(lock, "/test/a/b/c");

    }
    EXPECT_TRUE(zoo->get("/test/a", &value));
    EXPECT_TRUE(zoo->get("/test/a/b", &value));
    EXPECT_FALSE(zoo->get("/test/a/b/c", &value));
}
TEST_F(ZooStorageTest, createParent_bogusName) {
    ZooStorage::Lock lock(zoo->mutex);
    Buffer value;
    EXPECT_THROW(zoo->createParent(lock, "noSlashes"), FatalError);
    EXPECT_EQ("createParent: ZooKeeper node name \"noSlashes\" "
            "contains no slashes",
            TestLog::get());
}

TEST_F(ZooStorageTest, handleError_reopenConnection) {
    ZooStorage::Lock lock(zoo->mutex);
    EXPECT_NO_THROW(zoo->handleError(lock, ZINVALIDSTATE));
    EXPECT_EQ("handleError: ZooKeeper error (invalid zhandle state): "
            "reopening connection | close: ZooKeeper connection closed | "
            "open: ZooKeeper connection opened with localhost:2181",
            TestLog::get());
    TestLog::reset();
    EXPECT_NO_THROW(zoo->handleError(lock, ZSYSTEMERROR));
    EXPECT_EQ("handleError: ZooKeeper error (system error): "
            "reopening connection | close: ZooKeeper connection closed | "
            "open: ZooKeeper connection opened with localhost:2181",
            TestLog::get());
    TestLog::reset();
    EXPECT_NO_THROW(zoo->handleError(lock, ZSESSIONEXPIRED));
    EXPECT_EQ("handleError: ZooKeeper error (session expired): "
            "reopening connection | close: ZooKeeper connection closed | "
            "open: ZooKeeper connection opened with localhost:2181",
            TestLog::get());
}
TEST_F(ZooStorageTest, handleError_throwError) {
    ZooStorage::Lock lock(zoo->mutex);
    EXPECT_THROW(zoo->handleError(lock, ZBADARGUMENTS), FatalError);
    EXPECT_EQ("handleError: ZooKeeper API error: bad arguments",
            TestLog::get());
    TestLog::reset();
    EXPECT_THROW(zoo->handleError(lock, ZNONODE), FatalError);
    EXPECT_EQ("handleError: ZooKeeper API error: no node",
            TestLog::get());
}

// No tests for open: I can't figure out how to test this method.

TEST_F(ZooStorageTest, renewLease_basics) {
    zoo->renewLeaseIntervalCycles = Cycles::fromSeconds(1000.0);
    zoo->becomeLeader("/test/leader", "Old leader info");
    zoo->leaderInfo = "New leader info";
    {
        ZooStorage::Lock lock(zoo->mutex);
        EXPECT_TRUE(zoo->renewLease(lock));
    }
    Buffer value;
    zoo->get("/test/leader", &value);
    EXPECT_EQ("New leader info", TestUtil::toString(&value));
    {
        ZooStorage::Lock lock(zoo->mutex);
        EXPECT_TRUE(zoo->renewLease(lock));
    }
    EXPECT_EQ(2, zoo->leaderVersion);
}
TEST_F(ZooStorageTest, renewLease_versionMismatchButDataUnchanged) {
    zoo->renewLeaseIntervalCycles = Cycles::fromSeconds(1000.0);
    zoo->becomeLeader("/test/leader", "Old leader info");
    TestLog::reset();
    zoo->set(ZooStorage::Hint::UPDATE, "/test/leader", "Old leader info", -1);
    {
        ZooStorage::Lock lock(zoo->mutex);
        EXPECT_FALSE(zoo->renewLease(lock));
    }
    EXPECT_EQ("renewLease: False positive for leadership loss; updating "
            "version from 0 to 1", TestLog::get());
}
TEST_F(ZooStorageTest, renewLease_versionMismatchAndDataChanged) {
    zoo->renewLeaseIntervalCycles = Cycles::fromSeconds(1000.0);
    zoo->becomeLeader("/test/leader", "Old leader info");
    TestLog::reset();
    zoo->set(ZooStorage::Hint::UPDATE, "/test/leader", "New leader info", -1);
    {
        ZooStorage::Lock lock(zoo->mutex);
        EXPECT_TRUE(zoo->renewLease(lock));
    }
    EXPECT_EQ("renewLease: Lost ZooKeeper leadership; current leader info: "
            "New leader info, version: 1, our version: 0",
            TestLog::get());
    EXPECT_TRUE(zoo->lostLeadership);
}
TEST_F(ZooStorageTest, renewLease_missingNodeWhileReading) {
    zoo->renewLeaseIntervalCycles = Cycles::fromSeconds(1000.0);
    zoo->becomeLeader("/test/leader", "Old leader info");
    zoo->set(ZooStorage::Hint::UPDATE, "/test/leader", "New leader info", -1);
    zoo->testStatus2 = ZNONODE;
    TestLog::reset();
    {
        ZooStorage::Lock lock(zoo->mutex);
        EXPECT_FALSE(zoo->renewLease(lock));
    }
    EXPECT_EQ("", TestLog::get());
    EXPECT_FALSE(zoo->lostLeadership);
}
TEST_F(ZooStorageTest, renewLease_errorWhileReading) {
    zoo->renewLeaseIntervalCycles = Cycles::fromSeconds(1000.0);
    zoo->becomeLeader("/test/leader", "Old leader info");
    zoo->set(ZooStorage::Hint::UPDATE, "/test/leader", "New leader info", -1);
    zoo->testStatus2 = ZSESSIONEXPIRED;
    TestLog::reset();
    {
        ZooStorage::Lock lock(zoo->mutex);
        EXPECT_FALSE(zoo->renewLease(lock));
    }
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "renewLease: Retrying after session expired error while "
            "reading /test/leader"));
    EXPECT_FALSE(zoo->lostLeadership);
}
TEST_F(ZooStorageTest, renewLease_leaderObjectDeleted) {
    zoo->renewLeaseIntervalCycles = Cycles::fromSeconds(1000.0);
    zoo->becomeLeader("/test/leader", "Old leader info");
    TestLog::reset();
    zoo->remove("/test/leader");
    {
        ZooStorage::Lock lock(zoo->mutex);
        EXPECT_TRUE(zoo->renewLease(lock));
    }
    EXPECT_EQ("renewLease: renewLease: Lost ZooKeeper leadership; leader "
            "object /test/leader no longer exists",
            TestLog::get());
    EXPECT_TRUE(zoo->lostLeadership);
}
TEST_F(ZooStorageTest, renewLease_errorDuringSetOperation) {
    zoo->renewLeaseIntervalCycles = Cycles::fromSeconds(1000.0);
    zoo->becomeLeader("/test/leader", "Old leader info");
    zoo->testStatus1 = ZSESSIONEXPIRED;
    TestLog::reset();
    {
        ZooStorage::Lock lock(zoo->mutex);
        EXPECT_FALSE(zoo->renewLease(lock));
    }
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "renewLease: Retrying after session expired error while "
            "setting /test/leader"));
    EXPECT_FALSE(zoo->lostLeadership);
}

TEST_F(ZooStorageTest, stateString) {
    EXPECT_STREQ("connected", zoo->stateString(ZOO_CONNECTED_STATE));
    EXPECT_STREQ("expired session", zoo->stateString(
            ZOO_EXPIRED_SESSION_STATE));
    EXPECT_STREQ("authentication failed", zoo->stateString(
            ZOO_AUTH_FAILED_STATE));
    EXPECT_STREQ("connecting", zoo->stateString(ZOO_CONNECTING_STATE));
    EXPECT_STREQ("associating", zoo->stateString(ZOO_ASSOCIATING_STATE));
    EXPECT_STREQ("closed", zoo->stateString(0));
    EXPECT_STREQ("not connected", zoo->stateString(999));
    EXPECT_STREQ("unknown state", zoo->stateString(45));
}

TEST_F(ZooStorageTest, LeaseRenewer_handleTimerEvent) {
    // This test ensures that renewLease gets called multiple times
    // if needed (first time fixes the version mismatch, second time
    // renews the lease).
    zoo->renewLeaseIntervalCycles = Cycles::fromSeconds(1000.0);
    zoo->becomeLeader("/test/leader", "Old leader info");
    TestLog::reset();
    zoo->set(ZooStorage::Hint::UPDATE, "/test/leader", "Old leader info", -1);
    zoo->leaseRenewer->handleTimerEvent();
    EXPECT_EQ("renewLease: False positive for leadership loss; updating "
            "version from 0 to 1", TestLog::get());
    EXPECT_EQ(2, zoo->leaderVersion);
    EXPECT_TRUE(zoo->leaseRenewer->isRunning());
}

// Test whether ExternalStorage::open works with ZooStorage; this
// test logically belongs in ExternalStorageTest.cc, but it requires
// a ZooKeeper server, which is only available when tests in this
// file are run.
TEST_F(ZooStorageTest, ExternalStorage_open) {
    Context context;
    Buffer value;
    zoo->set(ExternalStorage::Hint::CREATE, "/test/var1", "value1");
    ExternalStorage* storage = ExternalStorage::open(
            "zk:localhost:2181", &context);
    EXPECT_TRUE(storage->get("/test/var1", &value));
    EXPECT_EQ("value1", TestUtil::toString(&value));
    delete storage;
}

}  // namespace RAMCloud

#endif // ENABLE_ZOOKEEPER
