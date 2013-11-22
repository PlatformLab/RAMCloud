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

        // For many tests it's convenient to invoke ZooKeeper without first
        // becoming leader, so just pretend we're already leader.
        zoo->leader = true;

        // For most tests is easier if the lease renewer never runs.
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
        std::sort(children->begin(), children->end(), CompareObjects());
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
    EXPECT_EQ("checkLeader: Became leader (old leader info "
            "was \"locator:old\")", TestLog::get());
    zoo->get("/test/leader", &value);
    EXPECT_EQ("locator:new", TestUtil::toString(&value));
}

TEST_F(ZooStorageTest, get_notLeader) {
    zoo->leader = false;
    Buffer value;
    EXPECT_THROW(zoo->get("/test/var1", &value),
                ExternalStorage::NotLeaderException);
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
TEST_F(ZooStorageTest, getChildren_notLeader) {
    zoo->leader = false;
    vector<ZooStorage::Object> children;
    EXPECT_THROW(zoo->getChildren("/test", &children),
                ExternalStorage::NotLeaderException);
}
TEST_F(ZooStorageTest, getChildren_noSuchObject) {
    vector<ZooStorage::Object> children;
    children.emplace_back("a", "b", 1);
    zoo->getChildren("/test/bogus", &children);
    EXPECT_EQ(0u, children.size());
}
TEST_F(ZooStorageTest, getChildren_unrecoverableError) {
    vector<ZooStorage::Object> children;
    zoo->testName = "nameWithNoSlash";
    EXPECT_THROW(zoo->getChildren("ignored", &children), FatalError);
    EXPECT_EQ("handleError: ZooKeeper API error: bad arguments",
            TestLog::get());
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
    result.append(children[1].value, children[1].length);
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

TEST_F(ZooStorageTest, remove_notLeader) {
    zoo->leader = false;
    EXPECT_THROW(zoo->remove("/test/var1"),
                ExternalStorage::NotLeaderException);
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
TEST_F(ZooStorageTest, removeInternal_unrecoverableError) {
    zoo->testName = "nameWithNoSlash";
    EXPECT_THROW(zoo->remove("ignored"), FatalError);
    EXPECT_EQ("handleError: ZooKeeper API error: bad arguments",
            TestLog::get());
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

TEST_F(ZooStorageTest, set_notLeader) {
    zoo->leader = false;
    EXPECT_THROW(zoo->set(ExternalStorage::Hint::CREATE, "/test", "value1"),
                ExternalStorage::NotLeaderException);
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
    EXPECT_EQ("setInternal: CREATE hint for \"/test/var1\" ZooKeeper object "
            "was incorrect: object already exists",
            TestLog::get());
}
TEST_F(ZooStorageTest, setInternal_createError) {
    zoo->testName = "nameWithNoSlash";
    EXPECT_THROW(zoo->set(ExternalStorage::Hint::CREATE, "ignored",
            "value1"), FatalError);
    EXPECT_EQ("handleError: ZooKeeper API error: bad arguments",
            TestLog::get());
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
    EXPECT_EQ("setInternal: UPDATE hint for \"/test/var1\" ZooKeeper object "
            "was incorrect: object doesn't exist",
            TestLog::get());
}
TEST_F(ZooStorageTest, setInternal_updateError) {
    zoo->testName = "nameWithNoSlash";
    EXPECT_THROW(zoo->set(ExternalStorage::Hint::UPDATE, "ignored",
            "value1"), FatalError);
    EXPECT_EQ("handleError: ZooKeeper API error: bad arguments",
            TestLog::get());
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
    EXPECT_EQ("checkLeader: Became leader (old leader info "
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
            "open: ZooKeeper connection opened: localhost:2181",
            TestLog::get());
    TestLog::reset();
    EXPECT_NO_THROW(zoo->handleError(lock, ZSYSTEMERROR));
    EXPECT_EQ("handleError: ZooKeeper error (system error): "
            "reopening connection | close: ZooKeeper connection closed | "
            "open: ZooKeeper connection opened: localhost:2181",
            TestLog::get());
    TestLog::reset();
    EXPECT_NO_THROW(zoo->handleError(lock, ZSESSIONEXPIRED));
    EXPECT_EQ("handleError: ZooKeeper error (session expired): "
            "reopening connection | close: ZooKeeper connection closed | "
            "open: ZooKeeper connection opened: localhost:2181",
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

TEST_F(ZooStorageTest, LeaseRenewer_handleTimerEvent_basics) {
    zoo->renewLeaseIntervalCycles = Cycles::fromSeconds(1000.0);
    zoo->becomeLeader("/test/leader", "Old leader info");
    zoo->leaderInfo = "New leader info";
    zoo->leaseRenewer->handleTimerEvent();
    Buffer value;
    zoo->get("/test/leader", &value);
    EXPECT_EQ("New leader info", TestUtil::toString(&value));
    zoo->leaseRenewer->handleTimerEvent();
    EXPECT_EQ(2, zoo->leaderVersion);
}
TEST_F(ZooStorageTest, LeaseRenewer_handleTimerEvent_lostLease) {
    zoo->renewLeaseIntervalCycles = Cycles::fromSeconds(1000.0);
    zoo->becomeLeader("/test/leader", "Old leader info");

    // Simulate another leader overwriting the leader object to take control.
    zoo->set(ExternalStorage::Hint::UPDATE, "/test/leader", "New leader info");
    zoo->leaseRenewer->handleTimerEvent();
    EXPECT_FALSE(zoo->leader);
    EXPECT_EQ(0, zoo->leaderVersion);
}
TEST_F(ZooStorageTest, LeaseRenewer_handleTimerEvent_otherError) {
    zoo->renewLeaseIntervalCycles = Cycles::fromSeconds(1000.0);
    zoo->becomeLeader("/test/leader", "Leader info");

    // Delete the leader object to force an error.
    zoo->remove("/test/leader");
    EXPECT_THROW(zoo->leaseRenewer->handleTimerEvent(), FatalError);
    EXPECT_TRUE(zoo->leader);
}

}  // namespace RAMCloud
