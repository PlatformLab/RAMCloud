/* Copyright (c) 2013 Stanford University
 * Copyright (c) 2015 Diego Ongaro
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

#if ENABLE_LOGCABIN

#include <LogCabin/Client.h>
#include <LogCabin/Debug.h>

#include "TestUtil.h"
#include "LogCabinStorage.h"

namespace RAMCloud {
namespace {

typedef LogCabinStorage::BecomeLeaderState BecomeLeaderState;

/**
 * This is plugged into LogCabinStorage in place of usleep, which is useful for
 * testing becomeLeader() without taking too long or relying on timing.
 */
struct MockUsleep {
    explicit MockUsleep(LogCabinStorage* storage)
        : storage(storage)
        , numSleeps(0)
        , lastSleep(1337)
    {
    }
    virtual ~MockUsleep() {
    }
    void operator()(unsigned int micros) {
        ++numSleeps;
        lastSleep = micros;
        hook();
    }
    virtual void hook() {}
    LogCabinStorage* storage;
    uint64_t numSleeps;
    unsigned int lastSleep;
    DISALLOW_COPY_AND_ASSIGN(MockUsleep);
};

class LogCabinStorageTest : public ::testing::Test {
  public:
    LogCabinStorageTest()
        : storage()
        , mockUsleep()
    {
        LogCabin::Client::Debug::setLogPolicy(
            LogCabin::Client::Debug::logPolicyFromString(
                "WARNING"));
        // create mock cluster with in-process tree data structure
        storage.construct(LogCabin::Client::Cluster(
                std::shared_ptr<LogCabin::Client::TestingCallbacks>()));
        mockUsleep.reset(new MockUsleep(storage.get()));
        storage->mockableUsleep = std::ref(*mockUsleep);
        // For most tests it's easier if the lease renewer never runs.
        storage->renewLeaseIntervalMs = 0;
        storage->keepAliveKey = "/keepalive";
    }

    ~LogCabinStorageTest() {
    }

    // Pretty-print the results from a getChildren call (collect all names
    // and values into a single string).
    std::string toString(std::vector<ExternalStorage::Object>* children) {
        std::string result;
        for (size_t i = 0; i < children->size(); i++) {
            if (result.length() != 0) {
                result += ", ";
            }
            ExternalStorage::Object* o = &children->at(i);
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

    Tub<LogCabinStorage> storage;
    std::unique_ptr<MockUsleep> mockUsleep;

    DISALLOW_COPY_AND_ASSIGN(LogCabinStorageTest);
};

TEST_F(LogCabinStorageTest, sanityCheck) {
    Buffer value;
    storage->set(ExternalStorage::Hint::CREATE, "/test/var1", "value1");
    EXPECT_TRUE(storage->get("/test/var1", &value));
    EXPECT_EQ("value1", TestUtil::toString(&value));
}

TEST_F(LogCabinStorageTest, destructor) {
    // this test should exit quickly and shouldn't get to actually renewing the
    // lease
    storage->renewLeaseIntervalMs = 5000;
    storage->leaseRenewer = std::thread(&LogCabinStorage::leaseRenewerMain,
                                        storage.get(),
                                        LogCabinStorage::Clock::now());
    storage.destroy();
}

// This tests the becomeLeader() function itself, from INITIAL to
// OTHERS_LEASE_OBSERVED to OTHERS_LEASE_EXPIRED to LEASE_ACQUIRED.
// As if the prior leader had set a key but then died.
TEST_F(LogCabinStorageTest, becomeLeader_ownerKeySet_dead) {
    storage->checkLeaderIntervalMs = 300;
    Buffer value;
    storage->set(ExternalStorage::Hint::CREATE, "/test/leader", "locator:old");
    storage->becomeLeader("/test/leader", "locator:new");
    EXPECT_EQ(1U, mockUsleep->numSleeps);
    EXPECT_EQ(300000UL, mockUsleep->lastSleep);
    storage->getLeaderInfo("/test/leader", &value);
    EXPECT_EQ("locator:new", TestUtil::toString(&value));

    // check assignments at top of becomeLeader()
    EXPECT_EQ("/test/leader", storage->ownerKey);
    EXPECT_EQ("locator:new", storage->leaderInfo);
    EXPECT_EQ("/test/leader-keepalive", storage->keepAliveKey);
}

// Basic test for overall becomeLeader() when there is no
// owner key set.
TEST_F(LogCabinStorageTest, becomeLeader_noOwnerKey) {
    Buffer value;
    storage->becomeLeader("/test/leader", "locator:new");
    EXPECT_EQ(0U, mockUsleep->numSleeps);
    storage->getLeaderInfo("/test/leader", &value);
    EXPECT_EQ("locator:new", TestUtil::toString(&value));

}

/**
 * Helper for becomeLeader_ownerKeySet_alive_keepaliveNew and
 * becomeLeader_ownerKeySet_alive_keepaliveExist.
 */
struct MockUsleep_OwnerAlive : public MockUsleep {
    explicit MockUsleep_OwnerAlive(LogCabinStorage* storage)
        : MockUsleep(storage)
    {
    }
    virtual void hook() {
        if (numSleeps < 5) {
            storage->tree.write(storage->keepAliveKey,
                                format("sleep %lu", numSleeps));
        }
    }
};

// This is an older test for becomeLeader before it was broken apart. There may
// be some small value in keeping this test, so I've left it here for now. If
// it's causing a maintenance headache, it should probably be deleted.
TEST_F(LogCabinStorageTest, becomeLeader_ownerKeySet_alive_keepaliveNew) {
    mockUsleep.reset(new MockUsleep_OwnerAlive(storage.get()));
    storage->mockableUsleep = std::ref(*mockUsleep);
    storage->checkLeaderIntervalMs = 300;
    Buffer value;
    storage->set(ExternalStorage::Hint::CREATE, "/test/leader", "locator:old");
    storage->becomeLeader("/test/leader", "locator:new");
    EXPECT_EQ(5U, mockUsleep->numSleeps);
    EXPECT_EQ(300000UL, mockUsleep->lastSleep);
    storage->getLeaderInfo("/test/leader", &value);
    EXPECT_EQ("locator:new", TestUtil::toString(&value));
}

// This is an older test for becomeLeader before it was broken apart. There may
// be some small value in keeping this test, so I've left it here for now. If
// it's causing a maintenance headache, it should probably be deleted.
TEST_F(LogCabinStorageTest, becomeLeader_ownerKeySet_alive_keepaliveExist) {
    mockUsleep.reset(new MockUsleep_OwnerAlive(storage.get()));
    storage->mockableUsleep = std::ref(*mockUsleep);
    storage->checkLeaderIntervalMs = 300;
    Buffer value;
    storage->set(ExternalStorage::Hint::CREATE,
                 "/test/leader", "locator:old");
    storage->set(ExternalStorage::Hint::CREATE,
                 storage->keepAliveKey.c_str(), "zero");
    storage->becomeLeader("/test/leader", "locator:new");
    EXPECT_EQ(5U, mockUsleep->numSleeps);
    EXPECT_EQ(300000UL, mockUsleep->lastSleep);
    storage->getLeaderInfo("/test/leader", &value);
    EXPECT_EQ("locator:new", TestUtil::toString(&value));
}

/**
 * Helper for becomeLeader_ownerKeyChanged.
 */
struct MockUsleep_OwnerKeyChanged : public MockUsleep {
    explicit MockUsleep_OwnerKeyChanged(LogCabinStorage* storage)
        : MockUsleep(storage)
    {
    }
    virtual void hook() {
        if (numSleeps < 5) {
            storage->tree.write(storage->keepAliveKey,
                                format("sleep %lu", numSleeps));
        } else if (numSleeps == 5) {
            storage->tree.write("/test/leader", "locator:other");
        }
    }
};

// This is an older test for becomeLeader before it was broken apart. There may
// be some small value in keeping this test, so I've left it here for now. If
// it's causing a maintenance headache, it should probably be deleted.
// owner key set, owner changed while reading nth keepalive
TEST_F(LogCabinStorageTest, becomeLeader_ownerKeyChanged) {
    mockUsleep.reset(new MockUsleep_OwnerKeyChanged(storage.get()));
    storage->mockableUsleep = std::ref(*mockUsleep);
    Buffer value;
    storage->set(ExternalStorage::Hint::CREATE, "/test/leader", "locator:old");
    storage->becomeLeader("/test/leader", "locator:new");
    EXPECT_EQ(6U, mockUsleep->numSleeps);
    storage->getLeaderInfo("/test/leader", &value);
    EXPECT_EQ("locator:new", TestUtil::toString(&value));
}

TEST_F(LogCabinStorageTest, get_lostLeadership) {
    storage->tree.setCondition("/test/leader", "me");
    Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
    Buffer value;
    EXPECT_THROW(storage->get("/test/var1", &value),
                 FatalError);
}

TEST_F(LogCabinStorageTest, get_noSuchObject) {
    Buffer value;
    value.fillFromString("abc");
    EXPECT_FALSE(storage->get("/test/var1", &value));
    EXPECT_EQ("", TestUtil::toString(&value));
}

TEST_F(LogCabinStorageTest, get_valueFound) {
    Buffer value;
    storage->set(ExternalStorage::Hint::CREATE, "/test/var1", "value1");
    EXPECT_TRUE(storage->get("/test/var1", &value));
    EXPECT_EQ("value1", TestUtil::toString(&value));
}

TEST_F(LogCabinStorageTest, get_directory) {
    Buffer value;
    storage->set(ExternalStorage::Hint::CREATE, "/test/var1/x", "value1");
    EXPECT_TRUE(storage->get("/test/var1", &value));
    EXPECT_EQ("", TestUtil::toString(&value));
}

TEST_F(LogCabinStorageTest, getChildren_basics) {
    std::vector<ExternalStorage::Object> children;
    storage->set(ExternalStorage::Hint::CREATE, "/test/var1", "value1");
    storage->set(ExternalStorage::Hint::CREATE, "/test/var2", "value2");
    storage->set(ExternalStorage::Hint::CREATE, "/test/var3", "value3");
    storage->getChildren("/test", &children);
    EXPECT_EQ(3u, children.size());
    EXPECT_EQ("/test/var1: value1, /test/var2: value2, /test/var3: value3",
            toString(&children));
}

TEST_F(LogCabinStorageTest, getChildren_lostLeadership) {
    storage->tree.setCondition("/test/leader", "me");
    Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
    std::vector<ExternalStorage::Object> children;
    EXPECT_THROW(storage->getChildren("/test", &children),
                 FatalError);
}

TEST_F(LogCabinStorageTest, getChildren_noSuchObject) {
    std::vector<ExternalStorage::Object> children;
    children.emplace_back("a", "b", 1);
    storage->getChildren("/test/bogus", &children);
    EXPECT_EQ(0u, children.size());
}

TEST_F(LogCabinStorageTest, getChildren_weirdNodes) {
    // This test tries a node with no data, and one with an empty string.
    std::vector<ExternalStorage::Object> children;
    storage->set(ExternalStorage::Hint::CREATE, "/test/v1", "normal");
    storage->set(ExternalStorage::Hint::CREATE, "/test/child/v2", "value2");
    storage->set(ExternalStorage::Hint::CREATE, "/test/empty", "");
    storage->getChildren("/test", &children);
    EXPECT_EQ("/test/child: -, /test/empty: -, /test/v1: normal",
              toString(&children));
}

TEST_F(LogCabinStorageTest, getLeaderInfo_ok) {
    storage->becomeLeader("/test/leader", "locator:new");
    Buffer value;
    storage->getLeaderInfo("/test/leader", &value);
    EXPECT_EQ("locator:new", TestUtil::toString(&value));
}

TEST_F(LogCabinStorageTest, getLeaderInfo_misc) {
    // not sure what the return value should be, but at least it shouldn't
    // crash
    storage->set(ExternalStorage::Hint::CREATE, "/test/leader", "123");
    Buffer value;
    storage->getLeaderInfo("/test/leader", &value);
    EXPECT_EQ("123", TestUtil::toString(&value));
}

TEST_F(LogCabinStorageTest, remove_lostLeadership) {
    storage->tree.setCondition("/test/leader", "me");
    Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
    EXPECT_THROW(storage->remove("/test/var1"),
                 FatalError);
}

TEST_F(LogCabinStorageTest, remove_file) {
    Buffer value;
    storage->set(ExternalStorage::Hint::CREATE, "/test/var1", "value1");
    storage->remove("/test/var1");
    EXPECT_FALSE(storage->get("/test/var1", &value));
}

TEST_F(LogCabinStorageTest, remove_directory) {
    Buffer value;
    storage->set(ExternalStorage::Hint::CREATE, "/test/var1/x", "value1");
    storage->remove("/test/var1");
    EXPECT_FALSE(storage->get("/test/var1", &value));
}

TEST_F(LogCabinStorageTest, remove_nonexistent) {
    Buffer value;
    storage->remove("/test/var1");
    EXPECT_FALSE(storage->get("/test/var1", &value));
}

TEST_F(LogCabinStorageTest, set_lostLeadership) {
    storage->tree.setCondition("/test/leader", "me");
    Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
    EXPECT_THROW(storage->set(ExternalStorage::Hint::CREATE,
                              "/test", "value1"),
                 FatalError);
}

TEST_F(LogCabinStorageTest, set_sucess) {
    Buffer value;
    storage->set(ExternalStorage::Hint::CREATE, "/test", "value1");
    EXPECT_TRUE(storage->get("/test", &value));
    EXPECT_EQ("value1", TestUtil::toString(&value));
}

TEST_F(LogCabinStorageTest, set_createParent) {
    Buffer value;
    storage->set(ExternalStorage::Hint::CREATE, "/test/var1", "value1");
    EXPECT_TRUE(storage->get("/test/var1", &value));
    EXPECT_EQ("value1", TestUtil::toString(&value));
}

TEST_F(LogCabinStorageTest, set_lengthProvided) {
    Buffer value;
    storage->set(ExternalStorage::Hint::CREATE, "/test", "value1", 3);
    EXPECT_TRUE(storage->get("/test", &value));
    EXPECT_EQ("val", TestUtil::toString(&value));
}

TEST_F(LogCabinStorageTest, setWorkspace_lostLeadership) {
    storage->tree.setCondition("/test/leader", "me");
    Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
    EXPECT_THROW(storage->setWorkspace("/a/"),
                 FatalError);
}

TEST_F(LogCabinStorageTest, setWorkspace_ok) {
    storage->setWorkspace("/test/leader/");
    storage->set(ExternalStorage::Hint::CREATE, "foo", "value1");
    Buffer value;
    EXPECT_TRUE(storage->get("foo", &value));
    EXPECT_EQ("value1", TestUtil::toString(&value));
    EXPECT_TRUE(storage->get("/test/leader/foo", &value));
    EXPECT_EQ("value1", TestUtil::toString(&value));
}

TEST_F(LogCabinStorageTest, readExistingLease_none) {
    storage->ownerKey = "/test/leader";
    storage->leaderInfo = "locator:new";
    storage->keepAliveKey = storage->ownerKey + "-keepalive";
    EXPECT_EQ(BecomeLeaderState::OTHERS_LEASE_EXPIRED,
              storage->readExistingLease());
    EXPECT_EQ(storage->ownerKey,
              storage->tree.getCondition().first);
    EXPECT_EQ("",
              storage->tree.getCondition().second);
}

TEST_F(LogCabinStorageTest, readExistingLease_ok) {
    storage->ownerKey = "/test/leader";
    storage->leaderInfo = "locator:new";
    storage->keepAliveKey = storage->ownerKey + "-keepalive";
    storage->keepAliveValue = "nonsense";
    storage->set(ExternalStorage::Hint::CREATE,
                 "/test/leader", "locator:old");
    EXPECT_EQ(BecomeLeaderState::OTHERS_LEASE_OBSERVED,
              storage->readExistingLease());
    EXPECT_EQ(storage->ownerKey,
              storage->tree.getCondition().first);
    EXPECT_EQ("locator:old",
              storage->tree.getCondition().second);
    EXPECT_EQ("", storage->keepAliveValue);

    storage->set(ExternalStorage::Hint::CREATE,
                 "/test/leader-keepalive", "rawr");
    EXPECT_EQ(BecomeLeaderState::OTHERS_LEASE_OBSERVED,
              storage->readExistingLease());
    EXPECT_EQ(storage->ownerKey,
              storage->tree.getCondition().first);
    EXPECT_EQ("locator:old",
              storage->tree.getCondition().second);
    EXPECT_EQ("rawr", storage->keepAliveValue);
}

// readExistingLease_ownerKeyChanged hard to test

TEST_F(LogCabinStorageTest, waitAndCheckLease_ok) {
    storage->checkLeaderIntervalMs = 300;
    storage->ownerKey = "/test/leader";
    storage->leaderInfo = "locator:new";
    storage->keepAliveKey = storage->ownerKey + "-keepalive";
    storage->keepAliveValue = "val1";
    storage->set(ExternalStorage::Hint::CREATE,
                 "/test/leader", "locator:old");
    storage->set(ExternalStorage::Hint::CREATE,
                 "/test/leader-keepalive", "val2");
    EXPECT_EQ(BecomeLeaderState::OTHERS_LEASE_OBSERVED,
              storage->waitAndCheckLease());
    EXPECT_EQ("val2",
              storage->keepAliveValue);
    EXPECT_EQ(1U, mockUsleep->numSleeps);
    EXPECT_EQ(300000UL, mockUsleep->lastSleep);

    EXPECT_EQ(BecomeLeaderState::OTHERS_LEASE_EXPIRED,
              storage->waitAndCheckLease());
    EXPECT_EQ(2U, mockUsleep->numSleeps);
}

TEST_F(LogCabinStorageTest, waitAndCheckLease_none) {
    storage->ownerKey = "/test/leader";
    storage->leaderInfo = "locator:new";
    storage->keepAliveKey = storage->ownerKey + "-keepalive";
    storage->keepAliveValue = "val1";
    storage->set(ExternalStorage::Hint::CREATE,
                 "/test/leader", "locator:old");
    EXPECT_EQ(BecomeLeaderState::OTHERS_LEASE_OBSERVED,
              storage->waitAndCheckLease());
    EXPECT_EQ("",
              storage->keepAliveValue);
    EXPECT_EQ(BecomeLeaderState::OTHERS_LEASE_EXPIRED,
              storage->waitAndCheckLease());
}

TEST_F(LogCabinStorageTest, waitAndCheckLease_ownerKeyChanged) {
    storage->ownerKey = "/test/leader";
    storage->leaderInfo = "locator:new";
    storage->keepAliveKey = storage->ownerKey + "-keepalive";
    storage->tree.setCondition("foo", "bar");
    EXPECT_EQ(BecomeLeaderState::INITIAL,
              storage->waitAndCheckLease());
}

TEST_F(LogCabinStorageTest, takeOverLease_ok) {
    storage->ownerKey = "/test/leader";
    storage->leaderInfo = "locator:new";
    storage->keepAliveKey = storage->ownerKey + "-keepalive";
    storage->tree.makeDirectoryEx("/test");
    MockRandom randomMocker(16);

    EXPECT_EQ(BecomeLeaderState::LEASE_ACQUIRED,
              storage->takeOverLease());
    EXPECT_EQ("0000000000000010:locator:new",
              storage->tree.readEx("/test/leader"));
    EXPECT_EQ("/test/leader",
              storage->tree.getCondition().first);
    EXPECT_EQ("0000000000000010:locator:new",
              storage->tree.getCondition().second);
}

TEST_F(LogCabinStorageTest, takeOverLease_makeParents_ok) {
    storage->ownerKey = "/test/foo/leader";
    storage->leaderInfo = "locator:new";
    storage->keepAliveKey = storage->ownerKey + "-keepalive";
    EXPECT_EQ(BecomeLeaderState::OTHERS_LEASE_EXPIRED,
              storage->takeOverLease());
    EXPECT_EQ(0U,
              storage->tree.listDirectoryEx("/test/foo/").size());
}

// takeOverLease_makeParents_ownerKeyChanged hard to test

TEST_F(LogCabinStorageTest, takeOverLease_ownerKeyChanged) {
    storage->ownerKey = "/test/foo/leader";
    storage->leaderInfo = "locator:new";
    storage->keepAliveKey = storage->ownerKey + "-keepalive";
    storage->tree.makeDirectoryEx("/test");
    storage->tree.setCondition("foo", "bar");
    EXPECT_EQ(BecomeLeaderState::INITIAL,
              storage->takeOverLease());
}

// leaseRenewerMain exiting behavior tested with destructor

TEST_F(LogCabinStorageTest, leaseRewnerMain_deadline) {
    storage->renewLeaseIntervalMs = 0;
    storage->set(ExternalStorage::Hint::CREATE,
                 storage->keepAliveKey.c_str(), "asdf");
    storage->tree.setCondition(storage->keepAliveKey, "asdf");
    // first renewLease will succeed, second will fail due to condition
    Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
    EXPECT_THROW(storage->leaseRenewerMain(LogCabinStorage::Clock::now()),
                 FatalError);
    EXPECT_GE(storage->expireLeaseIntervalMs * 1000UL * 1000UL,
              storage->lastTimeoutNs);
    EXPECT_LT(storage->expireLeaseIntervalMs * 1000UL * 500UL,
              storage->lastTimeoutNs) << "timing sensitive";
}

TEST_F(LogCabinStorageTest, makeParents_lostLeadership) {
    storage->tree.setCondition("/test/leader", "me");
    Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
    EXPECT_THROW(storage->makeParents("/a/b"),
                 FatalError);
}

TEST_F(LogCabinStorageTest, makeParents_severalLevels) {
    Buffer value;
    storage->makeParents("/test/a/b/c");
    EXPECT_TRUE(storage->get("/test/a", &value));
    EXPECT_TRUE(storage->get("/test/a/b", &value));
    EXPECT_FALSE(storage->get("/test/a/b/c", &value));
}

TEST_F(LogCabinStorageTest, makeParents_root) {
    Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
    EXPECT_THROW(storage->makeParents("/"), FatalError);
}

TEST_F(LogCabinStorageTest, renewLease_lostLeadership) {
    storage->tree.setCondition("/test/leader", "me");
    Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
    EXPECT_THROW(storage->renewLease(LogCabinStorage::TimePoint::max()),
                 FatalError);
}

TEST_F(LogCabinStorageTest, renewLease_timeout) {
    Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
    EXPECT_THROW(storage->renewLease(LogCabinStorage::Clock::now() -
                                     std::chrono::milliseconds(10)),
                 FatalError);
    EXPECT_EQ(1U, storage->lastTimeoutNs);
    EXPECT_THROW(storage->renewLease(LogCabinStorage::Clock::now()),
                 FatalError);
    EXPECT_EQ(1U, storage->lastTimeoutNs);
    // timeouts were set on copy of tree, original was left intact
    EXPECT_EQ(0U, storage->tree.getTimeout());
}

TEST_F(LogCabinStorageTest, renewLease_ok) {
    storage->set(ExternalStorage::Hint::CREATE,
                 storage->keepAliveKey.c_str(), "asdf");
    Buffer value;
    storage->get(storage->keepAliveKey.c_str(), &value);
    EXPECT_EQ("asdf", TestUtil::toString(&value));
    storage->renewLease(LogCabinStorage::Clock::now() +
                        std::chrono::seconds(10));
    storage->get(storage->keepAliveKey.c_str(), &value);
    EXPECT_NE("asdf", TestUtil::toString(&value));
    Buffer value2;
    storage->renewLease(LogCabinStorage::Clock::now() +
                        std::chrono::seconds(10));
    storage->get(storage->keepAliveKey.c_str(), &value2);
    EXPECT_NE(TestUtil::toString(&value),
              TestUtil::toString(&value2));
    EXPECT_GT(10000000000UL, storage->lastTimeoutNs) << "< 10s";
    EXPECT_LT(9000000000UL, storage->lastTimeoutNs) << "> 9s";
}

// Test whether ExternalStorage::open works with LogCabinStorage; this
// test logically belongs in ExternalStorageTest.cc, but it requires
// more intimate knowledge of LogCabin. It doesn't test a whole lot, really.
TEST_F(LogCabinStorageTest, ExternalStorage_open) {
    Context context;
    Buffer value;
    storage->set(ExternalStorage::Hint::CREATE, "/test/var1", "value1");
    std::unique_ptr<ExternalStorage> storage2(ExternalStorage::open(
            "lc:THIS-IS-NOT-A-VALID-HOSTNAME", &context));
    static_cast<LogCabinStorage*>(storage2.get())->cluster = storage->cluster;
    static_cast<LogCabinStorage*>(storage2.get())->tree = storage->tree;
    EXPECT_TRUE(storage2->get("/test/var1", &value));
    EXPECT_EQ("value1", TestUtil::toString(&value));
}

}  // namespace RAMCloud::<anonymous>
}  // namespace RAMCloud

#endif // ENABLE_LOGCABIN
