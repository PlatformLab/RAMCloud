/* Copyright (c) 2014 Stanford University
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

#include "TestUtil.h"       //Has to be first, compiler complains
#include "LeaseManager.h"
#include "CoordinatorClusterClock.pb.h"
#include "MockExternalStorage.h"

namespace RAMCloud {

class LeaseManagerTest : public ::testing::Test {
  public:
    Context context;
    MockExternalStorage storage;
    Tub<LeaseManager> leaseMgr;

    LeaseManagerTest()
        : context()
        , storage(true)
        , leaseMgr()
    {
        // Add data to storage to affect recovered clock time.
        ProtoBuf::CoordinatorClusterClock info;
        info.set_next_safe_time(1000);
        std::string str;
        info.SerializeToString(&str);
        storage.getResults.push(str);

        context.externalStorage = &storage;
        leaseMgr.construct(&context);
        leaseMgr->lastIssuedLeaseId = 0;
        leaseMgr->maxAllocatedLeaseId = 0;
    }

    DISALLOW_COPY_AND_ASSIGN(LeaseManagerTest);
};

TEST_F(LeaseManagerTest, getLeaseInfo) {
    WireFormat::ClientLease lease;

    lease = leaseMgr->getLeaseInfo(25);
    EXPECT_EQ(0U, lease.leaseId);
    EXPECT_EQ(0U, lease.leaseTerm);

    leaseMgr->leaseMap[25] = 8888;

    lease = leaseMgr->getLeaseInfo(25);
    EXPECT_EQ(25U, lease.leaseId);
    EXPECT_EQ(8888U, lease.leaseTerm);
}

TEST_F(LeaseManagerTest, renewLease) {
    EXPECT_FALSE(leaseMgr->preallocator.isRunning());
    leaseMgr->maxAllocatedLeaseId = 1000;
    leaseMgr->renewLease(0);
    EXPECT_FALSE(leaseMgr->preallocator.isRunning());
    leaseMgr->lastIssuedLeaseId = 900;
    leaseMgr->renewLease(0);
    EXPECT_TRUE(leaseMgr->preallocator.isRunning());
}

TEST_F(LeaseManagerTest, startUpdaters) {
    EXPECT_FALSE(leaseMgr->preallocator.isRunning());
    EXPECT_FALSE(leaseMgr->cleaner.isRunning());
    EXPECT_FALSE(leaseMgr->clock.updater.isRunning());
    leaseMgr->startUpdaters();
    EXPECT_TRUE(leaseMgr->preallocator.isRunning());
    EXPECT_TRUE(leaseMgr->cleaner.isRunning());
    EXPECT_TRUE(leaseMgr->clock.updater.isRunning());
}

TEST_F(LeaseManagerTest, leasePreallocator_handleTimerEvent) {
    EXPECT_EQ(0U, leaseMgr->lastIssuedLeaseId);
    EXPECT_EQ(0U, leaseMgr->maxAllocatedLeaseId);
    leaseMgr->preallocator.handleTimerEvent();
    EXPECT_EQ(0U, leaseMgr->lastIssuedLeaseId);
    EXPECT_EQ(1000U, leaseMgr->maxAllocatedLeaseId);
    leaseMgr->lastIssuedLeaseId = 5;
    leaseMgr->preallocator.handleTimerEvent();
    EXPECT_EQ(5U, leaseMgr->lastIssuedLeaseId);
    EXPECT_EQ(1005U, leaseMgr->maxAllocatedLeaseId);
}

TEST_F(LeaseManagerTest, leaseCleaner_handleTimerEvent) {
    leaseMgr->leaseMap[25] = 0;
    leaseMgr->revLeaseMap[0].insert(25);
    leaseMgr->leaseMap[52] = 0;
    leaseMgr->revLeaseMap[0].insert(52);
    leaseMgr->revLeaseMap[1];
    leaseMgr->revLeaseMap[2];
    leaseMgr->leaseMap[88] = 3;
    leaseMgr->revLeaseMap[3].insert(88);
    leaseMgr->leaseMap[99] = 60000;
    leaseMgr->revLeaseMap[60000].insert(99);
    leaseMgr->revLeaseMap[60001];

    leaseMgr->cleaner.handleTimerEvent();

    EXPECT_EQ(1U, leaseMgr->leaseMap.size());
    EXPECT_EQ(2U, leaseMgr->revLeaseMap.size());
}

TEST_F(LeaseManagerTest, allocateNextLease) {
    LeaseManager::Lock lock(leaseMgr->mutex);
    storage.log.clear();
    leaseMgr->maxAllocatedLeaseId = 4294967296;
    EXPECT_EQ(4294967296U, leaseMgr->maxAllocatedLeaseId);
    leaseMgr->allocateNextLease(lock);
    EXPECT_EQ("set(CREATE, leaseManager/4294967297)", storage.log);
    EXPECT_EQ(4294967297U, leaseMgr->maxAllocatedLeaseId);
}

TEST_F(LeaseManagerTest, cleanNextLease) {
    // Time dependent test.
    leaseMgr->leaseMap[25] = 0;
    leaseMgr->revLeaseMap[0].insert(25);
    leaseMgr->leaseMap[4294967297] = 0;
    leaseMgr->revLeaseMap[0].insert(4294967297);
    leaseMgr->revLeaseMap[1];
    leaseMgr->revLeaseMap[2];

    EXPECT_EQ(2U, leaseMgr->leaseMap.size());
    EXPECT_EQ(3U, leaseMgr->revLeaseMap.size());

    storage.log.clear();
    EXPECT_TRUE(leaseMgr->cleanNextLease());
    EXPECT_EQ("remove(leaseManager/25)", storage.log);
    EXPECT_EQ(1U, leaseMgr->leaseMap.size());
    EXPECT_EQ(3U, leaseMgr->revLeaseMap.size());

    storage.log.clear();
    EXPECT_TRUE(leaseMgr->cleanNextLease());
    EXPECT_EQ("remove(leaseManager/4294967297)", storage.log);
    EXPECT_EQ(0U, leaseMgr->leaseMap.size());
    EXPECT_EQ(3U, leaseMgr->revLeaseMap.size());

    EXPECT_FALSE(leaseMgr->cleanNextLease());
    EXPECT_EQ(0U, leaseMgr->leaseMap.size());
    EXPECT_EQ(0U, leaseMgr->revLeaseMap.size());

    leaseMgr->revLeaseMap[1];
    leaseMgr->revLeaseMap[2];
    leaseMgr->leaseMap[88] = 3;
    leaseMgr->revLeaseMap[3].insert(88);
    leaseMgr->leaseMap[99] = 60000;
    leaseMgr->revLeaseMap[60000].insert(99);
    leaseMgr->revLeaseMap[60001];

    EXPECT_EQ(2U, leaseMgr->leaseMap.size());
    EXPECT_EQ(5U, leaseMgr->revLeaseMap.size());

    EXPECT_TRUE(leaseMgr->cleanNextLease());
    EXPECT_EQ(1U, leaseMgr->leaseMap.size());
    EXPECT_EQ(3U, leaseMgr->revLeaseMap.size());

    EXPECT_FALSE(leaseMgr->cleanNextLease());
    EXPECT_EQ(1U, leaseMgr->leaseMap.size());
    EXPECT_EQ(2U, leaseMgr->revLeaseMap.size());
}

TEST_F(LeaseManagerTest, recover) {
    EXPECT_EQ(0U, leaseMgr->leaseMap.size());
    EXPECT_EQ(0U, leaseMgr->revLeaseMap.size());

    EXPECT_EQ(0U, leaseMgr->leaseMap.size());
    EXPECT_EQ(0U, leaseMgr->revLeaseMap.size());

    storage.getChildrenNames.push("1");
    storage.getChildrenValues.push("");
    storage.getChildrenNames.push("25");
    storage.getChildrenValues.push("");
    storage.getChildrenNames.push("3");
    storage.getChildrenValues.push("");
    storage.getChildrenNames.push("700000");
    storage.getChildrenValues.push("");

    leaseMgr->recover();

    EXPECT_EQ(4U, leaseMgr->leaseMap.size());
    EXPECT_TRUE(leaseMgr->leaseMap.end() !=
                leaseMgr->leaseMap.find(1));
    EXPECT_TRUE(leaseMgr->revLeaseMap[leaseMgr->leaseMap[1]].end() !=
                leaseMgr->revLeaseMap[leaseMgr->leaseMap[1]].find(1));
    EXPECT_TRUE(leaseMgr->leaseMap.end() !=
                leaseMgr->leaseMap.find(25));
    EXPECT_TRUE(leaseMgr->revLeaseMap[leaseMgr->leaseMap[25]].end() !=
                leaseMgr->revLeaseMap[leaseMgr->leaseMap[25]].find(25));
    EXPECT_TRUE(leaseMgr->leaseMap.end() !=
                leaseMgr->leaseMap.find(3));
    EXPECT_TRUE(leaseMgr->revLeaseMap[leaseMgr->leaseMap[3]].end() !=
                leaseMgr->revLeaseMap[leaseMgr->leaseMap[3]].find(3));
    EXPECT_TRUE(leaseMgr->leaseMap.end() !=
                leaseMgr->leaseMap.find(700000));
    EXPECT_TRUE(leaseMgr->revLeaseMap[leaseMgr->leaseMap[700000]].end() !=
                leaseMgr->revLeaseMap[leaseMgr->leaseMap[700000]].find(700000));
}

TEST_F(LeaseManagerTest, renewLeaseInternal_renew) {
    LeaseManager::Lock lock(leaseMgr->mutex);
    leaseMgr->leaseMap[1] = 1;
    leaseMgr->revLeaseMap[1].insert(1);
    EXPECT_EQ(1U, leaseMgr->leaseMap.size());
    EXPECT_EQ(1U, leaseMgr->revLeaseMap.size());
    WireFormat::ClientLease clientLease = leaseMgr->renewLeaseInternal(1, lock);
    EXPECT_EQ(1U, leaseMgr->leaseMap.size());
    EXPECT_EQ(2U, leaseMgr->revLeaseMap.size());
    EXPECT_TRUE(leaseMgr->revLeaseMap[1].end() ==
                leaseMgr->revLeaseMap[1].find(1));
    EXPECT_EQ(1U, clientLease.leaseId);
    EXPECT_EQ(clientLease.leaseTerm, leaseMgr->leaseMap[1]);
    EXPECT_TRUE(leaseMgr->revLeaseMap[clientLease.leaseTerm].end() !=
                leaseMgr->revLeaseMap[clientLease.leaseTerm].find(1));
}

TEST_F(LeaseManagerTest, renewLeaseInternal_new) {
    LeaseManager::Lock lock(leaseMgr->mutex);
    EXPECT_EQ(0U, leaseMgr->lastIssuedLeaseId);
    EXPECT_EQ(0U, leaseMgr->maxAllocatedLeaseId);

    WireFormat::ClientLease lease1 = leaseMgr->renewLeaseInternal(0, lock);
    EXPECT_EQ(1U, lease1.leaseId);
    EXPECT_TRUE(leaseMgr->leaseMap.end() !=
                leaseMgr->leaseMap.find(1));
    EXPECT_TRUE(leaseMgr->revLeaseMap[leaseMgr->leaseMap[1]].end() !=
                leaseMgr->revLeaseMap[leaseMgr->leaseMap[1]].find(1));
    EXPECT_EQ(1U, leaseMgr->lastIssuedLeaseId);
    EXPECT_EQ(1U, leaseMgr->maxAllocatedLeaseId);

    WireFormat::ClientLease lease2 = leaseMgr->renewLeaseInternal(0, lock);
    EXPECT_EQ(2U, lease2.leaseId);
    EXPECT_TRUE(leaseMgr->leaseMap.end() !=
                leaseMgr->leaseMap.find(2));
    EXPECT_TRUE(leaseMgr->revLeaseMap[leaseMgr->leaseMap[2]].end() !=
                leaseMgr->revLeaseMap[leaseMgr->leaseMap[2]].find(2));
    EXPECT_EQ(2U, leaseMgr->lastIssuedLeaseId);
    EXPECT_EQ(2U, leaseMgr->maxAllocatedLeaseId);

    leaseMgr->allocateNextLease(lock);
    leaseMgr->allocateNextLease(lock);
    leaseMgr->allocateNextLease(lock);

    EXPECT_EQ(5U, leaseMgr->maxAllocatedLeaseId);

    WireFormat::ClientLease lease3 = leaseMgr->renewLeaseInternal(0, lock);
    EXPECT_EQ(3U, lease3.leaseId);
    EXPECT_TRUE(leaseMgr->leaseMap.end() !=
                leaseMgr->leaseMap.find(3));
    EXPECT_TRUE(leaseMgr->revLeaseMap[leaseMgr->leaseMap[3]].end() !=
                leaseMgr->revLeaseMap[leaseMgr->leaseMap[3]].find(3));
    EXPECT_EQ(3U, leaseMgr->lastIssuedLeaseId);
    EXPECT_EQ(5U, leaseMgr->maxAllocatedLeaseId);
}

}  // namespace RAMCloud

