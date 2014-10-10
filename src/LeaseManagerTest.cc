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
        context.externalStorage = &storage;
        leaseMgr.construct(&context);
        leaseMgr->clock.updater.stop();
        while (leaseMgr->clock.updater.isRunning()) {
            continue;
        }
    }

    DISALLOW_COPY_AND_ASSIGN(LeaseManagerTest);
};

TEST_F(LeaseManagerTest, constructor) {
    EXPECT_TRUE(false);
}

TEST_F(LeaseManagerTest, renewLease_renew) {
    leaseMgr->leaseMap[1] = 1;
    leaseMgr->revLeaseMap[1].insert(1);
    EXPECT_EQ(1U, leaseMgr->leaseMap.size());
    EXPECT_EQ(1U, leaseMgr->revLeaseMap.size());
    WireFormat::ClientLease clientLease = leaseMgr->renewLease(1);
    EXPECT_EQ(1U, leaseMgr->leaseMap.size());
    EXPECT_EQ(2U, leaseMgr->revLeaseMap.size());
    EXPECT_TRUE(leaseMgr->revLeaseMap[1].end() ==
                leaseMgr->revLeaseMap[1].find(1));
    EXPECT_EQ(1U, clientLease.leaseId);
    EXPECT_EQ(clientLease.leaseTerm, leaseMgr->leaseMap[1]);
    EXPECT_TRUE(leaseMgr->revLeaseMap[clientLease.leaseTerm].end() !=
                leaseMgr->revLeaseMap[clientLease.leaseTerm].find(1));
}

TEST_F(LeaseManagerTest, renewLease_new) {
    EXPECT_EQ(0U, leaseMgr->lastIssuedLeaseId);
    EXPECT_EQ(0U, leaseMgr->maxAllocatedLeaseId);

    WireFormat::ClientLease clientLease1 = leaseMgr->renewLease(0);
    EXPECT_EQ(1U, clientLease1.leaseId);
    EXPECT_TRUE(leaseMgr->leaseMap.end() !=
                leaseMgr->leaseMap.find(1));
    EXPECT_TRUE(leaseMgr->revLeaseMap[leaseMgr->leaseMap[1]].end() !=
                leaseMgr->revLeaseMap[leaseMgr->leaseMap[1]].find(1));
    EXPECT_EQ(1U, leaseMgr->lastIssuedLeaseId);
    EXPECT_EQ(1U, leaseMgr->maxAllocatedLeaseId);

    WireFormat::ClientLease clientLease2 = leaseMgr->renewLease(0);
    EXPECT_EQ(2U, clientLease2.leaseId);
    EXPECT_TRUE(leaseMgr->leaseMap.end() !=
                leaseMgr->leaseMap.find(2));
    EXPECT_TRUE(leaseMgr->revLeaseMap[leaseMgr->leaseMap[2]].end() !=
                leaseMgr->revLeaseMap[leaseMgr->leaseMap[2]].find(2));
    EXPECT_EQ(2U, leaseMgr->lastIssuedLeaseId);
    EXPECT_EQ(2U, leaseMgr->maxAllocatedLeaseId);

    {
        LeaseManager::Lock lock(leaseMgr->mutex);
        leaseMgr->allocateNextLease(lock);
        leaseMgr->allocateNextLease(lock);
        leaseMgr->allocateNextLease(lock);
    }
    EXPECT_EQ(5U, leaseMgr->maxAllocatedLeaseId);

    WireFormat::ClientLease clientLease3 = leaseMgr->renewLease(0);
    EXPECT_EQ(3U, clientLease3.leaseId);
    EXPECT_TRUE(leaseMgr->leaseMap.end() !=
                leaseMgr->leaseMap.find(3));
    EXPECT_TRUE(leaseMgr->revLeaseMap[leaseMgr->leaseMap[3]].end() !=
                leaseMgr->revLeaseMap[leaseMgr->leaseMap[3]].find(3));
    EXPECT_EQ(3U, leaseMgr->lastIssuedLeaseId);
    EXPECT_EQ(5U, leaseMgr->maxAllocatedLeaseId);
}

TEST_F(LeaseManagerTest, allocateNextLease) {
    LeaseManager::Lock lock(leaseMgr->mutex);
    storage.log.clear();
    EXPECT_EQ(0U, leaseMgr->maxAllocatedLeaseId);
    leaseMgr->allocateNextLease(lock);
    EXPECT_EQ("set(CREATE, leaseManager/1)", storage.log);
    EXPECT_EQ(1U, leaseMgr->maxAllocatedLeaseId);
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

}  // namespace RAMCloud

