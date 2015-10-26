/* Copyright (c) 2014-2015 Stanford University
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
#include "ClientLeaseAuthority.h"
#include "CoordinatorClusterClock.pb.h"
#include "MockExternalStorage.h"

namespace RAMCloud {

class ClientLeaseAuthorityTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockExternalStorage storage;
    Tub<ClientLeaseAuthority> leaseAuthority;

    ClientLeaseAuthorityTest()
        : logEnabler()
        , context()
        , storage(true)
        , leaseAuthority()
    {
        // Add data to storage to affect recovered clock time.
        ProtoBuf::CoordinatorClusterClock info;
        info.set_next_safe_time(1000);
        std::string str;
        info.SerializeToString(&str);
        storage.getResults.push(str);

        context.externalStorage = &storage;
        leaseAuthority.construct(&context);
        leaseAuthority->lastIssuedLeaseId = 0;
        leaseAuthority->maxReservedLeaseId = 0;
    }

    DISALLOW_COPY_AND_ASSIGN(ClientLeaseAuthorityTest);
};

TEST_F(ClientLeaseAuthorityTest, getLeaseInfo) {
    WireFormat::ClientLease lease;

    lease = leaseAuthority->getLeaseInfo(25);
    EXPECT_EQ(0U, lease.leaseId);
    EXPECT_EQ(0U, lease.leaseExpiration);

    leaseAuthority->leaseMap[25] = ClusterTime(8888);

    lease = leaseAuthority->getLeaseInfo(25);
    EXPECT_EQ(25U, lease.leaseId);
    EXPECT_EQ(8888U, lease.leaseExpiration);
}

TEST_F(ClientLeaseAuthorityTest, recover_basic) {
    EXPECT_EQ(0U, leaseAuthority->leaseMap.size());
    EXPECT_EQ(0U, leaseAuthority->expirationOrder.size());

    storage.getChildrenNames.push("1");
    storage.getChildrenValues.push("");
    storage.getChildrenNames.push("25");
    storage.getChildrenValues.push("");
    storage.getChildrenNames.push("3");
    storage.getChildrenValues.push("");
    storage.getChildrenNames.push("700000");
    storage.getChildrenValues.push("");

    leaseAuthority->recover();

    EXPECT_EQ(3U, leaseAuthority->leaseMap.size());
    EXPECT_TRUE(leaseAuthority->leaseMap.end() !=
                leaseAuthority->leaseMap.find(1));
    EXPECT_TRUE(leaseAuthority->expirationOrder.end() !=
                leaseAuthority->expirationOrder.find(
                        {leaseAuthority->leaseMap[1], 1}));
    EXPECT_TRUE(leaseAuthority->leaseMap.end() !=
                leaseAuthority->leaseMap.find(25));
    EXPECT_TRUE(leaseAuthority->expirationOrder.end() !=
                leaseAuthority->expirationOrder.find(
                        {leaseAuthority->leaseMap[25], 25}));
    EXPECT_TRUE(leaseAuthority->leaseMap.end() !=
                leaseAuthority->leaseMap.find(3));
    EXPECT_TRUE(leaseAuthority->expirationOrder.end() !=
                leaseAuthority->expirationOrder.find(
                        {leaseAuthority->leaseMap[3], 3}));
    EXPECT_EQ(699999UL, leaseAuthority->lastIssuedLeaseId);
    EXPECT_EQ(700000UL, leaseAuthority->maxReservedLeaseId);
}

TEST_F(ClientLeaseAuthorityTest, recover_empty) {
    EXPECT_EQ(0U, leaseAuthority->leaseMap.size());
    EXPECT_EQ(0U, leaseAuthority->expirationOrder.size());

    leaseAuthority->recover();

    EXPECT_EQ(0U, leaseAuthority->leaseMap.size());
    EXPECT_EQ(0U, leaseAuthority->expirationOrder.size());

    EXPECT_EQ(0UL, leaseAuthority->lastIssuedLeaseId);
    EXPECT_EQ(0UL, leaseAuthority->maxReservedLeaseId);
}

TEST_F(ClientLeaseAuthorityTest, renewLease) {
    EXPECT_FALSE(leaseAuthority->reservationAgent.isRunning());
    leaseAuthority->maxReservedLeaseId = 1000;
    leaseAuthority->renewLease(0);
    EXPECT_FALSE(leaseAuthority->reservationAgent.isRunning());
    leaseAuthority->lastIssuedLeaseId = 900;
    leaseAuthority->renewLease(0);
    EXPECT_TRUE(leaseAuthority->reservationAgent.isRunning());
}

TEST_F(ClientLeaseAuthorityTest, leaseReservationAgent_handleTimerEvent) {
    EXPECT_EQ(0U, leaseAuthority->lastIssuedLeaseId);
    EXPECT_EQ(0U, leaseAuthority->maxReservedLeaseId);
    leaseAuthority->reservationAgent.handleTimerEvent();
    EXPECT_EQ(0U, leaseAuthority->lastIssuedLeaseId);
    EXPECT_EQ(1U, leaseAuthority->maxReservedLeaseId);
    EXPECT_TRUE(leaseAuthority->reservationAgent.isRunning());
    leaseAuthority->reservationAgent.stop();
    leaseAuthority->maxReservedLeaseId = 1000 - 1;
    leaseAuthority->reservationAgent.handleTimerEvent();
    EXPECT_EQ(0U, leaseAuthority->lastIssuedLeaseId);
    EXPECT_EQ(1000U, leaseAuthority->maxReservedLeaseId);
    EXPECT_FALSE(leaseAuthority->reservationAgent.isRunning());
}

TEST_F(ClientLeaseAuthorityTest, leaseCleaner_handleTimerEvent) {
    leaseAuthority->leaseMap[25] = ClusterTime(0);
    leaseAuthority->expirationOrder.insert({ClusterTime(0), 25});
    leaseAuthority->leaseMap[52] = ClusterTime(0);
    leaseAuthority->expirationOrder.insert({ClusterTime(0), 52});
    leaseAuthority->leaseMap[88] = ClusterTime(3);
    leaseAuthority->expirationOrder.insert({ClusterTime(3), 88});
    leaseAuthority->leaseMap[99] = ClusterTime(60000);
    leaseAuthority->expirationOrder.insert({ClusterTime(60000), 99});

    leaseAuthority->cleaner.handleTimerEvent();
    while (leaseAuthority->cleaner.triggerTime == 0U) {
        leaseAuthority->cleaner.handleTimerEvent();
    }

    EXPECT_EQ(1U, leaseAuthority->leaseMap.size());
    EXPECT_EQ(1U, leaseAuthority->expirationOrder.size());
}

TEST_F(ClientLeaseAuthorityTest, expirationOrder) {
    EXPECT_EQ(0UL, leaseAuthority->expirationOrder.size());
    leaseAuthority->expirationOrder.insert({ClusterTime(1), 1});
    EXPECT_EQ(1UL, leaseAuthority->expirationOrder.size());
    leaseAuthority->expirationOrder.insert({ClusterTime(3), 1});
    EXPECT_EQ(2UL, leaseAuthority->expirationOrder.size());
    leaseAuthority->expirationOrder.insert({ClusterTime(1), 3});
    EXPECT_EQ(3UL, leaseAuthority->expirationOrder.size());
    leaseAuthority->expirationOrder.insert({ClusterTime(2), 1});
    EXPECT_EQ(4UL, leaseAuthority->expirationOrder.size());
    leaseAuthority->expirationOrder.insert({ClusterTime(1), 2});
    EXPECT_EQ(5UL, leaseAuthority->expirationOrder.size());

    ClientLeaseAuthority::ExpirationOrderSet::iterator it =
            leaseAuthority->expirationOrder.begin();
    EXPECT_EQ(ClusterTime(1UL), it->leaseExpiration);
    EXPECT_EQ(1UL, it->leaseId);
    it++;
    EXPECT_EQ(ClusterTime(1UL), it->leaseExpiration);
    EXPECT_EQ(2UL, it->leaseId);
    it++;
    EXPECT_EQ(ClusterTime(1UL), it->leaseExpiration);
    EXPECT_EQ(3UL, it->leaseId);
    it++;
    EXPECT_EQ(ClusterTime(2UL), it->leaseExpiration);
    EXPECT_EQ(1UL, it->leaseId);
    it++;
    EXPECT_EQ(ClusterTime(3UL), it->leaseExpiration);
    EXPECT_EQ(1UL, it->leaseId);
    it++;
    EXPECT_TRUE(it == leaseAuthority->expirationOrder.end());
}

TEST_F(ClientLeaseAuthorityTest, cleanNextLease) {
    // Time dependent test.
    leaseAuthority->leaseMap[25] = ClusterTime(0);
    leaseAuthority->expirationOrder.insert({ClusterTime(0), 25});
    leaseAuthority->leaseMap[4294967297] = ClusterTime(0);
    leaseAuthority->expirationOrder.insert({ClusterTime(0), 4294967297});

    EXPECT_EQ(2U, leaseAuthority->leaseMap.size());
    EXPECT_EQ(2U, leaseAuthority->expirationOrder.size());

    storage.log.clear();
    EXPECT_TRUE(leaseAuthority->cleanNextLease());
    EXPECT_EQ("remove(clientLeaseAuthority/25)", storage.log);
    EXPECT_EQ(1U, leaseAuthority->leaseMap.size());
    EXPECT_EQ(1U, leaseAuthority->expirationOrder.size());

    storage.log.clear();
    EXPECT_TRUE(leaseAuthority->cleanNextLease());
    EXPECT_EQ("remove(clientLeaseAuthority/4294967297)", storage.log);
    EXPECT_EQ(0U, leaseAuthority->leaseMap.size());
    EXPECT_EQ(0U, leaseAuthority->expirationOrder.size());

    EXPECT_FALSE(leaseAuthority->cleanNextLease());
    EXPECT_EQ(0U, leaseAuthority->leaseMap.size());
    EXPECT_EQ(0U, leaseAuthority->expirationOrder.size());

    leaseAuthority->leaseMap[88] = ClusterTime(3);
    leaseAuthority->expirationOrder.insert({ClusterTime(3), 88});
    leaseAuthority->leaseMap[99] = ClusterTime(60000);
    leaseAuthority->expirationOrder.insert({ClusterTime(60000), 99});

    EXPECT_EQ(2U, leaseAuthority->leaseMap.size());
    EXPECT_EQ(2U, leaseAuthority->expirationOrder.size());

    EXPECT_TRUE(leaseAuthority->cleanNextLease());
    EXPECT_EQ(1U, leaseAuthority->leaseMap.size());
    EXPECT_EQ(1U, leaseAuthority->expirationOrder.size());

    EXPECT_FALSE(leaseAuthority->cleanNextLease());
    EXPECT_EQ(1U, leaseAuthority->leaseMap.size());
    EXPECT_EQ(1U, leaseAuthority->expirationOrder.size());
}

TEST_F(ClientLeaseAuthorityTest, getLeaseObjName) {
    EXPECT_EQ("clientLeaseAuthority/12345",
              leaseAuthority->getLeaseObjName(12345));
    EXPECT_EQ("clientLeaseAuthority/67890",
              leaseAuthority->getLeaseObjName(67890));
}

TEST_F(ClientLeaseAuthorityTest, renewLeaseInternal_renew) {
    ClientLeaseAuthority::Lock lock(leaseAuthority->mutex);
    leaseAuthority->leaseMap[1] = ClusterTime(1);
    leaseAuthority->expirationOrder.insert({ClusterTime(1), 1});
    EXPECT_EQ(1U, leaseAuthority->leaseMap.size());
    EXPECT_EQ(1U, leaseAuthority->expirationOrder.size());
    WireFormat::ClientLease clientLease =
            leaseAuthority->renewLeaseInternal(1, lock);
    EXPECT_EQ(1U, leaseAuthority->leaseMap.size());
    EXPECT_EQ(1U, leaseAuthority->expirationOrder.size());
    EXPECT_TRUE(leaseAuthority->expirationOrder.end() ==
                leaseAuthority->expirationOrder.find({ClusterTime(1), 1}));
    EXPECT_EQ(1U, clientLease.leaseId);
    EXPECT_EQ(ClusterTime(clientLease.leaseExpiration),
              leaseAuthority->leaseMap[1]);
    EXPECT_TRUE(leaseAuthority->expirationOrder.end() !=
                leaseAuthority->expirationOrder.find(
                        {ClusterTime(clientLease.leaseExpiration), 1}));
}

TEST_F(ClientLeaseAuthorityTest, renewLeaseInternal_new) {
    ClientLeaseAuthority::Lock lock(leaseAuthority->mutex);
    EXPECT_EQ(0U, leaseAuthority->lastIssuedLeaseId);
    EXPECT_EQ(0U, leaseAuthority->maxReservedLeaseId);

    WireFormat::ClientLease lease1 =
            leaseAuthority->renewLeaseInternal(0, lock);
    EXPECT_EQ(1U, lease1.leaseId);
    EXPECT_TRUE(leaseAuthority->leaseMap.end() !=
                leaseAuthority->leaseMap.find(1));
    EXPECT_TRUE(leaseAuthority->expirationOrder.end() !=
                leaseAuthority->expirationOrder.find(
                        {leaseAuthority->leaseMap[1], 1}));
    EXPECT_EQ(1U, leaseAuthority->lastIssuedLeaseId);
    EXPECT_EQ(2U, leaseAuthority->maxReservedLeaseId);

    WireFormat::ClientLease lease2 =
            leaseAuthority->renewLeaseInternal(0, lock);
    EXPECT_EQ(2U, lease2.leaseId);
    EXPECT_TRUE(leaseAuthority->leaseMap.end() !=
                leaseAuthority->leaseMap.find(2));
    EXPECT_TRUE(leaseAuthority->expirationOrder.end() !=
                leaseAuthority->expirationOrder.find(
                        {leaseAuthority->leaseMap[2], 2}));
    EXPECT_EQ(2U, leaseAuthority->lastIssuedLeaseId);
    EXPECT_EQ(3U, leaseAuthority->maxReservedLeaseId);

    leaseAuthority->reserveNextLease(lock);
    leaseAuthority->reserveNextLease(lock);
    leaseAuthority->reserveNextLease(lock);

    EXPECT_EQ(6U, leaseAuthority->maxReservedLeaseId);

    WireFormat::ClientLease lease3 =
            leaseAuthority->renewLeaseInternal(0, lock);
    EXPECT_EQ(3U, lease3.leaseId);
    EXPECT_TRUE(leaseAuthority->leaseMap.end() !=
                leaseAuthority->leaseMap.find(3));
    EXPECT_TRUE(leaseAuthority->expirationOrder.end() !=
                leaseAuthority->expirationOrder.find(
                        {leaseAuthority->leaseMap[3], 3}));
    EXPECT_EQ(3U, leaseAuthority->lastIssuedLeaseId);
    EXPECT_EQ(6U, leaseAuthority->maxReservedLeaseId);
}

TEST_F(ClientLeaseAuthorityTest, renewLeaseInternal_reservationsNotKeepingUp) {
    ClientLeaseAuthority::Lock lock(leaseAuthority->mutex);
    EXPECT_EQ(0U, leaseAuthority->lastIssuedLeaseId);
    EXPECT_EQ(0U, leaseAuthority->maxReservedLeaseId);

    leaseAuthority->lastIssuedLeaseId = 10;
    EXPECT_EQ(10U, leaseAuthority->lastIssuedLeaseId);
    EXPECT_EQ(0U, leaseAuthority->maxReservedLeaseId);

    WireFormat::ClientLease lease = leaseAuthority->renewLeaseInternal(0, lock);
    EXPECT_EQ(11U, lease.leaseId);

    EXPECT_EQ(11U, leaseAuthority->lastIssuedLeaseId);
    EXPECT_EQ(12U, leaseAuthority->maxReservedLeaseId);

    TestLog::Enable _("renewLeaseInternal");
    TestLog::reset();
    lease = leaseAuthority->renewLeaseInternal(0, lock);
    EXPECT_EQ("renewLeaseInternal: Lease reservations are not keeping up; "
              "maxReservedLeaseId = 12",
              TestLog::get());
}

TEST_F(ClientLeaseAuthorityTest, reserveNextLease) {
    ClientLeaseAuthority::Lock lock(leaseAuthority->mutex);
    storage.log.clear();
    leaseAuthority->maxReservedLeaseId = 4294967296;
    EXPECT_EQ(4294967296U, leaseAuthority->maxReservedLeaseId);
    leaseAuthority->reserveNextLease(lock);
    EXPECT_EQ("set(CREATE, clientLeaseAuthority/4294967297)", storage.log);
    EXPECT_EQ(4294967297U, leaseAuthority->maxReservedLeaseId);
}

}  // namespace RAMCloud

