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
#include "ClientLease.h"
#include "MockCluster.h"


namespace RAMCloud {

class ClientLeaseTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    RamCloud ramcloud;
    ClientLease lease;
    uint64_t RENEW_THRESHOLD_US;
    uint64_t DANGER_THRESHOLD_US;

    ClientLeaseTest()
        : logEnabler()
        , context()
        , cluster(&context, "mock:host=coordinator")
        , ramcloud(&context, "mock:host=coordinator")
        , lease(&ramcloud)
        , RENEW_THRESHOLD_US(ClientLease::RENEW_THRESHOLD_US)
        , DANGER_THRESHOLD_US(ClientLease::DANGER_THRESHOLD_US)
    {}

    ~ClientLeaseTest()
    {
        // Reset mockTsc so that we don't affect later running tests.
        Cycles::mockTscValue = 0;
    }

    DISALLOW_COPY_AND_ASSIGN(ClientLeaseTest);
};

TEST_F(ClientLeaseTest, getLease_basic) {
    lease.localTimestampCycles = 0;
    Cycles::mockTscValue = Cycles::fromNanoseconds(5000);
    WireFormat::ClientLease l = {0, 0, 0};
    lease.lease = l;
    EXPECT_EQ(0U, l.leaseId);
    EXPECT_EQ(0U, lease.lease.leaseId);
    l = lease.getLease();
    EXPECT_EQ(1U, l.leaseId);
    EXPECT_EQ(1U, lease.lease.leaseId);
}

TEST_F(ClientLeaseTest, getLease_shouldAsyncRenew) {
    lease.localTimestampCycles = 0;
    uint64_t leaseTerm = 300*1e6;
    uint64_t currentTimeUs = leaseTerm - RENEW_THRESHOLD_US + 1;
    Cycles::mockTscValue = Cycles::fromNanoseconds(currentTimeUs * 1000);
    WireFormat::ClientLease l = {0, leaseTerm, 0};
    lease.lease = l;
    EXPECT_EQ(0U, l.leaseId);
    EXPECT_EQ(0U, lease.lease.leaseId);
    EXPECT_FALSE(lease.renewLeaseRpc);
    EXPECT_GE(lease.leaseTermRemaining(), DANGER_THRESHOLD_US);
    l = lease.getLease();
    EXPECT_EQ(0U, l.leaseId);
    EXPECT_EQ(0U, lease.lease.leaseId);
    EXPECT_TRUE(lease.renewLeaseRpc);
}

TEST_F(ClientLeaseTest, getLease_shouldSyncRenew) {
    lease.localTimestampCycles = 0;
    uint64_t leaseTerm = 300*1e6;
    uint64_t currentTimeUs = leaseTerm - DANGER_THRESHOLD_US + 1;
    Cycles::mockTscValue = Cycles::fromNanoseconds(currentTimeUs * 1000);
    WireFormat::ClientLease l = {0, leaseTerm, 0};
    lease.lease = l;
    EXPECT_EQ(0U, l.leaseId);
    EXPECT_EQ(0U, lease.lease.leaseId);
    EXPECT_FALSE(lease.renewLeaseRpc);
    EXPECT_LT(lease.leaseTermRemaining(), DANGER_THRESHOLD_US);
    l = lease.getLease();
    EXPECT_EQ(1U, l.leaseId);
    EXPECT_EQ(1U, lease.lease.leaseId);
    EXPECT_FALSE(lease.renewLeaseRpc);
}

TEST_F(ClientLeaseTest, poll) {
    EXPECT_FALSE(lease.ramcloud->realRpcTracker.hasUnfinishedRpc());
    EXPECT_FALSE(lease.renewLeaseRpc);
    lease.poll();
    EXPECT_FALSE(lease.renewLeaseRpc);
    lease.ramcloud->realRpcTracker.nextRpcId = 2;
    EXPECT_TRUE(lease.ramcloud->realRpcTracker.hasUnfinishedRpc());
    lease.poll();
    EXPECT_TRUE(lease.renewLeaseRpc);
}

TEST_F(ClientLeaseTest, leaseTermRemaining_basic) {
    lease.lease.leaseTerm = 1000;
    lease.lease.timestamp = 0;
    lease.localTimestampCycles = 0;
    Cycles::mockTscValue = Cycles::fromNanoseconds(10000);
    EXPECT_EQ(990U, lease.leaseTermRemaining());
}

TEST_F(ClientLeaseTest, leaseTermRemaining_leaseTermInvalid) {
    lease.lease.leaseTerm = 10;
    lease.lease.timestamp = 1000;
    lease.localTimestampCycles = 1;
    Cycles::mockTscValue = 10;
    EXPECT_EQ(0U, lease.leaseTermRemaining());
}

TEST_F(ClientLeaseTest, leaseTermRemaining_leaseTermElapsed) {
    lease.lease.leaseTerm = 1000;
    lease.lease.timestamp = 0;
    lease.localTimestampCycles = 0;
    Cycles::mockTscValue = Cycles::fromNanoseconds(1005000);
    EXPECT_EQ(0U, lease.leaseTermRemaining());
}

TEST_F(ClientLeaseTest, pollInternal) {
    lease.localTimestampCycles = 0;
    Cycles::mockTscValue = Cycles::fromNanoseconds(5000);
    EXPECT_EQ(0U, lease.lease.leaseId);
    EXPECT_FALSE(lease.renewLeaseRpc);
    EXPECT_LT(lease.leaseTermRemaining(), RENEW_THRESHOLD_US);
    lease.pollInternal();
    EXPECT_EQ(0U, lease.lease.leaseId);
    EXPECT_TRUE(lease.renewLeaseRpc);
    EXPECT_EQ(0U, lease.localTimestampCycles);
    EXPECT_EQ(Cycles::fromNanoseconds(5000), lease.nextTimestampCycles);
    Cycles::mockTscValue = Cycles::fromNanoseconds(15000);
    lease.pollInternal();
    EXPECT_EQ(1U, lease.lease.leaseId);
    EXPECT_FALSE(lease.renewLeaseRpc);
    EXPECT_GE(lease.leaseTermRemaining(), RENEW_THRESHOLD_US);
    EXPECT_EQ(Cycles::fromNanoseconds(5000), lease.localTimestampCycles);
    EXPECT_EQ(Cycles::fromNanoseconds(5000), lease.nextTimestampCycles);
    lease.pollInternal();
    EXPECT_EQ(1U, lease.lease.leaseId);
    EXPECT_FALSE(lease.renewLeaseRpc);
    EXPECT_GE(lease.leaseTermRemaining(), RENEW_THRESHOLD_US);
    EXPECT_EQ(Cycles::fromNanoseconds(5000), lease.localTimestampCycles);
    EXPECT_EQ(Cycles::fromNanoseconds(5000), lease.nextTimestampCycles);
}

} // namespace RAMCloud
