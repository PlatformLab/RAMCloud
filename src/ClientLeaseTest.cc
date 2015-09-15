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
#include "ClientLease.h"
#include "LeaseCommon.h"
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
        , RENEW_THRESHOLD_US(LeaseCommon::RENEW_THRESHOLD_US)
        , DANGER_THRESHOLD_US(LeaseCommon::DANGER_THRESHOLD_US)
    {}

    ~ClientLeaseTest()
    {
        // Reset mockTsc so that we don't affect later running tests.
        Cycles::mockTscValue = 0;
    }

    DISALLOW_COPY_AND_ASSIGN(ClientLeaseTest);
};

TEST_F(ClientLeaseTest, getLease_basic) {
    lease.lastRenewalTimeCycles = 0;
    Cycles::mockTscValue = Cycles::fromNanoseconds(5000);
    WireFormat::ClientLease l = {0, 0, 0};
    lease.lease = l;
    EXPECT_EQ(0U, l.leaseId);
    EXPECT_EQ(0U, lease.lease.leaseId);
    l = lease.getLease();
    EXPECT_EQ(1U, l.leaseId);
    EXPECT_EQ(1U, lease.lease.leaseId);
}

TEST_F(ClientLeaseTest, getLease_nonblocking) {
    lease.lastRenewalTimeCycles = 0;
    uint64_t leaseExpiration = 300*1e6;
    uint64_t currentTimeUs = leaseExpiration - RENEW_THRESHOLD_US + 1;
    lease.leaseTermElapseCycles = Cycles::fromNanoseconds(
            (leaseExpiration - DANGER_THRESHOLD_US) * 1000);
    Cycles::mockTscValue = Cycles::fromNanoseconds(currentTimeUs * 1000);
    WireFormat::ClientLease l = {0, leaseExpiration, 0};
    lease.lease = l;
    TestLog::setPredicate("getLease");
    TestLog::reset();
    EXPECT_EQ(0U, l.leaseId);
    EXPECT_EQ(0U, lease.lease.leaseId);
    l = lease.getLease();
    EXPECT_EQ(0U, l.leaseId);
    EXPECT_EQ(0U, lease.lease.leaseId);
    EXPECT_EQ("", TestLog::get());
}

TEST_F(ClientLeaseTest, getLease_blocking) {
    lease.lastRenewalTimeCycles = 0;
    uint64_t leaseExpiration = 300*1e6;
    uint64_t currentTimeUs = leaseExpiration - DANGER_THRESHOLD_US + 1;
    lease.leaseTermElapseCycles = Cycles::fromNanoseconds(
            (leaseExpiration - DANGER_THRESHOLD_US) * 1000);
    Cycles::mockTscValue = Cycles::fromNanoseconds(currentTimeUs * 1000);
    WireFormat::ClientLease l = {0, leaseExpiration, 0};
    lease.lease = l;
    TestLog::setPredicate("getLease");
    TestLog::reset();
    EXPECT_EQ(0U, l.leaseId);
    EXPECT_EQ(0U, lease.lease.leaseId);
    l = lease.getLease();
    EXPECT_EQ(1U, l.leaseId);
    EXPECT_EQ(1U, lease.lease.leaseId);
    EXPECT_NE("", TestLog::get());
}

TEST_F(ClientLeaseTest, poll) {
    EXPECT_EQ(0U, lease.lease.leaseId);
    EXPECT_FALSE(lease.renewLeaseRpc);
    EXPECT_EQ(0U, lease.nextRenewalTimeCycles);

    lease.poll();

    EXPECT_EQ(0U, lease.lease.leaseId);
    EXPECT_TRUE(lease.renewLeaseRpc);
    EXPECT_EQ(0U, lease.nextRenewalTimeCycles);
    lease.renewLeaseRpc->state = RpcWrapper::RETRY;

    lease.poll();

    EXPECT_EQ(0U, lease.lease.leaseId);
    EXPECT_TRUE(lease.renewLeaseRpc);
    EXPECT_EQ(0U, lease.nextRenewalTimeCycles);

    lease.poll();

    EXPECT_EQ(2U, lease.lease.leaseId);
    EXPECT_FALSE(lease.renewLeaseRpc);
    EXPECT_NE(0U, lease.nextRenewalTimeCycles);
}

} // namespace RAMCloud
