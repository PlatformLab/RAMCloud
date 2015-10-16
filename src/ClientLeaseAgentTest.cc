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
#include "ClientLeaseAgent.h"
#include "LeaseCommon.h"
#include "MockCluster.h"


namespace RAMCloud {

class ClientLeaseAgentTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    RamCloud ramcloud;
    ClientLeaseAgent leaseAgent;
    uint64_t RENEW_THRESHOLD_NS;
    uint64_t DANGER_THRESHOLD_NS;

    ClientLeaseAgentTest()
        : logEnabler()
        , context()
        , cluster(&context, "mock:host=coordinator")
        , ramcloud(&context, "mock:host=coordinator")
        , leaseAgent(&ramcloud)
        , RENEW_THRESHOLD_NS(LeaseCommon::RENEW_THRESHOLD_NS)
        , DANGER_THRESHOLD_NS(LeaseCommon::DANGER_THRESHOLD_NS)
    {}

    ~ClientLeaseAgentTest()
    {
        // Reset mockTsc so that we don't affect later running tests.
        Cycles::mockTscValue = 0;
    }

    DISALLOW_COPY_AND_ASSIGN(ClientLeaseAgentTest);
};

TEST_F(ClientLeaseAgentTest, getLease_basic) {
    leaseAgent.lastRenewalTimeCycles = 0;
    Cycles::mockTscValue = Cycles::fromNanoseconds(5000);
    WireFormat::ClientLease l = {0, 0, 0};
    leaseAgent.lease = l;
    EXPECT_EQ(0U, l.leaseId);
    EXPECT_EQ(0U, leaseAgent.lease.leaseId);
    l = leaseAgent.getLease();
    EXPECT_EQ(1U, l.leaseId);
    EXPECT_EQ(1U, leaseAgent.lease.leaseId);
}

TEST_F(ClientLeaseAgentTest, getLease_nonblocking) {
    leaseAgent.lastRenewalTimeCycles = 0;
    uint64_t leaseExpiration = LeaseCommon::LEASE_TERM_NS;
    uint64_t currentTimeNS = leaseExpiration - RENEW_THRESHOLD_NS + 1;
    leaseAgent.leaseExpirationCycles = Cycles::fromNanoseconds(
            leaseExpiration - DANGER_THRESHOLD_NS);
    Cycles::mockTscValue = Cycles::fromNanoseconds(currentTimeNS);
    WireFormat::ClientLease l = {0, leaseExpiration, 0};
    leaseAgent.lease = l;
    TestLog::setPredicate("getLease");
    TestLog::reset();
    EXPECT_EQ(0U, l.leaseId);
    EXPECT_EQ(0U, leaseAgent.lease.leaseId);
    l = leaseAgent.getLease();
    EXPECT_EQ(0U, l.leaseId);
    EXPECT_EQ(0U, leaseAgent.lease.leaseId);
    EXPECT_EQ("", TestLog::get());
}

TEST_F(ClientLeaseAgentTest, getLease_blocking) {
    leaseAgent.lastRenewalTimeCycles = 0;
    uint64_t leaseExpiration = LeaseCommon::LEASE_TERM_NS;
    uint64_t currentTimeNS = leaseExpiration - DANGER_THRESHOLD_NS + 1;
    leaseAgent.leaseExpirationCycles = Cycles::fromNanoseconds(
            leaseExpiration - DANGER_THRESHOLD_NS);
    Cycles::mockTscValue = Cycles::fromNanoseconds(currentTimeNS);
    WireFormat::ClientLease l = {0, leaseExpiration, 0};
    leaseAgent.lease = l;
    TestLog::setPredicate("getLease");
    TestLog::reset();
    EXPECT_EQ(0U, l.leaseId);
    EXPECT_EQ(0U, leaseAgent.lease.leaseId);
    l = leaseAgent.getLease();
    EXPECT_EQ(1U, l.leaseId);
    EXPECT_EQ(1U, leaseAgent.lease.leaseId);
    EXPECT_NE("", TestLog::get());
}

TEST_F(ClientLeaseAgentTest, poll) {
    EXPECT_EQ(0U, leaseAgent.lease.leaseId);
    EXPECT_FALSE(leaseAgent.renewLeaseRpc);
    EXPECT_EQ(0U, leaseAgent.nextRenewalTimeCycles);

    leaseAgent.poll();

    EXPECT_EQ(0U, leaseAgent.lease.leaseId);
    EXPECT_TRUE(leaseAgent.renewLeaseRpc);
    EXPECT_EQ(0U, leaseAgent.nextRenewalTimeCycles);
    leaseAgent.renewLeaseRpc->state = RpcWrapper::RETRY;

    leaseAgent.poll();

    EXPECT_EQ(0U, leaseAgent.lease.leaseId);
    EXPECT_TRUE(leaseAgent.renewLeaseRpc);
    EXPECT_EQ(0U, leaseAgent.nextRenewalTimeCycles);

    leaseAgent.poll();

    EXPECT_EQ(2U, leaseAgent.lease.leaseId);
    EXPECT_FALSE(leaseAgent.renewLeaseRpc);
    EXPECT_NE(0U, leaseAgent.nextRenewalTimeCycles);
}

} // namespace RAMCloud
