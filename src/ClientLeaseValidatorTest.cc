/* Copyright (c) 2015 Stanford University
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

#include "TestUtil.h"
#include "ClientLeaseValidator.h"
#include "MockCluster.h"
#include "LeaseCommon.h"

namespace RAMCloud {

class ClientLeaseValidatorTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    ClusterClock clusterClock;
    ClientLeaseValidator validator;

    ClientLeaseValidatorTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , clusterClock()
        , validator(&context, &clusterClock)
    {
    }

    DISALLOW_COPY_AND_ASSIGN(ClientLeaseValidatorTest);
};

TEST_F(ClientLeaseValidatorTest, needsValidation) {
    EXPECT_EQ(ClusterTime(0U), clusterClock.getTime());

    ClientLease lease = {42, 100, 50};

    EXPECT_FALSE(validator.needsValidation(lease));
    EXPECT_EQ(ClusterTime(50U), clusterClock.getTime());

    clusterClock.updateClock(ClusterTime(99U));

    EXPECT_FALSE(validator.needsValidation(lease));
    EXPECT_EQ(ClusterTime(99U), clusterClock.getTime());

    clusterClock.updateClock(ClusterTime(100U));

    EXPECT_FALSE(validator.needsValidation(lease));
    EXPECT_EQ(ClusterTime(100U), clusterClock.getTime());

    clusterClock.updateClock(ClusterTime(101U));

    EXPECT_TRUE(validator.needsValidation(lease));
    EXPECT_EQ(ClusterTime(101U), clusterClock.getTime());
}

TEST_F(ClientLeaseValidatorTest, validate_basic) {
    EXPECT_EQ(ClusterTime(0U), clusterClock.getTime());

    ClientLease lease = {42, 100, 50};

    EXPECT_TRUE(validator.validate(lease));
    EXPECT_EQ(ClusterTime(50U), clusterClock.getTime());

    clusterClock.updateClock(ClusterTime(99U));

    EXPECT_TRUE(validator.validate(lease));
    EXPECT_EQ(ClusterTime(99U), clusterClock.getTime());

    clusterClock.updateClock(ClusterTime(100U));

    EXPECT_TRUE(validator.validate(lease));
    EXPECT_EQ(ClusterTime(100U), clusterClock.getTime());

    clusterClock.updateClock(ClusterTime(101U));

    EXPECT_FALSE(validator.validate(lease));
    EXPECT_EQ(ClusterTime(101U), clusterClock.getTime());
}

TEST_F(ClientLeaseValidatorTest, validate_validAfterCheck) {
    // Setup
    cluster.coordinator->leaseAuthority.lastIssuedLeaseId = 41;
    CoordinatorClusterClock* coordinatorClusterClock =
            &cluster.coordinator->leaseAuthority.clock;
    coordinatorClusterClock->safeClusterTime = ClusterTime();
    EXPECT_FALSE(cluster.coordinator->leaseAuthority.clock.updater.isRunning());
    ClusterTime expirationTime = ClusterTime() + LeaseCommon::LEASE_TERM;
    ClientLease lease =  cluster.coordinator->leaseAuthority.renewLease(0);
    cluster.coordinator->leaseAuthority.reservationAgent.stop();

    EXPECT_EQ(42U, lease.leaseId);
    EXPECT_EQ(expirationTime, ClusterTime(lease.leaseExpiration));
    EXPECT_EQ(ClusterTime(), ClusterTime(lease.timestamp));

    // Test
    lease.leaseExpiration = 25U;
    lease.timestamp = 50U;

    ClusterTime currentClusterTime =
            ClusterTime() + ClusterTimeDuration::fromNanoseconds(100);
    coordinatorClusterClock->safeClusterTime = currentClusterTime;

    EXPECT_TRUE(validator.validate(lease, &lease));
    EXPECT_EQ(currentClusterTime, clusterClock.getTime());
    EXPECT_EQ(42U, lease.leaseId);
    EXPECT_EQ(expirationTime, ClusterTime(lease.leaseExpiration));
    EXPECT_EQ(currentClusterTime, ClusterTime(lease.timestamp));

    clusterClock.updateClock(ClusterTime(200U));
    lease.leaseExpiration = 25U;

    coordinatorClusterClock->safeClusterTime = ClusterTime(150);

    EXPECT_TRUE(validator.validate(lease, &lease));
    EXPECT_EQ(ClusterTime(200U), clusterClock.getTime());
    EXPECT_EQ(42U, lease.leaseId);
    EXPECT_EQ(expirationTime, ClusterTime(lease.leaseExpiration));
    EXPECT_EQ(ClusterTime(150U), ClusterTime(lease.timestamp));

    coordinatorClusterClock->safeClusterTime = ClusterTime(300);
    lease.leaseExpiration = 25U;

    EXPECT_TRUE(validator.validate(lease));
    EXPECT_EQ(ClusterTime(300U), clusterClock.getTime());
}

TEST_F(ClientLeaseValidatorTest, validate_invalidAfterCheck) {
    // Setup
    CoordinatorClusterClock* coordinatorClusterClock =
            &cluster.coordinator->leaseAuthority.clock;
    coordinatorClusterClock->safeClusterTime = ClusterTime(0);

    // Test
    ClientLease lease = {42, 25, 50};

    coordinatorClusterClock->safeClusterTime = ClusterTime(100);

    EXPECT_FALSE(validator.validate(lease, &lease));
    EXPECT_EQ(ClusterTime(100U), clusterClock.getTime());
    EXPECT_EQ(42U, lease.leaseId);
    EXPECT_EQ(ClusterTime(25U), ClusterTime(lease.leaseExpiration));
    EXPECT_EQ(ClusterTime(50U), ClusterTime(lease.timestamp));

    clusterClock.updateClock(ClusterTime(200U));
    lease = {42, 25, 50};

    coordinatorClusterClock->safeClusterTime = ClusterTime(150);

    EXPECT_FALSE(validator.validate(lease, &lease));
    EXPECT_EQ(ClusterTime(200U), clusterClock.getTime());
    EXPECT_EQ(42U, lease.leaseId);
    EXPECT_EQ(ClusterTime(25U), ClusterTime(lease.leaseExpiration));
    EXPECT_EQ(ClusterTime(50U), ClusterTime(lease.timestamp));

    coordinatorClusterClock->safeClusterTime = ClusterTime(300);
    lease = {42, 25, 50};

    EXPECT_FALSE(validator.validate(lease));
    EXPECT_EQ(ClusterTime(300U), clusterClock.getTime());
}

}  // namespace RAMCloud
