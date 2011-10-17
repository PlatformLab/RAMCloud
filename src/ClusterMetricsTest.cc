/* Copyright (c) 2011 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for
 * any purpose with or without fee is hereby granted, provided that
 * the above copyright notice and this permission notice appear in all
 * copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL
 * WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL
 * AUTHORS BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR
 * CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS
 * OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
 * NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"
#include "BindTransport.h"
#include "ClusterMetrics.h"
#include "CoordinatorService.h"
#include "PingService.h"
#include "RamCloud.h"

namespace RAMCloud {

class ClusterMetricsTest : public ::testing::Test {
  public:
    ClusterMetricsTest() { }
    DISALLOW_COPY_AND_ASSIGN(ClusterMetricsTest);
};

TEST_F(ClusterMetricsTest, load) {
    BindTransport transport;
    Context::get().transportManager->registerMock(&transport);

    // Create a fake cluster.
    CoordinatorService coordinatorService;
    transport.addService(coordinatorService,
            "mock:host=coordinator", COORDINATOR_SERVICE);
    PingService pingforCoordinator;
    transport.addService(pingforCoordinator, "mock:host=coordinator",
            PING_SERVICE);
    RamCloud ramcloud(Context::get(), "mock:host=coordinator");

    // Create two servers (faked with ping).
    PingService ping1;
    transport.addService(ping1, "mock:host=ping1", PING_SERVICE);
    ramcloud.coordinator->enlistServer(MASTER, "mock:host=ping1");
    PingService ping2;
    transport.addService(ping2, "mock:host=ping2", PING_SERVICE);
    ramcloud.coordinator->enlistServer(MASTER, "mock:host=ping2");

    // Create an extra server using a redundant service locator.
    ramcloud.coordinator->enlistServer(BACKUP, "mock:host=ping1");

    ClusterMetrics clusterMetrics;
    // Insert some initial data to make sure it gets cleared.
    clusterMetrics["xyz"]["x"] = 192U;
    metrics->temp.count3 = 30303;
    clusterMetrics.load(&ramcloud);
    EXPECT_EQ(3U, clusterMetrics.size());
    EXPECT_EQ(0U, clusterMetrics["mock:host=coordinator"]["bogusValue"]);
    EXPECT_EQ(30303U, clusterMetrics["mock:host=coordinator"]["temp.count3"]);
    EXPECT_EQ(30303U, clusterMetrics["mock:host=ping1"]["temp.count3"]);

    Context::get().transportManager->unregisterMock();
}

TEST_F(ClusterMetricsTest, difference) {
    ClusterMetrics first;
    ClusterMetrics second;
    first["m0"]["x"] = 1;
    first["m0"]["y"] = 2;
    first["m1"]["x"] = 100;
    first["m1"]["y"] = 200;
    first["m2"]["x"] = 1000;
    second["m1"]["x"] = 1000;
    second["m1"]["y"] = 2000;
    second["m2"]["x"] = 10000;
    second["m3"]["x"] = 50;
    ClusterMetrics diff = second.difference(first);
    EXPECT_EQ(2U, diff.size());
    EXPECT_EQ(2U, diff["m1"].size());
    EXPECT_EQ(900U, diff["m1"]["x"]);
    EXPECT_EQ(1800U, diff["m1"]["y"]);
    EXPECT_EQ(9000U, diff["m2"]["x"]);
}

// The following tests are for methods defined in ClusterMetrics.h.

TEST_F(ClusterMetricsTest, iteration) {
    ClusterMetrics metrics;
    metrics["m0"]["a"] = 1;
    metrics["m1"]["a"] = 10;
    metrics["m2"]["a"] = 100;
    uint64_t total = 0;
    for (ClusterMetrics::iterator it = metrics.begin(); it != metrics.end();
            it++) {
        total += it->second["a"];
    }
    EXPECT_EQ(111U, total);
}

TEST_F(ClusterMetricsTest, find) {
    ClusterMetrics metrics;
    metrics["m0"]["a"] = 1;
    metrics["m1"]["a"] = 10;
    EXPECT_TRUE(metrics.find("m0") != metrics.end());
    EXPECT_TRUE(metrics.find("m3") == metrics.end());
}

TEST_F(ClusterMetricsTest, clear) {
    ClusterMetrics metrics;
    metrics["m0"]["a"] = 1;
    metrics["m1"]["a"] = 10;
    metrics["m2"]["a"] = 100;
    metrics.clear();
    uint64_t total = 0;
    for (ClusterMetrics::iterator it = metrics.begin(); it != metrics.end();
            it++) {
        total += it->second["a"];
    }
    EXPECT_EQ(0U, total);
}

TEST_F(ClusterMetricsTest, erase) {
    ClusterMetrics metrics;
    metrics["m0"]["a"] = 1;
    metrics["m1"]["a"] = 10;
    metrics["m2"]["a"] = 100;
    metrics.erase("m1");
    uint64_t total = 0;
    for (ClusterMetrics::iterator it = metrics.begin(); it != metrics.end();
            it++) {
        total += it->second["a"];
    }
    EXPECT_EQ(101U, total);
}

TEST_F(ClusterMetricsTest, empty) {
    ClusterMetrics metrics;
    metrics["m0"]["a"] = 1;
    metrics["m1"]["a"] = 10;
    metrics["m2"]["a"] = 100;
    metrics.erase("m1");
    EXPECT_FALSE(metrics.empty());
    metrics.erase("m2");
    EXPECT_FALSE(metrics.empty());
    metrics.erase("m0");
    EXPECT_TRUE(metrics.empty());
}

TEST_F(ClusterMetricsTest, size) {
    ClusterMetrics metrics;
    EXPECT_EQ(0U, metrics.size());
    metrics["m0"]["a"] = 1;
    EXPECT_EQ(1U, metrics.size());
    metrics["m1"]["a"] = 10;
    EXPECT_EQ(2U, metrics.size());
}

}
