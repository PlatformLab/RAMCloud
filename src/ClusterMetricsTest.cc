/* Copyright (c) 2011-2015 Stanford University
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
#include "ClusterMetrics.h"
#include "MockCluster.h"
#include "PingService.h"
#include "RamCloud.h"
#include "ServerList.h"

namespace RAMCloud {

class ClusterMetricsTest : public ::testing::Test {
  public:
    ClusterMetricsTest() { }
    DISALLOW_COPY_AND_ASSIGN(ClusterMetricsTest);
};

TEST_F(ClusterMetricsTest, load) {
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster(&context);

    PingService pingforCoordinator(&cluster.coordinatorContext);

    RamCloud ramcloud(&context, "mock:host=coordinator");

    // Create two servers (faked with ping).
    ServerConfig ping1Config = ServerConfig::forTesting();
    ping1Config.localLocator = "mock:host=ping1";
    ping1Config.services = {WireFormat::BACKUP_SERVICE,
                            WireFormat::PING_SERVICE};
    cluster.addServer(ping1Config);

    ServerConfig ping2Config = ping1Config;
    ping2Config.localLocator = "mock:host=ping2";
    ping2Config.services = {WireFormat::MASTER_SERVICE,
                            WireFormat::PING_SERVICE};
    cluster.addServer(ping2Config);

    ClusterMetrics clusterMetrics;
    // Insert some initial data to make sure it gets cleared.
    clusterMetrics["xyz"]["x"] = 192U;
    metrics->temp.count3 = 30303;
    clusterMetrics.load(&ramcloud);
    EXPECT_EQ(3u, clusterMetrics.size());
    EXPECT_EQ(0U, clusterMetrics["mock:host=coordinator"]["bogusValue"]);
    EXPECT_EQ(30303U, clusterMetrics["mock:host=coordinator"]["temp.count3"]);
    EXPECT_EQ(30303U, clusterMetrics["mock:host=ping1"]["temp.count3"]);
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
