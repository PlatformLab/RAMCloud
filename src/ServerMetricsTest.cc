/* Copyright (c) 2011-2014 Stanford University
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
#include "MetricList.pb.h"
#include "RawMetrics.h"
#include "ServerMetrics.h"

namespace RAMCloud {

class ServerMetricsTest : public ::testing::Test {
  public:
    ServerMetricsTest() { }
    DISALLOW_COPY_AND_ASSIGN(ServerMetricsTest);
};

TEST_F(ServerMetricsTest, basics) {
    ServerMetrics metrics;
    metrics["a.b.c"] = 24;
    metrics["a.b.d"] = 36;
    metrics["a.b.c"] = 48;
    EXPECT_EQ(48U, metrics["a.b.c"]);
    EXPECT_EQ(36U, metrics["a.b.d"]);
}

TEST_F(ServerMetricsTest, load) {
    RawMetrics data;
    data.master.recoveryTicks = 99;
    data.temp.count2 = 1000;
    string s;
    data.serialize(s);
    Buffer buffer;
    buffer.appendExternal(s.c_str(), downCast<uint32_t>(s.length()));
    ServerMetrics metrics;
    metrics.load(buffer);
    EXPECT_EQ(99U, metrics["master.recoveryTicks"]);
    EXPECT_EQ(1000U, metrics["temp.count2"]);
}

TEST_F(ServerMetricsTest, load_bogusInput) {
    string s("This string contains bogus data");
    Buffer buffer;
    buffer.appendExternal(s.c_str(), downCast<uint32_t>(s.length()));
    ServerMetrics metrics;
    EXPECT_THROW(metrics.load(buffer), ServerMetrics::FormatError);
}

TEST_F(ServerMetricsTest, difference) {
    ServerMetrics metrics;
    metrics["a"] = 10;
    metrics["b"] = 20;
    metrics["c"] = 30;
    ServerMetrics metrics2;
    metrics2["a"] = 1;
    metrics2["b"] = 2;
    metrics2["d"] = 3;
    ServerMetrics diff = metrics.difference(metrics2);
    EXPECT_EQ(3U, diff.size());
    EXPECT_EQ(9U, diff["a"]);
    EXPECT_EQ(18U, diff["b"]);
    EXPECT_EQ(30U, diff["c"]);
    EXPECT_EQ(0U, diff["d"]);
}

TEST_F(ServerMetricsTest, difference_skipSpecialValues) {
    ServerMetrics metrics;
    metrics["clockFrequency"] = 10;
    metrics["pid"] = 20;
    metrics["serverId"] = 30;
    ServerMetrics metrics2;
    metrics2["clockFrequency"] = 1;
    metrics2["pid"] = 2;
    metrics2["serverId"] = 3;
    ServerMetrics diff = metrics.difference(metrics2);
    EXPECT_EQ(10U, diff["clockFrequency"]);
    EXPECT_EQ(20U, diff["pid"]);
    EXPECT_EQ(30U, diff["serverId"]);
}

// The following tests are for methods defined in ServerMetrics.h.

TEST_F(ServerMetricsTest, iteration) {
    ServerMetrics metrics;
    metrics["a"] = 1;
    metrics["b"] = 10;
    metrics["c"] = 100;
    uint64_t total = 0;
    for (ServerMetrics::iterator it = metrics.begin(); it != metrics.end();
            it++) {
        total += it->second;
    }
    EXPECT_EQ(111U, total);
}

TEST_F(ServerMetricsTest, find) {
    ServerMetrics metrics;
    metrics["a"] = 1;
    metrics["b"] = 10;
    EXPECT_TRUE(metrics.find("a") != metrics.end());
    EXPECT_TRUE(metrics.find("x") == metrics.end());
}

TEST_F(ServerMetricsTest, clear) {
    ServerMetrics metrics;
    metrics["a"] = 1;
    metrics["b"] = 10;
    metrics["c"] = 100;
    metrics.clear();
    uint64_t total = 0;
    for (ServerMetrics::iterator it = metrics.begin(); it != metrics.end();
            it++) {
        total += it->second;
    }
    EXPECT_EQ(0U, total);
}

TEST_F(ServerMetricsTest, erase) {
    ServerMetrics metrics;
    metrics["a"] = 1;
    metrics["b"] = 10;
    metrics["c"] = 100;
    metrics.erase("b");
    uint64_t total = 0;
    for (ServerMetrics::iterator it = metrics.begin(); it != metrics.end();
            it++) {
        total += it->second;
    }
    EXPECT_EQ(101U, total);
}

TEST_F(ServerMetricsTest, empty) {
    ServerMetrics metrics;
    metrics["a"] = 1;
    metrics["b"] = 10;
    metrics["c"] = 100;
    metrics.erase("b");
    EXPECT_FALSE(metrics.empty());
    metrics.erase("a");
    EXPECT_FALSE(metrics.empty());
    metrics.erase("c");
    EXPECT_TRUE(metrics.empty());
}

TEST_F(ServerMetricsTest, size) {
    ServerMetrics metrics;
    EXPECT_EQ(0U, metrics.size());
    metrics["a"] = 1;
    EXPECT_EQ(1U, metrics.size());
    metrics["b"] = 10;
    EXPECT_EQ(2U, metrics.size());
}

}
