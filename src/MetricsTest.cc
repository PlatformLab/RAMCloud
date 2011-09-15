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
#include "Metrics.h"
#include "MetricList.pb.h"

namespace RAMCloud {

class MetricsTest : public ::testing::Test {
  public:
    MetricsTest() { }
    DISALLOW_COPY_AND_ASSIGN(MetricsTest);
};

TEST_F(MetricsTest, start) {
    Metrics metrics;
    metrics.master.recoveryTicks = 44;
    metrics.start();
    EXPECT_EQ(0U, metrics.master.recoveryTicks);
    metrics.master.recoveryTicks = 55;
    metrics.start();
    EXPECT_EQ(55U, metrics.master.recoveryTicks);
}

TEST_F(MetricsTest, end) {
    TestLog::Enable _;
    Metrics metrics;
    metrics.start();
    metrics.start();
    metrics.master.recoveryTicks = 44;
    metrics.end();
    EXPECT_EQ("", TestLog::get());
    metrics.end();
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
            "master\\.recoveryTicks = 44", TestLog::get()));
}

TEST_F(MetricsTest, reset) {
    Metrics metrics;
    metrics.master.recoveryTicks = 44;
    metrics.reset();
    EXPECT_EQ(0U, metrics.master.recoveryTicks);
}

TEST_F(MetricsTest, serialize) {
    Metrics metrics;
    metrics.master.recoveryTicks = 12345;
    string serialized;
    metrics.serialize(serialized);
    ProtoBuf::MetricList list;
    list.ParseFromString(serialized);
    for (int i = 0; i < list.metric_size(); i++) {
        const ProtoBuf::MetricList_Entry& metric = list.metric(i);
        if (metric.name().compare("master.recoveryTicks") == 0) {
            EXPECT_EQ(12345U, metric.value());
            return;
        }
    }
    FAIL() << "master.recoveryTicks record not found";
}

}
