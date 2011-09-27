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
#include "RawMetrics.h"
#include "MetricList.pb.h"

namespace RAMCloud {

class MetricsTest : public ::testing::Test {
  public:
    MetricsTest() { }
    DISALLOW_COPY_AND_ASSIGN(MetricsTest);
};

TEST_F(MetricsTest, serialize) {
    RawMetrics metrics;
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
