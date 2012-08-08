/* Copyright (c) 2012 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"
#include "MinOpenSegmentId.h"
#include "MockCluster.h"
#include "ServerConfig.h"

namespace RAMCloud {

class MinOpenSegmentIdTest : public ::testing::Test {
  public:
    Context context;
    MockCluster cluster;
    CoordinatorService* service;
    ServerId serverId;
    TaskQueue taskQueue;
    MinOpenSegmentId min;
    CoordinatorServerList* serverList;

    MinOpenSegmentIdTest()
        : context()
        , cluster(context)
        , service(cluster.coordinator.get())
        , serverId()
        , taskQueue()
        , min(context, &taskQueue, &serverId)
        , serverList(service->context.coordinatorServerList)
    {
        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::MASTER_SERVICE};
        serverId = cluster.addServer(config)->serverId;
    }

    DISALLOW_COPY_AND_ASSIGN(MinOpenSegmentIdTest);
};

TEST_F(MinOpenSegmentIdTest, isGreaterThan) {
    min.updateToAtLeast(1lu); // first request
    EXPECT_FALSE(min.isGreaterThan(0lu));
    taskQueue.performTask(); // send rpc
    EXPECT_FALSE(min.isGreaterThan(0lu));
    min.updateToAtLeast(2lu); // request while rpc outstanding
    EXPECT_FALSE(min.isGreaterThan(0lu));
    taskQueue.performTask(); // reap rpc
    EXPECT_TRUE(min.isGreaterThan(0lu));
    EXPECT_FALSE(min.isGreaterThan(1lu));
    taskQueue.performTask(); // send rpc
    taskQueue.performTask(); // reap rpc
    EXPECT_TRUE(min.isGreaterThan(1lu));
}

TEST_F(MinOpenSegmentIdTest, updateToAtLeast) {
    uint64_t coordMin = (*serverList)[serverId].minOpenSegmentId;
    min.updateToAtLeast(1lu); // first request
    EXPECT_EQ(1lu, min.requested);
    EXPECT_EQ(0lu, min.sent);
    EXPECT_EQ(0lu, min.current);
    EXPECT_EQ(0lu, coordMin);
    EXPECT_TRUE(min.isScheduled());
    taskQueue.performTask(); // send rpc
    coordMin = (*serverList)[serverId].minOpenSegmentId;
    min.updateToAtLeast(2lu); // request while rpc outstanding
    EXPECT_EQ(2lu, min.requested);
    EXPECT_EQ(1lu, min.sent);
    EXPECT_EQ(0lu, min.current);
    EXPECT_EQ(1lu, coordMin);
    EXPECT_TRUE(min.rpc);
    EXPECT_TRUE(min.isScheduled());
    taskQueue.performTask(); // reap rpc
    coordMin = (*serverList)[serverId].minOpenSegmentId;
    EXPECT_EQ(2lu, min.requested);
    EXPECT_EQ(1lu, min.sent);
    EXPECT_EQ(1lu, min.current);
    EXPECT_EQ(1lu, coordMin);
    EXPECT_FALSE(min.rpc);
    EXPECT_TRUE(min.isScheduled());
    taskQueue.performTask(); // send rpc
    taskQueue.performTask(); // reap rpc
    coordMin = (*serverList)[serverId].minOpenSegmentId;
    EXPECT_EQ(2lu, min.requested);
    EXPECT_EQ(2lu, min.sent);
    EXPECT_EQ(2lu, min.current);
    EXPECT_EQ(2lu, coordMin);
    EXPECT_FALSE(min.rpc);
    EXPECT_FALSE(min.isScheduled());
}

}  // namespace RAMCloud
