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
#include "UpdateReplicationEpochTask.h"
#include "MockCluster.h"
#include "ServerConfig.h"

namespace RAMCloud {

class UpdateReplicationEpochTaskTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    CoordinatorService* service;
    ServerId serverId;
    TaskQueue taskQueue;
    Tub<UpdateReplicationEpochTask> epoch;
    CoordinatorServerList* serverList;

    UpdateReplicationEpochTaskTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , service(cluster.coordinator.get())
        , serverId()
        , taskQueue()
        , epoch()
        , serverList(service->context->coordinatorServerList)
    {
        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::MASTER_SERVICE};
        serverId = cluster.addServer(config)->serverId;
        epoch.construct(&context, &taskQueue, &serverId);
    }

    DISALLOW_COPY_AND_ASSIGN(UpdateReplicationEpochTaskTest);
};

TEST_F(UpdateReplicationEpochTaskTest, isAtLeast) {
    epoch->updateToAtLeast(1, 0); // first request
    EXPECT_FALSE(epoch->isAtLeast(1, 0));
    taskQueue.performTask(); // send rpc
    EXPECT_FALSE(epoch->isAtLeast(1, 0));
    epoch->updateToAtLeast(2, 0); // request while rpc outstanding
    epoch->updateToAtLeast(2, 1); // request while rpc outstanding
    EXPECT_FALSE(epoch->isAtLeast(1, 0));
    taskQueue.performTask(); // reap rpc
    EXPECT_TRUE(epoch->isAtLeast(1, 0));
    EXPECT_FALSE(epoch->isAtLeast(2, 1));
    taskQueue.performTask(); // send rpc
    taskQueue.performTask(); // reap rpc
    EXPECT_TRUE(epoch->isAtLeast(2, 0));
    EXPECT_TRUE(epoch->isAtLeast(2, 1));
    EXPECT_FALSE(epoch->isAtLeast(2, 2));
    EXPECT_FALSE(epoch->isAtLeast(3, 0));
}

TEST_F(UpdateReplicationEpochTaskTest, updateToAtLeast) {
    CoordinatorServerList::Entry entry = (*serverList)[serverId];
    auto coordRecoveryInfo = &entry.masterRecoveryInfo;
    typedef UpdateReplicationEpochTask::ReplicationEpoch Epoch;
    epoch->updateToAtLeast(1, 0); // first request
    EXPECT_EQ(Epoch(1, 0), epoch->requested);
    EXPECT_EQ(Epoch(0, 0), epoch->sent);
    EXPECT_EQ(Epoch(0, 0), epoch->current);
    EXPECT_EQ(0lu, coordRecoveryInfo->min_open_segment_id());
    EXPECT_EQ(0lu, coordRecoveryInfo->min_open_segment_epoch());
    EXPECT_TRUE(epoch->isScheduled());
    taskQueue.performTask(); // send rpc
    epoch->updateToAtLeast(1, 1); // request while rpc outstanding
    EXPECT_EQ(Epoch(1, 1), epoch->requested);
    EXPECT_EQ(Epoch(1, 0), epoch->sent);
    EXPECT_EQ(Epoch(0, 0), epoch->current);
    entry = (*serverList)[serverId];
    coordRecoveryInfo = &entry.masterRecoveryInfo;
    EXPECT_EQ(1lu, coordRecoveryInfo->min_open_segment_id());
    EXPECT_EQ(0lu, coordRecoveryInfo->min_open_segment_epoch());
    EXPECT_TRUE(epoch->rpc);
    EXPECT_TRUE(epoch->isScheduled());
    taskQueue.performTask(); // reap rpc
    EXPECT_EQ(Epoch(1, 1), epoch->requested);
    EXPECT_EQ(Epoch(1, 0), epoch->sent);
    EXPECT_EQ(Epoch(1, 0), epoch->current);
    entry = (*serverList)[serverId];
    coordRecoveryInfo = &entry.masterRecoveryInfo;
    EXPECT_EQ(1lu, coordRecoveryInfo->min_open_segment_id());
    EXPECT_EQ(0lu, coordRecoveryInfo->min_open_segment_epoch());
    EXPECT_FALSE(epoch->rpc);
    EXPECT_TRUE(epoch->isScheduled());
    taskQueue.performTask(); // send rpc
    taskQueue.performTask(); // reap rpc
    EXPECT_EQ(Epoch(1, 1), epoch->requested);
    EXPECT_EQ(Epoch(1, 1), epoch->sent);
    EXPECT_EQ(Epoch(1, 1), epoch->current);
    entry = (*serverList)[serverId];
    coordRecoveryInfo = &entry.masterRecoveryInfo;
    EXPECT_EQ(1lu, coordRecoveryInfo->min_open_segment_id());
    EXPECT_EQ(1lu, coordRecoveryInfo->min_open_segment_epoch());
    EXPECT_FALSE(epoch->rpc);
    EXPECT_FALSE(epoch->isScheduled());
}

}  // namespace RAMCloud
