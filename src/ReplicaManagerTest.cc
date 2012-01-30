/* Copyright (c) 2009-2011 Stanford University
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
#include "MockCluster.h"
#include "Memory.h"
#include "ReplicaManager.h"
#include "Segment.h"
#include "ShortMacros.h"

namespace RAMCloud {

struct ReplicaManagerTest : public ::testing::Test {
    MockCluster cluster;
    const uint32_t segmentSize;
    Tub<ReplicaManager> mgr;
    ServerId serverId;
    ServerList serverList;

    ReplicaManagerTest()
        : cluster()
        , segmentSize(1 << 16)
        , mgr()
        , serverId(99, 0)
        , serverList()
    {
        Context::get().logger->setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        ServerConfig config = ServerConfig::forTesting();
        config.services = {BACKUP_SERVICE, MEMBERSHIP_SERVICE};
        config.segmentSize = segmentSize;
        config.backup.numSegmentFrames = 4;
        config.localLocator = "mock:host=backup1";
        addToServerList(cluster.addServer(config));

        config.localLocator = "mock:host=backup2";
        addToServerList(cluster.addServer(config));

        mgr.construct(serverList, serverId, 2);
    }

    void addToServerList(Server* server)
    {
        serverList.add(server->serverId,
                       server->config.localLocator,
                       server->config.services,
                       server->config.backup.mockSpeed);
    }

    void dumpReplicatedSegments()
    {
        LOG(ERROR, "%lu open segments:", mgr->replicatedSegmentList.size());
        foreach (auto& segment, mgr->replicatedSegmentList) {
            LOG(ERROR, "Segment %lu", segment.segmentId);
            LOG(ERROR, "  data: %p", segment.data);
            LOG(ERROR, "  openLen: %u", segment.openLen);
            LOG(ERROR, "  queued.bytes: %u", segment.queued.bytes);
            LOG(ERROR, "  queued.close: %u", segment.queued.close);
            foreach (auto& replica, segment.replicas) {
                LOG(ERROR, "  Replica:%s", replica ? "" : " non-existent");
                if (!replica)
                    continue;
                LOG(ERROR, "    acked.open: %u", replica->acked.open);
                LOG(ERROR, "    sent.bytes: %u", replica->sent.bytes);
                LOG(ERROR, "    sent.close: %u", replica->sent.close);
                LOG(ERROR, "    Write RPC: %s", replica->writeRpc ?
                                                    "active" : "inactive");
                LOG(ERROR, "    Free RPC: %s", replica->freeRpc ?
                                                    "active" : "inactive");
            }
            LOG(ERROR, " ");
        }
    }
};

TEST_F(ReplicaManagerTest, openSegment) {
    MockRandom _(1);
    const char data[] = "Hello world!";

    auto segment = mgr->openSegment(88, data, arrayLength(data));

    ASSERT_FALSE(mgr->taskManager.isIdle());
    EXPECT_EQ(segment, mgr->taskManager.tasks.front());
    EXPECT_EQ(1u, mgr->replicatedSegmentList.size());
    EXPECT_EQ(segment, &mgr->replicatedSegmentList.front());

    segment->sync(segment->queued.bytes);

    // make sure we think data was written
    EXPECT_EQ(data, segment->data);
    EXPECT_EQ(arrayLength(data), segment->queued.bytes);
    EXPECT_FALSE(segment->queued.close);
    foreach (auto& replica, segment->replicas) {
        EXPECT_EQ(arrayLength(data), replica->sent.bytes);
        EXPECT_FALSE(replica->sent.close);
        EXPECT_FALSE(replica->writeRpc);
        EXPECT_FALSE(replica->freeRpc);
    }
    EXPECT_EQ(arrayLength(data), cluster.servers[0]->backup->bytesWritten);
    EXPECT_EQ(arrayLength(data), cluster.servers[1]->backup->bytesWritten);

    // make sure ReplicatedSegment::replicas point to ok service locators
    vector<string> backupLocators;
    foreach (auto& replica, segment->replicas) {
        backupLocators.push_back(
            replica->client.getSession()->getServiceLocator());
    }
    EXPECT_EQ((vector<string> {"mock:host=backup1", "mock:host=backup2"}),
              backupLocators);
}

// This is a test that really belongs in SegmentTest.cc, but the setup
// overhead is too high.
TEST_F(ReplicaManagerTest, writeSegment) {
    void* segMem = Memory::xmemalign(HERE, segmentSize, segmentSize);
    Segment seg(*serverId, 88, segMem, segmentSize, mgr.get());
    SegmentHeader header = { *serverId, 88, segmentSize };
    seg.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header));
    Object object(sizeof(object));
    object.id.objectId = 10;
    object.id.tableId = 123;
    object.version = 0;
    seg.append(LOG_ENTRY_TYPE_OBJ, &object, sizeof(object));
    seg.close(NULL);

    EXPECT_EQ(1U, mgr->replicatedSegmentList.size());

    ProtoBuf::Tablets will;
    ProtoBuf::Tablets::Tablet& tablet(*will.add_tablet());
    tablet.set_table_id(123);
    tablet.set_start_object_id(0);
    tablet.set_end_object_id(100);
    tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
    tablet.set_user_data(0); // partition id

    foreach (auto& segment, mgr->replicatedSegmentList) {
        foreach (auto& replica, segment.replicas) {
            ASSERT_TRUE(replica);
            BackupClient& host(replica->client);
            Buffer resp;
            host.startReadingData(serverId, will);
            while (true) {
                try {
                    host.getRecoveryData(serverId, 88, 0, resp);
                } catch (const RetryException& e) {
                    resp.reset();
                    continue;
                }
                break;
            }
            auto* entry = resp.getStart<SegmentEntry>();
            EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, entry->type);
            EXPECT_EQ(sizeof(Object), entry->length);
            resp.truncateFront(sizeof(*entry));
            auto* obj = resp.getStart<Object>();
            EXPECT_EQ(10U, obj->id.objectId);
            EXPECT_EQ(123U, obj->id.tableId);
        }
    }

    free(segMem);
}

TEST_F(ReplicaManagerTest, proceed) {
    mgr->openSegment(89, NULL, 0)->close(NULL);
    auto& segment = mgr->replicatedSegmentList.front();
    EXPECT_FALSE(segment.replicas[0]);
    mgr->proceed();
    ASSERT_TRUE(segment.replicas[0]);
    EXPECT_TRUE(segment.replicas[0]->writeRpc);
}

TEST_F(ReplicaManagerTest, clusterConfigurationChanged) {
    // TODO(stutsman): Write once this method does something interesting.
}

TEST_F(ReplicaManagerTest, destroyAndFreeReplicatedSegment) {
    auto* segment = mgr->openSegment(89, NULL, 0);
    segment->sync(0);
    while (!mgr->taskManager.isIdle())
        mgr->proceed(); // Make sure the close gets pushed out as well.
    EXPECT_FALSE(mgr->replicatedSegmentList.empty());
    EXPECT_EQ(segment, &mgr->replicatedSegmentList.front());
    mgr->destroyAndFreeReplicatedSegment(segment);
    EXPECT_TRUE(mgr->replicatedSegmentList.empty());
}

} // namespace RAMCloud
