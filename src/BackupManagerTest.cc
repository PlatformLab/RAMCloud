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

#pragma GCC diagnostic ignored "-Weffc++"

#include <boost/scoped_ptr.hpp>
#include <set>

#include "TestUtil.h"
#include "Common.h"
#include "CoordinatorClient.h"
#include "CoordinatorService.h"
#include "BackupClient.h"
#include "BackupManager.h"
#include "BackupService.h"
#include "BackupStorage.h"
#include "BindTransport.h"
#include "MasterService.h"
#include "Memory.h"
#include "Segment.h"
#include "ShortMacros.h"

namespace RAMCloud {

struct BackupManagerTest : public ::testing::Test {
    const uint32_t segmentSize;
    const uint32_t segmentFrames;
    const char* coordinatorLocator;
    Tub<BindTransport> transport;
    Tub<TransportManager::MockRegistrar> mockRegistrar;
    Tub<CoordinatorService> coordinatorService;
    Tub<CoordinatorClient> coordinator;
    Tub<InMemoryStorage> storage1;
    Tub<InMemoryStorage> storage2;
    Tub<BackupService::Config> backupServiceConfig1;
    Tub<BackupService::Config> backupServiceConfig2;
    Tub<BackupService> backupService1;
    Tub<BackupService> backupService2;
    Tub<BackupClient> backup1;
    Tub<BackupClient> backup2;
    Tub<uint64_t> serverId;
    Tub<BackupManager> mgr;

    BackupManagerTest()
        : segmentSize(1 << 16)
        , segmentFrames(4)
        , coordinatorLocator("mock:host=coordinator")
    {
        transport.construct();
        mockRegistrar.construct(*transport);

        coordinatorService.construct();
        transport->addService(*coordinatorService, coordinatorLocator,
                COORDINATOR_SERVICE);

        coordinator.construct(coordinatorLocator);

        storage1.construct(segmentSize, segmentFrames);
        storage2.construct(segmentSize, segmentFrames);

        backupServiceConfig1.construct();
        backupServiceConfig1->coordinatorLocator = coordinatorLocator;
        backupServiceConfig1->localLocator = "mock:host=backup1";

        backupServiceConfig2.construct();
        backupServiceConfig2->coordinatorLocator = coordinatorLocator;
        backupServiceConfig2->localLocator = "mock:host=backup2";

        backupService1.construct(*backupServiceConfig1, *storage1);
        backupService2.construct(*backupServiceConfig2, *storage2);

        transport->addService(*backupService1, "mock:host=backup1",
                BACKUP_SERVICE);
        transport->addService(*backupService2, "mock:host=backup2",
                BACKUP_SERVICE);

        backupService1->init();
        backupService2->init();

        backup1.construct(Context::get().transportManager->getSession(
                            "mock:host=backup1"));
        backup2.construct(Context::get().transportManager->getSession(
                            "mock:host=backup2"));

        serverId.construct(99);
        mgr.construct(coordinator.get(), serverId, 2);
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

TEST_F(BackupManagerTest, openSegment) {
    MockRandom _(1);
    const char data[] = "Hello world!";

    auto segment = mgr->openSegment(88, data, arrayLength(data));

    ASSERT_FALSE(mgr->taskManager.isIdle());
    EXPECT_EQ(segment, mgr->taskManager.tasks.front());
    EXPECT_EQ(1u, mgr->replicatedSegmentList.size());
    EXPECT_EQ(segment, &mgr->replicatedSegmentList.front());

    mgr->sync();

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
    EXPECT_EQ(arrayLength(data), backupService1->bytesWritten);
    EXPECT_EQ(arrayLength(data), backupService2->bytesWritten);

    // make sure ReplicatedSegment::replicas point to ok service locators
    vector<string> backupLocators;
    foreach (auto& replica, segment->replicas) {
        backupLocators.push_back(
            replica->client.getSession()->getServiceLocator());
    }
    EXPECT_EQ((vector<string> {"mock:host=backup2", "mock:host=backup1"}),
              backupLocators);
}

// This is a test that really belongs in SegmentTest.cc, but the setup
// overhead is too high.
TEST_F(BackupManagerTest, writeSegment) {
    void* segMem = Memory::xmemalign(HERE, segmentSize, segmentSize);
    Segment seg(99, 88, segMem, segmentSize, mgr.get());
    SegmentHeader header = { 99, 88, segmentSize };
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
            BackupClient::StartReadingData::Result result;
            host.startReadingData(ServerId(99), will, &result);
            while (true) {
                try {
                    host.getRecoveryData(99, 88, 0, resp);
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

TEST_F(BackupManagerTest, proceed) {
    mgr->openSegment(89, NULL, 0)->close(NULL);
    auto& segment = mgr->replicatedSegmentList.front();
    EXPECT_FALSE(segment.replicas[0]);
    mgr->proceed();
    ASSERT_TRUE(segment.replicas[0]);
    EXPECT_TRUE(segment.replicas[0]->writeRpc);
}

TEST_F(BackupManagerTest, sync) {
    mgr->openSegment(89, NULL, 0)->close(NULL);
    mgr->openSegment(90, NULL, 0)->close(NULL);
    mgr->openSegment(91, NULL, 0)->close(NULL);
    mgr->sync();
}

TEST_F(BackupManagerTest, clusterConfigurationChanged) {
    // TODO(stutsman): Write once this method does something interesting.
}

TEST_F(BackupManagerTest, isSynced) {
    mgr->openSegment(89, NULL, 0)->close(NULL);
    mgr->openSegment(90, NULL, 0)->close(NULL);
    EXPECT_FALSE(mgr->isSynced());
    mgr->proceed(); // send opens
    EXPECT_FALSE(mgr->isSynced());
    mgr->proceed(); // reap opens
    EXPECT_FALSE(mgr->isSynced());
    mgr->proceed(); // send closes
    EXPECT_FALSE(mgr->isSynced());
    mgr->proceed(); // reap closes
    EXPECT_TRUE(mgr->isSynced());
}

TEST_F(BackupManagerTest, destroyAndFreeReplicatedSegment) {
    auto* segment = mgr->openSegment(89, NULL, 0);
    mgr->sync();
    EXPECT_FALSE(mgr->replicatedSegmentList.empty());
    EXPECT_EQ(segment, &mgr->replicatedSegmentList.front());
    mgr->destroyAndFreeReplicatedSegment(segment);
    EXPECT_TRUE(mgr->replicatedSegmentList.empty());
}

} // namespace RAMCloud
