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

void
BackupManager::dumpOpenSegments()
{
    BackupManager* mgr = const_cast<BackupManager*>(this);
    LOG(ERROR, "%lu open segments:", mgr->openSegmentList.size());
    foreach (auto& segment, mgr->openSegmentList) {
        LOG(ERROR, "Segment %lu", segment.segmentId);
        LOG(ERROR, "  data: %p", segment.data);
        LOG(ERROR, "  openLen: %u", segment.openLen);
        LOG(ERROR, "  offsetQueued: %u", segment.offsetQueued);
        LOG(ERROR, "  closeQueued: %u", segment.closeQueued);
        foreach (auto& backup, segment.backupIter()) {
            LOG(ERROR, "  Backup:%s", backup ? "" : " inactive");
            if (!backup)
                continue;
            LOG(ERROR, "    openIsDone: %u", backup->openIsDone);
            LOG(ERROR, "    offsetSent: %u", backup->offsetSent);
            LOG(ERROR, "    closeSent: %u", backup->closeSent);
            LOG(ERROR, "    RPC: %s", backup->rpc ? "active" : "inactive");
        }
        LOG(ERROR, " ");
    }
}

struct BackupManagerBaseTest : public ::testing::Test {
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

    BackupManagerBaseTest()
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

        backup1.construct(Context::get().transportManager->getSession(
                            "mock:host=backup1"));
        backup2.construct(Context::get().transportManager->getSession(
                            "mock:host=backup2"));

        serverId.construct(99);
        mgr.construct(coordinator.get(), serverId, 2);
    }
};

struct BackupManagerTest : public BackupManagerBaseTest {
    BackupManagerTest() {
        backupService1->init();
        backupService2->init();
    }
};

TEST_F(BackupManagerBaseTest, selectOpenHostsNotEnoughBackups) {
    auto seg = mgr->openSegment(88, NULL, 0);
    EXPECT_THROW(mgr->proceed(), InternalError);
    mgr->unopenSegment(seg); // so that destructor's sync is a no-op
}

TEST_F(BackupManagerTest, freeSegment) {
    mgr->freeSegment(88);
    mgr->openSegment(89, NULL, 0)->close();
    mgr->freeSegment(89);
    IGNORE_RESULT(mgr->openSegment(90, NULL, 0));
    mgr->freeSegment(90);

    ProtoBuf::Tablets will;
    EXPECT_EQ(0U, mgr->segments.size());

    {
        BackupClient::StartReadingData::Result result;
        backup1->startReadingData(99, will, &result);
        EXPECT_EQ(0U, result.segmentIdAndLength.size());
    }

    {
        BackupClient::StartReadingData::Result result;
        backup2->startReadingData(99, will, &result);
        EXPECT_EQ(0U, result.segmentIdAndLength.size());
    }
}

TEST_F(BackupManagerTest, sync) {
    mgr->openSegment(89, NULL, 0)->close();
    mgr->openSegment(90, NULL, 0)->close();
    mgr->openSegment(91, NULL, 0)->close();
    mgr->sync();
}

TEST_F(BackupManagerTest, openSegmentInsertOrder) {
    IGNORE_RESULT(mgr->openSegment(89, NULL, 0));
    IGNORE_RESULT(mgr->openSegment(79, NULL, 0));
    IGNORE_RESULT(mgr->openSegment(99, NULL, 0));
    auto it = mgr->openSegmentList.begin();
    EXPECT_EQ(89U, it++->segmentId);
    EXPECT_EQ(79U, it++->segmentId);
    EXPECT_EQ(99U, it++->segmentId);
}

TEST_F(BackupManagerTest, OpenSegmentVarLenArray) {
    // backups[0] must be the last member of OpenSegment
    BackupManager::OpenSegment* openSegment = NULL;
    EXPECT_EQ(static_cast<void*>(openSegment + 1),
              static_cast<void*>(openSegment->backups));
}

TEST_F(BackupManagerTest, OpenSegmentConstructor) {
    MockRandom _(1);
    const char data[] = "Hello world!";

    auto openSegment = mgr->openSegment(88, data, arrayLength(data));
    mgr->sync();

    // make sure we think data was written
    EXPECT_EQ(data, openSegment->data);
    EXPECT_EQ(arrayLength(data), openSegment->offsetQueued);
    EXPECT_FALSE(openSegment->closeQueued);
    foreach (auto& backup, openSegment->backupIter()) {
        EXPECT_EQ(arrayLength(data), backup->offsetSent);
        EXPECT_FALSE(backup->closeSent);
        EXPECT_FALSE(backup->rpc);
    }
    EXPECT_EQ(arrayLength(data), backupService1->bytesWritten);
    EXPECT_EQ(arrayLength(data), backupService2->bytesWritten);

    // make sure OpenSegment::backups point to reasonable service locators
    vector<string> backupLocators;
    foreach (auto& backup, openSegment->backupIter()) {
        backupLocators.push_back(
            backup->client.getSession()->getServiceLocator());
    }
    EXPECT_EQ((vector<string> {"mock:host=backup2", "mock:host=backup1"}),
              backupLocators);

    // TODO(ongaro): Unit test backup selection algorithm with varying disk
    // bandwidths

    // make sure BackupManager::segments looks sane
    std::set<string> segmentLocators;
    foreach (auto& s, mgr->segments) {
        EXPECT_EQ(88U, s.first);
        segmentLocators.insert(s.second->getServiceLocator());
    }
    EXPECT_EQ((std::set<string> {"mock:host=backup1", "mock:host=backup2"}),
              segmentLocators);
}

#if 0 // the sync method was deleted,
      // not sure if there's valuable stuff in here
TEST_F(BackupManagerTest, OpenSegmentsync) {
    const char data[] = "Hello world!";

    auto openSegment = mgr->openSegment(88, data, 0);
    openSegment->sync();
    openSegment->sync();
    openSegment->write(4, false);
    foreach (auto& backup, openSegment->backupIter()) {
        EXPECT_EQ(4U, backup->offsetSent);
        EXPECT_FALSE(backup->closeSent);
        EXPECT_TRUE(backup->rpc);
    }
    openSegment->sync();
    foreach (auto& backup, openSegment->backupIter()) {
        EXPECT_EQ(4U, backup->offsetSent);
        EXPECT_FALSE(backup->closeSent);
        EXPECT_FALSE(backup->rpc);
    }
    openSegment->write(6, false);
    openSegment->write(8, false);
    openSegment->sync();
    foreach (auto& backup, openSegment->backupIter()) {
        EXPECT_EQ(8U, backup->offsetSent);
        EXPECT_FALSE(backup->closeSent);
        EXPECT_FALSE(backup->rpc);
    }
    EXPECT_EQ(8U, backupService1->bytesWritten);
    EXPECT_EQ(8U, backupService2->bytesWritten);

    openSegment->write(9, true);
    openSegment = NULL;

    mgr->sync();
    EXPECT_EQ(9U, backupService1->bytesWritten);
    EXPECT_EQ(9U, backupService2->bytesWritten);
    EXPECT_EQ(0U, mgr->openSegmentList.size());
}
#endif

// TODO(ongaro): This is a test that really belongs in SegmentTest.cc, but the
// setup overhead is too high.
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
    seg.close();

    ASSERT_EQ(0U, mgr->openSegmentList.size());

    ProtoBuf::Tablets will;
    ProtoBuf::Tablets::Tablet& tablet(*will.add_tablet());
    tablet.set_table_id(123);
    tablet.set_start_object_id(0);
    tablet.set_end_object_id(100);
    tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
    tablet.set_user_data(0); // partition id

    foreach (auto v, mgr->segments) {
        BackupClient host(v.second);
        Buffer resp;
        BackupClient::StartReadingData::Result result;
        host.startReadingData(99, will, &result);
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

    free(segMem);
}

} // namespace RAMCloud
