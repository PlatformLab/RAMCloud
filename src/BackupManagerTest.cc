/* Copyright (c) 2009 Stanford University
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
#include "CoordinatorServer.h"
#include "BackupClient.h"
#include "BackupManager.h"
#include "BackupServer.h"
#include "BackupStorage.h"
#include "BindTransport.h"
#include "Logging.h"
#include "MasterServer.h"
#include "Segment.h"

namespace RAMCloud {

struct BackupManagerBaseTest : public ::testing::Test {
    const uint32_t segmentSize;
    const uint32_t segmentFrames;
    const char* coordinatorLocator;
    ObjectTub<BindTransport> transport;
    ObjectTub<TransportManager::MockRegistrar> mockRegistrar;
    ObjectTub<CoordinatorServer> coordinatorServer;
    ObjectTub<CoordinatorClient> coordinator;
    ObjectTub<InMemoryStorage> storage1;
    ObjectTub<InMemoryStorage> storage2;
    ObjectTub<BackupServer::Config> backupServerConfig;
    ObjectTub<BackupServer> backupServer1;
    ObjectTub<BackupServer> backupServer2;
    ObjectTub<BackupClient> backup1;
    ObjectTub<BackupClient> backup2;
    ObjectTub<BackupManager> mgr;

    BackupManagerBaseTest()
        : segmentSize(1 << 16)
        , segmentFrames(4)
        , coordinatorLocator("mock:host=coordinator")
    {
        transport.construct();
        mockRegistrar.construct(*transport);

        coordinatorServer.construct();
        transport->addServer(*coordinatorServer, coordinatorLocator);

        coordinator.construct(coordinatorLocator);

        storage1.construct(segmentSize, segmentFrames);
        storage2.construct(segmentSize, segmentFrames);

        backupServerConfig.construct();
        backupServerConfig->coordinatorLocator = coordinatorLocator;

        backupServer1.construct(*backupServerConfig, *storage1);
        backupServer2.construct(*backupServerConfig, *storage2);

        transport->addServer(*backupServer1, "mock:host=backup1");
        transport->addServer(*backupServer2, "mock:host=backup2");

        backup1.construct(transportManager.getSession("mock:host=backup1"));
        backup2.construct(transportManager.getSession("mock:host=backup2"));

        mgr.construct(coordinator.get(), 99, 2);
    }
};

struct BackupManagerTest : public BackupManagerBaseTest {
    BackupManagerTest() {
        coordinator->enlistServer(BACKUP, "mock:host=backup1");
        coordinator->enlistServer(BACKUP, "mock:host=backup2");
    }
};

TEST_F(BackupManagerBaseTest, selectOpenHostsNotEnoughBackups) {
    logger.setLogLevels(SILENT_LOG_LEVEL);
    EXPECT_THROW(IGNORE_RESULT(mgr->openSegment(88, NULL, 0)), FatalError);
}

TEST_F(BackupManagerTest, freeSegment) {
    mgr->freeSegment(88);
    mgr->openSegment(89, NULL, 0)->close();
    mgr->freeSegment(89);
    IGNORE_RESULT(mgr->openSegment(90, NULL, 0));
    mgr->freeSegment(90);

    ProtoBuf::Tablets will;
    EXPECT_EQ(0U, mgr->segments.size());
    EXPECT_EQ(0U, backup1->startReadingData(99, will).size());
    EXPECT_EQ(0U, backup2->startReadingData(99, will).size());
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

    // make sure we think data was written
    EXPECT_EQ(data, openSegment->data);
    EXPECT_EQ(arrayLength(data), openSegment->offsetQueued);
    EXPECT_FALSE(openSegment->closeQueued);
    foreach (auto& backup, openSegment->backupIter()) {
        EXPECT_EQ(arrayLength(data), backup.offsetSent);
        EXPECT_FALSE(backup.closeSent);
        EXPECT_FALSE(backup.writeSegmentTub);
    }
    EXPECT_EQ(arrayLength(data), backupServer1->bytesWritten);
    EXPECT_EQ(arrayLength(data), backupServer2->bytesWritten);

    // make sure OpenSegment::backups point to reasonable service locators
    vector<string> backupLocators;
    foreach (auto& backup, openSegment->backupIter()) {
        backupLocators.push_back(
            backup.client.getSession()->getServiceLocator());
    }
    EXPECT_EQ((vector<string> {"mock:host=backup2", "mock:host=backup1"}),
              backupLocators);

    // make sure BackupManager::segments looks sane
    std::set<string> segmentLocators;
    foreach (auto& s, mgr->segments) {
        EXPECT_EQ(88U, s.first);
        segmentLocators.insert(s.second->getServiceLocator());
    }
    EXPECT_EQ((std::set<string> {"mock:host=backup1", "mock:host=backup2"}),
              segmentLocators);
}

TEST_F(BackupManagerTest, OpenSegmentwriteAssertWriteAfterClose) {
    const char data[] = "Hello world!";
    auto openSegment = mgr->openSegment(88, data, 0);
    openSegment->write(4, true);
    EXPECT_DEATH(openSegment->write(5, true), "Assertion");
}

TEST_F(BackupManagerTest, OpenSegmentwriteAssertNonAppending) {
    const char data[] = "Hello world!";
    auto openSegment = mgr->openSegment(88, data, 0);
    openSegment->write(4, false);
    openSegment->write(4, false); // OK
    EXPECT_DEATH(openSegment->write(3, false), "Assertion");
}

TEST_F(BackupManagerTest, OpenSegmentwriteAssertPrevOpen) {
    const char data[] = "Hello world!";
    IGNORE_RESULT(mgr->openSegment(88, NULL, 0));
    auto openSegment = mgr->openSegment(89, data, 0);
    EXPECT_DEATH(openSegment->write(1, false), "Assertion");
}

TEST_F(BackupManagerTest, OpenSegmentsync) {
    const char data[] = "Hello world!";

    auto openSegment = mgr->openSegment(88, data, 0);
    openSegment->sync();
    openSegment->sync();
    openSegment->write(4, false);
    foreach (auto& backup, openSegment->backupIter()) {
        EXPECT_EQ(4U, backup.offsetSent);
        EXPECT_FALSE(backup.closeSent);
        EXPECT_TRUE(backup.writeSegmentTub);
    }
    openSegment->sync();
    foreach (auto& backup, openSegment->backupIter()) {
        EXPECT_EQ(4U, backup.offsetSent);
        EXPECT_FALSE(backup.closeSent);
        EXPECT_FALSE(backup.writeSegmentTub);
    }
    openSegment->write(6, false);
    openSegment->write(8, false);
    openSegment->sync();
    foreach (auto& backup, openSegment->backupIter()) {
        EXPECT_EQ(8U, backup.offsetSent);
        EXPECT_FALSE(backup.closeSent);
        EXPECT_FALSE(backup.writeSegmentTub);
    }
    EXPECT_EQ(8U, backupServer1->bytesWritten);
    EXPECT_EQ(8U, backupServer2->bytesWritten);

    openSegment->write(9, true);
    openSegment = NULL;

    mgr->sync();
    EXPECT_EQ(9U, backupServer1->bytesWritten);
    EXPECT_EQ(9U, backupServer2->bytesWritten);
    EXPECT_EQ(0U, mgr->openSegmentList.size());
}

// TODO(ongaro): This is a test that really belongs in SegmentTest.cc, but the
// setup overhead is too high.
TEST_F(BackupManagerTest, writeSegment) {
    char segMem[segmentSize];
    Segment seg(99, 88, segMem, segmentSize, mgr.get());
    SegmentHeader header = { 99, 88, segmentSize };
    seg.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header));
    Object object(sizeof(object));
    object.id = 10;
    object.table = 123;
    object.version = 0;
    object.checksum = 0xff00ff00ff00;
    object.data_len = 0;
    seg.append(LOG_ENTRY_TYPE_OBJ, &object, sizeof(object));
    seg.close();

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
        host.startReadingData(99, will);
        host.getRecoveryData(99, 88, 0, resp);
        auto* entry = resp.getStart<SegmentEntry>();
        EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, entry->type);
        EXPECT_EQ(sizeof(Object), entry->length);
        resp.truncateFront(sizeof(*entry));
        auto* obj = resp.getStart<Object>();
        EXPECT_EQ(10U, obj->id);
        EXPECT_EQ(123U, obj->table);
    }
}

} // namespace RAMCloud
