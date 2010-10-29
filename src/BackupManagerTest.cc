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
#include "Segment.h"

namespace RAMCloud {

/**
 * Unit tests for BackupManager.
 */
class BackupManagerTest : public CppUnit::TestFixture {

    CPPUNIT_TEST_SUITE(BackupManagerTest);
    CPPUNIT_TEST(test_closeSegment);
    CPPUNIT_TEST(test_freeSegment);
    CPPUNIT_TEST(test_openSegment);
    CPPUNIT_TEST(test_writeSegment);
    CPPUNIT_TEST(test_selectOpenHosts);
    CPPUNIT_TEST(test_selectOpenHosts_notEnoughBackups);
    CPPUNIT_TEST(test_selectOpenHosts_alreadyOpen);
    CPPUNIT_TEST_SUITE_END();

    BackupServer* backupServer1;
    BackupServer* backupServer2;
    CoordinatorClient* coordinator;
    CoordinatorServer* coordinatorServer;
    BackupServer::Config* config;
    BackupManager* mgr;
    const uint32_t segmentSize;
    const uint32_t segmentFrames;
    BackupStorage* storage1;
    BackupStorage* storage2;
    BindTransport* transport;

  public:
    BackupManagerTest()
        : backupServer1()
        , backupServer2()
        , coordinator()
        , coordinatorServer()
        , config()
        , mgr()
        , segmentSize(1 << 16)
        , segmentFrames(2)
        , storage1()
        , storage2()
        , transport()
    {
        logger.setLogLevels(SILENT_LOG_LEVEL);
    }

    void
    setUp(bool enlist)
    {
        if (!enlist)
            tearDown();

        transport = new BindTransport;
        transportManager.registerMock(transport);

        config = new BackupServer::Config;
        config->coordinatorLocator = "mock:host=coordinator";

        coordinatorServer = new CoordinatorServer;
        transport->addServer(*coordinatorServer, config->coordinatorLocator);

        coordinator = new CoordinatorClient(config->coordinatorLocator.c_str());

        storage1 = new InMemoryStorage(segmentSize, segmentFrames);
        storage2 = new InMemoryStorage(segmentSize, segmentFrames);

        backupServer1 = new BackupServer(*config, *storage1);
        backupServer2 = new BackupServer(*config, *storage2);

        transport->addServer(*backupServer1, "mock:host=backup1");
        transport->addServer(*backupServer2, "mock:host=backup2");

        if (enlist) {
            coordinator->enlistServer(BACKUP, "mock:host=backup1");
            coordinator->enlistServer(BACKUP, "mock:host=backup2");
        }

        mgr = new BackupManager(*coordinator);
    }

    void
    setUp()
    {
        setUp(true);
    }


    void
    tearDown()
    {
        delete mgr;
        delete backupServer2;
        delete backupServer1;
        delete storage2;
        delete storage1;
        delete coordinator;
        delete coordinatorServer;
        delete config;
        transportManager.unregisterMock();
        delete transport;
        CPPUNIT_ASSERT_EQUAL(0,
            BackupStorage::Handle::resetAllocatedHandlesCount());
    }

    void
    test_closeSegment()
    {
        char segMem[segmentSize];
        Segment seg(99, 88, segMem, segmentSize, mgr);
        CPPUNIT_ASSERT_EQUAL(2, mgr->openHosts.size());

        seg.close();

        CPPUNIT_ASSERT(mgr->openHosts.empty());
        CPPUNIT_ASSERT_EQUAL(2,
            BackupStorage::Handle::resetAllocatedHandlesCount());
    }

    void
    test_freeSegment()
    {
        mgr->openSegment(99, 88);
        mgr->closeSegment(99, 88);
        mgr->freeSegment(99, 88);
        BackupManager::SegmentMap::iterator it = mgr->segments.find(88);
        CPPUNIT_ASSERT(mgr->segments.end() == it);
    }

    void
    test_openSegment()
    {
        mgr->openSegment(99, 88);
        CPPUNIT_ASSERT_EQUAL(2, mgr->openHosts.size());
        BackupManager::SegmentMap::iterator it = mgr->segments.find(88);
        CPPUNIT_ASSERT(mgr->segments.end() != it);
        it++;
        CPPUNIT_ASSERT(mgr->segments.end() != it);
        it++;
        CPPUNIT_ASSERT(mgr->segments.end() == it);
        CPPUNIT_ASSERT_EQUAL(2,
            BackupStorage::Handle::resetAllocatedHandlesCount());
    }

    void
    test_writeSegment()
    {
        char segMem[segmentSize];
        Segment seg(99, 88, segMem, segmentSize, mgr);
        SegmentHeader header = { 99, 88, segmentSize };
        seg.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header));
        seg.close();
        foreach (BackupClient* host, mgr->openHosts) {
            Buffer resp;
            host->getRecoveryData(99, 88, TabletMap(), resp);
            const SegmentEntry* entry = resp.getStart<SegmentEntry>();
            CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_SEGHEADER, entry->type);
            CPPUNIT_ASSERT_EQUAL(sizeof(SegmentHeader), entry->length);
            resp.truncateFront(sizeof(*entry));
            const SegmentHeader* header = resp.getStart<SegmentHeader>();
            CPPUNIT_ASSERT_EQUAL(99, header->logId);
            CPPUNIT_ASSERT_EQUAL(88, header->segmentId);
            CPPUNIT_ASSERT_EQUAL(segmentSize, header->segmentCapacity);
        }
        CPPUNIT_ASSERT_EQUAL(2,
            BackupStorage::Handle::resetAllocatedHandlesCount());
    }

    void
    test_selectOpenHosts()
    {
        mgr->openSegment(99, 88);
        CPPUNIT_ASSERT_EQUAL(2, mgr->openHosts.size());
        CPPUNIT_ASSERT_EQUAL(2,
            BackupStorage::Handle::resetAllocatedHandlesCount());
    }

    void
    test_selectOpenHosts_notEnoughBackups()
    {
        setUp(false);
        CPPUNIT_ASSERT_THROW(mgr->openSegment(99, 88),
                             FatalError);
        CPPUNIT_ASSERT_EQUAL(0,
            BackupStorage::Handle::resetAllocatedHandlesCount());
    }

    void
    test_selectOpenHosts_alreadyOpen()
    {
        mgr->openSegment(99, 88);
        CPPUNIT_ASSERT_THROW(mgr->openSegment(99, 88),
                             FatalError);
        CPPUNIT_ASSERT_EQUAL(2,
            BackupStorage::Handle::resetAllocatedHandlesCount());
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(BackupManagerTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(BackupManagerTest);

} // namespace RAMCloud
