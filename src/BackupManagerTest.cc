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

#include <boost/scoped_ptr.hpp>

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

/**
 * Unit tests for BackupManager.
 */
class BackupManagerTest : public CppUnit::TestFixture {

    CPPUNIT_TEST_SUITE(BackupManagerTest);
    CPPUNIT_TEST(test_closeSegment);
    CPPUNIT_TEST(test_freeSegment);
    CPPUNIT_TEST(test_freeSegment_overlapping);
    CPPUNIT_TEST(test_openSegment);
    CPPUNIT_TEST(test_recover);
    CPPUNIT_TEST(test_recover_failedToRecoverAll);
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
        logger.setLogLevels(DEBUG);
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

        mgr = new BackupManager(coordinator);
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
        mgr->openSegment(99, 88);
        mgr->closeSegment(99, 88);

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
    test_freeSegment_overlapping()
    {
        // The end condition on the iterators is important, so this
        // checks to make sure the frees work without freeing segmentIds
        // that follow in the map.
        mgr->openSegment(99, 87);
        mgr->closeSegment(99, 87);
        mgr->openSegment(99, 88);
        mgr->closeSegment(99, 88);
        mgr->freeSegment(99, 87);
        mgr->freeSegment(99, 88);
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

    static bool
    recoverSegmentFilter(string s)
    {
        return (s == "recoverSegment" || s == "recover");
    }

    MasterServer*
    createMasterServer()
    {
        ServerConfig config;
        config.coordinatorLocator = "mock:host=coordinator";
        MasterServer::sizeLogAndHashTable("64", "8", &config);
        return new MasterServer(config, coordinator, mgr);
    }

    void
    appendTablet(ProtoBuf::Tablets& tablets,
                 uint64_t partitionId,
                 uint32_t tableId,
                 uint64_t start, uint64_t end)
    {
        ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
        tablet.set_table_id(tableId);
        tablet.set_start_object_id(start);
        tablet.set_end_object_id(end);
        tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
        tablet.set_user_data(partitionId);
    }

    void
    createTabletList(ProtoBuf::Tablets& tablets)
    {
        appendTablet(tablets, 0, 123, 0, 9);
        appendTablet(tablets, 0, 123, 10, 19);
        appendTablet(tablets, 0, 123, 20, 29);
        appendTablet(tablets, 0, 124, 20, 100);
    }

    void
    test_recover()
    {
        boost::scoped_ptr<MasterServer> master(createMasterServer());

        // Give them a name so that freeSegment doesn't get called on
        // destructor until after the test.
        char segMem1[segmentSize];
        Segment s1(99, 87, &segMem1, sizeof(segMem1), mgr);
        s1.close();
        char segMem2[segmentSize];
        Segment s2(99, 88, &segMem2, sizeof(segMem2), mgr);
        s2.close();

        BackupClient(transportManager.getSession("mock:host=backup1"))
            .startReadingData(99);
        BackupClient(transportManager.getSession("mock:host=backup2"))
            .startReadingData(99);

        ProtoBuf::Tablets tablets;
        createTabletList(tablets);

        ProtoBuf::ServerList backups; {
            ProtoBuf::ServerList_Entry& server(*backups.add_server());
            server.set_server_type(ProtoBuf::BACKUP);
            server.set_server_id(99);
            server.set_segment_id(87);
            server.set_service_locator("mock:host=backup1");
        }{
            ProtoBuf::ServerList_Entry& server(*backups.add_server());
            server.set_server_type(ProtoBuf::BACKUP);
            server.set_server_id(99);
            server.set_segment_id(88);
            server.set_service_locator("mock:host=backup1");

        }{
            ProtoBuf::ServerList_Entry& server(*backups.add_server());
            server.set_server_type(ProtoBuf::BACKUP);
            server.set_server_id(99);
            server.set_segment_id(88);
            server.set_service_locator("mock:host=backup2");
        }

        TestLog::Enable _(&recoverSegmentFilter);
        srand(0);
        mgr->recover(*master, 99, tablets, backups);
        CPPUNIT_ASSERT_EQUAL(
            "recover: Recovering master 99, 4 tablets, 3 hosts | "
            "recover: Waiting on recovery data for segment 87 from "
            "mock:host=backup1 | "
            "recover: Got it: 65536 bytes | "
            "recover: Recovering with segment size 65536 | "
            "recoverSegment: recoverSegment 87, ... | "
            "recoverSegment: Segment 87 replay complete | "
            "recover: Waiting on recovery data for segment 88 from "
            "mock:host=backup2 | "
            "recover: Got it: 65536 bytes | "
            "recover: Recovering with segment size 65536 | "
            "recoverSegment: recoverSegment 88, ... | "
            "recoverSegment: Segment 88 replay complete",
            TestLog::get());
    }

    void
    test_recover_failedToRecoverAll()
    {
        boost::scoped_ptr<MasterServer> master(createMasterServer());

        // tests !wasRecovered case both in-loop and end-of-loop
        ProtoBuf::Tablets tablets;
        ProtoBuf::ServerList backups; {
            ProtoBuf::ServerList_Entry& server(*backups.add_server());
            server.set_server_type(ProtoBuf::BACKUP);
            server.set_server_id(99);
            server.set_segment_id(87);
            server.set_service_locator("mock:host=backup1");
        }{
            ProtoBuf::ServerList_Entry& server(*backups.add_server());
            server.set_server_type(ProtoBuf::BACKUP);
            server.set_server_id(99);
            server.set_segment_id(88);
            server.set_service_locator("mock:host=backup1");
        }

        TestLog::Enable _(&recoverSegmentFilter);
        srand(0);
        CPPUNIT_ASSERT_THROW(
            mgr->recover(*master, 99, tablets, backups),
            SegmentRecoveryFailedException);
        string log = TestLog::get();
        CPPUNIT_ASSERT_EQUAL(
            "recover: Recovering master 99, 0 tablets, 2 hosts | "
            "recover: Waiting on recovery data for segment 87 from "
            "mock:host=backup1 | "
            "recover: getRecoveryData failed on mock:host=backup1, "
            "trying next backup; failure was: bad segment id",
            log.substr(0, log.find(" thrown at")));
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
            BackupClient::GetRecoveryData(*host, 99, 88,
                                          ProtoBuf::Tablets(), resp)();
            const SegmentEntry* entry = resp.getStart<SegmentEntry>();
            CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_SEGHEADER, entry->type);
            CPPUNIT_ASSERT_EQUAL(sizeof(SegmentHeader), entry->length);
            resp.truncateFront(sizeof(*entry));
            const SegmentHeader* header = resp.getStart<SegmentHeader>();
            CPPUNIT_ASSERT_EQUAL(99, header->logId);
            CPPUNIT_ASSERT_EQUAL(88, header->segmentId);
            CPPUNIT_ASSERT_EQUAL(segmentSize, header->segmentCapacity);
        }
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

class SegmentLocatorChooserTest : public CppUnit::TestFixture {

    CPPUNIT_TEST_SUITE(SegmentLocatorChooserTest);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_markAsDown);
    CPPUNIT_TEST_SUITE_END();

  public:

    SegmentLocatorChooserTest()
    {
    }

    void
    test_constructor()
    {
        ProtoBuf::ServerList backups; {
            ProtoBuf::ServerList_Entry& server(*backups.add_server());
            server.set_server_type(ProtoBuf::BACKUP);
            server.set_server_id(99);
            server.set_segment_id(87);
            server.set_service_locator("mock:host=backup1");
        }{
            ProtoBuf::ServerList_Entry& server(*backups.add_server());
            server.set_server_type(ProtoBuf::BACKUP);
            server.set_server_id(99);
            server.set_segment_id(88);
            server.set_service_locator("mock:host=backup2");
        }{
            ProtoBuf::ServerList_Entry& server(*backups.add_server());
            server.set_server_type(ProtoBuf::BACKUP);
            server.set_server_id(99);
            server.set_segment_id(88);
            server.set_service_locator("mock:host=backup3");
        }{
            ProtoBuf::ServerList_Entry& server(*backups.add_server());
            server.set_server_type(ProtoBuf::MASTER); // MASTER!
            server.set_server_id(99);
            server.set_segment_id(90);
            server.set_service_locator("mock:host=master1");
        }{
            ProtoBuf::ServerList_Entry& server(*backups.add_server());
            server.set_server_type(ProtoBuf::BACKUP);
            server.set_server_id(99);
            // no segment id!
            server.set_service_locator("mock:host=backup4");
        }

        SegmentLocatorChooser chooser(backups);
        CPPUNIT_ASSERT_EQUAL(3, chooser.map.size());
        MockRandom _(1);
        CPPUNIT_ASSERT_EQUAL("mock:host=backup1", chooser.get(87));

        CPPUNIT_ASSERT_EQUAL("mock:host=backup3", chooser.get(88));
        mockRandomValue = 2;
        CPPUNIT_ASSERT_EQUAL("mock:host=backup2", chooser.get(88));

        CPPUNIT_ASSERT_THROW(chooser.get(90),
                             SegmentRecoveryFailedException);

        // ensure the segmentIdList gets constructed properly
        bool contains87 = false;
        bool contains88 = false;
        uint32_t count = 0;
        foreach (uint64_t id, chooser.getSegmentIdList()) {
            switch (id) {
            case 87:
                contains87 = true;
                break;
            case 88:
                contains88 = true;
                break;
            default:
                CPPUNIT_ASSERT(false);
                break;
            }
            count++;
        }
        CPPUNIT_ASSERT_EQUAL(2, count);
        CPPUNIT_ASSERT(contains87);
        CPPUNIT_ASSERT(contains88);
    }

    void
    test_markAsDown()
    {
        ProtoBuf::ServerList backups; {
            ProtoBuf::ServerList_Entry& server(*backups.add_server());
            server.set_server_type(ProtoBuf::BACKUP);
            server.set_server_id(99);
            server.set_segment_id(87);
            server.set_service_locator("mock:host=backup1");
        }{
            ProtoBuf::ServerList_Entry& server(*backups.add_server());
            server.set_server_type(ProtoBuf::BACKUP);
            server.set_server_id(99);
            server.set_segment_id(88);
            server.set_service_locator("mock:host=backup2");
        }{
            ProtoBuf::ServerList_Entry& server(*backups.add_server());
            server.set_server_type(ProtoBuf::BACKUP);
            server.set_server_id(99);
            server.set_segment_id(88);
            server.set_service_locator("mock:host=backup3");
        }

        SegmentLocatorChooser chooser(backups);
        const string& locator = chooser.get(87);
        chooser.markAsDown(87, locator);
        CPPUNIT_ASSERT_THROW(chooser.get(87),
                             SegmentRecoveryFailedException);

        const string& locator1 = chooser.get(88);
        chooser.markAsDown(88, locator1);
        const string& locator2 = chooser.get(88);
        chooser.markAsDown(88, locator2);
        CPPUNIT_ASSERT_THROW(chooser.get(87),
                             SegmentRecoveryFailedException);
    }


  private:
    DISALLOW_COPY_AND_ASSIGN(SegmentLocatorChooserTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(SegmentLocatorChooserTest);

} // namespace RAMCloud
