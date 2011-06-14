/* Copyright (c) 2010 Stanford University
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

#include <cstring>

#include "TestUtil.h"
#include "BindTransport.h"
#include "BackupManager.h"
#include "BackupServer.h"
#include "BackupStorage.h"
#include "CoordinatorClient.h"
#include "CoordinatorServer.h"
#include "Logging.h"
#include "MasterServer.h"
#include "Tablets.pb.h"
#include "TransportManager.h"
#include "Recovery.h"

namespace RAMCloud {

/**
 * Unit tests for Recovery.
 */
class RecoveryTest : public CppUnit::TestFixture {

    CPPUNIT_TEST_SUITE(RecoveryTest);
    CPPUNIT_TEST(test_buildSegmentIdToBackups);
    CPPUNIT_TEST(test_buildSegmentIdToBackups_secondariesEarlyInSomeList);
    CPPUNIT_TEST(test_verifyCompleteLog);
    CPPUNIT_TEST(test_start);
    CPPUNIT_TEST(test_start_notEnoughMasters);
    CPPUNIT_TEST_SUITE_END();

    /**
     * Used to control precise timing of destruction of the Segment object
     * which implicitly calls freeSegment.
     */
    struct WriteValidSegment {
        ProtoBuf::ServerList backupList;
        Tub<uint64_t> masterIdTub;
        BackupManager* mgr;
        char *segMem;
        Segment* seg;

        WriteValidSegment(uint64_t masterId,
                          uint64_t segmentId,
                          vector<uint64_t> digestIds,
                          const uint32_t segmentSize,
                          const vector<const char*> locators,
                          bool close)
            : backupList()
            , masterIdTub(masterId)
            , mgr()
            , segMem()
            , seg()
        {
            mgr = new BackupManager(NULL, masterIdTub,
                                    downCast<uint32_t>(locators.size()));
            foreach (const auto& locator, locators) {
                ProtoBuf::ServerList::Entry& e(*backupList.add_server());
                e.set_service_locator(locator);
                e.set_server_type(ProtoBuf::BACKUP);
            }
            mgr->hosts = backupList;

            segMem = new char[segmentSize];
            seg = new Segment(masterId, segmentId, segMem, segmentSize, mgr);

            char temp[LogDigest::getBytesFromCount(
                                        downCast<uint32_t>(digestIds.size()))];
            LogDigest ld(downCast<uint32_t>(digestIds.size()),
                         temp,
                         downCast<uint32_t>(sizeof(temp)));
            for (unsigned int i = 0; i < digestIds.size(); i++)
                ld.addSegment(digestIds[i]);
            seg->append(LOG_ENTRY_TYPE_LOGDIGEST, temp,
                        downCast<uint32_t>(sizeof(temp)));

            if (close)
                seg->close();
        }

        ~WriteValidSegment()
        {
            delete seg;
            delete[] segMem;
            delete mgr;
        }

        DISALLOW_COPY_AND_ASSIGN(WriteValidSegment);
    };

    BackupClient* backup1;
    BackupClient* backup2;
    BackupClient* backup3;
    BackupServer* backupServer1;
    BackupServer* backupServer2;
    BackupServer* backupServer3;
    CoordinatorClient* coordinator;
    CoordinatorServer* coordinatorServer;
    BackupServer::Config* config;
    ProtoBuf::ServerList* masterHosts;
    ProtoBuf::ServerList* backupHosts;
    const uint32_t segmentFrames;
    const uint32_t segmentSize;
    vector<WriteValidSegment*> segmentsToFree;
    BackupStorage* storage1;
    BackupStorage* storage2;
    BackupStorage* storage3;
    BindTransport* transport;

  public:
    RecoveryTest()
        : backup1()
        , backup2()
        , backup3()
        , backupServer1()
        , backupServer2()
        , backupServer3()
        , coordinator()
        , coordinatorServer()
        , config()
        , masterHosts()
        , backupHosts()
        , segmentFrames(3)
        , segmentSize(1 << 16)
        , segmentsToFree()
        , storage1()
        , storage2()
        , storage3()
        , transport()
    {
    }

    void
    setUp(bool enlist)
    {
        logger.setLogLevels(SILENT_LOG_LEVEL);
        if (!enlist)
            tearDown();

        transport = new BindTransport;
        transportManager.registerMock(transport);

        config = new BackupServer::Config;
        config->coordinatorLocator = "mock:host=coordinator";

        coordinatorServer = new CoordinatorServer;
        transport->addService(*coordinatorServer, config->coordinatorLocator);

        coordinator = new CoordinatorClient(config->coordinatorLocator.c_str());

        storage1 = new InMemoryStorage(segmentSize, segmentFrames);
        storage2 = new InMemoryStorage(segmentSize, segmentFrames);
        storage3 = new InMemoryStorage(segmentSize, segmentFrames);

        backupServer1 = new BackupServer(*config, *storage1);
        backupServer2 = new BackupServer(*config, *storage2);
        backupServer3 = new BackupServer(*config, *storage3);

        transport->addService(*backupServer1, "mock:host=backup1");
        transport->addService(*backupServer2, "mock:host=backup2");
        transport->addService(*backupServer3, "mock:host=backup3");

        if (enlist) {
            coordinator->enlistServer(BACKUP, "mock:host=backup1");
            coordinator->enlistServer(BACKUP, "mock:host=backup2");
            coordinator->enlistServer(BACKUP, "mock:host=backup3");
        }

        backup1 =
            new BackupClient(transportManager.getSession("mock:host=backup1"));
        backup2 =
            new BackupClient(transportManager.getSession("mock:host=backup2"));
        backup3 =
            new BackupClient(transportManager.getSession("mock:host=backup3"));

        masterHosts = new ProtoBuf::ServerList();
        {
            ProtoBuf::ServerList::Entry& host(*masterHosts->add_server());
            host.set_server_type(ProtoBuf::MASTER);
            host.set_server_id(9999998);
            host.set_service_locator("mock:host=master1");
        }{
            ProtoBuf::ServerList::Entry& host(*masterHosts->add_server());
            host.set_server_type(ProtoBuf::MASTER);
            host.set_server_id(9999999);
            host.set_service_locator("mock:host=master2");
        }

        backupHosts = new ProtoBuf::ServerList();
        {
            ProtoBuf::ServerList::Entry& host(*backupHosts->add_server());
            host.set_server_type(ProtoBuf::BACKUP);
            host.set_server_id(backupServer1->getServerId());
            host.set_service_locator("mock:host=backup1");
        }{
            ProtoBuf::ServerList::Entry& host(*backupHosts->add_server());
            host.set_server_type(ProtoBuf::BACKUP);
            host.set_server_id(backupServer2->getServerId());
            host.set_service_locator("mock:host=backup2");
        }{
            ProtoBuf::ServerList::Entry& host(*backupHosts->add_server());
            host.set_server_type(ProtoBuf::BACKUP);
            host.set_server_id(backupServer3->getServerId());
            host.set_service_locator("mock:host=backup3");
        }
    }

    void
    setUp()
    {
        setUp(true);
    }

    void
    tearDown()
    {
        delete backupHosts;
        delete masterHosts;
        foreach (WriteValidSegment* s, segmentsToFree)
            delete s;
        delete backup3;
        delete backup2;
        delete backup1;
        delete backupServer3;
        delete backupServer2;
        delete backupServer1;
        delete storage3;
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
    test_buildSegmentIdToBackups()
    {
        MockRandom _(1);
        // Two segs on backup1, one that overlaps with backup2
        segmentsToFree.push_back(
            new WriteValidSegment(99, 88, { 88 }, segmentSize,
                {"mock:host=backup1"}, true));
        segmentsToFree.push_back(
            new WriteValidSegment(99, 89, { 88, 89 }, segmentSize,
                {"mock:host=backup1"}, false));
        // One seg on backup2
        segmentsToFree.push_back(
            new WriteValidSegment(99, 88, { 88 }, segmentSize,
                {"mock:host=backup2"}, true));
        // Zero segs on backup3

        ProtoBuf::Tablets tablets;
        Recovery recovery(99, tablets, *masterHosts, *backupHosts);

        CPPUNIT_ASSERT_EQUAL(3, recovery.backups.server_size());
        {
            const ProtoBuf::ServerList::Entry&
                backup(recovery.backups.server(0));
            CPPUNIT_ASSERT_EQUAL(89, backup.segment_id());
            CPPUNIT_ASSERT_EQUAL("mock:host=backup1", backup.service_locator());
            CPPUNIT_ASSERT_EQUAL(ProtoBuf::BACKUP, backup.server_type());
        }{
            const ProtoBuf::ServerList::Entry&
                backup(recovery.backups.server(1));
            CPPUNIT_ASSERT_EQUAL(88, backup.segment_id());
            CPPUNIT_ASSERT_EQUAL("mock:host=backup2", backup.service_locator());
            CPPUNIT_ASSERT_EQUAL(ProtoBuf::BACKUP, backup.server_type());
        }{
            const ProtoBuf::ServerList::Entry&
                backup(recovery.backups.server(2));
            CPPUNIT_ASSERT_EQUAL(88, backup.segment_id());
            CPPUNIT_ASSERT_EQUAL("mock:host=backup1", backup.service_locator());
            CPPUNIT_ASSERT_EQUAL(ProtoBuf::BACKUP, backup.server_type());
        }
    }

    void
    test_buildSegmentIdToBackups_secondariesEarlyInSomeList()
    {
        // Two segs on backup1, one that overlaps with backup2
        segmentsToFree.push_back(
            new WriteValidSegment(99, 88, { 88 }, segmentSize,
                {"mock:host=backup1"}, true));
        segmentsToFree.push_back(
            new WriteValidSegment(99, 89, { 88, 89 }, segmentSize,
                {"mock:host=backup1"}, true));
        // One seg on backup2
        segmentsToFree.push_back(
            new WriteValidSegment(99, 88, { 88 }, segmentSize,
                {"mock:host=backup2"}, true));
        // Zero segs on backup3
        // Add one more primary to backup1
        // Add a primary/secondary segment pair to backup2 and backup3
        // No matter which host its placed on it appears earlier in the
        // segment list of 2 or 3 than the latest primary on 1 (which is
        // in slot 3).  Check to make sure the code prevents this secondary
        // from showing up before any primary in the list.
        segmentsToFree.push_back(
            new WriteValidSegment(99, 90, { 88, 89, 90 }, segmentSize,
                {"mock:host=backup1"}, false));
        segmentsToFree.push_back(
            new WriteValidSegment(99, 91, { 88, 89, 90, 91 }, segmentSize,
                {"mock:host=backup2", "mock:host=backup3"}, true));

        ProtoBuf::Tablets tablets;
        Recovery recovery(99, tablets, *masterHosts, *backupHosts);

        CPPUNIT_ASSERT_EQUAL(4, recovery.backups.server_size());
        bool sawSecondary = false;
        foreach (const auto& backup, recovery.backups.server()) {
            if (!backup.user_data())
                sawSecondary = true;
            else
                CPPUNIT_ASSERT(!sawSecondary);
        }
    }

    static bool
    verifyCompleteLogFilter(string s)
    {
        return s == "verifyCompleteLog";
    }

    void
    test_verifyCompleteLog()
    {
        // TODO(ongaro): The buildSegmentIdToBackups method needs to be
        // refactored before it can be reasonably tested (see RAM-243).
        // Sorry. Kick me off the project.
#if 0
        ProtoBuf::Tablets tablets;
        Recovery recovery(99, tablets, *masterHosts, *backupHosts);

        vector<Recovery::SegmentAndDigestTuple> oldDigestList =
            recovery.digestList;
        CPPUNIT_ASSERT_EQUAL(1, oldDigestList.size());

        // no head is very bad news.
        recovery.digestList.clear();
        CPPUNIT_ASSERT_THROW(recovery.verifyCompleteLog(), Exception);

        // ensure the newest head is chosen
        recovery.digestList = oldDigestList;
        recovery.digestList.push_back({ oldDigestList[0].segmentId + 1,
            oldDigestList[0].segmentLength,
            oldDigestList[0].logDigest.getRawPointer(),
            oldDigestList[0].logDigest.getBytes() });
        TestLog::Enable _(&verifyCompleteLogFilter);
        recovery.verifyCompleteLog();
        CPPUNIT_ASSERT_EQUAL("verifyCompleteLog: Segment 90 of length "
            "64 bytes is the head of the log", TestLog::get());

        // ensure the longest newest head is chosen
        TestLog::reset();
        recovery.digestList.push_back({ oldDigestList[0].segmentId + 1,
            oldDigestList[0].segmentLength + 1,
            oldDigestList[0].logDigest.getRawPointer(),
            oldDigestList[0].logDigest.getBytes() });
        recovery.verifyCompleteLog();
        CPPUNIT_ASSERT_EQUAL("verifyCompleteLog: Segment 90 of length "
            "65 bytes is the head of the log", TestLog::get());

        // ensure we log missing segments
        TestLog::reset();
        recovery.segmentMap.erase(88);
        recovery.verifyCompleteLog();
        CPPUNIT_ASSERT_EQUAL("verifyCompleteLog: Segment 90 of length 65 bytes "
            "is the head of the log | verifyCompleteLog: Segment 88 is missing!"
            " | verifyCompleteLog: 1 segments in the digest, but not obtained "
            "from backups!", TestLog::get());
#endif
    }

    /// Create a master along with its config and clean them up on destruction.
    struct AutoMaster {
        AutoMaster(BindTransport& transport,
                   CoordinatorClient &coordinator,
                   const string& locator)
            : config()
            , master()
        {
            config.coordinatorLocator = "mock:host=coordinator";
            config.localLocator = locator;
            MasterServer::sizeLogAndHashTable("64", "8", &config);
            master = new MasterServer(config, &coordinator, 0);
            transport.addService(*master, locator);
            master->serverId.construct(
                coordinator.enlistServer(MASTER, locator));
        }

        ~AutoMaster()
        {
            delete master;
        }

        ServerConfig config;
        MasterServer* master;

        DISALLOW_COPY_AND_ASSIGN(AutoMaster);
    };

    static bool
    getRecoveryDataFilter(string s)
    {
        return s == "getRecoveryData" ||
               s == "start";
    }

    void
    test_start()
    {
        MockRandom __(1);

        // Two segs on backup1, one that overlaps with backup2
        segmentsToFree.push_back(
            new WriteValidSegment(99, 88, { 88 }, segmentSize,
                {"mock:host=backup1"}, true));
        segmentsToFree.push_back(
            new WriteValidSegment(99, 89, { 88, 89 }, segmentSize,
                {"mock:host=backup1"}, false));
        // One seg on backup2
        segmentsToFree.push_back(
            new WriteValidSegment(99, 88, { 88 }, segmentSize,
                {"mock:host=backup2"}, true));
        // Zero segs on backup3


        AutoMaster am1(*transport, *coordinator, "mock:host=master1");
        AutoMaster am2(*transport, *coordinator, "mock:host=master2");

        ProtoBuf::Tablets tablets; {
            ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
            tablet.set_table_id(123);
            tablet.set_start_object_id(0);
            tablet.set_end_object_id(9);
            tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
            tablet.set_user_data(0); // partition 0
        }{
            ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
            tablet.set_table_id(123);
            tablet.set_start_object_id(20);
            tablet.set_end_object_id(29);
            tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
            tablet.set_user_data(0); // partition 0
        }{
            ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
            tablet.set_table_id(123);
            tablet.set_start_object_id(10);
            tablet.set_end_object_id(19);
            tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
            tablet.set_user_data(1); // partition 1
        }

        Recovery recovery(99, tablets, *masterHosts, *backupHosts);
        TestLog::Enable _(&getRecoveryDataFilter);
        recovery.start();
        CPPUNIT_ASSERT_EQUAL(3, recovery.tabletsUnderRecovery);
        CPPUNIT_ASSERT_EQUAL(
            "start: Starting recovery for 2 partitions | "
            "getRecoveryData: getRecoveryData masterId 99, segmentId 89, "
            "partitionId 0 | "
            "getRecoveryData: getRecoveryData complete | "
            "getRecoveryData: getRecoveryData masterId 99, segmentId 88, "
            "partitionId 0 | "
            "getRecoveryData: getRecoveryData complete | "
            "getRecoveryData: getRecoveryData masterId 99, segmentId 89, "
            "partitionId 1 | "
            "getRecoveryData: getRecoveryData complete | "
            "getRecoveryData: getRecoveryData masterId 99, segmentId 88, "
            "partitionId 1 | "
            "getRecoveryData: getRecoveryData complete",
            TestLog::get());
    }

    void
    test_start_notEnoughMasters()
    {
        // Two segs on backup1, one that overlaps with backup2
        segmentsToFree.push_back(
            new WriteValidSegment(99, 88, { 88 }, segmentSize,
                {"mock:host=backup1"}, true));
        segmentsToFree.push_back(
            new WriteValidSegment(99, 89, { 88, 89 }, segmentSize,
                {"mock:host=backup1"}, false));
        // One seg on backup2
        segmentsToFree.push_back(
            new WriteValidSegment(99, 88, { 88 }, segmentSize,
                {"mock:host=backup2"}, true));
        // Zero segs on backup3

        AutoMaster am1(*transport, *coordinator, "mock:host=master1");
        AutoMaster am2(*transport, *coordinator, "mock:host=master2");

        ProtoBuf::Tablets tablets; {
            ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
            tablet.set_table_id(123);
            tablet.set_start_object_id(0);
            tablet.set_end_object_id(9);
            tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
            tablet.set_user_data(0); // partition 0
        }{
            ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
            tablet.set_table_id(123);
            tablet.set_start_object_id(10);
            tablet.set_end_object_id(19);
            tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
            tablet.set_user_data(1); // partition 1
        }{
            ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
            tablet.set_table_id(123);
            tablet.set_start_object_id(20);
            tablet.set_end_object_id(29);
            tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
            tablet.set_user_data(2); // partition 2
        }

        Recovery recovery(99, tablets, *masterHosts, *backupHosts);
        MockRandom __(1); // triggers deterministic rand().
        TestLog::Enable _(&getRecoveryDataFilter);
        CPPUNIT_ASSERT_THROW(recovery.start(), FatalError);
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(RecoveryTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(RecoveryTest);


} // namespace RAMCloud
