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

#include <boost/scoped_ptr.hpp>
#include "TestUtil.h"
#include "BackupManager.h"
#include "BackupServer.h"
#include "BackupStorage.h"
#include "BindTransport.h"
#include "Buffer.h"
#include "ClientException.h"
#include "CoordinatorClient.h"
#include "CoordinatorServer.h"
#include "Logging.h"
#include "MasterClient.h"
#include "MasterServer.h"
#include "TransportManager.h"

namespace RAMCloud {

class MasterTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(MasterTest);
    CPPUNIT_TEST(test_create_basics);
    CPPUNIT_TEST(test_create_badTable);
    CPPUNIT_TEST(test_ping);
    CPPUNIT_TEST(test_read_basics);
    CPPUNIT_TEST(test_read_badTable);
    CPPUNIT_TEST(test_read_noSuchObject);
    CPPUNIT_TEST(test_read_rejectRules);
    CPPUNIT_TEST(test_recover_basics);
    CPPUNIT_TEST(test_recoverSegment);
    CPPUNIT_TEST(test_remove_basics);
    CPPUNIT_TEST(test_remove_badTable);
    CPPUNIT_TEST(test_remove_rejectRules);
    CPPUNIT_TEST(test_remove_objectAlreadyDeletedRejectRules);
    CPPUNIT_TEST(test_remove_objectAlreadyDeleted);
    CPPUNIT_TEST(test_setTablets);
    CPPUNIT_TEST(test_write);
    CPPUNIT_TEST(test_write_rejectRules);
    CPPUNIT_TEST(test_getTable);
    CPPUNIT_TEST(test_rejectOperation);
    CPPUNIT_TEST_SUITE_END();

  public:
    ServerConfig config;
    BackupServer::Config backupConfig;
    BackupManager* backup;
    BackupServer* backupServer;
    BackupStorage* storage;
    const uint32_t segmentFrames;
    const uint32_t segmentSize;
    MasterServer* server;
    BindTransport* transport;
    MasterClient* client;
    CoordinatorClient* coordinator;
    CoordinatorServer* coordinatorServer;

    MasterTest()
        : config()
        , backupConfig()
        , backup(NULL)
        , backupServer()
        , storage(NULL)
        , segmentFrames(2)
        , segmentSize(1 << 16)
        , server(NULL)
        , transport(NULL)
        , client(NULL)
        , coordinator(NULL)
        , coordinatorServer(NULL)
    {
        config.localLocator = "mock:host=master";
        config.coordinatorLocator = "mock:host=coordinator";
        backupConfig.coordinatorLocator = "mock:host=coordinator";
        MasterServer::sizeLogAndHashTable("64", "8", &config);
    }

    void setUp() {
        logger.setLogLevels(SILENT_LOG_LEVEL);
        transport = new BindTransport();
        transportManager.registerMock(transport);
        coordinatorServer = new CoordinatorServer();
        transport->addServer(*coordinatorServer, "mock:host=coordinator");
        coordinator = new CoordinatorClient("mock:host=coordinator");

        storage = new InMemoryStorage(segmentSize, segmentFrames);
        backupServer = new BackupServer(backupConfig, *storage);
        transport->addServer(*backupServer, "mock:host=backup1");
        coordinator->enlistServer(BACKUP, "mock:host=backup1");
        backup = new BackupManager(coordinator, 1);

        server = new MasterServer(config, coordinator, backup);
        transport->addServer(*server, "mock:host=master");
        client =
            new MasterClient(transportManager.getSession("mock:host=master"));
        ProtoBuf::Tablets_Tablet& tablet(*server->tablets.add_tablet());
        tablet.set_table_id(0);
        tablet.set_start_object_id(0);
        tablet.set_end_object_id(~0UL);
        tablet.set_user_data(reinterpret_cast<uint64_t>(new Table(0)));
    }

    void tearDown() {
        delete client;
        delete server;
        delete backup;
        delete backupServer;
        delete storage;
        delete coordinator;
        delete coordinatorServer;
        transportManager.unregisterMock();
        delete transport;
    }

    void test_create_basics() {
        uint64_t version;
        CPPUNIT_ASSERT_EQUAL(0, client->create(0, "item0", 5, &version));
        CPPUNIT_ASSERT_EQUAL(1, version);
        CPPUNIT_ASSERT_EQUAL(1, client->create(0, "item1", 5, &version));
        CPPUNIT_ASSERT_EQUAL(2, version);
        CPPUNIT_ASSERT_EQUAL(2, client->create(0, "item2", 5));

        Buffer value;
        client->read(0, 0, &value);
        CPPUNIT_ASSERT_EQUAL("item0", toString(&value));
        client->read(0, 1, &value);
        CPPUNIT_ASSERT_EQUAL("item1", toString(&value));
        client->read(0, 2, &value);
        CPPUNIT_ASSERT_EQUAL("item2", toString(&value));
    }
    void test_create_badTable() {
        CPPUNIT_ASSERT_THROW(client->create(4, "", 1),
                             TableDoesntExistException);
    }

    void test_ping() {
        client->ping();
    }

    void test_read_basics() {
        client->create(0, "abcdef", 6);

        Buffer value;
        uint64_t version;
        client->read(0, 0, &value, NULL, &version);
        CPPUNIT_ASSERT_EQUAL(1, version);
        CPPUNIT_ASSERT_EQUAL("abcdef", toString(&value));
    }
    void test_read_badTable() {
        Buffer value;
        CPPUNIT_ASSERT_THROW(client->read(4, 0, &value),
                             TableDoesntExistException);
    }
    void test_read_noSuchObject() {
        Buffer value;
        CPPUNIT_ASSERT_THROW(client->read(0, 5, &value),
                             ObjectDoesntExistException);
    }
    void test_read_rejectRules() {
        client->create(0, "abcdef", 6);

        Buffer value;
        RejectRules rules;
        memset(&rules, 0, sizeof(rules));
        rules.versionNeGiven = true;
        rules.givenVersion = 2;
        uint64_t version;
        CPPUNIT_ASSERT_THROW(client->read(0, 0, &value, &rules, &version),
                             WrongVersionException);
        CPPUNIT_ASSERT_EQUAL(1, version);
    }

    static bool
    recoverSegmentFilter(string s)
    {
        return (s == "recoverSegment" || s == "recover" ||
                s == "tabletsRecovered" || s == "setTablets");
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

    void test_recover_basics() {
        char segMem[segmentSize];
        Segment _(123, 87, segMem, segmentSize, backup);
        // TODO(stutsman) for now just ensure that the arguments make it to
        // BackupManager::recover, we'll do the full check of the
        // returns later once the recovery procedures are complete

        ProtoBuf::Tablets tablets;
        createTabletList(tablets);
        BackupClient(transportManager.getSession("mock:host=backup1")).
            startReadingData(123);

        ProtoBuf::ServerList backups; {
            ProtoBuf::ServerList_Entry& server(*backups.add_server());
            server.set_server_type(ProtoBuf::BACKUP);
            server.set_server_id(123);
            server.set_segment_id(87);
            server.set_service_locator("mock:host=backup1");
        }

        TestLog::Enable __(&recoverSegmentFilter);
        client->recover(123, tablets, backups);
        CPPUNIT_ASSERT_EQUAL(
            "recover: Starting recovery of 4 tablets on masterId 2 | "
            "recover: Recovering master 123, 4 tablets, 1 hosts | "
            "recover: Waiting on recovery data for segment 87 from "
            "mock:host=backup1 | "
            "recover: Got it: 65536 bytes | "
            "recover: Recovering with segment size 65536 | "
            "recoverSegment: recoverSegment 87, ... | "
            "recoverSegment: Segment 87 replay complete | "
            "recover: set tablet 123 0 9 to locator mock:host=master, id 2 | "
            "recover: set tablet 123 10 19 to locator mock:host=master, id 2 | "
            "recover: set tablet 123 20 29 to locator mock:host=master, id 2 | "
            "recover: set tablet 124 20 100 to locator mock:host=master, id 2 |"
            " tabletsRecovered: called with 4 tablets | "
            "setTablets: Now serving tablets: | "
            "setTablets: table:                    0, "
                        "start:                    0, "
                        "end  : 18446744073709551615 | "
            "setTablets: table:                  123, "
                        "start:                    0, "
                        "end  :                    9 | "
            "setTablets: table:                  123, "
                        "start:                   10, "
                        "end  :                   19 | "
            "setTablets: table:                  123, "
                        "start:                   20, "
                        "end  :                   29 | "
            "setTablets: table:                  124, "
                        "start:                   20, "
                        "end  :                  100",
            TestLog::get());
    }

    void
    buildRecoverySegment(char *segmentBuf, uint64_t segmentCapacity,
                         uint64_t tblId, uint64_t objId, uint64_t version,
                         string objContents)
    {
        Segment s(0, 0, segmentBuf, segmentCapacity, NULL);

        DECLARE_OBJECT(newObject, objContents.length() + 1);
        newObject->id = objId;
        newObject->table = tblId;
        newObject->version = version;
        newObject->data_len = objContents.length() + 1;
        strcpy(newObject->data, objContents.c_str()); // NOLINT fuck off

        const void *p = s.append(LOG_ENTRY_TYPE_OBJ, newObject,
                                 newObject->size());
        assert(p != NULL);
        s.close();
    }

    void
    buildRecoverySegment(char *segmentBuf, uint64_t segmentCapacity,
                         ObjectTombstone *tomb)
    {
        Segment s(0, 0, segmentBuf, segmentCapacity, NULL);
        const void *p = s.append(LOG_ENTRY_TYPE_OBJTOMB, tomb, sizeof(*tomb));
        assert(p != NULL);
        s.close();
    }

    void
    verifyRecoveryObject(uint64_t tblId, uint64_t objId, string contents)
    {
        Buffer value;
        client->read(tblId, objId, &value);
        const char *s = reinterpret_cast<const char *>(
            value.getRange(0, value.getTotalLength()));
        CPPUNIT_ASSERT(strcmp(s, contents.c_str()) == 0);
    }

    void
    test_recoverSegment()
    {
        char seg[8192];
        Buffer value;
        bool ret;
        void *p = NULL;
        const ObjectTombstone *tomb1 = NULL;
        const ObjectTombstone *tomb2 = NULL;

        ObjectTombstoneMap tombstoneMap(64 * 1024 * 1024 /
            ObjectTombstoneMap::bytesPerCacheLine());

        ////////////////////////////////////////////////////////////////////
        // For Object recovery there are 3 major cases:
        //  1) Object is in the HashTable, but no corresponding Tombstone.
        //     The recovered obj is only added if the version is newer than
        //     the existing obj.
        //
        //  2) Opposite of 1 above.
        //     The recovered obj is only added if the version is newer than
        //     the tombstone. If so, the tombstone is also discarded.
        //
        //  3) Neither an Object nor Tombstone is present.
        //     The recovered obj is always added.
        ////////////////////////////////////////////////////////////////////

        // Case 1a: Newer object already there; ignore object.
        buildRecoverySegment(seg, sizeof(seg), 0, 2000, 1, "newer guy");
        server->recoverSegment(0, seg, sizeof(seg), tombstoneMap);
        verifyRecoveryObject(0, 2000, "newer guy");
        buildRecoverySegment(seg, sizeof(seg), 0, 2000, 0, "older guy");
        server->recoverSegment(0, seg, sizeof(seg), tombstoneMap);
        verifyRecoveryObject(0, 2000, "newer guy");

        // Case 1b: Older object already there; replace object.
        buildRecoverySegment(seg, sizeof(seg), 0, 2001, 0, "older guy");
        server->recoverSegment(0, seg, sizeof(seg), tombstoneMap);
        verifyRecoveryObject(0, 2001, "older guy");
        buildRecoverySegment(seg, sizeof(seg), 0, 2001, 1, "newer guy");
        server->recoverSegment(0, seg, sizeof(seg), tombstoneMap);
        verifyRecoveryObject(0, 2001, "newer guy");

        // Case 2a: Equal/newer tombstone already there; ignore object.
        ObjectTombstone t1(0, 0, 2002, 1);
        p = xmalloc(sizeof(t1));
        memcpy(p, &t1, sizeof(t1));
        ret = tombstoneMap.replace(0, 2002,
            reinterpret_cast<const ObjectTombstone *>(p));
        CPPUNIT_ASSERT_EQUAL(false, ret);
        buildRecoverySegment(seg, sizeof(seg), 0, 2001, 1, "equal guy");
        server->recoverSegment(0, seg, sizeof(seg), tombstoneMap);
        buildRecoverySegment(seg, sizeof(seg), 0, 2001, 0, "older guy");
        server->recoverSegment(0, seg, sizeof(seg), tombstoneMap);
        CPPUNIT_ASSERT_THROW(client->read(0, 2002, &value),
                             ObjectDoesntExistException);

        // Case 2b: Lesser tombstone already there; add object, remove tomb.
        ObjectTombstone t2(0, 0, 2003, 10);
        p = xmalloc(sizeof(t2));
        memcpy(p, &t2, sizeof(t2));
        assert(p != NULL);
        ret = tombstoneMap.replace(0, 2003,
            reinterpret_cast<const ObjectTombstone *>(p));
        CPPUNIT_ASSERT_EQUAL(false, ret);
        buildRecoverySegment(seg, sizeof(seg), 0, 2003, 11, "newer guy");
        server->recoverSegment(0, seg, sizeof(seg), tombstoneMap);
        verifyRecoveryObject(0, 2003, "newer guy");
        CPPUNIT_ASSERT_EQUAL(NULL, tombstoneMap.lookup(0, 2003));

        // Case 3: No tombstone, no object. Recovered object always added.
        CPPUNIT_ASSERT_EQUAL(NULL, tombstoneMap.lookup(0, 2004));
        CPPUNIT_ASSERT_EQUAL(NULL, server->objectMap.lookup(0, 2004));
        buildRecoverySegment(seg, sizeof(seg), 0, 2004, 0, "only guy");
        server->recoverSegment(0, seg, sizeof(seg), tombstoneMap);
        verifyRecoveryObject(0, 2004, "only guy");
        CPPUNIT_ASSERT_EQUAL(NULL, tombstoneMap.lookup(0, 2004));

        ////////////////////////////////////////////////////////////////////
        // For ObjectTombstone recovery there are the same 3 major cases:
        //  1) Object is in  the HashTable, but no corresponding Tombstone.
        //     The recovered tomb is only added if the version is equal to
        //     or greater than the object. If so, the object is purged.
        //
        //  2) Opposite of 1 above.
        //     The recovered tomb is only added if the version is newer than
        //     the current tombstone. If so, the old tombstone is discarded.
        //
        //  3) Neither an Object nor Tombstone is present.
        //     The recovered tombstone is always added.
        ////////////////////////////////////////////////////////////////////

        // Case 1a: Newer object already there; ignore tombstone.
        buildRecoverySegment(seg, sizeof(seg), 0, 2005, 1, "newer guy");
        server->recoverSegment(0, seg, sizeof(seg), tombstoneMap);
        ObjectTombstone t3(0, 0, 2005, 0);
        buildRecoverySegment(seg, sizeof(seg), &t3);
        server->recoverSegment(0, seg, sizeof(seg), tombstoneMap);
        verifyRecoveryObject(0, 2005, "newer guy");

        // Case 1b: Equal/older object already there; discard and add tombstone.
        buildRecoverySegment(seg, sizeof(seg), 0, 2006, 0, "equal guy");
        server->recoverSegment(0, seg, sizeof(seg), tombstoneMap);
        verifyRecoveryObject(0, 2006, "equal guy");
        ObjectTombstone t4(0, 0, 2006, 0);
        buildRecoverySegment(seg, sizeof(seg), &t4);
        server->recoverSegment(0, seg, sizeof(seg), tombstoneMap);
        CPPUNIT_ASSERT_THROW(client->read(0, 2006, &value),
                             ObjectDoesntExistException);
        buildRecoverySegment(seg, sizeof(seg), 0, 2007, 0, "older guy");
        server->recoverSegment(0, seg, sizeof(seg), tombstoneMap);
        verifyRecoveryObject(0, 2007, "older guy");
        ObjectTombstone t5(0, 0, 2007, 1);
        buildRecoverySegment(seg, sizeof(seg), &t5);
        server->recoverSegment(0, seg, sizeof(seg), tombstoneMap);
        CPPUNIT_ASSERT_THROW(client->read(0, 2007, &value),
                             ObjectDoesntExistException);

        // Case 2a: Newer tombstone already there; ignore.
        ObjectTombstone t6(0, 0, 2008, 1);
        buildRecoverySegment(seg, sizeof(seg), &t6);
        server->recoverSegment(0, seg, sizeof(seg), tombstoneMap);
        tomb1 = tombstoneMap.lookup(0, 2008);
        CPPUNIT_ASSERT(tomb1 != NULL);
        CPPUNIT_ASSERT_EQUAL(1, tomb1->objectVersion);
        ObjectTombstone t7(0, 0, 2008, 0);
        buildRecoverySegment(seg, sizeof(seg), &t7);
        server->recoverSegment(0, seg, sizeof(seg), tombstoneMap);
        tomb2 = tombstoneMap.lookup(0, 2008);
        CPPUNIT_ASSERT_EQUAL(tomb1, tomb2);

        // Case 2b: Older tombstone already there; replace.
        ObjectTombstone t8(0, 0, 2009, 0);
        buildRecoverySegment(seg, sizeof(seg), &t8);
        server->recoverSegment(0, seg, sizeof(seg), tombstoneMap);
        tomb1 = tombstoneMap.lookup(0, 2009);
        CPPUNIT_ASSERT(tomb1 != NULL);
        CPPUNIT_ASSERT_EQUAL(0, tomb1->objectVersion);
        ObjectTombstone t9(0, 0, 2009, 1);
        buildRecoverySegment(seg, sizeof(seg), &t9);
        server->recoverSegment(0, seg, sizeof(seg), tombstoneMap);
        tomb2 = tombstoneMap.lookup(0, 2009);
        CPPUNIT_ASSERT(tomb2 != NULL);
        CPPUNIT_ASSERT_EQUAL(1, tomb2->objectVersion);

        // Case 3: No tombstone, no object. Recovered tombstone always added.
        CPPUNIT_ASSERT_EQUAL(NULL, server->objectMap.lookup(0, 2010));
        ObjectTombstone t10(0, 0, 2010, 0);
        buildRecoverySegment(seg, sizeof(seg), &t10);
        server->recoverSegment(0, seg, sizeof(seg), tombstoneMap);
        CPPUNIT_ASSERT(tombstoneMap.lookup(0, 2010) != NULL);
        CPPUNIT_ASSERT_EQUAL(NULL, server->objectMap.lookup(0, 2010));
    }

    void test_remove_basics() {
        client->create(0, "item0", 5);

        uint64_t version;
        client->remove(0, 0, NULL, &version);
        CPPUNIT_ASSERT_EQUAL(1, version);

        Buffer value;
        CPPUNIT_ASSERT_THROW(client->read(0, 0, &value),
                             ObjectDoesntExistException);
    }
    void test_remove_badTable() {
        CPPUNIT_ASSERT_THROW(client->remove(4, 0),
                             TableDoesntExistException);
    }
    void test_remove_rejectRules() {
        client->create(0, "item0", 5);

        RejectRules rules;
        memset(&rules, 0, sizeof(rules));
        rules.versionNeGiven = true;
        rules.givenVersion = 2;
        uint64_t version;
        CPPUNIT_ASSERT_THROW(client->remove(0, 0, &rules, &version),
                             WrongVersionException);
        CPPUNIT_ASSERT_EQUAL(1, version);
    }
    void test_remove_objectAlreadyDeletedRejectRules() {
        RejectRules rules;
        memset(&rules, 0, sizeof(rules));
        rules.doesntExist = true;
        uint64_t version;
        CPPUNIT_ASSERT_THROW(client->remove(0, 0, &rules, &version),
                             ObjectDoesntExistException);
        CPPUNIT_ASSERT_EQUAL(VERSION_NONEXISTENT, version);
    }
    void test_remove_objectAlreadyDeleted() {
        uint64_t version;
        client->remove(0, 1, NULL, &version);
        CPPUNIT_ASSERT_EQUAL(VERSION_NONEXISTENT, version);
        client->create(0, "abcdef", 6);
        client->remove(0, 0);
        client->remove(0, 0, NULL, &version);
        CPPUNIT_ASSERT_EQUAL(VERSION_NONEXISTENT, version);
    }

    void test_setTablets() {

        std::unique_ptr<Table> table1(new Table(1));
        uint64_t addrTable1 = reinterpret_cast<uint64_t>(table1.get());
        std::unique_ptr<Table> table2(new Table(2));
        uint64_t addrTable2 = reinterpret_cast<uint64_t>(table2.get());

        { // clear out the tablets through client
            ProtoBuf::Tablets newTablets;
            client->setTablets(newTablets);
            CPPUNIT_ASSERT_EQUAL("", server->tablets.ShortDebugString());
        }

        { // set t1 and t2 directly
            ProtoBuf::Tablets_Tablet& t1(*server->tablets.add_tablet());
            t1.set_table_id(1);
            t1.set_start_object_id(0);
            t1.set_end_object_id(1);
            t1.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
            t1.set_user_data(reinterpret_cast<uint64_t>(table1.release()));

            ProtoBuf::Tablets_Tablet& t2(*server->tablets.add_tablet());
            t2.set_table_id(2);
            t2.set_start_object_id(0);
            t2.set_end_object_id(1);
            t2.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
            t2.set_user_data(reinterpret_cast<uint64_t>(table2.release()));

            CPPUNIT_ASSERT_EQUAL(format(
                "tablet { table_id: 1 start_object_id: 0 end_object_id: 1 "
                    "state: NORMAL user_data: %lu } "
                "tablet { table_id: 2 start_object_id: 0 end_object_id: 1 "
                    "state: NORMAL user_data: %lu }",
                addrTable1, addrTable2),
                                 server->tablets.ShortDebugString());
        }

        { // set t2, t2b, and t3 through client
            ProtoBuf::Tablets newTablets;

            ProtoBuf::Tablets_Tablet& t2(*newTablets.add_tablet());
            t2.set_table_id(2);
            t2.set_start_object_id(0);
            t2.set_end_object_id(1);
            t2.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);

            ProtoBuf::Tablets_Tablet& t2b(*newTablets.add_tablet());
            t2b.set_table_id(2);
            t2b.set_start_object_id(2);
            t2b.set_end_object_id(3);
            t2b.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);

            ProtoBuf::Tablets_Tablet& t3(*newTablets.add_tablet());
            t3.set_table_id(3);
            t3.set_start_object_id(0);
            t3.set_end_object_id(1);
            t3.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);

            client->setTablets(newTablets);

            CPPUNIT_ASSERT_EQUAL(format(
                "tablet { table_id: 2 start_object_id: 0 end_object_id: 1 "
                    "state: NORMAL user_data: %lu } "
                "tablet { table_id: 2 start_object_id: 2 end_object_id: 3 "
                    "state: NORMAL user_data: %lu } "
                "tablet { table_id: 3 start_object_id: 0 end_object_id: 1 "
                    "state: NORMAL user_data: %lu }",
                addrTable2, addrTable2,
                server->tablets.tablet(2).user_data()),
                                 server->tablets.ShortDebugString());
        }
    }

    void test_write() {
        Buffer value;
        uint64_t version;
        client->write(0, 3, "item0", 5, NULL, &version);
        CPPUNIT_ASSERT_EQUAL(1, version);
        client->read(0, 3, &value, NULL, &version);
        CPPUNIT_ASSERT_EQUAL("item0", toString(&value));
        CPPUNIT_ASSERT_EQUAL(1, version);

        client->write(0, 3, "item0-v2", 8, NULL, &version);
        CPPUNIT_ASSERT_EQUAL(2, version);
        client->read(0, 3, &value);
        CPPUNIT_ASSERT_EQUAL("item0-v2", toString(&value));

        client->write(0, 3, "item0-v3", 8, NULL, &version);
        CPPUNIT_ASSERT_EQUAL(3, version);
        client->read(0, 3, &value, NULL, &version);
        CPPUNIT_ASSERT_EQUAL("item0-v3", toString(&value));
        CPPUNIT_ASSERT_EQUAL(3, version);
    }
    void test_write_rejectRules() {
        RejectRules rules;
        memset(&rules, 0, sizeof(rules));
        rules.doesntExist = true;
        uint64_t version;
        CPPUNIT_ASSERT_THROW(client->write(0, 3, "item0", 5, &rules, &version),
                             ObjectDoesntExistException);
        CPPUNIT_ASSERT_EQUAL(VERSION_NONEXISTENT, version);
    }

    void test_getTable() {
        // Table exists.
        CPPUNIT_ASSERT_NO_THROW(server->getTable(0, 0));

        // Table doesn't exist.
        Status status = Status(-1);
        try {
            server->getTable(1000, 0);
        } catch (TableDoesntExistException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(1, status);
    }

    void test_rejectOperation() {
        RejectRules empty, rules;
        memset(&empty, 0, sizeof(empty));

        // Fail: object doesn't exist.
        rules = empty;
        rules.doesntExist = 1;
        CPPUNIT_ASSERT_THROW(
                server->rejectOperation(&rules, VERSION_NONEXISTENT),
                ObjectDoesntExistException);

        // Succeed: object doesn't exist.
        rules = empty;
        rules.exists = rules.versionLeGiven = rules.versionNeGiven = 1;
        CPPUNIT_ASSERT_NO_THROW(
                server->rejectOperation(&rules, VERSION_NONEXISTENT));

        // Fail: object exists.
        rules = empty;
        rules.exists = 1;
        CPPUNIT_ASSERT_THROW(server->rejectOperation(&rules, 2),
                             ObjectExistsException);

        // versionLeGiven.
        rules = empty;
        rules.givenVersion = 0x400000001;
        rules.versionLeGiven = 1;
        CPPUNIT_ASSERT_THROW(
                server->rejectOperation(&rules, 0x400000000),
                WrongVersionException);
        CPPUNIT_ASSERT_THROW(
                server->rejectOperation(&rules, 0x400000001),
                WrongVersionException);
        CPPUNIT_ASSERT_NO_THROW(
                server->rejectOperation(&rules, 0x400000002));

        // versionNeGiven.
        rules = empty;
        rules.givenVersion = 0x400000001;
        rules.versionNeGiven = 1;
        CPPUNIT_ASSERT_THROW(
                server->rejectOperation(&rules, 0x400000000),
                WrongVersionException);
        CPPUNIT_ASSERT_NO_THROW(
                server->rejectOperation(&rules, 0x400000001));
        CPPUNIT_ASSERT_THROW(
                server->rejectOperation(&rules, 0x400000002),
                WrongVersionException);
    }

    DISALLOW_COPY_AND_ASSIGN(MasterTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(MasterTest);


/**
 * Unit tests for Master::_recover.
 */
class MasterRecoverTest : public CppUnit::TestFixture {

    CPPUNIT_TEST_SUITE(MasterRecoverTest);
    CPPUNIT_TEST(test_recover);
    CPPUNIT_TEST(test_recover_failedToRecoverAll);
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
    MasterRecoverTest()
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

        ObjectTombstoneMap tombstoneMap(64 * 1024 * 1024 /
            ObjectTombstoneMap::bytesPerCacheLine());
        MockRandom __(1); // triggers deterministic rand().
        MockTSC ___(2); // triggers deterministic selection of backup locator
        TestLog::Enable _(&recoverSegmentFilter);
        master->recover(99, tablets, backups, tombstoneMap);
        CPPUNIT_ASSERT_EQUAL(
            "recover: Recovering master 99, 4 tablets, 3 hosts | "
            "recover: Waiting on recovery data for segment 88 from "
            "mock:host=backup1 | "
            "recover: Got it: 65536 bytes | "
            "recover: Recovering with segment size 65536 | "
            "recoverSegment: recoverSegment 88, ... | "
            "recoverSegment: Segment 88 replay complete | "
            "recover: Waiting on recovery data for segment 87 from "
            "mock:host=backup1 | "
            "recover: Got it: 65536 bytes | "
            "recover: Recovering with segment size 65536 | "
            "recoverSegment: recoverSegment 87, ... | "
            "recoverSegment: Segment 87 replay complete",
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

        ObjectTombstoneMap tombstoneMap(64 * 1024 * 1024 /
            ObjectTombstoneMap::bytesPerCacheLine());
        MockRandom __(1); // triggers deterministic rand().
        TestLog::Enable _(&recoverSegmentFilter);
        CPPUNIT_ASSERT_THROW(
            master->recover(99, tablets, backups, tombstoneMap),
            SegmentRecoveryFailedException);
        string log = TestLog::get();
        CPPUNIT_ASSERT_EQUAL(
            "recover: Recovering master 99, 0 tablets, 2 hosts | "
            "recover: Waiting on recovery data for segment 88 from "
            "mock:host=backup1 | "
            "recover: getRecoveryData failed on mock:host=backup1, "
            "trying next backup; failure was: bad segment id",
            log.substr(0, log.find(" thrown at")));
    }
    DISALLOW_COPY_AND_ASSIGN(MasterRecoverTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(MasterRecoverTest);

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
        MockTSC _(1);
        CPPUNIT_ASSERT_EQUAL("mock:host=backup1", chooser.get(87));

        CPPUNIT_ASSERT_EQUAL("mock:host=backup3", chooser.get(88));
        MockTSC __(2);
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

}  // namespace RAMCloud
