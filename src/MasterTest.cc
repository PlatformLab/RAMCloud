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

#include "TestUtil.h"
#include "BindTransport.h"
#include "Buffer.h"
#include "ClientException.h"
#include "CoordinatorClient.h"
#include "CoordinatorServer.h"
#include "MasterClient.h"
#include "MasterServer.h"
#include "TransportManager.h"

namespace RAMCloud {

class MasterTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(MasterTest);
    CPPUNIT_TEST(test_server_constructor_initializeTables);
    CPPUNIT_TEST(test_server_destructor_deleteTables);
    CPPUNIT_TEST(test_create_basics);
    CPPUNIT_TEST(test_create_badTable);
    CPPUNIT_TEST(test_ping);
    CPPUNIT_TEST(test_read_basics);
    CPPUNIT_TEST(test_read_badTable);
    CPPUNIT_TEST(test_read_noSuchObject);
    CPPUNIT_TEST(test_read_rejectRules);
    CPPUNIT_TEST(test_remove_basics);
    CPPUNIT_TEST(test_remove_badTable);
    CPPUNIT_TEST(test_remove_rejectRules);
    CPPUNIT_TEST(test_remove_objectAlreadyDeletedRejectRules);
    CPPUNIT_TEST(test_remove_objectAlreadyDeleted);
    CPPUNIT_TEST(test_write);
    CPPUNIT_TEST(test_write_rejectRules);
    CPPUNIT_TEST(test_getTable);
    CPPUNIT_TEST(test_rejectOperation);
    CPPUNIT_TEST_SUITE_END();

  public:
    ServerConfig config;
    BackupManager* backup;
    MasterServer* server;
    BindTransport* transport;
    MasterClient* client;
    CoordinatorClient* coordinator;
    CoordinatorServer* coordinatorServer;

    MasterTest()
        : config()
        , backup(NULL)
        , server(NULL)
        , transport(NULL)
        , client(NULL)
        , coordinator(NULL)
        , coordinatorServer(NULL)
    {
        config.coordinatorLocator = "mock:host=coordinator";
    }

    void setUp() {
        transport = new BindTransport();
        transportManager.registerMock(transport);
        coordinatorServer = new CoordinatorServer();
        transport->addServer(*coordinatorServer, "mock:host=coordinator");
        coordinator = new CoordinatorClient("mock:host=coordinator");
        backup = new BackupManager(0);
        server = new MasterServer(&config, backup);
        transport->addServer(*server, "mock:host=master");
        client =
            new MasterClient(transportManager.getSession("mock:host=master"));
    }

    void tearDown() {
        delete client;
        transportManager.unregisterMock();
        delete transport;
        delete server;
        delete backup;
        delete coordinator;
        delete coordinatorServer;
    }

    void test_server_constructor_initializeTables() {
        Table** tables = server->tables;
        for (int i = 0; i < MasterServer::NUM_TABLES; i++) {
            if (tables[i] != NULL)
                CPPUNIT_FAIL(format("table index %d wasn't null", i));
        }
    }

    void test_server_destructor_deleteTables() {
        MasterServer* s = new MasterServer(&config, NULL);
        Table::numDeletes = 0;
        Table** tables = s->tables;
        tables[0] = new Table();
        tables[2] = new Table();
        tables[MasterServer::NUM_TABLES-1] = new Table();
        delete s;
        CPPUNIT_ASSERT_EQUAL(3, Table::numDeletes);
    }

    void test_create_basics() {
        client->createTable("t1");

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

    // create, drop, open table going away soon so not unit tested

    void test_ping() {
        client->ping();
    }

    void test_read_basics() {
        client->createTable("t1");
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
        client->createTable("t1");
        CPPUNIT_ASSERT_THROW(client->read(0, 5, &value),
                             ObjectDoesntExistException);
    }
    void test_read_rejectRules() {
        client->createTable("t1");
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

    void test_remove_basics() {
        client->createTable("t1");
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
        client->createTable("t1");
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
        client->createTable("t1");
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
        client->createTable("t1");
        client->remove(0, 1, NULL, &version);
        CPPUNIT_ASSERT_EQUAL(VERSION_NONEXISTENT, version);
        client->create(0, "abcdef", 6);
        client->remove(0, 0);
        client->remove(0, 0, NULL, &version);
        CPPUNIT_ASSERT_EQUAL(VERSION_NONEXISTENT, version);
    }

    void test_write() {
        Buffer value;
        uint64_t version;
        client->createTable("t1");

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
        client->createTable("t1");
        RejectRules rules;
        memset(&rules, 0, sizeof(rules));
        rules.doesntExist = true;
        uint64_t version;
        CPPUNIT_ASSERT_THROW(client->write(0, 3, "item0", 5, &rules, &version),
                             ObjectDoesntExistException);
        CPPUNIT_ASSERT_EQUAL(VERSION_NONEXISTENT, version);
    }

    void test_getTable() {
        client->createTable("table1");

        // Table index out of range.
        Status status = Status(-1);
        try {
            server->getTable(1000);
        } catch (TableDoesntExistException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(1, status);

        // Table index in range, but table doesn't exist.
        status = Status(-1);
        try {
            server->getTable(6);
        } catch (TableDoesntExistException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(1, status);

        // Table exists.
        CPPUNIT_ASSERT_EQUAL("table1", server->getTable(0)->GetName());
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

}  // namespace RAMCloud
