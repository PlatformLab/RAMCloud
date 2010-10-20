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

#include "Buffer.h"
#include "Common.h"
#include "ClientException.h"
#include "Master.h"
#include "TestUtil.h"
#include "MockTransport.h"
#include "TransportManager.h"

namespace RAMCloud {

class MasterTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(MasterTest);
    CPPUNIT_TEST(test_constructor_initializeTables);
    CPPUNIT_TEST(test_destructor_deleteTables);
    CPPUNIT_TEST(test_create_basics);
    CPPUNIT_TEST(test_create_badTable);
    CPPUNIT_TEST(test_createTable_basics);
    CPPUNIT_TEST(test_createTable_badName);
    CPPUNIT_TEST(test_createTable_reuseExistingName);
    CPPUNIT_TEST(test_createTable_outOfTableSpace);
    CPPUNIT_TEST(test_dropTable_basics);
    CPPUNIT_TEST(test_dropTable_badName);
    CPPUNIT_TEST(test_dropTable_noSuchTable);
    CPPUNIT_TEST(test_openTable_basics);
    CPPUNIT_TEST(test_openTable_badName);
    CPPUNIT_TEST(test_openTable_noSuchName);
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
    CPPUNIT_TEST(test_getTable);
    CPPUNIT_TEST(test_rejectOperation);
    CPPUNIT_TEST_SUITE_END();

  public:
    MockTransport* transport;
    Master* server;
    ServerConfig config;

    MasterTest() : transport(NULL), server(NULL), config() {
        config.coordinatorLocator = "mock:";
    }

    void setUp() {
        transport = new MockTransport();
        transportManager.registerMock(transport);
        server = new Master(&config, NULL);
    }

    void tearDown() {
        delete server;
        transportManager.unregisterMock();
        delete transport;
    }

    /**
     * Convenience method for invoking an RPC with a particular
     * input message.
     *
     * \param input
     *      Textual description of the input message for the RPC,
     *      in the format expected by Buffer::fillFromString.
     */
    void rpc(const char* input) {
        transport->setInput(input);
        server->handleRpc<Master>();
    }

    void test_constructor_initializeTables() {
        Table** tables = server->tables;
        for (int i = 0; i < RC_NUM_TABLES; i++) {
            if (tables[i] != NULL) {
                char message[100];
                snprintf(message, sizeof(message),
                        "table index %d wasn't null", i);
                CPPUNIT_FAIL(message);
            }
        }
    }

    void test_destructor_deleteTables() {
        Master* s = new Master(&config, NULL);
        Table::numDeletes = 0;
        Table** tables = s->tables;
        tables[0] = new Table();
        tables[7] = new Table();
        tables[RC_NUM_TABLES-1] = new Table();
        delete s;
        CPPUNIT_ASSERT_EQUAL(3, Table::numDeletes);
    }

    void test_create_basics() {
        rpc("8 0 3 t1");                         // Create table t1.
        rpc("11 0 0 5 item0");                   // Create id 0.
        rpc("11 0 0 5 item1");                   // Create id 1.
        rpc("11 0 0 5 item2");                   // Create id 2.
        rpc("12 0 0 0 0 0 0 0 0 0");             // Read id 0.
        rpc("12 0 1 0 0 0 0 0 0 0");             // Read id 1.
        rpc("12 0 2 0 0 0 0 0 0 0");             // Read id 2.
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 0 0 0 0 1 0 | "
                "serverReply: 0 0 1 0 2 0 | serverReply: 0 0 2 0 3 0 | "
                "serverReply: 0 0 1 0 5 0 item0 | "
                "serverReply: 0 0 2 0 5 0 item1 | "
                "serverReply: 0 0 3 0 5 0 item2", transport->outputLog);
    }
    void test_create_badTable() {
        rpc("11 0 4 0");
        CPPUNIT_ASSERT_EQUAL("serverReply: 1 0 0 0 0 0", transport->outputLog);
    }

    void test_createTable_basics() {
        rpc("8 0 7 table1");                     // Create table "table1".
        rpc("9 0 7 table1");                     // Try to open table1.
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 0 0 0",
                transport->outputLog);
        transport->outputLog.clear();
        rpc("8 0 3 t2");                         // Create another table.
        rpc("9 0 3 t2");                         // Try to open it.
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 0 0 1",
                transport->outputLog);
    }
    void test_createTable_badName() {
        rpc("8 20");
        CPPUNIT_ASSERT_EQUAL("serverReply: 6 0", transport->outputLog);
    }
    void test_createTable_reuseExistingName() {
        // This test works by creating the same name twice, then
        // creating a new name, then opening the new name to make
        // sure it has id 1, not 2.
        rpc("8 0 3 t1");                         // Create t1.
        rpc("8 0 3 t1");                         // Create t1 again.
        rpc("8 0 3 t2");                         // Create t2.
        transport->outputLog.clear();
        rpc("9 0 3 t2");      // Open t2, check id.
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 1", transport->outputLog);
    }
    void test_createTable_outOfTableSpace() {
        // Fill up the table of tables.
        for (int i = 0; i < 256; ++i) {
            char message[100];
            snprintf(message, sizeof(message), "8 0 5 t%03d", i);
            rpc(message);
            CPPUNIT_ASSERT_EQUAL("serverReply: 0 0", transport->outputLog);
            transport->outputLog.clear();
        }
        rpc("8 0 4 new");
        CPPUNIT_ASSERT_EQUAL("serverReply: 5 0", transport->outputLog);
    }

    void test_dropTable_basics() {
        // Create 3 tables, then drop 2, then try to open all 3.
        rpc("8 0 3 t1");
        rpc("8 0 3 t2");
        rpc("8 0 3 t3");
        transport->outputLog.clear();
        rpc("10 0 3 t1");
        rpc("10 0 3 t3");
        rpc("9 0 3 t1");
        rpc("9 0 3 t2");
        rpc("9 0 3 t3");
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 0 0 | "
                "serverReply: 1 0 0 | serverReply: 0 0 1 | "
                "serverReply: 1 0 0",
                transport->outputLog);
    }
    void test_dropTable_badName() {
        rpc("10 0 20");
        CPPUNIT_ASSERT_EQUAL("serverReply: 6 0", transport->outputLog);
    }
    void test_dropTable_noSuchTable() {
        rpc("10 0 3 t1");
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0", transport->outputLog);
    }

    void test_openTable_basics() {
        // Create 3 tables, make sure each can be opened.
        rpc("8 0 3 t1");
        rpc("8 0 3 t2");
        rpc("8 0 3 t3");
        rpc("9 0 3 t1");
        rpc("9 0 3 t2");
        rpc("9 0 3 t3");
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 0 0 | "
                "serverReply: 0 0 | serverReply: 0 0 0 | "
                "serverReply: 0 0 1 | serverReply: 0 0 2",
                transport->outputLog);
    }
    void test_openTable_badName() {
        rpc("9 0 20");
        CPPUNIT_ASSERT_EQUAL("serverReply: 6 0 0", transport->outputLog);
    }
    void test_openTable_noSuchName() {
        rpc("8 0 3 t1");
        rpc("9 0 3 t2");
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 1 0 0",
                transport->outputLog);
    }

    void test_ping() {
        rpc("7 0");
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0", transport->outputLog);
    }

    void test_read_basics() {
        rpc("8 0 3 t1");                         // Create table t1.
        rpc("11 0 0 6 abcdef");                  // Create id 0.
        rpc("12 0 0 0 0 0 0 0 0 0");             // Read id 0.
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 0 0 0 0 1 0 | "
                "serverReply: 0 0 1 0 6 0 abcdef", transport->outputLog);
    }
    void test_read_badTable() {
        rpc("12 0 0 01001 0 0 0 0 0 0");
        CPPUNIT_ASSERT_EQUAL("serverReply: 1 0 0 0 0 0", transport->outputLog);
    }
    void test_read_noSuchObject() {
        rpc("8 0 3 t1");                         // Create table t1.
        rpc("12 0 0 5 0 0 0 0 0 0");               // Read id 5.
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 2 0 0 0 0 0",
                transport->outputLog);
    }
    void test_read_rejectRules() {
        rpc("8 0 3 t1");                         // Create table t1.
        rpc("11 0 0 6 abcdef");                  // Create id 0.
        transport->outputLog.clear();
        rpc("12 0 0 0 0 0 2 0 0x1000000 0");     // Read id 0, must match v2
        CPPUNIT_ASSERT_EQUAL("serverReply: 4 0 1 0 0 0",
                transport->outputLog);
    }

    void test_remove_basics() {
        rpc("8 0 3 t1");                         // Create table t1.
        rpc("11 0 0 5 item0");                   // Create id 0.
        rpc("12 0 0 0 0 0 0 0 0 0");             // Read id 0.
        rpc("14 0 0 0 0 0 0 0 0 0");             // Delete id 0.
        rpc("12 0 0 0 0 0 0 0 0 0");             // Read id 0 again.
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 0 0 0 0 1 0 | "
                "serverReply: 0 0 1 0 5 0 item0 | serverReply: 0 0 1 0 | "
                "serverReply: 2 0 0 0 0 0", transport->outputLog);
    }
    void test_remove_badTable() {
        rpc("14 0 7 0 0 0 0 0 0 0");             // Delete id 0.
        CPPUNIT_ASSERT_EQUAL("serverReply: 1 0 0 0", transport->outputLog);
    }
    void test_remove_rejectRules() {
        rpc("8 0 3 t1");                         // Create table t1.
        rpc("11 0 0 5 item0");                   // Create id 0.
        rpc("14 0 0 0 0 0 3 0 0x1000000 0");     // Delete id 0, reject unless
                                                 // version 3.
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 0 0 0 0 1 0 | "
                "serverReply: 4 0 1 0",
                transport->outputLog);
    }
    void test_remove_objectAlreadyDeletedRejectRules() {
        rpc("8 0 3 t1");                         // Create table t1.
        rpc("14 0 0 100 0 0 0 0 1 0");           // Delete id 100, reject if
                                                 // nonexistent.
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 2 0 0 0",
                transport->outputLog);
    }
    void test_remove_objectAlreadyDeleted() {
        rpc("8 0 3 t1");                         // Create table t1.
        rpc("14 0 100 0 0 0 0 0 0 0");             // Delete id 100.
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 0 0 0 0",
                transport->outputLog);
    }

    void test_write() {
        rpc("8 0 3 t1");                         // Create table t1.
        rpc("13 0 3 0 0 5 0 0 0 0 item0");       // Write id 3.
        rpc("12 0 3 0 0 0 0 0 0 0 0");           // Read id 3.
        rpc("13 0 3 0 0 8 0 0 0 0 item0-v2");    // Write id 3 again.
        rpc("12 0 3 0 0 0 0 0 0 0");             // Read id 3 again.
        rpc("13 0 3 0 0 8 0 0 0 0 item0-v3");    // Write id 3 again.
        rpc("12 0 3 0 0 0 0 0 0 0");             // Read id 3 again.
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 0 0 1 0 | "
                "serverReply: 0 0 1 0 5 0 item0 | serverReply: 0 0 2 0 | "
                "serverReply: 0 0 2 0 8 0 item0-v2 | serverReply: 0 0 3 0 | "
                "serverReply: 0 0 3 0 8 0 item0-v3", transport->outputLog);
    }

    void test_getTable() {
        rpc("8 0 7 table1");                     // Create table 0.

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
