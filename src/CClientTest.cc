/* Copyright (c) 2010 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.lie
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "Client.h"
#include "CClient.h"
#include "Mark.h"
#include "PerfCounterType.h"
#include "TestUtil.h"
#include "MockTransport.h"

class CClientTest : public CppUnit::TestFixture {

    CPPUNIT_TEST_SUITE(CClientTest);
    CPPUNIT_TEST(test_rc_clearPerfCounter);
    CPPUNIT_TEST(test_rc_create);
    CPPUNIT_TEST(test_rc_create_error);
    CPPUNIT_TEST(test_rc_createTable);
    CPPUNIT_TEST(test_rc_createTable_error);
    CPPUNIT_TEST(test_rc_dropTable);
    CPPUNIT_TEST(test_rc_dropTable_error);
    CPPUNIT_TEST(test_rc_getCounterValue);
    CPPUNIT_TEST(test_rc_getStatus);
    CPPUNIT_TEST(test_rc_openTable);
    CPPUNIT_TEST(test_rc_openTable_error);
    CPPUNIT_TEST(test_rc_ping);
    CPPUNIT_TEST(test_rc_ping_error);
    CPPUNIT_TEST(test_rc_read_normalCase);
    CPPUNIT_TEST(test_rc_read_bufferTooShort);
    CPPUNIT_TEST(test_rc_read_error);
    CPPUNIT_TEST(test_rc_remove);
    CPPUNIT_TEST(test_rc_remove_error);
    CPPUNIT_TEST(test_rc_selectPerfCounter);
    CPPUNIT_TEST(test_rc_write);
    CPPUNIT_TEST(test_rc_write_error);
    CPPUNIT_TEST_SUITE_END();

  public:
    RAMCloud::MockTransport* transport;
    struct rc_client* client;
    RAMCloud::RejectRules rules;
    RAMCloud::Status status;

    CClientTest(): transport(NULL), client(NULL), rules(), status() { }
    void setUp() {
        transport = new RAMCloud::MockTransport();
        struct RAMCloud::Client* foo = new RAMCloud::Client(
                new RAMCloud::Service(), transport);
        CPPUNIT_ASSERT_EQUAL(RAMCloud::STATUS_OK,
                rc_connectWithClient(foo, & client));
        bzero(&rules, sizeof(rules));
        rules.givenVersion = 0x500000009ull;
        rules.doesntExist = 1;
        rules.exists = 0;
        rules.versionLeGiven = 1;
        rules.versionNeGiven = 0;
    }

    void tearDown() {
        rc_disconnect(client);
        delete transport;
    }

    void test_rc_clearPerfCounter() {
        // First enable a counter.
        rc_selectPerfCounter(client, static_cast<RAMCloud::PerfCounterType>(5),
                static_cast<RAMCloud::Mark>(6), static_cast<RAMCloud::Mark>(7));
        transport->setInput("12345 222");
        status = rc_ping(client);

        // Now disable the counter and try again.
        rc_clearPerfCounter(client);
        transport->setInput("12345 222");
        status = rc_ping(client);
        CPPUNIT_ASSERT_EQUAL("clientSend: 7 0x5007006 | clientSend: 7 0",
                transport->outputLog);
    }

    void test_rc_create() {
        uint64_t id, version;
        transport->setInput("0 222 7 3 4 1");
        status = rc_create(client, 14, "Sample text.",
                                       10, &id, &version);
        CPPUNIT_ASSERT_EQUAL("clientSend: 11 0 14 10 Sample tex",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(0, status);
        CPPUNIT_ASSERT_EQUAL(0x300000007ull, id);
        CPPUNIT_ASSERT_EQUAL(0x100000004ull, version);
    }
    void test_rc_create_error() {
        uint64_t id, version;
        transport->setInput("4 222 7 3 4 1");
        status = rc_create(client, 14, "Sample text.",
                                       10, &id, &version);
        CPPUNIT_ASSERT_EQUAL(4, status);
    }

    void test_rc_createTable() {
        transport->setInput("0 222");
        rc_createTable(client, "table12345");
        CPPUNIT_ASSERT_EQUAL("clientSend: 8 0 11 table12345/0",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(0, status);
    }
    void test_rc_createTable_error() {
        transport->setInput("4 0");
        status = rc_createTable(client, "table12345");
        CPPUNIT_ASSERT_EQUAL(4, status);
    }

    void test_rc_dropTable() {
        transport->setInput("0 222");
        status = rc_dropTable(client, "tableToDrop");
        CPPUNIT_ASSERT_EQUAL("clientSend: 10 0 12 tableToDrop/0",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(0, status);
    }
    void test_rc_dropTable_error() {
        transport->setInput("4 0");
        status = rc_dropTable(client, "tableToDrop");
        CPPUNIT_ASSERT_EQUAL(4, status);
    }

    void test_rc_getCounterValue() {
        transport->setInput("12345 678");
        status = rc_ping(client);
        CPPUNIT_ASSERT_EQUAL(678, rc_getCounterValue(client));
    }

    void test_rc_getStatus() {
        transport->setInput("12345 678");
        status = rc_ping(client);
        CPPUNIT_ASSERT_EQUAL(12345, rc_getStatus(client));
    }

    void test_rc_openTable() {
        uint32_t id;
        transport->setInput("0 222 16");
        status = rc_openTable(client, "table6", &id);
        CPPUNIT_ASSERT_EQUAL("clientSend: 9 0 7 table6/0",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(0, status);
        CPPUNIT_ASSERT_EQUAL(16, id);
    }
    void test_rc_openTable_error() {
        uint32_t id;
        transport->setInput("4 0");
        status = rc_openTable(client, "table6", &id);
        CPPUNIT_ASSERT_EQUAL(4, status);
    }

    void test_rc_ping() {
        transport->setInput("0 222");
        status = rc_ping(client);
        CPPUNIT_ASSERT_EQUAL("clientSend: 7 0",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(0, status);
    }
    void test_rc_ping_error() {
        transport->setInput("4 0");
        status = rc_ping(client);
        CPPUNIT_ASSERT_EQUAL(4, status);
    }

    void test_rc_read_normalCase() {
        char result[] = "01234567890";
        uint64_t version;
        uint32_t actualLength = -1;

        transport->setInput("0 222 4 5 6 0 abcdef");
        status = rc_read(client, 44, 0x100000002ull, &rules,
                &version, result, 6, &actualLength);
        CPPUNIT_ASSERT_EQUAL("clientSend: 12 0 2 1 44 0 9 5 0x10001 0",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(0, status);
        CPPUNIT_ASSERT_EQUAL(0x500000004ull, version);
        CPPUNIT_ASSERT_EQUAL("abcdef67890", &result[0]);
        CPPUNIT_ASSERT_EQUAL(6, actualLength);
    }
    void test_rc_read_bufferTooShort() {
        char result[] = "01234567890";
        uint32_t actualLength = -1;

        transport->setInput("0 222 4 5 6 0 abcdef");
        status = rc_read(client, 44, 0x100000002ull, &rules,
                NULL, result, 5, &actualLength);
        CPPUNIT_ASSERT_EQUAL("clientSend: 12 0 2 1 44 0 9 5 0x10001 0",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL("abcde567890", &result[0]);
        CPPUNIT_ASSERT_EQUAL(6, actualLength);
    }
    void test_rc_read_error() {
        char result[] = "01234567890";
        uint32_t actualLength = -1;

        transport->setInput("44 222 4 5 6 0 abcdef");
        status = rc_read(client, 44, 0x100000002ull, &rules,
                NULL, result, 6, &actualLength);
        CPPUNIT_ASSERT_EQUAL("clientSend: 12 0 2 1 44 0 9 5 0x10001 0",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(44, status);
        CPPUNIT_ASSERT_EQUAL("01234567890", &result[0]);
        CPPUNIT_ASSERT_EQUAL(0, actualLength);
    }

    void test_rc_remove() {
        uint64_t version;
        transport->setInput("0 222 8 6");
        status = rc_remove(client, 44, 0x100000002ull, &rules, &version);
        CPPUNIT_ASSERT_EQUAL("clientSend: 14 0 2 1 44 0 9 5 0x10001 0",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(0, status);
        CPPUNIT_ASSERT_EQUAL(0x600000008ull, version);
    }
    void test_rc_remove_error() {
        transport->setInput("4 0");
        status = rc_remove(client, 44, 0x100000002ull, &rules, NULL);
        CPPUNIT_ASSERT_EQUAL(4, status);
    }

    void test_rc_selectPerfCounter() {
        rc_selectPerfCounter(client, static_cast<RAMCloud::PerfCounterType>(5),
                static_cast<RAMCloud::Mark>(6), static_cast<RAMCloud::Mark>(7));
        transport->setInput("12345 222");
        status = rc_ping(client);
        CPPUNIT_ASSERT_EQUAL("clientSend: 7 0x5007006",
                transport->outputLog);
    }

    void test_rc_write() {
        uint64_t version;
        transport->setInput("0 222 4 1");
        status = rc_write(client, 99, 0x500000004, "Sample text.",
                10, &rules, &version);
        CPPUNIT_ASSERT_EQUAL(
                "clientSend: 13 0 4 5 99 10 9 5 0x10001 0 Sample tex",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(0, status);
        CPPUNIT_ASSERT_EQUAL(0x100000004ull, version);
    }
    void test_rc_write_error() {
        transport->setInput("4 0");
        status = rc_write(client, 99, 0x500000004, "Sample text.",
                10, NULL, NULL);
        CPPUNIT_ASSERT_EQUAL(4, status);
    }

    DISALLOW_COPY_AND_ASSIGN(CClientTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(CClientTest);
