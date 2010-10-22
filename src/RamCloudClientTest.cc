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

#include "Buffer.h"
#include "Common.h"
#include "RamCloudClient.h"
#include "TestUtil.h"
#include "MockTransport.h"
#include "TransportManager.h"

namespace RAMCloud {

class RamCloudClientTest : public CppUnit::TestFixture {

    CPPUNIT_TEST_SUITE(RamCloudClientTest);
    CPPUNIT_TEST(test_create_basics);
    CPPUNIT_TEST(test_create_noVersion);
    CPPUNIT_TEST(test_createTable_basics);
    CPPUNIT_TEST(test_dropTable_basics);
    CPPUNIT_TEST(test_openTable_basics);
    CPPUNIT_TEST(test_ping_basics);
    CPPUNIT_TEST(test_read_basics);
    CPPUNIT_TEST(test_read_noVersionNoRejectRulesNoTruncation);
    CPPUNIT_TEST(test_remove_basics);
    CPPUNIT_TEST(test_remove_noVersionOrRejectRules);
    CPPUNIT_TEST(test_write);
    CPPUNIT_TEST(test_write_noRulesOrVersion);
    CPPUNIT_TEST_SUITE_END();

  public:
    MockTransport* transport;
    RamCloudClient* client;
    RejectRules rules;

    RamCloudClientTest() : transport(NULL), client(NULL), rules() { }
    void setUp() {
        transport = new MockTransport();
        transportManager.registerMock(transport);
        client = new RamCloudClient("mock:");
        client->selectPerfCounter(static_cast<PerfCounterType>(1),
                static_cast<Mark>(2), static_cast<Mark>(3));
        memset(&rules, 0, sizeof(rules));
        rules.givenVersion = 0x500000009ull;
        rules.doesntExist = 1;
        rules.exists = 0;
        rules.versionLeGiven = 1;
        rules.versionNeGiven = 0;
        TestLog::enable();
    }

    void tearDown() {
        TestLog::disable();
        delete client;
        transportManager.unregisterMock();
        delete transport;
    }

    // Tests for constructors or destructor: nothing interesting to test (yet).

    void test_create_basics() {
        uint64_t version;
        transport->setInput("0 222 7 3 4 1");
        uint64_t id = client->create(14, "Sample text.", 10, &version);
        CPPUNIT_ASSERT_EQUAL("clientSend: 11 0x1003002 14 10 Sample tex",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(0x100000004ull, version);
        CPPUNIT_ASSERT_EQUAL(0x300000007ull, id);
        assertMatchesPosixRegex("checkStatus", TestLog::get());
    }
    void test_create_noVersion() {
        transport->setInput("0 222 7 3 4 1");
        uint64_t id = client->create(14, "Sample text.", 10);
        CPPUNIT_ASSERT_EQUAL(0x300000007ull, id);
    }

    void test_createTable_basics() {
        transport->setInput("0 222");
        client->createTable("table12345");
        CPPUNIT_ASSERT_EQUAL("clientSend: 8 0x1003002 11 table12345/0",
                transport->outputLog);
        assertMatchesPosixRegex("checkStatus", TestLog::get());
    }

    void test_dropTable_basics() {
        transport->setInput("0 222");
        client->dropTable("tableToDrop");
        CPPUNIT_ASSERT_EQUAL("clientSend: 10 0x1003002 12 tableToDrop/0",
                transport->outputLog);
        assertMatchesPosixRegex("checkStatus", TestLog::get());
    }

    void test_openTable_basics() {
        transport->setInput("0 222 16");
        uint32_t id = client->openTable("table6");
        CPPUNIT_ASSERT_EQUAL("clientSend: 9 0x1003002 7 table6/0",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(16, id);
        assertMatchesPosixRegex("checkStatus", TestLog::get());
    }

    void test_ping_basics() {
        transport->setInput("0 222");
        client->ping();
        CPPUNIT_ASSERT_EQUAL("clientSend: 7 0x1003002",
                transport->outputLog);
        assertMatchesPosixRegex("checkStatus", TestLog::get());
    }

    void test_read_basics() {
        Buffer result;
        uint64_t version;

        // Include extra characters in response to test proper truncation
        // of result buffer.
        transport->setInput("0 222 4 5 6 0 abcdefghijklmnop");
        client->read(44, 0x100000002ull, &result, &rules, &version);
        CPPUNIT_ASSERT_EQUAL("clientSend: 12 0x1003002 2 1 44 0 9 5 0x10001 0",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(0x500000004ull, version);
        CPPUNIT_ASSERT_EQUAL("abcdef", toString(&result));
        assertMatchesPosixRegex("checkStatus", TestLog::get());
    }
    void test_read_noVersionNoRejectRulesNoTruncation() {
        Buffer result;
        transport->setInput("0 222 4 5 8 0 abcdefg");
        client->read(44, 0x100000002ull, &result);
        CPPUNIT_ASSERT_EQUAL("clientSend: 12 0x1003002 2 1 44 0 0 0 0 0",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL("abcdefg/0", toString(&result));
    }

    void test_remove_basics() {
        uint64_t version;
        transport->setInput("0 222 8 6");
        client->remove(44, 0x100000002ull, &rules, &version);
        CPPUNIT_ASSERT_EQUAL("clientSend: 14 0x1003002 2 1 44 0 9 5 0x10001 0",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(0x600000008ull, version);
        assertMatchesPosixRegex("checkStatus", TestLog::get());
    }
    void test_remove_noVersionOrRejectRules() {
        transport->setInput("0 222 8 6");
        client->remove(44, 0x100000002ull);
        CPPUNIT_ASSERT_EQUAL("clientSend: 14 0x1003002 2 1 44 0 0 0 0 0",
                transport->outputLog);
    }

    void test_write() {
        uint64_t version;
        transport->setInput("0 222 4 1");
        client->write(99, 0x500000004, "Sample text.", 10, &rules, &version);
        CPPUNIT_ASSERT_EQUAL(
                "clientSend: 13 0x1003002 4 5 99 10 9 5 0x10001 0 Sample tex",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(0x100000004ull, version);
        assertMatchesPosixRegex("checkStatus", TestLog::get());
    }
    void test_write_noRulesOrVersion() {
        transport->setInput("0 222 4 1");
        client->write(99, 0x500000004, "Sample text.", 10);
        CPPUNIT_ASSERT_EQUAL(
                "clientSend: 13 0x1003002 4 5 99 10 0 0 0 0 Sample tex",
                transport->outputLog);
    }

    DISALLOW_COPY_AND_ASSIGN(RamCloudClientTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(RamCloudClientTest);

}  // namespace RAMCloud
