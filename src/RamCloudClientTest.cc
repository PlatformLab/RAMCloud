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

// The following class exists in order to expose private
// info from the RamCloudClient class.
class TClient : public RamCloudClient {
  public:
    explicit TClient(const char* serviceLocator)
        : RamCloudClient(serviceLocator) { }
    void tThrowShortResponseError(Buffer* response) {
        throwShortResponseError(response);
    }
    char* getSelectedCounter() {
        static char buffer[30];
        snprintf(buffer, sizeof(buffer), "perf: %d %d %d",
                perfCounter.counterType, perfCounter.beginMark,
                perfCounter.endMark);
        return buffer;
    }
    void setCounterValue(uint32_t value) {
        counterValue = value;
    }
};

class ClientTest : public CppUnit::TestFixture {

    CPPUNIT_TEST_SUITE(ClientTest);
    CPPUNIT_TEST(test_clearPerfCounter);
    CPPUNIT_TEST(test_create_basics);
    CPPUNIT_TEST(test_create_shortResponse);
    CPPUNIT_TEST(test_create_noVersion);
    CPPUNIT_TEST(test_create_throwException);
    CPPUNIT_TEST(test_createTable_basics);
    CPPUNIT_TEST(test_createTable_shortResponse);
    CPPUNIT_TEST(test_createTable_throwException);
    CPPUNIT_TEST(test_dropTable_basics);
    CPPUNIT_TEST(test_dropTable_shortResponse);
    CPPUNIT_TEST(test_dropTable_throwException);
    CPPUNIT_TEST(test_openTable_basics);
    CPPUNIT_TEST(test_openTable_shortResponse);
    CPPUNIT_TEST(test_openTable_throwException);
    CPPUNIT_TEST(test_ping_basics);
    CPPUNIT_TEST(test_ping_shortResponse);
    CPPUNIT_TEST(test_ping_throwException);
    CPPUNIT_TEST(test_read_basics);
    CPPUNIT_TEST(test_read_shortResponse);
    CPPUNIT_TEST(test_read_noVersionNoRejectRulesNoTruncation);
    CPPUNIT_TEST(test_read_throwException);
    CPPUNIT_TEST(test_remove_basics);
    CPPUNIT_TEST(test_remove_noVersionOrRejectRules);
    CPPUNIT_TEST(test_remove_shortResponse);
    CPPUNIT_TEST(test_remove_throwException);
    CPPUNIT_TEST(test_selectPerfCounter);
    CPPUNIT_TEST(test_write);
    CPPUNIT_TEST(test_write_noRulesOrVersion);
    CPPUNIT_TEST(test_write_shortResponse);
    CPPUNIT_TEST(test_write_throwException);
    CPPUNIT_TEST(test_throwShortResponseError);
    CPPUNIT_TEST_SUITE_END();

  public:
    MockTransport* transport;
    TClient* client;
    RejectRules rules;

    ClientTest() : transport(NULL), client(NULL), rules() { }
    void setUp() {
        transport = new MockTransport();
        transportManager.registerMock(transport);
        client = new TClient("mock:");
        client->selectPerfCounter(static_cast<PerfCounterType>(1),
                static_cast<Mark>(2), static_cast<Mark>(3));
        memset(&rules, 0, sizeof(rules));
        rules.givenVersion = 0x500000009ull;
        rules.doesntExist = 1;
        rules.exists = 0;
        rules.versionLeGiven = 1;
        rules.versionNeGiven = 0;
    }

    void tearDown() {
        delete client;
        transportManager.unregisterMock();
        delete transport;
    }

    // Tests for constructors or destructor: nothing interesting to test (yet).

    void test_clearPerfCounter() {
        client->clearPerfCounter();
        CPPUNIT_ASSERT_EQUAL("perf: 0 0 0", client->getSelectedCounter());
    }

    void test_create_basics() {
        uint64_t version;
        transport->setInput("0 222 7 3 4 1");
        uint64_t id = client->create(14, "Sample text.", 10, &version);
        CPPUNIT_ASSERT_EQUAL("clientSend: 11 0x1003002 14 10 Sample tex",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(222, client->counterValue);
        CPPUNIT_ASSERT_EQUAL(0x100000004ull, version);
        CPPUNIT_ASSERT_EQUAL(0, client->status);
        CPPUNIT_ASSERT_EQUAL(0x300000007ull, id);
    }
    void test_create_shortResponse() {
        Status status(STATUS_OK);
        transport->setInput("");
        try {
            client->create(14, "Sample text.", 10);
        } catch (ClientException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(STATUS_RESPONSE_FORMAT_ERROR, status);
    }
    void test_create_noVersion() {
        transport->setInput("0 222 7 3 4 1");
        uint64_t id = client->create(14, "Sample text.", 10);
        CPPUNIT_ASSERT_EQUAL(0x300000007ull, id);
    }
    void test_create_throwException() {
        Status status(STATUS_OK);
        transport->setInput("4 0 7 3 4 1");
        try {
            client->create(14, "Sample text.", 10);
        } catch (ClientException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(4, status);
        CPPUNIT_ASSERT_EQUAL(4, client->status);
    }

    void test_createTable_basics() {
        transport->setInput("0 222");
        client->createTable("table12345");
        CPPUNIT_ASSERT_EQUAL("clientSend: 8 0x1003002 11 table12345/0",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(222, client->counterValue);
        CPPUNIT_ASSERT_EQUAL(0, client->status);
    }
    void test_createTable_shortResponse() {
        Status status(STATUS_OK);
        transport->setInput("");
        try {
            client->createTable("table12345");
        } catch (ClientException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(STATUS_RESPONSE_FORMAT_ERROR, status);
    }
    void test_createTable_throwException() {
        Status status(STATUS_OK);
        transport->setInput("4 0");
        try {
            client->createTable("table12345");
        } catch (ClientException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(4, status);
        CPPUNIT_ASSERT_EQUAL(4, client->status);
    }

    void test_dropTable_basics() {
        transport->setInput("0 222");
        client->dropTable("tableToDrop");
        CPPUNIT_ASSERT_EQUAL("clientSend: 10 0x1003002 12 tableToDrop/0",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(222, client->counterValue);
        CPPUNIT_ASSERT_EQUAL(0, client->status);
    }
    void test_dropTable_shortResponse() {
        Status status(STATUS_OK);
        transport->setInput("");
        try {
            client->dropTable("tableToDrop");
        } catch (ClientException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(STATUS_RESPONSE_FORMAT_ERROR, status);
    }
    void test_dropTable_throwException() {
        Status status(STATUS_OK);
        transport->setInput("4 0");
        try {
            client->dropTable("tableToDrop");
        } catch (ClientException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(4, status);
        CPPUNIT_ASSERT_EQUAL(4, client->status);
    }

    void test_openTable_basics() {
        transport->setInput("0 222 16");
        uint32_t id = client->openTable("table6");
        CPPUNIT_ASSERT_EQUAL("clientSend: 9 0x1003002 7 table6/0",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(222, client->counterValue);
        CPPUNIT_ASSERT_EQUAL(0, client->status);
        CPPUNIT_ASSERT_EQUAL(16, id);
    }
    void test_openTable_shortResponse() {
        Status status(STATUS_OK);
        transport->setInput("");
        try {
            client->openTable("table6");
        } catch (ClientException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(STATUS_RESPONSE_FORMAT_ERROR, status);
    }
    void test_openTable_throwException() {
        Status status(STATUS_OK);
        transport->setInput("4 0 16");
        try {
            client->openTable("table6");
        } catch (ClientException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(4, status);
        CPPUNIT_ASSERT_EQUAL(4, client->status);
    }

    void test_ping_basics() {
        transport->setInput("0 222");
        client->ping();
        CPPUNIT_ASSERT_EQUAL(222, client->counterValue);
        CPPUNIT_ASSERT_EQUAL(0, client->status);
        CPPUNIT_ASSERT_EQUAL("clientSend: 7 0x1003002",
                transport->outputLog);
    }
    void test_ping_shortResponse() {
        Status status(STATUS_OK);
        transport->setInput("");
        try {
            client->ping();
        } catch (ClientException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(STATUS_RESPONSE_FORMAT_ERROR, status);
    }
    void test_ping_throwException() {
        Status status(STATUS_OK);
        transport->setInput("4 0");
        try {
            client->ping();
        } catch (ClientException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(4, status);
        CPPUNIT_ASSERT_EQUAL(4, client->status);
    }

    void test_read_basics() {
        Buffer result;
        uint64_t version;

        // Include extra characters in response to test proper truncation
        // of result buffer.
        transport->setInput("0 222 4 5 6 0 abcdefghijklmnop");
        client->read(44, 0x100000002ull, &result, &rules, &version);
        CPPUNIT_ASSERT_EQUAL(0, client->status);
        CPPUNIT_ASSERT_EQUAL("clientSend: 12 0x1003002 2 1 44 0 9 5 0x10001 0",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(222, client->counterValue);
        CPPUNIT_ASSERT_EQUAL(0x500000004ull, version);
        CPPUNIT_ASSERT_EQUAL("abcdef", toString(&result));
    }
    void test_read_shortResponse() {
        Buffer result;
        Status status(STATUS_OK);
        transport->setInput("");
        try {
            client->read(44, 0x100000002ull, &result);
            client->ping();
        } catch (ClientException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(STATUS_RESPONSE_FORMAT_ERROR, status);
    }
    void test_read_noVersionNoRejectRulesNoTruncation() {
        Buffer result;
        transport->setInput("0 222 4 5 8 0 abcdefg");
        client->read(44, 0x100000002ull, &result);
        CPPUNIT_ASSERT_EQUAL(0, client->status);
        CPPUNIT_ASSERT_EQUAL("clientSend: 12 0x1003002 2 1 44 0 0 0 0 0",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL("abcdefg/0", toString(&result));
    }
    void test_read_throwException() {
        Buffer result;
        uint64_t version;
        Status status(STATUS_OK);
        transport->setInput("4 222 4 5 8 0 abcdefg");
        try {
            client->read(44, 0x100000002ull, &result, NULL, &version);
            client->ping();
        } catch (ClientException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(4, status);
        CPPUNIT_ASSERT_EQUAL(4, client->status);
        CPPUNIT_ASSERT_EQUAL(0x500000004ull, version);
        CPPUNIT_ASSERT_EQUAL("abcdefg/0", toString(&result));
    }

    void test_remove_basics() {
        uint64_t version;
        transport->setInput("0 222 8 6");
        client->remove(44, 0x100000002ull, &rules, &version);
        CPPUNIT_ASSERT_EQUAL("clientSend: 14 0x1003002 2 1 44 0 9 5 0x10001 0",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(222, client->counterValue);
        CPPUNIT_ASSERT_EQUAL(0x600000008ull, version);
        CPPUNIT_ASSERT_EQUAL(0, client->status);
    }
    void test_remove_noVersionOrRejectRules() {
        transport->setInput("0 222 8 6");
        client->remove(44, 0x100000002ull);
        CPPUNIT_ASSERT_EQUAL("clientSend: 14 0x1003002 2 1 44 0 0 0 0 0",
                transport->outputLog);
    }
    void test_remove_shortResponse() {
        Status status(STATUS_OK);
        transport->setInput("");
        try {
            client->remove(44, 0x100000002ull);
        } catch (ClientException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(STATUS_RESPONSE_FORMAT_ERROR, status);
    }
    void test_remove_throwException() {
        uint64_t version;
        Status status(STATUS_OK);
        transport->setInput("4 0 8 6");
        try {
            client->remove(44, 0x100000002ull, NULL, &version);
        } catch (ClientException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(4, status);
        CPPUNIT_ASSERT_EQUAL(4, client->status);
        CPPUNIT_ASSERT_EQUAL(0x600000008ull, version);
    }

    void test_selectPerfCounter() {
        client->selectPerfCounter(static_cast<PerfCounterType>(5),
                static_cast<Mark>(6), static_cast<Mark>(7));
        CPPUNIT_ASSERT_EQUAL("perf: 5 6 7", client->getSelectedCounter());
    }

    void test_write() {
        uint64_t version;
        transport->setInput("0 222 4 1");
        client->write(99, 0x500000004, "Sample text.", 10, &rules, &version);
        CPPUNIT_ASSERT_EQUAL(
                "clientSend: 13 0x1003002 4 5 99 10 9 5 0x10001 0 Sample tex",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(222, client->counterValue);
        CPPUNIT_ASSERT_EQUAL(0x100000004ull, version);
        CPPUNIT_ASSERT_EQUAL(0, client->status);
    }
    void test_write_noRulesOrVersion() {
        transport->setInput("0 222 4 1");
        client->write(99, 0x500000004, "Sample text.", 10);
        CPPUNIT_ASSERT_EQUAL(
                "clientSend: 13 0x1003002 4 5 99 10 0 0 0 0 Sample tex",
                transport->outputLog);
    }
    void test_write_shortResponse() {
        Status status(STATUS_OK);
        transport->setInput("");
        try {
            client->write(99, 0x500000004, "Sample text.", 10);
        } catch (ClientException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(STATUS_RESPONSE_FORMAT_ERROR, status);
    }
    void test_write_throwException() {
        uint64_t version;
        Status status(STATUS_OK);
        transport->setInput("4 0 8 6");
        try {
            client->write(99, 0x500000004, "Sample text.", 10, &rules,
                    &version);
        } catch (ClientException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(4, status);
        CPPUNIT_ASSERT_EQUAL(4, client->status);
        CPPUNIT_ASSERT_EQUAL(0x600000008ull, version);
    }

    void test_throwShortResponseError() {
        Buffer b;
        Status status;

        // Response says "success".
        b.fillFromString("0 0");
        status = STATUS_OK;
        try {
            client->tThrowShortResponseError(&b);
        } catch (ClientException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(9, status);

        // Valid RpcResponseCommon with error status.
        b.fillFromString("7 0");
        status = STATUS_OK;
        try {
            client->tThrowShortResponseError(&b);
        } catch (ClientException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(7, status);

        // Response too short for RpcResponseCommon.
        b.fillFromString("8");
        status = STATUS_OK;
        try {
            client->tThrowShortResponseError(&b);
        } catch (ClientException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(9, status);
    }

    DISALLOW_COPY_AND_ASSIGN(ClientTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(ClientTest);

}  // namespace RAMCloud
