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
#include "Coordinator.h"
#include "MockTransport.h"
#include "TransportManager.h"

namespace RAMCloud {

class CoordinatorTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(CoordinatorTest);
    CPPUNIT_TEST(test_enlistServer);
    CPPUNIT_TEST_SUITE_END();

    MockTransport* transport;
    Coordinator* coordinator;

  public:
    CoordinatorTest() : transport(NULL), coordinator(NULL) {}

    void setUp() {
        transport = new MockTransport();
        transportManager.registerMock(transport);
        coordinator = new Coordinator("mock:");
        TestLog::enable();
    }

    void tearDown() {
        TestLog::disable();
        delete coordinator;
        transportManager.unregisterMock();
        delete transport;
    }

    void test_enlistServer() {
        transport->setInput("0 0 2 0");
        uint64_t serverId =
            coordinator->enlistServer("tcp: host=foo, port=123");
        CPPUNIT_ASSERT_EQUAL("clientSend: 15 0 23 tcp: host=foo, port=123",
                transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(2, serverId);
    }

    DISALLOW_COPY_AND_ASSIGN(CoordinatorTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(CoordinatorTest);

}  // namespace RAMCloud
