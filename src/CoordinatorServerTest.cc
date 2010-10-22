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
#include "CoordinatorServer.h"
#include "MockTransport.h"
#include "TransportManager.h"

namespace RAMCloud {

class CoordinatorServerTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(CoordinatorServerTest);
    CPPUNIT_TEST(test_enlistServer);
    CPPUNIT_TEST_SUITE_END();

    MockTransport* transport;
    CoordinatorServer* server;

    void rpc(const char* input) {
        transport->setInput(input);
        server->handleRpc<CoordinatorServer>();
    }

  public:

    CoordinatorServerTest() : transport(NULL), server(NULL) {}

    void setUp() {
        transport = new MockTransport();
        transportManager.registerMock(transport);
        server = new CoordinatorServer();
    }

    void tearDown() {
        delete server;
        transportManager.unregisterMock();
        delete transport;
    }

    void test_enlistServer() {
        server->nextServerId = 3;
        rpc("15 0 23 tcp: host=foo, port=123");
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 3 0",
                             transport->outputLog);
    }

    DISALLOW_COPY_AND_ASSIGN(CoordinatorServerTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(CoordinatorServerTest);

}  // namespace RAMCloud
