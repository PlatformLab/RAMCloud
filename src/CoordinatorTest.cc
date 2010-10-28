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
#include "CoordinatorClient.h"
#include "CoordinatorServer.h"
#include "MockTransport.h"
#include "TransportManager.h"
#include "BindTransport.h"

namespace RAMCloud {

class CoordinatorTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(CoordinatorTest);
    CPPUNIT_TEST(test_enlistServer);
    CPPUNIT_TEST(test_getServerList);
    CPPUNIT_TEST_SUITE_END();

    BindTransport* transport;
    CoordinatorClient* client;
    CoordinatorServer* server;

  public:
    CoordinatorTest() : transport(NULL), client(NULL), server(NULL) {}

    void setUp() {
        transport = new BindTransport();
        transportManager.registerMock(transport);
        server = new CoordinatorServer();
        transport->addServer(*server, "mock:");
        client = new CoordinatorClient("mock:");
        TestLog::enable();
    }

    void tearDown() {
        TestLog::disable();
        delete client;
        delete server;
        transportManager.unregisterMock();
        delete transport;
    }

    void test_enlistServer() {
        server->nextServerId = 2;
        uint64_t serverId =
            client->enlistServer(MASTER, "tcp:host=foo,port=123");
        CPPUNIT_ASSERT_EQUAL(2, serverId);
        CPPUNIT_ASSERT_EQUAL("server { server_type: MASTER server_id: 2 "
                             "service_locator: \"tcp:host=foo,port=123\" }",
                             server->serverList.ShortDebugString());
    }

    void test_getServerList() {
        server->nextServerId = 2;
        client->enlistServer(MASTER, "tcp:host=foo,port=123");
        client->enlistServer(BACKUP, "tcp:host=bar,port=123");
        ProtoBuf::ServerList serverList;
        client->getServerList(serverList);
        CPPUNIT_ASSERT_EQUAL("server { server_type: MASTER server_id: 2 "
                             "service_locator: \"tcp:host=foo,port=123\" } "
                             "server { server_type: BACKUP server_id: 3 "
                             "service_locator: \"tcp:host=bar,port=123\" }",
                             serverList.ShortDebugString());
    }

    DISALLOW_COPY_AND_ASSIGN(CoordinatorTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(CoordinatorTest);

}  // namespace RAMCloud
