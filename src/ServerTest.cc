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
#include "MockTransport.h"
#include "Server.h"

namespace RAMCloud {

class ServerTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(ServerTest);
    CPPUNIT_TEST(test_callHandler_normal);
    CPPUNIT_TEST(test_callHandler_tooShort);
    CPPUNIT_TEST(test_dispatch_ping);
    CPPUNIT_TEST(test_dispatch_unknown);
    CPPUNIT_TEST(test_handleRpc_messageTooShortForCommon);
    CPPUNIT_TEST(test_getString_basics);
    CPPUNIT_TEST(test_getString_lengthZero);
    CPPUNIT_TEST(test_getString_bufferTooShort);
    CPPUNIT_TEST(test_getString_stringNotTerminated);
    CPPUNIT_TEST_SUITE_END();

    MockTransport* transport;
    Server* server;

    void rpc(const char* input) {
        transport->setInput(input);
        server->handleRpc<Server>();
    }

  public:
    ServerTest() : transport(NULL), server(NULL) {}

    void setUp() {
        transport = new MockTransport();
        transportManager.registerMock(transport);
        server = new Server();
        TestLog::enable();
    }

    void tearDown() {
        TestLog::disable();
        delete server;
        transportManager.unregisterMock();
        delete transport;
    }

    void test_callHandler_normal() {
        transport->setInput("7 0 0 0");
        Transport::ServerRpc& rpc(*transport->serverRecv());
        server->callHandler<PingRpc, Server, &Server::ping>(rpc);
        assertMatchesPosixRegex("ping", TestLog::get());
    }
    void test_callHandler_tooShort() {
        transport->setInput("");
        Transport::ServerRpc& rpc(*transport->serverRecv());
        CPPUNIT_ASSERT_THROW(
            (server->callHandler<PingRpc, Server, &Server::ping>(rpc)),
            MessageTooShortError);
    }

    void test_dispatch_ping() {
        transport->setInput("7 0 0 0");
        Transport::ServerRpc& rpc(*transport->serverRecv());
        Server::Responder responder(*server, rpc);
        server->dispatch(PingRpc::type, rpc, responder);
        assertMatchesPosixRegex("ping", TestLog::get());
    }
    void test_dispatch_unknown() {
        transport->setInput("0 0");
        Transport::ServerRpc& rpc(*transport->serverRecv());
        Server::Responder responder(*server, rpc);
        union {
            RpcType x;
            int y;
        } t;
        t.y = 12345;
        CPPUNIT_ASSERT_THROW(
            server->dispatch(t.x, rpc, responder),
            UnimplementedRequestError);
    }

    void test_handleRpc_messageTooShortForCommon() {
        rpc("a");
        CPPUNIT_ASSERT_EQUAL("serverReply: 6", transport->outputLog);
    }
    void test_getString_basics() {
        Buffer buffer;
        buffer.fillFromString("abcdefg");
        const char* result = server->getString(buffer, 3, 5);
        CPPUNIT_ASSERT_EQUAL("defg", result);
    }
    void test_getString_lengthZero() {
        Buffer buffer;
        Status status = Status(0);
        try {
            server->getString(buffer, 0, 0);
        } catch (RequestFormatError& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(8, status);
    }
    void test_getString_bufferTooShort() {
        Buffer buffer;
        buffer.fillFromString("abcde");
        Status status = Status(0);
        try {
            server->getString(buffer, 2, 5);
        } catch (MessageTooShortError& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(6, status);
    }
    void test_getString_stringNotTerminated() {
        Buffer buffer;
        buffer.fillFromString("abcde");
        Status status = Status(0);
        try {
            server->getString(buffer, 1, 3);
        } catch (RequestFormatError& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(8, status);
    }


    DISALLOW_COPY_AND_ASSIGN(ServerTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(ServerTest);

}  // namespace RAMCloud
