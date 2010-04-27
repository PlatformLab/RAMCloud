/* Copyright (c) 2010 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/**
 * \file RPCTest.cc Unit tests for ClientRPC and ServerRPC.
 */

#include <MockTransport.h>
#include <RPC.h>

#include <cppunit/extensions/HelperMacros.h>

namespace RAMCloud {

class ClientRPCTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(ClientRPCTest);
    CPPUNIT_TEST(test_startRPC_badInputValues);
    CPPUNIT_TEST(test_startRPC_badTrans);
    CPPUNIT_TEST(test_startRPC_normal);
    CPPUNIT_TEST(test_getReply_badChecks);
    CPPUNIT_TEST(test_getReply_normal);
    CPPUNIT_TEST(test_getReply_normalNonBlocking);
    CPPUNIT_TEST_SUITE_END();

    MockTransport* trans;
    Buffer* payload;
    Service* dest;

  public:
    ClientRPCTest()
            : trans(NULL), payload(NULL), dest(NULL) { }

    void setUp() {
        trans = new MockTransport();
        payload = new Buffer();
        dest = new Service();
    }

    void tearDown() {
        delete trans;
        delete payload;
        delete dest;
    }

    void test_startRPC_badInputValues() {
        ClientRPC rpc(trans);
        rpc.startRPC(NULL, payload);
        rpc.startRPC(dest, NULL);

        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, trans->clientSendCount);
    }

    void test_startRPC_badTrans() {
        ClientRPC rpc(NULL);
        rpc.startRPC(dest, payload);

        // Should not segfault.
    }

    void test_startRPC_normal() {
        ClientRPC rpc(trans);
        rpc.startRPC(dest, payload);

        CPPUNIT_ASSERT_EQUAL((uint32_t) 1, trans->clientSendCount);
    }

    void test_getReply_badChecks() {
        ClientRPC rpc(trans);
        CPPUNIT_ASSERT_EQUAL(reinterpret_cast<Buffer*>(NULL), rpc.getReply());

        ClientRPC rpc2(NULL);
        CPPUNIT_ASSERT_EQUAL(reinterpret_cast<Buffer*>(NULL), rpc2.getReply());
    }

    void test_getReply_normal() {
        ClientRPC rpc(trans);
        rpc.startRPC(dest, payload);
        rpc.getReply();
        CPPUNIT_ASSERT_EQUAL((uint32_t) 1, trans->clientRecvCount);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 1, trans->clientSendCount);
    }

    void test_getReply_normalNonBlocking() {
        ClientRPC rpc(trans);
        rpc.startRPC(dest, payload);
        rpc.replyPayload = new Buffer();
        CPPUNIT_ASSERT_EQUAL(rpc.replyPayload, rpc.getReply());
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, trans->clientRecvCount);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 1, trans->clientSendCount);
    }

    DISALLOW_COPY_AND_ASSIGN(ClientRPCTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(ClientRPCTest);

class ServerRPCTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(ServerRPCTest);
    CPPUNIT_TEST(test_getRequest_badTrans);
    CPPUNIT_TEST(test_getRequest_normal);
    CPPUNIT_TEST(test_sendReply_badInputValues);
    CPPUNIT_TEST(test_sendReply_normal);
    CPPUNIT_TEST_SUITE_END();

    MockTransport* trans;
    Buffer* payload;
    Service* dest;

  public:
    ServerRPCTest()
            : trans(NULL), payload(NULL), dest(NULL) { }

    void setUp() {
        trans = new MockTransport();
        payload = new Buffer();
        dest = new Service();
    }

    void tearDown() {
        delete trans;
        delete payload;
        delete dest;
    }

    void test_getRequest_badTrans() {
        ServerRPC rpc(NULL);
        CPPUNIT_ASSERT_EQUAL(reinterpret_cast<Buffer*>(NULL), rpc.getRequest());
    }

    void test_getRequest_normal() {
        ServerRPC rpc(trans);
        rpc.getRequest();
        CPPUNIT_ASSERT_EQUAL((uint32_t) 1, trans->serverRecvCount);
    }

    void test_sendReply_badInputValues() {
        ServerRPC rpc(trans);
        rpc.getRequest();
        rpc.sendReply(NULL);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, trans->serverSendCount);

        ServerRPC rpc2(trans);
        rpc2.sendReply(payload);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, trans->serverSendCount);
    }

    void test_sendReply_normal() {
        ServerRPC rpc(trans);
        rpc.getRequest();
        rpc.sendReply(payload);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 1, trans->serverSendCount);
    }

    DISALLOW_COPY_AND_ASSIGN(ServerRPCTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(ServerRPCTest);

}  // namespace RAMCloud
