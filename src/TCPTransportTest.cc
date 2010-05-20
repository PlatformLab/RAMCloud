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
 * \file
 * Unit tests for RAMCloud::TCPTransport.
 */

// RAMCloud pragma [GCCWARN=5]

#include <Common.h>
#include <TCPTransport.h>

#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cppunit/extensions/HelperMacros.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace ::testing; // NOLINT

namespace RAMCloud {

/**
 * A google mock action that sets the first 64 bits of \a arg0.
 * \a arg0 is a \c void*, and \a v is a \c uint64_t.
 */
ACTION_P(Set64Bits, v) {
    *static_cast<uint64_t*>(arg0) = v;
}

/**
 * A google mock action that sets the length of a TCPTransport::Header.
 * \a arg0 is a \c void* that is really a TCPTransport::Header,
 * and \a l is an integer.
 */
ACTION_P(SetHeaderLen, l) {
    TCPTransport::Header* header;
    header = static_cast<TCPTransport::Header*>(arg0);
    header->len = l;
}

/**
 * Converts a <tt>const void*</tt> to a <tt>const int*</tt>.
 * \param p See above.
 * \return  See above.
 */
static const int*
constVoidStarToIntStar(const void* p) {
    return static_cast<const int*>(p);
}

/**
 * A google mock matcher that tests whether \a arg is equal to given
 * <tt>struct sockaddr_in</tt>.
 * \a arg is a <tt>const struct sockaddr*</tt> that is really a
 * <tt>const struct sockaddr_in*</tt>,
 * and \a exp is a <tt>struct sockaddr_in*</tt>.
 */
MATCHER_P(SockaddrInEq, exp, "") {
    const struct sockaddr_in* act;
    act = reinterpret_cast<const struct sockaddr_in*>(arg);
    return ((act->sin_family == exp->sin_family) &&
            (act->sin_port == exp->sin_port) &&
            (act->sin_addr.s_addr == exp->sin_addr.s_addr));
}

/**
 * A google mock matcher that tests whether \a arg is an IO vector for an empty
 * message.
 * \a arg is a <tt>const struct msghdr*</tt>.
 */
MATCHER(EmptyMsgOk, "") {
    if (((arg->msg_name != NULL) ||
         (arg->msg_namelen != 0) ||
         (arg->msg_control != NULL) ||
         (arg->msg_controllen != 0) ||
         (arg->msg_flags != 0) ||
         (arg->msg_iovlen != 1)))
        return false;
    struct iovec* iov = arg->msg_iov;
    TCPTransport::Header* header;
    header = static_cast<TCPTransport::Header*>(iov[0].iov_base);
    return ((iov[0].iov_len == sizeof(*header)) &&
            (header->len == 0));
}

/**
 * A google mock matcher that tests whether \a arg is an IO vector for a
 * hard-coded message.
 * See #RAMCloud::SocketTest::test_MessageSocket_send_twoChunksWithError().
 * \a arg is a <tt>const struct msghdr*</tt> and \a data is a \c char*.
 */
MATCHER_P(MsgOk, data, "") {
    if (arg->msg_iovlen != 3)
        return false;
    struct iovec* iov = arg->msg_iov;
    TCPTransport::Header* header;
    header = static_cast<TCPTransport::Header*>(iov[0].iov_base);
    return ((iov[0].iov_len == sizeof(*header)) &&
            (header->len == 24) &&
            (iov[1].iov_base == &data[0]) &&
            (iov[1].iov_len == 16) &&
            (iov[2].iov_base == &data[16]) &&
            (iov[2].iov_len == 8));
}

/**
 * A google mock class for TCPTransport::Syscalls.
 */
class MockSyscalls : public TCPTransport::Syscalls {
  public:
    MOCK_METHOD3(accept, int(int sockfd, struct sockaddr* addr,
                             socklen_t* addrlen));
    MOCK_METHOD3(bind, int(int sockfd, const struct sockaddr* addr,
                           socklen_t addrlen));
    MOCK_METHOD1(close, int(int fd));
    MOCK_METHOD3(connect, int(int sockfd, const struct sockaddr* addr,
                              socklen_t addrlen));
    MOCK_METHOD2(listen, int(int sockfd, int backlog));
    MOCK_METHOD4(recv, ssize_t(int sockfd, void* buf, size_t len, int flags));
    MOCK_METHOD3(sendmsg, ssize_t(int sockfd, const struct msghdr* msg,
                                  int flags));
    MOCK_METHOD5(setsockopt, int(int sockfd, int level, int optname,
                                 const void* optval, socklen_t optlen));
    MOCK_METHOD3(socket, int(int domain, int type, int protocol));
};

/**
 * A google mock class for TCPTransport::ServerSocket.
 */
class MockServerSocket : public TCPTransport::ServerSocket {
  public:
    MOCK_METHOD1(init, void(TCPTransport::ListenSocket* listenSocket));
    MOCK_METHOD1(recv, void(Buffer* payload));
    MOCK_METHOD1(send, void(const Buffer* payload));
};

/**
 * A google mock class for TCPTransport::ClientSocket.
 */
class MockClientSocket : public TCPTransport::ClientSocket {
  public:
    MOCK_METHOD2(init, void(const char* ip, uint16_t port));
    MOCK_METHOD1(recv, void(Buffer* payload));
    MOCK_METHOD1(send, void(const Buffer* payload));
};

/**
 * Unit tests for TCPTransport::Socket and its subclasses.
 */
class SocketTest : public CppUnit::TestFixture {
    DISALLOW_COPY_AND_ASSIGN(SocketTest); // NOLINT

    CPPUNIT_TEST_SUITE(SocketTest);
    CPPUNIT_TEST(test_Socket_destructor);
    CPPUNIT_TEST(test_MessageSocket_recv0);
    CPPUNIT_TEST(test_MessageSocket_recv8);
    CPPUNIT_TEST(test_MessageSocket_recv_hdrError);
    CPPUNIT_TEST(test_MessageSocket_recv_hdrPeerClosed);
    CPPUNIT_TEST(test_MessageSocket_recv_msgTooLong);
    CPPUNIT_TEST(test_MessageSocket_recv_dataError);
    CPPUNIT_TEST(test_MessageSocket_recv_dataPeerClosed);
    CPPUNIT_TEST(test_MessageSocket_send0);
    CPPUNIT_TEST(test_MessageSocket_send_twoChunksWithError);
    CPPUNIT_TEST(test_ServerSocket_init);
    CPPUNIT_TEST(test_ListenSocket_constructor_noop);
    CPPUNIT_TEST(test_ListenSocket_constructor_normal);
    CPPUNIT_TEST(test_ListenSocket_constructor_socketError);
    CPPUNIT_TEST(test_ListenSocket_constructor_listenError);
    CPPUNIT_TEST(test_ListenSocket_accept_normal);
    CPPUNIT_TEST(test_ListenSocket_accept_transientError);
    CPPUNIT_TEST(test_ListenSocket_accept_error);
    CPPUNIT_TEST(test_ClientSocket_init_normal);
    CPPUNIT_TEST(test_ClientSocket_init_socketError);
    CPPUNIT_TEST(test_ClientSocket_init_connectTransientError);
    CPPUNIT_TEST(test_ClientSocket_init_connectError);
    CPPUNIT_TEST_SUITE_END();

    /**
     * An instantiable TCPTransport::MessageSocket. That class is abstract.
     */
    class XMessageSocket : public TCPTransport::MessageSocket {
        public:
            XMessageSocket() {}
    };

  public:
    SocketTest() {}

    void tearDown() {
        // put TCPTransport::sys back to the real Syscalls implementation.
        extern TCPTransport::Syscalls realSyscalls;
        TCPTransport::sys = &realSyscalls;
    }

    void test_Socket_destructor() {
        StrictMock<MockSyscalls> ts;
        EXPECT_CALL(ts, close(10));
        TCPTransport::sys = &ts;

        TCPTransport::Socket s;
        s.fd = 10;
    }

    // 0-byte message
    void test_MessageSocket_recv0() {
        StrictMock<MockSyscalls> ts;
        {
            InSequence expectations;
            EXPECT_CALL(ts, recv(10, _, sizeof(TCPTransport::Header),
                                 MSG_WAITALL))
                .WillOnce(DoAll(WithArg<1>(SetHeaderLen(0)),
                                Return(sizeof(TCPTransport::Header))));
            EXPECT_CALL(ts, close(10));
        }
        TCPTransport::sys = &ts;

        XMessageSocket s;
        Buffer payload;
        s.fd = 10;
        s.recv(&payload);
        CPPUNIT_ASSERT(payload.totalLength() == 0);
    }

    // 8-byte message
    void test_MessageSocket_recv8() {
        StrictMock<MockSyscalls> ts;
        {
            InSequence expectations;
            EXPECT_CALL(ts, recv(10, _, sizeof(TCPTransport::Header),
                                 MSG_WAITALL))
                .WillOnce(DoAll(WithArg<1>(SetHeaderLen(8)),
                                Return(sizeof(TCPTransport::Header))));
            EXPECT_CALL(ts, recv(10, _, 8, MSG_WAITALL))
                .WillOnce(DoAll(WithArg<1>(Set64Bits(0x0123456789abcdef)),
                                Return(8)));
            EXPECT_CALL(ts, close(10));
        }
        TCPTransport::sys = &ts;

        Buffer payload;
        XMessageSocket s;
        s.fd = 10;
        s.recv(&payload);
        CPPUNIT_ASSERT(payload.totalLength() == 8);
        uint64_t* data = static_cast<uint64_t*>(payload.getRange(0, 8));
        CPPUNIT_ASSERT(*data == 0x0123456789abcdef);
    }

    void test_MessageSocket_recv_hdrError() {
        StrictMock<MockSyscalls> ts;
        {
            InSequence expectations;
            EXPECT_CALL(ts, recv(10, _, sizeof(TCPTransport::Header),
                                 MSG_WAITALL))
                .WillOnce(SetErrnoAndReturn(ENOMEM, -1));
            EXPECT_CALL(ts, close(10));
        }
        TCPTransport::sys = &ts;

        Buffer payload;
        XMessageSocket s;
        s.fd = 10;
        try {
            s.recv(&payload);
            CPPUNIT_ASSERT(false);
        } catch (Transport::Exception e) {}
    }

    void test_MessageSocket_recv_hdrPeerClosed() {
        StrictMock<MockSyscalls> ts;
        {
            InSequence expectations;
            EXPECT_CALL(ts, recv(10, _, sizeof(TCPTransport::Header),
                                 MSG_WAITALL));
            EXPECT_CALL(ts, close(10));
        }
        TCPTransport::sys = &ts;

        Buffer payload;
        XMessageSocket s;
        s.fd = 10;
        try {
            s.recv(&payload);
            CPPUNIT_ASSERT(false);
        } catch (Transport::Exception e) {}
    }

    void test_MessageSocket_recv_msgTooLong() {
        StrictMock<MockSyscalls> ts;
        {
            InSequence expectations;
            EXPECT_CALL(ts, recv(10, _, sizeof(TCPTransport::Header),
                                 MSG_WAITALL))
                .WillOnce(DoAll(WithArg<1>(SetHeaderLen(MAX_RPC_LEN + 1)),
                                Return(sizeof(TCPTransport::Header))));
            EXPECT_CALL(ts, close(10));
        }
        TCPTransport::sys = &ts;

        Buffer payload;
        XMessageSocket s;
        s.fd = 10;
        try {
            s.recv(&payload);
            CPPUNIT_ASSERT(false);
        } catch (Transport::Exception e) {}
    }

    void test_MessageSocket_recv_dataError() {
        StrictMock<MockSyscalls> ts;
        {
            InSequence expectations;
            EXPECT_CALL(ts, recv(10, _, sizeof(TCPTransport::Header),
                                 MSG_WAITALL))
                .WillOnce(DoAll(WithArg<1>(SetHeaderLen(8)),
                                Return(sizeof(TCPTransport::Header))));
            EXPECT_CALL(ts, recv(10, _, 8, MSG_WAITALL))
                .WillOnce(SetErrnoAndReturn(ENOMEM, -1));
            EXPECT_CALL(ts, close(10));
        }
        TCPTransport::sys = &ts;

        Buffer payload;
        XMessageSocket s;
        s.fd = 10;
        try {
            s.recv(&payload);
            CPPUNIT_ASSERT(false);
        } catch (Transport::Exception e) {}
    }

    void test_MessageSocket_recv_dataPeerClosed() {
        StrictMock<MockSyscalls> ts;
        {
            InSequence expectations;
            EXPECT_CALL(ts, recv(10, _, sizeof(TCPTransport::Header),
                                 MSG_WAITALL))
                .WillOnce(DoAll(WithArg<1>(SetHeaderLen(8)),
                                Return(sizeof(TCPTransport::Header))));
            EXPECT_CALL(ts, recv(10, _, 8, MSG_WAITALL));
            EXPECT_CALL(ts, close(10));
        }
        TCPTransport::sys = &ts;

        Buffer payload;
        XMessageSocket s;
        s.fd = 10;
        try {
            s.recv(&payload);
            CPPUNIT_ASSERT(false);
        } catch (Transport::Exception e) {}
    }

    // 0-byte message
    void test_MessageSocket_send0() {
        StrictMock<MockSyscalls> ts;
        {
            InSequence expectations;
            EXPECT_CALL(ts, sendmsg(10, EmptyMsgOk(), 0))
                .WillOnce(Return(sizeof(TCPTransport::Header)));
            EXPECT_CALL(ts, close(10));
        }
        TCPTransport::sys = &ts;

        XMessageSocket s;
        Buffer payload;
        s.fd = 10;
        s.send(&payload);
    }

    void test_MessageSocket_send_twoChunksWithError() {
        static const char data[24] = {
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17};

        StrictMock<MockSyscalls> ts;
        {
            InSequence expectations;
            EXPECT_CALL(ts, sendmsg(10, MsgOk(data), 0))
                .WillOnce(SetErrnoAndReturn(ENOMEM, -1));
            EXPECT_CALL(ts, close(10));
        }
        TCPTransport::sys = &ts;

        XMessageSocket s;
        Buffer payload;
        payload.append(&data[0], 16);
        payload.append(&data[16], 8);
        s.fd = 10;
        try {
            s.send(&payload);
            CPPUNIT_ASSERT(false);
        } catch (Transport::Exception e) {}
    }

    void test_ServerSocket_init() {
        // Aiming to get an 11 out of listenSocket.accept():
        StrictMock<MockSyscalls> ts;
        {
            InSequence expectations;
            EXPECT_CALL(ts, accept(10, NULL, NULL))
                .WillOnce(Return(11));
            EXPECT_CALL(ts, close(10));
            EXPECT_CALL(ts, close(11));
        }
        TCPTransport::sys = &ts;

        TCPTransport::ServerSocket s;
        TCPTransport::ListenSocket listenSocket(NULL, 0);
        listenSocket.fd = 10;
        s.init(&listenSocket);
        CPPUNIT_ASSERT(s.fd == 11);
    };

    void test_ListenSocket_constructor_noop() {
        {
            StrictMock<MockSyscalls> ts;
            TCPTransport::sys = &ts;
            TCPTransport::ListenSocket s(NULL, 0xabcd);
        }
        {
            StrictMock<MockSyscalls> ts;
            TCPTransport::sys = &ts;
            TCPTransport::ListenSocket s("0.0.0.0", 0);
        }
    }

    void test_ListenSocket_constructor_normal() {
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(0xabcd);
        addr.sin_addr.s_addr = 0x04030201;

        StrictMock<MockSyscalls> ts;
        {
            InSequence expectations;
            EXPECT_CALL(ts, socket(PF_INET, SOCK_STREAM, 0))
                .WillOnce(Return(10));
            EXPECT_CALL(ts, setsockopt(10, SOL_SOCKET, SO_REUSEADDR,
                                       ResultOf(constVoidStarToIntStar,
                                                Pointee(1)),
                                       sizeof(int))); // NOLINT
            EXPECT_CALL(ts, bind(10, SockaddrInEq(&addr),
                                    sizeof(struct sockaddr_in)));
            EXPECT_CALL(ts, listen(10, Gt(1000)));
            EXPECT_CALL(ts, close(10));
        }
        TCPTransport::sys = &ts;

        TCPTransport::ListenSocket s("1.2.3.4", 0xabcd);
    }

    void test_ListenSocket_constructor_socketError() {
        StrictMock<MockSyscalls> ts;
        EXPECT_CALL(ts, socket(PF_INET, SOCK_STREAM, 0))
            .WillOnce(SetErrnoAndReturn(ENOMEM, -1));
        TCPTransport::sys = &ts;

        try {
            TCPTransport::ListenSocket s("1.2.3.4", 0xabcd);
            CPPUNIT_ASSERT(false);
        } catch (Transport::UnrecoverableException e) {}
    }

    void test_ListenSocket_constructor_listenError() {
        // args checked in normal test
        StrictMock<MockSyscalls> ts;
        {
            InSequence expectations;
            EXPECT_CALL(ts, socket(_, _, _))
                .WillOnce(Return(10));
            EXPECT_CALL(ts, setsockopt(_, _, _, _, _));
            EXPECT_CALL(ts, bind(_, _, _));
            EXPECT_CALL(ts, listen(10, _))
                .WillOnce(SetErrnoAndReturn(ENOMEM, -1));
            EXPECT_CALL(ts, close(10));
        }
        TCPTransport::sys = &ts;

        try {
            TCPTransport::ListenSocket s("1.2.3.4", 0xabcd);
            CPPUNIT_ASSERT(false);
        } catch (Transport::UnrecoverableException e) {}
    }

    void test_ListenSocket_accept_normal() {
        StrictMock<MockSyscalls> ts;
        {
            InSequence expectations;
            EXPECT_CALL(ts, accept(10, NULL, NULL))
                .WillOnce(Return(11));
            EXPECT_CALL(ts, close(10));
        }
        TCPTransport::sys = &ts;

        TCPTransport::ListenSocket s(NULL, 0);
        s.fd = 10;
        CPPUNIT_ASSERT(s.accept() == 11);
    }

    void test_ListenSocket_accept_transientError() {
        StrictMock<MockSyscalls> ts;
        {
            InSequence expectations;
            EXPECT_CALL(ts, accept(10, NULL, NULL))
                .WillOnce(SetErrnoAndReturn(EHOSTUNREACH, -1));
            EXPECT_CALL(ts, accept(10, NULL, NULL))
                .WillOnce(Return(11));
            EXPECT_CALL(ts, close(10));
        }
        TCPTransport::sys = &ts;

        TCPTransport::ListenSocket s(NULL, 0);
        s.fd = 10;
        CPPUNIT_ASSERT(s.accept() == 11);
    }

    void test_ListenSocket_accept_error() {
        StrictMock<MockSyscalls> ts;
        {
            InSequence expectations;
            EXPECT_CALL(ts, accept(10, NULL, NULL))
                .WillOnce(SetErrnoAndReturn(ENOMEM, -1));
            EXPECT_CALL(ts, close(10));
        }
        TCPTransport::sys = &ts;

        TCPTransport::ListenSocket s(NULL, 0);
        s.fd = 10;
        try {
            s.accept();
            CPPUNIT_ASSERT(false);
        } catch (Transport::UnrecoverableException e) {}
    }

    void test_ClientSocket_init_normal() {

        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(0xabcd);
        addr.sin_addr.s_addr = 0x10204080;

        StrictMock<MockSyscalls> ts;
        {
            InSequence expectations;
            EXPECT_CALL(ts, socket(PF_INET, SOCK_STREAM, 0))
                .WillOnce(Return(10));
            EXPECT_CALL(ts, connect(10, SockaddrInEq(&addr),
                                    sizeof(struct sockaddr_in)));
            EXPECT_CALL(ts, close(10));
        }
        TCPTransport::sys = &ts;

        TCPTransport::ClientSocket s;
        s.init("128.64.32.16", 0xabcd);
    }

    void test_ClientSocket_init_socketError() {
        // args checked in normal test
        StrictMock<MockSyscalls> ts;
        EXPECT_CALL(ts, socket(_, _, _))
                .WillOnce(SetErrnoAndReturn(ENOMEM, -1));
        TCPTransport::sys = &ts;

        TCPTransport::ClientSocket s;
        try {
            s.init("128.64.32.16", 0xabcd);
            CPPUNIT_ASSERT(false);
        } catch (Transport::UnrecoverableException e) {}
    }

    void test_ClientSocket_init_connectTransientError() {
        // args checked in normal test
        StrictMock<MockSyscalls> ts;
        {
            InSequence expectations;
            EXPECT_CALL(ts, socket(_, _, _))
                .WillOnce(Return(10));
            EXPECT_CALL(ts, connect(_, _, _))
                .WillOnce(SetErrnoAndReturn(ETIMEDOUT, -1));
            EXPECT_CALL(ts, close(10));
        }
        TCPTransport::sys = &ts;

        TCPTransport::ClientSocket s;
        try {
            s.init("128.64.32.16", 0xabcd);
            CPPUNIT_ASSERT(false);
        } catch (Transport::Exception e) {}
    }

    void test_ClientSocket_init_connectError() {
        // args checked in normal test
        StrictMock<MockSyscalls> ts;
        {
            InSequence expectations;
            EXPECT_CALL(ts, socket(_, _, _))
                .WillOnce(Return(10));
            EXPECT_CALL(ts, connect(_, _, _))
                .WillOnce(SetErrnoAndReturn(ENOMEM, -1));
            EXPECT_CALL(ts, close(10));
        }
        TCPTransport::sys = &ts;

        TCPTransport::ClientSocket s;
        try {
            s.init("128.64.32.16", 0xabcd);
            CPPUNIT_ASSERT(false);
        } catch (Transport::UnrecoverableException e) {}
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(SocketTest);

/**
 * Unit tests for TCPTransport, TCPServerRPC, and TCPClientRPC.
 */
class TCPTransportTest : public CppUnit::TestFixture {
    DISALLOW_COPY_AND_ASSIGN(TCPTransportTest); // NOLINT

    CPPUNIT_TEST_SUITE(TCPTransportTest);
    CPPUNIT_TEST(test_TCPServerRPC_sendReply);
    CPPUNIT_TEST(test_TCPClientRPC_getReply);
    CPPUNIT_TEST(test_TCPTransport_constructor);
    CPPUNIT_TEST(test_TCPTransport_serverRecv);
    CPPUNIT_TEST(test_TCPTransport_clientSend);
    CPPUNIT_TEST_SUITE_END();

  public:
    TCPTransportTest() {}

    void tearDown() {
        // disable mock socket instances
        TCPTransport::TCPServerRPC::mockServerSocket = NULL;
        TCPTransport::TCPClientRPC::mockClientSocket = NULL;

        // put TCPTransport::sys back to the real Syscalls implementation.
        extern TCPTransport::Syscalls realSyscalls;
        TCPTransport::sys = &realSyscalls;
    }

    void test_TCPServerRPC_sendReply() {
        const Buffer* send_expect;

        StrictMock<MockServerSocket> ts;
        EXPECT_CALL(ts, send(_))
            .WillOnce(SaveArg<0>(&send_expect));
        TCPTransport::TCPServerRPC::mockServerSocket = &ts;

        StrictMock<MockSyscalls> tsc;
        EXPECT_CALL(tsc, close(10));
        TCPTransport::sys = &tsc;

        TCPTransport t(NULL, 0);
        TCPTransport::TCPServerRPC* rpc = new TCPTransport::TCPServerRPC();
        rpc->realServerSocket.fd = 10;
        rpc->sendReply();
        CPPUNIT_ASSERT(send_expect == &rpc->replyPayload);
    }

    void test_TCPClientRPC_getReply() {
        Buffer payload;

        StrictMock<MockClientSocket> ts;
        EXPECT_CALL(ts, recv(&payload));
        TCPTransport::TCPClientRPC::mockClientSocket = &ts;

        StrictMock<MockSyscalls> tsc;
        EXPECT_CALL(tsc, close(10));
        TCPTransport::sys = &tsc;

        TCPTransport t(NULL, 0);
        TCPTransport::TCPClientRPC* rpc = new TCPTransport::TCPClientRPC();
        rpc->reply = &payload;
        rpc->realClientSocket.fd = 10;
        rpc->getReply();
    }

    void test_TCPTransport_constructor() {
        // the constructor doesn't add anything over ListenSocket(), so this is
        // just a placeholder
        TCPTransport t(NULL, 0);
    }

    void test_TCPTransport_serverRecv() {
        Buffer* recv_expect;
        Buffer* recv_expect2;
        TCPTransport t(NULL, 0);

        StrictMock<MockServerSocket> ts;
        {
            InSequence expectations;
            EXPECT_CALL(ts, init(&t.listenSocket));
            EXPECT_CALL(ts, recv(_))
                .WillOnce(DoAll(SaveArg<0>(&recv_expect),
                                Throw(Transport::Exception())));
            EXPECT_CALL(ts, init(&t.listenSocket));
            EXPECT_CALL(ts, recv(_))
                .WillOnce(SaveArg<0>(&recv_expect2));
        }

        TCPTransport::TCPServerRPC::mockServerSocket = &ts;

        Transport::ServerRPC* rpc = t.serverRecv();
        CPPUNIT_ASSERT(recv_expect == &rpc->recvPayload);
        CPPUNIT_ASSERT(recv_expect2 == &rpc->recvPayload);
        rpc->ignore();
    }

    void test_TCPTransport_clientSend() {
        TCPTransport t(NULL, 0);
        Buffer payload;
        Buffer response;

        StrictMock<MockClientSocket> ts;
        {
            InSequence expectations;
            EXPECT_CALL(ts, init(StrEq("1.2.3.4"), 0xef01));
            EXPECT_CALL(ts, send(&payload));
        }
        TCPTransport::TCPClientRPC::mockClientSocket = &ts;

        Service s;
        s.setIp("1.2.3.4");
        s.setPort(0xef01);
        delete t.clientSend(&s, &payload, &response);
    }

};
CPPUNIT_TEST_SUITE_REGISTRATION(TCPTransportTest);

} // namespace RAMCloud
