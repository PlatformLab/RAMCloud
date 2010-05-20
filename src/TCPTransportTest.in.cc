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
 *
 * Everything between BEGIN_MOCK and END_MOCK is handled by the Python
 * preprocessor to this file, TCPTransportTestMock.py. The things that look
 * like methods in between those actually declare expected method invocations.
 * Look at how a couple examples translate to TCPTransportTest.cc and you'll
 * figure it out.
 */

// RAMCloud pragma [GCCWARN=5]

#define BEGIN_MOCK "fail, see above"
#define END_MOCK "fail, see above"

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
 * An implementation of TCPTransport::Syscalls that complains when invoked.
 * The mock classes extend this.
 */
class SyscallsStub : public TCPTransport::Syscalls {
  public:
    struct NotImplementedException {};
    int accept(int sockfd, struct sockaddr* addr, socklen_t* addrlen)
        __attribute__ ((noreturn)) {
        throw NotImplementedException();
    }
    int bind(int sockfd, const struct sockaddr* addr, socklen_t addrlen)
        __attribute__ ((noreturn)) {
        throw NotImplementedException();
    }
    int close(int fd) __attribute__ ((noreturn)) {
        throw NotImplementedException();
    }
    int connect(int sockfd, const struct sockaddr* addr, socklen_t addrlen)
        __attribute__ ((noreturn)) {
        throw NotImplementedException();
    }
    int listen(int sockfd, int backlog) __attribute__ ((noreturn)) {
        throw NotImplementedException();
    }
    ssize_t recv(int sockfd, void* buf, size_t len, int flags)
        __attribute__ ((noreturn)) {
        throw NotImplementedException();
    }
    ssize_t sendmsg(int sockfd, const struct msghdr* msg, int flags)
        __attribute__ ((noreturn)) {
        throw NotImplementedException();
    }
    int setsockopt(int sockfd, int level, int optname, const void* optval,
                   socklen_t optlen) __attribute__ ((noreturn)) {
        throw NotImplementedException();
    }
    int socket(int domain, int type, int protocol) __attribute__ ((noreturn)) {
        throw NotImplementedException();
    }
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
        BEGIN_MOCK(TS, SyscallsStub);
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TS ts;
        TCPTransport::sys = &ts;

        TCPTransport::Socket s;
        s.fd = 10;
    }

    // 0-byte message
    void test_MessageSocket_recv0() {
        BEGIN_MOCK(TS, SyscallsStub);
            recv(sockfd == 10, buf, len == sizeof(TCPTransport::Header), \
                 flags == MSG_WAITALL) {
                TCPTransport::Header* header;
                header = static_cast<TCPTransport::Header*>(buf);
                header->len = 0;
                return len;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TS ts;
        TCPTransport::sys = &ts;

        XMessageSocket s;
        Buffer payload;
        s.fd = 10;
        s.recv(&payload);
        CPPUNIT_ASSERT(payload.totalLength() == 0);
    }

    // 8-byte message
    void test_MessageSocket_recv8() {
        BEGIN_MOCK(TS, SyscallsStub);
            recv(sockfd == 10, buf, len == sizeof(TCPTransport::Header), \
                 flags == MSG_WAITALL) {
                TCPTransport::Header* header;
                header = static_cast<TCPTransport::Header*>(buf);
                header->len = 8;
                return len;
            }
            recv(sockfd == 10, buf, len == 8, flags == MSG_WAITALL) {
                *static_cast<uint64_t*>(buf) = 0x0123456789abcdef;
                return 8;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        Buffer payload;
        {
            TS ts;
            TCPTransport::sys = &ts;

            XMessageSocket s;
            s.fd = 10;
            s.recv(&payload);
        }
        CPPUNIT_ASSERT(payload.totalLength() == 8);
        uint64_t* data = static_cast<uint64_t*>(payload.getRange(0, 8));
        CPPUNIT_ASSERT(*data == 0x0123456789abcdef);
    }

    void test_MessageSocket_recv_hdrError() {
        BEGIN_MOCK(TS, SyscallsStub);
            recv(sockfd == 10, buf, len == sizeof(TCPTransport::Header), \
                 flags == MSG_WAITALL) {
                errno = ENOMEM;
                return -1;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        Buffer payload;
        TS ts;
        TCPTransport::sys = &ts;

        XMessageSocket s;
        s.fd = 10;
        try {
            s.recv(&payload);
            CPPUNIT_ASSERT(false);
        } catch (Transport::Exception e) {}
    }

    void test_MessageSocket_recv_hdrPeerClosed() {
        BEGIN_MOCK(TS, SyscallsStub);
            recv(sockfd == 10, buf, len == sizeof(TCPTransport::Header), \
                 flags == MSG_WAITALL) {
                return 0;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        Buffer payload;
        TS ts;
        TCPTransport::sys = &ts;

        XMessageSocket s;
        s.fd = 10;
        try {
            s.recv(&payload);
            CPPUNIT_ASSERT(false);
        } catch (Transport::Exception e) {}
    }

    void test_MessageSocket_recv_msgTooLong() {
        BEGIN_MOCK(TS, SyscallsStub);
            recv(sockfd == 10, buf, len == sizeof(TCPTransport::Header), \
                 flags == MSG_WAITALL) {
                TCPTransport::Header* header;
                header = static_cast<TCPTransport::Header*>(buf);
                header->len = MAX_RPC_LEN + 1;
                return len;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        Buffer payload;
        TS ts;
        TCPTransport::sys = &ts;

        XMessageSocket s;
        s.fd = 10;
        try {
            s.recv(&payload);
            CPPUNIT_ASSERT(false);
        } catch (Transport::Exception e) {}
    }

    void test_MessageSocket_recv_dataError() {
        BEGIN_MOCK(TS, SyscallsStub);
            recv(sockfd == 10, buf, len == sizeof(TCPTransport::Header), \
                 flags == MSG_WAITALL) {
                TCPTransport::Header* header;
                header = static_cast<TCPTransport::Header*>(buf);
                header->len = 8;
                return len;
            }
            recv(sockfd == 10, buf, len == 8, flags == MSG_WAITALL) {
                errno = ENOMEM;
                return -1;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        Buffer payload;
        TS ts;
        TCPTransport::sys = &ts;

        XMessageSocket s;
        s.fd = 10;
        try {
            s.recv(&payload);
            CPPUNIT_ASSERT(false);
        } catch (Transport::Exception e) {}
    }

    void test_MessageSocket_recv_dataPeerClosed() {
        BEGIN_MOCK(TS, SyscallsStub);
            recv(sockfd == 10, buf, len == sizeof(TCPTransport::Header), \
                 flags == MSG_WAITALL) {
                TCPTransport::Header* header;
                header = static_cast<TCPTransport::Header*>(buf);
                header->len = 8;
                return len;
            }
            recv(sockfd == 10, buf, len == 8, flags == MSG_WAITALL) {
                return 0;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        Buffer payload;
        TS ts;
        TCPTransport::sys = &ts;

        XMessageSocket s;
        s.fd = 10;
        try {
            s.recv(&payload);
            CPPUNIT_ASSERT(false);
        } catch (Transport::Exception e) {}
    }

    // 0-byte message
    void test_MessageSocket_send0() {
        BEGIN_MOCK(TS, SyscallsStub);
            sendmsg(sockfd == 10, msg, flags == 0) {
                CPPUNIT_ASSERT(msg->msg_name == NULL);
                CPPUNIT_ASSERT(msg->msg_namelen == 0);
                CPPUNIT_ASSERT(msg->msg_control == NULL);
                CPPUNIT_ASSERT(msg->msg_controllen == 0);
                CPPUNIT_ASSERT(msg->msg_flags == 0);
                CPPUNIT_ASSERT(msg->msg_iovlen == 1);
                struct iovec* iov = msg->msg_iov;
                TCPTransport::Header* header;
                header = static_cast<TCPTransport::Header*>(iov[0].iov_base);
                CPPUNIT_ASSERT(iov[0].iov_len == sizeof(*header));
                CPPUNIT_ASSERT(header->len == 0);
                return sizeof(*header);
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TS ts;
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

        BEGIN_MOCK(TS, SyscallsStub);
            sendmsg(sockfd == 10, msg, flags == 0) {
                CPPUNIT_ASSERT(msg->msg_iovlen == 3);
                struct iovec* iov = msg->msg_iov;
                TCPTransport::Header* header;
                header = static_cast<TCPTransport::Header*>(iov[0].iov_base);
                CPPUNIT_ASSERT(iov[0].iov_len == sizeof(*header));
                CPPUNIT_ASSERT(header->len == 24);
                CPPUNIT_ASSERT(iov[1].iov_base == &data[0]);
                CPPUNIT_ASSERT(iov[1].iov_len == 16);
                CPPUNIT_ASSERT(iov[2].iov_base == &data[16]);
                CPPUNIT_ASSERT(iov[2].iov_len == 8);
                errno = ENOMEM;
                return -1;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TS ts;
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
        // This is an annoying amount of unrelated code to get an 11 out of
        // listenSocket->accept().
        BEGIN_MOCK(TS, SyscallsStub);
            accept(sockfd == 10, addr == NULL, addrlen == NULL) {
                return 11;
            }
            close(fd == 10) {
                return 0;
            }
            close(fd == 11) {
                return 0;
            }
        END_MOCK();

        TS ts;
        TCPTransport::sys = &ts;

        TCPTransport::ServerSocket s;
        TCPTransport::ListenSocket listenSocket(NULL, 0);
        listenSocket.fd = 10;
        s.init(&listenSocket);
        CPPUNIT_ASSERT(s.fd == 11);
    };

    void test_ListenSocket_constructor_noop() {
        BEGIN_MOCK(TS, SyscallsStub);
        END_MOCK();

        {
            TS ts;
            TCPTransport::sys = &ts;
            TCPTransport::ListenSocket s(NULL, 0xabcd);
        }
        {
            TS ts;
            TCPTransport::sys = &ts;
            TCPTransport::ListenSocket s("0.0.0.0", 0);
        }
    }

    void test_ListenSocket_constructor_normal() {
        BEGIN_MOCK(TS, SyscallsStub);
            socket(domain == PF_INET, type == SOCK_STREAM, protocol == 0) {
                return 10;
            }
            setsockopt(sockfd == 10, level == SOL_SOCKET, \
                       optname == SO_REUSEADDR, optval, optlen) {
                CPPUNIT_ASSERT(optlen == sizeof(int)); // NOLINT
                const int* val = static_cast<const int*>(optval);
                CPPUNIT_ASSERT(*val == 1);
                return 0;
            }
            bind(sockfd == 10, addr, addrlen == sizeof(struct sockaddr_in)) {
                const struct sockaddr_in* a;
                a = reinterpret_cast<const struct sockaddr_in*>(addr);
                CPPUNIT_ASSERT(a->sin_family == AF_INET);
                CPPUNIT_ASSERT(a->sin_port == htons(0xabcd));
                CPPUNIT_ASSERT(a->sin_addr.s_addr == 0x04030201);
                return 0;
            }
            listen(sockfd == 10, backlog) {
                CPPUNIT_ASSERT(backlog > 1000);
                return 0;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TS ts;
        TCPTransport::sys = &ts;

        TCPTransport::ListenSocket s("1.2.3.4", 0xabcd);
    }

    void test_ListenSocket_constructor_socketError() {
        BEGIN_MOCK(TS, SyscallsStub);
            socket(domain == PF_INET, type == SOCK_STREAM, protocol == 0) {
                errno = ENOMEM;
                return -1;
            }
        END_MOCK();

        TS ts;
        TCPTransport::sys = &ts;

        try {
            TCPTransport::ListenSocket s("1.2.3.4", 0xabcd);
            CPPUNIT_ASSERT(false);
        } catch (Transport::UnrecoverableException e) {}
    }

    void test_ListenSocket_constructor_listenError() {
        // args checked in normal test
        BEGIN_MOCK(TS, SyscallsStub);
            socket(domain, type, protocol) {
                return 10;
            }
            setsockopt(sockfd, level, optname, optval, optlen) {
                return 0;
            }
            bind(sockfd, addr, addrlen) {
                return 0;
            }
            listen(sockfd == 10, backlog) {
                errno = ENOMEM;
                return -1;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TS ts;
        TCPTransport::sys = &ts;

        try {
            TCPTransport::ListenSocket s("1.2.3.4", 0xabcd);
            CPPUNIT_ASSERT(false);
        } catch (Transport::UnrecoverableException e) {}
    }

    void test_ListenSocket_accept_normal() {
        BEGIN_MOCK(TS, SyscallsStub);
            accept(sockfd == 10, addr == NULL, addrlen == NULL) {
                return 11;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TS ts;
        TCPTransport::sys = &ts;

        TCPTransport::ListenSocket s(NULL, 0);
        s.fd = 10;
        CPPUNIT_ASSERT(s.accept() == 11);
    }

    void test_ListenSocket_accept_transientError() {
        BEGIN_MOCK(TS, SyscallsStub);
            accept(sockfd == 10, addr == NULL, addrlen == NULL) {
                errno = EHOSTUNREACH;
                return -1;
            }
            accept(sockfd == 10, addr == NULL, addrlen == NULL) {
                return 11;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TS ts;
        TCPTransport::sys = &ts;

        TCPTransport::ListenSocket s(NULL, 0);
        s.fd = 10;
        CPPUNIT_ASSERT(s.accept() == 11);
    }

    void test_ListenSocket_accept_error() {
        BEGIN_MOCK(TS, SyscallsStub);
            accept(sockfd == 10, addr == NULL, addrlen == NULL) {
                errno = ENOMEM;
                return -1;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TS ts;
        TCPTransport::sys = &ts;

        TCPTransport::ListenSocket s(NULL, 0);
        s.fd = 10;
        try {
            s.accept();
            CPPUNIT_ASSERT(false);
        } catch (Transport::UnrecoverableException e) {}
    }

    void test_ClientSocket_init_normal() {
        BEGIN_MOCK(TS, SyscallsStub);
            socket(domain == PF_INET, type == SOCK_STREAM, protocol == 0) {
                return 10;
            }
            connect(sockfd == 10, addr, addrlen == sizeof(struct sockaddr_in)) {
                const struct sockaddr_in* a;
                a = reinterpret_cast<const struct sockaddr_in*>(addr);
                CPPUNIT_ASSERT(a->sin_family == AF_INET);
                CPPUNIT_ASSERT(a->sin_port == htons(0xabcd));
                CPPUNIT_ASSERT(a->sin_addr.s_addr == 0x10204080);
                return 0;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TS ts;
        TCPTransport::sys = &ts;

        TCPTransport::ClientSocket s;
        s.init("128.64.32.16", 0xabcd);
    }

    void test_ClientSocket_init_socketError() {
        // args checked in normal test
        BEGIN_MOCK(TS, SyscallsStub);
            socket(domain, type, protocol) {
                errno = ENOMEM;
                return -1;
            }
        END_MOCK();

        TS ts;
        TCPTransport::sys = &ts;

        TCPTransport::ClientSocket s;
        try {
            s.init("128.64.32.16", 0xabcd);
            CPPUNIT_ASSERT(false);
        } catch (Transport::UnrecoverableException e) {}
    }

    void test_ClientSocket_init_connectTransientError() {
        // args checked in normal test
        BEGIN_MOCK(TS, SyscallsStub);
            socket(domain, type, protocol) {
                return 10;
            }
            connect(sockfd, addr, addrlen) {
                errno = ETIMEDOUT;
                return -1;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TS ts;
        TCPTransport::sys = &ts;

        TCPTransport::ClientSocket s;
        try {
            s.init("128.64.32.16", 0xabcd);
            CPPUNIT_ASSERT(false);
        } catch (Transport::Exception e) {}
    }

    void test_ClientSocket_init_connectError() {
        // args checked in normal test
        BEGIN_MOCK(TS, SyscallsStub);
            socket(domain, type, protocol) {
                return 10;
            }
            connect(sockfd, addr, addrlen) {
                errno = ENOMEM;
                return -1;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TS ts;
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

        BEGIN_MOCK(TSC, SyscallsStub);
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TSC tsc;
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

        BEGIN_MOCK(TSC, SyscallsStub);
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TSC tsc;
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
