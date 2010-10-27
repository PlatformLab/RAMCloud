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

// Everything between BEGIN_MOCK and END_MOCK is handled by the Python
// preprocessor to this file, TcpTransportTestMock.py. The things that look
// like methods in between those actually declare expected method invocations.
// Look at how a couple examples translate to TcpTransportTest.cc and you'll
// figure it out.
#define BEGIN_MOCK "fail, see above"
#define END_MOCK "fail, see above"

#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "TestUtil.h"

#include "Common.h"
#include "TcpTransport.h"

namespace RAMCloud {

/**
 * An implementation of TcpTransport::Syscalls that complains when invoked.
 * The mock classes extend this.
 */
BEGIN_STUB(SyscallsStub, TcpTransport::Syscalls);
    int accept(int sockfd, struct sockaddr* addr, socklen_t* addrlen);
    int bind(int sockfd, const struct sockaddr* addr, socklen_t addrlen);
    int close(int fd);
    int connect(int sockfd, const struct sockaddr* addr, socklen_t addrlen);
    int fcntl(int fd, int cmd, int arg1);
    int listen(int sockfd, int backlog);
    ssize_t recv(int sockfd, void* buf, size_t len, int flags);
    ssize_t sendmsg(int sockfd, const struct msghdr* msg, int flags);
    int setsockopt(int sockfd, int level, int optname, const void* optval, \
                   socklen_t optlen);
    int socket(int domain, int type, int protocol);
END_STUB();

/**
 * An implementation of TcpTransport::ServerSocket that complains when invoked.
 * The mock classes extend this.
 */
BEGIN_STUB(ServerSocketStub, TcpTransport::ServerSocket);
    void init(TcpTransport::ListenSocket* listenSocket);
    void recv(Buffer* payload);
    void send(const Buffer* payload);
END_STUB();

/**
 * An implementation of TcpTransport::ClientSocket that complains when invoked.
 * The mock classes extend this.
 */
BEGIN_STUB(ClientSocketStub, TcpTransport::ClientSocket);
    void init(const IpAddress &address);
    void recv(Buffer* payload);
    void send(const Buffer* payload);
END_STUB();

/**
 * Unit tests for TcpTransport::Socket and its subclasses.
 */
class SocketTest : public CppUnit::TestFixture {
    DISALLOW_COPY_AND_ASSIGN(SocketTest); // NOLINT

    CPPUNIT_TEST_SUITE(SocketTest);
    CPPUNIT_TEST(test_Socket_destructor);
    CPPUNIT_TEST(test_MessageSocket_recv0);
    CPPUNIT_TEST(test_MessageSocket_recv8);
    CPPUNIT_TEST(test_MessageSocket_recv_clearPayload);
    CPPUNIT_TEST(test_MessageSocket_recv_hdrError);
    CPPUNIT_TEST(test_MessageSocket_recv_hdrPeerClosed);
    CPPUNIT_TEST(test_MessageSocket_recv_msgTooLong);
    CPPUNIT_TEST(test_MessageSocket_recv_dataError);
    CPPUNIT_TEST(test_MessageSocket_recv_dataPeerClosed);
    CPPUNIT_TEST(test_MessageSocket_send0);
    CPPUNIT_TEST(test_MessageSocket_send_twoChunksWithError);
    CPPUNIT_TEST(test_ServerSocket_init_connectionWaiting);
    CPPUNIT_TEST(test_ServerSocket_init_noConnectionWaiting);
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
     * An instantiable TcpTransport::MessageSocket. That class is abstract.
     */
    class XMessageSocket : public TcpTransport::MessageSocket {
        public:
            XMessageSocket() {}
    };

  public:
    SocketTest() {}

    void tearDown() {
        // put TcpTransport::sys back to the real Syscalls implementation.
        extern TcpTransport::Syscalls realSyscalls;
        TcpTransport::sys = &realSyscalls;
    }

    void test_Socket_destructor() {
        BEGIN_MOCK(TS, SyscallsStub);
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TS ts;
        TcpTransport::sys = &ts;

        TcpTransport::Socket s;
        s.fd = 10;
    }

    // 0-byte message
    void test_MessageSocket_recv0() {
        BEGIN_MOCK(TS, SyscallsStub);
            recv(sockfd == 10, buf, len == sizeof(TcpTransport::Header), \
                 flags == MSG_WAITALL) {
                TcpTransport::Header* header;
                header = static_cast<TcpTransport::Header*>(buf);
                header->len = 0;
                return len;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TS ts;
        TcpTransport::sys = &ts;

        XMessageSocket s;
        Buffer payload;
        s.fd = 10;
        s.recv(&payload);
        CPPUNIT_ASSERT(payload.getTotalLength() == 0);
    }

    // 8-byte message
    void test_MessageSocket_recv8() {
        BEGIN_MOCK(TS, SyscallsStub);
            recv(sockfd == 10, buf, len == sizeof(TcpTransport::Header), \
                 flags == MSG_WAITALL) {
                TcpTransport::Header* header;
                header = static_cast<TcpTransport::Header*>(buf);
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
            TcpTransport::sys = &ts;

            XMessageSocket s;
            s.fd = 10;
            s.recv(&payload);
        }
        CPPUNIT_ASSERT(payload.getTotalLength() == 8);
        const uint64_t* data = payload.getStart<uint64_t>();
        CPPUNIT_ASSERT(*data == 0x0123456789abcdef);
    }

    // 8-byte message, but payload starts off with garbage that must
    // be cleared.
    void test_MessageSocket_recv_clearPayload() {
        BEGIN_MOCK(TS, SyscallsStub);
            recv(sockfd == 10, buf, len == sizeof(TcpTransport::Header), \
                 flags == MSG_WAITALL) {
                TcpTransport::Header* header;
                header = static_cast<TcpTransport::Header*>(buf);
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
        const char *junk = "initial garbage";
        memcpy(new(&payload, APPEND) char[strlen(junk)], junk, strlen(junk));
        {
            TS ts;
            TcpTransport::sys = &ts;

            XMessageSocket s;
            s.fd = 10;
            s.recv(&payload);
        }
        CPPUNIT_ASSERT(payload.getTotalLength() == 8);
        const uint64_t* data = payload.getStart<uint64_t>();
        CPPUNIT_ASSERT(*data == 0x0123456789abcdef);
    }

    void test_MessageSocket_recv_hdrError() {
        BEGIN_MOCK(TS, SyscallsStub);
            recv(sockfd == 10, buf, len == sizeof(TcpTransport::Header), \
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
        TcpTransport::sys = &ts;

        XMessageSocket s;
        s.fd = 10;
        try {
            s.recv(&payload);
            CPPUNIT_ASSERT(false);
        } catch (TransportException& e) {}
    }

    void test_MessageSocket_recv_hdrPeerClosed() {
        BEGIN_MOCK(TS, SyscallsStub);
            recv(sockfd == 10, buf, len == sizeof(TcpTransport::Header), \
                 flags == MSG_WAITALL) {
                return 0;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        Buffer payload;
        TS ts;
        TcpTransport::sys = &ts;

        XMessageSocket s;
        s.fd = 10;
        try {
            s.recv(&payload);
            CPPUNIT_ASSERT(false);
        } catch (TransportException& e) {}
    }

    void test_MessageSocket_recv_msgTooLong() {
        BEGIN_MOCK(TS, SyscallsStub);
            recv(sockfd == 10, buf, len == sizeof(TcpTransport::Header), \
                 flags == MSG_WAITALL) {
                TcpTransport::Header* header;
                header = static_cast<TcpTransport::Header*>(buf);
                header->len = Transport::MAX_RPC_LEN + 1;
                return len;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        Buffer payload;
        TS ts;
        TcpTransport::sys = &ts;

        XMessageSocket s;
        s.fd = 10;
        try {
            s.recv(&payload);
            CPPUNIT_ASSERT(false);
        } catch (TransportException& e) {}
    }

    void test_MessageSocket_recv_dataError() {
        BEGIN_MOCK(TS, SyscallsStub);
            recv(sockfd == 10, buf, len == sizeof(TcpTransport::Header), \
                 flags == MSG_WAITALL) {
                TcpTransport::Header* header;
                header = static_cast<TcpTransport::Header*>(buf);
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
        TcpTransport::sys = &ts;

        XMessageSocket s;
        s.fd = 10;
        try {
            s.recv(&payload);
            CPPUNIT_ASSERT(false);
        } catch (TransportException& e) {}
    }

    void test_MessageSocket_recv_dataPeerClosed() {
        BEGIN_MOCK(TS, SyscallsStub);
            recv(sockfd == 10, buf, len == sizeof(TcpTransport::Header), \
                 flags == MSG_WAITALL) {
                TcpTransport::Header* header;
                header = static_cast<TcpTransport::Header*>(buf);
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
        TcpTransport::sys = &ts;

        XMessageSocket s;
        s.fd = 10;
        try {
            s.recv(&payload);
            CPPUNIT_ASSERT(false);
        } catch (TransportException& e) {}
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
                TcpTransport::Header* header;
                header = static_cast<TcpTransport::Header*>(iov[0].iov_base);
                CPPUNIT_ASSERT(iov[0].iov_len == sizeof(*header));
                CPPUNIT_ASSERT(header->len == 0);
                return sizeof(*header);
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TS ts;
        TcpTransport::sys = &ts;

        XMessageSocket s;
        Buffer payload;
        s.fd = 10;
        s.send(&payload);
    }

    void test_MessageSocket_send_twoChunksWithError() {
        static char data[24] = {
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17};

        BEGIN_MOCK(TS, SyscallsStub);
            sendmsg(sockfd == 10, msg, flags == 0) {
                CPPUNIT_ASSERT(msg->msg_iovlen == 3);
                struct iovec* iov = msg->msg_iov;
                TcpTransport::Header* header;
                header = static_cast<TcpTransport::Header*>(iov[0].iov_base);
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
        TcpTransport::sys = &ts;

        XMessageSocket s;
        Buffer payload;
        Buffer::Chunk::appendToBuffer(&payload, &data[0], 16);
        Buffer::Chunk::appendToBuffer(&payload, &data[16], 8);
        s.fd = 10;
        try {
            s.send(&payload);
            CPPUNIT_ASSERT(false);
        } catch (TransportException& e) {}
    }

    void test_ServerSocket_init_connectionWaiting() {
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
        TcpTransport::sys = &ts;

        TcpTransport::ServerSocket s;
        TcpTransport::ListenSocket listenSocket;
        listenSocket.fd = 10;
        s.init(&listenSocket);
        CPPUNIT_ASSERT(s.fd == 11);
    }

    void test_ServerSocket_init_noConnectionWaiting() {
        // This is an annoying amount of unrelated code to get an 11 out of
        // listenSocket->accept().
        BEGIN_MOCK(TS, SyscallsStub);
            accept(sockfd == 10, addr == NULL, addrlen == NULL) {
                errno = EAGAIN;
                return -1;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TS ts;
        TcpTransport::sys = &ts;

        TcpTransport::ServerSocket s;
        TcpTransport::ListenSocket listenSocket;
        listenSocket.fd = 10;
        CPPUNIT_ASSERT_THROW(s.init(&listenSocket), TransportException);
        CPPUNIT_ASSERT(s.fd == -1);
    }

    void test_ListenSocket_constructor_noop() {
        BEGIN_MOCK(TS, SyscallsStub);
        END_MOCK();
        TS ts;
        TcpTransport::sys = &ts;
        TcpTransport::ListenSocket s;
    }

    void test_ListenSocket_constructor_normal() {
        BEGIN_MOCK(TS, SyscallsStub);
            socket(domain == PF_INET, type == SOCK_STREAM, protocol == 0) {
                return 10;
            }
            fcntl(fd == 10, cmd == F_SETFL, arg1 == O_NONBLOCK) {
                return 0;
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
                CPPUNIT_ASSERT(a->sin_port == htons(8888));
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
        TcpTransport::sys = &ts;

        ServiceLocator serviceLocator("tcp: host=1.2.3.4, port=8888");
        TcpTransport::ListenSocket s(&serviceLocator);
    }

    void test_ListenSocket_constructor_socketError() {
        BEGIN_MOCK(TS, SyscallsStub);
            socket(domain == PF_INET, type == SOCK_STREAM, protocol == 0) {
                errno = ENOMEM;
                return -1;
            }
        END_MOCK();

        TS ts;
        TcpTransport::sys = &ts;

        ServiceLocator serviceLocator("tcp: host=1.2.3.4, port=8888");
        try {
            TcpTransport::ListenSocket s(&serviceLocator);
            CPPUNIT_ASSERT(false);
        } catch (UnrecoverableTransportException& e) {}
    }

    void test_ListenSocket_constructor_listenError() {
        // args checked in normal test
        BEGIN_MOCK(TS, SyscallsStub);
            socket(domain, type, protocol) {
                return 10;
            }
            fcntl(fd == 10, cmd == F_SETFL, arg1 == O_NONBLOCK) {
                return 0;
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
        TcpTransport::sys = &ts;

        ServiceLocator serviceLocator("tcp: host=1.2.3.4, port=8888");
        try {
            TcpTransport::ListenSocket s(&serviceLocator);
            CPPUNIT_ASSERT(false);
        } catch (UnrecoverableTransportException& e) {}
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
        TcpTransport::sys = &ts;

        TcpTransport::ListenSocket s;
        s.fd = 10;
        CPPUNIT_ASSERT(s.accept() == 11);
    }

    void test_ListenSocket_accept_transientError() {
        BEGIN_MOCK(TS, SyscallsStub);
            accept(sockfd == 10, addr == NULL, addrlen == NULL) {
                errno = EHOSTUNREACH;
                return -1;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TS ts;
        TcpTransport::sys = &ts;

        TcpTransport::ListenSocket s;
        s.fd = 10;
        CPPUNIT_ASSERT(s.accept() == -1);
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
        TcpTransport::sys = &ts;

        TcpTransport::ListenSocket s;
        s.fd = 10;
        try {
            s.accept();
            CPPUNIT_ASSERT(false);
        } catch (UnrecoverableTransportException& e) {}
    }

    void test_ClientSocket_init_normal() {
        BEGIN_MOCK(TS, SyscallsStub);
            socket(domain == PF_INET, type == SOCK_STREAM, protocol == 0) {
                return 10;
            }
            connect(sockfd == 10, addr, addrlen == sizeof(struct sockaddr_in)) {
                const struct sockaddr_in* a;
                a = reinterpret_cast<const struct sockaddr_in*>(addr);
                CPPUNIT_ASSERT_EQUAL(AF_INET, a->sin_family);
                CPPUNIT_ASSERT_EQUAL(htons(8888), a->sin_port);
                CPPUNIT_ASSERT_EQUAL(0x10204080, a->sin_addr.s_addr);
                return 0;
            }
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TS ts;
        TcpTransport::sys = &ts;

        TcpTransport::ClientSocket s;
        s.init(IpAddress(ServiceLocator(
                "tcp: host=128.64.32.16, port=8888")));
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
        TcpTransport::sys = &ts;

        TcpTransport::ClientSocket s;
        try {
            s.init(IpAddress(ServiceLocator(
                    "tcp: host=128.64.32.16, port=8888")));
            CPPUNIT_ASSERT(false);
        } catch (UnrecoverableTransportException& e) {}
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
        TcpTransport::sys = &ts;

        TcpTransport::ClientSocket s;
        try {
            s.init(IpAddress(ServiceLocator(
                    "tcp: host=128.64.32.16, port=8888")));
            CPPUNIT_ASSERT(false);
        } catch (TransportException& e) {}
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
        TcpTransport::sys = &ts;

        TcpTransport::ClientSocket s;
        try {
            s.init(IpAddress(ServiceLocator(
                    "tcp: host=128.64.32.16, port=8888")));
            CPPUNIT_ASSERT(false);
        } catch (UnrecoverableTransportException& e) {}
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(SocketTest);

/**
 * Unit tests for TcpTransport, TcpServerRpc, and TcpClientRpc.
 */
class TcpTransportTest : public CppUnit::TestFixture {
    DISALLOW_COPY_AND_ASSIGN(TcpTransportTest); // NOLINT

    CPPUNIT_TEST_SUITE(TcpTransportTest);
    CPPUNIT_TEST(test_TcpServerRpc_sendReply);
    CPPUNIT_TEST(test_TcpClientRpc_getReply);
    CPPUNIT_TEST(test_TcpTransport_constructor);
    CPPUNIT_TEST(test_TcpTransport_serverRecv_normal);
    CPPUNIT_TEST(test_TcpTransport_serverRecv_error);
    CPPUNIT_TEST(test_TcpTransport_clientSend);
    CPPUNIT_TEST_SUITE_END();

  public:
    TcpTransportTest() {}

    void tearDown() {
        // disable mock socket instances
        TcpTransport::TcpServerRpc::mockServerSocket = NULL;
        TcpTransport::TcpClientRpc::mockClientSocket = NULL;

        // put TcpTransport::sys back to the real Syscalls implementation.
        extern TcpTransport::Syscalls realSyscalls;
        TcpTransport::sys = &realSyscalls;
    }

    void test_TcpServerRpc_sendReply() {
        static Buffer* send_expect;

        BEGIN_MOCK(TS, ServerSocketStub);
            send(payload == send_expect) {
            }
        END_MOCK();

        TS ts;
        TcpTransport::TcpServerRpc::mockServerSocket = &ts;

        BEGIN_MOCK(TSC, SyscallsStub);
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TSC tsc;
        TcpTransport::sys = &tsc;

        TcpTransport t;
        TcpTransport::TcpServerRpc* rpc = new TcpTransport::TcpServerRpc();
        send_expect = &rpc->replyPayload;
        rpc->realServerSocket.fd = 10;
        rpc->sendReply();
    }

    void test_TcpClientRpc_getReply() {
        static Buffer* recv_expect;

        BEGIN_MOCK(TS, ClientSocketStub);
            recv(payload == recv_expect) {
            }
        END_MOCK();

        TS ts;
        TcpTransport::TcpClientRpc::mockClientSocket = &ts;

        BEGIN_MOCK(TSC, SyscallsStub);
            close(fd == 10) {
                return 0;
            }
        END_MOCK();

        TSC tsc;
        TcpTransport::sys = &tsc;

        TcpTransport t;
        Buffer payload;
        recv_expect = &payload;
        TcpTransport::TcpClientRpc* rpc = new TcpTransport::TcpClientRpc();
        rpc->reply = &payload;
        rpc->realClientSocket.fd = 10;
        rpc->getReply();
    }

    void test_TcpTransport_constructor() {
        // the constructor doesn't add anything over ListenSocket(), so this is
        // just a placeholder
        TcpTransport t;
    }

    void test_TcpTransport_serverRecv_normal() {
        static TcpTransport::ListenSocket* init_expect;
        static Buffer* recv_expect;

        BEGIN_MOCK(TS, ServerSocketStub);
            init(listenSocket == init_expect) {
            }
            recv(payload) {
                recv_expect = payload;
            }
        END_MOCK();

        TS ts;
        TcpTransport::TcpServerRpc::mockServerSocket = &ts;

        TcpTransport t;
        init_expect = &t.listenSocket;
        Buffer payload;
        Transport::ServerRpc* rpc = t.serverRecv();
        CPPUNIT_ASSERT(recv_expect == &rpc->recvPayload);
        delete rpc;
    }

    void test_TcpTransport_serverRecv_error() {
        static TcpTransport::ListenSocket* init_expect;

        BEGIN_MOCK(TS, ServerSocketStub);
            init(listenSocket == init_expect) {
            }
            recv(payload) {
                throw TransportException();
            }
        END_MOCK();

        TS ts;
        TcpTransport::TcpServerRpc::mockServerSocket = &ts;

        TcpTransport t;
        init_expect = &t.listenSocket;
        Transport::ServerRpc* rpc = t.serverRecv();
        CPPUNIT_ASSERT(NULL == rpc);
    }

    void test_TcpTransport_clientSend() {
        static Buffer* send_expect;

        BEGIN_MOCK(TS, ClientSocketStub);
            init(address) {
                CPPUNIT_ASSERT_EQUAL("1.2.3.4:9999", address.toString());
            }
            send(payload == send_expect) {
            }
        END_MOCK();

        TS ts;
        TcpTransport::TcpClientRpc::mockClientSocket = &ts;

        TcpTransport t;
        Buffer payload;
        Buffer response;
        send_expect = &payload;
        ServiceLocator serviceLocator("tcp: host=1.2.3.4, port=9999");
        Transport::SessionRef session(t.getSession(serviceLocator));
        delete session->clientSend(&payload, &response);
    }

};
CPPUNIT_TEST_SUITE_REGISTRATION(TcpTransportTest);

} // namespace RAMCloud
