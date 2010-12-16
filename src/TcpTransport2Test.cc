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

#include "TestUtil.h"
#include "TcpTransport2.h"
#include "MockSyscall.h"
#include "ObjectTub.h"

namespace RAMCloud {

class TcpTransport2Test : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(TcpTransport2Test);
    CPPUNIT_TEST(test_sanityCheck);
    CPPUNIT_TEST(test_constructor_clientSideOnly);
    CPPUNIT_TEST(test_constructor_socketError);
    CPPUNIT_TEST(test_constructor_nonBlockError);
    CPPUNIT_TEST(test_constructor_reuseAddrError);
    CPPUNIT_TEST(test_constructor_bindError);
    CPPUNIT_TEST(test_constructor_listenError);
    CPPUNIT_TEST(test_destructor);
    CPPUNIT_TEST(test_tryAccept_noConnection);
    CPPUNIT_TEST(test_tryAccept_acceptFailure);
    CPPUNIT_TEST(test_tryAccept_success);
    CPPUNIT_TEST(test_tryServerRecv);
    CPPUNIT_TEST(test_tryServerRecv_unexpectedData);
    CPPUNIT_TEST(test_tryServerRecv_eof);
    CPPUNIT_TEST(test_tryServerRecv_error);
    CPPUNIT_TEST(test_sendMessage_multipleChunks);
    CPPUNIT_TEST(test_sendMessage_errorOnSend);
    CPPUNIT_TEST(test_sendMessage_brokenPipe);
    CPPUNIT_TEST(test_sendMessage_shortCount);
    CPPUNIT_TEST(test_recvCarefully_ioErrors);
    CPPUNIT_TEST(test_readMessage_receiveHeaderInPieces);
    CPPUNIT_TEST(test_readMessage_zeroLengthMessage);
    CPPUNIT_TEST(test_readMessage_receiveBodyInPieces);
    CPPUNIT_TEST(test_sessionConstructor_socketError);
    CPPUNIT_TEST(test_sessionConstructor_connectError);
    CPPUNIT_TEST(test_sessionDestructor);
    CPPUNIT_TEST(test_clientSend_sessionClosed);
    CPPUNIT_TEST(test_tryReadReply_eof);
    CPPUNIT_TEST(test_tryReadReply_eofOutsideRPC);
    CPPUNIT_TEST(test_tryReadReply_unexpectedDataFromServer);
    CPPUNIT_TEST(test_tryReadReply_ioError);
    CPPUNIT_TEST(test_wait_throwError);
    CPPUNIT_TEST_SUITE_END();

  public:

    ServiceLocator* locator;
    MockSyscall* sys;
    TestLog::Enable* logEnabler;

    TcpTransport2Test() : locator(NULL), sys(NULL), logEnabler(NULL)
    {}

    void setUp() {
        locator = new ServiceLocator("tcp+ip: host=localhost, port=11000");
        sys = new MockSyscall();
        TcpTransport2::sys = sys;
        logEnabler = new TestLog::Enable();
    }

    void tearDown() {
        delete locator;
        delete sys;
        delete logEnabler;
    }

    string catchConstruct(ServiceLocator* locator) {
        string message("no exception");
        try {
            TcpTransport2 server(locator);
        } catch (TransportException& e) {
            message = e.message;
        }
        return message;
    }

    // Open a connection to a server and return a file descriptor
    // for the socket.
    int connectToServer(const ServiceLocator& serviceLocator)
    {
        int fd = socket(PF_INET, SOCK_STREAM, 0);
        CPPUNIT_ASSERT(fd >= 0);
        IpAddress address(serviceLocator);
        int r = sys->connect(fd, &address.address, sizeof(address.address));
        CPPUNIT_ASSERT(r >= 0);
        return fd;
    }

    // Call serverRecv until there is an incoming RPC available.
    Transport::ServerRpc*
    waitRequest(Transport* transport) {
        Transport::ServerRpc* result;
        do {
            result = transport->serverRecv();
        } while (result == NULL);
        return result;
    }

    void test_sanityCheck() {
        // Create a server and a client and verify that we can
        // send a request, receive it, send a reply, and receive it.
        // Then try a second request.

        TcpTransport2 server(locator);
        TcpTransport2 client;
        Transport::SessionRef session = client.getSession(*locator);

        Buffer request;
        Buffer reply;
        request.fillFromString("abcdefg");
        Transport::ClientRpc* clientRpc = session->clientSend(&request,
                &reply);
        Transport::ServerRpc* serverRpc = waitRequest(&server);
        CPPUNIT_ASSERT_EQUAL("abcdefg/0", toString(&serverRpc->recvPayload));
        CPPUNIT_ASSERT_EQUAL(false, clientRpc->isReady());
        serverRpc->replyPayload.fillFromString("klmn");
        serverRpc->sendReply();
        event_loop(EVLOOP_ONCE);
        CPPUNIT_ASSERT_EQUAL(true, clientRpc->isReady());
        CPPUNIT_ASSERT_EQUAL("klmn/0", toString(&reply));

        request.fillFromString("request2");
        reply.reset();
        clientRpc = session->clientSend(&request, &reply);
        serverRpc = waitRequest(&server);
        CPPUNIT_ASSERT_EQUAL("request2/0", toString(&serverRpc->recvPayload));
        serverRpc->replyPayload.fillFromString("reply2");
        serverRpc->sendReply();
        clientRpc->wait();
        CPPUNIT_ASSERT_EQUAL("reply2/0", toString(&reply));
    }

    void test_constructor_clientSideOnly() {
        sys->socketErrno = EPERM;
        TcpTransport2 client;
    }

    void test_constructor_socketError() {
        sys->socketErrno = EPERM;
        CPPUNIT_ASSERT_EQUAL("TcpTransport couldn't create listen socket: "
            "Operation not permitted", catchConstruct(locator));
    }

    void test_constructor_nonBlockError() {
        sys->fcntlErrno = EPERM;
        CPPUNIT_ASSERT_EQUAL("TcpTransport couldn't set nonblocking on "
            "listen socket: Operation not permitted",
            catchConstruct(locator));
    }

    void test_constructor_reuseAddrError() {
        sys->setsockoptErrno = EPERM;
        CPPUNIT_ASSERT_EQUAL("TcpTransport couldn't set SO_REUSEADDR "
            "on listen socket: Operation not permitted",
            catchConstruct(locator));
    }

    void test_constructor_bindError() {
        sys->bindErrno = EPERM;
        CPPUNIT_ASSERT_EQUAL("TcpTransport couldn't bind to 'tcp+ip: "
            "host=localhost, port=11000': Operation not permitted",
            catchConstruct(locator));
    }

    void test_constructor_listenError() {
        sys->listenErrno = EPERM;
        CPPUNIT_ASSERT_EQUAL("TcpTransport couldn't listen on socket: "
            "Operation not permitted", catchConstruct(locator));
    }

    void test_destructor() {
        // Connect 2 clients to 1 server, then delete them all and make
        // sure that all of the sockets get closed.
        TcpTransport2* server = new TcpTransport2(locator);
        TcpTransport2* client = new TcpTransport2();
        Transport::SessionRef session1 = client->getSession(*locator);
        Transport::SessionRef session2 = client->getSession(*locator);

        Buffer request1, request2;
        Buffer reply1, reply2;
        request1.fillFromString("request1");
        request2.fillFromString("request2");
        Transport::ClientRpc* clientRpc1 = session1->clientSend(&request1,
                &reply1);
        Transport::ClientRpc* clientRpc2 = session2->clientSend(&request2,
                &reply2);
        Transport::ServerRpc* serverRpc1 = waitRequest(server);
        Transport::ServerRpc* serverRpc2 = waitRequest(server);
        CPPUNIT_ASSERT_EQUAL("request1/0", toString(&serverRpc1->recvPayload));
        CPPUNIT_ASSERT_EQUAL("request2/0", toString(&serverRpc2->recvPayload));
        serverRpc1->replyPayload.fillFromString("reply1");
        serverRpc1->sendReply();
        serverRpc2->replyPayload.fillFromString("reply2");
        serverRpc2->sendReply();
        clientRpc1->wait();
        clientRpc2->wait();
        CPPUNIT_ASSERT_EQUAL("reply1/0", toString(&reply1));
        CPPUNIT_ASSERT_EQUAL("reply2/0", toString(&reply2));

        sys->closeCount = 0;
        delete server;
        CPPUNIT_ASSERT_EQUAL(3, sys->closeCount);
        delete client;
        CPPUNIT_ASSERT_EQUAL(3, sys->closeCount);
        session1 = NULL;
        session2 = NULL;
        CPPUNIT_ASSERT_EQUAL(5, sys->closeCount);
    }

    void test_tryAccept_noConnection() {
        TcpTransport2 server(locator);
        TcpTransport2::tryAccept(-1, 0, &server);
        CPPUNIT_ASSERT_EQUAL(0, server.sockets.size());
    }

    void test_tryAccept_acceptFailure() {
        TcpTransport2 server(locator);
        sys->acceptErrno = EPERM;
        TcpTransport2::tryAccept(-1, 0, &server);
        CPPUNIT_ASSERT_EQUAL("tryAccept: error in TcpTransport "
                "accepting connection for 'tcp+ip: host=localhost, "
                "port=11000': Operation not permitted", TestLog::get());
        CPPUNIT_ASSERT_EQUAL(-1, server.listenSocket);
        CPPUNIT_ASSERT_EQUAL(1, sys->closeCount);
    }

    void test_tryAccept_success() {
        TcpTransport2 server(locator);
        int fd = connectToServer(*locator);
        TcpTransport2::tryAccept(-1, 0, &server);
        if (server.sockets.size() == 0) {
            CPPUNIT_FAIL("socket vector doesn't have enough space");
        }
        if (server.sockets[server.sockets.size() - 1] == NULL) {
            CPPUNIT_FAIL("no socket allocated in server transport");
        }
        close(fd);
    }

    void test_tryServerRecv() {
        TcpTransport2 server(locator);
        int fd = connectToServer(*locator);
        TcpTransport2::tryAccept(-1, 0, &server);
        if (server.sockets.size() == 0) {
            CPPUNIT_FAIL("no socket allocated in server transport");
        }
        int serverFd = server.sockets.size() - 1;

        // Send a message in 2 chunks.
        TcpTransport2::Header header;
        header.len = 6;
        CPPUNIT_ASSERT_EQUAL(sizeof(header),
            write(fd, &header, sizeof(header)));
        TcpTransport2::tryServerRecv(serverFd, 0, &server);
        if (server.sockets[serverFd]->rpc == 0) {
            CPPUNIT_FAIL("no rpc object allocated");
        }
        CPPUNIT_ASSERT_EQUAL(0, server.waitingRequests.size());

        CPPUNIT_ASSERT_EQUAL(6, write(fd, "abcdef", 6));
        TcpTransport2::tryServerRecv(serverFd, 0, &server);
        CPPUNIT_ASSERT_EQUAL(1, server.waitingRequests.size());

        close(fd);
    }

    void test_tryServerRecv_unexpectedData() {
        TcpTransport2 server(locator);
        int fd = connectToServer(*locator);
        TcpTransport2::tryAccept(-1, 0, &server);
        if (server.sockets.size() == 0) {
            CPPUNIT_FAIL("no socket allocated in server transport");
        }
        int serverFd = server.sockets.size() - 1;

        // Send a message to make the server busy.
        TcpTransport2::Header header;
        header.len = 0;
        write(fd, &header, sizeof(header));
        TcpTransport2::tryServerRecv(serverFd, 0, &server);
        CPPUNIT_ASSERT_EQUAL(true, server.sockets[serverFd]->busy);

        // Send more junk to the server.
        write(fd, "abcdef", 6);
        TcpTransport2::tryServerRecv(serverFd, 0, &server);
        CPPUNIT_ASSERT_EQUAL("tryServerRecv: TcpTransport discarding "
                "6 unexpected bytes from client", TestLog::get());

        close(fd);
    }

    void test_tryServerRecv_eof() {
        TcpTransport2 server(locator);
        int fd = connectToServer(*locator);
        TcpTransport2::tryAccept(-1, 0, &server);
        int serverFd = server.sockets.size() - 1;
        close(fd);
        TcpTransport2::tryServerRecv(serverFd, 0, &server);
        CPPUNIT_ASSERT_EQUAL(NULL, server.sockets[serverFd]);
    }

    void test_tryServerRecv_error() {
        TcpTransport2 server(locator);
        int fd = connectToServer(*locator);
        TcpTransport2::tryAccept(-1, 0, &server);
        int serverFd = server.sockets.size() - 1;
        sys->recvErrno = EPERM;
        TcpTransport2::tryServerRecv(serverFd, 0, &server);
        CPPUNIT_ASSERT_EQUAL(NULL, server.sockets[serverFd]);
        CPPUNIT_ASSERT_EQUAL("tryServerRecv: TcpTransport closing client "
                "connection: I/O read error in TcpTransport: Operation "
                "not permitted", TestLog::get());

        close(fd);
    }

    void test_sendMessage_multipleChunks() {
        TcpTransport2 server(locator);
        int fd = connectToServer(*locator);
        Buffer payload;
        Buffer::Chunk::appendToBuffer(&payload, "abcde", 5);
        Buffer::Chunk::appendToBuffer(&payload, "xxx", 3);
        Buffer::Chunk::appendToBuffer(&payload, "12345678", 8);
        TcpTransport2::sendMessage(fd, payload);

        Transport::ServerRpc* serverRpc = waitRequest(&server);
        CPPUNIT_ASSERT_EQUAL("abcdexxx12345678",
                toString(&serverRpc->recvPayload));

        close(fd);
    }

    void test_sendMessage_errorOnSend() {
        TcpTransport2 server(locator);
        int fd = connectToServer(*locator);
        Buffer payload;
        Buffer::Chunk::appendToBuffer(&payload, "test message", 5);

        sys->sendmsgErrno = EPERM;
        string message("no exception");
        try {
            TcpTransport2::sendMessage(fd, payload);
        } catch (TransportException& e) {
            message = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("I/O error in TcpTransport::sendMessage: "
                "Operation not permitted", message);

        close(fd);
    }

    void test_sendMessage_brokenPipe() {
        // The main reason for this test is to make sure that
        // broken pipe errors don't generate signals that kill
        // the process.
        TcpTransport2 server(locator);
        TcpTransport2 client;
        Transport::SessionRef session = client.getSession(*locator);
        event_loop(EVLOOP_ONCE|EVLOOP_NONBLOCK);
        int serverFd = server.sockets.size() - 1;
        server.closeSocket(serverFd);
        string message("no exception");
        try {
            Buffer request;
            Buffer::Chunk::appendToBuffer(&request, "message chunk", 13);
            TcpTransport2::TcpSession* rawSession =
                    reinterpret_cast<TcpTransport2::TcpSession*>(session.get());
            for (int i = 0; i < 1000; i++) {
                TcpTransport2::sendMessage(rawSession->fd, request);
            }
        } catch (TransportException& e) {
            message = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("I/O error in TcpTransport::sendMessage: "
                "Broken pipe", message);
    }

    void test_sendMessage_shortCount() {
        TcpTransport2 server(locator);
        int fd = connectToServer(*locator);
        Buffer payload;
        Buffer::Chunk::appendToBuffer(&payload, "test message", 5);

        sys->sendmsgReturnCount = 3;
        string message("no exception");
        try {
            TcpTransport2::sendMessage(fd, payload);
        } catch (TransportException& e) {
            message = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("Incomplete sendmsg in "
                "TcpTransport::sendMessage: 3 bytes sent out of 9",
                message);

        close(fd);
    }

    void test_recvCarefully_ioErrors() {
        string message("no exception");
        sys->recvEof = true;
        try {
            TcpTransport2::recvCarefully(2, NULL, 100);
        } catch (TcpTransport2::TcpTransportEof& e) {
            message = "eof";
        }
        CPPUNIT_ASSERT_EQUAL("eof", message);
        sys->recvEof = false;
        sys->recvErrno = EAGAIN;
        CPPUNIT_ASSERT_EQUAL(0, TcpTransport2::recvCarefully(2, NULL, 100));
        sys->recvErrno = EPERM;
        message = "no exception";
        try {
            TcpTransport2::recvCarefully(2, NULL, 100);
        } catch (TransportException& e) {
            message = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("I/O read error in TcpTransport: "
                "Operation not permitted", message);
    }

    void test_readMessage_receiveHeaderInPieces() {
        TcpTransport2 server(locator);
        int fd = connectToServer(*locator);
        TcpTransport2::tryAccept(-1, 0, &server);
        int serverFd = server.sockets.size() - 1;

        // Try to receive when there is no data at all.
        Buffer buffer;
        TcpTransport2::IncomingMessage incoming(&buffer);
        CPPUNIT_ASSERT_EQUAL(false, incoming.readMessage(serverFd));
        CPPUNIT_ASSERT_EQUAL(0, incoming.headerBytesReceived);

        // Send first part of header.
        TcpTransport2::Header header;
        header.len = 123456789;
        write(fd, &header, 3);
        CPPUNIT_ASSERT_EQUAL(false, incoming.readMessage(serverFd));
        CPPUNIT_ASSERT_EQUAL(3, incoming.headerBytesReceived);

        // Send second part of header (oversize counter will generate
        // exception).
        write(fd, reinterpret_cast<char*>(&header)+3, sizeof(header)-3);
        string message("no exception");
        try {
            incoming.readMessage(serverFd);
        } catch (TransportException& e) {
            message = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("TcpTransport received oversize message "
                "(123456789 bytes)", message);

        close(fd);
    }

    void test_readMessage_zeroLengthMessage() {
        TcpTransport2 server(locator);
        int fd = connectToServer(*locator);
        TcpTransport2::tryAccept(-1, 0, &server);
        int serverFd = server.sockets.size() - 1;
        Buffer buffer;
        TcpTransport2::IncomingMessage incoming(&buffer);
        TcpTransport2::Header header;
        header.len = 0;
        write(fd, &header, sizeof(header));
        CPPUNIT_ASSERT_EQUAL(true, incoming.readMessage(serverFd));
        CPPUNIT_ASSERT_EQUAL("", toString(&buffer));

        close(fd);
    }

    void test_readMessage_receiveBodyInPieces() {
        TcpTransport2 server(locator);
        int fd = connectToServer(*locator);
        TcpTransport2::tryAccept(-1, 0, &server);
        int serverFd = server.sockets.size() - 1;
        Buffer buffer;
        TcpTransport2::IncomingMessage incoming(&buffer);
        TcpTransport2::Header header;
        header.len = 11;
        write(fd, &header, sizeof(header));

        // First attempt: header present but no body bytes.
        CPPUNIT_ASSERT_EQUAL(false, incoming.readMessage(serverFd));
        CPPUNIT_ASSERT_EQUAL(0, incoming.messageBytesReceived);

        // Second attempt: part of body present.
        write(fd, "abcde", 5);
        CPPUNIT_ASSERT_EQUAL(false, incoming.readMessage(serverFd));
        CPPUNIT_ASSERT_EQUAL(5, incoming.messageBytesReceived);

        // Third attempt: remainder of body present, plus extra bytes
        // (don't read extras).
        write(fd, "0123456789", 10);
        CPPUNIT_ASSERT_EQUAL(true, incoming.readMessage(serverFd));
        CPPUNIT_ASSERT_EQUAL("abcde012345", toString(&buffer));

        close(fd);
    }

    void test_sessionConstructor_socketError() {
        sys->socketErrno = EPERM;
        string message("");
        try {
            TcpTransport2::TcpSession session(*locator);
        } catch (TransportException& e) {
            message = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("TcpTransport couldn't open socket "
                "for session: Operation not permitted", message);
    }

    void test_sessionConstructor_connectError() {
        sys->connectErrno = EPERM;
        string message("no exception");
        try {
            TcpTransport2::TcpSession session(*locator);
        } catch (TransportException& e) {
            message = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("Session connect error in TcpTransport: "
                "Operation not permitted", message);
        CPPUNIT_ASSERT_EQUAL(1, sys->closeCount);
    }

    void test_sessionDestructor() {
        TcpTransport2 server(locator);
        TcpTransport2 client;
        Transport::SessionRef session = client.getSession(*locator);
        session = NULL;
        CPPUNIT_ASSERT_EQUAL(1, sys->closeCount);
    }

    void test_clientSend_sessionClosed() {
        TcpTransport2 server(locator);
        TcpTransport2 client;
        Transport::SessionRef session = client.getSession(*locator);
        TcpTransport2::TcpSession* rawSession =
                reinterpret_cast<TcpTransport2::TcpSession*>(session.get());
        rawSession->close();
        rawSession->errorInfo = "session closed";
        string message("no exception");
        try {
            Buffer request, reply;
            delete session->clientSend(&request, &reply);
        } catch (TransportException& e) {
            message = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("session closed", message);
    }

    void test_tryReadReply_eof() {
        // In this test, arrange for the connection to get closed
        // while an RPC is outstanding and we are waiting for a response.
        TcpTransport2 server(locator);
        TcpTransport2 client;
        Transport::SessionRef session = client.getSession(*locator);
        Buffer request;
        Buffer reply;
        Buffer::Chunk::appendToBuffer(&request, "xxx", 3);
        Transport::ClientRpc* clientRpc = session->clientSend(&request,
                &reply);
        // The following line serves only to avoid an "unused result"
        // warning for the line above.
        clientRpc->isReady();
        TcpTransport2::TcpSession* rawSession =
                reinterpret_cast<TcpTransport2::TcpSession*>(session.get());
        sys->recvEof = true;
        TcpTransport2::TcpSession::tryReadReply(rawSession->fd, 0,
                rawSession);
        CPPUNIT_ASSERT_EQUAL(-1, rawSession->fd);
        CPPUNIT_ASSERT_EQUAL("socket closed by server", rawSession->errorInfo);
    }

    void test_tryReadReply_eofOutsideRPC() {
        // In this test, close the connection when there is no RPC
        // outstanding; this creates additional stress because not all
        // data structures have been initialized.
        TcpTransport2 server(locator);
        TcpTransport2 client;
        Transport::SessionRef session = client.getSession(*locator);
        TcpTransport2::tryAccept(-1, 0, &server);
        server.closeSocket(server.sockets.size() - 1);
        TcpTransport2::TcpSession* rawSession =
                reinterpret_cast<TcpTransport2::TcpSession*>(session.get());
        TcpTransport2::TcpSession::tryReadReply(rawSession->fd, 0,
                rawSession);
        CPPUNIT_ASSERT_EQUAL(-1, rawSession->fd);
        CPPUNIT_ASSERT_EQUAL("socket closed by server", rawSession->errorInfo);
    }

    void test_tryReadReply_unexpectedDataFromServer() {
        TcpTransport2 server(locator);
        TcpTransport2 client;
        Transport::SessionRef session = client.getSession(*locator);
        TcpTransport2::tryAccept(-1, 0, &server);
        write(server.sockets.size() - 1, "abcdef", 6);
        TcpTransport2::TcpSession* rawSession =
                reinterpret_cast<TcpTransport2::TcpSession*>(session.get());
        TcpTransport2::TcpSession::tryReadReply(rawSession->fd, 0,
                rawSession);
        CPPUNIT_ASSERT_EQUAL("tryReadReply: TcpTransport discarding 6 "
                "unexpected bytes from server 127.0.0.1:11000", TestLog::get());
    }

    void test_tryReadReply_ioError() {
        TcpTransport2 server(locator);
        TcpTransport2 client;
        Transport::SessionRef session = client.getSession(*locator);
        Buffer request;
        Buffer reply;
        Buffer::Chunk::appendToBuffer(&request, "xxx", 3);
        Transport::ClientRpc* clientRpc = session->clientSend(&request,
                &reply);
        // The following line serves only to avoid an "unused result"
        // warning for the line above.
        clientRpc->isReady();
        TcpTransport2::TcpSession* rawSession =
                reinterpret_cast<TcpTransport2::TcpSession*>(session.get());
        sys->recvErrno = EPERM;
        TcpTransport2::TcpSession::tryReadReply(rawSession->fd, 0,
                rawSession);
        CPPUNIT_ASSERT_EQUAL(-1, rawSession->fd);
        CPPUNIT_ASSERT_EQUAL("tryReadReply: TcpTransport closing session "
                "socket: I/O read error in TcpTransport: Operation not "
                "permitted", TestLog::get());
        CPPUNIT_ASSERT_EQUAL("I/O read error in TcpTransport: Operation "
                "not permitted", rawSession->errorInfo);
    }

    void test_wait_throwError() {
        TcpTransport2 server(locator);
        TcpTransport2 client;
        Transport::SessionRef session = client.getSession(*locator);
        Buffer request, reply;
        Transport::ClientRpc* clientRpc = session->clientSend(&request,
                &reply);
        TcpTransport2::TcpSession* rawSession =
                reinterpret_cast<TcpTransport2::TcpSession*>(session.get());
        rawSession->close();
        rawSession->errorInfo = "error message";
        string message("no exception");
        try {
            clientRpc->wait();
        } catch (TransportException& e) {
            message = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("error message", message);
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(TcpTransport2Test);
};
CPPUNIT_TEST_SUITE_REGISTRATION(TcpTransport2Test);

}  // namespace RAMCloud
