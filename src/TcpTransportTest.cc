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
#include "MockSyscall.h"
#include "ServiceManager.h"
#include "TcpTransport.h"
#include "Tub.h"

namespace RAMCloud {

class TcpTransportTest : public ::testing::Test {
  public:
    ServiceLocator* locator;
    MockSyscall* sys;
    Syscall* savedSyscall;
    TestLog::Enable logEnabler;

    TcpTransportTest()
            : locator(NULL), sys(NULL), savedSyscall(NULL), logEnabler(NULL)
    {
        locator = new ServiceLocator("tcp+ip: host=localhost, port=11000");
        sys = new MockSyscall();
        savedSyscall = TcpTransport::sys;
        TcpTransport::sys = sys;
        delete serviceManager;
        ServiceManager::init();
    }

    ~TcpTransportTest() {
        delete locator;
        delete sys;
        TcpTransport::sys = savedSyscall;
    }

    string catchConstruct(ServiceLocator* locator) {
        string message("no exception");
        try {
            TcpTransport server(locator);
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
        EXPECT_GE(fd, 0);
        IpAddress address(serviceLocator);
        int r = sys->connect(fd, &address.address, sizeof(address.address));
        EXPECT_GE(r, 0);
        return fd;
    }

    // Return a count of the number of complete RPC requests waiting
    // for service (also, discard all of these requests).
    int countWaitingRequests()
    {
        int result = 0;
        while (serviceManager->waitForRpc(0.0) != NULL)
            result++;
        return result;
    }

    // Run the dispatcher until a server-side session has been opened
    // (but give up if it takes too long).  The return value is true
    // if a session has been opened.
    bool waitForSession(TcpTransport& transport)
    {
        for (int i = 0; i < 1000; i++) {
            dispatch->poll();
            if (transport.sockets.size() > 0)
                return true;
            usleep(1000);
        }
        return false;
    }
    DISALLOW_COPY_AND_ASSIGN(TcpTransportTest);
};

TEST_F(TcpTransportTest, sanityCheck) {
    TcpTransport server(locator);
    TcpTransport client;
    Transport::SessionRef session = client.getSession(*locator);

    // Send two requests from the client.
    Buffer request1;
    Buffer reply1;
    request1.fillFromString("request1");
    Transport::ClientRpc* clientRpc1 = session->clientSend(&request1,
            &reply1);
    Buffer request2;
    Buffer reply2;
    request2.fillFromString("request2");
    Transport::ClientRpc* clientRpc2 = session->clientSend(&request2,
            &reply2);

    // Receive the two requests on the server.
    Transport::ServerRpc* serverRpc1 = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc1 != NULL);
    EXPECT_EQ("request1/0", toString(&serverRpc1->requestPayload));
    Transport::ServerRpc* serverRpc2 = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc2 != NULL);
    EXPECT_EQ("request2/0", toString(&serverRpc2->requestPayload));

    // Reply to the requests in backwards order.
    serverRpc2->replyPayload.fillFromString("response2");
    serverRpc2->sendReply();
    serverRpc1->replyPayload.fillFromString("response1");
    serverRpc1->sendReply();

    // Receive the responses in the client.
    EXPECT_FALSE(clientRpc1->isReady());
    EXPECT_FALSE(clientRpc2->isReady());
    EXPECT_TRUE(waitForRpc(*clientRpc1));
    EXPECT_EQ("response1/0", toString(&reply1));
    EXPECT_TRUE(clientRpc2->isReady());
    EXPECT_EQ("response2/0", toString(&reply2));
}

TEST_F(TcpTransportTest, constructor_clientSideOnly) {
    sys->socketErrno = EPERM;
    TcpTransport client;
}

TEST_F(TcpTransportTest, constructor_socketError) {
    sys->socketErrno = EPERM;
    EXPECT_EQ("TcpTransport couldn't create listen socket: "
        "Operation not permitted", catchConstruct(locator));
}

TEST_F(TcpTransportTest, constructor_nonBlockError) {
    sys->fcntlErrno = EPERM;
    EXPECT_EQ("TcpTransport couldn't set nonblocking on "
        "listen socket: Operation not permitted",
        catchConstruct(locator));
}

TEST_F(TcpTransportTest, constructor_reuseAddrError) {
    sys->setsockoptErrno = EPERM;
    EXPECT_EQ("TcpTransport couldn't set SO_REUSEADDR "
        "on listen socket: Operation not permitted",
        catchConstruct(locator));
}

TEST_F(TcpTransportTest, constructor_bindError) {
    sys->bindErrno = EPERM;
    EXPECT_EQ("TcpTransport couldn't bind to 'tcp+ip: "
        "host=localhost, port=11000': Operation not permitted",
        catchConstruct(locator));
}

TEST_F(TcpTransportTest, constructor_listenError) {
    sys->listenErrno = EPERM;
    EXPECT_EQ("TcpTransport couldn't listen on socket: "
        "Operation not permitted", catchConstruct(locator));
}

TEST_F(TcpTransportTest, destructor) {
    // Connect 2 clients to 1 server, then delete them all and make
    // sure that all of the sockets get closed.
    TcpTransport* server = new TcpTransport(locator);
    TcpTransport* client = new TcpTransport();
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
    Transport::ServerRpc* serverRpc1 = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc1 != NULL);
    Transport::ServerRpc* serverRpc2 = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc2 != NULL);
    EXPECT_EQ("request1/0", toString(&serverRpc1->requestPayload));
    EXPECT_EQ("request2/0", toString(&serverRpc2->requestPayload));
    serverRpc1->replyPayload.fillFromString("reply1");
    serverRpc1->sendReply();
    serverRpc2->replyPayload.fillFromString("reply2");
    serverRpc2->sendReply();
    clientRpc1->wait();
    clientRpc2->wait();
    EXPECT_EQ("reply1/0", toString(&reply1));
    EXPECT_EQ("reply2/0", toString(&reply2));

    sys->closeCount = 0;
    delete server;
    EXPECT_EQ(3, sys->closeCount);
    delete client;
    EXPECT_EQ(3, sys->closeCount);
    session1 = NULL;
    session2 = NULL;
    EXPECT_EQ(5, sys->closeCount);
}

TEST_F(TcpTransportTest, Socket_destructor) {
    // Send a partial message to a server, then close its socket and
    // ensure that the TcpServerRpc was deleted.
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    server.acceptHandler->handleFileEvent();
    EXPECT_NE(server.sockets.size(), 0U);
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;

    TcpTransport::Header header;
    header.len = 6;
    EXPECT_EQ(static_cast<int>(sizeof(header)),
        write(fd, &header, sizeof(header)));
    server.sockets[serverFd]->readHandler.handleFileEvent();
    EXPECT_TRUE(server.sockets[serverFd]->rpc != NULL);
    server.closeSocket(serverFd);
    EXPECT_EQ("~TcpServerRpc: deleted", TestLog::get());
}

TEST_F(TcpTransportTest, AcceptHandler_handleFileEvent_noConnection) {
    TcpTransport server(locator);
    server.acceptHandler->handleFileEvent();
    EXPECT_EQ(0U, server.sockets.size());
}

TEST_F(TcpTransportTest, AcceptHandler_handleFileEvent_acceptFailure) {
    TcpTransport server(locator);
    sys->acceptErrno = EPERM;
    server.acceptHandler->handleFileEvent();
    EXPECT_EQ("handleFileEvent: error in TcpTransport::AcceptHandler "
            "accepting connection for 'tcp+ip: host=localhost, "
            "port=11000': Operation not permitted", TestLog::get());
    EXPECT_EQ(-1, server.listenSocket);
    EXPECT_EQ(1, sys->closeCount);
}

TEST_F(TcpTransportTest, AcceptHandler_handleFileEvent_success) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    server.acceptHandler->handleFileEvent();
    EXPECT_NE(server.sockets.size(), 0U);
    EXPECT_FALSE(server.sockets[server.sockets.size() - 1] == NULL);
    close(fd);
}

TEST_F(TcpTransportTest, RequestReadHandler_handleFileEvent) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    server.acceptHandler->handleFileEvent();
    EXPECT_NE(server.sockets.size(), 0U);
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;

    // Send a message in 2 chunks.
    TcpTransport::Header header;
    header.len = 6;
    EXPECT_EQ(static_cast<int>(sizeof(header)),
        write(fd, &header, sizeof(header)));
    server.sockets[serverFd]->readHandler.handleFileEvent();
    EXPECT_TRUE(server.sockets[serverFd]->rpc != NULL);
    EXPECT_EQ(0, countWaitingRequests());

    EXPECT_EQ(6, write(fd, "abcdef", 6));
    server.sockets[serverFd]->readHandler.handleFileEvent();
    EXPECT_EQ(1, countWaitingRequests());

    close(fd);
}

TEST_F(TcpTransportTest, RequestReadHandler_handleFileEvent_eof) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    server.acceptHandler->handleFileEvent();
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;
    close(fd);
    server.sockets[serverFd]->readHandler.handleFileEvent();
    EXPECT_TRUE(server.sockets[serverFd] == NULL);
}

TEST_F(TcpTransportTest, RequestReadHandler_handleFileEvent_error) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    server.acceptHandler->handleFileEvent();
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;
    sys->recvErrno = EPERM;
    server.sockets[serverFd]->readHandler.handleFileEvent();
    EXPECT_TRUE(server.sockets[serverFd] == NULL);
    EXPECT_EQ("handleFileEvent: TcpTransport::RequestReadHandler "
            "closing client connection: I/O read error in TcpTransport: "
            "Operation not permitted | ~TcpServerRpc: deleted",
            TestLog::get());

    close(fd);
}

TEST_F(TcpTransportTest, sendMessage_multipleChunks) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    Buffer payload;
    Buffer::Chunk::appendToBuffer(&payload, "abcde", 5);
    Buffer::Chunk::appendToBuffer(&payload, "xxx", 3);
    Buffer::Chunk::appendToBuffer(&payload, "12345678", 8);
    TcpTransport::sendMessage(fd, 111, payload);

    Transport::ServerRpc* serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("abcdexxx12345678",
            toString(&serverRpc->requestPayload));

    close(fd);
}

TEST_F(TcpTransportTest, sendMessage_errorOnSend) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    Buffer payload;
    Buffer::Chunk::appendToBuffer(&payload, "test message", 5);

    sys->sendmsgErrno = EPERM;
    string message("no exception");
    try {
        TcpTransport::sendMessage(fd, 111, payload);
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("I/O error in TcpTransport::sendMessage: "
            "Operation not permitted", message);

    close(fd);
}

TEST_F(TcpTransportTest, sendMessage_largeBuffer) {
    TcpTransport server(locator);
    TcpTransport client;
    Transport::SessionRef session = client.getSession(*locator);
    Buffer request;
    Buffer reply;
    fillLargeBuffer(&request, 2000000);
    TcpTransport::messageChunks = 0;
    Transport::ClientRpc* clientRpc = session->clientSend(&request,
            &reply);
    Transport::ServerRpc* serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_GT(TcpTransport::messageChunks, 0);
    EXPECT_EQ("ok", checkLargeBuffer(&serverRpc->requestPayload, 2000000));
    fillLargeBuffer(&serverRpc->replyPayload, 1500000);
    TcpTransport::messageChunks = 0;
    serverRpc->sendReply();
    EXPECT_TRUE(waitForRpc(*clientRpc));
    EXPECT_EQ("ok", checkLargeBuffer(&reply, 1500000));
    EXPECT_GT(TcpTransport::messageChunks, 0);
}

TEST_F(TcpTransportTest, sendMessage_brokenPipe) {
    // The main reason for this test is to make sure that
    // broken pipe errors don't generate signals that kill
    // the process.
    TcpTransport server(locator);
    TcpTransport client;
    Transport::SessionRef session = client.getSession(*locator);
    ASSERT_TRUE(waitForSession(server));
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;
    server.closeSocket(serverFd);
    string message("no exception");
    try {
        Buffer request;
        Buffer::Chunk::appendToBuffer(&request, "message chunk", 13);
        TcpTransport::TcpSession* rawSession =
                reinterpret_cast<TcpTransport::TcpSession*>(session.get());
        for (int i = 0; i < 1000; i++) {
            TcpTransport::sendMessage(rawSession->fd, 111, request);
        }
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("I/O error in TcpTransport::sendMessage: "
            "Broken pipe", message);
}

TEST_F(TcpTransportTest, recvCarefully_ioErrors) {
    string message("no exception");
    sys->recvEof = true;
    try {
        TcpTransport::recvCarefully(2, NULL, 100);
    } catch (TcpTransport::TcpTransportEof& e) {
        message = "eof";
    }
    EXPECT_EQ("eof", message);
    sys->recvEof = false;
    sys->recvErrno = EAGAIN;
    EXPECT_EQ(0, TcpTransport::recvCarefully(2, NULL, 100));
    sys->recvErrno = EPERM;
    message = "no exception";
    try {
        TcpTransport::recvCarefully(2, NULL, 100);
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("I/O read error in TcpTransport: "
            "Operation not permitted", message);
}

TEST_F(TcpTransportTest, readMessage_receiveHeaderInPieces) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    server.acceptHandler->handleFileEvent();
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;

    // Try to receive when there is no data at all.
    Buffer buffer;
    TcpTransport::IncomingMessage incoming(&buffer, NULL);
    EXPECT_FALSE(incoming.readMessage(serverFd));
    EXPECT_EQ(0U, incoming.headerBytesReceived);

    // Send first part of header.
    TcpTransport::Header header;
    header.len = 240;
    write(fd, &header, 3);
    EXPECT_FALSE(incoming.readMessage(serverFd));
    EXPECT_EQ(3U, incoming.headerBytesReceived);

    // Send second part of header.
    write(fd, reinterpret_cast<char*>(&header)+3, sizeof(header)-3);
    EXPECT_FALSE(incoming.readMessage(serverFd));
    EXPECT_EQ(12U, incoming.headerBytesReceived);
    EXPECT_EQ(240U, incoming.messageLength);

    close(fd);
}

TEST_F(TcpTransportTest, readMessage_messageTooLong) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    server.acceptHandler->handleFileEvent();
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;
    Buffer buffer;
    TcpTransport::IncomingMessage incoming(&buffer, NULL);
    TcpTransport::Header header;
    header.len = 999999999;
    write(fd, &header, sizeof(header));
    EXPECT_FALSE(incoming.readMessage(serverFd));
    EXPECT_EQ("readMessage: TcpTransport received oversize message "
            "(999999999 bytes); discarding extra bytes",
            TestLog::get());
    EXPECT_EQ(0x1000000U, incoming.messageLength);

    close(fd);
}

TEST_F(TcpTransportTest, readMessage_getBufferFromSession) {
    // This test is a bit goofy, and that we set up a server, then
    // initialize the IncomingMessage to receive a client-side reply.
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    server.acceptHandler->handleFileEvent();
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;
    Buffer request, reply;
    TcpTransport::TcpSession session;
    TcpTransport::TcpClientRpc rpc(&session, &request, &reply, 66UL);
    session.outstandingRpcs.push_back(rpc);
    TcpTransport::IncomingMessage incoming(NULL, &session);
    TcpTransport::Header header;
    header.nonce = 66UL;
    header.len = 5;
    write(fd, &header, sizeof(header));
    write(fd, "abcde", 5);
    EXPECT_TRUE(incoming.readMessage(serverFd));
    EXPECT_EQ("abcde", toString(&reply));
    session.close();

    close(fd);
}

TEST_F(TcpTransportTest, readMessage_findRpcReturnsNull) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    server.acceptHandler->handleFileEvent();
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;
    TcpTransport::TcpSession session;
    TcpTransport::IncomingMessage incoming(NULL, &session);
    TcpTransport::Header header;
    header.nonce = 66UL;
    header.len = 5;
    write(fd, &header, sizeof(header));
    EXPECT_FALSE(incoming.readMessage(serverFd));
    EXPECT_EQ(0U, incoming.messageLength);
    close(fd);
}

TEST_F(TcpTransportTest, readMessage_receiveBodyInPieces) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    server.acceptHandler->handleFileEvent();
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;
    Buffer buffer;
    TcpTransport::IncomingMessage incoming(&buffer, NULL);
    TcpTransport::Header header;
    header.len = 11;
    write(fd, &header, sizeof(header));

    // First attempt: header present but no body bytes.
    EXPECT_FALSE(incoming.readMessage(serverFd));
    EXPECT_EQ(0U, incoming.messageBytesReceived);

    // Second attempt: part of body present.
    write(fd, "abcde", 5);
    EXPECT_FALSE(incoming.readMessage(serverFd));
    EXPECT_EQ(5U, incoming.messageBytesReceived);

    // Third attempt: remainder of body present, plus extra bytes
    // (don't read extras).
    write(fd, "0123456789", 10);
    EXPECT_TRUE(incoming.readMessage(serverFd));
    EXPECT_EQ("abcde012345", toString(&buffer));

    close(fd);
}

TEST_F(TcpTransportTest, readMessage_discardExtraneousBytes) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    server.acceptHandler->handleFileEvent();
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;
    Buffer buffer;
    TcpTransport::IncomingMessage incoming(&buffer, NULL);
    TcpTransport::Header header;
    header.len = 5000;
    char body[5000];
    write(fd, &header, sizeof(header));

    // Read the header and modify the message to ignore most of the body.
    EXPECT_FALSE(incoming.readMessage(serverFd));
    EXPECT_EQ(5000U, incoming.messageLength);
    incoming.messageLength = 5;
    buffer.reset();

    // Read the body and make sure the correct bytes are ignored
    snprintf(body, sizeof(body), "abcdefghijklmnop");
    write(fd, body, sizeof(body));
    EXPECT_FALSE(incoming.readMessage(serverFd));
    EXPECT_EQ(4101U, incoming.messageBytesReceived);
    TestLog::reset();
    EXPECT_TRUE(incoming.readMessage(serverFd));
    EXPECT_EQ(5000U, incoming.messageBytesReceived);
    EXPECT_EQ("abcde", toString(&buffer));

    // One more check to make sure exactly the right number of bytes were
    // read from the socket.  Also tests zero-length message bodies.
    header.nonce = 0xaaaabbbbccccddddUL;
    header.len = 0;
    buffer.reset();
    write(fd, &header, sizeof(header));
    TcpTransport::IncomingMessage incoming2(&buffer, NULL);
    EXPECT_TRUE(incoming2.readMessage(serverFd));
    EXPECT_EQ(0xaaaabbbbccccddddUL, incoming2.header.nonce);
    EXPECT_EQ("", toString(&buffer));

    close(fd);
}

TEST_F(TcpTransportTest, sessionConstructor_socketError) {
    sys->socketErrno = EPERM;
    string message("");
    try {
        TcpTransport::TcpSession session(*locator);
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("TcpTransport couldn't open socket "
            "for session: Operation not permitted", message);
}

TEST_F(TcpTransportTest, sessionConstructor_connectError) {
    sys->connectErrno = EPERM;
    string message("no exception");
    try {
        TcpTransport::TcpSession session(*locator);
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("Session connect error in TcpTransport: "
            "Operation not permitted", message);
    EXPECT_EQ(1, sys->closeCount);
}

TEST_F(TcpTransportTest, sessionDestructor) {
    TcpTransport server(locator);
    TcpTransport client;
    Transport::SessionRef session = client.getSession(*locator);
    session = NULL;
    EXPECT_EQ(1, sys->closeCount);
}

TEST_F(TcpTransportTest, TcpSession_close_cancelRequestsInProgress) {
    TcpTransport server(locator);
    TcpTransport client;
    Transport::SessionRef session = client.getSession(*locator);
    Buffer request1, request2, request3;
    Buffer reply1, reply2, reply3;

    // Queue several requests.
    Transport::ClientRpc* clientRpc1 = session->clientSend(&request1,
            &reply1);
    Transport::ClientRpc* clientRpc2 = session->clientSend(&request2,
            &reply2);
    Transport::ClientRpc* clientRpc3 = session->clientSend(&request3,
            &reply3);

    // Close the session and make sure all the requests terminate.

    session = NULL;
    EXPECT_TRUE(clientRpc1->isReady());
    EXPECT_TRUE(clientRpc2->isReady());
    EXPECT_TRUE(clientRpc3->isReady());
    string message1("no exception");
    try {
        clientRpc1->wait();
    } catch (TransportException& e) {
        message1 = e.message;
    }
    EXPECT_EQ("RPC cancelled: session closed", message1);
    string message3("no exception");
    try {
        clientRpc3->wait();
    } catch (TransportException& e) {
        message3 = e.message;
    }
    EXPECT_EQ("RPC cancelled: session closed", message3);
}

TEST_F(TcpTransportTest, clientSend_sessionClosed) {
    TcpTransport server(locator);
    TcpTransport client;
    Transport::SessionRef session = client.getSession(*locator);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());
    rawSession->close();
    rawSession->errorInfo = "session closed";
    string message("no exception");
    try {
        Buffer request, reply;
        delete session->clientSend(&request, &reply);
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("session closed", message);
}

TEST_F(TcpTransportTest, findRpc) {
    // This test is a bit goofy, and that we set up a server, then
    // initialize the IncomingMessage to receive a client-side reply.
    TcpTransport::TcpSession session;
    Buffer request1, reply1, request2, reply2, request3, reply3;
    reply1.fillFromString("1111");
    reply2.fillFromString("2222");
    reply3.fillFromString("3333");
    TcpTransport::TcpClientRpc rpc1(&session, &request1, &reply1, 111UL);
    session.outstandingRpcs.push_back(rpc1);
    TcpTransport::TcpClientRpc rpc2(&session, &request2, &reply2, 222UL);
    session.outstandingRpcs.push_back(rpc2);
    TcpTransport::TcpClientRpc rpc3(&session, &request3, &reply3, 333UL);
    session.outstandingRpcs.push_back(rpc3);
    TcpTransport::Header header;
    header.nonce = 111UL;
    EXPECT_EQ("1111", toString(session.findRpc(header)));
    header.nonce = 222UL;
    EXPECT_EQ("2222", toString(session.findRpc(header)));
    header.nonce = 333UL;
    EXPECT_EQ("3333", toString(session.findRpc(header)));
    header.nonce = 334UL;
    EXPECT_TRUE(session.findRpc(header) == NULL);
    session.close();
}

TEST_F(TcpTransportTest, ReplyReadHandler_handleFileEvent_finishRpc) {
    TcpTransport server(locator);
    TcpTransport client;
    Transport::SessionRef session = client.getSession(*locator);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());
    Buffer request;
    Buffer reply;
    request.fillFromString("request1");
    Transport::ClientRpc* clientRpc = session->clientSend(&request,
            &reply);
    Transport::ServerRpc* serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    serverRpc->replyPayload.fillFromString("response1");

    // Reply to the first request, make sure the session state gets
    // cleaned up.
    EXPECT_EQ(1U, rawSession->outstandingRpcs.size());
    serverRpc->sendReply();
    EXPECT_TRUE(waitForRpc(*clientRpc));
    EXPECT_EQ("response1/0", toString(&reply));
    EXPECT_TRUE(rawSession->current == NULL);
    EXPECT_TRUE(clientRpc->isReady());
    EXPECT_TRUE(rawSession->message->buffer == NULL);
}

TEST_F(TcpTransportTest, ReplyReadHandler_eof) {
    // In this test, arrange for the connection to get closed
    // while an RPC is outstanding and we are waiting for a response.
    TcpTransport server(locator);
    TcpTransport client;
    Transport::SessionRef session = client.getSession(*locator);
    Buffer request;
    Buffer reply;
    Buffer::Chunk::appendToBuffer(&request, "xxx", 3);
    Transport::ClientRpc* clientRpc = session->clientSend(&request,
            &reply);
    // The following line serves only to avoid an "unused result"
    // warning for the line above.
    clientRpc->isReady();
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());
    sys->recvEof = true;
    rawSession->replyHandler->handleFileEvent();
    EXPECT_EQ(-1, rawSession->fd);
    EXPECT_EQ("socket closed by server", rawSession->errorInfo);
}

TEST_F(TcpTransportTest, ReplyReadHandler_eofOutsideRPC) {
    // In this test, close the connection when there is no RPC
    // outstanding; this creates additional stress because not all
    // data structures have been initialized.
    TcpTransport server(locator);
    TcpTransport client;
    Transport::SessionRef session = client.getSession(*locator);
    server.acceptHandler->handleFileEvent();
    server.closeSocket(downCast<unsigned>(server.sockets.size()) - 1);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());
    rawSession->replyHandler->handleFileEvent();
    EXPECT_EQ(-1, rawSession->fd);
    EXPECT_EQ("socket closed by server", rawSession->errorInfo);
}

TEST_F(TcpTransportTest, ReplyReadHandler_ioError) {
    TcpTransport server(locator);
    TcpTransport client;
    Transport::SessionRef session = client.getSession(*locator);
    Buffer request;
    Buffer reply;
    Buffer::Chunk::appendToBuffer(&request, "xxx", 3);
    Transport::ClientRpc* clientRpc = session->clientSend(&request,
            &reply);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());
    sys->recvErrno = EPERM;
    rawSession->replyHandler->handleFileEvent();
    EXPECT_EQ(-1, rawSession->fd);
    EXPECT_EQ("handleFileEvent: TcpTransport::ReplyReadHandler "
            "closing session socket: I/O read error in TcpTransport: "
            "Operation not permitted", TestLog::get());
    string message("no exception");
    try {
        clientRpc->wait();
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("RPC cancelled: I/O read error in TcpTransport: "
            "Operation not permitted", message);
}

TEST_F(TcpTransportTest, TcpClientRpc_cancelCleanup) {
    TcpTransport server(locator);
    TcpTransport client;
    Transport::SessionRef session = client.getSession(*locator);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());
    Buffer request1, reply1, request2, reply2;
    request1.fillFromString("request1");
    Transport::ClientRpc* clientRpc1 = session->clientSend(&request1,
            &reply1);
    request2.fillFromString("request2");
    Transport::ClientRpc* clientRpc2 = session->clientSend(&request2,
            &reply2);
    EXPECT_EQ(2U, rawSession->outstandingRpcs.size());
    clientRpc2->cancel();
    EXPECT_EQ(1U, rawSession->outstandingRpcs.size());
    clientRpc1->cancel();
    EXPECT_EQ(0U, rawSession->outstandingRpcs.size());
}

}  // namespace RAMCloud
