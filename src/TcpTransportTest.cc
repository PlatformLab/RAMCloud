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
    TestLog::Enable* logEnabler;
    ServiceManager* serviceManager;

    TcpTransportTest()
            : locator(NULL), sys(NULL), savedSyscall(NULL), logEnabler(NULL),
              serviceManager(NULL)
    {
        locator = new ServiceLocator("tcp+ip: host=localhost, port=11000");
        sys = new MockSyscall();
        savedSyscall = TcpTransport::sys;
        TcpTransport::sys = sys;
        logEnabler = new TestLog::Enable();
        serviceManager = new ServiceManager(NULL);
    }

    ~TcpTransportTest() {
        delete serviceManager;
        delete locator;
        delete sys;
        TcpTransport::sys = savedSyscall;
        delete logEnabler;
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
        while (serviceManager->waitForRpc(0.0) != NULL) {
            result++;
        }
        return result;
    }
    DISALLOW_COPY_AND_ASSIGN(TcpTransportTest);
};

TEST_F(TcpTransportTest, sanityCheck) {
    // Create a server and a client and verify that we can
    // send a request, receive it, send a reply, and receive it.
    // Then try a second request.

    TcpTransport server(locator);
    TcpTransport client;
    Transport::SessionRef session = client.getSession(*locator);

    Buffer request;
    Buffer reply;
    request.fillFromString("abcdefg");
    Transport::ClientRpc* clientRpc = session->clientSend(&request,
            &reply);
    Transport::ServerRpc* serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("abcdefg/0", toString(&serverRpc->recvPayload));
    EXPECT_FALSE(clientRpc->isReady());
    serverRpc->replyPayload.fillFromString("klmn");
    serverRpc->sendReply();
    Dispatch::handleEvent();
    EXPECT_EQ(true, clientRpc->isReady());
    EXPECT_EQ("klmn/0", toString(&reply));

    request.fillFromString("request2");
    reply.reset();
    clientRpc = session->clientSend(&request, &reply);
    serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("request2/0", toString(&serverRpc->recvPayload));
    serverRpc->replyPayload.fillFromString("reply2");
    serverRpc->sendReply();
    clientRpc->wait();
    EXPECT_EQ("reply2/0", toString(&reply));
}

TEST_F(TcpTransportTest, constructorClientSideOnly) {
    sys->socketErrno = EPERM;
    TcpTransport client;
}

TEST_F(TcpTransportTest, constructorSocketError) {
    sys->socketErrno = EPERM;
    EXPECT_EQ("TcpTransport couldn't create listen socket: "
        "Operation not permitted", catchConstruct(locator));
}

TEST_F(TcpTransportTest, constructorNonBlockError) {
    sys->fcntlErrno = EPERM;
    EXPECT_EQ("TcpTransport couldn't set nonblocking on "
        "listen socket: Operation not permitted",
        catchConstruct(locator));
}

TEST_F(TcpTransportTest, constructorReuseAddrError) {
    sys->setsockoptErrno = EPERM;
    EXPECT_EQ("TcpTransport couldn't set SO_REUSEADDR "
        "on listen socket: Operation not permitted",
        catchConstruct(locator));
}

TEST_F(TcpTransportTest, constructorBindError) {
    sys->bindErrno = EPERM;
    EXPECT_EQ("TcpTransport couldn't bind to 'tcp+ip: "
        "host=localhost, port=11000': Operation not permitted",
        catchConstruct(locator));
}

TEST_F(TcpTransportTest, constructorListenError) {
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
    EXPECT_EQ("request1/0", toString(&serverRpc1->recvPayload));
    EXPECT_EQ("request2/0", toString(&serverRpc2->recvPayload));
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

TEST_F(TcpTransportTest, AcceptHandlerNoConnection) {
    TcpTransport server(locator);
    (*server.acceptHandler)();
    EXPECT_EQ(0U, server.sockets.size());
}

TEST_F(TcpTransportTest, AcceptHandlerAcceptFailure) {
    TcpTransport server(locator);
    sys->acceptErrno = EPERM;
    (*server.acceptHandler)();
    EXPECT_EQ("operator(): error in TcpTransport::AcceptHandler "
            "accepting connection for 'tcp+ip: host=localhost, "
            "port=11000': Operation not permitted", TestLog::get());
    EXPECT_EQ(-1, server.listenSocket);
    EXPECT_EQ(1, sys->closeCount);
}

TEST_F(TcpTransportTest, AcceptHandlerSuccess) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    (*server.acceptHandler)();
    EXPECT_NE(server.sockets.size(), 0);
    EXPECT_FALSE(server.sockets[server.sockets.size() - 1] == NULL);
    close(fd);
}

TEST_F(TcpTransportTest, RequestReadHandler) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    (*server.acceptHandler)();
    EXPECT_NE(server.sockets.size(), 0);
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;

    // Send a message in 2 chunks.
    TcpTransport::Header header;
    header.len = 6;
    EXPECT_EQ(static_cast<int>(sizeof(header)),
        write(fd, &header, sizeof(header)));
    server.sockets[serverFd]->readHandler();
    EXPECT_NE(server.sockets[serverFd]->rpc, 0);
    EXPECT_EQ(0, countWaitingRequests());

    EXPECT_EQ(6, write(fd, "abcdef", 6));
    server.sockets[serverFd]->readHandler();
    EXPECT_EQ(1, countWaitingRequests());

    close(fd);
}

TEST_F(TcpTransportTest, RequestReadHandlerUnexpectedData) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    (*server.acceptHandler)();
    EXPECT_NE(server.sockets.size(), 0);
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;

    // Send a message to make the server busy.
    TcpTransport::Header header;
    header.len = 0;
    write(fd, &header, sizeof(header));
    server.sockets[serverFd]->readHandler();
    EXPECT_TRUE(server.sockets[serverFd]->busy);

    // Send more junk to the server.
    write(fd, "abcdef", 6);
    server.sockets[serverFd]->readHandler();
    EXPECT_EQ("operator(): TcpTransport::RequestReadHandler "
            "discarding 6 unexpected bytes from client",
            TestLog::get());

    close(fd);
}

TEST_F(TcpTransportTest, RequestReadHandlerEof) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    (*server.acceptHandler)();
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;
    close(fd);
    server.sockets[serverFd]->readHandler();
    EXPECT_TRUE(server.sockets[serverFd] == NULL);
}

TEST_F(TcpTransportTest, RequestReadHandlerError) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    (*server.acceptHandler)();
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;
    sys->recvErrno = EPERM;
    server.sockets[serverFd]->readHandler();
    EXPECT_TRUE(server.sockets[serverFd] == NULL);
    EXPECT_EQ("operator(): TcpTransport::RequestReadHandler "
            "closing client connection: I/O read error in TcpTransport: "
            "Operation not permitted", TestLog::get());

    close(fd);
}

TEST_F(TcpTransportTest, sendMessageMultipleChunks) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    Buffer payload;
    Buffer::Chunk::appendToBuffer(&payload, "abcde", 5);
    Buffer::Chunk::appendToBuffer(&payload, "xxx", 3);
    Buffer::Chunk::appendToBuffer(&payload, "12345678", 8);
    TcpTransport::sendMessage(fd, payload);

    Transport::ServerRpc* serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("abcdexxx12345678",
            toString(&serverRpc->recvPayload));

    close(fd);
}

TEST_F(TcpTransportTest, sendMessageErrorOnSend) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    Buffer payload;
    Buffer::Chunk::appendToBuffer(&payload, "test message", 5);

    sys->sendmsgErrno = EPERM;
    string message("no exception");
    try {
        TcpTransport::sendMessage(fd, payload);
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("I/O error in TcpTransport::sendMessage: "
            "Operation not permitted", message);

    close(fd);
}

TEST_F(TcpTransportTest, sendMessageBrokenPipe) {
    // The main reason for this test is to make sure that
    // broken pipe errors don't generate signals that kill
    // the process.
    TcpTransport server(locator);
    TcpTransport client;
    Transport::SessionRef session = client.getSession(*locator);
    Dispatch::handleEvent();
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;
    server.closeSocket(serverFd);
    string message("no exception");
    try {
        Buffer request;
        Buffer::Chunk::appendToBuffer(&request, "message chunk", 13);
        TcpTransport::TcpSession* rawSession =
                reinterpret_cast<TcpTransport::TcpSession*>(session.get());
        for (int i = 0; i < 1000; i++) {
            TcpTransport::sendMessage(rawSession->fd, request);
        }
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("I/O error in TcpTransport::sendMessage: "
            "Broken pipe", message);
}

TEST_F(TcpTransportTest, sendMessageShortCount) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    Buffer payload;
    Buffer::Chunk::appendToBuffer(&payload, "test message", 5);

    sys->sendmsgReturnCount = 3;
    string message("no exception");
    try {
        TcpTransport::sendMessage(fd, payload);
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("Incomplete sendmsg in "
            "TcpTransport::sendMessage: 3 bytes sent out of 9",
            message);

    close(fd);
}

TEST_F(TcpTransportTest, recvCarefullyIoErrors) {
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

TEST_F(TcpTransportTest, readMessageReceiveHeaderInPieces) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    (*server.acceptHandler)();
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;

    // Try to receive when there is no data at all.
    Buffer buffer;
    TcpTransport::IncomingMessage incoming(&buffer);
    EXPECT_FALSE(incoming.readMessage(serverFd));
    EXPECT_EQ(0U, incoming.headerBytesReceived);

    // Send first part of header.
    TcpTransport::Header header;
    header.len = 123456789;
    write(fd, &header, 3);
    EXPECT_FALSE(incoming.readMessage(serverFd));
    EXPECT_EQ(3U, incoming.headerBytesReceived);

    // Send second part of header (oversize counter will generate
    // exception).
    write(fd, reinterpret_cast<char*>(&header)+3, sizeof(header)-3);
    string message("no exception");
    try {
        incoming.readMessage(serverFd);
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("TcpTransport received oversize message "
            "(123456789 bytes)", message);

    close(fd);
}

TEST_F(TcpTransportTest, readMessageZeroLengthMessage) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    (*server.acceptHandler)();
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;
    Buffer buffer;
    TcpTransport::IncomingMessage incoming(&buffer);
    TcpTransport::Header header;
    header.len = 0;
    write(fd, &header, sizeof(header));
    EXPECT_TRUE(incoming.readMessage(serverFd));
    EXPECT_EQ("", toString(&buffer));

    close(fd);
}

TEST_F(TcpTransportTest, readMessageReceiveBodyInPieces) {
    TcpTransport server(locator);
    int fd = connectToServer(*locator);
    (*server.acceptHandler)();
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;
    Buffer buffer;
    TcpTransport::IncomingMessage incoming(&buffer);
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

TEST_F(TcpTransportTest, sessionConstructorSocketError) {
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

TEST_F(TcpTransportTest, sessionConstructorConnectError) {
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

TEST_F(TcpTransportTest, sessionDestructorFinishRequestsInProgress) {
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
    EXPECT_EQ("session closed", message1);
    string message3("no exception");
    try {
        clientRpc3->wait();
    } catch (TransportException& e) {
        message3 = e.message;
    }
    EXPECT_EQ("session closed", message3);
}

TEST_F(TcpTransportTest, clientSendSessionClosed) {
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

TEST_F(TcpTransportTest, clientSendQueueRequest) {
    TcpTransport server(locator);
    TcpTransport client;
    Transport::SessionRef session = client.getSession(*locator);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());
    Buffer request1, request2;
    Buffer reply1, reply2;
    request1.fillFromString("abcdefg");
    Transport::ClientRpc* clientRpc1 = session->clientSend(&request1,
            &reply1);
    Transport::ClientRpc* clientRpc2 = session->clientSend(&request2,
            &reply2);
    EXPECT_EQ(clientRpc1, rawSession->current);
    EXPECT_EQ(1U, rawSession->waitingRpcs.size());
    EXPECT_EQ(clientRpc2, rawSession->waitingRpcs.back());
}

TEST_F(TcpTransportTest, ReplyReadHandlerUnexpectedDataFromServer) {
    TcpTransport server(locator);
    TcpTransport client;
    Transport::SessionRef session = client.getSession(*locator);
    (*server.acceptHandler)();
    write(downCast<unsigned>(server.sockets.size()) - 1, "abcdef", 6);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());
    (*rawSession->replyHandler)();
    EXPECT_EQ("operator(): TcpTransport::ReplyReadHandler "
            "discarding 6 unexpected bytes from server 127.0.0.1:11000",
            TestLog::get());
}

TEST_F(TcpTransportTest, ReplyReadHandlerStartNextRequest) {
    TcpTransport server(locator);
    TcpTransport client;
    Transport::SessionRef session = client.getSession(*locator);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());
    Buffer request1, request2;
    Buffer reply1, reply2;
    request1.fillFromString("abcdefg");
    request1.fillFromString("xyzzy");

    // Start 2 requests (the second one will get queued).
    Transport::ClientRpc* clientRpc1 = session->clientSend(&request1,
            &reply1);
    Transport::ClientRpc* clientRpc2 = session->clientSend(&request2,
            &reply2);
    EXPECT_EQ(1U, rawSession->waitingRpcs.size());
    Transport::ServerRpc* serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    serverRpc->replyPayload.fillFromString("klmn");

    // Reply to the first request, make sure the second one gets unqueued.
    serverRpc->sendReply();
    Dispatch::handleEvent();
    EXPECT_EQ(0U, rawSession->waitingRpcs.size());
    serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    serverRpc->replyPayload.fillFromString("aaaaa");
    serverRpc->sendReply();
    Dispatch::handleEvent();
    EXPECT_TRUE(clientRpc1->isReady());
    EXPECT_TRUE(clientRpc2->isReady());
    EXPECT_EQ("klmn/0", toString(&reply1));
    EXPECT_EQ("aaaaa/0", toString(&reply2));
}

TEST_F(TcpTransportTest, ReplyReadHandlerEof) {
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
    (*rawSession->replyHandler)();
    EXPECT_EQ(-1, rawSession->fd);
    EXPECT_EQ("socket closed by server", rawSession->errorInfo);
}

TEST_F(TcpTransportTest, ReplyReadHandlerEofOutsideRPC) {
    // In this test, close the connection when there is no RPC
    // outstanding; this creates additional stress because not all
    // data structures have been initialized.
    TcpTransport server(locator);
    TcpTransport client;
    Transport::SessionRef session = client.getSession(*locator);
    (*server.acceptHandler)();
    server.closeSocket(downCast<unsigned>(server.sockets.size()) - 1);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());
    (*rawSession->replyHandler)();
    EXPECT_EQ(-1, rawSession->fd);
    EXPECT_EQ("socket closed by server", rawSession->errorInfo);
}

TEST_F(TcpTransportTest, ReplyReadHandlerIoError) {
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
    (*rawSession->replyHandler)();
    EXPECT_EQ(-1, rawSession->fd);
    EXPECT_EQ("operator(): TcpTransport::ReplyReadHandler "
            "closing session socket: I/O read error in TcpTransport: "
            "Operation not permitted", TestLog::get());
    string message("no exception");
    try {
        clientRpc->wait();
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("I/O read error in TcpTransport: Operation not permitted",
            message);
}

}  // namespace RAMCloud
