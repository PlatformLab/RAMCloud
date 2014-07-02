/* Copyright (c) 2010-2014 Stanford University
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
#include "MockWrapper.h"
#include "ServiceManager.h"
#include "TcpTransport.h"
#include "Tub.h"

namespace RAMCloud {

class TcpTransportTest : public ::testing::Test {
  public:
    Context context;
    ServiceManager* serviceManager;
    ServiceLocator locator;
    MockSyscall* sys;
    Syscall* savedSyscall;
    TestLog::Enable logEnabler;
    TcpTransport server;
    TcpTransport client;

    TcpTransportTest()
            : context()
            , serviceManager(context.serviceManager)
            , locator("tcp+ip:host=localhost,port=11000")
            , sys(NULL)
            , savedSyscall(NULL)
            , logEnabler()
            , server(&context, &locator)
            , client(&context)
    {
        sys = new MockSyscall();
        savedSyscall = TcpTransport::sys;
        TcpTransport::sys = sys;
    }

    ~TcpTransportTest() {
        delete sys;
        TcpTransport::sys = savedSyscall;
    }

    string catchConstruct(ServiceLocator* locator) {
        string message("no exception");
        try {
            TcpTransport server2(&context, locator);
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
    int countWaitingRequests(TcpTransport* transport)
    {
        int result = 0;
        Transport::ServerRpc* rpc = NULL;
        while ((rpc = context.serviceManager->waitForRpc(0.0)) != NULL) {
            transport->serverRpcPool.destroy(
                static_cast<TcpTransport::TcpServerRpc*>(rpc));
            result++;
        }
        return result;
    }

    // Run the dispatcher until a server-side session has been opened
    // (but give up if it takes too long).  The return value is true
    // if a session has been opened.
    bool waitForSession(TcpTransport& transport)
    {
        // See "Timing-Dependent Tests" in designNotes.
        for (int i = 0; i < 1000; i++) {
            context.dispatch->poll();
            if (transport.sockets.size() > 0)
                return true;
            usleep(1000);
        }
        return false;
    }
    DISALLOW_COPY_AND_ASSIGN(TcpTransportTest);
};

TEST_F(TcpTransportTest, sanityCheck) {
    Transport::SessionRef session = client.getSession(locator);

    // Send two requests from the client.
    MockWrapper rpc1("request1");
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    MockWrapper rpc2("request2");
    session->sendRequest(&rpc2.request, &rpc2.response, &rpc2);

    // Receive the two requests on the server.
    Transport::ServerRpc* serverRpc1 = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc1 != NULL);
    EXPECT_EQ("request1", TestUtil::toString(&serverRpc1->requestPayload));
    Transport::ServerRpc* serverRpc2 = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc2 != NULL);
    EXPECT_EQ("request2", TestUtil::toString(&serverRpc2->requestPayload));

    // Reply to the requests in backwards order.
    serverRpc2->replyPayload.fillFromString("response2");
    serverRpc2->sendReply();
    serverRpc1->replyPayload.fillFromString("response1");
    serverRpc1->sendReply();

    // Receive the responses in the client.
    EXPECT_STREQ("completed: 0, failed: 0", rpc1.getState());
    EXPECT_STREQ("completed: 0, failed: 0", rpc2.getState());
    EXPECT_TRUE(TestUtil::waitForRpc(&context, rpc1));
    EXPECT_STREQ("completed: 1, failed: 0", rpc1.getState());
    EXPECT_STREQ("completed: 1, failed: 0", rpc2.getState());
}

TEST_F(TcpTransportTest, constructor_clientSideOnly) {
    sys->socketErrno = EPERM;
}

TEST_F(TcpTransportTest, constructor_socketError) {
    sys->socketErrno = EPERM;
    EXPECT_EQ("TcpTransport couldn't create listen socket: "
        "Operation not permitted", catchConstruct(&locator));
    EXPECT_EQ("TcpTransport: TcpTransport couldn't create listen socket: "
        "Operation not permitted", TestLog::get());
}

TEST_F(TcpTransportTest, constructor_nonBlockError) {
    sys->fcntlErrno = EPERM;
    EXPECT_EQ("TcpTransport couldn't set nonblocking on "
        "listen socket: Operation not permitted",
        catchConstruct(&locator));
    EXPECT_EQ("TcpTransport: TcpTransport couldn't set nonblocking "
        "on listen socket: Operation not permitted", TestLog::get());
}

TEST_F(TcpTransportTest, constructor_reuseAddrError) {
    sys->setsockoptErrno = EPERM;
    EXPECT_EQ("TcpTransport couldn't set SO_REUSEADDR "
        "on listen socket: Operation not permitted",
        catchConstruct(&locator));
    EXPECT_EQ("TcpTransport: TcpTransport couldn't set SO_REUSEADDR "
        "on listen socket: Operation not permitted", TestLog::get());
}

TEST_F(TcpTransportTest, constructor_bindError) {
    sys->bindErrno = EPERM;
    EXPECT_EQ("TcpTransport couldn't bind to 'tcp+ip:"
        "host=localhost,port=11000': Operation not permitted",
        catchConstruct(&locator));
    EXPECT_EQ("TcpTransport: TcpTransport couldn't bind to 'tcp+ip:"
        "host=localhost,port=11000': Operation not permitted", TestLog::get());
}

TEST_F(TcpTransportTest, constructor_listenError) {
    ServiceLocator locator("tcp+ip:host=localhost,port=11001");
    sys->listenErrno = EPERM;
    EXPECT_EQ("TcpTransport couldn't listen on socket: "
        "Operation not permitted", catchConstruct(&locator));
    EXPECT_EQ("TcpTransport: TcpTransport couldn't listen on socket: "
        "Operation not permitted", TestLog::get());
}

TEST_F(TcpTransportTest, destructor) {
    // Connect 2 clients to 1 server, then delete them all and make
    // sure that all of the sockets get closed.
    ServiceLocator locator("tcp+ip:host=localhost,port=11001");
    TcpTransport* server = new TcpTransport(&context, &locator);
    TcpTransport* client = new TcpTransport(&context);
    Transport::SessionRef session1 = client->getSession(locator);
    Transport::SessionRef session2 = client->getSession(locator);

    MockWrapper rpc1("request1");
    MockWrapper rpc2("request2");
    session1->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    session2->sendRequest(&rpc2.request, &rpc2.response, &rpc2);
    Transport::ServerRpc* serverRpc1 = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc1 != NULL);
    Transport::ServerRpc* serverRpc2 = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc2 != NULL);
    EXPECT_EQ("request1", TestUtil::toString(&serverRpc1->requestPayload));
    EXPECT_EQ("request2", TestUtil::toString(&serverRpc2->requestPayload));
    serverRpc1->replyPayload.fillFromString("reply1");
    serverRpc1->sendReply();
    serverRpc2->replyPayload.fillFromString("reply2");
    serverRpc2->sendReply();
    EXPECT_TRUE(TestUtil::waitForRpc(&context, rpc1));
    EXPECT_TRUE(TestUtil::waitForRpc(&context, rpc2));
    EXPECT_EQ("reply1/0", TestUtil::toString(&rpc1.response));
    EXPECT_EQ("reply2/0", TestUtil::toString(&rpc2.response));

    sys->closeCount = 0;
    session1 = NULL;
    session2 = NULL;
    EXPECT_EQ(2, sys->closeCount);
    delete server;
    EXPECT_EQ(5, sys->closeCount);
    delete client;
    EXPECT_EQ(5, sys->closeCount);
}

TEST_F(TcpTransportTest, Socket_destructor_deleteRpc) {
    // Send a partial message to a server, then close its socket and
    // ensure that the TcpServerRpc was deleted.
    int fd = connectToServer(locator);
    server.acceptHandler->handleFileEvent(Dispatch::FileEvent::READABLE);
    EXPECT_NE(server.sockets.size(), 0U);
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;

    TcpTransport::Header header;
    header.len = 6;
    EXPECT_EQ(static_cast<int>(sizeof(header)),
        write(fd, &header, sizeof(header)));
    server.sockets[serverFd]->ioHandler.handleFileEvent(
            Dispatch::FileEvent::READABLE);
    EXPECT_TRUE(server.sockets[serverFd]->rpc != NULL);
    server.closeSocket(serverFd);
    EXPECT_EQ("~TcpServerRpc: deleted", TestLog::get());
}

TEST_F(TcpTransportTest, Socket_destructor_clearRpcsWaitingToReply) {
    TestLog::Enable _("~TcpServerRpc");

    // Send several requests to the server; respond to all of them,
    // but make the first response large enough that it backs up
    // the others.
    Transport::SessionRef session = client.getSession(locator);

    // Send requests.
    MockWrapper rpc1("request1");
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    MockWrapper rpc2("request2");
    session->sendRequest(&rpc2.request, &rpc2.response, &rpc2);
    MockWrapper rpc3("request3");
    session->sendRequest(&rpc3.request, &rpc3.response, &rpc3);

    // Receive the requests on the server and respond to each.
    Transport::ServerRpc* serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    TestUtil::fillLargeBuffer(&serverRpc->replyPayload, 1000000);
    serverRpc->sendReply();
    serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    serverRpc->replyPayload.fillFromString("response2");
    serverRpc->sendReply();
    serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    serverRpc->replyPayload.fillFromString("response3");
    serverRpc->sendReply();

    EXPECT_NE(server.sockets.size(), 0U);
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;
    EXPECT_EQ(3U, server.sockets[serverFd]->rpcsWaitingToReply.size());
    server.closeSocket(serverFd);
    EXPECT_EQ("~TcpServerRpc: deleted | ~TcpServerRpc: deleted | "
            "~TcpServerRpc: deleted", TestLog::get());
}

TEST_F(TcpTransportTest, AcceptHandler_handleFileEvent_noConnection) {
    server.acceptHandler->handleFileEvent(Dispatch::FileEvent::READABLE);
    EXPECT_EQ(0U, server.sockets.size());
}

TEST_F(TcpTransportTest, AcceptHandler_handleFileEvent_acceptFailure) {
    sys->acceptErrno = EPERM;
    server.acceptHandler->handleFileEvent(Dispatch::FileEvent::READABLE);
    EXPECT_EQ("handleFileEvent: error in TcpTransport::AcceptHandler "
            "accepting connection for 'tcp+ip:host=localhost,"
            "port=11000': Operation not permitted", TestLog::get());
    EXPECT_EQ(-1, server.listenSocket);
    EXPECT_EQ(1, sys->closeCount);
}

TEST_F(TcpTransportTest, AcceptHandler_handleFileEvent_success) {
    int fd = connectToServer(locator);
    server.acceptHandler->handleFileEvent(Dispatch::FileEvent::READABLE);
    EXPECT_NE(server.sockets.size(), 0U);
    EXPECT_FALSE(server.sockets[server.sockets.size() - 1] == NULL);
    close(fd);
}

TEST_F(TcpTransportTest, ServerSocketHandler_handleFileEvent_reads) {
    int fd = connectToServer(locator);
    server.acceptHandler->handleFileEvent(Dispatch::FileEvent::READABLE);
    EXPECT_NE(server.sockets.size(), 0U);
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;

    // Send a message in 2 chunks.
    TcpTransport::Header header;
    header.len = 6;
    EXPECT_EQ(static_cast<int>(sizeof(header)),
        write(fd, &header, sizeof(header)));
    server.sockets[serverFd]->ioHandler.handleFileEvent(
            Dispatch::FileEvent::READABLE);
    EXPECT_TRUE(server.sockets[serverFd]->rpc != NULL);
    EXPECT_EQ(0, countWaitingRequests(&server));

    EXPECT_EQ(6, write(fd, "abcdef", 6));
    server.sockets[serverFd]->ioHandler.handleFileEvent(
            Dispatch::FileEvent::READABLE);
    EXPECT_EQ(1, countWaitingRequests(&server));

    close(fd);
}

TEST_F(TcpTransportTest, ServerSocketHandler_handleFileEvent_writes) {
    // Generate 3 requests and respond to each; make the first response
    // too large to send entirely in sendReply, so that handleFileEvent
    // must finish the sending.
    Transport::SessionRef session = client.getSession(locator);

    // Send requests.
    MockWrapper rpc1("request1");
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    MockWrapper rpc2("request2");
    session->sendRequest(&rpc2.request, &rpc2.response, &rpc2);
    MockWrapper rpc3("request3");
    session->sendRequest(&rpc3.request, &rpc3.response, &rpc3);

    // Send replies.
    Transport::ServerRpc* serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    TestUtil::fillLargeBuffer(&serverRpc->replyPayload, 199999);
    serverRpc->sendReply();
    serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    serverRpc->replyPayload.fillFromString("response2");
    serverRpc->sendReply();
    serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    serverRpc->replyPayload.fillFromString("response3");
    serverRpc->sendReply();

    // Receive and check replies.
    EXPECT_TRUE(TestUtil::waitForRpc(&context, rpc1));
    EXPECT_EQ("ok", TestUtil::checkLargeBuffer(&rpc1.response, 199999));
    EXPECT_TRUE(TestUtil::waitForRpc(&context, rpc2));
    EXPECT_EQ("response2/0", TestUtil::toString(&rpc2.response));
    EXPECT_TRUE(TestUtil::waitForRpc(&context, rpc3));
    EXPECT_EQ("response3/0", TestUtil::toString(&rpc3.response));
    EXPECT_EQ("~TcpServerRpc: deleted | ~TcpServerRpc: deleted "
            "| ~TcpServerRpc: deleted", TestLog::get());
    EXPECT_NE(server.sockets.size(), 0U);
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;
    EXPECT_EQ(0U, server.sockets[serverFd]->rpcsWaitingToReply.size());
}

TEST_F(TcpTransportTest, ServerSocketHandler_handleFileEvent_eof) {
    int fd = connectToServer(locator);
    server.acceptHandler->handleFileEvent(Dispatch::FileEvent::READABLE);
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;
    close(fd);
    server.sockets[serverFd]->ioHandler.handleFileEvent(
            Dispatch::FileEvent::READABLE);
    EXPECT_TRUE(server.sockets[serverFd] == NULL);
}

TEST_F(TcpTransportTest, ServerSocketHandler_handleFileEvent_error) {
    int fd = connectToServer(locator);
    server.acceptHandler->handleFileEvent(Dispatch::FileEvent::READABLE);
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;
    sys->recvErrno = EPERM;
    server.sockets[serverFd]->ioHandler.handleFileEvent(
            Dispatch::FileEvent::READABLE);
    EXPECT_TRUE(server.sockets[serverFd] == NULL);
    EXPECT_EQ("recvCarefully: TcpTransport recv error: "
            "Operation not permitted | ~TcpServerRpc: deleted",
            TestLog::get());

    close(fd);
}

// Most of the functionality of sendMessage was already tested by
// ServerSocketHandler_handleFileEvent_writes above.

TEST_F(TcpTransportTest, sendMessage_sendPartOfHeader) {
    int fd = connectToServer(locator);
    TcpTransport::Header header;
    header.nonce = 222;
    header.len = 10;
    // Send a few bytes of the header explicitly, then call sendMessage
    // to handle the rest.
    EXPECT_EQ(6, write(fd, &header, 6));
    Buffer payload;
    payload.fillFromString("0xaa55aa55 30 40");
    EXPECT_EQ(0, TcpTransport::sendMessage(fd, 222,
            &payload, 12 + sizeof(TcpTransport::Header) - 6));
    Transport::ServerRpc* serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("0xaa55aa55 30 40",
            TestUtil::toString(&serverRpc->requestPayload));
    server.serverRpcPool.destroy(
        static_cast<TcpTransport::TcpServerRpc*>(serverRpc));

    close(fd);
}

TEST_F(TcpTransportTest, sendMessage_multipleChunks) {
    int fd = connectToServer(locator);
    Buffer payload;
    payload.appendExternal("abcde", 5);
    payload.appendExternal("xxx", 3);
    payload.appendExternal("12345678", 8);
    TcpTransport::sendMessage(fd, 111, &payload, -1);

    Transport::ServerRpc* serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("abcdexxx12345678",
            TestUtil::toString(&serverRpc->requestPayload));
    server.serverRpcPool.destroy(
        static_cast<TcpTransport::TcpServerRpc*>(serverRpc));

    close(fd);
}

TEST_F(TcpTransportTest, sendMessage_tooManyChunksForOneMessage) {
    int fd = connectToServer(locator);
    Buffer payload;
    for (int i = 0; i < 60; i++) {
        payload.appendExternal("abcdefghijklmnopqrstuvwxyz" + i/10, 1);
        payload.appendExternal("0123456789" + i%10, 1);
    }
    int bytesLeft = TcpTransport::sendMessage(fd, 111, &payload, -1);
    EXPECT_EQ(21, bytesLeft);
    bytesLeft = TcpTransport::sendMessage(fd, 111, &payload, bytesLeft);
    EXPECT_EQ(0, bytesLeft);

    Transport::ServerRpc* serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("a0a1a2a3a4a5a6a7a8a9b0b1b2b3b4b5b6b7b8b9c0c1c2c3c4c5c6c7c8c9"
            "d0d1d2d3d4d5d6d7d8d9e0e1e2e3e4e5e6e7e8e9f0f1f2f3f4f5f6f7f8f9",
            TestUtil::toString(&serverRpc->requestPayload));
    server.serverRpcPool.destroy(
        static_cast<TcpTransport::TcpServerRpc*>(serverRpc));

    close(fd);
}

TEST_F(TcpTransportTest, sendMessage_errorOnSend) {
    int fd = connectToServer(locator);
    Buffer payload;
    payload.appendExternal("test message", 5);

    sys->sendmsgErrno = EPERM;
    string message("no exception");
    try {
        TcpTransport::sendMessage(fd, 111, &payload, -1);
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("TcpTransport sendmsg error: Operation not permitted", message);

    close(fd);
}

TEST_F(TcpTransportTest, sendMessage_largeBuffer) {
    Transport::SessionRef session = client.getSession(locator);
    MockWrapper rpc(NULL);
    TestUtil::fillLargeBuffer(&rpc.request, 300000);
    TcpTransport::messageChunks = 0;
    session->sendRequest(&rpc.request, &rpc.response, &rpc);
    Transport::ServerRpc* serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_GT(TcpTransport::messageChunks, 0)
        << "The message fit in one chunk. You may have to increase the size "
           "of the message for this test to be effective.";
    EXPECT_EQ("ok", TestUtil::checkLargeBuffer(&serverRpc->requestPayload,
            300000));
    TestUtil::fillLargeBuffer(&serverRpc->replyPayload, 350000);
    TcpTransport::messageChunks = 0;
    serverRpc->sendReply();
    EXPECT_TRUE(TestUtil::waitForRpc(&context, rpc));
    EXPECT_GT(TcpTransport::messageChunks, 0)
        << "The message fit in one chunk. You may have to increase the size "
           "of the message for this test to be effective.";
    EXPECT_EQ("ok", TestUtil::checkLargeBuffer(&rpc.response, 350000));
}

TEST_F(TcpTransportTest, sendMessage_brokenPipe) {
    // The main reason for this test is to make sure that
    // broken pipe errors don't generate signals that kill
    // the process.
    Transport::SessionRef session = client.getSession(locator);
    ASSERT_TRUE(waitForSession(server));
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;
    server.closeSocket(serverFd);
    string message("no exception");
    try {
        Buffer request;
        request.appendExternal("message chunk", 13);
        TcpTransport::TcpSession* rawSession =
                reinterpret_cast<TcpTransport::TcpSession*>(session.get());
        for (int i = 0; i < 1000; i++) {
            TcpTransport::sendMessage(rawSession->fd, 111, &request, -1);
        }
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("TcpTransport sendmsg error: Broken pipe", message);
}

TEST_F(TcpTransportTest, recvCarefully_ioErrors) {
    string message("no exception");
    sys->recvEof = true;
    try {
        TcpTransport::recvCarefully(2, NULL, 100);
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("session closed by peer", message);
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
    EXPECT_EQ("TcpTransport recv error: Operation not permitted", message);
}

// (IncomingMessage::cancel is tested by cancelRequest tests below.)

TEST_F(TcpTransportTest, IncomingMessage_readMessage_receiveHeaderInPieces) {
    int fd = connectToServer(locator);
    server.acceptHandler->handleFileEvent(Dispatch::FileEvent::READABLE);
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

TEST_F(TcpTransportTest, IncomingMessage_readMessage_messageTooLong) {
    TestLog::Enable filter("readMessage");
    int fd = connectToServer(locator);
    server.acceptHandler->handleFileEvent(Dispatch::FileEvent::READABLE);
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
    EXPECT_EQ(8388808U, incoming.messageLength);

    close(fd);
}

TEST_F(TcpTransportTest, IncomingMessage_readMessage_getBufferFromSession) {
    // This test is a bit goofy, in that we set up a server, then
    // initialize the IncomingMessage to receive a client-side reply.
    int fd = connectToServer(locator);
    server.acceptHandler->handleFileEvent(Dispatch::FileEvent::READABLE);
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;
    MockWrapper rpc1("request1");
    TcpTransport::TcpSession session(client);
    TcpTransport::TcpClientRpc* r1 = client.clientRpcPool.construct(
            &rpc1.request, &rpc1.response, &rpc1, 66UL);
    session.rpcsWaitingForResponse.push_back(*r1);
    TcpTransport::IncomingMessage incoming(NULL, &session);
    TcpTransport::Header header;
    header.nonce = 66UL;
    header.len = 5;
    write(fd, &header, sizeof(header));
    write(fd, "abcde", 5);
    EXPECT_TRUE(incoming.readMessage(serverFd));
    EXPECT_EQ("abcde", TestUtil::toString(&rpc1.response));
    session.abort();
    close(fd);
}

TEST_F(TcpTransportTest, IncomingMessage_readMessage_findRpcReturnsNull) {
    int fd = connectToServer(locator);
    server.acceptHandler->handleFileEvent(Dispatch::FileEvent::READABLE);
    int serverFd = downCast<unsigned>(server.sockets.size()) - 1;
    TcpTransport::TcpSession session(client);
    TcpTransport::IncomingMessage incoming(NULL, &session);
    TcpTransport::Header header;
    header.nonce = 66UL;
    header.len = 5;
    write(fd, &header, sizeof(header));
    EXPECT_FALSE(incoming.readMessage(serverFd));
    EXPECT_EQ(0U, incoming.messageLength);
    close(fd);
}

TEST_F(TcpTransportTest, IncomingMessage_readMessage_receiveBodyInPieces) {
    int fd = connectToServer(locator);
    server.acceptHandler->handleFileEvent(Dispatch::FileEvent::READABLE);
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
    EXPECT_EQ("abcde012345", TestUtil::toString(&buffer));

    close(fd);
}

TEST_F(TcpTransportTest, IncomingMessage_readMessage_discardExtraneousBytes) {
    int fd = connectToServer(locator);
    server.acceptHandler->handleFileEvent(Dispatch::FileEvent::READABLE);
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
    EXPECT_EQ("abcde", TestUtil::toString(&buffer));

    // One more check to make sure exactly the right number of bytes were
    // read from the socket.  Also tests zero-length message bodies.
    header.nonce = 0xaaaabbbbccccddddUL;
    header.len = 0;
    buffer.reset();
    write(fd, &header, sizeof(header));
    TcpTransport::IncomingMessage incoming2(&buffer, NULL);
    EXPECT_TRUE(incoming2.readMessage(serverFd));
    EXPECT_EQ(0xaaaabbbbccccddddUL, incoming2.header.nonce);
    EXPECT_EQ("", TestUtil::toString(&buffer));

    close(fd);
}

TEST_F(TcpTransportTest, sessionConstructor_socketError) {
    sys->socketErrno = EPERM;
    string message("");
    try {
        TcpTransport::TcpSession session(client, locator);
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("TcpTransport couldn't open socket "
            "for session: Operation not permitted", message);
    EXPECT_EQ("TcpSession: TcpTransport couldn't open socket "
            "for session: Operation not permitted", TestLog::get());
}

TEST_F(TcpTransportTest, sessionConstructor_connectError) {
    sys->connectErrno = EPERM;
    string message("no exception");
    try {
        TcpTransport::TcpSession session(client, locator);
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("TcpTransport couldn't connect to "
            "tcp+ip:host=localhost,port=11000: Operation not permitted",
            message);
    EXPECT_EQ("TcpSession: TcpTransport couldn't connect to "
            "tcp+ip:host=localhost,port=11000: Operation not permitted",
            TestLog::get());
    EXPECT_EQ(1, sys->closeCount);
}

TEST_F(TcpTransportTest, sessionDestructor) {
    Transport::SessionRef session = client.getSession(locator);
    session = NULL;
    EXPECT_EQ(1, sys->closeCount);
}

TEST_F(TcpTransportTest, TcpSession_abort) {
    Transport::SessionRef session = client.getSession(locator);
    MockWrapper rpc("request");
    session->sendRequest(&rpc.request, &rpc.response, &rpc);
    session->abort();
    EXPECT_STREQ("completed: 0, failed: 1", rpc.getState());
}

TEST_F(TcpTransportTest, TcpSession_cancelRequest_waitingForResponse) {
    Transport::SessionRef session = client.getSession(locator);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());

    // First create a few small requests, which will be sent and
    // end up on rpcsWaitingForResponse.
    MockWrapper rpc1("request1");
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    MockWrapper rpc2("request2");
    session->sendRequest(&rpc2.request, &rpc2.response, &rpc2);
    EXPECT_EQ(2U, rawSession->rpcsWaitingForResponse.size());

    // Now try cancelling them.
    session->cancelRequest(&rpc2);
    EXPECT_EQ(1U, rawSession->rpcsWaitingForResponse.size());
    EXPECT_EQ("request1", TestUtil::toString(
            rawSession->rpcsWaitingForResponse.front().request));
    session->cancelRequest(&rpc1);
    EXPECT_EQ(0U, rawSession->rpcsWaitingForResponse.size());
}

TEST_F(TcpTransportTest, TcpSession_cancelRequest_responsePartiallyReceived) {
    // Make sure that if an RPC is canceled part-way through receiving
    // its response, the response buffer is not accessed after cancelRequest
    // returns.
    Transport::SessionRef session = client.getSession(locator);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());

    // Send a request and receive it on the server.
    MockWrapper rpc("request1");
    session->sendRequest(&rpc.request, &rpc.response, &rpc);
    TcpTransport::TcpServerRpc* serverRpc =
            static_cast<TcpTransport::TcpServerRpc*>(
            serviceManager->waitForRpc(1.0));
    EXPECT_TRUE(serverRpc != NULL);
    TcpTransport::TcpClientRpc* clientRpc =
            &(rawSession->rpcsWaitingForResponse.front());
    EXPECT_EQ(&rpc, clientRpc->notifier);

    // Reply in two messages, calling cancelRequest in-between the two.
    TcpTransport::Header header;
    header.nonce = clientRpc->nonce;
    header.len = 21;
    write(serverRpc->fd, &header, sizeof(header));
    write(serverRpc->fd, "First part", 10);
    int readEvent = Dispatch::FileEvent::READABLE;
    rawSession->clientIoHandler->handleFileEvent(readEvent);
    EXPECT_EQ(21U, rpc.response.size());
    // The second part of the buffer is uninitialized, so store
    // something predictable there to simplify assertion checking.
    void *garbage;
    rpc.response.peek(10, &garbage);
    memcpy(garbage, "-------------", 11);
    EXPECT_EQ("First part-----------", TestUtil::toString(&rpc.response));
    session->cancelRequest(&rpc);
    EXPECT_TRUE(rawSession->current == NULL);
    write(serverRpc->fd, "Second part", 11);
    rawSession->clientIoHandler->handleFileEvent(readEvent);
    EXPECT_EQ("First part-----------", TestUtil::toString(&rpc.response));
    server.serverRpcPool.destroy(
        static_cast<TcpTransport::TcpServerRpc*>(serverRpc));
}

TEST_F(TcpTransportTest, TcpSession_cancelRequest_waitingToSend) {
    Transport::SessionRef session = client.getSession(locator);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());

    // Send a large request, which will queue on rpcsWaitingToSend,
    // followed by a couple more smaller ones, which will queue behind it.
    MockWrapper rpc1;
    TestUtil::fillLargeBuffer(&rpc1.request, 777777);
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    MockWrapper rpc2("request2");
    session->sendRequest(&rpc2.request, &rpc2.response, &rpc2);
    MockWrapper rpc3("request3");
    session->sendRequest(&rpc3.request, &rpc3.response, &rpc3);
    EXPECT_EQ(3U, rawSession->rpcsWaitingToSend.size());

    // Now try cancelling them.
    session->cancelRequest(&rpc2);
    EXPECT_EQ(2U, rawSession->rpcsWaitingToSend.size());
    EXPECT_EQ("ok", TestUtil::checkLargeBuffer(
            rawSession->rpcsWaitingToSend.front().request, 777777));
    EXPECT_EQ("request3", TestUtil::toString(
            rawSession->rpcsWaitingToSend.back().request));
    session->cancelRequest(&rpc1);
    EXPECT_EQ(1U, rawSession->rpcsWaitingToSend.size());
    EXPECT_EQ("request3", TestUtil::toString(
            rawSession->rpcsWaitingToSend.front().request));
}

TEST_F(TcpTransportTest, TcpSession_close_cancelRpcsWaitingToSend) {
    Transport::SessionRef session = client.getSession(locator);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());

    // Queue several requests (make the first one long, so they
    // all block on rpcsWaitingToSend.
    MockWrapper rpc1;
    TestUtil::fillLargeBuffer(&rpc1.request, 777777);
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    MockWrapper rpc2("request2");
    session->sendRequest(&rpc2.request, &rpc2.response, &rpc2);
    MockWrapper rpc3("request3");
    session->sendRequest(&rpc3.request, &rpc3.response, &rpc3);
    EXPECT_EQ(3U, rawSession->rpcsWaitingToSend.size());

    // Close the session and make sure all the requests terminate.
    rawSession->close();
    EXPECT_STREQ("completed: 0, failed: 1", rpc1.getState());
    EXPECT_STREQ("completed: 0, failed: 1", rpc2.getState());
    EXPECT_STREQ("completed: 0, failed: 1", rpc3.getState());
}

TEST_F(TcpTransportTest, TcpSession_close_cancelRpcsWaitingForResponse) {
    Transport::SessionRef session = client.getSession(locator);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());

    // First create a few small requests, which will be sent and
    // end up on rpcsWaitingForResponse.
    MockWrapper rpc1("request1");
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    MockWrapper rpc2("request2");
    session->sendRequest(&rpc2.request, &rpc2.response, &rpc2);
    MockWrapper rpc3("request3");
    session->sendRequest(&rpc3.request, &rpc3.response, &rpc3);
    EXPECT_EQ(3U, rawSession->rpcsWaitingForResponse.size());

    // Close the session and make sure all the requests terminate.
    rawSession->close();
    EXPECT_STREQ("completed: 0, failed: 1", rpc1.getState());
    EXPECT_STREQ("completed: 0, failed: 1", rpc2.getState());
    EXPECT_STREQ("completed: 0, failed: 1", rpc3.getState());
}

TEST_F(TcpTransportTest, findRpc) {
    // This test is a bit goofy, in that we set up a server, then
    // initialize the IncomingMessage to receive a client-side reply.
    TcpTransport::TcpSession session(client);
    MockWrapper rpc1("11111"), rpc2("2222"), rpc3("3333");
    TcpTransport::TcpClientRpc* r1 = client.clientRpcPool.construct(
            &rpc1.request, &rpc1.response, &rpc1, 111UL);
    session.rpcsWaitingForResponse.push_back(*r1);
    TcpTransport::TcpClientRpc* r2 = client.clientRpcPool.construct(
            &rpc2.request, &rpc2.response, &rpc2, 222UL);
    session.rpcsWaitingForResponse.push_back(*r2);
    TcpTransport::TcpClientRpc* r3 = client.clientRpcPool.construct(
            &rpc3.request, &rpc3.response, &rpc3, 333UL);
    session.rpcsWaitingForResponse.push_back(*r3);
    TcpTransport::Header header;
    header.nonce = 111UL;
    EXPECT_EQ(&rpc1.response, session.findRpc(header));
    header.nonce = 222UL;
    EXPECT_EQ(&rpc2.response, session.findRpc(header));
    header.nonce = 333UL;
    EXPECT_EQ(&rpc3.response, session.findRpc(header));
    header.nonce = 334UL;
    EXPECT_TRUE(session.findRpc(header) == NULL);
    session.abort();
}

TEST_F(TcpTransportTest, TcpSession_getRpcInfo) {
    Transport::SessionRef session = client.getSession(locator);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());

    // No active RPCs.
    EXPECT_EQ("no active RPCs to server at tcp+ip:host=localhost,port=11000",
                rawSession-> getRpcInfo());

    // Send a couple of small requests, which will end up queued on
    // rpcsWaitingForResponse, then send a large request, which will queue
    // on rpcsWaitingToSend, followed by a couple more smaller ones, which
    // will queue behind it.
    MockWrapper rpc1;
    rpc1.setOpcode(WireFormat::READ);
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    MockWrapper rpc2;
    rpc2.setOpcode(WireFormat::REMOVE);
    session->sendRequest(&rpc2.request, &rpc2.response, &rpc2);
    MockWrapper rpc3;
    TestUtil::fillLargeBuffer(&rpc3.request, 777777);
    rpc3.setOpcode(WireFormat::WRITE);
    session->sendRequest(&rpc3.request, &rpc3.response, &rpc3);
    MockWrapper rpc4;
    rpc4.setOpcode(WireFormat::PING);
    session->sendRequest(&rpc4.request, &rpc4.response, &rpc4);
    MockWrapper rpc5;
    rpc5.setOpcode(WireFormat::INCREMENT);
    session->sendRequest(&rpc5.request, &rpc5.response, &rpc5);
    EXPECT_EQ(3U, rawSession->rpcsWaitingToSend.size());

    EXPECT_EQ("READ, REMOVE, WRITE, PING, INCREMENT to server "
                "at tcp+ip:host=localhost,port=11000",
                rawSession-> getRpcInfo());
}

TEST_F(TcpTransportTest, sendRequest_clearResponse) {
    Transport::SessionRef session = client.getSession(locator);
    MockWrapper rpc("request");
    rpc.response.fillFromString("abcdef");
    session->sendRequest(&rpc.request, &rpc.response, &rpc);
    EXPECT_EQ(0U, rpc.response.size());
}

TEST_F(TcpTransportTest, sendRequest_sessionClosed) {
    Transport::SessionRef session = client.getSession(locator);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());
    rawSession->abort();
    MockWrapper rpc("request");
    session->sendRequest(&rpc.request, &rpc.response, &rpc);
    EXPECT_STREQ("completed: 0, failed: 1", rpc.getState());
}

TEST_F(TcpTransportTest, sendRequest_shortAndLongMessages) {
    Transport::SessionRef session = client.getSession(locator);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());

    // Send a short request followed by one that's too long to be sent
    // all at once, followed by another short request.
    MockWrapper rpc1("request1");
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    MockWrapper rpc2;
    TestUtil::fillLargeBuffer(&rpc2.request, 300000);
    session->sendRequest(&rpc2.request, &rpc2.response, &rpc2);
    MockWrapper rpc3("request3");
    session->sendRequest(&rpc3.request, &rpc3.response, &rpc3);

    EXPECT_EQ(2U, rawSession->rpcsWaitingToSend.size());
    EXPECT_EQ(1U, rawSession->rpcsWaitingForResponse.size());
    TcpTransport::TcpClientRpc& r1 =
            rawSession->rpcsWaitingForResponse.front();
    TcpTransport::TcpClientRpc& r2 =
            rawSession->rpcsWaitingToSend.front();
    TcpTransport::TcpClientRpc& r3 =
            rawSession->rpcsWaitingToSend.back();
    EXPECT_EQ("request1", TestUtil::toString(r1.request));
    EXPECT_EQ("ok", TestUtil::checkLargeBuffer(r2.request, 300000));
    EXPECT_EQ("request3", TestUtil::toString(r3.request));
    EXPECT_TRUE(r1.sent);
    EXPECT_FALSE(r2.sent);
    EXPECT_FALSE(r3.sent);
}

TEST_F(TcpTransportTest, sendRequest_sendMessageException) {
    Transport::SessionRef session = client.getSession(locator);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());
    sys->sendmsgErrno = EPERM;
    MockWrapper rpc("request1");
    session->sendRequest(&rpc.request, &rpc.response, &rpc);
    EXPECT_EQ(1, rpc.failedCount);
    EXPECT_EQ(-1, rawSession->fd);
    EXPECT_EQ(0U, rawSession->rpcsWaitingToSend.size());
    EXPECT_EQ(0U, rawSession->rpcsWaitingForResponse.size());
    EXPECT_EQ("sendMessage: TcpTransport sendmsg error: Operation not "
            "permitted", TestLog::get());
}

TEST_F(TcpTransportTest, ClientSocketHandler_handleFileEvent_readResponse) {
    Transport::SessionRef session = client.getSession(locator);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());
    MockWrapper rpc1("request1");
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    Transport::ServerRpc* serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    serverRpc->replyPayload.fillFromString("response1");

    // Reply to the first request, make sure the session state gets
    // cleaned up.
    EXPECT_EQ(1U, rawSession->rpcsWaitingForResponse.size());
    serverRpc->sendReply();
    EXPECT_TRUE(TestUtil::waitForRpc(&context, rpc1));
    EXPECT_EQ("response1/0", TestUtil::toString(&rpc1.response));
    EXPECT_TRUE(rawSession->current == NULL);
    EXPECT_STREQ("completed: 1, failed: 0", rpc1.getState());
    EXPECT_TRUE(rawSession->message->buffer == NULL);
}

TEST_F(TcpTransportTest, ClientSocketHandler_handleFileEvent_sendRequests) {
    Transport::SessionRef session = client.getSession(locator);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());

    // Send a long request (to fill up the socket) followed by 2
    // short requests.
    MockWrapper rpc1;
    TestUtil::fillLargeBuffer(&rpc1.request, 300000);
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    MockWrapper rpc2("request2");
    session->sendRequest(&rpc2.request, &rpc2.response, &rpc2);
    MockWrapper rpc3("request3");
    session->sendRequest(&rpc3.request, &rpc3.response, &rpc3);
    EXPECT_EQ(3U, rawSession->rpcsWaitingToSend.size());

    // Receive requests on the server and make sure they are all okay.
    Transport::ServerRpc* serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("ok", TestUtil::checkLargeBuffer(&serverRpc->requestPayload,
            300000));
    server.serverRpcPool.destroy(
        static_cast<TcpTransport::TcpServerRpc*>(serverRpc));

    serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("request2", TestUtil::toString(&serverRpc->requestPayload));
    server.serverRpcPool.destroy(
        static_cast<TcpTransport::TcpServerRpc*>(serverRpc));

    serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("request3", TestUtil::toString(&serverRpc->requestPayload));
    EXPECT_EQ(0U, rawSession->rpcsWaitingToSend.size());
    EXPECT_EQ(3U, rawSession->rpcsWaitingForResponse.size());
    EXPECT_TRUE(rawSession->rpcsWaitingForResponse.front().sent);
    server.serverRpcPool.destroy(
        static_cast<TcpTransport::TcpServerRpc*>(serverRpc));
}

TEST_F(TcpTransportTest, ClientSocketHandler_eof) {
    // In this test, arrange for the connection to get closed
    // while an RPC is outstanding and we are waiting for a response.
    Transport::SessionRef session = client.getSession(locator);
    MockWrapper rpc1("xxx");
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());
    sys->recvEof = true;
    rawSession->clientIoHandler->handleFileEvent(Dispatch::FileEvent::READABLE);
    EXPECT_EQ(-1, rawSession->fd);
}

TEST_F(TcpTransportTest, ClientSocketHandler_eofOutsideRPC) {
    // In this test, close the connection when there is no RPC
    // outstanding; this creates additional stress because not all
    // data structures have been initialized.
    Transport::SessionRef session = client.getSession(locator);
    server.acceptHandler->handleFileEvent(Dispatch::FileEvent::READABLE);
    server.closeSocket(downCast<unsigned>(server.sockets.size()) - 1);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());
    rawSession->clientIoHandler->handleFileEvent(Dispatch::FileEvent::READABLE);
    EXPECT_EQ(-1, rawSession->fd);
}

TEST_F(TcpTransportTest, ClientSocketHandler_ioError) {
    Transport::SessionRef session = client.getSession(locator);
    MockWrapper rpc1("xxx");
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    TcpTransport::TcpSession* rawSession =
            reinterpret_cast<TcpTransport::TcpSession*>(session.get());
    sys->recvErrno = EPERM;
    rawSession->clientIoHandler->handleFileEvent(Dispatch::FileEvent::READABLE);
    EXPECT_EQ(-1, rawSession->fd);
    EXPECT_EQ("recvCarefully: TcpTransport recv error: "
            "Operation not permitted", TestLog::get());
    string message("no exception");
    EXPECT_STREQ("completed: 0, failed: 1", rpc1.getState());
}

TEST_F(TcpTransportTest, sendReply_fdClosed) {
    // Create a situation where the server shuts down a socket before
    // an RPC response is sent.
    Transport::SessionRef session = client.getSession(locator);
    MockWrapper rpc1("request1");
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    TcpTransport::TcpServerRpc* serverRpc =
            static_cast<TcpTransport::TcpServerRpc*>(
            serviceManager->waitForRpc(1.0));
    EXPECT_TRUE(serverRpc != NULL);
    serverRpc->replyPayload.fillFromString("response1");
    server.closeSocket(serverRpc->fd);
    EXPECT_NO_THROW(serverRpc->sendReply());
}

TEST_F(TcpTransportTest, sendReply_fdReused) {
    // Create a situation where the socket for an RPC has been closed
    // and reused for a different connection before an RPC response is
    // sent.
    Transport::SessionRef session = client.getSession(locator);
    MockWrapper rpc1("request1");
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    TcpTransport::TcpServerRpc* serverRpc1 =
            static_cast<TcpTransport::TcpServerRpc*>(
            serviceManager->waitForRpc(1.0));
    EXPECT_TRUE(serverRpc1 != NULL);
    serverRpc1->replyPayload.fillFromString("response1");

    // First RPC is about to return.  Shut down its socket and
    // create a new session that will reuse the same socket.
    server.closeSocket(serverRpc1->fd);
    session = NULL;
    session = client.getSession(locator);
    MockWrapper rpc2("request2");
    session->sendRequest(&rpc2.request, &rpc2.response, &rpc2);
    TcpTransport::TcpServerRpc* serverRpc2 =
            static_cast<TcpTransport::TcpServerRpc*>(
            serviceManager->waitForRpc(1.0));

    // Attempt to send a reply, and make sure that it doesn't
    // accidentally get sent to the wrong request.
    serverRpc1->sendReply();
    EXPECT_FALSE(TestUtil::waitForRpc(&context, rpc2, 5));

    serverRpc2->sendReply();
    EXPECT_TRUE(TestUtil::waitForRpc(&context, rpc2));
}

TEST_F(TcpTransportTest, sendReply) {
    // Generate 3 requests and respond to each.  Make the first response
    // short so it can be transmitted immediately; make the next response
    // long, so it blocks; make the last response short (it should queue up
    // behind the long one).
    Transport::SessionRef session = client.getSession(locator);

    // Send requests.
    MockWrapper rpc1("request1");
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    MockWrapper rpc2("request2");
    session->sendRequest(&rpc2.request, &rpc2.response, &rpc2);
    MockWrapper rpc3("request3");
    session->sendRequest(&rpc3.request, &rpc3.response, &rpc3);

    // Send replies.
    Transport::ServerRpc* serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    serverRpc->replyPayload.fillFromString("response1");
    serverRpc->sendReply();
    serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    TestUtil::fillLargeBuffer(&serverRpc->replyPayload, 200000);
    serverRpc->sendReply();
    serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    serverRpc->replyPayload.fillFromString("response3");
    serverRpc->sendReply();

    // Check server state.
    EXPECT_NE(server.sockets.size(), 0U);
    TcpTransport::Socket* socket = server.sockets[server.sockets.size() - 1];
    EXPECT_EQ(2U, socket->rpcsWaitingToReply.size())
        << "There are no pending RPCs responses to send. You may have to "
           "increase the size of the message for this test to be effective.";
    EXPECT_EQ("~TcpServerRpc: deleted", TestLog::get());
    TestLog::reset();

    // Make sure the responses eventually get through.
    EXPECT_TRUE(TestUtil::waitForRpc(&context, rpc1));
    EXPECT_EQ("response1/0", TestUtil::toString(&rpc1.response));
    EXPECT_TRUE(TestUtil::waitForRpc(&context, rpc2));
    EXPECT_EQ("ok", TestUtil::checkLargeBuffer(&rpc2.response, 200000));
    EXPECT_TRUE(TestUtil::waitForRpc(&context, rpc3));
    EXPECT_EQ("response3/0", TestUtil::toString(&rpc3.response));
    EXPECT_EQ("~TcpServerRpc: deleted | ~TcpServerRpc: deleted",
            TestLog::get());
    EXPECT_EQ(0U, socket->rpcsWaitingToReply.size());
}

TEST_F(TcpTransportTest, sendReply_error) {
    Transport::SessionRef session = client.getSession(locator);
    MockWrapper rpc1("request1");
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    Transport::ServerRpc* serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    serverRpc->replyPayload.fillFromString("response1");

    // Arrange for an error to occur while sending the reply, and make
    // sure it is caught properly.
    sys->sendmsgErrno = EPERM;
    TcpTransport::TcpServerRpc* tcpRpc =
            static_cast<TcpTransport::TcpServerRpc*>(serverRpc);
    TcpTransport *transport = &tcpRpc->transport;
    int fd = tcpRpc->fd;
    EXPECT_NO_THROW(serverRpc->sendReply());
    EXPECT_EQ("sendMessage: TcpTransport sendmsg error: Operation not "
            "permitted | ~TcpServerRpc: deleted", TestLog::get());
    EXPECT_TRUE(transport->sockets[fd] == NULL);
}

TEST_F(TcpTransportTest, sessionAlarm) {
    TestLog::Enable _;
    TcpTransport::TcpSession* session = new TcpTransport::TcpSession(
            client, locator, 30);
    Transport::SessionRef ref = session;

    // First, let a request complete successfully, and make sure that
    // things get cleaned up well enough that a timeout doesn't occur.
    MockWrapper rpc1("request1");
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    // We do not want the session alarm timer firing unless we fire it
    // explicitly.
    context.sessionAlarmTimer->stop();
    Transport::ServerRpc* serverRpc = serviceManager->waitForRpc(1.0);
    serverRpc->replyPayload.fillFromString("response1");
    serverRpc->sendReply();
    EXPECT_TRUE(TestUtil::waitForRpc(&context, rpc1));
    for (int i = 0; i < 20; i++) {
        context.sessionAlarmTimer->handleTimerEvent();
    }
    EXPECT_NE(-1, session->fd);

    // Issue a second request, don't respond to it, and make sure it
    // times out.
    MockWrapper rpc2("request2");
    session->sendRequest(&rpc2.request, &rpc2.response, &rpc2);
    for (int i = 0; i < 20; i++) {
        context.sessionAlarmTimer->handleTimerEvent();
    }
    EXPECT_EQ(-1, session->fd);
    EXPECT_STREQ("completed: 0, failed: 1", rpc2.getState());
}

TEST_F(TcpTransportTest, TcpServerRpc_getClientServiceLocator) {
    Transport::SessionRef session = client.getSession(locator);
    TcpTransport::messageChunks = 0;
    MockWrapper rpc1("request1");
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    Transport::ServerRpc* serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "tcp:host=127\\.0\\.0\\.1,port=[0-9][0-9]*",
        serverRpc->getClientServiceLocator()));
    serverRpc->sendReply();
    EXPECT_TRUE(TestUtil::waitForRpc(&context, rpc1));
}

}  // namespace RAMCloud
