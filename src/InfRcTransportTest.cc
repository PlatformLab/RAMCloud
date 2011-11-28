/* Copyright (c) 2011 Stanford University
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
#include "InfRcTransport.h"
#include "ServiceManager.h"

namespace RAMCloud {

class InfRcTransportTest : public ::testing::Test {
  public:
    ServiceLocator* locator;

    InfRcTransportTest() : locator(NULL)
    {
        locator = new ServiceLocator("infrc: host=localhost, port=11000");
    }

    ~InfRcTransportTest() {
        delete locator;
    }
  private:
    DISALLOW_COPY_AND_ASSIGN(InfRcTransportTest);
};

TEST_F(InfRcTransportTest, sanityCheck) {
    // Create a server and a client and verify that we can
    // send a request, receive it, send a reply, and receive it.
    // Then try a second request with bigger chunks of data.

    InfRcTransport<RealInfiniband> server(locator);
    InfRcTransport<RealInfiniband> client;
    Transport::SessionRef session = client.getSession(*locator);

    Buffer request;
    Buffer reply;
    request.fillFromString("abcdefg");
    Transport::ClientRpc* clientRpc = session->clientSend(&request,
            &reply);
    Transport::ServerRpc* serverRpc =
            Context::get().serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("abcdefg/0", TestUtil::toString(&serverRpc->requestPayload));
    EXPECT_FALSE(clientRpc->isReady());
    serverRpc->replyPayload.fillFromString("klmn");
    serverRpc->sendReply();
    EXPECT_TRUE(TestUtil::waitForRpc(*clientRpc));
    EXPECT_EQ("klmn/0", TestUtil::toString(&reply));

    TestUtil::fillLargeBuffer(&request, 100000);
    reply.reset();
    clientRpc = session->clientSend(&request, &reply);
    serverRpc = Context::get().serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("ok",
            TestUtil::checkLargeBuffer(&serverRpc->requestPayload, 100000));
    TestUtil::fillLargeBuffer(&serverRpc->replyPayload, 50000);
    serverRpc->sendReply();
    clientRpc->wait();
    EXPECT_EQ("ok", TestUtil::checkLargeBuffer(&reply, 50000));
}

TEST_F(InfRcTransportTest, InfRcSession_abort_onClientSendQueue) {
    TestLog::Enable _;
    // Create a server and a client.
    InfRcTransport<RealInfiniband> server(locator);
    InfRcTransport<RealInfiniband> client;
    InfRcTransport<RealInfiniband>::InfRcSession session(&client, *locator, 0);

    // Arrange for 2 messages on clientSendQueue.
    Buffer request1, request2;
    Buffer reply1, reply2;
    request1.fillFromString("r1");
    request2.fillFromString("r2");
    client.numUsedClientSrqBuffers =
            InfRcTransport<RealInfiniband>::MAX_SHARED_RX_QUEUE_DEPTH+1;
    Transport::ClientRpc* clientRpc1 = session.clientSend(&request1,
            &reply1);
    Transport::ClientRpc* clientRpc2 = session.clientSend(&request2,
            &reply2);
    EXPECT_EQ(2U, client.clientSendQueue.size());

    session.abort("aborted by test");
    EXPECT_EQ(0U, client.clientSendQueue.size());
    EXPECT_EQ(0, session.alarm.outstandingRpcs);
    EXPECT_TRUE(clientRpc1->isReady());
    EXPECT_TRUE(clientRpc2->isReady());
    string message("no exception");
    try {
        clientRpc1->wait();
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("null RPC cancelled: aborted by test", message);
}

TEST_F(InfRcTransportTest, InfRcSession_abort_onOutstandingRpcs) {
    TestLog::Enable _;
    // Create a server and a client.
    InfRcTransport<RealInfiniband> server(locator);
    InfRcTransport<RealInfiniband> client;
    InfRcTransport<RealInfiniband>::InfRcSession session(&client, *locator, 0);

    // Arrange for 2 messages on outstandingRpcs.
    Buffer request1, request2;
    Buffer reply1, reply2;
    request1.fillFromString("r1");
    request2.fillFromString("r2");
    Transport::ClientRpc* clientRpc1 = session.clientSend(&request1,
            &reply1);
    Transport::ClientRpc* clientRpc2 = session.clientSend(&request2,
            &reply2);
    EXPECT_EQ(2U, client.outstandingRpcs.size());

    session.abort("aborted by test");
    EXPECT_EQ(0U, client.outstandingRpcs.size());
    EXPECT_TRUE(clientRpc1->isReady());
    EXPECT_TRUE(clientRpc2->isReady());
    string message("no exception");
    try {
        clientRpc1->wait();
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("null RPC cancelled: aborted by test", message);
}

TEST_F(InfRcTransportTest, ClientRpc_cancelCleanup_rpcPending) {
    TestLog::Enable _;
    // Create a server and a client.
    InfRcTransport<RealInfiniband> server(locator);
    InfRcTransport<RealInfiniband> client;
    InfRcTransport<RealInfiniband>::InfRcSession session(&client, *locator, 0);

    // Send a message, then cancel before the response is received.
    Buffer request;
    Buffer reply;
    request.fillFromString("abcdefg");
    client.numUsedClientSrqBuffers =
            InfRcTransport<RealInfiniband>::MAX_SHARED_RX_QUEUE_DEPTH+1;
    Transport::ClientRpc* clientRpc = session.clientSend(&request,
            &reply);
    EXPECT_EQ(1U, client.clientSendQueue.size());
    clientRpc->cancel("cancelled by test");
    EXPECT_EQ(0U, client.clientSendQueue.size());
    EXPECT_EQ(0, session.alarm.outstandingRpcs);
    EXPECT_TRUE(clientRpc->isReady());
    string message("no exception");
    try {
        clientRpc->wait();
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("unknown(25185) RPC cancelled: cancelled by test", message);
}

TEST_F(InfRcTransportTest, ClientRpc_cancelCleanup_rpcSent) {
    TestLog::Enable _;
    // Create a server and a client.
    InfRcTransport<RealInfiniband> server(locator);
    InfRcTransport<RealInfiniband> client;
    InfRcTransport<RealInfiniband>::InfRcSession session(&client, *locator, 0);

    // Send a message, then cancel before the response is received.
    Buffer request;
    Buffer reply;
    request.fillFromString("abcdefg");
    Transport::ClientRpc* clientRpc = session.clientSend(&request,
            &reply);
    Transport::ServerRpc* serverRpc =
            Context::get().serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_FALSE(clientRpc->isReady());
    EXPECT_EQ(1U, client.outstandingRpcs.size());
    EXPECT_EQ(1, session.alarm.outstandingRpcs);
    clientRpc->cancel();
    EXPECT_EQ(0U, client.outstandingRpcs.size());
    EXPECT_EQ(0, session.alarm.outstandingRpcs);
    EXPECT_TRUE(clientRpc->isReady());
    string message("no exception");
    try {
        clientRpc->wait();
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("unknown(25185) RPC cancelled", message);

    // Send the response, and make sure it is ignored by the client.
    serverRpc->replyPayload.fillFromString("klmn");
    serverRpc->sendReply();
    TestLog::reset();

    // Make sure we can send another request and receive its response.
    request.reset();
    request.fillFromString("xyzzy");
    reply.reset();
    clientRpc = session.clientSend(&request, &reply);
    serverRpc = Context::get().serviceManager->waitForRpc(1.0);

    // Note: the log entry for the unrecognized response to the canceled
    // RPC only appears here (InfRc doesn't check for responses unless
    // there are active RPCs).
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
                " incoming data doesn't match active RPC (nonce .*)",
                TestLog::get()));
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("xyzzy/0", TestUtil::toString(&serverRpc->requestPayload));
    serverRpc->replyPayload.fillFromString("response2");
    serverRpc->sendReply();
    clientRpc->wait();
    EXPECT_EQ("response2/0", TestUtil::toString(&reply));
}

TEST_F(InfRcTransportTest, ClientRpc_clientSend_sessionAborted) {
    InfRcTransport<RealInfiniband> server(locator);
    InfRcTransport<RealInfiniband> client;
    InfRcTransport<RealInfiniband>::InfRcSession session(&client, *locator, 0);
    session.abort("test abort");
    Buffer request;
    Buffer reply;
    string message("no exception");
    try {
        Transport::ClientRpc* clientRpc = session.clientSend(&request, &reply);
        // Must use clientRpc to keep compiler happy.
        clientRpc->isReady();
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("test abort", message);
}

}  // namespace RAMCloud
