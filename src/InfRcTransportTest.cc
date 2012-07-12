/* Copyright (c) 2011-2012 Stanford University
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
#include "RpcWrapper.h"
#include "ServiceManager.h"

namespace RAMCloud {

class InfRcTransportTest : public ::testing::Test {
  public:
    Context context;
    ServiceLocator locator;
    InfRcTransport<RealInfiniband> server;
    InfRcTransport<RealInfiniband> client;

    InfRcTransportTest()
        : context()
        , locator("infrc: host=localhost, port=11000")
        , server(context, &locator)
        , client(context)
    {
    }

    ~InfRcTransportTest() {}
  private:
    DISALLOW_COPY_AND_ASSIGN(InfRcTransportTest);
};

TEST_F(InfRcTransportTest, sanityCheck) {
    // Verify that we can send a request, receive it, send a reply,
    // and receive it. Then try a second request with bigger chunks
    // of data.

    Transport::SessionRef session = client.getSession(locator);
    Buffer response;
    // Put junk in the response buffer to make sure it gets cleared properly.
    response.fillFromString("abcde");
    RpcWrapper wrapper(4, &response);

    wrapper.request.fillFromString("abcdefg");
    wrapper.testSend(session);
    Transport::ServerRpc* serverRpc =
            context.serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("abcdefg/0", TestUtil::toString(&serverRpc->requestPayload));
    EXPECT_FALSE(wrapper.isReady());
    serverRpc->replyPayload.fillFromString("klmn");
    serverRpc->sendReply();
    EXPECT_TRUE(TestUtil::waitForRpc(context, wrapper));
    EXPECT_EQ("klmn/0", TestUtil::toString(wrapper.response));

    TestUtil::fillLargeBuffer(&wrapper.request, 100000);
    wrapper.response->reset();
    wrapper.testSend(session);
    serverRpc = context.serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("ok",
            TestUtil::checkLargeBuffer(&serverRpc->requestPayload, 100000));
    TestUtil::fillLargeBuffer(&serverRpc->replyPayload, 50000);
    serverRpc->sendReply();
    EXPECT_TRUE(TestUtil::waitForRpc(context, wrapper));
    EXPECT_EQ("ok", TestUtil::checkLargeBuffer(wrapper.response, 50000));
}

TEST_F(InfRcTransportTest, InfRcSession_abort_onClientSendQueue) {
    TestLog::Enable _;

    // Arrange for 2 messages on clientSendQueue.
    Transport::SessionRef session = client.getSession(locator);
    InfRcTransport<RealInfiniband>::InfRcSession* rawSession =
            reinterpret_cast<InfRcTransport<RealInfiniband>::
            InfRcSession*>(session.get());
    RpcWrapper wrapper1(4), wrapper2(4);
    wrapper1.request.fillFromString("r1");
    wrapper2.request.fillFromString("r2");
    client.numUsedClientSrqBuffers =
            InfRcTransport<RealInfiniband>::MAX_SHARED_RX_QUEUE_DEPTH+1;
    wrapper1.testSend(session);
    wrapper2.testSend(session);
    EXPECT_EQ(2U, client.clientSendQueue.size());

    session->abort("aborted by test");
    EXPECT_EQ(0U, client.clientSendQueue.size());
    EXPECT_EQ(0, rawSession->alarm.outstandingRpcs);
    EXPECT_STREQ("FAILED", wrapper1.stateString());
    EXPECT_STREQ("FAILED", wrapper2.stateString());
}

TEST_F(InfRcTransportTest, InfRcSession_abort_onOutstandingRpcs) {
    TestLog::Enable _;

    // Arrange for 2 messages on outstandingRpcs.
    Transport::SessionRef session = client.getSession(locator);
    RpcWrapper wrapper1(4), wrapper2(4);
    wrapper1.request.fillFromString("r1");
    wrapper2.request.fillFromString("r2");
    wrapper1.testSend(session);
    wrapper2.testSend(session);
    EXPECT_EQ(2U, client.outstandingRpcs.size());

    session->abort("aborted by test");
    EXPECT_EQ(0U, client.outstandingRpcs.size());
    EXPECT_STREQ("FAILED", wrapper1.stateString());
    EXPECT_STREQ("FAILED", wrapper2.stateString());
}

TEST_F(InfRcTransportTest, InfRcSession_cancelRequest_rpcPending) {
    TestLog::Enable _;

    // Send a message, then cancel before the response is received.
    Transport::SessionRef session = client.getSession(locator);
    InfRcTransport<RealInfiniband>::InfRcSession* rawSession =
            reinterpret_cast<InfRcTransport<RealInfiniband>::
            InfRcSession*>(session.get());
    RpcWrapper wrapper(4);
    wrapper.request.fillFromString("abcdefg");
    client.numUsedClientSrqBuffers =
            InfRcTransport<RealInfiniband>::MAX_SHARED_RX_QUEUE_DEPTH+1;
    wrapper.testSend(session);
    EXPECT_EQ(1U, client.clientSendQueue.size());
    wrapper.cancel();
    EXPECT_EQ(0U, client.clientSendQueue.size());
    EXPECT_EQ(0, rawSession->alarm.outstandingRpcs);
    EXPECT_STREQ("CANCELED", wrapper.stateString());
}

TEST_F(InfRcTransportTest, InfRcSession_cancelRequest_rpcSent) {
    TestLog::Enable _;

    // Send a message, then cancel before the response is received.
    Transport::SessionRef session = client.getSession(locator);
    InfRcTransport<RealInfiniband>::InfRcSession* rawSession =
            reinterpret_cast<InfRcTransport<RealInfiniband>::
            InfRcSession*>(session.get());
    RpcWrapper wrapper(4);
    wrapper.request.fillFromString("abcdefg");
    wrapper.testSend(session);
    Transport::ServerRpc* serverRpc =
            context.serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_FALSE(wrapper.isReady());
    EXPECT_EQ(1U, client.outstandingRpcs.size());
    EXPECT_EQ(1, rawSession->alarm.outstandingRpcs);
    wrapper.cancel();
    EXPECT_EQ(0U, client.outstandingRpcs.size());
    EXPECT_EQ(0, rawSession->alarm.outstandingRpcs);
    EXPECT_STREQ("CANCELED", wrapper.stateString());

    // Send the response, and make sure it is ignored by the client.
    serverRpc->replyPayload.fillFromString("klmn");
    serverRpc->sendReply();
    TestLog::reset();

    // Make sure we can send another request and receive its response.
    wrapper.request.reset();
    wrapper.request.fillFromString("xyzzy");
    wrapper.response->reset();
    wrapper.testSend(session);
    serverRpc = context.serviceManager->waitForRpc(1.0);

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
    EXPECT_TRUE(TestUtil::waitForRpc(context, wrapper));
    EXPECT_EQ("response2/0", TestUtil::toString(wrapper.response));
}

TEST_F(InfRcTransportTest, ClientRpc_clientSend_sessionAborted) {
    Transport::SessionRef session = client.getSession(locator);
    RpcWrapper wrapper(4);
    session->abort("test abort");
    string message("no exception");
    try {
        wrapper.testSend(session);
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("test abort", message);
}

TEST_F(InfRcTransportTest, ServerRpc_getClientServiceLocator) {
    Transport::SessionRef session = client.getSession(locator);
    RpcWrapper wrapper(4);
    wrapper.testSend(session);
    Transport::ServerRpc* serverRpc =
            context.serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "infrc:host=127\\.0\\.0\\.1,port=[0-9][0-9]*",
        serverRpc->getClientServiceLocator()));
    serverRpc->sendReply();
    EXPECT_TRUE(TestUtil::waitForRpc(context, wrapper));
}

}  // namespace RAMCloud
