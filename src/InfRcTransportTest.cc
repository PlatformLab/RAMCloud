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
        delete serviceManager;
        ServiceManager::init();
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
    Transport::ServerRpc* serverRpc = serviceManager->waitForRpc(1.0);
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
    serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("ok",
            TestUtil::checkLargeBuffer(&serverRpc->requestPayload, 100000));
    TestUtil::fillLargeBuffer(&serverRpc->replyPayload, 50000);
    serverRpc->sendReply();
    clientRpc->wait();
    EXPECT_EQ("ok", TestUtil::checkLargeBuffer(&reply, 50000));
}

TEST_F(InfRcTransportTest, ClientRpc_cancelCleanup) {
    TestLog::Enable _;
    // Create a server and a client.
    InfRcTransport<RealInfiniband> server(locator);
    InfRcTransport<RealInfiniband> client;
    Transport::SessionRef session = client.getSession(*locator);

    // Send a message, then cancel before the response is received.
    Buffer request;
    Buffer reply;
    request.fillFromString("abcdefg");
    Transport::ClientRpc* clientRpc = session->clientSend(&request,
            &reply);
    Transport::ServerRpc* serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_FALSE(clientRpc->isReady());
    EXPECT_EQ(1U, client.outstandingRpcs.size());
    clientRpc->cancel();
    EXPECT_EQ(0U, client.outstandingRpcs.size());
    EXPECT_TRUE(clientRpc->isReady());
    string message("no exception");
    try {
        clientRpc->wait();
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("RPC cancelled", message);

    // Send the response, and make sure it is ignored by the client.
    serverRpc->replyPayload.fillFromString("klmn");
    serverRpc->sendReply();
    TestLog::reset();

    // Make sure we can send another request and receive its response.
    request.reset();
    request.fillFromString("xyzzy");
    reply.reset();
    clientRpc = session->clientSend(&request, &reply);
    serverRpc = serviceManager->waitForRpc(1.0);

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

}  // namespace RAMCloud
