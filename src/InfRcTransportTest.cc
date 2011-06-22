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
    EXPECT_EQ("abcdefg/0", toString(&serverRpc->requestPayload));
    EXPECT_FALSE(clientRpc->isReady());
    serverRpc->replyPayload.fillFromString("klmn");
    serverRpc->sendReply();
    EXPECT_TRUE(waitForRpc(*clientRpc));
    EXPECT_EQ("klmn/0", toString(&reply));

    fillLargeBuffer(&request, 100000);
    reply.reset();
    clientRpc = session->clientSend(&request, &reply);
    serverRpc = serviceManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("ok",
            checkLargeBuffer(&serverRpc->requestPayload, 100000));
    fillLargeBuffer(&serverRpc->replyPayload, 50000);
    serverRpc->sendReply();
    clientRpc->wait();
    EXPECT_EQ("ok", checkLargeBuffer(&reply, 50000));
}

}  // namespace RAMCloud
