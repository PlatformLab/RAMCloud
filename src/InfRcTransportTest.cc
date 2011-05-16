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

class InfRcTransportTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(InfRcTransportTest);
    CPPUNIT_TEST(test_sanityCheck);
    CPPUNIT_TEST_SUITE_END();

  public:
    ServiceLocator* locator;
    ServiceManager* serviceManager;

    InfRcTransportTest() : locator(NULL), serviceManager(NULL)
    {}

    void setUp() {
        locator = new ServiceLocator("infrc: host=localhost, port=11000");
        serviceManager = new ServiceManager(NULL);
    }

    void tearDown() {
        delete serviceManager;
        delete locator;
    }

    void test_sanityCheck() {
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
        CPPUNIT_ASSERT(serverRpc != NULL);
        CPPUNIT_ASSERT_EQUAL("abcdefg/0", toString(&serverRpc->recvPayload));
        CPPUNIT_ASSERT_EQUAL(false, clientRpc->isReady());
        serverRpc->replyPayload.fillFromString("klmn");
        serverRpc->sendReply();
        Dispatch::handleEvent();
        CPPUNIT_ASSERT_EQUAL(true, clientRpc->isReady());
        CPPUNIT_ASSERT_EQUAL("klmn/0", toString(&reply));

        fillLargeBuffer(&request, 100000);
        reply.reset();
        clientRpc = session->clientSend(&request, &reply);
        serverRpc = serviceManager->waitForRpc(1.0);
        CPPUNIT_ASSERT(serverRpc != NULL);
        CPPUNIT_ASSERT_EQUAL("ok",
                checkLargeBuffer(&serverRpc->recvPayload, 100000));
        fillLargeBuffer(&serverRpc->replyPayload, 50000);
        serverRpc->sendReply();
        clientRpc->wait();
        CPPUNIT_ASSERT_EQUAL("ok", checkLargeBuffer(&reply, 50000));
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(InfRcTransportTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(InfRcTransportTest);

}  // namespace RAMCloud
