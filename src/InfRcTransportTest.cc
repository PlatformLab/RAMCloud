/* Copyright (c) 2011-2015 Stanford University
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
#include "MockWrapper.h"
#include "StringUtil.h"
#include "WorkerManager.h"

namespace RAMCloud {

class InfRcTransportTest : public ::testing::Test {
  public:
    Context context;
    ServiceLocator locator;
    InfRcTransport server;
    InfRcTransport client;

    InfRcTransportTest()
        : context()
        , locator("infrc: host=localhost, port=11000")
        , server(&context, &locator)
        , client(&context)
    {
        context.workerManager = new WorkerManager(&context);
        context.workerManager->testingSaveRpcs = 1;
    }

    ~InfRcTransportTest() {}
  private:
    DISALLOW_COPY_AND_ASSIGN(InfRcTransportTest);
};

TEST_F(InfRcTransportTest, sanityCheck) {
    // Verify that we can send a request, receive it, send a reply,
    // and receive it. Then try a second request with bigger chunks
    // of data.
    Transport::SessionRef session = client.getSession(&locator);
    MockWrapper rpc("abcdefg");
    // Put junk in the response buffer to make sure it gets cleared properly.
    rpc.response.fillFromString("abcde");
    session->sendRequest(&rpc.request, &rpc.response, &rpc);
    Transport::ServerRpc* serverRpc =
            context.workerManager->waitForRpc(1.0);
    ASSERT_TRUE(serverRpc != NULL);
    EXPECT_EQ("abcdefg", TestUtil::toString(&serverRpc->requestPayload));
    EXPECT_STREQ("completed: 0, failed: 0", rpc.getState());
    serverRpc->replyPayload.fillFromString("klmn");
    serverRpc->sendReply();
    EXPECT_TRUE(TestUtil::waitForRpc(&context, rpc));
    EXPECT_STREQ("completed: 1, failed: 0", rpc.getState());
    EXPECT_EQ("klmn/0", TestUtil::toString(&rpc.response));

    rpc.reset();
    TestUtil::fillLargeBuffer(&rpc.request, 100000);
    session->sendRequest(&rpc.request, &rpc.response, &rpc);
    serverRpc = context.workerManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("ok",
            TestUtil::checkLargeBuffer(&serverRpc->requestPayload, 100000));
    TestUtil::fillLargeBuffer(&serverRpc->replyPayload, 50000);
    serverRpc->sendReply();
    EXPECT_TRUE(TestUtil::waitForRpc(&context, rpc));
    EXPECT_EQ("ok", TestUtil::checkLargeBuffer(&rpc.response, 50000));
}

namespace {
bool sendZeroCopyFilter(string s) {
    return s == "sendZeroCopy";
}
}

TEST_F(InfRcTransportTest, ClientRpc_sendZeroCopy) {
    Transport::SessionRef session = client.getSession(&locator);
    client.testingDontReallySend = true;
    MockWrapper rpc1("r1");
    MockWrapper rpc2("r2");
    TestLog::Enable _(sendZeroCopyFilter);
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    EXPECT_EQ("sendZeroCopy: isge[0]: 10 bytes COPIED", TestLog::get());
    TestLog::reset();
    session->sendRequest(&rpc2.request, &rpc2.response, &rpc2);
    EXPECT_EQ("sendZeroCopy: isge[0]: 10 bytes COPIED", TestLog::get());

    void* page = Memory::xmemalign(HERE, getpagesize(), getpagesize());
    client.registerMemory(page, getpagesize());

    rpc1.request.appendExternal(page, getpagesize());
    TestLog::reset();
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    EXPECT_EQ("sendZeroCopy: isge[0]: 10 bytes COPIED | "
              "sendZeroCopy: isge[1]: 4096 bytes ZERO-COPY", TestLog::get());

    rpc1.request.appendExternal("other", 5);
    TestLog::reset();
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    EXPECT_EQ("sendZeroCopy: isge[0]: 10 bytes COPIED | "
              "sendZeroCopy: isge[1]: 4096 bytes ZERO-COPY | "
              "sendZeroCopy: isge[2]: 5 bytes COPIED", TestLog::get());

    rpc1.request.appendExternal("more", 5);
    TestLog::reset();
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    EXPECT_EQ("sendZeroCopy: isge[0]: 10 bytes COPIED | "
              "sendZeroCopy: isge[1]: 4096 bytes ZERO-COPY | "
              "sendZeroCopy: isge[2]: 10 bytes COPIED", TestLog::get());

    rpc1.request.appendExternal(page, getpagesize());
    TestLog::reset();
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    EXPECT_EQ("sendZeroCopy: isge[0]: 10 bytes COPIED | "
              "sendZeroCopy: isge[1]: 4096 bytes ZERO-COPY | "
              "sendZeroCopy: isge[2]: 10 bytes COPIED | "
              "sendZeroCopy: isge[3]: 4096 bytes ZERO-COPY", TestLog::get());

    free(page);
}

TEST_F(InfRcTransportTest, InfRcSession_abort_onClientSendQueue) {
    TestLog::Enable _;

    // Arrange for 2 messages on clientSendQueue.
    Transport::SessionRef session = client.getSession(&locator);
    InfRcTransport::InfRcSession* rawSession =
            reinterpret_cast<InfRcTransport::InfRcSession*>(session.get());
    MockWrapper rpc1("r1");
    MockWrapper rpc2("r2");
    client.numUsedClientSrqBuffers =
            InfRcTransport::MAX_SHARED_RX_QUEUE_DEPTH+1;
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    session->sendRequest(&rpc2.request, &rpc2.response, &rpc2);
    EXPECT_EQ(2U, client.clientSendQueue.size());

    session->abort();
    EXPECT_EQ(0U, client.clientSendQueue.size());
    EXPECT_EQ(0, rawSession->sessionAlarm.outstandingRpcs);
    EXPECT_STREQ("completed: 0, failed: 1", rpc1.getState());
    EXPECT_STREQ("completed: 0, failed: 1", rpc2.getState());
}

TEST_F(InfRcTransportTest, InfRcSession_abort_onOutstandingRpcs) {
    TestLog::Enable _;

    // Arrange for 2 messages on outstandingRpcs.
    Transport::SessionRef session = client.getSession(&locator);
    MockWrapper rpc1("r1");
    MockWrapper rpc2("r2");
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    session->sendRequest(&rpc2.request, &rpc2.response, &rpc2);
    EXPECT_EQ(2U, client.outstandingRpcs.size());

    session->abort();
    EXPECT_EQ(0U, client.outstandingRpcs.size());
    EXPECT_STREQ("completed: 0, failed: 1", rpc1.getState());
    EXPECT_STREQ("completed: 0, failed: 1", rpc2.getState());
}

TEST_F(InfRcTransportTest, InfRcSession_cancelRequest_rpcPending) {
    TestLog::Enable _;

    // Send a message, then cancel before the response is received.
    Transport::SessionRef session = client.getSession(&locator);
    InfRcTransport::InfRcSession* rawSession =
            reinterpret_cast<InfRcTransport::InfRcSession*>(session.get());
    MockWrapper rpc("abcdefg");
    client.numUsedClientSrqBuffers =
            InfRcTransport::MAX_SHARED_RX_QUEUE_DEPTH+1;
    session->sendRequest(&rpc.request, &rpc.response, &rpc);
    EXPECT_EQ(1U, client.clientSendQueue.size());
    session->cancelRequest(&rpc);
    EXPECT_EQ(0U, client.clientSendQueue.size());
    EXPECT_EQ(0, rawSession->sessionAlarm.outstandingRpcs);
    // (cancelRequest doesn't call either completed or failed)
    EXPECT_STREQ("completed: 0, failed: 0", rpc.getState());
}

TEST_F(InfRcTransportTest, InfRcSession_cancelRequest_rpcSent) {
    TestLog::Enable _;

    // Send a message, then cancel before the response is received.
    Transport::SessionRef session = client.getSession(&locator);
    InfRcTransport::InfRcSession* rawSession =
            reinterpret_cast<InfRcTransport::InfRcSession*>(session.get());
    MockWrapper rpc("abcdefg");
    session->sendRequest(&rpc.request, &rpc.response, &rpc);
    Transport::ServerRpc* serverRpc =
            context.workerManager->waitForRpc(1.0);
    ASSERT_TRUE(serverRpc != NULL);
    EXPECT_STREQ("completed: 0, failed: 0", rpc.getState());
    EXPECT_EQ(1U, client.outstandingRpcs.size());
    EXPECT_EQ(1u, client.numUsedClientSrqBuffers);
    EXPECT_EQ(1, rawSession->sessionAlarm.outstandingRpcs);
    uint32_t totalTxBuffers = InfRcTransport::MAX_TX_QUEUE_DEPTH;
    EXPECT_EQ(totalTxBuffers - 1, client.freeTxBuffers.size());
    session->cancelRequest(&rpc);
    EXPECT_EQ(totalTxBuffers, client.freeTxBuffers.size());
    EXPECT_EQ(0U, client.outstandingRpcs.size());
    EXPECT_EQ(0u, client.numUsedClientSrqBuffers);
    EXPECT_EQ(0, rawSession->sessionAlarm.outstandingRpcs);

    // Send the response, and make sure it is ignored by the client.
    serverRpc->replyPayload.fillFromString("klmn");
    serverRpc->sendReply();
    TestLog::reset();

    // Make sure we can send another request and receive its response.
    rpc.reset();
    rpc.request.reset();
    rpc.request.fillFromString("xyzzy");
    session->sendRequest(&rpc.request, &rpc.response, &rpc);
    serverRpc = context.workerManager->waitForRpc(1.0);

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
    EXPECT_TRUE(TestUtil::waitForRpc(&context, rpc));
    EXPECT_EQ("response2/0", TestUtil::toString(&rpc.response));
}

TEST_F(InfRcTransportTest, getRpcInfo) {
    TestLog::Enable _;
    Transport::SessionRef session = client.getSession(&locator);
    InfRcTransport::InfRcSession* rawSession =
            reinterpret_cast<InfRcTransport::InfRcSession*>(session.get());

    EXPECT_EQ("no active RPCs to server at infrc: host=localhost, port=11000",
            rawSession-> getRpcInfo());

    // Arrange for 1 message on outstandingRpcs and 2 messages on
    // clientSendQueue.
    MockWrapper rpc1;
    rpc1.setOpcode(WireFormat::READ);
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    EXPECT_EQ(1U, client.outstandingRpcs.size());

    client.numUsedClientSrqBuffers =
            InfRcTransport::MAX_SHARED_RX_QUEUE_DEPTH+1;
    MockWrapper rpc2;
    rpc2.setOpcode(WireFormat::REMOVE);
    session->sendRequest(&rpc2.request, &rpc2.response, &rpc2);
    MockWrapper rpc3;
    rpc3.setOpcode(WireFormat::INCREMENT);
    session->sendRequest(&rpc3.request, &rpc3.response, &rpc3);
    EXPECT_EQ(2U, client.clientSendQueue.size());

    EXPECT_EQ("READ, REMOVE, INCREMENT to server at infrc: host=localhost, "
            "port=11000", rawSession-> getRpcInfo());
}

TEST_F(InfRcTransportTest, ClientRpc_sendRequest_sessionAborted) {
    Transport::SessionRef session = client.getSession(&locator);
    MockWrapper rpc;
    session->abort();
    session->sendRequest(&rpc.request, &rpc.response, &rpc);
    EXPECT_STREQ("completed: 0, failed: 1", rpc.getState());
}

TEST_F(InfRcTransportTest, ServerRpc_getClientServiceLocator) {
    Transport::SessionRef session = client.getSession(&locator);
    MockWrapper rpc("request");
    session->sendRequest(&rpc.request, &rpc.response, &rpc);
    Transport::ServerRpc* serverRpc =
            context.workerManager->waitForRpc(1.0);
    ASSERT_TRUE(serverRpc != NULL);
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "infrc:host=127\\.0\\.0\\.1,port=[0-9][0-9]*",
        serverRpc->getClientServiceLocator()));
    serverRpc->sendReply();
    EXPECT_TRUE(TestUtil::waitForRpc(&context, rpc));
}

}  // namespace RAMCloud
