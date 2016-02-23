/* Copyright (c) 2015-2016 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.xx
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
#include "BasicTransport.h"
#include "MockDriver.h"
#include "MockWrapper.h"
#include "UdpDriver.h"
#include "WorkerManager.h"

namespace RAMCloud {

class BasicTransportTest : public ::testing::Test {
  public:
    Context context;
    MockDriver* driver;
    ServiceLocator locator1;
    ServiceLocator locator2;
    BasicTransport transport;
    MockDriver::MockAddress address1;
    MockDriver::MockAddress address2;
    Transport::SessionRef sessionRef;
    BasicTransport::Session* session;
    TestLog::Enable logEnabler;

    BasicTransportTest()
        : context(false)
        , driver(new MockDriver(BasicTransport::headerToString))
        , locator1("mock:node=1")
        , locator2("mock:node=2")
        , transport(&context, NULL, driver, 666)
        , address1(&locator1)
        , address2(&locator2)
        , sessionRef(transport.getSession(&locator1))
        , session(static_cast<BasicTransport::Session*>(sessionRef.get()))
        , logEnabler()
    {
        context.workerManager = new WorkerManager(&context, 5);
        context.workerManager->testingSaveRpcs = 1;
    }

    ~BasicTransportTest()
    {
    }

    // Convenience method: receive request, prepare response, but don't
    // call sendReply yet.
    BasicTransport::ServerRpc*
    prepareToRespond()
    {
        driver->receivePacket("mock:client=1",
                BasicTransport::AllDataHeader(BasicTransport::RpcId(100, 101),
                BasicTransport::FROM_CLIENT, 8), "message1");
        BasicTransport::ServerRpc* serverRpc =
                static_cast<BasicTransport::ServerRpc*>(
                context.workerManager->waitForRpc(0));
        EXPECT_TRUE(serverRpc != NULL);
        serverRpc->replyPayload.appendCopy("0123456789abcdefghij", 20);
        return serverRpc;
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(BasicTransportTest);
};

TEST_F(BasicTransportTest, sanityCheck) {
    // Create a server and a client and verify that we can
    // send a request, receive it, send a reply, and receive it.
    // Then try a second request with bigger chunks of data.

    ServiceLocator serverLocator("basic+udp: host=localhost, port=11101");
    UdpDriver* serverDriver = new UdpDriver(&context, &serverLocator);
    BasicTransport server(&context, &serverLocator, serverDriver, 1);
    UdpDriver* clientDriver = new UdpDriver(&context);
    BasicTransport client(&context, NULL, clientDriver, 2);
    Transport::SessionRef session = client.getSession(&serverLocator);

    MockWrapper rpc1("abcdefg");
    session->sendRequest(&rpc1.request, &rpc1.response, &rpc1);
    Transport::ServerRpc* serverRpc =
        context.workerManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("abcdefg", TestUtil::toString(&serverRpc->requestPayload));
    EXPECT_STREQ("completed: 0, failed: 0", rpc1.getState());
    serverRpc->replyPayload.fillFromString("klmn");
    serverRpc->sendReply();
    EXPECT_TRUE(TestUtil::waitForRpc(&context, rpc1));
    EXPECT_STREQ("completed: 1, failed: 0", rpc1.getState());
    EXPECT_EQ("klmn/0", TestUtil::toString(&rpc1.response));

    MockWrapper rpc2;
    TestUtil::fillLargeBuffer(&rpc2.request, 100000);
    session->sendRequest(&rpc2.request, &rpc2.response, &rpc2);
    serverRpc = context.workerManager->waitForRpc(1.0);
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("ok",
          TestUtil::checkLargeBuffer(&serverRpc->requestPayload, 100000));
    TestUtil::fillLargeBuffer(&serverRpc->replyPayload, 50000);
    serverRpc->sendReply();
    EXPECT_TRUE(TestUtil::waitForRpc(&context, rpc2));
    EXPECT_EQ("ok", TestUtil::checkLargeBuffer(&rpc2.response, 50000));
}

TEST_F(BasicTransportTest, constructor) {
    EXPECT_EQ(31602u, transport.roundTripBytes);
    EXPECT_TRUE(transport.timer.isRunning());
}

TEST_F(BasicTransportTest, getRoundTripBytes_basics) {
    transport.maxDataPerPacket = 1500;
    ServiceLocator locator("mock:gbs=8,rttMicros=2");
    EXPECT_EQ(3000u, transport.getRoundTripBytes(&locator));
}
TEST_F(BasicTransportTest, getRoundTripBytes_noGbsOption) {
    transport.maxDataPerPacket = 100;
    ServiceLocator locator("mock:rttMicros=4");
    EXPECT_EQ(5000u, transport.getRoundTripBytes(&locator));
}
TEST_F(BasicTransportTest, getRoundTripBytes_bogusGbsOption) {
    transport.maxDataPerPacket = 100;
    ServiceLocator locator("mock:gbs=xyz,rttMicros=4");
    TestLog::reset();
    EXPECT_EQ(5000u, transport.getRoundTripBytes(&locator));
    EXPECT_EQ("getRoundTripBytes: Bad BasicTransport gbs option value 'xyz' "
            "(expected positive integer); ignoring option",
            TestLog::get());

    ServiceLocator locator2("mock:gbs=99foo,rttMicros=4");
    TestLog::reset();
    EXPECT_EQ(5000u, transport.getRoundTripBytes(&locator2));
    EXPECT_EQ("getRoundTripBytes: Bad BasicTransport gbs option value '99foo' "
            "(expected positive integer); ignoring option",
            TestLog::get());
}
TEST_F(BasicTransportTest, getRoundTripBytes_noRttOption) {
    transport.maxDataPerPacket = 100;
    ServiceLocator locator("mock:gbs=8");
    EXPECT_EQ(25000u, transport.getRoundTripBytes(&locator));
}
TEST_F(BasicTransportTest, getRoundTripBytes_bogusRttOption) {
    transport.maxDataPerPacket = 100;
    ServiceLocator locator("mock:gbs=8,rttMicros=xyz");
    TestLog::reset();
    EXPECT_EQ(25000u, transport.getRoundTripBytes(&locator));
    EXPECT_EQ("getRoundTripBytes: Bad BasicTransport rttMicros option value "
            "'xyz' (expected positive integer); ignoring option",
            TestLog::get());

    ServiceLocator locator2("mock:gbs=8,rttMicros=5zzz");
    TestLog::reset();
    EXPECT_EQ(25000u, transport.getRoundTripBytes(&locator2));
    EXPECT_EQ("getRoundTripBytes: Bad BasicTransport rttMicros option value "
            "'5zzz' (expected positive integer); ignoring option",
            TestLog::get());
}
TEST_F(BasicTransportTest, getRoundTripBytes_roundUpToEvenPackets) {
    transport.maxDataPerPacket = 100;
    ServiceLocator locator("mock:gbs=1,rttMicros=9");
    EXPECT_EQ(1200u, transport.getRoundTripBytes(&locator));
}

TEST_F(BasicTransportTest, sendBytes_adjustLength) {
    Buffer buffer;
    buffer.append("abcdefghijklmno", 15);
    transport.sendBytes(&address1, BasicTransport::RpcId(5, 6), &buffer,
            5, 100, BasicTransport::FROM_SERVER);
    EXPECT_EQ("DATA FROM_SERVER, rpcId 5.6, totalLength 15, offset 5 "
            "fghijklmno",
            driver->outputLog);
}
TEST_F(BasicTransportTest, sendBytes_lengthZero) {
    Buffer buffer;
    buffer.append("abcdefghijklmnop", 16);
    transport.sendBytes(&address1, BasicTransport::RpcId(5, 6), &buffer,
            5, 0, BasicTransport::FROM_SERVER);
    transport.sendBytes(&address1, BasicTransport::RpcId(5, 6), &buffer,
            50, 5, BasicTransport::FROM_SERVER);
    EXPECT_EQ("", driver->outputLog);
}
TEST_F(BasicTransportTest, sendBytes_shortMessage) {
    Buffer buffer;
    buffer.append("abcdefghijklmno", 15);
    transport.sendBytes(&address1, BasicTransport::RpcId(5, 6), &buffer,
            0, 16, BasicTransport::FROM_CLIENT);
    EXPECT_EQ("ALL_DATA FROM_CLIENT, rpcId 5.6 abcdefghij (+5 more)",
            driver->outputLog);
}
TEST_F(BasicTransportTest, sendBytes_multiplePackets) {
    // Also test the flags argument in this test.
    transport.maxDataPerPacket = 6;
    Buffer buffer;
    buffer.append("abcdefghijklmno", 15);
    transport.sendBytes(&address1, BasicTransport::RpcId(5, 6), &buffer,
            1, 15, BasicTransport::RETRANSMISSION|BasicTransport::FROM_SERVER);
    EXPECT_EQ(
            "DATA FROM_SERVER, rpcId 5.6, totalLength 15, offset 1, "
            "RETRANSMISSION bcdefg | "
            "DATA FROM_SERVER, rpcId 5.6, totalLength 15, offset 7, "
            "RETRANSMISSION hijklm | "
            "DATA FROM_SERVER, rpcId 5.6, totalLength 15, offset 13, "
            "RETRANSMISSION no",
            driver->outputLog);
}
TEST_F(BasicTransportTest, sendBytes_needGrant) {
    transport.maxDataPerPacket = 10;
    Buffer buffer;
    buffer.append("abcdefghijklmno", 15);
    transport.sendBytes(&address1, BasicTransport::RpcId(5, 6), &buffer,
            1, 14, BasicTransport::FROM_CLIENT);
    EXPECT_EQ(
            "DATA FROM_CLIENT, rpcId 5.6, totalLength 15, offset 1 "
            "bcdefghijk | "
            "DATA FROM_CLIENT, rpcId 5.6, totalLength 15, offset 11 lmno",
            driver->outputLog);
    driver->outputLog.clear();
    transport.sendBytes(&address1, BasicTransport::RpcId(5, 6), &buffer,
            1, 13, BasicTransport::FROM_CLIENT);
    EXPECT_EQ(
            "DATA FROM_CLIENT, rpcId 5.6, totalLength 15, offset 1, "
            "NEED_GRANT bcdefghijk | "
            "DATA FROM_CLIENT, rpcId 5.6, totalLength 15, offset 11, "
            "NEED_GRANT lmn",
            driver->outputLog);
}

TEST_F(BasicTransportTest, Session_constructor) {
    ServiceLocator locator("basic+udp: host=localhost, port=11101");
    UdpDriver* driver2 = new UdpDriver(&context, &locator);
    BasicTransport transport2(&context, &locator, driver2, 1);
    string exceptionMessage("no exception");
    try {
        ServiceLocator bogusLocator("bogus:foo=bar");
        TestLog::reset();
        transport2.getSession(&bogusLocator);
    } catch (TransportException& e) {
        exceptionMessage = e.message;
    }
    EXPECT_EQ("BasicTransport couldn't parse service locator",
            exceptionMessage);
    EXPECT_EQ("Session: Service locator 'bogus:foo=bar' couldn't be "
            "converted to IP address: The option with key 'host' was "
            "not found in the ServiceLocator.", TestLog::get());
}

TEST_F(BasicTransportTest, Session_destructor) {
    Transport::RpcNotifier notifier1, notifier2;
    Buffer request1, request2;
    Buffer response1, response2;
    request1.append("message1", 8);
    request1.append("message2", 8);
    session->sendRequest(&request1, &response1, &notifier1);
    session->sendRequest(&request2, &response2, &notifier2);
    EXPECT_EQ(2u, transport.outgoingRpcs.size());
    session->abort();
    EXPECT_EQ(0u, transport.outgoingRpcs.size());
    EXPECT_EQ(0u, transport.clientRpcPool.outstandingObjects);
}

// Session::abort already tested by Session destructor above.

TEST_F(BasicTransportTest, Session_cancelRequest) {
    Transport::RpcNotifier notifier1, notifier2;
#define NUM_RPCS 10
    Transport::RpcNotifier notifiers[NUM_RPCS];
    Buffer requests[NUM_RPCS];
    Buffer responses[NUM_RPCS];
    for (int i = 0; i < NUM_RPCS; i++) {
        char message[100];
        snprintf(message, sizeof(message), "message%d", i);
        requests[i].append(message, downCast<uint32_t>(strlen(message)));
        session->sendRequest(&requests[i], &responses[i], &notifiers[i]);
    }
    EXPECT_EQ(10u, transport.outgoingRpcs.size());
    session->cancelRequest(&notifiers[7]);
    session->cancelRequest(&notifiers[9]);
    session->cancelRequest(&notifiers[0]);
    session->cancelRequest(&notifiers[2]);
    EXPECT_EQ(6u, transport.outgoingRpcs.size());
    EXPECT_EQ(6u, transport.clientRpcPool.outstandingObjects);
}

TEST_F(BasicTransportTest, Session_getRpcInfo) {
    Transport::RpcNotifier notifier1, notifier2;
    WireFormat::RequestCommon header1 = {WireFormat::PING, 0};
    WireFormat::RequestCommon header2 = {WireFormat::READ, 0};
    Buffer request1, request2;
    Buffer response1, response2;
    request1.appendCopy(&header1);
    request2.appendCopy(&header2);
    EXPECT_EQ("no active RPCs to server at mock:node=1",
            session->getRpcInfo());
    session->sendRequest(&request1, &response1, &notifier1);
    session->sendRequest(&request2, &response2, &notifier2);
    EXPECT_EQ("PING, READ to server at mock:node=1", session->getRpcInfo());
}

TEST_F(BasicTransportTest, Session_sendRequest_aborted) {
    MockWrapper wrapper("message1");
    session->abort();
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    EXPECT_EQ("", driver->outputLog);
}
TEST_F(BasicTransportTest, Session_sendRequest_normal) {
    MockWrapper wrapper1("message1");
    MockWrapper wrapper2("message2");
    session->sendRequest(&wrapper1.request, &wrapper1.response, &wrapper1);
    session->sendRequest(&wrapper2.request, &wrapper2.response, &wrapper2);
    EXPECT_EQ("ALL_DATA FROM_CLIENT, rpcId 666.1 message1 | "
            "ALL_DATA FROM_CLIENT, rpcId 666.2 message2",
            driver->outputLog);
    EXPECT_EQ(2u, transport.outgoingRpcs.size());
}

TEST_F(BasicTransportTest, handlePacket_noHeader) {
    driver->receivePacket(new MockDriver::MockReceived(
            "mock:client=1", "abcdef", 6, NULL));
    Transport::ServerRpc* serverRpc =
        context.workerManager->waitForRpc(0);
    EXPECT_TRUE(serverRpc == NULL);
    EXPECT_EQ("handlePacket: packet from mock:client=1 too short (6 bytes)",
            TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_clientPacketWithUnknownSequence) {
    driver->receivePacket("mock:server=1",
            BasicTransport::AllDataHeader(BasicTransport::RpcId(666, 1),
            BasicTransport::FROM_SERVER, 9), "response1");
    EXPECT_EQ("handlePacket: Received packet from mock:server=1 for unknown "
            "RPC: ALL_DATA FROM_SERVER, rpcId 666.1",
            TestLog::get());
    EXPECT_EQ(0u, driver->stealCount);
}
TEST_F(BasicTransportTest, handlePacket_allDataFromServer_basics) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->receivePacket("mock:server=1",
            BasicTransport::AllDataHeader(BasicTransport::RpcId(666, 1),
            BasicTransport::FROM_SERVER, 9), "response1");
    EXPECT_STREQ("completed: 1, failed: 0", wrapper.getState());
    EXPECT_EQ("response1", TestUtil::toString(&wrapper.response));
    EXPECT_EQ(0lu, transport.outgoingRpcs.size());
    EXPECT_EQ(1u, driver->stealCount);
}
TEST_F(BasicTransportTest, handlePacket_allDataFromServer_tooShort) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->receivePacket("mock:server=1",
            BasicTransport::AllDataHeader(BasicTransport::RpcId(666, 1),
            BasicTransport::FROM_SERVER, 10), "response1");
    EXPECT_STREQ("completed: 0, failed: 0", wrapper.getState());
    EXPECT_EQ(1u, driver->releaseCount);
    EXPECT_EQ("handlePacket: ALL_DATA response from mock:server=1 too short "
            "(got 29 bytes, expected 30)",
            TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_dataFromServer_incompleteHeader) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    driver->receivePacket("mock:server=1", BasicTransport::CommonHeader(
            BasicTransport::DATA, BasicTransport::RpcId(666, 1),
            BasicTransport::FROM_SERVER));
    EXPECT_EQ("handlePacket: packet of type DATA from mock:server=1 too "
            "short (18 bytes)", TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_dataFromServer_basics) {
    MockWrapper wrapper("message1");
    session->roundTripBytes = 1000;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    transport.grantIncrement = 500;

    // First packet of response.
    driver->receivePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 10, 0,
            BasicTransport::NEED_GRANT | BasicTransport::FROM_SERVER), "abcde");
    EXPECT_STREQ("completed: 0, failed: 0", wrapper.getState());
    EXPECT_EQ("GRANT FROM_CLIENT, rpcId 666.1, offset 1505",
            driver->outputLog);
    EXPECT_EQ("abcde", TestUtil::toString(&wrapper.response));
    EXPECT_EQ(1u, driver->stealCount);

    // Second packet of response
    driver->outputLog.clear();
    driver->receivePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 10, 5,
            BasicTransport::FROM_SERVER), "12345");
    EXPECT_STREQ("completed: 1, failed: 0", wrapper.getState());
    EXPECT_EQ("abcde12345", TestUtil::toString(&wrapper.response));
    EXPECT_EQ("", driver->outputLog);
    EXPECT_EQ(0lu, transport.outgoingRpcs.size());
    EXPECT_EQ(2u, driver->stealCount);
}
TEST_F(BasicTransportTest, handlePacket_dataFromServer_extraData) {
    MockWrapper wrapper("message1");
    session->roundTripBytes = 1000;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    transport.grantIncrement = 500;

    // First packet of response.
    driver->receivePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 10, 0,
            BasicTransport::NEED_GRANT | BasicTransport::FROM_SERVER), "abcde");

    // Final packet of response has extra data.
    driver->outputLog.clear();
    driver->receivePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 10, 5,
            BasicTransport::FROM_SERVER), "1234567890");
    EXPECT_STREQ("completed: 1, failed: 0", wrapper.getState());
    EXPECT_EQ("abcde12345", TestUtil::toString(&wrapper.response));
}
TEST_F(BasicTransportTest, handlePacket_dataFromServer_dontIssueGrant) {
    MockWrapper wrapper("message1");
    session->roundTripBytes = 1000;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    transport.grantIncrement = 500;

    // First packet of response (don't send grant: needGrant not set).
    driver->receivePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 15, 0,
            BasicTransport::FROM_SERVER), "abcde");
    EXPECT_EQ("", driver->outputLog);

    // Retransmit first packet, with request for grant this time.
    driver->receivePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 15, 0,
            BasicTransport::NEED_GRANT|BasicTransport::FROM_SERVER),
            "abcde");
    EXPECT_EQ("GRANT FROM_CLIENT, rpcId 666.1, offset 1505",
            driver->outputLog);

    // Second packet of response (still not complete, but no need for
    // another grant).
    driver->outputLog.clear();
    driver->receivePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 15,
            10, BasicTransport::NEED_GRANT|BasicTransport::FROM_SERVER),
            "12345");
    EXPECT_EQ("", driver->outputLog);
    EXPECT_EQ(1lu, transport.outgoingRpcs.size());

    // Third packet of response (now complete)
    driver->outputLog.clear();
    driver->receivePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 15, 5,
            BasicTransport::FROM_SERVER), "xyzzy");
    EXPECT_STREQ("completed: 1, failed: 0", wrapper.getState());
    EXPECT_EQ(0lu, transport.outgoingRpcs.size());
    EXPECT_EQ("abcdexyzzy12345", TestUtil::toString(&wrapper.response));
}
TEST_F(BasicTransportTest, handlePacket_dataFromServer_unnecessaryResend) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    BasicTransport::ClientRpc* clientRpc = transport.outgoingRpcs[1lu];
    clientRpc->resendLimit = 5;

    // First packet is above the limit: no warning.
    driver->receivePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 15, 5,
            BasicTransport::FROM_SERVER), "abcde");
    EXPECT_EQ("", TestLog::get());

    // Second packet is below the limit; expect a warning.
    driver->receivePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 15, 3,
            BasicTransport::FROM_SERVER), "xy");
    EXPECT_EQ("handlePacket: Original data arrived from server "
            "mock:server=1 after RESEND: sequence 1, offset 3, resendLimit 5",
            TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_grantFromServer_incompleteHeader) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    driver->receivePacket("mock:server=1", BasicTransport::CommonHeader(
            BasicTransport::GRANT, BasicTransport::RpcId(666, 1),
            BasicTransport::FROM_SERVER));
    EXPECT_EQ("handlePacket: packet of type GRANT from mock:server=1 too "
            "short (18 bytes)", TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_grantFromServer) {
    MockWrapper wrapper("abcdefghij0123456789");
    session->roundTripBytes = 10;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    EXPECT_EQ("DATA FROM_CLIENT, rpcId 666.1, totalLength 20, offset 0, "
            "NEED_GRANT abcdefghij", driver->outputLog);
    driver->outputLog.clear();

    // First grant doesn't get past transmitOffset.
    driver->receivePacket("mock:server=1",
            BasicTransport::GrantHeader(BasicTransport::RpcId(666, 1), 10,
            BasicTransport::FROM_SERVER));
    EXPECT_EQ("", driver->outputLog);

    // Second grant is far enough out to enable more bytes to be sent.
    driver->receivePacket("mock:server=1",
            BasicTransport::GrantHeader(BasicTransport::RpcId(666, 1), 15,
            BasicTransport::FROM_SERVER));
    EXPECT_EQ(
            "DATA FROM_CLIENT, rpcId 666.1, totalLength 20, offset 10, "
            "NEED_GRANT 01234",
            driver->outputLog);
    EXPECT_EQ(15lu, transport.outgoingRpcs[1]->transmitOffset);
}
TEST_F(BasicTransportTest, handlePacket_resendFromServer_incompleteHeader) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    driver->receivePacket("mock:server=1", BasicTransport::CommonHeader(
            BasicTransport::RESEND, BasicTransport::RpcId(666, 1),
            BasicTransport::FROM_SERVER));
    EXPECT_EQ("handlePacket: packet of type RESEND from mock:server=1 too "
            "short (18 bytes)", TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_resendFromServer) {
    MockWrapper wrapper("abcdefghij0123456789");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    EXPECT_EQ("ALL_DATA FROM_CLIENT, rpcId 666.1 abcdefghij (+10 more)",
            driver->outputLog);
    driver->outputLog.clear();

    driver->receivePacket("mock:server=1", BasicTransport::ResendHeader(
            BasicTransport::RpcId(666, 1), 12, 20,
            BasicTransport::FROM_SERVER));
    EXPECT_EQ("DATA FROM_CLIENT, rpcId 666.1, totalLength 20, offset 12, "
            "RETRANSMISSION 23456789",
            driver->outputLog);
}
TEST_F(BasicTransportTest, handlePacket_resendFromServer_restart) {
    MockWrapper wrapper("abcdefghij0123456789");
    session->roundTripBytes = 10;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    EXPECT_EQ("DATA FROM_CLIENT, rpcId 666.1, totalLength 20, offset 0, "
            "NEED_GRANT abcdefghij",
            driver->outputLog);
    driver->outputLog.clear();
    EXPECT_EQ(10lu, transport.outgoingRpcs[1]->transmitOffset);

    driver->receivePacket("mock:server=1", BasicTransport::ResendHeader(
            BasicTransport::RpcId(666, 1), 0, 5,
            BasicTransport::FROM_SERVER|BasicTransport::RESTART));
    EXPECT_EQ("DATA FROM_CLIENT, rpcId 666.1, totalLength 20, offset 0, "
            "NEED_GRANT, RETRANSMISSION abcde",
            driver->outputLog);
    EXPECT_EQ(5lu, transport.outgoingRpcs[1]->transmitOffset);
}
TEST_F(BasicTransportTest,
        handlePacket_resendFromServer_transmitOffsetChanges) {
    MockWrapper wrapper("abcdefghij0123456789");
    session->roundTripBytes = 10;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    EXPECT_EQ("DATA FROM_CLIENT, rpcId 666.1, totalLength 20, offset 0, "
            "NEED_GRANT abcdefghij",
            driver->outputLog);
    driver->outputLog.clear();
    EXPECT_EQ(10lu, transport.outgoingRpcs[1]->transmitOffset);

    driver->receivePacket("mock:server=1", BasicTransport::ResendHeader(
            BasicTransport::RpcId(666, 1), 8, 6, BasicTransport::FROM_SERVER));
    EXPECT_EQ("DATA FROM_CLIENT, rpcId 666.1, totalLength 20, offset 8, "
            "NEED_GRANT, RETRANSMISSION ij0123",
            driver->outputLog);
    EXPECT_EQ(14lu, transport.outgoingRpcs[1]->transmitOffset);
}
TEST_F(BasicTransportTest, handlePacket_retryFromServer) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    EXPECT_EQ("ALL_DATA FROM_CLIENT, rpcId 666.1 message1",
            driver->outputLog);
    driver->outputLog.clear();

    driver->receivePacket("mock:server=1", BasicTransport::RetryHeader(
            BasicTransport::RpcId(666, 1), BasicTransport::FROM_SERVER));
    EXPECT_STREQ("completed: 1, failed: 0", wrapper.getState());
    EXPECT_EQ(0lu, transport.outgoingRpcs.size());
    EXPECT_EQ(0u, driver->stealCount);
    WireFormat::RetryResponse* response =
            wrapper.response.getStart<WireFormat::RetryResponse>();
    ASSERT_TRUE(response != NULL);
    EXPECT_STREQ("STATUS_RETRY", statusToSymbol(response->common.status));
}
TEST_F(BasicTransportTest, handlePacket_unknownOpcodeFromServer) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    driver->receivePacket("mock:server=1", BasicTransport::CommonHeader(
            BasicTransport::PING, BasicTransport::RpcId(666, 1),
            BasicTransport::FROM_SERVER));
    EXPECT_EQ("handlePacket: unexpected opcode PING received from "
            "server mock:server=1", TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_allDataFromClient) {
    driver->receivePacket("mock:client=1",
            BasicTransport::AllDataHeader(BasicTransport::RpcId(100, 101),
            BasicTransport::FROM_CLIENT, 8), "message1");
    BasicTransport::ServerRpc* serverRpc =
            static_cast<BasicTransport::ServerRpc*>(
            context.workerManager->waitForRpc(0));
    ASSERT_TRUE(serverRpc != NULL);
    EXPECT_EQ("message1", TestUtil::toString(&serverRpc->requestPayload));
    EXPECT_TRUE(serverRpc->requestComplete);
}
TEST_F(BasicTransportTest, handlePacket_allDataFromClient_duplicate) {
    driver->receivePacket("mock:client=1",
            BasicTransport::AllDataHeader(BasicTransport::RpcId(100, 101),
            BasicTransport::FROM_CLIENT, 8), "message1");
    BasicTransport::ServerRpc* serverRpc =
            static_cast<BasicTransport::ServerRpc*>(
            context.workerManager->waitForRpc(0));
    ASSERT_TRUE(serverRpc != NULL);
    driver->receivePacket("mock:client=1",
            BasicTransport::AllDataHeader(BasicTransport::RpcId(100, 101),
            BasicTransport::FROM_CLIENT, 20), "0123457890abcdefghij");
    EXPECT_EQ("message1", TestUtil::toString(&serverRpc->requestPayload));
}
TEST_F(BasicTransportTest, handlePacket_allDataFromClient_tooShort) {
    driver->receivePacket("mock:client=1",
            BasicTransport::AllDataHeader(BasicTransport::RpcId(100, 101),
            BasicTransport::FROM_CLIENT, 9), "message1");
    EXPECT_EQ(1u, driver->releaseCount);
    EXPECT_EQ("handlePacket: ALL_DATA request from mock:client=1 too short "
            "(got 28 bytes, expected 29)",
            TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_dataFromClient_basics) {
    // Send a message in three packets. The first packet should result
    // in a GRANT.
    transport.roundTripBytes = 1000;
    transport.grantIncrement = 500;
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15,
            10, BasicTransport::NEED_GRANT|BasicTransport::FROM_CLIENT),
            "abcde");
    BasicTransport::ServerRpcMap::iterator it = transport.incomingRpcs.find(
            BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    BasicTransport::ServerRpc* serverRpc = it->second;
    EXPECT_FALSE(serverRpc->requestComplete);
    EXPECT_EQ("GRANT FROM_SERVER, rpcId 100.101, offset 1500",
            driver->outputLog);

    // Second packet: no GRANT should result.
    driver->outputLog.clear();
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15,
            0, BasicTransport::NEED_GRANT|BasicTransport::FROM_CLIENT),
            "01234");
    EXPECT_FALSE(serverRpc->requestComplete);
    EXPECT_EQ(1500u, serverRpc->grantOffset);
    EXPECT_EQ("", driver->outputLog);
    EXPECT_EQ(1lu, transport.incomingRpcs.size());
    EXPECT_EQ(1lu, transport.serverTimerList.size());

    // Third packet: no GRANT, message should now be complete.
    driver->outputLog.clear();
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15, 5,
            BasicTransport::FROM_CLIENT), "56789");
    EXPECT_TRUE(serverRpc->requestComplete);
    EXPECT_EQ("", driver->outputLog);
    EXPECT_EQ("0123456789abcde",
            TestUtil::toString(&serverRpc->requestPayload));
}
TEST_F(BasicTransportTest, handlePacket_dataFromClient_extraBytes) {
    // Send a message in two packets; the second packet contains more
    // than enough data to complete the message.
    transport.roundTripBytes = 1000;
    transport.grantIncrement = 500;
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 10,
            0, BasicTransport::NEED_GRANT|BasicTransport::FROM_CLIENT),
            "abcde");
    BasicTransport::ServerRpcMap::iterator it = transport.incomingRpcs.find(
            BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    BasicTransport::ServerRpc* serverRpc = it->second;
    EXPECT_FALSE(serverRpc->requestComplete);

    // Second packet.
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 10,
            5, BasicTransport::NEED_GRANT|BasicTransport::FROM_CLIENT),
            "0123456789");
    EXPECT_TRUE(serverRpc->requestComplete);
    EXPECT_EQ("abcde01234",
            TestUtil::toString(&serverRpc->requestPayload));
}
TEST_F(BasicTransportTest, handlePacket_dataFromClient_dontIssueGrant) {
    transport.roundTripBytes = 1000;
    transport.grantIncrement = 500;
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15, 10,
            BasicTransport::FROM_CLIENT), "abcde");
    EXPECT_EQ("", driver->outputLog);
}
TEST_F(BasicTransportTest, handlePacket_dataFromClient_extraneousPacket) {
    // Send an extra packet after the message is complete.
    driver->receivePacket("mock:client=1",
            BasicTransport::AllDataHeader(BasicTransport::RpcId(100, 101),
            BasicTransport::FROM_CLIENT, 8), "message1");
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 13,
            8, BasicTransport::NEED_GRANT|BasicTransport::FROM_CLIENT),
            "abcde");
    EXPECT_EQ("handlePacket: ignoring extraneous packet", TestLog::get());
    BasicTransport::ServerRpc* serverRpc =
            static_cast<BasicTransport::ServerRpc*>(
            context.workerManager->waitForRpc(0));
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("message1", TestUtil::toString(&serverRpc->requestPayload));
}
TEST_F(BasicTransportTest, handlePacket_dataFromClient_unnecessaryResend) {
    transport.roundTripBytes = 1000;
    transport.grantIncrement = 500;
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15,
            10, BasicTransport::NEED_GRANT|BasicTransport::FROM_CLIENT),
            "abcde");
    BasicTransport::ServerRpcMap::iterator it = transport.incomingRpcs.find(
            BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    BasicTransport::ServerRpc* serverRpc = it->second;
    serverRpc->resendLimit = 5;

    // First packet is above the limit, so no warning.
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15,
            5, BasicTransport::NEED_GRANT|BasicTransport::FROM_CLIENT),
            "01234");
    EXPECT_EQ("", TestLog::get());

    // Second packet is below the limit; expect a log message.
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15,
            4, BasicTransport::NEED_GRANT|BasicTransport::FROM_CLIENT), "X");
    EXPECT_EQ("handlePacket: Original data arrived from client "
            "mock:client=1 after RESEND: sequence 101, offset 4, "
            "resendLimit 5", TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_grantFromClient_bogusGrants) {
    prepareToRespond();
    transport.roundTripBytes = 5;

    // GRANT arriving for unknown RpcID: bogus.
    driver->receivePacket("mock:client=1",
            BasicTransport::GrantHeader(BasicTransport::RpcId(5, 6), 10,
            BasicTransport::FROM_CLIENT));
    EXPECT_EQ("handlePacket: unexpected GRANT from client mock:client=1, "
            "id (5,6), grantOffset 10",
            TestLog::get());

    // GRANT arriving before result transmission starts: bogus.
    TestLog::reset();
    driver->receivePacket("mock:client=1",
            BasicTransport::GrantHeader(BasicTransport::RpcId(100, 101), 10,
            BasicTransport::FROM_CLIENT));
    EXPECT_EQ("handlePacket: unexpected GRANT from client mock:client=1, "
            "id (100,101), grantOffset 10",
            TestLog::get());

    // GRANT packet too short: bogus.
    TestLog::reset();
    driver->receivePacket("mock:client=1", BasicTransport::CommonHeader(
            BasicTransport::GRANT, BasicTransport::RpcId(100, 101),
            BasicTransport::FROM_CLIENT));
    EXPECT_EQ("handlePacket: packet of type GRANT from mock:client=1 "
            "too short (18 bytes)", TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_grantFromClient) {
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    transport.roundTripBytes = 5;
    serverRpc->sendReply();
    driver->outputLog.clear();

    // First, send redundant grant (do nothing).
    driver->receivePacket("mock:client=1",
            BasicTransport::GrantHeader(BasicTransport::RpcId(100, 101), 5,
            BasicTransport::FROM_CLIENT));
    EXPECT_EQ("", driver->outputLog);

    // Second grant should cause more data to be transmitted.
    driver->receivePacket("mock:client=1",
            BasicTransport::GrantHeader(BasicTransport::RpcId(100, 101), 15,
            BasicTransport::FROM_CLIENT));
    EXPECT_EQ("DATA FROM_SERVER, rpcId 100.101, totalLength 20, offset 5, "
            "NEED_GRANT 56789abcde",
            driver->outputLog);

    // Third grant should complete the result transmission.
    driver->outputLog.clear();
    driver->receivePacket("mock:client=1",
            BasicTransport::GrantHeader(BasicTransport::RpcId(100, 101), 25,
            BasicTransport::FROM_CLIENT));
    EXPECT_EQ("DATA FROM_SERVER, rpcId 100.101, totalLength 20, offset 15 "
            "fghij",
            driver->outputLog);
    EXPECT_EQ(0lu, transport.incomingRpcs.size());
    EXPECT_EQ(0lu, transport.serverTimerList.size());
    EXPECT_EQ(0lu, transport.serverRpcPool.outstandingAllocations);
}
TEST_F(BasicTransportTest, handlePacket_resendFromClient_bogusResends) {
    prepareToRespond();
    transport.roundTripBytes = 5;

    // RESEND arriving before result transmission starts: bogus.
    TestLog::reset();
    driver->receivePacket("mock:client=1",
            BasicTransport::ResendHeader(BasicTransport::RpcId(100, 101),
            10, 5, BasicTransport::FROM_CLIENT));
    EXPECT_EQ("handlePacket: unexpected RESEND from client mock:client=1",
            TestLog::get());

    // RESEND packet too short: bogus.
    TestLog::reset();
    driver->receivePacket("mock:client=1", BasicTransport::CommonHeader(
            BasicTransport::RESEND, BasicTransport::RpcId(100, 101),
            BasicTransport::FROM_CLIENT));
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "handlePacket: packet of type RESEND from mock:client=1 "
            "too short (18 bytes)"));
}
TEST_F(BasicTransportTest, handlePacket_resendFromClient_unknownRpcId) {
    driver->receivePacket("mock:client=1",
            BasicTransport::ResendHeader(BasicTransport::RpcId(10, 11),
            10, 5, BasicTransport::FROM_CLIENT));
    EXPECT_EQ("handlePacket: received RESEND from client mock:client=1, "
            "but RPC state no longer exists",
            TestLog::get());
    EXPECT_EQ("RETRY FROM_SERVER, rpcId 10.11", driver->outputLog);
}
TEST_F(BasicTransportTest, handlePacket_resendFromClient_sendBytes) {
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    transport.roundTripBytes = 15;
    serverRpc->sendReply();

    driver->outputLog.clear();
    driver->receivePacket("mock:client=1",
            BasicTransport::ResendHeader(BasicTransport::RpcId(100, 101),
            5, 8, BasicTransport::FROM_CLIENT));
    EXPECT_EQ("DATA FROM_SERVER, rpcId 100.101, totalLength 20, offset 5, "
            "NEED_GRANT, RETRANSMISSION 56789abc",
            driver->outputLog);
}
TEST_F(BasicTransportTest,
        handlePacket_resendFromClient_transmitOffsetChanges) {
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    transport.roundTripBytes = 15;
    serverRpc->sendReply();

    // First RESEND advances transmitOffset but doesn't finish message.
    driver->outputLog.clear();
    driver->receivePacket("mock:client=1",
            BasicTransport::ResendHeader(BasicTransport::RpcId(100, 101),
            10, 7, BasicTransport::FROM_CLIENT));
    EXPECT_EQ("DATA FROM_SERVER, rpcId 100.101, totalLength 20, offset 10, "
            "NEED_GRANT, RETRANSMISSION abcdefg",
            driver->outputLog);
    EXPECT_EQ(17u, serverRpc->transmitOffset);

    // Second RESEND finishes the message.
    driver->outputLog.clear();
    driver->receivePacket("mock:client=1",
            BasicTransport::ResendHeader(BasicTransport::RpcId(100, 101),
            15, 10, BasicTransport::FROM_CLIENT));
    EXPECT_EQ("DATA FROM_SERVER, rpcId 100.101, totalLength 20, offset 15, "
            "RETRANSMISSION fghij",
            driver->outputLog);
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "deleteServerRpc: RpcId (100, 101)"));
}
TEST_F(BasicTransportTest, handlePacket_pingFromClient_unknownRpcId) {
    transport.roundTripBytes = 1000;
    driver->receivePacket("mock:client=1",
            BasicTransport::PingHeader(BasicTransport::RpcId(10, 11),
            BasicTransport::FROM_CLIENT));
    EXPECT_EQ("RESEND FROM_SERVER, rpcId 10.11, offset 0, length 1000, RESTART",
            driver->outputLog);
}
TEST_F(BasicTransportTest, handlePacket_pingFromClient_stillProcessing) {
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    transport.roundTripBytes = 15;
    serverRpc->grantOffset = 100;
    driver->receivePacket("mock:client=1",
            BasicTransport::PingHeader(BasicTransport::RpcId(100, 101),
            BasicTransport::FROM_CLIENT));
    EXPECT_EQ("GRANT FROM_SERVER, rpcId 100.101, offset 100",
            driver->outputLog);
}
TEST_F(BasicTransportTest, handlePacket_pingFromClient_resultPartiallySent) {
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    transport.roundTripBytes = 15;
    transport.maxDataPerPacket = 5;
    serverRpc->sendReply();
    EXPECT_EQ("DATA FROM_SERVER, rpcId 100.101, totalLength 20, offset 0, "
            "NEED_GRANT 01234 | "
            "DATA FROM_SERVER, rpcId 100.101, totalLength 20, offset 5, "
            "NEED_GRANT 56789 | "
            "DATA FROM_SERVER, rpcId 100.101, totalLength 20, offset 10, "
            "NEED_GRANT abcde",
            driver->outputLog);
    driver->outputLog.clear();
    driver->receivePacket("mock:client=1",
            BasicTransport::PingHeader(BasicTransport::RpcId(100, 101),
            BasicTransport::FROM_CLIENT));
    EXPECT_EQ("DATA FROM_SERVER, rpcId 100.101, totalLength 20, offset 0, "
            "NEED_GRANT, RETRANSMISSION 01234",
            driver->outputLog);
}
TEST_F(BasicTransportTest, handlePacket_unknownOpcodeFromClient) {
    driver->receivePacket("mock:client=1", BasicTransport::CommonHeader(
            BasicTransport::RETRY, BasicTransport::RpcId(100, 101),
            BasicTransport::FROM_CLIENT));
    EXPECT_EQ("handlePacket: unexpected opcode RETRY received from client "
            "mock:client=1", TestLog::get());
}

TEST_F(BasicTransportTest, sendReply_sendEntireMessage) {
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    serverRpc->sendReply();
    EXPECT_EQ("ALL_DATA FROM_SERVER, rpcId 100.101 0123456789 (+10 more)",
            driver->outputLog);
    EXPECT_EQ("deleteServerRpc: RpcId (100, 101)", TestLog::get());
}
TEST_F(BasicTransportTest, sendReply_sendPartialMessage) {
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    transport.roundTripBytes = 10;
    serverRpc->sendReply();
    EXPECT_EQ("DATA FROM_SERVER, rpcId 100.101, totalLength 20, offset 0, "
            "NEED_GRANT 0123456789",
            driver->outputLog);
    EXPECT_EQ("", TestLog::get());
    EXPECT_EQ(10lu, serverRpc->transmitOffset);
}

TEST_F(BasicTransportTest, MessageAccumulator_destructor) {
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20, 5,
            BasicTransport::FROM_CLIENT), "abcde");
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20, 15,
            BasicTransport::FROM_CLIENT), "56789");
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20, 10,
            BasicTransport::FROM_CLIENT), "01234");
    BasicTransport::ServerRpc* serverRpc =
            transport.incomingRpcs[BasicTransport::RpcId(100, 101)];
    EXPECT_TRUE(serverRpc->accumulator);
    EXPECT_EQ(3u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ(0u, driver->releaseCount);
    transport.deleteServerRpc(serverRpc);
    EXPECT_EQ(3u, driver->releaseCount);
}

TEST_F(BasicTransportTest, addPacket_basics) {
    // Receive a request in 5 packets, in the order P4, P2, P0, P1, P2.
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 25, 20,
            BasicTransport::FROM_CLIENT), "P4444");
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 25, 10,
            BasicTransport::FROM_CLIENT), "P2222");
    BasicTransport::ServerRpcMap::iterator it =
            transport.incomingRpcs.find(BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    BasicTransport::ServerRpc* serverRpc = it->second;
    ASSERT_TRUE(serverRpc->accumulator);
    EXPECT_EQ(2u, serverRpc->accumulator->fragments.size());

    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 25, 0,
            BasicTransport::FROM_CLIENT), "P0000");
    EXPECT_EQ(2u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ("P0000",
            TestUtil::toString(&serverRpc->requestPayload));

    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 25, 5,
            BasicTransport::FROM_CLIENT), "P1111");
    EXPECT_EQ(1u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ("P0000P1111P2222",
            TestUtil::toString(&serverRpc->requestPayload));

    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 25, 15,
            BasicTransport::FROM_CLIENT), "P3333");
    EXPECT_EQ(0u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ("P0000P1111P2222P3333P4444",
            TestUtil::toString(&serverRpc->requestPayload));
}
TEST_F(BasicTransportTest, addPacket_addMultipleFragmentsAtOnce) {
    // Receive a request in 4 packets, in the order P3, P2, P1, P0.
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20, 15,
            BasicTransport::FROM_CLIENT), "P3333");
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20, 10,
            BasicTransport::FROM_CLIENT), "P2222");
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20, 5,
            BasicTransport::FROM_CLIENT), "P1111");
    BasicTransport::ServerRpcMap::iterator it =
            transport.incomingRpcs.find(BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    BasicTransport::ServerRpc* serverRpc = it->second;
    ASSERT_TRUE(serverRpc->accumulator);
    EXPECT_EQ(3u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ("", TestUtil::toString(&serverRpc->requestPayload));

    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20, 0,
            BasicTransport::FROM_CLIENT), "P0000");
    EXPECT_EQ(0u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ("P0000P1111P2222P3333",
            TestUtil::toString(&serverRpc->requestPayload));
}

TEST_F(BasicTransportTest, appendFragment_discardFragment) {
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20, 5,
            BasicTransport::FROM_CLIENT), "xxxxx");
    BasicTransport::ServerRpcMap::iterator it =
            transport.incomingRpcs.find(BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    BasicTransport::ServerRpc* serverRpc = it->second;
    ASSERT_TRUE(serverRpc->accumulator);
    EXPECT_EQ(1u, serverRpc->accumulator->fragments.size());

    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20, 0,
            BasicTransport::FROM_CLIENT), "0123456789");
    EXPECT_EQ(0u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ("0123456789", TestUtil::toString(&serverRpc->requestPayload));
    EXPECT_EQ(1u, driver->releaseCount);
}

TEST_F(BasicTransportTest, appendFragment_truncateFragments) {
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20, 0,
            BasicTransport::FROM_CLIENT), "xxxxx");
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20, 8,
            BasicTransport::FROM_CLIENT), "zzzzz");
    BasicTransport::ServerRpcMap::iterator it =
            transport.incomingRpcs.find(BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    BasicTransport::ServerRpc* serverRpc = it->second;
    ASSERT_TRUE(serverRpc->accumulator);
    EXPECT_EQ(1u, serverRpc->accumulator->fragments.size());

    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20, 3,
            BasicTransport::FROM_CLIENT), "yyyyyy");
    EXPECT_EQ(0u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ("xxxxxyyyyzzzz", TestUtil::toString(&serverRpc->requestPayload));
    EXPECT_EQ(0u, driver->releaseCount);
}

TEST_F(BasicTransportTest, requestRetransmission) {
    transport.roundTripBytes = 100;

    // First retransmit: no fragments waiting, no grants sent.
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20, 0,
            BasicTransport::FROM_CLIENT), "01234");
    BasicTransport::ServerRpcMap::iterator it =
            transport.incomingRpcs.find(BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    BasicTransport::ServerRpc* serverRpc = it->second;
    ASSERT_TRUE(serverRpc->accumulator);
    EXPECT_EQ(0u, serverRpc->accumulator->fragments.size());

    driver->outputLog.clear();
    uint32_t limit = serverRpc->accumulator->requestRetransmission(
            &transport, &address1, BasicTransport::RpcId(100, 101), 0, 100,
            BasicTransport::FROM_SERVER);
    EXPECT_EQ(105u, limit);
    EXPECT_EQ("RESEND FROM_SERVER, rpcId 100.101, offset 5, length 100",
            driver->outputLog);

    // Second retransmit: no fragment, but grant was sent.
    driver->outputLog.clear();
    limit = serverRpc->accumulator->requestRetransmission(
            &transport, &address1, BasicTransport::RpcId(100, 101), 25, 100,
            BasicTransport::FROM_SERVER);
    EXPECT_EQ(25u, limit);
    EXPECT_EQ("RESEND FROM_SERVER, rpcId 100.101, offset 5, length 20",
            driver->outputLog);

    // Third retransmit: fragment waiting.
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20, 15,
            BasicTransport::FROM_CLIENT), "abcde");
    driver->outputLog.clear();
    limit = serverRpc->accumulator->requestRetransmission(
            &transport, &address1, BasicTransport::RpcId(100, 101), 25, 100,
            BasicTransport::FROM_SERVER);
    EXPECT_EQ(15u, limit);
    EXPECT_EQ("RESEND FROM_SERVER, rpcId 100.101, offset 5, length 10",
            driver->outputLog);
}

TEST_F(BasicTransportTest, handleTimerEvent_clientPingAndAbort) {
    MockWrapper wrapper(NULL);
    WireFormat::RequestCommon* header =
            wrapper.request.emplaceAppend<WireFormat::RequestCommon>();
    header->opcode = WireFormat::READ;
    header->service = WireFormat::MASTER_SERVICE;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    BasicTransport::ClientRpc* clientRpc = transport.outgoingRpcs[1lu];
    driver->outputLog.clear();
    transport.timeoutIntervals = 2*transport.pingIntervals+1;

    // Nothing should get logged for the first few calls to handleTimerEvent.
    transport.timer.handleTimerEvent();
    transport.timer.handleTimerEvent();
    EXPECT_EQ("", TestLog::get());
    EXPECT_EQ(2u, clientRpc->silentIntervals);

    // The next call should ping.
    clientRpc->silentIntervals = transport.pingIntervals - 1;
    transport.timer.handleTimerEvent();
    EXPECT_EQ("PING FROM_CLIENT, rpcId 666.1", driver->outputLog);
    driver->outputLog.clear();
    TestLog::reset();

    // Next call: nothing.
    transport.timer.handleTimerEvent();
    EXPECT_EQ("", TestLog::get());

    // Next call: warning message when there is no response to the ping.
    transport.timer.handleTimerEvent();
    EXPECT_EQ("handleTimerEvent: slow PING response from server mock:node=1 "
            "for READ RPC, sequence 1", TestLog::get());
    TestLog::reset();

    // Wait a while longer and make sure that the client eventually
    // aborts the request.
    uint64_t start = Cycles::rdtsc();
    while (1) {
        context.dispatch->poll();
        if ((wrapper.failedCount != 0) ||
                (Cycles::toSeconds(Cycles::rdtsc() - start) > 0.1)) {
            break;
        }
    }
    EXPECT_EQ(1, wrapper.failedCount);
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "handleTimerEvent: aborting READ RPC to server mock:node=1, "
            "sequence 1: timeout"));
}
TEST_F(BasicTransportTest, handleTimerEvent_sendResendFromClient) {
    session->roundTripBytes = 100;
    transport.grantIncrement = 50;
    MockWrapper wrapper(NULL);
    WireFormat::RequestCommon* header =
            wrapper.request.emplaceAppend<WireFormat::RequestCommon>();
    header->opcode = WireFormat::READ;
    header->service = WireFormat::MASTER_SERVICE;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    BasicTransport::ClientRpc* clientRpc = transport.outgoingRpcs[1lu];
    driver->receivePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 10,
            0, BasicTransport::NEED_GRANT|BasicTransport::FROM_SERVER),
            "abcde");
    driver->outputLog.clear();

    transport.timer.handleTimerEvent();
    EXPECT_EQ("", TestLog::get());

    transport.timer.handleTimerEvent();
    EXPECT_EQ("requestRetransmission: requested retransmit of response "
            "bytes 5-155 from mock:node=1, sequence 1, grantOffset 155 "
            "(packets lost\?)",
            TestLog::get());
    EXPECT_EQ("RESEND FROM_CLIENT, rpcId 666.1, offset 5, length 150",
            driver->outputLog);
    EXPECT_EQ(155lu, clientRpc->resendLimit);

    driver->outputLog.clear();
    transport.timer.handleTimerEvent();
    EXPECT_EQ("RESEND FROM_CLIENT, rpcId 666.1, offset 5, length 150",
            driver->outputLog);

    // If a packet arrives, RESENDS stop (for a while).
    driver->outputLog.clear();
    driver->receivePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 10, 5,
            BasicTransport::FROM_SERVER), "fgh");
    transport.timer.handleTimerEvent();
    EXPECT_EQ("", driver->outputLog);

    transport.timer.handleTimerEvent();
    EXPECT_EQ("RESEND FROM_CLIENT, rpcId 666.1, offset 8, length 147",
            driver->outputLog);
}
TEST_F(BasicTransportTest, handleTimerEvent_serverAbortsRequest) {
    transport.timeoutIntervals = 2;
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15, 0,
            BasicTransport::FROM_CLIENT), "abcde");

    transport.timer.handleTimerEvent();
    EXPECT_EQ("", TestLog::get());

    transport.timer.handleTimerEvent();
    EXPECT_EQ("handleTimerEvent: aborting unknown(25185) RPC from client "
            "mock:client=1: 5 request bytes assembled, "
            "0 unassembled fragments, request incomplete, "
            "0 response bytes transmitted | "
            "deleteServerRpc: RpcId (100, 101)",
            TestLog::get());
}
TEST_F(BasicTransportTest, handleTimerEvent_sendResendFromServer) {
    transport.roundTripBytes = 100;
    transport.grantIncrement = 50;
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15, 0,
            BasicTransport::FROM_CLIENT), "abcde");
    BasicTransport::ServerRpcMap::iterator it =
            transport.incomingRpcs.find(BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    BasicTransport::ServerRpc* serverRpc = it->second;
    driver->outputLog.clear();

    transport.timer.handleTimerEvent();
    EXPECT_EQ("", TestLog::get());

    transport.timer.handleTimerEvent();
    EXPECT_EQ("requestRetransmission: requested retransmit of request "
            "bytes 5-105 from mock:client=1, sequence 101, grantOffset 0 "
            "(packets lost\?)",
            TestLog::get());
    EXPECT_EQ("RESEND FROM_SERVER, rpcId 100.101, offset 5, length 100",
            driver->outputLog);
    EXPECT_EQ(105lu, serverRpc->resendLimit);

    // Packet arrival stops resends (for a while).
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15, 5,
            BasicTransport::FROM_CLIENT), "fgh");
    driver->outputLog.clear();
    transport.timer.handleTimerEvent();
    EXPECT_EQ("", driver->outputLog);

    transport.timer.handleTimerEvent();
    EXPECT_EQ("RESEND FROM_SERVER, rpcId 100.101, offset 8, length 100",
            driver->outputLog);
}

}  // namespace RAMCloud
