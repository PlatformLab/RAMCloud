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
    BasicTransport transport;
    MockDriver::MockAddress address1;
    MockDriver::MockAddress address2;
    ServiceLocator locator;
    Transport::SessionRef sessionRef;
    BasicTransport::Session* session;
    TestLog::Enable logEnabler;

    BasicTransportTest()
        : context(false)
        , driver(new MockDriver(&context, BasicTransport::headerToString))
        , transport(&context, NULL, driver, true, 666)
        , address1("mock:node=1")
        , address2("mock:node=2")
        , locator("mock:node=3")
        , sessionRef(transport.getSession(&locator))
        , session(static_cast<BasicTransport::Session*>(sessionRef.get()))
        , logEnabler()
    {
        context.workerManager = new WorkerManager(&context, 5);
        context.workerManager->testingSaveRpcs = 1;
        transport.smallMessageThreshold = 0;
        Driver::Received::stealCount = 0;
    }

    ~BasicTransportTest()
    {
        Cycles::mockTscValue = 0;
    }

    // Assembles a packet and passes it to the transport as input.
    template<typename T>
    void
    handlePacket(const char* sender, T header, const char* body = NULL)
    {
        MockDriver::PacketBuf* packet = new MockDriver::PacketBuf(
                sender, &header, sizeof32(T), body);
        Driver::Received received(&packet->address, driver, packet->length,
                packet->payload);
        transport.handlePacket(&received);
    }

    // Convenience method: receive request, prepare response, but don't
    // call sendReply yet.
    BasicTransport::ServerRpc*
    prepareToRespond(uint64_t sequence = 101, uint32_t responseSize = 20,
            const char* responseData = "0123456789abcdefghijABCDEFGHIJ")
    {
        handlePacket("mock:client=1",
                BasicTransport::AllDataHeader(
                BasicTransport::RpcId(100, sequence),
                BasicTransport::FROM_CLIENT, 8), "message1");
        BasicTransport::ServerRpc* serverRpc =
                static_cast<BasicTransport::ServerRpc*>(
                context.workerManager->waitForRpc(0));
        EXPECT_TRUE(serverRpc != NULL);
        uint32_t dataLength = downCast<uint32_t>(strlen(responseData));
        while (serverRpc->replyPayload.size() < responseSize) {
            uint32_t chunkSize = responseSize - serverRpc->replyPayload.size();
            if (chunkSize > dataLength) {
                chunkSize = dataLength;
            }
            serverRpc->replyPayload.appendCopy(responseData, chunkSize);
        }
        return serverRpc;
    }

    // Fill a character array with a single character value, leaving it
    // null-terminated. Return a pointer to the array.
    char*
    fillString(char* dest, char value, size_t length)
    {
        memset(dest, value, length);
        dest[length - 1] = 0;
        return dest;
    }

    // Queues a given number of BUSY input packets in the NIC, so that the
    // next call to receivePackets will return them.
    template<typename T>
    void
    createInputPackets(int count, T* header) {
        for (int i = 0; i < count; i++) {
            driver->packetArrived("dummySender", *header, NULL);
        }
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
    BasicTransport server(&context, &serverLocator, serverDriver, true, 1);
    UdpDriver* clientDriver = new UdpDriver(&context);
    BasicTransport client(&context, NULL, clientDriver, true, 2);
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
    EXPECT_EQ(10960u, transport.roundTripBytes);
}

TEST_F(BasicTransportTest, deleteClientRpc) {
    string request(10001, 'x');
    MockWrapper wrapper(request.c_str());
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    EXPECT_EQ(1u, transport.clientRpcPool.outstandingObjects);
    transport.deleteClientRpc(transport.outgoingRpcs[1]);
    EXPECT_EQ(0u, transport.clientRpcPool.outstandingObjects);
    EXPECT_EQ(0u, transport.outgoingRpcs.size());
}

TEST_F(BasicTransportTest, deleteServerRpc) {
    string response(10001, 'x');
    BasicTransport::ServerRpc* serverRpc = prepareToRespond(101,
            downCast<uint32_t>(response.size()), response.c_str());
    transport.roundTripBytes = 10;
    transport.maxDataPerPacket = 10;
    serverRpc->sendReply();
    EXPECT_EQ(1u, transport.serverRpcPool.outstandingAllocations);
    transport.deleteServerRpc(serverRpc);
    EXPECT_EQ(0u, transport.serverRpcPool.outstandingAllocations);
    EXPECT_EQ(0lu, transport.incomingRpcs.size());
    EXPECT_EQ(0lu, transport.outgoingResponses.size());
    EXPECT_EQ(0lu, transport.serverTimerList.size());
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
    EXPECT_EQ(8000u, transport.getRoundTripBytes(&locator));
}
TEST_F(BasicTransportTest, getRoundTripBytes_bogusRttOption) {
    transport.maxDataPerPacket = 100;
    ServiceLocator locator("mock:gbs=8,rttMicros=xyz");
    TestLog::reset();
    EXPECT_EQ(8000u, transport.getRoundTripBytes(&locator));
    EXPECT_EQ("getRoundTripBytes: Bad BasicTransport rttMicros option value "
            "'xyz' (expected positive integer); ignoring option",
            TestLog::get());

    ServiceLocator locator2("mock:gbs=8,rttMicros=5zzz");
    TestLog::reset();
    EXPECT_EQ(8000u, transport.getRoundTripBytes(&locator2));
    EXPECT_EQ("getRoundTripBytes: Bad BasicTransport rttMicros option value "
            "'5zzz' (expected positive integer); ignoring option",
            TestLog::get());
}
TEST_F(BasicTransportTest, getRoundTripBytes_roundUpToEvenPackets) {
    transport.maxDataPerPacket = 100;
    ServiceLocator locator("mock:gbs=1,rttMicros=9");
    EXPECT_EQ(1200u, transport.getRoundTripBytes(&locator));
}

TEST_F(BasicTransportTest, sendBytes_basics) {
    transport.maxDataPerPacket = 10;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    Buffer buffer;
    buffer.append("abcdefghijklmno1234567890", 25);
    uint32_t count = transport.sendBytes(&address1,
            BasicTransport::RpcId(5, 6), &buffer, 0, 50,
            unscheduledBytes, BasicTransport::FROM_SERVER);
    EXPECT_EQ("DATA FROM_SERVER, rpcId 5.6, totalLength 25, "
            "offset 0 abcdefghij | "
            "DATA FROM_SERVER, rpcId 5.6, totalLength 25, "
            "offset 10 klmno12345 | "
            "DATA FROM_SERVER, rpcId 5.6, totalLength 25, "
            "offset 20 67890",
            driver->outputLog);
    EXPECT_EQ(25u, count);
}

TEST_F(BasicTransportTest, sendBytes_shortMessage) {
    transport.maxDataPerPacket = 100;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    Buffer buffer;
    buffer.append("abcdefghijklmno", 15);
    uint32_t count = transport.sendBytes(&address1,
            BasicTransport::RpcId(5, 6), &buffer, 0, 16,
            unscheduledBytes, BasicTransport::FROM_CLIENT);
    EXPECT_EQ("ALL_DATA FROM_CLIENT, rpcId 5.6 abcdefghij (+5 more)",
            driver->outputLog);
    EXPECT_EQ(15u, count);
}

TEST_F(BasicTransportTest, tryToTransmitData_pickShortestRequest) {
    transport.maxDataPerPacket = 10;
    driver->transmitQueueSpace = 0;
    char longMessage[20001];
    MockWrapper wrapper1(fillString(longMessage, 'a', sizeof(longMessage)));
    session->sendRequest(&wrapper1.request, &wrapper1.response, &wrapper1);
    MockWrapper wrapper2("abcd");
    session->sendRequest(&wrapper2.request, &wrapper2.response, &wrapper2);
    MockWrapper wrapper3("012345678901234");
    session->sendRequest(&wrapper3.request, &wrapper3.response, &wrapper3);
    driver->transmitQueueSpace = 18;
    uint32_t result = transport.tryToTransmitData();
    EXPECT_EQ("ALL_DATA FROM_CLIENT, rpcId 666.2 abcd | "
            "DATA FROM_CLIENT, rpcId 666.3, totalLength 15, "
            "offset 0 0123456789",
            driver->outputLog);
    EXPECT_EQ(14u, result);
    EXPECT_EQ(2u, transport.outgoingRequests.size());
    BasicTransport::ClientRpc* clientRpc1 = transport.outgoingRpcs[1lu];
    EXPECT_EQ(0u, clientRpc1->request.transmitOffset);
    BasicTransport::ClientRpc* clientRpc3 = transport.outgoingRpcs[3lu];
    EXPECT_EQ(10u, clientRpc3->request.transmitOffset);
}
TEST_F(BasicTransportTest, tryToTransmitData_pickShortestResponse) {
    transport.maxDataPerPacket = 10;
    driver->transmitQueueSpace = 0;
    BasicTransport::ServerRpc* serverRpc1 = prepareToRespond(200, 20000,
            "aaaaaaaaaa");
    serverRpc1->sendReply();
    BasicTransport::ServerRpc* serverRpc2 = prepareToRespond(201, 5, "bbbbb");
    serverRpc2->sendReply();
    BasicTransport::ServerRpc* serverRpc3 = prepareToRespond(202, 15, "ccccc");
    serverRpc3->sendReply();
    driver->transmitQueueSpace = 18;
    uint32_t result = transport.tryToTransmitData();
    EXPECT_EQ("ALL_DATA FROM_SERVER, rpcId 100.201 bbbbb | "
            "DATA FROM_SERVER, rpcId 100.202, totalLength 15, "
            "offset 0 cccccccccc",
            driver->outputLog);
    EXPECT_EQ(15u, result);
    EXPECT_EQ(2u, transport.outgoingResponses.size());
    EXPECT_EQ(2u, transport.incomingRpcs.size());
    EXPECT_EQ(0u, serverRpc1->response.transmitOffset);
    EXPECT_EQ(5u, serverRpc2->response.transmitOffset);
    EXPECT_EQ(10u, serverRpc3->response.transmitOffset);
}
TEST_F(BasicTransportTest, tryToTransmitData_requestsAndResponsesToTransmit) {
    transport.maxDataPerPacket = 10;
    driver->transmitQueueSpace = 0;
    MockWrapper wrapper1("012345678901234");
    session->sendRequest(&wrapper1.request, &wrapper1.response, &wrapper1);
    MockWrapper wrapper2("abcd");
    session->sendRequest(&wrapper2.request, &wrapper2.response, &wrapper2);
    MockWrapper wrapper3("0123456789012345");
    BasicTransport::ServerRpc* serverRpc1 = prepareToRespond(200, 15);
    serverRpc1->sendReply();
    driver->transmitQueueSpace = 35;
    uint32_t result = transport.tryToTransmitData();
    EXPECT_EQ("ALL_DATA FROM_CLIENT, rpcId 666.2 abcd | "
            "DATA FROM_CLIENT, rpcId 666.1, totalLength 15, "
            "offset 0 0123456789 | "
            "DATA FROM_CLIENT, rpcId 666.1, totalLength 15, "
            "offset 10 01234 | "
            "DATA FROM_SERVER, rpcId 100.200, totalLength 15, "
            "offset 0 0123456789 | "
            "DATA FROM_SERVER, rpcId 100.200, totalLength 15, "
            "offset 10 abcde",
            driver->outputLog);
    EXPECT_EQ(34u, result);
}

TEST_F(BasicTransportTest, tryToTransmitData_negativeTransmitQueueSpace) {
    driver->transmitQueueSpace = -999;
    MockWrapper wrapper1("012345678901234");
    session->sendRequest(&wrapper1.request, &wrapper1.response, &wrapper1);
    EXPECT_EQ("", driver->outputLog);
}

TEST_F(BasicTransportTest, Session_constructor) {
    ServiceLocator locator("basic+udp: host=localhost, port=11101");
    UdpDriver* driver2 = new UdpDriver(&context, &locator);
    BasicTransport transport2(&context, &locator, driver2, true, 1);
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
    request2.append("message2", 8);
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
    driver->outputLog.clear();
    EXPECT_EQ(10u, transport.outgoingRpcs.size());
    session->cancelRequest(&notifiers[7]);
    session->cancelRequest(&notifiers[9]);
    session->cancelRequest(&notifiers[0]);
    session->cancelRequest(&notifiers[2]);
    EXPECT_EQ(6u, transport.outgoingRpcs.size());
    EXPECT_EQ(6u, transport.clientRpcPool.outstandingObjects);
    EXPECT_EQ("ABORT FROM_CLIENT, rpcId 666.8 | "
            "ABORT FROM_CLIENT, rpcId 666.10 | ABORT FROM_CLIENT, rpcId 666.1"
            " | ABORT FROM_CLIENT, rpcId 666.3", driver->outputLog);
}

TEST_F(BasicTransportTest, Session_getRpcInfo) {
    Transport::RpcNotifier notifier1, notifier2;
    WireFormat::RequestCommon header1 = {WireFormat::PING, 0};
    WireFormat::RequestCommon header2 = {WireFormat::READ, 0};
    Buffer request1, request2;
    Buffer response1, response2;
    request1.appendCopy(&header1);
    request2.appendCopy(&header2);
    EXPECT_EQ("no active RPCs to server at mock:node=3",
            session->getRpcInfo());
    session->sendRequest(&request1, &response1, &notifier1);
    session->sendRequest(&request2, &response2, &notifier2);
    EXPECT_TRUE((session->getRpcInfo().compare(
            "PING, READ to server at mock:node=3") == 0) ||
            (session->getRpcInfo().compare(
            "READ, PING to server at mock:node=3") == 0));
}

TEST_F(BasicTransportTest, Session_sendRequest_aborted) {
    MockWrapper wrapper("message1");
    session->abort();
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    EXPECT_EQ("", driver->outputLog);
}
TEST_F(BasicTransportTest, Session_sendRequest_normal) {
    transport.roundTripBytes = 1000;
    transport.maxDataPerPacket = 10;
    driver->transmitQueueSpace = 10;
    MockWrapper wrapper1("message1");
    MockWrapper wrapper2("message2");
    session->sendRequest(&wrapper1.request, &wrapper1.response, &wrapper1);
    session->sendRequest(&wrapper2.request, &wrapper2.response, &wrapper2);
    BasicTransport::ClientRpc* clientRpc1 = transport.outgoingRpcs[1lu];
    EXPECT_EQ(8u, clientRpc1->request.transmitOffset);
    BasicTransport::ClientRpc* clientRpc2 = transport.outgoingRpcs[2lu];
    EXPECT_EQ(8u, clientRpc2->request.transmitLimit);
    EXPECT_EQ(0u, clientRpc2->request.transmitOffset);
    EXPECT_EQ(2u, transport.outgoingRpcs.size());
    EXPECT_EQ(1u, transport.outgoingRequests.size());
    EXPECT_EQ(3u, transport.nextClientSequenceNumber);
}
TEST_F(BasicTransportTest, Session_sendRequest_smallMessage) {
    // Small messages are passed to the NIC directly, bypassing the
    // sender's congestion control mechanism in tryToTransmitData.
    driver->transmitQueueSpace = -1;
    transport.smallMessageThreshold = 10;
    MockWrapper wrapper1("012345678");
    MockWrapper wrapper2("0123456789");
    session->sendRequest(&wrapper1.request, &wrapper1.response, &wrapper1);
    session->sendRequest(&wrapper2.request, &wrapper2.response, &wrapper2);
    EXPECT_EQ(1u, transport.outgoingRequests.size());
    EXPECT_EQ("ALL_DATA FROM_CLIENT, rpcId 666.1 012345678",
            driver->outputLog);
}

TEST_F(BasicTransportTest, handlePacket_noHeader) {
    struct msg {
        char body[6];
    };
    msg msg;
    handlePacket("mock:client=1", msg, NULL);
    Transport::ServerRpc* serverRpc =
        context.workerManager->waitForRpc(0);
    EXPECT_TRUE(serverRpc == NULL);
    EXPECT_EQ("handlePacket: packet from mock:client=1 too short (6 bytes)",
            TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_timeTraceFromServerUnknownSequence) {
    handlePacket("mock:server=1",
            BasicTransport::LogTimeTraceHeader(BasicTransport::RpcId(666, 1),
            BasicTransport::FROM_SERVER));
    WorkerTimer::sync();
    EXPECT_TRUE(TestUtil::contains(TimeTrace::getTrace(),
            "client received LOG_TIME_TRACE"));
    EXPECT_EQ(0u, Driver::Received::stealCount);
}
TEST_F(BasicTransportTest, handlePacket_packetFromServerWithUnknownSequence) {
    handlePacket("mock:server=1",
            BasicTransport::AllDataHeader(BasicTransport::RpcId(666, 1),
            BasicTransport::FROM_SERVER, 9), "response1");
    EXPECT_EQ("handlePacket: Discarding unknown packet, sequence 1",
            TestLog::get());
    EXPECT_EQ(0u, Driver::Received::stealCount);
}
TEST_F(BasicTransportTest, handlePacket_allDataFromServer_basics) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    handlePacket("mock:server=1",
            BasicTransport::AllDataHeader(BasicTransport::RpcId(666, 1),
            BasicTransport::FROM_SERVER, 9), "response1");
    EXPECT_STREQ("completed: 1, failed: 0", wrapper.getState());
    EXPECT_EQ("response1", TestUtil::toString(&wrapper.response));
    EXPECT_EQ(0lu, transport.outgoingRpcs.size());
    EXPECT_EQ(1u, Driver::Received::stealCount);
}
TEST_F(BasicTransportTest, handlePacket_allDataFromServer_tooShort) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    handlePacket("mock:server=1",
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
    handlePacket("mock:server=1", BasicTransport::CommonHeader(
            BasicTransport::DATA, BasicTransport::RpcId(666, 1),
            BasicTransport::FROM_SERVER));
    EXPECT_EQ("handlePacket: packet of type DATA from mock:server=1 too "
            "short (18 bytes)", TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_dataFromServer_basics) {
    MockWrapper wrapper("message1");
    transport.maxDataPerPacket = 5;
    transport.roundTripBytes = 1000;
    uint32_t unscheduledBytes = 0;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    transport.grantIncrement = 500;

    // First packet of response.
    handlePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 10, 0,
            unscheduledBytes, BasicTransport::FROM_SERVER), "abcde");
    EXPECT_STREQ("completed: 0, failed: 0", wrapper.getState());
    BasicTransport::ScheduledMessage* response = transport.messagesToGrant[0];
    EXPECT_EQ(BasicTransport::RpcId(666, 1), response->rpcId);
    EXPECT_EQ(1505u, response->grantOffset);
    EXPECT_EQ("abcde", TestUtil::toString(&wrapper.response));
    EXPECT_EQ(1u, Driver::Received::stealCount);

    // Second packet of response
    driver->outputLog.clear();
    handlePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 10, 5,
            unscheduledBytes, BasicTransport::FROM_SERVER), "12345");
    EXPECT_STREQ("completed: 1, failed: 0", wrapper.getState());
    EXPECT_EQ("abcde12345", TestUtil::toString(&wrapper.response));
    EXPECT_EQ("", driver->outputLog);
    EXPECT_EQ(0lu, transport.outgoingRpcs.size());
    EXPECT_EQ(1u, Driver::Received::stealCount);
}
TEST_F(BasicTransportTest, handlePacket_dataFromServer_extraData) {
    MockWrapper wrapper("message1");
    transport.maxDataPerPacket = 5;
    transport.roundTripBytes = 1000;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    transport.grantIncrement = 500;

    // First packet of response.
    handlePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 6, 0,
            unscheduledBytes, BasicTransport::FROM_SERVER), "abcde");

    // Final packet of response has extra data.
    driver->outputLog.clear();
    handlePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 6, 5,
            unscheduledBytes, BasicTransport::FROM_SERVER), "12345");
    EXPECT_STREQ("completed: 1, failed: 0", wrapper.getState());
    EXPECT_EQ("abcde1", TestUtil::toString(&wrapper.response));
}
TEST_F(BasicTransportTest, handlePacket_dataFromServer_dontIssueGrant) {
    MockWrapper wrapper("message1");
    transport.maxDataPerPacket = 5;
    transport.roundTripBytes = 1000;
    uint32_t unscheduledBytes = 0;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    transport.grantIncrement = 500;

    // First packet of response; send a grant
    handlePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 15, 0,
            unscheduledBytes, BasicTransport::FROM_SERVER), "abcde");
    BasicTransport::ScheduledMessage* response = transport.messagesToGrant[0];
    EXPECT_EQ(666u, response->rpcId.clientId);
    EXPECT_EQ(1u, response->rpcId.sequence);
    EXPECT_EQ(1505u, response->grantOffset);

    // Second packet of response (still not complete, but no need for
    // another grant).
    driver->outputLog.clear();
    handlePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 15,
            10, unscheduledBytes, BasicTransport::FROM_SERVER),
            "12345");
    EXPECT_EQ("", driver->outputLog);
    EXPECT_EQ(1lu, transport.outgoingRpcs.size());

    // Third packet of response (now complete)
    driver->outputLog.clear();
    handlePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 15, 5,
            unscheduledBytes, BasicTransport::FROM_SERVER), "xyzzy");
    EXPECT_STREQ("completed: 1, failed: 0", wrapper.getState());
    EXPECT_EQ(0lu, transport.outgoingRpcs.size());
    EXPECT_EQ("abcdexyzzy12345", TestUtil::toString(&wrapper.response));
}
TEST_F(BasicTransportTest, handlePacket_grantFromServer_incompleteHeader) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    handlePacket("mock:server=1", BasicTransport::CommonHeader(
            BasicTransport::GRANT, BasicTransport::RpcId(666, 1),
            BasicTransport::FROM_SERVER));
    EXPECT_EQ("handlePacket: packet of type GRANT from mock:server=1 too "
            "short (18 bytes)", TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_grantFromServer) {
    MockWrapper wrapper("abcdefghij0123456789");
    transport.roundTripBytes = 10;
    transport.maxDataPerPacket = 10;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    BasicTransport::ClientRpc* clientRpc = transport.outgoingRpcs[1];
    EXPECT_EQ(10lu, clientRpc->request.transmitLimit);

    // First grant doesn't get past transmitLimit.
    handlePacket("mock:server=1",
            BasicTransport::GrantHeader(BasicTransport::RpcId(666, 1), 10,
            BasicTransport::FROM_SERVER));
    EXPECT_EQ(10lu, clientRpc->request.transmitLimit);

    // Second grant is far enough out to enable more bytes to be sent.
    handlePacket("mock:server=1",
            BasicTransport::GrantHeader(BasicTransport::RpcId(666, 1), 15,
            BasicTransport::FROM_SERVER));
    EXPECT_EQ(15lu, clientRpc->request.transmitLimit);
}
TEST_F(BasicTransportTest, handlePacket_logTimeTraceFromServer) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    TimeTrace::reset();

    handlePacket("mock:server=1", BasicTransport::LogTimeTraceHeader(
            BasicTransport::RpcId(666, 1), BasicTransport::FROM_SERVER));
    WorkerTimer::sync();
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "client received LOG_TIME_TRACE"));
}

TEST_F(BasicTransportTest, handlePacket_resendFromServer_incompleteHeader) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    handlePacket("mock:server=1", BasicTransport::CommonHeader(
            BasicTransport::RESEND, BasicTransport::RpcId(666, 1),
            BasicTransport::FROM_SERVER));
    EXPECT_EQ("handlePacket: packet of type RESEND from mock:server=1 too "
            "short (18 bytes)", TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_resendFromServer_restart) {
    MockWrapper wrapper("abcdefghij0123456789");
    transport.roundTripBytes = 10;
    transport.maxDataPerPacket = 10;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    transport.poller.poll();
    EXPECT_EQ("DATA FROM_CLIENT, rpcId 666.1, totalLength 20, offset 0 "
            "abcdefghij",
            driver->outputLog);
    driver->outputLog.clear();
    BasicTransport::OutgoingMessage* request =
            &transport.outgoingRpcs[1]->request;
    EXPECT_EQ(10u, request->transmitOffset);

    handlePacket("mock:server=1", BasicTransport::ResendHeader(
            BasicTransport::RpcId(666, 1), 0, 5,
            BasicTransport::FROM_SERVER|BasicTransport::RESTART));
    EXPECT_EQ(0u, request->transmitOffset);
    EXPECT_EQ(5u, request->transmitLimit);
}
TEST_F(BasicTransportTest,
        handlePacket_resendFromServer_transmitLimitChanges) {
    MockWrapper wrapper("abcdefghij0123456789");
    transport.roundTripBytes = 10;
    transport.maxDataPerPacket = 10;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    BasicTransport::OutgoingMessage* request =
            &transport.outgoingRpcs[1]->request;
    EXPECT_EQ(10lu, request->transmitLimit);
    driver->outputLog.clear();

    handlePacket("mock:server=1", BasicTransport::ResendHeader(
            BasicTransport::RpcId(666, 1), 10, 8, BasicTransport::FROM_SERVER));
    EXPECT_EQ("BUSY FROM_CLIENT, rpcId 666.1",
            driver->outputLog);
    EXPECT_EQ(18u, request->transmitLimit);
    EXPECT_EQ(10u, request->transmitOffset);
}
TEST_F(BasicTransportTest, handlePacket_resendFromServer_sendBusy) {
    driver->transmitQueueSpace = 0;
    MockWrapper wrapper("abcdefghij0123456789");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();

    // Case 1: data hasn't actually been transmitted yet.
    handlePacket("mock:server=1", BasicTransport::ResendHeader(
            BasicTransport::RpcId(666, 1), 0, 10,
            BasicTransport::FROM_SERVER));
    EXPECT_EQ("BUSY FROM_CLIENT, rpcId 666.1", driver->outputLog);

    // Case 2: data was sent shortly before RESEND arrived.
    Cycles::mockTscValue = 1000000;
    driver->transmitQueueSpace = 10;
    transport.tryToTransmitData();
    driver->outputLog.clear();
    Cycles::mockTscValue += transport.timerInterval - 10;
    handlePacket("mock:server=1", BasicTransport::ResendHeader(
            BasicTransport::RpcId(666, 1), 0, 10,
            BasicTransport::FROM_SERVER));
    EXPECT_EQ("BUSY FROM_CLIENT, rpcId 666.1", driver->outputLog);
    Cycles::mockTscValue = 0;
}
TEST_F(BasicTransportTest, handlePacket_resendFromServer_resend) {
    Cycles::mockTscValue = 1000000;
    transport.timerInterval = 1000;
    MockWrapper wrapper("abcdefghij0123456789");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();

    Cycles::mockTscValue += transport.timerInterval + 10;
    handlePacket("mock:server=1", BasicTransport::ResendHeader(
            BasicTransport::RpcId(666, 1), 12, 20,
            BasicTransport::FROM_SERVER));
    EXPECT_EQ("DATA FROM_CLIENT, rpcId 666.1, totalLength 20, offset 12, "
            "RETRANSMISSION 23456789",
            driver->outputLog);
    EXPECT_EQ(1001010lu, transport.outgoingRpcs[1]->request.lastTransmitTime);
    Cycles::mockTscValue = 0;
}
TEST_F(BasicTransportTest, handlePacket_busyFromServer) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    transport.outgoingRpcs[1]->silentIntervals = 2;
    handlePacket("mock:server=1", BasicTransport::BusyHeader(
            BasicTransport::RpcId(666, 1), BasicTransport::FROM_SERVER));
    EXPECT_EQ(0u, transport.outgoingRpcs[1]->silentIntervals);
}
TEST_F(BasicTransportTest, handlePacket_unknownOpcodeFromServer) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    handlePacket("mock:server=1", BasicTransport::CommonHeader(
            BasicTransport::BOGUS, BasicTransport::RpcId(666, 1),
            BasicTransport::FROM_SERVER));
    EXPECT_EQ("handlePacket: unexpected opcode 27 received from "
            "server mock:server=1", TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_allDataFromClient) {
    handlePacket("mock:client=1",
            BasicTransport::AllDataHeader(BasicTransport::RpcId(100, 101),
            BasicTransport::FROM_CLIENT, 8), "message1");
    BasicTransport::ServerRpc* serverRpc =
            static_cast<BasicTransport::ServerRpc*>(
            context.workerManager->waitForRpc(0));
    ASSERT_TRUE(serverRpc != NULL);
    EXPECT_EQ("message1", TestUtil::toString(&serverRpc->requestPayload));
    EXPECT_TRUE(serverRpc->requestComplete);
    EXPECT_EQ(2u, transport.nextServerSequenceNumber);
}
TEST_F(BasicTransportTest, handlePacket_allDataFromClient_duplicate) {
    handlePacket("mock:client=1",
            BasicTransport::AllDataHeader(BasicTransport::RpcId(100, 101),
            BasicTransport::FROM_CLIENT, 8), "message1");
    BasicTransport::ServerRpc* serverRpc =
            static_cast<BasicTransport::ServerRpc*>(
            context.workerManager->waitForRpc(0));
    ASSERT_TRUE(serverRpc != NULL);
    handlePacket("mock:client=1",
            BasicTransport::AllDataHeader(BasicTransport::RpcId(100, 101),
            BasicTransport::FROM_CLIENT, 20), "0123457890abcdefghij");
    EXPECT_EQ("message1", TestUtil::toString(&serverRpc->requestPayload));
}
TEST_F(BasicTransportTest, handlePacket_allDataFromClient_tooShort) {
    handlePacket("mock:client=1",
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
    transport.maxDataPerPacket = 5;
    transport.grantIncrement = 500;
    uint32_t unscheduledBytes = 10;
    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15,
            5, unscheduledBytes, BasicTransport::FROM_CLIENT),
            "56789");
    BasicTransport::ServerRpcMap::iterator it = transport.incomingRpcs.find(
            BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    BasicTransport::ServerRpc* serverRpc = it->second;
    EXPECT_FALSE(serverRpc->requestComplete);
    BasicTransport::ScheduledMessage* response = transport.messagesToGrant[0];
    EXPECT_EQ(BasicTransport::RpcId(100, 101), response->rpcId);
    EXPECT_EQ(1500u, response->grantOffset);
    EXPECT_EQ(2u, transport.nextServerSequenceNumber);

    // Second packet: no GRANT should result.
    driver->outputLog.clear();
    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15,
            0, unscheduledBytes, BasicTransport::FROM_CLIENT),
            "01234");
    EXPECT_FALSE(serverRpc->requestComplete);
    EXPECT_EQ(1500u, serverRpc->scheduledMessage->grantOffset);
    EXPECT_EQ("", driver->outputLog);
    EXPECT_EQ(1lu, transport.incomingRpcs.size());
    EXPECT_EQ(1lu, transport.serverTimerList.size());

    // Third packet: no GRANT, message should now be complete.
    driver->outputLog.clear();
    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15, 10,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "abcde");
    EXPECT_TRUE(serverRpc->requestComplete);
    EXPECT_EQ("", driver->outputLog);
    EXPECT_EQ("0123456789abcde",
            TestUtil::toString(&serverRpc->requestPayload));
}
TEST_F(BasicTransportTest, handlePacket_dataFromClient_extraBytes) {
    // Send a message in two packets; the second packet contains more
    // than enough data to complete the message.
    transport.roundTripBytes = 1000;
    transport.maxDataPerPacket = 5;
    transport.grantIncrement = 500;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 6,
            0, unscheduledBytes, BasicTransport::FROM_CLIENT),
            "abcde");
    BasicTransport::ServerRpcMap::iterator it = transport.incomingRpcs.find(
            BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    BasicTransport::ServerRpc* serverRpc = it->second;
    EXPECT_FALSE(serverRpc->requestComplete);

    // Second packet.
    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 6,
            5, unscheduledBytes, BasicTransport::FROM_CLIENT),
            "01234");
    EXPECT_TRUE(serverRpc->requestComplete);
    EXPECT_EQ("abcde0",
            TestUtil::toString(&serverRpc->requestPayload));
}
TEST_F(BasicTransportTest, handlePacket_dataFromClient_dontIssueGrant) {
    transport.maxDataPerPacket = 5;
    transport.roundTripBytes = 1000;
    transport.grantIncrement = 500;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15, 10,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "abcde");
    EXPECT_EQ("", driver->outputLog);
}
TEST_F(BasicTransportTest, handlePacket_dataFromClient_extraneousPacket) {
    // Send an extra packet after the message is complete.
    handlePacket("mock:client=1",
            BasicTransport::AllDataHeader(BasicTransport::RpcId(100, 101),
            BasicTransport::FROM_CLIENT, 8), "message1");
    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 13,
            8, 1000, BasicTransport::FROM_CLIENT),
            "abcde");
    EXPECT_EQ("handlePacket: ignoring extraneous packet | "
              "handlePacket: Redundant DATA from client, clientId 100, "
              "sequence 101, offset 8, totalLength 13", TestLog::get());
    BasicTransport::ServerRpc* serverRpc =
            dynamic_cast<BasicTransport::ServerRpc*>(
            context.workerManager->waitForRpc(0));
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("message1", TestUtil::toString(&serverRpc->requestPayload));
}
TEST_F(BasicTransportTest, handlePacket_grantFromClient_bogusGrants) {
    prepareToRespond();
    transport.roundTripBytes = 5;
    transport.maxDataPerPacket = 5;

    // GRANT arriving for unknown RpcID: bogus.
    handlePacket("mock:client=1",
            BasicTransport::GrantHeader(BasicTransport::RpcId(5, 6), 10,
            BasicTransport::FROM_CLIENT));
    EXPECT_EQ("handlePacket: unexpected GRANT from client mock:client=1, "
            "id (5,6), grantOffset 10, serverRpc not found",
            TestLog::get());

    // GRANT arriving before result transmission starts: bogus.
    TestLog::reset();
    handlePacket("mock:client=1",
            BasicTransport::GrantHeader(BasicTransport::RpcId(100, 101), 10,
            BasicTransport::FROM_CLIENT));
    EXPECT_EQ("handlePacket: unexpected GRANT from client mock:client=1, "
            "id (100,101), grantOffset 10, serverRpc not sending response",
            TestLog::get());

    // GRANT packet too short: bogus.
    TestLog::reset();
    handlePacket("mock:client=1", BasicTransport::CommonHeader(
            BasicTransport::GRANT, BasicTransport::RpcId(100, 101),
            BasicTransport::FROM_CLIENT));
    EXPECT_EQ("handlePacket: packet of type GRANT from mock:client=1 "
            "too short (18 bytes)", TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_grantFromClient) {
    transport.roundTripBytes = 5;
    transport.maxDataPerPacket = 5;
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    serverRpc->sendReply();
    EXPECT_EQ("DATA FROM_SERVER, rpcId 100.101, totalLength 20, offset 0 "
            "01234",
            driver->outputLog);
    driver->outputLog.clear();

    // First, send redundant grant (do nothing).
    handlePacket("mock:client=1",
            BasicTransport::GrantHeader(BasicTransport::RpcId(100, 101), 5,
            BasicTransport::FROM_CLIENT));
    transport.tryToTransmitData();
    EXPECT_EQ("", driver->outputLog);
    EXPECT_EQ(5u, serverRpc->response.transmitLimit);

    // Second grant should allow more data to be transmitted.
    handlePacket("mock:client=1",
            BasicTransport::GrantHeader(BasicTransport::RpcId(100, 101), 15,
            BasicTransport::FROM_CLIENT));
    transport.tryToTransmitData();
    EXPECT_EQ("DATA FROM_SERVER, rpcId 100.101, totalLength 20, offset 5 "
            "56789 | "
            "DATA FROM_SERVER, rpcId 100.101, totalLength 20, offset 10 "
            "abcde",
            driver->outputLog);
    EXPECT_EQ(15u, serverRpc->response.transmitLimit);

    // Third grant should complete the result transmission.
    driver->outputLog.clear();
    handlePacket("mock:client=1",
            BasicTransport::GrantHeader(BasicTransport::RpcId(100, 101), 25,
            BasicTransport::FROM_CLIENT));
    EXPECT_EQ(20u, serverRpc->response.transmitLimit);
    transport.tryToTransmitData();
    EXPECT_EQ("DATA FROM_SERVER, rpcId 100.101, totalLength 20, offset 15 "
            "fghij",
            driver->outputLog);
    EXPECT_EQ(0lu, transport.incomingRpcs.size());
    EXPECT_EQ(0lu, transport.serverTimerList.size());
    EXPECT_EQ(0lu, transport.serverRpcPool.outstandingAllocations);
}
TEST_F(BasicTransportTest, handlePacket_resendFromClient_packetTooShort) {
    prepareToRespond();
    transport.roundTripBytes = 5;
    transport.maxDataPerPacket = 5;
    TestLog::reset();
    handlePacket("mock:client=1", BasicTransport::CommonHeader(
            BasicTransport::RESEND, BasicTransport::RpcId(100, 101),
            BasicTransport::FROM_CLIENT));
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "handlePacket: packet of type RESEND from mock:client=1 "
            "too short (18 bytes)"));
}
TEST_F(BasicTransportTest, handlePacket_resendFromClient_unknownRpcId) {
    handlePacket("mock:client=1",
            BasicTransport::ResendHeader(BasicTransport::RpcId(10, 11),
            10, 5, BasicTransport::FROM_CLIENT));
    EXPECT_EQ("RESEND FROM_SERVER, rpcId 10.11, offset 0, length 10960, "
            "RESTART", driver->outputLog);
}
TEST_F(BasicTransportTest,
        handlePacket_resendFromClient_transmitLimitChanges) {
    transport.roundTripBytes = 15;
    transport.maxDataPerPacket = 15;
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    serverRpc->sendReply();

    driver->outputLog.clear();
    handlePacket("mock:client=1",
            BasicTransport::ResendHeader(BasicTransport::RpcId(100, 101),
            15, 8, BasicTransport::FROM_CLIENT));
    EXPECT_EQ("BUSY FROM_SERVER, rpcId 100.101", driver->outputLog);
    EXPECT_EQ(15u, serverRpc->response.transmitOffset);
    EXPECT_EQ(20u, serverRpc->response.transmitLimit);
}
TEST_F(BasicTransportTest, handlePacket_resendFromClient_sendBusy) {
    driver->transmitQueueSpace = 0;
    transport.roundTripBytes = 15;
    transport.maxDataPerPacket = 15;
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();

    // Case 1: sendReply hasn't been called yet
    handlePacket("mock:client=1",
            BasicTransport::ResendHeader(BasicTransport::RpcId(100, 101),
            10, 5, BasicTransport::FROM_CLIENT));
    EXPECT_EQ("BUSY FROM_SERVER, rpcId 100.101",
            driver->outputLog);

    // Case 2: the response is ready, but no bytes have been transmitted yet.
    driver->outputLog.clear();
    serverRpc->sendReply();
    handlePacket("mock:client=1",
            BasicTransport::ResendHeader(BasicTransport::RpcId(100, 101),
            5, 8, BasicTransport::FROM_CLIENT));
    EXPECT_EQ("BUSY FROM_SERVER, rpcId 100.101",
            driver->outputLog);

    // Case 3: we sent the bytes, but only recently.
    Cycles::mockTscValue = 1000000;
    driver->transmitQueueSpace = 100;
    transport.tryToTransmitData();
    driver->outputLog.clear();
    Cycles::mockTscValue += transport.timerInterval - 10;
    handlePacket("mock:client=1",
            BasicTransport::ResendHeader(BasicTransport::RpcId(100, 101),
            5, 8, BasicTransport::FROM_CLIENT));
    EXPECT_EQ("BUSY FROM_SERVER, rpcId 100.101",
            driver->outputLog);
    Cycles::mockTscValue = 0;
}
TEST_F(BasicTransportTest, handlePacket_resendFromClient_sendBytes) {
    Cycles::mockTscValue = 1000000;
    transport.timerInterval = 1000;
    transport.roundTripBytes = 15;
    transport.maxDataPerPacket = 15;
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    serverRpc->sendReply();

    driver->outputLog.clear();
    Cycles::mockTscValue += transport.timerInterval + 10;
    handlePacket("mock:client=1",
            BasicTransport::ResendHeader(BasicTransport::RpcId(100, 101),
            0, 15, BasicTransport::FROM_CLIENT));
    EXPECT_EQ("DATA FROM_SERVER, rpcId 100.101, totalLength 20, offset 0, "
            "RETRANSMISSION 0123456789 (+5 more)",
            driver->outputLog);
    EXPECT_EQ(1001010lu, serverRpc->response.lastTransmitTime);
    Cycles::mockTscValue = 0;
}
TEST_F(BasicTransportTest, handlePacket_busyFromClient) {
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    serverRpc->silentIntervals = 2;
    handlePacket("mock:client=1", BasicTransport::BusyHeader(
            BasicTransport::RpcId(100, 101), BasicTransport::FROM_CLIENT));
    EXPECT_EQ(0u, serverRpc->silentIntervals);
}
TEST_F(BasicTransportTest, handlePacket_abortFromClient_sendingResponse) {
    // Cancel a ServerRpc that is sending response.
    driver->transmitQueueSpace = 0;
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    serverRpc->sendReply();
    EXPECT_EQ(1u, transport.incomingRpcs.size());
    handlePacket("mock:client=1", BasicTransport::AbortHeader(
            BasicTransport::RpcId(100, 101)));
    EXPECT_EQ(0u, transport.incomingRpcs.size());
}
TEST_F(BasicTransportTest, handlePacket_abortFromClient_localExecution) {
    // Cancel a ServerRpc that is being executed locally.
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    EXPECT_EQ(1u, transport.incomingRpcs.size());
    handlePacket("mock:client=1", BasicTransport::AbortHeader(
            BasicTransport::RpcId(100, 101)));
    EXPECT_TRUE(serverRpc->cancelled);
    serverRpc->sendReply();
    EXPECT_EQ("", driver->outputLog);
    EXPECT_EQ(0u, transport.incomingRpcs.size());
}
TEST_F(BasicTransportTest, handlePacket_unknownOpcodeFromClient) {
    handlePacket("mock:client=1", BasicTransport::CommonHeader(
            BasicTransport::BOGUS, BasicTransport::RpcId(100, 101),
            BasicTransport::FROM_CLIENT));
    EXPECT_EQ("handlePacket: unexpected opcode 27 received from client "
            "mock:client=1", TestLog::get());
}

TEST_F(BasicTransportTest, sendReply_basics) {
    transport.roundTripBytes = 10;
    transport.maxDataPerPacket = 10;
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    serverRpc->sendReply();
    EXPECT_EQ(10lu, serverRpc->response.transmitLimit);
    EXPECT_EQ(1lu, transport.outgoingResponses.size());
    EXPECT_EQ(1lu, transport.serverTimerList.size());
}
TEST_F(BasicTransportTest, sendReply_smallMessage) {
    // Small messages are passed to the NIC directly, bypassing the
    // sender's congestion control mechanism in tryToTransmitData.
    driver->transmitQueueSpace = -1;
    transport.smallMessageThreshold = 10;
    BasicTransport::ServerRpc* serverRpc1 = prepareToRespond();
    BasicTransport::ServerRpc* serverRpc2 =
            prepareToRespond(102, 9, "012345678");
    serverRpc1->sendReply();
    serverRpc2->sendReply();
    EXPECT_EQ(1u, transport.outgoingResponses.size());
    EXPECT_EQ("ALL_DATA FROM_SERVER, rpcId 100.102 012345678",
            driver->outputLog);
}

TEST_F(BasicTransportTest, MessageAccumulator_destructor) {
    transport.maxDataPerPacket = 5;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 25, 10,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "abcde");
    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 25, 20,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "56789");
    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 25, 15,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "01234");
    BasicTransport::ServerRpc* serverRpc =
            transport.incomingRpcs[BasicTransport::RpcId(100, 101)];
    EXPECT_TRUE(serverRpc->accumulator);
    EXPECT_EQ(3u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ(0u, driver->releaseCount);
    transport.deleteServerRpc(serverRpc);
    EXPECT_EQ(3u, driver->releaseCount);
}

TEST_F(BasicTransportTest, addPacket_basics) {
    // Receive a request in 5 packets, in the order P4, P2, P0, P1, P3.
    transport.maxDataPerPacket = 5;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 25, 20,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "P4444");
    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 25, 10,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "P2222");
    BasicTransport::ServerRpcMap::iterator it =
            transport.incomingRpcs.find(BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    BasicTransport::ServerRpc* serverRpc = it->second;
    ASSERT_TRUE(serverRpc->accumulator);
    EXPECT_EQ(2u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ(2u, Driver::Received::stealCount);

    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 25, 0,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "P0000");
    EXPECT_EQ(2u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ("P0000",
            TestUtil::toString(&serverRpc->requestPayload));

    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 25, 5,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "P1111");
    EXPECT_EQ(1u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ("P0000P1111P2222",
            TestUtil::toString(&serverRpc->requestPayload));

    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 25, 15,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "P3333");
    EXPECT_EQ(0u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ("P0000P1111P2222P3333P4444",
            TestUtil::toString(&serverRpc->requestPayload));
    EXPECT_EQ(3u, Driver::Received::stealCount);
}
TEST_F(BasicTransportTest, addPacket_skipRedundantPacket) {
    // Receive two duplicate packets that contain bytes 10-14.
    transport.maxDataPerPacket = 5;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15, 10,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "abcde");
    BasicTransport::ServerRpcMap::iterator it =
            transport.incomingRpcs.find(BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    BasicTransport::ServerRpc* serverRpc = it->second;
    EXPECT_EQ(1u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ(1u, Driver::Received::stealCount);
    EXPECT_EQ(0u, driver->releaseCount);
    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15, 10,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "abcde");
    EXPECT_EQ(1u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ(1u, Driver::Received::stealCount);
    EXPECT_EQ(1u, driver->releaseCount);

    // Receive another two duplicate packets that contain bytes 0-4.
    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15, 0,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "01234");
    EXPECT_EQ(1u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ(2u, Driver::Received::stealCount);
    EXPECT_EQ(1u, driver->releaseCount);
    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15, 0,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "01234");
    EXPECT_EQ(1u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ(2u, Driver::Received::stealCount);
    EXPECT_EQ(2u, driver->releaseCount);
}

TEST_F(BasicTransportTest, requestRetransmission) {
    transport.roundTripBytes = 100;
    transport.maxDataPerPacket = 5;
    uint32_t unscheduledBytes = transport.roundTripBytes;

    // First retransmit: no fragments waiting, no grants sent.
    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20, 0,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "01234");
    BasicTransport::ServerRpcMap::iterator it =
            transport.incomingRpcs.find(BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    BasicTransport::ServerRpc* serverRpc = it->second;
    ASSERT_TRUE(serverRpc->accumulator);
    EXPECT_EQ(0u, serverRpc->accumulator->fragments.size());

    driver->outputLog.clear();
    uint32_t limit = serverRpc->accumulator->requestRetransmission(
            &transport, &address1, BasicTransport::RpcId(100, 101), 0,
            BasicTransport::FROM_SERVER);
    EXPECT_EQ(100u, limit);
    EXPECT_EQ("RESEND FROM_SERVER, rpcId 100.101, offset 5, length 95",
            driver->outputLog);

    // Second retransmit: no fragment, but grant was sent.
    driver->outputLog.clear();
    limit = serverRpc->accumulator->requestRetransmission(
            &transport, &address1, BasicTransport::RpcId(100, 101), 25,
            BasicTransport::FROM_SERVER);
    EXPECT_EQ(25u, limit);
    EXPECT_EQ("RESEND FROM_SERVER, rpcId 100.101, offset 5, length 20",
            driver->outputLog);

    // Third retransmit: fragment waiting.
    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20, 15,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "abcde");
    driver->outputLog.clear();
    limit = serverRpc->accumulator->requestRetransmission(
            &transport, &address1, BasicTransport::RpcId(100, 101), 25,
            BasicTransport::FROM_SERVER);
    EXPECT_EQ(15u, limit);
    EXPECT_EQ("RESEND FROM_SERVER, rpcId 100.101, offset 5, length 10",
            driver->outputLog);
}

TEST_F(BasicTransportTest, poll_nothingToDo) {
    transport.nextTimeoutCheck = ~0;
    int result = transport.poller.poll();
    EXPECT_EQ(0, result);
}
TEST_F(BasicTransportTest, poll_incomingPackets) {
    transport.maxDataPerPacket = 5;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    // Send 3 fragments, process all at once.
    driver->packetArrived("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15, 0,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "01234");
    driver->packetArrived("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15, 5,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "56789");
    driver->packetArrived("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15, 10,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "ABCDE");
    int result = transport.poller.poll();
    BasicTransport::ServerRpcMap::iterator it =
            transport.incomingRpcs.find(BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    BasicTransport::ServerRpc* serverRpc = it->second;
    EXPECT_EQ("0123456789ABCDE",
            TestUtil::toString(&serverRpc->requestPayload));
    EXPECT_EQ(1u, Driver::Received::stealCount);
    EXPECT_EQ(1, result);
}
TEST_F(BasicTransportTest, poll_callCheckTimeouts) {
    Cycles::mockTscValue = 100000;
    transport.poller.owner->currentTime = Cycles::rdtsc();
    transport.timerInterval = 1000;
    transport.nextTimeoutCheck = 102000;
    MockWrapper wrapper1("012345678901234");
    session->sendRequest(&wrapper1.request, &wrapper1.response, &wrapper1);
    BasicTransport::ClientRpc* clientRpc = transport.outgoingRpcs[1];

    // First call: timerInterval not reached.
    int result = transport.poller.poll();
    EXPECT_EQ(0u, clientRpc->silentIntervals);
    EXPECT_EQ(0, result);

    // Second call: timerInterval reached, but lots of input packets
    Cycles::mockTscValue = 103000;
    transport.poller.owner->currentTime = Cycles::rdtsc();
    BasicTransport::BusyHeader busy(BasicTransport::RpcId(1000, 1001),
            BasicTransport::FROM_CLIENT);
    createInputPackets(8, &busy);
    result = transport.poller.poll();
    EXPECT_EQ(0u, clientRpc->silentIntervals);
    EXPECT_EQ(1, result);
    EXPECT_EQ(102000lu, transport.nextTimeoutCheck);
    EXPECT_EQ(104000lu, transport.timeoutCheckDeadline);

    // Third call: timerInterval reached, no input packets; call checkTimeouts
    result = transport.poller.poll();
    EXPECT_EQ(1u, clientRpc->silentIntervals);
    EXPECT_EQ(1, result);
    EXPECT_EQ(104000lu, transport.nextTimeoutCheck);
    EXPECT_EQ(0lu, transport.timeoutCheckDeadline);

    // Fourth call: deadline reached, lots of input packets; call
    // checkTimeouts anyway.
    createInputPackets(8, &busy);
    Cycles::mockTscValue = 105000;
    transport.poller.owner->currentTime = Cycles::rdtsc();
    transport.timeoutCheckDeadline = 105000;
    result = transport.poller.poll();
    EXPECT_EQ(2u, clientRpc->silentIntervals);
    EXPECT_EQ(1, result);
    EXPECT_EQ(106000lu, transport.nextTimeoutCheck);
    EXPECT_EQ(0lu, transport.timeoutCheckDeadline);

    Cycles::mockTscValue = 0;
}
TEST_F(BasicTransportTest, poll_outgoingGrant) {
    transport.maxDataPerPacket = 5;
    transport.roundTripBytes = 20;
    transport.grantIncrement = 5;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    // Send 3 fragments, process all at once.
    driver->packetArrived("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 100, 0,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "01234");
    driver->packetArrived("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 100, 5,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "56789");
    driver->packetArrived("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 100, 10,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "abcde");
    driver->packetArrived("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 102), 100, 0,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "ABCDE");
    int result = transport.poller.poll();
    EXPECT_EQ("GRANT FROM_SERVER, rpcId 100.101, offset 40 | "
            "GRANT FROM_SERVER, rpcId 100.102, offset 30",
            driver->outputLog);
    EXPECT_EQ(1, result);
    EXPECT_EQ(0u, transport.messagesToGrant.size());
}
TEST_F(BasicTransportTest, poll_outgoingPacket) {
    driver->transmitQueueSpace = 0;
    MockWrapper wrapper1("012345678901234");
    session->sendRequest(&wrapper1.request, &wrapper1.response, &wrapper1);
    EXPECT_EQ("", driver->outputLog);
    driver->transmitQueueSpace = 10000;
    int result = transport.poller.poll();
    EXPECT_EQ("ALL_DATA FROM_CLIENT, rpcId 666.1 0123456789 (+5 more)",
            driver->outputLog);
    EXPECT_GT(result, 1);
}
TEST_F(BasicTransportTest, checkTimeouts_clientTransmissionNotStartedYet) {
    driver->transmitQueueSpace = 0;
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    BasicTransport::ClientRpc* clientRpc = transport.outgoingRpcs[1lu];
    transport.checkTimeouts();
    EXPECT_EQ(0u, clientRpc->silentIntervals);
    transport.checkTimeouts();
    transport.checkTimeouts();
    EXPECT_EQ(0u, clientRpc->silentIntervals);
}
TEST_F(BasicTransportTest, checkTimeouts_clientPingAndAbort) {
    transport.roundTripBytes = 100;
    MockWrapper wrapper(NULL);
    WireFormat::RequestCommon* header =
            wrapper.request.emplaceAppend<WireFormat::RequestCommon>();
    header->opcode = WireFormat::READ;
    header->service = WireFormat::MASTER_SERVICE;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    BasicTransport::ClientRpc* clientRpc = transport.outgoingRpcs[1lu];
    driver->outputLog.clear();
    transport.timeoutIntervals = 2*transport.pingIntervals+1;

    // Nothing should get logged for the first few calls to checkTimeouts.
    transport.checkTimeouts();
    transport.checkTimeouts();
    EXPECT_EQ("", TestLog::get());
    EXPECT_EQ(2u, clientRpc->silentIntervals);

    // The next call should ping.
    clientRpc->silentIntervals = transport.pingIntervals - 1;
    transport.checkTimeouts();
    EXPECT_EQ("RESEND FROM_CLIENT, rpcId 666.1, offset 0, length 100",
            driver->outputLog);
    driver->outputLog.clear();
    TestLog::reset();

    // Wait a while longer and make sure that the client eventually
    // aborts the request.
    for (int i = 0; i < 100; i++) {
        transport.checkTimeouts();
        if (wrapper.failedCount != 0) {
            break;
        }
    }
    EXPECT_EQ(1, wrapper.failedCount);
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "checkTimeouts: aborting READ RPC to server mock:node=3, "
            "sequence 1: timeout"));
}
TEST_F(BasicTransportTest, checkTimeouts_sendResendFromClient) {
    transport.roundTripBytes = 100;
    transport.maxDataPerPacket = 5;
    transport.grantIncrement = 50;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    MockWrapper wrapper(NULL);
    WireFormat::RequestCommon* header =
            wrapper.request.emplaceAppend<WireFormat::RequestCommon>();
    header->opcode = WireFormat::READ;
    header->service = WireFormat::MASTER_SERVICE;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    handlePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 200,
            0, unscheduledBytes, BasicTransport::FROM_SERVER),
            "abcde");
    driver->outputLog.clear();

    transport.checkTimeouts();
    transport.checkTimeouts();
    EXPECT_EQ("", TestLog::get());

    transport.checkTimeouts();
    EXPECT_EQ("RESEND FROM_CLIENT, rpcId 666.1, offset 5, length 150",
            driver->outputLog);

    // If a packet arrives, RESENDS stop (for a while).
    driver->outputLog.clear();
    handlePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 200, 5,
            unscheduledBytes, BasicTransport::FROM_SERVER), "fghij");
    transport.checkTimeouts();
    transport.checkTimeouts();
    EXPECT_EQ("", driver->outputLog);

    transport.checkTimeouts();
    EXPECT_EQ("RESEND FROM_CLIENT, rpcId 666.1, offset 10, length 145",
            driver->outputLog);
}
TEST_F(BasicTransportTest, checkTimeouts_serverResponseTransmissionDelayed) {
    driver->transmitQueueSpace = 0;
    transport.roundTripBytes = 15;
    transport.maxDataPerPacket = 15;
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    serverRpc->sendReply();

    transport.checkTimeouts();
    EXPECT_EQ(0u, serverRpc->silentIntervals);
    transport.checkTimeouts();
    transport.checkTimeouts();
    EXPECT_EQ(0u, serverRpc->silentIntervals);
}
TEST_F(BasicTransportTest, checkTimeouts_serverAbortsRequest) {
    transport.maxDataPerPacket = 5;
    transport.timeoutIntervals = 2;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15, 0,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "abcde");

    transport.checkTimeouts();
    EXPECT_EQ("", TestLog::get());

    transport.checkTimeouts();
    EXPECT_EQ("deleteServerRpc: RpcId (100, 101)",
            TestLog::get());
}
TEST_F(BasicTransportTest, checkTimeouts_sendResendFromServer) {
    transport.roundTripBytes = 100;
    transport.maxDataPerPacket = 5;
    transport.grantIncrement = 50;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15, 0,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "abcde");
    BasicTransport::ServerRpcMap::iterator it =
            transport.incomingRpcs.find(BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    driver->outputLog.clear();

    transport.checkTimeouts();
    EXPECT_EQ("", TestLog::get());

    transport.checkTimeouts();
    EXPECT_EQ("RESEND FROM_SERVER, rpcId 100.101, offset 5, length 95",
            driver->outputLog);

    // Packet arrival stops resends (for a while).
    handlePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15, 5,
            unscheduledBytes, BasicTransport::FROM_CLIENT), "fghij");
    driver->outputLog.clear();
    transport.checkTimeouts();
    EXPECT_EQ("", driver->outputLog);

    transport.checkTimeouts();
    EXPECT_EQ("RESEND FROM_SERVER, rpcId 100.101, offset 10, length 90",
            driver->outputLog);
}

}  // namespace RAMCloud
