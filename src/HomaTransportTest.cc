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
#include "HomaTransport.h"
#include "MockDriver.h"
#include "MockWrapper.h"
#include "UdpDriver.h"
#include "WorkerManager.h"

namespace RAMCloud {

class HomaTransportTest : public ::testing::Test {
  public:
    Context context;
    MockDriver* driver;
    HomaTransport transport;
    MockDriver::MockAddress address1;
    MockDriver::MockAddress address2;
    ServiceLocator locator;
    Transport::SessionRef sessionRef;
    HomaTransport::Session* session;
    TestLog::Enable logEnabler;

    HomaTransportTest()
        : context(false)
        , driver(new MockDriver(&context, HomaTransport::headerToString))
        , transport(&context, NULL, driver, true, 666)
        , address1("mock:node=1")
        , address2("mock:node=2")
        , locator("mock:node=3")
        , sessionRef(transport.getSession(&locator))
        , session(static_cast<HomaTransport::Session*>(sessionRef.get()))
        , logEnabler()
    {
        context.workerManager = new WorkerManager(&context, 5);
        context.workerManager->testingSaveRpcs = 1;
        transport.smallMessageThreshold = 0;
        Driver::Received::stealCount = 0;
    }

    ~HomaTransportTest()
    {
        Cycles::mockTscValue = 0;
    }

    HomaTransport::ServerRpc*
    getServerRpc(HomaTransport::RpcId rpcId)
    {
        HomaTransport::ServerRpcMap::iterator it =
                transport.incomingRpcs.find(rpcId);
        return it != transport.incomingRpcs.end() ? it->second : NULL;
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
    HomaTransport::ServerRpc*
    prepareToRespond(uint64_t sequence = 101, uint32_t responseSize = 20,
            const char* responseData = "0123456789abcdefghijABCDEFGHIJ")
    {
        handlePacket("mock:client=1",
                HomaTransport::AllDataHeader(
                HomaTransport::RpcId(100, sequence),
                HomaTransport::FROM_CLIENT, 8), "message1");
        HomaTransport::ServerRpc* serverRpc =
                static_cast<HomaTransport::ServerRpc*>(
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
    DISALLOW_COPY_AND_ASSIGN(HomaTransportTest);
};

TEST_F(HomaTransportTest, sanityCheck) {
    // Create a server and a client and verify that we can
    // send a request, receive it, send a reply, and receive it.
    // Then try a second request with bigger chunks of data.

    ServiceLocator serverLocator("homa+udp: host=localhost, port=11101");
    UdpDriver* serverDriver = new UdpDriver(&context, &serverLocator);
    HomaTransport server(&context, &serverLocator, serverDriver, true, 1);
    UdpDriver* clientDriver = new UdpDriver(&context);
    HomaTransport client(&context, NULL, clientDriver, true, 2);
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

TEST_F(HomaTransportTest, constructor) {
    EXPECT_EQ(10960u, transport.roundTripBytes);
}

TEST_F(HomaTransportTest, deleteClientRpc) {
    string request(10001, 'x');
    MockWrapper wrapper(request.c_str());
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    EXPECT_EQ(1u, transport.clientRpcPool.outstandingObjects);
    transport.deleteClientRpc(transport.outgoingRpcs[1]);
    EXPECT_EQ(0u, transport.clientRpcPool.outstandingObjects);
    EXPECT_EQ(0u, transport.outgoingRpcs.size());
}

TEST_F(HomaTransportTest, deleteServerRpc) {
    string response(10001, 'x');
    HomaTransport::ServerRpc* serverRpc = prepareToRespond(101,
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

TEST_F(HomaTransportTest, getRoundTripBytes_basics) {
    transport.maxDataPerPacket = 1500;
    ServiceLocator locator("mock:gbs=8,rttMicros=2");
    EXPECT_EQ(3000u, transport.getRoundTripBytes(&locator));
}
TEST_F(HomaTransportTest, getRoundTripBytes_noGbsOption) {
    transport.maxDataPerPacket = 100;
    ServiceLocator locator("mock:rttMicros=4");
    EXPECT_EQ(5000u, transport.getRoundTripBytes(&locator));
}
TEST_F(HomaTransportTest, getRoundTripBytes_bogusGbsOption) {
    transport.maxDataPerPacket = 100;
    ServiceLocator locator("mock:gbs=xyz,rttMicros=4");
    TestLog::reset();
    EXPECT_EQ(5000u, transport.getRoundTripBytes(&locator));
    EXPECT_EQ("getRoundTripBytes: Bad HomaTransport gbs option value 'xyz' "
            "(expected positive integer); ignoring option | getRoundTripBytes: "
            "roundTripMicros 4, mBitsPerSec 10000, roundTripBytes 5000",
            TestLog::get());

    ServiceLocator locator2("mock:gbs=99foo,rttMicros=4");
    TestLog::reset();
    EXPECT_EQ(5000u, transport.getRoundTripBytes(&locator2));
    EXPECT_EQ("getRoundTripBytes: Bad HomaTransport gbs option value '99foo' "
            "(expected positive integer); ignoring option | getRoundTripBytes: "
            "roundTripMicros 4, mBitsPerSec 10000, roundTripBytes 5000",
            TestLog::get());
}
TEST_F(HomaTransportTest, getRoundTripBytes_noRttOption) {
    transport.maxDataPerPacket = 100;
    ServiceLocator locator("mock:gbs=8");
    EXPECT_EQ(8000u, transport.getRoundTripBytes(&locator));
}
TEST_F(HomaTransportTest, getRoundTripBytes_bogusRttOption) {
    transport.maxDataPerPacket = 100;
    ServiceLocator locator("mock:gbs=8,rttMicros=xyz");
    TestLog::reset();
    EXPECT_EQ(8000u, transport.getRoundTripBytes(&locator));
    EXPECT_EQ("getRoundTripBytes: Bad HomaTransport rttMicros option value "
            "'xyz' (expected positive integer); ignoring option | "
            "getRoundTripBytes: roundTripMicros 8, mBitsPerSec 8000, "
            "roundTripBytes 8000", TestLog::get());

    ServiceLocator locator2("mock:gbs=8,rttMicros=5zzz");
    TestLog::reset();
    EXPECT_EQ(8000u, transport.getRoundTripBytes(&locator2));
    EXPECT_EQ("getRoundTripBytes: Bad HomaTransport rttMicros option value "
            "'5zzz' (expected positive integer); ignoring option | "
            "getRoundTripBytes: roundTripMicros 8, mBitsPerSec 8000, "
            "roundTripBytes 8000", TestLog::get());
}
TEST_F(HomaTransportTest, getRoundTripBytes_roundUpToEvenPackets) {
    transport.maxDataPerPacket = 100;
    ServiceLocator locator("mock:gbs=1,rttMicros=9");
    EXPECT_EQ(1200u, transport.getRoundTripBytes(&locator));
}
TEST_F(HomaTransportTest, getOvercommitmentDegree) {
    // No option supplied, # scheduled priorities too small.
    EXPECT_EQ(4u, transport.getOvercommitmentDegree(NULL, 3));
    // No option supplied, # scheduled priorities large enough.
    EXPECT_EQ(5u, transport.getOvercommitmentDegree(NULL, 5));
    // Bad "degreeOC" value.
    ServiceLocator locator1("mock:degreeOC=0");
    EXPECT_EQ(7u, transport.getOvercommitmentDegree(&locator1, 7));
    // Good "degreeOC" value.
    ServiceLocator locator2("mock:degreeOC=3");
    EXPECT_EQ(3u, transport.getOvercommitmentDegree(&locator2, 7));
}
TEST_F(HomaTransportTest, getUnschedPriorities) {
    int lowest, highest;
    EXPECT_EQ(7, driver->getHighestPacketPriority());
    // No option supplied.
    transport.getUnschedPriorities(NULL, &lowest, &highest);
    EXPECT_EQ(7, lowest);
    EXPECT_EQ(7, highest);
    // Supply only "numPrio".
    ServiceLocator locator1("mock:numPrio=4");
    transport.getUnschedPriorities(&locator1, &lowest, &highest);
    EXPECT_EQ(3, lowest);
    EXPECT_EQ(3, highest);
    // Supply only "unschedPrio".
    ServiceLocator locator2("mock:unschedPrio=4");
    transport.getUnschedPriorities(&locator2, &lowest, &highest);
    EXPECT_EQ(4, lowest);
    EXPECT_EQ(7, highest);
    // Supply both "numPrio" and "unschedPrio".
    ServiceLocator locator3("mock:numPrio=4, unschedPrio=2");
    transport.getUnschedPriorities(&locator3, &lowest, &highest);
    EXPECT_EQ(2, lowest);
    EXPECT_EQ(3, highest);
    // Bad "numPrio" value.
    TestLog::reset();
    ServiceLocator locator4("mock:numPrio=-1");
    transport.getUnschedPriorities(&locator4, &lowest, &highest);
    EXPECT_EQ("getUnschedPriorities: Bad HomaTransport numPrio option value"
            " '-1' (expected (0, 8]); ignoring option", TestLog::get());
    // Bad "unschedPrio" value.
    TestLog::reset();
    ServiceLocator locator5("mock:numPrio=4,unschedPrio=5");
    transport.getUnschedPriorities(&locator5, &lowest, &highest);
    EXPECT_EQ("getUnschedPriorities: Bad HomaTransport unschedPrio option "
            "value '5' (expected (0, 4]); ignoring option", TestLog::get());
}
TEST_F(HomaTransportTest, getUnschedPrioCutoffs) {
    TestLog::reset();
    vector<uint32_t> cutoffs = transport.getUnschedPrioCutoffs(NULL, 1);
    EXPECT_EQ("getUnschedPrioCutoffs: Priority brackets for unscheduled "
             "messages: [0, 4294967295]", TestLog::get());

    TestLog::reset();
    ServiceLocator locator("mock:unschedPrioCutoffs=469.5521.15267");
    cutoffs = transport.getUnschedPrioCutoffs(&locator, 4);
    EXPECT_EQ("getUnschedPrioCutoffs: Priority brackets for unscheduled "
             "messages: [0, 468] [469, 5520] [5521, 15266] [15267, 4294967295]",
             TestLog::get());
}

TEST_F(HomaTransportTest, sendBytes_basics) {
    transport.maxDataPerPacket = 10;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    Buffer buffer;
    buffer.append("abcdefghijklmno1234567890", 25);
    uint32_t count = transport.sendBytes(&address1,
            HomaTransport::RpcId(5, 6), &buffer, 0, 50,
            unscheduledBytes, 1, HomaTransport::FROM_SERVER);
    EXPECT_EQ("DATA FROM_SERVER, rpcId 5.6, totalLength 25, "
            "offset 0 abcdefghij | "
            "DATA FROM_SERVER, rpcId 5.6, totalLength 25, "
            "offset 10 klmno12345 | "
            "DATA FROM_SERVER, rpcId 5.6, totalLength 25, "
            "offset 20 67890",
            driver->outputLog);
    EXPECT_EQ(25u, count);
}

TEST_F(HomaTransportTest, sendBytes_shortMessage) {
    transport.maxDataPerPacket = 100;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    Buffer buffer;
    buffer.append("abcdefghijklmno", 15);
    uint32_t count = transport.sendBytes(&address1,
            HomaTransport::RpcId(5, 6), &buffer, 0, 16,
            unscheduledBytes, 1, HomaTransport::FROM_CLIENT);
    EXPECT_EQ("ALL_DATA FROM_CLIENT, rpcId 5.6 abcdefghij (+5 more)",
            driver->outputLog);
    EXPECT_EQ(15u, count);
}

TEST_F(HomaTransportTest, tryToTransmitData_pickShortestRequest) {
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
    HomaTransport::ClientRpc* clientRpc1 = transport.outgoingRpcs[1lu];
    EXPECT_EQ(0u, clientRpc1->request.transmitOffset);
    HomaTransport::ClientRpc* clientRpc3 = transport.outgoingRpcs[3lu];
    EXPECT_EQ(10u, clientRpc3->request.transmitOffset);
}
TEST_F(HomaTransportTest, tryToTransmitData_pickShortestResponse) {
    transport.maxDataPerPacket = 10;
    driver->transmitQueueSpace = 0;
    HomaTransport::ServerRpc* serverRpc1 = prepareToRespond(200, 20000,
            "aaaaaaaaaa");
    serverRpc1->sendReply();
    HomaTransport::ServerRpc* serverRpc2 = prepareToRespond(201, 5, "bbbbb");
    serverRpc2->sendReply();
    HomaTransport::ServerRpc* serverRpc3 = prepareToRespond(202, 15, "ccccc");
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
TEST_F(HomaTransportTest, tryToTransmitData_requestsAndResponsesToTransmit) {
    transport.maxDataPerPacket = 10;
    driver->transmitQueueSpace = 0;
    MockWrapper wrapper1("012345678901234");
    session->sendRequest(&wrapper1.request, &wrapper1.response, &wrapper1);
    MockWrapper wrapper2("abcd");
    session->sendRequest(&wrapper2.request, &wrapper2.response, &wrapper2);
    MockWrapper wrapper3("0123456789012345");
    HomaTransport::ServerRpc* serverRpc1 = prepareToRespond(200, 15);
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

TEST_F(HomaTransportTest, tryToTransmitData_negativeTransmitQueueSpace) {
    driver->transmitQueueSpace = -999;
    MockWrapper wrapper1("012345678901234");
    session->sendRequest(&wrapper1.request, &wrapper1.response, &wrapper1);
    EXPECT_EQ("", driver->outputLog);
}

TEST_F(HomaTransportTest, Session_constructor) {
    ServiceLocator locator("homa+udp: host=localhost, port=11101");
    UdpDriver* driver2 = new UdpDriver(&context, &locator);
    HomaTransport transport2(&context, &locator, driver2, true, 1);
    string exceptionMessage("no exception");
    try {
        ServiceLocator bogusLocator("bogus:foo=bar");
        TestLog::reset();
        transport2.getSession(&bogusLocator);
    } catch (TransportException& e) {
        exceptionMessage = e.message;
    }
    EXPECT_EQ("HomaTransport couldn't parse service locator",
            exceptionMessage);
    EXPECT_EQ("Session: Service locator 'bogus:foo=bar' couldn't be "
            "converted to IP address: The option with key 'host' was "
            "not found in the ServiceLocator.", TestLog::get());
}

TEST_F(HomaTransportTest, Session_destructor) {
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

TEST_F(HomaTransportTest, Session_cancelRequest) {
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

TEST_F(HomaTransportTest, Session_getRpcInfo) {
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

TEST_F(HomaTransportTest, Session_sendRequest_aborted) {
    MockWrapper wrapper("message1");
    session->abort();
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    EXPECT_EQ("", driver->outputLog);
}
TEST_F(HomaTransportTest, Session_sendRequest_normal) {
    transport.roundTripBytes = 1000;
    transport.maxDataPerPacket = 10;
    driver->transmitQueueSpace = 10;
    MockWrapper wrapper1("message1");
    MockWrapper wrapper2("message2");
    session->sendRequest(&wrapper1.request, &wrapper1.response, &wrapper1);
    session->sendRequest(&wrapper2.request, &wrapper2.response, &wrapper2);
    HomaTransport::ClientRpc* clientRpc1 = transport.outgoingRpcs[1lu];
    EXPECT_EQ(8u, clientRpc1->request.transmitOffset);
    HomaTransport::ClientRpc* clientRpc2 = transport.outgoingRpcs[2lu];
    EXPECT_EQ(8u, clientRpc2->request.transmitLimit);
    EXPECT_EQ(0u, clientRpc2->request.transmitOffset);
    EXPECT_EQ(2u, transport.outgoingRpcs.size());
    EXPECT_EQ(1u, transport.outgoingRequests.size());
    EXPECT_EQ(3u, transport.nextClientSequenceNumber);
}
TEST_F(HomaTransportTest, Session_sendRequest_smallMessage) {
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

TEST_F(HomaTransportTest, handlePacket_noHeader) {
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
TEST_F(HomaTransportTest, handlePacket_timeTraceFromServerUnknownSequence) {
    handlePacket("mock:server=1",
            HomaTransport::LogTimeTraceHeader(HomaTransport::RpcId(666, 1),
            HomaTransport::FROM_SERVER));
    WorkerTimer::sync();
    EXPECT_TRUE(TestUtil::contains(TimeTrace::getTrace(),
            "client received LOG_TIME_TRACE"));
    EXPECT_EQ(0u, Driver::Received::stealCount);
}
TEST_F(HomaTransportTest, handlePacket_packetFromServerWithUnknownSequence) {
    handlePacket("mock:server=1",
            HomaTransport::AllDataHeader(HomaTransport::RpcId(666, 1),
            HomaTransport::FROM_SERVER, 9), "response1");
    EXPECT_EQ("handlePacket: Discarding unknown packet, sequence 1",
            TestLog::get());
    EXPECT_EQ(0u, Driver::Received::stealCount);
}
TEST_F(HomaTransportTest, handlePacket_allDataFromServer_basics) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    handlePacket("mock:server=1",
            HomaTransport::AllDataHeader(HomaTransport::RpcId(666, 1),
            HomaTransport::FROM_SERVER, 9), "response1");
    EXPECT_STREQ("completed: 1, failed: 0", wrapper.getState());
    EXPECT_EQ("response1", TestUtil::toString(&wrapper.response));
    EXPECT_EQ(0lu, transport.outgoingRpcs.size());
    EXPECT_EQ(1u, Driver::Received::stealCount);
}
TEST_F(HomaTransportTest, handlePacket_allDataFromServer_tooShort) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    handlePacket("mock:server=1",
            HomaTransport::AllDataHeader(HomaTransport::RpcId(666, 1),
            HomaTransport::FROM_SERVER, 10), "response1");
    EXPECT_STREQ("completed: 0, failed: 0", wrapper.getState());
    EXPECT_EQ(1u, driver->releaseCount);
    EXPECT_EQ("handlePacket: ALL_DATA response from mock:server=1 too short "
            "(got 29 bytes, expected 30)",
            TestLog::get());
}
TEST_F(HomaTransportTest, handlePacket_dataFromServer_incompleteHeader) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    handlePacket("mock:server=1", HomaTransport::CommonHeader(
            HomaTransport::DATA, HomaTransport::RpcId(666, 1),
            HomaTransport::FROM_SERVER));
    EXPECT_EQ("handlePacket: packet of type DATA from mock:server=1 too "
            "short (18 bytes)", TestLog::get());
}
TEST_F(HomaTransportTest, handlePacket_dataFromServer_basics) {
    MockWrapper wrapper("message1");
    transport.maxDataPerPacket = 5;
    transport.roundTripBytes = 1000;
    uint32_t unscheduledBytes = 0;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    transport.grantIncrement = 500;

    // First packet of response.
    handlePacket("mock:server=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(666, 1), 10, 0,
            unscheduledBytes, HomaTransport::FROM_SERVER), "abcde");
    EXPECT_STREQ("completed: 0, failed: 0", wrapper.getState());
    HomaTransport::ScheduledMessage* response = transport.messagesToGrant[0];
    EXPECT_EQ(HomaTransport::RpcId(666, 1), response->rpcId);
    EXPECT_EQ(10u, response->grantOffset);
    EXPECT_EQ("abcde", TestUtil::toString(&wrapper.response));
    EXPECT_EQ(1u, Driver::Received::stealCount);

    // Second packet of response
    driver->outputLog.clear();
    handlePacket("mock:server=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(666, 1), 10, 5,
            unscheduledBytes, HomaTransport::FROM_SERVER), "12345");
    EXPECT_STREQ("completed: 1, failed: 0", wrapper.getState());
    EXPECT_EQ("abcde12345", TestUtil::toString(&wrapper.response));
    EXPECT_EQ("", driver->outputLog);
    EXPECT_EQ(0lu, transport.outgoingRpcs.size());
    EXPECT_EQ(1u, Driver::Received::stealCount);
}
TEST_F(HomaTransportTest, handlePacket_dataFromServer_extraData) {
    MockWrapper wrapper("message1");
    transport.maxDataPerPacket = 5;
    transport.roundTripBytes = 1000;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    transport.grantIncrement = 500;

    // First packet of response.
    handlePacket("mock:server=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(666, 1), 6, 0,
            unscheduledBytes, HomaTransport::FROM_SERVER), "abcde");

    // Final packet of response has extra data.
    driver->outputLog.clear();
    handlePacket("mock:server=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(666, 1), 6, 5,
            unscheduledBytes, HomaTransport::FROM_SERVER), "12345");
    EXPECT_STREQ("completed: 1, failed: 0", wrapper.getState());
    EXPECT_EQ("abcde1", TestUtil::toString(&wrapper.response));
}
TEST_F(HomaTransportTest, handlePacket_dataFromServer_dontIssueGrant) {
    MockWrapper wrapper("message1");
    transport.maxDataPerPacket = 5;
    transport.roundTripBytes = 1000;
    uint32_t unscheduledBytes = 0;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    transport.grantIncrement = 500;

    // First packet of response; send a grant
    handlePacket("mock:server=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(666, 1), 15, 0,
            unscheduledBytes, HomaTransport::FROM_SERVER), "abcde");
    HomaTransport::ScheduledMessage* response = transport.messagesToGrant[0];
    EXPECT_EQ(666u, response->rpcId.clientId);
    EXPECT_EQ(1u, response->rpcId.sequence);
    EXPECT_EQ(15u, response->grantOffset);

    // Second packet of response (still not complete, but no need for
    // another grant).
    driver->outputLog.clear();
    handlePacket("mock:server=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(666, 1), 15,
            10, unscheduledBytes, HomaTransport::FROM_SERVER),
            "12345");
    EXPECT_EQ("", driver->outputLog);
    EXPECT_EQ(1lu, transport.outgoingRpcs.size());

    // Third packet of response (now complete)
    driver->outputLog.clear();
    handlePacket("mock:server=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(666, 1), 15, 5,
            unscheduledBytes, HomaTransport::FROM_SERVER), "xyzzy");
    EXPECT_STREQ("completed: 1, failed: 0", wrapper.getState());
    EXPECT_EQ(0lu, transport.outgoingRpcs.size());
    EXPECT_EQ("abcdexyzzy12345", TestUtil::toString(&wrapper.response));
}
TEST_F(HomaTransportTest, handlePacket_grantFromServer_incompleteHeader) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    handlePacket("mock:server=1", HomaTransport::CommonHeader(
            HomaTransport::GRANT, HomaTransport::RpcId(666, 1),
            HomaTransport::FROM_SERVER));
    EXPECT_EQ("handlePacket: packet of type GRANT from mock:server=1 too "
            "short (18 bytes)", TestLog::get());
}
TEST_F(HomaTransportTest, handlePacket_grantFromServer) {
    MockWrapper wrapper("abcdefghij0123456789");
    transport.roundTripBytes = 10;
    transport.maxDataPerPacket = 10;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    HomaTransport::ClientRpc* clientRpc = transport.outgoingRpcs[1];
    EXPECT_EQ(10lu, clientRpc->request.transmitLimit);

    // First grant doesn't get past transmitLimit.
    handlePacket("mock:server=1",
            HomaTransport::GrantHeader(HomaTransport::RpcId(666, 1), 10, 1,
            HomaTransport::FROM_SERVER));
    EXPECT_EQ(10lu, clientRpc->request.transmitLimit);
    EXPECT_EQ(7u, clientRpc->request.transmitPriority);

    // Second grant is far enough out to enable more bytes to be sent.
    handlePacket("mock:server=1",
            HomaTransport::GrantHeader(HomaTransport::RpcId(666, 1), 15, 2,
            HomaTransport::FROM_SERVER));
    EXPECT_EQ(15lu, clientRpc->request.transmitLimit);
    EXPECT_EQ(2u, clientRpc->request.transmitPriority);
}
TEST_F(HomaTransportTest, handlePacket_logTimeTraceFromServer) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    TimeTrace::reset();

    handlePacket("mock:server=1", HomaTransport::LogTimeTraceHeader(
            HomaTransport::RpcId(666, 1), HomaTransport::FROM_SERVER));
    WorkerTimer::sync();
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "client received LOG_TIME_TRACE"));
}

TEST_F(HomaTransportTest, handlePacket_resendFromServer_incompleteHeader) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    handlePacket("mock:server=1", HomaTransport::CommonHeader(
            HomaTransport::RESEND, HomaTransport::RpcId(666, 1),
            HomaTransport::FROM_SERVER));
    EXPECT_EQ("handlePacket: packet of type RESEND from mock:server=1 too "
            "short (18 bytes)", TestLog::get());
}
TEST_F(HomaTransportTest, handlePacket_resendFromServer_restart) {
    MockWrapper wrapper("abcdefghij0123456789");
    transport.roundTripBytes = 10;
    transport.maxDataPerPacket = 10;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    transport.poller.poll();
    EXPECT_EQ("DATA FROM_CLIENT, rpcId 666.1, totalLength 20, offset 0 "
            "abcdefghij",
            driver->outputLog);
    driver->outputLog.clear();
    HomaTransport::OutgoingMessage* request =
            &transport.outgoingRpcs[1]->request;
    EXPECT_EQ(10u, request->transmitOffset);

    request->transmitPriority = 99u;
    handlePacket("mock:server=1", HomaTransport::ResendHeader(
            HomaTransport::RpcId(666, 1), 0, 5, 1,
            HomaTransport::FROM_SERVER|HomaTransport::RESTART));
    EXPECT_EQ(0u, request->transmitOffset);
    EXPECT_EQ(5u, request->transmitLimit);
    EXPECT_EQ(7u, request->transmitPriority);
}
TEST_F(HomaTransportTest,
        handlePacket_resendFromServer_transmitLimitChanges) {
    MockWrapper wrapper("abcdefghij0123456789");
    transport.roundTripBytes = 10;
    transport.maxDataPerPacket = 10;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    HomaTransport::OutgoingMessage* request =
            &transport.outgoingRpcs[1]->request;
    EXPECT_EQ(10lu, request->transmitLimit);
    driver->outputLog.clear();

    handlePacket("mock:server=1", HomaTransport::ResendHeader(
            HomaTransport::RpcId(666, 1), 10, 8, 99,
            HomaTransport::FROM_SERVER));
    EXPECT_EQ("BUSY FROM_CLIENT, rpcId 666.1",
            driver->outputLog);
    EXPECT_EQ(18u, request->transmitLimit);
    EXPECT_EQ(10u, request->transmitOffset);
    EXPECT_EQ(7u, request->transmitPriority);
}
TEST_F(HomaTransportTest, handlePacket_resendFromServer_sendBusy) {
    driver->transmitQueueSpace = 0;
    MockWrapper wrapper("abcdefghij0123456789");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();

    // Case 1: data hasn't actually been transmitted yet.
    handlePacket("mock:server=1", HomaTransport::ResendHeader(
            HomaTransport::RpcId(666, 1), 0, 10, 1,
            HomaTransport::FROM_SERVER));
    EXPECT_EQ("BUSY FROM_CLIENT, rpcId 666.1", driver->outputLog);

    // Case 2: data was sent shortly before RESEND arrived.
    Cycles::mockTscValue = 1000000;
    driver->transmitQueueSpace = 10;
    transport.tryToTransmitData();
    driver->outputLog.clear();
    Cycles::mockTscValue += transport.timerInterval - 10;
    handlePacket("mock:server=1", HomaTransport::ResendHeader(
            HomaTransport::RpcId(666, 1), 0, 10, 1,
            HomaTransport::FROM_SERVER));
    EXPECT_EQ("BUSY FROM_CLIENT, rpcId 666.1", driver->outputLog);
    Cycles::mockTscValue = 0;
}
TEST_F(HomaTransportTest, handlePacket_resendFromServer_resend) {
    Cycles::mockTscValue = 1000000;
    transport.timerInterval = 1000;
    MockWrapper wrapper("abcdefghij0123456789");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();

    Cycles::mockTscValue += transport.timerInterval + 10;
    handlePacket("mock:server=1", HomaTransport::ResendHeader(
            HomaTransport::RpcId(666, 1), 12, 20, 1,
            HomaTransport::FROM_SERVER));
    EXPECT_EQ("DATA FROM_CLIENT, rpcId 666.1, totalLength 20, offset 12, "
            "RETRANSMISSION 23456789",
            driver->outputLog);
    EXPECT_EQ(1001010lu, transport.outgoingRpcs[1]->request.lastTransmitTime);
    Cycles::mockTscValue = 0;
}
TEST_F(HomaTransportTest, handlePacket_busyFromServer) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    transport.outgoingRpcs[1]->silentIntervals = 2;
    handlePacket("mock:server=1", HomaTransport::BusyHeader(
            HomaTransport::RpcId(666, 1), HomaTransport::FROM_SERVER));
    EXPECT_EQ(0u, transport.outgoingRpcs[1]->silentIntervals);
}
TEST_F(HomaTransportTest, handlePacket_unknownOpcodeFromServer) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    handlePacket("mock:server=1", HomaTransport::CommonHeader(
            HomaTransport::BOGUS, HomaTransport::RpcId(666, 1),
            HomaTransport::FROM_SERVER));
    EXPECT_EQ("handlePacket: unexpected opcode 27 received from "
            "server mock:server=1", TestLog::get());
}
TEST_F(HomaTransportTest, handlePacket_allDataFromClient) {
    handlePacket("mock:client=1",
            HomaTransport::AllDataHeader(HomaTransport::RpcId(100, 101),
            HomaTransport::FROM_CLIENT, 8), "message1");
    HomaTransport::ServerRpc* serverRpc =
            static_cast<HomaTransport::ServerRpc*>(
            context.workerManager->waitForRpc(0));
    ASSERT_TRUE(serverRpc != NULL);
    EXPECT_EQ("message1", TestUtil::toString(&serverRpc->requestPayload));
    EXPECT_TRUE(serverRpc->requestComplete);
    EXPECT_EQ(2u, transport.nextServerSequenceNumber);
}
TEST_F(HomaTransportTest, handlePacket_allDataFromClient_duplicate) {
    handlePacket("mock:client=1",
            HomaTransport::AllDataHeader(HomaTransport::RpcId(100, 101),
            HomaTransport::FROM_CLIENT, 8), "message1");
    HomaTransport::ServerRpc* serverRpc =
            static_cast<HomaTransport::ServerRpc*>(
            context.workerManager->waitForRpc(0));
    ASSERT_TRUE(serverRpc != NULL);
    handlePacket("mock:client=1",
            HomaTransport::AllDataHeader(HomaTransport::RpcId(100, 101),
            HomaTransport::FROM_CLIENT, 20), "0123457890abcdefghij");
    EXPECT_EQ("message1", TestUtil::toString(&serverRpc->requestPayload));
}
TEST_F(HomaTransportTest, handlePacket_allDataFromClient_tooShort) {
    handlePacket("mock:client=1",
            HomaTransport::AllDataHeader(HomaTransport::RpcId(100, 101),
            HomaTransport::FROM_CLIENT, 9), "message1");
    EXPECT_EQ(1u, driver->releaseCount);
    EXPECT_EQ("handlePacket: ALL_DATA request from mock:client=1 too short "
            "(got 28 bytes, expected 29)",
            TestLog::get());
}
TEST_F(HomaTransportTest, handlePacket_dataFromClient_basics) {
    // Send a message in three packets. The first packet should result
    // in a GRANT.
    transport.roundTripBytes = 1000;
    transport.maxDataPerPacket = 5;
    transport.grantIncrement = 500;
    uint32_t unscheduledBytes = 10;
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 15,
            5, unscheduledBytes, HomaTransport::FROM_CLIENT),
            "56789");
    HomaTransport::ServerRpcMap::iterator it = transport.incomingRpcs.find(
            HomaTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    HomaTransport::ServerRpc* serverRpc = it->second;
    EXPECT_FALSE(serverRpc->requestComplete);
    HomaTransport::ScheduledMessage* response = transport.messagesToGrant[0];
    EXPECT_EQ(HomaTransport::RpcId(100, 101), response->rpcId);
    EXPECT_EQ(15u, response->grantOffset);
    EXPECT_EQ(2u, transport.nextServerSequenceNumber);

    // Second packet: no GRANT should result.
    driver->outputLog.clear();
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 15,
            0, unscheduledBytes, HomaTransport::FROM_CLIENT),
            "01234");
    EXPECT_FALSE(serverRpc->requestComplete);
    EXPECT_EQ(15u, serverRpc->scheduledMessage->grantOffset);
    EXPECT_EQ("", driver->outputLog);
    EXPECT_EQ(1lu, transport.incomingRpcs.size());
    EXPECT_EQ(1lu, transport.serverTimerList.size());

    // Third packet: no GRANT, message should now be complete.
    driver->outputLog.clear();
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 15, 10,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    EXPECT_TRUE(serverRpc->requestComplete);
    EXPECT_EQ("", driver->outputLog);
    EXPECT_EQ("0123456789abcde",
            TestUtil::toString(&serverRpc->requestPayload));
}
TEST_F(HomaTransportTest, handlePacket_dataFromClient_extraBytes) {
    // Send a message in two packets; the second packet contains more
    // than enough data to complete the message.
    transport.roundTripBytes = 1000;
    transport.maxDataPerPacket = 5;
    transport.grantIncrement = 500;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 6,
            0, unscheduledBytes, HomaTransport::FROM_CLIENT),
            "abcde");
    HomaTransport::ServerRpcMap::iterator it = transport.incomingRpcs.find(
            HomaTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    HomaTransport::ServerRpc* serverRpc = it->second;
    EXPECT_FALSE(serverRpc->requestComplete);

    // Second packet.
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 6,
            5, unscheduledBytes, HomaTransport::FROM_CLIENT),
            "01234");
    EXPECT_TRUE(serverRpc->requestComplete);
    EXPECT_EQ("abcde0",
            TestUtil::toString(&serverRpc->requestPayload));
}
TEST_F(HomaTransportTest, handlePacket_dataFromClient_dontIssueGrant) {
    transport.maxDataPerPacket = 5;
    transport.roundTripBytes = 1000;
    transport.grantIncrement = 500;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 15, 10,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    EXPECT_EQ("", driver->outputLog);
}
TEST_F(HomaTransportTest, handlePacket_dataFromClient_extraneousPacket) {
    // Send an extra packet after the message is complete.
    handlePacket("mock:client=1",
            HomaTransport::AllDataHeader(HomaTransport::RpcId(100, 101),
            HomaTransport::FROM_CLIENT, 8), "message1");
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 13,
            8, 1000, HomaTransport::FROM_CLIENT),
            "abcde");
    EXPECT_EQ("handlePacket: ignoring extraneous packet | "
              "handlePacket: Redundant DATA from client, clientId 100, "
              "sequence 101, offset 8, totalLength 13", TestLog::get());
    HomaTransport::ServerRpc* serverRpc =
            dynamic_cast<HomaTransport::ServerRpc*>(
            context.workerManager->waitForRpc(0));
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("message1", TestUtil::toString(&serverRpc->requestPayload));
}
TEST_F(HomaTransportTest, handlePacket_grantFromClient_bogusGrants) {
    prepareToRespond();
    transport.roundTripBytes = 5;
    transport.maxDataPerPacket = 5;

    // GRANT arriving for unknown RpcID: bogus.
    handlePacket("mock:client=1",
            HomaTransport::GrantHeader(HomaTransport::RpcId(5, 6), 10, 1,
            HomaTransport::FROM_CLIENT));
    EXPECT_EQ("handlePacket: unexpected GRANT from client mock:client=1, "
            "id (5,6), grantOffset 10, serverRpc not found",
            TestLog::get());

    // GRANT arriving before result transmission starts: bogus.
    TestLog::reset();
    handlePacket("mock:client=1",
            HomaTransport::GrantHeader(HomaTransport::RpcId(100, 101), 10, 1,
            HomaTransport::FROM_CLIENT));
    EXPECT_EQ("handlePacket: unexpected GRANT from client mock:client=1, "
            "id (100,101), grantOffset 10, serverRpc not sending response",
            TestLog::get());

    // GRANT packet too short: bogus.
    TestLog::reset();
    handlePacket("mock:client=1", HomaTransport::CommonHeader(
            HomaTransport::GRANT, HomaTransport::RpcId(100, 101),
            HomaTransport::FROM_CLIENT));
    EXPECT_EQ("handlePacket: packet of type GRANT from mock:client=1 "
            "too short (18 bytes)", TestLog::get());
}
TEST_F(HomaTransportTest, handlePacket_grantFromClient) {
    transport.roundTripBytes = 5;
    transport.maxDataPerPacket = 5;
    HomaTransport::ServerRpc* serverRpc = prepareToRespond();
    serverRpc->sendReply();
    EXPECT_EQ("DATA FROM_SERVER, rpcId 100.101, totalLength 20, offset 0 "
            "01234",
            driver->outputLog);
    driver->outputLog.clear();

    // First, send redundant grant (do nothing).
    handlePacket("mock:client=1",
            HomaTransport::GrantHeader(HomaTransport::RpcId(100, 101), 5, 99,
            HomaTransport::FROM_CLIENT));
    transport.tryToTransmitData();
    EXPECT_EQ("", driver->outputLog);
    EXPECT_EQ(5u, serverRpc->response.transmitLimit);
    EXPECT_EQ(7u, serverRpc->response.transmitPriority);

    // Second grant should allow more data to be transmitted.
    handlePacket("mock:client=1",
            HomaTransport::GrantHeader(HomaTransport::RpcId(100, 101), 15, 1,
            HomaTransport::FROM_CLIENT));
    transport.tryToTransmitData();
    EXPECT_EQ("DATA FROM_SERVER, rpcId 100.101, totalLength 20, offset 5 "
            "56789 | "
            "DATA FROM_SERVER, rpcId 100.101, totalLength 20, offset 10 "
            "abcde",
            driver->outputLog);
    EXPECT_EQ(15u, serverRpc->response.transmitLimit);
    EXPECT_EQ(1u, serverRpc->response.transmitPriority);

    // Third grant should complete the result transmission.
    driver->outputLog.clear();
    handlePacket("mock:client=1",
            HomaTransport::GrantHeader(HomaTransport::RpcId(100, 101), 25, 2,
            HomaTransport::FROM_CLIENT));
    EXPECT_EQ(20u, serverRpc->response.transmitLimit);
    EXPECT_EQ(2u, serverRpc->response.transmitPriority);
    transport.tryToTransmitData();
    EXPECT_EQ("DATA FROM_SERVER, rpcId 100.101, totalLength 20, offset 15 "
            "fghij",
            driver->outputLog);
    EXPECT_EQ(0lu, transport.incomingRpcs.size());
    EXPECT_EQ(0lu, transport.serverTimerList.size());
    EXPECT_EQ(0lu, transport.serverRpcPool.outstandingAllocations);
}
TEST_F(HomaTransportTest, handlePacket_resendFromClient_packetTooShort) {
    prepareToRespond();
    transport.roundTripBytes = 5;
    transport.maxDataPerPacket = 5;
    TestLog::reset();
    handlePacket("mock:client=1", HomaTransport::CommonHeader(
            HomaTransport::RESEND, HomaTransport::RpcId(100, 101),
            HomaTransport::FROM_CLIENT));
    EXPECT_TRUE(TestUtil::contains(TestLog::get(),
            "handlePacket: packet of type RESEND from mock:client=1 "
            "too short (18 bytes)"));
}
TEST_F(HomaTransportTest, handlePacket_resendFromClient_unknownRpcId) {
    handlePacket("mock:client=1",
            HomaTransport::ResendHeader(HomaTransport::RpcId(10, 11),
            10, 5, 1, HomaTransport::FROM_CLIENT));
    EXPECT_EQ("RESEND FROM_SERVER, rpcId 10.11, offset 0, length 10960, "
            "priority 0, RESTART", driver->outputLog);
}
TEST_F(HomaTransportTest,
        handlePacket_resendFromClient_transmitLimitChanges) {
    transport.roundTripBytes = 15;
    transport.maxDataPerPacket = 15;
    HomaTransport::ServerRpc* serverRpc = prepareToRespond();
    serverRpc->sendReply();

    driver->outputLog.clear();
    handlePacket("mock:client=1",
            HomaTransport::ResendHeader(HomaTransport::RpcId(100, 101),
            15, 8, 1, HomaTransport::FROM_CLIENT));
    EXPECT_EQ("BUSY FROM_SERVER, rpcId 100.101", driver->outputLog);
    EXPECT_EQ(15u, serverRpc->response.transmitOffset);
    EXPECT_EQ(20u, serverRpc->response.transmitLimit);
}
TEST_F(HomaTransportTest, handlePacket_resendFromClient_sendBusy) {
    driver->transmitQueueSpace = 0;
    transport.roundTripBytes = 15;
    transport.maxDataPerPacket = 15;
    HomaTransport::ServerRpc* serverRpc = prepareToRespond();

    // Case 1: sendReply hasn't been called yet
    handlePacket("mock:client=1",
            HomaTransport::ResendHeader(HomaTransport::RpcId(100, 101),
            10, 5, 1, HomaTransport::FROM_CLIENT));
    EXPECT_EQ("BUSY FROM_SERVER, rpcId 100.101",
            driver->outputLog);

    // Case 2: the response is ready, but no bytes have been transmitted yet.
    driver->outputLog.clear();
    serverRpc->sendReply();
    handlePacket("mock:client=1",
            HomaTransport::ResendHeader(HomaTransport::RpcId(100, 101),
            5, 8, 1, HomaTransport::FROM_CLIENT));
    EXPECT_EQ("BUSY FROM_SERVER, rpcId 100.101",
            driver->outputLog);

    // Case 3: we sent the bytes, but only recently.
    Cycles::mockTscValue = 1000000;
    driver->transmitQueueSpace = 100;
    transport.tryToTransmitData();
    driver->outputLog.clear();
    Cycles::mockTscValue += transport.timerInterval - 10;
    handlePacket("mock:client=1",
            HomaTransport::ResendHeader(HomaTransport::RpcId(100, 101),
            5, 8, 1, HomaTransport::FROM_CLIENT));
    EXPECT_EQ("BUSY FROM_SERVER, rpcId 100.101",
            driver->outputLog);
    Cycles::mockTscValue = 0;
}
TEST_F(HomaTransportTest, handlePacket_resendFromClient_sendBytes) {
    Cycles::mockTscValue = 1000000;
    transport.timerInterval = 1000;
    transport.roundTripBytes = 15;
    transport.maxDataPerPacket = 15;
    HomaTransport::ServerRpc* serverRpc = prepareToRespond();
    serverRpc->sendReply();

    driver->outputLog.clear();
    Cycles::mockTscValue += transport.timerInterval + 10;
    handlePacket("mock:client=1",
            HomaTransport::ResendHeader(HomaTransport::RpcId(100, 101),
            0, 15, 1, HomaTransport::FROM_CLIENT));
    EXPECT_EQ("DATA FROM_SERVER, rpcId 100.101, totalLength 20, offset 0, "
            "RETRANSMISSION 0123456789 (+5 more)",
            driver->outputLog);
    EXPECT_EQ(1001010lu, serverRpc->response.lastTransmitTime);
    Cycles::mockTscValue = 0;
}
TEST_F(HomaTransportTest, handlePacket_busyFromClient) {
    HomaTransport::ServerRpc* serverRpc = prepareToRespond();
    serverRpc->silentIntervals = 2;
    handlePacket("mock:client=1", HomaTransport::BusyHeader(
            HomaTransport::RpcId(100, 101), HomaTransport::FROM_CLIENT));
    EXPECT_EQ(0u, serverRpc->silentIntervals);
}
TEST_F(HomaTransportTest, handlePacket_abortFromClient_sendingResponse) {
    // Cancel a ServerRpc that is sending response.
    driver->transmitQueueSpace = 0;
    HomaTransport::ServerRpc* serverRpc = prepareToRespond();
    serverRpc->sendReply();
    EXPECT_EQ(1u, transport.incomingRpcs.size());
    handlePacket("mock:client=1", HomaTransport::AbortHeader(
            HomaTransport::RpcId(100, 101)));
    EXPECT_EQ(0u, transport.incomingRpcs.size());
}
TEST_F(HomaTransportTest, handlePacket_abortFromClient_localExecution) {
    // Cancel a ServerRpc that is being executed locally.
    HomaTransport::ServerRpc* serverRpc = prepareToRespond();
    EXPECT_EQ(1u, transport.incomingRpcs.size());
    handlePacket("mock:client=1", HomaTransport::AbortHeader(
            HomaTransport::RpcId(100, 101)));
    EXPECT_TRUE(serverRpc->cancelled);
    serverRpc->sendReply();
    EXPECT_EQ("", driver->outputLog);
    EXPECT_EQ(0u, transport.incomingRpcs.size());
}
TEST_F(HomaTransportTest, handlePacket_unknownOpcodeFromClient) {
    handlePacket("mock:client=1", HomaTransport::CommonHeader(
            HomaTransport::BOGUS, HomaTransport::RpcId(100, 101),
            HomaTransport::FROM_CLIENT));
    EXPECT_EQ("handlePacket: unexpected opcode 27 received from client "
            "mock:client=1", TestLog::get());
}

TEST_F(HomaTransportTest, sendReply_basics) {
    transport.roundTripBytes = 10;
    transport.maxDataPerPacket = 10;
    HomaTransport::ServerRpc* serverRpc = prepareToRespond();
    serverRpc->sendReply();
    EXPECT_EQ(10lu, serverRpc->response.transmitLimit);
    EXPECT_EQ(1lu, transport.outgoingResponses.size());
    EXPECT_EQ(1lu, transport.serverTimerList.size());
}
TEST_F(HomaTransportTest, sendReply_smallMessage) {
    // Small messages are passed to the NIC directly, bypassing the
    // sender's congestion control mechanism in tryToTransmitData.
    driver->transmitQueueSpace = -1;
    transport.smallMessageThreshold = 10;
    HomaTransport::ServerRpc* serverRpc1 = prepareToRespond();
    HomaTransport::ServerRpc* serverRpc2 =
            prepareToRespond(102, 9, "012345678");
    serverRpc1->sendReply();
    serverRpc2->sendReply();
    EXPECT_EQ(1u, transport.outgoingResponses.size());
    EXPECT_EQ("ALL_DATA FROM_SERVER, rpcId 100.102 012345678",
            driver->outputLog);
}

TEST_F(HomaTransportTest, MessageAccumulator_destructor) {
    transport.maxDataPerPacket = 5;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 25, 10,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 25, 20,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "56789");
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 25, 15,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "01234");
    HomaTransport::ServerRpc* serverRpc =
            transport.incomingRpcs[HomaTransport::RpcId(100, 101)];
    EXPECT_TRUE(serverRpc->accumulator);
    EXPECT_EQ(3u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ(0u, driver->releaseCount);
    transport.deleteServerRpc(serverRpc);
    EXPECT_EQ(3u, driver->releaseCount);
}

TEST_F(HomaTransportTest, addPacket_basics) {
    // Receive a request in 5 packets, in the order P4, P2, P0, P1, P3.
    transport.maxDataPerPacket = 5;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 25, 20,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "P4444");
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 25, 10,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "P2222");
    HomaTransport::ServerRpcMap::iterator it =
            transport.incomingRpcs.find(HomaTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    HomaTransport::ServerRpc* serverRpc = it->second;
    ASSERT_TRUE(serverRpc->accumulator);
    EXPECT_EQ(2u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ(2u, Driver::Received::stealCount);

    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 25, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "P0000");
    EXPECT_EQ(2u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ("P0000",
            TestUtil::toString(&serverRpc->requestPayload));

    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 25, 5,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "P1111");
    EXPECT_EQ(1u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ("P0000P1111P2222",
            TestUtil::toString(&serverRpc->requestPayload));

    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 25, 15,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "P3333");
    EXPECT_EQ(0u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ("P0000P1111P2222P3333P4444",
            TestUtil::toString(&serverRpc->requestPayload));
    EXPECT_EQ(3u, Driver::Received::stealCount);
}
TEST_F(HomaTransportTest, addPacket_skipRedundantPacket) {
    // Receive two duplicate packets that contain bytes 10-14.
    transport.maxDataPerPacket = 5;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 15, 10,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    HomaTransport::ServerRpcMap::iterator it =
            transport.incomingRpcs.find(HomaTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    HomaTransport::ServerRpc* serverRpc = it->second;
    EXPECT_EQ(1u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ(1u, Driver::Received::stealCount);
    EXPECT_EQ(0u, driver->releaseCount);
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 15, 10,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    EXPECT_EQ(1u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ(1u, Driver::Received::stealCount);
    EXPECT_EQ(1u, driver->releaseCount);

    // Receive another two duplicate packets that contain bytes 0-4.
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 15, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "01234");
    EXPECT_EQ(1u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ(2u, Driver::Received::stealCount);
    EXPECT_EQ(1u, driver->releaseCount);
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 15, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "01234");
    EXPECT_EQ(1u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ(2u, Driver::Received::stealCount);
    EXPECT_EQ(2u, driver->releaseCount);
}

TEST_F(HomaTransportTest, requestRetransmission) {
    transport.roundTripBytes = 100;
    transport.maxDataPerPacket = 5;
    uint32_t unscheduledBytes = transport.roundTripBytes;

    // First retransmit: no fragments waiting, no grants sent.
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 20, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "01234");
    HomaTransport::ServerRpcMap::iterator it =
            transport.incomingRpcs.find(HomaTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    HomaTransport::ServerRpc* serverRpc = it->second;
    ASSERT_TRUE(serverRpc->accumulator);
    EXPECT_EQ(0u, serverRpc->accumulator->fragments.size());

    driver->outputLog.clear();
    uint32_t limit = serverRpc->accumulator->requestRetransmission(
            &transport, &address1, HomaTransport::RpcId(100, 101), 0, 1,
            HomaTransport::FROM_SERVER);
    EXPECT_EQ(100u, limit);
    EXPECT_EQ("RESEND FROM_SERVER, rpcId 100.101, offset 5, length 95, "
            "priority 1", driver->outputLog);

    // Second retransmit: no fragment, but grant was sent.
    driver->outputLog.clear();
    limit = serverRpc->accumulator->requestRetransmission(
            &transport, &address1, HomaTransport::RpcId(100, 101), 25, 2,
            HomaTransport::FROM_SERVER);
    EXPECT_EQ(25u, limit);
    EXPECT_EQ("RESEND FROM_SERVER, rpcId 100.101, offset 5, length 20, "
            "priority 2", driver->outputLog);

    // Third retransmit: fragment waiting.
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 20, 15,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    driver->outputLog.clear();
    limit = serverRpc->accumulator->requestRetransmission(
            &transport, &address1, HomaTransport::RpcId(100, 101), 25, 3,
            HomaTransport::FROM_SERVER);
    EXPECT_EQ(15u, limit);
    EXPECT_EQ("RESEND FROM_SERVER, rpcId 100.101, offset 5, length 10, "
            "priority 3", driver->outputLog);
}

TEST_F(HomaTransportTest, poll_nothingToDo) {
    transport.nextTimeoutCheck = ~0;
    int result = transport.poller.poll();
    EXPECT_EQ(0, result);
}
TEST_F(HomaTransportTest, poll_incomingPackets) {
    transport.maxDataPerPacket = 5;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    // Send 3 fragments, process all at once.
    driver->packetArrived("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 15, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "01234");
    driver->packetArrived("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 15, 5,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "56789");
    driver->packetArrived("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 15, 10,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "ABCDE");
    int result = transport.poller.poll();
    HomaTransport::ServerRpcMap::iterator it =
            transport.incomingRpcs.find(HomaTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    HomaTransport::ServerRpc* serverRpc = it->second;
    EXPECT_EQ("0123456789ABCDE",
            TestUtil::toString(&serverRpc->requestPayload));
    EXPECT_EQ(1u, Driver::Received::stealCount);
    EXPECT_EQ(1, result);
}
TEST_F(HomaTransportTest, poll_callCheckTimeouts) {
    Cycles::mockTscValue = 100000;
    transport.poller.owner->currentTime = Cycles::rdtsc();
    transport.timerInterval = 1000;
    transport.nextTimeoutCheck = 102000;
    MockWrapper wrapper1("012345678901234");
    session->sendRequest(&wrapper1.request, &wrapper1.response, &wrapper1);
    HomaTransport::ClientRpc* clientRpc = transport.outgoingRpcs[1];

    // First call: timerInterval not reached.
    int result = transport.poller.poll();
    EXPECT_EQ(0u, clientRpc->silentIntervals);
    EXPECT_EQ(0, result);

    // Second call: timerInterval reached, but lots of input packets
    Cycles::mockTscValue = 103000;
    transport.poller.owner->currentTime = Cycles::rdtsc();
    HomaTransport::BusyHeader busy(HomaTransport::RpcId(1000, 1001),
            HomaTransport::FROM_CLIENT);
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
TEST_F(HomaTransportTest, poll_outgoingGrant) {
    transport.maxDataPerPacket = 5;
    transport.roundTripBytes = 20;
    transport.grantIncrement = 5;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    // Send 3 fragments, process all at once.
    driver->packetArrived("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 100, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "01234");
    driver->packetArrived("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 100, 5,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "56789");
    driver->packetArrived("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 100, 10,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    driver->packetArrived("mock:client=2",
            HomaTransport::DataHeader(HomaTransport::RpcId(101, 102), 50, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "ABCDE");
    int result = transport.poller.poll();
    EXPECT_EQ("GRANT FROM_SERVER, rpcId 101.102, offset 25, priority 1 | "
            "GRANT FROM_SERVER, rpcId 100.101, offset 35, priority 0",
            driver->outputLog);
    EXPECT_EQ(1, result);
    EXPECT_EQ(0u, transport.messagesToGrant.size());
}
TEST_F(HomaTransportTest, poll_outgoingPacket) {
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
TEST_F(HomaTransportTest, checkTimeouts_clientTransmissionNotStartedYet) {
    driver->transmitQueueSpace = 0;
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    HomaTransport::ClientRpc* clientRpc = transport.outgoingRpcs[1lu];
    transport.checkTimeouts();
    EXPECT_EQ(0u, clientRpc->silentIntervals);
    transport.checkTimeouts();
    transport.checkTimeouts();
    EXPECT_EQ(0u, clientRpc->silentIntervals);
}
TEST_F(HomaTransportTest, checkTimeouts_clientPingAndAbort) {
    transport.roundTripBytes = 100;
    MockWrapper wrapper(NULL);
    WireFormat::RequestCommon* header =
            wrapper.request.emplaceAppend<WireFormat::RequestCommon>();
    header->opcode = WireFormat::READ;
    header->service = WireFormat::MASTER_SERVICE;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    HomaTransport::ClientRpc* clientRpc = transport.outgoingRpcs[1lu];
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
    EXPECT_EQ("RESEND FROM_CLIENT, rpcId 666.1, offset 0, length 100, "
            "priority 7", driver->outputLog);
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
TEST_F(HomaTransportTest, checkTimeouts_sendResendFromClient) {
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
            HomaTransport::DataHeader(HomaTransport::RpcId(666, 1), 200,
            0, unscheduledBytes, HomaTransport::FROM_SERVER),
            "abcde");
    driver->outputLog.clear();

    transport.checkTimeouts();
    transport.checkTimeouts();
    EXPECT_EQ("", TestLog::get());

    transport.checkTimeouts();
    EXPECT_EQ("RESEND FROM_CLIENT, rpcId 666.1, offset 5, length 145, "
            "priority 7", driver->outputLog);

    // If a packet arrives, RESENDS stop (for a while).
    driver->outputLog.clear();
    handlePacket("mock:server=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(666, 1), 200, 5,
            unscheduledBytes, HomaTransport::FROM_SERVER), "fghij");
    transport.checkTimeouts();
    transport.checkTimeouts();
    EXPECT_EQ("", driver->outputLog);

    transport.checkTimeouts();
    EXPECT_EQ("RESEND FROM_CLIENT, rpcId 666.1, offset 10, length 140, "
            "priority 7", driver->outputLog);
}
TEST_F(HomaTransportTest, checkTimeouts_serverResponseTransmissionDelayed) {
    driver->transmitQueueSpace = 0;
    transport.roundTripBytes = 15;
    transport.maxDataPerPacket = 15;
    HomaTransport::ServerRpc* serverRpc = prepareToRespond();
    serverRpc->sendReply();

    transport.checkTimeouts();
    EXPECT_EQ(0u, serverRpc->silentIntervals);
    transport.checkTimeouts();
    transport.checkTimeouts();
    EXPECT_EQ(0u, serverRpc->silentIntervals);
}
TEST_F(HomaTransportTest, checkTimeouts_serverAbortsRequest) {
    transport.maxDataPerPacket = 5;
    transport.timeoutIntervals = 2;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 15, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");

    transport.checkTimeouts();
    EXPECT_EQ("", TestLog::get());

    transport.checkTimeouts();
    EXPECT_EQ("checkTimeouts: aborting unknown(25185) RPC from client "
            "mock:client=1, sequence 101: timeout | "
            "deleteServerRpc: RpcId (100, 101)", TestLog::get());
}
TEST_F(HomaTransportTest, checkTimeouts_sendResendFromServer) {
    transport.roundTripBytes = 100;
    transport.maxDataPerPacket = 5;
    transport.grantIncrement = 50;
    uint32_t unscheduledBytes = transport.roundTripBytes;
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 15, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    HomaTransport::ServerRpcMap::iterator it =
            transport.incomingRpcs.find(HomaTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    driver->outputLog.clear();

    transport.checkTimeouts();
    EXPECT_EQ("", TestLog::get());

    transport.checkTimeouts();
    EXPECT_EQ("RESEND FROM_SERVER, rpcId 100.101, offset 5, length 95, "
            "priority 7", driver->outputLog);

    // Packet arrival stops resends (for a while).
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 15, 5,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "fghij");
    driver->outputLog.clear();
    transport.checkTimeouts();
    EXPECT_EQ("", driver->outputLog);

    transport.checkTimeouts();
    EXPECT_EQ("RESEND FROM_SERVER, rpcId 100.101, offset 10, length 90, "
            "priority 7", driver->outputLog);
}

TEST_F(HomaTransportTest, adjustSchedulingPrecedence) {
    transport.roundTripBytes = 100;
    transport.maxDataPerPacket = 5;
    transport.grantIncrement = 5;
    transport.maxGrantedMessages = 2;
    uint32_t unscheduledBytes = transport.roundTripBytes;

    // m1 < m2 < m3
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 150, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    HomaTransport::ScheduledMessage* m1 = getServerRpc(
            HomaTransport::RpcId(100, 101))->scheduledMessage.get();

    // m2 also from client 1 but shorter.
    handlePacket("mock:client=2",
            HomaTransport::DataHeader(HomaTransport::RpcId(101, 102), 151, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    HomaTransport::ScheduledMessage* m2 = getServerRpc(
            HomaTransport::RpcId(101, 102))->scheduledMessage.get();

    // m3 still from client 1 but larger than m2.
    handlePacket("mock:client=3",
            HomaTransport::DataHeader(HomaTransport::RpcId(102, 103), 152, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    HomaTransport::ScheduledMessage* m3 = getServerRpc(
            HomaTransport::RpcId(102, 103))->scheduledMessage.get();

    // Receive one more packet from m3. Now m3 < m1 < m2.
    handlePacket("mock:client=3",
            HomaTransport::DataHeader(HomaTransport::RpcId(102, 103), 152, 5,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "fghij");
    EXPECT_EQ(2u, transport.activeMessages.size());
    EXPECT_EQ(&transport.activeMessages.front(), m3);
    EXPECT_EQ(&transport.activeMessages.back(), m1);

    // Receive one more packet from m2. Now m2 < m3 < m1.
    handlePacket("mock:client=2",
            HomaTransport::DataHeader(HomaTransport::RpcId(101, 102), 151, 5,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "fghij");
    EXPECT_EQ(2u, transport.activeMessages.size());
    EXPECT_EQ(&transport.activeMessages.front(), m2);
    EXPECT_EQ(&transport.activeMessages.back(), m3);
}
TEST_F(HomaTransportTest, replaceActiveMessage) {
    transport.roundTripBytes = 100;
    transport.maxDataPerPacket = 5;
    transport.grantIncrement = 5;
    transport.maxGrantedMessages = 2;
    uint32_t unscheduledBytes = transport.roundTripBytes;

    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 150, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    HomaTransport::ScheduledMessage* m1 = getServerRpc(
            HomaTransport::RpcId(100, 101))->scheduledMessage.get();

    // m2 > m1.
    handlePacket("mock:client=2",
            HomaTransport::DataHeader(HomaTransport::RpcId(101, 102), 200, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    HomaTransport::ScheduledMessage* m2 = getServerRpc(
            HomaTransport::RpcId(101, 102))->scheduledMessage.get();

    // m3 > m2.
    handlePacket("mock:client=3",
            HomaTransport::DataHeader(HomaTransport::RpcId(102, 103), 250, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    HomaTransport::ScheduledMessage* m3 = getServerRpc(
            HomaTransport::RpcId(102, 103))->scheduledMessage.get();

    // m4 > m3.
    handlePacket("mock:client=4",
            HomaTransport::DataHeader(HomaTransport::RpcId(103, 104), 300, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    HomaTransport::ScheduledMessage* m4 = getServerRpc(
            HomaTransport::RpcId(103, 104))->scheduledMessage.get();

    EXPECT_EQ(2u, transport.activeMessages.size());
    EXPECT_EQ(HomaTransport::ScheduledMessage::ACTIVE, m1->state);
    EXPECT_EQ(HomaTransport::ScheduledMessage::ACTIVE, m2->state);

    // Replace m1 with m3.
    m1->grantOffset = ~0u;
    transport.replaceActiveMessage(m1, m3);
    EXPECT_EQ(2u, transport.activeMessages.size());
    EXPECT_EQ(HomaTransport::ScheduledMessage::FULLY_GRANTED, m1->state);
    EXPECT_EQ(HomaTransport::ScheduledMessage::ACTIVE, m2->state);
    EXPECT_EQ(HomaTransport::ScheduledMessage::ACTIVE, m3->state);

    // Replace m2 without explicitly specifying its replacement.
    m2->grantOffset = ~0u;
    transport.replaceActiveMessage(m2, NULL);
    EXPECT_EQ(2u, transport.activeMessages.size());
    EXPECT_EQ(HomaTransport::ScheduledMessage::FULLY_GRANTED, m2->state);
    EXPECT_EQ(HomaTransport::ScheduledMessage::ACTIVE, m3->state);
    EXPECT_EQ(HomaTransport::ScheduledMessage::ACTIVE, m4->state);

    EXPECT_EQ(&transport.activeMessages.front(), m3);
    EXPECT_EQ(&transport.activeMessages.back(), m4);
}
TEST_F(HomaTransportTest, tryToSchedule) {
    transport.roundTripBytes = 100;
    transport.maxDataPerPacket = 5;
    transport.grantIncrement = 5;
    transport.maxGrantedMessages = 2;
    uint32_t unscheduledBytes = transport.roundTripBytes;

    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 200, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    HomaTransport::ScheduledMessage* m1 = getServerRpc(
            HomaTransport::RpcId(100, 101))->scheduledMessage.get();

    // m2 also from client 1 but shorter.
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 102), 150, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    HomaTransport::ScheduledMessage* m2 = getServerRpc(
            HomaTransport::RpcId(100, 102))->scheduledMessage.get();
    EXPECT_EQ(HomaTransport::ScheduledMessage::INACTIVE, m1->state);
    EXPECT_EQ(HomaTransport::ScheduledMessage::ACTIVE, m2->state);

    // m3 still from client 1 but larger than m2.
    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 103), 200, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    HomaTransport::ScheduledMessage* m3 = getServerRpc(
            HomaTransport::RpcId(100, 103))->scheduledMessage.get();
    EXPECT_EQ(1u, transport.activeMessages.size());
    EXPECT_EQ(HomaTransport::ScheduledMessage::ACTIVE, m2->state);
    EXPECT_EQ(HomaTransport::ScheduledMessage::INACTIVE, m3->state);

    // m4 is from client 2 and larger than m2. We can grant one more message
    // concurrently.
    handlePacket("mock:client=2",
            HomaTransport::DataHeader(HomaTransport::RpcId(101, 104), 200, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    HomaTransport::ScheduledMessage* m4 = getServerRpc(
            HomaTransport::RpcId(101, 104))->scheduledMessage.get();
    EXPECT_EQ(2u, transport.activeMessages.size());
    EXPECT_EQ(HomaTransport::ScheduledMessage::ACTIVE, m2->state);
    EXPECT_EQ(HomaTransport::ScheduledMessage::ACTIVE, m4->state);

    // m5 is from client 3 and m2 < m5 < m4. It should replace m4.
    handlePacket("mock:client=3",
            HomaTransport::DataHeader(HomaTransport::RpcId(102, 105), 175, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    HomaTransport::ScheduledMessage* m5 = getServerRpc(
            HomaTransport::RpcId(102, 105))->scheduledMessage.get();
    EXPECT_EQ(2u, transport.activeMessages.size());
    EXPECT_EQ(HomaTransport::ScheduledMessage::ACTIVE, m2->state);
    EXPECT_EQ(HomaTransport::ScheduledMessage::ACTIVE, m5->state);

    // m6 is from client 4 and m5 < m6. It should go to inactive message list.
    handlePacket("mock:client=4",
            HomaTransport::DataHeader(HomaTransport::RpcId(103, 106), 176, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    HomaTransport::ScheduledMessage* m6 = getServerRpc(
            HomaTransport::RpcId(103, 106))->scheduledMessage.get();
    EXPECT_EQ(HomaTransport::ScheduledMessage::INACTIVE, m6->state);

    // Receive one more packet from m6. Now m6 is shorter than m5.
    handlePacket("mock:client=4",
            HomaTransport::DataHeader(HomaTransport::RpcId(103, 106), 176, 5,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "fghij");
    EXPECT_EQ(2u, transport.activeMessages.size());
    EXPECT_EQ(HomaTransport::ScheduledMessage::ACTIVE, m2->state);
    EXPECT_EQ(HomaTransport::ScheduledMessage::ACTIVE, m6->state);

    EXPECT_EQ(&transport.activeMessages.front(), m2);
    EXPECT_EQ(&transport.activeMessages.back(), m6);
}
TEST_F(HomaTransportTest, dataPacketArrive) {
    transport.roundTripBytes = 100;
    transport.maxDataPerPacket = 5;
    transport.grantIncrement = 5;
    transport.maxGrantedMessages = 2;
    uint32_t unscheduledBytes = transport.roundTripBytes;

    handlePacket("mock:client=1",
            HomaTransport::DataHeader(HomaTransport::RpcId(100, 101), 300, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    HomaTransport::ScheduledMessage* m1 = getServerRpc(
            HomaTransport::RpcId(100, 101))->scheduledMessage.get();
    EXPECT_EQ(transport.messagesToGrant.back(), m1);
    EXPECT_EQ(0, m1->grantPriority);

    // m2 < m1
    handlePacket("mock:client=2",
            HomaTransport::DataHeader(HomaTransport::RpcId(101, 102), 250, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    HomaTransport::ScheduledMessage* m2 = getServerRpc(
            HomaTransport::RpcId(101, 102))->scheduledMessage.get();
    EXPECT_EQ(transport.messagesToGrant.back(), m2);
    EXPECT_EQ(1, m2->grantPriority);

    // m3 < m2. Use unscheduled priority for the last 1 RTT bytes.
    handlePacket("mock:client=3",
            HomaTransport::DataHeader(HomaTransport::RpcId(102, 103), 200, 0,
            unscheduledBytes, HomaTransport::FROM_CLIENT), "abcde");
    HomaTransport::ScheduledMessage* m3 = getServerRpc(
            HomaTransport::RpcId(102, 103))->scheduledMessage.get();
    EXPECT_EQ(transport.messagesToGrant.back(), m3);
    EXPECT_EQ(7, m3->grantPriority);

    EXPECT_EQ(3u, transport.messagesToGrant.size());
}

}  // namespace RAMCloud
