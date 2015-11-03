/* Copyright (c) 2015 Stanford University
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
    ServiceLocator locator1;
    ServiceLocator locator2;
    MockDriver::MockAddress address1;
    MockDriver::MockAddress address2;
    Transport::SessionRef session;
    TestLog::Enable logEnabler;

    BasicTransportTest()
        : context(false)
        , driver(new MockDriver(headerToString))
        , transport(&context, driver, 666)
        , locator1("mock:node=1")
        , locator2("mock:node=2")
        , address1(&locator1)
        , address2(&locator2)
        , session(transport.getSession(&locator1))
        , logEnabler()
    {
        context.workerManager = new WorkerManager(&context, 5);
        context.workerManager->testingSaveRpcs = 1;
    }

    ~BasicTransportTest()
    {
    }

    // Given to MockTransport to convert a packet header to a human-readable
    // string.
    static string headerToString(const void* header, uint32_t headerLength)
    {
        string result;
        if (headerLength < sizeof32(BasicTransport::CommonHeader)) {
            return format("header too short: only %u bytes", headerLength);
        }
        const BasicTransport::CommonHeader* common =
                static_cast<const BasicTransport::CommonHeader*>(header);
        result += BasicTransport::opcodeSymbol(common->opcode);
        result += format(" (rpcId %lu.%lu",
                common->rpcId.clientId, common->rpcId.sequence);
        uint32_t expectedLength = headerLength;
        switch (common->opcode) {
            case BasicTransport::PacketOpcode::ALL_DATA:
                expectedLength = sizeof32(BasicTransport::AllDataHeader);
                break;
            case BasicTransport::PacketOpcode::DATA: {
                expectedLength = sizeof32(BasicTransport::DataHeader);
                const BasicTransport::DataHeader* data =
                        static_cast<const BasicTransport::DataHeader*>(header);
                result += format(", totalLength %u, offset %u, needGrant %u",
                        data->totalLength, data->offset, data->needGrant);
                break;
            }
            case BasicTransport::PacketOpcode::GRANT: {
                expectedLength = sizeof32(BasicTransport::GrantHeader);
                const BasicTransport::GrantHeader* grant =
                        static_cast<const BasicTransport::GrantHeader*>(header);
                result += format(", offset %u", grant->offset);
                break;
            }
            case BasicTransport::PacketOpcode::RESEND: {
                expectedLength = sizeof32(BasicTransport::ResendHeader);
                const BasicTransport::ResendHeader* resend =
                        static_cast<const BasicTransport::ResendHeader*>(
                        header);
                result += format(", offset %u, length %u",
                        resend->offset, resend->length);
                break;
            }
            case BasicTransport::PacketOpcode::PING:
                expectedLength = sizeof32(BasicTransport::PingHeader);
                break;
            case BasicTransport::PacketOpcode::RETRY:
                expectedLength = sizeof32(BasicTransport::RetryHeader);
                break;
        }
        if (headerLength != expectedLength) {
            result += format(", bad header length %u (expected %u)",
                    headerLength, expectedLength);
        }
        result += ")";
        return result;
    }

    // Convenience method: receive request, prepare response, but don't
    // call sendReply yet.
    BasicTransport::ServerRpc*
    prepareToRespond()
    {
        driver->receivePacket("mock:client=1",
                BasicTransport::AllDataHeader(BasicTransport::RpcId(100, 101)),
                "message1");
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
    BasicTransport server(&context, serverDriver, 1);
    UdpDriver* clientDriver = new UdpDriver(&context);
    BasicTransport client(&context, clientDriver, 2);
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
    EXPECT_EQ(19236u, transport.roundTripBytes);
    EXPECT_TRUE(transport.timer.isRunning());
}

TEST_F(BasicTransportTest, sendBytes_adjustLength) {
    Buffer buffer;
    buffer.append("abcdefghijklmno", 15);
    transport.sendBytes(&address1, BasicTransport::RpcId(5, 6), &buffer,
            5, 100);
    EXPECT_EQ("DATA (rpcId 5.6, totalLength 15, offset 5, needGrant 0) "
            "fghijklmno",
            driver->outputLog);
}
TEST_F(BasicTransportTest, sendBytes_lengthZero) {
    Buffer buffer;
    buffer.append("abcdefghijklmnop", 16);
    transport.sendBytes(&address1, BasicTransport::RpcId(5, 6), &buffer,
            5, 0);
    transport.sendBytes(&address1, BasicTransport::RpcId(5, 6), &buffer,
            50, 5);
    EXPECT_EQ("", driver->outputLog);
}
TEST_F(BasicTransportTest, sendBytes_shortMessage) {
    Buffer buffer;
    buffer.append("abcdefghijklmno", 15);
    transport.sendBytes(&address1, BasicTransport::RpcId(5, 6), &buffer,
            0, 16);
    EXPECT_EQ("ALL_DATA (rpcId 5.6) abcdefghij (+5 more)",
            driver->outputLog);
}
TEST_F(BasicTransportTest, sendBytes_multiplePackets) {
    transport.maxDataPerPacket = 6;
    Buffer buffer;
    buffer.append("abcdefghijklmno", 15);
    transport.sendBytes(&address1, BasicTransport::RpcId(5, 6), &buffer,
            1, 15);
    EXPECT_EQ(
            "DATA (rpcId 5.6, totalLength 15, offset 1, needGrant 0) bcdefg | "
            "DATA (rpcId 5.6, totalLength 15, offset 7, needGrant 0) hijklm | "
            "DATA (rpcId 5.6, totalLength 15, offset 13, needGrant 0) no",
            driver->outputLog);
}
TEST_F(BasicTransportTest, sendBytes_needGrant) {
    transport.maxDataPerPacket = 10;
    Buffer buffer;
    buffer.append("abcdefghijklmno", 15);
    transport.sendBytes(&address1, BasicTransport::RpcId(5, 6), &buffer,
            1, 14);
    EXPECT_EQ(
            "DATA (rpcId 5.6, totalLength 15, offset 1, needGrant 0) "
            "bcdefghijk | "
            "DATA (rpcId 5.6, totalLength 15, offset 11, needGrant 0) lmno",
            driver->outputLog);
    driver->outputLog.clear();
    transport.sendBytes(&address1, BasicTransport::RpcId(5, 6), &buffer,
            1, 13);
    EXPECT_EQ(
            "DATA (rpcId 5.6, totalLength 15, offset 1, needGrant 1) "
            "bcdefghijk | "
            "DATA (rpcId 5.6, totalLength 15, offset 11, needGrant 1) lmn",
            driver->outputLog);
}

TEST_F(BasicTransportTest, Session_constructor) {
    ServiceLocator locator("basic+udp: host=localhost, port=11101");
    UdpDriver* driver2 = new UdpDriver(&context, &locator);
    BasicTransport transport2(&context, driver2, 1);
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
    Buffer request1, request2;
    Buffer response1, response2;
    request1.append("message1", 8);
    request1.append("message2", 8);
    session->sendRequest(&request1, &response1, &notifier1);
    session->sendRequest(&request2, &response2, &notifier2);
    EXPECT_EQ(2u, transport.outgoingRpcs.size());
    session->cancelRequest(&notifier1);
    EXPECT_EQ(1u, transport.outgoingRpcs.size());
    EXPECT_EQ(1u, transport.clientRpcPool.outstandingObjects);
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
    EXPECT_EQ("ALL_DATA (rpcId 666.1) message1 | "
            "ALL_DATA (rpcId 666.2) message2",
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
            BasicTransport::AllDataHeader(BasicTransport::RpcId(666, 1)),
            "response1");
    EXPECT_EQ("handlePacket: unknown packet for client",
            TestLog::get());
    EXPECT_EQ(0u, driver->stealCount);
}
TEST_F(BasicTransportTest, handlePacket_allDataForClient) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->receivePacket("mock:server=1",
            BasicTransport::AllDataHeader(BasicTransport::RpcId(666, 1)),
            "response1");
    EXPECT_STREQ("completed: 1, failed: 0", wrapper.getState());
    EXPECT_EQ("response1", TestUtil::toString(&wrapper.response));
    EXPECT_EQ(0lu, transport.outgoingRpcs.size());
    EXPECT_EQ(1u, driver->stealCount);
}
TEST_F(BasicTransportTest, handlePacket_dataForClient_incompleteHeader) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    driver->receivePacket("mock:server=1", BasicTransport::CommonHeader(
            BasicTransport::DATA, BasicTransport::RpcId(666, 1)));
    EXPECT_EQ("handlePacket: packet of type DATA from mock:server=1 too "
            "short (17 bytes)", TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_dataForClient_basics) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    transport.roundTripBytes = 1000;
    transport.grantIncrement = 500;

    // First packet of response.
    driver->receivePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 10, 0, 1),
            "abcde");
    EXPECT_STREQ("completed: 0, failed: 0", wrapper.getState());
    EXPECT_EQ("GRANT (rpcId 666.1, offset 1505)",
            driver->outputLog);
    EXPECT_EQ("abcde", TestUtil::toString(&wrapper.response));
    EXPECT_EQ(1u, driver->stealCount);

    // Second packet of response
    driver->outputLog.clear();
    driver->receivePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 10, 5, 0),
            "12345");
    EXPECT_STREQ("completed: 1, failed: 0", wrapper.getState());
    EXPECT_EQ("abcde12345", TestUtil::toString(&wrapper.response));
    EXPECT_EQ("", driver->outputLog);
    EXPECT_EQ(0lu, transport.outgoingRpcs.size());
    EXPECT_EQ(2u, driver->stealCount);
}
TEST_F(BasicTransportTest, handlePacket_dataForClient_dontIssueGrant) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    transport.roundTripBytes = 1000;
    transport.grantIncrement = 500;

    // First packet of response (don't send grant: needGrant not set).
    driver->receivePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 15, 0, 0),
            "abcde");
    EXPECT_EQ("", driver->outputLog);

    // Retransmit first packet, with request for grant this time.
    driver->receivePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 15, 0, 1),
            "abcde");
    EXPECT_EQ("GRANT (rpcId 666.1, offset 1505)", driver->outputLog);

    // Second packet of response (still not complete, but no need for
    // another grant).
    driver->outputLog.clear();
    driver->receivePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 15,
            10, 1), "12345");
    EXPECT_EQ("", driver->outputLog);
    EXPECT_EQ(1lu, transport.outgoingRpcs.size());

    // Third packet of response (now complete)
    driver->outputLog.clear();
    driver->receivePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 15, 5, 0),
            "xyzzy");
    EXPECT_STREQ("completed: 1, failed: 0", wrapper.getState());
    EXPECT_EQ(0lu, transport.outgoingRpcs.size());
    EXPECT_EQ("abcdexyzzy12345", TestUtil::toString(&wrapper.response));
}
TEST_F(BasicTransportTest, handlePacket_grantForClient_incompleteHeader) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    driver->receivePacket("mock:server=1", BasicTransport::CommonHeader(
            BasicTransport::GRANT, BasicTransport::RpcId(666, 1)));
    EXPECT_EQ("handlePacket: packet of type GRANT from mock:server=1 too "
            "short (17 bytes)", TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_grantForClient) {
    MockWrapper wrapper("abcdefghij0123456789");
    transport.roundTripBytes = 10;
    transport.grantIncrement = 5;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    EXPECT_EQ("DATA (rpcId 666.1, totalLength 20, offset 0, needGrant 1) "
            "abcdefghij", driver->outputLog);
    driver->outputLog.clear();

    // First grant doesn't get past transmitOffset.
    driver->receivePacket("mock:server=1",
            BasicTransport::GrantHeader(BasicTransport::RpcId(666, 1), 10));
    EXPECT_EQ("", driver->outputLog);

    // Second grant is far enough out to enable more bytes to be sent.
    driver->receivePacket("mock:server=1",
            BasicTransport::GrantHeader(BasicTransport::RpcId(666, 1), 15));
    EXPECT_EQ(
            "DATA (rpcId 666.1, totalLength 20, offset 10, needGrant 1) 01234",
            driver->outputLog);
    EXPECT_EQ(15lu, transport.outgoingRpcs[1]->transmitOffset);
}
TEST_F(BasicTransportTest, handlePacket_resendForClient_incompleteHeader) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    driver->receivePacket("mock:server=1", BasicTransport::CommonHeader(
            BasicTransport::RESEND, BasicTransport::RpcId(666, 1)));
    EXPECT_EQ("handlePacket: packet of type RESEND from mock:server=1 too "
            "short (17 bytes)", TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_resendForClient) {
    MockWrapper wrapper("abcdefghij0123456789");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    EXPECT_EQ("ALL_DATA (rpcId 666.1) abcdefghij (+10 more)",
            driver->outputLog);
    driver->outputLog.clear();

    driver->receivePacket("mock:server=1", BasicTransport::ResendHeader(
            BasicTransport::RpcId(666, 1), 12, 20));
    EXPECT_EQ("DATA (rpcId 666.1, totalLength 20, offset 12, needGrant 0) "
            "23456789",
            driver->outputLog);
}
TEST_F(BasicTransportTest, handlePacket_resendForClient_transmitOffsetChanges) {
    MockWrapper wrapper("abcdefghij0123456789");
    transport.roundTripBytes = 10;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    EXPECT_EQ("DATA (rpcId 666.1, totalLength 20, offset 0, needGrant 1) "
            "abcdefghij",
            driver->outputLog);
    driver->outputLog.clear();
    EXPECT_EQ(10lu, transport.outgoingRpcs[1]->transmitOffset);

    driver->receivePacket("mock:server=1", BasicTransport::ResendHeader(
            BasicTransport::RpcId(666, 1), 8, 6));
    EXPECT_EQ("DATA (rpcId 666.1, totalLength 20, offset 8, needGrant 1) "
            "ij0123",
            driver->outputLog);
    EXPECT_EQ(14lu, transport.outgoingRpcs[1]->transmitOffset);
}
TEST_F(BasicTransportTest, handlePacket_retryForClient) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    EXPECT_EQ("ALL_DATA (rpcId 666.1) message1", driver->outputLog);
    driver->outputLog.clear();

    driver->receivePacket("mock:server=1", BasicTransport::RetryHeader(
            BasicTransport::RpcId(666, 1)));
    EXPECT_STREQ("completed: 1, failed: 0", wrapper.getState());
    EXPECT_EQ(0lu, transport.outgoingRpcs.size());
    EXPECT_EQ(0u, driver->stealCount);
    WireFormat::RetryResponse* response =
            wrapper.response.getStart<WireFormat::RetryResponse>();
    ASSERT_TRUE(response != NULL);
    EXPECT_STREQ("STATUS_RETRY", statusToSymbol(response->common.status));
}
TEST_F(BasicTransportTest, handlePacket_unknownOpcodeForClient) {
    MockWrapper wrapper("message1");
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();
    driver->receivePacket("mock:server=1", BasicTransport::CommonHeader(
            BasicTransport::PING, BasicTransport::RpcId(666, 1)));
    EXPECT_EQ("handlePacket: unexpected opcode PING received from "
            "server mock:server=1", TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_allDataForServer) {
    driver->receivePacket("mock:client=1",
            BasicTransport::AllDataHeader(BasicTransport::RpcId(100, 101)),
            "message1");
    BasicTransport::ServerRpc* serverRpc =
            static_cast<BasicTransport::ServerRpc*>(
            context.workerManager->waitForRpc(0));
    ASSERT_TRUE(serverRpc != NULL);
    EXPECT_EQ("message1", TestUtil::toString(&serverRpc->requestPayload));
    EXPECT_TRUE(serverRpc->requestComplete);
}
TEST_F(BasicTransportTest, handlePacket_allDataForServer_duplicate) {
    driver->receivePacket("mock:client=1",
            BasicTransport::AllDataHeader(BasicTransport::RpcId(100, 101)),
            "message1");
    BasicTransport::ServerRpc* serverRpc =
            static_cast<BasicTransport::ServerRpc*>(
            context.workerManager->waitForRpc(0));
    ASSERT_TRUE(serverRpc != NULL);
    driver->receivePacket("mock:client=1",
            BasicTransport::AllDataHeader(BasicTransport::RpcId(100, 101)),
            "0123457890abcdefghij");
    EXPECT_EQ("message1", TestUtil::toString(&serverRpc->requestPayload));
}
TEST_F(BasicTransportTest, handlePacket_dataForServer_basics) {
    // Send a message in three packets. The first packet should result
    // in a GRANT.
    transport.roundTripBytes = 1000;
    transport.grantIncrement = 500;
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15,
            10, 1), "abcde");
    BasicTransport::ServerRpcMap::iterator it = transport.incomingRpcs.find(
            BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    BasicTransport::ServerRpc* serverRpc = it->second;
    EXPECT_FALSE(serverRpc->requestComplete);
    EXPECT_EQ("GRANT (rpcId 100.101, offset 1500)", driver->outputLog);

    // Second packet: no GRANT should result.
    driver->outputLog.clear();
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15,
            0, 1), "01234");
    EXPECT_FALSE(serverRpc->requestComplete);
    EXPECT_EQ(1500u, serverRpc->grantOffset);
    EXPECT_EQ("", driver->outputLog);
    EXPECT_EQ(1lu, transport.incomingRpcs.size());
    EXPECT_EQ(1lu, transport.serverTimerList.size());

    // Third packet: no GRANT, message should now be complete.
    driver->outputLog.clear();
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15,
            5, 0), "56789");
    EXPECT_TRUE(serverRpc->requestComplete);
    EXPECT_EQ("", driver->outputLog);
    EXPECT_EQ("0123456789abcde",
            TestUtil::toString(&serverRpc->requestPayload));
}
TEST_F(BasicTransportTest, handlePacket_dataForServer_dontIssueGrant) {
    transport.roundTripBytes = 1000;
    transport.grantIncrement = 500;
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15,
            10, 0), "abcde");
    EXPECT_EQ("", driver->outputLog);
}
TEST_F(BasicTransportTest, handlePacket_dataForServer_extraneousPacket) {
    // Send an extra packet after the message is complete.
    driver->receivePacket("mock:client=1",
            BasicTransport::AllDataHeader(BasicTransport::RpcId(100, 101)),
            "message1");
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 13,
            8, 1), "abcde");
    EXPECT_EQ("handlePacket: ignoring extraneous packet", TestLog::get());
    BasicTransport::ServerRpc* serverRpc =
            static_cast<BasicTransport::ServerRpc*>(
            context.workerManager->waitForRpc(0));
    EXPECT_TRUE(serverRpc != NULL);
    EXPECT_EQ("message1", TestUtil::toString(&serverRpc->requestPayload));
}
TEST_F(BasicTransportTest, handlePacket_grantForServer_bogusGrants) {
    prepareToRespond();
    transport.roundTripBytes = 5;

    // GRANT arriving for unknown RpcID: bogus.
    driver->receivePacket("mock:client=1",
            BasicTransport::GrantHeader(BasicTransport::RpcId(5, 6), 10));
    EXPECT_EQ("handlePacket: unexpected GRANT from client mock:client=1, "
            "id (5,6), grantOffset 10",
            TestLog::get());

    // GRANT arriving before result transmission starts: bogus.
    TestLog::reset();
    driver->receivePacket("mock:client=1",
            BasicTransport::GrantHeader(BasicTransport::RpcId(100, 101), 10));
    EXPECT_EQ("handlePacket: unexpected GRANT from client mock:client=1, "
            "id (100,101), grantOffset 10",
            TestLog::get());

    // GRANT packet too short: bogus.
    TestLog::reset();
    driver->receivePacket("mock:client=1", BasicTransport::CommonHeader(
            BasicTransport::GRANT, BasicTransport::RpcId(100, 101)));
    EXPECT_EQ("handlePacket: packet of type GRANT from mock:client=1 "
            "too short (17 bytes)", TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_grantForServer) {
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    transport.roundTripBytes = 5;
    serverRpc->sendReply();
    driver->outputLog.clear();

    // First, send redundant grant (do nothing).
    driver->receivePacket("mock:client=1",
            BasicTransport::GrantHeader(BasicTransport::RpcId(100, 101), 5));
    EXPECT_EQ("", driver->outputLog);

    // Second grant should cause more data to be transmitted.
    driver->receivePacket("mock:client=1",
            BasicTransport::GrantHeader(BasicTransport::RpcId(100, 101), 15));
    EXPECT_EQ("DATA (rpcId 100.101, totalLength 20, offset 5, needGrant 1) "
            "56789abcde",
            driver->outputLog);

    // Third grant should complete the result transmission.
    driver->outputLog.clear();
    driver->receivePacket("mock:client=1",
            BasicTransport::GrantHeader(BasicTransport::RpcId(100, 101), 25));
    EXPECT_EQ("DATA (rpcId 100.101, totalLength 20, offset 15, needGrant 0) "
            "fghij",
            driver->outputLog);
    EXPECT_EQ(0lu, transport.incomingRpcs.size());
    EXPECT_EQ(0lu, transport.serverTimerList.size());
    EXPECT_EQ(0lu, transport.serverRpcPool.outstandingAllocations);
}
TEST_F(BasicTransportTest, handlePacket_resendForServer_bogusResends) {
    prepareToRespond();
    transport.roundTripBytes = 5;

    // RESEND arriving before result transmission starts: bogus.
    TestLog::reset();
    driver->receivePacket("mock:client=1",
            BasicTransport::ResendHeader(BasicTransport::RpcId(100, 101),
            10, 5));
    EXPECT_EQ("handlePacket: unexpected RESEND from client mock:client=1",
            TestLog::get());

    // RESEND packet too short: bogus.
    TestLog::reset();
    driver->receivePacket("mock:client=1", BasicTransport::CommonHeader(
            BasicTransport::RESEND, BasicTransport::RpcId(100, 101)));
    EXPECT_EQ("handlePacket: packet of type RESEND from mock:client=1 "
            "too short (17 bytes)", TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_resendForServer_unknownRpcId) {
    driver->receivePacket("mock:client=1",
            BasicTransport::ResendHeader(BasicTransport::RpcId(10, 11),
            10, 5));
    EXPECT_EQ("handlePacket: received RESEND from client mock:client=1, "
            "but RPC state no longer exists",
            TestLog::get());
    EXPECT_EQ("RETRY (rpcId 10.11)", driver->outputLog);
}
TEST_F(BasicTransportTest, handlePacket_resendForServer_sendBytes) {
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    transport.roundTripBytes = 15;
    serverRpc->sendReply();

    driver->outputLog.clear();
    driver->receivePacket("mock:client=1",
            BasicTransport::ResendHeader(BasicTransport::RpcId(100, 101),
            5, 8));
    EXPECT_EQ("DATA (rpcId 100.101, totalLength 20, offset 5, needGrant 1) "
            "56789abc",
            driver->outputLog);
}
TEST_F(BasicTransportTest, handlePacket_resendForServer_transmitOffsetChanges) {
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    transport.roundTripBytes = 15;
    serverRpc->sendReply();

    // First RESEND advances transmitOffset but doesn't finish message.
    driver->outputLog.clear();
    driver->receivePacket("mock:client=1",
            BasicTransport::ResendHeader(BasicTransport::RpcId(100, 101),
            10, 7));
    EXPECT_EQ("DATA (rpcId 100.101, totalLength 20, offset 10, needGrant 1) "
            "abcdefg",
            driver->outputLog);
    EXPECT_EQ(17u, serverRpc->transmitOffset);
    EXPECT_EQ("", TestLog::get());

    // Second RESEND finishes the message.
    driver->outputLog.clear();
    driver->receivePacket("mock:client=1",
            BasicTransport::ResendHeader(BasicTransport::RpcId(100, 101),
            15, 10));
    EXPECT_EQ("DATA (rpcId 100.101, totalLength 20, offset 15, needGrant 0) "
            "fghij",
            driver->outputLog);
    EXPECT_EQ("deleteServerRpc: RpcId (100, 101)", TestLog::get());
}
TEST_F(BasicTransportTest, handlePacket_pingForServer_unknownRpcId) {
    transport.roundTripBytes = 1000;
    driver->receivePacket("mock:client=1",
            BasicTransport::PingHeader(BasicTransport::RpcId(10, 11)));
    EXPECT_EQ("RESEND (rpcId 10.11, offset 0, length 1000)",
            driver->outputLog);
}
TEST_F(BasicTransportTest, handlePacket_pingForServer_stillProcessing) {
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    transport.roundTripBytes = 15;
    serverRpc->grantOffset = 100;
    driver->receivePacket("mock:client=1",
            BasicTransport::PingHeader(BasicTransport::RpcId(100, 101)));
    EXPECT_EQ("GRANT (rpcId 100.101, offset 100)",
            driver->outputLog);
}
TEST_F(BasicTransportTest, handlePacket_pingForServer_resultPartiallySent) {
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    transport.roundTripBytes = 15;
    transport.maxDataPerPacket = 5;
    serverRpc->sendReply();
    EXPECT_EQ("DATA (rpcId 100.101, totalLength 20, offset 0, needGrant 1) "
            "01234 | "
            "DATA (rpcId 100.101, totalLength 20, offset 5, needGrant 1) "
            "56789 | "
            "DATA (rpcId 100.101, totalLength 20, offset 10, needGrant 1) "
            "abcde",
            driver->outputLog);
    driver->outputLog.clear();
    driver->receivePacket("mock:client=1",
            BasicTransport::PingHeader(BasicTransport::RpcId(100, 101)));
    EXPECT_EQ("DATA (rpcId 100.101, totalLength 20, offset 0, needGrant 1) "
            "01234",
            driver->outputLog);
}
TEST_F(BasicTransportTest, handlePacket_unknownOpcodeForServer) {
    driver->receivePacket("mock:client=1", BasicTransport::CommonHeader(
            BasicTransport::RETRY, BasicTransport::RpcId(100, 101)));
    EXPECT_EQ("handlePacket: unexpected opcode RETRY received from client "
            "mock:client=1", TestLog::get());
}

TEST_F(BasicTransportTest, sendReply_sendEntireMessage) {
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    serverRpc->sendReply();
    EXPECT_EQ("ALL_DATA (rpcId 100.101) 0123456789 (+10 more)",
            driver->outputLog);
    EXPECT_EQ("deleteServerRpc: RpcId (100, 101)", TestLog::get());
}
TEST_F(BasicTransportTest, sendReply_sendPartialMessage) {
    BasicTransport::ServerRpc* serverRpc = prepareToRespond();
    transport.roundTripBytes = 10;
    serverRpc->sendReply();
    EXPECT_EQ("DATA (rpcId 100.101, totalLength 20, offset 0, needGrant 1) "
            "0123456789",
            driver->outputLog);
    EXPECT_EQ("", TestLog::get());
    EXPECT_EQ(10lu, serverRpc->transmitOffset);
}

TEST_F(BasicTransportTest, MessageAccumulator_destructor) {
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20,
            5, 0), "abcde");
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20,
            15, 0), "56789");
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20,
            10, 0), "01234");
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
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 25,
            20, 0), "P4444");
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 25,
            10, 0), "P2222");
    BasicTransport::ServerRpcMap::iterator it =
            transport.incomingRpcs.find(BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    BasicTransport::ServerRpc* serverRpc = it->second;
    ASSERT_TRUE(serverRpc->accumulator);
    EXPECT_EQ(2u, serverRpc->accumulator->fragments.size());

    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 25,
            0, 0), "P0000");
    EXPECT_EQ(2u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ("P0000",
            TestUtil::toString(&serverRpc->requestPayload));

    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 25,
            5, 0), "P1111");
    EXPECT_EQ(1u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ("P0000P1111P2222",
            TestUtil::toString(&serverRpc->requestPayload));

    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 25,
            15, 0), "P3333");
    EXPECT_EQ(0u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ("P0000P1111P2222P3333P4444",
            TestUtil::toString(&serverRpc->requestPayload));
}
TEST_F(BasicTransportTest, addPacket_addMultipleFragmentsAtOnce) {
    // Receive a request in 4 packets, in the order P3, P2, P1, P0.
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20,
            15, 0), "P3333");
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20,
            10, 0), "P2222");
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20,
            5, 0), "P1111");
    BasicTransport::ServerRpcMap::iterator it =
            transport.incomingRpcs.find(BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    BasicTransport::ServerRpc* serverRpc = it->second;
    ASSERT_TRUE(serverRpc->accumulator);
    EXPECT_EQ(3u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ("", TestUtil::toString(&serverRpc->requestPayload));

    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20,
            0, 0), "P0000");
    EXPECT_EQ(0u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ("P0000P1111P2222P3333",
            TestUtil::toString(&serverRpc->requestPayload));
}

TEST_F(BasicTransportTest, appendFragment_discardFragment) {
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20,
            5, 0), "xxxxx");
    BasicTransport::ServerRpcMap::iterator it =
            transport.incomingRpcs.find(BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    BasicTransport::ServerRpc* serverRpc = it->second;
    ASSERT_TRUE(serverRpc->accumulator);
    EXPECT_EQ(1u, serverRpc->accumulator->fragments.size());

    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20,
            0, 0), "0123456789");
    EXPECT_EQ(0u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ("0123456789", TestUtil::toString(&serverRpc->requestPayload));
    EXPECT_EQ(1u, driver->releaseCount);
}

TEST_F(BasicTransportTest, appendFragment_truncateFragments) {
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20,
            0, 0), "xxxxx");
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20,
            8, 0), "zzzzz");
    BasicTransport::ServerRpcMap::iterator it =
            transport.incomingRpcs.find(BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    BasicTransport::ServerRpc* serverRpc = it->second;
    ASSERT_TRUE(serverRpc->accumulator);
    EXPECT_EQ(1u, serverRpc->accumulator->fragments.size());

    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20,
            3, 0), "yyyyyy");
    EXPECT_EQ(0u, serverRpc->accumulator->fragments.size());
    EXPECT_EQ("xxxxxyyyyzzzz", TestUtil::toString(&serverRpc->requestPayload));
    EXPECT_EQ(0u, driver->releaseCount);
}

TEST_F(BasicTransportTest, requestRetransmission) {
    transport.roundTripBytes = 100;
    transport.grantIncrement = 50;

    // First retransmit: no fragments waiting, no grants sent.
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20,
            0, 0), "01234");
    BasicTransport::ServerRpcMap::iterator it =
            transport.incomingRpcs.find(BasicTransport::RpcId(100, 101));
    ASSERT_TRUE(it != transport.incomingRpcs.end());
    BasicTransport::ServerRpc* serverRpc = it->second;
    ASSERT_TRUE(serverRpc->accumulator);
    EXPECT_EQ(0u, serverRpc->accumulator->fragments.size());

    driver->outputLog.clear();
    serverRpc->accumulator->requestRetransmission(&transport, &address1,
            BasicTransport::RpcId(100, 101), 0);
    EXPECT_EQ("RESEND (rpcId 100.101, offset 5, length 100)",
            driver->outputLog);

    // Second retransmit: no fragment, but grant was sent.
    driver->outputLog.clear();
    serverRpc->accumulator->requestRetransmission(&transport, &address1,
            BasicTransport::RpcId(100, 101), 25);
    EXPECT_EQ("RESEND (rpcId 100.101, offset 5, length 20)",
            driver->outputLog);

    // Third retransmit: fragment waiting.
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 20,
            15, 0), "abcde");
    driver->outputLog.clear();
    serverRpc->accumulator->requestRetransmission(&transport, &address1,
            BasicTransport::RpcId(100, 101), 25);
    EXPECT_EQ("RESEND (rpcId 100.101, offset 5, length 10)",
            driver->outputLog);
}

TEST_F(BasicTransportTest, handleTimerEvent_clientRequestTimeout) {
    // This test just ensures that the timer will reschedule itself
    // properly and eventually time out a client request.
    MockWrapper wrapper(NULL);
    WireFormat::RequestCommon* header =
            wrapper.request.emplaceAppend<WireFormat::RequestCommon>();
    header->opcode = WireFormat::READ;
    header->service = WireFormat::MASTER_SERVICE;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    transport.timeoutIntervals = 5;
    uint64_t start = Cycles::rdtsc();
    while (1) {
        context.dispatch->poll();
        if ((wrapper.failedCount != 0) ||
                (Cycles::toSeconds(Cycles::rdtsc() - start) > 0.1)) {
            break;
        }
    }
    EXPECT_EQ(1, wrapper.failedCount);
    EXPECT_EQ("handleTimerEvent: sending PING to server mock:node=1 for "
            "READ RPC | handleTimerEvent: aborting READ RPC to server "
            "mock:node=1: timeout", TestLog::get());
}
TEST_F(BasicTransportTest, handleTimerEvent_sendPingsFromClient) {
    MockWrapper wrapper(NULL);
    WireFormat::RequestCommon* header =
            wrapper.request.emplaceAppend<WireFormat::RequestCommon>();
    header->opcode = WireFormat::READ;
    header->service = WireFormat::MASTER_SERVICE;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->outputLog.clear();

    transport.timer.handleTimerEvent();
    EXPECT_EQ("", TestLog::get());

    transport.timer.handleTimerEvent();
    EXPECT_EQ("handleTimerEvent: sending PING to server mock:node=1 for "
            "READ RPC", TestLog::get());
    EXPECT_EQ("PING (rpcId 666.1)", driver->outputLog);

    driver->outputLog.clear();
    transport.pingIntervals = 5;
    transport.outgoingRpcs[1]->silentIntervals = 8;
    transport.timer.handleTimerEvent();
    EXPECT_EQ("", driver->outputLog);

    transport.timer.handleTimerEvent();
    EXPECT_EQ("PING (rpcId 666.1)", driver->outputLog);
}
TEST_F(BasicTransportTest, handleTimerEvent_sendResendFromClient) {
    transport.roundTripBytes = 100;
    transport.grantIncrement = 50;
    MockWrapper wrapper(NULL);
    WireFormat::RequestCommon* header =
            wrapper.request.emplaceAppend<WireFormat::RequestCommon>();
    header->opcode = WireFormat::READ;
    header->service = WireFormat::MASTER_SERVICE;
    session->sendRequest(&wrapper.request, &wrapper.response, &wrapper);
    driver->receivePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 10,
            0, 1), "abcde");
    driver->outputLog.clear();

    transport.timer.handleTimerEvent();
    EXPECT_EQ("", TestLog::get());

    transport.timer.handleTimerEvent();
    EXPECT_EQ("requestRetransmission: requested retransmit of response "
            "bytes 5-155 from mock:node=1", TestLog::get());
    EXPECT_EQ("RESEND (rpcId 666.1, offset 5, length 150)", driver->outputLog);

    driver->outputLog.clear();
    transport.timer.handleTimerEvent();
    EXPECT_EQ("RESEND (rpcId 666.1, offset 5, length 150)", driver->outputLog);

    // If a packet arrives, RESENDS stop (for a while).
    driver->outputLog.clear();
    driver->receivePacket("mock:server=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(666, 1), 10, 5, 0),
            "fgh");
    transport.timer.handleTimerEvent();
    EXPECT_EQ("", driver->outputLog);

    transport.timer.handleTimerEvent();
    EXPECT_EQ("RESEND (rpcId 666.1, offset 8, length 147)", driver->outputLog);
}
TEST_F(BasicTransportTest, handleTimerEvent_serverAbortsRequest) {
    transport.timeoutIntervals = 2;
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15,
            0, 0), "abcde");

    transport.timer.handleTimerEvent();
    EXPECT_EQ("", TestLog::get());

    transport.timer.handleTimerEvent();
    EXPECT_EQ("handleTimerEvent: aborting unknown(25185) RPC from client "
            "mock:client=1: timeout while receiving request | "
            "deleteServerRpc: RpcId (100, 101)",
            TestLog::get());
}
TEST_F(BasicTransportTest, handleTimerEvent_sendResendFromServer) {
    transport.roundTripBytes = 100;
    transport.grantIncrement = 50;
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15,
            0, 0), "abcde");
    driver->outputLog.clear();

    transport.timer.handleTimerEvent();
    EXPECT_EQ("", TestLog::get());

    transport.timer.handleTimerEvent();
    EXPECT_EQ("requestRetransmission: requested retransmit of request "
            "bytes 5-105 from mock:client=1",
            TestLog::get());
    EXPECT_EQ("RESEND (rpcId 100.101, offset 5, length 100)",
            driver->outputLog);

    // Packet arrival stops resends (for a while).
    driver->receivePacket("mock:client=1",
            BasicTransport::DataHeader(BasicTransport::RpcId(100, 101), 15,
            5, 0), "fgh");
    driver->outputLog.clear();
    transport.timer.handleTimerEvent();
    EXPECT_EQ("", driver->outputLog);

    transport.timer.handleTimerEvent();
    EXPECT_EQ("RESEND (rpcId 100.101, offset 8, length 100)",
            driver->outputLog);
}

}  // namespace RAMCloud
