/* Copyright (c) 2010-2011 Stanford University
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
#include "MockDriver.h"
#include "MockTransport.h"
#include "FastTransport.h"
#include "ServiceManager.h"
#include "UdpDriver.h"

namespace RAMCloud {

class MockReceived : public Driver::Received {
  private:
    void construct(uint32_t fragNumber,
                   uint32_t totalFrags,
                   const char* msg,
                   uint32_t len)
    {
        payload = new char[len];
        memcpy(getContents(), msg, len - sizeof(FastTransport::Header));
        FastTransport::Header *header = new(payload) FastTransport::Header;
        header->fragNumber = downCast<uint16_t>(fragNumber);
        header->totalFrags = downCast<uint16_t>(totalFrags);
    }
  public:
    MockReceived(uint32_t fragNumber,
                 uint32_t totalFrags,
                 const void* msg,
                 uint32_t len)
        : Received()
        , stealCount(0)
    {
        this->len = len + downCast<uint32_t>(sizeof(FastTransport::Header));
        construct(fragNumber, totalFrags,
                  static_cast<const char*>(msg), this->len);
        getHeader()->payloadType = FastTransport::Header::ACK;
    }
    MockReceived(uint32_t fragNumber,
                 uint32_t totalFrags,
                 const char* msg)
        : Received()
        , stealCount(0)
    {
        len = downCast<uint32_t>(strlen(msg) + sizeof(FastTransport::Header));
        construct(fragNumber, totalFrags, msg, len);
        getHeader()->payloadType = FastTransport::Header::DATA;
    }
    FastTransport::Header* getHeader()
    {
        return reinterpret_cast<FastTransport::Header*>(payload);
    }
    char* getContents()
    {
        return payload + sizeof(FastTransport::Header);
    }
    VIRTUAL_FOR_TESTING char *steal(uint32_t *len)
    {
        stealCount++;
        *len = this->len;
        return payload;
    }
    ~MockReceived()
    {
        delete[] payload;
        payload = NULL;
    }
    int stealCount;
    DISALLOW_COPY_AND_ASSIGN(MockReceived);
};

// --- FastTransportTest ---

class FastTransportTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(FastTransportTest);
    CPPUNIT_TEST(test_sanityCheck);
    CPPUNIT_TEST(test_getSession_noneExpirable);
    CPPUNIT_TEST(test_getSession_reuseExpired);
    CPPUNIT_TEST(test_numFrags_fullPacket);
    CPPUNIT_TEST(test_numFrags_oneByteTooBig);
    CPPUNIT_TEST(test_sendBadSessionError);
    CPPUNIT_TEST(test_handleIncomingPacket_tooSmall);
    CPPUNIT_TEST(test_handleIncomingPacket_dropped);
    CPPUNIT_TEST(test_handleIncomingPacket_c2sBadHintOpenSession);
    CPPUNIT_TEST(test_handleIncomingPacket_c2sBadSession);
    CPPUNIT_TEST(test_handleIncomingPacket_c2sGoodHint);
    CPPUNIT_TEST(test_handleIncomingPacket_c2sGoodHintBadToken);
    CPPUNIT_TEST(test_handleIncomingPacket_s2cGoodHint);
    CPPUNIT_TEST(test_handleIncomingPacket_s2cGoodHintBadToken);
    CPPUNIT_TEST(test_handleIncomingPacket_s2cBadHint);
    CPPUNIT_TEST_SUITE_END();

  public:

    void
    setUp()
    {
        driver = new MockDriver(FastTransport::Header::headerToString);
        transport = new FastTransport(driver);
        delete serviceManager;
        ServiceManager::init();

        logger.setLogLevels(SILENT_LOG_LEVEL);

        // The following is necessary in case some other tests messed up
        // currentTime (e.g. by resetting it to 0).
        dispatch->currentTime = rdtsc();
    }

    void
    tearDown()
    {
        delete transport;
        FastTransport::timeoutCyclesOverride = 0;
        FastTransport::sessionTimeoutCyclesOverride = 0;
    }

    FastTransportTest()
        : serviceLocator("fast+udp: host=1.2.3.4, port=1234")
        , transport(NULL)
        , driver(NULL)
        , address("1.2.3.4")
        , port(1234)
    {}

    void test_sanityCheck() {
#if !VALGRIND // RAM-260
        // Create a server and a client and verify that we can
        // send a request, receive it, send a reply, and receive it.
        // Then try a second request with bigger chunks of data.

        ServiceLocator serverLocator("fast+udp: host=localhost, port=11101");
        UdpDriver* serverDriver = new UdpDriver(&serverLocator);
        FastTransport server(serverDriver);
        UdpDriver* clientDriver = new UdpDriver();
        FastTransport client(clientDriver);
        Transport::SessionRef session = client.getSession(serverLocator);

        Buffer request;
        Buffer reply;
        request.fillFromString("abcdefg");
        Transport::ClientRpc* clientRpc = session->clientSend(&request,
                &reply);
        Transport::ServerRpc* serverRpc = serviceManager->waitForRpc(1.0);
        CPPUNIT_ASSERT(serverRpc != NULL);
        CPPUNIT_ASSERT_EQUAL("abcdefg/0", toString(&serverRpc->requestPayload));
        CPPUNIT_ASSERT_EQUAL(false, clientRpc->isReady());
        serverRpc->replyPayload.fillFromString("klmn");
        serverRpc->sendReply();
        CPPUNIT_ASSERT_EQUAL(true, waitForRpc(*clientRpc));
        CPPUNIT_ASSERT_EQUAL("klmn/0", toString(&reply));

        fillLargeBuffer(&request, 100000);
        reply.reset();
        clientRpc = session->clientSend(&request, &reply);
        serverRpc = serviceManager->waitForRpc(1.0);
        CPPUNIT_ASSERT(serverRpc != NULL);
        CPPUNIT_ASSERT_EQUAL("ok",
                checkLargeBuffer(&serverRpc->requestPayload, 100000));
        fillLargeBuffer(&serverRpc->replyPayload, 50000);
        serverRpc->sendReply();
        clientRpc->wait();
        CPPUNIT_ASSERT_EQUAL("ok", checkLargeBuffer(&reply, 50000));
#endif
    }

    void
    test_getSession_noneExpirable()
    {
        CPPUNIT_ASSERT_EQUAL(0, transport->clientSessions.size());
        FastTransport::SessionRef session =
                transport->getSession(serviceLocator);
        CPPUNIT_ASSERT(0 != session.get());
        CPPUNIT_ASSERT_EQUAL(1, transport->clientSessions.size());
        FastTransport::ClientSession* clientSession =
            static_cast<FastTransport::ClientSession*>(session.get());
        CPPUNIT_ASSERT(0 != clientSession->serverAddress.get());
    }

    void
    test_getSession_reuseExpired()
    {
        dispatch->currentTime = 0;
        CPPUNIT_ASSERT_EQUAL(0, transport->clientSessions.size());
        Transport::Session* firstSession =
            transport->getSession(serviceLocator).get();
        FastTransport::sessionTimeoutCyclesOverride =
                FastTransport::SESSION_TIMEOUT_NS;
        dispatch->currentTime = FastTransport::SESSION_TIMEOUT_NS;
        Transport::Session* lastSession =
            transport->getSession(serviceLocator).get();
        CPPUNIT_ASSERT_EQUAL(firstSession, lastSession);
        CPPUNIT_ASSERT_EQUAL(1, transport->clientSessions.size());
    }

    // Used in {add,remove,fire}Timer tests
    struct MockTimer : public Dispatch::Timer {
        MockTimer()
            : Dispatch::Timer()
            , onTimerFiredCount(0)
            , expectWhen(0)
        {}
        explicit MockTimer(uint64_t expectWhen)
            : Dispatch::Timer()
            , onTimerFiredCount(0)
            , expectWhen(expectWhen)
        {}
        virtual void operator() ()
        {
            if (expectWhen)
                CPPUNIT_ASSERT_EQUAL(expectWhen, dispatch->currentTime);
            onTimerFiredCount++;
        }
        uint32_t onTimerFiredCount;
        uint64_t expectWhen;
    };

    void
    test_numFrags_fullPacket()
    {
        Buffer b;
        new(&b, APPEND) char[transport->dataPerFragment()];
        CPPUNIT_ASSERT_EQUAL(1, transport->numFrags(&b));
    }

    void
    test_numFrags_oneByteTooBig()
    {
        Buffer b;
        new(&b, APPEND) char[transport->dataPerFragment() + 1];
        CPPUNIT_ASSERT_EQUAL(2, transport->numFrags(&b));
    }

    void
    test_sendBadSessionError()
    {
        MockReceived recvd(0, 1, "");
        FastTransport::Header *header = recvd.getHeader();
        header->sessionToken = 0xabcd;
        header->rpcId = 3;
        header->clientSessionHint = 4;
        header->serverSessionHint = 5;
        header->channelId = 6;
        header->payloadType = 7;
        header->direction = 1;
        transport->sendBadSessionError(recvd.getHeader(),
                                       recvd.sender);
        CPPUNIT_ASSERT_EQUAL(
            "{ sessionToken:abcd rpcId:3 clientSessionHint:4 "
            "serverSessionHint:5 0/0 frags channel:6 dir:1 reqACK:0 "
            "drop:0 payloadType:4 } ", driver->outputLog);
    }

    /// A predicate to limit TestLog messages to invoke
    static bool
    tppPred(string s)
    {
        return s == "handleIncomingPacket";
    }

    void
    test_handleIncomingPacket_tooSmall()
    {
        TestLog::Enable _(&tppPred);

        MockReceived recvd(0, 1, "");
        // corrupt the size
        recvd.len = 1;

        transport->handleIncomingPacket(&recvd);
        CPPUNIT_ASSERT_EQUAL(
            "handleIncomingPacket: "
            "packet too short (1 bytes)", TestLog::get());
    }

    void
    test_handleIncomingPacket_dropped()
    {
        TestLog::Enable _(&tppPred);

        MockReceived recvd(0, 1, "");
        recvd.getHeader()->pleaseDrop = 1;

        transport->handleIncomingPacket(&recvd);
        CPPUNIT_ASSERT_EQUAL(
            "handleIncomingPacket: "
            "dropped", TestLog::get());
    }

    void
    test_handleIncomingPacket_c2sBadHintOpenSession()
    {
        TestLog::Enable _(&tppPred);

        FastTransport::sessionTimeoutCyclesOverride = 2000;
        dispatch->currentTime = FastTransport::sessionTimeoutCycles() * 2;
        FastTransport::SessionOpenResponse sessResp =
                { FastTransport::NUM_CHANNELS_PER_SESSION };
        MockReceived recvd(0, 1, &sessResp, sizeof(sessResp));
        recvd.getHeader()->serverSessionHint =
                FastTransport::ClientSession::INVALID_HINT;
        recvd.getHeader()->payloadType = FastTransport::Header::SESSION_OPEN;
        ServiceLocator sl("mock:");
        recvd.sender = driver->newAddress(sl);

        transport->handleIncomingPacket(&recvd);
        CPPUNIT_ASSERT_EQUAL(
            "handleIncomingPacket: "
            "opening session 0", TestLog::get());
        CPPUNIT_ASSERT_EQUAL(1, transport->serverSessions.size());
    }

    void
    test_handleIncomingPacket_c2sBadSession()
    {
        TestLog::Enable _(&tppPred);

        MockReceived recvd(0, 1, "");

        transport->handleIncomingPacket(&recvd);
        CPPUNIT_ASSERT_EQUAL(
            "handleIncomingPacket: "
            "bad session hint 0", TestLog::get());
        CPPUNIT_ASSERT_EQUAL(
            "{ sessionToken:0 rpcId:0 clientSessionHint:0 "
              "serverSessionHint:0 0/0 frags channel:0 dir:1 reqACK:0 "
              "drop:0 payloadType:4 } ", driver->outputLog);
    }

    void
    test_handleIncomingPacket_c2sGoodHint()
    {
        TestLog::Enable _(&tppPred);

        FastTransport::ServerSession* session =
                transport->serverSessions.get();
        MockReceived recvd(0, 1, "");
        recvd.getHeader()->sessionToken = session->token;

        transport->handleIncomingPacket(&recvd);
        session->channels[0].state =
                FastTransport::ServerSession::ServerChannel::IDLE;
        CPPUNIT_ASSERT_EQUAL(
            "handleIncomingPacket: "
            "calling ServerSession::processInboundPacket", TestLog::get());
    }

    void
    test_handleIncomingPacket_c2sGoodHintBadToken()
    {
        TestLog::Enable _(&tppPred);

        FastTransport::ServerSession* session =
                transport->serverSessions.get();
        MockReceived recvd(0, 1, "");
        recvd.getHeader()->sessionToken = session->token+1;

        transport->handleIncomingPacket(&recvd);
        CPPUNIT_ASSERT_EQUAL(
            "handleIncomingPacket: "
            "bad session token (0xcccccccccccccccc in session 0, "
            "0xcccccccccccccccd in packet)", TestLog::get());
        CPPUNIT_ASSERT_EQUAL(
            "{ sessionToken:cccccccccccccccd rpcId:0 clientSessionHint:0 "
              "serverSessionHint:0 0/0 frags channel:0 dir:1 reqACK:0 "
              "drop:0 payloadType:4 } ", driver->outputLog);
    }

    void
    test_handleIncomingPacket_s2cGoodHint()
    {
        TestLog::Enable _(&tppPred);

        FastTransport::SessionRef session =
                transport->getSession(serviceLocator);
        FastTransport::ClientSession* clientSession =
            static_cast<FastTransport::ClientSession*>(session.get());

        MockReceived recvd(0, 1, "");
        recvd.getHeader()->direction = FastTransport::Header::SERVER_TO_CLIENT;
        clientSession->token = recvd.getHeader()->sessionToken;

        transport->handleIncomingPacket(&recvd);
        CPPUNIT_ASSERT_EQUAL(
            "handleIncomingPacket: "
            "client session processing packet", TestLog::get());
    }

    void
    test_handleIncomingPacket_s2cGoodHintBadToken()
    {
        TestLog::Enable _(&tppPred);

        transport->getSession(serviceLocator);

        MockReceived recvd(0, 1, "");
        recvd.getHeader()->direction =
                FastTransport::Header::SERVER_TO_CLIENT;

        transport->handleIncomingPacket(&recvd);
        CPPUNIT_ASSERT_EQUAL(
            "handleIncomingPacket: "
            "client session processing packet | "
            "handleIncomingPacket: "
            "bad fragment token (0xcccccccccccccccc in session 0, "
            "0x0 in packet), client dropping", TestLog::get());
    }

    void
    test_handleIncomingPacket_s2cBadHint()
    {
        TestLog::Enable _(&tppPred);

        MockReceived recvd(0, 1, "");
        recvd.getHeader()->direction =
                FastTransport::Header::SERVER_TO_CLIENT;

        transport->handleIncomingPacket(&recvd);
        CPPUNIT_ASSERT_EQUAL(
            "handleIncomingPacket: "
            "bad client session hint 0", TestLog::get());
    }

  private:
    ServiceLocator serviceLocator;
    FastTransport* transport;
    MockDriver* driver;
    const char* address;
    uint16_t port;

    DISALLOW_COPY_AND_ASSIGN(FastTransportTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(FastTransportTest);

// --- ClientRpcTest ---

class ClientRpcTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(ClientRpcTest);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST_SUITE_END();

  public:
    ClientRpcTest()
        : request(NULL)
        , response(NULL)
        , transport(NULL)
        , driver(NULL)
        , rpc(NULL)
        , address("1.2.3.4")
        , port(1234)
    {}

    void
    setUp()
    {
        tearDown();

        driver = new MockDriver(FastTransport::Header::headerToString);
        transport = new FastTransport(driver);

        request = new Buffer();
        response = new Buffer();

        rpc = new FastTransport::ClientRpc(transport, request, response);
    }

    void
    tearDown()
    {
        if (response)
            delete response;
        if (request)
            delete request;
        if (transport)
            delete transport;
    }

    void
    test_constructor()
    {
        CPPUNIT_ASSERT_EQUAL(request, rpc->requestBuffer);
        CPPUNIT_ASSERT_EQUAL(response, rpc->responseBuffer);
        CPPUNIT_ASSERT_EQUAL(transport, rpc->transport);
    }

  private:
    Buffer* request;
    Buffer* response;
    FastTransport* transport;
    MockDriver* driver;
    FastTransport::ClientRpc* rpc;
    const char* address;
    uint16_t port;

    DISALLOW_COPY_AND_ASSIGN(ClientRpcTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(ClientRpcTest);

// --- InboundMessageTest ---

class InboundMessageTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(InboundMessageTest);
    CPPUNIT_TEST(test_sendAck);
    CPPUNIT_TEST(test_reset);
    CPPUNIT_TEST(test_init);
    CPPUNIT_TEST(test_setup);
    CPPUNIT_TEST(test_processReceivedData_totalFragMismatch);
    CPPUNIT_TEST(test_processReceivedData_addFirstMissing);
    CPPUNIT_TEST(test_processReceivedData_addRunFromWindow);
    CPPUNIT_TEST(test_processReceivedData_fragmentBeyondWindow);
    CPPUNIT_TEST(test_processReceivedData_addToWindow);
    CPPUNIT_TEST(test_processReceivedData_duplicateFragment);
    CPPUNIT_TEST(test_processReceivedData_sendAckCalled);
    CPPUNIT_TEST(test_processReceivedData_timerAdded);
    CPPUNIT_TEST_SUITE_END();

  public:

    void dataStagingWindowToWindow(
                Window<pair<char*, uint32_t>,
                FastTransport::MAX_STAGING_FRAGMENTS>& w,
                string& s)
    {
        size_t max = 50;
        char tmp[max];

        for (uint32_t i = 0; i < w.getLength(); i++) {
            char* payload = w[w.getOffset() + i].first;
            uint32_t payloadLen = w[w.getOffset() + i].second;

            if (!payload) {
                snprintf(tmp, max, "-, ");
            } else {
                string payloadStr = toString(payload, payloadLen);
                payloadStr.resize(10);

                snprintf(tmp, max, "(%s, %u), ",
                         payloadStr.c_str(), payloadLen);
            }

            s.append(tmp);
        }

        uint32_t trim = 0;
        while (s[s.length() - trim - 1] == ' ')
            trim++;

        s.resize(s.length() - trim);
    }

    InboundMessageTest()
        : driver(NULL)
        , transport(NULL)
        , session()
        , buffer(NULL)
        , msg(NULL)
    {}

    void
    setUp()
    {
        setUp(2, false);
    }
    void
    setUp(uint16_t totalFrags, bool useTimer = false)
    {
        tearDown();

        driver = new MockDriver();
        transport = new FastTransport(driver);
        buffer = new Buffer();

        uint32_t channelId = 5;

        msg = new FastTransport::InboundMessage();

        ServiceLocator serviceLocator("fast+udp: host=1.2.3.4, port=1234");
        session = transport->getSession(serviceLocator);

        FastTransport::ClientSession* clientSession =
            static_cast<FastTransport::ClientSession*>(session.get());
        clientSession->numChannels =
                FastTransport::MAX_NUM_CHANNELS_PER_SESSION;
        clientSession->allocateChannels();
        msg->setup(transport, clientSession, channelId, useTimer);

        msg->reset();
        msg->init(totalFrags, buffer);

        // Initialize dataStagingWindow to check invariants after calls
        for (uint32_t i = 1; i <= msg->dataStagingWindow.getLength(); i++)
            msg->dataStagingWindow[i] =
                std::pair<char*, uint32_t>(static_cast<char*>(0), i);

    }

    void tearDown()
    {
        if (msg)
            delete msg;
        if (buffer)
            delete buffer;
        session = NULL;
        if (transport)
            delete transport;
    }

    void
    test_sendAck()
    {
        // no packets received yet
        msg->sendAck();
        CPPUNIT_ASSERT_EQUAL("0 /0 /0",
                             driver->outputLog);
        driver->outputLog = "";

        // first ten received with no drops
        msg->firstMissingFrag = 10;
        msg->dataStagingWindow.advance(10);
        msg->sendAck();
        CPPUNIT_ASSERT_EQUAL("10 /0 /0", driver->outputLog);
        driver->outputLog = "";

        // first twenty received, missing 21 and one at other end of window
        msg->firstMissingFrag = 20;
        msg->dataStagingWindow.advance(10);
        char junk[0];
        msg->dataStagingWindow[21] = std::pair<char*, uint32_t>(junk, 1);
        msg->dataStagingWindow[msg->dataStagingWindow.getLength() + 20] =
            std::pair<char*, uint32_t>(junk, 1);
        msg->sendAck();
        CPPUNIT_ASSERT_EQUAL("0x10014 /0 /x80", driver->outputLog);
        driver->outputLog = "";
    }
    void
    test_init()
    {
        setUp(2, true);

        Buffer buffer;
        msg->init(999, &buffer);

        CPPUNIT_ASSERT_EQUAL(999, msg->totalFrags);
        CPPUNIT_ASSERT_EQUAL(&buffer, msg->dataBuffer);
    }

    void
    test_setup()
    {
        bool useTimer = true;
        uint32_t channelId = 5;
        setUp(2, useTimer);

        msg->timer.startCycles(999);

        FastTransport::ClientSession* clientSession =
            static_cast<FastTransport::ClientSession*>(session.get());
        for (;;) {
            msg->setup(transport, clientSession, channelId, useTimer);
            CPPUNIT_ASSERT_EQUAL(clientSession, msg->session);
            CPPUNIT_ASSERT_EQUAL(transport, msg->transport);
            CPPUNIT_ASSERT_EQUAL(channelId, msg->channelId);
            CPPUNIT_ASSERT_EQUAL(useTimer, msg->useTimer);
            if (!useTimer)
                break;
            useTimer = !useTimer;
            channelId = 6;
            setUp(2, useTimer);
        }
    }

    void
    test_reset()
    {
        setUp(2, true);

        char junk[0];
        msg->timer.startCycles(1000);
        msg->dataStagingWindow[11] = std::pair<char*, uint32_t>(junk, 1);
        msg->dataStagingWindow[14] = std::pair<char*, uint32_t>(junk, 1);

        msg->reset();

        CPPUNIT_ASSERT_EQUAL(0, msg->totalFrags);
        CPPUNIT_ASSERT_EQUAL(0, msg->firstMissingFrag);
        string s;
        dataStagingWindowToWindow(msg->dataStagingWindow, s);
        CPPUNIT_ASSERT_EQUAL("-, -, -, -, -, -, -, -, -, -, -, -, -, -, "
                             "-, -, -, -, -, -, -, -, -, -, -, -, -, -, "
                             "-, -, -, -,", s);
        CPPUNIT_ASSERT_EQUAL(0, msg->dataBuffer);
        CPPUNIT_ASSERT_EQUAL(2, driver->releaseCount);
    }

    void
    test_processReceivedData_totalFragMismatch()
    {
        TestLog::Enable _;

        // NOTE Make sure to keep the MockReceiveds on the stack until
        // none of the data is in use as part of a buffer
        MockReceived recvd(0, msg->totalFrags + 1, "God hates ponies.");
        bool result = msg->processReceivedData(&recvd);
        CPPUNIT_ASSERT_EQUAL(false, result);
        CPPUNIT_ASSERT_EQUAL("", bufferToDebugString(msg->dataBuffer));
        CPPUNIT_ASSERT_EQUAL(0, msg->firstMissingFrag);
        CPPUNIT_ASSERT_EQUAL(0, recvd.stealCount);
        CPPUNIT_ASSERT_EQUAL(
            "processReceivedData: "
            "header->totalFrags (3) != totalFrags (2)", TestLog::get());
    }

    /**
     * Ensures that if packets arrive in order according to firstMissingFrag
     * everything works, including connection termination condition.
     */
    void
    test_processReceivedData_addFirstMissing()
    {
        // first recvd packet - never placed in window
        MockReceived recvd(0, msg->totalFrags, "God hates ponies.");
        bool result = msg->processReceivedData(&recvd);
        CPPUNIT_ASSERT_EQUAL(false, result);
        CPPUNIT_ASSERT_EQUAL("God hates ponies.",
                             bufferToDebugString(msg->dataBuffer));
        CPPUNIT_ASSERT_EQUAL(1, msg->firstMissingFrag);
        CPPUNIT_ASSERT_EQUAL(2, msg->dataStagingWindow[2].second);
        CPPUNIT_ASSERT_EQUAL(1, recvd.stealCount);

        // second recvd packet - never placed in window - msg complete
        MockReceived nextRecvd(1, msg->totalFrags, "I hate ponies, also.");
        result = msg->processReceivedData(&nextRecvd);
        CPPUNIT_ASSERT_EQUAL(true, result);
        CPPUNIT_ASSERT_EQUAL("God hates ponies. | I hate ponies, also.",
                             bufferToDebugString(msg->dataBuffer));
        CPPUNIT_ASSERT_EQUAL(msg->totalFrags, msg->firstMissingFrag);
        CPPUNIT_ASSERT_EQUAL(3, msg->dataStagingWindow[
                                msg->totalFrags+1].second);
        CPPUNIT_ASSERT_EQUAL(1, nextRecvd.stealCount);
    }

    /**
     * Ensures out-of-order packets transition from the window to the buffer
     * when the missing fragments before them are encountered.
     */
    void
    test_processReceivedData_addRunFromWindow()
    {
        // first recvd packet - out of order - enters window
        MockReceived recvd(1, msg->totalFrags, "I hate ponies, also.");
        bool result = msg->processReceivedData(&recvd);
        CPPUNIT_ASSERT_EQUAL(false, result);
        CPPUNIT_ASSERT_EQUAL("", bufferToDebugString(msg->dataBuffer));
        CPPUNIT_ASSERT_EQUAL(recvd.payload, msg->dataStagingWindow[1].first);
        CPPUNIT_ASSERT_EQUAL(recvd.len, msg->dataStagingWindow[1].second);

        // second recvd packet - completes connection
        MockReceived nextRecvd(0, msg->totalFrags, "God hates ponies.");
        result = msg->processReceivedData(&nextRecvd);
        CPPUNIT_ASSERT_EQUAL(true, result);
        CPPUNIT_ASSERT_EQUAL("God hates ponies. | I hate ponies, also.",
                             bufferToDebugString(msg->dataBuffer));
        CPPUNIT_ASSERT_EQUAL(3, msg->dataStagingWindow[3].second);
    }

    void
    test_processReceivedData_fragmentBeyondWindow()
    {
        // test with longer connection
        uint16_t totalFrags = 100;
        setUp(totalFrags, false);
        TestLog::Enable _;

        // first recvd packet - out of order - ensure in window in right place
        MockReceived recvd(33, totalFrags, "packet");
        bool result = msg->processReceivedData(&recvd);
        CPPUNIT_ASSERT_EQUAL(false, result);
        CPPUNIT_ASSERT_EQUAL("", bufferToDebugString(msg->dataBuffer));
        CPPUNIT_ASSERT_EQUAL(0, recvd.stealCount);
        CPPUNIT_ASSERT_EQUAL(
            "processReceivedData: "
            "fragNumber 33 out of range (last OK = 32)", TestLog::get());
    }

    /**
     * Ensures that if an out-of-order packet is encountered it is properly
     * stored in the staging window to be moved in to the result buffer later.
     */
    void
    test_processReceivedData_addToWindow()
    {
        // test with longer connection
        uint16_t totalFrags = 100;
        setUp(totalFrags, false);

        // first recvd packet - out of order - ensure in window in right place
        MockReceived recvd(2, totalFrags, "pkt two");
        bool result = msg->processReceivedData(&recvd);
        CPPUNIT_ASSERT_EQUAL(false, result);
        CPPUNIT_ASSERT_EQUAL("", bufferToDebugString(msg->dataBuffer));
        CPPUNIT_ASSERT_EQUAL(recvd.payload, msg->dataStagingWindow[2].first);
        CPPUNIT_ASSERT_EQUAL(recvd.len, msg->dataStagingWindow[2].second);
        CPPUNIT_ASSERT_EQUAL(1, recvd.stealCount);

        // 2nd recvd packet - out of order - ensure in window in right place
        MockReceived nextRecvd(1, totalFrags, "pkt one");
        result = msg->processReceivedData(&nextRecvd);
        CPPUNIT_ASSERT_EQUAL(false, result);
        CPPUNIT_ASSERT_EQUAL("", bufferToDebugString(msg->dataBuffer));
        CPPUNIT_ASSERT_EQUAL(nextRecvd.payload,
                             msg->dataStagingWindow[1].first);
        CPPUNIT_ASSERT_EQUAL(nextRecvd.len, msg->dataStagingWindow[1].second);
        CPPUNIT_ASSERT_EQUAL(1, nextRecvd.stealCount);
    }

    void
    test_processReceivedData_duplicateFragment()
    {
        // test with longer connection
        uint16_t totalFrags = 100;
        setUp(totalFrags, false);
        TestLog::Enable _;

        // first recvd packet - out of order
        MockReceived recvd(2, totalFrags, "first is short");
        bool result = msg->processReceivedData(&recvd);

        // 2nd recvd packet - same fragment
        MockReceived nextRecvd(2, totalFrags, "second is longer");
        result = msg->processReceivedData(&nextRecvd);
        CPPUNIT_ASSERT_EQUAL(false, result);
        CPPUNIT_ASSERT_EQUAL("", bufferToDebugString(msg->dataBuffer));
        CPPUNIT_ASSERT_EQUAL(recvd.len, msg->dataStagingWindow[2].second);
        CPPUNIT_ASSERT_EQUAL(0, nextRecvd.stealCount);
        CPPUNIT_ASSERT_EQUAL(
            "processReceivedData: "
            "duplicate fragment 2 received", TestLog::get());
    }

    void
    test_processReceivedData_sendAckCalled()
    {
        MockReceived recvd(0, msg->totalFrags, "pkt zero");
        recvd.getHeader()->requestAck = 1;
        msg->processReceivedData(&recvd);

        CPPUNIT_ASSERT("" != driver->outputLog);
    }

    void
    test_processReceivedData_timerAdded()
    {
        setUp(2, true);

        msg->timer.stop();
        MockReceived recvd(0, msg->totalFrags, "pkt zero");
        recvd.getHeader()->requestAck = 1;
        msg->processReceivedData(&recvd);
    }

  private:
    MockDriver* driver;
    FastTransport* transport;
    FastTransport::SessionRef session;
    Buffer* buffer;
    FastTransport::InboundMessage* msg;

    DISALLOW_COPY_AND_ASSIGN(InboundMessageTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(InboundMessageTest);

// --- OutboundMessageTest ---

class OutboundMessageTest: public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(OutboundMessageTest);
    CPPUNIT_TEST(test_reset);
    CPPUNIT_TEST(test_setup);
    CPPUNIT_TEST(test_beginSending);
    CPPUNIT_TEST(test_send);
    CPPUNIT_TEST(test_send_nothingToSend);
    CPPUNIT_TEST(test_send_dueToTimeout);
    CPPUNIT_TEST(test_send_ackAfter);
    CPPUNIT_TEST(test_send_dontAckLast);
    CPPUNIT_TEST(test_send_timers);
    CPPUNIT_TEST(test_processReceivedAck_noSendBuffer);
    CPPUNIT_TEST(test_processReceivedAck_packetTooShort);
    CPPUNIT_TEST(test_processReceivedAck_ackPastMessageEnd);
    CPPUNIT_TEST(test_processReceivedAck_ackPastWindowEnd);
    CPPUNIT_TEST(test_processReceivedAck_noneMissing);
    CPPUNIT_TEST(test_processReceivedAck_oneMissing);
    CPPUNIT_TEST(test_sendOneData_noRequestAck);
    CPPUNIT_TEST(test_sendOneData_requestAck);
    CPPUNIT_TEST_SUITE_END();

  public:
    static void sentTimesWindowToString(
                Window<uint64_t, FastTransport::MAX_STAGING_FRAGMENTS + 1>& w,
                string& s)
    {
        size_t max = 50;
        char tmp[max];

        for (uint32_t i = 0; i < w.getLength(); i++) {
            uint64_t val = w[w.getOffset() + i];
            if (val == FastTransport::OutboundMessage::ACKED)
                snprintf(tmp, max, "ACKED, ");
            else
                snprintf(tmp, max, "%lu, ", val);
            s.append(tmp);
        }

        uint32_t trim = 0;
        while (s[s.length() - trim - 1] == ' ')
            trim++;

        s.resize(s.length() - trim);
    }

    OutboundMessageTest()
        : driver(NULL)
        , transport(NULL)
        , session()
        , buffer(NULL)
        , msg(NULL)
        , tsc(999 + 2 * FastTransport::TIMEOUT_NS)
    {
    }

    void
    setUp()
    {
        setUp(1600, false);
    }

    void
    setUp(uint32_t messageLen, bool useTimer = false)
    {
        assert(!(messageLen % 10));

        tearDown();

        driver = new MockDriver(FastTransport::Header::headerToString);
        transport = new FastTransport(driver);
        buffer = new Buffer();

        const char* testMsg = "abcdefghij";
        size_t testMsgLen = strlen(testMsg);
        char* payload = new(buffer, APPEND) char[testMsgLen *
                                                 (messageLen / 10) + 1];
        for (uint32_t i = 0; i < (messageLen / 10); i++)
            memcpy(payload + i * testMsgLen, testMsg, testMsgLen);
        payload[testMsgLen * (messageLen / 10)] = '\0';

        uint32_t channelId = 5;

        ServiceLocator serviceLocator("fast+udp: host=1.2.3.4, port=1234");
        session = transport->getSession(serviceLocator);
        FastTransport::ClientSession* clientSession =
            static_cast<FastTransport::ClientSession*>(session.get());
        clientSession->numChannels =
                FastTransport::MAX_NUM_CHANNELS_PER_SESSION;
        clientSession->allocateChannels();

        msg = new FastTransport::OutboundMessage();
        msg->setup(transport, clientSession, channelId, useTimer);

        msg->reset();

        // same as a call to beginSending without the implicit call to send()
        msg->sendBuffer = buffer;
        msg->totalFrags = transport->numFrags(buffer);

        mockTSCValue = tsc;
        dispatch->currentTime = tsc;
    }

    void tearDown()
    {
        delete msg;
        session = NULL;
        if (buffer)
            delete buffer;
        if (transport)
            delete transport;
        mockTSCValue = 0;
    }

    /**
     * Simple check, but also checks subtler details regarding timers.
     * If the message was using the timer reset must remove it.
     */
    void
    test_reset()
    {
        setUp(1600, true);
        msg->timer.startCycles(999);

        msg->reset();

        CPPUNIT_ASSERT_EQUAL(0, msg->sendBuffer);
        CPPUNIT_ASSERT_EQUAL(0, msg->firstMissingFrag);
        CPPUNIT_ASSERT_EQUAL(0, msg->packetsSinceAckReq);
        string s;
        sentTimesWindowToString(msg->sentTimes, s);
        CPPUNIT_ASSERT_EQUAL("0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "
                             "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "
                             "0, 0, 0,", s);
        CPPUNIT_ASSERT_EQUAL(0, msg->numAcked);
    }

    void
    test_setup()
    {
        uint32_t channelId = 999;
        bool useTimer = false;

        FastTransport::ClientSession* clientSession =
            static_cast<FastTransport::ClientSession*>(session.get());
        msg->setup(transport, clientSession, channelId, useTimer);

        CPPUNIT_ASSERT_EQUAL(clientSession, msg->session);
        CPPUNIT_ASSERT_EQUAL(transport, msg->transport);
        CPPUNIT_ASSERT_EQUAL(channelId, msg->channelId);
        CPPUNIT_ASSERT_EQUAL(useTimer, msg->useTimer);
    }

    void
    test_beginSending()
    {
        msg->totalFrags = 0;
        msg->sendBuffer = 0;
        msg->beginSending(buffer);
        CPPUNIT_ASSERT_EQUAL(buffer, msg->sendBuffer);
        CPPUNIT_ASSERT(0 != msg->totalFrags);
    }

    void
    test_send()
    {
        msg->send();
        CPPUNIT_ASSERT_EQUAL(
            "{ sessionToken:cccccccccccccccc rpcId:0 clientSessionHint:0 "
            "serverSessionHint:cccccccc 0/2 frags channel:5 dir:0 reqACK:0 "
            "drop:0 payloadType:0 } abcdefghij (+1364 more) | "
            "{ sessionToken:cccccccccccccccc rpcId:0 clientSessionHint:0 "
            "serverSessionHint:cccccccc 1/2 frags channel:5 dir:0 reqACK:0 "
            "drop:0 payloadType:0 } efghijabcd (+217 more)",
            driver->outputLog);
        CPPUNIT_ASSERT_EQUAL(tsc, msg->sentTimes[0]);
        CPPUNIT_ASSERT_EQUAL(tsc, msg->sentTimes[1]);
    }

    void
    test_send_nothingToSend()
    {
        msg->sentTimes[0] = tsc - FastTransport::timeoutCycles();
        msg->sentTimes[1] = FastTransport::OutboundMessage::ACKED;
        msg->numAcked = 1;

        msg->send();
        CPPUNIT_ASSERT_EQUAL("", driver->outputLog);
    }

    void
    test_send_dueToTimeout()
    {
        // this will get resent due to timeout
        msg->sentTimes[0] = tsc - FastTransport::timeoutCycles() - 1;
        // note that though this is ready to send it will not go out
        // because the protocol out sends out a single packet when
        // a retransmit occurs
        msg->sentTimes[1] = 0;

        msg->send();
        CPPUNIT_ASSERT_EQUAL(
            "{ sessionToken:cccccccccccccccc rpcId:0 clientSessionHint:0 "
            "serverSessionHint:cccccccc 0/2 frags channel:5 dir:0 reqACK:1 "
            "drop:0 payloadType:0 } abcdefghij (+1364 more)",
            driver->outputLog);
        CPPUNIT_ASSERT_EQUAL(tsc, msg->sentTimes[0]);
        CPPUNIT_ASSERT_EQUAL(0, msg->sentTimes[1]);
    }

    void
    test_send_ackAfter()
    {
        setUp(driver->getMaxPacketSize() * 7, false);

        msg->send();
        string s = driver->outputLog;
        CPPUNIT_ASSERT_EQUAL("4/8 frags channel:5 dir:0 reqACK:1",
                             s.substr(s.find("4/8"), 34));
    }

    void
    test_send_dontAckLast()
    {
        setUp(driver->getMaxPacketSize() * 4, false);

        msg->send();
        string s = driver->outputLog;
        CPPUNIT_ASSERT_EQUAL("4/5 frags channel:5 dir:0 reqACK:0",
                             s.substr(s.find("4/5"), 34));
    }

    void
    test_send_timers()
    {
        setUp(driver->getMaxPacketSize() * 4, true);

        msg->sentTimes[0] = 100;
        msg->sentTimes[1] = FastTransport::OutboundMessage::ACKED;
        msg->sentTimes[2] = 99;
        msg->sentTimes[3] = 0;

        CPPUNIT_ASSERT_EQUAL(0, msg->timer.isRunning());
        msg->send();
        CPPUNIT_ASSERT_EQUAL(1, msg->timer.isRunning());
    }

    void
    test_processReceivedAck_noSendBuffer()
    {
        msg->sendBuffer = NULL;
        bool result = msg->processReceivedAck(0);
        CPPUNIT_ASSERT_EQUAL(false, result);
    }

    void
    test_processReceivedAck_packetTooShort()
    {
        TestLog::Enable _;
        msg->send();
        FastTransport::AckResponse ackResp(0, 0);
        MockReceived recvd(0, msg->totalFrags, &ackResp,
                           sizeof(FastTransport::AckResponse) - 1);
        bool result = msg->processReceivedAck(&recvd);
        CPPUNIT_ASSERT_EQUAL(false, result);
        CPPUNIT_ASSERT_EQUAL(
            "processReceivedAck: "
            "ACK packet too short (31 bytes)", TestLog::get());
    }

    void
    test_processReceivedAck_ackPastMessageEnd()
    {
        TestLog::Enable _;
        msg->send();
        FastTransport::AckResponse ackResp(0, 0);
        ackResp.firstMissingFrag = 3;
        MockReceived recvd(0, msg->totalFrags, &ackResp,
                           sizeof(FastTransport::AckResponse));
        bool result = msg->processReceivedAck(&recvd);
        CPPUNIT_ASSERT_EQUAL(false, result);
        CPPUNIT_ASSERT_EQUAL(
            "processReceivedAck: invalid ACK "
            "(firstMissingFrag 3 > totalFrags 2)", TestLog::get());
    }

    void
    test_processReceivedAck_ackPastWindowEnd()
    {
        TestLog::Enable _;
        msg->send();
        msg->totalFrags = 50;
        FastTransport::AckResponse ackResp(0, 0);
        ackResp.firstMissingFrag = 34;
        MockReceived recvd(0, msg->totalFrags, &ackResp,
                           sizeof(FastTransport::AckResponse));
        bool result = msg->processReceivedAck(&recvd);
        CPPUNIT_ASSERT_EQUAL(false, result);
        CPPUNIT_ASSERT_EQUAL(
            "processReceivedAck: "
            "invalid ACK (firstMissingFrag 34 beyond end of window 33)",
            TestLog::get());
    }

    /**
     * No packets missing; just ensure that bookkeeping slides up to
     * match the receiver side.
     */
    void
    test_processReceivedAck_noneMissing()
    {
        msg->send();
        string s;
        sentTimesWindowToString(msg->sentTimes, s);
        CPPUNIT_ASSERT_EQUAL("20000999, 20000999, 0, 0, 0, "
                             "0, 0, 0, 0, 0, 0, "
                             "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "
                             "0, 0, 0, 0, 0, 0,", s);
        s = "";

        FastTransport::AckResponse ackResp(0, 0);
        ackResp.firstMissingFrag = 2;
        MockReceived recvd(0, msg->totalFrags, &ackResp,
                           sizeof(FastTransport::AckResponse));

        bool result = msg->processReceivedAck(&recvd);

        CPPUNIT_ASSERT_EQUAL(true, result);
        sentTimesWindowToString(msg->sentTimes, s);
        CPPUNIT_ASSERT_EQUAL("0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "
                             "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "
                             "0, 0,", s);
        CPPUNIT_ASSERT_EQUAL(ackResp.firstMissingFrag, msg->numAcked);
    }

    /**
     * A packet is missing; eight packets beyond have been acked.
     * Excercises the bit vector.
     */
    void
    test_processReceivedAck_oneMissing()
    {
        msg->send();
        msg->totalFrags = 10;

        string s;
        sentTimesWindowToString(msg->sentTimes, s);
        CPPUNIT_ASSERT_EQUAL("20000999, 20000999, 0, 0, 0, "
                             "0, 0, 0, 0, 0, 0, "
                             "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "
                             "0, 0, 0, 0, 0, 0,", s);
        s = "";

        FastTransport::AckResponse ackResp(3, 0xff);
        MockReceived recvd(0, msg->totalFrags, &ackResp,
                           sizeof(FastTransport::AckResponse));

        bool result = msg->processReceivedAck(&recvd);

        CPPUNIT_ASSERT_EQUAL(false, result);
        s = "";
        sentTimesWindowToString(msg->sentTimes, s);
        CPPUNIT_ASSERT_EQUAL(
            "20000999, ACKED, ACKED, ACKED, ACKED, ACKED, "
            "ACKED, ACKED, ACKED, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "
            "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,",
            s);
        CPPUNIT_ASSERT_EQUAL(ackResp.firstMissingFrag + 8, msg->numAcked);
    }

    void
    test_sendOneData_noRequestAck()
    {
        msg->sendOneData(0, false);
        CPPUNIT_ASSERT_EQUAL(
            "{ sessionToken:cccccccccccccccc rpcId:0 clientSessionHint:0 "
            "serverSessionHint:cccccccc 0/2 frags channel:5 dir:0 reqACK:0 "
            "drop:0 payloadType:0 } abcdefghij (+1364 more)",
             driver->outputLog);
        CPPUNIT_ASSERT_EQUAL(1, msg->packetsSinceAckReq);
    }

    void
    test_sendOneData_requestAck()
    {
        msg->sendOneData(0, true);
        CPPUNIT_ASSERT_EQUAL(
            "{ sessionToken:cccccccccccccccc rpcId:0 clientSessionHint:0 "
            "serverSessionHint:cccccccc 0/2 frags channel:5 dir:0 reqACK:1 "
            "drop:0 payloadType:0 } abcdefghij (+1364 more)",
             driver->outputLog);
        CPPUNIT_ASSERT_EQUAL(0, msg->packetsSinceAckReq);
    }

  private:
    MockDriver* driver;
    FastTransport* transport;
    FastTransport::SessionRef session;
    Buffer* buffer;
    FastTransport::OutboundMessage* msg;
    uint64_t tsc;

    DISALLOW_COPY_AND_ASSIGN(OutboundMessageTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(OutboundMessageTest);

// --- ServerSessionTest ---

class ServerSessionTest: public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(ServerSessionTest);
    CPPUNIT_TEST(test_beginSending);
    CPPUNIT_TEST(test_expire_channelStillProcessing);
    CPPUNIT_TEST(test_expire_channelRecvOrSendWait);
    CPPUNIT_TEST(test_processInboundPacket_badChannel);
    CPPUNIT_TEST(test_processInboundPacket_currentRpcReceivedDataPacket);
    CPPUNIT_TEST(test_processInboundPacket_currentRpcReceivedAckPacket);
    CPPUNIT_TEST(test_processInboundPacket_currentRpcBadPayloadType);
    CPPUNIT_TEST(test_processInboundPacket_nextRpcReceivedDataPacket);
    CPPUNIT_TEST(test_processInboundPacket_newRpcBadPayloadType);
    CPPUNIT_TEST(test_startSession);
    CPPUNIT_TEST(test_processReceivedData_receiving);
    CPPUNIT_TEST(test_processReceivedData_processing);
    CPPUNIT_TEST_SUITE_END();

  public:
    ServerSessionTest()
        : driver(NULL)
        , transport(NULL)
        , session(NULL)
        , sessionId(0x98765432)
        , driverAddress(NULL)
        , address("1.2.3.4")
        , port(12345)
    {
    }

    void
    setUp()
    {
        dispatch->currentTime = 1000;
        driver = new MockDriver(FastTransport::Header::headerToString);
        transport = new FastTransport(driver);
        session = new FastTransport::ServerSession(transport, sessionId);
        ServiceLocator sl("mock: host=1.2.3.4, port=12345");
        driverAddress = driver->newAddress(sl);
        delete serviceManager;
        ServiceManager::init();
    }

    void
    tearDown()
    {
        delete driverAddress;
        delete session;
        delete transport;
    }

    void
    test_beginSending()
    {
        dispatch->currentTime = 9898;

        uint8_t channelId = 6;
        // Just here to flip us into a state where
        // channel.rpcId == 0 and we have an RPC setup
        CPPUNIT_ASSERT_EQUAL(FastTransport::ServerSession::
                             ServerChannel::INVALID_RPC_ID,
                             session->channels[0].rpcId);
        MockReceived junk(0, 1, "foo");
        junk.getHeader()->channelId = channelId;
        session->processInboundPacket(&junk);

        session->beginSending(channelId);
        CPPUNIT_ASSERT_EQUAL(FastTransport::ServerSession::
                             ServerChannel::SENDING_WAITING,
                             session->channels[channelId].state);
        CPPUNIT_ASSERT_EQUAL(dispatch->currentTime, session->lastActivityTime);
    }

    void
    test_expire_channelStillProcessing()
    {
        session->lastActivityTime = 1;
        session->channels[FastTransport::NUM_CHANNELS_PER_SESSION - 1].state =
            FastTransport::ServerSession::ServerChannel::PROCESSING;
        CPPUNIT_ASSERT(!session->expire());
        session->channels[FastTransport::NUM_CHANNELS_PER_SESSION - 1].state =
            FastTransport::ServerSession::ServerChannel::IDLE;
    }

    void
    test_expire_channelRecvOrSendWait()
    {
        session->lastActivityTime = 1;
        for (uint32_t i = 0; i < FastTransport::NUM_CHANNELS_PER_SESSION; i++)
            session->channels[i].state =
                FastTransport::ServerSession::ServerChannel::IDLE;
        uint32_t magic = 19281;
        session->channels[0].rpcId = magic;
        session->channels[1].state =
            FastTransport::ServerSession::ServerChannel::RECEIVING;
        session->channels[1].currentRpc.setup(session, 1);
        session->channels[2].state =
            FastTransport::ServerSession::ServerChannel::SENDING_WAITING;
        session->channels[2].currentRpc.setup(session, 2);

        CPPUNIT_ASSERT(session->expire());

        // ensure 0 got skipped
        CPPUNIT_ASSERT_EQUAL(FastTransport::ServerSession::ServerChannel::IDLE,
                             session->channels[0].state);
        CPPUNIT_ASSERT_EQUAL(magic, session->channels[0].rpcId);

        // ensure 1 got reset
        CPPUNIT_ASSERT_EQUAL(FastTransport::ServerSession::ServerChannel::IDLE,
                             session->channels[1].state);
        CPPUNIT_ASSERT_EQUAL(~(0u), session->channels[1].rpcId);
        CPPUNIT_ASSERT_EQUAL(0, session->channels[1].currentRpc.session);

        // ensure 2 got reset
        CPPUNIT_ASSERT_EQUAL(FastTransport::ServerSession::ServerChannel::IDLE,
                             session->channels[2].state);
        CPPUNIT_ASSERT_EQUAL(~(0u), session->channels[2].rpcId);
        CPPUNIT_ASSERT_EQUAL(0, session->channels[2].currentRpc.session);

        // check the minor tid-bits at the end
        CPPUNIT_ASSERT_EQUAL(FastTransport::ServerSession::INVALID_TOKEN,
                             session->token);
        CPPUNIT_ASSERT_EQUAL(FastTransport::ServerSession::INVALID_HINT,
                             session->clientSessionHint);
    }

    void
    test_processInboundPacket_badChannel()
    {
        TestLog::Enable dummy;
        dispatch->currentTime = 9898;

        MockReceived recvd(0, 1, "");
        recvd.getHeader()->channelId = FastTransport::NUM_CHANNELS_PER_SESSION;

        session->processInboundPacket(&recvd);
        CPPUNIT_ASSERT_EQUAL(dispatch->currentTime, session->lastActivityTime);
        CPPUNIT_ASSERT_EQUAL(
            "processInboundPacket: "
            "invalid channel id 8", TestLog::get());
    }

    /// A predicate to limit TestLog messages to processInboundPacket
    static bool pipPred(string s)
    {
        return (s == "processInboundPacket");
    }

    void
    test_processInboundPacket_currentRpcReceivedDataPacket()
    {
        TestLog::Enable _(&pipPred);

        // Just here to flip us into a state where
        // channel.rpcId == 0
        CPPUNIT_ASSERT_EQUAL(FastTransport::ServerSession::
                             ServerChannel::INVALID_RPC_ID,
                             session->channels[0].rpcId);
        MockReceived junk(0, 2, "foo");
        // Just to start the currentRpc
        session->processInboundPacket(&junk);
        TestLog::reset();

        CPPUNIT_ASSERT_EQUAL(0, session->channels[0].rpcId);
        // This one exercises the code path we are interested in
        MockReceived recvd(0, 1, "foo");
        session->processInboundPacket(&recvd);
        CPPUNIT_ASSERT_EQUAL(
            "processInboundPacket: "
            "processReceivedData",
            TestLog::get());
    }

    void
    test_processInboundPacket_currentRpcReceivedAckPacket()
    {
        TestLog::Enable _;

        // Just here to flip us into a state where
        // channel.rpcId == 0
        CPPUNIT_ASSERT_EQUAL(FastTransport::ServerSession::
                             ServerChannel::INVALID_RPC_ID,
                             session->channels[0].rpcId);
        MockReceived junk(0, 2, "foo");
        session->processInboundPacket(&junk);
        TestLog::reset();

        FastTransport::AckResponse ackResp(1, 0);
        MockReceived recvd(0, 1, &ackResp, sizeof(FastTransport::AckResponse));

        session->processInboundPacket(&recvd);
        CPPUNIT_ASSERT_EQUAL(
            "processInboundPacket: "
            "processReceivedAck",
            TestLog::get());
    }

    void
    test_processInboundPacket_currentRpcBadPayloadType()
    {
        TestLog::Enable _;

        MockReceived junk(0, 2, "foo");
        session->processInboundPacket(&junk);
        TestLog::reset();

        MockReceived recvd(1, 2, "foo");
        recvd.getHeader()->payloadType = FastTransport::Header::RESERVED1;
        session->processInboundPacket(&recvd);
        CPPUNIT_ASSERT_EQUAL(
            "processInboundPacket: "
            "current rpcId has bad packet type 3",
            TestLog::get());
    }

    void
    test_processInboundPacket_nextRpcReceivedDataPacket()
    {
        TestLog::Enable _;
        MockReceived recvd(0, 1, "God hates ponies.");

        session->processInboundPacket(&recvd);
        CPPUNIT_ASSERT_EQUAL(
            "processInboundPacket: "
            "start a new RPC | "
            "processInboundPacket: "
            "processReceivedData",
            TestLog::get());
        session->channels[0].state =
                FastTransport::ServerSession::ServerChannel::IDLE;
    }

    void
    test_processInboundPacket_newRpcBadPayloadType()
    {
        TestLog::Enable _(&pipPred);

        MockReceived recvd(0, 2, "foo");
        recvd.getHeader()->payloadType = FastTransport::Header::RESERVED1;
        session->processInboundPacket(&recvd);
        CPPUNIT_ASSERT_EQUAL(
            "processInboundPacket: "
            "start a new RPC | "
            "processInboundPacket: "
            "new rpcId has bad type 3",
            TestLog::get());
    }

    void
    test_startSession()
    {
        dispatch->currentTime = 9898;
        const uint64_t rand = 0x7676UL;
        MockRandom __(rand);

        uint32_t clientSessionHint = 0x12345678u;
        session->startSession(driverAddress, clientSessionHint);
        CPPUNIT_ASSERT(
            *static_cast<MockDriver::MockAddress*>(driverAddress) ==
            *static_cast<MockDriver::MockAddress*>(
                session->clientAddress.get()));
        CPPUNIT_ASSERT_EQUAL(clientSessionHint, session->clientSessionHint);
        CPPUNIT_ASSERT_EQUAL(rand, session->token);
        CPPUNIT_ASSERT_EQUAL(
            "{ sessionToken:7676 rpcId:0 clientSessionHint:12345678 "
            "serverSessionHint:98765432 0/0 frags channel:0 dir:1 reqACK:0 "
            "drop:0 payloadType:2 } /x08",
            driver->outputLog);
        CPPUNIT_ASSERT_EQUAL(9898, session->lastActivityTime);
    }

    void
    test_processReceivedData_receiving()
    {
        FastTransport::ServerSession::ServerChannel* channel =
                &session->channels[0];
        channel->state =
                FastTransport::ServerSession::ServerChannel::RECEIVING;
        channel->currentRpc.setup(session, 0);
        uint16_t totalFrags = 2;
        Buffer recvBuffer;
        channel->inboundMsg.init(totalFrags, &recvBuffer);

        // if not taken (not last fragment)
        MockReceived firstRecvd(0, totalFrags, "first");
        session->processReceivedData(channel, &firstRecvd);
        CPPUNIT_ASSERT_EQUAL("first", bufferToDebugString(&recvBuffer));
        CPPUNIT_ASSERT_EQUAL(FastTransport::ServerSession::
                             ServerChannel::RECEIVING,
                             session->channels[0].state);

        // if taken (last fragment)
        MockReceived lastRecvd(1, totalFrags, "last");
        session->processReceivedData(channel, &lastRecvd);
        CPPUNIT_ASSERT_EQUAL("first | last",
                             bufferToDebugString(&recvBuffer));
        CPPUNIT_ASSERT_EQUAL(&channel->currentRpc,
                             serviceManager->waitForRpc(0));
        CPPUNIT_ASSERT_EQUAL(FastTransport::ServerSession::
                             ServerChannel::PROCESSING,
                             session->channels[0].state);
        session->channels[0].state =
                FastTransport::ServerSession::ServerChannel::IDLE;
    }

    void
    test_processReceivedData_processing()
    {
        MockReceived recvd(0, 1, "");
        recvd.getHeader()->requestAck = 1;

        FastTransport::ServerSession::ServerChannel* channel =
                &session->channels[0];
        channel->state =
                FastTransport::ServerSession::ServerChannel::PROCESSING;
        session->processReceivedData(channel, &recvd);

        CPPUNIT_ASSERT(string::npos != driver->outputLog.find("payloadType:1"));
        channel->state = FastTransport::ServerSession::ServerChannel::IDLE;
    }

  private:
    MockDriver* driver;
    FastTransport* transport;
    FastTransport::ServerSession* session;
    const uint32_t sessionId;
    Driver::Address* driverAddress;
    const char* address;
    const uint16_t port;

    DISALLOW_COPY_AND_ASSIGN(ServerSessionTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(ServerSessionTest);

// --- ClientSessionTest ---

class ClientSessionTest: public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(ClientSessionTest);
    CPPUNIT_TEST(test_clientSend_notConnected);
    CPPUNIT_TEST(test_clientSend_noAvailableChannel);
    CPPUNIT_TEST(test_clientSend_availableChannel);
    CPPUNIT_TEST(test_close);
    CPPUNIT_TEST(test_connect);
    CPPUNIT_TEST(test_connect_sessionOpenRequestRetransmit);
    CPPUNIT_TEST(test_connect_sessionOpenRequestTimeout);
    CPPUNIT_TEST(test_expire_activeRef);
    CPPUNIT_TEST(test_expire_activeOnChannel);
    CPPUNIT_TEST(test_expire_rpcQueued);
    CPPUNIT_TEST(test_expire_nothingActive);
    CPPUNIT_TEST(test_fillHeader);
    CPPUNIT_TEST(test_init);
    CPPUNIT_TEST(test_processInboundPacket_sessionOpen);
    CPPUNIT_TEST(test_processInboundPacket_invalidChannel);
    CPPUNIT_TEST(test_processInboundPacket_data);
    CPPUNIT_TEST(test_processInboundPacket_ack);
    CPPUNIT_TEST(test_processInboundPacket_badSession);
    CPPUNIT_TEST(test_processInboundPacket_badPayloadType);
    CPPUNIT_TEST(test_processInboundPacket_stalePacket);
    CPPUNIT_TEST(test_sendSessionOpenRequest);
    CPPUNIT_TEST(test_allocateChannels);
    CPPUNIT_TEST(test_getAvailableChannel);
    CPPUNIT_TEST(test_processReceivedData_transitionSendToReceive);
    CPPUNIT_TEST(test_processReceivedData_queueEmpty);
    CPPUNIT_TEST(test_processReceivedData_getWorkFromQueue);
    CPPUNIT_TEST(test_processSessionOpenResponse);
    CPPUNIT_TEST(test_processSessionOpenResponse_tooManyChannelsOnServer);
    CPPUNIT_TEST_SUITE_END();

  public:
    ClientSessionTest()
        : driver(NULL)
        , transport(NULL)
        , session(NULL)
        , request(NULL)
        , response(NULL)
        , sessionId(0x98765432)
        , addr()
        , addrp(NULL)
        , addrLen(0)
        , address("1.2.3.4")
        , port(12345)
    {
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        assert(inet_aton(&address[0], &addr.sin_addr));
        addrLen = sizeof(addr);
        addrp = reinterpret_cast<sockaddr*>(&addr);
    }

    void
    setUp()
    {
        driver = new MockDriver(FastTransport::Header::headerToString);
        transport = new FastTransport(driver);
        session = new FastTransport::ClientSession(transport, sessionId);
        request = new Buffer();
        response = new Buffer();
    }

    void
    tearDown()
    {
        delete response;
        delete request;
        delete session;
        delete transport;
    }

    void
    test_clientSend_notConnected()
    {
        CPPUNIT_ASSERT(!session->isConnected());
        CPPUNIT_ASSERT(session->channelQueue.empty());
        FastTransport::ClientRpc* rpc = session->clientSend(request, response);
        CPPUNIT_ASSERT_EQUAL(rpc, &session->channelQueue.front());
        session->channelQueue.pop_front(); // satisfy boost assertion;
    }

    void
    test_clientSend_noAvailableChannel()
    {
        CPPUNIT_ASSERT(session->channelQueue.empty());
        FastTransport::ClientRpc* rpc = session->clientSend(request, response);
        CPPUNIT_ASSERT_EQUAL(rpc, &session->channelQueue.front());
        session->channelQueue.pop_front(); // satisfy boost assertion;
    }

    void
    test_clientSend_availableChannel()
    {
        dispatch->currentTime = 98328;

        session->numChannels = FastTransport::MAX_NUM_CHANNELS_PER_SESSION;
        session->allocateChannels();

        FastTransport::ClientRpc* rpc = session->clientSend(request,
                                                            response);
        FastTransport::ClientSession::ClientChannel* channel =
                &session->channels[0];
        CPPUNIT_ASSERT_EQUAL(FastTransport::ClientSession::
                             ClientChannel::SENDING,
                             channel->state);
        CPPUNIT_ASSERT_EQUAL(rpc, channel->currentRpc);
        channel->currentRpc = NULL;
        CPPUNIT_ASSERT_EQUAL(dispatch->currentTime, session->lastActivityTime);
    }

    void
    test_close()
    {
        session->numChannels = FastTransport::MAX_NUM_CHANNELS_PER_SESSION;
        session->allocateChannels();

        FastTransport::ClientRpc rpc1(transport, request, response);
        FastTransport::ClientRpc rpc2(transport, request, response);
        FastTransport::ClientRpc rpc3(transport, request, response);
        session->channelQueue.push_back(rpc3);

        session->channels[0].currentRpc = &rpc1;
        session->channels[1].currentRpc = &rpc2;

        session->close();
        CPPUNIT_ASSERT(rpc1.isReady());
        CPPUNIT_ASSERT_EQUAL("RPC aborted", rpc1.errorMessage->c_str());
        CPPUNIT_ASSERT(rpc2.isReady());
        CPPUNIT_ASSERT_EQUAL("RPC aborted", rpc2.errorMessage->c_str());
        CPPUNIT_ASSERT(session->channelQueue.empty());
        CPPUNIT_ASSERT_EQUAL("RPC aborted", rpc3.errorMessage->c_str());
        CPPUNIT_ASSERT_EQUAL(FastTransport::ClientSession::INVALID_HINT,
                             session->serverSessionHint);
        CPPUNIT_ASSERT_EQUAL(FastTransport::ClientSession::INVALID_TOKEN,
                             session->token);
    }

    void
    test_connect()
    {
        dispatch->currentTime = 91291;

        ServiceLocator serviceLocator("fast+udp: host=1.2.3.4, port=12345");
        session->init(serviceLocator);

        session->connect();
        CPPUNIT_ASSERT_EQUAL(true, session->sessionOpenRequestInFlight);

        // ensure the second call doesn't send an additional SessionOpenReq
        session->connect();
        CPPUNIT_ASSERT_EQUAL(
            "{ sessionToken:cccccccccccccccc rpcId:0 "
            "clientSessionHint:98765432 serverSessionHint:cccccccc "
            "0/0 frags channel:0 dir:0 reqACK:0 drop:0 payloadType:2 } ",
            driver->outputLog);
        CPPUNIT_ASSERT_EQUAL(dispatch->currentTime, session->lastActivityTime);
    }

    void
    test_connect_sessionOpenRequestRetransmit()
    {
        dispatch->currentTime = 91291;

        ServiceLocator serviceLocator("fast+udp: host=1.2.3.4, port=12345");
        session->init(serviceLocator);

        session->connect();
        CPPUNIT_ASSERT_EQUAL(true, session->sessionOpenRequestInFlight);


        MockTSC _(dispatch->currentTime + FastTransport::timeoutCycles() + 1);
        dispatch->poll();

        CPPUNIT_ASSERT_EQUAL(
            "{ sessionToken:cccccccccccccccc rpcId:0 "
            "clientSessionHint:98765432 serverSessionHint:cccccccc "
            "0/0 frags channel:0 dir:0 reqACK:0 drop:0 payloadType:2 }  | "
            "{ sessionToken:cccccccccccccccc rpcId:0 "
            "clientSessionHint:98765432 serverSessionHint:cccccccc "
            "0/0 frags channel:0 dir:0 reqACK:0 drop:0 payloadType:2 } ",
            driver->outputLog);
    }

    void
    test_connect_sessionOpenRequestTimeout()
    {
        dispatch->currentTime = 91291;

        ServiceLocator serviceLocator("fast+udp: host=1.2.3.4, port=12345");
        session->init(serviceLocator);

        session->connect();
        CPPUNIT_ASSERT_EQUAL(true, session->sessionOpenRequestInFlight);

        FastTransport::ClientRpc rpc(transport, request, response);
        session->channelQueue.push_back(rpc);

        MockTSC _(dispatch->currentTime +
                2*FastTransport::sessionTimeoutCycles());
        dispatch->poll();
        CPPUNIT_ASSERT_EQUAL(false, session->sessionOpenRequestInFlight);

        CPPUNIT_ASSERT_EQUAL("RPC aborted", rpc.errorMessage->c_str());
        CPPUNIT_ASSERT(!rpc.channelQueueEntries.is_linked());
    }

    void
    test_expire_activeRef()
    {
        session->numChannels = FastTransport::MAX_NUM_CHANNELS_PER_SESSION;
        session->allocateChannels();
        FastTransport::SessionRef s(session);
        bool didClose = session->expire();
        CPPUNIT_ASSERT_EQUAL(false, didClose);
    }

    void
    test_expire_activeOnChannel()
    {
        session->numChannels = FastTransport::MAX_NUM_CHANNELS_PER_SESSION;
        session->allocateChannels();

        FastTransport::ClientRpc rpc(transport, request, response);
        session->channels[0].currentRpc = &rpc;

        bool didClose = session->expire();
        CPPUNIT_ASSERT_EQUAL(false, didClose);
        session->channels[0].currentRpc = NULL;
    }

    void
    test_expire_rpcQueued()
    {
        session->numChannels = FastTransport::MAX_NUM_CHANNELS_PER_SESSION;
        session->allocateChannels();

        FastTransport::ClientRpc rpc(transport, request, response);
        session->channelQueue.push_back(rpc);

        bool didClose = session->expire();
        CPPUNIT_ASSERT_EQUAL(false, didClose);

        session->channelQueue.pop_front(); // satisfy boost assertion;
    }

    void
    test_expire_nothingActive()
    {
        session->numChannels = FastTransport::MAX_NUM_CHANNELS_PER_SESSION;
        session->allocateChannels();

        bool didClose = session->expire();
        CPPUNIT_ASSERT_EQUAL(true, didClose);
    }

    void
    test_fillHeader()
    {
        FastTransport::Header header;
        session->numChannels = FastTransport::MAX_NUM_CHANNELS_PER_SESSION;
        session->allocateChannels();
        session->fillHeader(&header, 6);
        CPPUNIT_ASSERT_EQUAL(
            "{ sessionToken:cccccccccccccccc rpcId:0 "
            "clientSessionHint:98765432 serverSessionHint:cccccccc "
            "0/0 frags channel:6 dir:0 reqACK:0 drop:0 payloadType:0 }",
            header.toString());
    }

    void
    test_init()
    {
        ServiceLocator serviceLocator("fast+udp: host=1.2.3.4, port=0x3742");
        session->init(serviceLocator);
        CPPUNIT_ASSERT(0 != session->serverAddress.get());
    }

    void
    test_processInboundPacket_sessionOpen()
    {
        FastTransport::SessionOpenResponse sessResp =
                { FastTransport::NUM_CHANNELS_PER_SESSION };
        MockReceived recvd(0, 1, &sessResp, sizeof(sessResp));
        recvd.getHeader()->payloadType = FastTransport::Header::SESSION_OPEN;

        session->processInboundPacket(&recvd);
        CPPUNIT_ASSERT_EQUAL(true, session->isConnected());
    }

    void
    test_processInboundPacket_invalidChannel()
    {
        TestLog::Enable _;
        MockReceived recvd(0, 1, "");

        session->processInboundPacket(&recvd);
        CPPUNIT_ASSERT_EQUAL(false, session->isConnected());
        CPPUNIT_ASSERT_EQUAL(
            "processInboundPacket: invalid channel id 0",
            TestLog::get());
    }

    void
    test_processInboundPacket_data()
    {
        dispatch->currentTime = 91291;

        session->numChannels = FastTransport::MAX_NUM_CHANNELS_PER_SESSION;
        session->allocateChannels();
        FastTransport::ClientSession::ClientChannel* channel =
                session->getAvailableChannel();
        CPPUNIT_ASSERT(channel);
        channel->state = FastTransport::ClientSession::ClientChannel::SENDING;
        channel->currentRpc = new FastTransport::ClientRpc(transport,
                                                           request,
                                                           response);

        MockReceived recvd(0, 2, "first of two");
        session->processInboundPacket(&recvd);
        CPPUNIT_ASSERT_EQUAL(FastTransport::ClientSession::
                             ClientChannel::RECEIVING,
                             channel->state);

        CPPUNIT_ASSERT_EQUAL(dispatch->currentTime, session->lastActivityTime);
        channel->currentRpc = NULL;
    }

    void
    test_processInboundPacket_ack()
    {
        session->numChannels = FastTransport::MAX_NUM_CHANNELS_PER_SESSION;
        session->allocateChannels();
        FastTransport::ClientSession::ClientChannel* channel =
                session->getAvailableChannel();
        CPPUNIT_ASSERT(channel);
        channel->state = FastTransport::ClientSession::ClientChannel::SENDING;

        channel->outboundMsg.totalFrags = 5;
        channel->outboundMsg.sendBuffer = request;

        FastTransport::AckResponse ackResp(2, 0);
        MockReceived recvd(0, 5, &ackResp, sizeof(FastTransport::AckResponse));
        recvd.getHeader()->payloadType = FastTransport::Header::ACK;

        session->processInboundPacket(&recvd);
        CPPUNIT_ASSERT_EQUAL(2, channel->outboundMsg.firstMissingFrag);
    }

    void
    test_processInboundPacket_badSession()
    {
        session->numChannels = FastTransport::MAX_NUM_CHANNELS_PER_SESSION;
        session->allocateChannels();
        CPPUNIT_ASSERT(session->channelQueue.empty());

        // Put an RPC on one of the channels
        FastTransport::ClientRpc* rpc =
                new FastTransport::ClientRpc(transport, request, response);
        session->channels[1].currentRpc = rpc;

        MockReceived recvd(0, 1, "");
        recvd.getHeader()->payloadType = FastTransport::Header::BAD_SESSION;

        session->processInboundPacket(&recvd);

        // Make sure the RPC made it back onto the queue safely
        CPPUNIT_ASSERT_EQUAL(rpc, &session->channelQueue.front());

        CPPUNIT_ASSERT_EQUAL(FastTransport::ClientSession::INVALID_HINT,
                             session->serverSessionHint);
        CPPUNIT_ASSERT_EQUAL(FastTransport::ClientSession::INVALID_TOKEN,
                             session->token);
        CPPUNIT_ASSERT_EQUAL(
            "{ sessionToken:cccccccccccccccc rpcId:0 "
            "clientSessionHint:98765432 serverSessionHint:cccccccc "
            "0/0 frags channel:0 dir:0 reqACK:0 drop:0 payloadType:2 } ",
            driver->outputLog);
        session->channelQueue.pop_front();
    }

    void
    test_processInboundPacket_badPayloadType()
    {
        TestLog::Enable _;
        session->numChannels = FastTransport::MAX_NUM_CHANNELS_PER_SESSION;
        session->allocateChannels();
        FastTransport::ClientSession::ClientChannel* channel =
                session->getAvailableChannel();
        CPPUNIT_ASSERT(channel);
        channel->state = FastTransport::ClientSession::ClientChannel::SENDING;
        channel->currentRpc = new FastTransport::ClientRpc(transport,
                                            request, response);

        MockReceived recvd(0, 2, "packet data");
        recvd.getHeader()->payloadType = FastTransport::Header::RESERVED1;
        session->processInboundPacket(&recvd);
        channel->currentRpc = NULL;
        CPPUNIT_ASSERT_EQUAL(
            "processInboundPacket: bad payload type 3",
            TestLog::get());
    }

    void
    test_processInboundPacket_stalePacket()
    {
        TestLog::Enable _;
        session->numChannels = FastTransport::MAX_NUM_CHANNELS_PER_SESSION;
        session->allocateChannels();
        FastTransport::ClientSession::ClientChannel* channel =
                session->getAvailableChannel();
        CPPUNIT_ASSERT(channel);
        channel->state = FastTransport::ClientSession::ClientChannel::SENDING;
        channel->currentRpc = new FastTransport::ClientRpc(transport,
                                            request, response);

        MockReceived recvd(0, 2, "packet data");
        recvd.getHeader()->rpcId = 3;
        channel->rpcId = 4;
        session->processInboundPacket(&recvd);
        channel->currentRpc = NULL;
        CPPUNIT_ASSERT_EQUAL(
            "processInboundPacket: "
            "out-of-order packet (got rpcId 3, current rpcId 4)",
            TestLog::get());
    }

    void
    test_sendSessionOpenRequest()
    {
        dispatch->currentTime = 91291;

        ServiceLocator serviceLocator("fast+udp: host=1.2.3.4, port=12345");
        session->init(serviceLocator);
        session->sendSessionOpenRequest();
        CPPUNIT_ASSERT_EQUAL(
            "{ sessionToken:cccccccccccccccc rpcId:0 "
            "clientSessionHint:98765432 serverSessionHint:cccccccc "
            "0/0 frags channel:0 dir:0 reqACK:0 drop:0 payloadType:2 } ",
            driver->outputLog);
        CPPUNIT_ASSERT_EQUAL(dispatch->currentTime, session->lastActivityTime);
        session->timer.stop();
    }

    void
    test_allocateChannels()
    {
        CPPUNIT_ASSERT(!session->channels);
        session->allocateChannels();
        CPPUNIT_ASSERT(session->channels);
        for (uint32_t i = 0; i < session->numChannels; i++)
            CPPUNIT_ASSERT_EQUAL(0, session->channels[i].currentRpc);
    }

    void
    test_getAvailableChannel()
    {
        session->numChannels = FastTransport::MAX_NUM_CHANNELS_PER_SESSION;
        session->allocateChannels();
        uint32_t i = 0;
        for (;;) {
            FastTransport::ClientSession::ClientChannel* channel =
                session->getAvailableChannel();
            if (!channel)
                break;
            channel->state =
                    FastTransport::ClientSession::ClientChannel::SENDING;
            i++;
        }
        CPPUNIT_ASSERT_EQUAL(session->numChannels, i);
    }

    void
    test_processReceivedData_transitionSendToReceive()
    {
        FastTransport::ClientRpc rpc(transport, request, response);
        session->channelQueue.push_back(rpc);

        FastTransport::SessionOpenResponse sessResp =
                { FastTransport::NUM_CHANNELS_PER_SESSION };
        MockReceived initRecvd(0, 1, &sessResp, sizeof(sessResp));
        session->processSessionOpenResponse(&initRecvd);

        MockReceived recvd(0, 2, "God hates ponies.");
        FastTransport::ClientSession::ClientChannel* channel =
                &session->channels[0];
        session->processReceivedData(channel, &recvd);
        CPPUNIT_ASSERT_EQUAL(FastTransport::ClientSession::
                             ClientChannel::RECEIVING,
                             channel->state);
        channel->currentRpc = NULL;
    }

    void
    test_processReceivedData_queueEmpty()
    {;
        FastTransport::ClientRpc rpc(transport, request, response);
        session->channelQueue.push_back(rpc);

        FastTransport::SessionOpenResponse sessResp =
                { FastTransport::NUM_CHANNELS_PER_SESSION };
        MockReceived initRecvd(0, 1, &sessResp, sizeof(sessResp));
        session->processSessionOpenResponse(&initRecvd);

        MockReceived recvd(0, 1, "God hates ponies.");
        FastTransport::ClientSession::ClientChannel* channel =
                &session->channels[0];
        uint32_t prevRpcId = channel->rpcId;
        session->processReceivedData(channel, &recvd);
        CPPUNIT_ASSERT_EQUAL(prevRpcId + 1, channel->rpcId);
        CPPUNIT_ASSERT_EQUAL(FastTransport::ClientSession::ClientChannel::IDLE,
                             channel->state);
        CPPUNIT_ASSERT_EQUAL(0, channel->currentRpc);
    }

    void
    test_processReceivedData_getWorkFromQueue()
    {
        FastTransport::ClientRpc rpc1(transport, request, response);
        session->channelQueue.push_back(rpc1);

        FastTransport::SessionOpenResponse sessResp =
                { FastTransport::NUM_CHANNELS_PER_SESSION };
        MockReceived initRecvd(0, 1, &sessResp, sizeof(sessResp));
        session->processSessionOpenResponse(&initRecvd);

        FastTransport::ClientRpc rpc2(transport, request, response);
        session->channelQueue.push_back(rpc2);

        MockReceived recvd(0, 1, "God hates ponies.");
        FastTransport::ClientSession::ClientChannel* channel =
                    &session->channels[0];
        session->processReceivedData(channel, &recvd);
        CPPUNIT_ASSERT_EQUAL(1, channel->rpcId);
        CPPUNIT_ASSERT_EQUAL(FastTransport::ClientSession::
                             ClientChannel::SENDING,
                             channel->state);
        CPPUNIT_ASSERT_EQUAL(&rpc2, channel->currentRpc);
        CPPUNIT_ASSERT(session->channelQueue.empty());
        channel->currentRpc = NULL;
    }

    void
    test_processSessionOpenResponse()
    {
        FastTransport::SessionOpenResponse sessResp =
                { FastTransport::NUM_CHANNELS_PER_SESSION };
        MockReceived recvd(0, 1, &sessResp, sizeof(sessResp));
        FastTransport::Header* header = recvd.getHeader();
        header->serverSessionHint = 0x192837;
        header->sessionToken = 0x1212343456567878;

        // Insert an RPC into the work queue
        FastTransport::ClientRpc rpc(transport, request, response);
        session->channelQueue.push_back(rpc);

        session->processSessionOpenResponse(&recvd);

        CPPUNIT_ASSERT_EQUAL(true, session->isConnected());
        CPPUNIT_ASSERT_EQUAL(header->serverSessionHint,
                             session->serverSessionHint);
        CPPUNIT_ASSERT_EQUAL(header->sessionToken, session->token);
        CPPUNIT_ASSERT_EQUAL(FastTransport::NUM_CHANNELS_PER_SESSION,
                             session->numChannels);

        // Make sure our queued RPC made it onto the channel from the queue
        CPPUNIT_ASSERT_EQUAL(&rpc, session->channels[0].currentRpc);
        CPPUNIT_ASSERT(session->channelQueue.empty());

        // Make sure we bailed on once the queue was empty
        CPPUNIT_ASSERT_EQUAL(0, session->channels[1].currentRpc);
        session->channels[0].currentRpc = NULL;
    }

    void
    test_processSessionOpenResponse_tooManyChannelsOnServer()
    {
        FastTransport::SessionOpenResponse sessResp =
                { FastTransport::NUM_CHANNELS_PER_SESSION + 1 };
        MockReceived recvd(0, 1, &sessResp, sizeof(sessResp));

        session->processSessionOpenResponse(&recvd);

        CPPUNIT_ASSERT_EQUAL(FastTransport::NUM_CHANNELS_PER_SESSION,
                             session->numChannels);
    }

  private:
    MockDriver* driver;
    FastTransport* transport;
    FastTransport::ClientSession* session;
    Buffer* request;
    Buffer* response;
    const uint32_t sessionId;
    sockaddr_in addr;
    sockaddr* addrp;
    socklen_t addrLen;
    const char* address;
    const uint16_t port;

    DISALLOW_COPY_AND_ASSIGN(ClientSessionTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(ClientSessionTest);

// --- SessionTableTest ---

class SessionTableTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(SessionTableTest);
    CPPUNIT_TEST(test_sanity);
    CPPUNIT_TEST(test_operator_brackets);
    CPPUNIT_TEST(test_get);
    CPPUNIT_TEST(test_put);
    CPPUNIT_TEST(test_expire);
    CPPUNIT_TEST_SUITE_END();

    struct MockSession {
        MockSession(FastTransport* transport, uint32_t sessionHint)
            : expired(true)
            , id(sessionHint)
            , lastActivityTime(0)
            , nextFree(FastTransport::SessionTable<MockSession>::NONE)
            , transport(transport)
        {
        }
        uint64_t getLastActivityTime() {
            return lastActivityTime;
        }
        bool expire() {
            return expired;
        }
        // just used to mock out return from getLastActivityTime
        void setLastActivityTime(uint64_t time) {
            this->lastActivityTime = time;
        }
        // just used to mock out return from expired
        void setExpired(bool expired) {
            this->expired = expired;
        }
        virtual uint32_t getId() {
            return id;
        }
        virtual ~MockSession() {}

        bool expired;
        uint32_t id;
        uint64_t lastActivityTime;
        uint32_t nextFree;
        FastTransport* transport;
        DISALLOW_COPY_AND_ASSIGN(MockSession);
    };

  public:
    SessionTableTest()
    {}

    void
    test_sanity()
    {
        FastTransport::SessionTable<MockSession> st(NULL);
        MockSession* s[5];

        CPPUNIT_ASSERT_EQUAL(FastTransport::SessionTable<MockSession>::TAIL,
                             st.firstFree);
        s[0] = st.get();
        CPPUNIT_ASSERT_EQUAL(0, s[0]->id);
        CPPUNIT_ASSERT_EQUAL(FastTransport::SessionTable<MockSession>::NONE,
                             s[0]->nextFree);

        s[1] = st.get();
        CPPUNIT_ASSERT_EQUAL(1, s[1]->id);
        CPPUNIT_ASSERT_EQUAL(FastTransport::SessionTable<MockSession>::NONE,
                             s[1]->nextFree);

        s[2] = st.get();
        CPPUNIT_ASSERT_EQUAL(2, s[2]->id);
        CPPUNIT_ASSERT_EQUAL(FastTransport::SessionTable<MockSession>::NONE,
                             s[2]->nextFree);

        s[3] = st.get();
        CPPUNIT_ASSERT_EQUAL(3, s[3]->id);
        CPPUNIT_ASSERT_EQUAL(FastTransport::SessionTable<MockSession>::NONE,
                             s[3]->nextFree);

        st.put(s[3]);
        CPPUNIT_ASSERT_EQUAL(st.firstFree, s[3]->id);
        CPPUNIT_ASSERT_EQUAL(FastTransport::SessionTable<MockSession>::TAIL,
                             s[3]->nextFree);
        s[3] = st.get();
        CPPUNIT_ASSERT_EQUAL(3, s[3]->id);
        CPPUNIT_ASSERT_EQUAL(FastTransport::SessionTable<MockSession>::NONE,
                             s[3]->nextFree);

        st.put(s[2]);
        s[2] = st.get();
        CPPUNIT_ASSERT_EQUAL(2, s[2]->id);
        CPPUNIT_ASSERT_EQUAL(FastTransport::SessionTable<MockSession>::NONE,
                             s[2]->nextFree);

        st.put(s[0]);
        st.put(s[2]);
        CPPUNIT_ASSERT_EQUAL(2, st.firstFree);
        CPPUNIT_ASSERT_EQUAL(0, s[2]->nextFree);
        CPPUNIT_ASSERT_EQUAL(FastTransport::SessionTable<MockSession>::TAIL,
                             s[0]->nextFree);

    }

    void
    test_operator_brackets()
    {
        FastTransport::SessionTable<MockSession> st(NULL);
        MockSession* s = st.get();
        CPPUNIT_ASSERT_EQUAL(s, st[0]);
    }

    void
    test_get()
    {
        FastTransport::SessionTable<MockSession> st(NULL);
        MockSession* s = st.get();
        CPPUNIT_ASSERT_EQUAL(1, st.size());

        st.put(s);
        CPPUNIT_ASSERT_EQUAL(s, st.get());
        CPPUNIT_ASSERT_EQUAL(1, st.size());

        CPPUNIT_ASSERT(s != st.get());
        CPPUNIT_ASSERT_EQUAL(2, st.size());
    }

    void
    test_put()
    {
        FastTransport::SessionTable<MockSession> st(NULL);
        MockSession* s[2];
        s[0] = st.get();
        s[1] = st.get();
        st.put(s[0]);
        CPPUNIT_ASSERT_EQUAL(0, st.firstFree);
        CPPUNIT_ASSERT_EQUAL(FastTransport::SessionTable<MockSession>::TAIL,
                             s[0]->nextFree);
        CPPUNIT_ASSERT_EQUAL(FastTransport::SessionTable<MockSession>::NONE,
                             s[1]->nextFree);
    }

    void
    test_expire()
    {
        dispatch->currentTime = FastTransport::sessionTimeoutCycles();
        FastTransport::SessionTable<MockSession> st(NULL);

        // Make sure it runs/doesn't segfault on 0 length
        st.expire();

        // Non-trivial test - expires some, not others
        for (uint32_t i = 0; i < 3; i++) {
            st.get();
            // even numbered sessions are up for expire
            st[i]->setLastActivityTime(i % 2 ? dispatch->currentTime : 0);
        }

        st.expire();

        // One tricky bit, expire records lastCleanedIndex starting at 0 so
        // the first item to be cleaned is 1
        CPPUNIT_ASSERT_EQUAL(0, st.firstFree);
        CPPUNIT_ASSERT_EQUAL(2, st[0]->nextFree);
        CPPUNIT_ASSERT_EQUAL(FastTransport::SessionTable<MockSession>::TAIL,
                             st[2]->nextFree);
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(SessionTableTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(SessionTableTest);

}  // namespace RAMCloud
