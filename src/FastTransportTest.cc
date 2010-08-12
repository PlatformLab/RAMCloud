/* Copyright (c) 2010 Stanford University
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

/**
 * \file
 * Unit tests for #RAMCloud::FastTransport.
 */

#include "TestUtil.h"
#include "MockDriver.h"
#include "MockTransport.h"
#include "FastTransport.h"

#include "queue.h"

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
        FastTransport::Header *header = getHeader();
        header->fragNumber = fragNumber;
        header->totalFrags = totalFrags;
        header->requestAck = 0;
        header->rpcId = 0;
        header->channelId = 0;
        header->direction = FastTransport::Header::SERVER_TO_CLIENT;
        header->clientSessionHint = 0;
        header->serverSessionHint = 0;
        header->sessionToken = 0;
    }
  public:
    MockReceived(uint32_t fragNumber,
                 uint32_t totalFrags,
                 const void* msg,
                 uint32_t len)
        : Received(), stealCount(0)
    {
        this->len = len + sizeof(FastTransport::Header);
        construct(fragNumber, totalFrags,
                  static_cast<const char*>(msg), this->len);
    }
    MockReceived(uint32_t fragNumber,
                 uint32_t totalFrags,
                 const char* msg)
        : Received(), stealCount(0)
    {
        len = strlen(msg) + sizeof(FastTransport::Header);
        construct(fragNumber, totalFrags, msg, len);
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
    }
    int stealCount;
    DISALLOW_COPY_AND_ASSIGN(MockReceived);
};

class FastTransportTest : public CppUnit::TestFixture, FastTransport {
    CPPUNIT_TEST_SUITE(FastTransportTest);
    CPPUNIT_TEST(test_queue_is_in);
    CPPUNIT_TEST(test_clientSend);
    CPPUNIT_TEST(test_serverRecv);
    CPPUNIT_TEST(test_clientRPC_send);
    CPPUNIT_TEST_SUITE_END();

  public:

    FastTransportTest() : FastTransport(0) {}

    void
    test_queue_is_in()
    {
        struct QTest {
            explicit QTest(int i) : i(i), entry() {}
            int i;
            LIST_ENTRY(QTest) entry;
        };
        LIST_HEAD(QTestHead, QTest) list;
        LIST_INIT(&list);

        QTest o1(1);
        QTest o2(2);

        CPPUNIT_ASSERT(!LIST_IS_IN(&o1, entry));
        CPPUNIT_ASSERT(!LIST_IS_IN(&o2, entry));

        LIST_INSERT_HEAD(&list, &o2, entry);
        LIST_INSERT_HEAD(&list, &o1, entry);

        QTest *elm;
        int i = 1;
        LIST_FOREACH(elm, &list, entry) {
            CPPUNIT_ASSERT(LIST_IS_IN(elm, entry));
            CPPUNIT_ASSERT_EQUAL(i, elm->i);
            i++;
        }

        LIST_REMOVE(&o2, entry);

        CPPUNIT_ASSERT(LIST_IS_IN(&o1, entry));
        CPPUNIT_ASSERT(!LIST_IS_IN(&o2, entry));
    }

    void
    test_clientSend()
    {
        MockDriver d;
        FastTransport t(&d);
        Buffer request;
        Buffer response;

        Service service;
        service.setIp("0.0.0.0");
        service.setPort(0);

        ClientRPC* rpc =
            t.clientSend(&service, &request, &response);
        CPPUNIT_ASSERT(rpc != 0);
    }

    void
    test_serverRecv()
    {
        FastTransport transport(NULL);
        ServerRPC rpc(NULL, 0);
        TAILQ_INSERT_TAIL(&transport.serverReadyQueue, &rpc, readyQueueEntries);
        CPPUNIT_ASSERT_EQUAL(&rpc, transport.serverRecv());
    }

    void
    test_clientRPC_send()
    {
        MockDriver d;
        FastTransport t(&d);
        Buffer request;
        Buffer response;

        Service service;
        service.setIp("0.0.0.0");
        service.setPort(0);

        t.clientSend(&service, &request, &response);
        // After the send we should have a session in our service
        CPPUNIT_ASSERT(service.getSession() != 0);

        // If we do an additional send we should use the same session
        void* s = service.getSession();
        t.clientSend(&service, &request, &response);
        CPPUNIT_ASSERT_EQUAL(s, service.getSession());
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(FastTransportTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(FastTransportTest);

class InboundMessageTest : public CppUnit::TestFixture, FastTransport {
    CPPUNIT_TEST_SUITE(InboundMessageTest);
    CPPUNIT_TEST(test_sendAck);
    CPPUNIT_TEST(test_clear);
    CPPUNIT_TEST(test_init);
    CPPUNIT_TEST(test_setup);
    CPPUNIT_TEST(test_processReceivedData_totalFragMismatch);
    CPPUNIT_TEST(test_processReceivedData_addFirstMissing);
    CPPUNIT_TEST(test_processReceivedData_addRunFromRing);
    CPPUNIT_TEST(test_processReceivedData_addToRing);
    CPPUNIT_TEST(test_processReceivedData_sendAckCalled);
    CPPUNIT_TEST(test_processReceivedData_timerAdded);
    CPPUNIT_TEST_SUITE_END();

  public:

    void dataStagingRingToString(
                Ring<pair<char*, uint32_t>, MAX_STAGING_FRAGMENTS>& ring,
                string& s)
    {
        size_t max = 50;
        char tmp[max];

        for (uint32_t i = 0; i < ring.getLength(); i++) {
            char* payload = ring[i].first;
            uint32_t payloadLen = ring[i].second;

            if (!payload) {
                snprintf(tmp, max, "-, ");
            } else {
                string payloadStr;
                bufToString(payload, payloadLen, payloadStr);
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
        : FastTransport(0), driver(0), transport(0), buffer(0), msg(0)
    {}

    void
    setUp()
    {
        setUp(2, false);
    }

    void
    setUp(uint32_t totalFrags, bool useTimer = false)
    {
        driver = new MockDriver();
        transport = new FastTransport(driver);
        buffer = new Buffer();

        uint32_t channelId = 5;

        msg = new InboundMessage();
        ClientSession *session = transport->getClientSession();
        session->numChannels = MAX_NUM_CHANNELS_PER_SESSION;
        session->allocateChannels();
        msg->setup(session, channelId, useTimer);

        msg->clear();
        msg->init(totalFrags, buffer);

        // Initialize dataStagingRing to check invariants after calls
        for (uint32_t i = 0; i < msg->dataStagingRing.getLength(); i++)
            msg->dataStagingRing[i] = std::pair<char*, uint32_t>(0, i + 1);

    }

    void tearDown()
    {
        delete msg;
        delete buffer;
        delete transport;
        delete driver;
    }

    void
    test_sendAck()
    {
        // no packets received yet
        msg->sendAck();
        CPPUNIT_ASSERT_EQUAL("sendPacket: 0 /0 /0", driver->outputLog);
        driver->outputLog = std::string();

        // first ten received with no drops
        msg->firstMissingFrag = 10;
        msg->sendAck();
        CPPUNIT_ASSERT_EQUAL("sendPacket: 10 /0 /0", driver->outputLog);
        driver->outputLog = std::string();

        // first twenty received, missing 21 and one at other end of window
        msg->firstMissingFrag = 20;
        char junk[0];
        msg->dataStagingRing[0] = std::pair<char*, uint32_t>(junk, 1);
        msg->dataStagingRing[msg->dataStagingRing.getLength() - 1] =
            std::pair<char*, uint32_t>(junk, 1);
        msg->sendAck();
        CPPUNIT_ASSERT_EQUAL("sendPacket: 0x10014 /0 /x80", driver->outputLog);
        driver->outputLog = std::string();
    }
    void
    test_init()
    {
        setUp(2, true);

        Buffer buffer;
        msg->init(999, &buffer);

        CPPUNIT_ASSERT_EQUAL(999, msg->totalFrags);
        CPPUNIT_ASSERT_EQUAL(&buffer, msg->dataBuffer);
        CPPUNIT_ASSERT_EQUAL(true, LIST_IS_IN(&msg->timer, listEntries));
    }

    void
    test_setup()
    {
        bool useTimer = true;
        uint32_t channelId = 5;
        setUp(2, useTimer);

        transport->addTimer(&msg->timer, 999);
        CPPUNIT_ASSERT_EQUAL(true, LIST_IS_IN(&msg->timer, listEntries));

        for (;;) {
            ClientSession* session = transport->getClientSession();
            msg->setup(session, channelId, useTimer);
            CPPUNIT_ASSERT_EQUAL(session, msg->session);
            CPPUNIT_ASSERT_EQUAL(transport, msg->transport);
            CPPUNIT_ASSERT_EQUAL(channelId, msg->channelId);
            CPPUNIT_ASSERT_EQUAL(0, msg->timer.when);
            CPPUNIT_ASSERT_EQUAL(0, msg->timer.numTimeouts);
            CPPUNIT_ASSERT_EQUAL(useTimer, msg->timer.useTimer);
            CPPUNIT_ASSERT_EQUAL(false, LIST_IS_IN(&msg->timer, listEntries));
            if (!useTimer)
                break;
            useTimer = !useTimer;
            channelId = 6;
            setUp(2, useTimer);
        }
    }

    void
    test_clear()
    {
        setUp(2, true);

        char junk[0];
        transport->addTimer(&msg->timer, 1000);
        CPPUNIT_ASSERT_EQUAL(true, LIST_IS_IN(&msg->timer, listEntries));
        msg->dataStagingRing[10] = std::pair<char*, uint32_t>(junk, 1);
        msg->dataStagingRing[13] = std::pair<char*, uint32_t>(junk, 1);

        msg->clear();

        CPPUNIT_ASSERT_EQUAL(0, msg->totalFrags);
        CPPUNIT_ASSERT_EQUAL(0, msg->firstMissingFrag);
        string s;
        dataStagingRingToString(msg->dataStagingRing, s);
        CPPUNIT_ASSERT_EQUAL("-, -, -, -, -, -, -, -, -, -, -, -, -, -, "
                             "-, -, -, -, -, -, -, -, -, -, -, -, -, -, "
                             "-, -, -, -,", s);
        CPPUNIT_ASSERT_EQUAL(0, msg->dataBuffer);
        CPPUNIT_ASSERT_EQUAL(2, driver->releaseCount);
        CPPUNIT_ASSERT_EQUAL(false, LIST_IS_IN(&msg->timer, listEntries));
    }

    void
    test_processReceivedData_totalFragMismatch()
    {
        // NOTE Make sure to keep the MockReceiveds on the stack until
        // none of the data is in use as part of a buffer
        MockReceived recvd(0, msg->totalFrags + 1, "God hates ponies.");
        bool result = msg->processReceivedData(&recvd);
        CPPUNIT_ASSERT_EQUAL(false, result);
        CPPUNIT_ASSERT_EQUAL("", msg->dataBuffer->debugString());
        CPPUNIT_ASSERT_EQUAL(0, msg->firstMissingFrag);
        CPPUNIT_ASSERT_EQUAL(0, recvd.stealCount);
    }

    /**
     * Ensures that if packets arrive in order according to firstMissingFrag
     * everything works, including connection termination condition.
     */
    void
    test_processReceivedData_addFirstMissing()
    {
        // first recvd packet - never placed in ring
        MockReceived recvd(0, msg->totalFrags, "God hates ponies.");
        bool result = msg->processReceivedData(&recvd);
        CPPUNIT_ASSERT_EQUAL(false, result);
        CPPUNIT_ASSERT_EQUAL("God hates ponies.",
                             msg->dataBuffer->debugString());
        CPPUNIT_ASSERT_EQUAL(1, msg->firstMissingFrag);
        // TODO(stutsman) Looking for feedback about how to make the dSRToString
        // catch these types of cases easily
        CPPUNIT_ASSERT_EQUAL(2, msg->dataStagingRing[0].second);
        CPPUNIT_ASSERT_EQUAL(1, recvd.stealCount);

        // second recvd packet - never placed in ring - msg complete
        MockReceived nextRecvd(1, msg->totalFrags, "I hate ponies, also.");
        result = msg->processReceivedData(&nextRecvd);
        CPPUNIT_ASSERT_EQUAL(true, result);
        CPPUNIT_ASSERT_EQUAL("God hates ponies. | I hate ponies, also.",
                             msg->dataBuffer->debugString());
        CPPUNIT_ASSERT_EQUAL(msg->totalFrags, msg->firstMissingFrag);
        CPPUNIT_ASSERT_EQUAL(3, msg->dataStagingRing[0].second);
        CPPUNIT_ASSERT_EQUAL(1, nextRecvd.stealCount);
    }

    /**
     * Ensures out-of-order packets transition from the ring to the buffer
     * when the missing fragments before them are encountered.
     */
    void
    test_processReceivedData_addRunFromRing()
    {
        // first recvd packet - out of order - enters ring
        MockReceived recvd(1, msg->totalFrags, "I hate ponies, also.");
        bool result = msg->processReceivedData(&recvd);
        CPPUNIT_ASSERT_EQUAL(false, result);
        CPPUNIT_ASSERT_EQUAL("", msg->dataBuffer->debugString());
        CPPUNIT_ASSERT_EQUAL(recvd.payload, msg->dataStagingRing[0].first);
        CPPUNIT_ASSERT_EQUAL(recvd.len, msg->dataStagingRing[0].second);

        // second recvd packet - completes connection
        MockReceived nextRecvd(0, msg->totalFrags, "God hates ponies.");
        result = msg->processReceivedData(&nextRecvd);
        CPPUNIT_ASSERT_EQUAL(true, result);
        CPPUNIT_ASSERT_EQUAL("God hates ponies. | I hate ponies, also.",
                             msg->dataBuffer->debugString());
        CPPUNIT_ASSERT_EQUAL(3, msg->dataStagingRing[0].second);
    }

    /**
     * Ensures that if an out-of-order packet is encountered it is properly
     * stored in the staging ring to be moved in to the result buffer later.
     */
    void
    test_processReceivedData_addToRing()
    {
        // test with longer connection
        uint32_t totalFrags = 100;
        setUp(totalFrags, false);

        // first recvd packet - out of order - ensure in ring in right place
        MockReceived recvd(2, totalFrags, "pkt two");
        bool result = msg->processReceivedData(&recvd);
        CPPUNIT_ASSERT_EQUAL(false, result);
        CPPUNIT_ASSERT_EQUAL("", msg->dataBuffer->debugString());
        CPPUNIT_ASSERT_EQUAL(recvd.payload, msg->dataStagingRing[1].first);
        CPPUNIT_ASSERT_EQUAL(recvd.len, msg->dataStagingRing[1].second);
        CPPUNIT_ASSERT_EQUAL(1, recvd.stealCount);

        // second recvd packet - out of order - ensure in ring in right place
        MockReceived nextRecvd(1, totalFrags, "pkt one");
        result = msg->processReceivedData(&nextRecvd);
        CPPUNIT_ASSERT_EQUAL(false, result);
        CPPUNIT_ASSERT_EQUAL("", msg->dataBuffer->debugString());
        CPPUNIT_ASSERT_EQUAL(nextRecvd.payload, msg->dataStagingRing[0].first);
        CPPUNIT_ASSERT_EQUAL(nextRecvd.len, msg->dataStagingRing[0].second);
        CPPUNIT_ASSERT_EQUAL(1, nextRecvd.stealCount);
    }

    void
    test_processReceivedData_sendAckCalled()
    {
        MockReceived recvd(0, msg->totalFrags, "pkt zero");
        recvd.getHeader()->requestAck = 1;
        msg->processReceivedData(&recvd);

        CPPUNIT_ASSERT_EQUAL("sendPacket: 1 /0 /0", driver->outputLog);
    }

    void
    test_processReceivedData_timerAdded()
    {
        setUp(2, true);

        transport->removeTimer(&msg->timer);
        MockReceived recvd(0, msg->totalFrags, "pkt zero");
        recvd.getHeader()->requestAck = 1;
        msg->processReceivedData(&recvd);

        CPPUNIT_ASSERT_EQUAL(true, LIST_IS_IN(&msg->timer, listEntries));
        CPPUNIT_ASSERT(TIMEOUT_NS <= msg->timer.when);
    }

  private:
    MockDriver* driver;
    FastTransport* transport;
    Buffer* buffer;
    InboundMessage* msg;

    DISALLOW_COPY_AND_ASSIGN(InboundMessageTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(InboundMessageTest);

class OutboundMessageTest: public CppUnit::TestFixture, FastTransport {
    CPPUNIT_TEST_SUITE(OutboundMessageTest);
    CPPUNIT_TEST(test_clear);
    CPPUNIT_TEST(test_setup);
    CPPUNIT_TEST(test_beginSending);
    CPPUNIT_TEST(test_processReceivedAck_noSendBuffer);
    CPPUNIT_TEST(test_processReceivedAck_noneMissing);
    CPPUNIT_TEST(test_processReceivedAck_oneMissing);
    CPPUNIT_TEST(test_sendOneData_noRequestAck);
    CPPUNIT_TEST(test_sendOneData_requestAck);
    CPPUNIT_TEST_SUITE_END();

  public:
    void sentTimesRingToString(
                Ring<uint64_t, MAX_STAGING_FRAGMENTS + 1>& ring,
                string& s)
    {
        size_t max = 50;
        char tmp[max];

        for (uint32_t i = 0; i < ring.getLength(); i++) {
            uint64_t val = ring[i];
            if (val == OutboundMessage::ACKED)
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
        : FastTransport(0), driver(0), transport(0), buffer(0), msg(0) {}

    void
    setUp()
    {
        setUp(2, false);
    }

    void
    setUp(uint32_t totalFrags, bool useTimer = false)
    {
        driver = new MockDriver();
        transport = new FastTransport(driver);
        buffer = new Buffer();

        const char* testMsg = "abcdefghij";
        size_t testMsgLen = strlen(testMsg);
        char* payload = new(buffer, APPEND) char[testMsgLen * 160 + 1];
        for (uint32_t i = 0; i < 160; i++)
            memcpy(payload + i * testMsgLen, testMsg, testMsgLen);
        payload[testMsgLen * 160] = '\0';

        uint32_t channelId = 5;

        msg = new OutboundMessage();
        ClientSession *session = transport->getClientSession();
        session->numChannels = MAX_NUM_CHANNELS_PER_SESSION;
        session->allocateChannels();
        msg->setup(session, channelId, useTimer);

        msg->clear();
    }

    void tearDown()
    {
        delete msg;
        delete buffer;
        delete transport;
        delete driver;
    }

    /**
     * Simple check, but also checks subtler details regarding timers.
     * If the message was using the timer clear must remove it.
     */
    void
    test_clear()
    {
        setUp(2, true);
        transport->addTimer(&msg->timer, 999);

        msg->clear();

        CPPUNIT_ASSERT_EQUAL(0, msg->sendBuffer);
        CPPUNIT_ASSERT_EQUAL(0, msg->firstMissingFrag);
        CPPUNIT_ASSERT_EQUAL(0, msg->packetsSinceAckReq);
        string s;
        sentTimesRingToString(msg->sentTimes, s);
        CPPUNIT_ASSERT_EQUAL("0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "
                             "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "
                             "0, 0, 0,", s);
        CPPUNIT_ASSERT_EQUAL(0, msg->numAcked);
        CPPUNIT_ASSERT_EQUAL(false, LIST_IS_IN(&msg->timer, listEntries));
        CPPUNIT_ASSERT_EQUAL(0, msg->timer.when);
        CPPUNIT_ASSERT_EQUAL(0, msg->timer.numTimeouts);
    }

    void
    test_setup()
    {
        uint32_t channelId = 999;
        bool useTimer = false;

        ClientSession *session = transport->getClientSession();
        msg->setup(session, channelId, useTimer);

        CPPUNIT_ASSERT_EQUAL(session, msg->session);
        CPPUNIT_ASSERT_EQUAL(transport, msg->transport);
        CPPUNIT_ASSERT_EQUAL(channelId, msg->channelId);
        CPPUNIT_ASSERT_EQUAL(useTimer, msg->timer.useTimer);
    }

    void
    test_beginSending()
    {
        msg->beginSending(buffer);
        CPPUNIT_ASSERT_EQUAL(buffer, msg->sendBuffer);
        CPPUNIT_ASSERT(0 != msg->totalFrags);
    }

    void
    test_send()
    {
        MockTSC _(999);
    }

    void
    test_processReceivedAck_noSendBuffer()
    {
        bool result = msg->processReceivedAck(0);
        CPPUNIT_ASSERT_EQUAL(false, result);
    }

    // TODO(stutsman) Something's just totally wrong here
    /**
     * No packets missing; just ensure that bookkeeping slides up to
     * match the receiver side.
     */
    void
    test_processReceivedAck_noneMissing()
    {
        MockTSC _(999);

        msg->beginSending(buffer);

        AckResponse ackResp(0, 0);
        ackResp.firstMissingFrag = 1;
        MockReceived recvd(0, msg->totalFrags, &ackResp, sizeof(AckResponse));

        bool result = msg->processReceivedAck(&recvd);

        CPPUNIT_ASSERT_EQUAL(false, result);
        string s;
        sentTimesRingToString(msg->sentTimes, s);
        CPPUNIT_ASSERT_EQUAL("999, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "
                             "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "
                             "0, 0,", s);
        CPPUNIT_ASSERT_EQUAL(ackResp.firstMissingFrag, msg->numAcked);
    }

    // TODO(stutsman) Something's just totally wrong here
    /**
     * A packet is missing; eight packets beyond have been acked.
     * Excercises the bit vector.
     */
    void
    test_processReceivedAck_oneMissing()
    {
        MockTSC _(999);
        msg->beginSending(buffer);
        msg->totalFrags = 10;

        AckResponse ackResp(3, 0xff);
        MockReceived recvd(0, msg->totalFrags, &ackResp, sizeof(AckResponse));

        string s;
        sentTimesRingToString(msg->sentTimes, s);
        /*
        CPPUNIT_ASSERT_EQUAL("999, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,",
                             s);
        */

        msg->processReceivedAck(&recvd);
        //bool result = msg->processReceivedAck(&recvd);

        //CPPUNIT_ASSERT_EQUAL(false, result);
        s = "";
        sentTimesRingToString(msg->sentTimes, s);
        CPPUNIT_ASSERT_EQUAL("999, ACKED, ACKED, ACKED, ACKED, ACKED, ACKED, "
                             "ACKED, ACKED, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "
                             "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,",
                             s);
        CPPUNIT_ASSERT_EQUAL(ackResp.firstMissingFrag + 8, msg->numAcked);
    }

    void
    test_sendOneData_noRequestAck()
    {
        msg->sendBuffer = buffer;
        msg->sendOneData(0, false);
        CPPUNIT_ASSERT_EQUAL("sendPacket: abcdefghij (+1364 more)",
                             driver->outputLog);
        CPPUNIT_ASSERT_EQUAL(1, msg->packetsSinceAckReq);
    }

    void
    test_sendOneData_requestAck()
    {
        msg->sendBuffer = buffer;
        msg->sendOneData(0, true);
        CPPUNIT_ASSERT_EQUAL("sendPacket: abcdefghij (+1364 more)",
                             driver->outputLog);
        CPPUNIT_ASSERT_EQUAL(0, msg->packetsSinceAckReq);
    }

  private:
    MockDriver* driver;
    FastTransport* transport;
    Buffer* buffer;
    OutboundMessage* msg;

    DISALLOW_COPY_AND_ASSIGN(OutboundMessageTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(OutboundMessageTest);

class SessionTableTest : public CppUnit::TestFixture, FastTransport {
    CPPUNIT_TEST_SUITE(SessionTableTest);
    CPPUNIT_TEST(test_sanity);
    CPPUNIT_TEST(test_operator_brackets);
    CPPUNIT_TEST(test_get);
    CPPUNIT_TEST(test_put);
    CPPUNIT_TEST(test_expire);
    CPPUNIT_TEST_SUITE_END();

    struct MockSession {
        MockSession(FastTransport* transport, uint32_t sessionHint)
            : transport(transport),
              id(sessionHint),
              nextFree(SessionTable<MockSession>::NONE),
              expired(true),
              time(0)
        {
        }
        uint64_t getLastActivityTime() {
            return time;
        }
        bool expire() {
            return expired;
        }
        // just used to mock out return from getLastActivityTime
        void setLastActivityTime(uint64_t time) {
            this->time = time;
        }
        // just used to mock out return from expired
        void setExpired(bool expired) {
            this->expired = expired;
        }
        virtual uint32_t getId() {
            return id;
        }
        virtual ~MockSession() {}
        FastTransport* transport;
        uint32_t id;
        uint32_t nextFree;
        bool expired;
        uint64_t time;
        DISALLOW_COPY_AND_ASSIGN(MockSession);
    };

  public:
    SessionTableTest() : FastTransport(0) {}

    void
    test_sanity()
    {
        SessionTable<MockSession> st(0);
        MockSession* s[5];

        CPPUNIT_ASSERT_EQUAL(SessionTable<MockSession>::TAIL,
                             st.firstFree);
        s[0] = st.get();
        CPPUNIT_ASSERT_EQUAL(0, s[0]->id);
        CPPUNIT_ASSERT_EQUAL(SessionTable<MockSession>::NONE,
                             s[0]->nextFree);

        s[1] = st.get();
        CPPUNIT_ASSERT_EQUAL(1, s[1]->id);
        CPPUNIT_ASSERT_EQUAL(SessionTable<MockSession>::NONE,
                             s[1]->nextFree);

        s[2] = st.get();
        CPPUNIT_ASSERT_EQUAL(2, s[2]->id);
        CPPUNIT_ASSERT_EQUAL(SessionTable<MockSession>::NONE,
                             s[2]->nextFree);

        s[3] = st.get();
        CPPUNIT_ASSERT_EQUAL(3, s[3]->id);
        CPPUNIT_ASSERT_EQUAL(SessionTable<MockSession>::NONE,
                             s[3]->nextFree);

        st.put(s[3]);
        CPPUNIT_ASSERT_EQUAL(st.firstFree, s[3]->id);
        CPPUNIT_ASSERT_EQUAL(SessionTable<MockSession>::TAIL,
                             s[3]->nextFree);
        s[3] = st.get();
        CPPUNIT_ASSERT_EQUAL(3, s[3]->id);
        CPPUNIT_ASSERT_EQUAL(SessionTable<MockSession>::NONE,
                             s[3]->nextFree);

        st.put(s[2]);
        s[2] = st.get();
        CPPUNIT_ASSERT_EQUAL(2, s[2]->id);
        CPPUNIT_ASSERT_EQUAL(SessionTable<MockSession>::NONE,
                             s[2]->nextFree);

        st.put(s[0]);
        st.put(s[2]);
        CPPUNIT_ASSERT_EQUAL(2, st.firstFree);
        CPPUNIT_ASSERT_EQUAL(0, s[2]->nextFree);
        CPPUNIT_ASSERT_EQUAL(SessionTable<MockSession>::TAIL,
                             s[0]->nextFree);

    }

    void
    test_operator_brackets()
    {
        SessionTable<MockSession> st(0);
        MockSession* s = st.get();
        CPPUNIT_ASSERT_EQUAL(s, st[0]);
    }

    void
    test_get()
    {
        SessionTable<MockSession> st(0);
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
        SessionTable<MockSession> st(0);
        MockSession* s[2];
        s[0] = st.get();
        s[1] = st.get();
        st.put(s[0]);
        CPPUNIT_ASSERT_EQUAL(0, st.firstFree);
        CPPUNIT_ASSERT_EQUAL(SessionTable<MockSession>::TAIL,
                             s[0]->nextFree);
        CPPUNIT_ASSERT_EQUAL(SessionTable<MockSession>::NONE,
                             s[1]->nextFree);
    }

    void
    test_expire()
    {
        SessionTable<MockSession> st(0);

        // Make sure it runs/doesn't segfault on 0 length
        st.expire();

        // Non-trivial test - expires some, not others
        for (uint32_t i = 0; i < 3; i++) {
            st.get();
            // even numbered sessions are up for expire
            if (i % 2)
                st[i]->setLastActivityTime(rdtsc() + 100000000);
            else
                st[i]->setLastActivityTime(0);
        }

        st.expire();

        // One tricky bit, expire records lastCleanedIndex starting at 0 so
        // the first item to be cleaned is 1
        CPPUNIT_ASSERT_EQUAL(0, st.firstFree);
        CPPUNIT_ASSERT_EQUAL(2, st[0]->nextFree);
        CPPUNIT_ASSERT_EQUAL(SessionTable<MockSession>::TAIL,
                             st[2]->nextFree);
    }


  private:
    DISALLOW_COPY_AND_ASSIGN(SessionTableTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(SessionTableTest);

}  // namespace RAMCloud
