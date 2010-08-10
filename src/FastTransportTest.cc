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
    CPPUNIT_TEST(test_totalFragMismatch_processReceivedData);
    CPPUNIT_TEST(test_addFirstMissing_processReceivedData);
    CPPUNIT_TEST(test_addRunFromRing_processReceivedData);
    CPPUNIT_TEST(test_addToRing_processReceivedData);
    CPPUNIT_TEST_SUITE_END();

  public:
    class MockReceived : public Driver::Received {
      public:
        MockReceived(uint32_t fragNumber,
                     uint32_t totalFrags,
                     const char* msg)
            : Received(), stealCount(0)
        {
            len = strlen(msg) + sizeof(Header);
            payload = new char[len];
            memcpy(getContents(), msg, len - sizeof(Header));
            Header *header = getHeader();
            header->fragNumber = fragNumber;
            header->totalFrags = totalFrags;
            header->requestAck = 0;
            header->rpcId = 0;
            header->channelId = 0;
            header->direction = Header::SERVER_TO_CLIENT;
            header->clientSessionHint = 0;
            header->serverSessionHint = 0;
            header->sessionToken = 0;
        }
        Header* getHeader() { return reinterpret_cast<Header*>(payload); }
        char* getContents() { return payload + sizeof(Header); }
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

    void resetup(uint32_t totalFrags, bool useTimer)
    {
        setUp(totalFrags, useTimer);
    }

    void
    test_sendAck()
    {
        ClientSession* session = dynamic_cast<ClientSession*>(msg->session);
        session->numChannels = MAX_NUM_CHANNELS_PER_SESSION;
        session->allocateChannels();

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
        resetup(2, true);

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
        resetup(2, useTimer);

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
            resetup(2, useTimer);
        }
    }

    void
    test_clear()
    {
        resetup(2, true);

        char junk[0];
        transport->addTimer(&msg->timer, 1000);
        CPPUNIT_ASSERT_EQUAL(true, LIST_IS_IN(&msg->timer, listEntries));
        msg->dataStagingRing[10] = std::pair<char*, uint32_t>(junk, 1);
        msg->dataStagingRing[13] = std::pair<char*, uint32_t>(junk, 1);

        msg->clear();

        CPPUNIT_ASSERT_EQUAL(0, msg->totalFrags);
        CPPUNIT_ASSERT_EQUAL(0, msg->firstMissingFrag);
        CPPUNIT_ASSERT_EQUAL(0, msg->dataBuffer);
        CPPUNIT_ASSERT_EQUAL(2, driver->releaseCount);
        CPPUNIT_ASSERT_EQUAL(false, LIST_IS_IN(&msg->timer, listEntries));
    }

    void
    test_totalFragMismatch_processReceivedData()
    {
        // NOTE Make sure to keep the MockReceiveds on the stack until
        // none of the data is in use as part of a buffer
        MockReceived recvd(0, msg->totalFrags + 1, "God hates ponies.");
        bool result = msg->processReceivedData(&recvd);
        CPPUNIT_ASSERT_EQUAL(false, result);
        CPPUNIT_ASSERT_EQUAL("", msg->dataBuffer->toString());
        CPPUNIT_ASSERT_EQUAL(0, msg->firstMissingFrag);
        CPPUNIT_ASSERT_EQUAL(1, msg->dataStagingRing[0].second);
        CPPUNIT_ASSERT_EQUAL(0, recvd.stealCount);
    }

    void
    test_addFirstMissing_processReceivedData()
    {
        // first recvd packet - never placed in ring
        MockReceived recvd(0, msg->totalFrags, "God hates ponies.");
        bool result = msg->processReceivedData(&recvd);
        CPPUNIT_ASSERT_EQUAL(false, result);
        CPPUNIT_ASSERT_EQUAL("God hates ponies.",
                             msg->dataBuffer->toString());
        CPPUNIT_ASSERT_EQUAL(1, msg->firstMissingFrag);
        CPPUNIT_ASSERT_EQUAL(2, msg->dataStagingRing[0].second);
        CPPUNIT_ASSERT_EQUAL(1, recvd.stealCount);

        // second recvd packet - never placed in ring - msg complete
        MockReceived nextRecvd(1, msg->totalFrags, "I hate ponies, also.");
        result = msg->processReceivedData(&nextRecvd);
        CPPUNIT_ASSERT_EQUAL(true, result);
        CPPUNIT_ASSERT_EQUAL("God hates ponies. | I hate ponies, also.",
                             msg->dataBuffer->toString());
        CPPUNIT_ASSERT_EQUAL(msg->totalFrags, msg->firstMissingFrag);
        CPPUNIT_ASSERT_EQUAL(3, msg->dataStagingRing[0].second);
        CPPUNIT_ASSERT_EQUAL(1, nextRecvd.stealCount);
    }

    void
    test_addRunFromRing_processReceivedData()
    {
        // first recvd packet - out of order - enters ring
        MockReceived recvd(1, msg->totalFrags, "I hate ponies, also.");
        bool result = msg->processReceivedData(&recvd);
        CPPUNIT_ASSERT_EQUAL(false, result);
        CPPUNIT_ASSERT_EQUAL("", msg->dataBuffer->toString());
        CPPUNIT_ASSERT_EQUAL(recvd.payload, msg->dataStagingRing[0].first);
        CPPUNIT_ASSERT_EQUAL(recvd.len, msg->dataStagingRing[0].second);

        // second recvd packet - completes connection
        MockReceived nextRecvd(0, msg->totalFrags, "God hates ponies.");
        result = msg->processReceivedData(&nextRecvd);
        CPPUNIT_ASSERT_EQUAL(true, result);
        CPPUNIT_ASSERT_EQUAL("God hates ponies. | I hate ponies, also.",
                             msg->dataBuffer->toString());
        CPPUNIT_ASSERT_EQUAL(3, msg->dataStagingRing[0].second);
    }

    void
    test_addToRing_processReceivedData()
    {
        // test with longer connection
        uint32_t totalFrags = 100;
        resetup(totalFrags, false);

        // first recvd packet - out of order - ensure in ring in right place
        MockReceived recvd(2, totalFrags, "pkt two");
        bool result = msg->processReceivedData(&recvd);
        CPPUNIT_ASSERT_EQUAL(false, result);
        CPPUNIT_ASSERT_EQUAL("", msg->dataBuffer->toString());
        CPPUNIT_ASSERT_EQUAL(recvd.payload, msg->dataStagingRing[1].first);
        CPPUNIT_ASSERT_EQUAL(recvd.len, msg->dataStagingRing[1].second);
        CPPUNIT_ASSERT_EQUAL(1, recvd.stealCount);

        // second recvd packet - out of order - ensure in ring in right place
        MockReceived nextRecvd(1, totalFrags, "pkt one");
        result = msg->processReceivedData(&nextRecvd);
        CPPUNIT_ASSERT_EQUAL(false, result);
        CPPUNIT_ASSERT_EQUAL("", msg->dataBuffer->toString());
        CPPUNIT_ASSERT_EQUAL(nextRecvd.payload, msg->dataStagingRing[0].first);
        CPPUNIT_ASSERT_EQUAL(nextRecvd.len, msg->dataStagingRing[0].second);
        CPPUNIT_ASSERT_EQUAL(1, nextRecvd.stealCount);
    }

  private:
    MockDriver* driver;
    FastTransport* transport;
    Buffer* buffer;
    InboundMessage* msg;

    DISALLOW_COPY_AND_ASSIGN(InboundMessageTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(InboundMessageTest);

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
                st[i]->setLastActivityTime(rdtsc());
            else
                st[i]->setLastActivityTime(0);
        }

        st.expire();

        // One tricky bit, expire records lastCleanedIndex starting at 0 so
        // the first item to be cleaned in 1
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
