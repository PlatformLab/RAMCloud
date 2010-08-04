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

#include <TestUtil.h>
#include <FastTransport.h>

namespace RAMCloud {

class FastTransportTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(FastTransportTest);
    CPPUNIT_TEST(test_queue_is_in);
    CPPUNIT_TEST(test_clientSend);
    CPPUNIT_TEST(test_serverRecv);
    CPPUNIT_TEST(test_clientRPC_send);
    CPPUNIT_TEST_SUITE_END();

  public:

    class MockDriver : public Driver {
        char toHexDigit(char nibble) {
            nibble &= 0x0f;
            if (nibble < 0xa)
                return '0' + nibble;
            return 'a' + (nibble - 0xa);
        }

      public:
        virtual uint32_t getMaxPayloadSize() { return 1400; }
        virtual void sendPacket(const sockaddr *addr,
                                socklen_t addrlen,
                                void *header,
                                uint32_t headerLen,
                                Buffer::Iterator *payload)
        {
            str += "REQUEST: ";
            if (!payload) {
                str += "NULL";
                return;
            }
            for (; payload->isDone(); payload->next()) {
                char* data = static_cast<char*>(payload->getData());
                for (uint32_t i = 0; i < payload->getLength(); i++) {
                    str += toHexDigit(data[i] >> 4);
                    str += toHexDigit(data[i]);
                }
            }
        }

        virtual bool tryRecvPacket(Received *received)
        {
            if (sessionStarted) {
                sessionStarted = true;
                return false;
            }

            FastTransport::Header* header =
                reinterpret_cast<FastTransport::Header*>(replyBuffer);
            header->direction = FastTransport::Header::SERVER_TO_CLIENT;
            header->clientSessionHint = 0;
            header->serverSessionHint = 0;
            header->sessionToken = 0;
            header->rpcId = 0;
            header->channelId = 0;
            header->payloadType = FastTransport::Header::SESSION_OPEN;

            FastTransport::SessionOpenResponse* openResp =
                reinterpret_cast<FastTransport::SessionOpenResponse*>
                    (replyBuffer + sizeof(FastTransport::Header));
            openResp->maxChannelId = 0;

            received->addrlen = 0;
            received->len = sizeof(FastTransport::Header);
            received->driver = this;
            received->payload = reinterpret_cast<char*>(&header);

            return true;
        }

        virtual void release(char *payload, uint32_t len)
        {
        }

        MockDriver()
            : str(), sessionStarted(false)
        {
        }

        const std::string& toString()
        {
            return str;
        }

        virtual ~MockDriver() {}

      private:
        std::string str;
        bool sessionStarted;
        char replyBuffer[sizeof(FastTransport::Header) +
                         sizeof(FastTransport::SessionOpenResponse)];
        DISALLOW_COPY_AND_ASSIGN(MockDriver);
    };

    FastTransportTest() {}

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

        FastTransport::ClientRPC* rpc =
            t.clientSend(&service, &request, &response);
        CPPUNIT_ASSERT(rpc != 0);
    }

    void
    test_serverRecv()
    {
        FastTransport transport(NULL);
        FastTransport::ServerRPC rpc(NULL, 0);
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
            : transport(transport),
              id(sessionHint),
              nextFree(FastTransport::SessionTable<MockSession>::NONE),
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
    SessionTableTest() {}

    void
    test_sanity()
    {
        FastTransport::SessionTable<MockSession> st(0);
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
        FastTransport::SessionTable<MockSession> st(0);
        MockSession* s = st.get();
        CPPUNIT_ASSERT_EQUAL(s, st[0]);
    }

    void
    test_get()
    {
        FastTransport::SessionTable<MockSession> st(0);
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
        FastTransport::SessionTable<MockSession> st(0);
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
        FastTransport::SessionTable<MockSession> st(0);

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
        CPPUNIT_ASSERT_EQUAL(FastTransport::SessionTable<MockSession>::TAIL,
                             st[2]->nextFree);
    }


  private:
    DISALLOW_COPY_AND_ASSIGN(SessionTableTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(SessionTableTest);

}  // namespace RAMCloud
