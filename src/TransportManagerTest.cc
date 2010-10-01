/* Copyright (c) 2010 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"
#include "TransportManager.h"
#include "TransportFactory.h"
#include "MockTransport.h"

namespace RAMCloud {

class TransportManagerTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(TransportManagerTest);
    CPPUNIT_TEST(test_initialize);
    CPPUNIT_TEST(test_getSession);
    CPPUNIT_TEST(test_serverRecv);
    CPPUNIT_TEST_SUITE_END();

  public:
    TransportManagerTest() {}

    void test_initialize() {
        static struct MockTransportFactory : public TransportFactory {
            MockTransportFactory() : TransportFactory("mock") {}
            Transport* createTransport(const ServiceLocator* local) {
                return new MockTransport();
            }
        } mockTransportFactory;
        TransportManager manager;
        manager.transportFactories.insert(&mockTransportFactory);
        manager.initialize("foo:; mock:; bar:");
        CPPUNIT_ASSERT_EQUAL(1, manager.listening.size());
        CPPUNIT_ASSERT(manager.transports.size() > 0);
    }

    void test_getSession() {
        TransportManager manager;
        MockTransport* mock1 = new MockTransport();
        manager.registerMock(mock1);
        mock1->setInput("x");
        CPPUNIT_ASSERT_THROW(manager.getSession("foo:"),
                             TransportException);
        Transport::SessionRef session(manager.getSession("foo:;mock:"));
        CPPUNIT_ASSERT(session.get() != NULL);
    }

    void test_serverRecv() {
        Transport::ServerRpc* rpc;

        TransportManager manager;

        CPPUNIT_ASSERT_THROW(manager.serverRecv(),
                             UnrecoverableTransportException);

        MockTransport* mock1 = new MockTransport();
        MockTransport* mock2 = new MockTransport();
        MockTransport* mock3 = new MockTransport();
        manager.registerMock(mock1);
        manager.registerMock(mock2);
        manager.registerMock(mock3);

        mock3->setInput("y");
        rpc = manager.serverRecv();
        CPPUNIT_ASSERT_EQUAL("y",
            static_cast<const char*>(rpc->recvPayload.getRange(0, 2)));
        delete rpc;

        mock2->setInput("x");
        rpc = manager.serverRecv();
        CPPUNIT_ASSERT_EQUAL("x",
            static_cast<const char*>(rpc->recvPayload.getRange(0, 2)));
        delete rpc;
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(TransportManagerTest);

}  // namespace RAMCloud
