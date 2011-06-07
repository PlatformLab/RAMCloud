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

class TransportManagerTest : public ::testing::Test {
  public:
    TransportManagerTest() {}
};

TEST_F(TransportManagerTest, initialize) {
    static struct MockTransportFactory : public TransportFactory {
        MockTransportFactory() : TransportFactory("mock") {}
        Transport* createTransport(const ServiceLocator* local) {
            return new MockTransport();
        }
    } mockTransportFactory;

    // If "mockThrow:" is _not_ in the service locator, that should be
    // caught and the transport ignored. If it is, then any exception
    // is an error and should propagate up.
    static struct MockThrowTransportFactory : public TransportFactory {
        MockThrowTransportFactory() : TransportFactory("mockThrow") {}
        Transport* createTransport(const ServiceLocator* local) {
            throw TransportException(HERE, "boom!");
        }
    } mockThrowTransportFactory;

    TransportManager manager;
    manager.transportFactories.clear();  /* Speeds up initialization. */
    manager.transportFactories.insert(&mockTransportFactory);
    manager.transportFactories.insert(&mockThrowTransportFactory);
    manager.initialize("foo:; mock:; bar:");
    EXPECT_EQ(1U, manager.listening.size());
    EXPECT_TRUE(manager.isServer);
    EXPECT_GT(manager.transports.size(), 0U);

    TransportManager manager2;
    manager2.transportFactories.clear();  /* Speeds up initialization. */
    manager2.transportFactories.insert(&mockThrowTransportFactory);
    EXPECT_THROW(manager2.initialize(
        "foo:; mock:; bar:; mockThrow:"), TransportException);

    TransportManager manager3;
    manager3.transportFactories.clear();  /* Speeds up initialization. */
    manager3.initialize("");
    EXPECT_FALSE(manager3.isServer);
}

TEST_F(TransportManagerTest, getSession) {
    TestLog::Enable _;
    TransportManager manager;
    MockTransport* mock1 = new MockTransport();
    manager.registerMock(mock1);
    mock1->setInput("x");
    EXPECT_THROW(manager.getSession("foo:"),
                            TransportException);
    Transport::SessionRef session(manager.getSession("foo:;mock:"));
    EXPECT_TRUE(session.get() != NULL);
    Transport::SessionRef session2(manager.getSession("foo:;mock:"));
    EXPECT_TRUE(session.get() == session2.get());
    EXPECT_STREQ("", TestLog::get().c_str());

    // Verify that WorkerSessions are created when needed.
    manager.sessionCache.clear();
    manager.isServer = true;
    Transport::SessionRef session3(manager.getSession("mock:"));
    EXPECT_TRUE(session3.get() != NULL);
    EXPECT_STREQ("WorkerSession: created", TestLog::get().c_str());
}

TEST_F(TransportManagerTest, serverRecv) {
    Transport::ServerRpc* rpc;

    TransportManager manager;

    EXPECT_THROW(manager.serverRecv(), TransportException);

    MockTransport* mock1 = new MockTransport();
    MockTransport* mock2 = new MockTransport();
    MockTransport* mock3 = new MockTransport();
    manager.registerMock(mock1);
    manager.registerMock(mock2);
    manager.registerMock(mock3);

    mock3->setInput("y");
    rpc = manager.serverRecv();
    EXPECT_STREQ("y",
        static_cast<const char*>(rpc->recvPayload.getRange(0, 2)));
    delete rpc;

    mock2->setInput("x");
    rpc = manager.serverRecv();
    EXPECT_STREQ("x",
        static_cast<const char*>(rpc->recvPayload.getRange(0, 2)));
    delete rpc;
}

TEST_F(TransportManagerTest, getListeningLocators) {
    TransportManager manager;

    EXPECT_THROW(manager.getListeningLocators(),
        TransportException);

    ServiceLocator mock1sl("hi:");
    ServiceLocator mock2sl("there:");
    MockTransport *mock1 = new MockTransport(&mock1sl);
    MockTransport *mock2 = new MockTransport(&mock2sl);
    manager.registerMock(mock1);
    manager.registerMock(mock2);

    EXPECT_EQ(2U, manager.listening.size());

    ServiceLocatorList sll = manager.getListeningLocators();
    EXPECT_EQ(2U, sll.size());
    EXPECT_STREQ("hi:", sll[0].getOriginalString().c_str());
    EXPECT_STREQ("there:", sll[1].getOriginalString().c_str());
}

TEST_F(TransportManagerTest, getListeningLocatorsString) {
    TransportManager manager;

    EXPECT_THROW(manager.getListeningLocators(),
        TransportException);

    ServiceLocator mock1sl("hi:");
    ServiceLocator mock2sl("there:");
    MockTransport *mock1 = new MockTransport(&mock1sl);
    MockTransport *mock2 = new MockTransport(&mock2sl);

    manager.transportFactories.clear();  /* Speeds up initialization. */
    manager.initialize("");
    EXPECT_STREQ("", manager.getListeningLocatorsString().c_str());

    manager.registerMock(mock1);
    EXPECT_STREQ("hi:", manager.getListeningLocatorsString().c_str());

    manager.registerMock(mock2);
    EXPECT_STREQ("hi:;there:",
        manager.getListeningLocatorsString().c_str());
}

// The following tests both the constructor and the clientSend method.
TEST_F(TransportManagerTest, workerSession) {
    MockTransport transport;
    Buffer request;
    Buffer reply;
    request.fillFromString("abcdefg");
    MockTransport::sessionDeleteCount = 0;

    Transport::Session* wrappedSession = new TransportManager::WorkerSession(
            transport.getSession());

    // Make sure that clientSend gets passed down to the underlying session.
    wrappedSession->clientSend(&request, &reply);
    EXPECT_STREQ("clientSend: abcdefg/0", transport.outputLog.c_str());
    EXPECT_EQ(0U, MockTransport::sessionDeleteCount);

    // Make sure that sessions get cleaned up properly.
    delete wrappedSession;
    EXPECT_EQ(1U, MockTransport::sessionDeleteCount);
}

// The next test makes sure that clientSend synchronizes properly with the
// dispatch thread.

void worker(Transport::SessionRef session) {
    Buffer request;
    Buffer reply;
    request.fillFromString("abcdefg");
    session->clientSend(&request, &reply);
}

TEST_F(TransportManagerTest, workerSessionSyncWithDispatchThread) {
    delete dispatch;
    dispatch = new Dispatch;

    MockTransport transport;
    Transport::SessionRef wrappedSession = new TransportManager::WorkerSession(
            transport.getSession());
    boost::thread child(worker, wrappedSession);

    // Make sure the child hangs in clientSend until we invoke the dispatcher.
    usleep(1000);
    EXPECT_STREQ("", transport.outputLog.c_str());
    for (int i = 0; i < 1000; i++) {
        dispatch->poll();
        if (transport.outputLog.size() > 0) {
            break;
        }
    }
    EXPECT_STREQ("clientSend: abcdefg/0", transport.outputLog.c_str());
    child.join();
}

}  // namespace RAMCloud
