/* Copyright (c) 2010-2012 Stanford University
 * Copyright (c) 2011 Facebook
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
#include "BindTransport.h"
#include "MembershipService.h"
#include "MockTransportFactory.h"
#include "MockTransport.h"
#include "ServerList.h"
#include "TransportManager.h"

namespace RAMCloud {

class TransportManagerTest : public ::testing::Test {
  public:
    TransportManagerTest() {}
};

TEST_F(TransportManagerTest, initialize) {
    MockTransportFactory mockTransportFactory(NULL, "mock");
    MockTransportFactory fooTransportFactory(NULL, "foo");

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
    manager.transportFactories.push_back(&mockTransportFactory);
    manager.transportFactories.push_back(&fooTransportFactory);
    manager.transportFactories.push_back(&mockThrowTransportFactory);
    manager.transports.resize(3, NULL);
    manager.initialize("foo:; mock:; bar:; mock:x=14");
    EXPECT_EQ("foo:;mock:;mock:x=14", manager.listeningLocators);
    EXPECT_TRUE(manager.isServer);
    EXPECT_EQ(4U, manager.transports.size());
    Transport* t = manager.transports[3];
    EXPECT_EQ("mock:x=14", t->getServiceLocator());
}

TEST_F(TransportManagerTest, initialize_emptyLocator) {
    TransportManager manager;
    EXPECT_THROW(manager.initialize(""), Exception);
    EXPECT_THROW(manager.initialize("  "), Exception);
}

TEST_F(TransportManagerTest, initialize_transportEmptyLocator) {
    MockTransport* t = new MockTransport();
    t->locatorString = "";
    MockTransportFactory mockTransportFactory(t, "mock");

    TransportManager manager;
    manager.transportFactories.clear();
    manager.transportFactories.push_back(&mockTransportFactory);
    EXPECT_THROW(manager.initialize("mock:"), Exception);
}

TEST_F(TransportManagerTest, initialize_noListeningTransports) {
    TransportManager manager;
    EXPECT_THROW(manager.initialize("rofl:"), Exception);
}

TEST_F(TransportManagerTest, initialize_registerExistingMemory) {
    TestLog::Enable _;
    MockTransportFactory mockTransportFactory(NULL, "mock");
    TransportManager manager;
    manager.transportFactories.clear();  /* Speeds up initialization. */
    manager.transportFactories.push_back(&mockTransportFactory);
    manager.transports.resize(1, NULL);
    manager.registerMemory(reinterpret_cast<void*>(11), 10);
    manager.registerMemory(reinterpret_cast<void*>(22), 20);
    manager.initialize("mock:");
    EXPECT_EQ("registerMemory: register 10 bytes at 11 for mock: | "
              "registerMemory: register 20 bytes at 22 for mock:",
              TestLog::get());
}

TEST_F(TransportManagerTest, getSession_basics) {
    TransportManager manager;
    manager.registerMock(NULL);
    Transport::SessionRef session(manager.getSession("foo:;mock:"));
    EXPECT_TRUE(session.get() != NULL);
    EXPECT_TRUE(manager.transports.back() != NULL);
}

TEST_F(TransportManagerTest, getSession_reuseCachedSession) {
    TransportManager manager;
    manager.registerMock(NULL);
    Transport::SessionRef session(manager.getSession("foo:;mock:"));
    EXPECT_TRUE(session.get() != NULL);
    Transport::SessionRef session2(manager.getSession("foo:;mock:"));
    EXPECT_TRUE(session.get() == session2.get());
}

TEST_F(TransportManagerTest, getSession_registerExistingMemory) {
    TestLog::Enable _;
    TransportManager manager;
    manager.registerMock(NULL, "mock");
    manager.registerMemory(reinterpret_cast<void*>(11), 10);
    manager.registerMemory(reinterpret_cast<void*>(22), 20);
    Transport::SessionRef session(manager.getSession("mock:"));
    EXPECT_TRUE(session.get() != NULL);
    EXPECT_EQ("registerMemory: register 10 bytes at 11 for mock: | "
              "registerMemory: register 20 bytes at 22 for mock:",
              TestLog::get());
}

TEST_F(TransportManagerTest, getSession_ignoreTransportCreateError) {
    TestLog::Enable _;
    TransportManager manager;
    manager.registerMock(NULL, "error");
    manager.registerMock(NULL, "mock");
    Transport::SessionRef session(manager.getSession("error:;mock:"));
    EXPECT_TRUE(session.get() != NULL);
    EXPECT_TRUE(manager.transports.back() != NULL);
    EXPECT_TRUE(manager.transports[manager.transports.size()-2] == NULL);
    EXPECT_EQ("createTransport: exception thrown", TestLog::get());
}

TEST_F(TransportManagerTest, getSession_createWorkerSession) {
    TestLog::Enable _;
    TransportManager manager;
    manager.registerMock(NULL);

    // First session: no need for WorkerSession
    Transport::SessionRef session(manager.getSession("mock:"));
    EXPECT_TRUE(session.get() != NULL);
    EXPECT_STREQ("", TestLog::get().c_str());

    // Second session: need a WorkerSession.
    manager.sessionCache.clear();
    manager.isServer = true;
    Transport::SessionRef session2(manager.getSession("mock:"));
    EXPECT_TRUE(session2.get() != NULL);
    EXPECT_EQ("WorkerSession: created", TestLog::get());
}

TEST_F(TransportManagerTest, getSession_badTransportFailure) {
    TransportManager manager;
    manager.registerMock(NULL);
    EXPECT_THROW(manager.getSession("foo:"), TransportException);
    try {
        manager.getSession("foo:");
    } catch (TransportException& e) {
        EXPECT_EQ(e.message, "No supported transport found for "
                             "this service locator: foo:");
    }
}

TEST_F(TransportManagerTest, getSession_transportgetSessionFailure) {
    TransportManager manager;
    manager.registerMock(NULL);
    EXPECT_THROW(manager.getSession("mock:host=error"), TransportException);
    try {
        manager.getSession("mock:host=error");
    } catch (TransportException& e) {
        EXPECT_TRUE(TestUtil::matchesPosixRegex(
            "Could not obtain transport session for this service locator: "
            "mock:host=error (details: RAMCloud::TransportException: "
            "Failed to open session thrown at .*)",
            e.message));
    }
}

TEST_F(TransportManagerTest, getSession_matchServerId) {
    TestLog::Enable _;

    ServerId id(1, 53);
    ServerList list;
    BindTransport transport;
    Context::get().transportManager->registerMock(&transport);
    MembershipService membership(id, list);
    transport.addService(membership, "mock:host=member", MEMBERSHIP_SERVICE);

    EXPECT_NO_THROW(Context::get().transportManager->getSession(
        "mock:host=member", id));
    EXPECT_THROW(Context::get().transportManager->getSession("mock:host=member",
        ServerId(1, 52)), TransportException);
    EXPECT_NE(string::npos, TestLog::get().find("getSession: Expected ServerId "
        "223338299393 at \"mock:host=member\", but actual server id was "
        "227633266689!"));

    Context::get().transportManager->unregisterMock();
}

// No tests for getListeningLocatorsString: it's trivial.

TEST_F(TransportManagerTest, registerMemory) {
    TestLog::Enable _;
    TransportManager manager;
    ServiceLocator s1("mock1:");
    MockTransport t1(&s1);
    manager.registerMock(&t1, "mock1");
    ServiceLocator s2("mock2:");
    MockTransport t2(&s2);
    manager.registerMock(&t2, "mock2");
    Transport::SessionRef session1(manager.getSession("mock1:"));
    EXPECT_EQ("", TestLog::get());
    manager.registerMemory(reinterpret_cast<void*>(11), 10);
    EXPECT_EQ("registerMemory: register 10 bytes at 11 for mock1:",
              TestLog::get());
    TestLog::reset();
    Transport::SessionRef session2(manager.getSession("mock2:"));
    EXPECT_EQ("registerMemory: register 10 bytes at 11 for mock2:",
              TestLog::get());
    TestLog::reset();
    manager.registerMemory(reinterpret_cast<void*>(22), 20);
    EXPECT_EQ("registerMemory: register 20 bytes at 22 for mock1: | "
              "registerMemory: register 20 bytes at 22 for mock2:",
              TestLog::get());
    manager.unregisterMock();
    manager.unregisterMock();
}

}  // namespace RAMCloud
