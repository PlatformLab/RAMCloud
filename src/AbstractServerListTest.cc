/* Copyright (c) 2012 Stanford University
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

#include <queue>

#include "TestUtil.h"       //Has to be first, compiler complains
#include "AbstractServerList.h"
#include "FailSession.h"
#include "ServerTracker.h"
#include "ShortMacros.h"

namespace RAMCloud {

namespace __AbstractServerListTest__ {
static std::queue<ServerTracker<int>::ServerChange> changes;

struct MockServerTracker : public ServerTracker<int> {
    explicit MockServerTracker(Context& context) : ServerTracker<int>(context)
    {
    }
    void enqueueChange(const ServerDetails& server, ServerChangeEvent event)
    {
        __AbstractServerListTest__::changes.push({server, event});
    }
    void fireCallback() {}
};

class AbstractServerListSubClass : public AbstractServerList {
  PUBLIC:
    std::vector<ServerDetails> servers;

    explicit AbstractServerListSubClass(Context& context)
        : AbstractServerList(context)
        , servers()
    {
    }

    ServerDetails*
    iget(ServerId id)
    {
        uint32_t index = id.indexNumber();
        if (index < servers.size()) {
            ServerDetails* details = &servers[index];
            if (details->serverId == id)
                return details;
        }
        return NULL;
    }

    ServerDetails*
    iget(uint32_t index) {
        return &(servers.at(index));
    }

    size_t
    isize() const {
        return servers.size();
    }

    ServerId&
    add(string locator, ServerStatus status) {
        ServerId* id = new ServerId(isize(), 0);

        ServerDetails sd;
        sd.serverId = *id;
        sd.status = status;
        sd.serviceLocator = locator;
        servers.push_back(sd);
        return *id;
    }

    void
    remove(ServerId id) {
        if (isUp(id))
            crashed(id);

        foreach (ServerTrackerInterface* tracker, trackers) {
            tracker->enqueueChange(
                ServerDetails(id, ServerStatus::DOWN),
                ServerChangeEvent::SERVER_REMOVED);
        }

        servers.erase(servers.begin() + id.indexNumber());
    }

    void
    crashed(ServerId id) {
        servers[id.indexNumber()].status = ServerStatus::CRASHED;
        foreach (ServerTrackerInterface* tracker, trackers) {
            tracker->enqueueChange(
                ServerDetails(id, ServerStatus::CRASHED),
                ServerChangeEvent::SERVER_CRASHED);
        }
    }

};

class AbstractServerListTest : public ::testing::Test {
  public:
    Context context;
    AbstractServerListSubClass sl;
    MockServerTracker tr;

    AbstractServerListTest()
        : context()
        , sl(context)
        , tr(context)
    {
        while (!changes.empty())
            changes.pop();
    }

};

TEST_F(AbstractServerListTest, constructor) {
    Context context;
    AbstractServerListSubClass sl(context);
    EXPECT_EQ(0UL, sl.getVersion());
}

TEST_F(AbstractServerListTest, destructor) {
    auto* sl = new AbstractServerListSubClass(context);
    MockServerTracker tr(context);
    EXPECT_EQ(sl, tr.parent);

    delete sl;
    EXPECT_EQ(static_cast<AbstractServerList*>(NULL), tr.parent);
}

TEST_F(AbstractServerListTest, getLocator) {
    EXPECT_THROW(sl.getLocator(ServerId(1, 0)), ServerListException);
    ServerId& id = sl.add("mock::1", ServerStatus::UP);
    EXPECT_THROW(sl.getLocator(ServerId(2, 0)), ServerListException);
    EXPECT_STREQ("mock::1", sl.getLocator(id));
}

TEST_F(AbstractServerListTest, isUp) {
    EXPECT_FALSE(sl.isUp(ServerId(1, 0)));
    ServerId& id1 = sl.add("mock::2", ServerStatus::UP);
    ServerId& id2 = sl.add("mock::3", ServerStatus::DOWN);
    EXPECT_TRUE(sl.iget(id1) != NULL);
    EXPECT_TRUE(sl.iget(id2) != NULL);
    EXPECT_TRUE(sl.isUp(id1));
    EXPECT_FALSE(sl.isUp(ServerId(1, 2)));
    EXPECT_FALSE(sl.isUp(ServerId(2, 0)));
    sl.crashed(id1);
    EXPECT_FALSE(sl.isUp(id1));
    EXPECT_FALSE(sl.isUp(id2));
}

TEST_F(AbstractServerListTest, getSession_basics) {
    sl.skipServerIdCheck = true;
    MockTransport transport(context);
    context.transportManager->registerMock(&transport);

    ServerId& id1 = sl.add("mock:id=1", ServerStatus::UP);
    ServerId& id2 = sl.add("mock:id=2", ServerStatus::UP);
    Transport::SessionRef session1 = sl.getSession(id1);
    EXPECT_EQ("mock:id=1", session1->getServiceLocator());
    Transport::SessionRef session2 = sl.getSession(id2);
    EXPECT_EQ("mock:id=2", session2->getServiceLocator());
    Transport::SessionRef session3 = sl.getSession(id1);
    EXPECT_EQ(session1, session3);
    context.transportManager->unregisterMock();
}
TEST_F(AbstractServerListTest, getSession_bogusId) {
    EXPECT_EQ("fail:", sl.getSession({9999, 22})->getServiceLocator());
}
TEST_F(AbstractServerListTest, getSession_serverIdDoesntMatch) {
    TestLog::Enable _;
    MockTransport transport(context);
    context.transportManager->registerMock(&transport);
    sl.skipServerIdCheck = false;
    transport.setInput("0 6 7");
    ServerId& id1 = sl.add("mock:id=1", ServerStatus::UP);
    EXPECT_EQ("fail:", sl.getSession(id1)->getServiceLocator());
    EXPECT_EQ("getSession: Expected ServerId 0 for \"mock:id=1\", but "
            "actual server id was 30064771078", TestLog::get());
    context.transportManager->unregisterMock();
}
TEST_F(AbstractServerListTest, getSession_transportErrorCheckingId) {
    TestLog::Enable _;
    MockTransport transport(context);
    context.transportManager->registerMock(&transport);
    sl.skipServerIdCheck = false;
    transport.setInput(NULL);
    ServerId& id1 = sl.add("mock:id=1", ServerStatus::UP);
    EXPECT_EQ("fail:", sl.getSession(id1)->getServiceLocator());
    EXPECT_EQ("getSession: Failed to obtain ServerId from \"mock:id=1\": "
            "RAMCloud::TransportException:  thrown at GetServerIdRpc::wait "
            "at src/MembershipClient.cc:88", TestLog::get());
    context.transportManager->unregisterMock();
}
TEST_F(AbstractServerListTest, getSession_successfulServerIdCheck) {
    TestLog::Enable _;
    MockTransport transport(context);
    context.transportManager->registerMock(&transport);
    sl.skipServerIdCheck = false;
    transport.setInput("0 0 0");
    ServerId& id1 = sl.add("mock:id=1", ServerStatus::UP);
    EXPECT_EQ("mock:id=1", sl.getSession(id1)->getServiceLocator());
    context.transportManager->unregisterMock();
}

// Helper function for the following tests; invokes getSession in a
// separate thread and returns the result.
static void testGetSessionThread(AbstractServerListSubClass* sl,
        ServerId id, Transport::SessionRef* result) {
    *result = sl->getSession(id);
}

TEST_F(AbstractServerListTest, getSession_serverDisappearsDuringCheck) {
    TestLog::Enable _;
    MockTransport transport(context);
    context.transportManager->registerMock(&transport);
    sl.skipServerIdCheck = false;
    ServerId& id1 = sl.add("mock:id=1", ServerStatus::UP);
    Transport::SessionRef session(NULL);
    std::thread thread(testGetSessionThread, &sl, id1, &session);

    // Wait for the RPC to get underway.
    for (int i = 0; i < 1000; i++) {
        if (transport.lastNotifier != NULL)
            break;
        usleep(100);
    }
    EXPECT_TRUE(transport.lastNotifier != NULL);

    // Delete the server for which a session is being generated, then
    // allow the RPC to complete.
    sl.remove(id1);
    RpcWrapper* wrapper = static_cast<RpcWrapper*>(transport.lastNotifier);
    wrapper->response->fillFromString("0 0 0");
    transport.lastNotifier->completed();

    // Wait for completion.
    for (int i = 0; i < 1000; i++) {
        if (session != NULL)
            break;
        usleep(100);
    }
    EXPECT_EQ("fail:", session->getServiceLocator());
    thread.join();
    context.transportManager->unregisterMock();
}
TEST_F(AbstractServerListTest, getSession_sessionSetDuringCheck) {
    TestLog::Enable _;
    MockTransport transport(context);
    context.transportManager->registerMock(&transport);
    sl.skipServerIdCheck = false;
    ServerId& id1 = sl.add("mock:id=1", ServerStatus::UP);
    Transport::SessionRef session = transport.getSession();
    Transport::SessionRef result(NULL);
    std::thread thread(testGetSessionThread, &sl, id1, &result);

    // Wait for the RPC to get underway.
    for (int i = 0; i < 1000; i++) {
        if (transport.lastNotifier != NULL)
            break;
        usleep(100);
    }
    EXPECT_TRUE(transport.lastNotifier != NULL);

    // Fill in the session for the given server, then allow the RPC to complete.
    sl.iget(id1.indexNumber())->session = session;
    RpcWrapper* wrapper = static_cast<RpcWrapper*>(transport.lastNotifier);
    wrapper->response->fillFromString("0 0 0");
    transport.lastNotifier->completed();

    // Wait for completion.
    for (int i = 0; i < 1000; i++) {
        if (result != NULL)
            break;
        usleep(100);
    }
    EXPECT_TRUE(result != NULL);
    EXPECT_EQ("test:", result->getServiceLocator());
    thread.join();
    context.transportManager->unregisterMock();
}

TEST_F(AbstractServerListTest, flushSession) {
    MockTransport transport(context);
    context.transportManager->registerMock(&transport);

    ServerId& id = sl.add("mock:id=1", ServerStatus::UP);
    Transport::SessionRef session1 = sl.getSession(id);
    EXPECT_EQ("mock:id=1", session1->getServiceLocator());
    sl.flushSession({999, 999});
    sl.flushSession(id);
    Transport::SessionRef session2 = sl.getSession(id);
    EXPECT_EQ("mock:id=1", session2->getServiceLocator());
    EXPECT_NE(session1, session2);
    context.transportManager->unregisterMock();
}

TEST_F(AbstractServerListTest, contains) {
    EXPECT_FALSE(sl.contains(ServerId(0, 0)));
    EXPECT_FALSE(sl.contains(ServerId(1, 0)));

    ServerId& id1 = sl.add("mock::4", ServerStatus::DOWN);
    ServerId& id2 = sl.add("mock::5", ServerStatus::UP);

    EXPECT_TRUE(sl.contains(id1));
    EXPECT_TRUE(sl.contains(id2));
    sl.crashed(id1);
    EXPECT_TRUE(sl.contains(id1));
    EXPECT_FALSE(sl.contains(ServerId(1, 2)));
    EXPECT_FALSE(sl.contains(ServerId(2, 0)));
}

TEST_F(AbstractServerListTest, registerTracker) {
    sl.unregisterTracker(tr);
    EXPECT_EQ(0U, sl.trackers.size());
    sl.registerTracker(tr);
    EXPECT_EQ(1U, sl.trackers.size());
    EXPECT_EQ(&tr, sl.trackers[0]);
    EXPECT_THROW(sl.registerTracker(tr), Exception);
}

TEST_F(AbstractServerListTest, registerTracker_duringDestruction) {
    sl.unregisterTracker(tr);
    sl.isBeingDestroyed = true;
    EXPECT_THROW(sl.registerTracker(tr), ServerListException);
    EXPECT_EQ(0U, sl.trackers.size());
}

TEST_F(AbstractServerListTest, registerTracker_pushAdds) {
    sl.unregisterTracker(tr);
    ServerId& id1 = sl.add("mock:", ServerStatus::UP);
    ServerId& id2 = sl.add("mock:", ServerStatus::UP);
    ServerId& id3 = sl.add("mock:", ServerStatus::UP);
    ServerId& id4 = sl.add("mock:", ServerStatus::UP);

    sl.crashed(id4);
    sl.remove(id2);
    sl.registerTracker(tr);

    // Should be serverId4 up/crashed first, then in order,
    // but missing serverId2
    EXPECT_EQ(4U, changes.size());
    EXPECT_EQ(id4, changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, changes.front().event);
    changes.pop();
    EXPECT_EQ(id4, changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_CRASHED, changes.front().event);
    changes.pop();
    EXPECT_EQ(id1, changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, changes.front().event);
    changes.pop();
    EXPECT_EQ(id3, changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, changes.front().event);
    changes.pop();
}

TEST_F(AbstractServerListTest, unregisterTracker) {
    sl.unregisterTracker(tr);
    EXPECT_EQ(0U, sl.trackers.size());

    sl.registerTracker(tr);
    EXPECT_EQ(1U, sl.trackers.size());

    sl.unregisterTracker(tr);
    EXPECT_EQ(0U, sl.trackers.size());
}

TEST_F(AbstractServerListTest, unregisterTracker_duringDestruction) {
    sl.isBeingDestroyed = true;

    // Test to see if it really leaves queue untouched.
    sl.unregisterTracker(tr);
    EXPECT_EQ(1U, sl.trackers.size());

    // Unregister for reals otherwise there'd be an error.
    sl.isBeingDestroyed = false;
    sl.unregisterTracker(tr);
}

TEST_F(AbstractServerListTest, getVersion) {
    EXPECT_EQ(0UL, sl.getVersion());
    sl.version = 0xDEADBEEFCAFEBABEUL;
    EXPECT_EQ(0xDEADBEEFCAFEBABEUL, sl.getVersion());
}

TEST_F(AbstractServerListTest, size) {
    for (int n = 0; n < 22; n++)
        sl.add("Hasta La Vista, Baby.", ServerStatus::DOWN);

    EXPECT_EQ(22UL, sl.size());

    for (int n = 0; n < 13; n++)
        sl.add("Welcome to... JURASSIC PARK! *theme*", ServerStatus::UP);

    EXPECT_EQ(35UL, sl.size());
}

TEST_F(AbstractServerListTest, toString) {
    EXPECT_EQ("server 1 at (locator unavailable)",
              sl.toString(ServerId(1)));
    ServerId& id = sl.add("mock:service=locator", ServerStatus::UP);
    EXPECT_EQ("server 0 at mock:service=locator",
              sl.toString(id));
}

TEST_F(AbstractServerListTest, toString_status) {
    EXPECT_EQ("UP", ServerList::toString(ServerStatus::UP));
    EXPECT_EQ("CRASHED", ServerList::toString(ServerStatus::CRASHED));
    EXPECT_EQ("DOWN", ServerList::toString(ServerStatus::DOWN));
}

TEST_F(AbstractServerListTest, toString_all) {
    EXPECT_EQ("", sl.toString());
    sl.add("locator 1", ServerStatus::CRASHED);
    sl.servers.back().services = {WireFormat::MASTER_SERVICE};
    EXPECT_EQ(
        "server 0 at locator 1 with MASTER_SERVICE is CRASHED\n",
        sl.toString());
    sl.add("locatez twoz", ServerStatus::UP);
    sl.servers.back().services = {WireFormat::BACKUP_SERVICE};
    EXPECT_EQ(
        "server 0 at locator 1 with MASTER_SERVICE is CRASHED\n"
        "server 1 at locatez twoz with BACKUP_SERVICE is UP\n",
        sl.toString());
}


} /// namespace __AbstractServerListTest__
} /// namespace RAMCloud
