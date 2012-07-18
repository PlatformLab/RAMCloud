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
#include "ServerTracker.h"
#include "ShortMacros.h"

namespace RAMCloud {

namespace {
static std::queue<ServerTracker<int>::ServerChange> changes;

class MockServerTracker : public ServerTrackerInterface {
    void
    enqueueChange(const ServerDetails& server, ServerChangeEvent event)
    {
        changes.push({server, event});
    }
    void fireCallback() {}
};
}

class AbstractServerListSubClass : public AbstractServerList {
  PUBLIC:
    std::vector<ServerDetails> servers;

    explicit AbstractServerListSubClass(Context& context)
        : AbstractServerList(context)
        , servers()
    {
    }

    ServerDetails*
    iget(size_t index) {
        return &(servers.at(index));
    }

    bool
    icontains(ServerId id) const {
        uint32_t index = id.indexNumber();

        return  index < servers.size() &&
                servers.at(index).serverId == id;
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
        , tr()
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
    EXPECT_TRUE(sl.icontains(id1));
    EXPECT_TRUE(sl.icontains(id2));
    EXPECT_TRUE(sl.isUp(id1));
    EXPECT_FALSE(sl.isUp(ServerId(1, 2)));
    EXPECT_FALSE(sl.isUp(ServerId(2, 0)));
    sl.crashed(id1);
    EXPECT_FALSE(sl.isUp(id1));
    EXPECT_FALSE(sl.isUp(id2));
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
    sl.registerTracker(tr);
    EXPECT_EQ(1U, sl.trackers.size());
    EXPECT_EQ(&tr, sl.trackers[0]);
    EXPECT_THROW(sl.registerTracker(tr), Exception);
}

TEST_F(AbstractServerListTest, registerTracker_pushAdds) {
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
    EXPECT_EQ(0U, sl.trackers.size());

    sl.unregisterTracker(tr);
    EXPECT_EQ(0U, sl.trackers.size());

    sl.registerTracker(tr);
    EXPECT_EQ(1U, sl.trackers.size());

    sl.unregisterTracker(tr);
    EXPECT_EQ(0U, sl.trackers.size());
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
    sl.servers.back().services = {MASTER_SERVICE};
    EXPECT_EQ(
        "server 0 at locator 1 with MASTER_SERVICE is CRASHED\n",
        sl.toString());
    sl.add("locatez twoz", ServerStatus::UP);
    sl.servers.back().services = {BACKUP_SERVICE};
    EXPECT_EQ(
        "server 0 at locator 1 with MASTER_SERVICE is CRASHED\n"
        "server 1 at locatez twoz with BACKUP_SERVICE is UP\n",
        sl.toString());
}

} /// namespace RAMCloud
