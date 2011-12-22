/* Copyright (c) 2011 Stanford University
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

#include <queue>

#include "TestUtil.h"
#include "ServerList.h"
#include "ServerTracker.h"

namespace RAMCloud {

static std::queue<ServerTracker<int>::ServerChange> changes;

class MockServerTracker : public ServerTrackerInterface {
    void
    enqueueChange(ServerId serverId, ServerChangeEvent event)
    {
        changes.push({serverId, event});
    }
};

class ServerListTest : public ::testing::Test {
  public:
    ServerListTest()
        : sl(),
          tr()
    {
    }

    ServerList sl;
    MockServerTracker tr;

    DISALLOW_COPY_AND_ASSIGN(ServerListTest);
};

static bool
addFilter(string s)
{
    return (s == "add");
}

TEST_F(ServerListTest, add) {
    sl.registerTracker(tr);
    TestLog::Enable _(&addFilter);

    sl.add(ServerId(ServerId(/*invalid id*/)), ServiceLocator("mock:"));
    EXPECT_EQ("add: Ignoring addition of invalid ServerId.", TestLog::get());
    TestLog::reset();

    EXPECT_EQ(0U, sl.serverList.size());
    sl.add(ServerId(57, 1), ServiceLocator("mock:"));
    EXPECT_EQ(58U, sl.serverList.size());
    EXPECT_EQ(ServerId(57, 1), sl.serverList[57]->serverId);
    EXPECT_EQ(1U, changes.size());
    EXPECT_EQ(ServerId(57, 1), changes.front().serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, changes.front().event);
    changes.pop();

    // Duplicate ADD
    sl.add(ServerId(57, 1), ServiceLocator("mock:"));
    EXPECT_EQ("add: Duplicate add of ServerId 4294967353!", TestLog::get());
    TestLog::reset();
    EXPECT_EQ(0U, changes.size());

    // ADD of older ServerId
    sl.add(ServerId(57, 0), ServiceLocator("mock:"));
    EXPECT_EQ("add: Dropping addition of ServerId older than the current entry "
        "(57 < 4294967353)!", TestLog::get());
    TestLog::reset();
    EXPECT_EQ(0U, changes.size());

    // ADD before previous REMOVE
    sl.add(ServerId(57, 2), ServiceLocator("mock:"));
    EXPECT_EQ("add: Addition of 8589934649 seen before removal of 4294967353! "
        "Issuing removal before addition.", TestLog::get());
    TestLog::reset();
    EXPECT_EQ(2U, changes.size());
    EXPECT_EQ(ServerId(57, 1), changes.front().serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(57, 2), changes.front().serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, changes.front().event);
    changes.pop();
}

static bool
removeFilter(string s)
{
    return (s == "remove");
}

TEST_F(ServerListTest, remove) {
    sl.registerTracker(tr);
    TestLog::Enable _(&removeFilter);

    sl.remove(ServerId(/* invalid id */));
    EXPECT_EQ("remove: Ignoring removal of invalid ServerId.", TestLog::get());
    TestLog::reset();

    EXPECT_EQ(0U, sl.serverList.size());
    sl.remove(ServerId(0, 0));
    sl.add(ServerId(1, 1), ServiceLocator("mock:"));
    changes.pop();
    EXPECT_EQ(2U, sl.serverList.size());
    sl.remove(ServerId(0, 0));
    sl.remove(ServerId(1, 0));

    EXPECT_EQ("remove: Ignoring removal of unknown ServerId 0 | "
        "remove: Ignoring removal of unknown ServerId 0 | "
        "remove: Ignoring removal of unknown ServerId 1",
        TestLog::get());
    TestLog::reset();

    // Exact match.
    sl.remove(ServerId(1, 1));
    EXPECT_FALSE(sl.serverList[1]);
    EXPECT_EQ(1U, changes.size());
    EXPECT_EQ(ServerId(1, 1), changes.front().serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, changes.front().event);
    changes.pop();

    // Newer one.
    sl.add(ServerId(1, 1), ServiceLocator("mock:"));
    changes.pop();
    sl.remove(ServerId(1, 2));
    EXPECT_EQ("remove: Removing ServerId 4294967297 because removal for a "
        "newer generation number was received (8589934593)", TestLog::get());
    TestLog::reset();
    EXPECT_FALSE(sl.serverList[1]);
    EXPECT_EQ(1U, changes.size());
    EXPECT_EQ(ServerId(1, 1), changes.front().serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, changes.front().event);
    changes.pop();
}

TEST_F(ServerListTest, getLocator) {
    EXPECT_THROW(sl.getLocator(ServerId(1, 0)), ServerListException);
    sl.add(ServerId(1, 0), ServiceLocator("mock:"));
    EXPECT_THROW(sl.getLocator(ServerId(2, 0)), ServerListException);
    EXPECT_EQ("mock:", sl.getLocator(ServerId(1, 0)));
}

TEST_F(ServerListTest, size) {
    EXPECT_EQ(sl.serverList.size(), sl.size());
    sl.add(ServerId(572, 0), ServiceLocator("mock:"));
    EXPECT_EQ(573U, sl.size());
}

TEST_F(ServerListTest, indexOperator) {
    EXPECT_FALSE(sl[0].isValid());
    EXPECT_FALSE(sl[183742].isValid());
    sl.add(ServerId(7572, 2734), ServiceLocator("mock:"));
    EXPECT_EQ(ServerId(7572, 2734), sl[7572]);
    sl.remove(ServerId(7572, 2734));
    EXPECT_FALSE(sl[7572].isValid());
}

TEST_F(ServerListTest, contains) {
    EXPECT_FALSE(sl.contains(ServerId(0, 0)));
    EXPECT_FALSE(sl.contains(ServerId(1, 0)));
    sl.add(ServerId(1, 0), ServiceLocator("mock:"));
    EXPECT_TRUE(sl.contains(ServerId(1, 0)));
    sl.remove(ServerId(1, 0));
    EXPECT_FALSE(sl.contains(ServerId(1, 0)));
}

TEST_F(ServerListTest, registerTracker) {
    sl.registerTracker(tr);
    EXPECT_EQ(1U, sl.trackers.size());
    EXPECT_EQ(&tr, sl.trackers[0]);
    EXPECT_THROW(sl.registerTracker(tr), Exception);
}

TEST_F(ServerListTest, registerTracker_pushAdds) {
    sl.add(ServerId(1, 2), ServiceLocator("mock:"));
    sl.add(ServerId(2, 3), ServiceLocator("mock:"));
    sl.add(ServerId(0, 1), ServiceLocator("mock:"));
    sl.add(ServerId(3, 4), ServiceLocator("mock:"));
    sl.remove(ServerId(2, 3));
    sl.registerTracker(tr);

    // Should be in order, but missing (2, 3)
    EXPECT_EQ(3U, changes.size());
    EXPECT_EQ(ServerId(0, 1), changes.front().serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(1, 2), changes.front().serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(3, 4), changes.front().serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, changes.front().event);
    changes.pop();
}

TEST_F(ServerListTest, unregisterTracker) {
    EXPECT_EQ(0U, sl.trackers.size());

    sl.unregisterTracker(tr);
    EXPECT_EQ(0U, sl.trackers.size());

    sl.registerTracker(tr);
    EXPECT_EQ(1U, sl.trackers.size());

    sl.unregisterTracker(tr);
    EXPECT_EQ(0U, sl.trackers.size());
}

}  // namespace RAMCloud
