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

#include "TestUtil.h"
#include "TestLog.h"
#include "ServerList.h"
#include "ServerTracker.h"

namespace RAMCloud {

static int callbacksFired = 0;

static void
callback()
{
    callbacksFired++;
}

class ServerTrackerTest : public ::testing::Test {
  public:
    ServerTrackerTest()
        : sl(),
          tr(sl),
          trcb(sl, callback)
    {
    }

    ServerList sl;
    ServerTracker<int> tr;
    ServerTracker<int> trcb;

    DISALLOW_COPY_AND_ASSIGN(ServerTrackerTest);
};

TEST_F(ServerTrackerTest, constructors) {
    EXPECT_EQ(0U, tr.serverList.size());
    EXPECT_FALSE(tr.changes.areChanges());
    EXPECT_FALSE(tr.eventCallback);
    EXPECT_EQ(static_cast<uint32_t>(-1), tr.lastRemovedIndex);

    EXPECT_EQ(0U, trcb.serverList.size());
    EXPECT_FALSE(trcb.changes.areChanges());
    EXPECT_TRUE(trcb.eventCallback);
    EXPECT_TRUE(callback == *(trcb.eventCallback));
    EXPECT_EQ(static_cast<uint32_t>(-1), trcb.lastRemovedIndex);
}

TEST_F(ServerTrackerTest, enqueueChange) {
    EXPECT_EQ(0U, tr.serverList.size());
    EXPECT_EQ(0U, tr.changes.changes.size());
    tr.enqueueChange(ServerId(2, 0), ServerChangeEvent::SERVER_ADDED);
    EXPECT_EQ(3U, tr.serverList.size());
    EXPECT_EQ(1U, tr.changes.changes.size());

    trcb.enqueueChange(ServerId(0, 0), ServerChangeEvent::SERVER_ADDED);
    EXPECT_EQ(1, callbacksFired);
    trcb.enqueueChange(ServerId(0, 0), ServerChangeEvent::SERVER_REMOVED);
    EXPECT_EQ(2, callbacksFired);

    // Ensure nothing was actually added to the lists.
    for (size_t i = 0; i < tr.serverList.size(); i++) {
        EXPECT_FALSE(tr.serverList[i].serverId.isValid());
        EXPECT_TRUE(tr.serverList[i].pointer == NULL);
    }
    for (size_t i = 0; i < trcb.serverList.size(); i++) {
        EXPECT_FALSE(trcb.serverList[i].serverId.isValid());
        EXPECT_TRUE(trcb.serverList[i].pointer == NULL);
    }
}

TEST_F(ServerTrackerTest, areChanges) {
    EXPECT_FALSE(tr.areChanges());
    tr.enqueueChange(ServerId(2, 0), ServerChangeEvent::SERVER_ADDED);
    EXPECT_TRUE(tr.areChanges());
}

static bool
getChangeFilter(string s)
{
    return (s == "getChange");
}

TEST_F(ServerTrackerTest, getChange) {
    TestLog::Enable _(&getChangeFilter);
    ServerId id;
    ServerChangeEvent event;

    // Add
    EXPECT_FALSE(tr.getChange(id, event));
    EXPECT_EQ(0U, tr.serverList.size());
    tr.enqueueChange(ServerId(2, 0), ServerChangeEvent::SERVER_ADDED);
    EXPECT_EQ(3U, tr.serverList.size());
    EXPECT_FALSE(tr.serverList[2].serverId.isValid());
    EXPECT_TRUE(tr.serverList[2].pointer == NULL);
    EXPECT_TRUE(tr.getChange(id, event));
    EXPECT_EQ(ServerId(2, 0), id);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, event);
    EXPECT_FALSE(tr.getChange(id, event));
    EXPECT_EQ(ServerId(2, 0), tr.serverList[2].serverId);
    EXPECT_TRUE(tr.serverList[2].pointer == NULL);

    // Remove
    tr[ServerId(2, 0)] = reinterpret_cast<int*>(57);
    tr.enqueueChange(ServerId(2, 0), ServerChangeEvent::SERVER_REMOVED);
    EXPECT_EQ(reinterpret_cast<void*>(57), tr[ServerId(2, 0)]);
    EXPECT_TRUE(tr.getChange(id, event));
    EXPECT_EQ(ServerId(2, 0), id);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, event);
    EXPECT_EQ(2U, tr.lastRemovedIndex);
    tr.testing_avoidGetChangeAssertion = true;
    EXPECT_FALSE(tr.getChange(id, event));
    EXPECT_EQ("getChange: User of this ServerTracker did not NULL out previous "
        "pointer for index 2 (ServerId 2)!", TestLog::get());
    EXPECT_FALSE(tr.serverList[2].serverId.isValid());
    EXPECT_TRUE(tr.serverList[2].pointer == NULL);
    EXPECT_EQ(static_cast<uint32_t>(-1), tr.lastRemovedIndex);
}

TEST_F(ServerTrackerTest, getRandomServerId) {
    ServerId id;
    ServerChangeEvent event;

    EXPECT_FALSE(tr.getRandomServerId().isValid());
    tr.enqueueChange(ServerId(0, 1), ServerChangeEvent::SERVER_ADDED);
    EXPECT_FALSE(tr.getRandomServerId().isValid());

    EXPECT_TRUE(tr.getChange(id, event));
    for (int i = 0; i < 10; i++)
        EXPECT_EQ(ServerId(0, 1), tr.getRandomServerId());

    tr.enqueueChange(ServerId(1, 1), ServerChangeEvent::SERVER_ADDED);

    EXPECT_TRUE(tr.getChange(id, event));
    bool firstSeen = false;
    bool secondSeen = false;
    for (int i = 0; i < 100; i++) {
        ServerId id = tr.getRandomServerId();
        EXPECT_TRUE(id == ServerId(0, 1) || id == ServerId(1, 1));
        if (id == ServerId(0, 1)) firstSeen = true;
        if (id == ServerId(1, 1)) secondSeen = true;
    }
    EXPECT_TRUE(firstSeen);
    EXPECT_TRUE(secondSeen);

    // Ensure looping over empty list terminates.
    tr.enqueueChange(ServerId(0, 1), ServerChangeEvent::SERVER_REMOVED);
    tr.enqueueChange(ServerId(1, 1), ServerChangeEvent::SERVER_REMOVED);
    EXPECT_TRUE(tr.getChange(id, event));
    EXPECT_TRUE(tr.getChange(id, event));
    EXPECT_FALSE(tr.getRandomServerId().isValid());
}

TEST_F(ServerTrackerTest, indexOperator) {
    TestLog::Enable _; // suck up getChange WARNING
    ServerId id;
    ServerChangeEvent event;

    EXPECT_THROW(tr[ServerId(0, 0)], Exception);

    tr.enqueueChange(ServerId(0, 0), ServerChangeEvent::SERVER_ADDED);
    EXPECT_TRUE(tr.getChange(id, event));
    tr[ServerId(0, 0)] = reinterpret_cast<int*>(45);
    EXPECT_THROW(tr[ServerId(0, 1)], Exception);
    EXPECT_EQ(reinterpret_cast<int*>(45), tr[ServerId(0, 0)]);
    EXPECT_THROW(tr[ServerId(0, 1)], Exception);

    tr.enqueueChange(ServerId(0, 0), ServerChangeEvent::SERVER_REMOVED);
    EXPECT_TRUE(tr.getChange(id, event));
    EXPECT_NO_THROW(tr[ServerId(0, 0)]);
    EXPECT_NE(static_cast<int*>(NULL), tr.serverList[0].pointer);
    tr.testing_avoidGetChangeAssertion = true;
    EXPECT_FALSE(tr.getChange(id, event));
    EXPECT_THROW(tr[ServerId(0, 0)], Exception);
    EXPECT_EQ(static_cast<int*>(NULL), tr.serverList[0].pointer);
}

TEST_F(ServerTrackerTest, size) {
    ServerId id;
    ServerChangeEvent event;

    EXPECT_EQ(0U, tr.size());
    tr.enqueueChange(ServerId(0, 0), ServerChangeEvent::SERVER_ADDED);
    EXPECT_EQ(0U, tr.size());
    tr.getChange(id, event);
    EXPECT_EQ(1U, tr.size());

    tr.enqueueChange(ServerId(0, 0), ServerChangeEvent::SERVER_REMOVED);
    EXPECT_EQ(1U, tr.size());
    tr.getChange(id, event);
    EXPECT_EQ(0U, tr.size());
}

TEST_F(ServerTrackerTest, ChangeQueue_addChange) {
    EXPECT_EQ(0U, tr.changes.changes.size());
    tr.changes.addChange(ServerId(5, 4), ServerChangeEvent::SERVER_ADDED);
    EXPECT_EQ(1U, tr.changes.changes.size());
    EXPECT_EQ(ServerId(5, 4), tr.changes.changes.front().serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED,
        tr.changes.changes.front().event);
}

TEST_F(ServerTrackerTest, ChangeQueue_getChange) {
    EXPECT_THROW(tr.changes.getChange(), Exception);

    tr.changes.addChange(ServerId(5, 4), ServerChangeEvent::SERVER_ADDED);
    ServerTracker<int>::ServerChange change = tr.changes.getChange();
    EXPECT_EQ(0U, tr.changes.changes.size());
    EXPECT_EQ(ServerId(5, 4), change.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, change.event);
    EXPECT_THROW(tr.changes.getChange(), Exception);
}

TEST_F(ServerTrackerTest, ChangeQueue_areChanges) {

}

}  // namespace RAMCloud
