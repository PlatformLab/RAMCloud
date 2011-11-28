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
        : tr(),
          trcb(callback)
    {
    }

    ServerTracker<int> tr;
    ServerTracker<int> trcb;

    DISALLOW_COPY_AND_ASSIGN(ServerTrackerTest);
};

TEST_F(ServerTrackerTest, constructors) {
    EXPECT_EQ(0U, tr.serverList.size());
    EXPECT_FALSE(tr.changes.areChanges());
    EXPECT_FALSE(tr.eventCallback);

    EXPECT_EQ(0U, trcb.serverList.size());
    EXPECT_FALSE(trcb.changes.areChanges());
    EXPECT_TRUE(trcb.eventCallback);
    EXPECT_TRUE(callback == *(trcb.eventCallback));
}

TEST_F(ServerTrackerTest, handleChange) {
    EXPECT_EQ(0U, tr.serverList.size());
    EXPECT_EQ(0U, tr.changes.changes.size());
    tr.handleChange(ServerId(2, 0), ServerChangeEvent::SERVER_ADDED);
    EXPECT_EQ(3U, tr.serverList.size());
    EXPECT_EQ(1U, tr.changes.changes.size());

    trcb.handleChange(ServerId(0, 0), ServerChangeEvent::SERVER_ADDED);
    EXPECT_EQ(1, callbacksFired);
    trcb.handleChange(ServerId(0, 0), ServerChangeEvent::SERVER_REMOVED);
    EXPECT_EQ(2, callbacksFired);

    // Ensure nothing was actually added to the lists.
    for (size_t i = 0; i < tr.serverList.size(); i++) {
        EXPECT_EQ(ServerId::INVALID_SERVERID, tr.serverList[i].first);
        EXPECT_FALSE(tr.serverList[i].second);
    }
    for (size_t i = 0; i < trcb.serverList.size(); i++) {
        EXPECT_EQ(ServerId::INVALID_SERVERID, trcb.serverList[i].first);
        EXPECT_FALSE(trcb.serverList[i].second);
    }
}

TEST_F(ServerTrackerTest, areChanges) {
    EXPECT_FALSE(tr.areChanges());
    tr.handleChange(ServerId(2, 0), ServerChangeEvent::SERVER_ADDED);
    EXPECT_TRUE(tr.areChanges());
}

TEST_F(ServerTrackerTest, getChange) {
    // Add
    EXPECT_THROW(tr.getChange(), Exception);
    EXPECT_EQ(0U, tr.serverList.size());
    tr.handleChange(ServerId(2, 0), ServerChangeEvent::SERVER_ADDED);
    EXPECT_EQ(3U, tr.serverList.size());
    EXPECT_EQ(ServerId::INVALID_SERVERID, tr.serverList[2].first);
    EXPECT_FALSE(tr.serverList[2].second);
    ServerTracker<int>::ServerChange change = tr.getChange();
    EXPECT_EQ(ServerId(2, 0), change.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, change.event);
    EXPECT_FALSE(change.removedData);
    EXPECT_THROW(tr.getChange(), Exception);
    EXPECT_EQ(ServerId(2, 0), tr.serverList[2].first);
    EXPECT_FALSE(tr.serverList[2].second);

    // Remove
    tr[ServerId(2, 0)] = 57;
    tr.handleChange(ServerId(2, 0), ServerChangeEvent::SERVER_REMOVED);
    EXPECT_EQ(57, *tr[ServerId(2, 0)]);
    change = tr.getChange();
    EXPECT_EQ(ServerId(2, 0), change.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, change.event);
    EXPECT_TRUE(change.removedData);
    EXPECT_EQ(57, *change.removedData);
    EXPECT_THROW(tr.getChange(), Exception);
    EXPECT_EQ(ServerId::INVALID_SERVERID, tr.serverList[2].first);
    EXPECT_FALSE(tr.serverList[2].second);
}

TEST_F(ServerTrackerTest, getRandomServerId) {
    EXPECT_EQ(ServerId::INVALID_SERVERID, tr.getRandomServerId());
    tr.handleChange(ServerId(0, 1), ServerChangeEvent::SERVER_ADDED);
    EXPECT_EQ(ServerId::INVALID_SERVERID, tr.getRandomServerId());

    tr.getChange();
    for (int i = 0; i < 10; i++)
        EXPECT_EQ(ServerId(0, 1), tr.getRandomServerId());

    tr.handleChange(ServerId(1, 1), ServerChangeEvent::SERVER_ADDED);

    tr.getChange();
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
    tr.handleChange(ServerId(0, 1), ServerChangeEvent::SERVER_REMOVED);
    tr.handleChange(ServerId(1, 1), ServerChangeEvent::SERVER_REMOVED);
    tr.getChange();
    tr.getChange();
    EXPECT_EQ(ServerId::INVALID_SERVERID, tr.getRandomServerId());
}

TEST_F(ServerTrackerTest, indexOperator) {
    EXPECT_THROW(tr[ServerId(0, 0)], Exception);

    tr.handleChange(ServerId(0, 0), ServerChangeEvent::SERVER_ADDED);
    tr.getChange();
    tr[ServerId(0, 0)] = 45;
    EXPECT_THROW(tr[ServerId(0, 1)], Exception);
    EXPECT_EQ(45, *tr[ServerId(0, 0)]);
    EXPECT_THROW(tr[ServerId(0, 1)], Exception);

    tr.handleChange(ServerId(0, 0), ServerChangeEvent::SERVER_REMOVED);
    tr.getChange();
    EXPECT_THROW(tr[ServerId(0, 0)], Exception);
}

TEST_F(ServerTrackerTest, ChangeQueue_addChange) {
    EXPECT_EQ(0U, tr.changes.changes.size());
    tr.changes.addChange(ServerId(5, 4), ServerChangeEvent::SERVER_ADDED);
    EXPECT_EQ(1U, tr.changes.changes.size());
    EXPECT_EQ(ServerId(5, 4), tr.changes.changes.front().serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED,
        tr.changes.changes.front().event);
    EXPECT_FALSE(tr.changes.changes.front().removedData);
}

TEST_F(ServerTrackerTest, ChangeQueue_getChange) {
    EXPECT_THROW(tr.changes.getChange(), Exception);

    tr.changes.addChange(ServerId(5, 4), ServerChangeEvent::SERVER_ADDED);
    ServerTracker<int>::ServerChange change = tr.changes.getChange();
    EXPECT_EQ(0U, tr.changes.changes.size());
    EXPECT_EQ(ServerId(5, 4), change.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, change.event);
    EXPECT_FALSE(change.removedData);
    EXPECT_THROW(tr.changes.getChange(), Exception);
}

TEST_F(ServerTrackerTest, ChangeQueue_areChanges) {

}

}  // namespace RAMCloud
