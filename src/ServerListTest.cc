/* Copyright (c) 2011-2012 Stanford University
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

#include "TestUtil.h"
#include "ServerList.h"
#include "ServerListBuilder.h"
#include "ServerTracker.h"

namespace RAMCloud {

class ServerListTest : public ::testing::Test {
  public:
    struct MockServerTracker : public ServerTracker<int> {
        explicit MockServerTracker(Context* context)
            : ServerTracker<int>(context)
            , changes()
        {}
        void enqueueChange(const ServerDetails& server, ServerChangeEvent event)
        {
            changes.push({server, event});
        }
        void fireCallback() {}
        std::queue<ServerChange> changes;
    };
    Context context;
    ServerList sl;
    MockServerTracker tr;

    ServerListTest()
        : context(),
          sl(&context),
          tr(&context)
    {}

    DISALLOW_COPY_AND_ASSIGN(ServerListTest);
};

TEST_F(ServerListTest, iget_serverId) {
    sl.testingAdd({{5, 2}, "mock:id=5", {}, 100, ServerStatus::UP});
    EXPECT_TRUE(sl.iget({10, 0}) == NULL);
    EXPECT_TRUE(sl.iget({2, 0}) == NULL);
    EXPECT_TRUE(sl.iget({5, 1}) == NULL);
    EXPECT_TRUE(sl.iget({5, 2}) != NULL);
    EXPECT_EQ("mock:id=5", sl.iget({5, 2})->serviceLocator);
}

TEST_F(ServerListTest, indexOperator) {
    EXPECT_FALSE(sl[0].isValid());
    EXPECT_FALSE(sl[183742].isValid());
    sl.testingAdd({{7572, 2734}, "mock:", {}, 100, ServerStatus::UP});
    EXPECT_EQ(ServerId(7572, 2734), sl[7572]);
    sl.testingRemove({7572, 2734});
    EXPECT_FALSE(sl[7572].isValid());
}


TEST_F(ServerListTest, applyServerList_doubleFullLists) {
    // Apply Full List
    ProtoBuf::ServerList wholeList;
    ServerListBuilder{wholeList}
        ({}, *ServerId{1, 0}, "mock:host=one", 101, 1);
    wholeList.set_version_number(1);
    wholeList.set_type(ProtoBuf::ServerList_Type_FULL_LIST);
    ASSERT_NO_THROW(sl.applyServerList(wholeList));

    // Apply Full List again
    wholeList.set_version_number(2);
    TestLog::Enable _;
    EXPECT_EQ(1lu, sl.applyServerList(wholeList));
    EXPECT_STREQ("applyServerList: Ignoring full server list with version "
            "2 (server list already populated, version 1)",
            TestLog::get().c_str());
}

TEST_F(ServerListTest, applyServerList_wrongVersion) {
    ProtoBuf::ServerList update;
    update.set_type(ProtoBuf::ServerList_Type_UPDATE);
    update.set_version_number(9);
    sl.version = 10;

    TestLog::Enable _;
    EXPECT_EQ(10lu, sl.applyServerList(update));
    EXPECT_EQ("applyServerList: Ignoring out-of order server list update "
            "with version 9 (local server list is at version 10)",
            TestLog::get());

    TestLog::reset();
    update.set_version_number(13);
    sl.applyServerList(update);
    EXPECT_EQ("applyServerList: Ignoring out-of order server list update "
            "with version 13 (local server list is at version 10)",
            TestLog::get());
}

TEST_F(ServerListTest, applyServerList_success) {
    // Apply Full List
    ProtoBuf::ServerList wholeList;
    ServerListBuilder{wholeList}
        ({}, *ServerId{1, 0}, "mock:host=one", 101, 1)
        ({}, *ServerId{2, 0}, "mock:host=two", 102, 1)
        ({}, *ServerId{3, 1}, "mock:host=two", 103, 2)
        ({}, *ServerId{4, 2}, "mock:host=two", 104, 2, ServerStatus::CRASHED)
        ({}, *ServerId{5, 0}, "mock:host=five", 104, 3, ServerStatus::REMOVE);
    wholeList.set_version_number(1);
    wholeList.set_type(ProtoBuf::ServerList_Type_FULL_LIST);
    sl.applyServerList(wholeList);
    EXPECT_EQ(1lu, sl.version);
    ASSERT_EQ(6lu, sl.size());
    EXPECT_TRUE(sl.isUp({1, 0}));
    EXPECT_TRUE(sl.isUp({2, 0}));
    EXPECT_FALSE(sl.isUp({3, 0}));
    EXPECT_TRUE(sl.isUp({3, 1}));
    EXPECT_FALSE(sl.isUp({4, 2}));
    EXPECT_TRUE(sl.contains({4, 2}));
    EXPECT_FALSE(sl.isUp({5, 0}));

    auto* change = &tr.changes.front();
    EXPECT_EQ(ServerId(1, 0), change->server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, change->event);
    tr.changes.pop();
    tr.changes.pop();
    tr.changes.pop();
    change = &tr.changes.front();
    EXPECT_EQ(ServerId(4, 2), change->server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_CRASHED, change->event);
    tr.changes.pop();

    // Apply an update
    TestLog::Enable _;
    ProtoBuf::ServerList update;
    ServerListBuilder{update}
        ({}, *ServerId{1, 0}, "mock:host=one", 101, 1, ServerStatus::CRASHED)
        ({}, *ServerId{5, 0}, "mock:host=one", 105, 1)
        ({}, *ServerId{4, 2}, "mock:host=one", 104, 1, ServerStatus::REMOVE);
    update.set_version_number(2);
    update.set_type(ProtoBuf::ServerList_Type_UPDATE);
    sl.applyServerList(update);
    EXPECT_EQ(2lu, sl.version);
    ASSERT_EQ(6lu, sl.size());
    EXPECT_FALSE(sl.isUp({1, 0}));
    EXPECT_TRUE(sl.contains({1, 0}));
    EXPECT_TRUE(sl.isUp({2, 0}));
    EXPECT_FALSE(sl.isUp({3, 0}));
    EXPECT_TRUE(sl.isUp({3, 1}));
    EXPECT_FALSE(sl.isUp({4, 2}));
    EXPECT_FALSE(sl.contains({4, 2}));
    EXPECT_TRUE(sl.isUp({5, 0}));

    change = &tr.changes.front();
    EXPECT_EQ(ServerId(1, 0), change->server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_CRASHED, change->event);
    tr.changes.pop();
    tr.changes.pop();
    change = &tr.changes.front();
    EXPECT_EQ(ServerId(4, 2), change->server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, change->event);
    tr.changes.pop();
}

}  // namespace RAMCloud
