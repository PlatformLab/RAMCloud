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

namespace __ServerListTest__ {
static std::queue<ServerTracker<int>::ServerChange> changes;

class MockServerTracker : public ServerTracker<int> {
  public:
    explicit MockServerTracker(Context* context): ServerTracker<int>(context) {}
    void enqueueChange(const ServerDetails& server, ServerChangeEvent event)
    {
        __ServerListTest__::changes.push({server, event});
    }
    void fireCallback() {}
};

class ServerListTest : public ::testing::Test {
  public:
    Context context;
    ServerList sl;
    MockServerTracker tr;

    ServerListTest()
        : context(),
          sl(&context),
          tr(&context)
    {
        while (!changes.empty())
            changes.pop();
    }

    DISALLOW_COPY_AND_ASSIGN(ServerListTest);
};

TEST_F(ServerListTest, iget_serverId) {
    sl.add(ServerId(5, 2), "mock:id=5", {}, 100);
    EXPECT_TRUE(sl.iget({10, 0}) == NULL);
    EXPECT_TRUE(sl.iget({2, 0}) == NULL);
    EXPECT_TRUE(sl.iget({5, 1}) == NULL);
    EXPECT_TRUE(sl.iget({5, 2}) != NULL);
    EXPECT_EQ("mock:id=5", sl.iget({5, 2})->serviceLocator);
}

TEST_F(ServerListTest, indexOperator) {
    EXPECT_FALSE(sl[0].isValid());
    EXPECT_FALSE(sl[183742].isValid());
    sl.add(ServerId(7572, 2734), "mock:", {}, 100);
    EXPECT_EQ(ServerId(7572, 2734), sl[7572]);
    sl.remove(ServerId(7572, 2734));
    EXPECT_FALSE(sl[7572].isValid());
}

TEST_F(ServerListTest, applyServerList) {
    // Apply Full List
    TestLog::Enable _;
    ProtoBuf::ServerList wholeList;
    wholeList.set_version_number(1);
    wholeList.set_type(ProtoBuf::ServerList_Type_FULL_LIST);
    sl.applyFullList(wholeList);
    EXPECT_EQ("applyFullList: Got complete list of servers containing 0 "
              "entries (version number 1)", TestLog::get());

    // Apply an update
    TestLog::reset();
    ProtoBuf::ServerList updateList;
    updateList.set_version_number(2);
    updateList.set_type(ProtoBuf::ServerList_Type_UPDATE);
    sl.applyServerList(updateList);
    EXPECT_EQ("applyServerList: Server List from coordinator:\n"
              "version_number: 2\ntype: UPDATE\n | "
              "applyUpdate: Got server list update (version number 2)",
              TestLog::get());
}

TEST_F(ServerListTest, applyServerList_oldVersion) {
    ProtoBuf::ServerList update;
    sl.version = 10;

    TestLog::Enable _;
    update.set_version_number(9);
    sl.applyServerList(update);
    EXPECT_EQ("applyServerList: A repeated/old update version 9 was "
            "sent to a ServerList with version 10.", TestLog::get());

    TestLog::reset();
    update.set_version_number(10);
    sl.applyServerList(update);
    EXPECT_EQ("applyServerList: A repeated/old update version 10 was "
            "sent to a ServerList with version 10.", TestLog::get());
}

static bool
applyFullListFilter(string s)
{
    return s == "applyFullList";
}

TEST_F(ServerListTest, applyFullList_fromEmpty) {
    TestLog::Enable _(applyFullListFilter);

    EXPECT_EQ(0U, sl.size());
    EXPECT_EQ(0U, sl.getVersion());

    ProtoBuf::ServerList wholeList;
    ServerListBuilder{wholeList}
        ({WireFormat::MASTER_SERVICE}, *ServerId(1, 0), "mock:host=one", 101)
        ({WireFormat::BACKUP_SERVICE}, *ServerId(2, 0), "mock:host=two", 102,
            ServerStatus::CRASHED);
    wholeList.set_version_number(99u);
    wholeList.set_type(ProtoBuf::ServerList_Type_FULL_LIST);
    sl.applyFullList(wholeList);

    EXPECT_EQ(3U, sl.size());       // [0] is reserved
    EXPECT_EQ(ServerId(1, 0), sl[1]);
    EXPECT_EQ(ServerId(2, 0), sl[2]);
    EXPECT_STREQ("mock:host=one", sl.getLocator(ServerId(1, 0)));
    EXPECT_EQ(ServerStatus::UP,
              sl.serverList[ServerId(1, 0).indexNumber()]->status);
    EXPECT_STREQ("mock:host=two", sl.getLocator(ServerId(2, 0)));
    EXPECT_EQ(ServerStatus::CRASHED,
              sl.serverList[ServerId(2, 0).indexNumber()]->status);
    EXPECT_EQ(99u, sl.version);
}

TEST_F(ServerListTest, applyFullList_overlap) {
    EXPECT_EQ(0U, sl.size());
    EXPECT_EQ(0U, sl.getVersion());

    // Set the initial list.
    ProtoBuf::ServerList initialList;
    ServerListBuilder{initialList}
        ({WireFormat::MASTER_SERVICE}, *ServerId(1, 0), "mock:host=one")
        ({WireFormat::BACKUP_SERVICE}, *ServerId(2, 0), "mock:host=two")
        ({WireFormat::MASTER_SERVICE}, *ServerId(4, 0), "mock:host=four", 104,
            ServerStatus::CRASHED);
    initialList.set_version_number(0);
    initialList.set_type(ProtoBuf::ServerList_Type_FULL_LIST);
    sl.applyFullList(initialList);

    // Now issue a new list that partially overlaps.
    ProtoBuf::ServerList newerList;
    ServerListBuilder{newerList}
        ({WireFormat::MASTER_SERVICE}, *ServerId(1, 5),
            "mock:host=oneBeta", 101)
        ({WireFormat::BACKUP_SERVICE}, *ServerId(2, 0),
            "mock:host=two", 102)
        ({WireFormat::BACKUP_SERVICE}, *ServerId(3, 0),
            "mock:host=three", 103)
        ({WireFormat::MASTER_SERVICE}, *ServerId(4, 1),
            "mock:host=fourBeta", 104, ServerStatus::CRASHED);
    newerList.set_version_number(1);

    while (!changes.empty())
        changes.pop();

    TestLog::Enable _(applyFullListFilter);
    newerList.set_type(ProtoBuf::ServerList_Type_FULL_LIST);
    sl.applyFullList(newerList);

    // We should now have (1, 5), (2, 0), and (3, 0) in our list.
    // (1, 0) was removed.
    EXPECT_EQ(5U, sl.size());       // [0] is reserved
    EXPECT_EQ(ServerId(1, 5), sl[1]);
    EXPECT_EQ(ServerId(2, 0), sl[2]);
    EXPECT_EQ(ServerId(3, 0), sl[3]);
    EXPECT_STREQ("mock:host=oneBeta", sl.getLocator(ServerId(1, 5)));
    EXPECT_STREQ("mock:host=two", sl.getLocator(ServerId(2, 0)));
    EXPECT_STREQ("mock:host=three", sl.getLocator(ServerId(3, 0)));
    EXPECT_EQ("applyFullList: Got complete list of servers containing 4 "
              "entries (version number 1)", TestLog::get());
    // Removal of {1, 0} proceeds everything but must be preceded by a crash
    // since {1, 0} was still listed as up.
    ASSERT_EQ(7u, changes.size());
    EXPECT_EQ(ServerId(1, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_CRASHED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(1, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, changes.front().event);
    changes.pop();
    // Crash of {4, 1} happens before ANY adds.  This is very important to
    // ensure that if one server enlists replacing another id that even
    // during full list replay the removal event for the old id will precede
    // the addition of the replacing id.  Crash of {4, 1} requires the
    // removal of the crashed {4, 0} and an addition of {4, 1} before it
    // can be crashed.
    EXPECT_EQ(ServerId(4, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(4, 1), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(4, 1), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_CRASHED, changes.front().event);
    changes.pop();
    // {1, 5} is added, removal for {1, 0} happens early due to
    // the early removal logic.  If it were for that logic the
    // 'add' logic would have performed the removal anyway.
    EXPECT_EQ(ServerId(1, 5), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, changes.front().event);
    changes.pop();
    // Addition of {3, 0}.
    EXPECT_EQ(ServerId(3, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, changes.front().event);
    changes.pop();
}

static bool
applyUpdateFilter(string s)
{
    return s == "applyUpdate";
}

TEST_F(ServerListTest, applyUpdate) {
    EXPECT_EQ(0U, sl.size());
    EXPECT_EQ(0U, sl.getVersion());

    // Set the initial list.
    ProtoBuf::ServerList initialList;
    ServerListBuilder{initialList}
        ({WireFormat::MASTER_SERVICE}, *ServerId(1, 0), "mock:host=one");
    initialList.set_version_number(0);
    initialList.set_type(ProtoBuf::ServerList_Type_FULL_LIST);
    sl.applyFullList(initialList);

    TestLog::Enable _(applyUpdateFilter);

    // Now issue an update.
    ProtoBuf::ServerList updateList;
    ServerListBuilder{updateList}
        ({WireFormat::MASTER_SERVICE}, *ServerId(1, 0), "mock:host=one", 101,
             ServerStatus::DOWN)
        ({WireFormat::BACKUP_SERVICE}, *ServerId(2, 0), "mock:host=two", 102);
    updateList.set_version_number(1);
    updateList.set_type(ProtoBuf::ServerList_Type_UPDATE);
    EXPECT_TRUE(sl.applyUpdate(updateList));
    EXPECT_FALSE(sl.contains(ServerId(1, 0)));
    EXPECT_STREQ("mock:host=two", sl.getLocator(ServerId(2, 0)));
    EXPECT_EQ(
        "applyUpdate: Got server list update (version number 1) | "
        "applyUpdate:   Removing server id 1.0 | "
        "applyUpdate:   Adding server id 2.0 (locator \"mock:host=two\") "
            "with services BACKUP_SERVICE and 102 MB/s storage"
        , TestLog::get());
}

TEST_F(ServerListTest, applyUpdate_missedUpdate) {
    TestLog::Enable _(applyUpdateFilter);

    ProtoBuf::ServerList updateList;
    ServerListBuilder{updateList}
        ({WireFormat::MASTER_SERVICE}, *ServerId(1, 0), "mock:host=one");
    updateList.set_version_number(57234);
    updateList.set_type(ProtoBuf::ServerList_Type_UPDATE);
    EXPECT_FALSE(sl.applyUpdate(updateList));
    EXPECT_EQ("applyUpdate: Update generation number is 57234, but last "
        "seen was 0. Something was lost! Shouldn't happen unless there\'s a "
            "programmer error in the CoordinatorServerList update management "
            "code.",
        TestLog::get());
}

// Paranoia: What happens if versions check out, but the udpate tells us to
// remove a server that isn't in our list. That's funky behaviour, but is it
// something worth crashing over?
TEST_F(ServerListTest, applyUpdate_versionOkButSomethingAmiss) {
    TestLog::Enable _(applyUpdateFilter);

    ProtoBuf::ServerList updateList;
    ServerListBuilder{updateList}
        ({WireFormat::MASTER_SERVICE}, *ServerId(1, 0), "mock:host=one", 0,
            ServerStatus::DOWN);
    updateList.set_version_number(1);
    updateList.set_type(ProtoBuf::ServerList_Type_UPDATE);
    EXPECT_FALSE(sl.applyUpdate(updateList));
    EXPECT_EQ("applyUpdate: Got server list update (version number 1) | "
        "applyUpdate:   Cannot remove server id 1.0: The server is not in "
        "our list, despite list version numbers matching (1). Something is "
        "screwed up! Requesting the entire list again.", TestLog::get());
}

static bool
addFilter(string s)
{
    return (s == "add");
}

TEST_F(ServerListTest, add) {
    TestLog::Enable _(&addFilter);

    EXPECT_EQ(0U, sl.serverList.size());
    sl.add(ServerId(57, 1), "mock:", {WireFormat::MASTER_SERVICE,
                                      WireFormat::BACKUP_SERVICE}, 100);
    EXPECT_EQ(58U, sl.serverList.size());
    EXPECT_EQ(ServerId(57, 1), sl.serverList[57]->serverId);
    EXPECT_EQ("mock:", sl.serverList[57]->serviceLocator);
    EXPECT_TRUE(sl.serverList[57]->services.has(WireFormat::MASTER_SERVICE));
    EXPECT_TRUE(sl.serverList[57]->services.has(WireFormat::BACKUP_SERVICE));
    EXPECT_FALSE(sl.serverList[57]->services.has(WireFormat::PING_SERVICE));
    EXPECT_EQ(100u, sl.serverList[57]->expectedReadMBytesPerSec);
    EXPECT_EQ(1U, changes.size());
    EXPECT_EQ(ServerId(57, 1), changes.front().server.serverId);
    EXPECT_EQ("mock:", changes.front().server.serviceLocator);
    EXPECT_TRUE(changes.front().server.services.has(
        WireFormat::MASTER_SERVICE));
    EXPECT_TRUE(changes.front().server.services.has(
        WireFormat::BACKUP_SERVICE));
    EXPECT_FALSE(changes.front().server.services.has(
        WireFormat::PING_SERVICE));
    EXPECT_EQ(100u, changes.front().server.expectedReadMBytesPerSec);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, changes.front().event);
    changes.pop();

    TestLog::reset();
    EXPECT_EQ(0U, changes.size());

    // ADD of older ServerId
    sl.add(ServerId(57, 0), "mock:", {}, 100);
    EXPECT_EQ("add: Dropping addition of ServerId older than the current entry "
        "(57.0 < 57.1)!", TestLog::get());
    TestLog::reset();
    EXPECT_EQ(0U, changes.size());

    // ADD before previous REMOVE
    sl.add(ServerId(57, 2), "mock:", {}, 100);
    EXPECT_EQ("add: Addition of 57.2 seen before removal of 57.1! "
        "Issuing removal before addition.", TestLog::get());
    TestLog::reset();
    EXPECT_EQ(3U, changes.size());
    EXPECT_EQ(ServerId(57, 1), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_CRASHED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(57, 1), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(57, 2), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, changes.front().event);
    changes.pop();
}

TEST_F(ServerListTest, addIdsMatchCurrentlyUp) {
    // Current entry == new entry, current entry is up.
    TestLog::Enable _(&addFilter);
    sl.add({1, 0}, "mock:", {}, 100);
    ASSERT_EQ(1u, changes.size());
    changes.pop();
    sl.add({1, 0}, "mock:", {}, 100);
    EXPECT_EQ(0U, changes.size());
}

TEST_F(ServerListTest, addIdsMatchCurrentlyCrashed) {
    // Current entry == new entry, current entry is crashed.
    TestLog::Enable _(&addFilter);
    sl.crashed({1, 0}, "mock:", {}, 100);
    ASSERT_EQ(2u, changes.size());
    changes.pop();
    changes.pop();
    sl.add({1, 0}, "mock:", {}, 100);
    EXPECT_EQ("add: Add of ServerId 1.0 after it had already been marked "
              "crashed; ignoring", TestLog::get());
    EXPECT_EQ(0U, changes.size());
}

TEST_F(ServerListTest, addCurrentlyDown) {
    // No current entry.
    sl.add({1, 0}, "mock:", {}, 100);
    EXPECT_EQ(1U, changes.size());
    EXPECT_EQ(ServerId(1, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, changes.front().event);
}

TEST_F(ServerListTest, addNewerIdCurrentlyUp) {
    // Current entry is older than new entry, current entry is up.
    sl.add({1, 0}, "mock:", {}, 100);
    EXPECT_EQ(1U, changes.size());
    changes.pop();
    TestLog::Enable _;
    sl.add({1, 1}, "mock:", {}, 100);
    EXPECT_EQ("add: Addition of 1.1 seen before removal of 1.0! "
              "Issuing removal before addition.", TestLog::get());
    ASSERT_EQ(3U, changes.size());
    EXPECT_EQ(ServerId(1, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_CRASHED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(1, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(1, 1), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, changes.front().event);
    changes.pop();
}

TEST_F(ServerListTest, addNewerIdCurrentlyCrashed) {
    // Current entry is older than new entry, current entry is crashed.
    sl.crashed({1, 0}, "mock:", {}, 100);
    EXPECT_EQ(2U, changes.size());
    changes.pop();
    changes.pop();
    TestLog::Enable _;
    sl.add({1, 1}, "mock:", {}, 100);
    EXPECT_EQ("add: Addition of 1.1 seen before removal of 1.0! "
              "Issuing removal before addition.", TestLog::get());
    ASSERT_EQ(2U, changes.size());
    EXPECT_EQ(ServerId(1, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(1, 1), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, changes.front().event);
    changes.pop();
}

TEST_F(ServerListTest, crashedBadIndex) {
    sl.crashed({1, 0}, "mock:", {}, 100);
    ASSERT_EQ(2u, changes.size());
    EXPECT_EQ(ServerId(1, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(1, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_CRASHED, changes.front().event);
    changes.pop();
}

TEST_F(ServerListTest, crashedIdsMatchCurrentlyUp) {
    // Current entry == new entry, current entry is up.
    sl.add({1, 0}, "mock:", {}, 100);
    ASSERT_EQ(1u, changes.size());
    changes.pop();
    sl.crashed({1, 0}, "mock:", {}, 100);
    EXPECT_TRUE(sl.serverList[1]);
    EXPECT_EQ(ServerStatus::CRASHED, sl.serverList[1]->status);
    ASSERT_EQ(1u, changes.size());
    EXPECT_EQ(ServerId(1, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_CRASHED, changes.front().event);
}

TEST_F(ServerListTest, crashedIdsMatchCurrentlyCrashed) {
    // Current entry == new entry, current entry is crashed.
    sl.add({1, 0}, "mock:", {}, 100);
    ASSERT_EQ(1u, changes.size());
    changes.pop();
    sl.crashed({1, 0}, "mock:", {}, 100);
    TestLog::Enable _;
    sl.crashed({1, 0}, "mock:", {}, 100);
    EXPECT_EQ("crashed: Duplicate crash of ServerId 1.0!", TestLog::get());
    EXPECT_TRUE(sl.serverList[1]);
    EXPECT_EQ(ServerStatus::CRASHED, sl.serverList[1]->status);
    ASSERT_EQ(1u, changes.size());
    EXPECT_EQ(ServerId(1, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_CRASHED, changes.front().event);
}

TEST_F(ServerListTest, crashedCurrentlyDown) {
    // No current entry.
    sl.add({1, 0}, "mock:", {}, 100);
    changes.pop();
    sl.remove({1, 0});
    changes.pop();
    changes.pop();
    sl.crashed({1, 0}, "mock:", {}, 100);
    EXPECT_TRUE(sl.serverList[1]);
    ASSERT_EQ(2u, changes.size());
    EXPECT_EQ(ServerId(1, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(1, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_CRASHED, changes.front().event);
}

TEST_F(ServerListTest, crashedNewerIdCurrentlyUp) {
    // Current entry is older than new entry, current entry is up.
    sl.add({1, 0}, "mock:", {}, 100);
    ASSERT_EQ(1u, changes.size());
    changes.pop();
    TestLog::Enable _;
    sl.crashed({1, 1}, "mock:", {}, 100);
    EXPECT_EQ("crashed: Crash of 1.1 seen before crash of 1.0! "
              "Issuing crash/removal before addition.", TestLog::get());
    EXPECT_TRUE(sl.serverList[1]);
    EXPECT_EQ(ServerStatus::CRASHED, sl.serverList[1]->status);
    ASSERT_EQ(4u, changes.size());
    EXPECT_EQ(ServerId(1, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_CRASHED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(1, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(1, 1), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(1, 1), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_CRASHED, changes.front().event);
    changes.pop();
}

TEST_F(ServerListTest, crashedNewerIdCurrentlyCrashed) {
    // Current entry is older than new entry, current entry is crashed.
    sl.add({1, 0}, "mock:", {}, 100);
    sl.crashed({1, 0}, "mock:", {}, 100);
    ASSERT_EQ(2u, changes.size());
    changes.pop();
    changes.pop();
    TestLog::Enable _;
    sl.crashed({1, 1}, "mock:", {}, 100);
    EXPECT_EQ("crashed: Crash of 1.1 seen before crash of 1.0! "
              "Issuing crash/removal before addition.", TestLog::get());
    EXPECT_TRUE(sl.serverList[1]);
    EXPECT_EQ(ServerStatus::CRASHED, sl.serverList[1]->status);
    ASSERT_EQ(3u, changes.size());
    EXPECT_EQ(ServerId(1, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(1, 1), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(1, 1), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_CRASHED, changes.front().event);
    changes.pop();
}

static bool
removeFilter(string s)
{
    return (s == "remove");
}

TEST_F(ServerListTest, remove) {
    Logger::get().setLogLevels(DEBUG);
    TestLog::Enable _(&removeFilter);

    EXPECT_EQ(0U, sl.serverList.size());
    sl.remove(ServerId(0, 0)); // remove beyond size
    sl.add(ServerId(1, 1), "mock:", {}, 100);
    changes.pop();
    EXPECT_EQ(2U, sl.serverList.size());
    sl.remove(ServerId(0, 0)); // remove non-existant
    sl.remove(ServerId(1, 0)); // remove for old version

    EXPECT_EQ("remove: Ignoring removal of unknown ServerId 0.0 | "
        "remove: Ignoring removal of unknown ServerId 0.0 | "
        "remove: Ignoring removal of unknown ServerId 1.0",
        TestLog::get());
    TestLog::reset();

    // Exact match, current entry is up.
    ASSERT_EQ(0u, changes.size());
    sl.remove(ServerId(1, 1));
    EXPECT_FALSE(sl.serverList[1]);
    EXPECT_EQ(2U, changes.size());
    EXPECT_EQ(ServerId(1, 1), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_CRASHED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(1, 1), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, changes.front().event);
    changes.pop();

    // Newer one, current entry is up.
    sl.add(ServerId(1, 1), "mock:", {}, 100);
    changes.pop();
    sl.remove(ServerId(1, 2));
    EXPECT_EQ("remove: Removing ServerId 1.1 because removal for a "
        "newer generation number was received (1.2)", TestLog::get());
    TestLog::reset();
    EXPECT_FALSE(sl.serverList[1]);
    EXPECT_EQ(2U, changes.size());
    EXPECT_EQ(ServerId(1, 1), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_CRASHED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(1, 1), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, changes.front().event);
    changes.pop();

    // Exact match, current entry is crashed.
    ASSERT_EQ(0u, changes.size());
    sl.add({1, 3}, "mock:", {}, 100);
    changes.pop();
    sl.crashed({1, 3}, "mock:", {}, 100);
    changes.pop();
    ASSERT_EQ(0u, changes.size());
    sl.remove({1, 3});
    EXPECT_FALSE(sl.serverList[1]);
    EXPECT_EQ(1u, changes.size());
    EXPECT_EQ(ServerId(1, 3), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, changes.front().event);
    changes.pop();

    // Newer one, current entry is crashed.
    sl.add(ServerId(1, 1), "mock:", {}, 100);
    changes.pop();
    sl.remove(ServerId(1, 2));
    EXPECT_EQ("remove: Removing ServerId 1.1 because removal for a "
        "newer generation number was received (1.2)", TestLog::get());
    TestLog::reset();
    EXPECT_FALSE(sl.serverList[1]);
    EXPECT_EQ(2U, changes.size());
    EXPECT_EQ(ServerId(1, 1), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_CRASHED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(1, 1), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, changes.front().event);
    changes.pop();
}

TEST_F(ServerListTest, removeIdsMatchCurrentlyUp) {
    // Current entry == new entry, current entry is up.
    sl.add({1, 0}, "mock:", {}, 100);
    EXPECT_EQ(1u, changes.size());
    changes.pop();
    sl.remove({1, 0});
    ASSERT_EQ(2u, changes.size());
    EXPECT_EQ(ServerId(1, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_CRASHED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(1, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, changes.front().event);
    changes.pop();
}

TEST_F(ServerListTest, removeIdsMatchCurrentlyCrashed) {
    // Current entry == new entry, current entry is crashed.
    sl.crashed({1, 0}, "mock:", {}, 100);
    ASSERT_EQ(2u, changes.size());
    changes.pop();
    changes.pop();
    sl.remove({1, 0});
    ASSERT_EQ(1u, changes.size());
    EXPECT_EQ(ServerId(1, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, changes.front().event);
    changes.pop();
}

TEST_F(ServerListTest, removeCurrentlyDown) {
    // Have to do an add/remove to get the array sized right.
    sl.add({1, 0}, "mock:", {}, 100);
    sl.remove({1, 0});
    EXPECT_EQ(3u, changes.size());
    changes.pop();
    changes.pop();
    changes.pop();
    TestLog::Enable _;
    sl.remove({1, 0});
    EXPECT_EQ("remove: Ignoring removal of unknown ServerId 1.0",
              TestLog::get());
    EXPECT_EQ(0u, changes.size());
}

TEST_F(ServerListTest, removeNewerIdCurrentlyUp) {
    // Current entry is older than new entry, current entry is up.
    sl.add({1, 0}, "mock:", {}, 100);
    EXPECT_EQ(1u, changes.size());
    changes.pop();
    TestLog::Enable _;
    sl.remove({1, 1});
    EXPECT_EQ("remove: Removing ServerId 1.0 because removal for a newer "
              "generation number was received (1.1)", TestLog::get());
    ASSERT_EQ(2u, changes.size());
    EXPECT_EQ(ServerId(1, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_CRASHED, changes.front().event);
    changes.pop();
    EXPECT_EQ(ServerId(1, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, changes.front().event);
    changes.pop();
}

TEST_F(ServerListTest, removeNewerIdCurrentlyCrashed) {
    // Current entry is older than new entry, current entry is crashed.
    sl.crashed({1, 0}, "mock:", {}, 100);
    ASSERT_EQ(2u, changes.size());
    changes.pop();
    changes.pop();
    sl.remove({1, 0});
    ASSERT_EQ(1u, changes.size());
    EXPECT_EQ(ServerId(1, 0), changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, changes.front().event);
    changes.pop();
}

}  // namespace __ServerListTest__
}  // namespace RAMCloud
