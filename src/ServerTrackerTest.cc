/* Copyright (c) 2011-2012 Stanford University
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
#include "ServerListBuilder.h"
#include "ServerTracker.h"
#include "ShortMacros.h"

namespace RAMCloud {

struct CountCallback : public ServerTracker<int>::Callback {
    CountCallback() : callbacksFired() {}
    void trackerChangesEnqueued() { ++callbacksFired; }
    int callbacksFired;
};

class ServerTrackerTest : public ::testing::Test {
  public:
    ServerTrackerTest()
        : context()
        , callback()
        , sl(context)
        , tr(context, sl)
        , trcb(context, sl, &callback)
    {
    }

    Context context;
    CountCallback callback;
    ServerList sl;
    ServerTracker<int> tr;
    ServerTracker<int> trcb;

    DISALLOW_COPY_AND_ASSIGN(ServerTrackerTest);
};

TEST_F(ServerTrackerTest, constructors) {
    EXPECT_EQ(0U, tr.serverList.size());
    EXPECT_FALSE(tr.changes.hasChanges());
    EXPECT_FALSE(tr.eventCallback);
    EXPECT_EQ(static_cast<uint32_t>(-1), tr.lastRemovedIndex);

    EXPECT_EQ(0U, trcb.serverList.size());
    EXPECT_FALSE(trcb.changes.hasChanges());
    EXPECT_TRUE(trcb.eventCallback);
    EXPECT_TRUE(&callback == trcb.eventCallback);
    EXPECT_EQ(static_cast<uint32_t>(-1), trcb.lastRemovedIndex);
}

TEST_F(ServerTrackerTest, enqueueChange) {
    EXPECT_EQ(0U, tr.serverList.size());
    EXPECT_EQ(0U, tr.changes.changes.size());
    tr.enqueueChange(ServerDetails(ServerId(2, 0), ServerStatus::UP),
                     ServerChangeEvent::SERVER_ADDED);
    EXPECT_EQ(3U, tr.serverList.size());
    EXPECT_EQ(1U, tr.changes.changes.size());

    // Ensure nothing was actually added to the lists.
    for (size_t i = 0; i < tr.serverList.size(); i++) {
        EXPECT_FALSE(tr.serverList[i].server.serverId.isValid());
        EXPECT_TRUE(tr.serverList[i].pointer == NULL);
    }
    for (size_t i = 0; i < trcb.serverList.size(); i++) {
        EXPECT_FALSE(trcb.serverList[i].server.serverId.isValid());
        EXPECT_TRUE(trcb.serverList[i].pointer == NULL);
    }
}

struct EnsureBothHaveChangesCallback : public ServerTracker<int>::Callback {
    typedef ServerTracker<int> Tr;
    EnsureBothHaveChangesCallback()
        : tr1() , tr2() , ok() {}

    void trackerChangesEnqueued() {
        if (!tr1 || !tr2)
            return;
        ServerChangeEvent event;
        ServerDetails details;
        // Make sure the tr2 callback hasn't been fired yet.
        EXPECT_EQ(0,
            static_cast<CountCallback*>(tr2->eventCallback)->callbacksFired);
        // Ensure that both trackers have the change enqueued.
        ok = tr1->getChange(details, event) && tr2->getChange(details, event);
        // Also asserted in the unit test to ensure the code gets run.
        EXPECT_TRUE(ok);
    }

    Tr* tr1;
    Tr* tr2;
    bool ok;

    DISALLOW_COPY_AND_ASSIGN(EnsureBothHaveChangesCallback);
};

TEST_F(ServerTrackerTest, fireCallback) {
    callback.callbacksFired = 0;
    ProtoBuf::ServerList wholeList;
    ServerListBuilder{wholeList}
        ({MASTER_SERVICE}, *ServerId(1, 0), "mock:host=one", 101)
        ({BACKUP_SERVICE}, *ServerId(2, 0), "mock:host=two", 102,
            ServerStatus::CRASHED);
    wholeList.set_version_number(1u);
    sl.applyFullList(wholeList);
    EXPECT_EQ(1, callback.callbacksFired);

    ProtoBuf::ServerList update;
    ServerListBuilder{update}
        ({MASTER_SERVICE}, *ServerId(1, 5), "mock:host=oneBeta", 101);
    update.set_version_number(2);
    TestLog::Enable _;
    sl.applyUpdate(update);
    EXPECT_EQ(2, callback.callbacksFired);

    ServerChangeEvent event;
    ServerDetails details;

    // Ensure that all trackers have changes enqueued
    // before any of the trackers receives notification.
    EnsureBothHaveChangesCallback orderCheckCb;
    ServerTracker<int> tr1(context, sl, &orderCheckCb);
    CountCallback countCb;
    ServerTracker<int> tr2(context, sl, &countCb);
    orderCheckCb.tr1 = &tr1;
    orderCheckCb.tr2 = &tr2;
    while (tr1.getChange(details, event)); // clear out both queues
    while (tr2.getChange(details, event));
    countCb.callbacksFired = 0;
    ProtoBuf::ServerList update2;
    ServerListBuilder{update2}
        ({MASTER_SERVICE}, *ServerId(1, 6), "mock:host=oneBeta", 101);
    update2.set_version_number(3);
    sl.applyUpdate(update2);
    // Make sure the normal cb got called.
    EXPECT_EQ(1, countCb.callbacksFired);
    // Make sure the second cb got called.
    // If it returns true then that means tr1 and tr2 both had the add
    // enqueued even though only tr1 had its callback fired yet.
    EXPECT_TRUE(orderCheckCb.ok);
}

TEST_F(ServerTrackerTest, hasChanges) {
    EXPECT_FALSE(tr.hasChanges());
    tr.enqueueChange(ServerDetails(ServerId(2, 0), ServerStatus::UP),
                     ServerChangeEvent::SERVER_ADDED);
    EXPECT_TRUE(tr.hasChanges());
}

static bool
getChangeFilter(string s)
{
    return (s == "getChange");
}

TEST_F(ServerTrackerTest, getChange) {
    TestLog::Enable _(&getChangeFilter);
    ServerDetails server;
    ServerChangeEvent event;

    // Add
    EXPECT_FALSE(tr.getChange(server, event));
    EXPECT_EQ(0U, tr.serverList.size());
    tr.enqueueChange(ServerDetails(ServerId(2, 0), "Prophylaxis",
                                   {BACKUP_SERVICE}, 100, ServerStatus::UP),
                     ServerChangeEvent::SERVER_ADDED);
    EXPECT_EQ(3U, tr.serverList.size());
    EXPECT_FALSE(tr.serverList[2].server.serverId.isValid());
    EXPECT_TRUE(tr.serverList[2].pointer == NULL);
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_EQ(ServerId(2, 0), server.serverId);
    EXPECT_EQ("Prophylaxis", server.serviceLocator);
    EXPECT_TRUE(server.services.has(BACKUP_SERVICE));
    EXPECT_FALSE(server.services.has(MASTER_SERVICE));
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, event);
    EXPECT_FALSE(tr.getChange(server, event));
    EXPECT_EQ(ServerId(2, 0), tr.serverList[2].server.serverId);
    EXPECT_TRUE(tr.serverList[2].pointer == NULL);

    // Crashed

    tr.enqueueChange(ServerDetails(ServerId(2, 0), ServerStatus::CRASHED),
                     ServerChangeEvent::SERVER_CRASHED);
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_EQ(ServerId(2, 0), server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_CRASHED, event);
    EXPECT_EQ(ServerStatus::CRASHED, tr.getServerDetails({2, 0})->status);

    // Remove
    tr[ServerId(2, 0)] = reinterpret_cast<int*>(57);
    tr.enqueueChange(ServerDetails(ServerId(2, 0), ServerStatus::DOWN),
                     ServerChangeEvent::SERVER_REMOVED);
    EXPECT_EQ(reinterpret_cast<void*>(57), tr[ServerId(2, 0)]);
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_EQ(ServerId(2, 0), server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_REMOVED, event);
    EXPECT_EQ(2U, tr.lastRemovedIndex);
    tr.testing_avoidGetChangeAssertion = true;
    EXPECT_FALSE(tr.getChange(server, event));
    EXPECT_EQ("getChange: User of this ServerTracker did not NULL out previous "
        "pointer for index 2 (ServerId 2)!", TestLog::get());
    EXPECT_FALSE(tr.serverList[2].server.serverId.isValid());
    EXPECT_EQ("", tr.serverList[2].server.serviceLocator);
    EXPECT_EQ(0u, tr.serverList[2].server.services.serialize());
    EXPECT_TRUE(tr.serverList[2].pointer == NULL);
    EXPECT_EQ(static_cast<uint32_t>(-1), tr.lastRemovedIndex);
}

TEST_F(ServerTrackerTest, getRandomServerIdWithService) {
    Logger::get().setLogLevels(SILENT_LOG_LEVEL);

    ServerDetails server;
    ServerChangeEvent event;

    EXPECT_FALSE(tr.getRandomServerIdWithService(MASTER_SERVICE).isValid());
    tr.enqueueChange(ServerDetails(ServerId(0, 1), "", {MASTER_SERVICE}, 100,
                                   ServerStatus::UP),
                     ServerChangeEvent::SERVER_ADDED);
    EXPECT_FALSE(tr.getRandomServerIdWithService(MASTER_SERVICE).isValid());

    EXPECT_TRUE(tr.getChange(server, event));
    for (int i = 0; i < 10; i++) {
        // Ensure asking for a specific service filters properly.
        // Should find one with low order bit set.
        EXPECT_EQ(ServerId(0, 1),
                  tr.getRandomServerIdWithService(MASTER_SERVICE));
        // No host available with this service bit set.
        EXPECT_EQ(ServerId(),
                  tr.getRandomServerIdWithService(BACKUP_SERVICE));
    }

    tr.enqueueChange(ServerDetails(ServerId(1, 1), "", {MASTER_SERVICE}, 100,
                                   ServerStatus::UP),
                     ServerChangeEvent::SERVER_ADDED);

    EXPECT_TRUE(tr.getChange(server, event));
    bool firstSeen = false;
    bool secondSeen = false;
    for (int i = 0; i < 100; i++) {
        ServerId id = tr.getRandomServerIdWithService(MASTER_SERVICE);
        EXPECT_TRUE(id == ServerId(0, 1) ||
                    id == ServerId(1, 1));
        if (id == ServerId(0, 1)) firstSeen = true;
        if (id == ServerId(1, 1)) secondSeen = true;
    }
    EXPECT_TRUE(firstSeen);
    EXPECT_TRUE(secondSeen);

    // Ensure looping over empty list terminates.
    tr.enqueueChange(ServerDetails(ServerId(0, 1), ServerStatus::DOWN),
                     ServerChangeEvent::SERVER_REMOVED);
    tr.enqueueChange(ServerDetails(ServerId(1, 1), ServerStatus::DOWN),
                     ServerChangeEvent::SERVER_REMOVED);
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_FALSE(tr.getRandomServerIdWithService({MASTER_SERVICE}).isValid());
}

TEST_F(ServerTrackerTest, getRandomServerIdWithService_evenDistribution) {
    Logger::get().setLogLevels(SILENT_LOG_LEVEL);

    ServerDetails server;
    ServerChangeEvent event;
    tr.enqueueChange(ServerDetails(ServerId(1, 0), "", {BACKUP_SERVICE}, 100,
                                   ServerStatus::UP),
                     ServerChangeEvent::SERVER_ADDED);
    tr.enqueueChange(ServerDetails(ServerId(2, 0), "", {BACKUP_SERVICE}, 100,
                                   ServerStatus::UP),
                     ServerChangeEvent::SERVER_ADDED);
    tr.enqueueChange(ServerDetails(ServerId(3, 0), "", {BACKUP_SERVICE}, 100,
                                   ServerStatus::UP),
                     ServerChangeEvent::SERVER_ADDED);
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_FALSE(tr.getChange(server, event));
    ASSERT_EQ(3u, tr.size());

    std::vector<uint32_t> counts(tr.size(), 0);
    for (int i = 0; i < 10000; ++i) {
        ServerId id =
            tr.getRandomServerIdWithService(BACKUP_SERVICE);
        counts[id.indexNumber() - 1]++;
    }

    // Check to make sure the most-significant digit is what we expect:
    // Each backup should be returned about 1/3 of the time (3333 times).
    foreach (uint32_t count, counts) {
        LOG(ERROR, "%u", count);
        EXPECT_EQ(3u, count / 1000);
    }
}

TEST_F(ServerTrackerTest, getLocator) {
    EXPECT_THROW(tr.getLocator(ServerId(1, 0)), Exception);
    tr.enqueueChange(ServerDetails(ServerId(1, 1), "mock:",
                                   {MASTER_SERVICE}, 100,
                                   ServerStatus::UP),
                     ServerChangeEvent::SERVER_ADDED);
    ServerDetails server;
    ServerChangeEvent event;
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_THROW(tr.getLocator(ServerId(2, 0)), Exception);
    EXPECT_EQ("mock:", tr.getLocator(ServerId(1, 1)));
}

TEST_F(ServerTrackerTest, getServerDetails) {
    EXPECT_THROW(tr.getLocator(ServerId(1, 0)), Exception);
    ServerDetails details(ServerId(1, 1), "mock:", {MASTER_SERVICE}, 100,
                          ServerStatus::UP);
    tr.enqueueChange(details, ServerChangeEvent::SERVER_ADDED);
    ServerDetails server;
    ServerChangeEvent event;
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_THROW(tr.getLocator(ServerId(2, 0)), Exception);
    EXPECT_EQ(details.services.serialize(),
              tr.getServerDetails(ServerId(1, 1))->services.serialize());

    details.status = ServerStatus::CRASHED;
    tr.enqueueChange(details, ServerChangeEvent::SERVER_CRASHED);
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_EQ(details.services.serialize(),
              tr.getServerDetails(ServerId(1, 1))->services.serialize());
    EXPECT_EQ(details.status,
              tr.getServerDetails(ServerId(1, 1))->status);
}

TEST_F(ServerTrackerTest, getSession) {
    TestLog::Enable _;
    EXPECT_THROW(tr.getSession(ServerId(1, 0)), TransportException);

    ServerDetails details(ServerId(1, 1), "mock:", {MASTER_SERVICE},
                          100, ServerStatus::UP);
    tr.enqueueChange(details, ServerChangeEvent::SERVER_ADDED);
    ServerDetails server;
    ServerChangeEvent event;
    EXPECT_TRUE(tr.getChange(server, event));

    details.status = ServerStatus::CRASHED;
    tr.enqueueChange(details, ServerChangeEvent::SERVER_CRASHED);
    EXPECT_THROW(tr.getSession(ServerId(1, 1)), TransportException);
}

TEST_F(ServerTrackerTest, indexOperator) {
    TestLog::Enable _; // suck up getChange WARNING
    ServerDetails server;
    ServerChangeEvent event;

    EXPECT_THROW(tr[ServerId(0, 0)], Exception);

    tr.enqueueChange(ServerDetails(ServerId(0, 0), ServerStatus::UP),
                     ServerChangeEvent::SERVER_ADDED);
    EXPECT_TRUE(tr.getChange(server, event));
    tr[ServerId(0, 0)] = reinterpret_cast<int*>(45);
    EXPECT_THROW(tr[ServerId(0, 1)], Exception);
    EXPECT_EQ(reinterpret_cast<int*>(45), tr[ServerId(0, 0)]);
    EXPECT_THROW(tr[ServerId(0, 1)], Exception);

    tr.enqueueChange(ServerDetails(ServerId(0, 0), ServerStatus::DOWN),
                     ServerChangeEvent::SERVER_REMOVED);
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_NO_THROW(tr[ServerId(0, 0)]);
    EXPECT_NE(static_cast<int*>(NULL), tr.serverList[0].pointer);
    tr.testing_avoidGetChangeAssertion = true;
    EXPECT_FALSE(tr.getChange(server, event));
    EXPECT_THROW(tr[ServerId(0, 0)], Exception);
    EXPECT_EQ(static_cast<int*>(NULL), tr.serverList[0].pointer);
}

TEST_F(ServerTrackerTest, size) {
    ServerDetails server;
    ServerChangeEvent event;

    EXPECT_EQ(0U, tr.size());
    tr.enqueueChange(ServerDetails(ServerId(0, 0), ServerStatus::UP),
                     ServerChangeEvent::SERVER_ADDED);
    EXPECT_EQ(0U, tr.size());
    tr.getChange(server, event);
    EXPECT_EQ(1U, tr.size());

    tr.enqueueChange(ServerDetails(ServerId(0, 0), ServerStatus::DOWN),
                     ServerChangeEvent::SERVER_REMOVED);
    EXPECT_EQ(1U, tr.size());
    tr.getChange(server, event);
    EXPECT_EQ(0U, tr.size());
}

TEST_F(ServerTrackerTest, toString) {
    EXPECT_EQ("", tr.toString());
    tr.enqueueChange(ServerDetails(ServerId(1, 0), "mock:",
                                   {MASTER_SERVICE}, 100,
                                   ServerStatus::UP),
                     ServerChangeEvent::SERVER_ADDED);
    ServerDetails server;
    ServerChangeEvent event;
    EXPECT_EQ("", tr.toString());
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_EQ(
        "server 1 at mock: with MASTER_SERVICE is UP\n",
        tr.toString());
}

TEST_F(ServerTrackerTest, getServersWithService) {
    tr.enqueueChange({{1, 0}, "", {MASTER_SERVICE}, 100, ServerStatus::UP},
                      ServerChangeEvent::SERVER_ADDED);
    tr.enqueueChange({{2, 0}, "", {BACKUP_SERVICE}, 100, ServerStatus::UP},
                      ServerChangeEvent::SERVER_ADDED);
    tr.enqueueChange({{3, 0}, "", {BACKUP_SERVICE}, 100, ServerStatus::UP},
                      ServerChangeEvent::SERVER_ADDED);
    tr.enqueueChange({{3, 0}, "", {BACKUP_SERVICE}, 100, ServerStatus::CRASHED},
                      ServerChangeEvent::SERVER_CRASHED);
    tr.enqueueChange({{4, 0}, "", {BACKUP_SERVICE}, 100, ServerStatus::UP},
                      ServerChangeEvent::SERVER_ADDED);
    tr.enqueueChange({{4, 0}, "", {BACKUP_SERVICE}, 100, ServerStatus::DOWN},
                      ServerChangeEvent::SERVER_REMOVED);
    ServerDetails server;
    ServerChangeEvent event;
    while (tr.getChange(server, event));
    auto servers = tr.getServersWithService(MASTER_SERVICE);
    ASSERT_EQ(1lu, servers.size());
    EXPECT_EQ(ServerId(1, 0), servers[0]);
    servers = tr.getServersWithService(BACKUP_SERVICE);
    ASSERT_EQ(1lu, servers.size());
    EXPECT_EQ(ServerId(2, 0), servers[0]);
}

TEST_F(ServerTrackerTest, ChangeQueue_addChange) {
    EXPECT_EQ(0U, tr.changes.changes.size());
    auto details = ServerDetails(ServerId(5, 4), ServerStatus::UP);
    tr.changes.addChange(details, ServerChangeEvent::SERVER_ADDED);
    EXPECT_EQ(1U, tr.changes.changes.size());
    EXPECT_EQ(ServerId(5, 4), tr.changes.changes.front().server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED,
        tr.changes.changes.front().event);
}

TEST_F(ServerTrackerTest, ChangeQueue_getChange) {
    EXPECT_THROW(tr.changes.getChange(), Exception);

    tr.changes.addChange(ServerDetails(ServerId(5, 4), ServerStatus::UP),
                         ServerChangeEvent::SERVER_ADDED);
    ServerTracker<int>::ServerChange change = tr.changes.getChange();
    EXPECT_EQ(0U, tr.changes.changes.size());
    EXPECT_EQ(ServerId(5, 4), change.server.serverId);
    EXPECT_EQ(ServerChangeEvent::SERVER_ADDED, change.event);
    EXPECT_THROW(tr.changes.getChange(), Exception);
}

TEST_F(ServerTrackerTest, ChangeQueue_hasChanges) {

}

}  // namespace RAMCloud
