/* Copyright (c) 2011-2019 Stanford University
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
#include "FailSession.h"
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
        , sl(&context)
        , tr(&context)
        , trcb(&context, &callback)
        , logSilencer()
    {
    }

    Context context;
    CountCallback callback;
    ServerList sl;
    ServerTracker<int> tr;
    ServerTracker<int> trcb;
    TestLog::Enable logSilencer;

    // The next methods provide shortcuts for queueing events.
    void upEvent(ServerId id)
    {
        tr.enqueueChange(ServerDetails(id, "mock:",
                {WireFormat::BACKUP_SERVICE}, 100, ServerStatus::UP),
                ServerChangeEvent::SERVER_ADDED);
    }
    void crashedEvent(ServerId id)
    {
        tr.enqueueChange(ServerDetails(id, "mock:",
                {WireFormat::BACKUP_SERVICE}, 100, ServerStatus::CRASHED),
                ServerChangeEvent::SERVER_CRASHED);
    }
    void removedEvent(ServerId id)
    {
        tr.enqueueChange(ServerDetails(id, "mock:",
                {WireFormat::BACKUP_SERVICE}, 100, ServerStatus::REMOVE),
                ServerChangeEvent::SERVER_REMOVED);
    }

    DISALLOW_COPY_AND_ASSIGN(ServerTrackerTest);
};

TEST_F(ServerTrackerTest, constructors) {
    EXPECT_EQ(0U, tr.serverList.size());
    EXPECT_FALSE(tr.hasChanges());
    EXPECT_FALSE(tr.eventCallback);
    EXPECT_EQ(static_cast<uint32_t>(-1), tr.lastRemovedIndex);

    EXPECT_EQ(0U, trcb.serverList.size());
    EXPECT_FALSE(trcb.hasChanges());
    EXPECT_TRUE(trcb.eventCallback);
    EXPECT_TRUE(&callback == trcb.eventCallback);
    EXPECT_EQ(static_cast<uint32_t>(-1), trcb.lastRemovedIndex);
}

TEST_F(ServerTrackerTest, destructor) {
    EXPECT_EQ(2U, sl.trackers.size());
    ServerTracker<int>* tr2 = new ServerTracker<int>(&context);
    EXPECT_EQ(3U, sl.trackers.size());
    delete tr2;
    EXPECT_EQ(2U, sl.trackers.size());
}

TEST_F(ServerTrackerTest, enqueueChange) {
    EXPECT_EQ(0U, tr.serverList.size());
    EXPECT_EQ(0U, tr.changes.size());
    tr.enqueueChange(ServerDetails(ServerId(2, 0), ServerStatus::UP),
                     ServerChangeEvent::SERVER_ADDED);
    EXPECT_EQ(0U, tr.serverList.size()); // No vector resize before getChange()!
    EXPECT_EQ(1U, tr.changes.size());

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
        ({WireFormat::MASTER_SERVICE}, *ServerId(1, 0), "mock:host=one", 101, 0)
        ({WireFormat::BACKUP_SERVICE}, *ServerId(2, 0), "mock:host=two", 102, 0,
            ServerStatus::CRASHED);
    wholeList.set_version_number(1u);
    sl.applyServerList(wholeList);
    EXPECT_EQ(1, callback.callbacksFired);

    ProtoBuf::ServerList update;
    ServerListBuilder{update}
        ({WireFormat::MASTER_SERVICE}, *ServerId(3, 0),
         "mock:host=oneBeta", 101);
    update.set_version_number(2);
    update.set_type(ProtoBuf::ServerList_Type_UPDATE);
    sl.applyServerList(update);
    EXPECT_EQ(2, callback.callbacksFired);

    ServerChangeEvent event;
    ServerDetails details;

    // Ensure that all trackers have changes enqueued
    // before any of the trackers receives notification.
    EnsureBothHaveChangesCallback orderCheckCb;
    ServerTracker<int> tr1(&context, &orderCheckCb);
    CountCallback countCb;
    ServerTracker<int> tr2(&context, &countCb);
    orderCheckCb.tr1 = &tr1;
    orderCheckCb.tr2 = &tr2;
    while (tr1.getChange(details, event)); // clear out both queues
    while (tr2.getChange(details, event));
    countCb.callbacksFired = 0;
    ProtoBuf::ServerList update2;
    ServerListBuilder{update2}
        ({WireFormat::MASTER_SERVICE}, *ServerId(4, 0),
         "mock:host=oneBeta", 101);
    update2.set_version_number(3);
    update2.set_type(ProtoBuf::ServerList_Type_UPDATE);
    sl.applyServerList(update2);
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

TEST_F(ServerTrackerTest, getChange_detectUnclearedPointer) {
    ServerDetails server;
    ServerChangeEvent event;
    upEvent(ServerId(2, 0));
    EXPECT_TRUE(tr.getChange(server, event));
    tr[server.serverId] = reinterpret_cast<int*>(57);
    removedEvent(ServerId(2, 0));
    string message("no exception");
    try {
        while (tr.getChange(server, event)) {
            // Empty loop body; simply read every event;
        }
    } catch (FatalError& e) {
        message = e.message;
    }
    EXPECT_EQ("User of this ServerTracker did not NULL out previous "
            "pointer for server 2.0", message);
}

TEST_F(ServerTrackerTest, getChange_resetStateAfterRemove) {
    ServerDetails server;
    ServerChangeEvent event;
    upEvent(ServerId(2, 0));
    EXPECT_TRUE(tr.getChange(server, event));
    tr[server.serverId] = reinterpret_cast<int*>(57);
    removedEvent(ServerId(2, 0));
    EXPECT_TRUE(tr.getChange(server, event));             // SERVER_CRASHED //
    EXPECT_TRUE(tr.getChange(server, event));             // SERVER_REMOVED //
    tr[server.serverId] = NULL;
    EXPECT_FALSE(tr.getChange(server, event));            // reset state //
    EXPECT_EQ(0xffffffff, tr.lastRemovedIndex);
    EXPECT_EQ(ServerId(), tr.serverList[2].server.serverId);
}

TEST_F(ServerTrackerTest, getChange_emptyQueue) {
    ServerDetails server;
    ServerChangeEvent event;
    EXPECT_FALSE(tr.getChange(server, event));
}

TEST_F(ServerTrackerTest, getChange_growList) {
    ServerDetails server;
    ServerChangeEvent event;
    upEvent(ServerId(2, 0));
    EXPECT_EQ(0u, tr.serverList.size());
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_EQ(3u, tr.serverList.size());
    upEvent(ServerId(5, 0));
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_EQ(6u, tr.serverList.size());
}

TEST_F(ServerTrackerTest, getChange_add) {
    ServerDetails server;
    ServerChangeEvent event;

    // First add is normal.
    upEvent(ServerId(2, 0));
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_EQ(ServerId(2, 0), server.serverId);
    EXPECT_EQ("mock:", server.serviceLocator);
    EXPECT_TRUE(server.services.has(WireFormat::BACKUP_SERVICE));
    EXPECT_FALSE(server.services.has(WireFormat::MASTER_SERVICE));
    EXPECT_STREQ("SERVER_ADDED", ServerTracker<int>::eventString(event));
    EXPECT_EQ(1u, tr.numberOfServers);
    EXPECT_EQ(0u, tr.changes.size());
    EXPECT_EQ("UP", AbstractServerList::toString(
            tr.serverList[2].server.status));

    // Try add after add: should be OK (e.g. updating replication group).
    upEvent(ServerId(2, 0));
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_STREQ("SERVER_ADDED", ServerTracker<int>::eventString(event));

    // Try add after crash: not allowed.
    crashedEvent(ServerId(2, 0));
    EXPECT_TRUE(tr.getChange(server, event));
    string message("no exception");
    try {
        upEvent(ServerId(2, 0));
        tr.getChange(server, event);
    } catch (FatalError& e) {
        message = e.message;
    }
    EXPECT_EQ("Consistency error between event (server 2.0, status "
            "SERVER_ADDED) and slot (server 2.0, status CRASHED)",
            message);
    EXPECT_EQ(1u, tr.changes.size());
}

TEST_F(ServerTrackerTest, getChange_normalCrashedEvent) {
    ServerDetails server;
    ServerChangeEvent event;
    upEvent(ServerId(2, 0));
    crashedEvent(ServerId(2, 0));
    EXPECT_EQ(2u, tr.changes.size());
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_EQ(1u, tr.changes.size());
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_EQ("CRASHED", AbstractServerList::toString(
            tr.serverList[2].server.status));
    EXPECT_EQ(ServerId(2, 0), server.serverId);
    EXPECT_EQ("CRASHED", AbstractServerList::toString(server.status));
    EXPECT_STREQ("SERVER_CRASHED", ServerTracker<int>::eventString(event));
    EXPECT_EQ(0u, tr.changes.size());
}

TEST_F(ServerTrackerTest, getChange_crashedWithAddMissing) {
    ServerDetails server;
    ServerChangeEvent event;
    crashedEvent(ServerId(2, 0));
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_EQ("UP", AbstractServerList::toString(
            tr.serverList[2].server.status));
    EXPECT_EQ(ServerId(2, 0), server.serverId);
    EXPECT_EQ("UP", AbstractServerList::toString(server.status));
    EXPECT_STREQ("SERVER_ADDED", ServerTracker<int>::eventString(event));
    EXPECT_EQ(1u, tr.changes.size());
    EXPECT_EQ(1u, tr.numberOfServers);
}

TEST_F(ServerTrackerTest, getChange_duplicateCrash) {
    ServerDetails server;
    ServerChangeEvent event;
    upEvent(ServerId(2, 0));
    crashedEvent(ServerId(2, 0));
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_TRUE(tr.getChange(server, event));

    crashedEvent(ServerId(2, 0));
    string message("no exception");
    try {
        tr.getChange(server, event);
    } catch (FatalError& e) {
        message = e.message;
    }
    EXPECT_EQ("Consistency error between event (server 2.0, "
            "status SERVER_CRASHED) and slot (server 2.0, status CRASHED)",
            message);
    EXPECT_EQ(1u, tr.changes.size());
}

TEST_F(ServerTrackerTest, getChange_removedWithCrashMissing) {
    ServerDetails server;
    ServerChangeEvent event;
    upEvent(ServerId(2, 0));
    EXPECT_TRUE(tr.getChange(server, event));
    removedEvent(ServerId(2, 0));
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_EQ("CRASHED", AbstractServerList::toString(
            tr.serverList[2].server.status));
    EXPECT_EQ(ServerId(2, 0), server.serverId);
    EXPECT_EQ("CRASHED", AbstractServerList::toString(server.status));
    EXPECT_STREQ("SERVER_CRASHED", ServerTracker<int>::eventString(event));
    EXPECT_EQ(1u, tr.changes.size());
    EXPECT_EQ(1u, tr.numberOfServers);
}

TEST_F(ServerTrackerTest, getChange_normalRemovedEvent) {
    ServerDetails server;
    ServerChangeEvent event;
    upEvent(ServerId(2, 0));
    crashedEvent(ServerId(2, 0));
    removedEvent(ServerId(2, 0));
    EXPECT_EQ(3u, tr.changes.size());
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_EQ("REMOVE", AbstractServerList::toString(
            tr.serverList[2].server.status));
    EXPECT_EQ(ServerId(2, 0), server.serverId);
    EXPECT_EQ("REMOVE", AbstractServerList::toString(server.status));
    EXPECT_STREQ("SERVER_REMOVED", ServerTracker<int>::eventString(event));
    EXPECT_EQ(0u, tr.changes.size());
    EXPECT_EQ(0u, tr.numberOfServers);
    EXPECT_EQ(2u, tr.lastRemovedIndex);
}

TEST_F(ServerTrackerTest, getChange_removedButServerNeverAdded) {
    ServerDetails server;
    ServerChangeEvent event;
    removedEvent(ServerId(2, 0));
    removedEvent(ServerId(3, 0));
    EXPECT_FALSE(tr.getChange(server, event));
    EXPECT_EQ(0u, tr.changes.size());
}

TEST_F(ServerTrackerTest, getServerIdAtIndexWithService) {
    Logger::get().setLogLevels(SILENT_LOG_LEVEL);

    ServerDetails server;
    ServerChangeEvent event;

    EXPECT_FALSE(tr.getServerIdAtIndexWithService(
        1, WireFormat::BACKUP_SERVICE).isValid());

    upEvent(ServerId(1, 0));
    EXPECT_FALSE(tr.getServerIdAtIndexWithService(
        1, WireFormat::BACKUP_SERVICE).isValid());
    EXPECT_TRUE(tr.getChange(server, event));

    EXPECT_EQ(ServerId(1, 0), tr.getServerIdAtIndexWithService(1,
                                                  WireFormat::BACKUP_SERVICE));
    // No host available with this service bit set.
    EXPECT_EQ(ServerId(), tr.getServerIdAtIndexWithService(1,
                                                  WireFormat::MASTER_SERVICE));

    // Ensure looping over empty list terminates.
    removedEvent(ServerId(1, 0));
    while (tr.getChange(server, event)) {
        // Do nothing; just process all events.
    }
    EXPECT_FALSE(tr.getServerIdAtIndexWithService(
        1, WireFormat::BACKUP_SERVICE).isValid());
}

TEST_F(ServerTrackerTest, getRandomServerIdWithService) {
    Logger::get().setLogLevels(SILENT_LOG_LEVEL);

    ServerDetails server;
    ServerChangeEvent event;

    EXPECT_FALSE(tr.getRandomServerIdWithService(
        WireFormat::BACKUP_SERVICE).isValid());
    upEvent(ServerId(1, 0));
    EXPECT_FALSE(tr.getRandomServerIdWithService(
        WireFormat::BACKUP_SERVICE).isValid());

    EXPECT_TRUE(tr.getChange(server, event));
    for (int i = 0; i < 10; i++) {
        // Ensure asking for a specific service filters properly.
        // Should find one with low order bit set.
        EXPECT_EQ(ServerId(1, 0),
                  tr.getRandomServerIdWithService(WireFormat::BACKUP_SERVICE));
        // No host available with this service bit set.
        EXPECT_EQ(ServerId(),
                  tr.getRandomServerIdWithService(WireFormat::MASTER_SERVICE));
    }

    upEvent(ServerId(2, 0));
    EXPECT_TRUE(tr.getChange(server, event));
    bool firstSeen = false;
    bool secondSeen = false;
    for (int i = 0; i < 100; i++) {
        ServerId id = tr.getRandomServerIdWithService(
            WireFormat::BACKUP_SERVICE);
        EXPECT_TRUE(id == ServerId(1, 0) ||
                    id == ServerId(2, 0));
        if (id == ServerId(1, 0)) firstSeen = true;
        if (id == ServerId(2, 0)) secondSeen = true;
    }
    EXPECT_TRUE(firstSeen);
    EXPECT_TRUE(secondSeen);

    // Ensure looping over empty list terminates.
    removedEvent(ServerId(1, 0));
    removedEvent(ServerId(2, 0));
    while (tr.getChange(server, event)) {
        // Do nothing; just process all events.
    }
    EXPECT_FALSE(tr.getRandomServerIdWithService(
        {WireFormat::BACKUP_SERVICE}).isValid());
}

TEST_F(ServerTrackerTest, getRandomServerIdWithService_evenDistribution) {
    Logger::get().setLogLevels(SILENT_LOG_LEVEL);

    ServerDetails server;
    ServerChangeEvent event;
    upEvent(ServerId(1, 0));
    upEvent(ServerId(2, 0));
    upEvent(ServerId(3, 0));
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_FALSE(tr.getChange(server, event));
    ASSERT_EQ(3u, tr.size());

    std::vector<uint32_t> counts(tr.size(), 0);
    for (int i = 0; i < 10000; ++i) {
        ServerId id =
            tr.getRandomServerIdWithService(WireFormat::BACKUP_SERVICE);
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
                                   {WireFormat::MASTER_SERVICE}, 100,
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
    ServerDetails details(ServerId(1, 1), "mock:",
                          {WireFormat::MASTER_SERVICE}, 100,
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

TEST_F(ServerTrackerTest, indexOperator) {
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

    tr.enqueueChange(ServerDetails(ServerId(0, 0), ServerStatus::REMOVE),
                     ServerChangeEvent::SERVER_REMOVED);
    EXPECT_TRUE(tr.getChange(server, event));
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

    tr.enqueueChange(ServerDetails(ServerId(0, 0), ServerStatus::REMOVE),
                     ServerChangeEvent::SERVER_REMOVED);
    EXPECT_EQ(1U, tr.size());
    tr.getChange(server, event);
    tr.getChange(server, event);
    EXPECT_EQ(0U, tr.size());
}

TEST_F(ServerTrackerTest, toString) {
    EXPECT_EQ("", tr.toString());
    tr.enqueueChange(ServerDetails(ServerId(1, 0), "mock:",
                                   {WireFormat::MASTER_SERVICE}, 100,
                                   ServerStatus::UP),
                     ServerChangeEvent::SERVER_ADDED);
    ServerDetails server;
    ServerChangeEvent event;
    EXPECT_EQ("", tr.toString());
    EXPECT_TRUE(tr.getChange(server, event));
    EXPECT_EQ(
        "server 1.0 at mock: with MASTER_SERVICE is UP\n",
        tr.toString());
}

TEST_F(ServerTrackerTest, getServersWithService) {
    tr.enqueueChange({{1, 0}, "", {WireFormat::MASTER_SERVICE}, 100,
                      ServerStatus::UP}, ServerChangeEvent::SERVER_ADDED);
    tr.enqueueChange({{2, 0}, "", {WireFormat::BACKUP_SERVICE}, 100,
                      ServerStatus::UP}, ServerChangeEvent::SERVER_ADDED);
    tr.enqueueChange({{3, 0}, "", {WireFormat::BACKUP_SERVICE}, 100,
                      ServerStatus::UP}, ServerChangeEvent::SERVER_ADDED);
    tr.enqueueChange({{3, 0}, "", {WireFormat::BACKUP_SERVICE}, 100,
                      ServerStatus::CRASHED},
                      ServerChangeEvent::SERVER_CRASHED);
    tr.enqueueChange({{4, 0}, "", {WireFormat::BACKUP_SERVICE}, 100,
                      ServerStatus::UP}, ServerChangeEvent::SERVER_ADDED);
    tr.enqueueChange({{4, 0}, "", {WireFormat::BACKUP_SERVICE}, 100,
                      ServerStatus::REMOVE}, ServerChangeEvent::SERVER_REMOVED);
    ServerDetails server;
    ServerChangeEvent event;
    while (tr.getChange(server, event));
    auto servers = tr.getServersWithService(WireFormat::MASTER_SERVICE);
    ASSERT_EQ(1lu, servers.size());
    EXPECT_EQ(ServerId(1, 0), servers[0]);
    servers = tr.getServersWithService(WireFormat::BACKUP_SERVICE);
    ASSERT_EQ(1lu, servers.size());
    EXPECT_EQ(ServerId(2, 0), servers[0]);
}

TEST_F(ServerTrackerTest, setParent) {
    EXPECT_EQ(&sl, tr.parent);
    tr.setParent(NULL);
    EXPECT_EQ(static_cast<AbstractServerList*>(NULL), tr.parent);

    tr.setParent(&sl);
    EXPECT_EQ(&sl, tr.parent);
}

}  // namespace RAMCloud
