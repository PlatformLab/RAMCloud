/* Copyright (c) 2014-2016 Stanford University
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
#include "ObjectRpcWrapper.h"
#include "UnsyncedRpcTracker.h"
#include "Memory.h"
#include "MockCluster.h"
#include "Transport.h"
#include "RamCloud.h"
#include "TabletManager.h"
#include "RpcRequestPool.h"

namespace RAMCloud {

class UnsyncedRpcTrackerTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    RamCloud ramcloud;
    UnsyncedRpcTracker* tracker;
    Transport::SessionRef session;
    ClientRequest request;

    ServerConfig masterConfig;
    MasterService* service;
    Server* masterServer;
    uint64_t tableId;

    UnsyncedRpcTrackerTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , ramcloud(&context, "mock:host=coordinator")
        , tracker(ramcloud.unsyncedRpcTracker)
        , session(new Transport::Session("Test"))
        , request({Memory::xmalloc(HERE, 1000), 1000})
        , masterConfig(ServerConfig::forTesting())
        , service()
        , masterServer()
        , tableId()
    {
        masterConfig.localLocator = "mock:host=master";
        masterServer = cluster.addServer(masterConfig);
        service = masterServer->master.get();
        tableId = ramcloud.createTable("table1");
        assert(service->tabletManager.changeState(tableId, 0, ~0UL,
                TabletManager::NORMAL, TabletManager::LOCKED_FOR_RETRIES));
    }

    ~UnsyncedRpcTrackerTest()
    {
    }

    DISALLOW_COPY_AND_ASSIGN(UnsyncedRpcTrackerTest);
};

TEST_F(UnsyncedRpcTrackerTest, registerUnsynced) {
    WireFormat::LogState logPos = {2, 10, 5};
    auto callback = []() {};
    tracker->registerUnsynced(session, request, 1, 2, 3, logPos, callback);

    EXPECT_EQ(1U, tracker->masters.size());
    EXPECT_EQ(2, session->refCount);
    auto master = tracker->masters[session.get()];
    EXPECT_EQ(session.get(), master->session.get());
    EXPECT_EQ(1U, master->rpcs.size());
    auto unsynced = &master->rpcs.front();
    EXPECT_EQ(request.data, unsynced->request.data);
    EXPECT_EQ(1UL, unsynced->tableId);
    EXPECT_EQ(2UL, unsynced->keyHash);
    EXPECT_EQ(3UL, unsynced->objVersion);
    EXPECT_EQ(2UL, unsynced->logPosition.headSegmentId);
    EXPECT_EQ(10UL, unsynced->logPosition.appended);
}

TEST_F(UnsyncedRpcTrackerTest, flushSession) {
    WriteRpc wrpc(&ramcloud, tableId, "1", 1, "xyz", 3, NULL, false);
    wrpc.cancel();

    // Fabricate a fake Unsynced RPC request.
    void* reqData = ramcloud.rpcRequestPool->alloc(wrpc.rawRequest.size);
    memcpy(reqData, wrpc.rawRequest.data, wrpc.rawRequest.size);
    ClientRequest request({reqData, wrpc.rawRequest.size});
    Transport::SessionRef session(wrpc.session);

    tracker->registerUnsynced(session, request, 1, 1, 1U, {1, 5, 0}, []{});
    tracker->flushSession(session.get());

    EXPECT_TRUE(service->tabletManager.changeState(tableId, 0, ~0UL,
            TabletManager::LOCKED_FOR_RETRIES, TabletManager::NORMAL));
    ObjectBuffer value;
    ramcloud.readKeysAndValue(tableId, "1", 1, &value);
    EXPECT_EQ("xyz", string(reinterpret_cast<const char*>(
                     value.getValue()), 3));
}

TEST_F(UnsyncedRpcTrackerTest, flushSession_byHandleTransportError) {
    WriteRpc wrpc(&ramcloud, tableId, "1", 1, "xyz", 3, NULL, false);
    wrpc.cancel();

    // Fabricate a fake Unsynced RPC request.
    void* reqData = ramcloud.rpcRequestPool->alloc(wrpc.rawRequest.size);
    memcpy(reqData, wrpc.rawRequest.data, wrpc.rawRequest.size);
    ClientRequest request({reqData, wrpc.rawRequest.size});
    Transport::SessionRef session(wrpc.session);

    tracker->registerUnsynced(session, request, 1, 1, 1U, {1, 5, 0}, []{});
    // Invoke handleTransportError() to trigger flush of session.
    wrpc.handleTransportError();
    wrpc.cancel();

    EXPECT_TRUE(service->tabletManager.changeState(tableId, 0, ~0UL,
            TabletManager::LOCKED_FOR_RETRIES, TabletManager::NORMAL));
    ObjectBuffer value;
    ramcloud.readKeysAndValue(tableId, "1", 1, &value);
    EXPECT_EQ("xyz", string(reinterpret_cast<const char*>(
                     value.getValue()), 3));
}

TEST_F(UnsyncedRpcTrackerTest, UpdateSyncPoint) {
    WireFormat::LogState logPos = {2, 10, 5};
    bool callbackInvoked = false;
    auto callback = [&callbackInvoked]() {
        callbackInvoked = true;
    };
    tracker->registerUnsynced(session, request, 1, 2, 3, logPos, callback);

    auto master = tracker->masters[session.get()];
    EXPECT_EQ(1U, master->rpcs.size());

    // Normal case: GC and callback is invoked.
    WireFormat::LogState syncPos = {3, 1, 1};
    tracker->updateLogState(session.get(), syncPos);
    EXPECT_TRUE(master->rpcs.empty());
    EXPECT_TRUE(callbackInvoked);

    // No matching session / master.
    tracker->updateLogState(NULL, syncPos);
    EXPECT_TRUE(master->rpcs.empty());
}

TEST_F(UnsyncedRpcTrackerTest, syncWithCallback) {
    auto callback = []() {};
    Transport::SessionRef session2(new Transport::Session("Test2"));
    Transport::SessionRef session3(new Transport::Session("Test3"));
    ClientRequest req2({Memory::xmalloc(HERE, 1000), 1000});
    ClientRequest req3({Memory::xmalloc(HERE, 1000), 1000});
    ClientRequest req4({Memory::xmalloc(HERE, 1000), 1000});
    tracker->registerUnsynced(session, request, 1, 2, 3, {2, 10, 5}, callback);
    tracker->registerUnsynced(session, req2, 1, 2, 4, {2, 11, 5}, callback);
    tracker->registerUnsynced(session2, req3, 2, 2, 4, {2, 11, 5}, callback);
    // Adding a master with empty rpcs queue.
    tracker->registerUnsynced(session3, req4, 3, 2, 4, {1, 1, 1}, callback);

    // Check UnsyncedRpcTracker already cleared the first write.
    EXPECT_EQ(3U, tracker->masters.size());
    int totalUnsynced = 0;
    int totalNonEmptyMasters = 0;
    for (auto it = tracker->masters.begin();
            it != tracker->masters.end(); ++it) {
        totalUnsynced += static_cast<int>(it->second->rpcs.size());
        totalNonEmptyMasters += it->second->rpcs.size() > 0 ? 1 : 0;
    }
    EXPECT_LE(3, totalUnsynced);
    EXPECT_LE(2, totalNonEmptyMasters);

    bool callbackInvoked = false;
    tracker->sync([&callbackInvoked] {
        callbackInvoked = true;
    });
    EXPECT_FALSE(callbackInvoked);

    tracker->updateLogState(session.get(), {2, 11, 11});

    // One master is synced is not enough.
    EXPECT_FALSE(callbackInvoked);

    // New unsynced RPC registered after sync does not matter.
    // Also, new log state info is triggering the GC and callback.
    ClientRequest req5({Memory::xmalloc(HERE, 1000), 1000});
    tracker->registerUnsynced(session2, req5, 2, 2, 4, {3, 3, 1}, callback);

    // Once all masters are synced, callback was fired.
    EXPECT_TRUE(callbackInvoked);
}

}  // namespace RAMCloud
