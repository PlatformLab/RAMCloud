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

#include <thread>
#include <mutex>
#include <queue>

#include "TestUtil.h"
#include "AbstractServerList.h"
#include "CoordinatorServerList.h"
#include "MockCluster.h"
#include "MockTransport.h"
#include "RamCloud.h"
#include "ServerTracker.h"
#include "ShortMacros.h"
#include "TransportManager.h"
#include "ServerList.pb.h"

namespace RAMCloud {

namespace {
struct MockServerTracker : public ServerTracker<int> {
    explicit MockServerTracker(Context* context)
    : ServerTracker<int>(context)
    , changes() {
    }

    void enqueueChange(const ServerDetails& server, ServerChangeEvent event) {
        changes.push({server, event});
    }

    void fireCallback() {
        TEST_LOG("called");
    }
    std::queue<ServerTracker<int>::ServerChange> changes;
};
}

class CoordinatorServerListTest : public ::testing::Test {
  public:
    Context context;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    MasterService* master; // Unit tests need to call enlistMaster()
                           // before trying to use master.
    ServerId masterServerId; // Unit tests need to call enlistMaster()
                             // before trying to use masterServerId.
    CoordinatorService* service;
    CoordinatorServerList* sl;
    Tub<MockServerTracker> tr;
    LogCabinHelper* logCabinHelper;
    LogCabin::Client::Log* logCabinLog;
    std::mutex mutex;

    CoordinatorServerListTest()
        : context()
        , cluster(&context)
        , ramcloud()
        , master()
        , masterServerId()
        , service()
        , sl()
        , tr()
        , logCabinHelper()
        , logCabinLog()
        , mutex()
    {
        service = cluster.coordinator.get();

        ramcloud.construct(service->context, "mock:host=coordinator");
        sl = service->context->coordinatorServerList;
        tr.construct(service->context);
        logCabinHelper = service->context->logCabinHelper;
        logCabinLog = service->context->logCabinLog;

        sl->haltUpdater();
    }

    ~CoordinatorServerListTest() {
        // Finish all pending ServerList updates before destroying cluster.
        cluster.syncCoordinatorServerList();
    }

    void add(ServerId serverId, string serviceLocator,
            ServiceMask serviceMask, uint32_t readSpeed) {
        Lock lock(mutex);
        sl->add(lock, serverId, serviceLocator, serviceMask, readSpeed);
        sl->commitUpdate(lock);
    }

    void crashed(ServerId serverId) {
        Lock lock(mutex);
        sl->crashed(lock, serverId);
        sl->commitUpdate(lock);
    }

    void remove(ServerId serverId) {
        Lock lock(mutex);
        sl->remove(lock, serverId);
        sl->commitUpdate(lock);
    }

    ServerId generateUniqueId() {
        Lock lock(mutex);
        return sl->generateUniqueId(lock);
    }

    bool isClusterUpToDate() {
        Lock lock(mutex);
        return sl->isClusterUpToDate(lock);
    }

    bool hasUpdates() {
        Lock lock(mutex);
        return sl->hasUpdates(lock);
    }

    // Enlist a master and store details in master and masterServerId.
    void
    enlistMaster() {
        ServerConfig masterConfig(ServerConfig::forTesting());
        masterConfig.services = {WireFormat::MASTER_SERVICE,
                                 WireFormat::PING_SERVICE,
                                 WireFormat::MEMBERSHIP_SERVICE};
        masterConfig.localLocator = "mock:host=master";
        Server* masterServer = cluster.addServer(masterConfig);
        master = masterServer->master.get();
        master->log->sync();
        masterServerId = masterServer->serverId;
    }

    // From the debug log messages, find the entry id specified immediately
    // next to the given search string.
    EntryId
    findEntryId(string searchString) {
        auto position = TestLog::get().find(searchString);
        if (position == string::npos) {
            throw "Search string not found";
        } else {
            string entryIdString =
                TestLog::get().substr(TestLog::get().find(searchString) +
                                      searchString.length(), 1);
            return strtoul(entryIdString.c_str(), NULL, 0);
        }
    }

    typedef std::unique_lock<std::mutex> Lock;
    DISALLOW_COPY_AND_ASSIGN(CoordinatorServerListTest);
};

/*
 * Return true if a CoordinatorServerList::Entry is indentical to the
 * given serialized protobuf entry.
 */
static bool
protoBufMatchesEntry(const ProtoBuf::ServerList_Entry& protoBufEntry,
        const CoordinatorServerList::Entry& serverListEntry,
        ServerStatus status) {
    if (serverListEntry.services.serialize() !=
            protoBufEntry.services())
        return false;
    if (*serverListEntry.serverId != protoBufEntry.server_id())
        return false;
    if (serverListEntry.serviceLocator != protoBufEntry.service_locator())
        return false;
    if (serverListEntry.expectedReadMBytesPerSec !=
            protoBufEntry.expected_read_mbytes_per_sec())
        return false;
    if (status != ServerStatus(protoBufEntry.status()))
        return false;

    return true;
}

TEST_F(CoordinatorServerListTest, constructor) {
    EXPECT_EQ(0U, sl->numberOfMasters);
    EXPECT_EQ(0U, sl->numberOfBackups);
    EXPECT_EQ(0U, sl->version);
}

TEST_F(CoordinatorServerListTest, iget_serverId) {
    sl->serverList.resize(6);

    add(ServerId(5, 2), "mock:id=5", {WireFormat::MASTER_SERVICE}, 100);
    EXPECT_TRUE(sl->iget({20, 0}) == NULL);
    EXPECT_TRUE(sl->iget({2, 0}) == NULL);
    EXPECT_TRUE(sl->iget({5, 1}) == NULL);
    EXPECT_TRUE(sl->iget({5, 2}) != NULL);
    EXPECT_EQ("mock:id=5", sl->iget({5, 2})->serviceLocator);
}

TEST_F(CoordinatorServerListTest, add) {
    sl->haltUpdater();
    EXPECT_EQ(0U, sl->serverList.size());
    EXPECT_EQ(0U, sl->numberOfMasters);
    EXPECT_EQ(0U, sl->numberOfBackups);

    {
        EXPECT_EQ(0U, sl->version);

        add(ServerId(1, 0), "mock:host=server1",
                {WireFormat::MASTER_SERVICE}, 100);
        EXPECT_EQ(1U, sl->version);
        EXPECT_TRUE(sl->serverList[1].entry);
        EXPECT_FALSE(sl->serverList[0].entry);
        EXPECT_EQ(1U, sl->numberOfMasters);
        EXPECT_EQ(0U, sl->numberOfBackups);
        EXPECT_EQ(ServerId(1, 0), sl->serverList[1].entry->serverId);
        EXPECT_EQ("mock:host=server1", sl->serverList[1].entry->serviceLocator);
        EXPECT_TRUE(sl->serverList[1].entry->isMaster());
        EXPECT_FALSE(sl->serverList[1].entry->isBackup());
        EXPECT_EQ(0u, sl->serverList[1].entry->expectedReadMBytesPerSec);
        EXPECT_EQ(1U, sl->serverList[1].nextGenerationNumber);
        ProtoBuf::ServerList update = sl->updates[0];
        EXPECT_EQ(1U, sl->version);
        EXPECT_EQ(1U, update.version_number());
        EXPECT_EQ(1, update.server_size());
        EXPECT_TRUE(protoBufMatchesEntry(update.server(0),
                *sl->serverList[1].entry, ServerStatus::UP));
    }

    {
        EXPECT_EQ(1U, sl->version);

        add(ServerId(2, 0), "hi again", {
            WireFormat::BACKUP_SERVICE}, 100);
        EXPECT_EQ(2U, sl->version);
        EXPECT_TRUE(sl->serverList[2].entry);
        EXPECT_EQ(ServerId(2, 0), sl->serverList[2].entry->serverId);
        EXPECT_EQ("hi again", sl->serverList[2].entry->serviceLocator);
        EXPECT_FALSE(sl->serverList[2].entry->isMaster());
        EXPECT_TRUE(sl->serverList[2].entry->isBackup());
        EXPECT_EQ(100u, sl->serverList[2].entry->expectedReadMBytesPerSec);
        EXPECT_EQ(1U, sl->serverList[2].nextGenerationNumber);
        EXPECT_EQ(1U, sl->numberOfMasters);
        EXPECT_EQ(1U, sl->numberOfBackups);
        ProtoBuf::ServerList update = sl->updates[1];
        EXPECT_EQ(2U, sl->version);
        EXPECT_EQ(2U, update.version_number());
        EXPECT_TRUE(protoBufMatchesEntry(update.server(0),
                *sl->serverList[2].entry, ServerStatus::UP));
    }
}

namespace {
bool fireCallbackFilter(string s) {
    return s == "fireCallback";
}
}

TEST_F(CoordinatorServerListTest, add_trackerUpdated) {
    TestLog::Enable _(fireCallbackFilter);
    ServerId serverId = generateUniqueId();
    add(serverId, "hi!", {WireFormat::MASTER_SERVICE}, 100);
    EXPECT_EQ("fireCallback: called", TestLog::get());
    ASSERT_FALSE(tr->changes.empty());
    auto& server = tr->changes.front().server;
    EXPECT_EQ(ServerId(1, 0), server.serverId);
    EXPECT_EQ("hi!", server.serviceLocator);
    EXPECT_EQ("MASTER_SERVICE", server.services.toString());
    // Not set when no BACKUP_SERVICE.
    EXPECT_EQ(0u, server.expectedReadMBytesPerSec);
    EXPECT_EQ(ServerStatus::UP, server.status);
    EXPECT_EQ(SERVER_ADDED, tr->changes.front().event);
}

TEST_F(CoordinatorServerListTest, crashed) {
    uint64_t orig_version = sl->version;
    EXPECT_THROW(crashed(ServerId(0, 0)), Exception);
    // Ensure no update was generated
    EXPECT_EQ(orig_version, sl->version);

    ServerId serverId = generateUniqueId();
    add(serverId, "hi!", {WireFormat::MASTER_SERVICE}, 100);

    CoordinatorServerList::Entry entryCopy = (*sl)[ServerId(1, 0)];
    EXPECT_NO_THROW(crashed(ServerId(1, 0)));
    ASSERT_TRUE(sl->serverList[1].entry);
    EXPECT_EQ(ServerStatus::CRASHED, sl->serverList[1].entry->status);
    EXPECT_TRUE(protoBufMatchesEntry(sl->updates[1].server(0),
                entryCopy, ServerStatus::CRASHED));

    orig_version = sl->version;
    // Already crashed; a no-op.
    crashed(ServerId(1, 0));
    EXPECT_EQ(orig_version, sl->version);
    EXPECT_EQ(0U, sl->numberOfMasters);
    EXPECT_EQ(0U, sl->numberOfBackups);
}

TEST_F(CoordinatorServerListTest, crashed_trackerUpdated) {
    TestLog::Enable _(fireCallbackFilter);
    ServerId serverId = generateUniqueId();
    add(serverId, "hi!", {WireFormat::MASTER_SERVICE}, 100);
    crashed(serverId);
    EXPECT_EQ("fireCallback: called | fireCallback: called", TestLog::get());
    ASSERT_FALSE(tr->changes.empty());
    tr->changes.pop();
    ASSERT_FALSE(tr->changes.empty());
    auto& server = tr->changes.front().server;
    EXPECT_EQ(serverId, server.serverId);
    EXPECT_EQ("hi!", server.serviceLocator);
    EXPECT_EQ("MASTER_SERVICE", server.services.toString());
    // Not set when no BACKUP_SERVICE.
    EXPECT_EQ(0u, server.expectedReadMBytesPerSec);
    EXPECT_EQ(ServerStatus::CRASHED, server.status);
    EXPECT_EQ(SERVER_CRASHED, tr->changes.front().event);
}

TEST_F(CoordinatorServerListTest, generateUniqueId) {
    EXPECT_EQ(ServerId(1, 0), generateUniqueId());
    EXPECT_EQ(ServerId(2, 0), generateUniqueId());

    remove(ServerId(1, 0));
    EXPECT_EQ(ServerId(1, 1), generateUniqueId());
}

TEST_F(CoordinatorServerListTest, remove) {
    uint64_t orig_version = sl->version;
    EXPECT_THROW(remove(ServerId(0, 0)), Exception);
    EXPECT_EQ(orig_version, sl->version);

    ServerId serverId1 = generateUniqueId();
    add(serverId1, "hi!", {WireFormat::MASTER_SERVICE}, 100);
    CoordinatorServerList::Entry entryCopy = (*sl)[ServerId(1, 0)];
    EXPECT_EQ(1, sl->updates[0].server_size());

    EXPECT_NO_THROW(remove(ServerId(1, 0)));
    EXPECT_FALSE(sl->serverList[1].entry);
    // Critical that an UP server gets both crashed and down events.
    EXPECT_TRUE(protoBufMatchesEntry(sl->updates[1].server(0),
            entryCopy, ServerStatus::CRASHED));
    EXPECT_TRUE(protoBufMatchesEntry(sl->updates[1].server(1),
            entryCopy, ServerStatus::DOWN));

    orig_version = sl->version;
    EXPECT_THROW(remove(ServerId(1, 0)), Exception);
    EXPECT_EQ(orig_version, sl->version);
    EXPECT_EQ(0U, sl->numberOfMasters);
    EXPECT_EQ(0U, sl->numberOfBackups);

    ServerId serverId2 = generateUniqueId();
    add(serverId2, "hi, again", {WireFormat::BACKUP_SERVICE}, 100);
    EXPECT_EQ(uint32_t(ServerStatus::UP), sl->updates[2].server(0).status());
    crashed(ServerId(1, 1));
    EXPECT_EQ(uint32_t(ServerStatus::CRASHED),
            sl->updates[3].server(0).status());
    EXPECT_TRUE(sl->serverList[1].entry);
    EXPECT_THROW(remove(ServerId(1, 2)), Exception);
    EXPECT_NO_THROW(remove(ServerId(1, 1)));
    EXPECT_EQ(uint32_t(ServerStatus::DOWN), sl->updates[4].server(0).status());
    EXPECT_EQ(0U, sl->numberOfMasters);
    EXPECT_EQ(0U, sl->numberOfBackups);
}

TEST_F(CoordinatorServerListTest, remove_trackerUpdated) {
    TestLog::Enable _(fireCallbackFilter);
    ServerId serverId = generateUniqueId();
    add(serverId, "hi!", {WireFormat::MASTER_SERVICE}, 100);
    remove(serverId);
    EXPECT_EQ("fireCallback: called | fireCallback: called | "
            "fireCallback: called", TestLog::get());
    ASSERT_FALSE(tr->changes.empty());
    tr->changes.pop();
    ASSERT_FALSE(tr->changes.empty());
    tr->changes.pop();
    ASSERT_FALSE(tr->changes.empty());
    auto& server = tr->changes.front().server;
    EXPECT_EQ(serverId, server.serverId);
    EXPECT_EQ("hi!", server.serviceLocator);
    EXPECT_EQ("MASTER_SERVICE", server.services.toString());
    // Not set when no BACKUP_SERVICE.
    EXPECT_EQ(0u, server.expectedReadMBytesPerSec);
    EXPECT_EQ(ServerStatus::DOWN, server.status);
    EXPECT_EQ(SERVER_REMOVED, tr->changes.front().event);
}

TEST_F(CoordinatorServerListTest, indexOperator) {
    EXPECT_THROW((*sl)[ServerId(0, 0)], Exception);
    ServerId serverId = generateUniqueId();
    add(serverId, "yo!", {WireFormat::MASTER_SERVICE}, 100);
    EXPECT_EQ(ServerId(1, 0), (*sl)[ServerId(1, 0)].serverId);
    EXPECT_EQ("yo!", (*sl)[ServerId(1, 0)].serviceLocator);
    crashed(ServerId(1, 0));
    remove(ServerId(1, 0));
    EXPECT_THROW((*sl)[ServerId(1, 0)], Exception);
}

TEST_F(CoordinatorServerListTest, serialize) {
    {
        ProtoBuf::ServerList serverList;
        sl->serialize(serverList, {});
        EXPECT_EQ(0, serverList.server_size());
        sl->serialize(serverList, {WireFormat::MASTER_SERVICE,
            WireFormat::BACKUP_SERVICE});
        EXPECT_EQ(0, serverList.server_size());
    }

    ServerId first = generateUniqueId();
    add(first, "", {WireFormat::MASTER_SERVICE}, 100);
    ServerId second = generateUniqueId();
    add(second, "", {WireFormat::MASTER_SERVICE}, 100);
    ServerId third = generateUniqueId();
    add(third, "", {WireFormat::MASTER_SERVICE}, 100);
    ServerId fourth = generateUniqueId();
    add(fourth, "", {WireFormat::BACKUP_SERVICE}, 100);
    ServerId last = generateUniqueId();
    add(last, "", {WireFormat::MASTER_SERVICE,
        WireFormat::BACKUP_SERVICE}, 100);
    remove(first); // ensure removed entries are skipped
    crashed(last); // ensure crashed entries are included

    auto masterMask = ServiceMask{WireFormat::MASTER_SERVICE}.serialize();
    auto backupMask = ServiceMask{WireFormat::BACKUP_SERVICE}.serialize();
    auto bothMask = ServiceMask{WireFormat::MASTER_SERVICE,
        WireFormat::BACKUP_SERVICE}.serialize();
    {
        ProtoBuf::ServerList serverList;
        sl->serialize(serverList, {});
        EXPECT_EQ(0, serverList.server_size());
        sl->serialize(serverList, {WireFormat::MASTER_SERVICE});
        EXPECT_EQ(3, serverList.server_size());
        EXPECT_EQ(masterMask, serverList.server(0).services());
        EXPECT_EQ(masterMask, serverList.server(1).services());
        EXPECT_EQ(bothMask, serverList.server(2).services());
        EXPECT_EQ(ServerStatus::CRASHED,
                  ServerStatus(serverList.server(2).status()));
    }

    {
        ProtoBuf::ServerList serverList;
        sl->serialize(serverList, {WireFormat::BACKUP_SERVICE});
        EXPECT_EQ(2, serverList.server_size());
        EXPECT_EQ(backupMask, serverList.server(0).services());
        EXPECT_EQ(bothMask, serverList.server(1).services());
        EXPECT_EQ(ServerStatus::CRASHED,
                  ServerStatus(serverList.server(1).status()));
    }

    {
        ProtoBuf::ServerList serverList;
        sl->serialize(serverList, {WireFormat::MASTER_SERVICE,
                                   WireFormat::BACKUP_SERVICE});
        EXPECT_EQ(4, serverList.server_size());
        EXPECT_EQ(masterMask, serverList.server(0).services());
        EXPECT_EQ(masterMask, serverList.server(1).services());
        EXPECT_EQ(backupMask, serverList.server(2).services());
        EXPECT_EQ(bothMask, serverList.server(3).services());
        EXPECT_EQ(ServerStatus::CRASHED,
                  ServerStatus(serverList.server(3).status()));
    }
}

namespace {
bool statusFilter(string s) {
    return s != "checkStatus";
}
}

TEST_F(CoordinatorServerListTest, firstFreeIndex) {
    EXPECT_EQ(0U, sl->serverList.size());
    EXPECT_EQ(1U, sl->firstFreeIndex());
    EXPECT_EQ(2U, sl->serverList.size());
    ServerId serverId1 = generateUniqueId();
    add(serverId1, "hi", {WireFormat::MASTER_SERVICE}, 100);
    EXPECT_EQ(2U, sl->firstFreeIndex());
    ServerId serverId2 = generateUniqueId();
    add(serverId2, "hi again", {WireFormat::MASTER_SERVICE}, 100);
    EXPECT_EQ(3U, sl->firstFreeIndex());
    remove(ServerId(2, 0));
    EXPECT_EQ(2U, sl->firstFreeIndex());
    remove(ServerId(1, 0));
    EXPECT_EQ(1U, sl->firstFreeIndex());
}

TEST_F(CoordinatorServerListTest, getReferenceFromServerId) {
    EXPECT_THROW((*sl)[ServerId(0, 0)], Exception);
    EXPECT_THROW((*sl)[ServerId(1, 0)], Exception);
    ServerId serverId = generateUniqueId();
    add(serverId, "", {WireFormat::MASTER_SERVICE}, 100);
    EXPECT_THROW((*sl)[ServerId(0, 0)], Exception);
    EXPECT_NO_THROW((*sl)[ServerId(1, 0)]);
    EXPECT_THROW((*sl)[ServerId(1, 1)], Exception);
    EXPECT_THROW((*sl)[ServerId(2, 0)], Exception);
}

TEST_F(CoordinatorServerListTest, Entry_constructor) {
    CoordinatorServerList::Entry a(ServerId(52, 374),
        "You forgot your boarding pass", {WireFormat::MASTER_SERVICE});
    EXPECT_EQ(ServerId(52, 374), a.serverId);
    EXPECT_EQ("You forgot your boarding pass", a.serviceLocator);
    EXPECT_TRUE(a.isMaster());
    EXPECT_FALSE(a.isBackup());
    EXPECT_EQ(0U, a.expectedReadMBytesPerSec);

    CoordinatorServerList::Entry b(ServerId(27, 72),
        "I ain't got time to bleed", { WireFormat::BACKUP_SERVICE});
    EXPECT_EQ(ServerId(27, 72), b.serverId);
    EXPECT_EQ("I ain't got time to bleed", b.serviceLocator);
    EXPECT_FALSE(b.isMaster());
    EXPECT_TRUE(b.isBackup());
    EXPECT_EQ(0U, b.expectedReadMBytesPerSec);
}

TEST_F(CoordinatorServerListTest, Entry_serialize) {
    CoordinatorServerList::Entry entry(ServerId(0, 0), "",
                                       {WireFormat::BACKUP_SERVICE});
    entry.serverId = ServerId(5234, 23482);
    entry.serviceLocator = "giggity";
    entry.expectedReadMBytesPerSec = 723;

    ProtoBuf::ServerList_Entry serialEntry;
    entry.serialize(serialEntry);
    auto backupMask = ServiceMask{WireFormat::BACKUP_SERVICE}.serialize();
    EXPECT_EQ(backupMask, serialEntry.services());
    EXPECT_EQ(ServerId(5234, 23482).getId(), serialEntry.server_id());
    EXPECT_EQ("giggity", serialEntry.service_locator());
    EXPECT_EQ(723U, serialEntry.expected_read_mbytes_per_sec());
    EXPECT_EQ(ServerStatus::UP, ServerStatus(serialEntry.status()));

    entry.services = ServiceMask{WireFormat::MASTER_SERVICE};
    ProtoBuf::ServerList_Entry serialEntry2;
    entry.serialize(serialEntry2);
    auto masterMask = ServiceMask{WireFormat::MASTER_SERVICE}.serialize();
    EXPECT_EQ(masterMask, serialEntry2.services());
    EXPECT_EQ(0U, serialEntry2.expected_read_mbytes_per_sec());
    EXPECT_EQ(ServerStatus::UP, ServerStatus(serialEntry2.status()));
}

TEST_F(CoordinatorServerListTest, addServerInfoLogId) {
    ServerId serverId = generateUniqueId();
    add(serverId, "", {WireFormat::MASTER_SERVICE}, 100);

    Lock lock(mutex); // tricks private calls
    sl->addServerInfoLogId(lock, serverId, 10);

    CoordinatorServerList::Entry entry((*sl)[serverId]);
    EXPECT_EQ(10U, entry.serverInfoLogId);
}

TEST_F(CoordinatorServerListTest, getServerInfoLogId) {
    ServerId serverId = generateUniqueId();
    add(serverId, "", {WireFormat::MASTER_SERVICE}, 100);

    Lock lock(mutex); // tricks private calls
    CoordinatorServerList::Entry& entry =
        const_cast<CoordinatorServerList::Entry&>(
            sl->getReferenceFromServerId(lock, serverId));
    entry.serverInfoLogId = 10U;

    LogCabin::Client::EntryId entryId = sl->getServerInfoLogId(lock, serverId);
    EXPECT_EQ(10U, entryId);
}

TEST_F(CoordinatorServerListTest, addServerUpdateLogId) {
    ServerId serverId = generateUniqueId();
    add(serverId, "", {WireFormat::MASTER_SERVICE}, 100);

    Lock lock(mutex); // tricks private calls
    sl->addServerUpdateLogId(lock, serverId, 10);

    CoordinatorServerList::Entry entry((*sl)[serverId]);
    EXPECT_EQ(10U, entry.serverUpdateLogId);
}

TEST_F(CoordinatorServerListTest, getServerUpdateLogId) {
    ServerId serverId = generateUniqueId();
    add(serverId, "", {WireFormat::MASTER_SERVICE}, 100);

    Lock lock(mutex); // tricks private calls
    CoordinatorServerList::Entry& entry =
            const_cast<CoordinatorServerList::Entry&> (
            sl->getReferenceFromServerId(lock, serverId));
    entry.serverUpdateLogId = 10U;

    LogCabin::Client::EntryId entryId =
            sl->getServerUpdateLogId(lock, serverId);
    EXPECT_EQ(10U, entryId);
}

TEST_F(CoordinatorServerListTest, isClusterUpToDate) {
    sl->haltUpdater();

    ServerId id1 = generateUniqueId();
    add(id1, "mock:host=server1", {WireFormat::MEMBERSHIP_SERVICE}, 100);
    ServerId id2 = generateUniqueId();
    add(id2, "mock:host=server2", {WireFormat::MEMBERSHIP_SERVICE}, 100);
    EXPECT_FALSE(isClusterUpToDate());

    // Normal update path
    sl->updateEntryVersion(id1, sl->version);
    EXPECT_FALSE(isClusterUpToDate());
    sl->updateEntryVersion(id2, sl->version);
    EXPECT_TRUE(isClusterUpToDate());
    crashed(id2);
    EXPECT_FALSE(isClusterUpToDate());
    sl->updateEntryVersion(id1, sl->version);
    EXPECT_TRUE(isClusterUpToDate());

    // Not eligible for update, but others are
    ServerId id3 = generateUniqueId();
    add(id3, "mock:host=server3", {WireFormat::BACKUP_SERVICE}, 100);
    EXPECT_FALSE(isClusterUpToDate());
    sl->updateEntryVersion(id1, 1);
    EXPECT_FALSE(isClusterUpToDate());
    sl->updateEntryVersion(id1, sl->version);
    EXPECT_TRUE(isClusterUpToDate());

    // Server List now contains no servers that can receive updates
    // so it's automatically "up-to-date"
    ServerId id4 = generateUniqueId();
    add(id4, "mock:host=server4", {}, 100);
    remove(id1);
    EXPECT_TRUE(isClusterUpToDate());

    // Test that even tho a server is "updating" the cluster is not up to date
    ServerId id5 = generateUniqueId();
    add(id5, "mock:host=server5", {WireFormat::MEMBERSHIP_SERVICE}, 100);

    Lock lock(mutex);
    CoordinatorServerList::Entry& entry =
            const_cast<CoordinatorServerList::Entry&>
            (sl->getReferenceFromServerId(lock, id5));
    lock.unlock();
    entry.serverListVersion = sl->version;
    EXPECT_TRUE(isClusterUpToDate());
    entry.isBeingUpdated = sl->version;
    EXPECT_FALSE(isClusterUpToDate());
    entry.isBeingUpdated = 0;
    EXPECT_TRUE(isClusterUpToDate());
}

TEST_F(CoordinatorServerListTest, commitUpdate) {
    Lock lock(mutex); // Used to trick internal calls
    ProtoBuf::ServerList& update = sl->update;
    uint64_t orig_ver = sl->version;
    sl->lastScan.noUpdatesFound = true;

    // Empty update, not committed
    sl->commitUpdate(lock);
    EXPECT_EQ(orig_ver, sl->version);
    EXPECT_TRUE(sl->updates.empty());
    EXPECT_TRUE(sl->lastScan.noUpdatesFound);

    // Update with at least something in it
    update.add_server();
    sl->commitUpdate(lock);
    EXPECT_EQ(orig_ver + 1, sl->version);
    EXPECT_EQ(1UL, sl->updates.size());
    EXPECT_FALSE(sl->lastScan.noUpdatesFound);

    // Add a second one in
    update.add_server();
    sl->commitUpdate(lock);
    EXPECT_EQ(2UL, sl->updates.size());
    EXPECT_EQ(orig_ver + 1, sl->updates.front().version_number());
    EXPECT_EQ(orig_ver + 2, sl->updates.back().version_number());
}

TEST_F(CoordinatorServerListTest, pruneUpdates) {
    Lock lock(mutex); // Used to trick internal calls.
    ProtoBuf::ServerList& update = sl->update;
    for (int i = 1; i <= 10; i++) {
        for (int j = 1; j <= i; j++)
            update.add_server();
        sl->commitUpdate(lock);
    }

    EXPECT_EQ(10UL, sl->version);
    EXPECT_EQ(10UL, sl->updates.size());

    // Nothing should be pruned
    sl->pruneUpdates(lock, 0);
    EXPECT_EQ(10UL, sl->version);
    EXPECT_EQ(10UL, sl->updates.size());

    // Normal Prune
    sl->pruneUpdates(lock, 4);
    EXPECT_EQ(10UL, sl->version);
    EXPECT_EQ(6UL, sl->updates.size());

    for (int i = 5; i <= 10; i++) {
        ProtoBuf::ServerList& update = sl->updates.at(i - 5);
        EXPECT_EQ(i, static_cast<int>(update.version_number()));
        EXPECT_EQ(i, update.server_size());
    }

    // No-op
    sl->pruneUpdates(lock, 2);
    EXPECT_EQ(6UL, sl->updates.size());
}

TEST_F(CoordinatorServerListTest, startUpdater) {
    EXPECT_TRUE(sl->stopUpdater);
    EXPECT_FALSE(sl->thread);

    sl->startUpdater();

    EXPECT_FALSE(sl->stopUpdater);
    EXPECT_TRUE(sl->thread);
    EXPECT_TRUE(sl->thread->joinable());
}

TEST_F(CoordinatorServerListTest, haltUpdater) {
    sl->startUpdater();
    EXPECT_FALSE(sl->stopUpdater);
    EXPECT_TRUE(sl->thread);
    EXPECT_TRUE(sl->thread->joinable());

    sl->haltUpdater();
    EXPECT_FALSE(sl->thread);
    EXPECT_TRUE(sl->stopUpdater);
}

TEST_F(CoordinatorServerListTest, hasUpdates) {
    sl->haltUpdater();

    // Empty list
    EXPECT_FALSE(hasUpdates());

    ServerId id1 = generateUniqueId();
    add(id1, "mock:host=server1", {WireFormat::MASTER_SERVICE,
        WireFormat::MEMBERSHIP_SERVICE}, 100);
    ServerId id2 = generateUniqueId();
    add(id2, "mock:host=server2", {WireFormat::MASTER_SERVICE}, 100);
    EXPECT_TRUE(hasUpdates());

    sl->updateEntryVersion(id1, sl->version);
    EXPECT_FALSE(hasUpdates());

    crashed(id2);
    EXPECT_TRUE(hasUpdates());
    sl->updateEntryVersion(id1, sl->version);
    EXPECT_FALSE(hasUpdates());

    // Impossible case, but just in case
    sl->updateEntryVersion(id1, 0);
    EXPECT_TRUE(hasUpdates());

    sl->serverList[id1.indexNumber()].entry->isBeingUpdated = sl->version;
    EXPECT_FALSE(hasUpdates());

    sl->updateEntryVersion(id1, sl->version);
    sl->lastScan.noUpdatesFound = false;
    EXPECT_FALSE(hasUpdates());

    // Check that the minVersion and updates have been pruned completely
    EXPECT_TRUE(sl->updates.empty());
}

TEST_F(CoordinatorServerListTest, hasUpdates_partialPrune) {
    sl->haltUpdater();

    // Add three candidates and update them striped.
    ServerId id1 = generateUniqueId();
    add(id1, "mock:host=server1", {WireFormat::MEMBERSHIP_SERVICE}, 100);
    ServerId id2 = generateUniqueId();
    add(id2, "mock:host=server2", {WireFormat::MEMBERSHIP_SERVICE}, 100);
    ServerId id3 = generateUniqueId();
    add(id3, "mock:host=server3", {WireFormat::MEMBERSHIP_SERVICE}, 100);
    EXPECT_TRUE(hasUpdates());
    EXPECT_EQ(3UL, sl->version);
    EXPECT_EQ(3UL, sl->updates.size());

    sl->updateEntryVersion(id1, 0);
    sl->updateEntryVersion(id2, 2);
    sl->updateEntryVersion(id3, 3);
    EXPECT_TRUE(hasUpdates());

    // Make sure no prunes occur while updating
    sl->serverList[id1.indexNumber()].entry->isBeingUpdated = sl->version;
    sl->serverList[id2.indexNumber()].entry->isBeingUpdated = sl->version;
    sl->serverList[id3.indexNumber()].entry->isBeingUpdated = sl->version;
    EXPECT_EQ(3UL, sl->updates.size());
    sl->lastScan.noUpdatesFound = false; // tricks full rescan
    EXPECT_FALSE(hasUpdates()); // False because all are updating

    // Update first one and expect prune to version 3
    sl->updateEntryVersion(id1, 2);
    EXPECT_TRUE(hasUpdates());
    sl->serverList[id1.indexNumber()].entry->isBeingUpdated = sl->version;

    EXPECT_EQ(1UL, sl->updates.size());
    EXPECT_EQ(3UL, sl->updates.front().version_number());

    // Clean up to allow teardown.
    sl->updateEntryVersion(id1, sl->version);
    sl->updateEntryVersion(id2, sl->version);
    sl->updateEntryVersion(id3, sl->version);
}

TEST_F(CoordinatorServerListTest, hasUpdates_specialCase) {
    // This case was discovered in a bug whereby the prune was pruning too much
    // when it assumes an entry with version 0 will always update to the
    // latest version. The case this would not be true is in this timeline:
    // Server1 -> initiate update from 0 -> 2
    // Server2 added
    // Server2 -> initiate and finish update 0 -> 3
    // Prune Occurs (pruning up to 3)
    // Server1 -> finish update to 2, asks for pruned 3.
    sl->haltUpdater();

    ServerId id1 = generateUniqueId();
    add(id1, "mock:host=server1", {WireFormat::MEMBERSHIP_SERVICE}, 100);
    EXPECT_TRUE(hasUpdates());
    EXPECT_FALSE(sl->updates.empty());
    Lock lock(mutex);
    (const_cast<CoordinatorServerList::Entry&>
            (sl->getReferenceFromServerId(lock, id1))).isBeingUpdated =
            sl->version;
    lock.unlock();
    EXPECT_FALSE(hasUpdates());

    // Start and finish update of server 2
    ServerId id2 = generateUniqueId();
    add(id2, "mock:host=server1", {WireFormat::MEMBERSHIP_SERVICE}, 100);
    EXPECT_TRUE(hasUpdates());
    sl->updateEntryVersion(id2, sl->version);
    EXPECT_FALSE(hasUpdates());
    EXPECT_FALSE(sl->updates.empty());

    // make sure the update 1->2 has not been pruned
    EXPECT_EQ(sl->version, sl->updates.front().version_number());

    // Clean up to allow teardown.
    sl->updateEntryVersion(id1, sl->version);
    sl->updateEntryVersion(id2, sl->version);
}

TEST_F(CoordinatorServerListTest, updateEntryVersion) {
    sl->haltUpdater();
    ServerId id1 = generateUniqueId();
    add(id1, "mock:host=server1", {WireFormat::MASTER_SERVICE}, 100);
    EXPECT_EQ(0UL, (*sl)[id1].serverListVersion);

    sl->updateEntryVersion(id1, 2358);
    EXPECT_EQ(2358UL, (*sl)[id1].serverListVersion);
    EXPECT_EQ(0UL, (*sl)[id1].isBeingUpdated);
}

TEST_F(CoordinatorServerListTest, updateEntryVersion_booleanToggles) {
    sl->haltUpdater();

    ServerId id1 = generateUniqueId();
    add(id1, "mock:host=server1", {WireFormat::MASTER_SERVICE}, 100);

    sl->version = 10;
    sl->serverList[id1.indexNumber()].entry->isBeingUpdated = 100;
    sl->lastScan.noUpdatesFound = true;

    sl->updateEntryVersion(id1, 10);
    EXPECT_TRUE(sl->lastScan.noUpdatesFound);
    EXPECT_EQ(0UL, (*sl)[id1].isBeingUpdated);

    // Lower update version should trigger noUpdatesFound
    sl->updateEntryVersion(id1, 2);
    EXPECT_FALSE(sl->lastScan.noUpdatesFound);
}

TEST_F(CoordinatorServerListTest, updateLoop) {
    MockTransport transport(service->context);
    TransportManager::MockRegistrar _(service->context, transport);
    Lock lock(mutex);   // Trick for internal calls

    // Test unoccupied server slot. Remove must wait until after last add to
    // ensure slot isn't recycled.
    ServerId serverId1 = sl->generateUniqueId(lock);
    sl->add(lock, serverId1, "mock:host=server1",
            {WireFormat::MEMBERSHIP_SERVICE}, 0);

    // Test crashed server gets skipped as a recipient.
    ServerId serverId2 = sl->generateUniqueId(lock);
    sl->add(lock, serverId2, "mock:host=server2", {}, 0);
    sl->crashed(lock, serverId2);

    // Test server with no membership service.
    ServerId serverId3 = sl->generateUniqueId(lock);
    sl->add(lock, serverId3, "mock:host=server3", {}, 0);

    // Only Server that can receive updates
    ServerId serverId4 = sl->generateUniqueId(lock);
    sl->add(lock, serverId4, "mock:host=server4",
            {WireFormat::MEMBERSHIP_SERVICE}, 0);
    sl->remove(lock, serverId1);

    TestLog::Enable __(statusFilter);

    transport.setInput("0"); // Server 4 response -> ok

    // Send Full List to server 4
    sl->commitUpdate(lock);
    sl->sync();
    EXPECT_EQ("updateEntryVersion: server 4.0 updated (0->1)", TestLog::get());
    EXPECT_EQ("sendRequest: 0x40023 4 0 11 273 0 /0 /x18/0",
               transport.outputLog);

    TestLog::reset();
    transport.outputLog = "";

    transport.setInput("0"); // Server 5 full list received
    transport.setInput("0"); // Server 4 update received
    transport.setInput("0"); // Server 4 update received
    transport.setInput("0"); // Server 4 update received

    // Add two more servers eligible for updates and crash one
    ServerId serverId5 = sl->generateUniqueId(lock);
    sl->add(lock, serverId5, "mock:host=server5",
            {WireFormat::MEMBERSHIP_SERVICE}, 0);
    ServerId serverId6 = sl->generateUniqueId(lock);
    sl->add(lock, serverId6, "mock:host=server6",
            {WireFormat::MEMBERSHIP_SERVICE}, 0);
    sl->crashed(lock, serverId6);

    TestLog::reset();
    sl->commitUpdate(lock);
    sl->sync();
    EXPECT_EQ("updateEntryVersion: server 1.1 updated (0->2) | "
              "updateEntryVersion: server 4.0 updated (1->2)",
              TestLog::get());

    EXPECT_EQ(
        "sendRequest: 0x40023 1 1 11 529 0 /0 /x18/0 | "
        "sendRequest: 0x40023 4 0 173 0x100d340a 0x11000000 1 1 0x6f6d111a "
        "ck:host=server5-/0 0x35000000 0 57 0 0xd340a00 16 1297 0 0x6d111a00 "
        "ock:host=server6-/0 0x35000000 0 57 0 0xd340a00 16 1297 0 0x6d111a00 "
        "ock:host=server6-/0 0x35000000 1 57 0 0x21100 0 0x1180000",
        transport.outputLog);
}

TEST_F(CoordinatorServerListTest, updateLoop_multiAllAtOnce) {
    MockTransport transport(service->context);
    TransportManager::MockRegistrar _(service->context, transport);
    sl->haltUpdater();

    sl->concurrentRPCs = 2;
    for (int i = 1; i <= 5; i++) {
        transport.setInput("0");
        ServerId id = generateUniqueId();
        add(id, "mock:host=server_xxx", {WireFormat::MEMBERSHIP_SERVICE}, 100);
    }

    TestLog::Enable __;
    sl->sync();

    EXPECT_EQ("updateEntryVersion: server 1.0 updated (0->5) | "
              "updateEntryVersion: server 2.0 updated (0->5) | "
              "updateEntryVersion: server 3.0 updated (0->5) | "
              "updateEntryVersion: server 4.0 updated (0->5) | "
              "updateEntryVersion: server 5.0 updated (0->5)",
               TestLog::get());
}

namespace {
    bool updateEntryFilter(string s) {
        return s == "updateEntryVersion";
    }
}

TEST_F(CoordinatorServerListTest, updateLoop_multiStriped) {
    MockTransport transport(service->context);
    TransportManager::MockRegistrar _(service->context, transport);
    sl->haltUpdater();

    TestLog::Enable __(updateEntryFilter);
    sl->concurrentRPCs = 2;
    for (int i = 1; i <= 4; i++) {
        ServerId id = generateUniqueId();
        add(id, "mock:host=server_xxx",
                    {WireFormat::MEMBERSHIP_SERVICE}, 100);

        // (every n-th server will cause n updates)
        for (int j = 0; j < i; j++)
            transport.setInput("0");

        sl->sync();

        // loop should prune updates
        EXPECT_GE(1LU, sl->updates.size());
    }

    EXPECT_EQ("updateEntryVersion: server 1.0 updated (0->1) | "
              "updateEntryVersion: server 1.0 updated (1->2) | "
              "updateEntryVersion: server 2.0 updated (0->2) | "
              "updateEntryVersion: server 1.0 updated (2->3) | "
              "updateEntryVersion: server 2.0 updated (2->3) | "
              "updateEntryVersion: server 3.0 updated (0->3) | "
              "updateEntryVersion: server 1.0 updated (3->4) | "
              "updateEntryVersion: server 2.0 updated (3->4) | "
              "updateEntryVersion: server 3.0 updated (3->4) | "
              "updateEntryVersion: server 4.0 updated (0->4)",
               TestLog::get());
}

namespace {
bool updateLoopFilter(string s) {
    return s == "updateLoop";
}
}

TEST_F(CoordinatorServerListTest, updateLoop_expansion) {
    MockTransport transport(service->context);
    TransportManager::MockRegistrar _(service->context, transport);
    sl->haltUpdater();

    // Start with 0 and grow to 5; since mockTransport RPCs finish
    // instantaneously, we have to add all (5*4)/2 entries at once
    // But there will be 1 contraction in the final round,
    // causing the total to be 4
    sl->concurrentRPCs = 1;

    for (int i = 0; i < 10; i++) {
        ServerId id = generateUniqueId();
        add(id, "mock:host=server", {WireFormat::MEMBERSHIP_SERVICE}, 100);
        transport.setInput("0");
    }

    sl->sync();
    EXPECT_EQ(4UL, sl->concurrentRPCs);

    // Test that syncs w/o updates don't lower the slot count
    sl->sync();
    sl->sync();
    EXPECT_EQ(4UL, sl->concurrentRPCs);
}

TEST_F(CoordinatorServerListTest, updateLoop_contraction) {
    MockTransport transport(service->context);
    TransportManager::MockRegistrar _(service->context, transport);
    sl->haltUpdater();
    sl->concurrentRPCs = 5;

    // Single contraction since 2 < 5
    ServerId id = generateUniqueId();
    add(id, "mock:host=server", {WireFormat::MEMBERSHIP_SERVICE}, 100);
    id = generateUniqueId();
    add(id, "mock:host=server", {WireFormat::MEMBERSHIP_SERVICE}, 100);
    transport.setInput("0");
    transport.setInput("0");
    sl->sync();
    EXPECT_EQ(4UL, sl->concurrentRPCs);

    // 3 < 4 contraction
    id = generateUniqueId();
    add(id, "mock:host=server", {WireFormat::MEMBERSHIP_SERVICE}, 100);
    transport.setInput("0");
    transport.setInput("0");
    transport.setInput("0");
    sl->sync();
    EXPECT_EQ(3UL, sl->concurrentRPCs);

    // 4 updates, no change (1 expand + 1 contract)
    id = generateUniqueId();
    add(id, "mock:host=server", {WireFormat::MEMBERSHIP_SERVICE}, 100);
    transport.setInput("0");
    transport.setInput("0");
    transport.setInput("0");
    transport.setInput("0");
    sl->sync();
    EXPECT_EQ(3UL, sl->concurrentRPCs);

    // 5 + 7 = 13 updates (2 expand + 1 contract)
    Lock lock(mutex);
    id = sl->generateUniqueId(lock);
    sl->add(lock, id, "mock:host=server",
                {WireFormat::MEMBERSHIP_SERVICE}, 100);
    sl->commitUpdate(lock);
    id = sl->generateUniqueId(lock);
    sl->add(lock, id, "mock:host=server",
                {WireFormat::MEMBERSHIP_SERVICE}, 100);
    sl->commitUpdate(lock);
    for (int i = 0; i < 12; i++)
        transport.setInput("0");
    sl->sync();
    EXPECT_EQ(4UL, sl->concurrentRPCs);
}

TEST_F(CoordinatorServerListTest, sync) {
    // Test that sync wakes up thread and flushes all updates
    MockTransport transport(service->context);
    TransportManager::MockRegistrar _(service->context, transport);
    sl->haltUpdater();

    ServerId id = generateUniqueId();
    add(id, "mock:host=server", {WireFormat::MEMBERSHIP_SERVICE}, 100);
    transport.setInput("0");

    sl->sync();
    EXPECT_EQ("sendRequest: 0x40023 1 0 11 273 0 /0 /x18/0",
            transport.outputLog);

    // Test that syncs on up-to-date list don't clog
    sl->sync();
    sl->sync();
}

TEST_F(CoordinatorServerListTest, handleRpc) {
    sl->haltUpdater();
    sl->rpcTimeoutNs = 1000;
    MockTransport transport(service->context);
    TransportManager::MockRegistrar _(service->context, transport);
    CoordinatorServerList::UpdateSlot slot;

    // No servers in server list so no RPCs out
    sl->dispatchRpc(slot);
    EXPECT_FALSE(slot.rpc);
    EXPECT_FALSE(slot.rpc);

    // Expect update to id1
    ServerId id1 = generateUniqueId();
    add(id1, "mock:host=server1", {WireFormat::MEMBERSHIP_SERVICE}, 0);

    ServerId id2 = generateUniqueId();
    add(id2, "mock:host=server1", {WireFormat::MEMBERSHIP_SERVICE}, 0);

    sl->dispatchRpc(slot);
    EXPECT_TRUE(slot.rpc);
    EXPECT_TRUE(slot.rpc);
    EXPECT_EQ(ProtoBuf::ServerList_Type_FULL_LIST, slot.protobuf.type());
    EXPECT_EQ(0UL, slot.originalVersion);
    EXPECT_EQ(2UL, slot.protobuf.version_number());

    // Expect a Timeout to roll over to updating id2
    TestLog::Enable __;
    Cycles::mockTscValue = slot.startCycle +
            Cycles::fromNanoseconds(sl->rpcTimeoutNs) + 100;
    sl->dispatchRpc(slot);
    EXPECT_EQ("dispatchRpc: ServerList update to 1.0 timed out after 0 ms; "
            "trying again later | "
            "updateEntryVersion: server 1.0 updated (0->0)", TestLog::get());
    EXPECT_TRUE(slot.rpc);
    EXPECT_EQ(id2, slot.serverId);

    // Time out again, expect to roll back over to id1
    TestLog::reset();
    Cycles::mockTscValue = slot.startCycle +
            Cycles::fromNanoseconds(sl->rpcTimeoutNs) + 100;
    sl->dispatchRpc(slot);
    EXPECT_EQ("dispatchRpc: ServerList update to 2.0 timed out after 0 ms; "
              "trying again later | "
              "updateEntryVersion: server 2.0 updated (0->0)", TestLog::get());
    EXPECT_TRUE(slot.rpc);
    EXPECT_EQ(id1, slot.serverId);

    // OK responses
    transport.setInput("0");
    transport.setInput("0");
    Cycles::mockTscValue = 0;

    TestLog::reset();
    sl->dispatchRpc(slot);      // fails due to time roll back to 0 (underflow)
    sl->dispatchRpc(slot);      // Updates 2
    sl->dispatchRpc(slot);      // Updates 1
    EXPECT_EQ("dispatchRpc: ServerList update to 1.0 timed out after 0 ms; "
              "trying again later | "
              "updateEntryVersion: server 1.0 updated (0->0) | "
              "updateEntryVersion: server 2.0 updated (0->2) | "
              "updateEntryVersion: server 1.0 updated (0->2)", TestLog::get());
    EXPECT_FALSE(slot.rpc);
}

TEST_F(CoordinatorServerListTest, handleRpc_nonExistantServer) {
    sl->haltUpdater();

    MockTransport transport(service->context);
    TransportManager::MockRegistrar _(service->context, transport);

    CoordinatorServerList::UpdateSlot slot;
    ServerId id1 = generateUniqueId();
    add(id1, "mock:host=server1", {WireFormat::MEMBERSHIP_SERVICE}, 0);

    // Start an RPC to id1
    transport.setInput("20"); // ServerDoesNotExist!
    sl->dispatchRpc(slot); // Loads the Rpc first
    EXPECT_TRUE(slot.rpc);

    remove(id1);
    TestLog::Enable __;
    sl->dispatchRpc(slot); // check status
    EXPECT_FALSE(slot.rpc);

    EXPECT_EQ("dispatchRpc: Async update to 1.0 occurred during/after it was "
            "crashed/downed in the CoordinatorServerList.", TestLog::get());
}

TEST_F(CoordinatorServerListTest, loadNextUpdate) {
    sl->haltUpdater();
    CoordinatorServerList::UpdateSlot slot;

    // Empty list
    EXPECT_FALSE(sl->loadNextUpdate(slot));

    // Load 2 updatable servers and 1 not updatable
    ServerId id1 = generateUniqueId();
    add(id1, "mock:host=server1", {WireFormat::MASTER_SERVICE,
        WireFormat::MEMBERSHIP_SERVICE}, 100);
    ServerId id2 = generateUniqueId();
    add(id2, "mock:host=server2", {WireFormat::MASTER_SERVICE}, 100);
    ServerId id3 = generateUniqueId();
    add(id3, "mock:host=server3", {WireFormat::MEMBERSHIP_SERVICE}, 100);

    EXPECT_TRUE(sl->loadNextUpdate(slot));      // id1 update
    EXPECT_EQ(id1, slot.serverId);
    EXPECT_EQ(sl->version, slot.protobuf.version_number());
    EXPECT_EQ(ProtoBuf::ServerList_Type_FULL_LIST, slot.protobuf.type());

    EXPECT_TRUE(sl->loadNextUpdate(slot));      // id3 update
    EXPECT_EQ(id3, slot.serverId);
    EXPECT_EQ(ProtoBuf::ServerList_Type_FULL_LIST, slot.protobuf.type());
    EXPECT_EQ(sl->version, slot.protobuf.version_number());

    EXPECT_FALSE(sl->loadNextUpdate(slot)); // no more id2 doesn't qualify

    // generate more version updates via non-updatable server additions
    ServerId id4 = generateUniqueId();
    add(id4, "mock:host=server3", {WireFormat::MASTER_SERVICE}, 100);
    ServerId id5 = generateUniqueId();
    add(id4, "mock:host=server3", {WireFormat::MASTER_SERVICE}, 100);
    EXPECT_FALSE(sl->loadNextUpdate(slot));

    remove(id1);
    sl->updateEntryVersion(id3, 4);

    EXPECT_FALSE(sl->lastScan.noUpdatesFound);
    EXPECT_TRUE(sl->loadNextUpdate(slot));
    EXPECT_EQ(id3, slot.serverId);
    EXPECT_EQ(ProtoBuf::ServerList_Type_UPDATE, slot.protobuf.type());
    EXPECT_EQ(5UL, slot.protobuf.version_number());
    EXPECT_FALSE(sl->loadNextUpdate(slot));

    // expect prune updates to work
    EXPECT_EQ(5UL, sl->updates.front().version_number());

    // Crash our remaining server with membership updates
    crashed(id3);
    EXPECT_FALSE(sl->loadNextUpdate(slot));
}

TEST_F(CoordinatorServerListTest, assignReplicationGroup) {
    Lock lock(mutex); // Used to trick internal calls

    vector<ServerId> serverIds;
    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::BACKUP_SERVICE,
        WireFormat::MEMBERSHIP_SERVICE, WireFormat::PING_SERVICE};
    for (uint32_t i = 0; i < 3; i++) {
        config.localLocator = format("mock:host=backup%u", i);
        serverIds.push_back(cluster.addServer(config)->serverId);
    }

    // Check normal functionality.
    EXPECT_TRUE(sl->assignReplicationGroup(lock, 10U, serverIds));
    EXPECT_EQ(10U, (*sl)[serverIds[0]].replicationId);
    EXPECT_EQ(10U, (*sl)[serverIds[1]].replicationId);
    EXPECT_EQ(10U, (*sl)[serverIds[2]].replicationId);

    service->forceServerDownForTesting = false;
}

TEST_F(CoordinatorServerListTest, createReplicationGroup) {
    Lock lock(mutex); // Used to trick internal calls

    ServerId serverIds[10];
    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::BACKUP_SERVICE,
        WireFormat::MEMBERSHIP_SERVICE, WireFormat::PING_SERVICE};
    for (uint32_t i = 0; i < 8; i++) {
        config.localLocator = format("mock:host=backup%u", i);
        serverIds[i] = cluster.addServer(config)->serverId;
    }

    EXPECT_EQ(1U, (*sl)[serverIds[0]].replicationId);
    EXPECT_EQ(1U, (*sl)[serverIds[1]].replicationId);
    EXPECT_EQ(1U, (*sl)[serverIds[2]].replicationId);
    EXPECT_EQ(2U, (*sl)[serverIds[3]].replicationId);
    EXPECT_EQ(2U, (*sl)[serverIds[4]].replicationId);
    EXPECT_EQ(2U, (*sl)[serverIds[5]].replicationId);
    EXPECT_EQ(0U, (*sl)[serverIds[6]].replicationId);
    EXPECT_EQ(0U, (*sl)[serverIds[7]].replicationId);
    // Kill server 7.
    service->forceServerDownForTesting = true;
    sl->serverDown(lock, serverIds[7]);
    service->forceServerDownForTesting = false;
    // Create a new server.
    config.localLocator = format("mock:host=backup%u", 9);
    serverIds[8] = cluster.addServer(config)->serverId;
    EXPECT_EQ(0U, (*sl)[serverIds[6]].replicationId);
    EXPECT_EQ(0U, (*sl)[serverIds[8]].replicationId);
    config.localLocator = format("mock:host=backup%u", 10);
    serverIds[9] = cluster.addServer(config)->serverId;
    EXPECT_EQ(3U, (*sl)[serverIds[6]].replicationId);
    EXPECT_EQ(3U, (*sl)[serverIds[9]].replicationId);
}

TEST_F(CoordinatorServerListTest, enlistServer) {
    enlistMaster();
    EXPECT_EQ(1U, master->serverId.getId());
    EXPECT_EQ(ServerId(2, 0),
              sl->enlistServer({}, {WireFormat::BACKUP_SERVICE}, 0,
                               "mock:host=backup"));

    ProtoBuf::ServerList masterList;
    sl->serialize(masterList, {WireFormat::MASTER_SERVICE});
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
                "server { services: 25 server_id: 1 "
                "service_locator: \"mock:host=master\" "
                "expected_read_mbytes_per_sec: [0-9]\\+ status: 0 "
                "replication_id: 0 } "
                "version_number: 2",
                 masterList.ShortDebugString()));

    ProtoBuf::ServerList backupList;
    sl->serialize(backupList, {WireFormat::BACKUP_SERVICE});
    EXPECT_EQ("server { services: 2 server_id: 2 "
              "service_locator: \"mock:host=backup\" "
              "expected_read_mbytes_per_sec: 0 status: 0 "
              "replication_id: 0 } "
              "version_number: 2 type: FULL_LIST",
               backupList.ShortDebugString());
}

namespace {
bool startMasterRecoveryFilter(string s) {
    return s == "startMasterRecovery";
}
}

TEST_F(CoordinatorServerListTest, enlistServer_ReplaceAMaster) {
    enlistMaster();
    service->context->recoveryManager->doNotStartRecoveries = true;

    ServerTracker<void> tracker(service->context);
    ASSERT_EQ(sl, tracker.parent);

    ramcloud->createTable("foo");
    TestLog::Enable _(startMasterRecoveryFilter);
    EXPECT_EQ(ServerId(2, 0),
              sl->enlistServer(masterServerId, {WireFormat::BACKUP_SERVICE},
                               0, "mock:host=backup"));
    EXPECT_EQ("startMasterRecovery: Scheduling recovery of master 1.0 | "
              "startMasterRecovery: Recovery crashedServerId: 1.0",
               TestLog::get());
    EXPECT_TRUE(sl->contains(masterServerId));
    EXPECT_EQ(ServerStatus::CRASHED, sl->operator[](masterServerId).status);

    ServerDetails details;
    ServerChangeEvent event;
    ASSERT_TRUE(tracker.getChange(details, event));
    EXPECT_EQ(ServerId(1, 0), details.serverId);
    EXPECT_EQ(SERVER_ADDED, event);

    ASSERT_TRUE(tracker.getChange(details, event));
    EXPECT_EQ(ServerId(1, 0), details.serverId);
    EXPECT_EQ(SERVER_CRASHED, event);

    ASSERT_TRUE(tracker.getChange(details, event));
    EXPECT_EQ(ServerId(2, 0), details.serverId);
    EXPECT_EQ(SERVER_ADDED, event);
}

TEST_F(CoordinatorServerListTest, enlistServer_ReplaceANonMaster) {
    enlistMaster();
    service->context->recoveryManager->doNotStartRecoveries = true;

    ServerConfig config = ServerConfig::forTesting();
    config.localLocator = "mock:host=backup1";
    config.services = {WireFormat::BACKUP_SERVICE};
    ServerId replacesId = cluster.addServer(config)->serverId;

    TestLog::Enable _(startMasterRecoveryFilter);
    EXPECT_EQ(ServerId(2, 1),
              sl->enlistServer(replacesId, {WireFormat::BACKUP_SERVICE},
                               0, "mock:host=backup2"));
    EXPECT_EQ("startMasterRecovery: Server 2.0 crashed, but it had no tablets",
            TestLog::get());
    EXPECT_FALSE(sl->contains(replacesId));
}

TEST_F(CoordinatorServerListTest, enlistServer_LogCabin) {
    enlistMaster();
    service->context->recoveryManager->doNotStartRecoveries = true;
    ramcloud->createTable("foo");

    TestLog::Enable _;
    EXPECT_EQ(ServerId(2, 0),
              sl->enlistServer(masterServerId, {WireFormat::BACKUP_SERVICE},
                               0, "mock:host=backup"));

    vector<Entry> entriesRead = logCabinLog->read(0);
    string searchString;

    ProtoBuf::ServerInformation readState;
    searchString = "execute: LogCabin: ServerEnlisting entryId: ";
    ASSERT_NO_THROW(findEntryId(searchString));
    logCabinHelper->parseProtoBufFromEntry(
            entriesRead[findEntryId(searchString)], readState);
    EXPECT_EQ("entry_type: \"ServerEnlisting\"\n"
              "server_id: 2\nservice_mask: 2\n"
              "read_speed: 0\n"
              "service_locator: \"mock:host=backup\"\n",
               readState.DebugString());

    searchString = "complete: LogCabin: ServerEnlisted entryId: ";
    ASSERT_NO_THROW(findEntryId(searchString));
    ProtoBuf::ServerInformation readInfo;
    logCabinHelper->parseProtoBufFromEntry(
            entriesRead[findEntryId(searchString)], readInfo);
    EXPECT_EQ("entry_type: \"ServerEnlisted\"\n"
              "server_id: 2\nservice_mask: 2\n"
              "read_speed: 0\n"
              "service_locator: \"mock:host=backup\"\n",
               readInfo.DebugString());
}

namespace {
bool enlistServerFilter(string s) {
    return s == "complete";
}
}

TEST_F(CoordinatorServerListTest, enlistServerRecover) {
    enlistMaster();
    EXPECT_EQ(1U, master->serverId.getId());

    ProtoBuf::ServerInformation state;
    state.set_entry_type("ServerEnlisting");
    state.set_server_id(ServerId(2, 0).getId());
    state.set_service_mask(
            ServiceMask({WireFormat::BACKUP_SERVICE}).serialize());
    state.set_read_speed(0);
    state.set_service_locator("mock:host=backup");

    EntryId entryId = logCabinHelper->appendProtoBuf(
            *service->context->expectedEntryId, state);

    TestLog::Enable _(enlistServerFilter);
    sl->enlistServerRecover(&state, entryId);

    string searchString = "complete: LogCabin: ServerEnlisted entryId: ";
    ASSERT_NO_THROW(findEntryId(searchString));

    EXPECT_EQ(format("complete: Enlisting new server at mock:host=backup "
                     "(server id 2.0) supporting services: BACKUP_SERVICE | "
                     "complete: Backup at id 2.0 has 0 MB/s read | "
                     "complete: LogCabin: ServerEnlisted entryId: %lu",
                      findEntryId(searchString)),
              TestLog::get());

    ProtoBuf::ServerList masterList;
    sl->serialize(masterList, {WireFormat::MASTER_SERVICE});
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
                "server { services: 25 server_id: 1 "
                "service_locator: \"mock:host=master\" "
                "expected_read_mbytes_per_sec: [0-9]\\+ status: 0 "
                "replication_id: 0 } "
                "version_number: 2",
                 masterList.ShortDebugString()));

    ProtoBuf::ServerList backupList;
    sl->serialize(backupList, {WireFormat::BACKUP_SERVICE});
    EXPECT_EQ("server { services: 2 server_id: 2 "
              "service_locator: \"mock:host=backup\" "
              "expected_read_mbytes_per_sec: 0 status: 0 "
              "replication_id: 0 } "
              "version_number: 2 type: FULL_LIST",
               backupList.ShortDebugString());
}

TEST_F(CoordinatorServerListTest, enlistedServerRecover) {
    enlistMaster();
    EXPECT_EQ(1U, master->serverId.getId());

    ProtoBuf::ServerInformation state;
    state.set_entry_type("ServerEnlisted");
    state.set_server_id(ServerId(2, 0).getId());
    state.set_service_mask(
            ServiceMask({WireFormat::BACKUP_SERVICE}).serialize());
    state.set_read_speed(0);
    state.set_service_locator("mock:host=backup");

    EntryId entryId = logCabinHelper->appendProtoBuf(
            *service->context->expectedEntryId, state);

    TestLog::Enable _(enlistServerFilter);
    sl->enlistedServerRecover(&state, entryId);

    EXPECT_EQ("", TestLog::get());

    ProtoBuf::ServerList masterList;
    sl->serialize(masterList, {WireFormat::MASTER_SERVICE});
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
                "server { services: 25 server_id: 1 "
                "service_locator: \"mock:host=master\" "
                "expected_read_mbytes_per_sec: [0-9]\\+ status: 0 "
                "replication_id: 0 } "
                "version_number: 2 type: FULL_LIST",
                 masterList.ShortDebugString()));

    ProtoBuf::ServerList backupList;
    sl->serialize(backupList, {WireFormat::BACKUP_SERVICE});
    EXPECT_EQ("server { services: 2 server_id: 2 "
              "service_locator: \"mock:host=backup\" "
              "expected_read_mbytes_per_sec: 0 status: 0 "
              "replication_id: 0 } "
              "version_number: 2 type: FULL_LIST",
               backupList.ShortDebugString());
}

TEST_F(CoordinatorServerListTest, removeReplicationGroup) {
    enlistMaster();
    Lock lock(mutex); // Used to trick internal calls

    ServerId serverIds[4];
    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::BACKUP_SERVICE,
        WireFormat::MEMBERSHIP_SERVICE, WireFormat::PING_SERVICE};
    for (uint32_t i = 0; i < 3; i++) {
        config.localLocator = format("mock:host=backup%u", i);
        serverIds[i] = cluster.addServer(config)->serverId;
    }
    EXPECT_EQ(1U, (*sl)[serverIds[1]].replicationId);
    service->forceServerDownForTesting = true;
    sl->serverDown(lock, serverIds[1]);
    service->forceServerDownForTesting = false;
    EXPECT_EQ(0U, (*sl)[serverIds[0]].replicationId);
    EXPECT_EQ(0U, (*sl)[serverIds[2]].replicationId);
    config.localLocator = format("mock:host=backup%u", 3);
    serverIds[3] = cluster.addServer(config)->serverId;
    EXPECT_EQ(2U, (*sl)[serverIds[2]].replicationId);
    sl->removeReplicationGroup(lock, 2);
    EXPECT_EQ(0U, (*sl)[serverIds[0]].replicationId);
    EXPECT_EQ(0U, (*sl)[serverIds[2]].replicationId);
    EXPECT_EQ(0U, (*sl)[serverIds[3]].replicationId);
}

TEST_F(CoordinatorServerListTest, serverDown_backup) {
    Lock lock(mutex); // Used to trick internal calls
    ServerId id = sl->enlistServer({}, {WireFormat::BACKUP_SERVICE},
                                   0, "mock:host=backup");
    EXPECT_EQ(1U, sl->backupCount());
    service->forceServerDownForTesting = true;
    sl->serverDown(lock, id);
    EXPECT_EQ(0U, sl->backupCount());
    EXPECT_FALSE(sl->contains(id));
}

TEST_F(CoordinatorServerListTest, serverDown_server) {
    enlistMaster();
    Lock lock(mutex); // Used to trick internal calls
    service->context->recoveryManager->doNotStartRecoveries = true;

    ramcloud->createTable("foo");
    service->forceServerDownForTesting = true;
    TestLog::Enable _(startMasterRecoveryFilter);

    sl->serverDown(lock, masterServerId);

    EXPECT_EQ("startMasterRecovery: Scheduling recovery of master 1.0 | "
              "startMasterRecovery: Recovery crashedServerId: 1.0",
               TestLog::get());
    EXPECT_EQ(ServerStatus::CRASHED, (*sl)[master->serverId].status);
}

TEST_F(CoordinatorServerListTest, serverDown_LogCabin) {
    enlistMaster();
    Lock lock(mutex); // Used to trick internal calls
    service->context->recoveryManager->doNotStartRecoveries = true;

    ramcloud->createTable("foo");
    service->forceServerDownForTesting = true;

    TestLog::Enable _;
    sl->serverDown(lock, masterServerId);

    vector<Entry> entriesRead = logCabinLog->read(0);
    string searchString = "execute: LogCabin: ServerDown entryId: ";
    ASSERT_NO_THROW(findEntryId(searchString));
    ProtoBuf::ServerDown readState;
    logCabinHelper->parseProtoBufFromEntry(
            entriesRead[findEntryId(searchString)], readState);

    EXPECT_EQ("entry_type: \"ServerDown\"\nserver_id: 1\n",
            readState.DebugString());
}

TEST_F(CoordinatorServerListTest, serverDownRecover) {
    enlistMaster();
    service->context->recoveryManager->doNotStartRecoveries = true;

    ramcloud->createTable("foo");
    service->forceServerDownForTesting = true;
    TestLog::Enable _(startMasterRecoveryFilter);

    ProtoBuf::ServerDown state;
    state.set_entry_type("ServerDown");
    state.set_server_id(masterServerId.getId());

    EntryId entryId = logCabinHelper->appendProtoBuf(
            *service->context->expectedEntryId, state);

    sl->serverDownRecover(&state, entryId);

    EXPECT_EQ("startMasterRecovery: Scheduling recovery of master 1.0 | "
            "startMasterRecovery: Recovery crashedServerId: 1.0",
            TestLog::get());
    EXPECT_EQ(ServerStatus::CRASHED,
            (*sl)[master->serverId].status);
}

TEST_F(CoordinatorServerListTest, setMasterRecoveryInfo) {
    enlistMaster();
    ProtoBuf::MasterRecoveryInfo info;
    info.set_min_open_segment_id(10);
    info.set_min_open_segment_epoch(1);
    sl->setMasterRecoveryInfo(masterServerId, info);
    auto other = (*sl)[masterServerId].masterRecoveryInfo;
    EXPECT_EQ(10lu, other.min_open_segment_id());
    EXPECT_EQ(1lu, other.min_open_segment_epoch());
    info.set_min_open_segment_id(9);
    info.set_min_open_segment_epoch(0);
    sl->setMasterRecoveryInfo(masterServerId, info);
    other = (*sl)[masterServerId].masterRecoveryInfo;
    EXPECT_EQ(9lu, other.min_open_segment_id());
    EXPECT_EQ(0lu, other.min_open_segment_epoch());
}

TEST_F(CoordinatorServerListTest, setMasterRecoveryInfoRecover) {
    enlistMaster();
    ProtoBuf::ServerUpdate serverUpdate;
    serverUpdate.set_entry_type("ServerUpdate");
    serverUpdate.set_server_id(masterServerId.getId());
    serverUpdate.mutable_master_recovery_info()->set_min_open_segment_id(10);
    serverUpdate.mutable_master_recovery_info()->set_min_open_segment_epoch(1);
    EntryId entryId = logCabinHelper->appendProtoBuf(
            *service->context->expectedEntryId, serverUpdate);

    sl->setMasterRecoveryInfoRecover(&serverUpdate, entryId);

    EXPECT_EQ(10lu,
            (*sl)[masterServerId].masterRecoveryInfo.min_open_segment_id());
    EXPECT_EQ(1lu,
            (*sl)[masterServerId].masterRecoveryInfo.min_open_segment_epoch());
}

TEST_F(CoordinatorServerListTest, setMasterRecoveryInfo_execute) {
    enlistMaster();
    Lock lock(mutex); // Used to trick internal calls
    TestLog::Enable _;
    ProtoBuf::MasterRecoveryInfo info;
    info.set_min_open_segment_id(10);
    info.set_min_open_segment_epoch(1);
    sl->setMasterRecoveryInfo(masterServerId, info);

    vector<Entry> entriesRead = logCabinLog->read(0);

    EntryId entryId = sl->getServerUpdateLogId(lock, masterServerId);
    ProtoBuf::ServerUpdate readUpdate;
    logCabinHelper->parseProtoBufFromEntry(entriesRead[entryId], readUpdate);

    EXPECT_EQ(10u, readUpdate.master_recovery_info().min_open_segment_id());
    EXPECT_EQ(1u, readUpdate.master_recovery_info().min_open_segment_epoch());
}

TEST_F(CoordinatorServerListTest, setMasterRecoveryInfo_complete_noSuchServer) {
    ProtoBuf::MasterRecoveryInfo info;
    info.set_min_open_segment_id(10);
    info.set_min_open_segment_epoch(1);
    EXPECT_THROW(sl->setMasterRecoveryInfo({2, 2}, info),
            ServerListException);
}

} // namespace RAMCloud
