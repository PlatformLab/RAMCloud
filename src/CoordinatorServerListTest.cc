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

#include <mutex>
#include <queue>

#include "TestUtil.h"
#include "CoordinatorServerList.h"
#include "MockTransport.h"
#include "ServerTracker.h"
#include "ShortMacros.h"
#include "TransportManager.h"

namespace RAMCloud {

namespace {
struct MockServerTracker : public ServerTracker<int> {
    explicit MockServerTracker(Context& context)
            : ServerTracker<int>(context)
            , changes() {}
    void enqueueChange(const ServerDetails& server, ServerChangeEvent event)
    {
        changes.push({server, event});
    }
    void fireCallback() { TEST_LOG("called"); }
    std::queue<ServerTracker<int>::ServerChange> changes;
};
}

class CoordinatorServerListTest : public ::testing::Test {
  public:
    Context context;
    CoordinatorServerList sl;
    MockServerTracker tr;
    std::mutex mutex;

    CoordinatorServerListTest()
        : context()
        , sl(context)
        , tr(context)
        , mutex()
    {
    }

    typedef std::lock_guard<std::mutex> Lock;
    DISALLOW_COPY_AND_ASSIGN(CoordinatorServerListTest);
};

/*
 * Return true if a CoordinatorServerList::Entry is indentical to the
 * given serialized protobuf entry.
 */
static bool
protoBufMatchesEntry(const ProtoBuf::ServerList_Entry& protoBufEntry,
                     const CoordinatorServerList::Entry& serverListEntry,
                     ServerStatus status)
{
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

/**
 * Gets the ProtoBuf::ServerList that's been queued on a ServerList, removes
 * it from the queue, and returns it.
 *
 * NOTE: ServerList.Updater should be halt()'ed before sendMembershipUpdate(..)
 * was called in order for the protobuf to be retrieved correctly.
 *
 * \param csl - CoordinatorServerList that you're interested in
 * \return ProtoBuf::ServerList update that was queued.
 */
static ProtoBuf::ServerList
getAndPopUpdateFrom(CoordinatorServerList& csl) {
    csl.updater.stop = 1;
    ProtoBuf::ServerList ret = csl.updater.msgQueue.front().update;
    csl.updater.msgQueue.pop();
    return ret;
}

TEST_F(CoordinatorServerListTest, constructor) {
    EXPECT_EQ(0U, sl.numberOfMasters);
    EXPECT_EQ(0U, sl.numberOfBackups);
    EXPECT_EQ(0U, sl.version);
    EXPECT_FALSE(sl.updater.stop);
}

TEST_F(CoordinatorServerListTest, iget_serverId) {
    sl.serverList.resize(6);
    sl.add(ServerId(5, 2), "mock:id=5", {WireFormat::MASTER_SERVICE}, 100);
    EXPECT_TRUE(sl.iget({20, 0}) == NULL);
    EXPECT_TRUE(sl.iget({2, 0}) == NULL);
    EXPECT_TRUE(sl.iget({5, 1}) == NULL);
    EXPECT_TRUE(sl.iget({5, 2}) != NULL);
    EXPECT_EQ("mock:id=5", sl.iget({5, 2})->serviceLocator);
}

TEST_F(CoordinatorServerListTest, add) {

    sl.updater.halt(); // Stop Updater to see enqueued protobufs
    EXPECT_EQ(0U, sl.serverList.size());
    EXPECT_EQ(0U, sl.numberOfMasters);
    EXPECT_EQ(0U, sl.numberOfBackups);

    {
        EXPECT_EQ(0U, sl.version);
        sl.add(ServerId(1, 0), "mock:host=server1",
            {WireFormat::MASTER_SERVICE}, 100);
        EXPECT_EQ(1U, sl.version);
        EXPECT_TRUE(sl.serverList[1].entry);
        EXPECT_FALSE(sl.serverList[0].entry);
        EXPECT_EQ(1U, sl.numberOfMasters);
        EXPECT_EQ(0U, sl.numberOfBackups);
        EXPECT_EQ(ServerId(1, 0), sl.serverList[1].entry->serverId);
        EXPECT_EQ("mock:host=server1", sl.serverList[1].entry->serviceLocator);
        EXPECT_TRUE(sl.serverList[1].entry->isMaster());
        EXPECT_FALSE(sl.serverList[1].entry->isBackup());
        EXPECT_EQ(0u, sl.serverList[1].entry->expectedReadMBytesPerSec);
        EXPECT_EQ(1U, sl.serverList[1].nextGenerationNumber);
        ProtoBuf::ServerList update = getAndPopUpdateFrom(sl);
        EXPECT_EQ(1U, sl.version);
        EXPECT_EQ(1U, update.version_number());
        EXPECT_EQ(1, update.server_size());
        EXPECT_TRUE(protoBufMatchesEntry(update.server(0),
            *sl.serverList[1].entry, ServerStatus::UP));
    }

    {
        EXPECT_EQ(1U, sl.version);
        sl.add(ServerId(2, 0), "hi again", {WireFormat::BACKUP_SERVICE}, 100);
        EXPECT_EQ(2U, sl.version);
        EXPECT_TRUE(sl.serverList[2].entry);
        EXPECT_EQ(ServerId(2, 0), sl.serverList[2].entry->serverId);
        EXPECT_EQ("hi again", sl.serverList[2].entry->serviceLocator);
        EXPECT_FALSE(sl.serverList[2].entry->isMaster());
        EXPECT_TRUE(sl.serverList[2].entry->isBackup());
        EXPECT_EQ(100u, sl.serverList[2].entry->expectedReadMBytesPerSec);
        EXPECT_EQ(1U, sl.serverList[2].nextGenerationNumber);
        EXPECT_EQ(1U, sl.numberOfMasters);
        EXPECT_EQ(1U, sl.numberOfBackups);
        ProtoBuf::ServerList update = getAndPopUpdateFrom(sl);
        EXPECT_EQ(2U, sl.version);
        EXPECT_EQ(2U, update.version_number());
        EXPECT_TRUE(protoBufMatchesEntry(update.server(0),
            *sl.serverList[2].entry, ServerStatus::UP));
    }
}

TEST_F(CoordinatorServerListTest, add_trackerUpdated) {
    TestLog::Enable _;
    ServerId serverId = sl.generateUniqueId();
    sl.add(serverId, "hi!", {WireFormat::MASTER_SERVICE}, 100);
    EXPECT_EQ("fireCallback: called", TestLog::get());
    ASSERT_FALSE(tr.changes.empty());
    auto& server = tr.changes.front().server;
    EXPECT_EQ(ServerId(1, 0), server.serverId);
    EXPECT_EQ("hi!", server.serviceLocator);
    EXPECT_EQ("MASTER_SERVICE", server.services.toString());
    // Not set when no BACKUP_SERVICE.
    EXPECT_EQ(0u, server.expectedReadMBytesPerSec);
    EXPECT_EQ(ServerStatus::UP, server.status);
    EXPECT_EQ(SERVER_ADDED, tr.changes.front().event);
}

TEST_F(CoordinatorServerListTest, crashed) {
    ProtoBuf::ServerList updates;
    sl.updater.halt(); //Stop updates

    EXPECT_THROW(sl.crashed(ServerId(0, 0)), Exception);
    // Ensure nothing was sent
    EXPECT_EQ(0U, sl.updater.msgQueue.size());

    ServerId serverId = sl.generateUniqueId();
    sl.add(serverId, "hi!", {WireFormat::MASTER_SERVICE}, 100);
    getAndPopUpdateFrom(sl);

    CoordinatorServerList::Entry entryCopy = sl[ServerId(1, 0)];
    EXPECT_NO_THROW(sl.crashed(ServerId(1, 0)));
    ASSERT_TRUE(sl.serverList[1].entry);
    EXPECT_EQ(ServerStatus::CRASHED, sl.serverList[1].entry->status);
    updates = getAndPopUpdateFrom(sl);
    EXPECT_TRUE(protoBufMatchesEntry(updates.server(0),
                                     entryCopy, ServerStatus::CRASHED));

    // Already crashed; a no-op.
    sl.crashed(ServerId(1, 0));
    EXPECT_TRUE(sl.updater.msgQueue.empty());
    EXPECT_EQ(0U, sl.numberOfMasters);
    EXPECT_EQ(0U, sl.numberOfBackups);
}

TEST_F(CoordinatorServerListTest, crashed_trackerUpdated) {
    TestLog::Enable _;
    ServerId serverId = sl.generateUniqueId();
    sl.add(serverId, "hi!", {WireFormat::MASTER_SERVICE}, 100);
    sl.crashed(serverId);
    EXPECT_EQ("fireCallback: called | fireCallback: called", TestLog::get());
    ASSERT_FALSE(tr.changes.empty());
    tr.changes.pop();
    ASSERT_FALSE(tr.changes.empty());
    auto& server = tr.changes.front().server;
    EXPECT_EQ(serverId, server.serverId);
    EXPECT_EQ("hi!", server.serviceLocator);
    EXPECT_EQ("MASTER_SERVICE", server.services.toString());
    // Not set when no BACKUP_SERVICE.
    EXPECT_EQ(0u, server.expectedReadMBytesPerSec);
    EXPECT_EQ(ServerStatus::CRASHED, server.status);
    EXPECT_EQ(SERVER_CRASHED, tr.changes.front().event);
}

TEST_F(CoordinatorServerListTest, generateUniqueId) {
    EXPECT_EQ(ServerId(1, 0), sl.generateUniqueId());
    EXPECT_EQ(ServerId(2, 0), sl.generateUniqueId());

    sl.remove(ServerId(1, 0));
    EXPECT_EQ(ServerId(1, 1), sl.generateUniqueId());
}

TEST_F(CoordinatorServerListTest, remove) {
    ProtoBuf::ServerList update;
    sl.updater.halt();

    EXPECT_THROW(sl.remove(ServerId(0, 0)), Exception);
    EXPECT_TRUE(sl.updater.msgQueue.empty());

    ServerId serverId1 = sl.generateUniqueId();
    sl.add(serverId1, "hi!", {WireFormat::MASTER_SERVICE}, 100);
    CoordinatorServerList::Entry entryCopy = sl[ServerId(1, 0)];
    update = getAndPopUpdateFrom(sl);
    EXPECT_EQ(1 , update.server_size());

    EXPECT_NO_THROW(sl.remove(ServerId(1, 0)));
    EXPECT_FALSE(sl.serverList[1].entry);
    update = getAndPopUpdateFrom(sl);
    EXPECT_TRUE(protoBufMatchesEntry(update.server(0),
            entryCopy, ServerStatus::CRASHED));
    EXPECT_TRUE(protoBufMatchesEntry(update.server(1),
            entryCopy, ServerStatus::DOWN));

    EXPECT_THROW(sl.remove(ServerId(1, 0)), Exception);
    EXPECT_TRUE(sl.updater.msgQueue.empty());
    EXPECT_EQ(0U, sl.numberOfMasters);
    EXPECT_EQ(0U, sl.numberOfBackups);

    ServerId serverId2 = sl.generateUniqueId();
    sl.add(serverId2, "hi, again", {WireFormat::BACKUP_SERVICE}, 100);
    sl.crashed(ServerId(1, 1));
    EXPECT_TRUE(sl.serverList[1].entry);
    getAndPopUpdateFrom(sl); // remove add Protobuf
    getAndPopUpdateFrom(sl); // remove crashed Protobuf
    EXPECT_THROW(sl.remove(ServerId(1, 2)), Exception);
    EXPECT_NO_THROW(sl.remove(ServerId(1, 1)));
    update = getAndPopUpdateFrom(sl);
    EXPECT_EQ(uint32_t(ServerStatus::DOWN), update.server(0).status());
    EXPECT_EQ(0U, sl.numberOfMasters);
    EXPECT_EQ(0U, sl.numberOfBackups);
}

TEST_F(CoordinatorServerListTest, remove_trackerUpdated) {
    TestLog::Enable _;
    ServerId serverId = sl.generateUniqueId();
    sl.add(serverId, "hi!", {WireFormat::MASTER_SERVICE}, 100);
    sl.remove(serverId);
    EXPECT_EQ("fireCallback: called | fireCallback: called | "
              "fireCallback: called", TestLog::get());
    ASSERT_FALSE(tr.changes.empty());
    tr.changes.pop();
    ASSERT_FALSE(tr.changes.empty());
    tr.changes.pop();
    ASSERT_FALSE(tr.changes.empty());
    auto& server = tr.changes.front().server;
    EXPECT_EQ(serverId, server.serverId);
    EXPECT_EQ("hi!", server.serviceLocator);
    EXPECT_EQ("MASTER_SERVICE", server.services.toString());
    // Not set when no BACKUP_SERVICE.
    EXPECT_EQ(0u, server.expectedReadMBytesPerSec);
    EXPECT_EQ(ServerStatus::DOWN, server.status);
    EXPECT_EQ(SERVER_REMOVED, tr.changes.front().event);
}

TEST_F(CoordinatorServerListTest, indexOperator) {
    EXPECT_THROW(sl[ServerId(0, 0)], Exception);
    ServerId serverId = sl.generateUniqueId();
    sl.add(serverId, "yo!", {WireFormat::MASTER_SERVICE}, 100);
    EXPECT_EQ(ServerId(1, 0), sl[ServerId(1, 0)].serverId);
    EXPECT_EQ("yo!", sl[ServerId(1, 0)].serviceLocator);
    sl.crashed(ServerId(1, 0));
    sl.remove(ServerId(1, 0));
    EXPECT_THROW(sl[ServerId(1, 0)], Exception);
}

TEST_F(CoordinatorServerListTest, nextMasterIndex) {
    EXPECT_EQ(-1U, sl.nextMasterIndex(0));
    ServerId serverId1 = sl.generateUniqueId();
    sl.add(serverId1, "", {WireFormat::BACKUP_SERVICE}, 100);
    ServerId serverId2 = sl.generateUniqueId();
    sl.add(serverId2, "", {WireFormat::MASTER_SERVICE}, 100);
    ServerId serverId3 = sl.generateUniqueId();
    sl.add(serverId3, "", {WireFormat::BACKUP_SERVICE}, 100);
    ServerId serverId4 = sl.generateUniqueId();
    sl.add(serverId4, "", {WireFormat::BACKUP_SERVICE}, 100);
    ServerId serverId5 = sl.generateUniqueId();
    sl.add(serverId5, "", {WireFormat::MASTER_SERVICE}, 100);
    ServerId serverId6 = sl.generateUniqueId();
    sl.add(serverId6, "", {WireFormat::BACKUP_SERVICE}, 100);

    EXPECT_EQ(2U, sl.nextMasterIndex(0));
    EXPECT_EQ(2U, sl.nextMasterIndex(2));
    EXPECT_EQ(5U, sl.nextMasterIndex(3));
    EXPECT_EQ(-1U, sl.nextMasterIndex(6));
}

TEST_F(CoordinatorServerListTest, nextBackupIndex) {
    EXPECT_EQ(-1U, sl.nextMasterIndex(0));
    ServerId serverId1 = sl.generateUniqueId();
    sl.add(serverId1, "", {WireFormat::MASTER_SERVICE}, 100);
    ServerId serverId2 = sl.generateUniqueId();
    sl.add(serverId2, "", {WireFormat::BACKUP_SERVICE}, 100);
    ServerId serverId3 = sl.generateUniqueId();
    sl.add(serverId3, "", {WireFormat::MASTER_SERVICE}, 100);

    EXPECT_EQ(2U, sl.nextBackupIndex(0));
    EXPECT_EQ(2U, sl.nextBackupIndex(2));
    EXPECT_EQ(-1U, sl.nextBackupIndex(3));
}

TEST_F(CoordinatorServerListTest, serialize) {
    {
        ProtoBuf::ServerList serverList;
        sl.serialize(serverList, {});
        EXPECT_EQ(0, serverList.server_size());
        sl.serialize(serverList, {WireFormat::MASTER_SERVICE,
            WireFormat::BACKUP_SERVICE});
        EXPECT_EQ(0, serverList.server_size());
    }

    ServerId first = sl.generateUniqueId();
    sl.add(first, "", {WireFormat::MASTER_SERVICE}, 100);
    ServerId second = sl.generateUniqueId();
    sl.add(second, "", {WireFormat::MASTER_SERVICE}, 100);
    ServerId third = sl.generateUniqueId();
    sl.add(third, "", {WireFormat::MASTER_SERVICE}, 100);
    ServerId fourth = sl.generateUniqueId();
    sl.add(fourth, "", {WireFormat::BACKUP_SERVICE}, 100);
    ServerId last = sl.generateUniqueId();
    sl.add(last, "", {WireFormat::MASTER_SERVICE,
        WireFormat::BACKUP_SERVICE}, 100);
    sl.remove(first);       // ensure removed entries are skipped
    sl.crashed(last);       // ensure crashed entries are included

    auto masterMask = ServiceMask{WireFormat::MASTER_SERVICE}.serialize();
    auto backupMask = ServiceMask{WireFormat::BACKUP_SERVICE}.serialize();
    auto bothMask = ServiceMask{WireFormat::MASTER_SERVICE,
        WireFormat::BACKUP_SERVICE}.serialize();
    {
        ProtoBuf::ServerList serverList;
        sl.serialize(serverList, {});
        EXPECT_EQ(0, serverList.server_size());
        sl.serialize(serverList, {WireFormat::MASTER_SERVICE});
        EXPECT_EQ(3, serverList.server_size());
        EXPECT_EQ(masterMask, serverList.server(0).services());
        EXPECT_EQ(masterMask, serverList.server(1).services());
        EXPECT_EQ(bothMask, serverList.server(2).services());
        EXPECT_EQ(ServerStatus::CRASHED,
                  ServerStatus(serverList.server(2).status()));
    }

    {
        ProtoBuf::ServerList serverList;
        sl.serialize(serverList, {WireFormat::BACKUP_SERVICE});
        EXPECT_EQ(2, serverList.server_size());
        EXPECT_EQ(backupMask, serverList.server(0).services());
        EXPECT_EQ(bothMask, serverList.server(1).services());
        EXPECT_EQ(ServerStatus::CRASHED,
                  ServerStatus(serverList.server(1).status()));
    }

    {
        ProtoBuf::ServerList serverList;
        sl.serialize(serverList, {WireFormat::MASTER_SERVICE,
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

TEST_F(CoordinatorServerListTest, sendMembershipUpdate) {
    MockTransport transport(context);
    TransportManager::MockRegistrar _(context, transport);
    ProtoBuf::ServerList& update = sl.updates;

    // Used for internal calls that don't call sendMembersShipUpdates
    Lock lock(mutex);

    // Test unoccupied server slot. Remove must wait until after last add to
    // ensure slot isn't recycled.
    ServerId serverId1 = sl.generateUniqueId();
    sl.add(lock, serverId1, "mock:host=server1",
            {WireFormat::MEMBERSHIP_SERVICE}, 0);

    // Test crashed server gets skipped as a recipient.
    ServerId serverId2 = sl.generateUniqueId();
    sl.add(lock, serverId2, "mock:host=server2", {}, 0);
    sl.crashed(lock, serverId2);

    // Test server with no membership service.
    ServerId serverId3 = sl.generateUniqueId();
    sl.add(lock, serverId3, "mock:host=server3", {}, 0);

    // Test exclude list.
    ServerId serverId4 = sl.generateUniqueId();
    sl.add(lock, serverId4, "mock:host=server4",
            {WireFormat::MEMBERSHIP_SERVICE}, 0);
    sl.remove(lock, serverId1);

    update.Clear();
    TestLog::Enable __(statusFilter);
    sl.sendMembershipUpdate(serverId4);
    sl.sync();

    // Nothing should be sent. All servers are invalid recipients for
    // various reasons.
    EXPECT_EQ("", transport.outputLog);
    EXPECT_EQ("", TestLog::get());

    ServerId serverId5 = sl.generateUniqueId();
    sl.add(lock, serverId5, "mock:host=server5",
            {WireFormat::MEMBERSHIP_SERVICE}, 0);

    update.Clear();

    TestLog::reset();
    transport.outputLog = "";
    sl.version = 0;

    transport.setInput("0 1"); // Server 5 (in the first slot) has trouble.
    transport.setInput("0");   // Server 5 ok to the send of the entire list.
    transport.setInput("0 0"); // Server 4 gets the update just fine.

    // Call without add to generate update
    ServerId serverId6 = sl.generateUniqueId();
    sl.add(serverId6, "mock:host=server6", {WireFormat::MEMBERSHIP_SERVICE}, 0);

    sl.sync(); // Need to wait for updates to propagate

    EXPECT_EQ("sendRequest: 0x40024 54 0x100d2b0a 0x11000000 5 0 0x6f6d111a "
            "ck:host=server6-/0 0x35000000 0 273 0 /0 | " // Update to server 5
            "sendRequest: 0x40023 9 273 0 /0 | "          // SetList to server 5
            "sendRequest: 0x40024 54 0x100d2b0a 0x11000000 5 0 0x6f6d111a "
            "ck:host=server6-/0 0x35000000 0 273 0 /0",   // Update to server 4
            transport.outputLog);

    EXPECT_EQ("fireCallback: called | sendMembershipUpdate: Server 1.1 "
              "had lost an update. Sending whole list. | "
              "sendMembershipUpdate: Server list update sent to server 4.0",
              TestLog::get());
}

TEST_F(CoordinatorServerListTest, firstFreeIndex) {
    EXPECT_EQ(0U, sl.serverList.size());
    EXPECT_EQ(1U, sl.firstFreeIndex());
    EXPECT_EQ(2U, sl.serverList.size());
    ServerId serverId1 = sl.generateUniqueId();
    sl.add(serverId1, "hi", {WireFormat::MASTER_SERVICE}, 100);
    EXPECT_EQ(2U, sl.firstFreeIndex());
    ServerId serverId2 = sl.generateUniqueId();
    sl.add(serverId2, "hi again", {WireFormat::MASTER_SERVICE}, 100);
    EXPECT_EQ(3U, sl.firstFreeIndex());
    sl.remove(ServerId(2, 0));
    EXPECT_EQ(2U, sl.firstFreeIndex());
    sl.remove(ServerId(1, 0));
    EXPECT_EQ(1U, sl.firstFreeIndex());
}

TEST_F(CoordinatorServerListTest, getReferenceFromServerId) {
    EXPECT_THROW(sl.getReferenceFromServerId(ServerId(0, 0)), Exception);
    EXPECT_THROW(sl.getReferenceFromServerId(ServerId(1, 0)), Exception);
    ServerId serverId = sl.generateUniqueId();
    sl.add(serverId, "", {WireFormat::MASTER_SERVICE}, 100);
    EXPECT_THROW(sl.getReferenceFromServerId(ServerId(0, 0)), Exception);
    EXPECT_NO_THROW(sl.getReferenceFromServerId(ServerId(1, 0)));
    EXPECT_THROW(sl.getReferenceFromServerId(ServerId(1, 1)), Exception);
    EXPECT_THROW(sl.getReferenceFromServerId(ServerId(2, 0)), Exception);
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
        "I ain't got time to bleed", {WireFormat::BACKUP_SERVICE});
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
    ServerId serverId = sl.generateUniqueId();
    sl.add(serverId, "", {WireFormat::MASTER_SERVICE}, 100);
    sl.addServerInfoLogId(serverId, 10);

    CoordinatorServerList::Entry entry(sl.getReferenceFromServerId(serverId));
    EXPECT_EQ(10U, entry.serverInfoLogId);
}

TEST_F(CoordinatorServerListTest, getServerInfoLogId) {
    ServerId serverId = sl.generateUniqueId();
    sl.add(serverId, "", {WireFormat::MASTER_SERVICE}, 100);
    CoordinatorServerList::Entry& entry =
        const_cast<CoordinatorServerList::Entry&>(
            sl.getReferenceFromServerId(serverId));
    entry.serverInfoLogId = 10U;

    LogCabin::Client::EntryId entryId = sl.getServerInfoLogId(serverId);
    EXPECT_EQ(10U, entryId);
}

TEST_F(CoordinatorServerListTest, addServerUpdateLogId) {
    ServerId serverId = sl.generateUniqueId();
    sl.add(serverId, "", {WireFormat::MASTER_SERVICE}, 100);
    sl.addServerUpdateLogId(serverId, 10);

    CoordinatorServerList::Entry entry(sl.getReferenceFromServerId(serverId));
    EXPECT_EQ(10U, entry.serverUpdateLogId);
}

TEST_F(CoordinatorServerListTest, getServerUpdateLogId) {
    ServerId serverId = sl.generateUniqueId();
    sl.add(serverId, "", {WireFormat::MASTER_SERVICE}, 100);
    CoordinatorServerList::Entry& entry =
        const_cast<CoordinatorServerList::Entry&>(
            sl.getReferenceFromServerId(serverId));
    entry.serverUpdateLogId = 10U;

    LogCabin::Client::EntryId entryId = sl.getServerUpdateLogId(serverId);
    EXPECT_EQ(10U, entryId);
}

}  // namespace RAMCloud
