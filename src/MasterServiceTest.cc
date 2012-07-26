/* Copyright (c) 2010-2012 Stanford University
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
#include "BackupStorage.h"
#include "Buffer.h"
#include "CoordinatorClient.h"
#include "MockCluster.h"
#include "Memory.h"
#include "MasterClient.h"
#include "MasterService.h"
#include "ReplicaManager.h"
#include "ShortMacros.h"
#include "StringUtil.h"
#include "Tablets.pb.h"

namespace RAMCloud {

class MasterServiceTest : public ::testing::Test {
  public:
    Context context;
    MockCluster cluster;
    ServerConfig backup1Config;
    ServerId backup1Id;

    ServerConfig masterConfig;
    MasterService* service;
    std::unique_ptr<MasterClient> client;
    CoordinatorClient* coordinator;
    Server* masterServer;

    // To make tests that don't need big segments faster, set a smaller default
    // segmentSize. Since we can't provide arguments to it in gtest, nor can we
    // apparently template easily on that, we need to subclass this if we want
    // to provide a fixture with a different value.
    explicit MasterServiceTest(uint32_t segmentSize = 256 * 1024)
        : context()
        , cluster(context)
        , backup1Config(ServerConfig::forTesting())
        , backup1Id()
        , masterConfig(ServerConfig::forTesting())
        , service()
        , client()
        , coordinator()
        , masterServer()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        coordinator = cluster.getCoordinatorClient();

        backup1Config.localLocator = "mock:host=backup1";
        backup1Config.services = {BACKUP_SERVICE, MEMBERSHIP_SERVICE};
        backup1Config.segmentSize = segmentSize;
        backup1Config.backup.numSegmentFrames = 2;
        backup1Id = cluster.addServer(backup1Config)->serverId;

        masterConfig = ServerConfig::forTesting();
        masterConfig.segmentSize = segmentSize;
        masterConfig.maxObjectDataSize = segmentSize / 4;
        masterConfig.localLocator = "mock:host=master";
        masterConfig.services = {MASTER_SERVICE, MEMBERSHIP_SERVICE};
        masterConfig.master.numReplicas = 1;
        masterServer = cluster.addServer(masterConfig);
        service = masterServer->master.get();
        client = cluster.get<MasterClient>(masterServer);

        ProtoBuf::Tablets_Tablet& tablet(*service->tablets.add_tablet());
        tablet.set_table_id(0);
        tablet.set_start_key_hash(0);
        tablet.set_end_key_hash(~0UL);
        tablet.set_user_data(reinterpret_cast<uint64_t>(new Table(0, 0, ~0UL)));
    }

    uint32_t
    buildRecoverySegment(char *segmentBuf, uint32_t segmentCapacity,
                         Key& key, uint64_t version, string objContents)
    {
        Segment s;
        uint32_t dataLength = downCast<uint32_t>(objContents.length()) + 1;
        Object newObject(key, objContents.c_str(), dataLength, version, 0);

        Buffer newObjectBuffer;
        newObject.serializeToBuffer(newObjectBuffer);
        bool success = s.append(LOG_ENTRY_TYPE_OBJ, newObjectBuffer);
        EXPECT_TRUE(success);
        s.close();

        Buffer buffer;
        s.appendToBuffer(buffer);
        EXPECT_GE(segmentCapacity, buffer.getTotalLength());
        buffer.copy(0, buffer.getTotalLength(), segmentBuf);

        return buffer.getTotalLength();
    }

    uint32_t
    buildRecoverySegment(char *segmentBuf, uint64_t segmentCapacity,
                         ObjectTombstone& tomb)
    {
        Segment s;
        Buffer newTombstoneBuffer;
        tomb.serializeToBuffer(newTombstoneBuffer);
        bool success = s.append(LOG_ENTRY_TYPE_OBJTOMB, newTombstoneBuffer);
        EXPECT_TRUE(success);
        s.close();

        Buffer buffer;
        s.appendToBuffer(buffer);
        EXPECT_GE(segmentCapacity, buffer.getTotalLength());

        return buffer.getTotalLength();
    }

    void
    verifyRecoveryObject(Key& key, string contents)
    {
        Buffer value;
        client->read(key.getTableId(),
                     key.getStringKey(),
                     key.getStringKeyLength(),
                     &value);
        const char *s = reinterpret_cast<const char *>(
            value.getRange(0, value.getTotalLength()));
        EXPECT_EQ(0, strcmp(s, contents.c_str()));
    }

    static bool
    recoverSegmentFilter(string s)
    {
        return (s == "recoverSegment" || s == "recover" ||
                s == "recoveryMasterFinished");
    }

    void
    appendTablet(ProtoBuf::Tablets& tablets,
                 uint64_t partitionId,
                 uint64_t tableId,
                 uint64_t start, uint64_t end,
                 uint64_t ctimeHeadSegmentId, uint32_t ctimeHeadSegmentOffset)
    {
        ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
        tablet.set_table_id(tableId);
        tablet.set_start_key_hash(start);
        tablet.set_end_key_hash(end);
        tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
        tablet.set_user_data(partitionId);
        tablet.set_ctime_log_head_id(ctimeHeadSegmentId);
        tablet.set_ctime_log_head_offset(ctimeHeadSegmentOffset);
    }

    void
    createTabletList(ProtoBuf::Tablets& tablets)
    {
        appendTablet(tablets, 0, 123, 0, 9, 0, 0);
        appendTablet(tablets, 0, 123, 10, 19, 0, 0);
        appendTablet(tablets, 0, 123, 20, 29, 0, 0);
        appendTablet(tablets, 0, 124, 20, 100, 0, 0);
    }

    DISALLOW_COPY_AND_ASSIGN(MasterServiceTest);
};

TEST_F(MasterServiceTest, read_basics) {
    client->write(0, "0", 1, "abcdef", 6);
    Buffer value;
    uint64_t version;
    client->read(0, "0", 1, &value, NULL, &version);
    EXPECT_EQ(1U, version);
    EXPECT_EQ("abcdef", TestUtil::toString(&value));
}

TEST_F(MasterServiceTest, read_badTable) {
    Buffer value;
    EXPECT_THROW(client->read(4, "0", 1, &value),
                 UnknownTableException);
}

TEST_F(MasterServiceTest, read_noSuchObject) {
    Buffer value;
    EXPECT_THROW(client->read(0, "5", 1, &value),
                 ObjectDoesntExistException);
}

TEST_F(MasterServiceTest, read_rejectRules) {
    client->write(0, "0", 1, "abcdef", 6);

    Buffer value;
    RejectRules rules;
    memset(&rules, 0, sizeof(rules));
    rules.versionNeGiven = true;
    rules.givenVersion = 2;
    uint64_t version;
    EXPECT_THROW(client->read(0, "0", 1, &value, &rules, &version),
                 WrongVersionException);
    EXPECT_EQ(1U, version);
}

TEST_F(MasterServiceTest, multiRead_basics) {
    client->write(0, "0", 1, "firstVal", 8);
    client->write(0, "1", 1, "secondVal", 9);

    std::vector<MasterClient::ReadObject*> requests;

    Tub<Buffer> val1;
    MasterClient::ReadObject request1(0, "0", 1, &val1);
    request1.status = STATUS_RETRY;
    requests.push_back(&request1);
    Tub<Buffer> val2;
    MasterClient::ReadObject request2(0, "1", 1, &val2);
    request2.status = STATUS_RETRY;
    requests.push_back(&request2);

    client->multiRead(requests);

    EXPECT_STREQ("STATUS_OK", statusToSymbol(request1.status));
    EXPECT_EQ(1U, request1.version);
    EXPECT_EQ("firstVal", TestUtil::toString(val1.get()));
    EXPECT_STREQ("STATUS_OK", statusToSymbol(request2.status));
    EXPECT_EQ(2U, request2.version);
    EXPECT_EQ("secondVal", TestUtil::toString(val2.get()));
}

TEST_F(MasterServiceTest, multiRead_badTable) {
    std::vector<MasterClient::ReadObject*> requests;
    Tub<Buffer> valError;
    MasterClient::ReadObject requestError(10, "0", 1, &valError);
    requestError.status = STATUS_RETRY;
    requests.push_back(&requestError);

    client->multiRead(requests);

    EXPECT_STREQ("STATUS_UNKNOWN_TABLE",
                 statusToSymbol(requestError.status));
}

TEST_F(MasterServiceTest, multiRead_noSuchObject) {
    std::vector<MasterClient::ReadObject*> requests;
    Tub<Buffer> valError;
    MasterClient::ReadObject requestError(0, "0", 1, &valError);
    requestError.status = STATUS_RETRY;
    requests.push_back(&requestError);

    client->multiRead(requests);

    EXPECT_STREQ("STATUS_OBJECT_DOESNT_EXIST",
                 statusToSymbol(requestError.status));
}

TEST_F(MasterServiceTest, detectSegmentRecoveryFailure_success) {
    typedef MasterService::Replica::State State;
    vector<MasterService::Replica> replicas {
        { 123, 87, State::FAILED },
        { 123, 88, State::OK },
        { 123, 89, State::OK },
        { 123, 88, State::OK },
        { 123, 87, State::OK },
    };
    MasterService::detectSegmentRecoveryFailure(ServerId(99, 0), 3, replicas);
}

TEST_F(MasterServiceTest, detectSegmentRecoveryFailure_failure) {
    typedef MasterService::Replica::State State;
    vector<MasterService::Replica> replicas {
        { 123, 87, State::FAILED },
        { 123, 88, State::OK },
    };
    EXPECT_THROW(MasterService::detectSegmentRecoveryFailure(ServerId(99, 0),
                                                             3, replicas),
                  SegmentRecoveryFailedException);
}

TEST_F(MasterServiceTest, getHeadOfLog) {
    EXPECT_EQ(Log::Position(0, 44), client->getHeadOfLog());
    client->write(0, "0", 1, "abcdef", 6);
    EXPECT_EQ(Log::Position(0, 79), client->getHeadOfLog());
}

TEST_F(MasterServiceTest, recover_basics) {
    const uint32_t segmentSize = backup1Config.segmentSize;
    char* segMem =
        static_cast<char*>(Memory::xmemalign(HERE, segmentSize, segmentSize));
    ServerId serverId(123, 0);
    ServerList serverList(context);
    foreach (auto* server, cluster.servers)
        serverList.add(server->serverId, server->config.localLocator,
                       server->config.services, 100);
    ReplicaManager mgr(context, serverList, serverId, 1, NULL);

#if 0 // Won't work now that Segments don't control replication.
    Segment segment(123, 87, segMem, segmentSize, &mgr);
    segment.sync();
#endif

    ProtoBuf::Tablets tablets;
    createTabletList(tablets);
    BackupClient(cluster.context.transportManager->getSession(
                                                "mock:host=backup1"))
        .startReadingData(ServerId(123), tablets);

    ProtoBuf::ServerList backups;
    RecoverRpc::Replica replicas[] = {
        {backup1Id.getId(), 87},
    };

    TestLog::Enable __(&recoverSegmentFilter);
    client->recover(10lu, ServerId(123), 0, tablets,
                    replicas, arrayLength(replicas));

    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "recover: Starting recovery of 4 tablets on masterId 2 | "
        "recover: Recovering master 123, partition 0, 1 replicas available | "
        "recover: Starting getRecoveryData from server 1 at "
        "mock:host=backup1 for "
        "segment 87 on channel 0 (initial round of RPCs) | "
        "recover: Waiting on recovery data for segment 87 from "
        "server 1 at mock:host=backup1 | ",
        TestLog::get()));
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "recover: Recovering segment 87 with size 0 | "
        "recoverSegment: recoverSegment 87, ... | ",
        TestLog::get()));
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "recover: Checking server 1 at mock:host=backup1 "
        "off the list for 87 | "
        "recover: Checking server 1 at mock:host=backup1 "
        "off the list for 87 | ",
        TestLog::get()));
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "recover: set tablet 123 0 9 to locator mock:host=master, id 2 | "
        "recover: set tablet 123 10 19 to locator mock:host=master, id 2 | "
        "recover: set tablet 123 20 29 to locator mock:host=master, id 2 | "
        "recover: set tablet 124 20 100 to locator mock:host=master, "
        "id 2 | "
        "recover: Reporting completion of recovery 10 | "
        "recoveryMasterFinished: called by masterId 2 with 4 tablets",
        TestLog::get()));
    free(segMem);
}

TEST_F(MasterServiceTest, removeIfFromUnknownTablet) {
    coordinator->createTable("table");
    uint64_t tableId = coordinator->getTableId("table");
    Key key(tableId, "1", 1); 
    HashTable::Reference reference;

    client->write(tableId, "1", 1, NULL, 0);

    bool success = service->objectMap.lookup(key, reference);
    EXPECT_TRUE(success);

    EXPECT_TRUE(service->getTable(key) != NULL);
    removeObjectIfFromUnknownTablet(reference, service);
    EXPECT_TRUE(service->objectMap.lookup(key, reference));

    foreach (const ProtoBuf::Tablets::Tablet& tablet, service->tablets.tablet())
        delete reinterpret_cast<Table*>(tablet.user_data());
    service->tablets.Clear();

    EXPECT_TRUE(service->getTable(key) == NULL);
    removeObjectIfFromUnknownTablet(reference, service);
    EXPECT_FALSE(service->objectMap.lookup(key, reference));
}

/**
  * Properties checked:
  * 1) At most length of tasks number of RPCs are started initially
  *    even with a longer backup list.
  * 2) Ensures that if a segment is only requested in the initial
  *    round of RPCs once.
  * 3) Ensures that if an entry in the server list is skipped because
  *    another RPC is outstanding for the same segment it is retried
  *    if the earlier RPC fails.
  * 4) Ensures that if an RPC succeeds for one copy of a segment other
  *    RPCs for that segment don't occur.
  * 5) A transport exception at construction time caused that entry
  *    to be skipped and a new entry to be tried immediate, both
  *    during initial RPC starts and following ones.
  */
TEST_F(MasterServiceTest, recover) {
    const uint32_t segmentSize = backup1Config.segmentSize;
    char* segMem =
        static_cast<char*>(Memory::xmemalign(HERE, segmentSize, segmentSize));
    ServerId serverId(123, 0);
    ServerList serverList(context);
    foreach (auto* server, cluster.servers)
        serverList.add(server->serverId, server->config.localLocator,
                       server->config.services, 100);
    ReplicaManager mgr(context, serverList, serverId, 1, NULL);

#if 0   // Doesn't work anymore when segments don't know of replication.
    Segment __(123, 88, segMem, segmentSize, &mgr);
    __.sync();
#endif

    ServerConfig backup2Config = backup1Config;
    backup2Config.localLocator = "mock:host=backup2";
    ServerId backup2Id = cluster.addServer(backup2Config)->serverId;

    ProtoBuf::Tablets tablets;
    createTabletList(tablets);
    BackupClient(context.transportManager->getSession(
                                                    "mock:host=backup1"))
        .startReadingData(ServerId(123), tablets);

    vector<MasterService::Replica> replicas {
        // Started in initial round of RPCs - eventually fails
        {backup1Id.getId(), 87},
        // Skipped in initial round of RPCs (prior is in-flight)
        // starts later after failure from earlier entry
        {backup2Id.getId(), 87},
        // Started in initial round of RPCs - eventually succeeds
        {backup1Id.getId(), 88},
        // Skipped in all rounds of RPCs (prior succeeds)
        {backup2Id.getId(), 88},
        // Started in initial round of RPCs - eventually fails
        {backup1Id.getId(), 89},
        // Fails to start in initial round of RPCs - bad locator
        {1003, 90},
        // Started in initial round of RPCs - eventually fails
        {backup1Id.getId(), 91},
        // Fails to start in later rounds of RPCs - bad locator
        {1004, 92},
        // Started in later rounds of RPCs - eventually fails
        {backup1Id.getId(), 93},
    };

    TestLog::Enable _;
    EXPECT_THROW(service->recover(ServerId(123, 0), 0, replicas),
                 SegmentRecoveryFailedException);
    // 1,2,3) 87 was requested from the first server list entry.
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "recover: Starting getRecoveryData from server . at mock:host=backup1 "
        "for segment 87 on channel . (initial round of RPCs)",
        TestLog::get()));
    typedef MasterService::Replica::State State;
    EXPECT_EQ(State::FAILED, replicas.at(0).state);
    // 2,3) 87 was *not* requested a second time in the initial RPC round
    // but was requested later once the first failed.
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "recover: Starting getRecoveryData from server . at mock:host=backup2 "
        "for segment 87 .* (after RPC completion)",
        TestLog::get()));
    // 1,4) 88 was requested from the third server list entry and
    //      succeeded, which knocks the third and forth entries into
    //      OK status, preventing the launch of the forth entry
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "recover: Starting getRecoveryData from server . at mock:host=backup1 "
        "for segment 88 on channel . (initial round of RPCs)",
        TestLog::get()));
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "recover: Checking server . at mock:host=backup1 off the list for 88"
        " | "
        "recover: Checking server . at mock:host=backup2 off the list for 88",
        TestLog::get()));
    // 1,4) 88 was requested NOT from the forth server list entry.
    EXPECT_TRUE(TestUtil::doesNotMatchPosixRegex(
        "recover: Starting getRecoveryData from server . at mock:host=backup2 "
        "for segment 88 .* (after RPC completion)",
        TestLog::get()));
    EXPECT_EQ(State::OK, replicas.at(2).state);
    EXPECT_EQ(State::OK, replicas.at(3).state);
    // 1) Checking to ensure RPCs for 87, 88, 89, 90 went first round
    //    and that 91 got issued in place, first-found due to 90's
    //    bad locator
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "recover: Starting getRecoveryData from server . at mock:host=backup1 "
        "for segment 89 on channel . (initial round of RPCs)",
        TestLog::get()));
    EXPECT_EQ(State::FAILED, replicas.at(4).state);
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "recover: Starting getRecoveryData from server 1003 at "
        "(locator unavailable) "
        "for segment 90 on channel . (initial round of RPCs)",
        TestLog::get()));
    // 5) Checks bad locators for initial RPCs are handled
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "recover: No record of backup ID 1003, trying next backup",
        TestLog::get()));
    EXPECT_EQ(State::FAILED, replicas.at(5).state);
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "recover: Starting getRecoveryData from server . at mock:host=backup1 "
        "for segment 91 on channel . (initial round of RPCs)",
        TestLog::get()));
    EXPECT_EQ(State::FAILED, replicas.at(6).state);
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "recover: Starting getRecoveryData from server 1004 at "
        "(locator unavailable) "
        "for segment 92 on channel . (after RPC completion)",
        TestLog::get()));
    // 5) Checks bad locators for non-initial RPCs are handled
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "recover: No record of backup ID 1004, trying next backup",
        TestLog::get()));
    EXPECT_EQ(State::FAILED, replicas.at(7).state);
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "recover: Starting getRecoveryData from server . at mock:host=backup1 "
        "for segment 93 on channel . (after RPC completion)",
        TestLog::get()));
    EXPECT_EQ(State::FAILED, replicas.at(8).state);

    free(segMem);
}

static bool
recoveryMasterFinishedFilter(string s)
{
    return (s == "recoveryMasterFinished");
}

TEST_F(MasterServiceTest, recover_ctimeUpdateIssued) {
    TestLog::Enable _(recoveryMasterFinishedFilter);
    client->write(0, "0", 1, "abcdef", 6);
    ProtoBuf::Tablets tablets;
    createTabletList(tablets);
    RecoverRpc::Replica replicas[] = {};
    client->recover(10lu, ServerId(123), 0, tablets, replicas, 0);

    EXPECT_TRUE(StringUtil::startsWith(TestLog::get(),
        "recoveryMasterFinished: called by masterId 2 with 4 tablets | "
        "recoveryMasterFinished: Recovered tablets | "
        "recoveryMasterFinished: tablet { "
        "table_id: 123 start_key_hash: 0 end_key_hash: 9 state: RECOVERING "
        "server_id: 2 service_locator: \"mock:host=master\" user_data: 0 "
        "ctime_log_head_id: 0 ctime_log_head_offset: 79 } tablet { table_id: "
        "123 start_key_hash: 10 end_key_hash: 19 state: RECOVERING server_id: "
        "2 service_locator: \"mock:host=master\" user_data: 0 "
        "ctime_log_head_id: 0 ctime_log_head_offset: 79 } tablet { table_id: "
        "123 start_key_hash: 20 end_key_hash: 29 state: RECOVERING server_id: "
        "2 service_locator: \"mock:host=master\" user_data: "));
}

namespace {
bool recoverFilter(string s) {
    return s == "recover";
}
}

TEST_F(MasterServiceTest, recover_unsuccessful) {
    TestLog::Enable _(recoverFilter);
    client->write(0, "0", 1, "abcdef", 6);
    ProtoBuf::Tablets tablets;
    createTabletList(tablets);
    RecoverRpc::Replica replicas[] = {
        // Bad ServerId, should cause recovery to fail.
        {1004, 92},
    };
    client->recover(10lu, {123, 0}, 0, tablets, replicas, 1);

    string log = TestLog::get();
    log = log.substr(log.rfind("recover:"));
    EXPECT_EQ("recover: Failed to recover partition for recovery 10; "
              "aborting recovery on this recovery master", log);

    foreach (const auto& tablet, tablets.tablet()) {
        foreach (const auto& mtablet, service->tablets.tablet()) {
            EXPECT_FALSE(tablet.table_id() == mtablet.table_id() &&
                         tablet.start_key_hash() == mtablet.start_key_hash() &&
                         tablet.end_key_hash() == mtablet.end_key_hash());
        }
    }
}

TEST_F(MasterServiceTest, recoverSegment) {
    uint32_t segLen = 8192;
    char seg[segLen];
    uint32_t len; // number of bytes in a recovery segment
    Buffer value;
    Buffer buffer;
    HashTable::Reference reference;
    HashTable::Reference logTomb1Ref;
    HashTable::Reference logTomb2Ref;
    LogEntryType type;
    bool ret;

    ////////////////////////////////////////////////////////////////////
    // For Object recovery there are 3 major cases.
    //  1) Object is in the HashTable, but no corresponding
    //     ObjectTombstone.
    //     The recovered obj is only added if the version is newer than
    //     the existing obj.
    //
    //  2) Opposite of 1 above.
    //     The recovered obj is only added if the version is newer than
    //     the tombstone. If so, the tombstone is also discarded.
    //
    //  3) Neither an Object nor ObjectTombstone is present.
    //     The recovered obj is always added.
    ////////////////////////////////////////////////////////////////////

    // Case 1a: Newer object already there; ignore object.
    Key key0(0, "key0", 4);
    len = buildRecoverySegment(seg, segLen, key0, 1, "newer guy");
    service->recoverSegment(0, seg, len);
    verifyRecoveryObject(key0, "newer guy");
    len = buildRecoverySegment(seg, segLen, key0, 0, "older guy");
    service->recoverSegment(0, seg, len);
    verifyRecoveryObject(key0, "newer guy");

    // Case 1b: Older object already there; replace object.
    Key key1(0, "key1", 4);
    len = buildRecoverySegment(seg, segLen, key1, 0, "older guy");
    service->recoverSegment(0, seg, len);
    verifyRecoveryObject(key1, "older guy");
    len = buildRecoverySegment(seg, segLen, key1, 1, "newer guy");
    service->recoverSegment(0, seg, len);
    verifyRecoveryObject(key1, "newer guy");

    // Case 2a: Equal/newer tombstone already there; ignore object.
    Key key2(0, "key2", 4);
    Object o1(key2, NULL, 0, 1, 0);
    ObjectTombstone t1(o1, 0, 0);
    buffer.reset();
    t1.serializeToBuffer(buffer);
    service->log.append(LOG_ENTRY_TYPE_OBJTOMB, buffer, true, logTomb1Ref);
    ret = service->objectMap.replace(key2, logTomb1Ref);
    EXPECT_FALSE(ret);
    len = buildRecoverySegment(seg, segLen, key2, 1, "equal guy");
    service->recoverSegment(0, seg, len);
    len = buildRecoverySegment(seg, segLen, key2, 0, "older guy");
    service->recoverSegment(0, seg, len);
    EXPECT_TRUE(service->objectMap.lookup(key2, reference));
    EXPECT_EQ(reference, logTomb1Ref);
    service->removeTombstones();
    EXPECT_THROW(client->read(0, "key2", 4, &value),
                 ObjectDoesntExistException);

    // Case 2b: Lesser tombstone already there; add object, remove tomb.
    Key key3(0, "key3", 4);
    Object o2(key3, NULL, 0, 10, 0);
    ObjectTombstone t2(o2, 0, 0);
    buffer.reset();
    t2.serializeToBuffer(buffer);
    ret = service->log.append(LOG_ENTRY_TYPE_OBJTOMB, buffer, true, logTomb2Ref);
    EXPECT_TRUE(ret);
    ret = service->objectMap.replace(key3, logTomb2Ref);
    EXPECT_FALSE(ret);
    len = buildRecoverySegment(seg, segLen, key3, 11, "newer guy");
    service->recoverSegment(0, seg, len);
    verifyRecoveryObject(key3, "newer guy");
    EXPECT_TRUE(service->objectMap.lookup(key3, reference));
    EXPECT_NE(reference, logTomb1Ref);
    EXPECT_NE(reference, logTomb2Ref);
    service->removeTombstones();

    // Case 3: No tombstone, no object. Recovered object always added.
    Key key4(0 , "key4", 4);
    EXPECT_FALSE(service->objectMap.lookup(key4, reference));
    len = buildRecoverySegment(seg, segLen, key4, 0, "only guy");
    service->recoverSegment(0, seg, len);
    verifyRecoveryObject(key4, "only guy");

    ////////////////////////////////////////////////////////////////////
    // For ObjectTombstone recovery there are the same 3 major cases:
    //  1) Object is in  the HashTable, but no corresponding
    //     ObjectTombstone.
    //     The recovered tomb is only added if the version is equal to
    //     or greater than the object. If so, the object is purged.
    //
    //  2) Opposite of 1 above.
    //     The recovered tomb is only added if the version is newer than
    //     the current tombstone. If so, the old tombstone is discarded.
    //
    //  3) Neither an Object nor ObjectTombstone is present.
    //     The recovered tombstone is always added.
    ////////////////////////////////////////////////////////////////////

    // Case 1a: Newer object already there; ignore tombstone.
    Key key5(0, "key5", 4);
    len = buildRecoverySegment(seg, segLen, key5, 1, "newer guy");
    service->recoverSegment(0, seg, len);
    Object o3(key5, NULL, 0, 4, 0);
    ObjectTombstone t3(o3, 0, 0);
    len = buildRecoverySegment(seg, segLen, t3);
    service->recoverSegment(0, seg, len);
    verifyRecoveryObject(key5, "newer guy");

    // Case 1b: Equal/older object already there; discard and add tombstone.
    Key key6(0, "key6", 4);
    len = buildRecoverySegment(seg, segLen, key6, 0, "equal guy");
    service->recoverSegment(0, seg, len);
    verifyRecoveryObject(key6, "equal guy");
    Object o4(key6, NULL, 0, 0, 0);
    ObjectTombstone t4(o4, 0, 0);
    len = buildRecoverySegment(seg, segLen, t4);
    service->recoverSegment(0, seg, len);
    service->removeTombstones();
    EXPECT_FALSE(service->objectMap.lookup(key6, reference));
    EXPECT_THROW(client->read(0, "key6", 4, &value),
                 ObjectDoesntExistException);

    Key key7(0, "key7", 4);
    len = buildRecoverySegment(seg, segLen, key7, 0, "older guy");
    service->recoverSegment(0, seg, len);
    verifyRecoveryObject(key7, "older guy");
    Object o5(key7, NULL, 0, 1, 0);
    ObjectTombstone t5(o5, 0, 0);
    len = buildRecoverySegment(seg, segLen, t5);
    service->recoverSegment(0, seg, len);
    service->removeTombstones();
    EXPECT_FALSE(service->objectMap.lookup(key7, reference));
    EXPECT_THROW(client->read(0, "key7", 4, &value),
                 ObjectDoesntExistException);

    // Case 2a: Newer tombstone already there; ignore.
    Key key8(0, "key8", 4);
    Object o6(key8, NULL, 0, 1, 0);
    ObjectTombstone t6(o6, 0, 0);
    len = buildRecoverySegment(seg, segLen, t6);
    service->recoverSegment(0, seg, len);
    buffer.reset();
    ret = service->lookup(key8, type, buffer);
    EXPECT_TRUE(ret);
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJTOMB, type);
    ObjectTombstone t6InLog(buffer);
    EXPECT_EQ(1U, t6InLog.getObjectVersion());
    ObjectTombstone t7(o6, 0, 0);
    t7.serializedForm.objectVersion = 0;
    len = buildRecoverySegment(seg, segLen, t7);
    service->recoverSegment(0, seg, len);
    const uint8_t* t6LogPtr = buffer.getStart<uint8_t>();
    buffer.reset();
    ret = service->lookup(key8, type, buffer);
    EXPECT_TRUE(ret);
    EXPECT_EQ(t6LogPtr, buffer.getStart<uint8_t>());

    // Case 2b: Older tombstone already there; replace.
    Key key9(0, "key9", 4);
    Object o8(key9, NULL, 0, 0, 0);
    ObjectTombstone t8(o8, 0, 0);
    len = buildRecoverySegment(seg, segLen, t8);
    service->recoverSegment(0, seg, len);
    buffer.reset();
    ret = service->lookup(key9, type, buffer);
    EXPECT_TRUE(ret);
    ObjectTombstone t8InLog(buffer);
    EXPECT_EQ(0U, t8InLog.getObjectVersion());

    Object o9(key9, NULL, 0, 1, 0);
    ObjectTombstone t9(o8, 0, 0);
    len = buildRecoverySegment(seg, segLen, t9);
    service->recoverSegment(0, seg, len);
    buffer.reset();
    ret = service->lookup(key9, type, buffer);
    EXPECT_TRUE(ret);
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJTOMB, type);
    ObjectTombstone t9InLog(buffer);
    EXPECT_EQ(1U, t9InLog.getObjectVersion());

    // Case 3: No tombstone, no object. Recovered tombstone always added.
    Key key10(0, "key10", 5);
    EXPECT_FALSE(service->objectMap.lookup(key10, reference));
    Object o10(key10, NULL, 0, 0, 0);
    ObjectTombstone t10(o10, 0, 0);
    len = buildRecoverySegment(seg, segLen, t10);
    service->recoverSegment(0, seg, len);
    buffer.reset();
    EXPECT_TRUE(service->lookup(key10, type, buffer));
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJTOMB, type);
    Buffer t10Buffer;
    t10.serializeToBuffer(t10Buffer);
    EXPECT_EQ(0, memcmp(t10Buffer.getRange(0, t10Buffer.getTotalLength()),
                        buffer.getRange(0, buffer.getTotalLength()),
                        buffer.getTotalLength()));
}

TEST_F(MasterServiceTest, remove_basics) {
    client->write(0, "key0", 4, "item0", 5);

    uint64_t version;
    client->remove(0, "key0", 4, NULL, &version);
    EXPECT_EQ(1U, version);

    Buffer value;
    EXPECT_THROW(client->read(0, "key0", 4, &value),
                 ObjectDoesntExistException);
}

TEST_F(MasterServiceTest, remove_badTable) {
    EXPECT_THROW(client->remove(4, "key0", 4), UnknownTableException);
}

TEST_F(MasterServiceTest, remove_rejectRules) {
    client->write(0, "key0", 4, "item0", 5);

    RejectRules rules;
    memset(&rules, 0, sizeof(rules));
    rules.versionNeGiven = true;
    rules.givenVersion = 2;
    uint64_t version;
    EXPECT_THROW(client->remove(0, "key0", 4, &rules, &version),
                 WrongVersionException);
    EXPECT_EQ(1U, version);
}

TEST_F(MasterServiceTest, remove_objectAlreadyDeletedRejectRules) {
    RejectRules rules;
    memset(&rules, 0, sizeof(rules));
    rules.doesntExist = true;
    uint64_t version;
    EXPECT_THROW(client->remove(0, "key0", 4, &rules, &version),
                 ObjectDoesntExistException);
    EXPECT_EQ(VERSION_NONEXISTENT, version);
}

TEST_F(MasterServiceTest, remove_objectAlreadyDeleted) {
    uint64_t version;
    client->remove(0, "key1", 4, NULL, &version);
    EXPECT_EQ(VERSION_NONEXISTENT, version);

    client->write(0, "key0", 4, "item0", 5);
    client->remove(0, "key0", 4);
    client->remove(0, "key0", 4, NULL, &version);
    EXPECT_EQ(VERSION_NONEXISTENT, version);
}

TEST_F(MasterServiceTest, incrementReadAndWriteStatistics) {
    Buffer value;
    uint64_t version;
    int64_t objectValue = 16;

    client->write(0, "key0", 4, &objectValue, 8, NULL, &version);
    client->write(0, "key1", 4, &objectValue, 8, NULL, &version);
    client->read(0, "key0", 4, &value);
    client->increment(0, "key0", 4, 5, NULL, &version, &objectValue);

    std::vector<MasterClient::ReadObject*> requests;

    Tub<Buffer> val1;
    MasterClient::ReadObject request1(0, "key0", 4, &val1);
    request1.status = STATUS_RETRY;
    requests.push_back(&request1);
    Tub<Buffer> val2;
    MasterClient::ReadObject request2(0, "key1", 4, &val2);
    request2.status = STATUS_RETRY;
    requests.push_back(&request2);

    client->multiRead(requests);

    Table* table = reinterpret_cast<Table*>(
                                    service->tablets.tablet(0).user_data());
    EXPECT_EQ((uint64_t)7, table->statEntry.number_read_and_writes());
}


TEST_F(MasterServiceTest, GetServerStatistics) {
    Buffer value;
    uint64_t version;
    int64_t objectValue = 16;

    client->write(0, "key0", 4, &objectValue, 8, NULL, &version);
    client->read(0, "key0", 4, &value);
    client->read(0, "key0", 4, &value);
    client->read(0, "key0", 4, &value);

    ProtoBuf::ServerStatistics serverStats;
    client->getServerStatistics(serverStats);
    EXPECT_EQ("tabletentry { table_id: 0 start_key_hash: 0 "
              "end_key_hash: 18446744073709551615 number_read_and_writes: 4 }",
              serverStats.ShortDebugString());

    client->splitMasterTablet(0, 0, ~0UL, (~0UL/2));
    client->getServerStatistics(serverStats);
    EXPECT_EQ("tabletentry { table_id: 0 "
              "start_key_hash: 0 "
              "end_key_hash: 9223372036854775806 } "
              "tabletentry { table_id: 0 start_key_hash: 9223372036854775807 "
              "end_key_hash: 18446744073709551615 }",
              serverStats.ShortDebugString());
}


TEST_F(MasterServiceTest, splitMasterTablet) {

    client->splitMasterTablet(0, 0, ~0UL, (~0UL/2));
    EXPECT_TRUE(TestUtil::matchesPosixRegex("tablet { table_id: 0 "
              "start_key_hash: 0 "
              "end_key_hash: 9223372036854775806 user_data: [0-9]* } "
              "tablet { table_id: 0 start_key_hash: 9223372036854775807 "
              "end_key_hash: 18446744073709551615 user_data: [0-9]* }",
              service->tablets.ShortDebugString()));
}

static bool
dropTabletOwnership_filter(string s)
{
    return s == "dropTabletOwnership";
}

TEST_F(MasterServiceTest, dropTabletOwnership) {
    TestLog::Enable _(dropTabletOwnership_filter);

    EXPECT_THROW(client->dropTabletOwnership(1, 1, 1), ClientException);
    EXPECT_EQ("dropTabletOwnership: Could not drop ownership on unknown "
        "tablet (1, range [1,1])!", TestLog::get());

    TestLog::reset();

    client->takeTabletOwnership(1, 1, 1);
    client->dropTabletOwnership(1, 1, 1);
    EXPECT_EQ("dropTabletOwnership: Dropping ownership of tablet "
        "(1, range [1,1])", TestLog::get());
}

static bool
takeTabletOwnership_filter(string s)
{
    return s == "takeTabletOwnership";
}

TEST_F(MasterServiceTest, takeTabletOwnership_newTablet) {
    TestLog::Enable _(takeTabletOwnership_filter);

    std::unique_ptr<Table> table1(new Table(1 , 0 , 1));
    uint64_t addrTable1 = reinterpret_cast<uint64_t>(table1.get());
    std::unique_ptr<Table> table2(new Table(2 , 0 , 1));
    uint64_t addrTable2 = reinterpret_cast<uint64_t>(table2.get());

    // Start empty.
    service->tablets.mutable_tablet()->RemoveLast();
    EXPECT_EQ("", service->tablets.ShortDebugString());

    { // set t1 and t2 directly
        ProtoBuf::Tablets_Tablet& t1(*service->tablets.add_tablet());
        t1.set_table_id(1);
        t1.set_start_key_hash(0);
        t1.set_end_key_hash(1);
        t1.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
        t1.set_user_data(reinterpret_cast<uint64_t>(table1.release()));

        ProtoBuf::Tablets_Tablet& t2(*service->tablets.add_tablet());
        t2.set_table_id(2);
        t2.set_start_key_hash(0);
        t2.set_end_key_hash(1);
        t2.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
        t2.set_user_data(reinterpret_cast<uint64_t>(table2.release()));

        EXPECT_EQ(format(
            "tablet { table_id: 1 start_key_hash: 0 end_key_hash: 1 "
                "state: NORMAL user_data: %lu } "
            "tablet { table_id: 2 start_key_hash: 0 end_key_hash: 1 "
                "state: NORMAL user_data: %lu }",
            addrTable1, addrTable2),
                                service->tablets.ShortDebugString());
    }

    { // set t2, t2b, and t3 through client
        client->takeTabletOwnership(2, 2, 3);
        client->takeTabletOwnership(2, 4, 5);
        client->takeTabletOwnership(3, 0, 1);

        EXPECT_EQ(format(
            "tablet { table_id: 1 start_key_hash: 0 end_key_hash: 1 "
                "state: NORMAL user_data: %lu } "
            "tablet { table_id: 2 start_key_hash: 0 end_key_hash: 1 "
                "state: NORMAL user_data: %lu } "
            "tablet { table_id: 2 start_key_hash: 2 end_key_hash: 3 "
                "state: NORMAL user_data: %lu } "
            "tablet { table_id: 2 start_key_hash: 4 end_key_hash: 5 "
                "state: NORMAL user_data: %lu } "
            "tablet { table_id: 3 start_key_hash: 0 end_key_hash: 1 "
                "state: NORMAL user_data: %lu }",
            addrTable1, addrTable2,
            service->tablets.tablet(2).user_data(),
            service->tablets.tablet(3).user_data(),
            service->tablets.tablet(4).user_data()),
                                service->tablets.ShortDebugString());

        EXPECT_EQ(
            "takeTabletOwnership: Taking ownership of new tablet "
                "(2, range [2,3]) | "
            "takeTabletOwnership: Taking ownership of new tablet "
                "(2, range [4,5]) | "
            "takeTabletOwnership: Taking ownership of new tablet "
                "(3, range [0,1])", TestLog::get());
    }

    TestLog::reset();

    // Test assigning ownership of an already-owned tablet.
    {
        client->takeTabletOwnership(2, 2, 3);
        EXPECT_EQ("takeTabletOwnership: Taking ownership of existing tablet "
            "(2, range [2,3]) in state 0 | takeTabletOwnership: Taking "
            "ownership when existing tablet is in unexpected state (0)!",
            TestLog::get());
    }

    TestLog::reset();

    // Test partially overlapping sanity check. The coordinator should
    // know better, but I'd rather be safe sorry...
    {
        EXPECT_THROW(client->takeTabletOwnership(2, 2, 2), ClientException);
        EXPECT_EQ("takeTabletOwnership: Tablet being assigned (2, range [2,2]) "
            "partially overlaps an existing tablet!", TestLog::get());
    }
}

TEST_F(MasterServiceTest, takeTabletOwnership_migratingTablet) {
    TestLog::Enable _(takeTabletOwnership_filter);

    // Fake up a tablet in migration.
    ProtoBuf::Tablets_Tablet& tab(*service->tablets.add_tablet());
    tab.set_table_id(1);
    tab.set_start_key_hash(0);
    tab.set_end_key_hash(5);
    tab.set_state(ProtoBuf::Tablets_Tablet_State_RECOVERING);
    tab.set_user_data(reinterpret_cast<uint64_t>(new Table(1 , 0 , 5)));

    client->takeTabletOwnership(1, 0, 5);

    EXPECT_EQ(
        "takeTabletOwnership: Taking ownership of existing tablet "
            "(1, range [0,5]) in state 1", TestLog::get());
}

static bool
prepForMigrationFilter(string s)
{
    return s == "prepForMigration";
}

TEST_F(MasterServiceTest, prepForMigration) {
    ProtoBuf::Tablets_Tablet& tablet(*service->tablets.add_tablet());
    tablet.set_table_id(5);
    tablet.set_start_key_hash(27);
    tablet.set_end_key_hash(873);
    tablet.set_user_data(reinterpret_cast<uint64_t>(new Table(0 , 27 , 873)));

    TestLog::Enable _(prepForMigrationFilter);

    // Overlap
    EXPECT_THROW(client->prepForMigration(5, 27, 873, 0, 0),
        ObjectExistsException);
    EXPECT_EQ("prepForMigration: already have tablet in range "
        "[27, 873] for tableId 5", TestLog::get());
    EXPECT_THROW(client->prepForMigration(5, 0, 27, 0, 0),
        ObjectExistsException);
    EXPECT_THROW(client->prepForMigration(5, 873, 82743, 0, 0),
        ObjectExistsException);

    TestLog::reset();
    client->prepForMigration(5, 1000, 2000, 0, 0);
    int i = service->tablets.tablet_size() - 1;
    EXPECT_EQ(5U, service->tablets.tablet(i).table_id());
    EXPECT_EQ(1000U, service->tablets.tablet(i).start_key_hash());
    EXPECT_EQ(2000U, service->tablets.tablet(i).end_key_hash());
    EXPECT_EQ(ProtoBuf::Tablets_Tablet_State_RECOVERING,
        service->tablets.tablet(i).state());
    EXPECT_EQ("prepForMigration: Ready to receive tablet from "
        "\"\?\?\". Table 5, range [1000,2000]", TestLog::get());
}

static bool
migrateTabletFilter(string s)
{
    return s == "migrateTablet";
}

TEST_F(MasterServiceTest, migrateTablet_simple) {
    ProtoBuf::Tablets_Tablet& tablet(*service->tablets.add_tablet());
    tablet.set_table_id(5);
    tablet.set_start_key_hash(27);
    tablet.set_end_key_hash(873);
    tablet.set_user_data(reinterpret_cast<uint64_t>(new Table(0 , 27 , 873)));

    TestLog::Enable _(migrateTabletFilter);

    // Wrong table
    EXPECT_THROW(client->migrateTablet(4, 0, -1, ServerId(0, 0)),
        UnknownTableException);
    EXPECT_EQ("migrateTablet: Migration request for range this master does "
        "not own. TableId 4, range [0,18446744073709551615]", TestLog::get());
    EXPECT_THROW(client->migrateTablet(5, 0, 26, ServerId(0, 0)),
        UnknownTableException);
    EXPECT_THROW(client->migrateTablet(5, 874, -1, ServerId(0, 0)),
        UnknownTableException);

    // Migrating to self!?
    TestLog::reset();
    EXPECT_THROW(client->migrateTablet(5, 27, 873, masterServer->serverId),
        RequestFormatError);
    EXPECT_EQ("migrateTablet: Migrating to myself doesn't make much sense",
        TestLog::get());
}

TEST_F(MasterServiceTest, migrateTablet_movingData) {
    coordinator->createTable("migrationTable");
    uint64_t tbl = coordinator->getTableId("migrationTable");
    client->write(tbl, "hi", 2, "abcdefg", 7);

    ServerConfig master2Config = masterConfig;
    master2Config.master.numReplicas = 0;
    master2Config.localLocator = "mock:host=master2";
    Server* master2 = cluster.addServer(master2Config);

    TestLog::Enable _(migrateTabletFilter);

    client->migrateTablet(tbl, 0, -1, master2->serverId);
    EXPECT_EQ("migrateTablet: Migrating tablet (id 0, first 0, last "
        "18446744073709551615) to ServerId 3 (\"mock:host=master2\") "
        "| migrateTablet: Sending last migration segment | "
        "migrateTablet: Tablet migration succeeded. Sent 1 objects "
        "and 0 tombstones. 35 bytes in total.", TestLog::get());
}

static bool
receiveMigrationDataFilter(string s)
{
    return s == "receiveMigrationData";
}

TEST_F(MasterServiceTest, receiveMigrationData) {
    Segment s;

    client->prepForMigration(5, 1000, 2000, 0, 0);

    TestLog::Enable _(receiveMigrationDataFilter);

    EXPECT_THROW(client->receiveMigrationData(6, 0, s),
        UnknownTableException);
    EXPECT_EQ("receiveMigrationData: migration data received for "
        "unknown tablet 6, firstKey 0", TestLog::get());
    EXPECT_THROW(client->receiveMigrationData(5, 0, s),
        UnknownTableException);

    TestLog::reset();
    EXPECT_THROW(client->receiveMigrationData(0, 0, s),
        InternalError);
    EXPECT_EQ("receiveMigrationData: migration data received for tablet "
        "not in the RECOVERING state (state = NORMAL)!", TestLog::get());

    Key key(5, "wee!", 4);
    Object o(key, "watch out for the migrant object", 32, 0, 0);

    Buffer buffer;
    o.serializeToBuffer(buffer);

    s.append(LOG_ENTRY_TYPE_OBJ, buffer);
    s.close();

    client->receiveMigrationData(5, 1000, s);

    LogEntryType logType;
    Buffer logBuffer;
    bool success = service->lookup(key, logType, logBuffer);
    EXPECT_TRUE(success);
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, logType);
    Object logObject(logBuffer);
    EXPECT_EQ(0, memcmp(logObject.getData(),
                        "watch out for the migrant object",
                        32));
}

TEST_F(MasterServiceTest, write_basics) {
    Buffer value;
    uint64_t version;

    client->write(0, "key0", 4, "item0", 5, NULL, &version);
    EXPECT_EQ(1U, version);
    client->read(0, "key0", 4, &value);
    EXPECT_EQ("item0", TestUtil::toString(&value));
    EXPECT_EQ(1U, version);

    client->write(0, "key0", 4, "item0-v2", 8, NULL, &version);
    EXPECT_EQ(2U, version);
    client->read(0, "key0", 4, &value);
    EXPECT_EQ("item0-v2", TestUtil::toString(&value));
    EXPECT_EQ(2U, version);

    client->write(0, "key0", 4, "item0-v3", 8, NULL, &version);
    EXPECT_EQ(3U, version);
    client->read(0, "key0", 4, &value);
    EXPECT_EQ("item0-v3", TestUtil::toString(&value));
    EXPECT_EQ(3U, version);
}

TEST_F(MasterServiceTest, write_rejectRules) {
    RejectRules rules;
    memset(&rules, 0, sizeof(rules));
    rules.doesntExist = true;
    uint64_t version;
    EXPECT_THROW(client->write(0, "key0", 4, "item0", 5, &rules, &version),
                 ObjectDoesntExistException);
    EXPECT_EQ(VERSION_NONEXISTENT, version);
}

TEST_F(MasterServiceTest, increment) {
    Buffer buffer;
    uint64_t version;
    int64_t oldValue = 16;
    int32_t oldValue32 = 16;
    int64_t newValue;
    int64_t readResult;

    client->write(0, "key0", 4, &oldValue, 8, NULL, &version);
    client->increment(0, "key0", 4, 5, NULL, &version, &newValue);
    client->increment(0, "key0", 4, 0, NULL, NULL, NULL);
    EXPECT_EQ(2U, version);
    EXPECT_EQ(21, newValue);

    client->read(0, "key0", 4, &buffer);
    buffer.copy(0, sizeof(int64_t), &readResult);
    EXPECT_EQ(newValue, readResult);

    client->write(0, "key1", 4, &oldValue, 8, NULL, &version);
    client->increment(0, "key1", 4, -32, NULL, &version, &newValue);
    EXPECT_EQ(-16, newValue);

    client->read(0, "key1", 4, &buffer);
    buffer.copy(0, sizeof(int64_t), &readResult);
    EXPECT_EQ(newValue, readResult);

    client->write(0, "key2", 4, &oldValue32, 4, NULL, &version);
    EXPECT_THROW(client->increment(0, "key2", 4, 4, NULL, &version, &newValue),
                 InvalidObjectException);
}

TEST_F(MasterServiceTest, increment_rejectRules) {
    Buffer buffer;
    RejectRules rules;
    memset(&rules, 0, sizeof(rules));
    rules.exists = true;
    uint64_t version;
    int64_t oldValue = 16;
    int64_t newValue;

    client->write(0, "key0", 4, &oldValue, 8, NULL, &version);
    EXPECT_THROW(client->increment(0, "key0", 4, 5, &rules, &version,
                 &newValue),
        ObjectExistsException);
}

/**
 * Generate a random string.
 *
 * \param str
 *      Pointer to location where the string generated will be stored.
 * \param length
 *      Length of the string to be generated in bytes including the terminating
 *      null character.
 */
void
genRandomString(char* str, const int length) {
    static const char alphanum[] =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    for (int i = 0; i < length - 1; ++i) {
        str[i] = alphanum[generateRandom() % (sizeof(alphanum) - 1)];
    }
    str[length - 1] = 0;
}

TEST_F(MasterServiceTest, write_varyingKeyLength) {
    uint16_t keyLengths[] = {
         1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50,
         55, 60, 65, 70, 75, 80, 85, 90, 95, 100,
         200, 300, 400, 500, 600, 700, 800, 900, 1000,
         2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000,
         20000, 30000, 40000, 50000, 60000
    };

    foreach (uint16_t keyLength, keyLengths) {
        char key[keyLength];
        genRandomString(key, keyLength);

        string writeVal = format("objectValue%u", keyLength);
        Buffer value;
        uint64_t version;

        client->write(0, key, keyLength, writeVal.c_str(),
                      downCast<uint16_t>(writeVal.length()),
                      NULL, &version);
        client->read(0, key, keyLength, &value);

        EXPECT_EQ(writeVal, TestUtil::toString(&value));
    }
}

TEST_F(MasterServiceTest, getTable) {
    // Table exists.
    Key key1(0, "0", 1);
    EXPECT_TRUE(service->getTable(key1) != NULL);

    // Table doesn't exist.
    Key key2(1000, "0", 1);
    EXPECT_TRUE(service->getTable(key2) == NULL);
}

TEST_F(MasterServiceTest, rejectOperation) {
    RejectRules empty, rules;
    memset(&empty, 0, sizeof(empty));

    // Fail: object doesn't exist.
    rules = empty;
    rules.doesntExist = 1;
    EXPECT_EQ(service->rejectOperation(rules, VERSION_NONEXISTENT),
              STATUS_OBJECT_DOESNT_EXIST);

    // Succeed: object doesn't exist.
    rules = empty;
    rules.exists = rules.versionLeGiven = rules.versionNeGiven = 1;
    EXPECT_EQ(service->rejectOperation(rules, VERSION_NONEXISTENT),
              STATUS_OK);

    // Fail: object exists.
    rules = empty;
    rules.exists = 1;
    EXPECT_EQ(service->rejectOperation(rules, 2),
              STATUS_OBJECT_EXISTS);

    // versionLeGiven.
    rules = empty;
    rules.givenVersion = 0x400000001;
    rules.versionLeGiven = 1;
    EXPECT_EQ(service->rejectOperation(rules, 0x400000000),
              STATUS_WRONG_VERSION);
    EXPECT_EQ(service->rejectOperation(rules, 0x400000001),
              STATUS_WRONG_VERSION);
    EXPECT_EQ(service->rejectOperation(rules, 0x400000002),
              STATUS_OK);

    // versionNeGiven.
    rules = empty;
    rules.givenVersion = 0x400000001;
    rules.versionNeGiven = 1;
    EXPECT_EQ(service->rejectOperation(rules, 0x400000000),
              STATUS_WRONG_VERSION);
    EXPECT_EQ(service->rejectOperation(rules, 0x400000001),
              STATUS_OK);
    EXPECT_EQ(service->rejectOperation(rules, 0x400000002),
              STATUS_WRONG_VERSION);
}

TEST_F(MasterServiceTest, objectLivenessCallback_objectAlive) {
    Key key(0, "key0", 4);
    uint64_t version;

    client->write(key.getTableId(),
                  key.getStringKey(),
                  key.getStringKeyLength(),
                  "item0", 5, NULL, &version);

    LogEntryType type;
    Buffer buffer;
    bool success = service->lookup(key, type, buffer);
    EXPECT_TRUE(success);

    // Object exists
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, type);
    EXPECT_TRUE(service->checkLiveness(type, buffer));
}

TEST_F(MasterServiceTest, objectLivenessCallback_objectDeleted) {
    Key key(0, "key0", 4);
    uint64_t version;

    client->write(key.getTableId(),
                  key.getStringKey(),
                  key.getStringKeyLength(),
                  "item0", 5, NULL, &version);

    LogEntryType type;
    Buffer buffer;
    bool success = service->lookup(key, type, buffer);
    EXPECT_TRUE(success);

    client->remove(key.getTableId(),
                   key.getStringKey(),
                   key.getStringKeyLength());

    // Object does not exist
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, type);
    EXPECT_FALSE(service->checkLiveness(type, buffer));
}

TEST_F(MasterServiceTest, objectLivenessCallback_objectModified) {
    Key key(0, "key0", 4);
    uint64_t version;

    client->write(key.getTableId(),
                  key.getStringKey(),
                  key.getStringKeyLength(),
                  "item0", 5, NULL, &version);

    LogEntryType type;
    Buffer buffer;
    bool success = service->lookup(key, type, buffer);
    EXPECT_TRUE(success);

    // Object referenced by logObj1 exists
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, type);
    EXPECT_TRUE(service->checkLiveness(type, buffer));

    client->write(key.getTableId(),
                  key.getStringKey(),
                  key.getStringKeyLength(),
                  "item0-v2", 5, NULL, &version);
    LogEntryType type2;
    Buffer buffer2;
    success = service->lookup(key, type2, buffer2);

    // Object referenced by logObj1 does not exist anymore
    EXPECT_FALSE(service->checkLiveness(type, buffer));

    // Object referenced by logObj2 exists
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, type2);
    EXPECT_TRUE(service->checkLiveness(type2, buffer2));
}

TEST_F(MasterServiceTest, objectLivenessCallback_tableDoesntExist) {
    coordinator->createTable("table1");
    uint64_t tableId = coordinator->getTableId("table1");
    Key key(tableId, "key0", 4);
    uint64_t version;

    client->write(key.getTableId(),
                  key.getStringKey(),
                  key.getStringKeyLength(),
                  "item0", 5, NULL, &version);

    LogEntryType type;
    Buffer buffer;
    bool success = service->lookup(key, type, buffer);
    EXPECT_TRUE(success);
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, type);

    // Object exists
    EXPECT_TRUE(service->checkLiveness(type, buffer));

    coordinator->dropTable("table1");

    // Object is not live since table does not exist anymore
    EXPECT_FALSE(service->checkLiveness(type, buffer));
}

TEST_F(MasterServiceTest, objectRelocationCallback_objectAlive) {
    Key key(0, "key0", 4);
    uint64_t version;

    client->write(key.getTableId(),
                  key.getStringKey(),
                  key.getStringKeyLength(),
                  "item0", 5, NULL, &version);

    LogEntryType oldType;
    Buffer oldBuffer; 
    bool success = service->lookup(key, oldType, oldBuffer);
    EXPECT_TRUE(success);

    Table* table = service->getTable(key);
    table->objectCount++;
    table->objectBytes += oldBuffer.getTotalLength();

    HashTable::Reference newReference;
    success = service->log.append(LOG_ENTRY_TYPE_OBJ,
                                  oldBuffer,
                                  true,
                                  newReference);
    EXPECT_TRUE(success);

    LogEntryType newType;
    Buffer newBuffer;
    service->log.lookup(newReference, newType, newBuffer);

    LogEntryType oldType2;
    Buffer oldBuffer2;
    HashTable::Reference oldReference;
    success = service->lookup(key, oldType2, oldBuffer2, oldReference);
    EXPECT_TRUE(success);
    EXPECT_EQ(oldType, oldType2);
    EXPECT_EQ(oldBuffer.getStart<uint8_t>(), oldBuffer2.getStart<uint8_t>());

    uint64_t initialTotalTrackedBytes = table->objectBytes;

    EXPECT_TRUE(service->relocate(LOG_ENTRY_TYPE_OBJ, oldBuffer, newReference));
    EXPECT_EQ(initialTotalTrackedBytes, table->objectBytes);

    LogEntryType newType2;
    Buffer newBuffer2;
    HashTable::Reference newReference2;
    success = service->lookup(key, newType2, newBuffer2, newReference2);
    EXPECT_TRUE(success);
    EXPECT_EQ(newType, newType2);
    EXPECT_EQ(newReference, newReference2);
    EXPECT_NE(oldReference, newReference);
    EXPECT_NE(newBuffer.getStart<uint8_t>(), oldBuffer.getStart<uint8_t>());
    EXPECT_EQ(newBuffer.getStart<uint8_t>(), newBuffer2.getStart<uint8_t>());
}

TEST_F(MasterServiceTest, objectRelocationCallback_objectDeleted) {
    Key key(0, "key0", 4);
    uint64_t version;

    client->write(key.getTableId(),
                  key.getStringKey(),
                  key.getStringKeyLength(),
                  "item0", 5, NULL, &version);

    LogEntryType type;
    Buffer buffer;
    bool success = service->lookup(key, type, buffer);
    EXPECT_TRUE(success);

    Table* table = service->getTable(key);
    table->objectCount++;
    table->objectBytes += buffer.getTotalLength();

    client->remove(key.getTableId(),
                   key.getStringKey(),
                   key.getStringKeyLength());

    HashTable::Reference dummyReference;
    uint64_t initialTotalTrackedBytes = table->objectBytes;

    success = service->relocate(LOG_ENTRY_TYPE_OBJ, buffer, dummyReference);
    EXPECT_FALSE(success);
    EXPECT_EQ(initialTotalTrackedBytes - buffer.getTotalLength(),
              table->objectBytes);
}

TEST_F(MasterServiceTest, objectRelocationCallback_objectModified) {
    Key key(0, "key0", 4);
    uint64_t version;

    client->write(key.getTableId(),
                  key.getStringKey(),
                  key.getStringKeyLength(),
                  "item0", 5, NULL, &version);

    LogEntryType type;
    Buffer buffer;
    bool success = service->lookup(key, type, buffer);

    Table* table = service->getTable(key);
    table->objectCount++;
    table->objectBytes += buffer.getTotalLength();

    client->write(key.getTableId(),
                  key.getStringKey(),
                  key.getStringKeyLength(),
                  "item0-v2", 8, NULL, &version);

    HashTable::Reference dummyReference;
    uint64_t initialTotalTrackedBytes = table->objectBytes;

    success = service->relocate(LOG_ENTRY_TYPE_OBJ, buffer, dummyReference);
    EXPECT_FALSE(success);
    EXPECT_EQ(initialTotalTrackedBytes - buffer.getTotalLength(),
              table->objectBytes);
}

static bool
containsSegment(string s)
{
    return s == "containsSegment";
}

TEST_F(MasterServiceTest, tombstoneRelocationCallback_basics) {
    TestLog::Enable _(&containsSegment);
    Key key(0, "key0", 4);
    uint64_t version;

    client->write(key.getTableId(),
                  key.getStringKey(),
                  key.getStringKeyLength(),
                  "item0", 5, NULL, &version);

    LogEntryType type;
    Buffer buffer;
    HashTable::Reference reference;
    bool success = service->lookup(key, type, buffer, reference);
    EXPECT_TRUE(success);
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, type);

    Object object(buffer);
    ObjectTombstone tombstone(object,
                              service->log.getSegmentId(reference),
                              0);

    Buffer tombstoneBuffer;
    tombstone.serializeToBuffer(tombstoneBuffer);

    HashTable::Reference oldTombstoneReference;
    success = service->log.append(LOG_ENTRY_TYPE_OBJTOMB, tombstoneBuffer,
                                  true, oldTombstoneReference);
    EXPECT_TRUE(success);

    HashTable::Reference newTombstoneReference;
    success = service->log.append(LOG_ENTRY_TYPE_OBJTOMB, tombstoneBuffer,
                                  true, newTombstoneReference);
    EXPECT_TRUE(success);

    LogEntryType oldTypeInLog;
    Buffer oldBufferInLog; 
    service->log.lookup(oldTombstoneReference, oldTypeInLog, oldBufferInLog);

    success = service->relocate(LOG_ENTRY_TYPE_OBJTOMB,
                                  oldBufferInLog,
                                  newTombstoneReference);
    EXPECT_TRUE(success);

    // Check that tombstoneRelocationCallback() is checking the liveness
    // of the right segment (in log.containsSegment() function call).
    string comparisonString = "containsSegment: " +
            format("%lu", service->log.getSegmentId(oldTombstoneReference));
    EXPECT_EQ(comparisonString, TestLog::get());
}

/**
 * Unit tests requiring a full segment size (rather than the smaller default
 * allocation that's done to make tests faster).
 */

class MasterServiceFullSegmentSizeTest : public MasterServiceTest {
  public:
    MasterServiceFullSegmentSizeTest()
        : MasterServiceTest(Segment::DEFAULT_SEGMENT_SIZE)
    {
    }

    DISALLOW_COPY_AND_ASSIGN(MasterServiceFullSegmentSizeTest);
};

TEST_F(MasterServiceFullSegmentSizeTest, write_maximumObjectSize) {
    char* key = new char[masterConfig.maxObjectKeySize];
    char* buf = new char[masterConfig.maxObjectDataSize + 1];

    // should fail
    EXPECT_THROW(client->write(0, key, masterConfig.maxObjectKeySize,
                               buf, masterConfig.maxObjectDataSize + 1),
                 LogException);

    // should succeed
    EXPECT_NO_THROW(client->write(0, key, masterConfig.maxObjectKeySize,
                                  buf, masterConfig.maxObjectDataSize));

    // overwrite should also succeed
    EXPECT_NO_THROW(client->write(0, key, masterConfig.maxObjectKeySize,
                                  buf, masterConfig.maxObjectDataSize));

    delete[] buf;
    delete[] key;
}

class MasterRecoverTest : public ::testing::Test {
  public:
    Context context;
    Tub<MockCluster> cluster;
    CoordinatorClient* coordinator;
    const uint32_t segmentSize;
    const uint32_t segmentFrames;
    ServerId backup1Id;
    ServerId backup2Id;

    public:
    MasterRecoverTest()
        : context()
        , cluster()
        , coordinator()
        , segmentSize(1 << 16) // Smaller than usual to make tests faster.
        , segmentFrames(2)
        , backup1Id()
        , backup2Id()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        cluster.construct(context);
        coordinator = cluster->getCoordinatorClient();

        ServerConfig config = ServerConfig::forTesting();
        config.localLocator = "mock:host=backup1";
        config.services = {BACKUP_SERVICE, MEMBERSHIP_SERVICE};
        config.segmentSize = segmentSize;
        config.backup.numSegmentFrames = segmentFrames;
        backup1Id = cluster->addServer(config)->serverId;

        config.localLocator = "mock:host=backup2";
        backup2Id = cluster->addServer(config)->serverId;
    }

    ~MasterRecoverTest()
    {
        cluster.destroy();
        EXPECT_EQ(0,
            BackupStorage::Handle::resetAllocatedHandlesCount());
    }

    static bool
    recoverSegmentFilter(string s)
    {
        return (s == "recoverSegment" || s == "recover");
    }

    MasterService*
    createMasterService()
    {
        ServerConfig config = ServerConfig::forTesting();
        config.localLocator = "mock:host=master";
        config.services = {MASTER_SERVICE, MEMBERSHIP_SERVICE};
        config.master.numReplicas = 2;
        return cluster->addServer(config)->master.get();
    }

    void
    appendTablet(ProtoBuf::Tablets& tablets,
                 uint64_t partitionId,
                 uint64_t tableId,
                 uint64_t start, uint64_t end,
                 uint64_t ctimeHeadSegmentId, uint32_t ctimeHeadSegmentOffset)
    {
        ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
        tablet.set_table_id(tableId);
        tablet.set_start_key_hash(start);
        tablet.set_end_key_hash(end);
        tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
        tablet.set_user_data(partitionId);
        tablet.set_ctime_log_head_id(ctimeHeadSegmentId);
        tablet.set_ctime_log_head_offset(ctimeHeadSegmentOffset);
    }

    void
    createTabletList(ProtoBuf::Tablets& tablets)
    {
        appendTablet(tablets, 0, 123, 0, 9, 0, 0);
        appendTablet(tablets, 0, 123, 10, 19, 0, 0);
        appendTablet(tablets, 0, 123, 20, 29, 0, 0);
        appendTablet(tablets, 0, 124, 20, 100, 0, 0);
    }
    DISALLOW_COPY_AND_ASSIGN(MasterRecoverTest);
};

TEST_F(MasterRecoverTest, recover) {
    MasterService* master = createMasterService();

    // Give them a name so that freeSegment doesn't get called on
    // destructor until after the test.
    ServerId serverId(99, 0);
    ServerList serverList(context);
    serverList.add(backup1Id, "mock:host=backup1", {BACKUP_SERVICE,
                                                    MEMBERSHIP_SERVICE}, 100);
    ReplicaManager mgr(context, serverList, serverId, 1, NULL);

#if 0   // Broken now that segments don't deal with replication.
    Segment s1(99, 87, segMem1, segmentSize, &mgr);
    s1.close(NULL);
    char* segMem2 = static_cast<char*>(Memory::xmemalign(HERE, segmentSize,
                                                         segmentSize));
    Segment s2(99, 88, segMem2, segmentSize, &mgr);
    s2.close(NULL);
#endif

    ProtoBuf::Tablets tablets;
    createTabletList(tablets);
    {
        BackupClient(context.transportManager->getSession(
                                                    "mock:host=backup1"))
            .startReadingData(ServerId(99), tablets);
    }
    {
        BackupClient(context.transportManager->getSession(
                                                    "mock:host=backup2"))
            .startReadingData(ServerId(99), tablets);
    }

    vector<MasterService::Replica> replicas {
        { backup1Id.getId(), 87 },
        { backup1Id.getId(), 88 },
        { backup1Id.getId(), 88 },
    };

    MockRandom __(1); // triggers deterministic rand().
    TestLog::Enable _(&recoverSegmentFilter);
    master->recover(ServerId(99, 0), 0, replicas);
    EXPECT_EQ(0U, TestLog::get().find(
        "recover: Recovering master 99, partition 0, 3 replicas "
        "available"));
    EXPECT_NE(string::npos, TestLog::get().find(
        "recoverSegment: Segment 88 replay complete"));
    EXPECT_NE(string::npos, TestLog::get().find(
        "recoverSegment: Segment 87 replay complete"));
}

TEST_F(MasterRecoverTest, failedToRecoverAll) {
    MasterService* master = createMasterService();

    ProtoBuf::Tablets tablets;
    ProtoBuf::ServerList backups;
    vector<MasterService::Replica> replicas {
        { backup1Id.getId(), 87 },
        { backup1Id.getId(), 88 },
    };

    MockRandom __(1); // triggers deterministic rand().
    TestLog::Enable _(&recoverSegmentFilter);
    EXPECT_THROW(master->recover(ServerId(99, 0), 0, replicas),
                 SegmentRecoveryFailedException);
    string log = TestLog::get();
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "recover: Recovering master 99, partition 0, 2 replicas available | "
        "recover: Starting getRecoveryData from server . at mock:host=backup1 "
        "for segment 87 on channel 0 (initial round of RPCs) | "
        "recover: Starting getRecoveryData from server . at mock:host=backup1 "
        "for segment 88 on channel 1 (initial round of RPCs) | "
        "recover: Waiting on recovery data for segment 87 from "
        "server . at mock:host=backup1 | "
        "recover: getRecoveryData failed on server . at mock:host=backup1, "
        "trying next backup; failure was: bad segment id",
        log.substr(0, log.find(" thrown at"))));
}

/*
 * We should add a test for the kill method, but all process ending tests
 * were removed previously for performance reasons.
 */

}  // namespace RAMCloud
