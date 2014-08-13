/* Copyright (c) 2010-2014 Stanford University
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
#include "Cycles.h"
#include "EnumerationIterator.h"
#include "LogIterator.h"
#include "MasterClient.h"
#include "MasterService.h"
#include "Memory.h"
#include "MockCluster.h"
#include "MultiRead.h"
#include "MultiRemove.h"
#include "MultiWrite.h"
#include "ObjectBuffer.h"
#include "RamCloud.h"
#include "ShortMacros.h"
#include "StringUtil.h"
#include "Tablets.pb.h"

namespace RAMCloud {

// This class provides tablet map info to ObjectFinder, so we
// can control which server handles which object.  It maps tables
// 0 and 99 to "mock:host=master".
class MasterServiceRefresher : public ObjectFinder::TableConfigFetcher {
  public:
    MasterServiceRefresher() : refreshCount(1) {}
    void getTableConfig(
            uint64_t tableId,
            std::map<TabletKey, TabletWithLocator>* tableMap,
            std::multimap< std::pair<uint64_t, uint8_t>,
                    ObjectFinder::Indexlet>* tableIndexMap) {
        tableMap->clear();

        Tablet rawEntry({1, 0, ~0, ServerId(),
                            Tablet::NORMAL, Log::Position()});
        TabletWithLocator entry(rawEntry, "mock:host=master");

        TabletKey key {entry.tablet.tableId, entry.tablet.startKeyHash};
        tableMap->insert(std::make_pair(key, entry));

        if (refreshCount > 0) {
            Tablet rawEntry2({99, 0, ~0, ServerId(),
                    Tablet::NORMAL, Log::Position()});
            TabletWithLocator entry2(rawEntry2, "mock:host=master");

            TabletKey key2 {entry2.tablet.tableId, entry2.tablet.startKeyHash};
            tableMap->insert(std::make_pair(key2, entry2));

        }
        refreshCount--;
    }
    // After this many refreshes we stop including table 99 in the
    // map; used to detect that misdirected requests are rejected by
    // the target server.
    int refreshCount;
};

class MasterServiceTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    ServerList serverList;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    ServerConfig backup1Config;
    ServerId backup1Id;

    ServerConfig masterConfig;
    MasterService* service;
    Server* masterServer;

    mutable std::mutex mutex;
    typedef std::unique_lock<std::mutex> Lock;

    // To make tests that don't need big segments faster, set a smaller default
    // segmentSize. Since we can't provide arguments to it in gtest, nor can we
    // apparently template easily on that, we need to subclass this if we want
    // to provide a fixture with a different value.
    explicit MasterServiceTest(uint32_t segmentSize = 256 * 1024)
        : logEnabler()
        , context()
        , serverList(&context)
        , cluster(&context)
        , ramcloud()
        , backup1Config(ServerConfig::forTesting())
        , backup1Id()
        , masterConfig(ServerConfig::forTesting())
        , service()
        , masterServer()
        , mutex()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        backup1Config.localLocator = "mock:host=backup1";
        backup1Config.services = {WireFormat::BACKUP_SERVICE,
                WireFormat::MEMBERSHIP_SERVICE};
        backup1Config.segmentSize = segmentSize;
        backup1Config.backup.numSegmentFrames = 30;
        Server* server = cluster.addServer(backup1Config);
        server->backup->testingSkipCallerIdCheck = true;
        backup1Id = server->serverId;

        masterConfig = ServerConfig::forTesting();
        masterConfig.segmentSize = segmentSize;
        masterConfig.maxObjectDataSize = segmentSize / 4;
        masterConfig.localLocator = "mock:host=master";
        masterConfig.services = {WireFormat::MASTER_SERVICE,
                WireFormat::MEMBERSHIP_SERVICE};
        masterConfig.master.logBytes = segmentSize * 30;
        masterConfig.master.numReplicas = 1;
        masterServer = cluster.addServer(masterConfig);
        service = masterServer->master.get();
        service->objectManager.log.sync();

        ramcloud.construct(&context, "mock:host=coordinator");
        ramcloud->objectFinder.tableConfigFetcher.reset(
                new MasterServiceRefresher);

        service->tabletManager.addTablet(1, 0, ~0UL, TabletManager::NORMAL);
    }

    // Adds integer value 1 to the given table and key.  This function is used
    // in the multi-threading test of increment.
    void
    threadfunIncrementOne(uint64_t tblid, std::string key) {
        RamCloud myRamcloud(&context, "mock:host=coordinator");
        myRamcloud.objectFinder.tableConfigFetcher.reset(
                new MasterServiceRefresher);
        uint16_t keyLen = uint16_t(key.length());
        myRamcloud.incrementInt64(tblid, key.data(), keyLen, 1, NULL, NULL);
    }

    // Build a properly formatted segment containing a single object. This
    // segment may be passed directly to the MasterService::recover() routine.
    uint32_t
    buildRecoverySegment(char *segmentBuf, uint32_t segmentCapacity,
            Key& key, uint64_t version, string objContents,
            Segment::Certificate* outCertificate)
    {
        Segment s;
        uint32_t dataLength = downCast<uint32_t>(objContents.length()) + 1;

        Buffer dataBuffer;
        Object newObject(key, objContents.c_str(), dataLength,
                version, 0, dataBuffer);

        Buffer newObjectBuffer;
        newObject.assembleForLog(newObjectBuffer);
        bool success = s.append(LOG_ENTRY_TYPE_OBJ, newObjectBuffer);
        EXPECT_TRUE(success);
        s.close();

        Buffer buffer;
        s.appendToBuffer(buffer);
        EXPECT_GE(segmentCapacity, buffer.size());
        buffer.copy(0, buffer.size(), segmentBuf);
        s.getAppendedLength(outCertificate);

        return buffer.size();
    }

    // Build a properly formatted segment containing a single tombstone. This
    // segment may be passed directly to the MasterService::recover() routine.
    uint32_t
    buildRecoverySegment(char *segmentBuf, uint64_t segmentCapacity,
            ObjectTombstone& tomb,
            Segment::Certificate* outCertificate)
    {
        Segment s;
        Buffer newTombstoneBuffer;
        tomb.assembleForLog(newTombstoneBuffer);
        bool success = s.append(LOG_ENTRY_TYPE_OBJTOMB, newTombstoneBuffer);
        EXPECT_TRUE(success);
        s.close();

        Buffer buffer;
        s.appendToBuffer(buffer);
        EXPECT_GE(segmentCapacity, buffer.size());
        buffer.copy(0, buffer.size(), segmentBuf);
        s.getAppendedLength(outCertificate);

        return buffer.size();
    }

    // Build a properly formatted segment containing a single safeVersion.
    // This segment may be passed directly to the MasterService::recover()
    //  routine.
    uint32_t
    buildRecoverySegment(char *segmentBuf, uint64_t segmentCapacity,
            ObjectSafeVersion &safeVer,
            Segment::Certificate* outCertificate)
    {
        Segment s;
        Buffer newSafeVerBuffer;
        safeVer.assembleForLog(newSafeVerBuffer);
        bool success = s.append(LOG_ENTRY_TYPE_SAFEVERSION, newSafeVerBuffer);
        EXPECT_TRUE(success);
        s.close();

        Buffer buffer;
        s.appendToBuffer(buffer);
        EXPECT_GE(segmentCapacity, buffer.size());
        buffer.copy(0, buffer.size(), segmentBuf);
        s.getAppendedLength(outCertificate);

        return buffer.size();
    }

    // Write a segment containing nothing but a header to a backup. This is used
    // to test fetching of recovery segments in various tests.
    static void
    writeRecoverableSegment(Context* context, ReplicaManager& mgr,
            ServerId serverId, uint64_t logId, uint64_t segmentId)
    {
        Segment seg;
        SegmentHeader header(logId, segmentId, 1000);
        seg.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header));
        ReplicatedSegment* rs = mgr.allocateHead(segmentId, &seg, NULL);
        rs->sync(seg.getAppendedLength());
    }

    // Write a segment containing a header and a safeVersion to a backup.
    // This is used to test fetching of recovery segments and
    // safeVersion Recovery
    static void
    writeRecoverableSegment(Context* context, ReplicaManager& mgr,
            ServerId serverId, uint64_t logId, uint64_t segmentId,
            uint64_t safeVer)
    {
        Segment seg;
        SegmentHeader header(logId, segmentId, 1000);
        Segment::Certificate certificate;
        seg.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header));
        seg.getAppendedLength(&certificate);

        ObjectSafeVersion objSafeVer(safeVer);
        seg.append(LOG_ENTRY_TYPE_SAFEVERSION,
                   &objSafeVer, sizeof(objSafeVer));
        seg.getAppendedLength(&certificate);

        ReplicatedSegment* rs = mgr.allocateHead(segmentId, &seg, NULL);
        seg.getAppendedLength(&certificate);

        rs->sync(seg.getAppendedLength(&certificate));
    }

    void
    verifyRecoveryObject(Key& key, string contents)
    {
        Buffer value;
        EXPECT_NO_THROW(ramcloud->read(key.getTableId(),
                key.getStringKey(), key.getStringKeyLength(), &value));
        const char *s = reinterpret_cast<const char *>(
                value.getRange(0, value.size()));
        EXPECT_EQ(0, strcmp(s, contents.c_str()));
    }

    int
    verifyCopiedSafeVer(const ObjectSafeVersion *safeVerSrc)
    {
        bool safeVerFound = false;
        int  safeVerScanned = 0;

        Log* log = &service->objectManager.log;
        for (LogIterator it(*log); !it.isDone(); it.next()) {
            // Notice that more than two safeVersion objects exist
            // in the head segment:
            // 1st safeVersion is allocated when the segment is opened.
            // 2nd or lator is the one copied by the recovery.
            if (it.getType() == LOG_ENTRY_TYPE_SAFEVERSION) {
                safeVerScanned++;
                Buffer buf;
                it.setBufferTo(buf);
                ObjectSafeVersion safeVerDest(buf);
                if (safeVerSrc->header.safeVersion
                        == safeVerDest.header.safeVersion) {
                    safeVerFound = true;
                }
            }
        }
        EXPECT_TRUE(safeVerFound);
        return safeVerScanned;
    }

    void
    appendRecoveryPartition(ProtoBuf::RecoveryPartition& recoveryPartition,
            uint64_t partitionId, uint64_t tableId,
            uint64_t start, uint64_t end,
            uint64_t ctimeHeadSegmentId, uint32_t ctimeHeadSegmentOffset)
    {
        ProtoBuf::Tablets::Tablet& tablet(*recoveryPartition.add_tablet());
        tablet.set_table_id(tableId);
        tablet.set_start_key_hash(start);
        tablet.set_end_key_hash(end);
        tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
        tablet.set_user_data(partitionId);
        tablet.set_ctime_log_head_id(ctimeHeadSegmentId);
        tablet.set_ctime_log_head_offset(ctimeHeadSegmentOffset);
    }

    void
    createRecoveryPartition(ProtoBuf::RecoveryPartition& recoveryPartition)
    {
        appendRecoveryPartition(recoveryPartition, 0, 123, 0, 9, 0, 0);
        appendRecoveryPartition(recoveryPartition, 0, 123, 10, 19, 0, 0);
        appendRecoveryPartition(recoveryPartition, 0, 123, 20, 29, 0, 0);
        appendRecoveryPartition(recoveryPartition, 0, 124, 20, 100, 0, 0);
    }

    DISALLOW_COPY_AND_ASSIGN(MasterServiceTest);
};

TEST_F(MasterServiceTest, dispatch_initializationNotFinished) {
    Buffer request, response;
    Service::Rpc rpc(NULL, &request, &response);
    string message("no exception");
    try {
        service->initCalled = false;
        service->dispatch(WireFormat::Opcode::ILLEGAL_RPC_TYPE, &rpc);
    } catch (RetryException& e) {
        message = e.message;
    }
    EXPECT_EQ("master service not yet initialized", message);
}

TEST_F(MasterServiceTest, dispatch_disableCount) {
    Buffer request, response;

    // Attempt #1: service is enabled.
    Service::Rpc rpc(NULL, &request, &response);
    service->dispatch(WireFormat::Opcode::ILLEGAL_RPC_TYPE, &rpc);
    EXPECT_EQ(STATUS_UNIMPLEMENTED_REQUEST, WireFormat::getStatus(&response));

    // Attempt #2: service is disabled.
    MasterService::Disabler disabler(service);
    response.reset();
    service->dispatch(WireFormat::Opcode::ILLEGAL_RPC_TYPE, &rpc);
    EXPECT_EQ(STATUS_RETRY, WireFormat::getStatus(&response));

    // Attempt #3: service is reenabled.
    disabler.reenable();
    response.reset();
    service->dispatch(WireFormat::Opcode::ILLEGAL_RPC_TYPE, &rpc);
    EXPECT_EQ(STATUS_UNIMPLEMENTED_REQUEST, WireFormat::getStatus(&response));
}

TEST_F(MasterServiceTest, Disabler) {
    {
        MasterService::Disabler disabler1(service);
        EXPECT_EQ(1, service->disableCount.load());
        MasterService::Disabler disabler2(service);
        EXPECT_EQ(2, service->disableCount.load());
        disabler2.reenable();
        EXPECT_EQ(1, service->disableCount.load());
        disabler2.reenable();
        EXPECT_EQ(1, service->disableCount.load());
    }
    EXPECT_EQ(0, service->disableCount.load());
}

TEST_F(MasterServiceTest, dropTabletOwnership) {
    TestLog::Enable _("dropTabletOwnership");

    MasterClient::dropTabletOwnership(&context,
            masterServer-> serverId, 2, 1, 1);
    EXPECT_EQ("dropTabletOwnership: Dropped ownership of (or did not own) "
            "tablet [0x1,0x1] in tableId 2", TestLog::get());

    TestLog::reset();

    MasterClient::takeTabletOwnership(&context, masterServer->serverId,
            2, 1, 1);
    MasterClient::dropTabletOwnership(&context, masterServer-> serverId,
            2, 1, 1);
    EXPECT_EQ("dropTabletOwnership: Dropped ownership of (or did not own) "
            "tablet [0x1,0x1] in tableId 2", TestLog::get());
}

TEST_F(MasterServiceTest, dropIndexletOwnership) {
    TestLog::Enable _("dropIndexletOwnership");

    string key1 = "a";
    string key2 = "c";
    MasterClient::dropIndexletOwnership(&context, masterServer->serverId,
            2, 1, reinterpret_cast<const void*>(key1.c_str()),
            (uint16_t)key1.length(),
            reinterpret_cast<const void*>(key2.c_str()),
            (uint16_t)key2.length());
    EXPECT_EQ("dropIndexletOwnership: Dropped ownership of (or did not own) "
            "indexlet in tableId 2, indexId 1",
            TestLog::get());

    TestLog::reset();
    MasterClient::takeIndexletOwnership(&context, masterServer->serverId,
            2, 1, 0, reinterpret_cast<const void*>(key1.c_str()),
            (uint16_t)key1.length(),
            reinterpret_cast<const void*>(key2.c_str()),
            (uint16_t)key2.length());
    MasterClient::dropIndexletOwnership(&context, masterServer->serverId,
            2, 1, reinterpret_cast<const void*>(key1.c_str()),
            (uint16_t)key1.length(),
            reinterpret_cast<const void*>(key2.c_str()),
            (uint16_t)key2.length());
    EXPECT_EQ("dropIndexletOwnership: Dropped ownership of (or did not own) "
            "indexlet in tableId 2, indexId 1", TestLog::get());
}


TEST_F(MasterServiceTest, takeIndexletOwnership) {

    TestLog::Enable _("takeIndexletOwnership");

    string key1 = "a";
    string key2 = "c";
    string key3 = "b";
    MasterClient::takeIndexletOwnership(&context, masterServer->serverId, 2,
            1, 0, reinterpret_cast<const void *>(key1.c_str()),
            (uint16_t)key1.length(),
            reinterpret_cast<const void *>(key2.c_str()),
            (uint16_t)key2.length());
    EXPECT_EQ("takeIndexletOwnership: Took ownership of indexlet "
            "in tableId 2 indexId 1", TestLog::get());

    TestLog::reset();
    MasterClient::takeIndexletOwnership(&context, masterServer->serverId, 2,
            1, 0, reinterpret_cast<const void *>(key1.c_str()),
            (uint16_t)key1.length(),
            reinterpret_cast<const void *>(key2.c_str()),
            (uint16_t)key2.length());
    EXPECT_EQ("takeIndexletOwnership: Took ownership of indexlet "
            "in tableId 2 indexId 1", TestLog::get());
    // The message about already owning the indexlet gets logged by
    // IndexletManager and is tested in IndexletManagerTest.

    TestLog::reset();
    // Test partially overlapping sanity check.
    EXPECT_THROW(MasterClient::takeIndexletOwnership(
            &context, masterServer->serverId, 2,
            1, 0, reinterpret_cast<const void *>(key1.c_str()),
            (uint16_t)key1.length(),
            reinterpret_cast<const void *>(key3.c_str()),
            (uint16_t)key3.length()), ClientException);
    EXPECT_EQ("", TestLog::get());
    // The message logging overlapping range and throwing the exception
    // is done by IndexletManager.
}

TEST_F(MasterServiceTest, enumerate_basics) {
    uint64_t version0, version1;
    ramcloud->write(1, "0", 1, "abcdef", 6, NULL, &version0, false);
    ramcloud->write(1, "1", 1, "ghijkl", 6, NULL, &version1, false);
    Buffer iter, nextIter, finalIter, objects;
    uint64_t nextTabletStartHash;
    EnumerateTableRpc rpc(ramcloud.get(), 1, false, 0, iter, objects);
    nextTabletStartHash = rpc.wait(nextIter);
    EXPECT_EQ(0U, nextTabletStartHash);
    EXPECT_EQ(76U, objects.size());

    // First object.
    EXPECT_EQ(34U, *objects.getOffset<uint32_t>(0));            // size
    Buffer buffer1;
    buffer1.appendExternal(objects.getRange(4, objects.size() - 4),
            objects.size() - 4);
    Object object1(buffer1);
    EXPECT_EQ(1U, object1.getTableId());                        // table ID
    EXPECT_EQ(1U, object1.getKeyLength());                      // key length
    EXPECT_EQ(version0, object1.getVersion());                  // version
    EXPECT_EQ("0", string(reinterpret_cast<const char*>(
            object1.getKey()), 1));                             // key
    EXPECT_EQ("abcdef", string(reinterpret_cast<const char*>(
            object1.getValue()), 6));

    // Second object.
    EXPECT_EQ(34U, *objects.getOffset<uint32_t>(38));           // size
    Buffer buffer2;
    buffer2.appendExternal(objects.getRange(42, objects.size() - 42),
            objects.size() - 42);
    Object object2(buffer2);
    EXPECT_EQ(1U, object2.getTableId());                        // table ID
    EXPECT_EQ(1U, object2.getKeyLength());                      // key length
    EXPECT_EQ(version1, object2.getVersion());                  // version
    EXPECT_EQ("1", string(reinterpret_cast<const char*>(
            object2.getKey()), 1));                             // key
    EXPECT_EQ("ghijkl", string(reinterpret_cast<const char*>(
            object2.getValue()), 6));

    // We don't actually care about the contents of the iterator as
    // long as we get back 0 objects on the second call.
    EnumerateTableRpc rpc2(ramcloud.get(), 1, false, nextTabletStartHash,
            nextIter, objects);
    nextTabletStartHash = rpc2.wait(finalIter);
    EXPECT_EQ(0U, nextTabletStartHash);
    EXPECT_EQ(0U, objects.size());
}

TEST_F(MasterServiceTest, enumerate_tabletNotOnServer) {
    TestLog::Enable _;
    Buffer iter, nextIter, objects;
    EnumerateTableRpc rpc(ramcloud.get(), 99, false, 0, iter, objects);
    EXPECT_THROW(rpc.wait(nextIter), TableDoesntExistException);
    EXPECT_EQ("checkStatus: Server mock:host=master "
            "doesn't store <99, 0x0>; refreshing object map",
            TestLog::get());
}

TEST_F(MasterServiceTest, enumerate_mergeTablet) {
    uint64_t version0, version1;
    ramcloud->write(1, "012345", 6, "abcdef", 6, NULL, &version0, false);
    ramcloud->write(1, "678910", 6, "ghijkl", 6, NULL, &version1, false);

    // (tableId = 1, key = "012345") hashes to 0x7fc19e9dda158f61
    // (tableId = 1, key = "678910") hashes to 0xb1e38b2242e1bbf4

    Buffer iter, nextIter, finalIter, objects;
    uint64_t nextTabletStartHash;

    // We fake a tablet merge by setting up the initial iterator as if
    // the merge already happened. We can be sure that the faked merge
    // worked as expected because the enumeration will not return
    // objects that it thinks would have lived on the pre-merge
    // tablet.
    EnumerationIterator initialIter(iter, 0, 0);
    EnumerationIterator::Frame preMergeConfiguration(
            0x0000000000000000LLU, 0x8fffffffffffffffLLU,
            service->objectManager.objectMap.getNumBuckets(),
            service->objectManager.objectMap.getNumBuckets()*4/5, 0U);
    initialIter.push(preMergeConfiguration);
    initialIter.serialize(iter);
    EnumerateTableRpc rpc(ramcloud.get(), 1, false, 0, iter, objects);
    nextTabletStartHash = rpc.wait(nextIter);
    EXPECT_EQ(0U, nextTabletStartHash);
    EXPECT_EQ(43U, objects.size());

    // Object coresponding to key "678910"
    EXPECT_EQ(39U, *objects.getOffset<uint32_t>(0));            // size
    Buffer buffer1;
    buffer1.appendExternal(objects.getRange(4, objects.size() - 4),
            objects.size() - 4);
    Object object1(buffer1);
    EXPECT_EQ(1U, object1.getTableId());                        // table ID
    EXPECT_EQ(6U, object1.getKeyLength());                      // key length
    EXPECT_EQ(version1, object1.getVersion());                  // version
    EXPECT_EQ("678910", string(reinterpret_cast<const char*>(
            object1.getKey()), 6))         ;                    // key
    EXPECT_EQ("ghijkl", string(reinterpret_cast<const char*>(
            object1.getValue()), 6));

    // The second object is not returned because it would have lived
    // on the part of the pre-merge tablet that we (pretended to have)
    // already iterated.

    // We don't actually care about the contents of the iterator as
    // long as we get back 0 objects on the second call.
    EnumerateTableRpc rpc2(ramcloud.get(), 1, false, 0, nextIter, objects);
    rpc2.wait(finalIter);
    EXPECT_EQ(0U, nextTabletStartHash);
    EXPECT_EQ(0U, objects.size());
}

TEST_F(MasterServiceTest, getHeadOfLog) {
    EXPECT_EQ(Log::Position(2, 88),
            MasterClient::getHeadOfLog(&context, masterServer->serverId));
    ramcloud->write(1, "0", 1, "abcdef", 6);
    EXPECT_EQ(Log::Position(3, 96),
            MasterClient::getHeadOfLog(&context, masterServer->serverId));
}

TEST_F(MasterServiceTest, getServerStatistics) {
    Buffer value;
    uint64_t version;
    int64_t objectValue = 16;

    ramcloud->write(1, "key0", 4, &objectValue, 8, NULL, &version);
    ramcloud->read(1, "key0", 4, &value);
    ramcloud->read(1, "key0", 4, &value);
    ramcloud->read(1, "key0", 4, &value);

    ProtoBuf::ServerStatistics serverStats;
    ramcloud->getServerStatistics("mock:host=master", serverStats);
    EXPECT_TRUE(StringUtil::startsWith(serverStats.ShortDebugString(),
            "tabletentry { table_id: 1 start_key_hash: 0 "
            "end_key_hash: 18446744073709551615 number_read_and_writes: 4 } "
            "spin_lock_stats { locks { name:"));

    MasterClient::splitMasterTablet(&context, masterServer->serverId, 1,
            (~0UL/2));
    ramcloud->getServerStatistics("mock:host=master", serverStats);
    EXPECT_TRUE(StringUtil::startsWith(serverStats.ShortDebugString(),
            "tabletentry { table_id: 1 "
            "start_key_hash: 0 "
            "end_key_hash: 9223372036854775806 } "
            "tabletentry { table_id: 1 start_key_hash: 9223372036854775807 "
            "end_key_hash: 18446744073709551615 } "
            "spin_lock_stats { locks { name:"));
}

TEST_F(MasterServiceTest, increment_basic) {
    Buffer buffer;
    uint64_t version = 0;
    int64_t oldInt64 = 1;
    int64_t newInt64;
    double oldDouble = 1.0;
    double newDouble;

    ramcloud->write(1, "key0", 4, &oldInt64, sizeof(oldInt64), NULL, NULL);
    newInt64 = ramcloud->incrementInt64(1, "key0", 4, 2, NULL, &version);
    EXPECT_EQ(2U, version);
    EXPECT_EQ(3, newInt64);

    ramcloud->read(1, "key0", 4, &buffer);
    buffer.copy(0, sizeof(newInt64), &newInt64);
    EXPECT_EQ(3, newInt64);

    ramcloud->write(1, "key1", 4, &oldDouble, sizeof(oldDouble), NULL, NULL);
    newDouble = ramcloud->incrementDouble(1, "key1", 4, 2.0, NULL, &version);
    EXPECT_EQ(3U, version);
    EXPECT_DOUBLE_EQ(3.0, newDouble);

    buffer.reset();
    ramcloud->read(1, "key1", 4, &buffer);
    buffer.copy(0, sizeof(newDouble), &newDouble);
    EXPECT_EQ(3.0, newDouble);
}

TEST_F(MasterServiceTest, increment_create) {
    uint64_t version = 0;
    int64_t newInt64;
    double newDouble;

    newInt64 = ramcloud->incrementInt64(1, "key0", 4, 1, NULL, &version);
    EXPECT_EQ(1U, version);
    EXPECT_EQ(1, newInt64);

    newDouble = ramcloud->incrementDouble(1, "key1", 4, 1.0, NULL, &version);
    EXPECT_EQ(2U, version);
    EXPECT_DOUBLE_EQ(1.0, newDouble);

    newInt64 = ramcloud->incrementInt64(1, "key2", 4, 0, NULL, &version);
    EXPECT_EQ(3U, version);
    EXPECT_EQ(0, newInt64);

    newDouble = ramcloud->incrementDouble(1, "key2", 4, 0.0, NULL, &version);
    EXPECT_EQ(4U, version);
    EXPECT_DOUBLE_EQ(0.0, newDouble);
}

TEST_F(MasterServiceTest, increment_rejectRules) {
    RejectRules rules;
    int64_t value = 0;
    uint64_t version = 0;

    memset(&rules, 0, sizeof(rules));
    rules.doesntExist = true;
    EXPECT_THROW(ramcloud->incrementInt64(1, "key0", 4, 1, &rules, NULL),
                 ObjectDoesntExistException);

    memset(&rules, 0, sizeof(rules));
    rules.exists = true;
    ramcloud->write(1, "key1", 4, &value, sizeof(value), NULL, NULL);
    EXPECT_THROW(ramcloud->incrementInt64(1, "key1", 4, 1, &rules, NULL),
                 ObjectExistsException);

    ramcloud->write(1, "key2", 4, &value, sizeof(value), NULL, &version);
    ramcloud->incrementInt64(1, "key2", 4, 1, NULL, NULL);
    memset(&rules, 0, sizeof(rules));
    rules.givenVersion = version;
    rules.versionNeGiven = true;
    EXPECT_THROW(ramcloud->incrementInt64(1, "key2", 4, 1, &rules, NULL),
                 WrongVersionException);
}

TEST_F(MasterServiceTest, increment_invalidObject) {
    int32_t intVal = 0;
    float floatVal = 0;

    ramcloud->write(1, "key0", 4, &intVal, sizeof(intVal), NULL, NULL);
    EXPECT_THROW(ramcloud->incrementInt64(1, "key0", 4, 1, NULL, NULL),
                 InvalidObjectException);

    ramcloud->write(1, "key1", 4, &floatVal, sizeof(floatVal), NULL, NULL);
    EXPECT_THROW(ramcloud->incrementDouble(1, "key1", 4, 1.0, NULL, NULL),
                 InvalidObjectException);
}

TEST_F(MasterServiceTest, increment_parallel) {
    // Tests concurrent modification of the incremntee on the master service.
    // Concretely, the master service must verify that the object's version
    // didn't change during the read-increment-write cycle.
    // The condition is triggered by a first thread that sets
    // pauseIncrementObject in the MasterService.  Meanwhile a second thread
    // finishes an increment operation and clears the pause marker.
    TestLog::Enable _("incrementObject");
    std::thread threads[2];
    MasterService::pauseIncrement = 1;
    threads[0] =
      std::thread(&MasterServiceTest::threadfunIncrementOne, this, 1, "key0");

    // Wait for the first thread to reset the pause guard inside
    // incrementObject.
    uint64_t deadline = Cycles::rdtsc() + Cycles::fromSeconds(1.);
    do {
    } while ((MasterService::pauseIncrement == 1) &&
             (Cycles::rdtsc() < deadline));
    EXPECT_EQ(MasterService::pauseIncrement, 0);

    // Increment the same object by another thread without waiting.
    threads[1] =
      std::thread(&MasterServiceTest::threadfunIncrementOne, this, 1, "key0");
    threads[1].join();

    // Let the first thread finish.
    MasterService::continueIncrement = 1;
    threads[0].join();

    EXPECT_EQ("incrementObject: retry after version mismatch", TestLog::get());

    int64_t value;
    Buffer buffer;
    ramcloud->read(1, "key0", 4, &buffer);
    buffer.copy(0, sizeof(value), &value);
    EXPECT_EQ(2, value);
}

TEST_F(MasterServiceTest, migrateTablet_tabletNotOnServer) {
    TestLog::Enable _;
    EXPECT_THROW(ramcloud->migrateTablet(99, 0, -1, ServerId(0, 0)),
            TableDoesntExistException);
    EXPECT_EQ("migrateTablet: Migration request for tablet this master "
            "does not own: tablet [0x0,0xffffffffffffffff] in tableId 99 | "
            "checkStatus: Server mock:host=master doesn't store "
            "<99, 0x0>; refreshing object map",
            TestLog::get());
}

TEST_F(MasterServiceTest, migrateTablet_firstKeyHashTooLow) {
    service->tabletManager.addTablet(99, 27, 873, TabletManager::NORMAL);

    TestLog::Enable _("migrateTablet");

    EXPECT_THROW(ramcloud->migrateTablet(99, 0, 26, ServerId(0, 0)),
            TableDoesntExistException);
    EXPECT_EQ("migrateTablet: Migration request for tablet this master "
            "does not own: tablet [0x0,0x1a] in tableId 99",
            TestLog::get());
}

TEST_F(MasterServiceTest, migrateTablet_lastKeyHashTooHigh) {
    service->tabletManager.addTablet(99, 27, 873, TabletManager::NORMAL);

    TestLog::Enable _("migrateTablet");

    EXPECT_THROW(ramcloud->migrateTablet(99, 874, -1, ServerId(0, 0)),
            TableDoesntExistException);
    EXPECT_EQ("migrateTablet: Migration request for tablet this master "
            "does not own: tablet [0x36a,0xffffffffffffffff] in tableId 99",
            TestLog::get());
}

TEST_F(MasterServiceTest, migrateTablet_migrateToSelf) {
    service->tabletManager.addTablet(99, 27, 873, TabletManager::NORMAL);

    TestLog::Enable _("migrateTablet");

    EXPECT_THROW(ramcloud->migrateTablet(99, 27, 873, masterServer->serverId),
            RequestFormatError);
    EXPECT_EQ("migrateTablet: Migrating to myself doesn't make much sense",
            TestLog::get());
}

TEST_F(MasterServiceTest, migrateTablet_movingData) {
    ramcloud->createTable("migrationTable");
    uint64_t tbl = ramcloud->getTableId("migrationTable");
    ramcloud->write(tbl, "hi", 2, "abcdefg", 7);

    ServerConfig master2Config = masterConfig;
    master2Config.master.numReplicas = 0;
    master2Config.localLocator = "mock:host=master2";
    Server* master2 = cluster.addServer(master2Config);
    Log* master2Log = &master2->master->objectManager.log;
    master2Log->sync();

    Log::Position master2HeadPositionBefore = Log::Position(
            master2Log->head->id, master2Log->head->getAppendedLength());

    // TODO(syang0) RAM-441 without the syncCoordinatorServerList() call  in
    // cluster.addServer(..) above, this crashes since the CoordinatorServerList
    // update is asynchronous and the client calls a migrate before the CSL has
    // been propagated. The recipient servers basically don't know about each
    // other yet and can't perform a migrate.
    TestLog::Enable _("migrateTablet");

    ramcloud->migrateTablet(tbl, 0, -1, master2->serverId);
    EXPECT_EQ("migrateTablet: Migrating tablet [0x0,0xffffffffffffffff] "
            "in tableId 1 to server 3.0 at mock:host=master2 | "
            "migrateTablet: Sending last migration segment | "
            "migrateTablet: Migration succeeded for tablet "
            "[0x0,0xffffffffffffffff] in tableId 1; sent 1 objects and "
            "0 tombstones to server 3.0 at mock:host=master2, 36 bytes in total"
            , TestLog::get());

    // Ensure that the tablet ``creation'' time on the new master is
    // appropriate. It should be greater than the log position before
    // migration, but less than the current log position (since we added
    // data).
    Log::Position master2HeadPositionAfter = Log::Position(
            master2Log->head->id, master2Log->head->getAppendedLength());
    Log::Position ctimeCoord =
            cluster.coordinator->tableManager.getTablet(tbl, 0).ctime;
    EXPECT_GT(ctimeCoord, master2HeadPositionBefore);
    EXPECT_LT(ctimeCoord, master2HeadPositionAfter);
}

TEST_F(MasterServiceTest, multiIncrement_basics) {
    uint64_t tableId1 = ramcloud->createTable("table1");

    MultiIncrementObject request1(tableId1, "0", 1, 1, 0.0);
    MultiIncrementObject request2(tableId1, "1", 1, 0, 1.0);
    MultiIncrementObject* requests[] = {&request1, &request2};

    ramcloud->multiIncrement(requests, 2);
    EXPECT_STREQ("STATUS_OK", statusToSymbol(request1.status));
    EXPECT_EQ(1U, request1.version);
    EXPECT_EQ(1, request1.newValue.asInt64);
    EXPECT_STREQ("STATUS_OK", statusToSymbol(request2.status));
    EXPECT_EQ(2U, request2.version);
    EXPECT_DOUBLE_EQ(1.0, request2.newValue.asDouble);
}

TEST_F(MasterServiceTest, multiIncrement_rejectRules) {
    RejectRules rules;
    memset(&rules, 0, sizeof(rules));
    rules.doesntExist = true;
    MultiIncrementObject request(1, "key0", 4, 1, 0.0, &rules);
    MultiIncrementObject* requests[] = {&request};
    ramcloud->multiIncrement(requests, 1);
    EXPECT_EQ(STATUS_OBJECT_DOESNT_EXIST, request.status);
    EXPECT_EQ(VERSION_NONEXISTENT, request.version);
}

TEST_F(MasterServiceTest, multiIncrement_malformedRequests) {
    // Fabricate a valid-looking RPC, but truncate the increment payload and
    // the key in the buffer.
    WireFormat::MultiOp::Request reqHdr;
    WireFormat::MultiOp::Response respHdr;
    WireFormat::MultiOp::Request::IncrementPart part(
        1, 4, 0, 0.0, RejectRules());  // 4 sets the key length

    reqHdr.common.opcode = downCast<uint16_t>(WireFormat::MULTI_OP);
    reqHdr.common.service = downCast<uint16_t>(WireFormat::MASTER_SERVICE);
    reqHdr.count = 1;
    reqHdr.type = WireFormat::MultiOp::OpType::INCREMENT;

    Buffer requestPayload;
    Buffer replyPayload;
    requestPayload.appendExternal(&reqHdr, sizeof(reqHdr));
    replyPayload.appendExternal(&respHdr, sizeof(respHdr));

    Service::Rpc rpc(NULL, &requestPayload, &replyPayload);

    // Part field is bogus.
    requestPayload.appendExternal(&part, sizeof(part) - 1);
    respHdr.common.status = STATUS_OK;
    service->multiIncrement(&reqHdr, &respHdr, &rpc);
    EXPECT_EQ(STATUS_REQUEST_FORMAT_ERROR, respHdr.common.status);

    // Key is missing.
    requestPayload.truncate(requestPayload.size() - (sizeof32(part) - 1));
    requestPayload.appendExternal(&part, sizeof(part));
    respHdr.common.status = STATUS_OK;
    service->multiIncrement(&reqHdr, &respHdr, &rpc);
    EXPECT_EQ(STATUS_REQUEST_FORMAT_ERROR, respHdr.common.status);

    // Cross-validation: should work with complete 4 byte key.
    requestPayload.appendCopy("key0", 4);
    respHdr.common.status = STATUS_OK;
    service->multiIncrement(&reqHdr, &respHdr, &rpc);
    EXPECT_EQ(STATUS_OK, respHdr.common.status);
}

TEST_F(MasterServiceTest, multiRead_basics) {
    uint64_t tableId1 = ramcloud->createTable("table1");
    ramcloud->write(tableId1, "0", 1, "firstVal", 8);
    ramcloud->write(tableId1, "1", 1, "secondVal", 9);
    Tub<ObjectBuffer> value1, value2;
    MultiReadObject request1(tableId1, "0", 1, &value1);
    MultiReadObject request2(tableId1, "1", 1, &value2);
    MultiReadObject* requests[] = {&request1, &request2};
    ramcloud->multiRead(requests, 2);

    EXPECT_EQ(STATUS_OK, request1.status);
    EXPECT_EQ(1U, request1.version);
    EXPECT_EQ("firstVal", string(reinterpret_cast<const char*>(
            value1.get()->getValue()), 8));
    EXPECT_EQ(STATUS_OK, request2.status);
    EXPECT_EQ(2U, request2.version);
    EXPECT_EQ("secondVal", string(reinterpret_cast<const char*>(
            value2.get()->getValue()), 9));
}

TEST_F(MasterServiceTest, multiRead_bufferSizeExceeded) {
    uint64_t tableId1 = ramcloud->createTable("table1");
    service->maxResponseRpcLen = 78;
    // We want to test such that the first object is returned
    // in the first try and the second object is returned on
    // the second try. For the first object to be returned,
    // the maxResponseRpcLen has to be >= 78
    ramcloud->write(tableId1, "0", 1,
            "chunk1:12 chunk2:12 chunk3:12 chunk4:12 chunk5:12 ", 50);
    ramcloud->write(tableId1, "1", 1,
            "chunk6:12 chunk7:12 chunk8:12 chunk9:12 chunk10:12", 50);
    Tub<ObjectBuffer> value1, value2;
    MultiReadObject object1(tableId1, "0", 1, &value1);
    MultiReadObject object2(tableId1, "1", 1, &value2);
    MultiReadObject* requests[] = {&object1, &object2};
    MultiRead request(ramcloud.get(), requests, 2);

    // The first try will return only the first object.
    EXPECT_FALSE(request.isReady());
    EXPECT_TRUE(value1);
    EXPECT_EQ(STATUS_OK, object1.status);
    EXPECT_FALSE(value2);

    // When we retry, the second object will be returned.
    EXPECT_TRUE(request.isReady());
    EXPECT_TRUE(value1);
    EXPECT_TRUE(value2);
    EXPECT_EQ("chunk1:12 chunk2:12 chunk3:12 chunk4:12 chunk5:12 ",
            string(reinterpret_cast<const char*>(value1.get()->getValue()),
            50));
    EXPECT_EQ("chunk6:12 chunk7:12 chunk8:12 chunk9:12 chunk10:12",
            string(reinterpret_cast<const char*>(value2.get()->getValue()),
            50));
}

TEST_F(MasterServiceTest, multiRead_unknownTable) {
    // Table 99 will be directed to the server, but the server
    // doesn't know about it.
    Tub<ObjectBuffer> value;
    MultiReadObject request(99, "bogus", 5, &value);
    MultiReadObject* requests[] = {&request};
    MultiRead op(ramcloud.get(), requests, 1);

    // Check the status in the response message.
    Transport::SessionRef session =
            ramcloud->clientContext->transportManager->getSession(
            "mock:host=master");
    BindTransport::BindSession* rawSession =
            static_cast<BindTransport::BindSession*>(session.get());
    const Status* status =
            rawSession->lastResponse->getOffset<Status>(
            sizeof(WireFormat::MultiOp::Response));
    EXPECT_TRUE(status != NULL);
    if (status != NULL) {
        EXPECT_EQ(STATUS_UNKNOWN_TABLET, *status);
    }
}

TEST_F(MasterServiceTest, multiRead_noSuchObject) {
    uint64_t tableId1 = ramcloud->createTable("table1");
    Tub<ObjectBuffer> value;
    MultiReadObject request(tableId1, "bogus", 5, &value);
    MultiReadObject* requests[] = {&request};
    ramcloud->multiRead(requests, 1);

    EXPECT_EQ(STATUS_OBJECT_DOESNT_EXIST, request.status);
}

TEST_F(MasterServiceTest, multiRemove_basics) {
    uint64_t tableId1 = ramcloud->createTable("table1");
    ramcloud->write(tableId1, "0", 1, "firstVal", 8);
    ramcloud->write(tableId1, "1", 1, "secondVal", 9);

    MultiRemoveObject request1(tableId1, "0", 1);
    MultiRemoveObject request2(tableId1, "1", 1);
    MultiRemoveObject* requests[] = {&request1, &request2};

    ramcloud->multiRemove(requests, 2);
    EXPECT_EQ(STATUS_OK, request1.status);
    EXPECT_EQ(1U, request1.version);
    EXPECT_EQ(STATUS_OK, request2.status);
    EXPECT_EQ(2U, request2.version);

    // Try to remove the same objects again.
    ramcloud->multiRemove(requests, 2);
    EXPECT_EQ(STATUS_OK, request1.status);
    EXPECT_EQ(0U, request1.version);
    EXPECT_EQ(STATUS_OK, request2.status);
    EXPECT_EQ(0U, request2.version);
}

TEST_F(MasterServiceTest, multiRemove_rejectRules) {
    RejectRules rules;
    memset(&rules, 0, sizeof(rules));
    rules.doesntExist = true;
    MultiRemoveObject request(1, "key0", 4, &rules);
    MultiRemoveObject* requests[] = {&request};
    ramcloud->multiRemove(requests, 1);
    EXPECT_EQ(STATUS_OBJECT_DOESNT_EXIST, request.status);
    EXPECT_EQ(VERSION_NONEXISTENT, request.version);
}

TEST_F(MasterServiceTest, multiRemove_unknownTable) {
    // Table 99 will be directed to the server, but the server
    // doesn't know about it.
    MultiRemoveObject request(99, "bogus", 2);
    MultiRemoveObject* requests[] = {&request};
    MultiRemove op(ramcloud.get(), requests, 1);

    // Check the status in the response message.
    Transport::SessionRef session =
            ramcloud->clientContext->transportManager->getSession(
            "mock:host=master");
    BindTransport::BindSession* rawSession =
            static_cast<BindTransport::BindSession*>(session.get());
    const WireFormat::MultiOp::Response::RemovePart* part =
            rawSession->lastResponse->getOffset<
            WireFormat::MultiOp::Response::RemovePart>(
            sizeof(WireFormat::MultiOp::Response));
    EXPECT_TRUE(part != NULL);
    if (part != NULL) {
        EXPECT_EQ(STATUS_UNKNOWN_TABLET, part->status);
    }
}

TEST_F(MasterServiceTest, multiRemove_malformedRequests) {
    // Fabricate a valid-looking RPC, but truncate the increment payload and
    // the key in the buffer.
    WireFormat::MultiOp::Request reqHdr;
    WireFormat::MultiOp::Response respHdr;
    WireFormat::MultiOp::Request::RemovePart part(1, 4, RejectRules());
    // 4 sets the key length

    reqHdr.common.opcode = downCast<uint16_t>(WireFormat::MULTI_OP);
    reqHdr.common.service = downCast<uint16_t>(WireFormat::MASTER_SERVICE);
    reqHdr.count = 1;
    reqHdr.type = WireFormat::MultiOp::OpType::REMOVE;

    Buffer requestPayload;
    Buffer replyPayload;
    requestPayload.appendExternal(&reqHdr, sizeof(reqHdr));
    replyPayload.appendExternal(&respHdr, sizeof(respHdr));

    Service::Rpc rpc(NULL, &requestPayload, &replyPayload);

    // Part field is bogus.
    requestPayload.appendExternal(&part, sizeof(part) - 1);
    respHdr.common.status = STATUS_OK;
    service->multiRemove(&reqHdr, &respHdr, &rpc);
    EXPECT_EQ(STATUS_REQUEST_FORMAT_ERROR, respHdr.common.status);

    // Key is missing.
    requestPayload.truncate(requestPayload.size() - (sizeof32(part) - 1));
    requestPayload.appendExternal(&part, sizeof(part));
    respHdr.common.status = STATUS_OK;
    service->multiRemove(&reqHdr, &respHdr, &rpc);
    EXPECT_EQ(STATUS_REQUEST_FORMAT_ERROR, respHdr.common.status);

    // Cross-validation: should work with complete 4 byte key.
    requestPayload.appendCopy("key0", 4);
    respHdr.common.status = STATUS_OK;
    service->multiRemove(&reqHdr, &respHdr, &rpc);
    EXPECT_EQ(STATUS_OK, respHdr.common.status);
}

TEST_F(MasterServiceTest, multiWrite_basics) {
    uint64_t tableId1 = ramcloud->createTable("table1");
    ramcloud->write(tableId1, "1", 1, "originalVal", 12);
    MultiWriteObject request1(tableId1, "0", 1, "firstVal", 8);
    MultiWriteObject request2(tableId1, "1", 1, "secondVal", 9);
    MultiWriteObject* requests[] = {&request1, &request2};
    ramcloud->multiWrite(requests, 2);

    EXPECT_EQ(STATUS_OK, request1.status);
    EXPECT_EQ(2U, request1.version);
    EXPECT_EQ(STATUS_OK, request2.status);
    EXPECT_EQ(2U, request2.version);

    ObjectBuffer value;
    uint64_t version;

    ramcloud->readKeysAndValue(tableId1, "0", 1, &value, NULL, &version);
    EXPECT_EQ("firstVal", string(reinterpret_cast<const char*>(
            value.getValue()), 8));
    EXPECT_EQ(2U, request1.version);
    ramcloud->readKeysAndValue(tableId1, "1", 1, &value, NULL, &version);
    EXPECT_EQ("secondVal", string(reinterpret_cast<const char*>(
            value.getValue()), 9));
    EXPECT_EQ(2U, request2.version);
}

TEST_F(MasterServiceTest, multiWrite_rejectRules) {
    RejectRules rules;
    memset(&rules, 0, sizeof(rules));
    rules.doesntExist = true;
    MultiWriteObject request(1, "key0", 4, "item0", 5, &rules);
    MultiWriteObject* requests[] = {&request};
    ramcloud->multiWrite(requests, 1);
    EXPECT_EQ(STATUS_OBJECT_DOESNT_EXIST, request.status);
    EXPECT_EQ(VERSION_NONEXISTENT, request.version);
}

TEST_F(MasterServiceTest, multiWrite_unknownTable) {
    // Table 99 will be directed to the server, but the server
    // doesn't know about it.
    MultiWriteObject request(99, "bogus", 5, "hi", 2);
    MultiWriteObject* requests[] = {&request};
    MultiWrite op(ramcloud.get(), requests, 1);

    // Check the status in the response message.
    Transport::SessionRef session =
            ramcloud->clientContext->transportManager->getSession(
            "mock:host=master");
    BindTransport::BindSession* rawSession =
            static_cast<BindTransport::BindSession*>(session.get());
    const WireFormat::MultiOp::Response::WritePart* part =
            rawSession->lastResponse->getOffset<
            WireFormat::MultiOp::Response::WritePart>(
            sizeof(WireFormat::MultiOp::Response));
    EXPECT_TRUE(part != NULL);
    if (part != NULL) {
        EXPECT_STREQ("STATUS_UNKNOWN_TABLET", statusToSymbol(part->status));
    }
}

TEST_F(MasterServiceTest, multiWrite_malformedRequests) {
    // Fabricate a valid-looking RPC, but make the key and value length
    // fields not match what's in the buffer.
    WireFormat::MultiOp::Request reqHdr;
    WireFormat::MultiOp::Response respHdr;
    WireFormat::MultiOp::Request::WritePart part(0, 23, RejectRules());
    // 23 includes a key of length 10, a value of length 10, 1 byte for
    // numKeys and 2 bytes for keyOffset

    reqHdr.common.opcode = downCast<uint16_t>(WireFormat::MULTI_OP);
    reqHdr.common.service = downCast<uint16_t>(WireFormat::MASTER_SERVICE);
    reqHdr.count = 1;
    reqHdr.type = WireFormat::MultiOp::OpType::WRITE;

    Buffer requestPayload;
    Buffer replyPayload;
    requestPayload.appendExternal(&reqHdr, sizeof(reqHdr));
    replyPayload.appendExternal(&respHdr, sizeof(respHdr));

    Service::Rpc rpc(NULL, &requestPayload, &replyPayload);

    // part field is bogus
    requestPayload.appendExternal(&part, sizeof(part) - 1);
    respHdr.common.status = STATUS_OK;
    service->multiWrite(&reqHdr, &respHdr, &rpc);
    EXPECT_EQ(STATUS_REQUEST_FORMAT_ERROR, respHdr.common.status);

    requestPayload.truncate(requestPayload.size() - (sizeof32(part) - 1));
    requestPayload.appendExternal(&part, sizeof(part));

    // Malformed requests with both the key and the value length fields
    // as bogus and requests with only the value length field bogus will
    // not be caught. It will seg fault. So, it is up to the client
    // to make sure the requests are formatted well. See comment in
    // the multiWrite handler in MasterService for reasons

    // sanity check: should work with 10 bytes of key and 10 of value
    Key key(0, "tenchars!!", 10);
    Object::appendKeysAndValueToBuffer(key, "tenmorechars", 10,
                                       &requestPayload);
    respHdr.common.status = STATUS_OK;
    service->multiWrite(&reqHdr, &respHdr, &rpc);
    EXPECT_EQ(STATUS_OK, respHdr.common.status);
}

TEST_F(MasterServiceTest, prepForMigration) {
    service->tabletManager.addTablet(5, 27, 873, TabletManager::NORMAL);

    TestLog::Enable _("prepForMigration");

    // Overlap
    EXPECT_THROW(MasterClient::prepForMigration(&context,
            masterServer->serverId, 5, 27, 873, 0, 0),
            ObjectExistsException);
    EXPECT_EQ("prepForMigration: Already have tablet [0x1b,0x369] "
            "in tableId 5, cannot add [0x1b,0x369]", TestLog::get());
    EXPECT_THROW(MasterClient::prepForMigration(&context,
            masterServer->serverId, 5, 0, 27, 0, 0),
            ObjectExistsException);
    EXPECT_THROW(MasterClient::prepForMigration(&context,
            masterServer->serverId, 5, 873, 82743, 0, 0),
            ObjectExistsException);

    TestLog::reset();
    MasterClient::prepForMigration(&context,
            masterServer->serverId, 5, 1000, 2000, 0, 0);
    TabletManager::Tablet tablet;
    EXPECT_TRUE(service->tabletManager.getTablet(5, 1000, 2000, &tablet));
    EXPECT_EQ(5U, tablet.tableId);
    EXPECT_EQ(1000U, tablet.startKeyHash);
    EXPECT_EQ(2000U, tablet.endKeyHash);
    EXPECT_EQ(TabletManager::RECOVERING, tablet.state);
    EXPECT_EQ("prepForMigration: Ready to receive tablet [0x3e8,0x7d0] "
            "in tableId 5 from \"??\"", TestLog::get());
}

TEST_F(MasterServiceTest, indexedRead) {
    // Most of the functionality for indexedRead is in ObjectManager,
    // so we do extensive unit testing there.
    // Let's just ensure that the strings are parsed fine in the MasterService
    // class.
    // This may be overkill, and we may not acually need this test here.
    uint64_t tableId = 1;
    uint8_t numKeys = 2;

    KeyInfo keyList0[2];
    keyList0[0].keyLength = 8;
    keyList0[0].key = "obj0key0";
    keyList0[1].keyLength = 8;
    keyList0[1].key = "obj0key1";

    ramcloud->write(tableId, numKeys, keyList0, "obj0value",
            NULL, NULL, false);

    Buffer pKHashes;
    // Key::getHash(tableId, "obj0key0", 8) gives 4921604586378860710,
    uint64_t hashVal0 = 4921604586378860710;
    pKHashes.appendExternal(&hashVal0, 8);

    // indexedRead such that both objects are read.
    Buffer responseBuffer;
    uint32_t numObjectsResponse;
    uint32_t numHashesResponse = ramcloud->indexedRead(
            tableId, 1, &pKHashes, 1, "obj0key1", 8, "obj1key1", 8,
            &responseBuffer, &numObjectsResponse);

    // numHashes and numObjects
    EXPECT_EQ(1U, numHashesResponse);
    EXPECT_EQ(1U, numObjectsResponse);
    uint32_t respOffset = sizeof32(WireFormat::IndexedRead::Response);

    // version of object0
    EXPECT_EQ(1U, *responseBuffer.getOffset<uint64_t>(respOffset));
    respOffset += 8;
    // value of object0
    uint32_t obj1Length = *responseBuffer.getOffset<uint32_t>(respOffset);
    respOffset += 4;
    Object o1(tableId, 1, 0, responseBuffer, respOffset, obj1Length);
    respOffset += obj1Length;
    EXPECT_EQ("obj0value", string(reinterpret_cast<const char*>(o1.getValue()),
            o1.getValueLength()));
}

TEST_F(MasterServiceTest, read_basics) {
    ramcloud->write(1, "0", 1, "abcdef", 6);
    Buffer value;
    uint64_t version;
    ramcloud->read(1, "0", 1, &value, NULL, &version);
    EXPECT_EQ(1U, version);
    EXPECT_EQ("abcdef", TestUtil::toString(&value));
    EXPECT_EQ(6U, value.size());
}

TEST_F(MasterServiceTest, readKeysAndValue_basics) {
    uint64_t tableId1 = 1;
    ObjectBuffer keysAndValue;
    uint8_t numKeys = 3;
    KeyInfo keyList[3];
    keyList[0].keyLength = 2;
    keyList[0].key = "ha";
    keyList[1].keyLength = 2;
    keyList[1].key = "hi";
    keyList[2].keyLength = 2;
    keyList[2].key = "ho";

    ramcloud->write(tableId1, numKeys, keyList, "data value",
            NULL, NULL, false);
    ramcloud->readKeysAndValue(tableId1, "ha", 2, &keysAndValue);
    EXPECT_EQ("data value", string(reinterpret_cast<const char*>(
            keysAndValue.getValue()), 10));

    EXPECT_EQ("ha", string(reinterpret_cast<const char *>(
            keysAndValue.getKey(0)), 2));
    EXPECT_EQ(2U, keysAndValue.getKeyLength(0));
    EXPECT_EQ("hi", string(reinterpret_cast<const char *>(
            keysAndValue.getKey(1)), 2));
    EXPECT_EQ(2U, keysAndValue.getKeyLength(1));
    EXPECT_EQ("ho", string(reinterpret_cast<const char *>(
            keysAndValue.getKey(2)), 2));
    EXPECT_EQ(2U, keysAndValue.getKeyLength(2));
}

TEST_F(MasterServiceTest, read_tableNotOnServer) {
    TestLog::Enable _;
    Buffer value;
    EXPECT_THROW(ramcloud->read(99, "0", 1, &value),
            TableDoesntExistException);
    EXPECT_EQ("checkStatus: Server mock:host=master doesn't store "
            "<99, 0xbaf01774b348c879>; refreshing object map",
            TestLog::get());
}

TEST_F(MasterServiceTest, read_noSuchObject) {
    Buffer value;
    EXPECT_THROW(ramcloud->read(1, "5", 1, &value), ObjectDoesntExistException);
}

TEST_F(MasterServiceTest, read_rejectRules) {
    ramcloud->write(1, "0", 1, "abcdef", 6);

    Buffer value;
    RejectRules rules;
    memset(&rules, 0, sizeof(rules));
    rules.versionNeGiven = true;
    rules.givenVersion = 2;
    uint64_t version;
    EXPECT_THROW(ramcloud->read(1, "0", 1, &value, &rules, &version),
            WrongVersionException);
    EXPECT_EQ(1U, version);
}

TEST_F(MasterServiceTest, receiveMigrationData) {
    Segment s;

    MasterClient::prepForMigration(&context, masterServer->serverId,
            5, 1, -1UL, 0, 0);

    TestLog::Enable _("receiveMigrationData");

    EXPECT_THROW(MasterClient::receiveMigrationData(&context,
            masterServer->serverId, 6, 0, &s),
            UnknownTabletException);
    EXPECT_EQ("receiveMigrationData: Receiving 0 bytes of migration data "
            "for tablet [0x0,??] in tableId 6 | "
            "receiveMigrationData: migration data received for unknown "
            "tablet [0x0,??] in tableId 6", TestLog::get());
    EXPECT_THROW(MasterClient::receiveMigrationData(&context,
            masterServer->serverId, 5, 0, &s),
            UnknownTabletException);

    TestLog::reset();
    EXPECT_THROW(MasterClient::receiveMigrationData(&context,
            masterServer->serverId, 1, 0, &s),
            InternalError);
    EXPECT_EQ("receiveMigrationData: Receiving 0 bytes of migration data for "
            "tablet [0x0,??] in tableId 1 | receiveMigrationData: migration "
            "data received for tablet not in the RECOVERING state (state = 0)!",
            TestLog::get());

    Key key(5, "wee!", 4);
    Buffer dataBuffer;
    Object o(key, "watch out for the migrant object", 32, 0, 0, dataBuffer);

    Buffer buffer;
    o.assembleForLog(buffer);

    s.append(LOG_ENTRY_TYPE_OBJ, buffer);
    s.close();

    MasterClient::receiveMigrationData(&context, masterServer->serverId,
            5, 1, &s);

    ObjectBuffer logBuffer;
    Status status = service->objectManager.readObject(key, &logBuffer, 0, 0);
    EXPECT_NE(STATUS_OK, status);
    // Need to mark the tablet as NORMAL before we can read from it.
    service->tabletManager.changeState(5, 1, -1UL, TabletManager::RECOVERING,
            TabletManager::NORMAL);
    status = service->objectManager.readObject(key, &logBuffer, 0, 0);
    EXPECT_EQ(STATUS_OK, status);
    EXPECT_EQ(string(reinterpret_cast<const char*>(logBuffer.getValue()), 32),
            "watch out for the migrant object");
}

static bool
antiGetEntryFilter(string s)
{
    return s != "getEntry";
}

TEST_F(MasterServiceTest, remove_basics) {
    ramcloud->write(1, "key0", 4, "item0", 5);

    TestLog::Enable _(antiGetEntryFilter);
    Key key(1, "key0", 4);
    HashTable::Candidates c;
    service->objectManager.objectMap.lookup(key.getHash(), c);
    uint64_t ref = c.getReference();
    uint64_t version;
    ramcloud->remove(1, "key0", 4, NULL, &version);
    EXPECT_EQ(1U, version);
    EXPECT_EQ(format("free: free on reference %lu | "
            "sync: syncing segment 1 to offset 156 | "
            "schedule: scheduled | "
            "performWrite: Sending write to backup 1.0 | "
            "schedule: scheduled | "
            "performWrite: Write RPC finished for replica slot 0 | "
            "sync: log synced", ref),
            TestLog::get());

    Buffer value;
    EXPECT_THROW(ramcloud->read(1, "key0", 4, &value),
            ObjectDoesntExistException);
}

TEST_F(MasterServiceTest, remove_tableNotOnServer) {
    TestLog::Enable _;
    EXPECT_THROW(ramcloud->remove(99, "key0", 4), TableDoesntExistException);
    EXPECT_EQ("checkStatus: Server mock:host=master doesn't store "
            "<99, 0xb3a4e310e6f49dd8>; refreshing object map",
            TestLog::get());
}

TEST_F(MasterServiceTest, remove_rejectRules) {
    ramcloud->write(1, "key0", 4, "item0", 5);

    RejectRules rules;
    memset(&rules, 0, sizeof(rules));
    rules.versionNeGiven = true;
    rules.givenVersion = 2;
    uint64_t version;
    EXPECT_THROW(ramcloud->remove(1, "key0", 4, &rules, &version),
            WrongVersionException);
    EXPECT_EQ(1U, version);
}

TEST_F(MasterServiceTest, remove_objectAlreadyDeletedRejectRules) {
    RejectRules rules;
    memset(&rules, 0, sizeof(rules));
    rules.doesntExist = true;
    uint64_t version;
    EXPECT_THROW(ramcloud->remove(1, "key0", 4, &rules, &version),
            ObjectDoesntExistException);
    EXPECT_EQ(VERSION_NONEXISTENT, version);
}

TEST_F(MasterServiceTest, remove_objectAlreadyDeleted) {
    uint64_t version;
    ramcloud->remove(1, "key1", 4, NULL, &version);
    EXPECT_EQ(VERSION_NONEXISTENT, version);

    ramcloud->write(1, "key0", 4, "item0", 5);
    ramcloud->remove(1, "key0", 4);
    ramcloud->remove(1, "key0", 4, NULL, &version);
    EXPECT_EQ(VERSION_NONEXISTENT, version);
}

TEST_F(MasterServiceTest, splitMasterTablet) {

    MasterClient::splitMasterTablet(&context, masterServer->serverId, 1,
            (~0UL/2));
    EXPECT_EQ(
            "{ tableId: 1 startKeyHash: 0 "
            "endKeyHash: 9223372036854775806 state: 0 reads: 0 writes: 0 }\n"
            "{ tableId: 1 startKeyHash: 9223372036854775807 "
            "endKeyHash: 18446744073709551615 state: 0 reads: 0 writes: 0 }",
            service->tabletManager.toString());
}

TEST_F(MasterServiceTest, takeTabletOwnership_syncLog) {
    TestLog::Enable _("takeTabletOwnership", "sync", NULL);

    service->logEverSynced = false;
    MasterClient::takeTabletOwnership(&context, masterServer->serverId,
            2, 2, 3);
    EXPECT_TRUE(service->logEverSynced);
    EXPECT_EQ("sync: sync not needed: already fully replicated | "
            "takeTabletOwnership: Took ownership of new tablet "
            "[0x2,0x3] in tableId 2", TestLog::get());

    TestLog::reset();
    MasterClient::takeTabletOwnership(&context, masterServer->serverId,
            2, 4, 5);
    EXPECT_TRUE(service->logEverSynced);
    EXPECT_EQ("takeTabletOwnership: Took ownership of new tablet "
            "[0x4,0x5] in tableId 2", TestLog::get());
}

TEST_F(MasterServiceTest, takeTabletOwnership_newTablet) {
    TestLog::Enable _("takeTabletOwnership");

    // Start empty.
    service->tabletManager.deleteTablet(1, 0, ~0UL);
    EXPECT_EQ("", service->tabletManager.toString());

    { // set t1 and t2 directly
        service->tabletManager.addTablet(1, 0, 1, TabletManager::NORMAL);
        service->tabletManager.addTablet(2, 0, 1, TabletManager::NORMAL);

        EXPECT_EQ(
            "{ tableId: 1 startKeyHash: 0 "
            "endKeyHash: 1 state: 0 reads: 0 writes: 0 }\n"
            "{ tableId: 2 startKeyHash: 0 "
            "endKeyHash: 1 state: 0 reads: 0 writes: 0 }",
            service->tabletManager.toString());
    }

    { // set t2, t2b, and t3 through client
        MasterClient::takeTabletOwnership(&context, masterServer->serverId,
                2, 2, 3);
        MasterClient::takeTabletOwnership(&context, masterServer->serverId,
                2, 4, 5);
        MasterClient::takeTabletOwnership(&context, masterServer->serverId,
                3, 0, 1);

        EXPECT_EQ(
                "{ tableId: 1 startKeyHash: 0 "
                "endKeyHash: 1 state: 0 reads: 0 writes: 0 }\n"
                "{ tableId: 2 startKeyHash: 0 "
                "endKeyHash: 1 state: 0 reads: 0 writes: 0 }\n"
                "{ tableId: 2 startKeyHash: 4 "
                "endKeyHash: 5 state: 0 reads: 0 writes: 0 }\n"
                "{ tableId: 2 startKeyHash: 2 "
                "endKeyHash: 3 state: 0 reads: 0 writes: 0 }\n"
                "{ tableId: 3 startKeyHash: 0 "
                "endKeyHash: 1 state: 0 reads: 0 writes: 0 }",
                service->tabletManager.toString());

        EXPECT_EQ(
                "takeTabletOwnership: Took ownership of new tablet [0x2,0x3] "
                "in tableId 2 | "
                "takeTabletOwnership: Took ownership of new tablet [0x4,0x5] "
                "in tableId 2 | "
                "takeTabletOwnership: Took ownership of new tablet [0x0,0x1] "
                "in tableId 3",
                TestLog::get());
    }

    TestLog::reset();

    // Test assigning ownership of an already-owned tablet. This isn't a
    // failure case, but we log the strange occurrence.
    {
        MasterClient::takeTabletOwnership(&context, masterServer->serverId,
                2, 2, 3);
        EXPECT_EQ("takeTabletOwnership: Told to take ownership of tablet "
                "[0x2,0x3] in tableId 2, but already own [0x2,0x3]. Returning "
                "success.", TestLog::get());
    }

    TestLog::reset();

    // Test partially overlapping sanity check. The coordinator should
    // know better, but I'd rather be safe than sorry...
    {
        EXPECT_THROW(MasterClient::takeTabletOwnership(&context,
                masterServer->serverId, 2, 2, 2), ClientException);
        EXPECT_EQ("takeTabletOwnership: Could not take ownership of tablet "
                "[0x2,0x2] in tableId 2: overlaps with one or more different "
                "ranges.", TestLog::get());
    }
}

TEST_F(MasterServiceTest, takeTabletOwnership_migratingTablet) {
    TestLog::Enable _("takeTabletOwnership");

    // Fake up a tablet in migration.
    service->tabletManager.addTablet(2, 0, 5, TabletManager::RECOVERING);

    MasterClient::takeTabletOwnership(&context, masterServer->serverId,
            2, 0, 5);

    EXPECT_EQ("takeTabletOwnership: Took ownership of existing tablet "
            "[0x0,0x5] in tableId 2 in RECOVERING state", TestLog::get());
}

TEST_F(MasterServiceTest, write_basics) {
    ObjectBuffer value;
    uint64_t version;

    TestLog::Enable _;
    ramcloud->write(1, "key0", 4, "item0", 5, NULL, &version);
    EXPECT_EQ(1U, version);
    EXPECT_EQ("writeObject: object: 36 bytes, version 1 | "
            "sync: syncing segment 1 to offset 118 | "
            "schedule: scheduled | "
            "performWrite: Sending write to backup 1.0 | "
            "schedule: scheduled | "
            "performWrite: Write RPC finished for replica slot 0 | "
            "sync: log synced",
            TestLog::get());
    ramcloud->readKeysAndValue(1, "key0", 4, &value);
    EXPECT_EQ("item0", string(reinterpret_cast<const char*>(
            value.getValue()), 5));
    EXPECT_EQ(1U, version);

    ramcloud->write(1, "key0", 4, "item0-v2", 8, NULL, &version);
    EXPECT_EQ(2U, version);
    ramcloud->readKeysAndValue(1, "key0", 4, &value);
    EXPECT_EQ("item0-v2", string(reinterpret_cast<const char*>(
            value.getValue()), 8));
    EXPECT_EQ(2U, version);

    ramcloud->write(1, "key0", 4, "item0-v3", 8, NULL, &version);
    EXPECT_EQ(3U, version);
    ramcloud->readKeysAndValue(1, "key0", 4, &value);
    EXPECT_EQ("item0-v3", string(reinterpret_cast<const char*>(
            value.getValue()), 8));
    EXPECT_EQ(3U, version);
}

TEST_F(MasterServiceTest, write_safeVersionNumberUpdate) {
    ObjectBuffer value;
    uint64_t version;

    SegmentManager* segmentManager = &service->objectManager.segmentManager;
    segmentManager->safeVersion = 1UL; // reset safeVersion
    // initial data to original table
    //         Table, Key, KeyLen, Data, Len, rejectRule, Version
    ramcloud->write(1, "k0", 2, "value0", 6, NULL, &version);
    EXPECT_EQ(1U, version); // safeVersion++ is given
    ramcloud->readKeysAndValue(1,  "k0", 2, &value);
    EXPECT_EQ("value0", string(reinterpret_cast<const char*>(
            value.getValue()), 6));
    EXPECT_EQ(1U, version); // current object version returned
    EXPECT_EQ(2U, segmentManager->safeVersion); // incremented

    // original key to original table
    ramcloud->write(1, "k0", 2, "value1", 6, NULL, &version);
    EXPECT_EQ(2U, version); // object version incremented
    ramcloud->readKeysAndValue(1,  "k0", 2, &value);
    EXPECT_EQ("value1", string(reinterpret_cast<const char*>(
            value.getValue()), 6));
    EXPECT_EQ(2U, version); // current object version returned
    EXPECT_EQ(2U, segmentManager->safeVersion); // unchanged

    segmentManager->safeVersion = 29UL; // increase safeVersion
    // different key to original table
    ramcloud->write(1, "k1", 2, "value3", 6, NULL, &version);
    EXPECT_EQ(29U, version);  // safeVersion++ is given
    ramcloud->readKeysAndValue(1, "k1", 2, &value);
    EXPECT_EQ("value3", string(reinterpret_cast<const char*>(
            value.getValue()), 6));
    EXPECT_EQ(29U, version);  // current object version returned
    EXPECT_EQ(30U, segmentManager->safeVersion); // incremented

    // original key to original table
    ramcloud->write(1, "k0", 2, "value4", 6, NULL, &version);
    EXPECT_EQ(3U, version); // object version incremented
    ramcloud->readKeysAndValue(1,  "k0", 2, &value);
    EXPECT_EQ("value4", string(reinterpret_cast<const char*>(
            value.getValue()), 6));
    EXPECT_EQ(3U, version); // current object version returned
    EXPECT_EQ(30U, segmentManager->safeVersion); // unchanged
}

TEST_F(MasterServiceTest, write_rejectRules) {
    RejectRules rules;
    memset(&rules, 0, sizeof(rules));
    rules.doesntExist = true;
    uint64_t version;
    EXPECT_THROW(ramcloud->write(1, "key0", 4, "item0", 5, &rules, &version),
            ObjectDoesntExistException);
    EXPECT_EQ(VERSION_NONEXISTENT, version);
}

TEST_F(MasterServiceTest, requestInsertIndexEntries_noIndexEntries) {
    TestLog::Enable _;
    uint64_t tableId = 1;
    Key key(tableId, "key0", 4);
    Buffer objBuffer;
    Object obj(key, "value", 5, 1, 0, objBuffer);
    service->requestInsertIndexEntries(obj);
    EXPECT_EQ("", TestLog::get());
}

TEST_F(MasterServiceTest, requestInsertIndexEntries_basics) {
    TestLog::Enable _;

    uint64_t tableId = 1;
    uint8_t numKeys = 3;

    KeyInfo keyList[3];
    keyList[0].keyLength = 4;
    keyList[0].key = "key0";
    keyList[1].keyLength = 4;
    keyList[1].key = "key1";
    keyList[2].keyLength = 4;
    keyList[2].key = "key2";

    Buffer keysAndValBuffer;
    Object::appendKeysAndValueToBuffer(tableId, numKeys, keyList,
                                       "value", 5, &keysAndValBuffer);
    Object obj(tableId, 1, 0, keysAndValBuffer);
    Key key(tableId, keyList[0].key, keyList[0].keyLength);

    service->requestInsertIndexEntries(obj);
    EXPECT_EQ(format("requestInsertIndexEntries: "
            "Inserting index entry for tableId 1, keyIndex 1, "
            "key key1, primaryKeyHash %lu | "
            "requestInsertIndexEntries: "
            "Inserting index entry for tableId 1, keyIndex 2, "
            "key key2, primaryKeyHash %lu" ,
            key.getHash(), key.getHash()),
            TestLog::get());
}

TEST_F(MasterServiceTest, requestRemoveIndexEntries_noIndexEntries) {
    TestLog::Enable _;

    uint64_t tableId = 1;
    Key key(tableId, "key0", 4);
    Buffer tempBuffer;
    Object obj(key, "value", 5, 0, 0, tempBuffer);
    Buffer objBuffer;
    obj.assembleForLog(objBuffer);

    service->requestRemoveIndexEntries(objBuffer);
    EXPECT_EQ("", TestLog::get());
}

TEST_F(MasterServiceTest, requestRemoveIndexEntries_basics) {
    TestLog::Enable _;

    uint64_t tableId = 1;
    uint8_t numKeys = 3;

    KeyInfo keyList[3];
    keyList[0].keyLength = 4;
    keyList[0].key = "key0";
    keyList[1].keyLength = 4;
    keyList[1].key = "key1";
    keyList[2].keyLength = 4;
    keyList[2].key = "key2";

    Buffer keysAndValBuffer;
    Object::appendKeysAndValueToBuffer(tableId, numKeys, keyList,
            "value", 5, &keysAndValBuffer);
    Object obj(tableId, 0, 0, keysAndValBuffer);
    Buffer objBuffer;
    obj.assembleForLog(objBuffer);
    Key key(tableId, keyList[0].key, keyList[0].keyLength);
    service->requestRemoveIndexEntries(objBuffer);

    EXPECT_EQ(format("requestRemoveIndexEntries: "
            "Removing index entry for tableId 1, keyIndex 1, "
            "key key1, primaryKeyHash %lu | "
            "requestRemoveIndexEntries: "
            "Removing index entry for tableId 1, keyIndex 2, "
            "key key2, primaryKeyHash %lu" ,
            key.getHash(), key.getHash()),
            TestLog::get());
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
        ObjectBuffer value;
        uint64_t version;

        ramcloud->write(1, key, keyLength, writeVal.c_str(),
                downCast<uint16_t>(writeVal.length()),
                NULL, &version);
        ramcloud->readKeysAndValue(1, key, keyLength, &value);

        EXPECT_EQ(writeVal.c_str(), string(reinterpret_cast<const char*>(
                value.getValue()), downCast<uint16_t>(writeVal.length())));
    }
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
    char* buf = new char[masterConfig.maxObjectDataSize];

    // should succeed
    EXPECT_NO_THROW(ramcloud->write(1, key, masterConfig.maxObjectKeySize,
            buf, masterConfig.maxObjectDataSize));

    // overwrite should also succeed
    EXPECT_NO_THROW(ramcloud->write(1, key, masterConfig.maxObjectKeySize,
            buf, masterConfig.maxObjectDataSize));

    delete[] buf;
    delete[] key;
}

///////////////////////////////////////////////////////////////////////////////
/////Recovery related tests. This should eventually move into its own file.////
///////////////////////////////////////////////////////////////////////////////


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
            3, replicas), SegmentRecoveryFailedException);
}

TEST_F(MasterServiceTest, recover_basics) {
    cluster.coordinator->recoveryManager.start();
    ServerId serverId(123, 0);
    ReplicaManager mgr(&context, &serverId, 1, false, false);

    // Create a segment with objectSafeVersion 23
    writeRecoverableSegment(&context, mgr, serverId, serverId.getId(), 87, 23U);

    ProtoBuf::RecoveryPartition recoveryPartition;
    createRecoveryPartition(recoveryPartition);
    auto result = BackupClient::startReadingData(&context, backup1Id,
            10lu, serverId);
    BackupClient::StartPartitioningReplicas(&context, backup1Id,
            10lu, serverId, &recoveryPartition);
    ASSERT_EQ(1lu, result.replicas.size());
    ASSERT_EQ(87lu, result.replicas.at(0).segmentId);

    SegmentManager* segmentManager = &service->objectManager.segmentManager;
    segmentManager->safeVersion = 1U; // reset safeVersion
    EXPECT_EQ(1U, segmentManager->safeVersion); // check safeVersion

    ProtoBuf::ServerList backups;
    WireFormat::Recover::Replica replicas[] = {
        {backup1Id.getId(), 87},
    };

    TestLog::Enable __("replaySegment", "recover", "recoveryMasterFinished",
            NULL);
    MasterClient::recover(&context, masterServer->serverId, 10lu,
            serverId, 0, &recoveryPartition, replicas,
            arrayLength(replicas));
    // safeVersion Recovered
    EXPECT_EQ(23U, segmentManager->safeVersion);

    size_t curPos = 0; // Current Pos: given to getUntil()
    // Proceed read pointer
    TestLog::getUntil("recover: Recovering master 123.0", curPos, &curPos);

    EXPECT_EQ(
        "recover: Recovering master 123.0, partition 0, 1 replicas available | "
        "recover: Starting getRecoveryData from server 1.0 at "
        "mock:host=backup1 for "
        "segment 87 on channel 0 (initial round of RPCs) | "
        "recover: Waiting on recovery data for segment 87 from "
        "server 1.0 at mock:host=backup1 | ",
        TestLog::getUntil("replaySegment: SAFEVERSION 23 recovered",
                curPos, &curPos));

    EXPECT_EQ(
        "replaySegment: SAFEVERSION 23 recovered | "
        "recover: Segment 87 replay complete | ",
        TestLog::getUntil(
                "recover: Checking server 1.0 at mock:host=backup1 ",
                curPos, &curPos));

    EXPECT_EQ(
        "recover: Checking server 1.0 at mock:host=backup1 "
        "off the list for 87 | "
        "recover: Checking server 1.0 at mock:host=backup1 "
        "off the list for 87 | ",
        TestLog::getUntil("recover: Committing the SideLog... | ",
                curPos, &curPos));

    // Proceed read pointer
    TestLog::getUntil("recover: set tablet 123 0 9 to ", curPos, &curPos);

    EXPECT_EQ(
        "recover: set tablet 123 0 9 to locator mock:host=master, id 2.0 | "
        "recover: set tablet 123 10 19 to locator mock:host=master, id 2.0 | "
        "recover: set tablet 123 20 29 to locator mock:host=master, id 2.0 | "
        "recover: set tablet 124 20 100 to locator mock:host=master, "
        "id 2.0 | "
        "recover: Reporting completion of recovery 10 | "
        "recoveryMasterFinished: Called by masterId 2.0 with 4 tablets "
        "and 0 indexlets | "
        , TestLog::getUntil(
            "recoveryMasterFinished: Recovered tablets | "
            ,  curPos, &curPos));
}

TEST_F(MasterServiceTest, recover_basic_indexlet) {
    cluster.coordinator->recoveryManager.start();
    ServerId serverId(123, 0);
    ReplicaManager mgr(&context, &serverId, 1, false, false);

    // Create a segment with objectSafeVersion 23
    writeRecoverableSegment(&context, mgr, serverId, serverId.getId(),
            87, 23U);

    ProtoBuf::RecoveryPartition recoveryPartition;
    createRecoveryPartition(recoveryPartition);
    ProtoBuf::Indexlets::Indexlet& entry = *recoveryPartition.add_indexlet();
    entry.set_table_id(123);
    entry.set_index_id(4);
    entry.set_indexlet_table_id(0);
    string key0 = "a", key1 = "z";
    entry.set_first_key(key0);
    entry.set_first_not_owned_key(key1);
    auto result = BackupClient::startReadingData(&context, backup1Id,
            10lu, serverId);
    BackupClient::StartPartitioningReplicas(&context, backup1Id,
            10lu, serverId, &recoveryPartition);
    ASSERT_EQ(1lu, result.replicas.size());
    ASSERT_EQ(87lu, result.replicas.at(0).segmentId);

    SegmentManager* segmentManager = &service->objectManager.segmentManager;
    segmentManager->safeVersion = 1U; // reset safeVersion
    EXPECT_EQ(1U, segmentManager->safeVersion); // check safeVersion

    ProtoBuf::ServerList backups;
    WireFormat::Recover::Replica replicas[] = {
        {backup1Id.getId(), 87},
    };

    TestLog::Enable __("replaySegment", "recover", "recoveryMasterFinished",
            NULL);
    MasterClient::recover(&context, masterServer->serverId, 10lu,
            serverId, 0, &recoveryPartition, replicas, arrayLength(replicas));
    // safeVersion Recovered
    EXPECT_EQ(23U, segmentManager->safeVersion);

    size_t curPos = 0; // Current Pos: given to getUntil()
    TestLog::getUntil("recover: Recovering master 123.0",
            curPos, &curPos); // Proceed read pointer

    EXPECT_EQ(
        "recover: Recovering master 123.0, partition 0, 1 replicas available | "
        "recover: Starting getRecoveryData from server 1.0 at "
        "mock:host=backup1 for "
        "segment 87 on channel 0 (initial round of RPCs) | "
        "recover: Waiting on recovery data for segment 87 from "
        "server 1.0 at mock:host=backup1 | ",
        TestLog::getUntil(
            "replaySegment: SAFEVERSION 23 recovered", curPos, &curPos));

    EXPECT_EQ(
        "replaySegment: SAFEVERSION 23 recovered | "
        "recover: Segment 87 replay complete | ",
        TestLog::getUntil(
            "recover: Checking server 1.0 at mock:host=backup1 ",
            curPos, &curPos));

    EXPECT_EQ(
        "recover: Checking server 1.0 at mock:host=backup1 "
        "off the list for 87 | "
        "recover: Checking server 1.0 at mock:host=backup1 "
        "off the list for 87 | ",
        TestLog::getUntil(
            "recover: Committing the SideLog... | ", curPos, &curPos));

    // Proceed read pointer
    TestLog::getUntil("recover: set tablet 123 0 9 to ", curPos, &curPos);

    EXPECT_EQ(
        "recover: set tablet 123 0 9 to locator mock:host=master, id 2.0 | "
        "recover: set tablet 123 10 19 to locator mock:host=master, id 2.0 | "
        "recover: set tablet 123 20 29 to locator mock:host=master, id 2.0 | "
        "recover: set tablet 124 20 100 to locator mock:host=master, "
        "id 2.0 | "
        "recover: set indexlet 123 to locator mock:host=master, id 2.0 | "
        "recover: Reporting completion of recovery 10 | "
        "recoveryMasterFinished: Called by masterId 2.0 with 4 tablets "
        "and 1 indexlets | ",
        TestLog::getUntil(
            "recoveryMasterFinished: Recovered tablets | ",  curPos, &curPos));
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
  * 5) ServerNotUpExceptions are deferred until the RPC is waited on.
  */
TEST_F(MasterServiceTest, recover) {
    ServerId serverId(123, 0);

    ReplicaManager mgr(&context, &serverId, 1, false, false);
    writeRecoverableSegment(&context, mgr, serverId, serverId.getId(), 88);

    ServerConfig backup2Config = backup1Config;
    backup2Config.localLocator = "mock:host=backup2";
    ServerId backup2Id = cluster.addServer(backup2Config)->serverId;

    ProtoBuf::RecoveryPartition recoveryPartition;
    createRecoveryPartition(recoveryPartition);
    BackupClient::startReadingData(&context, backup1Id, 456lu, serverId);

    BackupClient::StartPartitioningReplicas(&context, backup1Id, 456lu,
            serverId, &recoveryPartition);

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
        // Started in initial round of RPCs - eventually fails (bad server id)
        {1003, 90},
        // Started in initial round of RPCs - eventually fails
        {backup1Id.getId(), 91},
        // Started in later rounds of RPCs - eventually fails (bad server id)
        {1004, 92},
        // Started in later rounds of RPCs - eventually fails
        {backup1Id.getId(), 93},
    };

    TestLog::Enable _;
    std::unordered_map<uint64_t, uint64_t> highestBTreeIdMap;
    EXPECT_THROW(service->recover(456lu, serverId, 0, replicas,
            highestBTreeIdMap),
            SegmentRecoveryFailedException);
    // 1,2,3) 87 was requested from the first server list entry.
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
            "recover: Starting getRecoveryData from server .\\.0 "
            "at mock:host=backup1 "
            "for segment 87 on channel . (initial round of RPCs)",
            TestLog::get()));
    typedef MasterService::Replica::State State;
    EXPECT_EQ(State::FAILED, replicas.at(0).state);
    // 2,3) 87 was *not* requested a second time in the initial RPC round
    // but was requested later once the first failed.
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
            "recover: Starting getRecoveryData from server .\\.0 "
            "at mock:host=backup2 "
            "for segment 87 .* (after RPC completion)",
            TestLog::get()));
    // 1,4) 88 was requested from the third server list entry and
    //      succeeded, which knocks the third and forth entries into
    //      OK status, preventing the launch of the forth entry
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
            "recover: Starting getRecoveryData from server .\\.0 "
            "at mock:host=backup1 "
            "for segment 88 on channel . (initial round of RPCs)",
            TestLog::get()));
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
            "recover: Checking server .\\.0 at mock:host=backup1 off "
            "the list for 88 | "
            "recover: Checking server .\\.0 at mock:host=backup2 off "
            "the list for 88",
            TestLog::get()));
    // 1,4) 88 was requested NOT from the forth server list entry.
    EXPECT_TRUE(TestUtil::doesNotMatchPosixRegex(
            "recover: Starting getRecoveryData from server .\\.0 "
            "at mock:host=backup2 "
            "for segment 88 .* (after RPC completion)",
            TestLog::get()));
    EXPECT_EQ(State::OK, replicas.at(2).state);
    EXPECT_EQ(State::OK, replicas.at(3).state);
    // 1) Checking to ensure RPCs for 87, 88, 89, 90 went first round
    //    and that 91 got issued in place, first-found due to 90's
    //    bad locator
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
            "recover: Starting getRecoveryData from server .\\.0 "
            "at mock:host=backup1 "
            "for segment 89 on channel . (initial round of RPCs)",
            TestLog::get()));
    EXPECT_EQ(State::FAILED, replicas.at(4).state);
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
            "recover: Starting getRecoveryData from server 1003.0 at "
            "(locator unavailable) "
            "for segment 90 on channel . (initial round of RPCs)",
            TestLog::get()));
    // 5) Checks bad locators for initial RPCs are handled
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
            "recover: No record of backup 1003.0, trying next backup",
            TestLog::get()));
    EXPECT_EQ(State::FAILED, replicas.at(5).state);
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
            "recover: Starting getRecoveryData from server .\\.0 "
            "at mock:host=backup1 "
            "for segment 91 on channel . (after RPC completion)",
            TestLog::get()));
    EXPECT_EQ(State::FAILED, replicas.at(6).state);
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
            "recover: Starting getRecoveryData from server 1004.0 at "
            "(locator unavailable) "
            "for segment 92 on channel . (after RPC completion)",
            TestLog::get()));
    // 5) Checks bad locators for non-initial RPCs are handled
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
            "recover: No record of backup 1004.0, trying next backup",
            TestLog::get()));
    EXPECT_EQ(State::FAILED, replicas.at(7).state);
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
            "recover: Starting getRecoveryData from server .\\.0 "
            "at mock:host=backup1 "
            "for segment 93 on channel . (after RPC completion)",
            TestLog::get()));
    EXPECT_EQ(State::FAILED, replicas.at(8).state);
}

TEST_F(MasterServiceTest, recover_ctimeUpdateIssued) {
    cluster.coordinator->recoveryManager.start();
    TestLog::Enable _("recoveryMasterFinished");
    ramcloud->write(1, "0", 1, "abcdef", 6);
    ProtoBuf::RecoveryPartition recoveryPartition;
    createRecoveryPartition(recoveryPartition);
    WireFormat::Recover::Replica replicas[] = {};
    MasterClient::recover(&context, masterServer->serverId, 10lu,
            ServerId(123), 0, &recoveryPartition, replicas, 0);

    size_t curPos = 0; // Current Pos: given to getUntil() as 2nd arg, and
    EXPECT_EQ(
            "recoveryMasterFinished: Called by masterId 2.0 with 4 tablets "
            "and 0 indexlets | "
            "recoveryMasterFinished: Recovered tablets | "
            "recoveryMasterFinished: tablet { "
            "table_id: 123 start_key_hash: 0 end_key_hash: 9 state: RECOVERING "
            "server_id: 2 service_locator: \"mock:host=master\" user_data: 0 ",
            TestLog::getUntil("ctime_log_head_id:", curPos, &curPos));
    EXPECT_EQ(
            "ctime_log_head_id: 2 "
            "ctime_log_head_offset: 88 } tablet { table_id: "
            "123 start_key_hash: 10 end_key_hash: 19 state: RECOVERING "
            "server_id: 2 service_locator: \"mock:host=master\" user_data: 0 ",
            TestLog::getUntil("ctime_log_head_id", curPos, &curPos));
    EXPECT_EQ(
            "ctime_log_head_id: 2 ctime_log_head_offset: 88 } "
            "tablet { table_id: "
            "123 start_key_hash: 20 end_key_hash: 29 state: RECOVERING "
            "server_id: 2 service_locator: \"mock:host=master\" user_data: 0 ",
            TestLog::getUntil("ctime_log_head_id", curPos, &curPos));
}

TEST_F(MasterServiceTest, recover_unsuccessful) {
    cluster.coordinator->recoveryManager.start();
    TestLog::Enable _("recover");
    ramcloud->write(1, "0", 1, "abcdef", 6);
    ProtoBuf::RecoveryPartition recoveryPartition;
    createRecoveryPartition(recoveryPartition);
    WireFormat::Recover::Replica replicas[] = {
        // Bad ServerId, should cause recovery to fail.
        {1004, 92},
    };
    MasterClient::recover(&context, masterServer->serverId, 10lu, {123, 0},
            0, &recoveryPartition, replicas, 1);

    string log = TestLog::get();
    log = log.substr(log.rfind("recover:"));
    EXPECT_EQ("recover: Failed to recover partition for recovery 10; "
            "aborting recovery on this recovery master", log);

    foreach (const auto& tablet, recoveryPartition.tablet()) {
        EXPECT_FALSE(service->tabletManager.getTablet(tablet.table_id(),
                tablet.start_key_hash(), tablet.end_key_hash()));
    }
}

class MasterRecoverTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    const uint32_t segmentSize;
    const uint32_t segmentFrames;
    ServerId backup1Id;
    ServerId backup2Id;

    public:
    MasterRecoverTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , segmentSize(1 << 16)  // Smaller than usual to make tests faster.
        , segmentFrames(30)     // Master's log uses one when constructed.
        , backup1Id()
        , backup2Id()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        ServerConfig config = ServerConfig::forTesting();
        config.localLocator = "mock:host=backup1";
        config.services = {WireFormat::BACKUP_SERVICE,
                WireFormat::MEMBERSHIP_SERVICE};
        config.segmentSize = segmentSize;
        config.backup.numSegmentFrames = segmentFrames;
        Server* server = cluster.addServer(config);
        server->backup->testingSkipCallerIdCheck = true;
        backup1Id = server->serverId;

        config.localLocator = "mock:host=backup2";
        backup2Id = cluster.addServer(config)->serverId;
        cluster.coordinatorContext.coordinatorServerList->sync();
    }

    ~MasterRecoverTest()
    { }

    MasterService*
    createMasterService()
    {
        ServerConfig config = ServerConfig::forTesting();
        config.localLocator = "mock:host=master";
        config.services = {WireFormat::MASTER_SERVICE,
                WireFormat::MEMBERSHIP_SERVICE};
        config.master.numReplicas = 2;
        return cluster.addServer(config)->master.get();
    }

    void
    appendRecoveryPartition(ProtoBuf::RecoveryPartition& recoveryPartition,
            uint64_t partitionId, uint64_t tableId,
            uint64_t start, uint64_t end,
            uint64_t ctimeHeadSegmentId, uint32_t ctimeHeadSegmentOffset)
    {
        ProtoBuf::Tablets::Tablet& tablet(*recoveryPartition.add_tablet());
        tablet.set_table_id(tableId);
        tablet.set_start_key_hash(start);
        tablet.set_end_key_hash(end);
        tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
        tablet.set_user_data(partitionId);
        tablet.set_ctime_log_head_id(ctimeHeadSegmentId);
        tablet.set_ctime_log_head_offset(ctimeHeadSegmentOffset);
    }

    void
    createRecoveryPartition(ProtoBuf::RecoveryPartition& recoveryPartition)
    {
        appendRecoveryPartition(recoveryPartition, 0, 123, 0, 9, 0, 0);
        appendRecoveryPartition(recoveryPartition, 0, 123, 10, 19, 0, 0);
        appendRecoveryPartition(recoveryPartition, 0, 123, 20, 29, 0, 0);
        appendRecoveryPartition(recoveryPartition, 0, 124, 20, 100, 0, 0);
    }
    DISALLOW_COPY_AND_ASSIGN(MasterRecoverTest);
};

TEST_F(MasterRecoverTest, recover) {
    MasterService* master = createMasterService();

    // Create a separate fake "server" (private context and serverList) and
    // use it to replicate 2 segments worth of data on a single backup.
    Context context2;
    ServerList serverList2(&context2);
    context2.transportManager->registerMock(&cluster.transport);
    serverList2.testingAdd({backup1Id, "mock:host=backup1",
            {WireFormat::BACKUP_SERVICE, WireFormat::MEMBERSHIP_SERVICE},
            100, ServerStatus::UP});
    ServerId serverId(99, 0);
    ReplicaManager mgr(&context2, &serverId, 1, false, false);
    MasterServiceTest::writeRecoverableSegment(&context, mgr, serverId, 99, 87);
    MasterServiceTest::writeRecoverableSegment(&context, mgr, serverId, 99, 88);

    // Now run recovery, as if the fake server failed.
    ProtoBuf::RecoveryPartition recoveryPartition;
    createRecoveryPartition(recoveryPartition);
    {
        BackupClient::startReadingData(&context, backup1Id, 456lu,
                ServerId(99));
        BackupClient::StartPartitioningReplicas(&context, backup1Id, 456lu,
                ServerId(99), &recoveryPartition);
    }
    {
        BackupClient::startReadingData(&context, backup2Id, 456lu,
                ServerId(99));
        BackupClient::StartPartitioningReplicas(&context, backup2Id, 456lu,
                ServerId(99), &recoveryPartition);
    }

    vector<MasterService::Replica> replicas {
        { backup1Id.getId(), 87 },
        { backup1Id.getId(), 88 },
        { backup1Id.getId(), 88 },
    };

    MockRandom __(1); // triggers deterministic rand().
    TestLog::Enable _("replaySegment", "recover", NULL);
    std::unordered_map<uint64_t, uint64_t> highestBTreeIdMap;
    master->recover(456lu, ServerId(99, 0), 0, replicas, highestBTreeIdMap);
    EXPECT_EQ(0U, TestLog::get().find(
            "recover: Recovering master 99.0, partition 0, 3 replicas "
            "available"));
    EXPECT_NE(string::npos, TestLog::get().find(
            "recover: Segment 88 replay complete"));
    EXPECT_NE(string::npos, TestLog::get().find(
            "recover: Segment 87 replay complete"));
}

TEST_F(MasterRecoverTest, failedToRecoverAll) {
    MasterService* master = createMasterService();

    ProtoBuf::RecoveryPartition recoveryPartition;
    ProtoBuf::ServerList backups;
    vector<MasterService::Replica> replicas {
        { backup1Id.getId(), 87 },
        { backup1Id.getId(), 88 },
    };

    MockRandom __(1); // triggers deterministic rand().
    TestLog::Enable _("replaySegment", "recover", NULL);
    std::unordered_map<uint64_t, uint64_t> highestBTreeIdMap;
    EXPECT_THROW(master->recover(456lu, ServerId(99, 0), 0, replicas,
            highestBTreeIdMap), SegmentRecoveryFailedException);
    string log = TestLog::get();
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
            "recover: Recovering master 99.0, partition 0, "
            "2 replicas available | "
            "recover: Starting getRecoveryData from server .\\.0 at "
            "mock:host=backup1 for segment 87 on channel 0 "
            "(initial round of RPCs) | "
            "recover: Starting getRecoveryData from server .\\.0 at "
            "mock:host=backup1 for segment 88 on channel 1 "
            "(initial round of RPCs) | "
            "recover: Waiting on recovery data for segment 87 from "
            "server .\\.0 at mock:host=backup1 | "
            "recover: getRecoveryData failed on server .\\.0 at "
            "mock:host=backup1, trying next backup; failure was: "
            "bad segment id",
            log.substr(0, log.find(" thrown at"))));
}

}  // namespace RAMCloud
