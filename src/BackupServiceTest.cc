/* Copyright (c) 2009-2012 Stanford University
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

#include <cstring>

#include "TestUtil.h"
#include "BackupReplica.h"
#include "BackupService.h"
#include "Log.h"
#include "LogDigest.h"
#include "MockCluster.h"
#include "SegmentIterator.h"
#include "Server.h"
#include "Key.h"
#include "ShortMacros.h"
#include "StringUtil.h"

namespace RAMCloud {
/**
 * Unit tests for BackupService.
 */

class BackupServiceTest : public ::testing::Test {
  public:
    Context context;
    ServerConfig config;
    Tub<MockCluster> cluster;
    Server* server;
    BackupService* backup;
    mode_t oldUmask;
    ServerList serverList;
    ServerId backupId;

    BackupServiceTest()
        : context()
        , config(ServerConfig::forTesting())
        , cluster()
        , server()
        , backup()
        , oldUmask(umask(0))
        , serverList(&context)
        , backupId(5, 0)
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        cluster.construct(&context);
        config.services = {WireFormat::BACKUP_SERVICE};
        config.backup.numSegmentFrames = 5;
        server = cluster->addServer(config);
        backup = server->backup.get();

        serverList.add(backupId, server->config.localLocator,
                                {WireFormat::BACKUP_SERVICE}, 100);
    }

    ~BackupServiceTest()
    {
        cluster.destroy();
        umask(oldUmask);
        EXPECT_EQ(0,
            BackupStorage::Handle::resetAllocatedHandlesCount());
    }

    void
    closeSegment(ServerId masterId, uint64_t segmentId) {
        Segment segment;
        BackupClient::writeSegment(&context, backupId, masterId, segmentId,
                                   &segment, 0, 0, {},
                                   WireFormat::BackupWrite::CLOSE, false);
    }

    vector<ServerId>
    openSegment(ServerId masterId, uint64_t segmentId, bool primary = true,
                bool atomic = false)
    {
        Segment segment;
        auto flags = primary ? WireFormat::BackupWrite::OPENPRIMARY
                             : WireFormat::BackupWrite::OPEN;
        return BackupClient::writeSegment(&context, backupId, masterId,
                                          segmentId, &segment, 0, 0, {},
                                          flags, atomic);
    }

    /**
     * Write a raw string to the segment on backup (including the nul-
     * terminator). The segment will not be properly formatted and so
     * will not be recoverable.
     */
    void
    writeRawString(ServerId masterId, uint64_t segmentId,
                   uint32_t offset, const string& s,
                   WireFormat::BackupWrite::Flags flags =
                   WireFormat::BackupWrite::NONE,
                   bool atomic = false)
    {
        Segment segment;
        segment.copyIn(offset, s.c_str(), downCast<uint32_t>(s.length()));
        BackupClient::writeSegment(&context, backupId, masterId, segmentId,
                                   &segment,
                                   offset,
                                   uint32_t(s.length() + 1), {},
                                   flags, atomic);
    }

    /**
     * Helper method for the various other write* methods. Writes a typed
     * method to the given segment and propagates it to the backup. The
     * segment on backup will be properly formatted and will be recoverable.
     */
    void
    appendEntry(Segment& segment, ServerId masterId, uint64_t segmentId,
                LogEntryType type, const void *data, uint32_t bytes)
    {
        Segment::OpaqueFooterEntry footerEntry;
        uint32_t before = segment.getAppendedLength(footerEntry);
        segment.append(type, data, bytes);
        uint32_t after = segment.getAppendedLength(footerEntry);

        BackupClient::writeSegment(&context, backupId, masterId, segmentId,
                                   &segment,
                                   before,
                                   after - before, &footerEntry);
    }

    /**
     * Append an object to the given segment and replicate. This will maintain
     * proper formatting of the segment.
     */
    void
    appendObject(Segment& segment, ServerId masterId, uint64_t segmentId,
                 const char *data, uint32_t bytes, uint64_t tableId,
                 const char* stringKey, uint16_t stringKeyLength)
    {
        Key key(tableId, stringKey, stringKeyLength);
        Object object(key, data, bytes, 0, 0);
        Buffer buffer;
        object.serializeToBuffer(buffer);
        const void* contiguous = buffer.getRange(0, buffer.getTotalLength());
        appendEntry(segment, masterId, segmentId, LOG_ENTRY_TYPE_OBJ,
                    contiguous, buffer.getTotalLength());
    }

    /**
     * Append a tombstone to the given segment and replicate. This will maintain
     * proper formatting of the segment.
     */
    void
    appendTombstone(Segment& segment, ServerId masterId, uint64_t segmentId,
                    uint64_t tableId, const char* stringKey,
                    uint16_t stringKeyLength)
    {
        Key key(tableId, stringKey, stringKeyLength);
        Object object(key, NULL, 0, 0, 0);
        ObjectTombstone tombstone(object, segmentId, 0);
        Buffer buffer;
        tombstone.serializeToBuffer(buffer);
        const void* contiguous = buffer.getRange(0, buffer.getTotalLength());
        appendEntry(segment, masterId, segmentId, LOG_ENTRY_TYPE_OBJTOMB,
                    contiguous, buffer.getTotalLength());
    }

    /**
     * Append a header to the given segment and replicate. This will maintain
     * proper formatting of the segment.
     */
    void
    appendHeader(Segment& segment, ServerId masterId, uint64_t segmentId)
    {
        SegmentHeader header(*masterId, segmentId, config.segmentSize,
                             Segment::INVALID_SEGMENT_ID);
        appendEntry(segment, masterId, segmentId, LOG_ENTRY_TYPE_SEGHEADER,
                    &header, sizeof(header));
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
        // partition 0
        appendTablet(tablets, 0, 123,
            Key::getHash(123, "9", 1), Key::getHash(123, "9", 1), 0, 0);
        appendTablet(tablets, 0, 123,
            Key::getHash(123, "10", 2), Key::getHash(123, "10", 2), 0, 0);
        appendTablet(tablets, 0, 123,
            Key::getHash(123, "29", 2), Key::getHash(123, "29", 2), 0, 0);

        appendTablet(tablets, 0, 124,
            Key::getHash(124, "20", 2), Key::getHash(124, "20", 2), 0, 0);

        // partition 1
        appendTablet(tablets, 1, 123,
            Key::getHash(123, "30", 2), Key::getHash(123, "30", 2), 0, 0);
        appendTablet(tablets, 1, 125,
            0, std::numeric_limits<uint64_t>::max(), 0, 0);
    }

    static bool
    inMemoryStorageFreePred(string s)
    {
        return s == "free";
    }

    // Helper method for the LogDigest tests. This writes a proper Segment
    // with a LogDigest containing the given IDs.
    void
    writeDigestedSegment(ServerId masterId, uint64_t segmentId,
        vector<uint64_t> digestIds, bool atomic = false)
    {
        LogDigest digest;
        for (uint32_t i = 0; i < digestIds.size(); i++)
            digest.addSegmentId(digestIds[i]);
        Buffer digestBuffer;
        digest.appendToBuffer(digestBuffer);

        Segment s;
        s.append(LOG_ENTRY_TYPE_LOGDIGEST, digestBuffer);

        Buffer buffer;
        s.appendToBuffer(buffer);
        Segment::OpaqueFooterEntry footerEntry;
        uint32_t appendedBytes = s.getAppendedLength(footerEntry);
        BackupClient::writeSegment(&context, backupId, masterId,
                                   segmentId, &s, 0, appendedBytes,
                                   &footerEntry,
                                   WireFormat::BackupWrite::NONE, atomic);
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(BackupServiceTest);
};


struct TempCleanup {
    string path;
    explicit TempCleanup(string path) : path(path) {}
    ~TempCleanup() {
        int r = unlink(path.c_str());
        if (r == -1) {
            DIE("Unit test left garbage tmp file around %s: %s",
                path.c_str(), strerror(errno));
        }
    }
};

namespace {
bool constructFilter(string s) {
    return s == "BackupService" || s == "init";
}
};

TEST_F(BackupServiceTest, constructorNoReuseReplicas) {
    config.backup.inMemory = false;
    config.clusterName = "testing";
    config.backup.file = "/tmp/ramcloud-backup-storage-test-delete-this";

    TempCleanup __(config.backup.file);
    cluster->addServer(config);

    config.clusterName = "__unnamed__";
    TestLog::Enable _(constructFilter);
    BackupService* backup = cluster->addServer(config)->backup.get();
    EXPECT_EQ(ServerId(), backup->getFormerServerId());
    EXPECT_EQ(
        "BackupService: Cluster '__unnamed__'; ignoring existing backup "
            "storage. Any replicas stored will not be reusable by future "
            "backups. Specify clusterName for persistence across backup "
            "restarts. | "
        "init: My server ID is 3.0 | "
        "init: Backup 3.0 will store replicas under cluster name '__unnamed__'"
        , TestLog::get());
}

TEST_F(BackupServiceTest, constructorDestroyConfusingReplicas) {
    config.backup.inMemory = false;
    config.clusterName = "__unnamed__";
    config.backup.file = "/tmp/ramcloud-backup-storage-test-delete-this";

    TempCleanup __(config.backup.file);
    cluster->addServer(config);

    config.clusterName = "testing";
    TestLog::Enable _(constructFilter);
    BackupService* backup = cluster->addServer(config)->backup.get();
    EXPECT_EQ(ServerId(), backup->getFormerServerId());
    EXPECT_EQ(
        "BackupService: Backup storing replicas with clusterName 'testing'. "
            "Future backups must be restarted with the same clusterName for "
            "replicas stored on this backup to be reused. | "
        "BackupService: Replicas stored on disk have a different clusterName "
            "('__unnamed__'). Scribbling storage to ensure any stale replicas "
            "left behind by old backups aren't used by future backups | "
        "init: My server ID is 3.0 | "
        "init: Backup 3.0 will store replicas under cluster name 'testing'"
        , TestLog::get());
}

TEST_F(BackupServiceTest, constructorReuseReplicas)
{
    config.backup.inMemory = false;
    config.clusterName = "testing";
    config.backup.file = "/tmp/ramcloud-backup-storage-test-delete-this";

    TempCleanup __(config.backup.file);
    cluster->addServer(config);

    TestLog::Enable _(constructFilter);
    cluster->addServer(config);
    EXPECT_EQ(
        "BackupService: Backup storing replicas with clusterName 'testing'. "
            "Future backups must be restarted with the same clusterName for "
            "replicas stored on this backup to be reused. | "
        "BackupService: Replicas stored on disk have matching clusterName "
            "('testing'). Scanning storage to find all replicas and to make "
            "them available to recoveries. | "
        "BackupService: Will enlist as a replacement for formerly crashed "
            "server 2.0 which left replicas behind on disk | "
        "init: My server ID is 2.1 | "
        "init: Backup 2.1 will store replicas under cluster name "
            "'testing'"
        , TestLog::get());
}

TEST_F(BackupServiceTest, findBackupReplica) {
    EXPECT_TRUE(NULL == backup->findBackupReplica(ServerId(99, 0), 88));
    openSegment(ServerId(99, 0), 88);
    closeSegment(ServerId(99, 0), 88);
    BackupReplica* replica =
        backup->findBackupReplica(ServerId(99, 0), 88);
    EXPECT_TRUE(replica != NULL);
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, findBackupReplica_notIn) {
    EXPECT_TRUE(NULL == backup->findBackupReplica(ServerId(99, 0), 88));
}

TEST_F(BackupServiceTest, assignGroup) {
    uint64_t groupId = 100;
    const uint32_t numReplicas = 3;
    ServerId ids[numReplicas] = {ServerId(15), ServerId(16), ServerId(99)};
    BackupClient::assignGroup(&context, backupId, groupId, numReplicas, ids);
    EXPECT_EQ(groupId, backup->replicationId);
    EXPECT_EQ(15U, backup->replicationGroup.at(0).getId());
    EXPECT_EQ(16U, backup->replicationGroup.at(1).getId());
    EXPECT_EQ(99U, backup->replicationGroup.at(2).getId());
    ids[0] = ServerId(33);
    ids[1] = ServerId(22);
    ids[2] = ServerId(11);
    BackupClient::assignGroup(&context, backupId, groupId, numReplicas, ids);
    EXPECT_EQ(3U, backup->replicationGroup.size());
    EXPECT_EQ(33U, backup->replicationGroup.at(0).getId());
}

TEST_F(BackupServiceTest, freeSegment) {
    openSegment({99, 0}, 88);
    closeSegment({99, 0}, 88);
    {
        TestLog::Enable _(&inMemoryStorageFreePred);
        BackupClient::freeSegment(&context, backupId, ServerId(99, 0), 88);
        EXPECT_EQ("free: called", TestLog::get());
    }
    EXPECT_TRUE(NULL == backup->findBackupReplica(ServerId(99, 0), 88));
    BackupClient::freeSegment(&context, backupId, ServerId(99, 0), 88);
    EXPECT_TRUE(NULL == backup->findBackupReplica(ServerId(99, 0), 88));
}

TEST_F(BackupServiceTest, freeSegment_stillOpen) {
    openSegment(ServerId(99, 0), 88);
    BackupClient::freeSegment(&context, backupId, ServerId(99, 0), 88);
    EXPECT_TRUE(NULL == backup->findBackupReplica(ServerId(99, 0), 88));
}

TEST_F(BackupServiceTest, getRecoveryData) {
    ProtoBuf::Tablets tablets;
    createTabletList(tablets);

    openSegment(ServerId(99, 0), 88);
    Segment s;
    appendHeader(s, ServerId(99, 0), 88);
    // Objects
    // Barely in tablet
    appendObject(s, ServerId(99, 0), 88, "test1", 6, 123, "29", 2);
    // Barely out of tablets
    appendObject(s, ServerId(99, 0), 88, "test2", 6, 123, "30", 2);
    // In another table
    appendObject(s, ServerId(99, 0), 88, "test3", 6, 124, "20", 2);
    // Not in any table
    appendObject(s, ServerId(99, 0), 88, "test4", 6, 125, "20", 2);
    // Tombstones
    // Barely in tablet
    appendTombstone(s, ServerId(99, 0), 88, 123, "29", 2);
    // Barely out of tablets
    appendTombstone(s, ServerId(99, 0), 88, 123, "30", 2);
    // In another table
    appendTombstone(s, ServerId(99, 0), 88, 124, "20", 2);
    // Not in any table
    appendTombstone(s, ServerId(99, 0), 88, 125, "20", 2);
    closeSegment(ServerId(99, 0), 88);
    BackupClient::startReadingData(&context, backupId, ServerId(99, 0),
                                   &tablets);

    Buffer response;
    BackupClient::getRecoveryData(&context, backupId, ServerId(99, 0),
                                  88, 0, &response);

    SegmentIterator it(
        response.getRange(0, response.getTotalLength()),
        response.getTotalLength());

    {
        Buffer b;
        EXPECT_FALSE(it.isDone());
        EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
        it.setBufferTo(b);
        Object object(b);
        EXPECT_EQ(123U, object.getTableId());
        EXPECT_EQ("29", TestUtil::toString(
            object.getKey(), object.getKeyLength()));
        it.next();
    }

    {
        Buffer b;
        EXPECT_FALSE(it.isDone());
        EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
        it.setBufferTo(b);
        Object object(b);
        EXPECT_EQ(124U, object.getTableId());
        EXPECT_EQ("20", TestUtil::toString(
            object.getKey(), object.getKeyLength()));
        it.next();
    }

    {
        Buffer b;
        EXPECT_FALSE(it.isDone());
        EXPECT_EQ(LOG_ENTRY_TYPE_OBJTOMB, it.getType());
        it.setBufferTo(b);
        ObjectTombstone tomb(b);
        EXPECT_EQ(123U, tomb.getTableId());
        EXPECT_EQ("29", TestUtil::toString(tomb.getKey(), tomb.getKeyLength()));
        it.next();
    }

    {
        Buffer b;
        EXPECT_FALSE(it.isDone());
        EXPECT_EQ(LOG_ENTRY_TYPE_OBJTOMB, it.getType());
        it.setBufferTo(b);
        ObjectTombstone tomb(b);
        EXPECT_EQ(124U, tomb.getTableId());
        EXPECT_EQ("20", TestUtil::toString(
            tomb.getKey(), tomb.getKeyLength()));
        it.next();
    }

    EXPECT_TRUE(it.isDone());
}

TEST_F(BackupServiceTest, getRecoveryData_moreThanOneSegmentStored) {
    openSegment(ServerId(99, 0), 87);
    Segment seg87;
    appendHeader(seg87, ServerId(99, 0), 87);
    appendObject(seg87, ServerId(99, 0), 87, "test1", 6, 123, "9", 1);
    closeSegment(ServerId(99, 0), 87);

    openSegment(ServerId(99, 0), 88);
    Segment seg88;
    appendHeader(seg88, ServerId(99, 0), 88);
    appendObject(seg88, ServerId(99, 0), 88, "test2", 6, 123, "10", 2);
    closeSegment(ServerId(99, 0), 88);

    ProtoBuf::Tablets tablets;
    createTabletList(tablets);

    BackupClient::startReadingData(&context, backupId, ServerId(99, 0),
                                   &tablets);

    {
        Buffer response;
        BackupClient::getRecoveryData(&context, backupId, ServerId(99, 0),
                                      88, 0, &response);

        SegmentIterator it(
            response.getRange(0, response.getTotalLength()),
            response.getTotalLength());
        EXPECT_FALSE(it.isDone());
        EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());

        Buffer b;
        it.setBufferTo(b);
        Object object(b);
        EXPECT_EQ("test2", TestUtil::toString(object.getData(),
            object.getDataLength() - 1));

        it.next();
        EXPECT_TRUE(it.isDone());
    }
    {
        Buffer response;
        BackupClient::getRecoveryData(&context, backupId, ServerId(99, 0),
                                      87, 0, &response);

        SegmentIterator it(
            response.getRange(0, response.getTotalLength()),
            response.getTotalLength());
        EXPECT_FALSE(it.isDone());
        EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());

        Buffer b;
        it.setBufferTo(b);
        Object object(b);
        EXPECT_EQ("test1", TestUtil::toString(object.getData(),
            object.getDataLength() - 1));

        it.next();
        EXPECT_TRUE(it.isDone());
    }

    BackupClient::freeSegment(&context, backupId, ServerId(99, 0), 87);
    BackupClient::freeSegment(&context, backupId, ServerId(99, 0), 88);
}

TEST_F(BackupServiceTest, getRecoveryData_malformedSegment) {
    openSegment(ServerId(99, 0), 88);
    closeSegment(ServerId(99, 0), 88);

    ProtoBuf::Tablets tablets;
    BackupClient::startReadingData(&context, backupId, ServerId(99, 0),
                                   &tablets);

    while (true) {
        Buffer response;
        EXPECT_THROW(
            BackupClient::getRecoveryData(&context, backupId, ServerId(99, 0),
                                          88, 0, &response),
            SegmentRecoveryFailedException);
        break;
    }

    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, getRecoveryData_notRecovered) {
    openSegment(ServerId(99, 0), 88);
    Segment s;
    appendHeader(s, ServerId(99, 0), 88);
    appendObject(s, ServerId(99, 0), 88, "test2", 6, 123, "10", 2);
    Buffer response;
    EXPECT_THROW(
        BackupClient::getRecoveryData(&context, backupId, ServerId(99, 0),
                                      88, 0, &response),
        BackupBadSegmentIdException);

    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, killAllStorage)
{
    const char* path = "/tmp/ramcloud-backup-storage-test-delete-this";
    TempCleanup __(path);
    ServerConfig config = ServerConfig::forTesting();
    config.backup.inMemory = false;
    config.segmentSize = 4096;
    config.backup.numSegmentFrames = 6;
    config.backup.file = path;
    config.services = {WireFormat::BACKUP_SERVICE};

    config.clusterName = "old";
    cluster->addServer(config);

    config.clusterName = "new";
    BackupService* backup = cluster->addServer(config)->backup.get();
    std::unique_ptr<BackupStorage::Handle>
        handle(backup->storage->associate(0));
    Memory::unique_ptr_free segment(
        Memory::xmemalign(HERE, getpagesize(),
                          config.segmentSize),
        std::free);
    char *p = static_cast<char*>(segment.get());
    backup->storage->getSegment(handle.get(), p);
    EXPECT_EQ(0, memcmp("\0DIE", p, 4));
}

TEST_F(BackupServiceTest, recoverySegmentBuilder) {
    Context context;
    openSegment(ServerId(99, 0), 87);
    Segment seg87;
    appendHeader(seg87, ServerId(99, 0), 87);
    appendObject(seg87, ServerId(99, 0), 87, "test1", 6, 123, "9", 1);
    closeSegment(ServerId(99, 0), 87);

    openSegment(ServerId(99, 0), 88);
    Segment seg88;
    appendHeader(seg88, ServerId(99, 0), 88);
    appendObject(seg88, ServerId(99, 0), 88, "test2", 6, 123, "30", 2);
    closeSegment(ServerId(99, 0), 88);

    vector<BackupReplica*> toBuild;
    auto replica = backup->findBackupReplica(ServerId(99, 0), 87);
    EXPECT_TRUE(NULL != replica);
    replica->setRecovering();
    replica->startLoading();
    toBuild.push_back(replica);
    replica = backup->findBackupReplica(ServerId(99, 0), 88);
    EXPECT_TRUE(NULL != replica);
    replica->setRecovering();
    replica->startLoading();
    toBuild.push_back(replica);

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);
    Atomic<int> recoveryThreadCount{0};
    BackupService::RecoverySegmentBuilder builder(&context,
                                                  toBuild,
                                                  partitions,
                                                  recoveryThreadCount,
                                                  config.segmentSize);
    builder();

    EXPECT_EQ(BackupReplica::RECOVERING,
                            toBuild[0]->state);
    ASSERT_TRUE(toBuild[0]->recoverySegments);
    Segment* seg = &toBuild[0]->recoverySegments[0];
    ASSERT_TRUE(seg);
    SegmentIterator it(*seg);
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());

    {
        Buffer b;
        it.setBufferTo(b);
        Object object(b);
        EXPECT_EQ("test1", TestUtil::toString(
            object.getData(), object.getDataLength() - 1));
        it.next();
        EXPECT_TRUE(it.isDone());
    }

    EXPECT_EQ(BackupReplica::RECOVERING,
              toBuild[1]->state);
    EXPECT_TRUE(NULL != toBuild[1]->recoverySegments);
    seg = &toBuild[1]->recoverySegments[1];
    SegmentIterator it2(*seg);
    EXPECT_FALSE(it2.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it2.getType());

    {
        Buffer b;
        it2.setBufferTo(b);
        Object object(b);
        EXPECT_EQ("test2", TestUtil::toString(
            object.getData(), object.getDataLength() - 1));
        it2.next();
        EXPECT_TRUE(it2.isDone());
    }
}

namespace {
bool restartFilter(string s) {
    return s == "restartFromStorage";
}
}

#if 0
// We can't do this anymore. This file can't just fake up entries. For one, it's
// hard because the length fields aren't part of the Segment::EntryHeader
// anymore. More importantly, however, it shouldn't be reaching into segment
// internals. I think this might need to just build temporary segments instead.
TEST_F(BackupServiceTest, restartFromStorage)
{
    const char* path = "/tmp/ramcloud-backup-storage-test-delete-this";
    int fd = -1;
    void* file = NULL;
    ServerConfig config = ServerConfig::forTesting();
    config.backup.inMemory = false;
    config.segmentSize = 4096;
    config.backup.numSegmentFrames = 6;
    config.backup.file = path;
    config.services = {WireFormat::BACKUP_SERVICE};
    config.clusterName = "testing";
    // Space for superblock images and then segment frames.
    const size_t superblockSize = 2 * SingleFileStorage::BLOCK_SIZE;
    const size_t fileSize = superblockSize +
                            config.segmentSize * config.backup.numSegmentFrames;

    try {
    fd = open(path, O_CREAT | O_RDWR, 0666);
    ASSERT_NE(-1, fd);
    ASSERT_NE(-1, ftruncate(fd, fileSize));
    file = mmap(NULL, fileSize, PROT_READ | PROT_WRITE, MAP_SHARED,
                fd, 0);
    ASSERT_NE(MAP_FAILED, file);
    ASSERT_NE(-1, close(fd));

    typedef char* b;

    BackupStorage::Superblock superblock(0, {99, 0}, "testing");
    memcpy(file, &superblock, sizeof(superblock));
    Crc32C crc;
    crc.update(&superblock, sizeof(superblock));
    auto checksum = crc.getResult();
    memcpy(b(file) + sizeof(superblock), &checksum, sizeof(checksum));

    for (uint32_t frame = 0; frame < config.backup.numSegmentFrames; ++frame) {
        SegmentHeader header{70 + (frame % 2), 88, config.segmentSize,
            Segment::INVALID_SEGMENT_ID};
        Segment::EntryHeader headerEntry(LOG_ENTRY_TYPE_SEGHEADER,
                                         sizeof(header));
        SegmentFooter footer{0xcafebabe};
        Segment::EntryHeader footerEntry(LOG_ENTRY_TYPE_SEGFOOTER,
                                         sizeof(footer));

        bool close = true;

        // Set up various weird scenarios for each segment frame.
        switch (frame) {
        case 0:
            // Normal header and footer for a closed segment.
            break;
        case 1:
            // Normal header, no footer (open).
            header.segmentId = 89;
            close = false;
            break;
        case 2:
            // Bad entry type for header.
            headerEntry.type = 'q';
            header.segmentId = 90;
            close = true;
            break;
        case 3:
            // Bad entry length for header.
            headerEntry.length = 999;
            header.segmentId = 91;
            close = true;
            break;
        case 4:
            // Bad entry type for footer.
            footerEntry.type = 'q';
            header.segmentId = 92;
            close = true;
            break;
        case 5:
            // Bad entry length for footer.
            footerEntry.length = 999;
            header.segmentId = 93;
            close = true;
            break;
        default:
            FAIL();
        };

        off_t offset = superblockSize;
        offset += frame * config.segmentSize;
        memcpy(b(file) + offset, &headerEntry, sizeof(headerEntry));
        offset += sizeof(headerEntry);
        memcpy(b(file) + offset, &header, sizeof(header));
        offset += sizeof(header);

        if (close) {
            offset = superblockSize;
            offset += (frame + 1) * config.segmentSize -
                sizeof(footerEntry) - sizeof(footer);
            memcpy(b(file) + offset, &footerEntry, sizeof(footerEntry));
            offset += sizeof(footerEntry);
            memcpy(b(file) + offset, &footer, sizeof(footer));
            offset += sizeof(footer);
        }
    }

    ASSERT_NE(-1, munmap(file, fileSize));

    TestLog::Enable _(restartFilter);
    BackupService* backup = cluster->addServer(config)->backup.get();
    EXPECT_EQ(ServerId(99, 0), backup->getFormerServerId());
    EXPECT_NE(string::npos, TestLog::get().find(
        "restartFromStorage: Found stored replica <70,88> on backup storage "
            "in frame 0 which was closed | "
        "restartFromStorage: Found stored replica <71,89> on backup storage "
            "in frame 1 which was open | "
        "restartFromStorage: Log entry type for header does not match in "
            "frame 2 | "
        "restartFromStorage: Unexpected log entry length while reading "
            "segment replica header from backup storage, discarding replica, "
            "(expected length 28, stored length 999) | "
        "restartFromStorage: Found stored replica <70,92> on backup storage "
            "in frame 4 which was open | "
        "restartFromStorage: Found stored replica <71,93> on backup storage "
            "in frame 5 which was open"));

    EXPECT_TRUE(backup->findBackupReplica({70, 0}, 88));
    EXPECT_TRUE(backup->findBackupReplica({71, 0}, 89));
    EXPECT_FALSE(backup->findBackupReplica({70, 0}, 90));
    EXPECT_FALSE(backup->findBackupReplica({71, 0}, 91));
    EXPECT_TRUE(backup->findBackupReplica({70, 0}, 92));
    EXPECT_TRUE(backup->findBackupReplica({71, 0}, 93));

    SingleFileStorage* storage =
        static_cast<SingleFileStorage*>(backup->storage.get());
    EXPECT_FALSE(storage->freeMap.test(0));
    EXPECT_FALSE(storage->freeMap.test(1));
    EXPECT_TRUE(storage->freeMap.test(2));
    EXPECT_TRUE(storage->freeMap.test(3));
    EXPECT_FALSE(storage->freeMap.test(4));
    EXPECT_FALSE(storage->freeMap.test(5));

    EXPECT_EQ(2lu, backup->gcTaskQueue.outstandingTasks());
    // Because config.backup.gc is false these tasks delete themselves
    // immediately when performed.
    backup->gcTaskQueue.performTask();
    backup->gcTaskQueue.performTask();
    EXPECT_EQ(0lu, backup->gcTaskQueue.outstandingTasks());

    } catch (...) {
        close(fd);
        munmap(file, fileSize);
        cluster.destroy();
        unlink(path);
        throw;
    }
    cluster.destroy();
    unlink(path);
}
#endif
TEST_F(BackupServiceTest, startReadingData) {
    MockRandom _(1);
    openSegment(ServerId(99, 0), 88);
    Segment s;
    appendHeader(s, ServerId(99, 0), 88);
    openSegment(ServerId(99, 0), 89);
    openSegment(ServerId(99, 0), 98, false);
    openSegment(ServerId(99, 0), 99, false);

    ProtoBuf::Tablets tablets;
    StartReadingDataRpc::Result result =
        BackupClient::startReadingData(&context, backupId, ServerId(99, 0),
                                       &tablets);
    EXPECT_EQ(4u, result.segmentIdAndLength.size());

    Segment::OpaqueFooterEntry unused;
    EXPECT_EQ(88U, result.segmentIdAndLength[0].first);
    EXPECT_EQ(s.getAppendedLength(unused), result.segmentIdAndLength[0].second);
    {
        BackupReplica& replica =
            *backup->findBackupReplica(ServerId(99, 0), 88);
        BackupReplica::Lock lock(replica.mutex);
        EXPECT_EQ(BackupReplica::RECOVERING, replica.state);
    }

    EXPECT_EQ(89U, result.segmentIdAndLength[1].first);
    EXPECT_EQ(0U, result.segmentIdAndLength[1].second);
    {
        BackupReplica& replica =
            *backup->findBackupReplica(ServerId(99, 0), 89);
        BackupReplica::Lock lock(replica.mutex);
        EXPECT_EQ(BackupReplica::RECOVERING, replica.state);
    }

    EXPECT_EQ(98U, result.segmentIdAndLength[2].first);
    EXPECT_EQ(0U, result.segmentIdAndLength[2].second);
    {
        BackupReplica& replica =
            *backup->findBackupReplica(ServerId(99, 0), 98);
        BackupReplica::Lock lock(replica.mutex);
        EXPECT_EQ(BackupReplica::RECOVERING, replica.state);
        EXPECT_TRUE(replica.recoveryPartitions);
    }

    EXPECT_EQ(99U, result.segmentIdAndLength[3].first);
    EXPECT_EQ(0U, result.segmentIdAndLength[3].second);
    EXPECT_TRUE(backup->findBackupReplica(
        ServerId(99, 0), 99)->recoveryPartitions);
    {
        BackupReplica& replica =
            *backup->findBackupReplica(ServerId(99, 0), 99);
        BackupReplica::Lock lock(replica.mutex);
        EXPECT_EQ(BackupReplica::RECOVERING, replica.state);
        EXPECT_TRUE(replica.recoveryPartitions);
    }

    EXPECT_EQ(4, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, startReadingData_empty) {
    ProtoBuf::Tablets tablets;
    StartReadingDataRpc::Result result =
        BackupClient::startReadingData(&context, backupId, ServerId(99, 0),
                                       &tablets);
    EXPECT_EQ(0U, result.segmentIdAndLength.size());
    EXPECT_EQ(0U, result.logDigestBytes);
    EXPECT_TRUE(NULL == result.logDigestBuffer);
}

TEST_F(BackupServiceTest, startReadingData_logDigest_simple) {
    // ensure that we get the LogDigest back at all.
    openSegment(ServerId(99, 0), 88);
    writeDigestedSegment(ServerId(99, 0), 88, { 0x3f17c2451f0cafUL });

    ProtoBuf::Tablets tablets;
    StartReadingDataRpc::Result result =
        BackupClient::startReadingData(&context, backupId, ServerId(99, 0),
                                       &tablets);
    EXPECT_EQ(12U, result.logDigestBytes);
    EXPECT_EQ(88U, result.logDigestSegmentId);
    EXPECT_EQ(14U, result.logDigestSegmentLen);
    {
        LogDigest ld(result.logDigestBuffer.get(), result.logDigestBytes);
        EXPECT_EQ(1U, ld.size());
        EXPECT_EQ(0x3f17c2451f0cafUL, ld[0]);
    }

    // Repeating the call should yield the same digest.
    result = BackupClient::startReadingData(&context, backupId, {99, 0},
                                            &tablets);
    EXPECT_EQ(12U, result.logDigestBytes);
    EXPECT_EQ(88U, result.logDigestSegmentId);
    EXPECT_EQ(14U, result.logDigestSegmentLen);
    {
        LogDigest ld(result.logDigestBuffer.get(), result.logDigestBytes);
        EXPECT_EQ(1U, ld.size());
        EXPECT_EQ(0x3f17c2451f0cafUL, ld[0]);
    }

    auto* replica = backup->findBackupReplica({99, 0}, 88);
    // Make 88 look like it was actually closed.
    replica->rightmostWrittenOffset = ~0u;

    // add a newer Segment and check that we get its LogDigest instead.
    openSegment(ServerId(99, 0), 89);
    writeDigestedSegment(ServerId(99, 0), 89, { 0x5d8ec445d537e15UL });

    result = BackupClient::startReadingData(&context, backupId, ServerId(99, 0),
                                            &tablets);
    EXPECT_EQ(12U, result.logDigestBytes);
    EXPECT_EQ(89U, result.logDigestSegmentId);
    EXPECT_EQ(14U, result.logDigestSegmentLen);
    {
        LogDigest ld(result.logDigestBuffer.get(), result.logDigestBytes);
        EXPECT_EQ(1U, ld.size());
        EXPECT_EQ(0x5d8ec445d537e15UL, ld[0]);
    }
}

TEST_F(BackupServiceTest, startReadingData_logDigest_latest) {
    openSegment(ServerId(99, 0), 88);
    writeDigestedSegment(ServerId(99, 0), 88, { 0x39e874a1e85fcUL });

    openSegment(ServerId(99, 0), 89);
    writeDigestedSegment(ServerId(99, 0), 89, { 0xbe5fbc1e62af6UL });

    // close the new one. we should get the old one now.
    closeSegment(ServerId(99, 0), 89);
    {
        ProtoBuf::Tablets tablets;
        StartReadingDataRpc::Result result =
            BackupClient::startReadingData(&context, backupId, ServerId(99, 0),
                                           &tablets);
        EXPECT_EQ(88U, result.logDigestSegmentId);
        EXPECT_EQ(14U, result.logDigestSegmentLen);
        EXPECT_EQ(12U, result.logDigestBytes);
        LogDigest ld(result.logDigestBuffer.get(), result.logDigestBytes);
        EXPECT_EQ(1U, ld.size());
        EXPECT_EQ(0x39e874a1e85fcUL, ld[0]);
    }
}

TEST_F(BackupServiceTest, startReadingData_logDigest_none) {
    // closed segments don't count.
    openSegment(ServerId(99, 0), 88);
    writeDigestedSegment(ServerId(99, 0), 88, { 0xe966e17be4aUL });

    closeSegment(ServerId(99, 0), 88);
    {
        ProtoBuf::Tablets tablets;
        StartReadingDataRpc::Result result =
            BackupClient::startReadingData(&context, backupId, ServerId(99, 0),
                                           &tablets);
        EXPECT_EQ(1U, result.segmentIdAndLength.size());
        EXPECT_EQ(0U, result.logDigestBytes);
        EXPECT_TRUE(NULL == result.logDigestBuffer);
    }
}

TEST_F(BackupServiceTest, startReadingData_atomic) {
    // Open segments being replicated atomically shouldn't be
    // part of recoveries.
    openSegment(ServerId(99, 0), 88);
    writeDigestedSegment(ServerId(99, 0), 88, { 0xe966e17be4aUL }, true);

    {
        ProtoBuf::Tablets tablets;
        StartReadingDataRpc::Result result =
            BackupClient::startReadingData(&context, backupId, ServerId(99, 0),
                                           &tablets);
        BackupReplica &replica =
            *backup->findBackupReplica(ServerId(99, 0), 88);
        EXPECT_FALSE(replica.satisfiesAtomicReplicationGuarantees());
        EXPECT_EQ(0U, result.segmentIdAndLength.size());
        EXPECT_EQ(0U, result.logDigestBytes);
        EXPECT_TRUE(NULL == result.logDigestBuffer);
    }

    // Once atomic replicas close they should instantly be part of
    // recoveries.
    closeSegment(ServerId(99, 0), 88);
    {
        ProtoBuf::Tablets tablets;
        StartReadingDataRpc::Result result =
            BackupClient::startReadingData(&context, backupId, ServerId(99, 0),
                                           &tablets);
        BackupReplica &replica =
            *backup->findBackupReplica(ServerId(99, 0), 88);
        EXPECT_TRUE(replica.satisfiesAtomicReplicationGuarantees());
        EXPECT_EQ(1U, result.segmentIdAndLength.size());
        EXPECT_EQ(0U, result.logDigestBytes);
        EXPECT_TRUE(NULL == result.logDigestBuffer);
    }
}

TEST_F(BackupServiceTest, writeSegment) {
    openSegment(ServerId(99, 0), 88);
    // test for idempotence
    for (int i = 0; i < 2; ++i) {
        writeRawString({99, 0}, 88, 10, "test");
        BackupReplica &replica =
            *backup->findBackupReplica(ServerId(99, 0), 88);
        EXPECT_TRUE(NULL != replica.segment);
        EXPECT_STREQ("test", &replica.segment[10]);
        EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
    }
}

TEST_F(BackupServiceTest, writeSegment_response) {
    uint64_t groupId = 100;
    const uint32_t numReplicas = 3;
    ServerId ids[numReplicas] = {ServerId(15), ServerId(16), ServerId(33)};
    BackupClient::assignGroup(&context, backupId, groupId, numReplicas, ids);
    const vector<ServerId> group =
        openSegment(ServerId(99, 0), 88);
    EXPECT_EQ(3U, group.size());
    EXPECT_EQ(15U, group.at(0).getId());
    EXPECT_EQ(16U, group.at(1).getId());
    EXPECT_EQ(33U, group.at(2).getId());
    ServerId newIds[1] = {ServerId(99)};
    BackupClient::assignGroup(&context, backupId, 0, 1, newIds);
    const vector<ServerId> newGroup =
        openSegment(ServerId(99, 0), 88);
    EXPECT_EQ(1U, newGroup.size());
    EXPECT_EQ(99U, newGroup.at(0).getId());
}

TEST_F(BackupServiceTest, writeSegment_segmentNotOpen) {
    EXPECT_THROW(
        writeRawString({99, 0}, 88, 10, "test"),
        BackupBadSegmentIdException);
}

TEST_F(BackupServiceTest, writeSegment_segmentClosed) {
    openSegment(ServerId(99, 0), 88);
    closeSegment(ServerId(99, 0), 88);
    EXPECT_THROW(
        writeRawString({99, 0}, 88, 10, "test"),
        BackupBadSegmentIdException);
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, writeSegment_segmentClosedRedundantClosingWrite) {
    openSegment(ServerId(99, 0), 88);
    closeSegment(ServerId(99, 0), 88);
    writeRawString({99, 0}, 88, 10, "test", WireFormat::BackupWrite::CLOSE);
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, writeSegment_badOffset) {
    openSegment(ServerId(99, 0), 88);
    EXPECT_THROW(
        writeRawString({99, 0}, 88, 500000, "test"),
        BackupSegmentOverflowException);
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, writeSegment_badLength) {
    openSegment(ServerId(99, 0), 88);
    uint32_t length = config.segmentSize + 1;
    ASSERT_TRUE(Segment::DEFAULT_SEGMENT_SIZE >= length);
    Segment segment;
    EXPECT_THROW(
        BackupClient::writeSegment(&context, backupId, ServerId(99, 0),
                                   88, &segment, 0, length, {}),
        BackupSegmentOverflowException);
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, writeSegment_badOffsetPlusLength) {
    openSegment(ServerId(99, 0), 88);
    uint32_t length = config.segmentSize;
    ASSERT_TRUE(Segment::DEFAULT_SEGMENT_SIZE >= length);
    Segment segment;
    EXPECT_THROW(
        BackupClient::writeSegment(&context, backupId, ServerId(99, 0),
                                   88, &segment, 1, length, {}),
        BackupSegmentOverflowException);
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, writeSegment_closeSegment) {
    openSegment(ServerId(99, 0), 88);
    writeRawString({99, 0}, 88, 10, "test");
    // loop to test for idempotence
    for (int i = 0; i > 2; ++i) {
        closeSegment(ServerId(99, 0), 88);
        BackupReplica &replica =
            *backup->findBackupReplica(ServerId(99, 0), 88);
        char* storageAddress =
            static_cast<InMemoryStorage::Handle*>(replica.storageHandle)->
                getAddress();
        {
            BackupReplica::Lock lock(replica.mutex);
            while (replica.segment)
                replica.condition.wait(lock);
        }
        EXPECT_TRUE(NULL != storageAddress);
        EXPECT_EQ("test", &storageAddress[10]);
        EXPECT_TRUE(NULL == static_cast<void*>(replica.segment));
        EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
    }
}

TEST_F(BackupServiceTest, writeSegment_closeSegmentSegmentNotOpen) {
    EXPECT_THROW(closeSegment(ServerId(99, 0), 88),
                            BackupBadSegmentIdException);
}

TEST_F(BackupServiceTest, writeSegment_openSegment) {
    // loop to test for idempotence
    for (int i = 0; i < 2; ++i) {
        openSegment(ServerId(99, 0), 88);
        BackupReplica &replica =
            *backup->findBackupReplica(ServerId(99, 0), 88);
        EXPECT_TRUE(NULL != replica.segment);
        EXPECT_EQ(0, *replica.segment);
        EXPECT_TRUE(replica.primary);
        char* address =
            static_cast<InMemoryStorage::Handle*>(replica.storageHandle)->
                getAddress();
        EXPECT_TRUE(NULL != address);
        EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
    }
}

TEST_F(BackupServiceTest, writeSegment_openSegmentSecondary) {
    openSegment(ServerId(99, 0), 88, false);
    BackupReplica &replica =
        *backup->findBackupReplica(ServerId(99, 0), 88);
    EXPECT_TRUE(!replica.primary);
}

TEST_F(BackupServiceTest, writeSegment_openSegmentOutOfStorage) {
    openSegment(ServerId(99, 0), 85);
    openSegment(ServerId(99, 0), 86);
    openSegment(ServerId(99, 0), 87);
    openSegment(ServerId(99, 0), 88);
    openSegment(ServerId(99, 0), 89);
    EXPECT_THROW(
        openSegment(ServerId(99, 0), 90),
        BackupOpenRejectedException);
    EXPECT_EQ(5, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, writeSegment_atomic) {
    openSegment(ServerId(99, 0), 88, true, false);
    BackupReplica &replica =
        *backup->findBackupReplica(ServerId(99, 0), 88);
    EXPECT_FALSE(replica.replicateAtomically);
    EXPECT_TRUE(replica.satisfiesAtomicReplicationGuarantees());
    writeRawString({99, 0}, 88, 10, "test",
        WireFormat::BackupWrite::NONE, true);
    EXPECT_TRUE(replica.replicateAtomically);
    EXPECT_FALSE(replica.satisfiesAtomicReplicationGuarantees());
    writeRawString({99, 0}, 88, 15, "test",
        WireFormat::BackupWrite::CLOSE, true);
    EXPECT_TRUE(replica.replicateAtomically);
    EXPECT_TRUE(replica.satisfiesAtomicReplicationGuarantees());
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, writeSegment_disallowOnReplicasFromStorage) {
    openSegment({99, 0}, 88);
    writeRawString({99, 0}, 88, 10, "test");
    BackupReplica &replica = *backup->findBackupReplica({99, 0}, 88);

    openSegment({99, 0}, 88);
    replica.createdByCurrentProcess = false;

    EXPECT_THROW(openSegment({99, 0}, 88),
                 BackupOpenRejectedException);
    EXPECT_THROW(writeRawString({99, 0}, 88, 10, "test"),
                 BackupBadSegmentIdException);
}

TEST_F(BackupServiceTest, GarbageCollectDownServerTask) {
    openSegment({99, 0}, 88);
    openSegment({99, 0}, 89);
    openSegment({99, 1}, 88);

    EXPECT_TRUE(backup->findBackupReplica({99, 0}, 88));
    EXPECT_TRUE(backup->findBackupReplica({99, 0}, 89));
    EXPECT_TRUE(backup->findBackupReplica({99, 1}, 88));

    typedef BackupService::GarbageCollectDownServerTask Task;
    std::unique_ptr<Task> task(new Task(*backup, {99, 0}));
    task->schedule();
    const_cast<ServerConfig&>(backup->config).backup.gc = true;

    backup->gcTaskQueue.performTask();
    EXPECT_FALSE(backup->findBackupReplica({99, 0}, 88));
    EXPECT_TRUE(backup->findBackupReplica({99, 0}, 89));
    EXPECT_TRUE(backup->findBackupReplica({99, 1}, 88));

    backup->gcTaskQueue.performTask();
    EXPECT_FALSE(backup->findBackupReplica({99, 0}, 88));
    EXPECT_FALSE(backup->findBackupReplica({99, 0}, 89));
    EXPECT_TRUE(backup->findBackupReplica({99, 1}, 88));

    backup->gcTaskQueue.performTask();
    EXPECT_FALSE(backup->findBackupReplica({99, 0}, 88));
    EXPECT_FALSE(backup->findBackupReplica({99, 0}, 89));
    EXPECT_TRUE(backup->findBackupReplica({99, 1}, 88));

    task.release();
}

namespace {
class GcMockMasterService : public Service {
    void dispatch(WireFormat::Opcode opcode, Rpc& rpc) {
        const WireFormat::RequestCommon* hdr =
            rpc.requestPayload.getStart<WireFormat::RequestCommon>();
        switch (hdr->service) {
        case WireFormat::MEMBERSHIP_SERVICE:
            switch (opcode) {
            case WireFormat::Opcode::GET_SERVER_ID:
            {
                auto* resp = new(&rpc.replyPayload, APPEND)
                    WireFormat::GetServerId::Response();
                resp->serverId = ServerId(13, 0).getId();
                resp->common.status = STATUS_OK;
                break;
            }
            default:
                FAIL();
                break;
            }
            break;
        case WireFormat::MASTER_SERVICE:
            switch (hdr->opcode) {
            case WireFormat::Opcode::IS_REPLICA_NEEDED:
            {
                const WireFormat::IsReplicaNeeded::Request* req =
                    rpc.requestPayload.getStart<
                    WireFormat::IsReplicaNeeded::Request>();
                auto* resp =
                    new(&rpc.replyPayload, APPEND)
                        WireFormat::IsReplicaNeeded::Response();
                resp->needed = req->segmentId % 2;
                resp->common.status = STATUS_OK;
                break;
            }
            default:
                FAIL();
                break;
            }
            break;
        default:
            FAIL();
            break;
        }
    }
};
};

TEST_F(BackupServiceTest, GarbageCollectReplicaFoundOnStorageTask) {
    GcMockMasterService master;
    cluster->transport.addService(master, "mock:host=m",
                                  WireFormat::MEMBERSHIP_SERVICE);
    cluster->transport.addService(master, "mock:host=m",
                                  WireFormat::MASTER_SERVICE);
    ServerList* backupServerList = static_cast<ServerList*>(
        backup->context->serverList);
    backupServerList->add({13, 0}, "mock:host=m", {}, 100);
    serverList.add({13, 0}, "mock:host=m", {}, 100);

    openSegment({13, 0}, 10);
    closeSegment({13, 0}, 10);
    backup->findBackupReplica({13, 0}, 10)->createdByCurrentProcess = false;
    openSegment({13, 0}, 11);
    closeSegment({13, 0}, 11);
    backup->findBackupReplica({13, 0}, 11)->createdByCurrentProcess = false;
    openSegment({13, 0}, 12);
    closeSegment({13, 0}, 12);
    backup->findBackupReplica({13, 0}, 12)->createdByCurrentProcess = false;

    typedef BackupService::GarbageCollectReplicasFoundOnStorageTask Task;
    std::unique_ptr<Task> task(new Task(*backup, {13, 0}));
    task->addSegmentId(10);
    task->addSegmentId(11);
    task->addSegmentId(12);
    task->schedule();
    const_cast<ServerConfig&>(backup->config).backup.gc = true;

    EXPECT_FALSE(task->rpc);
    backup->gcTaskQueue.performTask(); // send rpc to probe 10
    ASSERT_TRUE(task->rpc);

    TestLog::Enable _;
    backup->gcTaskQueue.performTask(); // get response - false for 10
    EXPECT_FALSE(task->rpc);
    EXPECT_TRUE(StringUtil::contains(TestLog::get(),
        "tryToFreeReplica: Server has recovered from lost replica; "
        "freeing replica for <13.0,10>"));
    EXPECT_EQ(1lu, backup->gcTaskQueue.outstandingTasks());
    EXPECT_FALSE(backup->findBackupReplica({13, 0}, 10));
    EXPECT_TRUE(backup->findBackupReplica({13, 0}, 11));
    EXPECT_TRUE(backup->findBackupReplica({13, 0}, 12));

    EXPECT_FALSE(task->rpc);
    backup->gcTaskQueue.performTask(); // send rpc to probe 11
    ASSERT_TRUE(task->rpc);

    TestLog::reset();
    backup->gcTaskQueue.performTask(); // get response - true for 11
    EXPECT_TRUE(StringUtil::contains(TestLog::get(),
        "tryToFreeReplica: Server has not recovered from lost replica; "
        "retaining replica for <13.0,11>; "
        "will probe replica status again later"));
    EXPECT_EQ(1lu, backup->gcTaskQueue.outstandingTasks());

    backupServerList->crashed({13, 0}, "mock:host=m", {}, 100);

    TestLog::reset();
    EXPECT_FALSE(task->rpc);
    backup->gcTaskQueue.performTask(); // find out server crashed
    EXPECT_TRUE(StringUtil::contains(TestLog::get(),
        "tryToFreeReplica: Server 13.0 marked crashed; "
        "waiting for cluster to recover from its failure "
        "before freeing <13.0,11>"));
    EXPECT_EQ(1lu, backup->gcTaskQueue.outstandingTasks());

    backupServerList->remove({13, 0});

    TestLog::reset();
    EXPECT_FALSE(task->rpc);
    backup->gcTaskQueue.performTask(); // send rpc
    EXPECT_TRUE(task->rpc);
    backup->gcTaskQueue.performTask(); // get response - server doesn't exist
    EXPECT_TRUE(StringUtil::contains(TestLog::get(),
        "tryToFreeReplica: Server 13.0 marked down; cluster has recovered from "
            "its failure | "
        "tryToFreeReplica: Server has recovered from lost replica; "
            "freeing replica for <13.0,12>"));
    EXPECT_EQ(1lu, backup->gcTaskQueue.outstandingTasks());

    // Final perform finds no segments to free and just cleans up
    backup->gcTaskQueue.performTask();
    EXPECT_EQ(0lu, backup->gcTaskQueue.outstandingTasks());
    task.release();
}

static bool
taskScheduleFilter(string s)
{
    return s != "schedule";
}

TEST_F(BackupServiceTest, GarbageCollectReplicaFoundOnStorageTask_freedFirst) {
    typedef BackupService::GarbageCollectReplicasFoundOnStorageTask Task;
    std::unique_ptr<Task> task(new Task(*backup, {99, 0}));
    task->addSegmentId(88);
    task->schedule();
    const_cast<ServerConfig&>(backup->config).backup.gc = true;

    TestLog::Enable _(taskScheduleFilter);
    backup->gcTaskQueue.performTask();
    EXPECT_EQ("", TestLog::get());

    // Final perform finds no segments to free and just cleans up
    backup->gcTaskQueue.performTask();
    EXPECT_EQ(0lu, backup->gcTaskQueue.outstandingTasks());
    task.release();
}

TEST_F(BackupServiceTest, trackerChangesEnqueued) {
    backup->testingDoNotStartGcThread = true;
    backup->gcTracker.enqueueChange({{99, 0}, "", {}, 0, ServerStatus::UP},
                                    SERVER_ADDED);
    backup->trackerChangesEnqueued();
    EXPECT_EQ(0lu, backup->gcTaskQueue.outstandingTasks());

    backup->gcTracker.enqueueChange({{99, 0}, "", {}, 0, ServerStatus::CRASHED},
                                    SERVER_CRASHED);
    backup->trackerChangesEnqueued();
    EXPECT_EQ(0lu, backup->gcTaskQueue.outstandingTasks());

    backup->gcTracker.enqueueChange({{99, 0}, "", {}, 0, ServerStatus::DOWN},
                                    SERVER_REMOVED);
    backup->gcTracker.enqueueChange({{98, 0}, "", {}, 0, ServerStatus::UP},
                                    SERVER_ADDED);
    backup->gcTracker.enqueueChange({{98, 0}, "", {}, 0, ServerStatus::DOWN},
                                    SERVER_REMOVED);
    backup->trackerChangesEnqueued();
    EXPECT_EQ(2lu, backup->gcTaskQueue.outstandingTasks());
    backup->gcTaskQueue.performTask();
    backup->gcTaskQueue.performTask();
    EXPECT_EQ(0lu, backup->gcTaskQueue.outstandingTasks());
}

} // namespace RAMCloud
