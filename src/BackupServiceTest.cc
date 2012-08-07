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
#include "BackupService.h"
#include "Log.h"
#include "MockCluster.h"
#include "RecoverySegmentIterator.h"
#include "Server.h"
#include "ShortMacros.h"
#include "StringUtil.h"
#include "KeyHash.h"

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
    uint32_t scratchSegBufSize;
    Memory::unique_ptr_free scratchSegBuf;

    BackupServiceTest()
        : context()
        , config(ServerConfig::forTesting())
        , cluster()
        , server()
        , backup()
        , oldUmask(umask(0))
        , serverList(context)
        , backupId(5, 0)
        , scratchSegBufSize(1024 * 1024)
        , scratchSegBuf(Memory::unique_ptr_free(
                            Memory::xmemalign(HERE,
                                              scratchSegBufSize,
                                              scratchSegBufSize),
                            std::free))
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        memset(scratchSegBuf.get(), 0, scratchSegBufSize);

        cluster.construct(context);
        config.services = {WireFormat::BACKUP_SERVICE};
        config.backup.numSegmentFrames = 5;
        server = cluster->addServer(config);
        backup = server->backup.get();

        context.serverList->add(backupId, server->config.localLocator,
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
        Segment segment(masterId.getId(), segmentId,
                        scratchSegBuf.get(), scratchSegBufSize);
        BackupClient::writeSegment(context, backupId, masterId,
                                   &segment, 0, 0, {},
                                   WireFormat::BackupWrite::CLOSE, false);
    }

    vector<ServerId>
    openSegment(ServerId masterId, uint64_t segmentId, bool primary = true,
                bool atomic = false)
    {
        Segment segment(masterId.getId(), segmentId,
                        scratchSegBuf.get(), scratchSegBufSize);
        auto flags = primary ? WireFormat::BackupWrite::OPENPRIMARY
                             : WireFormat::BackupWrite::OPEN;
        return BackupClient::writeSegment(context, backupId, masterId,
                                          &segment, 0, 0, {}, flags, atomic);
    }

    uint32_t
    writeString(ServerId masterId, uint64_t segmentId,
                uint32_t offset, const string& s,
                WireFormat::BackupWrite::Flags flags =
                    WireFormat::BackupWrite::NONE,
                bool atomic = false)
    {
        Segment segment(masterId.getId(), segmentId,
                        scratchSegBuf.get(), scratchSegBufSize);
        char* buf = static_cast<char*>(scratchSegBuf.get());
        memcpy(buf + offset, s.c_str(), s.length());
        BackupClient::writeSegment(context, backupId, masterId, &segment,
                                   offset, uint32_t(s.length()), {},
                                   flags, atomic);
        return uint32_t(s.length());
    }

    uint32_t
    writeEntry(ServerId masterId, uint64_t segmentId, LogEntryType type,
               uint32_t offset, const void *data, uint32_t bytes)
    {
        Segment segment(masterId.getId(), segmentId,
                        scratchSegBuf.get(), scratchSegBufSize);
        char* buf = static_cast<char*>(scratchSegBuf.get());
        SegmentEntry entry(type, bytes);
        memcpy(buf + offset, &entry, sizeof(entry));
        memcpy(buf + offset + sizeof(entry), data, bytes);
        BackupClient::writeSegment(context, backupId, masterId, &segment,
                                   offset, uint32_t(sizeof(entry) + bytes),
                                   {});
        return uint32_t(sizeof(entry) + bytes);
    }

    uint32_t
    writeObject(ServerId masterId, uint64_t segmentId,
                uint32_t offset, const char *data, uint32_t bytes,
                uint64_t tableId, const char* key, uint16_t keyLength)
    {
        char objectMem[sizeof(Object) + keyLength + bytes];
        Object* obj = reinterpret_cast<Object*>(objectMem);
        memset(obj, 'A', sizeof(*obj));
        obj->keyLength = keyLength;
        obj->tableId = tableId;
        obj->version = 0;
        memcpy(objectMem + sizeof(*obj), key, keyLength);
        memcpy(objectMem + sizeof(*obj) + keyLength, data, bytes);
        return writeEntry(masterId, segmentId, LOG_ENTRY_TYPE_OBJ,
                          offset, objectMem, obj->objectLength(bytes));
    }

    uint32_t
    writeTombstone(ServerId masterId, uint64_t segmentId,
                   uint32_t offset, uint64_t tableId,
                   const char* key, uint16_t keyLength)
    {
        char tombMem[sizeof(ObjectTombstone) + keyLength];
        ObjectTombstone* tomb =
            reinterpret_cast<ObjectTombstone*>(tombMem);
        memset(tomb, 'A', sizeof(*tomb));
        tomb->keyLength = keyLength;
        tomb->tableId = tableId;
        tomb->objectVersion = 0;
        memcpy(tombMem + sizeof(*tomb), key, keyLength);
        return writeEntry(masterId, segmentId, LOG_ENTRY_TYPE_OBJTOMB,
                          offset, tombMem, tomb->tombLength());
    }

    uint32_t
    writeHeader(ServerId masterId, uint64_t segmentId)
    {
        SegmentHeader header;
        header.logId = *masterId;
        header.segmentId = segmentId;
        header.segmentCapacity = config.segmentSize;
        return writeEntry(masterId, segmentId, LOG_ENTRY_TYPE_SEGHEADER, 0,
                          &header, sizeof(header));
    }

    uint32_t
    writeFooter(ServerId masterId, uint64_t segmentId, uint32_t offset)
    {
        Segment segment(masterId.getId(), segmentId,
                        scratchSegBuf.get(), scratchSegBufSize);
        SegmentFooterEntry footerEntry(0x1234abcdu);
        BackupClient::writeSegment(context, backupId, masterId, &segment,
                                   offset, 0, &footerEntry);
        return offset + downCast<uint32_t>(sizeof(footerEntry));
    }

    void
    writeFooterAtEnd(ServerId masterId, uint64_t segmentId)
    {
        const uint32_t offset = config.segmentSize -
                                downCast<uint32_t>(sizeof(SegmentEntry)) -
                                downCast<uint32_t>(sizeof(SegmentFooter));
        assert(writeFooter(masterId, segmentId, offset) == config.segmentSize);
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
            getKeyHash("9", 1), getKeyHash("9", 1), 0, 0);
        appendTablet(tablets, 0, 123,
            getKeyHash("10", 2), getKeyHash("10", 2), 0, 0);
        appendTablet(tablets, 0, 123,
            getKeyHash("29", 2), getKeyHash("29", 2), 0, 0);

        appendTablet(tablets, 0, 124,
            getKeyHash("20", 2), getKeyHash("20", 2), 0, 0);

        // partition 1
        appendTablet(tablets, 1, 123,
            getKeyHash("30", 2), getKeyHash("30", 2), 0, 0);
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
        void* segBuf = Memory::xmemalign(HERE, 1024 * 1024, 1024 * 1024);
        Segment s((uint64_t)0, segmentId, segBuf, 1024 * 1024);

        char digestBuf[LogDigest::getBytesFromCount
                            (downCast<uint32_t>(digestIds.size()))];
        LogDigest src(downCast<uint32_t>(digestIds.size()),
                        digestBuf,
                        downCast<uint32_t>(sizeof(digestBuf)));
        for (uint32_t i = 0; i < digestIds.size(); i++)
            src.addSegment(digestIds[i]);

        SegmentEntryHandle seh = s.append(LOG_ENTRY_TYPE_LOGDIGEST,
            digestBuf, downCast<uint32_t>(sizeof(digestBuf)));
        ASSERT_TRUE(seh);
        auto committed = s.getCommittedLength();
        BackupClient::writeSegment(context, backupId, masterId,
                                   &s, 0, committed.first, &committed.second,
                                   WireFormat::BackupWrite::NONE, atomic);

        free(segBuf);
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
        "init: My server ID is 3 | "
        "init: Backup 3 will store replicas under cluster name '__unnamed__'"
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
        "init: My server ID is 3 | "
        "init: Backup 3 will store replicas under cluster name 'testing'"
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
            "server 2 which left replicas behind on disk | "
        "init: My server ID is 4294967298 | "
        "init: Backup 4294967298 will store replicas under cluster name "
            "'testing'"
        , TestLog::get());
}

TEST_F(BackupServiceTest, findSegmentInfo) {
    EXPECT_TRUE(NULL == backup->findSegmentInfo(ServerId(99, 0), 88));
    openSegment(ServerId(99, 0), 88);
    closeSegment(ServerId(99, 0), 88);
    BackupService::SegmentInfo* infop =
        backup->findSegmentInfo(ServerId(99, 0), 88);
    EXPECT_TRUE(infop != NULL);
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, findSegmentInfo_notIn) {
    EXPECT_TRUE(NULL == backup->findSegmentInfo(ServerId(99, 0), 88));
}

TEST_F(BackupServiceTest, assignGroup) {
    uint64_t groupId = 100;
    const uint32_t numReplicas = 3;
    ServerId ids[numReplicas] = {ServerId(15), ServerId(16), ServerId(99)};
    BackupClient::assignGroup(context, backupId, groupId, numReplicas, ids);
    EXPECT_EQ(groupId, backup->replicationId);
    EXPECT_EQ(15U, backup->replicationGroup.at(0).getId());
    EXPECT_EQ(16U, backup->replicationGroup.at(1).getId());
    EXPECT_EQ(99U, backup->replicationGroup.at(2).getId());
    ids[0] = ServerId(33);
    ids[1] = ServerId(22);
    ids[2] = ServerId(11);
    BackupClient::assignGroup(context, backupId, groupId, numReplicas, ids);
    EXPECT_EQ(3U, backup->replicationGroup.size());
    EXPECT_EQ(33U, backup->replicationGroup.at(0).getId());
}

TEST_F(BackupServiceTest, freeSegment) {
    openSegment({99, 0}, 88);
    closeSegment({99, 0}, 88);
    {
        TestLog::Enable _(&inMemoryStorageFreePred);
        BackupClient::freeSegment(context, backupId, ServerId(99, 0), 88);
        EXPECT_EQ("free: called", TestLog::get());
    }
    EXPECT_TRUE(NULL == backup->findSegmentInfo(ServerId(99, 0), 88));
    BackupClient::freeSegment(context, backupId, ServerId(99, 0), 88);
    EXPECT_TRUE(NULL == backup->findSegmentInfo(ServerId(99, 0), 88));
}

TEST_F(BackupServiceTest, freeSegment_stillOpen) {
    openSegment(ServerId(99, 0), 88);
    BackupClient::freeSegment(context, backupId, ServerId(99, 0), 88);
    EXPECT_TRUE(NULL == backup->findSegmentInfo(ServerId(99, 0), 88));
}

TEST_F(BackupServiceTest, getRecoveryData) {
    ProtoBuf::Tablets tablets;
    createTabletList(tablets);

    uint32_t offset = 0;
    openSegment(ServerId(99, 0), 88);
    offset = writeHeader(ServerId(99, 0), 88);
    // Objects
    // Barely in tablet
    offset +=
        writeObject(ServerId(99, 0), 88, offset, "test1", 6, 123, "29", 2);
    // Barely out of tablets
    offset +=
        writeObject(ServerId(99, 0), 88, offset, "test2", 6, 123, "30", 2);
    // In on other table
    offset +=
        writeObject(ServerId(99, 0), 88, offset, "test3", 6, 124, "20", 2);
    // Not in any table
    offset +=
        writeObject(ServerId(99, 0), 88, offset, "test4", 6, 125, "20", 2);
    // Tombstones
    // Barely in tablet
    offset += writeTombstone(ServerId(99, 0), 88, offset, 123, "29", 2);
    // Barely out of tablets
    offset += writeTombstone(ServerId(99, 0), 88, offset, 123, "30", 2);
    // In on other table
    offset += writeTombstone(ServerId(99, 0), 88, offset, 124, "20", 2);
    // Not in any table
    offset += writeTombstone(ServerId(99, 0), 88, offset, 125, "20", 2);
    offset += writeFooter(ServerId(99, 0), 88, offset);
    closeSegment(ServerId(99, 0), 88);
    BackupClient::startReadingData(context, backupId, ServerId(99, 0), tablets);

    Buffer response;
    BackupClient::getRecoveryData(context, backupId, ServerId(99, 0),
                                  88, 0, response);

    RecoverySegmentIterator it(
        response.getRange(0, response.getTotalLength()),
        response.getTotalLength());

    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
    EXPECT_EQ(123U, it.get<Object>()->tableId);
    EXPECT_EQ("29", TestUtil::toString(it.get<Object>()->getKey(),
                                       it.get<Object>()->keyLength));
    it.next();

    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
    EXPECT_EQ(124U, it.get<Object>()->tableId);
    EXPECT_EQ("20", TestUtil::toString(it.get<Object>()->getKey(),
                                       it.get<Object>()->keyLength));
    it.next();

    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJTOMB, it.getType());
    EXPECT_EQ(123U, it.get<ObjectTombstone>()->tableId);
    EXPECT_EQ("29",
              TestUtil::toString(it.get<ObjectTombstone>()->getKey(),
                                 it.get<ObjectTombstone>()->keyLength));
    it.next();

    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJTOMB, it.getType());
    EXPECT_EQ(124U, it.get<ObjectTombstone>()->tableId);
    EXPECT_EQ("20",
              TestUtil::toString(it.get<ObjectTombstone>()->getKey(),
                                 it.get<ObjectTombstone>()->keyLength));
    it.next();

    EXPECT_TRUE(it.isDone());
}

TEST_F(BackupServiceTest, getRecoveryData_moreThanOneSegmentStored) {
    uint32_t offset = 0;
    openSegment(ServerId(99, 0), 87);
    offset = writeHeader(ServerId(99, 0), 87);
    offset +=
        writeObject(ServerId(99, 0), 87, offset, "test1", 6, 123, "9", 1);
    offset += writeFooter(ServerId(99, 0), 87, offset);
    closeSegment(ServerId(99, 0), 87);

    openSegment(ServerId(99, 0), 88);
    offset = writeHeader(ServerId(99, 0), 88);
    offset +=
        writeObject(ServerId(99, 0), 88, offset, "test2", 6, 123, "10", 2);
    offset += writeFooter(ServerId(99, 0), 88, offset);
    closeSegment(ServerId(99, 0), 88);

    ProtoBuf::Tablets tablets;
    createTabletList(tablets);

    BackupClient::startReadingData(context, backupId, ServerId(99, 0),
                                   tablets);

    {
        Buffer response;
        BackupClient::getRecoveryData(context, backupId, ServerId(99, 0),
                                      88, 0, response);

        RecoverySegmentIterator it(
            response.getRange(0, response.getTotalLength()),
            response.getTotalLength());
        EXPECT_FALSE(it.isDone());
        EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
        EXPECT_STREQ("test2", static_cast<const Object*>(
                              it.getPointer())->getData());
        it.next();
        EXPECT_TRUE(it.isDone());
    }
    {
        Buffer response;
        BackupClient::getRecoveryData(context, backupId, ServerId(99, 0),
                                      87, 0, response);

        RecoverySegmentIterator it(
            response.getRange(0, response.getTotalLength()),
            response.getTotalLength());
        EXPECT_FALSE(it.isDone());
        EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
        EXPECT_STREQ("test1", static_cast<const Object*>(
                              it.getPointer())->getData());
        it.next();
        EXPECT_TRUE(it.isDone());
    }

    BackupClient::freeSegment(context, backupId, ServerId(99, 0), 87);
    BackupClient::freeSegment(context, backupId, ServerId(99, 0), 88);
}

TEST_F(BackupServiceTest, getRecoveryData_malformedSegment) {
    openSegment(ServerId(99, 0), 88);
    closeSegment(ServerId(99, 0), 88);

    BackupClient::startReadingData(context, backupId, ServerId(99, 0),
                                   ProtoBuf::Tablets());

    while (true) {
        Buffer response;
        EXPECT_THROW(
            BackupClient::getRecoveryData(context, backupId, ServerId(99, 0),
                                          88, 0, response),
            SegmentRecoveryFailedException);
        break;
    }

    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, getRecoveryData_notRecovered) {
    uint32_t offset = 0;
    openSegment(ServerId(99, 0), 88);
    offset += writeHeader(ServerId(99, 0), 88);
    offset +=
        writeObject(ServerId(99, 0), 88, offset, "test2", 6, 123, "10", 2);
    offset += writeFooter(ServerId(99, 0), 88, offset);
    Buffer response;
    EXPECT_THROW(
        BackupClient::getRecoveryData(context, backupId, ServerId(99, 0),
                                      88, 0, response),
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
    uint32_t offset = 0;
    openSegment(ServerId(99, 0), 87);
    offset = writeHeader(ServerId(99, 0), 87);
    offset +=
        writeObject(ServerId(99, 0), 87, offset, "test1", 6, 123, "9", 1);
    offset += writeFooter(ServerId(99, 0), 87, offset);
    closeSegment(ServerId(99, 0), 87);

    openSegment(ServerId(99, 0), 88);
    offset = writeHeader(ServerId(99, 0), 88);
    offset +=
        writeObject(ServerId(99, 0), 88, offset, "test2", 6, 123, "30", 2);
    offset += writeFooter(ServerId(99, 0), 88, offset);
    closeSegment(ServerId(99, 0), 88);

    vector<BackupService::SegmentInfo*> toBuild;
    auto info = backup->findSegmentInfo(ServerId(99, 0), 87);
    EXPECT_TRUE(NULL != info);
    info->setRecovering();
    info->startLoading();
    toBuild.push_back(info);
    info = backup->findSegmentInfo(ServerId(99, 0), 88);
    EXPECT_TRUE(NULL != info);
    info->setRecovering();
    info->startLoading();
    toBuild.push_back(info);

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);
    Atomic<int> recoveryThreadCount{0};
    BackupService::RecoverySegmentBuilder builder(context,
                                                  toBuild,
                                                  partitions,
                                                  recoveryThreadCount,
                                                  config.segmentSize);
    builder();

    EXPECT_EQ(BackupService::SegmentInfo::RECOVERING,
                            toBuild[0]->state);
    ASSERT_TRUE(toBuild[0]->recoverySegments);
    Buffer* buf = &toBuild[0]->recoverySegments[0];
    ASSERT_TRUE(buf);
    RecoverySegmentIterator it(buf->getRange(0, buf->getTotalLength()),
                                buf->getTotalLength());
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
    EXPECT_STREQ("test1", it.get<Object>()->getData());
    it.next();
    EXPECT_TRUE(it.isDone());

    EXPECT_EQ(BackupService::SegmentInfo::RECOVERING,
              toBuild[1]->state);
    EXPECT_TRUE(NULL != toBuild[1]->recoverySegments);
    buf = &toBuild[1]->recoverySegments[1];
    RecoverySegmentIterator it2(buf->getRange(0, buf->getTotalLength()),
                                buf->getTotalLength());
    EXPECT_FALSE(it2.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it2.getType());
    EXPECT_STREQ("test2", it2.get<Object>()->getData());
    it2.next();
    EXPECT_TRUE(it2.isDone());
}

namespace {
bool restartFilter(string s) {
    return s == "restartFromStorage";
}
}

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
        SegmentEntry headerEntry(LOG_ENTRY_TYPE_SEGHEADER, sizeof(header));
        SegmentFooter footer{0xcafebabe};
        SegmentEntry footerEntry(LOG_ENTRY_TYPE_SEGFOOTER, sizeof(footer));
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

    EXPECT_TRUE(backup->findSegmentInfo({70, 0}, 88));
    EXPECT_TRUE(backup->findSegmentInfo({71, 0}, 89));
    EXPECT_FALSE(backup->findSegmentInfo({70, 0}, 90));
    EXPECT_FALSE(backup->findSegmentInfo({71, 0}, 91));
    EXPECT_TRUE(backup->findSegmentInfo({70, 0}, 92));
    EXPECT_TRUE(backup->findSegmentInfo({71, 0}, 93));

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

TEST_F(BackupServiceTest, startReadingData) {
    MockRandom _(1);
    openSegment(ServerId(99, 0), 88);
    uint32_t offset = writeHeader(ServerId(99, 0), 88);
    openSegment(ServerId(99, 0), 89);
    openSegment(ServerId(99, 0), 98, false);
    openSegment(ServerId(99, 0), 99, false);

    StartReadingDataRpc::Result result =
        BackupClient::startReadingData(context, backupId, ServerId(99, 0),
                                       ProtoBuf::Tablets());
    EXPECT_EQ(4u, result.segmentIdAndLength.size());

    EXPECT_EQ(88U, result.segmentIdAndLength[0].first);
    EXPECT_EQ(offset, result.segmentIdAndLength[0].second);
    {
        BackupService::SegmentInfo& info =
            *backup->findSegmentInfo(ServerId(99, 0), 88);
        BackupService::SegmentInfo::Lock lock(info.mutex);
        EXPECT_EQ(BackupService::SegmentInfo::RECOVERING, info.state);
    }

    EXPECT_EQ(89U, result.segmentIdAndLength[1].first);
    EXPECT_EQ(0U, result.segmentIdAndLength[1].second);
    {
        BackupService::SegmentInfo& info =
            *backup->findSegmentInfo(ServerId(99, 0), 89);
        BackupService::SegmentInfo::Lock lock(info.mutex);
        EXPECT_EQ(BackupService::SegmentInfo::RECOVERING, info.state);
    }

    EXPECT_EQ(98U, result.segmentIdAndLength[2].first);
    EXPECT_EQ(0U, result.segmentIdAndLength[2].second);
    {
        BackupService::SegmentInfo& info =
            *backup->findSegmentInfo(ServerId(99, 0), 98);
        BackupService::SegmentInfo::Lock lock(info.mutex);
        EXPECT_EQ(BackupService::SegmentInfo::RECOVERING, info.state);
        EXPECT_TRUE(info.recoveryPartitions);
    }

    EXPECT_EQ(99U, result.segmentIdAndLength[3].first);
    EXPECT_EQ(0U, result.segmentIdAndLength[3].second);
    EXPECT_TRUE(backup->findSegmentInfo(
        ServerId(99, 0), 99)->recoveryPartitions);
    {
        BackupService::SegmentInfo& info =
            *backup->findSegmentInfo(ServerId(99, 0), 99);
        BackupService::SegmentInfo::Lock lock(info.mutex);
        EXPECT_EQ(BackupService::SegmentInfo::RECOVERING, info.state);
        EXPECT_TRUE(info.recoveryPartitions);
    }

    EXPECT_EQ(4, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, startReadingData_empty) {
    StartReadingDataRpc::Result result =
        BackupClient::startReadingData(context, backupId, ServerId(99, 0),
                                       ProtoBuf::Tablets());
    EXPECT_EQ(0U, result.segmentIdAndLength.size());
    EXPECT_EQ(0U, result.logDigestBytes);
    EXPECT_TRUE(NULL == result.logDigestBuffer);
}

TEST_F(BackupServiceTest, startReadingData_logDigest_simple) {
    // ensure that we get the LogDigest back at all.
    openSegment(ServerId(99, 0), 88);
    writeDigestedSegment(ServerId(99, 0), 88, { 0x3f17c2451f0cafUL });

    StartReadingDataRpc::Result result =
        BackupClient::startReadingData(context, backupId, ServerId(99, 0),
                                       ProtoBuf::Tablets());
    EXPECT_EQ(LogDigest::getBytesFromCount(1),
        result.logDigestBytes);
    EXPECT_EQ(88U, result.logDigestSegmentId);
    EXPECT_EQ(60U, result.logDigestSegmentLen);
    {
        LogDigest ld(result.logDigestBuffer.get(), result.logDigestBytes);
        EXPECT_EQ(1, ld.getSegmentCount());
        EXPECT_EQ(0x3f17c2451f0cafUL, ld.getSegmentIds()[0]);
    }

    // Repeating the call should yield the same digest.
    result = BackupClient::startReadingData(context, backupId, {99, 0},
                                            ProtoBuf::Tablets());
    EXPECT_EQ(LogDigest::getBytesFromCount(1), result.logDigestBytes);
    EXPECT_EQ(88U, result.logDigestSegmentId);
    EXPECT_EQ(60U, result.logDigestSegmentLen);
    {
        LogDigest ld(result.logDigestBuffer.get(), result.logDigestBytes);
        EXPECT_EQ(1, ld.getSegmentCount());
        EXPECT_EQ(0x3f17c2451f0cafUL, ld.getSegmentIds()[0]);
    }

    auto* info = backup->findSegmentInfo({99, 0}, 88);
    // Make 88 look like it was actually closed.
    info->rightmostWrittenOffset = ~0u;

    // add a newer Segment and check that we get its LogDigest instead.
    openSegment(ServerId(99, 0), 89);
    writeDigestedSegment(ServerId(99, 0), 89, { 0x5d8ec445d537e15UL });

    result = BackupClient::startReadingData(context, backupId, ServerId(99, 0),
                                            ProtoBuf::Tablets());
    EXPECT_EQ(LogDigest::getBytesFromCount(1),
        result.logDigestBytes);
    EXPECT_EQ(89U, result.logDigestSegmentId);
    EXPECT_EQ(60U, result.logDigestSegmentLen);
    {
        LogDigest ld(result.logDigestBuffer.get(), result.logDigestBytes);
        EXPECT_EQ(1, ld.getSegmentCount());
        EXPECT_EQ(0x5d8ec445d537e15UL, ld.getSegmentIds()[0]);
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
        StartReadingDataRpc::Result result =
            BackupClient::startReadingData(context, backupId, ServerId(99, 0),
                                           ProtoBuf::Tablets());
        EXPECT_EQ(88U, result.logDigestSegmentId);
        EXPECT_EQ(60U, result.logDigestSegmentLen);
        EXPECT_EQ(LogDigest::getBytesFromCount(1),
            result.logDigestBytes);
        LogDigest ld(result.logDigestBuffer.get(), result.logDigestBytes);
        EXPECT_EQ(1, ld.getSegmentCount());
        EXPECT_EQ(0x39e874a1e85fcUL, ld.getSegmentIds()[0]);
    }
}

TEST_F(BackupServiceTest, startReadingData_logDigest_none) {
    // closed segments don't count.
    openSegment(ServerId(99, 0), 88);
    writeDigestedSegment(ServerId(99, 0), 88, { 0xe966e17be4aUL });

    closeSegment(ServerId(99, 0), 88);
    {
        StartReadingDataRpc::Result result =
            BackupClient::startReadingData(context, backupId, ServerId(99, 0),
                                           ProtoBuf::Tablets());
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
        StartReadingDataRpc::Result result =
            BackupClient::startReadingData(context, backupId, ServerId(99, 0),
                                           ProtoBuf::Tablets());
        BackupService::SegmentInfo &info =
            *backup->findSegmentInfo(ServerId(99, 0), 88);
        EXPECT_FALSE(info.satisfiesAtomicReplicationGuarantees());
        EXPECT_EQ(0U, result.segmentIdAndLength.size());
        EXPECT_EQ(0U, result.logDigestBytes);
        EXPECT_TRUE(NULL == result.logDigestBuffer);
    }

    // Once atomic replicas close they should instantly be part of
    // recoveries.
    closeSegment(ServerId(99, 0), 88);
    {
        StartReadingDataRpc::Result result =
            BackupClient::startReadingData(context, backupId, ServerId(99, 0),
                                           ProtoBuf::Tablets());
        BackupService::SegmentInfo &info =
            *backup->findSegmentInfo(ServerId(99, 0), 88);
        EXPECT_TRUE(info.satisfiesAtomicReplicationGuarantees());
        EXPECT_EQ(1U, result.segmentIdAndLength.size());
        EXPECT_EQ(0U, result.logDigestBytes);
        EXPECT_TRUE(NULL == result.logDigestBuffer);
    }
}

TEST_F(BackupServiceTest, writeSegment) {
    openSegment(ServerId(99, 0), 88);
    // test for idempotence
    for (int i = 0; i < 2; ++i) {
        writeString({99, 0}, 88, 10, "test");
        BackupService::SegmentInfo &info =
            *backup->findSegmentInfo(ServerId(99, 0), 88);
        EXPECT_TRUE(NULL != info.segment);
        EXPECT_STREQ("test", &info.segment[10]);
        EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
    }
}

TEST_F(BackupServiceTest, writeSegment_response) {
    uint64_t groupId = 100;
    const uint32_t numReplicas = 3;
    ServerId ids[numReplicas] = {ServerId(15), ServerId(16), ServerId(33)};
    BackupClient::assignGroup(context, backupId, groupId, numReplicas, ids);
    const vector<ServerId> group =
        openSegment(ServerId(99, 0), 88);
    EXPECT_EQ(3U, group.size());
    EXPECT_EQ(15U, group.at(0).getId());
    EXPECT_EQ(16U, group.at(1).getId());
    EXPECT_EQ(33U, group.at(2).getId());
    ServerId newIds[1] = {ServerId(99)};
    BackupClient::assignGroup(context, backupId, 0, 1, newIds);
    const vector<ServerId> newGroup =
        openSegment(ServerId(99, 0), 88);
    EXPECT_EQ(1U, newGroup.size());
    EXPECT_EQ(99U, newGroup.at(0).getId());
}

TEST_F(BackupServiceTest, writeSegment_segmentNotOpen) {
    EXPECT_THROW(
        writeString({99, 0}, 88, 10, "test"),
        BackupBadSegmentIdException);
}

TEST_F(BackupServiceTest, writeSegment_segmentClosed) {
    openSegment(ServerId(99, 0), 88);
    closeSegment(ServerId(99, 0), 88);
    EXPECT_THROW(
        writeString({99, 0}, 88, 10, "test"),
        BackupBadSegmentIdException);
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, writeSegment_segmentClosedRedundantClosingWrite) {
    openSegment(ServerId(99, 0), 88);
    closeSegment(ServerId(99, 0), 88);
    writeString({99, 0}, 88, 10, "test", WireFormat::BackupWrite::CLOSE);
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, writeSegment_badOffset) {
    openSegment(ServerId(99, 0), 88);
    EXPECT_THROW(
        writeString({99, 0}, 88, 500000, "test"),
        BackupSegmentOverflowException);
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, writeSegment_badLength) {
    openSegment(ServerId(99, 0), 88);
    uint32_t length = config.segmentSize + 1;
    ASSERT_TRUE(scratchSegBufSize >= length);
    Segment segment(99lu, 88,
                    scratchSegBuf.get(), scratchSegBufSize);
    EXPECT_THROW(
        BackupClient::writeSegment(context, backupId, ServerId(99, 0),
                                   &segment, 0, length, {}),
        BackupSegmentOverflowException);
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, writeSegment_badOffsetPlusLength) {
    openSegment(ServerId(99, 0), 88);
    uint32_t length = config.segmentSize;
    ASSERT_TRUE(scratchSegBufSize >= length);
    Segment segment(99lu, 88,
                    scratchSegBuf.get(), scratchSegBufSize);
    EXPECT_THROW(
        BackupClient::writeSegment(context, backupId, ServerId(99, 0),
                                   &segment, 1, length, {}),
        BackupSegmentOverflowException);
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, writeSegment_closeSegment) {
    openSegment(ServerId(99, 0), 88);
    writeString({99, 0}, 88, 10, "test");
    // loop to test for idempotence
    for (int i = 0; i > 2; ++i) {
        closeSegment(ServerId(99, 0), 88);
        BackupService::SegmentInfo &info =
            *backup->findSegmentInfo(ServerId(99, 0), 88);
        char* storageAddress =
            static_cast<InMemoryStorage::Handle*>(info.storageHandle)->
                getAddress();
        {
            BackupService::SegmentInfo::Lock lock(info.mutex);
            while (info.segment)
                info.condition.wait(lock);
        }
        EXPECT_TRUE(NULL != storageAddress);
        EXPECT_EQ("test", &storageAddress[10]);
        EXPECT_TRUE(NULL == static_cast<void*>(info.segment));
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
        BackupService::SegmentInfo &info =
            *backup->findSegmentInfo(ServerId(99, 0), 88);
        EXPECT_TRUE(NULL != info.segment);
        EXPECT_EQ(0, *info.segment);
        EXPECT_TRUE(info.primary);
        char* address =
            static_cast<InMemoryStorage::Handle*>(info.storageHandle)->
                getAddress();
        EXPECT_TRUE(NULL != address);
        EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
    }
}

TEST_F(BackupServiceTest, writeSegment_openSegmentSecondary) {
    openSegment(ServerId(99, 0), 88, false);
    BackupService::SegmentInfo &info =
        *backup->findSegmentInfo(ServerId(99, 0), 88);
    EXPECT_TRUE(!info.primary);
}

TEST_F(BackupServiceTest, writeSegment_openSegmentOutOfStorage) {
    openSegment(ServerId(99, 0), 85);
    openSegment(ServerId(99, 0), 86);
    openSegment(ServerId(99, 0), 87);
    openSegment(ServerId(99, 0), 88);
    openSegment(ServerId(99, 0), 89);
    EXPECT_THROW(
        openSegment(ServerId(99, 0), 90),
        BackupStorageException);
    EXPECT_EQ(5, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, writeSegment_atomic) {
    openSegment(ServerId(99, 0), 88, true, false);
    BackupService::SegmentInfo &info =
        *backup->findSegmentInfo(ServerId(99, 0), 88);
    EXPECT_FALSE(info.replicateAtomically);
    EXPECT_TRUE(info.satisfiesAtomicReplicationGuarantees());
    writeString({99, 0}, 88, 10, "test", WireFormat::BackupWrite::NONE, true);
    EXPECT_TRUE(info.replicateAtomically);
    EXPECT_FALSE(info.satisfiesAtomicReplicationGuarantees());
    writeString({99, 0}, 88, 15, "test", WireFormat::BackupWrite::CLOSE, true);
    EXPECT_TRUE(info.replicateAtomically);
    EXPECT_TRUE(info.satisfiesAtomicReplicationGuarantees());
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, writeSegment_disallowOnReplicasFromStorage) {
    openSegment({99, 0}, 88);
    writeString({99, 0}, 88, 10, "test");
    BackupService::SegmentInfo &info = *backup->findSegmentInfo({99, 0}, 88);

    openSegment({99, 0}, 88);
    info.createdByCurrentProcess = false;

    EXPECT_THROW(openSegment({99, 0}, 88),
                 BackupOpenRejectedException);
    EXPECT_THROW(writeString({99, 0}, 88, 10, "test"),
                 BackupBadSegmentIdException);
}

TEST_F(BackupServiceTest, GarbageCollectDownServerTask) {
    openSegment({99, 0}, 88);
    openSegment({99, 0}, 89);
    openSegment({99, 1}, 88);

    EXPECT_TRUE(backup->findSegmentInfo({99, 0}, 88));
    EXPECT_TRUE(backup->findSegmentInfo({99, 0}, 89));
    EXPECT_TRUE(backup->findSegmentInfo({99, 1}, 88));

    typedef BackupService::GarbageCollectDownServerTask Task;
    std::unique_ptr<Task> task(new Task(*backup, {99, 0}));
    task->schedule();
    const_cast<ServerConfig&>(backup->config).backup.gc = true;

    backup->gcTaskQueue.performTask();
    EXPECT_FALSE(backup->findSegmentInfo({99, 0}, 88));
    EXPECT_TRUE(backup->findSegmentInfo({99, 0}, 89));
    EXPECT_TRUE(backup->findSegmentInfo({99, 1}, 88));

    backup->gcTaskQueue.performTask();
    EXPECT_FALSE(backup->findSegmentInfo({99, 0}, 88));
    EXPECT_FALSE(backup->findSegmentInfo({99, 0}, 89));
    EXPECT_TRUE(backup->findSegmentInfo({99, 1}, 88));

    backup->gcTaskQueue.performTask();
    EXPECT_FALSE(backup->findSegmentInfo({99, 0}, 88));
    EXPECT_FALSE(backup->findSegmentInfo({99, 0}, 89));
    EXPECT_TRUE(backup->findSegmentInfo({99, 1}, 88));

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
    backup->context.serverList->add({13, 0}, "mock:host=m", {}, 100);
    context.serverList->add({13, 0}, "mock:host=m", {}, 100);

    openSegment({13, 0}, 10);
    closeSegment({13, 0}, 10);
    backup->findSegmentInfo({13, 0}, 10)->createdByCurrentProcess = false;
    openSegment({13, 0}, 11);
    closeSegment({13, 0}, 11);
    backup->findSegmentInfo({13, 0}, 11)->createdByCurrentProcess = false;

    typedef BackupService::GarbageCollectReplicasFoundOnStorageTask Task;
    std::unique_ptr<Task> task(new Task(*backup, {13, 0}));
    task->addSegmentId(10);
    task->addSegmentId(11);
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
        "freeing replica for <13,10>"));
    EXPECT_EQ(1lu, backup->gcTaskQueue.outstandingTasks());
    EXPECT_FALSE(backup->findSegmentInfo({13, 0}, 10));
    EXPECT_TRUE(backup->findSegmentInfo({13, 0}, 11));

    EXPECT_FALSE(task->rpc);
    backup->gcTaskQueue.performTask(); // send rpc to probe 11
    ASSERT_TRUE(task->rpc);

    TestLog::reset();
    backup->gcTaskQueue.performTask(); // get response - true for 11
    EXPECT_TRUE(StringUtil::contains(TestLog::get(),
        "tryToFreeReplica: Server has not recovered from lost replica; "
        "retaining replica for <13,11>; "
        "will probe replica status again later"));
    EXPECT_EQ(1lu, backup->gcTaskQueue.outstandingTasks());

    backup->context.serverList->remove({13, 0});

    TestLog::reset();
    EXPECT_FALSE(task->rpc);
    backup->gcTaskQueue.performTask(); // send rpc
    EXPECT_TRUE(task->rpc);
    backup->gcTaskQueue.performTask(); // get response - server doesn't exist
    EXPECT_TRUE(StringUtil::contains(TestLog::get(),
        "tryToFreeReplica: Server 13 marked down; cluster has recovered from "
            "its failure | "
        "tryToFreeReplica: Server has recovered from lost replica; "
            "freeing replica for <13,11>"));
    EXPECT_EQ(1lu, backup->gcTaskQueue.outstandingTasks());

    // Final perform finds no segments to free and just cleans up
    backup->gcTaskQueue.performTask();
    EXPECT_EQ(0lu, backup->gcTaskQueue.outstandingTasks());
    task.release();
}

TEST_F(BackupServiceTest, GarbageCollectReplicaFoundOnStorageTask_freedFirst) {
    typedef BackupService::GarbageCollectReplicasFoundOnStorageTask Task;
    std::unique_ptr<Task> task(new Task(*backup, {99, 0}));
    task->addSegmentId(88);
    task->schedule();
    const_cast<ServerConfig&>(backup->config).backup.gc = true;

    TestLog::Enable _;
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

class SegmentInfoTest : public ::testing::Test {
  public:
    typedef BackupService::SegmentInfo SegmentInfo;
    typedef BackupStorage::Handle Handle;
    SegmentInfoTest()
        : segmentSize(64 * 1024)
        , pool{segmentSize}
        , storage{segmentSize, 2}
        , ioScheduler()
        , ioThread(std::ref(ioScheduler))
        , info{storage, pool, ioScheduler,
            ServerId(99, 0), 88, segmentSize, true}
        , scratchSegBuf(Memory::unique_ptr_free(
                            Memory::xmemalign(HERE,
                                              segmentSize,
                                              segmentSize),
                          std::free))
    {
        memset(scratchSegBuf.get(), 0, segmentSize);
    }

    ~SegmentInfoTest()
    {
        ioScheduler.shutdown(ioThread);
    }

    uint32_t segmentSize;
    BackupService::ThreadSafePool pool;
    InMemoryStorage storage;
    BackupService::IoScheduler ioScheduler;
    std::thread ioThread;
    SegmentInfo info;
    Memory::unique_ptr_free scratchSegBuf;
};

TEST_F(SegmentInfoTest, destructor) {
    TestLog::Enable _;
    {
        // Normal replica.
        SegmentInfo info{storage, pool, ioScheduler,
            ServerId(99, 0), 88, segmentSize, true};
        info.open();
        EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
    }
    EXPECT_EQ("~SegmentInfo: Backup shutting down with open segment <99,88>, "
              "closing out to storage", TestLog::get());
    TestLog::reset();
    {
        // Still open atomic replica.  Shouldn't get persisted.
        SegmentInfo info2{storage, pool, ioScheduler,
            ServerId(99, 0), 89, segmentSize, true};
        info2.open();
        Buffer src;
        info2.write(src, 0, 0, 0, NULL, true);
        EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
    }
    EXPECT_EQ("~SegmentInfo: Backup shutting down with open segment <99,89>, "
              "which was open for atomic replication; discarding since the "
              "replica was incomplete",
              TestLog::get());
    EXPECT_EQ(0, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(SegmentInfoTest, destructorLoading) {
    {
        SegmentInfo info{storage, pool, ioScheduler,
            ServerId(99, 0), 88, segmentSize, true};
        info.open();
        EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
        info.close();
        info.startLoading();
    }
    EXPECT_EQ(0, BackupStorage::Handle::getAllocatedHandlesCount());
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
    appendTablet(tablets, 0, 123,
        getKeyHash("10", 2), getKeyHash("10", 2), 0, 0);
    appendTablet(tablets, 1, 123,
        getKeyHash("30", 2), getKeyHash("30", 2), 0, 0);

    // tablet created when log head was > (0, 0)
    appendTablet(tablets, 0, 123,
        getKeyHash("XX", 2), getKeyHash("XX", 2), 12741, 57273);
}

TEST_F(SegmentInfoTest, appendRecoverySegment) {
    info.open();
    Segment segment(123, 88, scratchSegBuf.get(), segmentSize);

    SegmentHeader header = { 99, 88, segmentSize, Segment::INVALID_SEGMENT_ID };
    segment.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header));

    DECLARE_OBJECT(object, 2, 0);
    object->tableId = 123;
    object->keyLength = 2;
    object->version = 0;
    memcpy(object->getKeyLocation(), "10", 2);
    segment.append(LOG_ENTRY_TYPE_OBJ, object, object->objectLength(0));

    segment.close(true);
    Buffer src;
    auto committed = segment.getCommittedLength();
    segment.appendRangeToBuffer(src, 0, committed.first);
    info.write(src, 0, committed.first, 0, &committed.second, true);
    info.close();
    info.setRecovering();
    info.startLoading();

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);
    info.buildRecoverySegments(partitions);

    Buffer buffer;
    Status status = info.appendRecoverySegment(0, buffer);
    ASSERT_EQ(STATUS_OK, status);
    RecoverySegmentIterator it(buffer.getRange(0, buffer.getTotalLength()),
                               buffer.getTotalLength());
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
    EXPECT_EQ(object->objectLength(0), it.getLength());

    it.next();
    EXPECT_TRUE(it.isDone());
}

TEST_F(SegmentInfoTest, appendRecoverySegmentSecondarySegment) {
    SegmentInfo info{storage, pool, ioScheduler,
        ServerId(99, 0), 88, segmentSize, false};
    info.open();
    Segment segment(123, 88, scratchSegBuf.get(), segmentSize);

    SegmentHeader header = { 99, 88, segmentSize, Segment::INVALID_SEGMENT_ID };
    segment.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header));

    DECLARE_OBJECT(object, 2, 0);
    object->tableId = 123;
    object->keyLength = 2;
    object->version = 0;
    memcpy(object->getKeyLocation(), "10", 2);
    segment.append(LOG_ENTRY_TYPE_OBJ, object, object->objectLength(0));

    segment.close(true);
    Buffer src;
    auto committed = segment.getCommittedLength();
    segment.appendRangeToBuffer(src, 0, committed.first);
    info.write(src, 0, committed.first, 0, &committed.second, true);
    info.close();

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);
    info.setRecovering(partitions);

    Buffer buffer;
    while (true) {
        Status status = info.appendRecoverySegment(0, buffer);
        if (status == STATUS_RETRY) {
            buffer.reset();
            continue;
        }
        ASSERT_EQ(status, STATUS_OK);
        break;
    }
    buffer.reset();
    while (true) {
        Status status = info.appendRecoverySegment(0, buffer);
        if (status == STATUS_RETRY) {
            buffer.reset();
            continue;
        }
        ASSERT_EQ(status, STATUS_OK);
        break;
    }
    RecoverySegmentIterator it(buffer.getRange(0, buffer.getTotalLength()),
                               buffer.getTotalLength());
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
    EXPECT_EQ(object->objectLength(0), it.getLength());

    it.next();
    EXPECT_TRUE(it.isDone());
}

TEST_F(SegmentInfoTest, appendRecoverySegmentMalformedSegment) {
    TestLog::Enable _;
    info.open();
    memcpy(info.segment, "garbage", 7);
    info.setRecovering();
    info.startLoading();

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);
    info.buildRecoverySegments(partitions);

    Buffer buffer;
    Status status;
    EXPECT_THROW(status = info.appendRecoverySegment(0, buffer),
                 SegmentRecoveryFailedException);
    EXPECT_EQ(STATUS_OK, status);
}

TEST_F(SegmentInfoTest, appendRecoverySegmentNotYetRecovered) {
    Buffer buffer;
    TestLog::Enable _;
    Status status;
    EXPECT_THROW(status = info.appendRecoverySegment(0, buffer),
                 BackupBadSegmentIdException);
    EXPECT_EQ("appendRecoverySegment: Asked for segment <99,88> which isn't "
              "recovering", TestLog::get());
}

TEST_F(SegmentInfoTest, appendRecoverySegmentPartitionOutOfBounds) {
    info.open();
    Segment segment(123, 88, scratchSegBuf.get(), segmentSize);
    segment.close(true);
    Buffer src;
    auto committed = segment.getCommittedLength();
    segment.appendRangeToBuffer(src, 0, committed.first);
    info.write(src, 0, committed.first, 0, &committed.second, true);
    info.close();
    info.setRecovering();
    info.startLoading();

    ProtoBuf::Tablets partitions;
    info.buildRecoverySegments(partitions);

    EXPECT_EQ(0u, info.recoverySegmentsLength);
    Buffer buffer;
    TestLog::Enable _;
    Status status;
    EXPECT_THROW(status = info.appendRecoverySegment(0, buffer),
                 BackupBadSegmentIdException);
    EXPECT_EQ(STATUS_OK, status);
    EXPECT_EQ("appendRecoverySegment: Asked for recovery segment 0 from "
              "segment <99,88> but there are only 0 partitions",
              TestLog::get());
}

class MockSegmentIterator : public SegmentIterator {
  public:
    MockSegmentIterator(LogEntryType type,
                        uint64_t headSegmentIdDuringCleaning,
                        LogPosition pos)
        : SegmentIterator(),
          type(type),
          header(),
          pos(pos)
    {
        header.headSegmentIdDuringCleaning = headSegmentIdDuringCleaning;
    }

    LogEntryType getType() const { return type; }
    const SegmentHeader& getHeader() const { return header; }
    LogPosition getLogPosition() const { return pos; }

  private:
    LogEntryType type;
    SegmentHeader header;
    LogPosition pos;
};

TEST_F(SegmentInfoTest, isEntryAlive) {
    ProtoBuf::Tablets partitions;
    createTabletList(partitions);

    // Tablet's creation time log position was (12741, 57273)
    const ProtoBuf::Tablets::Tablet& tablet(partitions.tablet(2));

    // Is a cleaner segment...
    {
        MockSegmentIterator it(LOG_ENTRY_TYPE_OBJ,
                               12742,
                               LogPosition());
        EXPECT_TRUE(isEntryAlive(it, tablet));
    }
    {
        MockSegmentIterator it(LOG_ENTRY_TYPE_OBJ,
                               12740,
                               LogPosition());
        EXPECT_FALSE(isEntryAlive(it, tablet));
    }
    {
        MockSegmentIterator it(LOG_ENTRY_TYPE_OBJ,
                               12741,
                               LogPosition());
        EXPECT_FALSE(isEntryAlive(it, tablet));
    }

    // Is not a cleaner segment...
    {
        MockSegmentIterator it(LOG_ENTRY_TYPE_OBJ,
                               Segment::INVALID_SEGMENT_ID,
                               LogPosition(12741, 57273));
        EXPECT_TRUE(isEntryAlive(it, tablet));
    }
    {
        MockSegmentIterator it(LOG_ENTRY_TYPE_OBJ,
                               Segment::INVALID_SEGMENT_ID,
                               LogPosition(12741, 57274));
        EXPECT_TRUE(isEntryAlive(it, tablet));
    }
    {
        MockSegmentIterator it(LOG_ENTRY_TYPE_OBJ,
                               Segment::INVALID_SEGMENT_ID,
                               LogPosition(12742, 57273));
        EXPECT_TRUE(isEntryAlive(it, tablet));
    }
    {
        MockSegmentIterator it(LOG_ENTRY_TYPE_OBJ,
                               Segment::INVALID_SEGMENT_ID,
                               LogPosition(12740, 57273));
        EXPECT_FALSE(isEntryAlive(it, tablet));
    }
    {
        MockSegmentIterator it(LOG_ENTRY_TYPE_OBJ,
                               Segment::INVALID_SEGMENT_ID,
                               LogPosition(12741, 57272));
        EXPECT_FALSE(isEntryAlive(it, tablet));
    }
}

TEST_F(SegmentInfoTest, whichPartition) {
    ProtoBuf::Tablets partitions;
    createTabletList(partitions);

    info.open();
    Segment segment(123, 88, info.segment, segmentSize);

    DECLARE_OBJECT(object, 2, 0);
    object->tableId = 123;
    object->keyLength = 2;
    object->version = 0;

    // Create some test objects with different keys and append to segment.
    memcpy(object->getKeyLocation(), "10", 2);
    segment.append(LOG_ENTRY_TYPE_OBJ, object, object->objectLength(0));
    memcpy(object->getKeyLocation(), "30", 2);
    segment.append(LOG_ENTRY_TYPE_OBJ, object, object->objectLength(0));
    memcpy(object->getKeyLocation(), "40", 2);
    segment.append(LOG_ENTRY_TYPE_OBJ, object, object->objectLength(0));
    memcpy(object->getKeyLocation(), "XX", 2);
    segment.append(LOG_ENTRY_TYPE_OBJ, object, object->objectLength(0));

    SegmentIterator it(&segment);

    it.next();
    auto r = whichPartition(it, partitions);
    EXPECT_TRUE(r);
    EXPECT_EQ(0u, *r);

    it.next();
    r = whichPartition(it, partitions);
    EXPECT_TRUE(r);
    EXPECT_EQ(1u, *r);

    it.next();
    TestLog::Enable _;
    r = whichPartition(it, partitions);
    EXPECT_FALSE(r);
    HashType keyHash = getKeyHash("40", 2);
    EXPECT_EQ(format("whichPartition: Couldn't place object with "
              "<tableId, keyHash> of <123,%lu> into any "
              "of the given tablets for recovery; hopefully it belonged to "
              "a deleted tablet or lives in another log now", keyHash),
              TestLog::get());

    TestLog::reset();
    it.next();
    r = whichPartition(it, partitions);
    EXPECT_FALSE(r);

    keyHash = getKeyHash("XX", 2);
    EXPECT_EQ(format("whichPartition: Skipping object with <tableId, keyHash> "
        "of <123,%lu> because it appears to have existed prior to this "
        "tablet's creation.", keyHash), TestLog::get());
}

TEST_F(SegmentInfoTest, buildRecoverySegment) {
    info.open();
    Segment segment(123, 88, scratchSegBuf.get(), segmentSize);

    SegmentHeader header = { 99, 88, segmentSize, Segment::INVALID_SEGMENT_ID };
    segment.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header));

    DECLARE_OBJECT(object, 2, 0);
    object->tableId = 123;
    object->keyLength = 2;
    object->version = 0;
    memcpy(object->getKeyLocation(), "10", 2);
    segment.append(LOG_ENTRY_TYPE_OBJ, object,
                   object->objectLength(0));

    segment.close(true);
    Buffer src;
    auto committed = segment.getCommittedLength();
    segment.appendRangeToBuffer(src, 0, committed.first);
    info.write(src, 0, committed.first, 0, &committed.second, true);
    info.close();
    info.setRecovering();
    info.startLoading();

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);

    info.buildRecoverySegments(partitions);

    // Make sure subsequent calls have no effect.
    TestLog::Enable _;
    info.buildRecoverySegments(partitions);
    EXPECT_EQ("buildRecoverySegments: Recovery segments already built for "
              "<99,88>", TestLog::get());

    EXPECT_FALSE(info.recoveryException);
    EXPECT_EQ(2u, info.recoverySegmentsLength);
    ASSERT_TRUE(info.recoverySegments);
    EXPECT_EQ(object->objectLength(0) + sizeof(SegmentEntry),
              info.recoverySegments[0].getTotalLength());
    EXPECT_EQ(0u, info.recoverySegments[1].getTotalLength());
}

TEST_F(SegmentInfoTest, buildRecoverySegmentMalformedSegment) {
    TestLog::Enable _;
    info.open();
    memcpy(info.segment, "garbage", 7);
    info.setRecovering();
    info.startLoading();

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);

    info.buildRecoverySegments(partitions);
    EXPECT_TRUE(info.recoveryException);
    EXPECT_FALSE(info.recoverySegments);
    EXPECT_EQ(0u, info.recoverySegmentsLength);
}

TEST_F(SegmentInfoTest, buildRecoverySegmentNoTablets) {
    info.open();
    Segment segment(123, 88, scratchSegBuf.get(), segmentSize);
    segment.close(true);
    Buffer src;
    auto committed = segment.getCommittedLength();
    segment.appendRangeToBuffer(src, 0, committed.first);
    info.write(src, 0, committed.first, 0, &committed.second, true);
    info.setRecovering();
    info.startLoading();
    info.buildRecoverySegments(ProtoBuf::Tablets());
    EXPECT_FALSE(info.recoveryException);
    EXPECT_EQ(0u, info.recoverySegmentsLength);
    ASSERT_TRUE(info.recoverySegments);
}

TEST_F(SegmentInfoTest, close) {
    info.open();
    EXPECT_EQ(SegmentInfo::OPEN, info.state);
    ASSERT_TRUE(pool.is_from(info.segment));
    // The F gets tacked on by close() from the header given during write().
    const char* magic = "kitties!F";
    uint32_t bytesToCopy = downCast<uint32_t>(strlen(magic)) - 1;
    Buffer src;
    Buffer::Chunk::appendToBuffer(&src, magic, bytesToCopy);
    SegmentFooterEntry footerEntry;
    info.write(src, 0, bytesToCopy, 0, &footerEntry, false);

    info.close();
    EXPECT_EQ(SegmentInfo::CLOSED, info.state);
    {
        // wait for the store op to complete
        SegmentInfo::Lock lock(info.mutex);
        info.waitForOngoingOps(lock);
    }
    EXPECT_FALSE(pool.is_from(info.segment));

    char seg[segmentSize];
    storage.getSegment(info.storageHandle, seg);
    EXPECT_STREQ(magic, seg);

    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(SegmentInfoTest, closeWriteFooterEntry) {
    info.open();
    Buffer src;
    const char message[] = "this is a test";
    Buffer::Chunk::appendToBuffer(&src, message, arrayLength(message));
    SegmentFooterEntry footerEntry(0x1234abcdu);
    info.write(src, 10, 4, 1, &footerEntry, true);
    // Footer isn't there yet, stored off to the side.
    EXPECT_NE(0, memcmp(info.segment + info.footerOffset,
                        &footerEntry, sizeof(footerEntry)));
    info.close();
    // Footer plopped down correctly.
    EXPECT_EQ(0, memcmp(info.segment, "\0test", 5));
    EXPECT_EQ(0, memcmp(info.segment + info.footerOffset,
                        &footerEntry, sizeof(footerEntry)));
    // Ensure the segment-end-aligned footer is also written out.
    EXPECT_EQ(0, memcmp(info.segment + segmentSize - sizeof(footerEntry),
                        &footerEntry, sizeof(footerEntry)));
}

TEST_F(SegmentInfoTest, closeWhileNotOpen) {
    EXPECT_THROW(info.close(), BackupBadSegmentIdException);
}

TEST_F(SegmentInfoTest, free) {
    info.open();
    info.close();
    {
        // wait for the store op to complete
        SegmentInfo::Lock lock(info.mutex);
        info.waitForOngoingOps(lock);
    }
    EXPECT_FALSE(info.inMemory());
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
    info.free();
    EXPECT_FALSE(pool.is_from(info.segment));
    EXPECT_EQ(0, BackupStorage::Handle::getAllocatedHandlesCount());
    EXPECT_EQ(SegmentInfo::FREED, info.state);
}

TEST_F(SegmentInfoTest, freeRecoveringSecondary) {
    SegmentInfo info{storage, pool, ioScheduler,
        ServerId(99, 0), 88, segmentSize, false};
    info.open();
    info.close();
    info.setRecovering(ProtoBuf::Tablets());
    info.free();
    EXPECT_FALSE(pool.is_from(info.segment));
    EXPECT_EQ(0, BackupStorage::Handle::getAllocatedHandlesCount());
    EXPECT_EQ(SegmentInfo::FREED, info.state);
}

TEST_F(SegmentInfoTest, open) {
    info.open();
    ASSERT_NE(static_cast<char*>(NULL), info.segment);
    EXPECT_EQ('\0', info.segment[0]);
    EXPECT_NE(static_cast<Handle*>(NULL), info.storageHandle);
    EXPECT_EQ(SegmentInfo::OPEN, info.state);
}

TEST_F(SegmentInfoTest, openStorageAllocationFailure) {
    InMemoryStorage storage{segmentSize, 0};
    SegmentInfo info{storage, pool, ioScheduler,
        ServerId(99, 0), 88, segmentSize, true};
    EXPECT_THROW(info.open(), BackupStorageException);
    ASSERT_EQ(static_cast<char*>(NULL), info.segment);
    EXPECT_EQ(static_cast<Handle*>(NULL), info.storageHandle);
    EXPECT_EQ(SegmentInfo::UNINIT, info.state);
}

TEST_F(SegmentInfoTest, setRecoveringNoArgsWriteFooterEntry) {
    info.open();
    Buffer src;
    const char message[] = "this is a test";
    Buffer::Chunk::appendToBuffer(&src, message, arrayLength(message));
    SegmentFooterEntry footerEntry(0x1234abcdu);
    info.write(src, 10, 4, 1, &footerEntry, true);
    // Footer isn't there yet, stored off to the side.
    EXPECT_NE(0, memcmp(info.segment + info.footerOffset,
                        &footerEntry, sizeof(footerEntry)));

    EXPECT_FALSE(info.setRecovering());
    EXPECT_EQ(SegmentInfo::RECOVERING, info.state);
    // Footer plopped down correctly.
    EXPECT_EQ(0, memcmp(info.segment, "\0test", 5));
    EXPECT_EQ(0, memcmp(info.segment + info.footerOffset,
                        &footerEntry, sizeof(footerEntry)));
    // Ensure the segment-end-aligned footer is also written out.
    EXPECT_EQ(0, memcmp(info.segment + segmentSize - sizeof(footerEntry),
                        &footerEntry, sizeof(footerEntry)));
    EXPECT_TRUE(info.setRecovering());
}

TEST_F(SegmentInfoTest, setRecoveringArgsWriteFooterEntry) {
    SegmentInfo info{storage, pool, ioScheduler,
        ServerId(99, 0), 88, segmentSize, false};
    info.open();
    Buffer src;
    const char message[] = "this is a test";
    Buffer::Chunk::appendToBuffer(&src, message, arrayLength(message));
    SegmentFooterEntry footerEntry(0x1234abcdu);
    info.write(src, 10, 4, 1, &footerEntry, true);
    // Footer isn't there yet, stored off to the side.
    EXPECT_NE(0, memcmp(info.segment + info.footerOffset,
                        &footerEntry, sizeof(footerEntry)));

    ProtoBuf::Tablets tablets;
    EXPECT_FALSE(info.setRecovering(tablets));
    EXPECT_EQ(SegmentInfo::RECOVERING, info.state);
    // Footer plopped down correctly.
    EXPECT_EQ(0, memcmp(info.segment, "\0test", 5));
    EXPECT_EQ(0, memcmp(info.segment + info.footerOffset,
                        &footerEntry, sizeof(footerEntry)));
    // Ensure the segment-end-aligned footer is also written out.
    EXPECT_EQ(0, memcmp(info.segment + segmentSize - sizeof(footerEntry),
                        &footerEntry, sizeof(footerEntry)));
    ASSERT_TRUE(info.recoveryPartitions);
    EXPECT_EQ(0, info.recoveryPartitions->tablet_size());

    appendTablet(tablets, 0, 123,
        getKeyHash("9", 1), getKeyHash("9", 1), 0, 0);
    EXPECT_TRUE(info.setRecovering(tablets));
    ASSERT_TRUE(info.recoveryPartitions);
    EXPECT_EQ(1, info.recoveryPartitions->tablet_size());
}

TEST_F(SegmentInfoTest, startLoading) {
    info.open();
    info.close();
    info.startLoading();
    EXPECT_EQ(SegmentInfo::CLOSED, info.state);
}

TEST_F(SegmentInfoTest, write) {
    info.open();
    Buffer src;
    const char message[] = "this is a test";
    Buffer::Chunk::appendToBuffer(&src, message, arrayLength(message));
    SegmentFooterEntry footerEntry(0x1234abcdu);
    info.write(src, 10, 4, 1, &footerEntry, true);
    EXPECT_EQ(0, memcmp(info.segment, "\0test", 5));
    EXPECT_EQ(5lu, info.footerOffset);
    // Footer isn't there yet, stored off to the side.
    EXPECT_NE(0, memcmp(info.segment + info.footerOffset,
                        &footerEntry, sizeof(footerEntry)));
}

TEST_F(SegmentInfoTest, writeNonMonotonicFooterOffset) {
    info.open();
    Buffer src;
    const char message[] = "this is a test";
    Buffer::Chunk::appendToBuffer(&src, message, arrayLength(message));
    SegmentFooterEntry footerEntry(0x1234abcdu);
    info.write(src, 10, 4, 0, &footerEntry, true);
    info.write(src, 10, 4, 0, &footerEntry, true);
    info.write(src, 10, 4, 1, &footerEntry, true);
    TestLog::Enable _;
    EXPECT_THROW(info.write(src, 10, 4, 0, &footerEntry, true),
                 BackupSegmentOverflowException);
    EXPECT_EQ(
        "write: Write to <99,88> included a footer which was requested "
        "to be written at offset 4 but a prior write placed a footer later "
        "in the segment at 5", TestLog::get());
}

TEST_F(SegmentInfoTest, writeInsufficientSpaceForFooters) {
    info.open();
    Buffer src;
    SegmentFooterEntry footerEntry(0x1234abcdu);
    uint32_t offset = downCast<uint32_t>(info.segmentSize -
                                         2 * sizeof(footerEntry));
    TestLog::Enable _;
    info.write(src, 0, 0, offset, &footerEntry, true);
    EXPECT_THROW(info.write(src, 0, 0, offset + 1, &footerEntry, true),
                 BackupSegmentOverflowException);
    EXPECT_EQ(
        "write: Write to <99,88> included a footer which was requested to be "
        "written at offset 65509 but there isn't enough room in the segment "
        "for the footer",
        TestLog::get());
}

} // namespace RAMCloud
