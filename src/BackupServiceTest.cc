/* Copyright (c) 2009-2013 Stanford University
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
#include "InMemoryStorage.h"
#include "LogDigest.h"
#include "MockCluster.h"
#include "SegmentIterator.h"
#include "Server.h"
#include "Key.h"
#include "ServerListBuilder.h"
#include "SingleFileStorage.h"
#include "ShortMacros.h"
#include "StringUtil.h"
#include "TabletsBuilder.h"

namespace RAMCloud {

using namespace WireFormat; // NOLINT

class BackupServiceTest : public ::testing::Test {
  public:
    Context context;
    ServerConfig config;
    Tub<MockCluster> cluster;
    Server* server;
    BackupService* backup;
    ServerId backupId;
    TestLog::Enable logSilencer;

    BackupServiceTest()
        : context()
        , config(ServerConfig::forTesting())
        , cluster()
        , server()
        , backup()
        , backupId()
        , logSilencer()
    {
        Logger::get().setLogLevels(SILENT_LOG_LEVEL);

        cluster.construct(&context);
        config.services = {BACKUP_SERVICE};
        config.backup.numSegmentFrames = 5;
        server = cluster->addServer(config);
        backup = server->backup.get();
        backup->testingSkipCallerIdCheck = true;
        backupId = server->serverId;
    }

    ~BackupServiceTest()
    {
        cluster.destroy();
    }

    void
    closeSegment(ServerId masterId, uint64_t segmentId) {
        Segment segment;
        Segment::Certificate certificate;
        uint32_t length = segment.getAppendedLength(&certificate);
        BackupClient::writeSegment(&context, backupId, masterId, segmentId, 0,
                                   &segment, 0, length, &certificate,
                                   false, true, false);
    }

    void
    openSegment(ServerId masterId, uint64_t segmentId, bool primary = true)
    {
        Segment segment;
        Segment::Certificate certificate;
        uint32_t length = segment.getAppendedLength(&certificate);
        BackupClient::writeSegment(&context, backupId, masterId,
                                   segmentId, 0, &segment, 0, length,
                                   &certificate,
                                   true, false, primary);
    }

    void
    markNotWritten(ServerId masterId, uint64_t segmentId) {
        InMemoryStorage::Frame* frame =
                static_cast<InMemoryStorage::Frame*>(
                backup->frames.find({masterId, segmentId})->second.get());
        frame->appendedToByCurrentProcess = false;
    }

    /**
     * Write a raw string to the segment on backup (including the nul-
     * terminator). The segment will not be properly formatted and so
     * will not be recoverable.
     */
    void
    writeRawString(ServerId masterId, uint64_t segmentId,
                   uint32_t offset, const string& s,
                   bool close = false, uint64_t epoch = 0)
    {
        Segment segment;
        segment.copyIn(offset, s.c_str(), downCast<uint32_t>(s.length()) + 1);
        Segment::Certificate certificate;
        BackupClient::writeSegment(&context, backupId, masterId,
                                   segmentId, epoch,
                                   &segment,
                                   offset,
                                   uint32_t(s.length() + 1), &certificate,
                                   false, false, close);
    }

    const BackupReplicaMetadata*
    toMetadata(const void* metadata)
    {
        return static_cast<const BackupReplicaMetadata*>(metadata);
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(BackupServiceTest);
};

TEST_F(BackupServiceTest, constructorNoReuseReplicas) {
    config.backup.inMemory = false;
    config.clusterName = "testing";
    config.backup.file = ""; // use auto-generated testing name.

    cluster->addServer(config);

    config.clusterName = "__unnamed__";
    TestLog::Enable _("BackupService", "initOnceEnlisted", NULL);
    BackupService* backup = cluster->addServer(config)->backup.get();
    EXPECT_EQ(ServerId(), backup->getFormerServerId());
    EXPECT_EQ(
        "BackupService: Cluster '__unnamed__'; ignoring existing backup "
            "storage. Any replicas stored will not be reusable by future "
            "backups. Specify clusterName for persistence across backup "
            "restarts. | "
        "initOnceEnlisted: My server ID is 3.0 | "
        "initOnceEnlisted: Backup 3.0 will store replicas under cluster "
            "name '__unnamed__'"
        , TestLog::get());
}

TEST_F(BackupServiceTest, constructorDestroyConfusingReplicas) {
    config.backup.inMemory = false;
    config.clusterName = "__unnamed__";
    config.backup.file = ""; // use auto-generated testing name.

    cluster->addServer(config);

    config.clusterName = "testing";
    TestLog::Enable _("BackupService", "initOnceEnlisted", NULL);
    BackupService* backup = cluster->addServer(config)->backup.get();
    EXPECT_EQ(ServerId(), backup->getFormerServerId());
    EXPECT_EQ(
        "BackupService: Backup storing replicas with clusterName 'testing'. "
            "Future backups must be restarted with the same clusterName for "
            "replicas stored on this backup to be reused. | "
        "BackupService: Replicas stored on disk have a different clusterName "
            "('__unnamed__'). Scribbling storage to ensure any stale replicas "
            "left behind by old backups aren't used by future backups | "
        "initOnceEnlisted: My server ID is 3.0 | "
        "initOnceEnlisted: Backup 3.0 will store replicas under cluster name "
            "'testing'",
        TestLog::get());
}

TEST_F(BackupServiceTest, constructorReuseReplicas)
{
    config.backup.inMemory = false;
    config.clusterName = "testing";
    config.backup.file = ""; // use auto-generated testing name.

    Server* server = cluster->addServer(config);
    BackupService* backup = server->backup.get();

    SingleFileStorage* storage =
        static_cast<SingleFileStorage*>(backup->storage.get());
    // Use same auto-generated testing name as above.
    // Will cause double unlink from file system. Meh.
    config.backup.file = string(storage->tempFilePath);

    TestLog::Enable _("BackupService", "initOnceEnlisted", NULL);
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
        "initOnceEnlisted: My server ID is 3.0 | "
        "initOnceEnlisted: Backup 3.0 will store replicas under cluster name "
            "'testing'"
        , TestLog::get());
}

TEST_F(BackupServiceTest, dispatch_initializationNotFinished) {
    Buffer request, response;
    Service::Rpc rpc(NULL, &request, &response);
    string message("no exception");
    try {
        backup->initCalled = false;
        backup->dispatch(WireFormat::Opcode::ILLEGAL_RPC_TYPE, &rpc);
    } catch (RetryException& e) {
        message = e.message;
    }
    EXPECT_EQ("backup service not yet initialized", message);
}

TEST_F(BackupServiceTest, freeSegment) {
    openSegment({99, 0}, 88);
    closeSegment({99, 0}, 88);
    EXPECT_NE(backup->frames.end(), backup->frames.find({{99, 0}, 88}));
    {
        TestLog::Enable _;
        BackupClient::freeSegment(&context, backupId, {99, 0}, 88);
        EXPECT_EQ("freeSegment: Freeing replica for master 99.0 segment 88",
                  TestLog::get());
    }
    BackupClient::freeSegment(&context, backupId, ServerId(99, 0), 88);
    EXPECT_EQ(backup->frames.end(), backup->frames.find({{99, 0}, 88}));
}

TEST_F(BackupServiceTest, freeSegment_stillOpen) {
    openSegment({99, 0}, 88);
    BackupClient::freeSegment(&context, backupId, ServerId(99, 0), 88);
    EXPECT_EQ(backup->frames.end(), backup->frames.find({{99, 0}, 88}));
}

TEST_F(BackupServiceTest, freeSegment_underRecovery) {
    auto storage = static_cast<InMemoryStorage*>(backup->storage.get());
    size_t totalFrames = storage->freeMap.count();
    openSegment({99, 0}, 88);

    ProtoBuf::Tablets tablets;
    TabletsBuilder{tablets}
        // partition 0
        (123, Key::getHash(123, "9", 1), Key::getHash(123, "9", 1),
            TabletsBuilder::RECOVERING, 0)
        (123, Key::getHash(123, "10", 2), Key::getHash(123, "10", 2),
            TabletsBuilder::RECOVERING, 0)
        (123, Key::getHash(123, "29", 2), Key::getHash(123, "29", 2),
            TabletsBuilder::RECOVERING, 0)
        (123, Key::getHash(123, "20", 2), Key::getHash(123, "20", 2),
            TabletsBuilder::RECOVERING, 0)
        // partition 1
        (123, Key::getHash(123, "30", 2), Key::getHash(123, "30", 2),
            TabletsBuilder::RECOVERING, 1)
        (125, 0, ~0lu, TabletsBuilder::RECOVERING, 1);

    backup->taskQueue.halt();
    BackupClient::startReadingData(&context, backupId, 456lu, {99, 0});
    BackupClient::freeSegment(&context, backupId, {99, 0}, 88);
    EXPECT_EQ(totalFrames - 1, storage->freeMap.count());
}

TEST_F(BackupServiceTest, getRecoveryData) {
    openSegment({99, 0}, 88);
    closeSegment({99, 0}, 88);

    ProtoBuf::Tablets tablets;
    TabletsBuilder{tablets}
        (1, 0, ~0lu, TabletsBuilder::RECOVERING, 0);
    auto results = BackupClient::startReadingData(&context, backupId,
                                                  456lu, {99, 0});
    ProtoBuf::RecoveryPartition recoveryPartition;
    for (int i = 0; i < tablets.tablet_size(); i++) {
        ProtoBuf::Tablets::Tablet& tablet(*recoveryPartition.add_tablet());
        tablet = tablets.tablet(i);
    }
    BackupClient::StartPartitioningReplicas(&context, backupId,
                                          456lu, {99, 0}, &recoveryPartition);
    EXPECT_EQ(1lu, results.replicas.size());
    EXPECT_EQ(1lu, backup->recoveries.size());

    Buffer recoverySegment;
    auto certificate = BackupClient::getRecoveryData(&context, backupId,
                                                     456lu, {99, 0}, 88, 0,
                                                     &recoverySegment);
    EXPECT_THROW(BackupClient::getRecoveryData(&context, backupId,
                                               457lu, {99, 0}, 88, 0,
                                               &recoverySegment),
                BackupBadSegmentIdException);
}

TEST_F(BackupServiceTest, restartFromStorage)
{
    ServerConfig config = ServerConfig::forTesting();
    config.backup.inMemory = false;
    config.segmentSize = 4096;
    config.backup.numSegmentFrames = 6;
    config.backup.file = ""; // use auto-generated testing name.
    config.services = {BACKUP_SERVICE};
    config.clusterName = "testing";

    server = cluster->addServer(config);
    backup = server->backup.get();
    SingleFileStorage* storage =
        static_cast<SingleFileStorage*>(backup->storage.get());

    Buffer empty;
    Segment::Certificate certificate;
    Tub<BackupReplicaMetadata> metadata;
    std::vector<BackupStorage::FrameRef> frames;
    { // closed
        metadata.construct(certificate,
                           70, 88, config.segmentSize, 0,
                           true, false);
        BackupStorage::FrameRef frame = storage->open(true);
        frames.push_back(frame);
        frame->append(empty, 0, 0, 0, &metadata, sizeof(metadata));
    }
    { // open
        metadata.construct(certificate,
                           70, 89, config.segmentSize, 0,
                           false, false);
        BackupStorage::FrameRef frame = storage->open(true);
        frames.push_back(frame);
        frame->append(empty, 0, 0, 0, &metadata, sizeof(metadata));
    }
    { // bad checksum
        metadata.construct(certificate,
                           70, 90, config.segmentSize, 0,
                           true, false);
        metadata->checksum = 0;
        BackupStorage::FrameRef frame = storage->open(true);
        frames.push_back(frame);
        frame->append(empty, 0, 0, 0, &metadata, sizeof(metadata));
    }
    { // bad segment capacity
        metadata.construct(certificate,
                           70, 91, config.segmentSize+100, 0,
                           true, false);
        BackupStorage::FrameRef frame = storage->open(true);
        frames.push_back(frame);
        frame->append(empty, 0, 0, 0, &metadata, sizeof(metadata));
    }
    { // closed, different master
        metadata.construct(certificate,
                           71, 89, config.segmentSize, 0,
                           false, false);
        BackupStorage::FrameRef frame = storage->open(true);
        frames.push_back(frame);
        frame->append(empty, 0, 0, 0, &metadata, sizeof(metadata));
    }
    frames.clear();

    TestLog::Enable _;
    backup->restartFromStorage();

    EXPECT_NE(backup->frames.end(), backup->frames.find({{70, 0}, 88}));
    EXPECT_NE(backup->frames.end(), backup->frames.find({{70, 0}, 89}));
    EXPECT_EQ(backup->frames.end(), backup->frames.find({{70, 0}, 90}));
    EXPECT_EQ(backup->frames.end(), backup->frames.find({{70, 0}, 91}));
    EXPECT_NE(backup->frames.end(), backup->frames.find({{71, 0}, 89}));

    EXPECT_FALSE(storage->freeMap.test(0));
    EXPECT_FALSE(storage->freeMap.test(1));
    EXPECT_TRUE(storage->freeMap.test(2));
    EXPECT_TRUE(storage->freeMap.test(3));
    EXPECT_FALSE(storage->freeMap.test(4));

    EXPECT_TRUE(StringUtil::contains(TestLog::get(),
        "restartFromStorage: Found stored replica <70.0,88> "
        "on backup storage in frame which was closed"));
    EXPECT_TRUE(StringUtil::contains(TestLog::get(),
        "restartFromStorage: Found stored replica <70.0,89> "
        "on backup storage in frame which was open"));
    EXPECT_TRUE(StringUtil::contains(TestLog::get(),
        "restartFromStorage: Found stored replica <71.0,89> "
        "on backup storage in frame which was open"));

    // Check that only open frames are loaded.
    BackupStorage::FrameRef frame = backup->frames[
            BackupService::MasterSegmentIdPair({70, 0}, 88)];
    EXPECT_FALSE(frame->isLoaded());
    frame = backup->frames[BackupService::MasterSegmentIdPair({70, 0}, 89)];
    EXPECT_TRUE(frame->isLoaded());
    frame = backup->frames[BackupService::MasterSegmentIdPair({71, 0}, 89)];
    EXPECT_TRUE(frame->isLoaded());

    EXPECT_EQ(2lu, backup->taskQueue.outstandingTasks());
    // Because config.backup.gc is false these tasks delete themselves
    // immediately when performed.
    backup->taskQueue.performTask();
    backup->taskQueue.performTask();
    EXPECT_EQ(0lu, backup->taskQueue.outstandingTasks());
}

TEST_F(BackupServiceTest, startReadingData) {
    openSegment({99, 0}, 88);
    closeSegment({99, 0}, 88);
    openSegment({99, 0}, 89);
    closeSegment({99, 0}, 89);

    ProtoBuf::RecoveryPartition recoveryPartition;
    auto results = BackupClient::startReadingData(&context, backupId,
                                                  456lu, {99, 0});
    EXPECT_EQ(2lu, results.replicas.size());
    EXPECT_EQ(1lu, backup->recoveries.size());

    results = BackupClient::startReadingData(&context, backupId,
                                             456lu, {99, 0});
    EXPECT_EQ(2lu, results.replicas.size());
    EXPECT_EQ(1lu, backup->recoveries.size());

    TestLog::Enable _;
    results = BackupClient::startReadingData(&context, backupId,
                                             457lu, {99, 0});
    BackupClient::StartPartitioningReplicas(&context, backupId,
                                          457lu, {99, 0}, &recoveryPartition);
    EXPECT_EQ(2lu, results.replicas.size());
    EXPECT_EQ(1lu, backup->recoveries.size());
    EXPECT_EQ(
        "startReadingData: Got startReadingData for recovery 457 for crashed "
            "master 99.0; abandoning existing recovery 456 for that master and "
            "starting anew. | "
        "free: Recovery 456 for crashed master 99.0 is no longer needed; "
            "will clean up as next possible chance. | "
        "schedule: scheduled | "
        "start: Backup preparing for recovery 457 of crashed server 99.0; "
            "loading replicas | "
        "populateStartResponse: Crashed master 99.0 had closed secondary "
            "replica for segment 88 | "
        "populateStartResponse: Crashed master 99.0 had closed secondary "
            "replica for segment 89 | "
        "populateStartResponse: Sending 2 segment ids for this master "
            "(0 primary) | "
        "setPartitionsAndSchedule: Recovery 457 building 0 recovery segments "
            "for each replica for crashed master 99.0 and filtering them "
            "according to the following partitions:\n | "
        "setPartitionsAndSchedule: Kicked off building recovery segments | "
        "schedule: scheduled"
            , TestLog::get());
}

TEST_F(BackupServiceTest, writeSegment) {
    openSegment({99, 0}, 88);
    // test for idempotence
    for (int i = 0; i < 2; ++i)
        writeRawString({99, 0}, 88, 10, "test", false);
    auto frameIt = backup->frames.find({{99, 0}, 88});
    EXPECT_STREQ("test",
                 static_cast<char*>(frameIt->second->load()) + 10);
}

TEST_F(BackupServiceTest, writeSegment_checkCallerId) {
    backup->testingSkipCallerIdCheck = false;
    EXPECT_THROW(openSegment({99, 0}, 88), CallerNotInClusterException);
}

TEST_F(BackupServiceTest, writeSegment_epochStored) {
    openSegment({99, 0}, 88);
    auto frameIt = backup->frames.find({{99, 0}, 88});
    auto metadata = toMetadata(frameIt->second->getMetadata());
    writeRawString({99, 0}, 88, 10, "test", false, 0);
    EXPECT_EQ(0lu, metadata->segmentEpoch);
    writeRawString({99, 0}, 88, 10, "test", false, 1);
    EXPECT_EQ(1lu, metadata->segmentEpoch);
    EXPECT_STREQ("test",
                 static_cast<char*>(frameIt->second->load()) + 10);
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
}

TEST_F(BackupServiceTest, writeSegment_segmentClosedRedundantClosingWrite) {
    // This may seem counterintuitive, but throwing an exception on a write
    // after close is actually better than idempotent behavior. The backup
    // throws a client exception on subsequent writes. If the master retried
    // the write rpc and the backup had already received the request then the
    // master should never receive the response with the client exception
    // (the request will have gotten the response from the first request).
    // If the backup never received the first request from the master then
    // it won't generate a client exception on the retry.
    openSegment(ServerId(99, 0), 88);
    closeSegment(ServerId(99, 0), 88);
    EXPECT_THROW(writeRawString({99, 0}, 88, 10, "test", true),
                 BackupBadSegmentIdException);
}

TEST_F(BackupServiceTest, writeSegment_badOffset) {
    openSegment(ServerId(99, 0), 88);
    EXPECT_THROW(
        writeRawString({99, 0}, 88, 500000, "test"),
        BackupSegmentOverflowException);
}

TEST_F(BackupServiceTest, writeSegment_badLength) {
    openSegment(ServerId(99, 0), 88);
    uint32_t length = config.segmentSize + 1;
    ASSERT_TRUE(Segment::DEFAULT_SEGMENT_SIZE >= length);
    Segment segment;
    EXPECT_THROW(
        BackupClient::writeSegment(&context, backupId, ServerId(99, 0),
                                   88, 0, &segment, 0, length, {},
                                   false, false, false),
        BackupSegmentOverflowException);
}

TEST_F(BackupServiceTest, writeSegment_badOffsetPlusLength) {
    openSegment(ServerId(99, 0), 88);
    uint32_t length = config.segmentSize;
    ASSERT_TRUE(Segment::DEFAULT_SEGMENT_SIZE >= length);
    Segment segment;
    EXPECT_THROW(
        BackupClient::writeSegment(&context, backupId, ServerId(99, 0),
                                   88, 0, &segment, 1, length, {},
                                   false, false, false),
        BackupSegmentOverflowException);
}

TEST_F(BackupServiceTest, writeSegment_closeSegment) {
    openSegment(ServerId(99, 0), 88);
    writeRawString({99, 0}, 88, 10, "test");
    // loop to test for idempotence
    for (int i = 0; i > 2; ++i) {
        closeSegment(ServerId(99, 0), 88);
        auto frameIt = backup->frames.find({{99, 0}, 88});
        const char* replicaData =
            static_cast<const char*>(frameIt->second->load());
        EXPECT_STREQ("test", &replicaData[10]);
    }
}

TEST_F(BackupServiceTest, writeSegment_closeSegmentSegmentNotOpen) {
    EXPECT_THROW(closeSegment(ServerId(99, 0), 88),
                            BackupBadSegmentIdException);
}

TEST_F(BackupServiceTest, writeSegment_openSegment) {
    // loop to test for idempotence
    BackupService::FrameMap::iterator frameIt;
    for (int i = 0; i < 2; ++i) {
        openSegment(ServerId(99, 0), 88);
        frameIt = backup->frames.find({{99, 0}, 88});
        auto metadata = toMetadata(frameIt->second->getMetadata());
        EXPECT_TRUE(metadata->primary);
    }
    const char* replicaData = static_cast<const char*>(frameIt->second->load());
    EXPECT_EQ(0, *replicaData);
}

TEST_F(BackupServiceTest, writeSegment_openSegmentSecondary) {
    openSegment(ServerId(99, 0), 88, false);
    auto frameIt = backup->frames.find({{99, 0}, 88});
    auto metadata = toMetadata(frameIt->second->getMetadata());
    EXPECT_TRUE(!metadata->primary);
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
}

TEST_F(BackupServiceTest, GarbageCollectDownServerTask) {
    openSegment({99, 0}, 88);
    openSegment({99, 0}, 89);
    openSegment({99, 1}, 88);

    EXPECT_NE(backup->frames.end(), backup->frames.find({{99, 0}, 88}));
    EXPECT_NE(backup->frames.end(), backup->frames.find({{99, 0}, 89}));
    EXPECT_NE(backup->frames.end(), backup->frames.find({{99, 1}, 88}));

    backup->recoveries[ServerId{99, 0}] =
        new BackupMasterRecovery(backup->taskQueue, 456, {99, 0}, 0);
    EXPECT_NE(backup->recoveries.end(), backup->recoveries.find({99, 0}));

    typedef BackupService::GarbageCollectDownServerTask Task;
    std::unique_ptr<Task> task(new Task(*backup, {99, 0}));
    task->schedule();
    const_cast<ServerConfig*>(backup->config)->backup.gc = true;

    backup->taskQueue.performTask();
    EXPECT_EQ(backup->recoveries.end(), backup->recoveries.find({99, 0}));
    EXPECT_EQ(backup->frames.end(), backup->frames.find({{99, 0}, 88}));
    EXPECT_NE(backup->frames.end(), backup->frames.find({{99, 0}, 89}));
    EXPECT_NE(backup->frames.end(), backup->frames.find({{99, 1}, 88}));

    TestLog::Enable _;
    // Runs the now scheduled BackupMasterRecovery to free it up.
    backup->taskQueue.performTask();
    EXPECT_EQ("performTask: State for recovery 456 for crashed master 99.0 "
              "freed on backup", TestLog::get());

    backup->taskQueue.performTask();
    EXPECT_EQ(backup->frames.end(), backup->frames.find({{99, 0}, 88}));
    EXPECT_EQ(backup->frames.end(), backup->frames.find({{99, 0}, 89}));
    EXPECT_NE(backup->frames.end(), backup->frames.find({{99, 1}, 88}));

    backup->taskQueue.performTask();
    EXPECT_EQ(backup->frames.end(), backup->frames.find({{99, 0}, 88}));
    EXPECT_EQ(backup->frames.end(), backup->frames.find({{99, 0}, 89}));
    EXPECT_NE(backup->frames.end(), backup->frames.find({{99, 1}, 88}));

    task.release();
}

namespace {
class GcMockMasterService : public Service {
    void dispatch(Opcode opcode, Rpc* rpc) {
        const RequestCommon* hdr =
            rpc->requestPayload->getStart<RequestCommon>();
        switch (hdr->service) {
        case MEMBERSHIP_SERVICE:
            switch (opcode) {
            default:
                FAIL();
                break;
            }
            break;
        case MASTER_SERVICE:
            switch (hdr->opcode) {
            case Opcode::IS_REPLICA_NEEDED:
            {
                const IsReplicaNeeded::Request* req =
                    rpc->requestPayload->getStart<
                    IsReplicaNeeded::Request>();
                auto* resp = rpc->replyPayload->emplaceAppend<
                        IsReplicaNeeded::Response>();
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
    TestLog::Enable _("tryToFreeReplica");
    GcMockMasterService master;
    cluster->transport.addService(master, "mock:host=m", MEMBERSHIP_SERVICE);
    cluster->transport.addService(master, "mock:host=m", MASTER_SERVICE);
    ServerList* backupServerList = static_cast<ServerList*>(
        backup->context->serverList);
    backupServerList->testingAdd({{13, 0}, "mock:host=m", {}, 100,
                                  ServerStatus::UP});

    openSegment({13, 0}, 10);
    closeSegment({13, 0}, 10);
    markNotWritten({13, 0}, 10);
    openSegment({13, 0}, 11);
    closeSegment({13, 0}, 11);
    markNotWritten({13, 0}, 11);
    openSegment({13, 0}, 12);
    closeSegment({13, 0}, 12);
    markNotWritten({13, 0}, 12);

    typedef BackupService::GarbageCollectReplicasFoundOnStorageTask Task;
    Task* task = new Task(*backup, {13, 0});  // freed by performTask below
    task->addSegmentId(10);
    task->addSegmentId(11);
    task->addSegmentId(12);
    EXPECT_EQ(3, backup->oldReplicas);
    task->schedule();
    const_cast<ServerConfig*>(backup->config)->backup.gc = true;

    EXPECT_FALSE(task->rpc);
    backup->taskQueue.performTask(); // send rpc to probe 10
    ASSERT_TRUE(task->rpc);

    backup->taskQueue.performTask(); // get response - false for 10
    EXPECT_FALSE(task->rpc);
    EXPECT_EQ("tryToFreeReplica: Server has recovered from lost replica; "
        "freeing replica for <13.0,10> (2 more old replicas left)",
        TestLog::get());
    EXPECT_EQ(1lu, backup->taskQueue.outstandingTasks());
    EXPECT_EQ(backup->frames.end(), backup->frames.find({{13, 0}, 10}));
    EXPECT_NE(backup->frames.end(), backup->frames.find({{13, 0}, 11}));
    EXPECT_NE(backup->frames.end(), backup->frames.find({{13, 0}, 12}));
    EXPECT_EQ(2, backup->oldReplicas);

    EXPECT_FALSE(task->rpc);
    backup->taskQueue.performTask(); // send rpc to probe 11
    ASSERT_TRUE(task->rpc);

    TestLog::reset();
    backup->taskQueue.performTask(); // get response - true for 11
    EXPECT_EQ("tryToFreeReplica: Server has not recovered from lost replica; "
        "retaining replica for <13.0,11>; "
        "will probe replica status again later",
        TestLog::get());
    EXPECT_EQ(1lu, backup->taskQueue.outstandingTasks());

    backupServerList->testingCrashed({13, 0});

    TestLog::reset();
    EXPECT_FALSE(task->rpc);
    backup->taskQueue.performTask(); // find out server crashed
    EXPECT_EQ("tryToFreeReplica: Server 13.0 marked crashed; "
        "waiting for cluster to recover from its failure "
        "before freeing <13.0,11>",
        TestLog::get());
    EXPECT_EQ(1lu, backup->taskQueue.outstandingTasks());

    backupServerList->testingRemove({13, 0});

    TestLog::reset();
    EXPECT_FALSE(task->rpc);
    backup->taskQueue.performTask(); // send rpc
    EXPECT_TRUE(task->rpc);
    backup->taskQueue.performTask(); // get response - server doesn't exist
    EXPECT_EQ("tryToFreeReplica: Server 13.0 marked down; cluster has "
            "recovered from its failure | "
        "tryToFreeReplica: Server has recovered from lost replica; "
            "freeing replica for <13.0,12> (0 more old replicas left)",
        TestLog::get());
    EXPECT_EQ(1lu, backup->taskQueue.outstandingTasks());
    EXPECT_EQ(0, backup->oldReplicas);

    // Final perform finds no segments to free and just cleans up
    backup->taskQueue.performTask();
    EXPECT_EQ(0lu, backup->taskQueue.outstandingTasks());
}

static bool
taskScheduleFilter(string s)
{
    return s != "schedule";
}

TEST_F(BackupServiceTest,
        GarbageCollectReplicaTask_tryToFreeReplica_freedFirst) {
    typedef BackupService::GarbageCollectReplicasFoundOnStorageTask Task;
    Task* task = new Task(*backup, {99, 0});  // freed by performTask below
    task->addSegmentId(88);
    task->schedule();
    const_cast<ServerConfig*>(backup->config)->backup.gc = true;
    backup->oldReplicas = 10;

    TestLog::Enable _(taskScheduleFilter);
    backup->taskQueue.performTask();
    EXPECT_EQ("", TestLog::get());
    EXPECT_EQ(9, backup->oldReplicas);

    // Final perform finds no segments to free and just cleans up
    backup->taskQueue.performTask();
    EXPECT_EQ(0lu, backup->taskQueue.outstandingTasks());
}

TEST_F(BackupServiceTest,
        GarbageCollectReplicaTask_deleteReplica_replicaWritten) {
    ServerList* backupServerList = static_cast<ServerList*>(
        backup->context->serverList);
    backupServerList->testingAdd({{13, 0}, "mock:host=m", {}, 100,
                                  ServerStatus::UP});

    // Create 2 replicas, of which one has been modified and one of which
    // has not.
    openSegment({13, 0}, 10);
    closeSegment({13, 0}, 10);
    openSegment({13, 0}, 20);
    closeSegment({13, 0}, 20);
    markNotWritten({13, 0}, 20);
    typedef BackupService::GarbageCollectReplicasFoundOnStorageTask Task;
    std::unique_ptr<Task> task(new Task(*backup, {13, 0}));

    backup->oldReplicas = 10;
    TestLog::reset();
    task->deleteReplica(10);
    EXPECT_EQ("deleteReplica: Old replica for <13.0,10> has been "
            "called back into service, so won't garbage-collect it "
            "(9 more old replicas left)", TestLog::get());
    auto frameIt = backup->frames.find({ServerId(13, 0), 10LU});
    EXPECT_FALSE(frameIt == backup->frames.end());

    TestLog::reset();
    task->deleteReplica(20);
    EXPECT_EQ("", TestLog::get());
    frameIt = backup->frames.find({ServerId(13, 0), 20LU});
    EXPECT_TRUE(frameIt == backup->frames.end());

    task.release();
}

TEST_F(BackupServiceTest, trackerChangesEnqueued) {
    backup->testingDoNotStartGcThread = true;
    backup->gcTracker.enqueueChange({{99, 0}, "", {}, 0, ServerStatus::UP},
                                    SERVER_ADDED);
    backup->trackerChangesEnqueued();
    EXPECT_EQ(0lu, backup->taskQueue.outstandingTasks());

    backup->gcTracker.enqueueChange({{99, 0}, "", {}, 0, ServerStatus::CRASHED},
                                    SERVER_CRASHED);
    backup->trackerChangesEnqueued();
    EXPECT_EQ(0lu, backup->taskQueue.outstandingTasks());

    backup->gcTracker.enqueueChange({{99, 0}, "", {}, 0, ServerStatus::REMOVE},
                                    SERVER_REMOVED);
    backup->gcTracker.enqueueChange({{98, 0}, "", {}, 0, ServerStatus::UP},
                                    SERVER_ADDED);
    backup->gcTracker.enqueueChange({{98, 0}, "", {}, 0, ServerStatus::REMOVE},
                                    SERVER_REMOVED);
    backup->trackerChangesEnqueued();
    EXPECT_EQ(2lu, backup->taskQueue.outstandingTasks());
    backup->taskQueue.performTask();
    backup->taskQueue.performTask();
    EXPECT_EQ(0lu, backup->taskQueue.outstandingTasks());
}

} // namespace RAMCloud
