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
#include "Recovery.h"
#include "ShortMacros.h"
#include "TabletsBuilder.h"
#include "TabletMap.h"
#include "ProtoBuf.h"

namespace RAMCloud {

using namespace RecoveryInternal; // NOLINT

struct RecoveryTest : public ::testing::Test {
    Context context;
    TaskQueue taskQueue;
    RecoveryTracker tracker;
    vector<Tablet> tablets;

    RecoveryTest()
        : context()
        , taskQueue()
        , tracker(context)
        , tablets()
    {
        Logger::get().setLogLevels(SILENT_LOG_LEVEL);
    }

    /**
     * Populate #tracker with bogus entries for servers.
     *
     * \param count
     *      Number of server entries to add.
     * \param services
     *      Services the bogus servers entries should claim to suport.
     */
    void
    addServersToTracker(size_t count, ServiceMask services)
    {
        for (uint32_t i = 1; i < count + 1; ++i) {
            string locator = format("mock:host=server%u", i);
            tracker.enqueueChange({{i, 0}, locator, services,
                                   100, ServerStatus::UP}, SERVER_ADDED);
        }
        ServerDetails _;
        ServerChangeEvent __;
        while (tracker.getChange(_, __));
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(RecoveryTest);
};

namespace {
/**
 * Helper for filling-in a log digest in a startReadingData result.
 *
 * \param[out] result
 *      Result whose log digest should be filled in.
 * \param segmentId
 *      Segment id of the "replica" where this digest was found.
 * \param segmentIds
 *      List of segment ids which should be in the log digest.
 */
void
populateLogDigest(BackupClient::StartReadingData::Result& result,
                  uint64_t segmentId,
                  std::vector<uint64_t> segmentIds)
{
    // Doesn't matter for these tests.
    result.logDigestSegmentLen = 100;
    result.logDigestSegmentId = segmentId;
    result.logDigestBytes =
        downCast<uint32_t>(LogDigest::getBytesFromCount(segmentIds.size()));
    result.logDigestBuffer =
        std::unique_ptr<char[]>(new char[result.logDigestBytes]);
    LogDigest digest(segmentIds.size(),
                     result.logDigestBuffer.get(), result.logDigestBytes);
    foreach (auto segmentId, segmentIds)
        digest.addSegment(segmentId);
}
}

TEST_F(RecoveryTest, constructor) {
    // Mostly trivial, but does compute numPartitions from the will.
    Tub<Recovery> recovery;
    Recovery::Owner* own = static_cast<Recovery::Owner*>(NULL);

    recovery.construct(taskQueue, &tracker, own, ServerId(99), tablets, 0lu);
    EXPECT_EQ(0lu, recovery->numPartitions);

    tablets = {
        {123,  0,  9, {99, 0}, Tablet::RECOVERING, {}},
        {123, 20, 29, {99, 0}, Tablet::RECOVERING, {}},
    };
    recovery.construct(taskQueue, &tracker, own, ServerId(99), tablets, 0lu);
    EXPECT_EQ(2lu, recovery->numPartitions);

    tablets.push_back(Tablet{123, 10, 19, {99, 0}, Tablet::RECOVERING, {}});
    recovery.construct(taskQueue, &tracker, own, ServerId(99), tablets, 0lu);
    EXPECT_EQ(3lu, recovery->numPartitions);
}

TEST_F(RecoveryTest, startBackups) {
    /**
     * Called by BackupStartTask instead of sending the startReadingData
     * RPC. The callback mocks out the result of the call for testing.
     * Each call into the callback corresponds to the send of the RPC
     * to an individual backup.
     */
    struct Cb : public BackupStartTask::TestingCallback {
        int callCount;
        Cb() : callCount() {}
        void backupStartTaskSend(BackupClient::StartReadingData::Result& result)
        {
            if (callCount == 0) {
                // Two segments on backup1, one that overlaps with backup2
                // Includes a segment digest
                result.segmentIdAndLength.push_back({88lu, 100u});
                result.segmentIdAndLength.push_back({89lu, 100u});
                populateLogDigest(result, 89, {88, 89});
                result.primarySegmentCount = 1;
            } else if (callCount == 1) {
                // One segment on backup2
                result.segmentIdAndLength.push_back({88lu, 100u});
                result.primarySegmentCount = 1;
            } else if (callCount == 2) {
                // No segments on backup3
            }
            callCount++;
        }
    } callback;
    addServersToTracker(3, {BACKUP_SERVICE});
    tablets = {{123, 10, 19, {99, 0}, Tablet::RECOVERING, {}}};
    Recovery recovery(taskQueue, &tracker, NULL, ServerId(99), tablets, 0lu);
    recovery.testingBackupStartTaskSendCallback = &callback;
    recovery.startBackups();
    EXPECT_EQ((vector<RecoverRpc::Replica> {
                    { 1, 88 },
                    { 2, 88 },
                    { 1, 89 },
               }),
              recovery.replicaMap);
}

TEST_F(RecoveryTest, startBackups_failureContactingSomeBackup) {
    Recovery recovery(taskQueue, &tracker, NULL, ServerId(99), tablets, 0lu);
    ProtoBuf::Tablets partitions;
    BackupStartTask task(&recovery, {2, 0}, {1, 0}, partitions, 0);
    EXPECT_NO_THROW(task.send());
}

TEST_F(RecoveryTest, startBackups_secondariesEarlyInSomeList) {
    // See buildReplicaMap test for info about how the callback is used.
    struct Cb : public BackupStartTask::TestingCallback {
        int callCount;
        Cb() : callCount() {}
        void backupStartTaskSend(BackupClient::StartReadingData::Result& result)
        {
            if (callCount == 0) {
                result.segmentIdAndLength.push_back({88lu, 100u});
                result.segmentIdAndLength.push_back({89lu, 100u});
                result.segmentIdAndLength.push_back({90lu, 100u});
                result.primarySegmentCount = 3;
            } else if (callCount == 1) {
                result.segmentIdAndLength.push_back({88lu, 100u});
                result.segmentIdAndLength.push_back({91lu, 100u});
                populateLogDigest(result, 91, {88, 89, 90, 91});
                result.primarySegmentCount = 1;
            } else if (callCount == 2) {
                result.segmentIdAndLength.push_back({91lu, 100u});
                result.primarySegmentCount = 1;
            }
            callCount++;
        }
    } callback;
    addServersToTracker(3, {BACKUP_SERVICE});
    tablets = {{123, 10, 19, {99, 0}, Tablet::RECOVERING, {}}};
    Recovery recovery(taskQueue, &tracker, NULL, ServerId(99), tablets, 0lu);
    recovery.testingBackupStartTaskSendCallback = &callback;
    recovery.startBackups();
    ASSERT_EQ(6U, recovery.replicaMap.size());
    // The secondary of segment 91 must be last in the list.
    EXPECT_EQ(91U, recovery.replicaMap.at(5).segmentId);
}

namespace {
bool startBackupsFilter(string s) {
    return s == "startBackups";
}
}

TEST_F(RecoveryTest, startBackups_noLogDigestFound) {
    BackupStartTask::TestingCallback callback; // No-op callback.
    addServersToTracker(3, {BACKUP_SERVICE});
    tablets = {{123, 10, 19, {99, 0}, Tablet::RECOVERING, {}}};
    Recovery recovery(taskQueue, &tracker, NULL, ServerId(99), tablets, 0lu);
    recovery.testingBackupStartTaskSendCallback = &callback;
    TestLog::Enable _(startBackupsFilter);
    recovery.startBackups();
    EXPECT_EQ(
        "startBackups: Getting segment lists from backups and preparing "
            "them for recovery | "
        "startBackups: No log digest among replicas on available backups. "
            "Will retry recovery later.", TestLog::get());
}

TEST_F(RecoveryTest, startBackups_someReplicasMissing) {
    // See buildReplicaMap test for info about how the callback is used.
    struct Cb : public BackupStartTask::TestingCallback {
        void backupStartTaskSend(BackupClient::StartReadingData::Result& result)
        {
            populateLogDigest(result, 91, {91});
        }
    } callback;
    addServersToTracker(3, {BACKUP_SERVICE});
    tablets = {{123, 10, 19, {99, 0}, Tablet::RECOVERING, {}}};
    Recovery recovery(taskQueue, &tracker, NULL, ServerId(99), tablets, 0lu);
    recovery.testingBackupStartTaskSendCallback = &callback;
    TestLog::Enable _(startBackupsFilter);
    recovery.startBackups();
    EXPECT_EQ(
        "startBackups: Getting segment lists from backups and preparing "
            "them for recovery | "
        "startBackups: Segment 91 of length 100 bytes is the head of the log | "
        "startBackups: Some replicas from log digest not on available backups. "
            "Will retry recovery later.", TestLog::get());
}

TEST_F(RecoveryTest, BackupStartTask_filterOutInvalidReplicas) {
    ProtoBuf::Tablets tablets;
    BackupStartTask task(NULL, {2, 0}, {1, 0}, tablets, 10lu);
    auto& segments = task.result.segmentIdAndLength;
    segments = {
        {2lu, 100u},
        {2lu, ~0u},
        {10lu, 100u},
        {10lu, ~0u},
        {11u, 100u},
        {11u, ~0},
    };

    task.result.primarySegmentCount = 2;
    uint32_t bytes = downCast<uint32_t>(LogDigest::getBytesFromCount(0));
    task.result.logDigestBytes = bytes;
    task.result.logDigestBuffer = std::unique_ptr<char[]>(new char[bytes]);
    task.result.logDigestSegmentId = 9;
    task.result.logDigestSegmentLen = 1;
    task.filterOutInvalidReplicas();
    ASSERT_EQ(5lu, segments.size());
    EXPECT_EQ((vector<pair<uint64_t, uint32_t>> {
                    { 2, ~0u },
                    { 10, 100 },
                    { 10, ~0u },
                    { 11, 100 },
                    { 11, ~0u },
               }),
              segments);
    EXPECT_EQ(1lu, task.result.primarySegmentCount);
    EXPECT_EQ(0u, task.result.logDigestBytes);
    EXPECT_EQ(static_cast<char*>(NULL), task.result.logDigestBuffer.get());
    EXPECT_EQ(~0lu, task.result.logDigestSegmentId);
    EXPECT_EQ(~0u, task.result.logDigestSegmentLen);
}

TEST_F(RecoveryTest, verifyLogComplete) {
    const size_t segmentCount = 3;
    const size_t bufferSize = LogDigest::getBytesFromCount(segmentCount);
    char buffer[bufferSize];
    LogDigest digest(segmentCount, buffer, bufferSize);
    digest.addSegment(10);
    digest.addSegment(11);
    digest.addSegment(12);

    Tub<BackupStartTask> tasks[1];
    ProtoBuf::Tablets tablets;
    Recovery* null = NULL;
    tasks[0].construct(null, ServerId(2, 0), ServerId(1, 0), tablets, 0);
    auto& segments = tasks[0]->result.segmentIdAndLength;

    TestLog::Enable _;
    segments = {{10, 0}, {12, 0}};
    EXPECT_FALSE(verifyLogComplete(tasks, 1, digest));
    EXPECT_EQ(
        "verifyLogComplete: Segment 11 listed in the log digest but "
            "not found among available backups | "
        "verifyLogComplete: 1 segments in the digest but not available "
            "from backups", TestLog::get());
    segments = {{10, 0}, {11, 0}, {12, 0}};
    EXPECT_TRUE(verifyLogComplete(tasks, 1, digest));
}

TEST_F(RecoveryTest, findLogDigest) {
    Tub<BackupStartTask> tasks[2];
    ProtoBuf::Tablets tablets;
    Recovery* null = NULL;
    tasks[0].construct(null, ServerId(2, 0), ServerId(1, 0), tablets, 0);
    tasks[1].construct(null, ServerId(3, 0), ServerId(1, 0), tablets, 0);

    // No log digest found.
    auto digest = findLogDigest(tasks, 2);
    EXPECT_FALSE(digest);

    auto& result0 = tasks[0]->result;
    auto& result1 = tasks[1]->result;

    uint32_t bytes = downCast<uint32_t>(LogDigest::getBytesFromCount(0));
    result0.logDigestBytes = bytes;
    result0.logDigestBuffer = std::unique_ptr<char[]>(new char[bytes]);
    result0.logDigestSegmentId = 10;
    result0.logDigestSegmentLen = 1;
    result1.logDigestBytes = bytes;
    result1.logDigestBuffer = std::unique_ptr<char[]>(new char[bytes]);
    result1.logDigestSegmentId = 10;
    result1.logDigestSegmentLen = 1;
    // Two log digests, same segment id, same length (keeps earlier of two).
    digest = findLogDigest(tasks, 2);
    ASSERT_TRUE(digest);
    EXPECT_EQ(10lu, get<0>(*digest));
    EXPECT_EQ(1u, get<1>(*digest));
    EXPECT_EQ(result0.logDigestBuffer.get(),
              reinterpret_cast<char*>(std::get<2>(*digest).ldd));

    result1.logDigestSegmentId = 9;
    // Two log digests, later one has a lower segment id.
    digest = findLogDigest(tasks, 2);
    ASSERT_TRUE(digest);
    EXPECT_EQ(9lu, get<0>(*digest));
    EXPECT_EQ(1u, get<1>(*digest));
    EXPECT_EQ(result1.logDigestBuffer.get(),
              reinterpret_cast<char*>(std::get<2>(*digest).ldd));
}

TEST_F(RecoveryTest, buildReplicaMap) {
    Tub<BackupStartTask> tasks[2];
    ProtoBuf::Tablets tablets;
    Recovery* null = NULL;
    tasks[0].construct(null, ServerId(2, 0), ServerId(1, 0), tablets, 0);
    auto* result = &tasks[0]->result;
    result->segmentIdAndLength.push_back({88lu, 100u});
    result->segmentIdAndLength.push_back({89lu, 100u});
    result->segmentIdAndLength.push_back({90lu, 100u});
    result->primarySegmentCount = 3;

    tasks[1].construct(null, ServerId(3, 0), ServerId(1, 0), tablets, 0);
    result = &tasks[1]->result;
    result->segmentIdAndLength.push_back({88lu, 100u});
    result->segmentIdAndLength.push_back({91lu, 100u});
    result->primarySegmentCount = 1;

    addServersToTracker(3, {BACKUP_SERVICE});

    auto replicaMap = buildReplicaMap(tasks, 2, &tracker, 91, 100);
    EXPECT_EQ((vector<RecoverRpc::Replica> {
                    { 2, 88 },
                    { 3, 88 },
                    { 2, 89 },
                    { 2, 90 },
                    { 3, 91 },
               }),
              replicaMap);

    tracker.getServerDetails({3, 0})->expectedReadMBytesPerSec = 101;
    TestLog::Enable _;
    replicaMap = buildReplicaMap(tasks, 2, &tracker, 91, 100);
    EXPECT_EQ((vector<RecoverRpc::Replica> {
                    { 3, 88 },
                    { 2, 88 },
                    { 2, 89 },
                    { 2, 90 },
                    { 3, 91 },
               }),
              replicaMap);
}

TEST_F(RecoveryTest, buildReplicaMap_badReplicas) {
    Tub<BackupStartTask> tasks[1];
    ProtoBuf::Tablets tablets;
    Recovery* null = NULL;
    tasks[0].construct(null, ServerId(2, 0), ServerId(1, 0), tablets, 0);
    auto* result = &tasks[0]->result;
    result->segmentIdAndLength.push_back({92lu, 100u}); // beyond head
    result->segmentIdAndLength.push_back({91lu, 99u}); // shorter than head
    result->segmentIdAndLength.push_back({91lu, 101u}); // longer than head
    result->primarySegmentCount = 3;

    addServersToTracker(2, {BACKUP_SERVICE});

    auto replicaMap = buildReplicaMap(tasks, 1, &tracker, 91, 100);
    EXPECT_EQ((vector<RecoverRpc::Replica>()), replicaMap);
}

TEST_F(RecoveryTest, startRecoveryMasters) {
    MockRandom _(1);
    struct Cb : public MasterStartTaskTestingCallback {
        int callCount;
        Cb() : callCount() {}
        void masterStartTaskSend(uint64_t recoveryId,
                                 ServerId crashedServerId,
                                 uint32_t partitionId,
                                 const ProtoBuf::Tablets& tablets,
                                 const RecoverRpc::Replica replicaMap[],
                                 size_t replicaMapSize)
        {
            if (callCount == 0) {
                EXPECT_EQ(1lu, recoveryId);
                EXPECT_EQ(ServerId(99, 0), crashedServerId);
                ASSERT_EQ(2, tablets.tablet_size());
                const auto* tablet = &tablets.tablet(0);
                EXPECT_EQ(123lu, tablet->table_id());
                EXPECT_EQ(0lu, tablet->start_key_hash());
                EXPECT_EQ(9lu, tablet->end_key_hash());
                EXPECT_EQ(TabletsBuilder::Tablet::RECOVERING, tablet->state());
                EXPECT_EQ(0lu, tablet->user_data());
                tablet = &tablets.tablet(1);
                EXPECT_EQ(123lu, tablet->table_id());
                EXPECT_EQ(20lu, tablet->start_key_hash());
                EXPECT_EQ(29lu, tablet->end_key_hash());
                EXPECT_EQ(TabletsBuilder::Tablet::RECOVERING, tablet->state());
                EXPECT_EQ(0lu, tablet->user_data());
            } else if (callCount == 1) {
                EXPECT_EQ(1lu, recoveryId);
                EXPECT_EQ(ServerId(99, 0), crashedServerId);
                ASSERT_EQ(1, tablets.tablet_size());
                const auto* tablet = &tablets.tablet(0);
                EXPECT_EQ(123lu, tablet->table_id());
                EXPECT_EQ(10lu, tablet->start_key_hash());
                EXPECT_EQ(19lu, tablet->end_key_hash());
                EXPECT_EQ(TabletsBuilder::Tablet::RECOVERING, tablet->state());
                EXPECT_EQ(1lu, tablet->user_data());
            } else {
                FAIL();
            }
            ++callCount;
        }
    } callback;
    addServersToTracker(2, {MASTER_SERVICE});
    tablets = {
        {123,  0,  9, {99, 0}, Tablet::RECOVERING, {}},
        {123, 20, 29, {99, 0}, Tablet::RECOVERING, {}},
        {123, 10, 19, {99, 0}, Tablet::RECOVERING, {}},
    };
    Recovery recovery(taskQueue, &tracker, NULL, {99, 0}, tablets, 0lu);
    // Hack 'tablets' to get the first two tablets on the same server.
    recovery.tablets.mutable_tablet(1)->set_user_data(0);
    recovery.tablets.mutable_tablet(2)->set_user_data(1);
    recovery.numPartitions = 2;
    recovery.testingMasterStartTaskSendCallback = &callback;
    recovery.startRecoveryMasters();

    EXPECT_EQ(2u, recovery.numPartitions);
    EXPECT_EQ(0u, recovery.successfulRecoveryMasters);
    EXPECT_EQ(0u, recovery.unsuccessfulRecoveryMasters);
}

/**
 * Tests two conditions. First, that recovery masters which already have
 * recoveries started on them aren't used for recovery. Second, that
 * if there aren't enough master which aren't already participating in a
 * recovery the recovery recovers what it can and schedules a follow
 * up recovery.
 */
TEST_F(RecoveryTest, startRecoveryMasters_tooFewIdleMasters) {
    MockRandom _(1);
    struct Cb : public MasterStartTaskTestingCallback {
        int callCount;
        Cb() : callCount() {}
        void masterStartTaskSend(uint64_t recoveryId,
                                 ServerId crashedServerId,
                                 uint32_t partitionId,
                                 const ProtoBuf::Tablets& tablets,
                                 const RecoverRpc::Replica replicaMap[],
                                 size_t replicaMapSize)
        {
            if (callCount == 0) {
                EXPECT_EQ(1lu, recoveryId);
                EXPECT_EQ(ServerId(99, 0), crashedServerId);
                ASSERT_EQ(2, tablets.tablet_size());
            } else {
                FAIL();
            }
            ++callCount;
        }
    } callback;
    addServersToTracker(2, {MASTER_SERVICE});
    tracker[ServerId(1, 0)] = reinterpret_cast<Recovery*>(0x1);
    tablets = {
        {123,  0,  9, {99, 0}, Tablet::RECOVERING, {}},
        {123, 20, 29, {99, 0}, Tablet::RECOVERING, {}},
        {123, 10, 19, {99, 0}, Tablet::RECOVERING, {}},
    };
    Recovery recovery(taskQueue, &tracker, NULL, {99, 0}, tablets, 0lu);
    // Hack 'tablets' to get the first two tablets on the same server.
    recovery.tablets.mutable_tablet(1)->set_user_data(0);
    recovery.tablets.mutable_tablet(2)->set_user_data(1);
    recovery.numPartitions = 2;
    recovery.testingMasterStartTaskSendCallback = &callback;
    recovery.startRecoveryMasters();

    recovery.recoveryMasterFinished({2, 0}, true);

    EXPECT_EQ(2u, recovery.numPartitions);
    EXPECT_EQ(1u, recovery.successfulRecoveryMasters);
    EXPECT_EQ(1u, recovery.unsuccessfulRecoveryMasters);
    EXPECT_FALSE(recovery.wasCompletelySuccessful());
}

/**
 * Slightly different than the tooFewIdleMasters case above: because
 * no recovery master get started we need to make sure recovery doesn't
 * enter the 'wait for recovery masters' phase and that it finishes early.
 */
TEST_F(RecoveryTest, startRecoveryMasters_noIdleMasters) {
    struct Owner : public Recovery::Owner {
        Owner() : finishedCalled(), destroyCalled() {}
        bool finishedCalled;
        bool destroyCalled;
        void recoveryFinished(Recovery* recovery) {
            finishedCalled = true;
        }
        void destroyAndFreeRecovery(Recovery* recovery) {
            destroyCalled = true;
        }
    } owner;
    MockRandom __(1);
    tablets = {
        {123,  0,  9, {99, 0}, Tablet::RECOVERING, {}},
        {123, 20, 29, {99, 0}, Tablet::RECOVERING, {}},
        {123, 10, 19, {99, 0}, Tablet::RECOVERING, {}},
    };
    addServersToTracker(2, {MASTER_SERVICE});
    tracker[ServerId(1, 0)] = reinterpret_cast<Recovery*>(0x1);
    tracker[ServerId(2, 0)] = reinterpret_cast<Recovery*>(0x1);
    Recovery recovery(taskQueue, &tracker, &owner, {99, 0}, tablets, 0lu);

    TestLog::Enable _;
    recovery.startRecoveryMasters();

    EXPECT_EQ(
        "startRecoveryMasters: Starting recovery 1 for crashed server 99 "
            "with 3 partitions | "
        "startRecoveryMasters: Couldn't find enough masters not already "
            "performing a recovery to recover all partitions: 3 partitions "
            "will be recovered later | "
        "recoveryMasterFinished: Recovery wasn't completely successful; "
            "will not broadcast the end of recovery 1 for server 99 to backups",
        TestLog::get());
    EXPECT_EQ(3u, recovery.numPartitions);
    EXPECT_EQ(0u, recovery.successfulRecoveryMasters);
    EXPECT_EQ(3u, recovery.unsuccessfulRecoveryMasters);
    EXPECT_EQ(Recovery::DONE, recovery.status);
    EXPECT_FALSE(recovery.wasCompletelySuccessful());
    EXPECT_TRUE(owner.finishedCalled);
    EXPECT_TRUE(owner.destroyCalled);
}

TEST_F(RecoveryTest, startRecoveryMasters_allFailDuringRecoverRpc) {
    tablets = {
        {123,  0,  9, {99, 0}, Tablet::RECOVERING, {}},
        {123, 20, 29, {99, 0}, Tablet::RECOVERING, {}},
        {123, 10, 19, {99, 0}, Tablet::RECOVERING, {}},
    };
    addServersToTracker(2, {MASTER_SERVICE});
    Recovery recovery(taskQueue, &tracker, NULL, {99, 0}, tablets, 0lu);
    recovery.startRecoveryMasters();

    EXPECT_EQ(3u, recovery.numPartitions);
    EXPECT_EQ(0u, recovery.successfulRecoveryMasters);
    EXPECT_EQ(3u, recovery.unsuccessfulRecoveryMasters);
    EXPECT_EQ(Recovery::DONE, recovery.status);
    EXPECT_FALSE(recovery.isScheduled()); // NOT scheduled to send broadcast
    EXPECT_FALSE(tracker[ServerId(1, 0)]);
    EXPECT_FALSE(tracker[ServerId(2, 0)]);
}

TEST_F(RecoveryTest, recoveryMasterFinished) {
    addServersToTracker(3, {MASTER_SERVICE});
    Recovery recovery(taskQueue, &tracker, NULL, {99, 0}, tablets, 0lu);
    tracker[ServerId(2, 0)] = &recovery;
    tracker[ServerId(3, 0)] = &recovery;
    recovery.numPartitions = 2;
    recovery.status = Recovery::WAIT_FOR_RECOVERY_MASTERS;

    recovery.recoveryMasterFinished({2, 0}, true);
    EXPECT_EQ(1u, recovery.successfulRecoveryMasters);
    EXPECT_EQ(0u, recovery.unsuccessfulRecoveryMasters);
    EXPECT_EQ(Recovery::WAIT_FOR_RECOVERY_MASTERS, recovery.status);

    recovery.recoveryMasterFinished({2, 0}, true);
    EXPECT_EQ(1u, recovery.successfulRecoveryMasters);
    EXPECT_EQ(0u, recovery.unsuccessfulRecoveryMasters);
    EXPECT_EQ(Recovery::WAIT_FOR_RECOVERY_MASTERS, recovery.status);

    recovery.recoveryMasterFinished({3, 0}, false);
    EXPECT_EQ(1u, recovery.successfulRecoveryMasters);
    EXPECT_EQ(1u, recovery.unsuccessfulRecoveryMasters);
    EXPECT_EQ(Recovery::DONE, recovery.status);
}

TEST_F(RecoveryTest, broadcastRecoveryComplete) {
    addServersToTracker(3, {BACKUP_SERVICE});
    struct Cb : public BackupEndTaskTestingCallback {
        int callCount;
        Cb() : callCount() {}
        void backupEndTaskSend(ServerId backupId,
                               ServerId crashedServerId)
        {
            ++callCount;
        }
    } callback;
    Recovery recovery(taskQueue, &tracker, NULL, {99, 0}, tablets, 0lu);
    recovery.testingBackupEndTaskSendCallback = &callback;
    recovery.broadcastRecoveryComplete();
    EXPECT_EQ(3, callback.callCount);
}

} // namespace RAMCloud
