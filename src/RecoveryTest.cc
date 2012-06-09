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
#include "ProtoBuf.h"

namespace RAMCloud {

struct RecoveryTest : public ::testing::Test {
    TaskQueue taskQueue;
    RecoveryTracker tracker;
    ProtoBuf::Tablets tablets;

    RecoveryTest()
        : taskQueue()
        , tracker()
        , tablets()
    {
        Context::get().logger->setLogLevels(SILENT_LOG_LEVEL);
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
 * \param segmentIds
 *      List of segment ids which should be in the log digest.
 */
void
populateLogDigest(BackupClient::StartReadingData::Result& result,
                  std::vector<uint64_t> segmentIds)
{
    // Doesn't matter for these tests.
    result.logDigestSegmentLen = 100;
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

TEST_F(RecoveryTest, buildReplicaMap) {
    /**
     * Called by BackupStartTask instead of sending the startReadingData
     * RPC. The callback mocks out the result of the call for testing.
     * Each call into the callback corresponds to the send of the RPC
     * to an individual backup.
     */
    struct Cb : public RecoveryInternal::BackupStartTaskTestingCallback {
        int callCount;
        Cb() : callCount() {}
        void backupStartTaskSend(BackupClient::StartReadingData::Result& result)
        {
            if (callCount == 0) {
                // Two segments on backup1, one that overlaps with backup2
                // Includes a segment digest
                result.segmentIdAndLength.push_back({88lu, 100u});
                result.segmentIdAndLength.push_back({89lu, 100u});
                populateLogDigest(result, {88, 89});
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
    Recovery recovery(taskQueue, &tracker, NULL, ServerId(99), tablets, 0lu);
    recovery.testingBackupStartTaskSendCallback = &callback;
    recovery.buildReplicaMap();
    EXPECT_EQ((vector<RecoverRpc::Replica> {
                    { 1, 88 },
                    { 2, 88 },
                    { 1, 89 },
               }),
              recovery.replicaMap);
}

TEST_F(RecoveryTest, buildReplicaMap_secondariesEarlyInSomeList) {
    // See buildReplicaMap test for info about how the callback is used.
    struct Cb : public RecoveryInternal::BackupStartTaskTestingCallback {
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
                populateLogDigest(result, {88, 89, 90, 91});
                result.primarySegmentCount = 1;
            } else if (callCount == 2) {
                result.segmentIdAndLength.push_back({91lu, 100u});
                result.primarySegmentCount = 1;
            }
            callCount++;
        }
    } callback;
    addServersToTracker(3, {BACKUP_SERVICE});
    Recovery recovery(taskQueue, &tracker, NULL, ServerId(99), tablets, 0lu);
    recovery.testingBackupStartTaskSendCallback = &callback;
    recovery.buildReplicaMap();
    ASSERT_EQ(6U, recovery.replicaMap.size());
    // The secondary of segment 91 must be last in the list.
    EXPECT_EQ(91U, recovery.replicaMap.at(5).segmentId);
}

#if 0
static bool
verifyCompleteLogFilter(string s)
{
    return s == "verifyCompleteLog";
}

TEST_F(RecoveryTest, verifyCompleteLog) {
    // TODO(ongaro): The buildSegmentIdToBackups method needs to be
    // refactored before it can be reasonably tested (see RAM-243).
    // Sorry. Kick me off the project.
    TestLog::Enable _(&verifyCompleteLogFilter);
    ProtoBuf::Tablets tablets;
    Recovery recovery(ServerId(99), tablets, serverList);

    vector<Recovery::SegmentAndDigestTuple> oldDigestList =
        recovery.digestList;
    EXPECT_EQ(1, oldDigestList.size());

    // no head is very bad news.
    recovery.digestList.clear();
    EXPECT_THROW(recovery.verifyCompleteLog(), Exception);

    // ensure the newest head is chosen
    recovery.digestList = oldDigestList;
    recovery.digestList.push_back({ oldDigestList[0].segmentId + 1,
        oldDigestList[0].segmentLength,
        oldDigestList[0].logDigest.getRawPointer(),
        oldDigestList[0].logDigest.getBytes() });
    recovery.verifyCompleteLog();
    EXPECT_EQ("verifyCompleteLog: Segment 90 of length "
        "64 bytes is the head of the log", TestLog::get());

    // ensure the longest newest head is chosen
    TestLog::reset();
    recovery.digestList.push_back({ oldDigestList[0].segmentId + 1,
        oldDigestList[0].segmentLength + 1,
        oldDigestList[0].logDigest.getRawPointer(),
        oldDigestList[0].logDigest.getBytes() });
    recovery.verifyCompleteLog();
    EXPECT_EQ("verifyCompleteLog: Segment 90 of length "
        "65 bytes is the head of the log", TestLog::get());

    // ensure we log missing segments
    TestLog::reset();
    recovery.segmentMap.erase(88);
    recovery.verifyCompleteLog();
    EXPECT_EQ("verifyCompleteLog: Segment 90 of length 65 bytes "
        "is the head of the log | verifyCompleteLog: Segment 88 is missing!"
        " | verifyCompleteLog: 1 segments in the digest, but not obtained "
        "from backups!", TestLog::get());
}
#endif

TEST_F(RecoveryTest, startRecoveryMasters) {
    MockRandom _(1);
    struct Cb : public RecoveryInternal::MasterStartTaskTestingCallback {
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
    TabletsBuilder{tablets}
        (123,  0,  9, TabletsBuilder::RECOVERING, 0)
        (123, 20, 29, TabletsBuilder::RECOVERING, 0)
        (123, 10, 19, TabletsBuilder::RECOVERING, 1);
    addServersToTracker(2, {MASTER_SERVICE});
    Recovery recovery(taskQueue, &tracker, NULL, {99, 0}, tablets, 0lu);
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
    struct Cb : public RecoveryInternal::MasterStartTaskTestingCallback {
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
    TabletsBuilder{tablets}
        (123,  0,  9, TabletsBuilder::RECOVERING, 0)
        (123, 20, 29, TabletsBuilder::RECOVERING, 0)
        (123, 10, 19, TabletsBuilder::RECOVERING, 1);
    addServersToTracker(2, {MASTER_SERVICE});
    tracker[ServerId(1, 0)] = reinterpret_cast<Recovery*>(0x1);
    Recovery recovery(taskQueue, &tracker, NULL, {99, 0}, tablets, 0lu);
    recovery.testingMasterStartTaskSendCallback = &callback;
    recovery.startRecoveryMasters();

    recovery.recoveryMasterFinished({2, 0}, true);

    EXPECT_EQ(2u, recovery.numPartitions);
    EXPECT_EQ(1u, recovery.successfulRecoveryMasters);
    EXPECT_EQ(1u, recovery.unsuccessfulRecoveryMasters);
    EXPECT_TRUE(recovery.isDone());
    EXPECT_FALSE(recovery.wasCompletelySuccessful());
}

} // namespace RAMCloud
