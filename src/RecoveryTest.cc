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
#include "Memory.h"
#include "MockCluster.h"
#include "Recovery.h"
#include "ReplicaManager.h"
#include "ShortMacros.h"
#include "Tablets.pb.h"

namespace RAMCloud {

/**
 * Unit tests for Recovery.
 */
class RecoveryTest : public ::testing::Test {
  public:

    /**
     * Used to control precise timing of destruction of the Segment object
     * which implicitly calls freeSegment.
     */
    struct WriteValidSegment {
        ServerList serverList;
        ServerId masterId;
        ReplicaManager* mgr;
        void *segMem;
        Segment* seg;

        WriteValidSegment(Context& context,
                          ServerId serverId,
                          uint64_t segmentId,
                          vector<uint64_t> digestIds,
                          const uint32_t segmentSize,
                          const vector<const char*> locators,
                          bool close)
            : serverList(context)
            , masterId(serverId)
            , mgr()
            , segMem()
            , seg()
        {
            // TODO(ongaro): Jesus, this is the moral equivalent of linking
            // with libhphp.a.

            mgr = new ReplicaManager(context, serverList, masterId,
                                     downCast<uint32_t>(locators.size()), NULL);
            foreach (const auto& locator, locators) {
                size_t len = strlen(locator);
                uint32_t backupId =
                    boost::lexical_cast<uint32_t>(locator[len - 1]);
                serverList.add(ServerId(backupId, 0), locator,
                               {BACKUP_SERVICE}, 100);
            }

            segMem = Memory::xmemalign(HERE, segmentSize, segmentSize);
            seg = new Segment(masterId.getId(), segmentId,
                              segMem, segmentSize, mgr);

            char temp[LogDigest::getBytesFromCount(
                                        downCast<uint32_t>(digestIds.size()))];
            LogDigest ld(downCast<uint32_t>(digestIds.size()),
                         temp,
                         downCast<uint32_t>(sizeof(temp)));
            for (unsigned int i = 0; i < digestIds.size(); i++)
                ld.addSegment(digestIds[i]);
            seg->append(LOG_ENTRY_TYPE_LOGDIGEST, temp,
                        downCast<uint32_t>(sizeof(temp)));

            if (close)
                seg->close(NULL);
        }

        ~WriteValidSegment()
        {
            delete seg;
            free(segMem);
            delete mgr;
        }

        DISALLOW_COPY_AND_ASSIGN(WriteValidSegment);
    };

    Context context;
    Tub<MockCluster> cluster;
    std::unique_ptr<BackupClient> backup1;
    std::unique_ptr<BackupClient> backup2;
    std::unique_ptr<BackupClient> backup3;
    CoordinatorServerList* serverList;
    const uint32_t segmentSize;
    vector<WriteValidSegment*> segmentsToFree;

  public:
    RecoveryTest()
        : context()
        , cluster()
        , backup1()
        , backup2()
        , backup3()
        , serverList()
        , segmentSize(1 << 16)
        , segmentsToFree()
    {
        Logger::get().setLogLevels(SILENT_LOG_LEVEL);

        cluster.construct(context);

        ServerConfig config = ServerConfig::forTesting();
        config.services = {BACKUP_SERVICE, MEMBERSHIP_SERVICE};
        config.backup.numSegmentFrames = 3;
        config.segmentSize = segmentSize;
        config.localLocator = "mock:host=backup1";
        backup1 = cluster->get<BackupClient>(cluster->addServer(config));

        config.localLocator = "mock:host=backup2";
        backup2 = cluster->get<BackupClient>(cluster->addServer(config));

        config.localLocator = "mock:host=backup3";
        backup3 = cluster->get<BackupClient>(cluster->addServer(config));

        serverList = &cluster->coordinator->serverList;
    }

    ~RecoveryTest()
    {
        foreach (WriteValidSegment* s, segmentsToFree)
            delete s;
        cluster.destroy();
        EXPECT_EQ(0,
            BackupStorage::Handle::resetAllocatedHandlesCount());
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(RecoveryTest);
};

TEST_F(RecoveryTest, buildSegmentIdToBackups) {
    MockRandom _(1);
    // Two segs on backup1, one that overlaps with backup2
    segmentsToFree.push_back(
        new WriteValidSegment(context,
            ServerId(99, 0), 88, { 88 },
            segmentSize, {"mock:host=backup1"}, true));
    segmentsToFree.push_back(
        new WriteValidSegment(context,
            ServerId(99, 0), 89, { 88, 89 },
            segmentSize, {"mock:host=backup1"}, false));
    // One seg on backup2
    segmentsToFree.push_back(
        new WriteValidSegment(context,
            ServerId(99, 0), 88, { 88 },
            segmentSize, {"mock:host=backup2"}, true));
    // Zero segs on backup3

    ProtoBuf::Tablets tablets;
    Recovery recovery(context, ServerId(99), tablets, *serverList);
    EXPECT_EQ((vector<RecoverRpc::Replica> {
                    { 1, 88 },
                    { 2, 88 },
                    { 1, 89 },
               }),
              recovery.replicaLocations);
}

TEST_F(RecoveryTest, buildSegmentIdToBackups_secondariesEarlyInSomeList) {
    // Two segs on backup1, one that overlaps with backup2
    segmentsToFree.push_back(
        new WriteValidSegment(context,
            ServerId(99, 0), 88, { 88 },
            segmentSize, {"mock:host=backup1"}, true));
    segmentsToFree.push_back(
        new WriteValidSegment(context,
            ServerId(99, 0), 89, { 88, 89 },
            segmentSize, {"mock:host=backup1"}, true));
    // One seg on backup2
    segmentsToFree.push_back(
        new WriteValidSegment(context,
            ServerId(99, 0), 88, { 88 },
            segmentSize, {"mock:host=backup2"}, true));
    // Zero segs on backup3
    // Add one more primary to backup1
    // Add a primary/secondary segment pair to backup2 and backup3
    // No matter which host its placed on it appears earlier in the
    // segment list of 2 or 3 than the latest primary on 1 (which is
    // in slot 3).  Check to make sure the code prevents this secondary
    // from showing up before any primary in the list.
    segmentsToFree.push_back(
        new WriteValidSegment(context,
            ServerId(99, 0), 90, { 88, 89, 90 },
            segmentSize, {"mock:host=backup1"}, true));
    segmentsToFree.push_back(
        new WriteValidSegment(context,
            ServerId(99, 0), 91, { 88, 89, 90, 91 },
            segmentSize, {"mock:host=backup2", "mock:host=backup3"}, false));

    ProtoBuf::Tablets tablets;
    Recovery recovery(context, ServerId(99), tablets, *serverList);

    EXPECT_EQ(6U, recovery.replicaLocations.size());
    // The secondary of segment 91 must be last in the list.
    EXPECT_EQ(91U, recovery.replicaLocations.at(5).segmentId);
}

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
#if 0
    ProtoBuf::Tablets tablets;
    Recovery recovery(context, ServerId(99), tablets, serverList);

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
#endif
}

static bool
getRecoveryDataFilter(string s)
{
    return s == "getRecoveryData" ||
            s == "start";
}

TEST_F(RecoveryTest, start) {
    MockRandom __(1);

    // Two segs on backup1, one that overlaps with backup2
    segmentsToFree.push_back(
        new WriteValidSegment(context,
            ServerId(99, 0), 88, { 88 },
            segmentSize, {"mock:host=backup1"}, true));
    segmentsToFree.push_back(
        new WriteValidSegment(context,
            ServerId(99, 0), 89, { 88, 89 },
            segmentSize, {"mock:host=backup1"}, false));
    // One seg on backup2
    segmentsToFree.push_back(
        new WriteValidSegment(context,
            ServerId(99, 0), 88, { 88 },
            segmentSize, {"mock:host=backup2"}, true));
    // Zero segs on backup3

    ServerConfig config = ServerConfig::forTesting();
    config.services = {MASTER_SERVICE, MEMBERSHIP_SERVICE};
    config.localLocator = "mock:host=master1";
    cluster->addServer(config);
    config.localLocator = "mock:host=master2";
    cluster->addServer(config);

    ProtoBuf::Tablets tablets; {
        ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
        tablet.set_table_id(123);
        tablet.set_start_key_hash(0);
        tablet.set_end_key_hash(9);
        tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
        tablet.set_user_data(0); // partition 0
        tablet.set_ctime_log_head_id(0);
        tablet.set_ctime_log_head_offset(0);
    }{
        ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
        tablet.set_table_id(123);
        tablet.set_start_key_hash(20);
        tablet.set_end_key_hash(29);
        tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
        tablet.set_user_data(0); // partition 0
        tablet.set_ctime_log_head_id(0);
        tablet.set_ctime_log_head_offset(0);
    }{
        ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
        tablet.set_table_id(123);
        tablet.set_start_key_hash(10);
        tablet.set_end_key_hash(19);
        tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
        tablet.set_user_data(1); // partition 1
        tablet.set_ctime_log_head_id(0);
        tablet.set_ctime_log_head_offset(0);
    }

    Recovery recovery(context, ServerId(99), tablets, *serverList);

    /*
     * Make sure all segments are partitioned on the backups before proceeding,
     * otherwise test output can be non-deterministic since sometimes
     * RetryExceptions are throw and certain requests can be repeated.
     */
    while (true) {
        try {
            for (uint32_t partId = 0; partId < 2; ++partId) {
                {
                    Buffer throwAway;
                    backup1->getRecoveryData(
                        ServerId(99, 0), 88, partId, throwAway);
                }
                {
                    Buffer throwAway;
                    backup1->getRecoveryData(
                        ServerId(99, 0), 89, partId, throwAway);
                }
                {
                    Buffer throwAway;
                    backup2->getRecoveryData(
                        ServerId(99, 0), 88, partId, throwAway);
                }
            }
        } catch (const RetryException& e) {
            continue;
        }
        break;
    }

    TestLog::Enable _(&getRecoveryDataFilter);
    recovery.start();
    EXPECT_EQ(3U, recovery.tabletsUnderRecovery);
    EXPECT_EQ(
        "start: Starting recovery for 2 partitions | "
        "getRecoveryData: getRecoveryData masterId 99, segmentId 88, "
        "partitionId 0 | "
        "getRecoveryData: getRecoveryData complete | "
        "getRecoveryData: getRecoveryData masterId 99, segmentId 89, "
        "partitionId 0 | "
        "getRecoveryData: getRecoveryData complete | "
        "getRecoveryData: getRecoveryData masterId 99, segmentId 88, "
        "partitionId 1 | "
        "getRecoveryData: getRecoveryData complete | "
        "getRecoveryData: getRecoveryData masterId 99, segmentId 89, "
        "partitionId 1 | "
        "getRecoveryData: getRecoveryData complete",
        TestLog::get());
}

TEST_F(RecoveryTest, start_notEnoughMasters) {
    ServerConfig config = ServerConfig::forTesting();
    config.services = {MASTER_SERVICE};
    config.localLocator = "mock:host=master1";
    cluster->addServer(config);
    config.localLocator = "mock:host=master2";
    cluster->addServer(config);

    // Two segs on backup1, one that overlaps with backup2
    segmentsToFree.push_back(
        new WriteValidSegment(context,
            ServerId(99, 0), 88, { 88 },
            segmentSize, {"mock:host=backup1"}, true));
    segmentsToFree.push_back(
        new WriteValidSegment(context,
            ServerId(99, 0), 89, { 88, 89 },
            segmentSize, {"mock:host=backup1"}, false));
    // One seg on backup2
    segmentsToFree.push_back(
        new WriteValidSegment(context,
            ServerId(99, 0), 88, { 88 },
            segmentSize, {"mock:host=backup2"}, true));
    // Zero segs on backup3

    // Constructor should have created two masters.
    EXPECT_EQ(2U, cluster->coordinator->serverList.masterCount());

    ProtoBuf::Tablets tablets; {
        ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
        tablet.set_table_id(123);
        tablet.set_start_key_hash(0);
        tablet.set_end_key_hash(9);
        tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
        tablet.set_user_data(0); // partition 0
        tablet.set_ctime_log_head_id(0);
        tablet.set_ctime_log_head_offset(0);
    }{
        ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
        tablet.set_table_id(123);
        tablet.set_start_key_hash(10);
        tablet.set_end_key_hash(19);
        tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
        tablet.set_user_data(1); // partition 1
        tablet.set_ctime_log_head_id(0);
        tablet.set_ctime_log_head_offset(0);
    }{
        ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
        tablet.set_table_id(123);
        tablet.set_start_key_hash(20);
        tablet.set_end_key_hash(29);
        tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
        tablet.set_user_data(2); // partition 2
        tablet.set_ctime_log_head_id(0);
        tablet.set_ctime_log_head_offset(0);
    }

    Recovery recovery(context, ServerId(99), tablets,
            *serverList);
    MockRandom __(1); // triggers deterministic rand().
    TestLog::Enable _(&getRecoveryDataFilter);
    EXPECT_THROW(recovery.start(), FatalError);
}

} // namespace RAMCloud
