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

#include <sstream>
#include "TestUtil.h"
#include "CoordinatorUpdateManager.h"
#include "MockExternalStorage.h"
#include "Recovery.h"
#include "ShortMacros.h"
#include "TabletsBuilder.h"
#include "ProtoBuf.h"

namespace RAMCloud {

using namespace RecoveryInternal; // NOLINT

struct RecoveryTest : public ::testing::Test {
    typedef StartReadingDataRpc::Replica Replica;
    Context context;
    TaskQueue taskQueue;
    RecoveryTracker tracker;
    ServerList serverList;
    MockExternalStorage storage;
    CoordinatorUpdateManager manager;
    TableManager tableManager;
    ProtoBuf::MasterRecoveryInfo recoveryInfo;
    std::mutex mutex;

    RecoveryTest()
        : context()
        , taskQueue()
        , tracker(&context)
        , serverList(&context)
        , storage(true)
        , manager(&storage)
        , tableManager(&context, &manager)
        , recoveryInfo()
        , mutex()
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

    typedef std::unique_lock<std::mutex> Lock;

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
populateLogDigest(StartReadingDataRpc::Result& result,
                  uint64_t segmentId,
                  std::vector<uint64_t> segmentIds)
{
    // Doesn't matter for these tests.
    LogDigest digest;
    foreach (uint64_t id, segmentIds)
        digest.addSegmentId(id);

    Buffer buffer;
    digest.appendToBuffer(buffer);
    result.logDigestSegmentEpoch = 100;
    result.logDigestSegmentId = segmentId;
    result.logDigestBytes = buffer.size();
    result.logDigestBuffer =
        std::unique_ptr<char[]>(new char[result.logDigestBytes]);
    buffer.copy(0, buffer.size(), result.logDigestBuffer.get());
}
} // namespace

TEST_F(RecoveryTest, splitTablets_no_estimator) {
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Recovery> recovery;
    Recovery::Owner* own = static_cast<Recovery::Owner*>(NULL);
    tableManager.testCreateTable("t1", 1);
    tableManager.testAddTablet({1,  0,  9, {99, 0}, Tablet::RECOVERING, {}});
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       ServerId(99), recoveryInfo);
    auto tablets = tableManager.markAllTabletsRecovering(ServerId(99));

    EXPECT_EQ(1u, tablets.size());

    recovery->splitTablets(&tablets, NULL);

    Tablet* tablet = NULL;

    EXPECT_EQ(1u, tablets.size());

    EXPECT_EQ("{ 1: 0x0-0x9 }", tablets[0].debugString(1));
    tablet = tableManager.testFindTablet(1u, 0u);
    EXPECT_TRUE(tablet != NULL);
    if (tablet != NULL) {
        EXPECT_EQ("{ 1: 0x0-0x9 }", tablet->debugString(1));
    }
    tablet = NULL;
}

TEST_F(RecoveryTest, splitTablets_bad_estimator) {
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Recovery> recovery;
    Recovery::Owner* own = static_cast<Recovery::Owner*>(NULL);
    tableManager.testCreateTable("t1", 1);
    tableManager.testAddTablet({1,  0,  9, {99, 0}, Tablet::RECOVERING, {}});
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       ServerId(99), recoveryInfo);
    auto tablets = tableManager.markAllTabletsRecovering(ServerId(99));

    TableStats::Estimator e(NULL, &tablets);

    EXPECT_FALSE(e.valid);

    EXPECT_EQ(1u, tablets.size());

    recovery->splitTablets(&tablets, &e);

    Tablet* tablet = NULL;

    EXPECT_EQ(1u, tablets.size());

    EXPECT_EQ("{ 1: 0x0-0x9 }", tablets[0].debugString(1));
    tablet = tableManager.testFindTablet(1u, 0u);
    EXPECT_TRUE(tablet != NULL);
    if (tablet != NULL) {
        EXPECT_EQ("{ 1: 0x0-0x9 }", tablets[0].debugString(1));
    }
    tablet = NULL;
}

TEST_F(RecoveryTest, splitTablets_multi_tablet) {
    // Case where there is more than one tablet.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Recovery> recovery;
    Recovery::Owner* own = static_cast<Recovery::Owner*>(NULL);
    tableManager.testCreateTable("t1", 1);
    tableManager.testAddTablet({1,  0,  9, {99, 0}, Tablet::RECOVERING, {}});
    tableManager.testCreateTable("t2", 2);
    tableManager.testAddTablet({2,  0,  19, {99, 0}, Tablet::RECOVERING, {}});
    tableManager.testCreateTable("t3", 3);
    tableManager.testAddTablet({3,  0,  29, {99, 0}, Tablet::RECOVERING, {}});
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       ServerId(99), recoveryInfo);
    auto tablets = tableManager.markAllTabletsRecovering(ServerId(99));

    char buffer[sizeof(TableStats::DigestHeader) +
                3 * sizeof(TableStats::DigestEntry)];
    TableStats::Digest* digest = reinterpret_cast<TableStats::Digest*>(buffer);
    digest->header.entryCount = 3;
    digest->header.otherByteCount = 0;
    digest->header.otherRecordCount = 0;
    digest->entries[0].tableId = 1;
    digest->entries[0].byteCount = 1 * 600*1024*1024;
    digest->entries[0].recordCount = 1 * 3000000;

    digest->entries[1].tableId = 2;
    digest->entries[1].byteCount = 2 * 600*1024*1024;
    digest->entries[1].recordCount = 2 * 3000000;

    digest->entries[2].tableId = 3;
    digest->entries[2].byteCount = 3 * 600*1024*1024;
    digest->entries[2].recordCount = 3 * 3000000;

    TableStats::Estimator e(digest, &tablets);

    EXPECT_TRUE(e.valid);

    EXPECT_EQ(3u, tablets.size());
    recovery->splitTablets(&tablets, &e);
    EXPECT_EQ(6u, tablets.size());
}

TEST_F(RecoveryTest, splitTablets_indexlet) {
    // Case where there is more than one tablet.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Recovery> recovery;
    Recovery::Owner* own = static_cast<Recovery::Owner*>(NULL);
    tableManager.testCreateTable("t1", 1);
    tableManager.testAddTablet({1,  0,  9, {99, 0}, Tablet::RECOVERING, {}});
    tableManager.testCreateTable("t2", 2);
    tableManager.testAddTablet({2,  0,  19, {99, 0}, Tablet::RECOVERING, {}});
    tableManager.testCreateTable("__indexTable:1:0:0", 3);
    tableManager.testAddTablet({3,  0,  ~0UL, {99, 0}, Tablet::RECOVERING, {}});
    tableManager.createIndex(1, 0, 0, 1);
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       ServerId(99), recoveryInfo);
    auto tablets = tableManager.markAllTabletsRecovering(ServerId(99));

    char buffer[sizeof(TableStats::DigestHeader) +
                3 * sizeof(TableStats::DigestEntry)];
    TableStats::Digest* digest = reinterpret_cast<TableStats::Digest*>(buffer);
    digest->header.entryCount = 3;
    digest->header.otherByteCount = 0;
    digest->header.otherRecordCount = 0;
    digest->entries[0].tableId = 1;
    digest->entries[0].byteCount = 1 * 600*1024*1024;
    digest->entries[0].recordCount = 1 * 3000000;

    digest->entries[1].tableId = 2;
    digest->entries[1].byteCount = 2 * 600*1024*1024;
    digest->entries[1].recordCount = 2 * 3000000;

    digest->entries[2].tableId = 3;
    digest->entries[2].byteCount = 3 * 600*1024*1024;
    digest->entries[2].recordCount = 3 * 3000000;

    TableStats::Estimator e(digest, &tablets);

    EXPECT_TRUE(e.valid);

    EXPECT_EQ(3u, tablets.size());
    recovery->splitTablets(&tablets, &e);
    EXPECT_EQ(4u, tablets.size());
}

TEST_F(RecoveryTest, splitTablets_basic) {
    // Tests that a basic split of a single tablet will execute and update the
    // TableManager tablet information.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Recovery> recovery;
    Recovery::Owner* own = static_cast<Recovery::Owner*>(NULL);
    tableManager.testCreateTable("t1", 1);
    tableManager.testAddTablet({1,  0,  4, {99, 0}, Tablet::RECOVERING, {}});
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       ServerId(99), recoveryInfo);
    auto tablets = tableManager.markAllTabletsRecovering(ServerId(99));

    char buffer[sizeof(TableStats::DigestHeader)];
    TableStats::Digest* digest = reinterpret_cast<TableStats::Digest*>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherByteCount = 3 * 600*1024*1024;
    digest->header.otherRecordCount = 3 * 3000000;

    TableStats::Estimator e(digest, &tablets);
    EXPECT_TRUE(e.valid);

    EXPECT_EQ(1u, tablets.size());

    recovery->splitTablets(&tablets, &e);

    Tablet* tablet = NULL;

    EXPECT_EQ(3u, tablets.size());

    // Check Tablet Contents
    EXPECT_EQ("{ 1: 0x0-0x1 }", tablets[0].debugString(1));
    tablet = tableManager.testFindTablet(1u, 0u);
    EXPECT_TRUE(tablet != NULL);
    if (tablet != NULL) {
        EXPECT_EQ("{ 1: 0x0-0x1 }", tablets[0].debugString(1));
    }
    tablet = NULL;

    // Check Tablet Contents
    EXPECT_EQ("{ 1: 0x2-0x3 }", tablets[1].debugString(1));
    tablet = tableManager.testFindTablet(1u, 0u);
    EXPECT_TRUE(tablet != NULL);
    if (tablet != NULL) {
        EXPECT_EQ("{ 1: 0x2-0x3 }", tablets[1].debugString(1));
    }
    tablet = NULL;

    // Check Tablet Contents
    EXPECT_EQ("{ 1: 0x4-0x4 }", tablets[2].debugString(1));
    tablet = tableManager.testFindTablet(1u, 0u);
    EXPECT_TRUE(tablet != NULL);
    if (tablet != NULL) {
        EXPECT_EQ("{ 1: 0x4-0x4 }", tablets[2].debugString(1));
    }
    tablet = NULL;
}

TEST_F(RecoveryTest, splitTablets_byte_dominated) {
    // Ensure the tablet count can be determined by the byte count.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Recovery> recovery;
    Recovery::Owner* own = static_cast<Recovery::Owner*>(NULL);
    tableManager.testCreateTable("t1", 1);
    tableManager.testAddTablet({1,  0, 999, {99, 0}, Tablet::RECOVERING, {}});
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       ServerId(99), recoveryInfo);
    auto tablets = tableManager.markAllTabletsRecovering(ServerId(99));

    char buffer[sizeof(TableStats::DigestHeader)];
    TableStats::Digest* digest = reinterpret_cast<TableStats::Digest*>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherByteCount = 6l * 600*1024*1024;
    digest->header.otherRecordCount = 2 * 3000000;

    TableStats::Estimator e(digest, &tablets);
    EXPECT_TRUE(e.valid);

    EXPECT_EQ(1u, tablets.size());

    recovery->splitTablets(&tablets, &e);

    EXPECT_EQ(6u, tablets.size());
}

TEST_F(RecoveryTest, splitTablets_record_dominated) {
    // Ensure the tablet count can be determined by the record count.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Recovery> recovery;
    Recovery::Owner* own = static_cast<Recovery::Owner*>(NULL);
    tableManager.testCreateTable("t1", 1);
    tableManager.testAddTablet({1,  0, 999, {99, 0}, Tablet::RECOVERING, {}});
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       ServerId(99), recoveryInfo);
    auto tablets = tableManager.markAllTabletsRecovering(ServerId(99));

    char buffer[sizeof(TableStats::DigestHeader)];
    TableStats::Digest* digest = reinterpret_cast<TableStats::Digest*>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherByteCount = 2 * 600*1024*1024;
    digest->header.otherRecordCount = 6l * 3000000;

    TableStats::Estimator e(digest, &tablets);
    EXPECT_TRUE(e.valid);

    EXPECT_EQ(1u, tablets.size());

    recovery->splitTablets(&tablets, &e);

    EXPECT_EQ(6u, tablets.size());
}

TEST_F(RecoveryTest, splitTablets_key_limited) {
    // Ensure the tablet count can be limited by the key range.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Recovery> recovery;
    Recovery::Owner* own = static_cast<Recovery::Owner*>(NULL);
    tableManager.testCreateTable("t1", 1);
    tableManager.testAddTablet({1,  0, 3, {99, 0}, Tablet::RECOVERING, {}});
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       ServerId(99), recoveryInfo);
    auto tablets = tableManager.markAllTabletsRecovering(ServerId(99));

    char buffer[sizeof(TableStats::DigestHeader)];
    TableStats::Digest* digest = reinterpret_cast<TableStats::Digest*>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherByteCount = 2 * 600*1024*1024;
    digest->header.otherRecordCount = 6l * 3000000;

    TableStats::Estimator e(digest, &tablets);
    EXPECT_TRUE(e.valid);

    EXPECT_EQ(1u, tablets.size());

    recovery->splitTablets(&tablets, &e);

    EXPECT_EQ(4u, tablets.size());
}

TEST_F(RecoveryTest, splitTablets_no_splits) {
    // Test case when no split needs to be performed.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Recovery> recovery;
    Recovery::Owner* own = static_cast<Recovery::Owner*>(NULL);
    tableManager.testCreateTable("t1", 1);
    tableManager.testAddTablet({1,  0, 999, {99, 0}, Tablet::RECOVERING, {}});
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       ServerId(99), recoveryInfo);
    auto tablets = tableManager.markAllTabletsRecovering(ServerId(99));

    char buffer[sizeof(TableStats::DigestHeader) +
                0 * sizeof(TableStats::DigestEntry)];
    TableStats::Digest* digest = reinterpret_cast<TableStats::Digest*>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherByteCount = 1 * 600*1024*1024;
    digest->header.otherRecordCount = 1 * 3000000;

    TableStats::Estimator e(digest, &tablets);
    EXPECT_TRUE(e.valid);

    EXPECT_EQ(1u, tablets.size());

    recovery->splitTablets(&tablets, &e);

    EXPECT_EQ(1u, tablets.size());
}

TEST_F(RecoveryTest, splitTablets_even_split) {
    // Case where all resulting tablets should be equal and thus only executes
    // the "keyRangeBig" loop. This also tests the only case where "keyRange" is
    // zero (see comments in splitTablet for explanation of correct behavior).
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Recovery> recovery;
    Recovery::Owner* own = static_cast<Recovery::Owner*>(NULL);
    tableManager.testCreateTable("t1", 1);
    tableManager.testAddTablet({1, 1, 4, {99, 0}, Tablet::RECOVERING, {}});
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       ServerId(99), recoveryInfo);
    auto tablets = tableManager.markAllTabletsRecovering(ServerId(99));

    char buffer[sizeof(TableStats::DigestHeader)];
    TableStats::Digest* digest = reinterpret_cast<TableStats::Digest*>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherByteCount = 5l * 600*1024*1024;
    digest->header.otherRecordCount = 5l * 3000000;

    TableStats::Estimator e(digest, &tablets);
    EXPECT_TRUE(e.valid);

    EXPECT_EQ(1u, tablets.size());

    recovery->splitTablets(&tablets, &e);

    EXPECT_EQ(4u, tablets.size());
    EXPECT_EQ("{ 1: 0x1-0x1 }", tablets[0].debugString(1));
    EXPECT_EQ("{ 1: 0x2-0x2 }", tablets[1].debugString(1));
    EXPECT_EQ("{ 1: 0x3-0x3 }", tablets[2].debugString(1));
    EXPECT_EQ("{ 1: 0x4-0x4 }", tablets[3].debugString(1));
}

TEST_F(RecoveryTest, splitTablets_uneven_split) {
    // Case where some tablets will will be big and some will be small.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Recovery> recovery;
    Recovery::Owner* own = static_cast<Recovery::Owner*>(NULL);
    tableManager.testCreateTable("t1", 1);
    tableManager.testAddTablet({1, 1, 8, {99, 0}, Tablet::RECOVERING, {}});
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       ServerId(99), recoveryInfo);
    auto tablets = tableManager.markAllTabletsRecovering(ServerId(99));

    char buffer[sizeof(TableStats::DigestHeader)];
    TableStats::Digest* digest = reinterpret_cast<TableStats::Digest*>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherByteCount = 5l * 600*1024*1024;
    digest->header.otherRecordCount = 5l * 3000000;

    TableStats::Estimator e(digest, &tablets);
    EXPECT_TRUE(e.valid);

    EXPECT_EQ(1u, tablets.size());

    recovery->splitTablets(&tablets, &e);

    EXPECT_EQ(5u, tablets.size());
    EXPECT_EQ("{ 1: 0x1-0x2 }", tablets[0].debugString(1));
    EXPECT_EQ("{ 1: 0x3-0x4 }", tablets[1].debugString(1));
    EXPECT_EQ("{ 1: 0x5-0x6 }", tablets[2].debugString(1));
    EXPECT_EQ("{ 1: 0x7-0x7 }", tablets[3].debugString(1));
    EXPECT_EQ("{ 1: 0x8-0x8 }", tablets[4].debugString(1));
}

TEST_F(RecoveryTest, splitTablets_full_table) {
    // Case where there is full key range.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Recovery> recovery;
    Recovery::Owner* own = static_cast<Recovery::Owner*>(NULL);
    tableManager.testCreateTable("t", 1);
    tableManager.testAddTablet(
        {1,  0,  0xFFFFFFFFFFFFFFFF, {99, 0}, Tablet::RECOVERING, {}});
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       ServerId(99), recoveryInfo);
    auto tablets = tableManager.markAllTabletsRecovering(ServerId(99));

    char buffer[sizeof(TableStats::DigestHeader)];
    TableStats::Digest* digest = reinterpret_cast<TableStats::Digest*>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherByteCount = 2 * 600*1024*1024;
    digest->header.otherRecordCount = 2 * 3000000;

    TableStats::Estimator e(digest, &tablets);

    EXPECT_TRUE(e.valid);

    EXPECT_EQ(1u, tablets.size());

    recovery->splitTablets(&tablets, &e);

    EXPECT_EQ(2u, tablets.size());

    EXPECT_EQ("{ 1: 0x0-0x7fffffffffffffff }", tablets[0].debugString(1));
    EXPECT_EQ("{ 1: 0x8000000000000000-0xffffffffffffffff }",
              tablets[1].debugString(1));
}

TEST_F(RecoveryTest, partitionTablets_no_estimator) {
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Recovery> recovery;
    Recovery::Owner* own = static_cast<Recovery::Owner*>(NULL);
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       ServerId(99), recoveryInfo);
    auto tablets = tableManager.markAllTabletsRecovering(ServerId(99));
    recovery->partitionTablets(tablets, NULL);
    EXPECT_EQ(0lu, recovery->numPartitions);

    tableManager.testCreateTable("t", 123);
    tableManager.testAddTablet({123,  0,  9, {99, 0}, Tablet::RECOVERING, {}});
    tableManager.testAddTablet({123, 20, 29, {99, 0}, Tablet::RECOVERING, {}});
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       ServerId(99), recoveryInfo);
    tablets = tableManager.markAllTabletsRecovering(ServerId(99));
    recovery->partitionTablets(tablets, NULL);
    EXPECT_EQ(2lu, recovery->numPartitions);

    tableManager.testAddTablet({123, 10, 19, {99, 0}, Tablet::RECOVERING, {}});
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       ServerId(99), recoveryInfo);
    tablets = tableManager.markAllTabletsRecovering(ServerId(99));
    recovery->partitionTablets(tablets, NULL);
    EXPECT_EQ(3lu, recovery->numPartitions);
}

TEST_F(RecoveryTest, partitionTablets_splits) {
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Recovery> recovery;
    Recovery::Owner* own = static_cast<Recovery::Owner*>(NULL);

    tableManager.testCreateTable("t1", 1);
    tableManager.testAddTablet({1,  0,  99, {99, 0}, Tablet::RECOVERING, {}});

    tableManager.testCreateTable("t2", 2);
    tableManager.testAddTablet({2,  0,  99, {99, 0}, Tablet::RECOVERING, {}});
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       ServerId(99), recoveryInfo);
    auto tablets = tableManager.markAllTabletsRecovering(ServerId(99));

    char buffer[sizeof(TableStats::DigestHeader) +
                2 * sizeof(TableStats::DigestEntry)];
    TableStats::Digest* digest = reinterpret_cast<TableStats::Digest*>(buffer);
    digest->header.entryCount = 2;
    digest->header.otherByteCount = 0;
    digest->header.otherRecordCount = 0;
    digest->entries[0].tableId = 1;
    digest->entries[0].byteCount = 10l * 600*1024*1024;
    digest->entries[0].recordCount = 1 * 3000000;
    digest->entries[1].tableId = 2;
    digest->entries[1].byteCount = 1 * 600*1024*1024;
    digest->entries[1].recordCount = 10l * 3000000;

    TableStats::Estimator e(digest, &tablets);

    recovery->partitionTablets(tablets, &e);
    EXPECT_EQ(20lu, recovery->numPartitions);
}

TEST_F(RecoveryTest, partitionTablets_basic) {
    // This covers the following cases:
    //      (1) No partitions to choose from
    //      (2) Single partiton to choose from
    //      (3) Evicting a partition
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Recovery> recovery;
    Recovery::Owner* own = static_cast<Recovery::Owner*>(NULL);
    for (int i = 1; i <= 250; i++) {
        tableManager.testCreateTable(TestUtil::toString(i).c_str(), i);
        tableManager.testAddTablet(
            {i,  0,  9, {99, 0}, Tablet::RECOVERING, {}});
    }
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       ServerId(99), recoveryInfo);
    auto tablets = tableManager.markAllTabletsRecovering(ServerId(99));

    char buffer[sizeof(TableStats::DigestHeader) +
                0 * sizeof(TableStats::DigestEntry)];
    TableStats::Digest* digest = reinterpret_cast<TableStats::Digest*>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherByteCount = 250l * 6*1024*1024;
    digest->header.otherRecordCount = 250l * 30000;

    TableStats::Estimator e(digest, &tablets);

    recovery->partitionTablets(tablets, &e);
    EXPECT_EQ(3lu, recovery->numPartitions);
}

TEST_F(RecoveryTest, partitionTablets_all_partitions_open) {
    // Covers the addtional case where no partitions are large enough to be
    // evicted, there is more than one partition to choose from and none will
    // fit.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Recovery> recovery;
    Recovery::Owner* own = static_cast<Recovery::Owner*>(NULL);
    for (int i = 1; i <= 20; i++) {
        tableManager.testCreateTable(TestUtil::toString(i).c_str(), i);
        tableManager.testAddTablet(
            {i,  0,  9, {99, 0}, Tablet::RECOVERING, {}});
    }
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       ServerId(99), recoveryInfo);
    auto tablets = tableManager.markAllTabletsRecovering(ServerId(99));

    char buffer[sizeof(TableStats::DigestHeader) +
                0 * sizeof(TableStats::DigestEntry)];
    TableStats::Digest* digest = reinterpret_cast<TableStats::Digest*>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherByteCount = 21l * 300*1024*1024;
    digest->header.otherRecordCount = 21l * 1500000;

    TableStats::Estimator e(digest, &tablets);

    recovery->partitionTablets(tablets, &e);
    EXPECT_EQ(20lu, recovery->numPartitions);
}

TEST_F(RecoveryTest, partitionTablets_mixed) {
    // Covers the addtional case where partitions will contain one large tablet
    // and filled by many smaller ones.  Case covers selecting from multiple
    // partitons and having a tablet fit.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Recovery> recovery;
    Recovery::Owner* own = static_cast<Recovery::Owner*>(NULL);
    for (int i = 1; i <= 6; i++) {
        tableManager.testCreateTable(TestUtil::toString(i).c_str(), i);
        tableManager.testAddTablet(
            {i,  0,  99, {99, 0}, Tablet::RECOVERING, {}});
    }
    for (int i = 1; i <= 250; i++) {
        tableManager.testCreateTable(TestUtil::toString(i+100).c_str(), i+100);
        tableManager.testAddTablet(
            {i + 100,  0,  9, {99, 0}, Tablet::RECOVERING, {}});
    }
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       ServerId(99), recoveryInfo);
    auto tablets = tableManager.markAllTabletsRecovering(ServerId(99));

    char buffer[sizeof(TableStats::DigestHeader) +
                0 * sizeof(TableStats::DigestEntry)];
    TableStats::Digest* digest = reinterpret_cast<TableStats::Digest*>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherByteCount = 500l * 6*1024*1024;
    digest->header.otherRecordCount = 500l * 30000;

    TableStats::Estimator e(digest, &tablets);

    recovery->partitionTablets(tablets, &e);
    EXPECT_EQ(6lu, recovery->numPartitions);
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
        void backupStartTaskSend(StartReadingDataRpc::Result& result)
        {
            if (callCount == 0) {
                // Two segments on backup1, one that overlaps with backup2
                // Includes a segment digest
                result.replicas.push_back(Replica{88lu, 100lu, false});
                result.replicas.push_back(Replica{89lu, 100lu, true});
                populateLogDigest(result, 89, {88, 89});
                result.primaryReplicaCount = 1;
            } else if (callCount == 1) {
                // One segment on backup2
                result.replicas.push_back(Replica{88lu, 100lu, false});
                result.primaryReplicaCount = 1;
            } else if (callCount == 2) {
                // No segments on backup3
            }
            callCount++;
        }
    } callback;
    Lock lock(mutex);     // To trick TableManager internal calls.
    addServersToTracker(3, {WireFormat::BACKUP_SERVICE});
    tableManager.testCreateTable("t", 123);
    tableManager.testAddTablet({123, 10, 19, {99, 0}, Tablet::RECOVERING, {}});
    Recovery recovery(&context, taskQueue, &tableManager, &tracker, NULL,
                      ServerId(99), recoveryInfo);
    recovery.testingBackupStartTaskSendCallback = &callback;
    recovery.startBackups();
    EXPECT_EQ((vector<WireFormat::Recover::Replica>{
                    { 1, 88 },
                    { 2, 88 },
                    { 1, 89 },
               }),
              recovery.replicaMap);
}

TEST_F(RecoveryTest, startBackups_failureContactingSomeBackup) {
    Recovery recovery(&context, taskQueue, &tableManager, &tracker, NULL,
                      ServerId(99), recoveryInfo);
    BackupStartTask task(&recovery, {2, 0});
    EXPECT_NO_THROW(task.send());
}

TEST_F(RecoveryTest, startBackups_secondariesEarlyInSomeList) {
    // See buildReplicaMap test for info about how the callback is used.
    struct Cb : public BackupStartTask::TestingCallback {
        int callCount;
        Cb() : callCount() {}
        void backupStartTaskSend(StartReadingDataRpc::Result& result)
        {
            if (callCount == 0) {
                result.replicas.push_back(Replica{88lu, 100u, false});
                result.replicas.push_back(Replica{89lu, 100u, false});
                result.replicas.push_back(Replica{90lu, 100u, false});
                result.primaryReplicaCount = 3;
            } else if (callCount == 1) {
                result.replicas.push_back(Replica{88lu, 100u, false});
                result.replicas.push_back(Replica{91lu, 100u, true});
                populateLogDigest(result, 91, {88, 89, 90, 91});
                result.primaryReplicaCount = 1;
            } else if (callCount == 2) {
                result.replicas.push_back(Replica{91lu, 100u, true});
                result.primaryReplicaCount = 1;
            }
            callCount++;
        }
    } callback;
    Lock lock(mutex);     // To trick TableManager internal calls.
    addServersToTracker(3, {WireFormat::BACKUP_SERVICE});
    tableManager.testCreateTable("t", 123);
    tableManager.testAddTablet({123, 10, 19, {99, 0}, Tablet::RECOVERING, {}});
    Recovery recovery(&context, taskQueue, &tableManager, &tracker, NULL,
                      ServerId(99), recoveryInfo);
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
    Lock lock(mutex);     // To trick TableManager internal calls.
    addServersToTracker(3, {WireFormat::BACKUP_SERVICE});
    tableManager.testCreateTable("t", 123);
    tableManager.testAddTablet({123, 10, 19, {99, 0}, Tablet::RECOVERING, {}});
    Recovery recovery(&context, taskQueue, &tableManager, &tracker, NULL,
                      ServerId(99), recoveryInfo);
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
        void backupStartTaskSend(StartReadingDataRpc::Result& result)
        {
            populateLogDigest(result, 91, {91});
        }
    } callback;
    Lock lock(mutex);     // To trick TableManager internal calls.
    addServersToTracker(3, {WireFormat::BACKUP_SERVICE});
    tableManager.testCreateTable("t", 123);
    tableManager.testAddTablet({123, 10, 19, {99, 0}, Tablet::RECOVERING, {}});
    Recovery recovery(&context, taskQueue, &tableManager, &tracker, NULL,
                      ServerId(99), recoveryInfo);
    recovery.testingBackupStartTaskSendCallback = &callback;
    TestLog::Enable _(startBackupsFilter);
    recovery.startBackups();
    EXPECT_EQ(
        "startBackups: Getting segment lists from backups and preparing "
            "them for recovery | "
        "startBackups: Segment 91 is the head of the log | "
        "startBackups: Some replicas from log digest not on available backups. "
            "Will retry recovery later.", TestLog::get());
}

TEST_F(RecoveryTest, BackupStartTask_filterOutInvalidReplicas) {
    recoveryInfo.set_min_open_segment_id(10);
    recoveryInfo.set_min_open_segment_epoch(1);
    Recovery recovery(&context, taskQueue, &tableManager, &tracker, NULL,
                      {1, 0}, recoveryInfo);
    BackupStartTask task(&recovery, {2, 0});
    auto& segments = task.result.replicas;
    segments = {
        {2, 1, true},
        {2, 0, false},
        {10, 1, false},
        {10, 0, true},
        {10, 0, false},
        {11, 1, true},
        {11, 0, false},
    };

    task.result.primaryReplicaCount = 2;
    uint32_t bytes = 4; // Doesn't really matter.
    task.result.logDigestBytes = bytes;
    task.result.logDigestBuffer = std::unique_ptr<char[]>(new char[bytes]);
    task.result.logDigestSegmentId = 9;
    task.result.logDigestSegmentEpoch = 1;
    task.filterOutInvalidReplicas();
    ASSERT_EQ(5lu, segments.size());
    EXPECT_EQ((vector<StartReadingDataRpc::Replica>{
                    { 2, 1, true },
                    { 10, 1, false },
                    { 10, 0, true },
                    { 11, 1, true },
                    { 11, 0, false },
               }),
              segments);
    EXPECT_EQ(1lu, task.result.primaryReplicaCount);
    EXPECT_EQ(0u, task.result.logDigestBytes);
    EXPECT_EQ(static_cast<char*>(NULL), task.result.logDigestBuffer.get());
    EXPECT_EQ(~0lu, task.result.logDigestSegmentId);
    EXPECT_EQ(~0lu, task.result.logDigestSegmentEpoch);
}

TEST_F(RecoveryTest, verifyLogComplete) {
    LogDigest digest;
    digest.addSegmentId(10);
    digest.addSegmentId(11);
    digest.addSegmentId(12);


    Tub<BackupStartTask> tasks[1];
    Recovery recovery(&context, taskQueue, &tableManager, &tracker, NULL,
            {1, 0}, recoveryInfo);
    tasks[0].construct(&recovery, ServerId(2, 0));
    auto& segments = tasks[0]->result.replicas;

    TestLog::Enable _;
    segments = {{10, 0, false}, {12, 0, true}};
    EXPECT_FALSE(verifyLogComplete(tasks, 1, digest));
    EXPECT_EQ(
        "verifyLogComplete: Segment 11 listed in the log digest but "
            "not found among available backups | "
        "verifyLogComplete: 1 segments in the digest but not available "
            "from backups", TestLog::get());
    segments = {{10, 0, false}, {11, 0, false}, {12, 0, true}};
    EXPECT_TRUE(verifyLogComplete(tasks, 1, digest));
}

TEST_F(RecoveryTest, findLogDigest) {
    recoveryInfo.set_min_open_segment_id(10);
    recoveryInfo.set_min_open_segment_epoch(1);
    Tub<BackupStartTask> tasks[2];
    Recovery recovery(&context, taskQueue, &tableManager, &tracker, NULL,
            {1, 0}, recoveryInfo);
    tasks[0].construct(&recovery, ServerId(2, 0));
    tasks[1].construct(&recovery, ServerId(3, 0));

    // No log digest found.
    auto digest = findLogDigest(tasks, 2);
    EXPECT_FALSE(digest);

    auto& result0 = tasks[0]->result;
    auto& result1 = tasks[1]->result;

    // Two digests with different contents to differentiate them below.
    LogDigest result0Digest, result1Digest;
    result0Digest.addSegmentId(0);
    result1Digest.addSegmentId(1);

    Buffer result0Buffer, result1Buffer;
    result0Digest.appendToBuffer(result0Buffer);
    result1Digest.appendToBuffer(result1Buffer);

    uint32_t bytes = result0Buffer.size();

    result0.logDigestBytes = bytes;
    result0.logDigestBuffer = std::unique_ptr<char[]>(new char[bytes]);
    result0.logDigestSegmentId = 10;
    result0Buffer.copy(0, bytes, result0.logDigestBuffer.get());

    result1.logDigestBytes = bytes;
    result1.logDigestBuffer = std::unique_ptr<char[]>(new char[bytes]);
    result1.logDigestSegmentId = 10;
    result1Buffer.copy(0, bytes, result1.logDigestBuffer.get());

    // Two log digests, same segment id, keeps earlier of two.
    digest = findLogDigest(tasks, 2);
    ASSERT_TRUE(digest);
    EXPECT_EQ(10lu, std::get<0>(*digest.get()));
    EXPECT_EQ(0u, std::get<1>(*digest.get())[0]);

    result1.logDigestSegmentId = 9;
    // Two log digests, later one has a lower segment id.
    digest = findLogDigest(tasks, 2);
    ASSERT_TRUE(digest);
    EXPECT_EQ(9lu, std::get<0>(*digest.get()));
    EXPECT_EQ(1u, std::get<1>(*digest.get())[0]);
}

TEST_F(RecoveryTest, buildReplicaMap) {
    Tub<BackupStartTask> tasks[2];
    Recovery recovery(&context, taskQueue, &tableManager, &tracker, NULL,
                      {1, 0}, recoveryInfo);
    tasks[0].construct(&recovery, ServerId(2, 0));
    auto* result = &tasks[0]->result;
    result->replicas.push_back(Replica{88lu, 100u, false});
    result->replicas.push_back(Replica{89lu, 100u, false});
    result->replicas.push_back(Replica{90lu, 100u, false});
    result->primaryReplicaCount = 3;

    tasks[1].construct(&recovery, ServerId(3, 0));
    result = &tasks[1]->result;
    result->replicas.push_back(Replica{88lu, 100u, false});
    result->replicas.push_back(Replica{91lu, 100u, false});
    result->primaryReplicaCount = 1;

    addServersToTracker(3, {WireFormat::BACKUP_SERVICE});

    auto replicaMap = buildReplicaMap(tasks, 2, &tracker, 91);
    EXPECT_EQ((vector<WireFormat::Recover::Replica> {
                    { 2, 88 },
                    { 3, 88 },
                    { 2, 89 },
                    { 2, 90 },
                    { 3, 91 },
               }),
              replicaMap);

    tracker.getServerDetails({3, 0})->expectedReadMBytesPerSec = 101;
    TestLog::Enable _;
    replicaMap = buildReplicaMap(tasks, 2, &tracker, 91);
    EXPECT_EQ((vector<WireFormat::Recover::Replica> {
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
    Recovery recovery(&context, taskQueue, &tableManager, &tracker, NULL,
                      {1, 0}, recoveryInfo);
    tasks[0].construct(&recovery, ServerId(2, 0));
    auto* result = &tasks[0]->result;
    result->replicas.push_back(Replica{92lu, 100u, true}); // beyond head
    result->primaryReplicaCount = 1;

    addServersToTracker(2, {WireFormat::BACKUP_SERVICE});

    auto replicaMap = buildReplicaMap(tasks, 1, &tracker, 91);
    EXPECT_EQ((vector<WireFormat::Recover::Replica>()), replicaMap);
}

TEST_F(RecoveryTest, startRecoveryMasters) {
    MockRandom _(1);
    struct Cb : public MasterStartTaskTestingCallback {
        int callCount;
        Cb() : callCount() {}
        void masterStartTaskSend(uint64_t recoveryId,
            ServerId crashedServerId, uint32_t partitionId,
            const ProtoBuf::RecoveryPartition& recoveryPartition,
            const WireFormat::Recover::Replica replicaMap[],
            size_t replicaMapSize)
        {
            if (callCount == 0) {
                EXPECT_EQ(1lu, recoveryId);
                EXPECT_EQ(ServerId(99, 0), crashedServerId);
                ASSERT_EQ(2, recoveryPartition.tablet_size());
                const auto* tablet = &recoveryPartition.tablet(0);
                EXPECT_EQ(123lu, tablet->table_id());
                EXPECT_EQ(0lu, tablet->start_key_hash());
                EXPECT_EQ(9lu, tablet->end_key_hash());
                EXPECT_EQ(TabletsBuilder::Tablet::RECOVERING, tablet->state());
                EXPECT_EQ(0lu, tablet->user_data());
                tablet = &recoveryPartition.tablet(1);
                EXPECT_EQ(123lu, tablet->table_id());
                EXPECT_EQ(20lu, tablet->start_key_hash());
                EXPECT_EQ(29lu, tablet->end_key_hash());
                EXPECT_EQ(TabletsBuilder::Tablet::RECOVERING, tablet->state());
                EXPECT_EQ(0lu, tablet->user_data());
            } else if (callCount == 1) {
                EXPECT_EQ(1lu, recoveryId);
                EXPECT_EQ(ServerId(99, 0), crashedServerId);
                ASSERT_EQ(1, recoveryPartition.tablet_size());
                const auto* tablet = &recoveryPartition.tablet(0);
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
    Lock lock(mutex);     // To trick TableManager internal calls.
    addServersToTracker(2, {WireFormat::MASTER_SERVICE});
    tableManager.testCreateTable("t", 123);
    tableManager.testAddTablet({123,  0,  9, {99, 0}, Tablet::RECOVERING, {}});
    tableManager.testAddTablet({123, 20, 29, {99, 0}, Tablet::RECOVERING, {}});
    tableManager.testAddTablet({123, 10, 19, {99, 0}, Tablet::RECOVERING, {}});
    Recovery recovery(&context, taskQueue, &tableManager, &tracker, NULL,
                      {99, 0}, recoveryInfo);
    recovery.partitionTablets(
                tableManager.markAllTabletsRecovering({99, 0}), NULL);
    // Hack 'tablets' to get the first two tablets on the same server.
    recovery.dataToRecover.mutable_tablet(1)->set_user_data(0);
    recovery.dataToRecover.mutable_tablet(2)->set_user_data(1);
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
            ServerId crashedServerId, uint32_t partitionId,
            const ProtoBuf::RecoveryPartition& recoveryPartition,
            const WireFormat::Recover::Replica replicaMap[],
            size_t replicaMapSize)
        {
            if (callCount == 0) {
                EXPECT_EQ(1lu, recoveryId);
                EXPECT_EQ(ServerId(99, 0), crashedServerId);
                ASSERT_EQ(2, recoveryPartition.tablet_size());
            } else {
                FAIL();
            }
            ++callCount;
        }
    } callback;
    Lock lock(mutex);     // To trick TableManager internal calls.
    addServersToTracker(2, {WireFormat::MASTER_SERVICE});
    tracker[ServerId(1, 0)] = reinterpret_cast<Recovery*>(0x1);
    tableManager.testCreateTable("t", 123);
    tableManager.testAddTablet({123,  0,  9, {99, 0}, Tablet::RECOVERING, {}});
    tableManager.testAddTablet({123, 20, 29, {99, 0}, Tablet::RECOVERING, {}});
    tableManager.testAddTablet({123, 10, 19, {99, 0}, Tablet::RECOVERING, {}});
    Recovery recovery(&context, taskQueue, &tableManager, &tracker, NULL,
                      {99, 0}, recoveryInfo);
    recovery.partitionTablets(
                tableManager.markAllTabletsRecovering({99, 0}), NULL);
    // Hack 'tablets' to get the first two tablets on the same server.
    recovery.dataToRecover.mutable_tablet(1)->set_user_data(0);
    recovery.dataToRecover.mutable_tablet(2)->set_user_data(1);
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
    Lock lock(mutex);     // To trick TableManager internal calls.
    MockRandom __(1);
    addServersToTracker(2, {WireFormat::MASTER_SERVICE});
    tracker[ServerId(1, 0)] = reinterpret_cast<Recovery*>(0x1);
    tracker[ServerId(2, 0)] = reinterpret_cast<Recovery*>(0x1);
    tableManager.testCreateTable("t", 123);
    tableManager.testAddTablet({123,  0,  9, {99, 0}, Tablet::RECOVERING, {}});
    tableManager.testAddTablet({123, 20, 29, {99, 0}, Tablet::RECOVERING, {}});
    tableManager.testAddTablet({123, 10, 19, {99, 0}, Tablet::RECOVERING, {}});
    Recovery recovery(&context, taskQueue, &tableManager, &tracker, &owner,
                      {99, 0}, recoveryInfo);
    recovery.partitionTablets(
                tableManager.markAllTabletsRecovering({99, 0}), NULL);

    TestLog::Enable _;
    recovery.startRecoveryMasters();

    EXPECT_EQ(
        "startRecoveryMasters: Starting recovery 1 for crashed server 99.0 "
            "with 3 partitions | "
        "startRecoveryMasters: Couldn't find enough masters not already "
            "performing a recovery to recover all partitions: 3 partitions "
            "will be recovered later | "
        "recoveryMasterFinished: Recovery wasn't completely successful; "
            "will not broadcast the end of recovery 1 for "
            "server 99.0 to backups",
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
    Lock lock(mutex);     // To trick TableManager internal calls.
    addServersToTracker(2, {WireFormat::MASTER_SERVICE});
    tableManager.testCreateTable("t", 123);
    tableManager.testAddTablet({123,  0,  9, {99, 0}, Tablet::RECOVERING, {}});
    tableManager.testAddTablet({123, 20, 29, {99, 0}, Tablet::RECOVERING, {}});
    tableManager.testAddTablet({123, 10, 19, {99, 0}, Tablet::RECOVERING, {}});
    Recovery recovery(&context, taskQueue, &tableManager, &tracker, NULL,
                      {99, 0}, recoveryInfo);
    recovery.partitionTablets(
                tableManager.markAllTabletsRecovering({99, 0}), NULL);
    recovery.startRecoveryMasters();

    EXPECT_EQ(3u, recovery.numPartitions);
    EXPECT_EQ(0u, recovery.successfulRecoveryMasters);
    EXPECT_EQ(3u, recovery.unsuccessfulRecoveryMasters);
    EXPECT_EQ(Recovery::DONE, recovery.status);
    EXPECT_FALSE(recovery.isScheduled()); // NOT scheduled to send broadcast
    EXPECT_FALSE(tracker[ServerId(1, 0)]);
    EXPECT_FALSE(tracker[ServerId(2, 0)]);
}

TEST_F(RecoveryTest, startRecoveryMasters_indexlet) {
    Lock lock(mutex);     // To trick TableManager internal calls.
    addServersToTracker(4, {WireFormat::MASTER_SERVICE});
    tableManager.testCreateTable("t", 123);
    tableManager.testAddTablet({123,  0,  9, {99, 0}, Tablet::RECOVERING, {}});
    tableManager.testAddTablet({123, 20, 29, {99, 0}, Tablet::RECOVERING, {}});
    tableManager.testAddTablet({123, 10, 19, {99, 0}, Tablet::RECOVERING, {}});
    tableManager.testCreateTable("__indexTable:123:0:0", 3);
    tableManager.testAddTablet({3,  0,  ~0UL, {99, 0}, Tablet::RECOVERING, {}});
    tableManager.createIndex(123, 0, 0, 1);
    Recovery recovery(&context, taskQueue, &tableManager, &tracker, NULL,
                      {99, 0}, recoveryInfo);
    recovery.partitionTablets(
                tableManager.markAllTabletsRecovering({99, 0}), NULL);
    recovery.startRecoveryMasters();

    EXPECT_EQ(4u, recovery.numPartitions);
    EXPECT_EQ(4, recovery.dataToRecover.tablet_size());
}

TEST_F(RecoveryTest, recoveryMasterFinished) {
    addServersToTracker(3, {WireFormat::MASTER_SERVICE});
    Recovery recovery(&context, taskQueue, &tableManager, &tracker, NULL,
                      {99, 0}, recoveryInfo);
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
    addServersToTracker(3, {WireFormat::BACKUP_SERVICE});
    struct Cb : public BackupEndTaskTestingCallback {
        int callCount;
        Cb() : callCount() {}
        void backupEndTaskSend(ServerId backupId,
                               ServerId crashedServerId)
        {
            ++callCount;
        }
    } callback;
    Recovery recovery(&context, taskQueue, &tableManager, &tracker, NULL,
                      {99, 0}, recoveryInfo);
    recovery.testingBackupEndTaskSendCallback = &callback;
    recovery.broadcastRecoveryComplete();
    EXPECT_EQ(3, callback.callCount);
}

} // namespace RAMCloud
