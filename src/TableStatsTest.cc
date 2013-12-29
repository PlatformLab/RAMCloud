/* Copyright (c) 2013 Stanford University
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

#include "TestUtil.h"
#include "TableStats.h"
#include "MasterTableMetadata.h"

namespace RAMCloud {

class TableStatsTest : public ::testing::Test {
  public:
    MasterTableMetadata mtm;
    vector<Tablet> tablets;
    Buffer digestBuffer;

    TableStatsTest()
        : mtm()
        , tablets()
        , digestBuffer()
    {
    }

    void fillMtm() {
        TableStats::increment(&mtm, 1, 11, 111);
        TableStats::increment(&mtm, 2, 22, 222);
        TableStats::increment(&mtm, 63, TableStats::threshold - 1, 63000);
        TableStats::increment(&mtm, 64, TableStats::threshold, 64000);
        TableStats::increment(&mtm, 65, TableStats::threshold + 1, 65000);
        TableStats::increment(&mtm, 66, TableStats::threshold + 2, 66000);
    }

    void fillTabletsStandard() {
        tablets.clear();
        Tablet t1 = {1, 1, 1, ServerId(), Tablet::NORMAL, Log::Position()};
        tablets.push_back(t1);
        Tablet t2 = {2, 1, 10, ServerId(), Tablet::NORMAL, Log::Position()};
        tablets.push_back(t2);
        Tablet t3 = {64, 1, 1, ServerId(), Tablet::NORMAL, Log::Position()};
        tablets.push_back(t3);
        Tablet t4 = {64, 21, 40, ServerId(), Tablet::NORMAL, Log::Position()};
        tablets.push_back(t4);
        Tablet t5 = {65, 1, 1, ServerId(), Tablet::NORMAL, Log::Position()};
        tablets.push_back(t5);
        Tablet t6 = {65, 21, 50, ServerId(), Tablet::NORMAL, Log::Position()};
        tablets.push_back(t6);
        Tablet t7 = {66, 1, 1, ServerId(), Tablet::NORMAL, Log::Position()};
        tablets.push_back(t7);
        Tablet t8 = {66, 21, 60, ServerId(), Tablet::NORMAL, Log::Position()};
        tablets.push_back(t8);
    }

    const TableStats::Digest* getDigest() {
        digestBuffer.reset();
        TableStats::serialize(&digestBuffer, &mtm);
        return reinterpret_cast<const TableStats::Digest*>(
                    digestBuffer.getRange(0,
                                          sizeof(TableStats::DigestHeader) +
                                          3 * sizeof(TableStats::DigestEntry)));
    }

    DISALLOW_COPY_AND_ASSIGN(TableStatsTest);
};

TEST_F(TableStatsTest, constructor_TableStats) {
    TableStats::Block* stats;
    stats = new TableStats::Block();
    EXPECT_TRUE(stats != NULL);
    if (stats != NULL) {
        EXPECT_EQ(0u, stats->byteCount);
        EXPECT_EQ(0u, stats->recordCount);
        EXPECT_TRUE(stats->lock.try_lock());
        EXPECT_FALSE(stats->lock.try_lock());
        stats->lock.unlock();
        stats->lock.lock();
        EXPECT_FALSE(stats->lock.try_lock());
        stats->lock.unlock();
        delete stats;
    }
}

TEST_F(TableStatsTest, increment) {
    MasterTableMetadata::Entry* entry;
    EXPECT_TRUE(mtm.find(0) == NULL);
    TableStats::increment(&mtm, 0, 2, 3);
    EXPECT_NE(mtm.tableMetadataMap.end(), mtm.tableMetadataMap.find(0));
    entry = mtm.find(0);
    EXPECT_FALSE(entry == NULL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(2u, entry->stats.byteCount);
        EXPECT_EQ(3u, entry->stats.recordCount);
    }

    TableStats::increment(&mtm, 0, 4, 5);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(6u, entry->stats.byteCount);
        EXPECT_EQ(8u, entry->stats.recordCount);
    }
}

TEST_F(TableStatsTest, decrement) {
    MasterTableMetadata::Entry* entry;
    EXPECT_TRUE(mtm.find(1) == NULL);
    TableStats::decrement(&mtm, 1, 2, 3);
    entry = mtm.find(1);
    EXPECT_TRUE(entry == NULL);

    TableStats::increment(&mtm, 1, 10, 10);
    entry = mtm.find(1);
    EXPECT_FALSE(entry == NULL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(10u, entry->stats.byteCount);
        EXPECT_EQ(10u, entry->stats.recordCount);
    }

    TableStats::decrement(&mtm, 1, 2, 3);
    EXPECT_FALSE(entry == NULL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(8u, entry->stats.byteCount);
        EXPECT_EQ(7u, entry->stats.recordCount);
    }
}

TEST_F(TableStatsTest, serialize) {
    // First Check an empty mtm.
    {
        Buffer buffer;
        TableStats::serialize(&buffer, &mtm);

        const void* temp = buffer.getRange(0, sizeof(TableStats::DigestHeader));
        const TableStats::Digest* digest;
        digest = reinterpret_cast<const TableStats::Digest*>(temp);

        EXPECT_EQ(0u, digest->header.entryCount);
        EXPECT_EQ(0u, digest->header.otherByteCount);
        EXPECT_EQ(0u, digest->header.otherRecordCount);
    }

    // Now check a populated mtm.
    fillMtm();

    const TableStats::Digest* digest = getDigest();

    EXPECT_EQ(3u, digest->header.entryCount);
    EXPECT_EQ(static_cast<unsigned>(TableStats::threshold
                                    - 1 + 11 + 22),
              digest->header.otherByteCount);
    EXPECT_EQ(63333u, digest->header.otherRecordCount);

    MasterTableMetadata::Entry* entry;

    entry = mtm.find(digest->entries[0].tableId);
    EXPECT_TRUE(entry != NULL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(entry->stats.byteCount, digest->entries[0].byteCount);
        EXPECT_EQ(entry->stats.recordCount, digest->entries[0].recordCount);
    }
    entry = NULL;

    entry = mtm.find(digest->entries[1].tableId);
    EXPECT_TRUE(entry != NULL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(entry->stats.byteCount, digest->entries[1].byteCount);
        EXPECT_EQ(entry->stats.recordCount, digest->entries[1].recordCount);
    }
    entry = NULL;

    entry = mtm.find(digest->entries[2].tableId);
    EXPECT_TRUE(entry != NULL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(entry->stats.byteCount, digest->entries[2].byteCount);
        EXPECT_EQ(entry->stats.recordCount, digest->entries[2].recordCount);
    }
    entry = NULL;

}

TEST_F(TableStatsTest, estimator_constructor) {
    fillMtm();
    const TableStats::Digest* digest = getDigest();
    // Fill tablets
    fillTabletsStandard();

    TableStats::Estimator e(digest, &tablets);

    EXPECT_EQ(static_cast<unsigned>(TableStats::threshold
                                    - 1 + 11 + 22),
              e.otherStats.byteCount);
    EXPECT_EQ(63333u, e.otherStats.recordCount);
    EXPECT_EQ(11u, e.otherStats.keyRange);

    MasterTableMetadata::Entry* entry;

    entry = mtm.find(64);
    EXPECT_TRUE(entry != NULL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(entry->stats.byteCount, e.tableStats[64].byteCount);
        EXPECT_EQ(entry->stats.recordCount, e.tableStats[64].recordCount);
        EXPECT_EQ(21u, e.tableStats[64].keyRange);
    }
    entry = NULL;

    entry = mtm.find(65);
    EXPECT_TRUE(entry != NULL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(entry->stats.byteCount, e.tableStats[65].byteCount);
        EXPECT_EQ(entry->stats.recordCount, e.tableStats[65].recordCount);
        EXPECT_EQ(31u, e.tableStats[65].keyRange);
    }
    entry = NULL;

    entry = mtm.find(66);
    EXPECT_TRUE(entry != NULL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(entry->stats.byteCount, e.tableStats[66].byteCount);
        EXPECT_EQ(entry->stats.recordCount, e.tableStats[66].recordCount);
        EXPECT_EQ(41u, e.tableStats[66].keyRange);
    }
    entry = NULL;

}

TEST_F(TableStatsTest, estimator_estimate_basic) {
    fillMtm();
    const TableStats::Digest* digest = getDigest();
    // Fill tablets
    fillTabletsStandard();

    TableStats::Estimator e(digest, &tablets);

    TableStats::Estimator::Entry ret;

    Tablet tt1 = {1, 1, 1, ServerId(), Tablet::NORMAL, Log::Position()};
    ret = e.estimate(&tt1);
    EXPECT_EQ(1u, ret.keyRange);
    EXPECT_EQ(static_cast<unsigned>((TableStats::threshold
                                    - 1 + 11 + 22) / 11),
              ret.byteCount);
    EXPECT_EQ(63333u / 11, ret.recordCount);

    Tablet tt2 = {66, 21, 60, ServerId(), Tablet::NORMAL, Log::Position()};
    ret = e.estimate(&tt2);

    MasterTableMetadata::Entry* entry;

    entry = mtm.find(66);
    EXPECT_TRUE(entry != NULL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(40u, ret.keyRange);
        EXPECT_EQ(entry->stats.byteCount * 40 / 41, ret.byteCount);
        EXPECT_EQ(entry->stats.recordCount * 40 /41, ret.recordCount);
    }
    entry = NULL;
}

TEST_F(TableStatsTest, estimator_estimate_keyRange_zero) {
    fillMtm();
    const TableStats::Digest* digest = getDigest();
    // Clear tablet information. Simulating no tablets.
    tablets.clear();

    TableStats::Estimator e(digest, &tablets);

    TableStats::Estimator::Entry ret;

    Tablet tt1 = {1, 1, 1, ServerId(), Tablet::NORMAL, Log::Position()};
    ret = e.estimate(&tt1);
    EXPECT_EQ(1.0, ret.keyRange);
    EXPECT_EQ(0u, ret.byteCount);
    EXPECT_EQ(0u, ret.recordCount);

    Tablet tt2 = {66, 1, 1,
                  ServerId(), Tablet::NORMAL, Log::Position()};
    ret = e.estimate(&tt2);
    EXPECT_EQ(1.0, ret.keyRange);
    EXPECT_EQ(0u, ret.byteCount);
    EXPECT_EQ(0u, ret.recordCount);
}

TEST_F(TableStatsTest, estimator_Estimate_full_tablet) {
    fillMtm();
    const TableStats::Digest* digest = getDigest();
    // Fill tablets
    tablets.clear();
    Tablet t7 = {66, 0, 0xFFFFFFFFFFFFFFFF,
                 ServerId(), Tablet::NORMAL, Log::Position()};
    tablets.push_back(t7);

    TableStats::Estimator e(digest, &tablets);

    TableStats::Estimator::Entry ret;

    Tablet tt2 = {66, 0, 0xFFFFFFFFFFFFFFFF,
                  ServerId(), Tablet::NORMAL, Log::Position()};
    ret = e.estimate(&tt2);

    MasterTableMetadata::Entry* entry;

    entry = mtm.find(66);
    EXPECT_TRUE(entry != NULL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(double(0xFFFFFFFFFFFFFFFF) + 1, ret.keyRange);
        EXPECT_EQ(entry->stats.byteCount, ret.byteCount);
        EXPECT_EQ(entry->stats.recordCount, ret.recordCount);
    }
    entry = NULL;
}


} // namespace RAMCloud
