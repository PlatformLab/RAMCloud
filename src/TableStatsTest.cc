/* Copyright (c) 2013-2015 Stanford University
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
    Buffer digestBuffer;

    TableStatsTest()
        : mtm()
        , digestBuffer()
    {
    }

    void fillMtm() {
        TableStats::addKeyHashRange(&mtm, 1, 0, 9);
        TableStats::increment(&mtm, 1, 11, 111);
        TableStats::addKeyHashRange(&mtm, 2, 0, 9);
        TableStats::increment(&mtm, 2, 22, 222);
        TableStats::addKeyHashRange(&mtm, 63, 0, 9);
        TableStats::increment(&mtm, 63, TableStats::threshold - 1, 63000);
        TableStats::addKeyHashRange(&mtm, 64, 0, 9);
        TableStats::increment(&mtm, 64, TableStats::threshold, 64000);
        TableStats::addKeyHashRange(&mtm, 65, 0, 9);
        TableStats::increment(&mtm, 65, TableStats::threshold + 1, 65000);
        TableStats::addKeyHashRange(&mtm, 66, 0, 9);
        TableStats::increment(&mtm, 66, TableStats::threshold + 2, 66000);
    }

    const TableStats::DigestEntry*
    findDigestWithTabletId(uint64_t tableId, const TableStats::Digest* digest) {
        for (size_t i = 0; i < digest->header.entryCount; ++i)
            if (digest->entries[i].tableId == tableId)
                return &(digest->entries[i]);

        return NULL;
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

TEST_F(TableStatsTest, addKeyHashRange) {
    MasterTableMetadata::Entry* entry;
    EXPECT_TRUE(mtm.find(0) == NULL);
    TableStats::addKeyHashRange(&mtm, 0, 0, 10);
    EXPECT_NE(mtm.tableMetadataMap.end(), mtm.tableMetadataMap.find(0));
    entry = mtm.find(0);
    EXPECT_FALSE(entry == NULL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(11U, entry->stats.keyHashCount);
        EXPECT_FALSE(entry->stats.totalOwnership);
    }

    TableStats::addKeyHashRange(&mtm, 0, 11, ~0UL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(0U, entry->stats.keyHashCount);
        EXPECT_TRUE(entry->stats.totalOwnership);
    }
}

TEST_F(TableStatsTest, deleteKeyHashRange) {
    MasterTableMetadata::Entry* entry;
    EXPECT_TRUE(mtm.find(0) == NULL);
    TableStats::deleteKeyHashRange(&mtm, 0, 11, ~0UL);
    entry = mtm.find(0);
    EXPECT_TRUE(entry == NULL);

    TableStats::addKeyHashRange(&mtm, 0, 0, ~0UL);
    entry = mtm.find(0);
    EXPECT_FALSE(entry == NULL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(0U, entry->stats.keyHashCount);
        EXPECT_TRUE(entry->stats.totalOwnership);
    }

    TableStats::deleteKeyHashRange(&mtm, 0, 11, ~0UL);
    EXPECT_FALSE(entry == NULL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(11U, entry->stats.keyHashCount);
        EXPECT_FALSE(entry->stats.totalOwnership);
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

TEST_F(TableStatsTest, serialize_basic) {
    // First Check an empty mtm.
    {
        Buffer buffer;
        TableStats::serialize(&buffer, &mtm);

        const void* temp = buffer.getRange(0, sizeof(TableStats::DigestHeader));
        const TableStats::Digest* digest;
        digest = reinterpret_cast<const TableStats::Digest*>(temp);

        EXPECT_EQ(0U, digest->header.entryCount);
        EXPECT_EQ(0, digest->header.otherBytesPerKeyHash);
        EXPECT_EQ(0, digest->header.otherRecordsPerKeyHash);
    }

    // Now check a populated mtm.
    fillMtm();

    const TableStats::Digest* digest = getDigest();

    EXPECT_EQ(3u, digest->header.entryCount);
    EXPECT_EQ((double(TableStats::threshold - 1) + 11 + 22) / 30,
              digest->header.otherBytesPerKeyHash);
    EXPECT_EQ(double(63333) / 30, digest->header.otherRecordsPerKeyHash);

    MasterTableMetadata::Entry* entry;

    entry = mtm.find(digest->entries[0].tableId);
    EXPECT_TRUE(entry != NULL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(double(entry->stats.byteCount) / 10,
                  digest->entries[0].bytesPerKeyHash);
        EXPECT_EQ(double(entry->stats.recordCount) / 10,
                  digest->entries[0].recordsPerKeyHash);
    }
    entry = NULL;

    entry = mtm.find(digest->entries[1].tableId);
    EXPECT_TRUE(entry != NULL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(double(entry->stats.byteCount) / 10,
                  digest->entries[1].bytesPerKeyHash);
        EXPECT_EQ(double(entry->stats.recordCount) / 10,
                  digest->entries[1].recordsPerKeyHash);
    }
    entry = NULL;

    entry = mtm.find(digest->entries[2].tableId);
    EXPECT_TRUE(entry != NULL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(double(entry->stats.byteCount) / 10,
                  digest->entries[2].bytesPerKeyHash);
        EXPECT_EQ(double(entry->stats.recordCount) / 10,
                  digest->entries[2].recordsPerKeyHash);
    }
    entry = NULL;

}

TEST_F(TableStatsTest, serialize_fullTable) {
    TableStats::addKeyHashRange(&mtm, 1, 0, ~0UL);
    TableStats::increment(&mtm, 1, 100, 200);

    Buffer buffer;
    TableStats::serialize(&buffer, &mtm);

    const void* temp = buffer.getRange(0, sizeof(TableStats::DigestHeader));
    const TableStats::Digest* digest;
    digest = reinterpret_cast<const TableStats::Digest*>(temp);

    EXPECT_EQ(0u, digest->header.entryCount);
    EXPECT_EQ(double(100) / (double(~0UL) + 1),
              digest->header.otherBytesPerKeyHash);
    EXPECT_EQ(double(200) / (double(~0UL) + 1),
              digest->header.otherRecordsPerKeyHash);
}

TEST_F(TableStatsTest, serialize_skipTable) {
    TableStats::addKeyHashRange(&mtm, 1, 0, 100);
    TableStats::increment(&mtm, 1, TableStats::threshold + 2, 200);
    TableStats::deleteKeyHashRange(&mtm, 1, 0, 100);

    Buffer buffer;
    TableStats::serialize(&buffer, &mtm);

    const void* temp = buffer.getRange(0, sizeof(TableStats::DigestHeader));
    const TableStats::Digest* digest;
    digest = reinterpret_cast<const TableStats::Digest*>(temp);

    EXPECT_EQ(0u, digest->header.entryCount);
    EXPECT_EQ(double(0), digest->header.otherBytesPerKeyHash);
    EXPECT_EQ(double(0), digest->header.otherRecordsPerKeyHash);
}

TEST_F(TableStatsTest, estimator_constructor) {
    fillMtm();
    const TableStats::Digest* digest = getDigest();

    TableStats::Estimator e(digest);

    EXPECT_EQ(digest->header.otherBytesPerKeyHash,
              e.otherStats.bytesPerKeyHash);
    EXPECT_EQ(digest->header.otherRecordsPerKeyHash,
              e.otherStats.recordsPerKeyHash);

    MasterTableMetadata::Entry* entry;
    const TableStats::DigestEntry* digestEntry;

    entry = mtm.find(64);
    digestEntry = findDigestWithTabletId(64, digest);
    EXPECT_TRUE(entry != NULL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(digestEntry->bytesPerKeyHash,
                  e.tableStats[64].bytesPerKeyHash);
        EXPECT_EQ(digestEntry->recordsPerKeyHash,
                  e.tableStats[64].recordsPerKeyHash);
    }
    entry = NULL;

    entry = mtm.find(65);
    digestEntry = findDigestWithTabletId(65, digest);
    EXPECT_TRUE(entry != NULL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(digestEntry->bytesPerKeyHash,
                  e.tableStats[65].bytesPerKeyHash);
        EXPECT_EQ(digestEntry->recordsPerKeyHash,
                  e.tableStats[65].recordsPerKeyHash);
    }
    entry = NULL;

    entry = mtm.find(66);
    digestEntry = findDigestWithTabletId(66, digest);
    EXPECT_TRUE(entry != NULL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(digestEntry->bytesPerKeyHash,
                  e.tableStats[66].bytesPerKeyHash);
        EXPECT_EQ(digestEntry->recordsPerKeyHash,
                  e.tableStats[66].recordsPerKeyHash);
    }
    entry = NULL;

}

TEST_F(TableStatsTest, estimator_estimate_basic) {
    fillMtm();
    const TableStats::Digest* digest = getDigest();

    TableStats::Estimator e(digest);

    TableStats::Estimator::Estimate ret;

    Tablet tt1 = {1, 0, 4, ServerId(), Tablet::NORMAL, LogPosition()};
    ret = e.estimate(&tt1);
    EXPECT_EQ(static_cast<unsigned>(
                    double(TableStats::threshold - 1 + 11 + 22) * 5 / 30),
              ret.byteCount);
    EXPECT_EQ(63333u * 5 / 30, ret.recordCount);

    Tablet tt2 = {66, 21, 25, ServerId(), Tablet::NORMAL, LogPosition()};
    ret = e.estimate(&tt2);

    MasterTableMetadata::Entry* entry;

    entry = mtm.find(66);
    EXPECT_TRUE(entry != NULL);
    {
        TableStats::Lock guard(entry->stats.lock);
        EXPECT_EQ(entry->stats.byteCount * 5 / 10, ret.byteCount);
        EXPECT_EQ(entry->stats.recordCount * 5 /10, ret.recordCount);
    }
    entry = NULL;
}

} // namespace RAMCloud
