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

class TabletStatsEstimatorTest : public ::testing::Test {
  public:
    MasterTableMetadata mtm;

    TabletStatsEstimatorTest()
        : mtm()
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

    DISALLOW_COPY_AND_ASSIGN(TabletStatsEstimatorTest);
};

TEST_F(TabletStatsEstimatorTest, constructor_TableStats) {
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

TEST_F(TabletStatsEstimatorTest, incrementTableStats) {
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

TEST_F(TabletStatsEstimatorTest, decrementTableStats) {
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

TEST_F(TabletStatsEstimatorTest, serializeTableStats) {
    fillMtm();

    Buffer buffer;
    TableStats::serialize(&buffer, &mtm);

    const void* temp = buffer.getRange(0,
                            sizeof(TableStats::DigestHeader) +
                            3 * sizeof(TableStats::DigestEntry));

    const TableStats::Digest* digest;
    digest = reinterpret_cast<const TableStats::Digest*>
                                                                        (temp);
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

} // namespace RAMCloud
