/* Copyright (c) 2013-2016 Stanford University
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
#include "MasterTableMetadata.h"

namespace RAMCloud {

class MasterTableMetadataTest : public ::testing::Test {
  public:
    MasterTableMetadata mtm;

    MasterTableMetadataTest()
        : mtm()
    {
    }

    DISALLOW_COPY_AND_ASSIGN(MasterTableMetadataTest);
};

TEST_F(MasterTableMetadataTest, constructor) {
    EXPECT_EQ(0U, mtm.tableMetadataMap.size());
}

TEST_F(MasterTableMetadataTest, contructor_TableMetadataEntry) {
    MasterTableMetadata::Entry* entry;
    entry = new MasterTableMetadata::Entry(42);
    EXPECT_TRUE(entry != NULL);
    if (entry != NULL) {
        EXPECT_EQ(42u, entry->tableId);
        delete entry;
    }
}

TEST_F(MasterTableMetadataTest, find_noneExisting) {
    MasterTableMetadata::Entry* entry = NULL;
    // Make sure entry doesn't exist to start with.
    EXPECT_EQ(mtm.tableMetadataMap.end(), mtm.tableMetadataMap.find(1));
    entry = mtm.find(1);
    EXPECT_TRUE(entry == NULL);
}

TEST_F(MasterTableMetadataTest, find_existing) {
    MasterTableMetadata::Entry* entry = NULL;
    EXPECT_EQ(mtm.tableMetadataMap.end(), mtm.tableMetadataMap.find(0));
    MasterTableMetadata::Entry old(42);
    mtm.tableMetadataMap[0] = &old;
    // Make sure entry does exist.
    EXPECT_NE(mtm.tableMetadataMap.end(), mtm.tableMetadataMap.find(0));
    EXPECT_EQ(&old, mtm.tableMetadataMap.find(0)->second);
    entry = mtm.find(0);
    EXPECT_NE(mtm.tableMetadataMap.end(), mtm.tableMetadataMap.find(0));
    EXPECT_EQ(&old, mtm.tableMetadataMap.find(0)->second);
    EXPECT_TRUE(entry == &old);
    EXPECT_EQ(42u, entry->tableId);

    // Delete the old Entry so that it does not get freed twice
    mtm.tableMetadataMap.erase(0);
}

TEST_F(MasterTableMetadataTest, insert_noneExisting) {
    MasterTableMetadata::Entry* entry = NULL;
    // Make sure entry doesn't exist to start with.
    EXPECT_EQ(mtm.tableMetadataMap.end(), mtm.tableMetadataMap.find(0));
    entry = mtm.findOrCreate(0);
    EXPECT_NE(mtm.tableMetadataMap.end(), mtm.tableMetadataMap.find(0));
    EXPECT_EQ(mtm.tableMetadataMap.find(0)->second, entry);
    EXPECT_EQ(0u, entry->tableId);
}

TEST_F(MasterTableMetadataTest, insert_existing) {
    MasterTableMetadata::Entry* entry = NULL;
    EXPECT_EQ(mtm.tableMetadataMap.end(), mtm.tableMetadataMap.find(1));
    MasterTableMetadata::Entry old(42);
    mtm.tableMetadataMap[1] = &old;
    // Make sure entry does exist.
    EXPECT_NE(mtm.tableMetadataMap.end(), mtm.tableMetadataMap.find(1));
    EXPECT_EQ(&old, mtm.tableMetadataMap.find(1)->second);
    entry = mtm.findOrCreate(1);
    EXPECT_NE(mtm.tableMetadataMap.end(), mtm.tableMetadataMap.find(1));
    EXPECT_EQ(&old, mtm.tableMetadataMap.find(1)->second);
    EXPECT_TRUE(entry == &old);
    EXPECT_EQ(42u, entry->tableId);

    // Delete the old Entry so that it does not get freed twice
    mtm.tableMetadataMap.erase(1);
}

TEST_F(MasterTableMetadataTest, insertAndFind) {
    MasterTableMetadata::Entry* entry = NULL;
    entry = mtm.findOrCreate(0);
    EXPECT_NE(mtm.tableMetadataMap.end(), mtm.tableMetadataMap.find(0));
    EXPECT_TRUE(entry != NULL);
    entry = mtm.find(0);
    EXPECT_TRUE(entry != NULL);
}

TEST_F(MasterTableMetadataTest, getScanner) {
    {
        MasterTableMetadata::scanner sc = mtm.getScanner();
        EXPECT_FALSE(sc.mtm == NULL);
        EXPECT_TRUE(sc.mtm == &mtm);
        EXPECT_FALSE(mtm.lock.try_lock());
        EXPECT_TRUE(sc.it == mtm.tableMetadataMap.begin());
    }

    EXPECT_TRUE(mtm.lock.try_lock());
    mtm.lock.unlock();

    {
        MasterTableMetadata::scanner sc;
        sc = mtm.getScanner();
        EXPECT_FALSE(sc.mtm == NULL);
        EXPECT_TRUE(sc.mtm == &mtm);
        EXPECT_FALSE(mtm.lock.try_lock());
        EXPECT_TRUE(sc.it == mtm.tableMetadataMap.begin());
    }

    EXPECT_TRUE(mtm.lock.try_lock());
    mtm.lock.unlock();
}

TEST_F(MasterTableMetadataTest, constructor_scanner) {
    // Default Constructor should produce an "invalid" scanner.
    MasterTableMetadata::scanner sc1;
    EXPECT_TRUE(sc1.mtm == NULL);
    EXPECT_TRUE(mtm.lock.try_lock());
    mtm.lock.unlock();

    // Constructor with pointer to mtm should be valid.
    MasterTableMetadata::scanner sc2(&mtm);
    EXPECT_FALSE(sc2.mtm == NULL);
    EXPECT_TRUE(sc2.mtm == &mtm);
    EXPECT_FALSE(mtm.lock.try_lock());
    EXPECT_TRUE(sc2.it == mtm.tableMetadataMap.begin());
}

TEST_F(MasterTableMetadataTest, constructor_scanner_move) {
    // Create valid scanner.
    MasterTableMetadata::scanner sc1(&mtm);
    EXPECT_FALSE(sc1.mtm == NULL);
    EXPECT_TRUE(sc1.mtm == &mtm);
    EXPECT_FALSE(mtm.lock.try_lock());
    EXPECT_TRUE(sc1.it == mtm.tableMetadataMap.begin());

    // Move construct a new scanner and check that it is valid.
    MasterTableMetadata::scanner sc2 = std::move(sc1);
    EXPECT_FALSE(sc2.mtm == NULL);
    EXPECT_TRUE(sc2.mtm == &mtm);
    EXPECT_FALSE(mtm.lock.try_lock());
    EXPECT_TRUE(sc2.it == mtm.tableMetadataMap.begin());

    // Make sure old scanner is now invalid.
    EXPECT_TRUE(sc1.mtm == NULL);
}

TEST_F(MasterTableMetadataTest, destructor_scanner_simple) {
    {
        MasterTableMetadata::scanner sc(&mtm);
        EXPECT_FALSE(sc.mtm == NULL);
        EXPECT_TRUE(sc.mtm == &mtm);
        EXPECT_FALSE(mtm.lock.try_lock());
        EXPECT_TRUE(sc.it == mtm.tableMetadataMap.begin());
    }

    EXPECT_TRUE(mtm.lock.try_lock());
    mtm.lock.unlock();
}

TEST_F(MasterTableMetadataTest, destructor_scanner_afterMove) {
    MasterTableMetadata::scanner* sc = new MasterTableMetadata::scanner(&mtm);
    EXPECT_FALSE(sc->mtm == NULL);
    EXPECT_TRUE(sc->mtm == &mtm);
    EXPECT_FALSE(mtm.lock.try_lock());
    EXPECT_TRUE(sc->it == mtm.tableMetadataMap.begin());

    MasterTableMetadata::scanner sc2 = std::move(*sc);

    delete sc;
    EXPECT_FALSE(mtm.lock.try_lock());
}

TEST_F(MasterTableMetadataTest, constructor_scanner_moveAssign) {
    // Create valid scanner.
    MasterTableMetadata::scanner sc1(&mtm);
    EXPECT_FALSE(sc1.mtm == NULL);
    EXPECT_TRUE(sc1.mtm == &mtm);
    EXPECT_FALSE(mtm.lock.try_lock());
    EXPECT_TRUE(sc1.it == mtm.tableMetadataMap.begin());

    // Move construct a new scanner and check that it is valid.
    MasterTableMetadata::scanner sc2;
    sc2 = std::move(sc1);
    EXPECT_FALSE(sc2.mtm == NULL);
    EXPECT_TRUE(sc2.mtm == &mtm);
    EXPECT_FALSE(mtm.lock.try_lock());
    EXPECT_TRUE(sc2.it == mtm.tableMetadataMap.begin());

    // Make sure old scanner is now invalid.
    EXPECT_TRUE(sc1.mtm == NULL);
}

TEST_F(MasterTableMetadataTest, scanner_hasNext) {
    {
        // Create valid scanner.
        MasterTableMetadata::scanner sc(&mtm);
        EXPECT_FALSE(sc.mtm == NULL);
        EXPECT_TRUE(sc.mtm == &mtm);
        EXPECT_FALSE(mtm.lock.try_lock());
        EXPECT_TRUE(sc.it == mtm.tableMetadataMap.begin());
        EXPECT_FALSE(sc.hasNext());
    }
    EXPECT_TRUE(mtm.lock.try_lock());
    mtm.lock.unlock();
    mtm.findOrCreate(1);
    {
        // Create valid scanner.
        MasterTableMetadata::scanner sc(&mtm);
        EXPECT_FALSE(sc.mtm == NULL);
        EXPECT_TRUE(sc.mtm == &mtm);
        EXPECT_FALSE(mtm.lock.try_lock());
        EXPECT_TRUE(sc.it == mtm.tableMetadataMap.begin());
        EXPECT_TRUE(sc.hasNext());
    }
    EXPECT_TRUE(mtm.lock.try_lock());
    mtm.lock.unlock();
}

TEST_F(MasterTableMetadataTest, scanner_next) {
    mtm.findOrCreate(1);
    mtm.findOrCreate(2);
    mtm.findOrCreate(3);
    int i = 0;
    {
        MasterTableMetadata::scanner sc = mtm.getScanner();
        while (sc.hasNext()) {
            MasterTableMetadata::Entry* entry = sc.next();
            EXPECT_TRUE(entry->tableId == 1 ||
                        entry->tableId == 2 ||
                        entry->tableId == 3);
            ++i;
        }
    }
    EXPECT_EQ(3, i);
}

} // namespace RAMCloud

