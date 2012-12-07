/* Copyright (c) 2012 Stanford University
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
#include "TabletManager.h"

namespace RAMCloud {

class TabletManagerTest : public ::testing::Test {
  public:
    TabletManager tm;

    TabletManagerTest()
        : tm()
    {
    }

    DISALLOW_COPY_AND_ASSIGN(TabletManagerTest);
};

TEST_F(TabletManagerTest, constructor) {
    EXPECT_EQ(0U, tm.tabletMap.size());
}

TEST_F(TabletManagerTest, addTablet) {
    EXPECT_TRUE(tm.addTablet(0, 10, 20, TabletManager::NORMAL));
    EXPECT_FALSE(tm.addTablet(0, 10, 20, TabletManager::NORMAL));
    EXPECT_FALSE(tm.addTablet(0, 0, 10, TabletManager::NORMAL));
    EXPECT_FALSE(tm.addTablet(0, 20, 30, TabletManager::NORMAL));
    EXPECT_FALSE(tm.addTablet(0, 0, 15, TabletManager::NORMAL));

    SpinLock lock;
    TabletManager::Lock fakeGuard(lock);
    TabletManager::Tablet* tablet = &tm.lookup(0, 10, fakeGuard)->second;
    EXPECT_EQ(0U, tablet->tableId);
    EXPECT_EQ(10U, tablet->startKeyHash);
    EXPECT_EQ(20U, tablet->endKeyHash);
    EXPECT_EQ(TabletManager::NORMAL, tablet->state);
}

TEST_F(TabletManagerTest, getTablet_byKey) {
    Key key(5, "hi", 2);
    EXPECT_FALSE(tm.getTablet(key));
    tm.addTablet(5, key.getHash(), key.getHash(), TabletManager::NORMAL);
    EXPECT_TRUE(tm.getTablet(key));

    TabletManager::Tablet tablet;
    EXPECT_TRUE(tm.getTablet(key, &tablet));
    EXPECT_EQ(5U, tablet.tableId);
    EXPECT_EQ(key.getHash(), tablet.startKeyHash);
    EXPECT_EQ(key.getHash(), tablet.endKeyHash);
    EXPECT_EQ(TabletManager::NORMAL, tablet.state);
}

TEST_F(TabletManagerTest, getTablet_byHashPoint) {
    EXPECT_FALSE(tm.getTablet(5, 10));
    tm.addTablet(5, 9, 11, TabletManager::RECOVERING);
    EXPECT_FALSE(tm.getTablet(5, 8));
    EXPECT_TRUE(tm.getTablet(5, 9));
    EXPECT_TRUE(tm.getTablet(5, 10));
    EXPECT_TRUE(tm.getTablet(5, 11));
    EXPECT_FALSE(tm.getTablet(5, 12));

    TabletManager::Tablet tablet;
    EXPECT_TRUE(tm.getTablet(5, 10, &tablet));
    EXPECT_EQ(5U, tablet.tableId);
    EXPECT_EQ(9U, tablet.startKeyHash);
    EXPECT_EQ(11U, tablet.endKeyHash);
    EXPECT_EQ(TabletManager::RECOVERING, tablet.state);
}

TEST_F(TabletManagerTest, getTablet_byHashRange) {
    EXPECT_FALSE(tm.getTablet(5, 9, 11));
    tm.addTablet(5, 9, 11, TabletManager::RECOVERING);
    EXPECT_TRUE(tm.getTablet(5, 9, 11));
    EXPECT_FALSE(tm.getTablet(4, 9, 11));
    EXPECT_FALSE(tm.getTablet(5, 9, 10));
    EXPECT_FALSE(tm.getTablet(5, 10, 11));
    EXPECT_FALSE(tm.getTablet(5, 8, 11));
    EXPECT_FALSE(tm.getTablet(5, 9, 12));
    EXPECT_FALSE(tm.getTablet(5, 8, 12));

    TabletManager::Tablet tablet;
    EXPECT_TRUE(tm.getTablet(5, 9, 11, &tablet));
    EXPECT_EQ(5U, tablet.tableId);
    EXPECT_EQ(9U, tablet.startKeyHash);
    EXPECT_EQ(11U, tablet.endKeyHash);
    EXPECT_EQ(TabletManager::RECOVERING, tablet.state);
}

TEST_F(TabletManagerTest, getTablets) {
    vector<TabletManager::Tablet> tablets;

    tm.getTablets(&tablets);
    EXPECT_EQ(0U, tablets.size());

    tm.addTablet(5, 9, 11, TabletManager::NORMAL);
    tm.addTablet(4, 0, 5, TabletManager::RECOVERING);
    tm.getTablets(&tablets);
    EXPECT_EQ(2U, tablets.size());
    // Note that the order isn't well-defined. We may need to sort to make this
    // test work with different unordered_multimap implementations.
    EXPECT_EQ(4U, tablets[0].tableId);
    EXPECT_EQ(0U, tablets[0].startKeyHash);
    EXPECT_EQ(5U, tablets[0].endKeyHash);
    EXPECT_EQ(TabletManager::RECOVERING, tablets[0].state);
    EXPECT_EQ(5U, tablets[1].tableId);
    EXPECT_EQ(9U, tablets[1].startKeyHash);
    EXPECT_EQ(11U, tablets[1].endKeyHash);
    EXPECT_EQ(TabletManager::NORMAL, tablets[1].state);
}

TEST_F(TabletManagerTest, deleteTablet) {
    EXPECT_FALSE(tm.deleteTablet(1, 1, 1));
    tm.addTablet(1, 1, 1, TabletManager::NORMAL);
    EXPECT_TRUE(tm.getTablet(1, 1, 1));
    EXPECT_TRUE(tm.deleteTablet(1, 1, 1));
    EXPECT_FALSE(tm.getTablet(1, 1, 1));
    EXPECT_FALSE(tm.deleteTablet(1, 1, 1));
    EXPECT_EQ(0U, tm.getCount());

    tm.addTablet(0, 1, 2, TabletManager::NORMAL);
    EXPECT_FALSE(tm.deleteTablet(0, 0, 2));
    EXPECT_FALSE(tm.deleteTablet(0, 1, 1));
    EXPECT_EQ(1U, tm.getCount());
}

TEST_F(TabletManagerTest, splitTablet) {
    EXPECT_TRUE(tm.addTablet(0, 50, 100, TabletManager::NORMAL));
    EXPECT_FALSE(tm.splitTablet(0, 49, 100, 75));
    EXPECT_FALSE(tm.splitTablet(0, 51, 100, 75));
    EXPECT_FALSE(tm.splitTablet(0, 50, 99, 75));
    EXPECT_FALSE(tm.splitTablet(0, 50, 101, 75));
    EXPECT_FALSE(tm.splitTablet(0, 50, 100, 50));
    EXPECT_FALSE(tm.splitTablet(0, 50, 100, 100));

    EXPECT_TRUE(tm.splitTablet(0, 50, 100, 51));

    TabletManager::Tablet tablet;
    EXPECT_TRUE(tm.getTablet(0, 50, &tablet));
    EXPECT_EQ(0U, tablet.tableId);
    EXPECT_EQ(50U, tablet.startKeyHash);
    EXPECT_EQ(50U, tablet.endKeyHash);
    EXPECT_EQ(TabletManager::NORMAL, tablet.state);

    EXPECT_TRUE(tm.getTablet(0, 51, &tablet));
    EXPECT_EQ(0U, tablet.tableId);
    EXPECT_EQ(51U, tablet.startKeyHash);
    EXPECT_EQ(100U, tablet.endKeyHash);
    EXPECT_EQ(TabletManager::NORMAL, tablet.state);
}

TEST_F(TabletManagerTest, changeState) {
    EXPECT_TRUE(tm.addTablet(0, 10, 20, TabletManager::RECOVERING));

    EXPECT_FALSE(tm.changeState(0, 10, 20, TabletManager::NORMAL,
                                           TabletManager::RECOVERING));
    EXPECT_FALSE(tm.changeState(0, 9, 20, TabletManager::RECOVERING,
                                          TabletManager::NORMAL));
    EXPECT_FALSE(tm.changeState(0, 10, 19, TabletManager::RECOVERING,
                                           TabletManager::NORMAL));

    TabletManager::Tablet tablet;
    EXPECT_TRUE(tm.getTablet(0, 10, &tablet));
    EXPECT_EQ(TabletManager::RECOVERING, tablet.state);
    EXPECT_TRUE(tm.changeState(0, 10, 20, TabletManager::RECOVERING,
                                          TabletManager::NORMAL));
    EXPECT_TRUE(tm.getTablet(0, 10, &tablet));
    EXPECT_EQ(TabletManager::NORMAL, tablet.state);
}

TEST_F(TabletManagerTest, getStatistics) {
    {
        ProtoBuf::ServerStatistics stats;
        tm.getStatistics(&stats);
        EXPECT_EQ("", stats.ShortDebugString());
    }

    tm.addTablet(58, 0, ~0UL, TabletManager::NORMAL);

    {
        ProtoBuf::ServerStatistics stats;
        tm.getStatistics(&stats);
        EXPECT_EQ("tabletentry { table_id: 58 start_key_hash: 0 "
            "end_key_hash: 18446744073709551615 }",
            stats.ShortDebugString());
    }

    Key key(58, "1", 1);
    tm.incrementReadCount(key);

    {
        ProtoBuf::ServerStatistics stats;
        tm.getStatistics(&stats);
        EXPECT_EQ("tabletentry { table_id: 58 start_key_hash: 0 "
            "end_key_hash: 18446744073709551615 number_read_and_writes: 1 }",
            stats.ShortDebugString());
    }

    tm.incrementWriteCount(key);

    {
        ProtoBuf::ServerStatistics stats;
        tm.getStatistics(&stats);
        EXPECT_EQ("tabletentry { table_id: 58 start_key_hash: 0 "
            "end_key_hash: 18446744073709551615 number_read_and_writes: 2 }",
            stats.ShortDebugString());
    }
}

TEST_F(TabletManagerTest, getCount) {
    EXPECT_EQ(0U, tm.getCount());
    tm.addTablet(0, 0, 0, TabletManager::NORMAL);
    EXPECT_EQ(1U, tm.getCount());
    tm.deleteTablet(0, 0, 0);
    EXPECT_EQ(0U, tm.getCount());
}

TEST_F(TabletManagerTest, toString) {
    EXPECT_EQ("", tm.toString());
    tm.addTablet(0, 1, 2, TabletManager::NORMAL);
    EXPECT_EQ("{ tableId: 0 startKeyHash: 1 endKeyHash: 2 state: 0 }",
        tm.toString());
    tm.addTablet(9, 8, 7, TabletManager::RECOVERING);
    EXPECT_EQ("{ tableId: 0 startKeyHash: 1 endKeyHash: 2 state: 0 }\n"
              "{ tableId: 9 startKeyHash: 8 endKeyHash: 7 state: 1 }",
        tm.toString());
}

TEST_F(TabletManagerTest, lookup) {
    SpinLock lock;
    TabletManager::Lock fakeGuard(lock);

    EXPECT_TRUE(tm.tabletMap.end() == tm.lookup(0, 75, fakeGuard));
    EXPECT_TRUE(tm.addTablet(0, 50, 100, TabletManager::NORMAL));
    EXPECT_TRUE(tm.tabletMap.end() == tm.lookup(0, 49, fakeGuard));
    EXPECT_TRUE(tm.tabletMap.end() == tm.lookup(0, 101, fakeGuard));
    EXPECT_FALSE(tm.tabletMap.end() == tm.lookup(0, 50, fakeGuard));
    EXPECT_FALSE(tm.tabletMap.end() == tm.lookup(0, 75, fakeGuard));
    EXPECT_FALSE(tm.tabletMap.end() == tm.lookup(0, 100, fakeGuard));

    EXPECT_TRUE(tm.tabletMap.end() == tm.lookup(0, 101, fakeGuard));
    EXPECT_TRUE(tm.addTablet(0, 101, 150, TabletManager::NORMAL));
    EXPECT_FALSE(tm.tabletMap.end() == tm.lookup(0, 101, fakeGuard));
}

}  // namespace RAMCloud
