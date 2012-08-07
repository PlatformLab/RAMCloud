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
#include "CoordinatorClient.h"
#include "CoordinatorService.h"
#include "MasterService.h"
#include "MembershipService.h"
#include "MockCluster.h"
#include "Recovery.h"
#include "ServerList.h"

namespace RAMCloud {

class TabletMapTest : public ::testing::Test {
  public:
    Context context;
    TabletMap map;

    TabletMapTest()
        : context()
        , map()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
    }

    void fillMap(uint32_t entries) {
        for (uint32_t i = 0; i < entries; ++i) {
            const uint32_t b = i * 10;
            map.addTablet({b + 1, b + 2, b + 3, {b + 4, b + 5},
                           Tablet::RECOVERING, {b + 6l, b + 7}});
        }
    }

    DISALLOW_COPY_AND_ASSIGN(TabletMapTest);
};

TEST_F(TabletMapTest, addTablet) {
    map.addTablet({1, 2, 3, {4, 5}, Tablet::RECOVERING, {6, 7}});
    EXPECT_EQ(1lu, map.size());
    Tablet tablet = map.getTablet(1, 2, 3);
    EXPECT_EQ(1lu, tablet.tableId);
    EXPECT_EQ(2lu, tablet.startKeyHash);
    EXPECT_EQ(3lu, tablet.endKeyHash);
    EXPECT_EQ(ServerId(4, 5), tablet.serverId);
    EXPECT_EQ(Tablet::RECOVERING, tablet.status);
    EXPECT_EQ(LogPosition(6, 7), tablet.ctime);
}

TEST_F(TabletMapTest, getTablet) {
    fillMap(3);
    for (uint32_t i = 0; i < 3; ++i) {
        const uint32_t b = i * 10;
        Tablet tablet = map.getTablet(b + 1, b + 2, b + 3);
        EXPECT_EQ(b + 1, tablet.tableId);
        EXPECT_EQ(b + 2, tablet.startKeyHash);
        EXPECT_EQ(b + 3, tablet.endKeyHash);
        EXPECT_EQ(ServerId(b + 4, b + 5), tablet.serverId);
        EXPECT_EQ(Tablet::RECOVERING, tablet.status);
        EXPECT_EQ(LogPosition(b + 6, b + 7), tablet.ctime);
    }
    EXPECT_THROW(map.getTablet(0, 0, 0), TabletMap::NoSuchTablet);
}

TEST_F(TabletMapTest, getTabletsForTable) {
    map.addTablet({0, 1, 6, {0, 1}, Tablet::NORMAL, {0, 5}});
    map.addTablet({1, 2, 7, {1, 1}, Tablet::NORMAL, {1, 6}});
    map.addTablet({0, 3, 8, {2, 1}, Tablet::NORMAL, {2, 7}});
    map.addTablet({1, 4, 9, {3, 1}, Tablet::NORMAL, {3, 8}});
    map.addTablet({2, 5, 10, {4, 1}, Tablet::NORMAL, {4, 9}});
    auto tablets = map.getTabletsForTable(0);
    EXPECT_EQ(2lu, tablets.size());
    EXPECT_EQ(ServerId(0, 1), tablets[0].serverId);
    EXPECT_EQ(ServerId(2, 1), tablets[1].serverId);

    tablets = map.getTabletsForTable(1);
    EXPECT_EQ(2lu, tablets.size());
    EXPECT_EQ(ServerId(1, 1), tablets[0].serverId);
    EXPECT_EQ(ServerId(3, 1), tablets[1].serverId);

    tablets = map.getTabletsForTable(2);
    EXPECT_EQ(1lu, tablets.size());
    EXPECT_EQ(ServerId(4, 1), tablets[0].serverId);

    tablets = map.getTabletsForTable(3);
    EXPECT_EQ(0lu, tablets.size());
}

TEST_F(TabletMapTest, modifyTablet) {
    map.addTablet({0, 1, 6, {0, 1}, Tablet::NORMAL, {0, 5}});
    map.modifyTablet(0, 1, 6, {1, 2}, Tablet::RECOVERING, {3, 9});
    Tablet tablet = map.getTablet(0, 1, 6);
    EXPECT_EQ(ServerId(1, 2), tablet.serverId);
    EXPECT_EQ(Tablet::RECOVERING, tablet.status);
    EXPECT_EQ(LogPosition(3, 9), tablet.ctime);
    EXPECT_THROW(map.modifyTablet(0, 0, 0, {0, 0}, Tablet::NORMAL, {0, 0}),
                 TabletMap::NoSuchTablet);
}

TEST_F(TabletMapTest, removeTabletsForTable) {
    map.addTablet({0, 1, 6, {0, 1}, Tablet::NORMAL, {0, 5}});
    map.addTablet({1, 2, 7, {1, 1}, Tablet::NORMAL, {1, 6}});
    map.addTablet({0, 3, 8, {2, 1}, Tablet::NORMAL, {2, 7}});

    EXPECT_EQ(0lu, map.removeTabletsForTable(2).size());
    EXPECT_EQ(3lu, map.size());

    auto tablets = map.removeTabletsForTable(1);
    EXPECT_EQ(2lu, map.size());
    foreach (const auto& tablet, tablets) {
        EXPECT_THROW(map.getTablet(tablet.tableId,
                                   tablet.startKeyHash,
                                   tablet.endKeyHash),
                     TabletMap::NoSuchTablet);
    }

    tablets = map.removeTabletsForTable(0);
    EXPECT_EQ(0lu, map.size());
    foreach (const auto& tablet, tablets) {
        EXPECT_THROW(map.getTablet(tablet.tableId,
                                   tablet.startKeyHash,
                                   tablet.endKeyHash),
                     TabletMap::NoSuchTablet);
    }
}

TEST_F(TabletMapTest, serialize) {
    CoordinatorServerList serverList(context);
    auto id1 = serverList.add("mock:host=one", {WireFormat::MASTER_SERVICE}, 1);
    auto id2 = serverList.add("mock:host=two", {WireFormat::MASTER_SERVICE}, 2);
    map.addTablet({0, 1, 6, id1, Tablet::NORMAL, {0, 5}});
    map.addTablet({1, 2, 7, id2, Tablet::NORMAL, {1, 6}});
    ProtoBuf::Tablets tablets;
    map.serialize(serverList, tablets);
    EXPECT_EQ("tablet { table_id: 0 start_key_hash: 1 end_key_hash: 6 "
              "state: NORMAL server_id: 1 service_locator: \"mock:host=one\" "
              "ctime_log_head_id: 0 ctime_log_head_offset: 5 } "
              "tablet { table_id: 1 start_key_hash: 2 end_key_hash: 7 "
              "state: NORMAL server_id: 2 service_locator: \"mock:host=two\" "
              "ctime_log_head_id: 1 ctime_log_head_offset: 6 }",
              tablets.ShortDebugString());
}

TEST_F(TabletMapTest, setStatusForServer) {
    map.addTablet({0, 1, 6, {0, 1}, Tablet::NORMAL, {0, 5}});
    map.addTablet({1, 2, 7, {1, 1}, Tablet::NORMAL, {1, 6}});
    map.addTablet({0, 3, 8, {0, 1}, Tablet::NORMAL, {2, 7}});

    EXPECT_EQ(0lu, map.setStatusForServer({2, 1}, Tablet::RECOVERING).size());

    auto tablets = map.setStatusForServer({0, 1}, Tablet::RECOVERING);
    EXPECT_EQ(2lu, tablets.size());
    foreach (const auto& tablet, tablets) {
        Tablet inMap = map.getTablet(tablet.tableId,
                                     tablet.startKeyHash,
                                     tablet.endKeyHash);
        EXPECT_EQ(ServerId(0, 1), tablet.serverId);
        EXPECT_EQ(ServerId(0, 1), inMap.serverId);
        EXPECT_EQ(Tablet::RECOVERING, tablet.status);
        EXPECT_EQ(Tablet::RECOVERING, inMap.status);
    }

    tablets = map.setStatusForServer({1, 1}, Tablet::RECOVERING);
    ASSERT_EQ(1lu, tablets.size());
    auto tablet = tablets[0];
    Tablet inMap = map.getTablet(tablet.tableId,
                                 tablet.startKeyHash,
                                 tablet.endKeyHash);
    EXPECT_EQ(ServerId(1, 1), tablet.serverId);
    EXPECT_EQ(ServerId(1, 1), inMap.serverId);
    EXPECT_EQ(Tablet::RECOVERING, tablet.status);
    EXPECT_EQ(Tablet::RECOVERING, inMap.status);
}

TEST_F(TabletMapTest, splitTablet) {
    map.addTablet({0, 0, ~0lu, {1, 0}, Tablet::NORMAL, {2, 3}});
    map.splitTablet(0, 0, ~0lu, ~0lu / 2);
    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 9223372036854775806 "
              "serverId: 1 status: NORMAL "
              "ctime: 2, 3 } "
              "Tablet { tableId: 0 "
              "startKeyHash: 9223372036854775807 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1 status: NORMAL "
              "ctime: 2, 3 }",
              map.debugString());

    map.splitTablet(0, 0, 9223372036854775806, 4611686018427387903);
    EXPECT_EQ("Tablet { tableId: 0 startKeyHash: 0 "
              "endKeyHash: 4611686018427387902 "
              "serverId: 1 status: NORMAL "
              "ctime: 2, 3 } "
              "Tablet { tableId: 0 "
              "startKeyHash: 9223372036854775807 "
              "endKeyHash: 18446744073709551615 "
              "serverId: 1 status: NORMAL "
              "ctime: 2, 3 } "
              "Tablet { tableId: 0 "
              "startKeyHash: 4611686018427387903 "
              "endKeyHash: 9223372036854775806 "
              "serverId: 1 status: NORMAL "
              "ctime: 2, 3 }",
              map.debugString());

    EXPECT_THROW(map.splitTablet(0, 0, 16, 8),
                 TabletMap::NoSuchTablet);

    EXPECT_THROW(map.splitTablet(0, 0, 0, ~0ul / 2),
                 TabletMap::BadSplit);

    EXPECT_THROW(map.splitTablet(1, 0, ~0ul, ~0ul / 2),
                 TabletMap::NoSuchTablet);
}

}  // namespace RAMCloud
