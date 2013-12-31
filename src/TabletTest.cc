/* Copyright (c) 2012-2013 Stanford University
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

#include "Context.h"
#include "CoordinatorServerList.h"
#include "Tablet.h"

namespace RAMCloud {

class TabletTest : public ::testing::Test {
  public:
    Context context;
    std::mutex mutex;

    TabletTest()
        : context()
        , mutex()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
    }

    typedef std::unique_lock<std::mutex> Lock;
    DISALLOW_COPY_AND_ASSIGN(TabletTest);
};

TEST_F(TabletTest, serialize) {
    Lock lock(mutex);
    Tablet tablet({0, 1, 6, ServerId(1, 0), Tablet::NORMAL, {0, 5}});
    ProtoBuf::Tablets::Tablet serializedTablet;
    tablet.serialize(serializedTablet);
    EXPECT_EQ("table_id: 0 start_key_hash: 1 end_key_hash: 6 "
              "state: NORMAL server_id: 1 "
              "ctime_log_head_id: 0 ctime_log_head_offset: 5",
              serializedTablet.ShortDebugString());
}

TEST_F(TabletTest, debugString_default) {
    Lock lock(mutex);
    Tablet tablet({0, 1, 6, ServerId(1, 0), Tablet::NORMAL, {0, 5}});
    EXPECT_EQ("Tablet { tableId: 0, startKeyHash: 0x1, "
              "endKeyHash: 0x6, serverId: 1.0, status: NORMAL, "
              "ctime: 0.5 }",
              tablet.debugString());
    EXPECT_EQ("Tablet { tableId: 0, startKeyHash: 0x1, "
              "endKeyHash: 0x6, serverId: 1.0, status: NORMAL, "
              "ctime: 0.5 }",
              tablet.debugString(0));
}

TEST_F(TabletTest, debugString_1) {
    Lock lock(mutex);
    Tablet tablet({0, 1, 6, ServerId(1, 0), Tablet::NORMAL, {0, 5}});
    EXPECT_EQ("{ 0: 0x1-0x6 }", tablet.debugString(1));
}

}  // namespace RAMCloud
