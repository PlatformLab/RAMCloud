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

#include "Segment.h"
#include "ServerRpcPool.h"
#include "SideLog.h"
#include "LogEntryTypes.h"
#include "Memory.h"
#include "ServerConfig.h"
#include "StringUtil.h"
#include "Transport.h"

namespace RAMCloud {

class DoNothingHandlers : public LogEntryHandlers {
  public:
    uint32_t getTimestamp(LogEntryType type, Buffer& buffer) { return 0; }
    void relocate(LogEntryType type, Buffer& oldBuffer,
                  LogEntryRelocator& relocator) { }
};

/**
 * Unit tests for Log.
 */
class SideLogTest : public ::testing::Test {
  public:
    Context context;
    ServerId serverId;
    ServerList serverList;
    ServerConfig serverConfig;
    ReplicaManager replicaManager;
    SegletAllocator allocator;
    SegmentManager segmentManager;
    DoNothingHandlers entryHandlers;
    Log l;

    SideLogTest()
        : context(),
          serverId(ServerId(57, 0)),
          serverList(&context),
          serverConfig(ServerConfig::forTesting()),
          replicaManager(&context, serverId, 0),
          allocator(serverConfig),
          segmentManager(&context, serverConfig, serverId,
                         allocator, replicaManager),
          entryHandlers(),
          l(&context, &serverConfig, &entryHandlers,
            &segmentManager, &replicaManager)
    {
        l.sync();
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(SideLogTest);
};

TEST_F(SideLogTest, constructor_regular) {
}

TEST_F(SideLogTest, constructor_cleaner) {
}

TEST_F(SideLogTest, destructor) {
    Tub<SideLog> sl;
    TestLog::Enable _;

    // an empty sidelog does nothing
    sl.construct(&l);
    sl.destroy();
    EXPECT_EQ("", TestLog::get());

    // a non-empty sidelog aborts and returns segments to the SegmentManager
    sl.construct(&l);
    EXPECT_TRUE(sl->append(LOG_ENTRY_TYPE_OBJ, 0, "hi", 2));
    sl.destroy();
    EXPECT_TRUE(StringUtil::contains(TestLog::get(),
        "~SideLog: Aborting 1 uncommitted segment(s)"));
}

TEST_F(SideLogTest, commit) {
    TestLog::Enable _;
    SideLog sl(&l);

    // an empty sidelog shouldn't alter the log
    uint64_t headId = l.head->id;
    sl.commit();
    EXPECT_EQ(headId, l.head->id);

    EXPECT_TRUE(sl.append(LOG_ENTRY_TYPE_OBJ, 0, "hi", 2));
    LogSegment* newSeg = sl.segments[0];
    sl.commit();
    EXPECT_NE(headId, l.head->id);
    EXPECT_TRUE(newSeg->closed);
    EXPECT_TRUE(sl.segments.empty());
    EXPECT_EQ(
        "alloc: purpose: 2 | "
        "allocateSegment: Allocating new replicated segment for <57.0,2> | "
        "schedule: zero replicas: nothing to schedule | "
        "allocSideSegment: id = 2 | "
        "close: 57.0, 2, 0 | "
        "schedule: zero replicas: nothing to schedule | "
        "close: Segment 2 closed (length 30) | "
        "sync: syncing | "
        "alloc: purpose: 0 | "
        "allocateSegment: Allocating new replicated segment for <57.0,3> | "
        "schedule: zero replicas: nothing to schedule | "
        "close: 57.0, 1, 3 | "
        "schedule: zero replicas: nothing to schedule | "
        "close: Segment 1 closed (length 54)",
        TestLog::get());

    // an empty sidelog still shouldn't alter the log
    headId = l.head->id;
    sl.commit();
    EXPECT_EQ(headId, l.head->id);
}

TEST_F(SideLogTest, allocNextSegment) {
    SideLog sl(&l);


}

} // namespace RAMCloud
