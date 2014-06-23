/* Copyright (c) 2013-2014 Stanford University
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
#include "CleanableSegmentManager.h"
#include "Segment.h"
#include "SegmentIterator.h"
#include "SegmentManager.h"
#include "ServerConfig.h"
#include "ServerId.h"
#include "ServerList.h"
#include "StringUtil.h"
#include "LogEntryTypes.h"
#include "LogCleaner.h"
#include "ReplicaManager.h"
#include "MasterTableMetadata.h"

namespace RAMCloud {

class TestEntryHandlers : public LogEntryHandlers {
  public:
    TestEntryHandlers()
        : timestamp(0),
          attemptToRelocate(false)
    {
    }

    uint32_t
    getTimestamp(LogEntryType type, Buffer& buffer)
    {
        return timestamp;
    }

    void
    relocate(LogEntryType type,
             Buffer& oldBuffer,
             Log::Reference oldReference,
             LogEntryRelocator& relocator)
    {
        RAMCLOUD_TEST_LOG("type %d, size %u",
            downCast<int>(type), oldBuffer.size());
        if (attemptToRelocate)
            relocator.append(type, oldBuffer);
    }

    uint32_t timestamp;
    bool attemptToRelocate;
};

class MyServerConfig {
  public:
    MyServerConfig()
        : serverConfig(ServerConfig::forTesting())
    {
        serverConfig.segmentSize = 8 * 1024 * 1024;
        serverConfig.segletSize = 64 * 1024;
        serverConfig.master.logBytes = serverConfig.segmentSize * 50;
    }

    ServerConfig*
    operator()()
    {
        return &serverConfig;
    }

    ServerConfig serverConfig;
};

class CleanableSegmentManagerTest : public ::testing::Test {
  public:
    Context context;
    ServerId serverId;
    ServerList serverList;
    MyServerConfig serverConfig;
    ReplicaManager replicaManager;
    SegletAllocator allocator;
    MasterTableMetadata masterTableMetadata;
    SegmentManager segmentManager;
    TestEntryHandlers entryHandlers;
    LogCleaner cleaner;

    CleanableSegmentManagerTest()
        : context(),
          serverId(ServerId(57, 0)),
          serverList(&context),
          serverConfig(),
          replicaManager(&context, &serverId, 0, false, false),
          allocator(serverConfig()),
          masterTableMetadata(),
          segmentManager(&context, serverConfig(), &serverId,
                         allocator, replicaManager, &masterTableMetadata),
          entryHandlers(),
          cleaner(&context, serverConfig(), segmentManager,
                  replicaManager, entryHandlers)
    {
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(CleanableSegmentManagerTest);
};

TEST_F(CleanableSegmentManagerTest, update) {
    CleanableSegmentManager& csm = cleaner.cleanableSegments;
    CleanableSegmentManager::Lock guard(csm.lock);

    csm.update(guard);
    EXPECT_EQ("costBenefitCandidates [ ] compactionCandidates [ ] "
              "tombstoneScanCandidates [ ]", csm.toString());

    segmentManager.allocHeadSegment();
    csm.update(guard);
    EXPECT_EQ("costBenefitCandidates [ ] compactionCandidates [ ] "
              "tombstoneScanCandidates [ ]", csm.toString());

    segmentManager.allocHeadSegment();
    segmentManager.allocHeadSegment();
    segmentManager.allocHeadSegment();
    csm.update(guard);
    EXPECT_EQ("costBenefitCandidates [ id=1,cb=18446744073709551615 "
              "id=2,cb=18446744073709551615 id=3,cb=18446744073709551615 ] "
              "compactionCandidates [ id=1,cb=18446744073709551615 "
              "id=2,cb=18446744073709551615 id=3,cb=18446744073709551615 ] "
              "tombstoneScanCandidates [ id=1,ss=0 id=2,ss=0 id=3,ss=0 ]",
              csm.toString());

    segmentManager.allocHeadSegment();
    segmentManager.allocHeadSegment();
    segmentManager.allocHeadSegment();
    csm.update(guard);
    EXPECT_EQ("costBenefitCandidates [ id=2,cb=18446744073709551615 "
              "id=3,cb=18446744073709551615 id=1,cb=18446744073709551615 "
              "id=4,cb=18446744073709551615 id=5,cb=18446744073709551615 "
              "id=6,cb=18446744073709551615 ] "
              "compactionCandidates [ id=2,cb=18446744073709551615 "
              "id=3,cb=18446744073709551615 id=1,cb=18446744073709551615 "
              "id=4,cb=18446744073709551615 id=5,cb=18446744073709551615 "
              "id=6,cb=18446744073709551615 ] "
              "tombstoneScanCandidates [ id=2,ss=0 id=3,ss=0 id=1,ss=0 id=4,"
              "ss=0 id=5,ss=0 id=6,ss=0 ]",
              csm.toString());
}

}  // namespace RAMCloud
