/* Copyright (c) 2010-2016 Stanford University
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
#include "Log.h"
#include "LogEntryTypes.h"
#include "Memory.h"
#include "MockCluster.h"
#include "RamCloud.h"
#include "ServerConfig.h"
#include "StringUtil.h"
#include "Transport.h"
#include "MasterTableMetadata.h"

namespace RAMCloud {

class DoNothingHandlers : public LogEntryHandlers {
  public:
    uint32_t getTimestamp(LogEntryType type, Buffer& buffer) { return 0; }
    void relocate(LogEntryType type,
                  Buffer& oldBuffer,
                  Log::Reference oldReference,
                  LogEntryRelocator& relocator) { }
};

/**
 * Unit tests for Log.
 */
class LogTest : public ::testing::Test {
  public:
    Context context;
    ServerId serverId;
    ServerList serverList;
    ServerConfig serverConfig;
    ReplicaManager replicaManager;
    MasterTableMetadata masterTableMetadata;
    SegletAllocator allocator;
    SegmentManager segmentManager;
    DoNothingHandlers entryHandlers;
    Log l;

    LogTest()
        : context(),
          serverId(ServerId(57, 0)),
          serverList(&context),
          serverConfig(ServerConfig::forTesting()),
          replicaManager(&context, &serverId, 0, false, false, false),
          masterTableMetadata(),
          allocator(&serverConfig),
          segmentManager(&context, &serverConfig, &serverId,
                         allocator, replicaManager, &masterTableMetadata),
          entryHandlers(),
          l(&context, &serverConfig, &entryHandlers,
            &segmentManager, &replicaManager)
    {
        l.sync();
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(LogTest);
};

/**
 * Unit tests for Log sync (especially syncTo).
 * Here, we build a mockCluster with a backup, and log has non-zero replica.
 * This real replica is necessary to test subtleties in checking previously
 * closed segment is fully replicated.
 */
class LogSyncTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    ServerList serverList;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    ServerConfig backup1Config;
    ServerId backup1Id;

    ServerConfig masterConfig;
    MasterService* service;
    Server* masterServer;
    Log *l;

    // To make tests that don't need big segments faster, set a smaller default
    // segmentSize. Since we can't provide arguments to it in gtest, nor can we
    // apparently template easily on that, we need to subclass this if we want
    // to provide a fixture with a different value.
    explicit LogSyncTest(uint32_t segmentSize = 256 * 1024)
        : logEnabler()
        , context()
        , serverList(&context)
        , cluster(&context)
        , ramcloud()
        , backup1Config(ServerConfig::forTesting())
        , backup1Id()
        , masterConfig(ServerConfig::forTesting())
        , service()
        , masterServer()
        , l()
    {
        //Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        backup1Config.localLocator = "mock:host=backup1";
        backup1Config.services = {WireFormat::BACKUP_SERVICE,
                WireFormat::ADMIN_SERVICE};
        backup1Config.segmentSize = segmentSize;
        backup1Config.backup.numSegmentFrames = 30;
        Server* server = cluster.addServer(backup1Config);
        server->backup->testingSkipCallerIdCheck = true;
        backup1Id = server->serverId;

        masterConfig = ServerConfig::forTesting();
        masterConfig.segmentSize = segmentSize;
        masterConfig.maxObjectDataSize = segmentSize / 4;
        masterConfig.localLocator = "mock:host=master";
        masterConfig.services = {WireFormat::MASTER_SERVICE,
                WireFormat::ADMIN_SERVICE};
        masterConfig.master.logBytes = segmentSize * 30;
        masterConfig.master.numReplicas = 1;
        masterServer = cluster.addServer(masterConfig);
        service = masterServer->master.get();
        service->objectManager.log.sync();
        l = &service->objectManager.log;
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(LogSyncTest);
};

TEST_F(LogTest, constructor) {
    SegletAllocator allocator2(&serverConfig);
    SegmentManager segmentManager2(&context, &serverConfig, &serverId,
                                   allocator2, replicaManager,
                                   &masterTableMetadata);
    Log l2(&context, &serverConfig, &entryHandlers,
           &segmentManager2, &replicaManager);
    EXPECT_EQ(static_cast<LogSegment*>(NULL), l2.head);
    EXPECT_NE(static_cast<LogCleaner*>(NULL), l2.cleaner);
}

TEST_F(LogTest, destructor) {
    // ensure that the cleaner is deleted
    SegletAllocator allocator2(&serverConfig);
    SegmentManager segmentManager2(&context, &serverConfig, &serverId,
                                   allocator2, replicaManager,
                                   &masterTableMetadata);
    Tub<Log> l2;
    l2.construct(&context, &serverConfig, &entryHandlers,
           &segmentManager2, &replicaManager);
    l2->enableCleaner();
    TestLog::Enable _;
    l2.destroy();
    EXPECT_EQ("cleanerThreadEntry: LogCleaner thread started | "
              "cleanerThreadEntry: LogCleaner thread stopping | "
              "~LogCleaner: destroyed", TestLog::get());
}

TEST_F(LogTest, enableCleaner_and_disableCleaner) {
    {
        TestLog::Enable _;
        l.enableCleaner();
        usleep(100);
        EXPECT_EQ("cleanerThreadEntry: LogCleaner thread started",
            TestLog::get());
    }

    {
        TestLog::Enable _;
        l.disableCleaner();
        usleep(100);
        EXPECT_EQ("cleanerThreadEntry: LogCleaner thread stopping",
            TestLog::get());
    }

    TestLog::Enable _;
    l.disableCleaner();
    l.disableCleaner();
    usleep(100);
    EXPECT_EQ("", TestLog::get());
}

TEST_F(LogTest, getHead) {
    EXPECT_EQ(l.getHead(),
            LogPosition(l.head->id, l.head->getAppendedLength()));
    LogPosition oldPos = l.getHead();
    l.append(LOG_ENTRY_TYPE_OBJ, "hi", 2);
    EXPECT_LT(oldPos, l.getHead());
}

static bool
syncFilter(string s)
{
    return s == "sync" || s == "syncTo";
}

TEST_F(LogTest, sync) {
    TestLog::Enable _(syncFilter);
    l.sync();
    EXPECT_EQ("sync: sync not needed: already fully replicated",
        TestLog::get());

    TestLog::reset();
    l.append(LOG_ENTRY_TYPE_OBJ, "hi", 2);
    EXPECT_NE(l.head->syncedLength, l.head->getAppendedLength());
    l.sync();
    EXPECT_EQ("sync: syncing segment 1 to offset 84 | sync: log synced",
        TestLog::get());
    EXPECT_EQ(l.head->syncedLength, l.head->getAppendedLength());

    TestLog::reset();
    l.sync();
    EXPECT_EQ("sync: sync not needed: already fully replicated",
        TestLog::get());

    // Test sync if preceding segment is not closed durably.
    TestLog::reset();
    l.append(LOG_ENTRY_TYPE_OBJ, "hi", 2);
    {
        SpinLock::Guard lock(l.appendLock);
        l.allocNewWritableHead();
    }
    l.sync();
    EXPECT_EQ("sync: syncing segment 2 to offset 88 | sync: log synced",
        TestLog::get());

    EXPECT_EQ(5U, l.metrics.totalSyncCalls);
}

TEST_F(LogSyncTest, syncTo) {
    TestLog::Enable _(syncFilter);
    l->sync();
    EXPECT_EQ("sync: sync not needed: already fully replicated",
        TestLog::get());

    TestLog::reset();
    Log::Reference reference, reference2;
    l->append(LOG_ENTRY_TYPE_OBJ, "hi", 2, &reference);
    EXPECT_NE(l->head->syncedLength, l->head->getAppendedLength());
    l->syncTo(reference);
    EXPECT_EQ("sync: syncing segment 1 to offset 84 | syncTo: log synced",
        TestLog::get());
    EXPECT_EQ(l->head->syncedLength, l->head->getAppendedLength());

    TestLog::reset();
    l->append(LOG_ENTRY_TYPE_OBJ, "ho", 2, &reference2);
    EXPECT_NE(l->head->syncedLength, l->head->getAppendedLength());
    l->syncTo(reference);
    EXPECT_EQ("syncTo: sync not needed: entry is already replicated",
        TestLog::get());

    // Test sync if preceding segment is not closed durably.
    TestLog::reset();
    {
        SpinLock::Guard lock(l->appendLock);
        l->allocNewWritableHead();
    }
    EXPECT_FALSE(l->getSegment(reference2)->closedCommitted);
    l->syncTo(reference2);
    EXPECT_TRUE(l->getSegment(reference2)->closedCommitted);
    EXPECT_EQ("sync: syncing segment 2 to offset 88 | syncTo: log synced",
        TestLog::get());

    EXPECT_EQ(5U, l->metrics.totalSyncCalls);
}

TEST_F(LogTest, rollHeadOver) {
    LogPosition oldPos = LogPosition(0, 0);
    LogSegment* oldHead = l.head;
    EXPECT_LT(oldPos, l.rollHeadOver());
    EXPECT_NE(oldHead, l.head);

    oldPos = LogPosition(l.head->id, l.head->getAppendedLength());
    oldHead = l.head;
    EXPECT_LT(oldPos, l.rollHeadOver());
    EXPECT_NE(oldHead, l.head);
}

TEST_F(LogTest, allocNextSegment) {
    SpinLock::Guard _(l.appendLock);

    LogSegment* segment = segmentManager.allocSideSegment(0, NULL);
    EXPECT_NE(static_cast<LogSegment*>(NULL), segment);
    while (segmentManager.allocSideSegment(0, NULL) != NULL) {
        // eat up all free segments
    }

    // if SegmentManager is tapped, should return NULL
    EXPECT_EQ(static_cast<LogSegment*>(NULL), l.allocNextSegment(false));

    // if we specify to block until we have space, it should return an
    // emergency head segment
    LogSegment* emergency = l.allocNextSegment(true);
    EXPECT_NE(static_cast<LogSegment*>(NULL), emergency);
    EXPECT_TRUE(emergency->isEmergencyHead);

    // and if we free a segment, we should get back an emegerency head
    // even if the blocking flag
    segment->replicatedSegment->close();
    segmentManager.free(segment);
    EXPECT_EQ(segment, l.allocNextSegment(true));
}

} // namespace RAMCloud
