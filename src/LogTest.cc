/* Copyright (c) 2010-2012 Stanford University
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
class LogTest : public ::testing::Test {
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

    LogTest()
        : context(),
          serverId(ServerId(57, 0)),
          serverList(&context),
          serverConfig(ServerConfig::forTesting()),
          replicaManager(&context, serverId, 0),
          allocator(serverConfig),
          segmentManager(&context, serverConfig, serverId,
                         allocator, replicaManager),
          entryHandlers(),
          l(&context, serverConfig, entryHandlers,
            segmentManager, replicaManager)
    {
        l.sync();
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(LogTest);
};

TEST_F(LogTest, constructor) {
    SegletAllocator allocator2(serverConfig);
    SegmentManager segmentManager2(&context, serverConfig, serverId,
                                   allocator2, replicaManager);
    Log l2(&context, serverConfig, entryHandlers,
           segmentManager2, replicaManager);
    EXPECT_EQ(static_cast<LogSegment*>(NULL), l2.head);
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

TEST_F(LogTest, append_basic) {
    uint32_t dataLen = serverConfig.segmentSize / 2 + 1;
    char* data = new char[dataLen];
    LogSegment* oldHead = l.head;

    int appends = 0;
    while (l.append(LOG_ENTRY_TYPE_OBJ, data, dataLen, true)) {
        if (appends++ == 0)
            EXPECT_EQ(oldHead, l.head);
        else
            EXPECT_NE(oldHead, l.head);
        oldHead = l.head;
    }
    // This depends on ServerConfig's number of bytes allocated to the log.
    EXPECT_EQ(239, appends);

    // getEntry()'s test ensures actual data gets there.

    delete[] data;
}

static bool
appendFilter(string s)
{
    return s == "append";
}

TEST_F(LogTest, append_tooBigToEverFit) {
    TestLog::Enable _(appendFilter);

    char* data = new char[serverConfig.segmentSize + 1];
    LogSegment* oldHead = l.head;

    EXPECT_THROW(l.append(LOG_ENTRY_TYPE_OBJ,
                          data,
                          serverConfig.segmentSize + 1,
                          true),
        FatalError);
    EXPECT_NE(oldHead, l.head);
    EXPECT_EQ("append: Entry too big to append to log: 131073 bytes of type 2",
        TestLog::get());
    delete[] data;
}

TEST_F(LogTest, free) {
    // Currently nothing to do - it just passes through to SegmentManager
}

TEST_F(LogTest, getEntry) {
    uint64_t data = 0x123456789ABCDEF0UL;
    Buffer sourceBuffer;
    sourceBuffer.append(&data, sizeof(data));
    HashTable::Reference reference;
    EXPECT_TRUE(l.append(LOG_ENTRY_TYPE_OBJ, sourceBuffer, false, reference));

    LogEntryType type;
    Buffer buffer;
    type = l.getEntry(reference, buffer);
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, type);
    EXPECT_EQ(sizeof(data), buffer.getTotalLength());
    EXPECT_EQ(data, *buffer.getStart<uint64_t>());
}

static bool
syncFilter(string s)
{
    return s == "sync";
}

TEST_F(LogTest, sync) {
    TestLog::Enable _(syncFilter);
    l.sync();
    EXPECT_TRUE(StringUtil::endsWith(TestLog::get(), "sync: log synced"));
}

TEST_F(LogTest, getHeadPosition) {
    {
        // unsynced should return <0, 0>...
        SegletAllocator allocator2(serverConfig);
        SegmentManager segmentManager2(&context, serverConfig, serverId,
                                       allocator2, replicaManager);
        Log l2(&context, serverConfig, entryHandlers,
               segmentManager2, replicaManager);
        EXPECT_EQ(Log::Position(0, 0), l2.getHeadPosition());
    }

    // synced returns something else...
    EXPECT_EQ(Log::Position(0, 62), l.getHeadPosition());

    char data[1000];
    l.append(LOG_ENTRY_TYPE_OBJ, data, sizeof(data), true);
    EXPECT_EQ(Log::Position(0, 1065), l.getHeadPosition());

    while (l.getHeadPosition().getSegmentId() == 0)
        l.append(LOG_ENTRY_TYPE_OBJ, data, sizeof(data), true);

    EXPECT_EQ(Log::Position(1, 1073), l.getHeadPosition());
}

TEST_F(LogTest, getSegmentId) {
    Buffer buffer;
    char data[1000];
    buffer.append(data, sizeof(data));
    HashTable::Reference reference;

    int zero = 0, one = 0, other = 0;
    while (l.getHeadPosition().getSegmentId() == 0) {
        EXPECT_TRUE(l.append(LOG_ENTRY_TYPE_OBJ, buffer, false, reference));
        switch (l.getSegmentId(reference)) {
            case 0: zero++; break;
            case 1: one++; break;
            default: other++; break;
        }
    }

    EXPECT_EQ(130, zero);
    EXPECT_EQ(1, one);
    EXPECT_EQ(0, other);
}

TEST_F(LogTest, allocateHeadIfStillOn) {
    LogSegment* oldHead = l.head;
    l.allocateHeadIfStillOn({0UL});
    EXPECT_NE(oldHead, l.head);

    oldHead = l.head;
    l.allocateHeadIfStillOn({});
    EXPECT_NE(oldHead, l.head);

    oldHead = l.head;
    l.allocateHeadIfStillOn({0UL});
    EXPECT_EQ(oldHead, l.head);
}

TEST_F(LogTest, containsSegment) {
    EXPECT_TRUE(l.containsSegment(0));
    EXPECT_FALSE(l.containsSegment(1));

    char data[1000];
    while (l.getHeadPosition().getSegmentId() == 0)
        l.append(LOG_ENTRY_TYPE_OBJ, data, sizeof(data), true);

    EXPECT_TRUE(l.containsSegment(0));
    EXPECT_TRUE(l.containsSegment(1));
    EXPECT_FALSE(l.containsSegment(2));
}

TEST_F(LogTest, buildReference) {
    HashTable::Reference r = l.buildReference(0x123456U, 0x789ABCU);
    EXPECT_EQ(0x123456789ABCUL, r.get());
}

TEST_F(LogTest, referenceToSlot) {
    HashTable::Reference r(0x123456789ABCUL);
    EXPECT_EQ(0x123456U, l.referenceToSlot(r));
}

TEST_F(LogTest, referenceToOffset) {
    HashTable::Reference r(0x123456789ABCUL);
    EXPECT_EQ(0x789ABCU, l.referenceToOffset(r));
}

} // namespace RAMCloud
