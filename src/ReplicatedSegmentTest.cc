/* Copyright (c) 2009-2011 Stanford University
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

#include <queue>

#include "TestUtil.h"
#include "Memory.h"
#include "ReplicatedSegment.h"
#include "ShortMacros.h"
#include "TaskManager.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * backups.push_back Backups into this selector and the select methods will
 * replay them.  No bounds checking done, use it carefully.
 */
struct MockBackupSelector : public BaseBackupSelector {
    MockBackupSelector(size_t count)
        : backups()
        , nextIndex(0)
    {
        makeSimpleHostList(count);
    }

    Backup* selectPrimary(uint32_t numBackups, const uint64_t backupIds[]) {
        return selectSecondary(numBackups, backupIds);
    }

    Backup* selectSecondary(uint32_t numBackups, const uint64_t backupIds[]) {
        assert(backups.size());
        Backup* backup = &backups[nextIndex++];
        nextIndex %= backups.size();
        return backup;
    }

    void makeSimpleHostList(size_t count) {
        for (size_t i = 0; i < count; ++i) {
            backups.push_back(Backup());
            Backup& backup = backups.back();
            backup.set_server_type(ProtoBuf::BACKUP);
            backup.set_server_id(downCast<uint32_t>(i));
            string locator = format("mock:host=backup%lu", backup.server_id());
            backup.set_service_locator(locator);
            backup.set_user_data(0u);
        }
    }

    std::vector<Backup> backups;
    size_t nextIndex;
};

struct CountingDeleter : public ReplicatedSegment::Deleter {
    CountingDeleter()
        : count(0) {}

    void destroyAndFreeReplicatedSegment(ReplicatedSegment*
                                            replicatedSegment) {
        ++count;
    }

    uint32_t count;
};

struct ReplicatedSegmentTest : public ::testing::Test {
    enum { DATA_LEN = 100 };
    enum { MAX_BYTES_PER_WRITE = 21 };
    TaskManager taskManager;
    CountingDeleter deleter;
    const uint64_t masterId;
    const uint64_t segmentId;
    char data[DATA_LEN];
    const uint32_t openLen;
    const uint32_t numReplicas;
    MockBackupSelector backupSelector;
    MockTransport transport;
    TransportManager::MockRegistrar _;
    std::unique_ptr<ReplicatedSegment> segment;

    ReplicatedSegmentTest()
        : taskManager()
        , deleter()
        , masterId(999)
        , segmentId(888)
        , data()
        , openLen(10)
        , numReplicas(2)
        , backupSelector(numReplicas)
        , transport()
        , _(transport)
        , segment(NULL)
    {
        // Queue STATUS_OK responses. Works to ack both frees and writes.
        transport.setInput("0");
        transport.setInput("0");
        transport.setInput("0");
        transport.setInput("0");
        void* segMem = operator new(ReplicatedSegment::sizeOf(numReplicas));
        segment = std::unique_ptr<ReplicatedSegment>(
                new (segMem) ReplicatedSegment(taskManager, backupSelector,
                                               deleter, masterId, segmentId,
                                               data, openLen, numReplicas,
                                               MAX_BYTES_PER_WRITE));
        const char* msg = "abcedfghijklmnopqrstuvwxyz";
        size_t msgLen = strlen(msg);
        for (int i = 0; i < DATA_LEN; ++i)
            data[i] = msg[i % msgLen];
    }

    void reset() {
        taskManager.tasks.pop();
        segment->scheduled = false;
    }

    DISALLOW_COPY_AND_ASSIGN(ReplicatedSegmentTest);
};

TEST_F(ReplicatedSegmentTest, constructor) {
    EXPECT_EQ(openLen, segment->queued.bytes);
    EXPECT_TRUE(segment->queued.open);
    ASSERT_FALSE(taskManager.isIdle());
    EXPECT_EQ(segment.get(), taskManager.tasks.front());
    reset();
}

TEST_F(ReplicatedSegmentTest, free) {
    segment->write(openLen, true);
    segment->free();
    EXPECT_TRUE(segment->freeQueued);
    EXPECT_TRUE(segment->isScheduled());
    ASSERT_FALSE(taskManager.isIdle());
    EXPECT_EQ(segment.get(), taskManager.tasks.front());
    reset();
}

TEST_F(ReplicatedSegmentTest, write) {
    segment->write(openLen + 10, false);
    EXPECT_TRUE(segment->isScheduled());
    ASSERT_FALSE(taskManager.isIdle());
    EXPECT_EQ(segment.get(), taskManager.tasks.front());
    reset();
    segment->write(openLen + 10, true);
    EXPECT_TRUE(segment->queued.close);
    reset();
}

TEST_F(ReplicatedSegmentTest, performTaskFreeNothingToDo) {
    segment->write(openLen + 10, true);
    segment->free();
    taskManager.proceed();
    EXPECT_FALSE(segment->isScheduled());
    EXPECT_EQ(1u, deleter.count);
}

TEST_F(ReplicatedSegmentTest, performTaskFreeOneReplicaToFree) {
    segment->write(openLen, true);
    segment->free();

    Transport::SessionRef session = transport.getSession();
    segment->replicas[0].construct(session);
    taskManager.proceed();
    EXPECT_TRUE(segment->isScheduled());
    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performTaskWrite) {
    taskManager.proceed();
    ASSERT_TRUE(segment->replicas[0]);
    EXPECT_TRUE(segment->isScheduled());
    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performFreeNothingToDo) {
    segment->freeQueued = true;
    segment->performFree(segment->replicas[0]);
    EXPECT_FALSE(segment->replicas[0]);
    EXPECT_TRUE(segment->isScheduled());
    reset();
}

TEST_F(ReplicatedSegmentTest, performFreeRpcIsReady) {
    reset();
    Transport::SessionRef session = transport.getSession();

    segment->freeQueued = true;
    segment->replicas[0].construct(session);
    segment->replicas[0]->freeRpc.construct(segment->replicas[0]->client,
                                            masterId, segmentId);
    EXPECT_STREQ("clientSend: 0x4001d 0 999 0 888 0",
                 transport.outputLog.c_str());
    segment->performFree(segment->replicas[0]);
    EXPECT_FALSE(segment->replicas[0]);
}

// TODO: Unit test performFreeRpcFailed once we have the right code there

TEST_F(ReplicatedSegmentTest, performFreeWriteRpcInProgress) {
    MockTransport transport;
    TransportManager::MockRegistrar _(transport);

    segment->write(openLen, true);
    taskManager.proceed();
    segment->free();

    // make sure the backup "free" opcode was not sent
    EXPECT_TRUE(TestUtil::doesNotMatchPosixRegex("0x4001d",
                                                 transport.outputLog));

    EXPECT_TRUE(segment->replicas[0]);
    EXPECT_FALSE(segment->replicas[0]->freeRpc);
    EXPECT_TRUE(segment->isScheduled());
    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performWriteNotOpen) {
    segment->write(openLen, true);
    taskManager.proceed();

    // "10 5" is length 10 (OPEN | PRIMARY), "10 1" is length 10 OPEN
    EXPECT_STREQ("clientSend: 0x40021 0 999 0 888 0 0 10 5 0 abcedfghij | "
                 "clientSend: 0x40021 0 999 0 888 0 0 10 1 0 abcedfghij",
                 transport.outputLog.c_str());

    EXPECT_TRUE(segment->replicas[0]);
    EXPECT_TRUE(segment->replicas[0]->writeRpc);
    EXPECT_TRUE(segment->replicas[0]->sent.open);
    EXPECT_EQ(openLen, segment->replicas[0]->sent.bytes);
    EXPECT_TRUE(segment->isScheduled());
    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performWriteRpcIsReady) {
    segment->write(openLen, true);

    taskManager.proceed();
    ASSERT_TRUE(segment->replicas[0]);
    EXPECT_EQ(openLen, segment->replicas[0]->sent.bytes);
    EXPECT_EQ(0u, segment->replicas[0]->acked.bytes);

    taskManager.proceed();
    ASSERT_TRUE(segment->replicas[0]);
    EXPECT_EQ(openLen, segment->replicas[0]->acked.bytes);
    EXPECT_TRUE(segment->isScheduled()); // TODO: Do we want one-shot O/W/C?
    EXPECT_FALSE(segment->replicas[0]->writeRpc);
    EXPECT_EQ(0u, deleter.count);
    reset();
}

// TODO: Unit test performWriteRpcFailed once we have the right code there

TEST_F(ReplicatedSegmentTest, performWriteMoreToSend) {
    segment->write(openLen + 20, true);
    taskManager.proceed(); // send open
    EXPECT_TRUE(segment->isScheduled());
    taskManager.proceed(); // reap opens
    EXPECT_TRUE(segment->isScheduled());
    transport.outputLog = "";
    taskManager.proceed(); // send second round

    // "20 4" is length 20 (PRIMARY), "20 0" is length 20 NONE
    EXPECT_STREQ(
        "clientSend: 0x40021 0 999 0 888 0 10 20 2 0 abcedfghijklmnopqrst | "
        "clientSend: 0x40021 0 999 0 888 0 10 20 2 0 abcedfghijklmnopqrst",
        transport.outputLog.c_str());
    EXPECT_TRUE(segment->isScheduled());
    EXPECT_TRUE(segment->replicas[0]->writeRpc);

    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performWriteClosedButLongerThanMaxTxLimit) {
    segment->write(segment->maxBytesPerWriteRpc + segment->openLen + 1, true);
    taskManager.proceed(); // send open
    EXPECT_TRUE(segment->isScheduled());
    taskManager.proceed(); // reap opens
    EXPECT_TRUE(segment->isScheduled());
    transport.outputLog = "";
    taskManager.proceed(); // send second round

    // "21 0" is length 21 NONE, "21 0" is length 21 NONE
    EXPECT_STREQ(
        "clientSend: 0x40021 0 999 0 888 0 10 21 0 0 abcedfghijklmnopqrstu | "
        "clientSend: 0x40021 0 999 0 888 0 10 21 0 0 abcedfghijklmnopqrstu",
        transport.outputLog.c_str());
    EXPECT_TRUE(segment->isScheduled());
    EXPECT_TRUE(segment->replicas[0]->writeRpc);
    transport.outputLog = "";

    taskManager.proceed(); // reap second round
    taskManager.proceed(); // send third (closing) round

    // "1 2" is length 1 CLOSE, "1 2" is length 1 CLOSE
    EXPECT_STREQ("clientSend: 0x40021 0 999 0 888 0 31 1 2 0 a | "
                 "clientSend: 0x40021 0 999 0 888 0 31 1 2 0 a",
                 transport.outputLog.c_str());
    EXPECT_TRUE(segment->isScheduled());
    EXPECT_TRUE(segment->replicas[0]->writeRpc);

    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performWriteEverythingIsInFlight) {
    // TODO: Test write RPC outstanding, make sure we stay
    // scheduled; hard to test with MockTransport.
    reset();
}

} // namespace RAMCloud
