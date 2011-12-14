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
    explicit MockBackupSelector(size_t count)
        : backups()
        , nextIndex(0)
    {
        makeSimpleHostList(count);
    }

    Backup* selectPrimary(uint32_t numBackups, const ServerId backupIds[]) {
        return selectSecondary(numBackups, backupIds);
    }

    Backup* selectSecondary(uint32_t numBackups, const ServerId backupIds[]) {
        assert(backups.size());
        for (uint32_t i = 0; i < numBackups; ++i)
            TEST_LOG("conflicting backupId: %lu", *backupIds[i]);
        Backup* backup = &backups[nextIndex++];
        nextIndex %= backups.size();
        return backup;
    }

    void makeSimpleHostList(size_t count) {
        for (size_t i = 0; i < count; ++i) {
            backups.push_back(Backup());
            Backup& backup = backups.back();
            backup.set_is_backup(true);
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
    uint32_t writeRpcsInFlight;
    CountingDeleter deleter;
    const ServerId masterId;
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
        , writeRpcsInFlight(0)
        , deleter()
        , masterId(ServerId(999, 0))
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
        for (int i = 0; i < 100; ++i)
            transport.setInput("0");

        void* segMem = operator new(ReplicatedSegment::sizeOf(numReplicas));
        segment = std::unique_ptr<ReplicatedSegment>(
                new(segMem) ReplicatedSegment(taskManager, backupSelector,
                                              deleter, writeRpcsInFlight,
                                              masterId, segmentId,
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

TEST_F(ReplicatedSegmentTest, varLenArrayAtEnd) {
    // replicas[0] must be the last member of ReplicatedSegment
    EXPECT_EQ(static_cast<void*>(segment.get() + 1),
              static_cast<void*>(&segment.get()->replicas[0]));
    reset();
}

TEST_F(ReplicatedSegmentTest, constructor) {
    EXPECT_EQ(openLen, segment->queued.bytes);
    EXPECT_TRUE(segment->queued.open);
    ASSERT_FALSE(taskManager.isIdle());
    EXPECT_EQ(segment.get(), taskManager.tasks.front());
    reset();
}

TEST_F(ReplicatedSegmentTest, free) {
    segment->write(openLen);
    segment->close(NULL);
    segment->free();
    EXPECT_TRUE(segment->freeQueued);
    EXPECT_TRUE(segment->isScheduled());
    ASSERT_FALSE(taskManager.isIdle());
    EXPECT_EQ(segment.get(), taskManager.tasks.front());
    reset();
}

TEST_F(ReplicatedSegmentTest, freeWriteRpcInProgress) {
    segment->write(openLen);
    segment->close(NULL);
    taskManager.proceed(); // writeRpc created
    segment->free();

    // make sure the backup "free" opcode was not sent
    EXPECT_TRUE(TestUtil::doesNotMatchPosixRegex("0x2001e",
                                                 transport.outputLog));
    ASSERT_TRUE(segment->replicas[0]);
    EXPECT_FALSE(segment->replicas[0]->writeRpc); // ensure the write completed
    EXPECT_FALSE(segment->replicas[0]->freeRpc);
    EXPECT_TRUE(segment->isScheduled());

    taskManager.proceed();
    ASSERT_TRUE(segment->replicas[0]);
    EXPECT_FALSE(segment->replicas[0]->writeRpc);
    EXPECT_TRUE(segment->replicas[0]->freeRpc); // ensure free gets sent
    EXPECT_TRUE(segment->isScheduled());

    reset();
}

TEST_F(ReplicatedSegmentTest, close) {
    reset();
    segment->close(NULL);
    EXPECT_TRUE(segment->isScheduled());
    ASSERT_FALSE(taskManager.isIdle());
    EXPECT_EQ(segment.get(), taskManager.tasks.front());
    EXPECT_TRUE(segment->queued.close);
    reset();
}

TEST_F(ReplicatedSegmentTest, sync) {
    segment->sync(); // first sync sends the opens
    EXPECT_EQ("clientSend: 0x20022 0 999 0 888 0 0 10 5 0 abcedfghij | "
              "clientSend: 0x20022 0 999 0 888 0 0 10 1 0 abcedfghij",
               transport.outputLog);
    transport.outputLog = "";
    EXPECT_TRUE(segment->getAcked().open);
    EXPECT_EQ(openLen, segment->getAcked().bytes);

    segment->sync(); // second sync doesn't need to send anything
    EXPECT_EQ("", transport.outputLog);
    transport.outputLog = "";
    EXPECT_EQ(openLen, segment->getAcked().bytes);

    segment->write(openLen + 10);
    segment->sync(openLen); // doesn't send anything
    EXPECT_EQ("", transport.outputLog);
    transport.outputLog = "";
    segment->sync(openLen + 1); // will wait until after the next send
    EXPECT_EQ("clientSend: 0x20022 0 999 0 888 0 10 10 0 0 klmnopqrst | "
              "clientSend: 0x20022 0 999 0 888 0 10 10 0 0 klmnopqrst",
               transport.outputLog);
    transport.outputLog = "";
    EXPECT_EQ(openLen + 10, segment->getAcked().bytes);
}

TEST_F(ReplicatedSegmentTest, syncDoubleCheckCrossSegmentOrderingConstraints) {
    void* segMem = operator new(ReplicatedSegment::sizeOf(numReplicas));
    auto newHead = std::unique_ptr<ReplicatedSegment>(
            new(segMem) ReplicatedSegment(taskManager, backupSelector,
                                          deleter, writeRpcsInFlight,
                                          masterId, segmentId + 1,
                                          data, openLen, numReplicas,
                                          MAX_BYTES_PER_WRITE));
    segment->close(newHead.get()); // close queued
    newHead->write(openLen + 10); // write queued

    // Mess up the queue order to simulate reorder due to failure.
    // This means newHead will be going first at 'taking turns'
    // with segment.
    {
        Task* t = taskManager.tasks.front();
        taskManager.tasks.pop();
        taskManager.tasks.push(t);
    }

    // Queued order of ops would be:
    // open newHead
    // open segment
    // write newHead
    // close segment

    // Required by constraints (and checked here):
    // open segment | open newHead (either order between these)
    // close segment
    // write newHead

    EXPECT_EQ("", transport.outputLog);
    newHead->sync();
    EXPECT_EQ("clientSend: 0x20022 0 999 0 889 0 0 10 5 0 abcedfghij | "
              "clientSend: 0x20022 0 999 0 889 0 0 10 1 0 abcedfghij | "
              "clientSend: 0x20022 0 999 0 888 0 0 10 5 0 abcedfghij | "
              "clientSend: 0x20022 0 999 0 888 0 0 10 1 0 abcedfghij | "
              "clientSend: 0x20022 0 999 0 888 0 10 0 2 0 | "
              "clientSend: 0x20022 0 999 0 888 0 10 0 2 0 | "
              "clientSend: 0x20022 0 999 0 889 0 10 10 0 0 klmnopqrst | "
              "clientSend: 0x20022 0 999 0 889 0 10 10 0 0 klmnopqrst",
              transport.outputLog);
}

TEST_F(ReplicatedSegmentTest, performTaskFreeNothingToDo) {
    segment->write(openLen + 10);
    segment->close(NULL);
    segment->free();
    taskManager.proceed();
    EXPECT_FALSE(segment->isScheduled());
    EXPECT_EQ(1u, deleter.count);
}

TEST_F(ReplicatedSegmentTest, performTaskFreeOneReplicaToFree) {
    segment->write(openLen);
    segment->close(NULL);
    segment->free();

    Transport::SessionRef session = transport.getSession();
    segment->replicas[0].construct(ServerId(666, 0), session);
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
    segment->replicas[0].construct(ServerId(666, 0), session);
    segment->replicas[0]->freeRpc.construct(segment->replicas[0]->client,
                                            masterId, segmentId);
    EXPECT_STREQ("clientSend: 0x2001e 0 999 0 888 0",
                 transport.outputLog.c_str());
    segment->performFree(segment->replicas[0]);
    EXPECT_FALSE(segment->replicas[0]);
}

TEST_F(ReplicatedSegmentTest, performFreeRpcFailed) {
    transport.clearInput();
    transport.setInput(NULL); // error response to replica free

    reset();
    Transport::SessionRef session = transport.getSession();

    segment->freeQueued = true;
    segment->replicas[0].construct(ServerId(666, 0), session);
    segment->replicas[0]->freeRpc.construct(segment->replicas[0]->client,
                                            masterId, segmentId);
    EXPECT_STREQ("clientSend: 0x2001e 0 999 0 888 0",
                 transport.outputLog.c_str());
    {
        TestLog::Enable _;
        segment->performFree(segment->replicas[0]);
        EXPECT_TRUE(TestUtil::matchesPosixRegex(
            "performFree: Failure freeing replica on backup, retrying: "
            "RAMCloud::TransportException: testing thrown", TestLog::get()));
    }
    EXPECT_TRUE(segment->freeQueued);
    EXPECT_TRUE(segment->isScheduled());
    ASSERT_TRUE(segment->replicas[0]);
    EXPECT_FALSE(segment->replicas[0]->freeRpc);

    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performFreeWriteRpcInProgress) {
    // It should be impossible to get into this situation now that free()
    // synchronously finishes outstanding write rpcs before starting the
    // free, but its worth keeping the code since it is more robust if
    // the code knows how to deal with queued frees while there are
    // outstanding writes in progress.
    segment->write(openLen);
    segment->close(NULL);
    taskManager.proceed(); // writeRpc created
    segment->freeQueued = true;
    segment->schedule();

    // make sure the backup "free" opcode was not sent
    EXPECT_TRUE(TestUtil::doesNotMatchPosixRegex("0x2001e",
                                                 transport.outputLog));
    ASSERT_TRUE(segment->replicas[0]);
    EXPECT_TRUE(segment->replicas[0]->writeRpc);
    EXPECT_FALSE(segment->replicas[0]->freeRpc);
    EXPECT_TRUE(segment->isScheduled());

    taskManager.proceed(); // performFree reaps the write, remains scheduled
    ASSERT_TRUE(segment->replicas[0]);
    EXPECT_FALSE(segment->replicas[0]->writeRpc);
    EXPECT_FALSE(segment->replicas[0]->freeRpc);
    EXPECT_TRUE(segment->isScheduled());

    taskManager.proceed(); // now it schedules the free
    ASSERT_TRUE(segment->replicas[0]);
    EXPECT_FALSE(segment->replicas[0]->writeRpc);
    EXPECT_TRUE(segment->replicas[0]->freeRpc);
    EXPECT_TRUE(segment->isScheduled());

    taskManager.proceed(); // free is reaped and the replica is destroyed
    EXPECT_FALSE(segment->replicas[0]);
    EXPECT_FALSE(segment->isScheduled());
    EXPECT_EQ(1u, deleter.count);
}

TEST_F(ReplicatedSegmentTest, performWriteOpen) {
    segment->write(openLen);
    segment->close(NULL);
    {
        TestLog::Enable _;
        taskManager.proceed();
        EXPECT_STREQ("selectSecondary: conflicting backupId: 0",
                     TestLog::get().c_str());
    }

    // "10 5" is length 10 (OPEN | PRIMARY), "10 1" is length 10 OPEN
    EXPECT_STREQ("clientSend: 0x20022 0 999 0 888 0 0 10 5 0 abcedfghij | "
                 "clientSend: 0x20022 0 999 0 888 0 0 10 1 0 abcedfghij",
                 transport.outputLog.c_str());

    EXPECT_TRUE(segment->replicas[0]);
    EXPECT_TRUE(segment->replicas[0]->writeRpc);
    EXPECT_TRUE(segment->replicas[0]->sent.open);
    EXPECT_EQ(openLen, segment->replicas[0]->sent.bytes);
    EXPECT_TRUE(segment->isScheduled());
    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performWriteOpenTooManyInFlight) {
    writeRpcsInFlight = ReplicatedSegment::MAX_WRITE_RPCS_IN_FLIGHT;
    segment->write(openLen);
    taskManager.proceed(); // try to send writes

    EXPECT_FALSE(segment->replicas[0]);
    EXPECT_EQ(ReplicatedSegment::MAX_WRITE_RPCS_IN_FLIGHT, writeRpcsInFlight);

    writeRpcsInFlight = ReplicatedSegment::MAX_WRITE_RPCS_IN_FLIGHT - 1;
    taskManager.proceed(); // retry writes since a slot freed up
    EXPECT_STREQ("clientSend: 0x20022 0 999 0 888 0 0 10 5 0 abcedfghij",
                 transport.outputLog.c_str());
    EXPECT_EQ(ReplicatedSegment::MAX_WRITE_RPCS_IN_FLIGHT, writeRpcsInFlight);
    ASSERT_TRUE(segment->replicas[0]);
    EXPECT_TRUE(segment->replicas[0]->writeRpc);
    EXPECT_TRUE(segment->replicas[0]->sent.open);
    EXPECT_EQ(openLen, segment->replicas[0]->sent.bytes);
    EXPECT_FALSE(segment->replicas[1]); // make sure only one was started
    EXPECT_TRUE(segment->isScheduled());

    taskManager.proceed(); // reap write and send the second replica's rpc
    EXPECT_STREQ("clientSend: 0x20022 0 999 0 888 0 0 10 5 0 abcedfghij | "
                 "clientSend: 0x20022 0 999 0 888 0 0 10 1 0 abcedfghij",
                 transport.outputLog.c_str());
    EXPECT_EQ(ReplicatedSegment::MAX_WRITE_RPCS_IN_FLIGHT, writeRpcsInFlight);
    ASSERT_TRUE(segment->replicas[1]);
    EXPECT_TRUE(segment->replicas[1]->writeRpc);
    EXPECT_TRUE(segment->replicas[1]->sent.open);
    EXPECT_EQ(openLen, segment->replicas[1]->sent.bytes);
    EXPECT_FALSE(segment->replicas[0]->writeRpc); // make sure one was started
    EXPECT_TRUE(segment->isScheduled());

    taskManager.proceed(); // reap write
    EXPECT_FALSE(segment->replicas[1]->writeRpc);
    EXPECT_EQ(uint32_t(ReplicatedSegment::MAX_WRITE_RPCS_IN_FLIGHT - 1),
              writeRpcsInFlight);
    EXPECT_FALSE(segment->isScheduled());
    EXPECT_EQ(0u, deleter.count);
}

TEST_F(ReplicatedSegmentTest, performWriteRpcIsReady) {
    segment->write(openLen);
    segment->close(NULL);

    taskManager.proceed();
    ASSERT_TRUE(segment->replicas[0]);
    EXPECT_EQ(openLen, segment->replicas[0]->sent.bytes);
    EXPECT_EQ(0u, segment->replicas[0]->acked.bytes);

    taskManager.proceed();
    ASSERT_TRUE(segment->replicas[0]);
    EXPECT_EQ(openLen, segment->replicas[0]->acked.bytes);
    EXPECT_TRUE(segment->isScheduled());
    EXPECT_FALSE(segment->replicas[0]->writeRpc);
    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performWriteRpcFailed) {
    transport.clearInput();
    transport.setInput("0"); // ok first replica open
    transport.setInput(NULL); // error second replica open
    transport.setInput("0"); // ok second replica reopen
    transport.setInput(NULL); // error first replica close
    transport.setInput("0"); // ok second replica close
    transport.setInput("0"); // ok first replica reclose

    segment->write(openLen);

    taskManager.proceed();  // send open requests
    ASSERT_TRUE(segment->replicas[0]);
    EXPECT_EQ(openLen, segment->replicas[0]->sent.bytes);
    EXPECT_EQ(0u, segment->replicas[0]->acked.bytes);
    ASSERT_TRUE(segment->replicas[1]);
    EXPECT_EQ(openLen, segment->replicas[1]->sent.bytes);

    EXPECT_STREQ("clientSend: 0x20022 0 999 0 888 0 0 10 5 0 abcedfghij | "
                 "clientSend: 0x20022 0 999 0 888 0 0 10 1 0 abcedfghij",
                 transport.outputLog.c_str());
    transport.outputLog = "";
    {
        TestLog::Enable _;
        taskManager.proceed();  // reap rpcs, second replica got error
        EXPECT_TRUE(TestUtil::matchesPosixRegex(
            "performWrite: Failure writing replica on backup, retrying: "
            "RAMCloud::TransportException: testing thrown", TestLog::get()));
    }
    EXPECT_TRUE(segment->isScheduled());
    ASSERT_TRUE(segment->replicas[0]);
    EXPECT_EQ(openLen, segment->replicas[0]->acked.bytes);
    EXPECT_FALSE(segment->replicas[0]->writeRpc);
    ASSERT_FALSE(segment->replicas[1]);

    taskManager.proceed();  // resend second open request
    EXPECT_STREQ("clientSend: 0x20022 0 999 0 888 0 0 10 1 0 abcedfghij",
                 transport.outputLog.c_str());
    transport.outputLog = "";
    taskManager.proceed();  // reap second open request

    segment->write(openLen + 10);
    segment->close(NULL);
    taskManager.proceed();  // send close requests
    EXPECT_STREQ("clientSend: 0x20022 0 999 0 888 0 10 10 2 0 klmnopqrst | "
                 "clientSend: 0x20022 0 999 0 888 0 10 10 2 0 klmnopqrst",
                 transport.outputLog.c_str());
    transport.outputLog = "";
    {
        TestLog::Enable _;
        taskManager.proceed();  // reap rpcs, first replica got error
        EXPECT_TRUE(TestUtil::matchesPosixRegex(
            "performWrite: Failure writing replica on backup, retrying: "
            "RAMCloud::TransportException: testing thrown", TestLog::get()));
    }
    EXPECT_TRUE(segment->isScheduled());
    ASSERT_TRUE(segment->replicas[0]);
    EXPECT_EQ(openLen, segment->replicas[0]->acked.bytes);
    EXPECT_EQ(openLen, segment->replicas[0]->sent.bytes);
    EXPECT_FALSE(segment->replicas[0]->writeRpc);
    ASSERT_TRUE(segment->replicas[1]);
    EXPECT_EQ(openLen + 10, segment->replicas[1]->acked.bytes);
    EXPECT_EQ(openLen + 10, segment->replicas[1]->sent.bytes);
    EXPECT_FALSE(segment->replicas[1]->writeRpc);

    taskManager.proceed();  // resend first close request
    EXPECT_STREQ("clientSend: 0x20022 0 999 0 888 0 10 10 2 0 klmnopqrst",
                 transport.outputLog.c_str());
    transport.outputLog = "";
    EXPECT_TRUE(segment->isScheduled());
    ASSERT_TRUE(segment->replicas[0]);
    EXPECT_EQ(openLen + 10, segment->replicas[0]->sent.bytes);
    EXPECT_TRUE(segment->replicas[0]->writeRpc);

    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performWriteMoreToSend) {
    segment->write(openLen + 20);
    segment->close(NULL);
    taskManager.proceed(); // send open
    EXPECT_TRUE(segment->isScheduled());
    taskManager.proceed(); // reap opens
    EXPECT_TRUE(segment->isScheduled());
    transport.outputLog = "";
    taskManager.proceed(); // send second round

    // "20 4" is length 20 (PRIMARY), "20 0" is length 20 NONE
    EXPECT_STREQ(
        "clientSend: 0x20022 0 999 0 888 0 10 20 2 0 klmnopqrstuvwxyzabce | "
        "clientSend: 0x20022 0 999 0 888 0 10 20 2 0 klmnopqrstuvwxyzabce",
        transport.outputLog.c_str());
    EXPECT_TRUE(segment->isScheduled());
    EXPECT_TRUE(segment->replicas[0]->writeRpc);

    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performWriteClosedButLongerThanMaxTxLimit) {
    segment->write(segment->maxBytesPerWriteRpc + segment->openLen + 1);
    segment->close(NULL);
    taskManager.proceed(); // send open
    EXPECT_TRUE(segment->isScheduled());
    taskManager.proceed(); // reap opens
    EXPECT_TRUE(segment->isScheduled());
    transport.outputLog = "";
    taskManager.proceed(); // send second round

    // "21 0" is length 21 NONE, "21 0" is length 21 NONE
    EXPECT_STREQ(
        "clientSend: 0x20022 0 999 0 888 0 10 21 0 0 klmnopqrstuvwxyzabced | "
        "clientSend: 0x20022 0 999 0 888 0 10 21 0 0 klmnopqrstuvwxyzabced",
        transport.outputLog.c_str());
    EXPECT_TRUE(segment->isScheduled());
    EXPECT_TRUE(segment->replicas[0]->writeRpc);
    transport.outputLog = "";

    taskManager.proceed(); // reap second round
    taskManager.proceed(); // send third (closing) round

    // "1 2" is length 1 CLOSE, "1 2" is length 1 CLOSE
    EXPECT_STREQ("clientSend: 0x20022 0 999 0 888 0 31 1 2 0 f | "
                 "clientSend: 0x20022 0 999 0 888 0 31 1 2 0 f",
                 transport.outputLog.c_str());
    EXPECT_TRUE(segment->isScheduled());
    EXPECT_TRUE(segment->replicas[0]->writeRpc);

    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performWriteEnsureNewHeadOpenAckedBeforeClose) {
    taskManager.proceed(); // send segment open
    taskManager.proceed(); // reap segment open

    void* segMem = operator new(ReplicatedSegment::sizeOf(numReplicas));
    auto newHead = std::unique_ptr<ReplicatedSegment>(
            new(segMem) ReplicatedSegment(taskManager, backupSelector,
                                          deleter, writeRpcsInFlight,
                                          masterId, segmentId + 1,
                                          data, openLen, numReplicas,
                                          MAX_BYTES_PER_WRITE));

    segment->close(newHead.get());

    taskManager.proceed(); // send newHead open, try segment close but can't

    EXPECT_TRUE(newHead->isScheduled());
    ASSERT_TRUE(newHead->replicas[0]);
    EXPECT_TRUE(newHead->replicas[0]->writeRpc);
    EXPECT_TRUE(newHead->replicas[0]->sent.open);
    EXPECT_FALSE(newHead->replicas[0]->acked.open);

    EXPECT_TRUE(segment->isScheduled());
    ASSERT_TRUE(segment->replicas[0]);
    EXPECT_FALSE(segment->replicas[0]->writeRpc);
    EXPECT_FALSE(segment->replicas[0]->sent.close);

    taskManager.proceed(); // reap newHead open, try segment close should work

    EXPECT_FALSE(newHead->isScheduled());
    ASSERT_TRUE(newHead->replicas[0]);
    EXPECT_FALSE(newHead->replicas[0]->writeRpc);
    EXPECT_TRUE(newHead->replicas[0]->acked.open);

    EXPECT_TRUE(segment->isScheduled());
    ASSERT_TRUE(segment->replicas[0]);
    EXPECT_TRUE(segment->replicas[0]->writeRpc);
    EXPECT_TRUE(segment->replicas[0]->sent.close);

    EXPECT_EQ(0u, deleter.count);
    reset();
}

TEST_F(ReplicatedSegmentTest, performWriteEnsureCloseBeforeNewHeadWrittenTo) {
    void* segMem = operator new(ReplicatedSegment::sizeOf(numReplicas));
    auto newHead = std::unique_ptr<ReplicatedSegment>(
            new(segMem) ReplicatedSegment(taskManager, backupSelector,
                                          deleter, writeRpcsInFlight,
                                          masterId, segmentId + 1,
                                          data, openLen, numReplicas,
                                          MAX_BYTES_PER_WRITE));
    taskManager.proceed(); // send segment open for both
    taskManager.proceed(); // reap segment open for both

    segment->close(newHead.get()); // close queued
    newHead->write(openLen + 10); // write queued

    taskManager.proceed(); // send close rpc, try newHead write but can't

    EXPECT_TRUE(newHead->isScheduled());
    EXPECT_FALSE(newHead->precedingSegmentCloseAcked);
    ASSERT_TRUE(newHead->replicas[0]);
    EXPECT_FALSE(newHead->replicas[0]->writeRpc);
    EXPECT_TRUE(newHead->replicas[0]->acked.open);
    EXPECT_EQ(openLen, newHead->replicas[0]->sent.bytes);

    EXPECT_TRUE(segment->isScheduled());
    ASSERT_TRUE(segment->replicas[0]);
    EXPECT_TRUE(segment->replicas[0]->writeRpc);
    EXPECT_TRUE(segment->replicas[0]->sent.close);
    EXPECT_FALSE(segment->replicas[0]->acked.close);

    taskManager.proceed(); // reap close rpc, send newHead write

    EXPECT_TRUE(newHead->isScheduled());
    EXPECT_TRUE(newHead->precedingSegmentCloseAcked);
    ASSERT_TRUE(newHead->replicas[0]);
    EXPECT_TRUE(newHead->replicas[0]->writeRpc);
    EXPECT_EQ(openLen + 10, newHead->replicas[0]->sent.bytes);

    EXPECT_FALSE(segment->isScheduled());
    ASSERT_TRUE(segment->replicas[0]);
    EXPECT_FALSE(segment->replicas[0]->writeRpc);
    EXPECT_TRUE(segment->replicas[0]->acked.close);

    EXPECT_EQ(0u, deleter.count);
    newHead->scheduled = false;
    reset();
}

} // namespace RAMCloud
