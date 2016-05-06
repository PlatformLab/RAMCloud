/* Copyright (c) 2014-2016 Stanford University
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

#include "AbstractLog.h"
#include "Context.h"
#include "MasterService.h"
#include "MockCluster.h"
#include "PreparedOps.h"
#include "RamCloud.h"
#include "StringUtil.h"

namespace RAMCloud {

using WireFormat::TxParticipant;

/**
 * Unit tests for PreparedOps.
 */
class PreparedOpsTest : public ::testing::Test {
  public:
    Context context;

    PreparedOps writes;

    PreparedOpsTest()
        : context()
        , writes(&context)
    {
        context.dispatch = new Dispatch(false);
        writes.bufferOp(1, 10, 1011);
    }

    ~PreparedOpsTest() {}

    DISALLOW_COPY_AND_ASSIGN(PreparedOpsTest);
};

TEST_F(PreparedOpsTest, bufferWrite) {
    writes.bufferOp(2, 8, 1028);
    PreparedOps::PreparedItem* item = writes.items[std::make_pair(2UL, 8UL)];
    EXPECT_EQ(1028UL, item->newOpPtr);
    EXPECT_TRUE(item->isRunning());

    // Use during recovery. Should not set timer.
    writes.bufferOp(2, 9, 1029, true);
    item = writes.items[std::make_pair(2UL, 9UL)];
    EXPECT_EQ(1029UL, item->newOpPtr);
    EXPECT_FALSE(item->isRunning());
}

TEST_F(PreparedOpsTest, removeOp) {
    EXPECT_EQ(1011UL, writes.items[std::make_pair(1, 10)]->newOpPtr);
    writes.removeOp(1, 10);
    EXPECT_EQ(writes.items.end(), writes.items.find(std::make_pair(1, 10)));
}

TEST_F(PreparedOpsTest, getOp) {
    EXPECT_EQ(1011UL, writes.getOp(1, 10));
    EXPECT_EQ(1011UL, writes.getOp(1, 10));
    writes.removeOp(1, 10);
    EXPECT_EQ(0UL, writes.getOp(1, 10));
    EXPECT_EQ(0UL, writes.getOp(1, 11));
    EXPECT_EQ(0UL, writes.getOp(2, 10));
}

TEST_F(PreparedOpsTest, markDeletedAndIsDeleted) {
    EXPECT_FALSE(writes.isDeleted(1, 11));
    writes.markDeleted(1, 11);
    EXPECT_TRUE(writes.isDeleted(1, 11));

    EXPECT_FALSE(writes.isDeleted(2, 9));
    writes.bufferOp(2, 9, 1029, true);
    EXPECT_EQ(1029UL, writes.getOp(2, 9));
    EXPECT_FALSE(writes.isDeleted(2, 9));
}

/**
 * Unit tests for PreparedItem.
 */
class PreparedItemTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    Server* server1;
    Server* server2;
    Server* server3;
    MasterService* service1;
    MasterService* service2;
    MasterService* service3;
    uint64_t tableId1;
    uint64_t tableId2;
    uint64_t tableId3;

    PreparedItemTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , ramcloud()
        , server1()
        , server2()
        , server3()
        , service1()
        , service2()
        , service3()
        , tableId1(-1)
        , tableId2(-2)
        , tableId3(-3)
    {
        Cycles::mockTscValue = 100;

        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master1";
        config.maxObjectKeySize = 512;
        config.maxObjectDataSize = 1024;
        config.segmentSize = 128*1024;
        config.segletSize = 128*1024;
        server1 = cluster.addServer(config);
        service1 = server1->master.get();
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master2";
        server2 = cluster.addServer(config);
        service2 = server2->master.get();
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master3";
        server3 = cluster.addServer(config);
        service3 = server3->master.get();
        ramcloud.construct(&context, "mock:host=coordinator");

        // Make some tables.
        tableId1 = ramcloud->createTable("table1");
        tableId2 = ramcloud->createTable("table2");
        tableId3 = ramcloud->createTable("table3");
    }

    ~PreparedItemTest()
    {
        Cycles::mockTscValue = 0;
    }

    bool waitForTxRecoveryDone()
    {
        // See "Timing-Dependent Tests" in designNotes.
        for (int i = 0; i < 1000; i++) {
            service1->context->dispatch->poll();
            service2->context->dispatch->poll();
            service3->context->dispatch->poll();
            if (service1->preparedOps.items.size() == 0 &&
                    service2->preparedOps.items.size() == 0 &&
                    service3->preparedOps.items.size() == 0 &&
                    service1->txRecoveryManager.recoveries.size() == 0 &&
                    service2->txRecoveryManager.recoveries.size() == 0 &&
                    service3->txRecoveryManager.recoveries.size() == 0) {
                return true;
            }
            usleep(1000);
        }
        EXPECT_EQ(0lu, service1->preparedOps.items.size());
        EXPECT_EQ(0lu, service2->preparedOps.items.size());
        EXPECT_EQ(0lu, service3->preparedOps.items.size());
        EXPECT_EQ(0lu, service1->txRecoveryManager.recoveries.size());
        EXPECT_EQ(0lu, service2->txRecoveryManager.recoveries.size());
        EXPECT_EQ(0lu, service3->txRecoveryManager.recoveries.size());
        return false;
    }

    DISALLOW_COPY_AND_ASSIGN(PreparedItemTest);
};

TEST_F(PreparedItemTest, handleTimerEvent_basic) {
    Key key1(tableId3, "key1", 4);
    Key key2(tableId2, "key2", 4);
    Key key3(tableId1, "key3", 4);
    WireFormat::TxParticipant participants[3];
    participants[0] = {tableId3, key1.getHash(), 11};
    participants[1] = {tableId2, key2.getHash(), 12};
    participants[2] = {tableId1, key3.getHash(), 13};

    ParticipantList participantList(participants, 3, 1, 10);
    uint64_t logRef;
    service1->objectManager.logTransactionParticipantList(participantList,
                                                          &logRef);
    service1->unackedRpcResults.recoverRecord(
            participantList.header.clientLeaseId,
            participantList.header.clientTransactionId,
            0,
            reinterpret_cast<void*>(logRef));

    Buffer keyAndValBuffer;

    Object::appendKeysAndValueToBuffer(key3, "val", 3, &keyAndValBuffer);

    PreparedOp op(WireFormat::TxPrepare::OpType::READ, 1, 10, 13,
                  tableId1, 0, 0, keyAndValBuffer);
    Buffer buf;
    op.assembleForLog(buf);
    Segment::Reference opRef;
    service1->objectManager.getLog()->append(LOG_ENTRY_TYPE_PREP, buf, &opRef);

    TestLog::Enable _;

    Cycles::mockTscValue = 100;
    service1->preparedOps.bufferOp(1, 13, opRef.toInteger());
    EXPECT_EQ(opRef.toInteger(), service1->preparedOps.getOp(1, 13));
    PreparedOps::PreparedItem* item = service1->preparedOps.items[
            std::make_pair<uint64_t, uint64_t>(1, 13)];
    EXPECT_TRUE(item != NULL && item->isRunning());
    service1->context->dispatch->poll();
    EXPECT_EQ("", TestLog::get());

    Cycles::mockTscValue += Cycles::fromMicroseconds(
            PreparedOps::PreparedItem::TX_TIMEOUT_US * 1.5);
    service1->context->dispatch->poll();
    EXPECT_TRUE(waitForTxRecoveryDone());
    EXPECT_TRUE(StringUtil::contains(TestLog::get(),
        "handleTimerEvent: TxHintFailed RPC is sent to owner of tableId 3 "
        "and keyHash 8205713012933148717."));
}

TEST_F(PreparedItemTest, handleTimerEvent_noParticipantList) {
    Key key1(tableId3, "key1", 4);
    Key key2(tableId2, "key2", 4);
    Key key3(tableId1, "key3", 4);

    Buffer keyAndValBuffer;

    Object::appendKeysAndValueToBuffer(key3, "val", 3, &keyAndValBuffer);

    PreparedOp op(WireFormat::TxPrepare::OpType::READ, 1, 11, 13,
                  tableId1, 0, 0, keyAndValBuffer);
    Buffer buf;
    op.assembleForLog(buf);
    Segment::Reference opRef;
    service1->objectManager.getLog()->append(LOG_ENTRY_TYPE_PREP, buf, &opRef);

    TestLog::Enable _("handleTimerEvent");

    Cycles::mockTscValue = 100;
    service1->preparedOps.bufferOp(1, 13, opRef.toInteger());
    EXPECT_EQ(opRef.toInteger(), service1->preparedOps.getOp(1, 13));
    PreparedOps::PreparedItem* item = service1->preparedOps.items[
            std::make_pair<uint64_t, uint64_t>(1, 13)];

    TestLog::reset();

    item->handleTimerEvent();

    EXPECT_EQ("handleTimerEvent: "
            "Unable to find participant list record for TxId (1, 11); "
            "client transaction recovery could not be requested.",
            TestLog::get());
}

} // namespace RAMCloud
