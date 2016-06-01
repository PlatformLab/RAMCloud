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
#include "RamCloud.h"
#include "StringUtil.h"
#include "TransactionManager.h"

namespace RAMCloud {

using WireFormat::TxParticipant;

/**
 * Unit tests for TransactionManager.
 */
class TransactionManagerTest : public ::testing::Test {
  public:
    Context context;
    ClusterClock clusterClock;
    ClientLeaseValidator clientLeaseValidator;
    ServerId serverId;
    ServerList serverList;
    ServerConfig masterConfig;
    MasterTableMetadata masterTableMetadata;
    UnackedRpcResults unackedRpcResults;
    TransactionManager transactionManager;
    TxRecoveryManager txRecoveryManager;
    TabletManager tabletManager;
    ObjectManager objectManager;

    TransactionManagerTest()
        : context()
        , clusterClock()
        , clientLeaseValidator(&context, &clusterClock)
        , serverId(5)
        , serverList(&context)
        , masterConfig(ServerConfig::forTesting())
        , masterTableMetadata()
        , unackedRpcResults(&context, NULL, &clientLeaseValidator)
        , transactionManager(&context, &unackedRpcResults)
        , txRecoveryManager(&context)
        , tabletManager()
        , objectManager(&context,
                        &serverId,
                        &masterConfig,
                        &tabletManager,
                        &masterTableMetadata,
                        &unackedRpcResults,
                        &transactionManager,
                        &txRecoveryManager)
    {
        context.dispatch = new Dispatch(false);
        transactionManager.bufferOp(TransactionId(1, 1), 10, 1011);
    }

    ~TransactionManagerTest()
    {
        Cycles::mockTscValue = 0;
    }

    DISALLOW_COPY_AND_ASSIGN(TransactionManagerTest);
};

TEST_F(TransactionManagerTest, registerTransaction_basic) {
    TransactionId txId(42, 9);
    TransactionManager::InProgressTransaction *iptx;
    WireFormat::TxParticipant participants[3];
    // construct participant list.
    participants[0] = WireFormat::TxParticipant(1, 2, 10);
    participants[1] = WireFormat::TxParticipant(123, 234, 11);
    participants[2] = WireFormat::TxParticipant(111, 222, 12);
    ParticipantList participantList(participants, 3, 42, 9);
    Buffer inBuffer;
    participantList.assembleForLog(inBuffer);

    {
        TransactionManager::Lock lock(transactionManager.mutex);
        EXPECT_TRUE(transactionManager.getTransaction(txId, lock) == NULL);
    }

    Cycles::mockTscValue = 100;

    Status status =
            transactionManager.registerTransaction(participantList,
                                                   inBuffer,
                                                   objectManager.getLog());
    EXPECT_EQ(STATUS_OK, status);

    {
        TransactionManager::Lock lock(transactionManager.mutex);
        iptx = transactionManager.getTransaction(txId, lock);
        EXPECT_TRUE(iptx != NULL);
    }

    Log::Reference logRef(iptx->participantListLogRef);
    Buffer outBuffer;
    LogEntryType type = objectManager.log.getEntry(logRef, outBuffer);
    ParticipantList outputParticipantList(outBuffer);

    EXPECT_EQ(LOG_ENTRY_TYPE_TXPLIST, type);
    EXPECT_EQ(3U, outputParticipantList.header.participantCount);
    EXPECT_EQ(42U, outputParticipantList.header.clientLeaseId);

    EXPECT_EQ(1U, outputParticipantList.participants[0].tableId);
    EXPECT_EQ(2U, outputParticipantList.participants[0].keyHash);
    EXPECT_EQ(10U, outputParticipantList.participants[0].rpcId);

    EXPECT_EQ(123U, outputParticipantList.participants[1].tableId);
    EXPECT_EQ(234U, outputParticipantList.participants[1].keyHash);
    EXPECT_EQ(11U, outputParticipantList.participants[1].rpcId);

    EXPECT_EQ(111U, outputParticipantList.participants[2].tableId);
    EXPECT_EQ(222U, outputParticipantList.participants[2].keyHash);
    EXPECT_EQ(12U, outputParticipantList.participants[2].rpcId);

    EXPECT_EQ(3 * Cycles::fromMicroseconds(50000), iptx->timeoutCycles);
    EXPECT_EQ(iptx->timeoutCycles + 100, iptx->triggerTime);
    EXPECT_TRUE(iptx->isRunning());
}

TEST_F(TransactionManagerTest, registerTransaction_duplicate) {
    TransactionId txId(42, 9);
    TransactionManager::InProgressTransaction *iptx;
    WireFormat::TxParticipant participants[3];
    // construct participant list.
    participants[0] = WireFormat::TxParticipant(1, 2, 10);
    participants[1] = WireFormat::TxParticipant(123, 234, 11);
    participants[2] = WireFormat::TxParticipant(111, 222, 12);
    ParticipantList participantList(participants, 3, 42, 9);
    Buffer inBuffer;
    participantList.assembleForLog(inBuffer);

    Cycles::mockTscValue = 1;

    // Pre-insert entry
    {
        TransactionManager::Lock lock(transactionManager.mutex);
        iptx = transactionManager.getOrAddTransaction(txId, lock);
        iptx->participantListLogRef = 100;
        iptx->timeoutCycles = 200;
        iptx->start(300);
    }

    EXPECT_EQ(100UL, iptx->participantListLogRef);
    EXPECT_EQ(200UL, iptx->timeoutCycles);
    EXPECT_EQ(300UL, iptx->triggerTime);
    EXPECT_TRUE(iptx->isRunning());

    TestLog::Enable _;
    ParticipantList participantListDuplicate(participants, 1, 42, 9);
    participantListDuplicate.assembleForLog(inBuffer);

    transactionManager.registerTransaction(participantListDuplicate,
                                           inBuffer,
                                           objectManager.getLog());
    EXPECT_EQ("registerTransaction: Skipping duplicate call to register "
              "transaction <42, 9>",
              TestLog::get());
    EXPECT_EQ(100UL, iptx->participantListLogRef);
    EXPECT_EQ(200UL, iptx->timeoutCycles);
    EXPECT_EQ(300UL, iptx->triggerTime);
    EXPECT_TRUE(iptx->isRunning());
}

TEST_F(TransactionManagerTest, bufferWrite) {
    transactionManager.bufferOp(TransactionId(2, 1), 8, 1028);
    TransactionManager::PreparedItem* item =
            transactionManager.items[std::make_pair(2UL, 8UL)];
    EXPECT_EQ(1028UL, item->newOpPtr);
    EXPECT_TRUE(item->isRunning());

    // Use during recovery. Should not set timer.
    transactionManager.bufferOp(TransactionId(2, 1), 9, 1029, true);
    item = transactionManager.items[std::make_pair(2UL, 9UL)];
    EXPECT_EQ(1029UL, item->newOpPtr);
    EXPECT_FALSE(item->isRunning());
}

TEST_F(TransactionManagerTest, removeOp) {
    EXPECT_EQ(1011UL,
              transactionManager.items[std::make_pair(1, 10)]->newOpPtr);
    transactionManager.removeOp(1, 10);
    EXPECT_EQ(transactionManager.items.end(),
              transactionManager.items.find(std::make_pair(1, 10)));
}

TEST_F(TransactionManagerTest, getOp) {
    EXPECT_EQ(1011UL, transactionManager.getOp(1, 10));
    EXPECT_EQ(1011UL, transactionManager.getOp(1, 10));
    transactionManager.removeOp(1, 10);
    EXPECT_EQ(0UL, transactionManager.getOp(1, 10));
    EXPECT_EQ(0UL, transactionManager.getOp(1, 11));
    EXPECT_EQ(0UL, transactionManager.getOp(2, 10));
}

TEST_F(TransactionManagerTest, markDeletedAndIsDeleted) {
    EXPECT_FALSE(transactionManager.isOpDeleted(1, 11));
    transactionManager.markOpDeleted(1, 11);
    EXPECT_TRUE(transactionManager.isOpDeleted(1, 11));

    EXPECT_FALSE(transactionManager.isOpDeleted(2, 9));
    transactionManager.bufferOp(TransactionId(2, 1), 9, 1029, true);
    EXPECT_EQ(1029UL, transactionManager.getOp(2, 9));
    EXPECT_FALSE(transactionManager.isOpDeleted(2, 9));
}

TEST_F(TransactionManagerTest, PreparedItem_constructor_destructor) {
    TransactionManager::Lock lock(transactionManager.mutex);
    TransactionId txId(42, 1);
    TransactionManager::InProgressTransaction* transaction =
            transactionManager.getOrAddTransaction(txId, lock);

    EXPECT_EQ(0, transaction->preparedOpCount);
    TransactionManager::PreparedItem* item =
            new TransactionManager::PreparedItem(transactionManager.context,
                                                 transaction,
                                                 0);
    EXPECT_EQ(1, transaction->preparedOpCount);
    {
        TransactionManager::PreparedItem otherItem(transactionManager.context,
                                                   transaction,
                                                   0);
        EXPECT_EQ(2, transaction->preparedOpCount);
        delete item;
        EXPECT_EQ(1, transaction->preparedOpCount);
    }
    EXPECT_EQ(0, transaction->preparedOpCount);
}

TEST_F(TransactionManagerTest, getTransaction) {
    TransactionManager::Lock lock(transactionManager.mutex);
    TransactionId txId(42, 1);
    EXPECT_TRUE(transactionManager.getTransaction(txId, lock) == NULL);
    TransactionManager::InProgressTransaction* iptx =
            new TransactionManager::InProgressTransaction(&transactionManager,
                                                          txId);
    transactionManager.transactions[txId] = iptx;
    EXPECT_TRUE(transactionManager.getTransaction(txId, lock) == iptx);
}

TEST_F(TransactionManagerTest, getOrAddTransaction) {
    TransactionManager::Lock lock(transactionManager.mutex);
    TransactionId txId(42, 1);
    EXPECT_TRUE(transactionManager.getTransaction(txId, lock) == NULL);
    TransactionManager::InProgressTransaction* iptx =
            transactionManager.getOrAddTransaction(txId, lock);
    TransactionManager::TransactionRegistry::iterator it =
            transactionManager.transactions.find(txId);
    EXPECT_TRUE(it != transactionManager.transactions.end());
    EXPECT_TRUE(it->second = iptx);
    EXPECT_TRUE(transactionManager.getTransaction(txId, lock) == iptx);
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
            if (service1->transactionManager.items.size() == 0 &&
                    service2->transactionManager.items.size() == 0 &&
                    service3->transactionManager.items.size() == 0 &&
                    service1->txRecoveryManager.recoveries.size() == 0 &&
                    service2->txRecoveryManager.recoveries.size() == 0 &&
                    service3->txRecoveryManager.recoveries.size() == 0) {
                return true;
            }
            usleep(1000);
        }
        EXPECT_EQ(0lu, service1->transactionManager.items.size());
        EXPECT_EQ(0lu, service2->transactionManager.items.size());
        EXPECT_EQ(0lu, service3->transactionManager.items.size());
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
    service1->transactionManager.bufferOp(
            participantList.getTransactionId(), 13, opRef.toInteger());
    EXPECT_EQ(opRef.toInteger(), service1->transactionManager.getOp(1, 13));
    TransactionManager::PreparedItem* item = service1->transactionManager.items[
            std::make_pair<uint64_t, uint64_t>(1, 13)];
    EXPECT_TRUE(item != NULL && item->isRunning());
    service1->context->dispatch->poll();
    EXPECT_EQ("", TestLog::get());

    Cycles::mockTscValue += Cycles::fromMicroseconds(
            TransactionManager::PreparedItem::TX_TIMEOUT_US * 1.5);
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
    service1->transactionManager.bufferOp(
            op.getTransactionId(), 13, opRef.toInteger());
    EXPECT_EQ(opRef.toInteger(), service1->transactionManager.getOp(1, 13));
    TransactionManager::PreparedItem* item = service1->transactionManager.items[
            std::make_pair<uint64_t, uint64_t>(1, 13)];

    TestLog::reset();

    item->handleTimerEvent();

    EXPECT_EQ("handleTimerEvent: "
            "Unable to find participant list record for TxId (1, 11); "
            "client transaction recovery could not be requested.",
            TestLog::get());
}

} // namespace RAMCloud
