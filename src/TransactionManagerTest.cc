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
 * Used for TransactionManager unit tests.
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
    ObjectManager objectManager;
    UnackedRpcResults unackedRpcResults;
    TransactionManager transactionManager;
    TxRecoveryManager txRecoveryManager;
    TabletManager tabletManager;

    TransactionManagerTest()
        : context()
        , clusterClock()
        , clientLeaseValidator(&context, &clusterClock)
        , serverId(5)
        , serverList(&context)
        , masterConfig(ServerConfig::forTesting())
        , masterTableMetadata()
        , objectManager(&context,
                        &serverId,
                        &masterConfig,
                        &tabletManager,
                        &masterTableMetadata,
                        &unackedRpcResults,
                        &transactionManager,
                        &txRecoveryManager)
        , unackedRpcResults(&context, NULL, &clientLeaseValidator)
        , transactionManager(&context,
                             objectManager.getLog(),
                             &unackedRpcResults)
        , txRecoveryManager(&context)
        , tabletManager()
    {
        context.dispatch = new Dispatch(false);
        transactionManager.bufferOp(TransactionId(1, 1), 10, 1011);
        WorkerTimer::disableTimerHandlers = true;
    }

    ~TransactionManagerTest()
    {
        Cycles::mockTscValue = 0;
        WorkerTimer::disableTimerHandlers = false;
    }

    DISALLOW_COPY_AND_ASSIGN(TransactionManagerTest);
};

/**
 * Used for InProgressTransaction unit tests.
 */
class InProgressTransactionTest : public ::testing::Test {
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
    TransactionId txId;

    InProgressTransactionTest()
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
        , txId(42, 9)
    {
        Cycles::mockTscValue = 100;
        WorkerTimer::disableTimerHandlers = true;

        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::ADMIN_SERVICE};
        config.localLocator = "mock:host=master1";
        config.maxObjectKeySize = 512;
        config.maxObjectDataSize = 1024;
        config.segmentSize = 128*1024;
        config.segletSize = 128*1024;
        server1 = cluster.addServer(config);
        service1 = server1->master.get();
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::ADMIN_SERVICE};
        config.localLocator = "mock:host=master2";
        server2 = cluster.addServer(config);
        service2 = server2->master.get();
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::ADMIN_SERVICE};
        config.localLocator = "mock:host=master3";
        server3 = cluster.addServer(config);
        service3 = server3->master.get();
        ramcloud.construct(&context, "mock:host=coordinator");

        // Make some tables.
        tableId1 = ramcloud->createTable("table1");
        tableId2 = ramcloud->createTable("table2");
        tableId3 = ramcloud->createTable("table3");

        // Setup InProgress Transactions
        WireFormat::TxParticipant participants[3];
        participants[0] = WireFormat::TxParticipant(1, 2, 10);
        participants[1] = WireFormat::TxParticipant(123, 234, 11);
        participants[2] = WireFormat::TxParticipant(111, 222, 12);
        ParticipantList participantList(participants, 3, 42, 9);
        Buffer buffer;
        participantList.assembleForLog(buffer);

        Cycles::mockTscValue = 100;

        service1->transactionManager.registerTransaction(
                participantList,
                buffer,
                service1->objectManager.getLog());
    }

    ~InProgressTransactionTest()
    {
        Cycles::mockTscValue = 0;
        WorkerTimer::disableTimerHandlers = false;
    }

    DISALLOW_COPY_AND_ASSIGN(InProgressTransactionTest);
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

    Buffer outBuffer;
    LogEntryType type = objectManager.log.getEntry(iptx->participantListLogRef,
                                                   outBuffer);
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

    // Stop iptx before it is allowed to actually run.
    iptx->stop();
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

    Cycles::mockTscValue = 100;

    // Pre-insert entry
    {
        TransactionManager::Lock lock(transactionManager.mutex);
        iptx = transactionManager.getOrAddTransaction(txId, lock);
        iptx->participantListLogRef = AbstractLog::Reference(100);
        iptx->timeoutCycles = 200;
        iptx->start(300);
    }

    EXPECT_TRUE(iptx->participantListLogRef == AbstractLog::Reference(100));
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
    EXPECT_TRUE(iptx->participantListLogRef == AbstractLog::Reference(100));
    EXPECT_EQ(200UL, iptx->timeoutCycles);
    EXPECT_EQ(300UL, iptx->triggerTime);
    EXPECT_TRUE(iptx->isRunning());

    // Reset iptx state to avoid unintended behavior during destruction
    iptx->participantListLogRef = AbstractLog::Reference();
    iptx->stop();
}

TEST_F(TransactionManagerTest, registerTransaction_timerStart) {
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

    Cycles::mockTscValue = 100;

    transactionManager.registerTransaction(participantList,
                                           inBuffer,
                                           objectManager.getLog());

    {
        TransactionManager::Lock lock(transactionManager.mutex);
        iptx = transactionManager.getTransaction(txId, lock);
    }

    EXPECT_EQ(100 + iptx->timeoutCycles, iptx->triggerTime);
    EXPECT_TRUE(iptx->isRunning());

    Cycles::mockTscValue = 200;

    transactionManager.registerTransaction(participantList,
                                           inBuffer,
                                           objectManager.getLog());

    EXPECT_EQ(200 + iptx->timeoutCycles, iptx->triggerTime);
    EXPECT_TRUE(iptx->isRunning());

    // Fake the existance of an async RPC.
    iptx->txHintFailedRpc.occupied = true;

    Cycles::mockTscValue = 300;

    transactionManager.registerTransaction(participantList,
                                           inBuffer,
                                           objectManager.getLog());

    EXPECT_EQ(200 + iptx->timeoutCycles, iptx->triggerTime);
    EXPECT_TRUE(iptx->isRunning());

    // Reset iptx to avoid unexpected behavior.
    iptx->stop();
    iptx->txHintFailedRpc.occupied = false;
}

TEST_F(TransactionManagerTest, markTransactionRecovered) {
    TransactionId txId(42, 9);
    TransactionManager::InProgressTransaction *iptx;

    transactionManager.markTransactionRecovered(txId);

    {
        TransactionManager::Lock lock(transactionManager.mutex);
        EXPECT_TRUE(transactionManager.getTransaction(txId, lock) == NULL);
    }

    {
        TransactionManager::Lock lock(transactionManager.mutex);
        iptx = transactionManager.getOrAddTransaction(txId, lock);
    }

    {
        TransactionManager::Lock lock(transactionManager.mutex);
        EXPECT_TRUE(transactionManager.getTransaction(txId, lock) == iptx);
    }

    EXPECT_FALSE(iptx->recovered);

    transactionManager.markTransactionRecovered(txId);

    EXPECT_TRUE(iptx->recovered);
}

TEST_F(TransactionManagerTest, relocateParticipantList_relocate) {
    WireFormat::TxParticipant participants[3];
    // construct participant list.
    participants[0] = WireFormat::TxParticipant(1, 2, 10);
    participants[1] = WireFormat::TxParticipant(123, 234, 11);
    participants[2] = WireFormat::TxParticipant(111, 222, 12);
    ParticipantList participantList(participants, 3, 42, 9);

    // Setup its initial existence.
    Buffer pListBuffer;
    participantList.assembleForLog(pListBuffer);
    TransactionId txId = participantList.getTransactionId();

    transactionManager.registerTransaction(participantList,
                                           pListBuffer,
                                           objectManager.getLog());

    TransactionManager::InProgressTransaction* transaction;
    {
        TransactionManager::Lock lock(transactionManager.mutex);
        transaction = transactionManager.getTransaction(txId, lock);
    }
    EXPECT_TRUE(transaction != NULL);
    Log::Reference oldPListReference = transaction->participantListLogRef;
    LogEntryType oldTypeInLog;
    Buffer oldBufferInLog;
    oldTypeInLog = objectManager.log.getEntry(oldPListReference,
                                              oldBufferInLog);
    EXPECT_EQ(LOG_ENTRY_TYPE_TXPLIST, oldTypeInLog);

    // Try to relocate.
    LogEntryRelocator relocator(
        objectManager.segmentManager.getHeadSegment(), 1000);
    transactionManager.relocateParticipantList(oldBufferInLog,
                                               oldPListReference,
                                               relocator);
    EXPECT_TRUE(relocator.didAppend);
    EXPECT_FALSE(transaction->participantListLogRef == oldPListReference);
}

TEST_F(TransactionManagerTest, relocateParticipantList_clean_completed) {
    WireFormat::TxParticipant participants[3];
    // construct participant list.
    participants[0] = WireFormat::TxParticipant(1, 2, 10);
    participants[1] = WireFormat::TxParticipant(123, 234, 11);
    participants[2] = WireFormat::TxParticipant(111, 222, 12);
    ParticipantList participantList(participants, 3, 42, 9);

    // Setup its initial existence.
    Buffer pListBuffer;
    Log::Reference oldPListReference;
    participantList.assembleForLog(pListBuffer);
    objectManager.getLog()->append(LOG_ENTRY_TYPE_TXPLIST,
                                   pListBuffer,
                                   &oldPListReference);

    LogEntryType oldTypeInLog;
    Buffer oldBufferInLog;
    oldTypeInLog = objectManager.log.getEntry(oldPListReference,
                                              oldBufferInLog);
    EXPECT_EQ(LOG_ENTRY_TYPE_TXPLIST, oldTypeInLog);

    // Try to relocate.
    LogEntryRelocator relocator(
        objectManager.segmentManager.getHeadSegment(), 1000);
    transactionManager.relocateParticipantList(oldBufferInLog,
                                               oldPListReference,
                                               relocator);
    EXPECT_FALSE(relocator.didAppend);
}

TEST_F(TransactionManagerTest, relocateParticipantList_clean_duplicate) {
    WireFormat::TxParticipant participants[3];
    // construct participant list.
    participants[0] = WireFormat::TxParticipant(1, 2, 10);
    participants[1] = WireFormat::TxParticipant(123, 234, 11);
    participants[2] = WireFormat::TxParticipant(111, 222, 12);
    ParticipantList participantList(participants, 3, 42, 9);

    // Setup its initial existence.
    Buffer pListBuffer;
    participantList.assembleForLog(pListBuffer);
    TransactionId txId = participantList.getTransactionId();

    transactionManager.registerTransaction(participantList,
                                           pListBuffer,
                                           objectManager.getLog());

    TransactionManager::InProgressTransaction* transaction;
    {
        TransactionManager::Lock lock(transactionManager.mutex);
        transaction = transactionManager.getTransaction(txId, lock);
    }
    EXPECT_TRUE(transaction != NULL);
    Log::Reference oldPListReference = transaction->participantListLogRef;
    LogEntryType oldTypeInLog;
    Buffer oldBufferInLog;
    oldTypeInLog = objectManager.log.getEntry(oldPListReference,
                                              oldBufferInLog);
    EXPECT_EQ(LOG_ENTRY_TYPE_TXPLIST, oldTypeInLog);

    // Change log reference so that the one in the log seems like a duplicate.
    uint64_t fakeLogRef = transaction->participantListLogRef.toInteger() + 1;
    transaction->participantListLogRef = AbstractLog::Reference(fakeLogRef);

    // Try to relocate.
    LogEntryRelocator relocator(
        objectManager.segmentManager.getHeadSegment(), 1000);
    transactionManager.relocateParticipantList(oldBufferInLog,
                                               oldPListReference,
                                               relocator);
    EXPECT_FALSE(relocator.didAppend);
    EXPECT_TRUE(transaction->participantListLogRef ==
                                        AbstractLog::Reference(fakeLogRef));

    // Clear log ref to avoid call to free
    transaction->participantListLogRef = AbstractLog::Reference();
}

TEST_F(TransactionManagerTest, bufferWrite) {
    transactionManager.bufferOp(TransactionId(2, 1), 8, 1028);
    TransactionManager::PreparedItem* item =
            transactionManager.items[std::make_pair(2UL, 8UL)];
    EXPECT_EQ(1028UL, item->newOpPtr);

    // Use during recovery. Should not set timer.
    transactionManager.bufferOp(TransactionId(2, 1), 9, 1029);
    item = transactionManager.items[std::make_pair(2UL, 9UL)];
    EXPECT_EQ(1029UL, item->newOpPtr);
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
    transactionManager.bufferOp(TransactionId(2, 1), 9, 1029);
    EXPECT_EQ(1029UL, transactionManager.getOp(2, 9));
    EXPECT_FALSE(transactionManager.isOpDeleted(2, 9));
}

TEST_F(TransactionManagerTest, Protector) {
    TransactionId txId(42, 21);
    TransactionId otherTxId(2, 1);
    TransactionManager::Protector* p2;
    TransactionManager::InProgressTransaction* iptx = NULL;
    {
        TransactionManager::Lock lock(transactionManager.mutex);
        iptx = transactionManager.getTransaction(txId, lock);
    }

    EXPECT_TRUE(iptx == NULL);
    {
        TransactionManager::Protector p0(&transactionManager, txId);
        // p0 is live
        {
            TransactionManager::Lock lock(transactionManager.mutex);
            iptx = transactionManager.getTransaction(txId, lock);
        }
        EXPECT_TRUE(iptx != NULL);
        EXPECT_EQ(1, iptx->cleaningDisabled);
        {
            TransactionManager::Protector p1(&transactionManager, txId);
            // p0, p1 is live
            EXPECT_EQ(2, iptx->cleaningDisabled);
            p2 = new TransactionManager::Protector(&transactionManager, txId);
            // p0, p1, p2 is live
            EXPECT_EQ(3, iptx->cleaningDisabled);
            {
                TransactionManager::Protector _(&transactionManager, otherTxId);
                EXPECT_EQ(3, iptx->cleaningDisabled);
            }
        }
        // p0, p2 is live
        EXPECT_EQ(2, iptx->cleaningDisabled);
        delete p2;
        // p0 is live
        EXPECT_EQ(1, iptx->cleaningDisabled);
    }
    // Nothing is live
    EXPECT_EQ(0, iptx->cleaningDisabled);
}

TEST_F(TransactionManagerTest, InProgressTransaction_destructor) {
    TransactionManager::Lock lock(transactionManager.mutex);

    TestLog::Enable _("free");
    {
        TransactionManager::InProgressTransaction iptx(&transactionManager,
                                                       TransactionId(42, 31),
                                                       lock);
    }
    EXPECT_EQ("", TestLog::get());

    TestLog::reset();

    uint64_t original = transactionManager.log->totalLiveBytes;
    Log::Reference logRef;
    {
        TransactionManager::InProgressTransaction iptx(&transactionManager,
                                                       TransactionId(42, 31),
                                                       lock);
        WireFormat::TxParticipant participants[3];
        // construct participant list.
        participants[0] = WireFormat::TxParticipant(1, 2, 10);
        participants[1] = WireFormat::TxParticipant(123, 234, 11);
        participants[2] = WireFormat::TxParticipant(111, 222, 12);
        ParticipantList participantList(participants, 1, 42, 9);
        Buffer assembledParticipantList;
        participantList.assembleForLog(assembledParticipantList);
        transactionManager.log->append(LOG_ENTRY_TYPE_TXPLIST,
                                       assembledParticipantList,
                                       &logRef);
        iptx.participantListLogRef = logRef;
        EXPECT_LT(original, transactionManager.log->totalLiveBytes);
    }
    EXPECT_EQ(original, transactionManager.log->totalLiveBytes);
    EXPECT_EQ(format("free: free on reference %" PRIu64, logRef.toInteger()),
              TestLog::get());
}

TEST_F(InProgressTransactionTest, handleTimerEvent_basic) {
    TransactionManager::InProgressTransaction* iptx;

    {
        TransactionManager::Lock lock(service1->transactionManager.mutex);
        iptx = service1->transactionManager.getTransaction(txId, lock);
    }

    TestLog::Enable _("handleTimerEvent");
    EXPECT_FALSE(iptx->txHintFailedRpc);
    EXPECT_EQ(100 + iptx->timeoutCycles, iptx->triggerTime);
    iptx->handleTimerEvent();
    iptx->stop();
    EXPECT_FALSE(iptx->txHintFailedRpc);
    EXPECT_EQ(100 + iptx->timeoutCycles, iptx->triggerTime);
    EXPECT_EQ("handleTimerEvent: TxID <42,9> sending TxHintFailed RPC to owner "
                    "of tableId 1 and keyHash 2. | "
              "handleTimerEvent: TxID <42,9> received ack for TxHintFailed "
                    "RPC; will wait for next timeout.",
              TestLog::get());
}

TEST_F(InProgressTransactionTest, handleTimerEvent_done) {
    TransactionManager::InProgressTransaction* iptx;

    {
        TransactionManager::Lock lock(service1->transactionManager.mutex);
        iptx = service1->transactionManager.getTransaction(txId, lock);
    }

    iptx->recovered = true;
    iptx->preparedOpCount = 1;

    TestLog::Enable _("handleTimerEvent", "isComplete");
    EXPECT_FALSE(iptx->txHintFailedRpc);
    EXPECT_EQ(100 + iptx->timeoutCycles, iptx->triggerTime);
    iptx->handleTimerEvent();
    {
        TransactionManager::Lock lock(service1->transactionManager.mutex);
        iptx = service1->transactionManager.getTransaction(txId, lock);
    }
    EXPECT_FALSE(iptx == NULL);
    EXPECT_FALSE(iptx->txHintFailedRpc);
    EXPECT_EQ(100 + iptx->timeoutCycles, iptx->triggerTime);
    EXPECT_EQ("handleTimerEvent: TxID <42,9> sending TxHintFailed RPC to owner "
                    "of tableId 1 and keyHash 2. | "
              "handleTimerEvent: TxID <42,9> received ack for TxHintFailed "
                    "RPC; will wait for next timeout.",
              TestLog::get());

    iptx->preparedOpCount = 0;

    TestLog::reset();

    iptx->handleTimerEvent();

    EXPECT_EQ("isComplete: TxID <42,9> has completed; "
              "Recovered with all prepared operations decided.",
              TestLog::get());
    EXPECT_EQ(1U, service1->transactionManager.cleaner.cleaningQueue.size());
    EXPECT_EQ(txId, service1->transactionManager.cleaner.cleaningQueue.front());

    // Stop iptx before it is allowed to actually run.
    iptx->stop();
}

TEST_F(InProgressTransactionTest, handleTimerEvent_error_no_participantList) {
    TransactionManager::InProgressTransaction* iptx;

    {
        TransactionManager::Lock lock(service1->transactionManager.mutex);
        iptx = service1->transactionManager.getTransaction(txId, lock);
    }

    iptx->participantListLogRef = AbstractLog::Reference();

    TestLog::Enable _("handleTimerEvent");
    EXPECT_FALSE(iptx->txHintFailedRpc);
    EXPECT_EQ(100 + iptx->timeoutCycles, iptx->triggerTime);
    iptx->handleTimerEvent();
    iptx->stop();
    EXPECT_FALSE(iptx->txHintFailedRpc);
    EXPECT_EQ(100 + iptx->timeoutCycles, iptx->triggerTime);
    EXPECT_EQ("handleTimerEvent: Unable to initiate transaction recovery for "
            "TxId (42, 9) because participant list record could not be found; "
            "BUG; transaction timeout timer may have started without first "
            "being registered.",
            TestLog::get());
}

TEST_F(InProgressTransactionTest, handleTimerEvent_waiting) {
    TransactionManager::InProgressTransaction* iptx;

    {
        TransactionManager::Lock lock(service1->transactionManager.mutex);
        TransactionId txId(42, 9);
        iptx = service1->transactionManager.getTransaction(txId, lock);
    }

    iptx->stop();

    Buffer pListBuf;
    service1->transactionManager.log->getEntry(iptx->participantListLogRef,
                                               pListBuf);
    ParticipantList participantList(pListBuf);
    iptx->txHintFailedRpc.construct(iptx->manager->context,
                                    participantList.getTableId(),
                                    participantList.getKeyHash(),
                                    iptx->txId.clientLeaseId,
                                    iptx->txId.clientTransactionId,
                                    participantList.getParticipantCount(),
                                    participantList.participants);
    iptx->txHintFailedRpc->state = RpcWrapper::IN_PROGRESS;

    TestLog::Enable _("handleTimerEvent");
    EXPECT_TRUE(iptx->txHintFailedRpc);
    EXPECT_EQ(100 + iptx->timeoutCycles, iptx->triggerTime);
    iptx->handleTimerEvent();
    iptx->stop();
    EXPECT_TRUE(iptx->txHintFailedRpc);
    EXPECT_EQ(0U, iptx->triggerTime);
    EXPECT_EQ("handleTimerEvent: TxID <42,9> waiting for TxHintFailed RPC ack.",
              TestLog::get());
}

TEST_F(InProgressTransactionTest, isComplete) {
    TestLog::Enable _("isComplete");
    TransactionManager::InProgressTransaction* iptx;
    TransactionManager::Lock lock(service1->transactionManager.mutex);
    iptx = service1->transactionManager.getTransaction(txId, lock);

    iptx->preparedOpCount = 1;

    TestLog::reset();
    EXPECT_FALSE(iptx->isComplete(lock));
    EXPECT_EQ("", TestLog::get());

    iptx->preparedOpCount = 0;

    TestLog::reset();
    EXPECT_FALSE(iptx->isComplete(lock));
    EXPECT_EQ("", TestLog::get());

    iptx->recovered = true;

    TestLog::reset();
    EXPECT_TRUE(iptx->isComplete(lock));
    EXPECT_EQ("isComplete: TxID <42,9> has completed; Recovered with all "
            "prepared operations decided.", TestLog::get());

    iptx->recovered = false;
    {
        UnackedRpcResults::Lock lock(service1->unackedRpcResults.mutex);
        UnackedRpcResults::Client* client =
                service1->unackedRpcResults.getOrInitClientRecord(42, lock);
        client->maxAckId = 12;
    }
    EXPECT_TRUE(service1->unackedRpcResults.isRpcAcked(42, 9));

    TestLog::reset();
    EXPECT_TRUE(iptx->isComplete(lock));
    EXPECT_EQ("isComplete: TxID <42,9> has completed; Acked by Client.",
            TestLog::get());
}

TEST_F(TransactionManagerTest, TransactionRegistryCleaner_handleTimerEvent) {
    {
        TransactionManager::Lock lock(transactionManager.mutex);
        {
            TransactionId txId(42, 1);
            transactionManager.getOrAddTransaction(txId, lock);
            transactionManager.getTransaction(txId, lock)->recovered = true;
            transactionManager.cleaner.cleaningQueue.push(txId);
        }
        {
            TransactionId txId(42, 2);
            transactionManager.getOrAddTransaction(txId, lock);
            transactionManager.getTransaction(txId, lock)->recovered = true;
            // Don't clean this one.
        }
        {
            TransactionId txId(42, 3);
            transactionManager.getOrAddTransaction(txId, lock);
            // This one's not complete
            transactionManager.cleaner.cleaningQueue.push(txId);
        }
        {
            TransactionId txId(42, 4);
            transactionManager.getOrAddTransaction(txId, lock);
            // Disable cleaning on this on.
            transactionManager.getTransaction(txId, lock)->cleaningDisabled = 1;
            transactionManager.cleaner.cleaningQueue.push(txId);
        }
        {
            TransactionId txId(43, 1);
            transactionManager.getOrAddTransaction(txId, lock);
            transactionManager.getTransaction(txId, lock)->recovered = true;
            transactionManager.cleaner.cleaningQueue.push(txId);
        }
    }

    EXPECT_EQ(4U, transactionManager.cleaner.cleaningQueue.size());
    EXPECT_EQ(6U, transactionManager.transactions.size());
    TestLog::Enable _("~InProgressTransaction", "handleTimerEvent", NULL);

    transactionManager.cleaner.handleTimerEvent();

    EXPECT_EQ(0U, transactionManager.cleaner.cleaningQueue.size());
    EXPECT_EQ(4U, transactionManager.transactions.size());
    EXPECT_EQ(
            "~InProgressTransaction: InProgressTransaction <42, 1> destroyed | "
            "handleTimerEvent: Cleaning disabled for TxId: <42,4> | "
            "~InProgressTransaction: InProgressTransaction <43, 1> destroyed",
            TestLog::get());
    {
        TransactionManager::Lock lock(transactionManager.mutex);
        TransactionId txId(42, 2);
        EXPECT_TRUE(transactionManager.getTransaction(txId, lock) != NULL);
    }
    {
        TransactionManager::Lock lock(transactionManager.mutex);
        TransactionId txId(42, 3);
        EXPECT_TRUE(transactionManager.getTransaction(txId, lock) != NULL);
    }
    {
        TransactionManager::Lock lock(transactionManager.mutex);
        TransactionId txId(42, 4);
        EXPECT_TRUE(transactionManager.getTransaction(txId, lock) != NULL);
    }
}

TEST_F(TransactionManagerTest, TransactionRegistryCleaner_queueForCleaning) {
    TransactionManager::Lock lock(transactionManager.mutex);
    TransactionId txId(42, 1);
    transactionManager.getOrAddTransaction(txId, lock);

    EXPECT_FALSE(transactionManager.cleaner.isRunning());
    EXPECT_EQ(0U, transactionManager.cleaner.cleaningQueue.size());

    transactionManager.cleaner.queueForCleaning(txId, lock);

    EXPECT_TRUE(transactionManager.cleaner.isRunning());
    EXPECT_EQ(1U, transactionManager.cleaner.cleaningQueue.size());
    EXPECT_EQ(txId, transactionManager.cleaner.cleaningQueue.front());
}

TEST_F(TransactionManagerTest, PreparedItem_constructor_destructor) {
    TransactionManager::Lock lock(transactionManager.mutex);
    TransactionId txId(42, 1);
    TransactionManager::InProgressTransaction* transaction =
            transactionManager.getOrAddTransaction(txId, lock);

    EXPECT_EQ(0, transaction->preparedOpCount);
    TransactionManager::PreparedItem* item =
            new TransactionManager::PreparedItem(transaction, 0);
    EXPECT_EQ(1, transaction->preparedOpCount);
    {
        TransactionManager::PreparedItem otherItem(transaction, 0);
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
                                                          txId,
                                                          lock);
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

} // namespace RAMCloud
