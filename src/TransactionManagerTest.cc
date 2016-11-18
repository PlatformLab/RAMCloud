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

#include <vector>

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
        , unackedRpcResults(&context,
                            NULL,
                            &clientLeaseValidator,
                            &tabletManager)
        , transactionManager(&context,
                             objectManager.getLog(),
                             &unackedRpcResults,
                             &tabletManager)
        , txRecoveryManager(&context)
        , tabletManager()
    {
        context.dispatch = new Dispatch(false);
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
 * Used for TransactionRecord unit tests.
 */
class TransactionRecordTest : public ::testing::Test {
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
    TransactionManager::TransactionRecord* transaction;

    TransactionRecordTest()
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
        , transaction(NULL)
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

        // Setup Registered Transactions
        WireFormat::TxParticipant participants[3];
        participants[0] = WireFormat::TxParticipant(tableId1, 2, 10);
        participants[1] = WireFormat::TxParticipant(tableId2, 234, 11);
        participants[2] = WireFormat::TxParticipant(tableId3, 222, 12);
        ParticipantList participantList(participants, 3, 42, 9);
        Buffer buffer;
        participantList.assembleForLog(buffer);

        Cycles::mockTscValue = 100;

        service1->transactionManager.registerTransaction(
                participantList,
                buffer,
                service1->objectManager.getLog());
        {
            TransactionManager::Lock lock(service1->transactionManager.mutex);
            transaction = service1->transactionManager.getTransaction(txId,
                                                                      lock);
        }
    }

    ~TransactionRecordTest()
    {
        Cycles::mockTscValue = 0;
        WorkerTimer::disableTimerHandlers = false;
    }

    DISALLOW_COPY_AND_ASSIGN(TransactionRecordTest);
};

TEST_F(TransactionManagerTest, startCleaner) {
    EXPECT_FALSE(transactionManager.cleaner.isRunning());
    transactionManager.startCleaner();
    EXPECT_TRUE(transactionManager.cleaner.isRunning());
    transactionManager.cleaner.stop();
}

TEST_F(TransactionManagerTest, registerTransaction_basic) {
    TransactionId txId(42, 9);
    TransactionManager::TransactionRecord *transaction;
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
        transaction = transactionManager.getTransaction(txId, lock);
        EXPECT_TRUE(transaction != NULL);
    }

    Buffer outBuffer;
    LogEntryType type = objectManager.log.getEntry(
            transaction->participantListLogRef, outBuffer);
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

    EXPECT_EQ(3 * Cycles::fromMicroseconds(50000), transaction->timeoutCycles);
    EXPECT_EQ(transaction->timeoutCycles + 100, transaction->triggerTime);
    EXPECT_TRUE(transaction->isRunning());

    // Stop transaction before it is allowed to actually run.
    transaction->stop();
}

TEST_F(TransactionManagerTest, registerTransaction_duplicate) {
    TransactionId txId(42, 9);
    TransactionManager::TransactionRecord *transaction;
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
        transaction = transactionManager.getOrAddTransaction(txId, lock);
        transaction->participantListLogRef = AbstractLog::Reference(100);
        transaction->timeoutCycles = 200;
        transaction->start(300);
    }

    EXPECT_TRUE(transaction->participantListLogRef
                == AbstractLog::Reference(100));
    EXPECT_EQ(200UL, transaction->timeoutCycles);
    EXPECT_EQ(300UL, transaction->triggerTime);
    EXPECT_TRUE(transaction->isRunning());

    TestLog::Enable _;
    ParticipantList participantListDuplicate(participants, 1, 42, 9);
    participantListDuplicate.assembleForLog(inBuffer);

    transactionManager.registerTransaction(participantListDuplicate,
                                           inBuffer,
                                           objectManager.getLog());
    EXPECT_EQ("registerTransaction: Skipping duplicate call to register "
              "transaction <42, 9>",
              TestLog::get());
    EXPECT_TRUE(transaction->participantListLogRef
                == AbstractLog::Reference(100));
    EXPECT_EQ(200UL, transaction->timeoutCycles);
    EXPECT_EQ(300UL, transaction->triggerTime);
    EXPECT_TRUE(transaction->isRunning());

    // Reset transaction state to avoid unintended behavior during destruction
    transaction->participantListLogRef = AbstractLog::Reference();
    transaction->stop();
}

TEST_F(TransactionManagerTest, registerTransaction_timerStart) {
    TransactionId txId(42, 9);
    TransactionManager::TransactionRecord *transaction;
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
        transaction = transactionManager.getTransaction(txId, lock);
    }

    EXPECT_EQ(100 + transaction->timeoutCycles, transaction->triggerTime);
    EXPECT_TRUE(transaction->isRunning());

    Cycles::mockTscValue = 200;

    transactionManager.registerTransaction(participantList,
                                           inBuffer,
                                           objectManager.getLog());

    EXPECT_EQ(200 + transaction->timeoutCycles, transaction->triggerTime);
    EXPECT_TRUE(transaction->isRunning());

    // Fake the existance of an async RPC.
    transaction->txHintFailedRpc.occupied = true;

    Cycles::mockTscValue = 300;

    transactionManager.registerTransaction(participantList,
                                           inBuffer,
                                           objectManager.getLog());

    EXPECT_EQ(200 + transaction->timeoutCycles, transaction->triggerTime);
    EXPECT_TRUE(transaction->isRunning());

    // Reset transaction to avoid unexpected behavior.
    transaction->stop();
    transaction->txHintFailedRpc.occupied = false;
}

TEST_F(TransactionManagerTest, markTransactionRecovered) {
    TransactionId txId(42, 9);
    TransactionManager::TransactionRecord *transaction;

    transactionManager.markTransactionRecovered(txId);

    {
        TransactionManager::Lock lock(transactionManager.mutex);
        EXPECT_TRUE(transactionManager.getTransaction(txId, lock) == NULL);
    }

    {
        TransactionManager::Lock lock(transactionManager.mutex);
        transaction = transactionManager.getOrAddTransaction(txId, lock);
    }

    {
        TransactionManager::Lock lock(transactionManager.mutex);
        EXPECT_TRUE(transactionManager.getTransaction(txId, lock)
                    == transaction);
    }

    EXPECT_FALSE(transaction->recovered);

    transactionManager.markTransactionRecovered(txId);

    EXPECT_TRUE(transaction->recovered);
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

    TransactionManager::TransactionRecord* transaction;
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

    TransactionManager::TransactionRecord* transaction;
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
    transactionManager.bufferOp(TransactionId(1, 1), 10, 1011);
    EXPECT_EQ(1011UL,
              transactionManager.items[std::make_pair(1, 10)]->newOpPtr);
    transactionManager.removeOp(1, 10);
    EXPECT_EQ(transactionManager.items.end(),
              transactionManager.items.find(std::make_pair(1, 10)));
}

TEST_F(TransactionManagerTest, getOp) {
    transactionManager.bufferOp(TransactionId(1, 1), 10, 1011);
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

TEST_F(TransactionManagerTest, removeOrphanedOps) {
    TransactionId txId(1, 10);
    // Will be removed
    {
        Buffer dataBuffer;
        Key key(1, "1", 1);
        uint64_t rpcId = 11;
        PreparedOp op(WireFormat::TxPrepare::WRITE,
                      txId.clientLeaseId, txId.clientTransactionId, rpcId,
                      key, "hello", 6, 0, 0, dataBuffer);
        Buffer buffer;
        op.assembleForLog(buffer);
        AbstractLog::Reference ref;
        transactionManager.log->append(LOG_ENTRY_TYPE_PREP, buffer, &ref);
        transactionManager.bufferOp(txId, rpcId, ref.toInteger());
    }
    // Don't remove, master owns tablet
    {
        Buffer dataBuffer;
        Key key(2, "2", 1);
        uint64_t rpcId = 12;
        PreparedOp op(WireFormat::TxPrepare::WRITE,
                      txId.clientLeaseId, txId.clientTransactionId, rpcId,
                      key, "hello", 6, 0, 0, dataBuffer);
        Buffer buffer;
        op.assembleForLog(buffer);
        AbstractLog::Reference ref;
        transactionManager.log->append(LOG_ENTRY_TYPE_PREP, buffer, &ref);
        transactionManager.bufferOp(txId, rpcId, ref.toInteger());
        tabletManager.addTablet(2, 0ul, ~0ul, TabletManager::NORMAL);
    }
    // Don't remove, op marked deleted
    {
        Buffer dataBuffer;
        Key key(3, "3", 1);
        uint64_t rpcId = 13;
        transactionManager.markOpDeleted(txId.clientLeaseId, rpcId);
    }
    EXPECT_EQ(3U, transactionManager.items.size());

    transactionManager.removeOrphanedOps();

    EXPECT_EQ(2U, transactionManager.items.size());
    EXPECT_TRUE(transactionManager.items.end() ==
                transactionManager.items.find(std::make_pair(txId.clientLeaseId,
                                                             11)));
    EXPECT_TRUE(transactionManager.items.end() !=
                transactionManager.items.find(std::make_pair(txId.clientLeaseId,
                                                             12)));
    EXPECT_TRUE(transactionManager.items.end() !=
                transactionManager.items.find(std::make_pair(txId.clientLeaseId,
                                                             13)));
}

TEST_F(TransactionManagerTest, Protector) {
    TransactionId txId(42, 21);
    TransactionId otherTxId(2, 1);
    TransactionManager::Protector* p2;
    TransactionManager::TransactionRecord* transaction = NULL;
    {
        TransactionManager::Lock lock(transactionManager.mutex);
        transaction = transactionManager.getTransaction(txId, lock);
    }

    EXPECT_TRUE(transaction == NULL);
    {
        TransactionManager::Protector p0(&transactionManager, txId);
        // p0 is live
        {
            TransactionManager::Lock lock(transactionManager.mutex);
            transaction = transactionManager.getTransaction(txId, lock);
        }
        EXPECT_TRUE(transaction != NULL);
        EXPECT_EQ(1, transaction->cleaningDisabled);
        {
            TransactionManager::Protector p1(&transactionManager, txId);
            // p0, p1 is live
            EXPECT_EQ(2, transaction->cleaningDisabled);
            p2 = new TransactionManager::Protector(&transactionManager, txId);
            // p0, p1, p2 is live
            EXPECT_EQ(3, transaction->cleaningDisabled);
            {
                TransactionManager::Protector _(&transactionManager, otherTxId);
                EXPECT_EQ(3, transaction->cleaningDisabled);
            }
        }
        // p0, p2 is live
        EXPECT_EQ(2, transaction->cleaningDisabled);
        delete p2;
        // p0 is live
        EXPECT_EQ(1, transaction->cleaningDisabled);
    }
    // Nothing is live
    EXPECT_EQ(0, transaction->cleaningDisabled);
}

TEST_F(TransactionManagerTest, TransactionRecord_destructor) {
    TransactionManager::Lock lock(transactionManager.mutex);

    TestLog::Enable _("free");
    {
        TransactionManager::TransactionRecord transaction(
                &transactionManager, TransactionId(42, 31), lock);
    }
    EXPECT_EQ("", TestLog::get());

    TestLog::reset();

    uint64_t original = transactionManager.log->totalLiveBytes;
    Log::Reference logRef;
    {
        TransactionManager::TransactionRecord transaction(
                &transactionManager, TransactionId(42, 31), lock);
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
        transaction.participantListLogRef = logRef;
        EXPECT_LT(original, transactionManager.log->totalLiveBytes);
    }
    EXPECT_EQ(original, transactionManager.log->totalLiveBytes);
    EXPECT_EQ(format("free: free on reference %" PRIu64, logRef.toInteger()),
              TestLog::get());
}

TEST_F(TransactionRecordTest, handleTimerEvent_basic) {
    TestLog::Enable _("handleTimerEvent");
    EXPECT_FALSE(transaction->txHintFailedRpc);
    EXPECT_EQ(100 + transaction->timeoutCycles, transaction->triggerTime);
    transaction->handleTimerEvent();
    transaction->stop();
    EXPECT_FALSE(transaction->txHintFailedRpc);
    EXPECT_EQ(100 + transaction->timeoutCycles, transaction->triggerTime);
    EXPECT_EQ("handleTimerEvent: TxID <42,9> sending TxHintFailed RPC to owner "
                    "of tableId 1 and keyHash 2. | "
              "handleTimerEvent: TxID <42,9> received ack for TxHintFailed "
                    "RPC; will wait for next timeout.",
              TestLog::get());
}

TEST_F(TransactionRecordTest, handleTimerEvent_error_no_participantList) {
    transaction->participantListLogRef = AbstractLog::Reference();
    transaction->preparedOpCount = 1;

    TestLog::Enable _("handleTimerEvent");
    EXPECT_FALSE(transaction->txHintFailedRpc);
    EXPECT_EQ(100 + transaction->timeoutCycles, transaction->triggerTime);
    transaction->handleTimerEvent();
    transaction->stop();
    EXPECT_FALSE(transaction->txHintFailedRpc);
    EXPECT_EQ(100 + transaction->timeoutCycles, transaction->triggerTime);
    EXPECT_EQ("handleTimerEvent: Unable to initiate transaction recovery for "
            "TxId (42, 9) because participant list record could not be found; "
            "BUG; transaction timeout timer may have started without first "
            "being registered.",
            TestLog::get());
}

TEST_F(TransactionRecordTest, handleTimerEvent_waiting) {
    transaction->stop();

    Buffer pListBuf;
    service1->transactionManager.log->getEntry(
            transaction->participantListLogRef, pListBuf);
    ParticipantList participantList(pListBuf);
    transaction->txHintFailedRpc.construct(
            transaction->transactionManager->context,
            participantList.getTableId(),
            participantList.getKeyHash(),
            transaction->txId.clientLeaseId,
            transaction->txId.clientTransactionId,
            participantList.getParticipantCount(),
            participantList.participants);
    transaction->txHintFailedRpc->state = RpcWrapper::IN_PROGRESS;

    TestLog::Enable _("handleTimerEvent");
    EXPECT_TRUE(transaction->txHintFailedRpc);
    EXPECT_EQ(100 + transaction->timeoutCycles, transaction->triggerTime);
    transaction->handleTimerEvent();
    transaction->stop();
    EXPECT_TRUE(transaction->txHintFailedRpc);
    EXPECT_EQ(0U, transaction->triggerTime);
    EXPECT_EQ("handleTimerEvent: TxID <42,9> waiting for TxHintFailed RPC ack.",
              TestLog::get());
}

TEST_F(TransactionRecordTest, inProgress_acked) {
    TestLog::Enable _("inProgress");
    TransactionManager::Lock lock(service1->transactionManager.mutex);
    TabletManager::Protector protector(
            service1->transactionManager.tabletManager);

    {
        UnackedRpcResults::Lock lock(service1->unackedRpcResults.mutex);
        UnackedRpcResults::Client* client =
                service1->unackedRpcResults.getOrInitClientRecord(42, lock);
        client->maxAckId = 12;
    }
    EXPECT_TRUE(service1->unackedRpcResults.isRpcAcked(42, 9));

    EXPECT_FALSE(transaction->inProgress(lock, protector));
    EXPECT_EQ("inProgress: TxID <42,9> has completed; Acked by Client.",
              TestLog::get());
}

TEST_F(TransactionRecordTest, inProgress_recovered) {
    TestLog::Enable _("inProgress");
    TransactionManager::Lock lock(service1->transactionManager.mutex);
    TabletManager::Protector protector(
            service1->transactionManager.tabletManager);

    transaction->recovered = true;

    EXPECT_FALSE(transaction->inProgress(lock, protector));
    EXPECT_EQ("inProgress: TxID <42,9> has completed; Recovered with all "
              "prepared operations decided.", TestLog::get());
}

TEST_F(TransactionRecordTest, inProgress_noParticipantList) {
    TestLog::Enable _("inProgress");
    TransactionManager::Lock lock(service1->transactionManager.mutex);
    TabletManager::Protector protector(
            service1->transactionManager.tabletManager);

    transaction->participantListLogRef = AbstractLog::Reference();

    EXPECT_FALSE(transaction->inProgress(lock, protector));
    EXPECT_EQ("inProgress: TxID <42,9> has no participant list; must not have"
              "registered.", TestLog::get());
}

TEST_F(TransactionRecordTest, inProgress_noLongerAParticipant) {
    TestLog::Enable _("inProgress");
    ramcloud->dropTable("table1");

    TransactionManager::Lock lock(service1->transactionManager.mutex);
    TabletManager::Protector protector(
            service1->transactionManager.tabletManager);

    EXPECT_FALSE(transaction->inProgress(lock, protector));
    EXPECT_EQ("inProgress: TxID <42,9> does not belong to this master.",
              TestLog::get());
}

TEST_F(TransactionRecordTest, inProgress_stillInProgress) {
    TestLog::Enable _("inProgress");
    TransactionManager::Lock lock(service1->transactionManager.mutex);
    TabletManager::Protector protector(
            service1->transactionManager.tabletManager);

    EXPECT_TRUE(transaction->inProgress(lock, protector));
    EXPECT_EQ("", TestLog::get());
}

TEST_F(TransactionRecordTest, inProgress_positivePreparedOpCount) {
    TestLog::Enable _("inProgress");
    TransactionManager::Lock lock(service1->transactionManager.mutex);
    TabletManager::Protector protector(
            service1->transactionManager.tabletManager);

    transaction->preparedOpCount = 1;

    EXPECT_TRUE(transaction->inProgress(lock, protector));
    EXPECT_EQ("", TestLog::get());
}

TEST_F(TransactionRecordTest, inProgress_checkMasterParticipantion) {
    TestLog::Enable _("checkMasterParticipantion");
    {
        TransactionManager::Lock lock(service1->transactionManager.mutex);
        TabletManager::Protector protector(
                service1->transactionManager.tabletManager);
        EXPECT_TRUE(transaction->checkMasterParticipantion(lock, protector));
        EXPECT_EQ("checkMasterParticipantion: "
                  "Found tablet for tableId: 1 keyHash: 2", TestLog::get());
    }

    TestLog::reset();
    ramcloud->dropTable("table1");

    {
        TransactionManager::Lock lock(service1->transactionManager.mutex);
        TabletManager::Protector protector(
                service1->transactionManager.tabletManager);
        EXPECT_FALSE(transaction->checkMasterParticipantion(lock, protector));
        EXPECT_EQ("", TestLog::get());
    }
}

TEST_F(TransactionManagerTest, TransactionRegistryCleaner_handleTimerEvent) {
    {
        TransactionManager::Lock lock(transactionManager.mutex);
        {
            TransactionId txId(42, 1);
            transactionManager.getOrAddTransaction(txId, lock);
            transactionManager.getTransaction(txId, lock)->recovered = true;
            transactionManager.cleaner.stop();
        }
        {
            TransactionId txId(42, 2);
            transactionManager.getOrAddTransaction(txId, lock);
            transactionManager.getTransaction(txId, lock)->recovered = true;
            // Disable cleaning on this on.
            transactionManager.getTransaction(txId, lock)->cleaningDisabled = 1;
            transactionManager.cleaner.stop();
        }
        {
            TransactionId txId(42, 3);
            transactionManager.getOrAddTransaction(txId, lock);
            // This one's not complete
            transactionManager.getTransaction(txId, lock)->preparedOpCount = 1;
            transactionManager.cleaner.stop();
        }
        {
            TransactionId txId(43, 1);
            transactionManager.getOrAddTransaction(txId, lock);
            transactionManager.getTransaction(txId, lock)->recovered = true;
            transactionManager.cleaner.stop();
        }
    }

    EXPECT_EQ(4U, transactionManager.transactions.size());
    EXPECT_EQ(4U, transactionManager.transactionIds.size());
    EXPECT_FALSE(transactionManager.cleaner.isRunning());
    TestLog::Enable _("~TransactionRecord", "handleTimerEvent", NULL);

    tabletManager.addTablet(42, 0, 1, TabletManager::NOT_READY);

    // Cleaner blocked by recovering tablet
    transactionManager.cleaner.handleTimerEvent();

    EXPECT_EQ(4U, transactionManager.transactions.size());
    EXPECT_EQ(4U, transactionManager.transactionIds.size());
    EXPECT_TRUE(transactionManager.cleaner.isRunning());
    EXPECT_EQ("", TestLog::get());
    transactionManager.cleaner.stop();
    tabletManager.deleteTablet(42, 0, 1);

    // Cleaner will run.
    transactionManager.cleaner.handleTimerEvent();

    EXPECT_EQ(2U, transactionManager.transactions.size());
    EXPECT_EQ(2U, transactionManager.transactionIds.size());
    EXPECT_TRUE(transactionManager.cleaner.isRunning());
    EXPECT_EQ(
            "~TransactionRecord: TransactionRecord <42, 1> destroyed | "
            "handleTimerEvent: Cleaning disabled for TxId: <42, 2> | "
            "~TransactionRecord: TransactionRecord <43, 1> destroyed",
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

    transactionManager.cleaner.stop();
}

TEST_F(TransactionManagerTest, PreparedItem_constructor_destructor) {
    TransactionManager::Lock lock(transactionManager.mutex);
    TransactionId txId(42, 1);
    TransactionManager::TransactionRecord* transaction =
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
    TransactionManager::TransactionRecord* transaction =
            new TransactionManager::TransactionRecord(&transactionManager,
                                                          txId,
                                                          lock);
    transactionManager.transactions[txId] = transaction;
    EXPECT_TRUE(transactionManager.getTransaction(txId, lock) == transaction);
}

TEST_F(TransactionManagerTest, getOrAddTransaction) {
    TransactionManager::Lock lock(transactionManager.mutex);
    TransactionId txId(42, 1);
    EXPECT_TRUE(transactionManager.getTransaction(txId, lock) == NULL);
    TransactionManager::TransactionRecord* transaction =
            transactionManager.getOrAddTransaction(txId, lock);
    TransactionManager::TransactionRegistry::iterator it =
            transactionManager.transactions.find(txId);
    EXPECT_TRUE(it != transactionManager.transactions.end());
    EXPECT_TRUE(it->second = transaction);
    EXPECT_TRUE(transactionManager.getTransaction(txId, lock) == transaction);
    EXPECT_TRUE(transactionManager.transactionIds.back() == txId);
}

} // namespace RAMCloud
