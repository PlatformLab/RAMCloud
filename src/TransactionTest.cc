/* Copyright (c) 2015 Stanford University
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

#include "TestUtil.h"       //Has to be first, compiler complains
#include "ClientTransactionTask.h"
#include "MockCluster.h"
#include "Transaction.h"
#include "MultiOp.h"

namespace RAMCloud {

class TransactionTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    uint64_t tableId1;
    uint64_t tableId2;
    uint64_t tableId3;
    BindTransport::BindSession* session1;
    BindTransport::BindSession* session2;
    BindTransport::BindSession* session3;
    Tub<Transaction> transaction;
    ClientTransactionTask* task;

    TransactionTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , ramcloud()
        , tableId1(-1)
        , tableId2(-2)
        , tableId3(-3)
        , session1(NULL)
        , session2(NULL)
        , session3(NULL)
        , transaction()
        , task()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master1";
        config.maxObjectKeySize = 512;
        config.maxObjectDataSize = 1024;
        config.segmentSize = 128*1024;
        config.segletSize = 128*1024;
        cluster.addServer(config);
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master2";
        cluster.addServer(config);
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master3";
        cluster.addServer(config);
        ramcloud.construct(&context, "mock:host=coordinator");

        // Get pointers to the master sessions.
        Transport::SessionRef session =
                ramcloud->clientContext->transportManager->getSession(
                "mock:host=master1");
        session1 = static_cast<BindTransport::BindSession*>(session.get());
        session = ramcloud->clientContext->transportManager->getSession(
                "mock:host=master2");
        session2 = static_cast<BindTransport::BindSession*>(session.get());
        session = ramcloud->clientContext->transportManager->getSession(
                "mock:host=master3");
        session3 = static_cast<BindTransport::BindSession*>(session.get());

        transaction.construct(ramcloud.get());
        task = transaction->taskPtr.get();

        // Make some tables.
        tableId1 = ramcloud->createTable("table1");
        tableId2 = ramcloud->createTable("table2");
        tableId3 = ramcloud->createTable("table3");
    }

    DISALLOW_COPY_AND_ASSIGN(TransactionTest);
};

TEST_F(TransactionTest, commit_basic) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Buffer value;
    transaction->read(tableId1, "0", 1, &value);
    transaction->write(tableId1, "0", 1, "hello", 5);

    EXPECT_FALSE(transaction->commitStarted);
    EXPECT_EQ(ClientTransactionTask::INIT,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commit());
    EXPECT_EQ(ClientTransactionTask::DONE,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitStarted);

    // Check that commit does not wait for decision rpcs to return.
    transaction->taskPtr.get()->state = ClientTransactionTask::DECISION;
    EXPECT_TRUE(transaction->commit());
    EXPECT_EQ(ClientTransactionTask::DECISION,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitStarted);
}

TEST_F(TransactionTest, commit_abort) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Buffer value;
    transaction->read(tableId1, "0", 1, &value);
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);
    transaction->write(tableId1, "0", 1, "hello", 5);

    EXPECT_FALSE(transaction->commitStarted);
    EXPECT_EQ(ClientTransactionTask::INIT,
              transaction->taskPtr.get()->state);
    EXPECT_FALSE(transaction->commit());
    EXPECT_EQ(ClientTransactionTask::DONE,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitStarted);

    // Check that commit does not wait for decision rpcs to return.
    transaction->taskPtr.get()->state = ClientTransactionTask::DECISION;
    EXPECT_FALSE(transaction->commit());
    EXPECT_EQ(ClientTransactionTask::DECISION,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitStarted);
}

TEST_F(TransactionTest, commit_internalError) {
    transaction->commit();
    transaction->taskPtr.get()->decision = WireFormat::TxDecision::UNDECIDED;
    EXPECT_THROW(transaction->commit(), InternalError);
}

TEST_F(TransactionTest, sync_basic) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Buffer value;
    transaction->read(tableId1, "0", 1, &value);
    transaction->write(tableId1, "0", 1, "hello", 5);

    EXPECT_FALSE(transaction->commitStarted);
    EXPECT_EQ(ClientTransactionTask::INIT,
              transaction->taskPtr.get()->state);
    transaction->sync();
    EXPECT_EQ(ClientTransactionTask::DONE,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitStarted);
}

TEST_F(TransactionTest, sync_beforeCommit) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Buffer value;
    transaction->read(tableId1, "0", 1, &value);
    transaction->write(tableId1, "0", 1, "hello", 5);

    EXPECT_FALSE(transaction->commitStarted);
    EXPECT_EQ(ClientTransactionTask::INIT,
              transaction->taskPtr.get()->state);
    transaction->sync();
    EXPECT_EQ(ClientTransactionTask::DONE,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitStarted);
    EXPECT_TRUE(transaction->commit());
    EXPECT_EQ(ClientTransactionTask::DONE,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitStarted);
}

TEST_F(TransactionTest, commitAndSync_basic) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Buffer value;
    transaction->read(tableId1, "0", 1, &value);
    transaction->write(tableId1, "0", 1, "hello", 5);

    EXPECT_FALSE(transaction->commitStarted);
    EXPECT_EQ(ClientTransactionTask::INIT,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitAndSync());
    EXPECT_EQ(ClientTransactionTask::DONE,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitStarted);
    EXPECT_TRUE(transaction->commitAndSync());
    EXPECT_EQ(ClientTransactionTask::DONE,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitStarted);
}

TEST_F(TransactionTest, commitAndSync_abort) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Buffer value;
    transaction->read(tableId1, "0", 1, &value);
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);
    transaction->write(tableId1, "0", 1, "hello", 5);

    EXPECT_FALSE(transaction->commitStarted);
    EXPECT_EQ(ClientTransactionTask::INIT,
              transaction->taskPtr.get()->state);
    EXPECT_FALSE(transaction->commitAndSync());
    EXPECT_EQ(ClientTransactionTask::DONE,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitStarted);
    EXPECT_FALSE(transaction->commitAndSync());
    EXPECT_EQ(ClientTransactionTask::DONE,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitStarted);
}

TEST_F(TransactionTest, read_basic) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    EXPECT_TRUE(task->readOnly);
    Key key(tableId1, "0", 1);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    Buffer value;
    transaction->read(tableId1, "0", 1, &value);
    EXPECT_TRUE(task->readOnly);
    EXPECT_EQ("abcdef", string(reinterpret_cast<const char*>(
                        value.getRange(0, value.size())),
                        value.size()));

    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    uint32_t dataLength = 0;
    const char* str;
    str = reinterpret_cast<const char*>(
            entry->objectBuf->getValue(&dataLength));
    EXPECT_EQ("abcdef", string(str, dataLength));
    EXPECT_EQ(ClientTransactionTask::CacheEntry::READ, entry->type);
    EXPECT_EQ(3U, entry->rejectRules.givenVersion);
}

TEST_F(TransactionTest, read_noObject) {
    Key key(tableId1, "0", 1);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    Buffer value;
    EXPECT_THROW(transaction->read(tableId1, "0", 1, &value),
                 ObjectDoesntExistException);

    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    uint32_t dataLength = 0;
    entry->objectBuf->getValue(&dataLength);
    EXPECT_EQ(0U, dataLength);
    EXPECT_EQ(ClientTransactionTask::CacheEntry::READ, entry->type);
    EXPECT_TRUE(entry->rejectRules.exists);

    EXPECT_THROW(transaction->read(tableId1, "0", 1, &value),
                 ObjectDoesntExistException);
}

TEST_F(TransactionTest, read_afterWrite) {
    uint32_t dataLength = 0;
    const char* str;

    Key key(1, "test", 4);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    transaction->write(1, "test", 4, "hello", 5);

    // Make sure the read, reads the last write.
    Buffer value;
    transaction->read(1, "test", 4, &value);
    EXPECT_EQ("hello", string(reinterpret_cast<const char*>(
                        value.getRange(0, value.size())),
                        value.size()));

    // Make sure the operations is still cached as a write.
    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    EXPECT_EQ(ClientTransactionTask::CacheEntry::WRITE, entry->type);
    EXPECT_EQ(0U, entry->rejectRules.givenVersion);
    str = reinterpret_cast<const char*>(
            entry->objectBuf->getValue(&dataLength));
    EXPECT_EQ("hello", string(str, dataLength));
}

TEST_F(TransactionTest, read_afterRemove) {
    Key key(1, "test", 4);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    transaction->remove(1, "test", 4);

    // Make read throws and exception following a remove.
    Buffer value;
    EXPECT_THROW(transaction->read(1, "test", 4, &value),
                 ObjectDoesntExistException);
}

TEST_F(TransactionTest, read_afterCommit) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);
    transaction->commitStarted = true;

    Buffer value;
    EXPECT_THROW(transaction->read(tableId1, "0", 1, &value),
                 TxOpAfterCommit);
}

TEST_F(TransactionTest, remove) {
    EXPECT_TRUE(task->readOnly);
    Key key(1, "test", 4);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    transaction->remove(1, "test", 4);

    EXPECT_FALSE(task->readOnly);
    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    EXPECT_EQ(ClientTransactionTask::CacheEntry::REMOVE, entry->type);
    EXPECT_EQ(0U, entry->rejectRules.givenVersion);

    transaction->write(1, "test", 4, "goodbye", 7);
    entry->rejectRules.givenVersion = 42;

    transaction->remove(1, "test", 4);

    EXPECT_EQ(ClientTransactionTask::CacheEntry::REMOVE, entry->type);
    EXPECT_EQ(42U, entry->rejectRules.givenVersion);

    EXPECT_EQ(entry, task->findCacheEntry(key));
}

TEST_F(TransactionTest, remove_afterCommit) {
    transaction->commitStarted = true;
    EXPECT_THROW(transaction->remove(1, "test", 4),
                 TxOpAfterCommit);
}

TEST_F(TransactionTest, write) {
    uint32_t dataLength = 0;
    const char* str;

    EXPECT_TRUE(task->readOnly);
    Key key(1, "test", 4);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    transaction->write(1, "test", 4, "hello", 5);

    EXPECT_FALSE(task->readOnly);
    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    EXPECT_EQ(ClientTransactionTask::CacheEntry::WRITE, entry->type);
    EXPECT_EQ(0U, entry->rejectRules.givenVersion);
    str = reinterpret_cast<const char*>(
            entry->objectBuf->getValue(&dataLength));
    EXPECT_EQ("hello", string(str, dataLength));

    entry->type = ClientTransactionTask::CacheEntry::INVALID;
    entry->rejectRules.givenVersion = 42;

    transaction->write(1, "test", 4, "goodbye", 7);

    EXPECT_EQ(ClientTransactionTask::CacheEntry::WRITE, entry->type);
    EXPECT_EQ(42U, entry->rejectRules.givenVersion);
    str = reinterpret_cast<const char*>(
            entry->objectBuf->getValue(&dataLength));
    EXPECT_EQ("goodbye", string(str, dataLength));

    EXPECT_EQ(entry, task->findCacheEntry(key));
}

TEST_F(TransactionTest, write_afterCommit) {
    transaction->commitStarted = true;
    EXPECT_THROW(transaction->write(1, "test", 4, "hello", 5),
                 TxOpAfterCommit);
}

TEST_F(TransactionTest, ReadOp_constructor_noCache) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Key key(tableId1, "0", 1);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    {   // Single Op
        Buffer value;
        Transaction::ReadOp readOp(transaction.get(), tableId1, "0", 1, &value);
        EXPECT_TRUE(readOp.singleRequest->readRpc);
        EXPECT_FALSE(readOp.batchedRequest);
    }

    {   // Batched Op
        Buffer value;
        Transaction::ReadOp
                readOp(transaction.get(), tableId1, "0", 1, &value, true);
        EXPECT_FALSE(readOp.singleRequest);
        EXPECT_TRUE(readOp.batchedRequest->readBatchPtr);
        EXPECT_EQ(1U,
                  readOp.batchedRequest->readBatchPtr->requests.size());
    }
}

TEST_F(TransactionTest, ReadOp_constructor_cached) {
    Key key(1, "test", 4);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    transaction->write(1, "test", 4, "hello", 5);

    {   // Single Op
        Buffer value;
        Transaction::ReadOp readOp(transaction.get(), 1, "test", 4, &value);
        EXPECT_FALSE(readOp.singleRequest->readRpc);
        EXPECT_FALSE(readOp.batchedRequest);
    }

    {   // Batched Op
        Buffer value;
        Transaction::ReadOp
                readOp(transaction.get(), 1, "test", 4, &value, true);
        EXPECT_FALSE(readOp.singleRequest);
        EXPECT_FALSE(readOp.batchedRequest->readBatchPtr);
    }
}

TEST_F(TransactionTest, ReadOp_isReady_single) {
    Buffer value;
    Transaction::ReadOp readOp(transaction.get(), tableId1, "0", 1, &value);
    EXPECT_TRUE(readOp.singleRequest->readRpc);
    EXPECT_TRUE(readOp.singleRequest->readRpc->isReady());
    EXPECT_TRUE(readOp.isReady());

    readOp.singleRequest->readRpc->state = RpcWrapper::IN_PROGRESS;

    EXPECT_TRUE(readOp.singleRequest->readRpc);
    EXPECT_FALSE(readOp.singleRequest->readRpc->isReady());
    EXPECT_FALSE(readOp.isReady());

    readOp.singleRequest->readRpc.destroy();

    EXPECT_FALSE(readOp.singleRequest->readRpc);
    EXPECT_TRUE(readOp.isReady());
}

TEST_F(TransactionTest, ReadOp_isReady_batched) {
    Buffer value;
    Transaction::ReadOp
            readOp(transaction.get(), tableId1, "0", 1, &value, true);

    // Filler requests to delay progress.
    Transaction::ReadOp
            temp1(transaction.get(), tableId2, "0", 1, &value, true);
    Transaction::ReadOp
            temp2(transaction.get(), tableId3, "0", 1, &value, true);
    Transaction::ReadOp
            temp3(transaction.get(), tableId3, "0", 1, &value, true);
    Transaction::ReadOp
            temp4(transaction.get(), tableId3, "0", 1, &value, true);
    Transaction::ReadOp
            temp5(transaction.get(), tableId3, "0", 1, &value, true);
    Transaction::ReadOp
            temp6(transaction.get(), tableId3, "0", 1, &value, true);
    Transaction::ReadOp
            temp7(transaction.get(), tableId3, "0", 1, &value, true);
    Transaction::ReadOp
            temp8(transaction.get(), tableId3, "0", 1, &value, true);

    EXPECT_TRUE(transaction->nextReadBatchPtr);
    EXPECT_TRUE(readOp.batchedRequest->readBatchPtr
                == transaction->nextReadBatchPtr);
    EXPECT_FALSE(readOp.batchedRequest->readBatchPtr->rpc);
    EXPECT_EQ(9U,
              readOp.batchedRequest->readBatchPtr->requests.size());

    EXPECT_FALSE(readOp.isReady());     // rpc has not been issued.

    EXPECT_TRUE(readOp.batchedRequest->readBatchPtr->rpc);
    EXPECT_FALSE(transaction->nextReadBatchPtr);

    EXPECT_FALSE(readOp.isReady());     // rpc should be in progress

    EXPECT_TRUE(readOp.isReady());      // rpc is complete

    readOp.batchedRequest->readBatchPtr.reset();
    EXPECT_FALSE(readOp.batchedRequest->readBatchPtr);

    EXPECT_TRUE(readOp.isReady());      // Mock no rpc sent implies cached.
}

TEST_F(TransactionTest, ReadOp_wait_async) {
    uint32_t dataLength = 0;
    const char* str;

    // Makes sure that the point of the read is when wait is called.
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Key key(tableId1, "0", 1);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    Buffer value;
    Transaction::ReadOp readOp(transaction.get(), tableId1, "0", 1, &value);
    EXPECT_TRUE(readOp.singleRequest->readRpc);

    transaction->write(tableId1, "0", 1, "hello", 5);

    readOp.wait();
    EXPECT_EQ("hello", string(reinterpret_cast<const char*>(
                                value.getRange(0, value.size())),
                                value.size()));

    // Make sure the operations is still cached as a write.
    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    EXPECT_EQ(ClientTransactionTask::CacheEntry::WRITE, entry->type);
    EXPECT_EQ(0U, entry->rejectRules.givenVersion);
    str = reinterpret_cast<const char*>(
            entry->objectBuf->getValue(&dataLength));
    EXPECT_EQ("hello", string(str, dataLength));
}

TEST_F(TransactionTest, ReadOp_wait_batch_basic) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Key key(tableId1, "0", 1);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    Buffer value;
    Transaction::ReadOp
            readOp(transaction.get(), tableId1, "0", 1, &value, true);
    readOp.wait();

    EXPECT_EQ("abcdef", string(reinterpret_cast<const char*>(
                        value.getRange(0, value.size())),
                        value.size()));

    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    uint32_t dataLength = 0;
    const char* str;
    str = reinterpret_cast<const char*>(
            entry->objectBuf->getValue(&dataLength));
    EXPECT_EQ("abcdef", string(str, dataLength));
    EXPECT_EQ(ClientTransactionTask::CacheEntry::READ, entry->type);
    EXPECT_EQ(3U, entry->rejectRules.givenVersion);
}

TEST_F(TransactionTest, ReadOp_wait_batch_noObject) {
    Key key(tableId1, "0", 1);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    Buffer value;
    Transaction::ReadOp
            readOp(transaction.get(), tableId1, "0", 1, &value, true);
    EXPECT_THROW(readOp.wait(),
                 ObjectDoesntExistException);

    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    uint32_t dataLength = 0;
    entry->objectBuf->getValue(&dataLength);
    EXPECT_EQ(0U, dataLength);
    EXPECT_EQ(ClientTransactionTask::CacheEntry::READ, entry->type);
    EXPECT_TRUE(entry->rejectRules.exists);

    EXPECT_THROW(transaction->read(tableId1, "0", 1, &value),
                 ObjectDoesntExistException);
}

TEST_F(TransactionTest, ReadOp_wait_batch_unexpectedStatus) {
    Buffer value;
    Transaction::ReadOp
            readOp(transaction.get(), tableId1, "0", 1, &value, true);
    EXPECT_TRUE(readOp.isReady());
    readOp.batchedRequest->request.status = STATUS_INTERNAL_ERROR;
    EXPECT_THROW(readOp.wait(), InternalError);
}

TEST_F(TransactionTest, ReadOp_wait_afterCommit) {
    // Makes sure that the point of the read is when wait is called.
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Key key(tableId1, "0", 1);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    Buffer value;
    Transaction::ReadOp readOp(transaction.get(), tableId1, "0", 1, &value);
    EXPECT_TRUE(readOp.singleRequest->readRpc);

    transaction->commit();

    EXPECT_THROW(readOp.wait(), TxOpAfterCommit);
}

}  // namespace RAMCloud
