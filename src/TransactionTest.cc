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
#include "MockCluster.h"
#include "Transaction.h"

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
    Tub<Transaction::DecisionRpc> decisionRpc;
    Tub<Transaction::PrepareRpc> prepareRpc;
    Tub<Transaction> transaction;

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
        , decisionRpc()
        , prepareRpc()
        , transaction()
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
        transaction->lease = ramcloud->clientLease.getLease();

        prepareRpc.construct(ramcloud.get(), session, transaction.get());
        decisionRpc.construct(ramcloud.get(), session, transaction.get());

        // Make some tables.
        tableId1 = ramcloud->createTable("table1");
        tableId2 = ramcloud->createTable("table2");
        tableId3 = ramcloud->createTable("table3");
    }

    void populateCommitCache() {
        transaction->insertCacheEntry(8, "01", 2, "hello 81", 8);
        transaction->insertCacheEntry(1, "01", 2, "hello 11", 8);
        transaction->insertCacheEntry(2, "01", 2, "hello 21", 8);
        transaction->insertCacheEntry(2, "01", 2, "goodbye 21", 10);
        transaction->insertCacheEntry(2, "02", 2, "hello 22", 8);
        transaction->insertCacheEntry(4, "01", 2, "hello 41", 8);
    }

    string participantListToString(Transaction* t) {
        string s;
        uint32_t offset = 0;
        s.append(format("ParticipantList["));
        for (uint32_t i = 0; i < t->participantCount; i++) {
            WireFormat::TxParticipant* entry =
                    t->participantList.getOffset<WireFormat::TxParticipant>(
                            offset);
            s.append(format(" {%lu, %lu, %lu}",
                            entry->tableId, entry->keyHash, entry->rpcId));
            offset += sizeof32(WireFormat::TxParticipant);
        }
        s.append(format(" ]"));
        return s;
    }

    string rpcToString(Transaction::DecisionRpc* rpc) {
        string s;
        s.append(
                format("DecisionRpc :: lease{%lu}", rpc->reqHdr->leaseId));
        s.append(
                format(" participantCount{%u}", rpc->reqHdr->participantCount));
        uint32_t offset = sizeof(WireFormat::TxDecision::Request);

        s.append(format(" ParticipantList["));
        for (uint32_t i = 0; i < rpc->reqHdr->participantCount; i++) {
            WireFormat::TxParticipant* entry =
                    rpc->request.getOffset<WireFormat::TxParticipant>(offset);
            s.append(format(" {%lu, %lu, %lu}",
                            entry->tableId, entry->keyHash, entry->rpcId));
            offset += sizeof32(WireFormat::TxParticipant);
        }
        s.append(format(" ]"));
        return s;
    }

    string rpcToString(Transaction::PrepareRpc* rpc) {
        string s;
        s.append(
                format("PrepareRpc :: lease{%lu}", rpc->reqHdr->lease.leaseId));
        s.append(format(" ackId{%lu} ", rpc->reqHdr->ackId));
        s.append(
                format("participantCount{%u}", rpc->reqHdr->participantCount));
        s.append(format(" opCount{%u}", rpc->reqHdr->opCount));
        uint32_t offset = sizeof(WireFormat::TxPrepare::Request);

        s.append(format(" ParticipantList["));
        for (uint32_t i = 0; i < rpc->reqHdr->participantCount; i++) {
            WireFormat::TxParticipant* entry =
                    rpc->request.getOffset<WireFormat::TxParticipant>(offset);
            s.append(format(" {%lu, %lu, %lu}",
                            entry->tableId, entry->keyHash, entry->rpcId));
            offset += sizeof32(WireFormat::TxParticipant);
        }
        s.append(format(" ]"));
        s.append(format(" OpSet["));
        for (uint32_t i = 0; i < rpc->reqHdr->opCount; i++) {
            WireFormat::TxPrepare::OpType* type =
                    rpc->request.getOffset<
                            WireFormat::TxPrepare::OpType>(offset);
            switch (*type) {
                case WireFormat::TxPrepare::READ:
                {
                    WireFormat::TxPrepare::Request::ReadOp* entry =
                            rpc->request.getOffset<
                                    WireFormat::TxPrepare::Request::ReadOp>(
                                            offset);
                    s.append(format(" READ{%lu, %lu}",
                            entry->tableId, entry->rpcId));
                    offset += sizeof32(WireFormat::TxPrepare::Request::ReadOp);
                    offset += entry->keyLength;
                    break;
                }
                case WireFormat::TxPrepare::REMOVE:
                {
                    WireFormat::TxPrepare::Request::RemoveOp* entry =
                            rpc->request.getOffset<
                                    WireFormat::TxPrepare::Request::RemoveOp>(
                                            offset);
                    s.append(format(" REMOVE{%lu, %lu}",
                            entry->tableId, entry->rpcId));
                    offset +=
                            sizeof32(WireFormat::TxPrepare::Request::RemoveOp);
                    offset += entry->keyLength;
                    break;
                }
                case WireFormat::TxPrepare::WRITE:
                {
                    WireFormat::TxPrepare::Request::WriteOp* entry =
                            rpc->request.getOffset<
                                    WireFormat::TxPrepare::Request::WriteOp>(
                                            offset);
                    s.append(format(" WRITE{%lu, %lu}",
                            entry->tableId, entry->rpcId));
                    offset += sizeof32(WireFormat::TxPrepare::Request::WriteOp);
                    offset += entry->length;
                    break;
                }
                default:
                    break;
            }
        }
        s.append(format(" ]"));
        return s;
    }

    DISALLOW_COPY_AND_ASSIGN(TransactionTest);
};

// TODO(cstlee) : Unit test Transaction::commit()

TEST_F(TransactionTest, read_basic) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Key key(tableId1, "0", 1);
    EXPECT_TRUE(transaction->findCacheEntry(key) == NULL);

    Buffer value;
    transaction->read(tableId1, "0", 1, &value);
    EXPECT_EQ("abcdef", string(reinterpret_cast<const char*>(
                        value.getRange(0, value.size())),
                        value.size()));

    Transaction::CacheEntry* entry = transaction->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    uint32_t dataLength = 0;
    const char* str;
    str = reinterpret_cast<const char*>(
            entry->objectBuf->getValue(&dataLength));
    EXPECT_EQ("abcdef", string(str, dataLength));
    EXPECT_EQ(Transaction::CacheEntry::READ, entry->type);
    EXPECT_EQ(3U, entry->rejectRules.givenVersion);
}

TEST_F(TransactionTest, read_noObject) {
    Buffer value;
    EXPECT_THROW(transaction->read(tableId1, "0", 1, &value),
                 ObjectDoesntExistException);
}

TEST_F(TransactionTest, read_afterWrite) {
    uint32_t dataLength = 0;
    const char* str;

    Key key(1, "test", 4);
    EXPECT_TRUE(transaction->findCacheEntry(key) == NULL);

    transaction->write(1, "test", 4, "hello", 5);

    // Make sure the read, reads the last write.
    Buffer value;
    transaction->read(1, "test", 4, &value);
    EXPECT_EQ("hello", string(reinterpret_cast<const char*>(
                        value.getRange(0, value.size())),
                        value.size()));

    // Make sure the operations is still cached as a write.
    Transaction::CacheEntry* entry = transaction->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    EXPECT_EQ(Transaction::CacheEntry::WRITE, entry->type);
    EXPECT_EQ(0U, entry->rejectRules.givenVersion);
    str = reinterpret_cast<const char*>(
            entry->objectBuf->getValue(&dataLength));
    EXPECT_EQ("hello", string(str, dataLength));
}

TEST_F(TransactionTest, read_afterRemove) {
    Key key(1, "test", 4);
    EXPECT_TRUE(transaction->findCacheEntry(key) == NULL);

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
    Key key(1, "test", 4);
    EXPECT_TRUE(transaction->findCacheEntry(key) == NULL);

    transaction->remove(1, "test", 4);

    Transaction::CacheEntry* entry = transaction->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    EXPECT_EQ(Transaction::CacheEntry::REMOVE, entry->type);
    EXPECT_EQ(0U, entry->rejectRules.givenVersion);

    transaction->write(1, "test", 4, "goodbye", 7);
    entry->rejectRules.givenVersion = 42;

    transaction->remove(1, "test", 4);

    EXPECT_EQ(Transaction::CacheEntry::REMOVE, entry->type);
    EXPECT_EQ(42U, entry->rejectRules.givenVersion);

    EXPECT_EQ(entry, transaction->findCacheEntry(key));
}

TEST_F(TransactionTest, remove_afterCommit) {
    transaction->commitStarted = true;
    EXPECT_THROW(transaction->remove(1, "test", 4),
                 TxOpAfterCommit);
}

TEST_F(TransactionTest, write) {
    uint32_t dataLength = 0;
    const char* str;

    Key key(1, "test", 4);
    EXPECT_TRUE(transaction->findCacheEntry(key) == NULL);

    transaction->write(1, "test", 4, "hello", 5);

    Transaction::CacheEntry* entry = transaction->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    EXPECT_EQ(Transaction::CacheEntry::WRITE, entry->type);
    EXPECT_EQ(0U, entry->rejectRules.givenVersion);
    str = reinterpret_cast<const char*>(
            entry->objectBuf->getValue(&dataLength));
    EXPECT_EQ("hello", string(str, dataLength));

    entry->type = Transaction::CacheEntry::INVALID;
    entry->rejectRules.givenVersion = 42;

    transaction->write(1, "test", 4, "goodbye", 7);

    EXPECT_EQ(Transaction::CacheEntry::WRITE, entry->type);
    EXPECT_EQ(42U, entry->rejectRules.givenVersion);
    str = reinterpret_cast<const char*>(
            entry->objectBuf->getValue(&dataLength));
    EXPECT_EQ("goodbye", string(str, dataLength));

    EXPECT_EQ(entry, transaction->findCacheEntry(key));
}

TEST_F(TransactionTest, write_afterCommit) {
    transaction->commitStarted = true;
    EXPECT_THROW(transaction->write(1, "test", 4, "hello", 5),
                 TxOpAfterCommit);
}

TEST_F(TransactionTest, commitCache) {
    populateCommitCache();
    uint32_t dataLength = 0;
    const char* str;

    EXPECT_EQ(6U, transaction->commitCache.size());
    EXPECT_EQ(1U,
            transaction->commitCache.count({1, Key::getHash(1, "01", 2)}));
    EXPECT_EQ(2U,
            transaction->commitCache.count({2, Key::getHash(2, "01", 2)}));
    EXPECT_EQ(1U,
            transaction->commitCache.count({2, Key::getHash(2, "02", 2)}));
    EXPECT_EQ(1U,
            transaction->commitCache.count({4, Key::getHash(4, "01", 2)}));

    Transaction::CommitCacheMap::iterator it = transaction->commitCache.begin();
    EXPECT_EQ(1U, it->first.tableId);
    EXPECT_EQ(Key::getHash(1, "01", 2), it->first.keyHash);
    it++;

    EXPECT_EQ(2U, it->first.tableId);
    EXPECT_EQ(Key::getHash(2, "02", 2), it->first.keyHash);
    it++;

    EXPECT_EQ(2U, it->first.tableId);
    EXPECT_EQ(Key::getHash(2, "01", 2), it->first.keyHash);
    str = reinterpret_cast<const char*>(
            it->second.objectBuf->getValue(&dataLength));
    EXPECT_EQ("hello 21", string(str, dataLength));
    it++;

    EXPECT_EQ(2U, it->first.tableId);
    EXPECT_EQ(Key::getHash(2, "01", 2), it->first.keyHash);
    str = reinterpret_cast<const char*>(
            it->second.objectBuf->getValue(&dataLength));
    EXPECT_EQ("goodbye 21", string(str, dataLength));
    it++;

    EXPECT_EQ(4U, it->first.tableId);
    EXPECT_EQ(Key::getHash(4, "01", 2), it->first.keyHash);
    it++;

    EXPECT_EQ(8U, it->first.tableId);
    EXPECT_EQ(Key::getHash(8, "01", 2), it->first.keyHash);
    it++;

    EXPECT_TRUE(it == transaction->commitCache.end());
}

TEST_F(TransactionTest, findCacheEntry_basic) {
    populateCommitCache();
    Key key(1, "01", 2);
    Transaction::CacheEntry* entry = transaction->findCacheEntry(key);

    uint32_t dataLength = 0;
    const char* str;
    Key testKey(
            1, entry->objectBuf->getKey(), entry->objectBuf->getKeyLength());
    EXPECT_EQ(key, testKey);
    str = reinterpret_cast<const char*>(
            entry->objectBuf->getValue(&dataLength));
    EXPECT_EQ("hello 11", string(str, dataLength));
}

TEST_F(TransactionTest, findCacheEntry_notFound) {
    Key key1(1, "test", 4);
    EXPECT_TRUE(transaction->findCacheEntry(key1) == NULL);
    Key key3(3, "test", 4);
    EXPECT_TRUE(transaction->findCacheEntry(key3) == NULL);
    Key key8(8, "test", 4);
    EXPECT_TRUE(transaction->findCacheEntry(key8) == NULL);
}

TEST_F(TransactionTest, findCacheEntry_keyCollision) {
    populateCommitCache();

    uint32_t dataLength = 0;
    const char* str;

    Key key(2, "01", 2);
    Transaction::CacheEntry* entry = transaction->findCacheEntry(key);

    EXPECT_TRUE(entry != NULL);

    Key testKey1(
            2, entry->objectBuf->getKey(), entry->objectBuf->getKeyLength());
    EXPECT_EQ(key, testKey1);
    str = reinterpret_cast<const char*>(
            entry->objectBuf->getValue(&dataLength));
    EXPECT_EQ("hello 21", string(str, dataLength));

    // Sanity check
    Transaction::CommitCacheMap::iterator it =
            transaction->commitCache.lower_bound({key.getTableId(),
                                                 key.getHash()});

    entry = &it->second;
    str = reinterpret_cast<const char*>(
            entry->objectBuf->getValue(&dataLength));
    EXPECT_EQ("hello 21", string(str, dataLength));

    it++;
    entry = &it->second;
    str = reinterpret_cast<const char*>(
            entry->objectBuf->getValue(&dataLength));
    EXPECT_EQ("goodbye 21", string(str, dataLength));

    // Change first entry so as to not match.
    entry = transaction->findCacheEntry(key);
    entry->objectBuf->reset();
    Key badKey(2, "BAD", 3);
    Object::appendKeysAndValueToBuffer(
            badKey, "BAD   11", 8, entry->objectBuf, true);

    entry = transaction->findCacheEntry(key);

    EXPECT_TRUE(entry != NULL);

    Key testKey2(
            2, entry->objectBuf->getKey(), entry->objectBuf->getKeyLength());
    EXPECT_EQ(key, testKey2);
    str = reinterpret_cast<const char*>(
            entry->objectBuf->getValue(&dataLength));
    EXPECT_EQ("goodbye 21", string(str, dataLength));

    // Sanity check
    it = transaction->commitCache.lower_bound({key.getTableId(),
                                               key.getHash()});

    entry = &it->second;
    str = reinterpret_cast<const char*>(
            entry->objectBuf->getValue(&dataLength));
    EXPECT_EQ("BAD   11", string(str, dataLength));

    it++;
    entry = &it->second;
    str = reinterpret_cast<const char*>(
            entry->objectBuf->getValue(&dataLength));
    EXPECT_EQ("goodbye 21", string(str, dataLength));
}

TEST_F(TransactionTest, findCacheEntry_empty) {
    Key key(1, "test", 4);
    EXPECT_TRUE(transaction->findCacheEntry(key) == NULL);
}

TEST_F(TransactionTest, insertCacheEntry) {
    Key key(1, "test", 4);
    Transaction::CacheEntry* entry = transaction->insertCacheEntry(
            1, "test", 4, "hello world", 11);

    uint32_t dataLength = 0;
    const char* str;
    Key testKey(
            1, entry->objectBuf->getKey(), entry->objectBuf->getKeyLength());
    EXPECT_EQ(key, testKey);
    str = reinterpret_cast<const char*>(
            entry->objectBuf->getValue(&dataLength));
    EXPECT_EQ("hello world", string(str, dataLength));
}

TEST_F(TransactionTest, buildParticipantList) {
    transaction->write(1, "test", 4, "hello", 5);
    transaction->write(2, "test", 4, "hello", 5);
    transaction->write(3, "test", 4, "hello", 5);

    transaction->buildParticipantList();
    EXPECT_EQ("ParticipantList[ {1, 14087593745509316690, 1}"
                              " {2, 2793085152624492990, 2}"
                              " {3, 17667676865770333572, 3} ]",
              participantListToString(transaction.get()));
}

// TODO(cstlee) : Unit test Transaction::processDecisionRpcs()
// TODO(cstlee) : Unit test Transaction::processPrepareRpcs()

TEST_F(TransactionTest, sendDecisionRpc_basic) {
    transaction->write(tableId1, "test1", 5, "hello", 5);
    transaction->write(tableId1, "test2", 5, "hello", 5);
    transaction->write(tableId1, "test3", 5, "hello", 5);
    transaction->write(tableId1, "test4", 5, "hello", 5);
    transaction->write(tableId1, "test5", 5, "hello", 5);
    transaction->write(tableId2, "test1", 5, "hello", 5);
    transaction->write(tableId3, "test1", 5, "hello", 5);

    Transaction::DecisionRpc* rpc;
    transaction->nextCacheEntry = transaction->commitCache.begin();

    EXPECT_EQ(0U, transaction->decisionRpcs.size());

    // Should issue 1 rpc to master 1 with 3 objects in it.
    transaction->sendDecisionRpc();
    EXPECT_EQ(1U, transaction->decisionRpcs.size());
    rpc = &transaction->decisionRpcs.back();
    EXPECT_EQ(3U, rpc->reqHdr->participantCount);
    EXPECT_EQ("mock:host=master1", rpc->session.get()->serviceLocator);
    EXPECT_EQ("DecisionRpc :: lease{1} participantCount{3} "
              "ParticipantList[ {1, 2318870434438256899, 0} "
                               "{1, 5620473113711160829, 0} "
                               "{1, 8393261455223623089, 0} ]",
              rpcToString(rpc));

    // Rest nextCacheEntry to make see if processed ops will be skipped.
    transaction->nextCacheEntry = transaction->commitCache.begin();

    // Should issue 1 rpc to master 1 with 2 objects in it.
    transaction->sendDecisionRpc();
    EXPECT_EQ(2U, transaction->decisionRpcs.size());
    rpc = &transaction->decisionRpcs.back();
    EXPECT_EQ(2U, rpc->reqHdr->participantCount);
    EXPECT_EQ("mock:host=master1", rpc->session.get()->serviceLocator);
    EXPECT_EQ("DecisionRpc :: lease{1} participantCount{2} "
              "ParticipantList[ {1, 9099387403658286820, 0} "
                               "{1, 17025739677450802839, 0} ]",
              rpcToString(rpc));

    // Should issue 1 rpc to master 2 with 1 objects in it.
    transaction->sendDecisionRpc();
    EXPECT_EQ(3U, transaction->decisionRpcs.size());
    rpc = &transaction->decisionRpcs.back();
    EXPECT_EQ(1U, rpc->reqHdr->participantCount);
    EXPECT_EQ("mock:host=master2", rpc->session.get()->serviceLocator);
    EXPECT_EQ("DecisionRpc :: lease{1} participantCount{1} "
              "ParticipantList[ {2, 8137432257469122462, 0} ]",
              rpcToString(rpc));

    // Should issue 1 rpc to master 3 with 1 objects in it.
    transaction->sendDecisionRpc();
    EXPECT_EQ(4U, transaction->decisionRpcs.size());
    rpc = &transaction->decisionRpcs.back();
    EXPECT_EQ(1U, rpc->reqHdr->participantCount);
    EXPECT_EQ("mock:host=master3", rpc->session.get()->serviceLocator);
    EXPECT_EQ("DecisionRpc :: lease{1} participantCount{1} "
              "ParticipantList[ {3, 17123020360203364791, 0} ]",
              rpcToString(rpc));

    // Should issue do nothing.
    transaction->sendDecisionRpc();
    EXPECT_EQ(4U, transaction->decisionRpcs.size());
}

TEST_F(TransactionTest, sendDecisionRpc_TableDoesntExist) {
    transaction->write(0, "test1", 5, "hello", 5);
    transaction->nextCacheEntry = transaction->commitCache.begin();

    EXPECT_EQ(0U, transaction->prepareRpcs.size());
    EXPECT_EQ(STATUS_OK, transaction->status);
    EXPECT_EQ(Transaction::CacheEntry::PENDING,
              transaction->commitCache.begin()->second.state);
    transaction->sendPrepareRpc();
    EXPECT_EQ(0U, transaction->prepareRpcs.size());
    EXPECT_EQ(STATUS_TABLE_DOESNT_EXIST, transaction->status);
    EXPECT_EQ(Transaction::CacheEntry::FAILED,
              transaction->commitCache.begin()->second.state);
}

TEST_F(TransactionTest, sendPrepareRpc_basic) {
    transaction->write(tableId1, "test1", 5, "hello", 5);
    transaction->write(tableId1, "test2", 5, "hello", 5);
    transaction->write(tableId1, "test3", 5, "hello", 5);
    transaction->write(tableId1, "test4", 5, "hello", 5);
    transaction->write(tableId1, "test5", 5, "hello", 5);
    transaction->write(tableId2, "test1", 5, "hello", 5);
    transaction->write(tableId3, "test1", 5, "hello", 5);

    Transaction::PrepareRpc* rpc;
    transaction->nextCacheEntry = transaction->commitCache.begin();

    EXPECT_EQ(0U, transaction->prepareRpcs.size());

    // Should issue 1 rpc to master 1 with 3 objects in it.
    transaction->sendPrepareRpc();
    EXPECT_EQ(1U, transaction->prepareRpcs.size());
    rpc = &transaction->prepareRpcs.back();
    EXPECT_EQ(3U, rpc->reqHdr->opCount);
    EXPECT_EQ("mock:host=master1", rpc->session.get()->serviceLocator);
    EXPECT_EQ("PrepareRpc :: lease{1} ackId{0} participantCount{0} opCount{3} "
              "ParticipantList[ ] OpSet[ WRITE{1, 0} WRITE{1, 0} WRITE{1, 0} ]",
              rpcToString(rpc));

    // Rest nextCacheEntry to make see if processed ops will be skipped.
    transaction->nextCacheEntry = transaction->commitCache.begin();

    // Should issue 1 rpc to master 1 with 2 objects in it.
    transaction->sendPrepareRpc();
    EXPECT_EQ(2U, transaction->prepareRpcs.size());
    rpc = &transaction->prepareRpcs.back();
    EXPECT_EQ(2U, rpc->reqHdr->opCount);
    EXPECT_EQ("mock:host=master1", rpc->session.get()->serviceLocator);
    EXPECT_EQ("PrepareRpc :: lease{1} ackId{0} participantCount{0} opCount{2} "
              "ParticipantList[ ] OpSet[ WRITE{1, 0} WRITE{1, 0} ]",
              rpcToString(rpc));

    // Should issue 1 rpc to master 2 with 1 objects in it.
    transaction->sendPrepareRpc();
    EXPECT_EQ(3U, transaction->prepareRpcs.size());
    rpc = &transaction->prepareRpcs.back();
    EXPECT_EQ(1U, rpc->reqHdr->opCount);
    EXPECT_EQ("mock:host=master2", rpc->session.get()->serviceLocator);
    EXPECT_EQ("PrepareRpc :: lease{1} ackId{0} participantCount{0} opCount{1} "
              "ParticipantList[ ] OpSet[ WRITE{2, 0} ]", rpcToString(rpc));

    // Should issue 1 rpc to master 3 with 1 objects in it.
    transaction->sendPrepareRpc();
    EXPECT_EQ(4U, transaction->prepareRpcs.size());
    rpc = &transaction->prepareRpcs.back();
    EXPECT_EQ(1U, rpc->reqHdr->opCount);
    EXPECT_EQ("mock:host=master3", rpc->session.get()->serviceLocator);
    EXPECT_EQ("PrepareRpc :: lease{1} ackId{0} participantCount{0} opCount{1} "
              "ParticipantList[ ] OpSet[ WRITE{3, 0} ]", rpcToString(rpc));

    // Should issue do nothing.
    transaction->sendPrepareRpc();
    EXPECT_EQ(4U, transaction->prepareRpcs.size());
}

TEST_F(TransactionTest, sendPrepareRpc_TableDoesntExist) {
    transaction->write(0, "test1", 5, "hello", 5);
    transaction->nextCacheEntry = transaction->commitCache.begin();

    EXPECT_EQ(0U, transaction->prepareRpcs.size());
    EXPECT_EQ(STATUS_OK, transaction->status);
    EXPECT_EQ(Transaction::CacheEntry::PENDING,
              transaction->commitCache.begin()->second.state);
    transaction->sendPrepareRpc();
    EXPECT_EQ(0U, transaction->prepareRpcs.size());
    EXPECT_EQ(STATUS_TABLE_DOESNT_EXIST, transaction->status);
    EXPECT_EQ(Transaction::CacheEntry::FAILED,
              transaction->commitCache.begin()->second.state);
}

TEST_F(TransactionTest, DecisionRpc_constructor) {
    Transport::SessionRef session =
                ramcloud->clientContext->transportManager->getSession(
                "mock:host=master1");
    transaction->decision = WireFormat::TxDecision::ABORT;
    transaction->lease.leaseId = 42;
    transaction->participantCount = 2;

    Transaction::DecisionRpc rpc(ramcloud.get(), session, transaction.get());
    EXPECT_EQ(transaction->decision, rpc.reqHdr->decision);
    EXPECT_EQ(transaction->lease.leaseId, rpc.reqHdr->leaseId);
    EXPECT_EQ(0U, rpc.reqHdr->participantCount);
}

TEST_F(TransactionTest, DecisionRpc_checkStatus) {
    transaction->remove(1, "test", 4);
    transaction->nextCacheEntry = transaction->commitCache.end();
    WireFormat::TxPrepare::Response resp;
    resp.common.status = STATUS_TABLE_DOESNT_EXIST;
    decisionRpc->responseHeader = &resp.common;
    decisionRpc->checkStatus();
    EXPECT_EQ(transaction->commitCache.end(), transaction->nextCacheEntry);
    resp.common.status = STATUS_UNKNOWN_TABLET;
    decisionRpc->checkStatus();
    EXPECT_EQ(transaction->commitCache.begin(), transaction->nextCacheEntry);
}

TEST_F(TransactionTest, DecisionRpc_handleTransportError) {
    transaction->remove(1, "test", 4);
    transaction->nextCacheEntry = transaction->commitCache.end();
    TestLog::reset();
    decisionRpc->handleTransportError();
    EXPECT_TRUE(decisionRpc->session == NULL);
    EXPECT_EQ(transaction->commitCache.begin(), transaction->nextCacheEntry);
    EXPECT_EQ("flushSession: flushing session for mock:host=master3",
              TestLog::get());
}

TEST_F(TransactionTest, DecisionRpc_send) {
    EXPECT_TRUE(RpcWrapper::NOT_STARTED == decisionRpc->state);
    decisionRpc->send();
    EXPECT_TRUE(RpcWrapper::NOT_STARTED != decisionRpc->state);
}

TEST_F(TransactionTest, DecisionRpc_appendOp) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Buffer buf;
    transaction->read(tableId1, "0", 1, &buf);

    Transaction::CommitCacheMap::iterator it = transaction->commitCache.begin();
    it->second.rpcId = 42;
    EXPECT_EQ(Transaction::CacheEntry::PENDING, it->second.state);
    decisionRpc->appendOp(it);
    EXPECT_EQ(Transaction::CacheEntry::DECIDE, it->second.state);
    EXPECT_EQ(decisionRpc->ops[decisionRpc->reqHdr->participantCount - 1], it);
    EXPECT_EQ("DecisionRpc :: lease{1} participantCount{1} "
              "ParticipantList[ {1, 3894390131083956718, 42} ]",
              rpcToString(decisionRpc.get()));
}

TEST_F(TransactionTest, DecisionRpc_retryRequest) {
    transaction->write(1, "test", 4, "hello", 5);
    transaction->write(2, "test", 4, "hello", 5);
    transaction->write(3, "test", 4, "hello", 5);
    Transaction::CommitCacheMap::iterator it = transaction->commitCache.begin();
    while (it != transaction->commitCache.end()) {
        decisionRpc->appendOp(it);
        EXPECT_EQ(Transaction::CacheEntry::DECIDE, it->second.state);
        it++;
    }

    decisionRpc->retryRequest();

    it = transaction->commitCache.begin();
    while (it != transaction->commitCache.end()) {
        EXPECT_EQ(Transaction::CacheEntry::PENDING, it->second.state);
        it++;
    }
}

TEST_F(TransactionTest, PrepareRpc_constructor) {
    Transport::SessionRef session =
                ramcloud->clientContext->transportManager->getSession(
                "mock:host=master1");
    transaction->lease.leaseId = 42;
    transaction->participantCount = 2;
    transaction->participantList.emplaceAppend<WireFormat::TxParticipant>(
            1, 2, 3);
    transaction->participantList.emplaceAppend<WireFormat::TxParticipant>(
            4, 5, 6);

    Transaction::PrepareRpc rpc(ramcloud.get(), session, transaction.get());
    EXPECT_EQ(transaction->lease.leaseId, rpc.reqHdr->lease.leaseId);
    EXPECT_EQ(0U, rpc.reqHdr->ackId);
    EXPECT_EQ(transaction->participantCount, rpc.reqHdr->participantCount);
    EXPECT_EQ("PrepareRpc :: lease{42} ackId{0} participantCount{2} opCount{0}"
              " ParticipantList[ {1, 2, 3} {4, 5, 6} ] OpSet[ ]",
              rpcToString(&rpc));
}

TEST_F(TransactionTest, PrepareRpc_checkStatus) {
    transaction->remove(1, "test", 4);
    transaction->nextCacheEntry = transaction->commitCache.end();
    WireFormat::TxPrepare::Response resp;
    resp.common.status = STATUS_TABLE_DOESNT_EXIST;
    prepareRpc->responseHeader = &resp.common;
    prepareRpc->checkStatus();
    EXPECT_EQ(transaction->commitCache.end(), transaction->nextCacheEntry);
    resp.common.status = STATUS_UNKNOWN_TABLET;
    prepareRpc->checkStatus();
    EXPECT_EQ(transaction->commitCache.begin(), transaction->nextCacheEntry);
}

TEST_F(TransactionTest, PrepareRpc_handleTransportError) {
    transaction->remove(1, "test", 4);
    transaction->nextCacheEntry = transaction->commitCache.end();
    TestLog::reset();
    prepareRpc->handleTransportError();
    EXPECT_TRUE(prepareRpc->session == NULL);
    EXPECT_EQ(transaction->commitCache.begin(), transaction->nextCacheEntry);
    EXPECT_EQ("flushSession: flushing session for mock:host=master3",
              TestLog::get());
}

TEST_F(TransactionTest, PrepareRpc_send) {
    EXPECT_EQ(0U, prepareRpc->reqHdr->ackId);
    EXPECT_TRUE(RpcWrapper::NOT_STARTED == prepareRpc->state);
    prepareRpc->send();
    EXPECT_EQ(ramcloud->rpcTracker.ackId(), prepareRpc->reqHdr->ackId);
    EXPECT_TRUE(RpcWrapper::NOT_STARTED != prepareRpc->state);
}

TEST_F(TransactionTest, PrepareRpc_appendOp_read) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Buffer buf;
    transaction->read(tableId1, "0", 1, &buf);

    Transaction::CommitCacheMap::iterator it = transaction->commitCache.begin();
    it->second.rpcId = 42;
    EXPECT_EQ(Transaction::CacheEntry::PENDING, it->second.state);
    prepareRpc->appendOp(it);
    EXPECT_EQ(Transaction::CacheEntry::PREPARE, it->second.state);
    EXPECT_EQ(prepareRpc->ops[prepareRpc->reqHdr->opCount - 1], it);
    EXPECT_EQ("PrepareRpc :: lease{1} ackId{0} participantCount{0} opCount{1} "
              "ParticipantList[ ] OpSet[ READ{1, 42} ]",
              rpcToString(prepareRpc.get()));
}

TEST_F(TransactionTest, PrepareRpc_appendOp_remove) {
    transaction->remove(2, "test", 4);

    Transaction::CommitCacheMap::iterator it = transaction->commitCache.begin();
    it->second.rpcId = 42;
    EXPECT_EQ(Transaction::CacheEntry::PENDING, it->second.state);
    prepareRpc->appendOp(it);
    EXPECT_EQ(Transaction::CacheEntry::PREPARE, it->second.state);
    EXPECT_EQ(prepareRpc->ops[prepareRpc->reqHdr->opCount - 1], it);
    EXPECT_EQ("PrepareRpc :: lease{1} ackId{0} participantCount{0} opCount{1} "
              "ParticipantList[ ] OpSet[ REMOVE{2, 42} ]",
              rpcToString(prepareRpc.get()));
}

TEST_F(TransactionTest, PrepareRpc_appendOp_write) {
    transaction->write(3, "test", 4, "hello", 5);

    Transaction::CommitCacheMap::iterator it = transaction->commitCache.begin();
    it->second.rpcId = 42;
    EXPECT_EQ(Transaction::CacheEntry::PENDING, it->second.state);
    prepareRpc->appendOp(it);
    EXPECT_EQ(Transaction::CacheEntry::PREPARE, it->second.state);
    EXPECT_EQ(prepareRpc->ops[prepareRpc->reqHdr->opCount - 1], it);
    EXPECT_EQ("PrepareRpc :: lease{1} ackId{0} participantCount{0} opCount{1} "
              "ParticipantList[ ] OpSet[ WRITE{3, 42} ]",
              rpcToString(prepareRpc.get()));
}

TEST_F(TransactionTest, PrepareRpc_appendOp_invalid) {
    transaction->write(3, "test", 4, "hello", 5);

    Transaction::CommitCacheMap::iterator it = transaction->commitCache.begin();
    it->second.rpcId = 42;
    it->second.type = Transaction::CacheEntry::INVALID;
    EXPECT_EQ(Transaction::CacheEntry::PENDING, it->second.state);
    TestLog::reset();
    prepareRpc->appendOp(it);
    EXPECT_EQ(Transaction::CacheEntry::PENDING, it->second.state);
    EXPECT_EQ(0U, prepareRpc->reqHdr->opCount);
    EXPECT_EQ("PrepareRpc :: lease{1} ackId{0} participantCount{0} opCount{0} "
              "ParticipantList[ ] OpSet[ ]", rpcToString(prepareRpc.get()));
    EXPECT_EQ("appendOp: Unknown transaction op type.", TestLog::get());
}

TEST_F(TransactionTest, PrepareRpc_retryRequest) {
    transaction->write(1, "test", 4, "hello", 5);
    transaction->write(2, "test", 4, "hello", 5);
    transaction->write(3, "test", 4, "hello", 5);
    Transaction::CommitCacheMap::iterator it = transaction->commitCache.begin();
    while (it != transaction->commitCache.end()) {
        prepareRpc->appendOp(it);
        EXPECT_EQ(Transaction::CacheEntry::PREPARE, it->second.state);
        it++;
    }

    prepareRpc->retryRequest();

    it = transaction->commitCache.begin();
    while (it != transaction->commitCache.end()) {
        EXPECT_EQ(Transaction::CacheEntry::PENDING, it->second.state);
        it++;
    }
}

}  // namespace RAMCloud
