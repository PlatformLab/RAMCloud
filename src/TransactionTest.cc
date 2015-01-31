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
    }

    void populateCommitCache() {
        transaction->insertCacheEntry(8, "01", 2, "hello 81", 8);
        transaction->insertCacheEntry(1, "01", 2, "hello 11", 8);
        transaction->insertCacheEntry(2, "01", 2, "hello 21", 8);
        transaction->insertCacheEntry(2, "01", 2, "goodbye 21", 10);
        transaction->insertCacheEntry(2, "02", 2, "hello 22", 8);
        transaction->insertCacheEntry(4, "01", 2, "hello 41", 8);
    }

    DISALLOW_COPY_AND_ASSIGN(TransactionTest);
};

TEST_F(TransactionTest, read_basic) {
    ramcloud->createTable("testTable");
    uint64_t tableId = ramcloud->getTableId("testTable");

    ramcloud->write(tableId, "0", 1, "abcdef", 6);
    ramcloud->write(tableId, "0", 1, "abcdef", 6);
    ramcloud->write(tableId, "0", 1, "abcdef", 6);

    Key key(tableId, "0", 1);
    EXPECT_TRUE(transaction->findCacheEntry(key) == NULL);

    Buffer value;
    transaction->read(tableId, "0", 1, &value);
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
    ramcloud->createTable("testTable");
    uint64_t tableId = ramcloud->getTableId("testTable");

    Buffer value;
    EXPECT_THROW(transaction->read(tableId, "0", 1, &value),
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

}  // namespace RAMCloud
