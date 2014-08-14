/* Copyright (c) 2014 Stanford University
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
#include "IndexletManager.h"
#include "IndexKey.h"
#include "MockCluster.h"
#include "RamCloud.h"
#include "StringUtil.h"

namespace RAMCloud {

class IndexletManagerTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;

    Context context;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    IndexletManager* im;
    TabletManager* tm;

    // Data table. Individual tests can use ramcloud->createIndex()
    // to create index(es) corresponding to this table.
    uint64_t dataTableId;
    // Backing table that can be used by a test that wants to individually
    // create and access indexlets (rather than an entire index)
    // using the indexletManager class (rather than ramcloud class).
    uint64_t indexletTableId;

    // Declare variables commonly needed by most tests.
    Buffer responseBuffer;
    uint32_t numHashes;
    uint16_t nextKeyLength;
    uint64_t nextKeyHash;
    uint32_t lookupOffset;

    IndexletManagerTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , ramcloud()
        , im()
        , tm()
        , dataTableId()
        , indexletTableId()
        , responseBuffer()
        , numHashes()
        , nextKeyLength()
        , nextKeyHash()
        , lookupOffset(sizeof32(WireFormat::LookupIndexKeys::Response))
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::BACKUP_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master1";
        cluster.addServer(config);

        im = &cluster.contexts[0]->masterService->indexletManager;
        tm = &cluster.contexts[0]->masterService->tabletManager;

        ramcloud.construct(&context, "mock:host=coordinator");

        dataTableId = ramcloud->createTable("dataTable");
        indexletTableId = ramcloud->createTable("indexletTable");
    }

    IndexletManager::Indexlet*
    testGetIndexlet(uint64_t tableId, uint8_t indexId,
            const void *firstKey, uint16_t firstKeyLength,
            const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength)
    {
        SpinLock mutex;
        IndexletManager::Lock fakeGuard(mutex);
        IndexletManager::IndexletMap::iterator it = im->getIndexlet(
                tableId, indexId, firstKey, firstKeyLength,
                firstNotOwnedKey, firstNotOwnedKeyLength,
                fakeGuard);
        if (it == im->indexletMap.end()) {
            return NULL;
        }
        return &it->second;
    }

    DISALLOW_COPY_AND_ASSIGN(IndexletManagerTest);
};

TEST_F(IndexletManagerTest, constructor) {
    EXPECT_EQ(0U, im->indexletMap.size());
}

TEST_F(IndexletManagerTest, addIndexlet) {
    string key1 = "a";
    string key2 = "c";
    string key3 = "f";
    string key4 = "k";
    string key5 = "u";

    TestLog::Enable _("addIndexlet", "getIndexlet", NULL);
    EXPECT_NO_THROW(im->addIndexlet(dataTableId, 1, indexletTableId,
            key2.c_str(), (uint16_t)key2.length(),
            key4.c_str(), (uint16_t)key4.length()));
    EXPECT_EQ("", TestLog::get());


    EXPECT_NO_THROW(im->addIndexlet(dataTableId, 1,
            indexletTableId + 1, key2.c_str(),
            (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));
    EXPECT_EQ("addIndexlet: Adding indexlet in tableId 1 indexId 1, "
            "but already own. Returning success.", TestLog::get());
    TestLog::reset();

    EXPECT_NO_THROW(im->addIndexlet(dataTableId, 1, indexletTableId + 2,
            key1.c_str(), (uint16_t)key1.length(),
            key2.c_str(), (uint16_t)key2.length()));
    EXPECT_EQ("", TestLog::get());


    EXPECT_NO_THROW(im->addIndexlet(dataTableId, 1, indexletTableId + 3,
            key4.c_str(), (uint16_t)key4.length(),
            key5.c_str(), (uint16_t)key5.length()));
    EXPECT_EQ("", TestLog::get());


    EXPECT_THROW(im->addIndexlet(dataTableId, 1, indexletTableId + 4,
            key1.c_str(), (uint16_t)key1.length(),
            key3.c_str(), (uint16_t)key3.length()), InternalError);
    EXPECT_EQ("getIndexlet: Given indexlet in tableId 1, indexId 1 "
            "overlaps with one or more other ranges.", TestLog::get());

    SpinLock mutex;
    IndexletManager::Lock fakeGuard(mutex);
    IndexletManager::Indexlet* indexlet = &im->lookupIndexlet(dataTableId, 1,
            key2.c_str(), (uint16_t)key2.length(), fakeGuard)->second;
    string firstKey = StringUtil::binaryToString(
            indexlet->firstKey, indexlet->firstKeyLength);
    string firstNotOwnedKey = StringUtil::binaryToString(
            indexlet->firstNotOwnedKey, indexlet->firstNotOwnedKeyLength);

    EXPECT_EQ(0, firstKey.compare("c"));
    EXPECT_EQ(0, firstNotOwnedKey.compare("k"));
}

TEST_F(IndexletManagerTest, hasIndexlet) {
    string key1 = "a";
    string key2 = "c";
    string key3 = "f";
    string key4 = "k";
    string key5 = "u";

    // check if indexlet exist corresponding to c
    EXPECT_FALSE(im->hasIndexlet(dataTableId, 1,
            key2.c_str(), (uint16_t)key2.length()));

    // add indexlet exist corresponding to [c, k)
    im->addIndexlet(dataTableId, 1, indexletTableId, key2.c_str(),
            (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length());

    // check if indexlet exist corresponding to c
    EXPECT_TRUE(im->hasIndexlet(dataTableId, 1,
            key2.c_str(), (uint16_t)key2.length()));

    // different table id
    EXPECT_FALSE(im->hasIndexlet(dataTableId+100, 1,
            key2.c_str(), (uint16_t)key2.length()));

    // different index id
    EXPECT_FALSE(im->hasIndexlet(dataTableId, 2,
            key2.c_str(), (uint16_t)key2.length()));

    // key is within the range of indexlet
    EXPECT_TRUE(im->hasIndexlet(dataTableId, 1,
            key3.c_str(), (uint16_t)key3.length()));

    // key is before the range of indexlet
    EXPECT_FALSE(im->hasIndexlet(dataTableId, 1,
            key1.c_str(), (uint16_t)key1.length()));

    // key is after the range of indexlet
    EXPECT_FALSE(im->hasIndexlet(dataTableId, 2,
            key2.c_str(), (uint16_t)key2.length()));
}

TEST_F(IndexletManagerTest, getIndexlet) {
    string key1 = "a";
    string key2 = "c";
    string key3 = "f";
    string key4 = "k";
    string key5 = "u";

    // check if indexlet exist corresponding to [c, k)
    EXPECT_FALSE(testGetIndexlet(dataTableId, 1, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    // add indexlet exist corresponding to [c, k)
    im->addIndexlet(dataTableId, 1, indexletTableId, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length());

    // check if indexlet exist corresponding to [c, k)
    EXPECT_TRUE(testGetIndexlet(dataTableId, 1, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    // different table id
    EXPECT_FALSE(testGetIndexlet(dataTableId+100, 1, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    // different index id
    EXPECT_FALSE(testGetIndexlet(dataTableId, 2, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    // last key is within the range of indexlet
    EXPECT_THROW(testGetIndexlet(dataTableId, 1, key2.c_str(),
        (uint16_t)key2.length(), key3.c_str(), (uint16_t)key3.length()),
        InternalError);

    // first key is within the range of indexlet
    EXPECT_THROW(testGetIndexlet(dataTableId, 1, key3.c_str(),
        (uint16_t)key3.length(), key4.c_str(), (uint16_t)key4.length()),
        InternalError);

    // first key is before the range of indexlet
    EXPECT_FALSE(testGetIndexlet(dataTableId, 1, key1.c_str(),
        (uint16_t)key1.length(), key4.c_str(), (uint16_t)key4.length()));

    // last key is after the range of indexlet
    EXPECT_FALSE(testGetIndexlet(dataTableId, 2, key2.c_str(),
        (uint16_t)key2.length(), key5.c_str(), (uint16_t)key5.length()));

    // first and last key are beyond the range of indexlet
    EXPECT_FALSE(testGetIndexlet(dataTableId, 2, key1.c_str(),
        (uint16_t)key1.length(), key5.c_str(), (uint16_t)key5.length()));

    IndexletManager::Indexlet* indexlet = testGetIndexlet(
            dataTableId, 1, key2.c_str(), (uint16_t)key2.length(),
            key4.c_str(), (uint16_t)key4.length());
    EXPECT_TRUE(indexlet);

    string firstKey = StringUtil::binaryToString(
            indexlet->firstKey, indexlet->firstKeyLength);
    EXPECT_EQ(0, firstKey.compare("c"));
    string firstNotOwnedKey = StringUtil::binaryToString(
            indexlet->firstNotOwnedKey, indexlet->firstNotOwnedKeyLength);
    EXPECT_EQ(0, firstNotOwnedKey.compare("k"));
}

TEST_F(IndexletManagerTest, deleteIndexlet) {
    TestLog::Enable _("deleteIndexlet", "getIndexlet", NULL);

    string key1 = "a";
    string key2 = "c";
    string key3 = "f";
    string key4 = "k";
    string key5 = "u";

    SpinLock mutex;
    IndexletManager::Lock fakeGuard(mutex);

    EXPECT_NO_THROW(testGetIndexlet(dataTableId, 1, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    im->addIndexlet(dataTableId, 1, indexletTableId, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length());

    EXPECT_TRUE(testGetIndexlet(dataTableId, 1, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_NO_THROW(im->deleteIndexlet(dataTableId, 1, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));
    EXPECT_EQ("", TestLog::get());

    EXPECT_FALSE(testGetIndexlet(dataTableId, 1, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_NO_THROW(im->deleteIndexlet(dataTableId, 1, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));
    EXPECT_EQ("deleteIndexlet: Unknown indexlet in tableId 1, indexId 1",
              TestLog::get());
    TestLog::reset();

    // we need to create the table corresponding to each indexlet
    // (b-tree). There is one indexlet that is created as part
    // of the constructor of IndexletManagerTest
    tm->addTablet(indexletTableId + 1, 0, ~0UL,
                            TabletManager::NORMAL);
    im->addIndexlet(dataTableId, 1, indexletTableId, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length());

    EXPECT_THROW(im->deleteIndexlet(dataTableId, 1, key2.c_str(),
        (uint16_t)key2.length(), key5.c_str(), (uint16_t)key5.length()),
        InternalError);
    EXPECT_EQ("getIndexlet: Given indexlet in tableId 1, indexId 1 "
              "overlaps with one or more other ranges.", TestLog::get());
    TestLog::reset();

    EXPECT_NO_THROW(im->deleteIndexlet(dataTableId, 1, key1.c_str(),
        (uint16_t)key1.length(), key4.c_str(), (uint16_t)key4.length()));
    EXPECT_EQ("deleteIndexlet: Unknown indexlet in tableId 1, indexId 1",
              TestLog::get());
}

// TODO(ashgup): Add unit tests for functions that currently don't have them:
// getCount, lookupIndexlet.

TEST_F(IndexletManagerTest, insertIndexEntry) {
    ramcloud->createIndex(dataTableId, 1, 0);

    Status insertStatus1 = im->insertEntry(dataTableId, 1, "air", 3, 5678);
    EXPECT_EQ(STATUS_OK, insertStatus1);
    Status insertStatus2 = im->insertEntry(dataTableId, 1, "earth", 5, 9876);
    EXPECT_EQ(STATUS_OK, insertStatus2);

    ramcloud->lookupIndexKeys(dataTableId, 1, "air", 3, 0, "air", 3, 100,
                              &responseBuffer, &numHashes,
                              &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(1U, numHashes);
    EXPECT_EQ(5678U, *responseBuffer.getOffset<uint64_t>(lookupOffset));
}

TEST_F(IndexletManagerTest, insertIndexEntry_unknownIndexlet) {
    im->addIndexlet(dataTableId, 1, indexletTableId, "a", 1, "k", 1);

    Status insertStatus = im->insertEntry(dataTableId, 1, "water", 5, 1234);
    EXPECT_EQ(STATUS_UNKNOWN_INDEXLET, insertStatus);
}

TEST_F(IndexletManagerTest, insertIndexEntry_duplicate) {
    im->addIndexlet(dataTableId, 1, indexletTableId, "a", 1, "k", 1);

    Status insertStatus1 = im->insertEntry(dataTableId, 1, "air", 3, 1234);
    EXPECT_EQ(STATUS_OK, insertStatus1);
    Status insertStatus2 = im->insertEntry(dataTableId, 1, "air", 3, 5678);
    EXPECT_EQ(STATUS_OK, insertStatus2);

    // Lookup for duplicates is tested in lookIndexKeys_duplicate.
}

TEST_F(IndexletManagerTest, lookupIndexKeys_unknownIndex) {
    ramcloud->lookupIndexKeys(dataTableId, 1, "water", 5, 0, "water", 5,
                              100, &responseBuffer, &numHashes,
                              &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, WireFormat::getStatus(&responseBuffer));
    EXPECT_EQ(0U, numHashes);
    EXPECT_EQ(0U, nextKeyLength);
    EXPECT_EQ(0U, nextKeyHash);
}

TEST_F(IndexletManagerTest, lookupIndexKeys_keyNotFound) {
    ramcloud->createIndex(dataTableId, 1, 0);

    // Lookup a key when indexlet doesn't have any data.
    ramcloud->lookupIndexKeys(dataTableId, 1, "air", 3, 0, "air", 3, 100,
                              &responseBuffer, &numHashes,
                              &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, WireFormat::getStatus(&responseBuffer));
    EXPECT_EQ(0U, numHashes);

    // Lookup a key k1 when indexlet doesn't have k1 but has k2.
    im->insertEntry(dataTableId, 1, "earth", 5, 9876);

    ramcloud->lookupIndexKeys(dataTableId, 1, "air", 3, 0, "air", 3, 100,
                             &responseBuffer, &numHashes,
                             &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, WireFormat::getStatus(&responseBuffer));
    EXPECT_EQ(0U, numHashes);
}

TEST_F(IndexletManagerTest, lookupIndexKeys_single) {
    ramcloud->createIndex(dataTableId, 1, 0);

    // Lookup such that the result should be a single object when only
    // one object exists in the indexlet.
    im->insertEntry(dataTableId, 1, "air", 3, 5678);

    // First key = key = last key
    ramcloud->lookupIndexKeys(dataTableId, 1, "air", 3, 0, "air", 3, 100,
                              &responseBuffer, &numHashes,
                              &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, WireFormat::getStatus(&responseBuffer));
    EXPECT_EQ(1U, numHashes);
    EXPECT_EQ(5678U, *responseBuffer.getOffset<uint64_t>(lookupOffset));

    // First key < key < last key
    responseBuffer.reset();
    ramcloud->lookupIndexKeys(dataTableId, 1, "a", 1, 0, "c", 1, 100,
                              &responseBuffer, &numHashes,
                              &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, WireFormat::getStatus(&responseBuffer));
    EXPECT_EQ(1U, numHashes);
    EXPECT_EQ(5678U, *responseBuffer.getOffset<uint64_t>(lookupOffset));


    // Lookup such that the result should be a single object when
    // multiple objects exist in the indexlet.
    im->insertEntry(dataTableId, 1, "earth", 5, 9876);

    responseBuffer.reset();
    ramcloud->lookupIndexKeys(dataTableId, 1, "a", 1, 0, "c", 1, 100,
                              &responseBuffer, &numHashes,
                              &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, WireFormat::getStatus(&responseBuffer));
    EXPECT_EQ(1U, numHashes);
    EXPECT_EQ(5678U, *responseBuffer.getOffset<uint64_t>(lookupOffset));
}

TEST_F(IndexletManagerTest, lookupIndexKeys_multiple) {
    ramcloud->createIndex(dataTableId, 1, 0);

    im->insertEntry(dataTableId, 1, "air", 3, 5678);
    im->insertEntry(dataTableId, 1, "earth", 5, 9876);
    im->insertEntry(dataTableId, 1, "fire", 4, 5432);

    // Point lookup only for first entry.
    responseBuffer.reset();
    ramcloud->lookupIndexKeys(dataTableId, 1, "air", 3, 0, "air", 3, 100,
                              &responseBuffer, &numHashes,
                              &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, WireFormat::getStatus(&responseBuffer));
    EXPECT_EQ(1U, numHashes);
    EXPECT_EQ(5678U, *responseBuffer.getOffset<uint64_t>(lookupOffset));

    // Point lookup only for second entry.
    responseBuffer.reset();
    ramcloud->lookupIndexKeys(dataTableId, 1, "earth", 5, 0, "earth", 5, 100,
                              &responseBuffer, &numHashes,
                              &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, WireFormat::getStatus(&responseBuffer));
    EXPECT_EQ(1U, numHashes);
    EXPECT_EQ(9876U, *responseBuffer.getOffset<uint64_t>(lookupOffset));

    // Point lookup only for third entry.
    responseBuffer.reset();
    ramcloud->lookupIndexKeys(dataTableId, 1, "fire", 4, 0, "fire", 4, 100,
                              &responseBuffer, &numHashes,
                              &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, WireFormat::getStatus(&responseBuffer));
    EXPECT_EQ(1U, numHashes);
    EXPECT_EQ(5432U, *responseBuffer.getOffset<uint64_t>(lookupOffset));

    // Range lookup for all entries such that:
    // first key = lowest expected key < highest expected key = last key
    responseBuffer.reset();
    ramcloud->lookupIndexKeys(dataTableId, 1, "air", 3, 0, "fire", 4, 100,
                              &responseBuffer, &numHashes,
                              &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, WireFormat::getStatus(&responseBuffer));
    EXPECT_EQ(3U, numHashes);
    EXPECT_EQ(5678U, *responseBuffer.getOffset<uint64_t>(lookupOffset));
    EXPECT_EQ(9876U, *responseBuffer.getOffset<uint64_t>(lookupOffset + 8));
    EXPECT_EQ(5432U, *responseBuffer.getOffset<uint64_t>(lookupOffset + 16));

    // Range lookup for all entries such that:
    // first key < lowest expected key < highest expected key < last key
    responseBuffer.reset();
    ramcloud->lookupIndexKeys(dataTableId, 1, "a", 1, 0, "g", 1, 100,
                              &responseBuffer, &numHashes,
                              &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, WireFormat::getStatus(&responseBuffer));
    EXPECT_EQ(3U, numHashes);
    EXPECT_EQ(5678U, *responseBuffer.getOffset<uint64_t>(lookupOffset));
    EXPECT_EQ(9876U, *responseBuffer.getOffset<uint64_t>(lookupOffset + 8));
    EXPECT_EQ(5432U, *responseBuffer.getOffset<uint64_t>(lookupOffset + 16));
}

TEST_F(IndexletManagerTest, lookupIndexKeys_duplicate) {
    ramcloud->createIndex(dataTableId, 1, 0);

    im->insertEntry(dataTableId, 1, "air", 3, 1234);
    im->insertEntry(dataTableId, 1, "air", 3, 5678);

    ramcloud->lookupIndexKeys(dataTableId, 1, "air", 3, 0, "air", 3, 100,
                              &responseBuffer, &numHashes,
                              &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, WireFormat::getStatus(&responseBuffer));
    EXPECT_EQ(2U, numHashes);
    EXPECT_EQ(1234U, *responseBuffer.getOffset<uint64_t>(lookupOffset));
    EXPECT_EQ(5678U, *responseBuffer.getOffset<uint64_t>(lookupOffset + 8));
}

TEST_F(IndexletManagerTest, lookupIndexKeys_firstAllowedKeyHash) {
    ramcloud->createIndex(dataTableId, 1, 0);

    im->insertEntry(dataTableId, 1, "air", 3, 1234);
    im->insertEntry(dataTableId, 1, "air", 3, 5678);
    im->insertEntry(dataTableId, 1, "air", 3, 9012);

    // firstAllowedKeyHash is between the key hashes.
    ramcloud->lookupIndexKeys(dataTableId, 1, "air", 3,
                              2000 /*firstAllowedKeyHash*/, "air", 3, 100,
                              &responseBuffer, &numHashes,
                              &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, WireFormat::getStatus(&responseBuffer));
    EXPECT_EQ(2U, numHashes);
    EXPECT_EQ(5678U, *responseBuffer.getOffset<uint64_t>(lookupOffset));
    EXPECT_EQ(9012U, *responseBuffer.getOffset<uint64_t>(lookupOffset + 8));

    // firstAllowedKeyHash is equal to one of the key hashes.
    responseBuffer.reset();
    ramcloud->lookupIndexKeys(dataTableId, 1, "air", 3,
                              5678 /*firstAllowedKeyHash*/, "air", 3, 100,
                              &responseBuffer, &numHashes,
                              &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, WireFormat::getStatus(&responseBuffer));
    EXPECT_EQ(2U, numHashes);
    EXPECT_EQ(5678U, *responseBuffer.getOffset<uint64_t>(lookupOffset));
    EXPECT_EQ(9012U, *responseBuffer.getOffset<uint64_t>(lookupOffset + 8));
}

TEST_F(IndexletManagerTest, lookupIndexKeys_largerRange) {
    // Lookup such that the range of keys in the lookup request is larger than
    // the range of keys owned by this indexlet.

    ramcloud->createIndex(dataTableId, 1, 0, 2);

    im->insertEntry(dataTableId, 1, "air", 3, 5678);

    ramcloud->lookupIndexKeys(dataTableId, 1, "a", 1, 0, "z", 1, 100,
                              &responseBuffer, &numHashes,
                              &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, WireFormat::getStatus(&responseBuffer));
    EXPECT_EQ(1U, numHashes);
    EXPECT_EQ(5678U, *responseBuffer.getOffset<uint64_t>(lookupOffset));
    EXPECT_EQ(1U, nextKeyLength);
    EXPECT_EQ("\x1", string(reinterpret_cast<const char*>(
            responseBuffer.getRange(8, nextKeyLength)), nextKeyLength));
    EXPECT_EQ(0U, nextKeyHash);
}

TEST_F(IndexletManagerTest, lookupIndexKeys_largerThanMax) {
    // Lookup such that the number of key hashes that would be returned
    // is larger than the maximum allowed.
    // We can set the max to be a small number here; In real operation, it
    // will typically be the maximum that can fit in an RPC, and will
    // be set by the MasterService.

    ramcloud->createIndex(dataTableId, 1, 0);

    im->insertEntry(dataTableId, 1, "air", 3, 5678);
    im->insertEntry(dataTableId, 1, "earth", 5, 9876);
    im->insertEntry(dataTableId, 1, "fire", 4, 5432);

    ramcloud->lookupIndexKeys(dataTableId, 1, "a", 1, 0, "g", 1, 2,
                              &responseBuffer, &numHashes,
                              &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, WireFormat::getStatus(&responseBuffer));
    EXPECT_EQ(2U, numHashes);
    EXPECT_EQ(5678U, *responseBuffer.getOffset<uint64_t>(lookupOffset));
    EXPECT_EQ(9876U, *responseBuffer.getOffset<uint64_t>(lookupOffset + 8));
    EXPECT_EQ(4U, nextKeyLength);
    EXPECT_EQ("fire", string(reinterpret_cast<const char*>(
                responseBuffer.getRange(lookupOffset + 16, nextKeyLength)),
                nextKeyLength));
    EXPECT_EQ(5432U, nextKeyHash);
}

TEST_F(IndexletManagerTest, removeEntry_single) {
    ramcloud->createIndex(dataTableId, 1, 0);

    im->insertEntry(dataTableId, 1, "air", 3, 5678);

    Status removeStatus = im->removeEntry(dataTableId, 1, "air", 3, 5678);
    EXPECT_EQ(STATUS_OK, removeStatus);

    ramcloud->lookupIndexKeys(dataTableId, 1, "air", 3, 0, "air", 3, 100,
                       &responseBuffer, &numHashes,
                       &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(0U, numHashes);
}

TEST_F(IndexletManagerTest, removeEntry_multipleEntries) {
    ramcloud->createIndex(dataTableId, 1, 0);

    im->insertEntry(dataTableId, 1, "air", 3, 5678);
    im->insertEntry(dataTableId, 1, "earth", 5, 9876);
    im->insertEntry(dataTableId, 1, "fire", 4, 5432);

    Status removeStatus1 = im->removeEntry(dataTableId, 1, "air", 3, 5678);
    Status removeStatus2 = im->removeEntry(dataTableId, 1, "fire", 4, 5432);
    EXPECT_EQ(STATUS_OK, removeStatus1);
    EXPECT_EQ(STATUS_OK, removeStatus2);

    responseBuffer.reset();
    ramcloud->lookupIndexKeys(dataTableId, 1, "a", 1, 0, "h", 1, 100,
                       &responseBuffer, &numHashes,
                       &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(1U, numHashes);
    EXPECT_EQ(9876U, *responseBuffer.getOffset<uint64_t>(lookupOffset));
}

TEST_F(IndexletManagerTest, removeEntry_multipleIndexlets) {
    ramcloud->createIndex(dataTableId, 1, 0);
    ramcloud->createIndex(dataTableId, 2, 0);

    KeyInfo keyListA[3];
    keyListA[0].keyLength = 5;
    keyListA[0].key = "keyA0";
    keyListA[1].keyLength = 5;
    keyListA[1].key = "keyA1";
    keyListA[2].keyLength = 5;
    keyListA[2].key = "keyA2";

    KeyInfo keyListB[3];
    keyListB[0].keyLength = 5;
    keyListB[0].key = "keyB0";
    keyListB[1].keyLength = 5;
    keyListB[1].key = "keyB1";
    keyListB[2].keyLength = 5;
    keyListB[2].key = "keyB2";

    Key primaryKeyA(dataTableId, keyListA[0].key, keyListA[0].keyLength);
    Key primaryKeyB(dataTableId, keyListB[0].key, keyListB[0].keyLength);

    im->insertEntry(dataTableId, 1, keyListA[1].key, keyListA[1].keyLength,
                    primaryKeyA.getHash());
    im->insertEntry(dataTableId, 2, keyListA[2].key, keyListA[2].keyLength,
                    primaryKeyA.getHash());
    im->insertEntry(dataTableId, 1, keyListB[1].key, keyListB[1].keyLength,
                    primaryKeyB.getHash());
    im->insertEntry(dataTableId, 2, keyListB[2].key, keyListB[2].keyLength,
                    primaryKeyB.getHash());

    Status removeStatus1 =
            im->removeEntry(dataTableId, 1, keyListA[1].key,
                            keyListA[1].keyLength, primaryKeyA.getHash());
    Status removeStatus2 =
            im->removeEntry(dataTableId, 2, keyListA[2].key,
                            keyListA[2].keyLength, primaryKeyA.getHash());
    Status removeStatus3 =
            im->removeEntry(dataTableId, 1, keyListB[1].key,
                            keyListB[1].keyLength, primaryKeyB.getHash());
    Status removeStatus4 =
            im->removeEntry(dataTableId, 2, keyListB[2].key,
                            keyListB[2].keyLength, primaryKeyB.getHash());

    EXPECT_EQ(STATUS_OK, removeStatus1);
    EXPECT_EQ(STATUS_OK, removeStatus2);
    EXPECT_EQ(STATUS_OK, removeStatus3);
    EXPECT_EQ(STATUS_OK, removeStatus4);

    responseBuffer.reset();
    ramcloud->lookupIndexKeys(dataTableId, 1, "a", 1, 0, "z", 1, 100,
                              &responseBuffer, &numHashes,
                              &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(0U, numHashes);

    responseBuffer.reset();
    ramcloud->lookupIndexKeys(dataTableId, 2, "a", 1, 0, "z", 1, 100,
                              &responseBuffer, &numHashes,
                              &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(0U, numHashes);
}

TEST_F(IndexletManagerTest, removeEntry_duplicate) {
    ramcloud->createIndex(dataTableId, 1, 0);

    im->insertEntry(dataTableId, 1, "air", 3, 1234);
    im->insertEntry(dataTableId, 1, "air", 3, 5678);
    im->insertEntry(dataTableId, 1, "air", 3, 9012);

    Status removeStatus = im->removeEntry(dataTableId, 1, "air", 3, 5678);
    EXPECT_EQ(STATUS_OK, removeStatus);

    ramcloud->lookupIndexKeys(dataTableId, 1, "air", 3, 0, "air", 3, 100,
                              &responseBuffer, &numHashes,
                              &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, WireFormat::getStatus(&responseBuffer));
    EXPECT_EQ(2U, numHashes);
    EXPECT_EQ(1234U, *responseBuffer.getOffset<uint64_t>(lookupOffset));
    EXPECT_EQ(9012U, *responseBuffer.getOffset<uint64_t>(lookupOffset + 8));
}

TEST_F(IndexletManagerTest, removeEntry_unknownIndexlet) {
    Status removeStatus = im->removeEntry(dataTableId, 1, "air", 3, 5678);
    EXPECT_EQ(STATUS_UNKNOWN_INDEXLET, removeStatus);
}

TEST_F(IndexletManagerTest, removeEntry_keyNotFound) {
    ramcloud->createIndex(dataTableId, 1, 0);

    Status removeStatus;

    // Remove a key when indexlet doesn't have any data.
    removeStatus = im->removeEntry(dataTableId, 1, "air", 3, 5678);
    EXPECT_EQ(STATUS_OK, removeStatus);

    // Remove key k1 when indexlet only contains k2.
    im->insertEntry(dataTableId, 1, "earth", 5, 9876);
    removeStatus = im->removeEntry(dataTableId, 1, "air", 3, 5678);
    EXPECT_EQ(STATUS_OK, removeStatus);
}

}  // namespace RAMCloud
