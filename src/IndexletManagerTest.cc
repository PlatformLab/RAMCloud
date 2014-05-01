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
#include "RamCloud.h"
#include "StringUtil.h"

namespace RAMCloud {

class IndexletManagerTest : public ::testing::Test {
  public:
    Context context;
    ServerId serverId;
    ServerList serverList;
    ServerConfig masterConfig;
    TabletManager tabletManager;
    MasterTableMetadata masterTableMetadata;
    ObjectManager objectManager;
    IndexletManager im;

    uint64_t indexletTableId;

    IndexletManagerTest()
        : context()
        , serverId(5)
        , serverList(&context)
        , masterConfig(ServerConfig::forTesting())
        , tabletManager()
        , masterTableMetadata()
        , objectManager(&context,
                        &serverId,
                        &masterConfig,
                        &tabletManager,
                        &masterTableMetadata)
        , im(&context, &objectManager)
        , indexletTableId(100)
    {
        tabletManager.addTablet(100, 0, ~0UL, TabletManager::NORMAL);
    }
    DISALLOW_COPY_AND_ASSIGN(IndexletManagerTest);
};

TEST_F(IndexletManagerTest, constructor) {
    EXPECT_EQ(0U, im.indexletMap.size());
}

TEST_F(IndexletManagerTest, addIndexlet) {
    string key1 = "a";
    string key2 = "c";
    string key3 = "f";
    string key4 = "k";
    string key5 = "u";

    EXPECT_TRUE(im.addIndexlet(0, 0, indexletTableId, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    // we need to create the table corresponding to each indexlet
    // (b-tree). There is one indexlet that is created as part
    // of the constructor of IndexletManagerTest
    tabletManager.addTablet(indexletTableId + 1, 0, ~0UL,
                            TabletManager::NORMAL);
    EXPECT_FALSE(im.addIndexlet(0, 0, indexletTableId + 1, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));


    // we need to create the table corresponding to each indexlet
    // (b-tree). There is one indexlet that is created as part
    // of the constructor of IndexletManagerTest
    tabletManager.addTablet(indexletTableId + 2, 0, ~0UL,
                            TabletManager::NORMAL);
    EXPECT_TRUE(im.addIndexlet(0, 0, indexletTableId + 2, key1.c_str(),
        (uint16_t)key1.length(), key2.c_str(), (uint16_t)key2.length()));


    // we need to create the table corresponding to each indexlet
    // (b-tree). There is one indexlet that is created as part
    // of the constructor of IndexletManagerTest
    tabletManager.addTablet(indexletTableId + 3, 0, ~0UL,
                            TabletManager::NORMAL);
    EXPECT_TRUE(im.addIndexlet(0, 0, indexletTableId + 3, key4.c_str(),
        (uint16_t)key4.length(), key5.c_str(), (uint16_t)key5.length()));


    // we need to create the table corresponding to each indexlet
    // (b-tree). There is one indexlet that is created as part
    // of the constructor of IndexletManagerTest
    tabletManager.addTablet(indexletTableId + 4, 0, ~0UL,
                            TabletManager::NORMAL);
    EXPECT_FALSE(im.addIndexlet(0, 0, indexletTableId + 4, key1.c_str(),
        (uint16_t)key1.length(), key3.c_str(), (uint16_t)key3.length()));

    std::mutex indexletMapMutex;
    IndexletManager::Lock fakeGuard(indexletMapMutex);
    IndexletManager::Indexlet* indexlet = &im.lookupIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), fakeGuard)->second;
    string firstKey = StringUtil::binaryToString(
                        indexlet->firstKey, indexlet->firstKeyLength);
    string firstNotOwnedKey = StringUtil::binaryToString(
                indexlet->firstNotOwnedKey, indexlet->firstNotOwnedKeyLength);

    EXPECT_EQ(0, firstKey.compare("c"));
    EXPECT_EQ(0, firstNotOwnedKey.compare("k"));
}

TEST_F(IndexletManagerTest, getIndexlet) {
    string key1 = "a";
    string key2 = "c";
    string key3 = "f";
    string key4 = "k";
    string key5 = "u";

    //TODO(ashgup): add comments on each individual test
    EXPECT_FALSE(im.getIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    im.addIndexlet(0, 0, indexletTableId, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length());

    EXPECT_TRUE(im.getIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_FALSE(im.getIndexlet(1, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_FALSE(im.getIndexlet(0, 1, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_FALSE(im.getIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key3.c_str(), (uint16_t)key3.length()));

    EXPECT_FALSE(im.getIndexlet(0, 0, key3.c_str(),
        (uint16_t)key3.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_FALSE(im.getIndexlet(0, 0, key1.c_str(),
        (uint16_t)key1.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_FALSE(im.getIndexlet(0, 1, key2.c_str(),
        (uint16_t)key2.length(), key5.c_str(), (uint16_t)key5.length()));

    EXPECT_FALSE(im.getIndexlet(0, 1, key1.c_str(),
        (uint16_t)key1.length(), key5.c_str(), (uint16_t)key5.length()));

    IndexletManager::Indexlet* indexlet =
                    im.getIndexlet(0, 0, key2.c_str(), (uint16_t)key2.length(),
                                        key4.c_str(), (uint16_t)key4.length());
    EXPECT_TRUE(indexlet);
    string firstKey = StringUtil::binaryToString(
                        indexlet->firstKey, indexlet->firstKeyLength);
    string firstNotOwnedKey = StringUtil::binaryToString(
                indexlet->firstNotOwnedKey, indexlet->firstNotOwnedKeyLength);

    EXPECT_EQ(0, firstKey.compare("c"));
    EXPECT_EQ(0, firstNotOwnedKey.compare("k"));
}

TEST_F(IndexletManagerTest, deleteIndexlet) {
    string key1 = "a";
    string key2 = "c";
    string key3 = "f";
    string key4 = "k";
    string key5 = "u";

    //TODO(ashgup): add comments on each individual test
    EXPECT_FALSE(im.getIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    im.addIndexlet(0, 0, indexletTableId, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length());

    EXPECT_TRUE(im.getIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_TRUE(im.deleteIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_FALSE(im.getIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_FALSE(im.deleteIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    // we need to create the table corresponding to each indexlet
    // (b-tree). There is one indexlet that is created as part
    // of the constructor of IndexletManagerTest
    tabletManager.addTablet(indexletTableId + 1, 0, ~0UL,
                            TabletManager::NORMAL);
    im.addIndexlet(0, 0, indexletTableId, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length());

    EXPECT_FALSE(im.deleteIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key5.c_str(), (uint16_t)key5.length()));

    EXPECT_FALSE(im.deleteIndexlet(0, 0, key1.c_str(),
        (uint16_t)key1.length(), key4.c_str(), (uint16_t)key4.length()));
}

// TODO(ashgup): Add unit tests for functions that currently don't have them:
// getCount, lookupIndexlet.

TEST_F(IndexletManagerTest, insertIndexEntry) {
    im.addIndexlet(0, 0, indexletTableId, "a", 1, "k", 1);

    Status insertStatus1 = im.insertEntry(0, 0, "air", 3, 5678);
    EXPECT_EQ(STATUS_OK, insertStatus1);
    Status insertStatus2 = im.insertEntry(0, 0, "earth", 5, 9876);
    EXPECT_EQ(STATUS_OK, insertStatus2);

    Buffer responseBuffer;
    uint32_t numHashes;
    uint16_t nextKeyLength;
    uint64_t nextKeyHash;

    im.lookupIndexKeys(0, 0, "air", 3, 0, "air", 3, 100,
                       &responseBuffer, &numHashes,
                       &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(5678U, *responseBuffer.getStart<uint64_t>());
}

TEST_F(IndexletManagerTest, insertIndexEntry_stress) {
    im.addIndexlet(0, 0, indexletTableId, "a", 1, "z", 1);
    for (int i=0; i< 100000; i++){

        char primaryKey[30];
        snprintf(primaryKey, sizeof(primaryKey), "%dp%0*d", i, 30, 0);

        char secondaryKey[30];
        snprintf(secondaryKey, sizeof(secondaryKey), "b%ds%0*d", i, 30, 0);
        Key pk(0, primaryKey, 30);

        Status insertStatus1 = im.insertEntry(0, 0, secondaryKey, 
                                                30, pk.getHash());
        EXPECT_EQ(STATUS_OK, insertStatus1);

        insertStatus1 = im.insertEntry(0, 0, secondaryKey,
                                       30, pk.getHash());
        EXPECT_EQ(STATUS_OK, insertStatus1);

        insertStatus1 = im.insertEntry(0, 0, secondaryKey,
                                       30, pk.getHash());
        EXPECT_EQ(STATUS_OK, insertStatus1);

        Buffer lookupResp;
        uint32_t numHashes;
        uint16_t nextKeyLength;
        uint64_t nextKeyHash;
        im.lookupIndexKeys(0, 0, secondaryKey, 30, 0, secondaryKey, 30, 100,
            &lookupResp, &numHashes, &nextKeyLength, &nextKeyHash);

        EXPECT_EQ(numHashes, 3U);
        EXPECT_EQ(pk.getHash(), *lookupResp.getStart<uint64_t>());
    }
}

TEST_F(IndexletManagerTest, insertIndexEntry_unknownIndexlet) {
    im.addIndexlet(0, 0, indexletTableId, "a", 1, "k", 1);

    Status insertStatus = im.insertEntry(0, 0, "water", 5, 1234);
    EXPECT_EQ(STATUS_UNKNOWN_INDEXLET, insertStatus);
}

TEST_F(IndexletManagerTest, insertIndexEntry_duplicate) {
    im.addIndexlet(0, 0, indexletTableId, "a", 1, "k", 1);

    Status insertStatus1 = im.insertEntry(0, 0, "air", 3, 1234);
    EXPECT_EQ(STATUS_OK, insertStatus1);
    Status insertStatus2 = im.insertEntry(0, 0, "air", 3, 5678);
    EXPECT_EQ(STATUS_OK, insertStatus2);

    // Lookup for duplicates is tested in lookIndexKeys_duplicate.
}

TEST_F(IndexletManagerTest, lookupIndexKeys_unknownIndexlet) {
    Buffer responseBuffer;
    uint32_t numHashes;
    uint16_t nextKeyLength;
    uint64_t nextKeyHash;

    Status lookupStatus = im.lookupIndexKeys(0, 0, "water", 5, 0, "water", 5,
                                             100, &responseBuffer, &numHashes,
                                             &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_UNKNOWN_INDEXLET, lookupStatus);
}

TEST_F(IndexletManagerTest, lookupIndexKeys_keyNotFound) {
    im.addIndexlet(0, 0, indexletTableId, "a", 1, "k", 1);

    Buffer responseBuffer;
    uint32_t numHashes;
    uint16_t nextKeyLength;
    uint64_t nextKeyHash;
    Status lookupStatus;

    // Lookup a key when indexlet doesn't have any data.
    lookupStatus = im.lookupIndexKeys(0, 0, "air", 3, 0, "air", 3, 100,
                                      &responseBuffer, &numHashes,
                                      &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, lookupStatus);
    EXPECT_EQ(0U, numHashes);

    // Lookup a key k1 when indexlet doesn't have k1 but has k2.
    im.insertEntry(0, 0, "earth", 5, 9876);

    lookupStatus = im.lookupIndexKeys(0, 0, "air", 3, 0, "air", 3, 100,
                                      &responseBuffer, &numHashes,
                                      &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, lookupStatus);
    EXPECT_EQ(0U, numHashes);
}

TEST_F(IndexletManagerTest, lookupIndexKeys_single) {
    im.addIndexlet(0, 0, indexletTableId, "a", 1, "k", 1);

    Buffer responseBuffer;
    uint32_t numHashes;
    uint16_t nextKeyLength;
    uint64_t nextKeyHash;
    Status lookupStatus;

    // Lookup such that the result should be a single object when only
    // one object exists in the indexlet.
    im.insertEntry(0, 0, "air", 3, 5678);

    // First key = key = last key
    lookupStatus = im.lookupIndexKeys(0, 0, "air", 3, 0, "air", 3, 100,
                                      &responseBuffer, &numHashes,
                                      &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, lookupStatus);
    EXPECT_EQ(1U, numHashes);
    EXPECT_EQ(5678U, *responseBuffer.getStart<uint64_t>());

    // First key < key < last key
    responseBuffer.reset();
    lookupStatus = im.lookupIndexKeys(0, 0, "a", 1, 0, "c", 1, 100,
                                      &responseBuffer, &numHashes,
                                      &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, lookupStatus);
    EXPECT_EQ(1U, numHashes);
    EXPECT_EQ(5678U, *responseBuffer.getStart<uint64_t>());


    // Lookup such that the result should be a single object when
    // multiple objects exist in the indexlet.
    im.insertEntry(0, 0, "earth", 5, 9876);

    responseBuffer.reset();
    lookupStatus = im.lookupIndexKeys(0, 0, "a", 1, 0, "c", 1, 100,
                                      &responseBuffer, &numHashes,
                                      &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, lookupStatus);
    EXPECT_EQ(1U, numHashes);
    EXPECT_EQ(5678U, *responseBuffer.getStart<uint64_t>());
}

TEST_F(IndexletManagerTest, lookupIndexKeys_multiple) {
    im.addIndexlet(0, 0, indexletTableId, "a", 1, "k", 1);

    im.insertEntry(0, 0, "air", 3, 5678);
    im.insertEntry(0, 0, "earth", 5, 9876);
    im.insertEntry(0, 0, "fire", 4, 5432);

    Buffer responseBuffer;
    uint32_t numHashes;
    uint16_t nextKeyLength;
    uint64_t nextKeyHash;
    Status lookupStatus;

    // Point lookup only for first entry.
    responseBuffer.reset();
    lookupStatus = im.lookupIndexKeys(0, 0, "air", 3, 0, "air", 3, 100,
                                      &responseBuffer, &numHashes,
                                      &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, lookupStatus);
    EXPECT_EQ(1U, numHashes);
    EXPECT_EQ(5678U, *responseBuffer.getOffset<uint64_t>(0));

    // Point lookup only for second entry.
    responseBuffer.reset();
    lookupStatus = im.lookupIndexKeys(0, 0, "earth", 5, 0, "earth", 5, 100,
                                      &responseBuffer, &numHashes,
                                      &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, lookupStatus);
    EXPECT_EQ(1U, numHashes);
    EXPECT_EQ(9876U, *responseBuffer.getOffset<uint64_t>(0));

    // Point lookup only for third entry.
    responseBuffer.reset();
    lookupStatus = im.lookupIndexKeys(0, 0, "fire", 4, 0, "fire", 4, 100,
                                      &responseBuffer, &numHashes,
                                      &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, lookupStatus);
    EXPECT_EQ(1U, numHashes);
    EXPECT_EQ(5432U, *responseBuffer.getOffset<uint64_t>(0));

    // Range lookup for all entries such that:
    // first key = lowest expected key < highest expected key = last key
    responseBuffer.reset();
    lookupStatus = im.lookupIndexKeys(0, 0, "air", 3, 0, "fire", 4, 100,
                                      &responseBuffer, &numHashes,
                                      &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, lookupStatus);
    EXPECT_EQ(3U, numHashes);
    EXPECT_EQ(5678U, *responseBuffer.getOffset<uint64_t>(0));
    EXPECT_EQ(9876U, *responseBuffer.getOffset<uint64_t>(8));
    EXPECT_EQ(5432U, *responseBuffer.getOffset<uint64_t>(16));

    // Range lookup for all entries such that:
    // first key < lowest expected key < highest expected key < last key
    responseBuffer.reset();
    lookupStatus = im.lookupIndexKeys(0, 0, "a", 1, 0, "g", 1, 100,
                                      &responseBuffer, &numHashes,
                                      &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, lookupStatus);
    EXPECT_EQ(3U, numHashes);
    EXPECT_EQ(5678U, *responseBuffer.getOffset<uint64_t>(0));
    EXPECT_EQ(9876U, *responseBuffer.getOffset<uint64_t>(8));
    EXPECT_EQ(5432U, *responseBuffer.getOffset<uint64_t>(16));
}

TEST_F(IndexletManagerTest, lookupIndexKeys_duplicate) {
    im.addIndexlet(0, 0, indexletTableId, "a", 1, "k", 1);

    im.insertEntry(0, 0, "air", 3, 1234);
    im.insertEntry(0, 0, "air", 3, 5678);

    Buffer responseBuffer;
    uint32_t numHashes;
    uint16_t nextKeyLength;
    uint64_t nextKeyHash;

    Status lookupStatus = im.lookupIndexKeys(0, 0, "air", 3, 0, "air", 3, 100,
                                             &responseBuffer, &numHashes,
                                             &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, lookupStatus);
    EXPECT_EQ(2U, numHashes);
    EXPECT_EQ(1234U, *responseBuffer.getOffset<uint64_t>(0));
    EXPECT_EQ(5678U, *responseBuffer.getOffset<uint64_t>(8));
}

TEST_F(IndexletManagerTest, lookupIndexKeys_firstAllowedKeyHash) {
    im.addIndexlet(0, 0, indexletTableId, "a", 1, "k", 1);

    im.insertEntry(0, 0, "air", 3, 1234);
    im.insertEntry(0, 0, "air", 3, 5678);
    im.insertEntry(0, 0, "air", 3, 9012);

    Buffer responseBuffer;
    uint32_t numHashes;
    uint16_t nextKeyLength;
    uint64_t nextKeyHash;
    Status lookupStatus;

    // firstAllowedKeyHash is between the key hashes.
    lookupStatus = im.lookupIndexKeys(0, 0, "air", 3,
                                      2000 /*firstAllowedKeyHash*/,
                                      "air", 3, 100,
                                      &responseBuffer, &numHashes,
                                      &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, lookupStatus);
    EXPECT_EQ(2U, numHashes);
    EXPECT_EQ(5678U, *responseBuffer.getOffset<uint64_t>(0));
    EXPECT_EQ(9012U, *responseBuffer.getOffset<uint64_t>(8));

    // firstAllowedKeyHash is equal to one of the key hashes.
    responseBuffer.reset();
    lookupStatus = im.lookupIndexKeys(0, 0, "air", 3,
                                      5678 /*firstAllowedKeyHash*/,
                                      "air", 3, 100,
                                      &responseBuffer, &numHashes,
                                      &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, lookupStatus);
    EXPECT_EQ(2U, numHashes);
    EXPECT_EQ(5678U, *responseBuffer.getOffset<uint64_t>(0));
    EXPECT_EQ(9012U, *responseBuffer.getOffset<uint64_t>(8));
}

TEST_F(IndexletManagerTest, lookupIndexKeys_largerRange) {
    // Lookup such that the range of keys in the lookup request is larger than
    // the range of keys owned by this indexlet.

    im.addIndexlet(0, 0, indexletTableId, "a", 1, "kext", 4);

    im.insertEntry(0, 0, "air", 3, 5678);

    Buffer responseBuffer;
    uint32_t numHashes;
    uint16_t nextKeyLength;
    uint64_t nextKeyHash;

    Status lookupStatus = im.lookupIndexKeys(0, 0, "a", 1, 0, "z", 1, 100,
                                             &responseBuffer, &numHashes,
                                             &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, lookupStatus);
    EXPECT_EQ(1U, numHashes);
    EXPECT_EQ(5678U, *responseBuffer.getStart<uint64_t>());
    EXPECT_EQ(4U, nextKeyLength);
    EXPECT_EQ("kext", string(reinterpret_cast<const char*>(
            responseBuffer.getRange(8, nextKeyLength)), nextKeyLength));
    EXPECT_EQ(0U, nextKeyHash);
}

TEST_F(IndexletManagerTest, lookupIndexKeys_largerThanMax) {
    // Lookup such that the number of key hashes that would be returned
    // is larger than the maximum allowed.
    // We can set the max to be a small number here; In real operation, it
    // will typically be the maximum that can fit in an RPC, and will
    // be set by the MasterService.

    im.addIndexlet(0, 0, indexletTableId, "a", 1, "k", 1);

    im.insertEntry(0, 0, "air", 3, 5678);
    im.insertEntry(0, 0, "earth", 5, 9876);
    im.insertEntry(0, 0, "fire", 4, 5432);

    Buffer responseBuffer;
    uint32_t numHashes;
    uint16_t nextKeyLength;
    uint64_t nextKeyHash;

    Status lookupStatus = im.lookupIndexKeys(0, 0, "a", 1, 0, "g", 1, 2,
                                             &responseBuffer, &numHashes,
                                             &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, lookupStatus);
    EXPECT_EQ(2U, numHashes);
    EXPECT_EQ(5678U, *responseBuffer.getOffset<uint64_t>(0));
    EXPECT_EQ(9876U, *responseBuffer.getOffset<uint64_t>(8));
    EXPECT_EQ(4U, nextKeyLength);
    EXPECT_EQ("fire", string(reinterpret_cast<const char*>(
            responseBuffer.getRange(16, nextKeyLength)), nextKeyLength));
    EXPECT_EQ(5432U, nextKeyHash);
}

TEST_F(IndexletManagerTest, removeEntry_single) {
    im.addIndexlet(0, 0, indexletTableId, "a", 1, "k", 1);

    im.insertEntry(0, 0, "air", 3, 5678);

    Status removeStatus = im.removeEntry(0, 0, "air", 3, 5678);
    EXPECT_EQ(STATUS_OK, removeStatus);

    Buffer responseBuffer;
    uint32_t numHashes;
    uint16_t nextKeyLength;
    uint64_t nextKeyHash;
    im.lookupIndexKeys(0, 0, "air", 3, 0, "air", 3, 100,
                       &responseBuffer, &numHashes,
                       &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(0U, numHashes);
}

TEST_F(IndexletManagerTest, removeEntry_multipleEntries) {
    im.addIndexlet(0, 0, indexletTableId, "a", 1, "k", 1);

    Buffer responseBuffer;
    uint32_t numHashes;
    uint16_t nextKeyLength;
    uint64_t nextKeyHash;

    im.insertEntry(0, 0, "air", 3, 5678);
    im.insertEntry(0, 0, "earth", 5, 9876);
    im.insertEntry(0, 0, "fire", 4, 5432);

    Status removeStatus1 = im.removeEntry(0, 0, "air", 3, 5678);
    Status removeStatus2 = im.removeEntry(0, 0, "fire", 4, 5432);
    EXPECT_EQ(STATUS_OK, removeStatus1);
    EXPECT_EQ(STATUS_OK, removeStatus2);

    responseBuffer.reset();
    im.lookupIndexKeys(0, 0, "a", 1, 0, "h", 1, 100,
                       &responseBuffer, &numHashes,
                       &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(1U, numHashes);
    EXPECT_EQ(9876U, *responseBuffer.getOffset<uint64_t>(0));
}

TEST_F(IndexletManagerTest, removeEntry_multipleIndexlets) {
    uint64_t tableId = 4;
    im.addIndexlet(tableId, 1, indexletTableId, "", 0, "", 0);

    // we need to create the table corresponding to each indexlet
    // (b-tree). There is one indexlet that is created as part
    // of the constructor of IndexletManagerTest
    tabletManager.addTablet(indexletTableId + 1, 0, ~0UL,
                            TabletManager::NORMAL);
    im.addIndexlet(tableId, 2, indexletTableId + 1, "", 0, "", 0);

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

    Key primaryKeyA(tableId, keyListA[0].key, keyListA[0].keyLength);
    Key primaryKeyB(tableId, keyListB[0].key, keyListB[0].keyLength);

    im.insertEntry(tableId, 1, keyListA[1].key, keyListA[1].keyLength,
                           primaryKeyA.getHash());
    im.insertEntry(tableId, 2, keyListA[2].key, keyListA[2].keyLength,
                           primaryKeyA.getHash());
    im.insertEntry(tableId, 1, keyListB[1].key, keyListB[1].keyLength,
                           primaryKeyB.getHash());
    im.insertEntry(tableId, 2, keyListB[2].key, keyListB[2].keyLength,
                           primaryKeyB.getHash());

    Status removeStatus1 =
            im.removeEntry(tableId, 1, keyListA[1].key, keyListA[1].keyLength,
                           primaryKeyA.getHash());
    Status removeStatus2 =
            im.removeEntry(tableId, 2, keyListA[2].key, keyListA[2].keyLength,
                           primaryKeyA.getHash());
    Status removeStatus3 =
            im.removeEntry(tableId, 1, keyListB[1].key, keyListB[1].keyLength,
                           primaryKeyB.getHash());
    Status removeStatus4 =
            im.removeEntry(tableId, 2, keyListB[2].key, keyListB[2].keyLength,
                           primaryKeyB.getHash());

    EXPECT_EQ(STATUS_OK, removeStatus1);
    EXPECT_EQ(STATUS_OK, removeStatus2);
    EXPECT_EQ(STATUS_OK, removeStatus3);
    EXPECT_EQ(STATUS_OK, removeStatus4);

    Buffer responseBuffer;
    uint32_t numHashes;
    uint16_t nextKeyLength;
    uint64_t nextKeyHash;

    responseBuffer.reset();
    im.lookupIndexKeys(tableId, 1, "a", 1, 0, "z", 1, 100,
                       &responseBuffer, &numHashes,
                       &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(0U, numHashes);

    responseBuffer.reset();
    im.lookupIndexKeys(tableId, 2, "a", 1, 0, "z", 1, 100,
                       &responseBuffer, &numHashes,
                       &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(0U, numHashes);
}

TEST_F(IndexletManagerTest, removeEntry_duplicate) {
    im.addIndexlet(0, 0, indexletTableId, "a", 1, "k", 1);

    im.insertEntry(0, 0, "air", 3, 1234);
    im.insertEntry(0, 0, "air", 3, 5678);
    im.insertEntry(0, 0, "air", 3, 9012);

    Status removeStatus = im.removeEntry(0, 0, "air", 3, 5678);
    EXPECT_EQ(STATUS_OK, removeStatus);

    Buffer responseBuffer;
    uint32_t numHashes;
    uint16_t nextKeyLength;
    uint64_t nextKeyHash;
    Status lookupStatus = im.lookupIndexKeys(0, 0, "air", 3, 0, "air", 3, 100,
                                             &responseBuffer, &numHashes,
                                             &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(STATUS_OK, lookupStatus);
    EXPECT_EQ(2U, numHashes);
    EXPECT_EQ(1234U, *responseBuffer.getOffset<uint64_t>(0));
    EXPECT_EQ(9012U, *responseBuffer.getOffset<uint64_t>(8));
}

TEST_F(IndexletManagerTest, removeEntry_unknownIndexlet) {
    Status removeStatus = im.removeEntry(0, 0, "air", 3, 5678);
    EXPECT_EQ(STATUS_UNKNOWN_INDEXLET, removeStatus);
}

TEST_F(IndexletManagerTest, removeEntry_keyNotFound) {
    im.addIndexlet(0, 0, indexletTableId, "a", 1, "k", 1);

    Status removeStatus;

    // Remove a key when indexlet doesn't have any data.
    removeStatus = im.removeEntry(0, 0, "air", 3, 5678);
    EXPECT_EQ(STATUS_OK, removeStatus);

    // Remove key k1 when indexlet only contains k2.
    im.insertEntry(0, 0, "earth", 5, 9876);
    removeStatus = im.removeEntry(0, 0, "air", 3, 5678);
    EXPECT_EQ(STATUS_OK, removeStatus);
}

TEST_F(IndexletManagerTest, isKeyInRange)
{
    // Construct Object obj.
    uint64_t tableId = 1;
    uint8_t numKeys = 3;

    KeyInfo keyList[3];
    keyList[0].keyLength = 8;
    keyList[0].key = "objkey0";
    keyList[1].keyLength = 8;
    keyList[1].key = "objkey1";
    keyList[2].keyLength = 8;
    keyList[2].key = "objkey2";

    const void* value = "objvalue";
    uint32_t valueLength = 9;

    Buffer keysAndValueBuffer;
    Object::appendKeysAndValueToBuffer(tableId, numKeys, keyList,
                                       value, valueLength, keysAndValueBuffer);

    Object obj(tableId, 1, 0, keysAndValueBuffer);

    // Note: If isKeyInRange() didn't use keyCompare() then we'd have to
    // test many more cases here.

    // Case0: firstKey > key < lastKey
    IndexKeyRange testRange0 = {1, "objkey2", 8, "objkey2", 8};
    bool isInRange0 = IndexletManager::isKeyInRange(&obj, &testRange0);
    EXPECT_FALSE(isInRange0);

    // Case1: firstKey < key > lastKey
    IndexKeyRange testRange1 = {1, "objkey0", 8, "objkey0", 8};
    bool isInRange1 = IndexletManager::isKeyInRange(&obj, &testRange1);
    EXPECT_FALSE(isInRange1);

    // Case2: firstKey > key > lastKey
    IndexKeyRange testRange2 = {1, "objkey2", 8, "objkey0", 8};
    bool isInRange2 = IndexletManager::isKeyInRange(&obj, &testRange2);
    EXPECT_FALSE(isInRange2);

    // Case3: firstKey < key < lastKey
    IndexKeyRange testRange3 = {1, "objkey0", 8, "objkey2", 8};
    bool isInRange3 = IndexletManager::isKeyInRange(&obj, &testRange3);
    EXPECT_TRUE(isInRange3);

    // Case4: firstKey = key = lastKey
    IndexKeyRange testRange4 = {1, "objkey1", 8, "objkey1", 8};
    bool isInRange4 = IndexletManager::isKeyInRange(&obj, &testRange4);
    EXPECT_TRUE(isInRange4);
}

TEST_F(IndexletManagerTest, keyCompare)
{
    EXPECT_EQ(0, IndexletManager::keyCompare("abc", 3, "abc", 3));
    EXPECT_GT(0, IndexletManager::keyCompare("abb", 3, "abc", 3));
    EXPECT_LT(0, IndexletManager::keyCompare("abd", 3, "abc", 3));
    EXPECT_GT(0, IndexletManager::keyCompare("ab", 2, "abc", 3));
    EXPECT_LT(0, IndexletManager::keyCompare("abcd", 4, "abc", 3));
    EXPECT_GT(0, IndexletManager::keyCompare("abbc", 4, "abc", 3));
    EXPECT_LT(0, IndexletManager::keyCompare("ac", 2, "abc", 3));
    EXPECT_GT(0, IndexletManager::keyCompare("", 0, "abc", 3));
    EXPECT_LT(0, IndexletManager::keyCompare("abc", 3, "", 0));
}

}  // namespace RAMCloud
