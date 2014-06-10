/* Copyright (c) 2011-2014 Stanford University
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
#include "MockCluster.h"
#include "RawMetrics.h"
#include "ServerMetrics.h"
#include "RamCloud.h"
#include "IndexLookup.h"

namespace RAMCloud {

class IndexLookupTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;

  public:
    IndexLookupTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , ramcloud()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master1";
        cluster.addServer(config);
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::BACKUP_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master2";
        cluster.addServer(config);
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::BACKUP_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master3";
        cluster.addServer(config);
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::BACKUP_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master4";
        cluster.addServer(config);
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::BACKUP_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master5";
        cluster.addServer(config);
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::BACKUP_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master6";
        cluster.addServer(config);
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::BACKUP_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master7";
        cluster.addServer(config);
        config.services = {WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=ping1";
        cluster.addServer(config);

        ramcloud.construct(&context, "mock:host=coordinator");
    }

    DISALLOW_COPY_AND_ASSIGN(IndexLookupTest);
};

/// Simple test: first write two objects, and then
/// look up one of them, by using IndexLookup class.
TEST_F(IndexLookupTest, getNext_simple) {
    uint64_t tableId = ramcloud->createTable("table");
    ramcloud->createIndex(tableId, 1, 0);
    ramcloud->createIndex(tableId, 2, 0);

    uint8_t numKeys = 3;

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

    ramcloud->write(tableId, numKeys, keyListA, "valueA");
    ramcloud->write(tableId, numKeys, keyListB, "valueB");

    IndexLookup indexLookup(ramcloud.get(), tableId, 1,
                            "keyA1", 5, 0, "keyA1", 5);
    EXPECT_TRUE(indexLookup.getNext());
    EXPECT_EQ(std::strncmp((const char*)keyListA[0].key,
                           (const char*)indexLookup.getKey(0), 5), 0);
    EXPECT_EQ(std::strncmp((const char*)keyListA[1].key,
                           (const char*)indexLookup.getKey(1), 5), 0);
    EXPECT_EQ(std::strncmp((const char*)keyListA[2].key,
                           (const char*)indexLookup.getKey(2), 5), 0);
}

/// Only a single indexlet is going to handle all lookupIndexKeys queries,
/// while the table is splited into several tablets.
/// This is to test if IndexLookup can head to the correct data server
/// to ask for objects.
TEST_F(IndexLookupTest, getNext_singleIndexlet) {
    uint32_t serverSpan = 6;
    uint64_t tableId = ramcloud->createTable("table", serverSpan);
    ramcloud->createIndex(tableId, 1, 0);
    ramcloud->createIndex(tableId, 2, 0);
    uint8_t numKey = 3;
    KeyInfo keyList[3];
    char keyStr[3][20];
    uint64_t numObjects = 199;
    for (uint64_t i = 100; i < numObjects; i++) {
        snprintf(keyStr[0], sizeof(keyStr[0]), "keyA%lu", i);
        keyList[0].key = keyStr[0];
        keyList[0].keyLength = 7;
        snprintf(keyStr[1], sizeof(keyStr[1]), "keyB%lu", i);
        keyList[1].key = keyStr[1];
        keyList[1].keyLength = 7;
        snprintf(keyStr[2], sizeof(keyStr[2]), "keyC%lu", i);
        keyList[2].key = keyStr[2];
        keyList[2].keyLength = 7;
        std::string value = "value";
        ramcloud->write(tableId, numKey, keyList, value.c_str());
    }

    IndexLookup indexLookup(ramcloud.get(), tableId, 1,
                            "key", 3, 0, "keyZ", 4);
    for (uint64_t i = 100; i < numObjects; i++) {
        snprintf(keyStr[0], sizeof(keyStr[0]), "keyA%lu", i);
        keyList[0].key = keyStr[0];
        keyList[0].keyLength = 7;
        snprintf(keyStr[1], sizeof(keyStr[1]), "keyB%lu", i);
        keyList[1].key = keyStr[1];
        keyList[1].keyLength = 7;
        snprintf(keyStr[2], sizeof(keyStr[2]), "keyC%lu", i);
        keyList[2].key = keyStr[2];
        keyList[2].keyLength = 7;
        EXPECT_TRUE(indexLookup.getNext());
        const void *retKey;
        KeyLength retKeyLength;
        for (KeyIndex idx = 0; idx < 3; idx ++) {
            retKey = indexLookup.getKey(idx, &retKeyLength);
            EXPECT_EQ(std::strncmp((const char *)retKey,
                                   (const char *)keyList[idx].key,
                                   retKeyLength), 0);
        }
    }
}

/// A single tablet is going to handle all indexedRead queires, while
/// the index is splited into several indexlets, and we generate objects
/// for each of the indexlets.
/// This is to test (a) if IndexLookup can build correct set of PKHashes among
/// different indexlets, and (b) if IndexLookup can return objects in the
/// correct index order.
TEST_F(IndexLookupTest, getNext_singleTablet) {
    uint8_t numIndexlets = 26;
    uint64_t tableId = ramcloud->createTable("table");
    ramcloud->createIndex(tableId, 1, 0, numIndexlets);
    ramcloud->createIndex(tableId, 2, 0, numIndexlets);
    uint8_t numKey = 3;
    KeyInfo keyList[3];
    char keyStr[3][20];
    uint64_t numObjects = 109;
    for (uint64_t i = 100; i < numObjects; i++)
        for (char prefix = 'a'; prefix < 'z'; prefix++) {
            snprintf(keyStr[0], sizeof(keyStr[0]), "%ckeyA%lu", prefix, i);
            keyList[0].key = keyStr[0];
            keyList[0].keyLength = 8;
            snprintf(keyStr[1], sizeof(keyStr[1]), "%ckeyB%lu", prefix, i);
            keyList[1].key = keyStr[1];
            keyList[1].keyLength = 8;
            snprintf(keyStr[2], sizeof(keyStr[2]), "%ckeyC%lu", prefix, i);
            keyList[2].key = keyStr[2];
            keyList[2].keyLength = 8;
            std::string value = "value";
            ramcloud->write(tableId, numKey, keyList, value.c_str());
        }

    IndexLookup indexLookup(ramcloud.get(), tableId, 1,
                            "a", 1, 0, "z", 1);
    for (char prefix = 'a'; prefix < 'z'; prefix++)
        for (uint64_t i = 100; i < numObjects; i++) {
            snprintf(keyStr[0], sizeof(keyStr[0]), "%ckeyA%lu", prefix, i);
            keyList[0].key = keyStr[0];
            keyList[0].keyLength = 8;
            snprintf(keyStr[1], sizeof(keyStr[1]), "%ckeyB%lu", prefix, i);
            keyList[1].key = keyStr[1];
            keyList[1].keyLength = 8;
            snprintf(keyStr[2], sizeof(keyStr[2]), "%ckeyC%lu", prefix, i);
            keyList[2].key = keyStr[2];
            keyList[2].keyLength = 8;
            EXPECT_TRUE(indexLookup.getNext());
            const void *retKey;
            KeyLength retKeyLength;
            for (KeyIndex idx = 0; idx < 3; idx ++) {
                retKey = indexLookup.getKey(idx, &retKeyLength);
                EXPECT_EQ(std::strncmp((const char *)retKey,
                                       (const char *)keyList[idx].key,
                                       retKeyLength), 0);
            }
        }
}

/// Comprehensive tests for IndexLookup. We split index and table into
/// several indexlets and tablets respectively. This is to test: (a) if
/// IndexLookup can build correct set of PK Hashes; (b) if IndexLookup
/// can head to correct tablet server to fetch objects; and (c) if IndexLookup
/// can return objects in index order.
TEST_F(IndexLookupTest, getNext_general) {
    uint32_t serverSpan = 10;
    uint8_t numIndexlets = 26;
    uint64_t tableId = ramcloud->createTable("table", serverSpan);
    ramcloud->createIndex(tableId, 1, 0, numIndexlets);
    ramcloud->createIndex(tableId, 2, 0, numIndexlets);
    uint8_t numKey = 3;
    KeyInfo keyList[3];
    char keyStr[3][20];
    uint64_t numObjects = 109;
    for (uint64_t i = 100; i < numObjects; i++)
        for (char prefix = 'a'; prefix < 'z'; prefix++) {
            snprintf(keyStr[0], sizeof(keyStr[0]), "%ckeyA%lu", prefix, i);
            keyList[0].key = keyStr[0];
            keyList[0].keyLength = 8;
            snprintf(keyStr[1], sizeof(keyStr[1]), "%ckeyB%lu", prefix, i);
            keyList[1].key = keyStr[1];
            keyList[1].keyLength = 8;
            snprintf(keyStr[2], sizeof(keyStr[2]), "%ckeyC%lu", prefix, i);
            keyList[2].key = keyStr[2];
            keyList[2].keyLength = 8;
            std::string value = "value";
            ramcloud->write(tableId, numKey, keyList, value.c_str());
        }

    IndexLookup indexLookup(ramcloud.get(), tableId, 1,
                            "a", 1, 0, "z", 1);
    for (char prefix = 'a'; prefix < 'z'; prefix++)
        for (uint64_t i = 100; i < numObjects; i++) {
            snprintf(keyStr[0], sizeof(keyStr[0]), "%ckeyA%lu", prefix, i);
            keyList[0].key = keyStr[0];
            keyList[0].keyLength = 8;
            snprintf(keyStr[1], sizeof(keyStr[1]), "%ckeyB%lu", prefix, i);
            keyList[1].key = keyStr[1];
            keyList[1].keyLength = 8;
            snprintf(keyStr[2], sizeof(keyStr[2]), "%ckeyC%lu", prefix, i);
            keyList[2].key = keyStr[2];
            keyList[2].keyLength = 8;
            EXPECT_TRUE(indexLookup.getNext());
            const void *retKey;
            KeyLength retKeyLength;
            for (KeyIndex idx = 0; idx < 3; idx ++) {
                retKey = indexLookup.getKey(idx, &retKeyLength);
                EXPECT_EQ(std::strncmp((const char *)retKey,
                                       (const char *)keyList[idx].key,
                                       retKeyLength), 0);
            }
        }
}


TEST_F(IndexLookupTest, getNext_recovery) {
    uint32_t serverSpan = 10;
    //KeyHash startKeyHash = 0;
    //KeyHash endKeyHash = ~0UL / serverSpan;
    uint8_t numIndexlets = 26;
    uint64_t tableId = ramcloud->createTable("table", serverSpan);
    ramcloud->createIndex(tableId, 1, 0, numIndexlets);
    ramcloud->createIndex(tableId, 2, 0, numIndexlets);
    uint8_t numKey = 3;
    KeyInfo keyList[3];
    char keyStr[3][20];
    uint64_t numObjects = 109;
    for (uint64_t i = 100; i < numObjects; i++)
        for (char prefix = 'a'; prefix < 'z'; prefix++) {
            snprintf(keyStr[0], sizeof(keyStr[0]), "%ckeyA%lu", prefix, i);
            keyList[0].key = keyStr[0];
            keyList[0].keyLength = 8;
            snprintf(keyStr[1], sizeof(keyStr[1]), "%ckeyB%lu", prefix, i);
            keyList[1].key = keyStr[1];
            keyList[1].keyLength = 8;
            snprintf(keyStr[2], sizeof(keyStr[2]), "%ckeyC%lu", prefix, i);
            keyList[2].key = keyStr[2];
            keyList[2].keyLength = 8;
            std::string value = "value";
            ramcloud->write(tableId, numKey, keyList, value.c_str());
        }

    IndexLookup indexLookup(ramcloud.get(), tableId, 1,
                            "a", 1, 0, "z", 1);
    for (char prefix = 'a'; prefix < 'z'; prefix++) {
        for (uint64_t i = 100; i < numObjects; i++) {
            snprintf(keyStr[0], sizeof(keyStr[0]), "%ckeyA%lu", prefix, i);
            keyList[0].key = keyStr[0];
            keyList[0].keyLength = 8;
            snprintf(keyStr[1], sizeof(keyStr[1]), "%ckeyB%lu", prefix, i);
            keyList[1].key = keyStr[1];
            keyList[1].keyLength = 8;
            snprintf(keyStr[2], sizeof(keyStr[2]), "%ckeyC%lu", prefix, i);
            keyList[2].key = keyStr[2];
            keyList[2].keyLength = 8;
            EXPECT_TRUE(indexLookup.getNext());
            const void *retKey;
            KeyLength retKeyLength;
            for (KeyIndex idx = 0; idx < 3; idx ++) {
                retKey = indexLookup.getKey(idx, &retKeyLength);
                EXPECT_EQ(std::strncmp((const char *)retKey,
                                       (const char *)keyList[idx].key,
                                       retKeyLength), 0);
            }
        }
    }
}
} // namespace ramcloud


