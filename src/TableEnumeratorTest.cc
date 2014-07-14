/* Copyright (c) 2012 Stanford University
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
#include "TableEnumerator.h"

namespace RAMCloud {

class TableEnumeratorTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    RamCloud ramcloud;
    uint64_t tableId1;

  public:
    TableEnumeratorTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , ramcloud(&context, "mock:host=coordinator")
        , tableId1(-1)
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master1";
        cluster.addServer(config);
        config.localLocator = "mock:host=master2";
        cluster.addServer(config);

        tableId1 = ramcloud.createTable("table1", 2);
    }

    DISALLOW_COPY_AND_ASSIGN(TableEnumeratorTest);
};

TEST_F(TableEnumeratorTest, basics) {
    uint64_t version0, version1, version2, version3, version4;
    ramcloud.write(tableId1, "0", 1, "abcdef", 6, NULL, &version0);
    ramcloud.write(tableId1, "1", 1, "ghijkl", 6, NULL, &version1);
    ramcloud.write(tableId1, "2", 1, "mnopqr", 6, NULL, &version2);
    ramcloud.write(tableId1, "3", 1, "stuvwx", 6, NULL, &version3);
    ramcloud.write(tableId1, "4", 1, "yzabcd", 6, NULL, &version4);

    uint32_t size = 0;
    const void* buffer = 0;
    TableEnumerator iter(ramcloud, tableId1, false);
    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);

    // First object.
    Object object1(buffer, size);
    EXPECT_EQ(34U, size);                                       // size
    EXPECT_EQ(tableId1, object1.getTableId());                  // table ID
    EXPECT_EQ(1U, object1.getKeyLength());                      // key length
    EXPECT_EQ(version0, object1.getVersion());                  // version
    EXPECT_EQ("0", string(reinterpret_cast<const char*>(
                   object1.getKey()), 1));                      // key
    EXPECT_EQ("abcdef", string(reinterpret_cast<const char*>    // value
        (object1.getValue()), 6));

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);

    // Second object.
    Object object2(buffer, size);
    EXPECT_EQ(34U, size);                                       // size
    EXPECT_EQ(tableId1, object2.getTableId());                  // table ID
    EXPECT_EQ(1U, object2.getKeyLength());                      // key length
    EXPECT_EQ(version4, object2.getVersion());                  // version
    EXPECT_EQ("4", string(reinterpret_cast<const char*>(
                   object2.getKey()), 1));                      // key
    EXPECT_EQ("yzabcd", string(reinterpret_cast<const char*>    // value
        (object2.getValue()), 6));

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);

    // Third object.
    Object object3(buffer, size);
    EXPECT_EQ(34U, size);                                       // size
    EXPECT_EQ(tableId1, object3.getTableId());                  // table ID
    EXPECT_EQ(1U, object3.getKeyLength());                      // key length
    EXPECT_EQ(version2, object3.getVersion());                  // version
    EXPECT_EQ("2", string(reinterpret_cast<const char*>(
                   object3.getKey()), 1));                      // key
    EXPECT_EQ("mnopqr", string(reinterpret_cast<const char*>    // value
        (object3.getValue()), 6));

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);

    // Fourth object.
    Object object4(buffer, size);
    EXPECT_EQ(34U, size);                                       // size
    EXPECT_EQ(tableId1, object4.getTableId());                  // table ID
    EXPECT_EQ(1U, object4.getKeyLength());                      // key length
    EXPECT_EQ(version1, object4.getVersion());                  // version
    EXPECT_EQ("1", string(reinterpret_cast<const char*>(
                   object4.getKey()), 1));                      // key
    EXPECT_EQ("ghijkl", string(reinterpret_cast<const char*>    // value
        (object4.getValue()), 6));

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);

    // Fifth object.
    Object object5(buffer, size);
    EXPECT_EQ(34U, size);                                       // size
    EXPECT_EQ(tableId1, object5.getTableId());                  // table ID
    EXPECT_EQ(1U, object5.getKeyLength());                      // key length
    EXPECT_EQ(version3, object5.getVersion());                  // version
    EXPECT_EQ("3", string(reinterpret_cast<const char*>(
                   object5.getKey()), 1));                      // key
    EXPECT_EQ("stuvwx", string(reinterpret_cast<const char*>    // value
        (object5.getValue()), 6));

    EXPECT_FALSE(iter.hasNext());
}

TEST_F(TableEnumeratorTest, rpcOverflow) {
    uint32_t dataSize(1024 * 32);
    char data [dataSize];
    uint32_t totalObjects(1024);
    uint64_t version [totalObjects];
    for (uint32_t i = 0; i < totalObjects; i++) {
        ramcloud.write(tableId1, &i, 4, data, dataSize, NULL, &version[i]);
    }

    uint32_t size = 0;
    const void* buffer = 0;
    TableEnumerator iter(ramcloud, tableId1, false);

    uint32_t count = 0;
    while (iter.hasNext()) {
        iter.next(&size, &buffer);
        count++;
        Object object(buffer, size);
        EXPECT_EQ(tableId1, object.getTableId());
        EXPECT_EQ(4U, object.getKeyLength());
        EXPECT_EQ(dataSize, object.getValueLength());
    }

    EXPECT_EQ(totalObjects, count);

    for (uint32_t i = 0; i < totalObjects; i++) {
        ramcloud.remove(tableId1, &i, 4);
    }
}

TEST_F(TableEnumeratorTest, keysOnly) {
    uint64_t version0, version1, version2, version3, version4;
    ramcloud.write(tableId1, "0", 1, "abcdef", 6, NULL, &version0);
    ramcloud.write(tableId1, "1", 1, "ghijkl", 6, NULL, &version1);
    ramcloud.write(tableId1, "2", 1, "mnopqr", 6, NULL, &version2);
    ramcloud.write(tableId1, "3", 1, "stuvwx", 6, NULL, &version3);
    ramcloud.write(tableId1, "4", 1, "yzabcd", 6, NULL, &version4);

    uint32_t size = 0;
    const void* buffer = 0;
    TableEnumerator iter(ramcloud, tableId1, true);
    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);

    // First object.
    Object object1(buffer, size);
    EXPECT_EQ(28U, size);                                       // size
    EXPECT_EQ(tableId1, object1.getTableId());                  // table ID
    EXPECT_EQ(1U, object1.getKeyLength());                      // key length
    EXPECT_EQ(version0, object1.getVersion());                  // version
    EXPECT_EQ("0", string(reinterpret_cast<const char*>(
                   object1.getKey()), 1));                      // key
    uint32_t dataLength;
    object1.getValue(&dataLength);
    EXPECT_EQ(0U, dataLength);                           // data length

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);

    // Second object.
    Object object2(buffer, size);
    EXPECT_EQ(28U, size);                                       // size
    EXPECT_EQ(tableId1, object2.getTableId());                  // table ID
    EXPECT_EQ(1U, object2.getKeyLength());                      // key length
    EXPECT_EQ(version4, object2.getVersion());                  // version
    EXPECT_EQ("4", string(reinterpret_cast<const char*>(
                   object2.getKey()), 1));                      // key
    object2.getValue(&dataLength);
    EXPECT_EQ(0U, dataLength);                           // data length

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);

    // Third object.
    Object object3(buffer, size);
    EXPECT_EQ(28U, size);                                       // size
    EXPECT_EQ(tableId1, object3.getTableId());                  // table ID
    EXPECT_EQ(1U, object3.getKeyLength());                      // key length
    EXPECT_EQ(version2, object3.getVersion());                  // version
    EXPECT_EQ("2", string(reinterpret_cast<const char*>(
                   object3.getKey()), 1));                      // key
    object3.getValue(&dataLength);
    EXPECT_EQ(0U, dataLength);                           // data length

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);

    // Fourth object.
    Object object4(buffer, size);
    EXPECT_EQ(28U, size);                                       // size
    EXPECT_EQ(tableId1, object4.getTableId());                  // table ID
    EXPECT_EQ(1U, object4.getKeyLength());                      // key length
    EXPECT_EQ(version1, object4.getVersion());                  // version
    EXPECT_EQ("1", string(reinterpret_cast<const char*>(
                   object4.getKey()), 1));                      // key
    object4.getValue(&dataLength);
    EXPECT_EQ(0U, dataLength);                           // data length

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);

    // Fifth object.
    Object object5(buffer, size);
    EXPECT_EQ(28U, size);                                       // size
    EXPECT_EQ(tableId1, object5.getTableId());                  // table ID
    EXPECT_EQ(1U, object5.getKeyLength());                      // key length
    EXPECT_EQ(version3, object5.getVersion());                  // version
    EXPECT_EQ("3", string(reinterpret_cast<const char*>(
                   object5.getKey()), 1));                      // key
    object5.getValue(&dataLength);
    EXPECT_EQ(0U, dataLength);                           // data length

    EXPECT_FALSE(iter.hasNext());
}

TEST_F(TableEnumeratorTest, nextKeyData) {
    uint64_t version0, version1, version2, version3, version4;
    ramcloud.write(tableId1, "0", 1, "abcdef", 6, NULL, &version0);
    ramcloud.write(tableId1, "1", 1, "ghijkl", 6, NULL, &version1);
    ramcloud.write(tableId1, "2", 1, "mnopqr", 6, NULL, &version2);
    ramcloud.write(tableId1, "3", 1, "stuvwx", 6, NULL, &version3);
    ramcloud.write(tableId1, "4", 1, "yzabcd", 6, NULL, &version4);

    uint32_t keyLength = 0;
    const void* keyBuffer = 0;
    uint32_t dataLength = 0;
    const void* dataBuffer = 0;

    TableEnumerator iter(ramcloud, tableId1, false);
    EXPECT_TRUE(iter.hasNext());
    iter.nextKeyAndData(&keyLength, &keyBuffer, &dataLength, &dataBuffer);

    // First object.
    EXPECT_EQ("0", string(reinterpret_cast<const char*>(
                   keyBuffer), keyLength));                      // key
    EXPECT_EQ("abcdef", string(reinterpret_cast<const char*>     // data
        (dataBuffer), dataLength));

    EXPECT_TRUE(iter.hasNext());
    iter.nextKeyAndData(&keyLength, &keyBuffer, &dataLength, &dataBuffer);

    // Second object.
    EXPECT_EQ("4", string(reinterpret_cast<const char*>(
                   keyBuffer), keyLength));                      // key
    EXPECT_EQ("yzabcd", string(reinterpret_cast<const char*>     // data
        (dataBuffer), dataLength));

    EXPECT_TRUE(iter.hasNext());
    iter.nextKeyAndData(&keyLength, &keyBuffer, &dataLength, &dataBuffer);

    // Third object.
    EXPECT_EQ("2", string(reinterpret_cast<const char*>(
                   keyBuffer), keyLength));                      // key
    EXPECT_EQ("mnopqr", string(reinterpret_cast<const char*>     // data
        (dataBuffer), dataLength));

    EXPECT_TRUE(iter.hasNext());
    iter.nextKeyAndData(&keyLength, &keyBuffer, &dataLength, &dataBuffer);

    // Fourth object.
    EXPECT_EQ("1", string(reinterpret_cast<const char*>(
                   keyBuffer), keyLength));                      // key
    EXPECT_EQ("ghijkl", string(reinterpret_cast<const char*>     // data
        (dataBuffer), dataLength));

    EXPECT_TRUE(iter.hasNext());
    iter.nextKeyAndData(&keyLength, &keyBuffer, &dataLength, &dataBuffer);

    // Fifth object.
    EXPECT_EQ("3", string(reinterpret_cast<const char*>(
                   keyBuffer), keyLength));                      // key
    EXPECT_EQ("stuvwx", string(reinterpret_cast<const char*>     // data
        (dataBuffer), dataLength));

    EXPECT_FALSE(iter.hasNext());
}

}  // namespace RAMCloud
