/* Copyright (c) 2011-2012 Stanford University
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
#include "TableEnumerator.h"

namespace RAMCloud {

class RamCloudTest : public ::testing::Test {
  public:
    Context context;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    uint64_t tableId1;
    uint64_t tableId2;
    uint64_t tableId3;

  public:
    RamCloudTest()
        : context()
        , cluster(context)
        , ramcloud()
        , tableId1(-1)
        , tableId2(-2)
        , tableId3(-3)
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
        config.services = {WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=ping1";
        cluster.addServer(config);

        ramcloud.construct(context, "mock:host=coordinator");
        tableId1 = ramcloud->createTable("table1");
        tableId2 = ramcloud->createTable("table2");
        tableId3 = ramcloud->createTable("table3", 4);
    }

    DISALLOW_COPY_AND_ASSIGN(RamCloudTest);
};

TEST_F(RamCloudTest, createTable) {
    string message("no exception");
    try {
        ramcloud->getTableId("newTable");
    }
    catch (ClientException& e) {
        message = e.toSymbol();
    }
    EXPECT_EQ("STATUS_TABLE_DOESNT_EXIST", message);
    uint64_t id = ramcloud->createTable("newTable");
    EXPECT_EQ(3UL, id);
    uint64_t id2 = ramcloud->getTableId("newTable");
    EXPECT_EQ(id, id2);
}

TEST_F(RamCloudTest, dropTable) {
    ramcloud->dropTable("table1");
    string message("no exception");
    try {
        ramcloud->getTableId("table1");
    }
    catch (ClientException& e) {
        message = e.toSymbol();
    }
    EXPECT_EQ("STATUS_TABLE_DOESNT_EXIST", message);
}

TEST_F(RamCloudTest, enumeration_basics) {
    uint64_t version0, version1, version2, version3, version4;
    ramcloud->write(tableId3, "0", 1, "abcdef", 6, NULL, &version0);
    ramcloud->write(tableId3, "1", 1, "ghijkl", 6, NULL, &version1);
    ramcloud->write(tableId3, "2", 1, "mnopqr", 6, NULL, &version2);
    ramcloud->write(tableId3, "3", 1, "stuvwx", 6, NULL, &version3);
    ramcloud->write(tableId3, "4", 1, "yzabcd", 6, NULL, &version4);
    // Write some objects into other tables to make sure they are not returned.
    ramcloud->write(tableId1, "5", 1, "efghij", 6);
    ramcloud->write(tableId2, "6", 1, "klmnop", 6);

    TableEnumerator iter(*ramcloud, tableId3);
    uint32_t size = 0;
    const void* buffer = 0;

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);
    const Object* object = static_cast<const Object*>(buffer);

    // First object.
    EXPECT_EQ(29U, size);                                   // size
    EXPECT_EQ(tableId3, object->tableId);                   // table ID
    EXPECT_EQ(1U, object->keyLength);                       // key length
    EXPECT_EQ(version0, object->version);                   // version
    EXPECT_EQ('0', object->keyAndData[0]);                  // key
    EXPECT_EQ("abcdef", string(&object->keyAndData[1], 6)); // value

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);
    object = static_cast<const Object*>(buffer);

    // Second object.
    EXPECT_EQ(29U, size);                                   // size
    EXPECT_EQ(tableId3, object->tableId);                   // table ID
    EXPECT_EQ(1U, object->keyLength);                       // key length
    EXPECT_EQ(version1, object->version);                   // version
    EXPECT_EQ('1', object->keyAndData[0]);                  // key
    EXPECT_EQ("ghijkl", string(&object->keyAndData[1], 6)); // value

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);
    object = static_cast<const Object*>(buffer);

    // Third object.
    EXPECT_EQ(29U, size);                                   // size
    EXPECT_EQ(tableId3, object->tableId);                   // table ID
    EXPECT_EQ(1U, object->keyLength);                       // key length
    EXPECT_EQ(version2, object->version);                   // version
    EXPECT_EQ('2', object->keyAndData[0]);                  // key
    EXPECT_EQ("mnopqr", string(&object->keyAndData[1], 6)); // value

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);
    object = static_cast<const Object*>(buffer);

    // Fourth object.
    EXPECT_EQ(29U, size);                                   // size
    EXPECT_EQ(tableId3, object->tableId);                   // table ID
    EXPECT_EQ(1U, object->keyLength);                       // key length
    EXPECT_EQ(version4, object->version);                   // version
    EXPECT_EQ('4', object->keyAndData[0]);                  // key
    EXPECT_EQ("yzabcd", string(&object->keyAndData[1], 6)); // value


    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);
    object = static_cast<const Object*>(buffer);

    // Fifth object.
    EXPECT_EQ(29U, size);                                   // size
    EXPECT_EQ(tableId3, object->tableId);                   // table ID
    EXPECT_EQ(1U, object->keyLength);                       // key length
    EXPECT_EQ(version3, object->version);                   // version
    EXPECT_EQ('3', object->keyAndData[0]);                  // key
    EXPECT_EQ("stuvwx", string(&object->keyAndData[1], 6)); // value

    EXPECT_FALSE(iter.hasNext());
}

TEST_F(RamCloudTest, enumeration_badTable) {
    TableEnumerator iter(*ramcloud, -1);
    EXPECT_THROW(iter.hasNext(), TableDoesntExistException);
}

TEST_F(RamCloudTest, getMetrics_byObject) {
    metrics->temp.count3 = 20202;
    ServerMetrics metrics = ramcloud->getMetrics(tableId1, "0", 1);
    EXPECT_EQ(20202U, metrics["temp.count3"]);
}

TEST_F(RamCloudTest, getMetrics_byLocator) {
    metrics->temp.count3 = 10101;
    ServerMetrics metrics = ramcloud->getMetrics("mock:host=master1");
    EXPECT_EQ(10101U, metrics["temp.count3"]);
}

TEST_F(RamCloudTest, getTableId) {
    string message("no exception");
    try {
        ramcloud->getTableId("bogusTable");
    }
    catch (ClientException& e) {
        message = e.toSymbol();
    }
    EXPECT_EQ("STATUS_TABLE_DOESNT_EXIST", message);
    uint64_t id = ramcloud->getTableId("table2");
    EXPECT_EQ(1UL, id);
}

TEST_F(RamCloudTest, increment) {
    int64_t value = 99;
    ramcloud->write(tableId1, "key1", 4, &value, sizeof(int64_t));
    uint64_t version;
    EXPECT_EQ(114L, ramcloud->increment(tableId1, "key1", 4, 15L,
            NULL, &version));
    EXPECT_EQ(2U, version);
    EXPECT_EQ(111L, ramcloud->increment(tableId1, "key1", 4, -3L));
    ramcloud->write(tableId1, "key2", 4, &value, sizeof(int64_t)-1);
    string message("no exception");
    try {
        ramcloud->increment(tableId1, "key21", 4, 15L);
    }
    catch (ClientException& e) {
        message = e.toSymbol();
    }
    EXPECT_EQ("STATUS_INVALID_OBJECT", message);
}

TEST_F(RamCloudTest, multiRead) {
    ramcloud->write(tableId1, "0", 1, "first");
    ramcloud->write(tableId1, "1", 1, "second");
    Tub<Buffer> value1, value2;
    MultiReadObject request1(tableId1, "0", 1, &value1);
    MultiReadObject request2(tableId1, "1", 1, &value2);
    MultiReadObject* requests[] = {&request1, &request2};
    ramcloud->multiRead(requests, 2);
    ASSERT_TRUE(value1);
    ASSERT_TRUE(value2);
    EXPECT_EQ("first", TestUtil::toString(value1.get()));
    EXPECT_EQ("second", TestUtil::toString(value2.get()));
}

TEST_F(RamCloudTest, quiesce) {
    TestLog::Enable _;
    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::BACKUP_SERVICE, WireFormat::PING_SERVICE};
    config.localLocator = "mock:host=backup1";
    cluster.addServer(config);
    TestLog::reset();
    ramcloud->quiesce();
    EXPECT_EQ("quiesce: Backup at mock:host=master2 quiescing | "
            "quiesce: Backup at mock:host=backup1 quiescing",
            TestLog::get());
}

TEST_F(RamCloudTest, read) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);
    Buffer value;
    uint64_t version;
    ramcloud->read(tableId1, "0", 1, &value, NULL, &version);
    EXPECT_EQ(1U, version);
    EXPECT_EQ("abcdef", TestUtil::toString(&value));
}

TEST_F(RamCloudTest, remove) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);
    uint64_t version;
    ramcloud->remove(tableId1, "0", 1, NULL, &version);
    EXPECT_EQ(1U, version);
    Buffer value;
    string message("no exception");
    try {
        ramcloud->read(tableId1, "0", 1, &value);
    }
    catch (ClientException& e) {
        message = e.toSymbol();
    }
    EXPECT_EQ("STATUS_OBJECT_DOESNT_EXIST", message);
}

TEST_F(RamCloudTest, splitTablet) {
    string message("no exception");
    try {
        ramcloud->splitTablet("table1", 0, 10, 5);
    }
    catch (ClientException& e) {
        message = e.toSymbol();
    }
    EXPECT_EQ("STATUS_TABLET_DOESNT_EXIST", message);
    ramcloud->splitTablet("table2", 0, ~0LU, 0x100000000U);
}

TEST_F(RamCloudTest, testingFill) {
    ramcloud->testingFill(tableId2, "0", 1, 10, 10);
    Buffer value;
    ramcloud->read(tableId2, "0", 1, &value);
    EXPECT_EQ("0xcccccccc 0xcccccccc /xcc/xcc", TestUtil::toString(&value));
    value.reset();
    ramcloud->read(tableId2, "99", 1, &value);
    EXPECT_EQ("0xcccccccc 0xcccccccc /xcc/xcc", TestUtil::toString(&value));
}

TEST_F(RamCloudTest, testingKill) {
    TestLog::Enable _;
    cluster.servers[0]->ping->ignoreKill = true;
    // Create the RPC object directly rather than calling testingKill
    // (testingKill would hang in objectFinder.waitForTabletDown).
    KillRpc2 rpc(*ramcloud, tableId1, "0", 1);
    EXPECT_EQ("kill: Server remotely told to kill itself.", TestLog::get());
}

TEST_F(RamCloudTest, testingSetRuntimeOption) {
    ramcloud->testingSetRuntimeOption("failRecoveryMasters", "103");
    EXPECT_EQ(103U,
            cluster.coordinator->runtimeOptions.failRecoveryMasters.front());
}

TEST_F(RamCloudTest, write) {
    uint64_t version;
    ramcloud->write(tableId1, "0", 1, "abcdef", 6, NULL, &version);
    EXPECT_EQ(1U, version);
    ramcloud->write(tableId1, "0", 1, "xyzzy", 5, NULL, &version);
    EXPECT_EQ(2U, version);
    Buffer value;
    ramcloud->read(tableId1, "0", 1, &value);
    EXPECT_EQ("xyzzy", TestUtil::toString(&value));
    ramcloud->write(tableId1, "0", 1, "new value");
    ramcloud->read(tableId1, "0", 1, &value);
    EXPECT_EQ("new value", TestUtil::toString(&value));
}

}  // namespace RAMCloud
