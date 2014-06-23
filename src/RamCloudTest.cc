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
#include "TableEnumerator.h"

namespace RAMCloud {

class RamCloudTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    uint64_t tableId1;
    uint64_t tableId2;
    uint64_t tableId3;

  public:
    RamCloudTest()
        : logEnabler()
        , context()
        , cluster(&context)
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

        ramcloud.construct(&context, "mock:host=coordinator");
        tableId1 = ramcloud->createTable("table1");
        tableId2 = ramcloud->createTable("table2");
        tableId3 = ramcloud->createTable("table3", 4);
    }

    DISALLOW_COPY_AND_ASSIGN(RamCloudTest);
};

static void pollTestThread(RamCloud* ramcloud) {
    //Calling poll() from other thread should not invoke poller.
    for (int i = 0; i < 10; ++i) {
        ramcloud->poll();
    }
}

TEST(RamCloudSimpleTest, poll) {
    class CountPoller : public Dispatch::Poller {
      public:
        explicit CountPoller(Dispatch* dispatch)
                : Dispatch::Poller(dispatch, "CountPoller"), count(0) { }
        void poll() {
            count++;
        }
        volatile int count;
      private:
        DISALLOW_COPY_AND_ASSIGN(CountPoller);
    };

    Context context(true);
    CountPoller poller(context.dispatch);
    RamCloud ramcloud(&context, "mock:host=coordinator");

    for (int i = 0; i < 100; ++i) {
        ramcloud.poll();
    }
    EXPECT_EQ(100, poller.count);

    std::thread thread(pollTestThread, &ramcloud);
    thread.join();
    EXPECT_EQ(100, poller.count);
}

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
    EXPECT_EQ(4UL, id);
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

TEST_F(RamCloudTest, createIndex) {
    TestLog::Enable _("createIndex");
    EXPECT_THROW(ramcloud->createIndex(10, 1, 0), TableManager::NoSuchTable);
    EXPECT_EQ("createIndex: Cannot find table '10'", TestLog::get());
    TestLog::reset();
    ramcloud->createIndex(tableId1, 1, 0);
    EXPECT_EQ("createIndex: Creating table '1' index '1'", TestLog::get());
}

TEST_F(RamCloudTest, dropIndex) {
    TestLog::Enable _("dropIndex");
    ramcloud->createIndex(tableId1, 1, 0);
    ramcloud->dropIndex(10, 1);
    EXPECT_EQ("dropIndex: Cannot find table '10'", TestLog::get());
    TestLog::reset();
    ramcloud->dropIndex(tableId1, 2);
    EXPECT_EQ("dropIndex: Cannot find index '2' for table '1'", TestLog::get());
    TestLog::reset();
    ramcloud->dropIndex(tableId1, 1);
    EXPECT_EQ("dropIndex: Dropping table '1' index '1'", TestLog::get());
}

TEST_F(RamCloudTest, concurrentAsyncRpc) {
    string message1("no exception");
    try {
        ramcloud->getTableId("newTable");
    }
    catch (ClientException& e) {
        message1 = e.toSymbol();
    }
    EXPECT_EQ("STATUS_TABLE_DOESNT_EXIST", message1);

    //Dispatches async RPCs.
    CreateTableRpc ct_rpc(ramcloud.get(), "newTable");
    DropTableRpc dt_rpc(ramcloud.get(), "table1");

    //Waits for either rpc.
    uint64_t id;
    bool ct_done = false;
    bool dt_done = false;
    while (!ct_done || !dt_done) {
        if (!ct_done && ct_rpc.isReady()) {
            id = ct_rpc.wait();
            EXPECT_EQ(4UL, id);
            ct_done = true;
        } else if (!dt_done && dt_rpc.isReady()) {
            dt_rpc.wait();
            dt_done = true;
        }
        ramcloud->poll();
    }

    EXPECT_EQ(true, ct_done && dt_done);

    //Extra checks for CreateTableRpc.
    uint64_t id2 = ramcloud->getTableId("newTable");
    EXPECT_EQ(id, id2);

    //Extra checks for DropTableRpc.
    string message2("no exception");
    try {
        ramcloud->getTableId("table1");
    }
    catch (ClientException& e) {
        message2 = e.toSymbol();
    }
    EXPECT_EQ("STATUS_TABLE_DOESNT_EXIST", message2);
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

    uint32_t size = 0;
    const void* buffer = 0;

    // Testing keys and data enumeration
    TableEnumerator iter(*ramcloud, tableId3, false);
    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);

    // First object.
    Object object1(buffer, size);
    EXPECT_EQ(34U, size);                                       // size
    EXPECT_EQ(tableId3, object1.getTableId());                  // table ID
    EXPECT_EQ(1U, object1.getKeyLength());                      // key length
    EXPECT_EQ(version0, object1.getVersion());                  // version
    EXPECT_EQ("0", string(reinterpret_cast<const char*>(
                   object1.getKey()), 1));                         // key
    EXPECT_EQ("abcdef", string(reinterpret_cast<const char*>    // value
        (object1.getValue()), 6));

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);

    // Second object.
    Object object2(buffer, size);
    EXPECT_EQ(34U, size);                                       // size
    EXPECT_EQ(tableId3, object2.getTableId());                  // table ID
    EXPECT_EQ(1U, object2.getKeyLength());                      // key length
    EXPECT_EQ(version1, object2.getVersion());                  // version
    EXPECT_EQ("1", string(reinterpret_cast<const char*>(
                   object2.getKey()), 1));                         // key
    EXPECT_EQ("ghijkl", string(reinterpret_cast<const char*>    // value
        (object2.getValue()), 6));

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);

    // Third object.
    Object object3(buffer, size);
    EXPECT_EQ(34U, size);                                       // size
    EXPECT_EQ(tableId3, object3.getTableId());                  // table ID
    EXPECT_EQ(1U, object3.getKeyLength());                      // key length
    EXPECT_EQ(version3, object3.getVersion());                  // version
    EXPECT_EQ("3", string(reinterpret_cast<const char*>(
                   object3.getKey()), 1));                         // key
    EXPECT_EQ("stuvwx", string(reinterpret_cast<const char*>    // value
        (object3.getValue()), 6));

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);

    // Fourth object.
    Object object4(buffer, size);
    EXPECT_EQ(34U, size);                                       // size
    EXPECT_EQ(tableId3, object4.getTableId());                  // table ID
    EXPECT_EQ(1U, object4.getKeyLength());                      // key length
    EXPECT_EQ(version2, object4.getVersion());                  // version
    EXPECT_EQ("2", string(reinterpret_cast<const char*>(
                   object4.getKey()), 1));                         // key
    EXPECT_EQ("mnopqr", string(reinterpret_cast<const char*>    // value
        (object4.getValue()), 6));

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);

    // Fifth object.
    Object object5(buffer, size);
    EXPECT_EQ(34U, size);                                       // size
    EXPECT_EQ(tableId3, object5.getTableId());                  // table ID
    EXPECT_EQ(1U, object5.getKeyLength());                      // key length
    EXPECT_EQ(version4, object5.getVersion());                  // version
    EXPECT_EQ("4", string(reinterpret_cast<const char*>(
                   object5.getKey()), 1));                         // key
    EXPECT_EQ("yzabcd", string(reinterpret_cast<const char*>    // value
        (object5.getValue()), 6));

    EXPECT_FALSE(iter.hasNext());
}

TEST_F(RamCloudTest, enumeration_keys_only) {
    uint64_t version0, version1, version2, version3, version4;
    ramcloud->write(tableId3, "0", 1, "abcdef", 6, NULL, &version0);
    ramcloud->write(tableId3, "1", 1, "ghijkl", 6, NULL, &version1);
    ramcloud->write(tableId3, "2", 1, "mnopqr", 6, NULL, &version2);
    ramcloud->write(tableId3, "3", 1, "stuvwx", 6, NULL, &version3);
    ramcloud->write(tableId3, "4", 1, "yzabcd", 6, NULL, &version4);
    // Write some objects into other tables to make sure they are not returned.
    ramcloud->write(tableId1, "5", 1, "efghij", 6);
    ramcloud->write(tableId2, "6", 1, "klmnop", 6);

    uint32_t size = 0;
    const void* buffer = 0;

    // Testing keys only enumeration
    TableEnumerator iter(*ramcloud, tableId3, true);
    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);

    // First object.
    Object object1(buffer, size);
    EXPECT_EQ(28U, size);                                       // size
    EXPECT_EQ(tableId3, object1.getTableId());                  // table ID
    EXPECT_EQ(1U, object1.getKeyLength());                      // key length
    EXPECT_EQ(version0, object1.getVersion());                  // version
    EXPECT_EQ("0", string(reinterpret_cast<const char*>(
                   object1.getKey()), 1));                         // key
    uint32_t dataLength;
    object1.getValue(&dataLength);
    EXPECT_EQ(0U, dataLength);                           // data length

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);

    // Second object.
    Object object2(buffer, size);
    EXPECT_EQ(28U, size);                                       // size
    EXPECT_EQ(tableId3, object2.getTableId());                  // table ID
    EXPECT_EQ(1U, object2.getKeyLength());                      // key length
    EXPECT_EQ(version1, object2.getVersion());                  // version
    EXPECT_EQ("1", string(reinterpret_cast<const char*>(
                   object2.getKey()), 1));                         // key
    object2.getValue(&dataLength);
    EXPECT_EQ(0U, dataLength);                           // data length

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);

    // Third object.
    Object object3(buffer, size);
    EXPECT_EQ(28U, size);                                       // size
    EXPECT_EQ(tableId3, object3.getTableId());                  // table ID
    EXPECT_EQ(1U, object3.getKeyLength());                      // key length
    EXPECT_EQ(version3, object3.getVersion());                  // version
    EXPECT_EQ("3", string(reinterpret_cast<const char*>(
                   object3.getKey()), 1));                         // key
    object3.getValue(&dataLength);
    EXPECT_EQ(0U, dataLength);                           // data length

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);

    // Fourth object.
    Object object4(buffer, size);
    EXPECT_EQ(28U, size);                                       // size
    EXPECT_EQ(tableId3, object4.getTableId());                  // table ID
    EXPECT_EQ(1U, object4.getKeyLength());                      // key length
    EXPECT_EQ(version2, object4.getVersion());                  // version
    EXPECT_EQ("2", string(reinterpret_cast<const char*>(
                   object4.getKey()), 1));                         // key
    object4.getValue(&dataLength);
    EXPECT_EQ(0U, dataLength);                           // data length

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);

    // Fifth object.
    Object object5(buffer, size);
    EXPECT_EQ(28U, size);                                       // size
    EXPECT_EQ(tableId3, object5.getTableId());                  // table ID
    EXPECT_EQ(1U, object5.getKeyLength());                      // key length
    EXPECT_EQ(version4, object5.getVersion());                  // version
    EXPECT_EQ("4", string(reinterpret_cast<const char*>(
                   object5.getKey()), 1));                         // key
    object5.getValue(&dataLength);
    EXPECT_EQ(0U, dataLength);                           // data length

    EXPECT_FALSE(iter.hasNext());
}

TEST_F(RamCloudTest, enumeration_badTable) {
    TableEnumerator iter(*ramcloud, -1, false);
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
    EXPECT_EQ(2UL, id);
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

TEST_F(RamCloudTest, indexServerControl) {
    Buffer output;
    TestLog::Enable _("createIndex");
    ramcloud->createIndex(tableId1, 2, 0);
    EXPECT_EQ("createIndex: Creating table '1' index '2'", TestLog::get());
    ramcloud->indexServerControl(tableId1, 2, "0", 1,
            WireFormat::GET_TIME_TRACE, "abc", 3, &output);
    EXPECT_EQ("No events to print", TestUtil::toString(&output));
}

TEST_F(RamCloudTest, quiesce) {
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
    ObjectBuffer keysAndValue;
    Buffer value;
    uint64_t version, versionValue;
    ramcloud->readKeysAndValue(tableId1, "0", 1, &keysAndValue, NULL, &version);
    EXPECT_EQ(1U, version);
    EXPECT_EQ("abcdef", string(reinterpret_cast<const char*>(
                        keysAndValue.getValue()), 6));
    EXPECT_EQ("0", string(reinterpret_cast<const char*>(
                        keysAndValue.getKey(0)), 1));
    // test if the value-only return read RPC works fine
    ramcloud->read(tableId1, "0", 1, &value, NULL, &versionValue);
    EXPECT_EQ("abcdef", string(reinterpret_cast<const char*>(
                        value.getRange(0, value.size())),
                        value.size()));

    // test multikey object
    value.reset();
    keysAndValue.reset();
    uint8_t numKeys = 3;
    KeyInfo keyList[3];
    keyList[0].keyLength = 2;
    keyList[0].key = "ha";
    // Key 1 does not exist
    keyList[1].keyLength = 0;
    keyList[1].key = NULL;
    keyList[2].keyLength = 2;
    keyList[2].key = "ho";

    ramcloud->write(tableId1, numKeys, keyList, "new value",
                        NULL, NULL, false);
    ramcloud->readKeysAndValue(tableId1, "ha", 2, &keysAndValue);
    EXPECT_EQ("new value", string(reinterpret_cast<const char*>(
                        keysAndValue.getValue()), 9));

    EXPECT_EQ("ha", string(reinterpret_cast<const char *>(
                    keysAndValue.getKey(0)), 2));
    EXPECT_EQ(2U, keysAndValue.getKeyLength(0));
    EXPECT_EQ((const char*)NULL, keysAndValue.getKey(1));
    EXPECT_EQ(0U, keysAndValue.getKeyLength(1));
    EXPECT_EQ("ho", string(reinterpret_cast<const char *>(
                    keysAndValue.getKey(2)), 2));
    EXPECT_EQ(2U, keysAndValue.getKeyLength(2));

    // again test if the value-return only version of read RPC works fine
    ramcloud->read(tableId1, "ha", 2, &value);
    EXPECT_EQ("new value", string(reinterpret_cast<const char*>(
                        value.getRange(0, value.size())),
                        value.size()));
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

TEST_F(RamCloudTest, objectServerControl){
    ramcloud->write(tableId1, "0", 1, "zfzfzf", 6);
    string serverLocator = ramcloud->objectFinder.lookupTablet(tableId1
                           , Key::getHash(tableId1, "0", 1))->serviceLocator;
    Server* targetServer;
    foreach (Server* server, cluster.servers) {
        if (serverLocator.compare(server->config.localLocator) == 0)
            targetServer = server;
    }
    ASSERT_FALSE(targetServer->context->dispatch->profilerFlag);
    uint64_t totalElements = 100000;
    Buffer output;
    ramcloud->objectServerControl(tableId1, "0", 1,
                            WireFormat::START_DISPATCH_PROFILER,
                            &totalElements, sizeof32(totalElements), &output);
    ASSERT_TRUE(targetServer->context->dispatch->profilerFlag);
    ASSERT_EQ(totalElements, targetServer->context->dispatch->totalElements);
    ramcloud->objectServerControl(tableId1, "0", 1,
                            WireFormat::STOP_DISPATCH_PROFILER,
                            " ", 1, &output);
    ASSERT_FALSE(targetServer->context->dispatch->profilerFlag);
    ramcloud->objectServerControl(tableId1, "0", 1,
                            WireFormat::DUMP_DISPATCH_PROFILE,
                            "pollingTimes.txt", 17, &output);
}

TEST_F(RamCloudTest, splitTablet) {
    string message("no exception");
    try {
        ramcloud->splitTablet("table1", 5);
    }
    catch (ClientException& e) {
        message = e.toSymbol();
    }
    ramcloud->splitTablet("table2", 0x100000000U);
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

TEST_F(RamCloudTest, getRuntimeOption){
    ramcloud->setRuntimeOption("failRecoveryMasters", "1 2 3");
    Buffer value;
    ramcloud->getRuntimeOption("failRecoveryMasters", &value);
    EXPECT_STREQ("1 2 3", cluster.coordinator->getString(&value, 0,
                                                   value.size()));
}

TEST_F(RamCloudTest, testingKill) {
    TestLog::reset();
    cluster.servers[0]->ping->ignoreKill = true;
    // Create the RPC object directly rather than calling testingKill
    // (testingKill would hang in objectFinder.waitForTabletDown).
    KillRpc rpc(ramcloud.get(), tableId1, "0", 1);
    EXPECT_EQ("kill: Server remotely told to kill itself.", TestLog::get());
}

TEST_F(RamCloudTest, setRuntimeOption) {
    ramcloud->setRuntimeOption("failRecoveryMasters", "103");
    EXPECT_EQ(103U,
            cluster.coordinator->runtimeOptions.failRecoveryMasters.front());
}

TEST_F(RamCloudTest, write) {
    uint64_t version;
    ramcloud->write(tableId1, "0", 1, "abcdef", 6, NULL, &version);
    EXPECT_EQ(1U, version);
    ramcloud->write(tableId1, "0", 1, "xyzzy", 5, NULL, &version);
    EXPECT_EQ(2U, version);
    ObjectBuffer value;
    ramcloud->readKeysAndValue(tableId1, "0", 1, &value);
    EXPECT_EQ("xyzzy", string(reinterpret_cast<const char*>(
                        value.getValue()), 5));
    ramcloud->write(tableId1, "0", 1, "new value");
    ramcloud->readKeysAndValue(tableId1, "0", 1, &value);
    EXPECT_EQ("new value", string(reinterpret_cast<const char*>(
                        value.getValue()), 9));

    value.reset();
    uint8_t numKeys = 3;
    KeyInfo keyList[3];
    keyList[0].keyLength = 2;
    keyList[0].key = "ha";
    keyList[1].keyLength = 2;
    keyList[1].key = "hi";
    keyList[2].keyLength = 2;
    keyList[2].key = "ho";

    ramcloud->write(tableId1, numKeys, keyList, "data value",
                        NULL, NULL, false);
    ramcloud->readKeysAndValue(tableId1, "ha", 2, &value);
    EXPECT_EQ("data value", string(reinterpret_cast<const char*>(
                        value.getValue()), 10));
}

TEST_F(RamCloudTest, index_endToEnd) {
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

    KeyInfo keyListC[3];
    keyListC[0].keyLength = 5;
    keyListC[0].key = "keyC0";
    keyListC[1].keyLength = 5;
    keyListC[1].key = "keyC1";
    keyListC[2].keyLength = 5;
    keyListC[2].key = "keyC2";

    Key primaryKeyA(tableId, keyListA[0].key, keyListA[0].keyLength);
    Key primaryKeyB(tableId, keyListB[0].key, keyListB[0].keyLength);
    Key primaryKeyC(tableId, keyListC[0].key, keyListC[0].keyLength);

    ////////////////////////////////////////////////////////////////////////
    ////////// Write the objects, which inserts index entries. /////////////
    ////////////////////////////////////////////////////////////////////////

    // NOTE: Since this write will first involve writing of
    // the corresponding index entries, the version for this
    // data object will be 2 and increases then on for every
    // new object. This is because, indexlets are implemented
    // using RamCloud objects and when an entry is first added,
    // it gets allocated a version of 1.
    ramcloud->write(tableId, numKeys, keyListA, "valueA");
    ramcloud->write(tableId, numKeys, keyListB, "valueB");
    ramcloud->write(tableId, numKeys, keyListC, "valueC");

    ////////////////////////////////////////////////////////////////////////
    //// Lookup for each index keys. Should return hash of primary key. ////
    ////////////////////////////////////////////////////////////////////////

    Buffer lookupResp;
    uint32_t numHashes;
    uint16_t nextKeyLength;
    uint64_t nextKeyHash;
    uint32_t lookupOffset;

    // Lookup on index id 1.
    // Point lookup for one, and partial range lookup for the other two.
    lookupResp.reset();
    ramcloud->lookupIndexKeys(tableId, 1, "keyA1", 5, 0, "keyA1", 5,
                &lookupResp, &numHashes, &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(1U, numHashes);
    lookupOffset = sizeof32(WireFormat::LookupIndexKeys::Response);
    EXPECT_EQ(primaryKeyA.getHash(),
              *lookupResp.getOffset<uint64_t>(lookupOffset));

    lookupResp.reset();
    ramcloud->lookupIndexKeys(tableId, 1, "keyB1", 5, 0, "keyC1", 5,
                &lookupResp, &numHashes, &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(2U, numHashes);
    lookupOffset = sizeof32(WireFormat::LookupIndexKeys::Response);
    EXPECT_EQ(primaryKeyB.getHash(),
              *lookupResp.getOffset<uint64_t>(lookupOffset));
    lookupOffset += sizeof32(uint64_t);
    EXPECT_EQ(primaryKeyC.getHash(),
              *lookupResp.getOffset<uint64_t>(lookupOffset));

    // Lookup on index id 2. Range lookup to get all.
    lookupResp.reset();
    ramcloud->lookupIndexKeys(tableId, 2, "a", 1, 0, "z", 1, &lookupResp,
                              &numHashes, &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(3U, numHashes);
    lookupOffset = sizeof32(WireFormat::LookupIndexKeys::Response);
    EXPECT_EQ(primaryKeyA.getHash(),
              *lookupResp.getOffset<uint64_t>(lookupOffset));
    lookupOffset += sizeof32(uint64_t);
    EXPECT_EQ(primaryKeyB.getHash(),
              *lookupResp.getOffset<uint64_t>(lookupOffset));
    lookupOffset += sizeof32(uint64_t);
    EXPECT_EQ(primaryKeyC.getHash(),
              *lookupResp.getOffset<uint64_t>(lookupOffset));

    ////////////////////////////////////////////////////////////////////////
    //// Indexed reads on primary key hash + index key, for each index. ////
    //// Should return entire objects. /////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////

    Buffer pKHashes;
    Buffer readResp;
    uint32_t numObjects;
    uint32_t readOffset;

    // Indexed read on index id 1.
    // Point read for object, range read for the other two.
    readResp.reset();
    pKHashes.reset();
    pKHashes.emplaceAppend<uint64_t>(primaryKeyA.getHash());
    ramcloud->indexedRead(tableId, 1, &pKHashes, 1, "keyA1", 5, "keyA1", 5,
                          &readResp, &numObjects);

    EXPECT_EQ(1U, numObjects);
    readOffset = sizeof32(WireFormat::IndexedRead::Response);

    uint64_t versionA1 = *readResp.getOffset<uint64_t>(readOffset);
    EXPECT_EQ(2U, versionA1);
    readOffset += sizeof32(uint64_t);
    uint32_t lengthA1 = *readResp.getOffset<uint32_t>(readOffset);
    readOffset += sizeof32(uint32_t);
    Object objA1(tableId, versionA1, 0, readResp, readOffset, lengthA1);
    readOffset += lengthA1;
    uint32_t valueLengthA1;
    const void* valueA1 = objA1.getValue(&valueLengthA1);
    EXPECT_EQ("valueA", string(reinterpret_cast<const char*>(valueA1),
                               valueLengthA1));

    readResp.reset();
    pKHashes.reset();
    pKHashes.emplaceAppend<uint64_t>(primaryKeyB.getHash());
    pKHashes.emplaceAppend<uint64_t>(primaryKeyC.getHash());
    ramcloud->indexedRead(tableId, 2, &pKHashes, 1, "keyB1", 5, "keyC1", 5,
                          &readResp, &numObjects);

    EXPECT_EQ(2U, numObjects);
    readOffset = sizeof32(WireFormat::IndexedRead::Response);

    uint64_t versionB1 = *readResp.getOffset<uint64_t>(readOffset);
    EXPECT_EQ(3U, versionB1);
    readOffset += sizeof32(uint64_t);
    uint32_t lengthB1 = *readResp.getOffset<uint32_t>(readOffset);
    readOffset += sizeof32(uint32_t);
    Object objB1(tableId, versionB1, 0, readResp, readOffset, lengthB1);
    readOffset += lengthB1;
    uint32_t valueLengthB1;
    const void* valueB1 = objB1.getValue(&valueLengthB1);
    EXPECT_EQ("valueB", string(reinterpret_cast<const char*>(valueB1),
                               valueLengthB1));

    uint64_t versionC1 = *readResp.getOffset<uint64_t>(readOffset);
    EXPECT_EQ(4U, versionC1);
    readOffset += sizeof32(uint64_t);
    uint32_t lengthC1 = *readResp.getOffset<uint32_t>(readOffset);
    readOffset += sizeof32(uint32_t);
    Object objC1(tableId, versionB1, 0, readResp, readOffset, lengthC1);
    readOffset += lengthC1;
    uint32_t valueLengthC1;
    const void* valueC1 = objC1.getValue(&valueLengthC1);
    EXPECT_EQ("valueC", string(reinterpret_cast<const char*>(valueC1),
                               valueLengthC1));

    // Indexed read on index id 2. Range read all.
    readResp.reset();
    pKHashes.reset();
    pKHashes.emplaceAppend<uint64_t>(primaryKeyA.getHash());
    pKHashes.emplaceAppend<uint64_t>(primaryKeyB.getHash());
    pKHashes.emplaceAppend<uint64_t>(primaryKeyC.getHash());
    ramcloud->indexedRead(tableId, 3, &pKHashes, 2, "a", 1, "z", 1,
                          &readResp, &numObjects);

    EXPECT_EQ(3U, numObjects);
    readOffset = sizeof32(WireFormat::IndexedRead::Response);

    uint64_t versionA2 = *readResp.getOffset<uint64_t>(readOffset);
    EXPECT_EQ(2U, versionA2);
    readOffset += sizeof32(uint64_t);
    uint32_t lengthA2 = *readResp.getOffset<uint32_t>(readOffset);
    readOffset += sizeof32(uint32_t);
    Object objA2(tableId, versionA2, 0, readResp, readOffset, lengthA2);
    readOffset += lengthA2;
    uint32_t valueLengthA2;
    const void* valueA2 = objA2.getValue(&valueLengthA2);
    EXPECT_EQ("valueA", string(reinterpret_cast<const char*>(valueA2),
                               valueLengthA2));

    uint64_t versionB2 = *readResp.getOffset<uint64_t>(readOffset);
    EXPECT_EQ(3U, versionB2);
    readOffset += sizeof32(uint64_t);
    uint32_t lengthB2 = *readResp.getOffset<uint32_t>(readOffset);
    readOffset += sizeof32(uint32_t);
    Object objB2(tableId, versionB2, 0, readResp, readOffset, lengthB2);
    readOffset += lengthB2;
    uint32_t valueLengthB2;
    const void* valueB2 = objB2.getValue(&valueLengthB2);
    EXPECT_EQ("valueB", string(reinterpret_cast<const char*>(valueB2),
                               valueLengthB2));

    uint64_t versionC2 = *readResp.getOffset<uint64_t>(readOffset);
    EXPECT_EQ(4U, versionC2);
    readOffset += sizeof32(uint64_t);
    uint32_t lengthC2 = *readResp.getOffset<uint32_t>(readOffset);
    readOffset += sizeof32(uint32_t);
    Object objC2(tableId, versionB1, 0, readResp, readOffset, lengthC2);
    readOffset += lengthC2;
    uint32_t valueLengthC2;
    const void* valueC2 = objC2.getValue(&valueLengthC2);
    EXPECT_EQ("valueC", string(reinterpret_cast<const char*>(valueC2),
                               valueLengthC2));

    ////////////////////////////////////////////////////////////////////////
    ////////// Remove two objects, which removes their index keys. /////////
    ////////////////////////////////////////////////////////////////////////

    ramcloud->remove(tableId, keyListA[0].key, keyListA[0].keyLength);
    ramcloud->remove(tableId, keyListC[0].key, keyListC[0].keyLength);

    ////////////////////////////////////////////////////////////////////////
    /////// Lookup for each index keys. Should return only one obj. ////////
    ////////////////////////////////////////////////////////////////////////

    // Required declarations already done during previous lookup.
    lookupOffset = sizeof32(WireFormat::LookupIndexKeys::Response);

    // Lookup on index id 1.
    lookupResp.reset();
    ramcloud->lookupIndexKeys(tableId, 1, "a", 1, 0, "z", 1, &lookupResp,
                              &numHashes, &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(1U, numHashes);
    EXPECT_EQ(primaryKeyB.getHash(),
              *lookupResp.getOffset<uint64_t>(lookupOffset));

    // Lookup on index id 2.
    lookupResp.reset();
    ramcloud->lookupIndexKeys(tableId, 2, "a", 1, 0, "z", 1, &lookupResp,
                              &numHashes, &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(1U, numHashes);
    EXPECT_EQ(primaryKeyB.getHash(),
              *lookupResp.getOffset<uint64_t>(lookupOffset));

    ////////////////////////////////////////////////////////////////////////
    ////////// Remove other object, which removes it's index keys. /////////
    ////////////////////////////////////////////////////////////////////////

    ramcloud->remove(tableId, keyListB[0].key, keyListB[0].keyLength);

    ////////////////////////////////////////////////////////////////////////
    /////// Lookup for each index keys. Should return nothing. /////////////
    ////////////////////////////////////////////////////////////////////////

    // Required declarations already done during previous lookup.

    // Lookup on index id 1.
    lookupResp.reset();
    ramcloud->lookupIndexKeys(tableId, 1, "a", 1, 0, "z", 1, &lookupResp,
                              &numHashes, &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(0U, numHashes);

    // Lookup on index id 2.
    lookupResp.reset();
    ramcloud->lookupIndexKeys(tableId, 2, "a", 1, 0, "z", 1, &lookupResp,
                              &numHashes, &nextKeyLength, &nextKeyHash);
    EXPECT_EQ(0U, numHashes);
}

}  // namespace RAMCloud
