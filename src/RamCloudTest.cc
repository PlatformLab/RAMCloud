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
        config.services = {MASTER_SERVICE, PING_SERVICE};
        config.localLocator = "mock:host=master1";
        cluster.addServer(config);
        config.services = {MASTER_SERVICE, PING_SERVICE};
        config.localLocator = "mock:host=master2";
        cluster.addServer(config);
        config.services = {PING_SERVICE};
        config.localLocator = "mock:host=ping1";
        cluster.addServer(config);

        ramcloud.construct(context, "mock:host=coordinator");
        tableId1 = ramcloud->createTable("table1");
        tableId2 = ramcloud->createTable("table2");
        tableId3 = ramcloud->createTable("table3", 4);
    }

    DISALLOW_COPY_AND_ASSIGN(RamCloudTest);
};

TEST_F(RamCloudTest, enumeration_basics) {
    uint64_t version0, version1, version2, version3, version4;
    ramcloud->write(tableId3, "0", 1, "abcdef", 6, NULL, &version0, false);
    ramcloud->write(tableId3, "1", 1, "ghijkl", 6, NULL, &version1, false);
    ramcloud->write(tableId3, "2", 1, "mnopqr", 6, NULL, &version2, false);
    ramcloud->write(tableId3, "3", 1, "stuvwx", 6, NULL, &version3, false);
    ramcloud->write(tableId3, "4", 1, "yzabcd", 6, NULL, &version4, false);
    // Write some objects into other tables to make sure they are not returned.
    ramcloud->write(tableId1, "5", 1, "efghij", 6);
    ramcloud->write(tableId2, "6", 1, "klmnop", 6);

    RamCloud::Enumeration iter(*ramcloud, tableId3);
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
    RamCloud::Enumeration iter(*ramcloud, -1);
    EXPECT_THROW(iter.hasNext(), TableDoesntExistException);
}

TEST_F(RamCloudTest, getMetrics) {
    metrics->temp.count3 = 10101;
    ServerMetrics metrics = ramcloud->getMetrics("mock:host=master1");
    EXPECT_EQ(10101U, metrics["temp.count3"]);
}

TEST_F(RamCloudTest, getMetrics_byTableId) {
    metrics->temp.count3 = 20202;
    ServerMetrics metrics = ramcloud->getMetrics(tableId1, "0", 1);
    EXPECT_EQ(20202U, metrics["temp.count3"]);
}

TEST_F(RamCloudTest, ping) {
    EXPECT_EQ(12345U, ramcloud->ping("mock:host=ping1", 12345U, 100000));
}

TEST_F(RamCloudTest, proxyPing) {
    EXPECT_NE(0xffffffffffffffffU, ramcloud->proxyPing("mock:host=ping1",
                "mock:host=master1", 100000, 100000));
}

TEST_F(RamCloudTest, multiRead) {
    // Write objects to be read later
    uint64_t version1;
    ramcloud->write(tableId1, "0", 1, "firstVal", 8, NULL, &version1, false);

    uint64_t version2;
    ramcloud->write(tableId2, "0", 1, "secondVal", 9, NULL, &version2, false);
    uint64_t version3;
    ramcloud->write(tableId2, "1", 1, "thirdVal", 8, NULL, &version3, false);

    // Construct requests and read
    MasterClient::ReadObject* requests[3];

    Tub<Buffer> readValue1;
    MasterClient::ReadObject request1(tableId1, "0", 1, &readValue1);
    request1.status = STATUS_RETRY;
    requests[0] = &request1;

    Tub<Buffer> readValue2;
    MasterClient::ReadObject request2(tableId2, "0", 1, &readValue2);
    request2.status = STATUS_RETRY;
    requests[1] = &request2;

    Tub<Buffer> readValue3;
    MasterClient::ReadObject request3(tableId2, "1", 1, &readValue3);
    request3.status = STATUS_RETRY;
    requests[2] = &request3;

    ramcloud->multiRead(requests, 3);

    EXPECT_STREQ("STATUS_OK", statusToSymbol(request1.status));
    EXPECT_EQ(1U, request1.version);
    EXPECT_EQ("firstVal", TestUtil::toString(readValue1.get()));
    EXPECT_STREQ("STATUS_OK", statusToSymbol(request2.status));
    EXPECT_EQ(1U, request2.version);
    EXPECT_EQ("secondVal", TestUtil::toString(readValue2.get()));
    EXPECT_STREQ("STATUS_OK", statusToSymbol(request3.status));
    EXPECT_EQ(2U, request3.version);
    EXPECT_EQ("thirdVal", TestUtil::toString(readValue3.get()));
}

TEST_F(RamCloudTest, writeString) {
    uint64_t tableId1 = ramcloud->getTableId("table1");
    ramcloud->write(tableId1, "99", 2, "abcdef");
    Buffer value;
    ramcloud->read(tableId1, "99", 2, &value);
    EXPECT_EQ(6U, value.getTotalLength());
    char buffer[200];
    value.copy(0, value.getTotalLength(), buffer);
    EXPECT_STREQ("abcdef", buffer);
}

}  // namespace RAMCloud
