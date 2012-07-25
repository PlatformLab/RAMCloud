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
    Context context;
    MockCluster cluster;
    RamCloud ramcloud;
    uint64_t tableId1;

  public:
    TableEnumeratorTest()
        : context()
        , cluster(context)
        , ramcloud(context, "mock:host=coordinator")
        , tableId1(-1)
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        ServerConfig config = ServerConfig::forTesting();
        config.services = {MASTER_SERVICE, PING_SERVICE};
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

    TableEnumerator iter(ramcloud, tableId1);
    uint32_t size = 0;
    const void* buffer = 0;

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);
    const Object* object = static_cast<const Object*>(buffer);

    // First object.
    EXPECT_EQ(29U, size);                                   // size
    EXPECT_EQ(tableId1, object->tableId);                   // table ID
    EXPECT_EQ(1U, object->keyLength);                       // key length
    EXPECT_EQ(version2, object->version);                   // version
    EXPECT_EQ('2', object->keyAndData[0]);                  // key
    EXPECT_EQ("mnopqr", string(&object->keyAndData[1], 6)); // value

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);
    object = static_cast<const Object*>(buffer);

    // Second object.
    EXPECT_EQ(29U, size);                                   // size
    EXPECT_EQ(tableId1, object->tableId);                   // table ID
    EXPECT_EQ(1U, object->keyLength);                       // key length
    EXPECT_EQ(version0, object->version);                   // version
    EXPECT_EQ('0', object->keyAndData[0]);                  // key
    EXPECT_EQ("abcdef", string(&object->keyAndData[1], 6)); // value

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);
    object = static_cast<const Object*>(buffer);

    // Third object.
    EXPECT_EQ(29U, size);                                   // size
    EXPECT_EQ(tableId1, object->tableId);                   // table ID
    EXPECT_EQ(1U, object->keyLength);                       // key length
    EXPECT_EQ(version1, object->version);                   // version
    EXPECT_EQ('1', object->keyAndData[0]);                  // key
    EXPECT_EQ("ghijkl", string(&object->keyAndData[1], 6)); // value

    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);
    object = static_cast<const Object*>(buffer);

    // Fourth object.
    EXPECT_EQ(29U, size);                                   // size
    EXPECT_EQ(tableId1, object->tableId);                   // table ID
    EXPECT_EQ(1U, object->keyLength);                       // key length
    EXPECT_EQ(version4, object->version);                   // version
    EXPECT_EQ('4', object->keyAndData[0]);                  // key
    EXPECT_EQ("yzabcd", string(&object->keyAndData[1], 6)); // value


    EXPECT_TRUE(iter.hasNext());
    iter.next(&size, &buffer);
    object = static_cast<const Object*>(buffer);

    // Fifth object.
    EXPECT_EQ(29U, size);                                   // size
    EXPECT_EQ(tableId1, object->tableId);                   // table ID
    EXPECT_EQ(1U, object->keyLength);                       // key length
    EXPECT_EQ(version3, object->version);                   // version
    EXPECT_EQ('3', object->keyAndData[0]);                  // key
    EXPECT_EQ("stuvwx", string(&object->keyAndData[1], 6)); // value

    EXPECT_FALSE(iter.hasNext());
}

}  // namespace RAMCloud
