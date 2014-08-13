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

#include <cstring>

#include "TestUtil.h"
#include "CRamCloud.h"
#include "MockCluster.h"
#include "RamCloud.h"
#include "RawMetrics.h"
#include "ServerMetrics.h"
#include "TableEnumerator.h"

namespace RAMCloud {

class CRamCloudTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    rc_client *client;
    Status status;
    uint64_t tableId1;
    uint64_t tableId2;
    uint64_t tableId3;
    std::string key;
    std::string value;
    uint16_t keyLength;
    uint16_t valueLength;
    uint16_t szMultiOpIncrement;
    uint16_t szMultiOpRead;
    uint16_t szMultiOpWrite;
    uint16_t szMultiOpRemove;
    unsigned numMultiOps;
    int *keys;

  public:
    CRamCloudTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , ramcloud()
        , client(NULL)
        , status(STATUS_MAX_VALUE)
        , tableId1(-1)
        , tableId2(-2)
        , tableId3(-3)
        , key("key")
        , value("abc")
        , keyLength(3)
        , valueLength(3)
        , szMultiOpIncrement(rc_multiOpSizeOf(MULTI_OP_INCREMENT))
        , szMultiOpRead(rc_multiOpSizeOf(MULTI_OP_READ))
        , szMultiOpWrite(rc_multiOpSizeOf(MULTI_OP_WRITE))
        , szMultiOpRemove(rc_multiOpSizeOf(MULTI_OP_REMOVE))
        , numMultiOps(100)
        , keys(NULL)
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
        rc_connectWithClient(ramcloud.get(), &client);

        // Create twice as many keys as are written into the table
        // Required for rc_multiRead_overflow test
        keys = new int[2 * numMultiOps];
        for (unsigned i = 0; i < 2 * numMultiOps; ++i)
            keys[i] = i;

        for (unsigned i = 0; i < numMultiOps; ++i) {
            ramcloud->write(tableId3, &keys[i], sizeof(keys[0]),
                            value.data(), valueLength, NULL, NULL);
        }
    }

    virtual ~CRamCloudTest() {
        delete[] keys;
    }

    DISALLOW_COPY_AND_ASSIGN(CRamCloudTest);
};

TEST_F(CRamCloudTest, rc_createTable) {
    uint64_t tableId = 0;

    status = rc_getTableId(client, "newTable", &tableId);
    EXPECT_EQ(STATUS_TABLE_DOESNT_EXIST, status);
    status = rc_createTable(client, "newTable", 1);
    EXPECT_EQ(STATUS_OK, status);
    status = rc_getTableId(client, "newTable", &tableId);
    EXPECT_EQ(STATUS_OK, status);
    EXPECT_GT(tableId, uint64_t(0));

    status = rc_createTable(client, "newTable", 1);
    EXPECT_EQ(STATUS_OK, status);
}

TEST_F(CRamCloudTest, rc_dropTable) {
    uint64_t tableId;
    status = rc_dropTable(client, "table1");
    EXPECT_EQ(STATUS_OK, status);
    status = rc_getTableId(client, "table1", &tableId);
    EXPECT_EQ(STATUS_TABLE_DOESNT_EXIST, status);
}

TEST_F(CRamCloudTest, rc_getTableId) {
    uint64_t tableId = 0;
    status = rc_getTableId(client, "bogusTable", &tableId);
    EXPECT_EQ(STATUS_TABLE_DOESNT_EXIST, status);
    status = rc_getTableId(client, "table2", &tableId);
    EXPECT_EQ(STATUS_OK, status);
    EXPECT_EQ(tableId, tableId2);
}

TEST_F(CRamCloudTest, rc_incrementInt64_basics) {
    uint64_t version = 0;
    int64_t newValue = 0;
    status = rc_incrementInt64(client, tableId1, "iinc", 4, 42, NULL,
                               &version, &newValue);
    EXPECT_EQ(status, STATUS_OK);
    EXPECT_GT(version, uint64_t(0));
    EXPECT_EQ(newValue, 42);

    status = rc_incrementInt64(client, tableId1, "iinc", 4, -2, NULL,
                               &version, &newValue);
    EXPECT_EQ(status, STATUS_OK);
    EXPECT_GT(version, uint64_t(0));
    EXPECT_EQ(newValue, 40);
}

TEST_F(CRamCloudTest, rc_incrementInt64_RejectRules) {
    RejectRules rrules;
    memset(&rrules, 0, sizeof(rrules));
    rrules.doesntExist = 1;
    status = rc_incrementInt64(client, tableId1, "new", 3, 42, &rrules,
                               NULL, NULL);
    EXPECT_EQ(status, STATUS_OBJECT_DOESNT_EXIST);
}

TEST_F(CRamCloudTest, rc_incrementInt64_noReturnValue) {
    status = rc_incrementInt64(client, tableId1, "iinc", 4, 42, NULL,
                               NULL, NULL);
    EXPECT_EQ(status, STATUS_OK);

    int64_t value;
    uint32_t actualLength;
    status = rc_read(client, tableId1, "iinc", 4, NULL, NULL,
                     &value, sizeof(value), &actualLength);
    EXPECT_EQ(status, STATUS_OK);
    EXPECT_EQ(actualLength, sizeof(value));
    EXPECT_EQ(value, 42);
}

TEST_F(CRamCloudTest, rc_incrementInt64_TableDoesntExist) {
    status = rc_incrementInt64(client, 101, "0", 1, 42, NULL,
                               NULL, NULL);
    EXPECT_EQ(status, STATUS_TABLE_DOESNT_EXIST);
}

TEST_F(CRamCloudTest, rc_incrementDouble_basics) {
    uint64_t version = 0;
    double newValue = 0;
    status = rc_incrementDouble(client, tableId1, "finc", 4, 42.0, NULL,
                                &version, &newValue);
    EXPECT_EQ(status, STATUS_OK);
    EXPECT_GT(version, uint64_t(0));
    EXPECT_DOUBLE_EQ(newValue, 42.0);

    status = rc_incrementDouble(client, tableId1, "finc", 4, -2.0, NULL,
                                &version, &newValue);
    EXPECT_EQ(status, STATUS_OK);
    EXPECT_GT(version, uint64_t(0));
    EXPECT_DOUBLE_EQ(newValue, 40.0);
}

TEST_F(CRamCloudTest, rc_incrementDouble_RejectRules) {
    RejectRules rrules;
    memset(&rrules, 0, sizeof(rrules));
    rrules.doesntExist = 1;
    status = rc_incrementDouble(client, tableId1, "new", 3, 42.0, &rrules,
                                NULL, NULL);
    EXPECT_EQ(status, STATUS_OBJECT_DOESNT_EXIST);
}

TEST_F(CRamCloudTest, rc_incrementDouble_noReturnValue) {
    status = rc_incrementDouble(client, tableId1, "finc", 4, 42.0, NULL,
                                NULL, NULL);
    EXPECT_EQ(status, STATUS_OK);

    double value;
    uint32_t actualLength;
    status = rc_read(client, tableId1, "finc", 4, NULL, NULL,
                     &value, sizeof(value), &actualLength);
    EXPECT_EQ(status, STATUS_OK);
    EXPECT_EQ(actualLength, sizeof(value));
    EXPECT_DOUBLE_EQ(value, 42.0);
}

TEST_F(CRamCloudTest, rc_incrementDouble_TableDoesntExist) {
    status = rc_incrementDouble(client, 101, "0", 1, 42, NULL,
                                NULL, NULL);
    EXPECT_EQ(status, STATUS_TABLE_DOESNT_EXIST);
}

TEST_F(CRamCloudTest, rc_read_basic) {
    status = rc_write(client, tableId1, key.data(), keyLength,
                      value.data(), valueLength, NULL, NULL);
    EXPECT_EQ(status, STATUS_OK);

    char buf[valueLength];
    memset(buf, 0, valueLength);
    uint32_t actualLength = 0;
    uint64_t version = 0;
    status = rc_read(client, tableId1, key.data(), keyLength, NULL, &version,
                     buf, valueLength, &actualLength);
    EXPECT_EQ(status, STATUS_OK);
    EXPECT_GT(version, uint64_t(0));
    EXPECT_EQ(actualLength, valueLength);
    EXPECT_EQ(std::string(buf, actualLength), value);
}

TEST_F(CRamCloudTest, rc_read_short) {
    status = rc_write(client, tableId1, key.data(), keyLength,
                      value.data(), valueLength, NULL, NULL);
    EXPECT_EQ(status, STATUS_OK);

    char buf;
    uint32_t actualLength = 0;
    uint64_t version = 0;
    status = rc_read(client, tableId1, key.data(), keyLength, NULL, &version,
                     &buf, 1, &actualLength);
    EXPECT_EQ(status, STATUS_OK);
    EXPECT_EQ(actualLength, valueLength);
    EXPECT_EQ(buf, value[0]);
}

TEST_F(CRamCloudTest, rc_read_TableDoesntExist) {
    char buf;
    uint32_t actualLength;
    status = rc_read(client, 101, "k", 1, NULL, NULL, &buf, 1, &actualLength);
    EXPECT_EQ(status, STATUS_TABLE_DOESNT_EXIST);
}

TEST_F(CRamCloudTest, rc_read_ObjectDoesntExist) {
    char buf;
    uint32_t actualLength;
    status =
        rc_read(client, tableId1, "0", 1, NULL, NULL, &buf, 1, &actualLength);
    EXPECT_EQ(status, STATUS_OBJECT_DOESNT_EXIST);
}

TEST_F(CRamCloudTest, rc_remove_basic) {
    uint64_t version = 0;
    status = rc_write(client, tableId1, key.data(), keyLength,
                      value.data(), valueLength, NULL, &version);
    EXPECT_EQ(status, STATUS_OK);

    uint64_t version_remove = 0;
    status = rc_remove(client, tableId1, "0", 1, NULL, &version_remove);
    EXPECT_EQ(status, STATUS_OK);
    EXPECT_EQ(version_remove, uint64_t(0));

    status = rc_remove(client, tableId1, key.data(), keyLength, NULL,
                       &version_remove);
    EXPECT_EQ(status, STATUS_OK);
    EXPECT_EQ(version_remove, version);
}

TEST_F(CRamCloudTest, rc_remove_RejectRules) {
    uint64_t version = 0;
    status = rc_write(client, tableId1, key.data(), keyLength,
                      value.data(), valueLength, NULL, &version);
    EXPECT_EQ(status, STATUS_OK);

    RejectRules rrules;
    memset(&rrules, 0, sizeof(rrules));
    rrules.givenVersion = version-1;
    rrules.versionNeGiven = 1;
    status = rc_remove(client, tableId1, key.data(), keyLength, &rrules, NULL);
    EXPECT_EQ(status, STATUS_WRONG_VERSION);

    rrules.givenVersion = version;
    status = rc_remove(client, tableId1, key.data(), keyLength, &rrules, NULL);
    EXPECT_EQ(status, STATUS_OK);
}

TEST_F(CRamCloudTest, rc_remove_TableDoesntExist) {
    status = rc_remove(client, 101, "0", 1, NULL, NULL);
    EXPECT_EQ(status, STATUS_TABLE_DOESNT_EXIST);
}

TEST_F(CRamCloudTest, rc_write_basic) {
    status = rc_write(client, tableId1, key.data(), keyLength,
                      value.data(), valueLength, NULL, NULL);
    EXPECT_EQ(status, STATUS_OK);
}

TEST_F(CRamCloudTest, rc_write_RejectRules) {
    uint64_t version = 0;

    status = rc_write(client, tableId1, key.data(), keyLength,
                      value.data(), valueLength, NULL, &version);
    EXPECT_EQ(status, STATUS_OK);
    EXPECT_GT(version, uint64_t(0));

    value[0] = 'z';
    RejectRules rrules;
    memset(&rrules, 0, sizeof(rrules));

    rrules.doesntExist = 1;
    status = rc_write(client, tableId1, "0", 1, "0", 1, &rrules, NULL);
    EXPECT_EQ(status, STATUS_OBJECT_DOESNT_EXIST);

    rrules.doesntExist = 0;
    rrules.exists = 1;
    status = rc_write(client, tableId1, key.data(), keyLength,
                      value.data(), valueLength, &rrules, NULL);
    EXPECT_EQ(status, STATUS_OBJECT_EXISTS);

    rrules.exists = 0;
    rrules.givenVersion = version-1;
    rrules.versionNeGiven = 1;
    status = rc_write(client, tableId1, key.data(), keyLength,
                      value.data(), valueLength, &rrules, NULL);
    EXPECT_EQ(status, STATUS_WRONG_VERSION);

    rrules.givenVersion = version;
    status = rc_write(client, tableId1, key.data(), keyLength,
                      value.data(), valueLength, &rrules, &version);
    EXPECT_EQ(status, STATUS_OK);
}

TEST_F(CRamCloudTest, rc_write_TableDoesntExist) {
    status = rc_write(client, 101, key.data(), keyLength,
                      value.data(), valueLength, NULL, NULL);
    EXPECT_EQ(status, STATUS_TABLE_DOESNT_EXIST);
}

TEST_F(CRamCloudTest, rc_multiIncrement) {
    unsigned char *mIncrementObjects = reinterpret_cast<unsigned char *>
        (malloc(numMultiOps * szMultiOpIncrement));
    void **pmIncrementObjects = reinterpret_cast<void **>
        (malloc(numMultiOps * sizeof(pmIncrementObjects[0])));
    const char *key = "key";
    uint16_t keyLength = 3;
    for (unsigned i = 0; i < numMultiOps; ++i) {
        pmIncrementObjects[i] = mIncrementObjects + (i * szMultiOpIncrement);
        rc_multiIncrementCreate(tableId3, key, keyLength,
                                1, 0.0, NULL, pmIncrementObjects[i]);
    }
    rc_multiIncrement(client, pmIncrementObjects, numMultiOps);
    for (unsigned i = 0; i < numMultiOps; ++i) {
        Status thisStatus =
            rc_multiOpStatus(pmIncrementObjects[i], MULTI_OP_INCREMENT);
        EXPECT_EQ(STATUS_OK, thisStatus);
        rc_multiOpDestroy(pmIncrementObjects[i], MULTI_OP_INCREMENT);
    }
    free(pmIncrementObjects);
    free(mIncrementObjects);

    int64_t value;
    uint32_t actualLength;
    status = rc_read(client, tableId3, key, keyLength, NULL, NULL,
                     &value, sizeof(value), &actualLength);
    EXPECT_EQ(status, STATUS_OK);
    EXPECT_EQ(actualLength, sizeof(value));
    EXPECT_EQ(value, numMultiOps);
}

TEST_F(CRamCloudTest, rc_multiRead) {
    unsigned char *mReadObjects = reinterpret_cast<unsigned char *>
        (malloc(numMultiOps * szMultiOpRead));
    void **pmReadObjects = reinterpret_cast<void **>
        (malloc(numMultiOps * sizeof(pmReadObjects[0])));
    char *buffers = reinterpret_cast<char *>(malloc(numMultiOps * valueLength));
    uint32_t *actualSizes = reinterpret_cast<uint32_t *>
        (malloc(numMultiOps * sizeof(actualSizes[0])));
    for (unsigned i = 0; i < numMultiOps; ++i) {
        pmReadObjects[i] = mReadObjects + (i * szMultiOpRead);
        rc_multiReadCreate(tableId3,
                           &(keys[i]), sizeof(keys[i]),
                           buffers + (i * valueLength), valueLength,
                           &(actualSizes[i]),
                           pmReadObjects[i]);
    }
    rc_multiRead(client, pmReadObjects, numMultiOps);
    for (unsigned i = 0; i < numMultiOps; ++i) {
        Status thisStatus = rc_multiOpStatus(pmReadObjects[i], MULTI_OP_READ);
        EXPECT_EQ(STATUS_OK, thisStatus);
        EXPECT_EQ(actualSizes[i], valueLength);
        EXPECT_EQ(std::string(buffers + i * valueLength, valueLength), value);
        rc_multiOpDestroy(pmReadObjects[i], MULTI_OP_READ);
    }
    free(actualSizes);
    free(buffers);
    free(pmReadObjects);
    free(mReadObjects);
}

TEST_F(CRamCloudTest, rc_multiRead_overflow) {
    unsigned char *mReadObjects = reinterpret_cast<unsigned char *>
        (malloc(2 * numMultiOps * szMultiOpRead));
    void **pmReadObjects = reinterpret_cast<void **>
        (malloc(2 * numMultiOps * sizeof(pmReadObjects[0])));
    char *buffers = reinterpret_cast<char *>
        (malloc(2 * numMultiOps * valueLength));
    uint32_t *actualSizes = reinterpret_cast<uint32_t *>
        (malloc(2 * numMultiOps * sizeof(uint32_t)));
    for (unsigned i = 0; i < 2 * numMultiOps; ++i) {
        pmReadObjects[i] = mReadObjects + (i * szMultiOpRead);
        rc_multiReadCreate(tableId3,
                           &(keys[i]), sizeof(keys[i]),
                           buffers + (i * valueLength), valueLength,
                           &(actualSizes[i]),
                           pmReadObjects[i]);
    }
    rc_multiRead(client, pmReadObjects, 2 * numMultiOps);
    for (unsigned i = 0; i < numMultiOps; ++i) {
        Status thisStatus = rc_multiOpStatus(pmReadObjects[i], MULTI_OP_READ);
        EXPECT_EQ(STATUS_OK, thisStatus);
        EXPECT_EQ(actualSizes[i], valueLength);
        EXPECT_EQ(std::string(buffers + i * valueLength, valueLength), value);
        rc_multiOpDestroy(pmReadObjects[i], MULTI_OP_READ);
    }
    for (unsigned i = numMultiOps; i < 2 * numMultiOps; ++i) {
        Status thisStatus = rc_multiOpStatus(pmReadObjects[i], MULTI_OP_READ);
        EXPECT_EQ(STATUS_OBJECT_DOESNT_EXIST, thisStatus);
        rc_multiOpDestroy(pmReadObjects[i], MULTI_OP_READ);
    }
    free(actualSizes);
    free(buffers);
    free(pmReadObjects);
    free(mReadObjects);
}

TEST_F(CRamCloudTest, rc_multiRemove) {
    unsigned char *mRemoveObjects = reinterpret_cast<unsigned char *>
        (malloc(numMultiOps * szMultiOpRemove));
    void **pmRemoveObjects = reinterpret_cast<void **>
        (malloc(numMultiOps * sizeof(pmRemoveObjects[0])));
    for (unsigned i = 0; i < numMultiOps; ++i) {
        pmRemoveObjects[i] = mRemoveObjects + (i * szMultiOpRemove);
        rc_multiRemoveCreate(tableId3,
                             &(keys[i]), sizeof(keys[i]),
                             NULL, pmRemoveObjects[i]);
    }
    rc_multiRemove(client, pmRemoveObjects, numMultiOps);
    for (unsigned i = 0; i < numMultiOps; ++i) {
        Status thisStatus =
            rc_multiOpStatus(pmRemoveObjects[i], MULTI_OP_REMOVE);
        EXPECT_EQ(STATUS_OK, thisStatus);
        rc_multiOpDestroy(pmRemoveObjects[i], MULTI_OP_REMOVE);

        Buffer result;
        Status readStatus = STATUS_OK;
        try {
            ramcloud->read(tableId3, &keys[i], sizeof(keys[0]), &result,
                           NULL, NULL);
        } catch (ClientException& e) {
            readStatus = e.status;
        }
        EXPECT_EQ(STATUS_OBJECT_DOESNT_EXIST, readStatus);
    }
    free(pmRemoveObjects);
    free(mRemoveObjects);
}

TEST_F(CRamCloudTest, rc_multiWrite) {
    unsigned char *mWriteObjects = reinterpret_cast<unsigned char *>
        (malloc(numMultiOps * szMultiOpWrite));
    void **pmWriteObjects = reinterpret_cast<void **>
        (malloc(numMultiOps * sizeof(pmWriteObjects[0])));

    for (unsigned i = 0; i < numMultiOps; ++i) {
        pmWriteObjects[i] = mWriteObjects + (i * szMultiOpWrite);
        rc_multiWriteCreate(tableId1,
                            &(keys[i]), sizeof(keys[i]),
                            value.data(), valueLength,
                            NULL, pmWriteObjects[i]);
    }
    rc_multiWrite(client, pmWriteObjects, numMultiOps);
    for (unsigned i = 0; i < numMultiOps; ++i) {
        Status thisStatus = rc_multiOpStatus(pmWriteObjects[i], MULTI_OP_WRITE);
        EXPECT_EQ(STATUS_OK, thisStatus);
        EXPECT_GT(rc_multiOpVersion(pmWriteObjects[i], MULTI_OP_WRITE),
                  uint64_t(0));
        rc_multiOpDestroy(pmWriteObjects[i], MULTI_OP_WRITE);
    }
    free(pmWriteObjects);
    free(mWriteObjects);

    for (unsigned i = 0; i < numMultiOps; ++i) {
        Buffer result;
        Status readStatus = STATUS_OK;
        try {
            ramcloud->read(tableId1, &keys[i], sizeof(keys[0]), &result,
                           NULL, NULL);
        } catch (ClientException& e) {
            readStatus = e.status;
        }
        EXPECT_EQ(STATUS_OK, readStatus);
        EXPECT_EQ(result.size(), valueLength);
        EXPECT_EQ(std::string(reinterpret_cast<char *>
                  (result.getRange(0, valueLength)), valueLength),
                  value);
    }
}

}  // namespace RAMCloud
