/* Copyright (c) 2009-2016 Stanford University
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
#include "BackupService.h"
#include "InMemoryStorage.h"
#include "LogDigest.h"
#include "MockCluster.h"
#include "SegmentIterator.h"
#include "Server.h"
#include "Key.h"
#include "ServerListBuilder.h"
#include "ShortMacros.h"
#include "StringUtil.h"
#include "TabletsBuilder.h"
#include "UnsyncedObjectRpcWrapper.h"
#include "WitnessClient.h"

namespace RAMCloud {

using namespace WireFormat; // NOLINT

class WitnessServiceTest : public ::testing::Test {
  public:
    Context context;
    ServerConfig masterConfig;
    ServerConfig witnessConfig;
    MockCluster cluster;
    Server* masterServer;
    Server* witnessServer;
    WitnessService* witness;
    ServerId masterId;
    ServerId witnessId;
    TestLog::Enable logSilencer;

    WitnessServiceTest()
        : context()
        , masterConfig(ServerConfig::forTesting())
        , witnessConfig(ServerConfig::forTesting())
        , cluster(&context)
        , masterServer()
        , witnessServer()
        , witness()
        , masterId()
        , witnessId()
        , logSilencer()
    {
        Logger::get().setLogLevels(SILENT_LOG_LEVEL);

        witnessConfig.localLocator = "mock:host=witness1";
        witnessConfig.services = {WireFormat::WITNESS_SERVICE,
                WireFormat::ADMIN_SERVICE};
        witnessServer = cluster.addServer(witnessConfig);
        witnessId = witnessServer->serverId;
        witness = witnessServer->witness.get();

        masterConfig.localLocator = "mock:host=master";
        masterConfig.services = {WireFormat::MASTER_SERVICE,
                WireFormat::ADMIN_SERVICE};
        masterConfig.master.numReplicas = 0;
        masterServer = cluster.addServer(masterConfig);
        masterId = masterServer->serverId;
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(WitnessServiceTest);
};

TEST_F(WitnessServiceTest, start) {
    uint64_t bufferBasePtr =
            WitnessClient::witnessStart(&context, witnessId, masterId);
    EXPECT_NE(0UL, bufferBasePtr);
    WitnessService::Master* buffer =
            reinterpret_cast<WitnessService::Master*>(bufferBasePtr);
    EXPECT_EQ(masterId.getId(), buffer->id);
    EXPECT_TRUE(buffer->writable);
}

TEST_F(WitnessServiceTest, record) {
    uint64_t bufferBasePtr =
            WitnessClient::witnessStart(&context, witnessId, masterId);
    EXPECT_NE(0UL, bufferBasePtr);


    Transport::SessionRef sessionToWitness =
            context.transportManager->getSession("mock:host=witness1");
    int16_t hashIndex = 1;
    uint64_t tableId = 5;
    uint64_t keyHash = 999;

    char data[100] = "Random data is here..";
    uint32_t dataLen = static_cast<uint32_t>(strnlen(data, sizeof(data))) + 1;
    ClientRequest request = {data, dataLen};

    UnsyncedObjectRpcWrapper::WitnessRecordRpc rpc(&context,
                sessionToWitness, witnessId.getId(), masterId.getId(),
                bufferBasePtr, hashIndex, tableId, keyHash, request);
    bool accepted = rpc.wait();
    EXPECT_TRUE(accepted);

    // Verify successful recording..
    WitnessService::Master* buffer =
            reinterpret_cast<WitnessService::Master*>(bufferBasePtr);
    EXPECT_TRUE(buffer->table[hashIndex].occupied);
    EXPECT_EQ(tableId, buffer->table[hashIndex].header.tableId);
    EXPECT_EQ(keyHash, buffer->table[hashIndex].header.keyHash);
    EXPECT_EQ(dataLen, buffer->table[hashIndex].header.requestSize);
    EXPECT_EQ(0, strncmp(data, buffer->table[hashIndex].request, 100));

    // Try to insert into same index. It should be rejected.
    char data2[100] = "NOOOOO SHOULD NOT survive!!";
    uint32_t len2 = static_cast<uint32_t>(strnlen(data2, sizeof(data2))) + 1;
    ClientRequest request2 = {data2, len2};

    UnsyncedObjectRpcWrapper::WitnessRecordRpc rpc2(&context,
                sessionToWitness, witnessId.getId(), masterId.getId(),
                bufferBasePtr, hashIndex, tableId, 888, request2);
    EXPECT_FALSE(rpc2.wait());
    EXPECT_TRUE(buffer->table[hashIndex].occupied);
    EXPECT_EQ(tableId, buffer->table[hashIndex].header.tableId);
    EXPECT_EQ(keyHash, buffer->table[hashIndex].header.keyHash);
    EXPECT_EQ(dataLen, buffer->table[hashIndex].header.requestSize);
    EXPECT_EQ(0, strncmp(data, buffer->table[hashIndex].request, 100));
}

TEST_F(WitnessServiceTest, record_badRequest) {
    uint64_t bufferBasePtr =
            WitnessClient::witnessStart(&context, witnessId, masterId);
    EXPECT_NE(0UL, bufferBasePtr);
    WitnessService::Master* buffer =
            reinterpret_cast<WitnessService::Master*>(bufferBasePtr);

    Transport::SessionRef sessionToWitness =
            context.transportManager->getSession("mock:host=witness1");
    int16_t hashIndex = 1;
    uint64_t tableId = 5;
    uint64_t keyHash = 999;

    char data[100] = "Random data is here..";
    uint32_t dataLen = static_cast<uint32_t>(strnlen(data, sizeof(data))) + 1;
    ClientRequest request = {data, dataLen};

    // Bad target masterId.
    {
        UnsyncedObjectRpcWrapper::WitnessRecordRpc rpc(&context,
                    sessionToWitness, witnessId.getId(), masterId.getId() + 1,
                    bufferBasePtr, hashIndex, tableId, keyHash, request);
        bool accepted = rpc.wait();
        EXPECT_FALSE(accepted);
        EXPECT_FALSE(buffer->table[hashIndex].occupied);
    }

    // Bad bufferBasePtr test caused segfault... Cannot test it.
}

} // namespace RAMCloud
