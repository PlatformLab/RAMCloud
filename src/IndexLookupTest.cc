/* Copyright (c) 2014-2015 Stanford University
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
#include "IndexKey.h"
#include "IndexLookup.h"

namespace RAMCloud {
// This class provides tablet map and indexlet map info to ObjectFinder.
class IndexLookupRpcRefresher : public ObjectFinder::TableConfigFetcher {
  public:
    IndexLookupRpcRefresher() : called(0) {}
    void getTableConfig(uint64_t tableId,
                        std::map<TabletKey, TabletWithLocator>* tableMap,
                        std::multimap<std::pair<uint64_t, uint8_t>,
                                      ObjectFinder::Indexlet>* tableIndexMap) {

        called++;
        char buffer[100];
        uint64_t numTablets = 10;
        uint8_t numIndexlets = 26;

        tableMap->clear();
        uint64_t tabletRange = 1 + ~0UL / numTablets;
        for (uint64_t i = 0; i < numTablets; i++) {
            uint64_t startKeyHash = i * tabletRange;
            uint64_t endKeyHash = startKeyHash + tabletRange - 1;
            snprintf(buffer, sizeof(buffer), "mock:dataserver=%lu", i);
            if (i == (numTablets - 1))
                endKeyHash = ~0UL;
            Tablet rawEntry({10, startKeyHash, endKeyHash, ServerId(),
                            Tablet::NORMAL, LogPosition()});
            TabletWithLocator entry(rawEntry, buffer);

            TabletKey key {entry.tablet.tableId, entry.tablet.startKeyHash};
            tableMap->insert(std::make_pair(key, entry));
        }

        tableIndexMap->clear();
        for (uint8_t i = 0; i < numIndexlets; i++) {
            auto id = std::make_pair(10, 1); // Pair of table id and index id.
            char firstKey = static_cast<char>('a'+i);
            char firstNotOwnedKey = static_cast<char>('b'+i);
            snprintf(buffer, sizeof(buffer), "mock:indexserver=%u", i);
            ObjectFinder::Indexlet indexlet(
                reinterpret_cast<void *>(&firstKey), 1,
                reinterpret_cast<void *>(&firstNotOwnedKey), 1,
                ServerId(), buffer);
            tableIndexMap->insert(std::make_pair(id, indexlet));
        }
    }
    uint32_t called;
};

class IndexLookupTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Tub<RamCloud> ramcloud;
    Context context;
    MockCluster cluster;
    IndexletManager* im;
    Tub<MockTransport> transport;
    IndexKey::IndexKeyRange azKeyRange;

    IndexLookupTest()
        : logEnabler()
        , ramcloud()
        , context()
        , cluster(&context)
        , im()
        , transport()
        , azKeyRange(1, "a", 1, "z", 1)
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master1";
        cluster.addServer(config);

        config.services = {WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=ping1";
        cluster.addServer(config);

        im = &cluster.contexts[0]->getMasterService()->indexletManager;
        ramcloud.construct("mock:");
        transport.construct(ramcloud->clientContext);

        ramcloud->clientContext->objectFinder->tableConfigFetcher.reset(
                new IndexLookupRpcRefresher);
        ramcloud->clientContext->transportManager->registerMock(
                transport.get());

    }

    ~IndexLookupTest()
    {
    }

    DISALLOW_COPY_AND_ASSIGN(IndexLookupTest);
};

TEST_F(IndexLookupTest, construction) {
    TestLog::Enable _;
    IndexLookup indexLookup(ramcloud.get(), 10, azKeyRange);
    EXPECT_EQ("mock:indexserver=0",
        indexLookup.lookupRpc.rpc->session->getServiceLocator());
    EXPECT_EQ(IndexLookup::SENT, indexLookup.lookupRpc.status);
}

// Rule 1:
// Handle the completion of a lookIndexKeys RPC.
TEST_F(IndexLookupTest, isReady_lookupComplete) {
    TestLog::Enable _;
    IndexLookup indexLookup(ramcloud.get(), 10, azKeyRange);
    const char *nextKey = "next key for rpc";
    size_t nextKeyLen = strlen(nextKey) + 1; // include null char

    Buffer *respBuffer = indexLookup.lookupRpc.rpc->response;

    respBuffer->emplaceAppend<WireFormat::ResponseCommon>()->status = STATUS_OK;
    // numHashes
    respBuffer->emplaceAppend<uint32_t>(10);
    // nextKeyLength
    respBuffer->emplaceAppend<uint16_t>(uint16_t(nextKeyLen));
    // nextKeyHash
    respBuffer->emplaceAppend<uint64_t>(0);
    for (KeyHash i = 0; i < 10; i++) {
        respBuffer->emplaceAppend<KeyHash>(i);
    }
    respBuffer->appendCopy(nextKey, (uint32_t) nextKeyLen);

    indexLookup.lookupRpc.rpc->completed();
    EXPECT_EQ(IndexLookup::SENT, indexLookup.lookupRpc.status);
    indexLookup.isReady();
    EXPECT_EQ(10U, indexLookup.lookupRpc.numHashes + indexLookup.numInserted);
    EXPECT_EQ(0U, indexLookup.nextKeyHash);
    EXPECT_EQ(0, strcmp(reinterpret_cast<char*>(indexLookup.nextKey), nextKey));
}

// Rule 2:
// Copy PKHashes from response buffer of lookup RPC into activeHashes
TEST_F(IndexLookupTest, isReady_activeHashes) {
    TestLog::Enable _;
    IndexLookup indexLookup(ramcloud.get(), 10, azKeyRange);
    indexLookup.lookupRpc.rpc->response->emplaceAppend<
        WireFormat::ResponseCommon>()->status = STATUS_OK;
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint32_t>(10);
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint16_t>(uint16_t(0));
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint64_t>(0);
    for (KeyHash i = 0; i < 10; i++) {
        indexLookup.lookupRpc.rpc->response->emplaceAppend<KeyHash>(i);
    }
    indexLookup.lookupRpc.rpc->completed();
    EXPECT_EQ(IndexLookup::SENT, indexLookup.lookupRpc.status);
    indexLookup.isReady();
    EXPECT_EQ(IndexLookup::FREE, indexLookup.lookupRpc.status);
    for (KeyHash i = 0; i < 10; i++) {
        EXPECT_EQ(i, indexLookup.activeHashes[i]);
    }
}

// Rule 3(a):
// Issue next lookup RPC if an RESULT_READY lookupIndexKeys RPC
// has no unread RPC
TEST_F(IndexLookupTest, isReady_issueNextLookup) {
    TestLog::Enable _;
    IndexLookup indexLookup(ramcloud.get(), 10, azKeyRange);
    indexLookup.lookupRpc.rpc->response->emplaceAppend<
            WireFormat::ResponseCommon>()->status = STATUS_OK;
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint32_t>(10);
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint16_t>(uint16_t(1));
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint64_t>(0);
    for (KeyHash i = 0; i < 10; i++) {
        indexLookup.lookupRpc.rpc->response->emplaceAppend<KeyHash>(i);
    }
    indexLookup.lookupRpc.rpc->response->emplaceAppend<char>('b');
    EXPECT_EQ("mock:indexserver=0",
                indexLookup.lookupRpc.rpc->session->getServiceLocator());
    indexLookup.lookupRpc.rpc->completed();
    EXPECT_EQ(IndexLookup::SENT, indexLookup.lookupRpc.status);
    indexLookup.isReady();
    EXPECT_EQ(IndexLookup::SENT, indexLookup.lookupRpc.status);
    EXPECT_EQ("mock:indexserver=1",
            indexLookup.lookupRpc.rpc->session->getServiceLocator());
}

// Rule 3(b):
// If all lookup RPCs have completed, mark finishedLookup as true, which
// indicates no outgoing lookup RPC thereafter.
TEST_F(IndexLookupTest, isReady_allLookupCompleted) {
    TestLog::Enable _;
    IndexLookup indexLookup(ramcloud.get(), 10, azKeyRange);
    indexLookup.lookupRpc.rpc->response->emplaceAppend<
            WireFormat::ResponseCommon>()->status = STATUS_OK;
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint32_t>(10);
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint16_t>(uint16_t(0));
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint64_t>(0);
    for (KeyHash i = 0; i < 10; i++) {
        indexLookup.lookupRpc.rpc->response->emplaceAppend<KeyHash>(i);
    }
    indexLookup.lookupRpc.rpc->completed();
    EXPECT_EQ(IndexLookup::SENT, indexLookup.lookupRpc.status);
    indexLookup.isReady();
    EXPECT_EQ(IndexLookup::FREE, indexLookup.lookupRpc.status);
    EXPECT_TRUE(indexLookup.finishedLookup);
}

// Rule 5:
// Try to assign the current key hash to an existing RPC to the same server.
TEST_F(IndexLookupTest, isReady_assignPKHashesToSameServer) {
    TestLog::Enable _;
    IndexLookup indexLookup(ramcloud.get(), 10, azKeyRange);
    indexLookup.lookupRpc.rpc->response->emplaceAppend<
        WireFormat::ResponseCommon>()->status = STATUS_OK;
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint32_t>(10);
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint16_t>(uint16_t(0));
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint64_t>(0);
    for (KeyHash i = 0; i < 10; i++) {
        indexLookup.lookupRpc.rpc->response->emplaceAppend<KeyHash>(i);
    }
    indexLookup.lookupRpc.rpc->completed();
    EXPECT_EQ(IndexLookup::SENT, indexLookup.lookupRpc.status);
    indexLookup.isReady();
    EXPECT_EQ("mock:dataserver=0",
               indexLookup.readRpcs[0].rpc->session->getServiceLocator());
    EXPECT_EQ(10U, indexLookup.readRpcs[0].numHashes);
    EXPECT_EQ(9U, indexLookup.readRpcs[0].maxPos);
    EXPECT_EQ(IndexLookup::SENT, indexLookup.readRpcs[0].status);
}

// Adds bogus index entries for an object that shouldn't be in range query.
TEST_F(IndexLookupTest, getNext_filtering) {
    ramcloud.construct(&context, "mock:host=coordinator");
    uint64_t tableId = ramcloud->createTable("table");
    ramcloud->createIndex(tableId, 1, 0);

    uint8_t numKeys = 2;

    KeyInfo keyList1[2];
    keyList1[0].keyLength = 11;
    keyList1[0].key = "primaryKey1";
    keyList1[1].keyLength = 1;
    keyList1[1].key = "a";

    KeyInfo keyList2[2];
    keyList2[0].keyLength = 11;
    keyList2[0].key = "primaryKey2";
    keyList2[1].keyLength = 1;
    keyList2[1].key = "b";

    KeyInfo keyList3[2];
    keyList3[0].keyLength = 11;
    keyList3[0].key = "primaryKey3";
    keyList3[1].keyLength = 1;
    keyList3[1].key = "c";

    ramcloud->write(tableId, numKeys, keyList1, "value1");
    ramcloud->write(tableId, numKeys, keyList2, "value2");
    ramcloud->write(tableId, numKeys, keyList3, "value3");


    Key primaryKey1(tableId, keyList1[0].key, keyList1[0].keyLength);
    uint64_t pkhash = primaryKey1.getHash();
    // insert extra entries for pkhash A that would land in search range
    im->insertEntry(tableId, 1, "B2", 2, pkhash);
    im->insertEntry(tableId, 1, "C7", 2, pkhash);

    IndexKey::IndexKeyRange keyRange(1, "B", 1, "D", 1);
    IndexLookup indexLookup(ramcloud.get(), tableId, keyRange);

    uint32_t itemsReturned = 0;
    while (indexLookup.getNext()) {
        itemsReturned++;
    }
    EXPECT_EQ(0U, itemsReturned);

    // insert extra entries for pkhash A that would land in search range
    im->insertEntry(tableId, 1, "b2", 2, 81);
    im->insertEntry(tableId, 1, "c7", 2, pkhash);

    IndexKey::IndexKeyRange keyRange2(1, "b", 1, "d", 1);
    IndexLookup indexLookup2(ramcloud.get(), tableId, keyRange2);

    EXPECT_TRUE(indexLookup2.getNext());
    EXPECT_STREQ("primaryKey2",
        std::string(static_cast<const char*>(
                        indexLookup2.currentObject()->getKey()),
                    indexLookup2.currentObject()->getKeyLength(0)).c_str());

    EXPECT_TRUE(indexLookup2.getNext());
    EXPECT_STREQ("primaryKey3",
        std::string(static_cast<const char*>(
                        indexLookup2.currentObject()->getKey()),
                    indexLookup2.currentObject()->getKeyLength(0)).c_str());

    EXPECT_FALSE(indexLookup2.getNext());
}
} // namespace ramcloud
