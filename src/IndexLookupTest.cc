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
                            Tablet::NORMAL, Log::Position()});
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
    RamCloud ramcloud;
    MockTransport transport;

    IndexLookupTest()
        : ramcloud("mock:")
        , transport(ramcloud.clientContext)
    {
        ramcloud.objectFinder.tableConfigFetcher.reset(
                new IndexLookupRpcRefresher);
        ramcloud.clientContext->transportManager->registerMock(&transport);
    }

    ~IndexLookupTest()
    {
    }

    DISALLOW_COPY_AND_ASSIGN(IndexLookupTest);
};

TEST_F(IndexLookupTest, construction) {
    TestLog::Enable _;
    IndexLookup indexLookup(&ramcloud, 10, 1, "a", 1, "z", 1);
    EXPECT_EQ("mock:indexserver=0",
        indexLookup.lookupRpc.rpc->session->getServiceLocator());
    EXPECT_EQ(IndexLookup::SENT, indexLookup.lookupRpc.status);
}

// Rule 1:
// Handle the completion of a lookIndexKeys RPC.
TEST_F(IndexLookupTest, isReady_lookupComplete) {
    TestLog::Enable _;
    IndexLookup indexLookup(&ramcloud, 10, 1, "a", 1, "z", 1);
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
    EXPECT_EQ(10U, indexLookup.lookupRpc.numHashes + indexLookup.numInserted);
    EXPECT_EQ(0U, indexLookup.nextKeyHash);
}

// Rule 2:
// Copy PKHashes from response buffer of lookup RPC into activeHashes
TEST_F(IndexLookupTest, isReady_activeHashes) {
    TestLog::Enable _;
    IndexLookup indexLookup(&ramcloud, 10, 1, "a", 1, "z", 1);
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
    IndexLookup indexLookup(&ramcloud, 10, 1, "a", 1, "z", 1);
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
    IndexLookup indexLookup(&ramcloud, 10, 1, "a", 1, "z", 1);
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
    IndexLookup indexLookup(&ramcloud, 10, 1, "a", 1, "z", 1);
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
               indexLookup.readRpc[0].rpc->session->getServiceLocator());
    EXPECT_EQ(10U, indexLookup.readRpc[0].numHashes);
    EXPECT_EQ(9U, indexLookup.readRpc[0].maxPos);
    EXPECT_EQ(IndexLookup::SENT, indexLookup.readRpc[0].status);
}

} // namespace ramcloud


