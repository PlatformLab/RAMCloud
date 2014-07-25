/* Copyright (c) 2014 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"
#include "MockTransport.h"
#include "IndexRpcWrapper.h"
#include "ShortMacros.h"

namespace RAMCloud {

// This class provides tablet map info to ObjectFinder, with a
// different locator each time it is invoked.
class IndexRpcWrapperRefresher : public ObjectFinder::TableConfigFetcher {
  public:
    IndexRpcWrapperRefresher() : called(0) {}
    void getTableConfig(uint64_t tableId,
                        std::map<TabletKey, TabletWithLocator>* tableMap,
                        std::multimap<std::pair<uint64_t, uint8_t>,
                                      ObjectFinder::Indexlet>* tableIndexMap) {

        called++;
        char buffer[100];
        snprintf(buffer, sizeof(buffer), "mock:refresh=%d", called);

        tableIndexMap->clear();
        auto id = std::make_pair(10, 1); // Pair of table id and index id.

        ObjectFinder::Indexlet indexlet("", 0, "", 0, ServerId(), buffer);
        tableIndexMap->insert(std::make_pair(id, indexlet));
    }
    uint32_t called;
};

class IndexRpcWrapperTest : public ::testing::Test {
  public:
    RamCloud ramcloud;
    MockTransport transport;

    IndexRpcWrapperTest()
        : ramcloud("mock:")
        , transport(ramcloud.clientContext)
    {
        ramcloud.objectFinder.tableConfigFetcher.reset(
                new IndexRpcWrapperRefresher);
        ramcloud.clientContext->transportManager->registerMock(&transport);
    }

    ~IndexRpcWrapperTest()
    {
    }

    DISALLOW_COPY_AND_ASSIGN(IndexRpcWrapperTest);
};

TEST_F(IndexRpcWrapperTest, checkStatus_unknownIndexlet) {
    TestLog::Enable _;
    Buffer responseBuffer;
    IndexRpcWrapper wrapper(&ramcloud, 10, 1, "abc", 3, 4, &responseBuffer);
    wrapper.request.fillFromString("100");
    wrapper.send();
    EXPECT_EQ("mock:refresh=1", wrapper.session->getServiceLocator());
    wrapper.response->emplaceAppend<WireFormat::ResponseCommon>()->status =
            STATUS_UNKNOWN_INDEXLET;
    wrapper.state = RpcWrapper::RpcState::FINISHED;
    EXPECT_FALSE(wrapper.isReady());
    EXPECT_STREQ("IN_PROGRESS", wrapper.stateString());
    EXPECT_EQ("checkStatus: Server mock:refresh=1 doesn't store "
            "given secondary key for table 10, index id 1; "
            "refreshing object map",
            TestLog::get());
    EXPECT_EQ("mock:refresh=2", wrapper.session->getServiceLocator());
}

TEST_F(IndexRpcWrapperTest, checkStatus_otherError) {
    TestLog::Enable _;
    Buffer responseBuffer;
    IndexRpcWrapper wrapper(&ramcloud, 10, 1, "abc", 3, 4, &responseBuffer);
    wrapper.request.fillFromString("100");
    wrapper.send();
    wrapper.response->emplaceAppend<WireFormat::ResponseCommon>()->status =
            STATUS_UNIMPLEMENTED_REQUEST;
    wrapper.state = RpcWrapper::RpcState::FINISHED;
    EXPECT_TRUE(wrapper.isReady());
    EXPECT_STREQ("FINISHED", wrapper.stateString());
    EXPECT_EQ("", TestLog::get());
    EXPECT_EQ("mock:refresh=1", wrapper.session->getServiceLocator());
}

TEST_F(IndexRpcWrapperTest, indexNotFound) {
    Buffer responseBuffer;
    IndexRpcWrapper wrapper(&ramcloud, 10, 2, "abc", 3, 4, &responseBuffer);
    wrapper.request.fillFromString("100");
    wrapper.send();
    EXPECT_TRUE(wrapper.isReady());
    EXPECT_STREQ("unknown index",
                 statusToString(wrapper.responseHeader->status));
}

TEST_F(IndexRpcWrapperTest, handleTransportError) {
    TestLog::Enable _;
    Buffer responseBuffer;
    IndexRpcWrapper wrapper(&ramcloud, 10, 1, "abc", 3, 4, &responseBuffer);
    wrapper.request.fillFromString("100");
    wrapper.send();
    EXPECT_EQ("mock:refresh=1", wrapper.session->getServiceLocator());
    wrapper.state = RpcWrapper::RpcState::FAILED;
    EXPECT_FALSE(wrapper.isReady());
    EXPECT_STREQ("IN_PROGRESS", wrapper.stateString());
    EXPECT_EQ("flushSession: flushing session for mock:refresh=1",
                TestLog::get());
    EXPECT_EQ("mock:refresh=2", wrapper.session->getServiceLocator());
}

TEST_F(IndexRpcWrapperTest, send) {
    Buffer responseBuffer;
    IndexRpcWrapper wrapper(&ramcloud, 10, 1, "abc", 3, 4, &responseBuffer);
    wrapper.request.fillFromString("100");
    wrapper.send();
    EXPECT_STREQ("IN_PROGRESS", wrapper.stateString());
    EXPECT_EQ("sendRequest: 100", transport.outputLog);
    EXPECT_EQ("mock:refresh=1", wrapper.session->getServiceLocator());
}

TEST_F(IndexRpcWrapperTest, send_noSession) {
    TestLog::Enable _;
    Buffer responseBuffer;
    IndexRpcWrapper wrapper(&ramcloud, 10, 2, "abc", 3, 4, &responseBuffer);
    wrapper.request.fillFromString("100");
    wrapper.send();
    EXPECT_TRUE(wrapper.isReady());
    EXPECT_STREQ("FINISHED", wrapper.stateString());
    EXPECT_EQ("", transport.outputLog);
    EXPECT_EQ("indexNotFound: Index not found for tableId 10, indexId 2",
                TestLog::get());
}

}  // namespace RAMCloud
