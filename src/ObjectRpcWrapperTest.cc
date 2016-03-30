/* Copyright (c) 2013-2015 Stanford University
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
#include "ObjectFinder.h"
#include "ObjectRpcWrapper.h"
#include "ShortMacros.h"

namespace RAMCloud {

// This class provides tablet map info to ObjectFinder, with a
// different locator each time it is invoked.
class ObjRpcWrapperRefresher : public ObjectFinder::TableConfigFetcher {
  public:
    ObjRpcWrapperRefresher() : called(0) {}
    void getTableConfig(
        uint64_t tableId,
        std::map<TabletKey, TabletWithLocator>* tableMap,
        std::multimap< std::pair<uint64_t, uint8_t>,
                                    ObjectFinder::Indexlet>* tableIndexMap) {

        called++;
        char buffer[100];
        snprintf(buffer, sizeof(buffer), "mock:refresh=%d", called);

        tableMap->clear();
        Tablet rawEntry({10, 0, uint64_t(~0), ServerId(),
                    Tablet::NORMAL, LogPosition()});
        TabletWithLocator entry(rawEntry, buffer);

        TabletKey key {entry.tablet.tableId, entry.tablet.startKeyHash};
        tableMap->insert(std::make_pair(key, entry));
    }
    uint32_t called;
};

class ObjectRpcWrapperTest : public ::testing::Test {
  public:
    RamCloud ramcloud;
    MockTransport transport;

    ObjectRpcWrapperTest()
        : ramcloud("mock:")
        , transport(ramcloud.clientContext)
    {
        ramcloud.clientContext->objectFinder->tableConfigFetcher.reset(
                new ObjRpcWrapperRefresher);
        ramcloud.clientContext->transportManager->registerMock(&transport);
    }

    ~ObjectRpcWrapperTest()
    {
    }

    DISALLOW_COPY_AND_ASSIGN(ObjectRpcWrapperTest);
};

TEST_F(ObjectRpcWrapperTest, checkStatus_unknownTablet) {
    TestLog::Enable _;
    ObjectRpcWrapper wrapper(ramcloud.clientContext, 10, "abc", 3, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    EXPECT_EQ("mock:refresh=1", wrapper.session->getServiceLocator());
    wrapper.response->emplaceAppend<WireFormat::ResponseCommon>()->status =
            STATUS_UNKNOWN_TABLET;
    wrapper.state = RpcWrapper::RpcState::FINISHED;
    EXPECT_FALSE(wrapper.isReady());
    EXPECT_STREQ("IN_PROGRESS", wrapper.stateString());
    EXPECT_EQ("checkStatus: Server mock:refresh=1 doesn't store "
            "<10, 0xac94bec52029fa02>; refreshing object map",
            TestLog::get());
    EXPECT_EQ("mock:refresh=2", wrapper.session->getServiceLocator());
}

TEST_F(ObjectRpcWrapperTest, checkStatus_otherError) {
    TestLog::Enable _;
    ObjectRpcWrapper wrapper(ramcloud.clientContext, 10, "abc", 3, 4);
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

TEST_F(ObjectRpcWrapperTest, handleTransportError) {
    TestLog::Enable _;
    ObjectRpcWrapper wrapper(ramcloud.clientContext, 10, "abc", 3, 4);
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

TEST_F(ObjectRpcWrapperTest, send) {
    ObjectRpcWrapper wrapper(ramcloud.clientContext, 10, "abc", 3, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    EXPECT_STREQ("IN_PROGRESS", wrapper.stateString());
    EXPECT_EQ("sendRequest: 100", transport.outputLog);
    EXPECT_EQ("mock:refresh=1", wrapper.session->getServiceLocator());
}


}  // namespace RAMCloud
