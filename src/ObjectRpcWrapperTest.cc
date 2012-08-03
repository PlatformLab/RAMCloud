/* Copyright (c) 2012 Stanford University
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
#include "ObjectRpcWrapper.h"
#include "ShortMacros.h"

namespace RAMCloud {

// This class provides tablet map info to ObjectFinder, with a
// different locator each time it is invoked.
class ObjRpcWrapperRefresher : public ObjectFinder::TabletMapFetcher {
  public:
    ObjRpcWrapperRefresher() : called(0) {}
    void getTabletMap(ProtoBuf::Tablets& tabletMap) {
        called++;
        char buffer[100];
        snprintf(buffer, sizeof(buffer), "mock:refresh=%d", called);

        tabletMap.clear_tablet();
        ProtoBuf::Tablets_Tablet& entry(*tabletMap.add_tablet());
        entry.set_table_id(10);
        entry.set_start_key_hash(0);
        entry.set_end_key_hash(~0UL);
        entry.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
        entry.set_service_locator(buffer);
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
        ramcloud.objectFinder.tabletMapFetcher.reset(
                new ObjRpcWrapperRefresher);
        ramcloud.clientContext.transportManager->registerMock(&transport);
    }

    ~ObjectRpcWrapperTest()
    {
        ramcloud.clientContext.transportManager->unregisterMock();
    }

    DISALLOW_COPY_AND_ASSIGN(ObjectRpcWrapperTest);
};

TEST_F(ObjectRpcWrapperTest, checkStatus_unknownTablet) {
    TestLog::Enable _;
    ObjectRpcWrapper wrapper(ramcloud, 10, "abc", 3, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    EXPECT_EQ("mock:refresh=1", wrapper.session->getServiceLocator());
    (new(wrapper.response, APPEND) WireFormat::ResponseCommon)->status =
            STATUS_UNKNOWN_TABLE;
    wrapper.state = RpcWrapper::RpcState::FINISHED;
    EXPECT_FALSE(wrapper.isReady());
    EXPECT_STREQ("IN_PROGRESS", wrapper.stateString());
    EXPECT_EQ("checkStatus: Server mock:refresh=1 doesn't store "
            "<10, 0xb4963f3f3fad7867>; refreshing object map | "
            "flush: flushing object map",
            TestLog::get());
    EXPECT_EQ("mock:refresh=2", wrapper.session->getServiceLocator());
}

TEST_F(ObjectRpcWrapperTest, checkStatus_otherError) {
    TestLog::Enable _;
    ObjectRpcWrapper wrapper(ramcloud, 10, "abc", 3, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    (new(wrapper.response, APPEND) WireFormat::ResponseCommon)->status =
            STATUS_UNIMPLEMENTED_REQUEST;
    wrapper.state = RpcWrapper::RpcState::FINISHED;
    EXPECT_TRUE(wrapper.isReady());
    EXPECT_STREQ("FINISHED", wrapper.stateString());
    EXPECT_EQ("", TestLog::get());
    EXPECT_EQ("mock:refresh=1", wrapper.session->getServiceLocator());
}

TEST_F(ObjectRpcWrapperTest, handleTransportError) {
    TestLog::Enable _;
    ObjectRpcWrapper wrapper(ramcloud, 10, "abc", 3, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    EXPECT_EQ("mock:refresh=1", wrapper.session->getServiceLocator());
    wrapper.state = RpcWrapper::RpcState::FAILED;
    EXPECT_FALSE(wrapper.isReady());
    EXPECT_STREQ("IN_PROGRESS", wrapper.stateString());
    EXPECT_EQ("flushSession: flushing session for mock:refresh=1 "
            "| flush: flushing object map", TestLog::get());
    EXPECT_EQ("mock:refresh=2", wrapper.session->getServiceLocator());
}

TEST_F(ObjectRpcWrapperTest, send) {
    ObjectRpcWrapper wrapper(ramcloud, 10, "abc", 3, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    EXPECT_STREQ("IN_PROGRESS", wrapper.stateString());
    EXPECT_EQ("sendRequest: 100", transport.outputLog);
    EXPECT_EQ("mock:refresh=1", wrapper.session->getServiceLocator());
}


}  // namespace RAMCloud
