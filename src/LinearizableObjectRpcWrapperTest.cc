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
#include "LinearizableObjectRpcWrapper.h"
#include "MockCluster.h"
#include "WireFormat.h"

namespace RAMCloud {

class LinearizableObjectRpcWrapperTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    RamCloud ramcloud;
    MockCluster cluster;
    MockTransport transport;

    LinearizableObjectRpcWrapperTest()
        : logEnabler()
        , ramcloud("mock:")
        , cluster(ramcloud.clientContext)
        , transport(ramcloud.clientContext)
    {
        ramcloud.clientContext->transportManager->registerMock(&transport);
    }

    ~LinearizableObjectRpcWrapperTest()
    {
    }

    DISALLOW_COPY_AND_ASSIGN(LinearizableObjectRpcWrapperTest);
};

TEST_F(LinearizableObjectRpcWrapperTest, destroy_rpc_in_progress) {
    {
        LinearizableObjectRpcWrapper wrapper(&ramcloud, true, 10, "abc", 3, 4);
        WireFormat::Write::Request* reqHdr(
                wrapper.allocHeader<WireFormat::Write>());
        wrapper.fillLinearizabilityHeader<WireFormat::Write::Request>(reqHdr);
        EXPECT_EQ(0UL, ramcloud.clientContext->rpcTracker->ackId());
    }
    EXPECT_EQ(1UL, ramcloud.clientContext->rpcTracker->ackId());
}

TEST_F(LinearizableObjectRpcWrapperTest, fillLinearizabilityHeader_writeRpc) {
    LinearizableObjectRpcWrapper wrapper(&ramcloud, true, 10, "abc", 3, 4);
    WireFormat::Write::Request reqHdr;
    wrapper.fillLinearizabilityHeader<WireFormat::Write::Request>(&reqHdr);
    EXPECT_EQ(1UL, reqHdr.rpcId);
    EXPECT_EQ(0UL, reqHdr.ackId);

    LinearizableObjectRpcWrapper wrapper2(&ramcloud, false, 10, "abc", 3, 4);
    wrapper2.fillLinearizabilityHeader<WireFormat::Write::Request>(&reqHdr);
    EXPECT_EQ(0UL, reqHdr.rpcId);
    EXPECT_EQ(0UL, reqHdr.ackId);
}

TEST_F(LinearizableObjectRpcWrapperTest, rpcTracker_window_full_on_create) {
    LinearizableObjectRpcWrapper wrapper(&ramcloud, true, 10, "abc", 3, 4);
    WireFormat::Write::Request* reqHdr(
            wrapper.allocHeader<WireFormat::Write>());
    wrapper.fillLinearizabilityHeader<WireFormat::Write::Request>(reqHdr);

    WireFormat::Write::Response* resp =
            wrapper.response->emplaceAppend<WireFormat::Write::Response>();
    memset(resp, 0, sizeof(*resp));

    resp->common.status = STATUS_OK;
    wrapper.state = RpcWrapper::RpcState::FINISHED;

    EXPECT_EQ(0UL, ramcloud.clientContext->rpcTracker->ackId());
    EXPECT_FALSE(wrapper.responseProcessed);

    for (int i = 1; i < RpcTracker::windowSize; ++i) {
        ramcloud.clientContext->rpcTracker->newRpcId(
            reinterpret_cast<LinearizableObjectRpcWrapper*>(1));
    }
    EXPECT_FALSE(wrapper.responseProcessed);

    WireFormat::Write::Request reqHdr2;
    LinearizableObjectRpcWrapper wrapper2(&ramcloud, true, 10, "abc", 3, 4);
    wrapper2.fillLinearizabilityHeader<WireFormat::Write::Request>(&reqHdr2);

    EXPECT_TRUE(wrapper.responseProcessed);
    EXPECT_EQ((uint64_t) (RpcTracker::windowSize + 1), wrapper2.assignedRpcId);
    EXPECT_EQ((uint64_t) (RpcTracker::windowSize + 1), reqHdr2.rpcId);
    EXPECT_EQ(1UL, reqHdr2.ackId);
    EXPECT_EQ(1UL, ramcloud.clientContext->rpcTracker->ackId());

    wrapper.waitInternal(ramcloud.clientContext->dispatch);
}

TEST_F(LinearizableObjectRpcWrapperTest, waitInternal) {
    //1. Normal operation
    LinearizableObjectRpcWrapper wrapper(&ramcloud, true, 10, "abc", 3, 4);
    WireFormat::Write::Request* reqHdr(
            wrapper.allocHeader<WireFormat::Write>());
    wrapper.fillLinearizabilityHeader<WireFormat::Write::Request>(reqHdr);

    WireFormat::Write::Response* resp =
            wrapper.response->emplaceAppend<WireFormat::Write::Response>();
    memset(resp, 0, sizeof(*resp));

    resp->common.status = STATUS_OK;
    wrapper.state = RpcWrapper::RpcState::FINISHED;

    EXPECT_EQ(1UL, reqHdr->rpcId);
    EXPECT_EQ(0UL, reqHdr->ackId);

    EXPECT_EQ(0UL, ramcloud.clientContext->rpcTracker->ackId());
    wrapper.waitInternal(ramcloud.clientContext->dispatch);
    EXPECT_EQ(1UL, ramcloud.clientContext->rpcTracker->ackId());

    //2. CANCELLED operation.
    LinearizableObjectRpcWrapper wrapper2(&ramcloud, true, 10, "abc", 3, 4);
    reqHdr = wrapper2.allocHeader<WireFormat::Write>();
    wrapper2.fillLinearizabilityHeader<WireFormat::Write::Request>(reqHdr);

    resp = wrapper2.response->emplaceAppend<WireFormat::Write::Response>();
    memset(resp, 0, sizeof(*resp));

    EXPECT_EQ(2UL, reqHdr->rpcId);
    EXPECT_EQ(1UL, reqHdr->ackId);

    EXPECT_EQ(1UL, ramcloud.clientContext->rpcTracker->ackId());
    wrapper2.cancel();
    EXPECT_EQ(2UL, ramcloud.clientContext->rpcTracker->ackId());
}

}  // namespace RAMCloud
