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
#include "WireFormat.h"

namespace RAMCloud {

class LinearizableObjectRpcWrapperTest : public ::testing::Test {
  public:
    RamCloud ramcloud;
    MockTransport transport;

    LinearizableObjectRpcWrapperTest()
        : ramcloud("mock:")
        , transport(ramcloud.clientContext)
    {
        ramcloud.clientContext->transportManager->registerMock(&transport);
    }

    ~LinearizableObjectRpcWrapperTest()
    {
    }

    DISALLOW_COPY_AND_ASSIGN(LinearizableObjectRpcWrapperTest);
};

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

TEST_F(LinearizableObjectRpcWrapperTest, handleLinearizabilityResp_writeRpc) {
    LinearizableObjectRpcWrapper wrapper(&ramcloud, true, 10, "abc", 3, 4);
    WireFormat::Write::Request reqHdr;
    wrapper.fillLinearizabilityHeader<WireFormat::Write::Request>(&reqHdr);
    EXPECT_EQ(1UL, reqHdr.rpcId);
    EXPECT_EQ(0UL, reqHdr.ackId);

    {
        LinearizableObjectRpcWrapper wrapperTmp(&ramcloud, true, 10, "abc", 3, 4);
        wrapperTmp.fillLinearizabilityHeader<WireFormat::Write::Request>(&reqHdr);
        EXPECT_EQ(2UL, reqHdr.rpcId);
        EXPECT_EQ(0UL, reqHdr.ackId);
    }

    WireFormat::Write::Response respHdr;
    respHdr.common.status = STATUS_OK;
    wrapper.handleLinearizabilityResp<WireFormat::Write::Response>(&respHdr);

    {
        LinearizableObjectRpcWrapper wrapperTmp(&ramcloud, true, 10, "abc", 3, 4);
        WireFormat::Write::Request reqHdr;
        wrapperTmp.fillLinearizabilityHeader<WireFormat::Write::Request>(&reqHdr);
        EXPECT_EQ(3UL, reqHdr.rpcId);
        EXPECT_EQ(1UL, reqHdr.ackId);
    }
}

}  // namespace RAMCloud
