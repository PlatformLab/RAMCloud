/* Copyright (c) 2012-2013 Stanford University
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
#include "CoordinatorSession.h"
#include "MockTransport.h"
#include "CoordinatorRpcWrapper.h"
#include "TransportManager.h"

namespace RAMCloud {

class CoordinatorRpcWrapperTest : public ::testing::Test {
  public:
    Context context;
    MockTransport transport;

    CoordinatorRpcWrapperTest()
        : context()
        , transport(&context)
    {
        context.transportManager->registerMock(&transport);
        context.coordinatorSession->setLocation("mock:");
    }

    ~CoordinatorRpcWrapperTest()
    {
    }

    DISALLOW_COPY_AND_ASSIGN(CoordinatorRpcWrapperTest);
};

TEST_F(CoordinatorRpcWrapperTest, handleTransportError) {
    TestLog::Enable _("flush");
    CoordinatorRpcWrapper wrapper(&context, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    wrapper.state = RpcWrapper::RpcState::FAILED;
    EXPECT_FALSE(wrapper.isReady());
    EXPECT_STREQ("IN_PROGRESS", wrapper.stateString());
    EXPECT_EQ("flush: flushing session", TestLog::get());
}

TEST_F(CoordinatorRpcWrapperTest, send) {
    CoordinatorRpcWrapper wrapper(&context, 4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    EXPECT_STREQ("IN_PROGRESS", wrapper.stateString());
    EXPECT_EQ("sendRequest: 100", transport.outputLog);
    EXPECT_EQ("mock:", wrapper.session->serviceLocator);
}

}  // namespace RAMCloud
