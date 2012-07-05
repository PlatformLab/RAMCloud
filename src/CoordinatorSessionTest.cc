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
#include "CoordinatorSession.h"
#include "FailSession.h"

namespace RAMCloud {

class CoordinatorSessionTest : public ::testing::Test {
  public:
    Context context;
    MockTransport transport;

    CoordinatorSessionTest()
        : context()
        , transport(context)
    {
        context.transportManager->registerMock(&transport);
    }

    ~CoordinatorSessionTest()
    {
        context.transportManager->unregisterMock();
    }

    DISALLOW_COPY_AND_ASSIGN(CoordinatorSessionTest);
};

// The following test covers most of the functionality of this class.
TEST_F(CoordinatorSessionTest, basics) {
    CoordinatorSession cs(context);
    cs.setLocation("mock:");
    EXPECT_TRUE(cs.getSession() != NULL);
    EXPECT_EQ(1U, transport.sessionCreateCount);
    cs.getSession();
    EXPECT_EQ(1U, transport.sessionCreateCount);
    cs.flush();
    cs.getSession();
    EXPECT_EQ(2U, transport.sessionCreateCount);
    EXPECT_EQ("mock:", cs.getLocation());
}

TEST_F(CoordinatorSessionTest, getSession_noLocator) {
    CoordinatorSession cs(context);
    string message = "no exception";
    try {
        cs.getSession();
    } catch (FatalError& e) {
        EXPECT_EQ("CoordinatorSession::setLocation never invoked", e.message);
        message = "exception occurred";
    }
    EXPECT_EQ("exception occurred", message);
}

TEST_F(CoordinatorSessionTest, getSession_cantOpenSession) {
    TestLog::Enable _;
    CoordinatorSession cs(context);
    string message = "no exception";
    cs.setLocation("mock:host=error");
    EXPECT_EQ(cs.getSession(), FailSession::get());
}

}  // namespace RAMCloud
