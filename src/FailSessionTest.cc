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
#include "FailSession.h"

namespace RAMCloud {

class TestNotifier : public Transport::RpcNotifier {
  public:
    bool failedInvoked;
    TestNotifier() : failedInvoked(false) {}
    void completed() {}
    void failed() {failedInvoked = true;}
};

TEST(FailSessionTest, basics) {
    TestLog::Enable _;
    Transport::SessionRef session = FailSession::get();
    TestNotifier notifier;
    EXPECT_FALSE(notifier.failedInvoked);
    EXPECT_EQ(FailSession::get(), session);
    session->sendRequest(NULL, NULL, &notifier);
    EXPECT_TRUE(notifier.failedInvoked);
}

}  // namespace RAMCloud
