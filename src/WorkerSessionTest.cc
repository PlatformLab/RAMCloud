/* Copyright (c) 2012-2014 Stanford University
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
#include "MockTransport.h"
#include "WorkerSession.h"

namespace RAMCloud {

class WorkerSessionTest : public ::testing::Test {
  public:
    Context context;
    MockTransport transport;
    MockWrapper rpc;
    WorkerSessionTest()
        : context()
        , transport(&context)
        , rpc("abcdefg")
    {}
    DISALLOW_COPY_AND_ASSIGN(WorkerSessionTest);
};

// The following tests both the constructor and the sendRequest method
// for WorkerSession.
TEST_F(WorkerSessionTest, basics) {
    MockTransport::sessionDeleteCount = 0;

    Transport::SessionRef wrappedSession = new WorkerSession(
            &context, transport.getSession());

    // Make sure that sendRequest gets passed down to the underlying session.
    wrappedSession->sendRequest(&rpc.request, &rpc.response, &rpc);
    context.dispatchExec->poll();
    EXPECT_STREQ("sendRequest: abcdefg", transport.outputLog.c_str());
    EXPECT_EQ(0U, MockTransport::sessionDeleteCount);

    // Make sure that sessions get cleaned up properly.
    wrappedSession = NULL;
    EXPECT_EQ(1U, MockTransport::sessionDeleteCount);
}

TEST_F(WorkerSessionTest, abort) {
    WorkerSession* wrappedSession = new WorkerSession(
            &context, transport.getSession());

    wrappedSession->abort();
    EXPECT_STREQ("abort: ", transport.outputLog.c_str());
}

TEST_F(WorkerSessionTest, cancelRequest) {
    WorkerSession* wrappedSession = new WorkerSession(
            &context, transport.getSession());
    wrappedSession->sendRequest(&rpc.request, &rpc.response, &rpc);
    wrappedSession->cancelRequest(&rpc);
    context.dispatchExec->poll();
    EXPECT_STREQ("sendRequest: abcdefg | cancel: ",
            transport.outputLog.c_str());
}

TEST_F(WorkerSessionTest, getRpcInfo) {
    WorkerSession* wrappedSession = new WorkerSession(
            &context, transport.getSession());
    EXPECT_STREQ("dummy RPC info for MockTransport",
            wrappedSession->getRpcInfo().c_str());
}

TEST_F(WorkerSessionTest, getServiceLocator) {
    WorkerSession* wrappedSession = new WorkerSession(
            &context, transport.getSession());
    EXPECT_STREQ("test:",
            wrappedSession->serviceLocator.c_str());
}

TEST_F(WorkerSessionTest, sendRequest) {
    WorkerSession* wrappedSession = new WorkerSession(
            &context, transport.getSession());
    wrappedSession->sendRequest(&rpc.request, &rpc.response, &rpc);
    context.dispatchExec->poll();
    EXPECT_STREQ("sendRequest: abcdefg", transport.outputLog.c_str());
}

// The next test makes sure that sendRequest synchronizes properly with the
// dispatch thread.

void testDispatchSync(Context* context, MockWrapper* rpc,
        WorkerSession* session)
{
    session->sendRequest(&rpc->request, &rpc->response, rpc);
}

TEST_F(WorkerSessionTest, sendRequest_syncWithDispatchThread) {
    Context context(true);
    TestLog::Enable logSilencer;

    MockTransport transport(&context);
    MockWrapper rpc("abcdefg");
    WorkerSession session(&context, transport.getSession());
    std::thread child(testDispatchSync, &context, &rpc, &session);

    // Make sure the child hangs in sendRequest until we invoke the dispatcher.
    // See "Timing-Dependent Tests" in designNotes.
    usleep(1000);
    EXPECT_STREQ("", transport.outputLog.c_str());
    for (int i = 0; i < 1000; i++) {
        context.dispatch->poll();
        if (transport.outputLog.size() > 0) {
            break;
        }
        usleep(1000);
    }
    EXPECT_STREQ("sendRequest: abcdefg", transport.outputLog.c_str());
    child.join();
}

} // namespace RAMCloud
