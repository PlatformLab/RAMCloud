/* Copyright (c) 2012-2014 Stanford University
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
#include "RpcWrapper.h"
#include "Service.h"
#include "ShortMacros.h"

namespace RAMCloud {

class LogRpcWrapper : public RpcWrapper {
  public:
    explicit LogRpcWrapper(uint32_t responseHeaderLength)
        : RpcWrapper(responseHeaderLength)
    {}
    virtual bool checkStatus() {
        TEST_LOG("checkStatus called");
        return true;
    }
    virtual bool handleTransportError() {
        TEST_LOG("handleTransportError called");
        return true;
    }
};

class RpcWrapperTest : public ::testing::Test {
  public:
    Context context;
    ServiceLocator locator;
    MockTransport transport;
    Transport::SessionRef session;

    RpcWrapperTest()
        : context()
        , locator("test:server=1")
        , transport(&context)
        , session(transport.getSession(locator))
    {}

    ~RpcWrapperTest()
    {}

    void
    setStatus(Buffer* buffer, Status status) {
        buffer->emplaceAppend<WireFormat::ResponseCommon>()->status = status;
    }

    DISALLOW_COPY_AND_ASSIGN(RpcWrapperTest);
};

TEST_F(RpcWrapperTest, constructor_explicit_buffer) {
    Buffer buffer;
    RpcWrapper wrapper(100, &buffer);
    wrapper.response->fillFromString("abcde");
    EXPECT_EQ("abcde/0", TestUtil::toString(&buffer));
}

TEST_F(RpcWrapperTest, destructor_cancel) {
    Tub<RpcWrapper> wrapper1, wrapper2;
    wrapper1.construct(100);
    wrapper2.construct(100);
    wrapper2->send();
    wrapper1.destroy();
    EXPECT_EQ("", transport.outputLog);
    wrapper2->session = session;
    wrapper2.destroy();
    EXPECT_EQ("cancel: ", transport.outputLog);
}

TEST_F(RpcWrapperTest, cancel) {
    RpcWrapper wrapper(100);
    wrapper.cancel();
    EXPECT_EQ("", transport.outputLog);
    EXPECT_STREQ("CANCELED", wrapper.stateString());
    wrapper.send();
    wrapper.session = session;
    wrapper.cancel();
    EXPECT_EQ("cancel: ", transport.outputLog);
}

TEST_F(RpcWrapperTest, completed) {
    RpcWrapper wrapper(100);
    wrapper.completed();
    EXPECT_STREQ("FINISHED", wrapper.stateString());
}

TEST_F(RpcWrapperTest, failed) {
    RpcWrapper wrapper(100);
    wrapper.failed();
    EXPECT_STREQ("FAILED", wrapper.stateString());
}

TEST_F(RpcWrapperTest, isReady_finished) {
    RpcWrapper wrapper(4);
    wrapper.state = RpcWrapper::RpcState::FINISHED;
    setStatus(wrapper.response, Status::STATUS_OK);
    EXPECT_EQ(4U, wrapper.response->size());
    EXPECT_TRUE(wrapper.isReady());
}

TEST_F(RpcWrapperTest, isReady_shortResponseWithErrorStatus) {
    TestLog::Enable _;
    RpcWrapper wrapper(8);
    wrapper.state = RpcWrapper::RpcState::FINISHED;
    wrapper.session = session;
    setStatus(wrapper.response, Status::STATUS_UNIMPLEMENTED_REQUEST);
    EXPECT_TRUE(wrapper.isReady());
    EXPECT_EQ("", TestLog::get());
    EXPECT_STREQ("FINISHED", wrapper.stateString());
}

TEST_F(RpcWrapperTest, isReady_responseTooShort) {
    TestLog::Enable _;
    RpcWrapper wrapper(5);
    wrapper.state = RpcWrapper::RpcState::FINISHED;
    wrapper.session = session;
    setStatus(wrapper.response, Status::STATUS_OK);
    EXPECT_THROW(wrapper.isReady(), MessageTooShortError);
    EXPECT_EQ("isReady: Response from test:server=1 for null RPC "
            "is too short (needed at least 5 bytes, got 4)",
            TestLog::get());
}

TEST_F(RpcWrapperTest, isReady_retryStatusDefault) {
    TestLog::Enable _;
    RpcWrapper wrapper(4);
    wrapper.session = session;
    setStatus(wrapper.response, Status::STATUS_RETRY);
    wrapper.state = RpcWrapper::RpcState::FINISHED;
    Cycles::mockTscValue = 1000;
    EXPECT_FALSE(wrapper.isReady());
    EXPECT_EQ("isReady: Server test:server=1 returned STATUS_RETRY "
            "from null request", TestLog::get());
    EXPECT_STREQ("RETRY", wrapper.stateString());
    uint64_t delay = wrapper.retryTime - 1000;
    EXPECT_LE(Cycles::fromSeconds(100e-06), delay);
    EXPECT_GE(Cycles::fromSeconds(200e-06), delay);
    Cycles::mockTscValue = 0;
}
TEST_F(RpcWrapperTest, isReady_infoInResponse) {
    TestLog::Enable _;
    RpcWrapper wrapper(4);
    wrapper.session = session;
    Service::prepareRetryResponse(wrapper.response, 1000, 2000,
            "sample message");
    wrapper.state = RpcWrapper::RpcState::FINISHED;
    Cycles::mockTscValue = 1000;
    EXPECT_FALSE(wrapper.isReady());
    EXPECT_EQ("isReady: Server test:server=1 returned STATUS_RETRY "
            "from null request: sample message", TestLog::get());
    EXPECT_STREQ("RETRY", wrapper.stateString());
    uint64_t delay = wrapper.retryTime - 1000;
    EXPECT_LE(Cycles::fromSeconds(1.0e-03), delay);
    EXPECT_GE(Cycles::fromSeconds(2.0e-03), delay);
    Cycles::mockTscValue = 1000;
}
TEST_F(RpcWrapperTest, isReady_retryResponseTooShort) {
    TestLog::Enable _;
    RpcWrapper wrapper(4);
    wrapper.session = session;
    Service::prepareRetryResponse(wrapper.response, 1000, 2000,
            "sample message");
    wrapper.response->truncate(wrapper.response->size() - 2);
    wrapper.state = RpcWrapper::RpcState::FINISHED;
    EXPECT_FALSE(wrapper.isReady());
    EXPECT_EQ("isReady: Server test:server=1 returned STATUS_RETRY "
            "from null request: <response format error>", TestLog::get());
}

TEST_F(RpcWrapperTest, isReady_callCheckStatus) {
    TestLog::Enable _;
    LogRpcWrapper wrapper(4);
    wrapper.session = session;
    setStatus(wrapper.response, Status::STATUS_UNIMPLEMENTED_REQUEST);
    wrapper.state = RpcWrapper::RpcState::FINISHED;
    EXPECT_TRUE(wrapper.isReady());
    EXPECT_EQ("checkStatus: checkStatus called", TestLog::get());
}

TEST_F(RpcWrapperTest, isReady_inProgress) {
    TestLog::Enable _;
    RpcWrapper wrapper(4);
    wrapper.session = session;
    wrapper.state = RpcWrapper::RpcState::IN_PROGRESS;
    EXPECT_FALSE(wrapper.isReady());
    EXPECT_STREQ("IN_PROGRESS", wrapper.stateString());
}

TEST_F(RpcWrapperTest, isReady_retry) {
    RpcWrapper wrapper(4);
    wrapper.session = session;
    wrapper.state = RpcWrapper::RpcState::RETRY;

    // First attempt: retry interval has not completed.
    wrapper.retryTime = Cycles::rdtsc() + Cycles::fromSeconds(100);
    EXPECT_FALSE(wrapper.isReady());
    EXPECT_STREQ("RETRY", wrapper.stateString());

    // Second attempt: retry interval has completed.
    wrapper.retryTime = Cycles::rdtsc();
    EXPECT_FALSE(wrapper.isReady());
    EXPECT_STREQ("IN_PROGRESS", wrapper.stateString());
}

TEST_F(RpcWrapperTest, isReady_canceled) {
    TestLog::Enable _;
    RpcWrapper wrapper(4);
    wrapper.session = session;
    wrapper.state = RpcWrapper::RpcState::CANCELED;
    EXPECT_TRUE(wrapper.isReady());
    EXPECT_STREQ("CANCELED", wrapper.stateString());
}

TEST_F(RpcWrapperTest, isReady_failed) {
    TestLog::Enable _;
    LogRpcWrapper wrapper(4);
    wrapper.session = session;
    wrapper.state = RpcWrapper::RpcState::FAILED;
    EXPECT_TRUE(wrapper.isReady());
    EXPECT_EQ("handleTransportError: handleTransportError called",
            TestLog::get());
}

TEST_F(RpcWrapperTest, isReady_unknownState) {
    TestLog::Enable _;
    RpcWrapper wrapper(4);
    wrapper.session = session;
    int bogus = 99;
    wrapper.state = RpcWrapper::RpcState(bogus);
    EXPECT_THROW(wrapper.isReady(), InternalError);
    EXPECT_EQ("isReady: RpcWrapper::isReady found unknown state 99 "
            "for null request", TestLog::get());
}

TEST_F(RpcWrapperTest, retry) {
    TestLog::Enable _;
    Cycles::mockTscValue = 1000;
    RpcWrapper wrapper(4);
    wrapper.retry(100, 100);
    EXPECT_STREQ("RETRY", wrapper.stateString());
    EXPECT_FALSE(wrapper.isReady());
    EXPECT_EQ(100, int(1e06*Cycles::toSeconds(wrapper.retryTime - 1000)
            + 0.5));
    Cycles::mockTscValue = 0;
}
TEST_F(RpcWrapperTest, retry_randomization) {
    TestLog::Enable _;
    Cycles::mockTscValue = 1000;
    RpcWrapper wrapper(4);
    wrapper.retry(1000000, 2000000);
    uint64_t time1 = wrapper.retryTime - 1000;
    wrapper.retry(1000000, 2000000);
    uint64_t time2 = wrapper.retryTime - 1000;
    wrapper.retry(1000000, 2000000);
    uint64_t time3 = wrapper.retryTime - 1000;
    EXPECT_NE(time1, time2);
    EXPECT_NE(time1, time3);
    uint64_t oneSecond = Cycles::fromSeconds(1.0);
    EXPECT_LE(oneSecond, time1);
    EXPECT_GE(2*oneSecond, time1);
    EXPECT_LE(oneSecond, time2);
    EXPECT_GE(2*oneSecond, time2);
    EXPECT_LE(oneSecond, time3);
    EXPECT_GE(2*oneSecond, time3);
    Cycles::mockTscValue = 0;
}

TEST_F(RpcWrapperTest, send) {
    RpcWrapper wrapper(100);
    wrapper.send();
    EXPECT_STREQ("IN_PROGRESS", wrapper.stateString());
    EXPECT_EQ("", transport.outputLog);
    wrapper.state = RpcWrapper::RpcState::FAILED;
    wrapper.session = session;
    wrapper.send();
    EXPECT_STREQ("IN_PROGRESS", wrapper.stateString());
    EXPECT_EQ("sendRequest: ", transport.outputLog);
}

TEST_F(RpcWrapperTest, simpleWait_success) {
    TestLog::Enable _;
    RpcWrapper wrapper(4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    wrapper.response->emplaceAppend<WireFormat::ResponseCommon>()->status =
            STATUS_OK;
    wrapper.state = RpcWrapper::RpcState::FINISHED;
    wrapper.simpleWait(context.dispatch);
}

TEST_F(RpcWrapperTest, simpleWait_errorStatus) {
    TestLog::Enable _;
    RpcWrapper wrapper(4);
    wrapper.request.fillFromString("100");
    wrapper.send();
    wrapper.response->emplaceAppend<WireFormat::ResponseCommon>()->status =
            STATUS_UNIMPLEMENTED_REQUEST;
    wrapper.state = RpcWrapper::RpcState::FINISHED;
    string message = "no exception";
    try {
        wrapper.simpleWait(context.dispatch);
    }
    catch (ClientException& e) {
        message = e.toSymbol();
    }
    EXPECT_EQ("STATUS_UNIMPLEMENTED_REQUEST", message);
}

TEST_F(RpcWrapperTest, waitInternal_timeout) {
    TestLog::Enable _;
    RpcWrapper wrapper(4);
    wrapper.session = session;
    wrapper.send();
    uint64_t start = Cycles::rdtsc();
    EXPECT_FALSE(wrapper.waitInternal(context.dispatch,
            start + Cycles::fromSeconds(100e-06)));
    double elapsed = 1e06 * Cycles::toSeconds(Cycles::rdtsc() - start);
    EXPECT_LE(100.0, elapsed);
    EXPECT_GE(2000.0, elapsed);
}

// Helper function that runs in a separate thread for the following tess.
static void waitTestThread(Dispatch* dispatch, RpcWrapper* wrapper) {
    wrapper->waitInternal(dispatch);
    TEST_LOG("wrapper finished");
}

TEST_F(RpcWrapperTest, waitInternal_delayedCompletion) {
    TestLog::Enable _;
    RpcWrapper wrapper(4);
    wrapper.session = session;
    wrapper.send();
    std::thread thread(waitTestThread, context.dispatch, &wrapper);
    usleep(1000);
    EXPECT_EQ("", TestLog::get());
    setStatus(wrapper.response, Status::STATUS_OK);
    wrapper.completed();

    // Give the waiting thread a chance to finish.
    // See "Timing-Dependent Tests" in designNotes.
    for (int i = 0; i < 1000; i++) {
        if (TestLog::get().size() != 0)
            break;
        usleep(100);
    }
    EXPECT_EQ("waitTestThread: wrapper finished", TestLog::get());
    thread.join();
}

TEST_F(RpcWrapperTest, waitInternal_canceled) {
    TestLog::Enable _;
    RpcWrapper wrapper(4);
    wrapper.state = RpcWrapper::RpcState::CANCELED;
    string message = "no exception";
    try {
        EXPECT_TRUE(wrapper.waitInternal(context.dispatch));
    }
    catch (RpcCanceledException& e) {
        message = "RpcCanceledException";
    }
    EXPECT_EQ("RpcCanceledException", message);
}

TEST_F(RpcWrapperTest, waitInternal_simpleSuccess) {
    TestLog::Enable _;
    RpcWrapper wrapper(4);
    wrapper.session = session;
    wrapper.send();
    setStatus(wrapper.response, Status::STATUS_OK);
    wrapper.completed();
    EXPECT_TRUE(wrapper.waitInternal(context.dispatch));
}

}  // namespace RAMCloud
