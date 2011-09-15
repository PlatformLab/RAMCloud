/* Copyright (c) 2011 Stanford University
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
#include "Transport.h"

namespace RAMCloud {


// Wait for a given number of invocations, then mark an RPC as finished.
class TransportTestPoller : public Dispatch::Poller {
  public:
    TransportTestPoller(int *count, Transport::ClientRpc* rpc,
            const char* errorMessage)
        : Dispatch::Poller(*Context::get().dispatch),
          count(count), rpc(rpc), errorMessage(errorMessage) { }
    void poll() {
        (*count)--;
        if (*count <= 0) {
            rpc->markFinished(errorMessage);
        }
    }
    int *count;
    Transport::ClientRpc* rpc;
    const char* errorMessage;
    DISALLOW_COPY_AND_ASSIGN(TransportTestPoller);
};


class TransportTest : public ::testing::Test {
  public:
    TransportTest() { }
    ~TransportTest() { }
    DISALLOW_COPY_AND_ASSIGN(TransportTest);
};

TEST_F(TransportTest, wait_noError) {
    Context context(true);
    Context::Guard _(context);
    int count = 3;
    Transport::ClientRpc rpc;
    TransportTestPoller poller(&count, &rpc, NULL);
    rpc.wait();
    EXPECT_EQ(0, count);
}

void waitOnRpc(Context* context, Transport::ClientRpc* rpc, const char** state)
{
    Context::Guard _(*context);
    rpc->wait();
    *state = "finished";
};

TEST_F(TransportTest, wait_notDispatchThread) {
    Context context(true);
    Context::Guard _(context);
    int count = 3;
    Transport::ClientRpc rpc;
    TransportTestPoller poller(&count, &rpc, NULL);
    const char *state = "not finished";
    boost::thread(waitOnRpc, &Context::get(), &rpc, &state).detach();

    // Wait a while and make sure that the RPC hasn't finished, and
    // that the dispatcher hasn't been invoked.
    usleep(500);
    EXPECT_EQ(3, count);
    EXPECT_STREQ("not finished", state);

    // Manually finish the RPC, then make sure it really does finish
    // soon.
    rpc.markFinished();
    for (int i = 0; i < 1000; i++) {
        usleep(100);
        if (rpc.isReady()) {
            break;
        }
    }
    EXPECT_EQ(3, count);
    EXPECT_STREQ("finished", state);
}

TEST_F(TransportTest, wait_error) {
    Context context(true);
    Context::Guard _(context);
    int count = 3;
    Transport::ClientRpc rpc;
    TransportTestPoller poller(&count, &rpc, "test error message");
    string message("no exception");
    try {
        rpc.wait();
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("test error message", message);
}

TEST_F(TransportTest, markFinished) {
    // No error.
    Transport::ClientRpc rpc;
    EXPECT_FALSE(rpc.isReady());
    rpc.markFinished();
    EXPECT_TRUE(rpc.isReady());
    rpc.wait();

    // Error via char*.
    Transport::ClientRpc rpc2;
    rpc2.markFinished("error XXX");
    EXPECT_TRUE(rpc2.isReady());
    string message("no exception");
    try {
        rpc2.wait();
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("error XXX", message);

    // Error via string.
    Transport::ClientRpc rpc3;
    string msg2("error 123");
    rpc3.markFinished(msg2);
    EXPECT_TRUE(rpc3.isReady());
    message = "no exception";
    try {
        rpc3.wait();
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("error 123", message);
}

TEST_F(TransportTest, cancel_alreadyFinished) {
    Transport::ClientRpc rpc;
    rpc.markFinished();
    rpc.cancel();
    EXPECT_NO_THROW(rpc.wait());
}
TEST_F(TransportTest, cancel_stringArgument) {
    Transport::ClientRpc rpc;
    string s("test message");
    rpc.cancel(s);
    string message("no exception");
    try {
        rpc.wait();
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("RPC cancelled: test message", message);
}
TEST_F(TransportTest, cancel_charArgument) {
    Transport::ClientRpc rpc;
    rpc.cancel("message2");
    string message("no exception");
    try {
        rpc.wait();
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("RPC cancelled: message2", message);
}
TEST_F(TransportTest, cancel_defaultMessage) {
    Transport::ClientRpc rpc;
    rpc.cancel();
    string message("no exception");
    try {
        rpc.wait();
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("RPC cancelled", message);
}

} // namespace RAMCloud
