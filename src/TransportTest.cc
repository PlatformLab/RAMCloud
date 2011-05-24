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
#include "BenchUtil.h"
#include "Transport.h"

namespace RAMCloud {


// Wait for a given number of invocations, then mark an RPC as finished.
class TransportTestPoller : public Dispatch::Poller {
  public:
    TransportTestPoller(int *count, Transport::ClientRpc* rpc,
            string* errorMessage)
        : count(count), rpc(rpc), errorMessage(errorMessage) { }
    void poll() {
        (*count)--;
        if (*count <= 0) {
            rpc->markFinished(errorMessage);
        }
    }
    int *count;
    Transport::ClientRpc* rpc;
    string* errorMessage;
    DISALLOW_COPY_AND_ASSIGN(TransportTestPoller);
};


class TransportTest : public ::testing::Test {
  public:
    TransportTest() { }
    ~TransportTest() { }
    DISALLOW_COPY_AND_ASSIGN(TransportTest);
};

TEST_F(TransportTest, wait_noError) {
    delete dispatch;
    dispatch = new Dispatch;
    int count = 3;
    Transport::ClientRpc rpc;
    TransportTestPoller poller(&count, &rpc, NULL);
    rpc.wait();
    EXPECT_EQ(0, count);
}

void waitOnRpc(Transport::ClientRpc* rpc, const char** state) {
    rpc->wait();
    *state = "finished";
};

TEST_F(TransportTest, wait_notDispatchThread) {
    delete dispatch;
    dispatch = new Dispatch;
    int count = 3;
    Transport::ClientRpc rpc;
    TransportTestPoller poller(&count, &rpc, NULL);
    const char *state = "not finished";
    boost::thread(waitOnRpc, &rpc, &state).detach();

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
    delete dispatch;
    dispatch = new Dispatch;
    int count = 3;
    Transport::ClientRpc rpc;
    string error("test error message");
    TransportTestPoller poller(&count, &rpc, &error);
    string message("no exception");
    try {
        rpc.wait();
    } catch (TransportException& e) {
        message = e.message;
    }
    EXPECT_EQ("test error message", message);
}

// No separate tests for markFinished: it's already fully tested by
// the tests for wait.

} // namespace RAMCloud
