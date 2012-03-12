/* Copyright (c) 2010 Stanford University
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
#include "Client.h"
#include "MockTransport.h"
#include "TransportManager.h"

namespace RAMCloud {

class MockTask {
  public:
    MockTask()
        : state(NOT_SENT)
    {
    }
    bool isReady() {
        if (state == SENT) {
            // randomly make progress
            if ((generateRandom() & 255) == 1)
                state = READY;
            return false;
        }
        return state == READY;
    }
    bool isDone() {
        return state == DONE;
    }
    void send() {
        EXPECT_EQ(NOT_SENT, state);
        state = SENT;
    }
    void wait() {
        EXPECT_EQ(READY, state);
        state = DONE;
    }
  private:
    enum { NOT_SENT, SENT, READY, DONE } state;
    DISALLOW_COPY_AND_ASSIGN(MockTask);
};

class MockRestartingTask {
  public:
    MockRestartingTask()
        : round(0)
        , state(NOT_SENT)
    {
    }
    bool isReady() {
        if (state == SENT) {
            // randomly make progress
            if ((generateRandom() & 255) == 1)
                state = READY;
            return false;
        }
        return state == READY;
    }
    bool isDone() {
        return state == DONE;
    }
    void send() {
        EXPECT_EQ(NOT_SENT, state);
        state = SENT;
    }
    void wait() {
        EXPECT_EQ(READY, state);
        if (round == 5) {
            state = DONE;
        } else {
            ++round;
            state = SENT;
        }
    }
  private:
    int round;
    enum { NOT_SENT, SENT, READY, DONE } state;
    DISALLOW_COPY_AND_ASSIGN(MockRestartingTask);
};

struct TestRpc {
    static const RpcOpcode opcode = PING;
    static const ServiceType service = MASTER_SERVICE; // anything is fine.
    struct Request {
        // set x to garbage to test that it's zeroed later
        Request() : common(), x(0xcccccccc) {}
        RpcRequestCommon common;
        uint32_t x;
    };
    struct Response {
        RpcResponseCommon common;
        uint32_t y;
    };
};

class ClientTest : public ::testing::Test {
  public:
    Client client;
    MockTransport* transport;
    Transport::SessionRef session;

    ClientTest() : client(), transport(NULL), session()
    {
        client.status = STATUS_OK;
        transport = new MockTransport();
        Context::get().transportManager->registerMock(transport);
        session = transport->getSession();
    }

    ~ClientTest()
    {
        Context::get().transportManager->unregisterMock();
        delete transport;
    }

    DISALLOW_COPY_AND_ASSIGN(ClientTest);
};

// maxOutstanding == 1
TEST_F(ClientTest, sequential) {
    for (uint32_t i = 0; i < 100; ++i) {
        Tub<MockTask> tasks[5];
        foreach (auto& task, tasks)
            task.construct();
        parallelRun(tasks, arrayLength(tasks), 1);
        foreach (auto& task, tasks)
            EXPECT_TRUE(task->isDone());
    }
}

// numTasks <= maxOutstanding
TEST_F(ClientTest, startAllInitially) {
    for (uint32_t i = 0; i < 100; ++i) {
        Tub<MockTask> tasks[5];
        foreach (auto& task, tasks)
            task.construct();
        parallelRun(tasks, arrayLength(tasks), 5);
        foreach (auto& task, tasks)
            EXPECT_TRUE(task->isDone());
    }
}

// numTasks > maxOutstanding
TEST_F(ClientTest, normal) {
    for (uint32_t i = 0; i < 100; ++i) {
        Tub<MockTask> tasks[10];
        foreach (auto& task, tasks)
            task.construct();
        parallelRun(tasks, arrayLength(tasks), 4);
        foreach (auto& task, tasks)
            EXPECT_TRUE(task->isDone());
    }
}

TEST_F(ClientTest, restartingTasks) {
    for (uint32_t i = 0; i < 20; ++i) {
        Tub<MockRestartingTask> tasks[10];
        foreach (auto& task, tasks)
            task.construct();
        parallelRun(tasks, arrayLength(tasks), 4);
        foreach (auto& task, tasks)
            EXPECT_TRUE(task->isDone());
    }
}

TEST_F(ClientTest, allocHeader) {
    Buffer req;
    TestRpc::Request& reqHdr = client.allocHeader<TestRpc>(req);
    EXPECT_EQ(0U, reqHdr.x);
    EXPECT_EQ(PING, reqHdr.common.opcode);
}

TEST_F(ClientTest, sendRecv_normal) {
    Buffer req, resp;
    transport->setInput("3 0x12345678");
    const TestRpc::Response& respHdr(
        client.sendRecv<TestRpc>(session, req, resp));
    EXPECT_EQ(static_cast<Status>(3), client.status);
    EXPECT_EQ(0x12345678U, respHdr.y);
}

TEST_F(ClientTest, sendRecv_shortResponse) {
    Buffer req, resp;
    transport->setInput("");
    EXPECT_THROW(client.sendRecv<TestRpc>(session, req, resp),
                            ResponseFormatError);
}

TEST_F(ClientTest, checkStatus) {
    client.status = STATUS_MESSAGE_TOO_SHORT;
    EXPECT_THROW(client.checkStatus(HERE), MessageTooShortError);
}

TEST_F(ClientTest, throwShortResponseError) {
    Buffer b;
    Status status;

    // Response says "success".
    b.fillFromString("0 0");
    status = STATUS_OK;
    try {
        client.throwShortResponseError(b);
    } catch (ClientException& e) {
        status = e.status;
    }
    EXPECT_EQ(10, status);

    // Valid RpcResponseCommon with error status.
    b.reset();
    b.fillFromString("7 0");
    status = STATUS_OK;
    try {
        client.throwShortResponseError(b);
    } catch (ClientException& e) {
        status = e.status;
    }
    EXPECT_EQ(7, status);

    // Response too short for RpcResponseCommon.
    b.reset();
    b.fillFromString("a");
    status = STATUS_OK;
    try {
        client.throwShortResponseError(b);
    } catch (ClientException& e) {
        status = e.status;
    }
    EXPECT_EQ(10, status);
}

}  // namespace RAMCloud
