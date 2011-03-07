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

// maxOutstanding == 1
TEST(parallelRun, sequential) {
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
TEST(parallelRun, startAllInitially) {
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
TEST(parallelRun, normal) {
    for (uint32_t i = 0; i < 100; ++i) {
        Tub<MockTask> tasks[10];
        foreach (auto& task, tasks)
            task.construct();
        parallelRun(tasks, arrayLength(tasks), 4);
        foreach (auto& task, tasks)
            EXPECT_TRUE(task->isDone());
    }
}

TEST(parallelRun, restartingTasks) {
    for (uint32_t i = 0; i < 20; ++i) {
        Tub<MockRestartingTask> tasks[10];
        foreach (auto& task, tasks)
            task.construct();
        parallelRun(tasks, arrayLength(tasks), 4);
        foreach (auto& task, tasks)
            EXPECT_TRUE(task->isDone());
    }
}

/////

struct TestRpc {
    static const RpcType type = PING;
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

class ClientTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(ClientTest);
    CPPUNIT_TEST(test_allocHeader);
    CPPUNIT_TEST(test_sendRecv_normal);
    CPPUNIT_TEST(test_sendRecv_shortResponse);
    CPPUNIT_TEST(test_checkStatus);
    CPPUNIT_TEST(test_throwShortResponseError);
    CPPUNIT_TEST_SUITE_END();

    Client client;
    MockTransport* transport;
    Transport::SessionRef session;

  public:
    ClientTest() : client(), transport(NULL), session() {}

    void setUp() {
        client.status = STATUS_OK;
        transport = new MockTransport();
        transportManager.registerMock(transport);
        session = transport->getSession();
    }

    void tearDown() {
        transportManager.unregisterMock();
        delete transport;
    }

    void test_allocHeader() {
        Buffer req;
        TestRpc::Request& reqHdr = client.allocHeader<TestRpc>(req);
        CPPUNIT_ASSERT_EQUAL(0, reqHdr.x);
        CPPUNIT_ASSERT_EQUAL(PING, reqHdr.common.type);
    }

    void test_sendRecv_normal() {
        Buffer req, resp;
        transport->setInput("3 0x12345678");
        const TestRpc::Response& respHdr(
            client.sendRecv<TestRpc>(session, req, resp));
        CPPUNIT_ASSERT_EQUAL(static_cast<Status>(3), client.status);
        CPPUNIT_ASSERT_EQUAL(0x12345678, respHdr.y);
    }

    void test_sendRecv_shortResponse() {
        Buffer req, resp;
        transport->setInput("");
        CPPUNIT_ASSERT_THROW(client.sendRecv<TestRpc>(session, req, resp),
                             ResponseFormatError);
    }

    void test_checkStatus() {
        client.status = STATUS_MESSAGE_TOO_SHORT;
        CPPUNIT_ASSERT_THROW(client.checkStatus(HERE), MessageTooShortError);
    }

    void test_throwShortResponseError() {
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
        CPPUNIT_ASSERT_EQUAL(9, status);

        // Valid RpcResponseCommon with error status.
        b.fillFromString("7 0");
        status = STATUS_OK;
        try {
            client.throwShortResponseError(b);
        } catch (ClientException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(7, status);

        // Response too short for RpcResponseCommon.
        b.fillFromString("a");
        status = STATUS_OK;
        try {
            client.throwShortResponseError(b);
        } catch (ClientException& e) {
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL(9, status);
    }

    DISALLOW_COPY_AND_ASSIGN(ClientTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(ClientTest);

}  // namespace RAMCloud
