/* Copyright (c) 2010-2011 Stanford University
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
#include "MockService.h"
#include "MockTransport.h"
#include "RawMetrics.h"
#include "Service.h"
#include "ServiceManager.h"

namespace RAMCloud {

class ServiceTest : public ::testing::Test {
  public:
    Service service;
    Buffer request, response;
    Worker worker;
    Service::Rpc rpc;

    ServiceTest()
        : service()
        , request()
        , response()
        , worker(Context::get())
        , rpc(&worker, request, response)
    {
        TestLog::enable();
    }

    ~ServiceTest()
    {
        TestLog::disable();
    }
};

TEST_F(ServiceTest, getString_basics) {
    Buffer buffer;
    buffer.fillFromString("abcdefg");
    const char* result = Service::getString(buffer, 3, 5);
    EXPECT_STREQ("defg", result);
}
TEST_F(ServiceTest, getString_lengthZero) {
    Buffer buffer;
    Status status = Status(0);
    try {
        Service::getString(buffer, 0, 0);
    } catch (RequestFormatError& e) {
        status = e.status;
    }
    EXPECT_EQ(8, status);
}
TEST_F(ServiceTest, getString_bufferTooShort) {
    Buffer buffer;
    buffer.fillFromString("abcde");
    Status status = Status(0);
    try {
        Service::getString(buffer, 2, 5);
    } catch (MessageTooShortError& e) {
        status = e.status;
    }
    EXPECT_EQ(6, status);
}
TEST_F(ServiceTest, getString_stringNotTerminated) {
    Buffer buffer;
    buffer.fillFromString("abcde");
    Status status = Status(0);
    try {
        Service::getString(buffer, 1, 3);
    } catch (RequestFormatError& e) {
        status = e.status;
    }
    EXPECT_EQ(8, status);
}

TEST_F(ServiceTest, dispatch_ping) {
    request.fillFromString("7 0 0 0");
    service.dispatch(PingRpc::opcode, rpc);
    EXPECT_TRUE(TestUtil::matchesPosixRegex("Service::ping invoked",
            TestLog::get()));
}
TEST_F(ServiceTest, dispatch_unknown) {
    request.fillFromString("0 0");
    union {
        RpcOpcode x;
        int y;
    } t;
    t.y = 12345;
    EXPECT_THROW(
        service.dispatch(t.x, rpc),
        UnimplementedRequestError);
}

TEST_F(ServiceTest, handleRpc_messageTooShortForCommon) {
    request.fillFromString("x");
    service.handleRpc(rpc);
    EXPECT_STREQ("STATUS_MESSAGE_TOO_SHORT", TestUtil::getStatus(&response));
}
TEST_F(ServiceTest, handleRpc_undefinedType) {
    metrics->rpc.illegalRpcCount = 0;
    request.fillFromString("1000 abcdef");
    service.handleRpc(rpc);
    EXPECT_STREQ("STATUS_UNIMPLEMENTED_REQUEST",
            TestUtil::getStatus(&response));
    EXPECT_EQ(1U, metrics->rpc.illegalRpcCount);
}
TEST_F(ServiceTest, handleRpc_clientException) {
    MockService service;
    request.fillFromString("1 2 54321 3 4");
    service.handleRpc(rpc);
    EXPECT_STREQ("STATUS_REQUEST_FORMAT_ERROR", TestUtil::getStatus(&response));
}

TEST_F(ServiceTest, prepareErrorResponse_bufferNotEmpty) {
    response.fillFromString("1 abcdef");
    Service::prepareErrorResponse(response, STATUS_WRONG_VERSION);
    EXPECT_STREQ("STATUS_WRONG_VERSION", TestUtil::getStatus(&response));
    EXPECT_STREQ("abcdef",
            static_cast<const char*>(response.getRange(4, 7)));
}
TEST_F(ServiceTest, prepareErrorResponse_bufferEmpty) {
    Service::prepareErrorResponse(response, STATUS_WRONG_VERSION);
    EXPECT_EQ(sizeof(RpcResponseCommon), response.getTotalLength());
    EXPECT_STREQ("STATUS_WRONG_VERSION", TestUtil::getStatus(&response));
}

TEST_F(ServiceTest, callHandler_messageTooShort) {
    request.fillFromString("");
    EXPECT_THROW(
        (service.callHandler<PingRpc, Service, &Service::ping>(rpc)),
        MessageTooShortError);
}
TEST_F(ServiceTest, callHandler_normal) {
    request.fillFromString("7 0 0 0");
    service.callHandler<PingRpc, Service, &Service::ping>(rpc);
    EXPECT_TRUE(TestUtil::matchesPosixRegex("ping", TestLog::get()));
}

TEST_F(ServiceTest, sendReply) {
    MockService service;
    service.gate = -1;
    service.sendReply = true;
    MockTransport transport;
    ServiceManager manager;
    manager.addService(service, RpcServiceType(2));
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "0x20000 3 4");
    manager.handleRpc(rpc);

    // Verify that the reply has been sent even though the worker has not
    // returned yet.
    for (int i = 0; i < 1000; i++) {
        Context::get().dispatch->poll();
        if (manager.busyThreads[0]->rpc == NULL) {
            break;
        }
        usleep(1000);
    }
    EXPECT_EQ((Transport::ServerRpc*) NULL, manager.busyThreads[0]->rpc);
    EXPECT_EQ(Worker::POSTPROCESSING, manager.busyThreads[0]->state.load());
    EXPECT_EQ("serverReply: 0x20001 4 5", transport.outputLog);
    service.gate = 3;
}

}  // namespace RAMCloud
