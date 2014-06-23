/* Copyright (c) 2010-2014 Stanford University
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
    Context context;
    Service service;
    Buffer request, response;
    Worker worker;
    Service::Rpc rpc;

    ServiceTest()
        : context()
        , service()
        , request()
        , response()
        , worker(&context)
        , rpc(&worker, &request, &response)
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
    const char* result = Service::getString(&buffer, 3, 5);
    EXPECT_STREQ("defg", result);
}
TEST_F(ServiceTest, getString_lengthZero) {
    Buffer buffer;
    Status status = Status(0);
    try {
        Service::getString(&buffer, 0, 0);
    } catch (RequestFormatError& e) {
        status = e.status;
    }
    EXPECT_EQ(9, status);
}
TEST_F(ServiceTest, getString_bufferTooShort) {
    Buffer buffer;
    buffer.fillFromString("abcde");
    Status status = Status(0);
    try {
        Service::getString(&buffer, 2, 5);
    } catch (MessageTooShortError& e) {
        status = e.status;
    }
    EXPECT_EQ(7, status);
}
TEST_F(ServiceTest, getString_stringNotTerminated) {
    Buffer buffer;
    buffer.fillFromString("abcde");
    Status status = Status(0);
    try {
        Service::getString(&buffer, 1, 3);
    } catch (RequestFormatError& e) {
        status = e.status;
    }
    EXPECT_EQ(9, status);
}

TEST_F(ServiceTest, dispatch_ping) {
    request.fillFromString("7 0 0 0 0 0");
    service.dispatch(WireFormat::Ping::opcode, &rpc);
    EXPECT_TRUE(TestUtil::matchesPosixRegex("Service::ping invoked",
            TestLog::get()));
}
TEST_F(ServiceTest, dispatch_unknown) {
    request.fillFromString("0 0");
    union {
        WireFormat::Opcode x;
        int y;
    } t;
    t.y = 12345;
    EXPECT_THROW(
        service.dispatch(t.x, &rpc),
        UnimplementedRequestError);
}

TEST_F(ServiceTest, handleRpc_messageTooShortForCommon) {
    request.fillFromString("x");
    service.handleRpc(&rpc);
    EXPECT_STREQ("STATUS_MESSAGE_TOO_SHORT", TestUtil::getStatus(&response));
}
TEST_F(ServiceTest, handleRpc_undefinedType) {
    metrics->rpc.illegal_rpc_typeCount = 0;
    WireFormat::RequestCommon* header =
            request.emplaceAppend<WireFormat::RequestCommon>();
    header->opcode = WireFormat::ILLEGAL_RPC_TYPE;
    service.handleRpc(&rpc);
    EXPECT_STREQ("STATUS_UNIMPLEMENTED_REQUEST",
            TestUtil::getStatus(&response));
    EXPECT_EQ(1U, metrics->rpc.illegal_rpc_typeCount);
}
TEST_F(ServiceTest, handleRpc_retryException) {
    MockService service;
    request.fillFromString("1 2 54322 3 4");
    service.handleRpc(&rpc);
    EXPECT_EQ("17 100 200 18 server overloaded/0",
            TestUtil::toString(&response));
}
TEST_F(ServiceTest, handleRpc_clientException) {
    MockService service;
    request.fillFromString("1 2 54321 3 4");
    service.handleRpc(&rpc);
    EXPECT_STREQ("STATUS_REQUEST_FORMAT_ERROR", TestUtil::getStatus(&response));
}

TEST_F(ServiceTest, prepareErrorResponse_bufferNotEmpty) {
    response.fillFromString("1 abcdef");
    Service::prepareErrorResponse(&response, STATUS_WRONG_VERSION);
    EXPECT_STREQ("STATUS_WRONG_VERSION", TestUtil::getStatus(&response));
    EXPECT_STREQ("abcdef",
            static_cast<const char*>(response.getRange(4, 7)));
}
TEST_F(ServiceTest, prepareErrorResponse_bufferEmpty) {
    Service::prepareErrorResponse(&response, STATUS_WRONG_VERSION);
    EXPECT_EQ(sizeof(WireFormat::ResponseCommon), response.size());
    EXPECT_STREQ("STATUS_WRONG_VERSION", TestUtil::getStatus(&response));
}

TEST_F(ServiceTest, prepareRetryResponse_withMessage) {
    response.fillFromString("abcdef");
    Service::prepareRetryResponse(&response, 1000, 2000, "test message");
    EXPECT_EQ("17 1000 2000 13 test message/0", TestUtil::toString(&response));
}
TEST_F(ServiceTest, prepareRetryResponse_noMessage) {
    response.fillFromString("abcdef");
    Service::prepareRetryResponse(&response, 100, 200, NULL);
    EXPECT_EQ("17 100 200 0", TestUtil::toString(&response));
}

TEST_F(ServiceTest, callHandler_messageTooShort) {
    request.fillFromString("");
    EXPECT_THROW(
        (service.callHandler<WireFormat::Ping, Service, &Service::ping>(&rpc)),
        MessageTooShortError);
}
TEST_F(ServiceTest, callHandler_normal) {
    request.fillFromString("7 0 0 0 0 0");
    service.callHandler<WireFormat::Ping, Service, &Service::ping>(&rpc);
    EXPECT_TRUE(TestUtil::matchesPosixRegex("ping", TestLog::get()));
}

// Fake RPC class for testing checkServerId.
class DummyService : public Service {
  public:
    struct Rpc1 {
        static const WireFormat::Opcode opcode = WireFormat::Opcode(4);
        static const WireFormat::ServiceType service =
                WireFormat::MASTER_SERVICE;
        struct Request {
            WireFormat::RequestCommonWithId common;
        } __attribute__((packed));
        struct Response {
            WireFormat::ResponseCommon common;
        } __attribute__((packed));
    };
    void serviceMethod1(const Rpc1::Request* reqHdr, Rpc1::Response* respHdr,
                Rpc* rpc)
    {
        // No-op.
    }
};

TEST_F(ServiceTest, checkServerId) {
    request.fillFromString("9 1 2");
    DummyService service;

    // First try: service's id is invalid, so mismatch should be ignored.
    string message("no exception");
    try {
        service.callHandler<DummyService::Rpc1, DummyService,
                &DummyService::serviceMethod1>(&rpc);
    } catch (WrongServerException& e) {
        message = e.toSymbol();
    }
    EXPECT_EQ("no exception", message);

    // Second try: should generate an exception.
    service.serverId = ServerId(1, 3);
    response.reset();
    message = "no exception";
    try {
        service.callHandler<DummyService::Rpc1, DummyService,
                &DummyService::serviceMethod1>(&rpc);
    } catch (WrongServerException& e) {
        message = e.toSymbol();
    }
    EXPECT_EQ("STATUS_WRONG_SERVER", message);

    // Third try: ids match.
    service.serverId = ServerId(1, 2);
    response.reset();
    message = "no exception";
    try {
        service.callHandler<DummyService::Rpc1, DummyService,
                &DummyService::serviceMethod1>(&rpc);
    } catch (WrongServerException& e) {
        message = e.toSymbol();
    }
    EXPECT_EQ("no exception", message);

    // Fourth try: serverId in RPC is invalid, so mismatch should be ignored.
    response.reset();
    request.reset();
    request.fillFromString("9 0 -1");
    message = "no exception";
    try {
        service.callHandler<DummyService::Rpc1, DummyService,
                &DummyService::serviceMethod1>(&rpc);
    } catch (WrongServerException& e) {
        message = e.toSymbol();
    }
    EXPECT_EQ("no exception", message);
}

TEST_F(ServiceTest, sendReply) {
    MockService service;
    service.gate = -1;
    service.sendReply = true;
    Context context;
    MockTransport transport(&context);
    ServiceManager* manager = context.serviceManager;
    manager->addService(service, WireFormat::BACKUP_SERVICE);
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "0x10000 3 4");
    manager->handleRpc(rpc);

    // Verify that the reply has been sent even though the worker has not
    // returned yet.
    for (int i = 0; i < 1000; i++) {
        context.dispatch->poll();
        if (manager->busyThreads[0]->rpc == NULL) {
            break;
        }
        usleep(1000);
    }
    EXPECT_EQ((Transport::ServerRpc*) NULL, manager->busyThreads[0]->rpc);
    EXPECT_EQ(Worker::POSTPROCESSING, manager->busyThreads[0]->state.load());
    EXPECT_EQ("serverReply: 0x10001 4 5", transport.outputLog);
    service.gate = 3;
}

}  // namespace RAMCloud
