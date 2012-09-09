/* Copyright (c) 2011-2012 Stanford University
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
#include "MockCluster.h"
#include "MultiRead.h"
#include "RawMetrics.h"
#include "ServerMetrics.h"
#include "ShortMacros.h"
#include "RamCloud.h"

namespace RAMCloud {

class MultiReadTest : public ::testing::Test {
  public:
    Context context;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    uint64_t tableId1;
    uint64_t tableId2;
    uint64_t tableId3;
    BindTransport::BindSession* session1;
    BindTransport::BindSession* session2;
    BindTransport::BindSession* session3;
    Tub<Buffer> values[6];
    MultiReadObject objects[6];

  public:
    MultiReadTest()
        : context()
        , cluster(&context)
        , ramcloud()
        , tableId1(-1)
        , tableId2(-2)
        , tableId3(-3)
        , session1(NULL)
        , session2(NULL)
        , session3(NULL)
        , values()
        , objects()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master1";
        config.maxObjectKeySize = 512;
        config.maxObjectDataSize = 1024;
        config.segmentSize = 128*1024;
        config.segletSize = 128*1024;
        cluster.addServer(config);
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master2";
        cluster.addServer(config);
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master3";
        cluster.addServer(config);
        ramcloud.construct(&context, "mock:host=coordinator");

        // Write some test data to the servers.
        tableId1 = ramcloud->createTable("table1");
        ramcloud->write(tableId1, "object1-1", 9, "value:1-1");
        ramcloud->write(tableId1, "object1-2", 9, "value:1-2");
        ramcloud->write(tableId1, "object1-3", 9, "value:1-3");
        tableId2 = ramcloud->createTable("table2");
        ramcloud->write(tableId2, "object2-1", 9, "value:2-1");
        tableId3 = ramcloud->createTable("table3");
        ramcloud->write(tableId3, "object3-1", 9, "value:3-1");
        ramcloud->write(tableId3, "object3-2", 9, "value:3-2");

        // Get pointers to the master sessions.
        Transport::SessionRef session =
                ramcloud->clientContext->transportManager->getSession(
                "mock:host=master1");
        session1 = static_cast<BindTransport::BindSession*>(session.get());
        session = ramcloud->clientContext->transportManager->getSession(
                "mock:host=master2");
        session2 = static_cast<BindTransport::BindSession*>(session.get());
        session = ramcloud->clientContext->transportManager->getSession(
                "mock:host=master3");
        session3 = static_cast<BindTransport::BindSession*>(session.get());

        // Create some object descriptors for use in requests.
        objects[0] = {tableId1, "object1-1", 9, &values[0]};
        objects[1] = {tableId1, "object1-2", 9, &values[1]};
        objects[2] = {tableId1, "object1-3", 9, &values[2]};
        objects[3] = {tableId2, "object2-1", 9, &values[3]};
        objects[4] = {tableId3, "object3-1", 9, &values[4]};
        objects[5] = {tableId3, "bogus", 5, &values[5]};
    }

    // Returns a string describing the status of the RPCs for request.
    // For example:
    //    mock:host=master1(2) -
    // means that rpcs[0] has an active RPC to master1 that is requesting
    // 2 objects, and rpcs[1] is not currently active ("-").
    string
    rpcStatus(MultiRead& request)
    {
        string result;
        const char* separator = "";
        for (uint32_t i = 0; i < MultiRead::MAX_RPCS; i++) {
            result.append(separator);
            separator = " ";
            if (request.rpcs[i]) {
                result.append(format("%s(%d)",
                    request.rpcs[i]->session->getServiceLocator().c_str(),
                    request.rpcs[i]->reqHdr->count));
            } else {
                result.append("-");
            }
        }
        return result;
    }

    string
    bufferString(Tub<Buffer>& buffer)
    {
        if (!buffer)
            return "uninitialized";
        return TestUtil::toString(buffer.get());
    }

    DISALLOW_COPY_AND_ASSIGN(MultiReadTest);
};

TEST_F(MultiReadTest, basics) {
    MultiReadObject* requests[] = {&objects[0], &objects[1], &objects[2],
            &objects[3], &objects[4], &objects[5]};
    ramcloud->multiRead(requests, 6);
    EXPECT_STREQ("STATUS_OK", statusToSymbol(objects[0].status));
    EXPECT_EQ("value:1-1", bufferString(values[0]));
    EXPECT_STREQ("STATUS_OK", statusToSymbol(objects[1].status));
    EXPECT_EQ("value:1-2", bufferString(values[1]));
    EXPECT_STREQ("STATUS_OK", statusToSymbol(objects[2].status));
    EXPECT_EQ("value:1-3", bufferString(values[2]));
    EXPECT_STREQ("STATUS_OK", statusToSymbol(objects[3].status));
    EXPECT_EQ("value:2-1", bufferString(values[3]));
    EXPECT_STREQ("STATUS_OK", statusToSymbol(objects[4].status));
    EXPECT_EQ("value:3-1", bufferString(values[4]));
    EXPECT_STREQ("STATUS_OBJECT_DOESNT_EXIST",
            statusToSymbol(objects[5].status));
    EXPECT_EQ("uninitialized", bufferString(values[5]));
}

TEST_F(MultiReadTest, cancel) {
    MultiReadObject* requests[] = {&objects[0], &objects[1], &objects[4]};
    session1->dontNotify = true;

    MultiRead request(ramcloud.get(), requests, 3);
    EXPECT_EQ("mock:host=master1(2) mock:host=master3(1)",
            rpcStatus(request));
    request.cancel();
    EXPECT_EQ("- -", rpcStatus(request));
}

TEST_F(MultiReadTest, isReady) {
    MultiReadObject* requests[] = {&objects[0], &objects[1], &objects[3],
            &objects[4]};
    session1->dontNotify = true;
    session3->dontNotify = true;

    // Launch RPCs, let one of them finish.
    MultiRead request(ramcloud.get(), requests, 4);
    EXPECT_EQ("mock:host=master1(2) mock:host=master2(1)",
            rpcStatus(request));
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("mock:host=master1(2) mock:host=master3(1)",
            rpcStatus(request));

    // Make sure that isReady calls don't do anything until something
    // finishes.
    EXPECT_FALSE(request.isReady());
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("mock:host=master1(2) mock:host=master3(1)",
            rpcStatus(request));

    // Let the remaining RPCs finish one at a time.
    session1->lastNotifier->completed();
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("- mock:host=master3(1)", rpcStatus(request));

    session3->lastNotifier->completed();
    EXPECT_TRUE(request.isReady());
    EXPECT_EQ("- -", rpcStatus(request));
    EXPECT_EQ("value:1-1", bufferString(values[0]));
    EXPECT_EQ("value:3-1", bufferString(values[4]));
}

TEST_F(MultiReadTest, startRpcs_skipFinishedOrFailed) {
    MultiReadObject* requests[] = {&objects[0], &objects[1], &objects[5]};
    MultiRead request(ramcloud.get(), requests, 3);
    EXPECT_TRUE(request.isReady());
    EXPECT_TRUE(request.startRpcs());
    EXPECT_EQ("- -", rpcStatus(request));
}
TEST_F(MultiReadTest, startRpcs_tableDoesntExist) {
    Tub<Buffer> value;
    MultiReadObject object(tableId3+1, "bogus", 5, &value);
    MultiReadObject* requests[] = {&object};
    MultiRead request(ramcloud.get(), requests, 1);
    EXPECT_TRUE(request.isReady());
    EXPECT_STREQ("STATUS_TABLE_DOESNT_EXIST", statusToSymbol(object.status));
    EXPECT_EQ("uninitialized", bufferString(value));
}
TEST_F(MultiReadTest, startRpcs_noAvailableRpc) {
    MultiReadObject* requests[] = {&objects[0], &objects[3], &objects[4]};

    // During the first call, the last object can't be sent because all
    // RPC slots are in use and its session doesn't match.
    MultiRead request(ramcloud.get(), requests, 3);
    EXPECT_EQ("mock:host=master1(1) mock:host=master2(1)",
            rpcStatus(request));

    // During the second call, the RPC slots are in process, so we don't
    // even check the session.
    EXPECT_FALSE(request.startRpcs());
    EXPECT_EQ("mock:host=master1(1) mock:host=master2(1)",
            rpcStatus(request));

    // During the third call RPC slots open up.
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("mock:host=master3(1) -", rpcStatus(request));
    EXPECT_EQ("uninitialized", bufferString(values[4]));

    EXPECT_TRUE(request.isReady());
    EXPECT_EQ("- -", rpcStatus(request));
    EXPECT_EQ("value:3-1", bufferString(values[4]));
}
TEST_F(MultiReadTest, startRpcs_tooManyObjectsForOneRpc) {
    Tub<Buffer> value4, value5;
    MultiReadObject object4(tableId1, "bogus1", 6, &value4);
    MultiReadObject object5(tableId1, "bogus2", 6, &value5);
    MultiReadObject* requests[] = {&objects[0], &objects[1], &objects[2],
            &object4, &object5};

    // Not enough space in RPC to send all requests in the first RPC.
    MultiRead request(ramcloud.get(), requests, 5);
    EXPECT_EQ("mock:host=master1(3) -", rpcStatus(request));

    // During the second call, still no space (first call
    // hasn't finished).
    EXPECT_FALSE(request.startRpcs());
    EXPECT_EQ("mock:host=master1(3) -", rpcStatus(request));

    // First call has finished, so another starts.
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("mock:host=master1(2) -", rpcStatus(request));
    EXPECT_EQ("uninitialized", bufferString(values[4]));

    EXPECT_TRUE(request.isReady());
    EXPECT_EQ("- -", rpcStatus(request));
    EXPECT_STREQ("STATUS_OBJECT_DOESNT_EXIST", statusToSymbol(object5.status));
}

// Helper function that runs in a separate thread for the following test.
static void multiReadWaitThread(MultiRead* request) {
    request->wait();
    TEST_LOG("request finished");
}

TEST_F(MultiReadTest, wait) {
    TestLog::Enable _;
    MultiReadObject* requests[] = {&objects[0]};
    session1->dontNotify = true;
    MultiRead request(ramcloud.get(), requests, 1);
    std::thread thread(multiReadWaitThread, &request);
    usleep(1000);
    EXPECT_EQ("", TestLog::get());
    session1->lastNotifier->completed();

    // Give the waiting thread a chance to finish.
    for (int i = 0; i < 1000; i++) {
        if (TestLog::get().size() != 0)
            break;
        usleep(100);
    }
    EXPECT_EQ("multiReadWaitThread: request finished", TestLog::get());
    thread.join();
    EXPECT_EQ("value:1-1", bufferString(values[0]));
}

TEST_F(MultiReadTest, wait_canceled) {
    MultiReadObject* requests[] = {&objects[0]};
    session1->dontNotify = true;

    MultiRead request(ramcloud.get(), requests, 1);
    EXPECT_EQ("mock:host=master1(1) -", rpcStatus(request));
    request.cancel();
    string message = "no exception";
    try {
        request.wait();
    }
    catch (RpcCanceledException& e) {
        message = "RpcCanceledException";
    }
    EXPECT_EQ("RpcCanceledException", message);
}

TEST_F(MultiReadTest, PartRpc_finish_transportError) {
    MultiReadObject* requests[] = {&objects[0], &objects[1]};
    session1->dontNotify = true;
    MultiRead request(ramcloud.get(), requests, 2);
    EXPECT_EQ("mock:host=master1(2) -", rpcStatus(request));
    EXPECT_STREQ("STATUS_UNKNOWN", statusToSymbol(objects[0].status));
    EXPECT_STREQ("STATUS_UNKNOWN", statusToSymbol(objects[1].status));
    session1->lastNotifier->failed();
    request.rpcs[0]->finish();
    EXPECT_STREQ("STATUS_OK", statusToSymbol(objects[0].status));
    EXPECT_STREQ("STATUS_OK", statusToSymbol(objects[0].status));
    request.rpcs[0].destroy();
    request.wait();
    EXPECT_EQ("value:1-1", bufferString(values[0]));
    EXPECT_EQ("value:1-2", bufferString(values[1]));
}
TEST_F(MultiReadTest, PartRpc_finish_shortResponse) {
    // This test checks for proper handling of responses that are
    // too short.
    TestLog::Enable _;
    MultiReadObject* requests[] = {&objects[0], &objects[1]};
    session1->dontNotify = true;
    MultiRead request(ramcloud.get(), requests, 2);
    EXPECT_EQ("mock:host=master1(2) -", rpcStatus(request));

    // Can't read status from response.
    session1->lastResponse->truncateEnd(
        session1->lastResponse->getTotalLength() - 11);
    session1->lastNotifier->completed();
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("finish: missing status", TestLog::get());
    TestLog::reset();
    EXPECT_EQ("mock:host=master1(2) -", rpcStatus(request));

    // Can't read Response::Part from response.
    session1->lastResponse->truncateEnd(
        session1->lastResponse->getTotalLength() - 18);
    session1->lastNotifier->completed();
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("finish: missing Response::Part", TestLog::get());
    TestLog::reset();
    EXPECT_EQ("mock:host=master1(2) -", rpcStatus(request));

    // Can't read object data from response (first object complete,
    // this happens during the second object).
    session1->lastResponse->truncateEnd(1);
    session1->lastNotifier->completed();
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("finish: missing object data", TestLog::get());
    TestLog::reset();
    EXPECT_EQ("mock:host=master1(1) -", rpcStatus(request));

    // Let the request finally succeed.
    session1->lastNotifier->completed();
    EXPECT_TRUE(request.isReady());
    EXPECT_EQ("value:1-1", bufferString(values[0]));
    EXPECT_EQ("value:1-2", bufferString(values[1]));
}
TEST_F(MultiReadTest, PartRpc_unknownTable) {
    TestLog::Enable _;
    MultiReadObject* requests[] = {&objects[0], &objects[1],
            &objects[2]};
    session1->dontNotify = true;
    MultiRead request(ramcloud.get(), requests, 3);
    EXPECT_EQ("mock:host=master1(3) -", rpcStatus(request));

    // Modify the response to reject all objects.
    session1->lastResponse->truncateEnd(
            session1->lastResponse->getTotalLength() -
            downCast<uint32_t>(sizeof(WireFormat::MultiRead::Response)));
    Status* statuses = new(session1->lastResponse, APPEND) Status[3];
    statuses[0] = STATUS_UNKNOWN_TABLET;
    statuses[1] = STATUS_OBJECT_DOESNT_EXIST;
    statuses[2] = STATUS_UNKNOWN_TABLET;
    session1->lastNotifier->completed();
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("finish: Server mock:host=master1 doesn't store "
            "<0, object1-1>; refreshing object map | "
            "flush: flushing object map | flush: flushing object map",
            TestLog::get());
    EXPECT_EQ("mock:host=master1(2) -", rpcStatus(request));

    // Let the retry succeed.
    session1->lastNotifier->completed();
    EXPECT_TRUE(request.isReady());
    EXPECT_EQ("value:1-1", bufferString(values[0]));
    EXPECT_STREQ("STATUS_OBJECT_DOESNT_EXIST",
            statusToSymbol(objects[1].status));
    EXPECT_EQ("value:1-3", bufferString(values[2]));
}

TEST_F(MultiReadTest, PartRpc_handleTransportError) {
    TestLog::Enable _;
    MultiReadObject* requests[] = {&objects[0]};
    session1->dontNotify = true;
    MultiRead request(ramcloud.get(), requests, 1);
    EXPECT_EQ("mock:host=master1(1) -", rpcStatus(request));
    session1->lastNotifier->failed();
    request.wait();
    EXPECT_EQ("flushSession: flushing session for mock:host=master1 "
            "| flush: flushing object map", TestLog::get());
    EXPECT_EQ("value:1-1", bufferString(values[0]));
}

}  // namespace RAMCloud
