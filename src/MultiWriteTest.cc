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
#include "MultiWrite.h"
#include "RawMetrics.h"
#include "ServerMetrics.h"
#include "ShortMacros.h"
#include "RamCloud.h"

namespace RAMCloud {

class MultiWriteTest : public ::testing::Test {
  public:
    Context context;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    uint64_t tableId1;
    uint64_t tableId2;
    uint64_t tableId3;
    uint64_t bogusTableId;
    BindTransport::BindSession* session1;
    BindTransport::BindSession* session2;
    BindTransport::BindSession* session3;
    Tub<MultiWriteObject> objects[6];

  public:
    MultiWriteTest()
        : context()
        , cluster(&context)
        , ramcloud()
        , tableId1(-1)
        , tableId2(-2)
        , tableId3(-3)
        , bogusTableId(-4)
        , session1(NULL)
        , session2(NULL)
        , session3(NULL)
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
        tableId2 = ramcloud->createTable("table2");
        tableId3 = ramcloud->createTable("table3");

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
        uint16_t keyLen5 = 5;
        uint16_t keyLen9 = 9;
        objects[0].construct(tableId1, "object1-1", keyLen9, "value:1-1", 9);
        objects[1].construct(tableId1, "object1-2", keyLen9, "value:1-2", 9);
        objects[2].construct(tableId1, "object1-3", keyLen9, "value:1-3", 9);
        objects[3].construct(tableId2, "object2-1", keyLen9, "value:2-1", 9);
        objects[4].construct(tableId3, "object3-1", keyLen9, "value:3-1", 9);
        objects[5].construct(bogusTableId, "bogus", keyLen5, "value:bogus", 11);
    }

    // Returns a string describing the status of the RPCs for request.
    // For example:
    //    mock:host=master1(2) -
    // means that rpcs[0] has an active RPC to master1 that is requesting
    // 2 objects, and rpcs[1] is not currently active ("-").
    string
    rpcStatus(MultiWrite& request)
    {
        string result;
        const char* separator = "";
        for (uint32_t i = 0; i < MultiWrite::MAX_RPCS; i++) {
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

    DISALLOW_COPY_AND_ASSIGN(MultiWriteTest);
};

TEST_F(MultiWriteTest, basics) {
    MultiWriteObject* requests[] = {
        objects[0].get(), objects[1].get(), objects[2].get(),
        objects[3].get(), objects[4].get(), objects[5].get()
    };
    ramcloud->multiWrite(requests, 6);
    EXPECT_EQ(STATUS_OK, objects[0]->status);
    EXPECT_EQ(1U, objects[0]->version);
    EXPECT_EQ(STATUS_OK, objects[1]->status);
    EXPECT_EQ(2U, objects[1]->version);
    EXPECT_EQ(STATUS_OK, objects[2]->status);
    EXPECT_EQ(3U, objects[2]->version);
    EXPECT_EQ(STATUS_OK, objects[3]->status);
    EXPECT_EQ(1U, objects[3]->version);
    EXPECT_EQ(STATUS_OK, objects[4]->status);
    EXPECT_EQ(1U, objects[4]->version);
    EXPECT_EQ(STATUS_TABLE_DOESNT_EXIST, objects[5]->status);
    // object[5].version is undefined when tablet doesn't exist
}

TEST_F(MultiWriteTest, cancel) {
    MultiWriteObject* requests[] = {
        objects[0].get(), objects[1].get(), objects[4].get()
    };
    session1->dontNotify = true;

    MultiWrite request(ramcloud.get(), requests, 3);
    EXPECT_EQ("mock:host=master1(2) mock:host=master3(1)",
            rpcStatus(request));
    request.cancel();
    EXPECT_EQ("- -", rpcStatus(request));
}

TEST_F(MultiWriteTest, isReady) {
    MultiWriteObject* requests[] = {
        objects[0].get(), objects[1].get(), objects[3].get(), objects[4].get()
    };
    session1->dontNotify = true;
    session3->dontNotify = true;

    // Launch RPCs, let one of them finish.
    MultiWrite request(ramcloud.get(), requests, 4);
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
    EXPECT_EQ(STATUS_OK, objects[0]->status);
    EXPECT_EQ(STATUS_OK, objects[4]->status);
}

TEST_F(MultiWriteTest, startRpcs_skipFinishedOrFailed) {
    MultiWriteObject* requests[] = {
        objects[0].get(), objects[1].get(), objects[5].get()
    };
    MultiWrite request(ramcloud.get(), requests, 3);
    EXPECT_TRUE(request.isReady());
    EXPECT_TRUE(request.startRpcs());
    EXPECT_EQ("- -", rpcStatus(request));
}

TEST_F(MultiWriteTest, startRpcs_tableDoesntExist) {
    MultiWriteObject object(bogusTableId, "bogus", 5, "value:bogus", 11);
    MultiWriteObject* requests[] = { &object };
    MultiWrite request(ramcloud.get(), requests, 1);
    EXPECT_TRUE(request.isReady());
    EXPECT_EQ(STATUS_TABLE_DOESNT_EXIST, object.status);
}

TEST_F(MultiWriteTest, startRpcs_noAvailableRpc) {
    MultiWriteObject* requests[] = {
        objects[0].get(), objects[3].get(), objects[4].get()
    };

    // During the first call, the last object can't be sent because all
    // RPC slots are in use and its session doesn't match.
    MultiWrite request(ramcloud.get(), requests, 3);
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
    EXPECT_NE(STATUS_OK, objects[4]->status);

    EXPECT_TRUE(request.isReady());
    EXPECT_EQ("- -", rpcStatus(request));
    EXPECT_EQ(STATUS_OK, objects[4]->status);
}

TEST_F(MultiWriteTest, startRpcs_tooManyObjectsForOneRpc) {
    MultiWriteObject object4(tableId1, "extra1", 6, "value:extra1", 12);
    MultiWriteObject object5(tableId1, "extra2", 6, "value:extra2", 12);
    MultiWriteObject* requests[] = {
        objects[0].get(), objects[1].get(), objects[2].get(), &object4, &object5
    };

    // Not enough space in RPC to send all requests in the first RPC.
    MultiWrite request(ramcloud.get(), requests, 5);
    EXPECT_EQ("mock:host=master1(3) -", rpcStatus(request));

    // During the second call, still no space (first call
    // hasn't finished).
    EXPECT_FALSE(request.startRpcs());
    EXPECT_EQ("mock:host=master1(3) -", rpcStatus(request));

    // First call has finished, so another starts.
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("mock:host=master1(2) -", rpcStatus(request));
    EXPECT_EQ(STATUS_OK, objects[4]->status);

    EXPECT_TRUE(request.isReady());
    EXPECT_EQ("- -", rpcStatus(request));
    EXPECT_EQ(STATUS_OK, object5.status);
}

// Helper function that runs in a separate thread for the following test.
static void
multiWriteWaitThread(MultiWrite* request) {
    request->wait();
    TEST_LOG("request finished");
}

// Filter out the desired log entries below (skipping log and replicated segment
// messages made during the multiwrite operations).
static bool
testLogFilter(string s)
{
    return s == "multiWriteWaitThread" ||
           s == "finish" ||
           s == "flush" ||
           s == "flushSession";
}

TEST_F(MultiWriteTest, wait) {
    TestLog::Enable _(testLogFilter);
    MultiWriteObject* requests[] = { objects[0].get() };
    session1->dontNotify = true;
    MultiWrite request(ramcloud.get(), requests, 1);
    std::thread thread(multiWriteWaitThread, &request);
    usleep(1000);
    EXPECT_EQ("", TestLog::get());
    session1->lastNotifier->completed();

    // Give the waiting thread a chance to finish.
    for (int i = 0; i < 1000; i++) {
        if (TestLog::get().size() != 0)
            break;
        usleep(100);
    }
    EXPECT_EQ("multiWriteWaitThread: request finished", TestLog::get());
    thread.join();
    EXPECT_EQ(STATUS_OK, objects[0]->status);
    EXPECT_EQ(1U, objects[0]->version);
}

TEST_F(MultiWriteTest, wait_canceled) {
    MultiWriteObject* requests[] = { objects[0].get() };
    session1->dontNotify = true;

    MultiWrite request(ramcloud.get(), requests, 1);
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

TEST_F(MultiWriteTest, PartRpc_finish_transportError) {
    MultiWriteObject* requests[] = { objects[0].get(), objects[1].get() };
    session1->dontNotify = true;
    MultiWrite request(ramcloud.get(), requests, 2);
    EXPECT_EQ("mock:host=master1(2) -", rpcStatus(request));
    EXPECT_NE(STATUS_OK, objects[0]->status);
    EXPECT_NE(STATUS_OK, objects[1]->status);
    session1->lastNotifier->failed();
    request.rpcs[0]->finish();
    EXPECT_EQ(STATUS_OK, objects[0]->status);
    EXPECT_EQ(STATUS_OK, objects[1]->status);
    request.rpcs[0].destroy();
    request.wait();
    EXPECT_EQ(1U, objects[0]->version);
    EXPECT_EQ(2U, objects[1]->version);
}

TEST_F(MultiWriteTest, PartRpc_finish_shortResponse) {
    // This test checks for proper handling of responses that are
    // too short.
    TestLog::Enable _(testLogFilter);
    MultiWriteObject* requests[] = { objects[0].get(), objects[1].get() };
    session1->dontNotify = true;
    MultiWrite request(ramcloud.get(), requests, 2);
    EXPECT_EQ("mock:host=master1(2) -", rpcStatus(request));

    // Can't read first Response::Part from response.
    session1->lastResponse->truncateEnd(
        sizeof(WireFormat::MultiWrite::Response::Part) + 1);
    session1->lastNotifier->completed();
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("finish: missing Response::Part", TestLog::get());
    TestLog::reset();
    EXPECT_EQ("mock:host=master1(2) -", rpcStatus(request));

    // Can't read second Response::Part from response.
    session1->lastResponse->truncateEnd(1);
    session1->lastNotifier->completed();
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("finish: missing Response::Part", TestLog::get());
    TestLog::reset();
    EXPECT_EQ("mock:host=master1(1) -", rpcStatus(request));

    // Let the request finally succeed. Note that each write will have been
    // retried, so the versions will be larger than expected. That is,
    // objects[0] and objects[1] were written with version=1 and version=2,
    // respectively, but the responses were bad. So, both are retried.
    // Object[0] succeeds the second time and is stored with version=2.
    // Object[1] fails the second time, but succeeds the third time with
    // version=4.
    session1->lastNotifier->completed();
    EXPECT_TRUE(request.isReady());
    EXPECT_EQ(STATUS_OK, objects[0]->status);
    EXPECT_EQ(STATUS_OK, objects[1]->status);
    EXPECT_EQ(2U, objects[0]->version);
    EXPECT_EQ(4U, objects[1]->version);
}

TEST_F(MultiWriteTest, PartRpc_unknownTable) {
    TestLog::Enable _(testLogFilter);
    MultiWriteObject* requests[] = {
        objects[0].get(), objects[1].get(), objects[2].get()
    };
    session1->dontNotify = true;
    MultiWrite request(ramcloud.get(), requests, 3);
    EXPECT_EQ("mock:host=master1(3) -", rpcStatus(request));

    // Modify the response to reject all objects.
    session1->lastResponse->truncateEnd(
            session1->lastResponse->getTotalLength() -
            downCast<uint32_t>(sizeof(WireFormat::MultiWrite::Response)));
    WireFormat::MultiWrite::Response::Part* parts =
        new(session1->lastResponse, APPEND)
        WireFormat::MultiWrite::Response::Part[3];
    parts[0].status = STATUS_UNKNOWN_TABLET;
    parts[1].status = STATUS_OBJECT_DOESNT_EXIST;
    parts[2].status = STATUS_UNKNOWN_TABLET;
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
    EXPECT_EQ(STATUS_OK, objects[0]->status);
    EXPECT_EQ(2U, objects[0]->version);
    EXPECT_EQ(STATUS_OBJECT_DOESNT_EXIST, objects[1]->status);
    EXPECT_EQ(STATUS_OK, objects[2]->status);
    EXPECT_EQ(4U, objects[2]->version);
}

TEST_F(MultiWriteTest, PartRpc_handleTransportError) {
    TestLog::Enable _(testLogFilter);
    MultiWriteObject* requests[] = { objects[0].get() };
    session1->dontNotify = true;
    MultiWrite request(ramcloud.get(), requests, 1);
    EXPECT_EQ("mock:host=master1(1) -", rpcStatus(request));
    session1->lastNotifier->failed();
    request.wait();
    EXPECT_EQ("flushSession: flushing session for mock:host=master1 "
            "| flush: flushing object map", TestLog::get());
    EXPECT_EQ(STATUS_OK, objects[0]->status);
    EXPECT_EQ(1U, objects[0]->version);
}

}  // namespace RAMCloud
