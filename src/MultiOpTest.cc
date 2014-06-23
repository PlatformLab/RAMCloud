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
#include "MockCluster.h"
#include "RawMetrics.h"
#include "ServerMetrics.h"
#include "ShortMacros.h"
#include "RamCloud.h"
#include "MultiOp.h"

namespace RAMCloud {

// To all of MultiOp, it needs to have a concrete implementation
// with a corresponding Service. MultiRead is used for this purpose.
// Tests that use MultiRead will be marked as so.
class MultiOpTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    uint64_t tableId1;
    uint64_t tableId2;
    uint64_t tableId3;
    BindTransport::BindSession* session1;
    BindTransport::BindSession* session2;
    BindTransport::BindSession* session3;
    MultiOpObject objects[6];

  public:
    MultiOpTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , ramcloud()
        , tableId1(-1)
        , tableId2(-2)
        , tableId3(-3)
        , session1(NULL)
        , session2(NULL)
        , session3(NULL)
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
        objects[0] = {tableId1, "object1-1", 9};
        objects[1] = {tableId1, "object1-2", 9};
        objects[2] = {tableId1, "object1-3", 9};
        objects[3] = {tableId2, "object2-1", 9};
        objects[4] = {tableId3, "object3-1", 9};
        objects[5] = {tableId3, "bogus", 5};
    }

    // Returns a string describing the status of the RPCs for request.
    // For example:
    //    mock:host=master1(2) -
    // means that rpcs[0] has an active RPC to master1 that is requesting
    // 2 objects, and rpcs[1] is not currently active ("-").
    string
    rpcStatus(MultiOp& request)
    {
        string result;
        const char* separator = "";
        for (uint32_t i = 0; i < MultiOp::MAX_RPCS; i++) {
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

    class MultiOpTester : public MultiOp{
    public:
        static const WireFormat::MultiOp::OpType type =
                                          WireFormat::MultiOp::OpType::INVALID;
        uint32_t appendSize;
        uint32_t appendCalls;   // num times appendRequest has been called
        uint32_t readCalls;     // num times readResponse has been called

        // Encodes what to return for bool and status in readResponse().
        // Returns false and STATUS_OK if empty.
        std::deque<bool> missingDataInResponse;
        std::deque<Status> returnStatuses;

        MultiOpTester(RamCloud* rc,
                      MultiOpObject* const requests[],
                      uint32_t numRequests,
                      uint32_t appendSize = 0)
              : MultiOp(rc, type, requests, numRequests)
              , appendSize(appendSize)
              , appendCalls(0)
              , readCalls(0)
              , missingDataInResponse()
              , returnStatuses()
        {
            test_ignoreBufferOverflow = true;
            startRpcs();
        }

        void appendRequest(MultiOpObject* request,
                          Buffer* buf)
        {
            if (appendSize) {
                string str(appendSize, '0');
                buf->appendExternal(str.c_str(), appendSize);
            }
            TEST_LOG("append request %u", ++appendCalls);
        }

        bool readResponse(MultiOpObject* request,
                          Buffer* response,
                          uint32_t* respOffset)
        {
            TEST_LOG("read response at %u ", *respOffset);
            readCalls++;

            if (!returnStatuses.empty()) {
                request->status = returnStatuses.front();
                returnStatuses.pop_front();
            } else {
                request->status = STATUS_OK;
            }

            if (!missingDataInResponse.empty()) {
                bool ret = missingDataInResponse.front();
                missingDataInResponse.pop_front();
                return ret;
            }

            return false;
        }
    };

    DISALLOW_COPY_AND_ASSIGN(MultiOpTest);
};

namespace {
static const Status UNDERWAY = MultiOp::UNDERWAY;
}

TEST_F(MultiOpTest, constructor) {
    MultiOpObject* requests[] = {&objects[0], &objects[1],
                                    &objects[4]};

    MultiOpTester request(ramcloud.get(), requests, 3);
    EXPECT_EQ(3UL, request.workQueue.size());

    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(requests[i], request.workQueue.at(i));
    }
}

TEST_F(MultiOpTest, basics) {
    MultiOpObject* requests[] = {&objects[0], &objects[1], &objects[2],
            &objects[3], &objects[4], &objects[5]};
    MultiOpTester request(ramcloud.get(), requests, 6);
    request.wait();
    ASSERT_TRUE(request.isReady());
    for (int i = 0; i < 6; i++) {
        EXPECT_STREQ("STATUS_OK", statusToSymbol(objects[1].status));
    }
}

TEST_F(MultiOpTest, cancel) {
    MultiOpObject* requests[] = {&objects[0], &objects[1], &objects[4]};
    session1->dontNotify = true;

    MultiOpTester request(ramcloud.get(), requests, 3);

    EXPECT_EQ("mock:host=master1(2) mock:host=master3(1)",
            rpcStatus(request));
    request.cancel();
    EXPECT_EQ("- -", rpcStatus(request));
}

TEST_F(MultiOpTest, isReady) {
    MultiOpObject* requests[] = {&objects[0], &objects[1],
                                 &objects[3], &objects[4]};
    session1->dontNotify = true;
    session3->dontNotify = true;

    // Launch RPCs, let one of them finish.
    MultiOpTester request(ramcloud.get(), requests, 4);
    EXPECT_EQ("mock:host=master1(2) mock:host=master2(1)",
            rpcStatus(request));
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("mock:host=master1(2) mock:host=master3(1)",
            rpcStatus(request));
    EXPECT_EQ(4UL, request.appendCalls);
    EXPECT_EQ(1UL, request.readCalls);

    // Make sure that isReady calls don't do anything until something
    // finishes.
    EXPECT_FALSE(request.isReady());
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("mock:host=master1(2) mock:host=master3(1)",
            rpcStatus(request));
    EXPECT_EQ(1UL, request.readCalls);

    // Let the remaining RPCs finish one at a time.
    session1->lastNotifier->completed();
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("- mock:host=master3(1)", rpcStatus(request));
    EXPECT_EQ(3UL, request.readCalls);

    session3->lastNotifier->completed();
    EXPECT_TRUE(request.isReady());
    EXPECT_EQ("- -", rpcStatus(request));
    EXPECT_EQ(4UL, request.appendCalls);
    EXPECT_EQ(4UL, request.readCalls);
}

TEST_F(MultiOpTest, startRpcs_skipFinishedOrFailed) {
    MultiOpObject* requests[] = {&objects[0], &objects[1],
                                    &objects[5]};
    MultiOpTester request(ramcloud.get(), requests, 3);

    EXPECT_TRUE(request.isReady());
    EXPECT_TRUE(request.startRpcs());
    EXPECT_EQ(3UL, request.startIndex);
    EXPECT_EQ("- -", rpcStatus(request));
}

TEST_F(MultiOpTest, startRpcs_tableDoesntExist) {
    MultiOpObject object(tableId3+1, "bogus", 5);
    MultiOpObject* requests[] = {&object};
    MultiOpTester request(ramcloud.get(), requests, 1);

    EXPECT_TRUE(request.isReady());
    EXPECT_STREQ("STATUS_TABLE_DOESNT_EXIST", statusToSymbol(object.status));
}

TEST_F(MultiOpTest, startRpcs_noAvailableRpc) {
    MultiOpObject* requests[] = {&objects[0], &objects[3],
                                        &objects[4]};

    // During the first call, the last object can't be sent because all
    // RPC slots are in use and its session doesn't match.
    MultiOpTester request(ramcloud.get(), requests, 3);

    EXPECT_EQ("mock:host=master1(1) mock:host=master2(1)",
            rpcStatus(request));
    EXPECT_NE(UNDERWAY, requests[2]->status);

    // During the second call, the RPC slots are in process, so we don't
    // even check the session.
    EXPECT_FALSE(request.startRpcs());
    EXPECT_EQ("mock:host=master1(1) mock:host=master2(1)",
            rpcStatus(request));

    // During the third call RPC slots open up.
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("mock:host=master3(1) -", rpcStatus(request));
    EXPECT_EQ(UNDERWAY, requests[2]->status);

    EXPECT_TRUE(request.isReady());
    EXPECT_EQ("- -", rpcStatus(request));
    EXPECT_EQ(STATUS_OK, requests[2]->status);
}

TEST_F(MultiOpTest, startRpcs_50PercentRule) {
    MultiOpObject object4(tableId3, "bogus1", 6);
    MultiOpObject object5(tableId3, "bogus2", 6);

    MultiOpObject* requests[] = {&objects[0], &objects[3],
            &objects[4], &object4, &objects[1], &objects[2]};

    // 50% rule violated since it's in the tableId order 1 2 3 3 1 1
    // The batch of 3's violate the rule.
    MultiOpTester request(ramcloud.get(), requests, 5);
    EXPECT_EQ("mock:host=master1(1) mock:host=master2(1)", rpcStatus(request));
    EXPECT_EQ(2UL, request.startIndex);
}

TEST_F(MultiOpTest, startRpcs_tooManyObjectsForOneRoundOfRpcs) {
    MultiOpObject object4(tableId1, "bogus1", 6);
    MultiOpObject object5(tableId1, "bogus2", 6);
    MultiOpObject* requests[] = {&objects[0], &objects[1],
            &objects[2], &objects[3], &object4, &object5};

    // Not enough space in RPC to send all requests in the first RPCs.
    MultiOpTester request(ramcloud.get(), requests, 6);
    EXPECT_EQ("mock:host=master1(3) mock:host=master2(1)", rpcStatus(request));

    // During the second call, still no space (first call
    // hasn't finished).
    EXPECT_FALSE(request.startRpcs());
    EXPECT_EQ("mock:host=master1(3) mock:host=master2(1)", rpcStatus(request));

    // First call has finished, so another starts.
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("mock:host=master1(2) -", rpcStatus(request));

    EXPECT_TRUE(request.isReady());
    EXPECT_EQ("- -", rpcStatus(request));
}

TEST_F(MultiOpTest, startRpcs_largeAppends) {
    MultiOpObject* requests[] = {&objects[0], &objects[1]};

    // Make sure 2 requests don't fit in one rpc.
    uint32_t max_size = MultiOp::maxRequestSize/2 + 1;
    MultiOpTester request(ramcloud.get(), requests, 2, max_size);
    EXPECT_EQ("mock:host=master1(1) mock:host=master1(1)", rpcStatus(request));
    EXPECT_EQ(UNDERWAY, requests[0]->status);
    EXPECT_EQ(UNDERWAY, requests[1]->status);

    request.wait();
    EXPECT_EQ(STATUS_OK, requests[0]->status);
    EXPECT_EQ(STATUS_OK, requests[1]->status);
}

TEST_F(MultiOpTest, startRpcs_requestTooLarge) {
    MultiOpObject* requests[] = {&objects[0]};

    uint32_t max_size = MultiOp::maxRequestSize;
    MultiOpTester request(ramcloud.get(), requests, 1, max_size + 1);
    EXPECT_TRUE(request.isReady());
    EXPECT_STREQ("STATUS_REQUEST_TOO_LARGE",
                                        statusToSymbol(requests[0]->status));
}

namespace {
bool multiOpWaitThreadFilter(string s) {
        return s == "multiOpWaitThread";
}

// Helper function that runs in a separate thread for the following test.
static void multiOpWaitThread(MultiOp* request) {
    request->wait();
    TEST_LOG("request finished");
}
}

TEST_F(MultiOpTest, wait) {
    TestLog::Enable _(multiOpWaitThreadFilter);
    MultiOpObject* requests[] = {&objects[0]};
    session1->dontNotify = true;
    MultiOpTester request(ramcloud.get(), requests, 1);
    std::thread thread(multiOpWaitThread, &request);
    usleep(1000);
    EXPECT_EQ("", TestLog::get());
    session1->lastNotifier->completed();

    // Give the waiting thread a chance to finish.
    for (int i = 0; i < 1000; i++) {
        if (TestLog::get().size() != 0)
            break;
        usleep(100);
    }
    EXPECT_EQ("multiOpWaitThread: request finished", TestLog::get());
    thread.join();
}

TEST_F(MultiOpTest, wait_canceled) {
    MultiOpObject* requests[] = {&objects[0]};
    session1->dontNotify = true;

    MultiOpTester request(ramcloud.get(), requests, 1);
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

TEST_F(MultiOpTest, removeRequestAt) {
    MultiOpObject* requests[] = {&objects[0], &objects[1],
            &objects[2], &objects[3], &objects[4],
            &objects[5], &objects[0], &objects[1]};
    MultiOpTester request(ramcloud.get(), requests, 8);

    // 4U Results from the original scheduling.
    EXPECT_EQ(4UL, request.startIndex);
    EXPECT_EQ(8UL, request.workQueue.size());

    for (int ii = 0; ii < 8; ii++)
        ASSERT_EQ(requests[ii], request.workQueue[ii]);

    // Remove -> Expect startIndex++ and request to swap from slot 4 to 7
    request.removeRequestAt(7);
    EXPECT_EQ(5UL, request.startIndex);
    EXPECT_EQ(requests[4], request.workQueue[7]);

    // Remove Consecutive -> expect nothing to be swapped
    request.removeRequestAt(5);
    EXPECT_EQ(6UL, request.startIndex);
    EXPECT_EQ(requests[5], request.workQueue[5]);
    EXPECT_EQ(requests[6], request.workQueue[6]);
}

TEST_F(MultiOpTest, retryRequest) {
    MultiOpObject* requests[] = {&objects[0], &objects[1], &objects[2],
            &objects[3], &objects[4], &objects[5], &objects[0], &objects[1]};
    MultiOpTester request(ramcloud.get(), requests, 8);

    // 4U Results from the original scheduling.
    EXPECT_EQ(4UL, request.startIndex);
    EXPECT_EQ(8UL, request.workQueue.size());

    for (int ii = 0; ii< 8; ii++)
        ASSERT_EQ(requests[ii], request.workQueue[ii]);

    //Retry object[0] -> Should end up in startIndex's location.
    request.retryRequest(requests[0]);

    EXPECT_EQ(3UL, request.startIndex);
    EXPECT_EQ(&objects[0], request.workQueue[3]);
}

TEST_F(MultiOpTest, PartRpc_finish_transportError) {
    MultiOpObject* requests[] = {&objects[0], &objects[1]};
    session1->dontNotify = true;

    MultiOpTester request(ramcloud.get(), requests, 2);
    EXPECT_EQ("mock:host=master1(2) -", rpcStatus(request));
    EXPECT_STREQ("STATUS_UNKNOWN", statusToSymbol(objects[0].status));
    EXPECT_STREQ("STATUS_UNKNOWN", statusToSymbol(objects[1].status));
    EXPECT_EQ(UNDERWAY, objects[0].status);
    EXPECT_EQ(UNDERWAY, objects[1].status);
    session1->lastNotifier->failed();
    request.finishRpc(request.rpcs[0].get());
    EXPECT_STREQ("STATUS_RETRY", statusToSymbol(objects[0].status));
    EXPECT_STREQ("STATUS_RETRY", statusToSymbol(objects[0].status));
    request.rpcs[0].destroy();
    session1->dontNotify = false;
    request.wait();
}

TEST_F(MultiOpTest, PartRpc_finish_shortResponse) {
    // This test checks for proper handling of responses that are
    // too short.
    MultiOpObject* requests[] = {&objects[0], &objects[1]};
    session1->dontNotify = true;
    MultiOpTester request(ramcloud.get(), requests, 2);
    EXPECT_EQ("mock:host=master1(2) -", rpcStatus(request));

    // Can't read status from response, expect a retry
    session1->lastNotifier->completed();
    request.missingDataInResponse.push_back(true);
    EXPECT_FALSE(request.isReady()); // quits after 1st response
    ASSERT_EQ(0UL, request.missingDataInResponse.size()); // ensure consumption
    EXPECT_EQ("mock:host=master1(2) -", rpcStatus(request));

    // Can't read Response::Part from response again, retry.
    session1->lastNotifier->completed();
    request.missingDataInResponse.push_back(true);
    EXPECT_FALSE(request.isReady()); // quits after 1st response
    ASSERT_EQ(0UL, request.missingDataInResponse.size()); // ensure consumption
    EXPECT_EQ("mock:host=master1(2) -", rpcStatus(request));

    // Can't read object data from response (first object complete,
    // this happens during the second object).
    session1->lastNotifier->completed();
    request.missingDataInResponse.push_back(false);
    request.missingDataInResponse.push_back(true);
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("mock:host=master1(1) -", rpcStatus(request));
    ASSERT_EQ(0UL, request.missingDataInResponse.size()); // ensure consumption

    // Let the request finally succeed.
    session1->lastNotifier->completed();
    EXPECT_TRUE(request.isReady());
}

namespace {
bool finishRpcFlushFilter(string s) {
        return s == "finishRpc" || s == "flush" || s == "flushSession";
}
}

TEST_F(MultiOpTest, PartRpc_unknownTable) {
    TestLog::Enable _(finishRpcFlushFilter);
    MultiOpObject* requests[] = {&objects[0], &objects[1],
            &objects[2]};
    session1->dontNotify = true;
    MultiOpTester request(ramcloud.get(), requests, 3);
    EXPECT_EQ("mock:host=master1(3) -", rpcStatus(request));

    // Modify the response to reject all objects.
    request.returnStatuses.push_back(STATUS_UNKNOWN_TABLET);
    request.returnStatuses.push_back(STATUS_OBJECT_DOESNT_EXIST);
    request.returnStatuses.push_back(STATUS_UNKNOWN_TABLET);
    session1->lastNotifier->completed();
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("finishRpc: Server mock:host=master1 doesn't store "
            "<1, object1-1>; refreshing object map",
            TestLog::get());
    EXPECT_EQ("mock:host=master1(2) -", rpcStatus(request));

    // Let the retry succeed.
    session1->lastNotifier->completed();
    TestLog::reset();
    EXPECT_TRUE(request.isReady());
    EXPECT_STREQ("", TestLog::get().c_str());
}

TEST_F(MultiOpTest, PartRpc_handleTransportError) {
    TestLog::Enable _(finishRpcFlushFilter);
    MultiOpObject* requests[] = {&objects[0]};
    session1->dontNotify = true;
    MultiOpTester request(ramcloud.get(), requests, 1);
    EXPECT_EQ("mock:host=master1(1) -", rpcStatus(request));
    session1->lastNotifier->failed();
    request.wait();
    EXPECT_EQ("flushSession: flushing session for mock:host=master1",
            TestLog::get());
}
}
