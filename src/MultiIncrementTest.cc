/* Copyright (c) 2011-2014 Stanford University
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
#include "MultiIncrement.h"
#include "RawMetrics.h"
#include "ServerMetrics.h"
#include "ShortMacros.h"
#include "RamCloud.h"

namespace RAMCloud {

class MultiIncrementTest : public ::testing::Test {
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
    Tub<MultiIncrementObject> objects[6];

  public:
    MultiIncrementTest()
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
        uint16_t keyLen9 = 9;
        objects[0].construct(tableId1, "object1-1", keyLen9, 1, 0.0);
        objects[1].construct(tableId1, "object1-2", keyLen9, 0, 1.1);
        objects[2].construct(tableId1, "object1-3", keyLen9, -1, 0.0);
        objects[3].construct(tableId2, "object2-1", keyLen9, 0, -1.0);
        objects[4].construct(tableId3, "object3-1", keyLen9, 0, 0);
        objects[5].construct(101, "object1-1", keyLen9, 0, 0);
    }

    // Returns a string describing the status of the RPCs for request.
    // For example:
    //    mock:host=master1(2) -
    // means that rpcs[0] has an active RPC to master1 that is requesting
    // 2 objects, and rpcs[1] is not currently active ("-").
    string
    rpcStatus(MultiIncrement& request)
    {
        string result;
        const char* separator = "";
        for (uint32_t i = 0; i < MultiIncrement::MAX_RPCS; i++) {
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

    DISALLOW_COPY_AND_ASSIGN(MultiIncrementTest);
};

// Filter out the desired log entries below (skipping log and replicated segment
// messages made during the multiremove operations).
static bool
testLogFilter(string s)
{
    return s == "multiIncrementWaitThread" ||
           s == "readResponse" ||
           s == "finishRpc" ||
           s == "flush" ||
           s == "flushSession";
}

TEST_F(MultiIncrementTest, basics_end_to_end) {
    MultiIncrementObject* requests[] = {
        objects[0].get(), objects[1].get(), objects[2].get(),
        objects[3].get(), objects[4].get(), objects[5].get()
    };
    ramcloud->multiIncrement(requests, 6);
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

TEST_F(MultiIncrementTest, appendRequest) {
    MultiIncrementObject* requests[] = {objects[0].get()};
    uint32_t dif, before;
    Buffer buf;

    // Create a non-operating multi increment
    MultiIncrement request(ramcloud.get(), requests, 0);
    request.wait();

    before = buf.size();
    request.appendRequest(requests[0], &buf);
    dif = buf.size() - before;

    uint32_t expected_size =
                    sizeof32(WireFormat::MultiOp::Request::IncrementPart) +
                    requests[0]->keyLength;
    EXPECT_EQ(expected_size, dif);
}

TEST_F(MultiIncrementTest, readResponse_shortResponses) {
    // This test checks for proper handling of responses that are
    // too short.
    TestLog::Enable _(testLogFilter);
    MultiIncrementObject* requests[] = { objects[0].get(), objects[1].get() };
    session1->dontNotify = true;
    MultiIncrement request(ramcloud.get(), requests, 2);
    EXPECT_EQ("mock:host=master1(2) -", rpcStatus(request));
    // Initial order of requests is object 0, object 1.

    // Can't read second Response::Part from both responses.
    session1->lastResponse->truncate(session1->lastResponse->size()
        - (sizeof32(WireFormat::MultiOp::Response::IncrementPart) + 1));
    session1->lastNotifier->completed();
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("readResponse: missing Response::Part", TestLog::get());
    TestLog::reset();
    EXPECT_EQ("mock:host=master1(2) -", rpcStatus(request));
    // MultiWrite retries requests in the order object 1, object 0.

    // Can't read second Response::Part from response, retries object 0.
    session1->lastResponse->truncate(session1->lastResponse->size() - 1);
    session1->lastNotifier->completed();
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("readResponse: missing Response::Part", TestLog::get());
    TestLog::reset();
    EXPECT_EQ("mock:host=master1(1) -", rpcStatus(request));

    // Can't read Response::Part from response again, retries object 0.
    session1->lastResponse->truncate(session1->lastResponse->size() - 1);
    session1->lastNotifier->completed();
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("readResponse: missing Response::Part", TestLog::get());
    TestLog::reset();
    EXPECT_EQ("mock:host=master1(1) -", rpcStatus(request));

    // Let the request finally succeed. Note that each increment will have been
    // retried, so the versions will be larger than expected. That is,
    // objects[0] and objects[1] were written with version=1 and version=2,
    // respectively, but the responses were bad. So, both are retried.
    // Object[1] succeeds the second time and is stored with version=3.
    // Object[0] fails the second time and third time, but succeeds the
    // fourth time with version=4.

    session1->lastNotifier->completed();
    EXPECT_TRUE(request.isReady());
    EXPECT_EQ(STATUS_OK, objects[0]->status);
    EXPECT_EQ(STATUS_OK, objects[1]->status);
    EXPECT_EQ(4U, objects[0]->version);
    EXPECT_EQ(3U, objects[1]->version);
}

}  // namespace RAMCloud
