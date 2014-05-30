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
#include "MultiRead.h"
#include "RawMetrics.h"
#include "ServerMetrics.h"
#include "ShortMacros.h"
#include "RamCloud.h"

namespace RAMCloud {

static bool
antiGetEntryFilter(string s)
{
    return s != "getEntry";
}

class MultiReadTest : public ::testing::Test {
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
    Tub<ObjectBuffer> values[6];
    MultiReadObject objects[6];

  public:
    MultiReadTest()
        : logEnabler(antiGetEntryFilter)
        , context()
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
        //ramcloud->write(tableId2, "object2-1", 9, "value:2-1");

        // write a multi-key object:
        uint8_t numKeys = 3;
        KeyInfo keyList[3];
        // primary key
        keyList[0].keyLength = 9;
        keyList[0].key = "object2-1";
        // Key 1 does not exist
        keyList[1].keyLength = 0;
        keyList[1].key = NULL;
        keyList[2].keyLength = 8;
        keyList[2].key = "otherkey";

        ramcloud->write(tableId2, numKeys, keyList, "value:2-1",
                            NULL, NULL, false);

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

    const void *
    bufferString(Tub<ObjectBuffer>& buffer)
    {
        if (!buffer)
            return "uninitialized";
        return buffer.get()->getValue();
    }

    DISALLOW_COPY_AND_ASSIGN(MultiReadTest);
};

TEST_F(MultiReadTest, basics_end_to_end) {
    MultiReadObject* requests[] = {&objects[0], &objects[1], &objects[2],
            &objects[3], &objects[4], &objects[5]};
    MultiRead request(ramcloud.get(), requests, 6);
    request.wait();
    ASSERT_TRUE(request.isReady());
    EXPECT_STREQ("STATUS_OK", statusToSymbol(objects[0].status));
    EXPECT_EQ("value:1-1", string(reinterpret_cast<const char*>(
                           bufferString(values[0])), 9));
    EXPECT_EQ("object1-1", string(reinterpret_cast<const char*>(
                           values[0].get()->getKey()), 9));

    EXPECT_STREQ("STATUS_OK", statusToSymbol(objects[1].status));
    EXPECT_EQ("value:1-2", string(reinterpret_cast<const char*>(
                           bufferString(values[1])), 9));
    EXPECT_EQ("object1-2", string(reinterpret_cast<const char*>(
                           values[1].get()->getKey()), 9));

    EXPECT_STREQ("STATUS_OK", statusToSymbol(objects[2].status));
    EXPECT_EQ("value:1-3", string(reinterpret_cast<const char*>(
                           bufferString(values[2])), 9));
    EXPECT_EQ("object1-3", string(reinterpret_cast<const char*>(
                           values[2].get()->getKey()), 9));

    EXPECT_STREQ("STATUS_OK", statusToSymbol(objects[3].status));
    EXPECT_EQ("value:2-1", string(reinterpret_cast<const char*>(
                           bufferString(values[3])), 9));
    EXPECT_EQ("object2-1", string(reinterpret_cast<const char*>(
                           values[3].get()->getKey()), 9));
    EXPECT_STREQ((const char *)NULL, (const char *)values[3].get()->getKey(1));
    EXPECT_EQ("otherkey", string(reinterpret_cast<const char*>(
                           values[3].get()->getKey(2)), 8));

    EXPECT_STREQ("STATUS_OK", statusToSymbol(objects[4].status));
    EXPECT_EQ("value:3-1", string(reinterpret_cast<const char*>(
                           bufferString(values[4])), 9));
    EXPECT_EQ("object3-1", string(reinterpret_cast<const char*>(
                           values[4].get()->getKey()), 9));

    EXPECT_STREQ("STATUS_OBJECT_DOESNT_EXIST",
            statusToSymbol(objects[5].status));
    EXPECT_EQ("uninitialized", string(reinterpret_cast<const char*>(
                               bufferString(values[5])), 13));
}

TEST_F(MultiReadTest, appendRequest) {
    MultiReadObject* requests[] = {&objects[0]};
    uint32_t dif, before;
    Buffer buf;

    // Create a non-operating multi write
    MultiRead request(ramcloud.get(), requests, 0);
    request.wait();

    before = buf.getTotalLength();
    request.appendRequest(requests[0], &buf);
    dif = buf.getTotalLength() - before;

    uint32_t expected_size = sizeof32(WireFormat::MultiOp::Request::ReadPart) +
                    requests[0]->keyLength;
    EXPECT_EQ(expected_size, dif);
}

TEST_F(MultiReadTest, readResponse_shortResponse) {
    // This test checks for proper handling of responses that are
    // too short.
    TestLog::reset();
    MultiReadObject* requests[] = {&objects[0], &objects[1]};
    session1->dontNotify = true;
    MultiRead request(ramcloud.get(), requests, 2);
    EXPECT_EQ("mock:host=master1(2) -", rpcStatus(request));

    // Can't read status from response.
    session1->lastResponse->truncate(11);
    session1->lastNotifier->completed();
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("readResponse: missing Response::Part", TestLog::get());
    TestLog::reset();
    EXPECT_EQ("mock:host=master1(2) -", rpcStatus(request));

    // Can't read Response::Part from response.
    session1->lastResponse->truncate(18);
    session1->lastNotifier->completed();
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("readResponse: missing Response::Part", TestLog::get());
    TestLog::reset();
    EXPECT_EQ("mock:host=master1(2) -", rpcStatus(request));

    // Can't read object data from response (first object complete,
    // this happens during the second object).
    session1->lastResponse->truncate(session1->lastResponse->size() - 1);
    session1->lastNotifier->completed();
    EXPECT_FALSE(request.isReady());
    EXPECT_EQ("readResponse: missing object data", TestLog::get());
    TestLog::reset();
    EXPECT_EQ("mock:host=master1(1) -", rpcStatus(request));

    // Let the request finally succeed.
    session1->lastNotifier->completed();
    EXPECT_TRUE(request.isReady());
    EXPECT_EQ("value:1-1", string(reinterpret_cast<const char*>(
                           bufferString(values[0])), 9));
    EXPECT_EQ("value:1-2", string(reinterpret_cast<const char*>(
                           bufferString(values[1])), 9));
}
}  // namespace RAMCloud
