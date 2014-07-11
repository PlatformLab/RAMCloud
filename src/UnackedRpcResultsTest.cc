/* Copyright (c) 2014 Stanford University
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
#include "UnackedRpcResults.h"

namespace RAMCloud {

class UnackedRpcResultsTest : public ::testing::Test {
  public:
    UnackedRpcResults results;

    UnackedRpcResultsTest() : results()
    {
        void* result;
        results.addClient(1);
        results.checkDuplicate(1, 10, 5, &result);
        results.recordCompletion(1, 10, reinterpret_cast<void*>(1010));
    }

    DISALLOW_COPY_AND_ASSIGN(UnackedRpcResultsTest);
};

TEST_F(UnackedRpcResultsTest, addClient) {
    results.addClient(100);
    std::unordered_map<uint64_t, UnackedRpcResults::Client*>::iterator it;
    it = results.clients.find(100);
    EXPECT_NE(it, results.clients.end());

    it = results.clients.find(101);
    EXPECT_EQ(it, results.clients.end());
}

TEST_F(UnackedRpcResultsTest, checkDuplicate) {
    void* result;

    //1. No client info
    EXPECT_THROW(results.checkDuplicate(2, 1, 0, &result),
                 UnackedRpcResults::NoClientInfo);

    //2. Stale Rpc.
    EXPECT_THROW(results.checkDuplicate(1, 4, 3, &result),
                 UnackedRpcResults::StaleRpc);

    //3. Fast-path new RPC (rpcId > maxRpcId == true).
    EXPECT_EQ(10UL, results.clients[1]->maxRpcId);
    EXPECT_FALSE(results.checkDuplicate(1, 11, 6, &result));
    EXPECT_EQ(0UL, (uint64_t)result);
    EXPECT_EQ(11UL, results.clients[1]->maxRpcId);
    EXPECT_EQ(6UL, results.clients[1]->maxAckId);

    EXPECT_TRUE(results.checkDuplicate(1, 11, 6, &result));
    EXPECT_EQ(0UL, (uint64_t)result);

    //4. Duplicate RPC.
    EXPECT_TRUE(results.checkDuplicate(1, 10, 6, &result));
    EXPECT_EQ(1010UL, (uint64_t)result);
    EXPECT_EQ(6UL, results.clients[1]->maxAckId);

    //5. Inside the window and new RPC.
    EXPECT_FALSE(results.checkDuplicate(1, 9, 7, &result));
    EXPECT_EQ(0UL, (uint64_t)result);
    EXPECT_EQ(7UL, results.clients[1]->maxAckId);

    EXPECT_TRUE(results.checkDuplicate(1, 9, 7, &result));
    EXPECT_EQ(0UL, (uint64_t)result);
}

TEST_F(UnackedRpcResultsTest, shouldRecover) {
    //Basic Function
    EXPECT_TRUE(results.shouldRecover(1, 10, 5));
    EXPECT_TRUE(results.shouldRecover(1, 11, 5));
    EXPECT_FALSE(results.shouldRecover(1, 5, 4));

    //Auto client insertion
    EXPECT_TRUE(results.shouldRecover(2, 4, 2)); //ClientId = 2 inserted.
    std::unordered_map<uint64_t, UnackedRpcResults::Client*>::iterator it;
    it = results.clients.find(2);
    EXPECT_NE(it, results.clients.end());

    //Ack update
    UnackedRpcResults::Client* client = it->second;
    EXPECT_EQ(2UL, client->maxAckId);
}

TEST_F(UnackedRpcResultsTest, recordCompletion) {
    void* result;
    EXPECT_FALSE(results.checkDuplicate(1, 11, 5, &result));
    EXPECT_EQ(0UL, (uint64_t)result);
    results.recordCompletion(1, 11, reinterpret_cast<void*>(1011));
    EXPECT_TRUE(results.checkDuplicate(1, 11, 5, &result));
    EXPECT_EQ(1011UL, (uint64_t)result);

    //Reusing spaces for acked rpcs.
    results.checkDuplicate(1, 12, 5, &result);
    results.recordCompletion(1, 12, reinterpret_cast<void*>(1012));
    results.checkDuplicate(1, 13, 5, &result);
    results.recordCompletion(1, 13, reinterpret_cast<void*>(1013));
    results.checkDuplicate(1, 14, 10, &result);
    results.recordCompletion(1, 14, reinterpret_cast<void*>(1014));
    results.checkDuplicate(1, 15, 11, &result);   //Ack up to rpcId = 11.
    results.recordCompletion(1, 15, reinterpret_cast<void*>(1015));
    results.checkDuplicate(1, 16, 5, &result);
    results.recordCompletion(1, 16, reinterpret_cast<void*>(1016));

    EXPECT_EQ(16UL, results.clients[1]->maxRpcId);
    EXPECT_EQ(5, results.clients[1]->len);

    //Resized Client keeps the original data.
    results.checkDuplicate(1, 17, 5, &result);

    EXPECT_EQ(15, results.clients[1]->len);
    for (int i = 12; i <= 16; ++i) {
        EXPECT_TRUE(results.checkDuplicate(1, i, 5, &result));
        EXPECT_EQ((uint64_t)(i + 1000), (uint64_t)result);
    }
    EXPECT_TRUE(results.checkDuplicate(1, 17, 5, &result));
    EXPECT_EQ(0UL, (uint64_t)result);

    results.recordCompletion(1, 17, reinterpret_cast<void*>(1017));
    EXPECT_TRUE(results.checkDuplicate(1, 17, 5, &result));
    EXPECT_EQ(1017UL, (uint64_t)result);
}

TEST_F(UnackedRpcResultsTest, isRpcAcked) {
    //1. Cleaned up client.
    EXPECT_TRUE(results.isRpcAcked(2, 1));

    //2. Existing client.
    EXPECT_TRUE(results.isRpcAcked(1, 5));
    EXPECT_FALSE(results.isRpcAcked(1, 6));
}

//TODO(seojin): tests for API functions.
TEST_F(UnackedRpcResultsTest, hasRecord) {
    UnackedRpcResults::Client *client = results.clients[1];
    EXPECT_TRUE(client->hasRecord(10));
}

TEST_F(UnackedRpcResultsTest, result) {
    UnackedRpcResults::Client *client = results.clients[1];
    EXPECT_EQ(1010UL, (uint64_t)client->result(10));
}

TEST_F(UnackedRpcResultsTest, recordNewRpc) {
    UnackedRpcResults::Client *client = results.clients[1];
    client->recordNewRpc(11);
    EXPECT_TRUE(client->hasRecord(11));

    //Invoke resizing and test if all of the records are kept.
    int startRpcId = 12;
    int originalLen = client->len;
    for (int i = startRpcId; i < startRpcId + 2 * originalLen; ++i) {
        client->recordNewRpc(i);
    }
    EXPECT_NE(originalLen, client->len);
    for (int i = startRpcId; i < startRpcId + 2 * originalLen; ++i) {
        EXPECT_TRUE(client->hasRecord(i));
        EXPECT_EQ(0UL, (uint64_t)client->result(i));
    }
    EXPECT_EQ(1010UL, (uint64_t)client->result(10));
}

TEST_F(UnackedRpcResultsTest, updateResult) {
    UnackedRpcResults::Client *client = results.clients[1];
    EXPECT_EQ(1010UL, (uint64_t)client->result(10));
    client->updateResult(10, reinterpret_cast<void*>(1099));
    EXPECT_EQ(1099UL, (uint64_t)client->result(10));

    client->recordNewRpc(11);
    EXPECT_EQ(0UL, (uint64_t)client->result(11));
    client->updateResult(11, reinterpret_cast<void*>(1011));
    EXPECT_EQ(1011UL, (uint64_t)client->result(11));
}

}  // namespace RAMCloud
