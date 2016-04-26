/* Copyright (c) 2016 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <gtest/gtest.h>
#include <string.h>

#include <algorithm>
#include <vector>

#include "TestUtil.h"
#include "Minimal.h"
#include "ObjectManager.h"
#include "SkipList.h"

namespace RAMCloud {

class SkipListTest: public ::testing::Test {
  public:
    Context context;
    ClusterClock clusterClock;
    ClientLeaseValidator clientLeaseValidator;
    ServerId serverId;
    ServerList serverList;
    ServerConfig masterConfig;
    MasterTableMetadata masterTableMetadata;
    UnackedRpcResults unackedRpcResults;
    PreparedOps preparedOps;
    TxRecoveryManager txRecoveryManager;
    TabletManager tabletManager;
    ObjectManager objMgr;
    uint64_t tableId;
    SkipList *sk;
    Buffer buffer;
    SkipList::Node *nullNode;

    SkipListTest()
        : context()
        , clusterClock()
        , clientLeaseValidator(&context, &clusterClock)
        , serverId(5)
        , serverList(&context)
        , masterConfig(ServerConfig::forTesting())
        , masterTableMetadata()
        , unackedRpcResults(&context, NULL, &clientLeaseValidator)
        , preparedOps(&context)
        , txRecoveryManager(&context)
        , tabletManager()
        , objMgr(&context,
                        &serverId,
                        &masterConfig,
                        &tabletManager,
                        &masterTableMetadata,
                        &unackedRpcResults,
                        &preparedOps,
                        &txRecoveryManager)
        , tableId(1)
        , sk(NULL)
        , buffer()
        , nullNode(NULL)
    {
        objMgr.initOnceEnlisted();
        tabletManager.addTablet(1, 0, ~0UL, TabletManager::NORMAL);
        unackedRpcResults.resetFreer(&objMgr);
        sk = new SkipList(&objMgr, 1);
    }

    ~SkipListTest() {
        buffer.reset();
        free(sk);
    }

    DISALLOW_COPY_AND_ASSIGN(SkipListTest);
};

static const double PROBABILITY = SkipList::PROBABILITY;
static const uint8_t MAX_FORWARD_POINTERS = SkipList::MAX_FORWARD_POINTERS;
static const uint64_t HEAD_NODEID = SkipList::HEAD_NODEID;
static const SkipList::NodeId INVALID_NODEID = SkipList::INVALID_NODEID_1;

TEST_F(SkipListTest, freeNode) {
    TestLog::Enable _;

    // Try error cases first
    EXPECT_NO_THROW(sk->freeNode(HEAD_NODEID));
    EXPECT_NO_THROW(sk->freeNode(HEAD_NODEID - 10));
    EXPECT_EQ(0U, sk->numEntries);
    EXPECT_EQ(0U, sk->logBuffer.size());
    EXPECT_EQ("freeNode: SkipList attempted to delete an invalid node 100 | "
              "freeNode: SkipList attempted to delete an invalid node 90",
                TestLog::get());
    TestLog::reset();

    // Swap the tableId so that the writeTombstone should fail
    uint64_t oldTableId = sk->tableId;
    sk->tableId = 193859892;
    EXPECT_ANY_THROW(sk->freeNode(HEAD_NODEID + 10));
    EXPECT_EQ("freeNode: Could not stage node 110 delete from table 193859892 "
            "into buffer: \"unknown table (may exist elsewhere)\"",
                                                                TestLog::get());
    EXPECT_EQ(0U, sk->numEntries);
    EXPECT_EQ(0U, sk->logBuffer.size());
    sk->tableId = oldTableId;

    // Delete something that doesn't exist and flush
    sk->freeNode(HEAD_NODEID + 1);
    EXPECT_EQ(1U, sk->numEntries);
    EXPECT_ANY_THROW(sk->objMgr->flushEntriesToLog(
                                            &sk->logBuffer, sk->numEntries));

    // Note: Correct usage of freeNode is tested in the end-to-end test
}

TEST_F(SkipListTest, writeNode) {
    TestLog::Enable _;
    Buffer in, out1, out2;

    const char* strKey = "Blah blah blah";
    uint16_t keyLen = downCast<uint16_t>(strlen(strKey) + 1);
    size_t nodeSize = sizeof(SkipList::Node)
                    + MAX_FORWARD_POINTERS*sizeof(SkipList::NodeId) + keyLen;
    size_t headNodeSize = nodeSize - keyLen;

    SkipList::NodeId nodeid = HEAD_NODEID + 100;
    SkipList::Node *writeNode = static_cast<SkipList::Node*>(
                                                        in.alloc(nodeSize));
    writeNode->levels = MAX_FORWARD_POINTERS;
    writeNode->keyLen = keyLen;
    writeNode->pkHash = 1001;
    memcpy(writeNode->key(), strKey, keyLen);

    // Failed write
    TestLog::reset();
    uint64_t tableId = sk->tableId;
    sk->tableId = 192309;
    EXPECT_EQ(headNodeSize, sk->bytesWritten);
    EXPECT_EQ(1U, sk->nodesWritten);            // Expect 1 for head
    EXPECT_ANY_THROW(sk->writeNode(writeNode, nodeid));
    EXPECT_EQ("writeNode: Could not stage SkipList node 200 write for table "
            "192309: unknown table (may exist elsewhere)", TestLog::get());
    EXPECT_EQ(headNodeSize, sk->bytesWritten);
    EXPECT_EQ(1U, sk->nodesWritten);       // Expect 1 for head
    sk->tableId = tableId;
    sk->numEntries = 0;

    // Good Writes
    SkipList::NodeId setNodeId = sk->writeNode(writeNode, nodeid);
    EXPECT_EQ(nodeid, setNodeId);
    EXPECT_EQ(headNodeSize + nodeSize, sk->bytesWritten);
    EXPECT_EQ(2U, sk->nodesWritten);

    writeNode->pkHash = 2222;
    SkipList::NodeId assignedNodeId = sk->writeNode(writeNode);
    EXPECT_EQ(HEAD_NODEID + 1U, assignedNodeId);
    EXPECT_EQ(HEAD_NODEID + 2U, sk->nextNodeId);
    EXPECT_EQ(2U, sk->numEntries);
    EXPECT_EQ(headNodeSize + 2*nodeSize, sk->bytesWritten);
    EXPECT_EQ(3U, sk->nodesWritten);

    // Now let's flush and read them back
    EXPECT_TRUE(sk->objMgr->flushEntriesToLog(&sk->logBuffer, sk->numEntries));
    Key key1(sk->tableId, &nodeid, sizeof(SkipList::NodeId));
    Key key2(sk->tableId, &assignedNodeId, sizeof(SkipList::NodeId));

    ASSERT_EQ(STATUS_OK, objMgr.readObject(key1, &out1, NULL, NULL, true));
    ASSERT_EQ(STATUS_OK, objMgr.readObject(key2, &out2, NULL, NULL, true));

    SkipList::Node *node1 = out1.getOffset<SkipList::Node>(0);
    SkipList::Node *node2 = out2.getOffset<SkipList::Node>(0);

    ASSERT_EQ(writeNode->size(), node2->size());
    ASSERT_EQ(writeNode->size(), out2.size());
    EXPECT_NE(0, memcmp(writeNode, node1, writeNode->size()));
    EXPECT_EQ(0, memcmp(writeNode, node2, writeNode->size()));
    EXPECT_EQ(1001U, node1->pkHash);

    // Now let's overwrite node 200 again.
    sk->writeNode(writeNode, nodeid);
    EXPECT_EQ(2U, sk->numEntries);
    EXPECT_TRUE(sk->objMgr->flushEntriesToLog(&sk->logBuffer, sk->numEntries));
    EXPECT_EQ(headNodeSize + 3*nodeSize, sk->bytesWritten);
    EXPECT_EQ(4U, sk->nodesWritten);

    out1.reset();
    ASSERT_EQ(STATUS_OK, objMgr.readObject(key1, &out1, NULL, NULL, true));
    node1 = out1.getOffset<SkipList::Node>(0);
    EXPECT_EQ(0, memcmp(writeNode, node1, writeNode->size()));
}

TEST_F(SkipListTest, readNode_errors) {
    TestLog::Enable _;

    uint64_t bytesReadBefore = sk->bytesRead;
    uint64_t nodesReadBefore = sk->nodesRead;

    // Read a non-existent node from an exiting table.
    EXPECT_EQ(nullNode, sk->readNode(10001, &buffer));
    EXPECT_EQ("readNode: Could not read SkipList Node 10001 from table 1:"
              "\"object doesn't exist\"", TestLog::get());
    EXPECT_EQ(0U, buffer.size());
    TestLog::reset();

    // Read from a non-existent table.
    uint64_t origTableId = sk->tableId;
    sk->tableId = 12345;
    EXPECT_EQ(nullNode, sk->readNode(HEAD_NODEID, &buffer));
    EXPECT_EQ("readNode: Could not read SkipList Node 100 from table 12345:"
            "\"unknown table (may exist elsewhere)\"", TestLog::get());
    EXPECT_EQ(0U, buffer.size());
    sk->tableId = origTableId;
    TestLog::reset();

    // Expect that nothing has been read according to the statistics.
    EXPECT_EQ(0U, sk->bytesRead - bytesReadBefore);
    EXPECT_EQ(0U, sk->nodesRead - nodesReadBefore);
}

TEST_F(SkipListTest, readNode) {
    TestLog::Enable _("readNode");

    // Write a node then read it back
    const char* strKey = "Blah blah blah";
    uint16_t keyLen = downCast<uint16_t>(strlen(strKey) + 1);
    size_t nodeSize = sizeof(SkipList::Node)
                    + MAX_FORWARD_POINTERS*sizeof(SkipList::NodeId) + keyLen;

    SkipList::NodeId nodeid = HEAD_NODEID + 100;
    SkipList::Node *writeNode =
                static_cast<SkipList::Node*>(buffer.alloc(nodeSize));
    writeNode->levels = MAX_FORWARD_POINTERS;
    writeNode->keyLen = keyLen;
    writeNode->pkHash = 1001;
    memcpy(writeNode->key(), strKey, keyLen);

    EXPECT_EQ(buffer.size(), writeNode->size()); // Sanity check
    Key key(sk->tableId, &nodeid, sizeof(SkipList::NodeId));
    Object obj(key, writeNode, writeNode->size(), 1, 0, buffer);

    Status writeStatus = objMgr.writeObject(obj, NULL, NULL);
    ASSERT_EQ(STATUS_OK, writeStatus);

    Buffer out;
    TestLog::reset();
    SkipList::Node *nullNode = NULL;
    SkipList::Node *readNode = sk->readNode(nodeid, &out);
    ASSERT_NE(nullNode, readNode);
    ASSERT_EQ(writeNode->size(), readNode->size());
    EXPECT_EQ(0, memcmp(readNode, writeNode, writeNode->size()));
    EXPECT_EQ("readNode: Read node 200 of size 111 bytes from table 1",
                                                                TestLog::get());

    EXPECT_EQ(1U, sk->nodesRead);
    EXPECT_EQ(writeNode->size(), sk->bytesRead);
}

TEST_F(SkipListTest, write_read_freeNode_endToEnd) {
    TestLog::Enable _;
    Buffer buffer;

    const char* key = "Blah blah blah";
    uint16_t keyLen = downCast<uint16_t>(strlen(key) + 1);
    size_t nodeSize = sizeof(SkipList::Node)
                    + MAX_FORWARD_POINTERS*sizeof(SkipList::NodeId) + keyLen;
    SkipList::Node* newNode = static_cast<SkipList::Node*>(
                                                        buffer.alloc(nodeSize));

    newNode->levels = MAX_FORWARD_POINTERS;
    newNode->keyLen = keyLen;
    newNode->pkHash = 1001;
    memcpy(newNode->key(), key, keyLen);

    SkipList::NodeId nodeid = HEAD_NODEID + 100;
    sk->writeNode(newNode, nodeid);
    EXPECT_EQ(1U, sk->numEntries);
    EXPECT_TRUE(sk->objMgr->flushEntriesToLog(&sk->logBuffer, sk->numEntries));
    EXPECT_EQ(0U, sk->numEntries);

    // Try invalid read first
    Buffer readBackBuffer;
    EXPECT_EQ(nullNode, sk->readNode(nodeid + 1, &readBackBuffer));
    EXPECT_EQ(0U, readBackBuffer.size());

    // Now read it for reals
    SkipList::Node *nullNode = NULL, *ptr;
    ptr = sk->readNode(nodeid, &readBackBuffer);
    EXPECT_NE(nullNode, ptr);
    EXPECT_EQ(nodeSize, readBackBuffer.size());
    EXPECT_STREQ(key, ptr->key());
    EXPECT_EQ(1001U, ptr->pkHash);

    // Free it and try to read again
    sk->freeNode(nodeid);
    EXPECT_EQ(1U, sk->numEntries);
    EXPECT_TRUE(sk->objMgr->flushEntriesToLog(&sk->logBuffer, sk->numEntries));

    // try to read deleted node.
    EXPECT_EQ(nullNode, sk->readNode(nodeid, &readBackBuffer));
}

TEST_F(SkipListTest, constructor) {
    TestLog::Enable _;
    EXPECT_EQ(SkipList::HEAD_NODEID + 1, sk->nextNodeId);

    // Manually read the data back to ensure it was properly written
    Key key(tableId, &HEAD_NODEID, sizeof(SkipList::NodeId));
    Status s = objMgr.readObject(key, &buffer, NULL, NULL, true);
    EXPECT_EQ(s, STATUS_OK);

    SkipList::Node *node = buffer.getOffset<SkipList::Node>(0);
    ASSERT_NE(nullNode, node);
    EXPECT_EQ(0U, node->keyLen);
    EXPECT_EQ(0U, node->pkHash);
    EXPECT_EQ(MAX_FORWARD_POINTERS, node->levels);
    uint64_t size = buffer.size();
    EXPECT_EQ(size, node->size());
}

static bool cmp(IndexEntry& a, IndexEntry& b) {
    return SkipList::compareIndexEntries(a, b) < 0;
}

TEST_F(SkipListTest, insert_multiple) {
    TestLog::Enable _;

    std::vector<IndexEntry> entries;
    entries.push_back({"ac", 3, 12});
    entries.push_back({"ac", 3, 12});
    entries.push_back({"aa", 3, 12});
    entries.push_back({"b" , 2, 12});
    entries.push_back({"e" , 2, 12});
    entries.push_back({"d" , 2, 12});
    entries.push_back({"f" , 2, 12});
    entries.push_back({"aa", 3, 14});
    entries.push_back({"aa", 3, 10});

    for (IndexEntry &e : entries)
        sk->insert(e);

    // Expect everything to be in sorted order when we get it back linked
    // at the lowest level
    Key headKey(tableId, &HEAD_NODEID, sizeof(SkipList::NodeId));
    ASSERT_EQ(STATUS_OK,
                        objMgr.readObject(headKey, &buffer, NULL, NULL, true));
    SkipList::Node *curr = buffer.getOffset<SkipList::Node>(0);
    SkipList::NodeId firstId = curr->next()[0];

    // Read first node after head
    buffer.reset();
    EXPECT_NE(INVALID_NODEID, firstId);
    Key firstKey(tableId, &firstId, sizeof(SkipList::NodeId));
    ASSERT_EQ(STATUS_OK,
                    objMgr.readObject(firstKey, &buffer, NULL, NULL, true));

    curr = buffer.getOffset<SkipList::Node>(0);

    // Compare all entries from here on.
    std::sort(entries.begin(), entries.end(), cmp);
    std::vector<IndexEntry>::iterator it = entries.begin();
    while (it != entries.end()) {
        ASSERT_EQ(it->keyLen, curr->keyLen);
        EXPECT_STREQ(static_cast<const char*>(it->key),
                static_cast<const char*>(curr->key()));
        EXPECT_EQ(it->pkHash, curr->pkHash);

        ++it;
        SkipList::NodeId nextId = curr->next()[0];

        if (nextId == INVALID_NODEID) {
            curr = NULL;
        } else {
            buffer.reset();
            Key nextKey(tableId, &nextId, sizeof(SkipList::NodeId));
            objMgr.readObject(nextKey, &buffer, NULL, NULL, true);
            curr = buffer.getOffset<SkipList::Node>(0);
        }
    }

    EXPECT_EQ(NULL, curr);
}

TEST_F(SkipListTest, insert_probability) {
    TestLog::Enable _;
    Buffer currBuffer, afterBuffer;
    // We make insert be n levels deep here by manipulating mock random so that
    // it will cross the probability threshold n times.
    static_assert(MAX_FORWARD_POINTERS >= 8,
            "This test requires the SkipList to be able to handle >= 8 levels");

    sk->insert({"a", 2, 10}, 5);
    SkipList::Node *head = sk->readNode(HEAD_NODEID, &currBuffer);
    SkipList::NodeId firstId = head->next()[0];
    ASSERT_NE(INVALID_NODEID, firstId);
    currBuffer.reset();
    SkipList::Node *first = sk->readNode(firstId, &currBuffer);
    ASSERT_NE(nullNode, first);
    EXPECT_EQ(5U, first->levels);
    EXPECT_EQ(10U, first->pkHash);
    EXPECT_EQ(2U, first->keyLen);

    // Make it insert 7 levels
    sk->insert({"b", 2, 11}, 7);
    sk->insert({"e", 3, 14}, 6);
    sk->insert({"c", 2, 12}, 3);
    sk->insert({"d", 2, 13}, 1);

    // At this point the list should look something like this:
    // .....
    // head(7) -> --------------------------------------> null
    // head(6) -> ------> b|11 -> ----------------------> null
    // head(5) -> ------> b|11 -> --------------> e|14 -> null
    // head(4) -> a|10 -> b|11 -> --------------> e|14 -> null
    // head(3) -> a|10 -> b|11 -> --------------> e|14 -> null
    // head(2) -> a|10 -> b|11 -> c|12 -> ------> e|14 -> null
    // head(1) -> a|10 -> b|11 -> c|12 -> ------> e|14 -> null
    // head(0) -> a|10 -> b|11 -> c|12 -> d|13 -> e|14 -> null

    // Now let's verify everything matches the representation above by checking
    // pkHashes as a proxy.
    currBuffer.reset();
    SkipList::Node *curr = sk->readNode(HEAD_NODEID, &currBuffer);
    EXPECT_EQ(INVALID_NODEID, curr->next()[7]);
    ASSERT_NE(INVALID_NODEID, curr->next()[6]);
    EXPECT_EQ(11U, sk->readNode(curr->next()[6], &afterBuffer)->pkHash);
    EXPECT_EQ(11U, sk->readNode(curr->next()[5], &afterBuffer)->pkHash);

    for (int i = 0; i < 5; ++i) {
        ASSERT_NE(INVALID_NODEID, curr->next()[i]);
        EXPECT_EQ(10U, sk->readNode(curr->next()[i], &afterBuffer)->pkHash);
    }

    // Node a|10
    curr = sk->readNode(curr->next()[0], &currBuffer);
    EXPECT_EQ(5U, curr->levels);
    EXPECT_EQ(10U, curr->pkHash);
    for (int i = 0; i < 5; ++i) {
        ASSERT_NE(INVALID_NODEID, curr->next()[i]);
        EXPECT_EQ(11U, sk->readNode(curr->next()[i], &afterBuffer)->pkHash);
    }

    // node b11
    curr = sk->readNode(curr->next()[0], &currBuffer);
    EXPECT_EQ(7U, curr->levels);
    EXPECT_EQ(11U, curr->pkHash);
    EXPECT_EQ(INVALID_NODEID, curr->next()[6]);
    ASSERT_NE(INVALID_NODEID, curr->next()[5]);
    EXPECT_EQ(14U, sk->readNode(curr->next()[5], &afterBuffer)->pkHash);
    ASSERT_NE(INVALID_NODEID, curr->next()[4]);
    EXPECT_EQ(14U, sk->readNode(curr->next()[4], &afterBuffer)->pkHash);
    ASSERT_NE(INVALID_NODEID, curr->next()[3]);
    EXPECT_EQ(14U, sk->readNode(curr->next()[3], &afterBuffer)->pkHash);
    ASSERT_NE(INVALID_NODEID, curr->next()[2]);
    EXPECT_EQ(12U, sk->readNode(curr->next()[2], &afterBuffer)->pkHash);
    ASSERT_NE(INVALID_NODEID, curr->next()[1]);
    EXPECT_EQ(12U, sk->readNode(curr->next()[1], &afterBuffer)->pkHash);
    ASSERT_NE(INVALID_NODEID, curr->next()[0]);
    EXPECT_EQ(12U, sk->readNode(curr->next()[0], &afterBuffer)->pkHash);

    // node c12
    curr = sk->readNode(curr->next()[0], &currBuffer);
    EXPECT_EQ(3U, curr->levels);
    EXPECT_EQ(12U, curr->pkHash);
    EXPECT_EQ(14U, sk->readNode(curr->next()[2], &afterBuffer)->pkHash);
    EXPECT_EQ(14U, sk->readNode(curr->next()[1], &afterBuffer)->pkHash);
    EXPECT_EQ(13U, sk->readNode(curr->next()[0], &afterBuffer)->pkHash);

    // node d13
    curr = sk->readNode(curr->next()[0], &currBuffer);
    EXPECT_EQ(1U, curr->levels);
    EXPECT_EQ(13U, curr->pkHash);
    ASSERT_NE(INVALID_NODEID, curr->next()[0]);
    EXPECT_EQ(14U, sk->readNode(curr->next()[0], &afterBuffer)->pkHash);

    // node e14
    curr = sk->readNode(curr->next()[0], &currBuffer);
    EXPECT_EQ(6U, curr->levels);
    EXPECT_EQ(14U, curr->pkHash);
    for (int i = 0; i < curr->levels; ++i) {
        EXPECT_EQ(INVALID_NODEID, curr->next()[i]);
    }
}

TEST_F(SkipListTest, findLower) {
    TestLog::Enable _("readNode");
    // In theory, when searching for a particular node, we should only
    // have to read as many nodes as the shortest path to that node +
    // up to MAX_FORWARD_POINTERS. This test ensures that by examining the
    // node reads for various lookups.

    // Create a tree that looks like this
    // ----------------------------------------------> e|17
    // a|11 -----------------> d|14 -----------------> e|17
    // a|11 -> b|12 ---------> d|14 -> e|15 ---------> e|17
    // a|11 -> b|12 ---------> d|14 -> e|15 ---------> e|17
    // a|11 -> b|12 ---------> d|14 -> e|15 -> e|16 -> e|17
    // a|11 -> b|12 -> c|13 -> d|14 -> e|15 -> e|16 -> e|17
    static_assert(MAX_FORWARD_POINTERS >= 6,
            "This test requires 6 or more levels in the skiplist");

    sk->insert({"a", 2, 11}, 5);
    sk->insert({"b", 2, 12}, 4);
    sk->insert({"c", 2, 13}, 1);
    sk->insert({"d", 2, 14}, 5);
    sk->insert({"e", 2, 15}, 4);
    sk->insert({"e", 2, 16}, 2);
    sk->insert({"e", 2, 17}, 6);

    uint64_t nodesReadBefore = sk->nodesRead;

    std::pair<SkipList::Node*, SkipList::NodeId> result[MAX_FORWARD_POINTERS];
    TestLog::reset();
    SkipList::Node *nextNode = NULL;
    bool exactFound =
                sk->findLower({"d", 2, 14}, result, &buffer, true, &nextNode);
    EXPECT_EQ(6U, sk->nodesRead - nodesReadBefore); // head, e, a, d, b, c
    EXPECT_EQ(HEAD_NODEID, result[5].second);
    EXPECT_EQ(11U, result[4].first->pkHash);
    EXPECT_EQ(12U, result[3].first->pkHash);
    EXPECT_EQ(12U, result[2].first->pkHash);
    EXPECT_EQ(12U, result[1].first->pkHash);
    EXPECT_EQ(13U, result[0].first->pkHash);
    ASSERT_EQ(true, exactFound);
    ASSERT_NE(nullNode, nextNode);
    EXPECT_EQ(14U, nextNode->pkHash);
    EXPECT_EQ("readNode: Read node 100 of size 96 bytes from table 1 | "
                "readNode: Read node 107 of size 66 bytes from table 1 | "
                "readNode: Read node 101 of size 58 bytes from table 1 | "
                "readNode: Read node 104 of size 58 bytes from table 1 | "
                "readNode: Read node 102 of size 50 bytes from table 1 | "
                "readNode: Read node 103 of size 26 bytes from table 1",
                TestLog::get());

    nodesReadBefore = sk->nodesRead;
    TestLog::reset();

    // This time, search for d|14 with an exclusive begin.
    nextNode = NULL;
    exactFound = sk->findLower({"d", 2, 14}, result, &buffer, false, &nextNode);
    EXPECT_EQ(5U, sk->nodesRead - nodesReadBefore); // head, e, a, d, e
    EXPECT_EQ(HEAD_NODEID, result[5].second);
    EXPECT_EQ(14U, result[4].first->pkHash);
    EXPECT_EQ(14U, result[3].first->pkHash);
    EXPECT_EQ(14U, result[2].first->pkHash);
    EXPECT_EQ(14U, result[1].first->pkHash);
    EXPECT_EQ(14U, result[0].first->pkHash);
    ASSERT_TRUE(exactFound);
    ASSERT_NE(nullNode, nextNode);
    EXPECT_EQ(15U, nextNode->pkHash);
    EXPECT_EQ("readNode: Read node 100 of size 96 bytes from table 1 | "
                "readNode: Read node 107 of size 66 bytes from table 1 | "
                "readNode: Read node 101 of size 58 bytes from table 1 | "
                "readNode: Read node 104 of size 58 bytes from table 1 | "
                "readNode: Read node 105 of size 50 bytes from table 1",
                TestLog::get());

    TestLog::reset();
    nodesReadBefore = sk->nodesRead;

    // Find something that doesn't have an exact match, but we request for
    // beginInclusive
    nextNode = NULL;
    exactFound = sk->findLower({"a", 2, 16}, result, &buffer, true, &nextNode);

    EXPECT_EQ(5U, sk->nodesRead - nodesReadBefore); // head, e, a, d, b
    EXPECT_EQ(HEAD_NODEID, result[5].second);
    EXPECT_EQ(11U, result[4].first->pkHash);
    EXPECT_EQ(11U, result[3].first->pkHash);
    EXPECT_EQ(11U, result[2].first->pkHash);
    EXPECT_EQ(11U, result[1].first->pkHash);
    EXPECT_EQ(11U, result[0].first->pkHash);
    EXPECT_FALSE(exactFound);
    EXPECT_EQ(12U, nextNode->pkHash);
    EXPECT_EQ("readNode: Read node 100 of size 96 bytes from table 1 | "
            "readNode: Read node 107 of size 66 bytes from table 1 | "
            "readNode: Read node 101 of size 58 bytes from table 1 | "
            "readNode: Read node 104 of size 58 bytes from table 1 | "
            "readNode: Read node 102 of size 50 bytes from table 1",
                TestLog::get());

    TestLog::reset();
    nodesReadBefore = sk->nodesRead;

    // Find something that doesn't have an exact match, but we request for
    // beginInclusive = false
    nextNode = NULL;
    exactFound = sk->findLower({"a", 2, 16}, result, &buffer, false, &nextNode);

    EXPECT_EQ(5U, sk->nodesRead - nodesReadBefore); // head, e, a, d, b
    EXPECT_EQ(HEAD_NODEID, result[5].second);
    EXPECT_EQ(11U, result[4].first->pkHash);
    EXPECT_EQ(11U, result[3].first->pkHash);
    EXPECT_EQ(11U, result[2].first->pkHash);
    EXPECT_EQ(11U, result[1].first->pkHash);
    EXPECT_EQ(11U, result[0].first->pkHash);
    EXPECT_FALSE(exactFound);
    EXPECT_EQ(12U, nextNode->pkHash);
    EXPECT_EQ("readNode: Read node 100 of size 96 bytes from table 1 | "
            "readNode: Read node 107 of size 66 bytes from table 1 | "
            "readNode: Read node 101 of size 58 bytes from table 1 | "
            "readNode: Read node 104 of size 58 bytes from table 1 | "
            "readNode: Read node 102 of size 50 bytes from table 1",
                TestLog::get());

    TestLog::reset();
    nodesReadBefore = sk->nodesRead;

    // Create a tree that looks like this
    // ----------------------------------------------> e|17
    // a|11 -----------------> d|14 -----------------> e|17
    // a|11 -> b|12 ---------> d|14 -> e|15 ---------> e|17
    // a|11 -> b|12 ---------> d|14 -> e|15 ---------> e|17
    // a|11 -> b|12 ---------> d|14 -> e|15 -> e|16 -> e|17
    // a|11 -> b|12 -> c|13 -> d|14 -> e|15 -> e|16 -> e|17


}

TEST_F(SkipListTest, remove) {
    // Create a tree that looks like this
    // ----------------------> d|14
    // a|11 -----------------> d|14
    // a|11 -> b|12 ---------> d|14
    // a|11 -> b|12 -> c|13 -> d|14
    // And try various removes.
    static_assert(MAX_FORWARD_POINTERS >= 4,
            "This test requires 4 or more levels in the SkipList");

    sk->insert({"a", 2, 11}, 3);
    sk->insert({"b", 2, 12}, 2);
    sk->insert({"c", 2, 13}, 1);
    sk->insert({"d", 2, 14}, 4);

    // Remove invalid from ends and in between
    uint64_t nodesWrittenBeforeInvalidDeletes = sk->nodesWritten;
    sk->remove({"1", 2, 0});
    sk->remove({"a", 2, 10});
    sk->remove({"zzz", 4, 1});
    sk->remove({"d", 2, 13});
    sk->remove({"d", 2, 15});
    sk->remove({"a", 2, 12});
    EXPECT_EQ(0U, sk->nodesWritten - nodesWrittenBeforeInvalidDeletes);

    // Validate the structure still exists.
    Buffer *b = &buffer;
    SkipList::Node *curr = NULL;
    curr = sk->readNode(HEAD_NODEID, b);

    // For a|11
    ASSERT_NE(INVALID_NODEID, curr->next()[0]);
    SkipList::NodeId aid = curr->next()[0];
    curr = sk->readNode(aid, b);
    EXPECT_EQ(11U, curr->pkHash);

    // for b|12
    ASSERT_NE(INVALID_NODEID, curr->next()[0]);
    SkipList::NodeId bid = curr->next()[0];
    curr = sk->readNode(bid, b);
    EXPECT_EQ(12U, curr->pkHash);

    // for c|13
    ASSERT_NE(INVALID_NODEID, curr->next()[0]);
    SkipList::NodeId cid = curr->next()[0];
    curr = sk->readNode(cid, b);
    EXPECT_EQ(13U, curr->pkHash);

    // for d|14
    ASSERT_NE(INVALID_NODEID, curr->next()[0]);
    SkipList::NodeId did = curr->next()[0];
    curr = sk->readNode(did, b);
    EXPECT_EQ(14U, curr->pkHash);

    EXPECT_EQ(INVALID_NODEID, curr->next()[0]);
    b->reset();

    // Now let's remove some inner nodes and see if they properly permute
    // the structure
    sk->remove({"b", 2, 12});

    curr = sk->readNode(HEAD_NODEID, b);
    ASSERT_EQ(10U, curr->levels);
    EXPECT_EQ(INVALID_NODEID, curr->next()[4]);
    EXPECT_EQ(did, curr->next()[3]);
    EXPECT_EQ(aid, curr->next()[2]);
    EXPECT_EQ(aid, curr->next()[1]);
    EXPECT_EQ(aid, curr->next()[0]);

    curr = sk->readNode(aid, b);
    ASSERT_EQ(3U, curr->levels);
    EXPECT_EQ(did, curr->next()[2]);
    EXPECT_EQ(did, curr->next()[1]);
    EXPECT_EQ(cid, curr->next()[0]);

    curr = sk->readNode(cid, b);
    ASSERT_EQ(1U, curr->levels);
    EXPECT_EQ(did, curr->next()[0]);


    // Add b back in and remove the front.
    bid = sk->nextNodeId;
    sk->insert({"b", 2, 12}, 2);
    sk->remove({"a", 2, 11});

    curr = sk->readNode(HEAD_NODEID, b);
    ASSERT_EQ(10U, curr->levels);
    EXPECT_EQ(INVALID_NODEID, curr->next()[4]);
    EXPECT_EQ(did, curr->next()[3]);
    ASSERT_EQ(did, curr->next()[2]);
    ASSERT_EQ(bid, curr->next()[1]);
    ASSERT_EQ(bid, curr->next()[0]);

    curr = sk->readNode(bid, b);
    ASSERT_EQ(2U, curr->levels);
    EXPECT_EQ(did, curr->next()[1]);
    EXPECT_EQ(cid, curr->next()[0]);

    // Remove the last node in addition to the first.
    sk->remove({"d", 2, 14});

    curr = sk->readNode(HEAD_NODEID, b);
    ASSERT_EQ(10U, curr->levels);
    EXPECT_EQ(INVALID_NODEID, curr->next()[4]);
    EXPECT_EQ(INVALID_NODEID, curr->next()[3]);
    EXPECT_EQ(INVALID_NODEID, curr->next()[2]);
    ASSERT_EQ(bid, curr->next()[1]);
    ASSERT_EQ(bid, curr->next()[0]);

    curr = sk->readNode(bid, b);
    ASSERT_EQ(2U, curr->levels);
    EXPECT_EQ(INVALID_NODEID, curr->next()[1]);
    ASSERT_EQ(cid, curr->next()[0]);

    curr = sk->readNode(cid, b);
    ASSERT_EQ(1U, curr->levels);
    EXPECT_EQ(INVALID_NODEID, curr->next()[0]);
}

TEST_F(SkipListTest, remove_efficiency) {
    // This test ensures that the interior for loop in remove() runs and
    // avoids superfluous node reads and writes. In theory, there should be
    // no reads other than the ones in findLower() and there should be as
    // many writes as unique nodes pointing to the node to be removed.

    // Create a tree that looks like this
    // ----------------------------------------------> e|17
    // a|11 -----------------> d|14 -----------------> e|17
    // a|11 -> b|12 ---------> d|14 -----------------> e|17
    // a|11 -> b|12 ---------> d|14 -> e|15 ---------> e|17
    // a|11 -> b|12 ---------> d|14 -> e|15 -> e|16 -> e|17
    // a|11 -> b|12 -> c|13 -> d|14 -> e|15 -> e|16 -> e|17
    static_assert(MAX_FORWARD_POINTERS >= 6,
            "This test requires 6 or more levels in the skiplist");

    sk->insert({"a", 2, 11}, 5);
    sk->insert({"b", 2, 12}, 4);
    sk->insert({"c", 2, 13}, 1);
    sk->insert({"d", 2, 14}, 5);
    sk->insert({"e", 2, 15}, 3);
    sk->insert({"e", 2, 16}, 2);
    sk->insert({"e", 2, 17}, 6);

    Buffer b;
    std::pair<SkipList::Node*, SkipList::NodeId> results[MAX_FORWARD_POINTERS];
    // Should be HEAD, e|17, a|11, d|14, b|12, c|13
    uint64_t nodeReadsBeforeFindLower = sk->nodesRead;
    sk->findLower({"d", 2, 14}, results, &b, true, NULL);
    uint64_t readsBeforeFindLower = sk->nodesRead - nodeReadsBeforeFindLower;

    uint64_t nodeReadsBeforeDelete = sk->nodesRead;
    uint64_t nodeWritesBeforeDelete = sk->nodesWritten;
    sk->remove({"d", 2, 14});
    EXPECT_EQ(0U, sk->nodesRead - nodeReadsBeforeDelete - readsBeforeFindLower);
    EXPECT_EQ(3U, sk->nodesWritten - nodeWritesBeforeDelete); //a|11, b|12, c|13

    // Reinsert d|14 and try remove something invalid after c|13
    sk->insert({"d", 2, 14});

    nodeReadsBeforeFindLower = sk->nodesRead;
    sk->findLower({"c", 2, 14}, results, &b, true, NULL);
    readsBeforeFindLower = sk->nodesRead - nodeReadsBeforeFindLower;

    nodeReadsBeforeDelete = sk->nodesRead;
    nodeWritesBeforeDelete = sk->nodesWritten;
    sk->remove({"c", 2, 14});
    EXPECT_EQ(0U, sk->nodesRead - nodeReadsBeforeDelete - readsBeforeFindLower);
    EXPECT_EQ(0U, sk->nodesWritten - nodeWritesBeforeDelete);
}

TEST_F(SkipListTest, range_iterator_constructor) {
    sk->insert({"a", 2, 11}, 1);

    Buffer b;
    IndexEntry end = {"z", 2, 999};
    SkipList::Node* headNode = sk->readNode(HEAD_NODEID, &b);
    SkipList::range_iterator it(sk, headNode, end, false);

    EXPECT_NE(&end.key, &(it.end.key));
    EXPECT_EQ(end.keyLen, it.end.keyLen);
    EXPECT_EQ(end.keyLen + headNode->size(), it.buffer.size());

    // Now let's initialize the iterator to the first node and check that every
    // thing is copied over.
    SkipList::Node *firstNode = sk->readNode(headNode->next()[0], &b);
    SkipList::range_iterator it2(sk, firstNode, end, false);

    // Now let's intentionally corrupt our firstNode's data so that we can
    // check that the iterator has copied everything down and kept it safe.
    firstNode->pkHash = 100;
    memcpy(firstNode->key(), "g", 2);

    EXPECT_EQ(sk, it.sk);
    EXPECT_STREQ("z", static_cast<const char*>(it.end.key));
    EXPECT_EQ(2U, it.end.keyLen);
    EXPECT_EQ(999U, it.end.pkHash);

    EXPECT_NE(firstNode, it2.curr);
    EXPECT_EQ(11U, it2.curr->pkHash);
    EXPECT_STREQ("a", static_cast<const char*>(it2.currEntry.key));

    // Construct with a null curr
    SkipList::range_iterator it3(sk, NULL, end, false);
    EXPECT_EQ(sk, it3.sk);
    EXPECT_EQ(end.keyLen, it3.end.keyLen);
    EXPECT_EQ(end.keyLen, it3.buffer.size());
}

TEST_F(SkipListTest, copyConstructor) {
    sk->insert({"a", 2, 11}, 5);
    Buffer b;
    IndexEntry end = {"z", 2, 999};
    SkipList::Node* headNode = sk->readNode(HEAD_NODEID, &b);
    SkipList::Node *firstNode = sk->readNode(headNode->next()[0], &b);

    SkipList::range_iterator it(sk, firstNode, end, false);
    SkipList::range_iterator assign;

    TestLog::Enable _;
    assign = it;

    // Clear out original and make sure everything is still valid.
    bzero(it.buffer.getStart<char>(), it.buffer.size());
    bzero(&it, sizeof(it));

    EXPECT_EQ(sk, assign.sk);
    EXPECT_STREQ("z", static_cast<const char*>(assign.end.key));
    EXPECT_EQ(2U, assign.end.keyLen);
    EXPECT_EQ(999U, assign.end.pkHash);

    ASSERT_NE(nullNode, assign.curr);
    EXPECT_EQ(11U, assign.curr->pkHash);
    EXPECT_STREQ("a", assign.curr->key());
    EXPECT_STREQ("a", static_cast<const char*>(assign.currEntry.key));

    EXPECT_EQ(end.keyLen + firstNode->size(), assign.buffer.size());
    EXPECT_STREQ("operator=: Assignment Operator Invoked",
                    TestLog::get().c_str());

    TestLog::reset();
    SkipList::range_iterator copy(assign);

    // Clear out original and make sure everything is still valid.
    bzero(assign.buffer.getStart<char>(), assign.buffer.size());
    bzero(&assign, sizeof(assign));

    EXPECT_EQ(sk, copy.sk);
    EXPECT_STREQ("z", static_cast<const char*>(copy.end.key));
    EXPECT_EQ(2U, copy.end.keyLen);
    EXPECT_EQ(999U, copy.end.pkHash);

    ASSERT_NE(nullNode, copy.curr);
    EXPECT_EQ(11U, copy.curr->pkHash);
    EXPECT_STREQ("a", copy.curr->key());
    EXPECT_STREQ("a", static_cast<const char*>(copy.currEntry.key));

    EXPECT_EQ(end.keyLen + firstNode->size(), copy.buffer.size());
    EXPECT_STREQ("range_iterator: Copy Constructor Invoked",
        TestLog::get().c_str());
}

TEST_F(SkipListTest, incrementOperator) {
    // Create a tree that looks like this
    // ----------------------------------------------> e|17
    // a|11 -----------------> d|14 -----------------> e|17
    // a|11 -> b|12 ---------> d|14 -----------------> e|17
    // a|11 -> b|12 ---------> d|14 -> e|15 ---------> e|17
    // a|11 -> b|12 ---------> d|14 -> e|15 -> e|16 -> e|17
    // a|11 -> b|12 -> c|13 -> d|14 -> e|15 -> e|16 -> e|17
    static_assert(MAX_FORWARD_POINTERS >= 6,
            "This test requires 6 or more levels in the skiplist");

    sk->insert({"a", 2, 11}, 5);
    sk->insert({"b", 2, 12}, 4);
    sk->insert({"c", 2, 13}, 1);
    sk->insert({"d", 2, 14}, 5);
    sk->insert({"e", 2, 15}, 3);
    sk->insert({"e", 2, 16}, 2);
    sk->insert({"e", 2, 17}, 6);

    Buffer b;
    IndexEntry end = {"z", 2, 999};
    SkipList::Node* headNode = sk->readNode(HEAD_NODEID, &b);

    SkipList::range_iterator it(sk, headNode, end, false);
    ++it;

    // End Key + node + nextBuffer + key
    EXPECT_EQ(end.keyLen + sizeof32(SkipList::Node)
            + sizeof32(SkipList::NodeId)*5 + 2U, it.buffer.size());
    ASSERT_NE(nullNode, it.curr);
    EXPECT_STREQ("a", it.curr->key());
    EXPECT_EQ(2U, it.curr->keyLen);
    EXPECT_EQ(11U, it.curr->pkHash);
    EXPECT_EQ(5U, it.curr->levels);

    EXPECT_EQ(2U, it.currEntry.keyLen);
    EXPECT_EQ(11U, it.currEntry.pkHash);

    // From now on, only check the pk hashes
    ++it;
    EXPECT_EQ(end.keyLen + sizeof32(SkipList::Node)
            + sizeof32(SkipList::NodeId)*4 + 2U, it.buffer.size());
    EXPECT_EQ(12U, it.currEntry.pkHash);
    ++it;
    EXPECT_EQ(13U, it.currEntry.pkHash);
     ++it;
    EXPECT_EQ(14U, it.currEntry.pkHash);
    ++it;
    EXPECT_EQ(15U, it.currEntry.pkHash);
    ++it;
    EXPECT_EQ(16U, it.currEntry.pkHash);
    ++it;
    EXPECT_EQ(17U, it.currEntry.pkHash);

    // Should be at the end now
    ++it;
    EXPECT_EQ(nullNode, it.curr);
    EXPECT_EQ(NULL, it.currEntry.key);
    EXPECT_EQ(0U, it.currEntry.keyLen);


    // Incrementing should do no harm
    EXPECT_NO_THROW(++it);
    EXPECT_EQ(nullNode, it.curr);
    EXPECT_EQ(NULL, it.currEntry.key);
    EXPECT_EQ(0U, it.currEntry.keyLen);

    //Now let's try that again, but with an actual endKey that's in range.
    SkipList::range_iterator it2(sk, headNode, {"d", 2, 14} , false);
    ++it2;
    EXPECT_EQ(11U, it2.currEntry.pkHash);
    ++it2;
    EXPECT_EQ(12U, it2.currEntry.pkHash);
    ++it2;
    EXPECT_EQ(13U, it2.currEntry.pkHash);

    // Should be at 14 now, which is out of range which means it should be NULL
    ++it2;
    EXPECT_EQ(nullNode, it2.curr);
    EXPECT_EQ(0U, it2.currEntry.pkHash);

    // Again but with an endkey that's inclusive
    SkipList::range_iterator it3(sk, headNode, {"d", 2, 14} , true);
    ++it3;
    EXPECT_EQ(11U, it3.currEntry.pkHash);
    ++it3;
    EXPECT_EQ(12U, it3.currEntry.pkHash);
    ++it3;
    EXPECT_EQ(13U, it3.currEntry.pkHash);
    ++it3;
    EXPECT_EQ(14U, it3.currEntry.pkHash);
    // Now we should be at null
    ++it3;
    EXPECT_EQ(nullNode, it3.curr);
    EXPECT_EQ(0U, it3.currEntry.pkHash);

    // And one more time with end Keys that don't exist.
    SkipList::range_iterator it4(sk, headNode, {"d", 2, 15} , true);
    ++it4;
    EXPECT_EQ(11U, it4.currEntry.pkHash);
    ++it4;
    EXPECT_EQ(12U, it4.currEntry.pkHash);
    ++it4;
    EXPECT_EQ(13U, it4.currEntry.pkHash);
    ++it4;
    EXPECT_EQ(14U, it4.currEntry.pkHash);
    // Now we should be at null
    ++it4;
    EXPECT_EQ(nullNode, it4.curr);
    EXPECT_EQ(0U, it4.currEntry.pkHash);


    SkipList::range_iterator it5(sk, headNode, {"d", 2, 15} , false);
    ++it5;
    EXPECT_EQ(11U, it5.currEntry.pkHash);
    ++it5;
    EXPECT_EQ(12U, it5.currEntry.pkHash);
    ++it5;
    EXPECT_EQ(13U, it5.currEntry.pkHash);
    ++it5;
    EXPECT_EQ(14U, it5.currEntry.pkHash);
    // Now we should be at null
    ++it5;
    EXPECT_EQ(nullNode, it5.curr);
    EXPECT_EQ(0U, it5.currEntry.pkHash);


    // And two final ones for one that's waayyy before the range.
    SkipList::range_iterator it6(sk, headNode, {"0", 2, 15} , false);
    ++it6;
    EXPECT_EQ(nullNode, it6.curr);
    EXPECT_EQ(0U, it6.currEntry.pkHash);

    SkipList::range_iterator it7(sk, headNode, {"0", 2, 15} , true);
    ++it7;
    EXPECT_EQ(nullNode, it7.curr);
    EXPECT_EQ(0U, it7.currEntry.pkHash);
}

TEST_F(SkipListTest, findRange) {
    // First, easy, find range on an empty SkipList
    SkipList::range_iterator it;
    sk->findRange({"a", 2, 2}, {"z", 2, 2}, &it, true, true);

    EXPECT_EQ(nullNode, it.curr);
    const char *nullString = static_cast<const char*>(NULL);
    EXPECT_EQ(nullString, it->key);
    EXPECT_EQ(0U, it->keyLen);
    EXPECT_EQ(0U, it->pkHash);

    // Create a tree that looks like this
    // ----------------------------------------------> e|17
    // a|11 -----------------> d|14 -----------------> e|17
    // a|11 -> b|12 ---------> d|14 -----------------> e|17
    // a|11 -> b|12 ---------> d|14 -> e|15 ---------> e|17
    // a|11 -> b|12 ---------> d|14 -> e|15 -> e|16 -> e|17
    // a|11 -> b|12 -> c|13 -> d|14 -> e|15 -> e|16 -> e|17
    static_assert(MAX_FORWARD_POINTERS >= 6,
            "This test requires 6 or more levels in the skiplist");

    sk->insert({"a", 2, 11}, 5);
    sk->insert({"b", 2, 12}, 4);
    sk->insert({"c", 2, 13}, 1);
    sk->insert({"d", 2, 14}, 5);
    sk->insert({"e", 2, 15}, 3);
    sk->insert({"e", 2, 16}, 2);
    sk->insert({"e", 2, 17}, 6);

    // Now find range on the whole thing.
    sk->findRange({"0", 2, 0}, {"z", 2, 0}, &it, true, true);
    EXPECT_EQ(11U, it.currEntry.pkHash);

    for (uint64_t expectPK = 11; expectPK <= 17; ++expectPK) {
        EXPECT_EQ(expectPK, it->pkHash);
        ++it;
    }
    EXPECT_EQ(nullNode, it.curr);
    EXPECT_EQ(0U, it->keyLen);

    // Something that starts before the begin and ends somewhere in the middle
    sk->findRange({"0", 2, 0}, {"d", 2, 14}, &it, true, true);
    EXPECT_EQ(11U, it.currEntry.pkHash);

    for (uint64_t expectPK = 11; expectPK <= 14; ++expectPK) {
        EXPECT_EQ(expectPK, it->pkHash);
        ++it;
    }
    EXPECT_EQ(nullNode, it.curr);
    EXPECT_EQ(0U, it->keyLen);

    // Something that starts before the begin and ends somewhere in the middle
    sk->findRange({"0", 2, 0}, {"d", 2, 14}, &it, true, false);
    EXPECT_EQ(11U, it.currEntry.pkHash);

    for (uint64_t expectPK = 11; expectPK <= 13; ++expectPK) {
        EXPECT_EQ(expectPK, it->pkHash);
        ++it;
    }
    EXPECT_EQ(nullNode, it.curr);
    EXPECT_EQ(0U, it->keyLen);

    // Something that starts in the middle and ends far away
    sk->findRange({"c", 2, 13}, {"z", 2, 0}, &it, true, false);
    EXPECT_EQ(13U, it.currEntry.pkHash);

    for (uint64_t expectPK = 13; expectPK <= 17; ++expectPK) {
        EXPECT_EQ(expectPK, it->pkHash);
        ++it;
    }
    EXPECT_EQ(nullNode, it.curr);
    EXPECT_EQ(0U, it->keyLen);

    sk->findRange({"c", 2, 13}, {"z", 2, 0}, &it, false, false);
    EXPECT_EQ(14U, it.currEntry.pkHash);

    for (uint64_t expectPK = 14; expectPK <= 17; ++expectPK) {
        EXPECT_EQ(expectPK, it->pkHash);
        ++it;
    }
    EXPECT_EQ(nullNode, it.curr);
    EXPECT_EQ(0U, it->keyLen);


    // one that is in range.
    sk->findRange({"c", 2, 13}, {"e", 2, 15}, &it, true, true);
    EXPECT_EQ(13U, it.currEntry.pkHash);

    for (uint64_t expectPK = 13; expectPK <= 15; ++expectPK) {
        EXPECT_EQ(expectPK, it->pkHash);
        ++it;
    }
    EXPECT_EQ(nullNode, it.curr);
    EXPECT_EQ(0U, it->keyLen);

    // One where the range is so small it encompasses no elements.
    sk->findRange({"d", 2, 15}, {"e", 2, 14}, &it, true, true);
    EXPECT_EQ(nullNode, it.curr);
    EXPECT_EQ(0U, it->pkHash);
    ++it;
    EXPECT_EQ(nullNode, it.curr);
    EXPECT_EQ(0U, it->keyLen);

    // One where both the beg/end are far beyond the end
    sk->findRange({"z", 2, 15}, {"z", 2, 16}, &it, true, true);

    EXPECT_EQ(nullNode, it.curr);
    EXPECT_EQ(0U, it->pkHash);
    ++it;
    EXPECT_EQ(nullNode, it.curr);
    EXPECT_EQ(0U, it->keyLen);

    // One where both the beg/end are far beyond the front
    sk->findRange({"0", 2, 15}, {"0", 2, 16}, &it, true, true);

    EXPECT_EQ(nullNode, it.curr);
    EXPECT_EQ(0U, it->pkHash);
    ++it;
    EXPECT_EQ(nullNode, it.curr);
    EXPECT_EQ(0U, it->keyLen);
}

} // namespace RAMCloud
