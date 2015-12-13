/* Copyright (c) 2014-2015 Stanford University
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
#include "ObjectManager.h"

#include <gtest/gtest.h>
#include "btreeRamCloud/Btree.h"
#include "PreparedOps.h"
#include "RamCloud.h"
#include "TxRecoveryManager.h"
#include "UnackedRpcResults.h"
#include "Btree.h"

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <vector>
#include <set>
#include <sstream>
#include <iostream>

namespace RAMCloud {

class BtreeTest: public ::testing::Test {
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
    ObjectManager objectManager;
    uint64_t tableId;

    BtreeTest()
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
        , objectManager(&context,
                        &serverId,
                        &masterConfig,
                        &tabletManager,
                        &masterTableMetadata,
                        &unackedRpcResults,
                        &preparedOps,
                        &txRecoveryManager)
        , tableId(1)
    {
        objectManager.initOnceEnlisted();
        tabletManager.addTablet(1, 0, ~0UL, TabletManager::NORMAL);
        unackedRpcResults.resetFreer(&objectManager);
    }

    DISALLOW_COPY_AND_ASSIGN(BtreeTest);
};

void generateKeysInRange(int start, int end,
        std::vector<std::string> &keys,
        std::vector<BtreeEntry> &entries,
        int leadingZeros = 0)
{
    char buff[1000];
    for (int i = start; i < end; i++) {
        snprintf(buff, 1000, "%0*u", leadingZeros, i);
        keys.emplace_back(buff);
        entries.emplace_back(keys[i].c_str(), i);
    }
}

TEST_F(BtreeTest, node_getAt_setAt) {
    Buffer buffer1;
    IndexBtree::LeafNode *n = buffer1.emplaceAppend<IndexBtree::LeafNode>(&buffer1);
    EXPECT_EQ(buffer1.size(), n->keysBeginOffset);
    ASSERT_TRUE(n != NULL);

    const char *str1 = "OneOneOne";
    const char *str2 = "TwoTwo";
    const char *str3 = "ThreeThreeThree";
    const char *str4 = "FourFourFour";
    BtreeEntry entry1 = { str1, 111};
    BtreeEntry entry2 = { str2, 22};
    BtreeEntry entry3 = { str3, 333};
    BtreeEntry entry4 = { str4, 444};
    BtreeEntry entries[] = {entry1, entry2, entry3, entry4};
    BtreeEntry returnEntry;

    // Try One and Check
    n->setAt(0, entries[2]);
    EXPECT_EQ(1U, n->slotuse);
    returnEntry = n->getAt(0);
    ASSERT_EQ(entry3.keyLength, returnEntry.keyLength);
    EXPECT_EQ(entry3.pKHash, returnEntry.pKHash);
    EXPECT_EQ(0, memcmp(entry3.key, returnEntry.key, entry3.keyLength));

    // Scrambling Inserts
    n->setAt(1, entries[3]);
    n->setAt(2, entries[0]);

    // Set them to their correct values and check
    n->setAt(2, entries[2]);
    n->setAt(1, entries[1]);
    n->setAt(0, entries[0]);
    n->setAt(3, entries[3]);

    EXPECT_EQ(4U, n->slotuse);
    EXPECT_EQ(strlen(str1) + strlen(str2) + strlen(str3) + strlen(str4),
              n->keyStorageUsed);
    for (uint16_t i = 0; i < sizeof(entries)/sizeof(BtreeEntry); i++) {
        returnEntry = n->getAt(i);
        EXPECT_EQ(entries[i].keyLength, returnEntry.keyLength);
        EXPECT_EQ(entries[i].pKHash, returnEntry.pKHash);
        EXPECT_STREQ(
                string((const char*)entries[i].key, entries[i].keyLength).c_str(),
                string((const char*)returnEntry.key, returnEntry.keyLength).c_str());
    }
}

TEST_F(BtreeTest, getAt_keyOutBuffer) {
    Buffer b;
    BtreeEntry e1 = {"Yellow Car", 1};
    BtreeEntry e2 = {"Blue Submarine", 2222};
    IndexBtree::LeafNode *n = b.emplaceAppend<IndexBtree::LeafNode>(&b);

    n->insertAt(0, e1);
    n->insertAt(1, e2);

    EXPECT_EQ(e1, n->getAt(0));
    EXPECT_EQ(e2, n->getAt(1));

    // Test Destructability
    Buffer keyOut;
    BtreeEntry e1o, e2o;
    e1o = n->getAt(0, &keyOut);
    e2o = n->getAt(1, &keyOut);

    //Destroy original node and overwrite data
    b.reset();
    n = b.emplaceAppend<IndexBtree::LeafNode>(&b);
    n->insertAt(0, {"saod;giljwteo;ijafalkasd93kf-3", 911});
    n->insertAt(0, {"ouf099(((!!!!!!", 2920903950 });

    EXPECT_EQ(e1, e1o);
    EXPECT_EQ(e2, e2o);

}

TEST_F(BtreeTest, back) {
    Buffer b;
    BtreeEntry e1, e2;
    IndexBtree::LeafNode *n = b.emplaceAppend<IndexBtree::LeafNode>(&b);

    // Set 1 and check
    e1 = {"Hello", 1};
    n->setAt(0, e1);
    EXPECT_EQ(e1, n->back());

    // Set new last element
    e2 = {"YOLO", 1337};
    n->setAt(1, e2);
    EXPECT_EQ(e2, n->back());

    // Don't change last element
    n->insertAt(0, e1);
    EXPECT_EQ(e2, n->back());

    // remove last element
    n->pop_back();
    EXPECT_EQ(e1, n->back());
}

TEST_F(BtreeTest, pop_back) {
    Buffer b;
    BtreeEntry e1 = {"Yellow Car", 1};
    BtreeEntry e2 = {"Blue Submarine", 2222};
    IndexBtree::LeafNode *n = b.emplaceAppend<IndexBtree::LeafNode>(&b);

    n->insertAt(0, e1);
    n->insertAt(1, e2);

    n->pop_back();
    EXPECT_EQ(e1.keyLength, n->keyStorageUsed); // since only e1 is left
    EXPECT_EQ(1U, n->slotuse);
    EXPECT_EQ(e1, n->getAt(0));

    n->pop_back();
    EXPECT_EQ(0U, n->keyStorageUsed);
    EXPECT_EQ(0U, n->slotuse);
}

TEST_F(BtreeTest, pop_back_n) {
    Buffer b;
    BtreeEntry e1 = {"Yellow Car", 1};
    BtreeEntry e2 = {"Blue Submarine", 2222};
    BtreeEntry e3 = {"Ankita Kejirwal", 3333};
    BtreeEntry e4 = {"Diego Ongaro", 4444 };
    IndexBtree::LeafNode *n = b.emplaceAppend<IndexBtree::LeafNode>(&b);

    n->insertAt(0, e1);
    n->insertAt(1, e2);
    n->insertAt(2, e3);
    n->insertAt(3, e4);

    n->pop_back(1);
    EXPECT_EQ(3U, n->slotuse);
    EXPECT_EQ(e3, n->back());

    n->pop_back(2);
    EXPECT_EQ(e1.keyLength, n->keyStorageUsed); // since only e1 is left
    EXPECT_EQ(1U, n->slotuse);
    EXPECT_EQ(e1, n->back());

    n->pop_back(1);
    EXPECT_EQ(0U, n->slotuse);
    EXPECT_EQ(0U, n->keyStorageUsed);
}

static void fillNodeSorted(IndexBtree::LeafNode *n) {
  BtreeEntry entry0 = {"AaaaDFlkjasdf", 00};
  BtreeEntry entry1 = {"bbb0aDFlkjasdasdfasdfasdfasdfsafasdff", 11};
  BtreeEntry entry2 = {"bbbcaDFlasdf", 22};
  BtreeEntry entry3 = {"zzcccsdf", 33};

  n->setAt(0, entry0);
  n->setAt(1, entry1);
  n->setAt(2, entry2);
  n->setAt(3, entry3);
}

static void checkNodeEquals(const IndexBtree::LeafNode *orig,
                            const IndexBtree::LeafNode *cpy) {
  EXPECT_EQ(orig->level, cpy->level);
  EXPECT_EQ(orig->slotuse, cpy->slotuse);
  EXPECT_EQ(orig->keyStorageUsed, cpy->keyStorageUsed);

  for (uint16_t i = 0; i < orig->slotuse; i++) {
    BtreeEntry origEntry = orig->getAt(i);
    BtreeEntry cpyEntry = cpy->getAt(i);
    EXPECT_EQ(origEntry.keyLength, cpyEntry.keyLength);
    EXPECT_EQ(origEntry.pKHash, cpyEntry.pKHash);
    EXPECT_EQ(0, memcmp(origEntry.key, cpyEntry.key, origEntry.keyLength));
  }
}

TEST_F(BtreeTest, node_serializeToPreallocatedBuffer) {
    Buffer b;
    b.alloc(100); // Just to give us an offset.
    IndexBtree::LeafNode node(&b);

    char keys[1224];
    node.setAt(0, {keys, 100, 1});
    node.setAt(1, {keys + 100, 1024, 2});
    node.setAt(2, {keys + 1124, 100, 3});

    Buffer toBuffer;
    toBuffer.alloc(20); // Just to give us an offset.
    toBuffer.alloc(node.serializedLength());
    uint32_t bytesWritten = node.serializeToPreallocatedBuffer(&toBuffer, 20U);

    EXPECT_EQ(node.serializedLength(), bytesWritten);
    EXPECT_EQ(node.serializedLength() + 20U, toBuffer.size());
    EXPECT_EQ(2U, toBuffer.getNumberChunks());
    IndexBtree::LeafNode *readBack = static_cast<IndexBtree::LeafNode*>(
                                                toBuffer.getRange(20, 1224));

    BtreeEntry entry = readBack->getAt(0);
    EXPECT_EQ(100U, entry.keyLength);
    EXPECT_EQ(0, bcmp(keys, entry.key, 100));

    entry = readBack->getAt(1);
    EXPECT_EQ(1024U, entry.keyLength);
    EXPECT_EQ(0, bcmp(keys + 100, entry.key, 1024));

    entry = readBack->getAt(2);
    EXPECT_EQ(100U, entry.keyLength);
    EXPECT_EQ(0, bcmp(keys + 1124, entry.key, 100));
}

TEST_F(BtreeTest, node_serializeAppendToBuffer) {
  Buffer objBuffer, keyBuffer, out;
  IndexBtree::LeafNode *contig, *notContig, *copy;

  // Contiguous
  contig = objBuffer.emplaceAppend<IndexBtree::LeafNode>(&objBuffer);
  fillNodeSorted(contig);
  copy = static_cast<IndexBtree::LeafNode*>(contig->serializeAppendToBuffer(&out));
  checkNodeEquals(contig, copy);

  //Check that modifications are independent.
  contig->insertAt(0, {"blah blah bla", 29});
  EXPECT_EQ(copy->slotuse + 1U, contig->slotuse);
  EXPECT_NE(copy->getAt(0), contig->getAt(0));

  // Discontiguous via 2 buffers
  objBuffer.reset();
  out.reset();
  notContig = objBuffer.emplaceAppend<IndexBtree::LeafNode>(&keyBuffer);
  fillNodeSorted(notContig);
  copy = static_cast<IndexBtree::LeafNode*>(notContig->serializeAppendToBuffer(&out));
  checkNodeEquals(notContig, copy);

  //Check that modifications are independent.
  notContig->insertAt(0, {"blah blah bla", 29});
  EXPECT_EQ(copy->slotuse + 1U, notContig->slotuse);
  EXPECT_NE(copy->getAt(0), notContig->getAt(0));

  // Discontguous via stack allocation
  objBuffer.reset();
  keyBuffer.reset();
  out.reset();
  IndexBtree::LeafNode stack(&keyBuffer);
  fillNodeSorted(&stack);
  copy = static_cast<IndexBtree::LeafNode*>(stack.serializeAppendToBuffer(&out));
  checkNodeEquals(&stack, copy);

  //Check that modifications are independent.
  stack.insertAt(0, {"blah blah bla", 29});
  EXPECT_EQ(copy->slotuse + 1U, stack.slotuse);
  EXPECT_NE(copy->getAt(0), stack.getAt(0));
}

TEST_F(BtreeTest, serializedLength) {
    Buffer nodeBuffer, outBuffer;
    IndexBtree::InnerNode *inner = nodeBuffer.emplaceAppend<IndexBtree::InnerNode>(&nodeBuffer, uint16_t(10));
    IndexBtree::LeafNode *leaf= nodeBuffer.emplaceAppend<IndexBtree::LeafNode>(&nodeBuffer);
    BtreeEntry dummy = {"Dummy", 911};

    inner->insertAt(0, dummy, 10U, 11U);
    leaf->insertAt(0, dummy);
    leaf->insertAt(0, dummy);

    outBuffer.reset();
    inner->serializeAppendToBuffer(&outBuffer);
    EXPECT_EQ(outBuffer.size(), inner->serializedLength());

    outBuffer.reset();
    leaf->serializeAppendToBuffer(&outBuffer);
    EXPECT_EQ(outBuffer.size(), leaf->serializedLength());
}

TEST_F(BtreeTest, node_toString_printToLog) {
    Buffer objBuffer;
    IndexBtree::LeafNode *n = objBuffer.emplaceAppend<IndexBtree::LeafNode>(&objBuffer);

    std::vector<std::string> keys;
    std::vector<BtreeEntry> entries;
    generateKeysInRange(0, IndexBtree::innerslotmax/2, keys, entries);

    for (uint16_t i = 0; i < 4; ++i)
        n->insertAt(i, entries[i]);

    EXPECT_STREQ("Printing leaf node\r\n"
                    "1 <- prev | next -> 1\r\n"
                    " ( pKHash: 0 keyLength: 1 key: 0 )\r\n"
                    " ( pKHash: 1 keyLength: 1 key: 1 )\r\n"
                    " ( pKHash: 2 keyLength: 1 key: 2 )\r\n"
                    " ( pKHash: 3 keyLength: 1 key: 3 )\r\n"
                    , n->toString().c_str());


    TestLog::Enable _;
    n->printToLog(NOTICE);
    EXPECT_STREQ("printToLog: Printing leaf node\r\n"
                    "1 <- prev | next -> 1\r\n"
                    " ( pKHash: 0 keyLength: 1 key: 0 )\r\n"
                    " ( pKHash: 1 keyLength: 1 key: 1 )\r\n"
                    " ( pKHash: 2 keyLength: 1 key: 2 )\r\n"
                    " ( pKHash: 3 keyLength: 1 key: 3 )\r\n"
                    , TestLog::get().c_str());
}

static void node_insertAtEntryOnlyTest(IndexBtree::LeafNode *n) {
  const char *str1 = "OneOneOne";
  const char *str2 = "TwoTwo";
  const char *str3 = "ThreeThreeThree";
  const char *str4 = "FourFourFour";
  BtreeEntry entry1 = { str1, 111};
  BtreeEntry entry2 = { str2, 22};
  BtreeEntry entry3 = { str3, 333};
  BtreeEntry entry4 = { str4, 444};
  BtreeEntry entries[] = {entry1, entry2, entry3, entry4};
  BtreeEntry returnEntry;

  // Try One and Check
  n->insertAt(0, entries[2]);
  EXPECT_EQ(1U, n->slotuse);
  returnEntry = n->getAt(0);
  ASSERT_EQ(entries[2].keyLength, returnEntry.keyLength);
  EXPECT_EQ(entries[2].pKHash, returnEntry.pKHash);
  EXPECT_EQ(0, memcmp(entries[2].key, returnEntry.key, entries[2].keyLength));

  n->insertAt(1, entries[3]); // insert at end
  n->insertAt(0, entries[0]); // insert at beginning
  n->insertAt(1, entries[1]); // insert in middle

  EXPECT_EQ(4U, n->slotuse);
  EXPECT_EQ(strlen(str1) + strlen(str2) + strlen(str3) + strlen(str4),
            n->keyStorageUsed);
  for (uint16_t i = 0; i < sizeof(entries)/sizeof(BtreeEntry); i++) {
    returnEntry = n->getAt(i);
    EXPECT_EQ(entries[i].keyLength, returnEntry.keyLength);
    EXPECT_EQ(entries[i].pKHash, returnEntry.pKHash);
    EXPECT_STREQ(
            string((const char*)entries[i].key, entries[i].keyLength).c_str(),
            string((const char*)returnEntry.key, returnEntry.keyLength).c_str());
  }
}

TEST_F(BtreeTest, node_insertAtEntryOnly) {
  Buffer objBuffer, keyBuffer;

  // Internal Buffer
  IndexBtree::LeafNode *n = objBuffer.emplaceAppend<IndexBtree::LeafNode>(&objBuffer);
  EXPECT_EQ(objBuffer.size(), n->keysBeginOffset);
  ASSERT_TRUE(n != NULL);
  node_insertAtEntryOnlyTest(n);

  objBuffer.reset();

  //External buffer + an already filled keyBuffer
  n = objBuffer.emplaceAppend<IndexBtree::LeafNode>(&keyBuffer);
  EXPECT_EQ(keyBuffer.size(), n->keysBeginOffset);
  ASSERT_TRUE(n != NULL);

  node_insertAtEntryOnlyTest(n);
}

TEST_F(BtreeTest, node_eraseAtEntryOnly) {
    std::vector<std::string> keys;
    std::vector<BtreeEntry> entries;
    generateKeysInRange(0, IndexBtree::innerslotmax, keys, entries);

    Buffer buffer;
    IndexBtree::InnerNode *n =
            buffer.emplaceAppend<IndexBtree::InnerNode>(&buffer, uint16_t(10));

    for (uint16_t i = 0; i < IndexBtree::innerslotmax; i++) {
        n->setAt(i, entries[i]);
    }

    // Erase in the middle and check everything is okay
    n->eraseAtEntryOnly(1);
    BtreeEntry toVerify = n->getAt((uint16_t)(0));
        EXPECT_EQ(0U, toVerify.pKHash);
        EXPECT_EQ(entries[0].keyLength, toVerify.keyLength);
        EXPECT_STREQ(keys[0].c_str(),
                 string((const char*)toVerify.key, toVerify.keyLength).c_str());

    for (uint16_t i = 2; i < IndexBtree::innerslotmax; i++) {
        BtreeEntry toVerify = n->getAt((uint16_t)(i - 1));
        EXPECT_EQ(i, toVerify.pKHash);
        EXPECT_EQ(entries[i].keyLength, toVerify.keyLength);
        EXPECT_STREQ(keys[i].c_str(),
                 string((const char*)toVerify.key, toVerify.keyLength).c_str());
    }

    // Erase at front and check everything is okay
    n->eraseAtEntryOnly(0);
    EXPECT_EQ(n->slotuse, IndexBtree::innerslotmax - 2);
    for (uint16_t i = 2; i < IndexBtree::innerslotmax; i++) {
        BtreeEntry toVerify = n->getAt((uint16_t)(i - 2));
        EXPECT_EQ(i, toVerify.pKHash);
        EXPECT_EQ(entries[i].keyLength, toVerify.keyLength);
        EXPECT_STREQ(keys[i].c_str(),
                 string((const char*)toVerify.key, toVerify.keyLength).c_str());
    }


    // Erase at end and check that everything is okay
    n->eraseAtEntryOnly(IndexBtree::innerslotmax - 3);
    EXPECT_EQ(n->slotuse, IndexBtree::innerslotmax - 3);
    for (uint16_t i = 2; i < IndexBtree::innerslotmax - 1; i++) {
        BtreeEntry toVerify = n->getAt((uint16_t)(i - 2));
        EXPECT_EQ(i, toVerify.pKHash);
        EXPECT_EQ(entries[i].keyLength, toVerify.keyLength);
        EXPECT_STREQ(keys[i].c_str(),
                 string((const char*)toVerify.key, toVerify.keyLength).c_str());
    }
}

TEST_F(BtreeTest, node_moveBackEntriesToFrontOf) {
    Buffer b1, b2;
    IndexBtree::LeafNode *n1, *n2;

    BtreeEntry entry0 = {"zero", 00};
    BtreeEntry entry1 = {"one", 11};
    BtreeEntry entry2 = {"two", 22};
    BtreeEntry entry3 = {"three", 33};
    BtreeEntry entry4 = {"four", 44};
    BtreeEntry entry5 = {"five", 55};
    BtreeEntry entry6 = {"six", 66};
    BtreeEntry entry7 = {"seven", 77};
    BtreeEntry entries[] = {entry0, entry1, entry2, entry3,
                            entry4, entry5, entry6, entry7};

    n1 = b1.emplaceAppend<IndexBtree::LeafNode>(&b1);
    n2 = b2.emplaceAppend<IndexBtree::LeafNode>(&b2);

    n1->setAt(0, entries[0]);
    n1->setAt(1, entries[1]);
    n1->setAt(2, entries[2]);
    n1->setAt(3, entries[3]);

    n2->setAt(0, entries[4]);
    n2->setAt(1, entries[5]);
    n2->setAt(2, entries[6]);
    n2->setAt(3, entries[7]);

    n1->moveBackEntriesToFrontOf(n2, 2);
    EXPECT_EQ(2, n1->slotuse);
    EXPECT_EQ(6, n2->slotuse);

    BtreeEntry query, correct;
    query = n1->getAt(0);
    correct = entries[0];
    EXPECT_EQ(correct.keyLength, query.keyLength);
    EXPECT_EQ(correct.pKHash, query.pKHash);
    EXPECT_STREQ((const char*)correct.key,
                    std::string((const char*)query.key, query.keyLength).c_str());

    query = n1->getAt(1);
    correct = entries[1];
    EXPECT_EQ(correct.keyLength, query.keyLength);
    EXPECT_EQ(correct.pKHash, query.pKHash);
    EXPECT_STREQ((const char*)correct.key,
                    std::string((const char*)query.key, query.keyLength).c_str());

    query = n2->getAt(0);
    correct = entries[2];
    EXPECT_EQ(correct.keyLength, query.keyLength);
    EXPECT_EQ(correct.pKHash, query.pKHash);
    EXPECT_STREQ((const char*)correct.key,
                    std::string((const char*)query.key, query.keyLength).c_str());

    query = n2->getAt(1);
    correct = entries[3];
    EXPECT_EQ(correct.keyLength, query.keyLength);
    EXPECT_EQ(correct.pKHash, query.pKHash);
    EXPECT_STREQ((const char*)correct.key,
                    std::string((const char*)query.key, query.keyLength).c_str());

    query = n2->getAt(2);
    correct = entries[4];
    EXPECT_EQ(correct.keyLength, query.keyLength);
    EXPECT_EQ(correct.pKHash, query.pKHash);
    EXPECT_STREQ((const char*)correct.key,
                    std::string((const char*)query.key, query.keyLength).c_str());

    query = n2->getAt(5);
    correct = entries[7];
    EXPECT_EQ(correct.keyLength, query.keyLength);
    EXPECT_EQ(correct.pKHash, query.pKHash);
    EXPECT_STREQ((const char*)correct.key,
                    std::string((const char*)query.key, query.keyLength).c_str());

    // Move the rest over
    n1->moveBackEntriesToFrontOf(n2, 2);
    EXPECT_EQ(0, n1->slotuse);
    EXPECT_EQ(8, n2->slotuse);

    for (uint16_t i = 0; i < n2->slotuse; i++) {
        query = n2->getAt(i);
        correct = entries[i];
        EXPECT_EQ(correct.keyLength, query.keyLength);
        EXPECT_EQ(correct.pKHash, query.pKHash);
        EXPECT_STREQ((const char*)correct.key,
            std::string((const char*)query.key, query.keyLength).c_str());
    }

    // Move back to n1
    n2->moveBackEntriesToFrontOf(n1, 8);
    EXPECT_EQ(8, n1->slotuse);
    EXPECT_EQ(0, n2->slotuse);

    for (uint16_t i = 0; i < n1->slotuse; i++) {
        query = n1->getAt(i);
        correct = entries[i];
        EXPECT_EQ(correct.keyLength, query.keyLength);
        EXPECT_EQ(correct.pKHash, query.pKHash);
        EXPECT_STREQ((const char*)correct.key,
            std::string((const char*)query.key, query.keyLength).c_str());
    }
}

TEST_F(BtreeTest, node_moveFrontEntriesToBackOf) {
    Buffer b1, b2;
    IndexBtree::LeafNode *n1, *n2;

    BtreeEntry entry0 = {"zero", 00};
    BtreeEntry entry1 = {"one", 11};
    BtreeEntry entry2 = {"two", 22};
    BtreeEntry entry3 = {"three", 33};
    BtreeEntry entry4 = {"four", 44};
    BtreeEntry entry5 = {"five", 55};
    BtreeEntry entry6 = {"six", 66};
    BtreeEntry entry7 = {"seven", 77};
    BtreeEntry entries[] = {entry0, entry1, entry2, entry3,
                            entry4, entry5, entry6, entry7};

    n1 = b1.emplaceAppend<IndexBtree::LeafNode>(&b1);
    n2 = b2.emplaceAppend<IndexBtree::LeafNode>(&b2);

    n1->setAt(0, entries[4]);
    n1->setAt(1, entries[5]);
    n1->setAt(2, entries[6]);
    n1->setAt(3, entries[7]);

    n2->setAt(0, entries[0]);
    n2->setAt(1, entries[1]);
    n2->setAt(2, entries[2]);
    n2->setAt(3, entries[3]);

    n1->moveFrontEntriesToBackOf(n2, 2);
    EXPECT_EQ(2, n1->slotuse);
    EXPECT_EQ(6, n2->slotuse);

    // Check end of n1
    BtreeEntry query, correct;
    query = n1->getAt(1);
    correct = entries[7];
    EXPECT_EQ(correct.keyLength, query.keyLength);
    EXPECT_EQ(correct.pKHash, query.pKHash);
    EXPECT_STREQ((const char*)correct.key,
                std::string((const char*)query.key, query.keyLength).c_str());


    // Check 2 old and 2 new entries in n2
    query = n2->getAt(0);
    correct = entries[0];
    EXPECT_EQ(correct.keyLength, query.keyLength);
    EXPECT_EQ(correct.pKHash, query.pKHash);
    EXPECT_STREQ((const char*)correct.key,
                std::string((const char*)query.key, query.keyLength).c_str());

    query = n2->getAt(3);
    correct = entries[3];
    EXPECT_EQ(correct.keyLength, query.keyLength);
    EXPECT_EQ(correct.pKHash, query.pKHash);
    EXPECT_STREQ((const char*)correct.key,
                std::string((const char*)query.key, query.keyLength).c_str());

    query = n2->getAt(4);
    correct = entries[4];
    EXPECT_EQ(correct.keyLength, query.keyLength);
    EXPECT_EQ(correct.pKHash, query.pKHash);
    EXPECT_STREQ((const char*)correct.key,
                std::string((const char*)query.key, query.keyLength).c_str());

    query = n2->getAt(5);
    correct = entries[5];
    EXPECT_EQ(correct.keyLength, query.keyLength);
    EXPECT_EQ(correct.pKHash, query.pKHash);
    EXPECT_STREQ((const char*)correct.key,
                std::string((const char*)query.key, query.keyLength).c_str());

    // Move the rest over
    n1->moveFrontEntriesToBackOf(n2, 2);
    EXPECT_EQ(0, n1->slotuse);
    EXPECT_EQ(8, n2->slotuse);

    for (uint16_t i = 0; i < n2->slotuse; i++) {
        query = n2->getAt(i);
        correct = entries[i];
        EXPECT_EQ(correct.keyLength, query.keyLength);
        EXPECT_EQ(correct.pKHash, query.pKHash);
        EXPECT_STREQ((const char*)correct.key,
            std::string((const char*)query.key, query.keyLength).c_str());
    }

    // Move back to n1
    n2->moveFrontEntriesToBackOf(n1, 8);
    EXPECT_EQ(8, n1->slotuse);
    EXPECT_EQ(0, n2->slotuse);

    for (uint16_t i = 0; i < n1->slotuse; i++) {
        query = n1->getAt(i);
        correct = entries[i];
        EXPECT_EQ(correct.keyLength, query.keyLength);
        EXPECT_EQ(correct.pKHash, query.pKHash);
        EXPECT_STREQ((const char*)correct.key,
            std::string((const char*)query.key, query.keyLength).c_str());
    }
}

TEST_F(BtreeTest, inner_node_setRightMostLeafKey) {
    Buffer buff;
    IndexBtree::InnerNode *n =
                buff.emplaceAppend<IndexBtree::InnerNode>(&buff, uint16_t(6));

    BtreeEntry query;
    BtreeEntry entry0 = {"AaaaDFlkjasdf", 00};
    BtreeEntry entry1 = {"bbb0aDFlkjasdasdfasdfasdfasdfsafasdff", 11};
    BtreeEntry entry2 = {"bbbcaDFlasdf", 22};
    BtreeEntry entry3 = {"zzcccsdf", 33};
    BtreeEntry rightMost1 = {"zzzzzzzzzJLKDJFj", 55};
    BtreeEntry rightMost2 = {"zzzzzzzzzzzzzJLKDJFj", 556};
    // BtreeEntry rightMost3 = {"zzzzzzzzzzzzzzzzzzLKDJFj", 755};

    // Try a bunch of operations to see if the key gets overwritten
    n->setRightMostLeafKey(rightMost1);
    n->insertAt(0, entry0, 0, 1);
    n->insertAt(1, entry1, 22, 333);
    uint32_t nodeSize = sizeof(IndexBtree::InnerNode);

    EXPECT_EQ(2U, n->slotuse);
    EXPECT_EQ(uint32_t(rightMost1.keyLength
                        + entry0.keyLength
                        + entry1.keyLength
                        + nodeSize), n->serializedLength());


    EXPECT_EQ(rightMost1, n->getRightMostLeafKey());
    EXPECT_EQ(entry0, n->getAt(0));
    EXPECT_EQ(entry1, n->getAt(1));

    n->pop_back();
    EXPECT_EQ(rightMost1, n->getRightMostLeafKey());
    EXPECT_EQ(entry0, n->getAt(0));
    EXPECT_EQ(uint32_t(rightMost1.keyLength
                        + entry0.keyLength
                        + nodeSize), n->serializedLength());

    n->setRightMostLeafKey(rightMost2);
    EXPECT_EQ(uint32_t(rightMost2.keyLength + entry0.keyLength + nodeSize),
                            n->serializedLength());
    EXPECT_EQ(rightMost2, n->getRightMostLeafKey());
    EXPECT_EQ(entry0, n->getAt(0));

    n->insertAt(1, entry2, 333, 4444);
    n->insertAt(2, entry3, 4444, 55555);

    EXPECT_EQ(uint32_t(rightMost2.keyLength + entry0.keyLength
                        + entry2.keyLength + entry3.keyLength + nodeSize),
                            n->serializedLength());
    EXPECT_EQ(rightMost2, n->getRightMostLeafKey());
    EXPECT_EQ(entry0, n->getAt(0));
    EXPECT_EQ(entry2, n->getAt(1));
    EXPECT_EQ(entry3, n->getAt(2));
}

TEST_F(BtreeTest, inner_node_insertAt) {
    Buffer objBuffer;
    IndexBtree::InnerNode *n =
        objBuffer.emplaceAppend<IndexBtree::InnerNode>(&objBuffer, uint16_t(12));

    const char *str1 = "OneOneOne";
    const char *str2 = "TwoTwo";
    const char *str3 = "ThreeThreeThree";
    const char *str4 = "FourFourFour";
    BtreeEntry entry1 = { str1, 111};
    BtreeEntry entry2 = { str2, 22};
    BtreeEntry entry3 = { str3, 333};
    BtreeEntry entry4 = { str4, 444};
    BtreeEntry entries[] = {entry1, entry2, entry3, entry4};
    BtreeEntry returnEntry;

    // Try One and Check
    n->insertAt(0, entries[2], 12,13);
    EXPECT_EQ(1U, n->slotuse);
    returnEntry = n->getAt(0);
    ASSERT_EQ(entries[2].keyLength, returnEntry.keyLength);
    EXPECT_EQ(entries[2].pKHash, returnEntry.pKHash);
    EXPECT_EQ(0, memcmp(entries[2].key, returnEntry.key, entries[2].keyLength));

    n->insertAt(1, entries[3], 13, 14); // insert at end
    n->insertAt(0, entries[0], 10, 11); // insert at beginning
    n->insertAt(1, entries[1], 11, 12); // insert in middle

    EXPECT_EQ(4U, n->slotuse);
    EXPECT_EQ(strlen(str1) + strlen(str2) + strlen(str3) + strlen(str4),
              n->keyStorageUsed);
    for (uint16_t i = 0; i < sizeof(entries)/sizeof(BtreeEntry); i++) {
      returnEntry = n->getAt(i);
      EXPECT_EQ(entries[i].keyLength, returnEntry.keyLength);
      EXPECT_EQ(entries[i].pKHash, returnEntry.pKHash);
      EXPECT_EQ(uint16_t(i + 10), n->getChildAt(i));
      EXPECT_EQ(uint16_t(i + 11), n->getChildAt(uint16_t(i + 1)));
      EXPECT_STREQ(
              string((const char*)entries[i].key, entries[i].keyLength).c_str(),
              string((const char*)returnEntry.key, returnEntry.keyLength).c_str());
    }
}

TEST_F(BtreeTest, InnerNode_insertSplit) {
    uint16_t slots = IndexBtree::innerslotmax;
    uint16_t numEntries = uint16_t(slots + 1);

    BtreeEntry read, correct;
    std::vector<BtreeEntry> entries;
    std::vector<std::string> entryKeys;
    generateKeysInRange(0, numEntries, entryKeys, entries);

    Buffer b1, b2;
    IndexBtree::InnerNode *n1, *n2;
    n1 = b1.emplaceAppend<IndexBtree::InnerNode>(&b1, uint16_t(10));
    n2 = b2.emplaceAppend<IndexBtree::InnerNode>(&b2, uint16_t(10));

    for (uint16_t i = 0; i < slots; i++)
        n1->insertAt(i, entries[i], i, i + 1);

    uint16_t half = uint16_t(slots >> 1);
    BtreeEntry splitPoint =
            n1->insertSplit(slots, entries[slots], n2, slots, slots + 1);
    EXPECT_EQ(half, n1->slotuse);
    EXPECT_EQ(numEntries - half - 1, n2->slotuse);

    for (uint16_t i = 0; i < uint16_t (half); i++) {
        correct = entries[i];
        read = n1->getAt(i);
        EXPECT_STREQ(
                string((const char*)correct.key, correct.keyLength).c_str(),
                string((const char*)read.key, read.keyLength).c_str());
        EXPECT_EQ(correct.pKHash, read.pKHash);
        ASSERT_EQ(correct, read);
        EXPECT_EQ(i, n1->getChildAt(i));
        EXPECT_EQ(uint16_t(i + 1), n1->getChildAt(uint16_t(i + 1)));
    }

    EXPECT_EQ(entries[half], splitPoint);

    for (uint16_t i = uint16_t(half + 1); i < numEntries; i++) {
        correct = entries[i];
        read = n2->getAt(uint16_t(i - half - 1));
        EXPECT_STREQ(
                string((const char*)correct.key, correct.keyLength).c_str(),
                string((const char*)read.key, read.keyLength).c_str());
        EXPECT_EQ(correct.pKHash, read.pKHash);
        ASSERT_EQ(correct, read);
        EXPECT_EQ(i, n2->getChildAt(uint16_t(i - half - 1)));
        EXPECT_EQ(uint16_t(i + 1), n2->getChildAt(uint16_t(i - half)));
    }

    // Try a left heavy split by making the last insert at the front
    b1.reset(); b2.reset();
    n1 = b1.emplaceAppend<IndexBtree::InnerNode>(&b1, uint16_t(10));
    n2 = b2.emplaceAppend<IndexBtree::InnerNode>(&b2, uint16_t(10));

    for (uint16_t i = 1; i <= slots; i++) {
        n1->insertAt(uint16_t(i - 1), entries[i], i, uint16_t(i + 1));
    }

    half = uint16_t((slots >> 1) + 1);
    n1->insertSplit(0, entries[0], n2, 0, 1);
    EXPECT_EQ(half - 1, n1->slotuse);
    EXPECT_EQ(numEntries - half, n2->slotuse);

    for (uint16_t i = 0; i < uint16_t (half - 1); i++) {
        correct = entries[i];
        read = n1->getAt(i);
        EXPECT_STREQ(
                string((const char*)correct.key, correct.keyLength).c_str(),
                string((const char*)read.key, read.keyLength).c_str());
        EXPECT_EQ(correct.pKHash, read.pKHash);
        EXPECT_EQ(i, n1->getChildAt(i));
        EXPECT_EQ(uint16_t(i + 1), n1->getChildAt(uint16_t(i + 1)));
    }

    for (uint16_t i = half; i < numEntries; i++) {
        EXPECT_EQ(entries[i], n2->getAt(uint16_t(i - half)));
        EXPECT_EQ(i, n2->getChildAt(uint16_t(i - half)));
        EXPECT_EQ(uint16_t(i + 1), n2->getChildAt(uint16_t(i - half + 1)));
    }
}

TEST_F(BtreeTest, innerNode_eraseAt) {
    std::vector<std::string> keys;
    std::vector<BtreeEntry> entries;
    generateKeysInRange(0, IndexBtree::innerslotmax, keys, entries);

    Buffer buffer;
    IndexBtree::InnerNode *n =
            buffer.emplaceAppend<IndexBtree::InnerNode>(&buffer, uint16_t(10));

    for (uint16_t i = 0; i < IndexBtree::innerslotmax; i++)
        n->insertAt(i, entries[i], uint16_t(i + 10), uint16_t(i + 11));

    // Erase in the middle and check everything is okay
    n->eraseAt(1);
    EXPECT_EQ(10U, n->getChildAt(uint16_t(0)));
    EXPECT_EQ(12U, n->getChildAt(uint16_t(1)));
    BtreeEntry toVerify = n->getAt((uint16_t)(0));
    EXPECT_EQ(0U, toVerify.pKHash);
    EXPECT_EQ(entries[0].keyLength, toVerify.keyLength);
    EXPECT_STREQ(keys[0].c_str(),
             string((const char*)toVerify.key, toVerify.keyLength).c_str());

    for (uint16_t i = 2; i < IndexBtree::innerslotmax; i++) {
        BtreeEntry toVerify = n->getAt((uint16_t)(i - 1));
        EXPECT_EQ(uint16_t(i + 10), n->getChildAt(uint16_t(i - 1)));
        EXPECT_EQ(uint16_t(i + 11), n->getChildAt(uint16_t(i)));
        EXPECT_EQ(i, toVerify.pKHash);
        EXPECT_EQ(entries[i].keyLength, toVerify.keyLength);
        EXPECT_STREQ(keys[i].c_str(),
                 string((const char*)toVerify.key, toVerify.keyLength).c_str());
    }

    // Erase at front and check everything is okay
    n->eraseAt(0);
    EXPECT_EQ(n->slotuse, IndexBtree::innerslotmax - 2);
    for (uint16_t i = 2; i < IndexBtree::innerslotmax; i++) {
        BtreeEntry toVerify = n->getAt((uint16_t)(i - 2));
        EXPECT_EQ(uint16_t(i + 10), n->getChildAt(uint16_t(i - 2)));
        EXPECT_EQ(uint16_t(i + 11), n->getChildAt(uint16_t(i - 1)));
        EXPECT_EQ(i, toVerify.pKHash);
        EXPECT_EQ(entries[i].keyLength, toVerify.keyLength);
        EXPECT_STREQ(keys[i].c_str(),
                 string((const char*)toVerify.key, toVerify.keyLength).c_str());
    }

    // Erase near end and check that everything is okay
    n->eraseAt(IndexBtree::innerslotmax - 3);
    EXPECT_EQ(n->slotuse, IndexBtree::innerslotmax - 3);
    EXPECT_EQ(uint16_t(IndexBtree::innerslotmax + 10),
                                                    n->getChildAt(n->slotuse));
    for (uint16_t i = 2; i < IndexBtree::innerslotmax - 1; i++) {
        BtreeEntry toVerify = n->getAt((uint16_t)(i - 2));
        EXPECT_EQ(uint16_t(i + 10), n->getChildAt(uint16_t(i - 2)));
        EXPECT_EQ(i, toVerify.pKHash);
        EXPECT_EQ(entries[i].keyLength, toVerify.keyLength);
        EXPECT_STREQ(keys[i].c_str(),
                 string((const char*)toVerify.key, toVerify.keyLength).c_str());
    }

    // Erase AT end and check that last child is properly updated;
    n->eraseAt(n->slotuse);
    EXPECT_EQ(uint16_t(IndexBtree::innerslotmax +8), n->getChildAt(n->slotuse));
}

TEST_F(BtreeTest, writeReadFreeNode) {
  IndexBtree bt(tableId, &objectManager);
  Buffer buffer_in, buffer_out;
  IndexBtree::LeafNode *n =
          buffer_in.emplaceAppend<IndexBtree::LeafNode>(&buffer_in);

  fillNodeSorted(n);
  bt.writeNode(n, 1000);
  bt.flush();

  const void *ptr = bt.readNode(1000, &buffer_out);
  checkNodeEquals(n, (const IndexBtree::LeafNode*) ptr);

  bt.freeNode(1000);
  bt.flush();
  EXPECT_TRUE(NULL == bt.readNode(1000, &buffer_out));
}

TEST_F (BtreeTest, writeReadInnerNode) {
    BtreeEntry eTest = {"Testing", 123};
    BtreeEntry e0 = {"zero", 0};
    BtreeEntry e1 = {"One", 1};

    IndexBtree bt(tableId, &objectManager);
    Buffer buffer_in, buffer_out;
    IndexBtree::InnerNode *n = buffer_in.emplaceAppend<IndexBtree::InnerNode>(
                                                    &buffer_in, uint16_t(17));

    n->setRightMostLeafKey(eTest);
    n->insertAt(0, e0, 0, 1);
    n->insertAt(1, e1, 1, 2);

    bt.writeNode(n, 1000);
    bt.flush();

    IndexBtree::InnerNode *rn =
            static_cast<IndexBtree::InnerNode*>(bt.readNode(1000, &buffer_out));


    BtreeEntry query = rn->getRightMostLeafKey();
    EXPECT_EQ(eTest.pKHash, query.pKHash);
    EXPECT_EQ(eTest.keyLength, query.keyLength);
    EXPECT_STREQ((const char*) eTest.key,
            string((const char*) query.key, query.keyLength).c_str());

    EXPECT_EQ(e0, rn->getAt(0));
    EXPECT_EQ(e1, rn->getAt(1));
}

static void testBalanceWithRight(uint16_t leftSize, uint16_t rightSize) {
    Buffer b1, b2;
    std::vector<BtreeEntry> entries;
    std::vector<std::string> entryKeys;
    IndexBtree::InnerNode *left, *right;

    generateKeysInRange(0, (leftSize + rightSize + 1), entryKeys, entries, 5);
    left = b1.emplaceAppend<IndexBtree::InnerNode>(&b1, uint16_t(10));
    right = b2.emplaceAppend<IndexBtree::InnerNode>(&b2, uint16_t(10));

    // Fill the nodes accordingly
    uint16_t index = 0;
    for (uint16_t i = 0; i < leftSize; i++) {
        left->insertAt(i, entries[index], index, index + 1);
        index++;
    }

    BtreeEntry inbetween = entries[index++];

    for (uint16_t i = 0; i < rightSize; i++) {
        right->insertAt(i, entries[index], index, index + 1);
        index++;
    }

    // real test
    BtreeEntry middle = left->balanceWithRight(right, inbetween);

    uint32_t smallHalf = (leftSize + rightSize) >> 1;
    uint32_t largeHalf = (leftSize + rightSize) - smallHalf;
    uint32_t leftExpected, rightExpected;
    if (leftSize > rightSize) {
        leftExpected = largeHalf;
        rightExpected = smallHalf;
    } else {
        leftExpected = largeHalf;
        rightExpected = smallHalf;
    }
    EXPECT_EQ(leftExpected, left->slotuse);
    EXPECT_EQ(rightExpected, right->slotuse);

    // check middle key
    BtreeEntry correct = entries[leftExpected];
    EXPECT_EQ(correct.keyLength, middle.keyLength);
    EXPECT_STREQ(
        string((const char*)correct.key, correct.keyLength).c_str(),
        string((const char*)middle.key, middle.keyLength).c_str());


    // Check internals
    for (uint16_t i = 0; i < leftExpected; i++) {
        BtreeEntry correct = entries[i];
        BtreeEntry query = left->getAt(i);
        EXPECT_EQ(correct.pKHash, query.pKHash);
        EXPECT_EQ(i, left->getChildAt(i));
        EXPECT_EQ(i + 1U, left->getChildAt(uint16_t(i + 1)));
        EXPECT_STREQ(
            string((const char*)correct.key, correct.keyLength).c_str(),
            string((const char*)query.key, query.keyLength).c_str());
    }

    for (uint16_t i = 0; i < rightExpected; i++) {
        uint16_t correctIndex = (uint16_t)(i + leftExpected + 1);
        BtreeEntry correct = entries[correctIndex];
        BtreeEntry query = right->getAt(i);
        EXPECT_EQ(correct.pKHash, query.pKHash);
        EXPECT_EQ(correctIndex, right->getChildAt(i));
        EXPECT_EQ(correctIndex + 1U, right->getChildAt(uint16_t(i + 1U)));
        EXPECT_STREQ(
            string((const char*)correct.key, correct.keyLength).c_str(),
            string((const char*)query.key, query.keyLength).c_str());
    }
}

TEST_F(BtreeTest, InnerNode_balanceWithRight) {
    uint16_t min = IndexBtree::innerslotmax;
    uint16_t max = IndexBtree::mininnerslots;

    // Tests all balances with left node that is just underflowing with the
    // right sibling of varying fullness.
    for (uint16_t rightSize = min; rightSize < max; rightSize++) {
        testBalanceWithRight(uint16_t(min - 1), rightSize);
    }

    // Tests all balances with right node that is just underflowing with the
    // left sibling of varying fullness.
    for (uint16_t leftSize = min; leftSize < max; leftSize++) {
        testBalanceWithRight(leftSize, uint16_t(min - 1));
    }
}

TEST_F(BtreeTest, InnerNode_mergeIntoRight) {
    Buffer b1, b2;
    std::vector<BtreeEntry> entries;
    std::vector<std::string> entryKeys;
    IndexBtree::InnerNode *left, *right;
    uint16_t max = IndexBtree::innerslotmax;
    generateKeysInRange(0, max + 1, entryKeys, entries);

    left = b1.emplaceAppend<IndexBtree::InnerNode>(&b1, uint16_t(10));
    right = b2.emplaceAppend<IndexBtree::InnerNode>(&b2, uint16_t(10));

    // The only case where a merge will occur within the btree is when
    // its sibling is anemic. So we only test that case.
    uint16_t index = 0;
    for (uint16_t i = 0; i < 3; i++) {
        left->insertAt(i, entries[index], index, index + 1);
        index++;
    }

    BtreeEntry inbetween = entries[index++];

    for (uint16_t i = 0; i < 4; i++) {
        right->insertAt(i, entries[index], index, index + 1);
        index++;
    }

    left->mergeIntoRight(right, inbetween);
    EXPECT_EQ(0U, left->slotuse);
    EXPECT_EQ(8U, right->slotuse);

    for (uint16_t i = 0; i < 8U; i++) {
        BtreeEntry correct = entries[i];
        BtreeEntry query = right->getAt(i);
        EXPECT_EQ(correct.pKHash, query.pKHash);
        EXPECT_EQ(i, right->getChildAt(i));
        EXPECT_EQ(i + 1U, right->getChildAt(uint16_t(i + 1U)));
        EXPECT_STREQ(
            string((const char*)correct.key, correct.keyLength).c_str(),
            string((const char*)query.key, query.keyLength).c_str());
    }

    // switch roles
    b1.reset(); b2.reset();
    left = b1.emplaceAppend<IndexBtree::InnerNode>(&b1, uint16_t(10));
    right = b2.emplaceAppend<IndexBtree::InnerNode>(&b2, uint16_t(10));

    index = 0;
    for (uint16_t i = 0; i < 3; i++) {
        left->insertAt(i, entries[index], index, index + 1);
        index++;
    }

    inbetween = entries[index++];

    for (uint16_t i = 0; i < 4; i++) {
        right->insertAt(i, entries[index], index, index + 1);
        index++;
    }

    left->mergeIntoRight(right, inbetween);
    EXPECT_EQ(0U, left->slotuse);
    EXPECT_EQ(8U, right->slotuse);

    for (uint16_t i = 0; i < 8U; i++) {
        BtreeEntry correct = entries[i];
        BtreeEntry query = right->getAt(i);
        EXPECT_EQ(correct.pKHash, query.pKHash);
        EXPECT_EQ(i, right->getChildAt(i));
        EXPECT_EQ(i + 1U, right->getChildAt(uint16_t(i + 1U)));
        EXPECT_STREQ(
            string((const char*)correct.key, correct.keyLength).c_str(),
            string((const char*)query.key, query.keyLength).c_str());
    }
}

TEST_F(BtreeTest, InnerNode_serializeAppendToBuffer) {
    BtreeEntry eTest = {"Testing", 123};
    BtreeEntry e0 = {"zero", 0};
    BtreeEntry e1 = {"One", 1};
    char buffer[1024];
    BtreeEntry e2 = {buffer, 1024, 2};
    Buffer buffer_in, buffer_out;
    IndexBtree::InnerNode *n = buffer_in.emplaceAppend<IndexBtree::InnerNode>(
                                                    &buffer_in, uint16_t(17));

    n->setRightMostLeafKey(eTest);
    n->insertAt(0, e0, 0, 1);
    n->insertAt(1, e1, 1, 2);
    n->insertAt(2, e2, 2, 3);

    IndexBtree::InnerNode *rn =static_cast<IndexBtree::InnerNode*>(
                                    n->serializeAppendToBuffer(&buffer_out));

    EXPECT_EQ(1U, buffer_out.getNumberChunks());
    EXPECT_EQ(n->serializedLength(), rn->serializedLength());
    EXPECT_EQ(n->serializedLength(), buffer_out.size());

    BtreeEntry query = rn->getRightMostLeafKey();
    EXPECT_EQ(eTest.pKHash, query.pKHash);
    EXPECT_EQ(eTest.keyLength, query.keyLength);
    EXPECT_STREQ((const char*) eTest.key,
            string((const char*) query.key, query.keyLength).c_str());

    query = rn->getAt(0);
    EXPECT_EQ(e0.pKHash, query.pKHash);
    EXPECT_EQ(e0.keyLength, query.keyLength);
    EXPECT_STREQ((const char*) e0.key,
            string((const char*) query.key, query.keyLength).c_str());

    query = rn->getAt(1);
    EXPECT_EQ(e1.pKHash, query.pKHash);
    EXPECT_EQ(e1.keyLength, query.keyLength);
    EXPECT_STREQ((const char*) e1.key,
            string((const char*) query.key, query.keyLength).c_str());

    query = rn->getAt(2);
    EXPECT_EQ(e2.pKHash, query.pKHash);
    EXPECT_EQ(e2.keyLength, query.keyLength);
    EXPECT_EQ(0, bcmp(e2.key, query.key, query.keyLength));
}

TEST_F(BtreeTest, LeafNode_balanceWithRight) {
    Buffer b1, b2;
    std::vector<BtreeEntry> entries;
    std::vector<std::string> entryKeys;
    IndexBtree::LeafNode *left, *right;
    uint16_t max = IndexBtree::leafslotmax;
    uint16_t min = IndexBtree::minleafslots;
    generateKeysInRange(0, 2*max, entryKeys, entries, 5);

    // Since the logic for the LeafNode balance is very simple and relies
    // mostly on an already tested function, we just test that the split numbers
    // are correct
    for (uint16_t rightSize = min; rightSize < max; rightSize++) {
        uint16_t smallHalf = uint16_t((rightSize + min - 1) >> 1);
        uint16_t largeHalf = uint16_t((rightSize + min - 1) - smallHalf);

        b1.reset(); b2.reset();
        left = b1.emplaceAppend<IndexBtree::LeafNode>(&b1);
        right = b2.emplaceAppend<IndexBtree::LeafNode>(&b2);

        for (uint16_t i = 0 ; i < min - 1; i++)
            left->insertAt(i, entries[i]);

        for (uint16_t i = 0; i < rightSize; i++)
            right->insertAt(i, entries[i + min  - 1]);

        left->balanceWithRight(right);
        EXPECT_EQ(smallHalf, left->slotuse);
        EXPECT_EQ(largeHalf, right->slotuse);
    }

    for (uint16_t leftSize = min; leftSize < max; leftSize++) {
        uint16_t smallHalf = uint16_t((leftSize + min - 1) >> 1);
        uint16_t largeHalf = uint16_t((leftSize + min - 1) - smallHalf);

        b1.reset(); b2.reset();
        left = b1.emplaceAppend<IndexBtree::LeafNode>(&b1);
        right = b2.emplaceAppend<IndexBtree::LeafNode>(&b2);

        for (uint16_t i = 0 ; i < leftSize; i++)
            left->insertAt(i, entries[i]);

        for (uint16_t i = 0; i < min - 1; i++)
            right->insertAt(i, entries[i + min  - 1]);

        left->balanceWithRight(right);
        EXPECT_EQ(largeHalf, left->slotuse);
        EXPECT_EQ(smallHalf, right->slotuse);
    }
}

TEST_F(BtreeTest, LeafNode_mergeIntoRight) {
    Buffer b1, b2;
    std::vector<BtreeEntry> entries;
    std::vector<std::string> entryKeys;
    IndexBtree::LeafNode *left, *right;
    uint16_t max = IndexBtree::innerslotmax;
    generateKeysInRange(0, max + 1, entryKeys, entries);

    left = b1.emplaceAppend<IndexBtree::LeafNode>(&b1);
    right = b2.emplaceAppend<IndexBtree::LeafNode>(&b2);

    // The only case where a merge will occur within the btree is when
    // its sibling is anemic. So we only test that case.
    uint16_t index = 0;
    for (uint16_t i = 0; i < 3; i++) {
        left->insertAt(i, entries[index]);
        index++;
    }

    for (uint16_t i = 0; i < 4; i++) {
        right->insertAt(i, entries[index]);
        index++;
    }

    left->mergeIntoRight(right);
    EXPECT_EQ(0U, left->slotuse);
    EXPECT_EQ(7U, right->slotuse);

    for (uint16_t i = 0; i < 7U; i++) {
        BtreeEntry correct = entries[i];
        BtreeEntry query = right->getAt(i);
        EXPECT_EQ(correct.pKHash, query.pKHash);
        EXPECT_STREQ(
            string((const char*)correct.key, correct.keyLength).c_str(),
            string((const char*)query.key, query.keyLength).c_str());
    }

    // switch roles
    b1.reset(); b2.reset();
    left = b1.emplaceAppend<IndexBtree::LeafNode>(&b1);
    right = b2.emplaceAppend<IndexBtree::LeafNode>(&b2);

    index = 0;
    for (uint16_t i = 0; i < 4; i++) {
        left->insertAt(i, entries[index]);
        index++;
    }

    for (uint16_t i = 0; i < 3; i++) {
        right->insertAt(i, entries[index]);
        index++;
    }

    left->mergeIntoRight(right);
    EXPECT_EQ(0U, left->slotuse);
    EXPECT_EQ(7U, right->slotuse);

    for (uint16_t i = 0; i < 7U; i++) {
        BtreeEntry correct = entries[i];
        BtreeEntry query = right->getAt(i);
        EXPECT_EQ(correct.pKHash, query.pKHash);
        EXPECT_STREQ(
            string((const char*)correct.key, correct.keyLength).c_str(),
            string((const char*)query.key, query.keyLength).c_str());
    }
}

TEST_F(BtreeTest, InnerNode_toString_printToLog) {
    Buffer objBuffer;
    IndexBtree::InnerNode *n =
        objBuffer.emplaceAppend<IndexBtree::InnerNode>(&objBuffer, uint16_t(10));

    std::vector<std::string> keys;
    std::vector<BtreeEntry> entries;
    generateKeysInRange(0, IndexBtree::innerslotmax/2, keys, entries, 10);

    for (uint16_t i = 0; i < 4; ++i)
        n->insertAt(i, entries[i], i + 10, i + 11);

    n->setRightMostLeafKey({"Yello Cah", 1337});

    EXPECT_STREQ("Printing inner node\r\n"
            "NodeId: 10 <==  ( pKHash: 0 keyLength: 10 key: 0000000000 )\r\n"
            "NodeId: 11 <==  ( pKHash: 1 keyLength: 10 key: 0000000001 )\r\n"
            "NodeId: 12 <==  ( pKHash: 2 keyLength: 10 key: 0000000002 )\r\n"
            "NodeId: 13 <==  ( pKHash: 3 keyLength: 10 key: 0000000003 )\r\n"
            "NodeId: 14 <==  ( pKHash: 1337 keyLength: 9 key: Yello Cah )\r\n"
            , n->toString().c_str());

    TestLog::Enable _;
    n->printToLog(NOTICE);
    EXPECT_STREQ("printToLog: Printing inner node\r\n"
            "NodeId: 10 <==  ( pKHash: 0 keyLength: 10 key: 0000000000 )\r\n"
            "NodeId: 11 <==  ( pKHash: 1 keyLength: 10 key: 0000000001 )\r\n"
            "NodeId: 12 <==  ( pKHash: 2 keyLength: 10 key: 0000000002 )\r\n"
            "NodeId: 13 <==  ( pKHash: 3 keyLength: 10 key: 0000000003 )\r\n"
            "NodeId: 14 <==  ( pKHash: 1337 keyLength: 9 key: Yello Cah )\r\n"
            , TestLog::get().c_str());
}

TEST_F(BtreeTest, isGreaterOrEqual) {
    IndexBtree bt(tableId, &objectManager);
    Buffer buffer;
    IndexBtree::InnerNode *innerNode =
            buffer.emplaceAppend<IndexBtree::InnerNode>(&buffer, uint16_t(10));
    IndexBtree::LeafNode *leafNode =
            buffer.emplaceAppend<IndexBtree::LeafNode>(&buffer);

    uint32_t numEntries = IndexBtree::innerslotmax + 1;
    std::vector<BtreeEntry> entries;
    std::vector<std::string> entryKeys;
    generateKeysInRange(0, numEntries, entryKeys, entries, 4);

    uint16_t i = 0;
    for (; i < IndexBtree::innerslotmax; i++)
        innerNode->insertAt(i, entries[i], i, uint16_t(i + 1));
    innerNode->setRightMostLeafKey(entries[i]);

    for (uint16_t i = 0; i < IndexBtree::leafslotmax; i++)
        leafNode->insertAt(i, entries[i]);

    BtreeEntry less = {"0000000000", 0};
    BtreeEntry mid =  entries[numEntries/2];
    BtreeEntry max = entries.back();
    BtreeEntry largest = {"lola", 9999};

    NodeId innerId = 200, leafId = 300;
    bt.writeNode(innerNode, innerId);
    bt.writeNode(leafNode, leafId);
    bt.flush();

    Buffer innerBuffer;
    Buffer leafBuffer;
    bt.readNode(innerId, &innerBuffer);
    bt.readNode(leafId, &leafBuffer);

    EXPECT_TRUE(IndexBtree::isGreaterOrEqual(&innerBuffer, less));
    EXPECT_TRUE(IndexBtree::isGreaterOrEqual(&innerBuffer, mid));
    EXPECT_TRUE(IndexBtree::isGreaterOrEqual(&innerBuffer, max));
    EXPECT_FALSE(IndexBtree::isGreaterOrEqual(&innerBuffer, largest));

    EXPECT_TRUE(IndexBtree::isGreaterOrEqual(&leafBuffer, less));
    EXPECT_TRUE(IndexBtree::isGreaterOrEqual(&leafBuffer, mid));
    EXPECT_FALSE(IndexBtree::isGreaterOrEqual(&leafBuffer, max));
    EXPECT_FALSE(IndexBtree::isGreaterOrEqual(&leafBuffer, largest));
}

TEST_F(BtreeTest, clear) {
    uint16_t slots = IndexBtree::innerslotmax;
    uint32_t numEntries = static_cast<uint32_t>(slots*slots + 1);

    std::vector<BtreeEntry> entries;
    std::vector<std::string> entryKeys;
    generateKeysInRange(0, numEntries, entryKeys, entries);

    IndexBtree bt(tableId, &objectManager);
    for (uint32_t i = 0; i < numEntries; i++)
        bt.insert(entries[i]);

    NodeId highestUsed = bt.getNextNodeId();
    bt.clear();

    EXPECT_EQ(0U, bt.m_stats.itemcount);
    EXPECT_EQ(0U, bt.m_stats.innernodes);
    EXPECT_EQ(0U, bt.m_stats.leaves);
    EXPECT_EQ("", bt.verify());

    Buffer buffer;
    for(NodeId i = ROOT_ID; i < highestUsed; i++) {
        EXPECT_TRUE(NULL == bt.readNode(i, &buffer));
    }
}

TEST_F(BtreeTest, begin_end_size_exists_find_empty) {
    uint16_t slots = IndexBtree::innerslotmax;
    uint32_t numEntries = static_cast<uint32_t>(slots*slots + 1);

    std::vector<BtreeEntry> entries;
    std::vector<std::string> entryKeys;
    generateKeysInRange(0, numEntries, entryKeys, entries);

    IndexBtree bt(tableId, &objectManager);
    IndexBtree::iterator it = bt.begin();
    EXPECT_EQ(bt.end(), it);
    EXPECT_EQ(0U, bt.size());
    EXPECT_TRUE(bt.empty());

    for (uint32_t i = 0; i < numEntries; i++) {
        EXPECT_FALSE(bt.exists(entries[i]));
        EXPECT_EQ(bt.end(), bt.find(entries[i]));
        EXPECT_EQ(i, bt.size());

        bt.insert(entries[i]);

        EXPECT_EQ(i + 1, bt.size());
        IndexBtree::iterator found = bt.find(entries[i]);
        EXPECT_NE(bt.end(), found);
        EXPECT_EQ(entries[i], *found);
    }

    EXPECT_FALSE(bt.empty());

    it = bt.begin();
    EXPECT_EQ(entries[0], *it);
    for (uint32_t i = 0; i < numEntries; i++) {
        ASSERT_EQ(numEntries - i, bt.size());
        ASSERT_TRUE(bt.erase(entries[i]));
        ASSERT_EQ(numEntries - i - 1, bt.size());
    }

    EXPECT_EQ(bt.end(), bt.begin());
    EXPECT_EQ(0U, bt.size());
    EXPECT_TRUE(bt.empty());
}

TEST_F(BtreeTest, find_pkHashAgnostic) {
    uint32_t slots = IndexBtree::innerslotmax;
    uint32_t numEntries  = uint32_t(slots*slots + 1);

    // Load up the Btree with the same key over and but different pkhash
    IndexBtree bt(tableId, &objectManager);
    for (uint32_t i = 0; i < numEntries; i++) {
        BtreeEntry e = {"Aba", i};
        bt.insert(e);
    }

    // First make sure that everything is in order
    IndexBtree::iterator it = bt.begin();
    uint32_t j = 0;
    while(it != bt.end()) {
        EXPECT_EQ(j, it->pKHash);
        ++it;
        j++;
    }

    // Make sure we can find the right amount for each
    for (uint64_t i = numEntries - 1; i > 0; i--) {
        BtreeEntry e = {"Aba", i};
        IndexBtree::iterator it = bt.find(e);
        EXPECT_EQ(i, it->pKHash);

        // Count the entries that come after it.
        uint32_t j = 1;
        while (++it != bt.end()) {
            j++;
        }
        EXPECT_EQ(numEntries - i, j);
    }
}

TEST_F(BtreeTest, lower_upper_bound) {
    uint16_t slots = IndexBtree::innerslotmax;
    uint32_t numEntries = static_cast<uint32_t>(slots*slots + 1);

    std::vector<BtreeEntry> entries;
    std::vector<std::string> entryKeys;
    generateKeysInRange(0, numEntries, entryKeys, entries, 2);

    IndexBtree bt(tableId, &objectManager);
    // First insert uniform data
    for (uint32_t i = 1; i < numEntries; i++) {
        bt.insert(entries[i]);
    }


    // Sprinkle in some test data
    BtreeEntry same1 = {"0Hello", 1};
    BtreeEntry same2 = {"0Hello", 2};
    BtreeEntry same3 = {"0Hello", 3};
    BtreeEntry greater = {"Hello1", 1};
    BtreeEntry lesser = {"Hell", 1};

    bt.insert(same1);
    bt.insert(same2);
    bt.insert(same3);
    bt.insert(greater);
    bt.insert(lesser);

    // Start finding the bounds
    IndexBtree::iterator it;
    // Everything is larger
    it = bt.lower_bound({"000000", 100});
    EXPECT_STREQ(bt.begin()->toString().c_str(), it->toString().c_str());

    it = bt.upper_bound({"000000", 100});
    EXPECT_STREQ(bt.begin()->toString().c_str(), it->toString().c_str());

    /// Border of hell
    it = bt.lower_bound({"Hell", 100});
    EXPECT_STREQ(greater.toString().c_str(), it->toString().c_str());

    it = bt.upper_bound({"Hell", 100});
    EXPECT_STREQ(greater.toString().c_str(), it->toString().c_str());

    /// Other border of hell with pkHash = 0
    it = bt.lower_bound({"Hell", 0});
    EXPECT_STREQ(lesser.toString().c_str(), it->toString().c_str());

    it = bt.upper_bound({"Hell", 0});
    EXPECT_STREQ(lesser.toString().c_str(), it->toString().c_str());

    /// Right in hell
    it = bt.lower_bound({"Hell", 1});
    EXPECT_STREQ(lesser.toString().c_str(), it->toString().c_str());

    it = bt.upper_bound({"Hell", 1});
    EXPECT_STREQ(greater.toString().c_str(), it->toString().c_str());

    // Same Key, different pkHashes
    it = bt.lower_bound({"0Hello", 2});
    EXPECT_STREQ(same2.toString().c_str(), it->toString().c_str());

    it = bt.upper_bound({"0Hello", 2});
    EXPECT_STREQ(same3.toString().c_str(), it->toString().c_str());

    // Everything is too small
    it = bt.lower_bound({"Zzz", 100});
    EXPECT_EQ(bt.end(), it);

    it = bt.upper_bound({"Zzz", 100});
    EXPECT_EQ(bt.end(), it);\
}

TEST_F(BtreeTest, insert) {
  BtreeEntry result;
  uint64_t rootId = ROOT_ID;
  uint16_t slots = IndexBtree::innerslotmax + 1;

  std::vector<std::string> entryKeys;
  std::vector<BtreeEntry> entries;
  generateKeysInRange(0, slots, entryKeys, entries, 30);

  // Insert 1
  IndexBtree bt(tableId, &objectManager);
  bt.insert(entries[0]);
  EXPECT_EQ(1U, bt.size());
  EXPECT_EQ(1U, bt.m_stats.leaves);
  EXPECT_EQ(0U, bt.m_stats.innernodes);
  EXPECT_EQ(rootId + 1, bt.nextNodeId);

  IndexBtree::iterator it = bt.find(entries[0]);
  result = *it;
  EXPECT_EQ(entries[0].keyLength, result.keyLength);
  EXPECT_EQ(entries[0].pKHash, result.pKHash);
  EXPECT_STREQ((const char*)entries[0].key,
                string((const char*) result.key, result.keyLength).c_str());
  EXPECT_EQ(bt.end(), ++it);
  EXPECT_EQ("", bt.verify());

  // Insert 1 less from max
  for (int i = 1; i < slots - 1; i++) {
    bt.insert(entries[i]);
  }
  EXPECT_EQ("", bt.verify());

  EXPECT_EQ((uint32_t)(slots - 1), bt.size());
  EXPECT_EQ(1U, bt.m_stats.leaves);
  EXPECT_EQ(0U, bt.m_stats.innernodes);

  // Insert the last one, should observe a split
  bt.insert(entries[slots - 1]);
  EXPECT_EQ((uint32_t) slots, bt.size());
  EXPECT_EQ(2U, bt.m_stats.leaves);
  EXPECT_EQ(1U, bt.m_stats.innernodes);
  EXPECT_EQ("", bt.verify());

  // Get a tree iterator and check that everything is okay
  it = bt.begin();
  for (int i = 0; i < slots; i++) {
    EXPECT_EQ(entries[i].pKHash, it->pKHash);
    EXPECT_EQ(entries[i].keyLength, it->keyLength);
    EXPECT_STREQ((const char*)entries[i].key,
            string((const char*)it->key, it->keyLength).c_str());
    ++it;
  }

  EXPECT_EQ(bt.end(), it);
}

TEST_F(BtreeTest, insert_superLargeKeys) {
    uint16_t slots = IndexBtree::innerslotmax;
    uint32_t numEntries = static_cast<uint32_t>(slots);

    std::vector<BtreeEntry> entries;
    std::vector<std::string> entryKeys;
    generateKeysInRange(0, numEntries, entryKeys, entries, 4000);

    IndexBtree bt(tableId, &objectManager);
    for (uint32_t i = 0; i < numEntries; i++) {
        bt.insert(entries[i]);
        EXPECT_EQ("", bt.verify());
    }

    for (uint32_t j = 0; j < numEntries; j++) {
        IndexBtree::iterator it = bt.find(entries[j]);
        EXPECT_STREQ(entryKeys[j].c_str(),
                string((const char*)it->key, it->keyLength).c_str());
    }
}

TEST_F(BtreeTest, insert_alot) {
    uint16_t slots = IndexBtree::innerslotmax;
    uint32_t numEntries = static_cast<uint32_t>(slots*slots*slots);

    std::vector<BtreeEntry> entries;
    std::vector<std::string> entryKeys;
    generateKeysInRange(0, numEntries, entryKeys, entries);

    IndexBtree bt(tableId, &objectManager);
    while (entries.size() > 0) {
        bt.insert(entries.back());
        entries.pop_back();
        ASSERT_EQ("", bt.verify());
    }

    EXPECT_EQ("", bt.verify());
    for (uint32_t j = 0; j < numEntries; j++) {
        BtreeEntry entry = entries[j];
        IndexBtree::iterator it = bt.find(entry);
        EXPECT_STREQ(
                string((const char*)entry.key, entry.keyLength).c_str(),
                string((const char*)it->key, it->keyLength).c_str());
    }
}

TEST_F(BtreeTest, insert_random) {
    srand(0);
    uint16_t slots = IndexBtree::innerslotmax;
    uint32_t numEntries = static_cast<uint32_t>(slots*slots*slots);

    std::vector<BtreeEntry> entries;
    std::vector<std::string> entryKeys;
    std::vector<BtreeEntry> scrambled;
    generateKeysInRange(0, numEntries, entryKeys, entries);

    IndexBtree bt(tableId, &objectManager);
    while (entries.size() > 0) {
        uint32_t index = (uint32_t)(rand() %  entries.size());
        entries.erase(entries.begin() + index);
        BtreeEntry entry = entries[index];
        bt.insert(entry);
        scrambled.push_back(entry);
        ASSERT_EQ("", bt.verify());

    }

    EXPECT_EQ("", bt.verify());
    for (uint32_t j = 0; j < numEntries; j++) {
        BtreeEntry entry = scrambled[j];
        IndexBtree::iterator it = bt.find(entry);
        EXPECT_STREQ(
                string((const char*)entry.key, entry.keyLength).c_str(),
                string((const char*)it->key, it->keyLength).c_str());
    }
}

TEST_F(BtreeTest, key_all) {
    IndexBtree bt(tableId, &objectManager);

    BtreeEntry i0 = {"AHello", 2};
    BtreeEntry i1 = {"Hello1", 1};
    BtreeEntry i2 = {"Hello2", 2};
    BtreeEntry i3 = {"Hello3", 3};
    BtreeEntry i4 = {"ZHello", 2};

    // Keyless
    EXPECT_TRUE(bt.key_less(i2, i3));
    EXPECT_TRUE(bt.key_less(i2, i4));
    EXPECT_FALSE(bt.key_less(i2, i2));
    EXPECT_FALSE(bt.key_less(i2, i1));
    EXPECT_FALSE(bt.key_less(i2, i0));

    // Key Equal
    EXPECT_TRUE(bt.key_lessequal(i2, i3));
    EXPECT_TRUE(bt.key_lessequal(i2, i4));
    EXPECT_TRUE(bt.key_lessequal(i2, i2));
    EXPECT_FALSE(bt.key_lessequal(i2, i1));
    EXPECT_FALSE(bt.key_lessequal(i2, i0));

    // Key Greater
    EXPECT_FALSE(bt.key_greater(i2, i3));
    EXPECT_FALSE(bt.key_greater(i2, i4));
    EXPECT_FALSE(bt.key_greater(i2, i2));
    EXPECT_TRUE(bt.key_greater(i2, i1));
    EXPECT_TRUE(bt.key_greater(i2, i0));

    // Key Greater Equal
    EXPECT_FALSE(bt.key_greaterequal(i2, i3));
    EXPECT_FALSE(bt.key_greaterequal(i2, i4));
    EXPECT_TRUE(bt.key_greaterequal(i2, i2));
    EXPECT_TRUE(bt.key_greaterequal(i2, i1));
    EXPECT_TRUE(bt.key_greaterequal(i2, i0));

    // Key Equal
    EXPECT_FALSE(bt.key_equal(i2, i3));
    EXPECT_FALSE(bt.key_equal(i2, i4));
    EXPECT_TRUE(bt.key_equal(i2, i2));
    EXPECT_FALSE(bt.key_equal(i2, i1));
    EXPECT_FALSE(bt.key_equal(i2, i0));
}

TEST_F(BtreeTest, findEntryGE) {
    Buffer buffer1;
    IndexBtree::LeafNode *n = buffer1.emplaceAppend<IndexBtree::LeafNode>(&buffer1);

    BtreeEntry entry00 = {"0aaaDFlkjasdf", 22};
    BtreeEntry entry0 = {"AaaaDFlkjasdf", 22};
    BtreeEntry entry05 = {"abaaDFlkjasdf", 22};
    BtreeEntry entry1 = {"bbb0aDFlkjasdf", 22};
    BtreeEntry entry15 = {"bbbaaDFlkjasdf", 22};
    BtreeEntry entry2 = {"bbbcaDFlkjasdf", 22};
    BtreeEntry entry25 = {"zbbbcaDFlkjasdf", 22};
    BtreeEntry entry3 = {"zzccclkjasdf", 22};
    BtreeEntry entry35 = {"zzzz", 22};

    n->setAt(0, entry0);
    n->setAt(1, entry1);
    n->setAt(2, entry2);
    n->setAt(3, entry3);

    // Everything is in its place
    IndexBtree bt(10, NULL);
    EXPECT_EQ(0, bt.findEntryGE(n, entry0));
    EXPECT_EQ(1, bt.findEntryGE(n, entry1));
    EXPECT_EQ(2, bt.findEntryGE(n, entry2));
    EXPECT_EQ(3, bt.findEntryGE(n, entry3));

    // All the in-betweens also work
    EXPECT_EQ(0, bt.findEntryGE(n, entry00));
    EXPECT_EQ(1, bt.findEntryGE(n, entry05));
    EXPECT_EQ(2, bt.findEntryGE(n, entry15));
    EXPECT_EQ(3, bt.findEntryGE(n, entry25));
    EXPECT_EQ(4, bt.findEntryGE(n, entry35));
}

TEST_F(BtreeTest, findEntryGreater) {
    Buffer buffer1;
    IndexBtree::LeafNode *n = buffer1.emplaceAppend<IndexBtree::LeafNode>(&buffer1);

    BtreeEntry entry00 = {"0aaaDFlkjasdf", 22};
    BtreeEntry entry0 = {"AaaaDFlkjasdf", 22};
    BtreeEntry entry05 = {"abaaDFlkjasdf", 22};
    BtreeEntry entry1 = {"bbb0aDFlkjasdf", 22};
    BtreeEntry entry15 = {"bbbaaDFlkjasdf", 22};
    BtreeEntry entry2 = {"bbbcaDFlkjasdf", 22};
    BtreeEntry entry25 = {"zbbbcaDFlkjasdf", 22};
    BtreeEntry entry3 = {"zzccclkjasdf", 22};
    BtreeEntry entry35 = {"zzzz", 22};

    n->setAt(0, entry0);
    n->setAt(1, entry1);
    n->setAt(2, entry2);
    n->setAt(3, entry3);

    // Everything is in its place
    IndexBtree bt(10, NULL);
    EXPECT_EQ(1, bt.findEntryGreater(n, entry0));
    EXPECT_EQ(2, bt.findEntryGreater(n, entry1));
    EXPECT_EQ(3, bt.findEntryGreater(n, entry2));
    EXPECT_EQ(4, bt.findEntryGreater(n, entry3));

    // All the in-betweens also work
    EXPECT_EQ(0, bt.findEntryGreater(n, entry00));
    EXPECT_EQ(1, bt.findEntryGreater(n, entry05));
    EXPECT_EQ(2, bt.findEntryGreater(n, entry15));
    EXPECT_EQ(3, bt.findEntryGreater(n, entry25));
    EXPECT_EQ(4, bt.findEntryGreater(n, entry35));
}

TEST_F(BtreeTest, erase_one_rootOnly) {
    IndexBtree bt(tableId, &objectManager);
    EXPECT_FALSE(bt.erase({"Hello", 120}));

    // Fill up the root and erase
    std::vector<std::string> keys;
    std::vector<BtreeEntry> entries;
    generateKeysInRange(0, IndexBtree::innerslotmax, keys, entries, 4);
    for(uint16_t i = 0; i < IndexBtree::innerslotmax; i++) {
        bt.insert(entries[i]);
    }
    EXPECT_EQ((uint64_t)(IndexBtree::innerslotmax), bt.size());

    // Fake erase with different pkHash
    BtreeEntry modPk = entries[0];
    modPk.pKHash = 100;
    EXPECT_FALSE(bt.erase(modPk));

    // erase one at either ends, and then in the middle
    EXPECT_TRUE(bt.erase(entries[0]));
    EXPECT_EQ((uint64_t)(IndexBtree::innerslotmax - 1), bt.size());

    EXPECT_FALSE(bt.erase(entries[0]));
    EXPECT_EQ((uint64_t)(IndexBtree::innerslotmax - 1), bt.size());

    EXPECT_TRUE(bt.erase(entries[IndexBtree::innerslotmax - 1]));
    EXPECT_EQ((uint64_t)(IndexBtree::innerslotmax - 2), bt.size());

    EXPECT_TRUE(bt.erase(entries[1]));
    EXPECT_EQ((uint64_t)(IndexBtree::innerslotmax - 3), bt.size());

    // Check that everything was erased
    uint16_t i = 2;
    IndexBtree::iterator it = bt.begin();
    while (it != bt.end()) {
        EXPECT_EQ(entries[i].pKHash, it->pKHash);
        EXPECT_EQ(entries[i].keyLength, it->keyLength);
        EXPECT_STREQ((const char*)entries[i].key,
                string((const char*)it->key, it->keyLength).c_str());
        ++i;
        ++it;
    }

    uint16_t slots = IndexBtree::innerslotmax - 1; // -1 for the end deletion
    EXPECT_EQ(slots, i);
}

TEST_F(BtreeTest, erase_one_level1Merge) {
    std::vector<std::string> keys;
    std::vector<BtreeEntry> entries;
    IndexBtree bt(tableId, &objectManager);

    uint16_t slots = 2*IndexBtree::innerslotmax + 1;
    generateKeysInRange(0, slots, keys, entries, 25);

    // This test only works when the innerslots is >= 4 because it will attempt
    // to create 4 subnodes under the root.
    assert(4 <= IndexBtree::innerslotmax);

    for (uint16_t i = 0; i < slots; i++) {
        bt.insert(entries[i]);
        ASSERT_EQ("", bt.verify());
    }

    // Right now, the root should have, in order, 3 children that are at half
    // capacity and 1 that is at half + 1 capacity.
    EXPECT_EQ(1U, bt.m_stats.innernodes);
    EXPECT_EQ(4U, bt.m_stats.leaves);
    EXPECT_EQ("", bt.verify());

    // Delete one from end, leaving all 4 children with half capacity
    EXPECT_TRUE(bt.erase(entries[slots - 1]));
    EXPECT_EQ(1U, bt.m_stats.innernodes);
    EXPECT_EQ(4U, bt.m_stats.leaves);
    entries.pop_back();
    keys.pop_back();
    EXPECT_EQ("", bt.verify());
    slots--;

    // Underflow the last child; last two children to merge
    EXPECT_TRUE(bt.erase(entries[slots - 1]));
    EXPECT_EQ(1U, bt.m_stats.innernodes);
    EXPECT_EQ(3U, bt.m_stats.leaves);
    entries.pop_back();
    keys.pop_back();
    EXPECT_EQ("", bt.verify());
    slots--;

    // underflow the second child, this should cause a rebalance with last child
    uint16_t index = 3*IndexBtree::innerslotmax/4;
    EXPECT_TRUE(bt.erase(entries[index]));
    EXPECT_EQ(1U, bt.m_stats.innernodes);
    EXPECT_EQ(2U, bt.m_stats.leaves);
    entries.erase(entries.begin() + index);
    keys.erase(keys.begin() + index);
    EXPECT_EQ("", bt.verify());
    slots--;

    // Check Remaining Keys
        // Check that everything was erased
    uint16_t i = 0;
    IndexBtree::iterator it = bt.begin();
    while (it != bt.end()) {
        EXPECT_EQ(entries[i].pKHash, it->pKHash);
        EXPECT_EQ(entries[i].keyLength, it->keyLength);
        EXPECT_STREQ((const char*)entries[i].key,
                string((const char*)it->key, it->keyLength).c_str());
        ++i;
        ++it;
    }

    EXPECT_EQ(slots, i);
}

TEST_F(BtreeTest, erase_one_alot) {
    uint16_t slots = IndexBtree::innerslotmax;
    uint32_t numEntries = static_cast<uint32_t>((slots*slots*slots/2));

    std::vector<BtreeEntry> entries;
    std::vector<std::string> entryKeys;
    generateKeysInRange(0, numEntries, entryKeys, entries);

    IndexBtree bt(tableId, &objectManager);
    for (uint32_t i = 0; i < numEntries; i++) {
        bt.insert(entries[i]);
        ASSERT_EQ("", bt.verify());
    }

    srand(0);
    while (entries.size() > 0) {
        uint32_t index = (uint32_t)(rand() %  entries.size());
        BtreeEntry entry = entries[index];
        EXPECT_NE(bt.end(), bt.find(entry));
        EXPECT_TRUE(bt.erase(entry));
        entries.erase(entries.begin() + index);
        EXPECT_EQ(bt.end(), bt.find(entry));
        ASSERT_EQ("", bt.verify());
    }

    EXPECT_EQ(0U, bt.size());

}

TEST_F(BtreeTest, merge_leafPointers) {
    IndexBtree::LeafNode *left, *mid, *right, *farRight;
    NodeId leftId, midId, rightId, farRightId;
    Buffer b1, b2, b3, b4;

    // Btree is only used for its convenient writeNode and merge
    // Does not resemble a real tree in any way.
    IndexBtree bt(tableId, &objectManager);

    left = b1.emplaceAppend<IndexBtree::LeafNode>(&b1);
    mid = b2.emplaceAppend<IndexBtree::LeafNode>(&b2);
    right = b3.emplaceAppend<IndexBtree::LeafNode>(&b3);
    farRight = b4.emplaceAppend<IndexBtree::LeafNode>(&b4);

    farRightId = 10; leftId = 101; midId = 102; rightId = 103;

    fillNodeSorted(left);
    fillNodeSorted(mid);
    fillNodeSorted(right);
    fillNodeSorted(farRight);

    // Link the nodes together.
    left->prevleaf = INVALID_NODEID;
    left->nextleaf = midId;

    mid->prevleaf = leftId;
    mid->nextleaf = rightId;

    right->prevleaf = midId;
    right->nextleaf = farRightId;

    farRight->prevleaf = rightId;
    farRight->nextleaf = INVALID_NODEID;

    bt.writeNode(left, leftId);
    bt.writeNode(mid, midId);
    bt.writeNode(right, rightId);
    bt.writeNode(farRight, farRightId);
    bt.flush();

    bt.merge(mid, right, {});
    bt.flush();

    Buffer out;
    left = static_cast<IndexBtree::LeafNode*>(bt.readNode(leftId, &out));
    right = static_cast<IndexBtree::LeafNode*>(bt.readNode(rightId, &out));
    farRight = static_cast<IndexBtree::LeafNode*>(bt.readNode(farRightId, &out));
}


void resetNode_underflowHelper(IndexBtree::Node *n, uint16_t numEntries) {
    n->slotuse = 0;
    n->keyStorageUsed = 0;
    n->keysBeginOffset = n->keyBuffer->size();

    for (uint16_t i = 0; i < numEntries; i++)
        n->setAt(i, {"Hi!", i});
}

static NodeId createAndWriteNode(IndexBtree &bt, uint16_t numEntries,
                                 Buffer &buff, IndexBtree::Node **ptr = NULL,
                                 uint16_t level = 1)
{
    IndexBtree::Node *node;
    if (level == 0) {
        node = buff.emplaceAppend<IndexBtree::LeafNode>(&buff);
    } else {
        node= buff.emplaceAppend<IndexBtree::InnerNode>(&buff, level);
    }

    resetNode_underflowHelper(node, numEntries);


    NodeId id = bt.writeNode(node);
    assert(bt.objMgr->flushEntriesToLog(&bt.logBuffer, bt.numEntries));

    if (ptr != NULL) {
        *ptr = node;
    }
    return id;
}

void handleUnderflowAndWrite_Helper(IndexBtree &bt, Buffer &buff,
                                IndexBtree::Node **few, NodeId *fewId,
                                IndexBtree::Node **many, NodeId *manyId,
                                IndexBtree::Node **most, NodeId *mostId,
                                IndexBtree::Node **curr, NodeId *currId,
                                IndexBtree::Node **parent)
{
    *fewId = createAndWriteNode(bt, bt.mininnerslots, buff, few);
    *manyId = createAndWriteNode(bt, bt.innerslotmax - 1, buff, many);
    *mostId = createAndWriteNode(bt, bt.innerslotmax, buff, most);
    *currId = createAndWriteNode(bt, bt.mininnerslots - 1, buff, curr);
    *parent = buff.emplaceAppend<IndexBtree::InnerNode>(&buff, uint16_t(10));
}

TEST_F(BtreeTest, handleUnderflowAndWrite_root) {
    IndexBtree bt(tableId, &objectManager);
    BtreeEntry fakeEntry = {"Lalala", 100};
    BtreeEntry wishfulThinking = {"Imma be a root someday!", 777};
    IndexBtree::InnerNode *inner;
    IndexBtree::LeafNode *leaf;
    Buffer buff;

    // leaf root underflow
    leaf = buff.emplaceAppend<IndexBtree::LeafNode>(&buff);
    NodeId rootId = bt.writeNode(leaf);
    bt.m_rootId = rootId;
    bt.flush();
    EXPECT_FALSE(NULL == bt.readNode(rootId, &buff));

    IndexBtree::EraseUpdateInfo emptyLeaf;
    bt.handleUnderflowAndWrite(leaf, rootId, NULL, 0, &emptyLeaf);
    bt.flush();
    EXPECT_TRUE(NULL == bt.readNode(rootId, &buff));

    // Inner root underflow
    inner = buff.emplaceAppend<IndexBtree::InnerNode>(&buff, uint16_t(10));
    bt.m_rootId = ROOT_ID;
    inner->insertAt(0, fakeEntry, 2000, 1000);
    inner->slotuse = 0; // Simulates recent underflow
    leaf->insertAt(0, wishfulThinking);
    bt.writeNode(inner, ROOT_ID);
    bt.writeNode(leaf, 2000U);
    bt.flush();

    IndexBtree::EraseUpdateInfo emptyInner;
    bt.handleUnderflowAndWrite(inner, rootId, NULL, 0, &emptyInner);
    bt.flush();
    EXPECT_TRUE(NULL == bt.readNode(2000U, &buff));
    IndexBtree::LeafNode *newLeaf =
            static_cast<IndexBtree::LeafNode*>(bt.readNode(ROOT_ID, &buff));
    ASSERT_FALSE(NULL == newLeaf);
    EXPECT_EQ(wishfulThinking, newLeaf->getAt(0));

}
TEST_F(BtreeTest, handleUnderflowAndWrite) {
    IndexBtree bt(tableId, &objectManager);
    BtreeEntry fakeEntry = {"Lalala", 100};
    IndexBtree::EraseUpdateInfo info;
    Buffer buff;

    IndexBtree::Node *curr, *few, *many, *most;
    NodeId currId, fewId, manyId, mostId;
    IndexBtree::InnerNode *parent;
    IndexBtree::Node **parentReset = reinterpret_cast<IndexBtree::Node**>(&parent);


    ///////// Case 1 ////////
    // Left doesn't exist and right has few
    info.clearOp();
    handleUnderflowAndWrite_Helper(bt, buff, &few, &fewId, &many, &manyId, &most, &mostId, &curr, &currId, parentReset);
    parent->insertAt(0, fakeEntry, currId, fewId);
    bt.handleUnderflowAndWrite(curr, currId, parent, 0, &info);
    EXPECT_EQ(IndexBtree::EraseUpdateInfo::delCurr, info.op);

    // Left doesn't exist and right has many
    info.clearOp();
    handleUnderflowAndWrite_Helper(bt, buff, &few, &fewId, &many, &manyId, &most, &mostId, &curr, &currId, parentReset);
    parent->insertAt(0, fakeEntry, currId, manyId);
    bt.handleUnderflowAndWrite(curr, currId, parent, 0, &info);
    EXPECT_EQ(IndexBtree::EraseUpdateInfo::setCurr, info.op);

    ///////// Case 2 ////////
    // Right doesn't exist and left is few
    info.clearOp();
    handleUnderflowAndWrite_Helper(bt, buff, &few, &fewId, &many, &manyId, &most, &mostId, &curr, &currId, parentReset);
    parent->insertAt(0, fakeEntry, fewId, currId);
    bt.handleUnderflowAndWrite(curr, currId, parent, 1, &info);
    EXPECT_EQ(IndexBtree::EraseUpdateInfo::delLeft, info.op);

    // Right doesn't exist and left is many
    info.clearOp();
    handleUnderflowAndWrite_Helper(bt, buff, &few, &fewId, &many, &manyId, &most, &mostId, &curr, &currId, parentReset);
    parent->insertAt(0, fakeEntry, manyId, currId);
    bt.handleUnderflowAndWrite(curr, currId, parent, 1, &info);
    EXPECT_EQ(IndexBtree::EraseUpdateInfo::setLeft, info.op);

    ///////// Case 3 ////////
    // Left has few and right has many
    info.clearOp();
    handleUnderflowAndWrite_Helper(bt, buff, &few, &fewId, &many, &manyId, &most, &mostId, &curr, &currId, parentReset);
    parent->insertAt(0, fakeEntry, fewId, currId);
    parent->insertAt(1, fakeEntry, currId, manyId);
    bt.handleUnderflowAndWrite(curr, currId, parent, 1, &info);
    EXPECT_EQ(IndexBtree::EraseUpdateInfo::delLeft, info.op);

    // Left has many and right has few
    info.clearOp();
    handleUnderflowAndWrite_Helper(bt, buff, &few, &fewId, &many, &manyId, &most, &mostId, &curr, &currId, parentReset);
    parent->insertAt(0, fakeEntry, manyId, currId);
    parent->insertAt(1, fakeEntry, currId, fewId);
    bt.handleUnderflowAndWrite(curr, currId, parent, 1, &info);
    EXPECT_EQ(IndexBtree::EraseUpdateInfo::delCurr, info.op);

    // Left has many, but right has more.
    info.clearOp();
    handleUnderflowAndWrite_Helper(bt, buff, &few, &fewId, &many, &manyId, &most, &mostId, &curr, &currId, parentReset);
    parent->insertAt(0, fakeEntry, manyId, currId);
    parent->insertAt(1, fakeEntry, currId, mostId);
    bt.handleUnderflowAndWrite(curr, currId, parent, 1, &info);
    EXPECT_EQ(IndexBtree::EraseUpdateInfo::setCurr, info.op);

    // Left has most and right has many
    info.clearOp();
    handleUnderflowAndWrite_Helper(bt, buff, &few, &fewId, &many, &manyId, &most, &mostId, &curr, &currId, parentReset);
    parent->insertAt(0, fakeEntry, mostId, currId);
    parent->insertAt(1, fakeEntry, currId, manyId);
    bt.handleUnderflowAndWrite(curr, currId, parent, 1, &info);
    EXPECT_EQ(IndexBtree::EraseUpdateInfo::setLeft, info.op);
}

TEST_F(BtreeTest, invariant_BtreeCanOperateWithoutMetadata) {
    uint16_t slots = IndexBtree::innerslotmax;
    uint32_t numEntries = static_cast<uint32_t>(slots*slots*slots);

    std::vector<BtreeEntry> entries;
    std::vector<std::string> entryKeys;
    generateKeysInRange(0, numEntries, entryKeys, entries);

    IndexBtree origBtree(tableId, &objectManager);
    for (uint32_t i = 0; i < numEntries/2; i++)
        origBtree.insert(entries.at(i));

    IndexBtree recoveredBtree(tableId, &objectManager);
    recoveredBtree.setNextNodeId(origBtree.getNextNodeId());

    for (uint32_t i = 0; i < numEntries/2; i++)
        EXPECT_NE(recoveredBtree.end(), recoveredBtree.find(entries.at(i)));

    // Make modifications and make sure nothing breaks
    for (uint32_t i = numEntries/2; i < numEntries; i++)
        recoveredBtree.insert(entries.at(i));

    for (uint32_t i = 0; i < numEntries/4; i++)
        recoveredBtree.erase(entries.at(i));

    // Check everything is reachable.
    for (uint32_t i = 0; i < numEntries/4; i++)
        EXPECT_FALSE(recoveredBtree.exists(entries[i]));

    for (uint32_t i = numEntries/4; i < numEntries; i++)
        EXPECT_TRUE(recoveredBtree.exists(entries[i]));

    IndexBtree::iterator it = recoveredBtree.begin();
    for (uint32_t i = numEntries/4; i < numEntries; i++) {
        ASSERT_EQ(entries[i], *it);
        it++;
    }

    EXPECT_STREQ("", recoveredBtree.verify(false).c_str());
}
}
