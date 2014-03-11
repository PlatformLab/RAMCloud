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
#include "IndexletManager.h"
#include "RamCloud.h"
#include "StringUtil.h"

namespace RAMCloud {

class IndexletManagerTest : public ::testing::Test {
  public:
    Context context;
    IndexletManager im;

    IndexletManagerTest()
        : context()
        , im(&context)
    {
    }
    DISALLOW_COPY_AND_ASSIGN(IndexletManagerTest);
};

TEST_F(IndexletManagerTest, constructor) {
    EXPECT_EQ(0U, im.indexletMap.size());
}

TEST_F(IndexletManagerTest, addIndexlet) {
    string key1 = "a";
    string key2 = "c";
    string key3 = "f";
    string key4 = "k";
    string key5 = "u";

    EXPECT_TRUE(im.addIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));
    EXPECT_FALSE(im.addIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_TRUE(im.addIndexlet(0, 0, key1.c_str(),
        (uint16_t)key1.length(), key2.c_str(), (uint16_t)key2.length()));

    EXPECT_TRUE(im.addIndexlet(0, 0, key4.c_str(),
        (uint16_t)key4.length(), key5.c_str(), (uint16_t)key5.length()));

    EXPECT_FALSE(im.addIndexlet(0, 0, key1.c_str(),
        (uint16_t)key1.length(), key3.c_str(), (uint16_t)key3.length()));

    SpinLock lock;
    IndexletManager::Lock fakeGuard(lock);
    IndexletManager::Indexlet* indexlet = &im.lookup(0, 0, key2.c_str(),
        (uint16_t)key2.length(), fakeGuard)->second;
    string firstKey = StringUtil::binaryToString(
                        indexlet->firstKey, indexlet->firstKeyLength);
    string firstNotOwnedKey = StringUtil::binaryToString(
                indexlet->firstNotOwnedKey, indexlet->firstNotOwnedKeyLength);

    EXPECT_EQ(0, firstKey.compare("c"));
    EXPECT_EQ(0, firstNotOwnedKey.compare("k"));
}

TEST_F(IndexletManagerTest, getIndexlet) {
    string key1 = "a";
    string key2 = "c";
    string key3 = "f";
    string key4 = "k";
    string key5 = "u";

    EXPECT_FALSE(im.getIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    im.addIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length());

    EXPECT_TRUE(im.getIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_FALSE(im.getIndexlet(1, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_FALSE(im.getIndexlet(0, 1, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_FALSE(im.getIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key3.c_str(), (uint16_t)key3.length()));

    EXPECT_FALSE(im.getIndexlet(0, 0, key3.c_str(),
        (uint16_t)key3.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_FALSE(im.getIndexlet(0, 0, key1.c_str(),
        (uint16_t)key1.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_FALSE(im.getIndexlet(0, 1, key2.c_str(),
        (uint16_t)key2.length(), key5.c_str(), (uint16_t)key5.length()));

    EXPECT_FALSE(im.getIndexlet(0, 1, key1.c_str(),
        (uint16_t)key1.length(), key5.c_str(), (uint16_t)key5.length()));

    IndexletManager::Indexlet* indexlet =
                    im.getIndexlet(0, 0, key2.c_str(), (uint16_t)key2.length(),
                                        key4.c_str(), (uint16_t)key4.length());
    EXPECT_TRUE(indexlet);
    string firstKey = StringUtil::binaryToString(
                        indexlet->firstKey, indexlet->firstKeyLength);
    string firstNotOwnedKey = StringUtil::binaryToString(
                indexlet->firstNotOwnedKey, indexlet->firstNotOwnedKeyLength);

    EXPECT_EQ(0, firstKey.compare("c"));
    EXPECT_EQ(0, firstNotOwnedKey.compare("k"));
}

TEST_F(IndexletManagerTest, deleteIndexlet) {
    string key1 = "a";
    string key2 = "c";
    string key3 = "f";
    string key4 = "k";
    string key5 = "u";

    //TODO(ashgup): add comments on each individual test
    EXPECT_FALSE(im.getIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    im.addIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length());

    EXPECT_TRUE(im.getIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_TRUE(im.deleteIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_FALSE(im.getIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_FALSE(im.deleteIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    im.addIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length());

    EXPECT_FALSE(im.deleteIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key5.c_str(), (uint16_t)key5.length()));

    EXPECT_FALSE(im.deleteIndexlet(0, 0, key1.c_str(),
        (uint16_t)key1.length(), key4.c_str(), (uint16_t)key4.length()));
}

TEST_F(IndexletManagerTest, isKeyInRange)
{
    // Construct Object obj.
    uint64_t tableId = 1;
    uint8_t numKeys = 3;

    KeyInfo keyList[3];
    keyList[0].keyLength = 8;
    keyList[0].key = "objkey0";
    keyList[1].keyLength = 8;
    keyList[1].key = "objkey1";
    keyList[2].keyLength = 8;
    keyList[2].key = "objkey2";

    const void* value = "objvalue";
    uint32_t valueLength = 9;

    Buffer keysAndValueBuffer;
    Object::appendKeysAndValueToBuffer(tableId, numKeys, keyList,
                                       value, valueLength, keysAndValueBuffer);

    Object obj(tableId, 1, 0, keysAndValueBuffer);

    // Compare key for key index 1 of obj (called: "key") for different cases.

    // Case0: firstKeyLength = keyLength = lastKeyLength and
    // firstKey > key < lastKey
    IndexletManager::KeyRange testRange0 = {1, "objkey2", 8, "objkey2", 8};
    bool isInRange0 = IndexletManager::isKeyInRange(&obj, &testRange0);
    EXPECT_FALSE(isInRange0);

    // Case1: firstKeyLength = keyLength = lastKeyLength and
    // firstKey < key > lastKey
    IndexletManager::KeyRange testRange1 = {1, "objkey0", 8, "objkey0", 8};
    bool isInRange1 = IndexletManager::isKeyInRange(&obj, &testRange1);
    EXPECT_FALSE(isInRange1);

    // Case2: firstKeyLength = keyLength = lastKeyLength and
    // firstKey > key > lastKey
    IndexletManager::KeyRange testRange2 = {1, "objkey2", 8, "objkey0", 8};
    bool isInRange2 = IndexletManager::isKeyInRange(&obj, &testRange2);
    EXPECT_FALSE(isInRange2);

    // Case3: firstKeyLength = keyLength = lastKeyLength and
    // firstKey < key < lastKey
    IndexletManager::KeyRange testRange3 = {1, "objkey0", 8, "objkey2", 8};
    bool isInRange3 = IndexletManager::isKeyInRange(&obj, &testRange3);
    EXPECT_TRUE(isInRange3);

    // Case4: firstKeyLength = keyLength = lastKeyLength and
    // firstKey = key = lastKey
    IndexletManager::KeyRange testRange4 = {1, "objkey1", 8, "objkey1", 8};
    bool isInRange4 = IndexletManager::isKeyInRange(&obj, &testRange4);
    EXPECT_TRUE(isInRange4);

    // Case5: firstKeyLength < keyLength < lastKeyLength and
    // firstKey = key substring and key = lastKey substring
    IndexletManager::KeyRange testRange5 = {1, "obj", 4, "objkey11", 9};
    bool isInRange5 = IndexletManager::isKeyInRange(&obj, &testRange5);
    EXPECT_TRUE(isInRange5);

    // Case6: firstKeyLength > keyLength < lastKeyLength and
    // firstKey substring = key = lastKey substring
    IndexletManager::KeyRange testRange6 = {1, "objkey11", 9, "objkey11", 9};
    bool isInRange6 = IndexletManager::isKeyInRange(&obj, &testRange6);
    EXPECT_FALSE(isInRange6);

    // Case7: firstKeyLength < keyLength > lastKeyLength and
    // firstKey = key substring = lastKey
    IndexletManager::KeyRange testRange7 = {1, "obj", 4, "obj", 4};
    bool isInRange7 = IndexletManager::isKeyInRange(&obj, &testRange7);
    EXPECT_FALSE(isInRange7);
}

TEST_F(IndexletManagerTest, keyCompare)
{
    EXPECT_EQ(0, IndexletManager::keyCompare("abc", 3, "abc", 3));
    EXPECT_GT(0, IndexletManager::keyCompare("abb", 3, "abc", 3));
    EXPECT_LT(0, IndexletManager::keyCompare("abd", 3, "abc", 3));
    EXPECT_GT(0, IndexletManager::keyCompare("ab", 2, "abc", 3));
    EXPECT_LT(0, IndexletManager::keyCompare("abcd", 4, "abc", 3));
    EXPECT_GT(0, IndexletManager::keyCompare("abbc", 4, "abc", 3));
    EXPECT_LT(0, IndexletManager::keyCompare("ac", 2, "abc", 3));
}

}  // namespace RAMCloud
