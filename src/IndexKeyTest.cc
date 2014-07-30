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
#include "IndexKey.h"
#include "RamCloud.h"
#include "StringUtil.h"

namespace RAMCloud {

class IndexKeyTest : public ::testing::Test {
  public:
    IndexKeyTest() {}
    DISALLOW_COPY_AND_ASSIGN(IndexKeyTest);
};

TEST_F(IndexKeyTest, keyCompare)
{
    EXPECT_EQ(0, IndexKey::keyCompare("abc", 3, "abc", 3));
    EXPECT_GT(0, IndexKey::keyCompare("abb", 3, "abc", 3));
    EXPECT_LT(0, IndexKey::keyCompare("abd", 3, "abc", 3));
    EXPECT_GT(0, IndexKey::keyCompare("ab", 2, "abc", 3));
    EXPECT_LT(0, IndexKey::keyCompare("abcd", 4, "abc", 3));
    EXPECT_GT(0, IndexKey::keyCompare("abbc", 4, "abc", 3));
    EXPECT_LT(0, IndexKey::keyCompare("ac", 2, "abc", 3));
    EXPECT_GT(0, IndexKey::keyCompare("", 0, "abc", 3));
    EXPECT_LT(0, IndexKey::keyCompare("abc", 3, "", 0));
}

TEST_F(IndexKeyTest, isKeyInRange)
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
                                       value, valueLength, &keysAndValueBuffer);

    Object obj(tableId, 1, 0, keysAndValueBuffer);

    // Note: If isKeyInRange() didn't use keyCompare() then we'd have to
    // test many more cases here.

    // Case0: firstKey > key < lastKey
    IndexKey::IndexKeyRange testRange0 = {1, "objkey2", 8, "objkey2", 8};
    bool isInRange0 = IndexKey::isKeyInRange(&obj, &testRange0);
    EXPECT_FALSE(isInRange0);

    // Case1: firstKey < key > lastKey
    IndexKey::IndexKeyRange testRange1 = {1, "objkey0", 8, "objkey0", 8};
    bool isInRange1 = IndexKey::isKeyInRange(&obj, &testRange1);
    EXPECT_FALSE(isInRange1);

    // Case2: firstKey > key > lastKey
    IndexKey::IndexKeyRange testRange2 = {1, "objkey2", 8, "objkey0", 8};
    bool isInRange2 = IndexKey::isKeyInRange(&obj, &testRange2);
    EXPECT_FALSE(isInRange2);

    // Case3: firstKey < key < lastKey
    IndexKey::IndexKeyRange testRange3 = {1, "objkey0", 8, "objkey2", 8};
    bool isInRange3 = IndexKey::isKeyInRange(&obj, &testRange3);
    EXPECT_TRUE(isInRange3);

    // Case4: firstKey = key = lastKey
    IndexKey::IndexKeyRange testRange4 = {1, "objkey1", 8, "objkey1", 8};
    bool isInRange4 = IndexKey::isKeyInRange(&obj, &testRange4);
    EXPECT_TRUE(isInRange4);
}

}  // namespace RAMCloud
