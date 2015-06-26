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
    EXPECT_GT(0, IndexKey::keyCompare("abc", 3, "", 0));
    EXPECT_GT(0, IndexKey::keyCompare("", 0, "", 0));
}

TEST_F(IndexKeyTest, isKeyInRange)
{
    // Simplyfy widely used flags
    IndexKey::IndexKeyRange::BoundaryFlags includeBoth =
                IndexKey::IndexKeyRange::INCLUDE_BOTH;
    IndexKey::IndexKeyRange::BoundaryFlags excludeFirst =
                IndexKey::IndexKeyRange::EXCLUDE_FIRST;
    IndexKey::IndexKeyRange::BoundaryFlags excludeLast =
                IndexKey::IndexKeyRange::EXCLUDE_LAST;
    IndexKey::IndexKeyRange::BoundaryFlags excludeBoth =
                IndexKey::IndexKeyRange::EXCLUDE_BOTH;

    // Construct Object obj.
    uint64_t tableId = 1;
    uint8_t numKeys = 3;

    KeyInfo keyList[3];
    keyList[0].keyLength = 4;
    keyList[0].key = "key0";
    keyList[1].keyLength = 4;
    keyList[1].key = "key1";
    keyList[2].keyLength = 4;
    keyList[2].key = "key2";

    const void* value = "objvalue";
    uint32_t valueLength = 9;

    Buffer keysAndValueBuffer;
    Object::appendKeysAndValueToBuffer(tableId, numKeys, keyList,
            value, valueLength, &keysAndValueBuffer);
    Object obj(tableId, 1, 0, keysAndValueBuffer);

    //////////////////// Test for includeBoth ////////////////////

    // firstKey > key < lastKey
    IndexKey::IndexKeyRange testRange1(1, "key2", 4, "key2", 4, includeBoth);
    EXPECT_FALSE(IndexKey::isKeyInRange(&obj, &testRange1));

    // firstKey < key > lastKey
    IndexKey::IndexKeyRange testRange2(1, "key0", 4, "key0", 4, includeBoth);
    EXPECT_FALSE(IndexKey::isKeyInRange(&obj, &testRange2));

    // firstKey > key > lastKey
    IndexKey::IndexKeyRange testRange3(1, "key2", 4, "key0", 4, includeBoth);
    EXPECT_FALSE(IndexKey::isKeyInRange(&obj, &testRange3));

    // firstKey < key < lastKey
    IndexKey::IndexKeyRange testRange4(1, "key0", 4, "key2", 4, includeBoth);
    EXPECT_TRUE(IndexKey::isKeyInRange(&obj, &testRange4));

    // firstKey = key = lastKey
    IndexKey::IndexKeyRange testRange5(1, "key1", 4, "key1", 4, includeBoth);
    EXPECT_TRUE(IndexKey::isKeyInRange(&obj, &testRange5));

    //////////////////// Test for excludeFirst ////////////////////

    // firstKey > key < lastKey
    IndexKey::IndexKeyRange testRange6(1, "key2", 4, "key2", 4, excludeFirst);
    EXPECT_FALSE(IndexKey::isKeyInRange(&obj, &testRange6));

    // firstKey < key > lastKey
    IndexKey::IndexKeyRange testRange7(1, "key0", 4, "key0", 4, excludeFirst);
    EXPECT_FALSE(IndexKey::isKeyInRange(&obj, &testRange7));

    // firstKey > key > lastKey
    IndexKey::IndexKeyRange testRange8(1, "key2", 4, "key0", 4, excludeFirst);
    EXPECT_FALSE(IndexKey::isKeyInRange(&obj, &testRange8));

    // firstKey < key < lastKey
    IndexKey::IndexKeyRange testRange9(1, "key0", 4, "key2", 4, excludeFirst);
    EXPECT_TRUE(IndexKey::isKeyInRange(&obj, &testRange9));

    // firstKey = key = lastKey
    IndexKey::IndexKeyRange testRange10(1, "key1", 4, "key1", 4, excludeFirst);
    EXPECT_FALSE(IndexKey::isKeyInRange(&obj, &testRange10));

    // frstKey < key = lastKey
    IndexKey::IndexKeyRange testRange11(1, "key0", 4, "key1", 4, excludeFirst);
    EXPECT_TRUE(IndexKey::isKeyInRange(&obj, &testRange11));

    //////////////////// Test for excludeLast ////////////////////

    // firstKey > key < lastKey
    IndexKey::IndexKeyRange testRange12(1, "key2", 4, "key2", 4, excludeLast);
    EXPECT_FALSE(IndexKey::isKeyInRange(&obj, &testRange12));

    // firstKey < key > lastKey
    IndexKey::IndexKeyRange testRange13(1, "key0", 4, "key0", 4, excludeLast);
    EXPECT_FALSE(IndexKey::isKeyInRange(&obj, &testRange13));

    // firstKey > key > lastKey
    IndexKey::IndexKeyRange testRange14(1, "key2", 4, "key0", 4, excludeLast);
    EXPECT_FALSE(IndexKey::isKeyInRange(&obj, &testRange14));

    // firstKey < key < lastKey
    IndexKey::IndexKeyRange testRange15(1, "key0", 4, "key2", 4, excludeLast);
    EXPECT_TRUE(IndexKey::isKeyInRange(&obj, &testRange15));

    // firstKey = key = lastKey
    IndexKey::IndexKeyRange testRange16(1, "key1", 4, "key1", 4, excludeLast);
    EXPECT_FALSE(IndexKey::isKeyInRange(&obj, &testRange16));

    // firstKey = key < lastKey
    IndexKey::IndexKeyRange testRange17(1, "key1", 4, "key2", 4, excludeLast);
    EXPECT_TRUE(IndexKey::isKeyInRange(&obj, &testRange17));

    //////////////////// Test for excludeBoth ////////////////////

    // firstKey > key < lastKey
    IndexKey::IndexKeyRange testRange18(1, "key2", 4, "key2", 4, excludeBoth);
    EXPECT_FALSE(IndexKey::isKeyInRange(&obj, &testRange18));

    // firstKey < key > lastKey
    IndexKey::IndexKeyRange testRange19(1, "key0", 4, "key0", 4, excludeBoth);
    EXPECT_FALSE(IndexKey::isKeyInRange(&obj, &testRange19));

    // firstKey > key > lastKey
    IndexKey::IndexKeyRange testRange20(1, "key2", 4, "key0", 4, excludeBoth);
    EXPECT_FALSE(IndexKey::isKeyInRange(&obj, &testRange20));

    // firstKey < key < lastKey
    IndexKey::IndexKeyRange testRange21(1, "key0", 4, "key2", 4, excludeBoth);
    EXPECT_TRUE(IndexKey::isKeyInRange(&obj, &testRange21));

    // firstKey = key = lastKey
    IndexKey::IndexKeyRange testRange22(1, "key1", 4, "key1", 4, excludeBoth);
    EXPECT_FALSE(IndexKey::isKeyInRange(&obj, &testRange22));

    // firstKey = key < lastKey
    IndexKey::IndexKeyRange testRange23(1, "key1", 4, "key2", 4, excludeBoth);
    EXPECT_FALSE(IndexKey::isKeyInRange(&obj, &testRange23));

    // firstKey < key = lastKey
    IndexKey::IndexKeyRange testRange24(1, "key0", 4, "key1", 4, excludeBoth);
    EXPECT_FALSE(IndexKey::isKeyInRange(&obj, &testRange24));
}

}  // namespace RAMCloud
