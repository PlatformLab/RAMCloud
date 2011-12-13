/* Copyright (c) 2011 Stanford University
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

/**
 * \file
 * Unit tests for BitOps.
 */

#include "TestUtil.h"

#include "BitOps.h"

namespace RAMCloud {

TEST(BitOpsTest, isPowerOfTwo) {
    EXPECT_FALSE(BitOps::isPowerOfTwo(-1));
    EXPECT_FALSE(BitOps::isPowerOfTwo(0));
    EXPECT_TRUE(BitOps::isPowerOfTwo(1));
    EXPECT_TRUE(BitOps::isPowerOfTwo(2));
    EXPECT_TRUE(BitOps::isPowerOfTwo(1UL << 63));

    int count = 0;
    for (int i = 0; i <= 1024; i++) {
        if (BitOps::isPowerOfTwo(i))
            count++;
    }
    EXPECT_EQ(11, count);

    for (uint64_t i = 1; i != 0; i <<= 1)
        EXPECT_TRUE(BitOps::isPowerOfTwo(i));
}

TEST(BitOpsTest, findFirstSet) {
    EXPECT_EQ(0, BitOps::findFirstSet(0));
    EXPECT_EQ(1, BitOps::findFirstSet(1));
    EXPECT_EQ(32, BitOps::findFirstSet(1U << 31));
    EXPECT_EQ(18, BitOps::findFirstSet(0xfff20000U));
    EXPECT_EQ(64, BitOps::findFirstSet(1UL << 63));
    EXPECT_EQ(1, BitOps::findFirstSet(0UL - 1));
}

TEST(BitOpsTest, findLastSet) {
    EXPECT_EQ(0, BitOps::findLastSet(0));
    EXPECT_EQ(1, BitOps::findLastSet(1));
    EXPECT_EQ(32, BitOps::findLastSet(1U << 31));
    EXPECT_EQ(32, BitOps::findLastSet(0xfff20000U));
    EXPECT_EQ(64, BitOps::findLastSet(1UL << 63));
    EXPECT_EQ(64, BitOps::findLastSet(0UL - 1));
}

TEST(BitOpsTest, countBitsSet) {
    EXPECT_EQ(0, BitOps::countBitsSet(0));
    EXPECT_EQ(1, BitOps::countBitsSet(1));
    EXPECT_EQ(2, BitOps::countBitsSet(1U << 31 | 8));
    EXPECT_EQ(1, BitOps::countBitsSet(1UL << 63));
    EXPECT_EQ(2, BitOps::countBitsSet(1UL << 63 | 1));
    EXPECT_EQ(32, BitOps::countBitsSet(~0U));
    EXPECT_EQ(64, BitOps::countBitsSet(~0UL));
}

TEST(BitOpsTest, powerOfTwoGreaterOrEqual) {
    EXPECT_EQ(1, BitOps::powerOfTwoGreaterOrEqual(0));
    EXPECT_EQ(1, BitOps::powerOfTwoGreaterOrEqual(1));
    EXPECT_EQ(2, BitOps::powerOfTwoGreaterOrEqual(2));
    EXPECT_EQ(4, BitOps::powerOfTwoGreaterOrEqual(3));
    EXPECT_EQ(1UL << 63, BitOps::powerOfTwoGreaterOrEqual((1UL << 63) - 1));
    EXPECT_THROW(BitOps::powerOfTwoGreaterOrEqual((1UL << 63) + 1),
        BitOpsException);
    EXPECT_THROW(BitOps::powerOfTwoGreaterOrEqual(0UL - 1), BitOpsException);

    for (int i = 0; i < 50; i++) {
        uint64_t randInt;
        do {
            randInt = generateRandom();
        } while (randInt > (1UL << 63));
        uint64_t powerOfTwo = BitOps::powerOfTwoGreaterOrEqual(randInt);
        EXPECT_TRUE(BitOps::isPowerOfTwo(powerOfTwo));
        EXPECT_GE(powerOfTwo, randInt);
    }
}

TEST(BitOpsTest, powerOfTwoLessOrEqual) {
    EXPECT_EQ(1, BitOps::powerOfTwoLessOrEqual(1));
    EXPECT_EQ(2, BitOps::powerOfTwoLessOrEqual(2));
    EXPECT_EQ(2, BitOps::powerOfTwoLessOrEqual(3));
    EXPECT_EQ(1UL << 62, BitOps::powerOfTwoLessOrEqual((1UL << 63) - 1));
    EXPECT_THROW(BitOps::powerOfTwoLessOrEqual(0), BitOpsException);

    for (int i = 0; i < 50; i++) {
        uint64_t randInt;
        do {
            randInt = generateRandom();
        } while (randInt == 0);
        uint64_t powerOfTwo = BitOps::powerOfTwoLessOrEqual(randInt);
        EXPECT_TRUE(BitOps::isPowerOfTwo(powerOfTwo));
        EXPECT_LE(powerOfTwo, randInt);
    }
}


} // namespace RAMCloud
