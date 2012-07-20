/* Copyright (c) 2012 Stanford University
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

#include "Key.h"

namespace RAMCloud {

/**
 * Unit tests for Key.
 */
class KeyTest : public ::testing::Test {
  public:
    KeyTest()
    {
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(KeyTest);
};

TEST_F(KeyTest, constructor)
{
}

/*
 * Ensure that #RAMCloud::HashTable::hash() generates hashes using the full
 * range of bits.
 */
TEST_F(KeyTest, getHash_UsesAllBits) {
    uint64_t observedBits = 0UL;
    srand(1);
    for (uint32_t i = 0; i < 50; i++) {
        uint64_t input1 = generateRandom();
        uint64_t input2 = generateRandom();
        observedBits |= Key::getHash(input1, &input2, sizeof(input2));
    }
    EXPECT_EQ(~0UL, observedBits);
}

} // namespace RAMCloud
