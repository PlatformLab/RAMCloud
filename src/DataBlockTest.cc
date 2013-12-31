/* Copyright (c) 2013 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.xx
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"
#include "DataBlock.h"

namespace RAMCloud {
class DataBlockTest : public ::testing::Test {
  public:
    DataBlock block;

    DataBlockTest()
        : block()
    {}
    ~DataBlockTest()
    {}

  private:
    DISALLOW_COPY_AND_ASSIGN(DataBlockTest);
};

TEST_F(DataBlockTest, testEverything) {
    Buffer output;
    EXPECT_EQ("", TestUtil::toString(&output));

    block.set("abc", 3);
    block.get(&output);
    EXPECT_EQ("abc", TestUtil::toString(&output));

    block.set("12345", 5);
    block.get(&output);
    EXPECT_EQ("12345", TestUtil::toString(&output));

    block.set(NULL, 5);
    block.get(&output);
    EXPECT_EQ("", TestUtil::toString(&output));

    block.set("0123456789", 10);
    block.get(&output);
    EXPECT_EQ("0123456789", TestUtil::toString(&output));
}


}  // namespace RAMCloud
