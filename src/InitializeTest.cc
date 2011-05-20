/* Copyright (c) 2010-2011 Stanford University
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
#include "Initialize.h"

namespace RAMCloud {

int count;
void incCount() {
    count++;
}

TEST(InitializeTest, invokeFunction) {
    count = 0;
    Initialize foo1(incCount);
    EXPECT_EQ(1, count);
    Initialize foo2(incCount);
    EXPECT_EQ(2, count);
}

class Number {
  public:
    Number() : count(44) { }
    int count;
};
Number *num;

TEST(InitializeTest, setPointer) {
    num = NULL;
    Initialize foo1(num);
    EXPECT_TRUE(num != NULL);
    EXPECT_EQ(44, num->count);
    num->count = 33;
    Initialize foo2(num);
    EXPECT_EQ(33, num->count);
}

}  // namespace RAMCloud
