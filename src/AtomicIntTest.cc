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

#include "TestUtil.h"
#include "AtomicInt.h"

namespace RAMCloud {

TEST(AtomicIntTest, add) {
    AtomicInt i(3);
    i.add(6);
    EXPECT_EQ(9, i.load());
    i.add(-2);
    EXPECT_EQ(7, i.load());
}

TEST(AtomicIntTest, compareExchange) {
    AtomicInt i(3);
    EXPECT_EQ(3, i.compareExchange(3, 10));
    EXPECT_EQ(10, i.load());
    EXPECT_EQ(10, i.compareExchange(3, 20));
    EXPECT_EQ(10, i.load());
}

TEST(AtomicIntTest, exchange) {
    AtomicInt i(3);
    EXPECT_EQ(3, i.exchange(10));
    EXPECT_EQ(10, i.exchange(20));
    EXPECT_EQ(20, i.load());
}

TEST(AtomicIntTest, inc) {
    AtomicInt i(3);
    i.inc();
    EXPECT_EQ(4, i.load());
    i.inc();
    EXPECT_EQ(5, i.load());
}

TEST(AtomicIntTest, loadStore) {
    AtomicInt i(3);
    EXPECT_EQ(3, i.load());
    i.store(1234);
    EXPECT_EQ(1234, i.load());
}

TEST(AtomicIntTest, operatorEquals) {
    AtomicInt i(3);
    int j = i = 44;
    EXPECT_EQ(44, i.load());
    EXPECT_EQ(44, j);
}

TEST(AtomicIntTest, operatorInt) {
    AtomicInt i(3);
    int j = i;
    EXPECT_EQ(3, j);
}

TEST(AtomicIntTest, operatorPlusPlus) {
    AtomicInt i(3);
    i++;
    EXPECT_EQ(4, i.load());
    ++i;
    EXPECT_EQ(5, i.load());
}

TEST(AtomicIntTest, operatorMinusMinus) {
    AtomicInt i(3);
    i--;
    EXPECT_EQ(2, i.load());
    --i;
    EXPECT_EQ(1, i.load());
}

}  // namespace RAMCloud
