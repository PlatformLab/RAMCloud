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
#include "Atomic.h"

namespace RAMCloud {

TEST(AtomicTest, add_int64) {
    Atomic<int64_t> i(0xabcd00000002);
    i.add(0x1000000006);
    EXPECT_EQ(0xabdd00000008, i.load());
    i.add(-2);
    EXPECT_EQ(0xabdd00000006, i.load());
}

TEST(AtomicTest, add_int) {
    Atomic<int> i(2);
    i.add(6);
    EXPECT_EQ(8, i.load());
    i.add(-2);
    EXPECT_EQ(6, i.load());
}

TEST(AtomicTest, add_pointer) {
    Atomic<int*> p(reinterpret_cast<int*>(0xaaaa00000002));
    p.add(0x1000000002);
    EXPECT_EQ(0xaaea0000000aUL, reinterpret_cast<uint64_t>(p.load()));
    p.add(-1);
    EXPECT_EQ(0xaaea00000006UL, reinterpret_cast<uint64_t>(p.load()));
}

TEST(AtomicTest, compareExchange_int64) {
    Atomic<uint64_t> i(0xff00000100000003);

    // First try values that don't match (check differences in both
    // halves of the value to be sure that all 64 bits are being used).
    EXPECT_EQ(0xff00000100000003,
            i.compareExchange(3, 0xff00000100000004));
    EXPECT_EQ(0xff00000100000003, i.load());
    EXPECT_EQ(0xff00000100000003,
            i.compareExchange(3, 0xff00000200000003));
    EXPECT_EQ(0xff00000100000003, i.load());
    EXPECT_EQ(0xff00000100000003,
            i.compareExchange(0xff00000100000003, 0x100020003000));
    EXPECT_EQ(0x100020003000LU, i.load());
}

TEST(AtomicIntTest, compareExchange_int) {
    Atomic<int> i(3);
    EXPECT_EQ(3, i.compareExchange(3, 10));
    EXPECT_EQ(10, i.load());
    EXPECT_EQ(10, i.compareExchange(3, 20));
    EXPECT_EQ(10, i.load());
}

TEST(AtomicTest, exchange) {
    Atomic<int64_t> i(0x0001000200030004);
    EXPECT_EQ(0x0001000200030004, i.exchange(0x000a000b000c000d));
    EXPECT_EQ(0x000a000b000c000d, i.exchange(-5));
    EXPECT_EQ(-5L, i.load());
}

TEST(AtomicTest, inc_int64) {
    Atomic<uint64_t> i(0xffffffff);
    uint64_t prev = i.inc();
    EXPECT_EQ(0xffffffff, prev);
    EXPECT_EQ(0x100000000UL, i.load());
    prev = i.inc(10);
    EXPECT_EQ(0x100000000UL, prev);
    EXPECT_EQ(0x10000000AUL, i.load());
}

TEST(AtomicIntTest, inc_int) {
    Atomic<int> i(2);
    int prev = i.inc();
    EXPECT_EQ(2, prev);
    EXPECT_EQ(3, i.load());
    prev = i.inc(8);
    EXPECT_EQ(3, prev);
    EXPECT_EQ(11, i.load());
    prev = i.inc(-3);
    EXPECT_EQ(11, prev);
    EXPECT_EQ(8, i.load());
}

TEST(AtomicTest, inc_pointer) {
    Atomic<int*> p(reinterpret_cast<int*>(0xaaaa00000002));
    int* prev = p.inc();
    EXPECT_EQ(0xaaaa00000002UL, reinterpret_cast<uint64_t>(prev));
    EXPECT_EQ(0xaaaa00000006UL, reinterpret_cast<uint64_t>(p.load()));
    prev = p.inc(0x1000000002);
    EXPECT_EQ(0xaaaa00000006UL, reinterpret_cast<uint64_t>(prev));
    EXPECT_EQ(0xaaea0000000eUL, reinterpret_cast<uint64_t>(p.load()));
    prev = p.inc(-1);
    EXPECT_EQ(0xaaea0000000eUL, reinterpret_cast<uint64_t>(prev));
    EXPECT_EQ(0xaaea0000000aUL, reinterpret_cast<uint64_t>(p.load()));
}

TEST(AtomicTest, loadStore) {
    Atomic<uint64_t> i(0x0001000200030004);
    EXPECT_EQ(0x0001000200030004UL, i.load());
    i.store(0x000a000b000c000);
    EXPECT_EQ(0x000a000b000c000UL, i.load());
}

TEST(AtomicTest, operatorEquals) {
    Atomic<int64_t> i(3);
    int64_t j = i = 0x0001000200030004;
    EXPECT_EQ(0x0001000200030004, i.load());
    EXPECT_EQ(0x0001000200030004, j);
}

TEST(AtomicTest, operatorValueType) {
    Atomic<uint64_t> i(0x0001000200030004);
    uint64_t j = i;
    EXPECT_EQ(0x0001000200030004UL, j);
}

TEST(AtomicTest, operatorPlusPlus) {
    Atomic<int64_t> i(0x0001000200030004);
    i++;
    EXPECT_EQ(0x0001000200030005, i.load());
    ++i;
    EXPECT_EQ(0x0001000200030006, i.load());
}

TEST(AtomicTest, operatorMinusMinus) {
    Atomic<int64_t> i(0x0001000200030004);
    i--;
    EXPECT_EQ(0x0001000200030003, i.load());
    --i;
    EXPECT_EQ(0x0001000200030002, i.load());
}

struct TestRectangle {
    int x, y, width, height;
};

TEST(AtomicTest, pointerTypes) {
    TestRectangle r1, r2;
    r1.width = 40;
    r2.width = 50;
    Atomic<TestRectangle*> p1, p2;
    p1 = &r1;
    p2 = &r2;
    EXPECT_EQ(40, static_cast<TestRectangle*>(p1)->width);
    p1 = p2;
    EXPECT_EQ(50, static_cast<TestRectangle*>(p1)->width);
    EXPECT_EQ(&r2, static_cast<TestRectangle*>(p1));
}

}  // namespace RAMCloud
