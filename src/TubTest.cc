/* Copyright (c) 2010 Stanford University
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
#include "Tub.h"

namespace RAMCloud {

static_assert(__alignof__(Tub<char>) == 1,
              "Alignment of Tub<char> is wrong");
static_assert(__alignof__(Tub<uint64_t>) == 8,
              "Alignment of Tub<uint64_t> is wrong");

struct Foo {
    Foo(int x, int y, int z = 0)
        : x(x) , y(y) , z(z)
    {
        ++liveCount;
    }
    ~Foo() {
        --liveCount;
    }
    int getX() {
        return x;
    }
    int x, y, z;
    static int liveCount;
};
int Foo::liveCount = 0;

struct Bar {
    Bar() : x(123456789) {}
    Bar& operator=(const Bar& other) {
        if (this == &other)
            return *this;
        EXPECT_EQ(123456789, x);
        x = other.x;
        EXPECT_EQ(123456789, x);
        return *this;
    }
    int x;
};

typedef Tub<Foo> FooTub;
typedef Tub<int> IntTub;

TEST(Tub, basics) {
    {
        FooTub fooTub;
        EXPECT_FALSE(fooTub);
        EXPECT_FALSE(fooTub.get());
        Foo* foo = fooTub.construct(1, 2, 3);
        EXPECT_TRUE(fooTub);
        EXPECT_EQ(1, foo->x);
        EXPECT_EQ(2, foo->y);
        EXPECT_EQ(3, foo->z);
        EXPECT_EQ(foo, fooTub.get());
        EXPECT_EQ(foo, &*fooTub);
        EXPECT_EQ(1, fooTub->getX());

        EXPECT_EQ(foo, fooTub.construct(5, 6));
        EXPECT_EQ(5, foo->x);
        EXPECT_EQ(6, foo->y);
        EXPECT_EQ(0, foo->z);
    }
    EXPECT_EQ(0, Foo::liveCount);
}

TEST(Tub, copyAndAssign) {
    IntTub x;
    x.construct(5);
    IntTub y;
    y = x;
    IntTub z(y);
    EXPECT_EQ(5, *y);
    EXPECT_EQ(5, *z);

    int p = 5;
    IntTub q(p);
    EXPECT_EQ(5, *q);

    Tub<Bar> b1;
    Tub<Bar> b2;
    b2.construct();
    // Assignment to a tub with an unconstructed value needs
    // to use the copy constructor instead of assignment operator.
    b1 = b2;
}

TEST(Tub, putInVector) {
    vector<IntTub> v;
    v.push_back(IntTub());
    IntTub eight;
    eight.construct(8);
    v.push_back(eight);
    v.insert(v.begin(), IntTub());
    EXPECT_FALSE(v[0]);
    EXPECT_FALSE(v[1]);
    EXPECT_TRUE(v[2]);
    EXPECT_EQ(static_cast<void*>(&v[2]), static_cast<void*>(v[2].get()));
    EXPECT_EQ(8, *v[2]);
}

TEST(Tub, boolConversion) {
    IntTub x;
    EXPECT_FALSE(x);
    x.construct(5);
    EXPECT_TRUE(x);
}

}  // namespace RAMCloud
