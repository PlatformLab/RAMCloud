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

#include <array>

#include "TestUtil.h"
#include "VarLenArray.h"

namespace RAMCloud {

namespace {

struct Foo {
    Foo()
        : id(counter++)
    {
        if (throwOn == id) {
            RAMCLOUD_TEST_LOG("throw %u", id);
            throw 0;
        } else {
            RAMCLOUD_TEST_LOG("construct %u", id);
        }
    }
    ~Foo() {
        RAMCLOUD_TEST_LOG("destroy %u", id);
    }
    uint32_t id;

    static void reset() {
        counter = 0;
        throwOn = ~0U;
    }
    static uint32_t counter;
    static uint32_t throwOn;
};

uint32_t Foo::counter;
uint32_t Foo::throwOn;

typedef std::array<Foo, 4> RegularArray;

template<typename ElementType>
struct StackVarLenArray {
    StackVarLenArray() : array(4) {}
    VarLenArray<ElementType> array;
    char backing[sizeof(ElementType) * 4];
};

typedef StackVarLenArray<Foo> StackFooArray;

struct KiloByteAligned { } __attribute__((aligned (1024)));

} // anonymous namespace

TEST(VarLenArrayTest, constructorDestructorNormal) {
    string regularArrayLog = ({
        TestLog::Enable _;
        Foo::reset();
        EXPECT_NO_THROW(RegularArray x);
        TestLog::get();
    });
    string varLenArrayLog = ({
        TestLog::Enable _;
        Foo::reset();
        EXPECT_NO_THROW(StackFooArray x);
        TestLog::get();
    });

    EXPECT_EQ(regularArrayLog, varLenArrayLog);
}

TEST(VarLenArrayTest, constructorException) {
    string regularArrayLog = ({
        TestLog::Enable _;
        Foo::reset();
        Foo::throwOn = 2;
        EXPECT_THROW(RegularArray x, int);
        TestLog::get();
    });
    string varLenArrayLog = ({
        TestLog::Enable _;
        Foo::reset();
        Foo::throwOn = 2;
        EXPECT_THROW(StackFooArray x, int);
        TestLog::get();
    });

    EXPECT_EQ(regularArrayLog, varLenArrayLog);
}

TEST(VarLenArrayTest, operatorBrackets) {
    Foo::reset();
    StackFooArray x;
    for (uint32_t i = 0; i < 4; ++i)
        EXPECT_EQ(i, x.array[i].id);
}

TEST(VarLenArrayTest, iteration) {
    Foo::reset();
    StackFooArray x;
    uint32_t i = 0;
    foreach (Foo& f, x.array)
        EXPECT_EQ(i++, f.id);
}

TEST(VarLenArrayTest, alignment) {
    StackVarLenArray<KiloByteAligned> x;
    EXPECT_EQ(reinterpret_cast<uint64_t>(&x.array) + 1024,
              reinterpret_cast<uint64_t>(&x.array[0]));
    EXPECT_EQ(static_cast<void*>(x.backing),
              &x.array[0]);
};

}  // namespace RAMCloud
