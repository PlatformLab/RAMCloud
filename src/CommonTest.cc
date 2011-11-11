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
#include "Common.h"

namespace RAMCloud {

class CommonTest : public ::testing::Test {
  public:
    CommonTest() {}

    DISALLOW_COPY_AND_ASSIGN(CommonTest);
};

TEST_F(CommonTest, unsafeArrayLength) {
    int x[10];
    int y[] = {1, 2, 3};
    EXPECT_EQ(10U, unsafeArrayLength(x));
    EXPECT_EQ(3U, unsafeArrayLength(y));

    // The following will give bogus results:
    // int *z;
    // EXPECT_EQ(0U, arrayLength(z));
}

TEST_F(CommonTest, arrayLength) {
    int x[10];
    int y[] = {1, 2, 3};
    EXPECT_EQ(10U, arrayLength(x));
    EXPECT_EQ(3U, arrayLength(y));

    // The following should not compile:
    // int *z;
    // EXPECT_EQ(0U, arrayLength(z));
}

TEST_F(CommonTest, format_basic) {
    EXPECT_EQ("rofl3", format("rofl3"));
    EXPECT_EQ("rofl3", format("r%sl%d", "of", 3));
}

TEST_F(CommonTest, format_long) {
    char x[3000];
    memset(x, 0xcc, sizeof(x));
    x[sizeof(x) - 1] = '\0';
    EXPECT_EQ(x, format("%s", x));
}


// make sure generateRandom() uses all 64 bits
TEST_F(CommonTest, generateRandom) {
    uint64_t r = 0;
    for (uint32_t i = 0; i < 50; i++)
        r |= generateRandom();
    EXPECT_EQ(~0UL, r);
}

TEST_F(CommonTest, generateRandom_lowOrderIsOneSometimes) {
    for (uint32_t c = 0; c < 100000; ++c) {
        uint8_t r = static_cast<uint8_t>(generateRandom());
        if (r == 1)
            return;
    }
    FAIL();
}

TEST_F(CommonTest, getTotalSystemMemory) {
    // for portability, only test if /proc/meminfo exists
    FILE *fp = fopen("/proc/meminfo", "r");
    if (fp == NULL)
        return;
    fclose(fp);

    // seems reasonable?
    EXPECT_TRUE(getTotalSystemMemory() > 1024 * 1024);
}

TEST(CodeLocation, relativeFile) {
    CodeLocation where = HERE;
    EXPECT_EQ("src/CommonTest.cc", where.relativeFile());

    where.file = "CommonTest.cc";
    EXPECT_EQ(where.file, where.relativeFile());

    where.file = "/strange/path/to/ramcloud/src/CommonTest.cc";
    EXPECT_EQ(where.file, where.relativeFile());
}

TEST(CodeLocation, qualifiedFunction) {
    CodeLocation where("", 0, "", "");

    where.function = "func";
    where.prettyFunction = "std::string RAMCloud::CommonTest::func()";
    EXPECT_EQ("CommonTest::func", where.qualifiedFunction());

    where.function = "func";
    where.prettyFunction = "std::string RAMCloud::CommonTest::func("
                            "const RAMCloud::CodeLocation&) const";
    EXPECT_EQ("CommonTest::func", where.qualifiedFunction());

    where.function = "func";
    where.prettyFunction = "static std::string RAMCloud::CommonTest::func("
                            "const RAMCloud::CodeLocation&)";
    EXPECT_EQ("CommonTest::func", where.qualifiedFunction());

    where.function = "func";
    where.prettyFunction = "void RAMCloud::func(void (*)(int))";
    EXPECT_EQ("func", where.qualifiedFunction());

    where.prettyFunction = "void (* RAMCloud::func())(int)";
    EXPECT_EQ("func", where.qualifiedFunction());

    where.prettyFunction = "void (* RAMCloud::func(void (*)(int)))(int)";
    EXPECT_EQ("func", where.qualifiedFunction());

    where.prettyFunction = "void (* (* RAMCloud::func(void (* (*)(void (*)"
                           "(int), void (*)(int)))(int), void (* (*)(void (*)"
                           "(int), void (*)(int)))(int)))(void (*)(int), void "
                           "(*)(int)))(int)";
    EXPECT_EQ("func", where.qualifiedFunction());
}

}  // namespace RAMCloud
