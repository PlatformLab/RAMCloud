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

class CommonTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(CommonTest);
    CPPUNIT_TEST(test_unsafeArrayLength);
    CPPUNIT_TEST(test_arrayLength);
    CPPUNIT_TEST(test_vformat_long);
    CPPUNIT_TEST(test_format_copy);
    CPPUNIT_TEST(test_format_outArg);
    CPPUNIT_TEST(test_generateRandom);
    CPPUNIT_TEST(test_getTotalSystemMemory);
    CPPUNIT_TEST_SUITE_END();

  public:
    CommonTest() {}

    void test_unsafeArrayLength() {
        int x[10];
        int y[] = {1, 2, 3};
        CPPUNIT_ASSERT_EQUAL(10, unsafeArrayLength(x));
        CPPUNIT_ASSERT_EQUAL(3, unsafeArrayLength(y));

        // The following will give bogus results:
        // int *z;
        // CPPUNIT_ASSERT_EQUAL(0, arrayLength(z));
    }

    void test_arrayLength() {
        int x[10];
        int y[] = {1, 2, 3};
        CPPUNIT_ASSERT_EQUAL(10, arrayLength(x));
        CPPUNIT_ASSERT_EQUAL(3, arrayLength(y));

        // The following should not compile:
        // int *z;
        // CPPUNIT_ASSERT_EQUAL(0, arrayLength(z));
    }

    void test_vformat_long() {
        char x[3000];
        memset(x, 0xcc, sizeof(x));
        x[sizeof(x) - 1] = '\0';
        CPPUNIT_ASSERT_EQUAL(x, format("%s", x));
    }

    void test_format_copy() {
        CPPUNIT_ASSERT_EQUAL("rofl3",
                             format("rofl3"));
        CPPUNIT_ASSERT_EQUAL("rofl3",
                             format("r%sl%d", "of", 3));
    }

    void test_format_outArg() {
        string s;
        CPPUNIT_ASSERT_EQUAL("rofl3", format(s, "rofl3"));
        CPPUNIT_ASSERT_EQUAL(&s, &format(s, "r%sl%d", "of", 3));
        CPPUNIT_ASSERT_EQUAL("rofl3", s);
    }

    // make sure generateRandom() uses all 64 bits
    void test_generateRandom() {
        uint64_t r = 0;
        for (uint32_t i = 0; i < 50; i++)
            r |= generateRandom();
        CPPUNIT_ASSERT_EQUAL(~0UL, r);
    }

    void test_getTotalSystemMemory() {
        // for portability, only test if /proc/meminfo exists
        FILE *fp = fopen("/proc/meminfo", "r");
        if (fp == NULL)
            return;
        fclose(fp);

        // seems reasonable?
        CPPUNIT_ASSERT(getTotalSystemMemory() > 1024 * 1024);
    }

    DISALLOW_COPY_AND_ASSIGN(CommonTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(CommonTest);

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

TEST(isPowerOfTwo, simple) {
    for (uint64_t i = 1; i != 0; i <<= 1)
        EXPECT_TRUE(isPowerOfTwo(i));

    int total = 0;
    for (uint64_t i = 0; i <= 1024; i++) {
        if (isPowerOfTwo(i))
            total++;
    }
    EXPECT_EQ(11, total);
}

}  // namespace RAMCloud
