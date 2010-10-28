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
    CPPUNIT_TEST(test_vformat_long);
    CPPUNIT_TEST(test_format_copy);
    CPPUNIT_TEST(test_format_outArg);
    CPPUNIT_TEST(test_generateRandom);
    CPPUNIT_TEST_SUITE_END();

  public:
    CommonTest() {}

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

    DISALLOW_COPY_AND_ASSIGN(CommonTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(CommonTest);

}  // namespace RAMCloud
