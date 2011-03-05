/* Copyright (c) 2010 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
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
#include "Buffer.h"

namespace RAMCloud {

class TestUtilTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(TestUtilTest);

    CPPUNIT_TEST(test_assertEquals_charStar);
    CPPUNIT_TEST(test_toString);

    CPPUNIT_TEST(test_bufferToDebugString);
    CPPUNIT_TEST(test_toString_stringNotTerminated);

    CPPUNIT_TEST(test_convertChar);

    CPPUNIT_TEST_SUITE_END();

  public:
    TestUtilTest()
    {
    }

    void
    test_assertEquals_charStar()
    {
        int catches = 0;
        CPPUNIT_ASSERT_EQUAL(static_cast<const char*>(NULL),
                             static_cast<const char*>(NULL));
        try {
            CPPUNIT_ASSERT_EQUAL(static_cast<const char*>(NULL), "hi");
        } catch (CppUnit::Exception& e) {
            CPPUNIT_ASSERT_EQUAL("equality assertion failed\n"
                                 "- Expected: (NULL)\n"
                                 "- Actual  : hi\n",
                                 e.what());
            ++catches;
        }
        try {
            CPPUNIT_ASSERT_EQUAL("hi", static_cast<const char*>(NULL));
        } catch (CppUnit::Exception& e) {
            CPPUNIT_ASSERT_EQUAL("equality assertion failed\n"
                                 "- Expected: hi\n"
                                 "- Actual  : (NULL)\n",
                                 e.what());
            ++catches;
        }
        CPPUNIT_ASSERT_EQUAL("hi", "hi");
        try {
            CPPUNIT_ASSERT_EQUAL("bye", "hi");
        } catch (CppUnit::Exception& e) {
            CPPUNIT_ASSERT_EQUAL("equality assertion failed\n"
                                 "- Expected: bye\n"
                                 "- Actual  : hi\n",
                                 e.what());
            ++catches;
        }
        CPPUNIT_ASSERT_EQUAL(3, catches);
    }

    void
    test_toString()
    {
        Buffer b;
        int32_t *ip = new(&b, APPEND) int32_t;
        *ip = -45;
        ip = new(&b, APPEND) int32_t;
        *ip = 0x1020304;
        char *p = new(&b, APPEND) char[10];
        memcpy(p, "abcdefghi", 10);
        ip = new(&b, APPEND) int32_t;
        *ip = 99;
        CPPUNIT_ASSERT_EQUAL("-45 0x1020304 abcdefghi/0 99",
                             toString(&b));
    }

    void
    test_toString_stringNotTerminated()
    {
        Buffer b;
        char *p = new(&b, APPEND) char[5];
        memcpy(p, "abcdefghi", 5);
        CPPUNIT_ASSERT_EQUAL("abcde", toString(&b));
    }

    void
    test_bufferToDebugString()
    {
        Buffer b;
        Buffer::Chunk::appendToBuffer(&b, "abc\nxyz", 7);
        Buffer::Chunk::appendToBuffer(&b,
            "0123456789012345678901234567890abcdefg",
            37);
        Buffer::Chunk::appendToBuffer(&b, "xyz", 3);
        CPPUNIT_ASSERT_EQUAL("abc/nxyz | 01234567890123456789(+17 chars) "
                             "| xyz",
                             bufferToDebugString(&b));
    }

    void
    test_convertChar()
    {
        Buffer b;
        const char *test = "abc \x17--\x80--\x3--\n--\x7f--\\--\"--";
        uint32_t length = downCast<uint32_t>(strlen(test)) + 1;
        memcpy(static_cast<char*>(new(&b, APPEND) char[length]),
                test, length);
        CPPUNIT_ASSERT_EQUAL("abc /x17--/x80--/x03--/n--/x7f--/x5c--/x22--/0",
                toString(&b));
    }

    DISALLOW_COPY_AND_ASSIGN(TestUtilTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(TestUtilTest);

}  // namespace RAMCloud
