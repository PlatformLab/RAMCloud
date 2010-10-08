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
#include "Window.h"

namespace RAMCloud {

class WindowTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(WindowTest);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_reset);
    CPPUNIT_TEST(test_index);
    CPPUNIT_TEST(test_advance);
    CPPUNIT_TEST(test_getLength);
    CPPUNIT_TEST(test_getOffset);
    CPPUNIT_TEST_SUITE_END();

    Window<int, 8> window;

  public:
    WindowTest() : window() {}

    void setUp() {
        for (int i = 0; i < 8; i++)
            window.array[i] = i;
    }

    void test_constructor() {
        size_t approxSize = sizeof(int) * 8; // NOLINT
        CPPUNIT_ASSERT(approxSize <= sizeof(window) &&
                       sizeof(window) <= approxSize + 128);

        Window<int, 8> r2;
        CPPUNIT_ASSERT_EQUAL(0, r2.offset);
        CPPUNIT_ASSERT_EQUAL(0, r2.translation);
        CPPUNIT_ASSERT_EQUAL(0, r2.array[0]);
    }

    void test_reset() {
        window.advance(20);
        window.reset();
        for (int i = 0; i < 8; i++)
            CPPUNIT_ASSERT_EQUAL(0, window.array[i]);
        CPPUNIT_ASSERT_EQUAL(0, window.offset);
        CPPUNIT_ASSERT_EQUAL(0, window.translation);
    }

    void test_index() {
        CPPUNIT_ASSERT_EQUAL(0, window[0]);
        CPPUNIT_ASSERT_EQUAL(7, window[7]);
        window.advance(13);
        for (int i = 0; i < 8; i++)
            window.array[i] = i;
        CPPUNIT_ASSERT_EQUAL(5, window[13]);
        CPPUNIT_ASSERT_EQUAL(7, window[15]);
        CPPUNIT_ASSERT_EQUAL(0, window[16]);
        CPPUNIT_ASSERT_EQUAL(4, window[20]);
    }

    void test_advance() {
        // No advance at all.
        window.advance(0);
        CPPUNIT_ASSERT_EQUAL(0, window[0]);
        CPPUNIT_ASSERT_EQUAL(7, window[7]);

        // Advance a bit (not enough for a wrap-around)
        window.advance(4);
        CPPUNIT_ASSERT_EQUAL(0, window.translation);
        CPPUNIT_ASSERT_EQUAL(4, window[4]);
        CPPUNIT_ASSERT_EQUAL(5, window[5]);
        CPPUNIT_ASSERT_EQUAL(0, window[10]);
        CPPUNIT_ASSERT_EQUAL(0, window[11]);
        window[11] = 99;
        CPPUNIT_ASSERT_EQUAL(99, window.array[3]);

        // Advance enough to wrap around and change translation
        window.advance(13);
        CPPUNIT_ASSERT_EQUAL(-16, window.translation);
        for (int i = 17; i < 25; i++)
            CPPUNIT_ASSERT_EQUAL(0, window[i]);
        for (int i = 0; i < 8; i++)
            window.array[i] = i;
        CPPUNIT_ASSERT_EQUAL(1, window[17]);
        CPPUNIT_ASSERT_EQUAL(7, window[23]);
        CPPUNIT_ASSERT_EQUAL(0, window[24]);
    }

    void test_getLength() {
        CPPUNIT_ASSERT_EQUAL(8, window.getLength());
    }

    void test_getOffset() {
        CPPUNIT_ASSERT_EQUAL(0, window.getOffset());
        window.advance(1);
        CPPUNIT_ASSERT_EQUAL(1, window.getOffset());
        window.advance(5);
        CPPUNIT_ASSERT_EQUAL(6, window.getOffset());
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(WindowTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(WindowTest);

}  // namespace RAMCloud
