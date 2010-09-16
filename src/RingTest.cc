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
#include "Ring.h"

namespace RAMCloud {

class RingTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(RingTest);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_clear);
    CPPUNIT_TEST(test_index);
    CPPUNIT_TEST(test_advance);
    CPPUNIT_TEST_SUITE_END();

    Ring<int, 32> ring;

  public:
    RingTest() : ring() {}

    void setUp() {
        for (int i = 0; i < 32; i++)
            ring.array[i] = i;
    }

    void test_constructor() {
        size_t approxSize = sizeof(int) * 32; // NOLINT
        CPPUNIT_ASSERT(approxSize <= sizeof(ring) &&
                       sizeof(ring) <= approxSize + 128);

        Ring<int, 32> r2;
        CPPUNIT_ASSERT_EQUAL(0, r2.start);
        CPPUNIT_ASSERT_EQUAL(0, r2.array[0]);
    }

    void test_clear() {
        ring.clear();
        for (int i = 0; i < 32; i++)
            CPPUNIT_ASSERT_EQUAL(0, ring.array[i]);
    }

    void test_index() {
        CPPUNIT_ASSERT_EQUAL(0, ring[0]);
        CPPUNIT_ASSERT_EQUAL(31, ring[31]);
        ring.start += 16;
        CPPUNIT_ASSERT_EQUAL(16, ring[0]);
        CPPUNIT_ASSERT_EQUAL(15, ring[31]);
    }

    void test_advance() {
        ring.advance(0);
        CPPUNIT_ASSERT_EQUAL(0, ring[0]);
        CPPUNIT_ASSERT_EQUAL(31, ring[31]);
        ring.advance(16);
        CPPUNIT_ASSERT_EQUAL(16, ring[0]);
        CPPUNIT_ASSERT_EQUAL(31, ring[15]);
        CPPUNIT_ASSERT_EQUAL(0, ring[16]);
        CPPUNIT_ASSERT_EQUAL(0, ring[31]);
        ring.advance(48);
        CPPUNIT_ASSERT_EQUAL(0, ring.start);
        CPPUNIT_ASSERT_EQUAL(0, ring[0]);
        CPPUNIT_ASSERT_EQUAL(0, ring[15]);
        CPPUNIT_ASSERT_EQUAL(0, ring[16]);
        CPPUNIT_ASSERT_EQUAL(0, ring[31]);
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(RingTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(RingTest);

}  // namespace RAMCloud
