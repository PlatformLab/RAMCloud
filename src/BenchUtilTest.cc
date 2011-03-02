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
#include "BenchUtil.h"

namespace RAMCloud {

class BenchUtilTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(BenchUtilTest);
    CPPUNIT_TEST(test_getCyclesPerSecond_sanity);
    CPPUNIT_TEST(test_cyclesToNanoseconds_sanity);
    CPPUNIT_TEST(test_cyclesToSeconds_sanity);
    CPPUNIT_TEST(test_nanosecondsToCycles_sanity);
    CPPUNIT_TEST(test_fillRandom);
    CPPUNIT_TEST_SUITE_END();

  public:
    BenchUtilTest() {}

    void test_getCyclesPerSecond_sanity() {
        uint64_t cyclesPerSecond = getCyclesPerSecond();
        // We'll never have machines slower than 500MHz, will we?
        CPPUNIT_ASSERT(cyclesPerSecond > 500UL * 1000 * 1000);
        // And we'll never have machines faster than 10GHz, will we?
        CPPUNIT_ASSERT(cyclesPerSecond < 10000UL * 1000 * 1000);
    }

    void test_cyclesToNanoseconds_sanity() {
        // We'll never have machines slower than 500MHz, will we?
        CPPUNIT_ASSERT(1000UL * 1000 * 1000 >
                       cyclesToNanoseconds(500UL * 1000 * 1000));
        // And we'll never have machines faster than 10GHz, will we?
        CPPUNIT_ASSERT(1000UL * 1000 * 1000 <
                       cyclesToNanoseconds(10000UL * 1000 * 1000));
    }

    void test_cyclesToSeconds_sanity() {
        // We'll never have machines slower than 500MHz, will we?
        CPPUNIT_ASSERT(1.0 > cyclesToSeconds(500UL * 1000 * 1000));
        // And we'll never have machines faster than 10GHz, will we?
        CPPUNIT_ASSERT(1.0 < cyclesToSeconds(10000UL * 1000 * 1000));
    }

    void test_nanosecondsToCycles_sanity() {
        CPPUNIT_ASSERT_EQUAL(getCyclesPerSecond(),
                             nanosecondsToCycles(1000UL * 1000 * 1000));
     }

    void test_fillRandom() {
        uint8_t ored[128];
        uint8_t buf[128];
        memset(ored, 0, sizeof(ored));
        for (uint32_t i = 0; i < 50; i++) {
            fillRandom(buf, sizeof(buf));
            for (uint32_t j = 0; j < arrayLength(buf); j++)
                ored[j] |= buf[j];
        }
        for (uint32_t j = 0; j < arrayLength(ored); j++) {
            ++ored[j]; // should overflow
            CPPUNIT_ASSERT_EQUAL(0, ored[j]);
        }
    }

    DISALLOW_COPY_AND_ASSIGN(BenchUtilTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(BenchUtilTest);

}  // namespace RAMCloud
