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
    CPPUNIT_TEST(test_cyclesToNanoseconds_overflow);
    CPPUNIT_TEST(test_cyclesToSeconds_sanity);
    CPPUNIT_TEST(test_nanosecondsToCycles_sanity);
    CPPUNIT_TEST(test_nanosecondsToCycles_overflow);
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

    void test_cyclesToNanoseconds_overflow() {
        const uint64_t cycles[] = {
            10UL,
            100UL,
            1000UL,
            10000UL,
            100000UL,
            1000000UL,
            10000000UL,
            100000000UL,
            1000000000UL,
            10000000000UL,
            100000000000UL,
            1000000000000UL,
            10000000000000UL,
            100000000000000UL,
            1000000000000000UL,
            10000000000000000UL, // ~38 days for a 3GHz machine
            // And beyond that, overflow shouldn't be surprising.
        };

        for (uint32_t i = 1; i < arrayLength(cycles); i++) {
            uint64_t nanos1 = cyclesToNanoseconds(cycles[i - 1]);
            uint64_t nanos10 = cyclesToNanoseconds(cycles[i]);
            CPPUNIT_ASSERT_MESSAGE(
                format("%lu / 10 isn't roughly %lu", nanos10, nanos1),
                nanos10 * 0.08 < nanos1 && nanos10 * 0.12 > nanos1);
        }
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

    void test_nanosecondsToCycles_overflow() {
        const uint64_t nanoseconds[] = {
            1UL, // 1 ns
            10UL,
            100UL,
            1000UL, // 1 us
            10000UL,
            100000UL,
            1000000UL, // 1 ms
            10000000UL,
            100000000UL,
            1000000000UL, // 1 second
            10000000000UL,
            100000000000UL,
            1000000000000UL, // 1,000 seconds
            10000000000000UL,
            100000000000000UL,
            1000000000000000UL, // 1,000,000 seconds (~11 days)
            10000000000000000UL, // 10,000,000 seconds (~115 days)
            // And beyond that, overflow shouldn't be surprising.
        };

        for (uint32_t i = 1; i < arrayLength(nanoseconds); i++) {
            uint64_t cycles1 = nanosecondsToCycles(nanoseconds[i - 1]);
            uint64_t cycles10 = nanosecondsToCycles(nanoseconds[i]);
            CPPUNIT_ASSERT_MESSAGE(
                format("%lu / 10 isn't roughly %lu", cycles10, cycles1),
                cycles10 * 0.08 < cycles1 && cycles10 * 0.12 > cycles1);
        }
    }

    DISALLOW_COPY_AND_ASSIGN(BenchUtilTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(BenchUtilTest);

}  // namespace RAMCloud
