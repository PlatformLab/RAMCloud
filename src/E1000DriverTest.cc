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

/**
 * \file
 * Test file for the E1000Driver Class.
 */

#include <E1000Driver.h>

#include <cppunit/extensions/HelperMacros.h>

namespace RAMCloud {

class E1000DriverTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(E1000DriverTest);

    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_getRingBuffer_normal);
    CPPUNIT_TEST(test_getRingBuffer_full);

    CPPUNIT_TEST_SUITE_END();

  public:
    E1000DriverTest() { }

    void setUp() { }

    void tearDown() { }

    void test_constructor() {
        try {
            E1000Driver d;
        } catch(...) {
            CPPUNIT_ASSERT(false);
        }
    }

    void test_getRingBuffer_normal() {
        E1000Driver d;
        E1000Driver::RingBuffer* rb = d.getRingBuffer();
        CPPUNIT_ASSERT_EQUAL(false, rb->inUse);
        rb->inUse = true;
        E1000Driver::RingBuffer* rb2 = d.getRingBuffer();
        CPPUNIT_ASSERT_EQUAL(false, rb2->inUse);
        CPPUNIT_ASSERT_EQUAL((reinterpret_cast<uint8_t*>(rb) +
                              sizeof(E1000Driver::RingBuffer)),
                             reinterpret_cast<uint8_t*>(rb2));
    }

    void test_getRingBuffer_full() {
        E1000Driver d;
        uint64_t i = 0;
        while (1) {
            E1000Driver::RingBuffer* rb = d.getRingBuffer();
            if (rb == NULL) break;
            rb->inUse = true;
            ++i;
        }
        CPPUNIT_ASSERT_EQUAL(i, d.numRxRingBuffers);
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(E1000DriverTest);

}  // namespace RAMCloud
