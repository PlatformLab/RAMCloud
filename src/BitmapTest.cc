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

// RAMCloud pragma [CPPLINT=0]

#include <Bitmap.h>

#include <cppunit/extensions/HelperMacros.h>

namespace RAMCloud {

class BitmapTest : public CppUnit::TestFixture {
    DISALLOW_COPY_AND_ASSIGN(BitmapTest);
    CPPUNIT_TEST_SUITE(BitmapTest);
    CPPUNIT_TEST(test__sanityCheck);
    CPPUNIT_TEST_SUITE_END();

  public:
    BitmapTest() {}


    void
    setUp()
    {
    }

    void
    tearDown()
    {
    }

    void
    test__sanityCheck()
    {
        Bitmap<8192> free(false);

        free.set(63);
        CPPUNIT_ASSERT(!free.get(62));
        CPPUNIT_ASSERT(free.get(63));
        free.clear(63);

        free.set(0);
        CPPUNIT_ASSERT(free.nextSet(0) == 0);
        CPPUNIT_ASSERT(free.nextSet(1) == 0);
        free.set(0);
        free.clear(1);
        CPPUNIT_ASSERT(free.nextSet(1) == 0);
        CPPUNIT_ASSERT(free.nextSet(2) == 0);
        CPPUNIT_ASSERT(free.nextSet(63) == 0);
        CPPUNIT_ASSERT(free.nextSet(64) == 0);
        free.set(64);
        free.clear(0);
        CPPUNIT_ASSERT(free.nextSet(64) == 64);
        CPPUNIT_ASSERT(free.nextSet(1) == 64);
        free.clear(0);
        free.set(1);
        CPPUNIT_ASSERT(free.nextSet(1) == 1);
        CPPUNIT_ASSERT(free.nextSet(64) == 1);
        free.set(0);
        for (uint32_t i = 1; i < 8192; i++)
            free.clear(i);
        CPPUNIT_ASSERT(free.nextSet(8191) == 0);
        free.clear(0);
        CPPUNIT_ASSERT(free.nextSet(0) == -1);
        CPPUNIT_ASSERT(free.nextSet(2812) == -1);
        CPPUNIT_ASSERT(free.nextSet(8191) == -1);

        free.clear(0);
        CPPUNIT_ASSERT(!free.get(0));
        free.set(0);
        free.set(1);
        CPPUNIT_ASSERT(free.get(0));

        free.clear(0);
        CPPUNIT_ASSERT(!free.get(0));
        free.set(0);
        CPPUNIT_ASSERT(free.get(0));
        Bitmap<8> f(true);

        CPPUNIT_ASSERT(f.get(0));
        CPPUNIT_ASSERT(f.get(7));
        CPPUNIT_ASSERT(f.nextSet(0) == 0);
        CPPUNIT_ASSERT(f.nextSet(1) == 0);
        f.clearAll();
        CPPUNIT_ASSERT(f.nextSet(0) == -1);
        f.set(5);
        CPPUNIT_ASSERT(f.nextSet(0) == 5);
    }

};
CPPUNIT_TEST_SUITE_REGISTRATION(BitmapTest);

} // namespace RAMCloud
