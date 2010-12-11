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

#include "Log.h"
#include "Oustercount.h"

namespace RAMCloud {

/**
 * Unit tests for Oustercount.
 */
class OustercountTest : public CppUnit::TestFixture {

    DISALLOW_COPY_AND_ASSIGN(OustercountTest); // NOLINT

    CPPUNIT_TEST_SUITE(OustercountTest);
    CPPUNIT_TEST(test_Oustercount_constructor);
    CPPUNIT_TEST(test_Oustercount_addObject);
    CPPUNIT_TEST(test_Oustercount_removeObject);
    CPPUNIT_TEST(test_Oustercount_getPartitions);
    CPPUNIT_TEST_SUITE_END();

  public:
    OustercountTest() {}

    void
    test_Oustercount_constructor()
    {
        Oustercount o;
        CPPUNIT_ASSERT(o.root != NULL);
        CPPUNIT_ASSERT(o.root->createTime == LogTime(0, 0));
    }

    void
    test_Oustercount_addObject()
    {
        Oustercount o;
        o.addObject(0, 1, LogTime(0, 0));
    }

    void
    test_Oustercount_removeObject()
    {
    }

    void
    test_Oustercount_getPartitions()
    {
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(OustercountTest);

} // namespace RAMCloud
