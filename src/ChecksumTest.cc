/* Copyright (c) 2009 Stanford University
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
#include "rabinpoly.h"

class ChecksumTest : public CppUnit::TestFixture {
  public:
    void setUp();
    void tearDown();
    void TestSimple();
  private:
    CPPUNIT_TEST_SUITE(ChecksumTest);
    CPPUNIT_TEST(TestSimple);
    CPPUNIT_TEST_SUITE_END();
};
CPPUNIT_TEST_SUITE_REGISTRATION(ChecksumTest);

void
ChecksumTest::setUp()
{
}

void
ChecksumTest::tearDown()
{
}


void
ChecksumTest::TestSimple()
{
    uint64_t poly;
    uint64_t f, g;

    const char *data = "This is the data to test";

    poly = 0x92d42091a28158a5ull;
    CPPUNIT_ASSERT(polyirreducible(poly));

    rabinpoly *p = new rabinpoly(poly);

    f = 0;
    for (uint64_t i = 0; i < strlen(data); i++)
        f = p->append8(f, data[i]);

    printf("Fingerprint: %llx\n", f);
    delete p;

    p = new rabinpoly(poly);

    g = 0;
    for (uint64_t i = 0; i < strlen(data); i++)
        g = p->append8(g, data[i]);

    printf("Fingerprint: %llx\n", g);
    delete p;

    CPPUNIT_ASSERT(f == g);
}
