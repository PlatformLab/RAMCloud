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
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <Common.h>
#include <Segment.h>
#include <config.h> // for SEGMENT_SIZE

#include <cppunit/extensions/HelperMacros.h>

class SegmentTest : public CppUnit::TestFixture {
  public:
    void setUp();
    void tearDown();
    void TestSimple();
    void TestMain();
  private:
    CPPUNIT_TEST_SUITE(SegmentTest);
    CPPUNIT_TEST(TestSimple);
    CPPUNIT_TEST(TestMain);
    CPPUNIT_TEST_SUITE_END();
    RAMCloud::Segment *Seg;
    void *SegBase;
};
CPPUNIT_TEST_SUITE_REGISTRATION(SegmentTest);

void
SegmentTest::setUp()
{
    SegBase = xmalloc(SEGMENT_SIZE);
    Seg = new RAMCloud::Segment(SegBase, SEGMENT_SIZE, NULL);
}

void
SegmentTest::tearDown()
{
    free(SegBase);
    delete Seg;
}

void
SegmentTest::TestSimple()
{
}

void
SegmentTest::TestMain()
{
}
