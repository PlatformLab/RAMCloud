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

#include "ProtoBufTest.pb.h"

#include "TestUtil.h"
#include "ProtoBuf.h"

namespace RAMCloud {

using namespace ProtoBuf; // NOLINT

class ProtoBufTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(ProtoBufTest);
    CPPUNIT_TEST(test_serializeAndParse);
    CPPUNIT_TEST(test_serializeToBuffer);
    CPPUNIT_TEST(test_parseFromBuffer);
    CPPUNIT_TEST_SUITE_END();

  public:
    ProtoBufTest() {}

    void setUp() {
    }

    void tearDown() {
    }

    void test_serializeAndParse() {
        Buffer buf;
        TestMessage in;
        in.set_foo("bar");
        uint32_t length = serializeToRequest(buf, in);
        CPPUNIT_ASSERT(0 < length && length < 1024);
        CPPUNIT_ASSERT_EQUAL(length, buf.getTotalLength());
        TestMessage out;
        parseFromRequest(buf, 0, length, out);
        CPPUNIT_ASSERT_EQUAL(in.foo(), out.foo());
    }

    void test_serializeToBuffer() {
        Buffer buf;
        TestMessage msg;
        CPPUNIT_ASSERT_THROW(serializeToRequest(buf, msg),
                             RequestFormatError);
    }

    void test_parseFromBuffer() {
        Buffer buf;
        TestMessage msg;
        CPPUNIT_ASSERT_THROW(parseFromRequest(buf, 0, 0, msg),
                             RequestFormatError);
    }

    DISALLOW_COPY_AND_ASSIGN(ProtoBufTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(ProtoBufTest);

}  // namespace RAMCloud
