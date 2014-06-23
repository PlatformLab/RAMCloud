/* Copyright (c) 2010-2014 Stanford University
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

class ProtoBufTest : public ::testing::Test {
  public:
    ProtoBufTest() {}

    DISALLOW_COPY_AND_ASSIGN(ProtoBufTest);
};

TEST_F(ProtoBufTest, serializeAndParse) {
    Buffer buf;
    TestMessage in;
    in.set_foo("bar");
    uint32_t length = serializeToRequest(&buf, &in);
    EXPECT_TRUE(0 < length && length < 1024);
    EXPECT_EQ(length, buf.size());
    TestMessage out;
    parseFromRequest(&buf, 0, length, &out);
    EXPECT_EQ(in.foo(), out.foo());
}

TEST_F(ProtoBufTest, serializeToBuffer) {
    Buffer buf;
    TestMessage msg;
    EXPECT_THROW(serializeToRequest(&buf, &msg), RequestFormatError);
}

TEST_F(ProtoBufTest, parseFromBuffer) {
    Buffer buf;
    TestMessage msg;
    EXPECT_THROW(parseFromRequest(&buf, 0, 0, &msg), RequestFormatError);
}

}  // namespace RAMCloud
