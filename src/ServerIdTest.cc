/* Copyright (c) 2011 Stanford University
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
#include "ServerId.h"

namespace RAMCloud {

class ServerIdTest : public ::testing::Test {
  public:
    ServerIdTest() {}

  private:
    DISALLOW_COPY_AND_ASSIGN(ServerIdTest);
};

TEST_F(ServerIdTest, invalidServerId) {
    EXPECT_EQ(static_cast<uint32_t>(-1),
        ServerId::INVALID_SERVERID_GENERATION_NUMBER);
    EXPECT_EQ(ServerId().generationNumber(),
        ServerId::INVALID_SERVERID_GENERATION_NUMBER);
}

TEST_F(ServerIdTest, constructors) {
    ServerId a;
    EXPECT_EQ(static_cast<uint64_t>(
        ServerId::INVALID_SERVERID_GENERATION_NUMBER) << 32, a.serverId);
    EXPECT_FALSE(a.isValid());

    ServerId b(57);
    EXPECT_EQ(57U, b.serverId);

    ServerId c(57, 31);
    EXPECT_EQ((31UL << 32) | 57UL, c.serverId);
}

TEST_F(ServerIdTest, getId) {
    ServerId a(23472372347234723UL);
    EXPECT_EQ(23472372347234723UL, a.getId());
}

TEST_F(ServerIdTest, indexNumber) {
    ServerId a(287347, 15);
    EXPECT_EQ(287347U, a.indexNumber());
}

TEST_F(ServerIdTest, generationNumber) {
    ServerId a(75, 947273);
    EXPECT_EQ(947273U, a.generationNumber());
}

TEST_F(ServerIdTest, isValid) {
    EXPECT_TRUE(ServerId(0, 0).isValid());
    EXPECT_TRUE(ServerId(0, -2).isValid());
    EXPECT_FALSE(ServerId(0, -1).isValid());
    EXPECT_FALSE(ServerId(2347, -1).isValid());
}

TEST_F(ServerIdTest, toString) {
    EXPECT_EQ("1.2", ServerId(1, 2).toString());
    EXPECT_EQ("invalid", ServerId(77, -1).toString());
}

TEST_F(ServerIdTest, operatorEquals) {
    ServerId a(23742, 77650);
    ServerId b(23742, 77650);
    ServerId c(23741, 77650);
    ServerId d(23742, 77649);

    EXPECT_TRUE(a == a);
    EXPECT_TRUE(a == b);
    EXPECT_FALSE(a == c);
    EXPECT_FALSE(b == c);
    EXPECT_FALSE(a == d);
    EXPECT_FALSE(b == d);
    EXPECT_FALSE(c == d);

    // Two different invalids are equal.
    ServerId invA(234234, -1);
    ServerId invB(7572, -1);
    EXPECT_TRUE(invA == invB);
}

TEST_F(ServerIdTest, operatorNotEquals) {
    ServerId a(23742, 77650);
    ServerId b(23742, 77650);
    ServerId c(23741, 77650);

    EXPECT_FALSE(a != a);
    EXPECT_FALSE(a != b);
    EXPECT_TRUE(a != c);
    EXPECT_TRUE(b != c);
}

TEST_F(ServerIdTest, assignmentOperator) {
    ServerId a(8234872);
    ServerId b = a;
    EXPECT_EQ(a.serverId, b.serverId);
}

TEST_F(ServerIdTest, operatorLess) {
    EXPECT_TRUE(ServerId(4, 2) < ServerId(3, 4));
    EXPECT_FALSE(ServerId(4, 2) < ServerId(5, 1));
    EXPECT_FALSE(ServerId(4, 2) < ServerId(3, 2));
    EXPECT_TRUE(ServerId(3, 2) < ServerId(4, 2));
    EXPECT_FALSE(ServerId(3, 2) < ServerId(3, 2));

}

}  // namespace RAMCloud
