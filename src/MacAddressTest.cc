/* Copyright (c) 2011 Stanford University
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
#include "MacAddress.h"

namespace RAMCloud {

TEST(MacAddressTest, constructorRaw) {
    uint8_t raw[] = {0xde, 0xad, 0xbe, 0xef, 0x98, 0x76};
    EXPECT_EQ("de:ad:be:ef:98:76",
              MacAddress(raw).toString());
}

TEST(MacAddressTest, constructorString) {
    EXPECT_EQ("de:ad:be:ef:98:76",
              MacAddress("de:ad:be:ef:98:76").toString());
}

TEST(MacAddressTest, constructorRandom) {
    for (uint32_t i = 0; i < 100; ++i) {
        MacAddress r1(MacAddress::RANDOM);
        // make sure it's a locally administered unicast address
        EXPECT_EQ(2, r1.address[0] & 0x03);
        MacAddress r2(MacAddress::RANDOM);
        EXPECT_NE(r1.toString(),
                  r2.toString());
    }
}

TEST(MacAddressTest, clone) {
    MacAddress r1(MacAddress::RANDOM);
    std::unique_ptr<MacAddress> r2(r1.clone());
    EXPECT_EQ(0, memcmp(r1.address, r2->address, 6));
}

TEST(MacAddressTest, toString) {
    // tested sufficiently in constructor tests
}

TEST(MacAddressTest, isNull) {
    uint8_t raw[] = {0x0, 0x0, 0x0, 0x0, 0x0, 0x0};
    MacAddress r(MacAddress::RANDOM);
    EXPECT_TRUE(MacAddress(raw).isNull());
    EXPECT_FALSE(r.isNull());
}

}  // namespace RAMCloud
