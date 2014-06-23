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

#include "TestUtil.h"

#include "Crc32C.h"

namespace RAMCloud {

/*
 * A randomly-generated array of bytes to run checksums over.
 * The length of the array was chosen to exercise combinations
 * of 64-byte, 8-byte, and 1-byte block checksums.
 */
static const uint8_t input[81] = {
    0x67, 0xc6, 0x69, 0x73, 0x51, 0xff, 0x4a, 0xec, 0x29, 0xcd, 0xba, 0xab,
    0xf2, 0xfb, 0xe3, 0x46, 0x7c, 0xc2, 0x54, 0xf8, 0x1b, 0xe8, 0xe7, 0x8d,
    0x76, 0x5a, 0x2e, 0x63, 0x33, 0x9f, 0xc9, 0x9a, 0x66, 0x32, 0x0d, 0xb7,
    0x31, 0x58, 0xa3, 0x5a, 0x25, 0x5d, 0x05, 0x17, 0x58, 0xe9, 0x5e, 0xd4,
    0xab, 0xb2, 0xcd, 0xc6, 0x9b, 0xb4, 0x54, 0x11, 0x0e, 0x82, 0x74, 0x41,
    0x21, 0x3d, 0xdc, 0x87, 0x70, 0xe9, 0x3e, 0xa1, 0x41, 0xe1, 0xfc, 0x67,
    0x3e, 0x01, 0x7e, 0x97, 0xea, 0xdc, 0x6b, 0x96, 0x8f
};

/*
 * Checksums corresponding to the above random array. Each i'th offset is
 * the checksum starting at input[0] and spanning i bytes. That is,
 * crcByLength[0] is a checksum over nothing, and crcByLength[5] is a 
 * checksum from input[0] to input[4], inclusive.
 */ 
static const uint32_t crcByLength[sizeof(input) + 1] = {
    0x00000000, 0xe771a4d8, 0xeebc5abd, 0x46da99bf, 0xdcf560dc, 0x6f7fd19a,
    0x0625abfe, 0x27f4932c, 0x91d78106, 0x2c42362a, 0x65abdd96, 0x3fe6b982,
    0x0a4de0fa, 0xd8ae467e, 0xe5a2d273, 0xc0300d94, 0x6055f200, 0x6ec69d7e,
    0xad4528d5, 0x224dae02, 0xcad359b2, 0x884eee62, 0xbbe10eff, 0xc8412dff,
    0xc2163686, 0xa1d12a12, 0x997ec708, 0x54f844a0, 0x82f47e25, 0x640087a8,
    0xb6307e8c, 0x26416a8f, 0x77f4c148, 0xdecf9669, 0x9a0fed2f, 0x9361a295,
    0x9368ccda, 0x285738bc, 0x76ff3b6c, 0xcf95b68a, 0x8161274c, 0x4b887ad8,
    0xe531f44f, 0x78015721, 0x4447fc5f, 0x86f37046, 0xaea329a3, 0x1e95d3de,
    0x3981eeba, 0xb07196d3, 0xc16032a6, 0xaaf30b3a, 0xecfa00ff, 0xf4cdad2c,
    0x4af83a24, 0x23afff66, 0xf70ccc48, 0x3550a5c9, 0x8abab573, 0x863d8d0f,
    0xbff8cc47, 0x15a5df17, 0x19375068, 0x27eb81d7, 0x037f6203, 0x30b68bca,
    0x61a098f0, 0x3de96a2a, 0x493f2a78, 0x1a65fe06, 0x659dfa5e, 0x11680bfa,
    0x7fec8b9e, 0xf0490a7c, 0x9c3d0285, 0x3806aa1d, 0xbb5146bb, 0xf1885bc7,
    0xdb5bb75e, 0x57b4554b, 0x3ed14a7c, 0xb27d1e9a
};

/**
 * Test fixture for testing Crc32C, parameterized on whether or not to force
 * the software implementation.
 */
struct Crc32CTest : public ::testing::TestWithParam<bool> {
  Crc32CTest() : forceSoftware(GetParam()) {}
  bool forceSoftware;
};
INSTANTIATE_TEST_CASE_P(ForceSoftware, Crc32CTest, ::testing::Bool());

TEST_P(Crc32CTest, single) {
  for (unsigned int i = 0; i < sizeof(input); i++) {
      EXPECT_EQ(crcByLength[i],
                Crc32C(forceSoftware).update(input, i).getResult());
  }
}

TEST_P(Crc32CTest, accumulated) {
    Crc32C crc(forceSoftware);
    EXPECT_EQ(crcByLength[0], crc.getResult());
    for (unsigned int i = 0; i < sizeof(input); i++)
        EXPECT_EQ(crcByLength[i + 1], crc.update(&input[i], 1).getResult());
}

TEST_P(Crc32CTest, accumulatedVaried) {
    /*
     * All of these add up to 81 (sizeof(input)). The goal is to exercise
     * different block's worth for each invocation.
     */
    static const int spans[8][5] = {
        { 1, 7, 8, 64, 1 },
        { 1, 64, 8, 1, 7 },
        { 7, 1, 8, 64, 1 },
        { 7, 1, 1, 8, 64 },
        { 8, 1, 64, 1, 7 },
        { 8, 7, 1, 64, 1 },
        { 64, 7, 8, 1, 1 },
        { 64, 7, 1, 1, 8 }
    };

    for (int i = 0; i < 8; i++) {
        Crc32C crc(forceSoftware);
        int offset = 0;
        for (int j = 0; j < 5; j++) {
            crc.update(&input[offset], spans[i][j]);
            offset += spans[i][j];
            EXPECT_EQ(crcByLength[offset], crc.getResult());
        }
    }
}

TEST_P(Crc32CTest, updateFromBuffer) {
    char buf[8192];

    Crc32C a;
    Crc32C b;
    a.update(buf, sizeof(buf));
    Buffer buffer;
    buffer.appendExternal(buf, sizeof(buf));
    b.update(buffer);
    EXPECT_EQ(a.result, b.result);

    Crc32C c;
    Crc32C d;
    c.update(&buf[1], sizeof(buf) - 2);
    d.update(buffer, 1, sizeof(buf) - 2);
    EXPECT_EQ(c.result, d.result);
}

TEST_P(Crc32CTest, assignmentOperator) {
    Crc32C a;
    a.update(&a, sizeof(a));
    Crc32C b = a;
    EXPECT_EQ(a.result, b.result);
}

} // namespace RAMCloud
